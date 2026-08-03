use super::MetadataImagePairSample;
use crate::entity::{ChainId, ContractId, Dimension, EntityStore, NftId, ScopeKind};
use crate::error::DedupError;
#[cfg(test)]
use crate::metadata::bm25::may_share_term;
use crate::metadata::bm25::{
    PreparedDocument, UpperBoundPrune, lossless_prefix_len, similarity_at_least,
    similarity_at_least_after_overlap_filter,
};
use crate::progress::ProgressObserver;
use crate::radix::{sort_by_u32_bool_key_while, sort_u32_pairs_while, sort_u32_triples_while};
use crate::sampling::{
    ChainDuplicatePairSamples, ChainPairDuplicatePairSamples, DuplicatePairSample,
    DuplicatePairSamples, PairSampler, PairSamplingPlan,
};
use crate::scope::{ScopeCounts, ScopeKey};
use crate::stats::SummaryAccumulator;
use ahash::{AHashMap, AHasher};
use rayon::prelude::*;
use serde::Serialize;
use std::collections::{BTreeMap, BinaryHeap, HashSet};
use std::hash::{Hash, Hasher};
use std::mem::MaybeUninit;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};

type DocumentId = u32;
type TokenKeyId = u32;

const SCORE_TILE: usize = 256;
const SATURATION_BLOCK: usize = 32;
const MAX_SCORE_TILE_BATCH: u64 = 8;
const INTERN_SHARDS: usize = 256;
const PREPARE_BATCH: usize = 4096;
const INLINE_ANCHORS: usize = 8;
const TOKEN_MASK_WORDS: usize = 4;
const CANDIDATE_SHARDS: usize = 64;
const CANDIDATE_CANCEL_BATCH: u64 = 1 << 20;
#[cfg(test)]
const CANDIDATE_PAIR_CHUNK: usize = 4096;
#[cfg(test)]
const CANDIDATE_SCHEDULING_CHUNK: usize = 1;
const DIRECT_ACTIVITY_BATCH: u64 = 512;
const FULL_DIRECT_PROGRESS_BATCH: u64 = 65_536;
const DENSE_POSTING_MIN_PROFILES: usize = 1_024;
const NO_DENSE_POSTING: u32 = u32::MAX;

#[derive(Clone, Debug, Default, Serialize)]
pub struct MetadataStats {
    pub eligible_contracts: u64,
    pub eligible_contract_ratio: f64,
    pub unique_profiles: u64,
    pub profile_reduction_ratio: f64,
    pub unique_documents: u64,
    pub document_reuse_ratio: f64,
    pub unique_terms: u64,
    pub logical_contract_pairs: u64,
    pub profile_pair_tasks: u64,
    pub profile_pair_reduction_ratio: f64,
    pub equivalent_profile_tasks: u64,
    pub candidate_index_used: bool,
    pub candidate_posting_entries: u64,
    pub candidate_posting_bytes: u64,
    pub candidate_range_bytes: u64,
    pub candidate_index_bytes: u64,
    pub candidate_pair_bytes: u64,
    pub candidate_prefix_terms: u64,
    pub candidate_prefix_term_ratio: f64,
    pub candidate_pair_emissions: u64,
    pub candidate_pair_emission_ratio: f64,
    pub candidate_pair_dedup_reduction_ratio: f64,
    pub candidate_profile_pairs: u64,
    pub candidate_profile_pair_ratio: f64,
    pub candidate_zero_overlap_prunes: u64,
    pub candidate_zero_overlap_prune_ratio: f64,
    pub saturated_profile_pairs: u64,
    pub saturated_profile_pair_ratio: f64,
    pub block_saturated_profile_pairs: u64,
    pub block_saturated_profile_pair_ratio: f64,
    pub exact_document_pairs: u64,
    pub exact_document_pair_ratio: f64,
    pub bm25_cache_hits: u64,
    pub bm25_cache_probes: u64,
    pub bm25_cache_hit_ratio: f64,
    pub bm25_cache_bypassed_pairs: u64,
    pub bm25_cache_bypass_ratio: f64,
    pub bm25_scores: u64,
    pub bm25_score_ratio: f64,
    pub bm25_zero_overlap_prunes: u64,
    pub bm25_zero_overlap_prune_ratio: f64,
    pub bm25_upper_bound_prunes: u64,
    pub bm25_upper_bound_prune_ratio: f64,
    pub bm25_initial_upper_bound_prunes: u64,
    pub bm25_initial_upper_bound_prune_ratio: f64,
    pub bm25_iterative_upper_bound_prunes: u64,
    pub bm25_iterative_upper_bound_prune_ratio: f64,
    pub matched_profile_pairs: u64,
    pub matched_profile_pair_ratio: f64,
}

#[derive(Debug, Hash, PartialEq, Eq)]
struct ProfileKey {
    is_evm: bool,
    is_solana: bool,
    anchors: AnchorKey,
}

#[derive(Debug, Hash, PartialEq, Eq)]
enum AnchorKey {
    Inline {
        len: u8,
        values: [(TokenKeyId, DocumentId); INLINE_ANCHORS],
    },
    Heap(Box<[(TokenKeyId, DocumentId)]>),
}

impl AnchorKey {
    fn from_vec(values: Vec<(TokenKeyId, DocumentId)>) -> Self {
        if values.len() <= INLINE_ANCHORS {
            let mut inline = [(0, 0); INLINE_ANCHORS];
            inline[..values.len()].copy_from_slice(&values);
            Self::Inline {
                len: values.len() as u8,
                values: inline,
            }
        } else {
            Self::Heap(values.into_boxed_slice())
        }
    }

    fn into_boxed_slice(self) -> Box<[(TokenKeyId, DocumentId)]> {
        match self {
            Self::Inline { len, values } => values[..usize::from(len)].into(),
            Self::Heap(values) => values,
        }
    }

    fn remap_tokens(&mut self, remap: &[TokenKeyId]) {
        let anchors = match self {
            Self::Inline { len, values } => &mut values[..usize::from(*len)],
            Self::Heap(values) => values,
        };
        for (token, _) in anchors.iter_mut() {
            *token = remap[*token as usize];
        }
        debug_assert!(anchors.windows(2).all(|pair| pair[0].0 <= pair[1].0));
    }
}

#[derive(Clone, Copy, Debug)]
struct ContractProfile {
    is_evm: bool,
    is_solana: bool,
    has_empty_token_document: bool,
    anchor_start: u32,
    anchor_len: u32,
    max_document: DocumentId,
    token_mask: [u64; TOKEN_MASK_WORDS],
    chain_mask: u64,
    member_start: u32,
    member_len: u32,
    chain_start: u32,
    chain_len: u16,
}

#[derive(Debug)]
struct UnpackedProfile {
    is_evm: bool,
    is_solana: bool,
    anchors: Box<[(TokenKeyId, DocumentId)]>,
    members: ProfileMembers,
    chain_counts: ProfileChainCounts,
}

impl ContractProfile {
    fn max_document(&self) -> DocumentId {
        self.max_document
    }
}

#[derive(Debug)]
struct ProfileMembers {
    first: MetadataMember,
    rest: Option<Vec<MetadataMember>>,
}

impl ProfileMembers {
    fn new(first: MetadataMember) -> Self {
        Self { first, rest: None }
    }

    fn push(&mut self, member: MetadataMember) {
        self.rest.get_or_insert_with(Vec::new).push(member);
    }

    fn len(&self) -> usize {
        1 + self.rest.as_deref().map_or(0, <[MetadataMember]>::len)
    }

    fn iter(&self) -> impl Iterator<Item = MetadataMember> + '_ {
        std::iter::once(self.first).chain(
            self.rest
                .as_deref()
                .into_iter()
                .flat_map(|members| members.iter().copied()),
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct MetadataMember {
    contract_id: ContractId,
    nft_id: Option<NftId>,
}

#[derive(Debug)]
struct ProfileChainCounts {
    first: (ChainId, u32),
    rest: Option<Vec<(ChainId, u32)>>,
}

impl ProfileChainCounts {
    fn new(first: ChainId) -> Self {
        Self {
            first: (first, 1),
            rest: None,
        }
    }

    fn add(&mut self, chain: ChainId) {
        if self.first.0 == chain {
            self.first.1 += 1;
            return;
        }
        let rest = self.rest.get_or_insert_with(Vec::new);
        if let Some((_, count)) = rest.iter_mut().find(|(candidate, _)| *candidate == chain) {
            *count += 1;
        } else {
            rest.push((chain, 1));
        }
    }

    fn iter(&self) -> impl Iterator<Item = (ChainId, u32)> + '_ {
        std::iter::once(self.first).chain(
            self.rest
                .as_deref()
                .into_iter()
                .flat_map(|chains| chains.iter().copied()),
        )
    }
}

struct DirectIndex {
    documents: Vec<PreparedDocument>,
    terms: Vec<(u32, u32)>,
    document_context_weights: Box<[u32]>,
    profiles: Vec<ContractProfile>,
    anchors: Vec<(TokenKeyId, DocumentId)>,
    token_profile_counts: Box<[u32]>,
    members: Vec<MetadataMember>,
    chain_counts: Vec<(ChainId, u32)>,
    #[cfg(test)]
    chain_count: usize,
    query_profile_count: usize,
    eligible_contracts: u64,
    eligible_members: u64,
    anchor_count: u64,
    unique_terms: u64,
    image_witnesses: Option<AHashMap<(ContractId, TokenKeyId, DocumentId), NftId>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct MetadataImagePair {
    contract_a: ContractId,
    nft_a: NftId,
    contract_b: ContractId,
    nft_b: NftId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct MetadataImageSampleEntry {
    priority: u64,
    pair: MetadataImagePair,
}

#[derive(Default)]
struct MetadataImageSampler {
    capacity: usize,
    heap: BinaryHeap<MetadataImageSampleEntry>,
    retained: AHashMap<(ContractId, ContractId), MetadataImagePair>,
}

struct MetadataSamplingResult {
    pairs: PairSampler,
    images: MetadataImageSampler,
}

#[derive(Clone)]
struct CrossSamplingPlan {
    pairs: PairSamplingPlan,
    image_sample_size: usize,
}

impl MetadataImageSampler {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            ..Self::default()
        }
    }

    fn enabled(&self) -> bool {
        self.capacity != 0
    }

    fn observe(
        &mut self,
        left_contract: ContractId,
        left_nft: NftId,
        right_contract: ContractId,
        right_nft: NftId,
    ) {
        if self.capacity == 0 || left_contract == right_contract {
            return;
        }
        let (contract_a, nft_a, contract_b, nft_b) = if left_contract < right_contract {
            (left_contract, left_nft, right_contract, right_nft)
        } else {
            (right_contract, right_nft, left_contract, left_nft)
        };
        let pair = MetadataImagePair {
            contract_a,
            nft_a,
            contract_b,
            nft_b,
        };
        if let Some(current) = self.retained.get_mut(&(contract_a, contract_b)) {
            if pair < *current {
                *current = pair;
            }
            return;
        }
        let priority =
            splitmix64_image_sample((u64::from(contract_a) << 32) | u64::from(contract_b));
        let entry = MetadataImageSampleEntry { priority, pair };
        if self.heap.len() == self.capacity
            && self.heap.peek().is_some_and(|current| entry >= *current)
        {
            return;
        }
        if self.heap.len() == self.capacity {
            let removed = self
                .heap
                .pop()
                .expect("a full image sample heap is non-empty");
            self.retained
                .remove(&(removed.pair.contract_a, removed.pair.contract_b));
        }
        self.heap.push(entry);
        self.retained.insert((contract_a, contract_b), pair);
    }

    fn observe_cross(
        &mut self,
        left: &[(ContractId, NftId)],
        right: &[(ContractId, NftId)],
        salt: u64,
    ) {
        if !self.enabled() || left.is_empty() || right.is_empty() {
            return;
        }
        let remaining = self.capacity.saturating_sub(self.heap.len()).max(1);
        let pair_count = left.len().saturating_mul(right.len());
        if pair_count <= self.capacity.saturating_mul(8) {
            for &(left_contract, left_nft) in left {
                for &(right_contract, right_nft) in right {
                    self.observe(left_contract, left_nft, right_contract, right_nft);
                }
            }
            return;
        }
        for attempt in 0..remaining.saturating_mul(6).max(12) {
            let left_index = image_sample_index(salt, attempt as u64 * 2, left.len());
            let right_index = image_sample_index(salt, attempt as u64 * 2 + 1, right.len());
            let (left_contract, left_nft) = left[left_index];
            let (right_contract, right_nft) = right[right_index];
            self.observe(left_contract, left_nft, right_contract, right_nft);
        }
    }

    fn observe_clique(&mut self, members: &[(ContractId, NftId)], salt: u64) {
        if !self.enabled() || members.len() < 2 {
            return;
        }
        let remaining = self.capacity.saturating_sub(self.heap.len()).max(1);
        let pair_count = members.len().saturating_mul(members.len() - 1) / 2;
        if pair_count <= self.capacity.saturating_mul(8) {
            for left in 0..members.len() - 1 {
                for right in left + 1..members.len() {
                    let (left_contract, left_nft) = members[left];
                    let (right_contract, right_nft) = members[right];
                    self.observe(left_contract, left_nft, right_contract, right_nft);
                }
            }
            return;
        }
        for attempt in 0..remaining.saturating_mul(8).max(16) {
            let left = image_sample_index(salt, attempt as u64 * 2, members.len());
            let mut right = image_sample_index(salt, attempt as u64 * 2 + 1, members.len() - 1);
            if right >= left {
                right += 1;
            }
            let (left_contract, left_nft) = members[left];
            let (right_contract, right_nft) = members[right];
            self.observe(left_contract, left_nft, right_contract, right_nft);
        }
    }

    fn merge(&mut self, other: Self) {
        for pair in other.retained.into_values() {
            self.observe(pair.contract_a, pair.nft_a, pair.contract_b, pair.nft_b);
        }
    }

    fn into_samples(
        self,
        store: &EntityStore,
        progress: &dyn ProgressObserver,
    ) -> Result<Vec<MetadataImagePairSample>, DedupError> {
        let pairs = self.retained.into_values().collect::<Vec<_>>();
        let image_metadata = build_selected_image_metadata(store, &pairs, progress)?;
        pairs
            .into_iter()
            .map(|pair| {
                let contract_a = &store.contracts[pair.contract_a as usize];
                let contract_b = &store.contracts[pair.contract_b as usize];
                let nft_a = &store.nfts[pair.nft_a as usize];
                let nft_b = &store.nfts[pair.nft_b as usize];
                let image_uri_a = nft_a
                    .image_uri_id
                    .map(|id| store.string(id).to_owned())
                    .ok_or_else(|| {
                        DedupError::invalid("metadata", "sampled NFT A has no image URI")
                    })?;
                let image_uri_b = nft_b
                    .image_uri_id
                    .map(|id| store.string(id).to_owned())
                    .ok_or_else(|| {
                        DedupError::invalid("metadata", "sampled NFT B has no image URI")
                    })?;
                let metadata_json_a =
                    image_metadata.get(&pair.nft_a).cloned().ok_or_else(|| {
                        DedupError::invalid("metadata", "sampled NFT A has no Metadata record")
                    })?;
                let metadata_json_b =
                    image_metadata.get(&pair.nft_b).cloned().ok_or_else(|| {
                        DedupError::invalid("metadata", "sampled NFT B has no Metadata record")
                    })?;
                Ok(MetadataImagePairSample {
                    contract_a_chain: store.chain_name(contract_a.chain_id).to_owned(),
                    contract_a_address: contract_a.address.clone(),
                    token_id_a: nft_a.token_id.clone(),
                    image_uri_a,
                    metadata_json_a,
                    contract_b_chain: store.chain_name(contract_b.chain_id).to_owned(),
                    contract_b_address: contract_b.address.clone(),
                    token_id_b: nft_b.token_id.clone(),
                    image_uri_b,
                    metadata_json_b,
                })
            })
            .collect()
    }
}

fn splitmix64_image_sample(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn image_sample_index(salt: u64, ordinal: u64, len: usize) -> usize {
    splitmix64_image_sample(salt ^ ordinal.wrapping_mul(0x9e37_79b9_7f4a_7c15)) as usize % len
}

impl DirectIndex {
    fn document_terms(&self, document: DocumentId) -> &[(u32, u32)] {
        self.documents[document as usize].terms(&self.terms)
    }

    fn anchors(&self, profile: &ContractProfile) -> &[(TokenKeyId, DocumentId)] {
        let start = profile.anchor_start as usize;
        &self.anchors[start..start + profile.anchor_len as usize]
    }

    fn members(&self, profile: &ContractProfile) -> &[MetadataMember] {
        let start = profile.member_start as usize;
        &self.members[start..start + profile.member_len as usize]
    }

    fn chains(&self, profile: &ContractProfile) -> &[(ChainId, u32)] {
        let start = profile.chain_start as usize;
        &self.chain_counts[start..start + usize::from(profile.chain_len)]
    }

    fn exhaustive_profile_pairs(&self) -> u64 {
        choose_two(self.profiles.len() as u64)
    }

    fn logical_member_pairs(&self) -> u64 {
        choose_two(self.eligible_members)
    }

    fn image_member(
        &self,
        member: MetadataMember,
        anchor: (TokenKeyId, DocumentId),
    ) -> Option<(ContractId, NftId)> {
        self.image_witnesses
            .as_ref()?
            .get(&(member.contract_id, anchor.0, anchor.1))
            .copied()
            .map(|nft| (member.contract_id, nft))
    }

    fn image_members(
        &self,
        profile: &ContractProfile,
        anchor: (TokenKeyId, DocumentId),
    ) -> Vec<(ContractId, NftId)> {
        self.members(profile)
            .iter()
            .filter_map(|&member| self.image_member(member, anchor))
            .collect()
    }
}

enum CrossProfilePlan {
    Full,
    Indexed(ResidentCandidateIndex),
}

struct ResidentCandidateIndex {
    shards: Box<[CompactCandidateEntries]>,
    token_ranges: Box<[TokenPostingRanges]>,
    global_full: DensePostingIndex,
    prefixes: DocumentPrefixes,
    include_bm25: bool,
}

#[cfg(test)]
impl ResidentCandidateIndex {
    fn collect_pairs(
        &self,
        index: &DirectIndex,
        progress: &dyn ProgressObserver,
    ) -> Result<CandidateGeneration, DedupError> {
        let generated = generate_candidate_pairs(
            index,
            CandidateSources {
                shards: &self.shards,
                token_ranges: &self.token_ranges,
                global_full: &self.global_full,
                prefixes: &self.prefixes,
            },
            self.include_bm25,
            progress,
        )?;
        Ok(generated)
    }
}

#[cfg(test)]
struct IndexedPairs {
    chunks: Box<[Box<[CandidatePair]>]>,
    len: usize,
}

#[cfg(test)]
impl IndexedPairs {
    fn new(chunks: Vec<Box<[CandidatePair]>>, len: usize) -> Self {
        Self {
            chunks: chunks.into_boxed_slice(),
            len,
        }
    }

    fn iter(&self) -> impl Iterator<Item = &CandidatePair> {
        self.chunks.iter().flat_map(|chunk| chunk.iter())
    }

    fn len(&self) -> usize {
        self.len
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CandidatePair {
    profile_key: u64,
    document_key: u64,
}

#[cfg(test)]
impl CandidatePair {
    fn new(left: u32, right: u32, left_document: DocumentId, right_document: DocumentId) -> Self {
        Self {
            profile_key: profile_pair_key(left, right),
            document_key: document_pair_key(left_document, right_document),
        }
    }

    fn profiles(self) -> (usize, usize) {
        decode_profile_pair(self.profile_key)
    }

    fn documents(self) -> (DocumentId, DocumentId) {
        (
            (self.document_key >> 32) as DocumentId,
            self.document_key as DocumentId,
        )
    }
}

#[derive(Default)]
struct CandidatePlanStats {
    posting_entries: u64,
    posting_bytes: u64,
    range_bytes: u64,
    full_terms: u64,
    prefix_terms: u64,
    pair_emissions: u64,
    candidate_pairs: u64,
    candidate_zero_overlap_prunes: u64,
}

#[derive(Default)]
struct DocumentPrefixes {
    offsets: Box<[u32]>,
    terms: Box<[u32]>,
}

impl DocumentPrefixes {
    fn get(&self, document: DocumentId) -> &[u32] {
        let document = document as usize;
        let start = self.offsets[document] as usize;
        let end = self.offsets[document + 1] as usize;
        &self.terms[start..end]
    }
}

impl CrossProfilePlan {
    fn is_indexed(&self) -> bool {
        matches!(self, Self::Indexed(_))
    }

    fn needs_block_tracking(&self) -> bool {
        matches!(self, Self::Full)
    }
}

struct RawProfile {
    key: ProfileKey,
    member: MetadataMember,
    chain_id: ChainId,
}

#[derive(Clone, Copy)]
struct RawSolanaProfile {
    document: DocumentId,
    member: MetadataMember,
    chain_id: ChainId,
}

struct RawProfileBuckets {
    regular: Vec<Vec<RawProfile>>,
    solana: Vec<Vec<RawSolanaProfile>>,
}

struct DocumentShard<'a> {
    ids: AHashMap<&'a str, DocumentId>,
    values: Vec<(DocumentId, &'a str)>,
}

struct DocumentInterner<'a> {
    shards: Box<[Mutex<DocumentShard<'a>>]>,
    next_id: AtomicU64,
}

type CompactDocuments = (Vec<PreparedDocument>, Vec<(u32, u32)>, u64);

impl<'a> DocumentInterner<'a> {
    fn new() -> Self {
        Self {
            shards: (0..INTERN_SHARDS)
                .map(|_| {
                    Mutex::new(DocumentShard {
                        ids: AHashMap::new(),
                        values: Vec::new(),
                    })
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            next_id: AtomicU64::new(0),
        }
    }

    fn intern(&self, value: &'a str) -> Result<DocumentId, DedupError> {
        let shard_id = intern_shard(value);
        let mut shard = self.shards[shard_id]
            .lock()
            .map_err(|_| DedupError::invalid("metadata", "document interner lock poisoned"))?;
        if let Some(&id) = shard.ids.get(value) {
            return Ok(id);
        }
        let id = DocumentId::try_from(self.next_id.fetch_add(1, Ordering::Relaxed))
            .map_err(|_| DedupError::invalid("metadata", "too many unique metadata documents"))?;
        shard.ids.insert(value, id);
        shard.values.push((id, value));
        Ok(id)
    }

    fn into_documents(
        self,
        progress: &dyn ProgressObserver,
    ) -> Result<CompactDocuments, DedupError> {
        let document_count = usize::try_from(self.next_id.load(Ordering::Relaxed))
            .map_err(|_| DedupError::invalid("metadata", "metadata document count overflow"))?;
        progress.begin_phase("prepare_documents", Some(document_count as u64));
        let mut values = Vec::with_capacity(document_count);
        for shard in self.shards.into_vec() {
            let shard = shard
                .into_inner()
                .map_err(|_| DedupError::invalid("metadata", "document interner lock poisoned"))?;
            values.extend(shard.values);
        }
        let mut ordered_values = vec![None; document_count];
        for (id, value) in values {
            ordered_values[id as usize] = Some(value);
        }
        let terms = TermInterner::new();
        let prepared_chunks = ordered_values
            .par_chunks(PREPARE_BATCH)
            .map_init(
                || (AHashMap::<&'a str, u32>::new(), Vec::<u32>::new()),
                |(local_terms, scratch), chunk| {
                    progress.check_cancelled()?;
                    local_terms.clear();
                    let mut documents = Vec::with_capacity(chunk.len());
                    let mut compact_terms = Vec::new();
                    for &value in chunk {
                        let value = value.ok_or_else(|| {
                            DedupError::invalid("metadata", "missing interned metadata document")
                        })?;
                        let local_term_start =
                            u32::try_from(compact_terms.len()).map_err(|_| {
                                DedupError::invalid(
                                    "metadata",
                                    "metadata chunk term offset overflow",
                                )
                            })?;
                        let document = PreparedDocument::try_new_into(
                            value,
                            |term| {
                                if let Some(&id) = local_terms.get(term) {
                                    return Ok::<u32, DedupError>(id);
                                }
                                let id = terms.intern(term)?;
                                local_terms.insert(term, id);
                                Ok(id)
                            },
                            scratch,
                            &mut compact_terms,
                        )?;
                        documents.push((local_term_start, document));
                    }
                    progress.add_completed(chunk.len() as u64);
                    Ok::<_, DedupError>((documents, compact_terms))
                },
            )
            .collect::<Vec<_>>()
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        let compact_term_count = prepared_chunks
            .iter()
            .map(|(_, terms)| terms.len())
            .sum::<usize>();
        let mut documents = Vec::with_capacity(document_count);
        let mut compact_terms = Vec::with_capacity(compact_term_count);
        for chunk in prepared_chunks {
            let (chunk_documents, mut chunk_terms) = chunk;
            let chunk_term_start = u32::try_from(compact_terms.len())
                .map_err(|_| DedupError::invalid("metadata", "metadata term offset overflow"))?;
            for (local_term_start, mut document) in chunk_documents {
                document.set_term_start(
                    chunk_term_start
                        .checked_add(local_term_start)
                        .ok_or_else(|| {
                            DedupError::invalid("metadata", "metadata term offset overflow")
                        })?,
                );
                documents.push(document);
            }
            compact_terms.append(&mut chunk_terms);
        }
        if documents.len() != document_count {
            return Err(DedupError::invalid(
                "metadata",
                "prepared metadata document count mismatch",
            ));
        }
        Ok((
            documents,
            compact_terms,
            terms.next_id.load(Ordering::Relaxed),
        ))
    }
}

struct TermInterner<'a> {
    shards: Box<[Mutex<AHashMap<&'a str, u32>>]>,
    next_id: AtomicU64,
}

impl<'a> TermInterner<'a> {
    fn new() -> Self {
        Self {
            shards: (0..INTERN_SHARDS)
                .map(|_| Mutex::new(AHashMap::new()))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            next_id: AtomicU64::new(0),
        }
    }

    fn intern(&self, value: &'a str) -> Result<u32, DedupError> {
        let shard_id = intern_shard(value);
        let mut shard = self.shards[shard_id]
            .lock()
            .map_err(|_| DedupError::invalid("metadata", "term interner lock poisoned"))?;
        if let Some(&id) = shard.get(value) {
            return Ok(id);
        }
        let id = u32::try_from(self.next_id.fetch_add(1, Ordering::Relaxed))
            .map_err(|_| DedupError::invalid("metadata", "too many unique metadata terms"))?;
        shard.insert(value, id);
        Ok(id)
    }
}

struct TokenShard<'a> {
    ids: AHashMap<&'a str, TokenKeyId>,
    values: Vec<(TokenKeyId, &'a str)>,
}

struct TokenInterner<'a> {
    shards: Box<[Mutex<TokenShard<'a>>]>,
    next_id: AtomicU64,
}

impl<'a> TokenInterner<'a> {
    fn new() -> Self {
        Self {
            shards: (0..INTERN_SHARDS)
                .map(|_| {
                    Mutex::new(TokenShard {
                        ids: AHashMap::new(),
                        values: Vec::new(),
                    })
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            next_id: AtomicU64::new(0),
        }
    }

    fn intern(&self, value: &'a str) -> Result<TokenKeyId, DedupError> {
        let shard_id = intern_shard(value);
        let mut shard = self.shards[shard_id]
            .lock()
            .map_err(|_| DedupError::invalid("metadata", "token interner lock poisoned"))?;
        if let Some(&id) = shard.ids.get(value) {
            return Ok(id);
        }
        let id = TokenKeyId::try_from(self.next_id.fetch_add(1, Ordering::Relaxed))
            .map_err(|_| DedupError::invalid("metadata", "too many unique metadata token IDs"))?;
        shard.ids.insert(value, id);
        shard.values.push((id, value));
        Ok(id)
    }

    fn into_ordered_remap(self) -> Result<Vec<TokenKeyId>, DedupError> {
        let token_count = usize::try_from(self.next_id.load(Ordering::Relaxed))
            .map_err(|_| DedupError::invalid("metadata", "metadata token count overflow"))?;
        let mut ordered = Vec::with_capacity(token_count);
        for shard in self.shards.into_vec() {
            let shard = shard
                .into_inner()
                .map_err(|_| DedupError::invalid("metadata", "token interner lock poisoned"))?;
            ordered.extend(shard.values);
        }
        ordered.par_sort_unstable_by(|left, right| {
            crate::entity::compare_token_ids(left.1, right.1, true)
                .then_with(|| left.1.cmp(right.1))
        });
        let mut remap = vec![0; token_count];
        for (ordered_id, (old_id, _)) in ordered.into_iter().enumerate() {
            remap[old_id as usize] = TokenKeyId::try_from(ordered_id)
                .map_err(|_| DedupError::invalid("metadata", "too many metadata token IDs"))?;
        }
        Ok(remap)
    }
}

enum HitWords {
    Single(Box<[AtomicU64]>),
    Wide {
        words_per_profile: usize,
        words: Box<[AtomicU64]>,
    },
}

struct ProfileHits {
    words: HitWords,
    chain_count: usize,
    block_unsatisfied: Option<Box<[AtomicU32]>>,
}

impl ProfileHits {
    fn new(profile_count: usize, chain_count: usize, track_blocks: bool) -> Self {
        let words = match chain_count {
            0..=64 => HitWords::Single(
                (0..profile_count)
                    .map(|_| AtomicU64::new(0))
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            ),
            _ => {
                let words_per_profile = chain_count.div_ceil(64);
                HitWords::Wide {
                    words_per_profile,
                    words: (0..profile_count.saturating_mul(words_per_profile))
                        .map(|_| AtomicU64::new(0))
                        .collect::<Vec<_>>()
                        .into_boxed_slice(),
                }
            }
        };
        let block_unsatisfied = (track_blocks && chain_count <= 64).then(|| {
            let block_count = profile_count.div_ceil(SATURATION_BLOCK);
            let mut values = Vec::with_capacity(block_count.saturating_mul(chain_count));
            for block in 0..block_count {
                let block_len =
                    (profile_count - block * SATURATION_BLOCK).min(SATURATION_BLOCK) as u32;
                values.extend((0..chain_count).map(|_| AtomicU32::new(block_len)));
            }
            values.into_boxed_slice()
        });
        Self {
            words,
            chain_count,
            block_unsatisfied,
        }
    }

    fn insert(&self, profile: usize, chain: ChainId) {
        let chain = usize::from(chain);
        if self.is_single_word() {
            self.insert_mask(profile, 1_u64 << chain);
        } else if let HitWords::Wide {
            words_per_profile,
            words,
        } = &self.words
        {
            words[profile * words_per_profile + chain / 64]
                .fetch_or(1_u64 << (chain % 64), Ordering::Relaxed);
        }
    }

    fn contains(&self, profile: usize, chain: ChainId) -> bool {
        let chain = usize::from(chain);
        if self.is_single_word() {
            self.load_mask(profile) & (1_u64 << chain) != 0
        } else if let HitWords::Wide {
            words_per_profile,
            words,
        } = &self.words
        {
            words[profile * words_per_profile + chain / 64].load(Ordering::Relaxed)
                & (1_u64 << (chain % 64))
                != 0
        } else {
            false
        }
    }

    fn contains_all(&self, profile: usize, chains: &[(ChainId, u32)]) -> bool {
        chains
            .iter()
            .all(|(chain, _)| self.contains(profile, *chain))
    }

    fn is_single_word(&self) -> bool {
        !matches!(self.words, HitWords::Wide { .. })
    }

    fn contains_mask(&self, profile: usize, mask: u64) -> bool {
        self.load_mask(profile) & mask == mask
    }

    fn insert_mask(&self, profile: usize, mask: u64) {
        let previous = match &self.words {
            HitWords::Single(words) => words[profile].fetch_or(mask, Ordering::Relaxed),
            HitWords::Wide { .. } => return,
        };
        self.record_new_hits(profile, mask & !previous);
    }

    fn insert_mask_if_missing(&self, profile: usize, mask: u64) {
        if self.load_mask(profile) & mask != mask {
            self.insert_mask(profile, mask);
        }
    }

    fn contains_profile_chains(
        &self,
        profile: usize,
        target: &ContractProfile,
        chains: &[(ChainId, u32)],
    ) -> bool {
        if self.is_single_word() {
            self.contains_mask(profile, target.chain_mask)
        } else {
            self.contains_all(profile, chains)
        }
    }

    fn insert_profile_chains(
        &self,
        profile: usize,
        source: &ContractProfile,
        chains: &[(ChainId, u32)],
    ) {
        if self.is_single_word() {
            self.insert_mask(profile, source.chain_mask);
        } else {
            for &(chain, _) in chains {
                self.insert(profile, chain);
            }
        }
    }

    fn profile_mask(&self, profile: usize) -> Option<u64> {
        self.is_single_word().then(|| self.load_mask(profile))
    }

    fn block_contains_mask(&self, block: usize, mask: u64) -> bool {
        let Some(unsatisfied) = &self.block_unsatisfied else {
            return false;
        };
        let mut remaining = mask;
        while remaining != 0 {
            let chain = remaining.trailing_zeros() as usize;
            if unsatisfied[block * self.chain_count + chain].load(Ordering::Relaxed) != 0 {
                return false;
            }
            remaining &= remaining - 1;
        }
        true
    }

    fn load_mask(&self, profile: usize) -> u64 {
        match &self.words {
            HitWords::Single(words) => words[profile].load(Ordering::Relaxed),
            HitWords::Wide { .. } => 0,
        }
    }

    fn record_new_hits(&self, profile: usize, mut new_hits: u64) {
        let Some(unsatisfied) = &self.block_unsatisfied else {
            return;
        };
        let block = profile / SATURATION_BLOCK;
        while new_hits != 0 {
            let chain = new_hits.trailing_zeros() as usize;
            unsatisfied[block * self.chain_count + chain].fetch_sub(1, Ordering::Relaxed);
            new_hits &= new_hits - 1;
        }
    }
}

#[derive(Default)]
struct AtomicStats {
    saturated_profile_pairs: AtomicU64,
    block_saturated_profile_pairs: AtomicU64,
    exact_document_pairs: AtomicU64,
    bm25_cache_hits: AtomicU64,
    bm25_cache_probes: AtomicU64,
    bm25_cache_bypassed_pairs: AtomicU64,
    bm25_scores: AtomicU64,
    bm25_zero_overlap_prunes: AtomicU64,
    bm25_upper_bound_prunes: AtomicU64,
    bm25_initial_upper_bound_prunes: AtomicU64,
    bm25_iterative_upper_bound_prunes: AtomicU64,
    matched_profile_pairs: AtomicU64,
}

#[derive(Default)]
struct LocalStats {
    saturated_profile_pairs: u64,
    exact_document_pairs: u64,
    bm25_cache_hits: u64,
    bm25_cache_probes: u64,
    bm25_cache_bypassed_pairs: u64,
    bm25_scores: u64,
    bm25_zero_overlap_prunes: u64,
    bm25_upper_bound_prunes: u64,
    bm25_initial_upper_bound_prunes: u64,
    bm25_iterative_upper_bound_prunes: u64,
    matched_profile_pairs: u64,
}

impl LocalStats {
    fn flush(&self, target: &AtomicStats) {
        target
            .saturated_profile_pairs
            .fetch_add(self.saturated_profile_pairs, Ordering::Relaxed);
        target
            .exact_document_pairs
            .fetch_add(self.exact_document_pairs, Ordering::Relaxed);
        target
            .bm25_cache_hits
            .fetch_add(self.bm25_cache_hits, Ordering::Relaxed);
        target
            .bm25_cache_probes
            .fetch_add(self.bm25_cache_probes, Ordering::Relaxed);
        target
            .bm25_cache_bypassed_pairs
            .fetch_add(self.bm25_cache_bypassed_pairs, Ordering::Relaxed);
        target
            .bm25_scores
            .fetch_add(self.bm25_scores, Ordering::Relaxed);
        target
            .bm25_zero_overlap_prunes
            .fetch_add(self.bm25_zero_overlap_prunes, Ordering::Relaxed);
        target
            .bm25_upper_bound_prunes
            .fetch_add(self.bm25_upper_bound_prunes, Ordering::Relaxed);
        target
            .bm25_initial_upper_bound_prunes
            .fetch_add(self.bm25_initial_upper_bound_prunes, Ordering::Relaxed);
        target
            .bm25_iterative_upper_bound_prunes
            .fetch_add(self.bm25_iterative_upper_bound_prunes, Ordering::Relaxed);
        target
            .matched_profile_pairs
            .fetch_add(self.matched_profile_pairs, Ordering::Relaxed);
    }
}

#[cfg(test)]
pub fn run_direct(
    store: &EntityStore,
    evm_chains: &HashSet<String>,
    anchors_k: impl Into<Option<usize>>,
    threshold: f64,
    acc: &mut SummaryAccumulator,
    progress: &dyn ProgressObserver,
) -> Result<MetadataStats, DedupError> {
    let index = build_index(store, evm_chains, anchors_k.into(), progress)?;
    run_prepared_direct(
        store,
        index,
        threshold,
        acc,
        progress,
        PairSamplingPlan::disabled(),
        0,
    )
    .map(|(stats, _, _)| stats)
}

pub fn run_direct_releasing(
    store: &mut EntityStore,
    evm_chains: &HashSet<String>,
    anchors_k: impl Into<Option<usize>>,
    threshold: f64,
    acc: &mut SummaryAccumulator,
    progress: &dyn ProgressObserver,
) -> Result<MetadataStats, DedupError> {
    let index = build_index(store, evm_chains, anchors_k.into(), progress)?;
    store.release_metadata();
    run_prepared_direct(
        store,
        index,
        threshold,
        acc,
        progress,
        PairSamplingPlan::disabled(),
        0,
    )
    .map(|(stats, _, _)| stats)
}

#[allow(clippy::too_many_arguments)]
pub fn run_direct_releasing_with_samples(
    store: &mut EntityStore,
    evm_chains: &HashSet<String>,
    anchors_k: impl Into<Option<usize>>,
    threshold: f64,
    acc: &mut SummaryAccumulator,
    progress: &dyn ProgressObserver,
    sample_size: usize,
) -> Result<
    (
        MetadataStats,
        DuplicatePairSamples,
        Vec<MetadataImagePairSample>,
    ),
    DedupError,
> {
    let index = build_index_with_image_witnesses(
        store,
        evm_chains,
        anchors_k.into(),
        progress,
        sample_size != 0,
    )?;
    if sample_size == 0 {
        store.release_metadata();
        return run_prepared_direct(
            store,
            index,
            threshold,
            acc,
            progress,
            PairSamplingPlan::disabled(),
            0,
        )
        .map(|(stats, _, _)| (stats, DuplicatePairSamples::default(), Vec::new()));
    }

    let result = run_prepared_direct(
        store,
        index,
        threshold,
        acc,
        progress,
        PairSamplingPlan::disabled(),
        sample_size,
    );
    let result = result.and_then(|(stats, _, image_samples)| {
        let image_samples = image_samples.into_samples(store, progress)?;
        let contract_samples = contract_samples_from_images(&image_samples);
        Ok((stats, contract_samples, image_samples))
    });
    store.release_metadata();
    result
}

fn build_selected_image_metadata(
    store: &EntityStore,
    pairs: &[MetadataImagePair],
    progress: &dyn ProgressObserver,
) -> Result<AHashMap<NftId, String>, DedupError> {
    progress.begin_phase(
        "selected_image_metadata_index",
        Some((pairs.len() as u64).saturating_mul(2)),
    );
    let mut required = AHashMap::<ContractId, AHashMap<&str, NftId>>::new();
    let mut completed = 0_u64;
    for pair in pairs {
        for nft_id in [pair.nft_a, pair.nft_b] {
            let nft = &store.nfts[nft_id as usize];
            required
                .entry(nft.contract_id)
                .or_default()
                .insert(&nft.token_id, nft_id);
            completed += 1;
            if completed == PREPARE_BATCH as u64 {
                progress.add_completed(completed);
                progress.check_cancelled()?;
                completed = 0;
            }
        }
    }
    progress.add_completed(completed);
    progress.check_cancelled()?;

    let required_nfts = required.values().map(|tokens| tokens.len()).sum::<usize>();
    let metadata_work = required.keys().fold(0_u64, |total, &contract_id| {
        total.saturating_add(
            store.contracts[contract_id as usize]
                .metadata_by_token
                .len() as u64,
        )
    });
    progress.begin_phase("selected_image_metadata", Some(metadata_work));
    let mut metadata = AHashMap::with_capacity(required_nfts);
    completed = 0;
    for (contract_id, required_tokens) in required {
        for record in &store.contracts[contract_id as usize].metadata_by_token {
            if let Some(&nft_id) = required_tokens.get(record.token_id.as_str()) {
                metadata.insert(nft_id, record.canonical_json.clone());
            }
            completed += 1;
            if completed == PREPARE_BATCH as u64 {
                progress.add_completed(completed);
                progress.check_cancelled()?;
                completed = 0;
            }
        }
    }
    progress.add_completed(completed);
    progress.check_cancelled()?;
    if metadata.len() != required_nfts {
        return Err(DedupError::invalid(
            "metadata",
            "one or more sampled NFTs have no matching metadata record",
        ));
    }
    Ok(metadata)
}

fn contract_samples_from_images(image_samples: &[MetadataImagePairSample]) -> DuplicatePairSamples {
    let all_chains = image_samples
        .iter()
        .map(|sample| DuplicatePairSample {
            contract_a_chain: sample.contract_a_chain.clone(),
            contract_a_address: sample.contract_a_address.clone(),
            contract_b_chain: sample.contract_b_chain.clone(),
            contract_b_address: sample.contract_b_address.clone(),
        })
        .collect::<Vec<_>>();
    let mut intra_chain = BTreeMap::<String, Vec<DuplicatePairSample>>::new();
    let mut chain_pairs = BTreeMap::<(String, String), Vec<DuplicatePairSample>>::new();
    let mut cross_chain_summary = BTreeMap::<String, Vec<DuplicatePairSample>>::new();

    for pair in &all_chains {
        if pair.contract_a_chain == pair.contract_b_chain {
            intra_chain
                .entry(pair.contract_a_chain.clone())
                .or_default()
                .push(pair.clone());
            continue;
        }

        let (chain_a, chain_b) = if pair.contract_a_chain < pair.contract_b_chain {
            (&pair.contract_a_chain, &pair.contract_b_chain)
        } else {
            (&pair.contract_b_chain, &pair.contract_a_chain)
        };
        chain_pairs
            .entry((chain_a.clone(), chain_b.clone()))
            .or_default()
            .push(pair.clone());
        cross_chain_summary
            .entry(chain_a.clone())
            .or_default()
            .push(pair.clone());
        cross_chain_summary
            .entry(chain_b.clone())
            .or_default()
            .push(pair.clone());
    }

    DuplicatePairSamples {
        all_chains,
        intra_chain: intra_chain
            .into_iter()
            .map(|(chain, pairs)| ChainDuplicatePairSamples { chain, pairs })
            .collect(),
        chain_pairs: chain_pairs
            .into_iter()
            .map(
                |((chain_a, chain_b), pairs)| ChainPairDuplicatePairSamples {
                    chain_a,
                    chain_b,
                    pairs,
                },
            )
            .collect(),
        cross_chain_summary: cross_chain_summary
            .into_iter()
            .map(|(chain, pairs)| ChainDuplicatePairSamples { chain, pairs })
            .collect(),
    }
}

fn run_prepared_direct(
    store: &EntityStore,
    index: DirectIndex,
    threshold: f64,
    acc: &mut SummaryAccumulator,
    progress: &dyn ProgressObserver,
    sampling: PairSamplingPlan,
    image_sample_size: usize,
) -> Result<(MetadataStats, PairSampler, MetadataImageSampler), DedupError> {
    let eligible_members = index.eligible_members;
    if eligible_members < 2 {
        return Ok((
            base_stats(store, &index, 0, 0, 0),
            sampling.sampler(),
            MetadataImageSampler::new(image_sample_size),
        ));
    }

    let logical_contract_pairs = index.logical_member_pairs();
    let equivalent_profile_tasks = index
        .profiles
        .iter()
        .filter(|profile| profile.member_len > 1)
        .count() as u64;
    let exhaustive_cross_profile_tasks = index.exhaustive_profile_pairs();
    let (cross_profile_plan, mut candidate_stats) =
        build_candidate_plan(&index, threshold, exhaustive_cross_profile_tasks, progress)?;
    let hits = ProfileHits::new(
        index.profiles.len(),
        store.chains.len(),
        cross_profile_plan.needs_block_tracking(),
    );
    let stats = AtomicStats::default();
    let full_universe_samples = if threshold <= 0.0 {
        let samples = sampling.sampler();
        samples
            .enabled()
            .then(|| sample_all_eligible_contract_pairs(store, &index, samples, progress))
    } else {
        None
    }
    .transpose()?;
    let scoring_sampling = if full_universe_samples.is_some() {
        PairSamplingPlan::disabled()
    } else {
        sampling.clone()
    };
    let equivalent_work = equivalent_scoring_work(&index);
    progress.begin_phase("direct_bm25_equivalent", Some(equivalent_work));
    let mut samples = score_equivalent_profiles(
        &index,
        &hits,
        &stats,
        progress,
        scoring_sampling.clone(),
        image_sample_size,
    )?;
    let (cross_summary, cross_samples) = score_cross_profiles(
        &index,
        &hits,
        threshold,
        &stats,
        progress,
        &cross_profile_plan,
        CrossSamplingPlan {
            pairs: scoring_sampling,
            image_sample_size,
        },
    )?;
    if let Some(full_universe_samples) = full_universe_samples {
        samples.pairs = full_universe_samples;
    } else {
        samples.pairs.merge(cross_samples.pairs);
    }
    samples.images.merge(cross_samples.images);
    candidate_stats.prefix_terms = cross_summary.prefix_terms;
    candidate_stats.pair_emissions = cross_summary.pair_emissions;
    candidate_stats.candidate_pairs = cross_summary.pair_count;
    candidate_stats.candidate_zero_overlap_prunes = cross_summary.zero_overlap_prunes;
    let cross_profile_tasks = if cross_profile_plan.is_indexed() {
        cross_summary.pair_count
    } else {
        exhaustive_cross_profile_tasks
    };
    let profile_pair_tasks = equivalent_profile_tasks.saturating_add(cross_profile_tasks);

    let reduce_merge_tasks = index
        .profiles
        .len()
        .div_ceil(PREPARE_BATCH)
        .saturating_sub(1) as u64;
    progress.begin_phase(
        "reduce",
        Some(eligible_members.saturating_add(reduce_merge_tasks)),
    );
    let metadata_memberships = index
        .profiles
        .par_chunks(PREPARE_BATCH)
        .enumerate()
        .map(|(chunk_id, profiles)| {
            progress.check_cancelled()?;
            let mut memberships = AHashMap::new();
            let mut completed = 0_u64;
            for (offset, profile) in profiles.iter().enumerate() {
                let profile_id = chunk_id * PREPARE_BATCH + offset;
                let profile_chains = index.chains(profile);
                for &member in index.members(profile) {
                    let contract = &store.contracts[member.contract_id as usize];
                    let contract_chain = contract.chain_id;
                    if let Some(cross_profile_mask) = hits.profile_mask(profile_id) {
                        let own_chain_count = profile_chains
                            .iter()
                            .find(|(candidate, _)| *candidate == contract_chain)
                            .map(|(_, count)| *count)
                            .expect("a profile member's chain is represented in its profile");
                        let own_chain_bit = 1_u64 << usize::from(contract_chain);
                        let equivalent_mask = if own_chain_count > 1 {
                            profile.chain_mask
                        } else {
                            profile.chain_mask & !own_chain_bit
                        };
                        record_metadata_mask(
                            &mut memberships,
                            store,
                            contract_chain,
                            member,
                            cross_profile_mask | equivalent_mask,
                        )?;
                    } else {
                        record_wide_metadata_hits(
                            &mut memberships,
                            store,
                            &hits,
                            profile_id,
                            profile_chains,
                            contract_chain,
                            member,
                        )?;
                    }
                    completed += 1;
                    if completed == PREPARE_BATCH as u64 {
                        // Keep one unit pending until this chunk's contract IDs
                        // have been compacted, so the reduce phase cannot report
                        // 100% while a chunk is still sorting.
                        progress.add_completed(completed - 1);
                        progress.check_cancelled()?;
                        completed = 1;
                    }
                }
            }
            for members in memberships.values_mut() {
                progress.check_cancelled()?;
                members.dedup_contracts();
                progress.check_cancelled()?;
            }
            progress.add_completed(completed);
            Ok::<_, DedupError>(memberships)
        })
        .try_reduce_with(|left, right| {
            progress.check_cancelled()?;
            let contract_entries = |memberships: &AHashMap<_, MetadataScopeMembers>| {
                memberships.values().fold(0_usize, |total, members| {
                    total.saturating_add(members.contracts.len())
                })
            };
            let (mut left, right) = if contract_entries(&left) >= contract_entries(&right) {
                (left, right)
            } else {
                (right, left)
            };
            for (key, value) in right {
                if let Some(target) = left.get_mut(&key) {
                    target.merge(value);
                } else {
                    left.insert(key, value);
                }
            }
            progress.add_completed(1);
            progress.check_cancelled()?;
            Ok(left)
        })
        .transpose()?
        .unwrap_or_default();
    let reduce_contracts_work = metadata_memberships.values().fold(0_u64, |total, members| {
        total.saturating_add(members.contracts.len() as u64)
    });
    progress.begin_phase("reduce_contracts", Some(reduce_contracts_work));
    let metadata_counts = metadata_memberships
        .into_iter()
        .collect::<Vec<_>>()
        .into_par_iter()
        .map(|(key, members)| {
            progress.check_cancelled()?;
            let completed = members.contracts.len() as u64;
            let counts = members.into_counts();
            progress.check_cancelled()?;
            progress.add_completed(completed);
            Ok::<_, DedupError>((key, counts))
        })
        .collect::<Vec<_>>()
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .collect();
    acc.merge_unique_contract_counts(metadata_counts);

    let mut result = base_stats(
        store,
        &index,
        logical_contract_pairs,
        profile_pair_tasks,
        equivalent_profile_tasks,
    );
    result.candidate_index_used = cross_profile_plan.is_indexed();
    result.candidate_posting_entries = candidate_stats.posting_entries;
    result.candidate_posting_bytes = candidate_stats.posting_bytes;
    result.candidate_range_bytes = candidate_stats.range_bytes;
    result.candidate_index_bytes = candidate_stats
        .posting_bytes
        .saturating_add(candidate_stats.range_bytes);
    // Candidate pairs are generated and scored immediately. Keep these
    // measurements at zero because there is no resident pair storage.
    result.candidate_pair_bytes = 0;
    result.candidate_prefix_terms = candidate_stats.prefix_terms;
    result.candidate_prefix_term_ratio =
        ratio(candidate_stats.prefix_terms, candidate_stats.full_terms);
    result.candidate_pair_emissions = candidate_stats.pair_emissions;
    result.candidate_pair_emission_ratio = ratio(
        candidate_stats.pair_emissions,
        exhaustive_cross_profile_tasks,
    );
    result.candidate_pair_dedup_reduction_ratio = reduction_ratio(
        candidate_stats.candidate_pairs,
        candidate_stats.pair_emissions,
    );
    result.candidate_profile_pairs = cross_profile_tasks;
    result.candidate_profile_pair_ratio =
        ratio(cross_profile_tasks, exhaustive_cross_profile_tasks);
    result.candidate_zero_overlap_prunes = candidate_stats.candidate_zero_overlap_prunes;
    result.candidate_zero_overlap_prune_ratio = ratio(
        candidate_stats.candidate_zero_overlap_prunes,
        candidate_stats
            .candidate_pairs
            .saturating_add(candidate_stats.candidate_zero_overlap_prunes),
    );
    result.saturated_profile_pairs = stats.saturated_profile_pairs.load(Ordering::Relaxed);
    result.block_saturated_profile_pairs =
        stats.block_saturated_profile_pairs.load(Ordering::Relaxed);
    result.exact_document_pairs = stats.exact_document_pairs.load(Ordering::Relaxed);
    result.bm25_cache_hits = stats.bm25_cache_hits.load(Ordering::Relaxed);
    result.bm25_cache_probes = stats.bm25_cache_probes.load(Ordering::Relaxed);
    result.bm25_cache_bypassed_pairs = stats.bm25_cache_bypassed_pairs.load(Ordering::Relaxed);
    result.bm25_scores = stats.bm25_scores.load(Ordering::Relaxed);
    result.bm25_zero_overlap_prunes = stats.bm25_zero_overlap_prunes.load(Ordering::Relaxed);
    result.bm25_upper_bound_prunes = stats.bm25_upper_bound_prunes.load(Ordering::Relaxed);
    result.bm25_initial_upper_bound_prunes = stats
        .bm25_initial_upper_bound_prunes
        .load(Ordering::Relaxed);
    result.bm25_iterative_upper_bound_prunes = stats
        .bm25_iterative_upper_bound_prunes
        .load(Ordering::Relaxed);
    debug_assert_eq!(
        result.bm25_upper_bound_prunes,
        result
            .bm25_initial_upper_bound_prunes
            .saturating_add(result.bm25_iterative_upper_bound_prunes)
    );
    result.matched_profile_pairs = stats.matched_profile_pairs.load(Ordering::Relaxed);
    result.saturated_profile_pair_ratio = ratio(result.saturated_profile_pairs, profile_pair_tasks);
    result.block_saturated_profile_pair_ratio =
        ratio(result.block_saturated_profile_pairs, profile_pair_tasks);
    result.exact_document_pair_ratio = ratio(result.exact_document_pairs, profile_pair_tasks);
    result.bm25_cache_hit_ratio = ratio(result.bm25_cache_hits, result.bm25_cache_probes);
    result.bm25_cache_bypass_ratio = ratio(result.bm25_cache_bypassed_pairs, profile_pair_tasks);
    result.bm25_score_ratio = ratio(result.bm25_scores, profile_pair_tasks);
    result.bm25_zero_overlap_prune_ratio =
        ratio(result.bm25_zero_overlap_prunes, result.bm25_scores);
    result.bm25_upper_bound_prune_ratio = ratio(result.bm25_upper_bound_prunes, result.bm25_scores);
    result.bm25_initial_upper_bound_prune_ratio =
        ratio(result.bm25_initial_upper_bound_prunes, result.bm25_scores);
    result.bm25_iterative_upper_bound_prune_ratio =
        ratio(result.bm25_iterative_upper_bound_prunes, result.bm25_scores);
    result.matched_profile_pair_ratio = ratio(result.matched_profile_pairs, profile_pair_tasks);
    Ok((result, samples.pairs, samples.images))
}

fn sample_all_eligible_contract_pairs(
    store: &EntityStore,
    index: &DirectIndex,
    mut samples: PairSampler,
    progress: &dyn ProgressObserver,
) -> Result<PairSampler, DedupError> {
    let member_work = index.members.len() as u64;
    let contract_work = store.contracts.len() as u64;
    progress.begin_phase(
        "direct_bm25_sample_all",
        Some(
            member_work
                .saturating_add(contract_work)
                .saturating_add(contract_work),
        ),
    );
    let mut eligible = vec![0_u64; store.contracts.len().div_ceil(u64::BITS as usize)];
    let mut completed = 0_u64;
    for member in &index.members {
        let contract = member.contract_id as usize;
        eligible[contract / u64::BITS as usize] |= 1_u64 << (contract % u64::BITS as usize);
        completed += 1;
        if completed == PREPARE_BATCH as u64 {
            progress.add_completed(completed);
            progress.check_cancelled()?;
            completed = 0;
        }
    }
    progress.add_completed(completed);

    let mut contracts = Vec::with_capacity(index.eligible_contracts as usize);
    for (word_index, mut word) in eligible.into_iter().enumerate() {
        while word != 0 {
            let bit = word.trailing_zeros() as usize;
            word &= word - 1;
            contracts.push((word_index * u64::BITS as usize + bit) as ContractId);
        }
    }
    progress.add_completed(contract_work);
    progress.check_cancelled()?;
    samples.observe_clique_by(
        contracts.len(),
        |contract| contracts[contract],
        0x4d45_5441_414c_4c00,
    );
    progress.add_completed(contract_work);
    Ok(samples)
}

fn build_index(
    store: &EntityStore,
    evm_chains: &HashSet<String>,
    anchors_k: impl Into<Option<usize>>,
    progress: &dyn ProgressObserver,
) -> Result<DirectIndex, DedupError> {
    build_index_with_image_witnesses(store, evm_chains, anchors_k, progress, false)
}

fn build_index_with_image_witnesses(
    store: &EntityStore,
    evm_chains: &HashSet<String>,
    anchors_k: impl Into<Option<usize>>,
    progress: &dyn ProgressObserver,
    retain_image_witnesses: bool,
) -> Result<DirectIndex, DedupError> {
    let anchors_k = anchors_k.into();
    progress.begin_phase("prepare_direct", Some(store.contracts.len() as u64));
    let documents = DocumentInterner::new();
    let tokens = TokenInterner::new();
    let eligible_contracts = AtomicU64::new(0);
    let eligible_members = AtomicU64::new(0);
    let anchor_count = AtomicU64::new(0);
    let mut profile_buckets = store
        .contracts
        .par_chunks(PREPARE_BATCH)
        .map(|contracts| {
            progress.check_cancelled()?;
            let mut profile_buckets = empty_profile_buckets();
            let mut local_documents: AHashMap<&str, DocumentId> = AHashMap::new();
            let mut local_tokens: AHashMap<&str, TokenKeyId> = AHashMap::new();
            for contract in contracts {
                let is_solana = store.is_solana_chain(contract.chain_id);
                let is_evm = evm_chains.contains(store.chain_name(contract.chain_id));
                let take = anchors_k.map_or(contract.metadata_by_token.len(), |limit| {
                    contract.metadata_by_token.len().min(limit)
                });
                if take == 0 {
                    continue;
                }
                eligible_contracts.fetch_add(1, Ordering::Relaxed);
                if is_solana {
                    eligible_members.fetch_add(take as u64, Ordering::Relaxed);
                    anchor_count.fetch_add(take as u64, Ordering::Relaxed);
                    for record in &contract.metadata_by_token[..take] {
                        let document_id = if let Some(&id) =
                            local_documents.get(record.canonical_json.as_str())
                        {
                            id
                        } else {
                            let id = documents.intern(&record.canonical_json)?;
                            local_documents.insert(&record.canonical_json, id);
                            id
                        };
                        let nft_id =
                            store.nft_id(contract.id, &record.token_id).ok_or_else(|| {
                                DedupError::invalid(
                                    "metadata",
                                    "Solana metadata anchor has no matching NFT",
                                )
                            })?;
                        let raw = RawSolanaProfile {
                            document: document_id,
                            member: MetadataMember {
                                contract_id: contract.id,
                                nft_id: Some(nft_id),
                            },
                            chain_id: contract.chain_id,
                        };
                        let shard = intern_shard(&raw.document);
                        profile_buckets.solana[shard].push(raw);
                    }
                    continue;
                }
                eligible_members.fetch_add(1, Ordering::Relaxed);
                let selected_count = if is_evm { take } else { 1 };
                anchor_count.fetch_add(selected_count as u64, Ordering::Relaxed);
                let first = if is_evm { 0 } else { take - 1 };
                let mut anchors = Vec::with_capacity(selected_count);
                for record in &contract.metadata_by_token[first..take] {
                    let document_id =
                        if let Some(&id) = local_documents.get(record.canonical_json.as_str()) {
                            id
                        } else {
                            let id = documents.intern(&record.canonical_json)?;
                            local_documents.insert(&record.canonical_json, id);
                            id
                        };
                    let token_key = if is_evm {
                        let normalized_token = normalized_evm_token(&record.token_id);
                        if let Some(&id) = local_tokens.get(normalized_token) {
                            id
                        } else {
                            let id = tokens.intern(normalized_token)?;
                            local_tokens.insert(normalized_token, id);
                            id
                        }
                    } else {
                        0
                    };
                    anchors.push((token_key, document_id));
                }
                let raw = RawProfile {
                    key: ProfileKey {
                        is_evm,
                        is_solana: false,
                        anchors: AnchorKey::from_vec(anchors),
                    },
                    member: MetadataMember {
                        contract_id: contract.id,
                        nft_id: None,
                    },
                    chain_id: contract.chain_id,
                };
                let shard = intern_shard(&raw.key);
                profile_buckets.regular[shard].push(raw);
            }
            progress.add_completed(contracts.len() as u64);
            Ok::<_, DedupError>(profile_buckets)
        })
        .try_reduce(empty_profile_buckets, |mut left, right| {
            for (target, mut source) in left.regular.iter_mut().zip(right.regular) {
                target.append(&mut source);
            }
            for (target, mut source) in left.solana.iter_mut().zip(right.solana) {
                target.append(&mut source);
            }
            Ok(left)
        })?;
    let token_remap = tokens.into_ordered_remap()?;
    profile_buckets.regular.par_iter_mut().for_each(|bucket| {
        for raw in bucket {
            raw.key.anchors.remap_tokens(&token_remap);
        }
    });
    let (documents, terms, unique_terms) = documents.into_documents(progress)?;

    progress.begin_phase("profiles", Some(eligible_members.load(Ordering::Relaxed)));
    let RawProfileBuckets { regular, solana } = profile_buckets;
    let (regular_chunks, solana_chunks) = rayon::join(
        || {
            regular
                .into_par_iter()
                .map(|bucket| build_profile_bucket(bucket, progress))
                .collect::<Vec<_>>()
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
        },
        || {
            solana
                .into_par_iter()
                .map(|bucket| build_solana_profile_bucket(bucket, progress))
                .collect::<Vec<_>>()
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
        },
    );
    let mut profile_chunks = regular_chunks?;
    profile_chunks.extend(solana_chunks?);
    let profile_capacity = profile_chunks
        .iter()
        .map(|chunk| chunk.0.len())
        .sum::<usize>();
    let anchor_capacity = profile_chunks
        .iter()
        .map(|chunk| chunk.1.len())
        .sum::<usize>();
    let member_capacity = profile_chunks
        .iter()
        .map(|chunk| chunk.2.len())
        .sum::<usize>();
    let chain_capacity = profile_chunks
        .iter()
        .map(|chunk| chunk.3.len())
        .sum::<usize>();
    let mut document_context_weights = vec![0_u32; documents.len()];
    let mut token_profile_counts = vec![0_u32; token_remap.len()];
    let mut anchor_offset = 0_u32;
    let mut member_offset = 0_u32;
    let mut chain_offset = 0_u32;
    for (chunk_profiles, chunk_anchors, chunk_members, chunk_chain_counts) in &mut profile_chunks {
        let current_anchor_offset = anchor_offset;
        let current_member_offset = member_offset;
        let current_chain_offset = chain_offset;
        anchor_offset = anchor_offset
            .checked_add(
                u32::try_from(chunk_anchors.len()).map_err(|_| {
                    DedupError::invalid("metadata", "metadata anchor offset overflow")
                })?,
            )
            .ok_or_else(|| DedupError::invalid("metadata", "metadata anchor offset overflow"))?;
        member_offset = member_offset
            .checked_add(
                u32::try_from(chunk_members.len()).map_err(|_| {
                    DedupError::invalid("metadata", "metadata member offset overflow")
                })?,
            )
            .ok_or_else(|| DedupError::invalid("metadata", "metadata member offset overflow"))?;
        chain_offset =
            chain_offset
                .checked_add(u32::try_from(chunk_chain_counts.len()).map_err(|_| {
                    DedupError::invalid("metadata", "metadata chain offset overflow")
                })?)
                .ok_or_else(|| DedupError::invalid("metadata", "metadata chain offset overflow"))?;
        for profile in chunk_profiles {
            let start = profile.anchor_start as usize;
            let end = start + profile.anchor_len as usize;
            for &(token, document) in &chunk_anchors[start..end] {
                if profile.is_evm {
                    let weight = &mut document_context_weights[document as usize];
                    *weight = weight.saturating_add(1);
                    token_profile_counts[token as usize] =
                        token_profile_counts[token as usize].saturating_add(1);
                    profile.has_empty_token_document |=
                        documents[document as usize].terms(&terms).is_empty();
                }
            }
            let weight = &mut document_context_weights[profile.max_document() as usize];
            *weight = weight.saturating_add(1);
            profile.anchor_start = profile
                .anchor_start
                .checked_add(current_anchor_offset)
                .ok_or_else(|| {
                    DedupError::invalid("metadata", "metadata anchor offset overflow")
                })?;
            profile.member_start = profile
                .member_start
                .checked_add(current_member_offset)
                .ok_or_else(|| {
                    DedupError::invalid("metadata", "metadata member offset overflow")
                })?;
            profile.chain_start = profile
                .chain_start
                .checked_add(current_chain_offset)
                .ok_or_else(|| DedupError::invalid("metadata", "metadata chain offset overflow"))?;
        }
    }
    let mut profile_parts = Vec::with_capacity(profile_chunks.len());
    let mut anchor_parts = Vec::with_capacity(profile_chunks.len());
    let mut member_parts = Vec::with_capacity(profile_chunks.len());
    let mut chain_parts = Vec::with_capacity(profile_chunks.len());
    for (profiles, anchors, members, chains) in profile_chunks {
        profile_parts.push(profiles);
        anchor_parts.push(anchors);
        member_parts.push(members);
        chain_parts.push(chains);
    }
    progress.begin_phase(
        "profile_flatten",
        Some(
            profile_capacity
                .saturating_add(anchor_capacity)
                .saturating_add(member_capacity)
                .saturating_add(chain_capacity) as u64,
        ),
    );
    let ((mut profiles, anchors), (members, chain_counts)) = rayon::join(
        move || {
            rayon::join(
                move || {
                    profile_parts
                        .into_par_iter()
                        .map(|chunk| {
                            progress.add_completed(chunk.len() as u64);
                            chunk
                        })
                        .flatten()
                        .collect::<Vec<_>>()
                },
                move || {
                    anchor_parts
                        .into_par_iter()
                        .map(|chunk| {
                            progress.add_completed(chunk.len() as u64);
                            chunk
                        })
                        .flatten()
                        .collect::<Vec<_>>()
                },
            )
        },
        move || {
            rayon::join(
                move || {
                    member_parts
                        .into_par_iter()
                        .map(|chunk| {
                            progress.add_completed(chunk.len() as u64);
                            chunk
                        })
                        .flatten()
                        .collect::<Vec<_>>()
                },
                move || {
                    chain_parts
                        .into_par_iter()
                        .map(|chunk| {
                            progress.add_completed(chunk.len() as u64);
                            chunk
                        })
                        .flatten()
                        .collect::<Vec<_>>()
                },
            )
        },
    );
    let profile_sort_passes = if profiles.len() > 1 { 4 } else { 0 };
    progress.begin_phase("profile_sort", Some(profile_sort_passes));
    if !sort_by_u32_bool_key_while(
        &mut profiles,
        |profile| (profile.max_document(), profile.is_solana),
        || {
            progress.add_completed(1);
            progress.check_cancelled().is_ok()
        },
    ) {
        return Err(DedupError::Interrupted);
    }
    let query_profile_count = profiles.len();
    let mut index = DirectIndex {
        documents,
        terms,
        document_context_weights: document_context_weights.into_boxed_slice(),
        profiles,
        anchors,
        token_profile_counts: token_profile_counts.into_boxed_slice(),
        members,
        chain_counts,
        #[cfg(test)]
        chain_count: store.chains.len(),
        query_profile_count,
        eligible_contracts: eligible_contracts.load(Ordering::Relaxed),
        eligible_members: eligible_members.load(Ordering::Relaxed),
        anchor_count: anchor_count.load(Ordering::Relaxed),
        unique_terms,
        image_witnesses: None,
    };
    if retain_image_witnesses {
        index.image_witnesses = Some(build_image_witnesses(store, &index, progress)?);
    }
    Ok(index)
}

fn build_image_witnesses(
    store: &EntityStore,
    index: &DirectIndex,
    progress: &dyn ProgressObserver,
) -> Result<AHashMap<(ContractId, TokenKeyId, DocumentId), NftId>, DedupError> {
    let mut witnesses = AHashMap::new();
    progress.begin_phase("image_witnesses", Some(index.profiles.len() as u64));
    let mut completed = 0_u64;
    for profile in &index.profiles {
        let anchors = index.anchors(profile);
        for &member in index.members(profile) {
            if let Some(nft_id) = member.nft_id {
                let nft = &store.nfts[nft_id as usize];
                if nft.image_uri_id.is_some() {
                    let anchor = anchors
                        .first()
                        .copied()
                        .expect("a Solana metadata profile has one anchor");
                    witnesses.insert((member.contract_id, anchor.0, anchor.1), nft_id);
                }
                continue;
            }
            let contract = &store.contracts[member.contract_id as usize];
            let records = if profile.is_evm {
                &contract.metadata_by_token[..anchors.len()]
            } else {
                let start = contract.metadata_by_token.len().saturating_sub(1);
                &contract.metadata_by_token[start..]
            };
            if records.len() != anchors.len() {
                return Err(DedupError::invalid(
                    "metadata",
                    "metadata image witness layout does not match profile anchors",
                ));
            }
            for (&anchor, record) in anchors.iter().zip(records) {
                let nft_id = store
                    .nft_id(member.contract_id, &record.token_id)
                    .ok_or_else(|| {
                        DedupError::invalid(
                            "metadata",
                            "metadata image witness has no matching NFT",
                        )
                    })?;
                if store.nfts[nft_id as usize].image_uri_id.is_some() {
                    witnesses.insert((member.contract_id, anchor.0, anchor.1), nft_id);
                }
            }
        }
        completed += 1;
        if completed == PREPARE_BATCH as u64 {
            progress.add_completed(completed);
            progress.check_cancelled()?;
            completed = 0;
        }
    }
    progress.add_completed(completed);
    progress.check_cancelled()?;
    Ok(witnesses)
}

fn empty_profile_buckets() -> RawProfileBuckets {
    RawProfileBuckets {
        regular: (0..INTERN_SHARDS).map(|_| Vec::new()).collect(),
        solana: (0..INTERN_SHARDS).map(|_| Vec::new()).collect(),
    }
}

struct DensePostingIndex {
    offsets: Box<[usize]>,
    profiles: Box<[u32]>,
    dense_indices: Box<[u32]>,
    dense_postings: Box<[DensePosting]>,
    dense_words: Box<[u64]>,
    logical_len: u64,
}

impl DensePostingIndex {
    fn empty() -> Self {
        Self {
            offsets: Box::new([0]),
            profiles: Box::new([]),
            dense_indices: Box::new([]),
            dense_postings: Box::new([]),
            dense_words: Box::new([]),
            logical_len: 0,
        }
    }

    fn posting_after(&self, key: u32, left: u32) -> PostingView<'_> {
        if left == u32::MAX {
            return PostingView::EMPTY;
        }
        let key = key as usize;
        if key + 1 >= self.offsets.len() {
            return PostingView::EMPTY;
        }
        let dense_index = self
            .dense_indices
            .get(key)
            .copied()
            .unwrap_or(NO_DENSE_POSTING);
        if dense_index != NO_DENSE_POSTING {
            return self.dense_postings[dense_index as usize].view(&self.dense_words, left + 1);
        }
        let posting = &self.profiles[self.offsets[key]..self.offsets[key + 1]];
        let start = posting.partition_point(|profile| *profile <= left);
        PostingView::Sparse(&posting[start..])
    }

    fn len(&self) -> u64 {
        self.logical_len
    }

    fn posting_bytes(&self) -> u64 {
        (self.profiles.len() as u64)
            .saturating_mul(std::mem::size_of::<u32>() as u64)
            .saturating_add(
                (self.dense_words.len() as u64).saturating_mul(std::mem::size_of::<u64>() as u64),
            )
    }

    fn range_bytes(&self) -> u64 {
        (self.offsets.len() as u64)
            .saturating_mul(std::mem::size_of::<usize>() as u64)
            .saturating_add(
                (self.dense_indices.len() as u64).saturating_mul(std::mem::size_of::<u32>() as u64),
            )
            .saturating_add(
                (self.dense_postings.len() as u64)
                    .saturating_mul(std::mem::size_of::<DensePosting>() as u64),
            )
    }
}

#[derive(Clone, Copy)]
struct SharedProfileOutput(*mut MaybeUninit<u32>);

// Each lane receives disjoint per-term ranges computed from its dense cursor row.
unsafe impl Send for SharedProfileOutput {}
unsafe impl Sync for SharedProfileOutput {}

impl SharedProfileOutput {
    unsafe fn write(self, position: usize, profile: u32) {
        unsafe {
            self.0.add(position).write(MaybeUninit::new(profile));
        }
    }
}

#[derive(Clone, Copy)]
struct SharedCursorOutput(*mut usize);

// Terms are partitioned across tasks, so every cursor slot has one writer.
unsafe impl Send for SharedCursorOutput {}
unsafe impl Sync for SharedCursorOutput {}

impl SharedCursorOutput {
    unsafe fn replace(self, position: usize, value: usize) -> usize {
        unsafe {
            let cursor = self.0.add(position);
            let previous = cursor.read();
            cursor.write(value);
            previous
        }
    }
}

#[derive(Default)]
struct CandidateEntries {
    token_full: Vec<(u32, u32, u32)>,
    global_exact: Vec<(u32, u32)>,
    token_exact: Vec<(u32, u32, u32)>,
}

struct CompactCandidateEntries {
    token_full: CompactPosting<u64>,
    global_exact: CompactPosting<u32>,
    token_exact: CompactPosting<u64>,
}

struct CompactPosting<K> {
    keys: Box<[K]>,
    offsets: Box<[usize]>,
    profiles: Box<[u32]>,
    dense_indices: Box<[u32]>,
    dense_postings: Box<[DensePosting]>,
    dense_words: Box<[u64]>,
    logical_len: u64,
}

#[derive(Clone, Copy)]
struct DensePosting {
    word_start: usize,
    word_len: u32,
    base_word: u32,
}

impl DensePosting {
    fn view<'a>(self, words: &'a [u64], minimum: u32) -> PostingView<'a> {
        let start = self.word_start;
        let all_words = &words[start..start + self.word_len as usize];
        let minimum_word = minimum / u64::BITS;
        let first_offset = minimum_word.saturating_sub(self.base_word) as usize;
        if first_offset >= all_words.len() {
            return PostingView::EMPTY;
        }
        let words = &all_words[first_offset..];
        let base_word = self.base_word + first_offset as u32;
        let leading_empty = words
            .iter()
            .enumerate()
            .position(|(offset, &word)| {
                masked_dense_word(word, base_word + offset as u32, minimum) != 0
            })
            .unwrap_or(words.len());
        if leading_empty == words.len() {
            PostingView::EMPTY
        } else {
            PostingView::Dense {
                words: &words[leading_empty..],
                base_word: base_word + leading_empty as u32,
                minimum,
            }
        }
    }
}

#[derive(Clone, Copy)]
enum PostingView<'a> {
    Sparse(&'a [u32]),
    Dense {
        words: &'a [u64],
        base_word: u32,
        minimum: u32,
    },
}

impl<'a> PostingView<'a> {
    const EMPTY: Self = Self::Sparse(&[]);

    fn is_empty(self) -> bool {
        matches!(self, Self::Sparse(profiles) if profiles.is_empty())
    }

    fn iter(self) -> PostingIter<'a> {
        match self {
            Self::Sparse(profiles) => PostingIter::Sparse(profiles.iter().copied()),
            Self::Dense {
                words,
                base_word,
                minimum,
            } => PostingIter::Dense(DensePostingIter {
                words,
                word_offset: 0,
                current_word: 0,
                base_word,
                minimum,
            }),
        }
    }

    #[cfg(test)]
    fn to_vec(self) -> Vec<u32> {
        self.iter().collect()
    }
}

enum PostingIter<'a> {
    Sparse(std::iter::Copied<std::slice::Iter<'a, u32>>),
    Dense(DensePostingIter<'a>),
}

impl Iterator for PostingIter<'_> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Sparse(iter) => iter.next(),
            Self::Dense(iter) => iter.next(),
        }
    }
}

struct DensePostingIter<'a> {
    words: &'a [u64],
    word_offset: usize,
    current_word: u64,
    base_word: u32,
    minimum: u32,
}

impl Iterator for DensePostingIter<'_> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current_word != 0 {
                let bit = self.current_word.trailing_zeros();
                self.current_word &= self.current_word - 1;
                return Some(
                    (self.base_word + self.word_offset as u32 - 1)
                        .saturating_mul(u64::BITS)
                        .saturating_add(bit),
                );
            }
            let &word = self.words.get(self.word_offset)?;
            self.current_word =
                masked_dense_word(word, self.base_word + self.word_offset as u32, self.minimum);
            self.word_offset += 1;
        }
    }
}

fn masked_dense_word(word: u64, word_index: u32, minimum: u32) -> u64 {
    let minimum_word = minimum / u64::BITS;
    match word_index.cmp(&minimum_word) {
        std::cmp::Ordering::Less => 0,
        std::cmp::Ordering::Greater => word,
        std::cmp::Ordering::Equal => word & (u64::MAX << (minimum % u64::BITS)),
    }
}

fn use_dense_posting(profiles: &[u32]) -> bool {
    if profiles.len() < DENSE_POSTING_MIN_PROFILES {
        return false;
    }
    let first_word = profiles[0] as usize / u64::BITS as usize;
    let last_word = profiles[profiles.len() - 1] as usize / u64::BITS as usize;
    let dense_bytes = (last_word - first_word + 1).saturating_mul(std::mem::size_of::<u64>());
    let sparse_bytes = profiles.len().saturating_mul(std::mem::size_of::<u32>());
    dense_bytes.saturating_mul(2) <= sparse_bytes
}

fn append_dense_posting(profiles: &[u32], dense_words: &mut Vec<u64>) -> DensePosting {
    let base_word = profiles[0] / u64::BITS;
    let last_word = profiles[profiles.len() - 1] / u64::BITS;
    let word_start = dense_words.len();
    dense_words.resize(word_start + (last_word - base_word + 1) as usize, 0);
    for &profile in profiles {
        dense_words[word_start + (profile / u64::BITS - base_word) as usize] |=
            1_u64 << (profile % u64::BITS);
    }
    DensePosting {
        word_start,
        word_len: last_word - base_word + 1,
        base_word,
    }
}

#[derive(Clone, Copy, Default)]
struct CandidateCounts {
    global_full: u64,
    token_full: u64,
    global_exact: u64,
    token_exact: u64,
}

impl CandidateEntries {
    fn with_approximate_capacity(counts: CandidateCounts) -> Result<Self, DedupError> {
        let capacity = |total: u64| {
            usize::try_from(total.div_ceil(CANDIDATE_SHARDS as u64))
                .map_err(|_| DedupError::invalid("metadata", "candidate posting size overflow"))
        };
        Ok(Self {
            token_full: Vec::with_capacity(capacity(counts.token_full)?),
            global_exact: Vec::with_capacity(capacity(counts.global_exact)?),
            token_exact: Vec::with_capacity(capacity(counts.token_exact)?),
        })
    }

    fn append_from(&mut self, other: &mut Self) {
        self.token_full.append(&mut other.token_full);
        self.global_exact.append(&mut other.global_exact);
        self.token_exact.append(&mut other.token_exact);
    }

    fn posting_entries(&self) -> u64 {
        [
            self.token_full.len(),
            self.global_exact.len(),
            self.token_exact.len(),
        ]
        .into_iter()
        .fold(0_u64, |total, len| total.saturating_add(len as u64))
    }

    fn sort_work(&self) -> u64 {
        (self.global_exact.len() as u64)
            .saturating_mul(6)
            .saturating_add((self.token_full.len() as u64).saturating_mul(9))
            .saturating_add((self.token_exact.len() as u64).saturating_mul(9))
    }

    fn into_compact(self) -> CompactCandidateEntries {
        CompactCandidateEntries {
            token_full: CompactPosting::from_triples(self.token_full),
            global_exact: CompactPosting::from_pairs(self.global_exact),
            token_exact: CompactPosting::from_triples(self.token_exact),
        }
    }
}

impl CompactCandidateEntries {
    fn posting_entries(&self) -> u64 {
        [
            self.token_full.logical_len,
            self.global_exact.logical_len,
            self.token_exact.logical_len,
        ]
        .into_iter()
        .fold(0_u64, u64::saturating_add)
    }

    fn posting_bytes(&self) -> u64 {
        self.token_full
            .posting_bytes()
            .saturating_add(self.global_exact.posting_bytes())
            .saturating_add(self.token_exact.posting_bytes())
    }

    fn range_bytes(&self) -> u64 {
        self.token_full
            .range_bytes()
            .saturating_add(self.global_exact.range_bytes())
            .saturating_add(self.token_exact.range_bytes())
    }

    fn global_exact_after(&self, key: u32, left: u32) -> PostingView<'_> {
        self.global_exact.posting_after(key, left)
    }

    fn token_full(&self, token: u32, range: PostingKeyRange) -> CompactTokenPosting<'_> {
        self.token_full.for_token(token, range)
    }

    fn token_exact(&self, token: u32, range: PostingKeyRange) -> CompactTokenPosting<'_> {
        self.token_exact.for_token(token, range)
    }
}

struct CompactTokenPosting<'a> {
    posting: &'a CompactPosting<u64>,
    key_start: usize,
    key_end: usize,
    token: u32,
}

impl<'a> CompactTokenPosting<'a> {
    fn posting_after(&self, second: u32, left: u32) -> PostingView<'a> {
        let key = pack_pair_key((self.token, second));
        let keys = &self.posting.keys[self.key_start..self.key_end];
        let Ok(local_position) = keys.binary_search(&key) else {
            return PostingView::EMPTY;
        };
        let position = self.key_start + local_position;
        self.posting.posting_at(position, left)
    }

    fn visit_postings_after(
        &self,
        sorted_seconds: &[u32],
        left: u32,
        mut visit: impl FnMut(PostingView<'a>),
    ) {
        let keys = &self.posting.keys[self.key_start..self.key_end];
        if sorted_seconds.len().saturating_mul(8) < keys.len() {
            let mut search_start = 0;
            for &requested in sorted_seconds {
                let key = pack_pair_key((self.token, requested));
                let local_position = match keys[search_start..].binary_search(&key) {
                    Ok(position) => search_start + position,
                    Err(position) => {
                        search_start += position;
                        if search_start == keys.len() {
                            break;
                        }
                        continue;
                    }
                };
                let position = self.key_start + local_position;
                visit(self.posting.posting_at(position, left));
                // Keep the matching key in the suffix so duplicate requests
                // preserve the individual-lookup behavior of this branch.
                search_start = local_position;
            }
            return;
        }
        let mut key_position = 0;
        let mut second_position = 0;
        while key_position < keys.len() && second_position < sorted_seconds.len() {
            let key_second = keys[key_position] as u32;
            let requested = sorted_seconds[second_position];
            match key_second.cmp(&requested) {
                std::cmp::Ordering::Less => key_position += 1,
                std::cmp::Ordering::Greater => second_position += 1,
                std::cmp::Ordering::Equal => {
                    let position = self.key_start + key_position;
                    visit(self.posting.posting_at(position, left));
                    key_position += 1;
                    second_position += 1;
                }
            }
        }
    }
}

#[derive(Clone, Copy)]
struct PostingKeyRange {
    start: usize,
    end: usize,
}

impl Default for PostingKeyRange {
    fn default() -> Self {
        Self {
            start: usize::MAX,
            end: 0,
        }
    }
}

#[derive(Clone, Copy, Default)]
struct TokenPostingRanges {
    full: PostingKeyRange,
    exact: PostingKeyRange,
}

impl CompactPosting<u32> {
    fn from_pairs(entries: Vec<(u32, u32)>) -> Self {
        Self::from_sorted(entries)
    }
}

impl CompactPosting<u64> {
    fn from_triples(entries: Vec<(u32, u32, u32)>) -> Self {
        Self::from_sorted(
            entries
                .into_iter()
                .map(|(first, second, profile)| (pack_pair_key((first, second)), profile)),
        )
    }

    fn for_token(&self, token: u32, range: PostingKeyRange) -> CompactTokenPosting<'_> {
        let (key_start, key_end) = if range.start == usize::MAX {
            (0, 0)
        } else {
            (range.start, range.end)
        };
        CompactTokenPosting {
            posting: self,
            key_start,
            key_end,
            token,
        }
    }
}

impl<K: Copy + Ord> CompactPosting<K> {
    fn from_sorted(entries: impl IntoIterator<Item = (K, u32)>) -> Self {
        let entries = entries.into_iter();
        let (lower, _) = entries.size_hint();
        let mut keys = Vec::new();
        let mut offsets = Vec::new();
        let mut profiles = Vec::with_capacity(lower);
        let mut dense_indices = Vec::new();
        let mut dense_postings = Vec::new();
        let mut dense_words = Vec::new();
        let mut logical_len = 0_u64;
        offsets.push(0);
        let mut current_key = None;
        let mut group = Vec::new();
        for (key, profile) in entries {
            if current_key.is_some_and(|candidate| candidate != key) {
                append_compact_posting(
                    current_key.expect("a posting key exists"),
                    &group,
                    &mut keys,
                    &mut offsets,
                    &mut profiles,
                    &mut dense_indices,
                    &mut dense_postings,
                    &mut dense_words,
                    &mut logical_len,
                );
                group.clear();
                current_key = Some(key);
            } else if current_key.is_none() {
                current_key = Some(key);
            }
            if group.last() != Some(&profile) {
                group.push(profile);
            }
        }
        if let Some(key) = current_key {
            append_compact_posting(
                key,
                &group,
                &mut keys,
                &mut offsets,
                &mut profiles,
                &mut dense_indices,
                &mut dense_postings,
                &mut dense_words,
                &mut logical_len,
            );
        }
        Self {
            keys: keys.into_boxed_slice(),
            offsets: offsets.into_boxed_slice(),
            profiles: profiles.into_boxed_slice(),
            dense_indices: dense_indices.into_boxed_slice(),
            dense_postings: dense_postings.into_boxed_slice(),
            dense_words: dense_words.into_boxed_slice(),
            logical_len,
        }
    }

    fn posting_after(&self, key: K, left: u32) -> PostingView<'_> {
        let Ok(position) = self.keys.binary_search(&key) else {
            return PostingView::EMPTY;
        };
        self.posting_at(position, left)
    }

    fn posting_at(&self, position: usize, left: u32) -> PostingView<'_> {
        if left == u32::MAX {
            return PostingView::EMPTY;
        }
        let dense_index = self
            .dense_indices
            .get(position)
            .copied()
            .unwrap_or(NO_DENSE_POSTING);
        if dense_index != NO_DENSE_POSTING {
            return self.dense_postings[dense_index as usize].view(&self.dense_words, left + 1);
        }
        let posting = &self.profiles[self.offsets[position]..self.offsets[position + 1]];
        let start = posting.partition_point(|profile| *profile <= left);
        PostingView::Sparse(&posting[start..])
    }

    fn posting_bytes(&self) -> u64 {
        (self.profiles.len() as u64)
            .saturating_mul(std::mem::size_of::<u32>() as u64)
            .saturating_add(
                (self.dense_words.len() as u64).saturating_mul(std::mem::size_of::<u64>() as u64),
            )
    }

    fn range_bytes(&self) -> u64 {
        (self.keys.len() as u64)
            .saturating_mul(std::mem::size_of::<K>() as u64)
            .saturating_add(
                (self.offsets.len() as u64).saturating_mul(std::mem::size_of::<usize>() as u64),
            )
            .saturating_add(
                (self.dense_indices.len() as u64).saturating_mul(std::mem::size_of::<u32>() as u64),
            )
            .saturating_add(
                (self.dense_postings.len() as u64)
                    .saturating_mul(std::mem::size_of::<DensePosting>() as u64),
            )
    }
}

#[allow(clippy::too_many_arguments)]
fn append_compact_posting<K: Copy>(
    key: K,
    group: &[u32],
    keys: &mut Vec<K>,
    offsets: &mut Vec<usize>,
    profiles: &mut Vec<u32>,
    dense_indices: &mut Vec<u32>,
    dense_postings: &mut Vec<DensePosting>,
    dense_words: &mut Vec<u64>,
    logical_len: &mut u64,
) {
    if group.len() < 2 {
        return;
    }
    keys.push(key);
    *logical_len = logical_len.saturating_add(group.len() as u64);
    if use_dense_posting(group) {
        if dense_indices.is_empty() {
            dense_indices.resize(keys.len(), NO_DENSE_POSTING);
        } else {
            dense_indices.push(NO_DENSE_POSTING);
        }
        *dense_indices.last_mut().expect("a posting key exists") = dense_postings.len() as u32;
        dense_postings.push(append_dense_posting(group, dense_words));
    } else {
        if !dense_indices.is_empty() {
            dense_indices.push(NO_DENSE_POSTING);
        }
        profiles.extend_from_slice(group);
    }
    offsets.push(profiles.len());
}

fn pack_pair_key((first, second): (u32, u32)) -> u64 {
    (u64::from(first) << 32) | u64::from(second)
}

impl CandidateCounts {
    fn add(&mut self, other: Self) {
        self.global_full = self.global_full.saturating_add(other.global_full);
        self.token_full = self.token_full.saturating_add(other.token_full);
        self.global_exact = self.global_exact.saturating_add(other.global_exact);
        self.token_exact = self.token_exact.saturating_add(other.token_exact);
    }

    fn posting_entries(self) -> u64 {
        [
            self.global_full,
            self.token_full,
            self.global_exact,
            self.token_exact,
        ]
        .into_iter()
        .fold(0_u64, u64::saturating_add)
    }

    fn full_terms(self) -> u64 {
        self.global_full.saturating_add(self.token_full)
    }

    fn posting_bytes(self, unique_terms: u64) -> u64 {
        let global_full_bytes = if self.global_full == 0 {
            std::mem::size_of::<usize>() as u64
        } else {
            self.global_full
                .saturating_mul(std::mem::size_of::<u32>() as u64)
                .saturating_add(
                    unique_terms
                        .saturating_add(1)
                        .saturating_mul(std::mem::size_of::<usize>() as u64),
                )
        };
        let triple_entries = self.token_full.saturating_add(self.token_exact);
        global_full_bytes
            .saturating_add(
                self.global_exact
                    .saturating_mul(std::mem::size_of::<(u32, u32)>() as u64),
            )
            .saturating_add(
                triple_entries.saturating_mul(std::mem::size_of::<(u32, u32, u32)>() as u64),
            )
    }
}

fn candidate_shard(first: u32, second: u32) -> usize {
    let mixed = u64::from(first)
        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
        .rotate_left(23)
        ^ u64::from(second).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    mixed as usize & (CANDIDATE_SHARDS - 1)
}

fn token_candidate_shard(token: u32) -> usize {
    let mixed = u64::from(token).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    mixed as usize & (CANDIDATE_SHARDS - 1)
}

fn estimate_candidate_counts(
    index: &DirectIndex,
    include_bm25: bool,
    phase: &str,
    progress: &dyn ProgressObserver,
) -> Result<CandidateCounts, DedupError> {
    progress.begin_phase(phase, Some(index.profiles.len() as u64));
    let counts = index
        .profiles
        .par_chunks(PREPARE_BATCH)
        .map(|profiles| {
            progress.check_cancelled()?;
            let mut counts = CandidateCounts::default();
            for profile in profiles {
                let max_document = profile.max_document();
                let max_terms = index.document_terms(max_document);
                if !include_bm25 || max_terms.is_empty() {
                    counts.global_exact = counts.global_exact.saturating_add(1);
                }
                if include_bm25 {
                    counts.global_full = counts.global_full.saturating_add(max_terms.len() as u64);
                }
                if profile.is_evm {
                    for &(token, document) in index.anchors(profile) {
                        if index.token_profile_counts[token as usize] < 2 {
                            continue;
                        }
                        let terms = index.document_terms(document);
                        if !include_bm25 || terms.is_empty() {
                            counts.token_exact = counts.token_exact.saturating_add(1);
                        }
                        if include_bm25 {
                            counts.token_full =
                                counts.token_full.saturating_add(terms.len() as u64);
                        }
                    }
                }
            }
            progress.add_completed(profiles.len() as u64);
            Ok::<_, DedupError>(counts)
        })
        .try_reduce(CandidateCounts::default, |mut left, right| {
            left.add(right);
            Ok(left)
        })?;
    Ok(counts)
}

fn build_global_full_index(
    index: &DirectIndex,
    posting_count: u64,
    progress: &dyn ProgressObserver,
) -> Result<DensePostingIndex, DedupError> {
    let term_count = usize::try_from(index.unique_terms)
        .map_err(|_| DedupError::invalid("metadata", "metadata term count overflow"))?;
    if term_count == 0 || posting_count == 0 {
        return Ok(DensePostingIndex::empty());
    }
    let posting_count = usize::try_from(posting_count)
        .map_err(|_| DedupError::invalid("metadata", "global posting size overflow"))?;
    let average_reuse = posting_count.div_ceil(term_count).max(1);
    let desired_lanes = average_reuse.saturating_mul(2).max(8);
    let lane_count = rayon::current_num_threads()
        .min(index.profiles.len())
        .min(desired_lanes)
        .max(1);
    let cursor_count = term_count
        .checked_mul(lane_count)
        .ok_or_else(|| DedupError::invalid("metadata", "global posting cursor overflow"))?;
    let mut cursors = vec![0_usize; cursor_count];

    progress.begin_phase("candidate_global_count", Some(index.profiles.len() as u64));
    cursors
        .par_chunks_mut(term_count)
        .enumerate()
        .try_for_each(|(lane, counts)| {
            progress.check_cancelled()?;
            let start = index.profiles.len() * lane / lane_count;
            let end = index.profiles.len() * (lane + 1) / lane_count;
            for profile in &index.profiles[start..end] {
                for &(term, _) in index.document_terms(profile.max_document()) {
                    counts[term as usize] = counts[term as usize].saturating_add(1);
                }
            }
            progress.add_completed((end - start) as u64);
            Ok::<_, DedupError>(())
        })?;

    progress.begin_phase(
        "candidate_global_offsets",
        Some(term_count.saturating_mul(2) as u64),
    );
    let mut totals = vec![0_usize; term_count];
    totals
        .par_chunks_mut(PREPARE_BATCH)
        .enumerate()
        .try_for_each(|(chunk, values)| {
            progress.check_cancelled()?;
            let first_term = chunk * PREPARE_BATCH;
            for (offset, total) in values.iter_mut().enumerate() {
                let term = first_term + offset;
                *total = (0..lane_count).fold(0_usize, |total, lane| {
                    total.saturating_add(cursors[lane * term_count + term])
                });
                if *total < 2 {
                    *total = 0;
                }
            }
            progress.add_completed(values.len() as u64);
            Ok::<_, DedupError>(())
        })?;
    let mut offsets = Vec::with_capacity(term_count + 1);
    offsets.push(0_usize);
    for total in totals {
        let next = offsets
            .last()
            .copied()
            .unwrap_or_default()
            .checked_add(total)
            .ok_or_else(|| DedupError::invalid("metadata", "global posting offset overflow"))?;
        offsets.push(next);
    }
    let retained_posting_count = offsets.last().copied().unwrap_or_default();
    debug_assert!(retained_posting_count <= posting_count);
    let cursor_output = SharedCursorOutput(cursors.as_mut_ptr());
    offsets[..term_count]
        .par_chunks(PREPARE_BATCH)
        .enumerate()
        .try_for_each(|(chunk, starts)| {
            progress.check_cancelled()?;
            let first_term = chunk * PREPARE_BATCH;
            for (offset, &posting_start) in starts.iter().enumerate() {
                let term = first_term + offset;
                let mut cursor = posting_start;
                let retained = offsets[term] != offsets[term + 1];
                for lane in 0..lane_count {
                    let position = lane * term_count + term;
                    // Terms are partitioned across tasks, so every cursor slot
                    // has exactly one writer during this layout pass.
                    let count = unsafe { cursor_output.replace(position, cursor) };
                    if retained {
                        cursor += count;
                    }
                }
            }
            progress.add_completed(starts.len() as u64);
            Ok::<_, DedupError>(())
        })?;

    let mut profiles = Vec::<MaybeUninit<u32>>::with_capacity(retained_posting_count);
    // The parallel fill below writes every assigned posting slot once.
    unsafe {
        profiles.set_len(retained_posting_count);
    }
    let output = SharedProfileOutput(profiles.as_mut_ptr());
    progress.begin_phase("candidate_global_fill", Some(index.profiles.len() as u64));
    cursors
        .par_chunks_mut(term_count)
        .enumerate()
        .try_for_each(|(lane, lane_cursors)| {
            progress.check_cancelled()?;
            let start = index.profiles.len() * lane / lane_count;
            let end = index.profiles.len() * (lane + 1) / lane_count;
            for profile_id in start..end {
                let compact_profile = u32::try_from(profile_id)
                    .map_err(|_| DedupError::invalid("metadata", "too many metadata profiles"))?;
                for &(term, _) in index.document_terms(index.profiles[profile_id].max_document()) {
                    if offsets[term as usize] == offsets[term as usize + 1] {
                        continue;
                    }
                    let cursor = &mut lane_cursors[term as usize];
                    unsafe {
                        output.write(*cursor, compact_profile);
                    }
                    *cursor += 1;
                }
            }
            progress.add_completed((end - start) as u64);
            Ok::<_, DedupError>(())
        })?;
    let pointer = profiles.as_mut_ptr().cast::<u32>();
    let len = profiles.len();
    let capacity = profiles.capacity();
    std::mem::forget(profiles);
    // All slots were initialized by disjoint lane/term ranges above.
    let profiles = unsafe { Vec::from_raw_parts(pointer, len, capacity) }.into_boxed_slice();
    hybridize_global_postings(offsets, profiles.into_vec(), progress)
}

fn hybridize_global_postings(
    mut offsets: Vec<usize>,
    mut profiles: Vec<u32>,
    progress: &dyn ProgressObserver,
) -> Result<DensePostingIndex, DedupError> {
    let term_count = offsets.len().saturating_sub(1);
    let logical_len = profiles.len() as u64;
    let mut dense_indices = Vec::new();
    let mut dense_postings = Vec::new();
    let mut dense_words = Vec::new();
    let mut source_start = 0;
    let mut write = 0;
    progress.begin_phase("candidate_global_compress", Some(term_count as u64));
    for term in 0..term_count {
        if term % PREPARE_BATCH == 0 {
            progress.check_cancelled()?;
        }
        let source_end = offsets[term + 1];
        offsets[term] = write;
        if use_dense_posting(&profiles[source_start..source_end]) {
            if dense_indices.is_empty() {
                dense_indices.resize(term_count, NO_DENSE_POSTING);
            }
            dense_indices[term] = dense_postings.len() as u32;
            dense_postings.push(append_dense_posting(
                &profiles[source_start..source_end],
                &mut dense_words,
            ));
        } else if source_start != source_end {
            if write != source_start {
                profiles.copy_within(source_start..source_end, write);
            }
            write += source_end - source_start;
        }
        source_start = source_end;
        if (term + 1) % PREPARE_BATCH == 0 {
            progress.add_completed(PREPARE_BATCH as u64);
        }
    }
    progress.add_completed((term_count % PREPARE_BATCH) as u64);
    offsets[term_count] = write;
    profiles.truncate(write);
    profiles.shrink_to_fit();
    Ok(DensePostingIndex {
        offsets: offsets.into_boxed_slice(),
        profiles: profiles.into_boxed_slice(),
        dense_indices: dense_indices.into_boxed_slice(),
        dense_postings: dense_postings.into_boxed_slice(),
        dense_words: dense_words.into_boxed_slice(),
        logical_len,
    })
}

fn build_candidate_plan(
    index: &DirectIndex,
    threshold: f64,
    exhaustive_pairs: u64,
    progress: &dyn ProgressObserver,
) -> Result<(CrossProfilePlan, CandidatePlanStats), DedupError> {
    if threshold <= 0.0 || exhaustive_pairs == 0 {
        return Ok((CrossProfilePlan::Full, CandidatePlanStats::default()));
    }
    let include_bm25 = !threshold.is_nan() && threshold <= 1.0;
    let counts = estimate_candidate_counts(index, include_bm25, "candidate_admission", progress)?;
    let projected_posting_bytes = counts.posting_bytes(index.unique_terms);
    let mut stats = CandidatePlanStats {
        posting_entries: counts.posting_entries(),
        posting_bytes: projected_posting_bytes,
        full_terms: counts.full_terms(),
        ..CandidatePlanStats::default()
    };
    let prefixes = if include_bm25 {
        let term_ranks = build_term_ranks(index, progress)?;
        build_document_prefixes(index, &term_ranks, threshold, progress)?
    } else {
        DocumentPrefixes::default()
    };
    let global_full = if include_bm25 {
        build_global_full_index(index, counts.global_full, progress)?
    } else {
        DensePostingIndex::empty()
    };
    let sharded_entries = (0..CANDIDATE_SHARDS)
        .map(|_| CandidateEntries::with_approximate_capacity(counts).map(Mutex::new))
        .collect::<Result<Vec<_>, _>>()?
        .into_boxed_slice();
    progress.begin_phase("candidate_build", Some(index.profiles.len() as u64));
    index
        .profiles
        .par_chunks(PREPARE_BATCH)
        .enumerate()
        .try_for_each_init(
            || {
                Box::new(
                    std::array::from_fn::<CandidateEntries, CANDIDATE_SHARDS, _>(|_| {
                        CandidateEntries::default()
                    }),
                )
            },
            |local, (chunk_id, profiles)| {
                progress.check_cancelled()?;
                for (offset, profile) in profiles.iter().enumerate() {
                    let profile_id =
                        u32::try_from(chunk_id * PREPARE_BATCH + offset).map_err(|_| {
                            DedupError::invalid("metadata", "too many metadata profiles")
                        })?;
                    let max_document = profile.max_document();
                    let max_terms = index.document_terms(max_document);
                    if !include_bm25 || max_terms.is_empty() {
                        local[candidate_shard(max_document, 0)]
                            .global_exact
                            .push((max_document, profile_id));
                    }
                    if profile.is_evm {
                        for &(token, document) in index.anchors(profile) {
                            if index.token_profile_counts[token as usize] < 2 {
                                continue;
                            }
                            let terms = index.document_terms(document);
                            if !include_bm25 || terms.is_empty() {
                                local[token_candidate_shard(token)]
                                    .token_exact
                                    .push((token, document, profile_id));
                            }
                            if include_bm25 {
                                for &(term, _) in terms {
                                    local[token_candidate_shard(token)]
                                        .token_full
                                        .push((token, term, profile_id));
                                }
                            }
                        }
                    }
                }
                for (target, entries) in sharded_entries.iter().zip(local.iter_mut()) {
                    if entries.posting_entries() == 0 {
                        continue;
                    }
                    target
                        .lock()
                        .map_err(|_| {
                            DedupError::invalid("metadata", "candidate shard lock poisoned")
                        })?
                        .append_from(entries);
                }
                progress.add_completed(profiles.len() as u64);
                Ok::<(), DedupError>(())
            },
        )?;
    let shards = sharded_entries
        .into_vec()
        .into_iter()
        .map(|entries| {
            entries
                .into_inner()
                .map_err(|_| DedupError::invalid("metadata", "candidate shard lock poisoned"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    debug_assert!(
        shards
            .iter()
            .map(CandidateEntries::posting_entries)
            .fold(global_full.len(), u64::saturating_add)
            <= stats.posting_entries
    );

    let sort_passes = shards.iter().fold(0_u64, |total, entries| {
        let pair_sorts = [entries.global_exact.len()]
            .into_iter()
            .filter(|len| *len > 1)
            .count() as u64;
        let triple_sorts = [entries.token_full.len(), entries.token_exact.len()]
            .into_iter()
            .filter(|len| *len > 1)
            .count() as u64;
        total
            .saturating_add(pair_sorts.saturating_mul(6))
            .saturating_add(triple_sorts.saturating_mul(9))
    });
    progress.begin_phase("candidate_sort", Some(sort_passes));
    let mut weighted_shards = shards
        .into_iter()
        .enumerate()
        .map(|(shard_id, entries)| (shard_id, entries.sort_work(), entries))
        .collect::<Vec<_>>();
    weighted_shards.sort_unstable_by_key(|&(_, work, _)| std::cmp::Reverse(work));
    let mut sorted_shards = weighted_shards
        .into_iter()
        .par_bridge()
        .map(|(shard_id, _, mut entries)| {
            progress.check_cancelled()?;
            let sorted = sort_u32_pairs_while(&mut entries.global_exact, || {
                progress.add_completed(1);
                progress.check_cancelled().is_ok()
            }) && sort_u32_triples_while(&mut entries.token_full, || {
                progress.add_completed(1);
                progress.check_cancelled().is_ok()
            }) && sort_u32_triples_while(&mut entries.token_exact, || {
                progress.add_completed(1);
                progress.check_cancelled().is_ok()
            });
            if !sorted {
                return Err(DedupError::Interrupted);
            }
            Ok((shard_id, entries))
        })
        .collect::<Vec<_>>()
        .into_iter()
        .collect::<Result<Vec<_>, DedupError>>()?;
    sorted_shards.sort_unstable_by_key(|(shard_id, _)| *shard_id);
    let shards = sorted_shards
        .into_iter()
        .map(|(_, entries)| entries)
        .collect::<Vec<_>>();
    let sharded_posting_entries = shards
        .iter()
        .map(CandidateEntries::posting_entries)
        .fold(0_u64, u64::saturating_add);
    progress.begin_phase("candidate_ranges", Some(sharded_posting_entries));
    let mut weighted_shards = shards
        .into_iter()
        .enumerate()
        .map(|(shard_id, entries)| (shard_id, entries.posting_entries(), entries))
        .collect::<Vec<_>>();
    weighted_shards.sort_unstable_by_key(|&(_, work, _)| std::cmp::Reverse(work));
    let mut compact_shards = weighted_shards
        .into_iter()
        .par_bridge()
        .map(|(shard_id, posting_entries, entries)| {
            progress.check_cancelled()?;
            let compact = entries.into_compact();
            progress.add_completed(posting_entries);
            Ok::<_, DedupError>((shard_id, compact))
        })
        .collect::<Vec<_>>()
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    compact_shards.sort_unstable_by_key(|(shard_id, _)| *shard_id);
    let shards = compact_shards
        .into_iter()
        .map(|(_, entries)| entries)
        .collect::<Vec<_>>();
    let token_count = index
        .anchors
        .iter()
        .map(|(token, _)| *token as usize + 1)
        .max()
        .unwrap_or(0);
    let mut token_ranges = vec![TokenPostingRanges::default(); token_count];
    for shard in &shards {
        record_token_key_ranges(&shard.token_full.keys, &mut token_ranges, |ranges| {
            &mut ranges.full
        });
        record_token_key_ranges(&shard.token_exact.keys, &mut token_ranges, |ranges| {
            &mut ranges.exact
        });
    }
    stats.posting_entries = global_full.len().saturating_add(
        shards
            .iter()
            .map(CompactCandidateEntries::posting_entries)
            .fold(0_u64, u64::saturating_add),
    );
    stats.posting_bytes = global_full.posting_bytes().saturating_add(
        shards
            .iter()
            .map(CompactCandidateEntries::posting_bytes)
            .fold(0_u64, u64::saturating_add),
    );
    stats.range_bytes = global_full
        .range_bytes()
        .saturating_add(
            shards
                .iter()
                .map(CompactCandidateEntries::range_bytes)
                .fold(0_u64, u64::saturating_add),
        )
        .saturating_add(
            (token_ranges.len() as u64)
                .saturating_mul(std::mem::size_of::<TokenPostingRanges>() as u64),
        );
    Ok((
        CrossProfilePlan::Indexed(ResidentCandidateIndex {
            shards: shards.into_boxed_slice(),
            token_ranges: token_ranges.into_boxed_slice(),
            global_full,
            prefixes,
            include_bm25,
        }),
        stats,
    ))
}

fn record_token_key_ranges(
    keys: &[u64],
    ranges: &mut [TokenPostingRanges],
    mut select: impl FnMut(&mut TokenPostingRanges) -> &mut PostingKeyRange,
) {
    let mut start = 0;
    while start < keys.len() {
        let token = (keys[start] >> 32) as usize;
        let end = start + keys[start..].partition_point(|key| (*key >> 32) as usize == token);
        let range = select(&mut ranges[token]);
        debug_assert_eq!(range.start, usize::MAX);
        *range = PostingKeyRange { start, end };
        start = end;
    }
}

struct CandidateSeen {
    words: Vec<u64>,
    touched_words: Vec<u32>,
}

impl CandidateSeen {
    fn new(profile_count: usize) -> Self {
        Self {
            words: vec![0; profile_count.div_ceil(u64::BITS as usize)],
            touched_words: Vec::new(),
        }
    }

    fn begin_profile(&mut self, _profile: u32) {
        for word in self.touched_words.drain(..) {
            self.words[word as usize] = 0;
        }
    }

    fn insert(&mut self, profile: u32) -> bool {
        let word_index = profile as usize / u64::BITS as usize;
        let bit = 1_u64 << (profile as usize % u64::BITS as usize);
        let word = &mut self.words[word_index];
        if *word & bit != 0 {
            false
        } else {
            if *word == 0 {
                self.touched_words.push(word_index as u32);
            }
            *word |= bit;
            true
        }
    }

    fn insert_word(&mut self, word_index: u32, candidates: u64) -> u64 {
        if candidates == 0 {
            return 0;
        }
        let word = &mut self.words[word_index as usize];
        let unseen = candidates & !*word;
        if *word == 0 {
            self.touched_words.push(word_index);
        }
        *word |= candidates;
        unseen
    }
}

fn candidate_seen_lanes(query_profile_count: usize) -> usize {
    rayon::current_num_threads().min(query_profile_count).max(1)
}

#[cfg(test)]
struct CandidateGeneration {
    chunks: Vec<Box<[CandidatePair]>>,
    pair_count: usize,
    scoring_work: u64,
    pair_emissions: u64,
    prefix_terms: u64,
    zero_overlap_prunes: u64,
}

#[cfg(test)]
struct CandidateSources<'a> {
    shards: &'a [CompactCandidateEntries],
    token_ranges: &'a [TokenPostingRanges],
    global_full: &'a DensePostingIndex,
    prefixes: &'a DocumentPrefixes,
}

#[cfg(test)]
struct CandidatePairChunks {
    chunks: Vec<Box<[CandidatePair]>>,
    current: Vec<CandidatePair>,
    len: usize,
}

#[cfg(test)]
impl CandidatePairChunks {
    fn new() -> Self {
        Self {
            chunks: Vec::new(),
            current: Vec::with_capacity(CANDIDATE_PAIR_CHUNK),
            len: 0,
        }
    }

    fn push(&mut self, pair: CandidatePair) {
        if self.current.len() == CANDIDATE_PAIR_CHUNK {
            self.chunks
                .push(std::mem::take(&mut self.current).into_boxed_slice());
            self.current = Vec::with_capacity(CANDIDATE_PAIR_CHUNK);
        }
        self.current.push(pair);
        self.len += 1;
    }

    fn finish(mut self) -> (Vec<Box<[CandidatePair]>>, usize) {
        if !self.current.is_empty() {
            self.chunks.push(self.current.into_boxed_slice());
        }
        (self.chunks, self.len)
    }
}

#[cfg(test)]
fn generate_candidate_pairs(
    index: &DirectIndex,
    sources: CandidateSources<'_>,
    include_bm25: bool,
    progress: &dyn ProgressObserver,
) -> Result<CandidateGeneration, DedupError> {
    let profile_count = index.profiles.len();
    let query_profile_count = index.query_profile_count;
    if query_profile_count == 0 || profile_count < 2 {
        return Ok(CandidateGeneration {
            chunks: Vec::new(),
            pair_count: 0,
            scoring_work: 0,
            pair_emissions: 0,
            prefix_terms: 0,
            zero_overlap_prunes: 0,
        });
    }
    let lane_count = candidate_seen_lanes(query_profile_count);
    let next_left = AtomicUsize::new(0);
    progress.begin_phase("candidate_generate", Some(query_profile_count as u64));
    let lanes = (0..lane_count)
        .into_par_iter()
        .map(|_| {
            let mut seen = CandidateSeen::new(profile_count);
            let mut pairs = CandidatePairChunks::new();
            let mut pair_emissions = 0_u64;
            let mut scoring_work = 0_u64;
            let mut prefix_terms = 0_u64;
            let mut zero_overlap_prunes = 0_u64;
            let mut unchecked_emissions = 0_u64;
            let mut completed = 0_u64;
            loop {
                progress.check_cancelled()?;
                let start = next_left.fetch_add(CANDIDATE_SCHEDULING_CHUNK, Ordering::Relaxed);
                if start >= query_profile_count {
                    break;
                }
                let end = start
                    .saturating_add(CANDIDATE_SCHEDULING_CHUNK)
                    .min(query_profile_count);
                for left_id in start..end {
                    let left_profile = &index.profiles[left_id];
                    let left_id = left_id as u32;
                    seen.begin_profile(left_id);
                    let max_document = left_profile.max_document();
                    if !include_bm25 || index.document_terms(max_document).is_empty() {
                        append_owned_candidates(
                            sources.shards[candidate_shard(max_document, 0)]
                                .global_exact_after(max_document, left_id)
                                .iter(),
                            left_id,
                            |profile| profile,
                            |right| {
                                u64::from(left_profile.member_len).saturating_mul(u64::from(
                                    index.profiles[right as usize].member_len,
                                ))
                            },
                            |right| prepare_candidate_pair(index, include_bm25, left_id, right),
                            &mut seen,
                            &mut pairs,
                            &mut scoring_work,
                            &mut pair_emissions,
                            &mut zero_overlap_prunes,
                            &mut unchecked_emissions,
                            progress,
                        )?;
                    }
                    if include_bm25 {
                        let prefix = sources.prefixes.get(max_document);
                        prefix_terms = prefix_terms.saturating_add(prefix.len() as u64);
                        for &term in prefix {
                            append_owned_candidates(
                                sources.global_full.posting_after(term, left_id).iter(),
                                left_id,
                                |profile| profile,
                                |right| {
                                    u64::from(left_profile.member_len).saturating_mul(u64::from(
                                        index.profiles[right as usize].member_len,
                                    ))
                                },
                                |right| prepare_candidate_pair(index, include_bm25, left_id, right),
                                &mut seen,
                                &mut pairs,
                                &mut scoring_work,
                                &mut pair_emissions,
                                &mut zero_overlap_prunes,
                                &mut unchecked_emissions,
                                progress,
                            )?;
                        }
                    }
                    if left_profile.is_evm {
                        for &(token, document) in index.anchors(left_profile) {
                            if index.token_profile_counts[token as usize] < 2 {
                                continue;
                            }
                            let has_terms = !index.document_terms(document).is_empty();
                            let token_shard = &sources.shards[token_candidate_shard(token)];
                            let ranges = sources.token_ranges[token as usize];
                            if !include_bm25 || !has_terms {
                                let token_exact = token_shard.token_exact(token, ranges.exact);
                                append_owned_candidates(
                                    token_exact.posting_after(document, left_id).iter(),
                                    left_id,
                                    |profile| profile,
                                    |right| {
                                        u64::from(left_profile.member_len).saturating_mul(
                                            u64::from(index.profiles[right as usize].member_len),
                                        )
                                    },
                                    |right| {
                                        prepare_candidate_pair(index, include_bm25, left_id, right)
                                    },
                                    &mut seen,
                                    &mut pairs,
                                    &mut scoring_work,
                                    &mut pair_emissions,
                                    &mut zero_overlap_prunes,
                                    &mut unchecked_emissions,
                                    progress,
                                )?;
                            }
                            if include_bm25 && has_terms {
                                let prefix = sources.prefixes.get(document);
                                prefix_terms = prefix_terms.saturating_add(prefix.len() as u64);
                                let token_full = token_shard.token_full(token, ranges.full);
                                let mut result = Ok(());
                                token_full.visit_postings_after(prefix, left_id, |posting| {
                                    if result.is_err() {
                                        return;
                                    }
                                    result = append_owned_candidates(
                                        posting.iter(),
                                        left_id,
                                        |profile| profile,
                                        |right| {
                                            u64::from(left_profile.member_len).saturating_mul(
                                                u64::from(
                                                    index.profiles[right as usize].member_len,
                                                ),
                                            )
                                        },
                                        |right| {
                                            prepare_candidate_pair(
                                                index,
                                                include_bm25,
                                                left_id,
                                                right,
                                            )
                                        },
                                        &mut seen,
                                        &mut pairs,
                                        &mut scoring_work,
                                        &mut pair_emissions,
                                        &mut zero_overlap_prunes,
                                        &mut unchecked_emissions,
                                        progress,
                                    );
                                });
                                result?;
                            }
                        }
                    }
                    completed += 1;
                    if completed >= 64 {
                        progress.add_completed(completed);
                        completed = 0;
                    }
                }
            }
            progress.add_completed(completed);
            let (chunks, pair_count) = pairs.finish();
            Ok::<_, DedupError>(CandidateGeneration {
                chunks,
                pair_count,
                scoring_work,
                pair_emissions,
                prefix_terms,
                zero_overlap_prunes,
            })
        })
        .collect::<Vec<_>>()
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    let pair_count = lanes.iter().map(|lane| lane.pair_count).sum::<usize>();
    let pair_emissions = lanes.iter().fold(0_u64, |total, lane| {
        total.saturating_add(lane.pair_emissions)
    });
    let scoring_work = lanes
        .iter()
        .fold(0_u64, |total, lane| total.saturating_add(lane.scoring_work));
    let prefix_terms = lanes
        .iter()
        .fold(0_u64, |total, lane| total.saturating_add(lane.prefix_terms));
    let zero_overlap_prunes = lanes.iter().fold(0_u64, |total, lane| {
        total.saturating_add(lane.zero_overlap_prunes)
    });
    let chunks = lanes
        .into_iter()
        .flat_map(|lane| lane.chunks)
        .collect::<Vec<_>>();
    Ok(CandidateGeneration {
        chunks,
        pair_count,
        scoring_work,
        pair_emissions,
        prefix_terms,
        zero_overlap_prunes,
    })
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
fn append_owned_candidates<T: Copy>(
    posting: impl IntoIterator<Item = T>,
    left: u32,
    profile: impl Fn(T) -> u32,
    candidate_work: impl Fn(u32) -> u64,
    prepare: impl Fn(u32) -> Option<CandidatePair>,
    seen: &mut CandidateSeen,
    pairs: &mut CandidatePairChunks,
    scoring_work: &mut u64,
    pair_emissions: &mut u64,
    zero_overlap_prunes: &mut u64,
    unchecked_emissions: &mut u64,
    progress: &dyn ProgressObserver,
) -> Result<(), DedupError> {
    for entry in posting {
        let right = profile(entry);
        debug_assert!(right > left);
        *pair_emissions = pair_emissions.saturating_add(1);
        *unchecked_emissions += 1;
        if *unchecked_emissions >= CANDIDATE_CANCEL_BATCH {
            progress.check_cancelled()?;
            *unchecked_emissions = 0;
        }
        if seen.insert(right) {
            if let Some(candidate) = prepare(right) {
                pairs.push(candidate);
                *scoring_work = scoring_work.saturating_add(candidate_work(right));
            } else {
                *zero_overlap_prunes = zero_overlap_prunes.saturating_add(1);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
fn prepare_candidate_pair(
    index: &DirectIndex,
    include_bm25: bool,
    left: u32,
    right: u32,
) -> Option<CandidatePair> {
    let left_profile = &index.profiles[left as usize];
    let right_profile = &index.profiles[right as usize];
    let (left_document, right_document) = selected_documents(
        left_profile,
        index.anchors(left_profile),
        right_profile,
        index.anchors(right_profile),
    );
    (left_document == right_document
        || (include_bm25
            && may_share_term(
                &index.documents[left_document as usize],
                index.document_terms(left_document),
                &index.documents[right_document as usize],
                index.document_terms(right_document),
            )))
    .then(|| CandidatePair::new(left, right, left_document, right_document))
}

fn build_term_ranks(
    index: &DirectIndex,
    progress: &dyn ProgressObserver,
) -> Result<Vec<u32>, DedupError> {
    let term_count = usize::try_from(index.unique_terms)
        .map_err(|_| DedupError::invalid("metadata", "metadata term count overflow"))?;
    if term_count == 0 {
        return Ok(Vec::new());
    }
    let average_reuse = index.terms.len().div_ceil(term_count).max(1);
    let desired_lanes = average_reuse.saturating_mul(2).max(8);
    let lane_count = rayon::current_num_threads()
        .min(index.documents.len())
        .min(desired_lanes)
        .max(1);

    progress.begin_phase("candidate_term_rank", Some(index.terms.len() as u64));
    let mut frequency_lanes = (0..lane_count)
        .into_par_iter()
        .map(|lane| {
            let start = index.documents.len() * lane / lane_count;
            let end = index.documents.len() * (lane + 1) / lane_count;
            let mut frequencies = vec![0_u32; term_count];
            let mut completed = 0_u64;
            for document in start..end {
                let document_id = u32::try_from(document).map_err(|_| {
                    DedupError::invalid("metadata", "metadata document count overflow")
                })?;
                let terms = index.document_terms(document_id);
                let weight = index.document_context_weights[document];
                for &(term, _) in terms {
                    let frequency = &mut frequencies[term as usize];
                    *frequency = frequency.saturating_add(weight);
                }
                completed = completed.saturating_add(terms.len() as u64);
                if completed >= CANDIDATE_CANCEL_BATCH {
                    progress.add_completed(completed);
                    progress.check_cancelled()?;
                    completed = 0;
                }
            }
            progress.add_completed(completed);
            Ok::<_, DedupError>(frequencies)
        })
        .collect::<Vec<_>>()
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    let reduction_work = (lane_count - 1).saturating_mul(term_count) as u64;
    progress.begin_phase("candidate_term_reduce", Some(reduction_work));
    while frequency_lanes.len() > 1 {
        let mut pairs = Vec::with_capacity(frequency_lanes.len().div_ceil(2));
        let mut lanes = frequency_lanes.into_iter();
        while let Some(left) = lanes.next() {
            pairs.push((left, lanes.next()));
        }
        frequency_lanes = pairs
            .into_par_iter()
            .map(|(mut left, right)| {
                progress.check_cancelled()?;
                if let Some(right) = right {
                    for (target, value) in left.iter_mut().zip(right) {
                        *target = target.saturating_add(value);
                    }
                    progress.add_completed(term_count as u64);
                }
                Ok::<_, DedupError>(left)
            })
            .collect::<Vec<_>>()
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
    }
    let frequencies = frequency_lanes
        .pop()
        .expect("at least one metadata term-frequency lane exists");
    let mut ordered = frequencies
        .into_iter()
        .enumerate()
        .map(|(term, frequency)| (frequency, term as u32))
        .collect::<Vec<_>>();
    let rank_sort_passes = if ordered.len() > 1 { 6 } else { 0 };
    progress.begin_phase("candidate_term_order", Some(rank_sort_passes));
    if !sort_u32_pairs_while(&mut ordered, || {
        progress.add_completed(1);
        progress.check_cancelled().is_ok()
    }) {
        return Err(DedupError::Interrupted);
    }
    let mut ranks = vec![0_u32; term_count];
    for (rank, &(_, term)) in ordered.iter().enumerate() {
        ranks[term as usize] = rank as u32;
    }
    Ok(ranks)
}

fn build_document_prefixes(
    index: &DirectIndex,
    term_ranks: &[u32],
    threshold: f64,
    progress: &dyn ProgressObserver,
) -> Result<DocumentPrefixes, DedupError> {
    struct PrefixChunk {
        offsets: Vec<u32>,
        terms: Vec<u32>,
    }

    progress.begin_phase("candidate_prefixes", Some(index.documents.len() as u64));
    let chunks =
        index
            .documents
            .par_chunks(PREPARE_BATCH)
            .enumerate()
            .map_init(
                || (Vec::new(), Vec::new()),
                |(ranked, frequencies), (chunk_id, documents)| {
                    progress.check_cancelled()?;
                    let mut offsets = Vec::with_capacity(documents.len() + 1);
                    let mut terms = Vec::with_capacity(documents.len());
                    offsets.push(0_u32);
                    for (offset, _) in documents.iter().enumerate() {
                        let document = (chunk_id * PREPARE_BATCH + offset) as DocumentId;
                        ranked.clear();
                        ranked.extend(index.document_terms(document).iter().map(
                            |(term, frequency)| (term_ranks[*term as usize], *term, *frequency),
                        ));
                        ranked.sort_unstable_by_key(|(rank, _, _)| *rank);
                        frequencies.clear();
                        frequencies.extend(ranked.iter().map(|(_, _, frequency)| *frequency));
                        let len = lossless_prefix_len(frequencies, threshold);
                        ranked[..len].sort_unstable_by_key(|(_, term, _)| *term);
                        terms.extend(ranked[..len].iter().map(|(_, term, _)| *term));
                        offsets.push(u32::try_from(terms.len()).map_err(|_| {
                            DedupError::invalid("metadata", "metadata prefix term offset overflow")
                        })?);
                    }
                    progress.add_completed(documents.len() as u64);
                    Ok::<PrefixChunk, DedupError>(PrefixChunk { offsets, terms })
                },
            )
            .collect::<Vec<_>>()
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
    let mut chunk_term_starts = Vec::with_capacity(chunks.len() + 1);
    chunk_term_starts.push(0_usize);
    for chunk in &chunks {
        let start = chunk_term_starts.last().copied().unwrap_or_default();
        chunk_term_starts.push(start.checked_add(chunk.terms.len()).ok_or_else(|| {
            DedupError::invalid("metadata", "metadata prefix term offset overflow")
        })?);
    }
    let prefix_term_count = chunk_term_starts.last().copied().unwrap_or_default();
    u32::try_from(prefix_term_count)
        .map_err(|_| DedupError::invalid("metadata", "metadata prefix term offset overflow"))?;

    let mut offsets = vec![0_u32; index.documents.len() + 1];
    let offset_chunk_count = offsets[1..].chunks(PREPARE_BATCH).len();
    if offset_chunk_count != chunks.len() || chunks.len() + 1 != chunk_term_starts.len() {
        return Err(DedupError::invalid(
            "metadata",
            "metadata prefix chunk layout mismatch",
        ));
    }
    let mut terms = Vec::<MaybeUninit<u32>>::with_capacity(prefix_term_count);
    // The parallel fill below writes every prefix-term slot exactly once.
    unsafe {
        terms.set_len(prefix_term_count);
    }
    let term_output = SharedProfileOutput(terms.as_mut_ptr());
    progress.begin_phase(
        "candidate_prefix_flatten",
        Some(index.documents.len() as u64),
    );
    offsets[1..]
        .par_chunks_mut(PREPARE_BATCH)
        .zip(chunks.par_iter())
        .zip(chunk_term_starts[..chunks.len()].par_iter())
        .try_for_each(|((target_offsets, chunk), &term_start)| {
            progress.check_cancelled()?;
            debug_assert_eq!(target_offsets.len() + 1, chunk.offsets.len());
            let term_start = u32::try_from(term_start).map_err(|_| {
                DedupError::invalid("metadata", "metadata prefix term offset overflow")
            })?;
            for (target, &local) in target_offsets.iter_mut().zip(&chunk.offsets[1..]) {
                *target = term_start.checked_add(local).ok_or_else(|| {
                    DedupError::invalid("metadata", "metadata prefix term offset overflow")
                })?;
            }
            for (offset, &term) in chunk.terms.iter().enumerate() {
                // Chunks own disjoint ranges derived from `chunk_term_starts`.
                unsafe {
                    term_output.write(term_start as usize + offset, term);
                }
            }
            progress.add_completed(target_offsets.len() as u64);
            Ok::<_, DedupError>(())
        })?;
    let pointer = terms.as_mut_ptr().cast::<u32>();
    let len = terms.len();
    let capacity = terms.capacity();
    std::mem::forget(terms);
    // Every slot was initialized by one disjoint prefix chunk above.
    let terms = unsafe { Vec::from_raw_parts(pointer, len, capacity) }.into_boxed_slice();
    Ok(DocumentPrefixes {
        offsets: offsets.into_boxed_slice(),
        terms,
    })
}

fn build_profile_bucket(
    bucket: Vec<RawProfile>,
    progress: &dyn ProgressObserver,
) -> Result<CompactProfiles, DedupError> {
    progress.check_cancelled()?;
    let mut profile_ids: AHashMap<ProfileKey, usize> = AHashMap::new();
    let mut builders: Vec<(ProfileMembers, ProfileChainCounts)> = Vec::new();
    let mut completed = 0_u64;
    for raw in bucket {
        let key = raw.key;
        let profile_id = if let Some(&id) = profile_ids.get(&key) {
            id
        } else {
            let id = builders.len();
            builders.push((
                ProfileMembers::new(raw.member),
                ProfileChainCounts::new(raw.chain_id),
            ));
            profile_ids.insert(key, id);
            id
        };
        let (members, chain_counts) = &mut builders[profile_id];
        if members.first != raw.member {
            members.push(raw.member);
            chain_counts.add(raw.chain_id);
        }
        completed += 1;
        if completed == PREPARE_BATCH as u64 {
            progress.add_completed(completed - 1);
            progress.check_cancelled()?;
            completed = 1;
        }
    }
    let mut keys = (0..builders.len()).map(|_| None).collect::<Vec<_>>();
    for (key, id) in profile_ids {
        keys[id] = Some(key);
    }
    let profiles = builders
        .into_iter()
        .zip(keys)
        .map(|((members, chain_counts), key)| {
            let key = key.expect("every profile builder has one key");
            UnpackedProfile {
                is_evm: key.is_evm,
                is_solana: key.is_solana,
                anchors: key.anchors.into_boxed_slice(),
                members,
                chain_counts,
            }
        })
        .collect();
    let compact = compact_profiles(profiles)?;
    progress.add_completed(completed);
    Ok(compact)
}

fn build_solana_profile_bucket(
    bucket: Vec<RawSolanaProfile>,
    progress: &dyn ProgressObserver,
) -> Result<CompactProfiles, DedupError> {
    progress.check_cancelled()?;
    let mut profile_ids: AHashMap<DocumentId, usize> = AHashMap::new();
    let mut builders: Vec<(DocumentId, ProfileMembers, ProfileChainCounts)> = Vec::new();
    let mut completed = 0_u64;
    for raw in bucket {
        let profile_id = if let Some(&id) = profile_ids.get(&raw.document) {
            id
        } else {
            let id = builders.len();
            builders.push((
                raw.document,
                ProfileMembers::new(raw.member),
                ProfileChainCounts::new(raw.chain_id),
            ));
            profile_ids.insert(raw.document, id);
            id
        };
        let (_, members, chain_counts) = &mut builders[profile_id];
        if members.first != raw.member {
            members.push(raw.member);
            chain_counts.add(raw.chain_id);
        }
        completed += 1;
        if completed == PREPARE_BATCH as u64 {
            progress.add_completed(completed - 1);
            progress.check_cancelled()?;
            completed = 1;
        }
    }
    let compact = compact_solana_profiles(builders)?;
    progress.add_completed(completed);
    Ok(compact)
}

type CompactProfiles = (
    Vec<ContractProfile>,
    Vec<(TokenKeyId, DocumentId)>,
    Vec<MetadataMember>,
    Vec<(ChainId, u32)>,
);

fn compact_profiles(unpacked: Vec<UnpackedProfile>) -> Result<CompactProfiles, DedupError> {
    let anchor_capacity = unpacked.iter().map(|profile| profile.anchors.len()).sum();
    let member_capacity = unpacked.iter().map(|profile| profile.members.len()).sum();
    let chain_capacity = unpacked
        .iter()
        .map(|profile| profile.chain_counts.iter().count())
        .sum();
    let mut profiles = Vec::with_capacity(unpacked.len());
    let mut anchors = Vec::with_capacity(anchor_capacity);
    let mut members = Vec::with_capacity(member_capacity);
    let mut chain_counts = Vec::with_capacity(chain_capacity);
    for profile in unpacked {
        let anchor_start = u32::try_from(anchors.len())
            .map_err(|_| DedupError::invalid("metadata", "metadata anchor offset overflow"))?;
        let anchor_len = u32::try_from(profile.anchors.len())
            .map_err(|_| DedupError::invalid("metadata", "too many metadata anchors"))?;
        let max_document = profile
            .anchors
            .last()
            .expect("profiles always have an anchor")
            .1;
        let token_mask =
            profile
                .anchors
                .iter()
                .fold([0_u64; TOKEN_MASK_WORDS], |mut mask, (token, _)| {
                    let (word, bit) = token_bit(*token);
                    mask[word] |= bit;
                    mask
                });
        anchors.extend(profile.anchors.iter().copied());
        let member_start = u32::try_from(members.len())
            .map_err(|_| DedupError::invalid("metadata", "metadata member offset overflow"))?;
        let member_len = u32::try_from(profile.members.len())
            .map_err(|_| DedupError::invalid("metadata", "metadata profile too large"))?;
        members.extend(profile.members.iter());
        let chain_start = u32::try_from(chain_counts.len())
            .map_err(|_| DedupError::invalid("metadata", "metadata chain offset overflow"))?;
        let chain_len = u16::try_from(profile.chain_counts.iter().count())
            .map_err(|_| DedupError::invalid("metadata", "too many chains in metadata profile"))?;
        let chain_mask = profile.chain_counts.iter().fold(0_u64, |mask, (chain, _)| {
            let chain = usize::from(chain);
            if chain < 64 {
                mask | (1_u64 << chain)
            } else {
                mask
            }
        });
        chain_counts.extend(profile.chain_counts.iter());
        profiles.push(ContractProfile {
            is_evm: profile.is_evm,
            is_solana: profile.is_solana,
            has_empty_token_document: false,
            anchor_start,
            anchor_len,
            max_document,
            token_mask,
            chain_mask,
            member_start,
            member_len,
            chain_start,
            chain_len,
        });
    }
    Ok((profiles, anchors, members, chain_counts))
}

fn compact_solana_profiles(
    unpacked: Vec<(DocumentId, ProfileMembers, ProfileChainCounts)>,
) -> Result<CompactProfiles, DedupError> {
    let member_capacity = unpacked.iter().map(|(_, members, _)| members.len()).sum();
    let chain_capacity = unpacked
        .iter()
        .map(|(_, _, chain_counts)| chain_counts.iter().count())
        .sum();
    let mut profiles = Vec::with_capacity(unpacked.len());
    let mut anchors = Vec::with_capacity(unpacked.len());
    let mut members = Vec::with_capacity(member_capacity);
    let mut chain_counts = Vec::with_capacity(chain_capacity);
    let (token_word, token_bit) = token_bit(0);
    for (document, profile_members, profile_chain_counts) in unpacked {
        let anchor_start = u32::try_from(anchors.len())
            .map_err(|_| DedupError::invalid("metadata", "metadata anchor offset overflow"))?;
        anchors.push((0, document));
        let member_start = u32::try_from(members.len())
            .map_err(|_| DedupError::invalid("metadata", "metadata member offset overflow"))?;
        let member_len = u32::try_from(profile_members.len())
            .map_err(|_| DedupError::invalid("metadata", "metadata profile too large"))?;
        members.extend(profile_members.iter());
        let chain_start = u32::try_from(chain_counts.len())
            .map_err(|_| DedupError::invalid("metadata", "metadata chain offset overflow"))?;
        let chain_len = u16::try_from(profile_chain_counts.iter().count())
            .map_err(|_| DedupError::invalid("metadata", "too many chains in metadata profile"))?;
        let chain_mask = profile_chain_counts.iter().fold(0_u64, |mask, (chain, _)| {
            let chain = usize::from(chain);
            if chain < 64 {
                mask | (1_u64 << chain)
            } else {
                mask
            }
        });
        chain_counts.extend(profile_chain_counts.iter());
        let mut token_mask = [0_u64; TOKEN_MASK_WORDS];
        token_mask[token_word] = token_bit;
        profiles.push(ContractProfile {
            is_evm: false,
            is_solana: true,
            has_empty_token_document: false,
            anchor_start,
            anchor_len: 1,
            max_document: document,
            token_mask,
            chain_mask,
            member_start,
            member_len,
            chain_start,
            chain_len,
        });
    }
    Ok((profiles, anchors, members, chain_counts))
}

fn score_equivalent_profiles(
    index: &DirectIndex,
    hits: &ProfileHits,
    stats: &AtomicStats,
    progress: &dyn ProgressObserver,
    sampling: PairSamplingPlan,
    image_sample_size: usize,
) -> Result<MetadataSamplingResult, DedupError> {
    let cancelled = AtomicBool::new(false);
    let samples = index
        .profiles
        .par_chunks(PREPARE_BATCH)
        .enumerate()
        .fold(
            || MetadataSamplingResult {
                pairs: sampling.sampler(),
                images: MetadataImageSampler::new(image_sample_size),
            },
            |mut samples, (chunk_id, profiles)| {
                if progress.check_cancelled().is_err() {
                    cancelled.store(true, Ordering::Relaxed);
                    return samples;
                }
                let mut completed = 0_u64;
                let mut equivalent = 0_u64;
                for (offset, profile) in profiles.iter().enumerate() {
                    if profile.member_len < 2 {
                        continue;
                    }
                    let profile_id = chunk_id * PREPARE_BATCH + offset;
                    for &(chain, count) in index.chains(profile) {
                        if count > 1 {
                            hits.insert(profile_id, chain);
                        }
                    }
                    if samples.pairs.enabled() {
                        let members = index.members(profile);
                        samples.pairs.observe_clique_by(
                            members.len(),
                            |member| members[member].contract_id,
                            0x4d45_5441_4551_0000 ^ profile_id as u64,
                        );
                    }
                    if samples.images.enabled()
                        && let Some((left_anchor, _)) = selected_image_anchors(
                            profile,
                            index.anchors(profile),
                            profile,
                            index.anchors(profile),
                        )
                    {
                        let members = index.image_members(profile, left_anchor);
                        samples
                            .images
                            .observe_clique(&members, 0x494d_4745_4551_0000 ^ profile_id as u64);
                    }
                    equivalent += 1;
                    completed = completed.saturating_add(choose_two(u64::from(profile.member_len)));
                }
                stats
                    .exact_document_pairs
                    .fetch_add(equivalent, Ordering::Relaxed);
                stats
                    .matched_profile_pairs
                    .fetch_add(equivalent, Ordering::Relaxed);
                progress.add_completed(completed);
                samples
            },
        )
        .reduce(
            || MetadataSamplingResult {
                pairs: sampling.sampler(),
                images: MetadataImageSampler::new(image_sample_size),
            },
            |mut left, right| {
                left.pairs.merge(right.pairs);
                left.images.merge(right.images);
                left
            },
        );
    if cancelled.load(Ordering::Relaxed) {
        Err(DedupError::Interrupted)
    } else {
        Ok(samples)
    }
}

struct IndexedLeftContext<'a> {
    profile_id: u32,
    profile: &'a ContractProfile,
    anchors: &'a [(TokenKeyId, DocumentId)],
    max_document: DocumentId,
    max_prepared_document: &'a PreparedDocument,
    max_terms: &'a [(u32, u32)],
    known_hit_mask: Option<u64>,
}

impl<'a> IndexedLeftContext<'a> {
    fn new(index: &'a DirectIndex, hits: &ProfileHits, profile_id: u32) -> Self {
        let profile = &index.profiles[profile_id as usize];
        let max_document = profile.max_document();
        let max_prepared_document = &index.documents[max_document as usize];
        Self {
            profile_id,
            profile,
            anchors: if profile.is_evm {
                index.anchors(profile)
            } else {
                &[]
            },
            max_document,
            max_prepared_document,
            max_terms: max_prepared_document.terms(&index.terms),
            known_hit_mask: hits.profile_mask(profile_id as usize),
        }
    }
}

#[derive(Clone, Copy)]
struct PreparedIndexedDocuments<'a> {
    documents: (DocumentId, DocumentId),
    left: Option<(&'a PreparedDocument, &'a [(u32, u32)])>,
}

struct WorkerScorer<'a> {
    index: &'a DirectIndex,
    hits: &'a ProfileHits,
    threshold: f64,
    single_chain_word: bool,
    local_stats: LocalStats,
    samples: PairSampler,
    image_samples: MetadataImageSampler,
}

impl<'a> WorkerScorer<'a> {
    #[cfg(test)]
    fn new(index: &'a DirectIndex, hits: &'a ProfileHits, threshold: f64) -> Self {
        Self::new_with_sampling(index, hits, threshold, PairSamplingPlan::disabled(), 0)
    }

    fn new_with_sampling(
        index: &'a DirectIndex,
        hits: &'a ProfileHits,
        threshold: f64,
        sampling: PairSamplingPlan,
        image_sample_size: usize,
    ) -> Self {
        Self {
            index,
            hits,
            threshold,
            single_chain_word: hits.is_single_word(),
            local_stats: LocalStats::default(),
            samples: sampling.sampler(),
            image_samples: MetadataImageSampler::new(image_sample_size),
        }
    }

    fn score_pair(&mut self, left_id: usize, right_id: usize) -> u64 {
        let index = self.index;
        let left = &index.profiles[left_id];
        let right = &index.profiles[right_id];
        let completed = u64::from(left.member_len).saturating_mul(u64::from(right.member_len));
        self.score_pair_inner(left_id, left, right_id, right, None, None);
        completed
    }

    fn score_indexed_candidate(
        &mut self,
        left: &mut IndexedLeftContext<'a>,
        right_id: u32,
        _include_bm25: bool,
    ) -> bool {
        let index = self.index;
        let right = &index.profiles[right_id as usize];
        let both_evm = left.profile.is_evm && right.is_evm;
        let (left_document, right_document) = if both_evm {
            selected_evm_documents(left.profile, left.anchors, right, index.anchors(right))
        } else {
            (left.max_document, right.max_document())
        };
        let prepared_left = (left_document == left.max_document)
            .then_some((left.max_prepared_document, left.max_terms));
        let prepared_documents = PreparedIndexedDocuments {
            documents: (left_document, right_document),
            left: prepared_left,
        };
        self.score_pair_inner(
            left.profile_id as usize,
            left.profile,
            right_id as usize,
            right,
            Some(prepared_documents),
            left.known_hit_mask.as_mut(),
        );
        true
    }

    fn pair_is_saturated(
        &self,
        left_id: usize,
        left: &ContractProfile,
        right_id: usize,
        right: &ContractProfile,
        known_left_hit_mask: Option<u64>,
    ) -> bool {
        if self.single_chain_word {
            let left_contains = known_left_hit_mask.map_or_else(
                || self.hits.contains_mask(left_id, right.chain_mask),
                |known| known & right.chain_mask == right.chain_mask,
            );
            left_contains && self.hits.contains_mask(right_id, left.chain_mask)
        } else {
            self.hits
                .contains_profile_chains(left_id, right, self.index.chains(right))
                && self
                    .hits
                    .contains_profile_chains(right_id, left, self.index.chains(left))
        }
    }

    fn score_pair_inner(
        &mut self,
        left_id: usize,
        left: &ContractProfile,
        right_id: usize,
        right: &ContractProfile,
        prepared_documents: Option<PreparedIndexedDocuments<'_>>,
        known_left_hit_mask: Option<&mut u64>,
    ) {
        if prepared_documents.is_none()
            && self.pair_is_saturated(
                left_id,
                left,
                right_id,
                right,
                known_left_hit_mask.as_deref().copied(),
            )
        {
            self.local_stats.saturated_profile_pairs += 1;
            debug_assert!(self.threshold <= 0.0);
            self.observe_profile_pair(left_id, left, right_id, right);
            return;
        }
        let overlap_filter_passed = prepared_documents.is_some();
        let (left_document, right_document, prepared_left) =
            if let Some(prepared) = prepared_documents {
                (prepared.documents.0, prepared.documents.1, prepared.left)
            } else {
                let (left_document, right_document) = selected_documents(
                    left,
                    self.index.anchors(left),
                    right,
                    self.index.anchors(right),
                );
                (left_document, right_document, None)
            };
        let matched = if left_document == right_document {
            self.local_stats.exact_document_pairs += 1;
            true
        } else {
            self.local_stats.bm25_cache_bypassed_pairs += 1;
            self.score_document_pair(
                left_document,
                right_document,
                prepared_left,
                overlap_filter_passed,
            )
        };
        if matched {
            self.local_stats.matched_profile_pairs += 1;
            if self.single_chain_word {
                if let Some(known) = known_left_hit_mask {
                    let missing = right.chain_mask & !*known;
                    if missing != 0 {
                        self.hits.insert_mask(left_id, missing);
                    }
                    *known |= right.chain_mask;
                } else {
                    self.hits.insert_mask_if_missing(left_id, right.chain_mask);
                }
                self.hits.insert_mask_if_missing(right_id, left.chain_mask);
            } else {
                self.hits
                    .insert_profile_chains(left_id, right, self.index.chains(right));
                self.hits
                    .insert_profile_chains(right_id, left, self.index.chains(left));
            }
            // Publish hit masks before sampling so other workers can use the
            // saturation fast path without waiting for random pair selection.
            self.observe_profile_pair(left_id, left, right_id, right);
        }
    }

    fn observe_profile_pair(
        &mut self,
        left_id: usize,
        left: &ContractProfile,
        right_id: usize,
        right: &ContractProfile,
    ) {
        if !self.samples.enabled() && !self.image_samples.enabled() {
            return;
        }
        let left_members = self.index.members(left);
        let right_members = self.index.members(right);
        if self.samples.enabled() {
            self.samples.observe_cross_by(
                left_members.len(),
                right_members.len(),
                |member| left_members[member].contract_id,
                |member| right_members[member].contract_id,
                0x4d45_5441_4352_0000 ^ ((left_id as u64) << 32) ^ right_id as u64,
            );
        }
        if self.image_samples.enabled()
            && let Some((left_anchor, right_anchor)) = selected_image_anchors(
                left,
                self.index.anchors(left),
                right,
                self.index.anchors(right),
            )
        {
            let left_images = self.index.image_members(left, left_anchor);
            let right_images = self.index.image_members(right, right_anchor);
            self.image_samples.observe_cross(
                &left_images,
                &right_images,
                0x494d_4745_4352_0000 ^ ((left_id as u64) << 32) ^ right_id as u64,
            );
        }
    }

    #[inline]
    fn score_document_pair(
        &mut self,
        left: DocumentId,
        right: DocumentId,
        prepared_left: Option<(&PreparedDocument, &[(u32, u32)])>,
        overlap_filter_passed: bool,
    ) -> bool {
        self.local_stats.bm25_scores += 1;
        let (left_document, left_terms) = prepared_left.unwrap_or_else(|| {
            let document = &self.index.documents[left as usize];
            (document, document.terms(&self.index.terms))
        });
        let right_document = &self.index.documents[right as usize];
        let right_terms = right_document.terms(&self.index.terms);
        let decision = if overlap_filter_passed {
            similarity_at_least_after_overlap_filter(
                left_document,
                left_terms,
                right_document,
                right_terms,
                self.threshold,
            )
        } else {
            similarity_at_least(
                left_document,
                left_terms,
                right_document,
                right_terms,
                self.threshold,
            )
        };
        if decision.zero_overlap_pruned {
            self.local_stats.bm25_zero_overlap_prunes += 1;
        }
        match decision.upper_bound_prune {
            UpperBoundPrune::None => {}
            UpperBoundPrune::Initial => {
                self.local_stats.bm25_upper_bound_prunes += 1;
                self.local_stats.bm25_initial_upper_bound_prunes += 1;
            }
            UpperBoundPrune::Iterative => {
                self.local_stats.bm25_upper_bound_prunes += 1;
                self.local_stats.bm25_iterative_upper_bound_prunes += 1;
            }
        }
        decision.matched
    }

    fn finish(self, stats: &AtomicStats) -> MetadataSamplingResult {
        self.local_stats.flush(stats);
        MetadataSamplingResult {
            pairs: self.samples,
            images: self.image_samples,
        }
    }
}

struct ScoreBlockInfo {
    start: usize,
    end: usize,
    chain_mask: u64,
    member_sum: u64,
    equivalent_member_pairs: u64,
}

impl ScoreBlockInfo {
    fn profile_count(&self) -> u64 {
        (self.end - self.start) as u64
    }
}

fn build_score_blocks(index: &DirectIndex) -> Vec<ScoreBlockInfo> {
    index
        .profiles
        .chunks(SATURATION_BLOCK)
        .enumerate()
        .map(|(block, profiles)| {
            let start = block * SATURATION_BLOCK;
            ScoreBlockInfo {
                start,
                end: start + profiles.len(),
                chain_mask: profiles
                    .iter()
                    .fold(0_u64, |mask, profile| mask | profile.chain_mask),
                member_sum: profiles.iter().fold(0_u64, |total, profile| {
                    total.saturating_add(u64::from(profile.member_len))
                }),
                equivalent_member_pairs: profiles.iter().fold(0_u64, |total, profile| {
                    total.saturating_add(choose_two(u64::from(profile.member_len)))
                }),
            }
        })
        .collect()
}

#[derive(Clone, Copy)]
struct ScoreTileInfo {
    block_start: usize,
    block_end: usize,
}

fn build_score_tiles(block_count: usize) -> Vec<ScoreTileInfo> {
    let blocks_per_tile = SCORE_TILE / SATURATION_BLOCK;
    (0..block_count)
        .step_by(blocks_per_tile)
        .map(|block_start| ScoreTileInfo {
            block_start,
            block_end: (block_start + blocks_per_tile).min(block_count),
        })
        .collect()
}

#[derive(Clone, Copy, Debug, Default)]
struct CandidateScoreSummary {
    pair_count: u64,
    pair_emissions: u64,
    prefix_terms: u64,
    zero_overlap_prunes: u64,
    pending_activity: u64,
}

#[derive(Clone, Copy, Debug)]
struct InterleavedProfileSchedule {
    profile_count: usize,
    stripes: usize,
    rows: usize,
    slots: usize,
}

impl InterleavedProfileSchedule {
    fn new(profile_count: usize, stripes: usize) -> Self {
        let stripes = stripes.min(profile_count).max(1);
        let rows = profile_count.div_ceil(stripes);
        Self {
            profile_count,
            stripes,
            rows,
            slots: rows.saturating_mul(stripes),
        }
    }

    #[inline]
    fn profile(self, slot: usize) -> Option<usize> {
        if slot >= self.slots {
            return None;
        }
        let row = slot / self.stripes;
        let stripe = slot % self.stripes;
        let profile = stripe * self.rows + row;
        (profile < self.profile_count).then_some(profile)
    }
}

impl CandidateScoreSummary {
    fn record_activity(&mut self, progress: &dyn ProgressObserver) {
        self.pending_activity += 1;
        if self.pending_activity >= DIRECT_ACTIVITY_BATCH {
            progress.add_activity(self.pending_activity);
            self.pending_activity = 0;
        }
    }

    fn flush_activity(&mut self, progress: &dyn ProgressObserver) {
        progress.add_activity(self.pending_activity);
        self.pending_activity = 0;
    }
}

#[cfg(test)]
#[derive(Default)]
struct CandidatePostingSlices<'a> {
    first: Option<PostingView<'a>>,
    second: Option<PostingView<'a>>,
    additional: Vec<PostingView<'a>>,
}

#[cfg(test)]
impl<'a> CandidatePostingSlices<'a> {
    fn push(&mut self, posting: PostingView<'a>) {
        if posting.is_empty() {
            return;
        }
        if self.first.is_none() {
            self.first = Some(posting);
        } else if self.second.is_none() {
            self.second = Some(posting);
        } else {
            self.additional.push(posting);
        }
    }

    fn len(&self) -> usize {
        usize::from(self.first.is_some())
            + usize::from(self.second.is_some())
            + self.additional.len()
    }

    fn iter(&self) -> impl Iterator<Item = PostingView<'a>> + '_ {
        self.first
            .iter()
            .copied()
            .chain(self.second.iter().copied())
            .chain(self.additional.iter().copied())
    }
}

struct CandidatePostingConsumer<'work, 'index, 'posting> {
    first: Option<PostingView<'posting>>,
    second: Option<PostingView<'posting>>,
    streaming: bool,
    include_bm25: bool,
    left: &'work mut IndexedLeftContext<'index>,
    seen: &'work mut Option<CandidateSeen>,
    profile_count: usize,
    scorer: &'work mut WorkerScorer<'index>,
    summary: &'work mut CandidateScoreSummary,
    unchecked_emissions: &'work mut u64,
    progress: &'work dyn ProgressObserver,
}

impl<'work, 'index, 'posting> CandidatePostingConsumer<'work, 'index, 'posting> {
    #[allow(clippy::too_many_arguments)]
    fn new(
        include_bm25: bool,
        left: &'work mut IndexedLeftContext<'index>,
        seen: &'work mut Option<CandidateSeen>,
        profile_count: usize,
        scorer: &'work mut WorkerScorer<'index>,
        summary: &'work mut CandidateScoreSummary,
        unchecked_emissions: &'work mut u64,
        progress: &'work dyn ProgressObserver,
    ) -> Self {
        Self {
            first: None,
            second: None,
            streaming: false,
            include_bm25,
            left,
            seen,
            profile_count,
            scorer,
            summary,
            unchecked_emissions,
            progress,
        }
    }

    fn push(&mut self, posting: PostingView<'posting>) -> Result<(), DedupError> {
        if posting.is_empty() {
            return Ok(());
        }
        if self.streaming {
            return score_seen_candidate_posting(
                posting,
                self.include_bm25,
                self.left,
                self.seen.as_mut().expect("streaming candidate set exists"),
                self.scorer,
                self.summary,
                self.unchecked_emissions,
                self.progress,
            );
        }
        if self.first.is_none() {
            self.first = Some(posting);
            return Ok(());
        }
        if self.second.is_none() {
            self.second = Some(posting);
            return Ok(());
        }

        let seen = self
            .seen
            .get_or_insert_with(|| CandidateSeen::new(self.profile_count));
        seen.begin_profile(self.left.profile_id);
        score_seen_candidate_posting(
            self.first.expect("the first candidate posting exists"),
            self.include_bm25,
            self.left,
            seen,
            self.scorer,
            self.summary,
            self.unchecked_emissions,
            self.progress,
        )?;
        score_seen_candidate_posting(
            self.second.expect("the second candidate posting exists"),
            self.include_bm25,
            self.left,
            seen,
            self.scorer,
            self.summary,
            self.unchecked_emissions,
            self.progress,
        )?;
        self.streaming = true;
        score_seen_candidate_posting(
            posting,
            self.include_bm25,
            self.left,
            seen,
            self.scorer,
            self.summary,
            self.unchecked_emissions,
            self.progress,
        )
    }

    fn add_prefix_terms(&mut self, terms: usize) {
        self.summary.prefix_terms = self.summary.prefix_terms.saturating_add(terms as u64);
    }

    fn finish(self) -> Result<(), DedupError> {
        if self.streaming {
            return Ok(());
        }
        match (self.first, self.second) {
            (None, None) => Ok(()),
            (Some(first), None) => score_single_candidate_posting(
                first,
                self.include_bm25,
                self.left,
                self.scorer,
                self.summary,
                self.unchecked_emissions,
                self.progress,
            ),
            (Some(first), Some(second)) => score_merged_candidate_postings(
                first,
                second,
                self.include_bm25,
                self.left,
                self.scorer,
                self.summary,
                self.unchecked_emissions,
                self.progress,
            ),
            (None, Some(_)) => unreachable!("a second posting requires a first posting"),
        }
    }
}

fn upper_rect_tile_count(left_axis: u64, right_axis: u64) -> u64 {
    left_axis
        .saturating_mul(right_axis)
        .saturating_sub(choose_two(left_axis))
}

fn upper_rect_tile_coordinate(index: u64, left_axis: u64, right_axis: u64) -> (u64, u64) {
    debug_assert!(left_axis <= right_axis);
    debug_assert!(index < upper_rect_tile_count(left_axis, right_axis));
    let row_start = |row: u64| {
        row.saturating_mul(right_axis)
            .saturating_sub(choose_two(row))
    };
    let mut low = 0_u64;
    let mut high = left_axis;
    while low + 1 < high {
        let middle = low + (high - low) / 2;
        if row_start(middle) <= index {
            low = middle;
        } else {
            high = middle;
        }
    }
    let right = low + index.saturating_sub(row_start(low));
    (low, right)
}

struct UpperRectTileCoordinateCursor {
    left: u64,
    right: u64,
    right_axis: u64,
}

impl UpperRectTileCoordinateCursor {
    fn new(index: u64, left_axis: u64, right_axis: u64) -> Self {
        let (left, right) = upper_rect_tile_coordinate(index, left_axis, right_axis);
        Self {
            left,
            right,
            right_axis,
        }
    }

    fn next(&mut self) -> (u64, u64) {
        let coordinate = (self.left, self.right);
        self.right += 1;
        if self.right == self.right_axis {
            self.left += 1;
            self.right = self.left;
        }
        coordinate
    }
}

fn score_cross_profiles(
    index: &DirectIndex,
    hits: &ProfileHits,
    threshold: f64,
    stats: &AtomicStats,
    progress: &dyn ProgressObserver,
    plan: &CrossProfilePlan,
    sampling: CrossSamplingPlan,
) -> Result<(CandidateScoreSummary, MetadataSamplingResult), DedupError> {
    if let CrossProfilePlan::Indexed(candidates) = plan {
        return score_streamed_candidates(
            index, hits, threshold, stats, progress, candidates, sampling,
        );
    }

    progress.begin_phase(
        "direct_bm25",
        Some(
            index
                .logical_member_pairs()
                .saturating_sub(equivalent_scoring_work(index)),
        ),
    );
    let cancelled = AtomicBool::new(false);
    let blocks = build_score_blocks(index);
    let tiles = build_score_tiles(blocks.len());
    let left_tile_count = index.query_profile_count.div_ceil(SCORE_TILE) as u64;
    let right_tile_count = tiles.len() as u64;
    let tile_count = upper_rect_tile_count(left_tile_count, right_tile_count);
    let next_tile = AtomicU64::new(0);
    let workers = rayon::current_num_threads().max(1);
    let worker_samples = (0..workers)
        .into_par_iter()
        .map(|_| {
            let mut scorer = WorkerScorer::new_with_sampling(
                index,
                hits,
                threshold,
                sampling.pairs.clone(),
                sampling.image_sample_size,
            );
            'work: loop {
                let scheduled = next_tile.load(Ordering::Relaxed);
                let remaining = tile_count.saturating_sub(scheduled);
                let tile_batch =
                    if remaining > (workers as u64).saturating_mul(MAX_SCORE_TILE_BATCH) {
                        MAX_SCORE_TILE_BATCH
                    } else {
                        1
                    };
                let tile_start = next_tile.fetch_add(tile_batch, Ordering::Relaxed);
                if tile_start >= tile_count || cancelled.load(Ordering::Relaxed) {
                    break;
                }
                let tile_end = tile_start.saturating_add(tile_batch).min(tile_count);
                let mut coordinates = UpperRectTileCoordinateCursor::new(
                    tile_start,
                    left_tile_count,
                    right_tile_count,
                );
                for _ in tile_start..tile_end {
                    if cancelled.load(Ordering::Relaxed) {
                        break 'work;
                    }
                    if progress.check_cancelled().is_err() {
                        cancelled.store(true, Ordering::Relaxed);
                        break 'work;
                    }
                    let (left_tile_index, right_tile_index) = coordinates.next();
                    let left_tile = &tiles[left_tile_index as usize];
                    let right_tile = &tiles[right_tile_index as usize];
                    let mut completed = 0_u64;
                    for left_block_index in left_tile.block_start..left_tile.block_end {
                        let first_right_block = if left_tile_index == right_tile_index {
                            left_block_index
                        } else {
                            right_tile.block_start
                        };
                        for right_block_index in first_right_block..right_tile.block_end {
                            let left_block = &blocks[left_block_index];
                            let right_block = &blocks[right_block_index];
                            if sampling.image_sample_size == 0
                                && hits.is_single_word()
                                && hits
                                    .block_contains_mask(left_block_index, right_block.chain_mask)
                                && hits
                                    .block_contains_mask(right_block_index, left_block.chain_mask)
                            {
                                let (skipped_profiles, skipped_work) =
                                    if left_block_index == right_block_index {
                                        (
                                            choose_two(left_block.profile_count()),
                                            choose_two(left_block.member_sum)
                                                .saturating_sub(left_block.equivalent_member_pairs),
                                        )
                                    } else {
                                        (
                                            left_block
                                                .profile_count()
                                                .saturating_mul(right_block.profile_count()),
                                            left_block
                                                .member_sum
                                                .saturating_mul(right_block.member_sum),
                                        )
                                    };
                                stats
                                    .saturated_profile_pairs
                                    .fetch_add(skipped_profiles, Ordering::Relaxed);
                                stats
                                    .block_saturated_profile_pairs
                                    .fetch_add(skipped_profiles, Ordering::Relaxed);
                                completed = completed.saturating_add(skipped_work);
                                if completed >= FULL_DIRECT_PROGRESS_BATCH {
                                    progress.add_completed(completed);
                                    completed = 0;
                                }
                                continue;
                            }
                            for left_id in left_block.start..left_block.end {
                                let first_right = if left_block_index == right_block_index {
                                    left_id + 1
                                } else {
                                    right_block.start
                                };
                                for right_id in first_right..right_block.end {
                                    completed = completed
                                        .saturating_add(scorer.score_pair(left_id, right_id));
                                    if completed >= FULL_DIRECT_PROGRESS_BATCH {
                                        progress.add_completed(completed);
                                        completed = 0;
                                    }
                                }
                            }
                        }
                    }
                    progress.add_completed(completed);
                }
            }
            scorer.finish(stats)
        })
        .collect::<Vec<_>>();
    if cancelled.load(Ordering::Relaxed) {
        Err(DedupError::Interrupted)
    } else {
        let mut samples = MetadataSamplingResult {
            pairs: sampling.pairs.sampler(),
            images: MetadataImageSampler::new(sampling.image_sample_size),
        };
        for worker in worker_samples {
            samples.pairs.merge(worker.pairs);
            samples.images.merge(worker.images);
        }
        Ok((CandidateScoreSummary::default(), samples))
    }
}

fn score_streamed_candidates(
    index: &DirectIndex,
    hits: &ProfileHits,
    threshold: f64,
    stats: &AtomicStats,
    progress: &dyn ProgressObserver,
    candidates: &ResidentCandidateIndex,
    sampling: CrossSamplingPlan,
) -> Result<(CandidateScoreSummary, MetadataSamplingResult), DedupError> {
    score_candidate_sources(
        index, hits, threshold, stats, progress, candidates, sampling,
    )
}

fn score_candidate_sources(
    index: &DirectIndex,
    hits: &ProfileHits,
    threshold: f64,
    stats: &AtomicStats,
    progress: &dyn ProgressObserver,
    candidates: &ResidentCandidateIndex,
    sampling: CrossSamplingPlan,
) -> Result<(CandidateScoreSummary, MetadataSamplingResult), DedupError> {
    let profile_count = index.profiles.len();
    let query_profile_count = index.query_profile_count;
    if query_profile_count == 0 || profile_count < 2 {
        return Ok((
            CandidateScoreSummary::default(),
            MetadataSamplingResult {
                pairs: sampling.pairs.sampler(),
                images: MetadataImageSampler::new(sampling.image_sample_size),
            },
        ));
    }
    let lane_count = candidate_seen_lanes(query_profile_count);
    let schedule = InterleavedProfileSchedule::new(query_profile_count, lane_count);
    let next_left = AtomicUsize::new(0);
    progress.begin_phase("direct_bm25", Some(query_profile_count as u64));
    let lanes = (0..lane_count)
        .into_par_iter()
        .map(|_| {
            let mut seen = None;
            let mut scorer = WorkerScorer::new_with_sampling(
                index,
                hits,
                threshold,
                sampling.pairs.clone(),
                sampling.image_sample_size,
            );
            let mut summary = CandidateScoreSummary::default();
            let mut unchecked_emissions = 0_u64;
            loop {
                progress.check_cancelled()?;
                let slot = next_left.fetch_add(1, Ordering::Relaxed);
                if slot >= schedule.slots {
                    break;
                }
                let Some(left_id) = schedule.profile(slot) else {
                    continue;
                };
                let profile_id = left_id as u32;
                let left_profile = &index.profiles[left_id];
                let max_document = left_profile.max_document();
                let mut left = IndexedLeftContext::new(index, hits, profile_id);
                let mut postings = CandidatePostingConsumer::new(
                    candidates.include_bm25,
                    &mut left,
                    &mut seen,
                    profile_count,
                    &mut scorer,
                    &mut summary,
                    &mut unchecked_emissions,
                    progress,
                );
                if !candidates.include_bm25 || index.document_terms(max_document).is_empty() {
                    postings.push(
                        candidates.shards[candidate_shard(max_document, 0)]
                            .global_exact_after(max_document, profile_id),
                    )?;
                }
                if left_profile.is_evm
                    && (!candidates.include_bm25 || left_profile.has_empty_token_document)
                {
                    for &(token, document) in index.anchors(left_profile).iter().rev() {
                        if index.token_profile_counts[token as usize] < 2 {
                            continue;
                        }
                        if candidates.include_bm25 && !index.document_terms(document).is_empty() {
                            continue;
                        }
                        let token_exact = candidates.shards[token_candidate_shard(token)]
                            .token_exact(token, candidates.token_ranges[token as usize].exact);
                        postings.push(token_exact.posting_after(document, profile_id))?;
                    }
                }
                if candidates.include_bm25 {
                    let prefix = candidates.prefixes.get(max_document);
                    postings.add_prefix_terms(prefix.len());
                    for &term in prefix {
                        postings.push(candidates.global_full.posting_after(term, profile_id))?;
                    }
                    if left_profile.is_evm {
                        for &(token, document) in index.anchors(left_profile).iter().rev() {
                            if index.token_profile_counts[token as usize] < 2 {
                                continue;
                            }
                            if index.document_terms(document).is_empty() {
                                continue;
                            }
                            let token_full = candidates.shards[token_candidate_shard(token)]
                                .token_full(token, candidates.token_ranges[token as usize].full);
                            let prefix = candidates.prefixes.get(document);
                            postings.add_prefix_terms(prefix.len());
                            let mut posting_error = None;
                            token_full.visit_postings_after(prefix, profile_id, |posting| {
                                if posting_error.is_none() {
                                    posting_error = postings.push(posting).err();
                                }
                            });
                            if let Some(error) = posting_error {
                                return Err(error);
                            }
                        }
                    }
                }
                postings.finish()?;
                progress.add_completed(1);
            }
            summary.flush_activity(progress);
            let samples = scorer.finish(stats);
            Ok::<_, DedupError>((summary, samples))
        })
        .collect::<Vec<_>>()
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    let mut samples = MetadataSamplingResult {
        pairs: sampling.pairs.sampler(),
        images: MetadataImageSampler::new(sampling.image_sample_size),
    };
    let summary = lanes.into_iter().fold(
        CandidateScoreSummary::default(),
        |mut total, (lane, worker_samples)| {
            samples.pairs.merge(worker_samples.pairs);
            samples.images.merge(worker_samples.images);
            total.pair_count = total.pair_count.saturating_add(lane.pair_count);
            total.pair_emissions = total.pair_emissions.saturating_add(lane.pair_emissions);
            total.prefix_terms = total.prefix_terms.saturating_add(lane.prefix_terms);
            total.zero_overlap_prunes = total
                .zero_overlap_prunes
                .saturating_add(lane.zero_overlap_prunes);
            total
        },
    );
    Ok((summary, samples))
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
fn score_candidate_postings<'a>(
    postings: &CandidatePostingSlices<'_>,
    include_bm25: bool,
    left: &mut IndexedLeftContext<'a>,
    seen: &mut Option<CandidateSeen>,
    profile_count: usize,
    scorer: &mut WorkerScorer<'a>,
    summary: &mut CandidateScoreSummary,
    unchecked_emissions: &mut u64,
    progress: &dyn ProgressObserver,
) -> Result<(), DedupError> {
    match postings.len() {
        0 => Ok(()),
        1 => score_single_candidate_posting(
            postings.first.expect("one candidate posting exists"),
            include_bm25,
            left,
            scorer,
            summary,
            unchecked_emissions,
            progress,
        ),
        2 => score_merged_candidate_postings(
            postings.first.expect("the first candidate posting exists"),
            postings
                .second
                .expect("the second candidate posting exists"),
            include_bm25,
            left,
            scorer,
            summary,
            unchecked_emissions,
            progress,
        ),
        _ => {
            let seen = seen.get_or_insert_with(|| CandidateSeen::new(profile_count));
            seen.begin_profile(left.profile_id);
            for posting in postings.iter() {
                score_seen_candidate_posting(
                    posting,
                    include_bm25,
                    left,
                    seen,
                    scorer,
                    summary,
                    unchecked_emissions,
                    progress,
                )?;
            }
            Ok(())
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn score_seen_candidate_posting<'a>(
    posting: PostingView<'_>,
    include_bm25: bool,
    left: &mut IndexedLeftContext<'a>,
    seen: &mut CandidateSeen,
    scorer: &mut WorkerScorer<'a>,
    summary: &mut CandidateScoreSummary,
    unchecked_emissions: &mut u64,
    progress: &dyn ProgressObserver,
) -> Result<(), DedupError> {
    match posting {
        PostingView::Sparse(profiles) => {
            record_candidate_emissions(
                profiles.len() as u64,
                summary,
                unchecked_emissions,
                progress,
            )?;
            for &right in profiles {
                debug_assert!(right > left.profile_id);
                if seen.insert(right) {
                    score_unique_indexed_candidate(
                        left,
                        right,
                        include_bm25,
                        scorer,
                        summary,
                        progress,
                    );
                }
            }
        }
        PostingView::Dense {
            words,
            base_word,
            minimum,
        } => {
            let mut pending_emissions = 0_u64;
            for (offset, &word) in words.iter().enumerate() {
                let word_index = base_word + offset as u32;
                let candidates = masked_dense_word(word, word_index, minimum);
                queue_candidate_emissions(
                    u64::from(candidates.count_ones()),
                    &mut pending_emissions,
                    summary,
                    unchecked_emissions,
                    progress,
                )?;
                let mut new_candidates = seen.insert_word(word_index, candidates);
                while new_candidates != 0 {
                    let bit = new_candidates.trailing_zeros();
                    new_candidates &= new_candidates - 1;
                    let right = word_index * u64::BITS + bit;
                    debug_assert!(right > left.profile_id);
                    score_unique_indexed_candidate(
                        left,
                        right,
                        include_bm25,
                        scorer,
                        summary,
                        progress,
                    );
                }
            }
            flush_candidate_emissions(
                &mut pending_emissions,
                summary,
                unchecked_emissions,
                progress,
            )?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn score_single_candidate_posting<'a>(
    posting: PostingView<'_>,
    include_bm25: bool,
    left: &mut IndexedLeftContext<'a>,
    scorer: &mut WorkerScorer<'a>,
    summary: &mut CandidateScoreSummary,
    unchecked_emissions: &mut u64,
    progress: &dyn ProgressObserver,
) -> Result<(), DedupError> {
    match posting {
        PostingView::Sparse(profiles) => {
            record_candidate_emissions(
                profiles.len() as u64,
                summary,
                unchecked_emissions,
                progress,
            )?;
            let mut previous = None;
            for &right in profiles {
                debug_assert!(right > left.profile_id);
                if previous == Some(right) {
                    continue;
                }
                previous = Some(right);
                score_unique_indexed_candidate(
                    left,
                    right,
                    include_bm25,
                    scorer,
                    summary,
                    progress,
                );
            }
        }
        PostingView::Dense {
            words,
            base_word,
            minimum,
        } => {
            let mut pending_emissions = 0_u64;
            for (offset, &word) in words.iter().enumerate() {
                let word_index = base_word + offset as u32;
                let mut candidates = masked_dense_word(word, word_index, minimum);
                queue_candidate_emissions(
                    u64::from(candidates.count_ones()),
                    &mut pending_emissions,
                    summary,
                    unchecked_emissions,
                    progress,
                )?;
                while candidates != 0 {
                    let bit = candidates.trailing_zeros();
                    candidates &= candidates - 1;
                    let right = word_index * u64::BITS + bit;
                    debug_assert!(right > left.profile_id);
                    score_unique_indexed_candidate(
                        left,
                        right,
                        include_bm25,
                        scorer,
                        summary,
                        progress,
                    );
                }
            }
            flush_candidate_emissions(
                &mut pending_emissions,
                summary,
                unchecked_emissions,
                progress,
            )?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn score_merged_candidate_postings<'a>(
    first: PostingView<'_>,
    second: PostingView<'_>,
    include_bm25: bool,
    left: &mut IndexedLeftContext<'a>,
    scorer: &mut WorkerScorer<'a>,
    summary: &mut CandidateScoreSummary,
    unchecked_emissions: &mut u64,
    progress: &dyn ProgressObserver,
) -> Result<(), DedupError> {
    let count_while_merging =
        !matches!(first, PostingView::Sparse(_)) || !matches!(second, PostingView::Sparse(_));
    if let (PostingView::Sparse(first), PostingView::Sparse(second)) = (first, second) {
        record_candidate_emissions(
            (first.len() as u64).saturating_add(second.len() as u64),
            summary,
            unchecked_emissions,
            progress,
        )?;
    }
    let mut pending_emissions = 0_u64;
    let mut first = first.iter().peekable();
    let mut second = second.iter().peekable();
    let mut previous = None;
    loop {
        let (right, emissions) = match (first.peek(), second.peek()) {
            (Some(&first_profile), Some(&second_profile)) => {
                match first_profile.cmp(&second_profile) {
                    std::cmp::Ordering::Less => {
                        first.next();
                        (first_profile, 1)
                    }
                    std::cmp::Ordering::Greater => {
                        second.next();
                        (second_profile, 1)
                    }
                    std::cmp::Ordering::Equal => {
                        first.next();
                        second.next();
                        (first_profile, 2)
                    }
                }
            }
            (Some(&first_profile), None) => {
                first.next();
                (first_profile, 1)
            }
            (None, Some(&second_profile)) => {
                second.next();
                (second_profile, 1)
            }
            (None, None) => break,
        };
        if count_while_merging {
            queue_candidate_emissions(
                emissions,
                &mut pending_emissions,
                summary,
                unchecked_emissions,
                progress,
            )?;
        }
        debug_assert!(right > left.profile_id);
        if previous == Some(right) {
            continue;
        }
        previous = Some(right);
        score_unique_indexed_candidate(left, right, include_bm25, scorer, summary, progress);
    }
    if count_while_merging {
        flush_candidate_emissions(
            &mut pending_emissions,
            summary,
            unchecked_emissions,
            progress,
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn queue_candidate_emissions(
    emissions: u64,
    pending: &mut u64,
    summary: &mut CandidateScoreSummary,
    unchecked_emissions: &mut u64,
    progress: &dyn ProgressObserver,
) -> Result<(), DedupError> {
    *pending += emissions;
    loop {
        let until_cancel_check = CANDIDATE_CANCEL_BATCH - *unchecked_emissions;
        if *pending < until_cancel_check {
            return Ok(());
        }
        record_candidate_emissions(until_cancel_check, summary, unchecked_emissions, progress)?;
        *pending -= until_cancel_check;
    }
}

fn flush_candidate_emissions(
    pending: &mut u64,
    summary: &mut CandidateScoreSummary,
    unchecked_emissions: &mut u64,
    progress: &dyn ProgressObserver,
) -> Result<(), DedupError> {
    record_candidate_emissions(*pending, summary, unchecked_emissions, progress)?;
    *pending = 0;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn record_candidate_emissions(
    mut emissions: u64,
    summary: &mut CandidateScoreSummary,
    unchecked_emissions: &mut u64,
    progress: &dyn ProgressObserver,
) -> Result<(), DedupError> {
    while emissions != 0 {
        debug_assert!(*unchecked_emissions < CANDIDATE_CANCEL_BATCH);
        let capacity = CANDIDATE_CANCEL_BATCH - *unchecked_emissions;
        let batch = emissions.min(capacity);
        summary.pair_emissions = summary.pair_emissions.saturating_add(batch);
        *unchecked_emissions += batch;
        emissions -= batch;
        if *unchecked_emissions == CANDIDATE_CANCEL_BATCH {
            progress.check_cancelled()?;
            *unchecked_emissions = 0;
        }
    }
    Ok(())
}

fn score_unique_indexed_candidate<'a>(
    left: &mut IndexedLeftContext<'a>,
    right: u32,
    include_bm25: bool,
    scorer: &mut WorkerScorer<'a>,
    summary: &mut CandidateScoreSummary,
    progress: &dyn ProgressObserver,
) {
    summary.record_activity(progress);
    if scorer.score_indexed_candidate(left, right, include_bm25) {
        summary.pair_count = summary.pair_count.saturating_add(1);
    } else {
        summary.zero_overlap_prunes = summary.zero_overlap_prunes.saturating_add(1);
    }
}

#[cfg(test)]
fn score_document_pair(
    index: &DirectIndex,
    left: DocumentId,
    right: DocumentId,
    threshold: f64,
    overlap_filter_passed: bool,
    stats: &mut LocalStats,
) -> bool {
    let left_document = &index.documents[left as usize];
    let left_terms = index.document_terms(left);
    score_document_pair_with_prepared_left(
        index,
        left_document,
        left_terms,
        right,
        threshold,
        overlap_filter_passed,
        stats,
    )
}

#[cfg(test)]
fn score_document_pair_with_prepared_left(
    index: &DirectIndex,
    left_document: &PreparedDocument,
    left_terms: &[(u32, u32)],
    right: DocumentId,
    threshold: f64,
    overlap_filter_passed: bool,
    stats: &mut LocalStats,
) -> bool {
    stats.bm25_scores += 1;
    let right_document = &index.documents[right as usize];
    let right_terms = index.document_terms(right);
    let decision = if overlap_filter_passed {
        similarity_at_least_after_overlap_filter(
            left_document,
            left_terms,
            right_document,
            right_terms,
            threshold,
        )
    } else {
        similarity_at_least(
            left_document,
            left_terms,
            right_document,
            right_terms,
            threshold,
        )
    };
    if decision.zero_overlap_pruned {
        stats.bm25_zero_overlap_prunes += 1;
    }
    match decision.upper_bound_prune {
        UpperBoundPrune::None => {}
        UpperBoundPrune::Initial => {
            stats.bm25_upper_bound_prunes += 1;
            stats.bm25_initial_upper_bound_prunes += 1;
        }
        UpperBoundPrune::Iterative => {
            stats.bm25_upper_bound_prunes += 1;
            stats.bm25_iterative_upper_bound_prunes += 1;
        }
    }
    decision.matched
}

#[cfg(test)]
fn tile_coordinates(ordinal: u64, axis: u64) -> (u64, u64) {
    let mut low = 0_u64;
    let mut high = axis;
    while low + 1 < high {
        let middle = low + (high - low) / 2;
        if tile_row_start(middle, axis) <= ordinal {
            low = middle;
        } else {
            high = middle;
        }
    }
    let row_start = tile_row_start(low, axis);
    let left = ordinal - row_start;
    (left, left + low)
}

#[cfg(test)]
struct TileCoordinateCursor {
    axis: u64,
    gap: u64,
    left: u64,
}

#[cfg(test)]
impl TileCoordinateCursor {
    fn new(ordinal: u64, axis: u64) -> Self {
        let (left, right) = tile_coordinates(ordinal, axis);
        Self {
            axis,
            gap: right - left,
            left,
        }
    }

    fn next(&mut self) -> (u64, u64) {
        let coordinates = (self.left, self.left + self.gap);
        self.left += 1;
        if self.left + self.gap >= self.axis {
            self.gap += 1;
            self.left = 0;
        }
        coordinates
    }
}

#[cfg(test)]
fn tile_row_start(row: u64, axis: u64) -> u64 {
    row.saturating_mul(axis)
        .saturating_sub(row.saturating_mul(row.saturating_sub(1)) / 2)
}

fn selected_documents(
    left: &ContractProfile,
    left_anchors: &[(TokenKeyId, DocumentId)],
    right: &ContractProfile,
    right_anchors: &[(TokenKeyId, DocumentId)],
) -> (DocumentId, DocumentId) {
    if left.is_evm && right.is_evm {
        return selected_evm_documents(left, left_anchors, right, right_anchors);
    }
    (left.max_document(), right.max_document())
}

fn selected_image_anchors(
    left: &ContractProfile,
    left_anchors: &[(TokenKeyId, DocumentId)],
    right: &ContractProfile,
    right_anchors: &[(TokenKeyId, DocumentId)],
) -> Option<((TokenKeyId, DocumentId), (TokenKeyId, DocumentId))> {
    if left.is_evm
        && right.is_evm
        && anchor_token_ranges_overlap(left_anchors, right_anchors)
        && token_masks_overlap(&left.token_mask, &right.token_mask)
        && let Some(anchors) = highest_shared_anchor_entries(left_anchors, right_anchors)
    {
        return Some(anchors);
    }
    let left_anchor = left_anchors
        .iter()
        .rev()
        .find(|anchor| anchor.1 == left.max_document())
        .copied()?;
    let right_anchor = right_anchors
        .iter()
        .rev()
        .find(|anchor| anchor.1 == right.max_document())
        .copied()?;
    Some((left_anchor, right_anchor))
}

#[inline]
fn selected_evm_documents(
    left: &ContractProfile,
    left_anchors: &[(TokenKeyId, DocumentId)],
    right: &ContractProfile,
    right_anchors: &[(TokenKeyId, DocumentId)],
) -> (DocumentId, DocumentId) {
    debug_assert!(left.is_evm && right.is_evm);
    if anchor_token_ranges_overlap(left_anchors, right_anchors)
        && token_masks_overlap(&left.token_mask, &right.token_mask)
        && let Some(documents) = highest_shared_anchor(left_anchors, right_anchors)
    {
        return documents;
    }
    (left.max_document(), right.max_document())
}

fn anchor_token_ranges_overlap(
    left: &[(TokenKeyId, DocumentId)],
    right: &[(TokenKeyId, DocumentId)],
) -> bool {
    match (left.first(), left.last(), right.first(), right.last()) {
        (Some(left_first), Some(left_last), Some(right_first), Some(right_last)) => {
            left_first.0 <= right_last.0 && right_first.0 <= left_last.0
        }
        _ => false,
    }
}

#[inline]
fn token_masks_overlap(left: &[u64; TOKEN_MASK_WORDS], right: &[u64; TOKEN_MASK_WORDS]) -> bool {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        // The Linux production target is compiled for Zen 4, where AVX2 is
        // guaranteed. Unaligned loads keep the compact profile layout.
        unsafe {
            use std::arch::x86_64::{__m256i, _mm256_loadu_si256, _mm256_testz_si256};
            let left = _mm256_loadu_si256(left.as_ptr().cast::<__m256i>());
            let right = _mm256_loadu_si256(right.as_ptr().cast::<__m256i>());
            return _mm256_testz_si256(left, right) == 0;
        }
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
    {
        left[0] & right[0] != 0
            || left[1] & right[1] != 0
            || left[2] & right[2] != 0
            || left[3] & right[3] != 0
    }
}

fn highest_shared_anchor(
    left: &[(TokenKeyId, DocumentId)],
    right: &[(TokenKeyId, DocumentId)],
) -> Option<(DocumentId, DocumentId)> {
    const IMBALANCE_RATIO: usize = 8;
    if left.len().saturating_mul(IMBALANCE_RATIO) < right.len() {
        return highest_shared_anchor_imbalanced(left, right);
    }
    if right.len().saturating_mul(IMBALANCE_RATIO) < left.len() {
        return highest_shared_anchor_imbalanced(right, left).map(|(right, left)| (left, right));
    }

    let mut left_end = left.len();
    let mut right_end = right.len();
    while left_end != 0 && right_end != 0 {
        let left_anchor = left[left_end - 1];
        let right_anchor = right[right_end - 1];
        match left_anchor.0.cmp(&right_anchor.0) {
            std::cmp::Ordering::Equal => return Some((left_anchor.1, right_anchor.1)),
            std::cmp::Ordering::Greater => left_end -= 1,
            std::cmp::Ordering::Less => right_end -= 1,
        }
    }
    None
}

fn highest_shared_anchor_entries(
    left: &[(TokenKeyId, DocumentId)],
    right: &[(TokenKeyId, DocumentId)],
) -> Option<((TokenKeyId, DocumentId), (TokenKeyId, DocumentId))> {
    let mut left_end = left.len();
    let mut right_end = right.len();
    while left_end != 0 && right_end != 0 {
        let left_anchor = left[left_end - 1];
        let right_anchor = right[right_end - 1];
        match left_anchor.0.cmp(&right_anchor.0) {
            std::cmp::Ordering::Equal => return Some((left_anchor, right_anchor)),
            std::cmp::Ordering::Greater => left_end -= 1,
            std::cmp::Ordering::Less => right_end -= 1,
        }
    }
    None
}

fn highest_shared_anchor_imbalanced(
    shorter: &[(TokenKeyId, DocumentId)],
    longer: &[(TokenKeyId, DocumentId)],
) -> Option<(DocumentId, DocumentId)> {
    let mut longer_end = longer.len();
    for &(token, short_document) in shorter.iter().rev() {
        longer_end = longer[..longer_end].partition_point(|anchor| anchor.0 <= token);
        if longer_end != 0 && longer[longer_end - 1].0 == token {
            return Some((short_document, longer[longer_end - 1].1));
        }
    }
    None
}

#[derive(Default)]
struct MetadataScopeMembers {
    contracts: Vec<ContractId>,
    duplicate_nft_count: u64,
}

impl MetadataScopeMembers {
    fn insert(&mut self, store: &EntityStore, member: MetadataMember) {
        self.contracts.push(member.contract_id);
        if member.nft_id.is_some() {
            self.duplicate_nft_count = self.duplicate_nft_count.saturating_add(1);
        } else {
            self.duplicate_nft_count = self
                .duplicate_nft_count
                .saturating_add(store.contracts[member.contract_id as usize].nft_count);
        }
    }

    fn merge(&mut self, other: Self) {
        self.contracts.extend(other.contracts);
        self.duplicate_nft_count = self
            .duplicate_nft_count
            .saturating_add(other.duplicate_nft_count);
    }

    fn dedup_contracts(&mut self) {
        self.contracts.par_sort_unstable();
        self.contracts.dedup();
    }

    fn into_counts(mut self) -> ScopeCounts {
        self.dedup_contracts();
        ScopeCounts {
            duplicate_contract_count: self.contracts.len() as u64,
            duplicate_nft_count: self.duplicate_nft_count,
        }
    }
}

fn record_metadata_mask(
    memberships: &mut AHashMap<ScopeKey, MetadataScopeMembers>,
    store: &EntityStore,
    primary_chain: ChainId,
    member: MetadataMember,
    duplicate_mask: u64,
) -> Result<(), DedupError> {
    let own_bit = 1_u64 << usize::from(primary_chain);
    if duplicate_mask & own_bit != 0 {
        add_metadata_member(
            memberships,
            store,
            primary_chain,
            member,
            ScopeKind::IntraChain,
            None,
        );
    }
    let mut cross_mask = duplicate_mask & !own_bit;
    if cross_mask != 0 {
        add_metadata_member(
            memberships,
            store,
            primary_chain,
            member,
            ScopeKind::CrossChainSummary,
            None,
        );
    }
    while cross_mask != 0 {
        let chain = cross_mask.trailing_zeros() as usize;
        let secondary_chain = ChainId::try_from(chain)
            .map_err(|_| DedupError::invalid("metadata", "too many chains for ChainId"))?;
        add_metadata_member(
            memberships,
            store,
            primary_chain,
            member,
            ScopeKind::ChainMatrix,
            Some(secondary_chain),
        );
        cross_mask &= cross_mask - 1;
    }
    Ok(())
}

fn record_wide_metadata_hits(
    memberships: &mut AHashMap<ScopeKey, MetadataScopeMembers>,
    store: &EntityStore,
    hits: &ProfileHits,
    profile_id: usize,
    profile_chains: &[(ChainId, u32)],
    primary_chain: ChainId,
    member: MetadataMember,
) -> Result<(), DedupError> {
    let mut intra_chain = false;
    let mut cross_chain = false;
    for chain in 0..store.chains.len() {
        let chain_id = ChainId::try_from(chain)
            .map_err(|_| DedupError::invalid("metadata", "too many chains for ChainId"))?;
        let equivalent_peer = profile_chains
            .iter()
            .find(|(candidate, _)| *candidate == chain_id)
            .map(|(_, count)| *count)
            .is_some_and(|count| chain_id != primary_chain || count > 1);
        if !hits.contains(profile_id, chain_id) && !equivalent_peer {
            continue;
        }
        if chain_id == primary_chain {
            intra_chain = true;
        } else {
            cross_chain = true;
            add_metadata_member(
                memberships,
                store,
                primary_chain,
                member,
                ScopeKind::ChainMatrix,
                Some(chain_id),
            );
        }
    }
    if intra_chain {
        add_metadata_member(
            memberships,
            store,
            primary_chain,
            member,
            ScopeKind::IntraChain,
            None,
        );
    }
    if cross_chain {
        add_metadata_member(
            memberships,
            store,
            primary_chain,
            member,
            ScopeKind::CrossChainSummary,
            None,
        );
    }
    Ok(())
}

fn add_metadata_member(
    memberships: &mut AHashMap<ScopeKey, MetadataScopeMembers>,
    store: &EntityStore,
    primary_chain: ChainId,
    member: MetadataMember,
    kind: ScopeKind,
    secondary_chain: Option<ChainId>,
) {
    memberships
        .entry(ScopeKey {
            kind,
            primary_chain,
            secondary_chain,
            dimension: Dimension::Metadata,
        })
        .or_default()
        .insert(store, member);
}

fn base_stats(
    store: &EntityStore,
    index: &DirectIndex,
    logical_contract_pairs: u64,
    profile_pair_tasks: u64,
    equivalent_profile_tasks: u64,
) -> MetadataStats {
    let total_contracts = store.contracts.len() as u64;
    let profiles = index.profiles.len() as u64;
    MetadataStats {
        eligible_contracts: index.eligible_contracts,
        eligible_contract_ratio: ratio(index.eligible_contracts, total_contracts),
        unique_profiles: profiles,
        profile_reduction_ratio: reduction_ratio(profiles, index.eligible_members),
        unique_documents: index.documents.len() as u64,
        document_reuse_ratio: reduction_ratio(index.documents.len() as u64, index.anchor_count),
        unique_terms: index.unique_terms,
        logical_contract_pairs,
        profile_pair_tasks,
        profile_pair_reduction_ratio: reduction_ratio(profile_pair_tasks, logical_contract_pairs),
        equivalent_profile_tasks,
        ..MetadataStats::default()
    }
}

fn equivalent_scoring_work(index: &DirectIndex) -> u64 {
    index.profiles.iter().fold(0_u64, |total, profile| {
        total.saturating_add(choose_two(u64::from(profile.member_len)))
    })
}

fn normalized_evm_token(token: &str) -> &str {
    let trimmed = token.trim();
    if trimmed.bytes().all(|byte| byte.is_ascii_digit()) {
        let magnitude = trimmed.trim_start_matches('0');
        if magnitude.is_empty() { "0" } else { magnitude }
    } else {
        token
    }
}

fn intern_shard<T: Hash + ?Sized>(value: &T) -> usize {
    let mut hasher = AHasher::default();
    value.hash(&mut hasher);
    hasher.finish() as usize & (INTERN_SHARDS - 1)
}

fn token_bit(token: TokenKeyId) -> (usize, u64) {
    let mixed = u64::from(token).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    let bit = (mixed >> 56) as usize;
    (bit / 64, 1_u64 << (bit % 64))
}

#[cfg(test)]
fn document_pair_key(left: DocumentId, right: DocumentId) -> u64 {
    let (left, right) = if left <= right {
        (left, right)
    } else {
        (right, left)
    };
    (u64::from(left) << 32) | u64::from(right)
}

#[cfg(test)]
fn profile_pair_key(left: u32, right: u32) -> u64 {
    let (left, right) = if left <= right {
        (left, right)
    } else {
        (right, left)
    };
    (u64::from(left) << 32) | u64::from(right)
}

#[cfg(test)]
fn decode_profile_pair(key: u64) -> (usize, usize) {
    ((key >> 32) as usize, key as u32 as usize)
}

fn choose_two(value: u64) -> u64 {
    value.saturating_mul(value.saturating_sub(1)) / 2
}

fn ratio(part: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        part as f64 / total as f64
    }
}

fn reduction_ratio(after: u64, before: u64) -> f64 {
    if before == 0 {
        0.0
    } else {
        1.0 - ratio(after, before)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::{Contract, InputRow, MetadataRecord, ScopeKind, SourceOrder};
    use crate::progress::NoopProgress;

    struct CancelledProgress;

    impl ProgressObserver for CancelledProgress {
        fn set_stage(&self, _stage: &str) {}
        fn begin_phase(&self, _phase: &str, _total: Option<u64>) {}
        fn set_total(&self, _total: Option<u64>) {}
        fn add_completed(&self, _delta: u64) {}
        fn check_cancelled(&self) -> Result<(), DedupError> {
            Err(DedupError::Interrupted)
        }
    }

    struct CancelAfterChecks {
        allowed: usize,
        checks: AtomicUsize,
    }

    impl CancelAfterChecks {
        fn new(allowed: usize) -> Self {
            Self {
                allowed,
                checks: AtomicUsize::new(0),
            }
        }
    }

    impl ProgressObserver for CancelAfterChecks {
        fn set_stage(&self, _stage: &str) {}
        fn begin_phase(&self, _phase: &str, _total: Option<u64>) {}
        fn set_total(&self, _total: Option<u64>) {}
        fn add_completed(&self, _delta: u64) {}
        fn check_cancelled(&self) -> Result<(), DedupError> {
            let check = self.checks.fetch_add(1, Ordering::Relaxed);
            if check >= self.allowed {
                Err(DedupError::Interrupted)
            } else {
                Ok(())
            }
        }
    }

    #[derive(Default)]
    struct PhaseProgress {
        state: Mutex<PhaseProgressState>,
    }

    #[derive(Default)]
    struct PhaseProgressState {
        current: Option<String>,
        phases: AHashMap<String, (Option<u64>, u64)>,
    }

    impl PhaseProgress {
        fn assert_complete(&self, phase: &str) {
            let state = self.state.lock().unwrap();
            let &(total, completed) = state
                .phases
                .get(phase)
                .unwrap_or_else(|| panic!("missing progress phase {phase}"));
            assert_eq!(
                Some(completed),
                total,
                "phase {phase} did not report its exact total"
            );
        }
    }

    impl ProgressObserver for PhaseProgress {
        fn set_stage(&self, _stage: &str) {}

        fn begin_phase(&self, phase: &str, total: Option<u64>) {
            let mut state = self.state.lock().unwrap();
            state.current = Some(phase.to_owned());
            state.phases.insert(phase.to_owned(), (total, 0));
        }

        fn set_total(&self, total: Option<u64>) {
            let mut state = self.state.lock().unwrap();
            let phase = state
                .current
                .clone()
                .expect("a phase exists before its total is set");
            state.phases.entry(phase).or_default().0 = total;
        }

        fn add_completed(&self, delta: u64) {
            let mut state = self.state.lock().unwrap();
            let phase = state
                .current
                .clone()
                .expect("a phase exists before progress is reported");
            state.phases.entry(phase).or_default().1 += delta;
        }
    }

    fn record(token_id: &str, canonical_json: &str) -> MetadataRecord {
        MetadataRecord {
            token_id: token_id.to_owned(),
            canonical_json: canonical_json.to_owned(),
            source_order: SourceOrder {
                file_ordinal: 0,
                file_row_number: 0,
            },
        }
    }

    #[test]
    fn chunk_flat_document_preparation_preserves_all_csr_ranges() {
        let values = (0..PREPARE_BATCH + 17)
            .map(|index| format!("field-{index}"))
            .collect::<Vec<_>>();
        let documents = DocumentInterner::new();
        for value in &values {
            documents.intern(value).unwrap();
        }

        let (prepared, terms, unique_terms) = documents.into_documents(&NoopProgress).unwrap();
        assert_eq!(prepared.len(), values.len());
        let mut covered = vec![false; terms.len()];
        for document in &prepared {
            let range = document.term_range();
            let start = range.start;
            let end = range.end;
            assert!(end <= terms.len());
            for slot in &mut covered[start..end] {
                assert!(!*slot, "CSR ranges overlap");
                *slot = true;
            }
        }
        assert!(covered.into_iter().all(|slot| slot));

        let document_count = prepared.len();
        let index = DirectIndex {
            documents: prepared,
            terms,
            document_context_weights: vec![1; document_count].into_boxed_slice(),
            profiles: Vec::new(),
            anchors: Vec::new(),
            token_profile_counts: Box::new([]),
            members: Vec::new(),
            chain_counts: Vec::new(),
            chain_count: 0,
            query_profile_count: 0,
            eligible_contracts: 0,
            eligible_members: 0,
            anchor_count: 0,
            unique_terms,
            image_witnesses: None,
        };
        let term_count = usize::try_from(unique_terms).unwrap();
        let term_ranks = (0..term_count as u32).rev().collect::<Vec<_>>();
        let threshold = 0.6;
        let mut expected_offsets = Vec::with_capacity(document_count + 1);
        let mut expected_terms = Vec::new();
        let mut ranked = Vec::new();
        let mut frequencies = Vec::new();
        expected_offsets.push(0_u32);
        for document in 0..document_count as DocumentId {
            ranked.clear();
            ranked.extend(
                index
                    .document_terms(document)
                    .iter()
                    .map(|(term, frequency)| (term_ranks[*term as usize], *term, *frequency)),
            );
            ranked.sort_unstable_by_key(|(rank, _, _)| *rank);
            frequencies.clear();
            frequencies.extend(ranked.iter().map(|(_, _, frequency)| *frequency));
            let len = lossless_prefix_len(&frequencies, threshold);
            ranked[..len].sort_unstable_by_key(|(_, term, _)| *term);
            expected_terms.extend(ranked[..len].iter().map(|(_, term, _)| *term));
            expected_offsets.push(u32::try_from(expected_terms.len()).unwrap());
        }

        let progress = PhaseProgress::default();
        let prefixes = build_document_prefixes(&index, &term_ranks, threshold, &progress).unwrap();
        assert_eq!(prefixes.offsets.as_ref(), expected_offsets.as_slice());
        assert_eq!(prefixes.terms.as_ref(), expected_terms.as_slice());
        progress.assert_complete("candidate_prefixes");
        progress.assert_complete("candidate_prefix_flatten");
    }

    #[test]
    fn empty_document_prefixes_keep_a_single_zero_offset() {
        let index = DirectIndex {
            documents: Vec::new(),
            terms: Vec::new(),
            document_context_weights: Box::new([]),
            profiles: Vec::new(),
            anchors: Vec::new(),
            token_profile_counts: Box::new([]),
            members: Vec::new(),
            chain_counts: Vec::new(),
            chain_count: 0,
            query_profile_count: 0,
            eligible_contracts: 0,
            eligible_members: 0,
            anchor_count: 0,
            unique_terms: 0,
            image_witnesses: None,
        };
        let progress = PhaseProgress::default();
        let prefixes = build_document_prefixes(&index, &[], 0.6, &progress).unwrap();
        assert_eq!(prefixes.offsets.as_ref(), &[0]);
        assert!(prefixes.terms.is_empty());
        progress.assert_complete("candidate_prefixes");
        progress.assert_complete("candidate_prefix_flatten");
    }

    fn profile(is_evm: bool, anchors: &[(u32, u32)]) -> ContractProfile {
        let max_document = anchors.last().unwrap().1;
        ContractProfile {
            is_evm,
            is_solana: false,
            has_empty_token_document: false,
            anchor_start: 0,
            anchor_len: anchors.len() as u32,
            max_document,
            token_mask: anchors
                .iter()
                .fold([0_u64; TOKEN_MASK_WORDS], |mut mask, (token, _)| {
                    let (word, bit) = token_bit(*token);
                    mask[word] |= bit;
                    mask
                }),
            chain_mask: 1,
            member_start: 0,
            member_len: 1,
            chain_start: 0,
            chain_len: 1,
        }
    }

    fn indexed_scoring_index(
        values: &[&str],
        profile_documents: &[usize],
        profile_chains: &[ChainId],
    ) -> DirectIndex {
        assert_eq!(profile_documents.len(), profile_chains.len());
        let interner = DocumentInterner::new();
        let document_ids = values
            .iter()
            .map(|value| interner.intern(value).unwrap())
            .collect::<Vec<_>>();
        let (documents, terms, unique_terms) = interner.into_documents(&NoopProgress).unwrap();
        let mut profiles = Vec::with_capacity(profile_documents.len());
        let mut anchors = Vec::with_capacity(profile_documents.len());
        let mut members = Vec::with_capacity(profile_documents.len());
        let mut chain_counts = Vec::with_capacity(profile_documents.len());
        for (profile_id, (&document, &chain)) in
            profile_documents.iter().zip(profile_chains).enumerate()
        {
            let document = document_ids[document];
            let mut value = profile(false, &[(0, document)]);
            value.anchor_start = anchors.len() as u32;
            value.member_start = members.len() as u32;
            value.chain_start = chain_counts.len() as u32;
            value.chain_mask = 1_u64 << usize::from(chain);
            anchors.push((0, document));
            members.push(MetadataMember {
                contract_id: profile_id as ContractId,
                nft_id: None,
            });
            chain_counts.push((chain, 1));
            profiles.push(value);
        }
        DirectIndex {
            documents,
            terms,
            document_context_weights: vec![1; values.len()].into_boxed_slice(),
            profiles,
            anchors,
            token_profile_counts: Box::new([]),
            members,
            chain_counts,
            chain_count: profile_chains
                .iter()
                .copied()
                .map(usize::from)
                .max()
                .map_or(0, |chain| chain + 1),
            query_profile_count: profile_documents.len(),
            eligible_contracts: profile_documents.len() as u64,
            eligible_members: profile_documents.len() as u64,
            anchor_count: profile_documents.len() as u64,
            unique_terms,
            image_witnesses: None,
        }
    }

    fn score_test_candidate_postings(
        index: &DirectIndex,
        sources: &[&[u32]],
        initial_unchecked: u64,
        progress: &dyn ProgressObserver,
    ) -> Result<(CandidateScoreSummary, bool, u64), DedupError> {
        let hits = ProfileHits::new(index.profiles.len(), index.chain_count, false);
        let mut scorer = WorkerScorer::new(index, &hits, 0.99);
        let mut left = IndexedLeftContext::new(index, &hits, 0);
        let mut seen = None;
        let mut summary = CandidateScoreSummary::default();
        let mut unchecked = initial_unchecked;
        let mut postings = CandidatePostingConsumer::new(
            true,
            &mut left,
            &mut seen,
            index.profiles.len(),
            &mut scorer,
            &mut summary,
            &mut unchecked,
            progress,
        );
        for &source in sources {
            postings.push(PostingView::Sparse(source))?;
        }
        postings.finish()?;
        Ok((summary, seen.is_some(), unchecked))
    }

    #[test]
    fn owned_candidate_generation_deduplicates_and_checks_cancellation() {
        let entries = vec![(7, 1), (7, 1), (7, 2)];
        let mut seen = CandidateSeen::new(3);
        seen.begin_profile(0);
        let mut pairs = CandidatePairChunks::new();
        let mut scoring_work = 0;
        let mut emissions = 0;
        let mut zero_overlap_prunes = 0;
        let mut unchecked = 0;
        append_owned_candidates(
            &entries,
            0,
            |entry| entry.1,
            |_| 1,
            |right| Some(CandidatePair::new(0, right, 0, right)),
            &mut seen,
            &mut pairs,
            &mut scoring_work,
            &mut emissions,
            &mut zero_overlap_prunes,
            &mut unchecked,
            &NoopProgress,
        )
        .unwrap();
        let (pairs, pair_count) = pairs.finish();
        assert_eq!(pair_count, 2);
        assert_eq!(
            pairs
                .iter()
                .flatten()
                .map(|candidate| candidate.profile_key)
                .collect::<Vec<_>>(),
            vec![profile_pair_key(0, 1), profile_pair_key(0, 2)]
        );
        assert_eq!(emissions, 3);
        assert_eq!(zero_overlap_prunes, 0);
        assert_eq!(scoring_work, 2);

        let mut seen = CandidateSeen::new(3);
        seen.begin_profile(0);
        let mut unchecked = CANDIDATE_CANCEL_BATCH - 1;
        let mut scoring_work = 0;
        assert!(matches!(
            append_owned_candidates(
                &entries[..2],
                0,
                |entry| entry.1,
                |_| 1,
                |right| Some(CandidatePair::new(0, right, 0, right)),
                &mut seen,
                &mut CandidatePairChunks::new(),
                &mut scoring_work,
                &mut 0,
                &mut 0,
                &mut unchecked,
                &CancelledProgress,
            ),
            Err(DedupError::Interrupted)
        ));
    }

    #[test]
    fn candidate_pairs_stay_in_bounded_zero_copy_score_chunks() {
        let expected_len = CANDIDATE_PAIR_CHUNK * 2 + 7;
        let mut builder = CandidatePairChunks::new();
        for pair in 0..expected_len as u64 {
            builder.push(CandidatePair {
                profile_key: pair,
                document_key: pair,
            });
        }
        let (chunks, len) = builder.finish();
        let pairs = IndexedPairs::new(chunks, len);
        assert_eq!(pairs.len(), expected_len);
        assert!(
            pairs
                .chunks
                .iter()
                .all(|chunk| chunk.len() <= CANDIDATE_PAIR_CHUNK)
        );
        assert_eq!(
            pairs
                .iter()
                .map(|candidate| candidate.profile_key)
                .collect::<Vec<_>>(),
            (0..expected_len as u64).collect::<Vec<_>>()
        );
    }

    #[test]
    fn compact_posting_deduplicates_profiles_and_drops_singletons() {
        let posting = CompactPosting::from_pairs(vec![(1, 2), (1, 2), (1, 3), (2, 3)]);

        assert_eq!(posting.keys.as_ref(), &[1]);
        assert_eq!(posting.offsets.as_ref(), &[0, 2]);
        assert_eq!(posting.profiles.as_ref(), &[2, 3]);
        assert_eq!(posting.posting_after(1, 0).to_vec(), &[2, 3]);
        assert_eq!(posting.posting_after(1, 2).to_vec(), &[3]);
        assert!(posting.posting_after(2, 0).is_empty());
    }

    #[test]
    fn dense_posting_round_trips_every_profile_and_sparse_tail() {
        let entries = (0..DENSE_POSTING_MIN_PROFILES as u32 * 2)
            .map(|profile| (7, profile))
            .collect::<Vec<_>>();
        let posting = CompactPosting::from_pairs(entries);

        assert!(posting.profiles.is_empty());
        assert_eq!(posting.dense_postings.len(), 1);
        assert_eq!(posting.logical_len, (DENSE_POSTING_MIN_PROFILES * 2) as u64);
        assert_eq!(
            posting.posting_after(7, 1_023).to_vec(),
            (1_024..DENSE_POSTING_MIN_PROFILES as u32 * 2).collect::<Vec<_>>()
        );
    }

    #[test]
    fn conservative_dense_threshold_keeps_smaller_postings_sparse() {
        let entries = (0..DENSE_POSTING_MIN_PROFILES as u32 - 1)
            .map(|profile| (7, profile))
            .collect::<Vec<_>>();
        let posting = CompactPosting::from_pairs(entries);

        assert!(posting.dense_postings.is_empty());
        assert_eq!(posting.profiles.len(), DENSE_POSTING_MIN_PROFILES - 1);
    }

    #[test]
    fn candidate_seen_clears_only_touched_words_and_merges_dense_words() {
        let mut seen = CandidateSeen::new(192);
        seen.begin_profile(0);
        assert!(seen.insert(1));
        assert!(!seen.insert(1));
        assert_eq!(seen.insert_word(1, 0b1_1010), 0b1_1010);
        assert_eq!(seen.insert_word(1, 0b1_0010), 0);
        assert_eq!(seen.touched_words, &[0, 1]);

        seen.begin_profile(64);
        assert!(seen.touched_words.is_empty());
        assert!(seen.words.iter().all(|word| *word == 0));
        assert_eq!(seen.insert_word(2, 1_u64 << 63), 1_u64 << 63);
    }

    #[test]
    fn dense_scoring_fuses_emission_counting_with_seen_consumption() {
        let candidate_count = DENSE_POSTING_MIN_PROFILES * 2;
        let profile_documents = vec![0; candidate_count + 1];
        let profile_chains = vec![0; candidate_count + 1];
        let index = indexed_scoring_index(&["shared"], &profile_documents, &profile_chains);
        let posting = CompactPosting::from_pairs(
            (1..=candidate_count as u32)
                .map(|profile| (7, profile))
                .collect(),
        );
        let posting = posting.posting_after(7, 0);
        assert!(matches!(posting, PostingView::Dense { .. }));

        let hits = ProfileHits::new(index.profiles.len(), index.chain_count, false);
        let mut scorer = WorkerScorer::new(&index, &hits, 0.99);
        let mut left = IndexedLeftContext::new(&index, &hits, 0);
        let mut seen = CandidateSeen::new(index.profiles.len());
        seen.begin_profile(0);
        for profile in (2..=candidate_count as u32).step_by(2) {
            assert!(seen.insert(profile));
        }
        let mut summary = CandidateScoreSummary::default();
        let mut unchecked = CANDIDATE_CANCEL_BATCH - 10;
        let progress = CancelAfterChecks::new(1);

        score_seen_candidate_posting(
            posting,
            true,
            &mut left,
            &mut seen,
            &mut scorer,
            &mut summary,
            &mut unchecked,
            &progress,
        )
        .unwrap();

        assert_eq!(summary.pair_emissions, candidate_count as u64);
        assert_eq!(summary.pair_count, (candidate_count / 2) as u64);
        assert_eq!(unchecked, candidate_count as u64 - 10);
        assert_eq!(progress.checks.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn token_posting_batch_lookup_matches_individual_queries_for_both_strategies() {
        let mut entries = Vec::new();
        for second in 0..32_u32 {
            entries.push((7, second, 1));
            entries.push((7, second, 5));
        }
        entries.extend([(9, 1, 2), (9, 1, 6)]);
        let posting = CompactPosting::from_triples(entries);
        let mut ranges = vec![TokenPostingRanges::default(); 10];
        record_token_key_ranges(&posting.keys, &mut ranges, |candidate| &mut candidate.full);
        let token = posting.for_token(7, ranges[7].full);

        for requested in [
            vec![1, 17],
            vec![1, 1, 17],
            vec![0, 1, 16, 17, 40],
            (0..32_u32).filter(|second| second % 2 == 0).collect(),
        ] {
            let expected = requested
                .iter()
                .map(|&second| token.posting_after(second, 1).to_vec())
                .filter(|candidate| !candidate.is_empty())
                .collect::<Vec<_>>();
            let mut actual = Vec::new();
            token.visit_postings_after(&requested, 1, |candidate| {
                actual.push(candidate.to_vec());
            });
            assert_eq!(actual, expected);
        }

        let missing = posting.for_token(8, ranges[8].full);
        assert!(missing.posting_after(1, 0).is_empty());
        let mut visited = false;
        missing.visit_postings_after(&[1], 0, |_| visited = true);
        assert!(!visited);
    }

    #[test]
    fn dense_global_postings_match_the_profile_reference_and_tail() {
        let evm = ["ethereum".to_owned()].into_iter().collect::<HashSet<_>>();
        let mut store = EntityStore::with_options(2, &evm.iter().cloned().collect());
        for (contract, metadata) in [
            (0, r#"{"shared":"alpha beta","side":"zero"}"#),
            (1, r#"{"shared":"alpha gamma","side":"one"}"#),
            (2, r#"{"shared":"beta delta","side":"two"}"#),
            (3, r#"{"shared":"alpha delta","side":"three"}"#),
        ] {
            store
                .try_ingest_row(input("ethereum", &format!("0x{contract:x}"), "1", metadata))
                .unwrap();
        }
        let index = build_index(&store, &evm, 2, &NoopProgress).unwrap();
        let counts =
            estimate_candidate_counts(&index, true, "candidate_admission", &NoopProgress).unwrap();
        let progress = PhaseProgress::default();
        let postings = build_global_full_index(&index, counts.global_full, &progress).unwrap();
        assert!(postings.len() <= counts.global_full);

        for term in 0..index.unique_terms {
            let expected = index
                .profiles
                .iter()
                .enumerate()
                .filter_map(|(profile, candidate)| {
                    index
                        .document_terms(candidate.max_document())
                        .binary_search_by_key(&(term as u32), |(candidate, _)| *candidate)
                        .is_ok()
                        .then_some(profile as u32)
                })
                .collect::<Vec<_>>();
            let term = term as usize;
            let expected = if expected.len() < 2 {
                &[][..]
            } else {
                expected.as_slice()
            };
            assert_eq!(
                &postings.profiles[postings.offsets[term]..postings.offsets[term + 1]],
                expected
            );
            for left in 0..index.profiles.len() as u32 {
                assert_eq!(
                    postings.posting_after(term as u32, left).to_vec(),
                    &expected[expected.partition_point(|profile| *profile <= left)..]
                );
            }
        }
        for phase in [
            "candidate_global_count",
            "candidate_global_offsets",
            "candidate_global_fill",
        ] {
            progress.assert_complete(phase);
        }
    }

    #[test]
    fn global_hybrid_postings_compact_dense_terms_without_moving_sparse_ids() {
        let dense_len = DENSE_POSTING_MIN_PROFILES * 2;
        let mut profiles = vec![3_u32, 7];
        profiles.extend(0..dense_len as u32);
        profiles.extend([11, 13, 17]);
        let offsets = vec![0, 2, 2 + dense_len, 2 + dense_len + 3];
        let progress = PhaseProgress::default();
        let postings = hybridize_global_postings(offsets, profiles, &progress).unwrap();

        assert_eq!(postings.posting_after(0, 0).to_vec(), [3, 7]);
        assert_eq!(
            postings.posting_after(1, 1_023).to_vec(),
            (1_024..dense_len as u32).collect::<Vec<_>>()
        );
        assert_eq!(postings.posting_after(2, 12).to_vec(), [13, 17]);
        assert_eq!(postings.dense_postings.len(), 1);
        progress.assert_complete("candidate_global_compress");
    }

    #[test]
    fn evm_selects_largest_shared_token() {
        let left = profile(true, &[(1, 10), (2, 20), (3, 30)]);
        let right = profile(true, &[(1, 11), (3, 31), (4, 41)]);
        assert_eq!(
            selected_documents(
                &left,
                &[(1, 10), (2, 20), (3, 30)],
                &right,
                &[(1, 11), (3, 31), (4, 41)]
            ),
            (30, 31)
        );
    }

    #[test]
    fn token_interner_remaps_parallel_ids_into_evm_numeric_order() {
        let interner = TokenInterner::new();
        let ten = interner.intern("10").unwrap();
        let text = interner.intern("token").unwrap();
        let two = interner.intern("2").unwrap();
        let zero = interner.intern("0").unwrap();

        let remap = interner.into_ordered_remap().unwrap();
        assert!(remap[zero as usize] < remap[two as usize]);
        assert!(remap[two as usize] < remap[ten as usize]);
        assert!(remap[ten as usize] < remap[text as usize]);
    }

    #[test]
    fn highest_shared_anchor_handles_imbalanced_lists_and_duplicate_tokens() {
        let short = [(3, 30), (9, 90)];
        let mut long = (0..=32)
            .map(|token| (token, token + 100))
            .collect::<Vec<_>>();
        long.insert(10, (9, 999));

        assert_eq!(highest_shared_anchor(&short, &long), Some((90, 999)));
        assert_eq!(highest_shared_anchor(&long, &short), Some((999, 90)));
    }

    #[test]
    fn imbalanced_anchor_search_matches_linear_reverse_merge() {
        let reference = |left: &[(u32, u32)], right: &[(u32, u32)]| {
            let mut left_end = left.len();
            let mut right_end = right.len();
            while left_end != 0 && right_end != 0 {
                let left_anchor = left[left_end - 1];
                let right_anchor = right[right_end - 1];
                match left_anchor.0.cmp(&right_anchor.0) {
                    std::cmp::Ordering::Equal => {
                        return Some((left_anchor.1, right_anchor.1));
                    }
                    std::cmp::Ordering::Greater => left_end -= 1,
                    std::cmp::Ordering::Less => right_end -= 1,
                }
            }
            None
        };

        for short_len in 0..16 {
            for offset in 0..17_u32 {
                let short = (0..short_len)
                    .map(|index| {
                        let token = index as u32 * 19 + offset;
                        (token, token ^ 0x55aa)
                    })
                    .collect::<Vec<_>>();
                let long = (0..257_u32)
                    .filter(|token| token % 3 != 1)
                    .map(|token| (token, token ^ 0xaa55))
                    .collect::<Vec<_>>();
                assert_eq!(
                    highest_shared_anchor_imbalanced(&short, &long),
                    reference(&short, &long)
                );
            }
        }
    }

    #[test]
    fn no_shared_token_uses_both_max_documents() {
        let left = profile(true, &[(1, 10), (2, 20)]);
        let right = profile(true, &[(3, 30), (4, 40)]);
        assert_eq!(
            selected_documents(&left, &[(1, 10), (2, 20)], &right, &[(3, 30), (4, 40)]),
            (20, 40)
        );
    }

    #[test]
    fn metadata_scope_contract_compaction_preserves_nft_count() {
        let mut members = MetadataScopeMembers {
            contracts: vec![3, 1, 3, 2, 1],
            duplicate_nft_count: 19,
        };

        members.dedup_contracts();
        assert_eq!(members.contracts, vec![1, 2, 3]);
        assert_eq!(members.duplicate_nft_count, 19);

        let counts = members.into_counts();
        assert_eq!(counts.duplicate_contract_count, 3);
        assert_eq!(counts.duplicate_nft_count, 19);
    }

    #[test]
    fn token_mask_collision_still_uses_exact_shared_token_scan() {
        let first = 1_u32;
        let collision = (first + 1..)
            .find(|candidate| token_bit(*candidate) == token_bit(first))
            .unwrap();
        let left = profile(true, &[(first, 10), (collision, 20)]);
        let right = profile(true, &[(collision, 21)]);
        assert_eq!(
            selected_documents(
                &left,
                &[(first, 10), (collision, 20)],
                &right,
                &[(collision, 21)]
            ),
            (20, 21)
        );
    }

    #[test]
    fn token_mask_collision_cannot_create_a_false_shared_token() {
        let first = 1_u32;
        let collision = (first + 1..)
            .find(|candidate| token_bit(*candidate) == token_bit(first))
            .unwrap();
        let left = profile(true, &[(first, 10)]);
        let right = profile(true, &[(collision, 20)]);
        assert_eq!(
            selected_documents(&left, &[(first, 10)], &right, &[(collision, 20)]),
            (10, 20)
        );
    }

    #[test]
    fn token_mask_selection_matches_exact_reference() {
        let exact = |left: &ContractProfile,
                     left_anchors: &[(TokenKeyId, DocumentId)],
                     right: &ContractProfile,
                     right_anchors: &[(TokenKeyId, DocumentId)]| {
            if left.is_evm && right.is_evm {
                for left_anchor in left_anchors.iter().rev() {
                    if let Some(right_anchor) = right_anchors
                        .iter()
                        .rev()
                        .find(|anchor| anchor.0 == left_anchor.0)
                    {
                        return (left_anchor.1, right_anchor.1);
                    }
                }
            }
            (left.max_document(), right.max_document())
        };
        for seed in 0..512_u32 {
            let mut left_anchors = (0..1 + seed as usize % INLINE_ANCHORS)
                .map(|index| {
                    (
                        seed.wrapping_mul(17)
                            .wrapping_add((index as u32).wrapping_mul(29))
                            % 97,
                        100 + index as u32,
                    )
                })
                .collect::<Vec<_>>();
            left_anchors.sort_by_key(|anchor| anchor.0);
            let mut right_anchors = (0..1 + (seed as usize / 3) % INLINE_ANCHORS)
                .map(|index| {
                    (
                        seed.wrapping_mul(31)
                            .wrapping_add((index as u32).wrapping_mul(13))
                            % 97,
                        200 + index as u32,
                    )
                })
                .collect::<Vec<_>>();
            right_anchors.sort_by_key(|anchor| anchor.0);
            for (left_evm, right_evm) in [(true, true), (true, false), (false, true)] {
                let left = profile(left_evm, &left_anchors);
                let right = profile(right_evm, &right_anchors);
                assert_eq!(
                    selected_documents(&left, &left_anchors, &right, &right_anchors),
                    exact(&left, &left_anchors, &right, &right_anchors)
                );
            }
        }
    }

    #[test]
    fn non_evm_profile_only_depends_on_max_anchor() {
        let contract = Contract {
            id: 0,
            chain_id: 0,
            address: "other".to_owned(),
            nft_count: 2,
            metadata_by_token: vec![
                record("A", r#"{"name":"old"}"#),
                record("Z", r#"{"name":"max"}"#),
            ],
        };
        assert_eq!(contract.metadata_by_token.last().unwrap().token_id, "Z");
        let profile = profile(false, &[(0, 7)]);
        assert_eq!(profile.max_document(), 7);
    }

    #[test]
    fn equivalent_evm_tokens_ignore_leading_zeroes() {
        assert_eq!(normalized_evm_token("00010"), normalized_evm_token("10"));
        assert_eq!(normalized_evm_token("000"), normalized_evm_token("0"));
    }

    #[test]
    fn tile_coordinates_cover_upper_triangle_once() {
        for axis in 1..32_u64 {
            let tile_count = axis * (axis + 1) / 2;
            let coordinates = (0..tile_count)
                .map(|ordinal| tile_coordinates(ordinal, axis))
                .collect::<std::collections::HashSet<_>>();
            assert_eq!(coordinates.len() as u64, tile_count);
            assert!(
                coordinates
                    .iter()
                    .all(|&(left, right)| left <= right && right < axis)
            );
        }
    }

    #[test]
    fn tile_coordinates_schedule_diagonal_before_wider_gaps() {
        let axis = 5;
        assert_eq!(
            (0..axis)
                .map(|ordinal| tile_coordinates(ordinal, axis))
                .collect::<Vec<_>>(),
            vec![(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)]
        );
        assert_eq!(tile_coordinates(axis, axis), (0, 1));
    }

    #[test]
    fn upper_rect_tiles_cover_a_rectangular_upper_triangle() {
        let left_axis = 2;
        let right_axis = 5;
        let coordinates = (0..upper_rect_tile_count(left_axis, right_axis))
            .map(|index| upper_rect_tile_coordinate(index, left_axis, right_axis))
            .collect::<Vec<_>>();
        assert_eq!(
            coordinates,
            vec![
                (0, 0),
                (0, 1),
                (0, 2),
                (0, 3),
                (0, 4),
                (1, 1),
                (1, 2),
                (1, 3),
                (1, 4),
            ]
        );
        assert!(
            coordinates
                .iter()
                .all(|&(left, right)| left < left_axis && right < right_axis && left <= right)
        );
    }

    #[test]
    fn tile_coordinate_cursor_matches_random_access_mapping() {
        for axis in 1..32_u64 {
            let tile_count = axis * (axis + 1) / 2;
            for start in 0..tile_count {
                let mut cursor = TileCoordinateCursor::new(start, axis);
                for ordinal in start..tile_count.min(start + MAX_SCORE_TILE_BATCH) {
                    assert_eq!(cursor.next(), tile_coordinates(ordinal, axis));
                }
            }
        }
    }

    #[test]
    fn upper_rect_tile_coordinate_cursor_matches_random_access_mapping() {
        for left_axis in 1..32_u64 {
            for right_axis in left_axis..40_u64 {
                let tile_count = upper_rect_tile_count(left_axis, right_axis);
                for start in 0..tile_count {
                    let mut cursor =
                        UpperRectTileCoordinateCursor::new(start, left_axis, right_axis);
                    for ordinal in start..tile_count.min(start + MAX_SCORE_TILE_BATCH) {
                        assert_eq!(
                            cursor.next(),
                            upper_rect_tile_coordinate(ordinal, left_axis, right_axis)
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn interleaved_profile_schedule_is_complete_and_spreads_early_work() {
        let profile_count = 1_003;
        let stripes = 8;
        let schedule = InterleavedProfileSchedule::new(profile_count, stripes);
        let scheduled = (0..schedule.slots)
            .filter_map(|slot| schedule.profile(slot))
            .collect::<Vec<_>>();

        let mut sorted = scheduled.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, (0..profile_count).collect::<Vec<_>>());
        assert_eq!(scheduled.len(), profile_count);
        assert_eq!(
            &scheduled[..stripes],
            &[0, 126, 252, 378, 504, 630, 756, 882]
        );
    }

    #[test]
    fn interleaved_profile_schedule_handles_empty_and_short_inputs() {
        let empty = InterleavedProfileSchedule::new(0, 192);
        assert_eq!(empty.slots, 0);
        assert_eq!(empty.profile(0), None);

        let short = InterleavedProfileSchedule::new(3, 192);
        assert_eq!(short.slots, 3);
        assert_eq!(
            (0..short.slots)
                .filter_map(|slot| short.profile(slot))
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
    }

    #[test]
    fn indexed_scorer_recomputes_repeated_document_pairs_without_cache() {
        let index = indexed_scoring_index(&["shared alpha", "shared beta"], &[0, 0, 1], &[0, 0, 1]);
        let hits = ProfileHits::new(index.profiles.len(), index.chain_count, false);
        let mut scorer = WorkerScorer::new(&index, &hits, 0.99);

        let mut first_left = IndexedLeftContext::new(&index, &hits, 0);
        assert!(scorer.score_indexed_candidate(&mut first_left, 2, true));
        let mut second_left = IndexedLeftContext::new(&index, &hits, 1);
        assert!(scorer.score_indexed_candidate(&mut second_left, 2, true));
        assert_eq!(scorer.local_stats.bm25_cache_probes, 0);
        assert_eq!(scorer.local_stats.bm25_cache_hits, 0);
        assert_eq!(scorer.local_stats.bm25_cache_bypassed_pairs, 2);
        assert_eq!(scorer.local_stats.bm25_scores, 2);
    }

    #[test]
    fn indexed_emission_batches_preserve_raw_and_unique_counts() {
        let index = indexed_scoring_index(&["shared alpha", "shared beta"], &[0, 1], &[0, 1]);
        let hits = ProfileHits::new(index.profiles.len(), index.chain_count, false);
        let mut scorer = WorkerScorer::new(&index, &hits, 0.99);
        let mut left = IndexedLeftContext::new(&index, &hits, 0);
        let posting = [1_u32, 1_u32];
        let mut postings = CandidatePostingSlices::default();
        postings.push(PostingView::Sparse(&posting));
        let mut seen = None;
        let mut summary = CandidateScoreSummary::default();
        let mut unchecked = CANDIDATE_CANCEL_BATCH - 1;
        let progress = CancelAfterChecks::new(1);
        score_candidate_postings(
            &postings,
            true,
            &mut left,
            &mut seen,
            index.profiles.len(),
            &mut scorer,
            &mut summary,
            &mut unchecked,
            &progress,
        )
        .unwrap();
        assert_eq!(summary.pair_emissions, 2);
        assert_eq!(summary.pair_count, 1);
        assert_eq!(summary.zero_overlap_prunes, 0);
        assert_eq!(unchecked, 1);
        assert_eq!(progress.checks.load(Ordering::Relaxed), 1);
        assert!(seen.is_none());
    }

    #[test]
    fn two_posting_union_handles_overlap_and_disjoint_ranges_without_seen_storage() {
        let index = indexed_scoring_index(&["shared"], &[0, 0, 0, 0, 0], &[0, 1, 2, 3, 4]);
        let overlap_first = [1, 2, 4];
        let overlap_second = [2, 3, 4];
        let (overlap, allocated_seen, _) = score_test_candidate_postings(
            &index,
            &[&overlap_first, &overlap_second],
            0,
            &NoopProgress,
        )
        .unwrap();
        assert_eq!(overlap.pair_emissions, 6);
        assert_eq!(overlap.pair_count, 4);
        assert_eq!(overlap.zero_overlap_prunes, 0);
        assert!(!allocated_seen);

        let disjoint_first = [1, 2];
        let disjoint_second = [3, 4];
        let (disjoint, allocated_seen, _) = score_test_candidate_postings(
            &index,
            &[&disjoint_first, &disjoint_second],
            0,
            &NoopProgress,
        )
        .unwrap();
        assert_eq!(disjoint.pair_emissions, 4);
        assert_eq!(disjoint.pair_count, 4);
        assert_eq!(disjoint.zero_overlap_prunes, 0);
        assert!(!allocated_seen);
    }

    #[test]
    fn two_posting_union_batches_emissions_across_the_exact_cancel_boundary() {
        let index = indexed_scoring_index(&["shared"], &[0, 0], &[0, 1]);
        let hits = ProfileHits::new(index.profiles.len(), index.chain_count, false);
        let mut scorer = WorkerScorer::new(&index, &hits, 0.99);
        let mut left = IndexedLeftContext::new(&index, &hits, 0);
        let first = [1_u32];
        let second = [1_u32];
        let mut postings = CandidatePostingSlices::default();
        postings.push(PostingView::Sparse(&first));
        postings.push(PostingView::Sparse(&second));
        let mut seen = None;
        let mut summary = CandidateScoreSummary::default();
        let mut unchecked = CANDIDATE_CANCEL_BATCH - 1;
        let progress = CancelAfterChecks::new(1);

        score_candidate_postings(
            &postings,
            true,
            &mut left,
            &mut seen,
            index.profiles.len(),
            &mut scorer,
            &mut summary,
            &mut unchecked,
            &progress,
        )
        .unwrap();

        assert_eq!(summary.pair_emissions, 2);
        assert_eq!(summary.pair_count, 1);
        assert_eq!(unchecked, 1);
        assert_eq!(progress.checks.load(Ordering::Relaxed), 1);
        assert!(seen.is_none());
    }

    #[test]
    fn candidate_posting_union_allocates_seen_only_at_the_k2_k3_boundary() {
        let index = indexed_scoring_index(&["shared"], &[0, 0, 0, 0, 0], &[0, 1, 2, 3, 4]);
        let first = [1, 3];
        let second = [2, 3];
        let third = [1, 4];

        let (two, allocated_seen, _) =
            score_test_candidate_postings(&index, &[&first, &second], 0, &NoopProgress).unwrap();
        assert_eq!(two.pair_emissions, 4);
        assert_eq!(two.pair_count, 3);
        assert!(!allocated_seen);

        let (three, allocated_seen, _) =
            score_test_candidate_postings(&index, &[&first, &second, &third], 0, &NoopProgress)
                .unwrap();
        assert_eq!(three.pair_emissions, 6);
        assert_eq!(three.pair_count, 4);
        assert!(allocated_seen);
    }

    #[test]
    fn candidate_posting_union_matches_seen_reference_for_random_sorted_sources() {
        const PROFILE_COUNT: usize = 32;
        let profile_documents = vec![0; PROFILE_COUNT];
        let profile_chains = (0..PROFILE_COUNT)
            .map(|profile| (profile % 8) as ChainId)
            .collect::<Vec<_>>();
        let index = indexed_scoring_index(&["shared"], &profile_documents, &profile_chains);
        let mut state = 0x9e37_79b9_u32;
        for source_count in 1..=6 {
            for _ in 0..32 {
                let mut sources = Vec::with_capacity(source_count);
                for _ in 0..source_count {
                    state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                    let len = (state as usize >> 27) % 12;
                    let mut source = Vec::with_capacity(len);
                    for _ in 0..len {
                        state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                        source.push(1 + state % (PROFILE_COUNT as u32 - 1));
                    }
                    source.sort_unstable();
                    sources.push(source);
                }
                let source_refs = sources.iter().map(Vec::as_slice).collect::<Vec<_>>();
                let raw_emissions = sources.iter().map(Vec::len).sum::<usize>() as u64;
                let nonempty_sources = sources.iter().filter(|source| !source.is_empty()).count();
                let mut reference = CandidateSeen::new(PROFILE_COUNT);
                reference.begin_profile(0);
                let mut unique = 0_u64;
                for source in &sources {
                    for &right in source {
                        unique += u64::from(reference.insert(right));
                    }
                }

                let (actual, allocated_seen, _) =
                    score_test_candidate_postings(&index, &source_refs, 0, &NoopProgress).unwrap();
                assert_eq!(actual.pair_emissions, raw_emissions);
                assert_eq!(actual.pair_count, unique);
                assert_eq!(actual.zero_overlap_prunes, 0);
                assert_eq!(allocated_seen, nonempty_sources >= 3);
            }
        }
    }

    #[test]
    fn candidate_posting_union_checks_cancellation_at_the_exact_batch_boundary() {
        let index = indexed_scoring_index(&["shared"], &[0, 0], &[0, 1]);
        let posting = [1];
        let result = score_test_candidate_postings(
            &index,
            &[&posting],
            CANDIDATE_CANCEL_BATCH - 1,
            &CancelledProgress,
        );
        assert!(matches!(result, Err(DedupError::Interrupted)));
    }

    #[test]
    fn non_evm_posting_witness_matches_regular_bm25_for_shared_and_mask_collision_inputs() {
        let first = 1_u32;
        let collision = (first + 1..)
            .find(|candidate| {
                let bit = |term: u32| {
                    (u64::from(term).wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 57) as usize
                };
                bit(*candidate) == bit(first)
            })
            .unwrap();
        let left_collision = PreparedDocument::try_new("left tail", |term| {
            Ok::<_, ()>(if term == "left" { first } else { collision + 1 })
        })
        .unwrap();
        let right_collision =
            PreparedDocument::try_new("right", |_| Ok::<_, ()>(collision)).unwrap();
        assert!(may_share_term(
            &left_collision.document,
            &left_collision.terms,
            &right_collision.document,
            &right_collision.terms,
        ));
        assert!(
            left_collision
                .terms
                .iter()
                .all(|left| right_collision.terms.iter().all(|right| left.0 != right.0))
        );
        let ordinary_collision = similarity_at_least(
            &left_collision.document,
            &left_collision.terms,
            &right_collision.document,
            &right_collision.terms,
            0.5,
        );
        let witnessed_collision = similarity_at_least_after_overlap_filter(
            &left_collision.document,
            &left_collision.terms,
            &right_collision.document,
            &right_collision.terms,
            0.5,
        );
        assert_eq!(ordinary_collision, witnessed_collision);

        let index = indexed_scoring_index(&["shared alpha", "shared beta"], &[0, 1], &[0, 1]);
        let left_document = index.profiles[0].max_document();
        let right_document = index.profiles[1].max_document();
        let mut ordinary_stats = LocalStats::default();
        let ordinary = score_document_pair(
            &index,
            left_document,
            right_document,
            0.1,
            false,
            &mut ordinary_stats,
        );
        let mut witnessed_stats = LocalStats::default();
        let witnessed = score_document_pair(
            &index,
            left_document,
            right_document,
            0.1,
            true,
            &mut witnessed_stats,
        );
        assert_eq!(ordinary, witnessed);
        assert_eq!(ordinary_stats.bm25_zero_overlap_prunes, 0);
        assert_eq!(witnessed_stats.bm25_zero_overlap_prunes, 0);

        let hits = ProfileHits::new(index.profiles.len(), index.chain_count, false);
        let mut scorer = WorkerScorer::new(&index, &hits, 0.1);
        let mut left = IndexedLeftContext::new(&index, &hits, 0);
        assert!(!left.profile.is_evm);
        assert!(scorer.score_indexed_candidate(&mut left, 1, true));
        assert_eq!(scorer.local_stats.bm25_scores, 1);
    }

    #[test]
    fn every_non_double_evm_direction_uses_cached_max_documents_without_right_anchors() {
        for (left_evm, right_evm) in [(true, false), (false, true), (false, false)] {
            let mut index = indexed_scoring_index(
                &["global shared left", "global shared right", "token only"],
                &[0, 1],
                &[0, 1],
            );
            if left_evm {
                index.anchors = vec![(7, 2), (11, 0)];
                let left_profile = &mut index.profiles[0];
                left_profile.is_evm = true;
                left_profile.is_solana = false;
                left_profile.anchor_start = 0;
                left_profile.anchor_len = 2;
                left_profile.max_document = 0;
                left_profile.token_mask =
                    index
                        .anchors
                        .iter()
                        .fold([0; TOKEN_MASK_WORDS], |mut mask, (token, _)| {
                            let (word, bit) = token_bit(*token);
                            mask[word] |= bit;
                            mask
                        });
            } else {
                let left_profile = &mut index.profiles[0];
                left_profile.is_evm = false;
                left_profile.is_solana = true;
                left_profile.anchor_start = u32::MAX;
                left_profile.anchor_len = 1;
                left_profile.max_document = 0;
            }
            let right_profile = &mut index.profiles[1];
            right_profile.is_evm = right_evm;
            right_profile.is_solana = !right_evm;
            right_profile.anchor_start = u32::MAX;
            right_profile.anchor_len = 1;
            right_profile.max_document = 1;

            let mut expected_stats = LocalStats::default();
            let expected = score_document_pair(&index, 0, 1, 0.1, true, &mut expected_stats);
            let hits = ProfileHits::new(index.profiles.len(), index.chain_count, false);
            let mut left = IndexedLeftContext::new(&index, &hits, 0);
            assert_eq!(left.max_document, 0);
            if !left_evm {
                assert!(left.anchors.is_empty());
            }
            let mut scorer = WorkerScorer::new(&index, &hits, 0.1);

            assert!(scorer.score_indexed_candidate(&mut left, 1, true));
            assert_eq!(scorer.local_stats.bm25_scores, 1);
            assert_eq!(
                scorer.local_stats.bm25_upper_bound_prunes,
                expected_stats.bm25_upper_bound_prunes
            );
            assert_eq!(
                scorer.local_stats.matched_profile_pairs,
                u64::from(expected)
            );
        }
    }

    #[test]
    fn evm_indexed_candidate_scores_selected_documents_without_a_pre_overlap_scan() {
        let mut index = indexed_scoring_index(
            &[
                "lefttokenonly",
                "global shared left",
                "righttokenonly",
                "global shared right",
            ],
            &[1, 3],
            &[0, 1],
        );
        index.anchors = vec![(7, 0), (11, 1), (7, 2), (12, 3)];
        for (profile_id, anchor_start) in [0_u32, 2].into_iter().enumerate() {
            let profile = &mut index.profiles[profile_id];
            profile.is_evm = true;
            profile.anchor_start = anchor_start;
            profile.anchor_len = 2;
            profile.max_document = index.anchors[anchor_start as usize + 1].1;
            profile.token_mask = index.anchors[anchor_start as usize..anchor_start as usize + 2]
                .iter()
                .fold([0; TOKEN_MASK_WORDS], |mut mask, (token, _)| {
                    let (word, bit) = token_bit(*token);
                    mask[word] |= bit;
                    mask
                });
        }
        let (selected_left, selected_right) = selected_documents(
            &index.profiles[0],
            index.anchors(&index.profiles[0]),
            &index.profiles[1],
            index.anchors(&index.profiles[1]),
        );
        assert_eq!((selected_left, selected_right), (0, 2));
        assert!(!may_share_term(
            &index.documents[selected_left as usize],
            index.document_terms(selected_left),
            &index.documents[selected_right as usize],
            index.document_terms(selected_right),
        ));
        let hits = ProfileHits::new(index.profiles.len(), index.chain_count, false);
        hits.insert_mask(0, index.profiles[1].chain_mask);
        hits.insert_mask(1, index.profiles[0].chain_mask);
        let mut scorer = WorkerScorer::new(&index, &hits, 0.1);
        let mut left = IndexedLeftContext::new(&index, &hits, 0);
        assert!(scorer.score_indexed_candidate(&mut left, 1, true));
        assert_eq!(scorer.local_stats.saturated_profile_pairs, 0);
        assert_eq!(scorer.local_stats.bm25_scores, 1);
        assert_eq!(scorer.local_stats.matched_profile_pairs, 0);
    }

    #[test]
    fn indexed_path_does_not_pay_a_per_pair_saturation_check() {
        let index = indexed_scoring_index(&["shared alpha", "shared beta"], &[0, 1], &[0, 1]);
        let hits = ProfileHits::new(index.profiles.len(), index.chain_count, false);
        let mut left = IndexedLeftContext::new(&index, &hits, 0);
        assert_eq!(left.known_hit_mask, Some(0));
        hits.insert_mask(0, index.profiles[1].chain_mask);
        hits.insert_mask(1, index.profiles[0].chain_mask);

        let mut scorer = WorkerScorer::new(&index, &hits, 2.0);
        assert!(scorer.score_indexed_candidate(&mut left, 1, true));
        assert_eq!(left.known_hit_mask, Some(0));
        assert_eq!(scorer.local_stats.saturated_profile_pairs, 0);
        assert_eq!(scorer.local_stats.bm25_scores, 1);

        let mut current_left = IndexedLeftContext::new(&index, &hits, 0);
        let mut current_scorer = WorkerScorer::new(&index, &hits, 2.0);
        assert!(current_scorer.score_indexed_candidate(&mut current_left, 1, true));
        assert_eq!(current_scorer.local_stats.saturated_profile_pairs, 0);
        assert_eq!(current_scorer.local_stats.bm25_scores, 1);
    }

    #[test]
    fn profile_chain_mask_fast_path_matches_wide_fallback() {
        let mut narrow_profile = profile(true, &[(1, 1)]);
        narrow_profile.chain_mask = (1_u64 << 1) | (1_u64 << 3);
        let narrow_chains = [(1, 1), (3, 1)];
        let narrow_hits = ProfileHits::new(1, 4, false);
        assert!(narrow_hits.block_unsatisfied.is_none());
        assert!(!narrow_hits.contains_profile_chains(0, &narrow_profile, &narrow_chains));
        narrow_hits.insert_profile_chains(0, &narrow_profile, &narrow_chains);
        assert!(narrow_hits.contains_profile_chains(0, &narrow_profile, &narrow_chains));

        let wide_profile = profile(true, &[(1, 1)]);
        let wide_chains = [(1, 1), (65, 1)];
        let wide_hits = ProfileHits::new(1, 66, false);
        assert!(!wide_hits.contains_profile_chains(0, &wide_profile, &wide_chains));
        wide_hits.insert_profile_chains(0, &wide_profile, &wide_chains);
        assert!(wide_hits.contains_profile_chains(0, &wide_profile, &wide_chains));
    }

    #[test]
    fn narrow_hit_storage_updates_block_saturation_once_per_chain() {
        let hits = ProfileHits::new(4, 2, true);
        assert!(matches!(&hits.words, HitWords::Single(_)));
        assert!(!hits.block_contains_mask(0, 0b11));
        for profile in 0..4 {
            hits.insert_mask(profile, 0b11);
            hits.insert_mask(profile, 0b11);
        }
        assert!(hits.block_contains_mask(0, 0b11));
        assert_eq!(hits.profile_mask(3), Some(0b11));
    }

    #[test]
    fn compact_profile_header_stays_bounded() {
        assert!(std::mem::size_of::<ContractProfile>() <= 72);
        assert!(std::mem::size_of::<RawSolanaProfile>() < std::mem::size_of::<RawProfile>());
    }

    #[test]
    fn large_solana_profile_bucket_reports_progress_and_checks_cancellation() {
        let bucket = || {
            (0..=PREPARE_BATCH)
                .map(|member| RawSolanaProfile {
                    document: 7,
                    member: MetadataMember {
                        contract_id: member as ContractId,
                        nft_id: Some(member as NftId),
                    },
                    chain_id: 0,
                })
                .collect::<Vec<_>>()
        };
        let progress = PhaseProgress::default();
        progress.begin_phase("solana_bucket", Some((PREPARE_BATCH + 1) as u64));
        let (profiles, anchors, members, chain_counts) =
            build_solana_profile_bucket(bucket(), &progress).unwrap();
        assert_eq!(profiles.len(), 1);
        assert_eq!(profiles[0].member_len as usize, PREPARE_BATCH + 1);
        assert_eq!(anchors, vec![(0, 7)]);
        assert_eq!(members.len(), PREPARE_BATCH + 1);
        assert_eq!(chain_counts, vec![(0, (PREPARE_BATCH + 1) as u32)]);
        progress.assert_complete("solana_bucket");

        assert!(matches!(
            build_solana_profile_bucket(bucket(), &CancelAfterChecks::new(1)),
            Err(DedupError::Interrupted)
        ));
    }

    fn input(chain: &str, address: &str, token_id: &str, metadata: &str) -> InputRow {
        InputRow {
            chain: chain.to_owned(),
            contract_address: address.to_owned(),
            token_id: token_id.to_owned(),
            name_norm: String::new(),
            token_uri_norm: String::new(),
            image_uri_norm: String::new(),
            metadata_json: metadata.to_owned(),
            source_order: SourceOrder {
                file_ordinal: 0,
                file_row_number: 0,
            },
        }
    }

    fn input_with_image(
        chain: &str,
        address: &str,
        token_id: &str,
        image_uri: &str,
        metadata: &str,
    ) -> InputRow {
        let mut row = input(chain, address, token_id, metadata);
        row.image_uri_norm = image_uri.to_owned();
        row
    }

    #[test]
    fn image_samples_use_the_selected_metadata_token_and_require_both_uris() {
        let evm = HashSet::from(["ethereum".to_owned()]);
        let mut store = EntityStore::with_options(8, &evm.iter().cloned().collect());
        for address in ["0xa", "0xb", "0xc"] {
            store
                .try_ingest_row(input_with_image(
                    "ethereum",
                    address,
                    "1",
                    "",
                    r#"{"token":"one"}"#,
                ))
                .unwrap();
            let image = if address == "0xc" {
                String::new()
            } else {
                format!("https://images.example/{address}.png")
            };
            store
                .try_ingest_row(input_with_image(
                    "ethereum",
                    address,
                    "2",
                    &image,
                    r#"{"token":"two"}"#,
                ))
                .unwrap();
        }
        let mut acc = SummaryAccumulator::default();
        let (_, contract_samples, image_samples) = run_direct_releasing_with_samples(
            &mut store,
            &evm,
            8,
            1.0,
            &mut acc,
            &NoopProgress,
            10,
        )
        .unwrap();

        assert_eq!(contract_samples.all_chains.len(), 1);
        assert_eq!(image_samples.len(), 1);
        assert_eq!(
            contract_samples.all_chains[0].contract_a_address,
            image_samples[0].contract_a_address
        );
        assert_eq!(
            contract_samples.all_chains[0].contract_b_address,
            image_samples[0].contract_b_address
        );
        assert_eq!(image_samples[0].token_id_a, "2");
        assert_eq!(image_samples[0].token_id_b, "2");
        assert!(image_samples[0].image_uri_a.contains("images.example"));
        assert!(image_samples[0].image_uri_b.contains("images.example"));
        assert_eq!(image_samples[0].metadata_json_a, r#"{"token":"two"}"#);
        assert_eq!(image_samples[0].metadata_json_b, r#"{"token":"two"}"#);
        assert_ne!(
            image_samples[0].contract_a_address,
            image_samples[0].contract_b_address
        );
    }

    #[test]
    fn direct_run_preserves_intra_and_cross_chain_membership() {
        let evm = ["ethereum".to_owned(), "base".to_owned()]
            .into_iter()
            .collect::<HashSet<_>>();
        let mut store = EntityStore::with_options(8, &evm.iter().cloned().collect());
        let same = r#"{"collection":"same","name":"token one"}"#;
        store
            .try_ingest_row(input("ethereum", "0xa", "1", same))
            .unwrap();
        store
            .try_ingest_row(input("ethereum", "0xb", "1", same))
            .unwrap();
        store
            .try_ingest_row(input("base", "0xc", "1", same))
            .unwrap();
        store
            .try_ingest_row(input(
                "ethereum",
                "0xd",
                "1",
                r#"{"unrelated":"nothing in common"}"#,
            ))
            .unwrap();

        let ethereum = store.chain_ids["ethereum"];
        let base = store.chain_ids["base"];
        let mut acc = SummaryAccumulator::default();
        let progress = PhaseProgress::default();
        let stats = run_direct(&store, &evm, 8, 0.6, &mut acc, &progress).unwrap();
        let count = |kind, primary, secondary| {
            acc.counts()
                .iter()
                .find(|(key, _)| {
                    key.kind == kind
                        && key.primary_chain == primary
                        && key.secondary_chain == secondary
                        && key.dimension == Dimension::Metadata
                })
                .map_or(0, |(_, counts)| counts.duplicate_contract_count)
        };
        assert_eq!(count(ScopeKind::IntraChain, ethereum, None), 2);
        assert_eq!(count(ScopeKind::IntraChain, base, None), 0);
        assert_eq!(count(ScopeKind::CrossChainSummary, ethereum, None), 2);
        assert_eq!(count(ScopeKind::CrossChainSummary, base, None), 1);
        assert_eq!(count(ScopeKind::ChainMatrix, ethereum, Some(base)), 2);
        assert_eq!(count(ScopeKind::ChainMatrix, base, Some(ethereum)), 1);
        assert_eq!(stats.eligible_contracts, 4);
        assert_eq!(stats.logical_contract_pairs, 6);
        assert!(stats.profile_pair_tasks < stats.logical_contract_pairs);
        assert_eq!(
            stats.equivalent_profile_tasks
                + stats.saturated_profile_pairs
                + stats.exact_document_pairs
                + stats.bm25_cache_hits
                + stats.bm25_scores,
            stats.profile_pair_tasks + stats.equivalent_profile_tasks
        );
        progress.assert_complete("reduce");
        progress.assert_complete("reduce_contracts");
    }

    #[test]
    fn unbounded_anchors_include_matches_beyond_the_previous_default() {
        let evm = ["ethereum".to_owned()].into_iter().collect::<HashSet<_>>();
        let mut store = EntityStore::with_options(None, &evm.iter().cloned().collect());
        for address in ["0xa", "0xb"] {
            for token in 1..=8 {
                let metadata = format!(r#"{{"{address}_{token}":"{address}_{token}"}}"#);
                store
                    .try_ingest_row(input("ethereum", address, &token.to_string(), &metadata))
                    .unwrap();
            }
            store
                .try_ingest_row(input(
                    "ethereum",
                    address,
                    "9",
                    r#"{"shared":"match beyond eight"}"#,
                ))
                .unwrap();
        }
        assert!(
            store
                .contracts
                .iter()
                .all(|contract| contract.metadata_by_token.len() == 9)
        );

        let ethereum = store.chain_ids["ethereum"];
        let mut acc = SummaryAccumulator::default();
        run_direct(&store, &evm, None, 0.6, &mut acc, &NoopProgress).unwrap();
        let counts = acc
            .counts()
            .iter()
            .find(|(key, _)| {
                key.kind == ScopeKind::IntraChain
                    && key.primary_chain == ethereum
                    && key.dimension == Dimension::Metadata
            })
            .map(|(_, counts)| counts)
            .unwrap();
        assert_eq!(counts.duplicate_contract_count, 2);
        assert_eq!(counts.duplicate_nft_count, 18);
    }

    #[test]
    fn solana_participates_intra_and_cross_chain_per_nft() {
        let evm = ["ethereum".to_owned()].into_iter().collect::<HashSet<_>>();
        let mut store = EntityStore::with_options(8, &evm.iter().cloned().collect());
        let same = r#"{"collection":"same","name":"token one"}"#;
        for (chain, address, token, metadata) in [
            ("solana", "sol-a", "mint-1", same),
            (
                "solana",
                "sol-a",
                "mint-2",
                r#"{"collection":"unique","name":"different"}"#,
            ),
            ("solana", "sol-b", "mint-3", same),
            ("ethereum", "0xa", "1", same),
            ("ethereum", "0xb", "1", same),
        ] {
            store
                .try_ingest_row(input(chain, address, token, metadata))
                .unwrap();
        }

        let solana = store.chain_ids["solana"];
        let ethereum = store.chain_ids["ethereum"];
        let progress = PhaseProgress::default();
        let index = build_index(&store, &evm, 8, &progress).unwrap();
        progress.assert_complete("profiles");
        let solana_profiles = index
            .profiles
            .iter()
            .filter(|profile| profile.is_solana)
            .collect::<Vec<_>>();
        assert_eq!(solana_profiles.len(), 2);
        assert!(
            solana_profiles
                .iter()
                .all(|profile| profile.anchor_len == 1)
        );
        assert_eq!(
            solana_profiles
                .iter()
                .map(|profile| profile.member_len)
                .sum::<u32>(),
            3
        );
        let mut acc = SummaryAccumulator::default();
        let stats = run_direct(&store, &evm, 8, 0.6, &mut acc, &NoopProgress).unwrap();

        assert_eq!(stats.eligible_contracts, 4);
        assert_eq!(stats.logical_contract_pairs, 10);
        assert_eq!(stats.equivalent_profile_tasks, 2);
        assert_eq!(
            acc.counts()
                .iter()
                .find(|(key, _)| {
                    key.kind == ScopeKind::IntraChain
                        && key.primary_chain == ethereum
                        && key.dimension == Dimension::Metadata
                })
                .unwrap()
                .1
                .duplicate_contract_count,
            2
        );
        let count = |kind, primary, secondary| {
            acc.counts()
                .iter()
                .find(|(key, _)| {
                    key.kind == kind
                        && key.primary_chain == primary
                        && key.secondary_chain == secondary
                        && key.dimension == Dimension::Metadata
                })
                .map(|(_, counts)| (counts.duplicate_contract_count, counts.duplicate_nft_count))
        };
        assert_eq!(count(ScopeKind::IntraChain, solana, None), Some((2, 2)));
        assert_eq!(
            count(ScopeKind::ChainMatrix, solana, Some(ethereum)),
            Some((2, 2))
        );
        assert_eq!(
            count(ScopeKind::ChainMatrix, ethereum, Some(solana)),
            Some((2, 2))
        );
    }

    #[test]
    fn fuzzy_solana_nfts_are_active_in_indexed_and_full_scoring() {
        let evm = ["ethereum".to_owned()].into_iter().collect::<HashSet<_>>();
        let mut store = EntityStore::with_options(8, &evm.iter().cloned().collect());
        let shared = (0..64)
            .map(|term| format!("sharedterm{term}"))
            .collect::<Vec<_>>()
            .join(" ");
        let left = format!(r#"{{"description":"{shared} leftonly"}}"#);
        let right = format!(r#"{{"description":"{shared} rightonly"}}"#);
        for (chain, address, token, metadata) in [
            ("solana", "sol-a", "mint-1", left.as_str()),
            ("solana", "sol-a", "mint-2", right.as_str()),
            (
                "ethereum",
                "0xe",
                "1",
                r#"{"unrelated":"no common metadata vocabulary"}"#,
            ),
        ] {
            store
                .try_ingest_row(input(chain, address, token, metadata))
                .unwrap();
        }

        let index = build_index(&store, &evm, 8, &NoopProgress).unwrap();
        assert_eq!(index.query_profile_count, index.profiles.len());
        assert_eq!(index.exhaustive_profile_pairs(), 3);
        assert_eq!(index.logical_member_pairs(), 3);

        let full_hits = ProfileHits::new(index.profiles.len(), store.chains.len(), true);
        let full_stats = AtomicStats::default();
        score_cross_profiles(
            &index,
            &full_hits,
            0.6,
            &full_stats,
            &NoopProgress,
            &CrossProfilePlan::Full,
            CrossSamplingPlan {
                pairs: PairSamplingPlan::disabled(),
                image_sample_size: 0,
            },
        )
        .unwrap();
        let solana = store.chain_ids["solana"];
        let solana_profiles = index
            .profiles
            .iter()
            .enumerate()
            .filter(|(_, profile)| profile.is_solana)
            .map(|(profile_id, _)| profile_id)
            .collect::<Vec<_>>();
        assert_eq!(solana_profiles.len(), 2);
        assert!(
            solana_profiles
                .iter()
                .all(|profile| full_hits.contains(*profile, solana))
        );
        assert_eq!(full_stats.matched_profile_pairs.load(Ordering::Relaxed), 1);

        let mut acc = SummaryAccumulator::default();
        let indexed = run_direct(&store, &evm, 8, 0.6, &mut acc, &NoopProgress).unwrap();
        assert!(indexed.candidate_index_used);
        assert!(indexed.candidate_profile_pairs >= 1);
        assert!(indexed.bm25_scores >= 1);
        assert_eq!(
            acc.counts()
                .iter()
                .find(|(key, _)| {
                    key.kind == ScopeKind::IntraChain
                        && key.primary_chain == solana
                        && key.dimension == Dimension::Metadata
                })
                .map(|(_, counts)| (counts.duplicate_contract_count, counts.duplicate_nft_count)),
            Some((1, 2))
        );
    }

    #[test]
    fn identical_solana_nfts_in_one_contract_are_counted_per_nft() {
        let evm = HashSet::new();
        let mut store = EntityStore::with_options(8, &evm.iter().cloned().collect());
        let metadata = r#"{"collection":"same","description":"identical"}"#;
        store
            .try_ingest_row(input("solana", "sol-a", "mint-1", metadata))
            .unwrap();
        store
            .try_ingest_row(input("solana", "sol-a", "mint-2", metadata))
            .unwrap();

        let solana = store.chain_ids["solana"];
        let mut acc = SummaryAccumulator::default();
        let stats = run_direct(&store, &evm, 8, 0.6, &mut acc, &NoopProgress).unwrap();

        assert_eq!(stats.logical_contract_pairs, 1);
        assert_eq!(stats.equivalent_profile_tasks, 1);
        assert_eq!(stats.exact_document_pairs, 1);
        assert_eq!(
            acc.counts()
                .iter()
                .find(|(key, _)| {
                    key.kind == ScopeKind::IntraChain
                        && key.primary_chain == solana
                        && key.dimension == Dimension::Metadata
                })
                .map(|(_, counts)| (counts.duplicate_contract_count, counts.duplicate_nft_count)),
            Some((1, 2))
        );
    }

    #[test]
    fn direct_results_are_identical_across_thread_counts() {
        let evm = ["ethereum".to_owned(), "base".to_owned()]
            .into_iter()
            .collect::<HashSet<_>>();
        let mut store = EntityStore::with_options(8, &evm.iter().cloned().collect());
        let rows = [
            (
                "ethereum",
                "0xa",
                "1",
                r#"{"collection":"same","name":"one"}"#,
            ),
            (
                "ethereum",
                "0xa",
                "2",
                r#"{"collection":"left","name":"two"}"#,
            ),
            ("base", "0xb", "1", r#"{"collection":"same","name":"one"}"#),
            ("base", "0xb", "2", r#"{"collection":"right","name":"two"}"#),
            (
                "ethereum",
                "0xc",
                "7",
                r#"{"fallback":"identical document"}"#,
            ),
            ("base", "0xd", "8", r#"{"fallback":"identical document"}"#),
            (
                "ethereum",
                "0xe",
                "9",
                r#"{"unrelated":"no shared vocabulary"}"#,
            ),
        ];
        for (chain, address, token, metadata) in rows {
            store
                .try_ingest_row(input(chain, address, token, metadata))
                .unwrap();
        }

        let run = |threads| {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .unwrap();
            pool.install(|| {
                let mut acc = SummaryAccumulator::default();
                run_direct(&store, &evm, 8, 0.6, &mut acc, &NoopProgress).unwrap();
                acc.counts().clone()
            })
        };
        assert_eq!(run(1), run(4));
    }

    #[test]
    fn metadata_image_samples_include_every_pair_that_chain_saturation_would_hide() {
        let evm = ["ethereum".to_owned()].into_iter().collect::<HashSet<_>>();
        let mut store = EntityStore::with_options(8, &evm.iter().cloned().collect());
        for (address, suffix) in [("0xa", "alpha"), ("0xb", "beta"), ("0xc", "gamma")] {
            let metadata = format!(r#"{{"shared":"common value {suffix}"}}"#);
            store
                .try_ingest_row(input_with_image(
                    "ethereum",
                    address,
                    "1",
                    &format!("https://images.example/{address}.png"),
                    &metadata,
                ))
                .unwrap();
        }

        let mut acc = SummaryAccumulator::default();
        let (_, samples, _) = run_direct_releasing_with_samples(
            &mut store,
            &evm,
            8,
            0.01,
            &mut acc,
            &NoopProgress,
            10,
        )
        .unwrap();
        let pairs = samples
            .all_chains
            .iter()
            .map(|pair| {
                let mut addresses = [
                    pair.contract_a_address.as_str(),
                    pair.contract_b_address.as_str(),
                ];
                addresses.sort_unstable();
                (addresses[0].to_owned(), addresses[1].to_owned())
            })
            .collect::<HashSet<_>>();

        assert_eq!(
            pairs,
            HashSet::from([
                ("0xa".to_owned(), "0xb".to_owned()),
                ("0xa".to_owned(), "0xc".to_owned()),
                ("0xb".to_owned(), "0xc".to_owned()),
            ])
        );
    }

    #[test]
    fn full_fallback_skips_saturated_profile_blocks() {
        let evm = ["ethereum".to_owned(), "base".to_owned()]
            .into_iter()
            .collect::<HashSet<_>>();
        let mut store = EntityStore::with_options(1, &evm.iter().cloned().collect());
        for contract in 0..300 {
            let chain = if contract % 2 == 0 {
                "ethereum"
            } else {
                "base"
            };
            let metadata = format!(r#"{{"unique{contract}":"value{contract}"}}"#);
            store
                .try_ingest_row(input(chain, &format!("0x{contract:x}"), "1", &metadata))
                .unwrap();
        }
        let mut acc = SummaryAccumulator::default();
        let stats = run_direct(&store, &evm, 1, 0.0, &mut acc, &NoopProgress).unwrap();
        assert!(!stats.candidate_index_used);
        assert!(stats.block_saturated_profile_pairs > 0);
        assert_eq!(
            stats.equivalent_profile_tasks
                + stats.saturated_profile_pairs
                + stats.exact_document_pairs
                + stats.bm25_cache_hits
                + stats.bm25_scores,
            stats.profile_pair_tasks + stats.equivalent_profile_tasks
        );
    }

    #[test]
    fn full_fallback_samples_the_image_qualified_contract_universe() {
        let evm = ["ethereum".to_owned(), "base".to_owned()]
            .into_iter()
            .collect::<HashSet<_>>();
        let mut store = EntityStore::with_options(1, &evm.iter().cloned().collect());
        for contract in 0..300 {
            let chain = if contract % 2 == 0 {
                "ethereum"
            } else {
                "base"
            };
            let metadata = format!(r#"{{"unique{contract}":"value{contract}"}}"#);
            store
                .try_ingest_row(input_with_image(
                    chain,
                    &format!("0x{contract:x}"),
                    "1",
                    &format!("https://images.example/{contract}.png"),
                    &metadata,
                ))
                .unwrap();
        }
        let mut acc = SummaryAccumulator::default();
        let (stats, samples, image_samples) = run_direct_releasing_with_samples(
            &mut store,
            &evm,
            1,
            0.0,
            &mut acc,
            &NoopProgress,
            1_000,
        )
        .unwrap();

        assert_eq!(stats.block_saturated_profile_pairs, 0);
        assert_eq!(samples.all_chains.len(), 1_000);
        assert_eq!(image_samples.len(), samples.all_chains.len());
        assert!(
            image_samples
                .iter()
                .all(|sample| !sample.image_uri_a.is_empty() && !sample.image_uri_b.is_empty())
        );
        assert_eq!(samples.intra_chain.len(), 2);
        assert!(
            samples
                .intra_chain
                .iter()
                .all(|scope| !scope.pairs.is_empty() && scope.pairs.len() <= 1_000)
        );
        assert_eq!(samples.chain_pairs.len(), 1);
        assert!(!samples.chain_pairs[0].pairs.is_empty());
        assert!(samples.chain_pairs[0].pairs.len() <= 1_000);
        assert_eq!(samples.cross_chain_summary.len(), 2);
        assert!(
            samples
                .cross_chain_summary
                .iter()
                .all(|scope| !scope.pairs.is_empty() && scope.pairs.len() <= 1_000)
        );
    }

    #[test]
    fn candidate_index_contains_every_exhaustive_match() {
        let evm = ["ethereum".to_owned()].into_iter().collect::<HashSet<_>>();
        let mut store = EntityStore::with_options(16, &evm.iter().cloned().collect());
        let shared = (0..128)
            .map(|index| format!("sharedterm{index}"))
            .collect::<Vec<_>>()
            .join(" ");
        let similar_left = format!(r#"{{"alpha":"{shared} leftonly"}}"#);
        let similar_right = format!(r#"{{"alpha":"{shared} rightonly"}}"#);
        let token_similar_left = format!(r#"{{"tokenalpha":"{shared} tokenleft"}}"#);
        let token_similar_right = format!(r#"{{"tokenalpha":"{shared} tokenright"}}"#);
        let rows = [
            ("0xempty-a", "1", "{}"),
            ("0xempty-b", "2", "{}"),
            ("0xsimilar-a", "3", similar_left.as_str()),
            ("0xsimilar-b", "4", similar_right.as_str()),
            ("0xunique-a", "5", r#"{"uniquealpha":"onealpha"}"#),
            ("0xunique-b", "6", r#"{"uniquebeta":"onebeta"}"#),
            ("0xunique-c", "7", r#"{"uniquegamma":"onegamma"}"#),
            ("0xunique-d", "8", r#"{"uniquedelta":"onedelta"}"#),
            ("0xtoken-a", "1", token_similar_left.as_str()),
            ("0xtoken-a", "90", r#"{"maxtokenleft":"onlyleft"}"#),
            ("0xtoken-b", "1", token_similar_right.as_str()),
            ("0xtoken-b", "91", r#"{"maxtokenright":"onlyright"}"#),
            ("0xtoken-empty-a", "1", "{}"),
            ("0xtoken-empty-a", "100", r#"{"emptymaxleft":"onlyleft"}"#),
            ("0xtoken-empty-b", "1", "{}"),
            ("0xtoken-empty-b", "101", r#"{"emptymaxright":"onlyright"}"#),
        ];
        for (address, token, metadata) in rows {
            store
                .try_ingest_row(input("ethereum", address, token, metadata))
                .unwrap();
        }

        let index = build_index(&store, &evm, 8, &NoopProgress).unwrap();
        assert_eq!(
            index
                .profiles
                .iter()
                .map(|profile| profile.anchor_len as usize)
                .sum::<usize>(),
            index.anchors.len()
        );
        let exhaustive = choose_two(index.profiles.len() as u64);
        let (plan, candidate_stats) =
            build_candidate_plan(&index, 0.6, exhaustive, &NoopProgress).unwrap();
        let CrossProfilePlan::Indexed(candidate_index) = plan else {
            panic!("sparse fixture should use the lossless candidate index");
        };
        let generated = candidate_index
            .collect_pairs(&index, &NoopProgress)
            .unwrap();
        assert!(generated.prefix_terms > 0);
        assert!(generated.prefix_terms < candidate_stats.full_terms);
        assert!(generated.scoring_work > 0);
        let candidates = IndexedPairs::new(generated.chunks, generated.pair_count);
        for candidate in candidates.iter() {
            let (left, right) = candidate.profiles();
            assert_eq!(
                candidate.documents(),
                selected_documents(
                    &index.profiles[left],
                    index.anchors(&index.profiles[left]),
                    &index.profiles[right],
                    index.anchors(&index.profiles[right]),
                )
            );
        }
        let candidates = candidates
            .iter()
            .map(|candidate| candidate.profile_key)
            .collect::<HashSet<_>>();

        let mut exhaustive_matches = 0;
        for left_id in 0..index.profiles.len() {
            for right_id in left_id + 1..index.profiles.len() {
                let left = &index.profiles[left_id];
                let right = &index.profiles[right_id];
                let (left_document, right_document) =
                    selected_documents(left, index.anchors(left), right, index.anchors(right));
                let matched = left_document == right_document
                    || similarity_at_least(
                        &index.documents[left_document as usize],
                        index.document_terms(left_document),
                        &index.documents[right_document as usize],
                        index.document_terms(right_document),
                        0.6,
                    )
                    .matched;
                if matched {
                    exhaustive_matches += 1;
                    assert!(
                        candidates.contains(&profile_pair_key(left_id as u32, right_id as u32)),
                        "candidate index dropped matching profile pair {left_id}/{right_id}"
                    );
                }
            }
        }
        // Empty objects are rejected at entity ingestion, so only the two
        // non-empty similarity fixtures are expected to match.
        assert!(exhaustive_matches >= 2);
        assert!((candidates.len() as u64) < exhaustive);
    }

    #[test]
    fn fused_candidates_cover_generated_exhaustive_matches_at_all_thresholds() {
        let evm = ["ethereum".to_owned()].into_iter().collect::<HashSet<_>>();
        let mut store = EntityStore::with_options(4, &evm.iter().cloned().collect());
        for contract in 0..24 {
            let group = contract / 2;
            let shared = (0..32)
                .map(|term| format!("group{group}term{term}"))
                .collect::<Vec<_>>()
                .join(" ");
            let selected = format!(r#"{{"groupkey{group}":"{shared} side{contract}"}}"#);
            let unique = format!(r#"{{"uniquekey{contract}":"uniquevalue{contract}"}}"#);
            let address = format!("0x{contract:x}");
            store
                .try_ingest_row(input("ethereum", &address, "1", &selected))
                .unwrap();
            store
                .try_ingest_row(input(
                    "ethereum",
                    &address,
                    &format!("{}", 100 + contract),
                    &unique,
                ))
                .unwrap();
        }

        let index = build_index(&store, &evm, 4, &NoopProgress).unwrap();
        let exhaustive = choose_two(index.profiles.len() as u64);
        for threshold in [0.4, 0.6, 0.8, 0.95] {
            let (plan, _) =
                build_candidate_plan(&index, threshold, exhaustive, &NoopProgress).unwrap();
            let CrossProfilePlan::Indexed(candidate_index) = plan else {
                panic!("generated sparse fixture should use the candidate index");
            };
            let generated = candidate_index
                .collect_pairs(&index, &NoopProgress)
                .unwrap();
            let candidates = IndexedPairs::new(generated.chunks, generated.pair_count);
            let unique = candidates
                .iter()
                .map(|candidate| candidate.profile_key)
                .collect::<HashSet<_>>();
            assert_eq!(unique.len(), candidates.len());
            for left_id in 0..index.profiles.len() {
                for right_id in left_id + 1..index.profiles.len() {
                    let left = &index.profiles[left_id];
                    let right = &index.profiles[right_id];
                    let (left_document, right_document) =
                        selected_documents(left, index.anchors(left), right, index.anchors(right));
                    let matched = left_document == right_document
                        || similarity_at_least(
                            &index.documents[left_document as usize],
                            index.document_terms(left_document),
                            &index.documents[right_document as usize],
                            index.document_terms(right_document),
                            threshold,
                        )
                        .matched;
                    if matched {
                        assert!(
                            unique.contains(&profile_pair_key(left_id as u32, right_id as u32)),
                            "threshold={threshold} dropped {left_id}/{right_id}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn candidate_phases_report_exact_progress_under_parallel_generation() {
        let evm = ["ethereum".to_owned()].into_iter().collect::<HashSet<_>>();
        let mut store = EntityStore::with_options(2, &evm.iter().cloned().collect());
        for contract in 0..16 {
            let group = contract / 2;
            let metadata =
                format!(r#"{{"group{group}":"shared group value {group}","side":"{contract}"}}"#);
            store
                .try_ingest_row(input(
                    "ethereum",
                    &format!("0x{contract:x}"),
                    "1",
                    &metadata,
                ))
                .unwrap();
        }
        let index = build_index(&store, &evm, 2, &NoopProgress).unwrap();
        let exhaustive = choose_two(index.profiles.len() as u64);
        let progress = PhaseProgress::default();
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(4)
            .build()
            .unwrap();
        let (plan, _) = pool
            .install(|| build_candidate_plan(&index, 0.6, exhaustive, &progress))
            .unwrap();
        assert!(matches!(plan, CrossProfilePlan::Indexed(_)));
        for phase in [
            "candidate_admission",
            "candidate_term_rank",
            "candidate_term_reduce",
            "candidate_term_order",
            "candidate_prefixes",
            "candidate_global_count",
            "candidate_global_offsets",
            "candidate_global_fill",
            "candidate_global_compress",
            "candidate_build",
            "candidate_sort",
            "candidate_ranges",
        ] {
            progress.assert_complete(phase);
        }
    }

    #[test]
    fn token_specific_postings_skip_tokens_owned_by_only_one_profile() {
        let evm = ["ethereum".to_owned()].into_iter().collect::<HashSet<_>>();
        let mut store = EntityStore::with_options(4, &evm.iter().cloned().collect());
        for (address, token, metadata) in [
            ("0xa", "1", r#"{"shared":"alpha beta"}"#),
            ("0xa", "9", r#"{"unique":"left only"}"#),
            ("0xb", "1", r#"{"shared":"alpha gamma"}"#),
            ("0xb", "10", r#"{"unique":"right only"}"#),
        ] {
            store
                .try_ingest_row(input("ethereum", address, token, metadata))
                .unwrap();
        }
        let index = build_index(&store, &evm, 4, &NoopProgress).unwrap();
        let mut frequencies = index.token_profile_counts.to_vec();
        frequencies.sort_unstable();
        assert_eq!(frequencies, [1, 1, 2]);

        let exhaustive = choose_two(index.profiles.len() as u64);
        let (plan, _) = build_candidate_plan(&index, 0.6, exhaustive, &NoopProgress).unwrap();
        let CrossProfilePlan::Indexed(candidates) = plan else {
            panic!("non-empty fixture should use the candidate index");
        };
        for shard in &candidates.shards {
            for &key in shard
                .token_full
                .keys
                .iter()
                .chain(shard.token_exact.keys.iter())
            {
                assert!(index.token_profile_counts[(key >> 32) as usize] >= 2);
            }
        }
    }

    #[test]
    fn exact_postings_are_only_needed_for_documents_without_terms() {
        let evm = ["ethereum".to_owned()].into_iter().collect::<HashSet<_>>();
        let mut store = EntityStore::with_options(4, &evm.iter().cloned().collect());
        for (address, token, metadata) in [
            ("0xa", "1", r#"{"shared":"same value"}"#),
            ("0xa", "9", r#"{"left":"only left"}"#),
            ("0xb", "1", r#"{"shared":"same value"}"#),
            ("0xb", "10", r#"{"right":"only right"}"#),
            ("0xc", "7", r#"{"unrelated":"third profile"}"#),
        ] {
            store
                .try_ingest_row(input("ethereum", address, token, metadata))
                .unwrap();
        }
        let index = build_index(&store, &evm, 4, &NoopProgress).unwrap();
        assert!(
            index
                .profiles
                .iter()
                .all(|profile| !profile.has_empty_token_document)
        );
        let counts =
            estimate_candidate_counts(&index, true, "candidate_admission", &NoopProgress).unwrap();
        assert_eq!(counts.global_exact, 0);
        assert_eq!(counts.token_exact, 0);
        let exhaustive = choose_two(index.profiles.len() as u64);
        let (plan, _) = build_candidate_plan(&index, 0.6, exhaustive, &NoopProgress).unwrap();
        let CrossProfilePlan::Indexed(candidate_index) = plan else {
            panic!("sparse non-empty fixture should use the candidate index");
        };
        let generated = candidate_index
            .collect_pairs(&index, &NoopProgress)
            .unwrap();
        let candidates = IndexedPairs::new(generated.chunks, generated.pair_count);
        assert!(candidates.iter().any(|candidate| {
            let (left, right) = candidate.profiles();
            let (left_document, right_document) = selected_documents(
                &index.profiles[left],
                index.anchors(&index.profiles[left]),
                &index.profiles[right],
                index.anchors(&index.profiles[right]),
            );
            left_document == right_document
        }));

        let mut empty_store = EntityStore::with_options(4, &evm.iter().cloned().collect());
        for (address, token, metadata) in [
            ("0xa", "1", r#"{"!":"?"}"#),
            ("0xa", "9", r#"{"left":"only left"}"#),
            ("0xb", "1", r#"{"!":"?"}"#),
            ("0xb", "10", r#"{"right":"only right"}"#),
            ("0xc", "7", r#"{"unrelated":"third profile"}"#),
        ] {
            empty_store
                .try_ingest_row(input("ethereum", address, token, metadata))
                .unwrap();
        }
        let empty_index = build_index(&empty_store, &evm, 4, &NoopProgress).unwrap();
        assert_eq!(
            empty_index
                .profiles
                .iter()
                .filter(|profile| profile.has_empty_token_document)
                .count(),
            2
        );
        let empty_counts =
            estimate_candidate_counts(&empty_index, true, "candidate_admission", &NoopProgress)
                .unwrap();
        assert!(empty_counts.token_exact >= 2);
        let exhaustive = choose_two(empty_index.profiles.len() as u64);
        let (plan, _) = build_candidate_plan(&empty_index, 0.6, exhaustive, &NoopProgress).unwrap();
        let CrossProfilePlan::Indexed(candidate_index) = plan else {
            panic!("sparse empty-term fixture should use the candidate index");
        };
        let generated = candidate_index
            .collect_pairs(&empty_index, &NoopProgress)
            .unwrap();
        let candidates = IndexedPairs::new(generated.chunks, generated.pair_count);
        assert!(candidates.iter().any(|candidate| {
            let (left, right) = candidate.profiles();
            let (left_document, right_document) = selected_documents(
                &empty_index.profiles[left],
                empty_index.anchors(&empty_index.profiles[left]),
                &empty_index.profiles[right],
                empty_index.anchors(&empty_index.profiles[right]),
            );
            left_document == right_document
        }));
    }

    #[test]
    fn term_ranks_use_profile_context_frequency() {
        let evm = ["ethereum".to_owned()].into_iter().collect::<HashSet<_>>();
        let mut store = EntityStore::with_options(16, &evm.iter().cloned().collect());
        for contract in 0..8 {
            store
                .try_ingest_row(input(
                    "ethereum",
                    &format!("0x{contract:x}"),
                    "1",
                    r#"{"widelyreusedterm":"sharedvalue"}"#,
                ))
                .unwrap();
            store
                .try_ingest_row(input(
                    "ethereum",
                    &format!("0x{contract:x}"),
                    &format!("{}", contract + 100),
                    &format!(r#"{{"rareterm{contract}":"rarevalue{contract}"}}"#),
                ))
                .unwrap();
        }

        let index = build_index(&store, &evm, 16, &NoopProgress).unwrap();
        let popular_document = index
            .document_context_weights
            .iter()
            .enumerate()
            .max_by_key(|(_, weight)| *weight)
            .map(|(document, _)| document as DocumentId)
            .unwrap();
        let rare_document = index
            .document_context_weights
            .iter()
            .enumerate()
            .find(|(_, weight)| **weight == 2)
            .map(|(document, _)| document as DocumentId)
            .unwrap();
        let popular_terms = index
            .document_terms(popular_document)
            .iter()
            .map(|(term, _)| *term)
            .collect::<HashSet<_>>();
        let rare_term = index
            .document_terms(rare_document)
            .iter()
            .map(|(term, _)| *term)
            .find(|term| !popular_terms.contains(term))
            .unwrap();
        let popular_term = *popular_terms.iter().next().unwrap();

        let ranks = build_term_ranks(&index, &NoopProgress).unwrap();
        assert!(
            ranks[rare_term as usize] < ranks[popular_term as usize],
            "a term used by one profile context should rank before a term reused by eight contexts"
        );
    }
}
