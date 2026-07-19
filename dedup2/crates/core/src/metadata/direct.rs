use crate::entity::{ChainId, ContractId, Dimension, EntityStore, NftId, ScopeKind};
use crate::error::DedupError;
use crate::metadata::bm25::{
    PreparedDocument, lossless_prefix_len, may_share_term, similarity_at_least,
};
use crate::progress::ProgressObserver;
use crate::radix::{sort_u32_pairs_while, sort_u32_triples_while, sort_u64_while};
use crate::scope::{ScopeCounts, ScopeKey};
use crate::stats::SummaryAccumulator;
use ahash::{AHashMap, AHashSet, AHasher};
use rayon::prelude::*;
use serde::Serialize;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::mem::MaybeUninit;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, AtomicUsize, Ordering};

type DocumentId = u32;
type TokenKeyId = u32;

const SCORE_TILE: usize = 256;
const SATURATION_BLOCK: usize = 32;
const MAX_SCORE_TILE_BATCH: u64 = 8;
const SCORE_CACHE_SLOTS: usize = 1 << 20;
const LOCAL_CACHE_ENTRIES: usize = 16_384;
const CACHE_SAMPLE_PAIRS: u64 = 65_536;
const MIN_CACHE_HIT_PERCENT: u64 = 1;
const INTERN_SHARDS: usize = 256;
const PREPARE_BATCH: usize = 4096;
const INLINE_ANCHORS: usize = 8;
const TOKEN_MASK_WORDS: usize = 4;
const CANDIDATE_SHARDS: usize = 64;
const MAX_CANDIDATE_POSTING_BYTES: u64 = 128 << 30;
const MAX_CANDIDATE_PAIR_BYTES: u64 = 128 << 30;
const MAX_FULL_PREPASS_POSTING_BYTES: u64 = 8 << 30;
const MAX_FULL_PREPASS_PAIRS: usize = 1 << 24;
const DENSE_CANDIDATE_SEEN_BUDGET_BYTES: usize = 16 * 1024 * 1024 * 1024;
const DENSE_TERM_FREQUENCY_BUDGET_BYTES: usize = 16 * 1024 * 1024 * 1024;
const CANDIDATE_CANCEL_BATCH: u64 = 1 << 20;
const CANDIDATE_PAIR_CHUNK: usize = 4096;
const CANDIDATE_SCHEDULING_CHUNK: usize = 8;

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
    pub candidate_posting_budget_ratio: f64,
    pub candidate_index_budget_ratio: f64,
    pub candidate_pair_bytes: u64,
    pub candidate_pair_budget_ratio: f64,
    pub candidate_prefix_terms: u64,
    pub candidate_prefix_term_ratio: f64,
    pub candidate_pair_emissions: u64,
    pub candidate_pair_emission_ratio: f64,
    pub candidate_pair_dedup_reduction_ratio: f64,
    pub candidate_profile_pairs: u64,
    pub candidate_profile_pair_ratio: f64,
    pub candidate_zero_overlap_prunes: u64,
    pub candidate_zero_overlap_prune_ratio: f64,
    pub candidate_generation_fallback: bool,
    pub full_prepass_pairs: u64,
    pub full_prepass_pair_ratio: f64,
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
}

#[derive(Debug)]
struct ContractProfile {
    is_evm: bool,
    is_solana: bool,
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

fn should_compare_profiles(left: &ContractProfile, right: &ContractProfile) -> bool {
    match (left.is_solana, right.is_solana) {
        (true, true) => false,
        (true, false) => right.is_evm,
        (false, true) => left.is_evm,
        (false, false) => true,
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
    document_references: Box<[u8]>,
    document_context_weights: Box<[u32]>,
    profiles: Vec<ContractProfile>,
    anchors: Vec<(TokenKeyId, DocumentId)>,
    members: Vec<MetadataMember>,
    chain_counts: Vec<(ChainId, u32)>,
    query_profile_count: usize,
    eligible_contracts: u64,
    eligible_members: u64,
    anchor_count: u64,
    unique_terms: u64,
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

    fn document_pair_may_repeat(&self, left: DocumentId, right: DocumentId) -> bool {
        self.document_references[left as usize] > 1 || self.document_references[right as usize] > 1
    }

    fn exhaustive_profile_pairs(&self) -> u64 {
        let query_profiles = self.query_profile_count as u64;
        let solana_profiles = self.profiles.len().saturating_sub(self.query_profile_count) as u64;
        let evm_profiles = self.profiles[..self.query_profile_count]
            .iter()
            .filter(|profile| profile.is_evm)
            .count() as u64;
        choose_two(query_profiles).saturating_add(evm_profiles.saturating_mul(solana_profiles))
    }

    fn logical_member_pairs(&self) -> u64 {
        let mut query_members = 0_u64;
        let mut evm_members = 0_u64;
        let mut solana_members = 0_u64;
        for profile in &self.profiles {
            let members = u64::from(profile.member_len);
            if profile.is_solana {
                solana_members = solana_members.saturating_add(members);
            } else {
                query_members = query_members.saturating_add(members);
                if profile.is_evm {
                    evm_members = evm_members.saturating_add(members);
                }
            }
        }
        choose_two(query_members).saturating_add(evm_members.saturating_mul(solana_members))
    }
}

enum CrossProfilePlan {
    Full { exact_prepass: Box<[u64]> },
    Indexed(IndexedPairs),
}

struct IndexedPairs {
    chunks: Box<[Box<[CandidatePair]>]>,
    len: usize,
}

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

    fn is_empty(&self) -> bool {
        self.len == 0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CandidatePair {
    profile_key: u64,
    document_key: u64,
}

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
    generation_fallback: bool,
    prepass_pairs: u64,
}

#[derive(Clone, Copy, Debug, Default)]
struct DocumentPrefix {
    cutoff_rank: u32,
    len: u32,
}

impl DocumentPrefix {
    fn contains(self, rank: u32) -> bool {
        self.len != 0 && rank <= self.cutoff_rank
    }
}

impl CrossProfilePlan {
    fn pair_count(&self, exhaustive_pairs: u64) -> u64 {
        match self {
            Self::Full { .. } => exhaustive_pairs,
            Self::Indexed(pairs) => pairs.len() as u64,
        }
    }

    fn is_indexed(&self) -> bool {
        matches!(self, Self::Indexed(_))
    }

    fn exact_prepass(&self) -> &[u64] {
        match self {
            Self::Full { exact_prepass } => exact_prepass,
            Self::Indexed(_) => &[],
        }
    }

    fn needs_block_tracking(&self) -> bool {
        matches!(self, Self::Full { .. })
    }
}

struct RawProfile {
    key: ProfileKey,
    member: MetadataMember,
    chain_id: ChainId,
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
        let terms = TermInterner::new();
        let prepared_chunks = values
            .par_chunks(PREPARE_BATCH)
            .map_init(
                || (AHashMap::<&'a str, u32>::new(), Vec::<u32>::new()),
                |(local_terms, scratch), chunk| {
                    progress.check_cancelled()?;
                    local_terms.clear();
                    let mut documents = Vec::with_capacity(chunk.len());
                    let mut compact_terms = Vec::new();
                    for &(id, value) in chunk {
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
                        documents.push((id, local_term_start, document));
                    }
                    progress.add_completed(chunk.len() as u64);
                    Ok::<_, DedupError>((documents, compact_terms))
                },
            )
            .collect::<Vec<_>>();
        let mut documents = (0..document_count).map(|_| None).collect::<Vec<_>>();
        let mut compact_terms = Vec::new();
        for chunk in prepared_chunks {
            let (chunk_documents, mut chunk_terms) = chunk?;
            let chunk_term_start = u32::try_from(compact_terms.len())
                .map_err(|_| DedupError::invalid("metadata", "metadata term offset overflow"))?;
            for (id, local_term_start, mut document) in chunk_documents {
                document.set_term_start(
                    chunk_term_start
                        .checked_add(local_term_start)
                        .ok_or_else(|| {
                            DedupError::invalid("metadata", "metadata term offset overflow")
                        })?,
                );
                documents[id as usize] = Some(document);
            }
            compact_terms.append(&mut chunk_terms);
        }
        let compact_documents = documents
            .into_iter()
            .map(|document| {
                document.ok_or_else(|| {
                    DedupError::invalid("metadata", "missing prepared metadata document")
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok((
            compact_documents,
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

struct TokenInterner<'a> {
    shards: Box<[Mutex<AHashMap<&'a str, TokenKeyId>>]>,
    next_id: AtomicU64,
}

impl<'a> TokenInterner<'a> {
    fn new() -> Self {
        Self {
            shards: (0..INTERN_SHARDS)
                .map(|_| Mutex::new(AHashMap::new()))
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
        if let Some(&id) = shard.get(value) {
            return Ok(id);
        }
        let id = TokenKeyId::try_from(self.next_id.fetch_add(1, Ordering::Relaxed))
            .map_err(|_| DedupError::invalid("metadata", "too many unique metadata token IDs"))?;
        shard.insert(value, id);
        Ok(id)
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

struct ScoreCache {
    slots: Box<[ScoreCacheSlot]>,
}

struct ScoreCacheSlot {
    version: AtomicU64,
    key: AtomicU64,
    value: AtomicU8,
}

impl ScoreCache {
    fn new() -> Self {
        Self {
            slots: (0..SCORE_CACHE_SLOTS)
                .map(|_| ScoreCacheSlot {
                    version: AtomicU64::new(0),
                    key: AtomicU64::new(0),
                    value: AtomicU8::new(0),
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }

    fn get(&self, key: u64) -> Option<bool> {
        let slot = &self.slots[self.slot(key)];
        let before = slot.version.load(Ordering::Acquire);
        if before & 1 != 0 {
            return None;
        }
        let stored_key = slot.key.load(Ordering::Relaxed);
        let stored_value = slot.value.load(Ordering::Relaxed);
        let after = slot.version.load(Ordering::Acquire);
        if before != after || after & 1 != 0 || stored_value == 0 || stored_key != key {
            None
        } else {
            Some(stored_value == 2)
        }
    }

    fn insert(&self, key: u64, value: bool) {
        let slot = &self.slots[self.slot(key)];
        let version = slot.version.load(Ordering::Relaxed);
        if version & 1 != 0
            || slot
                .version
                .compare_exchange_weak(
                    version,
                    version.wrapping_add(1),
                    Ordering::Acquire,
                    Ordering::Relaxed,
                )
                .is_err()
        {
            return;
        }
        slot.key.store(key, Ordering::Relaxed);
        slot.value
            .store(if value { 2 } else { 1 }, Ordering::Relaxed);
        slot.version
            .store(version.wrapping_add(2), Ordering::Release);
    }

    fn slot(&self, key: u64) -> usize {
        score_cache_slot(key, SCORE_CACHE_SLOTS)
    }
}

struct LocalScoreCache {
    keys: Box<[u64]>,
    values: Box<[u8]>,
}

impl LocalScoreCache {
    fn new() -> Self {
        Self {
            keys: vec![0; LOCAL_CACHE_ENTRIES].into_boxed_slice(),
            values: vec![0; LOCAL_CACHE_ENTRIES].into_boxed_slice(),
        }
    }

    fn get(&self, key: u64) -> Option<bool> {
        let slot = self.slot(key);
        (self.values[slot] != 0 && self.keys[slot] == key).then_some(self.values[slot] == 2)
    }

    fn insert(&mut self, key: u64, value: bool) {
        let slot = self.slot(key);
        self.keys[slot] = key;
        self.values[slot] = if value { 2 } else { 1 };
    }

    fn clear(&mut self) {
        self.values.fill(0);
    }

    fn slot(&self, key: u64) -> usize {
        score_cache_slot(key, LOCAL_CACHE_ENTRIES)
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
            .matched_profile_pairs
            .fetch_add(self.matched_profile_pairs, Ordering::Relaxed);
    }
}

pub fn run_direct(
    store: &EntityStore,
    evm_chains: &HashSet<String>,
    anchors_k: usize,
    threshold: f64,
    acc: &mut SummaryAccumulator,
    progress: &dyn ProgressObserver,
) -> Result<MetadataStats, DedupError> {
    let index = build_index(store, evm_chains, anchors_k, progress)?;
    let eligible_members = index.eligible_members;
    if eligible_members < 2 {
        return Ok(base_stats(store, &index, 0, 0, 0));
    }

    let logical_contract_pairs = index.logical_member_pairs();
    let equivalent_profile_tasks = index
        .profiles
        .iter()
        .filter(|profile| !profile.is_solana && profile.member_len > 1)
        .count() as u64;
    let exhaustive_cross_profile_tasks = index.exhaustive_profile_pairs();
    let (cross_profile_plan, candidate_stats) =
        build_candidate_plan(&index, threshold, exhaustive_cross_profile_tasks, progress)?;
    let cross_profile_tasks = cross_profile_plan.pair_count(exhaustive_cross_profile_tasks);
    let profile_pair_tasks = equivalent_profile_tasks.saturating_add(cross_profile_tasks);
    let scoring_work = scoring_work(&index, &cross_profile_plan);
    let hits = ProfileHits::new(
        index.profiles.len(),
        store.chains.len(),
        cross_profile_plan.needs_block_tracking(),
    );
    let stats = AtomicStats::default();
    seed_exact_prepass(&index, &hits, cross_profile_plan.exact_prepass(), progress)?;
    progress.begin_phase("direct_bm25", Some(scoring_work));
    score_equivalent_profiles(&index, &hits, &stats, progress)?;
    score_cross_profiles(
        &index,
        &hits,
        threshold,
        &stats,
        progress,
        &cross_profile_plan,
    )?;

    progress.begin_phase("reduce", Some(eligible_members));
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
                        progress.add_completed(completed);
                        progress.check_cancelled()?;
                        completed = 0;
                    }
                }
            }
            progress.add_completed(completed);
            Ok::<_, DedupError>(memberships)
        })
        .try_reduce(AHashMap::new, |mut left, right| {
            for (key, value) in right {
                left.entry(key)
                    .or_insert_with(MetadataScopeMembers::default)
                    .merge(value);
            }
            Ok(left)
        })?;
    let metadata_counts = metadata_memberships
        .into_iter()
        .map(|(key, members)| (key, members.into_counts()))
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
    result.candidate_posting_budget_ratio =
        ratio(candidate_stats.posting_bytes, MAX_CANDIDATE_POSTING_BYTES);
    result.candidate_index_budget_ratio =
        ratio(result.candidate_index_bytes, MAX_CANDIDATE_POSTING_BYTES);
    result.candidate_pair_bytes = candidate_stats
        .candidate_pairs
        .saturating_mul(std::mem::size_of::<CandidatePair>() as u64);
    result.candidate_pair_budget_ratio =
        ratio(result.candidate_pair_bytes, MAX_CANDIDATE_PAIR_BYTES);
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
    result.candidate_generation_fallback = candidate_stats.generation_fallback;
    result.full_prepass_pairs = candidate_stats.prepass_pairs;
    result.full_prepass_pair_ratio = ratio(
        candidate_stats.prepass_pairs,
        exhaustive_cross_profile_tasks,
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
    result.matched_profile_pair_ratio = ratio(result.matched_profile_pairs, profile_pair_tasks);
    Ok(result)
}

fn build_index(
    store: &EntityStore,
    evm_chains: &HashSet<String>,
    anchors_k: usize,
    progress: &dyn ProgressObserver,
) -> Result<DirectIndex, DedupError> {
    progress.begin_phase("prepare_direct", Some(store.contracts.len() as u64));
    let documents = DocumentInterner::new();
    let tokens = TokenInterner::new();
    let eligible_contracts = AtomicU64::new(0);
    let eligible_members = AtomicU64::new(0);
    let anchor_count = AtomicU64::new(0);
    let profile_buckets = store
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
                let take = contract.metadata_by_token.len().min(anchors_k);
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
                        let raw = RawProfile {
                            key: ProfileKey {
                                is_evm: false,
                                is_solana: true,
                                anchors: AnchorKey::from_vec(vec![(0, document_id)]),
                            },
                            member: MetadataMember {
                                contract_id: contract.id,
                                nft_id: Some(nft_id),
                            },
                            chain_id: contract.chain_id,
                        };
                        let shard = intern_shard(&raw.key);
                        profile_buckets[shard].push(raw);
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
                profile_buckets[shard].push(raw);
            }
            progress.add_completed(contracts.len() as u64);
            Ok::<_, DedupError>(profile_buckets)
        })
        .try_reduce(empty_profile_buckets, |mut left, right| {
            for (target, mut source) in left.iter_mut().zip(right) {
                target.append(&mut source);
            }
            Ok(left)
        })?;
    let (documents, terms, unique_terms) = documents.into_documents(progress)?;

    progress.begin_phase("profiles", Some(eligible_contracts.load(Ordering::Relaxed)));
    let mut profile_chunks = profile_buckets
        .into_par_iter()
        .map(|bucket| build_profile_bucket(bucket, progress))
        .collect::<Vec<_>>()
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
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
    let mut document_references = vec![0_u8; documents.len()];
    let mut document_context_weights = vec![0_u32; documents.len()];
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
            for &(_, document) in &chunk_anchors[start..end] {
                let references = &mut document_references[document as usize];
                *references = references.saturating_add(1);
                if profile.is_evm {
                    let weight = &mut document_context_weights[document as usize];
                    *weight = weight.saturating_add(1);
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
    profiles.par_sort_unstable_by_key(|profile| (profile.is_solana, profile.max_document()));
    let query_profile_count = profiles.partition_point(|profile| !profile.is_solana);
    Ok(DirectIndex {
        documents,
        terms,
        document_references: document_references.into_boxed_slice(),
        document_context_weights: document_context_weights.into_boxed_slice(),
        profiles,
        anchors,
        members,
        chain_counts,
        query_profile_count,
        eligible_contracts: eligible_contracts.load(Ordering::Relaxed),
        eligible_members: eligible_members.load(Ordering::Relaxed),
        anchor_count: anchor_count.load(Ordering::Relaxed),
        unique_terms,
    })
}

fn empty_profile_buckets() -> Vec<Vec<RawProfile>> {
    (0..INTERN_SHARDS).map(|_| Vec::new()).collect()
}

struct DensePostingIndex {
    offsets: Box<[usize]>,
    profiles: Box<[u32]>,
}

impl DensePostingIndex {
    fn empty() -> Self {
        Self {
            offsets: Box::new([0]),
            profiles: Box::new([]),
        }
    }

    fn posting_after(&self, key: u32, left: u32) -> &[u32] {
        let key = key as usize;
        if key + 1 >= self.offsets.len() {
            return &[];
        }
        let posting = &self.profiles[self.offsets[key]..self.offsets[key + 1]];
        let start = posting.partition_point(|profile| *profile <= left);
        &posting[start..]
    }

    fn len(&self) -> usize {
        self.profiles.len()
    }

    fn bytes(&self) -> u64 {
        (self.profiles.len() as u64)
            .saturating_mul(std::mem::size_of::<u32>() as u64)
            .saturating_add(
                (self.offsets.len() as u64).saturating_mul(std::mem::size_of::<usize>() as u64),
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
    token_full_ranges: Vec<TriplePostingRange>,
    global_exact_ranges: Vec<PairPostingRange>,
    token_exact_ranges: Vec<TriplePostingRange>,
}

#[derive(Clone, Copy)]
struct PairPostingRange {
    key: u32,
    start: usize,
    end: usize,
}

#[derive(Clone, Copy)]
struct TriplePostingRange {
    key: (u32, u32),
    start: usize,
    end: usize,
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
            token_full_ranges: Vec::new(),
            global_exact_ranges: Vec::new(),
            token_exact_ranges: Vec::new(),
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

    fn range_bytes(&self) -> u64 {
        let pair_ranges = self.global_exact_ranges.len();
        let triple_ranges = self
            .token_full_ranges
            .len()
            .saturating_add(self.token_exact_ranges.len());
        (pair_ranges as u64)
            .saturating_mul(std::mem::size_of::<PairPostingRange>() as u64)
            .saturating_add(
                (triple_ranges as u64)
                    .saturating_mul(std::mem::size_of::<TriplePostingRange>() as u64),
            )
    }

    fn build_ranges(&mut self) {
        self.token_full_ranges = compact_triple_posting_ranges(&self.token_full);
        self.global_exact_ranges = pair_posting_ranges(&self.global_exact);
        self.token_exact_ranges = compact_triple_posting_ranges(&self.token_exact);
    }

    fn global_exact_after(&self, key: u32, left: u32) -> &[(u32, u32)] {
        pair_posting_after(&self.global_exact, &self.global_exact_ranges, key, left)
    }

    fn token_full_after(&self, key: (u32, u32), left: u32) -> &[(u32, u32, u32)] {
        triple_posting_after(&self.token_full, &self.token_full_ranges, key, left)
    }

    fn token_exact_after(&self, key: (u32, u32), left: u32) -> &[(u32, u32, u32)] {
        triple_posting_after(&self.token_exact, &self.token_exact_ranges, key, left)
    }
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

fn estimate_candidate_counts(
    index: &DirectIndex,
    include_bm25: bool,
    phase: &str,
    progress: &dyn ProgressObserver,
) -> Result<CandidateCounts, DedupError> {
    progress.begin_phase(phase, Some(index.profiles.len() as u64));
    index
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
                    for &(_, document) in index.anchors(profile) {
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
        })
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
    let bytes_per_lane = term_count
        .checked_mul(std::mem::size_of::<usize>())
        .ok_or_else(|| DedupError::invalid("metadata", "global posting cursor overflow"))?;
    let max_budget_lanes = (DENSE_TERM_FREQUENCY_BUDGET_BYTES / bytes_per_lane).max(1);
    let average_reuse = posting_count.div_ceil(term_count).max(1);
    let desired_lanes = average_reuse.saturating_mul(2).max(8);
    let lane_count = rayon::current_num_threads()
        .min(index.profiles.len())
        .min(max_budget_lanes)
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
    if offsets.last().copied() != Some(posting_count) {
        return Err(DedupError::invalid(
            "metadata",
            "global posting count mismatch",
        ));
    }
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
                for lane in 0..lane_count {
                    let position = lane * term_count + term;
                    // Terms are partitioned across tasks, so every cursor slot
                    // has exactly one writer during this layout pass.
                    let count = unsafe { cursor_output.replace(position, cursor) };
                    cursor += count;
                }
            }
            progress.add_completed(starts.len() as u64);
            Ok::<_, DedupError>(())
        })?;

    let mut profiles = Vec::<MaybeUninit<u32>>::with_capacity(posting_count);
    // The parallel fill below writes every assigned posting slot once.
    unsafe {
        profiles.set_len(posting_count);
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
    Ok(DensePostingIndex {
        offsets: offsets.into_boxed_slice(),
        profiles,
    })
}

fn build_full_plan(
    index: &DirectIndex,
    mut stats: CandidatePlanStats,
    progress: &dyn ProgressObserver,
) -> Result<(CrossProfilePlan, CandidatePlanStats), DedupError> {
    let exact_prepass = build_exact_prepass(index, progress)?;
    stats.prepass_pairs = exact_prepass.len() as u64;
    Ok((CrossProfilePlan::Full { exact_prepass }, stats))
}

fn build_exact_prepass(
    index: &DirectIndex,
    progress: &dyn ProgressObserver,
) -> Result<Box<[u64]>, DedupError> {
    let global_count = index.profiles.len() as u64;
    let token_count = index
        .profiles
        .par_iter()
        .filter(|profile| profile.is_evm)
        .map(|profile| u64::from(profile.anchor_len))
        .sum::<u64>();
    let posting_bytes = global_count
        .saturating_mul(std::mem::size_of::<(u32, u32)>() as u64)
        .saturating_add(token_count.saturating_mul(std::mem::size_of::<(u32, u32, u32)>() as u64));
    if posting_bytes > MAX_FULL_PREPASS_POSTING_BYTES {
        return Ok(Box::new([]));
    }

    let global_capacity = usize::try_from(global_count)
        .map_err(|_| DedupError::invalid("metadata", "exact prepass size overflow"))?;
    let token_capacity = usize::try_from(token_count)
        .map_err(|_| DedupError::invalid("metadata", "exact prepass size overflow"))?;
    let postings = Mutex::new((
        Vec::with_capacity(global_capacity),
        Vec::with_capacity(token_capacity),
    ));
    progress.begin_phase("candidate_prepass_build", Some(index.profiles.len() as u64));
    index
        .profiles
        .par_chunks(PREPARE_BATCH)
        .enumerate()
        .try_for_each(|(chunk_id, profiles)| {
            progress.check_cancelled()?;
            let mut global = Vec::with_capacity(profiles.len());
            let mut token = Vec::new();
            for (offset, profile) in profiles.iter().enumerate() {
                let profile_id = u32::try_from(chunk_id * PREPARE_BATCH + offset)
                    .map_err(|_| DedupError::invalid("metadata", "too many metadata profiles"))?;
                global.push((profile.max_document(), profile_id));
                if profile.is_evm {
                    token.extend(
                        index
                            .anchors(profile)
                            .iter()
                            .map(|&(token, document)| (token, document, profile_id)),
                    );
                }
            }
            let mut target = postings.lock().map_err(|_| {
                DedupError::invalid("metadata", "exact prepass posting lock poisoned")
            })?;
            target.0.append(&mut global);
            target.1.append(&mut token);
            progress.add_completed(profiles.len() as u64);
            Ok::<(), DedupError>(())
        })?;
    let (mut global, mut token) = postings
        .into_inner()
        .map_err(|_| DedupError::invalid("metadata", "exact prepass posting lock poisoned"))?;
    let sort_passes = u64::from(global.len() > 1) * 6 + u64::from(token.len() > 1) * 9;
    progress.begin_phase("candidate_prepass_sort", Some(sort_passes));
    if !sort_u32_pairs_while(&mut global, || {
        progress.add_completed(1);
        progress.check_cancelled().is_ok()
    }) || !sort_u32_triples_while(&mut token, || {
        progress.add_completed(1);
        progress.check_cancelled().is_ok()
    }) {
        return Err(DedupError::Interrupted);
    }

    let global_ranges = posting_ranges(&global);
    let token_ranges = triple_posting_ranges(&token);
    let global_emissions =
        estimate_allowed_pair_emissions(index, &global, &global_ranges, |entry| entry.1);
    let token_emissions =
        estimate_allowed_pair_emissions(index, &token, &token_ranges, |entry| entry.2);
    let global_target = global_emissions.min((MAX_FULL_PREPASS_PAIRS / 2) as u64) as usize;
    let token_target =
        token_emissions.min((MAX_FULL_PREPASS_PAIRS - global_target) as u64) as usize;
    let raw_pairs = global_target.saturating_add(token_target);
    progress.begin_phase("candidate_prepass_collect", Some(raw_pairs as u64));
    let mut pairs = Vec::with_capacity(raw_pairs);
    append_bounded_symmetric_pairs_by(
        index,
        &global,
        &global_ranges,
        |entry| entry.1,
        global_target,
        &mut pairs,
        progress,
    )?;
    append_bounded_symmetric_pairs_by(
        index,
        &token,
        &token_ranges,
        |entry| entry.2,
        global_target.saturating_add(token_target),
        &mut pairs,
        progress,
    )?;
    drop(global);
    drop(token);
    finalize_exact_prepass(index, pairs, progress)
}

fn finalize_exact_prepass(
    index: &DirectIndex,
    mut pairs: Vec<u64>,
    progress: &dyn ProgressObserver,
) -> Result<Box<[u64]>, DedupError> {
    let sort_passes = if pairs.len() > 1 { 6 } else { 0 };
    progress.begin_phase("candidate_prepass_dedup", Some(sort_passes));
    if !sort_u64_while(&mut pairs, || {
        progress.add_completed(1);
        progress.check_cancelled().is_ok()
    }) {
        return Err(DedupError::Interrupted);
    }
    pairs.dedup();
    progress.begin_phase("candidate_prepass_validate", Some(pairs.len() as u64));
    let chunks = pairs
        .par_chunks(PREPARE_BATCH)
        .map(|keys| {
            progress.check_cancelled()?;
            let exact = keys
                .iter()
                .copied()
                .filter(|key| {
                    let (left_id, right_id) = decode_profile_pair(*key);
                    let left = &index.profiles[left_id];
                    let right = &index.profiles[right_id];
                    if !should_compare_profiles(left, right) {
                        return false;
                    }
                    let (left_document, right_document) =
                        selected_documents(left, index.anchors(left), right, index.anchors(right));
                    left_document == right_document
                })
                .collect::<Vec<_>>();
            progress.add_completed(keys.len() as u64);
            Ok::<_, DedupError>(exact)
        })
        .collect::<Vec<_>>()
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    Ok(chunks
        .into_par_iter()
        .flatten()
        .collect::<Vec<_>>()
        .into_boxed_slice())
}

fn append_bounded_symmetric_pairs_by<T>(
    index: &DirectIndex,
    entries: &[T],
    ranges: &[(usize, usize)],
    profile: impl Fn(&T) -> u32,
    limit: usize,
    pairs: &mut Vec<u64>,
    progress: &dyn ProgressObserver,
) -> Result<(), DedupError> {
    let mut completed = 0_u64;
    'ranges: for &(start, end) in ranges {
        for left in start..end - 1 {
            let left_profile = profile(&entries[left]);
            if index.profiles[left_profile as usize].is_solana {
                break;
            }
            for right_entry in entries.iter().take(end).skip(left + 1) {
                if pairs.len() == limit {
                    break 'ranges;
                }
                let right_profile = profile(right_entry);
                if !should_compare_profiles(
                    &index.profiles[left_profile as usize],
                    &index.profiles[right_profile as usize],
                ) {
                    continue;
                }
                pairs.push(profile_pair_key(left_profile, right_profile));
                completed += 1;
                if completed == PREPARE_BATCH as u64 {
                    progress.add_completed(completed);
                    progress.check_cancelled()?;
                    completed = 0;
                }
            }
        }
    }
    progress.add_completed(completed);
    Ok(())
}

fn build_candidate_plan(
    index: &DirectIndex,
    threshold: f64,
    exhaustive_pairs: u64,
    progress: &dyn ProgressObserver,
) -> Result<(CrossProfilePlan, CandidatePlanStats), DedupError> {
    if threshold <= 0.0 || exhaustive_pairs == 0 {
        return Ok((
            CrossProfilePlan::Full {
                exact_prepass: Box::new([]),
            },
            CandidatePlanStats::default(),
        ));
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
    if projected_posting_bytes > MAX_CANDIDATE_POSTING_BYTES {
        return build_full_plan(index, stats, progress);
    }
    let (term_ranks, prefixes) = if include_bm25 {
        let term_ranks = build_term_ranks(index, progress)?;
        let prefixes = build_document_prefixes(index, &term_ranks, threshold, progress)?;
        (term_ranks, prefixes)
    } else {
        (Vec::new(), Vec::new())
    };
    let global_full = if include_bm25 {
        build_global_full_index(index, counts.global_full, progress)?
    } else {
        DensePostingIndex::empty()
    };
    debug_assert_eq!(projected_posting_bytes, {
        counts
            .global_exact
            .saturating_mul(std::mem::size_of::<(u32, u32)>() as u64)
            .saturating_add(
                counts
                    .token_full
                    .saturating_add(counts.token_exact)
                    .saturating_mul(std::mem::size_of::<(u32, u32, u32)>() as u64),
            )
            .saturating_add(global_full.bytes())
    });

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
                            let terms = index.document_terms(document);
                            if !include_bm25 || terms.is_empty() {
                                local[candidate_shard(token, document)]
                                    .token_exact
                                    .push((token, document, profile_id));
                            }
                            if include_bm25 {
                                for &(term, _) in terms {
                                    local[candidate_shard(token, term)]
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
    let mut shards = sharded_entries
        .into_vec()
        .into_iter()
        .map(|entries| {
            entries
                .into_inner()
                .map_err(|_| DedupError::invalid("metadata", "candidate shard lock poisoned"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    debug_assert_eq!(
        shards
            .iter()
            .map(CandidateEntries::posting_entries)
            .fold(global_full.len() as u64, u64::saturating_add),
        stats.posting_entries
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
    for entries in &mut shards {
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
    }
    let sharded_posting_entries = stats
        .posting_entries
        .saturating_sub(global_full.len() as u64);
    progress.begin_phase("candidate_ranges", Some(sharded_posting_entries));
    shards.par_iter_mut().try_for_each(|entries| {
        progress.check_cancelled()?;
        let posting_entries = entries.posting_entries();
        entries.build_ranges();
        progress.add_completed(posting_entries);
        Ok::<_, DedupError>(())
    })?;
    stats.range_bytes = shards
        .iter()
        .map(CandidateEntries::range_bytes)
        .fold(0_u64, u64::saturating_add);
    if stats.posting_bytes.saturating_add(stats.range_bytes) > MAX_CANDIDATE_POSTING_BYTES {
        drop(shards);
        return build_full_plan(index, stats, progress);
    }
    let candidate_limit = exhaustive_pairs
        .min(MAX_CANDIDATE_PAIR_BYTES / std::mem::size_of::<CandidatePair>() as u64);
    let generated = generate_candidate_pairs(
        index,
        CandidateSources {
            shards: &shards,
            global_full: &global_full,
            term_ranks: &term_ranks,
            prefixes: &prefixes,
        },
        include_bm25,
        candidate_limit,
        progress,
    )?;
    stats.prefix_terms = generated.prefix_terms;
    stats.pair_emissions = generated.pair_emissions;
    stats.candidate_pairs = generated.pair_count as u64;
    stats.candidate_zero_overlap_prunes = generated.zero_overlap_prunes;
    stats.generation_fallback = generated.abandoned;
    if generated.abandoned || stats.candidate_pairs >= exhaustive_pairs {
        drop(generated);
        drop(shards);
        return build_full_plan(index, stats, progress);
    }
    Ok((
        CrossProfilePlan::Indexed(IndexedPairs::new(generated.chunks, generated.pair_count)),
        stats,
    ))
}

enum CandidateSeen {
    Dense {
        generations: Vec<u32>,
        generation: u32,
    },
    Sparse(AHashSet<u32>),
}

impl CandidateSeen {
    fn new(profile_count: usize, dense: bool) -> Self {
        if dense {
            Self::Dense {
                generations: vec![0; profile_count],
                generation: 0,
            }
        } else {
            Self::Sparse(AHashSet::new())
        }
    }

    fn begin_profile(&mut self) {
        match self {
            Self::Dense {
                generations,
                generation,
            } => {
                *generation = generation.wrapping_add(1);
                if *generation == 0 {
                    generations.fill(0);
                    *generation = 1;
                }
            }
            Self::Sparse(seen) => seen.clear(),
        }
    }

    fn insert(&mut self, profile: u32) -> bool {
        match self {
            Self::Dense {
                generations,
                generation,
            } => {
                let slot = &mut generations[profile as usize];
                if *slot == *generation {
                    false
                } else {
                    *slot = *generation;
                    true
                }
            }
            Self::Sparse(seen) => seen.insert(profile),
        }
    }
}

struct CandidateGeneration {
    chunks: Vec<Box<[CandidatePair]>>,
    pair_count: usize,
    pair_emissions: u64,
    prefix_terms: u64,
    zero_overlap_prunes: u64,
    abandoned: bool,
}

struct CandidateSources<'a> {
    shards: &'a [CandidateEntries],
    global_full: &'a DensePostingIndex,
    term_ranks: &'a [u32],
    prefixes: &'a [DocumentPrefix],
}

struct CandidatePairChunks {
    chunks: Vec<Box<[CandidatePair]>>,
    current: Vec<CandidatePair>,
    len: usize,
    unreserved: u64,
}

impl CandidatePairChunks {
    fn new() -> Self {
        Self {
            chunks: Vec::new(),
            current: Vec::with_capacity(CANDIDATE_PAIR_CHUNK),
            len: 0,
            unreserved: 0,
        }
    }

    fn push(&mut self, pair: CandidatePair, budget: &CandidateBudget) -> bool {
        if self.current.len() == CANDIDATE_PAIR_CHUNK {
            self.chunks
                .push(std::mem::take(&mut self.current).into_boxed_slice());
            self.current = Vec::with_capacity(CANDIDATE_PAIR_CHUNK);
        }
        self.current.push(pair);
        self.len += 1;
        self.unreserved += 1;
        if self.unreserved == CANDIDATE_PAIR_CHUNK as u64 {
            if !budget.reserve(self.unreserved) {
                return false;
            }
            self.unreserved = 0;
        }
        true
    }

    fn reserve_remainder(&mut self, budget: &CandidateBudget) -> bool {
        if self.unreserved == 0 {
            return !budget.exceeded.load(Ordering::Relaxed);
        }
        let amount = std::mem::take(&mut self.unreserved);
        budget.reserve(amount)
    }

    fn finish(mut self) -> (Vec<Box<[CandidatePair]>>, usize) {
        if !self.current.is_empty() {
            self.chunks.push(self.current.into_boxed_slice());
        }
        (self.chunks, self.len)
    }
}

struct CandidateBudget {
    limit: u64,
    reserved: AtomicU64,
    exceeded: AtomicBool,
}

impl CandidateBudget {
    fn new(limit: u64) -> Self {
        Self {
            limit,
            reserved: AtomicU64::new(0),
            exceeded: AtomicBool::new(false),
        }
    }

    fn reserve(&self, amount: u64) -> bool {
        if self.exceeded.load(Ordering::Relaxed) {
            return false;
        }
        let reserved =
            self.reserved
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    current
                        .checked_add(amount)
                        .filter(|next| *next <= self.limit)
                });
        if reserved.is_ok() {
            true
        } else {
            self.exceeded.store(true, Ordering::Relaxed);
            false
        }
    }
}

fn generate_candidate_pairs(
    index: &DirectIndex,
    sources: CandidateSources<'_>,
    include_bm25: bool,
    candidate_limit: u64,
    progress: &dyn ProgressObserver,
) -> Result<CandidateGeneration, DedupError> {
    let profile_count = index.profiles.len();
    let query_profile_count = index.query_profile_count;
    if query_profile_count == 0 || profile_count < 2 {
        return Ok(CandidateGeneration {
            chunks: Vec::new(),
            pair_count: 0,
            pair_emissions: 0,
            prefix_terms: 0,
            zero_overlap_prunes: 0,
            abandoned: false,
        });
    }
    let lane_count = rayon::current_num_threads().min(query_profile_count).max(1);
    let dense_bytes = profile_count
        .checked_mul(lane_count)
        .and_then(|values| values.checked_mul(std::mem::size_of::<u32>()))
        .unwrap_or(usize::MAX);
    let dense_seen = dense_bytes <= DENSE_CANDIDATE_SEEN_BUDGET_BYTES;
    let budget = CandidateBudget::new(candidate_limit);
    let next_left = AtomicUsize::new(0);
    progress.begin_phase("candidate_generate", Some(query_profile_count as u64));
    let lanes = (0..lane_count)
        .into_par_iter()
        .map(|_| {
            let mut seen = CandidateSeen::new(profile_count, dense_seen);
            let mut pairs = CandidatePairChunks::new();
            let mut pair_emissions = 0_u64;
            let mut prefix_terms = 0_u64;
            let mut zero_overlap_prunes = 0_u64;
            let mut unchecked_emissions = 0_u64;
            let mut completed = 0_u64;
            'profiles: loop {
                if budget.exceeded.load(Ordering::Relaxed) {
                    break;
                }
                progress.check_cancelled()?;
                let start = next_left.fetch_add(CANDIDATE_SCHEDULING_CHUNK, Ordering::Relaxed);
                if start >= query_profile_count {
                    break;
                }
                let end = start
                    .saturating_add(CANDIDATE_SCHEDULING_CHUNK)
                    .min(query_profile_count);
                for left_id in start..end {
                    if budget.exceeded.load(Ordering::Relaxed) {
                        break 'profiles;
                    }
                    seen.begin_profile();
                    let left_profile = &index.profiles[left_id];
                    let left_id = left_id as u32;
                    let max_document = left_profile.max_document();
                    if !append_owned_candidates(
                        sources.shards[candidate_shard(max_document, 0)]
                            .global_exact_after(max_document, left_id),
                        left_id,
                        |entry| entry.1,
                        |right| {
                            should_compare_profiles(left_profile, &index.profiles[right as usize])
                        },
                        |right| prepare_candidate_pair(index, include_bm25, left_id, right),
                        &mut seen,
                        &mut pairs,
                        &mut pair_emissions,
                        &mut zero_overlap_prunes,
                        &mut unchecked_emissions,
                        &budget,
                        progress,
                    )? {
                        break 'profiles;
                    }
                    if include_bm25 {
                        let prefix = sources.prefixes[max_document as usize];
                        prefix_terms = prefix_terms.saturating_add(u64::from(prefix.len));
                        for &(term, _) in index.document_terms(max_document) {
                            if !prefix.contains(sources.term_ranks[term as usize]) {
                                continue;
                            }
                            if !append_owned_candidates(
                                sources.global_full.posting_after(term, left_id),
                                left_id,
                                |profile| *profile,
                                |right| {
                                    should_compare_profiles(
                                        left_profile,
                                        &index.profiles[right as usize],
                                    )
                                },
                                |right| prepare_candidate_pair(index, include_bm25, left_id, right),
                                &mut seen,
                                &mut pairs,
                                &mut pair_emissions,
                                &mut zero_overlap_prunes,
                                &mut unchecked_emissions,
                                &budget,
                                progress,
                            )? {
                                break 'profiles;
                            }
                        }
                    }
                    if left_profile.is_evm {
                        for &(token, document) in index.anchors(left_profile) {
                            if !append_owned_candidates(
                                sources.shards[candidate_shard(token, document)]
                                    .token_exact_after((token, document), left_id),
                                left_id,
                                |entry| entry.2,
                                |right| {
                                    should_compare_profiles(
                                        left_profile,
                                        &index.profiles[right as usize],
                                    )
                                },
                                |right| prepare_candidate_pair(index, include_bm25, left_id, right),
                                &mut seen,
                                &mut pairs,
                                &mut pair_emissions,
                                &mut zero_overlap_prunes,
                                &mut unchecked_emissions,
                                &budget,
                                progress,
                            )? {
                                break 'profiles;
                            }
                            if include_bm25 {
                                let prefix = sources.prefixes[document as usize];
                                prefix_terms = prefix_terms.saturating_add(u64::from(prefix.len));
                                for &(term, _) in index.document_terms(document) {
                                    if !prefix.contains(sources.term_ranks[term as usize]) {
                                        continue;
                                    }
                                    if !append_owned_candidates(
                                        sources.shards[candidate_shard(token, term)]
                                            .token_full_after((token, term), left_id),
                                        left_id,
                                        |entry| entry.2,
                                        |right| {
                                            should_compare_profiles(
                                                left_profile,
                                                &index.profiles[right as usize],
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
                                        &mut pair_emissions,
                                        &mut zero_overlap_prunes,
                                        &mut unchecked_emissions,
                                        &budget,
                                        progress,
                                    )? {
                                        break 'profiles;
                                    }
                                }
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
            let _ = pairs.reserve_remainder(&budget);
            let (chunks, pair_count) = pairs.finish();
            Ok::<_, DedupError>(CandidateGeneration {
                chunks,
                pair_count,
                pair_emissions,
                prefix_terms,
                zero_overlap_prunes,
                abandoned: budget.exceeded.load(Ordering::Relaxed),
            })
        })
        .collect::<Vec<_>>()
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    let pair_count = lanes.iter().map(|lane| lane.pair_count).sum::<usize>();
    let pair_emissions = lanes.iter().fold(0_u64, |total, lane| {
        total.saturating_add(lane.pair_emissions)
    });
    let prefix_terms = lanes
        .iter()
        .fold(0_u64, |total, lane| total.saturating_add(lane.prefix_terms));
    let zero_overlap_prunes = lanes.iter().fold(0_u64, |total, lane| {
        total.saturating_add(lane.zero_overlap_prunes)
    });
    let abandoned = budget.exceeded.load(Ordering::Relaxed);
    let chunks = lanes
        .into_iter()
        .flat_map(|lane| lane.chunks)
        .collect::<Vec<_>>();
    Ok(CandidateGeneration {
        chunks,
        pair_count,
        pair_emissions,
        prefix_terms,
        zero_overlap_prunes,
        abandoned,
    })
}

#[allow(clippy::too_many_arguments)]
fn append_owned_candidates<T>(
    posting: &[T],
    left: u32,
    profile: impl Fn(&T) -> u32,
    should_compare: impl Fn(u32) -> bool,
    prepare: impl Fn(u32) -> Option<CandidatePair>,
    seen: &mut CandidateSeen,
    pairs: &mut CandidatePairChunks,
    pair_emissions: &mut u64,
    zero_overlap_prunes: &mut u64,
    unchecked_emissions: &mut u64,
    budget: &CandidateBudget,
    progress: &dyn ProgressObserver,
) -> Result<bool, DedupError> {
    for entry in posting {
        let right = profile(entry);
        debug_assert!(right > left);
        if !should_compare(right) {
            continue;
        }
        *pair_emissions = pair_emissions.saturating_add(1);
        *unchecked_emissions += 1;
        if *unchecked_emissions >= CANDIDATE_CANCEL_BATCH {
            progress.check_cancelled()?;
            if budget.exceeded.load(Ordering::Relaxed) {
                return Ok(false);
            }
            *unchecked_emissions = 0;
        }
        if seen.insert(right) {
            if let Some(candidate) = prepare(right) {
                if !pairs.push(candidate, budget) {
                    return Ok(false);
                }
            } else {
                *zero_overlap_prunes = zero_overlap_prunes.saturating_add(1);
            }
        }
    }
    Ok(true)
}

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

fn pair_posting_ranges(entries: &[(u32, u32)]) -> Vec<PairPostingRange> {
    let mut ranges = Vec::new();
    let mut start = 0;
    while start < entries.len() {
        let key = entries[start].0;
        let end = entries[start..].partition_point(|entry| entry.0 == key) + start;
        ranges.push(PairPostingRange { key, start, end });
        start = end;
    }
    ranges
}

fn compact_triple_posting_ranges(entries: &[(u32, u32, u32)]) -> Vec<TriplePostingRange> {
    let mut ranges = Vec::new();
    let mut start = 0;
    while start < entries.len() {
        let key = (entries[start].0, entries[start].1);
        let end = entries[start..].partition_point(|entry| (entry.0, entry.1) == key) + start;
        ranges.push(TriplePostingRange { key, start, end });
        start = end;
    }
    ranges
}

fn pair_posting_after<'a>(
    entries: &'a [(u32, u32)],
    ranges: &[PairPostingRange],
    key: u32,
    left: u32,
) -> &'a [(u32, u32)] {
    let Ok(range) = ranges.binary_search_by_key(&key, |range| range.key) else {
        return &[];
    };
    let range = ranges[range];
    let entries = &entries[range.start..range.end];
    let start = entries.partition_point(|entry| entry.1 <= left);
    &entries[start..]
}

fn triple_posting_after<'a>(
    entries: &'a [(u32, u32, u32)],
    ranges: &[TriplePostingRange],
    key: (u32, u32),
    left: u32,
) -> &'a [(u32, u32, u32)] {
    let Ok(range) = ranges.binary_search_by_key(&key, |range| range.key) else {
        return &[];
    };
    let range = ranges[range];
    let entries = &entries[range.start..range.end];
    let start = entries.partition_point(|entry| entry.2 <= left);
    &entries[start..]
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
    let bytes_per_lane = term_count
        .checked_mul(std::mem::size_of::<u32>())
        .ok_or_else(|| DedupError::invalid("metadata", "metadata term frequency size overflow"))?;
    let max_budget_lanes = (DENSE_TERM_FREQUENCY_BUDGET_BYTES / bytes_per_lane).max(1);
    let average_reuse = index.terms.len().div_ceil(term_count).max(1);
    let desired_lanes = average_reuse.saturating_mul(2).max(8);
    let lane_count = rayon::current_num_threads()
        .min(index.documents.len())
        .min(max_budget_lanes)
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
) -> Result<Vec<DocumentPrefix>, DedupError> {
    progress.begin_phase("candidate_prefixes", Some(index.documents.len() as u64));
    let chunks = index
        .documents
        .par_chunks(PREPARE_BATCH)
        .enumerate()
        .map_init(
            || (Vec::new(), Vec::new()),
            |(ranked, frequencies), (chunk_id, documents)| {
                progress.check_cancelled()?;
                let mut prefixes = Vec::with_capacity(documents.len());
                for (offset, _) in documents.iter().enumerate() {
                    let document = (chunk_id * PREPARE_BATCH + offset) as DocumentId;
                    ranked.clear();
                    ranked.extend(
                        index
                            .document_terms(document)
                            .iter()
                            .map(|(term, frequency)| (term_ranks[*term as usize], *frequency)),
                    );
                    ranked.sort_unstable_by_key(|(rank, _)| *rank);
                    frequencies.clear();
                    frequencies.extend(ranked.iter().map(|(_, frequency)| *frequency));
                    let len = lossless_prefix_len(frequencies, threshold);
                    prefixes.push(if len == 0 {
                        DocumentPrefix::default()
                    } else {
                        DocumentPrefix {
                            cutoff_rank: ranked[len - 1].0,
                            len: len as u32,
                        }
                    });
                }
                progress.add_completed(documents.len() as u64);
                Ok::<Vec<DocumentPrefix>, DedupError>(prefixes)
            },
        )
        .collect::<Vec<_>>()
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    progress.begin_phase(
        "candidate_prefix_flatten",
        Some(index.documents.len() as u64),
    );
    Ok(chunks
        .into_par_iter()
        .map(|chunk| {
            progress.add_completed(chunk.len() as u64);
            chunk
        })
        .flatten()
        .collect())
}

fn posting_ranges(entries: &[(u32, u32)]) -> Vec<(usize, usize)> {
    let mut ranges = Vec::new();
    let mut start = 0;
    while start < entries.len() {
        let mut end = start + 1;
        while end < entries.len() && entries[end].0 == entries[start].0 {
            end += 1;
        }
        if end - start > 1 {
            ranges.push((start, end));
        }
        start = end;
    }
    ranges
}

fn triple_posting_ranges(entries: &[(u32, u32, u32)]) -> Vec<(usize, usize)> {
    let mut ranges = Vec::new();
    let mut start = 0;
    while start < entries.len() {
        let key = (entries[start].0, entries[start].1);
        let mut end = start + 1;
        while end < entries.len() && (entries[end].0, entries[end].1) == key {
            end += 1;
        }
        if end - start > 1 {
            ranges.push((start, end));
        }
        start = end;
    }
    ranges
}

fn estimate_allowed_pair_emissions<T>(
    index: &DirectIndex,
    entries: &[T],
    ranges: &[(usize, usize)],
    profile: impl Fn(&T) -> u32,
) -> u64 {
    ranges.iter().fold(0_u64, |total, &(start, end)| {
        let query_end = (start..end)
            .find(|&position| index.profiles[profile(&entries[position]) as usize].is_solana)
            .unwrap_or(end);
        let query_count = (query_end - start) as u64;
        let solana_count = (end - query_end) as u64;
        let evm_count = entries[start..query_end]
            .iter()
            .filter(|entry| index.profiles[profile(entry) as usize].is_evm)
            .count() as u64;
        total
            .saturating_add(choose_two(query_count))
            .saturating_add(evm_count.saturating_mul(solana_count))
    })
}

fn build_profile_bucket(
    bucket: Vec<RawProfile>,
    progress: &dyn ProgressObserver,
) -> Result<CompactProfiles, DedupError> {
    progress.check_cancelled()?;
    let mut profile_ids: AHashMap<ProfileKey, usize> = AHashMap::new();
    let mut builders: Vec<(ProfileMembers, ProfileChainCounts)> = Vec::new();
    let completed = bucket.len() as u64;
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
    progress.add_completed(completed);
    compact_profiles(profiles)
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

fn seed_exact_prepass(
    index: &DirectIndex,
    hits: &ProfileHits,
    pairs: &[u64],
    progress: &dyn ProgressObserver,
) -> Result<(), DedupError> {
    if pairs.is_empty() {
        return Ok(());
    }
    progress.begin_phase("direct_bm25_exact_prepass", Some(pairs.len() as u64));
    let cancelled = AtomicBool::new(false);
    pairs.par_chunks(PREPARE_BATCH).for_each(|keys| {
        if progress.check_cancelled().is_err() {
            cancelled.store(true, Ordering::Relaxed);
            return;
        }
        for &key in keys {
            let (left_id, right_id) = decode_profile_pair(key);
            let left = &index.profiles[left_id];
            let right = &index.profiles[right_id];
            if hits.is_single_word() {
                hits.insert_mask(left_id, right.chain_mask);
                hits.insert_mask(right_id, left.chain_mask);
            } else {
                hits.insert_profile_chains(left_id, right, index.chains(right));
                hits.insert_profile_chains(right_id, left, index.chains(left));
            }
        }
        progress.add_completed(keys.len() as u64);
    });
    if cancelled.load(Ordering::Relaxed) {
        Err(DedupError::Interrupted)
    } else {
        Ok(())
    }
}

fn score_equivalent_profiles(
    index: &DirectIndex,
    hits: &ProfileHits,
    stats: &AtomicStats,
    progress: &dyn ProgressObserver,
) -> Result<(), DedupError> {
    let cancelled = AtomicBool::new(false);
    index
        .profiles
        .par_chunks(PREPARE_BATCH)
        .enumerate()
        .for_each(|(chunk_id, profiles)| {
            if progress.check_cancelled().is_err() {
                cancelled.store(true, Ordering::Relaxed);
                return;
            }
            let mut completed = 0_u64;
            let mut equivalent = 0_u64;
            for (offset, profile) in profiles.iter().enumerate() {
                if profile.is_solana || profile.member_len < 2 {
                    continue;
                }
                let profile_id = chunk_id * PREPARE_BATCH + offset;
                for &(chain, count) in index.chains(profile) {
                    if count > 1 {
                        hits.insert(profile_id, chain);
                    }
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
        });
    if cancelled.load(Ordering::Relaxed) {
        Err(DedupError::Interrupted)
    } else {
        Ok(())
    }
}

struct WorkerScorer<'a> {
    index: &'a DirectIndex,
    hits: &'a ProfileHits,
    cache: Option<&'a ScoreCache>,
    threshold: f64,
    single_chain_word: bool,
    local_cache: Option<LocalScoreCache>,
    local_stats: LocalStats,
    cache_enabled: bool,
    cache_sample_complete: bool,
    cache_probes: u64,
    cache_hits: u64,
}

impl<'a> WorkerScorer<'a> {
    fn new(
        index: &'a DirectIndex,
        hits: &'a ProfileHits,
        cache: Option<&'a ScoreCache>,
        threshold: f64,
    ) -> Self {
        Self {
            index,
            hits,
            cache,
            threshold,
            single_chain_word: hits.is_single_word(),
            local_cache: cache.map(|_| LocalScoreCache::new()),
            local_stats: LocalStats::default(),
            cache_enabled: cache.is_some(),
            cache_sample_complete: false,
            cache_probes: 0,
            cache_hits: 0,
        }
    }

    fn score_pair(
        &mut self,
        left_id: usize,
        right_id: usize,
        prepared_documents: Option<(DocumentId, DocumentId)>,
    ) -> u64 {
        let left = &self.index.profiles[left_id];
        let right = &self.index.profiles[right_id];
        let completed = u64::from(left.member_len).saturating_mul(u64::from(right.member_len));
        let saturated = if self.single_chain_word {
            self.hits.contains_mask(left_id, right.chain_mask)
                && self.hits.contains_mask(right_id, left.chain_mask)
        } else {
            self.hits
                .contains_profile_chains(left_id, right, self.index.chains(right))
                && self
                    .hits
                    .contains_profile_chains(right_id, left, self.index.chains(left))
        };
        if saturated {
            self.local_stats.saturated_profile_pairs += 1;
            return completed;
        }
        let (left_document, right_document) = prepared_documents.unwrap_or_else(|| {
            selected_documents(
                left,
                self.index.anchors(left),
                right,
                self.index.anchors(right),
            )
        });
        let matched = if left_document == right_document {
            self.local_stats.exact_document_pairs += 1;
            true
        } else {
            let cache_key = document_pair_key(left_document, right_document);
            let use_cache = self.cache_enabled
                && self
                    .index
                    .document_pair_may_repeat(left_document, right_document);
            let cached = if use_cache {
                self.cache_probes += 1;
                self.local_stats.bm25_cache_probes += 1;
                self.local_cache
                    .as_ref()
                    .and_then(|local| local.get(cache_key))
                    .or_else(|| self.cache.and_then(|global| global.get(cache_key)))
            } else {
                self.local_stats.bm25_cache_bypassed_pairs += 1;
                None
            };
            if let Some(value) = cached {
                self.cache_hits += 1;
                self.local_stats.bm25_cache_hits += 1;
                if let Some(local) = &mut self.local_cache {
                    local.insert(cache_key, value);
                }
                value
            } else {
                let value = score_document_pair(
                    self.index,
                    left_document,
                    right_document,
                    self.threshold,
                    &mut self.local_stats,
                );
                if use_cache {
                    if let Some(global) = self.cache {
                        global.insert(cache_key, value);
                    }
                    if let Some(local) = &mut self.local_cache {
                        local.insert(cache_key, value);
                    }
                }
                value
            }
        };
        if matched {
            self.local_stats.matched_profile_pairs += 1;
            if self.single_chain_word {
                self.hits.insert_mask(left_id, right.chain_mask);
                self.hits.insert_mask(right_id, left.chain_mask);
            } else {
                self.hits
                    .insert_profile_chains(left_id, right, self.index.chains(right));
                self.hits
                    .insert_profile_chains(right_id, left, self.index.chains(left));
            }
        }
        self.update_cache_policy();
        completed
    }

    fn update_cache_policy(&mut self) {
        if self.cache_enabled
            && !self.cache_sample_complete
            && self.cache_probes >= CACHE_SAMPLE_PAIRS
        {
            self.cache_sample_complete = true;
            if self.cache_hits.saturating_mul(100)
                < self.cache_probes.saturating_mul(MIN_CACHE_HIT_PERCENT)
            {
                self.cache_enabled = false;
                if let Some(local) = &mut self.local_cache {
                    local.clear();
                }
            }
        }
    }

    fn flush(self, stats: &AtomicStats) {
        self.local_stats.flush(stats);
    }
}

struct ScoreBlockInfo {
    start: usize,
    end: usize,
    chain_mask: u64,
    member_sum: u64,
    equivalent_member_pairs: u64,
    all_evm: bool,
    all_solana: bool,
    has_solana: bool,
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
                all_evm: profiles.iter().all(|profile| profile.is_evm),
                all_solana: profiles.iter().all(|profile| profile.is_solana),
                has_solana: profiles.iter().any(|profile| profile.is_solana),
            }
        })
        .collect()
}

fn score_block_pair_all_allowed(left: &ScoreBlockInfo, right: &ScoreBlockInfo) -> bool {
    if left.all_solana {
        right.all_evm
    } else if right.all_solana {
        left.all_evm
    } else {
        !left.has_solana && !right.has_solana
    }
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

fn score_cross_profiles(
    index: &DirectIndex,
    hits: &ProfileHits,
    threshold: f64,
    stats: &AtomicStats,
    progress: &dyn ProgressObserver,
    plan: &CrossProfilePlan,
) -> Result<(), DedupError> {
    if matches!(plan, CrossProfilePlan::Indexed(pairs) if pairs.is_empty()) {
        return Ok(());
    }
    let cache = index
        .document_references
        .iter()
        .any(|references| *references > 1)
        .then(ScoreCache::new);
    if let CrossProfilePlan::Indexed(pairs) = plan {
        return score_indexed_profiles(
            index,
            hits,
            threshold,
            stats,
            progress,
            cache.as_ref(),
            pairs,
        );
    }

    let cancelled = AtomicBool::new(false);
    let blocks = build_score_blocks(index);
    let tiles = build_score_tiles(blocks.len());
    let left_tile_count = index.query_profile_count.div_ceil(SCORE_TILE) as u64;
    let right_tile_count = tiles.len() as u64;
    let tile_count = upper_rect_tile_count(left_tile_count, right_tile_count);
    let next_tile = AtomicU64::new(0);
    let workers = rayon::current_num_threads().max(1);
    (0..workers).into_par_iter().for_each(|_| {
        let mut scorer = WorkerScorer::new(index, hits, cache.as_ref(), threshold);
        'work: loop {
            let scheduled = next_tile.load(Ordering::Relaxed);
            let remaining = tile_count.saturating_sub(scheduled);
            let tile_batch = if remaining > (workers as u64).saturating_mul(MAX_SCORE_TILE_BATCH) {
                MAX_SCORE_TILE_BATCH
            } else {
                1
            };
            let tile_start = next_tile.fetch_add(tile_batch, Ordering::Relaxed);
            if tile_start >= tile_count || cancelled.load(Ordering::Relaxed) {
                break;
            }
            let tile_end = tile_start.saturating_add(tile_batch).min(tile_count);
            for tile_index in tile_start..tile_end {
                if cancelled.load(Ordering::Relaxed) {
                    break 'work;
                }
                if progress.check_cancelled().is_err() {
                    cancelled.store(true, Ordering::Relaxed);
                    break 'work;
                }
                let (left_tile_index, right_tile_index) =
                    upper_rect_tile_coordinate(tile_index, left_tile_count, right_tile_count);
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
                        if left_block.all_solana && right_block.all_solana {
                            continue;
                        }
                        if score_block_pair_all_allowed(left_block, right_block)
                            && hits.is_single_word()
                            && hits.block_contains_mask(left_block_index, right_block.chain_mask)
                            && hits.block_contains_mask(right_block_index, left_block.chain_mask)
                        {
                            let (skipped_profiles, skipped_work) = if left_block_index
                                == right_block_index
                            {
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
                                    left_block.member_sum.saturating_mul(right_block.member_sum),
                                )
                            };
                            stats
                                .saturated_profile_pairs
                                .fetch_add(skipped_profiles, Ordering::Relaxed);
                            stats
                                .block_saturated_profile_pairs
                                .fetch_add(skipped_profiles, Ordering::Relaxed);
                            completed = completed.saturating_add(skipped_work);
                            continue;
                        }
                        for left_id in left_block.start..left_block.end {
                            let first_right = if left_block_index == right_block_index {
                                left_id + 1
                            } else {
                                right_block.start
                            };
                            for right_id in first_right..right_block.end {
                                if !should_compare_profiles(
                                    &index.profiles[left_id],
                                    &index.profiles[right_id],
                                ) {
                                    continue;
                                }
                                completed = completed
                                    .saturating_add(scorer.score_pair(left_id, right_id, None));
                            }
                        }
                    }
                }
                progress.add_completed(completed);
            }
        }
        scorer.flush(stats);
    });
    if cancelled.load(Ordering::Relaxed) {
        Err(DedupError::Interrupted)
    } else {
        Ok(())
    }
}

fn score_indexed_profiles(
    index: &DirectIndex,
    hits: &ProfileHits,
    threshold: f64,
    stats: &AtomicStats,
    progress: &dyn ProgressObserver,
    cache: Option<&ScoreCache>,
    pairs: &IndexedPairs,
) -> Result<(), DedupError> {
    let cancelled = AtomicBool::new(false);
    let next_chunk = AtomicU64::new(0);
    let chunk_count = pairs.chunks.len() as u64;
    let workers = rayon::current_num_threads().max(1);
    (0..workers).into_par_iter().for_each(|_| {
        let mut scorer = WorkerScorer::new(index, hits, cache, threshold);
        'work: loop {
            let chunk = next_chunk.fetch_add(1, Ordering::Relaxed);
            if chunk >= chunk_count || cancelled.load(Ordering::Relaxed) {
                break;
            }
            if progress.check_cancelled().is_err() {
                cancelled.store(true, Ordering::Relaxed);
                break;
            }
            let mut completed = 0_u64;
            for &candidate in &pairs.chunks[chunk as usize] {
                if cancelled.load(Ordering::Relaxed) {
                    break 'work;
                }
                let (left, right) = candidate.profiles();
                completed = completed.saturating_add(scorer.score_pair(
                    left,
                    right,
                    Some(candidate.documents()),
                ));
            }
            progress.add_completed(completed);
        }
        scorer.flush(stats);
    });
    if cancelled.load(Ordering::Relaxed) {
        Err(DedupError::Interrupted)
    } else {
        Ok(())
    }
}

fn score_document_pair(
    index: &DirectIndex,
    left: DocumentId,
    right: DocumentId,
    threshold: f64,
    stats: &mut LocalStats,
) -> bool {
    stats.bm25_scores += 1;
    let decision = similarity_at_least(
        &index.documents[left as usize],
        index.document_terms(left),
        &index.documents[right as usize],
        index.document_terms(right),
        threshold,
    );
    if decision.zero_overlap_pruned {
        stats.bm25_zero_overlap_prunes += 1;
    }
    if decision.upper_bound_pruned {
        stats.bm25_upper_bound_prunes += 1;
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
    if left.is_evm
        && right.is_evm
        && left
            .token_mask
            .iter()
            .zip(right.token_mask)
            .any(|(left, right)| left & right != 0)
    {
        for left_anchor in left_anchors.iter().rev() {
            let (word, bit) = token_bit(left_anchor.0);
            if right.token_mask[word] & bit == 0 {
                continue;
            }
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
}

#[derive(Default)]
struct MetadataScopeMembers {
    contracts: AHashSet<ContractId>,
    duplicate_nft_count: u64,
}

impl MetadataScopeMembers {
    fn insert(&mut self, store: &EntityStore, member: MetadataMember) {
        self.contracts.insert(member.contract_id);
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

    fn into_counts(self) -> ScopeCounts {
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
    if !store.is_solana_chain(primary_chain) && duplicate_mask & own_bit != 0 {
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
    if !store.is_solana_chain(primary_chain) && intra_chain {
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

fn scoring_work(index: &DirectIndex, plan: &CrossProfilePlan) -> u64 {
    let equivalent = index.profiles.iter().fold(0_u64, |total, profile| {
        if profile.is_solana {
            total
        } else {
            total.saturating_add(choose_two(u64::from(profile.member_len)))
        }
    });
    let cross = match plan {
        CrossProfilePlan::Full { .. } => index.profiles[..index.query_profile_count]
            .iter()
            .enumerate()
            .fold(0_u64, |total, (left_id, left)| {
                index.profiles[left_id + 1..]
                    .iter()
                    .filter(|right| should_compare_profiles(left, right))
                    .fold(total, |total, right| {
                        total.saturating_add(
                            u64::from(left.member_len).saturating_mul(u64::from(right.member_len)),
                        )
                    })
            }),
        CrossProfilePlan::Indexed(pairs) => pairs.iter().fold(0_u64, |total, candidate| {
            let (left, right) = candidate.profiles();
            total.saturating_add(
                u64::from(index.profiles[left].member_len)
                    .saturating_mul(u64::from(index.profiles[right].member_len)),
            )
        }),
    };
    equivalent.saturating_add(cross)
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

fn score_cache_slot(key: u64, slots: usize) -> usize {
    let mixed = key ^ (key >> 33) ^ (key << 11);
    mixed as usize & (slots - 1)
}

fn document_pair_key(left: DocumentId, right: DocumentId) -> u64 {
    let (left, right) = if left <= right {
        (left, right)
    } else {
        (right, left)
    };
    (u64::from(left) << 32) | u64::from(right)
}

fn profile_pair_key(left: u32, right: u32) -> u64 {
    let (left, right) = if left <= right {
        (left, right)
    } else {
        (right, left)
    };
    (u64::from(left) << 32) | u64::from(right)
}

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
            json: canonical_json.to_owned(),
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

        let (prepared, terms, _) = documents.into_documents(&NoopProgress).unwrap();
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
    }

    fn profile(is_evm: bool, anchors: &[(u32, u32)]) -> ContractProfile {
        let max_document = anchors.last().unwrap().1;
        ContractProfile {
            is_evm,
            is_solana: false,
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

    #[test]
    fn owned_candidate_generation_deduplicates_and_checks_cancellation() {
        let entries = vec![(7, 1), (7, 1), (7, 2)];
        let mut seen = CandidateSeen::new(3, true);
        seen.begin_profile();
        let mut pairs = CandidatePairChunks::new();
        let mut emissions = 0;
        let mut zero_overlap_prunes = 0;
        let mut unchecked = 0;
        let budget = CandidateBudget::new(u64::MAX);
        append_owned_candidates(
            &entries,
            0,
            |entry| entry.1,
            |_| true,
            |right| Some(CandidatePair::new(0, right, 0, right)),
            &mut seen,
            &mut pairs,
            &mut emissions,
            &mut zero_overlap_prunes,
            &mut unchecked,
            &budget,
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

        let mut seen = CandidateSeen::new(3, false);
        seen.begin_profile();
        let mut unchecked = CANDIDATE_CANCEL_BATCH - 1;
        assert!(matches!(
            append_owned_candidates(
                &entries[..2],
                0,
                |entry| entry.1,
                |_| true,
                |right| Some(CandidatePair::new(0, right, 0, right)),
                &mut seen,
                &mut CandidatePairChunks::new(),
                &mut 0,
                &mut 0,
                &mut unchecked,
                &budget,
                &CancelledProgress,
            ),
            Err(DedupError::Interrupted)
        ));
    }

    #[test]
    fn candidate_pairs_stay_in_bounded_zero_copy_score_chunks() {
        let expected_len = CANDIDATE_PAIR_CHUNK * 2 + 7;
        let mut builder = CandidatePairChunks::new();
        let budget = CandidateBudget::new(u64::MAX);
        for pair in 0..expected_len as u64 {
            assert!(builder.push(
                CandidatePair {
                    profile_key: pair,
                    document_key: pair,
                },
                &budget,
            ));
        }
        assert!(builder.reserve_remainder(&budget));
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
    fn candidate_pair_budget_stops_at_a_chunk_boundary() {
        let budget = CandidateBudget::new(CANDIDATE_PAIR_CHUNK as u64 - 1);
        let mut builder = CandidatePairChunks::new();
        for pair in 0..CANDIDATE_PAIR_CHUNK {
            let admitted = builder.push(
                CandidatePair {
                    profile_key: pair as u64,
                    document_key: pair as u64,
                },
                &budget,
            );
            if pair + 1 == CANDIDATE_PAIR_CHUNK {
                assert!(!admitted);
            } else {
                assert!(admitted);
            }
        }
        assert!(budget.exceeded.load(Ordering::Relaxed));
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
        assert_eq!(postings.len() as u64, counts.global_full);

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
            assert_eq!(
                &postings.profiles[postings.offsets[term]..postings.offsets[term + 1]],
                expected
            );
            for left in 0..index.profiles.len() as u32 {
                assert_eq!(
                    postings.posting_after(term as u32, left),
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
    fn no_shared_token_uses_both_max_documents() {
        let left = profile(true, &[(1, 10), (2, 20)]);
        let right = profile(true, &[(3, 30), (4, 40)]);
        assert_eq!(
            selected_documents(&left, &[(1, 10), (2, 20)], &right, &[(3, 30), (4, 40)]),
            (20, 40)
        );
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
            let left_anchors = (0..1 + seed as usize % INLINE_ANCHORS)
                .map(|index| {
                    (
                        seed.wrapping_mul(17)
                            .wrapping_add((index as u32).wrapping_mul(29))
                            % 97,
                        100 + index as u32,
                    )
                })
                .collect::<Vec<_>>();
            let right_anchors = (0..1 + (seed as usize / 3) % INLINE_ANCHORS)
                .map(|index| {
                    (
                        seed.wrapping_mul(31)
                            .wrapping_add((index as u32).wrapping_mul(13))
                            % 97,
                        200 + index as u32,
                    )
                })
                .collect::<Vec<_>>();
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
    fn upper_rect_tiles_exclude_passive_solana_rows() {
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
    fn score_cache_never_returns_a_colliding_key() {
        let cache = ScoreCache::new();
        let first = 0_u64;
        let collision = (1_u64..)
            .find(|candidate| cache.slot(*candidate) == cache.slot(first))
            .unwrap();
        cache.insert(first, false);
        assert_eq!(cache.get(first), Some(false));
        cache.insert(collision, true);
        assert_eq!(cache.get(collision), Some(true));
        assert_eq!(cache.get(first), None);
    }

    #[test]
    fn local_score_cache_never_returns_a_colliding_key() {
        let mut cache = LocalScoreCache::new();
        let first = 0_u64;
        let collision = (1_u64..)
            .find(|candidate| cache.slot(*candidate) == cache.slot(first))
            .unwrap();
        cache.insert(first, false);
        assert_eq!(cache.get(first), Some(false));
        cache.insert(collision, true);
        assert_eq!(cache.get(collision), Some(true));
        assert_eq!(cache.get(first), None);
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
        assert!(std::mem::size_of::<ContractProfile>() <= 80);
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
        let stats = run_direct(&store, &evm, 8, 0.6, &mut acc, &NoopProgress).unwrap();
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
    }

    #[test]
    fn solana_participates_cross_chain_per_nft_but_not_intra_chain() {
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
        let mut acc = SummaryAccumulator::default();
        let stats = run_direct(&store, &evm, 8, 0.6, &mut acc, &NoopProgress).unwrap();

        assert_eq!(stats.eligible_contracts, 4);
        assert_eq!(stats.logical_contract_pairs, 7);
        assert_eq!(stats.equivalent_profile_tasks, 1);
        assert_eq!(stats.profile_pair_tasks, 2);
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
        assert_eq!(count(ScopeKind::IntraChain, solana, None), None);
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
    fn solana_profiles_are_passive_and_never_score_each_other() {
        let evm = ["ethereum".to_owned()].into_iter().collect::<HashSet<_>>();
        let mut store = EntityStore::with_options(8, &evm.iter().cloned().collect());
        for (chain, address, token, metadata) in [
            (
                "solana",
                "sol-a",
                "mint-1",
                r#"{"collection":"shared","name":"alpha left"}"#,
            ),
            (
                "solana",
                "sol-b",
                "mint-2",
                r#"{"collection":"shared","name":"alpha right"}"#,
            ),
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
        assert_eq!(index.query_profile_count, 1);
        assert!(
            index.profiles[..index.query_profile_count]
                .iter()
                .all(|profile| !profile.is_solana)
        );
        assert!(
            index.profiles[index.query_profile_count..]
                .iter()
                .all(|profile| profile.is_solana)
        );
        assert_eq!(index.exhaustive_profile_pairs(), 2);

        let mut acc = SummaryAccumulator::default();
        let indexed = run_direct(&store, &evm, 8, 0.6, &mut acc, &NoopProgress).unwrap();
        assert!(indexed.candidate_index_used);
        assert_eq!(indexed.candidate_profile_pairs, 0);
        assert_eq!(indexed.profile_pair_tasks, 0);
        assert_eq!(indexed.bm25_scores, 0);
        assert!(acc.counts().is_empty());

        let mut full_acc = SummaryAccumulator::default();
        let full = run_direct(&store, &evm, 8, 0.0, &mut full_acc, &NoopProgress).unwrap();
        assert!(!full.candidate_index_used);
        assert_eq!(full.profile_pair_tasks, 2);
        assert_eq!(
            full.saturated_profile_pairs
                + full.exact_document_pairs
                + full.bm25_cache_hits
                + full.bm25_scores,
            2
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
    fn full_fallback_keeps_bounded_exact_prepass() {
        let evm = HashSet::new();
        let mut store = EntityStore::with_options(2, &evm.iter().cloned().collect());
        for contract in 0..3 {
            store
                .try_ingest_row(input(
                    "other",
                    &format!("contract-{contract}"),
                    "A",
                    &format!(r#"{{"unique":"value-{contract}"}}"#),
                ))
                .unwrap();
            store
                .try_ingest_row(input(
                    "other",
                    &format!("contract-{contract}"),
                    "Z",
                    r#"{"selected":"identical"}"#,
                ))
                .unwrap();
        }
        let index = build_index(&store, &evm, 2, &NoopProgress).unwrap();
        let exhaustive = choose_two(index.profiles.len() as u64);
        let (plan, stats) = build_candidate_plan(&index, 0.6, exhaustive, &NoopProgress).unwrap();
        let CrossProfilePlan::Full { exact_prepass } = plan else {
            panic!("all profile pairs should make the candidate index fall back");
        };
        assert_eq!(exact_prepass.len() as u64, exhaustive);
        assert_eq!(stats.prepass_pairs, exhaustive);
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
        assert!(candidate_stats.prefix_terms > 0);
        assert!(candidate_stats.prefix_terms < candidate_stats.full_terms);
        let CrossProfilePlan::Indexed(candidates) = plan else {
            panic!("sparse fixture should use the lossless candidate index");
        };
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
        assert_eq!(candidates.len() as u64, candidate_stats.candidate_pairs);

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
            let (plan, stats) =
                build_candidate_plan(&index, threshold, exhaustive, &NoopProgress).unwrap();
            let CrossProfilePlan::Indexed(candidates) = plan else {
                panic!("generated sparse fixture should use the candidate index");
            };
            let unique = candidates
                .iter()
                .map(|candidate| candidate.profile_key)
                .collect::<HashSet<_>>();
            assert_eq!(unique.len(), candidates.len());
            assert_eq!(stats.candidate_pairs, candidates.len() as u64);
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
            "candidate_build",
            "candidate_sort",
            "candidate_ranges",
            "candidate_generate",
        ] {
            progress.assert_complete(phase);
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
        let counts =
            estimate_candidate_counts(&index, true, "candidate_admission", &NoopProgress).unwrap();
        assert_eq!(counts.global_exact, 0);
        assert_eq!(counts.token_exact, 0);
        let exhaustive = choose_two(index.profiles.len() as u64);
        let (plan, _) = build_candidate_plan(&index, 0.6, exhaustive, &NoopProgress).unwrap();
        let CrossProfilePlan::Indexed(candidates) = plan else {
            panic!("sparse non-empty fixture should use the candidate index");
        };
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
        let empty_counts =
            estimate_candidate_counts(&empty_index, true, "candidate_admission", &NoopProgress)
                .unwrap();
        assert!(empty_counts.token_exact >= 2);
        let exhaustive = choose_two(empty_index.profiles.len() as u64);
        let (plan, _) = build_candidate_plan(&empty_index, 0.6, exhaustive, &NoopProgress).unwrap();
        let CrossProfilePlan::Indexed(candidates) = plan else {
            panic!("sparse empty-term fixture should use the candidate index");
        };
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
