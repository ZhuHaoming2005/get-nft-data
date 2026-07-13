use std::borrow::Cow;
use std::collections::{hash_map::RandomState, HashMap};
use std::hash::BuildHasher;
use std::path::Path;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
#[cfg(test)]
use std::sync::Mutex;
#[cfg(test)]
use std::time::Duration;

use duckdb::Connection;
use rayon::prelude::*;

use super::super::{
    arrow_i64_column, arrow_string_column, chain_pair_index, AnalysisError, SparseUnionFind,
    UnionFind,
};
use super::bm25::{
    compact_metadata_content_docs_share_token, compact_metadata_content_pair_score,
    CompactMetadataContentDocument, CompactMetadataScoring, InternedMetadataCorpus,
    InternedMetadataSourceDoc, MetadataBm25Document, PreparedInternedMetadataDoc,
    PreparedInternedMetadataQuery,
};
#[cfg(test)]
use super::bm25::{CompactMetadataContentSet, CompactMetadataPostings, MetadataContentRecord};
use super::parse::metadata_document_from_json;
use super::{
    metadata_contract_index_from_usize, metadata_contract_index_to_usize,
    metadata_doc_index_from_usize, metadata_doc_index_to_usize, CompactContractTokens,
    MetadataContractIndex, MetadataData, MetadataDocIndex, SourceMetadataDocEntry,
    METADATA_CONTENT_PARALLEL_MIN_RECORDS, METADATA_CONTENT_SCORE_BATCH_PAIRS, METADATA_THRESHOLD,
};
#[cfg(test)]
use super::{MetadataDocPair, MetadataTemplateMatches, METADATA_PAIR_LEFT_CHUNK_SIZE};

pub(super) const METADATA_RAW_GROUP_CHUNK_SIZE: usize = 1024;
const METADATA_TOKEN_GROUP_BATCH_MULTIPLIER: usize = 4;
const NO_METADATA_ATOM: usize = usize::MAX;
const METADATA_TEMPLATE_SCORE_CACHE_SLOTS: usize = 256;
const METADATA_DIRECT_ATOM_GROUP_SIZE: usize = 2;

pub(super) struct MetadataContentAtom {
    pub(super) chain_index: usize,
    pub(super) template_doc_index: MetadataDocIndex,
    pub(super) representative_record_index: MetadataDocIndex,
    pub(super) members: Vec<MetadataContractIndex>,
    pub(super) fallback_token_groups: Vec<MetadataFallbackTokenGroup>,
}

#[derive(Debug)]
pub(super) struct MetadataFallbackTokenGroup {
    pub(super) members: Vec<MetadataContractIndex>,
}

pub(super) struct MetadataContentCandidateIndex {
    posting_offsets: Vec<u64>,
    posting_atoms: Vec<MetadataDocIndex>,
}

pub(super) struct MetadataTemplateCandidateIndex {
    full_entries: Vec<(u32, MetadataDocIndex)>,
    prefix_entries: Vec<(u32, MetadataDocIndex)>,
}

pub(super) enum MetadataLocalCandidateIndex {
    Adaptive {
        template: MetadataTemplateCandidateIndex,
        content: MetadataContentCandidateIndex,
    },
    #[cfg(test)]
    ContentOnly(MetadataContentCandidateIndex),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum MetadataLocalCandidateBasis {
    Template,
    Content,
}

#[derive(Default)]
struct CompactMetadataContentGroupBuilder {
    token_ids: HashMap<String, u32>,
    atom_hasher: RandomState,
    atom_index_by_hash: HashMap<u64, usize>,
    next_atom_with_same_hash: Vec<usize>,
    fallback_group_index_by_hash: HashMap<(usize, u64), usize>,
    next_fallback_group_with_same_hash: Vec<Vec<usize>>,
    docs: Vec<CompactMetadataContentDocument>,
    atoms: Vec<MetadataContentAtom>,
    token_key_bytes: usize,
    term_count: usize,
    template_candidate_term_count: usize,
    member_count: usize,
    fallback_group_count: usize,
    fallback_member_count: usize,
}

#[derive(Default)]
pub(super) struct MetadataRawTokenGroup {
    raw_records: Vec<(MetadataContractIndex, String)>,
    raw_payload_bytes: usize,
    compact: CompactMetadataContentGroupBuilder,
    raw_record_count: usize,
    #[cfg(test)]
    max_raw_buffer_len: usize,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug)]
pub(super) enum MetadataContentScope {
    SharedToken,
    NoCommonToken,
}

#[derive(Debug)]
pub(crate) struct InternedMetadataIndex {
    doc_count: usize,
    pub(super) scoring: CompactMetadataScoring,
    #[cfg(test)]
    pub(super) postings: CompactMetadataPostings,
    #[cfg(test)]
    pub(super) token_ids: HashMap<String, usize>,
    #[cfg(test)]
    pub(super) build_thread_count: usize,
}

pub(super) struct MetadataCandidateScratch {
    pub(super) seen_generation: Vec<u16>,
    generation: u16,
    pub(super) candidates: Vec<MetadataDocIndex>,
}

#[cfg(test)]
pub(super) struct MetadataCandidateScratchPool {
    pub(super) doc_count: usize,
    scratches: Mutex<Vec<MetadataCandidateScratch>>,
}

#[cfg(test)]
pub(super) struct MetadataCandidateScratchLease<'a> {
    pool: &'a MetadataCandidateScratchPool,
    scratch: Option<MetadataCandidateScratch>,
}

#[cfg(test)]
pub(super) struct MetadataPairScoringContext<'a> {
    pub(super) postings: &'a CompactMetadataPostings,
    pub(super) scoring: &'a CompactMetadataScoring,
}

#[cfg(test)]
struct MetadataHitPermits {
    remaining: AtomicUsize,
    exceeded: AtomicBool,
}

#[cfg(test)]
impl MetadataHitPermits {
    fn new(remaining: usize) -> Self {
        Self {
            remaining: AtomicUsize::new(remaining),
            exceeded: AtomicBool::new(false),
        }
    }

    fn exceeded(&self) -> bool {
        self.exceeded.load(Ordering::Relaxed)
    }

    fn try_acquire(&self) -> bool {
        if self.exceeded() {
            return false;
        }
        if self
            .remaining
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            true
        } else {
            self.exceeded.store(true, Ordering::Relaxed);
            false
        }
    }
}

pub(super) struct MetadataContentUnionContext<'a> {
    pub(super) data: &'a MetadataData,
    pub(super) template_compatibility: MetadataTemplateCompatibility<'a>,
    pub(super) contract_tokens: &'a CompactContractTokens,
    pub(super) chain_count: usize,
    pub(super) pool: &'a rayon::ThreadPool,
}

#[derive(Clone, Copy)]
pub(super) enum MetadataTemplateCompatibility<'a> {
    Scored(&'a CompactMetadataScoring),
    #[cfg(test)]
    Precomputed(&'a MetadataTemplateMatches),
}

pub(super) struct MetadataUnionState {
    pub(super) intra: UnionFind,
    pub(super) cross: Option<SparseUnionFind>,
    pub(super) chain_matrix: Option<Vec<SparseUnionFind>>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct MetadataContentUnionStats {
    pub(super) atom_count: usize,
    pub(super) candidate_pairs: u64,
    pub(super) scored_pairs: u64,
    pub(super) template_candidate_pairs: u64,
    pub(super) template_scored_pairs: u64,
    pub(super) template_matched_pairs: u64,
}

impl MetadataContentUnionStats {
    pub(super) fn accumulate(&mut self, other: Self) {
        self.atom_count = self.atom_count.saturating_add(other.atom_count);
        self.candidate_pairs = self.candidate_pairs.saturating_add(other.candidate_pairs);
        self.scored_pairs = self.scored_pairs.saturating_add(other.scored_pairs);
        self.template_candidate_pairs = self
            .template_candidate_pairs
            .saturating_add(other.template_candidate_pairs);
        self.template_scored_pairs = self
            .template_scored_pairs
            .saturating_add(other.template_scored_pairs);
        self.template_matched_pairs = self
            .template_matched_pairs
            .saturating_add(other.template_matched_pairs);
    }

    fn accumulate_pair_scoring(&mut self, other: MetadataPairScoringStats) {
        self.scored_pairs = self.scored_pairs.saturating_add(other.content_scored_pairs);
        self.template_candidate_pairs = self
            .template_candidate_pairs
            .saturating_add(other.template_candidate_pairs);
        self.template_scored_pairs = self
            .template_scored_pairs
            .saturating_add(other.template_scored_pairs);
        self.template_matched_pairs = self
            .template_matched_pairs
            .saturating_add(other.template_matched_pairs);
    }
}

impl<'a> MetadataTemplateCompatibility<'a> {
    pub(super) fn evaluate(self, left: MetadataDocIndex, right: MetadataDocIndex) -> (bool, u64) {
        if left == right {
            return (true, 0);
        }
        match self {
            Self::Scored(scoring) => {
                let left = metadata_doc_index_to_usize(left);
                let right = metadata_doc_index_to_usize(right);
                if scoring.score(left, right) >= METADATA_THRESHOLD {
                    (true, 1)
                } else {
                    (scoring.score(right, left) >= METADATA_THRESHOLD, 2)
                }
            }
            #[cfg(test)]
            Self::Precomputed(matches) => (
                matches.matches(
                    metadata_doc_index_to_usize(left),
                    metadata_doc_index_to_usize(right),
                ),
                0,
            ),
        }
    }

    fn scoring(self) -> Option<&'a CompactMetadataScoring> {
        match self {
            Self::Scored(scoring) => Some(scoring),
            #[cfg(test)]
            Self::Precomputed(_) => None,
        }
    }

    #[cfg(test)]
    fn matches(
        self,
        left: MetadataDocIndex,
        right: MetadataDocIndex,
        stats: &mut MetadataContentUnionStats,
    ) -> bool {
        let (matched, scored) = self.evaluate(left, right);
        stats.template_candidate_pairs = stats.template_candidate_pairs.saturating_add(1);
        stats.template_scored_pairs = stats.template_scored_pairs.saturating_add(scored);
        if matched {
            stats.template_matched_pairs = stats.template_matched_pairs.saturating_add(1);
        }
        matched
    }
}

#[derive(Clone, Copy)]
struct MetadataTemplateScoreCacheEntry {
    key: u64,
    matched: bool,
    valid: bool,
}

impl MetadataTemplateScoreCacheEntry {
    const EMPTY: Self = Self {
        key: 0,
        matched: false,
        valid: false,
    };
}

pub(super) struct MetadataTemplateScoreCache {
    entries: [MetadataTemplateScoreCacheEntry; METADATA_TEMPLATE_SCORE_CACHE_SLOTS],
}

impl Default for MetadataTemplateScoreCache {
    fn default() -> Self {
        Self {
            entries: [MetadataTemplateScoreCacheEntry::EMPTY; METADATA_TEMPLATE_SCORE_CACHE_SLOTS],
        }
    }
}

impl MetadataTemplateScoreCache {
    fn slot(key: u64) -> usize {
        debug_assert!(METADATA_TEMPLATE_SCORE_CACHE_SLOTS.is_power_of_two());
        let mixed = key
            .wrapping_mul(0x9e37_79b9_7f4a_7c15)
            .wrapping_add(key.rotate_right(29));
        mixed as usize & (METADATA_TEMPLATE_SCORE_CACHE_SLOTS - 1)
    }

    pub(super) fn evaluate(
        &mut self,
        left: MetadataDocIndex,
        right: MetadataDocIndex,
        compatibility: MetadataTemplateCompatibility<'_>,
    ) -> (bool, u64) {
        if left == right {
            return (true, 0);
        }
        let (left, right) = if left < right {
            (left, right)
        } else {
            (right, left)
        };
        let key = (u64::from(left) << 32) | u64::from(right);
        let slot = Self::slot(key);
        let cached = self.entries[slot];
        if cached.valid && cached.key == key {
            return (cached.matched, 0);
        }
        let (matched, scores) = compatibility.evaluate(left, right);
        self.entries[slot] = MetadataTemplateScoreCacheEntry {
            key,
            matched,
            valid: true,
        };
        (matched, scores)
    }
}

impl CompactMetadataContentGroupBuilder {
    fn vec_bytes_upper(len: usize, element_bytes: usize) -> usize {
        if len == 0 {
            0
        } else {
            len.saturating_mul(2)
                .saturating_add(4)
                .saturating_mul(element_bytes)
        }
    }

    fn atomized_memory_bytes(&self) -> usize {
        Self::vec_bytes_upper(
            self.docs.len(),
            std::mem::size_of::<CompactMetadataContentDocument>(),
        )
        .saturating_add(Self::vec_bytes_upper(
            self.atoms.len(),
            std::mem::size_of::<MetadataContentAtom>(),
        ))
        .saturating_add(Self::vec_bytes_upper(
            self.term_count,
            std::mem::size_of::<(u32, u32)>(),
        ))
        .saturating_add(Self::vec_bytes_upper(
            self.member_count,
            std::mem::size_of::<MetadataContractIndex>(),
        ))
        .saturating_add(Self::vec_bytes_upper(
            self.fallback_group_count,
            std::mem::size_of::<MetadataFallbackTokenGroup>(),
        ))
        .saturating_add(Self::vec_bytes_upper(
            self.fallback_member_count,
            std::mem::size_of::<MetadataContractIndex>(),
        ))
    }

    fn builder_memory_bytes(&self) -> usize {
        self.atomized_memory_bytes()
            .saturating_add(super::load::hash_table_allocation_for_len_upper(
                self.token_ids.len(),
                std::mem::size_of::<(String, u32)>(),
            ))
            .saturating_add(self.token_key_bytes)
            .saturating_add(super::load::hash_table_allocation_for_len_upper(
                self.atom_index_by_hash.len(),
                std::mem::size_of::<(u64, usize)>(),
            ))
            .saturating_add(Self::vec_bytes_upper(
                self.next_atom_with_same_hash.len(),
                std::mem::size_of::<usize>(),
            ))
            .saturating_add(super::load::hash_table_allocation_for_len_upper(
                self.fallback_group_index_by_hash.len(),
                std::mem::size_of::<((usize, u64), usize)>(),
            ))
            .saturating_add(Self::vec_bytes_upper(
                self.next_fallback_group_with_same_hash.len(),
                std::mem::size_of::<Vec<usize>>(),
            ))
            .saturating_add(Self::vec_bytes_upper(
                self.fallback_group_count,
                std::mem::size_of::<usize>(),
            ))
    }

    fn scoring_peak_bytes(&self, scoring_workers: usize) -> usize {
        if self.atoms.is_empty() {
            return 0;
        }
        let candidate_entry_bytes = std::mem::size_of::<(u32, MetadataDocIndex)>();
        let content_candidate_index =
            Self::vec_bytes_upper(self.term_count, std::mem::size_of::<MetadataDocIndex>())
                .saturating_add(Self::vec_bytes_upper(
                    self.token_ids.len().saturating_add(1),
                    2usize.saturating_mul(std::mem::size_of::<u64>()),
                ));
        let template_candidate_index =
            Self::vec_bytes_upper(self.template_candidate_term_count, candidate_entry_bytes)
                .saturating_add(4usize.saturating_mul(candidate_entry_bytes));
        let uses_adaptive_index = self.atoms.len() > METADATA_DIRECT_ATOM_GROUP_SIZE;
        let candidate_index = if uses_adaptive_index {
            content_candidate_index.saturating_add(template_candidate_index)
        } else {
            0
        };
        let candidate_scratch = if uses_adaptive_index {
            self.atoms.len().saturating_mul(
                std::mem::size_of::<u16>()
                    .saturating_add(2 * std::mem::size_of::<MetadataDocIndex>()),
            )
        } else {
            0
        };
        let pair_batch_capacity = if uses_adaptive_index {
            METADATA_CONTENT_SCORE_BATCH_PAIRS
        } else {
            usize::from(self.atoms.len() == METADATA_DIRECT_ATOM_GROUP_SIZE)
        };
        let pair_batches = pair_batch_capacity
            .saturating_mul(std::mem::size_of::<(usize, MetadataDocIndex)>())
            .saturating_mul(2);
        // A parallel fold and its reduce-side accumulator can coexist for
        // every worker, so reserve both fixed-size template caches.
        let template_cache_count = if uses_adaptive_index {
            scoring_workers.max(1).saturating_mul(2)
        } else {
            pair_batch_capacity
        };
        let template_score_caches =
            template_cache_count.saturating_mul(std::mem::size_of::<MetadataTemplateScoreCache>());
        let union_scratch = self.member_count.saturating_mul(
            2usize
                .saturating_mul(std::mem::size_of::<usize>())
                .saturating_add(std::mem::size_of::<MetadataContractIndex>()),
        );
        let peak = self
            .atomized_memory_bytes()
            .saturating_add(candidate_index)
            .saturating_add(candidate_scratch)
            .saturating_add(pair_batches)
            .saturating_add(template_score_caches)
            .saturating_add(union_scratch);
        peak.saturating_add(peak.saturating_div(4))
    }

    fn ensure_within_memory_budget(
        &self,
        raw_parse_reserve_bytes: usize,
        maximum_bytes: usize,
        scoring_workers: usize,
    ) -> Result<(), AnalysisError> {
        let build_peak = self
            .builder_memory_bytes()
            .saturating_add(raw_parse_reserve_bytes);
        let peak = build_peak.max(self.scoring_peak_bytes(scoring_workers));
        if peak > maximum_bytes {
            return Err(AnalysisError::InvalidData(format!(
                "metadata content working set needs about {}, exceeding remaining analysis budget {}",
                super::super::format_byte_size(peak),
                super::super::format_byte_size(maximum_bytes)
            )));
        }
        Ok(())
    }

    fn push_document(
        &mut self,
        contract_index: MetadataContractIndex,
        document: &MetadataBm25Document,
        data: &MetadataData,
        contract_tokens: Option<&CompactContractTokens>,
    ) {
        let mut terms = Vec::with_capacity(document.unique_len());
        for (token, term_frequency) in document.terms() {
            let token_id = if let Some(&token_id) = self.token_ids.get(token.as_str()) {
                token_id
            } else {
                let token_id = u32::try_from(self.token_ids.len())
                    .expect("metadata content token dictionary exceeds u32 indexes");
                let token = token.clone();
                self.token_key_bytes = self.token_key_bytes.saturating_add(token.capacity());
                self.token_ids.insert(token, token_id);
                token_id
            };
            terms.push((
                token_id,
                u32::try_from(*term_frequency)
                    .expect("metadata content term frequency exceeds u32"),
            ));
        }
        terms.sort_unstable_by_key(|(token_id, _)| *token_id);
        self.member_count = self.member_count.saturating_add(1);
        let contract = &data.contracts[metadata_contract_index_to_usize(contract_index)];
        let atom_hash = self.atom_hasher.hash_one((
            contract.chain_index,
            contract.template_doc_index,
            terms.as_slice(),
        ));
        let mut candidate_atom = self.atom_index_by_hash.get(&atom_hash).copied();
        let mut existing_atom = None;
        while let Some(atom_index) = candidate_atom {
            let atom = &self.atoms[atom_index];
            if atom.chain_index == contract.chain_index
                && atom.template_doc_index == contract.template_doc_index
                && self.docs[metadata_doc_index_to_usize(atom.representative_record_index)].terms
                    == terms
            {
                existing_atom = Some(atom_index);
                break;
            }
            let next = self.next_atom_with_same_hash[atom_index];
            candidate_atom = (next != NO_METADATA_ATOM).then_some(next);
        }
        let atom_index = if let Some(atom_index) = existing_atom {
            self.atoms[atom_index].members.push(contract_index);
            atom_index
        } else {
            let compact_doc_index = metadata_doc_index_from_usize(self.docs.len());
            self.term_count = self.term_count.saturating_add(terms.len());
            self.docs.push(CompactMetadataContentDocument {
                len: document.len(),
                terms,
            });
            let atom_index = self.atoms.len();
            self.atoms.push(MetadataContentAtom {
                chain_index: contract.chain_index,
                template_doc_index: contract.template_doc_index,
                representative_record_index: compact_doc_index,
                members: vec![contract_index],
                fallback_token_groups: Vec::new(),
            });
            let template_doc_index = metadata_doc_index_to_usize(contract.template_doc_index);
            self.template_candidate_term_count = self
                .template_candidate_term_count
                .saturating_add(
                    data.metadata_index
                        .scoring
                        .query_tokens(template_doc_index)
                        .len(),
                )
                .saturating_add(
                    data.metadata_index
                        .scoring
                        .candidate_tokens(template_doc_index)
                        .len(),
                );
            let previous_atom = self
                .atom_index_by_hash
                .insert(atom_hash, atom_index)
                .unwrap_or(NO_METADATA_ATOM);
            self.next_atom_with_same_hash.push(previous_atom);
            atom_index
        };
        if let Some(contract_tokens) = contract_tokens {
            self.push_fallback_token_group(atom_index, contract_index, contract_tokens);
        }
    }

    fn push_fallback_token_group(
        &mut self,
        atom_index: usize,
        contract_index: MetadataContractIndex,
        contract_tokens: &CompactContractTokens,
    ) {
        let tokens = contract_tokens.tokens(metadata_contract_index_to_usize(contract_index));
        self.fallback_member_count = self.fallback_member_count.saturating_add(1);
        let token_hash = self.atom_hasher.hash_one(tokens);
        let lookup_key = (atom_index, token_hash);
        let mut candidate_group = self.fallback_group_index_by_hash.get(&lookup_key).copied();
        let mut existing_group = None;
        while let Some(group_index) = candidate_group {
            let group = &self.atoms[atom_index].fallback_token_groups[group_index];
            let representative = metadata_contract_index_to_usize(group.members[0]);
            if contract_tokens.tokens(representative) == tokens {
                existing_group = Some(group_index);
                break;
            }
            let next = self.next_fallback_group_with_same_hash[atom_index][group_index];
            candidate_group = (next != NO_METADATA_ATOM).then_some(next);
        }
        if let Some(group_index) = existing_group {
            self.atoms[atom_index].fallback_token_groups[group_index]
                .members
                .push(contract_index);
            return;
        }

        while self.next_fallback_group_with_same_hash.len() <= atom_index {
            self.next_fallback_group_with_same_hash.push(Vec::new());
        }
        let group_index = self.atoms[atom_index].fallback_token_groups.len();
        self.fallback_group_count = self.fallback_group_count.saturating_add(1);
        self.atoms[atom_index]
            .fallback_token_groups
            .push(MetadataFallbackTokenGroup {
                members: vec![contract_index],
            });
        let previous_group = self
            .fallback_group_index_by_hash
            .insert(lookup_key, group_index)
            .unwrap_or(NO_METADATA_ATOM);
        self.next_fallback_group_with_same_hash[atom_index].push(previous_group);
    }

    fn into_atomized_parts(
        self,
    ) -> (
        Vec<MetadataContentAtom>,
        Vec<CompactMetadataContentDocument>,
    ) {
        let Self { docs, atoms, .. } = self;
        (atoms, docs)
    }
}

impl MetadataRawTokenGroup {
    pub(super) fn raw_parse_reserve_bytes(&self) -> usize {
        let raw_bytes = self
            .raw_records
            .capacity()
            .saturating_mul(std::mem::size_of::<(MetadataContractIndex, String)>())
            .saturating_add(self.raw_payload_bytes);
        // Use the same adversarial high-cardinality estimate as the initial
        // metadata loader. JSON normalization, token strings, term-frequency
        // maps and Rayon result buffers coexist before online atomization.
        super::load::metadata_uncached_parse_transient_bytes(raw_bytes, 0)
    }

    fn parallel_prepare_bytes(&self) -> usize {
        self.compact
            .builder_memory_bytes()
            .saturating_add(self.raw_parse_reserve_bytes())
    }

    fn record_count(&self) -> usize {
        self.raw_record_count
    }

    fn reserve_raw_record(&mut self) -> Result<(), AnalysisError> {
        self.raw_records.try_reserve(1).map_err(|_| {
            AnalysisError::InvalidData(
                "unable to reserve bounded metadata raw-group chunk".to_string(),
            )
        })
    }

    fn projected_raw_parse_reserve_bytes(&self, candidate_payload_bytes: usize) -> usize {
        let raw_bytes = self
            .raw_records
            .capacity()
            .saturating_mul(std::mem::size_of::<(MetadataContractIndex, String)>())
            .saturating_add(self.raw_payload_bytes)
            .saturating_add(candidate_payload_bytes);
        super::load::metadata_uncached_parse_transient_bytes(raw_bytes, 0)
    }

    #[cfg(test)]
    pub(super) fn push_raw(
        &mut self,
        contract_index: MetadataContractIndex,
        metadata_json: String,
        context: &MetadataContentUnionContext<'_>,
    ) {
        self.push_raw_with_budget(contract_index, metadata_json, context, usize::MAX)
            .expect("unbounded metadata test group must fit memory");
    }

    pub(super) fn push_raw_with_budget(
        &mut self,
        contract_index: MetadataContractIndex,
        metadata_json: String,
        context: &MetadataContentUnionContext<'_>,
        maximum_bytes: usize,
    ) -> Result<(), AnalysisError> {
        let candidate_payload_bytes = metadata_json.capacity();
        self.reserve_raw_record()?;
        let projected_reserve = self.projected_raw_parse_reserve_bytes(candidate_payload_bytes);
        if !self.raw_records.is_empty()
            && self
                .compact
                .ensure_within_memory_budget(
                    projected_reserve,
                    maximum_bytes,
                    context.pool.current_num_threads(),
                )
                .is_err()
        {
            self.flush_raw(context, maximum_bytes)?;
            self.reserve_raw_record()?;
        }
        self.compact.ensure_within_memory_budget(
            self.projected_raw_parse_reserve_bytes(candidate_payload_bytes),
            maximum_bytes,
            context.pool.current_num_threads(),
        )?;
        self.raw_payload_bytes = self
            .raw_payload_bytes
            .saturating_add(candidate_payload_bytes);
        self.raw_records.push((contract_index, metadata_json));
        self.raw_record_count = self.raw_record_count.saturating_add(1);
        #[cfg(test)]
        {
            self.max_raw_buffer_len = self.max_raw_buffer_len.max(self.raw_records.len());
        }
        if self.raw_records.len() >= METADATA_RAW_GROUP_CHUNK_SIZE {
            self.flush_raw(context, maximum_bytes)?;
        }
        Ok(())
    }

    fn push_loaded_representative_with_budget(
        &mut self,
        contract_index: MetadataContractIndex,
        context: &MetadataContentUnionContext<'_>,
        maximum_bytes: usize,
    ) -> Result<(), AnalysisError> {
        self.raw_record_count = self.raw_record_count.saturating_add(1);
        if let Some(document) = context.data.contracts
            [metadata_contract_index_to_usize(contract_index)]
        .content_doc
        .as_deref()
        {
            self.compact
                .push_document(contract_index, document, context.data, None);
            self.compact.ensure_within_memory_budget(
                self.projected_raw_parse_reserve_bytes(0),
                maximum_bytes,
                context.pool.current_num_threads(),
            )?;
        }
        Ok(())
    }

    fn flush_raw(
        &mut self,
        context: &MetadataContentUnionContext<'_>,
        maximum_bytes: usize,
    ) -> Result<(), AnalysisError> {
        if self.raw_records.is_empty() {
            return Ok(());
        }
        let raw_records = std::mem::take(&mut self.raw_records);
        self.raw_payload_bytes = 0;
        if raw_records.len() >= METADATA_CONTENT_PARALLEL_MIN_RECORDS {
            let parsed = context.pool.install(|| {
                raw_records
                    .into_par_iter()
                    .map(|(contract_index, metadata_json)| {
                        metadata_content_document(context.data, &metadata_json)
                            .map(|document| (contract_index, document))
                    })
                    .collect::<Vec<_>>()
            });
            for (contract_index, document) in parsed.into_iter().flatten() {
                self.compact
                    .push_document(contract_index, document.as_ref(), context.data, None);
            }
        } else {
            for (contract_index, metadata_json) in raw_records {
                if let Some(document) = metadata_content_document(context.data, &metadata_json) {
                    self.compact.push_document(
                        contract_index,
                        document.as_ref(),
                        context.data,
                        None,
                    );
                }
            }
        }
        self.compact.ensure_within_memory_budget(
            0,
            maximum_bytes,
            context.pool.current_num_threads(),
        )?;
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn union(
        self,
        context: &MetadataContentUnionContext<'_>,
        state: &mut MetadataUnionState,
    ) -> MetadataContentUnionStats {
        self.union_with_budget(context, state, usize::MAX)
            .expect("unbounded metadata test group must fit memory")
    }

    fn union_with_budget(
        mut self,
        context: &MetadataContentUnionContext<'_>,
        state: &mut MetadataUnionState,
        maximum_bytes: usize,
    ) -> Result<MetadataContentUnionStats, AnalysisError> {
        if self.raw_record_count < 2 {
            return Ok(MetadataContentUnionStats::default());
        }
        self.flush_raw(context, maximum_bytes)?;
        drop(self.raw_records);
        self.compact.ensure_within_memory_budget(
            0,
            maximum_bytes,
            context.pool.current_num_threads(),
        )?;
        let (atoms, docs) = self.compact.into_atomized_parts();
        Ok(union_metadata_shared_token_atom_core(
            atoms, &docs, context, state,
        ))
    }

    #[cfg(test)]
    pub(super) fn raw_buffer_len(&self) -> usize {
        self.raw_records.len()
    }

    #[cfg(test)]
    pub(super) fn max_raw_buffer_len(&self) -> usize {
        self.max_raw_buffer_len
    }

    #[cfg(test)]
    pub(super) fn compact_doc_count(&self) -> usize {
        self.compact.docs.len()
    }

    #[cfg(test)]
    pub(super) fn compact_member_count(&self) -> usize {
        self.compact
            .atoms
            .iter()
            .map(|atom| atom.members.len())
            .sum()
    }
}

fn prepare_metadata_token_group_batch(
    groups: &mut Vec<MetadataRawTokenGroup>,
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
    maximum_working_bytes: usize,
) -> Result<MetadataContentUnionStats, AnalysisError> {
    if groups.len() > 1 {
        context.pool.install(|| {
            groups
                .par_iter_mut()
                .try_for_each(|group| group.flush_raw(context, maximum_working_bytes))
        })?;
    }
    let mut remaining_prepared_bytes = groups.iter().fold(0usize, |bytes, group| {
        bytes.saturating_add(group.parallel_prepare_bytes())
    });
    if remaining_prepared_bytes > maximum_working_bytes {
        return Err(AnalysisError::InvalidData(format!(
            "prepared metadata token groups need about {}, exceeding remaining analysis budget {}",
            super::super::format_byte_size(remaining_prepared_bytes),
            super::super::format_byte_size(maximum_working_bytes)
        )));
    }
    let mut stats = MetadataContentUnionStats::default();
    for group in groups.drain(..) {
        remaining_prepared_bytes =
            remaining_prepared_bytes.saturating_sub(group.parallel_prepare_bytes());
        let group_working_bytes = maximum_working_bytes.saturating_sub(remaining_prepared_bytes);
        stats.accumulate(group.union_with_budget(context, state, group_working_bytes)?);
    }
    Ok(stats)
}

impl MetadataContentCandidateIndex {
    fn from_document_iter<'a, I>(documents: I) -> Self
    where
        I: Clone + Iterator<Item = (usize, &'a CompactMetadataContentDocument)>,
    {
        let token_count = documents
            .clone()
            .flat_map(|(_, doc)| doc.terms.iter().map(|&(token_id, _)| token_id as usize + 1))
            .max()
            .unwrap_or(0);
        let posting_count = documents
            .clone()
            .map(|(_, doc)| doc.terms.len())
            .sum::<usize>();
        let mut posting_offsets = vec![0u64; token_count.saturating_add(1)];
        for (_, document) in documents.clone() {
            for &(token_id, _) in &document.terms {
                posting_offsets[token_id as usize + 1] =
                    posting_offsets[token_id as usize + 1].saturating_add(1);
            }
        }
        for token_index in 0..token_count {
            posting_offsets[token_index + 1] =
                posting_offsets[token_index + 1].saturating_add(posting_offsets[token_index]);
        }

        let mut cursors = posting_offsets[..token_count].to_vec();
        let mut posting_atoms = vec![0; posting_count];
        for (atom_index, document) in documents {
            let compact_atom_index = metadata_doc_index_from_usize(atom_index);
            for &(token_id, _) in &document.terms {
                let cursor = &mut cursors[token_id as usize];
                posting_atoms[*cursor as usize] = compact_atom_index;
                *cursor = cursor.saturating_add(1);
            }
        }
        Self {
            posting_offsets,
            posting_atoms,
        }
    }

    #[cfg(test)]
    pub(super) fn new(docs: &[CompactMetadataContentDocument]) -> Self {
        Self::from_document_iter(docs.iter().enumerate())
    }

    pub(super) fn from_atoms(
        docs: &[CompactMetadataContentDocument],
        atoms: &[MetadataContentAtom],
    ) -> Self {
        Self::from_document_iter(atoms.iter().enumerate().map(|(atom_index, atom)| {
            (
                atom_index,
                &docs[metadata_doc_index_to_usize(atom.representative_record_index)],
            )
        }))
    }

    pub(super) fn from_atoms_parallel(
        docs: &[CompactMetadataContentDocument],
        atoms: &[MetadataContentAtom],
    ) -> Self {
        // CSR construction is linear and writes each posting exactly once;
        // comparison sorting costs more than this memory-bandwidth pass. The
        // caller already builds the independent template index concurrently.
        Self::from_atoms(docs, atoms)
    }

    pub(super) fn append_candidates_after(
        &self,
        record_index: usize,
        document: &CompactMetadataContentDocument,
        scratch: &mut MetadataCandidateScratch,
    ) {
        let compact_record_index = metadata_doc_index_from_usize(record_index);
        for &(token_id, _) in &document.terms {
            for &right in self.posting_after(token_id, compact_record_index) {
                scratch.push_once(right);
            }
        }
    }

    fn scan_cost_after(
        &self,
        record_index: usize,
        document: &CompactMetadataContentDocument,
    ) -> usize {
        let compact_record_index = metadata_doc_index_from_usize(record_index);
        document.terms.iter().fold(0usize, |cost, &(token_id, _)| {
            cost.saturating_add(self.posting_after(token_id, compact_record_index).len())
        })
    }

    fn posting_after(&self, token_id: u32, record_index: MetadataDocIndex) -> &[MetadataDocIndex] {
        let token_index = token_id as usize;
        if token_index + 1 >= self.posting_offsets.len() {
            return &self.posting_atoms[..0];
        }
        let posting_start = self.posting_offsets[token_index] as usize;
        let posting_end = self.posting_offsets[token_index + 1] as usize;
        let posting = &self.posting_atoms[posting_start..posting_end];
        let start = posting.partition_point(|&right| right <= record_index);
        &posting[start..]
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.posting_atoms.len()
    }

    #[cfg(test)]
    pub(super) fn offset_count(&self) -> usize {
        self.posting_offsets.len()
    }

    #[cfg(test)]
    pub(super) fn memory_bytes(&self) -> usize {
        self.posting_atoms
            .capacity()
            .saturating_mul(std::mem::size_of::<MetadataDocIndex>())
            .saturating_add(
                self.posting_offsets
                    .capacity()
                    .saturating_mul(std::mem::size_of::<u64>()),
            )
    }
}

fn flat_candidate_posting_after(
    entries: &[(u32, MetadataDocIndex)],
    token_id: u32,
    record_index: MetadataDocIndex,
) -> &[(u32, MetadataDocIndex)] {
    let posting_start = entries.partition_point(|&(token, _)| token < token_id);
    let posting_end = entries.partition_point(|&(token, _)| token <= token_id);
    let posting = &entries[posting_start..posting_end];
    let start = posting.partition_point(|&(_, right)| right <= record_index);
    &posting[start..]
}

fn append_flat_candidate_posting_after(
    entries: &[(u32, MetadataDocIndex)],
    token_id: u32,
    record_index: MetadataDocIndex,
    scratch: &mut MetadataCandidateScratch,
) {
    for &(_, right) in flat_candidate_posting_after(entries, token_id, record_index) {
        scratch.push_once(right);
    }
}

impl MetadataTemplateCandidateIndex {
    fn atom_entries(
        scoring: &CompactMetadataScoring,
        atoms: &[MetadataContentAtom],
        prefix: bool,
    ) -> Vec<(u32, MetadataDocIndex)> {
        let token_count = atoms
            .iter()
            .map(|atom| {
                let template = metadata_doc_index_to_usize(atom.template_doc_index);
                if prefix {
                    scoring.candidate_tokens(template).len()
                } else {
                    scoring.query_tokens(template).len()
                }
            })
            .sum();
        let mut entries = Vec::with_capacity(token_count);
        for (atom_index, atom) in atoms.iter().enumerate() {
            let atom_index = metadata_doc_index_from_usize(atom_index);
            let template = metadata_doc_index_to_usize(atom.template_doc_index);
            let tokens = if prefix {
                scoring.candidate_tokens(template)
            } else {
                scoring.query_tokens(template)
            };
            entries.extend(tokens.iter().map(|&token| (token, atom_index)));
        }
        entries
    }

    pub(super) fn from_atoms(
        scoring: &CompactMetadataScoring,
        atoms: &[MetadataContentAtom],
    ) -> Self {
        let mut full_entries = Self::atom_entries(scoring, atoms, false);
        let mut prefix_entries = Self::atom_entries(scoring, atoms, true);
        full_entries.sort_unstable();
        prefix_entries.sort_unstable();
        Self {
            full_entries,
            prefix_entries,
        }
    }

    pub(super) fn from_atoms_parallel(
        scoring: &CompactMetadataScoring,
        atoms: &[MetadataContentAtom],
    ) -> Self {
        let mut full_entries = Self::atom_entries(scoring, atoms, false);
        let mut prefix_entries = Self::atom_entries(scoring, atoms, true);
        rayon::join(
            || full_entries.par_sort_unstable(),
            || prefix_entries.par_sort_unstable(),
        );
        Self {
            full_entries,
            prefix_entries,
        }
    }

    pub(super) fn append_candidates_after(
        &self,
        atom_index: usize,
        atom: &MetadataContentAtom,
        scoring: &CompactMetadataScoring,
        scratch: &mut MetadataCandidateScratch,
    ) {
        let compact_atom_index = metadata_doc_index_from_usize(atom_index);
        let template = metadata_doc_index_to_usize(atom.template_doc_index);
        for &token in scoring.candidate_tokens(template) {
            append_flat_candidate_posting_after(
                &self.full_entries,
                token,
                compact_atom_index,
                scratch,
            );
        }
        for &token in scoring.query_tokens(template) {
            append_flat_candidate_posting_after(
                &self.prefix_entries,
                token,
                compact_atom_index,
                scratch,
            );
        }
    }

    fn scan_cost_after(
        &self,
        atom_index: usize,
        atom: &MetadataContentAtom,
        scoring: &CompactMetadataScoring,
    ) -> usize {
        let compact_atom_index = metadata_doc_index_from_usize(atom_index);
        let template = metadata_doc_index_to_usize(atom.template_doc_index);
        let prefix_to_full =
            scoring
                .candidate_tokens(template)
                .iter()
                .fold(0usize, |cost, &token| {
                    cost.saturating_add(
                        flat_candidate_posting_after(&self.full_entries, token, compact_atom_index)
                            .len(),
                    )
                });
        scoring
            .query_tokens(template)
            .iter()
            .fold(prefix_to_full, |cost, &token| {
                cost.saturating_add(
                    flat_candidate_posting_after(&self.prefix_entries, token, compact_atom_index)
                        .len(),
                )
            })
    }
}

impl MetadataLocalCandidateIndex {
    pub(super) fn from_atoms(
        docs: &[CompactMetadataContentDocument],
        atoms: &[MetadataContentAtom],
        compatibility: MetadataTemplateCompatibility<'_>,
        parallel: bool,
    ) -> Self {
        match compatibility {
            MetadataTemplateCompatibility::Scored(scoring) => {
                let (template, content) = if parallel {
                    rayon::join(
                        || MetadataTemplateCandidateIndex::from_atoms_parallel(scoring, atoms),
                        || MetadataContentCandidateIndex::from_atoms_parallel(docs, atoms),
                    )
                } else {
                    (
                        MetadataTemplateCandidateIndex::from_atoms(scoring, atoms),
                        MetadataContentCandidateIndex::from_atoms(docs, atoms),
                    )
                };
                Self::Adaptive { template, content }
            }
            #[cfg(test)]
            MetadataTemplateCompatibility::Precomputed(_) => {
                let index = if parallel {
                    MetadataContentCandidateIndex::from_atoms_parallel(docs, atoms)
                } else {
                    MetadataContentCandidateIndex::from_atoms(docs, atoms)
                };
                Self::ContentOnly(index)
            }
        }
    }

    pub(super) fn append_candidates_after(
        &self,
        atom_index: usize,
        atom: &MetadataContentAtom,
        document: &CompactMetadataContentDocument,
        compatibility: MetadataTemplateCompatibility<'_>,
        scratch: &mut MetadataCandidateScratch,
    ) -> MetadataLocalCandidateBasis {
        match self {
            Self::Adaptive { template, content } => {
                let scoring = compatibility
                    .scoring()
                    .expect("template candidate index requires scored compatibility");
                let template_cost = template.scan_cost_after(atom_index, atom, scoring);
                let content_cost = content.scan_cost_after(atom_index, document);
                if content_cost < template_cost {
                    content.append_candidates_after(atom_index, document, scratch);
                    MetadataLocalCandidateBasis::Content
                } else {
                    template.append_candidates_after(atom_index, atom, scoring, scratch);
                    MetadataLocalCandidateBasis::Template
                }
            }
            #[cfg(test)]
            Self::ContentOnly(index) => {
                index.append_candidates_after(atom_index, document, scratch);
                MetadataLocalCandidateBasis::Content
            }
        }
    }
}

impl MetadataCandidateScratch {
    pub(super) fn new(doc_count: usize) -> Self {
        Self {
            seen_generation: vec![0; doc_count],
            generation: 0,
            candidates: Vec::new(),
        }
    }

    pub(super) fn clear_for_next_left(&mut self) {
        self.candidates.clear();
        self.generation = self.generation.wrapping_add(1);
        if self.generation == 0 {
            self.seen_generation.fill(0);
            self.generation = 1;
        }
    }

    pub(super) fn push_once(&mut self, index: MetadataDocIndex) {
        let index_usize = metadata_doc_index_to_usize(index);
        if self.seen_generation[index_usize] == self.generation {
            return;
        }
        self.seen_generation[index_usize] = self.generation;
        self.candidates.push(index);
    }
}

#[cfg(test)]
impl MetadataCandidateScratchPool {
    pub(super) fn new(doc_count: usize) -> Self {
        Self {
            doc_count,
            scratches: Mutex::new(Vec::new()),
        }
    }

    pub(super) fn take(&self) -> MetadataCandidateScratchLease<'_> {
        let scratch = self
            .scratches
            .lock()
            .expect("metadata candidate scratch pool lock poisoned")
            .pop()
            .unwrap_or_else(|| MetadataCandidateScratch::new(self.doc_count));
        MetadataCandidateScratchLease {
            pool: self,
            scratch: Some(scratch),
        }
    }
}

#[cfg(test)]
impl std::ops::Deref for MetadataCandidateScratchLease<'_> {
    type Target = MetadataCandidateScratch;

    fn deref(&self) -> &Self::Target {
        self.scratch
            .as_ref()
            .expect("metadata candidate scratch lease is empty")
    }
}

#[cfg(test)]
impl std::ops::DerefMut for MetadataCandidateScratchLease<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.scratch
            .as_mut()
            .expect("metadata candidate scratch lease is empty")
    }
}

#[cfg(test)]
impl Drop for MetadataCandidateScratchLease<'_> {
    fn drop(&mut self) {
        let Some(scratch) = self.scratch.take() else {
            return;
        };
        self.pool
            .scratches
            .lock()
            .expect("metadata candidate scratch pool lock poisoned")
            .push(scratch);
    }
}

pub(super) fn lowest_common_metadata_token(left: &[u32], right: &[u32]) -> Option<u32> {
    let mut left_index = 0usize;
    let mut right_index = 0usize;
    while left_index < left.len() && right_index < right.len() {
        match left[left_index].cmp(&right[right_index]) {
            std::cmp::Ordering::Equal => return Some(left[left_index]),
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
        }
    }
    None
}

pub(super) fn union_metadata_token_content_matches(
    conn: &Connection,
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
    maximum_working_bytes: usize,
) -> Result<MetadataContentUnionStats, AnalysisError> {
    let mut stmt = conn.prepare(metadata_token_content_rows_sql())?;
    let mut current_token = None;
    let mut group = MetadataRawTokenGroup::default();
    let mut pending_groups = Vec::new();
    let mut pending_prepare_bytes = 0usize;
    let small_group_reserve_bytes = super::load::metadata_uncached_parse_transient_bytes(
        METADATA_CONTENT_PARALLEL_MIN_RECORDS
            .saturating_mul(super::parse::MAX_METADATA_BYTES_FOR_DEDUP),
        0,
    )
    .saturating_mul(2)
    .min(maximum_working_bytes);
    let pending_batch_budget = maximum_working_bytes.saturating_sub(small_group_reserve_bytes);
    let maximum_pending_groups = context
        .pool
        .current_num_threads()
        .max(1)
        .saturating_mul(METADATA_TOKEN_GROUP_BATCH_MULTIPLIER);
    let mut stats = MetadataContentUnionStats::default();
    for batch in stmt.query_arrow([])? {
        let token_column = arrow_i64_column(&batch, 0, "token_index")?;
        let contract_column = arrow_i64_column(&batch, 1, "contract_index")?;
        let representative_column = arrow_i64_column(&batch, 2, "uses_loaded_representative")?;
        let metadata_column = arrow_string_column(&batch, 3, "metadata_json")?;
        for row_index in 0..batch.num_rows() {
            let token_index = u32::try_from(token_column.value(row_index)).map_err(|_| {
                AnalysisError::InvalidData(
                    "metadata token dictionary exceeds compact u32 indexes".to_string(),
                )
            })?;
            if current_token.is_some_and(|current| current != token_index) {
                let completed = std::mem::take(&mut group);
                let prepare_bytes = completed.parallel_prepare_bytes();
                let can_prepare_in_small_batch = completed.record_count()
                    < METADATA_CONTENT_PARALLEL_MIN_RECORDS
                    && prepare_bytes <= small_group_reserve_bytes
                    && prepare_bytes <= pending_batch_budget;
                if !can_prepare_in_small_batch {
                    stats.accumulate(prepare_metadata_token_group_batch(
                        &mut pending_groups,
                        context,
                        state,
                        maximum_working_bytes,
                    )?);
                    pending_prepare_bytes = 0;
                    stats.accumulate(completed.union_with_budget(
                        context,
                        state,
                        maximum_working_bytes,
                    )?);
                } else {
                    if pending_groups.len() >= maximum_pending_groups
                        || pending_prepare_bytes.saturating_add(prepare_bytes)
                            > pending_batch_budget
                    {
                        stats.accumulate(prepare_metadata_token_group_batch(
                            &mut pending_groups,
                            context,
                            state,
                            maximum_working_bytes,
                        )?);
                        pending_prepare_bytes = 0;
                    }
                    pending_prepare_bytes = pending_prepare_bytes.saturating_add(prepare_bytes);
                    pending_groups.push(completed);
                }
            }
            current_token = Some(token_index);
            let source_contract_index =
                usize::try_from(contract_column.value(row_index)).map_err(|_| {
                    AnalysisError::InvalidData(
                        "negative metadata source contract index".to_string(),
                    )
                })?;
            let Some(contract_index) = context
                .data
                .compact_contract_index_for_source(source_contract_index)
            else {
                continue;
            };
            if !pending_groups.is_empty()
                && group.record_count() >= METADATA_CONTENT_PARALLEL_MIN_RECORDS
            {
                stats.accumulate(prepare_metadata_token_group_batch(
                    &mut pending_groups,
                    context,
                    state,
                    maximum_working_bytes,
                )?);
                pending_prepare_bytes = 0;
            }
            let current_group_budget = maximum_working_bytes.saturating_sub(pending_prepare_bytes);
            let uses_loaded_representative = representative_column.value(row_index) != 0
                && context.data.contracts[metadata_contract_index_to_usize(contract_index)]
                    .uses_declared_metadata_source;
            if uses_loaded_representative {
                group.push_loaded_representative_with_budget(
                    contract_index,
                    context,
                    current_group_budget,
                )?;
            } else {
                if duckdb::arrow::array::Array::is_null(metadata_column, row_index) {
                    return Err(AnalysisError::InvalidData(
                        "non-representative metadata token row is missing JSON".to_string(),
                    ));
                }
                group.push_raw_with_budget(
                    contract_index,
                    metadata_column.value(row_index).to_owned(),
                    context,
                    current_group_budget,
                )?;
            }
        }
    }
    if current_token.is_some() {
        let prepare_bytes = group.parallel_prepare_bytes();
        if group.record_count() < METADATA_CONTENT_PARALLEL_MIN_RECORDS
            && prepare_bytes <= small_group_reserve_bytes
            && pending_prepare_bytes.saturating_add(prepare_bytes) <= pending_batch_budget
        {
            pending_groups.push(group);
        } else {
            stats.accumulate(prepare_metadata_token_group_batch(
                &mut pending_groups,
                context,
                state,
                maximum_working_bytes,
            )?);
            stats.accumulate(group.union_with_budget(context, state, maximum_working_bytes)?);
        }
    }
    stats.accumulate(prepare_metadata_token_group_batch(
        &mut pending_groups,
        context,
        state,
        maximum_working_bytes,
    )?);
    Ok(stats)
}

pub(super) fn metadata_token_content_rows_sql() -> &'static str {
    "
        SELECT t.token_index,
               t.contract_index,
               (t.metadata_source_file = c.metadata_source_file
                   AND t.metadata_source_row_number = c.metadata_source_row_number)::BIGINT
                   AS uses_loaded_representative,
               a.metadata_json
        FROM metadata_contract_token_rows t
        JOIN analysis_contracts c
          ON c.metadata_contract_index = t.contract_index
        JOIN metadata_rows a
          ON a.source_file = t.metadata_source_file
         AND a.source_row_number = t.metadata_source_row_number
        ORDER BY t.token_index, t.contract_index
    "
}

pub(super) fn metadata_content_document<'a>(
    data: &'a MetadataData,
    raw: &str,
) -> Option<Cow<'a, MetadataBm25Document>> {
    data.reused_documents
        .get(raw)
        .and_then(|cached| cached.content.as_deref())
        .map(Cow::Borrowed)
        .or_else(|| {
            MetadataBm25Document::from_normalized_text(&metadata_document_from_json(raw))
                .map(Cow::Owned)
        })
}

pub(super) fn union_metadata_representative_content_fallback(
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
    maximum_working_bytes: usize,
) -> Result<MetadataContentUnionStats, AnalysisError> {
    let mut builder = CompactMetadataContentGroupBuilder::default();
    for (contract_index, contract) in context.data.contracts.iter().enumerate() {
        if let Some(document) = &contract.content_doc {
            builder.push_document(
                metadata_contract_index_from_usize(contract_index),
                document.as_ref(),
                context.data,
                Some(context.contract_tokens),
            );
            builder.ensure_within_memory_budget(
                0,
                maximum_working_bytes,
                context.pool.current_num_threads(),
            )?;
        }
    }
    builder.ensure_within_memory_budget(
        0,
        maximum_working_bytes,
        context.pool.current_num_threads(),
    )?;
    let (atoms, docs) = builder.into_atomized_parts();
    Ok(union_metadata_no_common_atom_core(
        atoms, &docs, context, state,
    ))
}

pub(super) fn apply_metadata_contract_pair_union(
    data: &MetadataData,
    chain_count: usize,
    state: &mut MetadataUnionState,
    left: usize,
    right: usize,
) {
    let left_chain = data.contracts[left].chain_index;
    let right_chain = data.contracts[right].chain_index;
    if left_chain == right_chain {
        state.intra.union(left, right);
        return;
    }
    if let Some(cross) = &mut state.cross {
        cross.union(left, right);
    }
    if let Some(matrix) = &mut state.chain_matrix {
        let (primary_chain, secondary_chain) = if left_chain < right_chain {
            (left_chain, right_chain)
        } else {
            (right_chain, left_chain)
        };
        let pair_index = chain_pair_index(primary_chain, secondary_chain, chain_count);
        matrix[pair_index].union(left, right);
    }
}

pub(super) fn apply_metadata_same_chain_group_union(
    data: &MetadataData,
    chain_count: usize,
    state: &mut MetadataUnionState,
    members: &[MetadataContractIndex],
) {
    let Some((&anchor, rest)) = members.split_first() else {
        return;
    };
    let anchor = metadata_contract_index_to_usize(anchor);
    let anchor_chain = data.contracts[anchor].chain_index;
    for &member in rest {
        let member = metadata_contract_index_to_usize(member);
        debug_assert_eq!(data.contracts[member].chain_index, anchor_chain);
        apply_metadata_contract_pair_union(data, chain_count, state, anchor, member);
    }
}

pub(super) fn apply_metadata_complete_bipartite_group_union(
    data: &MetadataData,
    chain_count: usize,
    state: &mut MetadataUnionState,
    left_members: &[MetadataContractIndex],
    right_members: &[MetadataContractIndex],
) {
    let Some((&left_anchor, left_rest)) = left_members.split_first() else {
        return;
    };
    let Some((&right_anchor, right_rest)) = right_members.split_first() else {
        return;
    };
    apply_metadata_contract_pair_union(
        data,
        chain_count,
        state,
        metadata_contract_index_to_usize(left_anchor),
        metadata_contract_index_to_usize(right_anchor),
    );
    for &left in left_rest {
        apply_metadata_contract_pair_union(
            data,
            chain_count,
            state,
            metadata_contract_index_to_usize(left),
            metadata_contract_index_to_usize(right_anchor),
        );
    }
    for &right in right_rest {
        apply_metadata_contract_pair_union(
            data,
            chain_count,
            state,
            metadata_contract_index_to_usize(left_anchor),
            metadata_contract_index_to_usize(right),
        );
    }
}

pub(super) fn apply_metadata_atom_pair_union(
    data: &MetadataData,
    chain_count: usize,
    state: &mut MetadataUnionState,
    left: &MetadataContentAtom,
    right: &MetadataContentAtom,
) {
    debug_assert!(!left.members.is_empty());
    debug_assert!(!right.members.is_empty());
    if left.chain_index == right.chain_index {
        apply_metadata_contract_pair_union(
            data,
            chain_count,
            state,
            metadata_contract_index_to_usize(left.members[0]),
            metadata_contract_index_to_usize(right.members[0]),
        );
    } else {
        apply_metadata_complete_bipartite_group_union(
            data,
            chain_count,
            state,
            &left.members,
            &right.members,
        );
    }
}

pub(super) fn metadata_fallback_token_group_tokens<'a>(
    group: &MetadataFallbackTokenGroup,
    contract_tokens: &'a CompactContractTokens,
) -> &'a [u32] {
    let representative = metadata_contract_index_to_usize(group.members[0]);
    &contract_tokens[representative]
}

pub(super) fn metadata_fallback_token_groups_are_disjoint(
    left: &MetadataFallbackTokenGroup,
    right: &MetadataFallbackTokenGroup,
    contract_tokens: &CompactContractTokens,
) -> bool {
    lowest_common_metadata_token(
        metadata_fallback_token_group_tokens(left, contract_tokens),
        metadata_fallback_token_group_tokens(right, contract_tokens),
    )
    .is_none()
}

pub(super) fn apply_metadata_fallback_atom_internal_unions(
    atom: &MetadataContentAtom,
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
) {
    for group in &atom.fallback_token_groups {
        if metadata_fallback_token_group_tokens(group, context.contract_tokens).is_empty() {
            apply_metadata_same_chain_group_union(
                context.data,
                context.chain_count,
                state,
                &group.members,
            );
        }
    }

    let mut unvisited = (0..atom.fallback_token_groups.len()).collect::<Vec<_>>();
    while let Some(root) = unvisited.pop() {
        let mut queue = vec![root];
        while let Some(current) = queue.pop() {
            let mut index = 0;
            while index < unvisited.len() {
                let other = unvisited[index];
                if !metadata_fallback_token_groups_are_disjoint(
                    &atom.fallback_token_groups[current],
                    &atom.fallback_token_groups[other],
                    context.contract_tokens,
                ) {
                    index += 1;
                    continue;
                }
                let other = unvisited.swap_remove(index);
                apply_metadata_complete_bipartite_group_union(
                    context.data,
                    context.chain_count,
                    state,
                    &atom.fallback_token_groups[current].members,
                    &atom.fallback_token_groups[other].members,
                );
                queue.push(other);
            }
        }
    }
}

pub(super) fn metadata_fallback_atoms_have_disjoint_token_groups(
    left: &MetadataContentAtom,
    right: &MetadataContentAtom,
    contract_tokens: &CompactContractTokens,
) -> bool {
    left.fallback_token_groups.iter().any(|left_group| {
        right.fallback_token_groups.iter().any(|right_group| {
            metadata_fallback_token_groups_are_disjoint(left_group, right_group, contract_tokens)
        })
    })
}

pub(super) fn apply_metadata_fallback_atom_pair_union(
    left: &MetadataContentAtom,
    right: &MetadataContentAtom,
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
) {
    let mut unvisited_left = (0..left.fallback_token_groups.len()).collect::<Vec<_>>();
    let mut unvisited_right = (0..right.fallback_token_groups.len()).collect::<Vec<_>>();
    while let Some(root) = unvisited_left.pop() {
        let mut queue = vec![(true, root)];
        while let Some((is_left, current)) = queue.pop() {
            let (current_group, opposite_groups, unvisited_opposite) = if is_left {
                (
                    &left.fallback_token_groups[current],
                    &right.fallback_token_groups,
                    &mut unvisited_right,
                )
            } else {
                (
                    &right.fallback_token_groups[current],
                    &left.fallback_token_groups,
                    &mut unvisited_left,
                )
            };
            let mut index = 0;
            while index < unvisited_opposite.len() {
                let other = unvisited_opposite[index];
                let other_group = &opposite_groups[other];
                if !metadata_fallback_token_groups_are_disjoint(
                    current_group,
                    other_group,
                    context.contract_tokens,
                ) {
                    index += 1;
                    continue;
                }
                let other = unvisited_opposite.swap_remove(index);
                let (left_group, right_group) = if is_left {
                    (current_group, &right.fallback_token_groups[other])
                } else {
                    (&left.fallback_token_groups[other], current_group)
                };
                apply_metadata_complete_bipartite_group_union(
                    context.data,
                    context.chain_count,
                    state,
                    &left_group.members,
                    &right_group.members,
                );
                queue.push((!is_left, other));
            }
        }
    }
}

#[cfg(test)]
#[derive(Debug, PartialEq, Eq)]
pub(super) struct MetadataDocPairBatch {
    pub(super) hits: Vec<MetadataDocPair>,
    pub(super) candidate_pairs: u64,
}

#[cfg(test)]
#[derive(Debug, PartialEq, Eq)]
pub(super) struct MetadataHitLimitExceeded {
    pub(super) retained_hits: usize,
}

#[cfg(test)]
pub(super) fn metadata_scoring_progress_units(scoring_left_count: usize) -> u64 {
    scoring_left_count as u64
}

#[cfg(test)]
pub(super) fn metadata_pair_left_chunk_size(doc_count: usize, max_match_pairs: u64) -> usize {
    let doc_count = u64::try_from(doc_count.max(1)).unwrap_or(u64::MAX);
    let budgeted_chunk = max_match_pairs / doc_count;
    budgeted_chunk.clamp(1, METADATA_PAIR_LEFT_CHUNK_SIZE as u64) as usize
}

#[cfg(test)]
pub(super) fn metadata_template_match_pair_budget(available_bytes: usize, doc_count: usize) -> u64 {
    let fixed_offsets_and_cursors = doc_count.saturating_mul(2 * std::mem::size_of::<u64>());
    let pairs = available_bytes
        .saturating_sub(fixed_offsets_and_cursors)
        // A compact pair is 8 bytes. During scoring, the retained pair Vec,
        // Rayon-local/reduced batch Vecs and append reallocation can overlap;
        // conversion then overlaps the retained Vec with 8-byte symmetric
        // postings. Reserve a conservative 40 bytes per logical pair.
        .saturating_div(40);
    u64::try_from(pairs).unwrap_or(u64::MAX)
}

#[cfg(test)]
pub(super) fn metadata_scoring_batch_progress_units(left_start: usize, left_end: usize) -> u64 {
    left_end.saturating_sub(left_start) as u64
}

#[cfg(test)]
pub(super) fn metadata_pair_progress_message(
    scored_pairs: u64,
    scored_left_docs: usize,
    total_left_docs: usize,
    matched_pairs: u64,
    elapsed: Duration,
) -> String {
    let remaining_left_docs = total_left_docs.saturating_sub(scored_left_docs);
    let estimated_remaining_pairs = estimate_remaining_metadata_candidate_pairs(
        scored_pairs,
        scored_left_docs,
        remaining_left_docs,
    );
    let throughput = format_metadata_pair_throughput(scored_pairs, elapsed);
    let eta = format_metadata_pair_eta(estimated_remaining_pairs, scored_pairs, elapsed);
    format!(
        "metadata candidate pairs scored {scored_pairs}; left docs {scored_left_docs}/{total_left_docs}; estimated remaining {estimated_remaining_pairs}; throughput {throughput}; ETA {eta}; matched doc pairs {matched_pairs}"
    )
}

#[cfg(test)]
pub(super) fn estimate_remaining_metadata_candidate_pairs(
    scored_pairs: u64,
    scored_left_docs: usize,
    remaining_left_docs: usize,
) -> u64 {
    if scored_pairs == 0 || scored_left_docs == 0 || remaining_left_docs == 0 {
        return 0;
    }
    let estimated = (scored_pairs as u128)
        .saturating_mul(remaining_left_docs as u128)
        .div_ceil(scored_left_docs as u128);
    estimated.min(u64::MAX as u128) as u64
}

#[cfg(test)]
pub(super) fn format_metadata_pair_throughput(scored_pairs: u64, elapsed: Duration) -> String {
    let Some(pairs_per_second) = metadata_pairs_per_second(scored_pairs, elapsed) else {
        return "n/a".to_string();
    };
    format!("{pairs_per_second:.1} pairs/s")
}

#[cfg(test)]
pub(super) fn format_metadata_pair_eta(
    remaining_pairs: u64,
    scored_pairs: u64,
    elapsed: Duration,
) -> String {
    if scored_pairs == 0 {
        return "n/a".to_string();
    }
    if remaining_pairs == 0 {
        return "0s".to_string();
    }
    let Some(pairs_per_second) = metadata_pairs_per_second(scored_pairs, elapsed) else {
        return "n/a".to_string();
    };
    format_metadata_duration(Duration::from_secs_f64(
        (remaining_pairs as f64 / pairs_per_second).ceil(),
    ))
}

#[cfg(test)]
pub(super) fn metadata_pairs_per_second(scored_pairs: u64, elapsed: Duration) -> Option<f64> {
    if scored_pairs == 0 {
        return None;
    }
    let elapsed_seconds = elapsed.as_secs_f64();
    if elapsed_seconds <= 0.0 {
        return None;
    }
    Some(scored_pairs as f64 / elapsed_seconds)
}

#[cfg(test)]
pub(super) fn format_metadata_duration(duration: Duration) -> String {
    let seconds = duration.as_secs();
    if seconds < 60 {
        return format!("{seconds}s");
    }
    let minutes = seconds / 60;
    let remaining_seconds = seconds % 60;
    if minutes < 60 {
        return format!("{minutes}m {remaining_seconds:02}s");
    }
    let hours = minutes / 60;
    let remaining_minutes = minutes % 60;
    format!("{hours}h {remaining_minutes:02}m")
}

#[cfg(test)]
pub(super) fn collect_metadata_doc_pair_hits_for_left_range(
    left_range: std::ops::Range<usize>,
    context: MetadataPairScoringContext<'_>,
    scratch_pool: &MetadataCandidateScratchPool,
) -> MetadataDocPairBatch {
    collect_metadata_doc_pair_hits_for_left_range_bounded(
        left_range,
        context,
        scratch_pool,
        usize::MAX,
    )
    .expect("an unbounded metadata hit collector cannot exhaust pair permits")
}

#[cfg(test)]
pub(super) fn collect_metadata_doc_pair_hits_for_left_range_bounded(
    left_range: std::ops::Range<usize>,
    context: MetadataPairScoringContext<'_>,
    scratch_pool: &MetadataCandidateScratchPool,
    maximum_hits: usize,
) -> Result<MetadataDocPairBatch, MetadataHitLimitExceeded> {
    let context = &context;
    let permits = MetadataHitPermits::new(maximum_hits);
    let (mut hits, candidate_pairs) = left_range
        .into_par_iter()
        .map_init(
            || scratch_pool.take(),
            |scratch, left| {
                let mut local_hits = Vec::new();
                let local_candidate_pairs =
                    collect_metadata_doc_pair_hits_for_left_with_scratch_bounded(
                        left,
                        context,
                        scratch,
                        &mut local_hits,
                        Some(&permits),
                    );
                (local_hits, local_candidate_pairs)
            },
        )
        .reduce(
            || (Vec::new(), 0u64),
            |(mut left_hits, left_candidate_pairs), (mut right_hits, right_candidate_pairs)| {
                left_hits.append(&mut right_hits);
                (
                    left_hits,
                    left_candidate_pairs.saturating_add(right_candidate_pairs),
                )
            },
        );
    if permits.exceeded() {
        return Err(MetadataHitLimitExceeded {
            retained_hits: hits.len(),
        });
    }
    hits.sort_unstable();
    hits.dedup();
    Ok(MetadataDocPairBatch {
        hits,
        candidate_pairs,
    })
}

#[cfg(test)]
pub(super) fn collect_metadata_doc_pair_hits_for_left_with_scratch(
    left: usize,
    context: &MetadataPairScoringContext<'_>,
    scratch: &mut MetadataCandidateScratch,
    hits: &mut Vec<MetadataDocPair>,
) -> u64 {
    collect_metadata_doc_pair_hits_for_left_with_scratch_bounded(left, context, scratch, hits, None)
}

#[cfg(test)]
fn collect_metadata_doc_pair_hits_for_left_with_scratch_bounded(
    left: usize,
    context: &MetadataPairScoringContext<'_>,
    scratch: &mut MetadataCandidateScratch,
    hits: &mut Vec<MetadataDocPair>,
    permits: Option<&MetadataHitPermits>,
) -> u64 {
    let candidates = metadata_candidate_indices_for_left_with_scratch(left, context, scratch);
    let mut scored_candidates = 0u64;
    for &right in candidates {
        if permits.is_some_and(MetadataHitPermits::exceeded) {
            break;
        }
        let right = metadata_doc_index_to_usize(right);
        scored_candidates = scored_candidates.saturating_add(1);
        if context.scoring.score(left, right) >= METADATA_THRESHOLD {
            if permits.is_some_and(|permits| !permits.try_acquire()) {
                break;
            }
            hits.push(ordered_metadata_doc_pair(left, right));
        }
    }
    scored_candidates
}

#[cfg(test)]
pub(super) fn metadata_candidate_indices_for_left_with_scratch<'a>(
    left: usize,
    context: &MetadataPairScoringContext<'_>,
    scratch: &'a mut MetadataCandidateScratch,
) -> &'a [MetadataDocIndex] {
    scratch.clear_for_next_left();
    let compact_left = metadata_doc_index_from_usize(left);
    for &token in context.scoring.candidate_tokens(left) {
        append_metadata_posting_except(
            context.postings.posting(token as usize),
            compact_left,
            scratch,
        );
    }
    scratch.candidates.sort_unstable();
    &scratch.candidates
}

#[cfg(test)]
pub(super) fn append_metadata_posting_except(
    posting: &[MetadataDocIndex],
    excluded: MetadataDocIndex,
    scratch: &mut MetadataCandidateScratch,
) {
    for &index in posting {
        if index != excluded {
            scratch.push_once(index);
        }
    }
}

#[cfg(test)]
pub(super) fn ordered_metadata_doc_pair(left: usize, right: usize) -> MetadataDocPair {
    let left = metadata_doc_index_from_usize(left);
    let right = metadata_doc_index_from_usize(right);
    if left <= right {
        (left, right)
    } else {
        (right, left)
    }
}

pub(super) fn metadata_content_pair_matches(
    left: &CompactMetadataContentDocument,
    right: &CompactMetadataContentDocument,
    threshold: f64,
) -> bool {
    compact_metadata_content_pair_score(left, right) >= threshold
}

#[cfg(test)]
pub(super) fn build_metadata_content_atoms(
    records: &[MetadataContentRecord],
    compact_docs: &[CompactMetadataContentDocument],
    data: &MetadataData,
) -> Vec<MetadataContentAtom> {
    build_metadata_content_atoms_core(records.len(), compact_docs, data, |record_index| {
        records[record_index].contract_index
    })
}

#[cfg(test)]
fn build_metadata_content_atoms_core(
    record_count: usize,
    compact_docs: &[CompactMetadataContentDocument],
    data: &MetadataData,
    mut contract_index_at: impl FnMut(usize) -> MetadataContractIndex,
) -> Vec<MetadataContentAtom> {
    debug_assert_eq!(record_count, compact_docs.len());
    let mut atom_index_by_key = HashMap::<(usize, MetadataDocIndex, &[(u32, u32)]), usize>::new();
    let mut atoms = Vec::<MetadataContentAtom>::new();
    for (record_index, document) in compact_docs.iter().enumerate() {
        let compact_contract_index = contract_index_at(record_index);
        let contract_index = metadata_contract_index_to_usize(compact_contract_index);
        let contract = &data.contracts[contract_index];
        let key = (
            contract.chain_index,
            contract.template_doc_index,
            document.terms.as_slice(),
        );
        if let Some(&atom_index) = atom_index_by_key.get(&key) {
            atoms[atom_index].members.push(compact_contract_index);
            continue;
        }
        let atom_index = atoms.len();
        atom_index_by_key.insert(key, atom_index);
        atoms.push(MetadataContentAtom {
            chain_index: contract.chain_index,
            template_doc_index: contract.template_doc_index,
            representative_record_index: metadata_doc_index_from_usize(record_index),
            members: vec![compact_contract_index],
            fallback_token_groups: Vec::new(),
        });
    }
    atoms
}

#[cfg(test)]
pub(super) fn build_metadata_fallback_atoms(
    records: &[MetadataContentRecord],
    compact_docs: &[CompactMetadataContentDocument],
    data: &MetadataData,
    contract_tokens: &CompactContractTokens,
) -> Vec<MetadataContentAtom> {
    let mut atom_index_by_key = HashMap::<(usize, MetadataDocIndex, &[(u32, u32)]), usize>::new();
    let mut token_group_index_by_atom = Vec::<HashMap<&[u32], usize>>::new();
    let mut atoms = Vec::<MetadataContentAtom>::new();
    for (record_index, record) in records.iter().enumerate() {
        let contract_index = metadata_contract_index_to_usize(record.contract_index);
        let contract = &data.contracts[contract_index];
        let key = (
            contract.chain_index,
            contract.template_doc_index,
            compact_docs[record_index].terms.as_slice(),
        );
        if let Some(&atom_index) = atom_index_by_key.get(&key) {
            let atom = &mut atoms[atom_index];
            atom.members.push(record.contract_index);
            let token_group_indexes = &mut token_group_index_by_atom[atom_index];
            let tokens = contract_tokens.tokens(contract_index);
            if let Some(&token_group_index) = token_group_indexes.get(tokens) {
                atom.fallback_token_groups[token_group_index]
                    .members
                    .push(record.contract_index);
            } else {
                let token_group_index = atom.fallback_token_groups.len();
                token_group_indexes.insert(tokens, token_group_index);
                atom.fallback_token_groups.push(MetadataFallbackTokenGroup {
                    members: vec![record.contract_index],
                });
            }
            continue;
        }
        let atom_index = atoms.len();
        atom_index_by_key.insert(key, atom_index);
        token_group_index_by_atom
            .push(HashMap::from([(contract_tokens.tokens(contract_index), 0)]));
        atoms.push(MetadataContentAtom {
            chain_index: contract.chain_index,
            template_doc_index: contract.template_doc_index,
            representative_record_index: metadata_doc_index_from_usize(record_index),
            members: vec![record.contract_index],
            fallback_token_groups: vec![MetadataFallbackTokenGroup {
                members: vec![record.contract_index],
            }],
        });
    }
    atoms
}

pub(super) fn metadata_content_atom_pair_matches(
    pair: (usize, MetadataDocIndex),
    atoms: &[MetadataContentAtom],
    compact_docs: &[CompactMetadataContentDocument],
) -> bool {
    let (left, right) = pair;
    let left_record = metadata_doc_index_to_usize(atoms[left].representative_record_index);
    let right_record = metadata_doc_index_to_usize(
        atoms[metadata_doc_index_to_usize(right)].representative_record_index,
    );
    metadata_content_pair_matches(
        &compact_docs[left_record],
        &compact_docs[right_record],
        METADATA_THRESHOLD,
    )
}

fn metadata_content_atoms_share_token(
    left: usize,
    right: MetadataDocIndex,
    atoms: &[MetadataContentAtom],
    compact_docs: &[CompactMetadataContentDocument],
) -> bool {
    let left_record = metadata_doc_index_to_usize(atoms[left].representative_record_index);
    let right_record = metadata_doc_index_to_usize(
        atoms[metadata_doc_index_to_usize(right)].representative_record_index,
    );
    compact_metadata_content_docs_share_token(
        &compact_docs[left_record],
        &compact_docs[right_record],
    )
}

fn metadata_prefix_intersects_sorted_terms(prefix: &[u32], terms: &[u32]) -> bool {
    prefix
        .iter()
        .any(|token| terms.binary_search(token).is_ok())
}

fn metadata_template_atoms_share_safe_prefix(
    left: usize,
    right: MetadataDocIndex,
    atoms: &[MetadataContentAtom],
    compatibility: MetadataTemplateCompatibility<'_>,
) -> bool {
    let Some(scoring) = compatibility.scoring() else {
        // Test-only precomputed compatibility performs the exact lookup in the
        // scoring batch and has no compact prefix arrays.
        return true;
    };
    let left_template = metadata_doc_index_to_usize(atoms[left].template_doc_index);
    let right_template =
        metadata_doc_index_to_usize(atoms[metadata_doc_index_to_usize(right)].template_doc_index);
    metadata_prefix_intersects_sorted_terms(
        scoring.candidate_tokens(left_template),
        scoring.query_tokens(right_template),
    ) || metadata_prefix_intersects_sorted_terms(
        scoring.candidate_tokens(right_template),
        scoring.query_tokens(left_template),
    )
}

fn metadata_candidate_intersects_both_dimensions(
    basis: MetadataLocalCandidateBasis,
    left: usize,
    right: MetadataDocIndex,
    atoms: &[MetadataContentAtom],
    compact_docs: &[CompactMetadataContentDocument],
    compatibility: MetadataTemplateCompatibility<'_>,
) -> bool {
    match basis {
        MetadataLocalCandidateBasis::Template => {
            metadata_content_atoms_share_token(left, right, atoms, compact_docs)
        }
        MetadataLocalCandidateBasis::Content => {
            metadata_template_atoms_share_safe_prefix(left, right, atoms, compatibility)
        }
    }
}

#[cfg(test)]
pub(super) fn collect_metadata_content_atom_pair_hits(
    candidate_pairs: &[(usize, MetadataDocIndex)],
    atoms: &[MetadataContentAtom],
    compact_docs: &[CompactMetadataContentDocument],
    pool: &rayon::ThreadPool,
) -> Vec<(usize, MetadataDocIndex)> {
    if candidate_pairs.len() >= METADATA_CONTENT_PARALLEL_MIN_RECORDS {
        pool.install(|| {
            candidate_pairs
                .par_iter()
                .copied()
                .filter(|&pair| metadata_content_atom_pair_matches(pair, atoms, compact_docs))
                .collect()
        })
    } else {
        candidate_pairs
            .iter()
            .copied()
            .filter(|&pair| metadata_content_atom_pair_matches(pair, atoms, compact_docs))
            .collect()
    }
}

#[derive(Default)]
pub(super) struct MetadataPairScoringStats {
    template_candidate_pairs: u64,
    template_scored_pairs: u64,
    template_matched_pairs: u64,
    content_scored_pairs: u64,
}

#[derive(Default)]
struct MetadataValidatedPairBatch {
    hits: Vec<(usize, MetadataDocIndex)>,
    stats: MetadataPairScoringStats,
    template_cache: MetadataTemplateScoreCache,
}

impl MetadataValidatedPairBatch {
    fn score_pair(
        &mut self,
        pair: (usize, MetadataDocIndex),
        atoms: &[MetadataContentAtom],
        compact_docs: &[CompactMetadataContentDocument],
        template_compatibility: MetadataTemplateCompatibility<'_>,
    ) {
        let (left, right) = pair;
        let left_template = atoms[left].template_doc_index;
        let right_template = atoms[metadata_doc_index_to_usize(right)].template_doc_index;
        let (template_matches, template_scores) =
            self.template_cache
                .evaluate(left_template, right_template, template_compatibility);
        self.stats.template_candidate_pairs = self.stats.template_candidate_pairs.saturating_add(1);
        self.stats.template_scored_pairs = self
            .stats
            .template_scored_pairs
            .saturating_add(template_scores);
        if !template_matches {
            return;
        }
        self.stats.template_matched_pairs = self.stats.template_matched_pairs.saturating_add(1);
        self.stats.content_scored_pairs = self.stats.content_scored_pairs.saturating_add(1);
        if metadata_content_atom_pair_matches(pair, atoms, compact_docs) {
            self.hits.push(pair);
        }
    }

    fn merge(mut self, mut other: Self) -> Self {
        self.hits.append(&mut other.hits);
        self.stats.template_candidate_pairs = self
            .stats
            .template_candidate_pairs
            .saturating_add(other.stats.template_candidate_pairs);
        self.stats.template_scored_pairs = self
            .stats
            .template_scored_pairs
            .saturating_add(other.stats.template_scored_pairs);
        self.stats.template_matched_pairs = self
            .stats
            .template_matched_pairs
            .saturating_add(other.stats.template_matched_pairs);
        self.stats.content_scored_pairs = self
            .stats
            .content_scored_pairs
            .saturating_add(other.stats.content_scored_pairs);
        self
    }
}

fn collect_metadata_validated_atom_pair_hits(
    candidate_pairs: &[(usize, MetadataDocIndex)],
    atoms: &[MetadataContentAtom],
    compact_docs: &[CompactMetadataContentDocument],
    template_compatibility: MetadataTemplateCompatibility<'_>,
    pool: &rayon::ThreadPool,
) -> MetadataValidatedPairBatch {
    if candidate_pairs.len() >= METADATA_CONTENT_PARALLEL_MIN_RECORDS {
        pool.install(|| {
            candidate_pairs
                .par_iter()
                .copied()
                .fold(MetadataValidatedPairBatch::default, |mut batch, pair| {
                    batch.score_pair(pair, atoms, compact_docs, template_compatibility);
                    batch
                })
                .reduce(
                    MetadataValidatedPairBatch::default,
                    MetadataValidatedPairBatch::merge,
                )
        })
    } else {
        let mut batch = MetadataValidatedPairBatch::default();
        for &pair in candidate_pairs {
            batch.score_pair(pair, atoms, compact_docs, template_compatibility);
        }
        batch
    }
}

pub(super) fn score_and_apply_metadata_atom_pair_batch(
    candidate_pairs: &mut Vec<(usize, MetadataDocIndex)>,
    atoms: &[MetadataContentAtom],
    compact_docs: &[CompactMetadataContentDocument],
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
) -> MetadataPairScoringStats {
    if candidate_pairs.is_empty() {
        return MetadataPairScoringStats::default();
    }
    let batch = collect_metadata_validated_atom_pair_hits(
        candidate_pairs,
        atoms,
        compact_docs,
        context.template_compatibility,
        context.pool,
    );
    candidate_pairs.clear();
    for (left, right) in batch.hits {
        let left_atom = &atoms[left];
        let right_atom = &atoms[metadata_doc_index_to_usize(right)];
        apply_metadata_atom_pair_union(
            context.data,
            context.chain_count,
            state,
            left_atom,
            right_atom,
        );
    }
    batch.stats
}

pub(super) fn score_and_apply_metadata_fallback_atom_pair_batch(
    candidate_pairs: &mut Vec<(usize, MetadataDocIndex)>,
    atoms: &[MetadataContentAtom],
    compact_docs: &[CompactMetadataContentDocument],
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
) -> MetadataPairScoringStats {
    if candidate_pairs.is_empty() {
        return MetadataPairScoringStats::default();
    }
    let batch = collect_metadata_validated_atom_pair_hits(
        candidate_pairs,
        atoms,
        compact_docs,
        context.template_compatibility,
        context.pool,
    );
    candidate_pairs.clear();
    for (left, right) in batch.hits {
        apply_metadata_fallback_atom_pair_union(
            &atoms[left],
            &atoms[metadata_doc_index_to_usize(right)],
            context,
            state,
        );
    }
    batch.stats
}

#[cfg(test)]
pub(super) fn collect_metadata_content_candidate_pairs(
    records: &[MetadataContentRecord],
    template_docs: &[MetadataDocIndex],
    template_matches: &MetadataTemplateMatches,
) -> Vec<(MetadataContractIndex, MetadataContractIndex)> {
    let compact = CompactMetadataContentSet::from_records(records);
    let index = MetadataContentCandidateIndex::new(&compact.docs);
    let mut scratch = MetadataCandidateScratch::new(records.len());
    let mut stats = MetadataContentUnionStats::default();
    let compatibility = MetadataTemplateCompatibility::Precomputed(template_matches);
    let mut pairs = Vec::new();
    for left in 0..records.len().saturating_sub(1) {
        scratch.clear_for_next_left();
        index.append_candidates_after(left, &compact.docs[left], &mut scratch);
        for &right in &scratch.candidates {
            let right_index = metadata_doc_index_to_usize(right);
            if !compatibility.matches(template_docs[left], template_docs[right_index], &mut stats) {
                continue;
            }
            pairs.push((
                records[left].contract_index,
                records[right_index].contract_index,
            ));
        }
    }
    pairs.sort_unstable();
    pairs
}

#[cfg(test)]
pub(super) fn union_metadata_shared_token_atoms(
    records: &[MetadataContentRecord],
    compact_docs: &[CompactMetadataContentDocument],
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
) -> MetadataContentUnionStats {
    let atoms = build_metadata_content_atoms(records, compact_docs, context.data);
    union_metadata_shared_token_atom_core(atoms, compact_docs, context, state)
}

fn union_metadata_shared_token_atom_core(
    atoms: Vec<MetadataContentAtom>,
    compact_docs: &[CompactMetadataContentDocument],
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
) -> MetadataContentUnionStats {
    let mut stats = MetadataContentUnionStats {
        atom_count: atoms.len(),
        ..MetadataContentUnionStats::default()
    };
    for atom in &atoms {
        apply_metadata_same_chain_group_union(
            context.data,
            context.chain_count,
            state,
            &atom.members,
        );
    }
    if atoms.len() < 2 {
        return stats;
    }
    if atoms.len() == METADATA_DIRECT_ATOM_GROUP_SIZE {
        let left = 0usize;
        let right = metadata_doc_index_from_usize(1);
        if !metadata_content_atoms_share_token(left, right, &atoms, compact_docs)
            || !metadata_template_atoms_share_safe_prefix(
                left,
                right,
                &atoms,
                context.template_compatibility,
            )
        {
            return stats;
        }
        stats.candidate_pairs = 1;
        let left_atom = &atoms[left];
        let right_atom = &atoms[1];
        let left_contract_index = metadata_contract_index_to_usize(left_atom.members[0]);
        let right_contract_index = metadata_contract_index_to_usize(right_atom.members[0]);
        let singleton_pair = left_atom.members.len() == 1 && right_atom.members.len() == 1;
        let same_chain = left_atom.chain_index == right_atom.chain_index;
        if (singleton_pair || same_chain)
            && metadata_pair_already_connected(
                context.data,
                context.chain_count,
                state,
                left_contract_index,
                right_contract_index,
            )
        {
            return stats;
        }
        let mut candidate_pairs = vec![(left, right)];
        let pair_stats = score_and_apply_metadata_atom_pair_batch(
            &mut candidate_pairs,
            &atoms,
            compact_docs,
            context,
            state,
        );
        stats.accumulate_pair_scoring(pair_stats);
        return stats;
    }
    let parallel = atoms.len() >= METADATA_CONTENT_PARALLEL_MIN_RECORDS;
    let candidate_index = if parallel {
        context.pool.install(|| {
            MetadataLocalCandidateIndex::from_atoms(
                compact_docs,
                &atoms,
                context.template_compatibility,
                true,
            )
        })
    } else {
        MetadataLocalCandidateIndex::from_atoms(
            compact_docs,
            &atoms,
            context.template_compatibility,
            false,
        )
    };
    let mut scratch = MetadataCandidateScratch::new(atoms.len());
    let mut candidate_pairs = Vec::with_capacity(METADATA_CONTENT_SCORE_BATCH_PAIRS);
    for left in 0..atoms.len().saturating_sub(1) {
        let left_atom = &atoms[left];
        let left_record_index = metadata_doc_index_to_usize(left_atom.representative_record_index);
        let left_contract_index = metadata_contract_index_to_usize(left_atom.members[0]);
        debug_assert_eq!(
            context.data.contracts[left_contract_index].chain_index,
            left_atom.chain_index
        );
        scratch.clear_for_next_left();
        let candidate_basis = candidate_index.append_candidates_after(
            left,
            left_atom,
            &compact_docs[left_record_index],
            context.template_compatibility,
            &mut scratch,
        );
        for &right in &scratch.candidates {
            if !metadata_candidate_intersects_both_dimensions(
                candidate_basis,
                left,
                right,
                &atoms,
                compact_docs,
                context.template_compatibility,
            ) {
                continue;
            }
            stats.candidate_pairs = stats.candidate_pairs.saturating_add(1);
            let right_atom = &atoms[metadata_doc_index_to_usize(right)];
            let right_contract_index = metadata_contract_index_to_usize(right_atom.members[0]);
            let singleton_pair = left_atom.members.len() == 1 && right_atom.members.len() == 1;
            let same_chain = left_atom.chain_index == right_atom.chain_index;
            if (singleton_pair || same_chain)
                && metadata_pair_already_connected(
                    context.data,
                    context.chain_count,
                    state,
                    left_contract_index,
                    right_contract_index,
                )
            {
                continue;
            }
            candidate_pairs.push((left, right));
            if candidate_pairs.len() >= METADATA_CONTENT_SCORE_BATCH_PAIRS {
                let batch_stats = score_and_apply_metadata_atom_pair_batch(
                    &mut candidate_pairs,
                    &atoms,
                    compact_docs,
                    context,
                    state,
                );
                stats.accumulate_pair_scoring(batch_stats);
            }
        }
    }
    let batch_stats = score_and_apply_metadata_atom_pair_batch(
        &mut candidate_pairs,
        &atoms,
        compact_docs,
        context,
        state,
    );
    stats.accumulate_pair_scoring(batch_stats);
    stats
}

#[cfg(test)]
pub(super) fn union_metadata_no_common_content_candidates(
    records: &[MetadataContentRecord],
    compact_docs: &[CompactMetadataContentDocument],
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
) -> MetadataContentUnionStats {
    let atoms =
        build_metadata_fallback_atoms(records, compact_docs, context.data, context.contract_tokens);
    union_metadata_no_common_atom_core(atoms, compact_docs, context, state)
}

fn union_metadata_no_common_atom_core(
    atoms: Vec<MetadataContentAtom>,
    compact_docs: &[CompactMetadataContentDocument],
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
) -> MetadataContentUnionStats {
    let mut stats = MetadataContentUnionStats {
        atom_count: atoms.len(),
        ..MetadataContentUnionStats::default()
    };
    for atom in &atoms {
        apply_metadata_fallback_atom_internal_unions(atom, context, state);
    }
    if atoms.len() < 2 {
        return stats;
    }
    if atoms.len() == METADATA_DIRECT_ATOM_GROUP_SIZE {
        let left = 0usize;
        let right = metadata_doc_index_from_usize(1);
        if !metadata_content_atoms_share_token(left, right, &atoms, compact_docs)
            || !metadata_template_atoms_share_safe_prefix(
                left,
                right,
                &atoms,
                context.template_compatibility,
            )
        {
            return stats;
        }
        stats.candidate_pairs = 1;
        let left_atom = &atoms[left];
        let right_atom = &atoms[1];
        let left_contract_index = metadata_contract_index_to_usize(left_atom.members[0]);
        let right_contract_index = metadata_contract_index_to_usize(right_atom.members[0]);
        let singleton_pair = left_atom.members.len() == 1 && right_atom.members.len() == 1;
        if singleton_pair
            && metadata_pair_already_connected(
                context.data,
                context.chain_count,
                state,
                left_contract_index,
                right_contract_index,
            )
        {
            return stats;
        }
        if !metadata_fallback_atoms_have_disjoint_token_groups(
            left_atom,
            right_atom,
            context.contract_tokens,
        ) {
            return stats;
        }
        let mut candidate_pairs = vec![(left, right)];
        let pair_stats = score_and_apply_metadata_fallback_atom_pair_batch(
            &mut candidate_pairs,
            &atoms,
            compact_docs,
            context,
            state,
        );
        stats.accumulate_pair_scoring(pair_stats);
        return stats;
    }
    let parallel = atoms.len() >= METADATA_CONTENT_PARALLEL_MIN_RECORDS;
    let candidate_index = if parallel {
        context.pool.install(|| {
            MetadataLocalCandidateIndex::from_atoms(
                compact_docs,
                &atoms,
                context.template_compatibility,
                true,
            )
        })
    } else {
        MetadataLocalCandidateIndex::from_atoms(
            compact_docs,
            &atoms,
            context.template_compatibility,
            false,
        )
    };
    let mut scratch = MetadataCandidateScratch::new(atoms.len());
    let mut candidate_pairs = Vec::with_capacity(METADATA_CONTENT_SCORE_BATCH_PAIRS);
    for left in 0..atoms.len().saturating_sub(1) {
        let left_atom = &atoms[left];
        let left_record_index = metadata_doc_index_to_usize(left_atom.representative_record_index);
        scratch.clear_for_next_left();
        let candidate_basis = candidate_index.append_candidates_after(
            left,
            left_atom,
            &compact_docs[left_record_index],
            context.template_compatibility,
            &mut scratch,
        );
        let left_contract_index = metadata_contract_index_to_usize(left_atom.members[0]);
        for &right in &scratch.candidates {
            if !metadata_candidate_intersects_both_dimensions(
                candidate_basis,
                left,
                right,
                &atoms,
                compact_docs,
                context.template_compatibility,
            ) {
                continue;
            }
            stats.candidate_pairs = stats.candidate_pairs.saturating_add(1);
            let right_atom = &atoms[metadata_doc_index_to_usize(right)];
            let right_index = metadata_contract_index_to_usize(right_atom.members[0]);
            let singleton_pair = left_atom.members.len() == 1 && right_atom.members.len() == 1;
            if singleton_pair
                && metadata_pair_already_connected(
                    context.data,
                    context.chain_count,
                    state,
                    left_contract_index,
                    right_index,
                )
            {
                continue;
            }
            if metadata_fallback_atoms_have_disjoint_token_groups(
                left_atom,
                right_atom,
                context.contract_tokens,
            ) {
                candidate_pairs.push((left, right));
                if candidate_pairs.len() >= METADATA_CONTENT_SCORE_BATCH_PAIRS {
                    let batch_stats = score_and_apply_metadata_fallback_atom_pair_batch(
                        &mut candidate_pairs,
                        &atoms,
                        compact_docs,
                        context,
                        state,
                    );
                    stats.accumulate_pair_scoring(batch_stats);
                }
            }
        }
    }
    let batch_stats = score_and_apply_metadata_fallback_atom_pair_batch(
        &mut candidate_pairs,
        &atoms,
        compact_docs,
        context,
        state,
    );
    stats.accumulate_pair_scoring(batch_stats);
    stats
}

#[cfg(test)]
pub(super) fn union_metadata_content_candidates(
    records: &[MetadataContentRecord],
    scope: MetadataContentScope,
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
) -> MetadataContentUnionStats {
    let compact = CompactMetadataContentSet::from_records(records);
    match scope {
        MetadataContentScope::SharedToken => {
            union_metadata_shared_token_atoms(records, &compact.docs, context, state)
        }
        MetadataContentScope::NoCommonToken => {
            union_metadata_no_common_content_candidates(records, &compact.docs, context, state)
        }
    }
}

pub(super) fn metadata_pair_already_connected(
    data: &MetadataData,
    chain_count: usize,
    state: &mut MetadataUnionState,
    left: usize,
    right: usize,
) -> bool {
    let left_chain = data.contracts[left].chain_index;
    let right_chain = data.contracts[right].chain_index;
    if left_chain == right_chain {
        return state.intra.find(left) == state.intra.find(right);
    }
    let cross_connected = state
        .cross
        .as_mut()
        .is_some_and(|cross| cross.connected(left, right));
    let (primary_chain, secondary_chain) = if left_chain < right_chain {
        (left_chain, right_chain)
    } else {
        (right_chain, left_chain)
    };
    let matrix_connected = state.chain_matrix.as_mut().is_some_and(|matrix| {
        matrix[chain_pair_index(primary_chain, secondary_chain, chain_count)].connected(left, right)
    });
    cross_connected && matrix_connected
}

pub(super) fn lexical_metadata_token_ids(
    entries: &[SourceMetadataDocEntry],
) -> HashMap<&str, usize> {
    let mut tokens = entries
        .iter()
        .flat_map(|entry| entry.doc.terms().iter().map(|(token, _)| token.as_str()))
        .collect::<Vec<_>>();
    tokens.par_sort_unstable();
    tokens.dedup();
    tokens
        .into_iter()
        .enumerate()
        .map(|(token_id, token)| (token, token_id))
        .collect()
}

impl InternedMetadataIndex {
    pub(super) fn doc_count(&self) -> usize {
        self.doc_count
    }

    pub(super) fn is_empty(&self) -> bool {
        self.doc_count == 0
    }

    pub(super) fn owned_memory_bytes(&self) -> usize {
        let bytes = self.scoring.owned_memory_bytes();
        #[cfg(test)]
        let bytes = bytes.saturating_add(self.postings.owned_memory_bytes());
        bytes
    }

    pub(super) fn logical_memory_bytes(&self) -> usize {
        let bytes = self.scoring.logical_memory_bytes();
        #[cfg(test)]
        let bytes = bytes.saturating_add(self.postings.logical_memory_bytes());
        bytes
    }

    pub(super) fn mapped_bytes(&self) -> usize {
        let bytes = self.scoring.mapped_bytes();
        #[cfg(test)]
        let bytes = bytes.saturating_add(self.postings.mapped_bytes());
        bytes
    }

    pub(super) fn remap_if_over_budget(
        &mut self,
        directory: &Path,
        maximum_owned_bytes: usize,
    ) -> std::io::Result<bool> {
        if self.owned_memory_bytes() <= maximum_owned_bytes {
            return Ok(false);
        }
        self.remap_postings(directory)?;
        Ok(true)
    }

    pub(super) fn remap_postings(&mut self, directory: &Path) -> std::io::Result<()> {
        #[cfg(test)]
        {
            let postings = std::mem::replace(
                &mut self.postings,
                CompactMetadataPostings::from_nested(Vec::new()),
            );
            self.postings = postings.persist_and_remap(directory)?;
        }
        let scoring = std::mem::replace(
            &mut self.scoring,
            CompactMetadataScoring::from_nested(Vec::new(), Vec::new()),
        );
        self.scoring = scoring.persist_and_remap(directory)?;
        Ok(())
    }

    pub(super) fn from_source_doc_entries(entries: Vec<SourceMetadataDocEntry>) -> Self {
        let token_ids = lexical_metadata_token_ids(&entries);
        let token_count = token_ids.len();

        // Phase 1 (parallel): intern each already-normalized compact term list
        // into token-ID source docs and weights; `unzip` preserves doc order.
        let (doc_weights, source_docs): (Vec<usize>, Vec<InternedMetadataSourceDoc>) = entries
            .par_iter()
            .map(|entry| {
                let doc_weight = entry.contracts.len();
                let source_doc =
                    InternedMetadataSourceDoc::from_metadata_doc(&entry.doc, &token_ids);
                (doc_weight, source_doc)
            })
            .unzip();

        #[cfg(test)]
        let test_token_ids = token_ids
            .iter()
            .map(|(token, token_id)| ((*token).to_owned(), *token_id))
            .collect();
        drop(token_ids);
        drop(entries);

        #[cfg(test)]
        let postings = {
            // Test-only global-recall index retained for the candidate-prefix
            // equivalence tests. Production scores templates lazily only for
            // content candidates and does not allocate this global posting set.
            let mut postings = vec![Vec::new(); token_count];
            for (doc_index, doc) in source_docs.iter().enumerate() {
                let compact_doc_index = metadata_doc_index_from_usize(doc_index);
                for &(token_id, _) in doc.terms() {
                    postings[token_id as usize].push(compact_doc_index);
                }
            }
            postings.par_iter_mut().for_each(|indices| {
                indices.sort_unstable();
                indices.dedup();
            });
            CompactMetadataPostings::from_nested(postings)
        };
        let corpus =
            InternedMetadataCorpus::from_doc_weights(&doc_weights, &source_docs, token_count);
        drop(doc_weights);
        let prepared_docs = source_docs
            .par_iter()
            .map(|doc| PreparedInternedMetadataDoc::new(doc, &corpus))
            .collect::<Vec<_>>();
        let max_token_weights = {
            let mut max_token_weights = vec![0.0f64; token_count];
            for prepared in &prepared_docs {
                for &(token, weight) in &prepared.token_weights {
                    max_token_weights[token] = max_token_weights[token].max(weight);
                }
            }
            max_token_weights
        };
        let queries = source_docs
            .par_iter()
            .map(|doc| PreparedInternedMetadataQuery::new_direct(doc, &corpus, &max_token_weights))
            .collect::<Vec<_>>();
        let doc_count = source_docs.len();
        drop(source_docs);
        drop(corpus);
        let scoring = CompactMetadataScoring::from_nested(queries, prepared_docs);
        Self {
            doc_count,
            scoring,
            #[cfg(test)]
            postings,
            #[cfg(test)]
            token_ids: test_token_ids,
            #[cfg(test)]
            build_thread_count: rayon::current_num_threads(),
        }
    }

    #[cfg(test)]
    pub(super) fn token_id(&self, token: &str) -> Option<usize> {
        self.token_ids.get(token).copied()
    }
}
