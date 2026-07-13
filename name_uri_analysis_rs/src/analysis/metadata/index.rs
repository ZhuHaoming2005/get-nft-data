use std::borrow::Cow;
use std::collections::{hash_map::RandomState, HashMap};
use std::hash::BuildHasher;
use std::path::Path;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;
#[cfg(test)]
use std::time::Duration;

use duckdb::Connection;
use rayon::prelude::*;

use super::super::{
    arrow_i64_column, arrow_string_column, chain_pair_index, AnalysisError, MetadataRecallMode,
    ProgressCounters, ProgressTracker, SparseUnionFind, UnionFind,
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
const METADATA_PARALLEL_LEFT_WAVE_MULTIPLIER: usize = 2;
pub(super) const METADATA_DENSE_INTERSECTION_MIN_SCAN_COST: usize = 4 * 1024;
const METADATA_DENSE_INTERSECTION_MAX_COST_RATIO: usize = 8;
const METADATA_TEMPLATE_COMPACTION_SAMPLE_SIZE: usize = 512;
const METADATA_TEMPLATE_COMPACTION_MIN_DUPLICATE_DENOMINATOR: usize = 8;
const METADATA_CONSERVATIVE_ANCHOR_COUNT: usize = 16;
const METADATA_CONSERVATIVE_SIMHASH_BANDS: usize = 8;
const METADATA_CONSERVATIVE_SIMHASH_BAND_BITS: usize = 8;
const METADATA_CONSERVATIVE_SIMHASH_HAMMING_THRESHOLD: u32 = 32;
const METADATA_CONSERVATIVE_HIGH_FREQUENCY_MIN_DOCS: usize = 32;
const METADATA_CONSERVATIVE_HIGH_FREQUENCY_DIVISOR: usize = 5;
const METADATA_CONSERVATIVE_MIN_ATOMS: usize = 256;
const METADATA_CONSERVATIVE_CALIBRATION_DIVISOR: u64 = 100;
const METADATA_CONSERVATIVE_CONTRACT_DRIFT_PER_MILLE: u64 = 5;
const METADATA_CONSERVATIVE_COMPONENT_DRIFT_PER_MILLE: u64 = 2;
const NO_METADATA_ATOM: usize = usize::MAX;
pub(super) const METADATA_TEMPLATE_SCORE_CACHE_SLOTS: usize = 256 * 1024;
pub(super) const METADATA_TEMPLATE_SCORE_CACHE_WAYS: usize = 4;
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

struct MetadataSparseCandidatePostings {
    token_ids: Vec<u32>,
    posting_offsets: Vec<u64>,
    posting_atoms: Vec<MetadataDocIndex>,
}

#[derive(Clone, Copy)]
struct MetadataConservativeSketch {
    simhash: u64,
    anchors: [u32; METADATA_CONSERVATIVE_ANCHOR_COUNT],
    anchor_len: u8,
    has_terms: bool,
}

#[derive(Clone, Copy)]
struct MetadataConservativeTokenStats {
    document_frequency: usize,
    idf: f64,
    hash: u64,
    anchor_eligible: bool,
}

fn insert_metadata_conservative_anchor(
    anchors: &mut [(usize, u32); METADATA_CONSERVATIVE_ANCHOR_COUNT],
    anchor_len: &mut usize,
    candidate: (usize, u32),
) {
    if *anchor_len < anchors.len() {
        anchors[*anchor_len] = candidate;
        *anchor_len += 1;
    } else if candidate >= anchors[*anchor_len - 1] {
        return;
    } else {
        anchors[*anchor_len - 1] = candidate;
    }
    let mut index = (*anchor_len).saturating_sub(1);
    while index > 0 && anchors[index] < anchors[index - 1] {
        anchors.swap(index, index - 1);
        index -= 1;
    }
}

pub(super) struct MetadataConservativeDimensionIndex {
    sketches: Vec<MetadataConservativeSketch>,
    anchor_postings: MetadataSparseCandidatePostings,
    simhash_band_postings: MetadataSparseCandidatePostings,
}

pub(super) struct MetadataTemplateCandidateIndex {
    full: MetadataSparseCandidatePostings,
    prefix: MetadataSparseCandidatePostings,
}

pub(super) struct MetadataConservativeCandidateIndex {
    exact_template: Option<MetadataTemplateCandidateIndex>,
    exact_content: Option<MetadataContentCandidateIndex>,
    template: MetadataConservativeDimensionIndex,
    content: MetadataConservativeDimensionIndex,
}

#[derive(Clone, Copy)]
struct MetadataPostingRange {
    start: usize,
    end: usize,
}

#[derive(Default)]
struct MetadataCandidatePostingPlan {
    content: Vec<MetadataPostingRange>,
    template_full: Vec<MetadataPostingRange>,
    template_prefix: Vec<MetadataPostingRange>,
}

impl MetadataCandidatePostingPlan {
    fn clear(&mut self) {
        self.content.clear();
        self.template_full.clear();
        self.template_prefix.clear();
    }
}

pub(super) enum MetadataLocalCandidateIndex {
    Adaptive {
        template: MetadataTemplateCandidateIndex,
        content: MetadataContentCandidateIndex,
    },
    Conservative(Box<MetadataConservativeCandidateIndex>),
    #[cfg(test)]
    ContentOnly(MetadataContentCandidateIndex),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum MetadataLocalCandidateBasis {
    Template,
    Content,
    Intersection,
    ConservativeIntersection,
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
    secondary_seen_generation: Vec<u16>,
    secondary_generation: u16,
    secondary_candidates: Vec<MetadataDocIndex>,
    posting_plan: MetadataCandidatePostingPlan,
    raw_candidate_count: usize,
}

pub(super) struct MetadataCandidateScratchPool {
    pub(super) doc_count: usize,
    scratches: Mutex<Vec<MetadataCandidateScratch>>,
}

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
    pub(super) recall_mode: MetadataRecallMode,
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
    pub(super) raw_candidate_pairs: u64,
    pub(super) dimension_rejected_pairs: u64,
    pub(super) candidate_pairs: u64,
    pub(super) already_connected_pairs: u64,
    pub(super) scored_pairs: u64,
    pub(super) matched_pairs: u64,
    pub(super) template_candidate_pairs: u64,
    pub(super) template_scored_pairs: u64,
    pub(super) template_matched_pairs: u64,
    pub(super) template_rejected_pairs: u64,
    pub(super) template_cache_hits: u64,
    pub(super) template_cache_misses: u64,
    pub(super) template_batch_unique_pairs: u64,
    pub(super) template_batch_reused_pairs: u64,
    pub(super) recall_calibration: MetadataRecallCalibrationStats,
    pub(super) conservative_groups: u64,
    pub(super) exact_fallback_groups: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct MetadataRecallCalibrationStats {
    pub(super) sampled_left_atoms: u64,
    pub(super) exact_candidate_pairs: u64,
    pub(super) conservative_candidate_pairs: u64,
    pub(super) exact_matched_pairs: u64,
    pub(super) missed_matched_pairs: u64,
    pub(super) exact_duplicate_contract_members: u64,
    pub(super) missed_duplicate_contract_members: u64,
    pub(super) exact_component_members: u64,
    pub(super) shifted_component_members: u64,
}

impl MetadataRecallCalibrationStats {
    pub(super) fn requires_exact_fallback(&self) -> bool {
        let contract_drift_exceeded = self.exact_duplicate_contract_members > 0
            && self.missed_duplicate_contract_members.saturating_mul(1_000)
                > self
                    .exact_duplicate_contract_members
                    .saturating_mul(METADATA_CONSERVATIVE_CONTRACT_DRIFT_PER_MILLE);
        let component_drift_exceeded = self.exact_component_members > 0
            && self.shifted_component_members.saturating_mul(1_000)
                > self
                    .exact_component_members
                    .saturating_mul(METADATA_CONSERVATIVE_COMPONENT_DRIFT_PER_MILLE);
        contract_drift_exceeded || component_drift_exceeded
    }

    fn accumulate(&mut self, other: Self) {
        self.sampled_left_atoms = self
            .sampled_left_atoms
            .saturating_add(other.sampled_left_atoms);
        self.exact_candidate_pairs = self
            .exact_candidate_pairs
            .saturating_add(other.exact_candidate_pairs);
        self.conservative_candidate_pairs = self
            .conservative_candidate_pairs
            .saturating_add(other.conservative_candidate_pairs);
        self.exact_matched_pairs = self
            .exact_matched_pairs
            .saturating_add(other.exact_matched_pairs);
        self.missed_matched_pairs = self
            .missed_matched_pairs
            .saturating_add(other.missed_matched_pairs);
        self.exact_duplicate_contract_members = self
            .exact_duplicate_contract_members
            .saturating_add(other.exact_duplicate_contract_members);
        self.missed_duplicate_contract_members = self
            .missed_duplicate_contract_members
            .saturating_add(other.missed_duplicate_contract_members);
        self.exact_component_members = self
            .exact_component_members
            .saturating_add(other.exact_component_members);
        self.shifted_component_members = self
            .shifted_component_members
            .saturating_add(other.shifted_component_members);
    }
}

impl MetadataContentUnionStats {
    pub(super) fn accumulate(&mut self, other: Self) {
        self.atom_count = self.atom_count.saturating_add(other.atom_count);
        self.raw_candidate_pairs = self
            .raw_candidate_pairs
            .saturating_add(other.raw_candidate_pairs);
        self.dimension_rejected_pairs = self
            .dimension_rejected_pairs
            .saturating_add(other.dimension_rejected_pairs);
        self.candidate_pairs = self.candidate_pairs.saturating_add(other.candidate_pairs);
        self.already_connected_pairs = self
            .already_connected_pairs
            .saturating_add(other.already_connected_pairs);
        self.scored_pairs = self.scored_pairs.saturating_add(other.scored_pairs);
        self.matched_pairs = self.matched_pairs.saturating_add(other.matched_pairs);
        self.template_candidate_pairs = self
            .template_candidate_pairs
            .saturating_add(other.template_candidate_pairs);
        self.template_scored_pairs = self
            .template_scored_pairs
            .saturating_add(other.template_scored_pairs);
        self.template_matched_pairs = self
            .template_matched_pairs
            .saturating_add(other.template_matched_pairs);
        self.template_rejected_pairs = self
            .template_rejected_pairs
            .saturating_add(other.template_rejected_pairs);
        self.template_cache_hits = self
            .template_cache_hits
            .saturating_add(other.template_cache_hits);
        self.template_cache_misses = self
            .template_cache_misses
            .saturating_add(other.template_cache_misses);
        self.template_batch_unique_pairs = self
            .template_batch_unique_pairs
            .saturating_add(other.template_batch_unique_pairs);
        self.template_batch_reused_pairs = self
            .template_batch_reused_pairs
            .saturating_add(other.template_batch_reused_pairs);
        self.recall_calibration.accumulate(other.recall_calibration);
        self.conservative_groups = self
            .conservative_groups
            .saturating_add(other.conservative_groups);
        self.exact_fallback_groups = self
            .exact_fallback_groups
            .saturating_add(other.exact_fallback_groups);
    }

    pub(super) fn accumulate_pair_scoring(&mut self, other: MetadataPairScoringStats) {
        self.scored_pairs = self.scored_pairs.saturating_add(other.content_scored_pairs);
        self.matched_pairs = self
            .matched_pairs
            .saturating_add(other.content_matched_pairs);
        self.template_candidate_pairs = self
            .template_candidate_pairs
            .saturating_add(other.template_candidate_pairs);
        self.template_scored_pairs = self
            .template_scored_pairs
            .saturating_add(other.template_scored_pairs);
        self.template_matched_pairs = self
            .template_matched_pairs
            .saturating_add(other.template_matched_pairs);
        self.template_rejected_pairs = self
            .template_rejected_pairs
            .saturating_add(other.template_rejected_pairs);
        self.template_cache_hits = self
            .template_cache_hits
            .saturating_add(other.template_cache_hits);
        self.template_cache_misses = self
            .template_cache_misses
            .saturating_add(other.template_cache_misses);
        self.template_batch_unique_pairs = self
            .template_batch_unique_pairs
            .saturating_add(other.template_batch_unique_pairs);
        self.template_batch_reused_pairs = self
            .template_batch_reused_pairs
            .saturating_add(other.template_batch_reused_pairs);
    }
}

pub(super) fn metadata_shared_token_group_progress_counters(
    completed_groups: u64,
    base: ProgressCounters,
    live: &MetadataContentUnionStats,
) -> ProgressCounters {
    ProgressCounters {
        groups: completed_groups,
        candidates: base.candidates.saturating_add(live.candidate_pairs),
        scored: base.scored.saturating_add(live.scored_pairs),
        matched: base.matched.saturating_add(live.matched_pairs),
    }
}

#[derive(Clone, Copy)]
struct MetadataSharedTokenGroupProgress<'a> {
    tracker: &'a ProgressTracker,
    completed_groups: u64,
    base: ProgressCounters,
}

impl MetadataSharedTokenGroupProgress<'_> {
    fn update(self, live: &MetadataContentUnionStats) {
        self.tracker.advance_task(
            0,
            metadata_shared_token_group_progress_counters(self.completed_groups, self.base, live),
        );
    }

    fn update_calibration(
        self,
        completed_lefts: usize,
        total_lefts: usize,
        calibration: &MetadataRecallCalibrationStats,
    ) {
        self.tracker.update_task_label(format!(
            "calibrating conservative metadata recall; {completed_lefts}/{total_lefts} sampled lefts; exact/conservative candidates {}/{}; exact/missed matches {}/{}",
            calibration.exact_candidate_pairs,
            calibration.conservative_candidate_pairs,
            calibration.exact_matched_pairs,
            calibration.missed_matched_pairs,
        ));
        self.update(&MetadataContentUnionStats::default());
    }

    fn finish_calibration(self) {
        self.tracker
            .update_task_label("matching shared-token memberships");
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
                let (left_score, right_score) = scoring.score_bidirectional(left, right);
                if left_score >= METADATA_THRESHOLD {
                    (true, 1)
                } else {
                    (right_score >= METADATA_THRESHOLD, 2)
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
    score_count: u8,
    matched: bool,
    valid: bool,
}

impl MetadataTemplateScoreCacheEntry {
    const EMPTY: Self = Self {
        key: 0,
        score_count: 0,
        matched: false,
        valid: false,
    };
}

pub(super) struct MetadataTemplateScoreCache {
    entries: Box<[MetadataTemplateScoreCacheEntry]>,
}

#[derive(Default)]
pub(super) struct MetadataTemplateScoreCachePool {
    caches: Mutex<Vec<MetadataTemplateScoreCache>>,
}

struct MetadataTemplateScoreCacheLease<'a> {
    pool: &'a MetadataTemplateScoreCachePool,
    cache: Option<MetadataTemplateScoreCache>,
}

impl Default for MetadataTemplateScoreCache {
    fn default() -> Self {
        Self {
            entries: vec![
                MetadataTemplateScoreCacheEntry::EMPTY;
                METADATA_TEMPLATE_SCORE_CACHE_SLOTS
            ]
            .into_boxed_slice(),
        }
    }
}

impl MetadataTemplateScoreCache {
    pub(super) const fn memory_bytes() -> usize {
        std::mem::size_of::<Self>().saturating_add(
            METADATA_TEMPLATE_SCORE_CACHE_SLOTS
                .saturating_mul(std::mem::size_of::<MetadataTemplateScoreCacheEntry>()),
        )
    }

    fn mixed_key(key: u64) -> u64 {
        key.wrapping_mul(0x9e37_79b9_7f4a_7c15)
            .wrapping_add(key.rotate_right(29))
    }

    fn set_start(key: u64) -> usize {
        debug_assert!(METADATA_TEMPLATE_SCORE_CACHE_SLOTS.is_power_of_two());
        debug_assert!(METADATA_TEMPLATE_SCORE_CACHE_WAYS.is_power_of_two());
        debug_assert_eq!(
            METADATA_TEMPLATE_SCORE_CACHE_SLOTS % METADATA_TEMPLATE_SCORE_CACHE_WAYS,
            0
        );
        let set_count = METADATA_TEMPLATE_SCORE_CACHE_SLOTS / METADATA_TEMPLATE_SCORE_CACHE_WAYS;
        (Self::mixed_key(key) as usize & (set_count - 1)) * METADATA_TEMPLATE_SCORE_CACHE_WAYS
    }

    pub(super) fn evaluate(
        &mut self,
        left: MetadataDocIndex,
        right: MetadataDocIndex,
        compatibility: MetadataTemplateCompatibility<'_>,
    ) -> (bool, u64, bool) {
        if left == right {
            return (true, 0, false);
        }
        let (left, right) = if left < right {
            (left, right)
        } else {
            (right, left)
        };
        let key = (u64::from(left) << 32) | u64::from(right);
        let set_start = Self::set_start(key);
        let set_end = set_start + METADATA_TEMPLATE_SCORE_CACHE_WAYS;
        for cached in &self.entries[set_start..set_end] {
            if cached.valid && cached.key == key {
                return (cached.matched, u64::from(cached.score_count), true);
            }
        }
        let (matched, scores) = compatibility.evaluate(left, right);
        let slot = self.entries[set_start..set_end]
            .iter()
            .position(|entry| !entry.valid)
            .map(|offset| set_start + offset)
            .unwrap_or_else(|| {
                let mixed = Self::mixed_key(key);
                set_start + ((mixed >> 32) as usize & (METADATA_TEMPLATE_SCORE_CACHE_WAYS - 1))
            });
        self.entries[slot] = MetadataTemplateScoreCacheEntry {
            key,
            score_count: scores as u8,
            matched,
            valid: true,
        };
        (matched, scores, false)
    }
}

impl MetadataTemplateScoreCachePool {
    fn take(&self) -> MetadataTemplateScoreCacheLease<'_> {
        let cache = self
            .caches
            .lock()
            .expect("metadata template score cache pool lock poisoned")
            .pop()
            .unwrap_or_default();
        MetadataTemplateScoreCacheLease {
            pool: self,
            cache: Some(cache),
        }
    }
}

impl std::ops::Deref for MetadataTemplateScoreCacheLease<'_> {
    type Target = MetadataTemplateScoreCache;

    fn deref(&self) -> &Self::Target {
        self.cache
            .as_ref()
            .expect("metadata template score cache lease is empty")
    }
}

impl std::ops::DerefMut for MetadataTemplateScoreCacheLease<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.cache
            .as_mut()
            .expect("metadata template score cache lease is empty")
    }
}

impl Drop for MetadataTemplateScoreCacheLease<'_> {
    fn drop(&mut self) {
        let Some(cache) = self.cache.take() else {
            return;
        };
        self.pool
            .caches
            .lock()
            .expect("metadata template score cache pool lock poisoned")
            .push(cache);
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

    fn scoring_peak_bytes(&self, scoring_workers: usize, recall_mode: MetadataRecallMode) -> usize {
        if self.atoms.is_empty() {
            return 0;
        }
        let sparse_template_entry_upper_bytes = std::mem::size_of::<MetadataDocIndex>()
            .saturating_add(std::mem::size_of::<u32>())
            .saturating_add(std::mem::size_of::<u64>());
        let content_candidate_index =
            Self::vec_bytes_upper(self.term_count, std::mem::size_of::<MetadataDocIndex>())
                .saturating_add(Self::vec_bytes_upper(
                    self.token_ids.len().saturating_add(1),
                    2usize.saturating_mul(std::mem::size_of::<u64>()),
                ));
        let template_candidate_index = Self::vec_bytes_upper(
            self.template_candidate_term_count,
            sparse_template_entry_upper_bytes,
        )
        .saturating_add(4usize.saturating_mul(sparse_template_entry_upper_bytes));
        let template_candidate_flat_build = Self::vec_bytes_upper(
            self.template_candidate_term_count,
            std::mem::size_of::<(u32, MetadataDocIndex)>(),
        );
        let uses_adaptive_index = self.atoms.len() > METADATA_DIRECT_ATOM_GROUP_SIZE;
        let conservative_candidate_index = if uses_adaptive_index
            && recall_mode == MetadataRecallMode::Conservative
            && self.atoms.len() >= METADATA_CONSERVATIVE_MIN_ATOMS
        {
            let posting_count = self.atoms.len().saturating_mul(
                2usize.saturating_mul(
                    METADATA_CONSERVATIVE_ANCHOR_COUNT
                        .saturating_add(METADATA_CONSERVATIVE_SIMHASH_BANDS),
                ),
            );
            let frequency_entry_count = self
                .term_count
                .saturating_add(self.template_candidate_term_count);
            2usize
                .saturating_mul(Self::vec_bytes_upper(
                    self.atoms.len(),
                    std::mem::size_of::<MetadataConservativeSketch>(),
                ))
                .saturating_add(Self::vec_bytes_upper(
                    posting_count,
                    std::mem::size_of::<(u32, MetadataDocIndex)>(),
                ))
                .saturating_add(Self::vec_bytes_upper(
                    posting_count,
                    std::mem::size_of::<MetadataDocIndex>()
                        .saturating_add(std::mem::size_of::<u32>())
                        .saturating_add(std::mem::size_of::<u64>()),
                ))
                .saturating_add(super::load::hash_table_allocation_for_len_upper(
                    frequency_entry_count,
                    std::mem::size_of::<(u32, u32)>(),
                ))
                .saturating_add(super::load::hash_table_allocation_for_len_upper(
                    frequency_entry_count,
                    std::mem::size_of::<(u32, MetadataConservativeTokenStats)>(),
                ))
                .saturating_add(Self::vec_bytes_upper(
                    self.atoms.len(),
                    2usize
                        .saturating_mul(std::mem::size_of::<usize>())
                        .saturating_add(2usize.saturating_mul(std::mem::size_of::<u8>()))
                        .saturating_add(std::mem::size_of::<u16>()),
                ))
                .saturating_add(3usize.saturating_mul(
                    super::load::hash_table_allocation_for_len_upper(
                        self.atoms.len(),
                        std::mem::size_of::<((usize, usize), u64)>(),
                    ),
                ))
        } else {
            0
        };
        let candidate_index = if uses_adaptive_index {
            content_candidate_index
                .saturating_add(template_candidate_index)
                .saturating_add(template_candidate_flat_build)
                .saturating_add(conservative_candidate_index)
        } else {
            0
        };
        let candidate_scratch = if uses_adaptive_index {
            self.atoms
                .len()
                .saturating_mul(
                    2usize
                        .saturating_mul(std::mem::size_of::<u16>())
                        .saturating_add(3 * std::mem::size_of::<MetadataDocIndex>()),
                )
                .saturating_mul(scoring_workers.max(1))
        } else {
            0
        };
        // Each parallel wave retains its filtered right-hand candidates until
        // the serial DSU consumer applies them in original left-atom order.
        // This deliberately spends bounded memory to parallelize the dominant
        // posting scans without changing union or scoring order.
        let candidate_wave = if uses_adaptive_index {
            Self::vec_bytes_upper(self.atoms.len(), std::mem::size_of::<MetadataDocIndex>())
                .saturating_mul(scoring_workers.max(1))
                .saturating_mul(METADATA_PARALLEL_LEFT_WAVE_MULTIPLIER)
                .saturating_mul(2)
        } else {
            0
        };
        let pair_batch_capacity = if uses_adaptive_index {
            METADATA_CONTENT_SCORE_BATCH_PAIRS
        } else {
            usize::from(self.atoms.len() == METADATA_DIRECT_ATOM_GROUP_SIZE)
        };
        let pair_batch_bytes = 2usize
            .saturating_mul(std::mem::size_of::<(usize, MetadataDocIndex)>())
            .saturating_add(std::mem::size_of::<(u64, usize)>())
            .saturating_add(std::mem::size_of::<u64>())
            .saturating_add(
                2usize.saturating_mul(std::mem::size_of::<MetadataTemplatePairEvaluation>()),
            );
        let pair_batches = pair_batch_capacity.saturating_mul(pair_batch_bytes);
        // A parallel fold and its reduce-side accumulator can coexist for
        // every worker, so reserve both fixed-size template caches.
        let template_cache_count = if uses_adaptive_index {
            scoring_workers.max(1).saturating_mul(2)
        } else {
            pair_batch_capacity
        };
        let template_score_caches =
            template_cache_count.saturating_mul(MetadataTemplateScoreCache::memory_bytes());
        let union_scratch = self.member_count.saturating_mul(
            2usize
                .saturating_mul(std::mem::size_of::<usize>())
                .saturating_add(std::mem::size_of::<MetadataContractIndex>()),
        );
        let peak = self
            .atomized_memory_bytes()
            .saturating_add(candidate_index)
            .saturating_add(candidate_scratch)
            .saturating_add(candidate_wave)
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
        recall_mode: MetadataRecallMode,
    ) -> Result<(), AnalysisError> {
        let build_peak = self
            .builder_memory_bytes()
            .saturating_add(raw_parse_reserve_bytes);
        let peak = build_peak.max(self.scoring_peak_bytes(scoring_workers, recall_mode));
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
                    context.recall_mode,
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
            context.recall_mode,
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
                context.recall_mode,
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
            context.recall_mode,
        )?;
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn union(
        self,
        context: &MetadataContentUnionContext<'_>,
        state: &mut MetadataUnionState,
    ) -> MetadataContentUnionStats {
        let template_cache_pool = MetadataTemplateScoreCachePool::default();
        self.union_with_budget(
            context,
            state,
            usize::MAX,
            &template_cache_pool,
            MetadataRecallMode::Exact,
            None,
        )
        .expect("unbounded metadata test group must fit memory")
    }

    fn union_with_budget(
        mut self,
        context: &MetadataContentUnionContext<'_>,
        state: &mut MetadataUnionState,
        maximum_bytes: usize,
        template_cache_pool: &MetadataTemplateScoreCachePool,
        recall_mode: MetadataRecallMode,
        progress: Option<MetadataSharedTokenGroupProgress<'_>>,
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
            recall_mode,
        )?;
        let (atoms, docs) = self.compact.into_atomized_parts();
        Ok(union_metadata_shared_token_atom_core(
            atoms,
            &docs,
            context,
            state,
            template_cache_pool,
            recall_mode,
            progress,
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
    template_cache_pool: &MetadataTemplateScoreCachePool,
    recall_mode: MetadataRecallMode,
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
        stats.accumulate(group.union_with_budget(
            context,
            state,
            group_working_bytes,
            template_cache_pool,
            recall_mode,
            None,
        )?);
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

    #[cfg(test)]
    pub(super) fn append_candidates_after(
        &self,
        record_index: usize,
        document: &CompactMetadataContentDocument,
        scratch: &mut MetadataCandidateScratch,
    ) {
        let mut plan = MetadataCandidatePostingPlan::default();
        self.plan_candidates_after(record_index, document, &mut plan);
        self.append_planned_candidates(&plan, scratch);
    }

    fn plan_candidates_after(
        &self,
        record_index: usize,
        document: &CompactMetadataContentDocument,
        plan: &mut MetadataCandidatePostingPlan,
    ) -> usize {
        let compact_record_index = metadata_doc_index_from_usize(record_index);
        for &(token_id, _) in &document.terms {
            plan.content
                .push(self.posting_range_after(token_id, compact_record_index));
        }
        plan.content.iter().fold(0usize, |cost, range| {
            cost.saturating_add(range.end.saturating_sub(range.start))
        })
    }

    fn append_planned_candidates(
        &self,
        plan: &MetadataCandidatePostingPlan,
        scratch: &mut MetadataCandidateScratch,
    ) {
        for range in &plan.content {
            for &right in &self.posting_atoms[range.start..range.end] {
                scratch.push_once(right);
            }
        }
    }

    fn posting_range_after(
        &self,
        token_id: u32,
        record_index: MetadataDocIndex,
    ) -> MetadataPostingRange {
        let token_index = token_id as usize;
        if token_index + 1 >= self.posting_offsets.len() {
            return MetadataPostingRange { start: 0, end: 0 };
        }
        let posting_start = self.posting_offsets[token_index] as usize;
        let posting_end = self.posting_offsets[token_index + 1] as usize;
        let posting = &self.posting_atoms[posting_start..posting_end];
        let relative_start = posting.partition_point(|&right| right <= record_index);
        MetadataPostingRange {
            start: posting_start + relative_start,
            end: posting_end,
        }
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

impl MetadataSparseCandidatePostings {
    fn from_sorted_entries(entries: Vec<(u32, MetadataDocIndex)>) -> Self {
        let mut token_ids = Vec::new();
        let mut posting_offsets = Vec::new();
        let mut posting_atoms = Vec::with_capacity(entries.len());
        for (token_id, atom) in entries {
            if token_ids.last().copied() != Some(token_id) {
                token_ids.push(token_id);
                posting_offsets.push(posting_atoms.len() as u64);
            }
            posting_atoms.push(atom);
        }
        posting_offsets.push(posting_atoms.len() as u64);
        Self {
            token_ids,
            posting_offsets,
            posting_atoms,
        }
    }

    fn from_bounded_unsorted_entries(
        entries: Vec<(u32, MetadataDocIndex)>,
        key_count: usize,
    ) -> Self {
        let mut posting_offsets = vec![0u64; key_count.saturating_add(1)];
        for &(key, _) in &entries {
            posting_offsets[key as usize + 1] = posting_offsets[key as usize + 1].saturating_add(1);
        }
        for key in 0..key_count {
            posting_offsets[key + 1] =
                posting_offsets[key + 1].saturating_add(posting_offsets[key]);
        }
        let mut cursors = posting_offsets[..key_count].to_vec();
        let mut posting_atoms = vec![0; entries.len()];
        for (key, atom) in entries {
            let cursor = &mut cursors[key as usize];
            posting_atoms[*cursor as usize] = atom;
            *cursor = cursor.saturating_add(1);
        }
        Self {
            token_ids: (0..key_count as u32).collect(),
            posting_offsets,
            posting_atoms,
        }
    }

    fn posting_range_after(
        &self,
        token_id: u32,
        record_index: MetadataDocIndex,
    ) -> MetadataPostingRange {
        let Ok(token_index) = self.token_ids.binary_search(&token_id) else {
            return MetadataPostingRange { start: 0, end: 0 };
        };
        let posting_start = self.posting_offsets[token_index] as usize;
        let posting_end = self.posting_offsets[token_index + 1] as usize;
        let posting = &self.posting_atoms[posting_start..posting_end];
        let relative_start = posting.partition_point(|&right| right <= record_index);
        MetadataPostingRange {
            start: posting_start + relative_start,
            end: posting_end,
        }
    }

    fn append_planned_candidates(
        &self,
        ranges: &[MetadataPostingRange],
        scratch: &mut MetadataCandidateScratch,
    ) {
        for range in ranges {
            for &right in &self.posting_atoms[range.start..range.end] {
                scratch.push_once(right);
            }
        }
    }
}

fn stable_metadata_recall_token_hash(token: u32) -> u64 {
    let mut value = u64::from(token).wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn metadata_recall_simhash_band_key(simhash: u64, band_index: usize) -> u32 {
    let shift = band_index.saturating_mul(METADATA_CONSERVATIVE_SIMHASH_BAND_BITS);
    let value = ((simhash >> shift) & 0xff) as u32;
    (band_index as u32) << METADATA_CONSERVATIVE_SIMHASH_BAND_BITS | value
}

impl MetadataConservativeDimensionIndex {
    fn from_token_visitor(
        atom_count: usize,
        parallel: bool,
        visit_tokens: impl Fn(usize, &mut dyn FnMut(u32)),
    ) -> Self {
        let mut document_frequencies = HashMap::<u32, u32>::new();
        for atom_index in 0..atom_count {
            visit_tokens(atom_index, &mut |token| {
                let frequency = document_frequencies.entry(token).or_default();
                *frequency = frequency.saturating_add(1);
            });
        }
        let total_documents = atom_count.max(1) as f64;
        let token_stats = document_frequencies
            .into_iter()
            .map(|(token, document_frequency)| {
                let document_frequency = document_frequency as usize;
                let high_frequency = atom_count >= METADATA_CONSERVATIVE_HIGH_FREQUENCY_MIN_DOCS
                    && document_frequency
                        .saturating_mul(METADATA_CONSERVATIVE_HIGH_FREQUENCY_DIVISOR)
                        > atom_count;
                (
                    token,
                    MetadataConservativeTokenStats {
                        document_frequency,
                        idf: ((total_documents + 1.0) / (document_frequency as f64 + 0.5)).ln(),
                        hash: stable_metadata_recall_token_hash(token),
                        anchor_eligible: !high_frequency,
                    },
                )
            })
            .collect::<HashMap<_, _>>();

        let mut sketches = Vec::with_capacity(atom_count);
        let mut anchor_entries = Vec::new();
        let mut band_entries =
            Vec::with_capacity(atom_count.saturating_mul(METADATA_CONSERVATIVE_SIMHASH_BANDS));
        for atom_index in 0..atom_count {
            let mut weights = [0.0f64; 64];
            let mut anchors = [(usize::MAX, u32::MAX); METADATA_CONSERVATIVE_ANCHOR_COUNT];
            let mut anchor_len = 0usize;
            let mut has_terms = false;
            visit_tokens(atom_index, &mut |token| {
                has_terms = true;
                let stats = &token_stats[&token];
                if stats.anchor_eligible {
                    insert_metadata_conservative_anchor(
                        &mut anchors,
                        &mut anchor_len,
                        (stats.document_frequency, token),
                    );
                }
                for (bit, weight) in weights.iter_mut().enumerate() {
                    if (stats.hash >> bit) & 1 == 1 {
                        *weight += stats.idf;
                    } else {
                        *weight -= stats.idf;
                    }
                }
            });
            let mut simhash = 0u64;
            for (bit, weight) in weights.into_iter().enumerate() {
                if weight >= 0.0 {
                    simhash |= 1u64 << bit;
                }
            }
            let mut anchor_values = [0u32; METADATA_CONSERVATIVE_ANCHOR_COUNT];
            for (output, &(_, token)) in anchor_values.iter_mut().zip(&anchors[..anchor_len]) {
                *output = token;
            }
            anchor_values[..anchor_len].sort_unstable();
            let compact_atom_index = metadata_doc_index_from_usize(atom_index);
            anchor_entries.extend(
                anchor_values[..anchor_len]
                    .iter()
                    .map(|&token| (token, compact_atom_index)),
            );
            if has_terms {
                band_entries.extend((0..METADATA_CONSERVATIVE_SIMHASH_BANDS).map(|band_index| {
                    (
                        metadata_recall_simhash_band_key(simhash, band_index),
                        compact_atom_index,
                    )
                }));
            }
            sketches.push(MetadataConservativeSketch {
                simhash,
                anchors: anchor_values,
                anchor_len: anchor_len as u8,
                has_terms,
            });
        }
        if parallel {
            anchor_entries.par_sort_unstable();
        } else {
            anchor_entries.sort_unstable();
        }
        Self {
            sketches,
            anchor_postings: MetadataSparseCandidatePostings::from_sorted_entries(anchor_entries),
            simhash_band_postings: MetadataSparseCandidatePostings::from_bounded_unsorted_entries(
                band_entries,
                METADATA_CONSERVATIVE_SIMHASH_BANDS << METADATA_CONSERVATIVE_SIMHASH_BAND_BITS,
            ),
        }
    }

    pub(super) fn from_content_docs(
        docs: &[CompactMetadataContentDocument],
        atoms: &[MetadataContentAtom],
        parallel: bool,
    ) -> Self {
        Self::from_token_visitor(atoms.len(), parallel, |atom_index, visitor| {
            let record_index =
                metadata_doc_index_to_usize(atoms[atom_index].representative_record_index);
            for &(token, _) in &docs[record_index].terms {
                visitor(token);
            }
        })
    }

    fn from_template_docs(
        scoring: &CompactMetadataScoring,
        atoms: &[MetadataContentAtom],
        parallel: bool,
    ) -> Self {
        Self::from_token_visitor(atoms.len(), parallel, |atom_index, visitor| {
            let template_index = metadata_doc_index_to_usize(atoms[atom_index].template_doc_index);
            for &token in scoring.query_tokens(template_index) {
                visitor(token);
            }
        })
    }

    pub(super) fn append_candidates_after(
        &self,
        atom_index: usize,
        scratch: &mut MetadataCandidateScratch,
    ) {
        let compact_atom_index = metadata_doc_index_from_usize(atom_index);
        let sketch = &self.sketches[atom_index];
        for &anchor in &sketch.anchors[..usize::from(sketch.anchor_len)] {
            let range = self
                .anchor_postings
                .posting_range_after(anchor, compact_atom_index);
            for &right in &self.anchor_postings.posting_atoms[range.start..range.end] {
                scratch.push_once(right);
            }
        }
        if sketch.has_terms {
            for band_index in 0..METADATA_CONSERVATIVE_SIMHASH_BANDS {
                let key = metadata_recall_simhash_band_key(sketch.simhash, band_index);
                let range = self
                    .simhash_band_postings
                    .posting_range_after(key, compact_atom_index);
                for &right in &self.simhash_band_postings.posting_atoms[range.start..range.end] {
                    scratch.push_once(right);
                }
            }
        }
    }

    pub(super) fn matches(&self, left: usize, right: usize) -> bool {
        let left = &self.sketches[left];
        let right = &self.sketches[right];
        if !left.has_terms || !right.has_terms {
            return false;
        }
        let shared_anchor = lowest_common_metadata_token(
            &left.anchors[..usize::from(left.anchor_len)],
            &right.anchors[..usize::from(right.anchor_len)],
        )
        .is_some();
        shared_anchor
            || (left.simhash ^ right.simhash).count_ones()
                <= METADATA_CONSERVATIVE_SIMHASH_HAMMING_THRESHOLD
    }

    #[cfg(test)]
    pub(super) fn memory_bytes(&self) -> usize {
        self.sketches
            .capacity()
            .saturating_mul(std::mem::size_of::<MetadataConservativeSketch>())
            .saturating_add(
                self.anchor_postings
                    .token_ids
                    .capacity()
                    .saturating_mul(std::mem::size_of::<u32>()),
            )
            .saturating_add(
                self.anchor_postings
                    .posting_offsets
                    .capacity()
                    .saturating_mul(std::mem::size_of::<u64>()),
            )
            .saturating_add(
                self.anchor_postings
                    .posting_atoms
                    .capacity()
                    .saturating_mul(std::mem::size_of::<MetadataDocIndex>()),
            )
            .saturating_add(
                self.simhash_band_postings
                    .token_ids
                    .capacity()
                    .saturating_mul(std::mem::size_of::<u32>()),
            )
            .saturating_add(
                self.simhash_band_postings
                    .posting_offsets
                    .capacity()
                    .saturating_mul(std::mem::size_of::<u64>()),
            )
            .saturating_add(
                self.simhash_band_postings
                    .posting_atoms
                    .capacity()
                    .saturating_mul(std::mem::size_of::<MetadataDocIndex>()),
            )
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
            full: MetadataSparseCandidatePostings::from_sorted_entries(full_entries),
            prefix: MetadataSparseCandidatePostings::from_sorted_entries(prefix_entries),
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
            full: MetadataSparseCandidatePostings::from_sorted_entries(full_entries),
            prefix: MetadataSparseCandidatePostings::from_sorted_entries(prefix_entries),
        }
    }

    #[cfg(test)]
    pub(super) fn append_candidates_after(
        &self,
        atom_index: usize,
        atom: &MetadataContentAtom,
        scoring: &CompactMetadataScoring,
        scratch: &mut MetadataCandidateScratch,
    ) {
        let mut plan = MetadataCandidatePostingPlan::default();
        self.plan_candidates_after(atom_index, atom, scoring, &mut plan);
        self.append_planned_candidates(&plan, scratch);
    }

    fn plan_candidates_after(
        &self,
        atom_index: usize,
        atom: &MetadataContentAtom,
        scoring: &CompactMetadataScoring,
        plan: &mut MetadataCandidatePostingPlan,
    ) -> usize {
        let compact_atom_index = metadata_doc_index_from_usize(atom_index);
        let template = metadata_doc_index_to_usize(atom.template_doc_index);
        for &token in scoring.candidate_tokens(template) {
            plan.template_full
                .push(self.full.posting_range_after(token, compact_atom_index));
        }
        for &token in scoring.query_tokens(template) {
            plan.template_prefix
                .push(self.prefix.posting_range_after(token, compact_atom_index));
        }
        plan.template_full
            .iter()
            .chain(&plan.template_prefix)
            .fold(0usize, |cost, range| {
                cost.saturating_add(range.end.saturating_sub(range.start))
            })
    }

    fn append_planned_candidates(
        &self,
        plan: &MetadataCandidatePostingPlan,
        scratch: &mut MetadataCandidateScratch,
    ) {
        self.full
            .append_planned_candidates(&plan.template_full, scratch);
        self.prefix
            .append_planned_candidates(&plan.template_prefix, scratch);
    }
}

impl MetadataLocalCandidateIndex {
    pub(super) fn from_atoms(
        docs: &[CompactMetadataContentDocument],
        atoms: &[MetadataContentAtom],
        compatibility: MetadataTemplateCompatibility<'_>,
        parallel: bool,
    ) -> Self {
        Self::from_atoms_with_mode(
            docs,
            atoms,
            compatibility,
            parallel,
            MetadataRecallMode::Exact,
        )
    }

    pub(super) fn from_atoms_with_mode(
        docs: &[CompactMetadataContentDocument],
        atoms: &[MetadataContentAtom],
        compatibility: MetadataTemplateCompatibility<'_>,
        parallel: bool,
        recall_mode: MetadataRecallMode,
    ) -> Self {
        match compatibility {
            MetadataTemplateCompatibility::Scored(scoring) => {
                if recall_mode == MetadataRecallMode::Conservative {
                    let ((exact_template, template), (exact_content, content)) = if parallel {
                        rayon::join(
                            || {
                                (
                                    MetadataTemplateCandidateIndex::from_atoms_parallel(
                                        scoring, atoms,
                                    ),
                                    MetadataConservativeDimensionIndex::from_template_docs(
                                        scoring, atoms, true,
                                    ),
                                )
                            },
                            || {
                                (
                                    MetadataContentCandidateIndex::from_atoms_parallel(docs, atoms),
                                    MetadataConservativeDimensionIndex::from_content_docs(
                                        docs, atoms, true,
                                    ),
                                )
                            },
                        )
                    } else {
                        (
                            (
                                MetadataTemplateCandidateIndex::from_atoms(scoring, atoms),
                                MetadataConservativeDimensionIndex::from_template_docs(
                                    scoring, atoms, false,
                                ),
                            ),
                            (
                                MetadataContentCandidateIndex::from_atoms(docs, atoms),
                                MetadataConservativeDimensionIndex::from_content_docs(
                                    docs, atoms, false,
                                ),
                            ),
                        )
                    };
                    return Self::Conservative(Box::new(MetadataConservativeCandidateIndex {
                        exact_template: Some(exact_template),
                        exact_content: Some(exact_content),
                        template,
                        content,
                    }));
                }
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
            Self::Conservative(index) => {
                index.template.append_candidates_after(atom_index, scratch);
                scratch.prepare_secondary_generation();
                index.content.append_candidates_after(atom_index, scratch);
                scratch.raw_candidate_count = scratch.secondary_candidates.len();
                scratch.retain_secondary_intersection();
                scratch.candidates.retain(|&right| {
                    let right = metadata_doc_index_to_usize(right);
                    index.template.matches(atom_index, right)
                        && index.content.matches(atom_index, right)
                });
                MetadataLocalCandidateBasis::ConservativeIntersection
            }
            Self::Adaptive { template, content } => Self::append_exact_index_candidates_after(
                template,
                content,
                atom_index,
                atom,
                document,
                compatibility,
                scratch,
            ),
            #[cfg(test)]
            Self::ContentOnly(index) => {
                index.append_candidates_after(atom_index, document, scratch);
                scratch.raw_candidate_count = scratch.candidates.len();
                MetadataLocalCandidateBasis::Content
            }
        }
    }

    fn append_exact_index_candidates_after(
        template: &MetadataTemplateCandidateIndex,
        content: &MetadataContentCandidateIndex,
        atom_index: usize,
        atom: &MetadataContentAtom,
        document: &CompactMetadataContentDocument,
        compatibility: MetadataTemplateCompatibility<'_>,
        scratch: &mut MetadataCandidateScratch,
    ) -> MetadataLocalCandidateBasis {
        let scoring = compatibility
            .scoring()
            .expect("template candidate index requires scored compatibility");
        let mut posting_plan = std::mem::take(&mut scratch.posting_plan);
        posting_plan.clear();
        let template_cost =
            template.plan_candidates_after(atom_index, atom, scoring, &mut posting_plan);
        let content_cost = content.plan_candidates_after(atom_index, document, &mut posting_plan);
        let minimum_cost = template_cost.min(content_cost);
        let maximum_cost = template_cost.max(content_cost);
        let basis = if minimum_cost >= METADATA_DENSE_INTERSECTION_MIN_SCAN_COST
            && maximum_cost
                <= minimum_cost.saturating_mul(METADATA_DENSE_INTERSECTION_MAX_COST_RATIO)
        {
            if content_cost < template_cost {
                template.append_planned_candidates(&posting_plan, scratch);
                scratch.prepare_secondary_generation();
                content.append_planned_candidates(&posting_plan, scratch);
            } else {
                content.append_planned_candidates(&posting_plan, scratch);
                scratch.prepare_secondary_generation();
                template.append_planned_candidates(&posting_plan, scratch);
            }
            scratch.raw_candidate_count = scratch.candidates.len();
            scratch.retain_secondary_intersection();
            MetadataLocalCandidateBasis::Intersection
        } else if content_cost < template_cost {
            content.append_planned_candidates(&posting_plan, scratch);
            scratch.raw_candidate_count = scratch.candidates.len();
            MetadataLocalCandidateBasis::Content
        } else {
            template.append_planned_candidates(&posting_plan, scratch);
            scratch.raw_candidate_count = scratch.candidates.len();
            MetadataLocalCandidateBasis::Template
        };
        scratch.posting_plan = posting_plan;
        basis
    }

    fn append_exact_candidates_after(
        &self,
        atom_index: usize,
        atom: &MetadataContentAtom,
        document: &CompactMetadataContentDocument,
        compatibility: MetadataTemplateCompatibility<'_>,
        scratch: &mut MetadataCandidateScratch,
    ) -> MetadataLocalCandidateBasis {
        match self {
            Self::Conservative(index) => Self::append_exact_index_candidates_after(
                index
                    .exact_template
                    .as_ref()
                    .expect("exact metadata calibration index already released"),
                index
                    .exact_content
                    .as_ref()
                    .expect("exact metadata calibration index already released"),
                atom_index,
                atom,
                document,
                compatibility,
                scratch,
            ),
            _ => self.append_candidates_after(atom_index, atom, document, compatibility, scratch),
        }
    }

    fn into_effective_recall(self, exact_recall: bool) -> Self {
        match self {
            Self::Conservative(mut index) if exact_recall => Self::Adaptive {
                template: index
                    .exact_template
                    .take()
                    .expect("exact metadata calibration index already released"),
                content: index
                    .exact_content
                    .take()
                    .expect("exact metadata calibration index already released"),
            },
            Self::Conservative(mut index) => {
                index.exact_template = None;
                index.exact_content = None;
                Self::Conservative(index)
            }
            index => index,
        }
    }
}

impl MetadataCandidateScratch {
    pub(super) fn new(doc_count: usize) -> Self {
        Self {
            seen_generation: vec![0; doc_count],
            generation: 0,
            candidates: Vec::new(),
            secondary_seen_generation: vec![0; doc_count],
            secondary_generation: 0,
            secondary_candidates: Vec::new(),
            posting_plan: MetadataCandidatePostingPlan::default(),
            raw_candidate_count: 0,
        }
    }

    pub(super) fn clear_for_next_left(&mut self) {
        self.candidates.clear();
        self.raw_candidate_count = 0;
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

    fn prepare_secondary_generation(&mut self) {
        std::mem::swap(
            &mut self.seen_generation,
            &mut self.secondary_seen_generation,
        );
        std::mem::swap(&mut self.generation, &mut self.secondary_generation);
        std::mem::swap(&mut self.candidates, &mut self.secondary_candidates);
        self.clear_for_next_left();
    }

    fn retain_secondary_intersection(&mut self) {
        let secondary_generation = self.secondary_generation;
        let secondary_seen_generation = &self.secondary_seen_generation;
        self.candidates.retain(|&index| {
            secondary_seen_generation[metadata_doc_index_to_usize(index)] == secondary_generation
        });
    }
}

impl MetadataCandidateScratchPool {
    pub(super) fn new(doc_count: usize) -> Self {
        Self {
            doc_count,
            scratches: Mutex::new(Vec::new()),
        }
    }

    pub(super) fn take(&self) -> MetadataCandidateScratchLease<'_> {
        let scratch = {
            self.scratches
                .lock()
                .expect("metadata candidate scratch pool lock poisoned")
                .pop()
        };
        let scratch = scratch.unwrap_or_else(|| MetadataCandidateScratch::new(self.doc_count));
        MetadataCandidateScratchLease {
            pool: self,
            scratch: Some(scratch),
        }
    }
}

impl std::ops::Deref for MetadataCandidateScratchLease<'_> {
    type Target = MetadataCandidateScratch;

    fn deref(&self) -> &Self::Target {
        self.scratch
            .as_ref()
            .expect("metadata candidate scratch lease is empty")
    }
}

impl std::ops::DerefMut for MetadataCandidateScratchLease<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.scratch
            .as_mut()
            .expect("metadata candidate scratch lease is empty")
    }
}

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
    recall_mode: MetadataRecallMode,
    progress: &ProgressTracker,
) -> Result<MetadataContentUnionStats, AnalysisError> {
    let mut stmt = conn.prepare(metadata_token_content_rows_sql())?;
    let template_cache_pool = MetadataTemplateScoreCachePool::default();
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
    let mut completed_groups = 0u64;
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
                completed_groups = completed_groups.saturating_add(1);
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
                        &template_cache_pool,
                        recall_mode,
                    )?);
                    pending_prepare_bytes = 0;
                    let group_progress = MetadataSharedTokenGroupProgress {
                        tracker: progress,
                        completed_groups,
                        base: ProgressCounters {
                            groups: completed_groups,
                            candidates: stats.candidate_pairs,
                            scored: stats.scored_pairs,
                            matched: stats.matched_pairs,
                        },
                    };
                    stats.accumulate(completed.union_with_budget(
                        context,
                        state,
                        maximum_working_bytes,
                        &template_cache_pool,
                        recall_mode,
                        Some(group_progress),
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
                            &template_cache_pool,
                            recall_mode,
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
                    &template_cache_pool,
                    recall_mode,
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
        progress.advance_task(
            batch.num_rows() as u64,
            ProgressCounters {
                groups: completed_groups,
                candidates: stats.candidate_pairs,
                scored: stats.scored_pairs,
                matched: stats.matched_pairs,
            },
        );
    }
    if current_token.is_some() {
        completed_groups = completed_groups.saturating_add(1);
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
                &template_cache_pool,
                recall_mode,
            )?);
            let group_progress = MetadataSharedTokenGroupProgress {
                tracker: progress,
                completed_groups,
                base: ProgressCounters {
                    groups: completed_groups,
                    candidates: stats.candidate_pairs,
                    scored: stats.scored_pairs,
                    matched: stats.matched_pairs,
                },
            };
            stats.accumulate(group.union_with_budget(
                context,
                state,
                maximum_working_bytes,
                &template_cache_pool,
                recall_mode,
                Some(group_progress),
            )?);
        }
    }
    stats.accumulate(prepare_metadata_token_group_batch(
        &mut pending_groups,
        context,
        state,
        maximum_working_bytes,
        &template_cache_pool,
        recall_mode,
    )?);
    progress.advance_task(
        0,
        ProgressCounters {
            groups: completed_groups,
            candidates: stats.candidate_pairs,
            scored: stats.scored_pairs,
            matched: stats.matched_pairs,
        },
    );
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
        ORDER BY count(*) OVER (PARTITION BY t.token_index),
                 t.token_index,
                 t.contract_index,
                 t.metadata_source_file,
                 t.metadata_source_row_number
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
    progress: &ProgressTracker,
) -> Result<MetadataContentUnionStats, AnalysisError> {
    progress.start_task(
        "building representative fallback atoms",
        Some(context.data.contracts.len() as u64),
        "contracts",
    );
    let mut builder = CompactMetadataContentGroupBuilder::default();
    let mut pending_progress = 0u64;
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
                context.recall_mode,
            )?;
        }
        pending_progress = pending_progress.saturating_add(1);
        if pending_progress >= 4_096 {
            progress.advance_task(pending_progress, ProgressCounters::default());
            pending_progress = 0;
        }
    }
    progress.advance_task(pending_progress, ProgressCounters::default());
    progress.finish_task("representative fallback atoms ready");
    builder.ensure_within_memory_budget(
        0,
        maximum_working_bytes,
        context.pool.current_num_threads(),
        context.recall_mode,
    )?;
    let (atoms, docs) = builder.into_atomized_parts();
    progress.start_task(
        "scoring representative fallback left atoms",
        Some(atoms.len().saturating_sub(1) as u64),
        "atoms",
    );
    let stats = union_metadata_no_common_atom_core(atoms, &docs, context, state, Some(progress));
    progress.finish_task(format!(
        "representative fallback complete; candidates {}; scored {}; matched {}",
        stats.candidate_pairs, stats.scored_pairs, stats.matched_pairs
    ));
    Ok(stats)
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

pub(super) fn metadata_content_atoms_share_token(
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

pub(super) fn metadata_template_atoms_share_safe_prefix(
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

pub(super) fn metadata_candidate_intersects_both_dimensions(
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
        MetadataLocalCandidateBasis::Intersection => true,
        MetadataLocalCandidateBasis::ConservativeIntersection => {
            metadata_content_atoms_share_token(left, right, atoms, compact_docs)
                && metadata_template_atoms_share_safe_prefix(left, right, atoms, compatibility)
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
    pub(super) template_candidate_pairs: u64,
    pub(super) template_scored_pairs: u64,
    pub(super) template_matched_pairs: u64,
    pub(super) content_scored_pairs: u64,
    pub(super) content_matched_pairs: u64,
    pub(super) template_cache_hits: u64,
    pub(super) template_cache_misses: u64,
    pub(super) template_rejected_pairs: u64,
    pub(super) template_batch_unique_pairs: u64,
    pub(super) template_batch_reused_pairs: u64,
}

#[derive(Clone, Copy)]
struct MetadataTemplatePairEvaluation {
    matched: bool,
    score_count: u64,
    cache_hit: bool,
}

fn metadata_template_pair_key(left: MetadataDocIndex, right: MetadataDocIndex) -> u64 {
    let (left, right) = if left < right {
        (left, right)
    } else {
        (right, left)
    };
    (u64::from(left) << 32) | u64::from(right)
}

fn should_compact_metadata_template_pairs(
    candidate_pairs: &[(usize, MetadataDocIndex)],
    atoms: &[MetadataContentAtom],
) -> bool {
    if candidate_pairs.len() < METADATA_CONTENT_PARALLEL_MIN_RECORDS {
        return false;
    }
    let sample_len = candidate_pairs
        .len()
        .min(METADATA_TEMPLATE_COMPACTION_SAMPLE_SIZE);
    let mut sample_keys = candidate_pairs[..sample_len]
        .iter()
        .map(|&(left, right)| {
            metadata_template_pair_key(
                atoms[left].template_doc_index,
                atoms[metadata_doc_index_to_usize(right)].template_doc_index,
            )
        })
        .collect::<Vec<_>>();
    sample_keys.sort_unstable();
    let duplicate_count = sample_keys
        .windows(2)
        .filter(|keys| keys[0] == keys[1])
        .count();
    duplicate_count.saturating_mul(METADATA_TEMPLATE_COMPACTION_MIN_DUPLICATE_DENOMINATOR)
        >= sample_len
}

fn collect_metadata_template_pair_evaluations(
    candidate_pairs: &[(usize, MetadataDocIndex)],
    atoms: &[MetadataContentAtom],
    compatibility: MetadataTemplateCompatibility<'_>,
    pool: &rayon::ThreadPool,
    template_cache_pool: &MetadataTemplateScoreCachePool,
) -> (
    Vec<MetadataTemplatePairEvaluation>,
    MetadataPairScoringStats,
) {
    if candidate_pairs.is_empty() {
        return (Vec::new(), MetadataPairScoringStats::default());
    }
    let mut pair_order = candidate_pairs
        .iter()
        .enumerate()
        .map(|(pair_index, &(left, right))| {
            let left_template = atoms[left].template_doc_index;
            let right_template = atoms[metadata_doc_index_to_usize(right)].template_doc_index;
            (
                metadata_template_pair_key(left_template, right_template),
                pair_index,
            )
        })
        .collect::<Vec<_>>();
    if pair_order.len() >= METADATA_CONTENT_PARALLEL_MIN_RECORDS {
        pool.install(|| pair_order.par_sort_unstable_by_key(|&(key, _)| key));
    } else {
        pair_order.sort_unstable_by_key(|&(key, _)| key);
    }
    let mut unique_keys = Vec::with_capacity(pair_order.len());
    for &(key, _) in &pair_order {
        if unique_keys.last().copied() != Some(key) {
            unique_keys.push(key);
        }
    }
    let evaluate_key = |key: u64, cache: &mut MetadataTemplateScoreCache| {
        let left = (key >> 32) as MetadataDocIndex;
        let right = key as MetadataDocIndex;
        let (matched, score_count, cache_hit) = cache.evaluate(left, right, compatibility);
        MetadataTemplatePairEvaluation {
            matched,
            score_count,
            cache_hit,
        }
    };
    let unique_evaluations = if unique_keys.len() >= METADATA_CONTENT_PARALLEL_MIN_RECORDS {
        pool.install(|| {
            unique_keys
                .par_iter()
                .map_init(
                    || template_cache_pool.take(),
                    |cache, &key| evaluate_key(key, cache),
                )
                .collect::<Vec<_>>()
        })
    } else {
        let mut cache = template_cache_pool.take();
        unique_keys
            .iter()
            .map(|&key| evaluate_key(key, &mut cache))
            .collect()
    };
    let mut pair_evaluations = vec![
        MetadataTemplatePairEvaluation {
            matched: false,
            score_count: 0,
            cache_hit: false,
        };
        candidate_pairs.len()
    ];
    let mut unique_index = 0usize;
    for &(key, pair_index) in &pair_order {
        while unique_keys[unique_index] != key {
            unique_index += 1;
        }
        pair_evaluations[pair_index] = unique_evaluations[unique_index];
    }
    let cache_hits = unique_evaluations
        .iter()
        .filter(|evaluation| evaluation.cache_hit)
        .count() as u64;
    let cache_misses = unique_evaluations
        .iter()
        .filter(|evaluation| !evaluation.cache_hit && evaluation.score_count > 0)
        .count() as u64;
    (
        pair_evaluations,
        MetadataPairScoringStats {
            template_batch_unique_pairs: unique_keys.len() as u64,
            template_batch_reused_pairs: candidate_pairs.len().saturating_sub(unique_keys.len())
                as u64,
            template_cache_hits: cache_hits,
            template_cache_misses: cache_misses,
            ..MetadataPairScoringStats::default()
        },
    )
}

#[derive(Default)]
pub(super) struct MetadataValidatedPairBatch {
    pub(super) hits: Vec<(usize, MetadataDocIndex)>,
    pub(super) stats: MetadataPairScoringStats,
}

impl MetadataValidatedPairBatch {
    fn score_pair_with_cache(
        &mut self,
        pair: (usize, MetadataDocIndex),
        atoms: &[MetadataContentAtom],
        compact_docs: &[CompactMetadataContentDocument],
        compatibility: MetadataTemplateCompatibility<'_>,
        cache: &mut MetadataTemplateScoreCache,
    ) {
        let left_template = atoms[pair.0].template_doc_index;
        let right_template = atoms[metadata_doc_index_to_usize(pair.1)].template_doc_index;
        let (matched, score_count, cache_hit) =
            cache.evaluate(left_template, right_template, compatibility);
        if cache_hit {
            self.stats.template_cache_hits = self.stats.template_cache_hits.saturating_add(1);
        } else if score_count > 0 {
            self.stats.template_cache_misses = self.stats.template_cache_misses.saturating_add(1);
        }
        self.score_pair(
            pair,
            MetadataTemplatePairEvaluation {
                matched,
                score_count,
                cache_hit,
            },
            atoms,
            compact_docs,
        );
    }

    fn score_pair(
        &mut self,
        pair: (usize, MetadataDocIndex),
        template_evaluation: MetadataTemplatePairEvaluation,
        atoms: &[MetadataContentAtom],
        compact_docs: &[CompactMetadataContentDocument],
    ) {
        self.stats.template_candidate_pairs = self.stats.template_candidate_pairs.saturating_add(1);
        self.stats.template_scored_pairs = self
            .stats
            .template_scored_pairs
            .saturating_add(template_evaluation.score_count);
        if !template_evaluation.matched {
            self.stats.template_rejected_pairs =
                self.stats.template_rejected_pairs.saturating_add(1);
            return;
        }
        self.stats.template_matched_pairs = self.stats.template_matched_pairs.saturating_add(1);
        self.stats.content_scored_pairs = self.stats.content_scored_pairs.saturating_add(1);
        if metadata_content_atom_pair_matches(pair, atoms, compact_docs) {
            self.stats.content_matched_pairs = self.stats.content_matched_pairs.saturating_add(1);
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
        self.stats.content_matched_pairs = self
            .stats
            .content_matched_pairs
            .saturating_add(other.stats.content_matched_pairs);
        self.stats.template_cache_hits = self
            .stats
            .template_cache_hits
            .saturating_add(other.stats.template_cache_hits);
        self.stats.template_cache_misses = self
            .stats
            .template_cache_misses
            .saturating_add(other.stats.template_cache_misses);
        self.stats.template_rejected_pairs = self
            .stats
            .template_rejected_pairs
            .saturating_add(other.stats.template_rejected_pairs);
        self.stats.template_batch_unique_pairs = self
            .stats
            .template_batch_unique_pairs
            .saturating_add(other.stats.template_batch_unique_pairs);
        self.stats.template_batch_reused_pairs = self
            .stats
            .template_batch_reused_pairs
            .saturating_add(other.stats.template_batch_reused_pairs);
        self
    }
}

pub(super) fn collect_metadata_validated_atom_pair_hits(
    candidate_pairs: &[(usize, MetadataDocIndex)],
    atoms: &[MetadataContentAtom],
    compact_docs: &[CompactMetadataContentDocument],
    template_compatibility: MetadataTemplateCompatibility<'_>,
    pool: &rayon::ThreadPool,
    template_cache_pool: &MetadataTemplateScoreCachePool,
) -> MetadataValidatedPairBatch {
    if !should_compact_metadata_template_pairs(candidate_pairs, atoms) {
        if candidate_pairs.len() >= METADATA_CONTENT_PARALLEL_MIN_RECORDS {
            return pool.install(|| {
                candidate_pairs
                    .par_chunks(METADATA_CONTENT_PARALLEL_MIN_RECORDS)
                    .map_init(
                        || template_cache_pool.take(),
                        |cache, pairs| {
                            let mut batch = MetadataValidatedPairBatch::default();
                            for &pair in pairs {
                                batch.score_pair_with_cache(
                                    pair,
                                    atoms,
                                    compact_docs,
                                    template_compatibility,
                                    cache,
                                );
                            }
                            batch
                        },
                    )
                    .reduce(
                        MetadataValidatedPairBatch::default,
                        MetadataValidatedPairBatch::merge,
                    )
            });
        }
        let mut cache = template_cache_pool.take();
        let mut batch = MetadataValidatedPairBatch::default();
        for &pair in candidate_pairs {
            batch.score_pair_with_cache(
                pair,
                atoms,
                compact_docs,
                template_compatibility,
                &mut cache,
            );
        }
        return batch;
    }
    let (template_evaluations, template_stats) = collect_metadata_template_pair_evaluations(
        candidate_pairs,
        atoms,
        template_compatibility,
        pool,
        template_cache_pool,
    );
    let mut batch = if candidate_pairs.len() >= METADATA_CONTENT_PARALLEL_MIN_RECORDS {
        pool.install(|| {
            candidate_pairs
                .par_chunks(METADATA_CONTENT_PARALLEL_MIN_RECORDS)
                .zip(template_evaluations.par_chunks(METADATA_CONTENT_PARALLEL_MIN_RECORDS))
                .map(|(pairs, evaluations)| {
                    let mut batch = MetadataValidatedPairBatch::default();
                    for (&pair, &evaluation) in pairs.iter().zip(evaluations) {
                        batch.score_pair(pair, evaluation, atoms, compact_docs);
                    }
                    batch
                })
                .reduce(
                    MetadataValidatedPairBatch::default,
                    MetadataValidatedPairBatch::merge,
                )
        })
    } else {
        let mut batch = MetadataValidatedPairBatch::default();
        for (&pair, &evaluation) in candidate_pairs.iter().zip(&template_evaluations) {
            batch.score_pair(pair, evaluation, atoms, compact_docs);
        }
        batch
    };
    batch.stats.template_batch_unique_pairs = batch
        .stats
        .template_batch_unique_pairs
        .saturating_add(template_stats.template_batch_unique_pairs);
    batch.stats.template_batch_reused_pairs = batch
        .stats
        .template_batch_reused_pairs
        .saturating_add(template_stats.template_batch_reused_pairs);
    batch.stats.template_cache_hits = batch
        .stats
        .template_cache_hits
        .saturating_add(template_stats.template_cache_hits);
    batch.stats.template_cache_misses = batch
        .stats
        .template_cache_misses
        .saturating_add(template_stats.template_cache_misses);
    batch
}

pub(super) fn score_and_apply_metadata_atom_pair_batch(
    candidate_pairs: &mut Vec<(usize, MetadataDocIndex)>,
    atoms: &[MetadataContentAtom],
    compact_docs: &[CompactMetadataContentDocument],
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
    template_cache_pool: &MetadataTemplateScoreCachePool,
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
        template_cache_pool,
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
    template_cache_pool: &MetadataTemplateScoreCachePool,
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
        template_cache_pool,
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
    union_metadata_shared_token_atoms_with_mode(
        records,
        compact_docs,
        context,
        state,
        MetadataRecallMode::Exact,
    )
}

#[cfg(test)]
pub(super) fn union_metadata_shared_token_atoms_with_mode(
    records: &[MetadataContentRecord],
    compact_docs: &[CompactMetadataContentDocument],
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
    recall_mode: MetadataRecallMode,
) -> MetadataContentUnionStats {
    let atoms = build_metadata_content_atoms(records, compact_docs, context.data);
    let template_cache_pool = MetadataTemplateScoreCachePool::default();
    union_metadata_shared_token_atom_core(
        atoms,
        compact_docs,
        context,
        state,
        &template_cache_pool,
        recall_mode,
        None,
    )
}

struct MetadataLeftCandidateBatch {
    left: usize,
    candidates: Vec<MetadataDocIndex>,
    raw_candidate_pairs: u64,
    dimension_rejected_pairs: u64,
}

fn collect_metadata_left_candidate_batch(
    left: usize,
    atoms: &[MetadataContentAtom],
    compact_docs: &[CompactMetadataContentDocument],
    candidate_index: &MetadataLocalCandidateIndex,
    compatibility: MetadataTemplateCompatibility<'_>,
    exact_recall: bool,
    scratch: &mut MetadataCandidateScratch,
) -> MetadataLeftCandidateBatch {
    let left_atom = &atoms[left];
    let left_record_index = metadata_doc_index_to_usize(left_atom.representative_record_index);
    scratch.clear_for_next_left();
    let candidate_basis = if exact_recall {
        candidate_index.append_exact_candidates_after(
            left,
            left_atom,
            &compact_docs[left_record_index],
            compatibility,
            scratch,
        )
    } else {
        candidate_index.append_candidates_after(
            left,
            left_atom,
            &compact_docs[left_record_index],
            compatibility,
            scratch,
        )
    };
    let raw_candidate_pairs = scratch.raw_candidate_count as u64;
    let candidates = scratch
        .candidates
        .iter()
        .copied()
        .filter(|&right| {
            metadata_candidate_intersects_both_dimensions(
                candidate_basis,
                left,
                right,
                atoms,
                compact_docs,
                compatibility,
            )
        })
        .collect::<Vec<_>>();
    let dimension_rejected_pairs = raw_candidate_pairs.saturating_sub(candidates.len() as u64);
    MetadataLeftCandidateBatch {
        left,
        candidates,
        raw_candidate_pairs,
        dimension_rejected_pairs,
    }
}

fn collect_metadata_left_candidate_wave(
    left_range: std::ops::Range<usize>,
    atoms: &[MetadataContentAtom],
    compact_docs: &[CompactMetadataContentDocument],
    candidate_index: &MetadataLocalCandidateIndex,
    compatibility: MetadataTemplateCompatibility<'_>,
    exact_recall: bool,
    scratch_pool: &MetadataCandidateScratchPool,
) -> Vec<MetadataLeftCandidateBatch> {
    left_range
        .into_par_iter()
        .map_init(
            || scratch_pool.take(),
            |scratch, left| {
                collect_metadata_left_candidate_batch(
                    left,
                    atoms,
                    compact_docs,
                    candidate_index,
                    compatibility,
                    exact_recall,
                    scratch,
                )
            },
        )
        .collect()
}

fn consume_metadata_left_candidate_wave(
    left_batches: Vec<MetadataLeftCandidateBatch>,
    mut consumer: MetadataLeftCandidateBatchConsumer<'_, '_>,
) {
    for left_batch in left_batches {
        consumer.apply(left_batch);
    }
}

struct MetadataLeftCandidateBatchConsumer<'a, 'context> {
    atoms: &'a [MetadataContentAtom],
    compact_docs: &'a [CompactMetadataContentDocument],
    context: &'a MetadataContentUnionContext<'context>,
    state: &'a mut MetadataUnionState,
    stats: &'a mut MetadataContentUnionStats,
    candidate_pairs: &'a mut Vec<(usize, MetadataDocIndex)>,
    template_cache_pool: &'a MetadataTemplateScoreCachePool,
}

impl MetadataLeftCandidateBatchConsumer<'_, '_> {
    fn apply(&mut self, left_batch: MetadataLeftCandidateBatch) {
        let left = left_batch.left;
        self.stats.raw_candidate_pairs = self
            .stats
            .raw_candidate_pairs
            .saturating_add(left_batch.raw_candidate_pairs);
        self.stats.dimension_rejected_pairs = self
            .stats
            .dimension_rejected_pairs
            .saturating_add(left_batch.dimension_rejected_pairs);
        let left_atom = &self.atoms[left];
        let left_contract_index = metadata_contract_index_to_usize(left_atom.members[0]);
        debug_assert_eq!(
            self.context.data.contracts[left_contract_index].chain_index,
            left_atom.chain_index
        );
        for right in left_batch.candidates {
            self.stats.candidate_pairs = self.stats.candidate_pairs.saturating_add(1);
            let right_atom = &self.atoms[metadata_doc_index_to_usize(right)];
            let right_contract_index = metadata_contract_index_to_usize(right_atom.members[0]);
            let singleton_pair = left_atom.members.len() == 1 && right_atom.members.len() == 1;
            let same_chain = left_atom.chain_index == right_atom.chain_index;
            if (singleton_pair || same_chain)
                && metadata_pair_already_connected(
                    self.context.data,
                    self.context.chain_count,
                    self.state,
                    left_contract_index,
                    right_contract_index,
                )
            {
                self.stats.already_connected_pairs =
                    self.stats.already_connected_pairs.saturating_add(1);
                continue;
            }
            self.candidate_pairs.push((left, right));
            if self.candidate_pairs.len() >= METADATA_CONTENT_SCORE_BATCH_PAIRS {
                let batch_stats = score_and_apply_metadata_atom_pair_batch(
                    self.candidate_pairs,
                    self.atoms,
                    self.compact_docs,
                    self.context,
                    self.state,
                    self.template_cache_pool,
                );
                self.stats.accumulate_pair_scoring(batch_stats);
            }
        }
    }
}

fn metadata_conservative_calibration_lefts(atoms: &[MetadataContentAtom]) -> Vec<usize> {
    let left_count = atoms.len().saturating_sub(1);
    if left_count == 0 {
        return Vec::new();
    }
    let seed = atoms
        .first()
        .and_then(|atom| atom.members.first())
        .copied()
        .map(stable_metadata_recall_token_hash)
        .unwrap_or(0);
    let folded_seed = seed as u32 ^ (seed >> 32) as u32;
    let mut sampled = (0..left_count)
        .filter(|&left| {
            let contract = atoms[left].members.first().copied().unwrap_or_default();
            stable_metadata_recall_token_hash(contract ^ folded_seed)
                .is_multiple_of(METADATA_CONSERVATIVE_CALIBRATION_DIVISOR)
        })
        .collect::<Vec<_>>();
    if sampled.is_empty() {
        sampled.push(seed as usize % left_count);
    }
    sampled
}

fn for_each_metadata_calibration_hit(
    left: usize,
    candidates: &[MetadataDocIndex],
    atoms: &[MetadataContentAtom],
    compact_docs: &[CompactMetadataContentDocument],
    context: &MetadataContentUnionContext<'_>,
    template_cache_pool: &MetadataTemplateScoreCachePool,
    mut on_hit: impl FnMut(MetadataDocIndex),
) {
    let mut pairs = Vec::with_capacity(METADATA_CONTENT_SCORE_BATCH_PAIRS);
    for chunk in candidates.chunks(METADATA_CONTENT_SCORE_BATCH_PAIRS) {
        pairs.clear();
        pairs.extend(chunk.iter().copied().map(|right| (left, right)));
        let batch = collect_metadata_validated_atom_pair_hits(
            &pairs,
            atoms,
            compact_docs,
            context.template_compatibility,
            context.pool,
            template_cache_pool,
        );
        for (_, right) in batch.hits {
            on_hit(right);
        }
    }
}

fn calibrate_metadata_conservative_recall(
    atoms: &[MetadataContentAtom],
    compact_docs: &[CompactMetadataContentDocument],
    candidate_index: &MetadataLocalCandidateIndex,
    context: &MetadataContentUnionContext<'_>,
    template_cache_pool: &MetadataTemplateScoreCachePool,
    progress: Option<MetadataSharedTokenGroupProgress<'_>>,
) -> MetadataRecallCalibrationStats {
    let lefts = metadata_conservative_calibration_lefts(atoms);
    let total_lefts = lefts.len();
    let mut calibration = MetadataRecallCalibrationStats {
        sampled_left_atoms: lefts.len() as u64,
        ..MetadataRecallCalibrationStats::default()
    };
    let mut scratch = MetadataCandidateScratch::new(atoms.len());
    let mut exact_duplicate_atoms = vec![false; atoms.len()];
    let mut conservative_duplicate_atoms = vec![false; atoms.len()];
    let mut conservative_hit_generations = vec![0u16; atoms.len()];
    let mut conservative_hit_generation = 0u16;
    let mut exact_components = UnionFind::new(atoms.len());
    let mut conservative_components = UnionFind::new(atoms.len());
    for (sample_index, left) in lefts.into_iter().enumerate() {
        conservative_hit_generation = conservative_hit_generation.wrapping_add(1);
        if conservative_hit_generation == 0 {
            conservative_hit_generations.fill(0);
            conservative_hit_generation = 1;
        }
        let conservative_batch = collect_metadata_left_candidate_batch(
            left,
            atoms,
            compact_docs,
            candidate_index,
            context.template_compatibility,
            false,
            &mut scratch,
        );
        calibration.conservative_candidate_pairs = calibration
            .conservative_candidate_pairs
            .saturating_add(conservative_batch.candidates.len() as u64);
        for_each_metadata_calibration_hit(
            left,
            &conservative_batch.candidates,
            atoms,
            compact_docs,
            context,
            template_cache_pool,
            |right| {
                let right = metadata_doc_index_to_usize(right);
                conservative_hit_generations[right] = conservative_hit_generation;
                conservative_duplicate_atoms[left] = true;
                conservative_duplicate_atoms[right] = true;
                conservative_components.union(left, right);
            },
        );
        drop(conservative_batch);

        let exact_batch = collect_metadata_left_candidate_batch(
            left,
            atoms,
            compact_docs,
            candidate_index,
            context.template_compatibility,
            true,
            &mut scratch,
        );
        calibration.exact_candidate_pairs = calibration
            .exact_candidate_pairs
            .saturating_add(exact_batch.candidates.len() as u64);
        for_each_metadata_calibration_hit(
            left,
            &exact_batch.candidates,
            atoms,
            compact_docs,
            context,
            template_cache_pool,
            |right| {
                calibration.exact_matched_pairs = calibration.exact_matched_pairs.saturating_add(1);
                if conservative_hit_generations[metadata_doc_index_to_usize(right)]
                    != conservative_hit_generation
                {
                    calibration.missed_matched_pairs =
                        calibration.missed_matched_pairs.saturating_add(1);
                }
                let right = metadata_doc_index_to_usize(right);
                exact_duplicate_atoms[left] = true;
                exact_duplicate_atoms[right] = true;
                exact_components.union(left, right);
            },
        );
        if let Some(progress) = progress {
            progress.update_calibration(sample_index.saturating_add(1), total_lefts, &calibration);
        }
    }

    let mut exact_component_weights = HashMap::<usize, u64>::new();
    let mut conservative_partition_weights = HashMap::<(usize, usize), u64>::new();
    let mut exact_duplicate_contract_members = 0u64;
    let mut missed_duplicate_contract_members = 0u64;
    for atom_index in 0..atoms.len() {
        if !exact_duplicate_atoms[atom_index] {
            continue;
        }
        let weight = atoms[atom_index].members.len() as u64;
        exact_duplicate_contract_members = exact_duplicate_contract_members.saturating_add(weight);
        if !conservative_duplicate_atoms[atom_index] {
            missed_duplicate_contract_members =
                missed_duplicate_contract_members.saturating_add(weight);
        }
        let exact_root = exact_components.find(atom_index);
        let conservative_root = conservative_components.find(atom_index);
        let exact_weight = exact_component_weights.entry(exact_root).or_default();
        *exact_weight = exact_weight.saturating_add(weight);
        let partition_weight = conservative_partition_weights
            .entry((exact_root, conservative_root))
            .or_default();
        *partition_weight = partition_weight.saturating_add(weight);
    }
    let mut largest_partition_by_exact_component = HashMap::<usize, u64>::new();
    for ((exact_root, _), weight) in conservative_partition_weights {
        let largest = largest_partition_by_exact_component
            .entry(exact_root)
            .or_default();
        *largest = (*largest).max(weight);
    }
    let exact_component_members = exact_component_weights
        .values()
        .copied()
        .fold(0u64, u64::saturating_add);
    let shifted_component_members = exact_component_weights
        .into_iter()
        .map(|(root, weight)| {
            weight.saturating_sub(
                largest_partition_by_exact_component
                    .get(&root)
                    .copied()
                    .unwrap_or(0),
            )
        })
        .fold(0u64, u64::saturating_add);
    calibration.exact_duplicate_contract_members = exact_duplicate_contract_members;
    calibration.missed_duplicate_contract_members = missed_duplicate_contract_members;
    calibration.exact_component_members = exact_component_members;
    calibration.shifted_component_members = shifted_component_members;
    calibration
}

fn union_metadata_shared_token_atom_core(
    atoms: Vec<MetadataContentAtom>,
    compact_docs: &[CompactMetadataContentDocument],
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
    template_cache_pool: &MetadataTemplateScoreCachePool,
    recall_mode: MetadataRecallMode,
    progress: Option<MetadataSharedTokenGroupProgress<'_>>,
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
        if let Some(progress) = progress {
            progress.update(&stats);
        }
        return stats;
    }
    if atoms.len() == METADATA_DIRECT_ATOM_GROUP_SIZE {
        let left = 0usize;
        let right = metadata_doc_index_from_usize(1);
        stats.raw_candidate_pairs = 1;
        if !metadata_content_atoms_share_token(left, right, &atoms, compact_docs)
            || !metadata_template_atoms_share_safe_prefix(
                left,
                right,
                &atoms,
                context.template_compatibility,
            )
        {
            stats.dimension_rejected_pairs = 1;
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
            stats.already_connected_pairs = 1;
            return stats;
        }
        let mut candidate_pairs = vec![(left, right)];
        let pair_stats = score_and_apply_metadata_atom_pair_batch(
            &mut candidate_pairs,
            &atoms,
            compact_docs,
            context,
            state,
            template_cache_pool,
        );
        stats.accumulate_pair_scoring(pair_stats);
        if let Some(progress) = progress {
            progress.update(&stats);
        }
        return stats;
    }
    let parallel = atoms.len() >= METADATA_CONTENT_PARALLEL_MIN_RECORDS;
    let conservative_group = recall_mode == MetadataRecallMode::Conservative
        && atoms.len() >= METADATA_CONSERVATIVE_MIN_ATOMS;
    let index_recall_mode = if conservative_group {
        MetadataRecallMode::Conservative
    } else {
        MetadataRecallMode::Exact
    };
    let candidate_index = if parallel {
        context.pool.install(|| {
            MetadataLocalCandidateIndex::from_atoms_with_mode(
                compact_docs,
                &atoms,
                context.template_compatibility,
                true,
                index_recall_mode,
            )
        })
    } else {
        MetadataLocalCandidateIndex::from_atoms_with_mode(
            compact_docs,
            &atoms,
            context.template_compatibility,
            false,
            index_recall_mode,
        )
    };
    let exact_recall = if conservative_group {
        stats.conservative_groups = 1;
        let calibration = calibrate_metadata_conservative_recall(
            &atoms,
            compact_docs,
            &candidate_index,
            context,
            template_cache_pool,
            progress,
        );
        let requires_exact_fallback = calibration.requires_exact_fallback();
        stats.recall_calibration = calibration;
        if requires_exact_fallback {
            stats.exact_fallback_groups = 1;
        }
        if let Some(progress) = progress {
            progress.finish_calibration();
            progress.update(&stats);
        }
        requires_exact_fallback
    } else {
        true
    };
    let candidate_index = candidate_index.into_effective_recall(exact_recall);
    let candidate_scratch_pool = MetadataCandidateScratchPool::new(atoms.len());
    let mut candidate_pairs = Vec::with_capacity(METADATA_CONTENT_SCORE_BATCH_PAIRS);
    let left_count = atoms.len().saturating_sub(1);
    if parallel {
        let wave_size = context
            .pool
            .current_num_threads()
            .max(1)
            .saturating_mul(METADATA_PARALLEL_LEFT_WAVE_MULTIPLIER);
        let first_wave_end = wave_size.min(left_count);
        let mut left_batches = context.pool.install(|| {
            collect_metadata_left_candidate_wave(
                0..first_wave_end,
                &atoms,
                compact_docs,
                &candidate_index,
                context.template_compatibility,
                exact_recall,
                &candidate_scratch_pool,
            )
        });
        let mut wave_end = first_wave_end;
        while wave_end < left_count {
            let next_wave_end = wave_end.saturating_add(wave_size).min(left_count);
            let current_left_batches = std::mem::take(&mut left_batches);
            let (next_left_batches, ()) = context.pool.install(|| {
                rayon::join(
                    || {
                        collect_metadata_left_candidate_wave(
                            wave_end..next_wave_end,
                            &atoms,
                            compact_docs,
                            &candidate_index,
                            context.template_compatibility,
                            exact_recall,
                            &candidate_scratch_pool,
                        )
                    },
                    || {
                        consume_metadata_left_candidate_wave(
                            current_left_batches,
                            MetadataLeftCandidateBatchConsumer {
                                atoms: &atoms,
                                compact_docs,
                                context,
                                state,
                                stats: &mut stats,
                                candidate_pairs: &mut candidate_pairs,
                                template_cache_pool,
                            },
                        );
                    },
                )
            });
            left_batches = next_left_batches;
            if let Some(progress) = progress {
                progress.update(&stats);
            }
            wave_end = next_wave_end;
        }
        consume_metadata_left_candidate_wave(
            left_batches,
            MetadataLeftCandidateBatchConsumer {
                atoms: &atoms,
                compact_docs,
                context,
                state,
                stats: &mut stats,
                candidate_pairs: &mut candidate_pairs,
                template_cache_pool,
            },
        );
        if let Some(progress) = progress {
            progress.update(&stats);
        }
    } else {
        let mut scratch = MetadataCandidateScratch::new(atoms.len());
        let mut pending_progress = 0usize;
        for left in 0..left_count {
            let left_batch = collect_metadata_left_candidate_batch(
                left,
                &atoms,
                compact_docs,
                &candidate_index,
                context.template_compatibility,
                exact_recall,
                &mut scratch,
            );
            MetadataLeftCandidateBatchConsumer {
                atoms: &atoms,
                compact_docs,
                context,
                state,
                stats: &mut stats,
                candidate_pairs: &mut candidate_pairs,
                template_cache_pool,
            }
            .apply(left_batch);
            pending_progress = pending_progress.saturating_add(1);
            if pending_progress >= 256 {
                if let Some(progress) = progress {
                    progress.update(&stats);
                }
                pending_progress = 0;
            }
        }
    }
    let batch_stats = score_and_apply_metadata_atom_pair_batch(
        &mut candidate_pairs,
        &atoms,
        compact_docs,
        context,
        state,
        template_cache_pool,
    );
    stats.accumulate_pair_scoring(batch_stats);
    if let Some(progress) = progress {
        progress.update(&stats);
    }
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
    union_metadata_no_common_atom_core(atoms, compact_docs, context, state, None)
}

fn union_metadata_no_common_atom_core(
    atoms: Vec<MetadataContentAtom>,
    compact_docs: &[CompactMetadataContentDocument],
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
    progress: Option<&ProgressTracker>,
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
    let template_cache_pool = MetadataTemplateScoreCachePool::default();
    if atoms.len() == METADATA_DIRECT_ATOM_GROUP_SIZE {
        let left = 0usize;
        let right = metadata_doc_index_from_usize(1);
        stats.raw_candidate_pairs = 1;
        if !metadata_content_atoms_share_token(left, right, &atoms, compact_docs)
            || !metadata_template_atoms_share_safe_prefix(
                left,
                right,
                &atoms,
                context.template_compatibility,
            )
        {
            stats.dimension_rejected_pairs = 1;
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
            stats.already_connected_pairs = 1;
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
            &template_cache_pool,
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
    let mut pending_progress = 0u64;
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
        stats.raw_candidate_pairs = stats
            .raw_candidate_pairs
            .saturating_add(scratch.raw_candidate_count as u64);
        stats.dimension_rejected_pairs = stats.dimension_rejected_pairs.saturating_add(
            scratch
                .raw_candidate_count
                .saturating_sub(scratch.candidates.len()) as u64,
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
                stats.dimension_rejected_pairs = stats.dimension_rejected_pairs.saturating_add(1);
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
                stats.already_connected_pairs = stats.already_connected_pairs.saturating_add(1);
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
                        &template_cache_pool,
                    );
                    stats.accumulate_pair_scoring(batch_stats);
                }
            }
        }
        pending_progress = pending_progress.saturating_add(1);
        if pending_progress >= 256 {
            if let Some(progress) = progress {
                progress.advance_task(
                    pending_progress,
                    ProgressCounters {
                        candidates: stats.candidate_pairs,
                        scored: stats.scored_pairs,
                        matched: stats.matched_pairs,
                        ..ProgressCounters::default()
                    },
                );
            }
            pending_progress = 0;
        }
    }
    let batch_stats = score_and_apply_metadata_fallback_atom_pair_batch(
        &mut candidate_pairs,
        &atoms,
        compact_docs,
        context,
        state,
        &template_cache_pool,
    );
    stats.accumulate_pair_scoring(batch_stats);
    if let Some(progress) = progress {
        progress.advance_task(
            pending_progress,
            ProgressCounters {
                candidates: stats.candidate_pairs,
                scored: stats.scored_pairs,
                matched: stats.matched_pairs,
                ..ProgressCounters::default()
            },
        );
    }
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
    if !cross_connected {
        return false;
    }
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
