use std::collections::{hash_map::RandomState, HashMap};
use std::path::Path;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::{Arc, Mutex};

use rayon::prelude::*;

use super::super::{
    MetadataRecallMode, ProgressCounters, ProgressTracker, SparseUnionFind, UnionFind,
};
#[cfg(test)]
use super::bm25::CompactMetadataPostings;
use super::bm25::{
    CompactMetadataContentDocument, CompactMetadataScoring, InternedMetadataCorpus,
    InternedMetadataSourceDoc, PreparedInternedMetadataDoc, PreparedInternedMetadataQuery,
};
#[cfg(test)]
use super::metadata_doc_index_from_usize;
use super::{
    CompactContractTokens, MetadataContractIndex, MetadataData, MetadataDocIndex,
    SourceMetadataDocEntry,
};
#[cfg(test)]
use super::{MetadataDocPair, MetadataTemplateMatches};

pub(super) const METADATA_RAW_GROUP_CHUNK_SIZE: usize = 1024;
pub(super) const METADATA_TOKEN_GROUP_BATCH_MULTIPLIER: usize = 4;
pub(super) const METADATA_PARALLEL_LEFT_WAVE_MULTIPLIER: usize = 2;
pub(super) const METADATA_DENSE_CANDIDATE_MIN_COUNT: usize = 65_536;
pub(super) const METADATA_DENSE_CANDIDATE_UNIVERSE_DIVISOR: usize = 32;
pub(super) const METADATA_DENSE_INTERSECTION_MIN_SCAN_COST: usize = 4 * 1024;
pub(super) const METADATA_DENSE_INTERSECTION_MAX_COST_RATIO: usize = 8;
pub(super) const METADATA_TEMPLATE_COMPACTION_SAMPLE_SIZE: usize = 512;
pub(super) const METADATA_TEMPLATE_COMPACTION_MIN_DUPLICATE_DENOMINATOR: usize = 8;
pub(super) const METADATA_CONSERVATIVE_ANCHOR_COUNT: usize = 16;
pub(super) const METADATA_CONSERVATIVE_SIMHASH_BANDS: usize = 8;
pub(super) const METADATA_CONSERVATIVE_SIMHASH_BAND_BITS: usize = 8;
pub(super) const METADATA_CONSERVATIVE_JOINT_BAND_FAMILIES: usize =
    METADATA_CONSERVATIVE_SIMHASH_BANDS * METADATA_CONSERVATIVE_SIMHASH_BANDS;
pub(super) const METADATA_CONSERVATIVE_JOINT_BAND_BUCKETS: usize =
    1 << (2 * METADATA_CONSERVATIVE_SIMHASH_BAND_BITS);
pub(super) const METADATA_CONSERVATIVE_JOINT_MIN_ATOMS: usize = 128 * 1024;
pub(super) const METADATA_CONSERVATIVE_SIMHASH_HAMMING_THRESHOLD: u32 = 32;
pub(super) const METADATA_CONSERVATIVE_HIGH_FREQUENCY_MIN_DOCS: usize = 32;
pub(super) const METADATA_CONSERVATIVE_HIGH_FREQUENCY_DIVISOR: usize = 5;
pub(super) const METADATA_CONSERVATIVE_MIN_ATOMS: usize = 256;
#[cfg(test)]
pub(super) const METADATA_CONSERVATIVE_CALIBRATION_DIVISOR: u64 = 100;
pub(super) const METADATA_CONSERVATIVE_CALIBRATION_MIN_LEFTS: usize = 256;
pub(super) const METADATA_CONSERVATIVE_CALIBRATION_MAX_LEFTS: usize = 4 * 1024;
pub(super) const METADATA_CONSERVATIVE_CALIBRATION_MAX_POSTING_VISITS: u64 = 1_000_000_000;
pub(super) const METADATA_CONSERVATIVE_RESCUE_MAX_POSTING_VISITS: u64 = 1_000_000_000;
pub(super) const METADATA_CONSERVATIVE_CONTRACT_DRIFT_PER_MILLE: u64 = 5;
pub(super) const METADATA_CONSERVATIVE_COMPONENT_DRIFT_PER_MILLE: u64 = 2;
pub(super) const METADATA_CONSERVATIVE_PAIR_DRIFT_MAX_RATE: f64 = 0.005;
pub(super) const METADATA_CONSERVATIVE_PAIR_WILSON_MIN_MATCHES: u64 = 768;
pub(super) const METADATA_CALIBRATION_WEIGHT_SCALE: u64 = 1_000_000;
pub(super) const NO_METADATA_ATOM: usize = usize::MAX;
pub(super) const METADATA_TEMPLATE_SCORE_CACHE_SLOTS: usize = 256 * 1024;
pub(super) const METADATA_TEMPLATE_SCORE_CACHE_WAYS: usize = 4;
pub(super) const METADATA_DIRECT_ATOM_GROUP_SIZE: usize = 2;

#[derive(Debug)]
pub(crate) struct InternedMetadataIndex {
    pub(super) doc_count: usize,
    pub(super) scoring: CompactMetadataScoring,
    #[cfg(test)]
    pub(super) postings: CompactMetadataPostings,
    #[cfg(test)]
    pub(super) token_ids: HashMap<String, usize>,
    #[cfg(test)]
    pub(super) build_thread_count: usize,
}

// types from atoms
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

pub(super) struct MetadataFallbackTokenExclusionIndex {
    pub(super) postings: MetadataSparseCandidatePostings,
}

pub(super) struct MetadataFallbackTokenExclusionScratch {
    pub(super) words: Vec<u64>,
    pub(super) touched_words: Vec<usize>,
    pub(super) prepared_single_group: bool,
}

#[derive(Default)]
pub(super) struct CompactMetadataContentGroupBuilder {
    pub(super) token_ids: HashMap<String, u32>,
    pub(super) atom_hasher: RandomState,
    pub(super) atom_index_by_hash: HashMap<u64, usize>,
    pub(super) next_atom_with_same_hash: Vec<usize>,
    pub(super) fallback_group_index_by_hash: HashMap<(usize, u64), usize>,
    pub(super) next_fallback_group_with_same_hash: Vec<Vec<usize>>,
    pub(super) docs: Vec<CompactMetadataContentDocument>,
    pub(super) atoms: Vec<MetadataContentAtom>,
    pub(super) token_key_bytes: usize,
    pub(super) term_count: usize,
    pub(super) template_candidate_term_count: usize,
    pub(super) member_count: usize,
    pub(super) fallback_group_count: usize,
    pub(super) fallback_member_count: usize,
    pub(super) fallback_token_posting_count: usize,
}

#[derive(Default)]
pub(super) struct MetadataRawTokenGroup {
    pub(super) raw_records: Vec<(MetadataContractIndex, String)>,
    pub(super) raw_payload_bytes: usize,
    pub(super) compact: CompactMetadataContentGroupBuilder,
    pub(super) raw_record_count: usize,
    #[cfg(test)]
    pub(super) max_raw_buffer_len: usize,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug)]
pub(super) enum MetadataContentScope {
    SharedToken,
    NoCommonToken,
}

// types from postings
pub(super) struct MetadataContentCandidateIndex {
    pub(super) posting_offsets: Vec<u64>,
    pub(super) posting_atoms: Vec<MetadataDocIndex>,
}

pub(super) struct MetadataSparseCandidatePostings {
    pub(super) token_ids: Vec<u32>,
    pub(super) posting_offsets: Vec<u64>,
    pub(super) posting_atoms: Vec<MetadataDocIndex>,
}

pub(super) struct MetadataTemplateCandidateIndex {
    pub(super) full: MetadataSparseCandidatePostings,
    pub(super) prefix: MetadataSparseCandidatePostings,
}

#[derive(Clone, Copy)]
pub(super) struct MetadataPostingRange {
    pub(super) start: usize,
    pub(super) end: usize,
}

#[derive(Default)]
pub(super) struct MetadataCandidatePostingPlan {
    pub(super) content: Vec<MetadataPostingRange>,
    pub(super) template_full: Vec<MetadataPostingRange>,
    pub(super) template_prefix: Vec<MetadataPostingRange>,
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

// types from conservative
#[derive(Clone, Copy)]
pub(super) struct MetadataConservativeSketch {
    pub(super) simhash: u64,
    pub(super) anchors: [u32; METADATA_CONSERVATIVE_ANCHOR_COUNT],
    pub(super) anchor_len: u8,
    pub(super) has_terms: bool,
}

#[derive(Clone, Copy)]
pub(super) struct MetadataConservativeTokenStats {
    pub(super) document_frequency: usize,
    pub(super) idf: f64,
    pub(super) hash: u64,
    pub(super) anchor_eligible: bool,
}

pub(super) struct MetadataConservativeDimensionIndex {
    pub(super) sketches: Vec<MetadataConservativeSketch>,
    pub(super) anchor_postings: MetadataSparseCandidatePostings,
    pub(super) simhash_band_postings: MetadataSparseCandidatePostings,
}

pub(super) struct MetadataConservativeJointBandFamily {
    pub(super) posting_offsets: Vec<u64>,
    pub(super) posting_atoms: Vec<MetadataDocIndex>,
}

pub(super) struct MetadataConservativeJointBandIndex {
    pub(super) families: Vec<MetadataConservativeJointBandFamily>,
}

pub(super) struct MetadataConservativeCandidateIndex {
    pub(super) exact_template: Option<MetadataTemplateCandidateIndex>,
    pub(super) exact_content: Option<MetadataContentCandidateIndex>,
    pub(super) template: MetadataConservativeDimensionIndex,
    pub(super) content: MetadataConservativeDimensionIndex,
    pub(super) joint_bands: Option<MetadataConservativeJointBandIndex>,
    pub(super) profile: MetadataConservativeRecallProfile,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum MetadataConservativeRecallProfile {
    Base,
    Widened,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct MetadataCalibrationWorkItem {
    pub(super) left: usize,
    pub(super) chain_index: usize,
    pub(super) estimated_posting_visits: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct MetadataCalibrationSample {
    pub(super) left: usize,
    pub(super) chain_index: usize,
    pub(super) cost_bucket: u32,
    pub(super) estimated_posting_visits: u64,
    pub(super) stratum_population: u64,
    pub(super) stratum_sample_count: u64,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct MetadataCalibrationPlan {
    pub(super) samples: Vec<MetadataCalibrationSample>,
    pub(super) estimated_posting_visits_by_left: Vec<u64>,
    pub(super) difficult_first_lefts: Vec<usize>,
    pub(super) estimated_total_posting_visits: u64,
    pub(super) estimated_sample_posting_visits: u64,
    pub(super) retained_calibration_candidates: usize,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct MetadataExactRescuePlan {
    pub(super) exact_recall_by_left: Vec<bool>,
    pub(super) exact_left_atoms: u64,
    pub(super) estimated_exact_posting_visits: u64,
    pub(super) unrescued_risk_strata: u64,
}

pub(super) struct MetadataRecallCalibrationOutcome {
    pub(super) stats: MetadataRecallCalibrationStats,
    pub(super) risk_strata: Vec<(usize, u32)>,
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
    pub(super) weighted_exact_matched_pairs: u128,
    pub(super) weighted_missed_matched_pairs: u128,
    pub(super) weighted_exact_duplicate_contract_members: u128,
    pub(super) weighted_missed_duplicate_contract_members: u128,
    pub(super) weighted_exact_component_members: u128,
    pub(super) weighted_shifted_component_members: u128,
}

// types from template_cache
#[derive(Clone, Copy)]
pub(super) enum MetadataTemplateCompatibility<'a> {
    Scored(&'a CompactMetadataScoring),
    #[cfg(test)]
    Precomputed(&'a MetadataTemplateMatches),
}

#[derive(Clone, Copy)]
pub(super) struct MetadataTemplateScoreCacheEntry {
    pub(super) key: u64,
    pub(super) score_count: u8,
    pub(super) matched: bool,
    pub(super) valid: bool,
}

pub(super) struct MetadataTemplateScoreCache {
    pub(super) entries: Box<[MetadataTemplateScoreCacheEntry]>,
}

#[derive(Default)]
pub(super) struct MetadataTemplateScoreCachePool {
    pub(super) caches: Mutex<Vec<MetadataTemplateScoreCache>>,
}

pub(super) struct MetadataTemplateScoreCacheLease<'a> {
    pub(super) pool: &'a MetadataTemplateScoreCachePool,
    pub(super) cache: Option<MetadataTemplateScoreCache>,
}

// types from scratch
pub(super) struct MetadataCandidateScratch {
    pub(super) seen_generation: Vec<u16>,
    pub(super) generation: u16,
    pub(super) candidates: Vec<MetadataDocIndex>,
    pub(super) secondary_seen_generation: Vec<u16>,
    pub(super) secondary_generation: u16,
    pub(super) secondary_candidates: Vec<MetadataDocIndex>,
    pub(super) posting_plan: MetadataCandidatePostingPlan,
    pub(super) raw_candidate_count: usize,
    pub(super) visited_posting_entries: u64,
    pub(super) fallback_token_exclusion: MetadataFallbackTokenExclusionScratch,
}

pub(super) struct MetadataCandidateScratchPool {
    pub(super) doc_count: usize,
    pub(super) scratches: Mutex<Vec<MetadataCandidateScratch>>,
}

pub(super) struct MetadataCandidateScratchLease<'a> {
    pub(super) pool: &'a MetadataCandidateScratchPool,
    pub(super) scratch: Option<MetadataCandidateScratch>,
}

pub(super) struct MetadataCandidateBufferPool {
    pub(super) universe_len: usize,
    pub(super) maximum_retained: usize,
    pub(super) sparse: Mutex<Vec<Vec<MetadataDocIndex>>>,
    pub(super) dense: Mutex<Vec<(Vec<u64>, Vec<usize>)>>,
}

pub(super) struct MetadataSparseCandidateBuffer {
    pub(super) candidates: Vec<MetadataDocIndex>,
    pub(super) pool: Option<Arc<MetadataCandidateBufferPool>>,
}

#[cfg(test)]
pub(super) struct MetadataPairScoringContext<'a> {
    pub(super) postings: &'a CompactMetadataPostings,
    pub(super) scoring: &'a CompactMetadataScoring,
}

#[cfg(test)]
pub(super) struct MetadataHitPermits {
    pub(super) remaining: AtomicUsize,
    pub(super) exceeded: AtomicBool,
}

// types from waves
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
pub(super) struct MetadataTemplatePairEvaluation {
    pub(super) matched: bool,
    pub(super) score_count: u64,
    pub(super) cache_hit: bool,
}

#[derive(Default)]
pub(super) struct MetadataValidatedPairBatch {
    pub(super) hits: Vec<(usize, MetadataDocIndex)>,
    pub(super) stats: MetadataPairScoringStats,
}

pub(super) struct MetadataLeftCandidateBatch {
    pub(super) left: usize,
    pub(super) candidates: MetadataCandidateSet,
    pub(super) raw_candidate_pairs: u64,
    pub(super) dimension_rejected_pairs: u64,
    pub(super) token_overlap_rejected_pairs: u64,
    pub(super) estimated_posting_visits: u64,
    pub(super) visited_posting_entries: u64,
    pub(super) token_exclusion_posting_visits: u64,
}

#[derive(Clone, Copy)]
pub(super) struct MetadataCandidateCollectionContext<'a> {
    pub(super) atoms: &'a [MetadataContentAtom],
    pub(super) compact_docs: &'a [CompactMetadataContentDocument],
    pub(super) candidate_index: &'a MetadataLocalCandidateIndex,
    pub(super) compatibility: MetadataTemplateCompatibility<'a>,
    pub(super) exact_recall: bool,
    pub(super) exact_recall_by_left: Option<&'a [bool]>,
    pub(super) scope: MetadataCandidateUnionScope,
    pub(super) contract_tokens: &'a CompactContractTokens,
    pub(super) fallback_token_exclusion_index: Option<&'a MetadataFallbackTokenExclusionIndex>,
    pub(super) candidate_buffer_pool: Option<&'a Arc<MetadataCandidateBufferPool>>,
    pub(super) estimated_posting_visits_by_left: Option<&'a [u64]>,
}

pub(super) struct MetadataRecallCalibrationRequest<'a, 'context> {
    pub(super) atoms: &'a [MetadataContentAtom],
    pub(super) compact_docs: &'a [CompactMetadataContentDocument],
    pub(super) candidate_index: &'a MetadataLocalCandidateIndex,
    pub(super) samples: Vec<MetadataCalibrationSample>,
    pub(super) estimated_posting_visits_by_left: &'a [u64],
    pub(super) context: &'a MetadataContentUnionContext<'context>,
    pub(super) template_cache_pool: &'a MetadataTemplateScoreCachePool,
    pub(super) scope: MetadataCandidateUnionScope,
    pub(super) fallback_token_exclusion_index: Option<&'a MetadataFallbackTokenExclusionIndex>,
    pub(super) candidate_buffer_pool: Option<&'a Arc<MetadataCandidateBufferPool>>,
    pub(super) progress: Option<MetadataSharedTokenGroupProgress<'a>>,
}

pub(super) enum MetadataCandidateSet {
    Sparse(MetadataSparseCandidateBuffer),
    Dense(MetadataDenseCandidateBitmap),
}

pub(super) struct MetadataDenseCandidateBitmap {
    pub(super) words: Vec<u64>,
    pub(super) touched_words: Vec<usize>,
    pub(super) len: usize,
    pub(super) pool: Option<Arc<MetadataCandidateBufferPool>>,
}

pub(super) enum MetadataCandidateSetIter<'a> {
    Sparse(std::iter::Copied<std::slice::Iter<'a, MetadataDocIndex>>),
    Dense(MetadataDenseCandidateBitmapIter<'a>),
}

pub(super) struct MetadataDenseCandidateBitmapIter<'a> {
    pub(super) words: &'a [u64],
    pub(super) word_index: usize,
    pub(super) remaining_word: u64,
}

#[derive(Clone, Copy)]
pub(super) enum MetadataCandidateUnionScope {
    SharedToken,
    Fallback,
}

pub(super) struct MetadataLeftCandidateBatchConsumer<'a, 'context> {
    pub(super) atoms: &'a [MetadataContentAtom],
    pub(super) compact_docs: &'a [CompactMetadataContentDocument],
    pub(super) context: &'a MetadataContentUnionContext<'context>,
    pub(super) state: &'a mut MetadataUnionState,
    pub(super) stats: &'a mut MetadataContentUnionStats,
    pub(super) candidate_pairs: &'a mut Vec<(usize, MetadataDocIndex)>,
    pub(super) template_cache_pool: &'a MetadataTemplateScoreCachePool,
    pub(super) scope: MetadataCandidateUnionScope,
}

// types from union
pub(super) struct MetadataContentUnionContext<'a> {
    pub(super) data: &'a MetadataData,
    pub(super) template_compatibility: MetadataTemplateCompatibility<'a>,
    pub(super) contract_tokens: &'a CompactContractTokens,
    pub(super) chain_count: usize,
    pub(super) pool: &'a rayon::ThreadPool,
    pub(super) recall_mode: MetadataRecallMode,
}

pub(super) struct MetadataUnionState {
    pub(super) intra: UnionFind,
    pub(super) cross: Option<SparseUnionFind>,
    pub(super) chain_matrix: Option<Vec<SparseUnionFind>>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct MetadataContentUnionStats {
    pub(super) atom_count: usize,
    pub(super) processed_left_atoms: u64,
    pub(super) estimated_posting_visits: u64,
    pub(super) visited_posting_entries: u64,
    pub(super) token_exclusion_posting_visits: u64,
    pub(super) dense_candidate_promotions: u64,
    pub(super) raw_candidate_pairs: u64,
    pub(super) dimension_rejected_pairs: u64,
    pub(super) token_overlap_rejected_pairs: u64,
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
    pub(super) exact_rescue_left_atoms: u64,
    pub(super) exact_rescue_estimated_posting_visits: u64,
    pub(super) unrescued_recall_risk_strata: u64,
    pub(super) recall_risk_exceeded_groups: u64,
}

#[derive(Clone, Copy)]
pub(super) struct MetadataSharedTokenGroupProgress<'a> {
    pub(super) tracker: &'a ProgressTracker,
    pub(super) completed_groups: u64,
    pub(super) base: ProgressCounters,
}

mod atoms;
mod conservative;
mod postings;
mod scratch;
mod template_cache;
mod union;
mod waves;

pub(super) use atoms::*;
pub(super) use conservative::*;
pub(super) use union::*;
pub(super) use waves::*;

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
