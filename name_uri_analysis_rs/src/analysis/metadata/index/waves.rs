#[cfg(test)]
use std::time::Duration;

use rayon::prelude::*;

#[cfg(test)]
use super::super::super::AnalysisError;
use super::super::bm25::CompactMetadataContentDocument;
#[cfg(test)]
use super::super::bm25::{CompactMetadataContentSet, MetadataContentRecord};
use super::super::{
    metadata_contract_index_to_usize, metadata_doc_index_to_usize, MetadataDocIndex,
    METADATA_CONTENT_PARALLEL_MIN_RECORDS, METADATA_CONTENT_SCORE_BATCH_PAIRS,
};
#[cfg(test)]
use super::super::{metadata_doc_index_from_usize, METADATA_THRESHOLD};
#[cfg(test)]
use super::super::{MetadataDocPair, MetadataTemplateMatches, METADATA_PAIR_LEFT_CHUNK_SIZE};

use super::*;

#[cfg(test)]
pub(in super::super) fn metadata_scoring_progress_units(scoring_left_count: usize) -> u64 {
    scoring_left_count as u64
}

#[cfg(test)]
pub(in super::super) fn metadata_pair_left_chunk_size(
    doc_count: usize,
    max_match_pairs: u64,
) -> usize {
    let doc_count = u64::try_from(doc_count.max(1)).unwrap_or(u64::MAX);
    let budgeted_chunk = max_match_pairs / doc_count;
    budgeted_chunk.clamp(1, METADATA_PAIR_LEFT_CHUNK_SIZE as u64) as usize
}

#[cfg(test)]
pub(in super::super) fn metadata_template_match_pair_budget(
    available_bytes: usize,
    doc_count: usize,
) -> u64 {
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
pub(in super::super) fn metadata_scoring_batch_progress_units(
    left_start: usize,
    left_end: usize,
) -> u64 {
    left_end.saturating_sub(left_start) as u64
}

#[cfg(test)]
pub(in super::super) fn metadata_pair_progress_message(
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
pub(in super::super) fn estimate_remaining_metadata_candidate_pairs(
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
pub(in super::super) fn format_metadata_pair_throughput(
    scored_pairs: u64,
    elapsed: Duration,
) -> String {
    let Some(pairs_per_second) = metadata_pairs_per_second(scored_pairs, elapsed) else {
        return "n/a".to_string();
    };
    format!("{pairs_per_second:.1} pairs/s")
}

#[cfg(test)]
pub(in super::super) fn format_metadata_pair_eta(
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
pub(in super::super) fn metadata_pairs_per_second(
    scored_pairs: u64,
    elapsed: Duration,
) -> Option<f64> {
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
pub(in super::super) fn format_metadata_duration(duration: Duration) -> String {
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
pub(in super::super) fn collect_metadata_doc_pair_hits_for_left_range(
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
pub(in super::super) fn collect_metadata_doc_pair_hits_for_left_range_bounded(
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
pub(in super::super) fn collect_metadata_doc_pair_hits_for_left_with_scratch(
    left: usize,
    context: &MetadataPairScoringContext<'_>,
    scratch: &mut MetadataCandidateScratch,
    hits: &mut Vec<MetadataDocPair>,
) -> u64 {
    collect_metadata_doc_pair_hits_for_left_with_scratch_bounded(left, context, scratch, hits, None)
}

#[cfg(test)]
pub(in super::super) fn collect_metadata_doc_pair_hits_for_left_with_scratch_bounded(
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
pub(in super::super) fn metadata_candidate_indices_for_left_with_scratch<'a>(
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
pub(in super::super) fn append_metadata_posting_except(
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
pub(in super::super) fn ordered_metadata_doc_pair(left: usize, right: usize) -> MetadataDocPair {
    let left = metadata_doc_index_from_usize(left);
    let right = metadata_doc_index_from_usize(right);
    if left <= right {
        (left, right)
    } else {
        (right, left)
    }
}

#[cfg(test)]
pub(in super::super) fn collect_metadata_content_atom_pair_hits(
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

pub(in super::super) fn metadata_template_pair_key(
    left: MetadataDocIndex,
    right: MetadataDocIndex,
) -> u64 {
    let (left, right) = if left < right {
        (left, right)
    } else {
        (right, left)
    };
    (u64::from(left) << 32) | u64::from(right)
}

pub(in super::super) fn should_compact_metadata_template_pairs(
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

pub(in super::super) fn collect_metadata_template_pair_evaluations(
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

impl MetadataValidatedPairBatch {
    pub(in super::super) fn with_hit_capacity(capacity: usize) -> Self {
        Self {
            hits: Vec::with_capacity(capacity),
            stats: MetadataPairScoringStats::default(),
        }
    }

    pub(in super::super) fn score_pair_with_cache(
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

    pub(in super::super) fn score_pair(
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

    pub(in super::super) fn merge(mut self, mut other: Self) -> Self {
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

pub(in super::super) fn collect_metadata_validated_atom_pair_hits(
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
                            let mut batch =
                                MetadataValidatedPairBatch::with_hit_capacity(pairs.len());
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
        let mut batch = MetadataValidatedPairBatch::with_hit_capacity(candidate_pairs.len());
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
                    let mut batch = MetadataValidatedPairBatch::with_hit_capacity(pairs.len());
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
        let mut batch = MetadataValidatedPairBatch::with_hit_capacity(candidate_pairs.len());
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

pub(in super::super) fn score_and_apply_metadata_atom_pair_batch(
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

pub(in super::super) fn score_and_apply_metadata_fallback_atom_pair_batch(
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
pub(in super::super) fn collect_metadata_content_candidate_pairs(
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
pub(in super::super) fn union_metadata_shared_token_atoms(
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
    .expect("exact shared-token atom union must complete")
}

#[cfg(test)]
pub(in super::super) fn union_metadata_shared_token_atoms_with_mode(
    records: &[MetadataContentRecord],
    compact_docs: &[CompactMetadataContentDocument],
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
    recall_mode: MetadataRecallMode,
) -> Result<MetadataContentUnionStats, AnalysisError> {
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

pub(in super::super) fn collect_metadata_left_candidate_batch(
    left: usize,
    collection: &MetadataCandidateCollectionContext<'_>,
    scratch: &mut MetadataCandidateScratch,
) -> MetadataLeftCandidateBatch {
    let atoms = collection.atoms;
    let compact_docs = collection.compact_docs;
    let compatibility = collection.compatibility;
    let left_atom = &atoms[left];
    let left_record_index = metadata_doc_index_to_usize(left_atom.representative_record_index);
    scratch.clear_for_next_left();
    let estimated_posting_visits = collection
        .estimated_posting_visits_by_left
        .and_then(|work| work.get(left).copied())
        .unwrap_or_else(|| {
            let mut posting_plan = std::mem::take(&mut scratch.posting_plan);
            posting_plan.clear();
            let estimated = collection.candidate_index.estimate_exact_posting_visits(
                left,
                left_atom,
                &compact_docs[left_record_index],
                compatibility,
                &mut posting_plan,
            );
            scratch.posting_plan = posting_plan;
            estimated as u64
        });
    let exact_recall = collection.exact_recall
        || collection
            .exact_recall_by_left
            .and_then(|exact_by_left| exact_by_left.get(left))
            .copied()
            .unwrap_or(false);
    let candidate_basis = if exact_recall {
        collection.candidate_index.append_exact_candidates_after(
            left,
            left_atom,
            &compact_docs[left_record_index],
            compatibility,
            scratch,
        )
    } else {
        collection.candidate_index.append_candidates_after(
            left,
            left_atom,
            &compact_docs[left_record_index],
            compatibility,
            scratch,
        )
    };
    let raw_candidate_pairs = scratch.raw_candidate_count as u64;
    let mut candidates = collection
        .candidate_buffer_pool
        .map(|pool| pool.take_sparse())
        .unwrap_or_default();
    candidates.reserve(scratch.candidates.len());
    for right in scratch.candidates.iter().copied() {
        if !metadata_candidate_intersects_both_dimensions(
            candidate_basis,
            left,
            right,
            atoms,
            compact_docs,
            compatibility,
        ) {
            continue;
        }
        candidates.push(right);
    }
    let dimension_accepted_pairs = candidates.len() as u64;
    let mut token_overlap_rejected_pairs = 0u64;
    let mut token_exclusion_posting_visits = 0u64;
    if matches!(collection.scope, MetadataCandidateUnionScope::Fallback) {
        if !candidates.is_empty() {
            if let Some(exclusion_index) = collection.fallback_token_exclusion_index {
                token_exclusion_posting_visits = exclusion_index.prepare_left_if_cheaper(
                    left,
                    &candidates,
                    atoms,
                    collection.contract_tokens,
                    &mut scratch.fallback_token_exclusion,
                ) as u64;
            }
        }
        candidates.retain(|&right| {
            let right_index = metadata_doc_index_to_usize(right);
            let has_disjoint_token_groups = collection
                .fallback_token_exclusion_index
                .map(|exclusion_index| {
                    exclusion_index.atoms_have_disjoint_token_groups(
                        left,
                        right_index,
                        atoms,
                        collection.contract_tokens,
                        &scratch.fallback_token_exclusion,
                    )
                })
                .unwrap_or_else(|| {
                    metadata_fallback_atoms_have_disjoint_token_groups(
                        left_atom,
                        &atoms[right_index],
                        collection.contract_tokens,
                    )
                });
            if !has_disjoint_token_groups {
                token_overlap_rejected_pairs = token_overlap_rejected_pairs.saturating_add(1);
            }
            has_disjoint_token_groups
        });
    }
    let dimension_rejected_pairs = raw_candidate_pairs.saturating_sub(dimension_accepted_pairs);
    let candidates = match collection.candidate_buffer_pool {
        Some(pool) => {
            MetadataCandidateSet::from_pooled_sparse(candidates, atoms.len(), Arc::clone(pool))
        }
        None => MetadataCandidateSet::from_sparse(candidates, atoms.len()),
    };
    debug_assert_eq!(
        candidates.len() as u64,
        dimension_accepted_pairs.saturating_sub(token_overlap_rejected_pairs)
    );
    MetadataLeftCandidateBatch {
        left,
        candidates,
        raw_candidate_pairs,
        dimension_rejected_pairs,
        token_overlap_rejected_pairs,
        estimated_posting_visits,
        visited_posting_entries: scratch.visited_posting_entries,
        token_exclusion_posting_visits,
    }
}

pub(in super::super) fn collect_metadata_left_candidate_wave(
    lefts: &[usize],
    collection: &MetadataCandidateCollectionContext<'_>,
    scratch_pool: &MetadataCandidateScratchPool,
) -> Vec<MetadataLeftCandidateBatch> {
    lefts
        .par_iter()
        .copied()
        .map_init(
            || scratch_pool.take(),
            |scratch, left| collect_metadata_left_candidate_batch(left, collection, scratch),
        )
        .collect()
}

pub(in super::super) fn consume_metadata_left_candidate_wave(
    left_batches: Vec<MetadataLeftCandidateBatch>,
    mut consumer: MetadataLeftCandidateBatchConsumer<'_, '_>,
) {
    for left_batch in left_batches {
        consumer.apply(left_batch);
    }
}

impl MetadataLeftCandidateBatchConsumer<'_, '_> {
    pub(in super::super) fn apply(&mut self, left_batch: MetadataLeftCandidateBatch) {
        let left = left_batch.left;
        self.stats.processed_left_atoms = self.stats.processed_left_atoms.saturating_add(1);
        self.stats.estimated_posting_visits = self
            .stats
            .estimated_posting_visits
            .saturating_add(left_batch.estimated_posting_visits);
        self.stats.visited_posting_entries = self
            .stats
            .visited_posting_entries
            .saturating_add(left_batch.visited_posting_entries);
        self.stats.token_exclusion_posting_visits = self
            .stats
            .token_exclusion_posting_visits
            .saturating_add(left_batch.token_exclusion_posting_visits);
        self.stats.dense_candidate_promotions = self
            .stats
            .dense_candidate_promotions
            .saturating_add(u64::from(left_batch.candidates.is_dense()));
        self.stats.raw_candidate_pairs = self
            .stats
            .raw_candidate_pairs
            .saturating_add(left_batch.raw_candidate_pairs);
        self.stats.dimension_rejected_pairs = self
            .stats
            .dimension_rejected_pairs
            .saturating_add(left_batch.dimension_rejected_pairs);
        self.stats.token_overlap_rejected_pairs = self
            .stats
            .token_overlap_rejected_pairs
            .saturating_add(left_batch.token_overlap_rejected_pairs);
        let left_atom = &self.atoms[left];
        let left_contract_index = metadata_contract_index_to_usize(left_atom.members[0]);
        debug_assert_eq!(
            self.context.data.contracts[left_contract_index].chain_index,
            left_atom.chain_index
        );
        for right in left_batch.candidates.iter() {
            self.stats.candidate_pairs = self.stats.candidate_pairs.saturating_add(1);
            let right_atom = &self.atoms[metadata_doc_index_to_usize(right)];
            let right_contract_index = metadata_contract_index_to_usize(right_atom.members[0]);
            let singleton_pair = left_atom.members.len() == 1 && right_atom.members.len() == 1;
            let same_chain = left_atom.chain_index == right_atom.chain_index;
            let should_check_connected = match self.scope {
                MetadataCandidateUnionScope::SharedToken => singleton_pair || same_chain,
                MetadataCandidateUnionScope::Fallback => singleton_pair,
            };
            if should_check_connected
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
                let batch_stats = match self.scope {
                    MetadataCandidateUnionScope::SharedToken => {
                        score_and_apply_metadata_atom_pair_batch(
                            self.candidate_pairs,
                            self.atoms,
                            self.compact_docs,
                            self.context,
                            self.state,
                            self.template_cache_pool,
                        )
                    }
                    MetadataCandidateUnionScope::Fallback => {
                        score_and_apply_metadata_fallback_atom_pair_batch(
                            self.candidate_pairs,
                            self.atoms,
                            self.compact_docs,
                            self.context,
                            self.state,
                            self.template_cache_pool,
                        )
                    }
                };
                self.stats.accumulate_pair_scoring(batch_stats);
            }
        }
    }
}
