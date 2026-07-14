use std::collections::HashMap;

use duckdb::Connection;
use rayon::prelude::*;

use super::super::super::{
    arrow_i64_column, arrow_string_column, chain_pair_index, AnalysisError, MetadataRecallMode,
    ProgressCounters, ProgressTracker,
};
use super::super::bm25::CompactMetadataContentDocument;
#[cfg(test)]
use super::super::bm25::{CompactMetadataContentSet, MetadataContentRecord};
use super::super::{
    metadata_contract_index_from_usize, metadata_contract_index_to_usize,
    metadata_doc_index_from_usize, MetadataContractIndex, MetadataData, SourceMetadataDocEntry,
    METADATA_CONTENT_PARALLEL_MIN_RECORDS, METADATA_CONTENT_SCORE_BATCH_PAIRS,
};

use super::*;

impl MetadataContentUnionStats {
    pub(in super::super) fn accumulate(&mut self, other: Self) {
        self.atom_count = self.atom_count.saturating_add(other.atom_count);
        self.processed_left_atoms = self
            .processed_left_atoms
            .saturating_add(other.processed_left_atoms);
        self.estimated_posting_visits = self
            .estimated_posting_visits
            .saturating_add(other.estimated_posting_visits);
        self.visited_posting_entries = self
            .visited_posting_entries
            .saturating_add(other.visited_posting_entries);
        self.dense_candidate_promotions = self
            .dense_candidate_promotions
            .saturating_add(other.dense_candidate_promotions);
        self.raw_candidate_pairs = self
            .raw_candidate_pairs
            .saturating_add(other.raw_candidate_pairs);
        self.dimension_rejected_pairs = self
            .dimension_rejected_pairs
            .saturating_add(other.dimension_rejected_pairs);
        self.token_overlap_rejected_pairs = self
            .token_overlap_rejected_pairs
            .saturating_add(other.token_overlap_rejected_pairs);
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

    pub(in super::super) fn accumulate_pair_scoring(&mut self, other: MetadataPairScoringStats) {
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

pub(in super::super) fn metadata_shared_token_group_progress_counters(
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

impl MetadataSharedTokenGroupProgress<'_> {
    pub(in super::super) fn update(self, live: &MetadataContentUnionStats) {
        self.tracker.advance_task(
            0,
            metadata_shared_token_group_progress_counters(self.completed_groups, self.base, live),
        );
    }

    pub(in super::super) fn update_calibration(
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

    pub(in super::super) fn finish_calibration(self) {
        self.tracker
            .update_task_label("matching shared-token memberships");
    }
}

pub(in super::super) fn union_metadata_token_content_matches(
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
    let small_group_reserve_bytes = super::super::load::metadata_uncached_parse_transient_bytes(
        METADATA_CONTENT_PARALLEL_MIN_RECORDS
            .saturating_mul(super::super::parse::MAX_METADATA_BYTES_FOR_DEDUP),
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

pub(in super::super) fn metadata_token_content_rows_sql() -> &'static str {
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

pub(in super::super) fn union_metadata_representative_content_fallback(
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
        "planning representative fallback posting work",
        Some(atoms.len().saturating_sub(1) as u64),
        "atoms",
    );
    let stats = union_metadata_no_common_atom_core(atoms, &docs, context, state, Some(progress))?;
    progress.finish_task(format!(
        "representative fallback complete; lefts {}; estimated/visited postings {}/{}; dense-promotions {}; raw {}; dimension-rejected {}; token-overlap-rejected {}; candidates {}; connected-skips {}; scored {}; matched {}",
        stats.processed_left_atoms,
        stats.estimated_posting_visits,
        stats.visited_posting_entries,
        stats.dense_candidate_promotions,
        stats.raw_candidate_pairs,
        stats.dimension_rejected_pairs,
        stats.token_overlap_rejected_pairs,
        stats.candidate_pairs,
        stats.already_connected_pairs,
        stats.scored_pairs,
        stats.matched_pairs
    ));
    Ok(stats)
}

pub(in super::super) fn apply_metadata_contract_pair_union(
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

pub(in super::super) fn apply_metadata_same_chain_group_union(
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

pub(in super::super) fn apply_metadata_complete_bipartite_group_union(
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

pub(in super::super) fn apply_metadata_atom_pair_union(
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

pub(in super::super) fn union_metadata_shared_token_atom_core(
    atoms: Vec<MetadataContentAtom>,
    compact_docs: &[CompactMetadataContentDocument],
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
    template_cache_pool: &MetadataTemplateScoreCachePool,
    recall_mode: MetadataRecallMode,
    progress: Option<MetadataSharedTokenGroupProgress<'_>>,
) -> Result<MetadataContentUnionStats, AnalysisError> {
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
        return Ok(stats);
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
            return Ok(stats);
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
            return Ok(stats);
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
        return Ok(stats);
    }
    let parallel = atoms.len() >= METADATA_CONTENT_PARALLEL_MIN_RECORDS;
    let conservative_group = recall_mode == MetadataRecallMode::Conservative
        && atoms.len() >= METADATA_CONSERVATIVE_MIN_ATOMS;
    let index_recall_mode = if conservative_group {
        MetadataRecallMode::Conservative
    } else {
        MetadataRecallMode::Exact
    };
    let mut candidate_index = if parallel {
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
    let candidate_buffer_pool = Arc::new(MetadataCandidateBufferPool::new(
        atoms.len(),
        context
            .pool
            .current_num_threads()
            .max(1)
            .saturating_mul(METADATA_PARALLEL_LEFT_WAVE_MULTIPLIER),
    ));
    let work_plan = if conservative_group {
        metadata_conservative_calibration_plan_with_work_budget(
            &atoms,
            compact_docs,
            &candidate_index,
            context.template_compatibility,
            context.pool,
        )?
    } else {
        metadata_exact_work_plan(
            &atoms,
            compact_docs,
            &candidate_index,
            context.template_compatibility,
            context.pool,
        )?
    };
    let exact_recall = if conservative_group {
        stats.conservative_groups = 1;
        let mut calibration =
            calibrate_metadata_conservative_recall(MetadataRecallCalibrationRequest {
                atoms: &atoms,
                compact_docs,
                candidate_index: &candidate_index,
                samples: work_plan.samples.clone(),
                estimated_posting_visits_by_left: &work_plan.estimated_posting_visits_by_left,
                context,
                template_cache_pool,
                scope: MetadataCandidateUnionScope::SharedToken,
                fallback_token_exclusion_index: None,
                candidate_buffer_pool: Some(&candidate_buffer_pool),
                progress,
            });
        if calibration.requires_exact_fallback() {
            candidate_index.set_conservative_profile(MetadataConservativeRecallProfile::Widened);
            calibration =
                calibrate_metadata_conservative_recall(MetadataRecallCalibrationRequest {
                    atoms: &atoms,
                    compact_docs,
                    candidate_index: &candidate_index,
                    samples: work_plan.samples,
                    estimated_posting_visits_by_left: &work_plan.estimated_posting_visits_by_left,
                    context,
                    template_cache_pool,
                    scope: MetadataCandidateUnionScope::SharedToken,
                    fallback_token_exclusion_index: None,
                    candidate_buffer_pool: Some(&candidate_buffer_pool),
                    progress,
                });
        }
        let recall_risk_exceeded = calibration.requires_exact_fallback();
        stats.recall_calibration = calibration;
        if recall_risk_exceeded {
            return Err(AnalysisError::InvalidData(format!(
                "metadata shared-token conservative recall drift exceeds limits: missed duplicate members {}/{}, shifted component members {}/{}; refusing an unbounded whole-group Exact fallback",
                stats.recall_calibration.missed_duplicate_contract_members,
                stats.recall_calibration.exact_duplicate_contract_members,
                stats.recall_calibration.shifted_component_members,
                stats.recall_calibration.exact_component_members,
            )));
        }
        if let Some(progress) = progress {
            progress.finish_calibration();
            progress.update(&stats);
        }
        false
    } else {
        true
    };
    let candidate_index = candidate_index.into_effective_recall(exact_recall);
    let candidate_collection = MetadataCandidateCollectionContext {
        atoms: &atoms,
        compact_docs,
        candidate_index: &candidate_index,
        compatibility: context.template_compatibility,
        exact_recall,
        scope: MetadataCandidateUnionScope::SharedToken,
        contract_tokens: context.contract_tokens,
        fallback_token_exclusion_index: None,
        candidate_buffer_pool: Some(&candidate_buffer_pool),
        estimated_posting_visits_by_left: Some(&work_plan.estimated_posting_visits_by_left),
    };
    let candidate_scratch_pool = MetadataCandidateScratchPool::new(atoms.len());
    let mut candidate_pairs = Vec::with_capacity(METADATA_CONTENT_SCORE_BATCH_PAIRS);
    let difficult_first_lefts = &work_plan.difficult_first_lefts;
    let left_count = difficult_first_lefts.len();
    if parallel {
        let wave_size = context
            .pool
            .current_num_threads()
            .max(1)
            .saturating_mul(METADATA_PARALLEL_LEFT_WAVE_MULTIPLIER);
        let first_wave_end = wave_size.min(left_count);
        let mut left_batches = context.pool.install(|| {
            collect_metadata_left_candidate_wave(
                &difficult_first_lefts[..first_wave_end],
                &candidate_collection,
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
                            &difficult_first_lefts[wave_end..next_wave_end],
                            &candidate_collection,
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
                                scope: MetadataCandidateUnionScope::SharedToken,
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
                scope: MetadataCandidateUnionScope::SharedToken,
            },
        );
        if let Some(progress) = progress {
            progress.update(&stats);
        }
    } else {
        let mut scratch = MetadataCandidateScratch::new(atoms.len());
        let mut pending_progress = 0usize;
        for &left in difficult_first_lefts {
            let left_batch =
                collect_metadata_left_candidate_batch(left, &candidate_collection, &mut scratch);
            MetadataLeftCandidateBatchConsumer {
                atoms: &atoms,
                compact_docs,
                context,
                state,
                stats: &mut stats,
                candidate_pairs: &mut candidate_pairs,
                template_cache_pool,
                scope: MetadataCandidateUnionScope::SharedToken,
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
    Ok(stats)
}

#[cfg(test)]
pub(in super::super) fn union_metadata_no_common_content_candidates(
    records: &[MetadataContentRecord],
    compact_docs: &[CompactMetadataContentDocument],
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
) -> MetadataContentUnionStats {
    let atoms =
        build_metadata_fallback_atoms(records, compact_docs, context.data, context.contract_tokens);
    union_metadata_no_common_atom_core(atoms, compact_docs, context, state, None)
        .expect("test fallback calibration should stay within recall drift limits")
}

pub(in super::super) fn union_metadata_no_common_atom_core(
    atoms: Vec<MetadataContentAtom>,
    compact_docs: &[CompactMetadataContentDocument],
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
    progress: Option<&ProgressTracker>,
) -> Result<MetadataContentUnionStats, AnalysisError> {
    let mut stats = MetadataContentUnionStats {
        atom_count: atoms.len(),
        ..MetadataContentUnionStats::default()
    };
    for atom in &atoms {
        apply_metadata_fallback_atom_internal_unions(atom, context, state);
    }
    if atoms.len() < 2 {
        return Ok(stats);
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
            return Ok(stats);
        }
        let left_atom = &atoms[left];
        let right_atom = &atoms[1];
        if !metadata_fallback_atoms_have_disjoint_token_groups(
            left_atom,
            right_atom,
            context.contract_tokens,
        ) {
            stats.token_overlap_rejected_pairs = 1;
            return Ok(stats);
        }
        stats.candidate_pairs = 1;
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
            return Ok(stats);
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
        return Ok(stats);
    }
    let parallel = atoms.len() >= METADATA_CONTENT_PARALLEL_MIN_RECORDS;
    let conservative_group = context.recall_mode == MetadataRecallMode::Conservative
        && atoms.len() >= METADATA_CONSERVATIVE_MIN_ATOMS;
    let index_recall_mode = if conservative_group {
        MetadataRecallMode::Conservative
    } else {
        MetadataRecallMode::Exact
    };
    let mut candidate_index = if parallel {
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
    let fallback_token_exclusion_index =
        MetadataFallbackTokenExclusionIndex::from_atoms(&atoms, context.contract_tokens);
    let candidate_buffer_pool = Arc::new(MetadataCandidateBufferPool::new(
        atoms.len(),
        context
            .pool
            .current_num_threads()
            .max(1)
            .saturating_mul(METADATA_PARALLEL_LEFT_WAVE_MULTIPLIER),
    ));
    let work_plan = if conservative_group {
        metadata_conservative_calibration_plan_with_work_budget(
            &atoms,
            compact_docs,
            &candidate_index,
            context.template_compatibility,
            context.pool,
        )?
    } else {
        metadata_exact_work_plan(
            &atoms,
            compact_docs,
            &candidate_index,
            context.template_compatibility,
            context.pool,
        )?
    };
    let exact_recall = if conservative_group {
        stats.conservative_groups = 1;
        let mut calibration =
            calibrate_metadata_conservative_recall(MetadataRecallCalibrationRequest {
                atoms: &atoms,
                compact_docs,
                candidate_index: &candidate_index,
                samples: work_plan.samples.clone(),
                estimated_posting_visits_by_left: &work_plan.estimated_posting_visits_by_left,
                context,
                template_cache_pool: &template_cache_pool,
                scope: MetadataCandidateUnionScope::Fallback,
                fallback_token_exclusion_index: Some(&fallback_token_exclusion_index),
                candidate_buffer_pool: Some(&candidate_buffer_pool),
                progress: None,
            });
        if calibration.representative_recall_risk_exceeded() {
            candidate_index.set_conservative_profile(MetadataConservativeRecallProfile::Widened);
            calibration =
                calibrate_metadata_conservative_recall(MetadataRecallCalibrationRequest {
                    atoms: &atoms,
                    compact_docs,
                    candidate_index: &candidate_index,
                    samples: work_plan.samples,
                    estimated_posting_visits_by_left: &work_plan.estimated_posting_visits_by_left,
                    context,
                    template_cache_pool: &template_cache_pool,
                    scope: MetadataCandidateUnionScope::Fallback,
                    fallback_token_exclusion_index: Some(&fallback_token_exclusion_index),
                    candidate_buffer_pool: Some(&candidate_buffer_pool),
                    progress: None,
                });
        }
        let requires_exact_fallback = calibration.representative_recall_risk_exceeded();
        stats.recall_calibration = calibration;
        if requires_exact_fallback {
            stats.exact_fallback_groups = 1;
            return Err(AnalysisError::InvalidData(format!(
                "metadata representative conservative recall drift exceeds limits: missed duplicate members {}/{}, shifted component members {}/{}; rerun with --metadata-recall-mode exact only if the estimated global fallback work is acceptable",
                stats.recall_calibration.missed_duplicate_contract_members,
                stats.recall_calibration.exact_duplicate_contract_members,
                stats.recall_calibration.shifted_component_members,
                stats.recall_calibration.exact_component_members,
            )));
        }
        requires_exact_fallback
    } else {
        true
    };
    let candidate_index = candidate_index.into_effective_recall(exact_recall);
    if let Some(progress) = progress {
        progress.start_task(
            "scoring representative fallback posting work",
            Some(work_plan.estimated_total_posting_visits),
            "posting visits",
        );
    }
    let candidate_collection = MetadataCandidateCollectionContext {
        atoms: &atoms,
        compact_docs,
        candidate_index: &candidate_index,
        compatibility: context.template_compatibility,
        exact_recall,
        scope: MetadataCandidateUnionScope::Fallback,
        contract_tokens: context.contract_tokens,
        fallback_token_exclusion_index: Some(&fallback_token_exclusion_index),
        candidate_buffer_pool: Some(&candidate_buffer_pool),
        estimated_posting_visits_by_left: Some(&work_plan.estimated_posting_visits_by_left),
    };
    let candidate_scratch_pool = MetadataCandidateScratchPool::new(atoms.len());
    let mut candidate_pairs = Vec::with_capacity(METADATA_CONTENT_SCORE_BATCH_PAIRS);
    let difficult_first_lefts = &work_plan.difficult_first_lefts;
    let left_count = difficult_first_lefts.len();
    if parallel {
        let wave_size = context
            .pool
            .current_num_threads()
            .max(1)
            .saturating_mul(METADATA_PARALLEL_LEFT_WAVE_MULTIPLIER);
        let first_wave_end = wave_size.min(left_count);
        let mut left_batches = context.pool.install(|| {
            collect_metadata_left_candidate_wave(
                &difficult_first_lefts[..first_wave_end],
                &candidate_collection,
                &candidate_scratch_pool,
            )
        });
        let mut wave_end = first_wave_end;
        while wave_end < left_count {
            let next_wave_end = wave_end.saturating_add(wave_size).min(left_count);
            let current_left_batches = std::mem::take(&mut left_batches);
            let completed_work = current_left_batches.iter().fold(0u64, |work, batch| {
                work.saturating_add(batch.estimated_posting_visits)
            });
            let (next_left_batches, ()) = context.pool.install(|| {
                rayon::join(
                    || {
                        collect_metadata_left_candidate_wave(
                            &difficult_first_lefts[wave_end..next_wave_end],
                            &candidate_collection,
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
                                template_cache_pool: &template_cache_pool,
                                scope: MetadataCandidateUnionScope::Fallback,
                            },
                        );
                    },
                )
            });
            left_batches = next_left_batches;
            if let Some(progress) = progress {
                progress.advance_task(
                    completed_work,
                    ProgressCounters {
                        candidates: stats.candidate_pairs,
                        scored: stats.scored_pairs,
                        matched: stats.matched_pairs,
                        ..ProgressCounters::default()
                    },
                );
            }
            wave_end = next_wave_end;
        }
        let completed_work = left_batches.iter().fold(0u64, |work, batch| {
            work.saturating_add(batch.estimated_posting_visits)
        });
        consume_metadata_left_candidate_wave(
            left_batches,
            MetadataLeftCandidateBatchConsumer {
                atoms: &atoms,
                compact_docs,
                context,
                state,
                stats: &mut stats,
                candidate_pairs: &mut candidate_pairs,
                template_cache_pool: &template_cache_pool,
                scope: MetadataCandidateUnionScope::Fallback,
            },
        );
        if let Some(progress) = progress {
            progress.advance_task(
                completed_work,
                ProgressCounters {
                    candidates: stats.candidate_pairs,
                    scored: stats.scored_pairs,
                    matched: stats.matched_pairs,
                    ..ProgressCounters::default()
                },
            );
        }
    } else {
        let mut scratch = MetadataCandidateScratch::new(atoms.len());
        let mut pending_work = 0u64;
        let mut pending_lefts = 0u64;
        for &left in difficult_first_lefts {
            let left_batch =
                collect_metadata_left_candidate_batch(left, &candidate_collection, &mut scratch);
            pending_work = pending_work.saturating_add(left_batch.estimated_posting_visits);
            MetadataLeftCandidateBatchConsumer {
                atoms: &atoms,
                compact_docs,
                context,
                state,
                stats: &mut stats,
                candidate_pairs: &mut candidate_pairs,
                template_cache_pool: &template_cache_pool,
                scope: MetadataCandidateUnionScope::Fallback,
            }
            .apply(left_batch);
            pending_lefts = pending_lefts.saturating_add(1);
            if pending_lefts >= 256 {
                if let Some(progress) = progress {
                    progress.advance_task(
                        pending_work,
                        ProgressCounters {
                            candidates: stats.candidate_pairs,
                            scored: stats.scored_pairs,
                            matched: stats.matched_pairs,
                            ..ProgressCounters::default()
                        },
                    );
                }
                pending_work = 0;
                pending_lefts = 0;
            }
        }
        if let Some(progress) = progress {
            progress.advance_task(
                pending_work,
                ProgressCounters {
                    candidates: stats.candidate_pairs,
                    scored: stats.scored_pairs,
                    matched: stats.matched_pairs,
                    ..ProgressCounters::default()
                },
            );
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
    Ok(stats)
}

#[cfg(test)]
pub(in super::super) fn union_metadata_content_candidates(
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

pub(in super::super) fn metadata_pair_already_connected(
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

pub(in super::super) fn lexical_metadata_token_ids(
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
