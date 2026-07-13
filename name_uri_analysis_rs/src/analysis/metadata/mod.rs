use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use duckdb::Connection;

use super::{
    accumulate_pair_component_summary, chain_pair_count, chain_pair_from_index,
    execute_progress_batch, format_byte_size, new_chain_matrix_reuse_states, summary_row,
    total_memory_budget_bytes, AnalysisError, GroupSummary, MetadataRecallMode, NameTotals,
    ProgressCounters, ProgressTracker, SparseUnionFind, SummaryRow, SummarySpec, UnionFind,
    SPARSE_UNION_NODE_BYTES,
};

mod bm25;
mod budget;
mod builder;
mod index;
mod load;
mod parse;
mod summary;
mod types;

#[cfg(test)]
pub(super) use load::metadata_raw_rows_sql;
pub(super) use parse::MAX_METADATA_BYTES_FOR_DEDUP;
pub(crate) use types::{
    MetadataAlgorithmMetrics, MetadataAnalysisResult, MetadataAnalysisSpec,
    MetadataTemplateDocument,
};

use bm25::*;
use budget::*;
use builder::*;
use index::*;
use load::*;
use summary::*;
use types::*;

pub(crate) fn prepare_metadata_compact_tables(
    conn: &Connection,
    progress: &ProgressTracker,
) -> Result<(), AnalysisError> {
    progress.start_stage("preparing compact metadata sources", 1);
    execute_progress_batch(
        conn,
        metadata_contract_token_rows_sql(),
        progress,
        "filtered singleton token IDs and materialized compact sources",
    )?;
    progress.finish_stage("compact metadata sources ready");
    Ok(())
}

fn release_metadata_scoring_state(data: &mut MetadataData) {
    data.metadata_index = InternedMetadataIndex::from_source_doc_entries(Vec::new());
    data.compact_contract_indexes_by_source = Vec::new();
    data.reused_documents = ReusedMetadataDocuments::new();
    for contract in &mut data.contracts {
        contract.content_doc = None;
    }
}

pub(super) fn run_metadata_analysis(
    conn: &Connection,
    chains: &[String],
    totals: &HashMap<String, NameTotals>,
    spec: MetadataAnalysisSpec<'_>,
    progress: &ProgressTracker,
) -> Result<MetadataAnalysisResult, AnalysisError> {
    let MetadataAnalysisSpec {
        threads,
        recall_mode: metadata_recall_mode,
        memory_limit: analysis_memory_limit,
        artifact_directory,
    } = spec;
    progress.start_stage("analyzing metadata duplicates", 7);
    let total_analysis_memory_bytes = total_memory_budget_bytes(analysis_memory_limit)?;
    let analysis_memory_bytes =
        metadata_structure_memory_budget_bytes(total_analysis_memory_bytes, threads)?;
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads.max(1))
        .thread_name(|index| format!("metadata-{index}"))
        .stack_size(METADATA_ANALYSIS_WORKER_STACK_BYTES)
        .build()
        .map_err(|err| AnalysisError::InvalidData(err.to_string()))?;
    let eligible_rows = scalar_u64(conn, "SELECT count(*)::UBIGINT FROM metadata_rows")?;
    let selected_sources = scalar_u64(
        conn,
        "SELECT count(*)::UBIGINT
         FROM analysis_contracts
         WHERE metadata_source_file IS NOT NULL",
    )?;
    let (singleton_tokens_removed, retained_shared_tokens) = conn.query_row(
        "SELECT singleton_token_count, retained_shared_token_count
         FROM metadata_token_stats",
        [],
        |row| Ok((row.get::<_, u64>(0)?, row.get::<_, u64>(1)?)),
    )?;
    let retained_contract_token_rows = scalar_u64(
        conn,
        "SELECT count(*)::UBIGINT FROM metadata_contract_token_rows",
    )?;
    let selected_source_count = usize::try_from(selected_sources).unwrap_or(usize::MAX);
    let retained_contract_token_count =
        usize::try_from(retained_contract_token_rows).unwrap_or(usize::MAX);
    let runtime_reserve_bytes = metadata_runtime_reserve_bytes(
        selected_source_count,
        retained_contract_token_count,
        chains.len(),
    );
    if runtime_reserve_bytes >= analysis_memory_bytes {
        return Err(AnalysisError::InvalidData(format!(
            "metadata union/token/summary state needs about {}, exceeding analysis budget {}",
            format_byte_size(runtime_reserve_bytes),
            format_byte_size(analysis_memory_bytes)
        )));
    }
    let build_overlap_reserve_bytes = metadata_build_overlap_reserve_bytes(selected_source_count);
    let load_transient_reserve_bytes =
        metadata_load_transient_reserve_bytes(analysis_memory_bytes, chains)?;
    let concurrent_load_reserve_bytes =
        build_overlap_reserve_bytes.saturating_add(load_transient_reserve_bytes);
    if concurrent_load_reserve_bytes >= analysis_memory_bytes {
        return Err(AnalysisError::InvalidData(format!(
            "metadata mapping/load transient state needs about {}, exceeding analysis budget {}",
            format_byte_size(concurrent_load_reserve_bytes),
            format_byte_size(analysis_memory_bytes)
        )));
    }
    progress.step_stage(format!(
        "planned metadata memory for {selected_sources} sources and {retained_contract_token_rows} shared-token memberships"
    ));
    let cache_budget_bytes = analysis_memory_bytes
        .saturating_div(METADATA_REUSE_CACHE_BUDGET_DIVISOR)
        .min(analysis_memory_bytes.saturating_sub(runtime_reserve_bytes))
        .min(analysis_memory_bytes.saturating_sub(concurrent_load_reserve_bytes));
    progress.start_task("loading reused metadata documents", None, "documents");
    let reused_documents = load_reused_metadata_documents(
        conn,
        &pool,
        Some(cache_budget_bytes),
        load_transient_reserve_bytes,
        Some(progress),
    )?;
    progress.finish_task(format!(
        "loaded {} reused metadata documents",
        reused_documents.len()
    ));
    progress.step_stage("loaded reused metadata cache");
    let reused_cache_bytes = reused_metadata_documents_memory_bytes(&reused_documents);
    let reused_raw_json_cache_entries = reused_documents.len() as u64;
    let builder_peak_budget_bytes = metadata_builder_peak_budget_bytes(
        analysis_memory_bytes,
        build_overlap_reserve_bytes,
        reused_cache_bytes,
        load_transient_reserve_bytes,
    )?;
    progress.start_task(
        "loading and interning metadata documents",
        None,
        "contracts",
    );
    let mut data = load_metadata_data(
        conn,
        chains,
        &pool,
        reused_documents,
        MetadataLoadBudgets::new(builder_peak_budget_bytes, load_transient_reserve_bytes),
        Some(progress),
    )?;
    let contract_token_reserve_bytes =
        metadata_contract_token_reserve_bytes(data.contracts.len(), retained_contract_token_count);
    let pre_token_resident_budget = metadata_pre_token_resident_budget_bytes(
        analysis_memory_bytes,
        contract_token_reserve_bytes,
    )?;
    let mut pre_token_resident_bytes = metadata_resident_memory_bytes(&data, None, chains.len());
    pre_token_resident_bytes = remap_metadata_index_for_resident_budget(
        &mut data,
        pre_token_resident_bytes,
        pre_token_resident_budget,
        artifact_directory,
    )?;
    if pre_token_resident_bytes > pre_token_resident_budget {
        return Err(AnalysisError::InvalidData(format!(
            "metadata resident state needs about {} before loading contract tokens, leaving less than the required {} token reserve inside analysis budget {}",
            format_byte_size(pre_token_resident_bytes),
            format_byte_size(contract_token_reserve_bytes),
            format_byte_size(analysis_memory_bytes)
        )));
    }
    progress.finish_task(format!(
        "loaded {} metadata documents for {} contracts",
        data.metadata_index.doc_count(),
        data.contracts.len()
    ));
    progress.step_stage("loaded metadata document index");
    progress.start_task(
        "loading contract-token CSR in two passes",
        Some(retained_contract_token_rows.saturating_mul(2)),
        "memberships",
    );
    let contract_tokens = load_metadata_contract_tokens(conn, &data, &pool, Some(progress))?;
    progress.finish_task(format!(
        "loaded {retained_contract_token_rows} shared-token memberships in two passes"
    ));
    progress.step_stage("loaded contract-token CSR");
    let template_document_count = data.metadata_index.doc_count() as u64;
    let content_document_count = data
        .contracts
        .iter()
        .filter(|contract| contract.content_doc.is_some())
        .count() as u64;
    let mut resident_bytes =
        metadata_resident_memory_bytes(&data, Some(&contract_tokens), chains.len());
    resident_bytes = remap_metadata_index_for_resident_budget(
        &mut data,
        resident_bytes,
        analysis_memory_bytes,
        artifact_directory,
    )?;
    if resident_bytes > analysis_memory_bytes {
        return Err(AnalysisError::InvalidData(format!(
            "metadata resident state needs about {}, exceeding analysis budget {}",
            format_byte_size(resident_bytes),
            format_byte_size(analysis_memory_bytes)
        )));
    }
    let mapped_index_bytes = u64::try_from(data.metadata_index.mapped_bytes()).unwrap_or(u64::MAX);
    let mut rows = Vec::new();
    if data.contracts.len() < 2 || data.metadata_index.is_empty() {
        push_empty_metadata_rows(&mut rows, chains, totals);
        progress.step_stage("shared-token scoring skipped");
        progress.step_stage("representative fallback skipped");
        progress.step_stage("metadata rows summarized");
        progress.finish_stage("metadata analysis complete");
        return Ok(MetadataAnalysisResult {
            rows,
            metrics: MetadataAlgorithmMetrics {
                recall_mode: metadata_recall_mode,
                eligible_rows,
                selected_sources,
                reused_raw_json_cache_entries,
                singleton_tokens_removed,
                retained_shared_tokens,
                template_documents: template_document_count,
                content_documents: content_document_count,
                template_candidate_pairs: 0,
                template_scored_pairs: 0,
                template_matched_pairs: 0,
                content_atoms: 0,
                content_raw_candidate_pairs: 0,
                content_dimension_rejected_pairs: 0,
                content_candidate_pairs: 0,
                content_already_connected_pairs: 0,
                content_scored_pairs: 0,
                template_rejected_pairs: 0,
                template_cache_hits: 0,
                template_cache_misses: 0,
                template_batch_unique_pairs: 0,
                template_batch_reused_pairs: 0,
                conservative_groups: 0,
                exact_fallback_groups: 0,
                recall_sampled_left_atoms: 0,
                recall_exact_candidate_pairs: 0,
                recall_conservative_candidate_pairs: 0,
                recall_exact_matched_pairs: 0,
                recall_missed_matched_pairs: 0,
                recall_exact_duplicate_contract_members: 0,
                recall_missed_duplicate_contract_members: 0,
                recall_exact_component_members: 0,
                recall_shifted_component_members: 0,
                mmap_bytes: mapped_index_bytes,
                dsu_bytes: 0,
            },
        });
    }

    let mut state = MetadataUnionState {
        intra: UnionFind::new(data.contracts.len()),
        cross: (chains.len() > 1).then(SparseUnionFind::default),
        chain_matrix: (chains.len() > 1)
            .then(|| new_chain_matrix_reuse_states(chain_pair_count(chains.len()))),
    };
    let maximum_shared_working_bytes = analysis_memory_bytes - resident_bytes;
    let mut content_stats = MetadataContentUnionStats::default();
    progress.start_task(
        "matching shared-token memberships",
        Some(retained_contract_token_rows),
        "memberships",
    );
    {
        let content_context = MetadataContentUnionContext {
            data: &data,
            template_compatibility: MetadataTemplateCompatibility::Scored(
                &data.metadata_index.scoring,
            ),
            contract_tokens: &contract_tokens,
            chain_count: chains.len(),
            pool: &pool,
            recall_mode: metadata_recall_mode,
        };
        let shared_stats = union_metadata_token_content_matches(
            conn,
            &content_context,
            &mut state,
            maximum_shared_working_bytes,
            metadata_recall_mode,
            progress,
        )?;
        content_stats.accumulate(shared_stats);
    }
    let contract_drift_percent = if content_stats
        .recall_calibration
        .exact_duplicate_contract_members
        == 0
    {
        0.0
    } else {
        content_stats
            .recall_calibration
            .missed_duplicate_contract_members as f64
            * 100.0
            / content_stats
                .recall_calibration
                .exact_duplicate_contract_members as f64
    };
    let component_drift_percent = if content_stats.recall_calibration.exact_component_members == 0 {
        0.0
    } else {
        content_stats.recall_calibration.shifted_component_members as f64 * 100.0
            / content_stats.recall_calibration.exact_component_members as f64
    };
    progress.finish_task(format!(
        "shared-token matching complete; mode {:?}; calibrated-lefts {}; sample drift before fallback contracts/components {:.3}%/{:.3}%; conservative/fallback groups {}/{}; raw {}; dimension-rejected {}; candidates {}; connected-skips {}; template batch unique/reused {}/{}; template-rejected {}; cache {}/{} hit/miss; content-scored {}; matched {}",
        metadata_recall_mode,
        content_stats.recall_calibration.sampled_left_atoms,
        contract_drift_percent,
        component_drift_percent,
        content_stats.conservative_groups,
        content_stats.exact_fallback_groups,
        content_stats.raw_candidate_pairs,
        content_stats.dimension_rejected_pairs,
        content_stats.candidate_pairs,
        content_stats.already_connected_pairs,
        content_stats.template_batch_unique_pairs,
        content_stats.template_batch_reused_pairs,
        content_stats.template_rejected_pairs,
        content_stats.template_cache_hits,
        content_stats.template_cache_misses,
        content_stats.scored_pairs,
        content_stats.matched_pairs
    ));
    progress.step_stage("matched shared-token metadata groups");
    drop(std::mem::take(&mut data.reused_documents));
    let fallback_resident_bytes =
        metadata_resident_memory_bytes(&data, Some(&contract_tokens), chains.len());
    if fallback_resident_bytes > analysis_memory_bytes {
        return Err(AnalysisError::InvalidData(format!(
            "metadata resident state needs about {} after releasing the reuse cache, exceeding analysis budget {}",
            format_byte_size(fallback_resident_bytes),
            format_byte_size(analysis_memory_bytes)
        )));
    }
    let maximum_fallback_working_bytes = analysis_memory_bytes - fallback_resident_bytes;
    {
        let content_context = MetadataContentUnionContext {
            data: &data,
            template_compatibility: MetadataTemplateCompatibility::Scored(
                &data.metadata_index.scoring,
            ),
            contract_tokens: &contract_tokens,
            chain_count: chains.len(),
            pool: &pool,
            recall_mode: MetadataRecallMode::Exact,
        };
        content_stats.accumulate(union_metadata_representative_content_fallback(
            &content_context,
            &mut state,
            maximum_fallback_working_bytes,
            progress,
        )?);
    }
    progress.step_stage("matched representative metadata fallback");
    drop(contract_tokens);
    drop(pool);
    release_metadata_scoring_state(&mut data);
    let summary_peak_bytes = metadata_summary_peak_memory_bytes(&data, &state);
    if summary_peak_bytes > analysis_memory_bytes {
        return Err(AnalysisError::InvalidData(format!(
            "metadata summary peak needs about {}, exceeding analysis budget {}",
            format_byte_size(summary_peak_bytes),
            format_byte_size(analysis_memory_bytes)
        )));
    }
    let summary_units = chains.len() as u64 + chain_pair_count(chains.len()) as u64 * 2;
    progress.start_task(
        "summarizing metadata components",
        Some(summary_units),
        "summaries",
    );
    push_metadata_summary_rows(&mut rows, &data, chains, totals, &mut state);
    progress.advance_task(summary_units, ProgressCounters::default());
    progress.finish_task("metadata component summaries ready");
    progress.step_stage("metadata rows summarized");
    progress.finish_stage("metadata analysis complete");
    let dsu_bytes = metadata_union_state_bytes(&state);
    Ok(MetadataAnalysisResult {
        rows,
        metrics: MetadataAlgorithmMetrics {
            recall_mode: metadata_recall_mode,
            eligible_rows,
            selected_sources,
            reused_raw_json_cache_entries,
            singleton_tokens_removed,
            retained_shared_tokens,
            template_documents: template_document_count,
            content_documents: content_document_count,
            template_candidate_pairs: content_stats.template_candidate_pairs,
            template_scored_pairs: content_stats.template_scored_pairs,
            template_matched_pairs: content_stats.template_matched_pairs,
            content_atoms: content_stats.atom_count as u64,
            content_raw_candidate_pairs: content_stats.raw_candidate_pairs,
            content_dimension_rejected_pairs: content_stats.dimension_rejected_pairs,
            content_candidate_pairs: content_stats.candidate_pairs,
            content_already_connected_pairs: content_stats.already_connected_pairs,
            content_scored_pairs: content_stats.scored_pairs,
            template_rejected_pairs: content_stats.template_rejected_pairs,
            template_cache_hits: content_stats.template_cache_hits,
            template_cache_misses: content_stats.template_cache_misses,
            template_batch_unique_pairs: content_stats.template_batch_unique_pairs,
            template_batch_reused_pairs: content_stats.template_batch_reused_pairs,
            conservative_groups: content_stats.conservative_groups,
            exact_fallback_groups: content_stats.exact_fallback_groups,
            recall_sampled_left_atoms: content_stats.recall_calibration.sampled_left_atoms,
            recall_exact_candidate_pairs: content_stats.recall_calibration.exact_candidate_pairs,
            recall_conservative_candidate_pairs: content_stats
                .recall_calibration
                .conservative_candidate_pairs,
            recall_exact_matched_pairs: content_stats.recall_calibration.exact_matched_pairs,
            recall_missed_matched_pairs: content_stats.recall_calibration.missed_matched_pairs,
            recall_exact_duplicate_contract_members: content_stats
                .recall_calibration
                .exact_duplicate_contract_members,
            recall_missed_duplicate_contract_members: content_stats
                .recall_calibration
                .missed_duplicate_contract_members,
            recall_exact_component_members: content_stats
                .recall_calibration
                .exact_component_members,
            recall_shifted_component_members: content_stats
                .recall_calibration
                .shifted_component_members,
            mmap_bytes: mapped_index_bytes,
            dsu_bytes,
        },
    })
}

fn scalar_u64(conn: &Connection, sql: &str) -> Result<u64, AnalysisError> {
    Ok(conn.query_row(sql, [], |row| row.get(0))?)
}

#[cfg(test)]
mod tests;
