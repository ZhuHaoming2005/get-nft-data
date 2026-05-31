use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use futures::{stream, StreamExt};
use serde::Deserialize;
use tokio::sync::Semaphore;

use crate::error::AppError;
use crate::models::{BatchSummaryPayload, SingleReportPayload};
use crate::progress::SeedProgressReporter;
use crate::reporting::{default_output_basename, write_outputs_to_directory};

use super::summary::build_batch_seed_aggregate;
use super::{
    acquire_optional_limit, analyze_matched_contracts_parallel, build_candidate_plan_from_snapshot,
    fetch_seed_context, finalize_seed_report, prepare_seed_analysis_state,
    seed_nfts_for_duplicate_matching, AnalysisDeps, AnalyzeRequest, BatchRequest,
    BatchSeedAggregate, CandidatePlan, SeedContext,
};

fn analyze_request_for_batch_seed(
    request: &BatchRequest,
    seed_contract_address: String,
) -> AnalyzeRequest {
    AnalyzeRequest {
        chain: request.chain.clone(),
        seed_contract_address,
        alchemy_api_key: request.alchemy_api_key.clone(),
        alchemy_network: request.alchemy_network.clone(),
        etherscan_api_key: request.etherscan_api_key.clone(),
        opensea_api_key: request.opensea_api_key.clone(),
        name_threshold: request.name_threshold,
        metadata_threshold: request.metadata_threshold,
        timeout_seconds: request.timeout_seconds,
        api_max_concurrency: request.api_max_concurrency,
        matched_contract_max_concurrency: request.matched_contract_max_concurrency,
        max_tokens_per_contract: request.max_tokens_per_contract,
        max_recall_rows: request.max_recall_rows,
        paper_stats_config: request.paper_stats_config,
    }
}

#[derive(Deserialize)]
struct CachedSingleSeedReport {
    seed_contract: crate::models::SeedContractPayload,
    paper_stats: crate::models::PaperStatsPayload,
}

struct PreparedSeedContext {
    seed_address: String,
    request: AnalyzeRequest,
    context: SeedContext,
    seed_progress: Arc<dyn SeedProgressReporter>,
}

struct PreparedSeedPlan {
    seed_address: String,
    request: AnalyzeRequest,
    context: SeedContext,
    plan: CandidatePlan,
    seed_progress: Arc<dyn SeedProgressReporter>,
}

fn load_cached_seed_entries(
    seed_addresses: &[String],
    chain: &str,
    output_dir: &Path,
) -> Result<BTreeMap<String, BatchSeedAggregate>, AppError> {
    if !output_dir.exists() {
        return Ok(BTreeMap::new());
    }

    let requested_seeds: BTreeSet<String> = seed_addresses.iter().cloned().collect();
    let mut cached_entries = BTreeMap::new();
    for entry in std::fs::read_dir(output_dir)? {
        let path = entry?.path();
        if path
            .extension()
            .and_then(|extension| extension.to_str())
            .is_none_or(|extension| !extension.eq_ignore_ascii_case("json"))
        {
            continue;
        }

        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) else {
            continue;
        };
        if value.get("report_type").and_then(|value| value.as_str()) != Some("single_seed") {
            continue;
        }
        if value.get("paper_stats").is_none() {
            continue;
        }

        let Ok(report) = serde_json::from_value::<CachedSingleSeedReport>(value) else {
            continue;
        };
        let seed_address = report.seed_contract.contract_address.trim().to_lowercase();
        if !requested_seeds.contains(&seed_address) {
            continue;
        }
        let report_chain = report.seed_contract.chain.trim();
        if !report_chain.is_empty() && !report_chain.eq_ignore_ascii_case(chain) {
            continue;
        }

        let canonical_payload = SingleReportPayload {
            seed_contract: report.seed_contract.clone(),
            ..SingleReportPayload::default()
        };
        let canonical_name = format!("{}.json", default_output_basename(&canonical_payload));
        if path.file_name().and_then(|name| name.to_str()) != Some(canonical_name.as_str()) {
            continue;
        }

        let payload = SingleReportPayload {
            seed_contract: report.seed_contract,
            paper_stats: report.paper_stats,
            ..SingleReportPayload::default()
        };
        cached_entries.insert(seed_address, build_batch_seed_aggregate(payload));
    }
    Ok(cached_entries)
}

async fn build_candidate_plan_for_prepared_seed(
    prepared: PreparedSeedContext,
    feature_store: Arc<dyn super::FeatureStoreReader>,
    cpu_limit: Arc<Semaphore>,
) -> Result<PreparedSeedPlan, AppError> {
    let PreparedSeedContext {
        seed_address,
        request,
        context,
        seed_progress,
    } = prepared;
    let dedup_seed_nfts =
        seed_nfts_for_duplicate_matching(&context.seed_nfts, &context.seed_contract);
    let snapshot_seed_nfts = dedup_seed_nfts.clone();
    let chain = request.chain.clone();
    let name_threshold = request.name_threshold;
    let metadata_threshold = request.metadata_threshold;
    let max_tokens_per_contract = request.max_tokens_per_contract;
    let max_recall_rows = request.max_recall_rows;
    seed_progress.on_seed_stage("load_snapshot").await;
    let _permit = acquire_optional_limit(&Some(cpu_limit)).await?;
    let snapshot = tokio::task::spawn_blocking(move || {
        feature_store.load_snapshot(
            &chain,
            &snapshot_seed_nfts,
            name_threshold,
            metadata_threshold,
            max_tokens_per_contract,
            max_recall_rows,
        )
    })
    .await
    .map_err(|err| AppError::InvalidData(format!("candidate CPU task failed: {err}")))??;
    seed_progress
        .on_seed_stage("find_duplicate_candidates")
        .await;
    let (request, plan) = tokio::task::spawn_blocking(move || {
        let plan = build_candidate_plan_from_snapshot(&request, &dedup_seed_nfts, snapshot);
        (request, plan)
    })
    .await
    .map_err(|err| AppError::InvalidData(format!("candidate CPU task failed: {err}")))?;
    Ok(PreparedSeedPlan {
        seed_address,
        request,
        context,
        plan,
        seed_progress,
    })
}

pub async fn run_batch(
    request: BatchRequest,
    deps: &AnalysisDeps,
) -> Result<BatchSummaryPayload, AppError> {
    let seed_addresses = read_seed_addresses(&request.seed_file)?;
    let mut seed_aggregates = Vec::new();
    let cached_entries =
        load_cached_seed_entries(&seed_addresses, &request.chain, &request.output_dir)?;
    for seed_address in &seed_addresses {
        if let Some(aggregate) = cached_entries.get(seed_address).cloned() {
            deps.batch_progress.on_seed_cached(seed_address);
            seed_aggregates.push(aggregate);
        }
    }
    let pending_seeds: Vec<String> = seed_addresses
        .iter()
        .filter(|seed_address| !cached_entries.contains_key(seed_address.as_str()))
        .cloned()
        .collect();

    let seed_network_max_concurrency = request.seed_network_max_concurrency.max(1);
    let seed_cpu_max_concurrency = request.seed_cpu_max_concurrency.max(1);
    let cpu_limit = Arc::new(Semaphore::new(seed_cpu_max_concurrency));
    let seed_network_limit = Arc::new(Semaphore::new(seed_network_max_concurrency));
    let matched_contract_limit = Arc::new(Semaphore::new(
        request.matched_contract_max_concurrency.max(1),
    ));
    let mut fresh_entries = Vec::new();
    let mut first_error: Option<AppError> = None;
    let output_dir = request.output_dir.clone();
    let pending_seed_count = pending_seeds.len().max(1);
    let seed_pipeline_max_concurrency = seed_network_max_concurrency
        .saturating_add(seed_cpu_max_concurrency)
        .saturating_add(request.matched_contract_max_concurrency.max(1))
        .min(pending_seed_count)
        .max(1);
    let mut seed_tasks = stream::iter(pending_seeds.into_iter().map(|seed_address| {
        let per_seed_request = analyze_request_for_batch_seed(&request, seed_address.clone());
        let deps = deps.clone();
        let seed_network_limit = seed_network_limit.clone();
        let cpu_limit = cpu_limit.clone();
        let matched_contract_limit = matched_contract_limit.clone();
        let output_dir = output_dir.clone();
        async move {
            let batch_progress = deps.batch_progress.clone();
            let seed_progress = batch_progress.create_seed_reporter(&seed_address);
            let context = {
                let _permit = acquire_optional_limit(&Some(seed_network_limit.clone())).await?;
                batch_progress.on_seed_started(&seed_address);
                match fetch_seed_context(&per_seed_request, &deps, seed_progress.clone()).await {
                    Ok(context) => context,
                    Err(err) => {
                        batch_progress.on_seed_failed(&seed_address, &err.to_string());
                        return Err(err);
                    }
                }
            };
            let prepared_context = PreparedSeedContext {
                seed_address,
                request: per_seed_request,
                context,
                seed_progress,
            };
            let seed_address = prepared_context.seed_address.clone();
            let prepared = match build_candidate_plan_for_prepared_seed(
                prepared_context,
                deps.feature_store.clone(),
                cpu_limit,
            )
            .await
            {
                Ok(prepared) => prepared,
                Err(err) => {
                    batch_progress.on_seed_failed(&seed_address, &err.to_string());
                    return Err(err);
                }
            };
            let seed_progress = prepared.seed_progress.clone();
            let state = prepare_seed_analysis_state(
                prepared.request,
                &deps,
                seed_progress.clone(),
                None,
                Some((prepared.context, prepared.plan)),
            )
            .await?;
            let mut state = state;
            let result = match analyze_matched_contracts_parallel(
                &mut state,
                &deps,
                seed_progress.clone(),
                matched_contract_limit,
            )
            .await
            {
                Ok(()) => finalize_seed_report(state, seed_progress).await,
                Err(err) => Err(err),
            };
            let payload = match result {
                Ok(payload) => payload,
                Err(err) => {
                    batch_progress.on_seed_failed(&prepared.seed_address, &err.to_string());
                    return Err(err);
                }
            };
            batch_progress.on_seed_finished(&prepared.seed_address);
            write_outputs_to_directory(&payload, &output_dir)?;
            let aggregate = build_batch_seed_aggregate(payload);
            Ok(aggregate)
        }
    }))
    .buffer_unordered(seed_pipeline_max_concurrency);
    while let Some(entry) = seed_tasks.next().await {
        match entry {
            Ok(aggregate) => fresh_entries.push(aggregate),
            Err(err) => {
                if first_error.is_none() {
                    first_error = Some(err);
                }
            }
        }
    }

    seed_aggregates.extend(fresh_entries);
    if let Some(err) = first_error {
        return Err(err);
    }
    let mut aggregates_by_seed: BTreeMap<String, BatchSeedAggregate> = seed_aggregates
        .into_iter()
        .map(|aggregate| (aggregate.seed_contract.contract_address.clone(), aggregate))
        .collect();
    let seed_aggregates: Vec<BatchSeedAggregate> = seed_addresses
        .iter()
        .filter_map(|seed| aggregates_by_seed.remove(seed))
        .collect();
    Ok(BatchSummaryPayload {
        paper_stats: super::paper_stats::merge_paper_stats(
            seed_aggregates
                .iter()
                .map(|aggregate| &aggregate.paper_stats),
            request.paper_stats_config,
        ),
        ..BatchSummaryPayload::default()
    })
}

pub fn read_seed_addresses(seed_file: &Path) -> Result<Vec<String>, AppError> {
    let content = std::fs::read_to_string(seed_file)?;
    Ok(content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(|line| line.to_lowercase())
        .collect())
}
