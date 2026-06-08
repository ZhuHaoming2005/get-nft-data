use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use futures::{stream, StreamExt};
use serde::Deserialize;
use tokio::sync::{mpsc, Semaphore};

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

const SNAPSHOT_BATCH_DEBOUNCE_MS: u64 = 50;

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

struct PreparedSeedPlanFailure {
    seed_address: String,
    error: AppError,
}

struct SnapshotPlanInput {
    seed_address: String,
    request: AnalyzeRequest,
    context: SeedContext,
    dedup_seed_nfts: Vec<crate::models::SeedNft>,
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

        let mut seed_contract = report.seed_contract;
        seed_contract.contract_address = seed_address.clone();
        let payload = SingleReportPayload {
            seed_contract,
            paper_stats: report.paper_stats,
            ..SingleReportPayload::default()
        };
        cached_entries.insert(seed_address, build_batch_seed_aggregate(payload));
    }
    Ok(cached_entries)
}

async fn build_candidate_plans_for_prepared_seeds(
    prepared: Vec<PreparedSeedContext>,
    feature_store: Arc<dyn super::FeatureStoreReader>,
    cpu_limit: Arc<Semaphore>,
) -> Result<Vec<Result<PreparedSeedPlan, PreparedSeedPlanFailure>>, AppError> {
    if prepared.is_empty() {
        return Ok(Vec::new());
    }

    let mut inputs = Vec::with_capacity(prepared.len());
    for prepared in prepared {
        let PreparedSeedContext {
            seed_address,
            request,
            context,
            seed_progress,
        } = prepared;
        let dedup_seed_nfts =
            seed_nfts_for_duplicate_matching(&context.seed_nfts, &context.seed_contract);
        seed_progress.on_seed_stage("load_snapshot").await;
        inputs.push(SnapshotPlanInput {
            seed_address,
            request,
            context,
            dedup_seed_nfts,
            seed_progress,
        });
    }

    let chain = inputs[0].request.chain.clone();
    let name_threshold = inputs[0].request.name_threshold;
    let metadata_threshold = inputs[0].request.metadata_threshold;
    let max_tokens_per_contract = inputs[0].request.max_tokens_per_contract;
    let max_recall_rows = inputs[0].request.max_recall_rows;
    let _permit = acquire_optional_limit(&Some(cpu_limit)).await?;
    let snapshot_results = tokio::task::spawn_blocking(move || {
        let snapshot_inputs = inputs
            .iter()
            .map(|input| (input.seed_address.clone(), input.dedup_seed_nfts.clone()))
            .collect::<Vec<_>>();
        let mut snapshots_by_seed: BTreeMap<
            String,
            Result<crate::models::DatabaseSnapshot, AppError>,
        > = if snapshot_inputs.len() > 1 {
            match feature_store.load_snapshots(
                &chain,
                &snapshot_inputs,
                name_threshold,
                metadata_threshold,
                max_tokens_per_contract,
                max_recall_rows,
            ) {
                Ok(snapshots) => snapshots
                    .into_iter()
                    .map(|(seed_address, snapshot)| (seed_address, Ok(snapshot)))
                    .collect(),
                Err(err) => {
                    eprintln!(
                        "warning: batched snapshot load failed: {err}; falling back to per-seed snapshot loads"
                    );
                    snapshot_inputs
                        .iter()
                        .map(|(seed_address, seed_nfts)| {
                            let snapshot = feature_store.load_snapshot(
                                &chain,
                                seed_nfts,
                                name_threshold,
                                metadata_threshold,
                                max_tokens_per_contract,
                                max_recall_rows,
                            );
                            (seed_address.clone(), snapshot)
                        })
                        .collect()
                }
            }
        } else {
            snapshot_inputs
                .iter()
                .map(|(seed_address, seed_nfts)| {
                    let snapshot = feature_store.load_snapshot(
                        &chain,
                        seed_nfts,
                        name_threshold,
                        metadata_threshold,
                        max_tokens_per_contract,
                        max_recall_rows,
                    );
                    (seed_address.clone(), snapshot)
                })
                .collect()
        };

        let mut snapshot_results = Vec::with_capacity(inputs.len());
        for input in inputs {
            let snapshot = snapshots_by_seed.remove(&input.seed_address).unwrap_or_else(|| {
                Err(AppError::InvalidData(format!(
                    "batched snapshot load did not return seed {}",
                    input.seed_address
                )))
            });
            snapshot_results.push((input, snapshot));
        }
        snapshot_results
    })
    .await
    .map_err(|err| AppError::InvalidData(format!("candidate CPU task failed: {err}")))?;

    let mut build_inputs = Vec::new();
    let mut plans = Vec::with_capacity(snapshot_results.len());
    for (input, snapshot) in snapshot_results {
        match snapshot {
            Ok(snapshot) => {
                input
                    .seed_progress
                    .on_seed_stage("find_duplicate_candidates")
                    .await;
                build_inputs.push((input, snapshot));
            }
            Err(error) => {
                plans.push(Err(PreparedSeedPlanFailure {
                    seed_address: input.seed_address,
                    error,
                }));
            }
        }
    }

    let mut built_plans = tokio::task::spawn_blocking(move || {
        build_inputs
            .into_iter()
            .map(|(input, snapshot)| {
                let plan = build_candidate_plan_from_snapshot(
                    &input.request,
                    &input.dedup_seed_nfts,
                    snapshot,
                );
                Ok(PreparedSeedPlan {
                    seed_address: input.seed_address,
                    request: input.request,
                    context: input.context,
                    plan,
                    seed_progress: input.seed_progress,
                })
            })
            .collect::<Vec<_>>()
    })
    .await
    .map_err(|err| AppError::InvalidData(format!("candidate CPU task failed: {err}")))?;
    plans.append(&mut built_plans);
    Ok(plans)
}

async fn run_prepared_seed_plans(
    successful_plans: Vec<PreparedSeedPlan>,
    deps: &AnalysisDeps,
    matched_contract_limit: Arc<Semaphore>,
    output_dir: &Path,
    max_concurrency: usize,
) -> Vec<Result<BatchSeedAggregate, AppError>> {
    let mut seed_tasks = stream::iter(successful_plans.into_iter().map(|prepared| {
        let deps = deps.clone();
        let matched_contract_limit = matched_contract_limit.clone();
        let output_dir = output_dir.to_path_buf();
        async move {
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
                    deps.batch_progress
                        .on_seed_failed(&prepared.seed_address, &err.to_string());
                    return Err(err);
                }
            };
            deps.batch_progress.on_seed_finished(&prepared.seed_address);
            write_outputs_to_directory(&payload, &output_dir)?;
            let aggregate = build_batch_seed_aggregate(payload);
            Ok(aggregate)
        }
    }))
    .buffer_unordered(max_concurrency);

    let mut results = Vec::new();
    while let Some(entry) = seed_tasks.next().await {
        results.push(entry);
    }
    results
}

async fn process_prepared_context_batch(
    ready_contexts: &mut Vec<PreparedSeedContext>,
    deps: &AnalysisDeps,
    cpu_limit: Arc<Semaphore>,
    matched_contract_limit: Arc<Semaphore>,
    output_dir: &Path,
    seed_pipeline_max_concurrency: usize,
) -> Result<(Vec<BatchSeedAggregate>, Option<AppError>), AppError> {
    if ready_contexts.is_empty() {
        return Ok((Vec::new(), None));
    }

    let prepared_contexts = std::mem::take(ready_contexts);
    let prepared_plans = build_candidate_plans_for_prepared_seeds(
        prepared_contexts,
        deps.feature_store.clone(),
        cpu_limit,
    )
    .await?;
    let mut successful_plans = Vec::new();
    let mut first_error = None;
    for prepared in prepared_plans {
        match prepared {
            Ok(prepared) => successful_plans.push(prepared),
            Err(err) => {
                deps.batch_progress
                    .on_seed_failed(&err.seed_address, &err.error.to_string());
                if first_error.is_none() {
                    first_error = Some(err.error);
                }
            }
        }
    }

    let mut fresh_entries = Vec::new();
    for result in run_prepared_seed_plans(
        successful_plans,
        deps,
        matched_contract_limit,
        output_dir,
        seed_pipeline_max_concurrency,
    )
    .await
    {
        match result {
            Ok(aggregate) => fresh_entries.push(aggregate),
            Err(err) => {
                if first_error.is_none() {
                    first_error = Some(err);
                }
            }
        }
    }

    Ok((fresh_entries, first_error))
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
    let (context_tx, mut context_rx) =
        mpsc::channel::<Result<PreparedSeedContext, AppError>>(seed_pipeline_max_concurrency);
    let context_request = request.clone();
    let context_deps = deps.clone();
    let context_seed_network_limit = seed_network_limit.clone();
    let context_handle = tokio::spawn(async move {
        let mut context_tasks = stream::iter(pending_seeds.into_iter().map(|seed_address| {
            let per_seed_request =
                analyze_request_for_batch_seed(&context_request, seed_address.clone());
            let deps = context_deps.clone();
            let seed_network_limit = context_seed_network_limit.clone();
            async move {
                let batch_progress = deps.batch_progress.clone();
                let seed_progress = batch_progress.create_seed_reporter(&seed_address);
                let _permit = acquire_optional_limit(&Some(seed_network_limit)).await?;
                batch_progress.on_seed_started(&seed_address);
                match fetch_seed_context(&per_seed_request, &deps, seed_progress.clone()).await {
                    Ok(context) => Ok(PreparedSeedContext {
                        seed_address,
                        request: per_seed_request,
                        context,
                        seed_progress,
                    }),
                    Err(err) => {
                        batch_progress.on_seed_failed(&seed_address, &err.to_string());
                        Err(err)
                    }
                }
            }
        }))
        .buffer_unordered(seed_network_max_concurrency);
        while let Some(result) = context_tasks.next().await {
            if context_tx.send(result).await.is_err() {
                break;
            }
        }
    });

    let snapshot_batch_debounce = std::time::Duration::from_millis(SNAPSHOT_BATCH_DEBOUNCE_MS);
    let mut ready_contexts = Vec::new();
    loop {
        let next_entry = if ready_contexts.is_empty() {
            context_rx.recv().await
        } else {
            match tokio::time::timeout(snapshot_batch_debounce, context_rx.recv()).await {
                Ok(entry) => entry,
                Err(_) => {
                    let (entries, err) = process_prepared_context_batch(
                        &mut ready_contexts,
                        deps,
                        cpu_limit.clone(),
                        matched_contract_limit.clone(),
                        &output_dir,
                        seed_pipeline_max_concurrency,
                    )
                    .await?;
                    fresh_entries.extend(entries);
                    if first_error.is_none() {
                        first_error = err;
                    }
                    continue;
                }
            }
        };

        match next_entry {
            Some(Ok(prepared)) => {
                ready_contexts.push(prepared);
                if ready_contexts.len() >= seed_pipeline_max_concurrency {
                    let (entries, err) = process_prepared_context_batch(
                        &mut ready_contexts,
                        deps,
                        cpu_limit.clone(),
                        matched_contract_limit.clone(),
                        &output_dir,
                        seed_pipeline_max_concurrency,
                    )
                    .await?;
                    fresh_entries.extend(entries);
                    if first_error.is_none() {
                        first_error = err;
                    }
                }
            }
            Some(Err(err)) => {
                if first_error.is_none() {
                    first_error = Some(err);
                }
            }
            None => break,
        }
    }

    let (entries, err) = process_prepared_context_batch(
        &mut ready_contexts,
        deps,
        cpu_limit.clone(),
        matched_contract_limit.clone(),
        &output_dir,
        seed_pipeline_max_concurrency,
    )
    .await?;
    fresh_entries.extend(entries);
    if first_error.is_none() {
        first_error = err;
    }

    if let Err(err) = context_handle.await {
        let app_error = AppError::InvalidData(format!("seed context task failed: {err}"));
        if first_error.is_none() {
            first_error = Some(app_error);
        }
    }

    seed_aggregates.extend(fresh_entries);
    if let Some(err) = first_error {
        return Err(err);
    }
    let mut aggregates_by_seed: BTreeMap<String, BatchSeedAggregate> = seed_aggregates
        .into_iter()
        .map(|aggregate| {
            (
                aggregate
                    .seed_contract
                    .contract_address
                    .trim()
                    .to_lowercase(),
                aggregate,
            )
        })
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
