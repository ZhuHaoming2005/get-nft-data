use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use futures::{stream, StreamExt};
use tokio::sync::Semaphore;

use crate::error::AppError;
use crate::models::BatchSummaryPayload;
use crate::reporting::write_outputs_to_directory;

use super::summary::build_batch_seed_aggregate;
use super::{
    acquire_optional_limit, analyze_matched_contracts_parallel, build_candidate_plan_for_seed,
    fetch_seed_context, finalize_seed_report, prepare_seed_analysis_state, AnalysisDeps,
    AnalyzeRequest, BatchRequest, BatchSeedAggregate,
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

pub async fn run_batch(
    request: BatchRequest,
    deps: &AnalysisDeps,
) -> Result<BatchSummaryPayload, AppError> {
    let seed_addresses = read_seed_addresses(&request.seed_file)?;
    let mut seed_aggregates = Vec::new();
    let pending_seeds = seed_addresses.clone();

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
            let (context, plan) = match build_candidate_plan_for_seed(
                per_seed_request.clone(),
                deps.feature_store.clone(),
                context,
                Some(cpu_limit),
                seed_progress.clone(),
            )
            .await
            {
                Ok(prepared) => prepared,
                Err(err) => {
                    batch_progress.on_seed_failed(&seed_address, &err.to_string());
                    return Err(err);
                }
            };
            let state = prepare_seed_analysis_state(
                per_seed_request,
                &deps,
                seed_progress.clone(),
                None,
                Some((context, plan)),
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
                    batch_progress.on_seed_failed(&seed_address, &err.to_string());
                    return Err(err);
                }
            };
            batch_progress.on_seed_finished(&seed_address);
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
