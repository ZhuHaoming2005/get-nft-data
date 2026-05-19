use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use futures::{stream, StreamExt};
use tokio::sync::Semaphore;

use crate::error::AppError;
use crate::models::{BatchSummaryPayload, OutputFilesPayload, SingleReportPayload};
use crate::reporting::write_outputs_to_directory;

use super::summary::{build_batch_report_summary, build_batch_seed_aggregate};
use super::{
    acquire_optional_limit, analyze_seed_contract_with_limits, build_candidate_plan_for_seed,
    fetch_seed_context, AnalysisDeps, AnalyzeRequest, BatchRequest, BatchSeedAggregate,
    RuntimeLimits,
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
        contract_max_concurrency: request.contract_max_concurrency,
        max_tokens_per_contract: request.max_tokens_per_contract,
        max_recall_rows: request.max_recall_rows,
    }
}

pub async fn run_batch(
    request: BatchRequest,
    deps: &AnalysisDeps,
) -> Result<BatchSummaryPayload, AppError> {
    let seed_addresses = read_seed_addresses(&request.seed_file)?;
    let cached_entries = load_cached_seed_entries(&request.output_dir, &request.chain)?;
    let mut seed_aggregates = Vec::new();
    let mut pending_seeds = Vec::new();
    for seed_address in seed_addresses.iter().cloned() {
        if let Some(cached) = cached_entries.get(&seed_address) {
            deps.batch_progress.on_seed_cached(&seed_address);
            seed_aggregates.push(cached.clone());
        } else {
            pending_seeds.push(seed_address);
        }
    }

    let seed_network_max_concurrency = request.seed_network_max_concurrency.max(1);
    let seed_cpu_max_concurrency = request.seed_cpu_max_concurrency.max(1);
    let cpu_limit = Arc::new(Semaphore::new(seed_cpu_max_concurrency));
    let seed_network_limit = Arc::new(Semaphore::new(seed_network_max_concurrency));
    let runtime_limits = RuntimeLimits {
        seed_metadata_limit: Some(Arc::new(Semaphore::new(
            request.seed_metadata_max_concurrency.max(1),
        ))),
        match_contract_limit: Some(Arc::new(Semaphore::new(
            request.contract_max_concurrency.max(1),
        ))),
    };
    let mut fresh_entries = Vec::new();
    let mut first_error: Option<AppError> = None;
    let pending_count = pending_seeds.len().max(1);
    let output_dir = request.output_dir.clone();
    let mut seed_analyses = stream::iter(pending_seeds.into_iter().map(|seed_address| {
        let per_seed_request = analyze_request_for_batch_seed(&request, seed_address.clone());
        let batch_progress = deps.batch_progress.clone();
        let seed_network_limit = seed_network_limit.clone();
        let cpu_limit = cpu_limit.clone();
        let runtime_limits = runtime_limits.clone();
        let feature_store = deps.feature_store.clone();
        let output_dir = output_dir.clone();
        async move {
            let seed_progress = batch_progress.create_seed_reporter(&seed_address);
            let context = {
                let _permit = acquire_optional_limit(&Some(seed_network_limit.clone())).await?;
                batch_progress.on_seed_started(&seed_address);
                match fetch_seed_context(
                    &per_seed_request,
                    deps,
                    &runtime_limits,
                    seed_progress.clone(),
                )
                .await
                {
                    Ok(context) => context,
                    Err(err) => {
                        batch_progress.on_seed_failed(&seed_address, &err.to_string());
                        return Err(err);
                    }
                }
            };
            let (context, plan) = match build_candidate_plan_for_seed(
                per_seed_request.clone(),
                feature_store,
                context,
                Some(cpu_limit.clone()),
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
            let result = async {
                let _permit = acquire_optional_limit(&Some(seed_network_limit)).await?;
                analyze_seed_contract_with_limits(
                    per_seed_request,
                    deps,
                    seed_progress,
                    Some(cpu_limit),
                    runtime_limits,
                    Some((context, plan)),
                )
                .await
            }
            .await;
            match result {
                Ok(payload) => {
                    batch_progress.on_seed_finished(&seed_address);
                    let (json_path, md_path) = write_outputs_to_directory(&payload, &output_dir)?;
                    let mut aggregate = build_batch_seed_aggregate(payload);
                    aggregate.report.output_files = Some(OutputFilesPayload {
                        json: json_path
                            .file_name()
                            .unwrap_or_default()
                            .to_string_lossy()
                            .into_owned(),
                        markdown: md_path
                            .file_name()
                            .unwrap_or_default()
                            .to_string_lossy()
                            .into_owned(),
                    });
                    Ok(aggregate)
                }
                Err(err) => {
                    batch_progress.on_seed_failed(&seed_address, &err.to_string());
                    Err(err)
                }
            }
        }
    }))
    .buffer_unordered(pending_count);
    while let Some(entry) = seed_analyses.next().await {
        match entry {
            Ok(entry) => fresh_entries.push(entry),
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
        .map(|aggregate| {
            (
                aggregate.report.seed_contract.contract_address.clone(),
                aggregate,
            )
        })
        .collect();
    let seed_aggregates: Vec<BatchSeedAggregate> = seed_addresses
        .iter()
        .filter_map(|seed| aggregates_by_seed.remove(seed))
        .collect();
    let seed_reports = seed_aggregates
        .iter()
        .map(|aggregate| aggregate.report.clone())
        .collect();

    Ok(BatchSummaryPayload {
        batch_summary: build_batch_report_summary(&seed_aggregates),
        seed_reports,
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

fn load_cached_seed_entries(
    output_dir: &Path,
    chain: &str,
) -> Result<BTreeMap<String, BatchSeedAggregate>, AppError> {
    let mut cached = BTreeMap::new();
    if !output_dir.exists() {
        return Ok(cached);
    }

    for entry in std::fs::read_dir(output_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let file_name = path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("");
        if !file_name.starts_with("top_contract_analysis__")
            || file_name == "top_contract_analysis__summary.json"
        {
            continue;
        }

        let raw: serde_json::Value = match serde_json::from_str(&std::fs::read_to_string(&path)?) {
            Ok(payload) => payload,
            Err(_) => continue,
        };
        let Ok(payload) = serde_json::from_value::<SingleReportPayload>(raw) else {
            continue;
        };
        let mut aggregate = build_batch_seed_aggregate(payload);
        if aggregate.report.seed_contract.chain.to_lowercase() != chain.to_lowercase() {
            continue;
        }
        let contract_address = aggregate
            .report
            .seed_contract
            .contract_address
            .to_lowercase();
        if contract_address.is_empty() {
            continue;
        }
        aggregate.report.output_files = Some(OutputFilesPayload {
            json: file_name.to_string(),
            markdown: path
                .with_extension("md")
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .into_owned(),
        });
        cached.insert(contract_address, aggregate);
    }

    Ok(cached)
}
