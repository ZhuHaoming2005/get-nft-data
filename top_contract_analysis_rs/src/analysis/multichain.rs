use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use futures::{stream, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

use crate::models::{
    rename_native_amount_keys, restore_internal_native_amount_keys, Chain, ChainTotalsPayload,
    ContractId, PaperDuplicateScaleRowPayload, PaperStatsPayload, ScopedDuplicateScaleRowPayload,
    SeedContractPayload, SingleReportPayload,
};
use crate::progress::NoopProgressReporter;

use super::paper_stats::{merge_paper_stats, PaperStatsConfig};
use super::{
    analyze_seed_contract_with_limits, build_candidate_plan_for_seed, fetch_seed_context,
    read_seed_contracts, AnalysisDeps, AnalyzeRequest,
};

const MAX_MULTICHAIN_SEED_PIPELINE_BACKLOG: usize = 8;

#[derive(Clone, Debug)]
pub struct MultiChainBatchRequest {
    pub seed_file: PathBuf,
    pub output_dir: PathBuf,
    pub alchemy_api_key: String,
    pub alchemy_networks: BTreeMap<Chain, String>,
    pub etherscan_api_key: String,
    pub opensea_api_key: String,
    pub name_threshold: f64,
    pub metadata_threshold: f64,
    pub timeout_seconds: u64,
    pub api_max_concurrency: usize,
    pub matched_contract_max_concurrency: usize,
    pub seed_network_max_concurrency: usize,
    pub seed_cpu_max_concurrency: usize,
    pub max_tokens_per_contract: usize,
    pub max_recall_rows: usize,
    pub paper_stats_config: PaperStatsConfig,
}

impl Default for MultiChainBatchRequest {
    fn default() -> Self {
        Self {
            seed_file: PathBuf::new(),
            output_dir: PathBuf::from("result"),
            alchemy_api_key: String::new(),
            alchemy_networks: BTreeMap::new(),
            etherscan_api_key: String::new(),
            opensea_api_key: String::new(),
            name_threshold: 95.0,
            metadata_threshold: 0.6,
            timeout_seconds: 60,
            api_max_concurrency: 8,
            matched_contract_max_concurrency: 4,
            seed_network_max_concurrency: 2,
            seed_cpu_max_concurrency: 1,
            max_tokens_per_contract: 0,
            max_recall_rows: 0,
            paper_stats_config: PaperStatsConfig::default(),
        }
    }
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct BatchFailurePayload {
    pub primary_chain: String,
    pub seed_address: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub secondary_chain: String,
    pub stage: String,
    pub provider: String,
    pub retryable: bool,
    pub error: String,
}

#[derive(Clone, Debug, Default, Serialize, PartialEq)]
pub struct MultiChainBatchResult {
    pub schema_version: u32,
    pub report_type: String,
    pub scoped_duplicate_scale: Vec<ScopedDuplicateScaleRowPayload>,
    pub scoped_paper_stats: Vec<ScopedPaperStatsPayload>,
    pub failures: Vec<BatchFailurePayload>,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct ScopedPaperStatsPayload {
    pub scope: String,
    pub primary_chain: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub secondary_chain: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub native_symbol: String,
    pub paper_stats: serde_json::Value,
}

#[derive(Serialize)]
struct ScopedSeedReport<'a> {
    schema_version: u32,
    scope: &'static str,
    primary_chain: String,
    secondary_chain: String,
    report: &'a SingleReportPayload,
}

#[derive(Deserialize)]
struct CachedScopedSeedReport {
    schema_version: u32,
    primary_chain: String,
    secondary_chain: String,
    report: CachedSingleSeedReport,
}

#[derive(Deserialize)]
struct CachedSingleSeedReport {
    seed_contract: SeedContractPayload,
    paper_stats: CachedPaperStats,
}

#[derive(Deserialize)]
struct CachedPaperStats {
    #[serde(flatten)]
    fields: BTreeMap<String, serde_json::Value>,
}

struct SeedPipelineOutput {
    seed_index: usize,
    stats: Vec<((Chain, Chain), PaperStatsPayload)>,
    failures: Vec<BatchFailurePayload>,
}

pub async fn run_multichain_batch(
    request: MultiChainBatchRequest,
    deps: &AnalysisDeps,
) -> Result<MultiChainBatchResult, crate::error::AppError> {
    let seeds = read_seed_contracts(&request.seed_file)?;
    std::fs::create_dir_all(&request.output_dir)?;
    let mut stats_by_pair = BTreeMap::<(Chain, Chain), Vec<PaperStatsPayload>>::new();
    let mut failures = Vec::new();
    let pending_seed_count = seeds.len().max(1);
    let seed_network_limit = Arc::new(Semaphore::new(request.seed_network_max_concurrency.max(1)));
    let seed_cpu_limit = Arc::new(Semaphore::new(request.seed_cpu_max_concurrency.max(1)));
    let matched_contract_limit = Arc::new(Semaphore::new(
        request.matched_contract_max_concurrency.max(1),
    ));
    let pipeline_backlog = request
        .seed_network_max_concurrency
        .max(1)
        .saturating_add(request.seed_cpu_max_concurrency.max(1))
        .saturating_add(request.matched_contract_max_concurrency.max(1))
        .min(pending_seed_count)
        .clamp(1, MAX_MULTICHAIN_SEED_PIPELINE_BACKLOG);
    let request = Arc::new(request);
    let mut seed_tasks = stream::iter(seeds.into_iter().enumerate().map(|(seed_index, seed)| {
        let request = request.clone();
        let seed_network_limit = seed_network_limit.clone();
        let seed_cpu_limit = seed_cpu_limit.clone();
        let matched_contract_limit = matched_contract_limit.clone();
        async move {
            process_seed_pipeline(
                seed_index,
                seed,
                request,
                deps,
                seed_network_limit,
                seed_cpu_limit,
                matched_contract_limit,
            )
            .await
        }
    }))
    .buffer_unordered(pipeline_backlog);
    let mut seed_outputs = Vec::new();
    while let Some(output) = seed_tasks.next().await {
        seed_outputs.push(output?);
    }
    seed_outputs.sort_by_key(|output| output.seed_index);
    for output in seed_outputs {
        for (pair, stats) in output.stats {
            stats_by_pair.entry(pair).or_default().push(stats);
        }
        failures.extend(output.failures);
    }

    let mut scoped_duplicate_scale = Vec::new();
    let mut scoped_paper_stats = Vec::new();
    for primary_chain in Chain::ALL {
        let mut analyses = Vec::new();
        for secondary_chain in Chain::ALL {
            let Some(stats) = stats_by_pair.get(&(primary_chain, secondary_chain)) else {
                continue;
            };
            analyses.push((
                secondary_chain,
                merge_paper_stats(stats.iter(), request.paper_stats_config),
            ));
        }
        if analyses.is_empty() {
            continue;
        }
        let totals = deps.feature_store.chain_totals(primary_chain.as_str())?;
        let scoped_rows = build_scoped_duplicate_scale(primary_chain, &analyses, totals);
        scoped_duplicate_scale.extend(scoped_rows.clone());
        for (secondary_chain, mut stats) in analyses.iter().cloned() {
            rebase_duplicate_scale(
                &mut stats,
                &scoped_rows,
                if secondary_chain == primary_chain {
                    "intra_chain"
                } else {
                    "chain_matrix"
                },
                Some(secondary_chain),
            );
            scoped_paper_stats.push(scoped_paper_stats_row(
                if secondary_chain == primary_chain {
                    "intra_chain"
                } else {
                    "chain_matrix"
                },
                primary_chain,
                Some(secondary_chain),
                stats,
                true,
            )?);
        }
        let mut qualified_cross = analyses
            .iter()
            .filter(|(secondary_chain, _)| *secondary_chain != primary_chain)
            .map(|(secondary_chain, stats)| {
                let mut stats = stats.clone();
                qualify_cross_chain_identities(&mut stats, *secondary_chain);
                stats
            })
            .collect::<Vec<_>>();
        if !qualified_cross.is_empty() {
            let mut cross = merge_paper_stats(qualified_cross.iter(), request.paper_stats_config);
            rebase_duplicate_scale(&mut cross, &scoped_rows, "cross_chain_summary", None);
            scoped_paper_stats.push(scoped_paper_stats_row(
                "cross_chain_summary",
                primary_chain,
                None,
                cross,
                false,
            )?);
            qualified_cross.clear();
        }
    }

    let result = MultiChainBatchResult {
        schema_version: 2,
        report_type: "multi_chain_batch_summary".to_string(),
        scoped_duplicate_scale,
        scoped_paper_stats,
        failures,
    };
    write_json_atomic(
        &request.output_dir.join("summary.json"),
        &serde_json::to_vec_pretty(&result)?,
    )?;
    write_json_atomic(
        &request.output_dir.join("failures.json"),
        &serde_json::to_vec_pretty(&result.failures)?,
    )?;
    Ok(result)
}

async fn process_seed_pipeline(
    seed_index: usize,
    seed: ContractId,
    request: Arc<MultiChainBatchRequest>,
    deps: &AnalysisDeps,
    seed_network_limit: Arc<Semaphore>,
    seed_cpu_limit: Arc<Semaphore>,
    matched_contract_limit: Arc<Semaphore>,
) -> Result<SeedPipelineOutput, crate::error::AppError> {
    let mut stats = Vec::new();
    let mut failures = Vec::new();
    let mut missing_chains = Vec::new();
    for secondary_chain in Chain::ALL {
        match load_cached_seed_report(&request.output_dir, &seed, secondary_chain) {
            Some(report) => stats.push(((seed.chain, secondary_chain), report)),
            None => missing_chains.push(secondary_chain),
        }
    }
    if missing_chains.is_empty() {
        deps.batch_progress.on_seed_cached(&seed.to_string());
        return Ok(SeedPipelineOutput {
            seed_index,
            stats,
            failures,
        });
    }

    let primary_request = analyze_request(&request, &seed, seed.chain);
    let progress = deps.batch_progress.create_seed_reporter(&seed.to_string());
    let network_permit = seed_network_limit.acquire_owned().await.map_err(|error| {
        crate::error::AppError::InvalidData(format!("seed network limit closed: {error}"))
    })?;
    deps.batch_progress.on_seed_started(&seed.to_string());
    let context = match fetch_seed_context(&primary_request, deps, progress).await {
        Ok(context) => context,
        Err(error) => {
            drop(network_permit);
            deps.batch_progress
                .on_seed_failed(&seed.to_string(), &error.to_string());
            failures.push(failure(&seed, None, "fetch_seed_context", &error));
            return Ok(SeedPipelineOutput {
                seed_index,
                stats,
                failures,
            });
        }
    };
    drop(network_permit);

    let mut seed_succeeded = false;
    for secondary_chain in missing_chains {
        let candidate_request = analyze_request(&request, &seed, secondary_chain);
        let cpu_permit = seed_cpu_limit
            .clone()
            .acquire_owned()
            .await
            .map_err(|error| {
                crate::error::AppError::InvalidData(format!("seed CPU limit closed: {error}"))
            })?;
        let prepared = build_candidate_plan_for_seed(
            candidate_request.clone(),
            deps.feature_store.clone(),
            context.clone(),
            None,
            Arc::new(NoopProgressReporter),
        )
        .await;
        drop(cpu_permit);
        let (prepared_context, plan) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                failures.push(failure(
                    &seed,
                    Some(secondary_chain),
                    "load_snapshot",
                    &error,
                ));
                continue;
            }
        };
        match analyze_seed_contract_with_limits(
            candidate_request,
            deps,
            Arc::new(NoopProgressReporter),
            None,
            Some((prepared_context, plan)),
            Some(matched_contract_limit.clone()),
        )
        .await
        {
            Ok(report) => {
                match write_scoped_seed_report(&request.output_dir, &seed, secondary_chain, &report)
                {
                    Ok(()) => {
                        seed_succeeded = true;
                        stats.push(((seed.chain, secondary_chain), report.paper_stats));
                    }
                    Err(error) => failures.push(failure(
                        &seed,
                        Some(secondary_chain),
                        "write_report",
                        &error,
                    )),
                }
            }
            Err(error) => failures.push(failure(
                &seed,
                Some(secondary_chain),
                "analyze_candidate_chain",
                &error,
            )),
        }
    }
    if seed_succeeded {
        deps.batch_progress.on_seed_finished(&seed.to_string());
    } else {
        deps.batch_progress
            .on_seed_failed(&seed.to_string(), "all candidate-chain analyses failed");
    }
    Ok(SeedPipelineOutput {
        seed_index,
        stats,
        failures,
    })
}

fn analyze_request(
    request: &MultiChainBatchRequest,
    seed: &ContractId,
    candidate_chain: Chain,
) -> AnalyzeRequest {
    AnalyzeRequest {
        chain: candidate_chain.to_string(),
        seed_contract_address: seed.address.clone(),
        alchemy_api_key: request.alchemy_api_key.clone(),
        alchemy_network: request.alchemy_networks.get(&candidate_chain).cloned(),
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

fn failure(
    seed: &ContractId,
    secondary_chain: Option<Chain>,
    stage: &str,
    error: &crate::error::AppError,
) -> BatchFailurePayload {
    BatchFailurePayload {
        primary_chain: seed.chain.to_string(),
        seed_address: seed.address.clone(),
        secondary_chain: secondary_chain
            .map(|chain| chain.to_string())
            .unwrap_or_default(),
        stage: stage.to_string(),
        provider: match secondary_chain {
            Some(Chain::Solana) => "helius",
            None if seed.chain == Chain::Solana => "helius",
            _ => "alchemy_opensea",
        }
        .to_string(),
        retryable: matches!(error, crate::error::AppError::Http(_)),
        error: error.to_string(),
    }
}

fn write_scoped_seed_report(
    output_dir: &std::path::Path,
    seed: &ContractId,
    secondary_chain: Chain,
    report: &SingleReportPayload,
) -> Result<(), crate::error::AppError> {
    let scope = if seed.chain == secondary_chain {
        "intra_chain"
    } else {
        "chain_matrix"
    };
    let payload = ScopedSeedReport {
        schema_version: 2,
        scope,
        primary_chain: seed.chain.to_string(),
        secondary_chain: secondary_chain.to_string(),
        report,
    };
    let path = scoped_seed_report_path(output_dir, seed, secondary_chain);
    write_json_atomic(&path, &serde_json::to_vec_pretty(&payload)?)
}

fn scoped_seed_report_path(
    output_dir: &std::path::Path,
    seed: &ContractId,
    secondary_chain: Chain,
) -> PathBuf {
    output_dir.join(format!(
        "{}__{}__vs__{}.json",
        seed.chain, seed.address, secondary_chain
    ))
}

fn load_cached_seed_report(
    output_dir: &std::path::Path,
    seed: &ContractId,
    secondary_chain: Chain,
) -> Option<PaperStatsPayload> {
    let path = scoped_seed_report_path(output_dir, seed, secondary_chain);
    let bytes = std::fs::read(path).ok()?;
    let cached = serde_json::from_slice::<CachedScopedSeedReport>(&bytes).ok()?;
    if cached.schema_version != 2
        || cached.primary_chain != seed.chain.as_str()
        || cached.secondary_chain != secondary_chain.as_str()
        || !cached
            .report
            .seed_contract
            .chain
            .eq_ignore_ascii_case(seed.chain.as_str())
        || cached.report.seed_contract.contract_address != seed.address
    {
        return None;
    }
    let mut value = serde_json::to_value(cached.report.paper_stats.fields).ok()?;
    restore_internal_native_amount_keys(&mut value);
    serde_json::from_value(value).ok()
}

fn scoped_paper_stats_row(
    scope: &str,
    primary_chain: Chain,
    secondary_chain: Option<Chain>,
    stats: PaperStatsPayload,
    include_native: bool,
) -> Result<ScopedPaperStatsPayload, crate::error::AppError> {
    let mut paper_stats = serde_json::to_value(stats)?;
    if !include_native {
        remove_internal_native_amounts(&mut paper_stats);
    }
    rename_native_amount_keys(&mut paper_stats);
    Ok(ScopedPaperStatsPayload {
        scope: scope.to_string(),
        primary_chain: primary_chain.to_string(),
        secondary_chain: secondary_chain
            .map(|chain| chain.to_string())
            .unwrap_or_default(),
        native_symbol: secondary_chain
            .filter(|_| include_native)
            .map(Chain::native_symbol)
            .unwrap_or("")
            .to_string(),
        paper_stats,
    })
}

fn remove_internal_native_amounts(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            map.retain(|key, _| !key.ends_with("_eth") && key != "is_native_eth");
            for child in map.values_mut() {
                remove_internal_native_amounts(child);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                remove_internal_native_amounts(item);
            }
        }
        _ => {}
    }
}

fn rebase_duplicate_scale(
    stats: &mut PaperStatsPayload,
    rows: &[ScopedDuplicateScaleRowPayload],
    scope: &str,
    secondary_chain: Option<Chain>,
) {
    stats.duplicate_scale = rows
        .iter()
        .filter(|row| {
            row.scope == scope
                && secondary_chain
                    .map(|chain| row.secondary_chain == chain.as_str())
                    .unwrap_or_else(|| row.secondary_chain.is_empty())
        })
        .map(|row| PaperDuplicateScaleRowPayload {
            category: row.category.clone(),
            duplicate_nft_count: row.duplicate_nft_count,
            duplicate_nft_ratio: row.duplicate_nft_ratio,
            duplicate_nft_ratio_numerator: row.duplicate_nft_ratio_numerator,
            duplicate_nft_ratio_denominator: row.duplicate_nft_ratio_denominator,
            duplicate_contract_count: row.duplicate_contract_count,
            duplicate_contract_ratio: row.duplicate_contract_ratio,
            duplicate_contract_ratio_numerator: row.duplicate_contract_ratio_numerator,
            duplicate_contract_ratio_denominator: row.duplicate_contract_ratio_denominator,
        })
        .collect();
}

fn qualify_cross_chain_identities(stats: &mut PaperStatsPayload, chain: Chain) {
    let prefix = format!("{}:", chain);
    let qualify = |value: &mut String| {
        if !value.is_empty() && !value.starts_with(&prefix) {
            *value = format!("{prefix}{value}");
        }
    };
    for values in [
        &mut stats.malicious_addresses,
        &mut stats.honest_addresses,
        &mut stats.repeat_infringing_malicious_addresses,
        &mut stats.duplicate_contract_denominator_keys,
        &mut stats.behavior_contract_denominator_keys,
    ] {
        values.iter_mut().for_each(&qualify);
    }
    for map in [
        &mut stats.duplicate_nft_keys_by_category,
        &mut stats.duplicate_contract_keys_by_category,
        &mut stats.behavior_contracts_by_type,
        &mut stats.behavior_addresses_by_type,
        &mut stats.behavior_nfts_by_type,
        &mut stats.behavior_buyers_by_type,
    ] {
        for values in map.values_mut() {
            values.iter_mut().for_each(&qualify);
        }
    }
    qualify_f64_map(&mut stats.attacker_cost_by_contract_usd, &prefix);
    qualify_f64_map(&mut stats.operator_output_by_contract_usd, &prefix);
    qualify_f64_map(&mut stats.honest_loss_by_contract_usd, &prefix);
    qualify_f64_map(&mut stats.stuck_time_numerator_by_contract, &prefix);
    qualify_f64_map(&mut stats.stuck_time_denominator_by_contract, &prefix);
    for row in &mut stats.contract_behavior_stats {
        qualify(&mut row.contract_address);
    }
    for row in &mut stats.attacker_cost_details {
        qualify(&mut row.contract_address);
    }
    for row in &mut stats.output_input_ratio_by_contract {
        qualify(&mut row.contract_address);
    }
}

fn qualify_f64_map(map: &mut BTreeMap<String, f64>, prefix: &str) {
    *map = std::mem::take(map)
        .into_iter()
        .map(|(key, value)| (format!("{prefix}{key}"), value))
        .collect();
}

fn write_json_atomic(
    path: &std::path::Path,
    contents: &[u8],
) -> Result<(), crate::error::AppError> {
    let temp = path.with_extension(format!("{}.tmp", std::process::id()));
    std::fs::write(&temp, contents)?;
    std::fs::rename(&temp, path)?;
    Ok(())
}

pub fn build_scoped_duplicate_scale(
    primary_chain: Chain,
    analyses: &[(Chain, PaperStatsPayload)],
    totals: ChainTotalsPayload,
) -> Vec<ScopedDuplicateScaleRowPayload> {
    let mut rows = Vec::new();
    let mut cross = BTreeMap::<String, (i64, i64)>::new();
    for (secondary_chain, stats) in analyses {
        for scale in &stats.duplicate_scale {
            let scope = if *secondary_chain == primary_chain {
                "intra_chain"
            } else {
                "chain_matrix"
            };
            rows.push(scoped_row(
                scope,
                primary_chain,
                Some(*secondary_chain),
                &scale.category,
                scale.duplicate_nft_count,
                scale.duplicate_contract_count,
                totals,
            ));
            if *secondary_chain != primary_chain {
                let entry = cross.entry(scale.category.clone()).or_default();
                entry.0 += scale.duplicate_nft_count;
                entry.1 += scale.duplicate_contract_count;
            }
        }
    }
    for (category, (duplicate_nfts, duplicate_contracts)) in cross {
        rows.push(scoped_row(
            "cross_chain_summary",
            primary_chain,
            None,
            &category,
            duplicate_nfts,
            duplicate_contracts,
            totals,
        ));
    }
    rows.sort_by(|left, right| {
        (
            left.scope.as_str(),
            left.primary_chain.as_str(),
            left.secondary_chain.as_str(),
            left.category.as_str(),
        )
            .cmp(&(
                right.scope.as_str(),
                right.primary_chain.as_str(),
                right.secondary_chain.as_str(),
                right.category.as_str(),
            ))
    });
    rows
}

pub fn parse_alchemy_networks(
    entries: &[String],
) -> Result<BTreeMap<Chain, String>, crate::error::AppError> {
    let mut networks = BTreeMap::new();
    for entry in entries {
        let (chain, network) = entry.split_once('=').ok_or_else(|| {
            crate::error::AppError::InvalidData(format!(
                "invalid --alchemy-network value {entry:?}; expected chain=network"
            ))
        })?;
        let chain = chain.parse::<Chain>()?;
        let network = network.trim();
        if network.is_empty() {
            return Err(crate::error::AppError::InvalidData(format!(
                "empty Alchemy network for {chain}"
            )));
        }
        if networks.insert(chain, network.to_string()).is_some() {
            return Err(crate::error::AppError::InvalidData(format!(
                "duplicate Alchemy network override for {chain}"
            )));
        }
    }
    Ok(networks)
}

fn scoped_row(
    scope: &str,
    primary_chain: Chain,
    secondary_chain: Option<Chain>,
    category: &str,
    duplicate_nfts: i64,
    duplicate_contracts: i64,
    totals: ChainTotalsPayload,
) -> ScopedDuplicateScaleRowPayload {
    ScopedDuplicateScaleRowPayload {
        scope: scope.to_string(),
        primary_chain: primary_chain.to_string(),
        secondary_chain: secondary_chain
            .map(|chain| chain.to_string())
            .unwrap_or_default(),
        aggregation: if scope == "cross_chain_summary" {
            "sum_across_secondary_chains".to_string()
        } else {
            "single_chain_pair".to_string()
        },
        category: category.to_string(),
        duplicate_nft_count: duplicate_nfts,
        duplicate_nft_ratio: ratio(duplicate_nfts, totals.total_nfts),
        duplicate_nft_ratio_numerator: duplicate_nfts,
        duplicate_nft_ratio_denominator: totals.total_nfts,
        duplicate_contract_count: duplicate_contracts,
        duplicate_contract_ratio: ratio(duplicate_contracts, totals.total_contracts),
        duplicate_contract_ratio_numerator: duplicate_contracts,
        duplicate_contract_ratio_denominator: totals.total_contracts,
    }
}

fn ratio(numerator: i64, denominator: i64) -> Option<f64> {
    (denominator > 0).then_some(numerator as f64 / denominator as f64)
}
