use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures::{stream, StreamExt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, oneshot, Semaphore};

use crate::models::{
    Chain, ChainTotalsPayload, ContractId, DatabaseSnapshot, PaperDuplicateScaleRowPayload,
    PaperStatsPayload, ScopedDuplicateScaleRowPayload, SeedContractPayload, SeedNft,
    SingleReportPayload,
};
use crate::progress::NoopProgressReporter;

use super::paper_stats::{merge_paper_stats, PaperStatsConfig};
use super::{
    analyze_seed_contract_with_limits, build_candidate_plan_with_snapshot, fetch_seed_context,
    seed_nfts_for_duplicate_matching, AnalysisDeps, AnalyzeRequest, FeatureStoreReader,
    ProviderEvidencePin,
};

const MAX_MULTICHAIN_SEED_PIPELINE_BACKLOG: usize = 8;
const RUN_MANIFEST_SCHEMA_VERSION: u32 = 1;

pub fn read_seed_contracts(seed_file: &Path) -> Result<Vec<ContractId>, crate::error::AppError> {
    let content = std::fs::read_to_string(seed_file)?;
    let mut lines = content.lines();
    let header = lines
        .next()
        .map(str::trim)
        .ok_or_else(|| crate::error::AppError::InvalidData("seed CSV is empty".to_string()))?;
    if header != "chain,address" {
        return Err(crate::error::AppError::InvalidData(
            "seed CSV header must be exactly chain,address".to_string(),
        ));
    }

    let mut seeds = Vec::new();
    let mut seen = BTreeSet::new();
    for (index, raw_line) in lines.enumerate() {
        let line_number = index + 2;
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        let mut fields = line.split(',');
        let chain = fields.next().unwrap_or_default();
        let address = fields.next().unwrap_or_default();
        if fields.next().is_some() || chain.trim().is_empty() || address.trim().is_empty() {
            return Err(crate::error::AppError::InvalidData(format!(
                "invalid seed CSV row {line_number}: expected chain,address"
            )));
        }
        let seed = ContractId::new(chain.parse::<Chain>()?, address)?;
        if !seen.insert(seed.clone()) {
            return Err(crate::error::AppError::InvalidData(format!(
                "duplicate seed contract at row {line_number}: {seed}"
            )));
        }
        seeds.push(seed);
    }
    if seeds.is_empty() {
        return Err(crate::error::AppError::InvalidData(
            "seed CSV does not contain any contracts".to_string(),
        ));
    }
    Ok(seeds)
}

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
    pub max_history_transactions_per_asset: usize,
    pub max_history_transactions_per_collection: usize,
    pub max_helius_assets_per_collection: usize,
    pub paper_stats_config: PaperStatsConfig,
    /// Ignore an unfinished run manifest and begin a fresh provider-backed run.
    pub refresh_scoped_cache: bool,
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
            max_history_transactions_per_asset: 100,
            max_history_transactions_per_collection: 10_000,
            max_helius_assets_per_collection: 10_000,
            paper_stats_config: PaperStatsConfig::default(),
            refresh_scoped_cache: false,
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
    analysis_fingerprint: &'a str,
    scope: &'static str,
    primary_chain: String,
    secondary_chain: String,
    report: &'a SingleReportPayload,
}

#[derive(Deserialize)]
struct CachedScopedSeedReport {
    schema_version: u32,
    analysis_fingerprint: String,
    primary_chain: String,
    secondary_chain: String,
    report: CachedSingleSeedReport,
}

#[derive(Deserialize)]
struct CachedSingleSeedReport {
    seed_contract: SeedContractPayload,
    paper_stats: PaperStatsPayload,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum RunStatus {
    Incomplete,
    Complete,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct RunManifest {
    schema_version: u32,
    run_id: String,
    analysis_timestamp: i64,
    config_fingerprint: String,
    seed_set_identity: String,
    snapshot_identities: BTreeMap<String, String>,
    started_at: i64,
    updated_at: i64,
    status: RunStatus,
}

struct SeedPipelineOutput {
    failures: Vec<BatchFailurePayload>,
}

struct SeedPipelineLimits {
    network: Arc<Semaphore>,
    cpu: Arc<Semaphore>,
    matched_contract: Arc<Semaphore>,
}

struct SnapshotBatchRequest {
    chain: String,
    seed_address: String,
    seed_nfts: Vec<SeedNft>,
    response: oneshot::Sender<Result<DatabaseSnapshot, crate::error::AppError>>,
}

#[derive(Clone)]
struct SnapshotBatcher {
    sender: mpsc::Sender<SnapshotBatchRequest>,
}

impl SnapshotBatcher {
    fn spawn(
        feature_store: Arc<dyn FeatureStoreReader>,
        request: &MultiChainBatchRequest,
        max_batch_size: usize,
    ) -> Self {
        let capacity = max_batch_size.max(1);
        let (sender, mut receiver) = mpsc::channel::<SnapshotBatchRequest>(capacity);
        let name_threshold = request.name_threshold;
        let metadata_threshold = request.metadata_threshold;
        let max_tokens_per_contract = request.max_tokens_per_contract;
        let max_recall_rows = request.max_recall_rows;
        let batch_limit = Arc::new(Semaphore::new(request.seed_cpu_max_concurrency.max(1)));
        tokio::spawn(async move {
            while let Some(first) = receiver.recv().await {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                let mut batch = vec![first];
                while batch.len() < capacity {
                    match receiver.try_recv() {
                        Ok(request) => batch.push(request),
                        Err(_) => break,
                    }
                }
                let mut by_chain = BTreeMap::<String, Vec<SnapshotBatchRequest>>::new();
                for request in batch {
                    by_chain
                        .entry(request.chain.clone())
                        .or_default()
                        .push(request);
                }
                for (chain, requests) in by_chain {
                    let store = feature_store.clone();
                    let batch_limit = batch_limit.clone();
                    tokio::spawn(async move {
                        let _permit = batch_limit
                            .acquire_owned()
                            .await
                            .expect("snapshot batch semaphore remains open");
                        let seeds = requests
                            .iter()
                            .map(|request| {
                                (request.seed_address.clone(), request.seed_nfts.clone())
                            })
                            .collect::<Vec<_>>();
                        let chain_for_load = chain.clone();
                        let loaded = tokio::task::spawn_blocking(move || {
                            store.load_snapshots(
                                &chain_for_load,
                                &seeds,
                                name_threshold,
                                metadata_threshold,
                                max_tokens_per_contract,
                                max_recall_rows,
                            )
                        })
                        .await;
                        match loaded {
                            Ok(Ok(mut snapshots)) => {
                                for request in requests {
                                    let result =
                                        snapshots.remove(&request.seed_address).ok_or_else(|| {
                                            crate::error::AppError::InvalidData(format!(
                                                "bulk snapshot response omitted {}:{}",
                                                chain, request.seed_address
                                            ))
                                        });
                                    let _ = request.response.send(result);
                                }
                            }
                            result => {
                                let message = match result {
                                    Ok(Err(error)) => error.to_string(),
                                    Err(error) => format!("bulk snapshot CPU task failed: {error}"),
                                    Ok(Ok(_)) => unreachable!(),
                                };
                                for request in requests {
                                    let _ = request.response.send(Err(
                                        crate::error::AppError::InvalidData(message.clone()),
                                    ));
                                }
                            }
                        }
                    });
                }
            }
        });
        Self { sender }
    }

    async fn load(
        &self,
        chain: &str,
        seed_address: &str,
        seed_nfts: Vec<SeedNft>,
    ) -> Result<DatabaseSnapshot, crate::error::AppError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(SnapshotBatchRequest {
                chain: chain.to_string(),
                seed_address: seed_address.to_string(),
                seed_nfts,
                response,
            })
            .await
            .map_err(|_| {
                crate::error::AppError::InvalidData("snapshot batch worker stopped".to_string())
            })?;
        receiver.await.map_err(|_| {
            crate::error::AppError::InvalidData("snapshot batch response dropped".to_string())
        })?
    }
}

pub struct BatchOutputLock {
    file: File,
    output_dir: PathBuf,
}

impl BatchOutputLock {
    fn acquire(output_dir: &Path) -> Result<Self, crate::error::AppError> {
        std::fs::create_dir_all(output_dir)?;
        let output_dir = std::fs::canonicalize(output_dir)?;
        let path = output_dir.join("run.lock");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;
        fs2::FileExt::try_lock_exclusive(&file).map_err(|error| {
            crate::error::AppError::InvalidData(format!(
                "output directory {} is already owned by another batch: {error}",
                output_dir.display()
            ))
        })?;
        write_lock_metadata(&mut file, None)?;
        Ok(Self { file, output_dir })
    }

    fn set_run_id(&mut self, run_id: &str) -> Result<(), crate::error::AppError> {
        write_lock_metadata(&mut self.file, Some(run_id))
    }
}

impl Drop for BatchOutputLock {
    fn drop(&mut self) {
        let _ = fs2::FileExt::unlock(&self.file);
    }
}

fn write_lock_metadata(
    file: &mut File,
    run_id: Option<&str>,
) -> Result<(), crate::error::AppError> {
    let payload = serde_json::json!({
        "pid": std::process::id(),
        "run_id": run_id.unwrap_or_default(),
        "started_at": chrono::Utc::now().timestamp(),
    });
    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&serde_json::to_vec_pretty(&payload)?)?;
    file.sync_data()?;
    Ok(())
}

pub async fn run_multichain_batch(
    request: MultiChainBatchRequest,
    deps: &AnalysisDeps,
) -> Result<MultiChainBatchResult, crate::error::AppError> {
    let output_lock = BatchOutputLock::acquire(&request.output_dir)?;
    run_multichain_batch_with_lock(request, deps, output_lock).await
}

pub fn acquire_batch_output_lock(
    output_dir: &Path,
) -> Result<BatchOutputLock, crate::error::AppError> {
    BatchOutputLock::acquire(output_dir)
}

pub async fn run_multichain_batch_with_lock(
    mut request: MultiChainBatchRequest,
    deps: &AnalysisDeps,
    mut output_lock: BatchOutputLock,
) -> Result<MultiChainBatchResult, crate::error::AppError> {
    let requested_output_dir = std::fs::canonicalize(&request.output_dir)?;
    if requested_output_dir != output_lock.output_dir {
        return Err(crate::error::AppError::InvalidData(format!(
            "batch output lock owns {}, not {}",
            output_lock.output_dir.display(),
            requested_output_dir.display()
        )));
    }
    let seeds = read_seed_contracts(&request.seed_file)?;
    std::fs::create_dir_all(&request.output_dir)?;
    let seed_set_identity = seed_set_identity(&seeds);
    let snapshot_identities = snapshot_identities(deps)?;
    let config_fingerprint = batch_config_fingerprint(&request, &seed_set_identity);
    let mut manifest = resolve_run_manifest(
        &request.output_dir,
        &request,
        config_fingerprint,
        seed_set_identity,
        snapshot_identities,
    );
    output_lock.set_run_id(&manifest.run_id)?;
    request.paper_stats_config.analysis_timestamp = manifest.analysis_timestamp;
    write_run_manifest(&request.output_dir, &manifest)?;
    let mut failures = Vec::new();
    let pending_seed_count = seeds.len().max(1);
    let pipeline_limits = Arc::new(SeedPipelineLimits {
        network: Arc::new(Semaphore::new(request.seed_network_max_concurrency.max(1))),
        cpu: Arc::new(Semaphore::new(request.seed_cpu_max_concurrency.max(1))),
        matched_contract: Arc::new(Semaphore::new(
            request.matched_contract_max_concurrency.max(1),
        )),
    });
    let pipeline_backlog = request
        .seed_network_max_concurrency
        .max(1)
        .saturating_add(request.seed_cpu_max_concurrency.max(1))
        .saturating_add(request.matched_contract_max_concurrency.max(1))
        .min(pending_seed_count)
        .clamp(1, MAX_MULTICHAIN_SEED_PIPELINE_BACKLOG);
    let snapshot_batcher =
        SnapshotBatcher::spawn(deps.feature_store.clone(), &request, pipeline_backlog);
    let request = Arc::new(request);
    let run_id = Arc::new(manifest.run_id.clone());
    let aggregation_seeds = seeds.clone();
    let mut seed_tasks = stream::iter(seeds.into_iter().map(|seed| {
        let request = request.clone();
        let pipeline_limits = pipeline_limits.clone();
        let run_id = run_id.clone();
        let snapshot_batcher = snapshot_batcher.clone();
        async move {
            process_seed_pipeline(
                seed,
                request,
                deps,
                pipeline_limits,
                run_id,
                snapshot_batcher,
            )
            .await
        }
    }))
    .buffered(pipeline_backlog);
    while let Some(output) = seed_tasks.next().await {
        let output = output?;
        failures.extend(output.failures);
    }

    let mut scoped_duplicate_scale = Vec::new();
    let mut scoped_paper_stats = Vec::new();
    for primary_chain in Chain::ALL {
        let mut analyses = Vec::new();
        for secondary_chain in Chain::ALL {
            let mut stats = aggregation_seeds
                .iter()
                .filter(|seed| seed.chain == primary_chain)
                .filter_map(|seed| {
                    let fingerprint = analysis_fingerprint(&run_id, seed, secondary_chain);
                    load_cached_seed_report(
                        &request.output_dir,
                        seed,
                        secondary_chain,
                        &fingerprint,
                    )
                })
                .peekable();
            if stats.peek().is_none() {
                continue;
            }
            analyses.push((
                secondary_chain,
                merge_paper_stats(stats, request.paper_stats_config),
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
    manifest.status = if result.failures.is_empty() {
        RunStatus::Complete
    } else {
        RunStatus::Incomplete
    };
    manifest.updated_at = chrono::Utc::now().timestamp();
    write_run_manifest(&request.output_dir, &manifest)?;
    Ok(result)
}

async fn process_seed_pipeline(
    seed: ContractId,
    request: Arc<MultiChainBatchRequest>,
    deps: &AnalysisDeps,
    limits: Arc<SeedPipelineLimits>,
    run_id: Arc<String>,
    snapshot_batcher: SnapshotBatcher,
) -> Result<SeedPipelineOutput, crate::error::AppError> {
    let _evidence_pin =
        ProviderEvidencePin::new(deps.api.as_ref(), seed.chain.as_str(), &seed.address);
    process_seed_pipeline_pinned(seed, request, deps, limits, run_id, snapshot_batcher).await
}

async fn process_seed_pipeline_pinned(
    seed: ContractId,
    request: Arc<MultiChainBatchRequest>,
    deps: &AnalysisDeps,
    limits: Arc<SeedPipelineLimits>,
    run_id: Arc<String>,
    snapshot_batcher: SnapshotBatcher,
) -> Result<SeedPipelineOutput, crate::error::AppError> {
    let mut failures = Vec::new();
    let mut missing_chains = Vec::new();
    for secondary_chain in Chain::ALL {
        let fingerprint = analysis_fingerprint(&run_id, &seed, secondary_chain);
        match load_cached_seed_report(&request.output_dir, &seed, secondary_chain, &fingerprint) {
            Some(_) => {}
            None => missing_chains.push((secondary_chain, fingerprint)),
        }
    }
    if missing_chains.is_empty() {
        deps.batch_progress.on_seed_cached(&seed.to_string());
        return Ok(SeedPipelineOutput { failures });
    }

    let primary_request = analyze_request(&request, &seed, seed.chain);
    let progress = deps.batch_progress.create_seed_reporter(&seed.to_string());
    let network_permit = limits
        .network
        .clone()
        .acquire_owned()
        .await
        .map_err(|error| {
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
            return Ok(SeedPipelineOutput { failures });
        }
    };
    drop(network_permit);

    let mut seed_succeeded = false;
    for (secondary_chain, fingerprint) in missing_chains {
        let candidate_request = analyze_request(&request, &seed, secondary_chain);
        let dedup_seed_nfts =
            seed_nfts_for_duplicate_matching(&context.seed_nfts, &context.seed_contract);
        let snapshot = snapshot_batcher
            .load(
                secondary_chain.as_str(),
                &seed.address,
                dedup_seed_nfts.clone(),
            )
            .await;
        let prepared = match snapshot {
            Ok(snapshot) => {
                let cpu_permit = limits.cpu.clone().acquire_owned().await.map_err(|error| {
                    crate::error::AppError::InvalidData(format!("seed CPU limit closed: {error}"))
                })?;
                let prepared = build_candidate_plan_with_snapshot(
                    candidate_request.clone(),
                    context.clone(),
                    snapshot,
                    dedup_seed_nfts,
                    Arc::new(NoopProgressReporter),
                )
                .await;
                drop(cpu_permit);
                prepared
            }
            Err(error) => Err(error),
        };
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
            Some(limits.matched_contract.clone()),
        )
        .await
        {
            Ok(report) => {
                match write_scoped_seed_report(
                    &request.output_dir,
                    &seed,
                    secondary_chain,
                    &fingerprint,
                    &report,
                ) {
                    Ok(()) => {
                        seed_succeeded = true;
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
    Ok(SeedPipelineOutput { failures })
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
    analysis_fingerprint: &str,
    report: &SingleReportPayload,
) -> Result<(), crate::error::AppError> {
    let scope = if seed.chain == secondary_chain {
        "intra_chain"
    } else {
        "chain_matrix"
    };
    let payload = ScopedSeedReport {
        schema_version: 3,
        analysis_fingerprint,
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
        seed.chain,
        lowercase_hex(seed.address.as_bytes()),
        secondary_chain
    ))
}

fn load_cached_seed_report(
    output_dir: &std::path::Path,
    seed: &ContractId,
    secondary_chain: Chain,
    analysis_fingerprint: &str,
) -> Option<PaperStatsPayload> {
    let path = scoped_seed_report_path(output_dir, seed, secondary_chain);
    let bytes = std::fs::read(path).ok()?;
    let cached = serde_json::from_slice::<CachedScopedSeedReport>(&bytes).ok()?;
    if cached.schema_version != 3
        || cached.analysis_fingerprint != analysis_fingerprint
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
    Some(cached.report.paper_stats)
}

fn analysis_fingerprint(run_id: &str, seed: &ContractId, secondary_chain: Chain) -> String {
    let canonical = format!(
        "v4|{run_id}|{}|{}|{}",
        seed.chain, seed.address, secondary_chain,
    );
    lowercase_hex(&Sha256::digest(canonical.as_bytes()))
}

fn snapshot_identities(
    deps: &AnalysisDeps,
) -> Result<BTreeMap<String, String>, crate::error::AppError> {
    Chain::ALL
        .into_iter()
        .map(|chain| {
            deps.feature_store
                .snapshot_identity(chain.as_str())
                .map(|identity| (chain.to_string(), identity))
        })
        .collect()
}

fn seed_set_identity(seeds: &[ContractId]) -> String {
    let mut canonical = seeds.iter().map(ToString::to_string).collect::<Vec<_>>();
    canonical.sort();
    lowercase_hex(&Sha256::digest(canonical.join("\n").as_bytes()))
}

fn batch_config_fingerprint(request: &MultiChainBatchRequest, seed_set_identity: &str) -> String {
    let config = request.paper_stats_config;
    let networks = request
        .alchemy_networks
        .iter()
        .map(|(chain, network)| format!("{chain}={network}"))
        .collect::<Vec<_>>()
        .join(",");
    let canonical = format!(
        concat!("v2|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}"),
        seed_set_identity,
        request.name_threshold.to_bits(),
        request.metadata_threshold.to_bits(),
        request.max_tokens_per_contract,
        request.max_recall_rows,
        request.max_history_transactions_per_asset,
        request.max_history_transactions_per_collection,
        request.max_helius_assets_per_collection,
        networks,
        config.min_cycle_size,
        config.min_path_length,
        config.center_fanout_threshold,
        config.concentration_top_pct.to_bits(),
        config.analysis_timestamp,
    );
    lowercase_hex(&Sha256::digest(canonical.as_bytes()))
}

fn resolve_run_manifest(
    output_dir: &Path,
    request: &MultiChainBatchRequest,
    config_fingerprint: String,
    seed_set_identity: String,
    snapshot_identities: BTreeMap<String, String>,
) -> RunManifest {
    let path = output_dir.join("run-manifest.json");
    if !request.refresh_scoped_cache {
        if let Ok(bytes) = std::fs::read(&path) {
            if let Ok(existing) = serde_json::from_slice::<RunManifest>(&bytes) {
                if existing.schema_version == RUN_MANIFEST_SCHEMA_VERSION
                    && existing.status == RunStatus::Incomplete
                    && existing.config_fingerprint == config_fingerprint
                    && existing.seed_set_identity == seed_set_identity
                    && existing.snapshot_identities == snapshot_identities
                {
                    return existing;
                }
            }
        }
    }
    let now = chrono::Utc::now().timestamp();
    let analysis_timestamp = if request.paper_stats_config.analysis_timestamp > 0 {
        request.paper_stats_config.analysis_timestamp
    } else {
        now
    };
    let nonce = format!(
        "{}|{}|{}|{}|{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default(),
        std::process::id(),
        config_fingerprint,
        seed_set_identity,
        analysis_timestamp
    );
    RunManifest {
        schema_version: RUN_MANIFEST_SCHEMA_VERSION,
        run_id: lowercase_hex(&Sha256::digest(nonce.as_bytes())),
        analysis_timestamp,
        config_fingerprint,
        seed_set_identity,
        snapshot_identities,
        started_at: now,
        updated_at: now,
        status: RunStatus::Incomplete,
    }
}

fn write_run_manifest(
    output_dir: &Path,
    manifest: &RunManifest,
) -> Result<(), crate::error::AppError> {
    write_json_atomic(
        &output_dir.join("run-manifest.json"),
        &serde_json::to_vec_pretty(manifest)?,
    )
}

fn lowercase_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
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
    const NATIVE_MONEY_FIELDS: &[&str] = &[
        "exit_gas_native",
        "fake_volume_native",
        "gas_native",
        "linked_loss_native",
        "lure_gas_native",
        "paid_mint_loss_native",
        "secondary_sale_loss_native",
        "setup_gas_native",
        "total_gas_native",
        "total_loss_native",
        "total_paid_native",
        "total_value_native",
        "value_collected_native",
        "is_native_payment",
    ];
    match value {
        serde_json::Value::Object(map) => {
            map.retain(|key, _| !NATIVE_MONEY_FIELDS.contains(&key.as_str()));
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

#[cfg(test)]
mod native_field_tests {
    use super::{
        batch_config_fingerprint, remove_internal_native_amounts, seed_set_identity,
        BatchOutputLock, MultiChainBatchRequest,
    };
    use crate::models::ContractId;
    use tempfile::tempdir;

    #[test]
    fn usd_only_filter_removes_known_amounts_but_preserves_unrelated_native_fields() {
        let mut value = serde_json::json!({
            "attacker_cost": {"gas_native": 1.0, "gas_usd": 2.0},
            "future": {"execution_native": true}
        });

        remove_internal_native_amounts(&mut value);

        assert!(value["attacker_cost"].get("gas_native").is_none());
        assert_eq!(value["attacker_cost"]["gas_usd"], 2.0);
        assert_eq!(value["future"]["execution_native"], true);
    }

    #[test]
    fn seed_identity_is_order_independent_but_changes_with_the_work_set() {
        let first = ContractId::new(
            "ethereum".parse().unwrap(),
            "0x1111111111111111111111111111111111111111",
        )
        .unwrap();
        let second = ContractId::new(
            "solana".parse().unwrap(),
            "So11111111111111111111111111111111111111112",
        )
        .unwrap();

        assert_eq!(
            seed_set_identity(&[first.clone(), second.clone()]),
            seed_set_identity(&[second.clone(), first.clone()])
        );
        assert_ne!(
            seed_set_identity(std::slice::from_ref(&first)),
            seed_set_identity(std::slice::from_ref(&second))
        );
        let request = MultiChainBatchRequest::default();
        assert_ne!(
            batch_config_fingerprint(&request, &seed_set_identity(&[first])),
            batch_config_fingerprint(&request, &seed_set_identity(&[second]))
        );
    }

    #[test]
    fn output_directory_rejects_a_second_live_batch_owner() {
        let directory = tempdir().unwrap();
        let _first = BatchOutputLock::acquire(directory.path()).unwrap();

        assert!(BatchOutputLock::acquire(directory.path()).is_err());
    }
}
