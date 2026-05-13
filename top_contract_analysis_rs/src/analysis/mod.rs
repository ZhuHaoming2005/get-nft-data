use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use futures::stream::{self, StreamExt};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::error::AppError;
use crate::models::{
    AddressAttributionPayload, AddressSignalPayload, BatchSeedReportPayload, ContractMetadata,
    DatabaseSnapshot, DuplicateCandidate, DuplicateContractPayload, FraudTradeStatsPayload,
    HonestAddressPayload, HonestAddressStatsPayload, InfringingTokenRecord,
    MaliciousAddressPayload, NftMarketEventRecord, NftPropagationPathPayload, OwnerBalance,
    SecondarySaleVictimAddressPayload, SeedContractPayload, SeedNft, SingleReportPayload,
    TransferRecord, ValueFlowEdgePayload, VictimSignalPayload,
};
use crate::progress::{BatchProgressReporter, SeedProgressReporter};
use crate::store::{CachedSignals, ContractSignalCache, DuckDbFeatureStore};

pub mod address_records;
mod api;
mod batch;
mod candidate_filter;
mod contract_analysis;
pub mod duplicate;
pub mod lifecycle;
pub mod propagation;
mod sale_metrics;
pub mod scoring;
pub mod signals;
mod summary;
mod value_flow;

pub use api::{AnalyzeApi, CandidateSeedHolderRequest, RealApi};
pub use batch::{read_seed_addresses, run_batch};
pub use candidate_filter::group_candidates_by_contract;

use candidate_filter::*;
use contract_analysis::*;
use sale_metrics::*;
use summary::*;
use value_flow::*;

const DEFAULT_NAME_THRESHOLD: f64 = 95.0;
const DEFAULT_METADATA_THRESHOLD: f64 = 0.6;
const DEFAULT_TIMEOUT_SECONDS: u64 = 60;

#[derive(Clone, Debug)]
pub struct AnalyzeRequest {
    pub chain: String,
    pub seed_contract_address: String,
    pub alchemy_api_key: String,
    pub alchemy_network: Option<String>,
    pub etherscan_api_key: String,
    pub opensea_api_key: String,
    pub name_threshold: f64,
    pub metadata_threshold: f64,
    pub timeout_seconds: u64,
    pub api_max_concurrency: usize,
    pub contract_max_concurrency: usize,
    pub sale_metric_max_concurrency: usize,
    pub max_tokens_per_contract: usize,
    pub max_recall_rows: usize,
}

impl Default for AnalyzeRequest {
    fn default() -> Self {
        Self {
            chain: "ethereum".into(),
            seed_contract_address: String::new(),
            alchemy_api_key: String::new(),
            alchemy_network: None,
            etherscan_api_key: String::new(),
            opensea_api_key: String::new(),
            name_threshold: DEFAULT_NAME_THRESHOLD,
            metadata_threshold: DEFAULT_METADATA_THRESHOLD,
            timeout_seconds: DEFAULT_TIMEOUT_SECONDS,
            api_max_concurrency: 8,
            contract_max_concurrency: 4,
            sale_metric_max_concurrency: 4,
            max_tokens_per_contract: 0,
            max_recall_rows: 0,
        }
    }
}

pub trait FeatureStoreReader: Send + Sync {
    fn load_snapshot(
        &self,
        chain: &str,
        seed_nfts: &[SeedNft],
        name_threshold: f64,
        metadata_threshold: f64,
        max_tokens_per_contract: usize,
        max_recall_rows: usize,
    ) -> Result<DatabaseSnapshot, AppError>;

    fn load_snapshots(
        &self,
        chain: &str,
        seeds: &[(String, Vec<SeedNft>)],
        name_threshold: f64,
        metadata_threshold: f64,
        max_tokens_per_contract: usize,
        max_recall_rows: usize,
    ) -> Result<BTreeMap<String, DatabaseSnapshot>, AppError> {
        let mut snapshots = BTreeMap::new();
        for (seed_address, seed_nfts) in seeds {
            snapshots.insert(
                seed_address.clone(),
                self.load_snapshot(
                    chain,
                    seed_nfts,
                    name_threshold,
                    metadata_threshold,
                    max_tokens_per_contract,
                    max_recall_rows,
                )?,
            );
        }
        Ok(snapshots)
    }
}

pub trait SignalCacheStore: Send + Sync {
    fn get(
        &self,
        chain: &str,
        contract_address: &str,
        token_type: &str,
    ) -> Result<Option<CachedSignals>, AppError>;

    fn put(
        &self,
        chain: &str,
        contract_address: &str,
        token_type: &str,
        transfers: &[TransferRecord],
        owners: &[OwnerBalance],
    ) -> Result<(), AppError>;
}

pub struct AnalysisDeps {
    pub api: Arc<dyn AnalyzeApi>,
    pub feature_store: Arc<dyn FeatureStoreReader>,
    pub signal_cache: Option<Arc<dyn SignalCacheStore>>,
    pub progress: Arc<dyn SeedProgressReporter>,
    pub batch_progress: Arc<dyn BatchProgressReporter>,
}

#[derive(Clone, Default)]
struct RuntimeLimits {
    seed_metadata_limit: Option<Arc<Semaphore>>,
    contract_limit: Option<Arc<Semaphore>>,
    sale_metric_limit: Option<Arc<Semaphore>>,
}

struct SeedContext {
    seed_contract: ContractMetadata,
    seed_nfts: Vec<SeedNft>,
    open_license: bool,
}

struct CandidatePlan {
    snapshot: DatabaseSnapshot,
    candidates: Vec<DuplicateCandidate>,
}

struct CandidateContractFilterResult {
    candidates: Vec<DuplicateCandidate>,
    seed_related_legit_duplicates: Vec<DuplicateContractPayload>,
}

async fn acquire_optional_limit(
    limit: &Option<Arc<Semaphore>>,
) -> Result<Option<OwnedSemaphorePermit>, AppError> {
    match limit {
        Some(limit) => limit
            .clone()
            .acquire_owned()
            .await
            .map(Some)
            .map_err(|err| AppError::InvalidData(format!("batch limit closed: {err}"))),
        None => Ok(None),
    }
}

#[derive(Clone, Debug)]
struct BatchSeedAggregate {
    report: BatchSeedReportPayload,
    malicious_addresses: BTreeSet<String>,
    neutral_addresses: BTreeSet<String>,
    minter_infringing_contracts: BTreeMap<String, BTreeSet<String>>,
}

struct ContractAnalysisResult {
    contract_address: String,
    contract_metadata: Option<ContractMetadata>,
    implausible_candidate_filtered: bool,
    legit_duplicate: Option<DuplicateContractPayload>,
    address_signal: Option<AddressSignalPayload>,
    victim_signal: Option<VictimSignalPayload>,
    infringing_tokens: Vec<InfringingTokenRecord>,
    malicious_addresses: Vec<MaliciousAddressPayload>,
    honest_addresses: Vec<HonestAddressPayload>,
    honest_address_stats: BTreeMap<String, HonestAddressStatsPayload>,
    secondary_sale_victim_addresses: Vec<SecondarySaleVictimAddressPayload>,
    address_attributions: Vec<AddressAttributionPayload>,
    market_events: Vec<NftMarketEventRecord>,
    mint_payment_edges: Vec<ValueFlowEdgePayload>,
    fraud_trade_stats: BTreeMap<String, FraudTradeStatsPayload>,
    nft_propagation_path: Option<NftPropagationPathPayload>,
}

#[derive(Default)]
struct AnalysisOutputState {
    legit_duplicates: Vec<DuplicateContractPayload>,
    legit_contract_addresses: BTreeSet<String>,
    address_signals: BTreeMap<String, AddressSignalPayload>,
    victim_signals: BTreeMap<String, VictimSignalPayload>,
    infringing_tokens: Vec<InfringingTokenRecord>,
    malicious_addresses: Vec<MaliciousAddressPayload>,
    honest_addresses: Vec<HonestAddressPayload>,
    honest_address_stats: BTreeMap<String, HonestAddressStatsPayload>,
    secondary_sale_victim_addresses: Vec<SecondarySaleVictimAddressPayload>,
    address_attributions: Vec<AddressAttributionPayload>,
    market_events: Vec<NftMarketEventRecord>,
    mint_payment_edges: Vec<ValueFlowEdgePayload>,
    fraud_trade_stats: BTreeMap<String, FraudTradeStatsPayload>,
    nft_propagation_paths: BTreeMap<String, NftPropagationPathPayload>,
    candidate_contract_metadata: BTreeMap<String, ContractMetadata>,
    implausible_candidate_contracts: BTreeSet<String>,
}

impl AnalysisOutputState {
    fn with_seed_related_legit_duplicates(legit_duplicates: Vec<DuplicateContractPayload>) -> Self {
        let legit_contract_addresses = legit_duplicates
            .iter()
            .map(|item| item.contract_address.clone())
            .collect();
        Self {
            legit_duplicates,
            legit_contract_addresses,
            ..Self::default()
        }
    }
}

#[derive(Clone, Debug)]
pub struct BatchRequest {
    pub chain: String,
    pub seed_file: PathBuf,
    pub output_dir: PathBuf,
    pub alchemy_api_key: String,
    pub alchemy_network: Option<String>,
    pub etherscan_api_key: String,
    pub opensea_api_key: String,
    pub name_threshold: f64,
    pub metadata_threshold: f64,
    pub timeout_seconds: u64,
    pub api_max_concurrency: usize,
    pub contract_max_concurrency: usize,
    pub sale_metric_max_concurrency: usize,
    pub max_tokens_per_contract: usize,
    pub max_recall_rows: usize,
    pub seed_metadata_max_concurrency: usize,
    pub cpu_max_concurrency: usize,
    pub workers: usize,
}

impl Default for BatchRequest {
    fn default() -> Self {
        Self {
            chain: "ethereum".into(),
            seed_file: PathBuf::new(),
            output_dir: PathBuf::from("result"),
            alchemy_api_key: String::new(),
            alchemy_network: None,
            etherscan_api_key: String::new(),
            opensea_api_key: String::new(),
            name_threshold: DEFAULT_NAME_THRESHOLD,
            metadata_threshold: DEFAULT_METADATA_THRESHOLD,
            timeout_seconds: DEFAULT_TIMEOUT_SECONDS,
            api_max_concurrency: 8,
            contract_max_concurrency: 4,
            sale_metric_max_concurrency: 4,
            max_tokens_per_contract: 0,
            max_recall_rows: 0,
            seed_metadata_max_concurrency: 1,
            cpu_max_concurrency: 1,
            workers: 1,
        }
    }
}

impl FeatureStoreReader for DuckDbFeatureStore {
    fn load_snapshot(
        &self,
        chain: &str,
        seed_nfts: &[SeedNft],
        name_threshold: f64,
        metadata_threshold: f64,
        max_tokens_per_contract: usize,
        max_recall_rows: usize,
    ) -> Result<DatabaseSnapshot, AppError> {
        DuckDbFeatureStore::load_snapshot(
            self,
            chain,
            seed_nfts,
            name_threshold,
            metadata_threshold,
            max_tokens_per_contract,
            max_recall_rows,
        )
    }

    fn load_snapshots(
        &self,
        chain: &str,
        seeds: &[(String, Vec<SeedNft>)],
        name_threshold: f64,
        metadata_threshold: f64,
        max_tokens_per_contract: usize,
        max_recall_rows: usize,
    ) -> Result<BTreeMap<String, DatabaseSnapshot>, AppError> {
        DuckDbFeatureStore::load_snapshots(
            self,
            chain,
            seeds,
            name_threshold,
            metadata_threshold,
            max_tokens_per_contract,
            max_recall_rows,
        )
    }
}

impl SignalCacheStore for ContractSignalCache {
    fn get(
        &self,
        chain: &str,
        contract_address: &str,
        token_type: &str,
    ) -> Result<Option<CachedSignals>, AppError> {
        ContractSignalCache::get(self, chain, contract_address, token_type)
    }

    fn put(
        &self,
        chain: &str,
        contract_address: &str,
        token_type: &str,
        transfers: &[TransferRecord],
        owners: &[OwnerBalance],
    ) -> Result<(), AppError> {
        ContractSignalCache::put(self, chain, contract_address, token_type, transfers, owners)
    }
}

pub async fn analyze_seed_contract(
    request: AnalyzeRequest,
    deps: &AnalysisDeps,
) -> Result<SingleReportPayload, AppError> {
    analyze_seed_contract_with_progress(request, deps, deps.progress.clone()).await
}

async fn fetch_seed_context(
    request: &AnalyzeRequest,
    deps: &AnalysisDeps,
    runtime_limits: &RuntimeLimits,
    progress: Arc<dyn SeedProgressReporter>,
) -> Result<SeedContext, AppError> {
    progress.on_seed_stage("fetch_seed_context").await;
    let seed_metadata_limit = runtime_limits.seed_metadata_limit.clone();
    let (seed_contract, seed_nfts) = tokio::try_join!(
        async {
            let _permit = acquire_optional_limit(&seed_metadata_limit).await?;
            deps.api
                .fetch_contract_metadata(
                    &request.chain,
                    &request.alchemy_api_key,
                    request.alchemy_network.as_deref(),
                    &request.opensea_api_key,
                    &request.seed_contract_address,
                )
                .await
        },
        deps.api.fetch_seed_contract_nfts(
            &request.chain,
            &request.alchemy_api_key,
            request.alchemy_network.as_deref(),
            &request.seed_contract_address,
        )
    )?;

    progress.on_seed_stage("fetch_license_sample").await;
    let open_license = deps
        .api
        .fetch_license_sample(
            &request.chain,
            &request.alchemy_api_key,
            request.alchemy_network.as_deref(),
            &seed_nfts,
        )
        .await?;

    Ok(SeedContext {
        seed_contract,
        seed_nfts,
        open_license,
    })
}

async fn build_candidate_plan_for_seed(
    request: AnalyzeRequest,
    feature_store: Arc<dyn FeatureStoreReader>,
    context: SeedContext,
    cpu_limit: Option<Arc<Semaphore>>,
    progress: Arc<dyn SeedProgressReporter>,
) -> Result<(SeedContext, CandidatePlan), AppError> {
    progress.on_seed_stage("load_snapshot").await;
    let _permit = acquire_optional_limit(&cpu_limit).await?;
    let (request, context, snapshot) = tokio::task::spawn_blocking(move || {
        let snapshot = feature_store.load_snapshot(
            &request.chain,
            &context.seed_nfts,
            request.name_threshold,
            request.metadata_threshold,
            request.max_tokens_per_contract,
            request.max_recall_rows,
        )?;
        Ok::<_, AppError>((request, context, snapshot))
    })
    .await
    .map_err(|err| AppError::InvalidData(format!("snapshot CPU task failed: {err}")))??;

    progress.on_seed_stage("find_duplicate_candidates").await;
    tokio::task::spawn_blocking(move || {
        let candidates =
            if snapshot.duplicate_contract_rows.is_empty() && !snapshot.nft_rows.is_empty() {
                duplicate::build_duplicate_candidates(
                    &request.chain,
                    &context.seed_nfts,
                    &snapshot.nft_rows,
                    request.name_threshold,
                    request.metadata_threshold,
                )
            } else {
                duplicate::build_duplicate_candidates_from_contract_rows(
                    &request.chain,
                    &context.seed_nfts,
                    &snapshot.duplicate_contract_rows,
                    request.name_threshold,
                    request.metadata_threshold,
                )
            };
        Ok::<_, AppError>((
            context,
            CandidatePlan {
                snapshot,
                candidates,
            },
        ))
    })
    .await
    .map_err(|err| AppError::InvalidData(format!("candidate CPU task failed: {err}")))?
}

fn build_candidate_plan_from_snapshot(
    request: &AnalyzeRequest,
    context: &SeedContext,
    snapshot: DatabaseSnapshot,
) -> CandidatePlan {
    let candidates = if snapshot.duplicate_contract_rows.is_empty() && !snapshot.nft_rows.is_empty()
    {
        duplicate::build_duplicate_candidates(
            &request.chain,
            &context.seed_nfts,
            &snapshot.nft_rows,
            request.name_threshold,
            request.metadata_threshold,
        )
    } else {
        duplicate::build_duplicate_candidates_from_contract_rows(
            &request.chain,
            &context.seed_nfts,
            &snapshot.duplicate_contract_rows,
            request.name_threshold,
            request.metadata_threshold,
        )
    };
    CandidatePlan {
        snapshot,
        candidates,
    }
}

pub async fn analyze_seed_contract_with_progress(
    request: AnalyzeRequest,
    deps: &AnalysisDeps,
    progress: Arc<dyn SeedProgressReporter>,
) -> Result<SingleReportPayload, AppError> {
    analyze_seed_contract_with_limits(
        request,
        deps,
        progress,
        None,
        RuntimeLimits::default(),
        None,
    )
    .await
}

async fn analyze_seed_contract_with_limits(
    request: AnalyzeRequest,
    deps: &AnalysisDeps,
    progress: Arc<dyn SeedProgressReporter>,
    cpu_limit: Option<Arc<Semaphore>>,
    runtime_limits: RuntimeLimits,
    prepared: Option<(SeedContext, CandidatePlan)>,
) -> Result<SingleReportPayload, AppError> {
    let (context, plan) = if let Some(prepared) = prepared {
        prepared
    } else {
        let context = fetch_seed_context(&request, deps, &runtime_limits, progress.clone()).await?;
        build_candidate_plan_for_seed(
            request.clone(),
            deps.feature_store.clone(),
            context,
            cpu_limit.clone(),
            progress.clone(),
        )
        .await?
    };
    let SeedContext {
        seed_contract,
        seed_nfts,
        open_license,
    } = context;
    let CandidatePlan {
        snapshot,
        candidates,
    } = plan;
    let contract_concurrency = request.contract_max_concurrency.max(1);
    let token_type = payload_token_type(&seed_contract);
    let CandidateContractFilterResult {
        candidates,
        seed_related_legit_duplicates,
    } = filter_seed_related_candidate_contracts(
        &request,
        deps,
        candidates,
        token_type.as_str(),
        contract_concurrency,
        &runtime_limits,
    )
    .await;
    let grouped = group_candidates_by_contract(&candidates);

    let contracts_to_analyze: Vec<String> = grouped.keys().cloned().collect();
    let snapshot_rows_by_key: HashMap<(String, String), _> = snapshot
        .nft_rows
        .iter()
        .map(|row| ((row.contract_address.clone(), row.token_id.clone()), row))
        .collect();
    let candidate_open_license_by_token: HashMap<(String, String), bool> = candidates
        .iter()
        .map(|candidate| {
            let key = (
                candidate.contract_address.clone(),
                candidate.token_id.clone(),
            );
            let is_open = snapshot_rows_by_key
                .get(&key)
                .map(|row| is_candidate_open_license(&row.metadata_json))
                .unwrap_or(false);
            (key, is_open)
        })
        .collect();
    let official_addresses: HashSet<String> = [
        seed_contract.contract_deployer.clone(),
        seed_contract.contract_address.clone(),
    ]
    .into_iter()
    .filter(|value| !value.is_empty())
    .collect();
    let mut output_state =
        AnalysisOutputState::with_seed_related_legit_duplicates(seed_related_legit_duplicates);
    let mut expanded_candidates_by_contract = BTreeMap::new();
    let analysis_timestamp = chrono::Utc::now().timestamp();
    let seed_deployed_block_number = seed_contract.deployed_block_number;
    if !open_license {
        progress
            .on_duplicate_contracts_started(contracts_to_analyze.len())
            .await;

        let mut completed_contracts = 0;
        let snapshot_token_index = SnapshotTokenIndex::new(&snapshot.nft_rows);
        let request_ref = &request;
        let deps_ref = deps;
        let token_type_ref = token_type.as_str();
        let grouped_ref = &grouped;
        let candidates_ref = &candidates;
        let snapshot_token_index_ref = &snapshot_token_index;
        let official_addresses_ref = &official_addresses;
        let candidate_open_license_by_token_ref = &candidate_open_license_by_token;
        let runtime_limits_ref = &runtime_limits;
        let mut contract_analyses = stream::iter(contracts_to_analyze.iter().enumerate().map(
            |(index, contract_address)| async move {
                let contract_metadata = fetch_candidate_contract_metadata(
                    request_ref,
                    deps_ref,
                    contract_address,
                    runtime_limits_ref,
                )
                .await?;
                if deployed_before_seed(seed_deployed_block_number, contract_metadata.as_ref()) {
                    let result =
                        implausible_candidate_filtered_result(contract_address, contract_metadata);
                    return Ok::<_, AppError>((
                        index,
                        contract_address.clone(),
                        Vec::new(),
                        result,
                    ));
                }
                let contract_candidates = fetch_and_expand_contract_candidates(
                    request_ref,
                    deps_ref,
                    contract_address,
                    grouped_ref,
                    candidates_ref,
                    snapshot_token_index_ref,
                    runtime_limits_ref,
                )
                .await?;
                let result = analyze_duplicate_contract(DuplicateContractAnalysisInput {
                    request: request_ref,
                    deps: deps_ref,
                    token_type: token_type_ref,
                    contract_address,
                    contract_candidates: &contract_candidates,
                    contract_metadata,
                    official_addresses: official_addresses_ref,
                    candidate_open_license_by_token: candidate_open_license_by_token_ref,
                    analysis_timestamp,
                    runtime_limits: runtime_limits_ref,
                })
                .await?;
                Ok::<_, AppError>((index, contract_address.clone(), contract_candidates, result))
            },
        ))
        .buffer_unordered(contract_concurrency);

        let mut pending_contract_results = BTreeMap::new();
        let mut next_contract_index_to_merge = 0usize;
        while let Some(result) = contract_analyses.next().await {
            let (index, contract_address, contract_candidates, result) = result?;
            completed_contracts += 1;
            progress
                .on_duplicate_contract_completed(
                    &contract_address,
                    completed_contracts,
                    contracts_to_analyze.len(),
                )
                .await;
            expanded_candidates_by_contract.insert(contract_address, contract_candidates);
            pending_contract_results.insert(index, result);
            while let Some(result) = pending_contract_results.remove(&next_contract_index_to_merge)
            {
                merge_contract_analysis_result(result, &mut output_state);
                next_contract_index_to_merge += 1;
            }
        }
    }
    expanded_candidates_by_contract.retain(|contract, _| {
        !output_state
            .implausible_candidate_contracts
            .contains(contract)
            && !output_state.legit_contract_addresses.contains(contract)
    });
    output_state
        .candidate_contract_metadata
        .retain(|contract, _| {
            !output_state
                .implausible_candidate_contracts
                .contains(contract)
                && !output_state.legit_contract_addresses.contains(contract)
        });
    let mut duplicate_contracts = build_duplicate_contract_payloads(
        &expanded_candidates_by_contract,
        &output_state.candidate_contract_metadata,
    );
    duplicate_contracts.retain(|item| {
        !output_state
            .legit_contract_addresses
            .contains(&item.contract_address)
    });
    if open_license {
        duplicate_contracts.clear();
    }

    let seed_contract_payload = SeedContractPayload {
        chain: seed_contract.chain,
        contract_address: seed_contract.contract_address,
        name: seed_contract.name,
        symbol: seed_contract.symbol,
        token_type: seed_contract.token_type,
        contract_deployer: seed_contract.contract_deployer,
        deployed_block_number: seed_contract.deployed_block_number,
    };
    let lifecycle_contract_addresses: BTreeSet<String> = duplicate_contracts
        .iter()
        .map(|item| item.contract_address.clone())
        .collect();
    let lifecycle_candidates: Vec<DuplicateCandidate> = if open_license {
        Vec::new()
    } else {
        candidates
            .iter()
            .filter(|candidate| lifecycle_contract_addresses.contains(&candidate.contract_address))
            .cloned()
            .collect()
    };
    let output_candidates: Vec<DuplicateCandidate> =
        if output_state.implausible_candidate_contracts.is_empty() {
            candidates
                .into_iter()
                .filter(|candidate| {
                    !output_state
                        .legit_contract_addresses
                        .contains(&candidate.contract_address)
                })
                .collect()
        } else {
            candidates
                .into_iter()
                .filter(|candidate| {
                    !output_state
                        .implausible_candidate_contracts
                        .contains(&candidate.contract_address)
                        && !output_state
                            .legit_contract_addresses
                            .contains(&candidate.contract_address)
                })
                .collect()
        };
    let summary_grouped = group_candidates_by_contract(&output_candidates);
    let mut lifecycle_outputs =
        lifecycle::build_lifecycle_model_outputs(lifecycle::LifecycleModelInput {
            seed_contract: &seed_contract_payload,
            duplicate_candidates: &lifecycle_candidates,
            duplicate_contracts: &duplicate_contracts,
            address_attributions: &output_state.address_attributions,
            nft_propagation_paths: &output_state.nft_propagation_paths,
            mint_payment_edges: &output_state.mint_payment_edges,
            market_events: &output_state.market_events,
        });

    let victim_acquisition_addresses = build_victim_acquisition_addresses(
        &output_state.secondary_sale_victim_addresses,
        &output_state.address_attributions,
        &lifecycle_outputs.value_flow_edges,
        &output_state.nft_propagation_paths,
    );
    output_state.address_attributions =
        address_records::add_acquisition_exposure_attribution_evidence(
            output_state.address_attributions,
            &victim_acquisition_addresses,
        );
    lifecycle_outputs = lifecycle::build_lifecycle_model_outputs(lifecycle::LifecycleModelInput {
        seed_contract: &seed_contract_payload,
        duplicate_candidates: &lifecycle_candidates,
        duplicate_contracts: &duplicate_contracts,
        address_attributions: &output_state.address_attributions,
        nft_propagation_paths: &output_state.nft_propagation_paths,
        mint_payment_edges: &output_state.mint_payment_edges,
        market_events: &output_state.market_events,
    });

    let payload = SingleReportPayload {
        seed_contract: seed_contract_payload,
        seed_collection_stats: build_seed_collection_stats(&seed_nfts),
        duplicate_candidates: output_candidates,
        contract_level_summary: build_contract_level_summary(&expanded_candidates_by_contract),
        report_summary: build_report_summary(ReportSummaryInput {
            open_license,
            grouped: &summary_grouped,
            implausible_candidate_contract_count: output_state.implausible_candidate_contracts.len()
                as i64,
            legit_duplicates: &output_state.legit_duplicates,
            infringing_tokens: &output_state.infringing_tokens,
            malicious_addresses: &output_state.malicious_addresses,
            honest_addresses: &output_state.honest_addresses,
            secondary_sale_victim_addresses: &output_state.secondary_sale_victim_addresses,
            victim_acquisition_addresses: &victim_acquisition_addresses,
            address_signals: &output_state.address_signals,
            address_attributions: &output_state.address_attributions,
            value_flow_edges: &lifecycle_outputs.value_flow_edges,
            propagation_paths: &output_state.nft_propagation_paths,
            lifecycle_metrics: &lifecycle_outputs.lifecycle_metrics,
        }),
        duplicate_contracts,
        legit_duplicates: output_state.legit_duplicates,
        address_signals: output_state.address_signals,
        victim_signals: output_state.victim_signals,
        infringing_tokens: output_state.infringing_tokens,
        malicious_addresses: output_state.malicious_addresses,
        honest_addresses: output_state.honest_addresses,
        honest_address_stats: output_state.honest_address_stats,
        secondary_sale_victim_addresses: output_state.secondary_sale_victim_addresses,
        victim_acquisition_addresses,
        address_attributions: output_state.address_attributions,
        contract_lifecycle_events: lifecycle_outputs.contract_lifecycle_events,
        address_evidence_features: lifecycle_outputs.address_evidence_features,
        value_flow_edges: lifecycle_outputs.value_flow_edges,
        content_similarity_edges: lifecycle_outputs.content_similarity_edges,
        campaign_clusters: lifecycle_outputs.campaign_clusters,
        lifecycle_metrics: lifecycle_outputs.lifecycle_metrics,
        weak_supervision_labels: lifecycle_outputs.weak_supervision_labels,
        early_detection_features: lifecycle_outputs.early_detection_features,
        market_events: output_state.market_events,
        fraud_trade_stats: output_state.fraud_trade_stats,
        nft_propagation_paths: output_state.nft_propagation_paths,
    };
    progress.on_seed_stage("finalize_report").await;
    progress.on_seed_completed().await;
    Ok(payload)
}

fn is_candidate_open_license(metadata_json: &str) -> bool {
    let mut payload = serde_json::Map::new();
    if !metadata_json.trim().is_empty() {
        match serde_json::from_str::<serde_json::Value>(metadata_json) {
            Ok(serde_json::Value::Object(object)) => {
                payload.extend(object);
            }
            Ok(other) => {
                payload.insert("metadata_json".into(), other);
            }
            Err(_) => {
                payload.insert(
                    "metadata_json".into(),
                    serde_json::Value::String(metadata_json.to_string()),
                );
            }
        }
    }
    if payload.is_empty() {
        return false;
    }

    let haystack = serde_json::Value::Object(payload)
        .to_string()
        .to_lowercase();
    [
        "cc0-1.0",
        "license: cc0",
        "creative commons zero",
        "public domain",
        "cc zero",
    ]
    .iter()
    .any(|needle| haystack.contains(needle))
}

fn analyze_victim_signals_from_active_sellers(
    transfers: &[TransferRecord],
    owners: &[OwnerBalance],
) -> VictimSignalPayload {
    let active_sellers: BTreeSet<&str> = transfers
        .iter()
        .map(|item| item.from_address.as_str())
        .filter(|address| !address.is_empty() && *address != crate::models::ZERO_ADDRESS)
        .collect();
    let mut owner_count = 0_i64;
    let mut stuck_holder_count = 0_i64;
    for owner in owners {
        if owner.owner_address.is_empty() || owner.owner_address == crate::models::ZERO_ADDRESS {
            continue;
        }
        if !owner.token_balances.values().any(|balance| *balance > 0) {
            continue;
        }
        owner_count += 1;
        if !active_sellers.contains(owner.owner_address.as_str()) {
            stuck_holder_count += 1;
        }
    }
    VictimSignalPayload {
        owner_count,
        stuck_holder_count,
        stuck_holder_ratio: if owner_count > 0 {
            Some(stuck_holder_count as f64 / owner_count as f64)
        } else {
            Some(0.0)
        },
        victim_wallet_count: stuck_holder_count,
    }
}

fn map_address_signals(signals: &crate::models::AddressSignals) -> AddressSignalPayload {
    AddressSignalPayload {
        mint_address_count: signals.mint_address_count as i64,
        mint_count: signals.mint_count as i64,
        unique_receiver_count: signals.unique_receiver_count as i64,
        cycle_edge_count: signals.cycle_edge_count as i64,
        star_distributor_count: signals.star_distributor_count as i64,
        first_transfer_delay_seconds: signals.first_transfer_delay_seconds,
        fast_spread: signals.fast_spread,
    }
}

#[cfg(test)]
mod tests;
