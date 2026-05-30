use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use futures::{stream, StreamExt};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::error::AppError;
use crate::models::{
    AddressAttributionPayload, AddressSignalPayload, ContractMetadata, DatabaseSnapshot,
    DuplicateCandidate, DuplicateContractPayload, HonestAddressPayload, InfringingTokenRecord,
    MaliciousAddressPayload, NftMarketEventRecord, NftPropagationPathPayload, OwnerBalance,
    PaperStatsPayload, SecondarySaleVictimAddressPayload, SeedContractPayload, SeedNft,
    SingleReportPayload, TransferRecord, ValueFlowEdgePayload, VictimSignalPayload,
};
use crate::progress::{BatchProgressReporter, SeedProgressReporter};
use crate::store::DuckDbFeatureStore;

pub mod address_records;
mod api;
mod batch;
mod candidate_filter;
mod contract_analysis;
pub mod duplicate;
pub mod lifecycle;
pub mod paper_stats;
pub mod propagation;
pub mod scoring;
pub mod signals;
mod summary;
mod value_flow;

pub use api::{AnalyzeApi, CandidateSeedHolderRequest, RealApi};
pub use batch::{read_seed_addresses, run_batch};
pub use candidate_filter::group_candidates_by_contract;

use candidate_filter::*;
use contract_analysis::*;
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
    pub matched_contract_max_concurrency: usize,
    pub max_tokens_per_contract: usize,
    pub max_recall_rows: usize,
    pub paper_stats_config: paper_stats::PaperStatsConfig,
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
            matched_contract_max_concurrency: 1,
            max_tokens_per_contract: 0,
            max_recall_rows: 0,
            paper_stats_config: paper_stats::PaperStatsConfig::default(),
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

#[derive(Clone)]
pub struct AnalysisDeps {
    pub api: Arc<dyn AnalyzeApi>,
    pub feature_store: Arc<dyn FeatureStoreReader>,
    pub progress: Arc<dyn SeedProgressReporter>,
    pub batch_progress: Arc<dyn BatchProgressReporter>,
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

struct MatchedContractAnalysisOutput {
    contract_address: String,
    contract_candidates: Vec<DuplicateCandidate>,
    result: ContractAnalysisResult,
}

struct MatchedContractAnalysisContext<'a> {
    request: AnalyzeRequest,
    deps: AnalysisDeps,
    token_type: String,
    grouped: Arc<BTreeMap<String, Vec<usize>>>,
    candidates: Arc<Vec<DuplicateCandidate>>,
    snapshot_token_index: Arc<SnapshotTokenIndex<'a>>,
    official_addresses: Arc<HashSet<String>>,
    candidate_open_license_by_token: Arc<HashMap<(String, String), bool>>,
    seed_deployed_block_number: i64,
    analysis_timestamp: i64,
}

fn seed_nfts_for_duplicate_matching(
    seed_nfts: &[SeedNft],
    seed_contract: &ContractMetadata,
) -> Vec<SeedNft> {
    let contract_name = seed_contract.name.trim();
    if seed_nfts.is_empty() {
        return if contract_name.is_empty() {
            Vec::new()
        } else {
            vec![SeedNft {
                chain: seed_contract.chain.clone(),
                contract_address: seed_contract.contract_address.clone(),
                name: contract_name.to_string(),
                symbol: seed_contract.symbol.clone(),
                ..SeedNft::default()
            }]
        };
    }

    let mut dedup_seed_nfts = seed_nfts.to_vec();
    for seed_nft in &mut dedup_seed_nfts {
        seed_nft.name.clear();
    }
    if !contract_name.is_empty() {
        if let Some(first_seed_nft) = dedup_seed_nfts.first_mut() {
            first_seed_nft.name = contract_name.to_string();
        }
    }
    dedup_seed_nfts
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
    seed_contract: SeedContractPayload,
    paper_stats: PaperStatsPayload,
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
    secondary_sale_victim_addresses: Vec<SecondarySaleVictimAddressPayload>,
    address_attributions: Vec<AddressAttributionPayload>,
    market_events: Vec<NftMarketEventRecord>,
    mint_payment_edges: Vec<ValueFlowEdgePayload>,
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
    secondary_sale_victim_addresses: Vec<SecondarySaleVictimAddressPayload>,
    address_attributions: Vec<AddressAttributionPayload>,
    market_events: Vec<NftMarketEventRecord>,
    mint_payment_edges: Vec<ValueFlowEdgePayload>,
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

struct SeedAnalysisState {
    request: AnalyzeRequest,
    seed_contract: ContractMetadata,
    seed_nfts: Vec<SeedNft>,
    open_license: bool,
    snapshot: DatabaseSnapshot,
    candidates: Vec<DuplicateCandidate>,
    token_type: String,
    grouped: BTreeMap<String, Vec<usize>>,
    contracts_to_analyze: Vec<String>,
    candidate_open_license_by_token: HashMap<(String, String), bool>,
    official_addresses: HashSet<String>,
    output_state: AnalysisOutputState,
    expanded_candidates_by_contract: BTreeMap<String, Vec<DuplicateCandidate>>,
    analysis_timestamp: i64,
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
    pub matched_contract_max_concurrency: usize,
    pub max_tokens_per_contract: usize,
    pub max_recall_rows: usize,
    pub seed_network_max_concurrency: usize,
    pub seed_cpu_max_concurrency: usize,
    pub paper_stats_config: paper_stats::PaperStatsConfig,
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
            matched_contract_max_concurrency: 1,
            max_tokens_per_contract: 0,
            max_recall_rows: 0,
            seed_network_max_concurrency: 1,
            seed_cpu_max_concurrency: 1,
            paper_stats_config: paper_stats::PaperStatsConfig::default(),
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

pub async fn analyze_seed_contract(
    request: AnalyzeRequest,
    deps: &AnalysisDeps,
) -> Result<SingleReportPayload, AppError> {
    analyze_seed_contract_with_progress(request, deps, deps.progress.clone()).await
}

async fn fetch_seed_context(
    request: &AnalyzeRequest,
    deps: &AnalysisDeps,
    progress: Arc<dyn SeedProgressReporter>,
) -> Result<SeedContext, AppError> {
    progress.on_seed_stage("fetch_seed_context").await;
    let (seed_contract, seed_nfts) = tokio::try_join!(
        async {
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
    let (request, context, snapshot, dedup_seed_nfts) = tokio::task::spawn_blocking(move || {
        let dedup_seed_nfts =
            seed_nfts_for_duplicate_matching(&context.seed_nfts, &context.seed_contract);
        let snapshot = feature_store.load_snapshot(
            &request.chain,
            &dedup_seed_nfts,
            request.name_threshold,
            request.metadata_threshold,
            request.max_tokens_per_contract,
            request.max_recall_rows,
        )?;
        Ok::<_, AppError>((request, context, snapshot, dedup_seed_nfts))
    })
    .await
    .map_err(|err| AppError::InvalidData(format!("snapshot CPU task failed: {err}")))??;

    progress.on_seed_stage("find_duplicate_candidates").await;
    tokio::task::spawn_blocking(move || {
        let candidates =
            if snapshot.duplicate_contract_rows.is_empty() && !snapshot.nft_rows.is_empty() {
                duplicate::build_duplicate_candidates(
                    &request.chain,
                    &dedup_seed_nfts,
                    &snapshot.nft_rows,
                    request.name_threshold,
                    request.metadata_threshold,
                )
            } else {
                duplicate::build_duplicate_candidates_from_contract_rows(
                    &request.chain,
                    &dedup_seed_nfts,
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

pub async fn analyze_seed_contract_with_progress(
    request: AnalyzeRequest,
    deps: &AnalysisDeps,
    progress: Arc<dyn SeedProgressReporter>,
) -> Result<SingleReportPayload, AppError> {
    analyze_seed_contract_with_limits(request, deps, progress, None, None).await
}

async fn analyze_seed_contract_with_limits(
    request: AnalyzeRequest,
    deps: &AnalysisDeps,
    progress: Arc<dyn SeedProgressReporter>,
    cpu_limit: Option<Arc<Semaphore>>,
    prepared: Option<(SeedContext, CandidatePlan)>,
) -> Result<SingleReportPayload, AppError> {
    let matched_contract_limit = Arc::new(Semaphore::new(
        request.matched_contract_max_concurrency.max(1),
    ));
    let mut state =
        prepare_seed_analysis_state(request, deps, progress.clone(), cpu_limit, prepared).await?;
    analyze_matched_contracts_parallel(&mut state, deps, progress.clone(), matched_contract_limit)
        .await?;
    finalize_seed_report(state, progress).await
}

async fn prepare_seed_analysis_state(
    request: AnalyzeRequest,
    deps: &AnalysisDeps,
    progress: Arc<dyn SeedProgressReporter>,
    cpu_limit: Option<Arc<Semaphore>>,
    prepared: Option<(SeedContext, CandidatePlan)>,
) -> Result<SeedAnalysisState, AppError> {
    let (context, plan) = if let Some(prepared) = prepared {
        prepared
    } else {
        let context = fetch_seed_context(&request, deps, progress.clone()).await?;
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
    let token_type = payload_token_type(&seed_contract);
    let CandidateContractFilterResult {
        candidates,
        seed_related_legit_duplicates,
    } = filter_seed_related_candidate_contracts(
        &request,
        deps,
        candidates,
        token_type.as_str(),
        request.api_max_concurrency.max(1),
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
    let output_state =
        AnalysisOutputState::with_seed_related_legit_duplicates(seed_related_legit_duplicates);
    let expanded_candidates_by_contract = BTreeMap::new();
    let analysis_timestamp = chrono::Utc::now().timestamp();

    Ok(SeedAnalysisState {
        request,
        seed_contract,
        seed_nfts,
        open_license,
        snapshot,
        candidates,
        token_type,
        grouped,
        contracts_to_analyze,
        candidate_open_license_by_token,
        official_addresses,
        output_state,
        expanded_candidates_by_contract,
        analysis_timestamp,
    })
}

async fn analyze_matched_contracts_parallel(
    state: &mut SeedAnalysisState,
    deps: &AnalysisDeps,
    progress: Arc<dyn SeedProgressReporter>,
    matched_contract_limit: Arc<Semaphore>,
) -> Result<(), AppError> {
    if state.open_license {
        return Ok(());
    }
    progress
        .on_duplicate_contracts_started(state.contracts_to_analyze.len())
        .await;

    let mut completed_contracts = 0;
    let mut outputs = {
        let snapshot_token_index = SnapshotTokenIndex::new(&state.snapshot.nft_rows);
        let context = Arc::new(MatchedContractAnalysisContext {
            request: state.request.clone(),
            deps: deps.clone(),
            token_type: state.token_type.clone(),
            grouped: Arc::new(state.grouped.clone()),
            candidates: Arc::new(state.candidates.clone()),
            snapshot_token_index: Arc::new(snapshot_token_index),
            official_addresses: Arc::new(state.official_addresses.clone()),
            candidate_open_license_by_token: Arc::new(
                state.candidate_open_license_by_token.clone(),
            ),
            seed_deployed_block_number: state.seed_contract.deployed_block_number,
            analysis_timestamp: state.analysis_timestamp,
        });
        let mut tasks = stream::iter(state.contracts_to_analyze.clone().into_iter().map(
            |contract_address| {
                let context = Arc::clone(&context);
                let matched_contract_limit = Arc::clone(&matched_contract_limit);
                async move {
                    let _permit = matched_contract_limit
                        .acquire_owned()
                        .await
                        .map_err(|err| {
                            AppError::InvalidData(format!("matched-contract limit closed: {err}"))
                        })?;
                    analyze_one_matched_contract(context, contract_address).await
                }
            },
        ))
        .buffer_unordered(state.request.matched_contract_max_concurrency.max(1));

        let mut outputs = BTreeMap::<String, MatchedContractAnalysisOutput>::new();
        while let Some(output) = tasks.next().await {
            let output = output?;
            completed_contracts += 1;
            progress
                .on_duplicate_contract_completed(
                    &output.contract_address,
                    completed_contracts,
                    state.contracts_to_analyze.len(),
                )
                .await;
            outputs.insert(output.contract_address.clone(), output);
        }
        outputs
    };

    for contract_address in &state.contracts_to_analyze {
        let Some(output) = outputs.remove(contract_address) else {
            continue;
        };
        state
            .expanded_candidates_by_contract
            .insert(contract_address.clone(), output.contract_candidates);
        merge_contract_analysis_result(output.result, &mut state.output_state);
    }
    Ok(())
}

async fn analyze_one_matched_contract(
    context: Arc<MatchedContractAnalysisContext<'_>>,
    contract_address: String,
) -> Result<MatchedContractAnalysisOutput, AppError> {
    let contract_metadata =
        fetch_candidate_contract_metadata(&context.request, &context.deps, &contract_address)
            .await?;
    let (contract_candidates, result) = if deployed_before_seed(
        context.seed_deployed_block_number,
        contract_metadata.as_ref(),
    ) {
        (
            Vec::new(),
            implausible_candidate_filtered_result(&contract_address, contract_metadata),
        )
    } else {
        let contract_candidates = fetch_and_expand_contract_candidates(
            &context.request,
            &context.deps,
            &contract_address,
            &context.grouped,
            &context.candidates,
            &context.snapshot_token_index,
        )
        .await?;
        let result = if current_supply_implausibly_smaller_than_candidates(
            &context.request,
            &context.deps,
            &contract_address,
            contract_candidates.len(),
        )
        .await
        {
            implausible_candidate_filtered_result(&contract_address, contract_metadata)
        } else {
            analyze_duplicate_contract(DuplicateContractAnalysisInput {
                request: &context.request,
                deps: &context.deps,
                token_type: context.token_type.as_str(),
                contract_address: &contract_address,
                contract_candidates: &contract_candidates,
                contract_metadata,
                official_addresses: &context.official_addresses,
                candidate_open_license_by_token: &context.candidate_open_license_by_token,
                analysis_timestamp: context.analysis_timestamp,
            })
            .await?
        };
        (contract_candidates, result)
    };
    Ok(MatchedContractAnalysisOutput {
        contract_address,
        contract_candidates,
        result,
    })
}

async fn finalize_seed_report(
    state: SeedAnalysisState,
    progress: Arc<dyn SeedProgressReporter>,
) -> Result<SingleReportPayload, AppError> {
    let SeedAnalysisState {
        request,
        seed_contract,
        seed_nfts,
        open_license,
        candidates,
        mut output_state,
        mut expanded_candidates_by_contract,
        analysis_timestamp,
        ..
    } = state;

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
    let value_flow_edges = output_state.mint_payment_edges.clone();
    let seed_collection_stats = build_seed_collection_stats(&seed_nfts);

    let victim_acquisition_addresses = build_victim_acquisition_addresses_excluding_malicious(
        &output_state.secondary_sale_victim_addresses,
        &output_state.address_attributions,
        &value_flow_edges,
        &output_state.nft_propagation_paths,
        &output_state.malicious_addresses,
    );
    let mut paper_stats_config = request.paper_stats_config;
    if paper_stats_config.analysis_timestamp <= 0 {
        paper_stats_config.analysis_timestamp = analysis_timestamp;
    }
    let paper_stats = paper_stats::build_paper_stats(paper_stats::PaperStatsInput {
        config: paper_stats_config,
        seed_collection_stats: &seed_collection_stats,
        duplicate_candidates: &output_candidates,
        duplicate_contracts: &duplicate_contracts,
        legit_duplicates: &output_state.legit_duplicates,
        infringing_tokens: &output_state.infringing_tokens,
        malicious_addresses: &output_state.malicious_addresses,
        victim_acquisition_addresses: &victim_acquisition_addresses,
        value_flow_edges: &value_flow_edges,
        nft_propagation_paths: &output_state.nft_propagation_paths,
    });

    let payload = SingleReportPayload {
        report_type: String::new(),
        seed_contract: seed_contract_payload,
        paper_stats,
        seed_collection_stats,
        duplicate_candidates: output_candidates,
        contract_level_summary: build_contract_level_summary(&expanded_candidates_by_contract),
        duplicate_contracts,
        legit_duplicates: output_state.legit_duplicates,
        address_signals: output_state.address_signals,
        victim_signals: output_state.victim_signals,
        infringing_tokens: output_state.infringing_tokens,
        malicious_addresses: output_state.malicious_addresses,
        honest_addresses: output_state.honest_addresses,
        secondary_sale_victim_addresses: output_state.secondary_sale_victim_addresses,
        victim_acquisition_addresses,
        address_attributions: output_state.address_attributions,
        contract_lifecycle_events: Vec::new(),
        address_evidence_features: Vec::new(),
        value_flow_edges,
        content_similarity_edges: Vec::new(),
        campaign_clusters: Vec::new(),
        lifecycle_metrics: Vec::new(),
        weak_supervision_labels: Vec::new(),
        early_detection_features: Vec::new(),
        market_events: output_state.market_events,
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
