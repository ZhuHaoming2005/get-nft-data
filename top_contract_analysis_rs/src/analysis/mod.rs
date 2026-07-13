use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use futures::{stream, StreamExt};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::error::AppError;
use crate::models::{
    AddressAttributionPayload, AddressSignalPayload, ContractMetadata, DatabaseSnapshot,
    DuplicateCandidate, DuplicateContractPayload, HonestAddressPayload, InfringingTokenRecord,
    MaliciousAddressPayload, NftPropagationPathPayload, OwnerBalance, ProviderDataQualityPayload,
    SecondarySaleVictimAddressPayload, SeedContractPayload, SeedNft, SingleReportPayload,
    TransferRecord, ValueFlowEdgePayload, VictimSignalPayload,
};
use crate::progress::{BatchProgressReporter, SeedProgressReporter};
use crate::store::DuckDbFeatureStore;

pub mod address_records;
mod api;
mod candidate_filter;
mod contract_analysis;
pub mod duplicate;
pub mod lifecycle;
pub mod multichain;
pub mod paper_stats;
pub mod propagation;
pub mod scoring;
pub mod signals;
mod summary;
mod value_flow;

pub use api::{AnalyzeApi, CandidateSeedHolderRequest, HeliusApiConfig, RealApi};
pub use candidate_filter::group_candidates_by_contract;
pub use multichain::{
    acquire_batch_output_lock, read_seed_contracts, run_multichain_batch,
    run_multichain_batch_with_lock, BatchOutputLock, MultiChainBatchRequest, MultiChainBatchResult,
};

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
            matched_contract_max_concurrency: 8,
            max_tokens_per_contract: 200,
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

    fn chain_totals(&self, _chain: &str) -> Result<crate::models::ChainTotalsPayload, AppError> {
        Err(AppError::InvalidData(
            "feature store does not expose chain totals".to_string(),
        ))
    }

    fn snapshot_identity(&self, chain: &str) -> Result<String, AppError> {
        let totals = self.chain_totals(chain)?;
        Ok(format!("{}:{}", totals.total_nfts, totals.total_contracts))
    }
}

#[derive(Clone)]
pub struct AnalysisDeps {
    pub api: Arc<dyn AnalyzeApi>,
    pub feature_store: Arc<dyn FeatureStoreReader>,
    pub progress: Arc<dyn SeedProgressReporter>,
    pub batch_progress: Arc<dyn BatchProgressReporter>,
}

#[derive(Clone)]
pub(super) struct SeedContext {
    seed_contract: ContractMetadata,
    seed_nfts: Vec<SeedNft>,
    open_license: bool,
}

pub(super) struct CandidatePlan {
    snapshot: DatabaseSnapshot,
    candidates: Vec<DuplicateCandidate>,
    candidate_open_license_by_token: HashMap<(String, String), bool>,
    estimated_memory_bytes: usize,
}

impl CandidatePlan {
    pub(super) fn estimated_memory_bytes(&self) -> usize {
        self.estimated_memory_bytes
    }
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

pub(super) fn seed_nfts_for_duplicate_matching(
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
    mint_payment_edges: Vec<ValueFlowEdgePayload>,
    attacker_cost_edges: Vec<ValueFlowEdgePayload>,
    nft_propagation_path: Option<NftPropagationPathPayload>,
    provider_data_quality: ProviderDataQualityPayload,
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
    mint_payment_edges: Vec<ValueFlowEdgePayload>,
    attacker_cost_edges: Vec<ValueFlowEdgePayload>,
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
    candidates: Arc<Vec<DuplicateCandidate>>,
    token_type: String,
    grouped: Arc<BTreeMap<String, Vec<usize>>>,
    contracts_to_analyze: Vec<String>,
    candidate_open_license_by_token: Arc<HashMap<(String, String), bool>>,
    official_addresses: Arc<HashSet<String>>,
    output_state: AnalysisOutputState,
    expanded_candidates_by_contract: BTreeMap<String, Vec<DuplicateCandidate>>,
    analysis_timestamp: i64,
    provider_data_quality: crate::models::ProviderDataQualityPayload,
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

    fn chain_totals(&self, chain: &str) -> Result<crate::models::ChainTotalsPayload, AppError> {
        DuckDbFeatureStore::chain_totals(self, chain)
    }

    fn snapshot_identity(&self, chain: &str) -> Result<String, AppError> {
        DuckDbFeatureStore::snapshot_identity(self, chain)
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

    build_candidate_plan_with_snapshot(request, context, snapshot, dedup_seed_nfts, progress).await
}

pub(super) async fn build_candidate_plan_with_snapshot(
    request: AnalyzeRequest,
    context: SeedContext,
    snapshot: DatabaseSnapshot,
    dedup_seed_nfts: Vec<SeedNft>,
    progress: Arc<dyn SeedProgressReporter>,
) -> Result<(SeedContext, CandidatePlan), AppError> {
    progress.on_seed_stage("find_duplicate_candidates").await;
    tokio::task::spawn_blocking(move || {
        let plan = build_candidate_plan_from_snapshot(&request, &dedup_seed_nfts, snapshot);
        Ok::<_, AppError>((context, plan))
    })
    .await
    .map_err(|err| AppError::InvalidData(format!("candidate CPU task failed: {err}")))?
}

fn build_candidate_plan_from_snapshot(
    request: &AnalyzeRequest,
    dedup_seed_nfts: &[SeedNft],
    mut snapshot: DatabaseSnapshot,
) -> CandidatePlan {
    let candidates = if snapshot.duplicate_contract_rows.is_empty() && !snapshot.nft_rows.is_empty()
    {
        duplicate::build_duplicate_candidates(
            &request.chain,
            dedup_seed_nfts,
            &snapshot.nft_rows,
            request.name_threshold,
            request.metadata_threshold,
        )
    } else {
        duplicate::build_duplicate_candidates_from_contract_rows(
            &request.chain,
            dedup_seed_nfts,
            &snapshot.duplicate_contract_rows,
            request.name_threshold,
            request.metadata_threshold,
        )
    };
    let candidate_contracts = candidates
        .iter()
        .map(|candidate| crate::models::normalize_chain_identity(&candidate.contract_address))
        .collect::<HashSet<_>>();
    snapshot.nft_rows.retain(|row| {
        candidate_contracts.contains(&crate::models::normalize_chain_identity(
            &row.contract_address,
        ))
    });
    let candidate_open_license_by_token = snapshot
        .nft_rows
        .iter()
        .map(|row| {
            (
                (row.contract_address.clone(), row.token_id.clone()),
                is_candidate_open_license(&row.metadata_json),
            )
        })
        .collect::<HashMap<_, _>>();
    // Candidate generation and license extraction are the only consumers of
    // the large metadata payload and prepared projections. Keep only the token
    // fields needed for provider-failure fallback before entering async API
    // analysis.
    for row in &mut snapshot.nft_rows {
        row.metadata_json = String::new();
    }
    snapshot.duplicate_contract_rows = Vec::new();
    snapshot.contract_names = Vec::new();
    snapshot.contract_signals = BTreeMap::new();
    let estimated_memory_bytes = estimate_compact_candidate_plan_bytes(
        &snapshot,
        &candidates,
        &candidate_open_license_by_token,
    );
    CandidatePlan {
        snapshot,
        candidates,
        candidate_open_license_by_token,
        estimated_memory_bytes,
    }
}

fn estimate_compact_candidate_plan_bytes(
    snapshot: &DatabaseSnapshot,
    candidates: &[DuplicateCandidate],
    licenses: &HashMap<(String, String), bool>,
) -> usize {
    let snapshot_bytes = snapshot
        .nft_rows
        .iter()
        .map(|row| {
            std::mem::size_of_val(row)
                .saturating_add(row.contract_address.capacity())
                .saturating_add(row.token_id.capacity())
                .saturating_add(row.token_uri.capacity())
                .saturating_add(row.image_uri.capacity())
                .saturating_add(row.name.capacity())
                .saturating_add(row.symbol.capacity())
        })
        .sum::<usize>()
        .saturating_add(
            snapshot
                .nft_rows
                .capacity()
                .saturating_mul(std::mem::size_of::<crate::models::DatabaseNftRecord>()),
        );
    let candidate_bytes = candidates
        .iter()
        .map(|candidate| {
            std::mem::size_of_val(candidate)
                .saturating_add(candidate.contract_address.capacity())
                .saturating_add(candidate.token_id.capacity())
                .saturating_add(candidate.confidence.capacity())
                .saturating_add(candidate.token_uri.capacity())
                .saturating_add(candidate.image_uri.capacity())
                .saturating_add(candidate.name.capacity())
                .saturating_add(candidate.symbol.capacity())
                .saturating_add(
                    candidate
                        .match_reasons
                        .iter()
                        .map(|reason| reason.capacity())
                        .sum::<usize>(),
                )
        })
        .sum::<usize>()
        .saturating_add(
            candidates
                .len()
                .saturating_mul(std::mem::size_of::<DuplicateCandidate>()),
        );
    let license_bytes = licenses
        .iter()
        .map(|((contract, token_id), _)| {
            contract
                .capacity()
                .saturating_add(token_id.capacity())
                .saturating_add(std::mem::size_of::<((String, String), bool)>())
        })
        .sum::<usize>()
        .saturating_add(
            licenses
                .capacity()
                .saturating_mul(std::mem::size_of::<((String, String), bool)>()),
        );
    // Downstream grouping/token indexes duplicate keys and vector headers.
    // Reserve 2x plus fixed allocator/runtime slack rather than under-account.
    snapshot_bytes
        .saturating_add(candidate_bytes)
        .saturating_add(license_bytes)
        .saturating_mul(2)
        .saturating_add(64_000_000)
}

pub async fn analyze_seed_contract_with_progress(
    request: AnalyzeRequest,
    deps: &AnalysisDeps,
    progress: Arc<dyn SeedProgressReporter>,
) -> Result<SingleReportPayload, AppError> {
    analyze_seed_contract_with_limits(request, deps, progress, None, None, None).await
}

pub(crate) struct ProviderEvidencePin<'a> {
    api: &'a dyn AnalyzeApi,
    chain: String,
    contract_address: String,
}

impl<'a> ProviderEvidencePin<'a> {
    pub(crate) fn new(api: &'a dyn AnalyzeApi, chain: &str, contract_address: &str) -> Self {
        api.set_provider_evidence_active(chain, contract_address, true);
        Self {
            api,
            chain: chain.to_string(),
            contract_address: contract_address.to_string(),
        }
    }
}

impl Drop for ProviderEvidencePin<'_> {
    fn drop(&mut self) {
        self.api
            .set_provider_evidence_active(&self.chain, &self.contract_address, false);
    }
}

async fn analyze_seed_contract_with_limits(
    request: AnalyzeRequest,
    deps: &AnalysisDeps,
    progress: Arc<dyn SeedProgressReporter>,
    cpu_limit: Option<Arc<Semaphore>>,
    prepared: Option<(SeedContext, CandidatePlan)>,
    matched_contract_limit: Option<Arc<Semaphore>>,
) -> Result<SingleReportPayload, AppError> {
    let _evidence_pin = ProviderEvidencePin::new(
        deps.api.as_ref(),
        &request.chain,
        &request.seed_contract_address,
    );
    analyze_seed_contract_with_limits_pinned(
        request,
        deps,
        progress,
        cpu_limit,
        prepared,
        matched_contract_limit,
    )
    .await
}

async fn analyze_seed_contract_with_limits_pinned(
    request: AnalyzeRequest,
    deps: &AnalysisDeps,
    progress: Arc<dyn SeedProgressReporter>,
    cpu_limit: Option<Arc<Semaphore>>,
    prepared: Option<(SeedContext, CandidatePlan)>,
    matched_contract_limit: Option<Arc<Semaphore>>,
) -> Result<SingleReportPayload, AppError> {
    let matched_contract_limit = matched_contract_limit.unwrap_or_else(|| {
        Arc::new(Semaphore::new(
            request.matched_contract_max_concurrency.max(1),
        ))
    });
    let mut state =
        prepare_seed_analysis_state(request, deps, progress.clone(), cpu_limit, prepared).await?;
    analyze_matched_contracts_parallel(&mut state, deps, progress.clone(), matched_contract_limit)
        .await?;
    if state
        .seed_contract
        .chain
        .eq_ignore_ascii_case(&state.request.chain)
    {
        let seed_quality = fetch_provider_quality_or_failure(
            deps.api.as_ref(),
            &state.request.chain,
            &state.seed_contract.contract_address,
        )
        .await;
        merge_provider_data_quality(&mut state.provider_data_quality, seed_quality);
    }
    finalize_seed_report(state, progress).await
}

async fn fetch_provider_quality_or_failure(
    api: &dyn AnalyzeApi,
    chain: &str,
    contract_address: &str,
) -> ProviderDataQualityPayload {
    match api
        .fetch_provider_data_quality(chain, contract_address)
        .await
    {
        Ok(quality) => quality,
        Err(error) => {
            eprintln!(
                "warning: provider data-quality lookup failed for {contract_address}: {error}; preserving the completed analysis with degraded quality metadata"
            );
            ProviderDataQualityPayload {
                supplemental_provider_failure_count: 1,
                provider_quality_lookup_failure_count: 1,
                ..ProviderDataQualityPayload::default()
            }
        }
    }
}

fn merge_provider_data_quality(
    target: &mut ProviderDataQualityPayload,
    source: ProviderDataQualityPayload,
) {
    let target_has_history =
        target.asset_listing_analyzed_count > 0 || target.history_requested_asset_count > 0;
    let source_has_history =
        source.asset_listing_analyzed_count > 0 || source.history_requested_asset_count > 0;
    if source_has_history {
        target.history_complete = if target_has_history {
            target.history_complete && source.history_complete
        } else {
            source.history_complete
        };
    }
    let has_unknown_quality = target.provider_quality_lookup_failure_count > 0
        || source.provider_quality_lookup_failure_count > 0;
    target.asset_listing_analyzed_count += source.asset_listing_analyzed_count;
    target.asset_listing_total_count += source.asset_listing_total_count;
    target.asset_listing_truncated_contract_count += source.asset_listing_truncated_contract_count;
    target.asset_listing_unknown_total_contract_count +=
        source.asset_listing_unknown_total_contract_count;
    target.asset_listing_coverage_ratio = None;
    target.history_failed_asset_count += source.history_failed_asset_count;
    target.history_requested_asset_count += source.history_requested_asset_count;
    target.history_successful_asset_count += source.history_successful_asset_count;
    target.history_complete_asset_count += source.history_complete_asset_count;
    target.history_unrequested_asset_count += source.history_unrequested_asset_count;
    target.history_truncated_asset_count += source.history_truncated_asset_count;
    target.history_fetched_transaction_count += source.history_fetched_transaction_count;
    target.history_reported_transaction_count += source.history_reported_transaction_count;
    target.history_failed_transaction_count += source.history_failed_transaction_count;
    target.history_signature_discovery_failure_count +=
        source.history_signature_discovery_failure_count;
    target.history_transaction_detail_failure_count +=
        source.history_transaction_detail_failure_count;
    target.history_unattributed_sol_transaction_count +=
        source.history_unattributed_sol_transaction_count;
    target.history_unresolved_compressed_mint_count +=
        source.history_unresolved_compressed_mint_count;
    target.mint_pre_balance_unavailable_count += source.mint_pre_balance_unavailable_count;
    target.collection_authority_missing_count += source.collection_authority_missing_count;
    target.supplemental_provider_failure_count += source.supplemental_provider_failure_count;
    target.provider_quality_lookup_failure_count += source.provider_quality_lookup_failure_count;
    if has_unknown_quality {
        target.history_complete = false;
    }
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
        candidate_open_license_by_token,
        estimated_memory_bytes: _,
    } = plan;
    let token_type = payload_token_type(&seed_contract);
    let CandidateContractFilterResult {
        candidates,
        seed_related_legit_duplicates,
    } = if seed_contract.chain.eq_ignore_ascii_case(&request.chain) {
        filter_seed_related_candidate_contracts(
            &request,
            deps,
            candidates,
            token_type.as_str(),
            request.api_max_concurrency.max(1),
        )
        .await
    } else {
        CandidateContractFilterResult {
            candidates,
            seed_related_legit_duplicates: Vec::new(),
        }
    };
    let grouped = group_candidates_by_contract(&candidates);

    let contracts_to_analyze: Vec<String> = grouped.keys().cloned().collect();
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
    let analysis_timestamp = if request.paper_stats_config.analysis_timestamp > 0 {
        request.paper_stats_config.analysis_timestamp
    } else {
        chrono::Utc::now().timestamp()
    };

    Ok(SeedAnalysisState {
        request,
        seed_contract,
        seed_nfts,
        open_license,
        snapshot,
        candidates: Arc::new(candidates),
        token_type,
        grouped: Arc::new(grouped),
        contracts_to_analyze,
        candidate_open_license_by_token: Arc::new(candidate_open_license_by_token),
        official_addresses: Arc::new(official_addresses),
        output_state,
        expanded_candidates_by_contract,
        analysis_timestamp,
        provider_data_quality: crate::models::ProviderDataQualityPayload::default(),
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
    let contracts_to_analyze = std::mem::take(&mut state.contracts_to_analyze);
    let contract_count = contracts_to_analyze.len();
    progress
        .on_duplicate_contracts_started(contract_count)
        .await;

    let mut completed_contracts = 0;
    let mut outputs = {
        let snapshot_token_index = SnapshotTokenIndex::new(&state.snapshot.nft_rows);
        let context = Arc::new(MatchedContractAnalysisContext {
            request: state.request.clone(),
            deps: deps.clone(),
            token_type: state.token_type.clone(),
            grouped: Arc::clone(&state.grouped),
            candidates: Arc::clone(&state.candidates),
            snapshot_token_index: Arc::new(snapshot_token_index),
            official_addresses: Arc::clone(&state.official_addresses),
            candidate_open_license_by_token: Arc::clone(&state.candidate_open_license_by_token),
            seed_deployed_block_number: state.seed_contract.deployed_block_number,
            analysis_timestamp: state.analysis_timestamp,
        });
        let mut tasks = stream::iter(contracts_to_analyze.iter().cloned().map(
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
                    contract_count,
                )
                .await;
            outputs.insert(output.contract_address.clone(), output);
        }
        outputs
    };

    for contract_address in &contracts_to_analyze {
        let Some(output) = outputs.remove(contract_address) else {
            continue;
        };
        state
            .expanded_candidates_by_contract
            .insert(contract_address.clone(), output.contract_candidates);
        let mut result = output.result;
        let quality = std::mem::take(&mut result.provider_data_quality);
        merge_contract_analysis_result(result, &mut state.output_state);
        merge_provider_data_quality(&mut state.provider_data_quality, quality);
    }
    Ok(())
}

async fn analyze_one_matched_contract(
    context: Arc<MatchedContractAnalysisContext<'_>>,
    contract_address: String,
) -> Result<MatchedContractAnalysisOutput, AppError> {
    let _evidence_pin = ProviderEvidencePin::new(
        context.deps.api.as_ref(),
        &context.request.chain,
        &contract_address,
    );
    analyze_one_matched_contract_pinned(Arc::clone(&context), contract_address).await
}

async fn analyze_one_matched_contract_pinned(
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
        let preliminary_candidate_count = context
            .grouped
            .get(&contract_address)
            .map(Vec::len)
            .unwrap_or_default()
            .max(
                context
                    .snapshot_token_index
                    .contract_token_count(&contract_address),
            );
        let (contract_candidates, current_total_supply) =
            if should_check_current_supply_for_candidate_count(preliminary_candidate_count) {
                let (contract_candidates, current_total_supply) = tokio::join!(
                    fetch_and_expand_contract_candidates(
                        &context.request,
                        &context.deps,
                        &contract_address,
                        &context.grouped,
                        &context.candidates,
                        &context.snapshot_token_index,
                    ),
                    fetch_current_total_supply_for_candidate_filter(
                        &context.request,
                        &context.deps,
                        &contract_address,
                    )
                );
                (contract_candidates?, current_total_supply)
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
                let current_total_supply =
                    if should_check_current_supply_for_candidate_count(contract_candidates.len()) {
                        fetch_current_total_supply_for_candidate_filter(
                            &context.request,
                            &context.deps,
                            &contract_address,
                        )
                        .await
                    } else {
                        None
                    };
                (contract_candidates, current_total_supply)
            };
        let result = if current_supply_implausibly_smaller_than_candidate_count(
            &contract_address,
            contract_candidates.len(),
            current_total_supply,
        ) {
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
    let mut result = result;
    result.provider_data_quality = fetch_provider_quality_or_failure(
        context.deps.api.as_ref(),
        &context.request.chain,
        &contract_address,
    )
    .await;
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
        mut provider_data_quality,
        ..
    } = state;
    let candidates = Arc::try_unwrap(candidates).unwrap_or_else(|shared| (*shared).clone());

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
    let mut value_flow_edges = output_state.mint_payment_edges.clone();
    value_flow_edges.extend(output_state.attacker_cost_edges.clone());
    if request.chain.eq_ignore_ascii_case("solana") {
        provider_data_quality.mint_pre_balance_unavailable_count =
            count_missing_mint_pre_balances(&value_flow_edges);
    }
    let seed_collection_stats = build_seed_collection_stats(&seed_nfts);

    let victim_acquisition_addresses = build_victim_acquisition_addresses_excluding_malicious(
        &output_state.secondary_sale_victim_addresses,
        &output_state.address_attributions,
        &output_state.mint_payment_edges,
        &output_state.nft_propagation_paths,
        &output_state.malicious_addresses,
    );
    let mut paper_stats_config = request.paper_stats_config;
    if paper_stats_config.analysis_timestamp <= 0 {
        paper_stats_config.analysis_timestamp = analysis_timestamp;
    }
    let mut paper_stats = paper_stats::build_paper_stats(paper_stats::PaperStatsInput {
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
    paper_stats.data_quality.asset_listing_analyzed_count =
        provider_data_quality.asset_listing_analyzed_count;
    paper_stats.data_quality.asset_listing_total_count =
        provider_data_quality.asset_listing_total_count;
    paper_stats
        .data_quality
        .asset_listing_truncated_contract_count =
        provider_data_quality.asset_listing_truncated_contract_count;
    paper_stats
        .data_quality
        .asset_listing_unknown_total_contract_count =
        provider_data_quality.asset_listing_unknown_total_contract_count;
    paper_stats.data_quality.asset_listing_coverage_ratio =
        (provider_data_quality.asset_listing_unknown_total_contract_count == 0
            && provider_data_quality.asset_listing_total_count > 0)
            .then_some(
                provider_data_quality.asset_listing_analyzed_count as f64
                    / provider_data_quality.asset_listing_total_count as f64,
            );
    paper_stats.data_quality.history_failed_asset_count =
        provider_data_quality.history_failed_asset_count;
    paper_stats.data_quality.history_requested_asset_count =
        provider_data_quality.history_requested_asset_count;
    paper_stats.data_quality.history_successful_asset_count =
        provider_data_quality.history_successful_asset_count;
    paper_stats.data_quality.history_complete_asset_count =
        provider_data_quality.history_complete_asset_count;
    paper_stats.data_quality.history_unrequested_asset_count =
        provider_data_quality.history_unrequested_asset_count;
    paper_stats.data_quality.history_asset_coverage_ratio =
        (provider_data_quality.history_requested_asset_count > 0).then_some(
            provider_data_quality.history_successful_asset_count as f64
                / provider_data_quality.history_requested_asset_count as f64,
        );
    paper_stats.data_quality.history_truncated_asset_count =
        provider_data_quality.history_truncated_asset_count;
    paper_stats.data_quality.history_fetched_transaction_count =
        provider_data_quality.history_fetched_transaction_count;
    paper_stats.data_quality.history_reported_transaction_count =
        provider_data_quality.history_reported_transaction_count;
    paper_stats.data_quality.history_failed_transaction_count =
        provider_data_quality.history_failed_transaction_count;
    paper_stats
        .data_quality
        .history_signature_discovery_failure_count =
        provider_data_quality.history_signature_discovery_failure_count;
    paper_stats
        .data_quality
        .history_transaction_detail_failure_count =
        provider_data_quality.history_transaction_detail_failure_count;
    paper_stats
        .data_quality
        .history_unattributed_sol_transaction_count =
        provider_data_quality.history_unattributed_sol_transaction_count;
    paper_stats
        .data_quality
        .history_unresolved_compressed_mint_count =
        provider_data_quality.history_unresolved_compressed_mint_count;
    paper_stats.data_quality.mint_pre_balance_unavailable_count =
        provider_data_quality.mint_pre_balance_unavailable_count;
    paper_stats.data_quality.collection_authority_missing_count =
        provider_data_quality.collection_authority_missing_count;
    paper_stats.data_quality.history_complete = provider_data_quality.history_complete;
    paper_stats.data_quality.supplemental_provider_failure_count =
        provider_data_quality.supplemental_provider_failure_count;
    paper_stats
        .data_quality
        .provider_quality_lookup_failure_count =
        provider_data_quality.provider_quality_lookup_failure_count;
    paper_stats.data_quality.history_transaction_coverage_ratio =
        (provider_data_quality.history_failed_asset_count == 0
            && provider_data_quality.history_reported_transaction_count > 0)
            .then_some(
                provider_data_quality.history_fetched_transaction_count as f64
                    / provider_data_quality.history_reported_transaction_count as f64,
            );

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
        nft_propagation_paths: output_state.nft_propagation_paths,
    };
    progress.on_seed_stage("finalize_report").await;
    progress.on_seed_completed().await;
    Ok(payload)
}

fn count_missing_mint_pre_balances(value_flow_edges: &[ValueFlowEdgePayload]) -> i64 {
    value_flow_edges
        .iter()
        .filter(|edge| edge.channel == "mint_payment" && edge.from_before_eth_balance.is_none())
        .map(|edge| (edge.tx_hash.as_str(), edge.from_address.as_str()))
        .collect::<HashSet<_>>()
        .len() as i64
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
