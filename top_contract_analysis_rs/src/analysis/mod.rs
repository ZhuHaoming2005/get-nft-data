use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::{self, FuturesUnordered, StreamExt};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::api::{
    fetch_contract_metadata_with_opensea_fallback, fetch_contract_owners, fetch_contract_sales,
    fetch_contract_transfers, fetch_eth_balance, fetch_etherscan_contract_transfers,
    fetch_is_holder_of_contract, fetch_license_sample, fetch_opensea_account_holds_contract_nft,
    fetch_opensea_contract_collection_slug, fetch_opensea_contract_market_events,
    fetch_opensea_contract_nfts, fetch_same_block_eth_transfers_for_address,
    fetch_same_block_value_transfers_for_address, fetch_seed_contract_nfts,
    fetch_transaction_receipt, fetch_transaction_receipts_for_block, is_open_license_payload,
    ApiEndpoints, AsyncApiClient,
};
use crate::error::AppError;
use crate::models::{
    AddressAttributionPayload, AddressSignalPayload, BatchReportSummary, BatchSeedReportPayload,
    BatchSummaryPayload, ContractLevelSummaryPayload, ContractMetadata, DatabaseSnapshot,
    DuplicateCandidate, DuplicateContractPayload, EthTransferRecord, FraudTradeStatsPayload,
    HonestAddressPayload, HonestAddressStatsPayload, InfringingTokenRecord,
    MaliciousAddressPayload, NftMarketEventRecord, NftPropagationPathPayload, NftSaleRecord,
    OutputFilesPayload, OwnerBalance, ReportSummary, SecondarySaleVictimAddressPayload,
    SeedCollectionStatsPayload, SeedContractPayload, SeedNft, SingleReportPayload,
    TransactionReceiptRecord, TransferRecord, ValueFlowEdgePayload,
    VictimAcquisitionAddressPayload, VictimSignalPayload, ZERO_ADDRESS,
};
use crate::normalize::{normalize_name, normalize_symbol, normalize_url};
use crate::progress::{BatchProgressReporter, SeedProgressReporter};
use crate::reporting::write_outputs_to_directory;
use crate::store::{CachedSignals, ContractSignalCache, DuckDbFeatureStore};

pub mod address_records;
pub mod duplicate;
pub mod lifecycle;
pub mod propagation;
pub mod scoring;
pub mod signals;

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

#[async_trait]
pub trait AnalyzeApi: Send + Sync {
    async fn fetch_contract_metadata(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        opensea_api_key: &str,
        contract_address: &str,
    ) -> Result<ContractMetadata, AppError>;

    async fn fetch_seed_contract_nfts(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        contract_address: &str,
    ) -> Result<Vec<SeedNft>, AppError>;

    async fn fetch_contract_nfts(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        etherscan_api_key: &str,
        opensea_api_key: &str,
        contract_address: &str,
    ) -> Result<Vec<SeedNft>, AppError> {
        let _ = etherscan_api_key;
        let _ = opensea_api_key;
        self.fetch_seed_contract_nfts(chain, alchemy_api_key, alchemy_network, contract_address)
            .await
    }

    async fn fetch_license_sample(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _seed_nfts: &[SeedNft],
    ) -> Result<bool, AppError> {
        Ok(false)
    }

    async fn fetch_contract_transfers(
        &self,
        chain: &str,
        etherscan_api_key: &str,
        alchemy_network: Option<&str>,
        alchemy_api_key: &str,
        contract_address: &str,
        token_type: &str,
    ) -> Result<Vec<TransferRecord>, AppError>;

    async fn fetch_contract_owners(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        contract_address: &str,
    ) -> Result<Vec<OwnerBalance>, AppError>;

    async fn candidate_currently_holds_seed_nft(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _opensea_api_key: &str,
        _seed_contract_address: &str,
        _candidate_contract_address: &str,
        _seed_collection_slug: Option<&str>,
    ) -> Result<Option<bool>, AppError> {
        Ok(None)
    }

    async fn fetch_seed_collection_slug(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        opensea_api_key: &str,
        seed_contract_address: &str,
    ) -> Result<Option<String>, AppError> {
        self.fetch_contract_collection_slug(
            chain,
            alchemy_api_key,
            alchemy_network,
            opensea_api_key,
            seed_contract_address,
        )
        .await
    }

    async fn fetch_contract_collection_slug(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _opensea_api_key: &str,
        _contract_address: &str,
    ) -> Result<Option<String>, AppError> {
        Ok(None)
    }

    async fn fetch_contract_sales(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        contract_address: &str,
        opensea_api_key: &str,
    ) -> Result<Vec<NftSaleRecord>, AppError>;

    async fn fetch_contract_market_events(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
        _opensea_api_key: &str,
    ) -> Result<Vec<NftMarketEventRecord>, AppError> {
        Ok(Vec::new())
    }

    async fn fetch_transaction_receipt(
        &self,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        tx_hash: &str,
    ) -> Result<TransactionReceiptRecord, AppError>;

    async fn fetch_transaction_receipts_for_block(
        &self,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        block_number: i64,
    ) -> Result<BTreeMap<String, TransactionReceiptRecord>, AppError>;

    async fn fetch_eth_balance(
        &self,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        address: &str,
        block_number: i64,
    ) -> Result<f64, AppError>;

    async fn fetch_same_block_eth_transfers_for_address(
        &self,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        block_number: i64,
        address: &str,
    ) -> Result<Vec<EthTransferRecord>, AppError>;

    async fn fetch_transaction_receipt_on_chain(
        &self,
        _chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        tx_hash: &str,
    ) -> Result<TransactionReceiptRecord, AppError> {
        self.fetch_transaction_receipt(alchemy_api_key, alchemy_network, tx_hash)
            .await
    }

    async fn fetch_transaction_receipts_for_block_on_chain(
        &self,
        _chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        block_number: i64,
    ) -> Result<BTreeMap<String, TransactionReceiptRecord>, AppError> {
        self.fetch_transaction_receipts_for_block(alchemy_api_key, alchemy_network, block_number)
            .await
    }

    async fn fetch_eth_balance_on_chain(
        &self,
        _chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        address: &str,
        block_number: i64,
    ) -> Result<f64, AppError> {
        self.fetch_eth_balance(alchemy_api_key, alchemy_network, address, block_number)
            .await
    }

    async fn fetch_same_block_eth_transfers_for_address_on_chain(
        &self,
        _chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        block_number: i64,
        address: &str,
    ) -> Result<Vec<EthTransferRecord>, AppError> {
        self.fetch_same_block_eth_transfers_for_address(
            alchemy_api_key,
            alchemy_network,
            block_number,
            address,
        )
        .await
    }

    async fn fetch_mint_payment_eth_transfers_on_chain(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _block_number: i64,
        _address: &str,
    ) -> Result<Vec<EthTransferRecord>, AppError> {
        Ok(Vec::new())
    }
}

pub trait FeatureStoreReader: Send + Sync {
    fn load_snapshot(
        &self,
        chain: &str,
        seed_nfts: &[SeedNft],
        max_tokens_per_contract: usize,
        max_recall_rows: usize,
    ) -> Result<DatabaseSnapshot, AppError>;
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
    honest_addresses: BTreeSet<String>,
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

pub struct RealApi {
    client: AsyncApiClient,
    eth_usd_rate: crate::currency::EthUsdRateCache,
    eth_usd_rate_warning_emitted: AtomicBool,
}

impl RealApi {
    pub fn new(timeout_seconds: u64, api_max_concurrency: usize) -> Result<Self, AppError> {
        Ok(Self {
            client: AsyncApiClient::new(timeout_seconds, api_max_concurrency)?,
            eth_usd_rate: crate::currency::EthUsdRateCache::default(),
            eth_usd_rate_warning_emitted: AtomicBool::new(false),
        })
    }

    fn endpoints(
        &self,
        chain: &str,
        explicit_network: Option<&str>,
        api_key: &str,
    ) -> ApiEndpoints {
        ApiEndpoints::for_alchemy(&normalize_network(chain, explicit_network), api_key)
    }

    async fn current_eth_usd_rate(&self) -> Result<f64, AppError> {
        self.eth_usd_rate
            .get_or_try_init(|| crate::currency::fetch_current_eth_usd_rate(&self.client))
            .await
    }
}

#[async_trait]
impl AnalyzeApi for RealApi {
    async fn fetch_contract_metadata(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        opensea_api_key: &str,
        contract_address: &str,
    ) -> Result<ContractMetadata, AppError> {
        let endpoints = self.endpoints(chain, alchemy_network, alchemy_api_key);
        fetch_contract_metadata_with_opensea_fallback(
            &self.client,
            &endpoints,
            chain,
            contract_address,
            opensea_api_key,
        )
        .await
    }

    async fn fetch_seed_contract_nfts(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        contract_address: &str,
    ) -> Result<Vec<SeedNft>, AppError> {
        let endpoints = self.endpoints(chain, alchemy_network, alchemy_api_key);
        fetch_seed_contract_nfts(&self.client, &endpoints, chain, contract_address).await
    }

    async fn fetch_contract_nfts(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        etherscan_api_key: &str,
        opensea_api_key: &str,
        contract_address: &str,
    ) -> Result<Vec<SeedNft>, AppError> {
        let endpoints = self.endpoints(chain, alchemy_network, alchemy_api_key);
        if !opensea_api_key.trim().is_empty() {
            match fetch_opensea_contract_nfts(
                &self.client,
                &endpoints.opensea_base,
                chain,
                contract_address,
                opensea_api_key,
            )
            .await
            {
                Ok(rows) if !rows.is_empty() => return Ok(rows),
                Ok(_) => {}
                Err(err) => {
                    eprintln!(
                        "warning: OpenSea NFT expansion failed for {contract_address}: {err}; falling back to Alchemy"
                    );
                }
            }
        }
        let alchemy_result =
            fetch_seed_contract_nfts(&self.client, &endpoints, chain, contract_address).await;
        match alchemy_result {
            Ok(rows) => Ok(rows),
            Err(alchemy_err) if etherscan_api_key.trim().is_empty() => Err(alchemy_err),
            Err(alchemy_err) => {
                eprintln!(
                    "warning: Alchemy NFT expansion failed for {contract_address}: {alchemy_err}; falling back to Etherscan transfers"
                );
                let transfers = fetch_etherscan_contract_transfers(
                    &self.client,
                    &endpoints.etherscan_base,
                    etherscan_api_key,
                    chain,
                    contract_address,
                    "ERC721",
                )
                .await?;
                let mut seen = BTreeSet::new();
                let mut rows = Vec::new();
                for transfer in transfers {
                    if transfer.token_id.is_empty() || !seen.insert(transfer.token_id.clone()) {
                        continue;
                    }
                    rows.push(SeedNft {
                        chain: chain.to_string(),
                        contract_address: contract_address.to_lowercase(),
                        token_id: transfer.token_id,
                        ..SeedNft::default()
                    });
                }
                Ok(rows)
            }
        }
    }

    async fn fetch_license_sample(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        seed_nfts: &[SeedNft],
    ) -> Result<bool, AppError> {
        let endpoints = self.endpoints(chain, alchemy_network, alchemy_api_key);
        let payload = fetch_license_sample(&self.client, &endpoints, seed_nfts).await?;
        Ok(is_open_license_payload(&payload))
    }

    async fn fetch_contract_transfers(
        &self,
        chain: &str,
        etherscan_api_key: &str,
        alchemy_network: Option<&str>,
        alchemy_api_key: &str,
        contract_address: &str,
        token_type: &str,
    ) -> Result<Vec<TransferRecord>, AppError> {
        let endpoints = self.endpoints(chain, alchemy_network, alchemy_api_key);
        fetch_contract_transfers(
            &self.client,
            &endpoints,
            etherscan_api_key,
            chain,
            contract_address,
            token_type,
        )
        .await
    }

    async fn fetch_contract_owners(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        contract_address: &str,
    ) -> Result<Vec<OwnerBalance>, AppError> {
        let endpoints = self.endpoints(chain, alchemy_network, alchemy_api_key);
        fetch_contract_owners(&self.client, &endpoints, contract_address).await
    }

    async fn candidate_currently_holds_seed_nft(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        opensea_api_key: &str,
        seed_contract_address: &str,
        candidate_contract_address: &str,
        seed_collection_slug: Option<&str>,
    ) -> Result<Option<bool>, AppError> {
        let endpoints = self.endpoints(chain, alchemy_network, alchemy_api_key);
        if !opensea_api_key.trim().is_empty() {
            if let Some(seed_collection_slug) = seed_collection_slug {
                match fetch_opensea_account_holds_contract_nft(
                    &self.client,
                    &endpoints.opensea_base,
                    chain,
                    candidate_contract_address,
                    seed_contract_address,
                    opensea_api_key,
                    Some(seed_collection_slug),
                )
                .await
                {
                    Ok(holds_seed_nft) => return Ok(Some(holds_seed_nft)),
                    Err(err) => {
                        eprintln!(
                            "warning: OpenSea account NFT lookup failed for {candidate_contract_address}: {err}; falling back to Alchemy isHolderOfContract"
                        );
                    }
                }
            }
        }

        fetch_is_holder_of_contract(
            &self.client,
            &endpoints,
            candidate_contract_address,
            seed_contract_address,
        )
        .await
        .map(Some)
    }

    async fn fetch_seed_collection_slug(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        opensea_api_key: &str,
        seed_contract_address: &str,
    ) -> Result<Option<String>, AppError> {
        self.fetch_contract_collection_slug(
            chain,
            alchemy_api_key,
            alchemy_network,
            opensea_api_key,
            seed_contract_address,
        )
        .await
    }

    async fn fetch_contract_collection_slug(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        opensea_api_key: &str,
        contract_address: &str,
    ) -> Result<Option<String>, AppError> {
        if opensea_api_key.trim().is_empty() {
            return Ok(None);
        }
        let endpoints = self.endpoints(chain, alchemy_network, alchemy_api_key);
        fetch_opensea_contract_collection_slug(
            &self.client,
            &endpoints.opensea_base,
            chain,
            contract_address,
            opensea_api_key,
        )
        .await
    }

    async fn fetch_contract_sales(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        contract_address: &str,
        opensea_api_key: &str,
    ) -> Result<Vec<NftSaleRecord>, AppError> {
        let endpoints = self.endpoints(chain, alchemy_network, alchemy_api_key);
        let eth_usd_rate = match self.current_eth_usd_rate().await {
            Ok(rate) => Some(rate),
            Err(err) => {
                if !self
                    .eth_usd_rate_warning_emitted
                    .swap(true, Ordering::Relaxed)
                {
                    eprintln!(
                        "warning: failed to fetch current ETH/USD rate for {contract_address}: {err}; ETH/WETH sales will not be USD-normalized"
                    );
                }
                None
            }
        };
        fetch_contract_sales(
            &self.client,
            &endpoints,
            chain,
            contract_address,
            opensea_api_key,
            eth_usd_rate,
        )
        .await
    }

    async fn fetch_contract_market_events(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        contract_address: &str,
        opensea_api_key: &str,
    ) -> Result<Vec<NftMarketEventRecord>, AppError> {
        if opensea_api_key.trim().is_empty() {
            return Ok(Vec::new());
        }
        let endpoints = self.endpoints(chain, alchemy_network, alchemy_api_key);
        let eth_usd_rate = match self.current_eth_usd_rate().await {
            Ok(rate) => Some(rate),
            Err(err) => {
                if !self
                    .eth_usd_rate_warning_emitted
                    .swap(true, Ordering::Relaxed)
                {
                    eprintln!(
                        "warning: failed to fetch current ETH/USD rate for {contract_address}: {err}; market events will not be USD-normalized"
                    );
                }
                None
            }
        };
        let collection_slug = match fetch_opensea_contract_collection_slug(
            &self.client,
            &endpoints.opensea_base,
            chain,
            contract_address,
            opensea_api_key,
        )
        .await
        {
            Ok(collection_slug) => collection_slug,
            Err(err) => {
                eprintln!(
                    "warning: OpenSea market events collection lookup failed for {contract_address}: {err}; continuing without market events"
                );
                None
            }
        };
        match fetch_opensea_contract_market_events(
            &self.client,
            &endpoints.opensea_base,
            chain,
            contract_address,
            collection_slug.as_deref(),
            opensea_api_key,
            eth_usd_rate,
        )
        .await
        {
            Ok(rows) => Ok(rows),
            Err(err) => {
                eprintln!(
                    "warning: OpenSea market events failed for {contract_address}: {err}; continuing without market events"
                );
                Ok(Vec::new())
            }
        }
    }

    async fn fetch_transaction_receipt(
        &self,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        tx_hash: &str,
    ) -> Result<TransactionReceiptRecord, AppError> {
        let endpoints = self.endpoints("ethereum", alchemy_network, alchemy_api_key);
        fetch_transaction_receipt(&self.client, &endpoints, tx_hash).await
    }

    async fn fetch_transaction_receipts_for_block(
        &self,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        block_number: i64,
    ) -> Result<BTreeMap<String, TransactionReceiptRecord>, AppError> {
        let endpoints = self.endpoints("ethereum", alchemy_network, alchemy_api_key);
        fetch_transaction_receipts_for_block(&self.client, &endpoints, block_number).await
    }

    async fn fetch_eth_balance(
        &self,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        address: &str,
        block_number: i64,
    ) -> Result<f64, AppError> {
        let endpoints = self.endpoints("ethereum", alchemy_network, alchemy_api_key);
        fetch_eth_balance(&self.client, &endpoints, address, block_number).await
    }

    async fn fetch_same_block_eth_transfers_for_address(
        &self,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        block_number: i64,
        address: &str,
    ) -> Result<Vec<EthTransferRecord>, AppError> {
        let endpoints = self.endpoints("ethereum", alchemy_network, alchemy_api_key);
        fetch_same_block_eth_transfers_for_address(&self.client, &endpoints, block_number, address)
            .await
    }

    async fn fetch_transaction_receipt_on_chain(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        tx_hash: &str,
    ) -> Result<TransactionReceiptRecord, AppError> {
        let endpoints = self.endpoints(chain, alchemy_network, alchemy_api_key);
        fetch_transaction_receipt(&self.client, &endpoints, tx_hash).await
    }

    async fn fetch_transaction_receipts_for_block_on_chain(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        block_number: i64,
    ) -> Result<BTreeMap<String, TransactionReceiptRecord>, AppError> {
        let endpoints = self.endpoints(chain, alchemy_network, alchemy_api_key);
        fetch_transaction_receipts_for_block(&self.client, &endpoints, block_number).await
    }

    async fn fetch_eth_balance_on_chain(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        address: &str,
        block_number: i64,
    ) -> Result<f64, AppError> {
        let endpoints = self.endpoints(chain, alchemy_network, alchemy_api_key);
        fetch_eth_balance(&self.client, &endpoints, address, block_number).await
    }

    async fn fetch_same_block_eth_transfers_for_address_on_chain(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        block_number: i64,
        address: &str,
    ) -> Result<Vec<EthTransferRecord>, AppError> {
        let endpoints = self.endpoints(chain, alchemy_network, alchemy_api_key);
        fetch_same_block_eth_transfers_for_address(&self.client, &endpoints, block_number, address)
            .await
    }

    async fn fetch_mint_payment_eth_transfers_on_chain(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        block_number: i64,
        address: &str,
    ) -> Result<Vec<EthTransferRecord>, AppError> {
        let eth_usd_rate = match self.current_eth_usd_rate().await {
            Ok(rate) => Some(rate),
            Err(err) => {
                if !self
                    .eth_usd_rate_warning_emitted
                    .swap(true, Ordering::Relaxed)
                {
                    eprintln!(
                        "warning: failed to fetch current ETH/USD rate for mint value-flow at {address}: {err}; ETH/WETH mint payments will not be USD-normalized"
                    );
                }
                None
            }
        };
        let endpoints = self.endpoints(chain, alchemy_network, alchemy_api_key);
        fetch_same_block_value_transfers_for_address(
            &self.client,
            &endpoints,
            block_number,
            address,
            eth_usd_rate,
        )
        .await
    }
}

impl FeatureStoreReader for DuckDbFeatureStore {
    fn load_snapshot(
        &self,
        chain: &str,
        seed_nfts: &[SeedNft],
        max_tokens_per_contract: usize,
        max_recall_rows: usize,
    ) -> Result<DatabaseSnapshot, AppError> {
        DuckDbFeatureStore::load_snapshot(
            self,
            chain,
            seed_nfts,
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

pub async fn analyze_seed_contract_with_progress(
    request: AnalyzeRequest,
    deps: &AnalysisDeps,
    progress: Arc<dyn SeedProgressReporter>,
) -> Result<SingleReportPayload, AppError> {
    analyze_seed_contract_with_limits(request, deps, progress, None, RuntimeLimits::default()).await
}

async fn analyze_seed_contract_with_limits(
    request: AnalyzeRequest,
    deps: &AnalysisDeps,
    progress: Arc<dyn SeedProgressReporter>,
    cpu_limit: Option<Arc<Semaphore>>,
    runtime_limits: RuntimeLimits,
) -> Result<SingleReportPayload, AppError> {
    let context = fetch_seed_context(&request, deps, &runtime_limits, progress.clone()).await?;
    let (context, plan) = build_candidate_plan_for_seed(
        request.clone(),
        deps.feature_store.clone(),
        context,
        cpu_limit,
        progress.clone(),
    )
    .await?;
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
                .map(|row| is_candidate_open_license(&row.metadata_json, &row.metadata_doc))
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
    let mut address_signals = BTreeMap::new();
    let mut victim_signals = BTreeMap::new();
    let mut infringing_tokens = Vec::new();
    let mut legit_duplicates = seed_related_legit_duplicates;
    let mut legit_contract_addresses = legit_duplicates
        .iter()
        .map(|item| item.contract_address.clone())
        .collect::<BTreeSet<_>>();
    let mut malicious_addresses = Vec::new();
    let mut honest_addresses = Vec::new();
    let mut honest_address_stats = BTreeMap::new();
    let mut secondary_sale_victim_addresses = Vec::new();
    let mut address_attributions = Vec::new();
    let mut market_events = Vec::new();
    let mut mint_payment_edges = Vec::new();
    let mut fraud_trade_stats = BTreeMap::<String, FraudTradeStatsPayload>::new();
    let mut nft_propagation_paths = BTreeMap::<String, NftPropagationPathPayload>::new();
    let mut expanded_candidates_by_contract = BTreeMap::new();
    let mut candidate_contract_metadata = BTreeMap::new();
    let mut implausible_candidate_contracts = BTreeSet::new();
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
                let result = analyze_duplicate_contract(
                    request_ref,
                    deps_ref,
                    token_type_ref,
                    contract_address,
                    &contract_candidates,
                    contract_metadata,
                    official_addresses_ref,
                    candidate_open_license_by_token_ref,
                    analysis_timestamp,
                    runtime_limits_ref,
                )
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
                merge_contract_analysis_result(
                    result,
                    &mut legit_contract_addresses,
                    &mut legit_duplicates,
                    &mut address_signals,
                    &mut victim_signals,
                    &mut honest_address_stats,
                    &mut fraud_trade_stats,
                    &mut infringing_tokens,
                    &mut malicious_addresses,
                    &mut honest_addresses,
                    &mut secondary_sale_victim_addresses,
                    &mut address_attributions,
                    &mut market_events,
                    &mut mint_payment_edges,
                    &mut nft_propagation_paths,
                    &mut candidate_contract_metadata,
                    &mut implausible_candidate_contracts,
                );
                next_contract_index_to_merge += 1;
            }
        }
    }
    expanded_candidates_by_contract
        .retain(|contract, _| !implausible_candidate_contracts.contains(contract));
    candidate_contract_metadata
        .retain(|contract, _| !implausible_candidate_contracts.contains(contract));
    let mut duplicate_contracts = build_duplicate_contract_payloads(
        &expanded_candidates_by_contract,
        &candidate_contract_metadata,
    );
    duplicate_contracts.retain(|item| !legit_contract_addresses.contains(&item.contract_address));
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
    let output_candidates: Vec<DuplicateCandidate> = if implausible_candidate_contracts.is_empty() {
        candidates.clone()
    } else {
        candidates
            .iter()
            .filter(|candidate| {
                !implausible_candidate_contracts.contains(&candidate.contract_address)
            })
            .cloned()
            .collect()
    };
    let summary_grouped = group_candidates_by_contract(&output_candidates);
    let lifecycle_outputs =
        lifecycle::build_lifecycle_model_outputs(lifecycle::LifecycleModelInput {
            seed_contract: &seed_contract_payload,
            duplicate_candidates: &lifecycle_candidates,
            duplicate_contracts: &duplicate_contracts,
            address_attributions: &address_attributions,
            nft_propagation_paths: &nft_propagation_paths,
            mint_payment_edges: &mint_payment_edges,
            market_events: &market_events,
        });

    let victim_acquisition_addresses = build_victim_acquisition_addresses(
        &secondary_sale_victim_addresses,
        &address_attributions,
        &lifecycle_outputs.value_flow_edges,
        &nft_propagation_paths,
    );

    let payload = SingleReportPayload {
        seed_contract: seed_contract_payload,
        seed_collection_stats: build_seed_collection_stats(&seed_nfts),
        duplicate_candidates: output_candidates,
        contract_level_summary: build_contract_level_summary(&expanded_candidates_by_contract),
        report_summary: build_report_summary(
            open_license,
            &summary_grouped,
            implausible_candidate_contracts.len() as i64,
            &legit_duplicates,
            &infringing_tokens,
            &malicious_addresses,
            &honest_addresses,
            &secondary_sale_victim_addresses,
            &address_signals,
            &address_attributions,
            &lifecycle_outputs.value_flow_edges,
            &nft_propagation_paths,
        ),
        duplicate_contracts,
        legit_duplicates,
        address_signals,
        victim_signals,
        infringing_tokens,
        malicious_addresses,
        honest_addresses,
        honest_address_stats,
        secondary_sale_victim_addresses,
        victim_acquisition_addresses,
        address_attributions,
        contract_lifecycle_events: lifecycle_outputs.contract_lifecycle_events,
        address_evidence_features: lifecycle_outputs.address_evidence_features,
        value_flow_edges: lifecycle_outputs.value_flow_edges,
        content_similarity_edges: lifecycle_outputs.content_similarity_edges,
        campaign_clusters: lifecycle_outputs.campaign_clusters,
        lifecycle_metrics: lifecycle_outputs.lifecycle_metrics,
        weak_supervision_labels: lifecycle_outputs.weak_supervision_labels,
        early_detection_features: lifecycle_outputs.early_detection_features,
        market_events,
        fraud_trade_stats,
        nft_propagation_paths,
    };
    progress.on_seed_stage("finalize_report").await;
    progress.on_seed_completed().await;
    Ok(payload)
}

fn merge_contract_analysis_result(
    result: ContractAnalysisResult,
    legit_contract_addresses: &mut BTreeSet<String>,
    legit_duplicates: &mut Vec<DuplicateContractPayload>,
    address_signals: &mut BTreeMap<String, AddressSignalPayload>,
    victim_signals: &mut BTreeMap<String, VictimSignalPayload>,
    honest_address_stats: &mut BTreeMap<String, HonestAddressStatsPayload>,
    fraud_trade_stats: &mut BTreeMap<String, FraudTradeStatsPayload>,
    infringing_tokens: &mut Vec<InfringingTokenRecord>,
    malicious_addresses: &mut Vec<MaliciousAddressPayload>,
    honest_addresses: &mut Vec<HonestAddressPayload>,
    secondary_sale_victim_addresses: &mut Vec<SecondarySaleVictimAddressPayload>,
    address_attributions: &mut Vec<AddressAttributionPayload>,
    market_events: &mut Vec<NftMarketEventRecord>,
    mint_payment_edges: &mut Vec<ValueFlowEdgePayload>,
    nft_propagation_paths: &mut BTreeMap<String, NftPropagationPathPayload>,
    candidate_contract_metadata: &mut BTreeMap<String, ContractMetadata>,
    implausible_candidate_contracts: &mut BTreeSet<String>,
) {
    if let Some(metadata) = result.contract_metadata {
        candidate_contract_metadata.insert(result.contract_address.clone(), metadata);
    }
    if result.implausible_candidate_filtered {
        implausible_candidate_contracts.insert(result.contract_address);
        return;
    }
    if let Some(legit_duplicate) = result.legit_duplicate {
        legit_contract_addresses.insert(result.contract_address.clone());
        legit_duplicates.push(legit_duplicate);
        return;
    }
    if let Some(address_signal) = result.address_signal {
        address_signals.insert(result.contract_address.clone(), address_signal);
    }
    if let Some(victim_signal) = result.victim_signal {
        victim_signals.insert(result.contract_address.clone(), victim_signal);
    }
    honest_address_stats.extend(result.honest_address_stats);
    fraud_trade_stats.extend(result.fraud_trade_stats);
    infringing_tokens.extend(result.infringing_tokens);
    malicious_addresses.extend(result.malicious_addresses);
    honest_addresses.extend(result.honest_addresses);
    secondary_sale_victim_addresses.extend(result.secondary_sale_victim_addresses);
    address_attributions.extend(result.address_attributions);
    market_events.extend(result.market_events);
    mint_payment_edges.extend(result.mint_payment_edges);
    if let Some(path) = result.nft_propagation_path {
        nft_propagation_paths.insert(result.contract_address, path);
    }
}

fn payload_token_type(seed_contract: &ContractMetadata) -> String {
    if seed_contract.token_type.trim().is_empty() {
        "ERC721".into()
    } else {
        seed_contract.token_type.clone()
    }
}

fn enrich_duplicate_contract_payload_with_metadata(
    mut payload: DuplicateContractPayload,
    metadata: Option<&ContractMetadata>,
) -> DuplicateContractPayload {
    if let Some(metadata) = metadata {
        payload.contract_deployer = metadata.contract_deployer.clone();
        payload.deployed_block_number = metadata.deployed_block_number;
        payload.token_type = metadata.token_type.clone();
        payload.owner_address = metadata.owner_address.clone();
        payload.admin_address = metadata.admin_address.clone();
        payload.proxy_admin_address = metadata.proxy_admin_address.clone();
        payload.name = metadata.name.clone();
        payload.symbol = metadata.symbol.clone();
    }
    payload
}

fn deployed_before_seed(
    seed_deployed_block_number: i64,
    metadata: Option<&ContractMetadata>,
) -> bool {
    seed_deployed_block_number > 0
        && metadata
            .map(|metadata| {
                metadata.deployed_block_number > 0
                    && metadata.deployed_block_number < seed_deployed_block_number
            })
            .unwrap_or(false)
}

fn implausible_candidate_filtered_result(
    contract_address: &str,
    contract_metadata: Option<ContractMetadata>,
) -> ContractAnalysisResult {
    ContractAnalysisResult {
        contract_address: contract_address.to_string(),
        contract_metadata,
        implausible_candidate_filtered: true,
        legit_duplicate: None,
        address_signal: None,
        victim_signal: None,
        infringing_tokens: Vec::new(),
        malicious_addresses: Vec::new(),
        honest_addresses: Vec::new(),
        honest_address_stats: BTreeMap::new(),
        secondary_sale_victim_addresses: Vec::new(),
        address_attributions: Vec::new(),
        market_events: Vec::new(),
        mint_payment_edges: Vec::new(),
        fraud_trade_stats: BTreeMap::new(),
        nft_propagation_path: None,
    }
}

async fn fetch_candidate_contract_metadata(
    request: &AnalyzeRequest,
    deps: &AnalysisDeps,
    contract_address: &str,
    runtime_limits: &RuntimeLimits,
) -> Result<Option<ContractMetadata>, AppError> {
    let _contract_permit = acquire_optional_limit(&runtime_limits.contract_limit).await?;
    match deps
        .api
        .fetch_contract_metadata(
            &request.chain,
            &request.alchemy_api_key,
            request.alchemy_network.as_deref(),
            &request.opensea_api_key,
            contract_address,
        )
        .await
    {
        Ok(metadata) => Ok(Some(metadata)),
        Err(err) => {
            eprintln!(
                "warning: contract metadata lookup failed for {contract_address}: {err}; continuing without deployment metadata"
            );
            Ok(None)
        }
    }
}

async fn analyze_duplicate_contract(
    request: &AnalyzeRequest,
    deps: &AnalysisDeps,
    token_type: &str,
    contract_address: &str,
    contract_candidates: &[DuplicateCandidate],
    contract_metadata: Option<ContractMetadata>,
    official_addresses: &HashSet<String>,
    candidate_open_license_by_token: &HashMap<(String, String), bool>,
    analysis_timestamp: i64,
    runtime_limits: &RuntimeLimits,
) -> Result<ContractAnalysisResult, AppError> {
    let contract_candidate_refs: Vec<&DuplicateCandidate> = contract_candidates.iter().collect();
    let cached_signals = if let Some(cache) = deps.signal_cache.as_ref() {
        cache.get(&request.chain, contract_address, token_type)?
    } else {
        None
    };
    let (transfers, owners, transfer_signals, victim_signal) = if let Some(cached) = cached_signals
    {
        (
            cached.transfers,
            cached.owners,
            cached.address_signals,
            cached
                .victim_signals
                .unwrap_or_else(|| analyze_victim_signals_from_active_sellers(&[], &[])),
        )
    } else {
        let (transfers, owners) = {
            let _contract_permit = acquire_optional_limit(&runtime_limits.contract_limit).await?;
            tokio::join!(
                deps.api.fetch_contract_transfers(
                    &request.chain,
                    &request.etherscan_api_key,
                    request.alchemy_network.as_deref(),
                    &request.alchemy_api_key,
                    contract_address,
                    token_type,
                ),
                deps.api.fetch_contract_owners(
                    &request.chain,
                    &request.alchemy_api_key,
                    request.alchemy_network.as_deref(),
                    contract_address,
                )
            )
        };
        let transfers = transfers?;
        let owners = owners?;
        if let Some(cache) = deps.signal_cache.as_ref() {
            cache.put(
                &request.chain,
                contract_address,
                token_type,
                &transfers,
                &owners,
            )?;
        }
        let transfer_signals = signals::analyze_transfer_signals(&transfers);
        let victim_signal = analyze_victim_signals_from_active_sellers(&transfers, &owners);
        (transfers, owners, transfer_signals, victim_signal)
    };

    let contract_infringing = address_records::build_infringing_token_records_with_context_refs(
        contract_address,
        &contract_candidate_refs,
        &transfers,
        official_addresses,
        candidate_open_license_by_token,
    );
    if !contract_infringing.is_empty()
        && contract_infringing
            .iter()
            .all(|item| item.official_or_legit_reissue)
    {
        return Ok(ContractAnalysisResult {
            contract_address: contract_address.to_string(),
            contract_metadata: contract_metadata.clone(),
            implausible_candidate_filtered: false,
            legit_duplicate: Some(enrich_duplicate_contract_payload_with_metadata(
                DuplicateContractPayload {
                    contract_address: contract_address.to_string(),
                    candidate_count: contract_candidates.len() as i64,
                    mint_recipients: contract_infringing
                        .iter()
                        .filter_map(|item| {
                            (!item.minter_address.is_empty()).then(|| item.minter_address.clone())
                        })
                        .collect::<BTreeSet<_>>()
                        .into_iter()
                        .collect(),
                    ..DuplicateContractPayload::default()
                },
                contract_metadata.as_ref(),
            )),
            address_signal: None,
            victim_signal: None,
            infringing_tokens: vec![],
            malicious_addresses: vec![],
            honest_addresses: vec![],
            honest_address_stats: BTreeMap::new(),
            secondary_sale_victim_addresses: vec![],
            address_attributions: vec![],
            market_events: vec![],
            mint_payment_edges: vec![],
            fraud_trade_stats: BTreeMap::new(),
            nft_propagation_path: None,
        });
    }

    let sales = {
        let _contract_permit = acquire_optional_limit(&runtime_limits.contract_limit).await?;
        deps.api
            .fetch_contract_sales(
                &request.chain,
                &request.alchemy_api_key,
                request.alchemy_network.as_deref(),
                contract_address,
                &request.opensea_api_key,
            )
            .await?
    };
    let market_events = {
        let _contract_permit = acquire_optional_limit(&runtime_limits.contract_limit).await?;
        deps.api
            .fetch_contract_market_events(
                &request.chain,
                &request.alchemy_api_key,
                request.alchemy_network.as_deref(),
                contract_address,
                &request.opensea_api_key,
            )
            .await?
    };
    let mint_payment_edges = compute_mint_payment_edges_for_contract(
        request,
        deps,
        contract_address,
        &contract_infringing,
        &transfers,
        contract_metadata.as_ref(),
        runtime_limits,
    )
    .await?;
    let sale_metrics_by_tx =
        compute_sale_metrics_for_contract(request, deps, &sales, runtime_limits).await?;

    let contract_activity = address_records::prepare_contract_activity(&transfers, &sales, &owners);
    let contract_malicious = address_records::build_malicious_address_records_from_activity(
        contract_address,
        &contract_activity,
        &contract_infringing,
        &mint_payment_edges,
    );
    let contract_secondary_sale_victims =
        address_records::build_secondary_sale_victim_address_records_from_activity(
            contract_address,
            &contract_activity,
            &sale_metrics_by_tx,
        );
    let contract_honest = address_records::build_honest_address_records_from_activity(
        contract_address,
        &contract_activity,
        &contract_infringing,
        &contract_malicious,
        &mint_payment_edges,
        analysis_timestamp,
    );
    let address_attributions = address_records::build_address_attribution_records(
        contract_address,
        &contract_infringing,
        &sales,
        &mint_payment_edges,
        &contract_malicious,
        &contract_honest,
        &contract_secondary_sale_victims,
    );
    let nft_propagation_path = propagation::build_nft_propagation_path(
        contract_address,
        &transfers,
        &sales,
        &owners,
        &contract_infringing,
        &contract_malicious,
        &contract_honest,
        &contract_secondary_sale_victims,
    );

    Ok(ContractAnalysisResult {
        contract_address: contract_address.to_string(),
        contract_metadata,
        implausible_candidate_filtered: false,
        legit_duplicate: None,
        address_signal: Some(map_address_signals(&transfer_signals)),
        victim_signal: Some(victim_signal),
        honest_address_stats: address_records::build_honest_address_stats(
            contract_address,
            &contract_honest,
        ),
        fraud_trade_stats: address_records::build_fraud_trade_stats(
            contract_address,
            &sales,
            &contract_secondary_sale_victims,
        ),
        infringing_tokens: contract_infringing,
        malicious_addresses: contract_malicious,
        honest_addresses: contract_honest,
        secondary_sale_victim_addresses: contract_secondary_sale_victims,
        address_attributions,
        market_events,
        mint_payment_edges,
        nft_propagation_path: Some(nft_propagation_path),
    })
}

#[derive(Clone, Debug)]
struct MintPaymentLookup {
    tx_hash: String,
    block_number: i64,
    block_time: i64,
    minter_address: String,
    token_ids: Vec<String>,
}

async fn compute_mint_payment_edges_for_contract(
    request: &AnalyzeRequest,
    deps: &AnalysisDeps,
    contract_address: &str,
    infringing_tokens: &[InfringingTokenRecord],
    transfers: &[TransferRecord],
    contract_metadata: Option<&ContractMetadata>,
    runtime_limits: &RuntimeLimits,
) -> Result<Vec<ValueFlowEdgePayload>, AppError> {
    let lookups = build_mint_payment_lookups(contract_address, infringing_tokens, transfers);
    if lookups.is_empty() {
        return Ok(vec![]);
    }
    let payment_limit = runtime_limits.sale_metric_limit.clone().or_else(|| {
        Some(Arc::new(Semaphore::new(
            request.sale_metric_max_concurrency.max(1),
        )))
    });
    let contract_deployer = contract_metadata
        .map(|metadata| metadata.contract_deployer.clone())
        .unwrap_or_default();

    let mut fetched = stream::iter(lookups.into_iter().map(|lookup| {
        let payment_limit = payment_limit.clone();
        let contract_deployer = contract_deployer.clone();
        async move {
            let _permit = acquire_optional_limit(&payment_limit).await?;
            let mut lookup_addresses = BTreeSet::from([
                lookup.minter_address.clone(),
                contract_address.to_string(),
            ]);
            if !contract_deployer.is_empty() {
                lookup_addresses.insert(contract_deployer.clone());
            }
            let mut transfers = Vec::new();
            for address in lookup_addresses {
                match deps
                    .api
                    .fetch_mint_payment_eth_transfers_on_chain(
                        &request.chain,
                        &request.alchemy_api_key,
                        request.alchemy_network.as_deref(),
                        lookup.block_number,
                        &address,
                    )
                    .await
                {
                    Ok(rows) => transfers.extend(rows),
                    Err(err) => {
                        eprintln!(
                            "warning: mint value-flow transfer lookup failed for {address} in {}: {err}; continuing without this value-flow evidence",
                            lookup.tx_hash
                        );
                    }
                }
            }
            Ok::<_, AppError>((lookup, transfers))
        }
    }))
    .buffer_unordered(request.sale_metric_max_concurrency.max(1));

    let mut rows = BTreeMap::<String, ValueFlowEdgePayload>::new();
    while let Some(result) = fetched.next().await {
        let (lookup, eth_transfers) = result?;
        for transfer in eth_transfers {
            if transfer.tx_hash != lookup.tx_hash
                || (transfer.value_eth <= 0.0 && transfer.value_usd.unwrap_or(0.0) <= 0.0)
            {
                continue;
            }
            let Some((channel, from_role, to_role, evidence_type, evidence_flags)) =
                classify_mint_value_flow_transfer(
                    &transfer,
                    &lookup,
                    contract_address,
                    contract_metadata,
                )
            else {
                continue;
            };
            let edge_id = format!(
                "value:{}:{}:{}:{}",
                channel, transfer.tx_hash, transfer.from_address, transfer.to_address
            );
            rows.entry(edge_id.clone()).or_insert(ValueFlowEdgePayload {
                edge_id,
                contract_address: contract_address.to_string(),
                from_address: transfer.from_address,
                to_address: transfer.to_address,
                tx_hash: lookup.tx_hash.clone(),
                block_number: lookup.block_number,
                block_time: lookup.block_time,
                token_id: lookup.token_ids.join(","),
                value_eth: (transfer.value_eth > 0.0).then_some(transfer.value_eth),
                value_usd: transfer.value_usd.filter(|value| *value > 0.0),
                payment_token_symbol: if transfer.payment_token_symbol.is_empty() {
                    "ETH".into()
                } else {
                    transfer.payment_token_symbol
                },
                payment_token_address: if transfer.payment_token_address.is_empty() {
                    ZERO_ADDRESS.into()
                } else {
                    transfer.payment_token_address
                },
                channel,
                marketplace: String::new(),
                evidence_type,
                from_role,
                to_role,
                recipient_known: true,
                evidence_flags,
            });
        }
    }

    Ok(rows.into_values().collect())
}

fn classify_mint_value_flow_transfer(
    transfer: &EthTransferRecord,
    lookup: &MintPaymentLookup,
    contract_address: &str,
    contract_metadata: Option<&ContractMetadata>,
) -> Option<(String, String, String, String, Vec<String>)> {
    let contract_deployer = contract_metadata
        .map(|metadata| metadata.contract_deployer.as_str())
        .unwrap_or("");
    if is_matching_mint_payment_transfer(
        transfer,
        lookup,
        contract_address,
        contract_deployer,
        contract_metadata,
    ) {
        let to_role =
            contract_control_role(&transfer.to_address, contract_address, contract_metadata)
                .unwrap_or("operator_wallet")
                .to_string();
        return Some((
            "mint_payment".into(),
            "paid_minter".into(),
            to_role,
            format!("same_tx_eth_transfer:{}", transfer.category),
            vec![
                "paid_mint".into(),
                "same_tx_eth_transfer".into(),
                transfer.category.clone(),
            ],
        ));
    }
    if transfer
        .to_address
        .eq_ignore_ascii_case(&lookup.minter_address)
        && !transfer
            .from_address
            .eq_ignore_ascii_case(&lookup.minter_address)
        && !transfer.from_address.eq_ignore_ascii_case(ZERO_ADDRESS)
        && transfer.category != "erc20"
    {
        return Some((
            "funding".into(),
            "external_funder".into(),
            "paid_minter".into(),
            format!("same_tx_mint_funding:{}", transfer.category),
            vec![
                "same_tx_mint_funding".into(),
                "pre_mint_capital_source".into(),
                transfer.category.clone(),
            ],
        ));
    }
    if transfer.from_address.eq_ignore_ascii_case(contract_address)
        && !transfer
            .to_address
            .eq_ignore_ascii_case(&lookup.minter_address)
        && !transfer.to_address.eq_ignore_ascii_case(ZERO_ADDRESS)
    {
        let to_role =
            contract_control_role(&transfer.to_address, contract_address, contract_metadata)
                .unwrap_or("external_wallet")
                .to_string();
        return Some((
            "withdrawal".into(),
            "mint_contract".into(),
            to_role,
            format!("same_tx_contract_outflow:{}", transfer.category),
            vec![
                "same_tx_contract_withdrawal".into(),
                "post_mint_value_extraction".into(),
                transfer.category.clone(),
            ],
        ));
    }
    None
}

fn contract_control_role<'a>(
    address: &str,
    contract_address: &str,
    metadata: Option<&'a ContractMetadata>,
) -> Option<&'a str> {
    if address.eq_ignore_ascii_case(contract_address) {
        return Some("mint_contract");
    }
    let metadata = metadata?;
    if !metadata.contract_deployer.is_empty()
        && address.eq_ignore_ascii_case(&metadata.contract_deployer)
    {
        return Some("contract_deployer");
    }
    if !metadata.owner_address.is_empty() && address.eq_ignore_ascii_case(&metadata.owner_address) {
        return Some("contract_owner");
    }
    if !metadata.admin_address.is_empty() && address.eq_ignore_ascii_case(&metadata.admin_address) {
        return Some("contract_admin");
    }
    if !metadata.proxy_admin_address.is_empty()
        && address.eq_ignore_ascii_case(&metadata.proxy_admin_address)
    {
        return Some("proxy_admin");
    }
    None
}

fn is_matching_mint_payment_transfer(
    transfer: &EthTransferRecord,
    lookup: &MintPaymentLookup,
    contract_address: &str,
    contract_deployer: &str,
    contract_metadata: Option<&ContractMetadata>,
) -> bool {
    transfer.tx_hash == lookup.tx_hash
        && transfer_value_positive(transfer)
        && transfer
            .from_address
            .eq_ignore_ascii_case(&lookup.minter_address)
        && (transfer.to_address.eq_ignore_ascii_case(contract_address)
            || (!contract_deployer.is_empty()
                && transfer.to_address.eq_ignore_ascii_case(contract_deployer))
            || contract_metadata
                .map(|metadata| {
                    (!metadata.owner_address.is_empty()
                        && transfer
                            .to_address
                            .eq_ignore_ascii_case(&metadata.owner_address))
                        || (!metadata.admin_address.is_empty()
                            && transfer
                                .to_address
                                .eq_ignore_ascii_case(&metadata.admin_address))
                        || (!metadata.proxy_admin_address.is_empty()
                            && transfer
                                .to_address
                                .eq_ignore_ascii_case(&metadata.proxy_admin_address))
                })
                .unwrap_or(false))
}

fn transfer_value_positive(transfer: &EthTransferRecord) -> bool {
    transfer.value_eth > 0.0 || transfer.value_usd.unwrap_or(0.0) > 0.0
}

fn build_mint_payment_lookups(
    contract_address: &str,
    infringing_tokens: &[InfringingTokenRecord],
    transfers: &[TransferRecord],
) -> Vec<MintPaymentLookup> {
    let mut block_time_by_tx = BTreeMap::<String, i64>::new();
    for transfer in transfers {
        if transfer.contract_address == contract_address
            && !transfer.tx_hash.is_empty()
            && transfer.block_time > 0
        {
            block_time_by_tx
                .entry(transfer.tx_hash.clone())
                .or_insert(transfer.block_time);
        }
    }

    let mut grouped = BTreeMap::<(String, i64, String), BTreeSet<String>>::new();
    for token in infringing_tokens {
        if token.mint_tx_hash.is_empty()
            || token.mint_block <= 0
            || token.minter_address.is_empty()
            || token.minter_address == ZERO_ADDRESS
        {
            continue;
        }
        grouped
            .entry((
                token.mint_tx_hash.clone(),
                token.mint_block,
                token.minter_address.clone(),
            ))
            .or_default()
            .insert(token.token_id.clone());
    }

    grouped
        .into_iter()
        .map(
            |((tx_hash, block_number, minter_address), token_ids)| MintPaymentLookup {
                block_time: block_time_by_tx.get(&tx_hash).copied().unwrap_or_default(),
                tx_hash,
                block_number,
                minter_address,
                token_ids: token_ids.into_iter().collect(),
            },
        )
        .collect()
}

fn is_candidate_open_license(metadata_json: &str, metadata_doc: &str) -> bool {
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
    if !metadata_doc.trim().is_empty() {
        payload.insert(
            "metadata_doc".into(),
            serde_json::Value::String(metadata_doc.to_string()),
        );
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
        mint_to_first_transfer_seconds: signals.mint_to_first_transfer_seconds,
        fast_spread: signals.fast_spread,
    }
}

async fn compute_sale_metrics_for_contract(
    request: &AnalyzeRequest,
    deps: &AnalysisDeps,
    sales: &[NftSaleRecord],
    runtime_limits: &RuntimeLimits,
) -> Result<BTreeMap<String, address_records::SaleMetricRecord>, AppError> {
    let sale_metric_limit = runtime_limits.sale_metric_limit.clone().or_else(|| {
        Some(Arc::new(Semaphore::new(
            request.sale_metric_max_concurrency.max(1),
        )))
    });
    let mut latest_sale_by_buyer = BTreeMap::<String, &NftSaleRecord>::new();
    for sale in sales {
        if sale.buyer_address.is_empty() {
            continue;
        }
        latest_sale_by_buyer
            .entry(sale.buyer_address.clone())
            .and_modify(|existing| {
                if sale_sort_key_for_metrics(sale) >= sale_sort_key_for_metrics(existing) {
                    *existing = sale;
                }
            })
            .or_insert(sale);
    }

    let mut unique_sales_by_purchase = BTreeMap::new();
    for sale in latest_sale_by_buyer.into_values() {
        unique_sales_by_purchase
            .entry(address_records::sale_metric_key(
                &sale.tx_hash,
                &sale.buyer_address,
            ))
            .or_insert(sale);
    }

    let mut prefetches = FuturesUnordered::new();
    for sale in unique_sales_by_purchase.into_values() {
        let sale_metric_limit = sale_metric_limit.clone();
        prefetches.push(async move {
            let _permit = acquire_optional_limit(&sale_metric_limit).await?;
            Ok::<_, AppError>(prefetch_sale_metric_inputs(request, deps, sale).await)
        });
    }

    let mut prefetched_by_purchase = BTreeMap::new();
    let mut queued_blocks = BTreeSet::new();
    let mut block_receipts = FuturesUnordered::new();
    let mut receipts_by_block = BTreeMap::new();
    loop {
        tokio::select! {
            Some(row) = prefetches.next(), if !prefetches.is_empty() => {
                let row = row?;
                if !row.same_block_transfers.is_empty() && queued_blocks.insert(row.block_number) {
                    let sale_metric_limit = sale_metric_limit.clone();
                    let block_number = row.block_number;
                    block_receipts.push(async move {
                        let _permit = match acquire_optional_limit(&sale_metric_limit).await {
                            Ok(permit) => permit,
                            Err(_) => return (block_number, BTreeMap::new()),
                        };
                        let receipts = deps
                            .api
                            .fetch_transaction_receipts_for_block_on_chain(
                                &request.chain,
                                &request.alchemy_api_key,
                                request.alchemy_network.as_deref(),
                                block_number,
                            )
                            .await
                            .unwrap_or_default();
                        (block_number, receipts)
                    });
                }
                prefetched_by_purchase.insert(row.metric_key.clone(), row);
            }
            Some((block_number, receipts)) = block_receipts.next(), if !block_receipts.is_empty() => {
                receipts_by_block.insert(block_number, receipts);
            }
            else => break,
        }
    }

    let mut rows = BTreeMap::new();
    for sale in sales {
        let metric_key = address_records::sale_metric_key(&sale.tx_hash, &sale.buyer_address);
        if rows.contains_key(&metric_key) {
            continue;
        }
        let unavailable;
        let prefetched = if let Some(prefetched) = prefetched_by_purchase.get(&metric_key) {
            prefetched
        } else {
            unavailable = SaleMetricPrefetch::unavailable(sale);
            &unavailable
        };
        rows.insert(
            metric_key,
            compute_sale_metrics_for_sale(sale, prefetched, &receipts_by_block),
        );
    }
    Ok(rows)
}

fn sale_sort_key_for_metrics(sale: &NftSaleRecord) -> (i64, i64, i64, &str) {
    (
        sale.block_number,
        sale.log_index,
        sale.bundle_index,
        sale.tx_hash.as_str(),
    )
}

struct SaleMetricPrefetch {
    metric_key: String,
    block_number: i64,
    purchase_receipt: Option<TransactionReceiptRecord>,
    base_balance_eth: Option<f64>,
    same_block_transfers: Vec<EthTransferRecord>,
}

impl SaleMetricPrefetch {
    fn unavailable(sale: &NftSaleRecord) -> Self {
        Self {
            metric_key: address_records::sale_metric_key(&sale.tx_hash, &sale.buyer_address),
            block_number: sale.block_number,
            purchase_receipt: None,
            base_balance_eth: None,
            same_block_transfers: vec![],
        }
    }
}

async fn prefetch_sale_metric_inputs(
    request: &AnalyzeRequest,
    deps: &AnalysisDeps,
    sale: &NftSaleRecord,
) -> SaleMetricPrefetch {
    if !sale.is_native_eth || sale.price_eth.is_none() {
        return SaleMetricPrefetch::unavailable(sale);
    }

    let (purchase_receipt, base_balance_eth, same_block_transfers) = tokio::join!(
        deps.api.fetch_transaction_receipt_on_chain(
            &request.chain,
            &request.alchemy_api_key,
            request.alchemy_network.as_deref(),
            &sale.tx_hash,
        ),
        deps.api.fetch_eth_balance_on_chain(
            &request.chain,
            &request.alchemy_api_key,
            request.alchemy_network.as_deref(),
            &sale.buyer_address,
            sale.block_number - 1,
        ),
        deps.api
            .fetch_same_block_eth_transfers_for_address_on_chain(
                &request.chain,
                &request.alchemy_api_key,
                request.alchemy_network.as_deref(),
                sale.block_number,
                &sale.buyer_address,
            )
    );
    let purchase_receipt = match purchase_receipt {
        Ok(row) => row,
        Err(_) => return SaleMetricPrefetch::unavailable(sale),
    };
    let base_balance_eth = match base_balance_eth {
        Ok(value) => value,
        Err(_) => return SaleMetricPrefetch::unavailable(sale),
    };
    let same_block_transfers = match same_block_transfers {
        Ok(rows) => rows,
        Err(_) => return SaleMetricPrefetch::unavailable(sale),
    };

    SaleMetricPrefetch {
        metric_key: address_records::sale_metric_key(&sale.tx_hash, &sale.buyer_address),
        block_number: sale.block_number,
        purchase_receipt: Some(purchase_receipt),
        base_balance_eth: Some(base_balance_eth),
        same_block_transfers,
    }
}

fn compute_sale_metrics_for_sale(
    sale: &NftSaleRecord,
    prefetched: &SaleMetricPrefetch,
    receipts_by_block: &BTreeMap<i64, BTreeMap<String, TransactionReceiptRecord>>,
) -> address_records::SaleMetricRecord {
    let Some(purchase_receipt) = prefetched.purchase_receipt.as_ref() else {
        return unavailable_sale_metrics();
    };
    let Some(base_balance_eth) = prefetched.base_balance_eth else {
        return unavailable_sale_metrics();
    };
    let empty_receipts = BTreeMap::new();
    let block_receipts = receipts_by_block
        .get(&prefetched.block_number)
        .unwrap_or(&empty_receipts);

    calculate_sale_eth_metrics(
        sale,
        purchase_receipt,
        base_balance_eth,
        &prefetched.same_block_transfers,
        block_receipts,
    )
}

fn calculate_sale_eth_metrics(
    sale: &NftSaleRecord,
    purchase_receipt: &TransactionReceiptRecord,
    base_balance_eth: f64,
    same_block_transfers: &[EthTransferRecord],
    receipts_by_hash: &BTreeMap<String, TransactionReceiptRecord>,
) -> address_records::SaleMetricRecord {
    if !sale.is_native_eth || sale.price_eth.is_none() {
        return unavailable_sale_metrics();
    }
    let mut same_block_delta = 0.0;
    for transfer in same_block_transfers {
        let Some(receipt) = receipts_by_hash.get(&transfer.tx_hash) else {
            return unavailable_sale_metrics();
        };
        if receipt.transaction_index >= purchase_receipt.transaction_index {
            continue;
        }
        if transfer.to_address == sale.buyer_address {
            same_block_delta += transfer.value_eth;
        }
        if transfer.from_address == sale.buyer_address {
            same_block_delta -= transfer.value_eth;
        }
    }
    let buy_before_eth_balance = base_balance_eth + same_block_delta;
    let mut buy_total_eth_out = sale.price_eth.unwrap_or(0.0);
    let eth_usd_rate = sale.price_eth.and_then(|price_eth| {
        sale.price_usd
            .filter(|price_usd| price_eth > 0.0 && *price_usd > 0.0)
            .map(|price_usd| price_usd / price_eth)
    });
    let buy_before_usd_balance = eth_usd_rate.map(|rate| buy_before_eth_balance * rate);
    let mut buy_total_usd_out = sale.price_usd;
    if purchase_receipt.from_address == sale.buyer_address {
        let gas_eth = (purchase_receipt.gas_used as f64
            * purchase_receipt.effective_gas_price_wei as f64)
            / 1_000_000_000_000_000_000_f64;
        buy_total_eth_out += gas_eth;
        if let (Some(total_usd), Some(rate)) = (buy_total_usd_out, eth_usd_rate) {
            buy_total_usd_out = Some(total_usd + gas_eth * rate);
        }
    }
    let (ratio_denominator, ratio_numerator, ratio_with_gas_numerator) =
        if let (Some(before_usd), Some(price_usd), Some(total_usd)) =
            (buy_before_usd_balance, sale.price_usd, buy_total_usd_out)
        {
            (before_usd, price_usd, total_usd)
        } else {
            (
                buy_before_eth_balance,
                sale.price_eth.unwrap_or(0.0),
                buy_total_eth_out,
            )
        };
    address_records::SaleMetricRecord {
        buy_before_eth_balance: Some(buy_before_eth_balance),
        buy_before_usd_balance,
        buy_asset_ratio: (ratio_denominator > 0.0).then(|| ratio_numerator / ratio_denominator),
        buy_asset_ratio_with_gas: (ratio_denominator > 0.0)
            .then(|| ratio_with_gas_numerator / ratio_denominator),
        ratio_status: if ratio_denominator > 0.0 {
            "ok".into()
        } else {
            "unavailable".into()
        },
    }
}

fn unavailable_sale_metrics() -> address_records::SaleMetricRecord {
    address_records::SaleMetricRecord {
        ratio_status: "unavailable".into(),
        ..address_records::SaleMetricRecord::default()
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

    let worker_count = request.workers.max(1);
    let cpu_limit = Arc::new(Semaphore::new(request.cpu_max_concurrency.max(1)));
    let runtime_limits = RuntimeLimits {
        seed_metadata_limit: Some(Arc::new(Semaphore::new(
            request.seed_metadata_max_concurrency.max(1),
        ))),
        contract_limit: Some(Arc::new(Semaphore::new(
            request.contract_max_concurrency.max(1),
        ))),
        sale_metric_limit: Some(Arc::new(Semaphore::new(
            request.sale_metric_max_concurrency.max(1),
        ))),
    };
    let fresh_entries: Vec<Result<BatchSeedAggregate, AppError>> =
        stream::iter(pending_seeds.into_iter().map(|seed_address| {
            let per_seed_request = AnalyzeRequest {
                chain: request.chain.clone(),
                seed_contract_address: seed_address.clone(),
                alchemy_api_key: request.alchemy_api_key.clone(),
                alchemy_network: request.alchemy_network.clone(),
                etherscan_api_key: request.etherscan_api_key.clone(),
                opensea_api_key: request.opensea_api_key.clone(),
                name_threshold: request.name_threshold,
                metadata_threshold: request.metadata_threshold,
                timeout_seconds: request.timeout_seconds,
                api_max_concurrency: request.api_max_concurrency,
                contract_max_concurrency: request.contract_max_concurrency,
                sale_metric_max_concurrency: request.sale_metric_max_concurrency,
                max_tokens_per_contract: request.max_tokens_per_contract,
                max_recall_rows: request.max_recall_rows,
            };
            let output_dir = request.output_dir.clone();
            let batch_progress = deps.batch_progress.clone();
            let cpu_limit = cpu_limit.clone();
            let runtime_limits = runtime_limits.clone();
            async move {
                batch_progress.on_seed_started(&seed_address);
                let seed_progress = batch_progress.create_seed_reporter(&seed_address);
                let result = analyze_seed_contract_with_limits(
                    per_seed_request,
                    deps,
                    seed_progress,
                    Some(cpu_limit),
                    runtime_limits,
                )
                .await;
                match result {
                    Ok(payload) => {
                        batch_progress.on_seed_finished(&seed_address);
                        let (json_path, md_path) =
                            write_outputs_to_directory(&payload, &output_dir)?;
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
        .buffer_unordered(worker_count)
        .collect()
        .await;

    for entry in fresh_entries {
        let aggregate = entry?;
        seed_aggregates.push(aggregate);
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

pub fn group_candidates_by_contract(
    candidates: &[DuplicateCandidate],
) -> BTreeMap<String, Vec<usize>> {
    let mut grouped = BTreeMap::new();
    for (index, candidate) in candidates.iter().enumerate() {
        grouped
            .entry(candidate.contract_address.clone())
            .or_insert_with(Vec::new)
            .push(index);
    }
    grouped
}

enum CandidateSeedRelationCheck {
    Exclude(&'static str),
    Holder(Result<Option<bool>, AppError>),
}

async fn filter_seed_related_candidate_contracts(
    request: &AnalyzeRequest,
    deps: &AnalysisDeps,
    candidates: Vec<DuplicateCandidate>,
    seed_token_type: &str,
    concurrency: usize,
    runtime_limits: &RuntimeLimits,
) -> CandidateContractFilterResult {
    if candidates.is_empty() {
        return CandidateContractFilterResult {
            candidates,
            seed_related_legit_duplicates: vec![],
        };
    }

    let candidate_contracts: BTreeMap<String, String> = candidates
        .iter()
        .map(|candidate| {
            (
                candidate.contract_address.to_lowercase(),
                candidate.contract_address.clone(),
            )
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    let mut exclusion_reasons_by_contract = BTreeMap::<String, BTreeSet<String>>::new();

    let seed_collection_slug = if candidate_contracts.is_empty() {
        None
    } else {
        match deps
            .api
            .fetch_seed_collection_slug(
                &request.chain,
                &request.alchemy_api_key,
                request.alchemy_network.as_deref(),
                &request.opensea_api_key,
                &request.seed_contract_address,
            )
            .await
        {
            Ok(collection_slug) => collection_slug,
            Err(err) => {
                eprintln!(
                    "warning: OpenSea seed collection lookup failed for {}: {err}; falling back to Alchemy isHolderOfContract",
                    request.seed_contract_address
                );
                None
            }
        }
    };
    let normalized_seed_collection_slug = seed_collection_slug
        .as_deref()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty());

    let mut holder_checks = stream::iter(candidate_contracts.values().cloned().map(|contract_address| {
        let seed_collection_slug = seed_collection_slug.clone();
        let normalized_seed_collection_slug = normalized_seed_collection_slug.clone();
        async move {
            let _permit = match acquire_optional_limit(&runtime_limits.contract_limit).await {
                Ok(permit) => permit,
                Err(err) => {
                    eprintln!(
                        "warning: contract concurrency limit failed for {contract_address}: {err}; continuing without holder-based candidate exclusion"
                    );
                    return (contract_address, CandidateSeedRelationCheck::Holder(Ok(None)));
                }
            };
            if let Some(seed_collection_slug) = normalized_seed_collection_slug.as_deref() {
                match deps
                    .api
                    .fetch_contract_collection_slug(
                        &request.chain,
                        &request.alchemy_api_key,
                        request.alchemy_network.as_deref(),
                        &request.opensea_api_key,
                        &contract_address,
                    )
                    .await
                {
                    Ok(Some(candidate_collection_slug))
                        if candidate_collection_slug
                            .trim()
                            .eq_ignore_ascii_case(seed_collection_slug) =>
                    {
                        return (
                            contract_address,
                            CandidateSeedRelationCheck::Exclude("OpenSea collection 与 seed 合约一致"),
                        );
                    }
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!(
                            "warning: OpenSea candidate collection lookup failed for {contract_address}: {err}; continuing without collection-based candidate exclusion"
                        );
                    }
                }
            }
            let holds_seed_nft = deps
                .api
                .candidate_currently_holds_seed_nft(
                    &request.chain,
                    &request.alchemy_api_key,
                    request.alchemy_network.as_deref(),
                    &request.opensea_api_key,
                    &request.seed_contract_address,
                    &contract_address,
                    seed_collection_slug.as_deref(),
                )
                .await;
            (
                contract_address,
                CandidateSeedRelationCheck::Holder(holds_seed_nft),
            )
        }
    }))
    .buffer_unordered(concurrency.max(1));

    while let Some((contract_address, check)) = holder_checks.next().await {
        let contract_key = contract_address.to_lowercase();
        match check {
            CandidateSeedRelationCheck::Exclude(reason) => {
                exclusion_reasons_by_contract
                    .entry(contract_key)
                    .or_default()
                    .insert(reason.to_string());
            }
            CandidateSeedRelationCheck::Holder(Ok(Some(true))) => {
                exclusion_reasons_by_contract
                    .entry(contract_key)
                    .or_default()
                    .insert("当前持有 seed 合约 NFT".to_string());
            }
            CandidateSeedRelationCheck::Holder(Ok(Some(false))) => {}
            CandidateSeedRelationCheck::Holder(Ok(None)) => {}
            CandidateSeedRelationCheck::Holder(Err(err)) => {
                eprintln!(
                    "warning: current seed NFT holder check failed for {contract_address}: {err}; continuing without holder-based candidate exclusion"
                );
            }
        }
    }

    let remaining_contracts: BTreeSet<String> = candidate_contracts
        .keys()
        .filter(|contract_key| !exclusion_reasons_by_contract.contains_key(*contract_key))
        .cloned()
        .collect();
    if !remaining_contracts.is_empty() {
        match deps
            .api
            .fetch_contract_transfers(
                &request.chain,
                &request.etherscan_api_key,
                request.alchemy_network.as_deref(),
                &request.alchemy_api_key,
                &request.seed_contract_address,
                seed_token_type,
            )
            .await
        {
            Ok(seed_transfers) => {
                for transfer in seed_transfers {
                    let to_address = transfer.to_address.to_lowercase();
                    if remaining_contracts.contains(&to_address) {
                        exclusion_reasons_by_contract
                            .entry(to_address)
                            .or_default()
                            .insert("链上历史 Transfer 显示接收过 seed 合约 NFT".to_string());
                    }
                    let from_address = transfer.from_address.to_lowercase();
                    if remaining_contracts.contains(&from_address) {
                        exclusion_reasons_by_contract
                            .entry(from_address)
                            .or_default()
                            .insert("链上历史 Transfer 显示转出过 seed 合约 NFT".to_string());
                    }
                }
            }
            Err(err) => {
                eprintln!(
                    "warning: seed NFT transfer history lookup failed for {}: {err}; continuing without historical holder-based candidate exclusion",
                    request.seed_contract_address
                );
            }
        }
    }

    let seed_related_legit_duplicates =
        build_seed_related_legit_duplicate_payloads(&candidates, &exclusion_reasons_by_contract);
    let candidates = candidates
        .into_iter()
        .filter(|candidate| {
            !exclusion_reasons_by_contract.contains_key(&candidate.contract_address.to_lowercase())
        })
        .collect();

    CandidateContractFilterResult {
        candidates,
        seed_related_legit_duplicates,
    }
}

fn build_seed_related_legit_duplicate_payloads(
    candidates: &[DuplicateCandidate],
    exclusion_reasons_by_contract: &BTreeMap<String, BTreeSet<String>>,
) -> Vec<DuplicateContractPayload> {
    exclusion_reasons_by_contract
        .iter()
        .filter_map(|(contract_key, reasons)| {
            let contract_candidates: Vec<&DuplicateCandidate> = candidates
                .iter()
                .filter(|candidate| candidate.contract_address.to_lowercase() == *contract_key)
                .collect();
            if contract_candidates.is_empty() {
                return None;
            }
            let mut match_reasons = BTreeSet::new();
            for candidate in &contract_candidates {
                match_reasons.extend(candidate.match_reasons.iter().cloned());
            }
            Some(DuplicateContractPayload {
                contract_address: contract_candidates[0].contract_address.clone(),
                candidate_count: contract_candidates.len() as i64,
                match_reasons: match_reasons.into_iter().collect(),
                exclusion_reasons: reasons.iter().cloned().collect(),
                ..DuplicateContractPayload::default()
            })
        })
        .collect()
}

struct SnapshotTokenIndex<'a> {
    rows_by_contract: HashMap<String, Vec<&'a crate::models::DatabaseNftRecord>>,
}

impl<'a> SnapshotTokenIndex<'a> {
    fn new(snapshot_rows: &'a [crate::models::DatabaseNftRecord]) -> Self {
        let mut rows_by_contract =
            HashMap::<String, Vec<&'a crate::models::DatabaseNftRecord>>::new();
        for row in snapshot_rows {
            rows_by_contract
                .entry(row.contract_address.to_lowercase())
                .or_default()
                .push(row);
        }
        Self { rows_by_contract }
    }

    fn expand_candidates_for_contract(
        &self,
        contract_address: &str,
        candidate_indexes: &[usize],
        candidates: &[DuplicateCandidate],
    ) -> Vec<DuplicateCandidate> {
        let rows = self
            .rows_by_contract
            .get(&contract_address.to_lowercase())
            .into_iter()
            .flat_map(|rows| rows.iter().copied());
        expand_candidate_indexes_to_contract_tokens(
            contract_address,
            candidate_indexes,
            candidates,
            rows,
        )
    }
}

async fn fetch_and_expand_contract_candidates(
    request: &AnalyzeRequest,
    deps: &AnalysisDeps,
    contract_address: &str,
    grouped: &BTreeMap<String, Vec<usize>>,
    candidates: &[DuplicateCandidate],
    snapshot_token_index: &SnapshotTokenIndex<'_>,
    runtime_limits: &RuntimeLimits,
) -> Result<Vec<DuplicateCandidate>, AppError> {
    let candidate_indexes = grouped
        .get(contract_address)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let provider_tokens = {
        let _permit = acquire_optional_limit(&runtime_limits.contract_limit).await?;
        deps.api
            .fetch_contract_nfts(
                &request.chain,
                &request.alchemy_api_key,
                request.alchemy_network.as_deref(),
                &request.etherscan_api_key,
                &request.opensea_api_key,
                contract_address,
            )
            .await
    };
    let expanded = match provider_tokens {
        Ok(tokens) => {
            let expanded = expand_candidate_indexes_to_contract_tokens(
                contract_address,
                candidate_indexes,
                candidates,
                tokens,
            );
            if expanded.is_empty() {
                eprintln!(
                    "warning: provider NFT expansion returned no tokens for {contract_address}; falling back to local snapshot rows"
                );
                snapshot_token_index.expand_candidates_for_contract(
                    contract_address,
                    candidate_indexes,
                    candidates,
                )
            } else {
                expanded
            }
        }
        Err(err) => {
            eprintln!(
                "warning: provider NFT expansion failed for {contract_address}: {err}; falling back to local snapshot rows"
            );
            snapshot_token_index.expand_candidates_for_contract(
                contract_address,
                candidate_indexes,
                candidates,
            )
        }
    };
    Ok(expanded)
}

trait ContractTokenFields {
    fn contract_address(&self) -> &str;
    fn token_id(&self) -> &str;
    fn token_uri(&self) -> &str;
    fn image_uri(&self) -> &str;
    fn name(&self) -> &str;
    fn symbol(&self) -> &str;
}

impl ContractTokenFields for SeedNft {
    fn contract_address(&self) -> &str {
        &self.contract_address
    }

    fn token_id(&self) -> &str {
        &self.token_id
    }

    fn token_uri(&self) -> &str {
        &self.token_uri
    }

    fn image_uri(&self) -> &str {
        &self.image_uri
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn symbol(&self) -> &str {
        &self.symbol
    }
}

impl ContractTokenFields for &crate::models::DatabaseNftRecord {
    fn contract_address(&self) -> &str {
        &self.contract_address
    }

    fn token_id(&self) -> &str {
        &self.token_id
    }

    fn token_uri(&self) -> &str {
        &self.token_uri
    }

    fn image_uri(&self) -> &str {
        &self.image_uri
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn symbol(&self) -> &str {
        &self.symbol
    }
}

fn expand_candidate_indexes_to_contract_tokens<I, T>(
    contract_address: &str,
    candidate_indexes: &[usize],
    candidates: &[DuplicateCandidate],
    contract_tokens: I,
) -> Vec<DuplicateCandidate>
where
    I: IntoIterator<Item = T>,
    T: ContractTokenFields,
{
    let template = candidate_indexes
        .iter()
        .find_map(|index| candidates.get(*index))
        .cloned()
        .unwrap_or_else(|| DuplicateCandidate {
            contract_address: contract_address.to_string(),
            ..DuplicateCandidate::default()
        });
    let contract_key = contract_address.to_lowercase();
    let mut seen_tokens = BTreeSet::new();
    let mut expanded: Vec<DuplicateCandidate> = contract_tokens
        .into_iter()
        .filter(|row| row.contract_address().to_lowercase() == contract_key)
        .filter_map(|row| {
            if !seen_tokens.insert(row.token_id().to_string()) {
                return None;
            }
            Some(DuplicateCandidate {
                contract_address: row.contract_address().to_string(),
                token_id: row.token_id().to_string(),
                match_reasons: template.match_reasons.clone(),
                confidence: template.confidence.clone(),
                token_uri: row.token_uri().to_string(),
                image_uri: row.image_uri().to_string(),
                name: row.name().to_string(),
                symbol: row.symbol().to_string(),
            })
        })
        .collect();
    expanded.sort_by(|left, right| left.token_id.cmp(&right.token_id));
    expanded
}

fn build_contract_payload(
    contract_address: &str,
    contract_candidates: &[DuplicateCandidate],
    metadata: Option<&ContractMetadata>,
) -> DuplicateContractPayload {
    let mut match_reasons: BTreeSet<String> = BTreeSet::new();
    for item in contract_candidates {
        for reason in &item.match_reasons {
            match_reasons.insert(reason.clone());
        }
    }
    enrich_duplicate_contract_payload_with_metadata(
        DuplicateContractPayload {
            contract_address: contract_address.to_string(),
            candidate_count: contract_candidates.len() as i64,
            match_reasons: match_reasons.into_iter().collect(),
            mint_recipients: vec![],
            ..DuplicateContractPayload::default()
        },
        metadata,
    )
}

fn build_duplicate_contract_payloads(
    expanded_candidates_by_contract: &BTreeMap<String, Vec<DuplicateCandidate>>,
    candidate_contract_metadata: &BTreeMap<String, ContractMetadata>,
) -> Vec<DuplicateContractPayload> {
    expanded_candidates_by_contract
        .iter()
        .map(|(contract_address, items)| {
            build_contract_payload(
                contract_address,
                items,
                candidate_contract_metadata.get(contract_address),
            )
        })
        .collect()
}

fn build_seed_collection_stats(seed_nfts: &[SeedNft]) -> SeedCollectionStatsPayload {
    let unique_token_uri_count = seed_nfts
        .iter()
        .filter_map(|item| normalize_url(&item.token_uri))
        .collect::<BTreeSet<_>>()
        .len() as i64;
    let unique_image_uri_count = seed_nfts
        .iter()
        .filter_map(|item| normalize_url(&item.image_uri))
        .collect::<BTreeSet<_>>()
        .len() as i64;
    let unique_name_count = seed_nfts
        .iter()
        .map(|item| normalize_name(&item.name))
        .filter(|value| !value.is_empty())
        .collect::<BTreeSet<_>>()
        .len() as i64;
    let unique_symbol_count = seed_nfts
        .iter()
        .map(|item| normalize_symbol(&item.symbol))
        .filter(|value| !value.is_empty())
        .collect::<BTreeSet<_>>()
        .len() as i64;

    SeedCollectionStatsPayload {
        seed_nft_count: seed_nfts.len() as i64,
        unique_token_uri_count,
        unique_image_uri_count,
        unique_name_count,
        unique_symbol_count,
    }
}

fn build_contract_level_summary(
    expanded_candidates_by_contract: &BTreeMap<String, Vec<DuplicateCandidate>>,
) -> BTreeMap<String, ContractLevelSummaryPayload> {
    expanded_candidates_by_contract
        .iter()
        .map(|(contract_address, items)| {
            (
                contract_address.clone(),
                ContractLevelSummaryPayload {
                    candidate_count: items.len() as i64,
                },
            )
        })
        .collect()
}

#[derive(Clone, Debug, Default)]
struct AcquisitionCostStats {
    paid_mint_victim_cost_eth: f64,
    paid_mint_victim_cost_usd: f64,
    paid_mint_victim_edge_count: i64,
    paid_mint_victim_address_count: i64,
    paid_mint_stuck_cost_eth: f64,
    paid_mint_stuck_cost_usd: f64,
    paid_mint_stuck_edge_count: i64,
    paid_mint_stuck_token_count: i64,
    stablecoin_erc20_value_usd: f64,
    stablecoin_erc20_edge_count: i64,
    value_flow_priced_edge_count: i64,
    value_flow_unpriced_edge_count: i64,
}

fn build_acquisition_cost_stats(
    address_attributions: &[AddressAttributionPayload],
    value_flow_edges: &[ValueFlowEdgePayload],
    propagation_paths: &BTreeMap<String, NftPropagationPathPayload>,
) -> AcquisitionCostStats {
    let paid_mint_victim_addresses = paid_mint_victim_address_set(address_attributions);
    let mut stats = AcquisitionCostStats {
        paid_mint_victim_address_count: paid_mint_victim_addresses.len() as i64,
        ..AcquisitionCostStats::default()
    };

    for edge in value_flow_edges {
        if edge.value_usd.unwrap_or_default() > 0.0 {
            stats.value_flow_priced_edge_count += 1;
        } else if edge.value_eth.unwrap_or_default() > 0.0 {
            stats.value_flow_unpriced_edge_count += 1;
        }
        if is_stablecoin_symbol(&edge.payment_token_symbol)
            && edge.value_usd.unwrap_or_default() > 0.0
        {
            stats.stablecoin_erc20_edge_count += 1;
            stats.stablecoin_erc20_value_usd += edge.value_usd.unwrap_or_default();
        }
        if edge.channel != "mint_payment" {
            continue;
        }
        let payer = normalized_address(&edge.from_address);
        if payer.is_empty() || !paid_mint_victim_addresses.contains(&payer) {
            continue;
        }
        stats.paid_mint_victim_edge_count += 1;
        stats.paid_mint_victim_cost_eth += edge.value_eth.unwrap_or_default();
        stats.paid_mint_victim_cost_usd += edge.value_usd.unwrap_or_default();

        let (stuck_token_count, total_token_count) =
            paid_mint_stuck_token_counts(edge, propagation_paths);
        if stuck_token_count > 0 && total_token_count > 0 {
            let stuck_fraction = stuck_token_count as f64 / total_token_count as f64;
            stats.paid_mint_stuck_edge_count += 1;
            stats.paid_mint_stuck_token_count += stuck_token_count as i64;
            stats.paid_mint_stuck_cost_eth += edge.value_eth.unwrap_or_default() * stuck_fraction;
            stats.paid_mint_stuck_cost_usd += edge.value_usd.unwrap_or_default() * stuck_fraction;
        }
    }

    stats
}

fn paid_mint_victim_address_set(
    address_attributions: &[AddressAttributionPayload],
) -> BTreeSet<String> {
    address_attributions
        .iter()
        .filter(|item| is_victim_attribution_label(&item.attribution_label))
        .filter(|item| {
            item.evidence
                .iter()
                .any(|evidence| evidence.evidence_type == "paid_mint_payment")
        })
        .map(|item| normalized_address(&item.address))
        .filter(|value| !value.is_empty())
        .collect()
}

fn build_victim_acquisition_address_set(
    secondary_sale_victim_addresses: &[SecondarySaleVictimAddressPayload],
    address_attributions: &[AddressAttributionPayload],
) -> BTreeSet<String> {
    let mut addresses: BTreeSet<String> = secondary_sale_victim_addresses
        .iter()
        .map(|item| normalized_address(&item.address))
        .filter(|value| !value.is_empty())
        .collect();
    addresses.extend(paid_mint_victim_address_set(address_attributions));
    addresses
}

fn build_victim_acquisition_addresses(
    secondary_sale_victim_addresses: &[SecondarySaleVictimAddressPayload],
    address_attributions: &[AddressAttributionPayload],
    value_flow_edges: &[ValueFlowEdgePayload],
    propagation_paths: &BTreeMap<String, NftPropagationPathPayload>,
) -> Vec<VictimAcquisitionAddressPayload> {
    let paid_mint_victim_addresses = paid_mint_victim_address_set(address_attributions);
    let mut labels_by_address = BTreeMap::<String, BTreeSet<String>>::new();
    for attribution in address_attributions {
        let address = normalized_address(&attribution.address);
        if address.is_empty() || !is_victim_attribution_label(&attribution.attribution_label) {
            continue;
        }
        labels_by_address
            .entry(address)
            .or_default()
            .insert(attribution.attribution_label.clone());
    }

    let mut rows = BTreeMap::<String, VictimAcquisitionAddressPayload>::new();
    for victim in secondary_sale_victim_addresses {
        let address = normalized_address(&victim.address);
        if address.is_empty() {
            continue;
        }
        let row = rows
            .entry(address.clone())
            .or_insert_with(|| VictimAcquisitionAddressPayload {
                address: victim.address.clone(),
                ..VictimAcquisitionAddressPayload::default()
            });
        push_unique(&mut row.contract_addresses, &victim.contract_address);
        push_unique(&mut row.acquisition_channels, "secondary_sale");
        push_unique_many(&mut row.tx_hashes, &victim.buy_tx_hashes);
        row.secondary_sale_cost_eth += victim.buy_amount_eth;
        row.secondary_sale_cost_usd += victim.buy_amount_usd;
        row.secondary_sale_count += victim.buy_tx_hashes.len() as i64;
        if victim.is_stuck {
            row.secondary_sale_stuck_cost_eth += victim.last_buy_amount_eth.unwrap_or_default();
            row.secondary_sale_stuck_cost_usd += victim.last_buy_amount_usd.unwrap_or_default();
            row.is_stuck = true;
        }
        if row.buy_asset_ratio.is_none() {
            row.buy_asset_ratio = victim.buy_asset_ratio;
        }
        if row.buy_asset_ratio_with_gas.is_none() {
            row.buy_asset_ratio_with_gas = victim.buy_asset_ratio_with_gas;
        }
    }

    for edge in value_flow_edges {
        if edge.channel != "mint_payment" {
            continue;
        }
        let payer = normalized_address(&edge.from_address);
        if payer.is_empty() || !paid_mint_victim_addresses.contains(&payer) {
            continue;
        }
        let row = rows
            .entry(payer)
            .or_insert_with(|| VictimAcquisitionAddressPayload {
                address: edge.from_address.clone(),
                ..VictimAcquisitionAddressPayload::default()
            });
        push_unique(&mut row.contract_addresses, &edge.contract_address);
        push_unique(&mut row.acquisition_channels, "paid_mint");
        push_unique(&mut row.tx_hashes, &edge.tx_hash);
        row.paid_mint_cost_eth += edge.value_eth.unwrap_or_default();
        row.paid_mint_cost_usd += edge.value_usd.unwrap_or_default();
        row.paid_mint_edge_count += 1;
        let (stuck_token_count, total_token_count) =
            paid_mint_stuck_token_counts(edge, propagation_paths);
        if stuck_token_count > 0 && total_token_count > 0 {
            let stuck_fraction = stuck_token_count as f64 / total_token_count as f64;
            row.paid_mint_stuck_token_count += stuck_token_count as i64;
            row.paid_mint_stuck_cost_eth += edge.value_eth.unwrap_or_default() * stuck_fraction;
            row.paid_mint_stuck_cost_usd += edge.value_usd.unwrap_or_default() * stuck_fraction;
            row.is_stuck = true;
        }
    }

    for (address, row) in &mut rows {
        if let Some(labels) = labels_by_address.get(address) {
            row.attribution_labels = labels.iter().cloned().collect();
            row.is_corrupted = labels.iter().any(|label| label == "corrupted_victim");
        }
        row.total_acquisition_cost_eth = row.secondary_sale_cost_eth + row.paid_mint_cost_eth;
        row.total_acquisition_cost_usd = row.secondary_sale_cost_usd + row.paid_mint_cost_usd;
        row.total_stuck_cost_eth = row.secondary_sale_stuck_cost_eth + row.paid_mint_stuck_cost_eth;
        row.total_stuck_cost_usd = row.secondary_sale_stuck_cost_usd + row.paid_mint_stuck_cost_usd;
    }

    rows.into_values().collect()
}

fn push_unique(values: &mut Vec<String>, value: &str) {
    let value = value.trim();
    if value.is_empty() || values.iter().any(|existing| existing == value) {
        return;
    }
    values.push(value.to_string());
}

fn push_unique_many(values: &mut Vec<String>, new_values: &[String]) {
    for value in new_values {
        push_unique(values, value);
    }
}

fn is_victim_attribution_label(label: &str) -> bool {
    matches!(label, "likely_victim" | "corrupted_victim")
}

fn normalized_address(address: &str) -> String {
    address.trim().to_lowercase()
}

fn paid_mint_stuck_token_counts(
    edge: &ValueFlowEdgePayload,
    propagation_paths: &BTreeMap<String, NftPropagationPathPayload>,
) -> (usize, usize) {
    let token_ids = value_flow_token_ids(edge);
    if token_ids.is_empty() {
        return (0, 0);
    }
    let Some(path) = propagation_paths.get(&edge.contract_address).or_else(|| {
        propagation_paths.values().find(|path| {
            path.contract_address
                .eq_ignore_ascii_case(&edge.contract_address)
        })
    }) else {
        return (0, token_ids.len());
    };
    let payer = normalized_address(&edge.from_address);
    let mut stuck_count = 0usize;
    for token_id in &token_ids {
        if path
            .token_paths
            .iter()
            .find(|token_path| token_path.token_id == *token_id)
            .map(|token_path| {
                token_path
                    .current_holder_addresses
                    .iter()
                    .any(|holder| normalized_address(holder) == payer)
            })
            .unwrap_or(false)
        {
            stuck_count += 1;
        }
    }
    (stuck_count, token_ids.len())
}

fn value_flow_token_ids(edge: &ValueFlowEdgePayload) -> Vec<String> {
    let mut token_ids = Vec::new();
    for token_id in edge.token_id.split(',') {
        let token_id = token_id.trim();
        if !token_id.is_empty() {
            token_ids.push(token_id.to_string());
        }
    }
    token_ids.sort();
    token_ids.dedup();
    token_ids
}

fn is_stablecoin_symbol(symbol: &str) -> bool {
    matches!(
        symbol.trim().to_ascii_uppercase().as_str(),
        "USDC" | "USDT" | "DAI" | "USDS" | "PYUSD" | "FRAX" | "LUSD" | "TUSD"
    )
}

fn build_report_summary(
    open_license: bool,
    grouped: &BTreeMap<String, Vec<usize>>,
    implausible_candidate_contract_count: i64,
    legit_duplicates: &[DuplicateContractPayload],
    infringing_tokens: &[InfringingTokenRecord],
    malicious_addresses: &[MaliciousAddressPayload],
    honest_addresses: &[HonestAddressPayload],
    secondary_sale_victim_addresses: &[SecondarySaleVictimAddressPayload],
    address_signals: &BTreeMap<String, AddressSignalPayload>,
    address_attributions: &[AddressAttributionPayload],
    value_flow_edges: &[ValueFlowEdgePayload],
    propagation_paths: &BTreeMap<String, NftPropagationPathPayload>,
) -> ReportSummary {
    let infringing_nft_count = infringing_tokens
        .iter()
        .map(|item| (item.contract_address.clone(), item.token_id.clone()))
        .collect::<BTreeSet<_>>()
        .len() as i64;
    let malicious_address_count = malicious_addresses
        .iter()
        .map(|item| item.address.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect::<BTreeSet<_>>()
        .len() as i64;
    let honest_address_count = honest_addresses
        .iter()
        .map(|item| item.address.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect::<BTreeSet<_>>()
        .len() as i64;
    let repeat_infringing_address_count = infringing_tokens
        .iter()
        .filter(|item| !item.minter_address.is_empty() && !item.contract_address.is_empty())
        .fold(
            BTreeMap::<String, BTreeSet<String>>::new(),
            |mut acc, item| {
                acc.entry(item.minter_address.clone())
                    .or_default()
                    .insert(item.contract_address.clone());
                acc
            },
        )
        .values()
        .filter(|contracts| contracts.len() > 1)
        .count() as i64;
    let candidate_open_license_tokens: Vec<&InfringingTokenRecord> = infringing_tokens
        .iter()
        .filter(|item| item.candidate_open_license)
        .collect();
    let candidate_open_license_contract_count = candidate_open_license_tokens
        .iter()
        .map(|item| item.contract_address.clone())
        .collect::<BTreeSet<_>>()
        .len() as i64;
    let secondary_sale_victim_cost_eth = secondary_sale_victim_addresses
        .iter()
        .map(|item| item.buy_amount_eth)
        .sum::<f64>();
    let secondary_sale_victim_cost_usd = secondary_sale_victim_addresses
        .iter()
        .map(|item| item.buy_amount_usd)
        .sum::<f64>();
    let secondary_sale_stuck_cost_eth = secondary_sale_victim_addresses
        .iter()
        .filter(|item| item.is_stuck)
        .map(|item| item.last_buy_amount_eth.unwrap_or(0.0))
        .sum::<f64>();
    let secondary_sale_stuck_cost_usd = secondary_sale_victim_addresses
        .iter()
        .filter(|item| item.is_stuck)
        .map(|item| item.last_buy_amount_usd.unwrap_or(0.0))
        .sum::<f64>();
    let buy_ratio_values: Vec<f64> = secondary_sale_victim_addresses
        .iter()
        .filter_map(|item| item.buy_asset_ratio)
        .collect();
    let ratio_known_count = buy_ratio_values.len() as i64;
    let ratio_over_60_count = buy_ratio_values
        .iter()
        .filter(|value| **value > 0.6)
        .count() as i64;
    let ratio_over_80_count = buy_ratio_values
        .iter()
        .filter(|value| **value > 0.8)
        .count() as i64;
    let stuck_honest_address_count = secondary_sale_victim_addresses
        .iter()
        .filter(|item| item.is_stuck)
        .count() as i64;
    let corrupted_honest_address_count = honest_addresses
        .iter()
        .filter(|item| item.is_corrupted_address)
        .count() as i64;
    let corrupted_holding_values: Vec<f64> = honest_addresses
        .iter()
        .filter(|item| item.is_corrupted_address)
        .filter_map(|item| item.hold_duration_median_seconds)
        .collect();
    let mint_to_honest_samples: Vec<f64> = honest_addresses
        .iter()
        .flat_map(|item| {
            item.mint_to_honest_seconds_samples
                .iter()
                .filter_map(|sample| positive_seconds(*sample))
        })
        .collect();
    let mint_to_first_transfer_values: Vec<f64> = address_signals
        .values()
        .filter_map(|signal| positive_seconds(signal.mint_to_first_transfer_seconds))
        .collect();
    let unique_receiver_values: Vec<f64> = address_signals
        .values()
        .map(|signal| signal.unique_receiver_count as f64)
        .collect();
    let acquisition_stats =
        build_acquisition_cost_stats(address_attributions, value_flow_edges, propagation_paths);
    let secondary_sale_stuck_cost_ratio = if secondary_sale_victim_cost_usd > 0.0 {
        Some(secondary_sale_stuck_cost_usd / secondary_sale_victim_cost_usd)
    } else if secondary_sale_victim_cost_eth > 0.0 {
        Some(secondary_sale_stuck_cost_eth / secondary_sale_victim_cost_eth)
    } else {
        None
    };
    let victim_acquisition_total_eth =
        secondary_sale_victim_cost_eth + acquisition_stats.paid_mint_victim_cost_eth;
    let victim_acquisition_total_usd =
        secondary_sale_victim_cost_usd + acquisition_stats.paid_mint_victim_cost_usd;
    let victim_acquisition_stuck_cost_eth =
        secondary_sale_stuck_cost_eth + acquisition_stats.paid_mint_stuck_cost_eth;
    let victim_acquisition_stuck_cost_usd =
        secondary_sale_stuck_cost_usd + acquisition_stats.paid_mint_stuck_cost_usd;
    let victim_acquisition_stuck_cost_ratio = if victim_acquisition_total_usd > 0.0 {
        Some(victim_acquisition_stuck_cost_usd / victim_acquisition_total_usd)
    } else if victim_acquisition_total_eth > 0.0 {
        Some(victim_acquisition_stuck_cost_eth / victim_acquisition_total_eth)
    } else {
        None
    };

    ReportSummary {
        open_license_detected: open_license,
        candidate_contract_count: grouped.len() as i64,
        implausible_candidate_contract_count,
        infringing_nft_count,
        malicious_address_count,
        honest_address_count,
        repeat_infringing_address_count,
        legit_duplicate_contract_count: legit_duplicates.len() as i64,
        candidate_open_license_token_count: candidate_open_license_tokens.len() as i64,
        candidate_open_license_contract_count,
        secondary_sale_victim_cost_eth,
        secondary_sale_victim_cost_usd,
        secondary_sale_victim_address_count: secondary_sale_victim_addresses.len() as i64,
        secondary_sale_stuck_cost_eth,
        secondary_sale_stuck_cost_usd,
        secondary_sale_stuck_cost_ratio,
        paid_mint_victim_cost_eth: acquisition_stats.paid_mint_victim_cost_eth,
        paid_mint_victim_cost_usd: acquisition_stats.paid_mint_victim_cost_usd,
        paid_mint_victim_edge_count: acquisition_stats.paid_mint_victim_edge_count,
        paid_mint_victim_address_count: acquisition_stats.paid_mint_victim_address_count,
        paid_mint_stuck_cost_eth: acquisition_stats.paid_mint_stuck_cost_eth,
        paid_mint_stuck_cost_usd: acquisition_stats.paid_mint_stuck_cost_usd,
        paid_mint_stuck_edge_count: acquisition_stats.paid_mint_stuck_edge_count,
        paid_mint_stuck_token_count: acquisition_stats.paid_mint_stuck_token_count,
        victim_acquisition_total_eth,
        victim_acquisition_total_usd,
        victim_acquisition_stuck_cost_eth,
        victim_acquisition_stuck_cost_usd,
        victim_acquisition_stuck_cost_ratio,
        victim_acquisition_address_count: build_victim_acquisition_address_set(
            secondary_sale_victim_addresses,
            address_attributions,
        )
        .len() as i64,
        stablecoin_erc20_value_usd: acquisition_stats.stablecoin_erc20_value_usd,
        stablecoin_erc20_edge_count: acquisition_stats.stablecoin_erc20_edge_count,
        value_flow_priced_edge_count: acquisition_stats.value_flow_priced_edge_count,
        value_flow_unpriced_edge_count: acquisition_stats.value_flow_unpriced_edge_count,
        buy_asset_ratio_known_address_count: ratio_known_count,
        ratio_over_60_address_count: ratio_over_60_count,
        ratio_over_60_address_ratio: if ratio_known_count > 0 {
            Some(ratio_over_60_count as f64 / ratio_known_count as f64)
        } else {
            None
        },
        ratio_over_80_address_count: ratio_over_80_count,
        ratio_over_80_address_ratio: if ratio_known_count > 0 {
            Some(ratio_over_80_count as f64 / ratio_known_count as f64)
        } else {
            None
        },
        stuck_honest_address_count,
        stuck_honest_address_ratio: if !secondary_sale_victim_addresses.is_empty() {
            Some(stuck_honest_address_count as f64 / secondary_sale_victim_addresses.len() as f64)
        } else {
            None
        },
        corrupted_honest_address_count,
        avg_corrupted_address_holding_seconds: mean_f64(&corrupted_holding_values),
        median_corrupted_address_holding_seconds: median_f64(&corrupted_holding_values),
        avg_seconds_to_honest_holder: mean_f64(&mint_to_honest_samples),
        median_seconds_to_honest_holder: median_f64(&mint_to_honest_samples),
        avg_mint_to_first_transfer_seconds: mean_f64(&mint_to_first_transfer_values),
        median_mint_to_first_transfer_seconds: median_f64(&mint_to_first_transfer_values),
        avg_unique_receiver_count: mean_f64(&unique_receiver_values),
    }
}

fn mean_f64(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        None
    } else {
        Some(values.iter().sum::<f64>() / values.len() as f64)
    }
}

fn median_f64(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|left, right| left.total_cmp(right));
    let mid = sorted.len() / 2;
    if sorted.len() % 2 == 1 {
        Some(sorted[mid])
    } else {
        Some((sorted[mid - 1] + sorted[mid]) / 2.0)
    }
}

fn build_batch_report_summary(seed_reports: &[BatchSeedAggregate]) -> BatchReportSummary {
    let distinct_chains: BTreeSet<String> = seed_reports
        .iter()
        .map(|item| item.report.seed_contract.chain.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect();
    let distinct_chains: Vec<String> = distinct_chains.into_iter().collect();
    let malicious_addresses: BTreeSet<String> = seed_reports
        .iter()
        .flat_map(|item| item.malicious_addresses.iter().cloned())
        .collect();
    let honest_addresses: BTreeSet<String> = seed_reports
        .iter()
        .flat_map(|item| item.honest_addresses.iter().cloned())
        .collect();
    let mut minter_infringing_contracts: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for seed_report in seed_reports {
        for (minter, contracts) in &seed_report.minter_infringing_contracts {
            minter_infringing_contracts
                .entry(minter.clone())
                .or_default()
                .extend(contracts.iter().cloned());
        }
    }
    let secondary_sale_victim_cost_eth_total: f64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.secondary_sale_victim_cost_eth)
        .sum();
    let secondary_sale_victim_cost_usd_total: f64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.secondary_sale_victim_cost_usd)
        .sum();
    let secondary_sale_victim_address_count_total: i64 = seed_reports
        .iter()
        .map(|item| {
            item.report
                .report_summary
                .secondary_sale_victim_address_count
        })
        .sum();
    let secondary_sale_stuck_cost_eth_total: f64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.secondary_sale_stuck_cost_eth)
        .sum();
    let secondary_sale_stuck_cost_usd_total: f64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.secondary_sale_stuck_cost_usd)
        .sum();
    let paid_mint_victim_cost_eth_total: f64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.paid_mint_victim_cost_eth)
        .sum();
    let paid_mint_victim_cost_usd_total: f64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.paid_mint_victim_cost_usd)
        .sum();
    let paid_mint_victim_edge_count_total: i64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.paid_mint_victim_edge_count)
        .sum();
    let paid_mint_victim_address_count_total: i64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.paid_mint_victim_address_count)
        .sum();
    let paid_mint_stuck_cost_eth_total: f64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.paid_mint_stuck_cost_eth)
        .sum();
    let paid_mint_stuck_cost_usd_total: f64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.paid_mint_stuck_cost_usd)
        .sum();
    let paid_mint_stuck_edge_count_total: i64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.paid_mint_stuck_edge_count)
        .sum();
    let paid_mint_stuck_token_count_total: i64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.paid_mint_stuck_token_count)
        .sum();
    let victim_acquisition_total_eth_total: f64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.victim_acquisition_total_eth)
        .sum();
    let victim_acquisition_total_usd_total: f64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.victim_acquisition_total_usd)
        .sum();
    let victim_acquisition_stuck_cost_eth_total: f64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.victim_acquisition_stuck_cost_eth)
        .sum();
    let victim_acquisition_stuck_cost_usd_total: f64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.victim_acquisition_stuck_cost_usd)
        .sum();
    let victim_acquisition_address_count_total: i64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.victim_acquisition_address_count)
        .sum();
    let stablecoin_erc20_value_usd_total: f64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.stablecoin_erc20_value_usd)
        .sum();
    let stablecoin_erc20_edge_count_total: i64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.stablecoin_erc20_edge_count)
        .sum();
    let value_flow_priced_edge_count_total: i64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.value_flow_priced_edge_count)
        .sum();
    let value_flow_unpriced_edge_count_total: i64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.value_flow_unpriced_edge_count)
        .sum();
    let buy_asset_ratio_known_address_count_total: i64 = seed_reports
        .iter()
        .map(|item| {
            item.report
                .report_summary
                .buy_asset_ratio_known_address_count
        })
        .sum();
    let ratio_over_60_address_count_total: i64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.ratio_over_60_address_count)
        .sum();
    let ratio_over_80_address_count_total: i64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.ratio_over_80_address_count)
        .sum();
    let stuck_honest_address_count_total: i64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.stuck_honest_address_count)
        .sum();
    let mean_corrupted_holding_values: Vec<f64> = seed_reports
        .iter()
        .filter_map(|item| {
            item.report
                .report_summary
                .avg_corrupted_address_holding_seconds
        })
        .collect();
    let median_corrupted_holding_values: Vec<f64> = seed_reports
        .iter()
        .filter_map(|item| {
            item.report
                .report_summary
                .median_corrupted_address_holding_seconds
        })
        .collect();
    let mean_honest_holder_values: Vec<f64> = seed_reports
        .iter()
        .filter_map(|item| item.report.report_summary.avg_seconds_to_honest_holder)
        .collect();
    let median_honest_holder_values: Vec<f64> = seed_reports
        .iter()
        .filter_map(|item| item.report.report_summary.median_seconds_to_honest_holder)
        .collect();
    let mean_first_transfer_values: Vec<f64> = seed_reports
        .iter()
        .filter_map(|item| {
            item.report
                .report_summary
                .avg_mint_to_first_transfer_seconds
        })
        .filter(|value| *value > 0.0)
        .collect();
    let median_first_transfer_values: Vec<f64> = seed_reports
        .iter()
        .filter_map(|item| {
            item.report
                .report_summary
                .median_mint_to_first_transfer_seconds
        })
        .filter(|value| *value > 0.0)
        .collect();
    let mean_unique_receiver_values: Vec<f64> = seed_reports
        .iter()
        .filter_map(|item| item.report.report_summary.avg_unique_receiver_count)
        .collect();
    BatchReportSummary {
        seed_report_count: seed_reports.len() as i64,
        chain: if distinct_chains.len() == 1 {
            distinct_chains[0].clone()
        } else {
            String::new()
        },
        chains: distinct_chains,
        open_license_detected_count: seed_reports
            .iter()
            .filter(|item| item.report.report_summary.open_license_detected)
            .count() as i64,
        candidate_contract_count_total: seed_reports
            .iter()
            .map(|item| item.report.report_summary.candidate_contract_count)
            .sum(),
        implausible_candidate_contract_count_total: seed_reports
            .iter()
            .map(|item| {
                item.report
                    .report_summary
                    .implausible_candidate_contract_count
            })
            .sum(),
        infringing_nft_count_total: seed_reports
            .iter()
            .map(|item| item.report.report_summary.infringing_nft_count)
            .sum(),
        malicious_address_count_total: malicious_addresses.len() as i64,
        honest_address_count_total: honest_addresses.len() as i64,
        repeat_infringing_address_count_total: seed_reports
            .iter()
            .map(|item| item.report.report_summary.repeat_infringing_address_count)
            .sum(),
        repeat_infringing_address_count_global: minter_infringing_contracts
            .values()
            .filter(|contracts| contracts.len() > 1)
            .count() as i64,
        legit_duplicate_contract_count_total: seed_reports
            .iter()
            .map(|item| item.report.report_summary.legit_duplicate_contract_count)
            .sum(),
        secondary_sale_victim_cost_eth_total,
        secondary_sale_victim_cost_usd_total,
        secondary_sale_victim_address_count_total,
        secondary_sale_stuck_cost_eth_total,
        secondary_sale_stuck_cost_usd_total,
        secondary_sale_stuck_cost_ratio_overall: if secondary_sale_victim_cost_usd_total > 0.0 {
            Some(secondary_sale_stuck_cost_usd_total / secondary_sale_victim_cost_usd_total)
        } else if secondary_sale_victim_cost_eth_total > 0.0 {
            Some(secondary_sale_stuck_cost_eth_total / secondary_sale_victim_cost_eth_total)
        } else {
            None
        },
        paid_mint_victim_cost_eth_total,
        paid_mint_victim_cost_usd_total,
        paid_mint_victim_edge_count_total,
        paid_mint_victim_address_count_total,
        paid_mint_stuck_cost_eth_total,
        paid_mint_stuck_cost_usd_total,
        paid_mint_stuck_edge_count_total,
        paid_mint_stuck_token_count_total,
        victim_acquisition_total_eth_total,
        victim_acquisition_total_usd_total,
        victim_acquisition_stuck_cost_eth_total,
        victim_acquisition_stuck_cost_usd_total,
        victim_acquisition_stuck_cost_ratio_overall: if victim_acquisition_total_usd_total > 0.0 {
            Some(victim_acquisition_stuck_cost_usd_total / victim_acquisition_total_usd_total)
        } else if victim_acquisition_total_eth_total > 0.0 {
            Some(victim_acquisition_stuck_cost_eth_total / victim_acquisition_total_eth_total)
        } else {
            None
        },
        victim_acquisition_address_count_total,
        stablecoin_erc20_value_usd_total,
        stablecoin_erc20_edge_count_total,
        value_flow_priced_edge_count_total,
        value_flow_unpriced_edge_count_total,
        buy_asset_ratio_known_address_count_total,
        ratio_over_60_address_count_total,
        ratio_over_60_address_ratio_overall: if buy_asset_ratio_known_address_count_total > 0 {
            Some(
                ratio_over_60_address_count_total as f64
                    / buy_asset_ratio_known_address_count_total as f64,
            )
        } else {
            None
        },
        ratio_over_80_address_count_total,
        ratio_over_80_address_ratio_overall: if buy_asset_ratio_known_address_count_total > 0 {
            Some(
                ratio_over_80_address_count_total as f64
                    / buy_asset_ratio_known_address_count_total as f64,
            )
        } else {
            None
        },
        stuck_honest_address_count_total,
        stuck_honest_address_ratio_overall: if buy_asset_ratio_known_address_count_total > 0 {
            Some(
                stuck_honest_address_count_total as f64
                    / buy_asset_ratio_known_address_count_total as f64,
            )
        } else {
            None
        },
        corrupted_honest_address_count_total: seed_reports
            .iter()
            .map(|item| item.report.report_summary.corrupted_honest_address_count)
            .sum(),
        avg_corrupted_address_holding_seconds_mean: mean(&mean_corrupted_holding_values),
        median_corrupted_address_holding_seconds_median: median_f64(
            &median_corrupted_holding_values,
        ),
        avg_seconds_to_honest_holder_mean: mean(&mean_honest_holder_values),
        median_seconds_to_honest_holder_median: median_f64(&median_honest_holder_values),
        avg_mint_to_first_transfer_seconds_mean: mean(&mean_first_transfer_values),
        median_mint_to_first_transfer_seconds_median: median_f64(&median_first_transfer_values),
        avg_unique_receiver_count_mean: mean(&mean_unique_receiver_values),
        generated_at: chrono::Utc::now().to_rfc3339(),
    }
}

fn build_batch_seed_aggregate(payload: SingleReportPayload) -> BatchSeedAggregate {
    let malicious_addresses: BTreeSet<String> = payload
        .malicious_addresses
        .iter()
        .map(|item| item.address.trim().to_lowercase())
        .filter(|value| !value.is_empty())
        .collect();
    let honest_addresses: BTreeSet<String> = payload
        .honest_addresses
        .iter()
        .map(|item| item.address.trim().to_lowercase())
        .filter(|value| !value.is_empty())
        .collect();
    let minter_infringing_contracts = payload_minter_contracts(&payload.infringing_tokens);
    let report_summary = payload.report_summary.clone();

    BatchSeedAggregate {
        report: BatchSeedReportPayload {
            seed_contract: payload.seed_contract,
            report_summary,
            output_files: None,
        },
        malicious_addresses,
        honest_addresses,
        minter_infringing_contracts,
    }
}

fn payload_minter_contracts(
    infringing_tokens: &[InfringingTokenRecord],
) -> BTreeMap<String, BTreeSet<String>> {
    let mut minter_contracts: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for token in infringing_tokens {
        let minter = token.minter_address.trim().to_lowercase();
        let contract = token.contract_address.trim().to_lowercase();
        if minter.is_empty() || contract.is_empty() {
            continue;
        }
        minter_contracts.entry(minter).or_default().insert(contract);
    }
    minter_contracts
}

#[cfg(test)]
fn payload_median_seconds_to_honest_holder(payload: &SingleReportPayload) -> Option<f64> {
    let values: Vec<f64> = payload
        .honest_addresses
        .iter()
        .flat_map(|item| item.mint_to_honest_seconds_samples.iter().copied())
        .filter_map(positive_seconds)
        .collect();
    median_f64(&values)
}

#[cfg(test)]
fn payload_median_mint_to_first_transfer_seconds(payload: &SingleReportPayload) -> Option<f64> {
    let values: Vec<f64> = payload
        .address_signals
        .values()
        .filter_map(|signal| positive_seconds(signal.mint_to_first_transfer_seconds))
        .collect();
    median_f64(&values)
}

fn positive_seconds(value: i64) -> Option<f64> {
    (value > 0).then_some(value as f64)
}

fn mean(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        None
    } else {
        Some(values.iter().sum::<f64>() / values.len() as f64)
    }
}

fn normalize_network(chain: &str, explicit_network: Option<&str>) -> String {
    if let Some(network) = explicit_network.filter(|value| !value.trim().is_empty()) {
        return network.to_string();
    }
    match chain.trim().to_lowercase().as_str() {
        "ethereum" => "eth-mainnet".into(),
        "base" => "base-mainnet".into(),
        "polygon" => "polygon-mainnet".into(),
        other => format!("{other}-mainnet"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{AddressEvidencePayload, NftTokenPropagationPayload};

    #[test]
    fn report_summary_ignores_zero_mint_to_first_transfer_samples() {
        let address_signals = BTreeMap::from([
            (
                "0xmintonly".into(),
                AddressSignalPayload {
                    mint_to_first_transfer_seconds: 0,
                    ..AddressSignalPayload::default()
                },
            ),
            (
                "0xfast".into(),
                AddressSignalPayload {
                    mint_to_first_transfer_seconds: 8,
                    ..AddressSignalPayload::default()
                },
            ),
            (
                "0xslow".into(),
                AddressSignalPayload {
                    mint_to_first_transfer_seconds: 20,
                    ..AddressSignalPayload::default()
                },
            ),
        ]);

        let summary = build_report_summary(
            false,
            &BTreeMap::new(),
            0,
            &[],
            &[],
            &[],
            &[],
            &[],
            &address_signals,
            &[],
            &[],
            &BTreeMap::new(),
        );

        assert_eq!(summary.avg_mint_to_first_transfer_seconds, Some(14.0));
        assert_eq!(summary.median_mint_to_first_transfer_seconds, Some(14.0));
    }

    #[test]
    fn report_summary_ignores_zero_mint_to_honest_holder_samples() {
        let honest_addresses = vec![
            HonestAddressPayload {
                address: "0xmintvictim".into(),
                mint_to_honest_seconds_samples: vec![0],
                ..HonestAddressPayload::default()
            },
            HonestAddressPayload {
                address: "0xpropagated1".into(),
                mint_to_honest_seconds_samples: vec![12],
                ..HonestAddressPayload::default()
            },
            HonestAddressPayload {
                address: "0xpropagated2".into(),
                mint_to_honest_seconds_samples: vec![20],
                ..HonestAddressPayload::default()
            },
        ];

        let summary = build_report_summary(
            false,
            &BTreeMap::new(),
            0,
            &[],
            &[],
            &[],
            &honest_addresses,
            &[],
            &BTreeMap::new(),
            &[],
            &[],
            &BTreeMap::new(),
        );

        assert_eq!(summary.avg_seconds_to_honest_holder, Some(16.0));
        assert_eq!(summary.median_seconds_to_honest_holder, Some(16.0));
    }

    #[test]
    fn report_summary_tracks_corrupted_address_holding_duration_stats() {
        let honest_addresses = vec![
            HonestAddressPayload {
                address: "0xcorrupted-fast".into(),
                is_corrupted_address: true,
                hold_duration_median_seconds: Some(12.0),
                ..HonestAddressPayload::default()
            },
            HonestAddressPayload {
                address: "0xcorrupted-slow".into(),
                is_corrupted_address: true,
                hold_duration_median_seconds: Some(30.0),
                ..HonestAddressPayload::default()
            },
            HonestAddressPayload {
                address: "0xvictim-no-duration".into(),
                is_corrupted_address: true,
                hold_duration_median_seconds: None,
                ..HonestAddressPayload::default()
            },
            HonestAddressPayload {
                address: "0xplain-victim".into(),
                is_corrupted_address: false,
                hold_duration_median_seconds: Some(100.0),
                ..HonestAddressPayload::default()
            },
        ];

        let summary = build_report_summary(
            false,
            &BTreeMap::new(),
            0,
            &[],
            &[],
            &[],
            &honest_addresses,
            &[],
            &BTreeMap::new(),
            &[],
            &[],
            &BTreeMap::new(),
        );

        assert_eq!(summary.corrupted_honest_address_count, 3);
        assert_eq!(summary.avg_corrupted_address_holding_seconds, Some(21.0));
        assert_eq!(summary.median_corrupted_address_holding_seconds, Some(21.0));
    }

    #[test]
    fn cached_payload_median_ignores_zero_mint_to_first_transfer_samples() {
        let payload = SingleReportPayload {
            address_signals: BTreeMap::from([
                (
                    "0xmintonly".into(),
                    AddressSignalPayload {
                        mint_to_first_transfer_seconds: 0,
                        ..AddressSignalPayload::default()
                    },
                ),
                (
                    "0xtransfer".into(),
                    AddressSignalPayload {
                        mint_to_first_transfer_seconds: 12,
                        ..AddressSignalPayload::default()
                    },
                ),
            ]),
            ..SingleReportPayload::default()
        };

        assert_eq!(
            payload_median_mint_to_first_transfer_seconds(&payload),
            Some(12.0)
        );
    }

    #[test]
    fn cached_payload_median_ignores_zero_mint_to_honest_holder_samples() {
        let payload = SingleReportPayload {
            honest_addresses: vec![
                HonestAddressPayload {
                    mint_to_honest_seconds_samples: vec![0],
                    ..HonestAddressPayload::default()
                },
                HonestAddressPayload {
                    mint_to_honest_seconds_samples: vec![12, 20],
                    ..HonestAddressPayload::default()
                },
            ],
            ..SingleReportPayload::default()
        };

        assert_eq!(
            payload_median_seconds_to_honest_holder(&payload),
            Some(16.0)
        );
    }

    #[test]
    fn report_summary_separates_secondary_sale_and_paid_mint_victim_costs() {
        let secondary_sale_victim_addresses = vec![SecondarySaleVictimAddressPayload {
            contract_address: "0xdup".into(),
            address: "0xsalevictim".into(),
            buy_amount_eth: 0.5,
            buy_amount_usd: 1_000.0,
            last_buy_amount_eth: Some(0.25),
            last_buy_amount_usd: Some(500.0),
            is_stuck: true,
            ..SecondarySaleVictimAddressPayload::default()
        }];
        let address_attributions = vec![AddressAttributionPayload {
            contract_address: "0xdup".into(),
            address: "0xpaidvictim".into(),
            attribution_label: "likely_victim".into(),
            victim_score: 0.45,
            evidence: vec![AddressEvidencePayload {
                evidence_type: "paid_mint_payment".into(),
                contract_address: "0xdup".into(),
                token_id: "1,2".into(),
                tx_hash: "0xmint".into(),
                weight: 0.45,
                detail: "paid mint victim evidence".into(),
            }],
            ..AddressAttributionPayload::default()
        }];
        let value_flow_edges = vec![ValueFlowEdgePayload {
            edge_id: "value:mint_payment:0xmint".into(),
            contract_address: "0xdup".into(),
            from_address: "0xpaidvictim".into(),
            to_address: "0xdup".into(),
            tx_hash: "0xmint".into(),
            token_id: "1,2".into(),
            value_eth: Some(2.0),
            value_usd: Some(4_000.0),
            payment_token_symbol: "ETH".into(),
            channel: "mint_payment".into(),
            to_role: "mint_contract".into(),
            ..ValueFlowEdgePayload::default()
        }];
        let propagation_paths = BTreeMap::from([(
            "0xdup".into(),
            NftPropagationPathPayload {
                contract_address: "0xdup".into(),
                token_paths: vec![
                    NftTokenPropagationPayload {
                        token_id: "1".into(),
                        current_holder_addresses: vec!["0xpaidvictim".into()],
                        ..NftTokenPropagationPayload::default()
                    },
                    NftTokenPropagationPayload {
                        token_id: "2".into(),
                        current_holder_addresses: vec!["0xother".into()],
                        ..NftTokenPropagationPayload::default()
                    },
                ],
                ..NftPropagationPathPayload::default()
            },
        )]);

        let summary = build_report_summary(
            false,
            &BTreeMap::new(),
            0,
            &[],
            &[],
            &[],
            &[],
            &secondary_sale_victim_addresses,
            &BTreeMap::new(),
            &address_attributions,
            &value_flow_edges,
            &propagation_paths,
        );

        assert_eq!(summary.secondary_sale_victim_cost_eth, 0.5);
        assert_eq!(summary.secondary_sale_stuck_cost_eth, 0.25);
        assert_eq!(summary.paid_mint_victim_cost_eth, 2.0);
        assert_eq!(summary.paid_mint_victim_cost_usd, 4_000.0);
        assert_eq!(summary.paid_mint_stuck_cost_eth, 1.0);
        assert_eq!(summary.paid_mint_stuck_cost_usd, 2_000.0);
        assert_eq!(summary.victim_acquisition_total_eth, 2.5);
        assert_eq!(summary.victim_acquisition_stuck_cost_eth, 1.25);
        assert_eq!(summary.victim_acquisition_address_count, 2);
    }

    #[test]
    fn mint_value_flow_does_not_classify_erc20_same_tx_transfers_to_minter_as_funding() {
        let lookup = MintPaymentLookup {
            tx_hash: "0xmint".into(),
            block_number: 100,
            block_time: 1_700_000_000,
            minter_address: "0xpaidvictim".into(),
            token_ids: vec!["1".into()],
        };
        let erc20_transfer = EthTransferRecord {
            tx_hash: "0xmint".into(),
            block_number: 100,
            from_address: "0xrouter".into(),
            to_address: "0xpaidvictim".into(),
            value_eth: 134.0,
            value_usd: Some(300_000.0),
            payment_token_symbol: "WETH".into(),
            category: "erc20".into(),
            ..EthTransferRecord::default()
        };
        let native_transfer = EthTransferRecord {
            category: "external".into(),
            value_eth: 0.5,
            value_usd: Some(1_000.0),
            ..erc20_transfer.clone()
        };

        assert!(
            classify_mint_value_flow_transfer(&erc20_transfer, &lookup, "0xdup", None).is_none()
        );
        let classified =
            classify_mint_value_flow_transfer(&native_transfer, &lookup, "0xdup", None).unwrap();
        assert_eq!(classified.0, "funding");
    }
}
