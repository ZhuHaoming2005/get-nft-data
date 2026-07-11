use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::Hash;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;

use crate::api::{
    fetch_account_holds_contract_alchemy_first, fetch_contract_collection_slug_alchemy_first,
    fetch_contract_metadata_with_opensea_fallback_clients,
    fetch_contract_nfts_with_fallback_clients, fetch_contract_owners,
    fetch_contract_sales_with_clients, fetch_contract_total_supply,
    fetch_contract_transfers_with_etherscan_fallback, fetch_eth_balance,
    fetch_helius_assets_history_with_budget, fetch_helius_block_details,
    fetch_helius_collection_snapshot, fetch_helius_transaction_details, fetch_license_sample,
    fetch_same_block_value_transfers_for_address, fetch_same_block_value_transfers_to_address,
    fetch_seed_contract_nfts, fetch_transaction_receipt, fetch_transaction_receipts_for_block,
    is_open_license_payload, ApiEndpoints, AsyncApiClient, HeliusCollectionHistory,
    HeliusCollectionSnapshot, OpenSeaAccountFallback,
};
use crate::currency::FALLBACK_ETH_USD_RATE;
use crate::error::AppError;
use crate::models::{
    ContractMetadata, EthTransferRecord, NftSaleRecord, OwnerBalance, ProviderDataQualityPayload,
    SeedNft, TransactionReceiptRecord, TransferRecord,
};

type HeliusCollectionCacheCell = Arc<tokio::sync::OnceCell<Arc<HeliusCollectionSnapshot>>>;
type HeliusBlockCacheCell =
    Arc<tokio::sync::OnceCell<Arc<Vec<crate::api::HeliusTransactionDetails>>>>;
type HeliusHistoryCacheCell = Arc<tokio::sync::OnceCell<Arc<HeliusCollectionHistory>>>;

struct BoundedLruCache<K, V> {
    entries: HashMap<K, (V, u64)>,
    clock: u64,
    capacity: usize,
}

impl<K, V> BoundedLruCache<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::new(),
            clock: 0,
            capacity: capacity.max(1),
        }
    }

    fn get(&mut self, key: &K) -> Option<V> {
        self.clock = self.clock.wrapping_add(1);
        let (value, last_used) = self.entries.get_mut(key)?;
        *last_used = self.clock;
        Some(value.clone())
    }

    fn get_or_insert_with(&mut self, key: K, create: impl FnOnce() -> V) -> V {
        if let Some(value) = self.get(&key) {
            return value;
        }
        if self.entries.len() >= self.capacity {
            if let Some(evicted) = self
                .entries
                .iter()
                .min_by_key(|(_, (_, last_used))| *last_used)
                .map(|(key, _)| key.clone())
            {
                self.entries.remove(&evicted);
            }
        }
        self.clock = self.clock.wrapping_add(1);
        let value = create();
        self.entries.insert(key, (value.clone(), self.clock));
        value
    }
}
type NativeRateCacheCell = Arc<tokio::sync::OnceCell<f64>>;

#[derive(Default)]
struct ProviderQualityRegistry {
    entries: Mutex<HashMap<String, ProviderDataQualityPayload>>,
}

impl ProviderQualityRegistry {
    fn record_snapshot(
        &self,
        contract_address: &str,
        snapshot: &HeliusCollectionSnapshot,
    ) -> Result<(), AppError> {
        let mut entries = self.entries.lock().map_err(|error| {
            AppError::InvalidData(format!("provider quality registry lock poisoned: {error}"))
        })?;
        let entry = entries
            .entry(contract_address.trim().to_string())
            .or_default();
        entry.asset_listing_analyzed_count = snapshot.assets.len() as i64;
        entry.asset_listing_total_count = snapshot.total as i64;
        entry.asset_listing_truncated_contract_count = i64::from(snapshot.truncated);
        Ok(())
    }

    fn record_history(
        &self,
        contract_address: &str,
        history: &HeliusCollectionHistory,
    ) -> Result<(), AppError> {
        let mut entries = self.entries.lock().map_err(|error| {
            AppError::InvalidData(format!("provider quality registry lock poisoned: {error}"))
        })?;
        let entry = entries
            .entry(contract_address.trim().to_string())
            .or_default();
        entry.history_failed_asset_count = history.failed_asset_count as i64;
        entry.history_requested_asset_count = history.requested_asset_count as i64;
        entry.history_successful_asset_count = history.successful_asset_count as i64;
        entry.history_truncated_asset_count = history.truncated_asset_history_count as i64;
        entry.history_fetched_transaction_count = history.fetched_transaction_count as i64;
        entry.history_reported_transaction_count = history.reported_transaction_count as i64;
        entry.history_failed_transaction_count = history.failed_transaction_count as i64;
        entry.history_unattributed_sol_transaction_count =
            history.unattributed_native_transaction_count as i64;
        entry.history_unresolved_compressed_mint_count =
            history.unresolved_compressed_mint_count as i64;
        Ok(())
    }

    fn get(&self, contract_address: &str) -> Result<ProviderDataQualityPayload, AppError> {
        Ok(self
            .entries
            .lock()
            .map_err(|error| {
                AppError::InvalidData(format!("provider quality registry lock poisoned: {error}"))
            })?
            .get(contract_address.trim())
            .cloned()
            .unwrap_or_default())
    }
}

#[derive(Clone, Copy)]
pub struct CandidateSeedHolderRequest<'a> {
    pub chain: &'a str,
    pub alchemy_api_key: &'a str,
    pub alchemy_network: Option<&'a str>,
    pub opensea_api_key: &'a str,
    pub seed_contract_address: &'a str,
    pub candidate_contract_address: &'a str,
    pub seed_collection_slug: Option<&'a str>,
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

    async fn fetch_contract_total_supply(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
    ) -> Result<Option<u64>, AppError> {
        Ok(None)
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
        _request: CandidateSeedHolderRequest<'_>,
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

    async fn fetch_pre_transaction_native_balance_on_chain(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        _tx_hash: &str,
        address: &str,
        block_number: i64,
    ) -> Result<f64, AppError> {
        self.fetch_eth_balance_on_chain(
            chain,
            alchemy_api_key,
            alchemy_network,
            address,
            block_number,
        )
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

    async fn fetch_mint_payment_eth_transfers_to_address_on_chain(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        block_number: i64,
        address: &str,
    ) -> Result<Vec<EthTransferRecord>, AppError> {
        self.fetch_mint_payment_eth_transfers_on_chain(
            chain,
            alchemy_api_key,
            alchemy_network,
            block_number,
            address,
        )
        .await
    }

    async fn fetch_transaction_value_transfers_to_address_on_chain(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        _tx_hash: &str,
        block_number: i64,
        address: &str,
    ) -> Result<Vec<EthTransferRecord>, AppError> {
        self.fetch_mint_payment_eth_transfers_to_address_on_chain(
            chain,
            alchemy_api_key,
            alchemy_network,
            block_number,
            address,
        )
        .await
    }

    async fn warm_eth_usd_rate(&self) -> Result<(), AppError> {
        Ok(())
    }

    async fn fetch_provider_data_quality(
        &self,
        _chain: &str,
        _contract_address: &str,
    ) -> Result<ProviderDataQualityPayload, AppError> {
        Ok(ProviderDataQualityPayload::default())
    }
}

pub struct RealApi {
    alchemy_client: AsyncApiClient,
    other_client: AsyncApiClient,
    helius_client: Option<AsyncApiClient>,
    helius_rpc_url: Option<String>,
    max_history_transactions_per_asset: usize,
    max_history_transactions_per_collection: usize,
    max_helius_assets_per_collection: usize,
    helius_collection_cache: Mutex<BoundedLruCache<String, HeliusCollectionCacheCell>>,
    helius_history_cache: Mutex<BoundedLruCache<String, HeliusHistoryCacheCell>>,
    helius_block_cache: Mutex<BoundedLruCache<i64, HeliusBlockCacheCell>>,
    opensea_supplement_failures: Mutex<HashSet<String>>,
    provider_quality: ProviderQualityRegistry,
    native_usd_rates: Mutex<HashMap<String, NativeRateCacheCell>>,
    eth_usd_rate: crate::currency::EthUsdRateCache,
    eth_usd_rate_warning_emitted: AtomicBool,
}

#[derive(Clone, Copy, Debug)]
pub struct HeliusApiConfig<'a> {
    pub max_concurrency: usize,
    pub rate_limit_refill_ms: u64,
    pub api_key: &'a str,
    pub max_history_transactions_per_asset: usize,
    pub max_history_transactions_per_collection: usize,
    pub max_assets_per_collection: usize,
}

impl RealApi {
    pub fn new(
        timeout_seconds: u64,
        alchemy_api_max_concurrency: usize,
        other_api_max_concurrency: usize,
        other_api_rate_limit_refill_ms: u64,
    ) -> Result<Self, AppError> {
        Self::new_with_optional_helius(
            timeout_seconds,
            alchemy_api_max_concurrency,
            other_api_max_concurrency,
            other_api_rate_limit_refill_ms,
            None,
        )
    }

    pub fn new_with_helius(
        timeout_seconds: u64,
        alchemy_api_max_concurrency: usize,
        other_api_max_concurrency: usize,
        other_api_rate_limit_refill_ms: u64,
        helius: HeliusApiConfig<'_>,
    ) -> Result<Self, AppError> {
        let key = helius.api_key.trim();
        let endpoint = (!key.is_empty()).then(|| {
            (
                helius.max_concurrency,
                helius.rate_limit_refill_ms,
                format!("https://mainnet.helius-rpc.com/?api-key={key}"),
                helius.max_history_transactions_per_asset,
                helius.max_history_transactions_per_collection,
                helius.max_assets_per_collection,
            )
        });
        Self::new_with_optional_helius(
            timeout_seconds,
            alchemy_api_max_concurrency,
            other_api_max_concurrency,
            other_api_rate_limit_refill_ms,
            endpoint,
        )
    }

    pub fn new_with_helius_endpoint(
        timeout_seconds: u64,
        alchemy_api_max_concurrency: usize,
        other_api_max_concurrency: usize,
        other_api_rate_limit_refill_ms: u64,
        helius_api_max_concurrency: usize,
        helius_rpc_url: String,
    ) -> Result<Self, AppError> {
        Self::new_with_optional_helius(
            timeout_seconds,
            alchemy_api_max_concurrency,
            other_api_max_concurrency,
            other_api_rate_limit_refill_ms,
            Some((
                helius_api_max_concurrency,
                100,
                helius_rpc_url,
                100,
                10_000,
                10_000,
            )),
        )
    }

    fn new_with_optional_helius(
        timeout_seconds: u64,
        alchemy_api_max_concurrency: usize,
        other_api_max_concurrency: usize,
        other_api_rate_limit_refill_ms: u64,
        helius: Option<(usize, u64, String, usize, usize, usize)>,
    ) -> Result<Self, AppError> {
        let (
            helius_client,
            helius_rpc_url,
            max_history_transactions_per_asset,
            max_history_transactions_per_collection,
            max_helius_assets_per_collection,
        ) = match helius {
            Some((
                concurrency,
                refill_ms,
                url,
                max_history,
                max_collection_history,
                max_assets,
            )) => (
                Some(AsyncApiClient::new_rate_limited_with_in_flight_limit(
                    timeout_seconds,
                    concurrency,
                    concurrency,
                    Duration::from_millis(refill_ms.max(1)),
                )?),
                Some(url),
                max_history,
                max_collection_history,
                max_assets,
            ),
            None => (None, None, 0, 0, 0),
        };
        Ok(Self {
            alchemy_client: AsyncApiClient::new(timeout_seconds, alchemy_api_max_concurrency)?,
            other_client: AsyncApiClient::new_rate_limited_with_retry_policy(
                timeout_seconds,
                other_api_max_concurrency,
                Duration::from_millis(other_api_rate_limit_refill_ms.max(1)),
                crate::api::DEFAULT_API_RETRIES,
                Duration::from_millis(crate::api::DEFAULT_API_RETRY_DELAY_MS),
            )?,
            helius_client,
            helius_rpc_url,
            max_history_transactions_per_asset,
            max_history_transactions_per_collection,
            max_helius_assets_per_collection,
            helius_collection_cache: Mutex::new(BoundedLruCache::new(16)),
            helius_history_cache: Mutex::new(BoundedLruCache::new(16)),
            helius_block_cache: Mutex::new(BoundedLruCache::new(128)),
            opensea_supplement_failures: Mutex::new(HashSet::new()),
            provider_quality: ProviderQualityRegistry::default(),
            native_usd_rates: Mutex::new(HashMap::new()),
            eth_usd_rate: crate::currency::EthUsdRateCache::default(),
            eth_usd_rate_warning_emitted: AtomicBool::new(false),
        })
    }

    fn helius(&self) -> Result<(&AsyncApiClient, &str), AppError> {
        self.helius_client
            .as_ref()
            .zip(self.helius_rpc_url.as_deref())
            .ok_or_else(|| {
                AppError::InvalidData("Helius API key is required for Solana analysis".to_string())
            })
    }

    async fn helius_collection_snapshot(
        &self,
        collection_address: &str,
    ) -> Result<Arc<HeliusCollectionSnapshot>, AppError> {
        let key = collection_address.trim().to_string();
        let cell = {
            let mut cache = self.helius_collection_cache.lock().map_err(|error| {
                AppError::InvalidData(format!("Helius collection cache lock poisoned: {error}"))
            })?;
            cache.get_or_insert_with(key.clone(), || Arc::new(tokio::sync::OnceCell::new()))
        };
        let (client, rpc_url) = self.helius()?;
        let snapshot = cell
            .get_or_try_init(|| async {
                fetch_helius_collection_snapshot(
                    client,
                    rpc_url,
                    &key,
                    1_000,
                    self.max_helius_assets_per_collection,
                )
                .await
                .map(Arc::new)
            })
            .await?;
        self.provider_quality.record_snapshot(&key, snapshot)?;
        Ok(snapshot.clone())
    }

    async fn current_native_usd_rate(&self, chain: &str) -> Result<f64, AppError> {
        let key = chain.trim().to_ascii_lowercase();
        let cell = self
            .native_usd_rates
            .lock()
            .map_err(|error| {
                AppError::InvalidData(format!("native rate cache lock poisoned: {error}"))
            })?
            .entry(key.clone())
            .or_insert_with(|| Arc::new(tokio::sync::OnceCell::new()))
            .clone();
        cell.get_or_try_init(|| async {
            crate::currency::fetch_current_native_usd_rate(&self.other_client, &key).await
        })
        .await
        .copied()
    }

    async fn helius_collection_history(
        &self,
        collection_address: &str,
    ) -> Result<Arc<HeliusCollectionHistory>, AppError> {
        let key = collection_address.trim().to_string();
        let cell = {
            let mut cache = self.helius_history_cache.lock().map_err(|error| {
                AppError::InvalidData(format!("Helius history cache lock poisoned: {error}"))
            })?;
            cache.get_or_insert_with(key.clone(), || Arc::new(tokio::sync::OnceCell::new()))
        };
        let (client, rpc_url) = self.helius()?;
        let snapshot = self.helius_collection_snapshot(&key).await?;
        let history = cell
            .get_or_try_init(|| async {
                let history = fetch_helius_assets_history_with_budget(
                    client,
                    rpc_url,
                    &key,
                    &snapshot.assets,
                    self.max_history_transactions_per_asset,
                    self.max_history_transactions_per_collection,
                )
                .await?;
                if history.failed_asset_count > 0
                    || history.failed_transaction_count > 0
                    || history.truncated_asset_history_count > 0
                {
                    eprintln!(
                        "warning: Helius history coverage for {key}: failed_assets={}, failed_transactions={}, truncated_assets={}, fetched_transactions={}, reported_transactions={}",
                        history.failed_asset_count,
                        history.failed_transaction_count,
                        history.truncated_asset_history_count,
                        history.fetched_transaction_count,
                        history.reported_transaction_count
                    );
                }
                Ok::<_, AppError>(Arc::new(history))
            })
            .await?;
        self.provider_quality.record_history(&key, history)?;
        Ok(history.clone())
    }

    async fn helius_block_details(
        &self,
        slot: i64,
    ) -> Result<Arc<Vec<crate::api::HeliusTransactionDetails>>, AppError> {
        let cell = {
            let mut cache = self.helius_block_cache.lock().map_err(|error| {
                AppError::InvalidData(format!("Helius block cache lock poisoned: {error}"))
            })?;
            cache.get_or_insert_with(slot, || Arc::new(tokio::sync::OnceCell::new()))
        };
        let (client, rpc_url) = self.helius()?;
        let details = cell
            .get_or_try_init(|| async {
                fetch_helius_block_details(client, rpc_url, slot, None)
                    .await
                    .map(Arc::new)
            })
            .await?;
        Ok(details.clone())
    }

    fn endpoints(
        &self,
        chain: &str,
        explicit_network: Option<&str>,
        api_key: &str,
    ) -> ApiEndpoints {
        ApiEndpoints::for_alchemy(&normalize_network(chain, explicit_network), api_key)
    }

    async fn current_eth_usd_rate(&self, alchemy_api_key: Option<&str>) -> Result<f64, AppError> {
        self.eth_usd_rate
            .get_or_try_init_or_fallback(
                || async {
                    match alchemy_api_key.filter(|key| !key.trim().is_empty()) {
                        Some(alchemy_api_key) => {
                            crate::currency::fetch_current_eth_usd_rate_alchemy_first(
                                &self.alchemy_client,
                                &self.other_client,
                                alchemy_api_key,
                            )
                            .await
                        }
                        None => {
                            crate::currency::fetch_current_eth_usd_rate_with_timeout(
                                &self.other_client,
                                Duration::from_secs(5),
                            )
                            .await
                        }
                    }
                },
                FALLBACK_ETH_USD_RATE,
            )
            .await
    }

    async fn fetch_helius_block_transfers(
        &self,
        slot: i64,
        address: &str,
        inbound_only: bool,
    ) -> Result<Vec<EthTransferRecord>, AppError> {
        let mut transfers = self
            .helius_block_details(slot)
            .await?
            .iter()
            .flat_map(|details| details.native_transfers.iter())
            .filter(|transfer| {
                transfer.to_address == address
                    || (!inbound_only && transfer.from_address == address)
            })
            .cloned()
            .collect::<Vec<_>>();
        if !transfers.is_empty() {
            if let Ok(rate) = self.current_native_usd_rate("solana").await {
                for transfer in &mut transfers {
                    if transfer.value_usd.is_none()
                        && matches!(transfer.payment_token_symbol.as_str(), "SOL" | "WSOL")
                    {
                        transfer.value_usd = Some(transfer.value_eth * rate);
                    }
                }
            }
        }
        Ok(transfers)
    }
}

#[async_trait]
impl AnalyzeApi for RealApi {
    async fn warm_eth_usd_rate(&self) -> Result<(), AppError> {
        self.current_eth_usd_rate(None).await.map(|_| ())
    }

    async fn fetch_provider_data_quality(
        &self,
        chain: &str,
        contract_address: &str,
    ) -> Result<ProviderDataQualityPayload, AppError> {
        if !chain.trim().eq_ignore_ascii_case("solana") {
            return Ok(ProviderDataQualityPayload::default());
        }
        let mut quality = self.provider_quality.get(contract_address)?;
        quality.supplemental_provider_failure_count = i64::from(
            self.opensea_supplement_failures
                .lock()
                .map_err(|error| {
                    AppError::InvalidData(format!(
                        "OpenSea supplement failure lock poisoned: {error}"
                    ))
                })?
                .contains(contract_address.trim()),
        );
        Ok(quality)
    }

    async fn fetch_contract_metadata(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        opensea_api_key: &str,
        contract_address: &str,
    ) -> Result<ContractMetadata, AppError> {
        if chain.trim().eq_ignore_ascii_case("solana") {
            let snapshot = self.helius_collection_snapshot(contract_address).await?;
            return Ok(ContractMetadata {
                chain: "solana".to_string(),
                contract_address: contract_address.trim().to_string(),
                token_type: "NonFungible".to_string(),
                name: snapshot.collection_name.clone(),
                symbol: snapshot.collection_symbol.clone(),
                owner_address: snapshot.collection_authority.clone(),
                ..ContractMetadata::default()
            });
        }
        let endpoints = self.endpoints(chain, alchemy_network, alchemy_api_key);
        fetch_contract_metadata_with_opensea_fallback_clients(
            &self.alchemy_client,
            &self.other_client,
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
        if chain.trim().eq_ignore_ascii_case("solana") {
            let snapshot = self.helius_collection_snapshot(contract_address).await?;
            return Ok(snapshot
                .assets
                .iter()
                .map(|asset| asset.nft.clone())
                .collect());
        }
        let endpoints = self.endpoints(chain, alchemy_network, alchemy_api_key);
        fetch_seed_contract_nfts(&self.alchemy_client, &endpoints, chain, contract_address).await
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
        if chain.trim().eq_ignore_ascii_case("solana") {
            let snapshot = self.helius_collection_snapshot(contract_address).await?;
            return Ok(snapshot
                .assets
                .iter()
                .map(|asset| asset.nft.clone())
                .collect());
        }
        let endpoints = self.endpoints(chain, alchemy_network, alchemy_api_key);
        fetch_contract_nfts_with_fallback_clients(
            &self.alchemy_client,
            &self.other_client,
            &endpoints,
            chain,
            contract_address,
            etherscan_api_key,
            opensea_api_key,
        )
        .await
    }

    async fn fetch_contract_total_supply(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        contract_address: &str,
    ) -> Result<Option<u64>, AppError> {
        if chain.trim().eq_ignore_ascii_case("solana") {
            let snapshot = self.helius_collection_snapshot(contract_address).await?;
            return Ok(Some(snapshot.total as u64));
        }
        let endpoints = self.endpoints(chain, alchemy_network, alchemy_api_key);
        fetch_contract_total_supply(&self.alchemy_client, &endpoints, contract_address).await
    }

    async fn fetch_license_sample(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        seed_nfts: &[SeedNft],
    ) -> Result<bool, AppError> {
        if chain.trim().eq_ignore_ascii_case("solana") {
            return Ok(seed_nfts.iter().any(|nft| {
                serde_json::from_str::<serde_json::Value>(&nft.metadata_json)
                    .ok()
                    .is_some_and(|payload| is_open_license_payload(&payload))
            }));
        }
        let endpoints = self.endpoints(chain, alchemy_network, alchemy_api_key);
        let payload = fetch_license_sample(&self.alchemy_client, &endpoints, seed_nfts).await?;
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
        if chain.trim().eq_ignore_ascii_case("solana") {
            return Ok(self
                .helius_collection_history(contract_address)
                .await?
                .transfers
                .clone());
        }
        let endpoints = self.endpoints(chain, alchemy_network, alchemy_api_key);
        fetch_contract_transfers_with_etherscan_fallback(
            &self.alchemy_client,
            &self.other_client,
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
        if chain.trim().eq_ignore_ascii_case("solana") {
            let snapshot = self.helius_collection_snapshot(contract_address).await?;
            let mut by_owner = BTreeMap::<String, BTreeMap<String, i64>>::new();
            for asset in &snapshot.assets {
                if asset.owner_address.is_empty() {
                    continue;
                }
                by_owner
                    .entry(asset.owner_address.clone())
                    .or_default()
                    .insert(asset.nft.token_id.clone(), 1);
            }
            return Ok(by_owner
                .into_iter()
                .map(|(owner_address, token_balances)| OwnerBalance {
                    owner_address,
                    token_balances,
                })
                .collect());
        }
        let endpoints = self.endpoints(chain, alchemy_network, alchemy_api_key);
        fetch_contract_owners(&self.alchemy_client, &endpoints, contract_address).await
    }

    async fn candidate_currently_holds_seed_nft(
        &self,
        request: CandidateSeedHolderRequest<'_>,
    ) -> Result<Option<bool>, AppError> {
        if request.chain.trim().eq_ignore_ascii_case("solana") {
            let snapshot = self
                .helius_collection_snapshot(request.seed_contract_address)
                .await?;
            if snapshot.truncated {
                return Ok(None);
            }
            return Ok(Some(snapshot.assets.iter().any(|asset| {
                asset.owner_address.trim() == request.candidate_contract_address.trim()
            })));
        }
        let endpoints = self.endpoints(
            request.chain,
            request.alchemy_network,
            request.alchemy_api_key,
        );
        fetch_account_holds_contract_alchemy_first(
            &self.alchemy_client,
            &self.other_client,
            &endpoints,
            request.chain,
            request.candidate_contract_address,
            request.seed_contract_address,
            OpenSeaAccountFallback {
                api_key: request.opensea_api_key,
                collection_slug: request.seed_collection_slug,
            },
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
        if chain.trim().eq_ignore_ascii_case("solana") {
            return Ok(None);
        }
        let endpoints = self.endpoints(chain, alchemy_network, alchemy_api_key);
        fetch_contract_collection_slug_alchemy_first(
            &self.alchemy_client,
            &self.other_client,
            &endpoints,
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
        if chain.trim().eq_ignore_ascii_case("solana") {
            let history = self.helius_collection_history(contract_address).await?;
            let mut rows = history.sales.clone();
            let needs_native_rate =
                rows.iter().any(|sale| sale.is_native_eth) || !opensea_api_key.trim().is_empty();
            let native_rate = if needs_native_rate {
                self.current_native_usd_rate("solana").await.ok()
            } else {
                None
            };
            if let Some(rate) = native_rate {
                for sale in &mut rows {
                    if sale.price_usd.is_none() && sale.is_native_eth {
                        sale.price_usd = sale.price_eth.map(|amount| amount * rate);
                        sale.seller_fee_usd = sale.seller_fee_eth * rate;
                        sale.protocol_fee_usd = sale.protocol_fee_eth * rate;
                        sale.royalty_fee_usd = sale.royalty_fee_eth * rate;
                    }
                }
            }
            let endpoints = self.endpoints(chain, alchemy_network, alchemy_api_key);
            let supplemental = match fetch_contract_sales_with_clients(
                &self.alchemy_client,
                &self.other_client,
                &endpoints,
                chain,
                contract_address,
                opensea_api_key,
                native_rate,
            )
            .await
            {
                Ok(rows) => rows,
                Err(error) => {
                    eprintln!(
                        "warning: OpenSea Solana contract sales failed for {contract_address}: {error}; continuing with Helius data and recording provider degradation"
                    );
                    self.opensea_supplement_failures
                        .lock()
                        .map_err(|lock_error| {
                            AppError::InvalidData(format!(
                                "OpenSea supplement failure lock poisoned: {lock_error}"
                            ))
                        })?
                        .insert(contract_address.trim().to_string());
                    Vec::new()
                }
            };
            let mut seen = rows
                .iter()
                .map(|sale| (sale.tx_hash.clone(), sale.token_id.clone()))
                .collect::<std::collections::HashSet<_>>();
            rows.extend(
                supplemental
                    .into_iter()
                    .filter(|sale| seen.insert((sale.tx_hash.clone(), sale.token_id.clone()))),
            );
            rows.sort_by_key(|sale| (sale.block_number, sale.log_index, sale.bundle_index));
            return Ok(rows);
        }
        let endpoints = self.endpoints(chain, alchemy_network, alchemy_api_key);
        let native_usd_rate = match if chain.trim().eq_ignore_ascii_case("ethereum")
            || chain.trim().eq_ignore_ascii_case("base")
        {
            self.current_eth_usd_rate(Some(alchemy_api_key)).await
        } else {
            self.current_native_usd_rate(chain).await
        } {
            Ok(rate) => Some(rate),
            Err(err) => {
                if !self
                    .eth_usd_rate_warning_emitted
                    .swap(true, Ordering::Relaxed)
                {
                    eprintln!(
                        "warning: failed to fetch native/USD rate for {chain}:{contract_address}: {err}; native sales will not be USD-normalized"
                    );
                }
                None
            }
        };
        fetch_contract_sales_with_clients(
            &self.alchemy_client,
            &self.other_client,
            &endpoints,
            chain,
            contract_address,
            opensea_api_key,
            native_usd_rate,
        )
        .await
    }

    async fn fetch_transaction_receipt(
        &self,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        tx_hash: &str,
    ) -> Result<TransactionReceiptRecord, AppError> {
        let endpoints = self.endpoints("ethereum", alchemy_network, alchemy_api_key);
        fetch_transaction_receipt(&self.alchemy_client, &endpoints, tx_hash).await
    }

    async fn fetch_transaction_receipts_for_block(
        &self,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        block_number: i64,
    ) -> Result<BTreeMap<String, TransactionReceiptRecord>, AppError> {
        let endpoints = self.endpoints("ethereum", alchemy_network, alchemy_api_key);
        fetch_transaction_receipts_for_block(&self.alchemy_client, &endpoints, block_number).await
    }

    async fn fetch_eth_balance(
        &self,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        address: &str,
        block_number: i64,
    ) -> Result<f64, AppError> {
        let endpoints = self.endpoints("ethereum", alchemy_network, alchemy_api_key);
        fetch_eth_balance(&self.alchemy_client, &endpoints, address, block_number).await
    }

    async fn fetch_same_block_eth_transfers_for_address(
        &self,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        block_number: i64,
        address: &str,
    ) -> Result<Vec<EthTransferRecord>, AppError> {
        let endpoints = self.endpoints("ethereum", alchemy_network, alchemy_api_key);
        fetch_same_block_value_transfers_for_address(
            &self.alchemy_client,
            &endpoints,
            "ethereum",
            block_number,
            address,
            None,
        )
        .await
    }

    async fn fetch_transaction_receipt_on_chain(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        tx_hash: &str,
    ) -> Result<TransactionReceiptRecord, AppError> {
        if chain.trim().eq_ignore_ascii_case("solana") {
            let (client, rpc_url) = self.helius()?;
            return Ok(
                fetch_helius_transaction_details(client, rpc_url, tx_hash, None)
                    .await?
                    .map(|details| details.receipt)
                    .unwrap_or_else(|| TransactionReceiptRecord {
                        tx_hash: tx_hash.to_string(),
                        ..TransactionReceiptRecord::default()
                    }),
            );
        }
        let endpoints = self.endpoints(chain, alchemy_network, alchemy_api_key);
        let mut receipt =
            fetch_transaction_receipt(&self.alchemy_client, &endpoints, tx_hash).await?;
        if chain.trim().eq_ignore_ascii_case("polygon") {
            enrich_polygon_receipt_fee(
                &mut receipt,
                self.current_native_usd_rate("polygon").await.ok(),
            );
        }
        Ok(receipt)
    }

    async fn fetch_transaction_receipts_for_block_on_chain(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        block_number: i64,
    ) -> Result<BTreeMap<String, TransactionReceiptRecord>, AppError> {
        if chain.trim().eq_ignore_ascii_case("solana") {
            return Ok(self
                .helius_block_details(block_number)
                .await?
                .iter()
                .filter(|details| !details.receipt.tx_hash.is_empty())
                .map(|details| (details.receipt.tx_hash.clone(), details.receipt.clone()))
                .collect());
        }
        let endpoints = self.endpoints(chain, alchemy_network, alchemy_api_key);
        let mut receipts =
            fetch_transaction_receipts_for_block(&self.alchemy_client, &endpoints, block_number)
                .await?;
        if chain.trim().eq_ignore_ascii_case("polygon") {
            let rate = self.current_native_usd_rate("polygon").await.ok();
            for receipt in receipts.values_mut() {
                enrich_polygon_receipt_fee(receipt, rate);
            }
        }
        Ok(receipts)
    }

    async fn fetch_eth_balance_on_chain(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        address: &str,
        block_number: i64,
    ) -> Result<f64, AppError> {
        if chain.trim().eq_ignore_ascii_case("solana") {
            return self
                .helius_block_details(block_number)
                .await?
                .iter()
                .find_map(|details| details.pre_balances_native.get(address).copied())
                .ok_or_else(|| {
                    AppError::InvalidData(format!(
                        "historical SOL balance is unavailable for {address} at slot {block_number}"
                    ))
                });
        }
        let endpoints = self.endpoints(chain, alchemy_network, alchemy_api_key);
        fetch_eth_balance(&self.alchemy_client, &endpoints, address, block_number).await
    }

    async fn fetch_pre_transaction_native_balance_on_chain(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        tx_hash: &str,
        address: &str,
        block_number: i64,
    ) -> Result<f64, AppError> {
        if chain.trim().eq_ignore_ascii_case("solana") {
            let (client, rpc_url) = self.helius()?;
            let details = fetch_helius_transaction_details(client, rpc_url, tx_hash, None)
                .await?
                .ok_or_else(|| {
                    AppError::InvalidData(format!(
                        "Solana transaction {tx_hash} is unavailable for pre-balance lookup"
                    ))
                })?;
            return details
                .pre_balances_native
                .get(address.trim())
                .copied()
                .ok_or_else(|| {
                    AppError::InvalidData(format!(
                        "pre-transaction SOL balance is unavailable for {address} in {tx_hash}"
                    ))
                });
        }
        self.fetch_eth_balance_on_chain(
            chain,
            alchemy_api_key,
            alchemy_network,
            address,
            block_number,
        )
        .await
    }

    async fn fetch_same_block_eth_transfers_for_address_on_chain(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        block_number: i64,
        address: &str,
    ) -> Result<Vec<EthTransferRecord>, AppError> {
        if chain.trim().eq_ignore_ascii_case("solana") {
            return self
                .fetch_helius_block_transfers(block_number, address, false)
                .await;
        }
        let endpoints = self.endpoints(chain, alchemy_network, alchemy_api_key);
        fetch_same_block_value_transfers_for_address(
            &self.alchemy_client,
            &endpoints,
            chain,
            block_number,
            address,
            None,
        )
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
        if chain.trim().eq_ignore_ascii_case("solana") {
            return self
                .fetch_helius_block_transfers(block_number, address, false)
                .await;
        }
        let eth_usd_rate = match self.current_eth_usd_rate(Some(alchemy_api_key)).await {
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
            &self.alchemy_client,
            &endpoints,
            chain,
            block_number,
            address,
            eth_usd_rate,
        )
        .await
    }

    async fn fetch_mint_payment_eth_transfers_to_address_on_chain(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        block_number: i64,
        address: &str,
    ) -> Result<Vec<EthTransferRecord>, AppError> {
        if chain.trim().eq_ignore_ascii_case("solana") {
            return self
                .fetch_helius_block_transfers(block_number, address, true)
                .await;
        }
        let eth_usd_rate = match self.current_eth_usd_rate(Some(alchemy_api_key)).await {
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
        fetch_same_block_value_transfers_to_address(
            &self.alchemy_client,
            &endpoints,
            chain,
            block_number,
            address,
            eth_usd_rate,
        )
        .await
    }

    async fn fetch_transaction_value_transfers_to_address_on_chain(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        tx_hash: &str,
        block_number: i64,
        address: &str,
    ) -> Result<Vec<EthTransferRecord>, AppError> {
        if chain.trim().eq_ignore_ascii_case("solana") {
            let (client, rpc_url) = self.helius()?;
            let mut transfers = fetch_helius_transaction_details(client, rpc_url, tx_hash, None)
                .await?
                .map(|details| {
                    details
                        .native_transfers
                        .into_iter()
                        .filter(|transfer| transfer.to_address == address)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            if let Ok(rate) = self.current_native_usd_rate("solana").await {
                for transfer in &mut transfers {
                    if transfer.value_usd.is_none()
                        && matches!(transfer.payment_token_symbol.as_str(), "SOL" | "WSOL")
                    {
                        transfer.value_usd = Some(transfer.value_eth * rate);
                    }
                }
            }
            return Ok(transfers);
        }
        self.fetch_mint_payment_eth_transfers_to_address_on_chain(
            chain,
            alchemy_api_key,
            alchemy_network,
            block_number,
            address,
        )
        .await
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

fn enrich_polygon_receipt_fee(
    receipt: &mut TransactionReceiptRecord,
    native_usd_rate: Option<f64>,
) {
    let fee_native = (receipt.gas_used as f64 * receipt.effective_gas_price_wei as f64)
        / 1_000_000_000_000_000_000_f64;
    if fee_native <= 0.0 || !fee_native.is_finite() {
        return;
    }
    receipt.fee_native = Some(fee_native);
    receipt.fee_usd = native_usd_rate
        .filter(|rate| rate.is_finite() && *rate > 0.0)
        .map(|rate| fee_native * rate);
}

#[cfg(test)]
mod provider_quality_tests {
    use super::{enrich_polygon_receipt_fee, ProviderQualityRegistry};
    use crate::api::{HeliusCollectionHistory, HeliusCollectionSnapshot};
    use crate::models::TransactionReceiptRecord;

    #[test]
    fn provider_quality_registry_retains_more_than_lru_capacity() {
        let registry = ProviderQualityRegistry::default();
        for index in 0..20 {
            registry
                .record_snapshot(
                    &format!("collection-{index}"),
                    &HeliusCollectionSnapshot {
                        total: 100,
                        ..HeliusCollectionSnapshot::default()
                    },
                )
                .unwrap();
            registry
                .record_history(
                    &format!("collection-{index}"),
                    &HeliusCollectionHistory {
                        requested_asset_count: index + 1,
                        successful_asset_count: index + 1,
                        ..HeliusCollectionHistory::default()
                    },
                )
                .unwrap();
        }

        let first = registry.get("collection-0").unwrap();
        assert_eq!(first.asset_listing_total_count, 100);
        assert_eq!(first.history_requested_asset_count, 1);
    }

    #[test]
    fn polygon_receipt_uses_polygon_native_usd_rate() {
        let mut receipt = TransactionReceiptRecord {
            gas_used: 21_000,
            effective_gas_price_wei: 1_000_000_000,
            ..TransactionReceiptRecord::default()
        };

        enrich_polygon_receipt_fee(&mut receipt, Some(0.5));

        assert_eq!(receipt.fee_native, Some(0.000021));
        assert_eq!(receipt.fee_usd, Some(0.0000105));
    }
}
