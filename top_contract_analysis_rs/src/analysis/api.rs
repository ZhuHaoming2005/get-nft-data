use std::collections::{BTreeMap, HashMap};
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
    fetch_helius_assets_history_with_budget, fetch_helius_collection_snapshot,
    fetch_helius_transaction_details, fetch_license_sample,
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
type HeliusHistoryCacheCell = Arc<tokio::sync::OnceCell<Arc<HeliusCollectionHistory>>>;
type HeliusTransactionCacheCell =
    Arc<tokio::sync::OnceCell<Arc<Option<crate::api::HeliusTransactionDetails>>>>;

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
        self.get_or_insert_with_protected(key, create, |_| false)
    }

    fn remove(&mut self, key: &K) -> Option<V> {
        self.entries.remove(key).map(|(value, _)| value)
    }

    fn get_or_insert_with_protected(
        &mut self,
        key: K,
        create: impl FnOnce() -> V,
        is_protected: impl Fn(&K) -> bool,
    ) -> V {
        if let Some(value) = self.get(&key) {
            return value;
        }
        while self.entries.len() >= self.capacity {
            let evicted = self
                .entries
                .iter()
                .filter(|(candidate, _)| !is_protected(candidate))
                .min_by_key(|(_, (_, last_used))| *last_used)
                .map(|(candidate, _)| candidate.clone());
            let Some(evicted) = evicted else {
                break;
            };
            self.entries.remove(&evicted);
        }
        self.clock = self.clock.wrapping_add(1);
        let value = create();
        self.entries.insert(key, (value.clone(), self.clock));
        value
    }
}
type NativeRateCacheCell = Arc<tokio::sync::OnceCell<f64>>;

fn provider_quality_from_evidence(
    snapshot: &HeliusCollectionSnapshot,
    history: &HeliusCollectionHistory,
) -> ProviderDataQualityPayload {
    let listing_total_known = snapshot.coverage_ratio.is_some() || !snapshot.truncated;
    let omitted_known_assets = if listing_total_known {
        snapshot.total.saturating_sub(snapshot.assets.len())
    } else {
        0
    };
    ProviderDataQualityPayload {
        asset_listing_analyzed_count: snapshot.assets.len() as i64,
        asset_listing_total_count: snapshot.total as i64,
        asset_listing_truncated_contract_count: i64::from(snapshot.truncated),
        asset_listing_unknown_total_contract_count: i64::from(
            snapshot.truncated && !listing_total_known,
        ),
        asset_listing_coverage_ratio: snapshot.coverage_ratio,
        history_failed_asset_count: history.failed_asset_count as i64,
        history_requested_asset_count: history.requested_asset_count as i64,
        history_successful_asset_count: history.successful_asset_count as i64,
        history_complete_asset_count: history.complete_asset_count as i64,
        history_unrequested_asset_count: history
            .unrequested_asset_count
            .saturating_add(omitted_known_assets) as i64,
        history_truncated_asset_count: history.truncated_asset_history_count as i64,
        history_fetched_transaction_count: history.fetched_transaction_count as i64,
        history_reported_transaction_count: history.reported_transaction_count as i64,
        history_failed_transaction_count: history.failed_transaction_count as i64,
        history_signature_discovery_failure_count: history.signature_discovery_failure_count as i64,
        history_transaction_detail_failure_count: history.transaction_detail_failure_count as i64,
        history_unattributed_sol_transaction_count: history.unattributed_native_transaction_count
            as i64,
        history_unresolved_compressed_mint_count: history.unresolved_compressed_mint_count as i64,
        collection_authority_missing_count: i64::from(snapshot.collection_authority.is_empty()),
        history_complete: history.complete && !snapshot.truncated,
        ..ProviderDataQualityPayload::default()
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

    fn set_provider_evidence_active(&self, _chain: &str, _contract_address: &str, _active: bool) {}
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
    helius_transaction_cache: Mutex<BoundedLruCache<String, HeliusTransactionCacheCell>>,
    provider_evidence_pins: Mutex<HashMap<String, usize>>,
    sales_provider_failures: Mutex<BoundedLruCache<String, ()>>,
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
    pub matched_contract_max_concurrency: usize,
}

struct HeliusRuntimeConfig {
    concurrency: usize,
    refill_ms: u64,
    rpc_url: String,
    max_history_transactions_per_asset: usize,
    max_history_transactions_per_collection: usize,
    max_assets_per_collection: usize,
    evidence_cache_capacity: usize,
}

fn helius_evidence_cache_capacity(matched_contract_max_concurrency: usize) -> usize {
    matched_contract_max_concurrency.saturating_add(8).max(16)
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
        let endpoint = (!key.is_empty()).then(|| HeliusRuntimeConfig {
            concurrency: helius.max_concurrency,
            refill_ms: helius.rate_limit_refill_ms,
            rpc_url: format!("https://mainnet.helius-rpc.com/?api-key={key}"),
            max_history_transactions_per_asset: helius.max_history_transactions_per_asset,
            max_history_transactions_per_collection: helius.max_history_transactions_per_collection,
            max_assets_per_collection: helius.max_assets_per_collection,
            evidence_cache_capacity: helius_evidence_cache_capacity(
                helius.matched_contract_max_concurrency,
            ),
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
            Some(HeliusRuntimeConfig {
                concurrency: helius_api_max_concurrency,
                refill_ms: 100,
                rpc_url: helius_rpc_url,
                max_history_transactions_per_asset: 100,
                max_history_transactions_per_collection: 10_000,
                max_assets_per_collection: 10_000,
                evidence_cache_capacity: 16,
            }),
        )
    }

    fn new_with_optional_helius(
        timeout_seconds: u64,
        alchemy_api_max_concurrency: usize,
        other_api_max_concurrency: usize,
        other_api_rate_limit_refill_ms: u64,
        helius: Option<HeliusRuntimeConfig>,
    ) -> Result<Self, AppError> {
        let (
            helius_client,
            helius_rpc_url,
            max_history_transactions_per_asset,
            max_history_transactions_per_collection,
            max_helius_assets_per_collection,
            helius_evidence_cache_capacity,
        ) = match helius {
            Some(config) => (
                Some(AsyncApiClient::new_rate_limited_with_in_flight_limit(
                    timeout_seconds,
                    config.concurrency,
                    config.concurrency,
                    Duration::from_millis(config.refill_ms.max(1)),
                )?),
                Some(config.rpc_url),
                config.max_history_transactions_per_asset,
                config.max_history_transactions_per_collection,
                config.max_assets_per_collection,
                config.evidence_cache_capacity,
            ),
            None => (None, None, 0, 0, 0, 16),
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
            helius_collection_cache: Mutex::new(BoundedLruCache::new(
                helius_evidence_cache_capacity,
            )),
            helius_history_cache: Mutex::new(BoundedLruCache::new(helius_evidence_cache_capacity)),
            helius_transaction_cache: Mutex::new(BoundedLruCache::new(512)),
            provider_evidence_pins: Mutex::new(HashMap::new()),
            sales_provider_failures: Mutex::new(BoundedLruCache::new(
                helius_evidence_cache_capacity.max(32),
            )),
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

    fn helius_collection_cache_cell(
        &self,
        key: &str,
    ) -> Result<HeliusCollectionCacheCell, AppError> {
        let pins = self.provider_evidence_pins.lock().map_err(|error| {
            AppError::InvalidData(format!("provider evidence pin lock poisoned: {error}"))
        })?;
        let mut cache = self.helius_collection_cache.lock().map_err(|error| {
            AppError::InvalidData(format!("Helius collection cache lock poisoned: {error}"))
        })?;
        Ok(cache.get_or_insert_with_protected(
            key.to_string(),
            || Arc::new(tokio::sync::OnceCell::new()),
            |candidate| pins.contains_key(candidate),
        ))
    }

    fn helius_history_cache_cell(&self, key: &str) -> Result<HeliusHistoryCacheCell, AppError> {
        let pins = self.provider_evidence_pins.lock().map_err(|error| {
            AppError::InvalidData(format!("provider evidence pin lock poisoned: {error}"))
        })?;
        let mut cache = self.helius_history_cache.lock().map_err(|error| {
            AppError::InvalidData(format!("Helius history cache lock poisoned: {error}"))
        })?;
        Ok(cache.get_or_insert_with_protected(
            key.to_string(),
            || Arc::new(tokio::sync::OnceCell::new()),
            |candidate| pins.contains_key(candidate),
        ))
    }

    async fn helius_collection_snapshot(
        &self,
        collection_address: &str,
    ) -> Result<Arc<HeliusCollectionSnapshot>, AppError> {
        let key = collection_address.trim().to_string();
        let cell = self.helius_collection_cache_cell(&key)?;
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

    async fn current_chain_native_usd_rate(
        &self,
        chain: &str,
        alchemy_api_key: &str,
    ) -> Result<f64, AppError> {
        if uses_eth_native_usd_rate(chain) {
            self.current_eth_usd_rate(Some(alchemy_api_key)).await
        } else {
            self.current_native_usd_rate(chain).await
        }
    }

    async fn helius_collection_history(
        &self,
        collection_address: &str,
    ) -> Result<Arc<HeliusCollectionHistory>, AppError> {
        let key = collection_address.trim().to_string();
        let cell = self.helius_history_cache_cell(&key)?;
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
        Ok(history.clone())
    }

    async fn helius_transaction_details(
        &self,
        signature: &str,
    ) -> Result<Arc<Option<crate::api::HeliusTransactionDetails>>, AppError> {
        let key = signature.trim().to_string();
        let cell = {
            let mut cache = self.helius_transaction_cache.lock().map_err(|error| {
                AppError::InvalidData(format!("Helius transaction cache lock poisoned: {error}"))
            })?;
            cache.get_or_insert_with(key.clone(), || Arc::new(tokio::sync::OnceCell::new()))
        };
        let (client, rpc_url) = self.helius()?;
        let details = cell
            .get_or_try_init(|| async {
                fetch_helius_transaction_details(client, rpc_url, &key, None)
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
}

#[async_trait]
impl AnalyzeApi for RealApi {
    fn set_provider_evidence_active(&self, chain: &str, contract_address: &str, active: bool) {
        if !chain.trim().eq_ignore_ascii_case("solana") {
            return;
        }
        let key = contract_address.trim().to_string();
        let mut pins = self
            .provider_evidence_pins
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if active {
            *pins.entry(key).or_default() += 1;
        } else if let Some(count) = pins.get_mut(&key) {
            *count -= 1;
            if *count == 0 {
                pins.remove(&key);
            }
        }
    }

    async fn warm_eth_usd_rate(&self) -> Result<(), AppError> {
        self.current_eth_usd_rate(None).await.map(|_| ())
    }

    async fn fetch_provider_data_quality(
        &self,
        chain: &str,
        contract_address: &str,
    ) -> Result<ProviderDataQualityPayload, AppError> {
        let sales_provider_failed = self
            .sales_provider_failures
            .lock()
            .map_err(|error| {
                AppError::InvalidData(format!("sales provider failure lock poisoned: {error}"))
            })?
            .remove(&contract_address.trim().to_string())
            .is_some();
        if !chain.trim().eq_ignore_ascii_case("solana") {
            return Ok(ProviderDataQualityPayload {
                supplemental_provider_failure_count: i64::from(sales_provider_failed),
                ..ProviderDataQualityPayload::default()
            });
        }
        let key = contract_address.trim().to_string();
        let snapshot = self
            .helius_collection_cache
            .lock()
            .map_err(|error| {
                AppError::InvalidData(format!("Helius collection cache lock poisoned: {error}"))
            })?
            .get(&key)
            .and_then(|cell| cell.get().cloned());
        let history = self
            .helius_history_cache
            .lock()
            .map_err(|error| {
                AppError::InvalidData(format!("Helius history cache lock poisoned: {error}"))
            })?
            .get(&key)
            .and_then(|cell| cell.get().cloned());
        let mut quality = match (snapshot.as_deref(), history.as_deref()) {
            (Some(snapshot), Some(history)) => provider_quality_from_evidence(snapshot, history),
            (Some(snapshot), None) => {
                let unrequested_history = HeliusCollectionHistory {
                    unrequested_asset_count: snapshot.assets.len(),
                    complete: false,
                    ..HeliusCollectionHistory::default()
                };
                let mut quality = provider_quality_from_evidence(snapshot, &unrequested_history);
                quality.provider_quality_lookup_failure_count = 1;
                quality
            }
            (None, Some(history)) => {
                let mut quality =
                    provider_quality_from_evidence(&HeliusCollectionSnapshot::default(), history);
                quality.collection_authority_missing_count = 0;
                quality.provider_quality_lookup_failure_count = 1;
                quality
            }
            (None, None) => ProviderDataQualityPayload {
                provider_quality_lookup_failure_count: 1,
                ..ProviderDataQualityPayload::default()
            },
        };
        if snapshot.is_none() || history.is_none() {
            quality.history_complete = false;
        }
        quality.supplemental_provider_failure_count = i64::from(sales_provider_failed);
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
                    self.sales_provider_failures
                        .lock()
                        .map_err(|lock_error| {
                            AppError::InvalidData(format!(
                                "sales provider failure lock poisoned: {lock_error}"
                            ))
                        })?
                        .get_or_insert_with(contract_address.trim().to_string(), || ());
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
        match fetch_contract_sales_with_clients(
            &self.alchemy_client,
            &self.other_client,
            &endpoints,
            chain,
            contract_address,
            opensea_api_key,
            native_usd_rate,
        )
        .await
        {
            Ok(rows) => Ok(rows),
            Err(error) => {
                eprintln!(
                    "warning: sales providers failed for {chain}:{contract_address}: {error}; recording provider degradation"
                );
                self.sales_provider_failures
                    .lock()
                    .map_err(|lock_error| {
                        AppError::InvalidData(format!(
                            "sales provider failure lock poisoned: {lock_error}"
                        ))
                    })?
                    .get_or_insert_with(contract_address.trim().to_string(), || ());
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
            return Ok(self
                .helius_transaction_details(tx_hash)
                .await?
                .as_ref()
                .as_ref()
                .map(|details| details.receipt.clone())
                .unwrap_or_else(|| TransactionReceiptRecord {
                    tx_hash: tx_hash.to_string(),
                    ..TransactionReceiptRecord::default()
                }));
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
            return Ok(BTreeMap::new());
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
            return Err(AppError::InvalidData(format!(
                "historical SOL balance requires a target transaction for {address} at slot {block_number}"
            )));
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
            let details = self.helius_transaction_details(tx_hash).await?;
            let details = details.as_ref().as_ref().ok_or_else(|| {
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
            return Ok(Vec::new());
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
            return Ok(Vec::new());
        }
        let native_usd_rate = match self
            .current_chain_native_usd_rate(chain, alchemy_api_key)
            .await
        {
            Ok(rate) => Some(rate),
            Err(err) => {
                if !self
                    .eth_usd_rate_warning_emitted
                    .swap(true, Ordering::Relaxed)
                {
                    eprintln!(
                        "warning: failed to fetch current native/USD rate for {chain} mint value-flow at {address}: {err}; native mint payments will not be USD-normalized"
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
            native_usd_rate,
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
            return Ok(Vec::new());
        }
        let native_usd_rate = match self
            .current_chain_native_usd_rate(chain, alchemy_api_key)
            .await
        {
            Ok(rate) => Some(rate),
            Err(err) => {
                if !self
                    .eth_usd_rate_warning_emitted
                    .swap(true, Ordering::Relaxed)
                {
                    eprintln!(
                        "warning: failed to fetch current native/USD rate for {chain} mint value-flow at {address}: {err}; native mint payments will not be USD-normalized"
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
            native_usd_rate,
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
            let details = self.helius_transaction_details(tx_hash).await?;
            let mut transfers = details
                .as_ref()
                .as_ref()
                .map(|details| {
                    details
                        .native_transfers
                        .iter()
                        .filter(|transfer| transfer.to_address == address)
                        .cloned()
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

fn uses_eth_native_usd_rate(chain: &str) -> bool {
    matches!(
        chain.trim().to_ascii_lowercase().as_str(),
        "ethereum" | "base"
    )
}

#[cfg(test)]
mod provider_quality_tests {
    use super::{
        enrich_polygon_receipt_fee, provider_quality_from_evidence, uses_eth_native_usd_rate,
        AnalyzeApi, HeliusApiConfig, RealApi,
    };
    use crate::analysis::ProviderEvidencePin;
    use crate::api::{HeliusCollectionAsset, HeliusCollectionHistory, HeliusCollectionSnapshot};
    use crate::models::TransactionReceiptRecord;

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

    #[test]
    fn polygon_mint_transfers_do_not_select_the_eth_native_rate() {
        assert!(uses_eth_native_usd_rate("ethereum"));
        assert!(uses_eth_native_usd_rate("base"));
        assert!(!uses_eth_native_usd_rate("polygon"));
    }

    #[test]
    fn truncated_unknown_total_snapshot_is_not_complete_or_fully_covered() {
        let snapshot = HeliusCollectionSnapshot {
            assets: vec![HeliusCollectionAsset {
                nft: crate::models::SeedNft::default(),
                owner_address: String::new(),
                compressed: false,
            }],
            total: 1,
            truncated: true,
            coverage_ratio: None,
            ..HeliusCollectionSnapshot::default()
        };
        let history = HeliusCollectionHistory {
            requested_asset_count: 1,
            successful_asset_count: 1,
            complete_asset_count: 1,
            complete: true,
            ..HeliusCollectionHistory::default()
        };

        let quality = provider_quality_from_evidence(&snapshot, &history);

        assert!(!quality.history_complete);
        assert_eq!(quality.asset_listing_unknown_total_contract_count, 1);
        assert_eq!(quality.asset_listing_coverage_ratio, None);
        assert_eq!(quality.collection_authority_missing_count, 1);
    }

    #[tokio::test]
    async fn real_api_sizes_evidence_caches_for_all_active_solana_contexts() {
        let api = RealApi::new_with_helius(
            5,
            2,
            2,
            10,
            HeliusApiConfig {
                max_concurrency: 2,
                rate_limit_refill_ms: 10,
                api_key: "test",
                max_history_transactions_per_asset: 100,
                max_history_transactions_per_collection: 10_000,
                max_assets_per_collection: 10_000,
                matched_contract_max_concurrency: 17,
            },
        )
        .unwrap();

        assert_eq!(api.helius_collection_cache.lock().unwrap().capacity, 25);
        assert_eq!(api.helius_history_cache.lock().unwrap().capacity, 25);
    }

    #[tokio::test]
    async fn active_solana_evidence_survives_fast_lru_churn() {
        let api = RealApi::new_with_helius(
            5,
            2,
            2,
            10,
            HeliusApiConfig {
                max_concurrency: 2,
                rate_limit_refill_ms: 10,
                api_key: "test",
                max_history_transactions_per_asset: 100,
                max_history_transactions_per_collection: 10_000,
                max_assets_per_collection: 10_000,
                matched_contract_max_concurrency: 2,
            },
        )
        .unwrap();
        api.set_provider_evidence_active("solana", "slow", true);
        api.helius_collection_cache_cell("slow").unwrap();
        api.helius_history_cache_cell("slow").unwrap();

        for index in 0..32 {
            let key = format!("fast-{index}");
            api.helius_collection_cache_cell(&key).unwrap();
            api.helius_history_cache_cell(&key).unwrap();
        }

        assert!(api
            .helius_collection_cache
            .lock()
            .unwrap()
            .get(&"slow".to_string())
            .is_some());
        assert!(api
            .helius_history_cache
            .lock()
            .unwrap()
            .get(&"slow".to_string())
            .is_some());

        api.set_provider_evidence_active("solana", "slow", false);
        for index in 32..64 {
            let key = format!("fast-{index}");
            api.helius_collection_cache_cell(&key).unwrap();
            api.helius_history_cache_cell(&key).unwrap();
        }
        assert!(api.provider_evidence_pins.lock().unwrap().is_empty());
        assert!(api
            .helius_collection_cache
            .lock()
            .unwrap()
            .get(&"slow".to_string())
            .is_none());
        assert!(api
            .helius_history_cache
            .lock()
            .unwrap()
            .get(&"slow".to_string())
            .is_none());
    }

    #[tokio::test]
    async fn provider_evidence_pin_is_released_when_future_is_aborted() {
        let api = std::sync::Arc::new(RealApi::new(5, 2, 2, 10).unwrap());
        let task_api = std::sync::Arc::clone(&api);
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
            let _pin = ProviderEvidencePin::new(task_api.as_ref(), "solana", "cancelled");
            started_tx.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        started_rx.await.unwrap();
        assert_eq!(
            api.provider_evidence_pins.lock().unwrap().get("cancelled"),
            Some(&1)
        );

        handle.abort();
        let _ = handle.await;

        assert!(!api
            .provider_evidence_pins
            .lock()
            .unwrap()
            .contains_key("cancelled"));
    }

    #[tokio::test]
    async fn evm_sales_provider_failure_is_reported_once_and_released() {
        let api = RealApi::new(5, 2, 2, 10).unwrap();
        api.sales_provider_failures
            .lock()
            .unwrap()
            .get_or_insert_with("0xabc".to_string(), || ());

        let first = api
            .fetch_provider_data_quality("ethereum", "0xabc")
            .await
            .unwrap();
        let second = api
            .fetch_provider_data_quality("ethereum", "0xabc")
            .await
            .unwrap();

        assert_eq!(first.supplemental_provider_failure_count, 1);
        assert_eq!(second.supplemental_provider_failure_count, 0);
        assert!(api
            .sales_provider_failures
            .lock()
            .unwrap()
            .entries
            .is_empty());
    }
}
