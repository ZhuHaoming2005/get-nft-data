use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;

use crate::api::{
    fetch_contract_metadata_with_opensea_fallback_clients, fetch_contract_owners,
    fetch_contract_sales_with_clients, fetch_contract_transfers_with_etherscan_fallback,
    fetch_eth_balance, fetch_etherscan_contract_transfers, fetch_is_holder_of_contract,
    fetch_license_sample, fetch_opensea_account_holds_contract_nft,
    fetch_opensea_contract_collection_slug, fetch_opensea_contract_market_events,
    fetch_opensea_contract_nfts, fetch_same_block_eth_transfers_for_address,
    fetch_same_block_value_transfers_for_address, fetch_same_block_value_transfers_to_address,
    fetch_seed_contract_nfts, fetch_transaction_receipt, fetch_transaction_receipts_for_block,
    is_open_license_payload, ApiEndpoints, AsyncApiClient,
};
use crate::currency::FALLBACK_ETH_USD_RATE;
use crate::error::AppError;
use crate::models::{
    ContractMetadata, EthTransferRecord, NftMarketEventRecord, NftSaleRecord, OwnerBalance,
    SeedNft, TransactionReceiptRecord, TransferRecord,
};

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

    async fn warm_eth_usd_rate(&self) -> Result<(), AppError> {
        Ok(())
    }
}

pub struct RealApi {
    alchemy_client: AsyncApiClient,
    other_client: AsyncApiClient,
    eth_usd_rate: crate::currency::EthUsdRateCache,
    eth_usd_rate_warning_emitted: AtomicBool,
}

impl RealApi {
    pub fn new(
        timeout_seconds: u64,
        alchemy_api_max_concurrency: usize,
        other_api_max_concurrency: usize,
        other_api_rate_limit_refill_ms: u64,
    ) -> Result<Self, AppError> {
        Ok(Self {
            alchemy_client: AsyncApiClient::new(timeout_seconds, alchemy_api_max_concurrency)?,
            other_client: AsyncApiClient::new_rate_limited_with_retry_policy(
                timeout_seconds,
                other_api_max_concurrency,
                Duration::from_millis(other_api_rate_limit_refill_ms.max(1)),
                crate::api::DEFAULT_API_RETRIES,
                Duration::from_millis(crate::api::DEFAULT_API_RETRY_DELAY_MS),
            )?,
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
            .get_or_try_init_or_fallback(
                || async {
                    match tokio::time::timeout(
                        Duration::from_secs(5),
                        crate::currency::fetch_current_eth_usd_rate(&self.other_client),
                    )
                    .await
                    {
                        Ok(result) => result,
                        Err(_) => Err(AppError::InvalidData(
                            "ETH/USD rate fetch timed out".to_string(),
                        )),
                    }
                },
                FALLBACK_ETH_USD_RATE,
            )
            .await
    }
}

#[async_trait]
impl AnalyzeApi for RealApi {
    async fn warm_eth_usd_rate(&self) -> Result<(), AppError> {
        self.current_eth_usd_rate().await.map(|_| ())
    }

    async fn fetch_contract_metadata(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        opensea_api_key: &str,
        contract_address: &str,
    ) -> Result<ContractMetadata, AppError> {
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
        let endpoints = self.endpoints(chain, alchemy_network, alchemy_api_key);
        if !opensea_api_key.trim().is_empty() {
            match fetch_opensea_contract_nfts(
                &self.other_client,
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
            fetch_seed_contract_nfts(&self.alchemy_client, &endpoints, chain, contract_address)
                .await;
        match alchemy_result {
            Ok(rows) => Ok(rows),
            Err(alchemy_err) if etherscan_api_key.trim().is_empty() => Err(alchemy_err),
            Err(alchemy_err) => {
                eprintln!(
                    "warning: Alchemy NFT expansion failed for {contract_address}: {alchemy_err}; falling back to Etherscan transfers"
                );
                let transfers = fetch_etherscan_contract_transfers(
                    &self.other_client,
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
        let endpoints = self.endpoints(chain, alchemy_network, alchemy_api_key);
        fetch_contract_owners(&self.alchemy_client, &endpoints, contract_address).await
    }

    async fn candidate_currently_holds_seed_nft(
        &self,
        request: CandidateSeedHolderRequest<'_>,
    ) -> Result<Option<bool>, AppError> {
        let endpoints = self.endpoints(
            request.chain,
            request.alchemy_network,
            request.alchemy_api_key,
        );
        if !request.opensea_api_key.trim().is_empty() {
            if let Some(seed_collection_slug) = request.seed_collection_slug {
                match fetch_opensea_account_holds_contract_nft(
                    &self.other_client,
                    &endpoints.opensea_base,
                    request.chain,
                    request.candidate_contract_address,
                    request.seed_contract_address,
                    request.opensea_api_key,
                    Some(seed_collection_slug),
                )
                .await
                {
                    Ok(holds_seed_nft) => return Ok(Some(holds_seed_nft)),
                    Err(err) => {
                        eprintln!(
                            "warning: OpenSea account NFT lookup failed for {}: {err}; falling back to Alchemy isHolderOfContract",
                            request.candidate_contract_address
                        );
                    }
                }
            }
        }

        fetch_is_holder_of_contract(
            &self.alchemy_client,
            &endpoints,
            request.candidate_contract_address,
            request.seed_contract_address,
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
            &self.other_client,
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
        fetch_contract_sales_with_clients(
            &self.alchemy_client,
            &self.other_client,
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
            &self.other_client,
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
            &self.other_client,
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
        fetch_same_block_eth_transfers_for_address(
            &self.alchemy_client,
            &endpoints,
            block_number,
            address,
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
        let endpoints = self.endpoints(chain, alchemy_network, alchemy_api_key);
        fetch_transaction_receipt(&self.alchemy_client, &endpoints, tx_hash).await
    }

    async fn fetch_transaction_receipts_for_block_on_chain(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        block_number: i64,
    ) -> Result<BTreeMap<String, TransactionReceiptRecord>, AppError> {
        let endpoints = self.endpoints(chain, alchemy_network, alchemy_api_key);
        fetch_transaction_receipts_for_block(&self.alchemy_client, &endpoints, block_number).await
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
        fetch_eth_balance(&self.alchemy_client, &endpoints, address, block_number).await
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
        fetch_same_block_eth_transfers_for_address(
            &self.alchemy_client,
            &endpoints,
            block_number,
            address,
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
            &self.alchemy_client,
            &endpoints,
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
        fetch_same_block_value_transfers_to_address(
            &self.alchemy_client,
            &endpoints,
            block_number,
            address,
            eth_usd_rate,
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
