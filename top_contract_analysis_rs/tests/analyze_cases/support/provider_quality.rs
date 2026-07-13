use super::*;

pub(in crate::analyze_cases) struct QualityApi {
    pub(in crate::analyze_cases) quality_calls: Arc<AtomicUsize>,
    pub(in crate::analyze_cases) fail_quality: bool,
}

#[async_trait]
impl AnalyzeApi for QualityApi {
    async fn fetch_contract_metadata(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        opensea_api_key: &str,
        contract_address: &str,
    ) -> Result<ContractMetadata, AppError> {
        FakeApi
            .fetch_contract_metadata(
                chain,
                alchemy_api_key,
                alchemy_network,
                opensea_api_key,
                contract_address,
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
        FakeApi
            .fetch_seed_contract_nfts(chain, alchemy_api_key, alchemy_network, contract_address)
            .await
    }

    async fn fetch_contract_transfers(
        &self,
        _chain: &str,
        _etherscan_api_key: &str,
        _alchemy_network: Option<&str>,
        _alchemy_api_key: &str,
        _contract_address: &str,
        _token_type: &str,
    ) -> Result<Vec<TransferRecord>, AppError> {
        Ok(Vec::new())
    }

    async fn fetch_contract_owners(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
    ) -> Result<Vec<OwnerBalance>, AppError> {
        Ok(Vec::new())
    }

    async fn fetch_contract_sales(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
        _opensea_api_key: &str,
    ) -> Result<Vec<NftSaleRecord>, AppError> {
        Ok(Vec::new())
    }

    async fn fetch_transaction_receipt(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _tx_hash: &str,
    ) -> Result<TransactionReceiptRecord, AppError> {
        Ok(TransactionReceiptRecord::default())
    }

    async fn fetch_transaction_receipts_for_block(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _block_number: i64,
    ) -> Result<BTreeMap<String, TransactionReceiptRecord>, AppError> {
        Ok(BTreeMap::new())
    }

    async fn fetch_eth_balance(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _address: &str,
        _block_number: i64,
    ) -> Result<f64, AppError> {
        Ok(0.0)
    }

    async fn fetch_same_block_eth_transfers_for_address(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _block_number: i64,
        _address: &str,
    ) -> Result<Vec<EthTransferRecord>, AppError> {
        Ok(Vec::new())
    }

    async fn fetch_provider_data_quality(
        &self,
        _chain: &str,
        _contract_address: &str,
    ) -> Result<ProviderDataQualityPayload, AppError> {
        self.quality_calls.fetch_add(1, Ordering::SeqCst);
        if self.fail_quality {
            return Err(AppError::InvalidData("quality fixture failure".into()));
        }
        Ok(ProviderDataQualityPayload {
            asset_listing_analyzed_count: 1,
            asset_listing_total_count: 1,
            history_requested_asset_count: 1,
            history_successful_asset_count: 1,
            history_complete_asset_count: 1,
            history_complete: true,
            ..ProviderDataQualityPayload::default()
        })
    }
}

pub(in crate::analyze_cases) struct WarmCountingApi<T> {
    pub(in crate::analyze_cases) inner: T,
    pub(in crate::analyze_cases) warm_calls: Arc<AtomicUsize>,
}

#[async_trait]
impl<T> AnalyzeApi for WarmCountingApi<T>
where
    T: AnalyzeApi,
{
    async fn fetch_contract_metadata(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        opensea_api_key: &str,
        contract_address: &str,
    ) -> Result<ContractMetadata, AppError> {
        self.inner
            .fetch_contract_metadata(
                chain,
                alchemy_api_key,
                alchemy_network,
                opensea_api_key,
                contract_address,
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
        self.inner
            .fetch_seed_contract_nfts(chain, alchemy_api_key, alchemy_network, contract_address)
            .await
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
        self.inner
            .fetch_contract_nfts(
                chain,
                alchemy_api_key,
                alchemy_network,
                etherscan_api_key,
                opensea_api_key,
                contract_address,
            )
            .await
    }

    async fn fetch_license_sample(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        seed_nfts: &[SeedNft],
    ) -> Result<bool, AppError> {
        self.inner
            .fetch_license_sample(chain, alchemy_api_key, alchemy_network, seed_nfts)
            .await
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
        self.inner
            .fetch_contract_transfers(
                chain,
                etherscan_api_key,
                alchemy_network,
                alchemy_api_key,
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
        self.inner
            .fetch_contract_owners(chain, alchemy_api_key, alchemy_network, contract_address)
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
        self.inner
            .fetch_contract_sales(
                chain,
                alchemy_api_key,
                alchemy_network,
                contract_address,
                opensea_api_key,
            )
            .await
    }

    async fn fetch_transaction_receipt(
        &self,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        tx_hash: &str,
    ) -> Result<TransactionReceiptRecord, AppError> {
        self.inner
            .fetch_transaction_receipt(alchemy_api_key, alchemy_network, tx_hash)
            .await
    }

    async fn fetch_transaction_receipts_for_block(
        &self,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        block_number: i64,
    ) -> Result<BTreeMap<String, TransactionReceiptRecord>, AppError> {
        self.inner
            .fetch_transaction_receipts_for_block(alchemy_api_key, alchemy_network, block_number)
            .await
    }

    async fn fetch_eth_balance(
        &self,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        address: &str,
        block_number: i64,
    ) -> Result<f64, AppError> {
        self.inner
            .fetch_eth_balance(alchemy_api_key, alchemy_network, address, block_number)
            .await
    }

    async fn fetch_same_block_eth_transfers_for_address(
        &self,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        block_number: i64,
        address: &str,
    ) -> Result<Vec<EthTransferRecord>, AppError> {
        self.inner
            .fetch_same_block_eth_transfers_for_address(
                alchemy_api_key,
                alchemy_network,
                block_number,
                address,
            )
            .await
    }

    async fn warm_eth_usd_rate(&self) -> Result<(), AppError> {
        self.warm_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.warm_eth_usd_rate().await
    }
}
