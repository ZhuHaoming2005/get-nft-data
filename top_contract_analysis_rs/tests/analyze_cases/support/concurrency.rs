use super::*;

pub(in crate::analyze_cases) struct ConcurrentContractApi {
    pub(in crate::analyze_cases) active_transfer_fetches: AtomicUsize,
    pub(in crate::analyze_cases) max_transfer_fetches: AtomicUsize,
}

impl ConcurrentContractApi {
    pub(in crate::analyze_cases) fn new() -> Self {
        Self {
            active_transfer_fetches: AtomicUsize::new(0),
            max_transfer_fetches: AtomicUsize::new(0),
        }
    }
}

pub(in crate::analyze_cases) struct StaggeredExpansionApi {
    pub(in crate::analyze_cases) slow_expansion_done: AtomicUsize,
    pub(in crate::analyze_cases) transfer_before_slow_expansion_done: AtomicUsize,
}

impl StaggeredExpansionApi {
    pub(in crate::analyze_cases) fn new() -> Self {
        Self {
            slow_expansion_done: AtomicUsize::new(0),
            transfer_before_slow_expansion_done: AtomicUsize::new(0),
        }
    }
}

pub(in crate::analyze_cases) struct ConcurrentExpansionSupplyApi {
    pub(in crate::analyze_cases) expansion_count: usize,
    pub(in crate::analyze_cases) expansion_active: AtomicBool,
    pub(in crate::analyze_cases) supply_observed_expansion_active: AtomicBool,
    pub(in crate::analyze_cases) total_supply_calls: AtomicUsize,
}

impl ConcurrentExpansionSupplyApi {
    pub(in crate::analyze_cases) fn new() -> Self {
        Self::with_expansion_count(1)
    }

    pub(in crate::analyze_cases) fn with_expansion_count(expansion_count: usize) -> Self {
        Self {
            expansion_count,
            expansion_active: AtomicBool::new(false),
            supply_observed_expansion_active: AtomicBool::new(false),
            total_supply_calls: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl AnalyzeApi for ConcurrentExpansionSupplyApi {
    async fn fetch_contract_metadata(
        &self,
        chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _opensea_api_key: &str,
        contract_address: &str,
    ) -> Result<ContractMetadata, AppError> {
        Ok(ContractMetadata {
            chain: chain.to_string(),
            contract_address: contract_address.to_string(),
            token_type: "ERC721".into(),
            contract_deployer: "0xcreator".into(),
            deployed_block_number: if contract_address == "0xseed" {
                100
            } else {
                200
            },
            deployed_block_time: 0,
            owner_address: String::new(),
            admin_address: String::new(),
            proxy_admin_address: String::new(),
            name: "Azuki".into(),
            symbol: "AZUKI".into(),
        })
    }

    async fn fetch_seed_contract_nfts(
        &self,
        chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        contract_address: &str,
    ) -> Result<Vec<SeedNft>, AppError> {
        Ok(vec![SeedNft {
            chain: chain.to_string(),
            contract_address: contract_address.to_string(),
            token_id: "1".into(),
            name: "Azuki #1".into(),
            symbol: "AZUKI".into(),
            token_uri: "ipfs://seed/1".into(),
            image_uri: "ipfs://image/1.png".into(),
            metadata_json: r#"{"name":"Azuki #1","description":"gold dragon"}"#.into(),
        }])
    }

    async fn fetch_contract_nfts(
        &self,
        chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _etherscan_api_key: &str,
        _opensea_api_key: &str,
        contract_address: &str,
    ) -> Result<Vec<SeedNft>, AppError> {
        self.expansion_active.store(true, Ordering::SeqCst);
        sleep(Duration::from_millis(80)).await;
        self.expansion_active.store(false, Ordering::SeqCst);
        Ok((1..=self.expansion_count)
            .map(|token_id| SeedNft {
                chain: chain.to_string(),
                contract_address: contract_address.to_string(),
                token_id: token_id.to_string(),
                name: format!("Azuki Mirror #{token_id}"),
                symbol: "AZUKI".into(),
                token_uri: format!("ipfs://seed/{token_id}"),
                image_uri: format!("ipfs://image/{token_id}.png"),
                metadata_json: r#"{"description":"gold dragon"}"#.into(),
            })
            .collect())
    }

    async fn fetch_contract_total_supply(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
    ) -> Result<Option<u64>, AppError> {
        self.total_supply_calls.fetch_add(1, Ordering::SeqCst);
        let mut waited = 0;
        while !self.expansion_active.load(Ordering::SeqCst) && waited < 100 {
            sleep(Duration::from_millis(1)).await;
            waited += 1;
        }
        if self.expansion_active.load(Ordering::SeqCst) {
            self.supply_observed_expansion_active
                .store(true, Ordering::SeqCst);
        }
        Ok(Some(100))
    }

    async fn fetch_contract_transfers(
        &self,
        _chain: &str,
        _etherscan_api_key: &str,
        _alchemy_network: Option<&str>,
        _alchemy_api_key: &str,
        contract_address: &str,
        _token_type: &str,
    ) -> Result<Vec<TransferRecord>, AppError> {
        Ok(vec![TransferRecord {
            contract_address: contract_address.to_string(),
            token_id: "1".into(),
            tx_hash: "0xmint".into(),
            log_index: 0,
            block_number: 1,
            block_time: 100,
            from_address: ZERO_ADDRESS.into(),
            to_address: "0xminter".into(),
            event_type: "erc721".into(),
            source: "alchemy".into(),
        }])
    }

    async fn fetch_contract_owners(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
    ) -> Result<Vec<OwnerBalance>, AppError> {
        Ok(vec![])
    }

    async fn fetch_contract_sales(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
        _opensea_api_key: &str,
    ) -> Result<Vec<NftSaleRecord>, AppError> {
        Ok(vec![])
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
        Ok(vec![])
    }
}

#[async_trait]
impl AnalyzeApi for StaggeredExpansionApi {
    async fn fetch_contract_metadata(
        &self,
        chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _opensea_api_key: &str,
        contract_address: &str,
    ) -> Result<ContractMetadata, AppError> {
        Ok(ContractMetadata {
            chain: chain.to_string(),
            contract_address: contract_address.to_string(),
            token_type: "ERC721".into(),
            contract_deployer: "0xcreator".into(),
            deployed_block_number: 123,
            deployed_block_time: 0,
            owner_address: String::new(),
            admin_address: String::new(),
            proxy_admin_address: String::new(),
            name: "Azuki".into(),
            symbol: "AZUKI".into(),
        })
    }

    async fn fetch_seed_contract_nfts(
        &self,
        chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        contract_address: &str,
    ) -> Result<Vec<SeedNft>, AppError> {
        Ok(vec![SeedNft {
            chain: chain.to_string(),
            contract_address: contract_address.to_string(),
            token_id: "1".into(),
            name: "Azuki #1".into(),
            symbol: "AZUKI".into(),
            token_uri: "ipfs://seed/1".into(),
            image_uri: "ipfs://image/1.png".into(),
            metadata_json: r#"{"name":"Azuki #1","description":"gold dragon"}"#.into(),
        }])
    }

    async fn fetch_contract_nfts(
        &self,
        chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _etherscan_api_key: &str,
        _opensea_api_key: &str,
        contract_address: &str,
    ) -> Result<Vec<SeedNft>, AppError> {
        if contract_address == "0xdup1" {
            sleep(Duration::from_millis(120)).await;
            self.slow_expansion_done.store(1, Ordering::SeqCst);
        }
        Ok(vec![SeedNft {
            chain: chain.to_string(),
            contract_address: contract_address.to_string(),
            token_id: "1".into(),
            name: format!("Azuki Mirror {}", &contract_address[5..]),
            symbol: "AZUKI".into(),
            token_uri: "ipfs://seed/1".into(),
            image_uri: "ipfs://image/1.png".into(),
            metadata_json: r#"{"description":"gold dragon"}"#.into(),
        }])
    }

    async fn fetch_contract_transfers(
        &self,
        _chain: &str,
        _etherscan_api_key: &str,
        _alchemy_network: Option<&str>,
        _alchemy_api_key: &str,
        contract_address: &str,
        _token_type: &str,
    ) -> Result<Vec<TransferRecord>, AppError> {
        if contract_address == "0xdup2" && self.slow_expansion_done.load(Ordering::SeqCst) == 0 {
            self.transfer_before_slow_expansion_done
                .store(1, Ordering::SeqCst);
        }
        Ok(vec![TransferRecord {
            contract_address: contract_address.to_string(),
            token_id: "1".into(),
            tx_hash: format!("0xmint-{contract_address}"),
            log_index: 0,
            block_number: 1,
            block_time: 100,
            from_address: "0x0000000000000000000000000000000000000000".into(),
            to_address: "0xminter".into(),
            event_type: "erc721".into(),
            source: "alchemy".into(),
        }])
    }

    async fn fetch_contract_owners(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
    ) -> Result<Vec<OwnerBalance>, AppError> {
        Ok(vec![])
    }

    async fn fetch_contract_sales(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
        _opensea_api_key: &str,
    ) -> Result<Vec<NftSaleRecord>, AppError> {
        Ok(vec![])
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
        Ok(vec![])
    }
}

#[async_trait]
impl AnalyzeApi for ConcurrentContractApi {
    async fn fetch_contract_metadata(
        &self,
        chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _opensea_api_key: &str,
        contract_address: &str,
    ) -> Result<ContractMetadata, AppError> {
        Ok(ContractMetadata {
            chain: chain.to_string(),
            contract_address: contract_address.to_string(),
            token_type: "ERC721".into(),
            contract_deployer: "0xcreator".into(),
            deployed_block_number: 123,
            deployed_block_time: 0,
            owner_address: String::new(),
            admin_address: String::new(),
            proxy_admin_address: String::new(),
            name: "Azuki".into(),
            symbol: "AZUKI".into(),
        })
    }

    async fn fetch_seed_contract_nfts(
        &self,
        chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        contract_address: &str,
    ) -> Result<Vec<SeedNft>, AppError> {
        Ok(vec![SeedNft {
            chain: chain.to_string(),
            contract_address: contract_address.to_string(),
            token_id: "1".into(),
            name: "Azuki #1".into(),
            symbol: "AZUKI".into(),
            token_uri: "ipfs://seed/1".into(),
            image_uri: "ipfs://image/1.png".into(),
            metadata_json: r#"{"name":"Azuki #1","description":"gold dragon"}"#.into(),
        }])
    }

    async fn fetch_contract_transfers(
        &self,
        _chain: &str,
        _etherscan_api_key: &str,
        _alchemy_network: Option<&str>,
        _alchemy_api_key: &str,
        contract_address: &str,
        _token_type: &str,
    ) -> Result<Vec<TransferRecord>, AppError> {
        let active = self.active_transfer_fetches.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_transfer_fetches
            .fetch_max(active, Ordering::SeqCst);
        sleep(Duration::from_millis(40)).await;
        self.active_transfer_fetches.fetch_sub(1, Ordering::SeqCst);

        Ok(vec![TransferRecord {
            contract_address: contract_address.to_string(),
            token_id: "1".into(),
            tx_hash: format!("0xmint-{contract_address}"),
            log_index: 0,
            block_number: 1,
            block_time: 100,
            from_address: "0x0000000000000000000000000000000000000000".into(),
            to_address: "0xminter".into(),
            event_type: "erc721".into(),
            source: "alchemy".into(),
        }])
    }

    async fn fetch_contract_owners(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
    ) -> Result<Vec<OwnerBalance>, AppError> {
        Ok(vec![])
    }

    async fn fetch_contract_sales(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
        _opensea_api_key: &str,
    ) -> Result<Vec<NftSaleRecord>, AppError> {
        Ok(vec![])
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
        Ok(vec![])
    }
}

pub(in crate::analyze_cases) struct ObsoleteReceiptMetricProbeApi {
    pub(in crate::analyze_cases) active_receipts: AtomicUsize,
    pub(in crate::analyze_cases) max_receipts: AtomicUsize,
    pub(in crate::analyze_cases) duplicate_sale_tx: bool,
    pub(in crate::analyze_cases) same_buyer_history: bool,
    pub(in crate::analyze_cases) receipt_calls: AtomicUsize,
    pub(in crate::analyze_cases) balance_calls: AtomicUsize,
    pub(in crate::analyze_cases) same_block_transfer_calls: AtomicUsize,
}

impl ObsoleteReceiptMetricProbeApi {
    pub(in crate::analyze_cases) fn new() -> Self {
        Self {
            active_receipts: AtomicUsize::new(0),
            max_receipts: AtomicUsize::new(0),
            duplicate_sale_tx: false,
            same_buyer_history: false,
            receipt_calls: AtomicUsize::new(0),
            balance_calls: AtomicUsize::new(0),
            same_block_transfer_calls: AtomicUsize::new(0),
        }
    }

    pub(in crate::analyze_cases) fn with_duplicate_sale_tx() -> Self {
        Self {
            duplicate_sale_tx: true,
            ..Self::new()
        }
    }

    pub(in crate::analyze_cases) fn with_same_buyer_history() -> Self {
        Self {
            same_buyer_history: true,
            ..Self::new()
        }
    }
}

pub(in crate::analyze_cases) struct ConcurrentSingleContractFetchApi {
    pub(in crate::analyze_cases) active_fetches: AtomicUsize,
    pub(in crate::analyze_cases) max_fetches: AtomicUsize,
    pub(in crate::analyze_cases) active_post_signal_fetches: AtomicUsize,
    pub(in crate::analyze_cases) max_post_signal_fetches: AtomicUsize,
}

impl ConcurrentSingleContractFetchApi {
    pub(in crate::analyze_cases) fn new() -> Self {
        Self {
            active_fetches: AtomicUsize::new(0),
            max_fetches: AtomicUsize::new(0),
            active_post_signal_fetches: AtomicUsize::new(0),
            max_post_signal_fetches: AtomicUsize::new(0),
        }
    }

    async fn overlap_delay(&self) {
        let active = self.active_fetches.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_fetches.fetch_max(active, Ordering::SeqCst);
        sleep(Duration::from_millis(40)).await;
        self.active_fetches.fetch_sub(1, Ordering::SeqCst);
    }

    async fn post_signal_overlap_delay(&self) {
        let active = self
            .active_post_signal_fetches
            .fetch_add(1, Ordering::SeqCst)
            + 1;
        self.max_post_signal_fetches
            .fetch_max(active, Ordering::SeqCst);
        sleep(Duration::from_millis(40)).await;
        self.active_post_signal_fetches
            .fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl AnalyzeApi for ConcurrentSingleContractFetchApi {
    async fn fetch_contract_metadata(
        &self,
        chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _opensea_api_key: &str,
        contract_address: &str,
    ) -> Result<ContractMetadata, AppError> {
        Ok(ContractMetadata {
            chain: chain.to_string(),
            contract_address: contract_address.to_string(),
            token_type: "ERC721".into(),
            contract_deployer: "0xcreator".into(),
            deployed_block_number: 123,
            deployed_block_time: 0,
            owner_address: String::new(),
            admin_address: String::new(),
            proxy_admin_address: String::new(),
            name: "Azuki".into(),
            symbol: "AZUKI".into(),
        })
    }

    async fn fetch_seed_contract_nfts(
        &self,
        chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        contract_address: &str,
    ) -> Result<Vec<SeedNft>, AppError> {
        Ok(vec![SeedNft {
            chain: chain.to_string(),
            contract_address: contract_address.to_string(),
            token_id: "1".into(),
            name: "Azuki #1".into(),
            symbol: "AZUKI".into(),
            token_uri: "ipfs://seed/1".into(),
            image_uri: "ipfs://image/1.png".into(),
            metadata_json: r#"{"name":"Azuki #1","description":"gold dragon"}"#.into(),
        }])
    }

    async fn fetch_contract_transfers(
        &self,
        _chain: &str,
        _etherscan_api_key: &str,
        _alchemy_network: Option<&str>,
        _alchemy_api_key: &str,
        contract_address: &str,
        _token_type: &str,
    ) -> Result<Vec<TransferRecord>, AppError> {
        self.overlap_delay().await;
        Ok(vec![TransferRecord {
            contract_address: contract_address.to_string(),
            token_id: "1".into(),
            tx_hash: "0xmint".into(),
            log_index: 0,
            block_number: 1,
            block_time: 100,
            from_address: "0x0000000000000000000000000000000000000000".into(),
            to_address: "0xminter".into(),
            event_type: "erc721".into(),
            source: "alchemy".into(),
        }])
    }

    async fn fetch_contract_owners(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
    ) -> Result<Vec<OwnerBalance>, AppError> {
        self.overlap_delay().await;
        Ok(vec![])
    }

    async fn fetch_contract_sales(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        contract_address: &str,
        _opensea_api_key: &str,
    ) -> Result<Vec<NftSaleRecord>, AppError> {
        self.post_signal_overlap_delay().await;
        Ok(vec![NftSaleRecord {
            contract_address: contract_address.to_string(),
            token_id: "1".into(),
            tx_hash: "0xsale".into(),
            block_number: 2,
            log_index: 0,
            bundle_index: 0,
            buyer_address: "0xvictim".into(),
            seller_address: "0xminter".into(),
            marketplace: "opensea".into(),
            taker: "buyer".into(),
            payment_token_symbol: "ETH".into(),
            payment_token_address: "0x0000000000000000000000000000000000000000".into(),
            price_eth: Some(1.0),
            price_usd: Some(1.0),
            seller_fee_eth: 0.0,
            seller_fee_usd: 0.0,
            protocol_fee_eth: 0.0,
            protocol_fee_usd: 0.0,
            royalty_fee_eth: 0.0,
            royalty_fee_usd: 0.0,
            royalty_recipient_address: String::new(),
            source: "opensea".into(),
            is_native_eth: false,
        }])
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
        Ok(vec![])
    }

    async fn fetch_mint_payment_eth_transfers_on_chain(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        block_number: i64,
        address: &str,
    ) -> Result<Vec<EthTransferRecord>, AppError> {
        self.post_signal_overlap_delay().await;
        if block_number == 1 && address == "0xminter" {
            return Ok(vec![EthTransferRecord {
                tx_hash: "0xmint".into(),
                block_number,
                from_address: "0xminter".into(),
                to_address: "0xcreator".into(),
                value_eth: 0.08,
                value_usd: Some(184.0),
                payment_token_symbol: "ETH".into(),
                payment_token_address: ZERO_ADDRESS.into(),
                category: "external".into(),
            }]);
        }
        Ok(vec![])
    }

    async fn fetch_mint_payment_eth_transfers_to_address_on_chain(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        block_number: i64,
        address: &str,
    ) -> Result<Vec<EthTransferRecord>, AppError> {
        self.post_signal_overlap_delay().await;
        if block_number == 1 && address == "0xcreator" {
            return Ok(vec![EthTransferRecord {
                tx_hash: "0xmint".into(),
                block_number,
                from_address: "0xminter".into(),
                to_address: "0xcreator".into(),
                value_eth: 0.08,
                value_usd: Some(184.0),
                payment_token_symbol: "ETH".into(),
                payment_token_address: ZERO_ADDRESS.into(),
                category: "external".into(),
            }]);
        }
        Ok(vec![])
    }
}
