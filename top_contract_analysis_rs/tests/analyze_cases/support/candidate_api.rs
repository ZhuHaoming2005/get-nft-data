use super::*;

pub(in crate::analyze_cases) struct FakeEnrichedApi;

#[async_trait]
impl AnalyzeApi for FakeEnrichedApi {
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
            owner_address: "0xowner".into(),
            admin_address: "0xadmin".into(),
            proxy_admin_address: "0xproxyadmin".into(),
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
        Ok(vec![
            TransferRecord {
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
            },
            TransferRecord {
                contract_address: contract_address.to_string(),
                token_id: "1".into(),
                tx_hash: "0xmove".into(),
                log_index: 1,
                block_number: 2,
                block_time: 120,
                from_address: "0xminter".into(),
                to_address: "0xholder".into(),
                event_type: "erc721".into(),
                source: "alchemy".into(),
            },
            TransferRecord {
                contract_address: contract_address.to_string(),
                token_id: "1".into(),
                tx_hash: "0xsale".into(),
                log_index: 2,
                block_number: 2,
                block_time: 150,
                from_address: "0xminter".into(),
                to_address: "0xvictim".into(),
                event_type: "erc721".into(),
                source: "alchemy".into(),
            },
        ])
    }

    async fn fetch_contract_owners(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
    ) -> Result<Vec<OwnerBalance>, AppError> {
        Ok(vec![OwnerBalance {
            owner_address: "0xholder".into(),
            token_balances: BTreeMap::from([("1".into(), 1)]),
        }])
    }

    async fn fetch_contract_sales(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        contract_address: &str,
        _opensea_api_key: &str,
    ) -> Result<Vec<NftSaleRecord>, AppError> {
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
            price_eth: Some(1.5),
            price_usd: Some(1.5),
            seller_fee_eth: 0.0,
            seller_fee_usd: 0.0,
            protocol_fee_eth: 0.0,
            protocol_fee_usd: 0.0,
            royalty_fee_eth: 0.0,
            royalty_fee_usd: 0.0,
            royalty_recipient_address: String::new(),
            source: "opensea".into(),
            is_native_eth: true,
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
        if block_number == 1 && address == "0xminter" {
            return Ok(vec![
                EthTransferRecord {
                    tx_hash: "0xmint".into(),
                    block_number,
                    from_address: "0xfunder".into(),
                    to_address: "0xminter".into(),
                    value_eth: 0.08,
                    value_usd: Some(184.0),
                    payment_token_symbol: "ETH".into(),
                    payment_token_address: ZERO_ADDRESS.into(),
                    category: "external".into(),
                },
                EthTransferRecord {
                    tx_hash: "0xmint".into(),
                    block_number,
                    from_address: "0xminter".into(),
                    to_address: "0xcreator".into(),
                    value_eth: 0.08,
                    value_usd: Some(184.0),
                    payment_token_symbol: "ETH".into(),
                    payment_token_address: ZERO_ADDRESS.into(),
                    category: "external".into(),
                },
            ]);
        }
        if block_number == 1 && address == "0xdup" {
            return Ok(vec![EthTransferRecord {
                tx_hash: "0xmint".into(),
                block_number,
                from_address: "0xdup".into(),
                to_address: "0xfunder".into(),
                value_eth: 0.04,
                value_usd: Some(92.0),
                payment_token_symbol: "ETH".into(),
                payment_token_address: ZERO_ADDRESS.into(),
                category: "internal".into(),
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

pub(in crate::analyze_cases) struct PreSeedDeploymentApi {
    pub(in crate::analyze_cases) contract_nft_calls: AtomicUsize,
    pub(in crate::analyze_cases) total_supply_calls: AtomicUsize,
    pub(in crate::analyze_cases) transfer_calls: AtomicUsize,
    pub(in crate::analyze_cases) owner_calls: AtomicUsize,
    pub(in crate::analyze_cases) sale_calls: AtomicUsize,
}

#[async_trait]
impl AnalyzeApi for PreSeedDeploymentApi {
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
                200
            } else {
                100
            },
            deployed_block_time: 0,
            owner_address: "0xowner".into(),
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
            metadata_json: r#"{"name":"Azuki #1"}"#.into(),
        }])
    }

    async fn fetch_contract_nfts(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _etherscan_api_key: &str,
        _opensea_api_key: &str,
        _contract_address: &str,
    ) -> Result<Vec<SeedNft>, AppError> {
        self.contract_nft_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Vec::new())
    }

    async fn fetch_contract_total_supply(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
    ) -> Result<Option<u64>, AppError> {
        self.total_supply_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Some(2))
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
        if contract_address != "0xseed" {
            self.transfer_calls.fetch_add(1, Ordering::SeqCst);
        }
        Ok(Vec::new())
    }

    async fn fetch_contract_owners(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
    ) -> Result<Vec<OwnerBalance>, AppError> {
        self.owner_calls.fetch_add(1, Ordering::SeqCst);
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
        self.sale_calls.fetch_add(1, Ordering::SeqCst);
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
}

pub(in crate::analyze_cases) struct SupplyMismatchApi {
    pub(in crate::analyze_cases) current_total_supply: u64,
    pub(in crate::analyze_cases) contract_nft_calls: AtomicUsize,
    pub(in crate::analyze_cases) total_supply_calls: AtomicUsize,
    pub(in crate::analyze_cases) transfer_calls: AtomicUsize,
    pub(in crate::analyze_cases) owner_calls: AtomicUsize,
    pub(in crate::analyze_cases) sale_calls: AtomicUsize,
}

#[async_trait]
impl AnalyzeApi for SupplyMismatchApi {
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
            owner_address: "0xowner".into(),
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
            metadata_json: r#"{"name":"Azuki #1"}"#.into(),
        }])
    }

    async fn fetch_contract_nfts(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _etherscan_api_key: &str,
        _opensea_api_key: &str,
        _contract_address: &str,
    ) -> Result<Vec<SeedNft>, AppError> {
        self.contract_nft_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Vec::new())
    }

    async fn fetch_contract_total_supply(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
    ) -> Result<Option<u64>, AppError> {
        self.total_supply_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Some(self.current_total_supply))
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
        if contract_address != "0xseed" {
            self.transfer_calls.fetch_add(1, Ordering::SeqCst);
        }
        Ok(Vec::new())
    }

    async fn fetch_contract_owners(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
    ) -> Result<Vec<OwnerBalance>, AppError> {
        self.owner_calls.fetch_add(1, Ordering::SeqCst);
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
        self.sale_calls.fetch_add(1, Ordering::SeqCst);
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
}

#[derive(Default)]
pub(in crate::analyze_cases) struct FakeLegitApi {
    pub(in crate::analyze_cases) sales_calls: AtomicUsize,
}

#[async_trait]
impl AnalyzeApi for FakeLegitApi {
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
            contract_deployer: "0xminter".into(),
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
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        contract_address: &str,
    ) -> Result<Vec<SeedNft>, AppError> {
        AnalyzeApi::fetch_seed_contract_nfts(
            &FakeEnrichedApi,
            chain,
            alchemy_api_key,
            alchemy_network,
            contract_address,
        )
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
        AnalyzeApi::fetch_contract_transfers(
            &FakeEnrichedApi,
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
        AnalyzeApi::fetch_contract_owners(
            &FakeEnrichedApi,
            chain,
            alchemy_api_key,
            alchemy_network,
            contract_address,
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
        self.sales_calls.fetch_add(1, Ordering::SeqCst);
        AnalyzeApi::fetch_contract_sales(
            &FakeEnrichedApi,
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
        AnalyzeApi::fetch_transaction_receipt(
            &FakeEnrichedApi,
            alchemy_api_key,
            alchemy_network,
            tx_hash,
        )
        .await
    }

    async fn fetch_transaction_receipts_for_block(
        &self,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        block_number: i64,
    ) -> Result<BTreeMap<String, TransactionReceiptRecord>, AppError> {
        AnalyzeApi::fetch_transaction_receipts_for_block(
            &FakeEnrichedApi,
            alchemy_api_key,
            alchemy_network,
            block_number,
        )
        .await
    }

    async fn fetch_eth_balance(
        &self,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        address: &str,
        block_number: i64,
    ) -> Result<f64, AppError> {
        AnalyzeApi::fetch_eth_balance(
            &FakeEnrichedApi,
            alchemy_api_key,
            alchemy_network,
            address,
            block_number,
        )
        .await
    }

    async fn fetch_same_block_eth_transfers_for_address(
        &self,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        block_number: i64,
        address: &str,
    ) -> Result<Vec<EthTransferRecord>, AppError> {
        AnalyzeApi::fetch_same_block_eth_transfers_for_address(
            &FakeEnrichedApi,
            alchemy_api_key,
            alchemy_network,
            block_number,
            address,
        )
        .await
    }
}
