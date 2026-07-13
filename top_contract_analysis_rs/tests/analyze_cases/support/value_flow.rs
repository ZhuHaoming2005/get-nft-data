use super::*;

pub(in crate::analyze_cases) struct DuplicateMintPaymentLookupApi {
    pub(in crate::analyze_cases) mint_transfer_calls: Mutex<Vec<(i64, String)>>,
    pub(in crate::analyze_cases) balance_calls: Mutex<Vec<(String, i64)>>,
    pub(in crate::analyze_cases) block_receipt_calls: Mutex<Vec<i64>>,
}

impl DuplicateMintPaymentLookupApi {
    pub(in crate::analyze_cases) fn new() -> Self {
        Self {
            mint_transfer_calls: Mutex::new(Vec::new()),
            balance_calls: Mutex::new(Vec::new()),
            block_receipt_calls: Mutex::new(Vec::new()),
        }
    }
}

pub(in crate::analyze_cases) const BINANCE_HOT_WALLET: &str =
    "0x28c6c06298d514db089934071355e5743bf21d60";
pub(in crate::analyze_cases) const TORNADO_CASH_1_ETH: &str =
    "0x47ce0c6ed5b0ce3d3a51fdb1c52dc66a7c3c2936";
pub(in crate::analyze_cases) const ARBITRUM_ONE_BRIDGE: &str =
    "0x8315177ab297ba92a06054ce80a67ed4dbd7ed3a";

pub(in crate::analyze_cases) struct CashoutTraceApi {
    pub(in crate::analyze_cases) mint_transfer_calls: Mutex<Vec<(i64, String)>>,
    pub(in crate::analyze_cases) second_level_calls: Mutex<Vec<String>>,
    pub(in crate::analyze_cases) parallel_branches: bool,
    pub(in crate::analyze_cases) ordered_frontier_probe: bool,
    pub(in crate::analyze_cases) active_hop_fetches: AtomicUsize,
    pub(in crate::analyze_cases) max_hop_fetches: AtomicUsize,
}

impl CashoutTraceApi {
    pub(in crate::analyze_cases) fn new() -> Self {
        Self {
            mint_transfer_calls: Mutex::new(Vec::new()),
            second_level_calls: Mutex::new(Vec::new()),
            parallel_branches: false,
            ordered_frontier_probe: false,
            active_hop_fetches: AtomicUsize::new(0),
            max_hop_fetches: AtomicUsize::new(0),
        }
    }

    pub(in crate::analyze_cases) fn with_parallel_branches() -> Self {
        Self {
            parallel_branches: true,
            ..Self::new()
        }
    }

    pub(in crate::analyze_cases) fn with_ordered_frontier_probe() -> Self {
        Self {
            parallel_branches: true,
            ordered_frontier_probe: true,
            ..Self::new()
        }
    }
}

#[async_trait]
impl AnalyzeApi for CashoutTraceApi {
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
            deployed_block_number: 90,
            deployed_block_time: 90,
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
        Ok(vec![TransferRecord {
            contract_address: contract_address.to_string(),
            token_id: "1".into(),
            tx_hash: "0xmint".into(),
            log_index: 0,
            block_number: 10,
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
        tx_hash: &str,
    ) -> Result<TransactionReceiptRecord, AppError> {
        Ok(TransactionReceiptRecord {
            tx_hash: tx_hash.into(),
            block_number: 10,
            transaction_index: 1,
            from_address: "0xminter".into(),
            contract_address: String::new(),
            gas_used: 21_000,
            effective_gas_price_wei: 1_000_000_000,
            fee_native: None,
            fee_usd: None,
        })
    }

    async fn fetch_transaction_receipts_for_block(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        block_number: i64,
    ) -> Result<BTreeMap<String, TransactionReceiptRecord>, AppError> {
        Ok(BTreeMap::from([
            (
                "0xmint".into(),
                TransactionReceiptRecord {
                    tx_hash: "0xmint".into(),
                    block_number,
                    transaction_index: 1,
                    from_address: "0xminter".into(),
                    contract_address: String::new(),
                    gas_used: 21_000,
                    effective_gas_price_wei: 1_000_000_000,
                    fee_native: None,
                    fee_usd: None,
                },
            ),
            (
                "0xbridge".into(),
                TransactionReceiptRecord {
                    tx_hash: "0xbridge".into(),
                    block_number,
                    transaction_index: 2,
                    from_address: "0xhop1".into(),
                    contract_address: String::new(),
                    gas_used: 21_000,
                    effective_gas_price_wei: 1_000_000_000,
                    fee_native: None,
                    fee_usd: None,
                },
            ),
        ]))
    }

    async fn fetch_eth_balance(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _address: &str,
        _block_number: i64,
    ) -> Result<f64, AppError> {
        Ok(1.0)
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
        self.mint_transfer_calls
            .lock()
            .unwrap()
            .push((block_number, address.to_string()));
        if address == "0xnext1" || address == "0xnext2" {
            self.second_level_calls
                .lock()
                .unwrap()
                .push(address.to_string());
        }
        if address == "0xhop1" || address == "0xhop2" {
            let active = self.active_hop_fetches.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_hop_fetches.fetch_max(active, Ordering::SeqCst);
            if self.ordered_frontier_probe && address == "0xhop1" {
                sleep(Duration::from_millis(80)).await;
            } else {
                sleep(Duration::from_millis(40)).await;
            }
            self.active_hop_fetches.fetch_sub(1, Ordering::SeqCst);
        }
        let transfers = match address {
            "0xminter" => vec![EthTransferRecord {
                tx_hash: "0xmint".into(),
                block_number,
                from_address: "0xminter".into(),
                to_address: "0xdup".into(),
                value_eth: 0.08,
                value_usd: Some(184.0),
                payment_token_symbol: "ETH".into(),
                payment_token_address: ZERO_ADDRESS.into(),
                category: "external".into(),
            }],
            "0xdup" => {
                let mut rows = vec![EthTransferRecord {
                    tx_hash: "0xmint".into(),
                    block_number,
                    from_address: "0xdup".into(),
                    to_address: "0xhop1".into(),
                    value_eth: 0.5,
                    value_usd: Some(1_150.0),
                    payment_token_symbol: "ETH".into(),
                    payment_token_address: ZERO_ADDRESS.into(),
                    category: "internal".into(),
                }];
                if self.parallel_branches {
                    rows.push(EthTransferRecord {
                        tx_hash: "0xmint".into(),
                        block_number,
                        from_address: "0xdup".into(),
                        to_address: "0xhop2".into(),
                        value_eth: 0.4,
                        value_usd: Some(920.0),
                        payment_token_symbol: "ETH".into(),
                        payment_token_address: ZERO_ADDRESS.into(),
                        category: "internal".into(),
                    });
                }
                rows.extend([
                    EthTransferRecord {
                        tx_hash: "0xmint".into(),
                        block_number,
                        from_address: "0xdup".into(),
                        to_address: BINANCE_HOT_WALLET.into(),
                        value_eth: 0.2,
                        value_usd: Some(460.0),
                        payment_token_symbol: "ETH".into(),
                        payment_token_address: ZERO_ADDRESS.into(),
                        category: "external".into(),
                    },
                    EthTransferRecord {
                        tx_hash: "0xmint".into(),
                        block_number,
                        from_address: "0xdup".into(),
                        to_address: TORNADO_CASH_1_ETH.into(),
                        value_eth: 0.1,
                        value_usd: Some(230.0),
                        payment_token_symbol: "ETH".into(),
                        payment_token_address: ZERO_ADDRESS.into(),
                        category: "external".into(),
                    },
                ]);
                rows
            }
            "0xhop1" if self.ordered_frontier_probe => vec![EthTransferRecord {
                tx_hash: "0xmint".into(),
                block_number,
                from_address: "0xhop1".into(),
                to_address: "0xnext1".into(),
                value_eth: 0.49,
                value_usd: Some(1_127.0),
                payment_token_symbol: "ETH".into(),
                payment_token_address: ZERO_ADDRESS.into(),
                category: "internal".into(),
            }],
            "0xhop2" if self.ordered_frontier_probe => vec![EthTransferRecord {
                tx_hash: "0xmint".into(),
                block_number,
                from_address: "0xhop2".into(),
                to_address: "0xnext2".into(),
                value_eth: 0.39,
                value_usd: Some(897.0),
                payment_token_symbol: "ETH".into(),
                payment_token_address: ZERO_ADDRESS.into(),
                category: "internal".into(),
            }],
            "0xnext1" => vec![EthTransferRecord {
                tx_hash: "0xmint".into(),
                block_number,
                from_address: "0xnext1".into(),
                to_address: ARBITRUM_ONE_BRIDGE.into(),
                value_eth: 0.48,
                value_usd: Some(1_104.0),
                payment_token_symbol: "ETH".into(),
                payment_token_address: ZERO_ADDRESS.into(),
                category: "external".into(),
            }],
            "0xnext2" => vec![EthTransferRecord {
                tx_hash: "0xmint".into(),
                block_number,
                from_address: "0xnext2".into(),
                to_address: BINANCE_HOT_WALLET.into(),
                value_eth: 0.38,
                value_usd: Some(874.0),
                payment_token_symbol: "ETH".into(),
                payment_token_address: ZERO_ADDRESS.into(),
                category: "external".into(),
            }],
            "0xhop1" => vec![
                EthTransferRecord {
                    tx_hash: "0xbridge".into(),
                    block_number,
                    from_address: "0xhop1".into(),
                    to_address: ARBITRUM_ONE_BRIDGE.into(),
                    value_eth: 0.49,
                    value_usd: Some(1_127.0),
                    payment_token_symbol: "ETH".into(),
                    payment_token_address: ZERO_ADDRESS.into(),
                    category: "external".into(),
                },
                EthTransferRecord {
                    tx_hash: "0xunrelated".into(),
                    block_number,
                    from_address: "0xhop1".into(),
                    to_address: "0xunrelatedrecipient".into(),
                    value_eth: 1.5,
                    value_usd: Some(3_450.0),
                    payment_token_symbol: "ETH".into(),
                    payment_token_address: ZERO_ADDRESS.into(),
                    category: "external".into(),
                },
            ],
            "0xhop2" => vec![EthTransferRecord {
                tx_hash: "0xexit2".into(),
                block_number,
                from_address: "0xhop2".into(),
                to_address: BINANCE_HOT_WALLET.into(),
                value_eth: 0.39,
                value_usd: Some(897.0),
                payment_token_symbol: "ETH".into(),
                payment_token_address: ZERO_ADDRESS.into(),
                category: "external".into(),
            }],
            _ => vec![],
        };
        Ok(transfers)
    }

    async fn fetch_mint_payment_eth_transfers_to_address_on_chain(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        block_number: i64,
        address: &str,
    ) -> Result<Vec<EthTransferRecord>, AppError> {
        self.mint_transfer_calls
            .lock()
            .unwrap()
            .push((block_number, address.to_string()));
        let transfers = match address {
            "0xdup" => vec![EthTransferRecord {
                tx_hash: "0xmint".into(),
                block_number,
                from_address: "0xminter".into(),
                to_address: "0xdup".into(),
                value_eth: 0.08,
                value_usd: Some(184.0),
                payment_token_symbol: "ETH".into(),
                payment_token_address: ZERO_ADDRESS.into(),
                category: "external".into(),
            }],
            _ => vec![],
        };
        Ok(transfers)
    }
}

#[async_trait]
impl AnalyzeApi for DuplicateMintPaymentLookupApi {
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
        Ok(vec![
            SeedNft {
                chain: chain.to_string(),
                contract_address: contract_address.to_string(),
                token_id: "1".into(),
                name: "Azuki #1".into(),
                symbol: "AZUKI".into(),
                token_uri: "ipfs://seed/1".into(),
                image_uri: "ipfs://image/1.png".into(),
                metadata_json: r#"{"name":"Azuki #1","description":"gold dragon"}"#.into(),
            },
            SeedNft {
                chain: chain.to_string(),
                contract_address: contract_address.to_string(),
                token_id: "2".into(),
                name: "Azuki #2".into(),
                symbol: "AZUKI".into(),
                token_uri: "ipfs://seed/2".into(),
                image_uri: "ipfs://image/2.png".into(),
                metadata_json: r#"{"name":"Azuki #2","description":"gold dragon"}"#.into(),
            },
        ])
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
                tx_hash: "0xmint1".into(),
                log_index: 0,
                block_number: 1,
                block_time: 100,
                from_address: ZERO_ADDRESS.into(),
                to_address: "0xminter".into(),
                event_type: "erc721".into(),
                source: "alchemy".into(),
            },
            TransferRecord {
                contract_address: contract_address.to_string(),
                token_id: "2".into(),
                tx_hash: "0xmint2".into(),
                log_index: 1,
                block_number: 1,
                block_time: 101,
                from_address: ZERO_ADDRESS.into(),
                to_address: "0xminter".into(),
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
        tx_hash: &str,
    ) -> Result<TransactionReceiptRecord, AppError> {
        Ok(TransactionReceiptRecord {
            tx_hash: tx_hash.into(),
            block_number: 1,
            transaction_index: 1,
            from_address: "0xminter".into(),
            contract_address: String::new(),
            gas_used: 21000,
            effective_gas_price_wei: 1_000_000_000,
            fee_native: None,
            fee_usd: None,
        })
    }

    async fn fetch_transaction_receipts_for_block(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        block_number: i64,
    ) -> Result<BTreeMap<String, TransactionReceiptRecord>, AppError> {
        self.block_receipt_calls.lock().unwrap().push(block_number);
        Ok(BTreeMap::new())
    }

    async fn fetch_eth_balance(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        address: &str,
        block_number: i64,
    ) -> Result<f64, AppError> {
        self.balance_calls
            .lock()
            .unwrap()
            .push((address.to_string(), block_number));
        Ok(1.0)
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

    async fn fetch_mint_payment_eth_transfers_to_address_on_chain(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        block_number: i64,
        address: &str,
    ) -> Result<Vec<EthTransferRecord>, AppError> {
        self.mint_transfer_calls
            .lock()
            .unwrap()
            .push((block_number, address.to_string()));
        if block_number == 1 && address == "0xcreator" {
            return Ok(vec![
                EthTransferRecord {
                    tx_hash: "0xmint1".into(),
                    block_number,
                    from_address: "0xminter".into(),
                    to_address: "0xcreator".into(),
                    value_eth: 0.08,
                    value_usd: Some(184.0),
                    payment_token_symbol: "ETH".into(),
                    payment_token_address: ZERO_ADDRESS.into(),
                    category: "external".into(),
                },
                EthTransferRecord {
                    tx_hash: "0xmint2".into(),
                    block_number,
                    from_address: "0xminter".into(),
                    to_address: "0xcreator".into(),
                    value_eth: 0.09,
                    value_usd: Some(207.0),
                    payment_token_symbol: "ETH".into(),
                    payment_token_address: ZERO_ADDRESS.into(),
                    category: "external".into(),
                },
            ]);
        }
        Ok(vec![])
    }
}

#[async_trait]
impl AnalyzeApi for ObsoleteReceiptMetricProbeApi {
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
        Ok(vec![
            OwnerBalance {
                owner_address: "0xvictim1".into(),
                token_balances: BTreeMap::from([("1".into(), 1)]),
            },
            OwnerBalance {
                owner_address: "0xvictim2".into(),
                token_balances: BTreeMap::from([("1".into(), 1)]),
            },
        ])
    }

    async fn fetch_contract_sales(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        contract_address: &str,
        _opensea_api_key: &str,
    ) -> Result<Vec<NftSaleRecord>, AppError> {
        let second_tx_hash = if self.duplicate_sale_tx {
            "0xsale1"
        } else {
            "0xsale2"
        };
        let second_buyer = if self.same_buyer_history {
            "0xvictim1"
        } else {
            "0xvictim2"
        };
        Ok(vec![
            NftSaleRecord {
                contract_address: contract_address.to_string(),
                token_id: "1".into(),
                tx_hash: "0xsale1".into(),
                block_number: 2,
                log_index: 0,
                bundle_index: 0,
                buyer_address: "0xvictim1".into(),
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
                is_native_eth: true,
            },
            NftSaleRecord {
                contract_address: contract_address.to_string(),
                token_id: "1".into(),
                tx_hash: second_tx_hash.into(),
                block_number: 3,
                log_index: 0,
                bundle_index: 0,
                buyer_address: second_buyer.into(),
                seller_address: "0xminter".into(),
                marketplace: "opensea".into(),
                taker: "buyer".into(),
                payment_token_symbol: "ETH".into(),
                payment_token_address: "0x0000000000000000000000000000000000000000".into(),
                price_eth: Some(2.0),
                price_usd: Some(2.0),
                seller_fee_eth: 0.0,
                seller_fee_usd: 0.0,
                protocol_fee_eth: 0.0,
                protocol_fee_usd: 0.0,
                royalty_fee_eth: 0.0,
                royalty_fee_usd: 0.0,
                royalty_recipient_address: String::new(),
                source: "opensea".into(),
                is_native_eth: true,
            },
        ])
    }

    async fn fetch_transaction_receipt(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        tx_hash: &str,
    ) -> Result<TransactionReceiptRecord, AppError> {
        self.receipt_calls.fetch_add(1, Ordering::SeqCst);
        let active = self.active_receipts.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_receipts.fetch_max(active, Ordering::SeqCst);
        sleep(Duration::from_millis(40)).await;
        self.active_receipts.fetch_sub(1, Ordering::SeqCst);

        Ok(TransactionReceiptRecord {
            tx_hash: tx_hash.into(),
            block_number: 2,
            transaction_index: 1,
            from_address: "0xvictim".into(),
            contract_address: String::new(),
            gas_used: 21000,
            effective_gas_price_wei: 1_000_000_000,
            fee_native: None,
            fee_usd: None,
        })
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
        self.balance_calls.fetch_add(1, Ordering::SeqCst);
        Ok(5.0)
    }

    async fn fetch_same_block_eth_transfers_for_address(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _block_number: i64,
        _address: &str,
    ) -> Result<Vec<EthTransferRecord>, AppError> {
        self.same_block_transfer_calls
            .fetch_add(1, Ordering::SeqCst);
        Ok(vec![])
    }
}
