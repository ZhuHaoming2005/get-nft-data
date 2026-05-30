use super::*;

#[tokio::test]
async fn analyze_deduplicates_mint_value_flow_transfer_lookups_by_block_and_address() {
    let api = Arc::new(DuplicateMintPaymentLookupApi::new());
    let deps = AnalysisDeps {
        api: api.clone(),
        feature_store: Arc::new(FakeFeatureStore {
            snapshot: DatabaseSnapshot {
                nft_rows: vec![
                    DatabaseNftRecord {
                        contract_address: "0xdup".into(),
                        token_id: "1".into(),
                        token_uri: "ipfs://seed/1".into(),
                        image_uri: "ipfs://image/1.png".into(),
                        name: "Azuki Mirror #1".into(),
                        symbol: "AZUKI".into(),
                        metadata_json: r#"{"description":"gold dragon"}"#.into(),
                        metadata_recall_checked: false,
                        metadata_recall_match: false,
                    },
                    DatabaseNftRecord {
                        contract_address: "0xdup".into(),
                        token_id: "2".into(),
                        token_uri: "ipfs://seed/2".into(),
                        image_uri: "ipfs://image/2.png".into(),
                        name: "Azuki Mirror #2".into(),
                        symbol: "AZUKI".into(),
                        metadata_json: r#"{"description":"gold dragon"}"#.into(),
                        metadata_recall_checked: false,
                        metadata_recall_match: false,
                    },
                ],
                ..DatabaseSnapshot::default()
            },
        }),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let payload = analyze_seed_contract(
        AnalyzeRequest {
            chain: "ethereum".into(),
            seed_contract_address: "0xseed".into(),
            alchemy_api_key: "key".into(),
            api_max_concurrency: 3,
            ..AnalyzeRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(
        payload
            .value_flow_edges
            .iter()
            .filter(|edge| edge.channel == "mint_payment")
            .count(),
        2
    );
    let calls = api.mint_transfer_calls.lock().unwrap().clone();
    assert_eq!(
        calls,
        vec![(1, "0xcreator".to_string()), (1, "0xdup".to_string()),],
        "expected mint value-flow to fetch each receiver once per block without minter lookups"
    );
    assert_eq!(
        api.balance_calls.lock().unwrap().clone(),
        vec![("0xminter".to_string(), 0)],
        "expected duplicate minter/block balance enrichment to be fetched once"
    );
    assert_eq!(
        api.block_receipt_calls.lock().unwrap().clone(),
        vec![1, 123],
        "expected duplicate mint block receipt enrichment to be fetched once, plus deployment gas lookup"
    );
}

#[tokio::test]
async fn analyze_traces_multi_hop_cashout_and_classifies_known_destinations() {
    let api = Arc::new(CashoutTraceApi::new());
    let deps = AnalysisDeps {
        api: api.clone(),
        feature_store: Arc::new(FakeFeatureStore {
            snapshot: DatabaseSnapshot {
                nft_rows: vec![DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "1".into(),
                    token_uri: "ipfs://seed/1".into(),
                    image_uri: "ipfs://image/1.png".into(),
                    name: "Azuki Mirror #1".into(),
                    symbol: "AZUKI".into(),
                    metadata_json: r#"{"description":"gold dragon"}"#.into(),
                    metadata_recall_checked: false,
                    metadata_recall_match: false,
                }],
                ..DatabaseSnapshot::default()
            },
        }),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let payload = analyze_seed_contract(
        AnalyzeRequest {
            chain: "ethereum".into(),
            seed_contract_address: "0xseed".into(),
            alchemy_api_key: "key".into(),
            api_max_concurrency: 3,
            ..AnalyzeRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    let calls = api.mint_transfer_calls.lock().unwrap().clone();
    assert!(
        calls.contains(&(10, "0xhop1".to_string())),
        "expected withdrawal tracing to fetch the intermediate cashout wallet"
    );

    let bridge_hop = payload
        .value_flow_edges
        .iter()
        .find(|edge| {
            edge.channel == "cashout_hop"
                && edge.to_address.eq_ignore_ascii_case(ARBITRUM_ONE_BRIDGE)
        })
        .expect("multi-hop cashout edge to bridge");
    assert_eq!(bridge_hop.from_address, "0xhop1");
    assert_eq!(bridge_hop.to_role, "bridge");
    assert!(bridge_hop
        .evidence_flags
        .contains(&"multi_hop_cashout".to_string()));
    assert!(bridge_hop
        .evidence_flags
        .contains(&"value_constrained_cashout".to_string()));
    assert!(bridge_hop
        .evidence_flags
        .contains(&"cashout_destination:bridge".to_string()));
    assert!(!payload.value_flow_edges.iter().any(|edge| {
        edge.channel == "cashout_hop"
            && edge.to_address.eq_ignore_ascii_case("0xunrelatedrecipient")
    }));

    let cex_withdrawal = payload
        .value_flow_edges
        .iter()
        .find(|edge| {
            edge.channel == "withdrawal" && edge.to_address.eq_ignore_ascii_case(BINANCE_HOT_WALLET)
        })
        .expect("direct withdrawal to known CEX");
    assert_eq!(cex_withdrawal.to_role, "cex");
    assert_eq!(cex_withdrawal.gas_payer_address, "0xminter");
    assert_eq!(cex_withdrawal.gas_eth, Some(0.000021));
    assert!(
        (cex_withdrawal.gas_usd.unwrap() - 0.0483).abs() < 0.0000001,
        "expected withdrawal gas USD to be derived from receipt gas and transfer ETH/USD rate"
    );
    assert!(cex_withdrawal
        .evidence_flags
        .contains(&"cashout_destination:cex".to_string()));

    let mixer_withdrawal = payload
        .value_flow_edges
        .iter()
        .find(|edge| {
            edge.channel == "withdrawal" && edge.to_address.eq_ignore_ascii_case(TORNADO_CASH_1_ETH)
        })
        .expect("direct withdrawal to known mixer");
    assert_eq!(mixer_withdrawal.to_role, "mixer");
    assert!(mixer_withdrawal
        .evidence_flags
        .contains(&"cashout_destination:mixer".to_string()));

    assert!(payload.contract_lifecycle_events.is_empty());
    assert!(payload.lifecycle_metrics.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn analyze_fetches_cashout_trace_frontier_in_parallel() {
    let api = Arc::new(CashoutTraceApi::with_parallel_branches());
    let deps = AnalysisDeps {
        api: api.clone(),
        feature_store: Arc::new(FakeFeatureStore {
            snapshot: DatabaseSnapshot {
                nft_rows: vec![DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "1".into(),
                    token_uri: "ipfs://seed/1".into(),
                    image_uri: "ipfs://image/1.png".into(),
                    name: "Azuki Mirror #1".into(),
                    symbol: "AZUKI".into(),
                    metadata_json: r#"{"description":"gold dragon"}"#.into(),
                    metadata_recall_checked: false,
                    metadata_recall_match: false,
                }],
                ..DatabaseSnapshot::default()
            },
        }),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let payload = analyze_seed_contract(
        AnalyzeRequest {
            chain: "ethereum".into(),
            seed_contract_address: "0xseed".into(),
            alchemy_api_key: "key".into(),
            api_max_concurrency: 2,
            ..AnalyzeRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert!(payload
        .value_flow_edges
        .iter()
        .any(|edge| edge.channel == "cashout_hop"));
    assert!(
        api.max_hop_fetches.load(Ordering::SeqCst) >= 2,
        "expected same-depth cashout frontier wallets to be fetched concurrently"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn analyze_processes_parallel_cashout_frontier_in_queue_order() {
    let api = Arc::new(CashoutTraceApi::with_ordered_frontier_probe());
    let deps = AnalysisDeps {
        api: api.clone(),
        feature_store: Arc::new(FakeFeatureStore {
            snapshot: DatabaseSnapshot {
                nft_rows: vec![DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "1".into(),
                    token_uri: "ipfs://seed/1".into(),
                    image_uri: "ipfs://image/1.png".into(),
                    name: "Azuki Mirror #1".into(),
                    symbol: "AZUKI".into(),
                    metadata_json: r#"{"description":"gold dragon"}"#.into(),
                    metadata_recall_checked: false,
                    metadata_recall_match: false,
                }],
                ..DatabaseSnapshot::default()
            },
        }),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let _payload = analyze_seed_contract(
        AnalyzeRequest {
            chain: "ethereum".into(),
            seed_contract_address: "0xseed".into(),
            alchemy_api_key: "key".into(),
            api_max_concurrency: 2,
            ..AnalyzeRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(
        api.second_level_calls.lock().unwrap().as_slice(),
        ["0xnext1", "0xnext2"],
        "cashout tracing should fetch frontier nodes concurrently but enqueue their children in the original queue order"
    );
}

#[tokio::test]
async fn analyze_counts_deployment_and_malicious_sale_gas_in_attacker_cost() {
    let deps = AnalysisDeps {
        api: Arc::new(AttackerCostGasApi),
        feature_store: Arc::new(FakeFeatureStore {
            snapshot: DatabaseSnapshot {
                nft_rows: vec![DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "1".into(),
                    token_uri: "ipfs://seed/1".into(),
                    image_uri: "ipfs://image/1.png".into(),
                    name: "Azuki Mirror #1".into(),
                    symbol: "AZUKI".into(),
                    metadata_json: r#"{"description":"gold dragon"}"#.into(),
                    metadata_recall_checked: false,
                    metadata_recall_match: false,
                }],
                ..DatabaseSnapshot::default()
            },
        }),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let payload = analyze_seed_contract(
        AnalyzeRequest {
            chain: "ethereum".into(),
            seed_contract_address: "0xseed".into(),
            alchemy_api_key: "key".into(),
            api_max_concurrency: 3,
            ..AnalyzeRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    let deploy_edge = payload
        .value_flow_edges
        .iter()
        .find(|edge| edge.channel == "contract_deploy")
        .expect("deployment gas edge");
    assert_eq!(deploy_edge.gas_payer_address, "0xdeployer");
    assert_eq!(deploy_edge.gas_eth, Some(0.000042));
    assert_eq!(deploy_edge.gas_usd, Some(0.0924));

    let lure_edge = payload
        .value_flow_edges
        .iter()
        .find(|edge| edge.channel == "lure_payment")
        .expect("malicious wash sale gas edge");
    assert_eq!(lure_edge.gas_payer_address, "0xattacker");
    assert_eq!(lure_edge.gas_eth, Some(0.000021));

    let exit_edge = payload
        .value_flow_edges
        .iter()
        .find(|edge| edge.channel == "exit_payment")
        .expect("malicious seller exit sale gas edge");
    assert_eq!(exit_edge.gas_payer_address, "0xattacker");
    assert_eq!(exit_edge.gas_eth, Some(0.000021));

    let cost = &payload.paper_stats.attacker_cost;
    assert_eq!(cost.setup_gas_eth, 0.000042);
    assert_eq!(cost.lure_gas_eth, 0.000021);
    assert_eq!(cost.exit_gas_eth, 0.000021);
    assert_eq!(cost.total_gas_eth, 0.000084);
}

#[tokio::test]
async fn analyze_counts_deployment_gas_when_deployment_time_is_missing() {
    let deps = AnalysisDeps {
        api: Arc::new(MissingDeploymentTimeAttackerCostApi),
        feature_store: Arc::new(FakeFeatureStore {
            snapshot: DatabaseSnapshot {
                nft_rows: vec![DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "1".into(),
                    token_uri: "ipfs://seed/1".into(),
                    image_uri: "ipfs://image/1.png".into(),
                    name: "Azuki Mirror #1".into(),
                    symbol: "AZUKI".into(),
                    metadata_json: r#"{"description":"gold dragon"}"#.into(),
                    metadata_recall_checked: false,
                    metadata_recall_match: false,
                }],
                ..DatabaseSnapshot::default()
            },
        }),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let payload = analyze_seed_contract(
        AnalyzeRequest {
            chain: "ethereum".into(),
            seed_contract_address: "0xseed".into(),
            alchemy_api_key: "key".into(),
            api_max_concurrency: 3,
            ..AnalyzeRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    let deploy_edge = payload
        .value_flow_edges
        .iter()
        .find(|edge| edge.channel == "contract_deploy")
        .expect("deployment gas edge should not require deployment block time");
    assert_eq!(deploy_edge.block_time, 0);
    assert_eq!(deploy_edge.gas_eth, Some(0.000042));
    assert_eq!(deploy_edge.gas_usd, Some(0.0924));
    assert_eq!(payload.paper_stats.attacker_cost.setup_gas_eth, 0.000042);
    assert_eq!(payload.paper_stats.attacker_cost.setup_gas_usd, 0.0924);
}

struct AttackerCostGasApi;

struct MissingDeploymentTimeAttackerCostApi;

#[async_trait]
impl AnalyzeApi for MissingDeploymentTimeAttackerCostApi {
    async fn fetch_contract_metadata(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        opensea_api_key: &str,
        contract_address: &str,
    ) -> Result<ContractMetadata, AppError> {
        let mut metadata = AttackerCostGasApi
            .fetch_contract_metadata(
                chain,
                alchemy_api_key,
                alchemy_network,
                opensea_api_key,
                contract_address,
            )
            .await?;
        if contract_address == "0xdup" {
            metadata.deployed_block_time = 0;
        }
        Ok(metadata)
    }

    async fn fetch_seed_contract_nfts(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        contract_address: &str,
    ) -> Result<Vec<SeedNft>, AppError> {
        AttackerCostGasApi
            .fetch_seed_contract_nfts(chain, alchemy_api_key, alchemy_network, contract_address)
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
        AttackerCostGasApi
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
        AttackerCostGasApi
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
        AttackerCostGasApi
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
        AttackerCostGasApi
            .fetch_transaction_receipt(alchemy_api_key, alchemy_network, tx_hash)
            .await
    }

    async fn fetch_transaction_receipts_for_block(
        &self,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        block_number: i64,
    ) -> Result<BTreeMap<String, TransactionReceiptRecord>, AppError> {
        AttackerCostGasApi
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
        AttackerCostGasApi
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
        AttackerCostGasApi
            .fetch_same_block_eth_transfers_for_address(
                alchemy_api_key,
                alchemy_network,
                block_number,
                address,
            )
            .await
    }
}

#[async_trait]
impl AnalyzeApi for AttackerCostGasApi {
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
            contract_deployer: if contract_address == "0xdup" {
                "0xdeployer".into()
            } else {
                "0xseeddeployer".into()
            },
            deployed_block_number: if contract_address == "0xdup" { 5 } else { 1 },
            deployed_block_time: if contract_address == "0xdup" { 50 } else { 10 },
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
        Ok(vec![
            TransferRecord {
                contract_address: contract_address.to_string(),
                token_id: "1".into(),
                tx_hash: "0xmint".into(),
                log_index: 0,
                block_number: 10,
                block_time: 100,
                from_address: ZERO_ADDRESS.into(),
                to_address: "0xattacker".into(),
                event_type: "erc721".into(),
                source: "alchemy".into(),
            },
            TransferRecord {
                contract_address: contract_address.to_string(),
                token_id: "1".into(),
                tx_hash: "0xsale".into(),
                log_index: 1,
                block_number: 12,
                block_time: 110,
                from_address: "0xattacker".into(),
                to_address: "0xvictim".into(),
                event_type: "erc721".into(),
                source: "alchemy".into(),
            },
            TransferRecord {
                contract_address: contract_address.to_string(),
                token_id: "1".into(),
                tx_hash: "0xmove2".into(),
                log_index: 2,
                block_number: 13,
                block_time: 111,
                from_address: "0xattacker".into(),
                to_address: "0xholder2".into(),
                event_type: "erc721".into(),
                source: "alchemy".into(),
            },
            TransferRecord {
                contract_address: contract_address.to_string(),
                token_id: "1".into(),
                tx_hash: "0xmove3".into(),
                log_index: 3,
                block_number: 14,
                block_time: 112,
                from_address: "0xattacker".into(),
                to_address: "0xholder3".into(),
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
            owner_address: "0xvictim".into(),
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
        Ok(vec![
            NftSaleRecord {
                contract_address: contract_address.to_string(),
                token_id: "1".into(),
                tx_hash: "0xwash".into(),
                block_number: 11,
                log_index: 0,
                bundle_index: 0,
                buyer_address: "0xattacker".into(),
                seller_address: "0xpeer".into(),
                marketplace: "opensea".into(),
                taker: "buyer".into(),
                payment_token_symbol: "ETH".into(),
                payment_token_address: ZERO_ADDRESS.into(),
                price_eth: Some(0.1),
                price_usd: Some(230.0),
                seller_fee_eth: 0.1,
                seller_fee_usd: 230.0,
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
                tx_hash: "0xsale".into(),
                block_number: 12,
                log_index: 1,
                bundle_index: 0,
                buyer_address: "0xvictim".into(),
                seller_address: "0xattacker".into(),
                marketplace: "opensea".into(),
                taker: "seller".into(),
                payment_token_symbol: "ETH".into(),
                payment_token_address: ZERO_ADDRESS.into(),
                price_eth: Some(1.0),
                price_usd: Some(2300.0),
                seller_fee_eth: 1.0,
                seller_fee_usd: 2300.0,
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
        Ok(TransactionReceiptRecord {
            tx_hash: tx_hash.into(),
            block_number: 12,
            transaction_index: 1,
            from_address: "0xattacker".into(),
            contract_address: String::new(),
            gas_used: 21_000,
            effective_gas_price_wei: 1_000_000_000,
        })
    }

    async fn fetch_transaction_receipts_for_block(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        block_number: i64,
    ) -> Result<BTreeMap<String, TransactionReceiptRecord>, AppError> {
        if block_number != 5 {
            return Ok(BTreeMap::new());
        }
        Ok(BTreeMap::from([(
            "0xdeploy".into(),
            TransactionReceiptRecord {
                tx_hash: "0xdeploy".into(),
                block_number,
                transaction_index: 0,
                from_address: "0xdeployer".into(),
                contract_address: "0xdup".into(),
                gas_used: 42_000,
                effective_gas_price_wei: 1_000_000_000,
            },
        )]))
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
