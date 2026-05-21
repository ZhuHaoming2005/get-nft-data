use super::*;

#[tokio::test]
async fn analyze_builds_address_profiles_and_trade_stats_for_duplicate_contracts() {
    let deps = AnalysisDeps {
        api: Arc::new(CountingApi::new()),
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
            ..AnalyzeRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(payload.malicious_addresses.len(), 1);
    assert_eq!(payload.malicious_addresses[0].address, "0xminter");
    assert_eq!(payload.honest_addresses.len(), 2);
    assert_eq!(payload.secondary_sale_victim_addresses.len(), 1);
    assert_eq!(
        payload.secondary_sale_victim_addresses[0].address,
        "0xvictim"
    );
    assert_eq!(
        payload.secondary_sale_victim_addresses[0].buy_amount_eth,
        1.5
    );
    assert!(!payload.secondary_sale_victim_addresses[0].is_stuck);
    assert_eq!(
        payload.honest_address_stats["0xdup"].honest_address_count,
        2
    );
    assert_eq!(
        payload.honest_address_stats["0xdup"].corrupted_address_count,
        0
    );
    assert_eq!(payload.honest_address_stats["0xdup"].victim_resale_count, 0);
    assert_eq!(
        payload.honest_address_stats["0xdup"].avg_deployment_to_neutral_holder_seconds,
        None
    );
    assert_eq!(payload.fraud_trade_stats["0xdup"].unique_buyers, 1);
    assert_eq!(
        payload.fraud_trade_stats["0xdup"].eth_priced_sale_count,
        Some(1)
    );
    assert_eq!(
        payload.fraud_trade_stats["0xdup"].eth_priced_volume,
        Some(1.5)
    );
    assert_eq!(payload.fraud_trade_stats["0xdup"].stuck_wallet_count, 0);
    assert_eq!(payload.report_summary.malicious_address_count, 1);
    let neutral_address_count = payload
        .address_attributions
        .iter()
        .filter(|item| item.attribution_label == "neutral_participant")
        .map(|item| item.address.as_str())
        .collect::<BTreeSet<_>>()
        .len() as i64;
    assert_eq!(
        payload.report_summary.neutral_address_count,
        neutral_address_count
    );
    assert_eq!(payload.report_summary.secondary_sale_victim_cost_eth, 1.5);
    assert_eq!(payload.report_summary.corrupted_victim_address_count, 0);

    let propagation = &payload.nft_propagation_paths["0xdup"];
    assert_eq!(propagation.summary.token_count, 1);
    assert_eq!(propagation.summary.mint_edge_count, 1);
    assert_eq!(propagation.summary.sale_edge_count, 1);
    assert_eq!(propagation.summary.malicious_node_count, 1);
    assert_eq!(propagation.summary.victim_node_count, 1);
    assert!(propagation.edges.iter().any(|edge| {
        edge.channel == "mint" && edge.to_address == "0xminter" && edge.token_id == "1"
    }));
    assert!(propagation.edges.iter().any(|edge| {
        edge.channel == "sale"
            && edge.from_address == "0xminter"
            && edge.to_address == "0xvictim"
            && edge.price_eth == Some(1.5)
    }));
    assert!(propagation.nodes["0xminter"]
        .roles
        .contains(&"malicious".to_string()));
    assert!(propagation.nodes["0xvictim"]
        .roles
        .contains(&"victim_buyer".to_string()));
}

#[tokio::test]
async fn analyze_fetches_transfers_and_owners_on_each_run() {
    let api = Arc::new(CountingApi::new());
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
    let request = AnalyzeRequest {
        chain: "ethereum".into(),
        seed_contract_address: "0xseed".into(),
        alchemy_api_key: "key".into(),
        ..AnalyzeRequest::default()
    };

    let first = analyze_seed_contract(request.clone(), &deps).await.unwrap();
    let second = analyze_seed_contract(request, &deps).await.unwrap();

    assert_eq!(api.transfer_fetch_count.load(Ordering::SeqCst), 2);
    assert_eq!(api.owner_fetch_count.load(Ordering::SeqCst), 2);
    assert_eq!(
        first.address_signals["0xdup"],
        second.address_signals["0xdup"]
    );
    assert_eq!(
        first.victim_signals["0xdup"],
        second.victim_signals["0xdup"]
    );
}

#[tokio::test]
async fn analyze_computes_native_eth_sale_metrics_for_secondary_sale_victim_addresses() {
    let deps = AnalysisDeps {
        api: Arc::new(FakeSaleMetricApi),
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
            ..AnalyzeRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(payload.secondary_sale_victim_addresses.len(), 1);
    assert_eq!(
        payload.secondary_sale_victim_addresses[0].address,
        "0xvictim"
    );
    assert_eq!(
        payload.secondary_sale_victim_addresses[0].buy_before_eth_balance,
        Some(3.0)
    );
    assert_eq!(
        payload.secondary_sale_victim_addresses[0].buy_asset_ratio,
        Some(0.5)
    );
    assert!(
        payload.secondary_sale_victim_addresses[0]
            .buy_asset_ratio_with_gas
            .unwrap()
            > 0.5
    );
    assert_eq!(
        payload.secondary_sale_victim_addresses[0].ratio_status,
        "ok"
    );
    assert_eq!(
        payload.report_summary.buy_asset_ratio_known_address_count,
        1
    );
    assert_eq!(payload.report_summary.ratio_over_60_address_count, 0);
}

#[tokio::test]
async fn analyze_sale_metrics_are_keyed_by_transaction_and_buyer() {
    let deps = AnalysisDeps {
        api: Arc::new(MultiBuyerSameTxSaleMetricApi),
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
            ..AnalyzeRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    let buyer1 = payload
        .secondary_sale_victim_addresses
        .iter()
        .find(|row| row.address == "0xbuyer1")
        .expect("buyer1 victim row");
    let buyer2 = payload
        .secondary_sale_victim_addresses
        .iter()
        .find(|row| row.address == "0xbuyer2")
        .expect("buyer2 victim row");

    assert_eq!(buyer1.buy_before_eth_balance, Some(1.0));
    assert_eq!(buyer1.buy_asset_ratio, Some(1.0));
    assert_eq!(buyer2.buy_before_eth_balance, Some(10.0));
    assert_eq!(buyer2.buy_asset_ratio, Some(0.2));
}
