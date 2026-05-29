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
    let neutral_address_count = payload
        .address_attributions
        .iter()
        .filter(|item| item.attribution_label == "neutral_participant")
        .map(|item| item.address.as_str())
        .collect::<BTreeSet<_>>()
        .len() as i64;
    assert_eq!(neutral_address_count, 1);
    assert_eq!(
        payload
            .secondary_sale_victim_addresses
            .iter()
            .map(|item| item.buy_amount_eth)
            .sum::<f64>(),
        1.5
    );
    assert_eq!(
        payload
            .honest_addresses
            .iter()
            .filter(|item| item.is_corrupted_address)
            .count(),
        0
    );

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
async fn analyze_records_secondary_sale_victims_without_balance_metric_fetches() {
    let deps = AnalysisDeps {
        api: Arc::new(SecondaryVictimApi),
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
    let buy_ratio_values: Vec<f64> = payload
        .victim_acquisition_addresses
        .iter()
        .filter_map(|item| item.buy_asset_ratio)
        .collect();
    assert!(buy_ratio_values.is_empty());
    assert_eq!(
        buy_ratio_values
            .iter()
            .filter(|value| **value > 0.6)
            .count(),
        0
    );
}

#[tokio::test]
async fn analyze_groups_secondary_sale_victims_by_buyer() {
    let deps = AnalysisDeps {
        api: Arc::new(MultiBuyerSameTxApi),
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

    assert_eq!(buyer1.buy_tx_hashes, vec!["0xbundle"]);
    assert_eq!(buyer2.buy_tx_hashes, vec!["0xbundle"]);
}
