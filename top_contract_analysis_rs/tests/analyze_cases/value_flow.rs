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
        vec![1],
        "expected duplicate block receipt enrichment to be fetched once"
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
