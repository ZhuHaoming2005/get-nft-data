use super::*;

#[tokio::test]
async fn analyze_processes_duplicate_contracts_within_a_seed_in_parallel() {
    let api = Arc::new(ConcurrentContractApi::new());
    let deps = AnalysisDeps {
        api: api.clone(),
        feature_store: Arc::new(FakeFeatureStore {
            snapshot: DatabaseSnapshot {
                nft_rows: vec![
                    DatabaseNftRecord {
                        contract_address: "0xdup1".into(),
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
                        contract_address: "0xdup2".into(),
                        token_id: "1".into(),
                        token_uri: "ipfs://seed/1".into(),
                        image_uri: "ipfs://image/1.png".into(),
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
            matched_contract_max_concurrency: 2,
            ..AnalyzeRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(payload.duplicate_contracts.len(), 2);
    assert_eq!(
        api.max_transfer_fetches.load(Ordering::SeqCst),
        2,
        "expected duplicate contract analysis to run matched contracts up to the configured limit"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn analyze_allows_later_matched_contract_to_start_before_previous_finishes() {
    let api = Arc::new(StaggeredExpansionApi::new());
    let deps = AnalysisDeps {
        api: api.clone(),
        feature_store: Arc::new(FakeFeatureStore {
            snapshot: DatabaseSnapshot {
                nft_rows: vec![
                    DatabaseNftRecord {
                        contract_address: "0xdup1".into(),
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
                        contract_address: "0xdup2".into(),
                        token_id: "1".into(),
                        token_uri: "ipfs://seed/1".into(),
                        image_uri: "ipfs://image/1.png".into(),
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
            matched_contract_max_concurrency: 2,
            ..AnalyzeRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(payload.duplicate_contracts.len(), 2);
    assert_eq!(
        api.transfer_before_slow_expansion_done
            .load(Ordering::SeqCst),
        1,
        "expected later matched contracts to start while an earlier contract is still expanding"
    );
}

#[tokio::test]
async fn analyze_does_not_fetch_obsolete_receipt_metrics_within_a_contract() {
    let api = Arc::new(ObsoleteReceiptMetricProbeApi::new());
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

    assert_eq!(payload.secondary_sale_victim_addresses.len(), 2);
    assert_eq!(
        api.max_receipts.load(Ordering::SeqCst),
        0,
        "removed receipt metric fetches are no longer part of the analysis path"
    );
}

#[tokio::test]
async fn analyze_does_not_prefetch_removed_metrics_per_buyer_with_shared_transaction_hash() {
    let api = Arc::new(ObsoleteReceiptMetricProbeApi::with_duplicate_sale_tx());
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

    assert_eq!(payload.secondary_sale_victim_addresses.len(), 2);
    assert_eq!(api.receipt_calls.load(Ordering::SeqCst), 0);
    assert_eq!(api.balance_calls.load(Ordering::SeqCst), 0);
    assert_eq!(api.same_block_transfer_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn analyze_does_not_prefetch_removed_metrics_for_latest_buyer_purchase() {
    let api = Arc::new(ObsoleteReceiptMetricProbeApi::with_same_buyer_history());
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

    assert_eq!(payload.secondary_sale_victim_addresses.len(), 1);
    assert_eq!(api.receipt_calls.load(Ordering::SeqCst), 0);
    assert_eq!(api.balance_calls.load(Ordering::SeqCst), 0);
    assert_eq!(api.same_block_transfer_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn analyze_fetches_contract_inputs_concurrently_within_one_contract() {
    let api = Arc::new(ConcurrentSingleContractFetchApi::new());
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
            ..AnalyzeRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(payload.duplicate_contracts.len(), 1);
    assert!(
        api.max_fetches.load(Ordering::SeqCst) >= 2,
        "expected transfers/owners/sales fetches to overlap within one contract"
    );
}

#[tokio::test]
async fn analyze_fetches_sales_and_mint_value_flow_without_market_events() {
    let api = Arc::new(ConcurrentSingleContractFetchApi::new());
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

    assert_eq!(payload.duplicate_contracts.len(), 1);
    assert!(payload.market_events.is_empty());
    assert_eq!(api.market_event_fetch_count.load(Ordering::SeqCst), 0);
    assert!(
        api.max_post_signal_fetches.load(Ordering::SeqCst) >= 2,
        "expected sales/mint value-flow fetches to overlap within one contract"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn analyze_overlaps_total_supply_check_with_candidate_expansion() {
    let api = Arc::new(ConcurrentExpansionSupplyApi::with_expansion_count(20));
    let deps = AnalysisDeps {
        api: api.clone(),
        feature_store: Arc::new(FakeFeatureStore {
            snapshot: DatabaseSnapshot {
                nft_rows: (1..=20)
                    .map(|token_id| DatabaseNftRecord {
                        contract_address: "0xdup".into(),
                        token_id: token_id.to_string(),
                        token_uri: format!("ipfs://seed/{token_id}"),
                        image_uri: format!("ipfs://image/{token_id}.png"),
                        name: format!("Azuki Mirror #{token_id}"),
                        symbol: "AZUKI".into(),
                        metadata_json: r#"{"description":"gold dragon"}"#.into(),
                        metadata_recall_checked: false,
                        metadata_recall_match: false,
                    })
                    .collect(),
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

    assert_eq!(payload.duplicate_contracts.len(), 1);
    assert!(
        api.supply_observed_expansion_active.load(Ordering::SeqCst),
        "expected totalSupply to be requested while NFT expansion is still in flight"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn analyze_skips_total_supply_check_for_small_expanded_candidate_set() {
    let api = Arc::new(ConcurrentExpansionSupplyApi::new());
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
            ..AnalyzeRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(payload.duplicate_contracts.len(), 1);
    assert_eq!(
        api.total_supply_calls.load(Ordering::SeqCst),
        0,
        "small candidate sets should not spend a totalSupply RPC that cannot affect filtering"
    );
}
