use super::*;

#[tokio::test]
async fn analyze_moves_official_reissues_into_legit_duplicates() {
    let api = Arc::new(FakeLegitApi::default());
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
                    metadata_json: r#"{"description":"Creative Commons Zero license: CC0"}"#.into(),
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

    assert!(payload.duplicate_contracts.is_empty());
    assert!(payload.duplicate_candidates.is_empty());
    assert!(payload.contract_level_summary.is_empty());
    assert!(payload.infringing_tokens.is_empty());
    assert_eq!(payload.legit_duplicates.len(), 1);
    assert_eq!(payload.legit_duplicates[0].contract_address, "0xdup");
    assert_eq!(
        payload.legit_duplicates[0].mint_recipients,
        vec!["0xminter"]
    );
    assert_eq!(
        payload
            .paper_stats
            .data_quality
            .legit_duplicate_contract_count,
        1
    );
    assert_eq!(payload.duplicate_contracts.len(), 0);
    assert!(payload.content_similarity_edges.is_empty());
    assert!(payload
        .contract_lifecycle_events
        .iter()
        .all(|event| event.contract_address != "0xdup"));
    assert!(payload.campaign_clusters.is_empty());
    assert!(payload.lifecycle_metrics.is_empty());
    assert_eq!(
        api.sales_calls.load(Ordering::SeqCst),
        0,
        "official reissues should not fetch sale history"
    );
}

#[tokio::test]
async fn analyze_moves_same_opensea_collection_candidates_into_legit_duplicates() {
    let api = Arc::new(CountingApi::with_seed_collection_slug("art-blocks"));
    let deps = AnalysisDeps {
        api,
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
            opensea_api_key: "opensea".into(),
            ..AnalyzeRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert!(payload.duplicate_candidates.is_empty());
    assert!(payload.duplicate_contracts.is_empty());
    assert_eq!(payload.legit_duplicates.len(), 1);
    assert_eq!(payload.legit_duplicates[0].contract_address, "0xdup");
    assert_eq!(
        payload.legit_duplicates[0].exclusion_reasons,
        vec!["OpenSea collection 与 seed 合约一致"]
    );
    assert_eq!(
        payload
            .paper_stats
            .data_quality
            .legit_duplicate_contract_count,
        1
    );
}

#[tokio::test]
async fn analyze_builds_expected_summary_counts() {
    let deps = AnalysisDeps {
        api: Arc::new(FakeApi),
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

    assert_eq!(payload.seed_contract.contract_address, "0xseed");
    assert_eq!(payload.duplicate_contracts.len(), 1);
    assert_eq!(payload.duplicate_candidates.len(), 1);
    assert_eq!(payload.duplicate_contracts.len(), 1);
    assert_eq!(payload.contract_level_summary["0xdup"].candidate_count, 1);
}

#[tokio::test]
async fn analyze_uses_contract_level_seed_name_for_snapshot_recall() {
    let feature_store = Arc::new(CapturingFeatureStore::default());
    let captured_seed_names = feature_store.captured_seed_names.clone();
    let deps = AnalysisDeps {
        api: Arc::new(FakeApi),
        feature_store,
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

    assert_eq!(payload.seed_contract.name, "Azuki");
    assert_eq!(
        captured_seed_names.lock().unwrap().as_slice(),
        &[vec!["Azuki".to_string()]]
    );
}

#[tokio::test]
async fn analyze_excludes_candidate_contract_that_currently_holds_seed_nft() {
    let deps = AnalysisDeps {
        api: Arc::new(FakeSeedOwnerApi),
        feature_store: Arc::new(FakeFeatureStore {
            snapshot: DatabaseSnapshot {
                nft_rows: vec![DatabaseNftRecord {
                    contract_address: "0xwrapped".into(),
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

    assert_eq!(payload.duplicate_contracts.len(), 0);
    assert!(payload.duplicate_candidates.is_empty());
    assert!(payload.duplicate_contracts.is_empty());
    assert!(payload.contract_level_summary.is_empty());
    assert_eq!(
        payload
            .paper_stats
            .data_quality
            .legit_duplicate_contract_count,
        1
    );
    assert_eq!(payload.legit_duplicates.len(), 1);
    assert_eq!(payload.legit_duplicates[0].contract_address, "0xwrapped");
    assert_eq!(
        payload.legit_duplicates[0].exclusion_reasons,
        vec!["当前持有 seed 合约 NFT"]
    );
}

#[tokio::test]
async fn analyze_excludes_candidate_contract_with_seed_transfer_history() {
    let deps = AnalysisDeps {
        api: Arc::new(FakeSeedTransferHistoryApi),
        feature_store: Arc::new(FakeFeatureStore {
            snapshot: DatabaseSnapshot {
                nft_rows: vec![DatabaseNftRecord {
                    contract_address: "0xwrapped".into(),
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

    assert_eq!(payload.duplicate_contracts.len(), 0);
    assert!(payload.duplicate_candidates.is_empty());
    assert!(payload.duplicate_contracts.is_empty());
    assert_eq!(
        payload
            .paper_stats
            .data_quality
            .legit_duplicate_contract_count,
        1
    );
    assert_eq!(payload.legit_duplicates[0].contract_address, "0xwrapped");
    assert_eq!(
        payload.legit_duplicates[0].exclusion_reasons,
        vec!["链上历史 Transfer 显示接收过 seed 合约 NFT"]
    );
}

#[tokio::test]
async fn analyze_keeps_wrapper_named_candidate_without_chain_seed_relation() {
    let deps = AnalysisDeps {
        api: Arc::new(FakeApi),
        feature_store: Arc::new(FakeFeatureStore {
            snapshot: DatabaseSnapshot {
                nft_rows: vec![DatabaseNftRecord {
                    contract_address: "0xwrapped".into(),
                    token_id: "1".into(),
                    token_uri: "ipfs://seed/1".into(),
                    image_uri: "ipfs://image/1.png".into(),
                    name: "Wrapped Azuki #1".into(),
                    symbol: "WAZUKI".into(),
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
    assert_eq!(payload.duplicate_candidates.len(), 1);
    assert_eq!(payload.duplicate_contracts.len(), 1);
    assert_eq!(
        payload.contract_level_summary["0xwrapped"].candidate_count,
        1
    );
    assert!(payload.legit_duplicates.is_empty());
}

#[tokio::test]
async fn analyze_keeps_locally_wrapper_named_candidate_without_chain_seed_relation() {
    let deps = AnalysisDeps {
        api: Arc::new(FakeApi),
        feature_store: Arc::new(FakeFeatureStore {
            snapshot: DatabaseSnapshot {
                nft_rows: vec![DatabaseNftRecord {
                    contract_address: "0xwrapped".into(),
                    token_id: "1".into(),
                    token_uri: "ipfs://seed/1".into(),
                    image_uri: "ipfs://image/1.png".into(),
                    name: "Azuki Mirror #1".into(),
                    symbol: "AZUKI".into(),
                    metadata_json: r#"{"description":"gold dragon"}"#.into(),
                    metadata_recall_checked: false,
                    metadata_recall_match: false,
                }],
                contract_names: vec![ContractNameRecord {
                    contract_address: "0xwrapped".into(),
                    name_norm: "wrapped azuki".into(),
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
    assert_eq!(payload.duplicate_candidates.len(), 1);
    assert_eq!(payload.duplicate_contracts.len(), 1);
    assert_eq!(
        payload.contract_level_summary["0xwrapped"].candidate_count,
        1
    );
    assert!(payload.legit_duplicates.is_empty());
}

#[tokio::test]
async fn analyze_marks_seed_open_license_and_skips_suspected_contracts() {
    let warm_calls = Arc::new(AtomicUsize::new(0));
    let deps = AnalysisDeps {
        api: Arc::new(WarmCountingApi {
            inner: FakeOpenLicenseApi,
            warm_calls: warm_calls.clone(),
        }),
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

    assert!(payload.duplicate_contracts.is_empty());
    assert!(payload.duplicate_contracts.is_empty());
    assert!(payload.infringing_tokens.is_empty());
    assert_eq!(warm_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn analyze_writes_default_json_and_markdown_files() {
    let deps = AnalysisDeps {
        api: Arc::new(FakeApi),
        feature_store: Arc::new(FakeFeatureStore {
            snapshot: DatabaseSnapshot::default(),
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
    let dir = tempdir().unwrap();

    let (json_path, md_path) = write_outputs_to_directory(&payload, dir.path()).unwrap();

    assert!(json_path
        .file_name()
        .unwrap()
        .to_string_lossy()
        .starts_with("top_contract_analysis__"));
    assert!(md_path.exists());
}

#[tokio::test]
async fn analyze_enriches_duplicate_contracts_with_signals_and_infringing_tokens() {
    let deps = AnalysisDeps {
        api: Arc::new(FakeEnrichedApi),
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

    assert_eq!(payload.infringing_tokens.len(), 1);
    assert_eq!(payload.infringing_tokens[0].contract_address, "0xdup");
    assert_eq!(payload.infringing_tokens[0].minter_address, "0xminter");
    assert_eq!(payload.address_signals["0xdup"].mint_address_count, 1);
    assert!(payload.address_signals["0xdup"].fast_spread);
    assert_eq!(payload.victim_signals["0xdup"].owner_count, 1);
    assert_eq!(payload.victim_signals["0xdup"].stuck_holder_count, 1);
    assert_eq!(payload.infringing_tokens.len(), 1);
    assert_eq!(
        payload.duplicate_contracts[0].contract_deployer,
        "0xcreator"
    );
    assert_eq!(payload.duplicate_contracts[0].deployed_block_number, 123);
    assert_eq!(payload.duplicate_contracts[0].owner_address, "0xowner");
    assert_eq!(payload.duplicate_contracts[0].admin_address, "0xadmin");
    assert_eq!(
        payload.duplicate_contracts[0].proxy_admin_address,
        "0xproxyadmin"
    );
    assert!(payload.contract_lifecycle_events.is_empty());
    let mint_payment_edge = payload
        .value_flow_edges
        .iter()
        .find(|edge| edge.channel == "mint_payment")
        .expect("mint payment value flow edge");
    assert_eq!(mint_payment_edge.tx_hash, "0xmint");
    assert_eq!(mint_payment_edge.from_address, "0xminter");
    assert_eq!(mint_payment_edge.to_address, "0xcreator");
    assert_eq!(mint_payment_edge.value_eth, Some(0.08));
    assert_eq!(mint_payment_edge.value_usd, Some(184.0));
    assert!(
        payload
            .value_flow_edges
            .iter()
            .all(|edge| edge.channel != "funding"),
        "receiver-only mint value-flow lookups should not fetch per-minter funding edges"
    );
    let withdrawal_edge = payload
        .value_flow_edges
        .iter()
        .find(|edge| edge.channel == "withdrawal")
        .expect("withdrawal value flow edge");
    assert_eq!(withdrawal_edge.from_address, "0xdup");
    assert_eq!(withdrawal_edge.to_address, "0xfunder");
    assert!(payload.lifecycle_metrics.is_empty());
    assert!(payload.weak_supervision_labels.is_empty());
    assert!(payload.early_detection_features.is_empty());
    assert!(payload.contract_lifecycle_events.is_empty());
}

#[tokio::test]
async fn analyze_filters_candidates_deployed_before_seed_contract() {
    let api = Arc::new(PreSeedDeploymentApi {
        contract_nft_calls: AtomicUsize::new(0),
        total_supply_calls: AtomicUsize::new(0),
        transfer_calls: AtomicUsize::new(0),
        owner_calls: AtomicUsize::new(0),
        sale_calls: AtomicUsize::new(0),
    });
    let deps = AnalysisDeps {
        api: api.clone(),
        feature_store: Arc::new(FakeFeatureStore {
            snapshot: DatabaseSnapshot {
                nft_rows: vec![DatabaseNftRecord {
                    contract_address: "0xold".into(),
                    token_id: "1".into(),
                    token_uri: "ipfs://seed/1".into(),
                    image_uri: "ipfs://image/1.png".into(),
                    name: "Azuki Mirror #1".into(),
                    symbol: "AZUKI".into(),
                    metadata_json: r#"{"name":"Azuki Mirror #1"}"#.into(),
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

    assert_eq!(payload.duplicate_contracts.len(), 0);
    assert!(payload.duplicate_candidates.is_empty());
    assert_eq!(payload.infringing_tokens.len(), 0);
    assert!(payload.duplicate_candidates.is_empty());
    assert!(payload.duplicate_contracts.is_empty());
    assert!(!payload
        .contract_lifecycle_events
        .iter()
        .any(|event| event.contract_address == "0xold"));
    assert_eq!(api.contract_nft_calls.load(Ordering::SeqCst), 0);
    assert_eq!(api.total_supply_calls.load(Ordering::SeqCst), 0);
    assert_eq!(api.transfer_calls.load(Ordering::SeqCst), 0);
    assert_eq!(api.owner_calls.load(Ordering::SeqCst), 0);
    assert_eq!(api.sale_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn analyze_hard_excludes_candidates_when_current_supply_conflicts_with_expanded_tokens() {
    let api = Arc::new(SupplyMismatchApi {
        current_total_supply: 2,
        contract_nft_calls: AtomicUsize::new(0),
        total_supply_calls: AtomicUsize::new(0),
        transfer_calls: AtomicUsize::new(0),
        owner_calls: AtomicUsize::new(0),
        sale_calls: AtomicUsize::new(0),
    });
    let deps = AnalysisDeps {
        api: api.clone(),
        feature_store: Arc::new(FakeFeatureStore {
            snapshot: DatabaseSnapshot {
                nft_rows: current_supply_snapshot_rows(25),
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

    assert_eq!(payload.duplicate_contracts.len(), 0);
    assert!(payload.duplicate_candidates.is_empty());
    assert_eq!(payload.infringing_tokens.len(), 0);
    assert!(payload.duplicate_candidates.is_empty());
    assert!(payload.duplicate_contracts.is_empty());
    assert_eq!(api.contract_nft_calls.load(Ordering::SeqCst), 1);
    assert_eq!(api.total_supply_calls.load(Ordering::SeqCst), 1);
    assert_eq!(api.transfer_calls.load(Ordering::SeqCst), 0);
    assert_eq!(api.owner_calls.load(Ordering::SeqCst), 0);
    assert_eq!(api.sale_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn analyze_keeps_candidate_when_current_supply_only_has_small_indexing_drift() {
    let api = Arc::new(SupplyMismatchApi {
        current_total_supply: 24,
        contract_nft_calls: AtomicUsize::new(0),
        total_supply_calls: AtomicUsize::new(0),
        transfer_calls: AtomicUsize::new(0),
        owner_calls: AtomicUsize::new(0),
        sale_calls: AtomicUsize::new(0),
    });
    let deps = AnalysisDeps {
        api: api.clone(),
        feature_store: Arc::new(FakeFeatureStore {
            snapshot: DatabaseSnapshot {
                nft_rows: current_supply_snapshot_rows(25),
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
    assert_eq!(api.contract_nft_calls.load(Ordering::SeqCst), 1);
    assert_eq!(api.total_supply_calls.load(Ordering::SeqCst), 1);
    assert_eq!(api.transfer_calls.load(Ordering::SeqCst), 1);
    assert_eq!(api.owner_calls.load(Ordering::SeqCst), 1);
    assert_eq!(api.sale_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn analyze_expands_matched_contracts_to_all_tokens_for_report_analysis() {
    let deps = AnalysisDeps {
        api: Arc::new(FakeTwoTokenOwnersApi),
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
                    ..DatabaseNftRecord::default()
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

    assert_eq!(payload.duplicate_candidates.len(), 1);
    assert_eq!(payload.duplicate_contracts[0].candidate_count, 2);
    assert_eq!(payload.contract_level_summary["0xdup"].candidate_count, 2);
    assert_eq!(payload.infringing_tokens.len(), 2);
    assert_eq!(
        payload
            .infringing_tokens
            .iter()
            .map(|item| item.token_id.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["1", "2"])
    );
    let neutral_address_count = payload
        .address_attributions
        .iter()
        .filter(|item| item.attribution_label == "neutral_participant")
        .map(|item| item.address.as_str())
        .collect::<BTreeSet<_>>()
        .len() as i64;
    assert_eq!(neutral_address_count, 4);
    assert_eq!(
        payload
            .honest_addresses
            .iter()
            .map(|item| item.address.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["0xholder1", "0xholder2"])
    );
}

#[tokio::test]
async fn analyze_falls_back_to_local_snapshot_when_provider_expansion_is_empty() {
    let deps = AnalysisDeps {
        api: Arc::new(FakeEmptyContractNftsApi),
        feature_store: Arc::new(FakeFeatureStore {
            snapshot: DatabaseSnapshot {
                nft_rows: vec![
                    DatabaseNftRecord {
                        contract_address: "0xdup".into(),
                        token_id: "1".into(),
                        token_uri: "ipfs://seed/1".into(),
                        image_uri: "ipfs://image/1.png".into(),
                        name: "Azuki Mirror #1".into(),
                        metadata_json: r#"{"description":"gold dragon"}"#.into(),
                        ..DatabaseNftRecord::default()
                    },
                    DatabaseNftRecord {
                        contract_address: "0xdup".into(),
                        token_id: "2".into(),
                        token_uri: "ipfs://candidate/2".into(),
                        image_uri: "ipfs://candidate/2.png".into(),
                        name: "Azuki Mirror #2".into(),
                        metadata_json: r#"{"description":"different trait"}"#.into(),
                        ..DatabaseNftRecord::default()
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

    assert_eq!(payload.duplicate_contracts[0].candidate_count, 2);
    assert_eq!(payload.contract_level_summary["0xdup"].candidate_count, 2);
    assert_eq!(payload.infringing_tokens.len(), 2);
}

#[tokio::test]
async fn analyze_ignores_symbol_only_candidate_contracts() {
    let deps = AnalysisDeps {
        api: Arc::new(FakeEnrichedApi),
        feature_store: Arc::new(FakeFeatureStore {
            snapshot: DatabaseSnapshot {
                nft_rows: vec![DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "1".into(),
                    token_uri: "ipfs://other/1".into(),
                    image_uri: "ipfs://other-image/1.png".into(),
                    name: "Completely Different".into(),
                    symbol: "AZUKI".into(),
                    metadata_json: r#"{"description":"unrelated"}"#.into(),
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

    assert!(payload.duplicate_contracts.is_empty());
    assert!(payload.infringing_tokens.is_empty());
    assert!(payload.address_signals.is_empty());
    assert!(payload.victim_signals.is_empty());
    assert_eq!(payload.duplicate_contracts.len(), 0);
    assert_eq!(payload.infringing_tokens.len(), 0);
}
