use super::*;

#[test]
fn selected_payload_exceeding_snapshot_budget_fails_before_payload_loading() {
    let options = DuckDbResourceOptions {
        max_snapshot_bytes_per_seed: 1,
        ..Default::default()
    };
    let store = DuckDbFeatureStore::new_with_options(":memory:", options).unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[DatabaseNftRecord {
                contract_address: "0xcandidate".into(),
                token_id: "1".into(),
                token_uri: "ipfs://shared".into(),
                metadata_json: "{\"description\":\"large selected payload\"}".into(),
                ..Default::default()
            }],
        )
        .unwrap();

    let error = store
        .load_snapshot(
            "ethereum",
            &[SeedNft {
                contract_address: "0xseed".into(),
                token_id: "1".into(),
                token_uri: "ipfs://shared".into(),
                ..Default::default()
            }],
            101.0,
            1.1,
            200,
            0,
        )
        .unwrap_err();

    let AppError::ResourceLimit(message) = error else {
        panic!("expected resource-limit error, got {error}");
    };
    assert!(message.contains("before payload loading"), "{message}");
}

#[test]
fn recall_index_cache_enforces_lru_count_and_byte_budget() {
    let mut cache = RecallIndexCache::new(2, 10);
    assert!(cache.insert("ethereum".into(), Arc::new(3_u64), 3));
    assert!(cache.insert("base".into(), Arc::new(4_u64), 4));
    assert!(cache.get("ethereum").is_some());
    assert!(cache.insert("polygon".into(), Arc::new(5_u64), 5));

    assert!(cache.contains("ethereum"));
    assert!(!cache.contains("base"));
    assert!(cache.contains("polygon"));
    assert_eq!(cache.resident_bytes(), 8);
    assert!(!cache.insert("oversized".into(), Arc::new(11_u64), 11));
    assert!(!cache.contains("oversized"));
}

#[test]
fn evicted_recall_index_arc_keeps_its_memory_lease_until_last_user_drops() {
    struct LeasedValue {
        _lease: MemoryLease,
    }

    let governor = Arc::new(MemoryGovernor::new(10));
    let first = Arc::new(LeasedValue {
        _lease: governor.try_reserve("first", 7).unwrap(),
    });
    let mut cache = RecallIndexCache::new(1, 10);
    assert!(cache.insert("first".into(), Arc::clone(&first), 7));
    drop(first);
    let active_user = cache.get("first").unwrap();
    let second = Arc::new(LeasedValue {
        _lease: governor.try_reserve("second", 3).unwrap(),
    });

    assert!(cache.insert("second".into(), Arc::clone(&second), 3));
    assert!(!cache.contains("first"));
    assert_eq!(governor.reserved_bytes(), 10);

    drop(active_user);
    assert_eq!(governor.reserved_bytes(), 3);
    drop(second);
    drop(cache);
    assert_eq!(governor.reserved_bytes(), 0);
}

#[test]
fn read_only_store_opens_the_configured_connection_pool() {
    let dir = tempfile::tempdir().unwrap();
    let database = dir.path().join("features.duckdb");
    {
        let store = DuckDbFeatureStore::new(database.to_str().unwrap()).unwrap();
        store
            .replace_chain_rows(
                "ethereum",
                &[DatabaseNftRecord {
                    contract_address: "0x1".into(),
                    token_id: "1".into(),
                    ..Default::default()
                }],
            )
            .unwrap();
    }
    let options = DuckDbResourceOptions::from_analysis_cli(
        96, "96GB", "280GB", 2, "24GB", 100_000, 2_000_000,
    )
    .unwrap();
    let store =
        DuckDbFeatureStore::open_read_only_with_options(database.to_str().unwrap(), options)
            .unwrap();

    assert_eq!(store.connection_count(), 2);
}

#[test]
fn production_analysis_resource_defaults_fit_the_512_gib_envelope() {
    let options = DuckDbResourceOptions::default();
    assert_eq!(options.recall_index_memory_limit_bytes, 260_000_000_000);

    DuckDbResourceOptions::from_analysis_cli(64, "96GB", "260GB", 2, "24GB", 100_000, 2_000_000)
        .unwrap();
}

#[test]
fn analysis_resource_options_reject_an_unsafe_combined_memory_envelope() {
    let error = DuckDbResourceOptions::from_analysis_cli(
        64, "300GB", "260GB", 2, "24GB", 100_000, 2_000_000,
    )
    .expect_err("unsafe memory envelope must be rejected");
    assert!(error
        .to_string()
        .contains("combined analysis memory envelope"));
}

#[test]
fn read_only_store_revalidates_programmatic_resource_options() {
    let dir = tempfile::tempdir().unwrap();
    let database = dir.path().join("features.duckdb");
    drop(DuckDbFeatureStore::new(database.to_str().unwrap()).unwrap());

    let unsafe_memory = DuckDbResourceOptions {
        memory_limit: "300GB".to_string(),
        ..Default::default()
    };
    let error =
        DuckDbFeatureStore::open_read_only_with_options(database.to_str().unwrap(), unsafe_memory)
            .err()
            .expect("unsafe memory envelope must be rejected");
    assert!(error
        .to_string()
        .contains("combined analysis memory envelope"));

    let zero_connections = DuckDbResourceOptions {
        read_connections: 0,
        ..Default::default()
    };
    let error = DuckDbFeatureStore::open_read_only_with_options(
        database.to_str().unwrap(),
        zero_connections,
    )
    .err()
    .expect("zero read connections must be rejected");
    assert!(error.to_string().contains("read connection count"));
}

#[test]
fn read_connection_worker_budgets_sum_to_the_declared_total() {
    assert_eq!(split_resource_budget(65, 2, 0), 33);
    assert_eq!(split_resource_budget(65, 2, 1), 32);
    assert_eq!(
        (0..3)
            .map(|index| split_resource_budget(100, 3, index))
            .sum::<usize>(),
        100
    );
}

#[test]
fn analysis_resource_options_reject_fewer_duckdb_threads_than_connections() {
    let error =
        DuckDbResourceOptions::from_analysis_cli(1, "96GB", "260GB", 2, "24GB", 100_000, 2_000_000)
            .expect_err("undersized DuckDB worker budget must be rejected");
    assert!(error
        .to_string()
        .contains("at least the read connection count"));
}

#[test]
fn selected_recall_rowid_table_is_reusable_across_fetches() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[
                DatabaseNftRecord {
                    contract_address: "0xa".into(),
                    token_id: "1".into(),
                    ..Default::default()
                },
                DatabaseNftRecord {
                    contract_address: "0xb".into(),
                    token_id: "2".into(),
                    ..Default::default()
                },
            ],
        )
        .unwrap();
    let conn = store.conn().unwrap();
    DuckDbFeatureStore::create_selected_recall_rowid_table(&conn).unwrap();

    let first = DuckDbFeatureStore::fetch_records_by_feature_rowid(&conn, &[0]).unwrap();
    let second = DuckDbFeatureStore::fetch_records_by_feature_rowid(&conn, &[1]).unwrap();
    let table_still_exists =
        DuckDbFeatureStore::table_exists(&conn, SELECTED_RECALL_ROWID_TABLE).unwrap();
    DuckDbFeatureStore::drop_seed_temp_tables(&conn).unwrap();

    assert_eq!(first[&0].contract_address, "0xa");
    assert_eq!(second[&1].contract_address, "0xb");
    assert!(table_still_exists);
}

#[test]
fn selected_recall_rowid_table_deduplicates_bulk_rowids() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    let conn = store.conn().unwrap();
    DuckDbFeatureStore::create_selected_recall_rowid_table(&conn).unwrap();

    DuckDbFeatureStore::replace_selected_recall_rowids(&conn, &[3, 1, 3, 2, 1]).unwrap();
    let rowids = conn
        .prepare(&format!(
            "SELECT feature_rowid FROM {SELECTED_RECALL_ROWID_TABLE} ORDER BY feature_rowid"
        ))
        .unwrap()
        .query_map([], |row| row.get::<_, i64>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    DuckDbFeatureStore::drop_seed_temp_tables(&conn).unwrap();

    assert_eq!(rowids, vec![1, 2, 3]);
}
