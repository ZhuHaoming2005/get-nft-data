use super::*;

#[test]
fn load_snapshot_recalls_metadata_from_only_one_seed_example() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[DatabaseNftRecord {
                contract_address: "0xcandidate".into(),
                token_id: "1".into(),
                metadata_json: r#"{"second_unique":"silver cat"}"#.into(),
                ..Default::default()
            }],
        )
        .unwrap();
    let seed_nfts = vec![
        SeedNft {
            contract_address: "0xseed".into(),
            token_id: "1".into(),
            metadata_json: r#"{"first_unique":"gold dragon"}"#.into(),
            ..Default::default()
        },
        SeedNft {
            contract_address: "0xseed".into(),
            token_id: "2".into(),
            metadata_json: r#"{"second_unique":"silver cat"}"#.into(),
            ..Default::default()
        },
    ];

    let snapshot = store
        .load_snapshot("ethereum", &seed_nfts, 95.0, 0.6, 0, 0)
        .unwrap();

    assert!(snapshot.nft_rows.is_empty());
}

#[test]
fn load_snapshot_marks_rows_that_were_recalled_by_metadata() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[
                DatabaseNftRecord {
                    contract_address: "0xmetadata".into(),
                    token_id: "1".into(),
                    metadata_json: r#"{"description":"gold dragon"}"#.into(),
                    ..Default::default()
                },
                DatabaseNftRecord {
                    contract_address: "0ximage".into(),
                    token_id: "1".into(),
                    image_uri: "ipfs://seed-image.png".into(),
                    ..Default::default()
                },
            ],
        )
        .unwrap();
    let seed_nfts = vec![SeedNft {
        contract_address: "0xseed".into(),
        token_id: "1".into(),
        image_uri: "ipfs://seed-image.png".into(),
        metadata_json: r#"{"description":"gold dragon"}"#.into(),
        ..Default::default()
    }];

    let snapshot = store
        .load_snapshot("ethereum", &seed_nfts, 95.0, 0.6, 0, 0)
        .unwrap();
    let by_contract: BTreeMap<_, _> = snapshot
        .nft_rows
        .iter()
        .map(|row| (row.contract_address.as_str(), row))
        .collect();

    assert!(by_contract["0xmetadata"].metadata_recall_checked);
    assert!(by_contract["0xmetadata"].metadata_recall_match);
    assert!(by_contract["0ximage"].metadata_recall_checked);
    assert!(!by_contract["0ximage"].metadata_recall_match);
}

#[test]
fn load_snapshot_uses_max_recall_rows_as_batch_size_not_total_limit() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[
                DatabaseNftRecord {
                    contract_address: "0xcandidate_a".into(),
                    token_id: "1".into(),
                    token_uri: "ipfs://shared/1".into(),
                    ..Default::default()
                },
                DatabaseNftRecord {
                    contract_address: "0xcandidate_b".into(),
                    token_id: "1".into(),
                    token_uri: "ipfs://shared/1".into(),
                    ..Default::default()
                },
            ],
        )
        .unwrap();
    let seed_nfts = vec![SeedNft {
        contract_address: "0xseed".into(),
        token_id: "1".into(),
        token_uri: "ipfs://shared/1".into(),
        ..Default::default()
    }];

    let snapshot = store
        .load_snapshot("ethereum", &seed_nfts, 95.0, 0.6, 0, 1)
        .unwrap();
    let contracts: Vec<_> = snapshot
        .nft_rows
        .iter()
        .map(|row| row.contract_address.as_str())
        .collect();

    assert_eq!(contracts, vec!["0xcandidate_a", "0xcandidate_b"]);
}

#[test]
fn duplicate_contract_row_uses_precomputed_name_pair_uniqueness() {
    let record = DatabaseNftRecord {
        contract_address: "0xcandidate".into(),
        token_id: "1".into(),
        ..Default::default()
    };
    let mut rows_by_contract = HashMap::new();

    update_duplicate_contract_row(
        &mut rows_by_contract,
        &record,
        false,
        false,
        "azuki",
        true,
        false,
    );
    update_duplicate_contract_row(
        &mut rows_by_contract,
        &record,
        false,
        false,
        "azuki",
        false,
        false,
    );

    assert_eq!(
        rows_by_contract["0xcandidate"].name_norms,
        vec!["azuki".to_string()]
    );
}

#[test]
fn load_snapshot_recalls_metadata_without_persistent_keyword_index() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[DatabaseNftRecord {
                contract_address: "0xcandidate".into(),
                token_id: "1".into(),
                metadata_json: r#"{"description":"gold dragon"}"#.into(),
                ..Default::default()
            }],
        )
        .unwrap();

    let conn = store.conn().unwrap();
    let has_persistent_index = conn
        .query_row(
            "
                SELECT EXISTS(
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_name = 'metadata_keyword_index'
                )
                ",
            [],
            |row| row.get::<_, bool>(0),
        )
        .unwrap();
    drop(conn);
    let snapshot = store
        .load_snapshot(
            "ethereum",
            &[SeedNft {
                contract_address: "0xseed".into(),
                token_id: "1".into(),
                metadata_json: r#"{"description":"gold dragon"}"#.into(),
                ..Default::default()
            }],
            95.0,
            0.6,
            0,
            0,
        )
        .unwrap();

    assert!(!has_persistent_index);
    assert_eq!(snapshot.duplicate_contract_rows.len(), 1);
    assert!(snapshot.contract_signals["0xcandidate"].keyword_match);
}

#[test]
fn opening_existing_feature_db_does_not_backfill_persistent_metadata_keyword_index() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("features.duckdb");
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
                "
                CREATE TABLE nft_features (
                    chain VARCHAR NOT NULL,
                    contract_address VARCHAR NOT NULL,
                    token_id VARCHAR NOT NULL,
                    token_uri VARCHAR,
                    image_uri VARCHAR,
                    name VARCHAR,
                    symbol VARCHAR,
                    metadata_json VARCHAR,
                    token_uri_norm VARCHAR,
                    image_uri_norm VARCHAR,
                    name_norm VARCHAR
                );
                INSERT INTO nft_features VALUES (
                    'ethereum', '0xcandidate', '1', '', '', '', '', '{\"description\":\"gold dragon\"}',
                    '', '', ''
                );
                ",
            )
            .unwrap();
    }

    let store = DuckDbFeatureStore::new(&db_path.to_string_lossy()).unwrap();
    let conn = store.conn().unwrap();
    let has_persistent_index = conn
        .query_row(
            "
                SELECT EXISTS(
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_name = 'metadata_keyword_index'
                )
                ",
            [],
            |row| row.get::<_, bool>(0),
        )
        .unwrap();
    drop(conn);
    let snapshot = store
        .load_snapshot(
            "ethereum",
            &[SeedNft {
                contract_address: "0xseed".into(),
                token_id: "1".into(),
                metadata_json: r#"{"description":"gold dragon"}"#.into(),
                ..Default::default()
            }],
            95.0,
            0.6,
            0,
            0,
        )
        .unwrap();

    assert!(!has_persistent_index);
    assert_eq!(snapshot.duplicate_contract_rows.len(), 1);
}

#[test]
fn load_snapshot_rejects_read_only_db_without_prepared_recall_tables() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("features.duckdb");
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "
                CREATE TABLE nft_features (
                    chain VARCHAR NOT NULL,
                    contract_address VARCHAR NOT NULL,
                    token_id VARCHAR NOT NULL,
                    token_uri VARCHAR,
                    image_uri VARCHAR,
                    name VARCHAR,
                    symbol VARCHAR,
                    metadata_json VARCHAR,
                    token_uri_norm VARCHAR,
                    image_uri_norm VARCHAR,
                    name_norm VARCHAR
                );
                INSERT INTO nft_features VALUES (
                    'ethereum', '0xcandidate', '1', 'ipfs://shared/1', '', '', '',
                    '{\"description\":\"gold dragon\"}', 'ipfs://shared/1', '', ''
                );
                ",
        )
        .unwrap();
    }

    let store = DuckDbFeatureStore::open_read_only_with_options(
        &db_path.to_string_lossy(),
        DuckDbResourceOptions::default(),
    )
    .unwrap();
    let err = store
        .load_snapshot(
            "ethereum",
            &[SeedNft {
                contract_address: "0xseed".into(),
                token_id: "1".into(),
                token_uri: "ipfs://shared/1".into(),
                metadata_json: r#"{"description":"gold dragon"}"#.into(),
                ..Default::default()
            }],
            101.0,
            0.6,
            0,
            0,
        )
        .unwrap_err();

    assert!(
        err.to_string().contains("prepared recall tables"),
        "unexpected error: {err}"
    );
}

#[test]
fn replace_chain_rows_populates_contract_representatives() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[
                DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "2".into(),
                    name: "Zed Clone".into(),
                    metadata_json: r#"{"description":"silver cat"}"#.into(),
                    ..Default::default()
                },
                DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "1".into(),
                    name: "Alpha Clone".into(),
                    metadata_json: r#"{"description":"gold dragon"}"#.into(),
                    ..Default::default()
                },
            ],
        )
        .unwrap();

    let conn = store.conn().unwrap();
    let (contract_address, name_token_id, metadata_token_id): (String, String, String) = conn
        .query_row(
            "
                SELECT r.contract_address, name_row.token_id, metadata_row.token_id
                FROM nft_contract_representatives r
                JOIN nft_features name_row ON name_row.rowid = r.name_feature_rowid
                JOIN nft_features metadata_row ON metadata_row.rowid = r.metadata_feature_rowid
                WHERE r.chain = 'ethereum'
                ",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();

    assert_eq!(contract_address, "0xdup");
    assert_eq!(name_token_id, "1");
    assert_eq!(metadata_token_id, "1");
}

#[test]
fn load_snapshot_reuses_and_invalidates_metadata_recall_index_cache() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[DatabaseNftRecord {
                contract_address: "0xdup".into(),
                token_id: "1".into(),
                metadata_json: r#"{"description":"gold dragon"}"#.into(),
                ..Default::default()
            }],
        )
        .unwrap();
    let seed = vec![SeedNft {
        chain: "ethereum".into(),
        contract_address: "0xseed".into(),
        token_id: "1".into(),
        metadata_json: r#"{"description":"gold dragon"}"#.into(),
        ..Default::default()
    }];

    assert_eq!(store.metadata_recall_index_cache_len(), 0);
    store
        .load_snapshot("ethereum", &seed, 101.0, 0.6, 0, 0)
        .unwrap();
    assert_eq!(store.metadata_recall_index_cache_len(), 1);
    store
        .load_snapshot("ethereum", &seed, 101.0, 0.6, 0, 0)
        .unwrap();
    assert_eq!(store.metadata_recall_index_cache_len(), 1);

    store
        .replace_chain_rows(
            "ethereum",
            &[DatabaseNftRecord {
                contract_address: "0xother".into(),
                token_id: "1".into(),
                metadata_json: r#"{"description":"silver cat"}"#.into(),
                ..Default::default()
            }],
        )
        .unwrap();
    assert_eq!(store.metadata_recall_index_cache_len(), 0);
}

#[test]
fn replace_chain_rows_populates_prepared_recall_tables() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[
                DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "2".into(),
                    token_uri: "ipfs://dup/2".into(),
                    image_uri: "ipfs://image/2.png".into(),
                    name: "Zed Clone".into(),
                    metadata_json: r#"{"description":"silver cat"}"#.into(),
                    ..Default::default()
                },
                DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "1".into(),
                    token_uri: "ipfs://dup/1".into(),
                    image_uri: "ipfs://image/1.png".into(),
                    name: "Alpha Clone".into(),
                    metadata_json: r#"{"description":"gold dragon"}"#.into(),
                    ..Default::default()
                },
            ],
        )
        .unwrap();

    let conn = store.conn().unwrap();
    let recall_count: i64 = conn
        .query_row(
            "SELECT count(*) FROM nft_feature_recall_rows WHERE chain = 'ethereum'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let (token_id, recall_doc): (String, String) = conn
        .query_row(
            "
                SELECT token_id, recall_doc
                FROM nft_metadata_recall_docs
                WHERE chain = 'ethereum'
                ",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let prepared: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM nft_prepared_recall_chains WHERE chain = 'ethereum')",
            [],
            |row| row.get(0),
        )
        .unwrap();

    assert_eq!(recall_count, 2);
    assert_eq!(token_id, "1");
    assert_eq!(recall_doc, "description gold dragon");
    assert!(prepared);
}

#[test]
fn load_snapshot_backfills_prepared_recall_tables_for_existing_writable_db() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("features.duckdb");
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "
                CREATE TABLE nft_features (
                    chain VARCHAR NOT NULL,
                    contract_address VARCHAR NOT NULL,
                    token_id VARCHAR NOT NULL,
                    token_uri VARCHAR,
                    image_uri VARCHAR,
                    name VARCHAR,
                    symbol VARCHAR,
                    metadata_json VARCHAR,
                    token_uri_norm VARCHAR,
                    image_uri_norm VARCHAR,
                    name_norm VARCHAR
                );
                INSERT INTO nft_features VALUES (
                    'ethereum', '0xcandidate', '1', 'ipfs://shared/1', '', 'Gold Clone', '',
                    '{\"description\":\"gold dragon\"}', 'ipfs://shared/1', '', 'gold clone'
                );
                ",
        )
        .unwrap();
    }

    let store = DuckDbFeatureStore::new(&db_path.to_string_lossy()).unwrap();
    let seed_nfts = vec![SeedNft {
        contract_address: "0xseed".into(),
        token_id: "1".into(),
        token_uri: "ipfs://shared/1".into(),
        metadata_json: r#"{"description":"gold dragon"}"#.into(),
        ..Default::default()
    }];
    let snapshot = store
        .load_snapshot("ethereum", &seed_nfts, 101.0, 0.6, 0, 0)
        .unwrap();

    let conn = store.conn().unwrap();
    let prepared: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM nft_prepared_recall_chains WHERE chain = 'ethereum')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let recall_count: i64 = conn
        .query_row(
            "SELECT count(*) FROM nft_feature_recall_rows WHERE chain = 'ethereum'",
            [],
            |row| row.get(0),
        )
        .unwrap();

    assert_eq!(snapshot.duplicate_contract_rows.len(), 1);
    assert!(prepared);
    assert_eq!(recall_count, 1);
}

#[test]
fn load_snapshot_refreshes_stale_prepared_recall_tables() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[DatabaseNftRecord {
                contract_address: "0xcandidate_a".into(),
                token_id: "1".into(),
                token_uri: "ipfs://shared/1".into(),
                ..Default::default()
            }],
        )
        .unwrap();

    {
        let conn = store.conn().unwrap();
        conn.execute(
            "
                INSERT INTO nft_features (
                    chain, contract_address, token_id, token_uri, image_uri, name, symbol,
                    metadata_json, token_uri_norm, image_uri_norm, name_norm
                ) VALUES ('ethereum', '0xcandidate_b', '1', 'ipfs://shared/1', '', '', '',
                          '', ?, '', '')
                ",
            params![normalize_url("ipfs://shared/1")],
        )
        .unwrap();
    }

    let seed_nfts = vec![SeedNft {
        contract_address: "0xseed".into(),
        token_id: "1".into(),
        token_uri: "ipfs://shared/1".into(),
        ..Default::default()
    }];
    let snapshot = store
        .load_snapshot("ethereum", &seed_nfts, 101.0, 0.6, 0, 0)
        .unwrap();
    let contracts = snapshot
        .nft_rows
        .iter()
        .map(|row| row.contract_address.as_str())
        .collect::<Vec<_>>();

    assert_eq!(contracts, vec!["0xcandidate_a", "0xcandidate_b"]);
}

#[test]
fn load_snapshot_invalidates_cached_metadata_index_when_prepared_tables_refresh() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[DatabaseNftRecord {
                contract_address: "0xcandidate_a".into(),
                token_id: "1".into(),
                metadata_json: r#"{"description":"gold dragon"}"#.into(),
                ..Default::default()
            }],
        )
        .unwrap();
    let seed_nfts = vec![SeedNft {
        contract_address: "0xseed".into(),
        token_id: "99".into(),
        metadata_json: r#"{"description":"gold dragon"}"#.into(),
        ..Default::default()
    }];
    store
        .load_snapshot("ethereum", &seed_nfts, 101.0, 0.6, 0, 0)
        .unwrap();
    assert_eq!(store.metadata_recall_index_cache_len(), 1);

    {
        let conn = store.conn().unwrap();
        conn.execute(
            "
                INSERT INTO nft_features (
                    chain, contract_address, token_id, token_uri, image_uri, name, symbol,
                    metadata_json, token_uri_norm, image_uri_norm, name_norm
                ) VALUES ('ethereum', '0xcandidate_b', '1', '', '', '', '',
                          '{\"description\":\"gold dragon\"}', '', '', '')
                ",
            [],
        )
        .unwrap();
    }

    let snapshot = store
        .load_snapshot("ethereum", &seed_nfts, 101.0, 0.6, 0, 0)
        .unwrap();
    let contracts = snapshot
        .nft_rows
        .iter()
        .map(|row| row.contract_address.as_str())
        .collect::<Vec<_>>();

    assert_eq!(contracts, vec!["0xcandidate_a", "0xcandidate_b"]);
}

#[test]
fn opening_writable_feature_db_drops_obsolete_metadata_columns() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("features.duckdb");
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
                "
                CREATE TABLE nft_features (
                    chain VARCHAR NOT NULL,
                    contract_address VARCHAR NOT NULL,
                    token_id VARCHAR NOT NULL,
                    token_uri VARCHAR,
                    image_uri VARCHAR,
                    name VARCHAR,
                    symbol VARCHAR,
                    metadata_json VARCHAR,
                    token_uri_norm VARCHAR,
                    image_uri_norm VARCHAR,
                    name_norm VARCHAR,
                    metadata_doc VARCHAR,
                    name_prefix8 VARCHAR,
                    metadata_keywords_arr VARCHAR
                );
                INSERT INTO nft_features VALUES (
                    'ethereum', '0xcandidate', '1', '', '', '', '', '{\"description\":\"gold dragon\"}',
                    '', '', '', 'gold dragon', '', '[\"description\",\"dragon\",\"gold\"]'
                );
                ",
            )
            .unwrap();
    }

    let store = DuckDbFeatureStore::new(&db_path.to_string_lossy()).unwrap();
    let conn = store.conn().unwrap();
    let columns = DuckDbFeatureStore::table_columns(&conn, "nft_features").unwrap();
    let row_count: i64 = conn
        .query_row("SELECT count(*) FROM nft_features", [], |row| row.get(0))
        .unwrap();

    assert!(!columns.contains("metadata_doc"));
    assert!(!columns.contains("name_prefix8"));
    assert!(!columns.contains("metadata_keywords_arr"));
    assert_eq!(row_count, 1);
}

#[test]
fn feature_store_configures_bulk_import_checkpoint_limits() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    let conn = store.conn().unwrap();
    let checkpoint_threshold: String = conn
        .query_row(
            "SELECT current_setting('checkpoint_threshold')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let skip_wal_threshold: u64 = conn
        .query_row(
            "SELECT current_setting('auto_checkpoint_skip_wal_threshold')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let write_buffer_row_group_count: u64 = conn
        .query_row(
            "SELECT current_setting('write_buffer_row_group_count')",
            [],
            |row| row.get(0),
        )
        .unwrap();

    assert!(checkpoint_threshold.contains("GiB"));
    assert_eq!(skip_wal_threshold, BULK_IMPORT_SKIP_WAL_THRESHOLD_BYTES);
    assert_eq!(write_buffer_row_group_count, 1);
}

#[test]
fn metadata_source_bucket_hit_estimate_deduplicates_candidates() {
    let seed = MetadataSketch {
        simhash: 0,
        anchors: vec!["gold".into()],
    };
    let source_index = MetadataSourceIndex {
        anchor_indices: HashMap::from([("gold".to_string(), vec![0, 1, 1])]),
        simhash_band_indices: vec![vec![0, 1]; METADATA_SIMHASH_BAND_COUNT],
    };

    assert_eq!(
        DuckDbFeatureStore::estimate_metadata_source_bucket_hits(
            &seed,
            &source_index,
            METADATA_SKETCH_SOURCE_HAMMING_THRESHOLD,
            2,
        ),
        2
    );
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
