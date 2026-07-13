use super::*;

#[test]
fn snapshot_identity_reads_generation_catalog_without_scanning_feature_rows() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[DatabaseNftRecord {
                contract_address: "0xcandidate".into(),
                token_id: "1".into(),
                ..Default::default()
            }],
        )
        .unwrap();
    let conn = store.conn().unwrap();
    conn.execute_batch("ALTER TABLE nft_features RENAME TO hidden_nft_features")
        .unwrap();
    drop(conn);

    let identity = store.snapshot_identity("ethereum").unwrap();

    assert!(!identity.is_empty());
}

#[test]
fn chain_totals_read_generation_catalog_without_scanning_feature_rows() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[DatabaseNftRecord {
                contract_address: "0xcandidate".into(),
                token_id: "1".into(),
                ..Default::default()
            }],
        )
        .unwrap();
    let conn = store.conn().unwrap();
    conn.execute_batch("ALTER TABLE nft_features RENAME TO hidden_nft_features")
        .unwrap();
    drop(conn);

    let totals = store.chain_totals("ethereum").unwrap();

    assert_eq!(totals.total_nfts, 1);
    assert_eq!(totals.total_contracts, 1);
}

#[test]
fn prepared_readiness_reads_generation_catalog_without_scanning_feature_rows() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[DatabaseNftRecord {
                contract_address: "0xcandidate".into(),
                token_id: "1".into(),
                token_uri: "ipfs://candidate/1".into(),
                ..Default::default()
            }],
        )
        .unwrap();
    let conn = store.conn().unwrap();
    conn.execute_batch("ALTER TABLE nft_features RENAME TO hidden_nft_features")
        .unwrap();
    drop(conn);

    store
        .require_prepared_for_chains(&[crate::models::Chain::Ethereum])
        .unwrap();
}

#[test]
fn prepared_readiness_rejects_every_version_identity_mismatch() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[DatabaseNftRecord {
                contract_address: "0xcandidate".into(),
                token_id: "1".into(),
                token_uri: "ipfs://candidate/1".into(),
                ..Default::default()
            }],
        )
        .unwrap();
    let versions = [
        ("prepared_format_version", PREPARED_FORMAT_VERSION),
        ("normalization_version", NORMALIZATION_VERSION),
        ("recall_algorithm_version", RECALL_ALGORITHM_VERSION),
        ("report_schema_version", REPORT_SCHEMA_VERSION),
        ("build_fingerprint", BUILD_FINGERPRINT),
    ];

    for (column, current) in versions {
        {
            let conn = store.conn().unwrap();
            conn.execute(
                &format!(
                    "UPDATE {PREPARED_RECALL_CHAIN_TABLE} SET {column} = 'stale' WHERE chain = 'ethereum'"
                ),
                [],
            )
            .unwrap();
        }
        let error = store
            .require_prepared_for_chains(&[crate::models::Chain::Ethereum])
            .unwrap_err();
        assert!(
            error.to_string().contains("prepare-features"),
            "{column}: {error}"
        );
        store
            .conn()
            .unwrap()
            .execute(
                &format!(
                    "UPDATE {PREPARED_RECALL_CHAIN_TABLE} SET {column} = ? WHERE chain = 'ethereum'"
                ),
                params![current],
            )
            .unwrap();
    }
}

#[test]
fn authoritative_prepared_readiness_requires_the_global_ready_phase() {
    let (_dir, store, chains) = prepared_authoritative_store();
    for phase in [
        "fingerprinted",
        "imported",
        "prepared:ethereum",
        "indexes_built",
    ] {
        store
            .conn()
            .unwrap()
            .execute(
                &format!("UPDATE {PREPARE_JOURNAL_TABLE} SET phase = ? WHERE journal_id = 1"),
                params![phase],
            )
            .unwrap();
        let error = store.require_prepared_for_chains(&chains).unwrap_err();
        assert!(error.to_string().contains("ready"), "{phase}: {error}");
    }
    store
        .conn()
        .unwrap()
        .execute(
            &format!("UPDATE {PREPARE_JOURNAL_TABLE} SET phase = 'ready' WHERE journal_id = 1"),
            [],
        )
        .unwrap();
    store.require_prepared_for_chains(&chains).unwrap();
}

#[test]
fn resume_from_indexes_built_preserves_or_rebuilds_both_global_indexes() {
    let (_dir, store, chains) = prepared_authoritative_store();
    store
        .conn()
        .unwrap()
        .execute(
            &format!(
                "UPDATE {PREPARE_JOURNAL_TABLE} SET phase = 'indexes_built' WHERE journal_id = 1"
            ),
            [],
        )
        .unwrap();

    store.prepare_recall_for_chains(&chains).unwrap();

    let conn = store.conn().unwrap();
    let index_count: i64 = conn
        .query_row(
            "SELECT count(*) FROM duckdb_indexes()
             WHERE index_name IN ('nft_uri_recall_posting_idx', 'nft_contract_metadata_cursor_idx')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    drop(conn);
    assert_eq!(index_count, 2);
    assert_eq!(
        store.prepare_journal_phase().unwrap().as_deref(),
        Some("ready")
    );
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
fn contract_representative_skips_metadata_without_bm25_terms() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[
                DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "1".into(),
                    name: "Alpha Clone".into(),
                    metadata_json: "{}".into(),
                    ..Default::default()
                },
                DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "2".into(),
                    name: "Alpha Clone".into(),
                    metadata_json: r#"{"description":"gold dragon"}"#.into(),
                    ..Default::default()
                },
            ],
        )
        .unwrap();

    let conn = store.conn().unwrap();
    let metadata_token_id: String = conn
        .query_row(
            "
                SELECT metadata_row.token_id
                FROM nft_contract_representatives r
                JOIN nft_features metadata_row ON metadata_row.rowid = r.metadata_feature_rowid
                WHERE r.chain = 'ethereum'
                ",
            [],
            |row| row.get(0),
        )
        .unwrap();

    assert_eq!(metadata_token_id, "2");
}

#[test]
fn contract_representatives_use_one_grouped_arg_min_query() {
    let sql = DuckDbFeatureStore::contract_representatives_insert_sql();

    assert_eq!(sql.matches("FROM nft_features").count(), 1);
    assert!(sql.contains("GROUP BY chain, contract_address"));
    assert!(sql.contains("arg_min("));
    assert!(sql.contains("FILTER"));
    assert!(!sql.contains("row_number() OVER"));
    assert!(!sql.contains("FULL OUTER JOIN"));
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
            "SELECT count(*) FROM nft_uri_recall_postings WHERE chain = 'ethereum'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let (token_id, recall_doc): (String, String) = conn
        .query_row(
            "
                SELECT token_id, recall_doc
                FROM nft_metadata_recall_docs d
                JOIN nft_features f ON f.rowid = d.feature_rowid
                WHERE d.chain = 'ethereum'
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

    assert_eq!(recall_count, 4);
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
            "SELECT count(*) FROM nft_uri_recall_postings WHERE chain = 'ethereum'",
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
        DuckDbFeatureStore::record_feature_generation(&conn, "ethereum").unwrap();
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
        DuckDbFeatureStore::record_feature_generation(&conn, "ethereum").unwrap();
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
