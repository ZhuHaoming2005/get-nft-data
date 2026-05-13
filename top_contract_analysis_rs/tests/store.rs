use std::collections::BTreeMap;

use duckdb::Connection;
use tempfile::tempdir;
use top_contract_analysis_rs::analysis::duplicate::{
    build_duplicate_candidates, build_duplicate_candidates_from_contract_rows,
};
use top_contract_analysis_rs::models::{DatabaseNftRecord, OwnerBalance, SeedNft, TransferRecord};
use top_contract_analysis_rs::store::{
    write_snapshot_rows_to_parquet, ContractSignalCache, DuckDbFeatureStore, DuckDbResourceOptions,
    SnapshotExportRow,
};

fn parquet_path_literal(path: &std::path::Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn write_parquet(sql: &str, path: &std::path::Path) {
    let conn = Connection::open_in_memory().unwrap();
    let path = parquet_path_literal(path);
    conn.execute_batch(&format!("COPY ({sql}) TO '{path}' (FORMAT PARQUET)"))
        .unwrap();
}

#[test]
fn feature_store_builds_contract_duplicate_rows_from_normalized_recall_columns() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[
                DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "2".into(),
                    token_uri: "https://gateway.example/ipfs/seed/meta-1?cache=1".into(),
                    image_uri: "ipfs://dup/image-2.png".into(),
                    name: "Azuki Mirror #2".into(),
                    metadata_json: r#"{"description":"silver cat"}"#.into(),
                    ..Default::default()
                },
                DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "1".into(),
                    token_uri: "ipfs://other/meta-1".into(),
                    image_uri: "https://cdn.example/ipfs/seed/image-1.png".into(),
                    name: "Azuki Mirror #1".into(),
                    metadata_json: r#"{"description":"gold dragon"}"#.into(),
                    ..Default::default()
                },
            ],
        )
        .unwrap();

    let seed_nfts = vec![SeedNft {
        chain: "ethereum".into(),
        contract_address: "0xseed".into(),
        token_id: "1".into(),
        token_uri: "ipfs://seed/meta-1".into(),
        image_uri: "ipfs://seed/image-1.png".into(),
        name: "Azuki #1".into(),
        metadata_json: r#"{"description":"gold dragon"}"#.into(),
        ..Default::default()
    }];

    let snapshot = store
        .load_snapshot("ethereum", &seed_nfts, 95.0, 0.6, 0, 0)
        .unwrap();

    assert_eq!(snapshot.duplicate_contract_rows.len(), 1);
    let row = &snapshot.duplicate_contract_rows[0];
    assert_eq!(row.contract_address, "0xdup");
    assert!(row.token_uri_match);
    assert!(row.image_uri_match);
    assert!(row.name_norms.contains(&"azuki mirror".to_string()));
    assert_eq!(row.representative.token_id, "1");

    let old_path =
        build_duplicate_candidates("ethereum", &seed_nfts, &snapshot.nft_rows, 95.0, 0.6);
    let new_path = build_duplicate_candidates_from_contract_rows(
        "ethereum",
        &seed_nfts,
        &snapshot.duplicate_contract_rows,
        95.0,
        0.6,
    );
    assert_eq!(new_path, old_path);
}

#[test]
fn feature_store_load_snapshots_matches_individual_recall_for_mixed_seed_keys() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[DatabaseNftRecord {
                contract_address: "0xdup".into(),
                token_id: "1".into(),
                token_uri: "ipfs://seed-one/meta-1".into(),
                image_uri: "ipfs://dup/image-1.png".into(),
                name: "Seed One Mirror #1".into(),
                metadata_json: r#"{"description":"silver cat"}"#.into(),
                ..Default::default()
            }],
        )
        .unwrap();

    let seed_one = vec![SeedNft {
        chain: "ethereum".into(),
        contract_address: "0xseed1".into(),
        token_id: "1".into(),
        token_uri: "ipfs://seed-one/meta-1".into(),
        name: "Seed One #1".into(),
        metadata_json: r#"{"description":"gold dragon"}"#.into(),
        ..Default::default()
    }];
    let seed_two = vec![SeedNft {
        chain: "ethereum".into(),
        contract_address: "0xseed2".into(),
        token_id: "1".into(),
        token_uri: "ipfs://seed-two/meta-1".into(),
        name: "Unrelated #1".into(),
        metadata_json: r#"{"description":"silver cat"}"#.into(),
        ..Default::default()
    }];

    let individual_one = store
        .load_snapshot("ethereum", &seed_one, 95.0, 0.6, 0, 0)
        .unwrap();
    let individual_two = store
        .load_snapshot("ethereum", &seed_two, 95.0, 0.6, 0, 0)
        .unwrap();
    let batch = store
        .load_snapshots(
            "ethereum",
            &[("0xseed1".into(), seed_one), ("0xseed2".into(), seed_two)],
            95.0,
            0.6,
            0,
            0,
        )
        .unwrap();

    assert_eq!(batch["0xseed1"], individual_one);
    assert_eq!(batch["0xseed2"], individual_two);
    assert!(batch["0xseed2"].contract_signals["0xdup"].keyword_match);
}

#[test]
fn feature_store_load_snapshots_does_not_mark_invalid_json_strong_rows_as_metadata_matches() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[DatabaseNftRecord {
                contract_address: "0xdup".into(),
                token_id: "1".into(),
                token_uri: "ipfs://seed/meta-1".into(),
                metadata_json: "not json metadata".into(),
                ..Default::default()
            }],
        )
        .unwrap();
    let seed = vec![SeedNft {
        chain: "ethereum".into(),
        contract_address: "0xseed".into(),
        token_id: "1".into(),
        token_uri: "ipfs://seed/meta-1".into(),
        metadata_json: r#"{"description":"json metadata"}"#.into(),
        ..Default::default()
    }];
    let other_seed = vec![SeedNft {
        chain: "ethereum".into(),
        contract_address: "0xotherseed".into(),
        token_id: "1".into(),
        token_uri: "ipfs://other/meta-1".into(),
        metadata_json: r#"{"description":"other metadata"}"#.into(),
        ..Default::default()
    }];

    let batch = store
        .load_snapshots(
            "ethereum",
            &[("0xseed".into(), seed), ("0xotherseed".into(), other_seed)],
            95.0,
            0.6,
            0,
            0,
        )
        .unwrap();

    assert!(!batch["0xseed"].contract_signals["0xdup"].keyword_match);
    assert!(!batch["0xseed"].nft_rows[0].metadata_recall_match);
}

#[test]
fn feature_store_marks_exact_uri_rows_that_also_pass_metadata_recall() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[DatabaseNftRecord {
                contract_address: "0xdup".into(),
                token_id: "1".into(),
                token_uri: "ipfs://seed/meta-1".into(),
                metadata_json: r#"{"description":"gold dragon"}"#.into(),
                ..Default::default()
            }],
        )
        .unwrap();
    let seed_nfts = vec![SeedNft {
        chain: "ethereum".into(),
        contract_address: "0xseed".into(),
        token_id: "1".into(),
        token_uri: "ipfs://seed/meta-1".into(),
        metadata_json: r#"{"description":"gold dragon"}"#.into(),
        ..Default::default()
    }];

    let snapshot = store
        .load_snapshot("ethereum", &seed_nfts, 95.0, 0.6, 0, 0)
        .unwrap();

    assert_eq!(snapshot.nft_rows.len(), 1);
    assert!(snapshot.nft_rows[0].metadata_recall_match);
    assert!(snapshot.contract_signals["0xdup"].keyword_match);
    assert!(snapshot.duplicate_contract_rows[0].metadata_recall_match);
}

#[test]
fn feature_store_marks_name_rows_that_also_pass_metadata_recall() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[DatabaseNftRecord {
                contract_address: "0xname".into(),
                token_id: "1".into(),
                name: "Azuki #1".into(),
                metadata_json: r#"{"description":"gold dragon"}"#.into(),
                ..Default::default()
            }],
        )
        .unwrap();
    let seed_nfts = vec![SeedNft {
        chain: "ethereum".into(),
        contract_address: "0xseed".into(),
        token_id: "1".into(),
        name: "Azuki #1".into(),
        metadata_json: r#"{"description":"gold dragon"}"#.into(),
        ..Default::default()
    }];

    let snapshot = store
        .load_snapshot("ethereum", &seed_nfts, 95.0, 0.6, 0, 0)
        .unwrap();

    assert_eq!(snapshot.nft_rows.len(), 1);
    assert!(snapshot.contract_signals["0xname"].name_prefix_match);
    assert!(snapshot.nft_rows[0].metadata_recall_match);
    assert!(snapshot.contract_signals["0xname"].keyword_match);
}

#[test]
fn feature_store_keeps_one_overlapping_metadata_row_for_final_duplicate_recheck() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[
                DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "1".into(),
                    metadata_json: r#"{"description":"alpha beta"}"#.into(),
                    ..Default::default()
                },
                DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "2".into(),
                    metadata_json: r#"{"description":"silver cat"}"#.into(),
                    ..Default::default()
                },
            ],
        )
        .unwrap();

    let seed_nfts = vec![
        SeedNft {
            chain: "ethereum".into(),
            contract_address: "0xseed".into(),
            token_id: "1".into(),
            metadata_json: r#"{"description":"alpha beta"}"#.into(),
            ..Default::default()
        },
        SeedNft {
            chain: "ethereum".into(),
            contract_address: "0xseed".into(),
            token_id: "2".into(),
            metadata_json: r#"{"description":"gold dragon"}"#.into(),
            ..Default::default()
        },
    ];

    let snapshot = store
        .load_snapshot("ethereum", &seed_nfts, 95.0, 0.6, 0, 0)
        .unwrap();

    assert_eq!(snapshot.duplicate_contract_rows.len(), 1);
    let metadata_token_ids = snapshot.duplicate_contract_rows[0]
        .metadata_token_rows
        .iter()
        .map(|row| row.token_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(metadata_token_ids, vec!["1"]);
}

#[test]
fn feature_store_recalls_metadata_candidates_from_representative_prefilter_keys() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[DatabaseNftRecord {
                contract_address: "0xprefilter".into(),
                token_id: "1".into(),
                metadata_json: r#"{"name":"Clone Beta","image":"ar://clonebeta"}"#.into(),
                ..Default::default()
            }],
        )
        .unwrap();

    let snapshot = store
        .load_snapshot(
            "ethereum",
            &[SeedNft {
                chain: "ethereum".into(),
                contract_address: "0xseed".into(),
                token_id: "1".into(),
                metadata_json: r#"{"name":"Seed Alpha","image":"ipfs://seedalpha"}"#.into(),
                ..Default::default()
            }],
            95.0,
            0.6,
            0,
            0,
        )
        .unwrap();

    assert_eq!(snapshot.duplicate_contract_rows.len(), 1);
    assert_eq!(
        snapshot.duplicate_contract_rows[0].contract_address,
        "0xprefilter"
    );
    assert!(snapshot.contract_signals["0xprefilter"].keyword_match);
}

#[test]
fn feature_store_recalls_name_candidates_by_similarity_without_prefix_overlap() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[DatabaseNftRecord {
                contract_address: "0xname".into(),
                token_id: "1".into(),
                name: "Azzuki #1".into(),
                ..Default::default()
            }],
        )
        .unwrap();

    let snapshot = store
        .load_snapshot(
            "ethereum",
            &[SeedNft {
                chain: "ethereum".into(),
                contract_address: "0xseed".into(),
                token_id: "1".into(),
                name: "Azuki #1".into(),
                ..Default::default()
            }],
            95.0,
            0.6,
            0,
            0,
        )
        .unwrap();

    assert_eq!(snapshot.nft_rows.len(), 1);
    assert_eq!(snapshot.nft_rows[0].contract_address, "0xname");
    assert!(snapshot.contract_signals["0xname"].name_prefix_match);
}

#[test]
fn feature_store_recalls_metadata_candidates_from_sketch_without_keyword_terms() {
    let dir = tempdir().unwrap();
    let parquet_path = dir.path().join("snapshot.parquet");
    write_parquet(
        "
        SELECT
            'ethereum' AS chain,
            '0xmetadata' AS contract_address,
            '1' AS token_id,
            '' AS token_uri,
            '' AS image_uri,
            '' AS name,
            '' AS symbol,
            '{\"description\":\"gold dragon\"}' AS metadata_json,
            '' AS token_uri_norm,
            '' AS image_uri_norm,
            '' AS name_norm
        ",
        &parquet_path,
    );

    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .load_parquet_dataset("ethereum", &parquet_path.to_string_lossy())
        .unwrap();

    let snapshot = store
        .load_snapshot(
            "ethereum",
            &[SeedNft {
                chain: "ethereum".into(),
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

    assert_eq!(snapshot.nft_rows.len(), 1);
    assert_eq!(snapshot.nft_rows[0].contract_address, "0xmetadata");
    assert!(snapshot.nft_rows[0].metadata_recall_match);
    assert!(snapshot.contract_signals["0xmetadata"].keyword_match);
}

#[test]
fn feature_store_uses_representative_seed_metadata_for_template_recall() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[DatabaseNftRecord {
                contract_address: "0xtemplate".into(),
                token_id: "2".into(),
                metadata_json: r#"{"attributes":[{"trait_type":"Background","value":"Red"}],"image":"ipfs://shared/2.png"}"#.into(),
                ..Default::default()
            }],
        )
        .unwrap();

    let seed_nfts = vec![
        SeedNft {
            chain: "ethereum".into(),
            contract_address: "0xseed".into(),
            token_id: "1".into(),
            metadata_json: r#"{"description":"unrelated alpha"}"#.into(),
            ..Default::default()
        },
        SeedNft {
            chain: "ethereum".into(),
            contract_address: "0xseed".into(),
            token_id: "2".into(),
            metadata_json: r#"{"attributes":[{"trait_type":"Background","value":"Red"}],"image":"ipfs://shared/2.png"}"#.into(),
            ..Default::default()
        },
    ];

    let snapshot = store
        .load_snapshot("ethereum", &seed_nfts, 95.0, 0.6, 0, 0)
        .unwrap();

    assert!(snapshot.duplicate_contract_rows.is_empty());
    assert!(!snapshot.contract_signals.contains_key("0xtemplate"));

    let candidates = build_duplicate_candidates_from_contract_rows(
        "ethereum",
        &seed_nfts,
        &snapshot.duplicate_contract_rows,
        95.0,
        0.9,
    );
    assert!(candidates.is_empty());
}

#[test]
fn feature_store_limits_overlapping_metadata_rows_per_candidate_contract() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[
                DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "0".into(),
                    token_uri: "ipfs://shared-recall".into(),
                    ..Default::default()
                },
                DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "1".into(),
                    metadata_json: r#"{"description":"alpha beta"}"#.into(),
                    ..Default::default()
                },
                DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "2".into(),
                    metadata_json: r#"{"description":"gold dragon"}"#.into(),
                    ..Default::default()
                },
            ],
        )
        .unwrap();

    let seed_nfts = vec![
        SeedNft {
            chain: "ethereum".into(),
            contract_address: "0xseed".into(),
            token_id: "1".into(),
            token_uri: "ipfs://shared-recall".into(),
            metadata_json: r#"{"description":"alpha beta"}"#.into(),
            ..Default::default()
        },
        SeedNft {
            chain: "ethereum".into(),
            contract_address: "0xseed".into(),
            token_id: "2".into(),
            metadata_json: r#"{"description":"gold dragon"}"#.into(),
            ..Default::default()
        },
    ];

    let snapshot = store
        .load_snapshot("ethereum", &seed_nfts, 95.0, 0.6, 0, 0)
        .unwrap();

    assert_eq!(snapshot.duplicate_contract_rows.len(), 1);
    let metadata_token_ids = snapshot.duplicate_contract_rows[0]
        .metadata_token_rows
        .iter()
        .map(|row| row.token_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(metadata_token_ids, vec!["1"]);
}

#[test]
fn feature_store_prefers_json_overlapping_metadata_row_for_final_recheck() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[
                DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "0".into(),
                    token_uri: "ipfs://shared-recall".into(),
                    ..Default::default()
                },
                DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "1".into(),
                    ..Default::default()
                },
                DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "2".into(),
                    metadata_json: r#"{"description":"gold dragon"}"#.into(),
                    ..Default::default()
                },
            ],
        )
        .unwrap();

    let seed_nfts = vec![
        SeedNft {
            chain: "ethereum".into(),
            contract_address: "0xseed".into(),
            token_id: "1".into(),
            token_uri: "ipfs://shared-recall".into(),
            ..Default::default()
        },
        SeedNft {
            chain: "ethereum".into(),
            contract_address: "0xseed".into(),
            token_id: "2".into(),
            metadata_json: r#"{"description":"gold dragon"}"#.into(),
            ..Default::default()
        },
    ];

    let snapshot = store
        .load_snapshot("ethereum", &seed_nfts, 95.0, 0.6, 0, 0)
        .unwrap();

    assert_eq!(snapshot.duplicate_contract_rows.len(), 1);
    let metadata_token_ids = snapshot.duplicate_contract_rows[0]
        .metadata_token_rows
        .iter()
        .map(|row| row.token_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(metadata_token_ids, vec!["2"]);
}

#[test]
fn parquet_rejects_missing_precomputed_columns() {
    let dir = tempdir().unwrap();
    let parquet_path = dir.path().join("missing_columns.parquet");
    write_parquet(
        "
        SELECT
            'ethereum' AS chain,
            '0xdup' AS contract_address,
            '1' AS token_id,
            'ipfs://seed/meta-1' AS token_uri,
            'ipfs://dup/image-1.png' AS image_uri,
            'Azuki Mirror #1' AS name,
            'AZUKI' AS symbol,
            '{\"description\":\"red hooded anime portrait\"}' AS metadata_json
        ",
        &parquet_path,
    );

    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    let err = store
        .load_parquet_dataset("ethereum", &parquet_path.to_string_lossy())
        .unwrap_err();

    assert!(err.to_string().contains("missing pre-computed columns"));
}

#[test]
fn existing_current_feature_db_chain_rows_take_priority_over_parquet() {
    let dir = tempdir().unwrap();
    let parquet_path = dir.path().join("snapshot.parquet");
    write_parquet(
        "
        SELECT
            'ethereum' AS chain,
            '0xparquet' AS contract_address,
            '1' AS token_id,
            'ipfs://seed/meta-1' AS token_uri,
            '' AS image_uri,
            'Parquet Clone #1' AS name,
            'AZUKI' AS symbol,
            '' AS metadata_json,
            'ipfs:seed/meta-1' AS token_uri_norm,
            '' AS image_uri_norm,
            'parquet clone' AS name_norm
        ",
        &parquet_path,
    );

    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[DatabaseNftRecord {
                contract_address: "0xdb".into(),
                token_id: "1".into(),
                token_uri: "ipfs://seed/meta-1".into(),
                image_uri: "".into(),
                name: "Db Clone #1".into(),
                symbol: "AZUKI".into(),
                metadata_json: "".into(),
                metadata_recall_checked: false,
                metadata_recall_match: false,
            }],
        )
        .unwrap();

    let loaded = store
        .load_parquet_dataset_if_chain_missing("ethereum", &parquet_path.to_string_lossy())
        .unwrap();
    assert!(!loaded);

    let snapshot = store
        .load_snapshot(
            "ethereum",
            &[SeedNft {
                chain: "ethereum".into(),
                contract_address: "0xseed".into(),
                token_id: "1".into(),
                name: "Azuki #1".into(),
                symbol: "AZUKI".into(),
                token_uri: "ipfs://seed/meta-1".into(),
                image_uri: "".into(),
                metadata_json: "".into(),
            }],
            95.0,
            0.6,
            0,
            0,
        )
        .unwrap();

    let contracts: Vec<&str> = snapshot
        .nft_rows
        .iter()
        .map(|row| row.contract_address.as_str())
        .collect();
    assert_eq!(contracts, vec!["0xdb"]);
}

#[test]
fn parquet_loads_when_feature_db_has_no_chain_rows() {
    let dir = tempdir().unwrap();
    let parquet_path = dir.path().join("snapshot.parquet");
    write_parquet(
        "
        SELECT
            'ethereum' AS chain,
            '0xparquet' AS contract_address,
            '1' AS token_id,
            'ipfs://seed/meta-1' AS token_uri,
            '' AS image_uri,
            'Parquet Clone #1' AS name,
            'AZUKI' AS symbol,
            '' AS metadata_json,
            'ipfs:seed/meta-1' AS token_uri_norm,
            '' AS image_uri_norm,
            'parquet clone' AS name_norm
        ",
        &parquet_path,
    );

    let store = DuckDbFeatureStore::new(":memory:").unwrap();

    let loaded = store
        .load_parquet_dataset_if_chain_missing("ethereum", &parquet_path.to_string_lossy())
        .unwrap();
    assert!(loaded);

    let snapshot = store
        .load_snapshot(
            "ethereum",
            &[SeedNft {
                chain: "ethereum".into(),
                contract_address: "0xseed".into(),
                token_id: "1".into(),
                name: "Azuki #1".into(),
                symbol: "AZUKI".into(),
                token_uri: "ipfs://seed/meta-1".into(),
                image_uri: "".into(),
                metadata_json: "".into(),
            }],
            95.0,
            0.6,
            0,
            0,
        )
        .unwrap();

    let contracts: Vec<&str> = snapshot
        .nft_rows
        .iter()
        .map(|row| row.contract_address.as_str())
        .collect();
    assert_eq!(contracts, vec!["0xparquet"]);
}

#[test]
fn feature_db_schema_does_not_require_legacy_precomputed_columns() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("old.duckdb");
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
            );
            ",
        )
        .unwrap();
    }

    DuckDbFeatureStore::new(&db_path.to_string_lossy()).unwrap();
}

#[test]
fn feature_db_schema_rejects_missing_current_precomputed_columns() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("old.duckdb");
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
                image_uri_norm VARCHAR
            );
            ",
        )
        .unwrap();
    }

    let err = match DuckDbFeatureStore::new(&db_path.to_string_lossy()) {
        Ok(_) => panic!("feature DB schema without current precomputed columns should be rejected"),
        Err(err) => err,
    };

    assert!(err
        .to_string()
        .contains("missing current pre-computed columns"));
}

#[test]
fn feature_store_applies_duckdb_resource_options() {
    let options = DuckDbResourceOptions {
        threads: 2,
        memory_limit: "2GB".into(),
    };
    let store = DuckDbFeatureStore::new_with_options(":memory:", options.clone()).unwrap();

    assert_eq!(store.resource_options(), &options);
}

#[test]
fn feature_store_applies_per_contract_token_cap() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[
                DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "1".into(),
                    token_uri: "ipfs://seed/meta-1".into(),
                    image_uri: "".into(),
                    name: "Clone #1".into(),
                    symbol: "AZUKI".into(),
                    metadata_json: "".into(),
                    metadata_recall_checked: false,
                    metadata_recall_match: false,
                },
                DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "2".into(),
                    token_uri: "ipfs://seed/meta-1".into(),
                    image_uri: "".into(),
                    name: "Clone #2".into(),
                    symbol: "AZUKI".into(),
                    metadata_json: "".into(),
                    metadata_recall_checked: false,
                    metadata_recall_match: false,
                },
                DatabaseNftRecord {
                    contract_address: "0xother".into(),
                    token_id: "1".into(),
                    token_uri: "ipfs://seed/meta-1".into(),
                    image_uri: "".into(),
                    name: "Other Clone #1".into(),
                    symbol: "AZUKI".into(),
                    metadata_json: "".into(),
                    metadata_recall_checked: false,
                    metadata_recall_match: false,
                },
            ],
        )
        .unwrap();

    let snapshot = store
        .load_snapshot(
            "ethereum",
            &[SeedNft {
                chain: "ethereum".into(),
                contract_address: "0xseed".into(),
                token_id: "1".into(),
                name: "Azuki #1".into(),
                symbol: "AZUKI".into(),
                token_uri: "ipfs://seed/meta-1".into(),
                image_uri: "".into(),
                metadata_json: "".into(),
            }],
            95.0,
            0.6,
            1,
            0,
        )
        .unwrap();

    assert_eq!(
        snapshot
            .nft_rows
            .iter()
            .filter(|row| row.contract_address == "0xdup")
            .count(),
        1
    );
    assert_eq!(snapshot.contract_signals["0xdup"].token_count, 1);
}

#[test]
fn feature_store_does_not_recall_metadata_candidates_without_valid_json() {
    let dir = tempdir().unwrap();
    let parquet_path = dir.path().join("snapshot.parquet");
    write_parquet(
        "
        SELECT
            'ethereum' AS chain,
            '0xkeyword' AS contract_address,
            '1' AS token_id,
            '' AS token_uri,
            '' AS image_uri,
            '' AS name,
            '' AS symbol,
            '' AS metadata_json,
            '' AS token_uri_norm,
            '' AS image_uri_norm,
            '' AS name_norm
        ",
        &parquet_path,
    );

    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .load_parquet_dataset("ethereum", &parquet_path.to_string_lossy())
        .unwrap();

    let snapshot = store
        .load_snapshot(
            "ethereum",
            &[SeedNft {
                chain: "ethereum".into(),
                contract_address: "0xseed".into(),
                token_id: "1".into(),
                metadata_json: r#"{"description":"cat"}"#.into(),
                ..Default::default()
            }],
            95.0,
            0.6,
            0,
            0,
        )
        .unwrap();

    assert!(snapshot.nft_rows.is_empty());
}

#[test]
fn feature_store_recalls_parquet_metadata_candidates_from_seed_sketch() {
    let dir = tempdir().unwrap();
    let parquet_path = dir.path().join("snapshot.parquet");
    write_parquet(
        "
        SELECT
            'ethereum' AS chain,
            '0xkeyword' AS contract_address,
            '1' AS token_id,
            '' AS token_uri,
            '' AS image_uri,
            '' AS name,
            '' AS symbol,
            '{\"description\":\"gold dragon\"}' AS metadata_json,
            '' AS token_uri_norm,
            '' AS image_uri_norm,
            '' AS name_norm
        ",
        &parquet_path,
    );

    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .load_parquet_dataset("ethereum", &parquet_path.to_string_lossy())
        .unwrap();

    let snapshot = store
        .load_snapshot(
            "ethereum",
            &[SeedNft {
                chain: "ethereum".into(),
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

    let contracts = snapshot
        .nft_rows
        .iter()
        .map(|row| row.contract_address.as_str())
        .collect::<Vec<_>>();
    assert_eq!(contracts, vec!["0xkeyword"]);
    assert!(snapshot.contract_signals["0xkeyword"].keyword_match);
}

#[test]
fn feature_store_recalls_short_metadata_tokens() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[
                DatabaseNftRecord {
                    contract_address: "0xshort".into(),
                    token_id: "1".into(),
                    metadata_json: r#"{"description":"ai cat"}"#.into(),
                    ..Default::default()
                },
                DatabaseNftRecord {
                    contract_address: "0xmiss".into(),
                    token_id: "1".into(),
                    ..Default::default()
                },
            ],
        )
        .unwrap();

    let snapshot = store
        .load_snapshot(
            "ethereum",
            &[SeedNft {
                chain: "ethereum".into(),
                contract_address: "0xseed".into(),
                token_id: "1".into(),
                metadata_json: r#"{"description":"ai cat"}"#.into(),
                ..Default::default()
            }],
            95.0,
            0.6,
            0,
            0,
        )
        .unwrap();

    let contracts: Vec<&str> = snapshot
        .nft_rows
        .iter()
        .map(|row| row.contract_address.as_str())
        .collect();
    assert_eq!(contracts, vec!["0xshort"]);
    assert!(snapshot.contract_signals["0xshort"].keyword_match);
}

#[test]
fn feature_store_uses_full_representative_metadata_document_for_sketch_recall() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    let representative_description = "alexandria boulevard charleston driftwood everglades fountainhead grandmaster hemisphere illuminate jewelcraft knowledgebase luminescence zz";
    store
        .replace_chain_rows(
            "ethereum",
            &[DatabaseNftRecord {
                contract_address: "0xlate".into(),
                token_id: "1".into(),
                metadata_json: format!(r#"{{"description":"{representative_description}"}}"#),
                ..Default::default()
            }],
        )
        .unwrap();

    let snapshot = store
        .load_snapshot(
            "ethereum",
            &[SeedNft {
                chain: "ethereum".into(),
                contract_address: "0xseed".into(),
                token_id: "1".into(),
                metadata_json: format!(r#"{{"description":"{representative_description}"}}"#),
                ..Default::default()
            }],
            95.0,
            0.6,
            0,
            0,
        )
        .unwrap();

    let contracts: Vec<&str> = snapshot
        .nft_rows
        .iter()
        .map(|row| row.contract_address.as_str())
        .collect();
    assert_eq!(contracts, vec!["0xlate"]);
    assert!(snapshot.contract_signals["0xlate"].keyword_match);
}

#[test]
fn feature_store_applies_metadata_only_cap_after_source_prefilter() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[
                DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "2".into(),
                    metadata_json: r#"{"description":"gold dragon"}"#.into(),
                    ..Default::default()
                },
                DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "1".into(),
                    metadata_json: r#"{"description":"gold dragon"}"#.into(),
                    ..Default::default()
                },
            ],
        )
        .unwrap();

    let snapshot = store
        .load_snapshot(
            "ethereum",
            &[SeedNft {
                chain: "ethereum".into(),
                contract_address: "0xseed".into(),
                token_id: "1".into(),
                metadata_json: r#"{"description":"gold dragon"}"#.into(),
                ..Default::default()
            }],
            95.0,
            0.6,
            1,
            0,
        )
        .unwrap();

    assert_eq!(snapshot.nft_rows.len(), 1);
    assert_eq!(snapshot.nft_rows[0].contract_address, "0xdup");
    assert_eq!(snapshot.nft_rows[0].token_id, "1");
    assert_eq!(
        snapshot.duplicate_contract_rows[0].representative.token_id,
        "1"
    );
}

#[test]
fn feature_store_prioritizes_strong_signal_rows_over_metadata_only_cap_rows() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[
                DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "1".into(),
                    metadata_json: r#"{"description":"template noise"}"#.into(),
                    ..Default::default()
                },
                DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "2".into(),
                    token_uri: "ipfs://shared-token".into(),
                    ..Default::default()
                },
            ],
        )
        .unwrap();

    let snapshot = store
        .load_snapshot(
            "ethereum",
            &[SeedNft {
                chain: "ethereum".into(),
                contract_address: "0xseed".into(),
                token_id: "1".into(),
                token_uri: "ipfs://shared-token".into(),
                metadata_json: r#"{"description":"template"}"#.into(),
                ..Default::default()
            }],
            95.0,
            0.6,
            1,
            0,
        )
        .unwrap();

    assert_eq!(snapshot.duplicate_contract_rows.len(), 1);
    let row = &snapshot.duplicate_contract_rows[0];
    assert_eq!(row.contract_address, "0xdup");
    assert!(row.token_uri_match);
    assert_eq!(row.representative.token_id, "2");
}

#[test]
fn feature_store_uses_max_recall_rows_as_batch_size_after_sql_recall() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[
                DatabaseNftRecord {
                    contract_address: "0xone".into(),
                    token_id: "1".into(),
                    metadata_json: r#"{"description":"gold dragon"}"#.into(),
                    ..Default::default()
                },
                DatabaseNftRecord {
                    contract_address: "0xtwo".into(),
                    token_id: "1".into(),
                    metadata_json: r#"{"description":"gold dragon"}"#.into(),
                    ..Default::default()
                },
                DatabaseNftRecord {
                    contract_address: "0xthree".into(),
                    token_id: "1".into(),
                    ..Default::default()
                },
            ],
        )
        .unwrap();

    let snapshot = store
        .load_snapshot(
            "ethereum",
            &[SeedNft {
                chain: "ethereum".into(),
                contract_address: "0xseed".into(),
                token_id: "1".into(),
                metadata_json: r#"{"description":"gold dragon"}"#.into(),
                ..Default::default()
            }],
            95.0,
            0.6,
            0,
            1,
        )
        .unwrap();

    let contracts: Vec<_> = snapshot
        .nft_rows
        .iter()
        .map(|row| row.contract_address.as_str())
        .collect();
    assert_eq!(contracts, vec!["0xone", "0xtwo"]);
}

#[test]
fn signal_cache_round_trips_transfers_and_owners() {
    let cache = ContractSignalCache::new(":memory:").unwrap();
    let transfers = vec![
        TransferRecord::mint("0xdup", "1", 100, "0xminter"),
        TransferRecord::transfer("0xdup", "1", 160, "0xminter", "0xbuyer"),
    ];
    let owners = vec![OwnerBalance {
        owner_address: "0xbuyer".into(),
        token_balances: BTreeMap::from([("1".into(), 1)]),
    }];

    cache
        .put("ethereum", "0xdup", "ERC721", &transfers, &owners)
        .unwrap();
    let cached = cache.get("ethereum", "0xdup", "ERC721").unwrap().unwrap();

    assert_eq!(cached.transfers.len(), 2);
    assert_eq!(cached.owners.len(), 1);
    assert_eq!(cached.mint_recipients, vec!["0xminter"]);
    assert_eq!(cached.active_sellers, vec!["0xminter"]);
    assert_eq!(cached.address_signals.mint_count, 1);
    assert_eq!(cached.victim_signals.unwrap().owner_count, 1);
}

#[test]
fn snapshot_export_writes_current_precomputed_columns_without_legacy_fields() {
    let dir = tempdir().unwrap();
    let parquet_path = dir.path().join("snapshot.parquet");
    write_snapshot_rows_to_parquet(
        "ethereum",
        &[SnapshotExportRow {
            contract_address: "0xdup".into(),
            token_id: "1".into(),
            token_uri: "ipfs://seed/meta-1".into(),
            image_uri: "ipfs://dup/image-1.png".into(),
            name: "Azuki Mirror #1".into(),
            symbol: "AZUKI".into(),
            metadata_json: "{\"description\":\"red hooded anime portrait\"}".into(),
        }],
        &parquet_path,
        true,
    )
    .unwrap();

    let conn = Connection::open_in_memory().unwrap();
    let path = parquet_path_literal(&parquet_path);
    let mut describe_stmt = conn
        .prepare(&format!("DESCRIBE SELECT * FROM read_parquet('{path}')"))
        .unwrap();
    let describe_rows = describe_stmt
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap();
    let mut column_names = Vec::new();
    for row in describe_rows {
        column_names.push(row.unwrap());
    }

    let (metadata_json, token_uri_norm, name_norm): (String, String, String) = conn
        .query_row(
            &format!("SELECT metadata_json, token_uri_norm, name_norm FROM read_parquet('{path}')"),
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();

    assert!(metadata_json.contains("red hooded anime portrait"));
    assert_eq!(token_uri_norm, "ipfs:seed/meta-1");
    assert_eq!(name_norm, "azuki mirror");
    assert!(!column_names.contains(&"name_prefix8".to_string()));
    assert!(!column_names.contains(&"metadata_keywords_arr".to_string()));
}
