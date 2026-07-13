use super::*;

#[test]
fn prepared_uri_rows_exclude_empty_uris_without_losing_name_recall() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[
                DatabaseNftRecord {
                    contract_address: "0xname-only".into(),
                    token_id: "1".into(),
                    name: "Moon Birds".into(),
                    ..Default::default()
                },
                DatabaseNftRecord {
                    contract_address: "0xuri".into(),
                    token_id: "1".into(),
                    token_uri: "ipfs://uri/1".into(),
                    ..Default::default()
                },
            ],
        )
        .unwrap();
    let conn = store.conn().unwrap();
    let uri_recall_rows = conn
        .query_row(
            "SELECT count(*) FROM nft_uri_recall_postings WHERE chain = 'ethereum'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap();
    drop(conn);

    let snapshot = store
        .load_snapshot(
            "ethereum",
            &[SeedNft {
                contract_address: "0xseed".into(),
                token_id: "99".into(),
                name: "Moon Birds".into(),
                ..Default::default()
            }],
            95.0,
            1.1,
            0,
            0,
        )
        .unwrap();
    let contracts = snapshot
        .nft_rows
        .iter()
        .map(|row| row.contract_address.as_str())
        .collect::<Vec<_>>();

    assert_eq!(uri_recall_rows, 1);
    assert_eq!(contracts, vec!["0xname-only"]);
}

#[test]
fn prepared_uri_index_uses_fixed_width_hash_postings() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[DatabaseNftRecord {
                contract_address: "0xuri".into(),
                token_id: "1".into(),
                token_uri: "ipfs://shared/1".into(),
                image_uri: "ipfs://shared/image".into(),
                ..Default::default()
            }],
        )
        .unwrap();
    let conn = store.conn().unwrap();
    let columns = DuckDbFeatureStore::table_columns(&conn, "nft_uri_recall_postings").unwrap();
    let postings: i64 = conn
        .query_row(
            "SELECT count(*) FROM nft_uri_recall_postings WHERE chain = 'ethereum'",
            [],
            |row| row.get(0),
        )
        .unwrap();

    assert_eq!(
        columns,
        HashSet::from([
            "chain".to_string(),
            "uri_kind".to_string(),
            "uri_hash".to_string(),
            "feature_rowid".to_string(),
        ])
    );
    assert_eq!(postings, 2);
    assert!(!DuckDbFeatureStore::table_exists(&conn, "nft_feature_recall_rows").unwrap());
}

#[test]
fn common_uri_exceeding_candidate_contract_limit_fails_before_payload_loading() {
    let options = DuckDbResourceOptions {
        max_candidate_contracts_per_seed: 1,
        ..Default::default()
    };
    let store = DuckDbFeatureStore::new_with_options(":memory:", options).unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[
                DatabaseNftRecord {
                    contract_address: "0xone".into(),
                    token_id: "1".into(),
                    token_uri: "ipfs://shared".into(),
                    ..Default::default()
                },
                DatabaseNftRecord {
                    contract_address: "0xtwo".into(),
                    token_id: "1".into(),
                    token_uri: "ipfs://shared".into(),
                    ..Default::default()
                },
            ],
        )
        .unwrap();
    store
        .conn()
        .unwrap()
        .execute_batch(
            "ALTER TABLE nft_features RENAME COLUMN metadata_json TO hidden_metadata_json",
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

    assert!(matches!(error, AppError::ResourceLimit(_)), "{error}");
}
