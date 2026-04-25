use std::collections::BTreeMap;

use duckdb::Connection;
use tempfile::tempdir;
use top_contract_analysis_rs::models::{DatabaseNftRecord, OwnerBalance, SeedNft, TransferRecord};
use top_contract_analysis_rs::store::{
    write_snapshot_rows_to_parquet, ContractSignalCache, DuckDbFeatureStore, SnapshotExportRow,
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
            'parquet clone' AS name_norm,
            'azuki' AS symbol_norm,
            '' AS metadata_doc,
            '[]' AS metadata_keywords_arr
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
                metadata_doc: "".into(),
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
                metadata_doc: "".into(),
            }],
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
            'parquet clone' AS name_norm,
            'azuki' AS symbol_norm,
            '' AS metadata_doc,
            '[]' AS metadata_keywords_arr
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
                metadata_doc: "".into(),
            }],
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
fn old_feature_db_schema_is_rejected() {
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
                symbol_norm VARCHAR,
                metadata_doc VARCHAR
            );
            ",
        )
        .unwrap();
    }

    let err = match DuckDbFeatureStore::new(&db_path.to_string_lossy()) {
        Ok(_) => panic!("old feature DB schema should be rejected"),
        Err(err) => err,
    };

    assert!(err
        .to_string()
        .contains("missing current pre-computed columns"));
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
                    metadata_doc: "".into(),
                },
                DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "2".into(),
                    token_uri: "ipfs://seed/meta-1".into(),
                    image_uri: "".into(),
                    name: "Clone #2".into(),
                    symbol: "AZUKI".into(),
                    metadata_json: "".into(),
                    metadata_doc: "".into(),
                },
                DatabaseNftRecord {
                    contract_address: "0xother".into(),
                    token_id: "1".into(),
                    token_uri: "ipfs://seed/meta-1".into(),
                    image_uri: "".into(),
                    name: "Other Clone #1".into(),
                    symbol: "AZUKI".into(),
                    metadata_json: "".into(),
                    metadata_doc: "".into(),
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
                metadata_doc: "".into(),
            }],
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
fn feature_store_recalls_short_metadata_terms() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[
                DatabaseNftRecord {
                    contract_address: "0xshort".into(),
                    token_id: "1".into(),
                    metadata_doc: "ai cat".into(),
                    ..Default::default()
                },
                DatabaseNftRecord {
                    contract_address: "0xmiss".into(),
                    token_id: "1".into(),
                    metadata_doc: "dog".into(),
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
                metadata_doc: "ai cat".into(),
                ..Default::default()
            }],
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
fn feature_store_applies_total_recall_limit_after_sql_recall() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[
                DatabaseNftRecord {
                    contract_address: "0xone".into(),
                    token_id: "1".into(),
                    symbol: "AZUKI".into(),
                    ..Default::default()
                },
                DatabaseNftRecord {
                    contract_address: "0xtwo".into(),
                    token_id: "1".into(),
                    symbol: "AZUKI".into(),
                    ..Default::default()
                },
                DatabaseNftRecord {
                    contract_address: "0xthree".into(),
                    token_id: "1".into(),
                    symbol: "OTHER".into(),
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
                symbol: "AZUKI".into(),
                ..Default::default()
            }],
            0,
            1,
        )
        .unwrap();

    assert_eq!(snapshot.nft_rows.len(), 1);
    assert_eq!(snapshot.nft_rows[0].contract_address, "0xone");
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
fn snapshot_export_writes_precomputed_columns() {
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
    let (metadata_json, token_uri_norm, metadata_doc): (String, String, String) = conn
        .query_row(
            &format!(
                "SELECT metadata_json, token_uri_norm, metadata_doc FROM read_parquet('{path}')"
            ),
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();

    assert!(metadata_json.contains("red hooded anime portrait"));
    assert_eq!(token_uri_norm, "ipfs:seed/meta-1");
    assert_eq!(metadata_doc, "red hooded anime portrait");
}
