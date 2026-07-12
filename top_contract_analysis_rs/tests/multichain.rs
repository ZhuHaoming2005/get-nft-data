use std::str::FromStr;

use tempfile::tempdir;
use top_contract_analysis_rs::analysis::multichain::{
    build_scoped_duplicate_scale, parse_alchemy_networks,
};
use top_contract_analysis_rs::analysis::read_seed_contracts;
use top_contract_analysis_rs::models::{
    Chain, ChainTotalsPayload, ContractId, DatabaseNftRecord, PaperDuplicateScaleRowPayload,
    PaperStatsPayload, SeedNft, SingleReportPayload,
};
use top_contract_analysis_rs::store::DuckDbFeatureStore;

#[test]
fn contract_id_normalizes_evm_and_preserves_solana_case() {
    let evm = ContractId::new(
        Chain::from_str("ethereum").unwrap(),
        "0xABCDEFabcdefABCDEFabcdefABCDEFabcdefABCD",
    )
    .unwrap();
    let solana = ContractId::new(
        Chain::from_str("solana").unwrap(),
        "So11111111111111111111111111111111111111112",
    )
    .unwrap();

    assert_eq!(evm.address, "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");
    assert_eq!(
        solana.address,
        "So11111111111111111111111111111111111111112"
    );
}

#[test]
fn read_seed_contracts_parses_mixed_chain_csv() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("seeds.csv");
    std::fs::write(
        &path,
        concat!(
            "chain,address\n",
            "ethereum,0x1111111111111111111111111111111111111111\n",
            "solana,So11111111111111111111111111111111111111112\n"
        ),
    )
    .unwrap();

    let seeds = read_seed_contracts(&path).unwrap();

    assert_eq!(seeds.len(), 2);
    assert_eq!(seeds[0].chain, Chain::Ethereum);
    assert_eq!(seeds[1].chain, Chain::Solana);
}

#[test]
fn read_seed_contracts_rejects_normalized_duplicates() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("seeds.csv");
    std::fs::write(
        &path,
        concat!(
            "chain,address\n",
            "base,0xABCDEFabcdefABCDEFabcdefABCDEFabcdefABCD\n",
            "base,0xabcdefabcdefabcdefabcdefabcdefabcdefabcd\n"
        ),
    )
    .unwrap();

    let error = read_seed_contracts(&path).unwrap_err();

    assert!(error.to_string().contains("duplicate seed contract"));
}

#[test]
fn solana_feature_rows_preserve_base58_case() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    let candidate = "So11111111111111111111111111111111111111112";
    store
        .replace_chain_rows(
            "solana",
            &[DatabaseNftRecord {
                contract_address: candidate.into(),
                token_id: "1".into(),
                token_uri: "ipfs://shared".into(),
                ..DatabaseNftRecord::default()
            }],
        )
        .unwrap();

    let snapshot = store
        .load_snapshot(
            "solana",
            &[SeedNft {
                chain: "solana".into(),
                contract_address: "Vote111111111111111111111111111111111111111".into(),
                token_id: "1".into(),
                token_uri: "ipfs://shared".into(),
                ..SeedNft::default()
            }],
            95.0,
            0.6,
            0,
            0,
        )
        .unwrap();

    assert_eq!(snapshot.nft_rows[0].contract_address, candidate);
}

#[test]
fn solana_recall_excludes_the_case_sensitive_seed_contract_before_row_budgeting() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    let seed = "SeedCaseSensitive111111111111111111111111111";
    let candidate = "ZandidateCaseSensitive1111111111111111111111111";
    store
        .replace_chain_rows(
            "solana",
            &[
                DatabaseNftRecord {
                    contract_address: seed.into(),
                    token_id: "seed-copy".into(),
                    token_uri: "ipfs://shared".into(),
                    ..DatabaseNftRecord::default()
                },
                DatabaseNftRecord {
                    contract_address: candidate.into(),
                    token_id: "candidate".into(),
                    token_uri: "ipfs://shared".into(),
                    ..DatabaseNftRecord::default()
                },
            ],
        )
        .unwrap();

    let snapshot = store
        .load_snapshot(
            "solana",
            &[SeedNft {
                chain: "solana".into(),
                contract_address: seed.into(),
                token_id: "original".into(),
                token_uri: "ipfs://shared".into(),
                ..SeedNft::default()
            }],
            95.0,
            0.6,
            0,
            1,
        )
        .unwrap();

    assert_eq!(snapshot.nft_rows.len(), 1);
    assert_eq!(snapshot.nft_rows[0].contract_address, candidate);
}

#[test]
fn parquet_auto_loader_imports_each_embedded_chain() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("mixed.parquet");
    let conn = duckdb::Connection::open_in_memory().unwrap();
    let path_sql = path
        .to_string_lossy()
        .replace('\\', "/")
        .replace('\'', "''");
    conn.execute_batch(&format!(
        "COPY (SELECT * FROM (VALUES
          ('ethereum', '0x1111111111111111111111111111111111111111', '1', 'ipfs://shared/eth', '', '', '', '', 'ipfs:shared/eth', '', ''),
          ('solana', 'So11111111111111111111111111111111111111112', '1', 'ipfs://shared/sol', '', '', '', '', 'ipfs:shared/sol', '', '')
        ) AS t(chain, contract_address, token_id, token_uri, image_uri, name, symbol, metadata_json, token_uri_norm, image_uri_norm, name_norm))
        TO '{path_sql}' (FORMAT PARQUET)"
    ))
    .unwrap();
    let store = DuckDbFeatureStore::new(":memory:").unwrap();

    let chains = store
        .load_parquet_dataset_auto(path.to_str().unwrap())
        .unwrap();

    assert_eq!(chains, vec![Chain::Ethereum, Chain::Solana]);
    assert!(store.has_chain_rows("ethereum").unwrap());
    assert!(store.has_chain_rows("solana").unwrap());
    assert_eq!(
        store.chain_totals("solana").unwrap(),
        ChainTotalsPayload {
            total_nfts: 1,
            total_contracts: 1,
        }
    );
    let solana = store
        .load_snapshot(
            "solana",
            &[SeedNft {
                chain: "solana".into(),
                token_uri: "ipfs://shared/sol".into(),
                ..SeedNft::default()
            }],
            95.0,
            0.6,
            0,
            0,
        )
        .unwrap();
    assert_eq!(
        solana.nft_rows[0].contract_address,
        "So11111111111111111111111111111111111111112"
    );
}

#[test]
fn parquet_auto_loader_appends_same_chain_shards_without_duplicates() {
    let dir = tempdir().unwrap();
    let first = dir.path().join("ethereum-1.parquet");
    let second = dir.path().join("ethereum-2.parquet");
    let conn = duckdb::Connection::open_in_memory().unwrap();
    for (path, contract) in [
        (&first, "0x1111111111111111111111111111111111111111"),
        (&second, "0x2222222222222222222222222222222222222222"),
    ] {
        let path_sql = path
            .to_string_lossy()
            .replace('\\', "/")
            .replace('\'', "''");
        conn.execute_batch(&format!(
            "COPY (SELECT * FROM (VALUES ('ethereum', '{contract}', '1', '', '', '', '', '', '', '', ''))
             AS t(chain, contract_address, token_id, token_uri, image_uri, name, symbol, metadata_json, token_uri_norm, image_uri_norm, name_norm))
             TO '{path_sql}' (FORMAT PARQUET)"
        ))
        .unwrap();
    }
    let store = DuckDbFeatureStore::new(":memory:").unwrap();

    store
        .load_parquet_dataset_auto(first.to_str().unwrap())
        .unwrap();
    store
        .load_parquet_dataset_auto(second.to_str().unwrap())
        .unwrap();
    store
        .load_parquet_dataset_auto(first.to_str().unwrap())
        .unwrap();

    assert_eq!(
        store.chain_totals("ethereum").unwrap(),
        ChainTotalsPayload {
            total_nfts: 2,
            total_contracts: 2,
        }
    );
}

#[test]
fn parquet_auto_loader_deduplicates_rows_within_one_file() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("duplicates.parquet");
    let conn = duckdb::Connection::open_in_memory().unwrap();
    let path_sql = path
        .to_string_lossy()
        .replace('\\', "/")
        .replace('\'', "''");
    conn.execute_batch(&format!(
        "COPY (SELECT * FROM (VALUES
          ('solana', 'CaseSensitiveContract11111111111111111111111', 'MintOne', 'ipfs://one', '', '', '', '', '', '', ''),
          ('solana', 'CaseSensitiveContract11111111111111111111111', 'MintOne', 'ipfs://duplicate', '', '', '', '', '', '', '')
        ) AS t(chain, contract_address, token_id, token_uri, image_uri, name, symbol, metadata_json, token_uri_norm, image_uri_norm, name_norm))
        TO '{path_sql}' (FORMAT PARQUET)"
    ))
    .unwrap();
    let store = DuckDbFeatureStore::new(":memory:").unwrap();

    store
        .load_parquet_dataset_auto(path.to_str().unwrap())
        .unwrap();

    assert_eq!(store.chain_totals("solana").unwrap().total_nfts, 1);
}

#[test]
fn parquet_bulk_loader_prefers_richer_duplicate_and_updates_existing_row() {
    let dir = tempdir().unwrap();
    let poor = dir.path().join("poor.parquet");
    let rich = dir.path().join("rich.parquet");
    let conn = duckdb::Connection::open_in_memory().unwrap();
    for (path, token_uri, name, metadata) in [
        (&poor, "", "", ""),
        (
            &rich,
            "ipfs://rich",
            "Rich Asset",
            r#"{"description":"complete"}"#,
        ),
    ] {
        let path_sql = path
            .to_string_lossy()
            .replace('\\', "/")
            .replace('\'', "''");
        conn.execute_batch(&format!(
            "COPY (SELECT * FROM (VALUES ('solana', 'CaseSensitiveContract11111111111111111111111', 'MintOne', '{token_uri}', '', '{name}', '', '{metadata}', '{token_uri}', '', 'rich asset'))
             AS t(chain, contract_address, token_id, token_uri, image_uri, name, symbol, metadata_json, token_uri_norm, image_uri_norm, name_norm))
             TO '{path_sql}' (FORMAT PARQUET)"
        ))
        .unwrap();
    }
    let store = DuckDbFeatureStore::new(":memory:").unwrap();

    store
        .load_parquet_datasets_auto(&[poor.to_string_lossy().into_owned()])
        .unwrap();
    store
        .load_parquet_datasets_auto(&[rich.to_string_lossy().into_owned()])
        .unwrap();
    let snapshot = store
        .load_snapshot(
            "solana",
            &[SeedNft {
                chain: "solana".into(),
                token_uri: "ipfs://rich".into(),
                name: "Rich Asset".into(),
                ..SeedNft::default()
            }],
            95.0,
            0.6,
            0,
            0,
        )
        .unwrap();

    assert_eq!(snapshot.nft_rows.len(), 1);
    assert_eq!(snapshot.nft_rows[0].name, "Rich Asset");
}

#[test]
fn single_chain_parquet_loader_prefers_richer_duplicate() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("single-chain-duplicates.parquet");
    let conn = duckdb::Connection::open_in_memory().unwrap();
    let path_sql = path
        .to_string_lossy()
        .replace('\\', "/")
        .replace('\'', "''");
    conn.execute_batch(&format!(
        "COPY (SELECT * FROM (VALUES
          ('ethereum', '0x1111111111111111111111111111111111111111', '1', '', '', '', '', '', '', '', ''),
          ('ethereum', '0x1111111111111111111111111111111111111111', '1', 'ipfs://rich', '', 'Rich Asset', '', '{{\"description\":\"complete\"}}', 'ipfs:rich', '', 'rich asset')
        ) AS t(chain, contract_address, token_id, token_uri, image_uri, name, symbol, metadata_json, token_uri_norm, image_uri_norm, name_norm))
        TO '{path_sql}' (FORMAT PARQUET)"
    ))
    .unwrap();
    let store = DuckDbFeatureStore::new(":memory:").unwrap();

    store
        .load_parquet_dataset("ethereum", path.to_str().unwrap())
        .unwrap();
    let snapshot = store
        .load_snapshot(
            "ethereum",
            &[SeedNft {
                chain: "ethereum".into(),
                token_uri: "ipfs://rich".into(),
                name: "Rich Asset".into(),
                ..SeedNft::default()
            }],
            95.0,
            0.6,
            0,
            0,
        )
        .unwrap();

    assert_eq!(snapshot.nft_rows.len(), 1);
    assert_eq!(snapshot.nft_rows[0].name, "Rich Asset");
}

#[test]
fn parquet_bulk_loader_rejects_missing_identity_columns_explicitly() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("missing-identity.parquet");
    let conn = duckdb::Connection::open_in_memory().unwrap();
    let path_sql = path
        .to_string_lossy()
        .replace('\\', "/")
        .replace('\'', "''");
    conn.execute_batch(&format!(
        "COPY (SELECT '', '', '', '' AS metadata_json, '' AS token_uri_norm, '' AS image_uri_norm, '' AS name_norm)
         TO '{path_sql}' (FORMAT PARQUET)"
    ))
    .unwrap();
    let store = DuckDbFeatureStore::new(":memory:").unwrap();

    let error = store
        .load_parquet_datasets_auto(&[path.to_string_lossy().into_owned()])
        .unwrap_err();

    assert!(error.to_string().contains("chain"));
    assert!(error.to_string().contains("contract_address"));
    assert!(error.to_string().contains("token_id"));
}

#[test]
fn report_v2_serializes_native_amount_names() {
    let mut report = SingleReportPayload::default();
    report.seed_contract.chain = "polygon".into();
    report.paper_stats.attacker_cost.setup_gas_eth = 1.25;

    let value = serde_json::to_value(report).unwrap();

    assert_eq!(value["schema_version"], 2);
    assert_eq!(value["native_symbol"], "POL");
    assert_eq!(
        value["paper_stats"]["attacker_cost"]["setup_gas_native"],
        1.25
    );
    assert!(value.to_string().find("_eth").is_none());
}

#[test]
fn scoped_duplicate_scale_uses_primary_chain_denominators() {
    let stats = |nfts, contracts| PaperStatsPayload {
        duplicate_scale: vec![PaperDuplicateScaleRowPayload {
            category: "total".into(),
            duplicate_nft_count: nfts,
            duplicate_contract_count: contracts,
            ..PaperDuplicateScaleRowPayload::default()
        }],
        ..PaperStatsPayload::default()
    };
    let rows = build_scoped_duplicate_scale(
        Chain::Ethereum,
        &[
            (Chain::Ethereum, stats(10, 2)),
            (Chain::Base, stats(20, 3)),
            (Chain::Polygon, stats(30, 4)),
            (Chain::Solana, stats(40, 5)),
        ],
        ChainTotalsPayload {
            total_nfts: 1_000,
            total_contracts: 100,
        },
    );

    let intra = rows.iter().find(|row| row.scope == "intra_chain").unwrap();
    let cross = rows
        .iter()
        .find(|row| row.scope == "cross_chain_summary")
        .unwrap();
    assert_eq!(intra.duplicate_nft_ratio_denominator, 1_000);
    assert_eq!(intra.duplicate_contract_ratio_denominator, 100);
    assert_eq!(cross.duplicate_nft_count, 90);
    assert_eq!(cross.duplicate_nft_ratio_denominator, 1_000);
    assert_eq!(
        rows.iter()
            .filter(|row| row.scope == "chain_matrix")
            .count(),
        3
    );
}

#[test]
fn alchemy_network_overrides_are_chain_keyed() {
    let networks = parse_alchemy_networks(&[
        "ethereum=eth-mainnet".to_string(),
        "base=base-mainnet".to_string(),
    ])
    .unwrap();

    assert_eq!(networks[&Chain::Ethereum], "eth-mainnet");
    assert_eq!(networks[&Chain::Base], "base-mainnet");
    assert!(
        parse_alchemy_networks(&["base=one".to_string(), "base=two".to_string(),])
            .unwrap_err()
            .to_string()
            .contains("duplicate")
    );
}
