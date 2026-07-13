use assert_cmd::Command;
use clap::Parser;
use duckdb::Connection;
use predicates::prelude::PredicateBooleanExt;
use predicates::str::contains;
use top_contract_analysis_rs::api::DEFAULT_OTHER_API_RATE_LIMIT_INTERVAL_MS;
use top_contract_analysis_rs::cli::{Command as CliCommand, TopContractAnalysisCli};
use top_contract_analysis_rs::models::SeedNft;
use top_contract_analysis_rs::store::{DuckDbFeatureStore, DuckDbResourceOptions};

#[test]
fn cli_defaults_other_api_rate_limit_burst_to_four() {
    let analyze = TopContractAnalysisCli::parse_from([
        "top_contract_analysis_rs",
        "analyze",
        "--seed-contract-address",
        "0xseed",
    ]);
    let CliCommand::Analyze(analyze_args) = analyze.command else {
        panic!("expected analyze command");
    };
    assert_eq!(analyze_args.other_api_max_concurrency, 4);
    assert_eq!(
        analyze_args.other_api_rate_limit_refill_ms,
        DEFAULT_OTHER_API_RATE_LIMIT_INTERVAL_MS
    );

    let batch = TopContractAnalysisCli::parse_from([
        "top_contract_analysis_rs",
        "batch",
        "--seed-file",
        "seeds.txt",
    ]);
    let CliCommand::Batch(batch_args) = batch.command else {
        panic!("expected batch command");
    };
    assert_eq!(batch_args.other_api_max_concurrency, 4);
    assert_eq!(
        batch_args.other_api_rate_limit_refill_ms,
        DEFAULT_OTHER_API_RATE_LIMIT_INTERVAL_MS
    );
}

#[test]
fn cli_defaults_match_the_128_vcpu_512_gib_production_profile() {
    let analyze = TopContractAnalysisCli::parse_from([
        "top_contract_analysis_rs",
        "analyze",
        "--seed-contract-address",
        "0xseed",
    ]);
    let CliCommand::Analyze(analyze) = analyze.command else {
        panic!("expected analyze command");
    };
    assert_eq!(analyze.duckdb_threads, 64);
    assert_eq!(analyze.rayon_threads, 96);
    assert_eq!(analyze.duckdb_memory_limit, "96GB");
    assert_eq!(analyze.recall_index_memory_limit, "260GB");
    assert_eq!(analyze.duckdb_read_connections, 2);
    assert_eq!(analyze.matched_contract_max_concurrency, 8);
    assert_eq!(analyze.max_tokens_per_contract, 200);
    assert_eq!(analyze.max_snapshot_bytes_per_seed, "24GB");
    assert_eq!(analyze.max_candidate_contracts_per_seed, 100_000);
    assert_eq!(analyze.max_selected_rows_per_seed, 2_000_000);

    let batch = TopContractAnalysisCli::parse_from([
        "top_contract_analysis_rs",
        "batch",
        "--seed-file",
        "seeds.txt",
    ]);
    let CliCommand::Batch(batch) = batch.command else {
        panic!("expected batch command");
    };
    assert_eq!(batch.duckdb_threads, 64);
    assert_eq!(batch.rayon_threads, 96);
    assert_eq!(batch.duckdb_memory_limit, "96GB");
    assert_eq!(batch.recall_index_memory_limit, "260GB");
    assert_eq!(batch.duckdb_read_connections, 2);
    assert_eq!(batch.matched_contract_max_concurrency, 8);
    assert_eq!(batch.seed_cpu_max_concurrency, 2);
    assert_eq!(batch.max_tokens_per_contract, 200);
    assert_eq!(batch.max_snapshot_bytes_per_seed, "24GB");
    assert_eq!(batch.max_candidate_contracts_per_seed, 100_000);
    assert_eq!(batch.max_selected_rows_per_seed, 2_000_000);

    let prepare = TopContractAnalysisCli::parse_from([
        "top_contract_analysis_rs",
        "prepare-features",
        "--feature-parquet",
        "features.parquet",
        "--feature-db",
        "features.duckdb",
    ]);
    let CliCommand::PrepareFeatures(prepare) = prepare.command else {
        panic!("expected prepare-features command");
    };
    assert_eq!(prepare.duckdb_threads, 96);
    assert_eq!(prepare.rayon_threads, 96);
    assert_eq!(prepare.duckdb_memory_limit, "300GB");
}

#[test]
fn cli_rejects_zero_for_bounded_concurrency_controls() {
    for (flag, value) in [
        ("--duckdb-threads", "0"),
        ("--rayon-threads", "0"),
        ("--duckdb-read-connections", "0"),
        ("--matched-contract-max-concurrency", "0"),
        ("--alchemy-api-max-concurrency", "0"),
        ("--other-api-rate-limit-refill-ms", "0"),
        ("--helius-rate-limit-refill-ms", "0"),
    ] {
        assert!(TopContractAnalysisCli::try_parse_from([
            "top_contract_analysis_rs",
            "analyze",
            "--seed-contract-address",
            "0xseed",
            flag,
            value,
        ])
        .is_err());
    }

    for flag in [
        "--seed-network-max-concurrency",
        "--seed-cpu-max-concurrency",
        "--duckdb-read-connections",
    ] {
        assert!(TopContractAnalysisCli::try_parse_from([
            "top_contract_analysis_rs",
            "batch",
            "--seed-file",
            "seeds.csv",
            flag,
            "0",
        ])
        .is_err());
    }
}

#[test]
fn cli_rejects_non_finite_or_out_of_range_recall_thresholds() {
    for name_threshold in ["NaN", "-0.1", "100.1"] {
        assert!(TopContractAnalysisCli::try_parse_from([
            "top_contract_analysis_rs",
            "analyze",
            "--seed-contract-address",
            "0xseed",
            "--name-threshold",
            name_threshold,
        ])
        .is_err());
    }
    for metadata_threshold in ["NaN", "-0.1", "1.1"] {
        assert!(TopContractAnalysisCli::try_parse_from([
            "top_contract_analysis_rs",
            "batch",
            "--seed-file",
            "seeds.csv",
            "--metadata-threshold",
            metadata_threshold,
        ])
        .is_err());
    }

    assert!(TopContractAnalysisCli::try_parse_from([
        "top_contract_analysis_rs",
        "analyze",
        "--seed-contract-address",
        "0xseed",
        "--name-threshold",
        "100",
        "--metadata-threshold",
        "0",
    ])
    .is_ok());
}

#[test]
fn cli_accepts_custom_other_api_rate_limit_refill_ms() {
    let analyze = TopContractAnalysisCli::parse_from([
        "top_contract_analysis_rs",
        "analyze",
        "--seed-contract-address",
        "0xseed",
        "--other-api-rate-limit-refill-ms",
        "450",
    ]);
    let CliCommand::Analyze(analyze_args) = analyze.command else {
        panic!("expected analyze command");
    };
    assert_eq!(analyze_args.other_api_rate_limit_refill_ms, 450);

    let batch = TopContractAnalysisCli::parse_from([
        "top_contract_analysis_rs",
        "batch",
        "--seed-file",
        "seeds.txt",
        "--other-api-rate-limit-refill-ms",
        "450",
    ]);
    let CliCommand::Batch(batch_args) = batch.command else {
        panic!("expected batch command");
    };
    assert_eq!(batch_args.other_api_rate_limit_refill_ms, 450);
}

#[test]
fn batch_cli_accepts_multichain_inputs_and_helius_limits() {
    let batch = TopContractAnalysisCli::parse_from([
        "top_contract_analysis_rs",
        "batch",
        "--seed-file",
        "seeds.csv",
        "--feature-parquet",
        "ethereum.parquet",
        "--feature-parquet",
        "solana.parquet",
        "--alchemy-network",
        "ethereum=eth-mainnet",
        "--alchemy-network",
        "base=base-mainnet",
        "--helius-api-key",
        "helius-key",
        "--helius-api-max-concurrency",
        "7",
        "--helius-rate-limit-refill-ms",
        "125",
        "--max-history-transactions-per-asset",
        "500",
        "--max-history-transactions-per-collection",
        "20000",
        "--seed-network-max-concurrency",
        "5",
        "--seed-cpu-max-concurrency",
        "2",
        "--refresh-scoped-cache",
    ]);
    let CliCommand::Batch(args) = batch.command else {
        panic!("expected batch command");
    };

    assert_eq!(args.feature_parquet, ["ethereum.parquet", "solana.parquet"]);
    assert_eq!(
        args.alchemy_network,
        ["ethereum=eth-mainnet", "base=base-mainnet"]
    );
    assert_eq!(args.helius_api_key, "helius-key");
    assert_eq!(args.helius_api_max_concurrency, 7);
    assert_eq!(args.helius_rate_limit_refill_ms, 125);
    assert_eq!(args.max_history_transactions_per_asset, 500);
    assert_eq!(args.max_history_transactions_per_collection, 20_000);
    assert_eq!(args.seed_network_max_concurrency, 5);
    assert_eq!(args.seed_cpu_max_concurrency, 2);
    assert!(args.refresh_scoped_cache);
}

#[test]
fn batch_cli_accepts_helius_refill_argument() {
    let command = TopContractAnalysisCli::try_parse_from([
        "top_contract_analysis_rs",
        "batch",
        "--seed-file",
        "seeds.csv",
        "--helius-rate-limit-refill-ms",
        "125",
    ])
    .unwrap();
    let CliCommand::Batch(args) = command.command else {
        panic!("expected batch command");
    };
    assert_eq!(args.helius_rate_limit_refill_ms, 125);
}

#[test]
fn prepare_features_subcommand_exposes_explicit_import_options() {
    Command::cargo_bin("top_contract_analysis_rs")
        .unwrap()
        .args(["prepare-features", "--help"])
        .assert()
        .success()
        .stdout(
            contains("--feature-parquet")
                .and(contains("--feature-db"))
                .and(contains("--duckdb-memory-limit"))
                .and(contains("--allow-in-memory-feature-db"))
                .and(contains("--prepare-only"))
                .and(contains("--restart-prepare")),
        );
}

#[test]
fn prepare_features_prepare_only_does_not_require_parquet_inputs() {
    let cli = TopContractAnalysisCli::try_parse_from([
        "top_contract_analysis_rs",
        "prepare-features",
        "--feature-db",
        "features.duckdb",
        "--prepare-only",
    ])
    .unwrap();
    let CliCommand::PrepareFeatures(args) = cli.command else {
        panic!("expected prepare-features command");
    };

    assert!(args.feature_parquet.is_empty());
    assert!(args.prepare_only);
    assert!(!args.restart_prepare);
}

#[test]
fn analyze_rejects_implicit_parquet_import() {
    Command::cargo_bin("top_contract_analysis_rs")
        .unwrap()
        .args([
            "analyze",
            "--seed-contract-address",
            "0xseed",
            "--feature-parquet",
            "features.parquet",
        ])
        .assert()
        .failure()
        .stderr(contains(
            "analyze does not import Parquet; run prepare-features first",
        ));
}

#[test]
fn batch_rejects_implicit_parquet_import() {
    Command::cargo_bin("top_contract_analysis_rs")
        .unwrap()
        .args([
            "batch",
            "--seed-file",
            "seeds.csv",
            "--feature-parquet",
            "features.parquet",
        ])
        .assert()
        .failure()
        .stderr(contains(
            "batch does not import Parquet; run prepare-features first",
        ));
}

#[test]
fn prepare_features_requires_explicit_in_memory_opt_in() {
    Command::cargo_bin("top_contract_analysis_rs")
        .unwrap()
        .args([
            "prepare-features",
            "--feature-parquet",
            "features.parquet",
            "--feature-db",
            ":memory:",
        ])
        .assert()
        .failure()
        .stderr(contains(
            "in-memory feature preparation requires --allow-in-memory-feature-db",
        ));
}

#[test]
fn prepare_features_builds_a_read_only_recall_ready_database() {
    let dir = tempfile::tempdir().unwrap();
    let parquet_path = dir.path().join("features.parquet");
    let database_path = dir.path().join("features.duckdb");
    let parquet_literal = parquet_path
        .to_string_lossy()
        .replace('\\', "/")
        .replace('\'', "''");
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(&format!(
        "COPY (
            SELECT
                'ethereum'::VARCHAR AS chain,
                '0xcandidate'::VARCHAR AS contract_address,
                '1'::VARCHAR AS token_id,
                'ipfs://shared/1'::VARCHAR AS token_uri,
                ''::VARCHAR AS image_uri,
                'Candidate'::VARCHAR AS name,
                'C'::VARCHAR AS symbol,
                '{{\"description\":\"gold dragon\"}}'::VARCHAR AS metadata_json,
                'ipfs:shared/1'::VARCHAR AS token_uri_norm,
                ''::VARCHAR AS image_uri_norm,
                'candidate'::VARCHAR AS name_norm
        ) TO '{parquet_literal}' (FORMAT PARQUET)"
    ))
    .unwrap();
    drop(conn);

    Command::cargo_bin("top_contract_analysis_rs")
        .unwrap()
        .args([
            "prepare-features",
            "--feature-parquet",
            parquet_path.to_str().unwrap(),
            "--feature-db",
            database_path.to_str().unwrap(),
            "--duckdb-memory-limit",
            "1GB",
        ])
        .assert()
        .success();

    Command::cargo_bin("top_contract_analysis_rs")
        .unwrap()
        .args([
            "prepare-features",
            "--prepare-only",
            "--feature-db",
            database_path.to_str().unwrap(),
            "--duckdb-memory-limit",
            "1GB",
        ])
        .assert()
        .success();

    let store = DuckDbFeatureStore::open_read_only_with_options(
        database_path.to_str().unwrap(),
        DuckDbResourceOptions {
            threads: 1,
            memory_limit: "1GB".to_string(),
            read_connections: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let snapshot = store
        .load_snapshot(
            "ethereum",
            &[SeedNft {
                contract_address: "0xseed".into(),
                token_id: "1".into(),
                token_uri: "ipfs://shared/1".into(),
                ..Default::default()
            }],
            95.0,
            0.6,
            0,
            0,
        )
        .unwrap();
    assert_eq!(snapshot.nft_rows.len(), 1);
    assert_eq!(snapshot.nft_rows[0].contract_address, "0xcandidate");
}

#[test]
fn analyze_rejects_unprepared_database_without_mutating_it() {
    let dir = tempfile::tempdir().unwrap();
    let database_path = dir.path().join("unprepared.duckdb");
    let conn = Connection::open(&database_path).unwrap();
    conn.execute_batch(
        "CREATE TABLE nft_features (
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
            'ethereum', '0xcandidate', '1', '', '', '', '', '', '', '', ''
        );",
    )
    .unwrap();
    drop(conn);

    Command::cargo_bin("top_contract_analysis_rs")
        .unwrap()
        .args([
            "analyze",
            "--chain",
            "ethereum",
            "--seed-contract-address",
            "0xseed",
            "--feature-db",
            database_path.to_str().unwrap(),
            "--duckdb-memory-limit",
            "1GB",
        ])
        .assert()
        .failure()
        .stderr(
            contains("authoritative prepare journal is missing")
                .and(contains("prepare-features before read-only analysis")),
        );

    let conn = Connection::open(&database_path).unwrap();
    let prepared_table_exists = conn
        .query_row(
            "SELECT EXISTS(
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'nft_prepared_recall_chains'
            )",
            [],
            |row| row.get::<_, bool>(0),
        )
        .unwrap();
    assert!(!prepared_table_exists);
}

#[test]
fn batch_cli_rejects_removed_global_chain_argument() {
    let error = TopContractAnalysisCli::try_parse_from([
        "top_contract_analysis_rs",
        "batch",
        "--seed-file",
        "seeds.csv",
        "--chain",
        "ethereum",
    ])
    .unwrap_err();

    assert!(error.to_string().contains("unexpected argument '--chain'"));
}

#[test]
fn cli_accepts_paper_statistics_threshold_flags() {
    let analyze = TopContractAnalysisCli::parse_from([
        "top_contract_analysis_rs",
        "analyze",
        "--seed-contract-address",
        "0xseed",
        "--paper-min-cycle-size",
        "3",
        "--paper-min-path-length",
        "4",
        "--paper-center-fanout-threshold",
        "5",
        "--paper-concentration-top-pct",
        "0.2",
    ]);
    let CliCommand::Analyze(analyze_args) = analyze.command else {
        panic!("expected analyze command");
    };
    assert_eq!(analyze_args.paper_min_cycle_size, 3);
    assert_eq!(analyze_args.paper_min_path_length, 4);
    assert_eq!(analyze_args.paper_center_fanout_threshold, 5);
    assert_eq!(analyze_args.paper_concentration_top_pct, 0.2);

    let batch = TopContractAnalysisCli::parse_from([
        "top_contract_analysis_rs",
        "batch",
        "--seed-file",
        "seeds.txt",
        "--paper-min-cycle-size",
        "2",
        "--paper-min-path-length",
        "6",
        "--paper-center-fanout-threshold",
        "4",
        "--paper-concentration-top-pct",
        "0.15",
    ]);
    let CliCommand::Batch(batch_args) = batch.command else {
        panic!("expected batch command");
    };
    assert_eq!(batch_args.paper_min_cycle_size, 2);
    assert_eq!(batch_args.paper_min_path_length, 6);
    assert_eq!(batch_args.paper_center_fanout_threshold, 4);
    assert_eq!(batch_args.paper_concentration_top_pct, 0.15);
}

#[test]
fn analyze_subcommand_rejects_removed_paper_top_k_flag() {
    Command::cargo_bin("top_contract_analysis_rs")
        .unwrap()
        .args([
            "analyze",
            "--seed-contract-address",
            "0xseed",
            "--paper-top-k",
            "7",
        ])
        .assert()
        .failure()
        .stderr(contains("unexpected argument"));
}

#[test]
fn batch_subcommand_rejects_removed_paper_top_k_flag() {
    Command::cargo_bin("top_contract_analysis_rs")
        .unwrap()
        .args(["batch", "--seed-file", "seeds.txt", "--paper-top-k", "7"])
        .assert()
        .failure()
        .stderr(contains("unexpected argument"));
}

#[test]
fn analyze_subcommand_accepts_existing_flag_names() {
    Command::cargo_bin("top_contract_analysis_rs")
        .unwrap()
        .args([
            "analyze",
            "--chain",
            "ethereum",
            "--seed-contract-address",
            "0xseed",
            "--alchemy-api-key",
            "key",
            "--feature-db",
            "some.db",
        ])
        .assert()
        .failure()
        .stderr(
            contains("unexpected argument")
                .not()
                .and(contains("required arguments were not provided").not()),
        );
}

#[test]
fn analyze_subcommand_rejects_removed_contract_concurrency_flag() {
    Command::cargo_bin("top_contract_analysis_rs")
        .unwrap()
        .args([
            "analyze",
            "--chain",
            "ethereum",
            "--seed-contract-address",
            "0xseed",
            "--alchemy-api-key",
            "key",
            "--contract-max-concurrency",
            "2",
        ])
        .assert()
        .failure()
        .stderr(contains("unexpected argument"));
}

#[test]
fn batch_subcommand_is_exposed() {
    Command::cargo_bin("top_contract_analysis_rs")
        .unwrap()
        .args([
            "batch",
            "--seed-file",
            "seeds.txt",
            "--chain",
            "ethereum",
            "--feature-db",
            "batch.db",
            "--output-dir",
            "result",
            "--alchemy-api-max-concurrency",
            "12",
            "--other-api-max-concurrency",
            "4",
            "--matched-contract-max-concurrency",
            "6",
            "--seed-cpu-max-concurrency",
            "2",
            "--seed-network-max-concurrency",
            "3",
        ])
        .assert()
        .failure()
        .stderr(
            contains("NotImplemented")
                .not()
                .and(contains("required arguments were not provided").not()),
        );
}

#[test]
fn batch_subcommand_rejects_removed_contract_concurrency_flag() {
    Command::cargo_bin("top_contract_analysis_rs")
        .unwrap()
        .args([
            "batch",
            "--seed-file",
            "seeds.txt",
            "--contract-max-concurrency",
            "2",
        ])
        .assert()
        .failure()
        .stderr(contains("unexpected argument"));
}

#[test]
fn batch_subcommand_rejects_removed_seed_metadata_concurrency_flag() {
    Command::cargo_bin("top_contract_analysis_rs")
        .unwrap()
        .args([
            "batch",
            "--seed-file",
            "seeds.txt",
            "--seed-metadata-max-concurrency",
            "1",
        ])
        .assert()
        .failure()
        .stderr(contains("unexpected argument"));
}

#[test]
fn batch_subcommand_rejects_removed_legacy_concurrency_flag() {
    Command::cargo_bin("top_contract_analysis_rs")
        .unwrap()
        .args([
            "batch",
            "--seed-file",
            "seeds.txt",
            "--sale-metric-max-concurrency",
            "10",
        ])
        .assert()
        .failure()
        .stderr(contains("unexpected argument"));
}

#[test]
fn batch_subcommand_rejects_removed_worker_flags() {
    Command::cargo_bin("top_contract_analysis_rs")
        .unwrap()
        .args([
            "batch",
            "--seed-file",
            "seeds.txt",
            "--workers",
            "2",
            "--cpu-max-concurrency",
            "1",
        ])
        .assert()
        .failure()
        .stderr(contains("unexpected argument"));
}

#[test]
fn export_snapshot_subcommand_is_exposed() {
    Command::cargo_bin("top_contract_analysis_rs")
        .unwrap()
        .args(["export-snapshot", "--output", "snapshot.json"])
        .assert()
        .failure()
        .stderr(
            contains("NotImplemented")
                .not()
                .and(contains("required arguments were not provided").not())
                .and(contains("Cannot start a runtime").not()),
        );
}

#[test]
fn export_snapshot_accepts_optional_block_bounds() {
    let cli = TopContractAnalysisCli::parse_from([
        "top_contract_analysis_rs",
        "export-snapshot",
        "--output",
        "snapshot.parquet",
        "--start-block",
        "10",
        "--end-block",
        "20",
    ]);
    let CliCommand::ExportSnapshot(args) = cli.command else {
        panic!("expected export-snapshot command");
    };
    assert_eq!(args.start_block, Some(10));
    assert_eq!(args.end_block, Some(20));
}

#[test]
fn export_snapshot_rejects_removed_keep_metadata_json_flag() {
    Command::cargo_bin("top_contract_analysis_rs")
        .unwrap()
        .args([
            "export-snapshot",
            "--output",
            "snapshot.json",
            "--keep-metadata-json",
        ])
        .assert()
        .failure()
        .stderr(contains("unexpected argument"));
}
