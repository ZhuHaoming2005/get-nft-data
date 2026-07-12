use assert_cmd::Command;
use clap::Parser;
use predicates::prelude::PredicateBooleanExt;
use predicates::str::contains;
use top_contract_analysis_rs::api::DEFAULT_OTHER_API_RATE_LIMIT_INTERVAL_MS;
use top_contract_analysis_rs::cli::{Command as CliCommand, TopContractAnalysisCli};

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
