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
fn batch_subcommand_rejects_removed_sale_metric_concurrency_flag() {
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
