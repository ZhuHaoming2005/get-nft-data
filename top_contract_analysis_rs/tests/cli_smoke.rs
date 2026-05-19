use assert_cmd::Command;
use predicates::prelude::PredicateBooleanExt;
use predicates::str::contains;

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
            "--signal-cache-db",
            "signals.db",
            "--output-dir",
            "result",
            "--api-max-concurrency",
            "24",
            "--seed-metadata-max-concurrency",
            "1",
            "--contract-max-concurrency",
            "12",
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
