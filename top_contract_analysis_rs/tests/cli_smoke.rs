use assert_cmd::Command;
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
        ])
        .assert()
        .failure()
        .stderr(contains("--feature-db"));
}

#[test]
fn export_snapshot_subcommand_is_exposed() {
    Command::cargo_bin("top_contract_analysis_rs")
        .unwrap()
        .arg("export-snapshot")
        .assert()
        .failure()
        .stderr(contains("--output"));
}
