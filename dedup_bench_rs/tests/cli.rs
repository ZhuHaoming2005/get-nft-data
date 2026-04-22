use std::fs;

use assert_cmd::Command;
use duckdb::Connection;
use tempfile::tempdir;

#[test]
fn cli_writes_json_and_markdown_outputs() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("feature_store.duckdb");
    let conn = Connection::open(&db_path).unwrap();
    conn.execute_batch(
        "
        CREATE TABLE nft_features (
            chain VARCHAR,
            contract_address VARCHAR,
            token_id VARCHAR,
            name VARCHAR,
            metadata_json VARCHAR,
            metadata_doc VARCHAR,
            name_norm VARCHAR,
            metadata_keywords_arr VARCHAR
        );
        INSERT INTO nft_features VALUES
        ('ethereum', '0xseed', '9', 'Excluded Seed #9', '{\"description\":\"rare dragon gold\"}', 'rare dragon gold', 'azuki', '[\"rare\",\"dragon\",\"gold\"]'),
        ('ethereum', '0xname', '1', 'Azuki #2', '{\"description\":\"nothing here\"}', 'nothing here', 'azuki', '[\"nothing\"]'),
        ('ethereum', '0xmeta', '2', 'Totally Different', '{\"description\":\"rare dragon gold\"}', 'rare dragon gold', 'totally different', '[\"rare\",\"dragon\",\"gold\"]');
        ",
    )
    .unwrap();
    drop(conn);

    let metadata_path = dir.path().join("metadata.json");
    let output_path = dir.path().join("report.json");
    fs::write(&metadata_path, r#"{"description":"rare dragon gold"}"#).unwrap();

    Command::cargo_bin("dedup_bench_rs")
        .unwrap()
        .args([
            "run",
            "--chain",
            "ethereum",
            "--contract-address",
            "0xseed",
            "--token-id",
            "1",
            "--name",
            "Azuki #1",
            "--metadata-file",
            &metadata_path.to_string_lossy(),
            "--feature-db",
            &db_path.to_string_lossy(),
            "--output",
            &output_path.to_string_lossy(),
            "--repeat",
            "1",
        ])
        .assert()
        .success();

    let json_output = fs::read_to_string(&output_path).unwrap();
    let markdown_output = fs::read_to_string(output_path.with_extension("md")).unwrap();
    assert!(json_output.contains("\"name_algorithms\""));
    assert!(json_output.contains("\"metadata_algorithms\""));
    assert!(json_output.contains("\"duplicate_count\""));
    assert!(json_output.contains("\"metadata_doc\": \"rare dragon gold\""));
    assert!(json_output.contains("\"name\": \"Azuki #2\""));
    assert!(!json_output.contains("\"reference\""));
    assert!(!json_output.contains("Excluded Seed #9"));
    assert!(markdown_output.contains("# NFT Name/Metadata Dedup Benchmark"));
    assert!(markdown_output.contains("## Name Algorithms"));
    assert!(markdown_output.contains("## Metadata Algorithms"));
    assert!(markdown_output.contains("duplicate_count"));
    assert!(markdown_output.contains("contract=`0xname` name=`Azuki #2`"));
    assert!(markdown_output.contains("metadata_doc=`rare dragon gold`"));
    assert!(!markdown_output.contains("## Current Name/Metadata Reference"));
    assert!(!markdown_output.contains("Excluded Seed #9"));
}
