use std::fs;

use name_metadata_change_samples::{collect_samples, SampleCollectionConfig};
use tempfile::tempdir;
use top_contract_analysis_rs::models::DatabaseNftRecord;
use top_contract_analysis_rs::store::DuckDbFeatureStore;

#[test]
fn collect_samples_outputs_only_name_metadata_candidates() {
    let temp = tempdir().unwrap();
    let db_path = temp.path().join("features.duckdb");
    let input_path = temp.path().join("contracts.txt");
    let output_path = temp.path().join("samples.md");

    let store = DuckDbFeatureStore::new(db_path.to_str().unwrap()).unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[
                DatabaseNftRecord {
                    contract_address: "0xseed".into(),
                    token_id: "1".into(),
                    name: "Azuki #1".into(),
                    symbol: "AZUKI".into(),
                    metadata_doc: "gold dragon red background".into(),
                    metadata_json: r#"{"description":"gold dragon red background"}"#.into(),
                    ..Default::default()
                },
                DatabaseNftRecord {
                    contract_address: "0xmetadata".into(),
                    token_id: "7".into(),
                    name: "Changed Creature".into(),
                    symbol: "FAKE".into(),
                    metadata_doc: "".into(),
                    metadata_json: "".into(),
                    ..Default::default()
                },
                DatabaseNftRecord {
                    contract_address: "0xmetadata".into(),
                    token_id: "8".into(),
                    name: "Changed Creature".into(),
                    symbol: "FAKE".into(),
                    metadata_doc: "gold dragon red background".into(),
                    metadata_json: r#"{"description":"gold dragon red background"}"#.into(),
                    ..Default::default()
                },
                DatabaseNftRecord {
                    contract_address: "0xname".into(),
                    token_id: "3".into(),
                    name: "Azuki #1".into(),
                    symbol: "FAKE".into(),
                    metadata_doc: "unrelated text".into(),
                    metadata_json: r#"{"description":"unrelated text"}"#.into(),
                    ..Default::default()
                },
                DatabaseNftRecord {
                    contract_address: "0xuri".into(),
                    token_id: "9".into(),
                    token_uri: "ipfs://seed/1".into(),
                    name: "Different".into(),
                    metadata_doc: "different".into(),
                    ..Default::default()
                },
            ],
        )
        .unwrap();
    drop(store);

    fs::write(&input_path, "0xseed\n").unwrap();

    let report = collect_samples(SampleCollectionConfig {
        chain: "ethereum".into(),
        feature_db: db_path,
        input: input_path,
        output: output_path.clone(),
        name_threshold: 95.0,
        metadata_threshold: 0.6,
        max_tokens_per_contract: 0,
        max_recall_rows: 0,
        max_seed_tokens: 0,
        duckdb_threads: 1,
        duckdb_memory_limit: "1GB".into(),
    })
    .unwrap();

    assert_eq!(report.seed_reports.len(), 1);
    assert_eq!(report.seed_reports[0].candidate_reports.len(), 2);
    assert_eq!(
        report.seed_reports[0]
            .seed_sample
            .as_ref()
            .unwrap()
            .metadata_source_token_id,
        "1"
    );
    let metadata_report = report.seed_reports[0]
        .candidate_reports
        .iter()
        .find(|candidate| candidate.contract_address == "0xmetadata")
        .unwrap();
    assert_eq!(metadata_report.sample.name, "Changed Creature");
    assert_eq!(metadata_report.sample.metadata_source_token_id, "8");
    assert_eq!(metadata_report.sample.row_count, 2);

    let output = fs::read_to_string(output_path).unwrap();
    assert!(output.contains("0xseed"));
    assert!(output.contains("0xmetadata"));
    assert!(output.contains("metadata_match"));
    assert!(output.contains("0xname"));
    assert!(output.contains("name_match"));
    assert!(!output.contains("0xuri"));
    assert!(!output.contains("#### Token"));
}

#[test]
fn collect_samples_preserves_visible_name_and_metadata_text() {
    let temp = tempdir().unwrap();
    let db_path = temp.path().join("features.duckdb");
    let input_path = temp.path().join("contracts.txt");
    let output_path = temp.path().join("samples.md");

    let store = DuckDbFeatureStore::new(db_path.to_str().unwrap()).unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[
                DatabaseNftRecord {
                    contract_address: "0xseed".into(),
                    token_id: "1".into(),
                    name: "Bored Ape #42".into(),
                    metadata_doc: "Blue Fur Laser Eyes".into(),
                    metadata_json: r#"{"attributes":[{"trait_type":"Fur","value":"Blue"}]}"#.into(),
                    ..Default::default()
                },
                DatabaseNftRecord {
                    contract_address: "0xcopy".into(),
                    token_id: "42".into(),
                    name: "Bored Ape Copy #42".into(),
                    metadata_doc: "Blue Fur Laser Eyes".into(),
                    metadata_json: r#"{"attributes":[{"trait_type":"Fur","value":"Blue"}]}"#.into(),
                    ..Default::default()
                },
            ],
        )
        .unwrap();
    drop(store);

    fs::write(&input_path, "# comments are ignored\n0xseed\n\n").unwrap();

    collect_samples(SampleCollectionConfig {
        chain: "ethereum".into(),
        feature_db: db_path,
        input: input_path,
        output: output_path.clone(),
        name_threshold: 95.0,
        metadata_threshold: 0.6,
        max_tokens_per_contract: 0,
        max_recall_rows: 0,
        max_seed_tokens: 0,
        duckdb_threads: 1,
        duckdb_memory_limit: "1GB".into(),
    })
    .unwrap();

    let output = fs::read_to_string(output_path).unwrap();
    assert!(output.contains("Bored Ape #42"));
    assert!(output.contains("Bored Ape Copy #42"));
    assert!(output.contains("Blue Fur Laser Eyes"));
    assert!(output.contains(r#""trait_type":"Fur""#));
}
