use name_metadata_change_samples::{
    collect_contract_samples, collect_samples, collect_samples_with_progress,
    SampleCollectionConfig, SampleProgressStage,
};
use std::fs;
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

#[test]
fn collect_contract_samples_reads_many_contracts_in_one_call() {
    let temp = tempdir().unwrap();
    let db_path = temp.path().join("features.duckdb");

    let store = DuckDbFeatureStore::new(db_path.to_str().unwrap()).unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[
                DatabaseNftRecord {
                    contract_address: "0xfirst".into(),
                    token_id: "1".into(),
                    name: "".into(),
                    symbol: "".into(),
                    metadata_doc: "".into(),
                    metadata_json: "".into(),
                    ..Default::default()
                },
                DatabaseNftRecord {
                    contract_address: "0xfirst".into(),
                    token_id: "2".into(),
                    name: "First Contract".into(),
                    symbol: "FIRST".into(),
                    metadata_doc: "first usable metadata".into(),
                    metadata_json: r#"{"description":"first usable metadata"}"#.into(),
                    ..Default::default()
                },
                DatabaseNftRecord {
                    contract_address: "0xsecond".into(),
                    token_id: "4".into(),
                    name: "Second Contract".into(),
                    symbol: "SECOND".into(),
                    metadata_doc: "second metadata".into(),
                    metadata_json: r#"{"description":"second metadata"}"#.into(),
                    ..Default::default()
                },
            ],
        )
        .unwrap();
    drop(store);

    let samples = collect_contract_samples(
        &db_path,
        "ethereum",
        &["0xsecond".to_string(), "0xfirst".to_string()],
    )
    .unwrap();

    assert_eq!(samples.len(), 2);
    assert_eq!(samples[0].contract_address, "0xsecond");
    assert_eq!(samples[0].name, "Second Contract");
    assert_eq!(samples[0].metadata_source_token_id, "4");
    assert_eq!(samples[0].row_count, 1);
    assert_eq!(samples[1].contract_address, "0xfirst");
    assert_eq!(samples[1].name, "First Contract");
    assert_eq!(samples[1].metadata_source_token_id, "2");
    assert_eq!(samples[1].row_count, 2);
}

#[test]
fn collect_samples_processes_multiple_seeds_serially() {
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
                    contract_address: "0xseed1".into(),
                    token_id: "1".into(),
                    name: "Seed One".into(),
                    metadata_doc: "shared one".into(),
                    ..Default::default()
                },
                DatabaseNftRecord {
                    contract_address: "0xcopy1".into(),
                    token_id: "1".into(),
                    name: "Seed One".into(),
                    metadata_doc: "changed".into(),
                    ..Default::default()
                },
                DatabaseNftRecord {
                    contract_address: "0xseed2".into(),
                    token_id: "1".into(),
                    name: "Seed Two".into(),
                    metadata_doc: "shared two".into(),
                    ..Default::default()
                },
                DatabaseNftRecord {
                    contract_address: "0xcopy2".into(),
                    token_id: "1".into(),
                    name: "Different".into(),
                    metadata_doc: "shared two".into(),
                    ..Default::default()
                },
            ],
        )
        .unwrap();
    drop(store);

    fs::write(&input_path, "0xseed1\n0xseed2\n").unwrap();

    let report = collect_samples(SampleCollectionConfig {
        chain: "ethereum".into(),
        feature_db: db_path,
        input: input_path,
        output: output_path,
        name_threshold: 95.0,
        metadata_threshold: 0.6,
        max_tokens_per_contract: 0,
        max_recall_rows: 0,
        max_seed_tokens: 0,
        duckdb_threads: 1,
        duckdb_memory_limit: "1GB".into(),
    })
    .unwrap();

    assert_eq!(report.seed_reports.len(), 2);
    assert_eq!(report.seed_reports[0].contract_address, "0xseed1");
    assert_eq!(report.seed_reports[1].contract_address, "0xseed2");
    assert_eq!(report.seed_reports[0].candidate_reports.len(), 1);
    assert_eq!(report.seed_reports[1].candidate_reports.len(), 1);
}

#[test]
fn collect_samples_reports_serial_in_contract_progress_without_addresses() {
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
                    contract_address: "0xseed1".into(),
                    token_id: "1".into(),
                    name: "Seed One".into(),
                    metadata_doc: "shared one".into(),
                    ..Default::default()
                },
                DatabaseNftRecord {
                    contract_address: "0xcopy1".into(),
                    token_id: "1".into(),
                    name: "Seed One".into(),
                    metadata_doc: "changed".into(),
                    ..Default::default()
                },
                DatabaseNftRecord {
                    contract_address: "0xseed2".into(),
                    token_id: "1".into(),
                    name: "Seed Two".into(),
                    metadata_doc: "shared two".into(),
                    ..Default::default()
                },
                DatabaseNftRecord {
                    contract_address: "0xcopy2".into(),
                    token_id: "1".into(),
                    name: "Different".into(),
                    metadata_doc: "shared two".into(),
                    ..Default::default()
                },
            ],
        )
        .unwrap();
    drop(store);

    fs::write(&input_path, "0xseed1\n0xseed2\n").unwrap();

    let mut events = Vec::new();
    collect_samples_with_progress(
        SampleCollectionConfig {
            chain: "ethereum".into(),
            feature_db: db_path,
            input: input_path,
            output: output_path,
            name_threshold: 95.0,
            metadata_threshold: 0.6,
            max_tokens_per_contract: 0,
            max_recall_rows: 0,
            max_seed_tokens: 0,
            duckdb_threads: 1,
            duckdb_memory_limit: "1GB".into(),
        },
        |event| {
            events.push((
                event.seed_index,
                event.total_seeds,
                event.stage,
                event.stage_index,
                event.stage_count,
                event.candidate_count,
            ));
        },
    )
    .unwrap();

    assert_eq!(events.len(), 10);
    assert!(events.iter().all(|event| event.1 == 2));
    assert!(events.iter().all(|event| event.4 == 5));
    assert_eq!(events[0].0, 1);
    assert_eq!(events[0].2, SampleProgressStage::ReadSeedRows);
    assert_eq!(events[0].3, 1);
    assert_eq!(events[4].0, 1);
    assert_eq!(events[4].2, SampleProgressStage::FinishedSeed);
    assert_eq!(events[4].3, 5);
    assert_eq!(events[4].5, Some(1));
    assert_eq!(events[5].0, 2);
    assert_eq!(events[5].2, SampleProgressStage::ReadSeedRows);
    assert_eq!(events[9].0, 2);
    assert_eq!(events[9].2, SampleProgressStage::FinishedSeed);
}
