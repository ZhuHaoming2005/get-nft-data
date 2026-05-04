use duckdb::{params, Connection};
use name_metadata_change_samples::{
    collect_samples, collect_samples_with_progress, SampleCollectionConfig, SampleProgressStage,
};
use std::fs;
use tempfile::tempdir;

struct TestRow {
    contract_address: &'static str,
    token_id: &'static str,
    name: &'static str,
    token_uri: &'static str,
    image_uri: &'static str,
    metadata_doc: &'static str,
    metadata_json: &'static str,
}

impl TestRow {
    fn new(contract_address: &'static str, token_id: &'static str) -> Self {
        Self {
            contract_address,
            token_id,
            name: "",
            token_uri: "",
            image_uri: "",
            metadata_doc: "",
            metadata_json: "",
        }
    }

    fn name(mut self, value: &'static str) -> Self {
        self.name = value;
        self
    }

    fn token_uri(mut self, value: &'static str) -> Self {
        self.token_uri = value;
        self
    }

    fn image_uri(mut self, value: &'static str) -> Self {
        self.image_uri = value;
        self
    }

    fn metadata_doc(mut self, value: &'static str) -> Self {
        self.metadata_doc = value;
        self
    }

    fn metadata_json(mut self, value: &'static str) -> Self {
        self.metadata_json = value;
        self
    }
}

fn config(
    feature_db: std::path::PathBuf,
    input: std::path::PathBuf,
    output: std::path::PathBuf,
) -> SampleCollectionConfig {
    SampleCollectionConfig {
        chain: "ethereum".into(),
        feature_db,
        input,
        output,
        name_threshold: 95.0,
        metadata_threshold: 0.6,
        max_recall_rows: 0,
        max_seed_tokens: 0,
        duckdb_threads: 1,
        duckdb_memory_limit: "1GB".into(),
    }
}

fn write_feature_db(path: &std::path::Path, rows: &[TestRow]) {
    let conn = Connection::open(path).unwrap();
    conn.execute_batch(
        "
        CREATE TABLE nft_features (
            chain VARCHAR NOT NULL,
            contract_address VARCHAR NOT NULL,
            token_id VARCHAR NOT NULL,
            token_uri VARCHAR,
            image_uri VARCHAR,
            name VARCHAR,
            metadata_doc VARCHAR,
            metadata_json VARCHAR
        );
        ",
    )
    .unwrap();
    let mut stmt = conn
        .prepare(
            "
            INSERT INTO nft_features (
                chain, contract_address, token_id, token_uri, image_uri, name, metadata_doc, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ",
        )
        .unwrap();
    for row in rows {
        stmt.execute(params![
            "ethereum",
            row.contract_address,
            row.token_id,
            row.token_uri,
            row.image_uri,
            row.name,
            row.metadata_doc,
            row.metadata_json
        ])
        .unwrap();
    }
}

#[test]
fn collect_samples_outputs_split_name_and_metadata_text_only() {
    let temp = tempdir().unwrap();
    let db_path = temp.path().join("features.duckdb");
    let input_path = temp.path().join("contracts.txt");
    let output_path = temp.path().join("samples.md");

    write_feature_db(
        &db_path,
        &[
            TestRow::new("0xseed", "1")
                .name("Azuki #1")
                .metadata_doc("gold dragon red background")
                .metadata_json(
                    r#"{"description":"gold dragon","attributes":[{"trait_type":"Background","value":"Red"}],"image":"ipfs://seed/image.png"}"#,
                ),
            TestRow::new("0xname", "3")
                .name("Azuki #1")
                .metadata_doc("unrelated text"),
            TestRow::new("0xmetadata", "8")
                .name("Changed Creature")
                .metadata_doc("gold dragon red background")
                .metadata_json(
                    r#"{"description":"gold dragon","attributes":[{"trait_type":"Background","value":"Red"}],"image":"ipfs://copy/image.png"}"#,
                ),
            TestRow::new("0xuri", "9")
                .name("Different")
                .token_uri("ipfs://seed/1")
                .image_uri("ipfs://seed/image.png")
                .metadata_doc("different"),
        ],
    );
    fs::write(&input_path, "0xseed\n").unwrap();

    let mut sample_config = config(db_path, input_path, output_path.clone());
    sample_config.metadata_threshold = 0.1;
    let report = collect_samples(sample_config).unwrap();

    assert_eq!(report.seed_reports.len(), 1);
    assert_eq!(report.seed_reports[0].name.seed, "Azuki #1");
    assert_eq!(report.seed_reports[0].name.matches, vec!["Azuki #1"]);
    assert_eq!(
        report.seed_reports[0].metadata.seed,
        r#"{"description":"gold dragon","attributes":[{"trait_type":"Background","value":"Red"}],"image":"ipfs://seed/image.png"}"#
    );
    assert_eq!(
        report.seed_reports[0].metadata.matches,
        vec![
            r#"{"description":"gold dragon","attributes":[{"trait_type":"Background","value":"Red"}],"image":"ipfs://copy/image.png"}"#
        ]
    );

    let output = fs::read_to_string(output_path).unwrap();
    assert!(output.contains("## Modification Summary"));
    assert!(output.contains("- exact_clone: 1"));
    assert!(output.contains("#### Metadata Change Matrix"));
    assert!(output.contains("| replaced | 0 | 0 | 0 | 1 | 0 | 0 | 0 |"));
    assert!(output.contains("## Name Matches"));
    assert!(output.contains("- seed: Azuki #1"));
    assert!(output.contains("[exact_clone] Azuki #1"));
    assert!(output.contains("## Metadata Matches"));
    assert!(output.contains("- match labels: references:replaced"));
    assert!(output.contains(r#""description":"gold dragon""#));
    assert!(output.contains(r#""image":"ipfs://copy/image.png""#));
    assert!(!output.contains("0xseed"));
    assert!(!output.contains("0xname"));
    assert!(!output.contains("0xmetadata"));
    assert!(!output.contains("0xuri"));
    assert!(!output.contains("match reasons"));
    assert!(!output.contains("token_uri_match"));
    assert!(!output.contains("image_uri_match"));
    assert!(!output.contains("metadata source token"));
    assert!(!output.contains("symbol"));
}

#[test]
fn collect_samples_uses_equivalent_name_and_metadata_normalization() {
    let temp = tempdir().unwrap();
    let db_path = temp.path().join("features.duckdb");
    let input_path = temp.path().join("contracts.txt");
    let output_path = temp.path().join("samples.md");

    write_feature_db(
        &db_path,
        &[
            TestRow::new("0xseed", "1")
                .name("Azuki #123")
                .metadata_json(
                    r#"{"description":"Gold Dragon","attributes":[{"trait_type":"Background","value":"Red"}],"ignored":"noise"}"#,
                ),
            TestRow::new("0xname", "1")
                .name("Ａｚｕｋｉ #456")
                .metadata_doc("unrelated"),
            TestRow::new("0xmetadata", "1")
                .name("Different")
                .metadata_doc("background red gold dragon"),
        ],
    );
    fs::write(&input_path, "0xseed\n").unwrap();

    let report = collect_samples(config(db_path, input_path, output_path)).unwrap();

    assert_eq!(report.seed_reports[0].name.seed, "Azuki #123");
    assert_eq!(report.seed_reports[0].name.matches, vec!["Ａｚｕｋｉ #456"]);
    assert_eq!(
        report.seed_reports[0].metadata.seed,
        r#"{"description":"Gold Dragon","attributes":[{"trait_type":"Background","value":"Red"}],"ignored":"noise"}"#
    );
    assert_eq!(
        report.seed_reports[0].metadata.matches,
        vec!["background red gold dragon"]
    );
}

#[test]
fn collect_samples_outputs_one_representative_name_per_matching_contract() {
    let temp = tempdir().unwrap();
    let db_path = temp.path().join("features.duckdb");
    let input_path = temp.path().join("contracts.txt");
    let output_path = temp.path().join("samples.md");

    write_feature_db(
        &db_path,
        &[
            TestRow::new("0xseed", "1")
                .name("Azuki #1")
                .metadata_doc("seed metadata"),
            TestRow::new("0xcopy", "2")
                .name("Azuki #2")
                .metadata_doc("copy metadata"),
            TestRow::new("0xcopy", "3")
                .name("Azuki #3")
                .metadata_doc("copy metadata"),
            TestRow::new("0xother", "1")
                .name("Different #1")
                .metadata_doc("other metadata"),
        ],
    );
    fs::write(&input_path, "0xseed\n").unwrap();

    let report = collect_samples(config(db_path, input_path, output_path)).unwrap();

    assert_eq!(report.seed_reports[0].name.matches, vec!["Azuki #2"]);
}

#[test]
fn collect_samples_matches_any_normalized_name_but_outputs_one_contract_name() {
    let temp = tempdir().unwrap();
    let db_path = temp.path().join("features.duckdb");
    let input_path = temp.path().join("contracts.txt");
    let output_path = temp.path().join("samples.md");

    write_feature_db(
        &db_path,
        &[
            TestRow::new("0xseed", "1")
                .name("Seed One")
                .metadata_doc("seed metadata"),
            TestRow::new("0xcopy", "1")
                .name("Displayed Name")
                .metadata_doc("copy metadata"),
            TestRow::new("0xcopy", "2")
                .name("Seed One")
                .metadata_doc("copy metadata"),
        ],
    );
    fs::write(&input_path, "0xseed\n").unwrap();

    let report = collect_samples(config(db_path, input_path, output_path)).unwrap();

    assert_eq!(report.seed_reports[0].name.matches, vec!["Displayed Name"]);
}

#[test]
fn collect_samples_processes_multiple_seeds_serially() {
    let temp = tempdir().unwrap();
    let db_path = temp.path().join("features.duckdb");
    let input_path = temp.path().join("contracts.txt");
    let output_path = temp.path().join("samples.md");

    write_feature_db(
        &db_path,
        &[
            TestRow::new("0xseed1", "1")
                .name("Seed One")
                .metadata_doc("shared one"),
            TestRow::new("0xcopy1", "1")
                .name("Seed One")
                .metadata_doc("changed"),
            TestRow::new("0xseed2", "1")
                .name("Seed Two")
                .metadata_doc("shared two"),
            TestRow::new("0xcopy2", "1")
                .name("Different")
                .metadata_doc("shared two"),
        ],
    );
    fs::write(&input_path, "0xseed1\n0xseed2\n").unwrap();

    let report = collect_samples(config(db_path, input_path, output_path)).unwrap();

    assert_eq!(report.seed_reports.len(), 2);
    assert_eq!(report.seed_reports[0].name.matches, vec!["Seed One"]);
    assert!(report.seed_reports[0].metadata.matches.is_empty());
    assert!(report.seed_reports[1].name.matches.is_empty());
    assert_eq!(report.seed_reports[1].metadata.matches, vec!["shared two"]);
}

#[test]
fn collect_samples_reports_serial_in_contract_progress_without_addresses() {
    let temp = tempdir().unwrap();
    let db_path = temp.path().join("features.duckdb");
    let input_path = temp.path().join("contracts.txt");
    let output_path = temp.path().join("samples.md");

    write_feature_db(
        &db_path,
        &[
            TestRow::new("0xseed1", "1")
                .name("Seed One")
                .metadata_doc("shared one"),
            TestRow::new("0xcopy1", "1")
                .name("Seed One")
                .metadata_doc("changed"),
            TestRow::new("0xseed2", "1")
                .name("Seed Two")
                .metadata_doc("shared two"),
            TestRow::new("0xcopy2", "1")
                .name("Different")
                .metadata_doc("shared two"),
        ],
    );
    fs::write(&input_path, "0xseed1\n0xseed2\n").unwrap();

    let mut events = Vec::new();
    collect_samples_with_progress(config(db_path, input_path, output_path), |event| {
        events.push((
            event.seed_index,
            event.total_seeds,
            event.stage,
            event.stage_index,
            event.stage_count,
            event.candidate_count,
        ));
    })
    .unwrap();

    assert_eq!(events.len(), 12);
    assert!(events.iter().all(|event| event.1 == 2));
    assert!(events.iter().all(|event| event.4 == 6));
    assert_eq!(events[0].0, 1);
    assert_eq!(events[0].2, SampleProgressStage::ReadSeedRows);
    assert_eq!(events[5].0, 1);
    assert_eq!(events[5].2, SampleProgressStage::FinishedSeed);
    assert_eq!(events[5].5, Some(1));
    assert_eq!(events[11].0, 2);
    assert_eq!(events[11].2, SampleProgressStage::FinishedSeed);
}
