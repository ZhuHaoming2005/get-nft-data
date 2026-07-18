use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use sha2::{Digest, Sha256};
use std::fs::{self, File};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;

const COLUMNS: [&str; 7] = [
    "chain",
    "contract_address",
    "token_id",
    "name_norm",
    "token_uri_norm",
    "image_uri_norm",
    "metadata_json",
];

fn write_parquet(path: &Path) {
    let schema = Arc::new(Schema::new(
        COLUMNS
            .iter()
            .map(|name| Field::new(*name, DataType::Utf8, false))
            .collect::<Vec<_>>(),
    ));
    let mut columns = vec![Vec::new(); COLUMNS.len()];
    for contract in 0..4 {
        let chain = if contract < 2 { "ethereum" } else { "solana" };
        for token in 0..2 {
            columns[0].push(chain.to_owned());
            columns[1].push(format!("contract-{contract}"));
            columns[2].push(token.to_string());
            columns[3].push(if contract == 3 {
                "collectiom".to_owned()
            } else {
                "collection".to_owned()
            });
            columns[4].push(format!("ipfs://shared/{token}"));
            columns[5].push(format!("ipfs://images/{token}"));
            columns[6].push(format!(
                r#"{{"collection":{{"name":"shared"}},"name":"token {token}","value":{token}}}"#
            ));
        }
    }
    let arrays: Vec<ArrayRef> = columns
        .into_iter()
        .map(|values| Arc::new(StringArray::from(values)) as ArrayRef)
        .collect();
    let batch = RecordBatch::try_new(Arc::clone(&schema), arrays).unwrap();
    let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

#[test]
fn all_command_produces_versioned_results() {
    let temp = tempfile::tempdir().unwrap();
    write_parquet(&temp.path().join("input.parquet"));
    let config = r#"
input_files = ["input.parquet"]
output_dir = "result"
temporary_volumes = ["tmp", "tmp2"]
chains = ["ethereum", "solana"]
evm_chains = ["ethereum"]
memory_limit = 1073741824
entity_execution_mode = "auto"
uri_execution_mode = "auto"
metadata_execution_mode = "auto"
name_threshold = 95.0
metadata_content_threshold = 0.6
metadata_anchor_tokens = 2

[stage_concurrency]
preflight = 1
entity = 1
name = 1
uri = 2
metadata = 1
report = 1

[metadata_prefilter_parameters]
template_jaccard_threshold = 0.75
lsh_bands = 8
lsh_rows_per_band = 2
target_candidate_recall = 0.9
neighbors_per_target_chain = 4
max_candidates_per_target_chain = 16
max_outgoing_candidates_per_contract = 32
exact_bucket_size_cap = 16

[metadata_guard_parameters]
min_anchor_documents = 2
stable_value_min_anchors = 2
stable_value_support_ratio = 0.8

[work_budgets]
name_scored_candidates = 10000
metadata_prefilter_pairs = 10000
metadata_verify_pairs = 10000

[quality_gate]
metadata_recall = 1.0
minimum_positive_pairs = 1
sample_seed = 7
"#;
    let config_path = temp.path().join("config.toml");
    fs::write(&config_path, config).unwrap();
    let status = Command::new(env!("CARGO_BIN_EXE_dedup"))
        .args([
            "--config",
            config_path.to_str().unwrap(),
            "--diagnostic",
            "--progress",
            "json",
            "--progress-interval-ms",
            "100",
            "all",
        ])
        .stderr(Stdio::null())
        .status()
        .unwrap();
    assert!(status.success());
    let result = temp.path().join("result");
    for file in [
        "summary.csv",
        "chain_matrix.csv",
        "run_manifest.json",
        "run/recall_audit.json",
        "run/progress.json",
        "run/progress.jsonl",
        "run/entity_execution.json",
        "run/name_resource_plan.json",
        "run/uri_resource_plan.json",
        "run/metadata_resource_plan.json",
        "run/entities/_SUCCESS",
        "run/name-hits/_SUCCESS",
        "run/uri-hits/_SUCCESS",
        "run/metadata-hits/_SUCCESS",
    ] {
        assert!(result.join(file).is_file(), "missing {file}");
    }
    let progress: serde_json::Value =
        serde_json::from_slice(&fs::read(result.join("run/progress.json")).unwrap()).unwrap();
    assert_eq!(progress["status"], "complete");
    assert!(progress["stage"].is_string());
    assert!(progress["completed"].is_u64());
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(result.join("run_manifest.json")).unwrap()).unwrap();
    assert_eq!(
        manifest["runtime_decisions"]["name_storage"],
        "resident_only"
    );
    assert_eq!(
        manifest["runtime_decisions"]["name_over_budget_policy"],
        "warn_and_continue"
    );
    assert_eq!(manifest["neighbors_per_target_chain"], 4);
    assert!(manifest["resource_plans"]["uri"].is_object());
    let uri_plan: serde_json::Value =
        serde_json::from_slice(&fs::read(result.join("run/uri_resource_plan.json")).unwrap())
            .unwrap();
    assert_eq!(uri_plan["radix_volumes"].as_array().unwrap().len(), 2);
    assert_eq!(uri_plan["hit_sink_shards"], 2);
    let metadata_plan: serde_json::Value =
        serde_json::from_slice(&fs::read(result.join("run/metadata_resource_plan.json")).unwrap())
            .unwrap();
    assert_eq!(metadata_plan["radix_volumes"].as_array().unwrap().len(), 2);
    let hit_checksum_before = [
        tree_checksum(&result.join("run/name-hits")),
        tree_checksum(&result.join("run/uri-hits")),
        tree_checksum(&result.join("run/metadata-hits")),
    ];
    let status = Command::new(env!("CARGO_BIN_EXE_dedup"))
        .args([
            "--config",
            config_path.to_str().unwrap(),
            "--diagnostic",
            "--progress",
            "off",
            "all",
        ])
        .status()
        .unwrap();
    assert!(status.success());
    assert_eq!(
        [
            tree_checksum(&result.join("run/name-hits")),
            tree_checksum(&result.join("run/uri-hits")),
            tree_checksum(&result.join("run/metadata-hits")),
        ],
        hit_checksum_before
    );
}

fn tree_checksum(root: &Path) -> [u8; 32] {
    let mut paths = Vec::new();
    let mut pending = vec![root.to_owned()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else {
                paths.push(path);
            }
        }
    }
    paths.sort();
    let mut digest = Sha256::new();
    for path in paths {
        digest.update(
            path.strip_prefix(root)
                .unwrap()
                .to_string_lossy()
                .as_bytes(),
        );
        digest.update(fs::read(path).unwrap());
    }
    digest.finalize().into()
}
