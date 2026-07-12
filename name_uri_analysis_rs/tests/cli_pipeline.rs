use std::fs;
use std::path::Path;
use std::process::Command;

use duckdb::Connection;

fn sql_path(path: &Path) -> String {
    format!(
        "'{}'",
        path.display()
            .to_string()
            .replace('\\', "/")
            .replace('\'', "''")
    )
}

#[test]
fn controller_rejects_invalid_memory_before_touching_parquet() {
    let temp = tempfile::tempdir().unwrap();
    let missing_input = temp.path().join("missing.parquet");
    let result = Command::new(env!("CARGO_BIN_EXE_name_uri_analysis_rs"))
        .args([
            "--parquet",
            missing_input.to_str().unwrap(),
            "--analysis-memory-limit",
            "unbounded",
        ])
        .output()
        .unwrap();

    assert!(!result.status.success());
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(
        stderr.contains("invalid analysis memory limit"),
        "stderr:\n{stderr}"
    );
    assert!(!stderr.contains("missing.parquet"), "stderr:\n{stderr}");
}

#[test]
fn public_controller_runs_all_children_and_resumes_finalized_pipeline() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("input.parquet");
    let output = temp.path().join("output");
    let work = temp.path().join("work");
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(&format!(
        r#"
        COPY (
            SELECT * FROM (VALUES
                ('ethereum', '0xaaa', '1', 'azuki', 'ipfs://one', 'ipfs://image-one', '{{"description":"shared"}}'),
                ('ethereum', '0xbbb', '1', 'azukii', 'ipfs://two', 'ipfs://image-two', '{{"description":"shared"}}'),
                ('base', '0xccc', '1', 'azuki', 'ipfs://one', 'ipfs://image-three', '{{"description":"shared"}}')
            ) rows(chain, contract_address, token_id, name_norm, token_uri_norm, image_uri_norm, metadata_json)
        ) TO {} (FORMAT PARQUET);
        "#,
        sql_path(&input)
    ))
    .unwrap();
    drop(conn);

    let binary = env!("CARGO_BIN_EXE_name_uri_analysis_rs");
    let common_args = [
        "--parquet",
        input.to_str().unwrap(),
        "--output-dir",
        output.to_str().unwrap(),
        "--work-directory",
        work.to_str().unwrap(),
        "--threads",
        "2",
        "--duckdb-memory-limit",
        "512MiB",
        "--analysis-memory-limit",
        "512MiB",
        "--keep-work-directory",
        "--no-progress",
        "--diagnostics",
    ];
    let first = Command::new(binary).args(common_args).output().unwrap();
    assert!(
        first.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&first.stdout),
        String::from_utf8_lossy(&first.stderr)
    );

    for path in [
        output.join("summary.json"),
        output.join("summary.csv"),
        output.join("summary.manifest.json"),
        work.join("manifest.json"),
        work.join("metrics/prepare-phase.json"),
        work.join("metrics/name-phase.json"),
        work.join("metrics/metadata-phase.json"),
        work.join("metrics/name-algorithm.json"),
        work.join("metrics/metadata-algorithm.json"),
        work.join("checkpoints/prepare.ready.json"),
        work.join("checkpoints/name.ready.json"),
        work.join("checkpoints/metadata.ready.json"),
    ] {
        assert!(path.is_file(), "missing {}", path.display());
    }
    assert!(fs::read_dir(work.join("metrics/duckdb-prepare"))
        .unwrap()
        .any(|entry| entry
            .unwrap()
            .path()
            .extension()
            .is_some_and(|ext| ext == "json")));

    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(work.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest["stages"]["finalized"]["complete"], true);
    let finalized_artifacts = manifest["stages"]["finalized"]["artifacts"]
        .as_array()
        .unwrap();
    assert_eq!(finalized_artifacts.len(), 3);
    assert!(finalized_artifacts.iter().any(|artifact| {
        artifact["path"]
            .as_str()
            .is_some_and(|path| path.ends_with("summary.manifest.json"))
    }));
    assert_eq!(manifest["inputs"][0]["file_id"], 0);
    assert_eq!(manifest["inputs"][0]["row_count"], 3);

    let resumed = Command::new(binary)
        .args(common_args)
        .arg("--resume")
        .output()
        .unwrap();
    assert!(
        resumed.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&resumed.stdout),
        String::from_utf8_lossy(&resumed.stderr)
    );
    assert!(String::from_utf8_lossy(&resumed.stdout).contains("reused"));
}
