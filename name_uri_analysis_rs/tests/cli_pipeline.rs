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
                ('ethereum', '0xaaa', '1', 'azuki', 'ipfs://one', 'ipfs://image-one', '{{}}'),
                ('ethereum', '0xaaa', '2', 'azuki', 'ipfs://one-2', 'ipfs://image-one-2', '{{"description":"shared"}}'),
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
        output.join("advisory/metadata-readiness-input.json"),
        output.join("advisory/metadata-production-readiness.json"),
        work.join("manifest.json"),
        work.join("metrics/prepare-phase.json"),
        work.join("metrics/metadata-encode-phase.json"),
        work.join("metrics/name-phase.json"),
        work.join("metrics/metadata-match-phase.json"),
        work.join("metrics/name-algorithm.json"),
        work.join("metrics/metadata-encode.json"),
        work.join("checkpoints/prepare.ready.json"),
        work.join("checkpoints/metadata-encode.ready.json"),
        work.join("checkpoints/name.ready.json"),
        work.join("checkpoints/metadata-match.ready.json"),
        work.join("partial/metadata-encode-summary.json"),
        work.join("partial/metadata-summary.json"),
        work.join("artifacts/metadata/readiness-input.json"),
        work.join("artifacts/metadata/production-readiness.json"),
        work.join("artifacts/metadata/encode-3/features.ready"),
        work.join("artifacts/metadata/blocking-3/blocking.ready"),
    ] {
        assert!(path.is_file(), "missing {}", path.display());
    }
    for recovery_only in [
        "index-1",
        "exact-islands",
        "rescue-plan-1",
        "recall-plan-1",
        "connectivity-runs",
        "component-snapshots",
        "metadata-summary-1",
    ] {
        assert!(!work
            .join("artifacts/metadata/match-1")
            .join(recovery_only)
            .exists());
    }
    let readiness: serde_json::Value = serde_json::from_slice(
        &fs::read(output.join("advisory/metadata-production-readiness.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(readiness["production_ready"], false);

    let summary_manifest_before = fs::read(output.join("summary.manifest.json")).unwrap();
    fs::create_dir_all(output.join("production-evidence")).unwrap();
    fs::write(
        output.join("production-evidence/metadata-v2.json"),
        b"{not valid json",
    )
    .unwrap();
    let refreshed = Command::new(binary)
        .args([
            "--refresh-production-readiness",
            "--output-dir",
            output.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        refreshed.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&refreshed.stdout),
        String::from_utf8_lossy(&refreshed.stderr)
    );
    let refreshed_readiness: serde_json::Value = serde_json::from_slice(
        &fs::read(output.join("advisory/metadata-production-readiness.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(refreshed_readiness["production_ready"], false);
    assert!(refreshed_readiness["blockers"]
        .as_array()
        .unwrap()
        .iter()
        .any(|blocker| blocker
            .as_str()
            .is_some_and(|text| text.contains("invalid"))));
    assert_eq!(
        fs::read(output.join("summary.manifest.json")).unwrap(),
        summary_manifest_before,
        "advisory refresh must not mutate the summary generation"
    );
    let encode_partial: serde_json::Value = serde_json::from_slice(
        &fs::read(work.join("partial/metadata-encode-summary.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        encode_partial["summary_rows"].as_array().unwrap().len(),
        0,
        "Encode must not emit production summary rows"
    );
    let encode_metrics: serde_json::Value =
        serde_json::from_slice(&fs::read(work.join("metrics/metadata-encode.json")).unwrap())
            .unwrap();
    assert_eq!(encode_metrics["schema_version"], 3);
    assert!(encode_metrics["encode_wall_millis"].as_u64().is_some());
    assert!(encode_metrics["blocking_wall_millis"].as_u64().is_some());
    assert!(encode_metrics["token_membership_count"].as_u64().is_some());
    assert!(encode_metrics["routing_membership_count"]
        .as_u64()
        .is_some());
    assert!(encode_metrics["admitted_resident_peak_bytes"]
        .as_u64()
        .is_some());
    assert!(fs::read_dir(work.join("metrics/duckdb-prepare"))
        .unwrap()
        .any(|entry| entry
            .unwrap()
            .path()
            .extension()
            .is_some_and(|ext| ext == "json")));

    let metadata_ready: serde_json::Value = serde_json::from_slice(
        &fs::read(work.join("artifacts/metadata/readiness-input.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        metadata_ready["engine_match_revision"],
        metadata_engine::scoring::MATCH_SEMANTICS_REVISION
    );
    assert_eq!(
        metadata_ready["evidence_gate_revision"],
        metadata_engine::evidence::EVIDENCE_GATE_REVISION
    );
    let metadata_summary: serde_json::Value =
        serde_json::from_slice(&fs::read(work.join("partial/metadata-summary.json")).unwrap())
            .unwrap();
    assert!(metadata_summary["summary_rows"].as_array().is_some());

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
    assert_eq!(manifest["inputs"][0]["row_count"], 4);

    fs::remove_file(output.join("production-evidence/metadata-v2.json")).unwrap();
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
    let resumed_readiness: serde_json::Value = serde_json::from_slice(
        &fs::read(output.join("advisory/metadata-production-readiness.json")).unwrap(),
    )
    .unwrap();
    assert!(resumed_readiness["blockers"]
        .as_array()
        .unwrap()
        .iter()
        .any(|blocker| blocker
            .as_str()
            .is_some_and(|text| text.contains("missing"))));
}
