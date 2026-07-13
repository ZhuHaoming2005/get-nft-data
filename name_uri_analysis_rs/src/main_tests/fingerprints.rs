use super::*;

#[test]
fn row_group_parallelism_warning_uses_effective_worker_count() {
    let temp = tempfile::tempdir().unwrap();
    let mut input = sample_manifest(temp.path()).inputs.remove(0);
    input.row_group_count = 2;

    assert!(row_group_parallelism_warning(&[input.clone()], 2).is_none());
    let warning = row_group_parallelism_warning(&[input], 4).unwrap();
    assert!(warning.contains("2 Parquet row groups"));
    assert!(warning.contains("4 workers"));
}

#[test]
fn row_group_warning_caps_parallelism_at_duckdb_worker_limit() {
    assert_eq!(duckdb_threads_for_row_group_warning(1), 1);
    assert_eq!(duckdb_threads_for_row_group_warning(64), 64);
    assert_eq!(duckdb_threads_for_row_group_warning(128), 64);
}

#[test]
fn phase_metrics_are_written_atomically() {
    let temp = tempfile::tempdir().unwrap();
    let metric = PhaseMetric {
        phase: "prepare",
        wall_millis: 42,
        cpu_millis: 21,
        success: true,
        input_rows: 100,
        summary_rows: 4,
        peak_rss_bytes: 1024,
        peak_duckdb_temp_bytes: 2048,
        io_read_bytes: 4096,
        io_written_bytes: 8192,
        database_bytes: 256,
        artifact_bytes: 128,
    };

    write_metric_atomically(temp.path(), &metric).unwrap();

    let destination = temp.path().join("metrics/prepare-phase.json");
    let value: serde_json::Value = serde_json::from_slice(&fs::read(destination).unwrap()).unwrap();
    assert_eq!(value["wall_millis"], 42);
    assert_eq!(value["success"], true);
    assert_eq!(value["peak_rss_bytes"], 1024);
    assert_eq!(value["cpu_millis"], 21);
    assert_eq!(value["peak_duckdb_temp_bytes"], 2048);
    assert!(!temp
        .path()
        .join("metrics/prepare-phase.json.partial")
        .exists());
}

#[test]
fn phase_metric_write_failure_is_noncritical() {
    let temp = tempfile::tempdir().unwrap();
    let blocked_work_directory = temp.path().join("blocked");
    fs::write(&blocked_work_directory, b"not a directory").unwrap();
    let metric = PhaseMetric {
        phase: "prepare",
        wall_millis: 1,
        cpu_millis: 1,
        success: true,
        input_rows: 1,
        summary_rows: 1,
        peak_rss_bytes: 1,
        peak_duckdb_temp_bytes: 1,
        io_read_bytes: 1,
        io_written_bytes: 1,
        database_bytes: 1,
        artifact_bytes: 1,
    };

    record_phase_metric(&blocked_work_directory, &metric);

    assert!(blocked_work_directory.is_file());
}

#[test]
fn failed_post_success_cleanup_does_not_turn_success_into_an_error() {
    let temp = tempfile::tempdir().unwrap();
    let not_a_directory = temp.path().join("work");
    fs::write(&not_a_directory, b"occupied by a file").unwrap();

    remove_work_directory_after_success(&not_a_directory);

    assert!(not_a_directory.is_file());
}

#[test]
fn input_fingerprint_records_file_order_rows_and_schema() {
    let temp = tempfile::tempdir().unwrap();
    let first = temp.path().join("first.parquet");
    let second = temp.path().join("second.parquet");
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(&format!(
        "COPY (SELECT 1::INTEGER AS id, 'ethereum'::VARCHAR AS chain) TO {} (FORMAT PARQUET);\
         COPY (SELECT * FROM (VALUES (1, 'base'), (2, 'base')) AS t(id, chain)) TO {} (FORMAT PARQUET);",
        parquet_sql_literal(&first),
        parquet_sql_literal(&second)
    ))
    .unwrap();

    let fingerprints = fingerprint_inputs(&[second.clone(), first.clone()]).unwrap();

    assert_eq!(fingerprints.len(), 2);
    assert_eq!(fingerprints[0].file_id, 0);
    assert_eq!(fingerprints[0].path, second.canonicalize().unwrap());
    assert_eq!(fingerprints[0].row_count, 2);
    assert_eq!(fingerprints[1].file_id, 1);
    assert_eq!(fingerprints[1].row_count, 1);
    assert_eq!(fingerprints[0].schema_sha256.len(), 64);
    assert_eq!(fingerprints[0].schema_sha256, fingerprints[1].schema_sha256);
}

#[test]
fn input_fingerprint_rejects_duplicate_canonical_files() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = temp.path().join("sample.parquet");
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(&format!(
        "COPY (SELECT 1::INTEGER AS id, 'ethereum'::VARCHAR AS chain) TO {} (FORMAT PARQUET);",
        parquet_sql_literal(&parquet)
    ))
    .unwrap();

    let error = fingerprint_inputs(&[parquet.clone(), parquet]).unwrap_err();

    assert!(error.to_string().contains("duplicate Parquet input"));
}
