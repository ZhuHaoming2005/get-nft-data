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
fn match_forecast_requires_eight_fresh_successes_with_an_exact_key() {
    let temp = tempfile::tempdir().unwrap();
    let manifest = sample_manifest(temp.path());
    let artifacts = temp.path().join("artifacts/metadata");
    fs::create_dir_all(artifacts.join("encode-3")).unwrap();
    fs::create_dir_all(artifacts.join("blocking-3")).unwrap();
    fs::write(
        artifacts.join("encode-3/features.ready"),
        br#"{"schema_revision":3,"source_count":1000,"payload_count":500,"chains":[],"chain_totals":[]}"#,
    )
    .unwrap();
    fs::write(
        artifacts.join("blocking-3/blocking.ready"),
        br#"{"blocking_revision":3,"atom_count":300}"#,
    )
    .unwrap();
    let key = match_observation_key(&manifest, temp.path()).unwrap();

    for wall_millis in 1..=7 {
        record_match_observation(
            &manifest.options.output_dir,
            &MatchObservation::for_test(
                key.clone(),
                MatchExecutionKind::Fresh,
                MatchOutcome::Success,
                wall_millis * 1_000,
            ),
        )
        .unwrap();
    }
    let warming = load_match_eta_forecast(&manifest.options.output_dir, &key).unwrap();
    assert_eq!(warming.sample_count, 7);
    assert_eq!(warming.lower_total_millis, None);
    assert_eq!(warming.upper_total_millis, None);

    record_match_observation(
        &manifest.options.output_dir,
        &MatchObservation::for_test(
            key.clone(),
            MatchExecutionKind::Fresh,
            MatchOutcome::Success,
            8_000,
        ),
    )
    .unwrap();
    let calibrated = load_match_eta_forecast(&manifest.options.output_dir, &key).unwrap();
    assert_eq!(calibrated.sample_count, 8);
    assert_eq!(calibrated.lower_total_millis, Some(1_000));
    assert_eq!(calibrated.upper_total_millis, Some(8_000));

    let mut different_revision = key.clone();
    different_revision.controller_match_revision += 1;
    let rejected =
        load_match_eta_forecast(&manifest.options.output_dir, &different_revision).unwrap();
    assert_eq!(rejected.sample_count, 0);
    assert_eq!(rejected.upper_total_millis, None);
}

#[test]
fn failures_and_resume_recomputes_never_calibrate_fresh_match_eta() {
    let temp = tempfile::tempdir().unwrap();
    let manifest = sample_manifest(temp.path());
    let artifacts = temp.path().join("artifacts/metadata");
    fs::create_dir_all(artifacts.join("encode-3")).unwrap();
    fs::create_dir_all(artifacts.join("blocking-3")).unwrap();
    fs::write(
        artifacts.join("encode-3/features.ready"),
        br#"{"schema_revision":3,"source_count":10,"payload_count":5,"chains":[],"chain_totals":[]}"#,
    )
    .unwrap();
    fs::write(
        artifacts.join("blocking-3/blocking.ready"),
        br#"{"blocking_revision":3,"atom_count":3}"#,
    )
    .unwrap();
    let key = match_observation_key(&manifest, temp.path()).unwrap();

    for (execution, outcome) in [
        (MatchExecutionKind::Fresh, MatchOutcome::Failure),
        (MatchExecutionKind::ResumeRecompute, MatchOutcome::Success),
        (MatchExecutionKind::ResumeRecompute, MatchOutcome::Failure),
    ] {
        record_match_observation(
            &manifest.options.output_dir,
            &MatchObservation::for_test(key.clone(), execution, outcome, 1_000),
        )
        .unwrap();
    }

    let forecast = load_match_eta_forecast(&manifest.options.output_dir, &key).unwrap();
    assert_eq!(forecast.sample_count, 0);
    assert_eq!(forecast.upper_total_millis, None);
}

#[test]
fn match_observation_partitions_are_rolling_and_bounded() {
    let temp = tempfile::tempdir().unwrap();
    let manifest = sample_manifest(temp.path());
    let artifacts = temp.path().join("artifacts/metadata");
    fs::create_dir_all(artifacts.join("encode-3")).unwrap();
    fs::create_dir_all(artifacts.join("blocking-3")).unwrap();
    fs::write(
        artifacts.join("encode-3/features.ready"),
        br#"{"schema_revision":3,"source_count":10,"payload_count":5,"chains":[],"chain_totals":[]}"#,
    )
    .unwrap();
    fs::write(
        artifacts.join("blocking-3/blocking.ready"),
        br#"{"blocking_revision":3,"atom_count":3}"#,
    )
    .unwrap();
    let key = match_observation_key(&manifest, temp.path()).unwrap();
    for wall_millis in 0..300 {
        record_match_observation(
            &manifest.options.output_dir,
            &MatchObservation::for_test(
                key.clone(),
                MatchExecutionKind::Fresh,
                MatchOutcome::Failure,
                wall_millis,
            ),
        )
        .unwrap();
    }

    let partition = temp
        .path()
        .join(".name-uri-analysis-history/metadata-match-v3/fresh-failure");
    assert!(fs::read_dir(partition).unwrap().count() <= 256);
}

#[test]
fn match_scale_key_rejects_equal_counts_with_different_membership_density() {
    let temp = tempfile::tempdir().unwrap();
    let manifest = sample_manifest(temp.path());
    let encode = temp.path().join("artifacts/metadata/encode-3");
    let blocking = temp.path().join("artifacts/metadata/blocking-3");
    fs::create_dir_all(&encode).unwrap();
    fs::create_dir_all(&blocking).unwrap();
    fs::write(
        encode.join("features.ready"),
        br#"{"schema_revision":3,"source_count":1000,"payload_count":500,"chains":[],"chain_totals":[]}"#,
    )
    .unwrap();
    fs::write(
        blocking.join("blocking.ready"),
        br#"{"blocking_revision":3,"atom_count":300}"#,
    )
    .unwrap();
    fs::write(encode.join("token_member_contracts.u32"), vec![0; 128]).unwrap();
    let sparse = match_observation_key(&manifest, temp.path()).unwrap();

    fs::write(encode.join("token_member_contracts.u32"), vec![0; 16_384]).unwrap();
    let dense = match_observation_key(&manifest, temp.path()).unwrap();

    assert_ne!(sparse, dense);
    assert_eq!(
        load_match_eta_forecast(&manifest.options.output_dir, &dense)
            .unwrap()
            .sample_count,
        0
    );
}

#[test]
fn match_scale_key_rejects_equal_bytes_with_different_candidate_pair_work() {
    let temp = tempfile::tempdir().unwrap();
    let manifest = sample_manifest(temp.path());
    let encode = temp.path().join("artifacts/metadata/encode-3");
    let blocking = temp.path().join("artifacts/metadata/blocking-3");
    fs::create_dir_all(&encode).unwrap();
    fs::create_dir_all(&blocking).unwrap();
    fs::write(encode.join("token_member_contracts.u32"), vec![0; 128]).unwrap();
    fs::write(encode.join("fallback_atoms_members.u32"), vec![0; 128]).unwrap();
    fs::write(blocking.join("block_atoms.u32"), vec![0; 128]).unwrap();
    fs::write(blocking.join("atom_block_ids.u32"), vec![0; 128]).unwrap();
    fs::write(
        encode.join("features.ready"),
        br#"{
        "schema_revision":3,"source_count":1000,"payload_count":500,
        "token_pair_work":64,"max_token_members":8,
        "fallback_pair_work":32,"max_fallback_members":4,
        "chains":[],"chain_totals":[]
    }"#,
    )
    .unwrap();
    fs::write(
        blocking.join("blocking.ready"),
        br#"{
        "blocking_revision":3,"atom_count":300,
        "block_pair_work":128,"max_block_members":16
    }"#,
    )
    .unwrap();
    let balanced = match_observation_key(&manifest, temp.path()).unwrap();

    fs::write(
        blocking.join("blocking.ready"),
        br#"{
        "blocking_revision":3,"atom_count":300,
        "block_pair_work":128,"contract_expansion_pair_work":8192,
        "max_block_members":16
    }"#,
    )
    .unwrap();
    let expansion_heavy = match_observation_key(&manifest, temp.path()).unwrap();
    assert_ne!(balanced, expansion_heavy);

    fs::write(
        encode.join("features.ready"),
        br#"{
        "schema_revision":3,"source_count":1000,"payload_count":500,
        "token_pair_work":4096,"max_token_members":128,
        "fallback_pair_work":2048,"max_fallback_members":64,
        "chains":[],"chain_totals":[]
    }"#,
    )
    .unwrap();
    let skewed = match_observation_key(&manifest, temp.path()).unwrap();

    assert_ne!(balanced, skewed);
}

#[test]
fn match_observation_history_survives_work_cleanup_and_names_sampled_resources() {
    let temp = tempfile::tempdir().unwrap();
    let output = temp.path().join("results/output");
    let work = temp.path().join("ephemeral-work");
    fs::create_dir_all(&work).unwrap();
    let mut manifest = sample_manifest(&work);
    manifest.options.output_dir = output.clone();
    let encode = work.join("artifacts/metadata/encode-3");
    let blocking = work.join("artifacts/metadata/blocking-3");
    fs::create_dir_all(&encode).unwrap();
    fs::create_dir_all(&blocking).unwrap();
    fs::write(
        encode.join("features.ready"),
        br#"{"schema_revision":3,"source_count":10,"payload_count":5,"chains":[],"chain_totals":[]}"#,
    )
    .unwrap();
    fs::write(
        blocking.join("blocking.ready"),
        br#"{"blocking_revision":3,"atom_count":3}"#,
    )
    .unwrap();
    let key = match_observation_key(&manifest, &work).unwrap();
    record_match_observation(
        &output,
        &MatchObservation::new(
            key,
            MatchExecutionKind::Fresh,
            MatchOutcome::Success,
            123,
            MatchSampledResources {
                peak_rss_bytes: 456,
                io_read_bytes: 789,
                io_written_bytes: 1_024,
                sample_interval_millis: 200,
            },
        ),
    )
    .unwrap();
    fs::remove_dir_all(&work).unwrap();

    let partition = temp
        .path()
        .join("results/.name-uri-analysis-history/metadata-match-v3/fresh-success");
    let observation_path = fs::read_dir(partition)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let value: serde_json::Value =
        serde_json::from_slice(&fs::read(observation_path).unwrap()).unwrap();
    assert_eq!(value["wall_millis"], 123);
    assert_eq!(value["sampled_peak_rss_bytes"], 456);
    assert_eq!(value["sampled_io_read_bytes"], 789);
    assert_eq!(value["sampled_io_written_bytes"], 1_024);
    assert_eq!(value["sample_interval_millis"], 200);
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
