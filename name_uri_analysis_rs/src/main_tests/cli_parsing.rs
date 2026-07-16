use super::*;

#[test]
fn cli_defaults_to_128_worker_threads() {
    let args =
        Args::try_parse_from(["name_uri_analysis_rs", "--parquet", "input.parquet"]).unwrap();

    assert_eq!(args.threads, 128);
}

#[test]
fn cli_rejects_zero_worker_threads() {
    let error = Args::try_parse_from([
        "name_uri_analysis_rs",
        "--parquet",
        "input.parquet",
        "--threads",
        "0",
    ])
    .unwrap_err();

    assert!(error.to_string().contains("--threads"));
}

#[test]
fn cli_uses_target_memory_defaults() {
    let args =
        Args::try_parse_from(["name_uri_analysis_rs", "--parquet", "input.parquet"]).unwrap();

    assert_eq!(args.duckdb_memory_limit, "320GiB");
    assert_eq!(args.analysis_memory_limit, "448GiB");
}

#[test]
fn readiness_refresh_does_not_require_parquet_inputs() {
    let args = Args::try_parse_from([
        "name_uri_analysis_rs",
        "--refresh-production-readiness",
        "--output-dir",
        "output",
    ])
    .unwrap();

    assert!(args.refresh_production_readiness);
    assert!(args.parquet_inputs.is_empty());
}

#[test]
fn normal_analysis_still_requires_parquet_inputs() {
    let error =
        Args::try_parse_from(["name_uri_analysis_rs", "--output-dir", "output"]).unwrap_err();

    assert!(error.to_string().contains("--parquet"));
}

#[test]
fn cli_exposes_one_name_threshold_and_resume_controls() {
    let args = Args::try_parse_from([
        "name_uri_analysis_rs",
        "--parquet",
        "input.parquet",
        "--name-threshold",
        "96.5",
        "--resume",
        "--keep-work-directory",
    ])
    .unwrap();

    assert_eq!(args.name_threshold, 96.5);
    assert!(args.resume);
    assert!(args.keep_work_directory);
}

#[test]
fn expensive_diagnostics_are_opt_in() {
    let defaults =
        Args::try_parse_from(["name_uri_analysis_rs", "--parquet", "input.parquet"]).unwrap();
    let enabled = Args::try_parse_from([
        "name_uri_analysis_rs",
        "--parquet",
        "input.parquet",
        "--diagnostics",
    ])
    .unwrap();

    assert!(!defaults.diagnostics);
    assert!(enabled.diagnostics);
}

#[test]
fn effective_threads_never_exceed_visible_cpus() {
    let visible = std::thread::available_parallelism()
        .map(|value| value.get())
        .unwrap_or(1);

    assert_eq!(resolve_worker_threads(usize::MAX), visible);
    assert_eq!(resolve_worker_threads(1), 1);
}

#[test]
fn cli_accepts_ephemeral_memory_and_phase_thread_overrides() {
    let args = Args::try_parse_from([
        "name_uri_analysis_rs",
        "--parquet",
        "input.parquet",
        "--ephemeral-in-memory",
        "--prepare-threads",
        "32",
        "--metadata-encode-threads",
        "96",
        "--name-threads",
        "64",
        "--metadata-match-threads",
        "128",
        "--duckdb-threads",
        "96",
        "--disable-numa-interleave",
    ])
    .unwrap();

    assert!(args.ephemeral_in_memory);
    assert_eq!(args.prepare_threads, Some(32));
    assert_eq!(args.metadata_encode_threads, Some(96));
    assert_eq!(args.name_threads, Some(64));
    assert_eq!(args.metadata_match_threads, Some(128));
    assert_eq!(args.duckdb_threads, Some(96));
    assert!(args.disable_numa_interleave);
}
