use super::*;

#[test]
fn cli_rejects_removed_physical_cores_option() {
    let error = Args::try_parse_from([
        "name_uri_analysis_rs",
        "--parquet",
        "input.parquet",
        "--physical-cores",
        "32",
    ])
    .unwrap_err();

    assert!(error.to_string().contains("--physical-cores"));
}

#[test]
fn cli_defaults_to_128_worker_threads() {
    let args =
        Args::try_parse_from(["name_uri_analysis_rs", "--parquet", "input.parquet"]).unwrap();

    assert_eq!(args.threads, 128);
}

#[test]
fn cli_defaults_to_conservative_metadata_recall_and_allows_exact() {
    let defaults =
        Args::try_parse_from(["name_uri_analysis_rs", "--parquet", "input.parquet"]).unwrap();
    let exact = Args::try_parse_from([
        "name_uri_analysis_rs",
        "--parquet",
        "input.parquet",
        "--metadata-recall-mode",
        "exact",
    ])
    .unwrap();

    assert_eq!(
        defaults.metadata_recall_mode,
        MetadataRecallMode::Conservative
    );
    assert_eq!(exact.metadata_recall_mode, MetadataRecallMode::Exact);
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
fn cli_rejects_removed_database_option() {
    let error = Args::try_parse_from([
        "name_uri_analysis_rs",
        "--parquet",
        "input.parquet",
        "--database",
        "stage.duckdb",
    ])
    .unwrap_err();

    assert!(error.to_string().contains("--database"));
}

#[test]
fn cli_uses_target_memory_defaults() {
    let args =
        Args::try_parse_from(["name_uri_analysis_rs", "--parquet", "input.parquet"]).unwrap();

    assert_eq!(args.duckdb_memory_limit, "320GiB");
    assert_eq!(args.analysis_memory_limit, "384GiB");
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
fn cli_rejects_removed_thresholds_option() {
    let error = Args::try_parse_from([
        "name_uri_analysis_rs",
        "--parquet",
        "input.parquet",
        "--thresholds",
        "95,96",
    ])
    .unwrap_err();

    assert!(error.to_string().contains("--thresholds"));
}

#[test]
fn effective_threads_never_exceed_visible_cpus() {
    let visible = std::thread::available_parallelism()
        .map(|value| value.get())
        .unwrap_or(1);

    assert_eq!(resolve_worker_threads(usize::MAX), visible);
    assert_eq!(resolve_worker_threads(1), 1);
}
