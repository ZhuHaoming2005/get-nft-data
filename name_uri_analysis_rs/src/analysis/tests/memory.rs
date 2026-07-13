use super::*;

#[test]
fn explicit_analysis_memory_limit_stays_inside_total_budget() {
    let plan = name_analysis_memory_plan("10GB", Some("16KB"), 0).unwrap();

    assert_eq!(plan.analysis_bytes, 16 * 1024);
}

#[test]
fn explicit_analysis_memory_limit_rejects_over_budget_value() {
    let error = name_analysis_memory_plan("1GB", Some("2GB"), 0).unwrap_err();

    assert!(error.to_string().contains("exceeds total --memory-limit"));
}

#[test]
fn explicit_analysis_memory_limit_is_a_hard_resident_limit() {
    let error = name_analysis_memory_plan("10GB", Some("16KB"), 32 * 1024).unwrap_err();

    assert!(error.to_string().contains("resident name state"));
    assert!(error.to_string().contains("16384B"));
}

#[test]
fn analysis_memory_auto_uses_total_budget_auto_balance() {
    let default_plan = name_analysis_memory_plan("4GB", None, 0).unwrap();
    let auto_plan = name_analysis_memory_plan("4GB", Some("auto"), 0).unwrap();

    assert_eq!(default_plan.analysis_bytes, 4 * 1024 * 1024 * 1024);
    assert_eq!(auto_plan.analysis_bytes, default_plan.analysis_bytes);
}

#[test]
fn metadata_memory_budget_accepts_auto() {
    assert!(total_memory_budget_bytes("auto").unwrap() > 0);
}

#[test]
fn controller_memory_validation_accepts_auto_and_rejects_invalid_static_limits() {
    validate_static_memory_options("auto", Some("auto"), "auto").unwrap();

    let analysis_error =
        validate_static_memory_options("unbounded", Some("1GiB"), "1GiB").unwrap_err();
    assert!(analysis_error
        .to_string()
        .contains("invalid analysis memory limit"));

    let duckdb_error =
        validate_static_memory_options("1GiB", Some("auto"), "automatic").unwrap_err();
    assert!(duckdb_error
        .to_string()
        .contains("invalid analysis memory limit"));
}

#[test]
fn diagnostic_environment_flag_is_explicit() {
    assert!(!diagnostics_requested(None));
    assert!(!diagnostics_requested(Some(std::ffi::OsStr::new("0"))));
    assert!(diagnostics_requested(Some(std::ffi::OsStr::new("1"))));
    assert!(diagnostics_requested(Some(std::ffi::OsStr::new("true"))));
}

#[test]
fn duckdb_configuration_does_not_parse_memory_limit() {
    let conn = Connection::open_in_memory().unwrap();
    let options = AnalysisOptions {
        database_path: PathBuf::from(":memory:"),
        parquet_inputs: Vec::new(),
        output_dir: PathBuf::from("unused"),
        name_threshold: 95.0,
        metadata_recall_mode: MetadataRecallMode::Conservative,
        threads: 1,
        memory_limit: "not-a-size".into(),
        analysis_memory_limit: None,
        duckdb_memory_limit: "1GB".into(),
        temp_directory: None,
        progress: false,
    };

    configure_duckdb(&conn, &options).unwrap();
}

#[test]
fn duckdb_threads_are_capped_at_the_64_physical_core_target() {
    let conn = Connection::open_in_memory().unwrap();
    let options = AnalysisOptions {
        database_path: PathBuf::from(":memory:"),
        parquet_inputs: Vec::new(),
        output_dir: PathBuf::from("unused"),
        name_threshold: 95.0,
        metadata_recall_mode: MetadataRecallMode::Conservative,
        threads: 128,
        memory_limit: "384GiB".into(),
        analysis_memory_limit: Some("384GiB".into()),
        duckdb_memory_limit: "320GiB".into(),
        temp_directory: None,
        progress: false,
    };

    configure_duckdb(&conn, &options).unwrap();

    let threads = conn
        .query_row("SELECT current_setting('threads')::UBIGINT", [], |row| {
            row.get::<_, u64>(0)
        })
        .unwrap();
    assert_eq!(threads, 64);
}

#[test]
fn prepare_only_uri_tables_are_released_before_metadata_compaction() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TEMP TABLE contract_dim(value INTEGER);
         CREATE TEMP TABLE uri_rows(value INTEGER);
         CREATE TEMP TABLE uri_key_contracts(value INTEGER);
         CREATE TEMP TABLE uri_duplicate_key_stats(value INTEGER);
         CREATE TEMP TABLE uri_cross_chain_keys(value INTEGER);
         CREATE TEMP TABLE uri_contract_flags(value INTEGER);
         CREATE TEMP TABLE uri_chain_pair_contract_flags(value INTEGER);",
    )
    .unwrap();

    drop_prepare_only_uri_tables(&conn).unwrap();

    for table in [
        "contract_dim",
        "uri_rows",
        "uri_key_contracts",
        "uri_duplicate_key_stats",
        "uri_cross_chain_keys",
        "uri_contract_flags",
        "uri_chain_pair_contract_flags",
    ] {
        let exists: bool = conn
            .query_row(
                "SELECT count(*) > 0 FROM duckdb_tables() WHERE table_name = ?",
                [table],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!exists, "temporary table still present: {table}");
    }
}

#[test]
fn rust_heavy_phases_clamp_duckdb_without_raising_smaller_limits() {
    let mut options = AnalysisOptions {
        database_path: PathBuf::from(":memory:"),
        parquet_inputs: Vec::new(),
        output_dir: PathBuf::from("unused"),
        name_threshold: 95.0,
        metadata_recall_mode: MetadataRecallMode::Conservative,
        threads: 1,
        memory_limit: "192GiB".into(),
        analysis_memory_limit: Some("192GiB".into()),
        duckdb_memory_limit: "160GiB".into(),
        temp_directory: None,
        progress: false,
    };
    assert_eq!(
        phase_duckdb_memory_limit(&options, NAME_DUCKDB_MEMORY_CAP).unwrap(),
        "8GiB"
    );
    assert_eq!(
        phase_duckdb_memory_limit(&options, METADATA_DUCKDB_MEMORY_CAP).unwrap(),
        "32GiB"
    );

    options.duckdb_memory_limit = "4GiB".to_string();
    assert_eq!(
        phase_duckdb_memory_limit(&options, NAME_DUCKDB_MEMORY_CAP).unwrap(),
        "4GiB"
    );
}

#[test]
fn auto_memory_plan_rejects_resident_atoms_over_budget() {
    let error = name_analysis_memory_plan("1GB", None, 2 * 1024 * 1024 * 1024).unwrap_err();

    assert!(error.to_string().contains("loaded name atoms need"));
}
