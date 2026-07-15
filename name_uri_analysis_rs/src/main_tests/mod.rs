use super::*;

fn ensure_phase_idle(work_directory: &Path) -> Result<(), Box<dyn std::error::Error>> {
    drop(PhaseLock::acquire(work_directory)?);
    Ok(())
}

fn validate_directory_layout(
    work_directory: &Path,
    output_directory: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    resolve_directory_layout(work_directory, output_directory).map(|_| ())
}

fn sample_manifest(root: &Path) -> PipelineManifest {
    PipelineManifest {
        schema_version: PIPELINE_SCHEMA_VERSION,
        binary_version: env!("CARGO_PKG_VERSION").to_string(),
        stage_revisions: StageRevisions::current(),
        inputs: vec![InputFingerprint {
            file_id: 0,
            path: root.join("input.parquet"),
            size: 10,
            modified_unix_nanos: 20,
            row_count: 30,
            row_group_count: 1,
            min_row_group_rows: 30,
            max_row_group_rows: 30,
            schema_sha256: "schema".to_string(),
        }],
        chains: vec!["ethereum".to_string()],
        options: AnalysisOptions {
            database_path: root.join("stage.duckdb"),
            parquet_inputs: vec![root.join("input.parquet")],
            output_dir: root.join("output"),
            name_threshold: 95.0,
            threads: 32,
            memory_limit: "192GiB".to_string(),
            analysis_memory_limit: Some("192GiB".to_string()),
            duckdb_memory_limit: "160GiB".to_string(),
            temp_directory: Some(root.join("duckdb-temp")),
            progress: false,
        },
        stages: initial_stage_checkpoints(),
    }
}

mod cli_parsing;
mod fingerprints;
mod resume_locks;
