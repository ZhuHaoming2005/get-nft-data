use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use duckdb::{params, Connection};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use rayon::prelude::*;
use serde::Serialize;
use strsim::jaro_winkler;
use sysinfo::{get_current_pid, Pid, ProcessesToUpdate, System};
use thiserror::Error;

include!("analysis/types.rs");
include!("analysis/progress.rs");

pub fn run_analysis(options: AnalysisOptions) -> Result<AnalysisReport, AnalysisError> {
    if options.database_path.to_string_lossy() == ":memory:" {
        return Err(AnalysisError::MemoryDatabaseDisabled);
    }
    if options.parquet_inputs.is_empty() {
        return Err(AnalysisError::MissingParquetInput);
    }
    if options.thresholds.is_empty() {
        return Err(AnalysisError::InvalidData(
            "at least one name threshold is required".to_string(),
        ));
    }
    if options.thresholds.len() != 1 {
        return Err(AnalysisError::InvalidData(
            "exactly one name threshold is supported; the default is 95".to_string(),
        ));
    }

    if let Some(parent) = options.database_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::create_dir_all(&options.output_dir)?;

    let progress = ProgressTracker::new(6, options.progress);
    progress.start_phase("configuring DuckDB", 1);
    let conn = Connection::open(&options.database_path)?;
    configure_duckdb(&conn, &options)?;
    progress.step("DuckDB configured");
    progress.finish_phase("DuckDB configured");
    let selected_chains = prepare_base_tables(&conn, &options, &progress)?;

    let mut summary_rows = Vec::new();
    summary_rows.extend(run_uri_analysis(&conn, &selected_chains, &progress)?);
    summary_rows.extend(run_name_analysis(
        &conn,
        &selected_chains,
        &options.thresholds,
        options.threads,
        &options.memory_limit,
        options.analysis_memory_limit.as_deref(),
        &progress,
    )?);
    summary_rows.extend(run_metadata_analysis(
        &conn,
        &selected_chains,
        options.threads,
        &progress,
    )?);

    summary_rows.sort_by(|left, right| {
        (
            left.field_name.as_str(),
            left.scope.as_str(),
            left.primary_chain.as_str(),
            left.secondary_chain.as_str(),
            left.threshold.unwrap_or(-1.0).to_bits(),
            left.match_mode.as_str(),
            left.metric.as_str(),
        )
            .cmp(&(
                right.field_name.as_str(),
                right.scope.as_str(),
                right.primary_chain.as_str(),
                right.secondary_chain.as_str(),
                right.threshold.unwrap_or(-1.0).to_bits(),
                right.match_mode.as_str(),
                right.metric.as_str(),
            ))
    });

    let report = AnalysisReport { summary_rows };
    progress.start_phase("writing outputs", 1);
    write_outputs(&report, &options.output_dir)?;
    progress.step("outputs written");
    progress.finish_phase("outputs written");
    progress.finish();
    Ok(report)
}

include!("analysis/duckdb_prep.rs");
include!("analysis/uri.rs");
include!("analysis/name.rs");
include!("analysis/metadata.rs");
include!("analysis/name_scoring.rs");
include!("analysis/chain_matrix.rs");
include!("analysis/components.rs");
include!("analysis/memory.rs");
include!("analysis/output.rs");
include!("analysis/tests.rs");
