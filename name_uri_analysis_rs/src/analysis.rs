use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use duckdb::Connection;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use rayon::prelude::*;
use serde::Serialize;
use sysinfo::{get_current_pid, Pid, ProcessesToUpdate, System};
use thiserror::Error;

mod chain_matrix;
mod components;
mod duckdb_prep;
mod memory;
mod metadata;
mod name;
mod name_scoring;
mod output;
mod progress;
mod types;
mod uri;

pub use types::{AnalysisError, AnalysisOptions, AnalysisReport, SummaryRow};
use chain_matrix::*;
use components::*;
use duckdb_prep::*;
use memory::*;
use metadata::{run_metadata_analysis, MAX_METADATA_BYTES_FOR_DEDUP};
use name::*;
use name_scoring::*;
use output::*;
use progress::*;
use types::*;
use uri::*;
#[cfg(test)]
use metadata::metadata_raw_rows_sql;

pub fn run_analysis(options: AnalysisOptions) -> Result<AnalysisReport, AnalysisError> {
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

    fs::create_dir_all(&options.output_dir)?;

    let progress = ProgressTracker::new(6, options.progress);
    progress.start_phase("configuring DuckDB", 1);
    let conn = Connection::open_in_memory()?;
    configure_duckdb(&conn, &options)?;
    progress.step("DuckDB configured");
    progress.finish_phase("DuckDB configured");
    let selected_chains = prepare_base_tables(&conn, &options, &progress)?;
    let chain_totals = load_chain_totals(&conn)?;

    let mut summary_rows = Vec::new();
    summary_rows.extend(run_uri_analysis(
        &conn,
        &selected_chains,
        &chain_totals,
        &progress,
    )?);
    summary_rows.extend(run_name_analysis(
        &conn,
        NameAnalysisSpec {
            chains: &selected_chains,
            totals: &chain_totals,
            thresholds: &options.thresholds,
            threads: options.threads,
            memory_limit: &options.memory_limit,
            analysis_memory_limit: options.analysis_memory_limit.as_deref(),
        },
        &progress,
    )?);
    summary_rows.extend(run_metadata_analysis(
        &conn,
        &selected_chains,
        &chain_totals,
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

#[cfg(test)]
mod tests;
