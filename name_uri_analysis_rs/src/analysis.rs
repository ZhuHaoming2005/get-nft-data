use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use duckdb::Connection;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use rayon::prelude::*;
use serde::Serialize;
use sha2::{Digest, Sha256};
use sysinfo::System;
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

use crate::atomic_file::replace_file_atomically;
use chain_matrix::*;
use components::*;
use duckdb_prep::*;
use memory::*;
#[cfg(test)]
use metadata::metadata_raw_rows_sql;
use metadata::{run_metadata_analysis, MAX_METADATA_BYTES_FOR_DEDUP};
use name::*;
use name_scoring::*;
use output::*;
pub use output::{validate_output_generation, SUMMARY_MANIFEST_FILE_NAME};
use progress::*;
use types::*;
pub use types::{AnalysisError, AnalysisOptions, AnalysisReport, SummaryRow};
use uri::*;

const NAME_DUCKDB_MEMORY_CAP: &str = "8GiB";
const METADATA_DUCKDB_MEMORY_CAP: &str = "32GiB";

fn open_analysis_connection(path: &Path) -> Result<Connection, AnalysisError> {
    if path != Path::new(":memory:") {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
    }
    Ok(Connection::open(path)?)
}

#[derive(Clone, Copy, Debug)]
pub enum AnalysisPhase {
    Prepare,
    Name,
    Metadata,
}

impl AnalysisPhase {
    fn partial_file_name(self) -> &'static str {
        match self {
            Self::Prepare => "uri-summary.json",
            Self::Name => "name-summary.json",
            Self::Metadata => "metadata-summary.json",
        }
    }
}

/// Execute one memory-heavy phase in the current process and persist only its
/// compact summary. The CLI controller invokes this through hidden child
/// commands so the operating system reclaims DuckDB/Rayon allocations between
/// phases.
pub fn run_analysis_phase(
    options: &AnalysisOptions,
    phase: AnalysisPhase,
    work_directory: &Path,
) -> Result<(), AnalysisError> {
    validate_options(options)?;
    let diagnostics = diagnostics_enabled();
    if diagnostics {
        record_diagnostic_result(
            "metrics directory",
            fs::create_dir_all(work_directory.join("metrics")).map_err(AnalysisError::from),
        );
    }
    let progress = ProgressTracker::new(1, options.progress);
    let conn = open_analysis_connection(&options.database_path)?;
    configure_duckdb(&conn, options)?;
    match phase {
        AnalysisPhase::Prepare => {}
        AnalysisPhase::Name => {
            set_phase_duckdb_memory_limit(&conn, options, NAME_DUCKDB_MEMORY_CAP)?
        }
        AnalysisPhase::Metadata => {
            set_phase_duckdb_memory_limit(&conn, options, METADATA_DUCKDB_MEMORY_CAP)?
        }
    }
    if matches!(phase, AnalysisPhase::Prepare) {
        if diagnostics {
            record_diagnostic_result(
                "DuckDB prepare profiling",
                enable_prepare_profiling(&conn, &work_directory.join("metrics/duckdb-prepare")),
            );
        }
        conn.execute_batch("BEGIN TRANSACTION")?;
    }

    let rows = match phase {
        AnalysisPhase::Prepare => {
            let chains = prepare_base_tables(&conn, options, &progress)?;
            let totals = load_chain_totals(&conn)?;
            let rows = run_uri_analysis(&conn, &chains, &totals, &progress)?;
            drop_prepare_only_uri_tables(&conn)?;
            metadata::prepare_metadata_compact_tables(&conn, &progress)?;
            rows
        }
        AnalysisPhase::Name => {
            let chains = load_selected_chains(&conn)?;
            let totals = load_chain_totals(&conn)?;
            let result = run_name_analysis(
                &conn,
                NameAnalysisSpec {
                    chains: &chains,
                    totals: &totals,
                    threshold: options.name_threshold,
                    threads: options.threads,
                    memory_limit: &options.memory_limit,
                    analysis_memory_limit: options.analysis_memory_limit.as_deref(),
                },
                &progress,
            )?;
            if diagnostics {
                record_diagnostic_result(
                    "name algorithm metrics",
                    write_json_atomically(
                        &result.metrics,
                        &work_directory.join("metrics/name-algorithm.json"),
                    ),
                );
            }
            result.rows
        }
        AnalysisPhase::Metadata => {
            let chains = load_selected_chains(&conn)?;
            let totals = load_chain_totals(&conn)?;
            let result = run_metadata_analysis(
                &conn,
                &chains,
                &totals,
                options.threads,
                options
                    .analysis_memory_limit
                    .as_deref()
                    .unwrap_or(&options.memory_limit),
                Some(&work_directory.join("artifacts/metadata")),
                &progress,
            )?;
            if diagnostics {
                record_diagnostic_result(
                    "metadata algorithm metrics",
                    write_json_atomically(
                        &result.metrics,
                        &work_directory.join("metrics/metadata-algorithm.json"),
                    ),
                );
            }
            result.rows
        }
    };
    if matches!(phase, AnalysisPhase::Prepare) {
        conn.execute_batch("COMMIT")?;
    }

    let partial_dir = work_directory.join("partial");
    fs::create_dir_all(&partial_dir)?;
    write_json_atomically(
        &AnalysisReport { summary_rows: rows },
        &partial_dir.join(phase.partial_file_name()),
    )?;
    // The result becomes resumable before the controller updates its manifest.
    // Stage tables are left intact for diagnostics/resume; the controller
    // removes the complete work directory after a normal one-shot run.
    write_phase_ready(work_directory, phase)?;
    progress.finish();
    Ok(())
}

fn diagnostics_enabled() -> bool {
    diagnostics_requested(std::env::var_os("NAME_URI_ANALYSIS_DIAGNOSTICS").as_deref())
}

fn diagnostics_requested(value: Option<&std::ffi::OsStr>) -> bool {
    value.is_some_and(|value| {
        let value = value.to_string_lossy();
        value == "1" || value.eq_ignore_ascii_case("true")
    })
}

fn record_diagnostic_result(label: &str, result: Result<(), AnalysisError>) {
    if let Err(error) = result {
        eprintln!("warning: could not persist {label}: {error}");
    }
}

fn set_phase_duckdb_memory_limit(
    conn: &Connection,
    options: &AnalysisOptions,
    cap: &str,
) -> Result<(), AnalysisError> {
    let effective = phase_duckdb_memory_limit(options, cap)?;
    conn.execute(
        &format!("PRAGMA memory_limit='{}'", sql_string(&effective)),
        [],
    )?;
    Ok(())
}

fn phase_duckdb_memory_limit(
    options: &AnalysisOptions,
    cap: &str,
) -> Result<String, AnalysisError> {
    let configured = resolve_duckdb_memory_limit(&options.duckdb_memory_limit)?;
    let configured_bytes = parse_byte_size(&configured)?;
    let cap_bytes = parse_byte_size(cap)?;
    let effective = if configured_bytes <= cap_bytes {
        configured
    } else {
        cap.to_string()
    };
    Ok(effective)
}

fn drop_prepare_only_uri_tables(conn: &Connection) -> Result<(), AnalysisError> {
    conn.execute_batch(
        "DROP TABLE IF EXISTS uri_chain_pair_contract_flags;
         DROP TABLE IF EXISTS uri_contract_flags;
         DROP TABLE IF EXISTS uri_cross_chain_keys;
         DROP TABLE IF EXISTS uri_duplicate_key_stats;
         DROP TABLE IF EXISTS uri_key_contracts;
         DROP TABLE IF EXISTS uri_rows;
         DROP TABLE IF EXISTS contract_dim;",
    )?;
    Ok(())
}

#[derive(Serialize)]
struct PhaseReady<'a> {
    phase: &'a str,
    partial_file: &'a str,
    size: u64,
    sha256: String,
}

fn write_phase_ready(work_directory: &Path, phase: AnalysisPhase) -> Result<(), AnalysisError> {
    let phase_name = match phase {
        AnalysisPhase::Prepare => "prepare",
        AnalysisPhase::Name => "name",
        AnalysisPhase::Metadata => "metadata",
    };
    let partial_file = phase.partial_file_name();
    let partial_path = work_directory.join("partial").join(partial_file);
    let mut file = fs::File::open(&partial_path)?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let ready = PhaseReady {
        phase: phase_name,
        partial_file,
        size: file.metadata()?.len(),
        sha256: sha256_hex(hasher.finalize().as_ref()),
    };
    let directory = work_directory.join("checkpoints");
    fs::create_dir_all(&directory)?;
    write_json_atomically(&ready, &directory.join(format!("{phase_name}.ready.json")))
}

fn sha256_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut output = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

pub fn finalize_analysis_phases(
    options: &AnalysisOptions,
    work_directory: &Path,
) -> Result<AnalysisReport, AnalysisError> {
    fs::create_dir_all(&options.output_dir)?;
    let mut summary_rows = Vec::new();
    for phase in [
        AnalysisPhase::Prepare,
        AnalysisPhase::Name,
        AnalysisPhase::Metadata,
    ] {
        let bytes = fs::read(
            work_directory
                .join("partial")
                .join(phase.partial_file_name()),
        )?;
        let report: AnalysisReport = serde_json::from_slice(&bytes)?;
        summary_rows.extend(report.summary_rows);
    }
    sort_summary_rows(&mut summary_rows);
    let report = AnalysisReport { summary_rows };
    write_outputs(&report, &options.output_dir)?;
    Ok(report)
}

fn write_json_atomically<T: Serialize>(value: &T, destination: &Path) -> Result<(), AnalysisError> {
    let partial = destination.with_extension("json.partial");
    let mut file = fs::File::create(&partial)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.flush()?;
    file.sync_all()?;
    drop(file);
    replace_file_atomically(&partial, destination)?;
    Ok(())
}

/// Validate every user-supplied memory limit without opening DuckDB or reading
/// Parquet. The controller calls this before input fingerprinting so malformed
/// limits fail before any expensive work starts.
pub fn validate_static_memory_options(
    memory_limit: &str,
    analysis_memory_limit: Option<&str>,
    duckdb_memory_limit: &str,
) -> Result<(), AnalysisError> {
    name_analysis_memory_plan(memory_limit, analysis_memory_limit, 0)?;
    let duckdb_limit = resolve_duckdb_memory_limit(duckdb_memory_limit)?;
    parse_byte_size(&duckdb_limit)?;
    Ok(())
}

fn validate_options(options: &AnalysisOptions) -> Result<(), AnalysisError> {
    if options.parquet_inputs.is_empty() {
        return Err(AnalysisError::MissingParquetInput);
    }
    if !options.name_threshold.is_finite() || !(0.0..=100.0).contains(&options.name_threshold) {
        return Err(AnalysisError::InvalidData(
            "name threshold must be between 0 and 100".to_string(),
        ));
    }
    validate_static_memory_options(
        &options.memory_limit,
        options.analysis_memory_limit.as_deref(),
        &options.duckdb_memory_limit,
    )?;
    Ok(())
}

fn sort_summary_rows(summary_rows: &mut [SummaryRow]) {
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
}

pub fn run_analysis(options: AnalysisOptions) -> Result<AnalysisReport, AnalysisError> {
    validate_options(&options)?;

    fs::create_dir_all(&options.output_dir)?;

    let progress = ProgressTracker::new(6, options.progress);
    progress.start_phase("configuring DuckDB", 1);
    let conn = open_analysis_connection(&options.database_path)?;
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
    drop_prepare_only_uri_tables(&conn)?;
    metadata::prepare_metadata_compact_tables(&conn, &progress)?;
    set_phase_duckdb_memory_limit(&conn, &options, NAME_DUCKDB_MEMORY_CAP)?;
    summary_rows.extend(
        run_name_analysis(
            &conn,
            NameAnalysisSpec {
                chains: &selected_chains,
                totals: &chain_totals,
                threshold: options.name_threshold,
                threads: options.threads,
                memory_limit: &options.memory_limit,
                analysis_memory_limit: options.analysis_memory_limit.as_deref(),
            },
            &progress,
        )?
        .rows,
    );
    set_phase_duckdb_memory_limit(&conn, &options, METADATA_DUCKDB_MEMORY_CAP)?;
    summary_rows.extend(
        run_metadata_analysis(
            &conn,
            &selected_chains,
            &chain_totals,
            options.threads,
            options
                .analysis_memory_limit
                .as_deref()
                .unwrap_or(&options.memory_limit),
            None,
            &progress,
        )?
        .rows,
    );

    // `run_analysis` remains the in-process library compatibility path. Its
    // prepared state is never a public cache, so do not leave large staging
    // tables behind in a caller-supplied database.
    conn.execute_batch(
        "DROP TABLE IF EXISTS core_rows;
         DROP TABLE IF EXISTS contract_dim;
         DROP TABLE IF EXISTS uri_rows;
         DROP TABLE IF EXISTS metadata_rows;
         DROP TABLE IF EXISTS metadata_contract_token_rows;
         DROP TABLE IF EXISTS metadata_token_stats;
         DROP TABLE IF EXISTS selected_chains;
         DROP TABLE IF EXISTS analysis_contracts;
         DROP TABLE IF EXISTS name_atoms;
         CHECKPOINT;",
    )?;

    sort_summary_rows(&mut summary_rows);

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
