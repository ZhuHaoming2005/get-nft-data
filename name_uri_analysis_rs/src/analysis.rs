use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use duckdb::Connection;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use rayon::prelude::*;
use serde::Serialize;
use thiserror::Error;

mod chain_matrix;
mod components;
mod duckdb_prep;
mod memory;
mod metadata;
mod name;
mod name_scoring;
mod output;
mod production;
mod progress;
mod semantic_oracle;
mod types;
mod uri;

use crate::{sha256_file, write_json_atomically};
use chain_matrix::*;
use components::*;
use duckdb_prep::*;
use memory::*;
pub use memory::{effective_available_memory_bytes, effective_memory_capacity_bytes};
use metadata::MAX_METADATA_BYTES_FOR_DEDUP;
use name::*;
use name_scoring::*;
use output::*;
pub use output::{parquet_sql_literal, validate_output_generation, SUMMARY_MANIFEST_FILE_NAME};
pub use production::refresh_metadata_production_readiness;
use production::{publish_metadata_production_readiness, write_metadata_production_readiness};
use progress::*;
use types::*;
pub use types::{AnalysisError, AnalysisOptions, AnalysisReport, SummaryRow};
use uri::*;

const NAME_DUCKDB_MEMORY_CAP: &str = "8GiB";
pub const DUCKDB_THREAD_CAP: usize = 64;
/// Controller-to-Match child contract for an advisory, schema-bound ETA model.
/// Invalid or absent values are ignored; ETA history must never affect results.
pub const MATCH_ETA_FORECAST_ENV: &str = "NAME_URI_ANALYSIS_MATCH_ETA_FORECAST_V3";
pub const MATCH_ETA_FORECAST_SCHEMA_VERSION: u32 = 3;
/// Controller wall-clock origin for the Match child. This keeps historical
/// wall-time forecasts and the live remaining-time subtraction on one boundary.
pub const MATCH_ETA_STARTED_UNIX_MILLIS_ENV: &str =
    "NAME_URI_ANALYSIS_MATCH_STARTED_UNIX_MILLIS_V3";

fn open_analysis_connection(path: &Path) -> Result<Connection, AnalysisError> {
    if path != Path::new(":memory:") {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
    }
    Ok(Connection::open(path)?)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AnalysisPhase {
    Prepare,
    MetadataEncode,
    Name,
    MetadataMatch,
}

impl AnalysisPhase {
    fn partial_file_name(self) -> &'static str {
        match self {
            Self::Prepare => "uri-summary.json",
            Self::MetadataEncode => "metadata-encode-summary.json",
            Self::Name => "name-summary.json",
            Self::MetadataMatch => "metadata-summary.json",
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
    configure_numa_interleave();
    let mut phase_options = options.clone();
    phase_options.threads = phase_worker_threads(options.threads, phase);
    let mut memory_advisories = Vec::new();
    if matches!(phase, AnalysisPhase::Prepare) {
        let configured = resolve_duckdb_memory_limit(&phase_options.duckdb_memory_limit)?;
        let configured_bytes = parse_byte_size(&configured)? as u64;
        let (host_total, host_available) = effective_memory_snapshot_bytes();
        let process_envelope = process_memory_envelope_bytes(host_total, host_available);
        let duckdb_cap = duckdb_buffer_cap_bytes(process_envelope);
        if configured_bytes > duckdb_cap {
            memory_advisories.push(format!(
                "Prepare DuckDB budget reduced from {} to {} so the phase remains inside the \
                 current process-memory envelope with room for non-buffer allocations",
                format_byte_size(usize::try_from(configured_bytes).unwrap_or(usize::MAX)),
                format_byte_size(usize::try_from(duckdb_cap).unwrap_or(usize::MAX)),
            ));
            phase_options.duckdb_memory_limit = format!("{duckdb_cap}B");
        }
    }
    if matches!(phase, AnalysisPhase::Name) {
        let requested_rust_bytes = name_analysis_memory_plan(
            &phase_options.memory_limit,
            phase_options.analysis_memory_limit.as_deref(),
            0,
        )?
        .analysis_bytes;
        let duckdb_limit = phase_duckdb_memory_limit(&phase_options, NAME_DUCKDB_MEMORY_CAP)?;
        let duckdb_bytes = parse_byte_size(&duckdb_limit)? as u64;
        let (host_total, host_available) = effective_memory_snapshot_bytes();
        let effective_rust_bytes = name_phase_rust_budget(
            requested_rust_bytes,
            duckdb_bytes,
            host_total,
            host_available,
        )?;
        if effective_rust_bytes < requested_rust_bytes {
            memory_advisories.push(format!(
                "Name Rust budget reduced from {} to {} so DuckDB {} and required host \
                 headroom remain inside the current process-memory envelope",
                format_byte_size(requested_rust_bytes),
                format_byte_size(effective_rust_bytes),
                format_byte_size(usize::try_from(duckdb_bytes).unwrap_or(usize::MAX)),
            ));
        }
        phase_options.analysis_memory_limit = Some(format!("{effective_rust_bytes}B"));
    }
    let options = &phase_options;
    // MetadataEncode reads Prepare's DuckDB state without mutating Prepare/Name
    // tables. Match remains the sole owner of production metadata summary rows.
    if matches!(phase, AnalysisPhase::MetadataEncode) {
        return metadata::run_metadata_encode(options, work_directory);
    }
    let diagnostics = diagnostics_enabled();
    if diagnostics {
        record_diagnostic_result(
            "metrics directory",
            fs::create_dir_all(work_directory.join("metrics")).map_err(AnalysisError::from),
        );
    }
    let pipeline_stage = PipelineStage::from(phase);
    let progress = ProgressTracker::for_pipeline_stage(pipeline_stage, options.progress);
    for advisory in memory_advisories {
        progress.warn(advisory);
    }
    let result: Result<(), AnalysisError> = (|| {
        if matches!(phase, AnalysisPhase::MetadataMatch) && !metadata_inputs_ready(work_directory) {
            return Err(AnalysisError::InvalidData(
                "metadata match requires completed encode and blocking artifacts".into(),
            ));
        }
        let rows = if matches!(phase, AnalysisPhase::MetadataMatch) {
            // Match owns only immutable encoded artifacts. Keeping this branch
            // ahead of connection setup prevents accidental DuckDB coupling.
            run_metadata_pipeline(options, work_directory, &progress)?
        } else {
            let conn = open_analysis_connection(&options.database_path)?;
            configure_duckdb(&conn, options)?;
            if matches!(phase, AnalysisPhase::Name) {
                set_phase_duckdb_memory_limit(&conn, options, NAME_DUCKDB_MEMORY_CAP)?;
            }
            if matches!(phase, AnalysisPhase::Prepare) {
                if diagnostics {
                    record_diagnostic_result(
                        "DuckDB prepare profiling",
                        enable_prepare_profiling(
                            &conn,
                            &work_directory.join("metrics/duckdb-prepare"),
                        ),
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
                    rows
                }
                AnalysisPhase::Name => {
                    let chains = load_selected_chains(&conn)?;
                    let totals = load_chain_totals(&conn)?;
                    let result = run_name_analysis(
                        &conn,
                        name_analysis_spec(options, &chains, &totals, work_directory),
                        &progress,
                    )?;
                    if diagnostics {
                        record_diagnostic_result(
                            "name algorithm metrics",
                            write_json_atomically(
                                &result.metrics,
                                &work_directory.join("metrics/name-algorithm.json"),
                            )
                            .map_err(AnalysisError::from),
                        );
                    }
                    result.rows
                }
                AnalysisPhase::MetadataEncode | AnalysisPhase::MetadataMatch => {
                    unreachable!("metadata phases are handled before DuckDB setup")
                }
            };
            if matches!(phase, AnalysisPhase::Prepare) {
                conn.execute_batch("COMMIT")?;
            }
            rows
        };

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
        progress.finish_pipeline_stage(format!("{} complete", pipeline_stage.label()));
        progress.finish_display(format!("{} phase complete", pipeline_stage.label()));
        Ok(())
    })();
    if let Err(error) = &result {
        progress.fail(error.to_string());
    }
    result
}

fn phase_worker_threads(default_threads: usize, phase: AnalysisPhase) -> usize {
    let key = match phase {
        AnalysisPhase::Prepare => "NAME_URI_ANALYSIS_PREPARE_THREADS",
        AnalysisPhase::MetadataEncode => "NAME_URI_ANALYSIS_METADATA_ENCODE_THREADS",
        AnalysisPhase::Name => "NAME_URI_ANALYSIS_NAME_THREADS",
        AnalysisPhase::MetadataMatch => "NAME_URI_ANALYSIS_METADATA_MATCH_THREADS",
    };
    let requested = std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|&value| value > 0)
        .unwrap_or(default_threads);
    requested
        .min(
            std::thread::available_parallelism()
                .map(|value| value.get())
                .unwrap_or(1),
        )
        .max(1)
}

#[cfg(target_os = "linux")]
fn configure_numa_interleave() {
    if std::env::var_os("NAME_URI_ANALYSIS_DISABLE_NUMA_INTERLEAVE").is_some() {
        return;
    }
    let Ok(online) = fs::read_to_string("/sys/devices/system/node/online") else {
        return;
    };
    let mut mask = 0u64;
    for part in online.trim().split(',') {
        let mut bounds = part.split('-');
        let Some(begin) = bounds.next().and_then(|value| value.parse::<u32>().ok()) else {
            return;
        };
        let end = bounds
            .next()
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(begin);
        for node in begin..=end.min(63) {
            mask |= 1u64 << node;
        }
    }
    if mask.count_ones() <= 1 {
        return;
    }
    const MPOL_INTERLEAVE: libc::c_int = 3;
    // Best effort: containers may deny set_mempolicy. Child threads inherit
    // the policy when the call succeeds; failure must not affect results.
    unsafe {
        libc::syscall(
            libc::SYS_set_mempolicy,
            MPOL_INTERLEAVE,
            &mask as *const u64,
            64usize,
        );
    }
}

#[cfg(not(target_os = "linux"))]
fn configure_numa_interleave() {}

fn metadata_inputs_ready(work_directory: &Path) -> bool {
    let layout = metadata_engine::artifacts::MetadataArtifactLayout::new(work_directory);
    layout.encode_dir().join("features.ready").is_file()
        && layout.blocking_dir().join("blocking.ready").is_file()
}

fn run_metadata_pipeline(
    options: &AnalysisOptions,
    work_directory: &Path,
    progress: &ProgressTracker,
) -> Result<Vec<SummaryRow>, AnalysisError> {
    let result = run_metadata_pipeline_result(options, work_directory, progress)?;
    record_diagnostic_result(
        "metadata production advisory",
        write_metadata_production_readiness(work_directory, &result),
    );
    let rows = result
        .summary_rows
        .into_iter()
        .map(|row| SummaryRow {
            field_name: "metadata".into(),
            scope: row.scope,
            primary_chain: row.primary_chain,
            secondary_chain: row.secondary_chain,
            threshold: Some(metadata_engine::scoring::METADATA_THRESHOLD),
            match_mode: "template_recall_hybrid_verify".into(),
            metric: "duplicate_group".into(),
            total_contracts: row.total_contracts,
            total_nfts: row.total_nfts,
            group_count: row.group_count,
            duplicate_contract_count: row.duplicate_contract_count,
            duplicate_nft_count: row.duplicate_nft_count,
            duplicate_contract_ratio: pct(row.duplicate_contract_count, row.total_contracts),
            duplicate_nft_ratio: pct(row.duplicate_nft_count, row.total_nfts),
            group_size_ge_2_count: row.group_size_ge_2_count,
            group_size_gt_2_count: row.group_size_gt_2_count,
        })
        .collect();
    Ok(rows)
}

pub(crate) fn run_metadata_pipeline_result(
    options: &AnalysisOptions,
    work_directory: &Path,
    progress: &ProgressTracker,
) -> Result<metadata_engine::pipeline::MetadataPipelineResult, AnalysisError> {
    use metadata_engine::resource::{GIB, MATCH_HARD_TOP};
    let layout = metadata_engine::artifacts::MetadataArtifactLayout::new(work_directory);
    let features = layout.encode_dir();
    let blocking = layout.blocking_dir();
    let out = metadata_engine::pipeline::default_output_dir(work_directory);
    let analysis_bytes = name_analysis_memory_plan(
        &options.memory_limit,
        options.analysis_memory_limit.as_deref(),
        0,
    )?
    .analysis_bytes as u64;
    let (host_total_memory, host_available_memory) = effective_memory_snapshot_bytes();
    let memory_hard_top = engine_memory_hard_top_bytes(
        analysis_bytes as usize,
        MATCH_HARD_TOP,
        host_total_memory,
        host_available_memory,
    )?;
    let requested_match_top = analysis_bytes.min(MATCH_HARD_TOP);
    if memory_hard_top < requested_match_top {
        progress.warn(format!(
            "Metadata Match memory ceiling reduced from {} to {} by current available memory and \
             required host headroom; exact resident paths will use bounded mmap/spill fallbacks",
            format_byte_size(usize::try_from(requested_match_top).unwrap_or(usize::MAX)),
            format_byte_size(usize::try_from(memory_hard_top).unwrap_or(usize::MAX)),
        ));
    }
    // The engine shrinks this ceiling to the exact per-scope forest upper
    // bound. A larger production ceiling avoids the old 4 GiB artificial
    // failure mode while keeping at least fifteen sixteenths of the admitted
    // Match memory available for snapshots, scorers and reduction scratch.
    let edge_bytes = (memory_hard_top / 16).clamp(64 * 1024, 32 * GIB);
    let config = metadata_engine::pipeline::MetadataPipelineConfig {
        storage_work_directory: work_directory.to_path_buf(),
        memory_hard_top,
        host_total_memory,
        threads: options.threads,
        // Catalog descriptors are fixed-width external columns and no longer
        // need an artificial one-million resident-Vec ceiling. u32 job ids are
        // the persisted format limit.
        max_catalog_jobs: u32::MAX as u64,
        max_candidate_pair_visits: metadata_engine::pipeline::DEFAULT_MAX_CANDIDATE_PAIR_VISITS,
        exact_sample_lefts: metadata_engine::pipeline::DEFAULT_EXACT_SAMPLE_LEFTS,
        exact_pair_work: metadata_engine::pipeline::DEFAULT_EXACT_PAIR_WORK,
        evidence_gate_policy: metadata_engine::evidence::EvidenceGatePolicy::production(),
        edge_bytes,
    };
    metadata_engine::pipeline::run_metadata_pipeline_durable_with_callbacks(
        &features,
        &blocking,
        &out,
        &config,
        |event| progress.observe_engine_event(event),
        |advisory| {
            progress.warn(format!(
                "{advisory}; continuing with complete outputs marked not production-ready"
            ));
        },
    )
    .map_err(|err| AnalysisError::InvalidData(format!("metadata pipeline: {err}")))
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

pub(crate) fn name_phase_rust_budget(
    requested_rust_bytes: usize,
    duckdb_bytes: u64,
    host_total: u64,
    host_available: u64,
) -> Result<usize, AnalysisError> {
    let host_envelope = process_memory_envelope_bytes(host_total, host_available);
    let rust_capacity = host_envelope.saturating_sub(duckdb_bytes);
    let effective = (requested_rust_bytes as u64).min(rust_capacity);
    if effective == 0 {
        return Err(AnalysisError::InvalidData(format!(
            "Name has no Rust memory inside the host envelope: host_total={host_total}, \
             DuckDB={duckdb_bytes}, required_headroom={}",
            metadata_engine::resource::required_host_headroom(host_total)
        )));
    }
    usize::try_from(effective)
        .map_err(|_| AnalysisError::InvalidData("Name memory budget exceeds usize".into()))
}

fn drop_prepare_only_uri_tables(conn: &Connection) -> Result<(), AnalysisError> {
    materialize_metadata_rows_after_uri(conn)?;
    conn.execute_batch(
        "DROP TABLE IF EXISTS uri_chain_pair_contract_flags;
         DROP TABLE IF EXISTS uri_contract_flags;
         DROP TABLE IF EXISTS uri_cross_chain_keys;
         DROP TABLE IF EXISTS uri_duplicate_key_stats;
         DROP TABLE IF EXISTS uri_key_chain_stats;
         DROP TABLE IF EXISTS uri_key_contracts;
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
    artifacts: Vec<serde_json::Value>,
}

fn write_phase_ready(work_directory: &Path, phase: AnalysisPhase) -> Result<(), AnalysisError> {
    let phase_name = match phase {
        AnalysisPhase::Prepare => "prepare",
        AnalysisPhase::MetadataEncode => "metadata-encode",
        AnalysisPhase::Name => "name",
        AnalysisPhase::MetadataMatch => "metadata-match",
    };
    let partial_file = phase.partial_file_name();
    let partial_path = work_directory.join("partial").join(partial_file);
    let (size, sha256) = sha256_file(&partial_path, 1024 * 1024)?;
    let ready = PhaseReady {
        phase: phase_name,
        partial_file,
        size,
        sha256,
        artifacts: Vec::new(),
    };
    let directory = work_directory.join("checkpoints");
    fs::create_dir_all(&directory)?;
    write_json_atomically(&ready, &directory.join(format!("{phase_name}.ready.json")))
        .map_err(AnalysisError::from)
}

pub fn finalize_analysis_phases(
    options: &AnalysisOptions,
    work_directory: &Path,
) -> Result<AnalysisReport, AnalysisError> {
    let progress = ProgressTracker::for_pipeline_stage(PipelineStage::Finalize, options.progress);
    let result: Result<AnalysisReport, AnalysisError> = (|| {
        progress.start_stage("finalizing outputs", 5);
        fs::create_dir_all(&options.output_dir)?;
        let mut summary_rows = Vec::new();
        // Encode owns artifacts only; production summary comes from Prepare,
        // Name, and Match.
        for phase in [
            AnalysisPhase::Prepare,
            AnalysisPhase::Name,
            AnalysisPhase::MetadataMatch,
        ] {
            let bytes = fs::read(
                work_directory
                    .join("partial")
                    .join(phase.partial_file_name()),
            )?;
            let report: AnalysisReport = serde_json::from_slice(&bytes)?;
            summary_rows.extend(report.summary_rows);
            progress.step_stage(format!(
                "loaded {} partial summary",
                phase.partial_file_name()
            ));
        }
        sort_summary_rows(&mut summary_rows);
        progress.step_stage("sorted summary rows");
        let report = AnalysisReport { summary_rows };
        write_outputs(&report, &options.output_dir)?;
        record_diagnostic_result(
            "metadata production advisory",
            publish_metadata_production_readiness(work_directory, &options.output_dir),
        );
        progress.step_stage("wrote and verified output generation");
        progress.finish_stage("outputs finalized");
        progress.finish_pipeline_stage("finalize outputs complete");
        progress.finish_display("analysis complete; outputs finalized");
        Ok(report)
    })();
    if let Err(error) = &result {
        progress.fail(error.to_string());
    }
    result
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

fn name_analysis_spec<'a>(
    options: &'a AnalysisOptions,
    chains: &'a [String],
    totals: &'a HashMap<String, NameTotals>,
    work_directory: &'a Path,
) -> NameAnalysisSpec<'a> {
    NameAnalysisSpec {
        chains,
        totals,
        threshold: options.name_threshold,
        threads: options.threads,
        memory_limit: &options.memory_limit,
        analysis_memory_limit: options.analysis_memory_limit.as_deref(),
        scratch_directory: work_directory,
    }
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
    let work = CompatibilityWorkDirectory::create(&options.output_dir)?;
    let mut phase_options = options;
    phase_options.database_path = work.path().join("stage.duckdb");
    phase_options.temp_directory = Some(work.path().join("duckdb-temp"));
    for phase in [
        AnalysisPhase::Prepare,
        AnalysisPhase::MetadataEncode,
        AnalysisPhase::Name,
        AnalysisPhase::MetadataMatch,
    ] {
        run_analysis_phase(&phase_options, phase, work.path())?;
    }
    finalize_analysis_phases(&phase_options, work.path())
}

struct CompatibilityWorkDirectory(PathBuf);

impl CompatibilityWorkDirectory {
    fn create(output_directory: &Path) -> Result<Self, AnalysisError> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NONCE: AtomicU64 = AtomicU64::new(0);

        let parent = output_directory
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)?;
        let stem = output_directory
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("name-uri-analysis");
        for _ in 0..1_000 {
            let nonce = NONCE.fetch_add(1, Ordering::Relaxed);
            let path = parent.join(format!(
                ".{stem}.metadata-work-{}-{nonce}",
                std::process::id()
            ));
            match fs::create_dir(&path) {
                Ok(()) => return Ok(Self(path)),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(AnalysisError::InvalidData(
            "could not allocate a unique metadata compatibility work directory".into(),
        ))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for CompatibilityWorkDirectory {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.0) {
            eprintln!(
                "warning: could not remove compatibility work directory {}: {error}",
                self.0.display()
            );
        }
    }
}

#[cfg(test)]
mod tests;
