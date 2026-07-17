mod controller_child;
mod controller_constants;
mod controller_fingerprint;
mod controller_layout;
mod controller_locks;
mod controller_manifest;

use std::fs;
use std::path::{Path, PathBuf};

use clap::{Parser, ValueEnum};
use name_uri_analysis_rs::analysis::{
    finalize_analysis_phases, refresh_metadata_production_readiness, validate_output_generation,
    validate_static_memory_options, AnalysisOptions, AnalysisPhase, SUMMARY_MANIFEST_FILE_NAME,
};

use controller_child::{remove_work_directory_after_success, run_child_phase, run_internal_phase};
use controller_constants::PIPELINE_SCHEMA_VERSION;
use controller_fingerprint::{
    binary_identity, duckdb_threads_for_row_group_warning, fingerprint_artifact,
    fingerprint_inputs, warn_for_suboptimal_row_groups,
};
use controller_layout::resolve_directory_layout;
use controller_locks::{ControllerLock, ControllerPhaseLease};
use controller_manifest::{
    checkpoint_is_complete_and_valid, initial_stage_checkpoints, prepare_work_directory,
    promote_ready_phase, validate_resume_database_for_downstream, write_manifest_atomically,
    PipelineManifest, StageCheckpoint, StageRevisions,
};

// Re-export controller items into the binary crate root so `main_tests` can keep
// using `use super::*` without behavior or visibility changes.
#[cfg(test)]
use controller_fingerprint::{row_group_parallelism_warning, InputFingerprint};
#[cfg(test)]
use controller_layout::path_is_same_or_descendant;
#[cfg(test)]
use controller_locks::{validate_phase_generation, watch_parent_liveness, PhaseLock};
#[cfg(test)]
use controller_manifest::{
    load_match_eta_forecast, manifests_have_same_inputs_and_options, match_observation_key,
    record_match_observation, record_phase_metric, write_metric_atomically, MatchExecutionKind,
    MatchObservation, MatchOutcome, MatchSampledResources, PhaseMetric, PhaseReady,
};
#[cfg(test)]
use duckdb::Connection;
#[cfg(test)]
use name_uri_analysis_rs::analysis::parquet_sql_literal;
#[cfg(test)]
use std::io::Read;
#[cfg(test)]
use std::thread;
#[cfg(test)]
use std::time::Duration;

fn parse_positive_usize(value: &str) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| format!("expected a positive integer, got {value:?}"))?;
    if parsed == 0 {
        return Err("value must be at least 1".to_string());
    }
    Ok(parsed)
}

fn resolve_worker_threads(requested: usize) -> usize {
    let available = std::thread::available_parallelism()
        .map(|value| value.get())
        .unwrap_or(1);
    requested.min(available).max(1)
}

fn default_durable_work_directory(output_directory: &Path) -> PathBuf {
    let parent = output_directory
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let output_name = output_directory
        .file_name()
        .map_or_else(|| "output".into(), |name| name.to_string_lossy());
    parent.join(format!(".{output_name}.name_uri_analysis_rs_work"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum InternalPhase {
    Prepare,
    #[value(name = "metadata-encode")]
    MetadataEncode,
    Name,
    #[value(name = "metadata-match")]
    MetadataMatch,
}

impl InternalPhase {
    const fn cli_name(self) -> &'static str {
        match self {
            Self::Prepare => "prepare",
            Self::MetadataEncode => "metadata-encode",
            Self::Name => "name",
            Self::MetadataMatch => "metadata-match",
        }
    }
}

impl From<InternalPhase> for AnalysisPhase {
    fn from(value: InternalPhase) -> Self {
        match value {
            InternalPhase::Prepare => Self::Prepare,
            InternalPhase::MetadataEncode => Self::MetadataEncode,
            InternalPhase::Name => Self::Name,
            InternalPhase::MetadataMatch => Self::MetadataMatch,
        }
    }
}

/// Serial controller schedule: Prepare unlocks Name and Encode; Encode unlocks
/// Match; Finalize needs Name + Match. Encode runs before Name so Name stays
/// independent of encode revision bumps.
fn scheduled_phases() -> &'static [(InternalPhase, &'static str, &'static str)] {
    &[
        (
            InternalPhase::Prepare,
            "prepare_complete",
            "uri-summary.json",
        ),
        (
            InternalPhase::MetadataEncode,
            "metadata_encode_complete",
            "metadata-encode-summary.json",
        ),
        (InternalPhase::Name, "name_complete", "name-summary.json"),
        (
            InternalPhase::MetadataMatch,
            "metadata_match_complete",
            "metadata-summary.json",
        ),
    ]
}

#[derive(Debug, Parser)]
#[command(version, about = "Rust + DuckDB NFT name/URI duplicate analysis")]
struct Args {
    #[arg(
        long = "parquet",
        required_unless_present_any = ["internal_phase", "refresh_production_readiness"]
    )]
    parquet_inputs: Vec<PathBuf>,

    #[arg(long, default_value = "name_uri_analysis_output")]
    output_dir: PathBuf,

    #[arg(long)]
    work_directory: Option<PathBuf>,

    #[arg(
        long,
        help = "Deprecated no-op; recovery checkpoints always use the durable work directory"
    )]
    ephemeral_in_memory: bool,

    #[arg(long, default_value_t = 95.0)]
    name_threshold: f64,

    /// Rayon worker ceiling; DuckDB is separately capped at 64 physical-core workers.
    #[arg(long, default_value_t = 128, value_parser = parse_positive_usize)]
    threads: usize,

    #[arg(long, value_parser = parse_positive_usize)]
    prepare_threads: Option<usize>,

    #[arg(long, value_parser = parse_positive_usize)]
    metadata_encode_threads: Option<usize>,

    #[arg(long, value_parser = parse_positive_usize)]
    name_threads: Option<usize>,

    #[arg(long, value_parser = parse_positive_usize)]
    metadata_match_threads: Option<usize>,

    #[arg(long, value_parser = parse_positive_usize)]
    duckdb_threads: Option<usize>,

    #[arg(long, help = "Disable automatic Linux NUMA interleave policy")]
    disable_numa_interleave: bool,

    #[arg(
        long,
        help = "Disable memory-pressure disk fallbacks; keep hot state resident and let the allocator or operating system handle OOM"
    )]
    no_disk_fallback: bool,

    #[arg(
        long,
        default_value = "448GiB",
        help = "Per-phase Rust ceiling; each phase is capped further by current available memory, DuckDB, and required host headroom"
    )]
    analysis_memory_limit: String,

    /// DuckDB buffer-manager target; Prepare/Encode cap or rebalance it to the
    /// current process-memory envelope, and Name is capped separately at 8 GiB.
    #[arg(long, default_value = "320GiB")]
    duckdb_memory_limit: String,

    #[arg(
        long,
        help = "Resume only complete phases with an identical input manifest"
    )]
    resume: bool,

    #[arg(long, help = "Keep stage database and phase artifacts after success")]
    keep_work_directory: bool,

    #[arg(long, help = "Disable terminal progress bars")]
    no_progress: bool,

    #[arg(
        long,
        conflicts_with_all = ["parquet_inputs", "internal_phase", "resume"],
        help = "Recompute the advisory production readiness from output-owned evidence"
    )]
    refresh_production_readiness: bool,

    #[arg(
        long,
        help = "Collect detailed profiling and resource metrics (adds monitoring overhead)"
    )]
    diagnostics: bool,

    #[arg(long, hide = true, value_enum)]
    internal_phase: Option<InternalPhase>,

    #[arg(long, hide = true, requires = "internal_phase")]
    internal_config: Option<PathBuf>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    for (key, value) in [
        ("NAME_URI_ANALYSIS_PREPARE_THREADS", args.prepare_threads),
        (
            "NAME_URI_ANALYSIS_METADATA_ENCODE_THREADS",
            args.metadata_encode_threads,
        ),
        ("NAME_URI_ANALYSIS_NAME_THREADS", args.name_threads),
        (
            "NAME_URI_ANALYSIS_METADATA_MATCH_THREADS",
            args.metadata_match_threads,
        ),
        ("NAME_URI_ANALYSIS_DUCKDB_THREADS", args.duckdb_threads),
    ] {
        if let Some(value) = value {
            std::env::set_var(key, value.to_string());
        }
    }
    if args.disable_numa_interleave {
        std::env::set_var("NAME_URI_ANALYSIS_DISABLE_NUMA_INTERLEAVE", "1");
    }
    if args.no_disk_fallback {
        std::env::set_var("NAME_URI_ANALYSIS_NO_DISK_FALLBACK", "1");
        eprintln!(
            "warning: --no-disk-fallback keeps memory-pressure working sets resident; temporary \
             spill/mmap fallbacks are disabled and an out-of-memory condition may terminate the \
             process"
        );
    }
    if let Some(phase) = args.internal_phase {
        let config = args
            .internal_config
            .as_deref()
            .ok_or("--internal-config is required for internal phases")?;
        return run_internal_phase(config, phase);
    }
    if args.refresh_production_readiness {
        refresh_metadata_production_readiness(&args.output_dir)?;
        println!(
            "refreshed metadata production readiness in {}",
            args.output_dir.display()
        );
        return Ok(());
    }

    validate_static_memory_options(
        &args.analysis_memory_limit,
        Some(&args.analysis_memory_limit),
        &args.duckdb_memory_limit,
    )?;
    let effective_threads = resolve_worker_threads(args.threads);

    if args.ephemeral_in_memory {
        eprintln!(
            "warning: --ephemeral-in-memory is deprecated and ignored; recovery checkpoints use \
             the durable work directory while Match hot data remains memory-resident"
        );
    }
    let requested_work_directory = args
        .work_directory
        .unwrap_or_else(|| default_durable_work_directory(&args.output_dir));
    let (work_directory, output_directory) =
        resolve_directory_layout(&requested_work_directory, &args.output_dir)?;
    let _controller_lock = ControllerLock::acquire(&work_directory)?;
    let mut phase_lease = ControllerPhaseLease::acquire(&work_directory)?;
    let inputs = fingerprint_inputs(&args.parquet_inputs)?;
    let warning_threads = args
        .duckdb_threads
        .unwrap_or_else(|| duckdb_threads_for_row_group_warning(effective_threads))
        .min(effective_threads)
        .max(1);
    warn_for_suboptimal_row_groups(&inputs, warning_threads);
    let canonical_inputs = inputs
        .iter()
        .map(|input| input.path.clone())
        .collect::<Vec<_>>();
    let options = AnalysisOptions {
        database_path: work_directory.join("stage.duckdb"),
        parquet_inputs: canonical_inputs,
        output_dir: output_directory,
        name_threshold: args.name_threshold,
        threads: effective_threads,
        memory_limit: args.analysis_memory_limit.clone(),
        analysis_memory_limit: Some(args.analysis_memory_limit),
        duckdb_memory_limit: args.duckdb_memory_limit,
        temp_directory: Some(work_directory.join("duckdb-temp")),
        progress: !args.no_progress,
    };
    let expected_manifest = PipelineManifest {
        schema_version: PIPELINE_SCHEMA_VERSION,
        binary_version: binary_identity()?,
        stage_revisions: StageRevisions::current(),
        inputs,
        // Prepare derives `selected_chains`; the controller avoids a duplicate
        // Parquet scan by leaving this field empty until then.
        chains: Vec::new(),
        options,
        stages: initial_stage_checkpoints(),
    };
    let (config_path, mut manifest) =
        prepare_work_directory(&work_directory, expected_manifest, args.resume)?;

    for &(phase, stage, partial) in scheduled_phases() {
        if args.resume && checkpoint_is_complete_and_valid(&manifest, stage, &work_directory)? {
            validate_resume_database_for_downstream(&manifest, stage)?;
            continue;
        }
        if args.resume && promote_ready_phase(&mut manifest, phase, partial, &work_directory)? {
            write_manifest_atomically(&config_path, &manifest)?;
            validate_resume_database_for_downstream(&manifest, stage)?;
            continue;
        }
        run_child_phase(
            phase,
            &config_path,
            args.diagnostics,
            args.resume,
            &mut phase_lease,
        )?;
        if !promote_ready_phase(&mut manifest, phase, partial, &work_directory)? {
            return Err(format!("{stage} child exited without a durable ready checkpoint").into());
        }
        write_manifest_atomically(&config_path, &manifest)?;
    }

    if args.resume && checkpoint_is_complete_and_valid(&manifest, "finalized", &work_directory)? {
        validate_output_generation(&manifest.options.output_dir)?;
        if let Err(error) = refresh_metadata_production_readiness(&manifest.options.output_dir) {
            eprintln!("warning: could not refresh metadata production advisory: {error}");
        }
        let report: serde_json::Value =
            serde_json::from_slice(&fs::read(manifest.options.output_dir.join("summary.json"))?)?;
        let row_count = report["summary_rows"]
            .as_array()
            .map_or(0, std::vec::Vec::len);
        println!(
            "reused {row_count} summary rows from {}",
            manifest.options.output_dir.display()
        );
        if !args.keep_work_directory {
            remove_work_directory_after_success(&work_directory);
        }
        return Ok(());
    }

    let report = finalize_analysis_phases(&manifest.options, &work_directory)?;
    validate_output_generation(&manifest.options.output_dir)?;
    let final_artifacts = ["summary.json", "summary.csv", SUMMARY_MANIFEST_FILE_NAME]
        .into_iter()
        .map(|name| fingerprint_artifact(&manifest.options.output_dir.join(name)))
        .collect::<Result<Vec<_>, _>>()?;
    manifest.stages.insert(
        "finalized".to_string(),
        StageCheckpoint {
            complete: true,
            artifacts: final_artifacts,
        },
    );
    write_manifest_atomically(&config_path, &manifest)?;
    println!(
        "wrote {} summary rows to {}",
        report.summary_rows.len(),
        manifest.options.output_dir.display()
    );
    if !args.keep_work_directory {
        remove_work_directory_after_success(&work_directory);
    }
    Ok(())
}

#[cfg(test)]
#[path = "main_tests/mod.rs"]
mod tests;
