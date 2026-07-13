use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;
use std::time::Instant;
use std::time::UNIX_EPOCH;

use clap::{Parser, ValueEnum};
use duckdb::Connection;
use name_uri_analysis_rs::analysis::{
    finalize_analysis_phases, run_analysis_phase, validate_output_generation,
    validate_static_memory_options, AnalysisOptions, AnalysisPhase, MetadataRecallMode,
    DUCKDB_THREAD_CAP, SUMMARY_MANIFEST_FILE_NAME,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sysinfo::{Pid, ProcessesToUpdate, System};

mod atomic_file;
use atomic_file::replace_file_atomically;

const PIPELINE_SCHEMA_VERSION: u32 = 2;
// Any semantic change to a resumable stage must bump that stage's revision;
// the controller invalidates only the affected checkpoint and its dependents.
const PREPARE_STAGE_REVISION: u32 = 1;
const NAME_STAGE_REVISION: u32 = 1;
const METADATA_STAGE_REVISION: u32 = 2;
const FINALIZER_STAGE_REVISION: u32 = 1;
const PARENT_LIVENESS_ENV: &str = "NAME_URI_ANALYSIS_PARENT_LIVENESS_PIPE";
const PHASE_GENERATION_ENV: &str = "NAME_URI_ANALYSIS_PHASE_GENERATION";

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

#[derive(Clone, Copy, Debug, ValueEnum)]
enum InternalPhase {
    Prepare,
    Name,
    Metadata,
}

impl From<InternalPhase> for AnalysisPhase {
    fn from(value: InternalPhase) -> Self {
        match value {
            InternalPhase::Prepare => Self::Prepare,
            InternalPhase::Name => Self::Name,
            InternalPhase::Metadata => Self::Metadata,
        }
    }
}

#[derive(Debug, Parser)]
#[command(version, about = "Rust + DuckDB NFT name/URI duplicate analysis")]
struct Args {
    #[arg(long = "parquet", required_unless_present = "internal_phase")]
    parquet_inputs: Vec<PathBuf>,

    #[arg(long, default_value = "name_uri_analysis_output")]
    output_dir: PathBuf,

    #[arg(long)]
    work_directory: Option<PathBuf>,

    #[arg(long, default_value_t = 95.0)]
    name_threshold: f64,

    #[arg(long, value_enum, default_value_t = MetadataRecallMode::Conservative)]
    metadata_recall_mode: MetadataRecallMode,

    /// Rayon worker ceiling; DuckDB is separately capped at 64 physical-core workers.
    #[arg(long, default_value_t = 128, value_parser = parse_positive_usize)]
    threads: usize,

    #[arg(
        long,
        default_value = "384GiB",
        help = "Hard budget for Rust analysis structures"
    )]
    analysis_memory_limit: String,

    /// DuckDB buffer-manager limit. Keep headroom for non-buffer allocations.
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
        help = "Collect detailed profiling and resource metrics (adds monitoring overhead)"
    )]
    diagnostics: bool,

    #[arg(long, hide = true, value_enum)]
    internal_phase: Option<InternalPhase>,

    #[arg(long, hide = true, requires = "internal_phase")]
    internal_config: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct InputFingerprint {
    file_id: u32,
    path: PathBuf,
    size: u64,
    modified_unix_nanos: u128,
    row_count: u64,
    row_group_count: u64,
    min_row_group_rows: u64,
    max_row_group_rows: u64,
    schema_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PipelineManifest {
    schema_version: u32,
    binary_version: String,
    #[serde(default)]
    stage_revisions: StageRevisions,
    inputs: Vec<InputFingerprint>,
    chains: Vec<String>,
    options: AnalysisOptions,
    stages: BTreeMap<String, StageCheckpoint>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
struct StageRevisions {
    prepare: u32,
    name: u32,
    metadata: u32,
    finalizer: u32,
}

impl StageRevisions {
    const fn current() -> Self {
        Self {
            prepare: PREPARE_STAGE_REVISION,
            name: NAME_STAGE_REVISION,
            metadata: METADATA_STAGE_REVISION,
            finalizer: FINALIZER_STAGE_REVISION,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ArtifactFingerprint {
    path: PathBuf,
    size: u64,
    row_count: Option<u64>,
    sha256: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
struct StageCheckpoint {
    complete: bool,
    artifacts: Vec<ArtifactFingerprint>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PhaseReady {
    phase: String,
    partial_file: String,
    size: u64,
    sha256: String,
}

#[derive(Debug, Serialize)]
struct PhaseMetric<'a> {
    phase: &'a str,
    wall_millis: u128,
    cpu_millis: u64,
    success: bool,
    input_rows: u64,
    summary_rows: u64,
    peak_rss_bytes: u64,
    peak_duckdb_temp_bytes: u64,
    io_read_bytes: u64,
    io_written_bytes: u64,
    database_bytes: u64,
    artifact_bytes: u64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if let Some(phase) = args.internal_phase {
        let config = args
            .internal_config
            .as_deref()
            .ok_or("--internal-config is required for internal phases")?;
        return run_internal_phase(config, phase);
    }

    validate_static_memory_options(
        &args.analysis_memory_limit,
        Some(&args.analysis_memory_limit),
        &args.duckdb_memory_limit,
    )?;
    let effective_threads = resolve_worker_threads(args.threads);

    let requested_work_directory = args
        .work_directory
        .unwrap_or_else(|| std::env::temp_dir().join("name_uri_analysis_rs_work"));
    let (work_directory, output_directory) =
        resolve_directory_layout(&requested_work_directory, &args.output_dir)?;
    let _controller_lock = ControllerLock::acquire(&work_directory)?;
    let mut phase_lease = ControllerPhaseLease::acquire(&work_directory)?;
    let inputs = fingerprint_inputs(&args.parquet_inputs)?;
    warn_for_suboptimal_row_groups(
        &inputs,
        duckdb_threads_for_row_group_warning(effective_threads),
    );
    let canonical_inputs = inputs
        .iter()
        .map(|input| input.path.clone())
        .collect::<Vec<_>>();
    let options = AnalysisOptions {
        database_path: work_directory.join("stage.duckdb"),
        parquet_inputs: canonical_inputs,
        output_dir: output_directory,
        name_threshold: args.name_threshold,
        metadata_recall_mode: args.metadata_recall_mode,
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
        // `selected_chains` is derived once by the prepare phase. Keeping this
        // legacy manifest field empty avoids a redundant full Parquet column
        // scan before the actual pipeline starts.
        chains: Vec::new(),
        options,
        stages: initial_stage_checkpoints(),
    };
    let (config_path, mut manifest) =
        prepare_work_directory(&work_directory, expected_manifest, args.resume)?;

    for (phase, stage, partial) in [
        (
            InternalPhase::Prepare,
            "prepare_complete",
            "uri-summary.json",
        ),
        (InternalPhase::Name, "name_complete", "name-summary.json"),
        (
            InternalPhase::Metadata,
            "metadata_complete",
            "metadata-summary.json",
        ),
    ] {
        if args.resume && checkpoint_is_complete_and_valid(&manifest, stage, &work_directory)? {
            validate_resume_database_for_downstream(&manifest, stage)?;
            continue;
        }
        if args.resume && promote_ready_phase(&mut manifest, phase, partial, &work_directory)? {
            write_manifest_atomically(&config_path, &manifest)?;
            validate_resume_database_for_downstream(&manifest, stage)?;
            continue;
        }
        run_child_phase(phase, &config_path, args.diagnostics, &mut phase_lease)?;
        if !promote_ready_phase(&mut manifest, phase, partial, &work_directory)? {
            return Err(format!("{stage} child exited without a durable ready checkpoint").into());
        }
        write_manifest_atomically(&config_path, &manifest)?;
    }

    if args.resume && checkpoint_is_complete_and_valid(&manifest, "finalized", &work_directory)? {
        validate_output_generation(&manifest.options.output_dir)?;
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

fn remove_work_directory_after_success(work_directory: &Path) {
    if let Err(error) = fs::remove_dir_all(work_directory) {
        eprintln!(
            "warning: analysis succeeded, but work directory {} could not be removed: {error}",
            work_directory.display()
        );
    }
}

#[derive(Debug)]
struct ControllerLock {
    _lock: ExclusiveFileLock,
}

impl ControllerLock {
    fn acquire(work_directory: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let parent = work_directory
            .parent()
            .ok_or("--work-directory must not be a filesystem root")?;
        let name = work_directory
            .file_name()
            .ok_or("--work-directory must have a final path component")?
            .to_string_lossy();
        fs::create_dir_all(parent)?;
        let path = parent.join(format!(".{name}.name-uri-analysis.lock"));
        let owner = format!(
            "{} {}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );

        match ExclusiveFileLock::try_acquire(&path, &owner)? {
            Some(lock) => Ok(Self { _lock: lock }),
            None => {
                let current_owner = fs::read_to_string(&path).unwrap_or_default();
                let owner_description = current_owner
                    .split_whitespace()
                    .next()
                    .and_then(|value| value.parse::<u32>().ok())
                    .map_or_else(
                        || "another process".to_string(),
                        |pid| format!("process {pid}"),
                    );
                Err(format!(
                    "work directory {} is already controlled by {owner_description}",
                    work_directory.display()
                )
                .into())
            }
        }
    }
}

#[derive(Debug)]
struct PhaseLock {
    _lock: ExclusiveFileLock,
}

impl PhaseLock {
    fn acquire(work_directory: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let (path, owner) = Self::target(work_directory)?;
        match ExclusiveFileLock::try_acquire(&path, &owner)? {
            Some(lock) => Ok(Self { _lock: lock }),
            None => Err(format!(
                "analysis phase is still active in another process for work directory {}",
                work_directory.display()
            )
            .into()),
        }
    }

    fn acquire_blocking(work_directory: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let (path, owner) = Self::target(work_directory)?;
        Ok(Self {
            _lock: ExclusiveFileLock::acquire(&path, &owner)?,
        })
    }

    fn target(work_directory: &Path) -> Result<(PathBuf, String), Box<dyn std::error::Error>> {
        let parent = work_directory
            .parent()
            .ok_or("--work-directory must not be a filesystem root")?;
        let name = work_directory
            .file_name()
            .ok_or("--work-directory must have a final path component")?
            .to_string_lossy();
        fs::create_dir_all(parent)?;
        let path = parent.join(format!(".{name}.name-uri-analysis.phase.lock"));
        let owner = format!(
            "{} {}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        Ok((path, owner))
    }
}

#[cfg(test)]
fn ensure_phase_idle(work_directory: &Path) -> Result<(), Box<dyn std::error::Error>> {
    drop(PhaseLock::acquire(work_directory)?);
    Ok(())
}

#[derive(Debug)]
struct ControllerPhaseLease {
    work_directory: PathBuf,
    generation: String,
    lock: Option<PhaseLock>,
}

impl ControllerPhaseLease {
    fn acquire(work_directory: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let lock = PhaseLock::acquire(work_directory)?;
        let generation = new_process_generation();
        write_phase_generation_atomically(work_directory, &generation)?;
        Ok(Self {
            work_directory: work_directory.to_path_buf(),
            generation,
            lock: Some(lock),
        })
    }

    fn generation(&self) -> &str {
        &self.generation
    }

    fn release_for_child(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let lock = self
            .lock
            .take()
            .ok_or("controller phase lease is already released")?;
        drop(lock);
        Ok(())
    }

    fn reclaim_after_child(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.lock.is_some() {
            return Err("controller phase lease was not released to the child".into());
        }
        self.lock = Some(PhaseLock::acquire_blocking(&self.work_directory)?);
        validate_phase_generation(&self.work_directory, &self.generation)?;
        Ok(())
    }
}

fn new_process_generation() -> String {
    format!(
        "{} {}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    )
}

fn phase_generation_path(work_directory: &Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let parent = work_directory
        .parent()
        .ok_or("--work-directory must not be a filesystem root")?;
    let name = work_directory
        .file_name()
        .ok_or("--work-directory must have a final path component")?
        .to_string_lossy();
    Ok(parent.join(format!(".{name}.name-uri-analysis.phase.generation")))
}

fn write_phase_generation_atomically(
    work_directory: &Path,
    generation: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let path = phase_generation_path(work_directory)?;
    let partial = path.with_extension("generation.partial");
    let mut file = fs::File::create(&partial)?;
    file.write_all(generation.as_bytes())?;
    file.flush()?;
    file.sync_all()?;
    drop(file);
    replace_file_atomically(&partial, &path)?;
    Ok(())
}

fn validate_phase_generation(
    work_directory: &Path,
    expected: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let path = phase_generation_path(work_directory)?;
    let actual = fs::read_to_string(&path)?;
    if actual != expected {
        return Err(format!(
            "stale internal phase generation rejected for work directory {}",
            work_directory.display()
        )
        .into());
    }
    Ok(())
}

fn validate_phase_generation_from_env(
    work_directory: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let expected = std::env::var(PHASE_GENERATION_ENV)
        .map_err(|_| "internal phase generation is missing or invalid")?;
    validate_phase_generation(work_directory, &expected)
}

#[derive(Debug)]
struct ExclusiveFileLock {
    file: fs::File,
}

impl ExclusiveFileLock {
    fn acquire(path: &Path, owner: &str) -> std::io::Result<Self> {
        let mut file = Self::open(path)?;
        file.lock()?;
        Self::write_owner(&mut file, owner)?;
        Ok(Self { file })
    }

    fn try_acquire(path: &Path, owner: &str) -> std::io::Result<Option<Self>> {
        let mut file = Self::open(path)?;
        match file.try_lock() {
            Ok(()) => {}
            Err(fs::TryLockError::WouldBlock) => return Ok(None),
            Err(fs::TryLockError::Error(error)) if lock_error_is_contention(&error) => {
                return Ok(None);
            }
            Err(fs::TryLockError::Error(error)) => return Err(error),
        }
        Self::write_owner(&mut file, owner)?;
        Ok(Some(Self { file }))
    }

    fn open(path: &Path) -> std::io::Result<fs::File> {
        fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
    }

    fn write_owner(file: &mut fs::File, owner: &str) -> std::io::Result<()> {
        file.set_len(0)?;
        file.write_all(owner.as_bytes())?;
        file.sync_all()
    }
}

fn lock_error_is_contention(error: &std::io::Error) -> bool {
    if error.kind() == std::io::ErrorKind::WouldBlock {
        return true;
    }
    #[cfg(windows)]
    if error.raw_os_error() == Some(33) {
        // LockFileEx reports ERROR_LOCK_VIOLATION for a conflicting range.
        return true;
    }
    false
}

impl Drop for ExclusiveFileLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

#[cfg(test)]
fn validate_directory_layout(
    work_directory: &Path,
    output_directory: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    resolve_directory_layout(work_directory, output_directory).map(|_| ())
}

fn resolve_directory_layout(
    work_directory: &Path,
    output_directory: &Path,
) -> Result<(PathBuf, PathBuf), Box<dyn std::error::Error>> {
    let work_directory = normalize_layout_path(work_directory)?;
    let output_directory = normalize_layout_path(output_directory)?;
    if path_is_same_or_descendant(&output_directory, &work_directory) {
        return Err(format!(
            "--output-dir {} cannot be inside --work-directory {}; successful cleanup would delete the outputs",
            output_directory.display(),
            work_directory.display()
        )
        .into());
    }
    Ok((work_directory, output_directory))
}

/// Resolve every existing component (including directory symlinks/junctions)
/// while retaining a normalized suffix for paths that have not been created
/// yet. Resolving incrementally preserves filesystem semantics for `link/..`.
fn normalize_layout_path(path: &Path) -> std::io::Result<PathBuf> {
    use std::path::Component;

    let absolute = std::path::absolute(path)?;
    let mut resolved = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::Prefix(prefix) => resolved.push(prefix.as_os_str()),
            Component::RootDir => resolved.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                resolved.pop();
            }
            Component::Normal(part) => {
                resolved.push(part);
                if resolved.exists() {
                    resolved = resolved.canonicalize()?;
                }
            }
        }
    }
    Ok(resolved)
}

fn path_is_same_or_descendant(path: &Path, ancestor: &Path) -> bool {
    let mut path_components = path.components();
    for ancestor_component in ancestor.components() {
        let Some(path_component) = path_components.next() else {
            return false;
        };
        if !path_components_equal(path_component.as_os_str(), ancestor_component.as_os_str()) {
            return false;
        }
    }
    true
}

#[cfg(windows)]
fn path_components_equal(left: &std::ffi::OsStr, right: &std::ffi::OsStr) -> bool {
    left.to_string_lossy()
        .eq_ignore_ascii_case(&right.to_string_lossy())
}

#[cfg(not(windows))]
fn path_components_equal(left: &std::ffi::OsStr, right: &std::ffi::OsStr) -> bool {
    left == right
}

fn binary_identity() -> Result<String, Box<dyn std::error::Error>> {
    let executable = std::env::current_exe()?;
    let fingerprint = fingerprint_artifact(&executable)?;
    Ok(format!(
        "{}+sha256:{}",
        env!("CARGO_PKG_VERSION"),
        fingerprint.sha256
    ))
}

fn run_internal_phase(
    config_path: &Path,
    phase: InternalPhase,
) -> Result<(), Box<dyn std::error::Error>> {
    let work_directory = config_path
        .parent()
        .ok_or("internal config has no parent directory")?;
    start_parent_liveness_watchdog()?;
    let _phase_lock = PhaseLock::acquire_blocking(work_directory)?;
    validate_phase_generation_from_env(work_directory)?;
    let manifest: PipelineManifest = serde_json::from_slice(&fs::read(config_path)?)?;
    if manifest.schema_version != PIPELINE_SCHEMA_VERSION {
        return Err(format!(
            "unsupported pipeline schema {}, expected {}",
            manifest.schema_version, PIPELINE_SCHEMA_VERSION
        )
        .into());
    }
    if manifest.binary_version != binary_identity()? {
        return Err("internal phase binary does not match the pipeline manifest".into());
    }
    run_analysis_phase(&manifest.options, phase.into(), work_directory)?;
    Ok(())
}

fn start_parent_liveness_watchdog() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var_os(PARENT_LIVENESS_ENV).as_deref() != Some(std::ffi::OsStr::new("1")) {
        return Err("internal phases must be launched by the pipeline controller".into());
    }
    thread::Builder::new()
        .name("controller-liveness".to_string())
        .spawn(|| {
            let stdin = std::io::stdin();
            watch_parent_liveness(stdin.lock(), || {
                eprintln!("pipeline controller disconnected; terminating internal phase");
                std::process::exit(1);
            });
        })?;
    Ok(())
}

fn watch_parent_liveness<R: Read>(mut reader: R, on_disconnect: impl FnOnce()) {
    let mut buffer = [0u8; 64];
    loop {
        match reader.read(&mut buffer) {
            Ok(0) | Err(_) => {
                on_disconnect();
                return;
            }
            Ok(_) => {}
        }
    }
}

fn fingerprint_inputs(
    paths: &[PathBuf],
) -> Result<Vec<InputFingerprint>, Box<dyn std::error::Error>> {
    let conn = Connection::open_in_memory()?;
    let mut seen = HashSet::with_capacity(paths.len());
    let canonical_paths = paths
        .iter()
        .map(|path| {
            let canonical = path.canonicalize()?;
            if !seen.insert(canonical.clone()) {
                return Err(format!("duplicate Parquet input: {}", canonical.display()).into());
            }
            Ok(canonical)
        })
        .collect::<Result<Vec<PathBuf>, Box<dyn std::error::Error>>>()?;
    canonical_paths
        .iter()
        .enumerate()
        .map(|(file_id, path)| {
            let metadata = fs::metadata(path)?;
            let modified_unix_nanos = metadata
                .modified()?
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let input = parquet_sql_path(path);
            let (row_count, row_group_count, min_row_group_rows, max_row_group_rows) = conn
                .query_row(
                    &format!(
                        "SELECT coalesce(sum(row_group_num_rows), 0)::UBIGINT,
                            count(*)::UBIGINT,
                            coalesce(min(row_group_num_rows), 0)::UBIGINT,
                            coalesce(max(row_group_num_rows), 0)::UBIGINT
                     FROM (
                         SELECT DISTINCT row_group_id, row_group_num_rows
                         FROM parquet_metadata({input})
                     ) groups"
                    ),
                    [],
                    |row| {
                        Ok((
                            row.get::<_, u64>(0)?,
                            row.get::<_, u64>(1)?,
                            row.get::<_, u64>(2)?,
                            row.get::<_, u64>(3)?,
                        ))
                    },
                )?;
            let mut statement =
                conn.prepare(&format!("DESCRIBE SELECT * FROM read_parquet({input})"))?;
            let mut schema = Vec::new();
            let columns = statement.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            for column in columns {
                let (name, data_type) = column?;
                schema.extend_from_slice(name.as_bytes());
                schema.push(0);
                schema.extend_from_slice(data_type.as_bytes());
                schema.push(0xff);
            }
            Ok(InputFingerprint {
                file_id: u32::try_from(file_id)
                    .map_err(|_| "Parquet input count exceeds u32 file IDs")?,
                path: path.clone(),
                size: metadata.len(),
                modified_unix_nanos,
                row_count,
                row_group_count,
                min_row_group_rows,
                max_row_group_rows,
                schema_sha256: sha256_bytes(&schema),
            })
        })
        .collect()
}

fn row_group_parallelism_warning(
    inputs: &[InputFingerprint],
    effective_threads: usize,
) -> Option<String> {
    let row_group_count = inputs.iter().fold(0u64, |total, input| {
        total.saturating_add(input.row_group_count)
    });
    if row_group_count < u64::try_from(effective_threads).unwrap_or(u64::MAX) {
        return Some(format!(
            "only {row_group_count} Parquet row groups are available for {effective_threads} workers"
        ));
    }
    None
}

fn duckdb_threads_for_row_group_warning(effective_threads: usize) -> usize {
    effective_threads.clamp(1, DUCKDB_THREAD_CAP)
}

fn warn_for_suboptimal_row_groups(inputs: &[InputFingerprint], effective_threads: usize) {
    if let Some(warning) = row_group_parallelism_warning(inputs, effective_threads) {
        eprintln!("warning: {warning}");
    }
    for input in inputs {
        if input.row_group_count > 0
            && (input.min_row_group_rows < 100_000 || input.max_row_group_rows > 1_000_000)
        {
            eprintln!(
                "warning: {} row-group sizes range from {} to {} rows; about 100k-1m is preferred",
                input.path.display(),
                input.min_row_group_rows,
                input.max_row_group_rows
            );
        }
    }
}

fn parquet_sql_path(path: &Path) -> String {
    format!(
        "'{}'",
        path.display()
            .to_string()
            .replace('\\', "/")
            .replace('\'', "''")
    )
}

fn sha256_bytes(bytes: &[u8]) -> String {
    sha256_hex(Sha256::digest(bytes).as_ref())
}

fn sha256_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut output = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

fn prepare_work_directory(
    work_directory: &Path,
    manifest: PipelineManifest,
    resume: bool,
) -> Result<(PathBuf, PipelineManifest), Box<dyn std::error::Error>> {
    let config_path = work_directory.join("manifest.json");
    if resume {
        let mut existing: PipelineManifest =
            serde_json::from_slice(&fs::read(&config_path).map_err(|error| {
                format!("cannot resume without {}: {error}", config_path.display())
            })?)?;
        if !manifests_have_same_inputs_and_options(&existing, &manifest) {
            return Err("resume rejected: input fingerprint or analysis options changed".into());
        }
        let metadata_recall_mode_changed =
            existing.options.metadata_recall_mode != manifest.options.metadata_recall_mode;
        if metadata_recall_mode_changed {
            invalidate_stage_checkpoints(&mut existing, &["metadata_complete", "finalized"]);
            remove_ready_checkpoints(work_directory, &["metadata"])?;
        }
        let revisions_changed = invalidate_changed_stage_revisions(
            &mut existing,
            manifest.stage_revisions,
            work_directory,
        )?;
        if metadata_recall_mode_changed
            || revisions_changed
            || existing.binary_version != manifest.binary_version
            || existing.options != manifest.options
        {
            existing.binary_version = manifest.binary_version;
            existing.options = manifest.options;
            write_manifest_atomically(&config_path, &existing)?;
        }
        return Ok((config_path, existing));
    }

    if work_directory.exists() && fs::read_dir(work_directory)?.next().is_some() {
        return Err(format!(
            "work directory {} is not empty; use --resume only for an identical run",
            work_directory.display()
        )
        .into());
    }
    fs::create_dir_all(work_directory.join("partial"))?;
    fs::create_dir_all(work_directory.join("duckdb-temp"))?;
    write_manifest_atomically(&config_path, &manifest)?;
    Ok((config_path, manifest))
}

fn invalidate_changed_stage_revisions(
    manifest: &mut PipelineManifest,
    expected: StageRevisions,
    work_directory: &Path,
) -> Result<bool, Box<dyn std::error::Error>> {
    let previous = manifest.stage_revisions;
    if previous == expected {
        return Ok(false);
    }

    if previous.prepare != expected.prepare {
        invalidate_stage_checkpoints(
            manifest,
            &[
                "contracts_ready",
                "uri_complete",
                "metadata_compact_ready",
                "prepare_complete",
                "name_complete",
                "metadata_complete",
                "finalized",
            ],
        );
        remove_ready_checkpoints(work_directory, &["prepare", "name", "metadata"])?;
    } else {
        if previous.name != expected.name {
            invalidate_stage_checkpoints(manifest, &["name_complete", "finalized"]);
            remove_ready_checkpoints(work_directory, &["name"])?;
        }
        if previous.metadata != expected.metadata {
            invalidate_stage_checkpoints(manifest, &["metadata_complete", "finalized"]);
            remove_ready_checkpoints(work_directory, &["metadata"])?;
        }
        if previous.finalizer != expected.finalizer {
            invalidate_stage_checkpoints(manifest, &["finalized"]);
        }
    }

    manifest.stage_revisions = expected;
    Ok(true)
}

fn invalidate_stage_checkpoints(manifest: &mut PipelineManifest, stages: &[&str]) {
    for stage in stages {
        manifest
            .stages
            .insert((*stage).to_string(), StageCheckpoint::default());
    }
}

fn remove_ready_checkpoints(
    work_directory: &Path,
    phases: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    for phase in phases {
        let path = work_directory
            .join("checkpoints")
            .join(format!("{phase}.ready.json"));
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(format!(
                    "could not invalidate stale ready checkpoint {}: {error}",
                    path.display()
                )
                .into());
            }
        }
    }
    Ok(())
}

fn initial_stage_checkpoints() -> BTreeMap<String, StageCheckpoint> {
    [
        ("input_validated", true),
        ("contracts_ready", false),
        ("uri_complete", false),
        ("metadata_compact_ready", false),
        ("prepare_complete", false),
        ("name_complete", false),
        ("metadata_complete", false),
        ("finalized", false),
    ]
    .into_iter()
    .map(|(name, complete)| {
        (
            name.to_string(),
            StageCheckpoint {
                complete,
                artifacts: Vec::new(),
            },
        )
    })
    .collect()
}

fn manifests_have_same_inputs_and_options(
    existing: &PipelineManifest,
    expected: &PipelineManifest,
) -> bool {
    existing.schema_version == expected.schema_version
        && existing.inputs == expected.inputs
        && existing.chains == expected.chains
        && existing.options.database_path == expected.options.database_path
        && existing.options.parquet_inputs == expected.options.parquet_inputs
        && existing.options.output_dir == expected.options.output_dir
        && existing.options.name_threshold == expected.options.name_threshold
}

fn fingerprint_artifact(path: &Path) -> Result<ArtifactFingerprint, Box<dyn std::error::Error>> {
    let canonical_path = path.canonicalize()?;
    let mut file = fs::File::open(&canonical_path)?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 8 * 1024 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(ArtifactFingerprint {
        path: canonical_path,
        size: file.metadata()?.len(),
        row_count: artifact_row_count(path),
        sha256: sha256_hex(hasher.finalize().as_ref()),
    })
}

fn artifact_row_count(path: &Path) -> Option<u64> {
    match path.extension().and_then(|extension| extension.to_str()) {
        Some("json") => fs::read(path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
            .and_then(|value| value["summary_rows"].as_array().map(std::vec::Vec::len))
            .and_then(|count| u64::try_from(count).ok()),
        Some("csv") => fs::read_to_string(path)
            .ok()
            .map(|text| text.lines().count().saturating_sub(1))
            .and_then(|count| u64::try_from(count).ok()),
        _ => None,
    }
}

fn checkpoint_is_complete_and_valid(
    manifest: &PipelineManifest,
    stage: &str,
    _work_directory: &Path,
) -> Result<bool, Box<dyn std::error::Error>> {
    let Some(checkpoint) = manifest.stages.get(stage) else {
        return Err(format!("manifest is missing required stage {stage:?}").into());
    };
    if !checkpoint.complete {
        return Ok(false);
    }
    for expected in &checkpoint.artifacts {
        let actual = fingerprint_artifact(&expected.path).map_err(|error| {
            format!(
                "resume rejected: artifact for stage {stage:?} is unavailable ({}): {error}",
                expected.path.display()
            )
        })?;
        if actual != *expected {
            return Err(format!(
                "resume rejected: artifact for stage {stage:?} changed: {}",
                expected.path.display()
            )
            .into());
        }
    }
    Ok(true)
}

fn validate_resume_database_for_downstream(
    manifest: &PipelineManifest,
    completed_stage: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let stage_complete = |stage: &str| {
        manifest
            .stages
            .get(stage)
            .is_some_and(|checkpoint| checkpoint.complete)
    };
    let required_tables: &[&str] = match completed_stage {
        "prepare_complete" if !stage_complete("name_complete") => &[
            "analysis_contracts",
            "metadata_rows",
            "metadata_contract_token_rows",
            "metadata_token_stats",
            "name_atoms",
            "selected_chains",
        ],
        "prepare_complete" if !stage_complete("metadata_complete") => &[
            "analysis_contracts",
            "metadata_rows",
            "metadata_contract_token_rows",
            "metadata_token_stats",
            "selected_chains",
        ],
        "name_complete" if !stage_complete("metadata_complete") => &[
            "analysis_contracts",
            "metadata_rows",
            "metadata_contract_token_rows",
            "metadata_token_stats",
            "selected_chains",
        ],
        _ => return Ok(()),
    };
    if !manifest.options.database_path.is_file() {
        return Err(format!(
            "resume rejected: {} is missing for incomplete downstream stages",
            manifest.options.database_path.display()
        )
        .into());
    }
    let conn = Connection::open(&manifest.options.database_path)?;
    for table in required_tables {
        let exists = conn.query_row(
            "SELECT count(*) > 0 FROM duckdb_tables() WHERE table_name = ?",
            [*table],
            |row| row.get::<_, bool>(0),
        )?;
        if !exists {
            return Err(format!(
                "resume rejected: stage database is missing required table {table:?}"
            )
            .into());
        }
    }
    Ok(())
}

fn mark_phase_complete(
    manifest: &mut PipelineManifest,
    phase: InternalPhase,
    artifact: ArtifactFingerprint,
) {
    let stages: &[&str] = match phase {
        InternalPhase::Prepare => &[
            "contracts_ready",
            "uri_complete",
            "metadata_compact_ready",
            "prepare_complete",
        ],
        InternalPhase::Name => &["name_complete"],
        InternalPhase::Metadata => &["metadata_complete"],
    };
    for stage in stages {
        manifest.stages.insert(
            (*stage).to_string(),
            StageCheckpoint {
                complete: true,
                artifacts: if *stage == "prepare_complete"
                    || *stage == "name_complete"
                    || *stage == "metadata_complete"
                {
                    vec![artifact.clone()]
                } else {
                    Vec::new()
                },
            },
        );
    }
}

fn promote_ready_phase(
    manifest: &mut PipelineManifest,
    phase: InternalPhase,
    expected_partial: &str,
    work_directory: &Path,
) -> Result<bool, Box<dyn std::error::Error>> {
    let phase_name = match phase {
        InternalPhase::Prepare => "prepare",
        InternalPhase::Name => "name",
        InternalPhase::Metadata => "metadata",
    };
    let ready_path = work_directory
        .join("checkpoints")
        .join(format!("{phase_name}.ready.json"));
    if !ready_path.is_file() {
        return Ok(false);
    }
    let ready: PhaseReady = serde_json::from_slice(&fs::read(&ready_path)?)?;
    if ready.phase != phase_name || ready.partial_file != expected_partial {
        return Err(format!(
            "resume rejected: malformed ready checkpoint {}",
            ready_path.display()
        )
        .into());
    }
    let artifact = fingerprint_artifact(&work_directory.join("partial").join(expected_partial))?;
    if artifact.size != ready.size || artifact.sha256 != ready.sha256 {
        return Err(format!(
            "resume rejected: ready checkpoint hash does not match {}",
            artifact.path.display()
        )
        .into());
    }
    mark_phase_complete(manifest, phase, artifact);
    Ok(true)
}

fn write_manifest_atomically(
    destination: &Path,
    manifest: &PipelineManifest,
) -> Result<(), Box<dyn std::error::Error>> {
    let partial = destination.with_extension("json.partial");
    let mut file = fs::File::create(&partial)?;
    serde_json::to_writer_pretty(&mut file, manifest)?;
    file.flush()?;
    file.sync_all()?;
    drop(file);
    replace_file_atomically(&partial, destination)?;
    Ok(())
}

fn run_child_phase(
    phase: InternalPhase,
    config_path: &Path,
    diagnostics: bool,
    phase_lease: &mut ControllerPhaseLease,
) -> Result<(), Box<dyn std::error::Error>> {
    let phase_name = match phase {
        InternalPhase::Prepare => "prepare",
        InternalPhase::Name => "name",
        InternalPhase::Metadata => "metadata",
    };
    let work_directory = config_path
        .parent()
        .ok_or("internal config has no parent directory")?;
    let manifest = if diagnostics {
        Some(serde_json::from_slice::<PipelineManifest>(&fs::read(
            config_path,
        )?)?)
    } else {
        None
    };
    let input_rows = manifest.as_ref().map_or(0, |manifest| {
        manifest
            .inputs
            .iter()
            .fold(0u64, |total, input| total.saturating_add(input.row_count))
    });
    let temp_directory = work_directory.join("duckdb-temp");
    let started = Instant::now();
    let mut command = Command::new(std::env::current_exe()?);
    command
        .arg("--internal-phase")
        .arg(phase_name)
        .arg("--internal-config")
        .arg(config_path)
        .stdin(Stdio::piped())
        .env(PARENT_LIVENESS_ENV, "1")
        .env(PHASE_GENERATION_ENV, phase_lease.generation());
    if diagnostics {
        command.env("NAME_URI_ANALYSIS_DIAGNOSTICS", "1");
    } else {
        command.env_remove("NAME_URI_ANALYSIS_DIAGNOSTICS");
    }
    let mut child = command.spawn()?;
    let parent_liveness = child
        .stdin
        .take()
        .ok_or("internal phase did not expose its parent-liveness pipe")?;
    // The controller owns the phase lease while reading/updating pipeline
    // state. Release it only after the child exists; the child blocks on the
    // same OS lock before reading the manifest or touching DuckDB.
    phase_lease.release_for_child()?;
    if !diagnostics {
        let status_result = child.wait();
        drop(parent_liveness);
        if status_result.is_err() {
            let _ = child.kill();
            let _ = child.wait();
        }
        let reclaim_result = phase_lease.reclaim_after_child();
        let status = status_result?;
        reclaim_result?;
        if !status.success() {
            return Err(format!("{phase_name} child process failed with {status}").into());
        }
        return Ok(());
    }

    let pid = Pid::from_u32(child.id());
    let mut system = System::new();
    let mut peak_rss_bytes = 0u64;
    let mut peak_duckdb_temp_bytes = directory_size(&temp_directory).unwrap_or(0);
    let mut cpu_millis = 0.0f64;
    let mut io_read_bytes = 0u64;
    let mut io_written_bytes = 0u64;
    let mut last_sample = Instant::now();
    let mut last_temp_sample = Instant::now();
    let status_result = loop {
        system.refresh_processes(ProcessesToUpdate::Some(&[pid]), false);
        if let Some(process) = system.process(pid) {
            peak_rss_bytes = peak_rss_bytes.max(process.memory());
            let sample_millis = last_sample.elapsed().as_secs_f64() * 1_000.0;
            cpu_millis += f64::from(process.cpu_usage()) * sample_millis / 100.0;
            let disk = process.disk_usage();
            io_read_bytes = disk.total_read_bytes;
            io_written_bytes = disk.total_written_bytes;
        }
        last_sample = Instant::now();
        if last_temp_sample.elapsed() >= Duration::from_secs(1) {
            peak_duckdb_temp_bytes =
                peak_duckdb_temp_bytes.max(directory_size(&temp_directory).unwrap_or(0));
            last_temp_sample = Instant::now();
        }
        match child.try_wait() {
            Ok(Some(status)) => break Ok(status),
            Ok(None) => {}
            Err(error) => break Err(error),
        }
        thread::sleep(Duration::from_millis(200));
    };
    drop(parent_liveness);
    if status_result.is_err() {
        let _ = child.kill();
        let _ = child.wait();
    }
    let reclaim_result = phase_lease.reclaim_after_child();
    let status = status_result?;
    reclaim_result?;
    peak_duckdb_temp_bytes =
        peak_duckdb_temp_bytes.max(directory_size(&temp_directory).unwrap_or(0));
    let partial_name = match phase {
        InternalPhase::Prepare => "uri-summary.json",
        InternalPhase::Name => "name-summary.json",
        InternalPhase::Metadata => "metadata-summary.json",
    };
    let partial_path = work_directory.join("partial").join(partial_name);
    let summary_rows = fs::read(&partial_path)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
        .and_then(|value| value["summary_rows"].as_array().map(std::vec::Vec::len))
        .and_then(|count| u64::try_from(count).ok())
        .unwrap_or(0);
    let metric = PhaseMetric {
        phase: phase_name,
        wall_millis: started.elapsed().as_millis(),
        cpu_millis: cpu_millis.round() as u64,
        success: status.success(),
        input_rows,
        summary_rows,
        peak_rss_bytes,
        peak_duckdb_temp_bytes,
        io_read_bytes,
        io_written_bytes,
        database_bytes: fs::metadata(
            &manifest
                .as_ref()
                .expect("diagnostic manifest is loaded before spawning the child")
                .options
                .database_path,
        )
        .map(|metadata| metadata.len())
        .unwrap_or(0),
        artifact_bytes: directory_size(&work_directory.join("artifacts"))
            .unwrap_or(0)
            .saturating_add(
                fs::metadata(partial_path)
                    .map(|value| value.len())
                    .unwrap_or(0),
            ),
    };
    record_phase_metric(work_directory, &metric);
    if !status.success() {
        return Err(format!("{phase_name} child process failed with {status}").into());
    }
    Ok(())
}

fn directory_size(path: &Path) -> std::io::Result<u64> {
    if !path.exists() {
        return Ok(0);
    }
    if path.is_file() {
        return Ok(fs::metadata(path)?.len());
    }
    let mut total = 0u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        total = total.saturating_add(directory_size(&entry.path())?);
    }
    Ok(total)
}

fn write_metric_atomically(
    work_directory: &Path,
    metric: &PhaseMetric<'_>,
) -> Result<(), Box<dyn std::error::Error>> {
    let metrics_directory = work_directory.join("metrics");
    fs::create_dir_all(&metrics_directory)?;
    let destination = metrics_directory.join(format!("{}-phase.json", metric.phase));
    let partial = destination.with_extension("json.partial");
    let mut file = fs::File::create(&partial)?;
    serde_json::to_writer_pretty(&mut file, metric)?;
    file.flush()?;
    file.sync_all()?;
    drop(file);
    replace_file_atomically(&partial, &destination)?;
    Ok(())
}

fn record_phase_metric(work_directory: &Path, metric: &PhaseMetric<'_>) {
    if let Err(error) = write_metric_atomically(work_directory, metric) {
        eprintln!(
            "warning: could not persist {} phase metrics: {error}",
            metric.phase
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
                metadata_recall_mode: MetadataRecallMode::Conservative,
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
    fn controller_does_not_scan_parquet_before_prepare_phase() {
        let source = include_str!("main.rs");
        let obsolete_call = ["inspect_selected_", "chains("].concat();
        assert!(!source.contains(&obsolete_call));
    }

    #[test]
    fn output_directory_cannot_be_deleted_with_work_directory() {
        let directory = tempfile::tempdir().unwrap();
        let work = directory.path().join("work");

        let error = validate_directory_layout(&work, &work.join("output")).unwrap_err();

        assert!(error.to_string().contains("inside --work-directory"));
    }

    #[test]
    fn output_containment_normalizes_parent_components() {
        let directory = tempfile::tempdir().unwrap();
        let work = directory.path().join("work");
        fs::create_dir_all(&work).unwrap();
        let disguised_child = directory
            .path()
            .join("other")
            .join("..")
            .join("work")
            .join("output");

        let error = validate_directory_layout(&work, &disguised_child).unwrap_err();

        assert!(error.to_string().contains("inside --work-directory"));
    }

    #[cfg(windows)]
    #[test]
    fn output_containment_resolves_directory_symlinks() {
        let directory = tempfile::tempdir().unwrap();
        let work = directory.path().join("work");
        let alias = directory.path().join("work-alias");
        fs::create_dir_all(&work).unwrap();
        std::os::windows::fs::symlink_dir(&work, &alias).unwrap();

        let error = validate_directory_layout(&work, &alias.join("output")).unwrap_err();

        assert!(error.to_string().contains("inside --work-directory"));
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
        let value: serde_json::Value =
            serde_json::from_slice(&fs::read(destination).unwrap()).unwrap();
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
    fn failed_post_success_cleanup_does_not_turn_success_into_an_error() {
        let temp = tempfile::tempdir().unwrap();
        let not_a_directory = temp.path().join("work");
        fs::write(&not_a_directory, b"occupied by a file").unwrap();

        remove_work_directory_after_success(&not_a_directory);

        assert!(not_a_directory.is_file());
    }

    #[test]
    fn controller_lock_rejects_a_concurrent_owner_and_releases_on_drop() {
        let temp = tempfile::tempdir().unwrap();
        let work = temp.path().join("work");
        let first = ControllerLock::acquire(&work).unwrap();

        let error = ControllerLock::acquire(&work).unwrap_err();
        assert!(error.to_string().contains("already controlled"));

        drop(first);
        ControllerLock::acquire(&work).unwrap();
    }

    #[test]
    fn controller_lock_reuses_stale_metadata_without_replacing_the_file() {
        let temp = tempfile::tempdir().unwrap();
        let work = temp.path().join("work");
        let lock = temp.path().join(".work.name-uri-analysis.lock");
        let alias = temp.path().join("controller-lock-alias");
        fs::write(&lock, format!("{} 0", u32::MAX)).unwrap();
        fs::hard_link(&lock, &alias).unwrap();

        let acquired = ControllerLock::acquire(&work).unwrap();

        assert!(lock.is_file());
        drop(acquired);
        assert_eq!(fs::read(&lock).unwrap(), fs::read(&alias).unwrap());
        assert!(lock.is_file());
    }

    #[test]
    fn phase_lock_blocks_controller_probe_until_the_phase_releases() {
        let temp = tempfile::tempdir().unwrap();
        let work = temp.path().join("work");
        let phase = PhaseLock::acquire(&work).unwrap();

        let error = ensure_phase_idle(&work).unwrap_err();
        assert!(error.to_string().contains("analysis phase is still active"));

        drop(phase);
        ensure_phase_idle(&work).unwrap();
    }

    #[test]
    fn controller_phase_lease_hands_work_to_a_child_and_reclaims_it() {
        let temp = tempfile::tempdir().unwrap();
        let work = temp.path().join("work");
        let mut lease = ControllerPhaseLease::acquire(&work).unwrap();

        assert!(ensure_phase_idle(&work).is_err());
        lease.release_for_child().unwrap();
        let child_phase = PhaseLock::acquire(&work).unwrap();
        drop(child_phase);
        lease.reclaim_after_child().unwrap();

        assert!(ensure_phase_idle(&work).is_err());
    }

    #[test]
    fn phase_generation_rejects_a_stale_waiter_after_controller_replacement() {
        let temp = tempfile::tempdir().unwrap();
        let work = temp.path().join("work");
        let stale_generation = {
            let lease = ControllerPhaseLease::acquire(&work).unwrap();
            lease.generation().to_string()
        };
        let mut current = ControllerPhaseLease::acquire(&work).unwrap();
        let current_generation = current.generation().to_string();
        assert_ne!(stale_generation, current_generation);

        current.release_for_child().unwrap();
        let stale_waiter = PhaseLock::acquire_blocking(&work).unwrap();
        let error = validate_phase_generation(&work, &stale_generation).unwrap_err();
        assert!(error
            .to_string()
            .contains("stale internal phase generation"));
        drop(stale_waiter);

        let current_child = PhaseLock::acquire_blocking(&work).unwrap();
        validate_phase_generation(&work, &current_generation).unwrap();
        drop(current_child);
        current.reclaim_after_child().unwrap();
    }

    #[test]
    fn internal_phase_waits_for_the_controller_to_release_its_lease() {
        let temp = tempfile::tempdir().unwrap();
        let work = temp.path().join("work");
        let controller_phase = PhaseLock::acquire(&work).unwrap();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (acquired_tx, acquired_rx) = std::sync::mpsc::channel();
        let child_work = work.clone();
        let child = thread::spawn(move || {
            started_tx.send(()).unwrap();
            let phase = PhaseLock::acquire_blocking(&child_work).unwrap();
            acquired_tx.send(()).unwrap();
            phase
        });

        started_rx.recv().unwrap();
        assert!(acquired_rx.recv_timeout(Duration::from_millis(50)).is_err());
        drop(controller_phase);
        acquired_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        drop(child.join().unwrap());
    }

    #[test]
    fn controller_holds_phase_lease_before_reading_pipeline_state() {
        let source = include_str!("main.rs");
        let controller = source.find("let _controller_lock").unwrap();
        let lease = source[controller..]
            .find("ControllerPhaseLease::acquire(&work_directory)")
            .map(|offset| controller + offset)
            .unwrap();
        let fingerprint = source[controller..]
            .find("fingerprint_inputs(&args.parquet_inputs)")
            .map(|offset| controller + offset)
            .unwrap();

        assert!(controller < lease && lease < fingerprint);
    }

    #[test]
    fn internal_phase_locks_work_state_before_reading_the_manifest() {
        let source = include_str!("main.rs");
        let start = source.find("fn run_internal_phase").unwrap();
        let end = source[start..].find("fn fingerprint_inputs").unwrap() + start;
        let body = &source[start..end];
        let phase_lock = body
            .find("PhaseLock::acquire_blocking(work_directory)")
            .unwrap();
        let generation = body.find("validate_phase_generation_from_env(").unwrap();
        let manifest_read = body.find("fs::read(config_path)").unwrap();

        assert!(phase_lock < generation && generation < manifest_read);
    }

    #[test]
    fn parent_liveness_watcher_invokes_callback_on_eof() {
        let callbacks = std::cell::Cell::new(0usize);

        watch_parent_liveness(std::io::Cursor::new(b"parent-alive"), || {
            callbacks.set(callbacks.get() + 1);
        });

        assert_eq!(callbacks.get(), 1);
    }

    #[test]
    fn parent_liveness_watcher_invokes_callback_on_read_error() {
        struct FailedReader;

        impl Read for FailedReader {
            fn read(&mut self, _buffer: &mut [u8]) -> std::io::Result<usize> {
                Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "test disconnect",
                ))
            }
        }

        let disconnected = std::cell::Cell::new(false);
        watch_parent_liveness(FailedReader, || disconnected.set(true));

        assert!(disconnected.get());
    }

    #[test]
    fn child_phase_keeps_a_private_parent_liveness_pipe_until_wait_finishes() {
        let source = include_str!("main.rs");
        let start = source.find("fn run_child_phase").unwrap();
        let end = source[start..].find("fn directory_size").unwrap() + start;
        let body = &source[start..end];
        let body = body
            .chars()
            .filter(|character| !character.is_whitespace())
            .collect::<String>();

        assert!(body.contains(".stdin(Stdio::piped())"));
        assert!(body.contains(".env(PARENT_LIVENESS_ENV,\"1\")"));
        assert!(body.contains(".env(PHASE_GENERATION_ENV,phase_lease.generation())"));
        let take = body.find("child.stdin.take()").unwrap();
        let wait = body.find("child.wait()").unwrap();
        let release = body.find("drop(parent_liveness)").unwrap();
        assert!(take < wait && wait < release);
    }

    #[test]
    fn child_phase_hands_off_and_reclaims_the_controller_phase_lease() {
        let source = include_str!("main.rs");
        let start = source.find("fn run_child_phase").unwrap();
        let end = source[start..].find("fn directory_size").unwrap() + start;
        let body = source[start..end]
            .chars()
            .filter(|character| !character.is_whitespace())
            .collect::<String>();

        let spawn = body.find("command.spawn()?").unwrap();
        let release = body.find("phase_lease.release_for_child()?").unwrap();
        let wait = body.find("child.wait()").unwrap();
        let reclaim = body.find("phase_lease.reclaim_after_child()").unwrap();
        assert!(spawn < release && release < wait && wait < reclaim);
    }

    #[test]
    fn internal_phase_starts_parent_watchdog_before_acquiring_phase_lock() {
        let source = include_str!("main.rs");
        let start = source.find("fn run_internal_phase").unwrap();
        let end = source[start..].find("fn fingerprint_inputs").unwrap() + start;
        let body = &source[start..end];
        let watchdog = body.find("start_parent_liveness_watchdog()?").unwrap();
        let phase_lock = body
            .find("PhaseLock::acquire_blocking(work_directory)")
            .unwrap();

        assert!(watchdog < phase_lock);
    }

    #[cfg(windows)]
    #[test]
    fn output_containment_comparison_is_case_insensitive_on_windows() {
        assert!(path_is_same_or_descendant(
            Path::new(r"C:\DATA\WORK\output"),
            Path::new(r"c:\data\work")
        ));
    }

    #[test]
    fn manifest_replacement_never_deletes_the_last_durable_copy_first() {
        let source = include_str!("main.rs");
        let start = source.find("fn write_manifest_atomically").unwrap();
        let end = source[start..].find("fn run_child_phase").unwrap() + start;
        let writer = &source[start..end];

        assert!(writer.contains("replace_file_atomically"));
        assert!(!writer.contains("remove_file(destination)"));
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
            parquet_sql_path(&first),
            parquet_sql_path(&second)
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
            parquet_sql_path(&parquet)
        ))
        .unwrap();

        let error = fingerprint_inputs(&[parquet.clone(), parquet]).unwrap_err();

        assert!(error.to_string().contains("duplicate Parquet input"));
    }

    #[test]
    fn manifest_compatibility_allows_resource_tuning_but_not_semantic_changes() {
        let temp = tempfile::tempdir().unwrap();
        let expected = sample_manifest(temp.path());
        let mut existing = expected.clone();
        existing.stages.get_mut("name_complete").unwrap().complete = true;
        assert!(manifests_have_same_inputs_and_options(&existing, &expected));

        existing.binary_version = "new-compatible-binary".to_string();
        assert!(manifests_have_same_inputs_and_options(&existing, &expected));

        existing.options.threads = 128;
        existing.options.memory_limit = "384GiB".to_string();
        existing.options.analysis_memory_limit = Some("384GiB".to_string());
        existing.options.duckdb_memory_limit = "320GiB".to_string();
        assert!(manifests_have_same_inputs_and_options(&existing, &expected));

        existing.inputs[0].row_count += 1;
        assert!(!manifests_have_same_inputs_and_options(
            &existing, &expected
        ));
        existing = expected.clone();
        existing.options.name_threshold = 96.0;
        assert!(!manifests_have_same_inputs_and_options(
            &existing, &expected
        ));
    }

    #[test]
    fn resume_rebinds_a_stage_compatible_manifest_to_the_current_binary() {
        let temp = tempfile::tempdir().unwrap();
        let work = temp.path().join("work");
        fs::create_dir_all(&work).unwrap();
        let mut existing = sample_manifest(&work);
        existing.binary_version = "old-binary".to_string();
        existing
            .stages
            .get_mut("prepare_complete")
            .unwrap()
            .complete = true;
        let manifest_path = work.join("manifest.json");
        fs::write(&manifest_path, serde_json::to_vec(&existing).unwrap()).unwrap();
        let mut expected = existing.clone();
        expected.binary_version = "new-binary".to_string();
        expected.options.threads = 128;
        expected.options.memory_limit = "384GiB".to_string();
        expected.options.analysis_memory_limit = Some("384GiB".to_string());
        expected.options.duckdb_memory_limit = "320GiB".to_string();

        let (_, rebound) = prepare_work_directory(&work, expected, true).unwrap();
        let persisted: PipelineManifest =
            serde_json::from_slice(&fs::read(manifest_path).unwrap()).unwrap();

        assert_eq!(rebound.binary_version, "new-binary");
        assert_eq!(persisted.binary_version, "new-binary");
        assert_eq!(persisted.options.threads, 128);
        assert_eq!(
            persisted.options.analysis_memory_limit.as_deref(),
            Some("384GiB")
        );
        assert!(persisted.stages["prepare_complete"].complete);
    }

    #[test]
    fn changing_metadata_recall_mode_invalidates_only_metadata_and_finalizer() {
        let temp = tempfile::tempdir().unwrap();
        let work = temp.path().join("work");
        fs::create_dir_all(&work).unwrap();
        let mut existing = sample_manifest(&work);
        existing.options.metadata_recall_mode = MetadataRecallMode::Exact;
        for checkpoint in existing.stages.values_mut() {
            checkpoint.complete = true;
        }
        fs::write(
            work.join("manifest.json"),
            serde_json::to_vec(&existing).unwrap(),
        )
        .unwrap();
        let mut expected = existing.clone();
        expected.options.metadata_recall_mode = MetadataRecallMode::Conservative;

        let (_, resumed) = prepare_work_directory(&work, expected, true).unwrap();

        assert!(resumed.stages["prepare_complete"].complete);
        assert!(resumed.stages["name_complete"].complete);
        assert!(!resumed.stages["metadata_complete"].complete);
        assert!(!resumed.stages["finalized"].complete);
    }

    #[test]
    fn resume_stage_revision_changes_follow_the_dependency_graph() {
        struct Case {
            revisions: serde_json::Value,
            invalidated_stages: &'static [&'static str],
            invalidated_ready_phases: &'static [&'static str],
        }

        let cases = [
            Case {
                revisions: serde_json::json!({
                    "prepare": 0,
                    "name": 1,
                    "metadata": 2,
                    "finalizer": 1,
                }),
                invalidated_stages: &[
                    "contracts_ready",
                    "uri_complete",
                    "metadata_compact_ready",
                    "prepare_complete",
                    "name_complete",
                    "metadata_complete",
                    "finalized",
                ],
                invalidated_ready_phases: &["prepare", "name", "metadata"],
            },
            Case {
                revisions: serde_json::json!({
                    "prepare": 1,
                    "name": 0,
                    "metadata": 2,
                    "finalizer": 1,
                }),
                invalidated_stages: &["name_complete", "finalized"],
                invalidated_ready_phases: &["name"],
            },
            Case {
                revisions: serde_json::json!({
                    "prepare": 1,
                    "name": 1,
                    "metadata": 1,
                    "finalizer": 1,
                }),
                invalidated_stages: &["metadata_complete", "finalized"],
                invalidated_ready_phases: &["metadata"],
            },
            Case {
                revisions: serde_json::json!({
                    "prepare": 1,
                    "name": 1,
                    "metadata": 2,
                    "finalizer": 0,
                }),
                invalidated_stages: &["finalized"],
                invalidated_ready_phases: &[],
            },
        ];

        for (case_index, case) in cases.into_iter().enumerate() {
            let temp = tempfile::tempdir().unwrap();
            let work = temp.path().join(format!("work-{case_index}"));
            let checkpoints = work.join("checkpoints");
            fs::create_dir_all(&checkpoints).unwrap();
            let mut existing = sample_manifest(&work);
            for checkpoint in existing.stages.values_mut() {
                checkpoint.complete = true;
            }
            let mut serialized = serde_json::to_value(existing).unwrap();
            serialized["stage_revisions"] = case.revisions;
            fs::write(
                work.join("manifest.json"),
                serde_json::to_vec(&serialized).unwrap(),
            )
            .unwrap();
            for phase in ["prepare", "name", "metadata"] {
                fs::write(
                    checkpoints.join(format!("{phase}.ready.json")),
                    b"stale-ready",
                )
                .unwrap();
            }

            let (_, rebound) = prepare_work_directory(&work, sample_manifest(&work), true).unwrap();

            for (stage, checkpoint) in &rebound.stages {
                let should_be_complete = !case.invalidated_stages.contains(&stage.as_str());
                assert_eq!(
                    checkpoint.complete, should_be_complete,
                    "unexpected {stage:?} state for case {case_index}"
                );
                if !should_be_complete {
                    assert!(checkpoint.artifacts.is_empty());
                }
            }
            for phase in ["prepare", "name", "metadata"] {
                let should_exist = !case.invalidated_ready_phases.contains(&phase);
                assert_eq!(
                    checkpoints.join(format!("{phase}.ready.json")).exists(),
                    should_exist,
                    "unexpected {phase:?} ready checkpoint state for case {case_index}"
                );
            }
        }
    }

    #[test]
    fn legacy_manifest_without_stage_revisions_is_safely_invalidated_and_upgraded() {
        let temp = tempfile::tempdir().unwrap();
        let work = temp.path().join("work");
        fs::create_dir_all(&work).unwrap();
        let mut legacy = sample_manifest(&work);
        for checkpoint in legacy.stages.values_mut() {
            checkpoint.complete = true;
        }
        let mut serialized = serde_json::to_value(legacy).unwrap();
        serialized
            .as_object_mut()
            .unwrap()
            .remove("stage_revisions");
        let manifest_path = work.join("manifest.json");
        fs::write(&manifest_path, serde_json::to_vec(&serialized).unwrap()).unwrap();

        let (_, rebound) = prepare_work_directory(&work, sample_manifest(&work), true).unwrap();
        let persisted: serde_json::Value =
            serde_json::from_slice(&fs::read(manifest_path).unwrap()).unwrap();

        assert!(rebound.stages["input_validated"].complete);
        for stage in [
            "contracts_ready",
            "uri_complete",
            "metadata_compact_ready",
            "prepare_complete",
            "name_complete",
            "metadata_complete",
            "finalized",
        ] {
            assert!(!rebound.stages[stage].complete, "legacy stage {stage:?}");
        }
        assert!(persisted["stage_revisions"].is_object());
    }

    #[test]
    fn completed_checkpoint_rejects_tampered_artifact() {
        let temp = tempfile::tempdir().unwrap();
        let artifact_path = temp.path().join("partial.json");
        fs::write(&artifact_path, b"original").unwrap();
        let mut manifest = sample_manifest(temp.path());
        manifest.stages.insert(
            "name_complete".to_string(),
            StageCheckpoint {
                complete: true,
                artifacts: vec![fingerprint_artifact(&artifact_path).unwrap()],
            },
        );
        assert!(checkpoint_is_complete_and_valid(&manifest, "name_complete", temp.path()).unwrap());

        fs::write(&artifact_path, b"tampered").unwrap();
        let error =
            checkpoint_is_complete_and_valid(&manifest, "name_complete", temp.path()).unwrap_err();
        assert!(error.to_string().contains("changed"));
    }

    #[test]
    fn resume_rejects_missing_database_table_needed_by_next_phase() {
        let temp = tempfile::tempdir().unwrap();
        let mut manifest = sample_manifest(temp.path());
        manifest
            .stages
            .get_mut("prepare_complete")
            .unwrap()
            .complete = true;
        let conn = Connection::open(&manifest.options.database_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE analysis_contracts(id INTEGER);
             CREATE TABLE metadata_rows(id INTEGER);
             CREATE TABLE metadata_contract_token_rows(id INTEGER);
             CREATE TABLE metadata_token_stats(id INTEGER);
             CREATE TABLE selected_chains(chain VARCHAR);",
        )
        .unwrap();
        drop(conn);

        let error =
            validate_resume_database_for_downstream(&manifest, "prepare_complete").unwrap_err();

        assert!(error.to_string().contains("name_atoms"));
    }

    #[test]
    fn ready_checkpoint_promotes_phase_after_controller_restart() {
        let temp = tempfile::tempdir().unwrap();
        fs::create_dir_all(temp.path().join("partial")).unwrap();
        fs::create_dir_all(temp.path().join("checkpoints")).unwrap();
        let partial = temp.path().join("partial/name-summary.json");
        fs::write(&partial, br#"{"summary_rows":[]}"#).unwrap();
        let fingerprint = fingerprint_artifact(&partial).unwrap();
        let ready = PhaseReady {
            phase: "name".to_string(),
            partial_file: "name-summary.json".to_string(),
            size: fingerprint.size,
            sha256: fingerprint.sha256,
        };
        fs::write(
            temp.path().join("checkpoints/name.ready.json"),
            serde_json::to_vec(&ready).unwrap(),
        )
        .unwrap();
        let mut manifest = sample_manifest(temp.path());

        assert!(promote_ready_phase(
            &mut manifest,
            InternalPhase::Name,
            "name-summary.json",
            temp.path(),
        )
        .unwrap());
        let checkpoint = manifest.stages.get("name_complete").unwrap();
        assert!(checkpoint.complete);
        assert_eq!(checkpoint.artifacts.len(), 1);
    }
}
