use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use name_uri_analysis_rs::analysis::run_analysis_phase;
use sysinfo::{Pid, ProcessesToUpdate, System};

use crate::controller_constants::{
    PARENT_LIVENESS_ENV, PHASE_GENERATION_ENV, PIPELINE_SCHEMA_VERSION,
};
use crate::controller_fingerprint::binary_identity;
use crate::controller_locks::{
    start_parent_liveness_watchdog, validate_phase_generation_from_env, ControllerPhaseLease,
    PhaseLock,
};
use crate::controller_manifest::{record_phase_metric, PhaseMetric, PipelineManifest};
use crate::InternalPhase;

pub(crate) fn remove_work_directory_after_success(work_directory: &Path) {
    if let Err(error) = fs::remove_dir_all(work_directory) {
        eprintln!(
            "warning: analysis succeeded, but work directory {} could not be removed: {error}",
            work_directory.display()
        );
    }
}

pub(crate) fn run_internal_phase(
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

pub(crate) fn run_child_phase(
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

pub(crate) fn directory_size(path: &Path) -> std::io::Result<u64> {
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
