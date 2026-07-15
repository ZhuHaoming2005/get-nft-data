use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use name_uri_analysis_rs::analysis::{run_analysis_phase, MATCH_ETA_FORECAST_ENV};
use sysinfo::{Pid, ProcessesToUpdate, System};

use crate::controller_constants::{
    PARENT_LIVENESS_ENV, PHASE_GENERATION_ENV, PIPELINE_SCHEMA_VERSION,
};
use crate::controller_fingerprint::binary_identity;
use crate::controller_locks::{
    start_parent_liveness_watchdog, validate_phase_generation_from_env, ControllerPhaseLease,
    PhaseLock,
};
use crate::controller_manifest::{
    load_match_eta_forecast, match_observation_key, record_match_observation, record_phase_metric,
    MatchExecutionKind, MatchObservation, MatchOutcome, MatchSampledResources, PhaseMetric,
    PipelineManifest,
};
use crate::InternalPhase;

const MATCH_RESOURCE_SAMPLE_INTERVAL: Duration = Duration::from_millis(200);

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
    resumed_run: bool,
    phase_lease: &mut ControllerPhaseLease,
) -> Result<(), Box<dyn std::error::Error>> {
    let phase_name = phase.cli_name();
    let work_directory = config_path
        .parent()
        .ok_or("internal config has no parent directory")?;
    let manifest = serde_json::from_slice::<PipelineManifest>(&fs::read(config_path)?)?;
    let input_rows = manifest
        .inputs
        .iter()
        .fold(0u64, |total, input| total.saturating_add(input.row_count));
    let match_key = if phase == InternalPhase::MetadataMatch {
        match match_observation_key(&manifest, work_directory) {
            Ok(key) => Some(key),
            Err(error) => {
                eprintln!("warning: could not identify Metadata Match observation key: {error}");
                None
            }
        }
    } else {
        None
    };
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
        .env(PHASE_GENERATION_ENV, phase_lease.generation())
        .env_remove(MATCH_ETA_FORECAST_ENV);
    if let Some(key) = match_key.as_ref() {
        match load_match_eta_forecast(&manifest.options.output_dir, key)
            .and_then(|forecast| serde_json::to_string(&forecast).map_err(Into::into))
        {
            Ok(forecast) => {
                command.env(MATCH_ETA_FORECAST_ENV, forecast);
            }
            Err(error) => {
                eprintln!("warning: could not load Metadata Match ETA history: {error}");
            }
        }
    }
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
    let monitor_resources = should_monitor_resources(phase, diagnostics);
    if !monitor_resources {
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
    let (monitor_stop, monitor_receiver) = mpsc::channel();
    let monitor_temp_directory = temp_directory.clone();
    let monitor = thread::spawn(move || {
        monitor_child_resources(pid, &monitor_temp_directory, diagnostics, monitor_receiver)
    });
    let status_result = child.wait();
    // Capture wall time at wait completion. Resource sampling runs separately,
    // so its one-second low-overhead cadence cannot quantize the observation.
    let wall_millis = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    drop(parent_liveness);
    if status_result.is_err() {
        let _ = child.kill();
        let _ = child.wait();
    }
    let _ = monitor_stop.send(());
    let mut resource_sample = monitor.join().unwrap_or_default();
    if diagnostics {
        resource_sample.peak_duckdb_temp_bytes = resource_sample
            .peak_duckdb_temp_bytes
            .max(directory_size(&temp_directory).unwrap_or(0));
    }
    let reclaim_result = phase_lease.reclaim_after_child();
    let succeeded = status_result
        .as_ref()
        .is_ok_and(std::process::ExitStatus::success);
    if let Some(key) = match_key {
        let observation = MatchObservation::new(
            key,
            if resumed_run {
                MatchExecutionKind::ResumeRecompute
            } else {
                MatchExecutionKind::Fresh
            },
            if succeeded {
                MatchOutcome::Success
            } else {
                MatchOutcome::Failure
            },
            wall_millis,
            MatchSampledResources {
                peak_rss_bytes: resource_sample.peak_rss_bytes,
                io_read_bytes: resource_sample.io_read_bytes,
                io_written_bytes: resource_sample.io_written_bytes,
                sample_interval_millis: u64::try_from(MATCH_RESOURCE_SAMPLE_INTERVAL.as_millis())
                    .unwrap_or(u64::MAX),
            },
        );
        if let Err(error) = record_match_observation(&manifest.options.output_dir, &observation) {
            eprintln!("warning: could not persist Metadata Match observation: {error}");
        }
    }
    reclaim_result?;
    let status = status_result?;
    if diagnostics {
        let partial_name = match phase {
            InternalPhase::Prepare => "uri-summary.json",
            InternalPhase::MetadataEncode => "metadata-encode-summary.json",
            InternalPhase::Name => "name-summary.json",
            InternalPhase::MetadataMatch => "metadata-summary.json",
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
            wall_millis: u128::from(wall_millis),
            cpu_millis: resource_sample.cpu_millis.round() as u64,
            success: status.success(),
            input_rows,
            summary_rows,
            peak_rss_bytes: resource_sample.peak_rss_bytes,
            peak_duckdb_temp_bytes: resource_sample.peak_duckdb_temp_bytes,
            io_read_bytes: resource_sample.io_read_bytes,
            io_written_bytes: resource_sample.io_written_bytes,
            database_bytes: fs::metadata(&manifest.options.database_path)
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
    }
    if !status.success() {
        return Err(format!("{phase_name} child process failed with {status}").into());
    }
    Ok(())
}

fn should_monitor_resources(phase: InternalPhase, diagnostics: bool) -> bool {
    diagnostics || phase == InternalPhase::MetadataMatch
}

#[derive(Default)]
struct ChildResourceSample {
    peak_rss_bytes: u64,
    peak_duckdb_temp_bytes: u64,
    cpu_millis: f64,
    io_read_bytes: u64,
    io_written_bytes: u64,
}

fn monitor_child_resources(
    pid: Pid,
    temp_directory: &Path,
    diagnostics: bool,
    stop: mpsc::Receiver<()>,
) -> ChildResourceSample {
    let interval = MATCH_RESOURCE_SAMPLE_INTERVAL;
    let mut system = System::new();
    let mut sample = ChildResourceSample {
        peak_duckdb_temp_bytes: if diagnostics {
            directory_size(temp_directory).unwrap_or(0)
        } else {
            0
        },
        ..ChildResourceSample::default()
    };
    let mut last_sample = Instant::now();
    let mut last_temp_sample = Instant::now();
    loop {
        system.refresh_processes(ProcessesToUpdate::Some(&[pid]), false);
        if let Some(process) = system.process(pid) {
            sample.peak_rss_bytes = sample.peak_rss_bytes.max(process.memory());
            let sample_millis = last_sample.elapsed().as_secs_f64() * 1_000.0;
            sample.cpu_millis += f64::from(process.cpu_usage()) * sample_millis / 100.0;
            let disk = process.disk_usage();
            sample.io_read_bytes = disk.total_read_bytes;
            sample.io_written_bytes = disk.total_written_bytes;
        }
        last_sample = Instant::now();
        if diagnostics && last_temp_sample.elapsed() >= Duration::from_secs(1) {
            sample.peak_duckdb_temp_bytes = sample
                .peak_duckdb_temp_bytes
                .max(directory_size(temp_directory).unwrap_or(0));
            last_temp_sample = Instant::now();
        }
        match stop.recv_timeout(interval) {
            Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                // Best-effort final refresh. On platforms where the process
                // remains queryable briefly after wait(), this captures the
                // final cumulative I/O counters without delaying wall time.
                system.refresh_processes(ProcessesToUpdate::Some(&[pid]), false);
                if let Some(process) = system.process(pid) {
                    sample.peak_rss_bytes = sample.peak_rss_bytes.max(process.memory());
                    let disk = process.disk_usage();
                    sample.io_read_bytes = disk.total_read_bytes;
                    sample.io_written_bytes = disk.total_written_bytes;
                }
                break;
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
    }
    sample
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_match_monitoring_is_always_on_without_diagnostics() {
        assert!(should_monitor_resources(
            InternalPhase::MetadataMatch,
            false
        ));
        assert!(!should_monitor_resources(InternalPhase::Name, false));
        assert!(should_monitor_resources(InternalPhase::Name, true));
    }
}
