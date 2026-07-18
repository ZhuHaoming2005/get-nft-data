use clap::ValueEnum;
use dedup_linux::{
    LifecycleHandle, NumaMetricsHandle, NumaNodeExecutionMetrics, process_page_faults,
    process_resident_memory_bytes, replace_file,
};
use dedup_model::{DedupError, ProgressObserver};
use serde::Serialize;
use std::fs::{self, File, OpenOptions};
use std::io::{IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const EWMA_ALPHA: f64 = 0.25;

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum ProgressMode {
    Auto,
    Tty,
    Json,
    Off,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EffectiveMode {
    Tty,
    Json,
    Off,
}

pub struct ProgressReporter {
    shared: Arc<Shared>,
    worker: Option<JoinHandle<()>>,
}

struct Shared {
    state: Mutex<State>,
    changed: Condvar,
    stopping: AtomicBool,
    memory_limit_bytes: AtomicU64,
    mode: EffectiveMode,
    interval: Duration,
    snapshot_path: PathBuf,
    history_path: PathBuf,
    lifecycle: LifecycleHandle,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RunStatus {
    Idle,
    Running,
    Complete,
    Interrupted,
    Failed,
}

#[derive(Debug)]
struct State {
    stage: String,
    phase: String,
    status: RunStatus,
    completed: u64,
    total: Option<u64>,
    stage_started: Option<Instant>,
    phase_started: Option<Instant>,
    phase_epoch: u64,
    sequence: u64,
    error: Option<String>,
    numa_metrics: Option<NumaMetricsHandle>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ProgressSnapshot {
    pub sequence: u64,
    pub unix_timestamp_seconds: u64,
    pub stage: String,
    pub phase: String,
    pub status: &'static str,
    pub completed: u64,
    pub total: Option<u64>,
    pub percent: Option<f64>,
    pub elapsed_seconds: f64,
    pub phase_elapsed_seconds: f64,
    pub throughput_per_second: Option<f64>,
    pub eta_seconds: Option<u64>,
    pub eta_confident: bool,
    pub error: Option<String>,
    pub resident_memory_bytes: Option<u64>,
    pub memory_limit_bytes: Option<u64>,
    pub memory_pressure_percent: Option<f64>,
    pub minor_page_faults: Option<u64>,
    pub major_page_faults: Option<u64>,
    pub numa_nodes: Vec<NumaNodeExecutionMetrics>,
}

struct Sampler {
    phase_epoch: u64,
    last_completed: u64,
    last_sample: Instant,
    ewma_rate: Option<f64>,
    positive_samples: u32,
}

impl ProgressReporter {
    pub fn should_stop_intake(&self) -> bool {
        self.shared.lifecycle.should_stop_intake()
    }

    pub fn new(
        run_dir: &Path,
        requested_mode: ProgressMode,
        interval: Duration,
        lifecycle: LifecycleHandle,
    ) -> Result<Self, DedupError> {
        let mode = match requested_mode {
            ProgressMode::Auto => {
                if std::io::stderr().is_terminal() {
                    EffectiveMode::Tty
                } else {
                    EffectiveMode::Json
                }
            }
            ProgressMode::Tty => EffectiveMode::Tty,
            ProgressMode::Json => EffectiveMode::Json,
            ProgressMode::Off => EffectiveMode::Off,
        };
        if interval < Duration::from_millis(100) {
            return Err(DedupError::InvalidInput {
                context: dedup_model::ErrorContext::stage("progress"),
                message: "progress interval must be at least 100 ms".to_owned(),
            });
        }
        fs::create_dir_all(run_dir)?;
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                stage: String::new(),
                phase: String::new(),
                status: RunStatus::Idle,
                completed: 0,
                total: None,
                stage_started: None,
                phase_started: None,
                phase_epoch: 0,
                sequence: 0,
                error: None,
                numa_metrics: None,
            }),
            changed: Condvar::new(),
            stopping: AtomicBool::new(false),
            memory_limit_bytes: AtomicU64::new(0),
            mode,
            interval,
            snapshot_path: run_dir.join("progress.json"),
            history_path: run_dir.join("progress.jsonl"),
            lifecycle,
        });
        let worker = if mode == EffectiveMode::Off {
            None
        } else {
            let worker_shared = Arc::clone(&shared);
            Some(
                thread::Builder::new()
                    .name("dedup-progress".to_owned())
                    .spawn(move || render_loop(&worker_shared))?,
            )
        };
        Ok(Self { shared, worker })
    }

    pub fn begin_stage(&self, stage: &'static str) {
        let now = Instant::now();
        self.update(|state| {
            state.stage.clear();
            state.stage.push_str(stage);
            state.phase.clear();
            state.status = RunStatus::Running;
            state.completed = 0;
            state.total = None;
            state.stage_started = Some(now);
            state.phase_started = Some(now);
            state.phase_epoch = state.phase_epoch.wrapping_add(1);
            state.error = None;
            state.numa_metrics = None;
        });
    }

    pub fn set_numa_metrics(&self, metrics: NumaMetricsHandle) {
        self.update(|state| state.numa_metrics = Some(metrics));
    }

    pub fn set_memory_limit(&self, bytes: u64) {
        self.shared
            .memory_limit_bytes
            .store(bytes, Ordering::Release);
    }

    pub fn stage_elapsed_seconds(&self) -> f64 {
        let state = lock(&self.shared.state);
        state
            .stage_started
            .map_or(0.0, |started| started.elapsed().as_secs_f64())
    }

    pub fn resident_memory_bytes(&self) -> Option<u64> {
        process_resident_memory_bytes()
    }

    pub fn finish_stage(&self, result: &Result<(), DedupError>) {
        self.update(|state| match result {
            Ok(()) => {
                state.status = RunStatus::Complete;
                if let Some(total) = state.total {
                    state.completed = total;
                }
                state.error = None;
            }
            Err(DedupError::Interrupted { stage }) => {
                state.status = RunStatus::Interrupted;
                state.error = Some(format!(
                    "controlled shutdown requested while running {stage}"
                ));
            }
            Err(error) => {
                state.status = RunStatus::Failed;
                state.error = Some(error.to_string());
            }
        });
    }

    fn update(&self, change: impl FnOnce(&mut State)) {
        let mut state = lock(&self.shared.state);
        change(&mut state);
        state.sequence = state.sequence.wrapping_add(1);
        drop(state);
        self.shared.changed.notify_all();
    }
}

impl ProgressObserver for ProgressReporter {
    fn begin_phase(&self, phase: &'static str, total: Option<u64>) {
        if self.shared.mode == EffectiveMode::Off {
            return;
        }
        let now = Instant::now();
        self.update(|state| {
            state.phase.clear();
            state.phase.push_str(phase);
            state.completed = 0;
            state.total = total;
            state.phase_started = Some(now);
            state.phase_epoch = state.phase_epoch.wrapping_add(1);
        });
    }

    fn set_total(&self, total: u64) {
        if self.shared.mode != EffectiveMode::Off {
            self.update(|state| state.total = Some(total));
        }
    }

    fn advance(&self, amount: u64) {
        if self.shared.mode == EffectiveMode::Off || amount == 0 {
            return;
        }
        // Work updates are sampled by the renderer interval. Avoid waking the
        // reporting thread from hot loops; phase and stage transitions still
        // notify immediately.
        let mut state = lock(&self.shared.state);
        state.completed = state.completed.saturating_add(amount);
        state.sequence = state.sequence.wrapping_add(1);
    }

    fn is_cancelled(&self) -> bool {
        self.shared.lifecycle.should_stop()
    }
}

impl Drop for ProgressReporter {
    fn drop(&mut self) {
        self.shared.stopping.store(true, Ordering::Release);
        self.shared.changed.notify_all();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn render_loop(shared: &Shared) {
    let mut sampler = Sampler {
        phase_epoch: 0,
        last_completed: 0,
        last_sample: Instant::now(),
        ewma_rate: None,
        positive_samples: 0,
    };
    let mut last_sequence = u64::MAX;
    let mut tty_line_open = false;
    loop {
        let state = lock(&shared.state);
        let wait = shared.changed.wait_timeout(state, shared.interval);
        let (state, _) = match wait {
            Ok(value) => value,
            Err(poisoned) => poisoned.into_inner(),
        };
        let stopping = shared.stopping.load(Ordering::Acquire);
        let now = Instant::now();
        let should_render =
            state.status != RunStatus::Idle && (state.sequence != last_sequence || !stopping);
        if should_render {
            let snapshot = sample_snapshot(
                &state,
                &mut sampler,
                now,
                shared.memory_limit_bytes.load(Ordering::Acquire),
            );
            if let Err(error) = persist_snapshot(&shared.snapshot_path, &snapshot) {
                let _ = writeln!(std::io::stderr(), "progress snapshot error: {error}");
            }
            if let Err(error) = append_snapshot(&shared.history_path, &snapshot) {
                let _ = writeln!(std::io::stderr(), "progress history error: {error}");
            }
            match shared.mode {
                EffectiveMode::Tty => {
                    let terminal = format_tty(&snapshot);
                    let final_state = matches!(
                        state.status,
                        RunStatus::Complete | RunStatus::Interrupted | RunStatus::Failed
                    );
                    if final_state {
                        let _ = writeln!(std::io::stderr(), "\r{terminal}\x1b[K");
                        tty_line_open = false;
                    } else {
                        let _ = write!(std::io::stderr(), "\r{terminal}\x1b[K");
                        let _ = std::io::stderr().flush();
                        tty_line_open = true;
                    }
                }
                EffectiveMode::Json => {
                    if let Ok(encoded) = serde_json::to_string(&snapshot) {
                        let _ = writeln!(std::io::stderr(), "{encoded}");
                    }
                }
                EffectiveMode::Off => {}
            }
            last_sequence = state.sequence;
        }
        drop(state);
        if stopping {
            if tty_line_open {
                let _ = writeln!(std::io::stderr());
            }
            break;
        }
    }
}

fn sample_snapshot(
    state: &State,
    sampler: &mut Sampler,
    now: Instant,
    configured_memory_limit: u64,
) -> ProgressSnapshot {
    if sampler.phase_epoch != state.phase_epoch {
        sampler.phase_epoch = state.phase_epoch;
        sampler.last_completed = state.completed;
        sampler.last_sample = now;
        sampler.ewma_rate = None;
        sampler.positive_samples = 0;
    } else {
        let seconds = now.duration_since(sampler.last_sample).as_secs_f64();
        let delta = state.completed.saturating_sub(sampler.last_completed);
        if seconds > 0.0 && delta > 0 {
            let observed = delta as f64 / seconds;
            sampler.ewma_rate = Some(sampler.ewma_rate.map_or(observed, |current| {
                EWMA_ALPHA.mul_add(observed, (1.0 - EWMA_ALPHA) * current)
            }));
            sampler.positive_samples = sampler.positive_samples.saturating_add(1);
        }
        sampler.last_completed = state.completed;
        sampler.last_sample = now;
    }
    let eta_confident = sampler.positive_samples >= 3;
    let eta_seconds = state.total.and_then(|total| {
        let remaining = total.saturating_sub(state.completed);
        sampler
            .ewma_rate
            .and_then(|rate| (rate > 0.0).then(|| (remaining as f64 / rate).ceil() as u64))
    });
    let percent = state.total.and_then(|total| {
        (total > 0).then(|| (state.completed.min(total) as f64 / total as f64) * 100.0)
    });
    let resident_memory_bytes = process_resident_memory_bytes();
    let memory_limit_bytes = (configured_memory_limit > 0).then_some(configured_memory_limit);
    let memory_pressure_percent = resident_memory_bytes
        .zip(memory_limit_bytes)
        .filter(|(_, limit)| *limit > 0)
        .map(|(resident, limit)| resident as f64 / limit as f64 * 100.0);
    let page_faults = process_page_faults();
    ProgressSnapshot {
        sequence: state.sequence,
        unix_timestamp_seconds: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_secs()),
        stage: state.stage.clone(),
        phase: state.phase.clone(),
        status: status_text(state.status),
        completed: state.completed,
        total: state.total,
        percent,
        elapsed_seconds: state
            .stage_started
            .map_or(0.0, |started| now.duration_since(started).as_secs_f64()),
        phase_elapsed_seconds: state
            .phase_started
            .map_or(0.0, |started| now.duration_since(started).as_secs_f64()),
        throughput_per_second: sampler.ewma_rate,
        eta_seconds,
        eta_confident,
        error: state.error.clone(),
        resident_memory_bytes,
        memory_limit_bytes,
        memory_pressure_percent,
        minor_page_faults: page_faults.map(|faults| faults.minor),
        major_page_faults: page_faults.map(|faults| faults.major),
        numa_nodes: state
            .numa_metrics
            .as_ref()
            .map_or_else(Vec::new, NumaMetricsHandle::snapshot),
    }
}

fn format_tty(snapshot: &ProgressSnapshot) -> String {
    let work = snapshot.total.map_or_else(
        || snapshot.completed.to_string(),
        |total| format!("{} / {total}", snapshot.completed),
    );
    let percent = snapshot
        .percent
        .map_or_else(|| "--.-%".to_owned(), |value| format!("{value:5.1}%"));
    let rate = snapshot
        .throughput_per_second
        .map_or_else(|| "--/s".to_owned(), |value| format!("{value:.1}/s"));
    let eta = snapshot.eta_seconds.map_or_else(
        || "ETA --".to_owned(),
        |seconds| {
            let prefix = if snapshot.eta_confident {
                "ETA"
            } else {
                "ETA≈"
            };
            format!("{prefix} {}", format_duration(seconds))
        },
    );
    let rss = snapshot.resident_memory_bytes.map_or_else(
        || "RSS --".to_owned(),
        |bytes| format!("RSS {}", format_bytes(bytes)),
    );
    let numa = if snapshot.numa_nodes.is_empty() {
        String::new()
    } else {
        let max_queue = snapshot
            .numa_nodes
            .iter()
            .map(|node| node.max_queue_depth)
            .max()
            .unwrap_or_default();
        let scheduled = snapshot
            .numa_nodes
            .iter()
            .map(|node| node.scheduled_chunks)
            .sum::<u64>();
        let remote = snapshot
            .numa_nodes
            .iter()
            .try_fold(0_u64, |total, node| {
                node.remote_chunks
                    .and_then(|remote| total.checked_add(remote))
            })
            .map_or_else(|| "unknown".to_owned(), |value| value.to_string());
        format!(" NUMA q={max_queue} scheduled={scheduled} remote={remote}")
    };
    format!(
        "[{}:{}] {percent} {work} {rate} {eta} {rss}{numa}",
        snapshot.stage, snapshot.phase,
    )
}

fn format_bytes(bytes: u64) -> String {
    const KIB: u64 = 1 << 10;
    const MIB: u64 = 1 << 20;
    const GIB: u64 = 1 << 30;
    if bytes >= GIB {
        format!("{:.1}GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.1}MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.1}KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{bytes}B")
    }
}

fn format_duration(seconds: u64) -> String {
    let hours = seconds / 3_600;
    let minutes = (seconds % 3_600) / 60;
    let seconds = seconds % 60;
    if hours > 0 {
        format!("{hours:02}:{minutes:02}:{seconds:02}")
    } else {
        format!("{minutes:02}:{seconds:02}")
    }
}

fn status_text(status: RunStatus) -> &'static str {
    match status {
        RunStatus::Idle => "idle",
        RunStatus::Running => "running",
        RunStatus::Complete => "complete",
        RunStatus::Interrupted => "interrupted",
        RunStatus::Failed => "failed",
    }
}

fn persist_snapshot(path: &Path, snapshot: &ProgressSnapshot) -> Result<(), std::io::Error> {
    let temporary = path.with_extension("json.tmp");
    let mut file = File::create(&temporary)?;
    serde_json::to_writer_pretty(&mut file, snapshot).map_err(std::io::Error::other)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    replace_file(&temporary, path)
}

fn append_snapshot(path: &Path, snapshot: &ProgressSnapshot) -> Result<(), std::io::Error> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    serde_json::to_writer(&mut file, snapshot).map_err(std::io::Error::other)?;
    file.write_all(b"\n")?;
    file.flush()
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eta_needs_positive_samples_and_uses_work_units() {
        let now = Instant::now();
        let mut state = State {
            stage: "name".to_owned(),
            phase: "score_candidates".to_owned(),
            status: RunStatus::Running,
            completed: 0,
            total: Some(100),
            stage_started: Some(now),
            phase_started: Some(now),
            phase_epoch: 1,
            sequence: 1,
            error: None,
            numa_metrics: None,
        };
        let mut sampler = Sampler {
            phase_epoch: 0,
            last_completed: 0,
            last_sample: now,
            ewma_rate: None,
            positive_samples: 0,
        };
        let first = sample_snapshot(&state, &mut sampler, now, 0);
        assert_eq!(first.eta_seconds, None);
        state.completed = 10;
        let second = sample_snapshot(&state, &mut sampler, now + Duration::from_secs(1), 0);
        assert_eq!(second.eta_seconds, Some(9));
        assert!(!second.eta_confident);
        state.completed = 20;
        let third = sample_snapshot(&state, &mut sampler, now + Duration::from_secs(2), 0);
        assert!(!third.eta_confident);
        state.completed = 30;
        let fourth = sample_snapshot(&state, &mut sampler, now + Duration::from_secs(3), 0);
        assert!(fourth.eta_confident);
        assert_eq!(fourth.eta_seconds, Some(7));
    }

    #[test]
    fn duration_format_is_stable() {
        assert_eq!(format_duration(65), "01:05");
        assert_eq!(format_duration(3_661), "01:01:01");
    }

    #[test]
    fn memory_format_uses_binary_units() {
        assert_eq!(format_bytes(512), "512B");
        assert_eq!(format_bytes(1 << 20), "1.0MiB");
        assert_eq!(format_bytes(3 << 30), "3.0GiB");
    }
}
