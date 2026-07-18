use clap::ValueEnum;
use dedup_core::{DedupError, EwmaEta, ProgressObserver};
use serde::Serialize;
use std::io::{self, IsTerminal, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

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
    meta: Mutex<Meta>,
    completed: AtomicU64,
    stopping: AtomicBool,
    cancelled: AtomicBool,
    mode: EffectiveMode,
    interval: Duration,
}

struct Meta {
    stage: String,
    phase: String,
    total: Option<u64>,
    stage_started: Instant,
    phase_started: Instant,
    last_completed: u64,
    last_tick: Instant,
    eta: EwmaEta,
}

#[derive(Serialize)]
struct ProgressLine {
    stage: String,
    phase: String,
    completed: u64,
    total: Option<u64>,
    percent: Option<f64>,
    rate: Option<f64>,
    eta_secs: Option<f64>,
    eta_confident: bool,
    phase_elapsed_secs: f64,
    stage_elapsed_secs: f64,
}

impl ProgressReporter {
    pub fn start(mode: ProgressMode, interval_ms: u64) -> Self {
        let effective = match mode {
            ProgressMode::Off => EffectiveMode::Off,
            ProgressMode::Tty => EffectiveMode::Tty,
            ProgressMode::Json => EffectiveMode::Json,
            ProgressMode::Auto => {
                if io::stderr().is_terminal() {
                    EffectiveMode::Tty
                } else {
                    EffectiveMode::Json
                }
            }
        };
        let now = Instant::now();
        let shared = Arc::new(Shared {
            meta: Mutex::new(Meta {
                stage: "idle".to_owned(),
                phase: String::new(),
                total: None,
                stage_started: now,
                phase_started: now,
                last_completed: 0,
                last_tick: now,
                eta: EwmaEta::new(EWMA_ALPHA),
            }),
            completed: AtomicU64::new(0),
            stopping: AtomicBool::new(false),
            cancelled: AtomicBool::new(false),
            mode: effective,
            interval: Duration::from_millis(interval_ms.max(100)),
        });
        let worker = if effective == EffectiveMode::Off {
            None
        } else {
            let shared_worker = Arc::clone(&shared);
            Some(thread::spawn(move || reporter_loop(shared_worker)))
        };
        Self { shared, worker }
    }

    pub fn cancel_handle(&self) -> CancelHandle {
        CancelHandle {
            shared: Arc::clone(&self.shared),
        }
    }

    pub fn finish(&mut self) {
        self.shared.stopping.store(true, Ordering::SeqCst);
        if let Some(handle) = self.worker.take() {
            let _ = handle.join();
        }
        self.emit_now();
        if self.shared.mode == EffectiveMode::Tty {
            let _ = writeln!(io::stderr());
        }
    }

    fn emit_now(&self) {
        emit_snapshot(&self.shared);
    }
}

#[derive(Clone)]
pub struct CancelHandle {
    shared: Arc<Shared>,
}

impl CancelHandle {
    pub fn request_cancel(&self) {
        self.shared.cancelled.store(true, Ordering::SeqCst);
    }
}

impl ProgressObserver for ProgressReporter {
    fn set_stage(&self, stage: &str) {
        let mut meta = self.shared.meta.lock().expect("progress lock");
        let now = Instant::now();
        meta.stage = stage.to_owned();
        meta.phase = String::new();
        meta.total = None;
        meta.stage_started = now;
        meta.phase_started = now;
        meta.last_completed = 0;
        meta.last_tick = now;
        meta.eta = EwmaEta::new(EWMA_ALPHA);
        self.shared.completed.store(0, Ordering::Relaxed);
    }

    fn begin_phase(&self, phase: &str, total: Option<u64>) {
        let mut meta = self.shared.meta.lock().expect("progress lock");
        let now = Instant::now();
        meta.phase = phase.to_owned();
        meta.total = total;
        meta.phase_started = now;
        meta.last_completed = 0;
        meta.last_tick = now;
        meta.eta = EwmaEta::new(EWMA_ALPHA);
        self.shared.completed.store(0, Ordering::Relaxed);
    }

    fn set_total(&self, total: Option<u64>) {
        self.shared.meta.lock().expect("progress lock").total = total;
    }

    fn add_completed(&self, delta: u64) {
        self.shared.completed.fetch_add(delta, Ordering::Relaxed);
    }

    fn check_cancelled(&self) -> Result<(), DedupError> {
        if self.shared.cancelled.load(Ordering::SeqCst) {
            Err(DedupError::Interrupted)
        } else {
            Ok(())
        }
    }
}

fn reporter_loop(shared: Arc<Shared>) {
    while !shared.stopping.load(Ordering::SeqCst) {
        thread::sleep(shared.interval);
        emit_snapshot(&shared);
    }
}

fn emit_snapshot(shared: &Shared) {
    if shared.mode == EffectiveMode::Off {
        return;
    }
    let mut meta = shared.meta.lock().expect("progress lock");
    // Read the phase metadata and its resettable counter under the same phase lock.
    // Workers still update the counter lock-free, while phase changes cannot pair a
    // new phase label with the previous phase's completed value.
    let completed = shared.completed.load(Ordering::Acquire);
    let now = Instant::now();
    let dt = now.duration_since(meta.last_tick).as_secs_f64().max(1e-6);
    let delta = completed.saturating_sub(meta.last_completed);
    let instant_rate = delta as f64 / dt;
    meta.eta.observe(instant_rate);
    meta.last_completed = completed;
    meta.last_tick = now;

    let remaining = meta.total.map(|total| total.saturating_sub(completed));
    let percent = meta
        .total
        .and_then(|t| (t > 0).then_some(100.0 * completed as f64 / t as f64));
    let line = ProgressLine {
        stage: meta.stage.clone(),
        phase: meta.phase.clone(),
        completed,
        total: meta.total,
        percent,
        rate: meta.eta.rate(),
        eta_secs: remaining.and_then(|r| meta.eta.eta_secs(r)),
        eta_confident: meta.eta.confident(),
        phase_elapsed_secs: meta.phase_started.elapsed().as_secs_f64(),
        stage_elapsed_secs: meta.stage_started.elapsed().as_secs_f64(),
    };
    drop(meta);

    match shared.mode {
        EffectiveMode::Json => {
            if let Ok(json) = serde_json::to_string(&line) {
                let _ = writeln!(io::stderr(), "{json}");
            }
        }
        EffectiveMode::Tty => {
            let label = if line.phase.is_empty() {
                line.stage.clone()
            } else {
                format!("{}/{}", line.stage, line.phase)
            };
            let progress = match line.total {
                Some(t) => format!("{}/{}", line.completed, t),
                None => format!("{} done", line.completed),
            };
            let pct = line
                .percent
                .map(|p| format!("{p:.1}%"))
                .unwrap_or_else(|| "--".to_owned());
            let rate = line
                .rate
                .map(|r| format!("{r:.0}/s"))
                .unwrap_or_else(|| "-/s".to_owned());
            let elapsed = format_duration(line.phase_elapsed_secs);
            let eta = match line.total {
                None => "n/a".to_owned(),
                Some(_) => match (line.eta_secs, line.eta_confident) {
                    (Some(secs), true) => format_duration(secs),
                    (Some(secs), false) => format!("~{}", format_duration(secs)),
                    (None, _) => "...".to_owned(),
                },
            };
            let _ = write!(
                io::stderr(),
                "\r[{label}] {progress} {pct} {rate} elapsed={elapsed} eta={eta}\x1b[K"
            );
            let _ = io::stderr().flush();
        }
        EffectiveMode::Off => {}
    }
}

fn format_duration(secs: f64) -> String {
    if !secs.is_finite() || secs < 0.0 {
        return "?".to_owned();
    }
    let total = secs.round() as u64;
    let h = total / 3600;
    let m = (total % 3600) / 60;
    let s = total % 60;
    if h > 0 {
        format!("{h}h{m:02}m{s:02}s")
    } else if m > 0 {
        format!("{m}m{s:02}s")
    } else {
        format!("{s}s")
    }
}
