use clap::ValueEnum;
use dedup_core::{DedupError, EwmaEta, ProgressObserver};
use serde::Serialize;
use std::io::{self, IsTerminal, Write};
use std::sync::atomic::{AtomicBool, Ordering};
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
    state: Mutex<State>,
    stopping: AtomicBool,
    cancelled: AtomicBool,
    mode: EffectiveMode,
    interval: Duration,
}

struct State {
    stage: String,
    phase: String,
    completed: u64,
    total: Option<u64>,
    started: Instant,
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
    rate: Option<f64>,
    eta_secs: Option<f64>,
    eta_confident: bool,
    elapsed_secs: f64,
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
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                stage: "idle".to_owned(),
                phase: String::new(),
                completed: 0,
                total: None,
                started: Instant::now(),
                last_completed: 0,
                last_tick: Instant::now(),
                eta: EwmaEta::new(EWMA_ALPHA),
            }),
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
        let mut state = self.shared.state.lock().expect("progress lock");
        state.stage = stage.to_owned();
        state.phase = String::new();
        state.completed = 0;
        state.total = None;
        state.started = Instant::now();
        state.last_completed = 0;
        state.last_tick = Instant::now();
        state.eta = EwmaEta::new(EWMA_ALPHA);
    }

    fn set_phase(&self, phase: &str) {
        let mut state = self.shared.state.lock().expect("progress lock");
        state.phase = phase.to_owned();
        state.completed = 0;
        state.total = None;
        state.last_completed = 0;
        state.last_tick = Instant::now();
        state.eta = EwmaEta::new(EWMA_ALPHA);
    }

    fn set_total(&self, total: Option<u64>) {
        self.shared.state.lock().expect("progress lock").total = total;
    }

    fn add_completed(&self, delta: u64) {
        self.shared.state.lock().expect("progress lock").completed += delta;
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
    let mut state = shared.state.lock().expect("progress lock");
    let now = Instant::now();
    let dt = now.duration_since(state.last_tick).as_secs_f64().max(1e-6);
    let delta = state.completed.saturating_sub(state.last_completed);
    let instant_rate = delta as f64 / dt;
    state.eta.observe(instant_rate);
    state.last_completed = state.completed;
    state.last_tick = now;

    let remaining = state
        .total
        .map(|total| total.saturating_sub(state.completed));
    let line = ProgressLine {
        stage: state.stage.clone(),
        phase: state.phase.clone(),
        completed: state.completed,
        total: state.total,
        rate: state.eta.rate(),
        eta_secs: remaining.and_then(|r| state.eta.eta_secs(r)),
        eta_confident: state.eta.confident(),
        elapsed_secs: state.started.elapsed().as_secs_f64(),
    };
    drop(state);

    match shared.mode {
        EffectiveMode::Json => {
            if let Ok(json) = serde_json::to_string(&line) {
                let _ = writeln!(io::stderr(), "{json}");
            }
        }
        EffectiveMode::Tty => {
            let total = line
                .total
                .map(|t| t.to_string())
                .unwrap_or_else(|| "?".to_owned());
            let pct = match line.total {
                Some(t) if t > 0 => format!("{:.1}%", 100.0 * line.completed as f64 / t as f64),
                _ => "--".to_owned(),
            };
            let rate = line
                .rate
                .map(|r| format!("{r:.0}/s"))
                .unwrap_or_else(|| "-".to_owned());
            let eta = match (line.eta_secs, line.eta_confident) {
                (Some(secs), true) => format_duration(secs),
                (Some(secs), false) => format!("~{}", format_duration(secs)),
                (None, _) => "?".to_owned(),
            };
            let _ = write!(
                io::stderr(),
                "\r[{}/{}] {}/{} {} {} eta={}   ",
                line.stage,
                line.phase,
                line.completed,
                total,
                pct,
                rate,
                eta
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
