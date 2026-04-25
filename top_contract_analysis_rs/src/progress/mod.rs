use std::collections::BTreeMap;
use std::io::{IsTerminal, Write};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

#[async_trait]
pub trait SeedProgressReporter: Send + Sync {
    async fn on_seed_stage(&self, _stage: &str) {}
    async fn on_duplicate_contracts_started(&self, _total: usize) {}
    async fn on_duplicate_contract_completed(
        &self,
        _contract_address: &str,
        _completed: usize,
        _total: usize,
    ) {
    }
    async fn on_seed_completed(&self) {}
}

pub struct NoopProgressReporter;

#[async_trait]
impl SeedProgressReporter for NoopProgressReporter {}

pub trait BatchProgressReporter: Send + Sync {
    fn on_seed_cached(&self, _seed_address: &str) {}
    fn on_seed_started(&self, _seed_address: &str) {}
    fn on_seed_finished(&self, _seed_address: &str) {}
    fn on_seed_failed(&self, _seed_address: &str, _error: &str) {}
    fn create_seed_reporter(&self, _seed_address: &str) -> Arc<dyn SeedProgressReporter> {
        Arc::new(NoopProgressReporter)
    }
}

pub struct NoopBatchProgressReporter;

impl BatchProgressReporter for NoopBatchProgressReporter {}

pub fn create_single_seed_progress_reporter(seed_address: &str) -> Arc<dyn SeedProgressReporter> {
    if std::io::stderr().is_terminal() {
        Arc::new(TerminalSingleSeedProgressReporter::new(seed_address))
    } else {
        Arc::new(NoopProgressReporter)
    }
}

pub fn create_batch_progress_reporter(
    seed_addresses: &[String],
    workers: usize,
) -> Arc<dyn BatchProgressReporter> {
    if std::io::stderr().is_terminal() {
        Arc::new(TerminalBatchProgressReporter::new(seed_addresses, workers))
    } else {
        Arc::new(NoopBatchProgressReporter)
    }
}

struct TerminalSingleSeedProgressReporter {
    seed_address: String,
}

impl TerminalSingleSeedProgressReporter {
    fn new(seed_address: &str) -> Self {
        Self {
            seed_address: seed_address.to_string(),
        }
    }

    fn emit(&self, message: &str) {
        let mut stderr = std::io::stderr().lock();
        let _ = writeln!(
            stderr,
            "[seed {}] {}",
            short_address(&self.seed_address, 10),
            message
        );
    }
}

#[async_trait]
impl SeedProgressReporter for TerminalSingleSeedProgressReporter {
    async fn on_seed_stage(&self, stage: &str) {
        self.emit(stage_label(stage).as_str());
    }

    async fn on_duplicate_contracts_started(&self, total: usize) {
        if total == 0 {
            self.emit("No duplicate contracts");
        } else {
            self.emit(format!("Analyzing contracts 0/{total}").as_str());
        }
    }

    async fn on_duplicate_contract_completed(
        &self,
        contract_address: &str,
        completed: usize,
        total: usize,
    ) {
        self.emit(
            format!(
                "Analyzing contracts {completed}/{total} ({})",
                short_address(contract_address, 10)
            )
            .as_str(),
        );
    }

    async fn on_seed_completed(&self) {
        self.emit("Completed");
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum SeedStatus {
    Queued,
    Running,
    Completed,
    Failed,
}

struct SeedProgressState {
    stage_label: String,
    contract_completed: usize,
    contract_total: usize,
    status: SeedStatus,
}

impl Default for SeedProgressState {
    fn default() -> Self {
        Self {
            stage_label: "Queued".into(),
            contract_completed: 0,
            contract_total: 0,
            status: SeedStatus::Queued,
        }
    }
}

struct BatchProgressState {
    total: usize,
    completed: usize,
    workers: usize,
    seeds: BTreeMap<String, SeedProgressState>,
}

struct TerminalBatchProgressReporter {
    state: Arc<Mutex<BatchProgressState>>,
}

impl TerminalBatchProgressReporter {
    fn new(seed_addresses: &[String], workers: usize) -> Self {
        let mut seeds = BTreeMap::new();
        for seed_address in seed_addresses {
            seeds.insert(seed_address.clone(), SeedProgressState::default());
        }
        Self {
            state: Arc::new(Mutex::new(BatchProgressState {
                total: seed_addresses.len().max(1),
                completed: 0,
                workers: workers.max(1),
                seeds,
            })),
        }
    }

    fn update_seed(
        &self,
        seed_address: &str,
        stage_label: Option<String>,
        status: Option<SeedStatus>,
        contract_completed: Option<usize>,
        contract_total: Option<usize>,
        prefix: &str,
    ) {
        let (completed, total, running, workers, message) = {
            let mut state = self.state.lock().unwrap();
            let mut increment_completed = false;
            let message = {
                let seed_state = state
                    .seeds
                    .entry(seed_address.to_string())
                    .or_insert_with(SeedProgressState::default);
                if let Some(stage_label) = stage_label {
                    seed_state.stage_label = stage_label;
                }
                if let Some(contract_completed) = contract_completed {
                    seed_state.contract_completed = contract_completed;
                }
                if let Some(contract_total) = contract_total {
                    seed_state.contract_total = contract_total;
                }
                if let Some(status) = status {
                    increment_completed = status == SeedStatus::Completed
                        && seed_state.status != SeedStatus::Completed;
                    seed_state.status = status;
                }
                seed_state.stage_label.clone()
            };
            if increment_completed {
                state.completed += 1;
            }
            let running = state
                .seeds
                .values()
                .filter(|entry| entry.status == SeedStatus::Running)
                .count();
            (
                state.completed,
                state.total,
                running,
                state.workers,
                message,
            )
        };
        let mut stderr = std::io::stderr().lock();
        let _ = writeln!(
            stderr,
            "[batch {completed}/{total} | running {running}/{workers}] {} {prefix}: {message}",
            short_address(seed_address, 10),
        );
    }
}

impl BatchProgressReporter for TerminalBatchProgressReporter {
    fn on_seed_cached(&self, seed_address: &str) {
        self.update_seed(
            seed_address,
            Some("Cached".into()),
            Some(SeedStatus::Completed),
            None,
            None,
            "cached",
        );
    }

    fn on_seed_started(&self, seed_address: &str) {
        self.update_seed(
            seed_address,
            Some("Starting".into()),
            Some(SeedStatus::Running),
            Some(0),
            Some(0),
            "started",
        );
    }

    fn on_seed_finished(&self, seed_address: &str) {
        self.update_seed(
            seed_address,
            Some("Completed".into()),
            Some(SeedStatus::Completed),
            None,
            None,
            "finished",
        );
    }

    fn on_seed_failed(&self, seed_address: &str, error: &str) {
        self.update_seed(
            seed_address,
            Some(format!("Failed: {error}")),
            Some(SeedStatus::Failed),
            None,
            None,
            "failed",
        );
    }

    fn create_seed_reporter(&self, seed_address: &str) -> Arc<dyn SeedProgressReporter> {
        {
            let mut state = self.state.lock().unwrap();
            state
                .seeds
                .entry(seed_address.to_string())
                .or_insert_with(SeedProgressState::default);
        }
        Arc::new(BatchSeedProgressReporter {
            seed_address: seed_address.to_string(),
            state: self.state.clone(),
        })
    }
}

struct BatchSeedProgressReporter {
    seed_address: String,
    state: Arc<Mutex<BatchProgressState>>,
}

impl BatchSeedProgressReporter {
    fn emit(
        &self,
        stage_label: String,
        contract_completed: Option<usize>,
        contract_total: Option<usize>,
    ) {
        let (completed, total, running, workers, changed) = {
            let mut state = self.state.lock().unwrap();
            let changed = {
                let seed_state = state
                    .seeds
                    .entry(self.seed_address.clone())
                    .or_insert_with(SeedProgressState::default);
                let changed = seed_state.stage_label != stage_label
                    || contract_completed
                        .map(|value| value != seed_state.contract_completed)
                        .unwrap_or(false)
                    || contract_total
                        .map(|value| value != seed_state.contract_total)
                        .unwrap_or(false)
                    || seed_state.status != SeedStatus::Running;
                seed_state.stage_label = stage_label.clone();
                seed_state.status = SeedStatus::Running;
                if let Some(contract_completed) = contract_completed {
                    seed_state.contract_completed = contract_completed;
                }
                if let Some(contract_total) = contract_total {
                    seed_state.contract_total = contract_total;
                }
                changed
            };
            let running = state
                .seeds
                .values()
                .filter(|entry| entry.status == SeedStatus::Running)
                .count();
            (
                state.completed,
                state.total,
                running,
                state.workers,
                changed,
            )
        };
        if !changed {
            return;
        }
        let mut stderr = std::io::stderr().lock();
        let _ = writeln!(
            stderr,
            "[batch {completed}/{total} | running {running}/{workers}] {} stage: {stage_label}",
            short_address(&self.seed_address, 10),
        );
    }
}

#[async_trait]
impl SeedProgressReporter for BatchSeedProgressReporter {
    async fn on_seed_stage(&self, stage: &str) {
        self.emit(stage_label(stage), None, None);
    }

    async fn on_duplicate_contracts_started(&self, total: usize) {
        let label = if total == 0 {
            "No duplicate contracts".to_string()
        } else {
            format!("Analyzing contracts 0/{total}")
        };
        self.emit(label, Some(0), Some(total));
    }

    async fn on_duplicate_contract_completed(
        &self,
        contract_address: &str,
        completed: usize,
        total: usize,
    ) {
        self.emit(
            format!(
                "Analyzing contracts {completed}/{total} ({})",
                short_address(contract_address, 8)
            ),
            Some(completed),
            Some(total),
        );
    }

    async fn on_seed_completed(&self) {
        self.emit("Completed".into(), None, None);
    }
}

fn stage_label(stage: &str) -> String {
    match stage {
        "fetch_seed_context" => "Fetching seed metadata".into(),
        "fetch_license_sample" => "Checking license".into(),
        "load_snapshot" => "Loading recall snapshot".into(),
        "find_duplicate_candidates" => "Finding duplicate candidates".into(),
        "postprocess_candidates" => "Post-processing candidates".into(),
        "analyze_duplicate_contracts" => "Analyzing duplicate contracts".into(),
        "finalize_report" => "Finalizing report".into(),
        other => other.replace('_', " "),
    }
}

fn short_address(value: &str, width: usize) -> String {
    if value.len() > width {
        format!("{}...", &value[..width])
    } else {
        value.to_string()
    }
}
