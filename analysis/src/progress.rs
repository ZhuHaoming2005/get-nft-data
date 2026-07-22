use parking_lot::Mutex;
use serde::Serialize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

const EWMA_ALPHA: f64 = 0.25;
const STALE_RATE_AFTER: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkPhase {
    LoadValidate,
    BaseScan,
    Merge,
    PrepareMetadata,
    MetadataScan,
    FinalizeStore,
    UriIndex,
    UriQuery,
    UriQueryNameIndex,
    NameIndex,
    NameQuery,
    MetadataIndex,
    MetadataExact,
    MetadataBm25,
    CandidatePipeline,
    FinalReports,
    Durability,
    Complete,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PhaseSlot {
    Primary,
    Secondary,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PhaseTiming {
    pub phase: WorkPhase,
    pub slot: PhaseSlot,
    pub completed: u64,
    pub total: Option<u64>,
    pub started_at_ms: u64,
    pub finished_at_ms: u64,
    pub elapsed_ms: u64,
}

impl WorkPhase {
    const fn label(self) -> &'static str {
        match self {
            Self::LoadValidate => "校验输入",
            Self::BaseScan => "加载 Parquet",
            Self::Merge => "合并输入分片",
            Self::PrepareMetadata => "准备 Metadata",
            Self::MetadataScan => "加载 Metadata",
            Self::FinalizeStore => "完成常驻数据",
            Self::UriIndex => "构建 URI 索引",
            Self::UriQuery => "URI 查重",
            Self::UriQueryNameIndex => "URI 查重 + Name 索引",
            Self::NameIndex => "构建 Name 索引",
            Self::NameQuery => "Name 查重",
            Self::MetadataIndex => "构建 Metadata 索引",
            Self::MetadataExact => "Metadata exact",
            Self::MetadataBm25 => "Metadata BM25",
            Self::CandidatePipeline => "候选分析流水线",
            Self::FinalReports => "生成最终报告",
            Self::Durability => "持久化屏障",
            Self::Complete => "完成",
        }
    }
}

#[derive(Debug)]
struct PhaseState {
    phase: Option<WorkPhase>,
    total: Option<u64>,
    started: Instant,
    finished_elapsed: Option<Duration>,
    last_completed: u64,
    last_tick: Instant,
    last_progress: Instant,
    rate: EwmaRate,
}

impl PhaseState {
    fn new(now: Instant) -> Self {
        Self {
            phase: None,
            total: None,
            started: now,
            finished_elapsed: None,
            last_completed: 0,
            last_tick: now,
            last_progress: now,
            rate: EwmaRate::new(EWMA_ALPHA),
        }
    }

    fn reset(&mut self, phase: WorkPhase, total: Option<u64>, completed: u64, now: Instant) {
        self.phase = Some(phase);
        self.total = total;
        self.started = now;
        self.finished_elapsed = None;
        self.last_completed = completed;
        self.last_tick = now;
        self.last_progress = now;
        self.rate = EwmaRate::new(EWMA_ALPHA);
    }

    fn observe(&mut self, completed: u64, now: Instant) {
        if self.finished_elapsed.is_some() {
            return;
        }
        let elapsed = now.duration_since(self.last_tick).as_secs_f64();
        let delta = completed.saturating_sub(self.last_completed);
        if elapsed > 0.0 && delta > 0 {
            self.rate.observe(delta as f64 / elapsed);
            self.last_progress = now;
        }
        self.last_completed = completed;
        self.last_tick = now;
    }

    fn elapsed(&self, now: Instant) -> Duration {
        self.finished_elapsed
            .unwrap_or_else(|| now.duration_since(self.started))
    }
}

#[derive(Clone, Debug)]
struct EwmaRate {
    alpha: f64,
    rate: Option<f64>,
    positive_samples: u32,
}

impl EwmaRate {
    const fn new(alpha: f64) -> Self {
        Self {
            alpha,
            rate: None,
            positive_samples: 0,
        }
    }

    fn observe(&mut self, items_per_sec: f64) {
        if !items_per_sec.is_finite() || items_per_sec <= 0.0 {
            return;
        }
        self.rate = Some(match self.rate {
            Some(previous) => self.alpha * items_per_sec + (1.0 - self.alpha) * previous,
            None => items_per_sec,
        });
        self.positive_samples = self.positive_samples.saturating_add(1);
    }

    const fn confident(&self) -> bool {
        self.positive_samples >= 3
    }

    fn eta_ms(&self, remaining: u64) -> Option<u64> {
        let rate = self.rate?;
        duration_ms_from_rate(remaining, rate)
    }
}

#[derive(Debug)]
struct CandidateRateState {
    started: bool,
    last_written: u64,
    last_tick: Instant,
    rate: EwmaRate,
}

#[derive(Clone, Copy, Debug, Default)]
struct TerminalCounts {
    fetch_succeeded: u64,
    fetch_failed: u64,
    fetch_truncated: u64,
    analysis_succeeded: u64,
    analysis_failed: u64,
}

impl CandidateRateState {
    fn new(now: Instant) -> Self {
        Self {
            started: false,
            last_written: 0,
            last_tick: now,
            rate: EwmaRate::new(EWMA_ALPHA),
        }
    }

    fn start(&mut self, written: u64, now: Instant) {
        if self.started {
            return;
        }
        self.started = true;
        self.last_written = written;
        self.last_tick = now;
        self.rate = EwmaRate::new(EWMA_ALPHA);
    }

    fn observe(&mut self, written: u64, now: Instant) {
        if !self.started {
            return;
        }
        let elapsed = now.duration_since(self.last_tick).as_secs_f64();
        let delta = written.saturating_sub(self.last_written);
        if elapsed > 0.0 && delta > 0 {
            self.rate.observe(delta as f64 / elapsed);
        }
        self.last_written = written;
        self.last_tick = now;
    }
}

#[derive(Debug)]
pub struct Progress {
    started: Instant,
    phase: Mutex<PhaseState>,
    phase_completed: AtomicU64,
    secondary_phase: Mutex<PhaseState>,
    secondary_phase_completed: AtomicU64,
    phase_history: Mutex<Vec<PhaseTiming>>,
    total_row_groups: AtomicU64,
    completed_row_groups: AtomicU64,
    input_rows: AtomicU64,
    logical_nfts: AtomicU64,
    contracts: AtomicU64,
    postings_built: AtomicU64,
    uri_postings_built: AtomicU64,
    name_postings_built: AtomicU64,
    metadata_postings_built: AtomicU64,
    shard_batches_completed: AtomicU64,
    shards_sealed: AtomicU64,
    incomplete_seed_bitmap: [AtomicU64; crate::model::SEED_BITMAP_WORDS],
    incomplete_relations: AtomicU64,
    prefetch_skipped: AtomicU64,
    candidates: AtomicU64,
    candidates_discovered: AtomicU64,
    discovery_complete: AtomicBool,
    candidate_rate: Mutex<CandidateRateState>,
    terminal_counts: Mutex<TerminalCounts>,
    written: AtomicU64,
    durable: AtomicU64,
    memory_current_bytes: AtomicU64,
    memory_peak_bytes: AtomicU64,
    cpu_workers: AtomicU64,
    cpu_active: AtomicU64,
    cpu_queued: AtomicU64,
    network_pending: AtomicU64,
    network_inflight: AtomicU64,
    analysis_inflight: AtomicU64,
    compression_pending: AtomicU64,
    compression_inflight: AtomicU64,
    writer_pending: AtomicU64,
    writer_inflight: AtomicU64,
    durability_pending: AtomicU64,
}

impl Default for Progress {
    fn default() -> Self {
        let now = Instant::now();
        Self {
            started: now,
            phase: Mutex::new(PhaseState::new(now)),
            phase_completed: AtomicU64::new(0),
            secondary_phase: Mutex::new(PhaseState::new(now)),
            secondary_phase_completed: AtomicU64::new(0),
            phase_history: Mutex::new(Vec::new()),
            total_row_groups: AtomicU64::new(0),
            completed_row_groups: AtomicU64::new(0),
            input_rows: AtomicU64::new(0),
            logical_nfts: AtomicU64::new(0),
            contracts: AtomicU64::new(0),
            postings_built: AtomicU64::new(0),
            uri_postings_built: AtomicU64::new(0),
            name_postings_built: AtomicU64::new(0),
            metadata_postings_built: AtomicU64::new(0),
            shard_batches_completed: AtomicU64::new(0),
            shards_sealed: AtomicU64::new(0),
            incomplete_seed_bitmap: std::array::from_fn(|_| AtomicU64::new(0)),
            incomplete_relations: AtomicU64::new(0),
            prefetch_skipped: AtomicU64::new(0),
            candidates: AtomicU64::new(0),
            candidates_discovered: AtomicU64::new(0),
            discovery_complete: AtomicBool::new(false),
            candidate_rate: Mutex::new(CandidateRateState::new(now)),
            terminal_counts: Mutex::new(TerminalCounts::default()),
            written: AtomicU64::new(0),
            durable: AtomicU64::new(0),
            memory_current_bytes: AtomicU64::new(0),
            memory_peak_bytes: AtomicU64::new(0),
            cpu_workers: AtomicU64::new(0),
            cpu_active: AtomicU64::new(0),
            cpu_queued: AtomicU64::new(0),
            network_pending: AtomicU64::new(0),
            network_inflight: AtomicU64::new(0),
            analysis_inflight: AtomicU64::new(0),
            compression_pending: AtomicU64::new(0),
            compression_inflight: AtomicU64::new(0),
            writer_pending: AtomicU64::new(0),
            writer_inflight: AtomicU64::new(0),
            durability_pending: AtomicU64::new(0),
        }
    }
}

impl Progress {
    /// Starts a new measurable unit of work. Phase metadata and its completed
    /// counter are reset together so reporters cannot pair a new label with a
    /// previous phase's count.
    pub fn begin_phase(&self, phase: WorkPhase, total: Option<u64>) {
        self.begin_phase_with_completed(phase, total, 0);
    }

    /// Starts a phase whose work was already partially completed while an
    /// upstream producer was still open. The completed value is installed as
    /// the EWMA baseline, so historical work is not reported as an
    /// instantaneous rate spike.
    pub fn begin_phase_with_completed(&self, phase: WorkPhase, total: Option<u64>, completed: u64) {
        self.begin_phase_in(PhaseSlot::Primary, phase, total, completed);
    }

    pub fn begin_secondary_phase(&self, phase: WorkPhase, total: Option<u64>) {
        self.begin_phase_in(PhaseSlot::Secondary, phase, total, 0);
    }

    fn begin_phase_in(
        &self,
        slot: PhaseSlot,
        phase: WorkPhase,
        total: Option<u64>,
        completed: u64,
    ) {
        let (state, counter) = match slot {
            PhaseSlot::Primary => (&self.phase, &self.phase_completed),
            PhaseSlot::Secondary => (&self.secondary_phase, &self.secondary_phase_completed),
        };
        let mut state = state.lock();
        counter.store(completed, Ordering::Release);
        state.reset(phase, total, completed, Instant::now());
    }

    pub fn add_phase_completed(&self, delta: u64) {
        self.phase_completed.fetch_add(delta, Ordering::Relaxed);
    }

    pub fn add_secondary_phase_completed(&self, delta: u64) {
        self.secondary_phase_completed
            .fetch_add(delta, Ordering::Relaxed);
    }

    pub fn add_phase_completed_in(&self, slot: PhaseSlot, delta: u64) {
        match slot {
            PhaseSlot::Primary => self.add_phase_completed(delta),
            PhaseSlot::Secondary => self.add_secondary_phase_completed(delta),
        }
    }

    /// Freezes the elapsed time for the current phase without fabricating
    /// completed work. A phase that finishes below its declared total remains
    /// visibly incomplete.
    pub fn finish_phase(&self) {
        self.finish_phase_in(PhaseSlot::Primary);
    }

    pub fn finish_secondary_phase(&self) {
        self.finish_phase_in(PhaseSlot::Secondary);
    }

    fn finish_phase_in(&self, slot: PhaseSlot) {
        let (state, counter) = match slot {
            PhaseSlot::Primary => (&self.phase, &self.phase_completed),
            PhaseSlot::Secondary => (&self.secondary_phase, &self.secondary_phase_completed),
        };
        let timing = {
            let mut state = state.lock();
            let Some(phase) = state.phase else {
                return;
            };
            if state.finished_elapsed.is_some() {
                return;
            }
            let now = Instant::now();
            let completed = counter.load(Ordering::Acquire);
            state.observe(completed, now);
            let elapsed = now.duration_since(state.started);
            state.finished_elapsed = Some(elapsed);
            let started_at = state
                .started
                .checked_duration_since(self.started)
                .unwrap_or_default();
            let timing = PhaseTiming {
                phase,
                slot,
                completed,
                total: state.total,
                started_at_ms: duration_ms(started_at),
                finished_at_ms: duration_ms(now.duration_since(self.started)),
                elapsed_ms: duration_ms(elapsed),
            };
            if slot == PhaseSlot::Secondary {
                state.phase = None;
                state.total = None;
                state.finished_elapsed = None;
                counter.store(0, Ordering::Release);
            }
            timing
        };
        self.phase_history.lock().push(timing);
    }

    pub fn add_total_row_groups(&self, value: u64) {
        self.total_row_groups.fetch_add(value, Ordering::Relaxed);
    }

    pub fn add_completed_row_groups(&self, value: u64) {
        self.completed_row_groups
            .fetch_add(value, Ordering::Relaxed);
    }

    pub fn add_input_rows(&self, value: u64) {
        self.input_rows.fetch_add(value, Ordering::Relaxed);
    }

    pub fn set_entities(&self, logical_nfts: u64, contracts: u64) {
        self.logical_nfts.store(logical_nfts, Ordering::Relaxed);
        self.contracts.store(contracts, Ordering::Relaxed);
    }

    pub fn add_postings(&self, dimension: crate::model::Dimension, value: u64) {
        self.postings_built.fetch_add(value, Ordering::Relaxed);
        match dimension {
            crate::model::Dimension::Name => &self.name_postings_built,
            crate::model::Dimension::TokenUri | crate::model::Dimension::ImageUri => {
                &self.uri_postings_built
            }
            crate::model::Dimension::Metadata => &self.metadata_postings_built,
        }
        .fetch_add(value, Ordering::Relaxed);
    }

    pub fn add_shard_batch(&self) {
        self.shard_batches_completed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_shard_seal(&self) {
        self.shards_sealed.fetch_add(1, Ordering::Relaxed);
    }

    /// Compatibility shim for existing relation-oriented call sites. Those
    /// call sites historically passed the number of relations whose
    /// `incomplete` bit changed, despite this method's old name. New code
    /// should call [`Self::mark_incomplete_seed`] for each failed SeedId and
    /// [`Self::add_incomplete_relations`] for relation counts.
    pub fn add_incomplete_seeds(&self, count: u64) {
        self.add_incomplete_relations(count);
    }

    /// Idempotently records one of the configured seed IDs in a fixed bitmap.
    /// Returns true only when this call marks the seed for the first time.
    pub fn mark_incomplete_seed(&self, seed_id: crate::model::SeedId) -> bool {
        let index = usize::from(seed_id.0);
        let Some(word) = self.incomplete_seed_bitmap.get(index / 64) else {
            return false;
        };
        let mask = 1_u64 << (index % 64);
        word.fetch_or(mask, Ordering::AcqRel) & mask == 0
    }

    pub fn add_incomplete_relations(&self, count: u64) {
        self.incomplete_relations
            .fetch_add(count, Ordering::Relaxed);
    }

    /// Records a candidate that could not be enqueued for network prefetch
    /// because `network_queue_capacity` was already exhausted. The candidate
    /// still receives a full fetch once its relations freeze.
    pub fn add_prefetch_skipped(&self) {
        self.prefetch_skipped.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_candidates(&self, value: u64) {
        self.candidates.store(value, Ordering::Relaxed);
        let previous = self
            .candidates_discovered
            .fetch_max(value, Ordering::AcqRel);
        if previous == 0 && value > 0 {
            self.candidate_rate
                .lock()
                .start(self.written.load(Ordering::Acquire), Instant::now());
        }
        self.discovery_complete.store(true, Ordering::Release);
    }

    pub fn add_candidate_discovered(&self) {
        self.add_candidates_discovered(1);
    }

    pub fn add_candidates_discovered(&self, value: u64) {
        if value == 0 {
            return;
        }
        let previous = self
            .candidates_discovered
            .fetch_add(value, Ordering::AcqRel);
        if previous == 0 {
            self.candidate_rate
                .lock()
                .start(self.written.load(Ordering::Acquire), Instant::now());
        }
    }

    pub fn mark_candidate_discovery_complete(&self) {
        let discovered = self.candidates_discovered.load(Ordering::Acquire);
        self.candidates.store(discovered, Ordering::Release);
        self.discovery_complete.store(true, Ordering::Release);
    }

    pub fn add_fetched(&self, success: bool, truncated: bool) {
        let mut counts = self.terminal_counts.lock();
        if success {
            counts.fetch_succeeded = counts.fetch_succeeded.saturating_add(1);
            if truncated {
                counts.fetch_truncated = counts.fetch_truncated.saturating_add(1);
            }
        } else {
            counts.fetch_failed = counts.fetch_failed.saturating_add(1);
        }
    }

    pub fn add_analyzed(&self, success: bool) {
        let mut counts = self.terminal_counts.lock();
        if success {
            counts.analysis_succeeded = counts.analysis_succeeded.saturating_add(1);
        } else {
            counts.analysis_failed = counts.analysis_failed.saturating_add(1);
        }
    }

    pub fn add_written(&self) {
        self.written.fetch_add(1, Ordering::Relaxed);
        self.durability_pending.fetch_add(1, Ordering::Relaxed);
    }

    /// Marks every successfully written artifact durable after the run-level
    /// durability barrier completes. Artifacts are intentionally not reported
    /// as durable when they are merely renamed into the run directory.
    pub fn mark_all_written_durable(&self) {
        self.durable
            .store(self.written.load(Ordering::Acquire), Ordering::Release);
        self.durability_pending.store(0, Ordering::Release);
    }

    pub fn record_memory(&self, bytes: u64) {
        self.memory_current_bytes.store(bytes, Ordering::Relaxed);
        self.memory_peak_bytes.fetch_max(bytes, Ordering::Relaxed);
    }

    pub fn record_cpu(&self, workers: usize, active: usize, queued: usize) {
        self.cpu_workers.store(workers as u64, Ordering::Relaxed);
        self.cpu_active.store(active as u64, Ordering::Relaxed);
        self.cpu_queued.store(queued as u64, Ordering::Relaxed);
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record_pipeline_queues(
        &self,
        network_pending: usize,
        network_inflight: usize,
        analysis_inflight: usize,
        compression_pending: usize,
        compression_inflight: usize,
        writer_pending: usize,
        writer_inflight: usize,
        _durability_pending: usize,
    ) {
        macro_rules! store {
            ($field:ident, $value:ident) => {
                self.$field.store($value as u64, Ordering::Relaxed);
            };
        }
        store!(network_pending, network_pending);
        store!(network_inflight, network_inflight);
        store!(analysis_inflight, analysis_inflight);
        store!(compression_pending, compression_pending);
        store!(compression_inflight, compression_inflight);
        store!(writer_pending, writer_pending);
        store!(writer_inflight, writer_inflight);
        self.durability_pending.store(
            self.written
                .load(Ordering::Acquire)
                .saturating_sub(self.durable.load(Ordering::Acquire)),
            Ordering::Release,
        );
    }

    pub fn snapshot(&self) -> ProgressSnapshot {
        let now = Instant::now();
        let elapsed_ms = duration_ms(now.duration_since(self.started));
        let phase = snapshot_phase(
            &mut self.phase.lock(),
            self.phase_completed.load(Ordering::Acquire),
            now,
        );
        let secondary_phase = snapshot_phase(
            &mut self.secondary_phase.lock(),
            self.secondary_phase_completed.load(Ordering::Acquire),
            now,
        );
        let completed_phases = self.phase_history.lock().clone();

        let total_row_groups = self.total_row_groups.load(Ordering::Acquire);
        let completed_row_groups = self.completed_row_groups.load(Ordering::Acquire);
        let discovery_complete = self.discovery_complete.load(Ordering::Acquire);
        let candidates = self.candidates.load(Ordering::Acquire);
        let candidates_discovered = self.candidates_discovered.load(Ordering::Acquire);
        let written = self.written.load(Ordering::Acquire);
        let durable = self.durable.load(Ordering::Acquire);
        let mut candidate_rate = self.candidate_rate.lock();
        candidate_rate.observe(written, now);
        let candidate_rate_per_sec = candidate_rate.rate.rate;
        let (candidate_eta_ms, candidate_eta_confident) = if discovery_complete {
            if written >= candidates {
                (Some(0), candidate_rate.rate.confident())
            } else {
                (
                    candidate_rate
                        .rate
                        .eta_ms(candidates.saturating_sub(written)),
                    candidate_rate.rate.confident(),
                )
            }
        } else {
            (None, false)
        };
        drop(candidate_rate);

        let terminal_counts = *self.terminal_counts.lock();
        let fetch_succeeded = terminal_counts.fetch_succeeded;
        let fetch_failed = terminal_counts.fetch_failed;
        let analysis_succeeded = terminal_counts.analysis_succeeded;
        let analysis_failed = terminal_counts.analysis_failed;
        let cpu_workers = self.cpu_workers.load(Ordering::Acquire);
        let cpu_active = self.cpu_active.load(Ordering::Acquire);
        let seeds_incomplete = self
            .incomplete_seed_bitmap
            .iter()
            .fold(0_u64, |total, word| {
                total + u64::from(word.load(Ordering::Acquire).count_ones())
            });
        ProgressSnapshot {
            elapsed_ms,
            phase: phase.phase,
            phase_completed: phase.completed,
            phase_total: phase.total,
            phase_elapsed_ms: phase.elapsed_ms,
            phase_rate_per_sec: phase.rate_per_sec,
            phase_eta_ms: phase.eta_ms,
            phase_eta_confident: phase.eta_confident,
            phase_stalled: phase.stalled,
            secondary_phase: secondary_phase.phase,
            secondary_phase_completed: secondary_phase.completed,
            secondary_phase_total: secondary_phase.total,
            secondary_phase_elapsed_ms: secondary_phase.elapsed_ms,
            secondary_phase_rate_per_sec: secondary_phase.rate_per_sec,
            secondary_phase_eta_ms: secondary_phase.eta_ms,
            secondary_phase_eta_confident: secondary_phase.eta_confident,
            secondary_phase_stalled: secondary_phase.stalled,
            completed_phases,
            total_row_groups,
            completed_row_groups,
            row_group_eta_ms: eta_ms(elapsed_ms, completed_row_groups, total_row_groups),
            input_rows: self.input_rows.load(Ordering::Relaxed),
            logical_nfts: self.logical_nfts.load(Ordering::Relaxed),
            contracts: self.contracts.load(Ordering::Relaxed),
            postings_built: self.postings_built.load(Ordering::Relaxed),
            uri_postings_built: self.uri_postings_built.load(Ordering::Relaxed),
            name_postings_built: self.name_postings_built.load(Ordering::Relaxed),
            metadata_postings_built: self.metadata_postings_built.load(Ordering::Relaxed),
            shard_batches_completed: self.shard_batches_completed.load(Ordering::Relaxed),
            shards_sealed: self.shards_sealed.load(Ordering::Relaxed),
            seeds_incomplete,
            incomplete_relations: self.incomplete_relations.load(Ordering::Acquire),
            prefetch_skipped: self.prefetch_skipped.load(Ordering::Relaxed),
            candidates,
            candidates_discovered,
            discovery_complete,
            candidate_rate_per_sec,
            candidate_eta_ms,
            candidate_eta_confident,
            fetched: fetch_succeeded.saturating_add(fetch_failed),
            fetch_succeeded,
            fetch_failed,
            fetch_truncated: terminal_counts.fetch_truncated,
            analyzed: analysis_succeeded.saturating_add(analysis_failed),
            analysis_succeeded,
            analysis_failed,
            written,
            durable,
            // Kept for API compatibility. This now denotes candidate write ETA;
            // durability remains a separate all-or-nothing phase.
            durable_eta_ms: candidate_eta_ms,
            memory_current_bytes: self.memory_current_bytes.load(Ordering::Relaxed),
            memory_peak_bytes: self.memory_peak_bytes.load(Ordering::Relaxed),
            cpu_workers,
            cpu_active,
            cpu_idle: cpu_workers.saturating_sub(cpu_active),
            cpu_queued: self.cpu_queued.load(Ordering::Relaxed),
            network_pending: self.network_pending.load(Ordering::Relaxed),
            network_inflight: self.network_inflight.load(Ordering::Relaxed),
            analysis_inflight: self.analysis_inflight.load(Ordering::Relaxed),
            compression_pending: self.compression_pending.load(Ordering::Relaxed),
            compression_inflight: self.compression_inflight.load(Ordering::Relaxed),
            writer_pending: self.writer_pending.load(Ordering::Relaxed),
            writer_inflight: self.writer_inflight.load(Ordering::Relaxed),
            durability_pending: written.saturating_sub(durable),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct PhaseMetrics {
    phase: Option<WorkPhase>,
    completed: u64,
    total: Option<u64>,
    elapsed_ms: u64,
    rate_per_sec: Option<f64>,
    eta_ms: Option<u64>,
    eta_confident: bool,
    stalled: bool,
}

fn snapshot_phase(state: &mut PhaseState, completed: u64, now: Instant) -> PhaseMetrics {
    if state.phase.is_none() {
        return PhaseMetrics {
            phase: None,
            completed: 0,
            total: None,
            elapsed_ms: 0,
            rate_per_sec: None,
            eta_ms: None,
            eta_confident: false,
            stalled: false,
        };
    }
    state.observe(completed, now);
    let finished = state.finished_elapsed.is_some();
    let homogeneous = state.phase != Some(WorkPhase::UriQueryNameIndex);
    let stalled = !finished
        && state.rate.rate.is_some()
        && now.duration_since(state.last_progress) >= STALE_RATE_AFTER
        && state.total.is_none_or(|total| completed < total);
    let (eta_ms, eta_confident) = match state.total {
        Some(total) if completed >= total => (Some(0), state.rate.confident() && homogeneous),
        Some(total) if !finished && !stalled => (
            state.rate.eta_ms(total.saturating_sub(completed)),
            state.rate.confident() && homogeneous,
        ),
        _ => (None, false),
    };
    PhaseMetrics {
        phase: state.phase,
        completed,
        total: state.total,
        elapsed_ms: duration_ms(state.elapsed(now)),
        rate_per_sec: (!stalled).then_some(state.rate.rate).flatten(),
        eta_ms,
        eta_confident,
        stalled,
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ProgressSnapshot {
    pub elapsed_ms: u64,
    pub phase: Option<WorkPhase>,
    pub phase_completed: u64,
    pub phase_total: Option<u64>,
    pub phase_elapsed_ms: u64,
    pub phase_rate_per_sec: Option<f64>,
    pub phase_eta_ms: Option<u64>,
    pub phase_eta_confident: bool,
    pub phase_stalled: bool,
    pub secondary_phase: Option<WorkPhase>,
    pub secondary_phase_completed: u64,
    pub secondary_phase_total: Option<u64>,
    pub secondary_phase_elapsed_ms: u64,
    pub secondary_phase_rate_per_sec: Option<f64>,
    pub secondary_phase_eta_ms: Option<u64>,
    pub secondary_phase_eta_confident: bool,
    pub secondary_phase_stalled: bool,
    pub completed_phases: Vec<PhaseTiming>,
    pub total_row_groups: u64,
    pub completed_row_groups: u64,
    pub row_group_eta_ms: Option<u64>,
    pub input_rows: u64,
    pub logical_nfts: u64,
    pub contracts: u64,
    pub postings_built: u64,
    pub uri_postings_built: u64,
    pub name_postings_built: u64,
    pub metadata_postings_built: u64,
    pub shard_batches_completed: u64,
    pub shards_sealed: u64,
    pub seeds_incomplete: u64,
    pub incomplete_relations: u64,
    pub prefetch_skipped: u64,
    pub candidates: u64,
    pub candidates_discovered: u64,
    pub discovery_complete: bool,
    pub candidate_rate_per_sec: Option<f64>,
    pub candidate_eta_ms: Option<u64>,
    pub candidate_eta_confident: bool,
    pub fetched: u64,
    pub fetch_succeeded: u64,
    pub fetch_failed: u64,
    pub fetch_truncated: u64,
    pub analyzed: u64,
    pub analysis_succeeded: u64,
    pub analysis_failed: u64,
    pub written: u64,
    pub durable: u64,
    pub durable_eta_ms: Option<u64>,
    pub memory_current_bytes: u64,
    pub memory_peak_bytes: u64,
    pub cpu_workers: u64,
    pub cpu_active: u64,
    pub cpu_idle: u64,
    pub cpu_queued: u64,
    pub network_pending: u64,
    pub network_inflight: u64,
    pub analysis_inflight: u64,
    pub compression_pending: u64,
    pub compression_inflight: u64,
    pub writer_pending: u64,
    pub writer_inflight: u64,
    pub durability_pending: u64,
}

impl ProgressSnapshot {
    pub fn human_line(&self) -> String {
        let elapsed = format_duration(self.elapsed_ms);
        let stage = if let Some(phase) = self.phase {
            let progress = self.phase_total.map_or_else(
                || format!("{} done", grouped(self.phase_completed)),
                |total| {
                    format!(
                        "{}/{} ({})",
                        grouped(self.phase_completed),
                        grouped(total),
                        percentage(self.phase_completed, total)
                    )
                },
            );
            format!(
                "{} {} | 速率 {} | 阶段耗时 {} | 阶段 ETA {}",
                phase.label(),
                progress,
                format_rate(self.phase_rate_per_sec),
                format_duration(self.phase_elapsed_ms),
                if self.phase_stalled {
                    "等待完成/背压".to_owned()
                } else {
                    format_eta(self.phase_eta_ms, self.phase_eta_confident)
                },
            )
        } else if self.logical_nfts == 0 {
            format!(
                "加载 Parquet {}/{} ({}) | 已读 {} 行 | ETA {}",
                grouped(self.completed_row_groups),
                grouped(self.total_row_groups),
                percentage(self.completed_row_groups, self.total_row_groups),
                grouped(self.input_rows),
                format_optional_duration(self.row_group_eta_ms),
            )
        } else {
            format!(
                "构建查重索引 | NFT {} | 合约 {} | postings URI={} Name={} Metadata={} | 分片批次 {} 封存 {}",
                grouped(self.logical_nfts),
                grouped(self.contracts),
                grouped(self.uri_postings_built),
                grouped(self.name_postings_built),
                grouped(self.metadata_postings_built),
                grouped(self.shard_batches_completed),
                grouped(self.shards_sealed),
            )
        };

        let secondary = self.secondary_phase.map_or_else(String::new, |phase| {
            let progress = self.secondary_phase_total.map_or_else(
                || format!("{} done", grouped(self.secondary_phase_completed)),
                |total| {
                    format!(
                        "{}/{} ({})",
                        grouped(self.secondary_phase_completed),
                        grouped(total),
                        percentage(self.secondary_phase_completed, total)
                    )
                },
            );
            format!(
                " | 并行 {} {} | 速率 {} | 耗时 {} | ETA {}",
                phase.label(),
                progress,
                format_rate(self.secondary_phase_rate_per_sec),
                format_duration(self.secondary_phase_elapsed_ms),
                if self.secondary_phase_stalled {
                    "等待完成/背压".to_owned()
                } else {
                    format_eta(
                        self.secondary_phase_eta_ms,
                        self.secondary_phase_eta_confident,
                    )
                },
            )
        });

        let recently_completed = self
            .completed_phases
            .last()
            .map_or_else(String::new, |phase| {
                format!(
                    " | 最近完成 {}={} ({}/{})",
                    phase.phase.label(),
                    format_duration(phase.elapsed_ms),
                    grouped(phase.completed),
                    phase.total.map_or_else(|| "--".to_owned(), grouped)
                )
            });

        let pipeline_started = self.candidates_discovered > 0
            || self.discovery_complete
            || self.fetched > 0
            || self.analyzed > 0
            || self.written > 0
            || self.network_pending > 0
            || self.network_inflight > 0
            || self.analysis_inflight > 0
            || self.compression_pending > 0
            || self.compression_inflight > 0
            || self.writer_pending > 0
            || self.writer_inflight > 0
            || self.durability_pending > 0;
        let pipeline = if pipeline_started {
            let total = if self.discovery_complete {
                grouped(self.candidates)
            } else {
                "开放".to_owned()
            };
            format!(
                " | 候选 发现 {} 总量 {} | 获取 {} (成功 {} / 失败 {} / 截断 {}) | 分析 {} (成功 {} / 失败 {}) | 写入 {}/{} / 持久化 {} (待屏障 {}) | 队列 网络={}/{} 分析={} 压缩={}/{} 写入={}/{} | 候选速率 {} | 候选 ETA {}",
                grouped(self.candidates_discovered),
                total,
                grouped(self.fetched),
                grouped(self.fetch_succeeded),
                grouped(self.fetch_failed),
                grouped(self.fetch_truncated),
                grouped(self.analyzed),
                grouped(self.analysis_succeeded),
                grouped(self.analysis_failed),
                grouped(self.written),
                total,
                grouped(self.durable),
                grouped(self.durability_pending),
                grouped(self.network_pending),
                grouped(self.network_inflight),
                grouped(self.analysis_inflight),
                grouped(self.compression_pending),
                grouped(self.compression_inflight),
                grouped(self.writer_pending),
                grouped(self.writer_inflight),
                format_rate(self.candidate_rate_per_sec),
                format_eta(self.candidate_eta_ms, self.candidate_eta_confident),
            )
        } else {
            String::new()
        };

        format!(
            "[{elapsed}] {stage}{secondary}{recently_completed}{pipeline} | incomplete seeds={} relations={} | CPU调度 {}/{} (排队 {}) | 内存 {} (峰值 {})",
            grouped(self.seeds_incomplete),
            grouped(self.incomplete_relations),
            grouped(self.cpu_active),
            grouped(self.cpu_workers),
            grouped(self.cpu_queued),
            format_bytes(self.memory_current_bytes),
            format_bytes(self.memory_peak_bytes),
        )
    }

    pub fn phase_history_line(&self) -> Option<String> {
        (!self.completed_phases.is_empty()).then(|| {
            let timings = self
                .completed_phases
                .iter()
                .map(|timing| {
                    let parallel = if timing.slot == PhaseSlot::Secondary {
                        "(并行)"
                    } else {
                        ""
                    };
                    format!(
                        "{}{}={} [{}-{}]",
                        timing.phase.label(),
                        parallel,
                        format_duration(timing.elapsed_ms),
                        format_duration(timing.started_at_ms),
                        format_duration(timing.finished_at_ms),
                    )
                })
                .collect::<Vec<_>>()
                .join("; ");
            format!("阶段耗时: {timings}")
        })
    }
}

fn grouped(value: u64) -> String {
    let raw = value.to_string();
    let mut output = String::with_capacity(raw.len() + raw.len() / 3);
    for (index, character) in raw.chars().enumerate() {
        if index > 0 && (raw.len() - index).is_multiple_of(3) {
            output.push(',');
        }
        output.push(character);
    }
    output
}

fn percentage(completed: u64, total: u64) -> String {
    if total == 0 {
        return "--".into();
    }
    format!("{:.1}%", completed as f64 * 100.0 / total as f64)
}

fn format_optional_duration(milliseconds: Option<u64>) -> String {
    milliseconds.map_or_else(|| "--".into(), format_duration)
}

fn format_rate(rate: Option<f64>) -> String {
    rate.filter(|value| value.is_finite() && *value >= 0.0)
        .map_or_else(|| "--/s".into(), |value| format!("{value:.1}/s"))
}

fn format_eta(milliseconds: Option<u64>, confident: bool) -> String {
    milliseconds.map_or_else(
        || "--".into(),
        |value| {
            let formatted = format_duration(value);
            if confident || value == 0 {
                formatted
            } else {
                format!("~{formatted}")
            }
        },
    )
}

fn format_duration(milliseconds: u64) -> String {
    if milliseconds < 60_000 {
        return format!("{:.1}秒", milliseconds as f64 / 1_000.0);
    }
    let total_seconds = milliseconds / 1_000;
    let hours = total_seconds / 3_600;
    let minutes = total_seconds % 3_600 / 60;
    let seconds = total_seconds % 60;
    if hours == 0 {
        format!("{minutes:02}:{seconds:02}")
    } else {
        format!("{hours:02}:{minutes:02}:{seconds:02}")
    }
}

fn format_bytes(bytes: u64) -> String {
    if bytes == 0 {
        return "--".into();
    }
    const GIB: f64 = (1024_u64 * 1024 * 1024) as f64;
    format!("{:.1} GiB", bytes as f64 / GIB)
}

fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

fn duration_ms_from_rate(remaining: u64, rate: f64) -> Option<u64> {
    if !rate.is_finite() || rate <= 0.0 {
        return None;
    }
    let milliseconds = remaining as f64 * 1_000.0 / rate;
    if !milliseconds.is_finite() || milliseconds < 0.0 {
        return None;
    }
    Some(milliseconds.min(u64::MAX as f64).round() as u64)
}

fn eta_ms(elapsed_ms: u64, completed: u64, total: u64) -> Option<u64> {
    if completed == 0 || total <= completed {
        return None;
    }
    Some(
        (elapsed_ms as u128)
            .saturating_mul(u128::from(total - completed))
            .checked_div(u128::from(completed))
            .unwrap_or(u128::MAX)
            .min(u128::from(u64::MAX)) as u64,
    )
}

#[cfg(test)]
mod tests {
    use super::{
        snapshot_phase, Duration, EwmaRate, Instant, PhaseSlot, PhaseState, Progress, WorkPhase,
    };

    #[test]
    fn renamed_artifacts_are_not_durable_before_the_final_barrier() {
        let progress = Progress::default();
        progress.add_written();
        let before = progress.snapshot();
        assert_eq!(before.written, 1);
        assert_eq!(before.durable, 0);
        assert_eq!(before.durability_pending, 1);

        progress.mark_all_written_durable();
        let after = progress.snapshot();
        assert_eq!(after.durable, 1);
        assert_eq!(after.durability_pending, 0);
    }

    #[test]
    fn parquet_progress_is_human_readable() {
        let progress = Progress::default();
        progress.add_total_row_groups(127);
        progress.add_completed_row_groups(118);
        progress.add_input_rows(122_758_830);

        let line = progress.snapshot().human_line();
        assert!(line.contains("加载 Parquet 118/127 (92.9%)"));
        assert!(line.contains("已读 122,758,830 行"));
        assert!(!line.starts_with('{'));
    }

    #[test]
    fn work_phase_serializes_with_stable_snake_case_names() {
        assert_eq!(
            serde_json::to_string(&WorkPhase::MetadataBm25).unwrap(),
            r#""metadata_bm25""#
        );
    }

    #[test]
    fn phase_progress_keeps_its_own_total_and_elapsed_time() {
        let progress = Progress::default();
        progress.begin_phase(WorkPhase::NameQuery, Some(128));
        progress.add_phase_completed(32);

        let active = progress.snapshot();
        assert_eq!(active.phase, Some(WorkPhase::NameQuery));
        assert_eq!(active.phase_completed, 32);
        assert_eq!(active.phase_total, Some(128));
        assert!(!active.phase_eta_confident);

        progress.finish_phase();
        let finished = progress.snapshot();
        assert_eq!(finished.phase_completed, 32);
        assert_eq!(finished.phase_eta_ms, None);
    }

    #[test]
    fn completed_phase_timings_survive_later_phases() {
        let progress = Progress::default();
        progress.begin_phase(WorkPhase::LoadValidate, Some(2));
        progress.add_phase_completed(2);
        progress.finish_phase();
        progress.begin_phase(WorkPhase::BaseScan, Some(4));

        let snapshot = progress.snapshot();
        assert_eq!(snapshot.phase, Some(WorkPhase::BaseScan));
        assert_eq!(snapshot.completed_phases.len(), 1);
        let timing = &snapshot.completed_phases[0];
        assert_eq!(timing.phase, WorkPhase::LoadValidate);
        assert_eq!(timing.slot, PhaseSlot::Primary);
        assert_eq!(timing.completed, 2);
        assert_eq!(timing.total, Some(2));
        assert!(snapshot.phase_history_line().unwrap().contains("校验输入"));
    }

    #[test]
    fn overlapping_phases_keep_independent_units() {
        let progress = Progress::default();
        progress.begin_phase(WorkPhase::UriQuery, Some(8));
        progress.begin_secondary_phase(WorkPhase::NameIndex, Some(100));
        progress.add_phase_completed(1);
        progress.add_secondary_phase_completed(25);

        let snapshot = progress.snapshot();
        assert_eq!(snapshot.phase, Some(WorkPhase::UriQuery));
        assert_eq!(snapshot.phase_completed, 1);
        assert_eq!(snapshot.phase_total, Some(8));
        assert_eq!(snapshot.secondary_phase, Some(WorkPhase::NameIndex));
        assert_eq!(snapshot.secondary_phase_completed, 25);
        assert_eq!(snapshot.secondary_phase_total, Some(100));
        assert!(snapshot.human_line().contains("并行 构建 Name 索引 25/100"));
    }

    #[test]
    fn historical_candidate_work_is_the_phase_rate_baseline() {
        let progress = Progress::default();
        progress.begin_phase_with_completed(WorkPhase::CandidatePipeline, Some(20), 10);

        let snapshot = progress.snapshot();
        assert_eq!(snapshot.phase_completed, 10);
        assert_eq!(snapshot.phase_rate_per_sec, None);
        assert_eq!(progress.phase.lock().last_completed, 10);
    }

    #[test]
    fn ewma_requires_three_positive_samples_for_confident_eta() {
        let mut rate = EwmaRate::new(0.25);
        rate.observe(100.0);
        rate.observe(100.0);
        assert!(!rate.confident());
        rate.observe(100.0);
        assert!(rate.confident());
        assert_eq!(rate.eta_ms(200), Some(2_000));
    }

    #[test]
    fn stalled_phase_drops_stale_rate_and_misleading_eta() {
        let started = Instant::now();
        let mut state = PhaseState::new(started);
        state.reset(WorkPhase::MetadataBm25, Some(256), 0, started);
        state.observe(204, started + Duration::from_secs(1));

        let metrics = snapshot_phase(&mut state, 204, started + Duration::from_secs(32));
        assert!(metrics.stalled);
        assert_eq!(metrics.rate_per_sec, None);
        assert_eq!(metrics.eta_ms, None);
        assert!(!metrics.eta_confident);
    }

    #[test]
    fn incomplete_seed_bitmap_is_idempotent_and_separate_from_relations() {
        let progress = Progress::default();
        assert!(progress.mark_incomplete_seed(crate::model::SeedId(3)));
        assert!(!progress.mark_incomplete_seed(crate::model::SeedId(3)));
        assert!(progress.mark_incomplete_seed(crate::model::SeedId(65)));
        assert!(progress.mark_incomplete_seed(crate::model::SeedId(1_000)));
        progress.add_incomplete_relations(7);

        let snapshot = progress.snapshot();
        assert_eq!(snapshot.seeds_incomplete, 3);
        assert_eq!(snapshot.incomplete_relations, 7);
    }

    #[test]
    fn classified_terminal_counters_define_consistent_totals() {
        let progress = Progress::default();
        progress.add_fetched(true, false);
        progress.add_fetched(true, true);
        progress.add_fetched(false, false);
        progress.add_analyzed(true);
        progress.add_analyzed(false);

        let snapshot = progress.snapshot();
        assert_eq!(snapshot.fetched, 3);
        assert_eq!(
            snapshot.fetched,
            snapshot.fetch_succeeded + snapshot.fetch_failed
        );
        assert_eq!(snapshot.fetch_truncated, 1);
        assert_eq!(snapshot.analyzed, 2);
        assert_eq!(
            snapshot.analyzed,
            snapshot.analysis_succeeded + snapshot.analysis_failed
        );
    }

    #[test]
    fn candidate_discovery_is_open_until_explicitly_sealed() {
        let progress = Progress::default();
        progress.add_candidates_discovered(4);
        let open = progress.snapshot();
        assert_eq!(open.candidates_discovered, 4);
        assert!(!open.discovery_complete);
        assert_eq!(open.candidate_eta_ms, None);

        progress.mark_candidate_discovery_complete();
        let sealed = progress.snapshot();
        assert!(sealed.discovery_complete);
        assert_eq!(sealed.candidates, 4);
    }

    #[test]
    fn candidate_eta_uses_written_terminal_work_not_durability() {
        let progress = Progress::default();
        progress.add_candidates_discovered(10);
        progress.add_written();
        progress.mark_candidate_discovery_complete();
        {
            let mut rate = progress.candidate_rate.lock();
            rate.rate.observe(1.0);
            rate.rate.observe(1.0);
            rate.rate.observe(1.0);
            rate.last_written = 1;
        }

        let snapshot = progress.snapshot();
        assert_eq!(snapshot.written, 1);
        assert_eq!(snapshot.durable, 0);
        assert_eq!(snapshot.candidate_eta_ms, Some(9_000));
        assert!(snapshot.candidate_eta_confident);
        assert_eq!(snapshot.durable_eta_ms, snapshot.candidate_eta_ms);
    }

    #[test]
    fn human_line_shows_phase_and_overlapped_candidate_pipeline() {
        let progress = Progress::default();
        progress.begin_phase(WorkPhase::MetadataBm25, Some(10));
        progress.add_phase_completed(5);
        progress.add_candidates_discovered(2);
        progress.add_fetched(true, false);

        let line = progress.snapshot().human_line();
        assert!(line.contains("Metadata BM25 5/10 (50.0%)"));
        assert!(line.contains("候选 发现 2 总量 开放"));
        assert!(line.contains("获取 1"));
    }
}
