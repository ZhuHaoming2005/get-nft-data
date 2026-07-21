use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

#[derive(Debug)]
pub struct Progress {
    started: Instant,
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
    seeds_incomplete: AtomicU64,
    prefetch_skipped: AtomicU64,
    candidates: AtomicU64,
    fetched: AtomicU64,
    fetch_succeeded: AtomicU64,
    fetch_failed: AtomicU64,
    fetch_truncated: AtomicU64,
    analyzed: AtomicU64,
    analysis_succeeded: AtomicU64,
    analysis_failed: AtomicU64,
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
        Self {
            started: Instant::now(),
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
            seeds_incomplete: AtomicU64::new(0),
            prefetch_skipped: AtomicU64::new(0),
            candidates: AtomicU64::new(0),
            fetched: AtomicU64::new(0),
            fetch_succeeded: AtomicU64::new(0),
            fetch_failed: AtomicU64::new(0),
            fetch_truncated: AtomicU64::new(0),
            analyzed: AtomicU64::new(0),
            analysis_succeeded: AtomicU64::new(0),
            analysis_failed: AtomicU64::new(0),
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

    /// Records seeds marked `incomplete` because they fell in a dimension
    /// shard's `failed_seed_bitmap` at seal time (soft-fail; see
    /// REWRITE_ARCHITECTURE §7.6/§8.4). Does not abort the run.
    pub fn add_incomplete_seeds(&self, count: u64) {
        self.seeds_incomplete.fetch_add(count, Ordering::Relaxed);
    }

    /// Records a candidate that could not be enqueued for network prefetch
    /// because `network_queue_capacity` was already exhausted. The candidate
    /// still receives a full fetch once its relations freeze.
    pub fn add_prefetch_skipped(&self) {
        self.prefetch_skipped.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_candidates(&self, value: u64) {
        self.candidates.store(value, Ordering::Relaxed);
    }

    pub fn add_fetched(&self, success: bool, truncated: bool) {
        self.fetched.fetch_add(1, Ordering::Relaxed);
        if success {
            self.fetch_succeeded.fetch_add(1, Ordering::Relaxed);
            if truncated {
                self.fetch_truncated.fetch_add(1, Ordering::Relaxed);
            }
        } else {
            self.fetch_failed.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn add_analyzed(&self, success: bool) {
        self.analyzed.fetch_add(1, Ordering::Relaxed);
        if success {
            self.analysis_succeeded.fetch_add(1, Ordering::Relaxed);
        } else {
            self.analysis_failed.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn add_written(&self) {
        self.written.fetch_add(1, Ordering::Relaxed);
    }

    /// Marks every successfully written artifact durable after the run-level
    /// durability barrier completes. Artifacts are intentionally not reported
    /// as durable when they are merely renamed into the run directory.
    pub fn mark_all_written_durable(&self) {
        self.durable
            .store(self.written.load(Ordering::Relaxed), Ordering::Relaxed);
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
        durability_pending: usize,
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
        store!(durability_pending, durability_pending);
    }

    pub fn snapshot(&self) -> ProgressSnapshot {
        let elapsed_ms = self.started.elapsed().as_millis() as u64;
        let total_row_groups = self.total_row_groups.load(Ordering::Relaxed);
        let completed_row_groups = self.completed_row_groups.load(Ordering::Relaxed);
        let candidates = self.candidates.load(Ordering::Relaxed);
        let durable = self.durable.load(Ordering::Relaxed);
        ProgressSnapshot {
            elapsed_ms,
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
            seeds_incomplete: self.seeds_incomplete.load(Ordering::Relaxed),
            prefetch_skipped: self.prefetch_skipped.load(Ordering::Relaxed),
            candidates,
            fetched: self.fetched.load(Ordering::Relaxed),
            fetch_succeeded: self.fetch_succeeded.load(Ordering::Relaxed),
            fetch_failed: self.fetch_failed.load(Ordering::Relaxed),
            fetch_truncated: self.fetch_truncated.load(Ordering::Relaxed),
            analyzed: self.analyzed.load(Ordering::Relaxed),
            analysis_succeeded: self.analysis_succeeded.load(Ordering::Relaxed),
            analysis_failed: self.analysis_failed.load(Ordering::Relaxed),
            written: self.written.load(Ordering::Relaxed),
            durable,
            durable_eta_ms: eta_ms(elapsed_ms, durable, candidates),
            memory_current_bytes: self.memory_current_bytes.load(Ordering::Relaxed),
            memory_peak_bytes: self.memory_peak_bytes.load(Ordering::Relaxed),
            cpu_workers: self.cpu_workers.load(Ordering::Relaxed),
            cpu_active: self.cpu_active.load(Ordering::Relaxed),
            cpu_idle: self
                .cpu_workers
                .load(Ordering::Relaxed)
                .saturating_sub(self.cpu_active.load(Ordering::Relaxed)),
            cpu_queued: self.cpu_queued.load(Ordering::Relaxed),
            network_pending: self.network_pending.load(Ordering::Relaxed),
            network_inflight: self.network_inflight.load(Ordering::Relaxed),
            analysis_inflight: self.analysis_inflight.load(Ordering::Relaxed),
            compression_pending: self.compression_pending.load(Ordering::Relaxed),
            compression_inflight: self.compression_inflight.load(Ordering::Relaxed),
            writer_pending: self.writer_pending.load(Ordering::Relaxed),
            writer_inflight: self.writer_inflight.load(Ordering::Relaxed),
            durability_pending: self.durability_pending.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ProgressSnapshot {
    pub elapsed_ms: u64,
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
    pub prefetch_skipped: u64,
    pub candidates: u64,
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
    use super::Progress;

    #[test]
    fn renamed_artifacts_are_not_durable_before_the_final_barrier() {
        let progress = Progress::default();
        progress.add_written();
        assert_eq!(progress.snapshot().written, 1);
        assert_eq!(progress.snapshot().durable, 0);

        progress.mark_all_written_durable();
        assert_eq!(progress.snapshot().durable, 1);
    }
}
