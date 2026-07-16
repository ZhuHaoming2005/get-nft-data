//! Snapshot-only production metadata pipeline. No DuckDB or payload API is reachable.

use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use thiserror::Error;

use crate::blocking::{
    build_base_equivalent_atom_sketches_from_feature_view_parallel, LocalRoutingPlan,
};
use crate::cascade::{score_pair, PairScoreDecision};
use crate::evidence::{
    evaluate_holdout, EvidenceError, EvidenceGatePolicy, EvidenceGateReport, HoldoutEvidence,
    RescuePlan, EVIDENCE_GATE_REVISION,
};
use crate::exact_islands::{
    open_pair_exact_evidence, open_shared_token_exact_evidence, plan_exact_evidence,
    plan_shared_token_evidence, run_pair_exact_island_with_progress,
    run_shared_token_exact_islands_with_progress, ExactEvidenceBudget, PairExactEvidence,
    SharedTokenExactEvidence,
};
use crate::index::{
    max_hot_block_candidate_index_bytes, max_hot_block_parallel_row_bytes, ConservativeIndex,
    IndexMetrics,
};
use crate::progress::{
    ProgressCounters, ProgressEvent, ProgressPhase, TotalKind, WorkClass, WorkUnit,
};
use crate::reduce::{
    commit_component_roots, open_component_snapshot_chain, recover_component_snapshots,
    reduce_components_with_progress, ComponentSnapshotIdentity, Edge, EdgeBudget, EdgeCollector,
    ForestRun,
};
use crate::resource::{MemoryBroker, MemoryLease};
use crate::scheduler::{job_routing_pair_work, JobShape, RecallPlan, UniverseBudget, WorkCatalog};
use crate::snapshot::{MetadataSnapshot, SnapshotError};
use crate::storage::{
    ArtifactClass, ArtifactRegistration, EvictionPlan, StorageBroker, StorageLease,
    StorageLedgerError,
};

pub const DEFAULT_MAX_CANDIDATE_PAIR_VISITS: u64 = 200_000_000_000;
pub const DEFAULT_EXACT_SAMPLE_LEFTS: u64 = 1_024;
pub const DEFAULT_EXACT_PAIR_WORK: u64 = 20_000_000_000;

// Exact evidence is a resident statistical data set, not a connectivity
// forest.  Keep its admission independent from `edge_bytes`: tying the two
// together made small contract forests impose only a few MiB on evidence even
// when hundreds of GiB were available to Match.
const MAX_EVIDENCE_RESIDENT_BYTES: u64 = 8 * 1024 * 1024 * 1024;

const CONNECTIVITY_RUN_REVISION: u32 = 5;
const MAX_RESCUE_PAYLOAD_CACHE_ENTRIES: usize = 65_536;
const RESCUE_PAYLOAD_CACHE_ENTRY_BYTES: u64 = 32;
const RESCUE_SEED_INDEX_ENTRY_BYTES: u64 = 64;
const SHARED_LOCAL_ROUTING_MIN_MEMBERS: usize = 256;

#[derive(Debug, Clone)]
pub struct MetadataPipelineConfig {
    pub storage_work_directory: PathBuf,
    pub memory_hard_top: u64,
    pub host_total_memory: u64,
    pub threads: usize,
    pub max_catalog_jobs: u64,
    pub max_candidate_pair_visits: u64,
    pub exact_sample_lefts: u64,
    pub exact_pair_work: u64,
    pub evidence_gate_policy: EvidenceGatePolicy,
    pub edge_bytes: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataPipelineResult {
    pub schema_revision: u32,
    pub evidence_gate_revision: u32,
    pub snapshot_fingerprint: String,
    pub snapshot_atoms: u64,
    pub index_metrics: SerializableIndexMetrics,
    pub exact_evidence: PairExactEvidence,
    pub pair_holdout_evidence: PairExactEvidence,
    pub shared_token_exact_evidence: SharedTokenExactEvidence,
    pub skipped_shared_token_evidence_groups: Vec<u32>,
    pub rescue_plan: RescuePlan,
    pub evidence_gate_report: EvidenceGateReport,
    pub planned_candidate_pair_visits: u64,
    pub evidence_holdout_misses: u64,
    pub effective_edge_budget_bytes: u64,
    pub edge_count: u64,
    pub scope_components: ScopeComponents,
    pub summary_rows: Vec<MetadataSummaryRow>,
    pub wall_millis: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ConnectivityRunManifest {
    revision: u32,
    schema_revision: u32,
    snapshot_fingerprint: String,
    connectivity_plan_digest: String,
    chain_count: usize,
    intra_runs: u32,
    cross_runs: u32,
    pair_runs: Vec<u32>,
    index_metrics: SerializableIndexMetrics,
    candidate_pair_visits: u64,
    accepted_edge_count: u64,
}

struct RecoveredConnectivity {
    intra: Vec<ForestRun>,
    cross: Vec<ForestRun>,
    pairs: Vec<Vec<ForestRun>>,
    index_metrics: SerializableIndexMetrics,
    candidate_pair_visits: u64,
    accepted_edge_count: u64,
}

struct ConnectivityCommit<'a> {
    snapshot_fingerprint: &'a str,
    connectivity_plan_digest: &'a str,
    chain_count: usize,
    intra: &'a [ForestRun],
    cross: &'a [ForestRun],
    pairs: &'a [Vec<ForestRun>],
    index_metrics: &'a SerializableIndexMetrics,
    candidate_pair_visits: u64,
    accepted_edge_count: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScopeComponents {
    pub intra_roots: Vec<u32>,
    pub cross_roots: Vec<u32>,
    pub chain_pair_roots: Vec<ChainPairRoots>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChainPairRoots {
    pub left_chain: u32,
    pub right_chain: u32,
    pub roots: Vec<u32>,
}

enum ComponentScopeKind {
    Intra,
    Cross,
    Pair { left: u32, right: u32 },
}

impl ComponentScopeKind {
    fn identity(&self) -> String {
        match self {
            Self::Intra => "intra".into(),
            Self::Cross => "cross".into(),
            Self::Pair { left, right } => format!("pair:{left}:{right}"),
        }
    }

    fn directory_name(&self) -> String {
        match self {
            Self::Intra => "intra".into(),
            Self::Cross => "cross".into(),
            Self::Pair { left, right } => format!("pair-{left}-{right}"),
        }
    }
}

struct ComponentScopePlan {
    kind: ComponentScopeKind,
    directory: PathBuf,
    identity: ComponentSnapshotIdentity,
    runs: Vec<ForestRun>,
    roots: Option<Vec<u32>>,
    needs_rebuild: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetadataSummaryRow {
    pub scope: String,
    pub primary_chain: String,
    pub secondary_chain: String,
    pub total_contracts: i64,
    pub total_nfts: i64,
    pub group_count: i64,
    pub duplicate_contract_count: i64,
    pub duplicate_nft_count: i64,
    pub group_size_ge_2_count: i64,
    pub group_size_gt_2_count: i64,
}
#[cfg(test)]
#[derive(Default)]
struct GroupAccumulator {
    total: i64,
    primary: i64,
    primary_nfts: i64,
    has_secondary: bool,
}

#[derive(Debug, Clone, Copy, Default)]
struct SummaryStats {
    group_count: i64,
    duplicate_contract_count: i64,
    duplicate_nft_count: i64,
    group_size_ge_2_count: i64,
    group_size_gt_2_count: i64,
}

#[cfg(test)]
struct DenseSummaryScratch {
    groups: Vec<GroupAccumulator>,
    touched: Vec<u32>,
}

#[cfg(test)]
impl DenseSummaryScratch {
    fn new(root_capacity: usize) -> Self {
        Self {
            groups: std::iter::repeat_with(GroupAccumulator::default)
                .take(root_capacity)
                .collect(),
            touched: Vec::new(),
        }
    }

    fn summarize(
        &mut self,
        roots: &[u32],
        contracts: impl IntoIterator<Item = (usize, bool, i64)>,
        require_secondary: bool,
    ) -> SummaryStats {
        if self.groups.len() < roots.len() {
            self.groups
                .resize_with(roots.len(), GroupAccumulator::default);
        }
        self.touched.clear();
        for (contract, primary, nfts) in contracts {
            let Some(&root) = roots.get(contract) else {
                continue;
            };
            let Some(group) = self.groups.get_mut(root as usize) else {
                continue;
            };
            if group.total == 0 {
                self.touched.push(root);
            }
            group.total += 1;
            if primary {
                group.primary += 1;
                group.primary_nfts = group.primary_nfts.saturating_add(nfts);
            } else {
                group.has_secondary = true;
            }
        }
        let mut stats = SummaryStats::default();
        for &root in &self.touched {
            let group = &self.groups[root as usize];
            if group.primary != 0 && group.total >= 2 && (!require_secondary || group.has_secondary)
            {
                stats.group_count += 1;
                stats.duplicate_contract_count += group.primary;
                stats.duplicate_nft_count =
                    stats.duplicate_nft_count.saturating_add(group.primary_nfts);
                stats.group_size_ge_2_count += 1;
                stats.group_size_gt_2_count += i64::from(group.total > 2);
            }
        }
        for &root in &self.touched {
            self.groups[root as usize] = GroupAccumulator::default();
        }
        stats
    }
}

type ScopeForestRuns = (Vec<ForestRun>, Vec<ForestRun>, Vec<Vec<ForestRun>>);

enum ScopeSinkMessage {
    Edges { scope: usize, edges: Vec<Edge> },
    Stop,
}

/// Bounded scope-sharded admission for MetadataMatch forest edges.
///
/// Each logical scope is assigned to exactly one sink worker. The collectors
/// remain individually owned behind a mutex only so rare global retained-byte
/// compaction can stop the world without copying their dense degree arrays.
struct ScopeCollectorBroker {
    collectors: Vec<Arc<std::sync::Mutex<Option<EdgeCollector>>>>,
    senders: Vec<std::sync::mpsc::SyncSender<ScopeSinkMessage>>,
    handles: Vec<std::thread::JoinHandle<()>>,
    accepted_edges: Arc<std::sync::atomic::AtomicU64>,
    cancelled: Arc<std::sync::atomic::AtomicBool>,
    first_error: Arc<std::sync::Mutex<Option<crate::reduce::ReduceError>>>,
    retained: Arc<ScopeRetainedBudget>,
    scorer_lanes: usize,
    logical_scope_count: usize,
    shards_per_scope: usize,
    next_shard: Vec<std::sync::atomic::AtomicUsize>,
}

struct ScopeRetainedBudget {
    max_bytes: u64,
    by_scope: Vec<std::sync::atomic::AtomicU64>,
    total: std::sync::atomic::AtomicU64,
    gate: std::sync::RwLock<()>,
}

impl ScopeCollectorBroker {
    fn new(
        node_count: u32,
        chain_pair_count: usize,
        budget: EdgeBudget,
        max_retained_bytes: u64,
        threads: usize,
    ) -> Result<Self, PipelineError> {
        let scope_count = chain_pair_count.saturating_add(2);
        let shards_per_scope = if threads >= 4 { 2 } else { 1 };
        let collector_count = scope_count.saturating_mul(shards_per_scope);
        let active_sink_workers = if threads <= 1 {
            0
        } else {
            let sink_cap = (threads / 4).max(2).min(threads.saturating_sub(1));
            collector_count.min(sink_cap)
        };
        let scorer_lanes = threads.saturating_sub(active_sink_workers).max(1);
        let collectors = (0..collector_count)
            .map(|_| {
                Arc::new(std::sync::Mutex::new(Some(EdgeCollector::new_serial(
                    node_count, budget, 1_048_576,
                ))))
            })
            .collect::<Vec<_>>();
        let accepted_edges = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let first_error = Arc::new(std::sync::Mutex::new(None));
        let retained = Arc::new(ScopeRetainedBudget {
            max_bytes: max_retained_bytes,
            by_scope: (0..collector_count)
                .map(|_| std::sync::atomic::AtomicU64::new(0))
                .collect(),
            total: std::sync::atomic::AtomicU64::new(0),
            gate: std::sync::RwLock::new(()),
        });
        let mut senders = Vec::with_capacity(active_sink_workers);
        let mut handles = Vec::with_capacity(active_sink_workers);
        for worker in 0..active_sink_workers {
            let (sender, receiver) = std::sync::mpsc::sync_channel::<ScopeSinkMessage>(2);
            senders.push(sender);
            let worker_collectors = collectors.clone();
            let worker_cancelled = cancelled.clone();
            let worker_error = first_error.clone();
            let worker_retained = retained.clone();
            let handle = std::thread::Builder::new()
                .name(format!("metadata-scope-sink-{worker}"))
                .spawn(move || {
                    while let Ok(message) = receiver.recv() {
                        match message {
                            ScopeSinkMessage::Stop => break,
                            ScopeSinkMessage::Edges { scope, edges } => {
                                if worker_cancelled.load(std::sync::atomic::Ordering::Acquire) {
                                    continue;
                                }
                                let result = (|| {
                                    let over_budget = {
                                        let _admission =
                                            worker_retained.gate.read().map_err(|_| {
                                                crate::reduce::ReduceError::WorkOverflow
                                            })?;
                                        let collector = worker_collectors
                                            .get(scope)
                                            .ok_or(crate::reduce::ReduceError::WorkOverflow)?;
                                        let mut guard = collector.lock().map_err(|_| {
                                            crate::reduce::ReduceError::WorkOverflow
                                        })?;
                                        let collector = guard
                                            .as_mut()
                                            .ok_or(crate::reduce::ReduceError::WorkOverflow)?;
                                        for edge in edges {
                                            collector.push(edge)?;
                                        }
                                        drop(guard);
                                        record_broker_retained_bytes(
                                            &worker_collectors,
                                            scope,
                                            &worker_retained,
                                        )?
                                    };
                                    if over_budget {
                                        compact_broker_retained_budget(
                                            &worker_collectors,
                                            &worker_retained,
                                        )?;
                                    }
                                    Ok(())
                                })();
                                if let Err(error) = result {
                                    worker_cancelled
                                        .store(true, std::sync::atomic::Ordering::Release);
                                    if let Ok(mut first) = worker_error.lock() {
                                        if first.is_none() {
                                            *first = Some(error);
                                        }
                                    }
                                }
                            }
                        }
                    }
                })
                .map_err(|error| PipelineError::Parallel(error.to_string()))?;
            handles.push(handle);
        }
        Ok(Self {
            collectors,
            senders,
            handles,
            accepted_edges,
            cancelled,
            first_error,
            retained,
            scorer_lanes,
            logical_scope_count: scope_count,
            shards_per_scope,
            next_shard: (0..scope_count)
                .map(|_| std::sync::atomic::AtomicUsize::new(0))
                .collect(),
        })
    }

    #[cfg(test)]
    fn active_sink_workers(&self) -> usize {
        self.senders.len()
    }

    fn scorer_lanes(&self) -> usize {
        self.scorer_lanes
    }

    fn accepted_edges(&self) -> u64 {
        self.accepted_edges
            .load(std::sync::atomic::Ordering::Acquire)
    }

    fn push_edges_by_chain(
        &self,
        contract_chain: &[u32],
        chain_count: usize,
        edges: Vec<Edge>,
    ) -> Result<(), PipelineError> {
        let accepted = edges.len() as u64;
        let mut by_scope = BTreeMap::<usize, Vec<Edge>>::new();
        for edge in edges {
            let left_chain = contract_chain[edge.left as usize] as usize;
            let right_chain = contract_chain[edge.right as usize] as usize;
            if left_chain == right_chain {
                by_scope.entry(0).or_default().push(edge);
            } else {
                by_scope.entry(1).or_default().push(edge);
                by_scope
                    .entry(2 + chain_pair_index(left_chain, right_chain, chain_count))
                    .or_default()
                    .push(edge);
            }
        }
        for (scope, edges) in by_scope {
            self.submit_scope(scope, edges)?;
        }
        self.accepted_edges
            .fetch_add(accepted, std::sync::atomic::Ordering::AcqRel);
        Ok(())
    }

    fn submit_compacted_catalog_batch(
        &self,
        batch: CompactedCatalogEdges,
    ) -> Result<(), PipelineError> {
        self.submit_scope(0, batch.intra)?;
        self.submit_scope(1, batch.cross)?;
        for (pair, edges) in batch.chain_pairs {
            self.submit_scope(pair.saturating_add(2), edges)?;
        }
        self.accepted_edges
            .fetch_add(batch.accepted_edges, std::sync::atomic::Ordering::AcqRel);
        Ok(())
    }

    fn submit_scope(&self, scope: usize, edges: Vec<Edge>) -> Result<(), PipelineError> {
        if edges.is_empty() {
            return Ok(());
        }
        if self.cancelled.load(std::sync::atomic::Ordering::Acquire) {
            return Err(PipelineError::Parallel(
                "scope collector broker cancelled".into(),
            ));
        }
        if scope >= self.logical_scope_count {
            return Err(PipelineError::Invariant("invalid collector scope".into()));
        }
        let shard = self.next_shard[scope].fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            % self.shards_per_scope;
        let collector_slot = scope
            .saturating_mul(self.shards_per_scope)
            .saturating_add(shard);
        if self.senders.is_empty() {
            let _admission = self
                .retained
                .gate
                .read()
                .map_err(|_| PipelineError::Parallel("scope budget lock poisoned".into()))?;
            let collector = self
                .collectors
                .get(collector_slot)
                .ok_or_else(|| PipelineError::Invariant("invalid collector scope".into()))?;
            let mut guard = collector
                .lock()
                .map_err(|_| PipelineError::Parallel("scope collector lock poisoned".into()))?;
            let collector = guard
                .as_mut()
                .ok_or_else(|| PipelineError::Invariant("collector already finished".into()))?;
            for edge in edges {
                collector.push(edge)?;
            }
            drop(guard);
            let over_budget =
                record_broker_retained_bytes(&self.collectors, collector_slot, &self.retained)?;
            drop(_admission);
            if over_budget {
                compact_broker_retained_budget(&self.collectors, &self.retained)?;
            }
            return Ok(());
        }
        let worker = collector_slot % self.senders.len();
        self.senders[worker]
            .send(ScopeSinkMessage::Edges {
                scope: collector_slot,
                edges,
            })
            .map_err(|_| PipelineError::Parallel("scope collector sink disconnected".into()))
    }

    fn shutdown(&mut self) -> Result<(), PipelineError> {
        for sender in &self.senders {
            let _ = sender.send(ScopeSinkMessage::Stop);
        }
        self.senders.clear();
        let mut panicked = false;
        for handle in self.handles.drain(..) {
            panicked |= handle.join().is_err();
        }
        if panicked {
            Err(PipelineError::Parallel(
                "scope collector sink panicked".into(),
            ))
        } else {
            Ok(())
        }
    }

    #[cfg(test)]
    fn finish(mut self) -> Result<ScopeForestRuns, PipelineError> {
        self.shutdown()?;
        if let Some(error) = self
            .first_error
            .lock()
            .map_err(|_| PipelineError::Parallel("scope collector error lock poisoned".into()))?
            .take()
        {
            return Err(error.into());
        }
        let mut runs = Vec::with_capacity(self.collectors.len());
        for collector in self.collectors.drain(..) {
            let collector = Arc::try_unwrap(collector)
                .map_err(|_| PipelineError::Parallel("scope collector still shared".into()))?
                .into_inner()
                .map_err(|_| PipelineError::Parallel("scope collector lock poisoned".into()))?
                .ok_or_else(|| PipelineError::Invariant("collector already finished".into()))?;
            runs.push(collector.finish()?);
        }
        Ok(collapse_scope_runs(
            runs,
            self.logical_scope_count,
            self.shards_per_scope,
        ))
    }

    fn finish_parallel(
        mut self,
        worker_pool: &rayon::ThreadPool,
    ) -> Result<ScopeForestRuns, PipelineError> {
        self.shutdown()?;
        if let Some(error) = self
            .first_error
            .lock()
            .map_err(|_| PipelineError::Parallel("scope collector error lock poisoned".into()))?
            .take()
        {
            return Err(error.into());
        }
        let collectors = self.collectors.drain(..).collect::<Vec<_>>();
        let finished = worker_pool.install(|| {
            collectors
                .into_par_iter()
                .map(|collector| -> Result<Vec<ForestRun>, PipelineError> {
                    let collector = Arc::try_unwrap(collector)
                        .map_err(|_| {
                            PipelineError::Parallel("scope collector still shared".into())
                        })?
                        .into_inner()
                        .map_err(|_| {
                            PipelineError::Parallel("scope collector lock poisoned".into())
                        })?
                        .ok_or_else(|| {
                            PipelineError::Invariant("collector already finished".into())
                        })?;
                    collector.finish().map_err(PipelineError::from)
                })
                .collect::<Vec<_>>()
        });
        Ok(collapse_scope_runs(
            finished.into_iter().collect::<Result<Vec<_>, _>>()?,
            self.logical_scope_count,
            self.shards_per_scope,
        ))
    }

    fn finish_with_progress(
        self,
        worker_pool: &rayon::ThreadPool,
        progress: &mut impl FnMut(ProgressEvent),
    ) -> Result<ScopeForestRuns, PipelineError> {
        let total = self.collectors.len() as u64;
        progress(
            ProgressEvent::determinate(
                ProgressPhase::FinalizeEdgeCollectors,
                0,
                total,
                WorkUnit::Items,
                ProgressCounters::default(),
            )
            .with_plan(WorkClass::ReduceItems, TotalKind::Exact),
        );
        let runs = self.finish_parallel(worker_pool)?;
        for completed in 1..=total {
            progress(
                ProgressEvent::determinate(
                    ProgressPhase::FinalizeEdgeCollectors,
                    completed,
                    total,
                    WorkUnit::Items,
                    ProgressCounters::default(),
                )
                .with_plan(WorkClass::ReduceItems, TotalKind::Exact),
            );
        }
        Ok(runs)
    }
}

fn collapse_scope_runs(
    physical_runs: Vec<Vec<ForestRun>>,
    logical_scope_count: usize,
    shards_per_scope: usize,
) -> ScopeForestRuns {
    let mut logical_runs = (0..logical_scope_count)
        .map(|_| Vec::new())
        .collect::<Vec<Vec<ForestRun>>>();
    for (collector_slot, mut runs) in physical_runs.into_iter().enumerate() {
        let logical_scope = collector_slot / shards_per_scope.max(1);
        if let Some(target) = logical_runs.get_mut(logical_scope) {
            target.append(&mut runs);
        }
    }
    let mut runs = logical_runs.into_iter();
    let intra = runs.next().unwrap_or_default();
    let cross = runs.next().unwrap_or_default();
    (intra, cross, runs.collect())
}

impl Drop for ScopeCollectorBroker {
    fn drop(&mut self) {
        self.cancelled
            .store(true, std::sync::atomic::Ordering::Release);
        let _ = self.shutdown();
    }
}

fn record_broker_retained_bytes(
    collectors: &[Arc<std::sync::Mutex<Option<EdgeCollector>>>],
    scope: usize,
    retained_budget: &ScopeRetainedBudget,
) -> Result<bool, crate::reduce::ReduceError> {
    let retained = collectors
        .get(scope)
        .ok_or(crate::reduce::ReduceError::WorkOverflow)?
        .lock()
        .map_err(|_| crate::reduce::ReduceError::WorkOverflow)?
        .as_ref()
        .ok_or(crate::reduce::ReduceError::WorkOverflow)?
        .retained_bytes();
    let previous =
        retained_budget.by_scope[scope].swap(retained, std::sync::atomic::Ordering::AcqRel);
    let total = if retained >= previous {
        let delta = retained - previous;
        retained_budget
            .total
            .fetch_update(
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
                |total| total.checked_add(delta),
            )
            .map_err(|_| crate::reduce::ReduceError::WorkOverflow)?
            .saturating_add(delta)
    } else {
        retained_budget
            .total
            .fetch_sub(previous - retained, std::sync::atomic::Ordering::AcqRel)
            .saturating_sub(previous - retained)
    };
    Ok(total > retained_budget.max_bytes)
}

fn compact_broker_retained_budget(
    collectors: &[Arc<std::sync::Mutex<Option<EdgeCollector>>>],
    retained_budget: &ScopeRetainedBudget,
) -> Result<(), crate::reduce::ReduceError> {
    let _gate = retained_budget
        .gate
        .write()
        .map_err(|_| crate::reduce::ReduceError::WorkOverflow)?;
    if retained_budget
        .total
        .load(std::sync::atomic::Ordering::Acquire)
        <= retained_budget.max_bytes
    {
        return Ok(());
    }
    let mut compacted_total = 0u64;
    for (scope, collector) in collectors.iter().enumerate() {
        let mut guard = collector
            .lock()
            .map_err(|_| crate::reduce::ReduceError::WorkOverflow)?;
        guard
            .as_mut()
            .ok_or(crate::reduce::ReduceError::WorkOverflow)?
            .compact_retained()?;
        let retained = guard
            .as_ref()
            .ok_or(crate::reduce::ReduceError::WorkOverflow)?
            .retained_bytes();
        retained_budget.by_scope[scope].store(retained, std::sync::atomic::Ordering::Release);
        compacted_total = compacted_total
            .checked_add(retained)
            .ok_or(crate::reduce::ReduceError::WorkOverflow)?;
    }
    retained_budget
        .total
        .store(compacted_total, std::sync::atomic::Ordering::Release);
    if compacted_total > retained_budget.max_bytes {
        return Err(crate::reduce::ReduceError::Budget {
            resource: "scope_forest_bytes",
            requested: compacted_total,
            limit: retained_budget.max_bytes,
        });
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SerializableIndexMetrics {
    pub block_pair_visits: u64,
    pub contract_pair_visits: u64,
    pub routed_pairs: u64,
    pub duplicate_routes: u64,
    pub exact_full_build_bytes: u64,
    pub exact_full_mmap_bytes: u64,
}
impl From<IndexMetrics> for SerializableIndexMetrics {
    fn from(m: IndexMetrics) -> Self {
        Self {
            block_pair_visits: m.block_pair_visits,
            contract_pair_visits: m.contract_pair_visits,
            routed_pairs: m.routed_pairs,
            duplicate_routes: m.duplicate_routes,
            exact_full_build_bytes: m.exact_full_build_bytes,
            exact_full_mmap_bytes: m.exact_full_mmap_bytes,
        }
    }
}

#[derive(Debug, Error)]
pub enum PipelineError {
    #[error(transparent)]
    Identity(#[from] crate::identity::IdentityOverflow),
    #[error("parallel catalog execution failed: {0}")]
    Parallel(String),
    #[error("pipeline invariant failed: {0}")]
    Invariant(String),
    #[error(transparent)]
    Snapshot(#[from] SnapshotError),
    #[error(transparent)]
    Scheduler(#[from] crate::scheduler::SchedulerError),
    #[error(transparent)]
    Exact(#[from] crate::exact_islands::ExactIslandError),
    #[error(transparent)]
    Evidence(#[from] EvidenceError),
    #[error(transparent)]
    Reduce(#[from] crate::reduce::ReduceError),
    #[error(transparent)]
    Memory(#[from] crate::resource::MemoryError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Format(#[from] crate::format::FormatError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Storage(#[from] crate::storage::StorageLedgerError),
}

fn build_metadata_worker_pool(threads: usize) -> Result<Arc<rayon::ThreadPool>, PipelineError> {
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads.max(1))
        .thread_name(|index| format!("metadata-worker-{index}"))
        .build()
        .map(Arc::new)
        .map_err(|error| PipelineError::Parallel(error.to_string()))
}

enum CatalogMessage {
    RoutingWork(u64),
    ExpansionWork(u64),
    Error(PipelineError),
    JobDone(IndexMetrics),
}

struct CompactedCatalogEdges {
    intra: Vec<Edge>,
    cross: Vec<Edge>,
    chain_pairs: Vec<(usize, Vec<Edge>)>,
    accepted_edges: u64,
}

fn compact_catalog_scope_batch(
    features: &crate::encode::FeatureView,
    chain_count: usize,
    edges: Vec<Edge>,
    scratch: &mut ScopeCompactionScratch,
) -> CompactedCatalogEdges {
    compact_catalog_scope_batch_by_chain(&features.contract_chain, chain_count, edges, scratch)
}

fn compact_catalog_scope_batch_by_chain(
    contract_chain: &[u32],
    chain_count: usize,
    edges: Vec<Edge>,
    scratch: &mut ScopeCompactionScratch,
) -> CompactedCatalogEdges {
    let accepted_edges = edges.len() as u64;
    let mut intra = Vec::new();
    let mut chain_pairs = BTreeMap::<usize, Vec<Edge>>::new();
    for edge in edges {
        let left_chain = contract_chain[edge.left as usize] as usize;
        let right_chain = contract_chain[edge.right as usize] as usize;
        if left_chain == right_chain {
            intra.push(edge);
        } else {
            chain_pairs
                .entry(chain_pair_index(left_chain, right_chain, chain_count))
                .or_default()
                .push(edge);
        }
    }
    let intra = compact_scope_edges_with_scratch(intra, scratch);
    let mut compacted_pairs = Vec::with_capacity(chain_pairs.len());
    for (pair, pair_edges) in chain_pairs {
        let pair_edges = compact_scope_edges_with_scratch(pair_edges, scratch);
        compacted_pairs.push((pair, pair_edges));
    }
    let cross_candidate_count = compacted_pairs
        .iter()
        .map(|(_, edges)| edges.len())
        .sum::<usize>();
    let cross = compact_scope_edge_iter_with_scratch(
        compacted_pairs
            .iter()
            .flat_map(|(_, edges)| edges.iter().copied()),
        cross_candidate_count,
        scratch,
    );
    CompactedCatalogEdges {
        intra,
        cross,
        chain_pairs: compacted_pairs,
        accepted_edges,
    }
}

struct ScopeCompactionScratch {
    sparse_identities: HashMap<u32, usize>,
    dense_local_ids: Vec<u32>,
    dense_generations: Vec<u32>,
    generation: u32,
    parent: Vec<usize>,
}

impl ScopeCompactionScratch {
    fn new(contract_count: usize, dense_budget_bytes: usize) -> Self {
        let dense_bytes = contract_count.saturating_mul(2 * std::mem::size_of::<u32>());
        let dense = dense_bytes <= dense_budget_bytes;
        Self {
            sparse_identities: HashMap::new(),
            dense_local_ids: if dense {
                vec![0; contract_count]
            } else {
                Vec::new()
            },
            dense_generations: if dense {
                vec![0; contract_count]
            } else {
                Vec::new()
            },
            generation: 0,
            parent: Vec::new(),
        }
    }

    fn begin_scope(&mut self, edge_count: usize) {
        self.parent.clear();
        self.parent.reserve(edge_count.saturating_mul(2));
        if self.dense_generations.is_empty() {
            self.sparse_identities.clear();
            self.sparse_identities.reserve(edge_count.saturating_mul(2));
            return;
        }
        self.generation = self.generation.wrapping_add(1);
        if self.generation == 0 {
            self.dense_generations.fill(0);
            self.generation = 1;
        }
    }

    fn identity(&mut self, contract_id: u32) -> usize {
        if self.dense_generations.is_empty() {
            if let Some(&identity) = self.sparse_identities.get(&contract_id) {
                return identity;
            }
            let identity = self.parent.len();
            self.parent.push(identity);
            self.sparse_identities.insert(contract_id, identity);
            return identity;
        }
        let slot = contract_id as usize;
        debug_assert!(slot < self.dense_generations.len());
        if self.dense_generations[slot] == self.generation {
            return self.dense_local_ids[slot] as usize;
        }
        let identity = self.parent.len();
        self.parent.push(identity);
        self.dense_generations[slot] = self.generation;
        self.dense_local_ids[slot] = identity as u32;
        identity
    }
}

#[cfg(test)]
fn compact_scope_edges(edges: Vec<Edge>) -> Vec<Edge> {
    let contract_count = edges
        .iter()
        .flat_map(|edge| [edge.left, edge.right])
        .max()
        .map_or(0, |maximum| maximum as usize + 1);
    let mut scratch = ScopeCompactionScratch::new(contract_count, usize::MAX);
    compact_scope_edges_with_scratch(edges, &mut scratch)
}

fn compact_scope_edges_with_scratch(
    mut edges: Vec<Edge>,
    scratch: &mut ScopeCompactionScratch,
) -> Vec<Edge> {
    // Forest connectivity is order-independent; skip global sort/dedup and let
    // union-find ignore duplicate and cyclic edges.
    scratch.begin_scope(edges.len());
    let mut written = 0usize;
    for read in 0..edges.len() {
        let edge = edges[read];
        let left = scratch.identity(edge.left);
        let right = scratch.identity(edge.right);
        let left_root = sparse_find(&mut scratch.parent, left);
        let right_root = sparse_find(&mut scratch.parent, right);
        if left_root != right_root {
            scratch.parent[right_root] = left_root;
            edges[written] = edge;
            written += 1;
        }
    }
    edges.truncate(written);
    edges
}

fn compact_scope_edge_iter_with_scratch(
    edges: impl IntoIterator<Item = Edge>,
    candidate_count: usize,
    scratch: &mut ScopeCompactionScratch,
) -> Vec<Edge> {
    scratch.begin_scope(candidate_count);
    let mut forest = Vec::with_capacity(candidate_count);
    for edge in edges {
        let left = scratch.identity(edge.left);
        let right = scratch.identity(edge.right);
        let left_root = sparse_find(&mut scratch.parent, left);
        let right_root = sparse_find(&mut scratch.parent, right);
        if left_root != right_root {
            scratch.parent[right_root] = left_root;
            forest.push(edge);
        }
    }
    forest
}

fn sparse_find(parent: &mut [usize], node: usize) -> usize {
    let mut root = node;
    while parent[root] != root {
        root = parent[root];
    }
    let mut cursor = node;
    while parent[cursor] != cursor {
        let next = parent[cursor];
        parent[cursor] = root;
        cursor = next;
    }
    root
}

enum SharedMessage {
    Work { pairs: u64, groups: u64 },
    Error(PipelineError),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg(test)]
struct FallbackPairTask {
    atom: usize,
    left: usize,
    right_begin: usize,
    right_end: usize,
}

#[derive(Default)]
#[cfg(test)]
struct FallbackPairCursor {
    atom: usize,
    left: usize,
    right: usize,
}

#[cfg(test)]
fn next_fallback_pair_task(
    cursor: &mut FallbackPairCursor,
    atom_count: usize,
    max_pairs: usize,
    mut member_count: impl FnMut(usize) -> usize,
) -> Option<FallbackPairTask> {
    let max_pairs = max_pairs.max(1);
    while cursor.atom < atom_count {
        let members = member_count(cursor.atom);
        if cursor.left.saturating_add(1) >= members {
            cursor.atom += 1;
            cursor.left = 0;
            cursor.right = 0;
            continue;
        }
        let right_begin = cursor.right.max(cursor.left + 1);
        let right_end = right_begin.saturating_add(max_pairs).min(members);
        let task = FallbackPairTask {
            atom: cursor.atom,
            left: cursor.left,
            right_begin,
            right_end,
        };
        if right_end == members {
            cursor.left += 1;
            cursor.right = cursor.left.saturating_add(1);
        } else {
            cursor.right = right_end;
        }
        return Some(task);
    }
    None
}

fn append_fallback_atom_edges_parallel(
    snapshot: &MetadataSnapshot,
    worker_pool: &rayon::ThreadPool,
    collectors: &ScopeCollectorBroker,
    chain_count: usize,
    progress: &mut impl FnMut(ProgressEvent),
) -> Result<u64, PipelineError> {
    let offsets = &snapshot.features().fallback_atom_offsets;
    let atom_count = offsets.len().saturating_sub(1);
    let total = offsets.windows(2).try_fold(0u64, |total, window| {
        checked_add_pairs(total, window[1] - window[0])
    })?;
    progress(
        ProgressEvent::determinate(
            ProgressPhase::FallbackPairs,
            0,
            total,
            WorkUnit::Pairs,
            ProgressCounters::default(),
        )
        .with_plan(WorkClass::Generic, TotalKind::UpperBound),
    );

    let batches = worker_pool.install(|| {
        (0..atom_count)
            .into_par_iter()
            .map(|atom| fallback_atom_forest(snapshot, atom as u32))
            .collect::<Result<Vec<_>, _>>()
    })?;
    let mut completed = 0u64;
    for (work, edges) in batches {
        completed = completed
            .checked_add(work)
            .ok_or(crate::resource::MemoryError::Overflow)?;
        collectors.push_edges_by_chain(&snapshot.features().contract_chain, chain_count, edges)?;
        progress(
            ProgressEvent::determinate(
                ProgressPhase::FallbackPairs,
                completed,
                total,
                WorkUnit::Pairs,
                ProgressCounters {
                    matched: collectors.accepted_edges(),
                    ..ProgressCounters::default()
                },
            )
            .with_plan(WorkClass::Generic, TotalKind::UpperBound),
        );
    }
    Ok(completed)
}

fn fallback_atom_forest(
    snapshot: &MetadataSnapshot,
    atom: u32,
) -> Result<(u64, Vec<Edge>), PipelineError> {
    let members = atom_contracts(snapshot, atom);
    if members.len() < 2 || atom_members_share_common_retained_token(snapshot.features(), members) {
        return Ok((0, Vec::new()));
    }
    let mut parent = (0..members.len()).collect::<Vec<_>>();
    let mut components = members.len();
    let mut visits = 0u64;
    let mut edges = Vec::with_capacity(members.len().saturating_sub(1));
    for left_index in 0..members.len() {
        for right_index in left_index + 1..members.len() {
            visits = visits
                .checked_add(1)
                .ok_or(crate::resource::MemoryError::Overflow)?;
            if contracts_share_retained_token(
                snapshot.features(),
                members[left_index],
                members[right_index],
            ) {
                continue;
            }
            let left_root = sparse_find(&mut parent, left_index);
            let right_root = sparse_find(&mut parent, right_index);
            if left_root != right_root {
                parent[right_root] = left_root;
                edges.push(Edge::new(members[left_index], members[right_index]));
                components -= 1;
                if components == 1 {
                    return Ok((visits, edges));
                }
            }
        }
    }
    Ok((visits, edges))
}

fn atom_members_share_common_retained_token(
    features: &crate::encode::FeatureView,
    members: &[u32],
) -> bool {
    let Some(shortest) = members
        .iter()
        .map(|&contract| contract_retained_tokens(features, contract))
        .min_by_key(|tokens| tokens.len())
    else {
        return false;
    };
    shortest.iter().any(|token| {
        members.iter().all(|&contract| {
            contract_retained_tokens(features, contract)
                .binary_search(token)
                .is_ok()
        })
    })
}

fn contract_retained_tokens(features: &crate::encode::FeatureView, contract: u32) -> &[u32] {
    let begin = features.contract_token_offsets[contract as usize] as usize;
    let end = features.contract_token_offsets[contract as usize + 1] as usize;
    &features.contract_tokens[begin..end]
}

#[derive(Clone, Copy)]
struct CatalogExecutionConfig {
    lanes: usize,
    chain_count: usize,
}

struct CatalogParallelLaneState {
    batch: Vec<Edge>,
    pending_expansion: u64,
    compaction_scratch: ScopeCompactionScratch,
    expansion_scratch: CatalogExpansionScratch,
}

impl CatalogParallelLaneState {
    fn new(
        contract_count: usize,
        chain_count: usize,
        edge_batch: usize,
        dense_scratch_bytes: usize,
    ) -> Self {
        Self {
            batch: Vec::with_capacity(edge_batch),
            pending_expansion: 0,
            compaction_scratch: ScopeCompactionScratch::new(contract_count, dense_scratch_bytes),
            expansion_scratch: CatalogExpansionScratch::new(chain_count),
        }
    }
}

struct CatalogExpansionScratch {
    left_by_chain: Vec<Vec<u32>>,
    right_by_chain: Vec<Vec<u32>>,
    retained_tokens: HashSet<u32>,
}

const MAX_CATALOG_TOKEN_SET_ENTRIES: usize = 131_072;

impl CatalogExpansionScratch {
    fn new(chain_count: usize) -> Self {
        Self {
            left_by_chain: (0..chain_count).map(|_| Vec::new()).collect(),
            right_by_chain: (0..chain_count).map(|_| Vec::new()).collect(),
            retained_tokens: HashSet::new(),
        }
    }

    fn partition(
        &mut self,
        features: &crate::encode::FeatureView,
        left_contracts: &[u32],
        right_contracts: &[u32],
    ) {
        for bucket in &mut self.left_by_chain {
            bucket.clear();
        }
        for bucket in &mut self.right_by_chain {
            bucket.clear();
        }
        for &contract in left_contracts {
            self.left_by_chain[features.contract_chain[contract as usize] as usize].push(contract);
        }
        for &contract in right_contracts {
            self.right_by_chain[features.contract_chain[contract as usize] as usize].push(contract);
        }
    }

    fn retained_tokens_disjoint(
        retained_tokens: &mut HashSet<u32>,
        features: &crate::encode::FeatureView,
        left: &[u32],
        right: &[u32],
    ) -> bool {
        let token_memberships = |contracts: &[u32]| {
            contracts.iter().fold(0usize, |total, &contract| {
                total.saturating_add(contract_retained_tokens(features, contract).len())
            })
        };
        let left_memberships = token_memberships(left);
        let right_memberships = token_memberships(right);
        let (indexed, scanned, indexed_memberships) = if left_memberships <= right_memberships {
            (left, right, left_memberships)
        } else {
            (right, left, right_memberships)
        };
        if indexed_memberships > MAX_CATALOG_TOKEN_SET_ENTRIES {
            return false;
        }
        retained_tokens.clear();
        retained_tokens.reserve(indexed_memberships);
        for &contract in indexed {
            retained_tokens.extend(contract_retained_tokens(features, contract));
        }
        !scanned.iter().any(|&contract| {
            contract_retained_tokens(features, contract)
                .iter()
                .any(|token| retained_tokens.contains(token))
        })
    }
}

fn max_catalog_expansion_scratch_bytes(
    snapshot: &MetadataSnapshot,
    chain_count: usize,
) -> Result<u64, PipelineError> {
    let max_atom_members = snapshot
        .features()
        .fallback_atom_offsets
        .windows(2)
        .map(|window| window[1].saturating_sub(window[0]))
        .max()
        .unwrap_or(0);
    // Two chain-bucket copies coexist. Reserve 2x capacity slack because Vec
    // growth may temporarily exceed exact length before settling.
    let bucket_members = max_atom_members
        .checked_mul(2)
        .and_then(|members| members.checked_mul(std::mem::size_of::<u32>() as u64))
        .and_then(|bytes| bytes.checked_mul(2))
        .ok_or(crate::resource::MemoryError::Overflow)?;
    let bucket_headers = (chain_count as u64)
        .checked_mul(2)
        .and_then(|buckets| buckets.checked_mul(std::mem::size_of::<Vec<u32>>() as u64))
        .ok_or(crate::resource::MemoryError::Overflow)?;
    // HashSet bucket/control overhead is implementation-specific. 32 bytes per
    // admitted token is deliberately conservative for the bounded fast path.
    let retained_token_index = (MAX_CATALOG_TOKEN_SET_ENTRIES as u64)
        .checked_mul(32)
        .ok_or(crate::resource::MemoryError::Overflow)?;
    bucket_members
        .checked_add(bucket_headers)
        .and_then(|bytes| bytes.checked_add(retained_token_index))
        .ok_or(crate::resource::MemoryError::Overflow.into())
}

fn submit_catalog_lane_batch(
    state: &mut CatalogParallelLaneState,
    snapshot: &MetadataSnapshot,
    chain_count: usize,
    collectors: &ScopeCollectorBroker,
    edge_batch: usize,
) -> Result<(), PipelineError> {
    if state.batch.is_empty() {
        return Ok(());
    }
    let ready = std::mem::replace(&mut state.batch, Vec::with_capacity(edge_batch));
    let ready = compact_catalog_scope_batch(
        snapshot.features(),
        chain_count,
        ready,
        &mut state.compaction_scratch,
    );
    collectors.submit_compacted_catalog_batch(ready)
}

fn score_catalog_parallel(
    snapshot: &MetadataSnapshot,
    catalog: &WorkCatalog,
    plan: &RecallPlan,
    execution: CatalogExecutionConfig,
    collectors: &ScopeCollectorBroker,
    progress: &mut impl FnMut(ProgressEvent),
) -> Result<(IndexMetrics, u64), PipelineError> {
    let CatalogExecutionConfig { lanes, chain_count } = execution;
    const EDGE_BATCH: usize = 32_768;
    let routing_total = catalog.jobs.iter().try_fold(0u64, |total, job| {
        total
            .checked_add(job_routing_pair_work(snapshot, job)?)
            .ok_or(crate::scheduler::SchedulerError::WorkOverflow)
    })?;
    // Hot blocks proof-reject most of their logical nC2 universe through a
    // secondary index, while contract expansion happens only after an exact
    // atom match. Adding both worst cases produced a stable but operationally
    // meaningless multi-trillion "upper bound" and an ETA that could be off by
    // orders of magnitude. Report observed work without a fabricated finite
    // total; the counters retain exact candidate, scoring, expansion and match
    // measurements for diagnostics and future calibrated forecasts.
    progress(ProgressEvent::indeterminate(
        ProgressPhase::CatalogPairs,
        0,
        WorkUnit::Pairs,
        ProgressCounters::default(),
    ));
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(lanes.max(1))
        .thread_name(|index| format!("metadata-catalog-{index}"))
        .build()
        .map_err(|error| PipelineError::Parallel(error.to_string()))?;
    let index = ConservativeIndex::open(snapshot);
    let (sender, receiver) = std::sync::mpsc::sync_channel::<CatalogMessage>(lanes.max(1) * 2);
    let cancelled = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(|scope| -> Result<(IndexMetrics, u64), PipelineError> {
        let producer_sender = sender.clone();
        let producer_cancelled = &cancelled;
        let producer = scope.spawn(move || {
            pool.install(|| {
                const DENSE_COMPACTION_SCRATCH_BYTES: usize = 4 * 1024 * 1024;
                plan.ordered_job_ids.par_iter().for_each_init(
                    || {
                        CatalogParallelLaneState::new(
                            snapshot.features().contract_chain.len(),
                            chain_count,
                            EDGE_BATCH,
                            DENSE_COMPACTION_SCRATCH_BYTES,
                        )
                    },
                    |lane_state, &job_id| {
                        if producer_cancelled.load(std::sync::atomic::Ordering::Acquire) {
                            return;
                        }
                        let Some(job) = catalog.jobs.get(job_id as usize) else {
                            return;
                        };
                        let send_failed = std::sync::atomic::AtomicBool::new(false);
                        if job.shape == JobShape::LeftTileFanout {
                            let metrics = index
                                .for_each_job_candidate_parallel_stateful_with_work_while(
                                    job,
                                    || {
                                        CatalogParallelLaneState::new(
                                            snapshot.features().contract_chain.len(),
                                            chain_count,
                                            EDGE_BATCH,
                                            DENSE_COMPACTION_SCRATCH_BYTES,
                                        )
                                    },
                                    |state, a, b| {
                                        if send_failed.load(std::sync::atomic::Ordering::Acquire)
                                            || producer_cancelled
                                                .load(std::sync::atomic::Ordering::Acquire)
                                        {
                                            return;
                                        }
                                        let expansion_result = {
                                            let batch = &mut state.batch;
                                            let expansion_scratch = &mut state.expansion_scratch;
                                            expand_catalog_atom_pair_streaming(
                                                snapshot,
                                                a,
                                                b,
                                                true,
                                                expansion_scratch,
                                                |left, right| {
                                                    batch.push(Edge::new(left, right));
                                                },
                                            )
                                        };
                                        let work = match expansion_result {
                                            Ok(work) => work,
                                            Err(error) => {
                                                let _ = producer_sender
                                                    .send(CatalogMessage::Error(error));
                                                send_failed.store(
                                                    true,
                                                    std::sync::atomic::Ordering::Release,
                                                );
                                                producer_cancelled.store(
                                                    true,
                                                    std::sync::atomic::Ordering::Release,
                                                );
                                                return;
                                            }
                                        };
                                        if state.batch.len() >= EDGE_BATCH {
                                            if let Err(error) = submit_catalog_lane_batch(
                                                state,
                                                snapshot,
                                                chain_count,
                                                collectors,
                                                EDGE_BATCH,
                                            ) {
                                                let _ = producer_sender
                                                    .send(CatalogMessage::Error(error));
                                                send_failed.store(
                                                    true,
                                                    std::sync::atomic::Ordering::Release,
                                                );
                                                producer_cancelled.store(
                                                    true,
                                                    std::sync::atomic::Ordering::Release,
                                                );
                                                return;
                                            }
                                        }
                                        state.pending_expansion =
                                            state.pending_expansion.saturating_add(work);
                                        if state.pending_expansion >= 100_000 {
                                            if producer_sender
                                                .send(CatalogMessage::ExpansionWork(
                                                    state.pending_expansion,
                                                ))
                                                .is_err()
                                            {
                                                send_failed.store(
                                                    true,
                                                    std::sync::atomic::Ordering::Release,
                                                );
                                                producer_cancelled.store(
                                                    true,
                                                    std::sync::atomic::Ordering::Release,
                                                );
                                            }
                                            state.pending_expansion = 0;
                                        }
                                    },
                                    |state| {
                                        if send_failed.load(std::sync::atomic::Ordering::Acquire)
                                            || producer_cancelled
                                                .load(std::sync::atomic::Ordering::Acquire)
                                        {
                                            return;
                                        }
                                        if let Err(error) = submit_catalog_lane_batch(
                                            state,
                                            snapshot,
                                            chain_count,
                                            collectors,
                                            EDGE_BATCH,
                                        ) {
                                            let _ =
                                                producer_sender.send(CatalogMessage::Error(error));
                                            send_failed
                                                .store(true, std::sync::atomic::Ordering::Release);
                                            producer_cancelled
                                                .store(true, std::sync::atomic::Ordering::Release);
                                            return;
                                        }
                                        if state.pending_expansion > 0 {
                                            if producer_sender
                                                .send(CatalogMessage::ExpansionWork(
                                                    state.pending_expansion,
                                                ))
                                                .is_err()
                                            {
                                                send_failed.store(
                                                    true,
                                                    std::sync::atomic::Ordering::Release,
                                                );
                                                producer_cancelled.store(
                                                    true,
                                                    std::sync::atomic::Ordering::Release,
                                                );
                                            }
                                            state.pending_expansion = 0;
                                        }
                                    },
                                    &mut |work| {
                                        if work > 0
                                            && producer_sender
                                                .send(CatalogMessage::RoutingWork(work))
                                                .is_err()
                                        {
                                            send_failed
                                                .store(true, std::sync::atomic::Ordering::Release);
                                            producer_cancelled
                                                .store(true, std::sync::atomic::Ordering::Release);
                                        }
                                    },
                                    &mut || {
                                        !send_failed.load(std::sync::atomic::Ordering::Acquire)
                                            && !producer_cancelled
                                                .load(std::sync::atomic::Ordering::Acquire)
                                    },
                                );
                            if !send_failed.load(std::sync::atomic::Ordering::Acquire) {
                                let _ = producer_sender.send(CatalogMessage::JobDone(metrics));
                            }
                            return;
                        }
                        let metrics = index.for_each_job_candidate_with_work_while(
                            job,
                            &mut |a, b| {
                                if send_failed.load(std::sync::atomic::Ordering::Acquire)
                                    || producer_cancelled.load(std::sync::atomic::Ordering::Acquire)
                                {
                                    return;
                                }
                                let expansion_result = {
                                    let batch = &mut lane_state.batch;
                                    let expansion_scratch = &mut lane_state.expansion_scratch;
                                    expand_catalog_atom_pair_streaming(
                                        snapshot,
                                        a,
                                        b,
                                        false,
                                        expansion_scratch,
                                        |left, right| {
                                            batch.push(Edge::new(left, right));
                                        },
                                    )
                                };
                                let work = match expansion_result {
                                    Ok(work) => work,
                                    Err(error) => {
                                        let _ = producer_sender.send(CatalogMessage::Error(error));
                                        send_failed
                                            .store(true, std::sync::atomic::Ordering::Release);
                                        producer_cancelled
                                            .store(true, std::sync::atomic::Ordering::Release);
                                        return;
                                    }
                                };
                                if lane_state.batch.len() >= EDGE_BATCH {
                                    if let Err(error) = submit_catalog_lane_batch(
                                        lane_state,
                                        snapshot,
                                        chain_count,
                                        collectors,
                                        EDGE_BATCH,
                                    ) {
                                        let _ = producer_sender.send(CatalogMessage::Error(error));
                                        send_failed
                                            .store(true, std::sync::atomic::Ordering::Release);
                                        producer_cancelled
                                            .store(true, std::sync::atomic::Ordering::Release);
                                        return;
                                    }
                                }
                                lane_state.pending_expansion =
                                    lane_state.pending_expansion.saturating_add(work);
                                if lane_state.pending_expansion >= 100_000 {
                                    if producer_sender
                                        .send(CatalogMessage::ExpansionWork(
                                            lane_state.pending_expansion,
                                        ))
                                        .is_err()
                                    {
                                        send_failed
                                            .store(true, std::sync::atomic::Ordering::Release);
                                        return;
                                    }
                                    lane_state.pending_expansion = 0;
                                }
                            },
                            &mut |work| {
                                if work > 0
                                    && producer_sender
                                        .send(CatalogMessage::RoutingWork(work))
                                        .is_err()
                                {
                                    send_failed.store(true, std::sync::atomic::Ordering::Release);
                                }
                            },
                            &mut || {
                                !send_failed.load(std::sync::atomic::Ordering::Acquire)
                                    && !producer_cancelled
                                        .load(std::sync::atomic::Ordering::Acquire)
                            },
                        );
                        if !lane_state.batch.is_empty() {
                            if let Err(error) = submit_catalog_lane_batch(
                                lane_state,
                                snapshot,
                                chain_count,
                                collectors,
                                EDGE_BATCH,
                            ) {
                                let _ = producer_sender.send(CatalogMessage::Error(error));
                                producer_cancelled
                                    .store(true, std::sync::atomic::Ordering::Release);
                                return;
                            }
                        }
                        if lane_state.pending_expansion > 0
                            && producer_sender
                                .send(CatalogMessage::ExpansionWork(lane_state.pending_expansion))
                                .is_err()
                        {
                            return;
                        }
                        lane_state.pending_expansion = 0;
                        let _ = producer_sender.send(CatalogMessage::JobDone(metrics));
                    },
                );
            });
        });
        drop(sender);
        let mut completed = 0u64;
        let mut expanded = 0u64;
        let mut metrics = IndexMetrics::default();
        for message in receiver {
            match message {
                CatalogMessage::RoutingWork(work) => {
                    completed = completed
                        .checked_add(work)
                        .ok_or(crate::resource::MemoryError::Overflow)?;
                    progress(ProgressEvent::indeterminate(
                        ProgressPhase::CatalogPairs,
                        completed.saturating_add(expanded),
                        WorkUnit::Pairs,
                        ProgressCounters {
                            candidates: metrics.routed_pairs,
                            scored: metrics.routed_pairs,
                            expanded,
                            matched: collectors.accepted_edges(),
                            ..ProgressCounters::default()
                        },
                    ));
                }
                CatalogMessage::ExpansionWork(work) => {
                    expanded = expanded
                        .checked_add(work)
                        .ok_or(crate::resource::MemoryError::Overflow)?;
                    progress(ProgressEvent::indeterminate(
                        ProgressPhase::CatalogPairs,
                        completed.saturating_add(expanded),
                        WorkUnit::Pairs,
                        ProgressCounters {
                            candidates: metrics.routed_pairs,
                            scored: metrics.routed_pairs,
                            expanded,
                            matched: collectors.accepted_edges(),
                            ..ProgressCounters::default()
                        },
                    ));
                }
                CatalogMessage::Error(error) => {
                    return Err(error);
                }
                CatalogMessage::JobDone(job_metrics) => {
                    metrics.add(job_metrics);
                    progress(ProgressEvent::indeterminate(
                        ProgressPhase::CatalogPairs,
                        completed.saturating_add(expanded),
                        WorkUnit::Pairs,
                        ProgressCounters {
                            candidates: metrics.routed_pairs,
                            scored: metrics.routed_pairs,
                            expanded,
                            matched: collectors.accepted_edges(),
                            ..ProgressCounters::default()
                        },
                    ));
                }
            }
        }
        producer
            .join()
            .map_err(|_| PipelineError::Parallel("worker panicked".into()))?;
        if completed != routing_total {
            return Err(PipelineError::Invariant(format!(
                "catalog routing progress mismatch: completed={completed}, planned={routing_total}"
            )));
        }
        let combined_completed = completed
            .checked_add(expanded)
            .ok_or(crate::resource::MemoryError::Overflow)?;
        progress(ProgressEvent::indeterminate(
            ProgressPhase::CatalogPairs,
            combined_completed,
            WorkUnit::Pairs,
            ProgressCounters {
                candidates: metrics.routed_pairs,
                scored: metrics.routed_pairs,
                expanded,
                matched: collectors.accepted_edges(),
                ..ProgressCounters::default()
            },
        ));
        let admitted_pair_visits = metrics
            .routed_pairs
            .checked_add(expanded)
            .ok_or(crate::resource::MemoryError::Overflow)?;
        Ok((metrics, admitted_pair_visits))
    })
}

pub fn run_metadata_pipeline(
    features: &Path,
    blocking: &Path,
    out: &Path,
    config: &MetadataPipelineConfig,
) -> Result<MetadataPipelineResult, PipelineError> {
    run_metadata_pipeline_with_progress_and_persistence(
        features,
        blocking,
        out,
        config,
        MatchPersistence::MemoryFirst,
        |_| {},
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchPersistence {
    /// Return production summaries without committing engine-private recovery
    /// artifacts. The caller may still publish its compact final output.
    Ephemeral,
    MemoryFirst,
    Durable,
}

pub fn run_metadata_pipeline_ephemeral_with_progress(
    features: &Path,
    blocking: &Path,
    out: &Path,
    config: &MetadataPipelineConfig,
    progress: impl FnMut(ProgressEvent),
) -> Result<MetadataPipelineResult, PipelineError> {
    run_metadata_pipeline_with_callbacks_and_persistence(
        features,
        blocking,
        out,
        config,
        MatchPersistence::Ephemeral,
        progress,
        emit_default_advisory,
    )
}

pub fn run_metadata_pipeline_ephemeral_with_callbacks(
    features: &Path,
    blocking: &Path,
    out: &Path,
    config: &MetadataPipelineConfig,
    progress: impl FnMut(ProgressEvent),
    advisory: impl FnMut(&str),
) -> Result<MetadataPipelineResult, PipelineError> {
    run_metadata_pipeline_with_callbacks_and_persistence(
        features,
        blocking,
        out,
        config,
        MatchPersistence::Ephemeral,
        progress,
        advisory,
    )
}

pub fn run_metadata_pipeline_durable(
    features: &Path,
    blocking: &Path,
    out: &Path,
    config: &MetadataPipelineConfig,
) -> Result<MetadataPipelineResult, PipelineError> {
    run_metadata_pipeline_with_progress_and_persistence(
        features,
        blocking,
        out,
        config,
        MatchPersistence::Durable,
        |_| {},
    )
}

pub fn run_metadata_pipeline_durable_with_callbacks(
    features: &Path,
    blocking: &Path,
    out: &Path,
    config: &MetadataPipelineConfig,
    progress: impl FnMut(ProgressEvent),
    advisory: impl FnMut(&str),
) -> Result<MetadataPipelineResult, PipelineError> {
    run_metadata_pipeline_with_callbacks_and_persistence(
        features,
        blocking,
        out,
        config,
        MatchPersistence::Durable,
        progress,
        advisory,
    )
}

pub fn run_metadata_pipeline_with_progress(
    features: &Path,
    blocking: &Path,
    out: &Path,
    config: &MetadataPipelineConfig,
    progress: impl FnMut(ProgressEvent),
) -> Result<MetadataPipelineResult, PipelineError> {
    run_metadata_pipeline_with_progress_and_persistence(
        features,
        blocking,
        out,
        config,
        MatchPersistence::MemoryFirst,
        progress,
    )
}

pub fn run_metadata_pipeline_with_callbacks(
    features: &Path,
    blocking: &Path,
    out: &Path,
    config: &MetadataPipelineConfig,
    progress: impl FnMut(ProgressEvent),
    advisory: impl FnMut(&str),
) -> Result<MetadataPipelineResult, PipelineError> {
    run_metadata_pipeline_with_callbacks_and_persistence(
        features,
        blocking,
        out,
        config,
        MatchPersistence::MemoryFirst,
        progress,
        advisory,
    )
}

pub fn run_metadata_pipeline_with_progress_and_persistence(
    features: &Path,
    blocking: &Path,
    out: &Path,
    config: &MetadataPipelineConfig,
    persistence: MatchPersistence,
    progress: impl FnMut(ProgressEvent),
) -> Result<MetadataPipelineResult, PipelineError> {
    run_metadata_pipeline_with_callbacks_and_persistence(
        features,
        blocking,
        out,
        config,
        persistence,
        progress,
        emit_default_advisory,
    )
}

fn emit_default_advisory(message: &str) {
    eprintln!("warning: {message}; continuing with outputs marked advisory");
}

fn run_metadata_pipeline_with_callbacks_and_persistence(
    features: &Path,
    blocking: &Path,
    out: &Path,
    config: &MetadataPipelineConfig,
    persistence: MatchPersistence,
    mut progress: impl FnMut(ProgressEvent),
    mut advisory: impl FnMut(&str),
) -> Result<MetadataPipelineResult, PipelineError> {
    let started = Instant::now();
    let worker_pool = build_metadata_worker_pool(config.threads)?;
    let snapshot_verification_bytes = MetadataSnapshot::verification_bytes(features, blocking)?;
    let memory = MemoryBroker::new(config.host_total_memory, config.memory_hard_top)?;
    // verification_bytes covers every file that remains mmap-backed by the
    // immutable snapshot.  Hold this lease through Match so later catalog,
    // edge, scorer, and component reservations are admitted cumulatively.
    let _snapshot_memory = memory.reserve(snapshot_verification_bytes)?;
    progress(ProgressEvent::determinate(
        ProgressPhase::OpenSnapshot,
        0,
        snapshot_verification_bytes,
        WorkUnit::Bytes,
        ProgressCounters::default(),
    ));
    let mut verified_bytes = 0u64;
    let snapshot = MetadataSnapshot::open_with_progress(features, blocking, |bytes| {
        verified_bytes = verified_bytes
            .saturating_add(bytes)
            .min(snapshot_verification_bytes);
        progress(ProgressEvent::determinate(
            ProgressPhase::OpenSnapshot,
            verified_bytes,
            snapshot_verification_bytes,
            WorkUnit::Bytes,
            ProgressCounters::default(),
        ));
    })?;
    crate::identity::checked_u32_identity("contracts", snapshot.contract_count() as u64)?;
    crate::identity::checked_u32_identity("atoms", snapshot.atom_count() as u64)?;
    crate::identity::checked_u32_identity("blocks", snapshot.blocking().block_kinds.len() as u64)?;
    crate::identity::checked_u32_identity(
        "token identities",
        snapshot
            .features()
            .token_member_offsets
            .len()
            .saturating_sub(1) as u64,
    )?;
    crate::identity::checked_u32_identity("chains", snapshot.chain_names().len() as u64)?;
    progress(ProgressEvent::determinate(
        ProgressPhase::OpenSnapshot,
        snapshot_verification_bytes,
        snapshot_verification_bytes,
        WorkUnit::Bytes,
        ProgressCounters::default(),
    ));
    // Both fallback atoms and catalog blocks may represent quadratic raw pair
    // universes while requiring only a bounded connectivity forest. They are
    // admitted dynamically by executed work below, not by raw Cartesian size.
    // Selected chains are part of the immutable snapshot identity even when a
    // chain has no eligible metadata contract.  Deriving this count from the
    // compact contract table silently drops trailing (or all) empty chains
    // from component scopes and summary rows.
    let chain_count = snapshot.chain_names().len();
    let chain_pair_count = chain_count
        .checked_mul(chain_count.saturating_sub(1))
        .and_then(|value| value.checked_div(2))
        .ok_or(crate::resource::MemoryError::Overflow)?;
    let component_bytes = (snapshot.contract_count() as u64)
        .checked_mul(std::mem::size_of::<u32>() as u64)
        .and_then(|bytes| bytes.checked_mul(chain_pair_count.saturating_add(2) as u64))
        .ok_or(crate::resource::MemoryError::Overflow)?;
    let scope_count = chain_pair_count.saturating_add(2) as u64;
    const COMPONENT_MANIFEST_ALLOWANCE_PER_SCOPE: u64 = 8 * 1024;
    let component_artifact_bytes = scope_count
        .checked_mul(COMPONENT_MANIFEST_ALLOWANCE_PER_SCOPE)
        .and_then(|metadata| metadata.checked_add(component_bytes))
        .ok_or(crate::resource::MemoryError::Overflow)?;
    let component_partial_peak_bytes = (snapshot.contract_count() as u64)
        .checked_mul(std::mem::size_of::<u32>() as u64)
        .and_then(|bytes| bytes.checked_add(COMPONENT_MANIFEST_ALLOWANCE_PER_SCOPE))
        .ok_or(crate::resource::MemoryError::Overflow)?;
    let forest_upper_bytes = (snapshot.contract_count() as u64)
        .saturating_sub(1)
        .checked_mul(scope_count)
        .and_then(|edges| edges.checked_mul(std::mem::size_of::<Edge>() as u64))
        .ok_or(crate::resource::MemoryError::Overflow)?;
    let edge_bytes = config.edge_bytes.min(forest_upper_bytes.max(64 * 1024));
    let catalog_bytes = config
        .max_catalog_jobs
        .checked_mul(std::mem::size_of::<crate::scheduler::JobDescriptor>() as u64)
        .ok_or(crate::resource::MemoryError::Overflow)?;
    let mut storage = StorageBroker::open(&config.storage_work_directory)?;
    storage.retire_checkpoint_artifacts("metadata_complete", "superseded metadata checkpoint")?;
    std::fs::create_dir_all(out)?;
    if persistence != MatchPersistence::Durable {
        clear_prior_match_artifacts_for_memory_first(&mut storage, out)?;
    }
    let mut storage_leases = Vec::new();
    if persistence != MatchPersistence::Ephemeral {
        for reservation in [
            (
                ArtifactClass::ComponentSnapshot,
                component_artifact_bytes,
                component_partial_peak_bytes,
                "metadata component snapshots",
            ),
            (
                ArtifactClass::Summary,
                16 << 20,
                16 << 20,
                "metadata summary",
            ),
        ] {
            if let Some(lease) = reserve_pipeline_storage_advisory(
                &mut storage,
                reservation.0,
                reservation.1,
                reservation.2,
                reservation.3,
                &mut advisory,
            )? {
                storage_leases.push(lease);
            }
        }
    }
    if persistence == MatchPersistence::Durable {
        for reservation in [
            (
                ArtifactClass::Index,
                catalog_bytes,
                catalog_bytes.min(64 << 20),
                "metadata catalog index",
            ),
            (
                ArtifactClass::ExactEvidence,
                edge_bytes,
                edge_bytes / 2,
                "metadata exact evidence",
            ),
            (
                ArtifactClass::ConnectivityRun,
                edge_bytes,
                edge_bytes,
                "metadata connectivity runs",
            ),
        ] {
            if let Some(lease) = reserve_pipeline_storage_advisory(
                &mut storage,
                reservation.0,
                reservation.1,
                reservation.2,
                reservation.3,
                &mut advisory,
            )? {
                storage_leases.push(lease);
            }
        }
    }
    let _catalog_mem = memory.reserve(
        config
            .max_catalog_jobs
            .saturating_mul(std::mem::size_of::<crate::scheduler::JobDescriptor>() as u64),
    )?;
    let catalog_blocks = snapshot.blocking().block_kinds.len() as u64;
    let index_dir = out.join("index-1");
    let catalog_dir = index_dir.join("catalog");
    if persistence == MatchPersistence::Durable {
        std::fs::create_dir_all(&index_dir)?;
    }
    progress(ProgressEvent::determinate(
        ProgressPhase::BuildCatalog,
        0,
        catalog_blocks,
        WorkUnit::Items,
        ProgressCounters::default(),
    ));
    let catalog_budget = UniverseBudget {
        max_jobs: config.max_catalog_jobs,
        max_catalog_bytes: config
            .max_catalog_jobs
            .saturating_mul(std::mem::size_of::<crate::scheduler::JobDescriptor>() as u64),
        cold_members_per_job: 262_144,
    };
    let catalog_progress = |completed, total, groups| {
        progress(ProgressEvent::determinate(
            ProgressPhase::BuildCatalog,
            completed,
            total,
            WorkUnit::Items,
            ProgressCounters {
                groups,
                ..ProgressCounters::default()
            },
        ));
    };
    let catalog = if persistence == MatchPersistence::Durable {
        WorkCatalog::open_or_rebuild_with_progress(
            &catalog_dir,
            &snapshot,
            catalog_budget,
            crate::blocking::DEFAULT_MAX_ROUTING_BLOCK_MEMBERS as u64,
            catalog_progress,
        )?
    } else {
        WorkCatalog::build_with_progress(
            &snapshot,
            catalog_budget,
            crate::blocking::DEFAULT_MAX_ROUTING_BLOCK_MEMBERS as u64,
            catalog_progress,
        )?
    };
    progress(ProgressEvent::determinate(
        ProgressPhase::BuildCatalog,
        catalog_blocks,
        catalog_blocks,
        WorkUnit::Items,
        ProgressCounters::default(),
    ));
    if persistence == MatchPersistence::Durable {
        let index_manifest=serde_json::json!({"index_revision":1,"profile":"base_equivalent","job_count":catalog.jobs.len(),"exact_full_build_bytes":0,"exact_full_mmap_bytes":0}).to_string();
        crate::format::commit_ready(&index_dir, "index.ready", &index_manifest)?;
    }
    let exact_plan = plan_exact_evidence(
        snapshot.atom_count() as u64,
        config.exact_sample_lefts,
        config.exact_pair_work,
    )?;
    let evidence_resident_target =
        (config.memory_hard_top / 32).clamp(1, MAX_EVIDENCE_RESIDENT_BYTES);
    let evidence_partition_floor = if persistence == MatchPersistence::Durable {
        64 * 1024
    } else {
        1
    };
    let evidence_partition_bytes = (evidence_resident_target / 3)
        .max(evidence_partition_floor)
        .min(
            config
                .exact_pair_work
                .saturating_mul(16)
                .max(evidence_partition_floor),
        );
    let evidence_resident_bytes = evidence_partition_bytes.saturating_mul(3);
    // Three retained evidence partitions coexist with per-worker vectors and
    // shared-token routing scratch.  Reserve the conservative peak for their
    // full lifetime so later lane admission sees the real resident pressure.
    let evidence_peak_bytes = evidence_resident_bytes
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(evidence_partition_bytes.saturating_mul(2)))
        .ok_or(crate::resource::MemoryError::Overflow)?;
    let _evidence_memory = memory.reserve(evidence_peak_bytes)?;
    let pair_sample_count = exact_plan
        .calibration_lefts
        .saturating_add(exact_plan.holdout_lefts) as usize;
    let pair_samples = deterministic_sample(snapshot.atom_count(), pair_sample_count);
    let samples = pair_samples.iter().step_by(2).copied().collect::<Vec<_>>();
    let holdout_samples = pair_samples
        .iter()
        .skip(1)
        .step_by(2)
        .copied()
        .collect::<Vec<_>>();
    let exact_root = out.join("exact-islands");
    let exact_dir = exact_root.join("pair-calibration-1");
    let exact = if persistence == MatchPersistence::Durable {
        open_pair_exact_evidence(&exact_dir, &snapshot, &samples)?
    } else {
        None
    };
    let exact = if let Some(evidence) = exact {
        progress(ProgressEvent::determinate(
            ProgressPhase::PairExactIsland,
            evidence.pair_work,
            evidence.pair_work,
            WorkUnit::Pairs,
            ProgressCounters {
                matched: evidence.exact_matches,
                ..ProgressCounters::default()
            },
        ));
        evidence
    } else {
        run_pair_exact_island_with_progress(
            &snapshot,
            &samples,
            ExactEvidenceBudget {
                max_lefts: exact_plan.calibration_lefts,
                max_pair_work: exact_plan
                    .calibration_lefts
                    .saturating_mul(snapshot.atom_count().saturating_sub(1) as u64),
                max_artifact_bytes: evidence_partition_bytes,
                max_lanes: config.threads.max(1),
            },
            (persistence == MatchPersistence::Durable).then_some(exact_dir.as_path()),
            &mut progress,
        )?
    };
    let holdout_dir = exact_root.join("pair-holdout-1");
    let pair_holdout_evidence = if let Some(evidence) = if persistence == MatchPersistence::Durable
    {
        open_pair_exact_evidence(&holdout_dir, &snapshot, &holdout_samples)?
    } else {
        None
    } {
        progress(ProgressEvent::determinate(
            ProgressPhase::PairExactHoldout,
            evidence.pair_work,
            evidence.pair_work,
            WorkUnit::Pairs,
            ProgressCounters {
                matched: evidence.exact_matches,
                ..ProgressCounters::default()
            },
        ));
        evidence
    } else {
        run_pair_exact_island_with_progress(
            &snapshot,
            &holdout_samples,
            ExactEvidenceBudget {
                max_lefts: exact_plan.holdout_lefts,
                max_pair_work: exact_plan
                    .holdout_lefts
                    .saturating_mul(snapshot.atom_count().saturating_sub(1) as u64),
                max_artifact_bytes: evidence_partition_bytes,
                max_lanes: config.threads.max(1),
            },
            (persistence == MatchPersistence::Durable).then_some(holdout_dir.as_path()),
            |mut event| {
                event.phase = match event.phase {
                    ProgressPhase::PairExactIsland => ProgressPhase::PairExactHoldout,
                    ProgressPhase::PairExactFinalize => ProgressPhase::PairExactHoldoutFinalize,
                    other => other,
                };
                progress(event);
            },
        )?
    };
    let shared_token_samples = stratified_active_token_sample(
        &snapshot.features().token_member_offsets,
        (config.exact_sample_lefts as usize).saturating_mul(2),
    );
    let shared_plan = plan_shared_token_evidence(
        &snapshot.features().token_member_offsets,
        &shared_token_samples,
        config.exact_sample_lefts,
        exact_plan.remaining_pair_work,
    )?;
    let shared_dir = out.join("exact-islands/shared-token-1");
    let shared_token_exact_evidence = if let Some(evidence) =
        if persistence == MatchPersistence::Durable {
            open_shared_token_exact_evidence(
                &shared_dir,
                &snapshot,
                &shared_plan.calibration_tokens,
                &shared_plan.holdout_tokens,
            )?
        } else {
            None
        } {
        progress(ProgressEvent::determinate(
            ProgressPhase::SharedTokenExactIsland,
            evidence.pair_work,
            evidence.pair_work,
            WorkUnit::Pairs,
            ProgressCounters {
                groups: evidence
                    .calibration_tokens
                    .len()
                    .saturating_add(evidence.holdout_tokens.len()) as u64,
                matched: evidence.exact_matches,
                ..ProgressCounters::default()
            },
        ));
        evidence
    } else {
        run_shared_token_exact_islands_with_progress(
            &snapshot,
            &shared_plan.calibration_tokens,
            &shared_plan.holdout_tokens,
            ExactEvidenceBudget {
                max_lefts: shared_plan
                    .calibration_tokens
                    .len()
                    .saturating_add(shared_plan.holdout_tokens.len())
                    as u64,
                max_pair_work: shared_plan.pair_work,
                max_artifact_bytes: evidence_partition_bytes,
                max_lanes: config.threads.max(1),
            },
            (persistence == MatchPersistence::Durable).then_some(shared_dir.as_path()),
            &mut progress,
        )?
    };
    let calibration_rescue_plan = RescuePlan::from_calibration(
        &exact.conservative_misses,
        &shared_token_exact_evidence.calibration_misses,
    );
    let evidence_gate_report = evaluate_holdout(
        HoldoutEvidence {
            evaluated_pair_work: pair_holdout_evidence
                .pair_work
                .checked_add(shared_token_exact_evidence.holdout_pair_work)
                .ok_or_else(|| {
                    PipelineError::Invariant("holdout evaluated pair work overflow".into())
                })?,
            exhaustive: evidence_scan_is_exhaustive(
                pair_frontier_covers_all_unordered_pairs(
                    &samples,
                    &holdout_samples,
                    snapshot.atom_count(),
                ),
                shared_plan.covers_all_active_groups(&snapshot.features().token_member_offsets),
                shared_token_exact_evidence.pair_work,
                shared_plan.pair_work,
            ),
            pair_exact_matches: pair_holdout_evidence.exact_matches,
            pair_misses: &pair_holdout_evidence.conservative_misses,
            shared_exact_matches: shared_token_exact_evidence.holdout_exact_matches,
            shared_misses: &shared_token_exact_evidence.holdout_misses,
            skipped_shared_groups: &shared_plan.skipped_tokens,
            skipped_shared_pair_work: shared_plan.skipped_pair_work,
            considered_shared_pair_work: shared_plan.considered_pair_work,
            shared_work_strata: &shared_plan.work_strata,
            pair_clusters: &pair_holdout_evidence.clusters,
            shared_clusters: &shared_token_exact_evidence.holdout_clusters,
        },
        &calibration_rescue_plan,
        config.evidence_gate_policy,
    )?;
    if let Some(message) = evidence_gate_report.advisory_message() {
        advisory(&message);
    }
    // Keep the advisory honest by evaluating the calibration-only plan above.
    // Once that report is frozen, add every holdout miss as an already
    // exact-verified direct edge. Only calibration endpoints generalize across
    // token groups, preventing holdout evidence from creating unbounded global
    // rescue scoring work.
    let holdout_rescue_plan = RescuePlan::from_holdout(
        &pair_holdout_evidence.conservative_misses,
        &shared_token_exact_evidence.holdout_misses,
    );
    let rescue_plan = calibration_rescue_plan.merge(holdout_rescue_plan);
    let connectivity_plan_digest = connectivity_plan_digest(&rescue_plan)?;
    if persistence == MatchPersistence::Durable {
        let rescue_json = serde_json::to_string_pretty(&serde_json::json!({
            "revision": 2,
            "schema_revision": crate::scoring::MATCH_SEMANTICS_REVISION,
            "snapshot_fingerprint": catalog.snapshot_fingerprint,
            "plan": rescue_plan,
        }))?;
        crate::format::commit_ready(
            &out.join("rescue-plan-1"),
            "rescue-plan.ready",
            &rescue_json,
        )?;
    }
    let recall = RecallPlan::freeze_with_rescue_lefts(
        &catalog,
        samples,
        Vec::new(),
        rescue_plan.pair_atoms.clone(),
    );
    if persistence == MatchPersistence::Durable {
        recall.commit(&out.join("recall-plan-1"))?;
    }
    let _edge_memory = memory.reserve(edge_bytes)?;
    let max_edge_count = edge_bytes / (std::mem::size_of::<Edge>() as u64);
    let scope_count = scope_count.max(1);
    let epoch_edge_cap = (max_edge_count / scope_count).clamp(1, 10_000_000);
    let budget = EdgeBudget {
        max_buffer_bytes: (edge_bytes / scope_count)
            .max(std::mem::size_of::<Edge>() as u64)
            .min(epoch_edge_cap.saturating_mul(std::mem::size_of::<Edge>() as u64)),
        max_run_edges: epoch_edge_cap,
        max_total_bytes: edge_bytes,
    };
    let component_peak_bytes = component_bytes
        .checked_mul(10)
        .ok_or(crate::resource::MemoryError::Overflow)?;
    let _component_memory = memory.reserve(component_peak_bytes)?;
    let node_count = snapshot.contract_count() as u32;
    let runs_dir = out.join("connectivity-runs");
    let recovered = if persistence == MatchPersistence::Durable {
        open_connectivity_runs(
            &runs_dir,
            &catalog.snapshot_fingerprint,
            &connectivity_plan_digest,
            chain_count,
        )?
    } else {
        None
    };
    let (intra_runs, cross_runs, pair_runs, metrics, candidate_pair_visits, _accepted_edge_count) =
        if let Some(recovered) = recovered {
            progress(ProgressEvent::determinate(
                ProgressPhase::FallbackPairs,
                0,
                0,
                WorkUnit::Pairs,
                ProgressCounters::default(),
            ));
            progress(ProgressEvent::determinate(
                ProgressPhase::CatalogPairs,
                0,
                0,
                WorkUnit::Work,
                ProgressCounters::default(),
            ));
            progress(ProgressEvent::determinate(
                ProgressPhase::SharedTokenPairs,
                0,
                0,
                WorkUnit::Pairs,
                ProgressCounters::default(),
            ));
            progress(ProgressEvent::determinate(
                ProgressPhase::PlanRescuePairs,
                0,
                0,
                WorkUnit::Pairs,
                ProgressCounters::default(),
            ));
            progress(ProgressEvent::determinate(
                ProgressPhase::RescuePairs,
                0,
                0,
                WorkUnit::Pairs,
                ProgressCounters::default(),
            ));
            progress(ProgressEvent::determinate(
                ProgressPhase::EdgeDispatch,
                recovered.accepted_edge_count,
                recovered.accepted_edge_count,
                WorkUnit::Edges,
                ProgressCounters::default(),
            ));
            for phase in [
                ProgressPhase::FinalizeEdgeCollectors,
                ProgressPhase::CommitConnectivityRuns,
            ] {
                progress(ProgressEvent::determinate(
                    phase,
                    0,
                    0,
                    WorkUnit::Items,
                    ProgressCounters::default(),
                ));
            }
            (
                recovered.intra,
                recovered.cross,
                recovered.pairs,
                recovered.index_metrics,
                recovered.candidate_pair_visits,
                recovered.accepted_edge_count,
            )
        } else {
            let rescue_execution = build_rescue_execution_plan(
                &snapshot,
                &rescue_plan,
                config.threads,
                &memory,
                &mut progress,
            )?;
            let rescue_execution_plan = &rescue_execution.plan;
            let rescue_pair_visits = rescue_execution_plan.total_visits();
            let collectors = ScopeCollectorBroker::new(
                node_count,
                chain_pair_count,
                budget,
                edge_bytes,
                config.threads,
            )?;
            let match_pool = build_metadata_worker_pool(collectors.scorer_lanes())?;
            // A representative fallback atom is chain-local and scoring-equivalent.
            // Build only the token-disjoint connectivity forest needed by reduction;
            // do not enumerate its raw quadratic pair universe once connected.
            let fallback_pair_visits = append_fallback_atom_edges_parallel(
                &snapshot,
                &match_pool,
                &collectors,
                chain_count,
                &mut progress,
            )?;
            let admitted_pair_visits = rescue_pair_visits
                .checked_add(fallback_pair_visits)
                .ok_or(crate::resource::MemoryError::Overflow)?;
            const BASE_CATALOG_LANE_BYTES: u64 = 8 * 1024 * 1024;
            let hot_index_bytes = max_hot_block_candidate_index_bytes(&snapshot, &catalog)?;
            let hot_row_bytes = max_hot_block_parallel_row_bytes(&snapshot, &catalog)?;
            let expansion_scratch_bytes =
                max_catalog_expansion_scratch_bytes(&snapshot, chain_count)?;
            let catalog_lane_bytes = BASE_CATALOG_LANE_BYTES
                .checked_add(hot_row_bytes)
                .and_then(|bytes| bytes.checked_add(expansion_scratch_bytes))
                .ok_or(crate::resource::MemoryError::Overflow)?;
            let hot_job_count = catalog
                .jobs
                .iter()
                .filter(|job| job.shape == JobShape::LeftTileFanout)
                .count();
            let mut lanes = collectors.scorer_lanes().max(1);
            loop {
                let concurrent_hot_jobs = hot_job_count.min(lanes) as u64;
                let fixed_hot_bytes = concurrent_hot_jobs.saturating_mul(hot_index_bytes);
                let admitted = memory.active_lanes(lanes, fixed_hot_bytes, catalog_lane_bytes);
                if admitted >= lanes || lanes == 1 {
                    break;
                }
                lanes = admitted.max(1);
            }
            let fixed_hot_bytes = (hot_job_count.min(lanes) as u64).saturating_mul(hot_index_bytes);
            let scorer_bytes = fixed_hot_bytes
                .checked_add((lanes as u64).saturating_mul(catalog_lane_bytes))
                .ok_or(crate::resource::MemoryError::Overflow)?;
            let _scorer_memory = memory.reserve(scorer_bytes)?;
            let catalog_result = score_catalog_parallel(
                &snapshot,
                &catalog,
                &recall,
                CatalogExecutionConfig { lanes, chain_count },
                &collectors,
                &mut progress,
            );
            let (catalog_metrics, catalog_pair_visits) = catalog_result?;
            let metrics: SerializableIndexMetrics = catalog_metrics.into();
            let catalog_admitted_pair_visits = admitted_pair_visits
                .checked_add(catalog_pair_visits)
                .ok_or(crate::resource::MemoryError::Overflow)?;
            drop(_scorer_memory);
            // Large shared-token scopes use group-local BaseEquivalent routing
            // while remaining source-context isolated.
            let shared_index_bytes =
                max_shared_group_index_bytes_with_progress(&snapshot, &mut progress)?;
            let shared_lane_bytes = shared_index_bytes
                .saturating_add(BASE_CATALOG_LANE_BYTES)
                .max(1);
            let shared_lanes = memory
                .active_lanes(collectors.scorer_lanes(), 0, shared_lane_bytes)
                .max(1);
            let _shared_index_mem =
                memory.reserve((shared_lanes as u64).saturating_mul(shared_lane_bytes))?;
            let shared_result = append_shared_token_edges(
                &snapshot,
                shared_lanes,
                &collectors,
                chain_count,
                &mut progress,
            );
            let shared_pair_visits = shared_result?;
            let candidate_pair_visits = catalog_admitted_pair_visits
                .checked_add(shared_pair_visits)
                .ok_or(crate::resource::MemoryError::Overflow)?;
            append_rescue_edges(
                &snapshot,
                rescue_execution_plan,
                &match_pool,
                &collectors,
                chain_count,
                &mut progress,
            )?;
            drop(rescue_execution);
            let accepted_edge_count = collectors.accepted_edges();
            let (intra_runs, cross_runs, pair_runs) =
                collectors.finish_with_progress(&match_pool, &mut progress)?;
            if persistence == MatchPersistence::Durable {
                commit_connectivity_runs(
                    &runs_dir,
                    ConnectivityCommit {
                        snapshot_fingerprint: &catalog.snapshot_fingerprint,
                        connectivity_plan_digest: &connectivity_plan_digest,
                        chain_count,
                        intra: &intra_runs,
                        cross: &cross_runs,
                        pairs: &pair_runs,
                        index_metrics: &metrics,
                        candidate_pair_visits,
                        accepted_edge_count,
                    },
                    &mut progress,
                )?;
            }
            progress(ProgressEvent::determinate(
                ProgressPhase::EdgeDispatch,
                accepted_edge_count,
                accepted_edge_count,
                WorkUnit::Edges,
                ProgressCounters::default(),
            ));
            (
                intra_runs,
                cross_runs,
                pair_runs,
                metrics,
                candidate_pair_visits,
                accepted_edge_count,
            )
        };
    let persisted_edges = run_edge_count(&intra_runs)
        .saturating_add(run_edge_count(&cross_runs))
        .saturating_add(
            pair_runs
                .iter()
                .map(|runs| run_edge_count(runs))
                .sum::<usize>(),
        );
    let persisted_bytes = persisted_edges.saturating_mul(std::mem::size_of::<Edge>()) as u64;
    if persisted_bytes > edge_bytes {
        return Err(crate::reduce::ReduceError::Budget {
            resource: "scope_forest_bytes",
            requested: persisted_bytes,
            limit: edge_bytes,
        }
        .into());
    }
    let component_root = out.join("component-snapshots");
    let mut scopes = Vec::with_capacity(pair_runs.len().saturating_add(2));
    let mut push_scope = |kind: ComponentScopeKind,
                          runs: Vec<ForestRun>|
     -> Result<(), PipelineError> {
        let directory = component_root.join(kind.directory_name());
        let identity = ComponentSnapshotIdentity {
            schema_revision: crate::scoring::MATCH_SEMANTICS_REVISION,
            snapshot_fingerprint: catalog.snapshot_fingerprint.clone(),
            connectivity_revision: CONNECTIVITY_RUN_REVISION,
            connectivity_plan_digest: connectivity_plan_digest.clone(),
            scope_identity: kind.identity(),
            node_count,
        };
        let roots = if persistence == MatchPersistence::Durable {
            open_component_snapshot_chain(&directory, &identity)?
                .map(|snapshots| {
                    recover_component_snapshots(&snapshots).map(|snapshot| snapshot.roots.clone())
                })
                .transpose()?
        } else {
            None
        };
        let needs_rebuild = roots.is_none();
        scopes.push(ComponentScopePlan {
            kind,
            directory,
            identity,
            runs,
            roots,
            needs_rebuild,
        });
        Ok(())
    };
    push_scope(ComponentScopeKind::Intra, intra_runs)?;
    push_scope(ComponentScopeKind::Cross, cross_runs)?;
    let mut pair_runs = pair_runs.into_iter();
    for left in 0..chain_count {
        for right in left + 1..chain_count {
            let runs = pair_runs
                .next()
                .ok_or_else(|| PipelineError::Invariant("missing chain-pair forest runs".into()))?;
            push_scope(
                ComponentScopeKind::Pair {
                    left: left as u32,
                    right: right as u32,
                },
                runs,
            )?;
        }
    }
    if pair_runs.next().is_some() {
        return Err(PipelineError::Invariant(
            "unexpected extra chain-pair forest runs".into(),
        ));
    }
    let reduce_total = scopes
        .iter()
        .filter(|scope| scope.roots.is_none())
        .try_fold(0u64, |total, scope| -> Result<u64, PipelineError> {
            total
                .checked_add(reduce_work(&scope.runs, node_count)?)
                .ok_or(crate::resource::MemoryError::Overflow.into())
        })?;
    progress(ProgressEvent::determinate(
        ProgressPhase::ReduceScopes,
        0,
        reduce_total,
        WorkUnit::Items,
        ProgressCounters::default(),
    ));
    reduce_component_scopes_parallel(
        &mut scopes,
        node_count,
        reduce_total,
        &worker_pool,
        &mut progress,
    )?;
    let recovery_total = scopes.len() as u64;
    let finalize_phase = if persistence == MatchPersistence::Durable {
        ProgressPhase::BuildRecoveryChain
    } else {
        ProgressPhase::FinalizeComponents
    };
    progress(ProgressEvent::determinate(
        finalize_phase,
        0,
        recovery_total,
        WorkUnit::Items,
        ProgressCounters::default(),
    ));
    let mut reused_scopes = 0u64;
    let mut rebuilt_scopes = 0u64;
    for (index, scope) in scopes.iter_mut().enumerate() {
        if scope.needs_rebuild {
            if scope.roots.is_none() {
                return Err(PipelineError::Invariant("missing reduced roots".into()));
            }
            rebuilt_scopes = rebuilt_scopes.saturating_add(1);
        } else {
            reused_scopes = reused_scopes.saturating_add(1);
        }
        progress(ProgressEvent::determinate(
            finalize_phase,
            index as u64 + 1,
            recovery_total,
            WorkUnit::Items,
            ProgressCounters {
                matched: reused_scopes,
                groups: rebuilt_scopes,
                ..ProgressCounters::default()
            },
        ));
    }
    let component_total = if persistence == MatchPersistence::Ephemeral {
        0
    } else {
        scopes
            .iter()
            .filter(|scope| scope.needs_rebuild)
            .count()
            .saturating_mul(2) as u64
    };
    progress(ProgressEvent::determinate(
        ProgressPhase::CommitComponents,
        0,
        component_total,
        WorkUnit::Files,
        ProgressCounters::default(),
    ));
    if persistence != MatchPersistence::Ephemeral {
        commit_component_scopes_parallel(&scopes, component_total, &worker_pool, &mut progress)?;
    }
    let mut intra_roots = None;
    let mut cross_roots = None;
    let mut chain_pair_roots = Vec::with_capacity(scopes.len().saturating_sub(2));
    for scope in scopes {
        let roots = scope.roots.ok_or_else(|| {
            PipelineError::Invariant("component scope has no recovered roots".into())
        })?;
        match scope.kind {
            ComponentScopeKind::Intra => intra_roots = Some(roots),
            ComponentScopeKind::Cross => cross_roots = Some(roots),
            ComponentScopeKind::Pair { left, right } => chain_pair_roots.push(ChainPairRoots {
                left_chain: left,
                right_chain: right,
                roots,
            }),
        }
    }
    let scope_components = ScopeComponents {
        intra_roots: intra_roots
            .ok_or_else(|| PipelineError::Invariant("missing intra component scope".into()))?,
        cross_roots: cross_roots
            .ok_or_else(|| PipelineError::Invariant("missing cross component scope".into()))?,
        chain_pair_roots,
    };
    let summary_rows = build_summary_rows_with_progress(
        &snapshot,
        &scope_components,
        chain_count,
        &worker_pool,
        &mut progress,
    );
    let result = MetadataPipelineResult {
        schema_revision: crate::scoring::MATCH_SEMANTICS_REVISION,
        evidence_gate_revision: EVIDENCE_GATE_REVISION,
        snapshot_fingerprint: catalog.snapshot_fingerprint.clone(),
        snapshot_atoms: snapshot.atom_count() as u64,
        index_metrics: metrics,
        exact_evidence: exact,
        pair_holdout_evidence,
        shared_token_exact_evidence,
        skipped_shared_token_evidence_groups: shared_plan.skipped_tokens,
        rescue_plan,
        planned_candidate_pair_visits: candidate_pair_visits,
        evidence_holdout_misses: evidence_gate_report.observed_misses,
        evidence_gate_report,
        effective_edge_budget_bytes: edge_bytes,
        edge_count: persisted_edges as u64,
        scope_components,
        summary_rows,
        wall_millis: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
    };
    let summary_dir = out.join("metadata-summary-1");
    progress(ProgressEvent::determinate(
        ProgressPhase::CommitArtifacts,
        0,
        1,
        WorkUnit::Items,
        ProgressCounters::default(),
    ));
    if persistence != MatchPersistence::Ephemeral {
        let ready = serde_json::json!({
            "schema_revision": result.schema_revision,
            "evidence_gate_revision": result.evidence_gate_revision,
            "snapshot_fingerprint": result.snapshot_fingerprint,
            "snapshot_atoms": result.snapshot_atoms,
            "index_metrics": result.index_metrics,
            "exact_evidence": result.exact_evidence,
            "pair_holdout_evidence": result.pair_holdout_evidence,
            "shared_token_exact_evidence": result.shared_token_exact_evidence,
            "skipped_shared_token_evidence_groups": result.skipped_shared_token_evidence_groups,
            "rescue_plan": result.rescue_plan,
            "evidence_gate_report": result.evidence_gate_report,
            "planned_candidate_pair_visits": result.planned_candidate_pair_visits,
            "evidence_holdout_misses": result.evidence_holdout_misses,
            "effective_edge_budget_bytes": result.effective_edge_budget_bytes,
            "edge_count": result.edge_count,
            "scope_component_counts": {
                "intra": result.scope_components.intra_roots.len(),
                "cross": result.scope_components.cross_roots.len(),
                "chain_pairs": result.scope_components.chain_pair_roots.len(),
            },
            "summary_rows": result.summary_rows,
            "wall_millis": result.wall_millis,
        });
        let json = serde_json::to_string_pretty(&ready)?;
        crate::format::commit_ready(&summary_dir, "metadata-summary.ready", &json)?;
    }
    drop(storage_leases);
    if persistence == MatchPersistence::Durable {
        register_match_artifacts(
            &mut storage,
            features,
            blocking,
            &index_dir,
            &exact_root,
            &out.join("rescue-plan-1"),
            &out.join("recall-plan-1"),
            &runs_dir,
            &out.join("component-snapshots"),
            &summary_dir,
        )?;
    } else if persistence == MatchPersistence::MemoryFirst {
        register_memory_first_match_artifacts(
            &mut storage,
            features,
            blocking,
            &out.join("component-snapshots"),
            &summary_dir,
        )?;
    }
    progress(ProgressEvent::determinate(
        ProgressPhase::CommitArtifacts,
        1,
        1,
        WorkUnit::Items,
        ProgressCounters::default(),
    ));
    Ok(result)
}

fn reserve_pipeline_storage_advisory(
    storage: &mut StorageBroker,
    class: ArtifactClass,
    final_bytes: u64,
    partial_peak_bytes: u64,
    label: &str,
    advisory: &mut dyn FnMut(&str),
) -> Result<Option<StorageLease>, PipelineError> {
    match storage.reserve(class, final_bytes, partial_peak_bytes) {
        Ok(lease) => Ok(Some(lease)),
        Err(StorageLedgerError::InsufficientSpace {
            requested,
            available,
        }) => {
            let message = format!(
                "conservative storage estimate for {label} requests {requested} bytes, but only \
                 {available} bytes are currently available; continuing without a reservation \
                 and relying on actual filesystem writes"
            );
            advisory(&message);
            Ok(None)
        }
        Err(error) => Err(error.into()),
    }
}

fn clear_prior_match_artifacts_for_memory_first(
    storage: &mut StorageBroker,
    out: &Path,
) -> Result<(), PipelineError> {
    // Dependency order matters: remove consumers before their inputs so the
    // ledger can safely retire every artifact from a prior Durable run.
    let paths = [
        "metadata-summary-1",
        "component-snapshots",
        "connectivity-runs",
        "recall-plan-1",
        "rescue-plan-1",
        "exact-islands",
        "index-1",
    ]
    .into_iter()
    .map(|relative| out.join(relative))
    .collect::<Vec<_>>();
    storage.commit_evict(&EvictionPlan {
        paths: paths.clone(),
    })?;
    // Old/unregistered artifacts are not represented in the ledger but must
    // not survive into a memory-first result.
    for path in paths {
        if path.is_dir() {
            std::fs::remove_dir_all(path)?;
        } else if path.is_file() {
            std::fs::remove_file(path)?;
        }
    }
    Ok(())
}

fn register_memory_first_match_artifacts(
    storage: &mut StorageBroker,
    features: &Path,
    blocking: &Path,
    components: &Path,
    summary: &Path,
) -> Result<(), PipelineError> {
    let mut snapshot_files = Vec::new();
    collect_top_level_files(features, &mut snapshot_files)?;
    collect_top_level_files(blocking, &mut snapshot_files)?;
    let snapshot_dependencies = snapshot_files
        .iter()
        .map(|path| path.to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    let components_key = components.to_string_lossy().into_owned();
    storage.register_batch(vec![
        ArtifactRegistration::new(
            components.to_path_buf(),
            ArtifactClass::ComponentSnapshot,
            directory_bytes(components)?,
            0,
            snapshot_dependencies,
        ),
        ArtifactRegistration::new(
            summary.to_path_buf(),
            ArtifactClass::Summary,
            directory_bytes(summary)?,
            0,
            vec![components_key],
        ),
    ])?;
    for lease in storage.pin_batch(
        &[components.to_path_buf(), summary.to_path_buf()],
        "metadata_complete",
    )? {
        lease.persist()?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn register_match_artifacts(
    storage: &mut StorageBroker,
    features: &Path,
    blocking: &Path,
    index: &Path,
    exact: &Path,
    rescue: &Path,
    recall: &Path,
    runs: &Path,
    components: &Path,
    summary: &Path,
) -> Result<(), PipelineError> {
    let mut snapshot_files = Vec::new();
    collect_top_level_files(features, &mut snapshot_files)?;
    collect_top_level_files(blocking, &mut snapshot_files)?;
    let snapshot_dependencies = snapshot_files
        .iter()
        .map(|path| path.to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    let index_key = index.to_string_lossy().into_owned();
    let exact_key = exact.to_string_lossy().into_owned();
    let rescue_key = rescue.to_string_lossy().into_owned();
    let recall_key = recall.to_string_lossy().into_owned();
    let runs_key = runs.to_string_lossy().into_owned();
    let components_key = components.to_string_lossy().into_owned();
    storage.register_batch(vec![
        ArtifactRegistration::new(
            index.to_path_buf(),
            ArtifactClass::Index,
            directory_bytes(index)?,
            0,
            snapshot_dependencies.clone(),
        ),
        ArtifactRegistration::new(
            exact.to_path_buf(),
            ArtifactClass::ExactEvidence,
            directory_bytes(exact)?,
            0,
            snapshot_dependencies,
        ),
        ArtifactRegistration::new(
            rescue.to_path_buf(),
            ArtifactClass::RecallPlan,
            directory_bytes(rescue)?,
            0,
            vec![exact_key.clone()],
        ),
        ArtifactRegistration::new(
            recall.to_path_buf(),
            ArtifactClass::RecallPlan,
            directory_bytes(recall)?,
            0,
            vec![index_key, exact_key, rescue_key.clone()],
        ),
        ArtifactRegistration::new(
            runs.to_path_buf(),
            ArtifactClass::ConnectivityRun,
            directory_bytes(runs)?,
            0,
            vec![recall_key.clone(), rescue_key],
        ),
        ArtifactRegistration::new(
            components.to_path_buf(),
            ArtifactClass::ComponentSnapshot,
            directory_bytes(components)?,
            0,
            vec![runs_key],
        ),
        ArtifactRegistration::new(
            summary.to_path_buf(),
            ArtifactClass::Summary,
            directory_bytes(summary)?,
            0,
            vec![components_key, recall_key],
        ),
    ])?;
    let artifact_paths = [index, exact, rescue, recall, runs, components, summary]
        .into_iter()
        .map(Path::to_path_buf)
        .collect::<Vec<_>>();
    for lease in storage.pin_batch(&artifact_paths, "metadata_complete")? {
        lease.persist()?;
    }
    Ok(())
}

fn collect_top_level_files(path: &Path, files: &mut Vec<PathBuf>) -> Result<(), std::io::Error> {
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            files.push(entry.path());
        }
    }
    Ok(())
}

fn collect_files(path: &Path, files: &mut Vec<PathBuf>) -> Result<(), std::io::Error> {
    if path.is_file() {
        files.push(path.to_path_buf());
        return Ok(());
    }
    for entry in std::fs::read_dir(path)? {
        collect_files(&entry?.path(), files)?;
    }
    Ok(())
}

fn directory_bytes(path: &Path) -> Result<u64, std::io::Error> {
    let mut files = Vec::new();
    collect_files(path, &mut files)?;
    files.into_iter().try_fold(0u64, |total, file| {
        Ok(total.saturating_add(std::fs::metadata(file)?.len()))
    })
}

fn deterministic_sample(n: usize, limit: usize) -> Vec<u32> {
    if limit == 0 || n == 0 {
        return Vec::new();
    }
    let count = n.min(limit);
    (0..count)
        .map(|i| ((i as u128 * n as u128 / count as u128) as u32).min(n as u32 - 1))
        .collect()
}

fn pair_frontier_covers_all_unordered_pairs(
    calibration: &[u32],
    holdout: &[u32],
    atom_count: usize,
) -> bool {
    if atom_count < 2 {
        return true;
    }
    let mut distinct = std::collections::BTreeSet::new();
    for atom in calibration.iter().chain(holdout).copied() {
        if atom as usize >= atom_count {
            return false;
        }
        distinct.insert(atom);
    }
    // Every unordered pair has at least one sampled endpoint exactly when at
    // most one universe atom remains outside the union of both frontiers.
    distinct.len() >= atom_count.saturating_sub(1)
}

fn evidence_scan_is_exhaustive(
    pair_frontier_exhaustive: bool,
    all_shared_groups_selected: bool,
    evaluated_shared_pair_work: u64,
    shared_pair_population: u64,
) -> bool {
    pair_frontier_exhaustive
        && all_shared_groups_selected
        && evaluated_shared_pair_work == shared_pair_population
}

fn shared_token_work_stratum(token_member_offsets: &[u64], token: u32) -> Option<u32> {
    let token = token as usize;
    let end = *token_member_offsets.get(token + 1)?;
    let begin = *token_member_offsets.get(token)?;
    let members = end.checked_sub(begin)?;
    if members < 2 {
        return None;
    }
    let work = members.checked_mul(members - 1)?.checked_div(2)?;
    Some(63 - work.leading_zeros())
}

fn stratified_active_token_sample(token_member_offsets: &[u64], limit: usize) -> Vec<u32> {
    if limit == 0 {
        return Vec::new();
    }
    let mut stratum_counts = [0usize; 64];
    for token in 0..token_member_offsets.len().saturating_sub(1) {
        if let Some(stratum) = shared_token_work_stratum(token_member_offsets, token as u32) {
            stratum_counts[stratum as usize] = stratum_counts[stratum as usize].saturating_add(1);
        }
    }
    let target = limit.min(stratum_counts.iter().sum());
    if target == 0 {
        return Vec::new();
    }

    let keys = stratum_counts
        .iter()
        .enumerate()
        .filter_map(|(stratum, &count)| (count != 0).then_some(stratum))
        .collect::<Vec<_>>();
    let mut allocation_order = Vec::with_capacity(keys.len());
    let (mut low, mut high) = (0usize, keys.len().saturating_sub(1));
    while low <= high && low < keys.len() {
        allocation_order.push(keys[low]);
        if high != low {
            allocation_order.push(keys[high]);
        }
        low = low.saturating_add(1);
        if high == 0 {
            break;
        }
        high -= 1;
    }

    let mut quotas = [0usize; 64];
    let mut remaining = target;
    while remaining > 0 {
        let mut allocated = false;
        for &stratum in &allocation_order {
            let capacity = stratum_counts[stratum];
            let used = quotas[stratum];
            if used >= capacity {
                continue;
            }
            // Adjacent samples are assigned to calibration/holdout. Allocate
            // pairs where possible so both partitions observe this work band.
            let amount = if capacity - used >= 2 && remaining >= 2 {
                2
            } else {
                1
            };
            quotas[stratum] = used + amount;
            remaining -= amount;
            allocated = true;
            if remaining == 0 {
                break;
            }
        }
        if !allocated {
            break;
        }
    }

    // A second sequential pass selects only the evenly spaced sample positions.
    // Peak scratch is O(number of strata + sample limit), rather than retaining
    // every active token identity in one Vec per stratum.
    let mut visited = [0usize; 64];
    let mut selected_by_stratum: [Vec<u32>; 64] =
        std::array::from_fn(|stratum| Vec::with_capacity(quotas[stratum]));
    for token in 0..token_member_offsets.len().saturating_sub(1) {
        let Some(stratum) = shared_token_work_stratum(token_member_offsets, token as u32) else {
            continue;
        };
        let stratum = stratum as usize;
        let position = visited[stratum];
        visited[stratum] = position.saturating_add(1);
        let quota = quotas[stratum];
        if quota == 0 {
            continue;
        }
        let selected_index = selected_by_stratum[stratum].len();
        if selected_index < quota
            && position == selected_index.saturating_mul(stratum_counts[stratum]) / quota
        {
            selected_by_stratum[stratum].push(token as u32);
        }
    }

    let mut sampled = Vec::with_capacity(target);
    for stratum in allocation_order {
        sampled.append(&mut selected_by_stratum[stratum]);
    }
    sampled
}

fn run_edge_count(runs: &[ForestRun]) -> usize {
    runs.iter().map(|run| run.edges.len()).sum()
}

fn reduce_work(runs: &[ForestRun], node_count: u32) -> Result<u64, PipelineError> {
    crate::reduce::planned_reduce_work(run_edge_count(runs) as u64, node_count)
        .map_err(PipelineError::from)
}

fn reduce_component_scopes_parallel(
    scopes: &mut [ComponentScopePlan],
    node_count: u32,
    reduce_total: u64,
    worker_pool: &rayon::ThreadPool,
    progress: &mut impl FnMut(ProgressEvent),
) -> Result<(), PipelineError> {
    let channel_capacity = worker_pool.current_num_threads().max(1).saturating_mul(2);
    let (sender, receiver) = std::sync::mpsc::sync_channel::<u64>(channel_capacity);
    std::thread::scope(|thread_scope| -> Result<(), PipelineError> {
        let producer_sender = sender.clone();
        let producer = thread_scope.spawn(move || {
            worker_pool.install(|| {
                scopes
                    .par_iter_mut()
                    .filter(|scope| scope.roots.is_none())
                    .try_for_each(|scope| -> Result<(), PipelineError> {
                        let mut previous = 0u64;
                        let roots = reduce_components_with_progress(
                            &scope.runs,
                            node_count,
                            |completed, _| {
                                let delta = completed.saturating_sub(previous);
                                previous = completed;
                                if delta != 0 {
                                    let _ = producer_sender.send(delta);
                                }
                            },
                        )?;
                        scope.roots = Some(roots);
                        Ok(())
                    })
            })
        });
        drop(sender);
        let mut reduced_work = 0u64;
        for delta in receiver {
            reduced_work = reduced_work
                .checked_add(delta)
                .ok_or(crate::resource::MemoryError::Overflow)?;
            progress(ProgressEvent::determinate(
                ProgressPhase::ReduceScopes,
                reduced_work.min(reduce_total),
                reduce_total,
                WorkUnit::Items,
                ProgressCounters::default(),
            ));
        }
        producer
            .join()
            .map_err(|_| PipelineError::Parallel("component worker panicked".into()))??;
        if reduced_work != reduce_total {
            return Err(PipelineError::Invariant(format!(
                "component reduction progress mismatch: completed={reduced_work}, planned={reduce_total}"
            )));
        }
        Ok(())
    })
}

fn commit_component_scopes_parallel(
    scopes: &[ComponentScopePlan],
    component_total: u64,
    worker_pool: &rayon::ThreadPool,
    progress: &mut impl FnMut(ProgressEvent),
) -> Result<(), PipelineError> {
    let channel_capacity = worker_pool.current_num_threads().max(1).saturating_mul(2);
    let (sender, receiver) = std::sync::mpsc::sync_channel::<()>(channel_capacity);
    std::thread::scope(|thread_scope| -> Result<(), PipelineError> {
        let producer_sender = sender.clone();
        let producer = thread_scope.spawn(move || {
            worker_pool.install(|| {
                scopes
                    .par_iter()
                    .filter(|scope| scope.needs_rebuild)
                    .try_for_each(|scope| -> Result<(), PipelineError> {
                        let roots = scope.roots.as_deref().ok_or_else(|| {
                            PipelineError::Invariant("missing reduced roots".into())
                        })?;
                        commit_component_roots(&scope.directory, &scope.identity, roots, || {
                            let _ = producer_sender.send(());
                        })?;
                        Ok(())
                    })
            })
        });
        drop(sender);
        let mut committed = 0u64;
        for () in receiver {
            committed = committed.saturating_add(1).min(component_total);
            progress(ProgressEvent::determinate(
                ProgressPhase::CommitComponents,
                committed,
                component_total,
                WorkUnit::Files,
                ProgressCounters::default(),
            ));
        }
        producer
            .join()
            .map_err(|_| PipelineError::Parallel("component commit worker panicked".into()))??;
        if committed != component_total {
            return Err(PipelineError::Invariant(format!(
                "component commit progress mismatch: completed={committed}, planned={component_total}"
            )));
        }
        Ok(())
    })
}

fn commit_runs(
    runs: &[ForestRun],
    directory: &Path,
    on_committed: &mut impl FnMut(),
) -> Result<(), PipelineError> {
    std::fs::create_dir_all(directory)?;
    for (run_id, run) in runs.iter().enumerate() {
        let run_id = u32::try_from(run_id).map_err(|_| crate::reduce::ReduceError::Budget {
            resource: "forest_run_count",
            requested: run_id as u64,
            limit: u32::MAX as u64,
        })?;
        run.commit(directory, run_id)?;
        on_committed();
    }
    Ok(())
}

fn open_run_count(directory: &Path, count: u32) -> Result<Vec<ForestRun>, PipelineError> {
    (0..count)
        .map(|run_id| ForestRun::open(directory, run_id).map_err(PipelineError::from))
        .collect()
}

fn open_connectivity_runs(
    directory: &Path,
    snapshot_fingerprint: &str,
    connectivity_plan_digest: &str,
    chain_count: usize,
) -> Result<Option<RecoveredConnectivity>, PipelineError> {
    let ready = directory.join("connectivity.ready");
    if !ready.is_file() {
        return Ok(None);
    }
    let manifest: ConnectivityRunManifest = serde_json::from_slice(&std::fs::read(ready)?)?;
    let expected_pairs = chain_count.saturating_mul(chain_count.saturating_sub(1)) / 2;
    if manifest.revision != CONNECTIVITY_RUN_REVISION
        || manifest.schema_revision != crate::scoring::MATCH_SEMANTICS_REVISION
        || manifest.snapshot_fingerprint != snapshot_fingerprint
        || manifest.connectivity_plan_digest != connectivity_plan_digest
        || manifest.chain_count != chain_count
        || manifest.pair_runs.len() != expected_pairs
    {
        return Ok(None);
    }
    let intra = open_run_count(&directory.join("intra"), manifest.intra_runs)?;
    let cross = open_run_count(&directory.join("cross"), manifest.cross_runs)?;
    let mut pairs = Vec::with_capacity(expected_pairs);
    let mut pair_index = 0usize;
    for left in 0..chain_count {
        for right in left + 1..chain_count {
            pairs.push(open_run_count(
                &directory.join(format!("pair-{left}-{right}")),
                manifest.pair_runs[pair_index],
            )?);
            pair_index += 1;
        }
    }
    Ok(Some(RecoveredConnectivity {
        intra,
        cross,
        pairs,
        index_metrics: manifest.index_metrics,
        candidate_pair_visits: manifest.candidate_pair_visits,
        accepted_edge_count: manifest.accepted_edge_count,
    }))
}

fn commit_connectivity_runs(
    directory: &Path,
    commit: ConnectivityCommit<'_>,
    progress: &mut impl FnMut(ProgressEvent),
) -> Result<(), PipelineError> {
    let total = commit
        .intra
        .len()
        .saturating_add(commit.cross.len())
        .saturating_add(commit.pairs.iter().map(Vec::len).sum::<usize>())
        .saturating_add(1) as u64;
    let mut completed = 0u64;
    progress(
        ProgressEvent::determinate(
            ProgressPhase::CommitConnectivityRuns,
            completed,
            total,
            WorkUnit::Files,
            ProgressCounters::default(),
        )
        .with_plan(WorkClass::ArtifactFiles, crate::progress::TotalKind::Exact),
    );
    let mut on_committed = || {
        completed += 1;
        progress(
            ProgressEvent::determinate(
                ProgressPhase::CommitConnectivityRuns,
                completed,
                total,
                WorkUnit::Files,
                ProgressCounters::default(),
            )
            .with_plan(WorkClass::ArtifactFiles, crate::progress::TotalKind::Exact),
        );
    };
    commit_runs(commit.intra, &directory.join("intra"), &mut on_committed)?;
    commit_runs(commit.cross, &directory.join("cross"), &mut on_committed)?;
    let mut pair_counts = Vec::with_capacity(commit.pairs.len());
    for left in 0..commit.chain_count {
        for right in left + 1..commit.chain_count {
            let index = chain_pair_index(left, right, commit.chain_count);
            commit_runs(
                &commit.pairs[index],
                &directory.join(format!("pair-{left}-{right}")),
                &mut on_committed,
            )?;
            pair_counts.push(u32::try_from(commit.pairs[index].len()).map_err(|_| {
                crate::reduce::ReduceError::Budget {
                    resource: "forest_run_count",
                    requested: commit.pairs[index].len() as u64,
                    limit: u32::MAX as u64,
                }
            })?);
        }
    }
    let manifest = ConnectivityRunManifest {
        revision: CONNECTIVITY_RUN_REVISION,
        schema_revision: crate::scoring::MATCH_SEMANTICS_REVISION,
        snapshot_fingerprint: commit.snapshot_fingerprint.to_owned(),
        connectivity_plan_digest: commit.connectivity_plan_digest.to_owned(),
        chain_count: commit.chain_count,
        intra_runs: u32::try_from(commit.intra.len()).map_err(|_| {
            crate::reduce::ReduceError::Budget {
                resource: "forest_run_count",
                requested: commit.intra.len() as u64,
                limit: u32::MAX as u64,
            }
        })?,
        cross_runs: u32::try_from(commit.cross.len()).map_err(|_| {
            crate::reduce::ReduceError::Budget {
                resource: "forest_run_count",
                requested: commit.cross.len() as u64,
                limit: u32::MAX as u64,
            }
        })?,
        pair_runs: pair_counts,
        index_metrics: commit.index_metrics.clone(),
        candidate_pair_visits: commit.candidate_pair_visits,
        accepted_edge_count: commit.accepted_edge_count,
    };
    crate::format::commit_ready(
        directory,
        "connectivity.ready",
        &serde_json::to_string_pretty(&manifest)?,
    )?;
    on_committed();
    Ok(())
}

fn connectivity_plan_digest(rescue: &RescuePlan) -> Result<String, serde_json::Error> {
    let mut hash = Sha256::new();
    hash.update(serde_json::to_vec(rescue)?);
    Ok(hash
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn shared_group_sketches(
    features: &crate::encode::FeatureView,
    sources: &[u32],
) -> Vec<crate::blocking::AtomSketch> {
    let payloads = sources
        .iter()
        .map(|&source| features.source_to_payload[source as usize])
        .collect::<Vec<_>>();
    build_base_equivalent_atom_sketches_from_feature_view_parallel(features, &payloads)
}

fn max_shared_group_index_bytes_with_progress(
    snapshot: &MetadataSnapshot,
    mut progress: impl FnMut(ProgressEvent),
) -> Result<u64, PipelineError> {
    const PROGRESS_CHUNK: u64 = 65_536;
    let features = snapshot.features();
    let total = features.token_member_offsets.last().copied().unwrap_or(0);
    progress(ProgressEvent::determinate(
        ProgressPhase::PlanSharedTokenPairs,
        0,
        total,
        WorkUnit::Items,
        ProgressCounters::default(),
    ));
    let mut maximum = 0u64;
    let mut completed = 0u64;
    let mut last_reported = 0u64;
    for (token, window) in features.token_member_offsets.windows(2).enumerate() {
        let mut bytes = 0u64;
        for member in window[0] as usize..window[1] as usize {
            let source = features.token_member_sources[member] as usize;
            let payload = features.source_to_payload[source] as usize;
            let terms = (features.payload_template_offsets[payload + 1]
                - features.payload_template_offsets[payload])
                .checked_add(
                    features.payload_content_offsets[payload + 1]
                        - features.payload_content_offsets[payload],
                )
                .ok_or(crate::resource::MemoryError::Overflow)?;
            bytes = bytes
                .checked_add(terms.saturating_mul(8).saturating_add(256))
                .ok_or(crate::resource::MemoryError::Overflow)?;
            completed = completed.saturating_add(1);
            if completed.saturating_sub(last_reported) >= PROGRESS_CHUNK {
                last_reported = completed;
                progress(ProgressEvent::determinate(
                    ProgressPhase::PlanSharedTokenPairs,
                    completed,
                    total,
                    WorkUnit::Items,
                    ProgressCounters {
                        groups: token as u64,
                        ..ProgressCounters::default()
                    },
                ));
            }
        }
        maximum = maximum.max(bytes);
        last_reported = completed;
        progress(ProgressEvent::determinate(
            ProgressPhase::PlanSharedTokenPairs,
            completed,
            total,
            WorkUnit::Items,
            ProgressCounters {
                groups: token as u64 + 1,
                ..ProgressCounters::default()
            },
        ));
    }
    Ok(maximum)
}

fn checked_add_pairs(total: u64, members: u64) -> Result<u64, PipelineError> {
    let pairs = members
        .checked_mul(members.saturating_sub(1))
        .and_then(|value| value.checked_div(2))
        .ok_or(crate::resource::MemoryError::Overflow)?;
    total
        .checked_add(pairs)
        .ok_or(crate::resource::MemoryError::Overflow.into())
}
fn atom_contracts(s: &MetadataSnapshot, a: u32) -> &[u32] {
    let f = s.features();
    &f.fallback_atom_contracts[f.fallback_atom_offsets[a as usize] as usize
        ..f.fallback_atom_offsets[a as usize + 1] as usize]
}

fn atom_payload(s: &MetadataSnapshot, atom: u32) -> u32 {
    let features = s.features();
    let contract =
        features.fallback_atom_contracts[features.fallback_atom_offsets[atom as usize] as usize];
    features.contract_payload[contract as usize]
}

#[cfg(test)]
thread_local! {
    static CATALOG_ATOM_SCORE_CALLS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

fn catalog_atom_score(
    snapshot: &MetadataSnapshot,
    left_atom: u32,
    right_atom: u32,
    template_match_proven: bool,
) -> bool {
    #[cfg(test)]
    CATALOG_ATOM_SCORE_CALLS.with(|calls| calls.set(calls.get().saturating_add(1)));
    let features = snapshot.features();
    let left_payload = atom_payload(snapshot, left_atom);
    let right_payload = atom_payload(snapshot, right_atom);
    if template_match_proven {
        crate::scoring::content_matches(features, left_payload, right_payload)
    } else {
        score_pair(features, left_payload, right_payload) == PairScoreDecision::ExactMatch
    }
}

#[cfg(test)]
fn expand_catalog_atom_pair(
    snapshot: &MetadataSnapshot,
    left_atom: u32,
    right_atom: u32,
    emit: impl FnMut(u32, u32),
) -> Result<u64, PipelineError> {
    let mut scratch = CatalogExpansionScratch::new(snapshot.chain_names().len());
    expand_catalog_atom_pair_streaming(snapshot, left_atom, right_atom, false, &mut scratch, emit)
}

fn expand_catalog_atom_pair_streaming(
    snapshot: &MetadataSnapshot,
    left_atom: u32,
    right_atom: u32,
    template_match_proven: bool,
    scratch: &mut CatalogExpansionScratch,
    mut emit: impl FnMut(u32, u32),
) -> Result<u64, PipelineError> {
    let left_contracts = atom_contracts(snapshot, left_atom);
    let right_contracts = atom_contracts(snapshot, right_atom);
    if !catalog_atom_score(snapshot, left_atom, right_atom, template_match_proven) {
        return Ok(0);
    }
    let work = (left_contracts.len() as u64)
        .checked_mul(right_contracts.len() as u64)
        .ok_or(crate::resource::MemoryError::Overflow)?;
    let features = snapshot.features();
    scratch.partition(features, left_contracts, right_contracts);
    for left_chain in 0..scratch.left_by_chain.len() {
        for right_chain in 0..scratch.right_by_chain.len() {
            let left = &scratch.left_by_chain[left_chain];
            let right = &scratch.right_by_chain[right_chain];
            if left.is_empty() || right.is_empty() {
                continue;
            }
            const FOREST_FAST_PATH_MIN_PAIR_WORK: usize = 64;
            let bucket_pair_work = left.len().saturating_mul(right.len());
            if bucket_pair_work >= FOREST_FAST_PATH_MIN_PAIR_WORK
                && CatalogExpansionScratch::retained_tokens_disjoint(
                    &mut scratch.retained_tokens,
                    features,
                    left,
                    right,
                )
            {
                emit_complete_bipartite_forest(left, right, &mut emit);
                continue;
            }
            for &left_contract in left {
                for &right_contract in right {
                    if left_contract != right_contract
                        && !contracts_share_retained_token(features, left_contract, right_contract)
                    {
                        emit(left_contract, right_contract);
                    }
                }
            }
        }
    }
    Ok(work)
}

fn emit_complete_bipartite_forest(left: &[u32], right: &[u32], emit: &mut impl FnMut(u32, u32)) {
    let Some((&left_root, left_tail)) = left.split_first() else {
        return;
    };
    let Some((&right_root, _)) = right.split_first() else {
        return;
    };
    for &right_contract in right {
        emit(left_root, right_contract);
    }
    for &left_contract in left_tail {
        emit(left_contract, right_root);
    }
}

#[cfg(test)]
fn reset_catalog_atom_score_count() {
    CATALOG_ATOM_SCORE_CALLS.with(|calls| calls.set(0));
}

#[cfg(test)]
fn catalog_atom_score_count() -> u64 {
    CATALOG_ATOM_SCORE_CALLS.with(std::cell::Cell::get)
}

struct RescueExecutionPlan {
    atom_score_visits: u64,
    contract_expansion_visits: u64,
    shared_score_visits: u64,
    matched_atom_pairs: Vec<(u32, u32)>,
    matched_shared_edges: Vec<(u32, u32)>,
}

impl RescueExecutionPlan {
    fn total_visits(&self) -> u64 {
        self.atom_score_visits
            .saturating_add(self.shared_score_visits)
            .saturating_add(self.contract_expansion_visits)
            .saturating_add(self.matched_shared_edges.len() as u64)
    }

    fn execution_work(&self) -> u64 {
        self.contract_expansion_visits
            .saturating_add(self.matched_shared_edges.len() as u64)
    }
}

struct AdmittedRescueExecutionPlan {
    plan: RescueExecutionPlan,
    _match_memory: MemoryLease,
}

struct SharedRescueGroup {
    token_id: u32,
    seed_contracts: Box<[u32]>,
}

const RESCUE_MATCH_CHUNK_PAIRS: usize = 4_096;
const RESCUE_SCORE_TILE: usize = 65_536;

struct RescueMatchChunk {
    pairs: Vec<(u32, u32)>,
    _memory: MemoryLease,
}

impl RescueMatchChunk {
    fn new(memory: &MemoryBroker) -> Result<Self, PipelineError> {
        let bytes = (RESCUE_MATCH_CHUNK_PAIRS as u64)
            .checked_mul(std::mem::size_of::<(u32, u32)>() as u64)
            .and_then(|value| value.checked_add(std::mem::size_of::<RescueMatchChunk>() as u64))
            .ok_or(crate::resource::MemoryError::Overflow)?;
        let lease = memory.reserve(bytes)?;
        Ok(Self {
            pairs: Vec::with_capacity(RESCUE_MATCH_CHUNK_PAIRS),
            _memory: lease,
        })
    }
}

enum RescuePlanMessage {
    Work(u64),
    AtomMatches(RescueMatchChunk),
    SharedMatches(RescueMatchChunk),
    RowDone(u64),
    Error(PipelineError),
}

fn record_rescue_match(
    chunk: &mut Option<RescueMatchChunk>,
    pair: (u32, u32),
    memory: &MemoryBroker,
    sender: &std::sync::mpsc::SyncSender<RescuePlanMessage>,
    shared: bool,
) -> Result<(), PipelineError> {
    if chunk.is_none() {
        *chunk = Some(RescueMatchChunk::new(memory)?);
    }
    let current = chunk.as_mut().expect("rescue match chunk initialized");
    current.pairs.push(pair);
    if current.pairs.len() == RESCUE_MATCH_CHUNK_PAIRS {
        let full = chunk.take().expect("full rescue match chunk");
        let message = if shared {
            RescuePlanMessage::SharedMatches(full)
        } else {
            RescuePlanMessage::AtomMatches(full)
        };
        sender
            .send(message)
            .map_err(|_| PipelineError::Parallel("rescue consumer stopped".into()))?;
    }
    Ok(())
}

fn flush_rescue_match_chunk(
    chunk: &mut Option<RescueMatchChunk>,
    sender: &std::sync::mpsc::SyncSender<RescuePlanMessage>,
    shared: bool,
) -> Result<(), PipelineError> {
    let Some(chunk) = chunk.take() else {
        return Ok(());
    };
    let message = if shared {
        RescuePlanMessage::SharedMatches(chunk)
    } else {
        RescuePlanMessage::AtomMatches(chunk)
    };
    sender
        .send(message)
        .map_err(|_| PipelineError::Parallel("rescue consumer stopped".into()))
}

fn record_rescue_plan_work(
    pending_work: &mut u64,
    progress_chunk: u64,
    sender: &std::sync::mpsc::SyncSender<RescuePlanMessage>,
) {
    *pending_work = pending_work.saturating_add(1);
    let progress_chunk = progress_chunk.max(1);
    if *pending_work > progress_chunk {
        *pending_work -= progress_chunk;
        let _ = sender.send(RescuePlanMessage::Work(progress_chunk));
    }
}

fn take_rescue_plan_work(pending_work: &mut u64) -> u64 {
    std::mem::take(pending_work)
}

fn build_rescue_execution_plan(
    snapshot: &MetadataSnapshot,
    rescue: &RescuePlan,
    lanes: usize,
    memory: &MemoryBroker,
    mut progress: impl FnMut(ProgressEvent),
) -> Result<AdmittedRescueExecutionPlan, PipelineError> {
    const PROGRESS_CHUNK: u64 = 65_536;
    let atom_count = snapshot.atom_count() as u32;
    let mut prepare_completed = 0u64;
    let mut prepare_reported = 0u64;
    progress(ProgressEvent::indeterminate(
        ProgressPhase::PrepareRescuePairs,
        0,
        WorkUnit::Items,
        ProgressCounters::default(),
    ));
    let rescue_fixed_bytes = (atom_count as u64)
        .saturating_mul(std::mem::size_of::<u32>() as u64)
        .saturating_add(atom_count as u64);
    let rescue_cache_bytes = (lanes as u64)
        .saturating_mul(MAX_RESCUE_PAYLOAD_CACHE_ENTRIES as u64)
        .saturating_mul(RESCUE_PAYLOAD_CACHE_ENTRY_BYTES);
    let contract_count = snapshot.contract_count();
    let rescue_seed_index_bytes = (rescue.shared_contracts.len() as u64)
        .saturating_mul(RESCUE_SEED_INDEX_ENTRY_BYTES)
        .saturating_mul(2)
        .saturating_add(
            (rescue.shared_edges.len() as u64)
                .saturating_mul(std::mem::size_of::<(u32, u32)>() as u64),
        );
    let _rescue_memory = memory.reserve(
        rescue_fixed_bytes
            .saturating_add(contract_count as u64)
            .saturating_add(rescue_cache_bytes)
            .saturating_add(rescue_seed_index_bytes),
    )?;
    for &left_atom in &rescue.pair_atoms {
        if left_atom >= atom_count {
            return Err(PipelineError::Invariant(format!(
                "rescue atom {left_atom} is outside atom universe {atom_count}"
            )));
        }
        prepare_completed = prepare_completed.saturating_add(1);
        if prepare_completed.saturating_sub(prepare_reported) >= PROGRESS_CHUNK {
            prepare_reported = prepare_completed;
            progress(ProgressEvent::indeterminate(
                ProgressPhase::PrepareRescuePairs,
                prepare_completed,
                WorkUnit::Items,
                ProgressCounters::default(),
            ));
        }
    }
    let rescue_atom_count = rescue.pair_atoms.len() as u64;
    let atom_score_visits = rescue_atom_count
        .checked_mul(u64::from(atom_count.saturating_sub(1)))
        .and_then(|visits| {
            rescue_atom_count
                .checked_mul(rescue_atom_count.saturating_sub(1))
                .and_then(|duplicates| duplicates.checked_div(2))
                .and_then(|duplicates| visits.checked_sub(duplicates))
        })
        .ok_or(crate::resource::MemoryError::Overflow)?;
    let features = snapshot.features();
    let mut shared_contract_mask = vec![false; contract_count];
    for &contract in &rescue.shared_contracts {
        if contract as usize >= contract_count {
            return Err(PipelineError::Invariant(format!(
                "shared rescue contract {contract} is outside contract universe {contract_count}"
            )));
        }
        shared_contract_mask[contract as usize] = true;
        prepare_completed = prepare_completed.saturating_add(1);
        if prepare_completed.saturating_sub(prepare_reported) >= PROGRESS_CHUNK {
            prepare_reported = prepare_completed;
            progress(ProgressEvent::indeterminate(
                ProgressPhase::PrepareRescuePairs,
                prepare_completed,
                WorkUnit::Items,
                ProgressCounters::default(),
            ));
        }
    }
    for &(left, right) in &rescue.shared_edges {
        if left == right || left as usize >= contract_count || right as usize >= contract_count {
            return Err(PipelineError::Invariant(format!(
                "direct shared rescue edge ({left}, {right}) is outside contract universe \
                 {contract_count} or is a self-edge"
            )));
        }
    }
    let mut shared_score_visits = 0u64;
    let mut shared_group_count = 0u64;
    let mut shared_seed_occurrences = 0u64;
    for token_id in 0..features.token_member_offsets.len().saturating_sub(1) {
        let begin = features.token_member_offsets[token_id] as usize;
        let end = features.token_member_offsets[token_id + 1] as usize;
        let contracts = &features.token_member_contracts[begin..end];
        let present = if contracts.len() >= SHARED_LOCAL_ROUTING_MIN_MEMBERS {
            contracts
                .iter()
                .filter(|&&contract| shared_contract_mask[contract as usize])
                .count() as u64
        } else {
            0
        };
        for _ in contracts {
            prepare_completed = prepare_completed.saturating_add(1);
            if prepare_completed.saturating_sub(prepare_reported) >= PROGRESS_CHUNK {
                prepare_reported = prepare_completed;
                progress(ProgressEvent::indeterminate(
                    ProgressPhase::PrepareRescuePairs,
                    prepare_completed,
                    WorkUnit::Items,
                    ProgressCounters {
                        groups: shared_group_count,
                        ..ProgressCounters::default()
                    },
                ));
            }
        }
        if present == 0 {
            continue;
        }
        shared_group_count = shared_group_count
            .checked_add(1)
            .ok_or(crate::resource::MemoryError::Overflow)?;
        shared_seed_occurrences = shared_seed_occurrences
            .checked_add(present)
            .ok_or(crate::resource::MemoryError::Overflow)?;
        let members = contracts.len() as u64;
        let visits = present
            .checked_mul(members.saturating_sub(1))
            .and_then(|visits| {
                present
                    .checked_mul(present.saturating_sub(1))
                    .and_then(|duplicates| duplicates.checked_div(2))
                    .and_then(|duplicates| visits.checked_sub(duplicates))
            })
            .ok_or(crate::resource::MemoryError::Overflow)?;
        shared_score_visits = shared_score_visits
            .checked_add(visits)
            .ok_or(crate::resource::MemoryError::Overflow)?;
    }
    let shared_group_bytes = shared_group_count
        .checked_mul(std::mem::size_of::<SharedRescueGroup>() as u64)
        .and_then(|bytes| {
            shared_seed_occurrences
                .checked_mul(std::mem::size_of::<u32>() as u64)
                .and_then(|seed_bytes| bytes.checked_add(seed_bytes))
        })
        .ok_or(crate::resource::MemoryError::Overflow)?;
    let _shared_group_memory = memory.reserve(shared_group_bytes)?;
    let shared_group_capacity =
        usize::try_from(shared_group_count).map_err(|_| crate::resource::MemoryError::Overflow)?;
    let mut shared_rescue_groups = Vec::with_capacity(shared_group_capacity);
    for token_id in 0..features.token_member_offsets.len().saturating_sub(1) {
        let begin = features.token_member_offsets[token_id] as usize;
        let end = features.token_member_offsets[token_id + 1] as usize;
        let contracts = &features.token_member_contracts[begin..end];
        if contracts.len() < SHARED_LOCAL_ROUTING_MIN_MEMBERS {
            continue;
        }
        let seed_count = contracts
            .iter()
            .filter(|&&contract| shared_contract_mask[contract as usize])
            .count();
        let mut seed_contracts = Vec::with_capacity(seed_count);
        seed_contracts.extend(
            contracts
                .iter()
                .copied()
                .filter(|&contract| shared_contract_mask[contract as usize]),
        );
        if !seed_contracts.is_empty() {
            shared_rescue_groups.push(SharedRescueGroup {
                token_id: token_id as u32,
                seed_contracts: seed_contracts.into_boxed_slice(),
            });
        }
    }
    progress(ProgressEvent::indeterminate(
        ProgressPhase::PrepareRescuePairs,
        prepare_completed,
        WorkUnit::Items,
        ProgressCounters {
            groups: shared_rescue_groups.len() as u64,
            ..ProgressCounters::default()
        },
    ));
    let score_visits = atom_score_visits
        .checked_add(shared_score_visits)
        .ok_or(crate::resource::MemoryError::Overflow)?;
    progress(ProgressEvent::determinate(
        ProgressPhase::PlanRescuePairs,
        0,
        score_visits,
        WorkUnit::Pairs,
        ProgressCounters::default(),
    ));

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(lanes.max(1))
        .thread_name(|index| format!("metadata-rescue-{index}"))
        .build()
        .map_err(|error| PipelineError::Parallel(error.to_string()))?;
    let (sender, receiver) = std::sync::mpsc::sync_channel(lanes.max(1) * 2);
    let mut rescue_atom_mask = vec![false; atom_count as usize];
    for &atom in &rescue.pair_atoms {
        rescue_atom_mask[atom as usize] = true;
    }
    let atom_payloads = (0..atom_count)
        .map(|atom| atom_payload(snapshot, atom))
        .collect::<Vec<_>>();
    let cancelled = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(
        |scope| -> Result<AdmittedRescueExecutionPlan, PipelineError> {
            let producer_sender = sender.clone();
            let rescue_pool = &pool;
            let producer_cancelled = &cancelled;
            let producer = scope.spawn(move || {
                rescue_pool.install(|| {
                    rayon::join(
                        || {
                            rescue.pair_atoms.par_iter().for_each(|&left_atom| {
                                let left_payload = atom_payloads[left_atom as usize];
                                let tile_count = (atom_count as usize).div_ceil(RESCUE_SCORE_TILE);
                                (0..tile_count).into_par_iter().for_each(|tile| {
                                    if producer_cancelled.load(std::sync::atomic::Ordering::Acquire)
                                    {
                                        return;
                                    }
                                    let begin = tile.saturating_mul(RESCUE_SCORE_TILE);
                                    let end = begin
                                        .saturating_add(RESCUE_SCORE_TILE)
                                        .min(atom_count as usize);
                                    let mut pending_work = 0u64;
                                    let mut matches = None;
                                    let mut payload_scores = HashMap::<u32, bool>::new();
                                    for right_atom in begin as u32..end as u32 {
                                        if producer_cancelled
                                            .load(std::sync::atomic::Ordering::Acquire)
                                        {
                                            return;
                                        }
                                        if left_atom == right_atom
                                            || (rescue_atom_mask[right_atom as usize]
                                                && right_atom < left_atom)
                                        {
                                            continue;
                                        }
                                        let right_payload = atom_payloads[right_atom as usize];
                                        let exact_match = bounded_payload_match(
                                            &mut payload_scores,
                                            snapshot.features(),
                                            left_payload,
                                            right_payload,
                                        );
                                        if exact_match {
                                            if let Err(error) = record_rescue_match(
                                                &mut matches,
                                                (left_atom, right_atom),
                                                memory,
                                                &producer_sender,
                                                false,
                                            ) {
                                                producer_cancelled.store(
                                                    true,
                                                    std::sync::atomic::Ordering::Release,
                                                );
                                                let _ = producer_sender
                                                    .send(RescuePlanMessage::Error(error));
                                                return;
                                            }
                                        }
                                        record_rescue_plan_work(
                                            &mut pending_work,
                                            PROGRESS_CHUNK,
                                            &producer_sender,
                                        );
                                    }
                                    if let Err(error) = flush_rescue_match_chunk(
                                        &mut matches,
                                        &producer_sender,
                                        false,
                                    ) {
                                        producer_cancelled
                                            .store(true, std::sync::atomic::Ordering::Release);
                                        let _ =
                                            producer_sender.send(RescuePlanMessage::Error(error));
                                        return;
                                    }
                                    let _ = producer_sender.send(RescuePlanMessage::RowDone(
                                        take_rescue_plan_work(&mut pending_work),
                                    ));
                                });
                            });
                        },
                        || {
                            shared_rescue_groups.par_iter().for_each(|group| {
                                let begin =
                                    features.token_member_offsets[group.token_id as usize] as usize;
                                let end = features.token_member_offsets[group.token_id as usize + 1]
                                    as usize;
                                let contracts = &features.token_member_contracts[begin..end];
                                let sources = &features.token_member_sources[begin..end];
                                group.seed_contracts.par_iter().for_each(|&seed_contract| {
                                    let seed_index = contracts
                                        .binary_search(&seed_contract)
                                        .expect("rescue seed came from this shared-token group");
                                    let seed_payload =
                                        features.source_to_payload[sources[seed_index] as usize];
                                    contracts
                                        .par_chunks(RESCUE_SCORE_TILE)
                                        .zip(sources.par_chunks(RESCUE_SCORE_TILE))
                                        .for_each(|(contracts, sources)| {
                                            if producer_cancelled
                                                .load(std::sync::atomic::Ordering::Acquire)
                                            {
                                                return;
                                            }
                                            let mut pending_work = 0u64;
                                            let mut matches = None;
                                            let mut payload_scores = HashMap::<u32, bool>::new();
                                            for (&contract, &source) in
                                                contracts.iter().zip(sources)
                                            {
                                                if producer_cancelled
                                                    .load(std::sync::atomic::Ordering::Acquire)
                                                {
                                                    return;
                                                }
                                                if contract == seed_contract
                                                    || (shared_contract_mask[contract as usize]
                                                        && contract < seed_contract)
                                                {
                                                    continue;
                                                }
                                                let payload =
                                                    features.source_to_payload[source as usize];
                                                let exact_match = bounded_payload_match(
                                                    &mut payload_scores,
                                                    features,
                                                    seed_payload,
                                                    payload,
                                                );
                                                if exact_match {
                                                    if let Err(error) = record_rescue_match(
                                                        &mut matches,
                                                        (seed_contract, contract),
                                                        memory,
                                                        &producer_sender,
                                                        true,
                                                    ) {
                                                        producer_cancelled.store(
                                                            true,
                                                            std::sync::atomic::Ordering::Release,
                                                        );
                                                        let _ = producer_sender
                                                            .send(RescuePlanMessage::Error(error));
                                                        return;
                                                    }
                                                }
                                                record_rescue_plan_work(
                                                    &mut pending_work,
                                                    PROGRESS_CHUNK,
                                                    &producer_sender,
                                                );
                                            }
                                            if let Err(error) = flush_rescue_match_chunk(
                                                &mut matches,
                                                &producer_sender,
                                                true,
                                            ) {
                                                producer_cancelled.store(
                                                    true,
                                                    std::sync::atomic::Ordering::Release,
                                                );
                                                let _ = producer_sender
                                                    .send(RescuePlanMessage::Error(error));
                                                return;
                                            }
                                            let _ =
                                                producer_sender.send(RescuePlanMessage::RowDone(
                                                    take_rescue_plan_work(&mut pending_work),
                                                ));
                                        });
                                });
                            });
                        },
                    );
                });
            });
            drop(sender);

            let mut completed = 0u64;
            let mut atom_chunks = Vec::<RescueMatchChunk>::new();
            let mut shared_chunks = Vec::<RescueMatchChunk>::new();
            let mut matched_atom_count = 0usize;
            let mut matched_shared_count = rescue.shared_edges.len();
            let mut first_error = None;
            for message in receiver {
                match message {
                    RescuePlanMessage::Work(work) => {
                        completed = completed.saturating_add(work).min(score_visits);
                    }
                    RescuePlanMessage::AtomMatches(chunk) => {
                        matched_atom_count = matched_atom_count.saturating_add(chunk.pairs.len());
                        atom_chunks.push(chunk);
                    }
                    RescuePlanMessage::SharedMatches(chunk) => {
                        matched_shared_count =
                            matched_shared_count.saturating_add(chunk.pairs.len());
                        shared_chunks.push(chunk);
                    }
                    RescuePlanMessage::RowDone(work) => {
                        completed = completed.saturating_add(work).min(score_visits);
                    }
                    RescuePlanMessage::Error(error) => {
                        if first_error.is_none() {
                            first_error = Some(error);
                        }
                    }
                }
                progress(ProgressEvent::determinate(
                    ProgressPhase::PlanRescuePairs,
                    completed,
                    score_visits,
                    WorkUnit::Pairs,
                    ProgressCounters {
                        scored: completed,
                        matched: matched_atom_count.saturating_add(matched_shared_count) as u64,
                        ..ProgressCounters::default()
                    },
                ));
            }
            producer
                .join()
                .map_err(|_| PipelineError::Parallel("rescue planner panicked".into()))?;
            if let Some(error) = first_error {
                return Err(error);
            }
            let total_matches = matched_atom_count
                .checked_add(matched_shared_count)
                .ok_or(crate::resource::MemoryError::Overflow)?;
            // Flattening temporarily overlaps admitted chunks and the final vectors.
            // Two pair-widths per result conservatively cover Vec capacity slack.
            let final_match_bytes = (total_matches as u64)
                .checked_mul(std::mem::size_of::<(u32, u32)>() as u64)
                .and_then(|bytes| bytes.checked_mul(2))
                .ok_or(crate::resource::MemoryError::Overflow)?;
            let match_memory = memory.reserve(final_match_bytes)?;
            let mut matched_atom_pairs = Vec::with_capacity(matched_atom_count);
            for chunk in atom_chunks {
                matched_atom_pairs.extend(chunk.pairs);
            }
            let mut matched_shared_edges = Vec::with_capacity(matched_shared_count);
            matched_shared_edges.extend_from_slice(&rescue.shared_edges);
            for chunk in shared_chunks {
                matched_shared_edges.extend(chunk.pairs);
            }
            progress(ProgressEvent::indeterminate(
                ProgressPhase::FinalizeRescuePlan,
                0,
                WorkUnit::Items,
                ProgressCounters {
                    matched: matched_atom_pairs
                        .len()
                        .saturating_add(matched_shared_edges.len())
                        as u64,
                    ..ProgressCounters::default()
                },
            ));
            pool.install(|| {
                rayon::join(
                    || matched_atom_pairs.par_sort_unstable(),
                    || matched_shared_edges.par_sort_unstable(),
                )
            });
            matched_atom_pairs.dedup();
            matched_shared_edges.dedup();
            let mut finalize_completed = matched_atom_pairs
                .len()
                .saturating_add(matched_shared_edges.len())
                as u64;
            progress(ProgressEvent::indeterminate(
                ProgressPhase::FinalizeRescuePlan,
                finalize_completed,
                WorkUnit::Items,
                ProgressCounters {
                    matched: finalize_completed,
                    ..ProgressCounters::default()
                },
            ));

            let mut contract_expansion_visits = 0u64;
            let mut pending_finalize = 0u64;
            for &(left_atom, right_atom) in &matched_atom_pairs {
                contract_expansion_visits = contract_expansion_visits
                    .checked_add(
                        (atom_contracts(snapshot, left_atom).len() as u64)
                            .checked_mul(atom_contracts(snapshot, right_atom).len() as u64)
                            .ok_or(crate::resource::MemoryError::Overflow)?,
                    )
                    .ok_or(crate::resource::MemoryError::Overflow)?;
                pending_finalize = pending_finalize.saturating_add(1);
                if pending_finalize == PROGRESS_CHUNK {
                    finalize_completed = finalize_completed.saturating_add(pending_finalize);
                    pending_finalize = 0;
                    progress(ProgressEvent::indeterminate(
                        ProgressPhase::FinalizeRescuePlan,
                        finalize_completed,
                        WorkUnit::Items,
                        ProgressCounters {
                            matched: matched_atom_pairs
                                .len()
                                .saturating_add(matched_shared_edges.len())
                                as u64,
                            ..ProgressCounters::default()
                        },
                    ));
                }
            }
            if pending_finalize != 0 {
                finalize_completed = finalize_completed.saturating_add(pending_finalize);
            }
            progress(ProgressEvent::indeterminate(
                ProgressPhase::FinalizeRescuePlan,
                finalize_completed,
                WorkUnit::Items,
                ProgressCounters {
                    matched: matched_atom_pairs
                        .len()
                        .saturating_add(matched_shared_edges.len())
                        as u64,
                    ..ProgressCounters::default()
                },
            ));
            Ok(AdmittedRescueExecutionPlan {
                plan: RescueExecutionPlan {
                    atom_score_visits,
                    contract_expansion_visits,
                    shared_score_visits,
                    matched_atom_pairs,
                    matched_shared_edges,
                },
                _match_memory: match_memory,
            })
        },
    )
}

fn bounded_payload_match(
    cache: &mut HashMap<u32, bool>,
    features: &crate::encode::FeatureView,
    left_payload: u32,
    right_payload: u32,
) -> bool {
    if let Some(&decision) = cache.get(&right_payload) {
        return decision;
    }
    let decision =
        score_pair(features, left_payload, right_payload) == PairScoreDecision::ExactMatch;
    if cache.len() < MAX_RESCUE_PAYLOAD_CACHE_ENTRIES {
        cache.insert(right_payload, decision);
    }
    decision
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RescueExpansionTile {
    left_begin: usize,
    left_end: usize,
    right_begin: usize,
    right_end: usize,
}

#[cfg(test)]
fn rescue_expansion_tiles(
    dimensions: &[(usize, usize)],
    tile_side: usize,
) -> Vec<RescueExpansionTile> {
    let tile_side = tile_side.max(1);
    let mut tiles = Vec::new();
    for &(left_count, right_count) in dimensions {
        for left_begin in (0..left_count).step_by(tile_side) {
            let left_end = left_begin.saturating_add(tile_side).min(left_count);
            for right_begin in (0..right_count).step_by(tile_side) {
                let right_end = right_begin.saturating_add(tile_side).min(right_count);
                tiles.push(RescueExpansionTile {
                    left_begin,
                    left_end,
                    right_begin,
                    right_end,
                });
            }
        }
    }
    tiles
}

enum RescueExpansionMessage {
    Work(u64),
    Error(PipelineError),
}

fn append_rescue_edges(
    snapshot: &MetadataSnapshot,
    plan: &RescueExecutionPlan,
    worker_pool: &rayon::ThreadPool,
    collectors: &ScopeCollectorBroker,
    chain_count: usize,
    progress: &mut impl FnMut(ProgressEvent),
) -> Result<(), PipelineError> {
    const TILE_SIDE: usize = 256;
    const EDGE_BATCH: usize = 4_096;
    let total = plan.execution_work();
    progress(ProgressEvent::determinate(
        ProgressPhase::RescuePairs,
        0,
        total,
        WorkUnit::Pairs,
        ProgressCounters::default(),
    ));
    let features = snapshot.features();
    let cancelled = std::sync::atomic::AtomicBool::new(false);
    let (sender, receiver) = std::sync::mpsc::sync_channel::<RescueExpansionMessage>(
        worker_pool.current_num_threads().max(1) * 2,
    );
    std::thread::scope(|scope| -> Result<(), PipelineError> {
        let producer_sender = sender.clone();
        let producer_cancelled = &cancelled;
        let producer = scope.spawn(move || {
            worker_pool.install(|| {
                rayon::join(
                    || {
                        plan.matched_atom_pairs
                            .par_iter()
                            .for_each(|&(left_atom, right_atom)| {
                                if producer_cancelled.load(std::sync::atomic::Ordering::Acquire) {
                                    return;
                                }
                                let left_contracts = atom_contracts(snapshot, left_atom);
                                let right_contracts = atom_contracts(snapshot, right_atom);
                                let left_tiles = left_contracts.len().div_ceil(TILE_SIDE);
                                let right_tiles = right_contracts.len().div_ceil(TILE_SIDE);
                                let tile_count = left_tiles.saturating_mul(right_tiles);
                                (0..tile_count).into_par_iter().for_each(|tile| {
                                    if producer_cancelled.load(std::sync::atomic::Ordering::Acquire)
                                    {
                                        return;
                                    }
                                    let left_tile = tile / right_tiles.max(1);
                                    let right_tile = tile % right_tiles.max(1);
                                    let left_begin = left_tile * TILE_SIDE;
                                    let right_begin = right_tile * TILE_SIDE;
                                    let left_end = left_begin
                                        .saturating_add(TILE_SIDE)
                                        .min(left_contracts.len());
                                    let right_end = right_begin
                                        .saturating_add(TILE_SIDE)
                                        .min(right_contracts.len());
                                    let mut edges = Vec::with_capacity(EDGE_BATCH);
                                    for &left in &left_contracts[left_begin..left_end] {
                                        for &right in &right_contracts[right_begin..right_end] {
                                            if left != right
                                                && !contracts_share_retained_token(
                                                    features, left, right,
                                                )
                                            {
                                                edges.push(Edge::new(left, right));
                                            }
                                        }
                                    }
                                    if !edges.is_empty() {
                                        if let Err(error) = collectors.push_edges_by_chain(
                                            &features.contract_chain,
                                            chain_count,
                                            edges,
                                        ) {
                                            let _ = producer_sender
                                                .send(RescueExpansionMessage::Error(error));
                                            producer_cancelled
                                                .store(true, std::sync::atomic::Ordering::Release);
                                            return;
                                        }
                                    }
                                    let work = (left_end - left_begin)
                                        .saturating_mul(right_end - right_begin)
                                        as u64;
                                    if producer_sender
                                        .send(RescueExpansionMessage::Work(work))
                                        .is_err()
                                    {
                                        producer_cancelled
                                            .store(true, std::sync::atomic::Ordering::Release);
                                    }
                                });
                            });
                    },
                    || {
                        plan.matched_shared_edges
                            .par_chunks(EDGE_BATCH)
                            .for_each(|chunk| {
                                if producer_cancelled.load(std::sync::atomic::Ordering::Acquire) {
                                    return;
                                }
                                let edges = chunk
                                    .iter()
                                    .map(|&(left, right)| Edge::new(left, right))
                                    .collect::<Vec<_>>();
                                if let Err(error) = collectors.push_edges_by_chain(
                                    &features.contract_chain,
                                    chain_count,
                                    edges,
                                ) {
                                    let _ =
                                        producer_sender.send(RescueExpansionMessage::Error(error));
                                    producer_cancelled
                                        .store(true, std::sync::atomic::Ordering::Release);
                                    return;
                                }
                                if producer_sender
                                    .send(RescueExpansionMessage::Work(chunk.len() as u64))
                                    .is_err()
                                {
                                    producer_cancelled
                                        .store(true, std::sync::atomic::Ordering::Release);
                                }
                            });
                    },
                );
            });
        });
        drop(sender);
        let mut completed = 0u64;
        let mut first_error = None;
        for message in receiver {
            match message {
                RescueExpansionMessage::Work(work) => {
                    completed = completed.saturating_add(work);
                }
                RescueExpansionMessage::Error(error) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                    cancelled.store(true, std::sync::atomic::Ordering::Release);
                }
            }
            progress(ProgressEvent::determinate(
                ProgressPhase::RescuePairs,
                completed,
                total,
                WorkUnit::Pairs,
                ProgressCounters {
                    matched: collectors.accepted_edges(),
                    ..ProgressCounters::default()
                },
            ));
        }
        producer
            .join()
            .map_err(|_| PipelineError::Parallel("rescue expansion worker panicked".into()))?;
        if let Some(error) = first_error {
            return Err(error);
        }
        if completed != total {
            return Err(PipelineError::Invariant(format!(
                "rescue expansion progress mismatch: completed={completed}, planned={total}"
            )));
        }
        progress(ProgressEvent::determinate(
            ProgressPhase::RescuePairs,
            total,
            total,
            WorkUnit::Pairs,
            ProgressCounters {
                matched: collectors.accepted_edges(),
                ..ProgressCounters::default()
            },
        ));
        Ok(())
    })
}

fn contracts_share_retained_token(
    features: &crate::encode::FeatureView,
    left: u32,
    right: u32,
) -> bool {
    let tokens = |contract: u32| {
        let begin = features.contract_token_offsets[contract as usize] as usize;
        let end = features.contract_token_offsets[contract as usize + 1] as usize;
        &features.contract_tokens[begin..end]
    };
    let (left_tokens, right_tokens) = (tokens(left), tokens(right));
    let (mut left_index, mut right_index) = (0, 0);
    while left_index < left_tokens.len() && right_index < right_tokens.len() {
        match left_tokens[left_index].cmp(&right_tokens[right_index]) {
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
            std::cmp::Ordering::Equal => return true,
        }
    }
    false
}
fn append_shared_token_edges(
    s: &MetadataSnapshot,
    lanes: usize,
    collectors: &ScopeCollectorBroker,
    chain_count: usize,
    progress: &mut impl FnMut(ProgressEvent),
) -> Result<u64, PipelineError> {
    let f = s.features();
    let token_count = f.token_member_offsets.len().saturating_sub(1);
    let shared_pair_work_upper_bound = f
        .token_member_offsets
        .windows(2)
        .try_fold(0u64, |total, window| {
            checked_add_pairs(total, window[1].saturating_sub(window[0]))
        })?;
    progress(
        ProgressEvent::determinate(
            ProgressPhase::SharedTokenPairs,
            0,
            shared_pair_work_upper_bound,
            WorkUnit::Pairs,
            ProgressCounters::default(),
        )
        .with_plan(WorkClass::SharedScores, TotalKind::UpperBound),
    );
    const EDGE_BATCH: usize = 4_096;
    const WORK_BATCH: u64 = 16_384;
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(lanes.max(1))
        .thread_name(|index| format!("metadata-shared-{index}"))
        .build()
        .map_err(|error| PipelineError::Parallel(error.to_string()))?;
    let (sender, receiver) = std::sync::mpsc::sync_channel::<SharedMessage>(lanes.max(1) * 2);
    let cancelled = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(|scope| -> Result<u64, PipelineError> {
        let worker_sender = sender.clone();
        let producer_cancelled = &cancelled;
        let producer = scope.spawn(move || {
            pool.install(|| {
                (0..token_count).into_par_iter().for_each(|token| {
                    if producer_cancelled.load(std::sync::atomic::Ordering::Acquire) {
                        return;
                    }
                    let begin = f.token_member_offsets[token] as usize;
                    let end = f.token_member_offsets[token + 1] as usize;
                    let contracts = &f.token_member_contracts[begin..end];
                    let sources = &f.token_member_sources[begin..end];
                    let member_payloads = sources
                        .iter()
                        .map(|&source| f.source_to_payload[source as usize])
                        .collect::<Vec<_>>();
                    if contracts.len() >= SHARED_LOCAL_ROUTING_MIN_MEMBERS {
                        const LOCAL_TILE_MEMBERS: usize = 256;
                        let sketches = shared_group_sketches(f, sources);
                        let plan = LocalRoutingPlan::build_parallel(&sketches);
                        plan.tiles(LOCAL_TILE_MEMBERS)
                            .par_bridge()
                            .for_each(|tile| {
                                if producer_cancelled.load(std::sync::atomic::Ordering::Acquire) {
                                    return;
                                }
                                let mut edges = Vec::with_capacity(EDGE_BATCH);
                                let mut pending_work = 0u64;
                                let mut failed = false;
                                let mut payload_scores = HashMap::<u64, bool>::new();
                                let _ = plan.visit_tile(&sketches, &tile, |i, j| {
                                    if failed
                                        || producer_cancelled
                                            .load(std::sync::atomic::Ordering::Acquire)
                                    {
                                        return false;
                                    }
                                    pending_work = pending_work.saturating_add(1);
                                    if let Some(edge) = shared_pair_edge_cached(
                                        f,
                                        contracts,
                                        &member_payloads,
                                        i as usize,
                                        j as usize,
                                        &mut payload_scores,
                                    ) {
                                        edges.push(edge);
                                        if edges.len() == EDGE_BATCH {
                                            let ready = std::mem::replace(
                                                &mut edges,
                                                Vec::with_capacity(EDGE_BATCH),
                                            );
                                            if let Err(error) = collectors.push_edges_by_chain(
                                                &f.contract_chain,
                                                chain_count,
                                                ready,
                                            ) {
                                                let _ =
                                                    worker_sender.send(SharedMessage::Error(error));
                                                failed = true;
                                                producer_cancelled.store(
                                                    true,
                                                    std::sync::atomic::Ordering::Release,
                                                );
                                                return false;
                                            }
                                        }
                                    }
                                    if pending_work >= WORK_BATCH {
                                        if worker_sender
                                            .send(SharedMessage::Work {
                                                pairs: pending_work,
                                                groups: 0,
                                            })
                                            .is_err()
                                        {
                                            failed = true;
                                            producer_cancelled
                                                .store(true, std::sync::atomic::Ordering::Release);
                                            return false;
                                        }
                                        pending_work = 0;
                                    }
                                    true
                                });
                                if !edges.is_empty()
                                    && !failed
                                    && collectors
                                        .push_edges_by_chain(&f.contract_chain, chain_count, edges)
                                        .map_err(|error| {
                                            let _ = worker_sender.send(SharedMessage::Error(error));
                                            producer_cancelled
                                                .store(true, std::sync::atomic::Ordering::Release);
                                        })
                                        .is_err()
                                {
                                    failed = true;
                                }
                                if pending_work > 0
                                    && !failed
                                    && worker_sender
                                        .send(SharedMessage::Work {
                                            pairs: pending_work,
                                            groups: 0,
                                        })
                                        .is_err()
                                {
                                    producer_cancelled
                                        .store(true, std::sync::atomic::Ordering::Release);
                                }
                            });
                        if !producer_cancelled.load(std::sync::atomic::Ordering::Acquire) {
                            let _ = worker_sender.send(SharedMessage::Work {
                                pairs: 0,
                                groups: 1,
                            });
                        }
                        return;
                    }
                    let mut edges = Vec::with_capacity(EDGE_BATCH);
                    let mut pending_work = 0u64;
                    let mut failed = false;
                    let mut payload_scores = HashMap::<u64, bool>::new();
                    let mut visit = |i: usize, j: usize| {
                        if failed || producer_cancelled.load(std::sync::atomic::Ordering::Acquire) {
                            return;
                        }
                        pending_work = pending_work.saturating_add(1);
                        if let Some(edge) = shared_pair_edge_cached(
                            f,
                            contracts,
                            &member_payloads,
                            i,
                            j,
                            &mut payload_scores,
                        ) {
                            edges.push(edge);
                            if edges.len() == EDGE_BATCH {
                                let ready =
                                    std::mem::replace(&mut edges, Vec::with_capacity(EDGE_BATCH));
                                if let Err(error) = collectors.push_edges_by_chain(
                                    &f.contract_chain,
                                    chain_count,
                                    ready,
                                ) {
                                    let _ = worker_sender.send(SharedMessage::Error(error));
                                    failed = true;
                                    producer_cancelled
                                        .store(true, std::sync::atomic::Ordering::Release);
                                }
                            }
                        }
                        if !failed && pending_work >= WORK_BATCH {
                            failed = worker_sender
                                .send(SharedMessage::Work {
                                    pairs: pending_work,
                                    groups: 0,
                                })
                                .is_err();
                            if failed {
                                producer_cancelled
                                    .store(true, std::sync::atomic::Ordering::Release);
                            }
                            pending_work = 0;
                        }
                    };
                    for i in 0..contracts.len() {
                        for j in i + 1..contracts.len() {
                            visit(i, j);
                        }
                    }
                    if !edges.is_empty() && !failed {
                        if let Err(error) =
                            collectors.push_edges_by_chain(&f.contract_chain, chain_count, edges)
                        {
                            let _ = worker_sender.send(SharedMessage::Error(error));
                            failed = true;
                            producer_cancelled.store(true, std::sync::atomic::Ordering::Release);
                        }
                    }
                    if !failed {
                        let _ = worker_sender.send(SharedMessage::Work {
                            pairs: pending_work,
                            groups: 1,
                        });
                    }
                });
            });
        });
        drop(sender);
        let mut completed = 0u64;
        let mut groups = 0u64;
        let mut collection_error = None;
        for message in receiver {
            match message {
                SharedMessage::Work {
                    pairs,
                    groups: finished,
                } => {
                    completed = completed
                        .checked_add(pairs)
                        .ok_or(crate::resource::MemoryError::Overflow)?;
                    groups = groups.saturating_add(finished);
                    progress(
                        ProgressEvent::determinate(
                            ProgressPhase::SharedTokenPairs,
                            completed,
                            shared_pair_work_upper_bound,
                            WorkUnit::Pairs,
                            ProgressCounters {
                                groups,
                                matched: collectors.accepted_edges(),
                                ..ProgressCounters::default()
                            },
                        )
                        .with_plan(WorkClass::SharedScores, TotalKind::UpperBound),
                    );
                }
                SharedMessage::Error(error) => {
                    if collection_error.is_none() {
                        collection_error = Some(error);
                    }
                    cancelled.store(true, std::sync::atomic::Ordering::Release);
                }
            }
        }
        producer
            .join()
            .map_err(|_| PipelineError::Parallel("worker panicked".into()))?;
        if let Some(error) = collection_error {
            return Err(error);
        }
        progress(
            ProgressEvent::determinate(
                ProgressPhase::SharedTokenPairs,
                completed,
                shared_pair_work_upper_bound,
                WorkUnit::Pairs,
                ProgressCounters {
                    groups,
                    matched: collectors.accepted_edges(),
                    ..ProgressCounters::default()
                },
            )
            .with_plan(WorkClass::SharedScores, TotalKind::UpperBound),
        );
        Ok(completed)
    })
}

fn shared_pair_edge_cached(
    f: &crate::encode::FeatureView,
    contracts: &[u32],
    payloads: &[u32],
    i: usize,
    j: usize,
    payload_scores: &mut HashMap<u64, bool>,
) -> Option<Edge> {
    let left = contracts[i];
    let right = contracts[j];
    if left == right {
        return None;
    }
    let lp = payloads[i];
    let rp = payloads[j];
    let key = payload_pair_key(lp, rp);
    let exact_match = *payload_scores
        .entry(key)
        .or_insert_with(|| score_pair(f, lp, rp) == PairScoreDecision::ExactMatch);
    if exact_match {
        Some(Edge::new(left, right))
    } else {
        None
    }
}

fn payload_pair_key(left: u32, right: u32) -> u64 {
    let (left, right) = (left.min(right), left.max(right));
    (u64::from(left) << 32) | u64::from(right)
}

fn chain_pair_index(left: usize, right: usize, chain_count: usize) -> usize {
    let (left, right) = (left.min(right), left.max(right));
    left * (2 * chain_count - left - 1) / 2 + (right - left - 1)
}

fn build_summary_rows_with_progress(
    snapshot: &MetadataSnapshot,
    scopes: &ScopeComponents,
    chain_count: usize,
    worker_pool: &rayon::ThreadPool,
    progress: &mut impl FnMut(ProgressEvent),
) -> Vec<MetadataSummaryRow> {
    let mut rows = Vec::new();
    let mut completed = 0u64;
    progress(ProgressEvent::indeterminate(
        ProgressPhase::BuildSummary,
        0,
        WorkUnit::Nodes,
        ProgressCounters::default(),
    ));
    let channel_capacity = worker_pool.current_num_threads().max(1).saturating_mul(2);
    let (sender, receiver) = std::sync::mpsc::sync_channel::<u64>(channel_capacity);
    let (intra_rows, cross_stats) = std::thread::scope(|thread_scope| {
        let producer_sender = sender.clone();
        let producer = thread_scope.spawn(move || {
            worker_pool.install(|| {
                let intra_sender = producer_sender.clone();
                rayon::join(
                    || {
                        summary_stats_by_chain(
                            snapshot,
                            &scopes.intra_roots,
                            chain_count,
                            false,
                            &|delta| {
                                let _ = intra_sender.send(delta);
                            },
                        )
                    },
                    || {
                        (chain_count > 1).then(|| {
                            summary_stats_by_chain(
                                snapshot,
                                &scopes.cross_roots,
                                chain_count,
                                true,
                                &|delta| {
                                    let _ = producer_sender.send(delta);
                                },
                            )
                        })
                    },
                )
            })
        });
        drop(sender);
        for delta in receiver {
            completed = completed.saturating_add(delta);
            progress(ProgressEvent::indeterminate(
                ProgressPhase::BuildSummary,
                completed,
                WorkUnit::Nodes,
                ProgressCounters::default(),
            ));
        }
        producer.join().expect("metadata summary worker panicked")
    });
    for chain in 0..chain_count {
        rows.push(summary_row_from_stats(
            snapshot,
            "intra_chain",
            chain,
            None,
            intra_rows[chain],
        ));
        if let Some(stats) = cross_stats.as_ref() {
            rows.push(summary_row_from_stats(
                snapshot,
                "cross_chain_summary",
                chain,
                None,
                stats[chain],
            ));
        }
    }
    let (sender, receiver) = std::sync::mpsc::sync_channel::<u64>(channel_capacity);
    let pair_rows = std::thread::scope(|thread_scope| {
        let producer_sender = sender.clone();
        let producer = thread_scope.spawn(move || {
            worker_pool.install(|| {
                scopes
                    .chain_pair_roots
                    .par_iter()
                    .map(|pair| {
                        let stats = summary_stats_for_chain_pair(
                            snapshot,
                            &pair.roots,
                            pair.left_chain as usize,
                            pair.right_chain as usize,
                            &|delta| {
                                let _ = producer_sender.send(delta);
                            },
                        );
                        [
                            summary_row_from_stats(
                                snapshot,
                                "chain_matrix",
                                pair.left_chain as usize,
                                Some(pair.right_chain as usize),
                                stats[0],
                            ),
                            summary_row_from_stats(
                                snapshot,
                                "chain_matrix",
                                pair.right_chain as usize,
                                Some(pair.left_chain as usize),
                                stats[1],
                            ),
                        ]
                    })
                    .collect::<Vec<_>>()
            })
        });
        drop(sender);
        for delta in receiver {
            completed = completed.saturating_add(delta);
            progress(ProgressEvent::indeterminate(
                ProgressPhase::BuildSummary,
                completed,
                WorkUnit::Nodes,
                ProgressCounters::default(),
            ));
        }
        producer.join().expect("metadata summary worker panicked")
    });
    for pair in pair_rows {
        for row in pair {
            rows.push(row);
        }
    }
    rows
}

#[derive(Clone, Copy)]
struct SummaryEntry {
    root: u32,
    chain: u32,
    nfts: i64,
}

fn summary_entries(
    snapshot: &MetadataSnapshot,
    roots: &[u32],
    include_chain: impl Fn(usize) -> bool + Sync,
    on_work: &(impl Fn(u64) + Sync),
) -> Vec<SummaryEntry> {
    const PROGRESS_CHUNK: u64 = 65_536;
    let f = snapshot.features();
    roots
        .par_chunks(PROGRESS_CHUNK as usize)
        .enumerate()
        .map(|(chunk_index, chunk)| {
            let begin = chunk_index.saturating_mul(PROGRESS_CHUNK as usize);
            let mut entries = Vec::with_capacity(chunk.len());
            for (offset, &root) in chunk.iter().enumerate() {
                let contract = begin.saturating_add(offset);
                let Some(&chain) = f.contract_chain.get(contract) else {
                    continue;
                };
                if include_chain(chain as usize) {
                    entries.push(SummaryEntry {
                        root,
                        chain,
                        nfts: i64::try_from(f.contract_weight[contract]).unwrap_or(i64::MAX),
                    });
                }
            }
            on_work(chunk.len() as u64);
            entries
        })
        .flatten()
        .collect()
}

fn summary_stats_by_chain(
    snapshot: &MetadataSnapshot,
    roots: &[u32],
    chain_count: usize,
    require_secondary: bool,
    on_work: &(impl Fn(u64) + Sync),
) -> Vec<SummaryStats> {
    let mut entries = summary_entries(snapshot, roots, |_| true, on_work);
    entries.par_sort_unstable_by_key(|entry| (entry.root, entry.chain));
    let mut stats = vec![SummaryStats::default(); chain_count];
    summarize_sorted_entries(&entries, require_secondary, |chain, summary| {
        if let Some(target) = stats.get_mut(chain) {
            accumulate_summary(target, summary);
        }
    });
    stats
}

fn summary_stats_for_chain_pair(
    snapshot: &MetadataSnapshot,
    roots: &[u32],
    left_chain: usize,
    right_chain: usize,
    on_work: &(impl Fn(u64) + Sync),
) -> [SummaryStats; 2] {
    let mut entries = summary_entries(
        snapshot,
        roots,
        |chain| chain == left_chain || chain == right_chain,
        on_work,
    );
    entries.par_sort_unstable_by_key(|entry| (entry.root, entry.chain));
    let mut stats = [SummaryStats::default(), SummaryStats::default()];
    summarize_sorted_entries(&entries, true, |chain, summary| {
        if chain == left_chain {
            accumulate_summary(&mut stats[0], summary);
        } else if chain == right_chain {
            accumulate_summary(&mut stats[1], summary);
        }
    });
    stats
}

fn summarize_sorted_entries(
    entries: &[SummaryEntry],
    require_secondary: bool,
    mut accumulate: impl FnMut(usize, SummaryStats),
) {
    let mut begin = 0usize;
    let mut chain_entries = Vec::<(usize, i64, i64)>::new();
    while begin < entries.len() {
        let root = entries[begin].root;
        let mut end = begin + 1;
        while end < entries.len() && entries[end].root == root {
            end += 1;
        }
        chain_entries.clear();
        let mut cursor = begin;
        while cursor < end {
            let chain = entries[cursor].chain as usize;
            let mut count = 0i64;
            let mut nfts = 0i64;
            while cursor < end && entries[cursor].chain as usize == chain {
                count += 1;
                nfts = nfts.saturating_add(entries[cursor].nfts);
                cursor += 1;
            }
            chain_entries.push((chain, count, nfts));
        }
        let global_total = (end - begin) as i64;
        for &(chain, count, nfts) in &chain_entries {
            let total = if require_secondary {
                global_total
            } else {
                count
            };
            if count != 0 && total >= 2 && (!require_secondary || chain_entries.len() > 1) {
                accumulate(
                    chain,
                    SummaryStats {
                        group_count: 1,
                        duplicate_contract_count: count,
                        duplicate_nft_count: nfts,
                        group_size_ge_2_count: 1,
                        group_size_gt_2_count: i64::from(total > 2),
                    },
                );
            }
        }
        begin = end;
    }
}

fn accumulate_summary(target: &mut SummaryStats, value: SummaryStats) {
    target.group_count += value.group_count;
    target.duplicate_contract_count += value.duplicate_contract_count;
    target.duplicate_nft_count = target
        .duplicate_nft_count
        .saturating_add(value.duplicate_nft_count);
    target.group_size_ge_2_count += value.group_size_ge_2_count;
    target.group_size_gt_2_count += value.group_size_gt_2_count;
}

fn summary_row_from_stats(
    snapshot: &MetadataSnapshot,
    scope: &str,
    primary: usize,
    secondary: Option<usize>,
    stats: SummaryStats,
) -> MetadataSummaryRow {
    let total = snapshot.chain_totals().get(primary);
    let name = snapshot
        .chain_names()
        .get(primary)
        .cloned()
        .unwrap_or_else(|| format!("chain-{primary}"));
    let secondary_name = secondary
        .and_then(|i| snapshot.chain_names().get(i))
        .cloned()
        .unwrap_or_default();
    MetadataSummaryRow {
        scope: scope.into(),
        primary_chain: name,
        secondary_chain: secondary_name,
        total_contracts: total.map_or(0, |t| t.contracts),
        total_nfts: total.map_or(0, |t| t.nfts),
        group_count: stats.group_count,
        duplicate_contract_count: stats.duplicate_contract_count,
        duplicate_nft_count: stats.duplicate_nft_count,
        group_size_ge_2_count: stats.group_size_ge_2_count,
        group_size_gt_2_count: stats.group_size_gt_2_count,
    }
}

#[cfg(test)]
fn dense_summary_stats(
    roots: &[u32],
    contracts: impl IntoIterator<Item = (usize, bool, i64)>,
    require_secondary: bool,
) -> SummaryStats {
    DenseSummaryScratch::new(roots.len()).summarize(roots, contracts, require_secondary)
}

pub fn default_output_dir(work: &Path) -> PathBuf {
    crate::artifacts::MetadataArtifactLayout::new(work).match_dir()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blocking::{compile_base_equivalent, AtomSketch, BlockingCompileConfig};
    use crate::encode::{
        write_encode_artifacts_with_contracts_and_atoms, EncodeContractRow, EncodePayloadRow,
        EncodeSourceRow,
    };
    use crate::format::commit_ready;

    #[test]
    fn configured_worker_pool_uses_requested_thread_count() {
        let pool = build_metadata_worker_pool(2).unwrap();
        assert_eq!(pool.install(rayon::current_num_threads), 2);
    }

    #[test]
    fn scope_collector_broker_shards_scopes_within_thread_ceiling() {
        let budget = EdgeBudget {
            max_buffer_bytes: u64::MAX,
            max_run_edges: u64::MAX,
            max_total_bytes: u64::MAX,
        };
        let contract_chain = [0, 0, 1, 1, 2, 2];
        let broker = ScopeCollectorBroker::new(6, 3, budget, u64::MAX, 4).unwrap();

        assert_eq!(broker.active_sink_workers(), 2);
        assert_eq!(broker.scorer_lanes(), 2);
        broker
            .push_edges_by_chain(&contract_chain, 3, vec![Edge::new(0, 1), Edge::new(0, 2)])
            .unwrap();
        broker
            .push_edges_by_chain(&contract_chain, 3, vec![Edge::new(2, 4)])
            .unwrap();
        let accepted = broker.accepted_edges();
        let (intra, cross, pairs) = broker.finish().unwrap();

        assert_eq!(accepted, 3);
        assert_eq!(
            canonical_edge_components(6, &intra[0].edges),
            vec![vec![0, 1]]
        );
        let cross_edges = cross
            .iter()
            .flat_map(|run| run.edges.iter().copied())
            .collect::<Vec<_>>();
        assert_eq!(
            canonical_edge_components(6, &cross_edges),
            vec![vec![0, 2, 4]]
        );
        assert_eq!(
            canonical_edge_components(6, &pairs[0][0].edges),
            vec![vec![0, 2]]
        );
        assert_eq!(
            canonical_edge_components(6, &pairs[2][0].edges),
            vec![vec![2, 4]]
        );
        assert!(pairs[1].is_empty());
    }

    #[test]
    fn scope_collector_broker_fails_closed_on_retained_budget_overflow() {
        let budget = EdgeBudget {
            max_buffer_bytes: u64::MAX,
            max_run_edges: u64::MAX,
            max_total_bytes: u64::MAX,
        };
        let broker = ScopeCollectorBroker::new(2, 0, budget, 0, 2).unwrap();

        broker
            .push_edges_by_chain(&[0, 0], 1, vec![Edge::new(0, 1)])
            .unwrap();
        let error = broker.finish().unwrap_err();

        assert!(error.to_string().contains("scope_forest_bytes"));
    }

    #[test]
    fn scope_collector_broker_preserves_scorer_capacity_with_many_scopes() {
        let budget = EdgeBudget {
            max_buffer_bytes: u64::MAX,
            max_run_edges: u64::MAX,
            max_total_bytes: u64::MAX,
        };
        let broker = ScopeCollectorBroker::new(1, 120, budget, u64::MAX, 128).unwrap();

        assert_eq!(broker.active_sink_workers(), 32);
        assert_eq!(broker.scorer_lanes(), 96);
        broker.finish().unwrap();
    }

    #[test]
    fn fallback_pair_tasks_cover_each_pair_once_in_stable_order() {
        let member_counts = [0usize, 1, 4, 3];
        let mut cursor = FallbackPairCursor::default();
        let mut pairs = Vec::new();
        while let Some(task) =
            next_fallback_pair_task(&mut cursor, member_counts.len(), 2, |atom| {
                member_counts[atom]
            })
        {
            for right in task.right_begin..task.right_end {
                pairs.push((task.atom, task.left, right));
            }
        }
        let expected = member_counts
            .iter()
            .copied()
            .enumerate()
            .flat_map(|(atom, members)| {
                (0..members)
                    .flat_map(move |left| (left + 1..members).map(move |right| (atom, left, right)))
            })
            .collect::<Vec<_>>();

        assert_eq!(pairs, expected);
    }

    #[test]
    fn rescue_expansion_tiles_cover_each_cartesian_visit_once() {
        let tiles = rescue_expansion_tiles(&[(3, 5)], 2);
        let mut visits = Vec::new();
        for tile in tiles {
            for left in tile.left_begin..tile.left_end {
                for right in tile.right_begin..tile.right_end {
                    visits.push((left, right));
                }
            }
        }
        visits.sort_unstable();

        assert_eq!(
            visits,
            (0..3)
                .flat_map(|left| (0..5).map(move |right| (left, right)))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn independent_component_scopes_reduce_in_parallel_with_exact_progress() {
        let budget = EdgeBudget {
            max_buffer_bytes: u64::MAX,
            max_run_edges: u64::MAX,
            max_total_bytes: u64::MAX,
        };
        let first = ForestRun::from_edges(5, [Edge::new(0, 1), Edge::new(1, 2)], budget).unwrap();
        let second = ForestRun::from_edges(5, [Edge::new(3, 4)], budget).unwrap();
        let identity = |scope_identity: &str| ComponentSnapshotIdentity {
            schema_revision: 1,
            snapshot_fingerprint: "snapshot".into(),
            connectivity_revision: 1,
            connectivity_plan_digest: "plan".into(),
            scope_identity: scope_identity.into(),
            node_count: 5,
        };
        let mut scopes = vec![
            ComponentScopePlan {
                kind: ComponentScopeKind::Intra,
                directory: PathBuf::new(),
                identity: identity("intra"),
                runs: vec![first],
                roots: None,
                needs_rebuild: true,
            },
            ComponentScopePlan {
                kind: ComponentScopeKind::Cross,
                directory: PathBuf::new(),
                identity: identity("cross"),
                runs: vec![second],
                roots: None,
                needs_rebuild: true,
            },
        ];
        let total = scopes
            .iter()
            .map(|scope| reduce_work(&scope.runs, 5).unwrap())
            .sum();
        let pool = build_metadata_worker_pool(2).unwrap();
        let mut events = Vec::new();

        reduce_component_scopes_parallel(&mut scopes, 5, total, &pool, &mut |event| {
            events.push(event)
        })
        .unwrap();

        assert_eq!(scopes[0].roots.as_ref().unwrap(), &[0, 0, 0, 3, 4]);
        assert_eq!(scopes[1].roots.as_ref().unwrap(), &[0, 1, 2, 3, 3]);
        assert_eq!(events.last().unwrap().completed, total);
    }

    fn canonical_edge_components(node_count: usize, edges: &[Edge]) -> Vec<Vec<u32>> {
        let mut parent = (0..node_count).collect::<Vec<_>>();
        let mut touched = vec![false; node_count];
        for edge in edges {
            let left = edge.left as usize;
            let right = edge.right as usize;
            touched[left] = true;
            touched[right] = true;
            let left_root = sparse_find(&mut parent, left);
            let right_root = sparse_find(&mut parent, right);
            if left_root != right_root {
                parent[right_root] = left_root;
            }
        }
        let mut groups = BTreeMap::<usize, Vec<u32>>::new();
        for (node, &is_touched) in touched.iter().enumerate() {
            if is_touched {
                groups
                    .entry(sparse_find(&mut parent, node))
                    .or_default()
                    .push(node as u32);
            }
        }
        let mut groups = groups.into_values().collect::<Vec<_>>();
        groups.sort();
        groups
    }

    #[test]
    fn pair_first_cross_compaction_preserves_pair_and_global_connectivity() {
        let contract_chain = [0, 0, 1, 1, 2, 2];
        let edges = vec![
            Edge::new(0, 1),
            Edge::new(2, 3),
            Edge::new(0, 2),
            Edge::new(1, 3),
            Edge::new(0, 3),
            Edge::new(0, 2),
            Edge::new(2, 4),
            Edge::new(3, 5),
            Edge::new(2, 5),
            Edge::new(1, 4),
        ];
        let mut scratch = ScopeCompactionScratch::new(contract_chain.len(), usize::MAX);

        let compacted =
            compact_catalog_scope_batch_by_chain(&contract_chain, 3, edges.clone(), &mut scratch);

        let expected_intra = edges
            .iter()
            .copied()
            .filter(|edge| {
                contract_chain[edge.left as usize] == contract_chain[edge.right as usize]
            })
            .collect::<Vec<_>>();
        let expected_cross = edges
            .iter()
            .copied()
            .filter(|edge| {
                contract_chain[edge.left as usize] != contract_chain[edge.right as usize]
            })
            .collect::<Vec<_>>();
        assert_eq!(compacted.accepted_edges, edges.len() as u64);
        assert_eq!(
            canonical_edge_components(contract_chain.len(), &compacted.intra),
            canonical_edge_components(contract_chain.len(), &expected_intra)
        );
        assert_eq!(
            canonical_edge_components(contract_chain.len(), &compacted.cross),
            canonical_edge_components(contract_chain.len(), &expected_cross)
        );
        let expected_pairs = expected_cross.iter().copied().fold(
            BTreeMap::<usize, Vec<Edge>>::new(),
            |mut pairs, edge| {
                pairs
                    .entry(chain_pair_index(
                        contract_chain[edge.left as usize] as usize,
                        contract_chain[edge.right as usize] as usize,
                        3,
                    ))
                    .or_default()
                    .push(edge);
                pairs
            },
        );
        assert_eq!(compacted.chain_pairs.len(), expected_pairs.len());
        for (pair, pair_edges) in compacted.chain_pairs {
            let expected = &expected_pairs[&pair];
            assert_eq!(
                canonical_edge_components(contract_chain.len(), &pair_edges),
                canonical_edge_components(contract_chain.len(), expected)
            );
        }
    }

    #[test]
    fn iterator_scope_compaction_builds_a_forest_without_a_candidate_copy() {
        let groups = [
            vec![Edge::new(0, 2), Edge::new(1, 3), Edge::new(0, 3)],
            vec![Edge::new(2, 4), Edge::new(3, 5), Edge::new(2, 5)],
        ];
        let expected = groups.iter().flatten().copied().collect::<Vec<_>>();
        let mut scratch = ScopeCompactionScratch::new(6, usize::MAX);

        let compacted = compact_scope_edge_iter_with_scratch(
            groups.iter().flatten().copied(),
            expected.len(),
            &mut scratch,
        );

        assert_eq!(
            canonical_edge_components(6, &compacted),
            canonical_edge_components(6, &expected)
        );
    }

    #[test]
    fn iterator_scope_compaction_uses_the_candidate_capacity_hint() {
        let edges = vec![Edge::new(0, 1); 64];
        let mut scratch = ScopeCompactionScratch::new(2, usize::MAX);

        let compacted =
            compact_scope_edge_iter_with_scratch(edges.iter().copied(), edges.len(), &mut scratch);

        assert_eq!(compacted, vec![Edge::new(0, 1)]);
        assert!(compacted.capacity() >= edges.len());
    }

    #[test]
    fn lane_local_scope_compaction_preserves_connectivity() {
        let compacted = compact_scope_edges(vec![
            Edge::new(0, 1),
            Edge::new(1, 2),
            Edge::new(0, 2),
            Edge::new(0, 1),
        ]);

        assert_eq!(compacted.len(), 2);
        let mut parent = [0usize, 1, 2];
        for edge in &compacted {
            let left = sparse_find(&mut parent, edge.left as usize);
            let right = sparse_find(&mut parent, edge.right as usize);
            if left != right {
                parent[right] = left;
            }
        }
        let roots: std::collections::BTreeSet<_> =
            (0..3).map(|node| sparse_find(&mut parent, node)).collect();
        assert_eq!(roots.len(), 1);
    }

    #[test]
    fn dense_summary_scratch_matches_primary_secondary_group_semantics() {
        let roots = [0, 0, 0, 3];
        let contracts = [(0usize, true, 10i64), (1, true, 20), (2, false, 30)];
        let stats = dense_summary_stats(&roots, contracts, true);

        assert_eq!(stats.group_count, 1);
        assert_eq!(stats.duplicate_contract_count, 2);
        assert_eq!(stats.duplicate_nft_count, 30);
        assert_eq!(stats.group_size_ge_2_count, 1);
        assert_eq!(stats.group_size_gt_2_count, 1);
    }

    #[test]
    fn sorted_scope_summary_matches_dense_chain_pair_semantics() {
        let roots = [0, 0, 0, 3, 3];
        let chains = [0usize, 0, 1, 0, 1];
        let nfts = [10i64, 20, 30, 40, 50];
        let mut entries = roots
            .iter()
            .copied()
            .enumerate()
            .map(|(contract, root)| SummaryEntry {
                root,
                chain: chains[contract] as u32,
                nfts: nfts[contract],
            })
            .collect::<Vec<_>>();
        entries.sort_unstable_by_key(|entry| (entry.root, entry.chain));
        let mut sorted = [SummaryStats::default(), SummaryStats::default()];
        summarize_sorted_entries(&entries, true, |chain, value| {
            accumulate_summary(&mut sorted[chain], value);
        });

        for (primary, sorted_stats) in sorted.iter().enumerate() {
            let dense = dense_summary_stats(
                &roots,
                (0..roots.len())
                    .map(|contract| (contract, chains[contract] == primary, nfts[contract])),
                true,
            );
            assert_eq!(sorted_stats.group_count, dense.group_count);
            assert_eq!(
                sorted_stats.duplicate_contract_count,
                dense.duplicate_contract_count
            );
            assert_eq!(sorted_stats.duplicate_nft_count, dense.duplicate_nft_count);
            assert_eq!(
                sorted_stats.group_size_ge_2_count,
                dense.group_size_ge_2_count
            );
            assert_eq!(
                sorted_stats.group_size_gt_2_count,
                dense.group_size_gt_2_count
            );
        }
    }

    #[test]
    fn catalog_expansion_counter_excludes_nonmatching_atom_products() {
        let dir = tempfile::tempdir().unwrap();
        let features = dir.path().join("features");
        let blocking = dir.path().join("blocking");
        let sources = (0..4)
            .map(|contract_id| EncodeSourceRow {
                contract_id,
                payload_id: contract_id / 2,
                retained_token_ids: vec![],
            })
            .collect::<Vec<_>>();
        let contracts = (0..4)
            .map(|contract_id| EncodeContractRow {
                contract_id,
                chain_id: 0,
                source_doc_id: contract_id,
                payload_id: contract_id / 2,
                weight: 1,
            })
            .collect::<Vec<_>>();
        let payloads = vec![
            EncodePayloadRow {
                template_terms: vec![(1, 1)],
                content_terms: vec![(2, 1)],
            },
            EncodePayloadRow {
                template_terms: vec![(3, 1)],
                content_terms: vec![(4, 1)],
            },
        ];
        write_encode_artifacts_with_contracts_and_atoms(
            &features,
            &sources,
            &payloads,
            &contracts,
            &[vec![0, 1], vec![2, 3]],
        )
        .unwrap();
        compile_base_equivalent(
            &[
                AtomSketch {
                    template_simhash: 0,
                    content_simhash: 0,
                    template_anchors: vec![1],
                    content_anchors: vec![2],
                    has_template_terms: true,
                    has_content_terms: true,
                },
                AtomSketch {
                    template_simhash: 1,
                    content_simhash: 1,
                    template_anchors: vec![1],
                    content_anchors: vec![2],
                    has_template_terms: true,
                    has_content_terms: true,
                },
            ],
            &BlockingCompileConfig {
                max_routing_block_members: 10,
            },
            &blocking,
        )
        .unwrap();
        commit_ready(
            &features,
            "features.ready",
            r#"{"schema_revision":3,"source_count":4,"payload_count":2,"chains":["x"],"chain_totals":[{"name":"x","contracts":4,"nfts":4}]}"#,
        )
        .unwrap();
        commit_ready(
            &blocking,
            "blocking.ready",
            r#"{"blocking_revision":3,"atom_count":2}"#,
        )
        .unwrap();
        let snapshot = MetadataSnapshot::open(&features, &blocking).unwrap();

        let work = expand_catalog_atom_pair(&snapshot, 0, 1, |_, _| {}).unwrap();

        assert_eq!(work, 0);
    }

    #[test]
    fn contract_level_rescue_expands_across_large_shared_token_groups() {
        let dir = tempfile::tempdir().unwrap();
        let features = dir.path().join("features");
        let blocking = dir.path().join("blocking");
        let sources = (0..256)
            .map(|contract_id| EncodeSourceRow {
                contract_id,
                payload_id: contract_id,
                retained_token_ids: vec![1, 2],
            })
            .collect::<Vec<_>>();
        let contracts = (0..256)
            .map(|contract_id| EncodeContractRow {
                contract_id,
                chain_id: 0,
                source_doc_id: contract_id,
                payload_id: contract_id,
                weight: 1,
            })
            .collect::<Vec<_>>();
        let payloads = (0..256)
            .map(|id| EncodePayloadRow {
                template_terms: vec![(10 + id, 1)],
                content_terms: vec![(1_000 + id, 1)],
            })
            .collect::<Vec<_>>();
        write_encode_artifacts_with_contracts_and_atoms(
            &features,
            &sources,
            &payloads,
            &contracts,
            &(0..256).map(|id| vec![id]).collect::<Vec<_>>(),
        )
        .unwrap();
        compile_base_equivalent(
            &(0..256)
                .map(|id| AtomSketch {
                    template_simhash: id,
                    content_simhash: id,
                    template_anchors: vec![10 + id as u32],
                    content_anchors: vec![1_000 + id as u32],
                    has_template_terms: true,
                    has_content_terms: true,
                })
                .collect::<Vec<_>>(),
            &BlockingCompileConfig {
                max_routing_block_members: 10,
            },
            &blocking,
        )
        .unwrap();
        commit_ready(
            &features,
            "features.ready",
            r#"{"schema_revision":3,"source_count":256,"payload_count":256,"chains":["x"],"chain_totals":[{"name":"x","contracts":256,"nfts":512}]}"#,
        )
        .unwrap();
        commit_ready(
            &blocking,
            "blocking.ready",
            r#"{"blocking_revision":3,"atom_count":256}"#,
        )
        .unwrap();
        let snapshot = MetadataSnapshot::open(&features, &blocking).unwrap();
        let rescue = RescuePlan {
            pair_atoms: vec![],
            shared_contracts: vec![0],
            shared_edges: vec![(1, 2)],
        };

        let memory = MemoryBroker::new(512 << 30, 448 << 30).unwrap();
        let mut events = Vec::new();
        let serial =
            build_rescue_execution_plan(&snapshot, &rescue, 1, &memory, |event| events.push(event))
                .unwrap();
        let parallel = build_rescue_execution_plan(&snapshot, &rescue, 4, &memory, |_| {}).unwrap();
        assert_eq!(serial.plan.shared_score_visits, 510);
        assert_eq!(serial.plan.total_visits(), 511);
        assert_eq!(serial.plan.matched_shared_edges, vec![(1, 2)]);
        assert_eq!(
            parallel.plan.shared_score_visits,
            serial.plan.shared_score_visits
        );
        assert_eq!(
            parallel.plan.matched_shared_edges,
            serial.plan.matched_shared_edges
        );
        for phase in [
            ProgressPhase::PrepareRescuePairs,
            ProgressPhase::FinalizeRescuePlan,
        ] {
            let phase_events = events
                .iter()
                .filter(|event| event.phase == phase)
                .collect::<Vec<_>>();
            assert!(!phase_events.is_empty());
            assert!(phase_events
                .iter()
                .all(|event| event.total_kind == TotalKind::Unknown && event.total.is_none()));
        }
    }

    #[test]
    fn rescue_execution_plan_counts_atom_scores_before_contract_expansion() {
        let dir = tempfile::tempdir().unwrap();
        let features = dir.path().join("features");
        let blocking = dir.path().join("blocking");
        let sources = (0..3)
            .map(|contract_id| EncodeSourceRow {
                contract_id,
                payload_id: contract_id,
                retained_token_ids: vec![contract_id],
            })
            .collect::<Vec<_>>();
        let payloads = (0..3)
            .map(|_| EncodePayloadRow {
                template_terms: vec![(1, 1)],
                content_terms: vec![(2, 1)],
            })
            .collect::<Vec<_>>();
        let contracts = (0..3)
            .map(|contract_id| EncodeContractRow {
                contract_id,
                chain_id: 0,
                source_doc_id: contract_id,
                payload_id: contract_id,
                weight: 1,
            })
            .collect::<Vec<_>>();
        write_encode_artifacts_with_contracts_and_atoms(
            &features,
            &sources,
            &payloads,
            &contracts,
            &[vec![0], vec![1], vec![2]],
        )
        .unwrap();
        compile_base_equivalent(
            &(0..3)
                .map(|_| AtomSketch {
                    template_simhash: 0,
                    content_simhash: 0,
                    template_anchors: vec![1],
                    content_anchors: vec![2],
                    has_template_terms: true,
                    has_content_terms: true,
                })
                .collect::<Vec<_>>(),
            &BlockingCompileConfig {
                max_routing_block_members: 10,
            },
            &blocking,
        )
        .unwrap();
        commit_ready(
            &features,
            "features.ready",
            r#"{"schema_revision":3,"source_count":3,"payload_count":3,"chains":["x"],"chain_totals":[{"name":"x","contracts":3,"nfts":3}]}"#,
        )
        .unwrap();
        commit_ready(
            &blocking,
            "blocking.ready",
            r#"{"blocking_revision":3,"atom_count":3}"#,
        )
        .unwrap();
        let snapshot = MetadataSnapshot::open(&features, &blocking).unwrap();
        let rescue = RescuePlan {
            pair_atoms: vec![0],
            shared_contracts: vec![],
            shared_edges: vec![],
        };
        let mut events = Vec::new();

        let memory = MemoryBroker::new(512 << 30, 448 << 30).unwrap();
        let admitted = build_rescue_execution_plan(&snapshot, &rescue, 1, &memory, |event| {
            events.push(event);
        })
        .unwrap();
        let plan = &admitted.plan;

        assert_eq!(plan.atom_score_visits, 2);
        assert_eq!(plan.matched_atom_pairs, vec![(0, 1), (0, 2)]);
        assert_eq!(plan.contract_expansion_visits, 2);
        assert_eq!(plan.total_visits(), 4);
        let score_terminal = events
            .iter()
            .rfind(|event| event.phase == ProgressPhase::PlanRescuePairs)
            .unwrap();
        assert_eq!(score_terminal.completed, 2);
        assert_eq!(score_terminal.total, Some(2));
        let finalize_terminal = events.last().unwrap();
        assert_eq!(finalize_terminal.phase, ProgressPhase::FinalizeRescuePlan);
        assert_eq!(finalize_terminal.total, None);
        assert_eq!(finalize_terminal.total_kind, TotalKind::Unknown);
    }

    #[test]
    fn pair_frontier_proof_uses_distinct_in_range_atoms() {
        assert!(!pair_frontier_covers_all_unordered_pairs(&[0, 0], &[1], 4));
        assert!(!pair_frontier_covers_all_unordered_pairs(&[0, 1], &[4], 4));
        assert!(pair_frontier_covers_all_unordered_pairs(&[0, 2], &[1], 4));
        assert!(pair_frontier_covers_all_unordered_pairs(&[], &[], 1));
    }

    #[test]
    fn rescue_plan_work_is_emitted_in_bounded_chunks() {
        let (sender, receiver) = std::sync::mpsc::sync_channel(4);
        let mut pending = 0u64;
        for _ in 0..10 {
            record_rescue_plan_work(&mut pending, 4, &sender);
        }
        let terminal_work = take_rescue_plan_work(&mut pending);
        drop(sender);

        let reports = receiver
            .into_iter()
            .map(|message| match message {
                RescuePlanMessage::Work(work) => work,
                RescuePlanMessage::AtomMatches(_)
                | RescuePlanMessage::SharedMatches(_)
                | RescuePlanMessage::RowDone(_)
                | RescuePlanMessage::Error(_) => panic!("unexpected rescue result message"),
            })
            .collect::<Vec<_>>();
        assert_eq!(reports, vec![4, 4]);
        assert_eq!(terminal_work, 2);
    }

    #[test]
    fn exhaustive_evidence_requires_full_shared_pair_population() {
        assert!(!evidence_scan_is_exhaustive(
            true, true, 4_000_000, 10_000_000
        ));
        assert!(evidence_scan_is_exhaustive(
            true, true, 10_000_000, 10_000_000
        ));
        assert!(!evidence_scan_is_exhaustive(
            false, true, 10_000_000, 10_000_000
        ));
    }

    #[test]
    fn shared_token_sample_stratifies_pair_work_across_both_partitions() {
        // Four active groups: two with 2 members (work=1), two with 16
        // members (work=120). Each alternating partition must see both strata.
        let offsets = vec![0, 2, 4, 20, 36];
        let sampled = stratified_active_token_sample(&offsets, 4);
        assert_eq!(sampled.len(), 4);
        let strata = |tokens: &[u32]| {
            tokens
                .iter()
                .map(|&token| shared_token_work_stratum(&offsets, token).unwrap())
                .collect::<std::collections::BTreeSet<_>>()
        };
        assert_eq!(
            strata(&sampled.iter().step_by(2).copied().collect::<Vec<_>>()).len(),
            2
        );
        assert_eq!(
            strata(
                &sampled
                    .iter()
                    .skip(1)
                    .step_by(2)
                    .copied()
                    .collect::<Vec<_>>()
            )
            .len(),
            2
        );
    }

    #[test]
    fn shared_token_sample_keeps_even_positions_without_retaining_all_tokens() {
        let offsets = (0..=10).map(|token| token * 2).collect::<Vec<_>>();

        assert_eq!(
            stratified_active_token_sample(&offsets, 4),
            vec![0, 2, 5, 7]
        );
    }

    #[test]
    fn conservative_match_storage_shortage_is_advisory() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = StorageBroker::open_with_physical_free(directory.path(), 1_000).unwrap();
        let mut advisories = Vec::new();

        let lease = reserve_pipeline_storage_advisory(
            &mut storage,
            ArtifactClass::ComponentSnapshot,
            800,
            400,
            "test components",
            &mut |message| advisories.push(message.to_string()),
        )
        .unwrap();

        assert!(lease.is_none());
        assert_eq!(advisories.len(), 1);
        assert!(advisories[0].contains("continuing without a reservation"));
    }

    #[test]
    fn catalog_scores_one_representative_per_atom_pair_before_expansion() {
        let dir = tempfile::tempdir().unwrap();
        let features = dir.path().join("features");
        let blocking = dir.path().join("blocking");
        let sources = (0..16)
            .map(|contract_id| EncodeSourceRow {
                contract_id,
                payload_id: u32::from(contract_id >= 8),
                retained_token_ids: vec![contract_id],
            })
            .collect::<Vec<_>>();
        let contracts = (0..16)
            .map(|contract_id| EncodeContractRow {
                contract_id,
                chain_id: 0,
                source_doc_id: contract_id,
                payload_id: u32::from(contract_id >= 8),
                weight: 1,
            })
            .collect::<Vec<_>>();
        let payloads = vec![
            EncodePayloadRow {
                template_terms: vec![(1, 1)],
                content_terms: vec![(2, 1)],
            },
            EncodePayloadRow {
                template_terms: vec![(1, 1)],
                content_terms: vec![(2, 1)],
            },
        ];
        write_encode_artifacts_with_contracts_and_atoms(
            &features,
            &sources,
            &payloads,
            &contracts,
            &[(0..8).collect(), (8..16).collect()],
        )
        .unwrap();
        compile_base_equivalent(
            &[
                AtomSketch {
                    template_simhash: 0,
                    content_simhash: 0,
                    template_anchors: vec![1],
                    content_anchors: vec![2],
                    has_template_terms: true,
                    has_content_terms: true,
                },
                AtomSketch {
                    template_simhash: 0,
                    content_simhash: 0,
                    template_anchors: vec![1],
                    content_anchors: vec![2],
                    has_template_terms: true,
                    has_content_terms: true,
                },
            ],
            &BlockingCompileConfig {
                max_routing_block_members: 10,
            },
            &blocking,
        )
        .unwrap();
        commit_ready(
            &features,
            "features.ready",
            r#"{"schema_revision":3,"source_count":16,"payload_count":2,"chains":["x"],"chain_totals":[{"name":"x","contracts":16,"nfts":16}]}"#,
        )
        .unwrap();
        commit_ready(
            &blocking,
            "blocking.ready",
            r#"{"blocking_revision":3,"atom_count":2}"#,
        )
        .unwrap();
        let snapshot = MetadataSnapshot::open(&features, &blocking).unwrap();
        assert_eq!(
            catalog_atom_score(&snapshot, 0, 1, false),
            catalog_atom_score(&snapshot, 0, 1, true),
            "hot-block template pushdown must preserve the exact scoring decision"
        );
        let mut edges = Vec::new();
        reset_catalog_atom_score_count();

        let work = expand_catalog_atom_pair(&snapshot, 0, 1, |left, right| {
            edges.push((left, right));
        })
        .unwrap();

        assert_eq!(work, 64);
        let expected = (8..16)
            .map(|right| (0, right))
            .chain((1..8).map(|left| (left, 8)))
            .collect::<Vec<_>>();
        assert_eq!(edges, expected);
        assert_eq!(edges.len(), 8 + 8 - 1);
        assert_eq!(catalog_atom_score_count(), 1);

        let mut planning_events = Vec::new();
        let _ = max_shared_group_index_bytes_with_progress(&snapshot, |event| {
            planning_events.push(event);
        })
        .unwrap();
        let terminal = planning_events.last().unwrap();
        assert_eq!(terminal.phase, ProgressPhase::PlanSharedTokenPairs);
        assert_eq!(terminal.completed, terminal.total.unwrap());
    }
}
