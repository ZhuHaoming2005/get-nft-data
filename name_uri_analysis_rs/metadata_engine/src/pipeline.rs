//! Snapshot-only production metadata pipeline. No DuckDB or payload API is reachable.

use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use thiserror::Error;

use crate::blocking::{
    build_base_equivalent_atom_sketches, for_each_local_base_equivalent_pair_while,
    BaseEquivalentAtomInput,
};
use crate::cascade::{score_pair, PairScoreDecision};
use crate::evidence::{
    evaluate_holdout, EvidenceError, EvidenceGatePolicy, EvidenceGateReport, HoldoutEvidence,
    RescuePlan,
};
use crate::exact_islands::{
    open_pair_exact_evidence, open_shared_token_exact_evidence, plan_exact_evidence,
    plan_shared_token_evidence, run_pair_exact_island_with_progress,
    run_shared_token_exact_islands_with_progress, ExactEvidenceBudget, PairExactEvidence,
    SharedTokenExactEvidence,
};
use crate::index::{ConservativeIndex, IndexMetrics};
use crate::progress::{ProgressCounters, ProgressEvent, ProgressPhase, WorkClass, WorkUnit};
use crate::reduce::{
    commit_component_roots, open_component_snapshot_chain, recover_component_snapshots,
    reduce_components_with_progress, ComponentSnapshotIdentity, Edge, EdgeBudget, EdgeCollector,
    ForestRun,
};
use crate::resource::MemoryBroker;
use crate::scheduler::{
    estimate_catalog_contract_pair_work, RecallPlan, UniverseBudget, WorkCatalog,
};
use crate::snapshot::{MetadataSnapshot, SnapshotError};
use crate::storage::{ArtifactClass, ArtifactRegistration, EvictionPlan, StorageBroker};

pub const DEFAULT_MAX_CANDIDATE_PAIR_VISITS: u64 = 200_000_000_000;
pub const DEFAULT_EXACT_SAMPLE_LEFTS: u64 = 1_024;
pub const DEFAULT_EXACT_PAIR_WORK: u64 = 20_000_000_000;

const CONNECTIVITY_RUN_REVISION: u32 = 2;

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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScopeComponents {
    pub intra_roots: Vec<u32>,
    pub cross_roots: Vec<u32>,
    pub chain_pair_roots: Vec<ChainPairRoots>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
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

struct SummaryRowRequest<'a> {
    scope: &'a str,
    primary: usize,
    secondary: Option<usize>,
    roots: &'a [u32],
    require_secondary: bool,
    contract_ids: &'a [u32],
}

struct DenseSummaryScratch {
    groups: Vec<GroupAccumulator>,
    touched: Vec<u32>,
}

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

struct ScopeEdgeCollectors {
    intra: EdgeCollector,
    cross: EdgeCollector,
    chain_pairs: Vec<EdgeCollector>,
    max_retained_bytes: u64,
    accepted_edges: u64,
}

type ScopeForestRuns = (Vec<ForestRun>, Vec<ForestRun>, Vec<Vec<ForestRun>>);

impl ScopeEdgeCollectors {
    fn new(
        node_count: u32,
        chain_pair_count: usize,
        budget: EdgeBudget,
        max_retained_bytes: u64,
        worker_pool: Arc<rayon::ThreadPool>,
    ) -> Self {
        Self {
            intra: EdgeCollector::new_with_pool(node_count, budget, 1_048_576, worker_pool.clone()),
            cross: EdgeCollector::new_with_pool(node_count, budget, 1_048_576, worker_pool.clone()),
            chain_pairs: (0..chain_pair_count)
                .map(|_| {
                    EdgeCollector::new_with_pool(node_count, budget, 1_048_576, worker_pool.clone())
                })
                .collect(),
            max_retained_bytes,
            accepted_edges: 0,
        }
    }

    fn push(
        &mut self,
        features: &crate::encode::FeatureView,
        chain_count: usize,
        edge: Edge,
    ) -> Result<(), crate::reduce::ReduceError> {
        let left_chain = features.contract_chain[edge.left as usize] as usize;
        let right_chain = features.contract_chain[edge.right as usize] as usize;
        if left_chain == right_chain {
            self.intra.push(edge)?;
        } else {
            self.cross.push(edge)?;
            self.chain_pairs[chain_pair_index(left_chain, right_chain, chain_count)].push(edge)?;
        }
        self.accepted_edges = self.accepted_edges.saturating_add(1);
        self.enforce_retained_budget()
    }

    fn push_compacted_catalog_batch(
        &mut self,
        batch: CompactedCatalogEdges,
    ) -> Result<(), crate::reduce::ReduceError> {
        for edge in batch.intra {
            self.intra.push(edge)?;
        }
        for edge in batch.cross {
            self.cross.push(edge)?;
        }
        for (pair, edges) in batch.chain_pairs {
            let Some(collector) = self.chain_pairs.get_mut(pair) else {
                return Err(crate::reduce::ReduceError::WorkOverflow);
            };
            for edge in edges {
                collector.push(edge)?;
            }
        }
        self.accepted_edges = self.accepted_edges.saturating_add(batch.accepted_edges);
        self.enforce_retained_budget()
    }

    fn enforce_retained_budget(&mut self) -> Result<(), crate::reduce::ReduceError> {
        let mut retained = self.retained_bytes();
        if retained > self.max_retained_bytes {
            self.intra.compact_retained()?;
            self.cross.compact_retained()?;
            for collector in &mut self.chain_pairs {
                collector.compact_retained()?;
            }
            retained = self.retained_bytes();
            if retained > self.max_retained_bytes {
                return Err(crate::reduce::ReduceError::Budget {
                    resource: "scope_forest_bytes",
                    requested: retained,
                    limit: self.max_retained_bytes,
                });
            }
        }
        Ok(())
    }

    fn retained_bytes(&self) -> u64 {
        self.intra
            .retained_bytes()
            .saturating_add(self.cross.retained_bytes())
            .saturating_add(
                self.chain_pairs
                    .iter()
                    .map(EdgeCollector::retained_bytes)
                    .sum::<u64>(),
            )
    }

    fn use_serial_compaction(&mut self) {
        self.intra.use_serial_sort();
        self.cross.use_serial_sort();
        for collector in &mut self.chain_pairs {
            collector.use_serial_sort();
        }
    }

    fn use_worker_pool(&mut self, worker_pool: Arc<rayon::ThreadPool>) {
        self.intra.use_worker_pool(worker_pool.clone());
        self.cross.use_worker_pool(worker_pool.clone());
        for collector in &mut self.chain_pairs {
            collector.use_worker_pool(worker_pool.clone());
        }
    }

    fn finish_with_progress(
        self,
        progress: &mut impl FnMut(ProgressEvent),
    ) -> Result<ScopeForestRuns, PipelineError> {
        let total = self.chain_pairs.len().saturating_add(2) as u64;
        let mut completed = 0u64;
        progress(
            ProgressEvent::determinate(
                ProgressPhase::FinalizeEdgeCollectors,
                completed,
                total,
                WorkUnit::Items,
                ProgressCounters::default(),
            )
            .with_plan(WorkClass::ReduceItems, crate::progress::TotalKind::Exact),
        );
        let intra = self.intra.finish()?;
        completed += 1;
        progress(
            ProgressEvent::determinate(
                ProgressPhase::FinalizeEdgeCollectors,
                completed,
                total,
                WorkUnit::Items,
                ProgressCounters::default(),
            )
            .with_plan(WorkClass::ReduceItems, crate::progress::TotalKind::Exact),
        );
        let cross = self.cross.finish()?;
        completed += 1;
        progress(
            ProgressEvent::determinate(
                ProgressPhase::FinalizeEdgeCollectors,
                completed,
                total,
                WorkUnit::Items,
                ProgressCounters::default(),
            )
            .with_plan(WorkClass::ReduceItems, crate::progress::TotalKind::Exact),
        );
        let mut chain_pairs = Vec::with_capacity(self.chain_pairs.len());
        for collector in self.chain_pairs {
            chain_pairs.push(collector.finish()?);
            completed += 1;
            progress(
                ProgressEvent::determinate(
                    ProgressPhase::FinalizeEdgeCollectors,
                    completed,
                    total,
                    WorkUnit::Items,
                    ProgressCounters::default(),
                )
                .with_plan(WorkClass::ReduceItems, crate::progress::TotalKind::Exact),
            );
        }
        Ok((intra, cross, chain_pairs))
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    #[error(
        "ExactEvidence gate failed: Wilson upper bound {upper_bound:.6}, maximum miss rate {limit:.6}, sample_sufficient={sample_sufficient} ({observed} residual misses across {exact_matches} exact matches)"
    )]
    EvidenceGate {
        observed: u64,
        exact_matches: u64,
        upper_bound: f64,
        limit: f64,
        sample_sufficient: bool,
    },
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
    CompactedEdges(CompactedCatalogEdges),
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
    Edges(Vec<Edge>),
    Work { pairs: u64, groups: u64 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FallbackPairTask {
    atom: usize,
    left: usize,
    right_begin: usize,
    right_end: usize,
}

#[derive(Default)]
struct FallbackPairCursor {
    atom: usize,
    left: usize,
    right: usize,
}

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
    collectors: &mut ScopeEdgeCollectors,
    chain_count: usize,
    progress: &mut impl FnMut(ProgressEvent),
) -> Result<u64, PipelineError> {
    const PAIRS_PER_TASK: usize = 16_384;
    let offsets = &snapshot.features().fallback_atom_offsets;
    let atom_count = offsets.len().saturating_sub(1);
    let total = offsets.windows(2).try_fold(0u64, |total, window| {
        checked_add_pairs(total, window[1] - window[0])
    })?;
    progress(ProgressEvent::determinate(
        ProgressPhase::FallbackPairs,
        0,
        total,
        WorkUnit::Pairs,
        ProgressCounters::default(),
    ));

    let wave_width = worker_pool.current_num_threads().max(1).saturating_mul(2);
    let mut cursor = FallbackPairCursor::default();
    let mut completed = 0u64;
    loop {
        let mut tasks = Vec::with_capacity(wave_width);
        while tasks.len() < wave_width {
            let Some(task) =
                next_fallback_pair_task(&mut cursor, atom_count, PAIRS_PER_TASK, |atom| {
                    (offsets[atom + 1] - offsets[atom]) as usize
                })
            else {
                break;
            };
            tasks.push(task);
        }
        if tasks.is_empty() {
            break;
        }
        let batches = worker_pool.install(|| {
            tasks
                .par_iter()
                .map(|task| {
                    let members = atom_contracts(snapshot, task.atom as u32);
                    let left = members[task.left];
                    let mut edges = Vec::new();
                    for &right in &members[task.right_begin..task.right_end] {
                        if !contracts_share_retained_token(snapshot.features(), left, right) {
                            edges.push(Edge::new(left, right));
                        }
                    }
                    (task.right_end - task.right_begin, edges)
                })
                .collect::<Vec<_>>()
        });
        for (work, edges) in batches {
            completed = completed.saturating_add(work as u64);
            for edge in edges {
                collectors.push(snapshot.features(), chain_count, edge)?;
            }
        }
        progress(ProgressEvent::determinate(
            ProgressPhase::FallbackPairs,
            completed.min(total),
            total,
            WorkUnit::Pairs,
            ProgressCounters {
                matched: collectors.accepted_edges,
                ..ProgressCounters::default()
            },
        ));
    }
    if completed != total {
        return Err(PipelineError::Invariant(format!(
            "fallback pair progress mismatch: completed={completed}, planned={total}"
        )));
    }
    Ok(completed)
}

fn score_catalog_parallel(
    snapshot: &MetadataSnapshot,
    catalog: &WorkCatalog,
    plan: &RecallPlan,
    lanes: usize,
    collectors: &mut ScopeEdgeCollectors,
    chain_count: usize,
    progress: &mut impl FnMut(ProgressEvent),
) -> Result<IndexMetrics, PipelineError> {
    const EDGE_BATCH: usize = 32_768;
    let routing_total = catalog.jobs.iter().try_fold(0u64, |total, job| {
        let first = job.first_block as usize;
        let end = first.saturating_add(job.block_count as usize);
        (first..end).try_fold(total, |total, block| {
            let begin = snapshot.blocking().block_atom_offsets[block];
            let end = snapshot.blocking().block_atom_offsets[block + 1];
            let members = end.saturating_sub(begin);
            total
                .checked_add(members.saturating_mul(members.saturating_sub(1)) / 2)
                .ok_or(crate::scheduler::SchedulerError::WorkOverflow)
        })
    })?;
    // Contract expansion is conditional on the atom score, so its total is
    // unknowable before this single-pass scorer runs. Report the combined
    // routing + expansion wall work honestly as indeterminate while retaining
    // the exact routing total for the invariant below.
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
    std::thread::scope(|scope| -> Result<IndexMetrics, PipelineError> {
        let producer_sender = sender.clone();
        let producer = scope.spawn(move || {
            pool.install(|| {
                const DENSE_COMPACTION_SCRATCH_BYTES: usize = 4 * 1024 * 1024;
                plan.ordered_job_ids.par_iter().for_each_init(
                    || {
                        ScopeCompactionScratch::new(
                            snapshot.features().contract_chain.len(),
                            DENSE_COMPACTION_SCRATCH_BYTES,
                        )
                    },
                    |compaction_scratch, &job_id| {
                        let Some(job) = catalog.jobs.get(job_id as usize) else {
                            return;
                        };
                        let mut batch = Vec::with_capacity(EDGE_BATCH);
                        let send_failed = std::cell::Cell::new(false);
                        let mut pending_expansion = 0u64;
                        let metrics = index.for_each_job_candidate_with_work(
                            job,
                            |a, b| {
                                if send_failed.get() {
                                    return;
                                }
                                let work =
                                    match expand_catalog_atom_pair(snapshot, a, b, |left, right| {
                                        batch.push(Edge::new(left, right));
                                        if batch.len() == EDGE_BATCH {
                                            let ready = std::mem::replace(
                                                &mut batch,
                                                Vec::with_capacity(EDGE_BATCH),
                                            );
                                            let ready = compact_catalog_scope_batch(
                                                snapshot.features(),
                                                chain_count,
                                                ready,
                                                compaction_scratch,
                                            );
                                            if producer_sender
                                                .send(CatalogMessage::CompactedEdges(ready))
                                                .is_err()
                                            {
                                                send_failed.set(true);
                                            }
                                        }
                                    }) {
                                        Ok(work) => work,
                                        Err(error) => {
                                            let _ =
                                                producer_sender.send(CatalogMessage::Error(error));
                                            send_failed.set(true);
                                            return;
                                        }
                                    };
                                pending_expansion = pending_expansion.saturating_add(work);
                                if pending_expansion >= 100_000 {
                                    if producer_sender
                                        .send(CatalogMessage::ExpansionWork(pending_expansion))
                                        .is_err()
                                    {
                                        send_failed.set(true);
                                        return;
                                    }
                                    pending_expansion = 0;
                                }
                            },
                            |work| {
                                if work > 0
                                    && producer_sender
                                        .send(CatalogMessage::RoutingWork(work))
                                        .is_err()
                                {
                                    send_failed.set(true);
                                }
                            },
                        );
                        if !batch.is_empty() {
                            let batch = compact_catalog_scope_batch(
                                snapshot.features(),
                                chain_count,
                                batch,
                                compaction_scratch,
                            );
                            if producer_sender
                                .send(CatalogMessage::CompactedEdges(batch))
                                .is_err()
                            {
                                return;
                            }
                        }
                        if pending_expansion > 0
                            && producer_sender
                                .send(CatalogMessage::ExpansionWork(pending_expansion))
                                .is_err()
                        {
                            return;
                        }
                        let _ = producer_sender.send(CatalogMessage::JobDone(metrics));
                    },
                );
            });
        });
        drop(sender);
        let mut completed = 0u64;
        let mut expanded = 0u64;
        let mut metrics = IndexMetrics::default();
        let mut collection_error = None;
        for message in receiver {
            match message {
                CatalogMessage::CompactedEdges(edges) => {
                    if collection_error.is_none() {
                        if let Err(error) = collectors.push_compacted_catalog_batch(edges) {
                            collection_error = Some(error);
                        }
                    }
                }
                CatalogMessage::RoutingWork(work) => {
                    completed = completed.saturating_add(work);
                    progress(ProgressEvent::indeterminate(
                        ProgressPhase::CatalogPairs,
                        completed.saturating_add(expanded),
                        WorkUnit::Pairs,
                        ProgressCounters {
                            candidates: metrics.routed_pairs,
                            scored: metrics.routed_pairs,
                            expanded,
                            matched: collectors.accepted_edges,
                            ..ProgressCounters::default()
                        },
                    ));
                }
                CatalogMessage::ExpansionWork(work) => {
                    expanded = expanded.saturating_add(work);
                    progress(ProgressEvent::indeterminate(
                        ProgressPhase::CatalogPairs,
                        completed.saturating_add(expanded),
                        WorkUnit::Pairs,
                        ProgressCounters {
                            candidates: metrics.routed_pairs,
                            scored: metrics.routed_pairs,
                            expanded,
                            matched: collectors.accepted_edges,
                            ..ProgressCounters::default()
                        },
                    ));
                }
                CatalogMessage::Error(error) => {
                    if collection_error.is_none() {
                        return Err(error);
                    }
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
                            matched: collectors.accepted_edges,
                            ..ProgressCounters::default()
                        },
                    ));
                }
            }
        }
        producer
            .join()
            .map_err(|_| PipelineError::Parallel("worker panicked".into()))?;
        if let Some(error) = collection_error {
            return Err(error.into());
        }
        if completed != routing_total {
            return Err(PipelineError::Invariant(format!(
                "catalog routing progress mismatch: completed={completed}, planned={routing_total}"
            )));
        }
        Ok(metrics)
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
    MemoryFirst,
    Durable,
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

pub fn run_metadata_pipeline_with_progress_and_persistence(
    features: &Path,
    blocking: &Path,
    out: &Path,
    config: &MetadataPipelineConfig,
    persistence: MatchPersistence,
    mut progress: impl FnMut(ProgressEvent),
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
    let base_candidate_pair_visits = planned_candidate_contract_pair_visits(&snapshot)?;
    if base_candidate_pair_visits > config.max_candidate_pair_visits {
        return Err(crate::reduce::ReduceError::Budget {
            resource: "candidate_pair_visits",
            requested: base_candidate_pair_visits,
            limit: config.max_candidate_pair_visits,
        }
        .into());
    }
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
    if persistence == MatchPersistence::MemoryFirst {
        clear_prior_match_artifacts_for_memory_first(&mut storage, out)?;
    }
    let mut storage_leases = vec![
        storage.reserve(
            ArtifactClass::ComponentSnapshot,
            component_artifact_bytes,
            component_partial_peak_bytes,
        )?,
        storage.reserve(ArtifactClass::Summary, 16 << 20, 16 << 20)?,
    ];
    if persistence == MatchPersistence::Durable {
        storage_leases.extend([
            storage.reserve(
                ArtifactClass::Index,
                catalog_bytes,
                catalog_bytes.min(64 << 20),
            )?,
            storage.reserve(ArtifactClass::ExactEvidence, edge_bytes, edge_bytes / 2)?,
            storage.reserve(ArtifactClass::ConnectivityRun, edge_bytes, edge_bytes)?,
        ]);
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
        let _exact_memory = memory.reserve(edge_bytes)?;
        run_pair_exact_island_with_progress(
            &snapshot,
            &samples,
            ExactEvidenceBudget {
                max_lefts: exact_plan.calibration_lefts,
                max_pair_work: exact_plan
                    .calibration_lefts
                    .saturating_mul(snapshot.atom_count().saturating_sub(1) as u64),
                max_artifact_bytes: edge_bytes / 3,
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
        let _exact_memory = memory.reserve(edge_bytes)?;
        run_pair_exact_island_with_progress(
            &snapshot,
            &holdout_samples,
            ExactEvidenceBudget {
                max_lefts: exact_plan.holdout_lefts,
                max_pair_work: exact_plan
                    .holdout_lefts
                    .saturating_mul(snapshot.atom_count().saturating_sub(1) as u64),
                max_artifact_bytes: edge_bytes / 3,
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
        let _exact_memory = memory.reserve(edge_bytes)?;
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
                max_artifact_bytes: edge_bytes / 3,
                max_lanes: config.threads.max(1),
            },
            (persistence == MatchPersistence::Durable).then_some(shared_dir.as_path()),
            &mut progress,
        )?
    };
    let rescue_plan = RescuePlan::from_calibration(
        &exact.conservative_misses,
        &shared_token_exact_evidence.calibration_misses,
    );
    let connectivity_plan_digest = connectivity_plan_digest(&rescue_plan)?;
    let rescue_json = serde_json::to_string_pretty(&serde_json::json!({
        "revision": 1,
        "schema_revision": crate::scoring::MATCH_SEMANTICS_REVISION,
        "snapshot_fingerprint": catalog.snapshot_fingerprint,
        "plan": rescue_plan,
    }))?;
    if persistence == MatchPersistence::Durable {
        crate::format::commit_ready(
            &out.join("rescue-plan-1"),
            "rescue-plan.ready",
            &rescue_json,
        )?;
    }
    let evidence_gate_report = evaluate_holdout(
        HoldoutEvidence {
            evaluated_pair_work: pair_holdout_evidence
                .pair_work
                .saturating_add(shared_token_exact_evidence.holdout_pair_work),
            exhaustive: pair_frontier_covers_all_unordered_pairs(
                &samples,
                &holdout_samples,
                snapshot.atom_count(),
            ) && shared_plan
                .covers_all_active_groups(&snapshot.features().token_member_offsets),
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
        &rescue_plan,
        config.evidence_gate_policy,
    )?;
    if !evidence_gate_report.passed {
        return Err(PipelineError::EvidenceGate {
            observed: evidence_gate_report.observed_misses,
            exact_matches: evidence_gate_report.exact_matches,
            upper_bound: evidence_gate_report.wilson_upper_bound,
            limit: config.evidence_gate_policy.max_miss_rate,
            sample_sufficient: evidence_gate_report.sample_sufficient,
        });
    }
    // Calibration freezes a deterministic rescue frontier. Holdout membership
    // never enters this plan, so the independent gate cannot mutate production.
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
    if let Some(recovered) = &recovered {
        if recovered.candidate_pair_visits > config.max_candidate_pair_visits {
            return Err(crate::reduce::ReduceError::Budget {
                resource: "candidate_pair_visits",
                requested: recovered.candidate_pair_visits,
                limit: config.max_candidate_pair_visits,
            }
            .into());
        }
    }
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
            let rescue_execution_plan = build_rescue_execution_plan(
                &snapshot,
                &rescue_plan,
                config.threads,
                config
                    .max_candidate_pair_visits
                    .saturating_sub(base_candidate_pair_visits),
                &mut progress,
            )?;
            let rescue_pair_visits = rescue_execution_plan.total_visits();
            let admitted_pair_visits = base_candidate_pair_visits
                .checked_add(rescue_pair_visits)
                .ok_or(crate::resource::MemoryError::Overflow)?;
            if admitted_pair_visits > config.max_candidate_pair_visits {
                return Err(crate::reduce::ReduceError::Budget {
                    resource: "candidate_and_rescue_pair_visits",
                    requested: admitted_pair_visits,
                    limit: config.max_candidate_pair_visits,
                }
                .into());
            }
            let mut collectors = ScopeEdgeCollectors::new(
                node_count,
                chain_pair_count,
                budget,
                edge_bytes,
                worker_pool.clone(),
            );
            // A representative fallback atom is chain-local and scoring-equivalent.
            // Enumerate token-disjoint pairs in bounded deterministic waves: scoring
            // runs in parallel, while edge admission retains lexicographic task order.
            append_fallback_atom_edges_parallel(
                &snapshot,
                &worker_pool,
                &mut collectors,
                chain_count,
                &mut progress,
            )?;
            const CATALOG_LANE_BYTES: u64 = 8 * 1024 * 1024;
            let lanes = memory
                .active_lanes(config.threads.max(1), 0, CATALOG_LANE_BYTES)
                .max(1);
            let _scorer_memory =
                memory.reserve((lanes as u64).saturating_mul(CATALOG_LANE_BYTES))?;
            // The catalog producer owns its configured Rayon lanes. Keep
            // receiver-side forest compaction serial so a flush cannot activate
            // the general worker pool concurrently and exceed --threads.
            collectors.use_serial_compaction();
            let catalog_result = score_catalog_parallel(
                &snapshot,
                &catalog,
                &recall,
                lanes,
                &mut collectors,
                chain_count,
                &mut progress,
            );
            collectors.use_worker_pool(worker_pool.clone());
            let metrics: SerializableIndexMetrics = catalog_result?.into();
            drop(_scorer_memory);
            // Large shared-token scopes use group-local BaseEquivalent routing
            // while remaining source-context isolated.
            let shared_index_bytes =
                max_shared_group_index_bytes_with_progress(&snapshot, &mut progress)?;
            let shared_lane_bytes = shared_index_bytes.saturating_add(CATALOG_LANE_BYTES).max(1);
            let shared_lanes = memory
                .active_lanes(config.threads.max(1), 0, shared_lane_bytes)
                .max(1);
            let _shared_index_mem =
                memory.reserve((shared_lanes as u64).saturating_mul(shared_lane_bytes))?;
            collectors.use_serial_compaction();
            let shared_result = append_shared_token_edges(
                &snapshot,
                shared_lanes,
                config
                    .max_candidate_pair_visits
                    .saturating_sub(admitted_pair_visits),
                &mut collectors,
                chain_count,
                &mut progress,
            );
            collectors.use_worker_pool(worker_pool.clone());
            let shared_pair_visits = shared_result?;
            let candidate_pair_visits = admitted_pair_visits
                .checked_add(shared_pair_visits)
                .ok_or(crate::resource::MemoryError::Overflow)?;
            append_rescue_edges(
                &snapshot,
                &rescue_execution_plan,
                &mut collectors,
                chain_count,
                &mut progress,
            )?;
            let accepted_edge_count = collectors.accepted_edges;
            let (intra_runs, cross_runs, pair_runs) =
                collectors.finish_with_progress(&mut progress)?;
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
    let component_total = scopes
        .iter()
        .filter(|scope| scope.needs_rebuild)
        .count()
        .saturating_mul(2) as u64;
    progress(ProgressEvent::determinate(
        ProgressPhase::CommitComponents,
        0,
        component_total,
        WorkUnit::Files,
        ProgressCounters::default(),
    ));
    let mut committed = 0u64;
    for scope in &scopes {
        if scope.needs_rebuild {
            let roots = scope
                .roots
                .as_deref()
                .ok_or_else(|| PipelineError::Invariant("missing reduced roots".into()))?;
            commit_component_roots(&scope.directory, &scope.identity, roots, || {
                committed = committed.saturating_add(1);
                progress(ProgressEvent::determinate(
                    ProgressPhase::CommitComponents,
                    committed,
                    component_total,
                    WorkUnit::Files,
                    ProgressCounters::default(),
                ));
            })?;
        }
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
    let ready = serde_json::json!({
        "schema_revision": result.schema_revision,
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
    let summary_dir = out.join("metadata-summary-1");
    progress(ProgressEvent::determinate(
        ProgressPhase::CommitArtifacts,
        0,
        1,
        WorkUnit::Items,
        ProgressCounters::default(),
    ));
    crate::format::commit_ready(&summary_dir, "metadata-summary.ready", &json)?;
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
    } else {
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
    let mut strata = std::collections::BTreeMap::<u32, Vec<u32>>::new();
    for token in 0..token_member_offsets.len().saturating_sub(1) {
        if let Some(stratum) = shared_token_work_stratum(token_member_offsets, token as u32) {
            strata.entry(stratum).or_default().push(token as u32);
        }
    }
    let target = limit.min(strata.values().map(Vec::len).sum());
    if target == 0 {
        return Vec::new();
    }

    let keys = strata.keys().copied().collect::<Vec<_>>();
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

    let mut quotas = std::collections::BTreeMap::<u32, usize>::new();
    let mut remaining = target;
    while remaining > 0 {
        let mut allocated = false;
        for &stratum in &allocation_order {
            let capacity = strata[&stratum].len();
            let used = quotas.get(&stratum).copied().unwrap_or(0);
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
            quotas.insert(stratum, used + amount);
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

    let mut sampled = Vec::with_capacity(target);
    for stratum in allocation_order {
        let quota = quotas.get(&stratum).copied().unwrap_or(0);
        if quota == 0 {
            continue;
        }
        let tokens = &strata[&stratum];
        sampled.extend((0..quota).map(|index| tokens[index * tokens.len() / quota]));
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
    if manifest.revision != 2
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
        revision: 2,
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

fn planned_candidate_contract_pair_visits(
    snapshot: &MetadataSnapshot,
) -> Result<u64, PipelineError> {
    let mut total = estimate_catalog_contract_pair_work(snapshot)?;
    for window in snapshot.features().fallback_atom_offsets.windows(2) {
        total = checked_add_pairs(total, window[1] - window[0])?;
    }
    Ok(total)
}

fn shared_group_sketches(
    features: &crate::encode::FeatureView,
    sources: &[u32],
) -> Vec<crate::blocking::AtomSketch> {
    let owned = sources
        .iter()
        .map(|&source| {
            let payload = features.source_to_payload[source as usize] as usize;
            let tr = features.payload_template_offsets[payload] as usize
                ..features.payload_template_offsets[payload + 1] as usize;
            let cr = features.payload_content_offsets[payload] as usize
                ..features.payload_content_offsets[payload + 1] as usize;
            (
                features.payload_template_terms[tr.clone()]
                    .iter()
                    .copied()
                    .zip(features.payload_template_freqs[tr].iter().copied())
                    .collect::<Vec<_>>(),
                features.payload_content_terms[cr.clone()]
                    .iter()
                    .copied()
                    .zip(features.payload_content_freqs[cr].iter().copied())
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>();
    let refs = owned
        .iter()
        .map(|(template_terms, content_terms)| BaseEquivalentAtomInput {
            template_terms,
            content_terms,
        })
        .collect::<Vec<_>>();
    build_base_equivalent_atom_sketches(&refs)
}

fn max_shared_group_index_bytes_with_progress(
    snapshot: &MetadataSnapshot,
    mut progress: impl FnMut(ProgressEvent),
) -> Result<u64, PipelineError> {
    let features = snapshot.features();
    let total = features.token_member_offsets.len().saturating_sub(1) as u64;
    progress(ProgressEvent::determinate(
        ProgressPhase::PlanSharedTokenPairs,
        0,
        total,
        WorkUnit::Items,
        ProgressCounters::default(),
    ));
    let mut maximum = 0u64;
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
                .checked_add(terms.saturating_mul(16).saturating_add(128))
                .ok_or(crate::resource::MemoryError::Overflow)?;
        }
        maximum = maximum.max(bytes);
        progress(ProgressEvent::determinate(
            ProgressPhase::PlanSharedTokenPairs,
            token as u64 + 1,
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

fn catalog_atom_score(snapshot: &MetadataSnapshot, left_atom: u32, right_atom: u32) -> bool {
    #[cfg(test)]
    CATALOG_ATOM_SCORE_CALLS.with(|calls| calls.set(calls.get().saturating_add(1)));
    score_pair(
        snapshot.features(),
        atom_payload(snapshot, left_atom),
        atom_payload(snapshot, right_atom),
    ) == PairScoreDecision::ExactMatch
}

fn expand_catalog_atom_pair(
    snapshot: &MetadataSnapshot,
    left_atom: u32,
    right_atom: u32,
    mut emit: impl FnMut(u32, u32),
) -> Result<u64, PipelineError> {
    let left_contracts = atom_contracts(snapshot, left_atom);
    let right_contracts = atom_contracts(snapshot, right_atom);
    if !catalog_atom_score(snapshot, left_atom, right_atom) {
        return Ok(0);
    }
    let work = (left_contracts.len() as u64)
        .checked_mul(right_contracts.len() as u64)
        .ok_or(crate::resource::MemoryError::Overflow)?;
    for &left in left_contracts {
        for &right in right_contracts {
            if left != right && !contracts_share_retained_token(snapshot.features(), left, right) {
                emit(left, right);
            }
        }
    }
    Ok(work)
}

#[cfg(test)]
fn reset_catalog_atom_score_count() {
    CATALOG_ATOM_SCORE_CALLS.with(|calls| calls.set(0));
}

#[cfg(test)]
fn catalog_atom_score_count() -> u64 {
    CATALOG_ATOM_SCORE_CALLS.with(std::cell::Cell::get)
}

#[derive(Debug, Default, PartialEq, Eq)]
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

enum RescuePlanMessage {
    AtomRow {
        scored: u64,
        matches: Vec<(u32, u32)>,
    },
    Shared {
        scored: u64,
        matches: Vec<(u32, u32)>,
    },
}

fn build_rescue_execution_plan(
    snapshot: &MetadataSnapshot,
    rescue: &RescuePlan,
    lanes: usize,
    max_pair_visits: u64,
    mut progress: impl FnMut(ProgressEvent),
) -> Result<RescueExecutionPlan, PipelineError> {
    let atom_count = snapshot.atom_count() as u32;
    let mut atom_score_visits = 0u64;
    let mut expansion_visit_upper_bound = 0u64;
    for &left_atom in &rescue.pair_atoms {
        if left_atom >= atom_count {
            return Err(PipelineError::Invariant(format!(
                "rescue atom {left_atom} is outside atom universe {atom_count}"
            )));
        }
        for right_atom in 0..atom_count {
            if left_atom == right_atom
                || (rescue.pair_atoms.binary_search(&right_atom).is_ok() && right_atom < left_atom)
            {
                continue;
            }
            atom_score_visits = atom_score_visits
                .checked_add(1)
                .ok_or(crate::resource::MemoryError::Overflow)?;
            expansion_visit_upper_bound = expansion_visit_upper_bound
                .checked_add(
                    (atom_contracts(snapshot, left_atom).len() as u64)
                        .checked_mul(atom_contracts(snapshot, right_atom).len() as u64)
                        .ok_or(crate::resource::MemoryError::Overflow)?,
                )
                .ok_or(crate::resource::MemoryError::Overflow)?;
        }
    }
    let features = snapshot.features();
    let mut shared_score_visits = 0u64;
    for seed in &rescue.shared_seeds {
        if seed.token_id as usize + 1 >= features.token_member_offsets.len() {
            return Err(PipelineError::Invariant(format!(
                "rescue token {} is outside token universe",
                seed.token_id
            )));
        }
        let begin = features.token_member_offsets[seed.token_id as usize] as usize;
        let end = features.token_member_offsets[seed.token_id as usize + 1] as usize;
        let contracts = &features.token_member_contracts[begin..end];
        if !contracts.contains(&seed.contract_id) {
            continue;
        }
        for &contract in contracts {
            if contract == seed.contract_id
                || (rescue
                    .shared_seeds
                    .binary_search(&crate::evidence::SharedRescueSeed {
                        token_id: seed.token_id,
                        contract_id: contract,
                    })
                    .is_ok()
                    && contract < seed.contract_id)
            {
                continue;
            }
            shared_score_visits = shared_score_visits
                .checked_add(1)
                .ok_or(crate::resource::MemoryError::Overflow)?;
            // Every successful shared score expands to exactly one edge push.
            expansion_visit_upper_bound = expansion_visit_upper_bound
                .checked_add(1)
                .ok_or(crate::resource::MemoryError::Overflow)?;
        }
    }
    let score_visits = atom_score_visits
        .checked_add(shared_score_visits)
        .ok_or(crate::resource::MemoryError::Overflow)?;
    let worst_case_pair_visits = score_visits
        .checked_add(expansion_visit_upper_bound)
        .ok_or(crate::resource::MemoryError::Overflow)?;
    if worst_case_pair_visits > max_pair_visits {
        return Err(crate::reduce::ReduceError::Budget {
            resource: "rescue_worst_case_pair_visits",
            requested: worst_case_pair_visits,
            limit: max_pair_visits,
        }
        .into());
    }
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
    std::thread::scope(|scope| -> Result<RescueExecutionPlan, PipelineError> {
        let producer_sender = sender.clone();
        let producer = scope.spawn(move || {
            pool.install(|| {
                rescue.pair_atoms.par_iter().for_each(|&left_atom| {
                    let mut scored = 0u64;
                    let mut matches = Vec::new();
                    for right_atom in 0..atom_count {
                        if left_atom == right_atom
                            || (rescue.pair_atoms.binary_search(&right_atom).is_ok()
                                && right_atom < left_atom)
                        {
                            continue;
                        }
                        scored = scored.saturating_add(1);
                        if score_pair(
                            snapshot.features(),
                            atom_payload(snapshot, left_atom),
                            atom_payload(snapshot, right_atom),
                        ) == PairScoreDecision::ExactMatch
                        {
                            matches.push((left_atom, right_atom));
                        }
                    }
                    let _ = producer_sender.send(RescuePlanMessage::AtomRow { scored, matches });
                });
            });

            let mut scored = 0u64;
            let mut matches = Vec::new();
            for seed in &rescue.shared_seeds {
                let begin = features.token_member_offsets[seed.token_id as usize] as usize;
                let end = features.token_member_offsets[seed.token_id as usize + 1] as usize;
                let contracts = &features.token_member_contracts[begin..end];
                let sources = &features.token_member_sources[begin..end];
                let Some(seed_index) = contracts
                    .iter()
                    .position(|&contract| contract == seed.contract_id)
                else {
                    continue;
                };
                let seed_payload = features.source_to_payload[sources[seed_index] as usize];
                for (offset, &contract) in contracts.iter().enumerate() {
                    if contract == seed.contract_id
                        || (rescue
                            .shared_seeds
                            .binary_search(&crate::evidence::SharedRescueSeed {
                                token_id: seed.token_id,
                                contract_id: contract,
                            })
                            .is_ok()
                            && contract < seed.contract_id)
                    {
                        continue;
                    }
                    scored = scored.saturating_add(1);
                    let payload = features.source_to_payload[sources[offset] as usize];
                    if score_pair(features, seed_payload, payload) == PairScoreDecision::ExactMatch
                    {
                        matches.push((seed.contract_id, contract));
                    }
                }
            }
            let _ = producer_sender.send(RescuePlanMessage::Shared { scored, matches });
        });
        drop(sender);

        let mut completed = 0u64;
        let mut matched_atom_pairs = Vec::new();
        let mut matched_shared_edges = Vec::new();
        for message in receiver {
            match message {
                RescuePlanMessage::AtomRow { scored, matches } => {
                    completed = completed.saturating_add(scored).min(score_visits);
                    matched_atom_pairs.extend(matches);
                }
                RescuePlanMessage::Shared { scored, matches } => {
                    completed = completed.saturating_add(scored).min(score_visits);
                    matched_shared_edges.extend(matches);
                }
            }
            progress(ProgressEvent::determinate(
                ProgressPhase::PlanRescuePairs,
                completed,
                score_visits,
                WorkUnit::Pairs,
                ProgressCounters {
                    scored: completed,
                    matched: matched_atom_pairs
                        .len()
                        .saturating_add(matched_shared_edges.len())
                        as u64,
                    ..ProgressCounters::default()
                },
            ));
        }
        producer
            .join()
            .map_err(|_| PipelineError::Parallel("rescue planner panicked".into()))?;
        matched_atom_pairs.sort_unstable();
        matched_shared_edges.sort_unstable();

        let mut contract_expansion_visits = 0u64;
        for &(left_atom, right_atom) in &matched_atom_pairs {
            contract_expansion_visits = contract_expansion_visits
                .checked_add(
                    (atom_contracts(snapshot, left_atom).len() as u64)
                        .checked_mul(atom_contracts(snapshot, right_atom).len() as u64)
                        .ok_or(crate::resource::MemoryError::Overflow)?,
                )
                .ok_or(crate::resource::MemoryError::Overflow)?;
        }
        let actual_expansion_visits = contract_expansion_visits
            .checked_add(matched_shared_edges.len() as u64)
            .ok_or(crate::resource::MemoryError::Overflow)?;
        let total = score_visits
            .checked_add(actual_expansion_visits)
            .ok_or(crate::resource::MemoryError::Overflow)?;
        if total > worst_case_pair_visits {
            return Err(PipelineError::Invariant(format!(
                "rescue actual work {total} exceeded pre-admitted upper bound {worst_case_pair_visits}"
            )));
        }
        Ok(RescueExecutionPlan {
            atom_score_visits,
            contract_expansion_visits,
            shared_score_visits,
            matched_atom_pairs,
            matched_shared_edges,
        })
    })
}

fn append_rescue_edges(
    snapshot: &MetadataSnapshot,
    plan: &RescueExecutionPlan,
    collectors: &mut ScopeEdgeCollectors,
    chain_count: usize,
    progress: &mut impl FnMut(ProgressEvent),
) -> Result<(), PipelineError> {
    let total = plan.execution_work();
    progress(ProgressEvent::determinate(
        ProgressPhase::RescuePairs,
        0,
        total,
        WorkUnit::Pairs,
        ProgressCounters::default(),
    ));
    let mut completed = 0u64;
    let mut pending = 0u64;
    for &(left_atom, right_atom) in &plan.matched_atom_pairs {
        for &left in atom_contracts(snapshot, left_atom) {
            for &right in atom_contracts(snapshot, right_atom) {
                completed = completed.saturating_add(1);
                pending = pending.saturating_add(1);
                if left != right
                    && !contracts_share_retained_token(snapshot.features(), left, right)
                {
                    collectors.push(snapshot.features(), chain_count, Edge::new(left, right))?;
                }
                if pending >= 65_536 {
                    progress(ProgressEvent::determinate(
                        ProgressPhase::RescuePairs,
                        completed,
                        total,
                        WorkUnit::Pairs,
                        ProgressCounters {
                            matched: collectors.accepted_edges,
                            ..ProgressCounters::default()
                        },
                    ));
                    pending = 0;
                }
            }
        }
    }
    let features = snapshot.features();
    for &(left, right) in &plan.matched_shared_edges {
        collectors.push(features, chain_count, Edge::new(left, right))?;
        completed = completed.saturating_add(1);
        pending = pending.saturating_add(1);
        if pending >= 65_536 {
            progress(ProgressEvent::determinate(
                ProgressPhase::RescuePairs,
                completed,
                total,
                WorkUnit::Pairs,
                ProgressCounters {
                    matched: collectors.accepted_edges,
                    ..ProgressCounters::default()
                },
            ));
            pending = 0;
        }
    }
    progress(ProgressEvent::determinate(
        ProgressPhase::RescuePairs,
        total,
        total,
        WorkUnit::Pairs,
        ProgressCounters {
            matched: collectors.accepted_edges,
            ..ProgressCounters::default()
        },
    ));
    Ok(())
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
    max_pair_visits: u64,
    collectors: &mut ScopeEdgeCollectors,
    chain_count: usize,
    progress: &mut impl FnMut(ProgressEvent),
) -> Result<u64, PipelineError> {
    let f = s.features();
    let token_count = f.token_member_offsets.len().saturating_sub(1);
    progress(ProgressEvent::indeterminate(
        ProgressPhase::SharedTokenPairs,
        0,
        WorkUnit::Pairs,
        ProgressCounters::default(),
    ));
    let small_group_pair_work =
        f.token_member_offsets
            .windows(2)
            .try_fold(0u64, |total, window| {
                let members = window[1].saturating_sub(window[0]);
                if members >= 256 {
                    return Ok(total);
                }
                checked_add_pairs(total, members)
            })?;
    if small_group_pair_work > max_pair_visits {
        return Err(crate::reduce::ReduceError::Budget {
            resource: "shared_token_candidate_pair_visits",
            requested: small_group_pair_work,
            limit: max_pair_visits,
        }
        .into());
    }
    const EDGE_BATCH: usize = 4_096;
    const WORK_BATCH: u64 = 16_384;
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(lanes.max(1))
        .thread_name(|index| format!("metadata-shared-{index}"))
        .build()
        .map_err(|error| PipelineError::Parallel(error.to_string()))?;
    let (sender, receiver) = std::sync::mpsc::sync_channel::<SharedMessage>(lanes.max(1) * 2);
    let reserved_pair_visits = std::sync::atomic::AtomicU64::new(small_group_pair_work);
    let overflow_requested = std::sync::atomic::AtomicU64::new(0);
    let cancelled = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(|scope| -> Result<u64, PipelineError> {
        let worker_sender = sender.clone();
        let producer_reserved_pair_visits = &reserved_pair_visits;
        let producer_overflow_requested = &overflow_requested;
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
                    let mut edges = Vec::with_capacity(EDGE_BATCH);
                    let mut pending_work = 0u64;
                    let mut failed = false;
                    let mut visit = |i: usize, j: usize| {
                        if failed || producer_cancelled.load(std::sync::atomic::Ordering::Acquire) {
                            return;
                        }
                        pending_work = pending_work.saturating_add(1);
                        if let Some(edge) = shared_pair_edge(f, contracts, sources, i, j) {
                            edges.push(edge);
                            if edges.len() == EDGE_BATCH {
                                let ready =
                                    std::mem::replace(&mut edges, Vec::with_capacity(EDGE_BATCH));
                                failed = worker_sender.send(SharedMessage::Edges(ready)).is_err();
                                if failed {
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
                    if contracts.len() < 256 {
                        for i in 0..contracts.len() {
                            for j in i + 1..contracts.len() {
                                visit(i, j);
                            }
                        }
                    } else {
                        let sketches = shared_group_sketches(f, sources);
                        let _ = for_each_local_base_equivalent_pair_while(&sketches, |i, j| {
                            if producer_cancelled.load(std::sync::atomic::Ordering::Acquire) {
                                return false;
                            }
                            if reserve_shared_pair_visit(
                                producer_reserved_pair_visits,
                                max_pair_visits,
                                producer_overflow_requested,
                            ) {
                                visit(i as usize, j as usize);
                                !producer_cancelled.load(std::sync::atomic::Ordering::Acquire)
                            } else {
                                producer_cancelled
                                    .store(true, std::sync::atomic::Ordering::Release);
                                false
                            }
                        });
                    }
                    if !edges.is_empty() && !failed {
                        failed = worker_sender.send(SharedMessage::Edges(edges)).is_err();
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
                SharedMessage::Edges(edges) if collection_error.is_none() => {
                    for edge in edges {
                        if let Err(error) = collectors.push(f, chain_count, edge) {
                            collection_error = Some(PipelineError::from(error));
                            cancelled.store(true, std::sync::atomic::Ordering::Release);
                            break;
                        }
                    }
                }
                SharedMessage::Work {
                    pairs,
                    groups: finished,
                } => {
                    completed = completed.saturating_add(pairs);
                    groups = groups.saturating_add(finished);
                    progress(ProgressEvent::indeterminate(
                        ProgressPhase::SharedTokenPairs,
                        completed,
                        WorkUnit::Pairs,
                        ProgressCounters {
                            groups,
                            matched: collectors.accepted_edges,
                            ..ProgressCounters::default()
                        },
                    ));
                }
                SharedMessage::Edges(_) => {}
            }
        }
        producer
            .join()
            .map_err(|_| PipelineError::Parallel("worker panicked".into()))?;
        let overflow = overflow_requested.load(std::sync::atomic::Ordering::Acquire);
        if overflow != 0 && collection_error.is_none() {
            collection_error = Some(PipelineError::from(crate::reduce::ReduceError::Budget {
                resource: "shared_token_candidate_pair_visits",
                requested: overflow,
                limit: max_pair_visits,
            }));
        }
        if let Some(error) = collection_error {
            return Err(error);
        }
        progress(ProgressEvent::determinate(
            ProgressPhase::SharedTokenPairs,
            completed,
            completed,
            WorkUnit::Pairs,
            ProgressCounters {
                groups,
                matched: collectors.accepted_edges,
                ..ProgressCounters::default()
            },
        ));
        Ok(completed)
    })
}

fn reserve_shared_pair_visit(
    reserved: &std::sync::atomic::AtomicU64,
    limit: u64,
    overflow_requested: &std::sync::atomic::AtomicU64,
) -> bool {
    let mut current = reserved.load(std::sync::atomic::Ordering::Acquire);
    loop {
        let Some(next) = current.checked_add(1) else {
            overflow_requested.store(u64::MAX, std::sync::atomic::Ordering::Release);
            return false;
        };
        if next > limit {
            let _ = overflow_requested.compare_exchange(
                0,
                next,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            );
            return false;
        }
        match reserved.compare_exchange_weak(
            current,
            next,
            std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Acquire,
        ) {
            Ok(_) => return true,
            Err(observed) => current = observed,
        }
    }
}

fn shared_pair_edge(
    f: &crate::encode::FeatureView,
    contracts: &[u32],
    sources: &[u32],
    i: usize,
    j: usize,
) -> Option<Edge> {
    let left = contracts[i];
    let right = contracts[j];
    if left == right {
        return None;
    }
    let lp = f.source_to_payload[sources[i] as usize];
    let rp = f.source_to_payload[sources[j] as usize];
    if score_pair(f, lp, rp) == PairScoreDecision::ExactMatch {
        Some(Edge::new(left, right))
    } else {
        None
    }
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
    let features = snapshot.features();
    let mut contracts_by_chain = vec![Vec::<u32>::new(); chain_count];
    for (contract, &chain) in features.contract_chain.iter().enumerate() {
        if let Some(contracts) = contracts_by_chain.get_mut(chain as usize) {
            contracts.push(contract as u32);
        }
    }
    let intra_work = snapshot.contract_count() as u64;
    let cross_work = if chain_count > 1 {
        snapshot.contract_count() as u64
    } else {
        0
    };
    let pair_work = scopes
        .chain_pair_roots
        .iter()
        .map(|pair| {
            let members = contracts_by_chain[pair.left_chain as usize]
                .len()
                .saturating_add(contracts_by_chain[pair.right_chain as usize].len());
            (members as u64).saturating_mul(2)
        })
        .sum::<u64>();
    let total = intra_work
        .saturating_add(cross_work)
        .saturating_add(pair_work);
    let mut completed = 0u64;
    let mut summary_scratch = DenseSummaryScratch::new(snapshot.contract_count());
    progress(ProgressEvent::determinate(
        ProgressPhase::BuildSummary,
        0,
        total,
        WorkUnit::Nodes,
        ProgressCounters::default(),
    ));
    let cross_stats = (chain_count > 1).then(|| {
        worker_pool.install(|| cross_summary_stats(snapshot, &scopes.cross_roots, chain_count))
    });
    let mut cross_work_reported = false;
    macro_rules! append {
        ($scope:expr, $primary:expr, $secondary:expr, $roots:expr, $require_secondary:expr, $contract_ids:expr $(,)?) => {{
            let row = summary_for_roots(
                snapshot,
                SummaryRowRequest {
                    scope: $scope,
                    primary: $primary,
                    secondary: $secondary,
                    roots: $roots,
                    require_secondary: $require_secondary,
                    contract_ids: $contract_ids,
                },
                &mut summary_scratch,
                &mut |work| {
                    completed = completed.saturating_add(work).min(total);
                    progress(ProgressEvent::determinate(
                        ProgressPhase::BuildSummary,
                        completed,
                        total,
                        WorkUnit::Nodes,
                        ProgressCounters::default(),
                    ));
                },
            );
            rows.push(row);
        }};
    }
    for chain in 0..chain_count {
        append!(
            "intra_chain",
            chain,
            None,
            &scopes.intra_roots,
            false,
            &contracts_by_chain[chain],
        );
        if let Some(stats) = cross_stats.as_ref() {
            if !cross_work_reported {
                completed = completed
                    .saturating_add(snapshot.contract_count() as u64)
                    .min(total);
                progress(ProgressEvent::determinate(
                    ProgressPhase::BuildSummary,
                    completed,
                    total,
                    WorkUnit::Nodes,
                    ProgressCounters::default(),
                ));
                cross_work_reported = true;
            }
            rows.push(summary_row_from_stats(
                snapshot,
                "cross_chain_summary",
                chain,
                None,
                stats[chain],
            ));
        }
    }
    let mut pair_contracts = Vec::new();
    for pair in &scopes.chain_pair_roots {
        pair_contracts.clear();
        pair_contracts.extend_from_slice(&contracts_by_chain[pair.left_chain as usize]);
        pair_contracts.extend_from_slice(&contracts_by_chain[pair.right_chain as usize]);
        append!(
            "chain_matrix",
            pair.left_chain as usize,
            Some(pair.right_chain as usize),
            &pair.roots,
            true,
            &pair_contracts,
        );
        append!(
            "chain_matrix",
            pair.right_chain as usize,
            Some(pair.left_chain as usize),
            &pair.roots,
            true,
            &pair_contracts,
        );
    }
    rows
}

fn summary_for_roots(
    snapshot: &MetadataSnapshot,
    request: SummaryRowRequest<'_>,
    scratch: &mut DenseSummaryScratch,
    on_work: &mut impl FnMut(u64),
) -> MetadataSummaryRow {
    const PROGRESS_CHUNK: u64 = 65_536;
    let f = snapshot.features();
    let mut pending_work = 0u64;
    let contracts = request
        .contract_ids
        .iter()
        .copied()
        .map(|contract| contract as usize)
        .filter(|&contract| contract < request.roots.len() && contract < f.contract_chain.len())
        .map(|contract| {
            let chain = f.contract_chain[contract] as usize;
            (
                contract,
                chain == request.primary,
                i64::try_from(f.contract_weight[contract]).unwrap_or(i64::MAX),
            )
        })
        .collect::<Vec<_>>();
    for _ in &contracts {
        pending_work += 1;
        if pending_work == PROGRESS_CHUNK {
            on_work(pending_work);
            pending_work = 0;
        }
    }
    if pending_work != 0 {
        on_work(pending_work);
    }
    let stats = scratch.summarize(request.roots, contracts, request.require_secondary);
    summary_row_from_stats(
        snapshot,
        request.scope,
        request.primary,
        request.secondary,
        stats,
    )
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

fn cross_summary_stats(
    snapshot: &MetadataSnapshot,
    roots: &[u32],
    chain_count: usize,
) -> Vec<SummaryStats> {
    #[derive(Clone, Copy)]
    struct Entry {
        root: u32,
        chain: u32,
        nfts: i64,
    }
    let features = snapshot.features();
    let mut entries = roots
        .iter()
        .copied()
        .enumerate()
        .take(features.contract_chain.len())
        .map(|(contract, root)| Entry {
            root,
            chain: features.contract_chain[contract],
            nfts: i64::try_from(features.contract_weight[contract]).unwrap_or(i64::MAX),
        })
        .collect::<Vec<_>>();
    entries.par_sort_unstable_by_key(|entry| (entry.root, entry.chain));
    let mut stats = vec![SummaryStats::default(); chain_count];
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
        let total = (end - begin) as i64;
        if total >= 2 && chain_entries.len() > 1 {
            for &(chain, count, nfts) in &chain_entries {
                if let Some(summary) = stats.get_mut(chain) {
                    summary.group_count += 1;
                    summary.duplicate_contract_count += count;
                    summary.duplicate_nft_count = summary.duplicate_nft_count.saturating_add(nfts);
                    summary.group_size_ge_2_count += 1;
                    summary.group_size_gt_2_count += i64::from(total > 2);
                }
            }
        }
        begin = end;
    }
    stats
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
    use crate::evidence::SharedRescueSeed;
    use crate::format::commit_ready;

    #[test]
    fn configured_worker_pool_uses_requested_thread_count() {
        let pool = build_metadata_worker_pool(2).unwrap();
        assert_eq!(pool.install(rayon::current_num_threads), 2);
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
    fn rescue_plan_counts_every_shared_seed_score_visit() {
        let dir = tempfile::tempdir().unwrap();
        let features = dir.path().join("features");
        let blocking = dir.path().join("blocking");
        let sources = (0..3)
            .map(|contract_id| EncodeSourceRow {
                contract_id,
                payload_id: contract_id,
                retained_token_ids: vec![1],
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
        let payloads = (0..3)
            .map(|id| EncodePayloadRow {
                template_terms: vec![(10 + id, 1)],
                content_terms: vec![(20 + id, 1)],
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
                .map(|id| AtomSketch {
                    template_simhash: id,
                    content_simhash: id,
                    template_anchors: vec![10 + id as u32],
                    content_anchors: vec![20 + id as u32],
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
            pair_atoms: vec![],
            shared_seeds: vec![SharedRescueSeed {
                token_id: 1,
                contract_id: 0,
            }],
        };

        let plan = build_rescue_execution_plan(&snapshot, &rescue, 1, u64::MAX, |_| {}).unwrap();
        assert_eq!(plan.shared_score_visits, 2);
        assert_eq!(plan.total_visits(), 2);
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
            shared_seeds: vec![],
        };
        let mut rejected_events = Vec::new();
        let error = build_rescue_execution_plan(&snapshot, &rescue, 1, 3, |event| {
            rejected_events.push(event)
        })
        .unwrap_err();
        assert!(error.to_string().contains("rescue_worst_case_pair_visits"));
        assert!(
            rejected_events.is_empty(),
            "worst-case expansion must be admitted before score progress starts"
        );
        let mut events = Vec::new();

        let plan = build_rescue_execution_plan(&snapshot, &rescue, 1, u64::MAX, |event| {
            events.push(event)
        })
        .unwrap();

        assert_eq!(plan.atom_score_visits, 2);
        assert_eq!(plan.matched_atom_pairs, vec![(0, 1), (0, 2)]);
        assert_eq!(plan.contract_expansion_visits, 2);
        assert_eq!(plan.total_visits(), 4);
        let terminal = events.last().unwrap();
        assert_eq!(terminal.phase, ProgressPhase::PlanRescuePairs);
        assert_eq!(terminal.completed, 2);
        assert_eq!(terminal.total, Some(2));
    }

    #[test]
    fn pair_frontier_proof_uses_distinct_in_range_atoms() {
        assert!(!pair_frontier_covers_all_unordered_pairs(&[0, 0], &[1], 4));
        assert!(!pair_frontier_covers_all_unordered_pairs(&[0, 1], &[4], 4));
        assert!(pair_frontier_covers_all_unordered_pairs(&[0, 2], &[1], 4));
        assert!(pair_frontier_covers_all_unordered_pairs(&[], &[], 1));
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
    fn catalog_scores_one_representative_per_atom_pair_before_expansion() {
        let dir = tempfile::tempdir().unwrap();
        let features = dir.path().join("features");
        let blocking = dir.path().join("blocking");
        let sources = (0..3)
            .map(|contract_id| EncodeSourceRow {
                contract_id,
                payload_id: contract_id.min(1),
                retained_token_ids: vec![contract_id],
            })
            .collect::<Vec<_>>();
        let contracts = (0..3)
            .map(|contract_id| EncodeContractRow {
                contract_id,
                chain_id: 0,
                source_doc_id: contract_id,
                payload_id: contract_id.min(1),
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
            &[vec![0, 1], vec![2]],
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
            r#"{"schema_revision":3,"source_count":3,"payload_count":2,"chains":["x"],"chain_totals":[{"name":"x","contracts":3,"nfts":3}]}"#,
        )
        .unwrap();
        commit_ready(
            &blocking,
            "blocking.ready",
            r#"{"blocking_revision":3,"atom_count":2}"#,
        )
        .unwrap();
        let snapshot = MetadataSnapshot::open(&features, &blocking).unwrap();
        let mut edges = Vec::new();
        reset_catalog_atom_score_count();

        let work = expand_catalog_atom_pair(&snapshot, 0, 1, |left, right| {
            edges.push((left, right));
        })
        .unwrap();

        assert_eq!(work, 2);
        assert_eq!(edges, vec![(0, 2), (1, 2)]);
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
