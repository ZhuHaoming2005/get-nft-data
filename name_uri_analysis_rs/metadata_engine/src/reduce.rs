//! Bounded connectivity forest runs and deterministic component reduction.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::path::Path;
use std::sync::Arc;

use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Edge {
    pub left: u32,
    pub right: u32,
}
impl Edge {
    pub fn new(a: u32, b: u32) -> Self {
        Self {
            left: a.min(b),
            right: a.max(b),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct EdgeBudget {
    pub max_buffer_bytes: u64,
    pub max_run_edges: u64,
    pub max_total_bytes: u64,
}

#[derive(Debug, Error)]
pub enum ReduceError {
    #[error("component reduction work overflow")]
    WorkOverflow,
    #[error("edge budget exceeded for {resource}: requested {requested}, limit {limit}")]
    Budget {
        resource: &'static str,
        requested: u64,
        limit: u64,
    },
    #[error("edge endpoint {endpoint} outside node_count {node_count}")]
    Endpoint { endpoint: u32, node_count: u32 },
    #[error("component snapshot chain invalid: {0}")]
    SnapshotChain(String),
    #[error("invalid component snapshot cadence: {0}")]
    SnapshotCadence(String),
    #[error(transparent)]
    Identity(#[from] crate::identity::IdentityOverflow),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Format(#[from] crate::format::FormatError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ForestRun {
    pub node_count: u32,
    pub edges: Vec<Edge>,
}

/// Bounded collector that compacts on byte or per-contract degree triggers.
enum EdgeSortPolicy {
    Global,
    Pool(Arc<rayon::ThreadPool>),
    Serial,
}

pub struct EdgeCollector {
    node_count: u32,
    budget: EdgeBudget,
    degree_trigger: u32,
    buffer: Vec<Edge>,
    degrees: Vec<u32>,
    touched: Vec<u32>,
    runs: Vec<ForestRun>,
    compacted_bytes: u64,
    sort_policy: EdgeSortPolicy,
}

impl EdgeCollector {
    pub fn new(node_count: u32, budget: EdgeBudget, degree_trigger: u32) -> Self {
        Self {
            node_count,
            budget,
            degree_trigger: degree_trigger.max(1),
            buffer: Vec::new(),
            degrees: vec![0; node_count as usize],
            touched: Vec::new(),
            runs: Vec::new(),
            compacted_bytes: 0,
            sort_policy: EdgeSortPolicy::Global,
        }
    }

    pub(crate) fn new_with_pool(
        node_count: u32,
        budget: EdgeBudget,
        degree_trigger: u32,
        worker_pool: Arc<rayon::ThreadPool>,
    ) -> Self {
        let mut collector = Self::new(node_count, budget, degree_trigger);
        collector.sort_policy = EdgeSortPolicy::Pool(worker_pool);
        collector
    }

    pub(crate) fn use_serial_sort(&mut self) {
        self.sort_policy = EdgeSortPolicy::Serial;
    }

    pub(crate) fn use_worker_pool(&mut self, worker_pool: Arc<rayon::ThreadPool>) {
        self.sort_policy = EdgeSortPolicy::Pool(worker_pool);
    }
    pub fn push(&mut self, edge: Edge) -> Result<(), ReduceError> {
        let next_bytes = (self.buffer.len().saturating_add(1) * std::mem::size_of::<Edge>()) as u64;
        if next_bytes > self.budget.max_buffer_bytes {
            self.flush()?;
        }
        for endpoint in [edge.left, edge.right] {
            if endpoint >= self.node_count {
                return Err(ReduceError::Endpoint {
                    endpoint,
                    node_count: self.node_count,
                });
            }
            let degree = &mut self.degrees[endpoint as usize];
            if *degree == 0 {
                self.touched.push(endpoint)
            }
            *degree = degree.saturating_add(1);
        }
        self.buffer.push(edge);
        if self.degrees[edge.left as usize] >= self.degree_trigger
            || self.degrees[edge.right as usize] >= self.degree_trigger
        {
            self.flush()?
        }
        Ok(())
    }
    pub fn finish(mut self) -> Result<Vec<ForestRun>, ReduceError> {
        self.flush()?;
        Ok(self.runs)
    }
    pub fn retained_bytes(&self) -> u64 {
        self.compacted_bytes.saturating_add(
            (self.buffer.len() as u64).saturating_mul(std::mem::size_of::<Edge>() as u64),
        )
    }
    pub fn compact_retained(&mut self) -> Result<(), ReduceError> {
        self.flush()?;
        if self.runs.len() > 1 {
            self.merge_runs()?;
        }
        Ok(())
    }
    fn flush(&mut self) -> Result<(), ReduceError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let edges = std::mem::take(&mut self.buffer);
        let budget = EdgeBudget {
            max_buffer_bytes: self.budget.max_buffer_bytes,
            max_run_edges: self.budget.max_run_edges,
            max_total_bytes: u64::MAX,
        };
        let node_count = self.node_count;
        let run = match &self.sort_policy {
            EdgeSortPolicy::Global => ForestRun::from_edges(node_count, edges, budget),
            EdgeSortPolicy::Pool(pool) => {
                pool.install(move || ForestRun::from_edges(node_count, edges, budget))
            }
            EdgeSortPolicy::Serial => ForestRun::from_edges_serial(node_count, edges, budget),
        }?;
        let bytes = (run.edges.len() * std::mem::size_of::<Edge>()) as u64;
        self.compacted_bytes = self.compacted_bytes.saturating_add(bytes);
        self.runs.push(run);
        if self.compacted_bytes > self.budget.max_total_bytes {
            self.merge_runs()?;
        }
        for endpoint in self.touched.drain(..) {
            self.degrees[endpoint as usize] = 0
        }
        Ok(())
    }

    fn merge_runs(&mut self) -> Result<(), ReduceError> {
        let retained = self
            .runs
            .drain(..)
            .flat_map(|run| run.edges)
            .collect::<Vec<_>>();
        let budget = EdgeBudget {
            max_buffer_bytes: u64::MAX,
            max_run_edges: u64::MAX,
            max_total_bytes: self.budget.max_total_bytes,
        };
        let node_count = self.node_count;
        let merged = match &self.sort_policy {
            EdgeSortPolicy::Global => ForestRun::from_edges(node_count, retained, budget),
            EdgeSortPolicy::Pool(pool) => {
                pool.install(move || ForestRun::from_edges(node_count, retained, budget))
            }
            EdgeSortPolicy::Serial => ForestRun::from_edges_serial(node_count, retained, budget),
        }?;
        self.compacted_bytes = (merged.edges.len() * std::mem::size_of::<Edge>()) as u64;
        self.runs.push(merged);
        Ok(())
    }
}

impl ForestRun {
    pub fn from_edges(
        node_count: u32,
        edges: impl IntoIterator<Item = Edge>,
        budget: EdgeBudget,
    ) -> Result<Self, ReduceError> {
        Self::from_edges_with_sort(node_count, edges, budget, true)
    }

    pub(crate) fn from_edges_serial(
        node_count: u32,
        edges: impl IntoIterator<Item = Edge>,
        budget: EdgeBudget,
    ) -> Result<Self, ReduceError> {
        Self::from_edges_with_sort(node_count, edges, budget, false)
    }

    fn from_edges_with_sort(
        node_count: u32,
        edges: impl IntoIterator<Item = Edge>,
        budget: EdgeBudget,
        parallel_sort: bool,
    ) -> Result<Self, ReduceError> {
        let mut edges = edges
            .into_iter()
            .filter(|e| e.left != e.right)
            .collect::<Vec<_>>();
        let bytes = (edges.len() * std::mem::size_of::<Edge>()) as u64;
        check("buffer_bytes", bytes, budget.max_buffer_bytes)?;
        if parallel_sort && edges.len() >= 16_384 {
            edges.par_sort_unstable();
        } else {
            edges.sort_unstable();
        }
        edges.dedup();
        for e in &edges {
            for x in [e.left, e.right] {
                if x >= node_count {
                    return Err(ReduceError::Endpoint {
                        endpoint: x,
                        node_count,
                    });
                }
            }
        }
        let mut dsu = Dsu::new(node_count as usize);
        let mut forest = Vec::new();
        for e in edges {
            if dsu.union(e.left, e.right) {
                forest.push(e)
            }
        }
        check("run_edges", forest.len() as u64, budget.max_run_edges)?;
        check(
            "total_bytes",
            (forest.len() * std::mem::size_of::<Edge>()) as u64,
            budget.max_total_bytes,
        )?;
        Ok(Self {
            node_count,
            edges: forest,
        })
    }
    pub fn commit(&self, dir: &Path, run_id: u32) -> Result<(), ReduceError> {
        std::fs::create_dir_all(dir)?;
        let prefix = format!("run-{run_id:06}");
        let left = self.edges.iter().map(|e| e.left).collect::<Vec<_>>();
        let right = self.edges.iter().map(|e| e.right).collect::<Vec<_>>();
        crate::format::write_u32_array(
            &dir.join(format!("{prefix}-left.u32")),
            crate::format::ArrayKind::U32,
            &left,
        )?;
        crate::format::write_u32_array(
            &dir.join(format!("{prefix}-right.u32")),
            crate::format::ArrayKind::U32,
            &right,
        )?;
        let ready=serde_json::json!({"revision":1,"node_count":self.node_count,"edge_count":self.edges.len(),"run_id":run_id}).to_string();
        crate::format::commit_ready(dir, &format!("{prefix}.ready"), &ready)?;
        Ok(())
    }

    pub fn open(dir: &Path, run_id: u32) -> Result<Self, ReduceError> {
        #[derive(Deserialize)]
        struct Ready {
            revision: u32,
            node_count: u32,
            edge_count: usize,
            run_id: u32,
        }
        let prefix = format!("run-{run_id:06}");
        let ready: Ready =
            serde_json::from_slice(&std::fs::read(dir.join(format!("{prefix}.ready")))?)?;
        if ready.revision != 1 || ready.run_id != run_id {
            return Err(ReduceError::SnapshotChain(
                "forest run revision/id mismatch".into(),
            ));
        }
        let left = crate::format::map_u32_array(&dir.join(format!("{prefix}-left.u32")))?;
        let right = crate::format::map_u32_array(&dir.join(format!("{prefix}-right.u32")))?;
        if left.len() != ready.edge_count || right.len() != ready.edge_count {
            return Err(ReduceError::SnapshotChain(
                "forest run edge count mismatch".into(),
            ));
        }
        let mut edges = Vec::with_capacity(ready.edge_count);
        for (&a, &b) in left.iter().zip(right.iter()) {
            if a >= ready.node_count || b >= ready.node_count {
                return Err(ReduceError::Endpoint {
                    endpoint: a.max(b),
                    node_count: ready.node_count,
                });
            }
            edges.push(Edge::new(a, b));
        }
        Ok(Self {
            node_count: ready.node_count,
            edges,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ComponentSnapshot {
    pub revision: u32,
    pub epoch: u32,
    pub base_epoch: Option<u32>,
    pub roots: Vec<u32>,
}

const COMPONENT_SNAPSHOT_CHAIN_REVISION: u32 = 1;

/// Immutable identity of one persisted component scope.  A chain is reusable
/// only when every field still describes the connectivity product that fed it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ComponentSnapshotIdentity {
    pub schema_revision: u32,
    pub snapshot_fingerprint: String,
    pub connectivity_revision: u32,
    pub connectivity_plan_digest: String,
    pub scope_identity: String,
    pub node_count: u32,
}

#[derive(Debug, Serialize, Deserialize)]
struct ComponentSnapshotChainManifest {
    revision: u32,
    identity: ComponentSnapshotIdentity,
    epochs: Vec<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotCadence {
    pub max_epoch_edges: u64,
    pub full_every_epochs: u32,
    pub max_replay_epochs: u32,
    pub max_replay_bytes: u64,
}

pub fn build_component_snapshot_chain(
    runs: &[ForestRun],
    node_count: u32,
    cadence: SnapshotCadence,
) -> Result<Vec<ComponentSnapshot>, ReduceError> {
    if cadence.max_epoch_edges == 0
        || cadence.full_every_epochs == 0
        || cadence.max_replay_epochs == 0
        || cadence.max_replay_bytes == 0
    {
        return Err(ReduceError::SnapshotCadence(
            "all cadence limits must be positive".into(),
        ));
    }
    let mut epochs = Vec::<std::ops::Range<usize>>::new();
    let mut epoch_start = 0usize;
    let mut current_edges = 0u64;
    for (run_index, run) in runs.iter().enumerate() {
        let run_edges = run.edges.len() as u64;
        if run_edges > cadence.max_epoch_edges {
            return Err(ReduceError::SnapshotCadence(format!(
                "run has {run_edges} edges, above epoch cap {}",
                cadence.max_epoch_edges
            )));
        }
        if run_index > epoch_start
            && current_edges.saturating_add(run_edges) > cadence.max_epoch_edges
        {
            epochs.push(epoch_start..run_index);
            epoch_start = run_index;
            current_edges = 0;
        }
        current_edges = current_edges.saturating_add(run_edges);
    }
    if epoch_start < runs.len() {
        epochs.push(epoch_start..runs.len());
    }
    if epochs.is_empty() {
        epochs.push(0..0);
    }

    let snapshot_bytes = u64::from(node_count)
        .checked_mul(std::mem::size_of::<u32>() as u64)
        .ok_or_else(|| ReduceError::SnapshotCadence("snapshot byte count overflow".into()))?;
    let mut snapshots = Vec::with_capacity(epochs.len());
    let mut replay_epochs = 0u32;
    let mut replay_bytes = 0u64;
    for (epoch, epoch_range) in epochs.into_iter().enumerate() {
        let epoch_runs = &runs[epoch_range.clone()];
        let cumulative_runs = &runs[..epoch_range.end];
        let epoch = u32::try_from(epoch)
            .map_err(|_| ReduceError::SnapshotCadence("epoch id exceeds u32".into()))?;
        let periodic_full = epoch.is_multiple_of(cadence.full_every_epochs);
        let exceeds_replay = replay_epochs.saturating_add(1) > cadence.max_replay_epochs
            || replay_bytes.saturating_add(snapshot_bytes) > cadence.max_replay_bytes;
        let start_new_chain = snapshots.is_empty() || periodic_full || exceeds_replay;
        let snapshot = if start_new_chain {
            replay_epochs = 0;
            replay_bytes = 0;
            ComponentSnapshot::full(epoch, cumulative_runs, node_count)?
        } else {
            replay_epochs = replay_epochs.saturating_add(1);
            replay_bytes = replay_bytes.saturating_add(snapshot_bytes);
            ComponentSnapshot::delta(epoch, snapshots.last().expect("non-empty"), epoch_runs)?
        };
        if start_new_chain {
            snapshots.clear();
        }
        snapshots.push(snapshot);
    }
    Ok(snapshots)
}
impl ComponentSnapshot {
    pub fn from_reduced_roots(epoch: u32, roots: Vec<u32>) -> Result<Self, ReduceError> {
        crate::identity::checked_u32_identity("component roots", roots.len() as u64)?;
        if roots.iter().any(|&root| root as usize >= roots.len()) {
            return Err(ReduceError::SnapshotChain(
                "reduced roots contain an out-of-range identity".into(),
            ));
        }
        Ok(Self {
            revision: 1,
            epoch,
            base_epoch: None,
            roots,
        })
    }

    pub fn full(epoch: u32, runs: &[ForestRun], node_count: u32) -> Result<Self, ReduceError> {
        Ok(Self {
            revision: 1,
            epoch,
            base_epoch: None,
            roots: reduce_components(runs, node_count)?,
        })
    }
    pub fn delta(epoch: u32, base: &Self, runs: &[ForestRun]) -> Result<Self, ReduceError> {
        let node_count =
            crate::identity::checked_u32_identity("component roots", base.roots.len() as u64)?;
        let mut all = Vec::new();
        for (i, &r) in base.roots.iter().enumerate() {
            if i as u32 != r {
                all.push(Edge::new(i as u32, r));
            }
        }
        for run in runs {
            all.extend(run.edges.iter().copied());
        }
        let budget = EdgeBudget {
            max_buffer_bytes: u64::MAX,
            max_run_edges: u64::MAX,
            max_total_bytes: u64::MAX,
        };
        let merged = ForestRun::from_edges(node_count, all, budget)?;
        Ok(Self {
            revision: 1,
            epoch,
            base_epoch: Some(base.epoch),
            roots: reduce_components(&[merged], node_count)?,
        })
    }

    pub fn commit(&self, dir: &Path) -> Result<(), ReduceError> {
        commit_component_snapshot_files(
            dir,
            self.revision,
            self.epoch,
            self.base_epoch,
            &self.roots,
        )
    }

    pub fn open(dir: &Path, epoch: u32) -> Result<Self, ReduceError> {
        #[derive(Deserialize)]
        struct Ready {
            revision: u32,
            epoch: u32,
            base_epoch: Option<u32>,
            #[serde(default)]
            roots_file: Option<String>,
            #[serde(default)]
            root_nodes_file: Option<String>,
            #[serde(default)]
            root_values_file: Option<String>,
            node_count: usize,
        }
        let ready: Ready = serde_json::from_slice(&std::fs::read(
            dir.join(format!("component-snapshot-{epoch:06}.ready")),
        )?)?;
        if ready.revision != 1 || ready.epoch != epoch {
            return Err(ReduceError::SnapshotChain(
                "component snapshot revision/epoch mismatch".into(),
            ));
        }
        let roots = match (
            ready.roots_file,
            ready.root_nodes_file,
            ready.root_values_file,
        ) {
            (Some(roots_file), None, None) => {
                crate::format::map_u32_array(&dir.join(roots_file))?.to_vec()
            }
            (None, Some(nodes_file), Some(values_file)) => {
                let nodes = crate::format::map_u32_array(&dir.join(nodes_file))?;
                let values = crate::format::map_u32_array(&dir.join(values_file))?;
                if nodes.len() != values.len() {
                    return Err(ReduceError::SnapshotChain(
                        "sparse component roots have mismatched columns".into(),
                    ));
                }
                let mut roots = (0..ready.node_count as u32).collect::<Vec<_>>();
                let mut previous = None;
                for (&node, &root) in nodes.iter().zip(values.iter()) {
                    if node as usize >= ready.node_count
                        || root as usize >= ready.node_count
                        || previous.is_some_and(|last| node <= last)
                    {
                        return Err(ReduceError::SnapshotChain(
                            "sparse component roots are invalid".into(),
                        ));
                    }
                    roots[node as usize] = root;
                    previous = Some(node);
                }
                roots
            }
            _ => {
                return Err(ReduceError::SnapshotChain(
                    "component snapshot root storage is invalid".into(),
                ));
            }
        };
        if roots.len() != ready.node_count
            || roots.iter().any(|&root| root as usize >= ready.node_count)
        {
            return Err(ReduceError::SnapshotChain(
                "component snapshot roots invalid".into(),
            ));
        }
        Ok(Self {
            revision: ready.revision,
            epoch,
            base_epoch: ready.base_epoch,
            roots,
        })
    }
}

/// Persist a complete, newest full+delta chain and publish its bound identity
/// only after every checksummed snapshot is durable.
pub fn commit_component_snapshot_chain(
    dir: &Path,
    identity: &ComponentSnapshotIdentity,
    snapshots: &[ComponentSnapshot],
    mut on_committed: impl FnMut(),
) -> Result<(), ReduceError> {
    validate_component_snapshot_chain(identity, snapshots)?;
    // Epoch filenames are reused. Invalidate the old identity binding before
    // replacing them so interruption yields a cache miss, never old identity
    // metadata silently bound to new component roots.
    remove_if_exists(&dir.join("component-chain.ready"))?;
    for snapshot in snapshots {
        snapshot.commit(dir)?;
        on_committed();
    }
    let manifest = ComponentSnapshotChainManifest {
        revision: COMPONENT_SNAPSHOT_CHAIN_REVISION,
        identity: identity.clone(),
        epochs: snapshots.iter().map(|snapshot| snapshot.epoch).collect(),
    };
    crate::format::commit_ready(
        dir,
        "component-chain.ready",
        &serde_json::to_string_pretty(&manifest)?,
    )?;
    on_committed();
    Ok(())
}

/// Commit a single full component snapshot directly from the final result
/// roots. This keeps the caller's vector available without cloning it solely
/// for persistence.
pub fn commit_component_roots(
    dir: &Path,
    identity: &ComponentSnapshotIdentity,
    roots: &[u32],
    mut on_committed: impl FnMut(),
) -> Result<(), ReduceError> {
    if roots.len() != identity.node_count as usize
        || roots.iter().any(|&root| root as usize >= roots.len())
    {
        return Err(ReduceError::SnapshotChain(
            "component roots do not match snapshot identity".into(),
        ));
    }
    std::fs::create_dir_all(dir)?;
    remove_if_exists(&dir.join("component-chain.ready"))?;
    commit_component_snapshot_files(dir, 1, 0, None, roots)?;
    on_committed();
    let manifest = ComponentSnapshotChainManifest {
        revision: COMPONENT_SNAPSHOT_CHAIN_REVISION,
        identity: identity.clone(),
        epochs: vec![0],
    };
    crate::format::commit_ready(
        dir,
        "component-chain.ready",
        &serde_json::to_string_pretty(&manifest)?,
    )?;
    on_committed();
    Ok(())
}

fn commit_component_snapshot_files(
    dir: &Path,
    revision: u32,
    epoch: u32,
    base_epoch: Option<u32>,
    roots: &[u32],
) -> Result<(), ReduceError> {
    std::fs::create_dir_all(dir)?;
    let dense_name = format!("component-roots-{epoch:06}.u32");
    let nodes_name = format!("component-root-nodes-{epoch:06}.u32");
    let values_name = format!("component-root-values-{epoch:06}.u32");
    let non_identity = roots
        .iter()
        .enumerate()
        .filter(|(node, root)| **root != *node as u32)
        .count();
    // Sparse storage uses two typed-array files instead of one. Each file has
    // a 32-byte padded header and 32-byte checksum, so the extra file costs
    // the equivalent of sixteen u32 values before payload bytes are counted.
    let sparse = non_identity
        .checked_mul(2)
        .and_then(|values| values.checked_add(16))
        .is_some_and(|values| values < roots.len());
    let ready = if sparse {
        let mut nodes = Vec::with_capacity(non_identity);
        let mut values = Vec::with_capacity(non_identity);
        for (node, &root) in roots.iter().enumerate() {
            if root != node as u32 {
                nodes.push(node as u32);
                values.push(root);
            }
        }
        crate::format::write_u32_array(
            &dir.join(&nodes_name),
            crate::format::ArrayKind::U32,
            &nodes,
        )?;
        crate::format::write_u32_array(
            &dir.join(&values_name),
            crate::format::ArrayKind::U32,
            &values,
        )?;
        serde_json::json!({
            "revision": revision,
            "epoch": epoch,
            "base_epoch": base_epoch,
            "root_nodes_file": nodes_name,
            "root_values_file": values_name,
            "node_count": roots.len(),
        })
    } else {
        crate::format::write_u32_array(
            &dir.join(&dense_name),
            crate::format::ArrayKind::U32,
            roots,
        )?;
        serde_json::json!({
            "revision": revision,
            "epoch": epoch,
            "base_epoch": base_epoch,
            "roots_file": dense_name,
            "node_count": roots.len(),
        })
    };
    crate::format::commit_ready(
        dir,
        &format!("component-snapshot-{epoch:06}.ready"),
        &ready.to_string(),
    )?;
    // Publish the new manifest before retiring files referenced by the old
    // one, preserving a complete prior snapshot across interruption.
    if sparse {
        remove_if_exists(&dir.join(&dense_name))?;
    } else {
        remove_if_exists(&dir.join(&nodes_name))?;
        remove_if_exists(&dir.join(&values_name))?;
    }
    Ok(())
}

fn remove_if_exists(path: &Path) -> Result<(), std::io::Error> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

/// Open a component chain when its complete identity matches.  Missing or
/// stale identities are cache misses and may rebuild just this scope.  Once an
/// identity matches, any malformed/missing snapshot is corruption and fails
/// closed rather than silently changing the recovered result.
pub fn open_component_snapshot_chain(
    dir: &Path,
    expected: &ComponentSnapshotIdentity,
) -> Result<Option<Vec<ComponentSnapshot>>, ReduceError> {
    let ready = dir.join("component-chain.ready");
    if !ready.is_file() {
        return Ok(None);
    }
    let manifest: ComponentSnapshotChainManifest = serde_json::from_slice(&std::fs::read(ready)?)?;
    if manifest.revision != COMPONENT_SNAPSHOT_CHAIN_REVISION || manifest.identity != *expected {
        return Ok(None);
    }
    let snapshots = manifest
        .epochs
        .iter()
        .map(|&epoch| {
            ComponentSnapshot::open(dir, epoch).map_err(|error| {
                ReduceError::SnapshotChain(format!(
                    "component snapshot epoch {epoch} failed validation: {error}"
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    validate_component_snapshot_chain(expected, &snapshots)?;
    Ok(Some(snapshots))
}

fn validate_component_snapshot_chain(
    identity: &ComponentSnapshotIdentity,
    snapshots: &[ComponentSnapshot],
) -> Result<(), ReduceError> {
    if snapshots.is_empty() {
        return Err(ReduceError::SnapshotChain(
            "component snapshot chain is empty".into(),
        ));
    }
    if snapshots[0].base_epoch.is_some()
        || snapshots
            .iter()
            .skip(1)
            .any(|snapshot| snapshot.base_epoch.is_none())
    {
        return Err(ReduceError::SnapshotChain(
            "component snapshot chain must contain exactly one leading full snapshot".into(),
        ));
    }
    if snapshots.iter().any(|snapshot| {
        snapshot.revision != 1 || snapshot.roots.len() != identity.node_count as usize
    }) {
        return Err(ReduceError::SnapshotChain(
            "component snapshot chain identity/node count mismatch".into(),
        ));
    }
    recover_component_snapshots(snapshots)?;
    Ok(())
}

/// Recover the newest contiguous full+delta chain; a broken chain fails closed.
pub fn recover_component_snapshots(
    chain: &[ComponentSnapshot],
) -> Result<&ComponentSnapshot, ReduceError> {
    if chain.is_empty() {
        return Err(ReduceError::SnapshotChain("empty snapshot chain".into()));
    }
    if chain
        .windows(2)
        .any(|window| window[1].epoch <= window[0].epoch)
    {
        return Err(ReduceError::SnapshotChain(
            "snapshot epochs are not strictly increasing".into(),
        ));
    }
    let full_index = chain
        .iter()
        .rposition(|snapshot| snapshot.base_epoch.is_none())
        .ok_or_else(|| ReduceError::SnapshotChain("snapshot chain has no full base".into()))?;
    let mut previous = &chain[full_index];
    for snapshot in &chain[full_index + 1..] {
        if snapshot.base_epoch != Some(previous.epoch)
            || snapshot.epoch <= previous.epoch
            || snapshot.roots.len() != previous.roots.len()
        {
            return Err(ReduceError::SnapshotChain(format!(
                "broken delta at epoch {}",
                snapshot.epoch
            )));
        }
        previous = snapshot;
    }
    Ok(previous)
}

pub fn reduce_components(runs: &[ForestRun], node_count: u32) -> Result<Vec<u32>, ReduceError> {
    reduce_components_with_progress(runs, node_count, |_, _| {})
}

pub fn planned_reduce_work(edge_work: u64, node_count: u32) -> Result<u64, ReduceError> {
    edge_work
        .checked_add(u64::from(node_count))
        .ok_or(ReduceError::WorkOverflow)
}

struct SortedForestEdges<'a> {
    runs: &'a [ForestRun],
    heap: BinaryHeap<Reverse<(Edge, usize, usize)>>,
}

impl<'a> SortedForestEdges<'a> {
    fn new(runs: &'a [ForestRun]) -> Self {
        let heap = runs
            .iter()
            .enumerate()
            .filter_map(|(run_index, run)| {
                run.edges
                    .first()
                    .copied()
                    .map(|edge| Reverse((edge, run_index, 0)))
            })
            .collect();
        Self { runs, heap }
    }
}

impl Iterator for SortedForestEdges<'_> {
    type Item = Edge;

    fn next(&mut self) -> Option<Self::Item> {
        let Reverse((edge, run_index, edge_index)) = self.heap.pop()?;
        let next_index = edge_index + 1;
        if let Some(&next) = self.runs[run_index].edges.get(next_index) {
            self.heap.push(Reverse((next, run_index, next_index)));
        }
        Some(edge)
    }
}

pub fn reduce_components_with_progress(
    runs: &[ForestRun],
    node_count: u32,
    mut progress: impl FnMut(u64, u64),
) -> Result<Vec<u32>, ReduceError> {
    const CHUNK: usize = 16_384;
    let edge_work = runs.iter().map(|run| run.edges.len() as u64).sum::<u64>();
    let total = planned_reduce_work(edge_work, node_count)?;
    progress(0, total);
    for run in runs {
        if run.node_count != node_count {
            return Err(ReduceError::Endpoint {
                endpoint: run.node_count,
                node_count,
            });
        }
    }
    let mut dsu = Dsu::new(node_count as usize);
    let mut previous = None;
    let mut processed = 0u64;
    for edge in SortedForestEdges::new(runs) {
        if previous != Some(edge) {
            dsu.union(edge.left, edge.right);
            previous = Some(edge);
        }
        processed = processed.saturating_add(1);
        if processed.is_multiple_of(CHUNK as u64) {
            progress(processed, total);
        }
    }
    progress(edge_work, total);
    let mut roots = Vec::with_capacity(node_count as usize);
    for begin in (0..node_count as usize).step_by(CHUNK) {
        let end = begin.saturating_add(CHUNK).min(node_count as usize);
        roots.extend((begin..end).map(|index| dsu.find(index as u32)));
        progress(edge_work.saturating_add(end as u64), total);
    }
    Ok(roots)
}

fn check(resource: &'static str, requested: u64, limit: u64) -> Result<(), ReduceError> {
    if requested > limit {
        Err(ReduceError::Budget {
            resource,
            requested,
            limit,
        })
    } else {
        Ok(())
    }
}
struct Dsu {
    parent: Vec<u32>,
}
impl Dsu {
    fn new(n: usize) -> Self {
        Self {
            parent: (0..n as u32).collect(),
        }
    }
    fn find(&mut self, x: u32) -> u32 {
        let mut r = x;
        while self.parent[r as usize] != r {
            r = self.parent[r as usize];
        }
        let mut p = x;
        while self.parent[p as usize] != p {
            let next = self.parent[p as usize];
            self.parent[p as usize] = r;
            p = next;
        }
        r
    }
    fn union(&mut self, a: u32, b: u32) -> bool {
        let x = self.find(a);
        let y = self.find(b);
        if x == y {
            return false;
        }
        let (lo, hi) = (x.min(y), x.max(y));
        self.parent[hi as usize] = lo;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sorted_forest_edges_merge_runs_without_materializing_all_edges() {
        let runs = vec![
            ForestRun {
                node_count: 6,
                edges: vec![Edge::new(0, 1), Edge::new(3, 4)],
            },
            ForestRun {
                node_count: 6,
                edges: vec![Edge::new(0, 1), Edge::new(1, 2), Edge::new(4, 5)],
            },
        ];

        let merged = SortedForestEdges::new(&runs).collect::<Vec<_>>();

        assert_eq!(
            merged,
            vec![
                Edge::new(0, 1),
                Edge::new(0, 1),
                Edge::new(1, 2),
                Edge::new(3, 4),
                Edge::new(4, 5),
            ]
        );
    }

    #[test]
    fn borrowed_component_roots_commit_without_transferring_the_result_vector() {
        let dir = tempfile::tempdir().unwrap();
        let identity = ComponentSnapshotIdentity {
            schema_revision: 1,
            snapshot_fingerprint: "snapshot".into(),
            connectivity_revision: 1,
            connectivity_plan_digest: "plan".into(),
            scope_identity: "intra".into(),
            node_count: 4,
        };
        let roots = vec![0, 0, 2, 2];
        let mut committed = 0;

        commit_component_roots(dir.path(), &identity, &roots, || committed += 1).unwrap();

        assert_eq!(roots, vec![0, 0, 2, 2]);
        assert_eq!(committed, 2);
        let chain = open_component_snapshot_chain(dir.path(), &identity)
            .unwrap()
            .unwrap();
        assert_eq!(recover_component_snapshots(&chain).unwrap().roots, roots);
    }

    #[test]
    fn component_root_republish_invalidates_old_identity_before_reusing_epoch_files() {
        let dir = tempfile::tempdir().unwrap();
        let identity = ComponentSnapshotIdentity {
            schema_revision: 1,
            snapshot_fingerprint: "snapshot".into(),
            connectivity_revision: 1,
            connectivity_plan_digest: "plan".into(),
            scope_identity: "intra".into(),
            node_count: 4,
        };
        commit_component_roots(dir.path(), &identity, &[0, 0, 2, 3], || {}).unwrap();
        assert!(dir.path().join("component-chain.ready").is_file());
        let mut commits = 0;

        commit_component_roots(dir.path(), &identity, &[0, 1, 2, 2], || {
            commits += 1;
            if commits == 1 {
                assert!(!dir.path().join("component-chain.ready").exists());
            }
        })
        .unwrap();

        let chain = open_component_snapshot_chain(dir.path(), &identity)
            .unwrap()
            .unwrap();
        assert_eq!(
            recover_component_snapshots(&chain).unwrap().roots,
            vec![0, 1, 2, 2]
        );
    }

    #[test]
    fn sparse_component_roots_roundtrip_identity_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let identity = ComponentSnapshotIdentity {
            schema_revision: 1,
            snapshot_fingerprint: "snapshot".into(),
            connectivity_revision: 1,
            connectivity_plan_digest: "plan".into(),
            scope_identity: "pair-0-1".into(),
            node_count: 64,
        };
        let mut roots = (0..64).collect::<Vec<_>>();
        roots[1] = 0;

        commit_component_roots(dir.path(), &identity, &roots, || {}).unwrap();

        assert!(dir.path().join("component-root-nodes-000000.u32").is_file());
        assert!(dir
            .path()
            .join("component-root-values-000000.u32")
            .is_file());
        assert!(!dir.path().join("component-roots-000000.u32").exists());
        let chain = open_component_snapshot_chain(dir.path(), &identity)
            .unwrap()
            .unwrap();
        assert_eq!(recover_component_snapshots(&chain).unwrap().roots, roots);
    }

    #[test]
    fn tiny_component_snapshot_stays_dense_when_two_sparse_headers_cost_more() {
        let dir = tempfile::tempdir().unwrap();
        let roots = vec![0, 0, 2, 3, 4, 5, 6, 7];

        ComponentSnapshot::from_reduced_roots(0, roots.clone())
            .unwrap()
            .commit(dir.path())
            .unwrap();

        assert!(dir.path().join("component-roots-000000.u32").is_file());
        assert!(!dir.path().join("component-root-nodes-000000.u32").exists());
        assert_eq!(ComponentSnapshot::open(dir.path(), 0).unwrap().roots, roots);
    }
}
