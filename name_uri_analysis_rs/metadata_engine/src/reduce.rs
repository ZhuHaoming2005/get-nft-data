//! Bounded connectivity forest runs and deterministic component reduction.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};

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
    #[error("allocation failed for {resource}: {detail}")]
    Allocation {
        resource: &'static str,
        detail: String,
    },
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

pub struct MappedForestRun {
    node_count: u32,
    edge_count: usize,
    edges: crate::format::MappedU32Array,
}

pub enum ForestRunStorage {
    Resident(ForestRun),
    Mapped(MappedForestRun),
}

impl ForestRunStorage {
    pub fn resident(run: ForestRun) -> Self {
        Self::Resident(run)
    }

    pub fn open_mapped(dir: &Path, run_id: u32) -> Result<Self, ReduceError> {
        MappedForestRun::open(dir, run_id).map(Self::Mapped)
    }

    pub fn node_count(&self) -> u32 {
        match self {
            Self::Resident(run) => run.node_count,
            Self::Mapped(run) => run.node_count,
        }
    }

    pub fn edge_count(&self) -> usize {
        match self {
            Self::Resident(run) => run.edges.len(),
            Self::Mapped(run) => run.edge_count,
        }
    }

    pub fn resident_capacity_bytes(&self) -> u64 {
        match self {
            Self::Resident(run) => edge_capacity_bytes(&run.edges),
            Self::Mapped(_) => 0,
        }
    }

    pub fn commit(&self, dir: &Path, run_id: u32) -> Result<(), ReduceError> {
        match self {
            Self::Resident(run) => run.commit(dir, run_id),
            Self::Mapped(run) => commit_forest_edges(
                dir,
                run_id,
                run.node_count,
                run.edge_count,
                (0..run.edge_count).map(|index| run.edge(index)),
            ),
        }
    }

    pub fn is_mapped(&self) -> bool {
        matches!(self, Self::Mapped(_))
    }

    #[cfg(test)]
    pub fn materialized_edges(&self) -> Vec<Edge> {
        match self {
            Self::Resident(run) => run.edges.clone(),
            Self::Mapped(run) => (0..run.edge_count).map(|index| run.edge(index)).collect(),
        }
    }
}

/// Bounded collector that compacts on byte or per-contract degree triggers.
enum EdgeSortPolicy {
    Global,
    Serial,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EdgeCollectorScratchKind {
    Dense,
    Sparse,
}

pub(crate) struct EdgeCollectorScratch {
    state: EdgeCollectorScratchState,
}

enum EdgeCollectorScratchState {
    Dense {
        degrees: Vec<u32>,
        run_dsu: Dsu,
        touched: Vec<u32>,
    },
    Sparse {
        endpoints: Vec<u32>,
        parent: Vec<u32>,
    },
}

impl EdgeCollectorScratch {
    pub(crate) fn try_new(
        kind: EdgeCollectorScratchKind,
        node_count: u32,
        max_touched_nodes: u64,
    ) -> Result<Self, ReduceError> {
        let node_count = node_count as usize;
        let max_touched_nodes = usize::try_from(max_touched_nodes)
            .map_err(|_| ReduceError::WorkOverflow)?
            .min(node_count);
        let state = match kind {
            EdgeCollectorScratchKind::Dense => {
                let mut degrees = Vec::new();
                try_reserve_exact(&mut degrees, node_count, "scope dense degrees")?;
                degrees.resize(node_count, 0);
                let run_dsu = Dsu::try_new(node_count, "scope dense dsu")?;
                let mut touched = Vec::new();
                try_reserve_exact(&mut touched, max_touched_nodes, "scope dense touched nodes")?;
                EdgeCollectorScratchState::Dense {
                    degrees,
                    run_dsu,
                    touched,
                }
            }
            EdgeCollectorScratchKind::Sparse => {
                let mut endpoints = Vec::new();
                try_reserve_exact(&mut endpoints, max_touched_nodes, "scope sparse endpoints")?;
                let mut parent = Vec::new();
                try_reserve_exact(&mut parent, max_touched_nodes, "scope sparse dsu")?;
                EdgeCollectorScratchState::Sparse { endpoints, parent }
            }
        };
        Ok(Self { state })
    }

    fn increment_degree(&mut self, endpoint: u32) -> u32 {
        match &mut self.state {
            EdgeCollectorScratchState::Dense {
                degrees, touched, ..
            } => {
                let degree = &mut degrees[endpoint as usize];
                if *degree == 0 {
                    touched.push(endpoint);
                }
                *degree = degree.saturating_add(1);
                *degree
            }
            EdgeCollectorScratchState::Sparse { .. } => 0,
        }
    }

    fn prepare_buffer_degrees(&mut self, edges: &[Edge]) {
        if let EdgeCollectorScratchState::Dense {
            degrees, touched, ..
        } = &mut self.state
        {
            for edge in edges {
                for endpoint in [edge.left, edge.right] {
                    let degree = &mut degrees[endpoint as usize];
                    if *degree == 0 {
                        touched.push(endpoint);
                    }
                    *degree = degree.saturating_add(1);
                }
            }
        }
    }

    fn prepare_edges<'a>(
        &mut self,
        node_count: u32,
        edges: impl Iterator<Item = &'a Edge>,
    ) -> Result<(), ReduceError> {
        match &mut self.state {
            EdgeCollectorScratchState::Dense {
                degrees, touched, ..
            } => {
                for edge in edges {
                    for endpoint in [edge.left, edge.right] {
                        if endpoint >= node_count {
                            return Err(ReduceError::Endpoint {
                                endpoint,
                                node_count,
                            });
                        }
                        if degrees[endpoint as usize] == 0 {
                            degrees[endpoint as usize] = 1;
                            touched.push(endpoint);
                        }
                    }
                }
            }
            EdgeCollectorScratchState::Sparse { endpoints, parent } => {
                endpoints.clear();
                for edge in edges {
                    for endpoint in [edge.left, edge.right] {
                        if endpoint >= node_count {
                            return Err(ReduceError::Endpoint {
                                endpoint,
                                node_count,
                            });
                        }
                        endpoints.push(endpoint);
                    }
                }
                endpoints.sort_unstable();
                endpoints.dedup();
                parent.clear();
                parent.extend(0..endpoints.len() as u32);
            }
        }
        Ok(())
    }

    fn union(&mut self, edge: Edge) -> bool {
        match &mut self.state {
            EdgeCollectorScratchState::Dense { run_dsu, .. } => {
                run_dsu.union(edge.left, edge.right)
            }
            EdgeCollectorScratchState::Sparse { endpoints, parent } => {
                let left = endpoints
                    .binary_search(&edge.left)
                    .expect("prepared sparse left endpoint") as u32;
                let right = endpoints
                    .binary_search(&edge.right)
                    .expect("prepared sparse right endpoint") as u32;
                union_parent(parent, left, right)
            }
        }
    }

    fn reset(&mut self) {
        match &mut self.state {
            EdgeCollectorScratchState::Dense {
                degrees,
                run_dsu,
                touched,
            } => {
                for endpoint in touched.drain(..) {
                    degrees[endpoint as usize] = 0;
                    run_dsu.reset(endpoint);
                }
            }
            EdgeCollectorScratchState::Sparse { endpoints, parent } => {
                endpoints.clear();
                parent.clear();
            }
        }
    }
}

pub struct EdgeCollector {
    node_count: u32,
    budget: EdgeBudget,
    degree_trigger: u32,
    buffer: Vec<Edge>,
    degrees: Vec<u32>,
    run_dsu: Dsu,
    touched: Vec<u32>,
    runs: Vec<ForestRun>,
    compacted_bytes: u64,
    sort_policy: EdgeSortPolicy,
}

impl EdgeCollector {
    pub fn new(node_count: u32, budget: EdgeBudget, degree_trigger: u32) -> Self {
        let buffer_capacity = bounded_initial_edge_capacity(budget.max_buffer_bytes);
        Self {
            node_count,
            budget,
            degree_trigger: degree_trigger.max(1),
            buffer: Vec::with_capacity(buffer_capacity),
            degrees: vec![0; node_count as usize],
            run_dsu: Dsu::new(node_count as usize),
            touched: Vec::new(),
            runs: Vec::new(),
            compacted_bytes: 0,
            sort_policy: EdgeSortPolicy::Global,
        }
    }

    pub(crate) fn new_serial_shared(
        node_count: u32,
        budget: EdgeBudget,
        degree_trigger: u32,
    ) -> Result<Self, ReduceError> {
        let edge_size = std::mem::size_of::<Edge>() as u64;
        let buffer_edges = usize::try_from(budget.max_buffer_bytes / edge_size)
            .map_err(|_| ReduceError::WorkOverflow)?
            .max(1);
        let mut buffer = Vec::new();
        try_reserve_exact(&mut buffer, buffer_edges, "scope edge buffer")?;
        Ok(Self {
            node_count,
            budget,
            degree_trigger: degree_trigger.max(1),
            buffer,
            degrees: Vec::new(),
            run_dsu: Dsu { parent: Vec::new() },
            touched: Vec::new(),
            runs: Vec::new(),
            compacted_bytes: 0,
            sort_policy: EdgeSortPolicy::Serial,
        })
    }

    pub(crate) fn push_batch_shared(
        &mut self,
        edges: Vec<Edge>,
        scratch: &mut EdgeCollectorScratch,
    ) -> Result<(), ReduceError> {
        scratch.prepare_buffer_degrees(&self.buffer);
        let result = (|| {
            for edge in edges {
                for endpoint in [edge.left, edge.right] {
                    if endpoint >= self.node_count {
                        return Err(ReduceError::Endpoint {
                            endpoint,
                            node_count: self.node_count,
                        });
                    }
                }
                let next_bytes =
                    (self.buffer.len().saturating_add(1) * std::mem::size_of::<Edge>()) as u64;
                if next_bytes > self.budget.max_buffer_bytes {
                    self.flush_shared(scratch)?;
                }
                let left_degree = scratch.increment_degree(edge.left);
                let right_degree = scratch.increment_degree(edge.right);
                self.buffer.push(edge);
                if left_degree >= self.degree_trigger || right_degree >= self.degree_trigger {
                    self.flush_shared(scratch)?;
                }
            }
            Ok(())
        })();
        scratch.reset();
        result
    }

    pub(crate) fn finish_shared(
        mut self,
        scratch: &mut EdgeCollectorScratch,
    ) -> Result<Vec<ForestRun>, ReduceError> {
        scratch.prepare_buffer_degrees(&self.buffer);
        self.flush_shared(scratch)?;
        self.buffer = Vec::new();
        Ok(self.runs)
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
            (self.buffer.capacity() as u64).saturating_mul(std::mem::size_of::<Edge>() as u64),
        )
    }
    pub(crate) fn drain_compacted_runs(&mut self) -> Vec<ForestRun> {
        self.compacted_bytes = 0;
        std::mem::take(&mut self.runs)
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
        let parallel_sort = matches!(self.sort_policy, EdgeSortPolicy::Global);
        let run = ForestRun::from_edges_with_dsu(
            node_count,
            edges,
            budget,
            parallel_sort,
            &mut self.run_dsu,
        )?;
        try_reserve_exact(
            &mut self.buffer,
            bounded_initial_edge_capacity(self.budget.max_buffer_bytes),
            "edge collector buffer",
        )?;
        let bytes = edge_capacity_bytes(&run.edges);
        self.compacted_bytes = self.compacted_bytes.saturating_add(bytes);
        self.runs.push(run);
        if self.compacted_bytes > self.budget.max_total_bytes {
            self.merge_runs()?;
        }
        for endpoint in self.touched.drain(..) {
            self.degrees[endpoint as usize] = 0;
            self.run_dsu.reset(endpoint);
        }
        Ok(())
    }

    fn flush_shared(&mut self, scratch: &mut EdgeCollectorScratch) -> Result<(), ReduceError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let edges = std::mem::take(&mut self.buffer);
        let budget = EdgeBudget {
            max_buffer_bytes: self.budget.max_buffer_bytes,
            max_run_edges: self.budget.max_run_edges,
            max_total_bytes: u64::MAX,
        };
        let (run, buffer) =
            ForestRun::from_edge_vec_with_scratch(self.node_count, edges, budget, false, scratch)?;
        self.buffer = buffer;
        let bytes = edge_capacity_bytes(&run.edges);
        self.compacted_bytes = self.compacted_bytes.saturating_add(bytes);
        self.runs.push(run);
        if self.compacted_bytes > self.budget.max_total_bytes {
            self.merge_runs_shared(scratch)?;
        }
        Ok(())
    }

    pub(crate) fn compact_retained_shared(
        &mut self,
        scratch: &mut EdgeCollectorScratch,
    ) -> Result<(), ReduceError> {
        self.flush_shared(scratch)?;
        if self.runs.len() > 1 {
            self.merge_runs_shared(scratch)?;
        }
        Ok(())
    }

    fn merge_runs_shared(&mut self, scratch: &mut EdgeCollectorScratch) -> Result<(), ReduceError> {
        if self.runs.len() <= 1 {
            return Ok(());
        }
        scratch.prepare_edges(
            self.node_count,
            self.runs
                .iter()
                .flat_map(|run| run.edges.iter())
                .filter(|edge| edge.left != edge.right),
        )?;
        let largest = self
            .runs
            .iter()
            .enumerate()
            .max_by_key(|(_, run)| run.edges.capacity())
            .map(|(index, _)| index)
            .unwrap_or(0);
        let mut merged = self.runs.swap_remove(largest).edges;
        let additional = self
            .runs
            .iter()
            .map(|run| run.edges.len())
            .try_fold(0usize, usize::checked_add)
            .ok_or(ReduceError::WorkOverflow)?;
        try_reserve_exact(&mut merged, additional, "scope merged forest")?;
        let mut write = 0usize;
        for read in 0..merged.len() {
            let edge = merged[read];
            if edge.left != edge.right && scratch.union(edge) {
                merged[write] = edge;
                write += 1;
            }
        }
        merged.truncate(write);
        for run in self.runs.drain(..) {
            for edge in run.edges {
                if edge.left != edge.right && scratch.union(edge) {
                    merged.push(edge);
                }
            }
        }
        scratch.reset();
        merged.sort_unstable();
        merged.dedup();
        let merged = compact_edge_capacity(merged, "scope compacted forest")?;
        check(
            "total_bytes",
            edge_capacity_bytes(&merged),
            self.budget.max_total_bytes,
        )?;
        self.compacted_bytes = edge_capacity_bytes(&merged);
        self.runs.push(ForestRun {
            node_count: self.node_count,
            edges: merged,
        });
        Ok(())
    }

    fn merge_runs(&mut self) -> Result<(), ReduceError> {
        let largest = self
            .runs
            .iter()
            .enumerate()
            .max_by_key(|(_, run)| run.edges.capacity())
            .map(|(index, _)| index)
            .unwrap_or(0);
        let mut retained = if self.runs.is_empty() {
            Vec::new()
        } else {
            self.runs.swap_remove(largest).edges
        };
        let additional = self
            .runs
            .iter()
            .map(|run| run.edges.len())
            .try_fold(0usize, usize::checked_add)
            .ok_or(ReduceError::WorkOverflow)?;
        retained
            .try_reserve_exact(additional)
            .map_err(|_| ReduceError::WorkOverflow)?;
        for run in self.runs.drain(..) {
            retained.extend(run.edges);
        }
        let budget = EdgeBudget {
            max_buffer_bytes: u64::MAX,
            max_run_edges: u64::MAX,
            max_total_bytes: self.budget.max_total_bytes,
        };
        let node_count = self.node_count;
        let merged = match &self.sort_policy {
            EdgeSortPolicy::Global => {
                ForestRun::from_edge_vec_with_sort(node_count, retained, budget, true)
            }
            EdgeSortPolicy::Serial => {
                ForestRun::from_edge_vec_with_sort(node_count, retained, budget, false)
            }
        }?;
        self.compacted_bytes = edge_capacity_bytes(&merged.edges);
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

    fn from_edges_with_sort(
        node_count: u32,
        edges: impl IntoIterator<Item = Edge>,
        budget: EdgeBudget,
        parallel_sort: bool,
    ) -> Result<Self, ReduceError> {
        let edges = edges.into_iter().collect::<Vec<_>>();
        Self::from_edge_vec_with_sort(node_count, edges, budget, parallel_sort)
    }

    fn from_edge_vec_with_sort(
        node_count: u32,
        edges: Vec<Edge>,
        budget: EdgeBudget,
        parallel_sort: bool,
    ) -> Result<Self, ReduceError> {
        let mut dsu = Dsu::new(node_count as usize);
        Self::from_edges_with_dsu(node_count, edges, budget, parallel_sort, &mut dsu)
    }

    fn from_edge_vec_with_scratch(
        node_count: u32,
        mut edges: Vec<Edge>,
        budget: EdgeBudget,
        parallel_sort: bool,
        scratch: &mut EdgeCollectorScratch,
    ) -> Result<(Self, Vec<Edge>), ReduceError> {
        edges.retain(|edge| edge.left != edge.right);
        let bytes = (edges.len() * std::mem::size_of::<Edge>()) as u64;
        check("buffer_bytes", bytes, budget.max_buffer_bytes)?;
        if parallel_sort && edges.len() >= 16_384 {
            edges.par_sort_unstable();
        } else {
            edges.sort_unstable();
        }
        edges.dedup();
        scratch.prepare_edges(node_count, edges.iter())?;
        let mut forest_len = 0usize;
        for index in 0..edges.len() {
            let edge = edges[index];
            if scratch.union(edge) {
                edges[forest_len] = edge;
                forest_len += 1;
            }
        }
        scratch.reset();
        edges.truncate(forest_len);
        check("run_edges", edges.len() as u64, budget.max_run_edges)?;
        let forest = compact_edge_slice(&edges, "scope forest edges")?;
        check(
            "total_bytes",
            edge_capacity_bytes(&forest),
            budget.max_total_bytes,
        )?;
        edges.clear();
        Ok((
            Self {
                node_count,
                edges: forest,
            },
            edges,
        ))
    }

    fn from_edges_with_dsu(
        node_count: u32,
        mut edges: Vec<Edge>,
        budget: EdgeBudget,
        parallel_sort: bool,
        dsu: &mut Dsu,
    ) -> Result<Self, ReduceError> {
        edges.retain(|edge| edge.left != edge.right);
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
        let mut forest_len = 0usize;
        for index in 0..edges.len() {
            let edge = edges[index];
            if dsu.union(edge.left, edge.right) {
                edges[forest_len] = edge;
                forest_len += 1;
            }
        }
        edges.truncate(forest_len);
        check("run_edges", edges.len() as u64, budget.max_run_edges)?;
        let edges = compact_edge_capacity(edges, "forest edges")?;
        check(
            "total_bytes",
            edge_capacity_bytes(&edges),
            budget.max_total_bytes,
        )?;
        Ok(Self { node_count, edges })
    }
    pub fn commit(&self, dir: &Path, run_id: u32) -> Result<(), ReduceError> {
        commit_forest_edges(
            dir,
            run_id,
            self.node_count,
            self.edges.len(),
            self.edges.iter().copied(),
        )
    }

    pub fn open(dir: &Path, run_id: u32) -> Result<Self, ReduceError> {
        let mapped = MappedForestRun::open(dir, run_id)?;
        let mut edges = Vec::new();
        try_reserve_exact(&mut edges, mapped.edge_count, "opened forest run")?;
        for index in 0..mapped.edge_count {
            let edge = mapped.edge(index);
            if edge.left >= mapped.node_count || edge.right >= mapped.node_count {
                return Err(ReduceError::Endpoint {
                    endpoint: edge.left.max(edge.right),
                    node_count: mapped.node_count,
                });
            }
            edges.push(edge);
        }
        Ok(Self {
            node_count: mapped.node_count,
            edges,
        })
    }
}

fn commit_forest_edges(
    dir: &Path,
    run_id: u32,
    node_count: u32,
    edge_count: usize,
    edges: impl IntoIterator<Item = Edge>,
) -> Result<(), ReduceError> {
    std::fs::create_dir_all(dir)?;
    let prefix = format!("run-{run_id:06}");
    crate::format::write_u32_iter(
        &dir.join(format!("{prefix}-edges.u32")),
        crate::format::ArrayKind::U32,
        (edge_count as u64)
            .checked_mul(2)
            .ok_or(ReduceError::WorkOverflow)?,
        edges.into_iter().flat_map(|edge| [edge.left, edge.right]),
    )?;
    let ready = serde_json::json!({
        "revision": 2,
        "node_count": node_count,
        "edge_count": edge_count,
        "run_id": run_id
    })
    .to_string();
    crate::format::commit_ready(dir, &format!("{prefix}.ready"), &ready)?;
    Ok(())
}

impl MappedForestRun {
    fn open(dir: &Path, run_id: u32) -> Result<Self, ReduceError> {
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
        if ready.revision != 2 || ready.run_id != run_id {
            return Err(ReduceError::SnapshotChain(
                "forest run revision/id mismatch".into(),
            ));
        }
        let edges = crate::format::map_u32_array(&dir.join(format!("{prefix}-edges.u32")))?;
        let encoded_values = ready
            .edge_count
            .checked_mul(2)
            .ok_or(ReduceError::WorkOverflow)?;
        if edges.len() != encoded_values {
            return Err(ReduceError::SnapshotChain(
                "forest run edge count mismatch".into(),
            ));
        }
        Ok(Self {
            node_count: ready.node_count,
            edge_count: ready.edge_count,
            edges,
        })
    }

    fn edge(&self, index: usize) -> Edge {
        let offset = index * 2;
        Edge::new(self.edges[offset], self.edges[offset + 1])
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

/// Commit one full component snapshot in dense typed-array form so callers can
/// reopen it directly as a verified mmap without rebuilding a `Vec<u32>`.
pub fn commit_component_roots_dense(
    dir: &Path,
    identity: &ComponentSnapshotIdentity,
    roots: &[u32],
    mut on_committed: impl FnMut(),
) -> Result<(), ReduceError> {
    validate_component_roots(identity, roots)?;
    std::fs::create_dir_all(dir)?;
    remove_if_exists(&dir.join("component-chain.ready"))?;
    commit_component_snapshot_dense_files(dir, 1, 0, None, roots)?;
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

pub enum OpenComponentRoots {
    Mapped(crate::format::MappedU32Array),
    Materialized(Vec<u32>),
}

/// Open the newest component roots without materializing dense snapshots.
/// Legacy sparse/delta chains are materialized one scope at a time so callers
/// can immediately rewrite them densely and release the temporary vector.
pub fn open_component_roots(
    dir: &Path,
    expected: &ComponentSnapshotIdentity,
) -> Result<Option<OpenComponentRoots>, ReduceError> {
    let chain_ready = dir.join("component-chain.ready");
    if !chain_ready.is_file() {
        return Ok(None);
    }
    let manifest: ComponentSnapshotChainManifest =
        serde_json::from_slice(&std::fs::read(&chain_ready)?)?;
    if manifest.revision != COMPONENT_SNAPSHOT_CHAIN_REVISION || manifest.identity != *expected {
        return Ok(None);
    }
    if manifest.epochs == [0] {
        #[derive(Deserialize)]
        struct Ready {
            revision: u32,
            epoch: u32,
            base_epoch: Option<u32>,
            roots_file: Option<String>,
            node_count: usize,
        }
        let ready: Ready =
            serde_json::from_slice(&std::fs::read(dir.join("component-snapshot-000000.ready"))?)?;
        if ready.revision != 1
            || ready.epoch != 0
            || ready.base_epoch.is_some()
            || ready.node_count != expected.node_count as usize
        {
            return Err(ReduceError::SnapshotChain(
                "component snapshot identity/node count mismatch".into(),
            ));
        }
        if let Some(roots_file) = ready.roots_file {
            let roots = crate::format::map_u32_array(&dir.join(roots_file)).map_err(|error| {
                ReduceError::SnapshotChain(format!(
                    "component snapshot epoch 0 failed validation: {error}"
                ))
            })?;
            validate_component_roots(expected, &roots).map_err(|error| {
                ReduceError::SnapshotChain(format!(
                    "component snapshot epoch 0 failed validation: {error}"
                ))
            })?;
            return Ok(Some(OpenComponentRoots::Mapped(roots)));
        }
    }
    let snapshots = open_component_snapshot_chain(dir, expected)?
        .ok_or_else(|| ReduceError::SnapshotChain("matching component chain disappeared".into()))?;
    let roots = recover_component_snapshots(&snapshots)?.roots.clone();
    Ok(Some(OpenComponentRoots::Materialized(roots)))
}

pub fn component_dense_roots_path(dir: &Path) -> PathBuf {
    dir.join("component-roots-000000.u32")
}

fn validate_component_roots(
    identity: &ComponentSnapshotIdentity,
    roots: &[u32],
) -> Result<(), ReduceError> {
    if roots.len() != identity.node_count as usize
        || roots.iter().any(|&root| root as usize >= roots.len())
    {
        return Err(ReduceError::SnapshotChain(
            "component roots do not match snapshot identity".into(),
        ));
    }
    Ok(())
}

fn commit_component_snapshot_dense_files(
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
    crate::format::write_u32_array(&dir.join(&dense_name), crate::format::ArrayKind::U32, roots)?;
    let ready = serde_json::json!({
        "revision": revision,
        "epoch": epoch,
        "base_epoch": base_epoch,
        "roots_file": dense_name,
        "node_count": roots.len(),
    });
    crate::format::commit_ready(
        dir,
        &format!("component-snapshot-{epoch:06}.ready"),
        &ready.to_string(),
    )?;
    remove_if_exists(&dir.join(nodes_name))?;
    remove_if_exists(&dir.join(values_name))?;
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
        crate::format::write_u32_iter(
            &dir.join(&nodes_name),
            crate::format::ArrayKind::U32,
            non_identity as u64,
            roots
                .iter()
                .enumerate()
                .filter_map(|(node, &root)| (root != node as u32).then_some(node as u32)),
        )?;
        crate::format::write_u32_iter(
            &dir.join(&values_name),
            crate::format::ArrayKind::U32,
            non_identity as u64,
            roots
                .iter()
                .enumerate()
                .filter_map(|(node, &root)| (root != node as u32).then_some(root)),
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

pub fn reduce_components_with_progress(
    runs: &[ForestRun],
    node_count: u32,
    mut progress: impl FnMut(u64, u64) + Send,
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
    let dsu = AtomicDsu::new(node_count as usize);
    let mut roots = vec![0u32; node_count as usize];
    let channel_capacity = rayon::current_num_threads().max(1).saturating_mul(2);
    let (sender, receiver) = std::sync::mpsc::sync_channel::<u64>(channel_capacity);
    std::thread::scope(|scope| -> Result<(), ReduceError> {
        let reporter = scope.spawn(move || {
            let mut processed = 0u64;
            for delta in receiver {
                processed = processed.saturating_add(delta).min(total);
                progress(processed, total);
            }
        });
        runs.par_iter().for_each(|run| {
            run.edges.par_chunks(CHUNK).for_each(|chunk| {
                for edge in chunk {
                    dsu.union(edge.left, edge.right);
                }
                let _ = sender.send(chunk.len() as u64);
            });
        });
        roots
            .par_chunks_mut(CHUNK)
            .enumerate()
            .for_each(|(chunk_index, chunk)| {
                let begin = chunk_index.saturating_mul(CHUNK);
                for (offset, root) in chunk.iter_mut().enumerate() {
                    *root = dsu.find(begin.saturating_add(offset) as u32);
                }
                let _ = sender.send(chunk.len() as u64);
            });
        drop(sender);
        reporter.join().map_err(|_| ReduceError::WorkOverflow)
    })?;
    Ok(roots)
}

pub fn reduce_stored_components_with_progress(
    runs: &[ForestRunStorage],
    source_node_count: u32,
    target_node_count: u32,
    map_edge: impl Fn(Edge) -> Result<Edge, ReduceError> + Sync,
    mut progress: impl FnMut(u64, u64) + Send,
) -> Result<Vec<u32>, ReduceError> {
    const CHUNK: usize = 16_384;
    let edge_work = runs.iter().try_fold(0u64, |total, run| {
        total
            .checked_add(run.edge_count() as u64)
            .ok_or(ReduceError::WorkOverflow)
    })?;
    let total = planned_reduce_work(edge_work, target_node_count)?;
    progress(0, total);
    for run in runs {
        if run.node_count() != source_node_count {
            return Err(ReduceError::Endpoint {
                endpoint: run.node_count(),
                node_count: source_node_count,
            });
        }
    }
    let dsu = AtomicDsu::new(target_node_count as usize);
    let mut roots = vec![0u32; target_node_count as usize];
    let channel_capacity = rayon::current_num_threads().max(1).saturating_mul(2);
    let (sender, receiver) = std::sync::mpsc::sync_channel::<u64>(channel_capacity);
    std::thread::scope(|scope| -> Result<(), ReduceError> {
        let reporter = scope.spawn(move || {
            let mut processed = 0u64;
            for delta in receiver {
                processed = processed.saturating_add(delta).min(total);
                progress(processed, total);
            }
        });
        let process_edge = |edge: Edge| -> Result<(), ReduceError> {
            if edge.left >= source_node_count || edge.right >= source_node_count {
                return Err(ReduceError::Endpoint {
                    endpoint: edge.left.max(edge.right),
                    node_count: source_node_count,
                });
            }
            let edge = map_edge(edge)?;
            if edge.left >= target_node_count || edge.right >= target_node_count {
                return Err(ReduceError::Endpoint {
                    endpoint: edge.left.max(edge.right),
                    node_count: target_node_count,
                });
            }
            dsu.union(edge.left, edge.right);
            Ok(())
        };
        let result = runs
            .par_iter()
            .try_for_each(|run| -> Result<(), ReduceError> {
                match run {
                    ForestRunStorage::Resident(run) => {
                        run.edges.par_chunks(CHUNK).try_for_each(|chunk| {
                            for &edge in chunk {
                                process_edge(edge)?;
                            }
                            let _ = sender.send(chunk.len() as u64);
                            Ok(())
                        })
                    }
                    ForestRunStorage::Mapped(run) => {
                        let chunk_count = run.edge_count.div_ceil(CHUNK);
                        (0..chunk_count).into_par_iter().try_for_each(|chunk| {
                            let begin = chunk.saturating_mul(CHUNK);
                            let end = begin.saturating_add(CHUNK).min(run.edge_count);
                            for index in begin..end {
                                process_edge(run.edge(index))?;
                            }
                            let _ = sender.send((end - begin) as u64);
                            Ok(())
                        })
                    }
                }
            });
        if result.is_ok() {
            roots
                .par_chunks_mut(CHUNK)
                .enumerate()
                .for_each(|(chunk_index, chunk)| {
                    let begin = chunk_index.saturating_mul(CHUNK);
                    for (offset, root) in chunk.iter_mut().enumerate() {
                        *root = dsu.find(begin.saturating_add(offset) as u32);
                    }
                    let _ = sender.send(chunk.len() as u64);
                });
        }
        drop(sender);
        reporter.join().map_err(|_| ReduceError::WorkOverflow)?;
        result
    })?;
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

fn edge_capacity_bytes(edges: &Vec<Edge>) -> u64 {
    (edges.capacity() as u64).saturating_mul(std::mem::size_of::<Edge>() as u64)
}

fn bounded_initial_edge_capacity(max_buffer_bytes: u64) -> usize {
    const INITIAL_EDGE_CAP: usize = 4_096;
    usize::try_from(max_buffer_bytes / std::mem::size_of::<Edge>() as u64)
        .unwrap_or(usize::MAX)
        .clamp(1, INITIAL_EDGE_CAP)
}

fn compact_edge_slice(edges: &[Edge], resource: &'static str) -> Result<Vec<Edge>, ReduceError> {
    let mut compact = Vec::new();
    try_reserve_exact(&mut compact, edges.len(), resource)?;
    compact.extend_from_slice(edges);
    Ok(compact)
}

fn compact_edge_capacity(
    edges: Vec<Edge>,
    resource: &'static str,
) -> Result<Vec<Edge>, ReduceError> {
    if edges.len() == edges.capacity() {
        Ok(edges)
    } else {
        compact_edge_slice(&edges, resource)
    }
}

fn try_reserve_exact<T>(
    values: &mut Vec<T>,
    additional: usize,
    resource: &'static str,
) -> Result<(), ReduceError> {
    values
        .try_reserve_exact(additional)
        .map_err(|error| ReduceError::Allocation {
            resource,
            detail: error.to_string(),
        })
}

fn find_parent(parent: &mut [u32], node: u32) -> u32 {
    let mut root = node;
    while parent[root as usize] != root {
        root = parent[root as usize];
    }
    let mut cursor = node;
    while parent[cursor as usize] != cursor {
        let next = parent[cursor as usize];
        parent[cursor as usize] = root;
        cursor = next;
    }
    root
}

fn union_parent(parent: &mut [u32], left: u32, right: u32) -> bool {
    let left = find_parent(parent, left);
    let right = find_parent(parent, right);
    if left == right {
        return false;
    }
    let (low, high) = (left.min(right), left.max(right));
    parent[high as usize] = low;
    true
}

struct Dsu {
    parent: Vec<u32>,
}

struct AtomicDsu {
    parent: Vec<AtomicU32>,
}

impl AtomicDsu {
    fn new(n: usize) -> Self {
        Self {
            parent: (0..n as u32).into_par_iter().map(AtomicU32::new).collect(),
        }
    }

    fn find(&self, mut node: u32) -> u32 {
        loop {
            let parent = self.parent[node as usize].load(Ordering::Acquire);
            if parent == node {
                return node;
            }
            let grandparent = self.parent[parent as usize].load(Ordering::Acquire);
            let _ = self.parent[node as usize].compare_exchange(
                parent,
                grandparent,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
            node = grandparent;
        }
    }

    fn union(&self, left: u32, right: u32) {
        loop {
            let left_root = self.find(left);
            let right_root = self.find(right);
            if left_root == right_root {
                return;
            }
            let (low, high) = (left_root.min(right_root), left_root.max(right_root));
            if self.parent[high as usize]
                .compare_exchange(high, low, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
        }
    }
}
impl Dsu {
    fn new(n: usize) -> Self {
        Self {
            parent: (0..n as u32).collect(),
        }
    }
    fn try_new(n: usize, resource: &'static str) -> Result<Self, ReduceError> {
        let mut parent = Vec::new();
        try_reserve_exact(&mut parent, n, resource)?;
        parent.extend(0..n as u32);
        Ok(Self { parent })
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
    fn reset(&mut self, x: u32) {
        self.parent[x as usize] = x;
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
    fn parallel_atomic_reduce_preserves_minimum_component_roots_and_progress() {
        let runs = vec![
            ForestRun {
                node_count: 8,
                edges: vec![Edge::new(4, 7), Edge::new(1, 3), Edge::new(3, 7)],
            },
            ForestRun {
                node_count: 8,
                edges: vec![Edge::new(0, 2), Edge::new(2, 4), Edge::new(5, 6)],
            },
        ];
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(4)
            .build()
            .unwrap();
        let mut last = (0, 0);
        let roots = pool
            .install(|| {
                reduce_components_with_progress(&runs, 8, |completed, total| {
                    last = (completed, total);
                })
            })
            .unwrap();
        assert_eq!(roots, vec![0, 0, 0, 0, 0, 5, 5, 0]);
        assert_eq!(last, (14, 14));
    }

    #[test]
    fn edge_collector_reuses_run_dsu_across_forced_flushes() {
        let mut collector = EdgeCollector::new(
            6,
            EdgeBudget {
                max_buffer_bytes: std::mem::size_of::<Edge>() as u64,
                max_run_edges: 6,
                max_total_bytes: u64::MAX,
            },
            u32::MAX,
        );
        for edge in [Edge::new(0, 1), Edge::new(1, 2), Edge::new(3, 4)] {
            collector.push(edge).unwrap();
        }
        let runs = collector.finish().unwrap();
        let roots = reduce_components(&runs, 6).unwrap();
        assert_eq!(roots, vec![0, 0, 0, 3, 3, 5]);
    }

    #[test]
    fn shared_sparse_collector_preserves_components_without_per_scope_dense_arrays() {
        let budget = EdgeBudget {
            max_buffer_bytes: 2 * std::mem::size_of::<Edge>() as u64,
            max_run_edges: 8,
            max_total_bytes: 8 * std::mem::size_of::<Edge>() as u64,
        };
        let mut collector = EdgeCollector::new_serial_shared(8, budget, 2).unwrap();
        assert!(collector.degrees.is_empty());
        assert!(collector.run_dsu.parent.is_empty());
        let mut scratch =
            EdgeCollectorScratch::try_new(EdgeCollectorScratchKind::Sparse, 8, 8).unwrap();

        collector
            .push_batch_shared(
                vec![
                    Edge::new(0, 1),
                    Edge::new(1, 2),
                    Edge::new(2, 0),
                    Edge::new(4, 5),
                ],
                &mut scratch,
            )
            .unwrap();
        collector
            .push_batch_shared(
                vec![Edge::new(2, 3), Edge::new(5, 6), Edge::new(3, 6)],
                &mut scratch,
            )
            .unwrap();

        let runs = collector.finish_shared(&mut scratch).unwrap();
        assert_eq!(
            reduce_components(&runs, 8).unwrap(),
            vec![0, 0, 0, 0, 0, 0, 0, 7]
        );
    }

    #[test]
    fn shared_dense_collector_reuses_one_worker_scratch_across_scopes() {
        let budget = EdgeBudget {
            max_buffer_bytes: 4 * std::mem::size_of::<Edge>() as u64,
            max_run_edges: 8,
            max_total_bytes: 8 * std::mem::size_of::<Edge>() as u64,
        };
        let mut first = EdgeCollector::new_serial_shared(6, budget, u32::MAX).unwrap();
        let mut second = EdgeCollector::new_serial_shared(6, budget, u32::MAX).unwrap();
        let mut scratch =
            EdgeCollectorScratch::try_new(EdgeCollectorScratchKind::Dense, 6, 6).unwrap();

        first
            .push_batch_shared(vec![Edge::new(0, 1), Edge::new(1, 2)], &mut scratch)
            .unwrap();
        second
            .push_batch_shared(vec![Edge::new(3, 4), Edge::new(4, 5)], &mut scratch)
            .unwrap();

        assert_eq!(
            reduce_components(&first.finish_shared(&mut scratch).unwrap(), 6).unwrap(),
            vec![0, 0, 0, 3, 4, 5]
        );
        assert_eq!(
            reduce_components(&second.finish_shared(&mut scratch).unwrap(), 6).unwrap(),
            vec![0, 1, 2, 3, 3, 3]
        );
    }

    #[test]
    fn highly_compressible_flushes_do_not_retain_candidate_vector_capacity_per_run() {
        let edge_size = std::mem::size_of::<Edge>() as u64;
        let budget = EdgeBudget {
            max_buffer_bytes: 100 * edge_size,
            max_run_edges: 100,
            max_total_bytes: u64::MAX,
        };
        let mut collector = EdgeCollector::new_serial_shared(2, budget, u32::MAX).unwrap();
        let mut scratch =
            EdgeCollectorScratch::try_new(EdgeCollectorScratchKind::Dense, 2, 2).unwrap();
        let edges = (0..10_000).map(|_| Edge::new(0, 1)).collect::<Vec<_>>();

        collector.push_batch_shared(edges, &mut scratch).unwrap();
        let runs = collector.finish_shared(&mut scratch).unwrap();

        assert!(runs.len() > 50);
        assert!(runs
            .iter()
            .all(|run| run.edges.capacity() == run.edges.len()));
        assert_eq!(
            runs.iter()
                .map(|run| edge_capacity_bytes(&run.edges))
                .sum::<u64>(),
            runs.len() as u64 * edge_size
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
    fn dense_component_roots_reopen_as_verified_mmap_without_vec_rebuild() {
        let dir = tempfile::tempdir().unwrap();
        let identity = ComponentSnapshotIdentity {
            schema_revision: 1,
            snapshot_fingerprint: "snapshot".into(),
            connectivity_revision: 1,
            connectivity_plan_digest: "plan".into(),
            scope_identity: "intra".into(),
            node_count: 6,
        };
        let roots = vec![0, 0, 0, 3, 3, 5];

        commit_component_roots_dense(dir.path(), &identity, &roots, || {}).unwrap();
        let opened = open_component_roots(dir.path(), &identity)
            .unwrap()
            .unwrap();

        match opened {
            OpenComponentRoots::Mapped(mapped) => assert_eq!(&*mapped, roots.as_slice()),
            OpenComponentRoots::Materialized(_) => {
                panic!("dense component roots unexpectedly rebuilt a Vec")
            }
        }
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
