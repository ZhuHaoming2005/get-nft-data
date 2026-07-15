//! BaseEquivalent per-block Conservative candidate relation.
//!
//! The compiled blocks are the postings. This view never constructs or mmaps a
//! full Exact index and streams canonical-owner pairs to its consumer.

use crate::progress::{
    ProgressCounters, ProgressEvent, ProgressPhase, TotalKind, WorkClass, WorkUnit,
};
use crate::scheduler::{
    JobDescriptor, JobShape, RecallPlan, SchedulerError, WorkCatalog, HOT_BLOCK_TILE,
};
use crate::snapshot::{BlockingView, MetadataSnapshot};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IndexMetrics {
    pub block_pair_visits: u64,
    pub contract_pair_visits: u64,
    pub routed_pairs: u64,
    pub duplicate_routes: u64,
    pub exact_full_build_bytes: u64,
    pub exact_full_mmap_bytes: u64,
}

pub struct ConservativeIndex<'a> {
    snapshot: &'a MetadataSnapshot,
}

impl<'a> ConservativeIndex<'a> {
    pub fn open(snapshot: &'a MetadataSnapshot) -> Self {
        Self { snapshot }
    }

    /// Stream every unique BaseEquivalent atom pair exactly once.
    pub fn for_each_candidate(&self, mut visit: impl FnMut(u32, u32)) -> IndexMetrics {
        self.visit_blocks(0..self.snapshot.blocking().block_kinds.len(), &mut visit)
    }

    /// Execute the frozen seed-first catalog. Job shape changes traversal and
    /// locality only; canonical ownership keeps the candidate relation stable.
    pub fn for_each_catalog_candidate(
        &self,
        catalog: &WorkCatalog,
        plan: &RecallPlan,
        mut visit: impl FnMut(u32, u32),
    ) -> IndexMetrics {
        let mut metrics = IndexMetrics::default();
        for &job_id in &plan.ordered_job_ids {
            let Some(job) = catalog.jobs.get(job_id as usize) else {
                continue;
            };
            let blocks = job.first_block as usize..(job.first_block + job.block_count) as usize;
            match job.shape {
                JobShape::MicroBatch => metrics.add(self.visit_blocks(blocks, &mut visit)),
                JobShape::LeftTileFanout => {
                    for block in blocks {
                        metrics.add(self.visit_hot_tile(
                            block,
                            job.tile_row as usize,
                            job.tile_col as usize,
                            &mut visit,
                        ));
                    }
                }
            }
        }
        metrics
    }

    pub fn for_each_catalog_candidate_with_progress(
        &self,
        catalog: &WorkCatalog,
        plan: &RecallPlan,
        mut visit: impl FnMut(u32, u32),
        mut progress: impl FnMut(ProgressEvent),
    ) -> Result<IndexMetrics, SchedulerError> {
        let total = catalog.jobs.iter().try_fold(0u64, |total, job| {
            let first = job.first_block as usize;
            let end = first.saturating_add(job.block_count as usize);
            (first..end).try_fold(total, |total, block| {
                let begin = self.snapshot.blocking().block_atom_offsets[block];
                let end = self.snapshot.blocking().block_atom_offsets[block + 1];
                let members = end.saturating_sub(begin);
                total
                    .checked_add(members.saturating_mul(members.saturating_sub(1)) / 2)
                    .ok_or(SchedulerError::WorkOverflow)
            })
        })?;
        let mut completed = 0u64;
        let mut metrics = IndexMetrics::default();
        progress(
            ProgressEvent::determinate(
                ProgressPhase::CatalogPairs,
                0,
                total,
                WorkUnit::Pairs,
                ProgressCounters::default(),
            )
            .with_plan(WorkClass::CatalogRoutes, TotalKind::Exact),
        );
        for &job_id in &plan.ordered_job_ids {
            let Some(job) = catalog.jobs.get(job_id as usize) else {
                continue;
            };
            let blocks = job.first_block as usize..(job.first_block + job.block_count) as usize;
            let job_metrics = match job.shape {
                JobShape::MicroBatch => self.visit_blocks(blocks, &mut visit),
                JobShape::LeftTileFanout => {
                    let mut job_metrics = IndexMetrics::default();
                    for block in blocks {
                        job_metrics.add(self.visit_hot_tile(
                            block,
                            job.tile_row as usize,
                            job.tile_col as usize,
                            &mut visit,
                        ));
                    }
                    job_metrics
                }
            };
            completed = completed
                .checked_add(job_metrics.block_pair_visits)
                .ok_or(SchedulerError::WorkOverflow)?;
            metrics.add(job_metrics);
            progress(
                ProgressEvent::determinate(
                    ProgressPhase::CatalogPairs,
                    completed,
                    total,
                    WorkUnit::Pairs,
                    ProgressCounters {
                        candidates: metrics.routed_pairs,
                        scored: metrics.routed_pairs,
                        ..ProgressCounters::default()
                    },
                )
                .with_plan(WorkClass::CatalogRoutes, TotalKind::Exact),
            );
        }
        Ok(metrics)
    }

    /// Execute one catalog job and report exact routing-pair work at block/tile
    /// boundaries.  The reported work is independent of whether scoring later
    /// triggers a potentially large contract expansion.
    pub fn for_each_job_candidate_with_work(
        &self,
        job: &JobDescriptor,
        mut visit: impl FnMut(u32, u32),
        mut work: impl FnMut(u64),
    ) -> IndexMetrics {
        self.for_each_job_candidate_with_work_while(job, &mut visit, &mut work, || true)
    }

    /// Execute one catalog job, stopping pair traversal as soon as
    /// `keep_going` becomes false. Work reports only fully visited pairs.
    pub fn for_each_job_candidate_with_work_while(
        &self,
        job: &JobDescriptor,
        mut visit: impl FnMut(u32, u32),
        mut work: impl FnMut(u64),
        mut keep_going: impl FnMut() -> bool,
    ) -> IndexMetrics {
        let blocks = job.first_block as usize..(job.first_block + job.block_count) as usize;
        match job.shape {
            JobShape::MicroBatch => {
                let blocking = self.snapshot.blocking();
                let mut metrics = IndexMetrics::default();
                for block in blocks {
                    let start = blocking.block_atom_offsets[block] as usize;
                    let end = blocking.block_atom_offsets[block + 1] as usize;
                    let members = &blocking.block_atoms[start..end];
                    let before = metrics.block_pair_visits;
                    for i in 0..members.len() {
                        for &right in &members[i + 1..] {
                            if !keep_going() {
                                work(metrics.block_pair_visits.saturating_sub(before));
                                return metrics;
                            }
                            metrics.block_pair_visits = metrics.block_pair_visits.saturating_add(1);
                            self.route_pair(block, members[i], right, &mut metrics, &mut visit);
                        }
                    }
                    work(metrics.block_pair_visits.saturating_sub(before));
                }
                metrics
            }
            JobShape::LeftTileFanout => {
                let mut metrics = IndexMetrics::default();
                for block in blocks {
                    let b = self.snapshot.blocking();
                    let start = b.block_atom_offsets[block] as usize;
                    let end = b.block_atom_offsets[block + 1] as usize;
                    let members = &b.block_atoms[start..end];
                    let a0 = (job.tile_row as usize).saturating_mul(HOT_BLOCK_TILE);
                    let a1 = (a0 + HOT_BLOCK_TILE).min(members.len());
                    let b0 = (job.tile_col as usize).saturating_mul(HOT_BLOCK_TILE);
                    let b1 = (b0 + HOT_BLOCK_TILE).min(members.len());
                    let before = metrics.block_pair_visits;
                    for i in a0..a1 {
                        let j0 = if job.tile_row == job.tile_col {
                            (i + 1).max(b0)
                        } else {
                            b0
                        };
                        for j in j0..b1 {
                            if !keep_going() {
                                work(metrics.block_pair_visits.saturating_sub(before));
                                return metrics;
                            }
                            metrics.block_pair_visits = metrics.block_pair_visits.saturating_add(1);
                            self.route_pair(
                                block,
                                members[i],
                                members[j],
                                &mut metrics,
                                &mut visit,
                            );
                        }
                    }
                    work(metrics.block_pair_visits.saturating_sub(before));
                }
                metrics
            }
        }
    }

    fn visit_blocks(
        &self,
        blocks: impl Iterator<Item = usize>,
        visit: &mut impl FnMut(u32, u32),
    ) -> IndexMetrics {
        self.visit_blocks_with_work(blocks, visit, &mut |_| {})
    }

    fn visit_blocks_with_work(
        &self,
        blocks: impl Iterator<Item = usize>,
        visit: &mut impl FnMut(u32, u32),
        work: &mut impl FnMut(u64),
    ) -> IndexMetrics {
        let blocking = self.snapshot.blocking();
        let mut metrics = IndexMetrics::default();
        for block in blocks {
            let start = blocking.block_atom_offsets[block] as usize;
            let end = blocking.block_atom_offsets[block + 1] as usize;
            let members = &blocking.block_atoms[start..end];
            for i in 0..members.len() {
                for &right in &members[i + 1..] {
                    metrics.block_pair_visits = metrics.block_pair_visits.saturating_add(1);
                    self.route_pair(block, members[i], right, &mut metrics, visit);
                }
            }
            work((members.len() as u64).saturating_mul(members.len().saturating_sub(1) as u64) / 2);
        }
        metrics
    }

    fn visit_hot_tile(
        &self,
        block: usize,
        tile_row: usize,
        tile_col: usize,
        visit: &mut impl FnMut(u32, u32),
    ) -> IndexMetrics {
        self.visit_hot_tile_with_work(block, tile_row, tile_col, visit, &mut |_| {})
    }

    fn visit_hot_tile_with_work(
        &self,
        block: usize,
        tile_row: usize,
        tile_col: usize,
        visit: &mut impl FnMut(u32, u32),
        work: &mut impl FnMut(u64),
    ) -> IndexMetrics {
        let b = self.snapshot.blocking();
        let start = b.block_atom_offsets[block] as usize;
        let end = b.block_atom_offsets[block + 1] as usize;
        let members = &b.block_atoms[start..end];
        let mut metrics = IndexMetrics::default();
        let a0 = tile_row.saturating_mul(HOT_BLOCK_TILE);
        let a1 = (a0 + HOT_BLOCK_TILE).min(members.len());
        let b0 = tile_col.saturating_mul(HOT_BLOCK_TILE);
        let b1 = (b0 + HOT_BLOCK_TILE).min(members.len());
        let before = metrics.block_pair_visits;
        for i in a0..a1 {
            let j0 = if tile_row == tile_col {
                (i + 1).max(b0)
            } else {
                b0
            };
            for j in j0..b1 {
                metrics.block_pair_visits = metrics.block_pair_visits.saturating_add(1);
                self.route_pair(block, members[i], members[j], &mut metrics, visit);
            }
        }
        work(metrics.block_pair_visits.saturating_sub(before));
        metrics
    }

    fn route_pair(
        &self,
        block: usize,
        a: u32,
        b: u32,
        metrics: &mut IndexMetrics,
        visit: &mut impl FnMut(u32, u32),
    ) {
        let left = a.min(b);
        let right = a.max(b);
        if candidate_owner(self.snapshot.blocking(), left, right) != Some(block as u32) {
            metrics.duplicate_routes = metrics.duplicate_routes.saturating_add(1);
            return;
        }
        metrics.routed_pairs = metrics.routed_pairs.saturating_add(1);
        let offsets = &self.snapshot.features().fallback_atom_offsets;
        let left_members = offsets[left as usize + 1] - offsets[left as usize];
        let right_members = offsets[right as usize + 1] - offsets[right as usize];
        metrics.contract_pair_visits = metrics
            .contract_pair_visits
            .saturating_add(left_members.saturating_mul(right_members));
        visit(left, right)
    }
}

impl IndexMetrics {
    pub fn add(&mut self, other: Self) {
        self.block_pair_visits = self
            .block_pair_visits
            .saturating_add(other.block_pair_visits);
        self.contract_pair_visits = self
            .contract_pair_visits
            .saturating_add(other.contract_pair_visits);
        self.routed_pairs = self.routed_pairs.saturating_add(other.routed_pairs);
        self.duplicate_routes = self.duplicate_routes.saturating_add(other.duplicate_routes);
        debug_assert_eq!(other.exact_full_build_bytes, 0);
        debug_assert_eq!(other.exact_full_mmap_bytes, 0);
    }
}

/// Minimum shared block that satisfies the legacy two-dimensional routing gate.
pub fn candidate_owner(view: &BlockingView, left: u32, right: u32) -> Option<u32> {
    let l = block_ids(view, left);
    let r = block_ids(view, right);
    let (mut i, mut j) = (0, 0);
    while i < l.len() && j < r.len() {
        match l[i].cmp(&r[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                let block = l[i] as usize;
                if block_gate(view, block, left, right) {
                    return Some(l[i]);
                }
                i += 1;
                j += 1;
            }
        }
    }
    None
}

fn block_ids(view: &BlockingView, atom: u32) -> &[u32] {
    let i = atom as usize;
    &view.atom_block_ids
        [view.atom_block_offsets[i] as usize..view.atom_block_offsets[i + 1] as usize]
}

fn block_gate(view: &BlockingView, block: usize, left: u32, right: u32) -> bool {
    match view.block_kinds[block] {
        0 => true,
        1 => dimension_recalls(view, left, right, false),
        2 => dimension_recalls(view, left, right, true),
        _ => false,
    }
}

fn dimension_recalls(view: &BlockingView, left: u32, right: u32, template: bool) -> bool {
    let anchor_kind = if template { 1 } else { 2 };
    if share_block_kind(view, left, right, anchor_kind) {
        return true;
    }
    let hashes = if template {
        &*view.template_simhashes
    } else {
        &*view.content_simhashes
    };
    let a = hashes[left as usize];
    let b = hashes[right as usize];
    (0..8).any(|band| ((a >> (band * 8)) & 0xff) == ((b >> (band * 8)) & 0xff))
}

fn share_block_kind(view: &BlockingView, left: u32, right: u32, kind: u32) -> bool {
    let l = block_ids(view, left);
    let r = block_ids(view, right);
    let (mut i, mut j) = (0, 0);
    while i < l.len() && j < r.len() {
        match l[i].cmp(&r[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                if view.block_kinds[l[i] as usize] == kind {
                    return true;
                }
                i += 1;
                j += 1;
            }
        }
    }
    false
}
