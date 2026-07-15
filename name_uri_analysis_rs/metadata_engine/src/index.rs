//! BaseEquivalent per-block Conservative candidate relation.
//!
//! The compiled blocks are the postings. This view never constructs or mmaps a
//! full Exact index and streams canonical-owner pairs to its consumer.

use crate::encode::FeatureView;
use crate::progress::{
    ProgressCounters, ProgressEvent, ProgressPhase, TotalKind, WorkClass, WorkUnit,
};
use crate::scheduler::{
    job_routing_pair_work, JobDescriptor, JobShape, RecallPlan, SchedulerError, WorkCatalog,
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

#[derive(Debug, Clone, Copy)]
enum HotIndexDimension {
    Template,
    Content,
}

impl<'a> ConservativeIndex<'a> {
    pub fn open(snapshot: &'a MetadataSnapshot) -> Self {
        Self { snapshot }
    }

    /// Stream every unique BaseEquivalent atom pair exactly once.
    pub fn for_each_candidate(&self, mut visit: impl FnMut(u32, u32)) -> IndexMetrics {
        self.visit_blocks(0..self.snapshot.blocking().block_kinds.len(), &mut visit)
    }

    /// Execute the frozen seed-first catalog. Cold jobs enumerate their block
    /// pairs directly; lazy hot jobs first apply proof-safe term-overlap
    /// rejection. Canonical ownership still emits every scorer-eligible pair
    /// exactly once.
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
                        metrics.add(self.visit_hot_block_with_work_while(
                            block,
                            &mut visit,
                            &mut |_| {},
                            &mut || true,
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
            total
                .checked_add(job_routing_pair_work(self.snapshot, job)?)
                .ok_or(SchedulerError::WorkOverflow)
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
                        job_metrics.add(self.visit_hot_block_with_work_while(
                            block,
                            &mut visit,
                            &mut |_| {},
                            &mut || true,
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
                    metrics.add(self.visit_hot_block_with_work_while(
                        block,
                        &mut visit,
                        &mut work,
                        &mut keep_going,
                    ));
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

    /// Execute one hot block through a proof-safe secondary index.
    ///
    /// An exact match requires at least one shared template term and one shared
    /// content term. We sample posting expansion to choose an index dimension,
    /// union its postings per left atom, then confirm exact overlap in the other
    /// dimension before invoking the scorer. Pairs rejected here therefore
    /// cannot be accepted by `score_pair`.
    fn visit_hot_block_with_work_while(
        &self,
        block: usize,
        visit: &mut impl FnMut(u32, u32),
        work: &mut impl FnMut(u64),
        keep_going: &mut impl FnMut() -> bool,
    ) -> IndexMetrics {
        let b = self.snapshot.blocking();
        let start = b.block_atom_offsets[block] as usize;
        let end = b.block_atom_offsets[block + 1] as usize;
        let members = &b.block_atoms[start..end];
        let features = self.snapshot.features();
        let payloads = members
            .iter()
            .map(|&atom| atom_payload(features, atom))
            .collect::<Vec<_>>();
        let template_memberships = payloads.iter().fold(0u64, |total, &payload| {
            total.saturating_add(
                payload_terms(features, payload, HotIndexDimension::Template).len() as u64,
            )
        });
        let content_memberships = payloads.iter().fold(0u64, |total, &payload| {
            total.saturating_add(
                payload_terms(features, payload, HotIndexDimension::Content).len() as u64,
            )
        });
        let indexed_dimension = choose_hot_index_dimension(
            features,
            &payloads,
            template_memberships,
            content_memberships,
        );
        let verification_dimension = match indexed_dimension {
            HotIndexDimension::Template => HotIndexDimension::Content,
            HotIndexDimension::Content => HotIndexDimension::Template,
        };
        let membership_capacity = match indexed_dimension {
            HotIndexDimension::Template => template_memberships,
            HotIndexDimension::Content => content_memberships,
        } as usize;
        let mut postings = Vec::<(u32, u32)>::with_capacity(membership_capacity);
        for (position, &payload) in payloads.iter().enumerate() {
            postings.extend(
                payload_terms(features, payload, indexed_dimension)
                    .iter()
                    .copied()
                    .map(|term| (term, position as u32)),
            );
        }
        postings.sort_unstable();

        let mut metrics = IndexMetrics::default();
        let mut marks = vec![0u32; members.len()];
        let mut epoch = 0u32;
        let mut candidates = Vec::<u32>::with_capacity(members.len());
        let mut pending_work = 0u64;
        const PROGRESS_CHUNK: u64 = 100_000_000;

        for (left_position, &left_payload) in payloads.iter().enumerate() {
            if !keep_going() {
                break;
            }
            epoch = epoch.wrapping_add(1);
            if epoch == 0 {
                marks.fill(0);
                epoch = 1;
            }
            candidates.clear();
            for &term in payload_terms(features, left_payload, indexed_dimension) {
                let posting_start = postings.partition_point(|&(candidate, _)| candidate < term);
                let posting_end = postings.partition_point(|&(candidate, _)| candidate <= term);
                for &(_, right_position) in &postings[posting_start..posting_end] {
                    let right_position = right_position as usize;
                    if right_position <= left_position || marks[right_position] == epoch {
                        continue;
                    }
                    marks[right_position] = epoch;
                    candidates.push(right_position as u32);
                }
            }
            let left_verification_terms =
                payload_terms(features, left_payload, verification_dimension);
            for &right_position in &candidates {
                if !keep_going() {
                    break;
                }
                let right_position = right_position as usize;
                let right_verification_terms =
                    payload_terms(features, payloads[right_position], verification_dimension);
                if sorted_terms_intersect(left_verification_terms, right_verification_terms) {
                    self.route_pair(
                        block,
                        members[left_position],
                        members[right_position],
                        &mut metrics,
                        visit,
                    );
                }
            }

            // Count the complete logical row covered by the proof, including
            // pairs rejected without materializing them. This keeps the stable
            // nC2 progress contract while actual scorer calls follow the much
            // smaller proof-safe candidate relation.
            let row_work = members.len().saturating_sub(left_position + 1) as u64;
            metrics.block_pair_visits = metrics.block_pair_visits.saturating_add(row_work);
            pending_work = pending_work.saturating_add(row_work);
            if pending_work >= PROGRESS_CHUNK {
                work(pending_work);
                pending_work = 0;
            }
        }
        if pending_work > 0 {
            work(pending_work);
        }
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

fn atom_payload(features: &FeatureView, atom: u32) -> u32 {
    let contract =
        features.fallback_atom_contracts[features.fallback_atom_offsets[atom as usize] as usize];
    features.contract_payload[contract as usize]
}

fn payload_terms(features: &FeatureView, payload: u32, dimension: HotIndexDimension) -> &[u32] {
    let payload = payload as usize;
    match dimension {
        HotIndexDimension::Template => {
            let start = features.payload_template_offsets[payload] as usize;
            let end = features.payload_template_offsets[payload + 1] as usize;
            &features.payload_template_terms[start..end]
        }
        HotIndexDimension::Content => {
            let start = features.payload_content_offsets[payload] as usize;
            let end = features.payload_content_offsets[payload + 1] as usize;
            &features.payload_content_terms[start..end]
        }
    }
}

fn sorted_terms_intersect(left: &[u32], right: &[u32]) -> bool {
    let (mut left_index, mut right_index) = (0usize, 0usize);
    while left_index < left.len() && right_index < right.len() {
        match left[left_index].cmp(&right[right_index]) {
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
            std::cmp::Ordering::Equal => return true,
        }
    }
    false
}

fn choose_hot_index_dimension(
    features: &FeatureView,
    payloads: &[u32],
    template_memberships: u64,
    content_memberships: u64,
) -> HotIndexDimension {
    let template_pair_work =
        sampled_posting_pair_work(features, payloads, HotIndexDimension::Template);
    let content_pair_work =
        sampled_posting_pair_work(features, payloads, HotIndexDimension::Content);
    match template_pair_work.cmp(&content_pair_work) {
        std::cmp::Ordering::Less => HotIndexDimension::Template,
        std::cmp::Ordering::Greater => HotIndexDimension::Content,
        std::cmp::Ordering::Equal if template_memberships <= content_memberships => {
            HotIndexDimension::Template
        }
        std::cmp::Ordering::Equal => HotIndexDimension::Content,
    }
}

/// Deterministically estimate posting expansion on an evenly spaced sample.
/// Membership count alone is a poor selector when a short template dimension
/// contains one extremely common term while richer content terms are sparse.
fn sampled_posting_pair_work(
    features: &FeatureView,
    payloads: &[u32],
    dimension: HotIndexDimension,
) -> u64 {
    const SAMPLE_PAYLOADS: usize = 16_384;
    let stride = payloads.len().div_ceil(SAMPLE_PAYLOADS).max(1);
    let mut terms = Vec::<u32>::new();
    for &payload in payloads.iter().step_by(stride) {
        terms.extend_from_slice(payload_terms(features, payload, dimension));
    }
    terms.sort_unstable();
    let mut pair_work = 0u64;
    let mut start = 0usize;
    while start < terms.len() {
        let term = terms[start];
        let end = start + terms[start..].partition_point(|&candidate| candidate == term);
        let count = (end - start) as u64;
        pair_work = pair_work.saturating_add(count.saturating_mul(count.saturating_sub(1)) / 2);
        start = end;
    }
    pair_work
}

/// Conservative peak scratch for one lazy hot-block candidate index.
pub fn max_hot_block_candidate_index_bytes(
    snapshot: &MetadataSnapshot,
    catalog: &WorkCatalog,
) -> Result<u64, SchedulerError> {
    catalog
        .jobs
        .iter()
        .filter(|job| job.shape == JobShape::LeftTileFanout)
        .try_fold(0u64, |maximum, job| {
            let first = job.first_block as usize;
            let end = first.saturating_add(job.block_count as usize);
            (first..end).try_fold(maximum, |maximum, block| {
                let blocking = snapshot.blocking();
                let begin = blocking.block_atom_offsets[block] as usize;
                let end = blocking.block_atom_offsets[block + 1] as usize;
                let members = &blocking.block_atoms[begin..end];
                let features = snapshot.features();
                let (template, content) =
                    members
                        .iter()
                        .try_fold((0u64, 0u64), |(template, content), &atom| {
                            let payload = atom_payload(features, atom);
                            let template_len =
                                payload_terms(features, payload, HotIndexDimension::Template).len()
                                    as u64;
                            let content_len =
                                payload_terms(features, payload, HotIndexDimension::Content).len()
                                    as u64;
                            Ok::<_, SchedulerError>((
                                template
                                    .checked_add(template_len)
                                    .ok_or(SchedulerError::WorkOverflow)?,
                                content
                                    .checked_add(content_len)
                                    .ok_or(SchedulerError::WorkOverflow)?,
                            ))
                        })?;
                // Execution chooses the dimension with lower sampled posting
                // expansion, which is not necessarily the dimension with fewer
                // memberships. Admit either outcome conservatively.
                let posting_bytes = template
                    .max(content)
                    .checked_mul(std::mem::size_of::<(u32, u32)>() as u64)
                    .ok_or(SchedulerError::WorkOverflow)?;
                let row_scratch = (members.len() as u64)
                    .checked_mul((std::mem::size_of::<u32>() * 3) as u64)
                    .ok_or(SchedulerError::WorkOverflow)?;
                let bytes = posting_bytes
                    .checked_add(row_scratch)
                    .ok_or(SchedulerError::WorkOverflow)?;
                Ok(maximum.max(bytes))
            })
        })
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
