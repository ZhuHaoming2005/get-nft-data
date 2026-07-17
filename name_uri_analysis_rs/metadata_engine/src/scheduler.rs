//! Fixed-width Work Catalog, frozen RecallPlan and coverage certificates.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::snapshot::MetadataSnapshot;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[repr(u8)]
pub enum JobShape {
    MicroBatch = 0,
    LeftTileFanout = 1,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[repr(C)]
pub struct JobDescriptor {
    pub job_id: u32,
    pub first_block: u32,
    pub block_count: u32,
    pub shape: JobShape,
    pub risk: u8,
    pub rescue: u8,
    /// Reserved for durable catalog compatibility. Hot blocks are represented
    /// by one lazy fanout descriptor rather than one descriptor per tile.
    pub tile_row: u16,
    /// Reserved for durable catalog compatibility.
    pub tile_col: u16,
    pub estimated_work: u64,
}

pub const CATALOG_REVISION: u32 = 4;
const MAX_MICRO_BLOCK_PAIR_WORK: u64 = 8_000_000;
const CATALOG_ENCODED_BYTES_PER_JOB: u64 = 24;

#[derive(Serialize)]
struct CatalogReadyRef<'a> {
    catalog_revision: u32,
    job_count: usize,
    snapshot_fingerprint: &'a str,
}

#[derive(Deserialize)]
struct CatalogReady {
    catalog_revision: u32,
    job_count: usize,
    snapshot_fingerprint: String,
}

fn block_requires_hot_index(members: u64, hot_members: u64) -> Result<bool, SchedulerError> {
    let pair_work = members
        .checked_mul(members.saturating_sub(1))
        .and_then(|work| work.checked_div(2))
        .ok_or(SchedulerError::WorkOverflow)?;
    Ok(members > hot_members || pair_work > MAX_MICRO_BLOCK_PAIR_WORK)
}

#[cfg(test)]
mod scheduler_shape_tests {
    use super::*;

    #[test]
    fn pair_work_guard_removes_the_just_below_member_threshold_cliff() {
        assert!(block_requires_hot_index(999_999, 1_000_000).unwrap());
        assert!(!block_requires_hot_index(4_000, 1_000_000).unwrap());
        assert!(block_requires_hot_index(4_001, 1_000_000).unwrap());
    }
}

#[derive(Debug, Clone, Copy)]
pub struct UniverseBudget {
    pub max_jobs: u64,
    pub max_catalog_bytes: u64,
    pub cold_members_per_job: u64,
}

#[derive(Debug, Error)]
pub enum SchedulerError {
    #[error(transparent)]
    Identity(#[from] crate::identity::IdentityOverflow),
    #[error("catalog budget exceeded: jobs={jobs}/{max_jobs}, bytes={bytes}/{max_bytes}")]
    Budget {
        jobs: u64,
        max_jobs: u64,
        bytes: u64,
        max_bytes: u64,
    },
    #[error("catalog estimated work overflow")]
    WorkOverflow,
    #[error("stale coverage certificate")]
    StaleCoverage,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Format(#[from] crate::format::FormatError),
}

struct CatalogWorkspace {
    root: PathBuf,
    cleanup: bool,
}

impl Drop for CatalogWorkspace {
    fn drop(&mut self) {
        if self.cleanup {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }
}

struct MappedCatalogJobs {
    first: crate::format::MappedU32Array,
    counts: crate::format::MappedU32Array,
    flags: crate::format::MappedU32Array,
    work: crate::format::MappedU64Array,
    tiles: crate::format::MappedU32Array,
    _workspace: Arc<CatalogWorkspace>,
}

enum CatalogJobsInner {
    Resident(Arc<[JobDescriptor]>),
    Mapped(Box<MappedCatalogJobs>),
}

#[derive(Clone)]
pub struct CatalogJobs {
    inner: Arc<CatalogJobsInner>,
}

impl CatalogJobs {
    fn resident(jobs: Vec<JobDescriptor>) -> Self {
        Self {
            inner: Arc::new(CatalogJobsInner::Resident(jobs.into())),
        }
    }

    fn mapped(dir: &Path, job_count: usize, cleanup: bool) -> Result<Self, SchedulerError> {
        let first = crate::format::map_u32_array(&dir.join("job_first_block.u32"))?;
        let counts = crate::format::map_u32_array(&dir.join("job_block_count.u32"))?;
        let flags = crate::format::map_u32_array(&dir.join("job_flags.u32"))?;
        let work = crate::format::map_u64_array(&dir.join("job_estimated_work.u64"))?;
        let tiles = crate::format::map_u32_array(&dir.join("job_tiles.u32"))?;
        if [
            first.len(),
            counts.len(),
            flags.len(),
            work.len(),
            tiles.len(),
        ]
        .into_iter()
        .any(|len| len != job_count)
            || flags.iter().any(|flags| !matches!(flags & 0xff, 0 | 1))
        {
            return Err(SchedulerError::StaleCoverage);
        }
        Ok(Self {
            inner: Arc::new(CatalogJobsInner::Mapped(Box::new(MappedCatalogJobs {
                first,
                counts,
                flags,
                work,
                tiles,
                _workspace: Arc::new(CatalogWorkspace {
                    root: dir.to_path_buf(),
                    cleanup,
                }),
            }))),
        })
    }

    pub fn len(&self) -> usize {
        match self.inner.as_ref() {
            CatalogJobsInner::Resident(jobs) => jobs.len(),
            CatalogJobsInner::Mapped(jobs) => jobs.first.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, index: usize) -> Option<JobDescriptor> {
        if index >= self.len() {
            return None;
        }
        Some(match self.inner.as_ref() {
            CatalogJobsInner::Resident(jobs) => jobs[index],
            CatalogJobsInner::Mapped(jobs) => {
                let flags = jobs.flags[index];
                let shape = match flags & 0xff {
                    0 => JobShape::MicroBatch,
                    1 => JobShape::LeftTileFanout,
                    _ => unreachable!("mapped catalog flags were validated while opening"),
                };
                JobDescriptor {
                    job_id: index as u32,
                    first_block: jobs.first[index],
                    block_count: jobs.counts[index],
                    shape,
                    risk: ((flags >> 8) & 0xff) as u8,
                    rescue: ((flags >> 16) & 0xff) as u8,
                    tile_row: (jobs.tiles[index] >> 16) as u16,
                    tile_col: (jobs.tiles[index] & 0xffff) as u16,
                    estimated_work: jobs.work[index],
                }
            }
        })
    }

    pub fn iter(&self) -> CatalogJobIter<'_> {
        CatalogJobIter {
            jobs: self,
            index: 0,
        }
    }

    pub fn is_mapped(&self) -> bool {
        matches!(self.inner.as_ref(), CatalogJobsInner::Mapped(_))
    }
}

pub struct CatalogJobIter<'a> {
    jobs: &'a CatalogJobs,
    index: usize,
}

impl Iterator for CatalogJobIter<'_> {
    type Item = JobDescriptor;

    fn next(&mut self) -> Option<Self::Item> {
        let job = self.jobs.get(self.index)?;
        self.index += 1;
        Some(job)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.jobs.len().saturating_sub(self.index);
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for CatalogJobIter<'_> {}

impl std::fmt::Debug for CatalogJobs {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CatalogJobs")
            .field("len", &self.len())
            .field("mapped", &self.is_mapped())
            .finish()
    }
}

impl PartialEq for CatalogJobs {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len() && self.iter().eq(other.iter())
    }
}

impl Eq for CatalogJobs {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkCatalog {
    pub jobs: CatalogJobs,
    pub snapshot_fingerprint: String,
}

impl WorkCatalog {
    pub fn build(
        snapshot: &MetadataSnapshot,
        budget: UniverseBudget,
        hot_members: u64,
    ) -> Result<Self, SchedulerError> {
        Self::build_with_progress(snapshot, budget, hot_members, |_, _, _| {})
    }

    pub fn build_with_progress(
        snapshot: &MetadataSnapshot,
        budget: UniverseBudget,
        hot_members: u64,
        mut progress: impl FnMut(u64, u64, u64),
    ) -> Result<Self, SchedulerError> {
        let mut jobs = Vec::new();
        visit_job_descriptors(
            snapshot,
            budget,
            hot_members,
            |job| {
                jobs.push(job);
                Ok(())
            },
            &mut progress,
        )?;
        Ok(Self {
            jobs: CatalogJobs::resident(jobs),
            snapshot_fingerprint: snapshot_fingerprint(snapshot),
        })
    }

    pub fn descriptor_count(
        snapshot: &MetadataSnapshot,
        budget: UniverseBudget,
        hot_members: u64,
    ) -> Result<u64, SchedulerError> {
        visit_job_descriptors(snapshot, budget, hot_members, |_| Ok(()), &mut |_, _, _| {})
    }

    pub fn encoded_bytes(&self) -> Result<u64, SchedulerError> {
        (self.jobs.len() as u64)
            .checked_mul(CATALOG_ENCODED_BYTES_PER_JOB)
            .ok_or(SchedulerError::WorkOverflow)
    }

    /// Reuse a catalog only when it is bound to the current snapshot.  A
    /// coverage mismatch is a cache miss, not a terminal resume failure.
    /// Corruption still fails closed.
    pub fn open_or_rebuild_with_progress(
        dir: &Path,
        snapshot: &MetadataSnapshot,
        budget: UniverseBudget,
        hot_members: u64,
        progress: impl FnMut(u64, u64, u64),
    ) -> Result<Self, SchedulerError> {
        if dir.join("catalog.ready").is_file() {
            match Self::open(dir, snapshot) {
                Ok(catalog) => return Ok(catalog),
                Err(SchedulerError::StaleCoverage) => {
                    std::fs::remove_dir_all(dir)?;
                }
                Err(error) => return Err(error),
            }
        }
        Self::build_external_with_progress(dir, snapshot, budget, hot_members, false, progress)
    }

    pub fn build_external_with_progress(
        dir: &Path,
        snapshot: &MetadataSnapshot,
        budget: UniverseBudget,
        hot_members: u64,
        cleanup_on_drop: bool,
        mut progress: impl FnMut(u64, u64, u64),
    ) -> Result<Self, SchedulerError> {
        let job_count =
            visit_job_descriptors(snapshot, budget, hot_members, |_| Ok(()), &mut progress)?;
        crate::identity::checked_u32_identity("catalog jobs", job_count)?;
        std::fs::create_dir_all(dir)?;
        let mut first = crate::format::TypedArraySink::create(
            &dir.join("job_first_block.u32"),
            crate::format::ArrayKind::U32,
            job_count,
        )?;
        let mut counts = crate::format::TypedArraySink::create(
            &dir.join("job_block_count.u32"),
            crate::format::ArrayKind::U32,
            job_count,
        )?;
        let mut flags = crate::format::TypedArraySink::create(
            &dir.join("job_flags.u32"),
            crate::format::ArrayKind::U32,
            job_count,
        )?;
        let mut work = crate::format::TypedArraySink::create(
            &dir.join("job_estimated_work.u64"),
            crate::format::ArrayKind::U64,
            job_count,
        )?;
        let mut tiles = crate::format::TypedArraySink::create(
            &dir.join("job_tiles.u32"),
            crate::format::ArrayKind::U32,
            job_count,
        )?;
        visit_job_descriptors(
            snapshot,
            budget,
            hot_members,
            |job| {
                first.push_u32(job.first_block)?;
                counts.push_u32(job.block_count)?;
                flags.push_u32(
                    (job.shape as u32) | (u32::from(job.risk) << 8) | (u32::from(job.rescue) << 16),
                )?;
                work.push_u64(job.estimated_work)?;
                tiles.push_u32((u32::from(job.tile_row) << 16) | u32::from(job.tile_col))?;
                Ok(())
            },
            &mut |_, _, _| {},
        )?;
        first.finish()?;
        counts.finish()?;
        flags.finish()?;
        work.finish()?;
        tiles.finish()?;
        let fingerprint = snapshot_fingerprint(snapshot);
        let ready = CatalogReadyRef {
            catalog_revision: CATALOG_REVISION,
            job_count: usize::try_from(job_count).map_err(|_| SchedulerError::WorkOverflow)?,
            snapshot_fingerprint: &fingerprint,
        };
        crate::format::commit_ready_serialized(dir, "catalog.ready", &ready)?;
        Self::open_with_cleanup(dir, snapshot, cleanup_on_drop)
    }

    pub fn commit(&self, dir: &Path) -> Result<(), SchedulerError> {
        crate::identity::checked_u32_identity("catalog jobs", self.jobs.len() as u64)?;
        std::fs::create_dir_all(dir)?;
        let job_count = self.jobs.len() as u64;
        crate::format::write_u32_iter(
            &dir.join("job_first_block.u32"),
            crate::format::ArrayKind::U32,
            job_count,
            self.jobs.iter().map(|job| job.first_block),
        )?;
        crate::format::write_u32_iter(
            &dir.join("job_block_count.u32"),
            crate::format::ArrayKind::U32,
            job_count,
            self.jobs.iter().map(|job| job.block_count),
        )?;
        crate::format::write_u32_iter(
            &dir.join("job_flags.u32"),
            crate::format::ArrayKind::U32,
            job_count,
            self.jobs.iter().map(|job| {
                (job.shape as u32) | (u32::from(job.risk) << 8) | (u32::from(job.rescue) << 16)
            }),
        )?;
        crate::format::write_u64_iter(
            &dir.join("job_estimated_work.u64"),
            crate::format::ArrayKind::U64,
            job_count,
            self.jobs.iter().map(|job| job.estimated_work),
        )?;
        crate::format::write_u32_iter(
            &dir.join("job_tiles.u32"),
            crate::format::ArrayKind::U32,
            job_count,
            self.jobs
                .iter()
                .map(|job| (u32::from(job.tile_row) << 16) | u32::from(job.tile_col)),
        )?;
        let ready = CatalogReadyRef {
            catalog_revision: CATALOG_REVISION,
            job_count: self.jobs.len(),
            snapshot_fingerprint: &self.snapshot_fingerprint,
        };
        crate::format::commit_ready_serialized(dir, "catalog.ready", &ready)?;
        Ok(())
    }

    pub fn open(dir: &Path, snapshot: &MetadataSnapshot) -> Result<Self, SchedulerError> {
        Self::open_with_cleanup(dir, snapshot, false)
    }

    fn open_with_cleanup(
        dir: &Path,
        snapshot: &MetadataSnapshot,
        cleanup_on_drop: bool,
    ) -> Result<Self, SchedulerError> {
        let ready: CatalogReady =
            serde_json::from_slice(&std::fs::read(dir.join("catalog.ready"))?)?;
        if ready.catalog_revision != CATALOG_REVISION
            || ready.snapshot_fingerprint != snapshot_fingerprint(snapshot)
        {
            return Err(SchedulerError::StaleCoverage);
        }
        crate::identity::checked_u32_identity("catalog jobs", ready.job_count as u64)?;
        Ok(Self {
            jobs: CatalogJobs::mapped(dir, ready.job_count, cleanup_on_drop)?,
            snapshot_fingerprint: ready.snapshot_fingerprint,
        })
    }

    pub fn estimated_work(&self) -> Result<u64, SchedulerError> {
        self.jobs.iter().try_fold(0u64, |total, job| {
            total
                .checked_add(job.estimated_work)
                .ok_or(SchedulerError::WorkOverflow)
        })
    }
}

pub fn estimate_catalog_contract_pair_work(
    snapshot: &MetadataSnapshot,
) -> Result<u64, SchedulerError> {
    (0..snapshot.blocking().block_kinds.len()).try_fold(0u64, |total, block| {
        total
            .checked_add(block_contract_pair_work(snapshot, block)?)
            .ok_or(SchedulerError::WorkOverflow)
    })
}

/// Exact logical routing universe covered by one catalog descriptor.
///
/// A lazy hot-block descriptor covers its block once. Keeping this calculation
/// job-aware prevents the former per-tile descriptors from multiplying the
/// whole block's `nC2` total for every tile.
pub fn job_routing_pair_work(
    snapshot: &MetadataSnapshot,
    job: &JobDescriptor,
) -> Result<u64, SchedulerError> {
    let first = job.first_block as usize;
    let end = first.saturating_add(job.block_count as usize);
    (first..end).try_fold(0u64, |total, block| {
        let begin = snapshot.blocking().block_atom_offsets[block];
        let end = snapshot.blocking().block_atom_offsets[block + 1];
        let members = end.saturating_sub(begin);
        total
            .checked_add(members.saturating_mul(members.saturating_sub(1)) / 2)
            .ok_or(SchedulerError::WorkOverflow)
    })
}

fn visit_job_descriptors(
    snapshot: &MetadataSnapshot,
    budget: UniverseBudget,
    hot_members: u64,
    mut visit: impl FnMut(JobDescriptor) -> Result<(), SchedulerError>,
    progress: &mut impl FnMut(u64, u64, u64),
) -> Result<u64, SchedulerError> {
    let blocking = snapshot.blocking();
    crate::identity::checked_u32_identity("catalog blocks", blocking.block_kinds.len() as u64)?;
    let total_blocks = blocking.block_kinds.len() as u64;
    let mut job_count = 0u64;
    let mut start = 0usize;
    progress(0, total_blocks, 0);
    while start < blocking.block_kinds.len() {
        let members = blocking.block_atom_offsets[start + 1] - blocking.block_atom_offsets[start];
        if block_requires_hot_index(members, hot_members)? {
            check_next_job(job_count, budget)?;
            visit(descriptor(
                job_count,
                start,
                1,
                JobShape::LeftTileFanout,
                0,
                0,
                block_contract_pair_work(snapshot, start)?,
            ))?;
            job_count += 1;
            start += 1;
            progress(start as u64, total_blocks, job_count);
            continue;
        }
        let first = start;
        let mut total = 0u64;
        let mut estimated_work = 0u64;
        while start < blocking.block_kinds.len() {
            let members =
                blocking.block_atom_offsets[start + 1] - blocking.block_atom_offsets[start];
            if block_requires_hot_index(members, hot_members)?
                || (start > first && total.saturating_add(members) > budget.cold_members_per_job)
            {
                break;
            }
            total = total.saturating_add(members);
            estimated_work = estimated_work
                .checked_add(block_contract_pair_work(snapshot, start)?)
                .ok_or(SchedulerError::WorkOverflow)?;
            start += 1;
        }
        check_next_job(job_count, budget)?;
        visit(descriptor(
            job_count,
            first,
            start - first,
            JobShape::MicroBatch,
            0,
            0,
            estimated_work,
        ))?;
        job_count += 1;
        progress(start as u64, total_blocks, job_count);
    }
    Ok(job_count)
}

fn check_next_job(current_jobs: u64, budget: UniverseBudget) -> Result<(), SchedulerError> {
    let jobs = current_jobs.saturating_add(1);
    let bytes = jobs.saturating_mul(std::mem::size_of::<JobDescriptor>() as u64);
    if jobs > budget.max_jobs || bytes > budget.max_catalog_bytes || jobs > u32::MAX as u64 {
        Err(SchedulerError::Budget {
            jobs,
            max_jobs: budget.max_jobs.min(u32::MAX as u64),
            bytes,
            max_bytes: budget.max_catalog_bytes,
        })
    } else {
        Ok(())
    }
}

fn descriptor(
    id: u64,
    first: usize,
    count: usize,
    shape: JobShape,
    tile_row: u16,
    tile_col: u16,
    estimated_work: u64,
) -> JobDescriptor {
    debug_assert!(u32::try_from(id).is_ok());
    debug_assert!(u32::try_from(first).is_ok());
    debug_assert!(u32::try_from(count).is_ok());
    JobDescriptor {
        job_id: id as u32,
        first_block: first as u32,
        block_count: count as u32,
        shape,
        risk: 0,
        rescue: 0,
        tile_row,
        tile_col,
        estimated_work,
    }
}

fn block_contract_pair_work(
    snapshot: &MetadataSnapshot,
    block: usize,
) -> Result<u64, SchedulerError> {
    let blocking = snapshot.blocking();
    let features = snapshot.features();
    let begin = blocking.block_atom_offsets[block] as usize;
    let end = blocking.block_atom_offsets[block + 1] as usize;
    let mut prefix_contracts = 0u64;
    let mut work = 0u64;
    for &atom in &blocking.block_atoms[begin..end] {
        let members = features.fallback_atom_offsets[atom as usize + 1]
            - features.fallback_atom_offsets[atom as usize];
        work = work
            .checked_add(
                prefix_contracts
                    .checked_mul(members)
                    .ok_or(SchedulerError::WorkOverflow)?,
            )
            .ok_or(SchedulerError::WorkOverflow)?;
        prefix_contracts = prefix_contracts
            .checked_add(members)
            .ok_or(SchedulerError::WorkOverflow)?;
    }
    Ok(work)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecallPlan {
    pub schema_revision: u32,
    pub snapshot_fingerprint: String,
    pub sampled_lefts: Vec<u32>,
    pub rescue_jobs: Vec<u32>,
    pub exact_rescue_lefts: Vec<u32>,
    pub ordered_job_ids: Vec<u32>,
    pub frozen: bool,
}

impl RecallPlan {
    pub fn freeze(catalog: &WorkCatalog, sampled_lefts: Vec<u32>, rescue_jobs: Vec<u32>) -> Self {
        Self::freeze_with_rescue_lefts(catalog, sampled_lefts, rescue_jobs, Vec::new())
    }

    pub fn freeze_with_rescue_lefts(
        catalog: &WorkCatalog,
        mut sampled_lefts: Vec<u32>,
        mut rescue_jobs: Vec<u32>,
        mut exact_rescue_lefts: Vec<u32>,
    ) -> Self {
        sampled_lefts.sort_unstable();
        sampled_lefts.dedup();
        rescue_jobs.sort_unstable();
        rescue_jobs.dedup();
        exact_rescue_lefts.sort_unstable();
        exact_rescue_lefts.dedup();
        let mut jobs = catalog.jobs.iter().collect::<Vec<_>>();
        for job in &mut jobs {
            job.rescue = u8::from(rescue_jobs.binary_search(&job.job_id).is_ok());
            job.risk = job.rescue;
        }
        jobs.sort_unstable_by(|a, b| {
            b.risk
                .cmp(&a.risk)
                .then_with(|| b.estimated_work.cmp(&a.estimated_work))
                .then_with(|| a.job_id.cmp(&b.job_id))
        });
        Self {
            schema_revision: 1,
            snapshot_fingerprint: catalog.snapshot_fingerprint.clone(),
            sampled_lefts,
            rescue_jobs,
            exact_rescue_lefts,
            ordered_job_ids: jobs.into_iter().map(|j| j.job_id).collect(),
            frozen: true,
        }
    }
    pub fn commit(&self, dir: &Path) -> Result<(), SchedulerError> {
        std::fs::create_dir_all(dir)?;
        let json = serde_json::to_string_pretty(self)?;
        crate::format::commit_ready(dir, "recall-plan.ready", &json)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoverageCertificate {
    pub snapshot_fingerprint: String,
    pub job_id: u32,
    pub frontier_hash: String,
}
impl CoverageCertificate {
    pub fn issue(catalog: &WorkCatalog, job_id: u32, frontier: &[u32]) -> Self {
        Self {
            snapshot_fingerprint: catalog.snapshot_fingerprint.clone(),
            job_id,
            frontier_hash: hash_u32(frontier),
        }
    }
    pub fn validate(&self, catalog: &WorkCatalog, frontier: &[u32]) -> Result<(), SchedulerError> {
        if self.snapshot_fingerprint == catalog.snapshot_fingerprint
            && self.frontier_hash == hash_u32(frontier)
        {
            Ok(())
        } else {
            Err(SchedulerError::StaleCoverage)
        }
    }
}

pub(crate) fn snapshot_fingerprint(s: &MetadataSnapshot) -> String {
    s.cached_fingerprint(|| compute_snapshot_fingerprint(s))
        .to_owned()
}

fn compute_snapshot_fingerprint(s: &MetadataSnapshot) -> String {
    let b = s.blocking();
    let f = s.features();
    let mut h = Sha256::new();
    h.update(b"metadata-snapshot-fingerprint-v2");
    h.update((s.atom_count() as u64).to_le_bytes());
    macro_rules! verified_array {
        ($label:literal, $array:expr) => {{
            h.update($label.as_bytes());
            h.update(($array.len() as u64).to_le_bytes());
            h.update($array.verified_checksum());
        }};
    }
    verified_array!("source_to_payload", f.source_to_payload);
    verified_array!("payload_template_offsets", f.payload_template_offsets);
    verified_array!("payload_template_terms", f.payload_template_terms);
    verified_array!("payload_template_freqs", f.payload_template_freqs);
    verified_array!("payload_content_offsets", f.payload_content_offsets);
    verified_array!("payload_content_terms", f.payload_content_terms);
    verified_array!("payload_content_freqs", f.payload_content_freqs);
    verified_array!("payload_template_sigs", f.payload_template_sigs);
    verified_array!("payload_content_sigs", f.payload_content_sigs);
    verified_array!("contract_token_offsets", f.contract_token_offsets);
    verified_array!("contract_tokens", f.contract_tokens);
    verified_array!("token_member_offsets", f.token_member_offsets);
    verified_array!("token_member_contracts", f.token_member_contracts);
    verified_array!("token_member_sources", f.token_member_sources);
    verified_array!("payload_lengths", f.payload_lengths);
    verified_array!("query_denominators", f.query_denominators);
    verified_array!("prepared_weight_offsets", f.prepared_weight_offsets);
    verified_array!("prepared_weights", f.prepared_weights);
    verified_array!("contract_source", f.contract_source);
    verified_array!("contract_chain", f.contract_chain);
    verified_array!("contract_payload", f.contract_payload);
    verified_array!("contract_weight", f.contract_weight);
    verified_array!("fallback_atom_offsets", f.fallback_atom_offsets);
    verified_array!("fallback_atom_contracts", f.fallback_atom_contracts);
    verified_array!("primary_storage_shards", b.primary_storage_shards);
    verified_array!("template_simhashes", b.template_simhashes);
    verified_array!("content_simhashes", b.content_simhashes);
    verified_array!("routing_statuses", b.routing_statuses);
    verified_array!("atom_block_offsets", b.atom_block_offsets);
    verified_array!("atom_block_ids", b.atom_block_ids);
    verified_array!("block_atom_offsets", b.block_atom_offsets);
    verified_array!("block_atoms", b.block_atoms);
    verified_array!("block_kinds", b.block_kinds);
    verified_array!("block_keys", b.block_keys);
    for name in s.chain_names() {
        h.update((name.len() as u64).to_le_bytes());
        h.update(name.as_bytes());
    }
    for total in s.chain_totals() {
        h.update(total.name.as_bytes());
        h.update(total.contracts.to_le_bytes());
        h.update(total.nfts.to_le_bytes());
    }
    hex_digest(h.finalize().as_slice())
}
fn hash_u32(v: &[u32]) -> String {
    let mut h = Sha256::new();
    for &x in v {
        h.update(x.to_le_bytes())
    }
    hex_digest(h.finalize().as_slice())
}

fn hex_digest(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
