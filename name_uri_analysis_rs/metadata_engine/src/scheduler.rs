//! Fixed-width Work Catalog, frozen RecallPlan and coverage certificates.

use std::path::Path;

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
    /// Hot-block tile row; ignored for [`JobShape::MicroBatch`].
    pub tile_row: u16,
    /// Hot-block tile column; ignored for [`JobShape::MicroBatch`].
    pub tile_col: u16,
    pub estimated_work: u64,
}

pub const CATALOG_REVISION: u32 = 2;

/// Atom-member tile size for hot-block LeftTileFanout catalog jobs.
pub const HOT_BLOCK_TILE: usize = 1024;

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkCatalog {
    pub jobs: Vec<JobDescriptor>,
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
        let b = snapshot.blocking();
        crate::identity::checked_u32_identity("catalog blocks", b.block_kinds.len() as u64)?;
        let total_blocks = b.block_kinds.len() as u64;
        let mut jobs = Vec::new();
        let mut start = 0usize;
        progress(0, total_blocks, 0);
        while start < b.block_kinds.len() {
            let members = b.block_atom_offsets[start + 1] - b.block_atom_offsets[start];
            if members > hot_members {
                let tile_count = (members as usize).div_ceil(HOT_BLOCK_TILE);
                for ti in 0..tile_count {
                    for tj in ti..tile_count {
                        check_next_job(jobs.len(), budget)?;
                        jobs.push(descriptor(
                            jobs.len(),
                            start,
                            1,
                            JobShape::LeftTileFanout,
                            ti as u16,
                            tj as u16,
                            tile_contract_pair_work(snapshot, start, ti, tj)?,
                        ));
                    }
                }
                start += 1;
                progress(start as u64, total_blocks, jobs.len() as u64);
                continue;
            }
            let first = start;
            let mut total = 0u64;
            let mut estimated_work = 0u64;
            while start < b.block_kinds.len() {
                let n = b.block_atom_offsets[start + 1] - b.block_atom_offsets[start];
                if n > hot_members
                    || (start > first && total.saturating_add(n) > budget.cold_members_per_job)
                {
                    break;
                }
                total = total.saturating_add(n);
                estimated_work = estimated_work
                    .checked_add(block_contract_pair_work(snapshot, start)?)
                    .ok_or(SchedulerError::WorkOverflow)?;
                start += 1;
            }
            check_next_job(jobs.len(), budget)?;
            jobs.push(descriptor(
                jobs.len(),
                first,
                start - first,
                JobShape::MicroBatch,
                0,
                0,
                estimated_work,
            ));
            progress(start as u64, total_blocks, jobs.len() as u64);
        }
        let bytes = (jobs.len() * std::mem::size_of::<JobDescriptor>()) as u64;
        if jobs.len() as u64 > budget.max_jobs || bytes > budget.max_catalog_bytes {
            return Err(SchedulerError::Budget {
                jobs: jobs.len() as u64,
                max_jobs: budget.max_jobs,
                bytes,
                max_bytes: budget.max_catalog_bytes,
            });
        }
        Ok(Self {
            jobs,
            snapshot_fingerprint: snapshot_fingerprint(snapshot),
        })
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
        let catalog = Self::build_with_progress(snapshot, budget, hot_members, progress)?;
        catalog.commit(dir)?;
        Ok(catalog)
    }

    pub fn commit(&self, dir: &Path) -> Result<(), SchedulerError> {
        crate::identity::checked_u32_identity("catalog jobs", self.jobs.len() as u64)?;
        std::fs::create_dir_all(dir)?;
        let first = self.jobs.iter().map(|j| j.first_block).collect::<Vec<_>>();
        let counts = self.jobs.iter().map(|j| j.block_count).collect::<Vec<_>>();
        let flags = self
            .jobs
            .iter()
            .map(|j| (j.shape as u32) | (u32::from(j.risk) << 8) | (u32::from(j.rescue) << 16))
            .collect::<Vec<_>>();
        let work = self
            .jobs
            .iter()
            .map(|j| j.estimated_work)
            .collect::<Vec<_>>();
        let tiles = self
            .jobs
            .iter()
            .map(|j| ((u32::from(j.tile_row)) << 16) | u32::from(j.tile_col))
            .collect::<Vec<_>>();
        crate::format::write_u32_array(
            &dir.join("job_first_block.u32"),
            crate::format::ArrayKind::U32,
            &first,
        )?;
        crate::format::write_u32_array(
            &dir.join("job_block_count.u32"),
            crate::format::ArrayKind::U32,
            &counts,
        )?;
        crate::format::write_u32_array(
            &dir.join("job_flags.u32"),
            crate::format::ArrayKind::U32,
            &flags,
        )?;
        crate::format::write_u64_array(
            &dir.join("job_estimated_work.u64"),
            crate::format::ArrayKind::U64,
            &work,
        )?;
        crate::format::write_u32_array(
            &dir.join("job_tiles.u32"),
            crate::format::ArrayKind::U32,
            &tiles,
        )?;
        let ready = serde_json::json!({
            "catalog_revision": CATALOG_REVISION,
            "job_count": self.jobs.len(),
            "snapshot_fingerprint": self.snapshot_fingerprint
        })
        .to_string();
        crate::format::commit_ready(dir, "catalog.ready", &ready)?;
        Ok(())
    }

    pub fn open(dir: &Path, snapshot: &MetadataSnapshot) -> Result<Self, SchedulerError> {
        #[derive(Deserialize)]
        struct Ready {
            catalog_revision: u32,
            job_count: usize,
            snapshot_fingerprint: String,
        }
        let ready: Ready = serde_json::from_slice(&std::fs::read(dir.join("catalog.ready"))?)?;
        if ready.catalog_revision != CATALOG_REVISION
            || ready.snapshot_fingerprint != snapshot_fingerprint(snapshot)
        {
            return Err(SchedulerError::StaleCoverage);
        }
        crate::identity::checked_u32_identity("catalog jobs", ready.job_count as u64)?;
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
        .any(|len| len != ready.job_count)
        {
            return Err(SchedulerError::StaleCoverage);
        }
        let mut jobs = Vec::with_capacity(ready.job_count);
        for index in 0..ready.job_count {
            let shape = match flags[index] & 0xff {
                0 => JobShape::MicroBatch,
                1 => JobShape::LeftTileFanout,
                _ => return Err(SchedulerError::StaleCoverage),
            };
            jobs.push(JobDescriptor {
                job_id: index as u32,
                first_block: first[index],
                block_count: counts[index],
                shape,
                risk: ((flags[index] >> 8) & 0xff) as u8,
                rescue: ((flags[index] >> 16) & 0xff) as u8,
                tile_row: (tiles[index] >> 16) as u16,
                tile_col: (tiles[index] & 0xffff) as u16,
                estimated_work: work[index],
            });
        }
        Ok(Self {
            jobs,
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

fn check_next_job(current_jobs: usize, budget: UniverseBudget) -> Result<(), SchedulerError> {
    let jobs = current_jobs.saturating_add(1) as u64;
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
    id: usize,
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

fn tile_contract_pair_work(
    snapshot: &MetadataSnapshot,
    block: usize,
    tile_row: usize,
    tile_col: usize,
) -> Result<u64, SchedulerError> {
    let blocking = snapshot.blocking();
    let features = snapshot.features();
    let begin = blocking.block_atom_offsets[block] as usize;
    let end = blocking.block_atom_offsets[block + 1] as usize;
    let members = &blocking.block_atoms[begin..end];
    let tile = HOT_BLOCK_TILE;
    let a0 = tile_row.saturating_mul(tile);
    let a1 = (a0 + tile).min(members.len());
    let b0 = tile_col.saturating_mul(tile);
    let b1 = (b0 + tile).min(members.len());
    let mut work = 0u64;
    for i in a0..a1 {
        let left_members = features.fallback_atom_offsets[members[i] as usize + 1]
            - features.fallback_atom_offsets[members[i] as usize];
        let j0 = if tile_row == tile_col {
            (i + 1).max(b0)
        } else {
            b0
        };
        for &right in &members[j0..b1] {
            let right_members = features.fallback_atom_offsets[right as usize + 1]
                - features.fallback_atom_offsets[right as usize];
            work = work
                .checked_add(
                    left_members
                        .checked_mul(right_members)
                        .ok_or(SchedulerError::WorkOverflow)?,
                )
                .ok_or(SchedulerError::WorkOverflow)?;
        }
    }
    Ok(work)
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
        let mut jobs = catalog.jobs.clone();
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
