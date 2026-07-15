//! BaseEquivalent blocking compiler: joint SimHash bands + anchor bridges.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use rayon::prelude::*;
use thiserror::Error;

use crate::format::{self, ArrayKind, FormatError};
use crate::progress::{ProgressCounters, ProgressEvent, ProgressPhase, WorkUnit};

use super::stats::{BlockStats, HotBlockPlan};
use super::{simhash_band_value, BLOCKING_REVISION};

/// Number of SimHash bands (legacy `METADATA_CONSERVATIVE_SIMHASH_BANDS`).
pub const BANDS: usize = 8;
/// Bits per SimHash band.
pub const BAND_BITS: usize = 8;
/// `BANDS * BANDS` joint template×content families.
pub const JOINT_BAND_FAMILIES: usize = BANDS * BANDS;
/// Max anchors retained per dimension sketch.
pub const ANCHOR_COUNT: usize = 16;

/// Default tile side when splitting a hot block's upper-triangle pair matrix.
const DEFAULT_HOT_TILE_SIZE: u32 = 1024;

/// Per-atom in-memory sketch consumed by the blocking compiler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AtomSketch {
    pub template_simhash: u64,
    pub content_simhash: u64,
    pub template_anchors: Vec<u32>,
    pub content_anchors: Vec<u32>,
    pub has_template_terms: bool,
    pub has_content_terms: bool,
}

/// Routing status persisted as `atom_routing_status.u8`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RoutingStatus {
    Routed = 0,
    HotBlock = 1,
    ProvenNoCandidate = 2,
}

/// Kind of a compiled routing block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockKind {
    Joint,
    TemplateAnchorBridge,
    ContentAnchorBridge,
}

impl BlockKind {
    pub fn is_joint(self) -> bool {
        matches!(self, BlockKind::Joint)
    }

    pub fn is_anchor_bridge(self) -> bool {
        matches!(
            self,
            BlockKind::TemplateAnchorBridge | BlockKind::ContentAnchorBridge
        )
    }
}

/// Compiler knobs for BaseEquivalent compile.
#[derive(Debug, Clone)]
pub struct BlockingCompileConfig {
    /// Planning cap: membership above this emits a hot-block plan (never drops pairs).
    pub max_routing_block_members: usize,
}

/// In-memory + on-disk BaseEquivalent blocking artifacts.
#[derive(Debug, Clone, PartialEq)]
pub struct BlockingBundle {
    pub primary_storage_shards: Vec<u32>,
    pub template_simhashes: Vec<u64>,
    pub content_simhashes: Vec<u64>,
    pub routing_statuses: Vec<u8>,
    pub atom_block_offsets: Vec<u64>,
    pub atom_block_ids: Vec<u32>,
    pub block_atom_offsets: Vec<u64>,
    pub block_atoms: Vec<u32>,
    pub block_kinds: Vec<BlockKind>,
    /// Revisioned routing descriptor key. Joint blocks encode
    /// `(family << 16) | bucket`; anchor blocks store the anchor term ID.
    pub block_keys: Vec<u64>,
    pub block_stats: BlockStats,
    pub hot_block_plans: Vec<HotBlockPlan>,
}

#[derive(Debug, Error)]
pub enum BlockingError {
    #[error(transparent)]
    Identity(#[from] crate::identity::IdentityOverflow),
    #[error(transparent)]
    Format(#[from] FormatError),
    #[error("hot block {block_id} with {member_count} members cannot be planned under cap {cap}")]
    HotBlockUnplannable {
        block_id: u32,
        member_count: usize,
        cap: usize,
    },
    /// Non-proven atom ended with zero routing memberships (fail closed).
    #[error(
        "atom {atom_index} is not ProvenNoCandidate but has no routing block membership \
         (has_template_terms={has_template_terms})"
    )]
    AtomWithoutRoutingMembership {
        atom_index: u32,
        has_template_terms: bool,
    },
    #[error("blocking progress work overflow")]
    WorkOverflow,
    #[error("could not create blocking worker pool")]
    WorkerPool,
}

/// Scoring owner = minimum shared BaseEquivalent routing `block_id`.
pub fn scoring_owner(bundle: &BlockingBundle, left: u32, right: u32) -> Option<u32> {
    let l0 = bundle.atom_block_offsets[left as usize] as usize;
    let l1 = bundle.atom_block_offsets[left as usize + 1] as usize;
    let r0 = bundle.atom_block_offsets[right as usize] as usize;
    let r1 = bundle.atom_block_offsets[right as usize + 1] as usize;
    let left_ids = &bundle.atom_block_ids[l0..l1];
    let right_ids = &bundle.atom_block_ids[r0..r1];
    let mut i = 0usize;
    let mut j = 0usize;
    while i < left_ids.len() && j < right_ids.len() {
        match left_ids[i].cmp(&right_ids[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => return Some(left_ids[i]),
        }
    }
    None
}

/// Compile BaseEquivalent joint/anchor routing CSR and persist under `out_dir`.
pub fn compile_base_equivalent(
    atoms: &[AtomSketch],
    config: &BlockingCompileConfig,
    out_dir: &Path,
) -> Result<BlockingBundle, BlockingError> {
    compile_base_equivalent_with_progress(atoms, config, out_dir, |_| {})
}

pub fn compile_base_equivalent_with_progress(
    atoms: &[AtomSketch],
    config: &BlockingCompileConfig,
    out_dir: &Path,
    progress: impl FnMut(ProgressEvent),
) -> Result<BlockingBundle, BlockingError> {
    compile_base_equivalent_parallel_with_progress(atoms, config, out_dir, 1, progress)
}

pub fn compile_base_equivalent_parallel_with_progress(
    atoms: &[AtomSketch],
    config: &BlockingCompileConfig,
    out_dir: &Path,
    lanes: usize,
    mut progress: impl FnMut(ProgressEvent),
) -> Result<BlockingBundle, BlockingError> {
    std::fs::create_dir_all(out_dir).map_err(FormatError::from)?;

    let atom_count = atoms.len();
    let joint_work = (atom_count as u64)
        .checked_mul(JOINT_BAND_FAMILIES as u64)
        .ok_or(BlockingError::WorkOverflow)?;
    let anchor_work = atoms.iter().try_fold(0u64, |total, atom| {
        let template = if atom.has_content_terms && atom.has_template_terms {
            atom.template_anchors.len().min(ANCHOR_COUNT) as u64
        } else {
            0
        };
        let content = if atom.has_content_terms {
            atom.content_anchors.len().min(ANCHOR_COUNT) as u64
        } else {
            0
        };
        total
            .checked_add(template)
            .and_then(|value| value.checked_add(content))
            .ok_or(BlockingError::WorkOverflow)
    })?;
    let compile_total = joint_work
        .checked_add(anchor_work)
        .ok_or(BlockingError::WorkOverflow)?;
    progress(ProgressEvent::determinate(
        ProgressPhase::BlockingCompile,
        0,
        compile_total,
        WorkUnit::Items,
        ProgressCounters::default(),
    ));
    let mut compile_completed = 0u64;
    let atom_count_u32 = crate::identity::checked_u32_identity("atoms", atom_count as u64)?;
    let primary_storage_shards: Vec<u32> = (0..atom_count_u32).collect();
    let template_simhashes: Vec<u64> = atoms.iter().map(|a| a.template_simhash).collect();
    let content_simhashes: Vec<u64> = atoms.iter().map(|a| a.content_simhash).collect();

    // Exact predicate: empty content terms ⇒ no candidate possible under current scorer.
    let proven: Vec<bool> = atoms.iter().map(|a| !a.has_content_terms).collect();

    // Build the forward CSR directly. Keeping one Vec per block and flattening
    // it later adds a third membership-sized allocation beside the two final
    // CSR directions at peak.
    let mut block_kinds = Vec::new();
    let mut block_keys = Vec::new();
    let mut block_atom_offsets = vec![0u64];
    let mut block_atoms = Vec::new();

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(lanes.clamp(1, JOINT_BAND_FAMILIES))
        .thread_name(|index| format!("metadata-blocking-{index}"))
        .build()
        .map_err(|_| BlockingError::WorkerPool)?;
    let joint_blocks = pool.install(|| {
        (0..JOINT_BAND_FAMILIES)
            .into_par_iter()
            .map(|family| build_joint_family(atoms, &proven, family))
            .collect::<Vec<_>>()
    });
    for (family, buckets) in joint_blocks.into_iter().enumerate() {
        for (bucket, members) in buckets {
            let key = ((family as u64) << 16) | u64::from(bucket);
            append_forward_block(
                &mut block_kinds,
                &mut block_keys,
                &mut block_atom_offsets,
                &mut block_atoms,
                BlockKind::Joint,
                key,
                members,
            );
        }
        compile_completed = compile_completed
            .checked_add(atom_count as u64)
            .ok_or(BlockingError::WorkOverflow)?;
        progress(ProgressEvent::determinate(
            ProgressPhase::BlockingCompile,
            compile_completed,
            compile_total,
            WorkUnit::Items,
            ProgressCounters::default(),
        ));
    }

    let (template_anchor_groups, content_anchor_groups) = pool.install(|| {
        rayon::join(
            || build_anchor_groups_parallel(atoms, &proven, true),
            || build_anchor_groups_parallel(atoms, &proven, false),
        )
    });
    {
        for (anchor, members) in template_anchor_groups {
            append_forward_block(
                &mut block_kinds,
                &mut block_keys,
                &mut block_atom_offsets,
                &mut block_atoms,
                BlockKind::TemplateAnchorBridge,
                u64::from(anchor),
                members,
            );
        }
        compile_completed = compile_completed
            .checked_add(
                atoms
                    .iter()
                    .enumerate()
                    .filter(|(index, atom)| !proven[*index] && atom.has_template_terms)
                    .map(|(_, atom)| atom.template_anchors.len().min(ANCHOR_COUNT) as u64)
                    .sum::<u64>(),
            )
            .ok_or(BlockingError::WorkOverflow)?;
        progress(ProgressEvent::determinate(
            ProgressPhase::BlockingCompile,
            compile_completed,
            compile_total,
            WorkUnit::Items,
            ProgressCounters::default(),
        ));
    }

    {
        for (anchor, members) in content_anchor_groups {
            append_forward_block(
                &mut block_kinds,
                &mut block_keys,
                &mut block_atom_offsets,
                &mut block_atoms,
                BlockKind::ContentAnchorBridge,
                u64::from(anchor),
                members,
            );
        }
        compile_completed = compile_completed
            .checked_add(
                atoms
                    .iter()
                    .enumerate()
                    .filter(|(index, atom)| !proven[*index] && atom.has_content_terms)
                    .map(|(_, atom)| atom.content_anchors.len().min(ANCHOR_COUNT) as u64)
                    .sum::<u64>(),
            )
            .ok_or(BlockingError::WorkOverflow)?;
        progress(ProgressEvent::determinate(
            ProgressPhase::BlockingCompile,
            compile_completed,
            compile_total,
            WorkUnit::Items,
            ProgressCounters::default(),
        ));
    }

    let block_count = block_kinds.len();
    crate::identity::checked_u32_identity("blocks", block_count as u64)?;
    crate::identity::checked_u32_identity(
        "maximum block membership",
        block_atom_offsets
            .windows(2)
            .map(|window| window[1].saturating_sub(window[0]))
            .max()
            .unwrap_or(0),
    )?;
    let finalize_total = (block_count as u64)
        .checked_mul(2)
        .and_then(|value| value.checked_add((atom_count as u64).checked_mul(2)?))
        .and_then(|value| value.checked_add(1))
        .ok_or(BlockingError::WorkOverflow)?;
    progress(ProgressEvent::determinate(
        ProgressPhase::BlockingFinalize,
        0,
        finalize_total,
        WorkUnit::Items,
        ProgressCounters::default(),
    ));
    let mut finalize_completed = 0u64;
    let mut sizes = Vec::with_capacity(block_count);
    let mut hot_block_plans = Vec::new();
    let mut atom_in_hot = vec![false; atom_count];
    let mut membership_count = 0usize;

    for block_id in 0..block_count {
        let start = block_atom_offsets[block_id] as usize;
        let end = block_atom_offsets[block_id + 1] as usize;
        let members = &block_atoms[start..end];
        sizes.push(members.len());
        if members.len() > config.max_routing_block_members {
            if config.max_routing_block_members == 0 {
                return Err(BlockingError::HotBlockUnplannable {
                    block_id: block_id as u32,
                    member_count: members.len(),
                    cap: 0,
                });
            }
            let tile_size = DEFAULT_HOT_TILE_SIZE
                .min(
                    u32::try_from(config.max_routing_block_members)
                        .expect("hot threshold is below a checked u32 membership"),
                )
                .max(1);
            let plan = HotBlockPlan::cover_upper_triangle(
                block_id as u32,
                members.len() as u32,
                tile_size,
            );
            if plan.tile_count == 0 && members.len() > 1 {
                return Err(BlockingError::HotBlockUnplannable {
                    block_id: block_id as u32,
                    member_count: members.len(),
                    cap: config.max_routing_block_members,
                });
            }
            for &a in members {
                atom_in_hot[a as usize] = true;
            }
            hot_block_plans.push(plan);
        }
        membership_count = membership_count
            .checked_add(members.len())
            .ok_or(BlockingError::WorkOverflow)?;
        finalize_completed = finalize_completed.saturating_add(1);
        emit_blocking_progress(
            &mut progress,
            ProgressPhase::BlockingFinalize,
            finalize_completed,
            finalize_total,
        );
    }

    debug_assert_eq!(membership_count, block_atoms.len());
    let (atom_block_offsets, atom_block_ids) =
        build_inverse_block_csr_parallel(&block_atom_offsets, &block_atoms, atom_count, lanes)?;
    finalize_completed = finalize_completed
        .saturating_add(atom_count as u64)
        .saturating_add(block_count as u64);
    emit_blocking_progress(
        &mut progress,
        ProgressPhase::BlockingFinalize,
        finalize_completed,
        finalize_total,
    );

    let mut routing_statuses = vec![RoutingStatus::Routed as u8; atom_count];
    for (i, is_proven) in proven.iter().enumerate() {
        let membership = (atom_block_offsets[i + 1] - atom_block_offsets[i]) as usize;
        if *is_proven {
            // Exact-safe: empty content ⇒ any pair score is 0; membership must stay empty.
            debug_assert_eq!(membership, 0);
            routing_statuses[i] = RoutingStatus::ProvenNoCandidate as u8;
        } else if membership == 0 {
            // Not exact-safe (has content terms) but no joint/anchor placement → fail closed.
            return Err(BlockingError::AtomWithoutRoutingMembership {
                atom_index: i as u32,
                has_template_terms: atoms[i].has_template_terms,
            });
        } else if atom_in_hot[i] {
            routing_statuses[i] = RoutingStatus::HotBlock as u8;
        } else {
            routing_statuses[i] = RoutingStatus::Routed as u8;
        }
        finalize_completed = finalize_completed.saturating_add(1);
        emit_blocking_progress(
            &mut progress,
            ProgressPhase::BlockingFinalize,
            finalize_completed,
            finalize_total,
        );
    }

    let block_stats = BlockStats::from_block_sizes(atom_count, &sizes);
    let bundle = BlockingBundle {
        primary_storage_shards,
        template_simhashes,
        content_simhashes,
        routing_statuses,
        atom_block_offsets,
        atom_block_ids,
        block_atom_offsets,
        block_atoms,
        block_kinds,
        block_keys,
        block_stats,
        hot_block_plans,
    };
    persist_bundle(&bundle, out_dir)?;
    finalize_completed = finalize_completed.saturating_add(1);
    emit_blocking_progress(
        &mut progress,
        ProgressPhase::BlockingFinalize,
        finalize_completed,
        finalize_total,
    );
    debug_assert_eq!(BLOCKING_REVISION, 3);
    Ok(bundle)
}

fn build_joint_family(
    atoms: &[AtomSketch],
    proven: &[bool],
    family: usize,
) -> Vec<(u16, Vec<u32>)> {
    let template_band = family / BANDS;
    let content_band = family % BANDS;
    let mut buckets: BTreeMap<u16, Vec<u32>> = BTreeMap::new();
    for (atom_index, atom) in atoms.iter().enumerate() {
        if proven[atom_index] || !atom.has_template_terms || !atom.has_content_terms {
            continue;
        }
        let tv = simhash_band_value(atom.template_simhash, template_band);
        let cv = simhash_band_value(atom.content_simhash, content_band);
        let bucket = (u16::from(tv) << BAND_BITS) | u16::from(cv);
        buckets.entry(bucket).or_default().push(atom_index as u32);
    }
    buckets.into_iter().collect()
}

fn build_inverse_block_csr_parallel(
    block_atom_offsets: &[u64],
    block_atoms: &[u32],
    atom_count: usize,
    lanes: usize,
) -> Result<(Vec<u64>, Vec<u32>), BlockingError> {
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(lanes.max(1))
        .build()
        .map_err(|_| BlockingError::WorkerPool)?;
    let counts = (0..atom_count)
        .map(|_| AtomicU64::new(0))
        .collect::<Vec<_>>();
    pool.install(|| {
        block_atoms.par_iter().for_each(|&atom| {
            counts[atom as usize].fetch_add(1, Ordering::Relaxed);
        });
    });
    let counts = counts
        .into_iter()
        .map(AtomicU64::into_inner)
        .collect::<Vec<_>>();
    let mut offsets = Vec::with_capacity(atom_count.saturating_add(1));
    offsets.push(0u64);
    for count in counts {
        offsets.push(
            offsets
                .last()
                .copied()
                .unwrap_or(0)
                .checked_add(count)
                .ok_or(BlockingError::WorkOverflow)?,
        );
    }
    if offsets.last().copied().unwrap_or(0) != block_atoms.len() as u64 {
        return Err(BlockingError::WorkOverflow);
    }
    let cursors = offsets[..atom_count]
        .iter()
        .copied()
        .map(AtomicU64::new)
        .collect::<Vec<_>>();
    let inverse = (0..block_atoms.len())
        .map(|_| AtomicU32::new(0))
        .collect::<Vec<_>>();
    pool.install(|| {
        (0..block_atom_offsets.len().saturating_sub(1))
            .into_par_iter()
            .for_each(|block_id| {
                let begin = block_atom_offsets[block_id] as usize;
                let end = block_atom_offsets[block_id + 1] as usize;
                for &atom in &block_atoms[begin..end] {
                    let position = cursors[atom as usize].fetch_add(1, Ordering::Relaxed) as usize;
                    inverse[position].store(block_id as u32, Ordering::Relaxed);
                }
            });
    });
    let mut inverse = pool.install(|| {
        inverse
            .into_par_iter()
            .map(AtomicU32::into_inner)
            .collect::<Vec<_>>()
    });
    pool.install(|| parallel_sort_buckets(&mut inverse, &offsets));
    Ok((offsets, inverse))
}

fn parallel_sort_buckets<T: Ord + Send>(values: &mut [T], offsets: &[u64]) {
    fn recurse<T: Ord + Send>(
        values: &mut [T],
        offsets: &[u64],
        first: usize,
        end: usize,
        base: usize,
    ) {
        if first >= end {
            return;
        }
        if values.len() < 16_384 || end - first <= 1 {
            for row in first..end {
                let begin = offsets[row] as usize - base;
                let finish = offsets[row + 1] as usize - base;
                values[begin..finish].sort_unstable();
            }
            return;
        }
        let middle = first + (end - first) / 2;
        let split = offsets[middle] as usize - base;
        let (left, right) = values.split_at_mut(split);
        rayon::join(
            || recurse(left, offsets, first, middle, base),
            || recurse(right, offsets, middle, end, offsets[middle] as usize),
        );
    }
    recurse(values, offsets, 0, offsets.len().saturating_sub(1), 0);
}

fn append_forward_block(
    block_kinds: &mut Vec<BlockKind>,
    block_keys: &mut Vec<u64>,
    block_atom_offsets: &mut Vec<u64>,
    block_atoms: &mut Vec<u32>,
    kind: BlockKind,
    key: u64,
    mut members: Vec<u32>,
) {
    members.sort_unstable();
    members.dedup();
    if !members.is_empty() {
        block_kinds.push(kind);
        block_keys.push(key);
        block_atoms.extend(members);
        block_atom_offsets.push(block_atoms.len() as u64);
    }
}

fn build_anchor_groups_parallel(
    atoms: &[AtomSketch],
    proven: &[bool],
    template: bool,
) -> Vec<(u32, Vec<u32>)> {
    let mut pairs = atoms
        .par_iter()
        .enumerate()
        .flat_map_iter(|(atom_index, atom)| {
            let anchors = if template {
                &atom.template_anchors
            } else {
                &atom.content_anchors
            };
            let has_terms = if template {
                atom.has_template_terms
            } else {
                atom.has_content_terms
            };
            let enabled = !proven[atom_index] && has_terms;
            anchors
                .iter()
                .take(ANCHOR_COUNT)
                .copied()
                .filter(move |_| enabled)
                .map(move |anchor| (anchor, atom_index as u32))
        })
        .collect::<Vec<_>>();
    pairs.par_sort_unstable();
    let mut groups = Vec::<(u32, Vec<u32>)>::new();
    for (anchor, atom) in pairs {
        if groups.last().is_none_or(|(current, _)| *current != anchor) {
            groups.push((anchor, Vec::new()));
        }
        groups.last_mut().expect("group just created").1.push(atom);
    }
    groups
}

fn emit_blocking_progress(
    progress: &mut impl FnMut(ProgressEvent),
    phase: ProgressPhase,
    completed: u64,
    total: u64,
) {
    if completed.is_multiple_of(16_384) || completed == total {
        progress(ProgressEvent::determinate(
            phase,
            completed,
            total,
            WorkUnit::Items,
            ProgressCounters::default(),
        ));
    }
}

fn persist_bundle(bundle: &BlockingBundle, out_dir: &Path) -> Result<(), BlockingError> {
    format::write_u32_array(
        &out_dir.join("atom_primary_storage_shard.u32"),
        ArrayKind::U32,
        &bundle.primary_storage_shards,
    )?;
    format::write_u64_array(
        &out_dir.join("atom_template_simhash.u64"),
        ArrayKind::U64,
        &bundle.template_simhashes,
    )?;
    format::write_u64_array(
        &out_dir.join("atom_content_simhash.u64"),
        ArrayKind::U64,
        &bundle.content_simhashes,
    )?;
    format::write_u8_array(
        &out_dir.join("atom_routing_status.u8"),
        &bundle.routing_statuses,
    )?;
    format::write_u64_array(
        &out_dir.join("atom_block_offsets.u64"),
        ArrayKind::U64,
        &bundle.atom_block_offsets,
    )?;
    format::write_u32_array(
        &out_dir.join("atom_block_ids.u32"),
        ArrayKind::U32,
        &bundle.atom_block_ids,
    )?;
    format::write_u64_array(
        &out_dir.join("block_atom_offsets.u64"),
        ArrayKind::U64,
        &bundle.block_atom_offsets,
    )?;
    format::write_u32_array(
        &out_dir.join("block_atoms.u32"),
        ArrayKind::U32,
        &bundle.block_atoms,
    )?;
    format::write_u32_iter(
        &out_dir.join("block_kinds.u32"),
        ArrayKind::U32,
        bundle.block_kinds.len() as u64,
        bundle.block_kinds.iter().map(|kind| match kind {
            BlockKind::Joint => 0,
            BlockKind::TemplateAnchorBridge => 1,
            BlockKind::ContentAnchorBridge => 2,
        }),
    )?;
    format::write_u64_array(
        &out_dir.join("block_keys.u64"),
        ArrayKind::U64,
        &bundle.block_keys,
    )?;
    bundle
        .block_stats
        .write_bin(&out_dir.join("block_stats.bin"))?;
    HotBlockPlan::write_plans_bin(
        &out_dir.join("hot_block_plans.bin"),
        &bundle.hot_block_plans,
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parallel_inverse_block_csr_matches_forward_memberships() {
        let (offsets, ids) =
            build_inverse_block_csr_parallel(&[0, 2, 4], &[0, 2, 1, 2], 3, 4).unwrap();

        assert_eq!(offsets, vec![0, 1, 2, 4]);
        assert_eq!(ids, vec![0, 1, 0, 1]);
    }

    #[test]
    fn parallel_anchor_grouping_is_sorted_and_deterministic() {
        let atoms = vec![
            AtomSketch {
                template_simhash: 0,
                content_simhash: 0,
                template_anchors: vec![9, 3],
                content_anchors: vec![],
                has_template_terms: true,
                has_content_terms: false,
            },
            AtomSketch {
                template_simhash: 0,
                content_simhash: 0,
                template_anchors: vec![3],
                content_anchors: vec![],
                has_template_terms: true,
                has_content_terms: false,
            },
        ];

        let groups = build_anchor_groups_parallel(&atoms, &[false, false], true);

        assert_eq!(groups, vec![(3, vec![0, 1]), (9, vec![0])]);
    }
}
