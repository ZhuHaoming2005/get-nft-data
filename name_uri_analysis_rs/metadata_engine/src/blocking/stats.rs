//! Block size statistics and hot-block tile plans.

use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::format::atomic;
use crate::format::FormatError;

/// Aggregate membership / work stats for a compiled blocking bundle.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BlockStats {
    pub block_count: u32,
    pub atom_count: u32,
    pub smax: u32,
    pub p50: u32,
    pub p95: u32,
    pub p99: u32,
    /// `sum(block_size) / atom_count` (0 when atom_count == 0).
    pub replication: f64,
    /// `sum(block_size * (block_size - 1) / 2)`.
    pub bucket_pair_work: u64,
}

impl BlockStats {
    pub fn from_block_sizes(atom_count: usize, sizes: &[usize]) -> Self {
        let mut sorted: Vec<usize> = sizes.to_vec();
        sorted.sort_unstable();
        let block_count = sorted.len() as u32;
        let smax = sorted.last().copied().unwrap_or(0) as u32;
        let p50 = percentile_sorted(&sorted, 50) as u32;
        let p95 = percentile_sorted(&sorted, 95) as u32;
        let p99 = percentile_sorted(&sorted, 99) as u32;
        let membership_sum: u64 = sorted.iter().map(|&s| s as u64).sum();
        let replication = if atom_count == 0 {
            0.0
        } else {
            membership_sum as f64 / atom_count as f64
        };
        let bucket_pair_work: u64 = sorted
            .iter()
            .map(|&s| {
                let s = s as u64;
                s.saturating_mul(s.saturating_sub(1)) / 2
            })
            .sum();
        Self {
            block_count,
            atom_count: atom_count as u32,
            smax,
            p50,
            p95,
            p99,
            replication,
            bucket_pair_work,
        }
    }

    pub fn write_bin(&self, path: &Path) -> Result<(), FormatError> {
        let json =
            serde_json::to_vec(self).map_err(|e| FormatError::InvalidManifest(e.to_string()))?;
        atomic::write_atomic(path, &json)
    }
}

fn percentile_sorted(sorted: &[usize], pct: usize) -> usize {
    if sorted.is_empty() {
        return 0;
    }
    let rank = ((pct as f64 / 100.0) * (sorted.len().saturating_sub(1) as f64)).round() as usize;
    sorted[rank.min(sorted.len() - 1)]
}

/// One upper-triangle tile over sorted hot-block members (`[start, end)` ranges).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HotBlockTile {
    pub left_start: u32,
    pub left_end: u32,
    pub right_start: u32,
    pub right_end: u32,
}

/// Hot-block execution plan: upper-triangle tiles covering all pairs in a hot block.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HotBlockPlan {
    pub block_id: u32,
    pub member_count: u32,
    pub tile_size: u32,
    pub tile_count: u64,
}

impl HotBlockPlan {
    /// Cover all upper-triangle pairs among `member_count` atoms with square tiles of
    /// side `tile_size` (at least 1). Never drops pairs.
    pub fn cover_upper_triangle(block_id: u32, member_count: u32, tile_size: u32) -> Self {
        let tile_size = tile_size.max(1);
        let side = u64::from(member_count.div_ceil(tile_size));
        let tile_count = side.saturating_mul(side.saturating_add(1)) / 2;
        Self {
            block_id,
            member_count,
            tile_size,
            tile_count,
        }
    }

    /// Generate tiles lazily; the persistent descriptor stays O(1) for a hot block.
    pub fn tiles(&self) -> impl Iterator<Item = HotBlockTile> + '_ {
        let side = self.member_count.div_ceil(self.tile_size);
        (0..side).flat_map(move |row_tile| {
            (row_tile..side).map(move |col_tile| {
                let left_start = row_tile * self.tile_size;
                let right_start = col_tile * self.tile_size;
                HotBlockTile {
                    left_start,
                    left_end: left_start
                        .saturating_add(self.tile_size)
                        .min(self.member_count),
                    right_start,
                    right_end: right_start
                        .saturating_add(self.tile_size)
                        .min(self.member_count),
                }
            })
        })
    }

    pub fn write_plans_bin(path: &Path, plans: &[HotBlockPlan]) -> Result<(), FormatError> {
        let json =
            serde_json::to_vec(plans).map_err(|e| FormatError::InvalidManifest(e.to_string()))?;
        atomic::write_atomic(path, &json)
    }
}

/// Incremental JSON-array writer used by the external blocking compiler.  Hot
/// plans stay O(1) per block and never require a block-count-sized Rust Vec.
pub struct HotBlockPlanSink {
    final_path: PathBuf,
    partial_path: PathBuf,
    file: Option<File>,
    first: bool,
}

impl HotBlockPlanSink {
    pub fn create(path: &Path) -> Result<Self, FormatError> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let partial_path = atomic::partial_path(path);
        let mut file = File::create(&partial_path)?;
        file.write_all(b"[")?;
        Ok(Self {
            final_path: path.to_path_buf(),
            partial_path,
            file: Some(file),
            first: true,
        })
    }

    pub fn push(&mut self, plan: &HotBlockPlan) -> Result<(), FormatError> {
        let file = self.file.as_mut().expect("hot-plan sink is open");
        if !self.first {
            file.write_all(b",")?;
        }
        serde_json::to_writer(&mut *file, plan)
            .map_err(|error| FormatError::InvalidManifest(error.to_string()))?;
        self.first = false;
        Ok(())
    }

    pub fn finish(mut self) -> Result<(), FormatError> {
        let mut file = self.file.take().expect("hot-plan sink is open");
        file.write_all(b"]")?;
        file.sync_all()?;
        drop(file);
        atomic::replace_file_atomically(&self.partial_path, &self.final_path)
    }
}

impl Drop for HotBlockPlanSink {
    fn drop(&mut self) {
        if self.file.is_some() {
            let _ = std::fs::remove_file(&self.partial_path);
        }
    }
}
