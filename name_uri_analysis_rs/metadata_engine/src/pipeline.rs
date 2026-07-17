//! Snapshot-only production metadata pipeline. No DuckDB or payload API is reachable.

use memmap2::MmapMut;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet};
use std::io::{BufReader, BufWriter, Read, Write};
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
    commit_component_roots_dense, component_dense_roots_path, open_component_roots,
    reduce_stored_components_with_progress, ComponentSnapshotIdentity, Edge, EdgeBudget,
    EdgeCollector, EdgeCollectorScratch, EdgeCollectorScratchKind, ForestRun, ForestRunStorage,
    OpenComponentRoots,
};
use crate::resource::{MemoryBroker, MemoryError, MemoryLease};
use crate::scheduler::{JobShape, RecallPlan, UniverseBudget, WorkCatalog};
use crate::snapshot::{MetadataSnapshot, SnapshotError};
use crate::storage::{
    ArtifactClass, ArtifactRegistration, EvictionPlan, StorageBroker, StorageLease,
};

pub const DEFAULT_MAX_CANDIDATE_PAIR_VISITS: u64 = 200_000_000_000;
pub const DEFAULT_EXACT_SAMPLE_LEFTS: u64 = 1_024;
pub const DEFAULT_EXACT_PAIR_WORK: u64 = 20_000_000_000;

// Exact evidence is a resident statistical data set, not a connectivity
// forest.  Keep its admission independent from `edge_bytes`: tying the two
// together made small contract forests impose only a few MiB on evidence even
// when hundreds of GiB were available to Match.
const MAX_EVIDENCE_RESIDENT_BYTES: u64 = 8 * 1024 * 1024 * 1024;

const CONNECTIVITY_RUN_REVISION: u32 = 6;
const COMPONENT_ROOT_LAYOUT_REVISION: u32 = 6;
const MAX_RESCUE_PAYLOAD_CACHE_ENTRIES: usize = 65_536;
const RESCUE_PAYLOAD_CACHE_ENTRY_BYTES: u64 = 32;
const RESCUE_SEED_INDEX_ENTRY_BYTES: u64 = 64;
const SHARED_LOCAL_ROUTING_MIN_MEMBERS: usize = 256;
const CANCELLATION_CHECK_PAIRS: usize = 4_096;
const SUMMARY_STREAM_MAX_SCRATCH_BYTES: u64 = 64 * 1024 * 1024;
const SUMMARY_STREAM_MIN_SCRATCH_BYTES: u64 = 1024 * 1024;
const SUMMARY_STREAM_MAX_IO_BUFFER_BYTES: usize = 256 * 1024;
const SUMMARY_ENTRY_ENCODED_BYTES: usize = 16;

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
    intra: Vec<ForestRunStorage>,
    cross: Vec<ForestRunStorage>,
    pairs: Vec<Vec<ForestRunStorage>>,
    index_metrics: SerializableIndexMetrics,
    candidate_pair_visits: u64,
    accepted_edge_count: u64,
}

struct ConnectivityCommit<'a> {
    snapshot_fingerprint: &'a str,
    connectivity_plan_digest: &'a str,
    chain_count: usize,
    intra: &'a [ForestRunStorage],
    cross: &'a [ForestRunStorage],
    pairs: &'a [Vec<ForestRunStorage>],
    index_metrics: &'a SerializableIndexMetrics,
    candidate_pair_visits: u64,
    accepted_edge_count: u64,
}
pub struct RootStorage {
    inner: RootStorageInner,
}

enum RootStorageInner {
    Resident(Vec<u32>),
    Mapped(Arc<MappedRootStorage>),
}

struct MappedRootStorage {
    roots: Option<crate::format::MappedU32Array>,
    cleanup_file: Option<PathBuf>,
    cleanup_dir: Option<PathBuf>,
    _workspace: Option<Arc<TemporaryMappedWorkspace>>,
}

struct TemporaryMappedWorkspace {
    root: PathBuf,
}

impl Drop for TemporaryMappedWorkspace {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

impl Drop for MappedRootStorage {
    fn drop(&mut self) {
        drop(self.roots.take());
        if let Some(path) = self.cleanup_file.take() {
            let _ = std::fs::remove_file(path);
        }
        if let Some(path) = self.cleanup_dir.take() {
            let _ = std::fs::remove_dir(path);
        }
    }
}

impl RootStorage {
    fn resident(roots: Vec<u32>) -> Self {
        Self {
            inner: RootStorageInner::Resident(roots),
        }
    }

    fn mapped(
        roots: crate::format::MappedU32Array,
        cleanup_file: Option<PathBuf>,
        cleanup_dir: Option<PathBuf>,
    ) -> Self {
        Self {
            inner: RootStorageInner::Mapped(Arc::new(MappedRootStorage {
                roots: Some(roots),
                cleanup_file,
                cleanup_dir,
                _workspace: None,
            })),
        }
    }

    fn mapped_in_workspace(
        roots: crate::format::MappedU32Array,
        workspace: Arc<TemporaryMappedWorkspace>,
    ) -> Self {
        Self {
            inner: RootStorageInner::Mapped(Arc::new(MappedRootStorage {
                roots: Some(roots),
                cleanup_file: None,
                cleanup_dir: None,
                _workspace: Some(workspace),
            })),
        }
    }

    fn as_slice(&self) -> &[u32] {
        match &self.inner {
            RootStorageInner::Resident(roots) => roots,
            RootStorageInner::Mapped(roots) => roots
                .roots
                .as_deref()
                .expect("mapped roots remain available while storage is alive"),
        }
    }

    fn resident_bytes(&self) -> u64 {
        match &self.inner {
            RootStorageInner::Resident(roots) => {
                (roots.capacity() as u64).saturating_mul(std::mem::size_of::<u32>() as u64)
            }
            RootStorageInner::Mapped(_) => 0,
        }
    }

    #[cfg(test)]
    fn is_mapped(&self) -> bool {
        matches!(&self.inner, RootStorageInner::Mapped(_))
    }
}

impl Default for RootStorage {
    fn default() -> Self {
        Self::resident(Vec::new())
    }
}

impl Clone for RootStorage {
    fn clone(&self) -> Self {
        let inner = match &self.inner {
            RootStorageInner::Resident(roots) => RootStorageInner::Resident(roots.clone()),
            RootStorageInner::Mapped(roots) => RootStorageInner::Mapped(roots.clone()),
        };
        Self { inner }
    }
}

impl std::fmt::Debug for RootStorage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_slice().fmt(formatter)
    }
}

impl std::ops::Deref for RootStorage {
    type Target = [u32];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl AsRef<[u32]> for RootStorage {
    fn as_ref(&self) -> &[u32] {
        self.as_slice()
    }
}

impl Serialize for RootStorage {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.as_slice().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RootStorage {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Vec::<u32>::deserialize(deserializer).map(Self::resident)
    }
}

impl PartialEq for RootStorage {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for RootStorage {}

impl PartialEq<Vec<u32>> for RootStorage {
    fn eq(&self, other: &Vec<u32>) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl PartialEq<RootStorage> for Vec<u32> {
    fn eq(&self, other: &RootStorage) -> bool {
        self.as_slice() == other.as_slice()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScopeComponents {
    pub intra_roots: RootStorage,
    pub cross_roots: RootStorage,
    pub chain_pair_roots: Vec<ChainPairRoots>,
    #[serde(default)]
    pub chain_contract_offsets: RootStorage,
    #[serde(default)]
    pub chain_contracts: RootStorage,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChainPairRoots {
    pub left_chain: u32,
    pub right_chain: u32,
    pub left_contract_count: u32,
    pub roots: RootStorage,
}

impl ScopeComponents {
    pub fn contracts_for_chain(&self, chain: u32) -> Option<&[u32]> {
        let chain = chain as usize;
        let begin = *self.chain_contract_offsets.get(chain)? as usize;
        let end = *self.chain_contract_offsets.get(chain + 1)? as usize;
        self.chain_contracts.get(begin..end)
    }
}

impl ChainPairRoots {
    pub fn expand_global_roots(&self, scopes: &ScopeComponents) -> Option<Vec<u32>> {
        let left_contracts = scopes.contracts_for_chain(self.left_chain)?;
        let right_contracts = scopes.contracts_for_chain(self.right_chain)?;
        if left_contracts.len() != self.left_contract_count as usize
            || self.roots.len() != left_contracts.len().saturating_add(right_contracts.len())
        {
            return None;
        }
        let node_count = u32::try_from(scopes.chain_contracts.len()).ok()?;
        let mut global = Vec::new();
        global.try_reserve_exact(node_count as usize).ok()?;
        global.extend(0..node_count);
        for (local, &root) in self.roots.iter().enumerate() {
            let contract = if local < left_contracts.len() {
                left_contracts[local]
            } else {
                right_contracts[local - left_contracts.len()]
            };
            let root = root as usize;
            let root_contract = if root < left_contracts.len() {
                left_contracts[root]
            } else {
                *right_contracts.get(root - left_contracts.len())?
            };
            let canonical = global.get_mut(root_contract as usize)?;
            *canonical = (*canonical).min(contract);
        }
        for (local, &root) in self.roots.iter().enumerate() {
            let contract = if local < left_contracts.len() {
                left_contracts[local]
            } else {
                right_contracts[local - left_contracts.len()]
            };
            let root = root as usize;
            let root_contract = if root < left_contracts.len() {
                left_contracts[root]
            } else {
                *right_contracts.get(root - left_contracts.len())?
            };
            let canonical = *global.get(root_contract as usize)?;
            *global.get_mut(contract as usize)? = canonical;
        }
        Some(global)
    }
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
    runs: Vec<ForestRunStorage>,
    roots: Option<RootStorage>,
    needs_rebuild: bool,
    committed: bool,
}

fn create_zeroed_u32_mmap(path: &Path, len: usize) -> Result<Option<MmapMut>, PipelineError> {
    let byte_len = len
        .checked_mul(std::mem::size_of::<u32>())
        .ok_or(MemoryError::Overflow)?;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    file.set_len(u64::try_from(byte_len).map_err(|_| MemoryError::Overflow)?)?;
    if byte_len == 0 {
        return Ok(None);
    }
    // SAFETY: the file is held open while the mapping is created, has exactly
    // `byte_len` bytes, and this private scratch file is not concurrently
    // resized or mutated through another mapping.
    let mmap = unsafe { MmapMut::map_mut(&file)? };
    Ok(Some(mmap))
}

fn mapped_u32_words(mmap: &MmapMut, len: usize) -> Result<&[u32], PipelineError> {
    let byte_len = len
        .checked_mul(std::mem::size_of::<u32>())
        .ok_or(MemoryError::Overflow)?;
    if mmap.len() != byte_len
        || !(mmap.as_ptr() as usize).is_multiple_of(std::mem::align_of::<u32>())
    {
        return Err(PipelineError::Invariant(
            "u32 scratch mapping has invalid length or alignment".into(),
        ));
    }
    // SAFETY: `MmapMut` starts at a page-aligned address, the length and u32
    // alignment were checked above, and every bit pattern is a valid u32.
    Ok(unsafe { std::slice::from_raw_parts(mmap.as_ptr().cast::<u32>(), len) })
}

fn mapped_u32_words_mut(mmap: &mut MmapMut, len: usize) -> Result<&mut [u32], PipelineError> {
    let byte_len = len
        .checked_mul(std::mem::size_of::<u32>())
        .ok_or(MemoryError::Overflow)?;
    if mmap.len() != byte_len
        || !(mmap.as_ptr() as usize).is_multiple_of(std::mem::align_of::<u32>())
    {
        return Err(PipelineError::Invariant(
            "u32 scratch mapping has invalid length or alignment".into(),
        ));
    }
    // SAFETY: this function has exclusive access to the mapping, its address
    // and length were validated above, and every bit pattern is a valid u32.
    Ok(unsafe { std::slice::from_raw_parts_mut(mmap.as_mut_ptr().cast::<u32>(), len) })
}

struct ChainContractIndex {
    offsets: RootStorage,
    contracts: RootStorage,
    local_rank: RootStorage,
}

impl ChainContractIndex {
    fn build(contract_chain: &[u32], chain_count: usize) -> Result<Self, PipelineError> {
        let mut counts = Vec::new();
        counts.try_reserve_exact(chain_count).map_err(|error| {
            PipelineError::Allocation(format!(
                "unable to allocate per-chain contract counts: {error}"
            ))
        })?;
        counts.resize(chain_count, 0u32);
        let mut local_rank = Vec::new();
        local_rank
            .try_reserve_exact(contract_chain.len())
            .map_err(|error| {
                PipelineError::Allocation(format!(
                    "unable to allocate contract chain-local ranks: {error}"
                ))
            })?;
        for &chain in contract_chain {
            let chain = chain as usize;
            let count = counts.get_mut(chain).ok_or_else(|| {
                PipelineError::Invariant("contract chain outside selected chain table".into())
            })?;
            local_rank.push(*count);
            *count = count.checked_add(1).ok_or(MemoryError::Overflow)?;
        }
        let mut offsets = Vec::<u32>::new();
        offsets
            .try_reserve_exact(chain_count.saturating_add(1))
            .map_err(|error| {
                PipelineError::Allocation(format!(
                    "unable to allocate chain contract offsets: {error}"
                ))
            })?;
        offsets.push(0);
        for count in counts {
            let next = offsets
                .last()
                .copied()
                .unwrap_or_default()
                .checked_add(count)
                .ok_or(MemoryError::Overflow)?;
            offsets.push(next);
        }
        let mut contracts = Vec::new();
        contracts
            .try_reserve_exact(contract_chain.len())
            .map_err(|error| {
                PipelineError::Allocation(format!("unable to allocate chain contract CSR: {error}"))
            })?;
        contracts.resize(contract_chain.len(), 0);
        let mut cursors = Vec::new();
        cursors.try_reserve_exact(chain_count).map_err(|error| {
            PipelineError::Allocation(format!("unable to allocate chain CSR cursors: {error}"))
        })?;
        cursors.extend_from_slice(&offsets[..chain_count]);
        for (contract, &chain) in contract_chain.iter().enumerate() {
            let cursor = cursors
                .get_mut(chain as usize)
                .ok_or_else(|| PipelineError::Invariant("invalid chain cursor".into()))?;
            let target = usize::try_from(*cursor).map_err(|_| MemoryError::Overflow)?;
            contracts[target] = u32::try_from(contract).map_err(|_| MemoryError::Overflow)?;
            *cursor = (*cursor).checked_add(1).ok_or(MemoryError::Overflow)?;
        }
        Ok(Self {
            offsets: RootStorage::resident(offsets),
            contracts: RootStorage::resident(contracts),
            local_rank: RootStorage::resident(local_rank),
        })
    }

    fn build_external(
        contract_chain: &[u32],
        chain_count: usize,
        workspace_root: &Path,
    ) -> Result<Self, PipelineError> {
        std::fs::create_dir(workspace_root)?;
        let workspace = Arc::new(TemporaryMappedWorkspace {
            root: workspace_root.to_path_buf(),
        });
        let offsets_len = chain_count.checked_add(1).ok_or(MemoryError::Overflow)?;
        let counts_raw_path = workspace_root.join("counts.raw.u32");
        let cursors_raw_path = workspace_root.join("cursors.raw.u32");
        let contracts_raw_path = workspace_root.join("contracts.raw.u32");
        let offsets_path = workspace_root.join("offsets.u32");
        let contracts_path = workspace_root.join("contracts.u32");
        let local_rank_path = workspace_root.join("local-rank.u32");

        let mut counts_map = create_zeroed_u32_mmap(&counts_raw_path, offsets_len)?
            .ok_or_else(|| PipelineError::Invariant("chain offsets map is empty".into()))?;
        {
            let counts = mapped_u32_words_mut(&mut counts_map, offsets_len)?;
            for &chain in contract_chain {
                let count = counts.get_mut(chain as usize).ok_or_else(|| {
                    PipelineError::Invariant("contract chain outside selected chain table".into())
                })?;
                *count = count.checked_add(1).ok_or(MemoryError::Overflow)?;
            }
            let mut next = 0u32;
            for count in &mut counts[..chain_count] {
                let value = *count;
                *count = next;
                next = next.checked_add(value).ok_or(MemoryError::Overflow)?;
            }
            counts[chain_count] = next;
            if next as usize != contract_chain.len() {
                return Err(PipelineError::Invariant(
                    "chain offsets do not cover every contract".into(),
                ));
            }
        }

        let mut cursors_map = create_zeroed_u32_mmap(&cursors_raw_path, chain_count)?;
        if let Some(cursors_map) = cursors_map.as_mut() {
            let offsets = mapped_u32_words(&counts_map, offsets_len)?;
            mapped_u32_words_mut(cursors_map, chain_count)?
                .copy_from_slice(&offsets[..chain_count]);
        } else if !contract_chain.is_empty() {
            return Err(PipelineError::Invariant(
                "non-empty contract table has no chain cursors".into(),
            ));
        }
        let mut contracts_map = create_zeroed_u32_mmap(&contracts_raw_path, contract_chain.len())?;
        let mut local_rank_sink = crate::format::TypedArraySink::create(
            &local_rank_path,
            crate::format::ArrayKind::U32,
            contract_chain.len() as u64,
        )?;
        if !contract_chain.is_empty() {
            let offsets = mapped_u32_words(&counts_map, offsets_len)?;
            let cursors = mapped_u32_words_mut(
                cursors_map.as_mut().ok_or_else(|| {
                    PipelineError::Invariant("missing chain cursor mapping".into())
                })?,
                chain_count,
            )?;
            let contracts = mapped_u32_words_mut(
                contracts_map.as_mut().ok_or_else(|| {
                    PipelineError::Invariant("missing contract CSR mapping".into())
                })?,
                contract_chain.len(),
            )?;
            for (contract, &chain) in contract_chain.iter().enumerate() {
                let chain = chain as usize;
                let cursor = cursors.get_mut(chain).ok_or_else(|| {
                    PipelineError::Invariant("contract chain outside selected chain table".into())
                })?;
                let target = *cursor;
                let rank = target
                    .checked_sub(offsets[chain])
                    .ok_or_else(|| PipelineError::Invariant("invalid chain cursor".into()))?;
                local_rank_sink.push_u32(rank)?;
                let target = target as usize;
                let slot = contracts.get_mut(target).ok_or_else(|| {
                    PipelineError::Invariant("chain CSR cursor outside contract table".into())
                })?;
                *slot = u32::try_from(contract).map_err(|_| MemoryError::Overflow)?;
                *cursor = cursor.checked_add(1).ok_or(MemoryError::Overflow)?;
            }
            if cursors
                .iter()
                .zip(&offsets[1..])
                .any(|(&cursor, &end)| cursor != end)
            {
                return Err(PipelineError::Invariant(
                    "chain CSR cursors did not reach their offsets".into(),
                ));
            }
        }
        local_rank_sink.finish().map_err(|error| {
            PipelineError::Invariant(format!(
                "unable to publish external chain local ranks: {error}"
            ))
        })?;

        let offsets = mapped_u32_words(&counts_map, offsets_len)?;
        let mut offsets_sink = crate::format::TypedArraySink::create(
            &offsets_path,
            crate::format::ArrayKind::U32,
            offsets_len as u64,
        )?;
        for &offset in offsets {
            offsets_sink.push_u32(offset)?;
        }
        offsets_sink.finish().map_err(|error| {
            PipelineError::Invariant(format!("unable to publish external chain offsets: {error}"))
        })?;

        let mut contracts_sink = crate::format::TypedArraySink::create(
            &contracts_path,
            crate::format::ArrayKind::U32,
            contract_chain.len() as u64,
        )?;
        if let Some(contracts_map) = contracts_map.as_ref() {
            for &contract in mapped_u32_words(contracts_map, contract_chain.len())? {
                contracts_sink.push_u32(contract)?;
            }
        }
        contracts_sink.finish().map_err(|error| {
            PipelineError::Invariant(format!("unable to publish external chain CSR: {error}"))
        })?;

        drop(contracts_map);
        drop(cursors_map);
        drop(counts_map);
        // Windows may retain a mapped-file section briefly after the last
        // view is dropped. Raw builders are best-effort unlinked here and are
        // always removed with the shared workspace after the typed maps close.
        let _ = std::fs::remove_file(contracts_raw_path);
        let _ = std::fs::remove_file(cursors_raw_path);
        let _ = std::fs::remove_file(counts_raw_path);

        let offsets = crate::format::map_u32_array(&offsets_path).map_err(|error| {
            PipelineError::Invariant(format!("unable to map external chain offsets: {error}"))
        })?;
        let contracts = crate::format::map_u32_array(&contracts_path).map_err(|error| {
            PipelineError::Invariant(format!("unable to map external chain CSR: {error}"))
        })?;
        let local_rank = crate::format::map_u32_array(&local_rank_path).map_err(|error| {
            PipelineError::Invariant(format!("unable to map external chain local ranks: {error}"))
        })?;
        if offsets.len() != offsets_len
            || contracts.len() != contract_chain.len()
            || local_rank.len() != contract_chain.len()
            || offsets.last().copied().unwrap_or_default() as usize != contract_chain.len()
        {
            return Err(PipelineError::Invariant(
                "external chain index typed arrays have inconsistent lengths".into(),
            ));
        }
        Ok(Self {
            offsets: RootStorage::mapped_in_workspace(offsets, workspace.clone()),
            contracts: RootStorage::mapped_in_workspace(contracts, workspace.clone()),
            local_rank: RootStorage::mapped_in_workspace(local_rank, workspace),
        })
    }

    fn contracts_for_chain(&self, chain: usize) -> Result<&[u32], PipelineError> {
        let begin = *self
            .offsets
            .get(chain)
            .ok_or_else(|| PipelineError::Invariant("missing chain offset".into()))?
            as usize;
        let end = *self
            .offsets
            .get(chain + 1)
            .ok_or_else(|| PipelineError::Invariant("missing chain end offset".into()))?
            as usize;
        Ok(&self.contracts[begin..end])
    }

    fn pair_node_count(&self, left: usize, right: usize) -> Result<u32, PipelineError> {
        let count = self
            .contracts_for_chain(left)?
            .len()
            .checked_add(self.contracts_for_chain(right)?.len())
            .ok_or(MemoryError::Overflow)?;
        u32::try_from(count).map_err(|_| MemoryError::Overflow.into())
    }

    fn chain_contract_count(&self, chain: usize) -> Result<u32, PipelineError> {
        let begin = *self
            .offsets
            .get(chain)
            .ok_or_else(|| PipelineError::Invariant("missing chain offset".into()))?;
        let end = *self
            .offsets
            .get(chain + 1)
            .ok_or_else(|| PipelineError::Invariant("missing chain end offset".into()))?;
        Ok(end.saturating_sub(begin))
    }

    fn pair_local_contract(
        &self,
        contract_chain: &[u32],
        contract: u32,
        left: usize,
        right: usize,
    ) -> Result<u32, PipelineError> {
        let contract = contract as usize;
        let chain = *contract_chain
            .get(contract)
            .ok_or_else(|| PipelineError::Invariant("pair edge contract out of range".into()))?
            as usize;
        let rank = *self
            .local_rank
            .get(contract)
            .ok_or_else(|| PipelineError::Invariant("missing contract local rank".into()))?;
        if chain == left {
            Ok(rank)
        } else if chain == right {
            let left_count = self.chain_contract_count(left)?;
            left_count
                .checked_add(rank)
                .ok_or(MemoryError::Overflow.into())
        } else {
            Err(PipelineError::Invariant(
                "chain-pair forest contains an endpoint from another chain".into(),
            ))
        }
    }

    fn retained_bytes(&self) -> u64 {
        self.offsets
            .resident_bytes()
            .saturating_add(self.contracts.resident_bytes())
    }

    fn total_bytes(&self) -> u64 {
        self.retained_bytes()
            .saturating_add(self.local_rank.resident_bytes())
    }

    fn release_local_rank(&mut self) {
        self.local_rank = RootStorage::default();
    }
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
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

type ScopeForestRuns = (
    Vec<ForestRunStorage>,
    Vec<ForestRunStorage>,
    Vec<Vec<ForestRunStorage>>,
);

struct TemporaryForestSpill {
    root: Option<PathBuf>,
}

impl TemporaryForestSpill {
    fn new(root: PathBuf) -> Self {
        Self { root: Some(root) }
    }

    fn cleanup(mut self) -> Result<(), std::io::Error> {
        if let Some(root) = self.root.take() {
            match std::fs::remove_dir_all(root) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
}

impl Drop for TemporaryForestSpill {
    fn drop(&mut self) {
        if let Some(root) = self.root.take() {
            let _ = std::fs::remove_dir_all(root);
        }
    }
}

enum ScopeSinkMessage {
    Edges { scope: usize, edges: Vec<Edge> },
    Stop,
}

#[derive(Debug, Clone, Copy)]
struct ScopeCollectorMemoryPlan {
    scratch_kind: EdgeCollectorScratchKind,
    active_sink_workers: usize,
    scorer_lanes: usize,
    max_buffer_bytes: u64,
    primary_scratch_touched_nodes: u64,
    worker_scratch_touched_nodes: u64,
    scratch_bytes: u64,
    edge_bytes: u64,
    buffer_pool_bytes: u64,
    retained_runtime_bytes: u64,
    merge_growth_bytes: u64,
    reserved_bytes: u64,
}

fn scope_collector_topology(chain_pair_count: usize, threads: usize) -> (usize, usize, usize) {
    let scope_count = chain_pair_count.saturating_add(2);
    let shards_per_scope = if threads >= 4 { 2 } else { 1 };
    let collector_count = scope_count.saturating_mul(shards_per_scope);
    let active_sink_workers = if threads <= 1 {
        0
    } else {
        let sink_cap = (threads / 4).max(2).min(threads.saturating_sub(1));
        collector_count.min(sink_cap)
    };
    (collector_count, active_sink_workers, shards_per_scope)
}

fn scope_collector_buffer_bytes(edge_bytes: u64, collector_count: usize) -> u64 {
    let edge_size = std::mem::size_of::<Edge>() as u64;
    let max_edge_count = edge_bytes / edge_size;
    let epoch_edge_cap = (max_edge_count / collector_count.max(1) as u64).clamp(1, 10_000_000);
    (edge_bytes / collector_count.max(1) as u64)
        .max(edge_size)
        .min(epoch_edge_cap.saturating_mul(edge_size))
}

fn touched_nodes_for_edge_bytes(node_count: u32, edge_bytes: u64) -> u64 {
    let edge_size = std::mem::size_of::<Edge>() as u64;
    u64::from(node_count).min(
        edge_bytes
            .checked_div(edge_size)
            .unwrap_or_default()
            .saturating_mul(2),
    )
}

fn scope_collector_scratch_bytes(
    kind: EdgeCollectorScratchKind,
    node_count: u32,
    scratch_workers: usize,
    primary_touched_nodes: u64,
    worker_touched_nodes: u64,
) -> Result<u64, PipelineError> {
    let workers = scratch_workers.max(1) as u64;
    let remaining_workers = workers.saturating_sub(1);
    let touched_nodes = primary_touched_nodes
        .checked_add(
            worker_touched_nodes
                .checked_mul(remaining_workers)
                .ok_or(crate::resource::MemoryError::Overflow)?,
        )
        .ok_or(crate::resource::MemoryError::Overflow)?;
    let touched_bytes = touched_nodes
        .checked_mul((2 * std::mem::size_of::<u32>()) as u64)
        .ok_or(crate::resource::MemoryError::Overflow)?;
    match kind {
        EdgeCollectorScratchKind::Dense => u64::from(node_count)
            .checked_mul((2 * std::mem::size_of::<u32>()) as u64)
            .and_then(|bytes| bytes.checked_mul(workers))
            .and_then(|bytes| bytes.checked_add(touched_bytes / 2))
            .ok_or(crate::resource::MemoryError::Overflow.into()),
        EdgeCollectorScratchKind::Sparse => Ok(touched_bytes),
    }
}

fn build_scope_collector_memory_plan(
    memory: &MemoryBroker,
    node_count: u32,
    chain_pair_count: usize,
    requested_edge_bytes: u64,
    threads: usize,
) -> Result<ScopeCollectorMemoryPlan, PipelineError> {
    let available = memory.available_bytes();
    let edge_size = std::mem::size_of::<Edge>() as u64;
    let (collector_count, requested_sink_workers, _) =
        scope_collector_topology(chain_pair_count, threads);
    let max_edge_bytes = requested_edge_bytes
        .max(edge_size)
        .min(available.max(edge_size));

    let try_plan = |kind, sink_workers: usize, edge_bytes: u64| {
        let scratch_workers = sink_workers.max(1);
        let max_buffer_bytes = scope_collector_buffer_bytes(edge_bytes, collector_count);
        let primary_touched_nodes = touched_nodes_for_edge_bytes(node_count, edge_bytes);
        let worker_touched_nodes = touched_nodes_for_edge_bytes(node_count, max_buffer_bytes);
        let scratch_bytes = scope_collector_scratch_bytes(
            kind,
            node_count,
            scratch_workers,
            primary_touched_nodes,
            worker_touched_nodes,
        )?;
        let buffer_pool_bytes = max_buffer_bytes
            .checked_mul(collector_count as u64)
            .ok_or(crate::resource::MemoryError::Overflow)?;
        let retained_runtime_bytes = edge_bytes
            .checked_add(buffer_pool_bytes)
            .ok_or(crate::resource::MemoryError::Overflow)?;
        let merge_growth_bytes = edge_bytes;
        let reserved_bytes = retained_runtime_bytes
            .checked_add(merge_growth_bytes)
            .and_then(|bytes| bytes.checked_add(scratch_bytes))
            .ok_or(crate::resource::MemoryError::Overflow)?;
        Ok::<_, PipelineError>(ScopeCollectorMemoryPlan {
            scratch_kind: kind,
            active_sink_workers: sink_workers,
            scorer_lanes: threads.saturating_sub(sink_workers).max(1),
            max_buffer_bytes,
            primary_scratch_touched_nodes: primary_touched_nodes,
            worker_scratch_touched_nodes: worker_touched_nodes,
            scratch_bytes,
            edge_bytes,
            buffer_pool_bytes,
            retained_runtime_bytes,
            merge_growth_bytes,
            reserved_bytes,
        })
    };

    for sink_workers in (0..=requested_sink_workers).rev() {
        if requested_sink_workers != 0 && sink_workers == 0 {
            continue;
        }
        let plan = try_plan(
            EdgeCollectorScratchKind::Dense,
            sink_workers,
            max_edge_bytes,
        )?;
        if plan.reserved_bytes <= available {
            return Ok(plan);
        }
    }
    for sink_workers in (0..=requested_sink_workers).rev() {
        if requested_sink_workers != 0 && sink_workers == 0 {
            continue;
        }
        let plan = try_plan(
            EdgeCollectorScratchKind::Sparse,
            sink_workers,
            max_edge_bytes,
        )?;
        if plan.reserved_bytes <= available {
            return Ok(plan);
        }
    }

    let sink_workers = requested_sink_workers;
    let mut low = edge_size;
    let mut high = max_edge_bytes;
    let mut best = None;
    while low <= high {
        let midpoint = low + (high - low) / 2;
        let aligned = (midpoint / edge_size).max(1).saturating_mul(edge_size);
        let plan = try_plan(EdgeCollectorScratchKind::Sparse, sink_workers, aligned)?;
        if plan.reserved_bytes <= available {
            best = Some(plan);
            low = aligned.saturating_add(edge_size);
        } else {
            if aligned <= edge_size {
                break;
            }
            high = aligned.saturating_sub(edge_size);
        }
    }
    if let Some(best) = best {
        return Ok(best);
    }

    // Even the minimum exact sparse collector may sit outside the accounting
    // envelope when earlier measured state already consumed the configured
    // top. Return that bounded plan and let the caller charge what fits; the
    // actual Vec allocation remains the authoritative failure point.
    try_plan(
        EdgeCollectorScratchKind::Sparse,
        requested_sink_workers,
        edge_size,
    )
}

/// Bounded scope-sharded admission for MetadataMatch forest edges.
///
/// Each logical scope is assigned to exactly one sink worker. The collectors
/// remain individually owned behind a mutex. Dense degree/DSU state belongs to
/// sink workers and is reused across scopes; the exact sparse fallback keeps
/// only endpoints touched by the current bounded batch/compaction.
struct ScopeCollectorBroker {
    collectors: Vec<Arc<std::sync::Mutex<Option<EdgeCollector>>>>,
    scratches: Vec<Arc<std::sync::Mutex<EdgeCollectorScratch>>>,
    senders: Vec<std::sync::mpsc::SyncSender<ScopeSinkMessage>>,
    handles: Vec<std::thread::JoinHandle<()>>,
    accepted_edges: Arc<std::sync::atomic::AtomicU64>,
    cancelled: Arc<std::sync::atomic::AtomicBool>,
    first_error: Arc<std::sync::Mutex<Option<crate::reduce::ReduceError>>>,
    retained: Arc<ScopeRetainedBudget>,
    spills: Arc<Vec<std::sync::Mutex<CollectorSpillState>>>,
    spilled_bytes: Arc<std::sync::atomic::AtomicU64>,
    resident_forest_limit: u64,
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

#[derive(Clone)]
struct SpilledForestRun {
    directory: PathBuf,
    run_id: u32,
}

struct CollectorSpillState {
    directory: PathBuf,
    next_run_id: u32,
    runs: Vec<SpilledForestRun>,
}

impl ScopeCollectorBroker {
    fn new(
        node_count: u32,
        chain_pair_count: usize,
        budget: EdgeBudget,
        max_retained_bytes: u64,
        plan: ScopeCollectorMemoryPlan,
        spill_root: &Path,
    ) -> Result<Self, PipelineError> {
        let scope_count = chain_pair_count.saturating_add(2);
        let threads = plan.scorer_lanes.saturating_add(plan.active_sink_workers);
        let (_, _, shards_per_scope) = scope_collector_topology(chain_pair_count, threads);
        let collector_count = scope_count.saturating_mul(shards_per_scope);
        let active_sink_workers = plan.active_sink_workers;
        let scorer_lanes = plan.scorer_lanes;
        let budget = EdgeBudget {
            max_buffer_bytes: budget.max_buffer_bytes.min(plan.max_buffer_bytes),
            ..budget
        };
        let collectors = (0..collector_count)
            .map(|_| -> Result<_, PipelineError> {
                Ok(Arc::new(std::sync::Mutex::new(Some(
                    EdgeCollector::new_serial_shared(node_count, budget, 1_048_576)?,
                ))))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let scratch_count = active_sink_workers.max(1);
        let mut scratches = Vec::with_capacity(scratch_count);
        for worker in 0..scratch_count {
            let touched_nodes = if worker == 0 {
                plan.primary_scratch_touched_nodes
            } else {
                plan.worker_scratch_touched_nodes
            };
            scratches.push(Arc::new(std::sync::Mutex::new(
                EdgeCollectorScratch::try_new(plan.scratch_kind, node_count, touched_nodes)?,
            )));
        }
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
        let spills = Arc::new(
            (0..collector_count)
                .map(|collector| {
                    std::sync::Mutex::new(CollectorSpillState {
                        directory: spill_root.join(format!("collector-{collector:06}")),
                        next_run_id: 0,
                        runs: Vec::new(),
                    })
                })
                .collect::<Vec<_>>(),
        );
        let spilled_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let mut senders = Vec::with_capacity(active_sink_workers);
        let mut handles = Vec::with_capacity(active_sink_workers);
        for worker in 0..active_sink_workers {
            let (sender, receiver) = std::sync::mpsc::sync_channel::<ScopeSinkMessage>(2);
            senders.push(sender);
            let worker_collectors = collectors.clone();
            let worker_scratch = scratches[worker].clone();
            let compaction_scratch = scratches[0].clone();
            let worker_cancelled = cancelled.clone();
            let worker_error = first_error.clone();
            let worker_retained = retained.clone();
            let worker_spills = spills.clone();
            let worker_spilled_bytes = spilled_bytes.clone();
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
                                        let mut scratch = worker_scratch.lock().map_err(|_| {
                                            crate::reduce::ReduceError::WorkOverflow
                                        })?;
                                        collector.push_batch_shared(edges, &mut scratch)?;
                                        drop(scratch);
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
                                            &compaction_scratch,
                                            &worker_spills,
                                            &worker_spilled_bytes,
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
            scratches,
            senders,
            handles,
            accepted_edges,
            cancelled,
            first_error,
            retained,
            spills,
            spilled_bytes,
            resident_forest_limit: plan.edge_bytes,
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
            let primary_scratch = self
                .scratches
                .first()
                .ok_or_else(|| PipelineError::Invariant("scope scratch missing".into()))?;
            let mut scratch = primary_scratch
                .lock()
                .map_err(|_| PipelineError::Parallel("scope scratch lock poisoned".into()))?;
            collector.push_batch_shared(edges, &mut scratch)?;
            drop(scratch);
            drop(guard);
            let over_budget =
                record_broker_retained_bytes(&self.collectors, collector_slot, &self.retained)?;
            drop(_admission);
            if over_budget {
                compact_broker_retained_budget(
                    &self.collectors,
                    &self.retained,
                    primary_scratch,
                    &self.spills,
                    &self.spilled_bytes,
                )?;
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
        for (index, collector) in self.collectors.drain(..).enumerate() {
            let collector = Arc::try_unwrap(collector)
                .map_err(|_| PipelineError::Parallel("scope collector still shared".into()))?
                .into_inner()
                .map_err(|_| PipelineError::Parallel("scope collector lock poisoned".into()))?
                .ok_or_else(|| PipelineError::Invariant("collector already finished".into()))?;
            let scratch = &self.scratches[index % self.scratches.len()];
            let mut scratch = scratch
                .lock()
                .map_err(|_| PipelineError::Parallel("scope scratch lock poisoned".into()))?;
            runs.push(collector.finish_shared(&mut scratch)?);
        }
        finalize_scope_runs(
            runs,
            self.logical_scope_count,
            self.shards_per_scope,
            self.resident_forest_limit,
            &self.spills,
            &self.spilled_bytes,
        )
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
        let scratches = self.scratches.clone();
        let finished = worker_pool.install(|| {
            collectors
                .into_par_iter()
                .enumerate()
                .map(
                    |(index, collector)| -> Result<Vec<ForestRun>, PipelineError> {
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
                        let scratch = &scratches[index % scratches.len()];
                        let mut scratch = scratch.lock().map_err(|_| {
                            PipelineError::Parallel("scope scratch lock poisoned".into())
                        })?;
                        collector
                            .finish_shared(&mut scratch)
                            .map_err(PipelineError::from)
                    },
                )
                .collect::<Vec<_>>()
        });
        finalize_scope_runs(
            finished.into_iter().collect::<Result<Vec<_>, _>>()?,
            self.logical_scope_count,
            self.shards_per_scope,
            self.resident_forest_limit,
            &self.spills,
            &self.spilled_bytes,
        )
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
    physical_runs: Vec<Vec<ForestRunStorage>>,
    logical_scope_count: usize,
    shards_per_scope: usize,
) -> ScopeForestRuns {
    let mut logical_runs = (0..logical_scope_count)
        .map(|_| Vec::new())
        .collect::<Vec<Vec<ForestRunStorage>>>();
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

fn finalize_scope_runs(
    mut physical_runs: Vec<Vec<ForestRun>>,
    logical_scope_count: usize,
    shards_per_scope: usize,
    resident_forest_limit: u64,
    spills: &[std::sync::Mutex<CollectorSpillState>],
    spilled_bytes: &std::sync::atomic::AtomicU64,
) -> Result<ScopeForestRuns, PipelineError> {
    let resident_bytes = physical_runs
        .iter()
        .map(|runs| resident_run_edge_capacity_bytes(runs))
        .fold(0u64, u64::saturating_add);
    if resident_bytes > resident_forest_limit {
        for (collector, runs) in physical_runs.iter_mut().enumerate() {
            spill_collector_runs(
                spills.get(collector).ok_or_else(|| {
                    PipelineError::Invariant("missing collector spill state".into())
                })?,
                std::mem::take(runs),
                spilled_bytes,
            )?;
        }
    }
    let mut stored = Vec::with_capacity(physical_runs.len());
    for (collector, runs) in physical_runs.into_iter().enumerate() {
        let spill_runs = spills
            .get(collector)
            .ok_or_else(|| PipelineError::Invariant("missing collector spill state".into()))?
            .lock()
            .map_err(|_| PipelineError::Parallel("collector spill lock poisoned".into()))?
            .runs
            .clone();
        let mut scope_runs = Vec::with_capacity(spill_runs.len().saturating_add(runs.len()));
        for spilled in spill_runs {
            scope_runs.push(ForestRunStorage::open_mapped(
                &spilled.directory,
                spilled.run_id,
            )?);
        }
        scope_runs.extend(runs.into_iter().map(ForestRunStorage::resident));
        stored.push(scope_runs);
    }
    Ok(collapse_scope_runs(
        stored,
        logical_scope_count,
        shards_per_scope,
    ))
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
    scratch: &Arc<std::sync::Mutex<EdgeCollectorScratch>>,
    spills: &[std::sync::Mutex<CollectorSpillState>],
    spilled_bytes: &std::sync::atomic::AtomicU64,
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
    let mut scratch = scratch
        .lock()
        .map_err(|_| crate::reduce::ReduceError::WorkOverflow)?;
    let mut compacted_total = 0u64;
    for (scope, collector) in collectors.iter().enumerate() {
        let mut guard = collector
            .lock()
            .map_err(|_| crate::reduce::ReduceError::WorkOverflow)?;
        guard
            .as_mut()
            .ok_or(crate::reduce::ReduceError::WorkOverflow)?
            .compact_retained_shared(&mut scratch)?;
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
        compacted_total = 0;
        for (scope, collector) in collectors.iter().enumerate() {
            let mut guard = collector
                .lock()
                .map_err(|_| crate::reduce::ReduceError::WorkOverflow)?;
            let collector = guard
                .as_mut()
                .ok_or(crate::reduce::ReduceError::WorkOverflow)?;
            let runs = collector.drain_compacted_runs();
            spill_collector_runs(
                spills
                    .get(scope)
                    .ok_or(crate::reduce::ReduceError::WorkOverflow)?,
                runs,
                spilled_bytes,
            )?;
            let retained = collector.retained_bytes();
            retained_budget.by_scope[scope].store(retained, std::sync::atomic::Ordering::Release);
            compacted_total = compacted_total
                .checked_add(retained)
                .ok_or(crate::reduce::ReduceError::WorkOverflow)?;
        }
        retained_budget
            .total
            .store(compacted_total, std::sync::atomic::Ordering::Release);
    }
    Ok(())
}

fn spill_collector_runs(
    state: &std::sync::Mutex<CollectorSpillState>,
    runs: Vec<ForestRun>,
    spilled_bytes: &std::sync::atomic::AtomicU64,
) -> Result<(), crate::reduce::ReduceError> {
    if runs.is_empty() {
        return Ok(());
    }
    let mut state = state
        .lock()
        .map_err(|_| crate::reduce::ReduceError::WorkOverflow)?;
    for run in runs {
        let run_id = state.next_run_id;
        state.next_run_id = state
            .next_run_id
            .checked_add(1)
            .ok_or(crate::reduce::ReduceError::WorkOverflow)?;
        run.commit(&state.directory, run_id)?;
        let bytes = (run.edges.len() as u64)
            .checked_mul(std::mem::size_of::<Edge>() as u64)
            .ok_or(crate::reduce::ReduceError::WorkOverflow)?;
        spilled_bytes.fetch_add(bytes, std::sync::atomic::Ordering::AcqRel);
        let directory = state.directory.clone();
        state.runs.push(SpilledForestRun { directory, run_id });
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
    #[error("allocation failed: {0}")]
    Allocation(String),
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

struct SnapshotResidency {
    lease: MemoryLease,
    mapped_bytes: u64,
    last_reported_hot_bytes: u64,
}

impl SnapshotResidency {
    fn new(lease: MemoryLease, mapped_bytes: u64) -> Self {
        Self {
            last_reported_hot_bytes: mapped_bytes,
            lease,
            mapped_bytes,
        }
    }

    fn report_paging(
        &mut self,
        phase: &str,
        memory: &MemoryBroker,
        advisory: &mut impl FnMut(&str),
    ) {
        let hot_bytes = self.lease.bytes();
        if hot_bytes >= self.last_reported_hot_bytes {
            return;
        }
        advisory(&format!(
            "metadata snapshot resident hot window reduced during {phase}: mapped={} bytes, \
             resident_budget={} bytes, newly_paged={} bytes, cumulatively_reclaimed={} bytes, \
             used={} bytes, hard_top={} bytes; continuing with demand-paged mmap access instead \
             of terminating",
            self.mapped_bytes,
            hot_bytes,
            self.last_reported_hot_bytes.saturating_sub(hot_bytes),
            memory.reclaimed_bytes(),
            memory.used_bytes(),
            memory.hard_top_bytes()
        ));
        self.last_reported_hot_bytes = hot_bytes;
    }
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

    let channel_capacity = worker_pool.current_num_threads().max(1).saturating_mul(2);
    let (sender, receiver) = std::sync::mpsc::sync_channel::<u64>(channel_capacity);
    std::thread::scope(|thread_scope| -> Result<u64, PipelineError> {
        let producer = thread_scope.spawn(move || {
            worker_pool.install(|| {
                (0..atom_count)
                    .into_par_iter()
                    .try_for_each(|atom| -> Result<(), PipelineError> {
                        let (work, edges) = fallback_atom_forest(snapshot, atom as u32)?;
                        collectors.push_edges_by_chain(
                            &snapshot.features().contract_chain,
                            chain_count,
                            edges,
                        )?;
                        sender.send(work).map_err(|_| {
                            PipelineError::Parallel(
                                "fallback forest progress channel disconnected".into(),
                            )
                        })
                    })
            })
        });
        let mut completed = 0u64;
        for work in receiver {
            completed = completed
                .checked_add(work)
                .ok_or(crate::resource::MemoryError::Overflow)?;
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
        producer
            .join()
            .map_err(|_| PipelineError::Parallel("fallback forest worker panicked".into()))??;
        Ok(completed)
    })
}

fn fallback_atom_forest(
    snapshot: &MetadataSnapshot,
    atom: u32,
) -> Result<(u64, Vec<Edge>), PipelineError> {
    let members = atom_contracts(snapshot, atom);
    if members.len() < 2 {
        return Ok((0, Vec::new()));
    }
    let features = snapshot.features();
    if let Some((root_index, &root)) = members
        .iter()
        .enumerate()
        .find(|(_, contract)| contract_retained_tokens(features, **contract).is_empty())
    {
        let edges = members
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != root_index)
            .map(|(_, &contract)| Edge::new(root, contract))
            .collect::<Vec<_>>();
        return Ok((members.len().saturating_sub(1) as u64, edges));
    }
    if atom_members_share_common_retained_token(features, members) {
        return Ok((0, Vec::new()));
    }
    const PARALLEL_FALLBACK_ATOM_MEMBERS: usize = 2_048;
    if members.len() >= PARALLEL_FALLBACK_ATOM_MEMBERS {
        return fallback_atom_forest_parallel(features, members);
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

struct FallbackAtomicDsu {
    parent: Vec<std::sync::atomic::AtomicU32>,
}

impl FallbackAtomicDsu {
    fn new(len: usize) -> Self {
        Self {
            parent: (0..len as u32)
                .into_par_iter()
                .map(std::sync::atomic::AtomicU32::new)
                .collect(),
        }
    }

    fn find(&self, mut node: u32) -> u32 {
        loop {
            let parent = self.parent[node as usize].load(std::sync::atomic::Ordering::Acquire);
            if parent == node {
                return node;
            }
            let grandparent =
                self.parent[parent as usize].load(std::sync::atomic::Ordering::Acquire);
            let _ = self.parent[node as usize].compare_exchange(
                parent,
                grandparent,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            );
            node = grandparent;
        }
    }

    fn union(&self, left: u32, right: u32) -> bool {
        loop {
            let left_root = self.find(left);
            let right_root = self.find(right);
            if left_root == right_root {
                return false;
            }
            let (low, high) = (left_root.min(right_root), left_root.max(right_root));
            if self.parent[high as usize]
                .compare_exchange(
                    high,
                    low,
                    std::sync::atomic::Ordering::AcqRel,
                    std::sync::atomic::Ordering::Acquire,
                )
                .is_ok()
            {
                return true;
            }
        }
    }
}

fn fallback_atom_forest_parallel(
    features: &crate::encode::FeatureView,
    members: &[u32],
) -> Result<(u64, Vec<Edge>), PipelineError> {
    let dsu = FallbackAtomicDsu::new(members.len());
    let components = std::sync::atomic::AtomicUsize::new(members.len());
    let connected = std::sync::atomic::AtomicBool::new(false);
    let (visits, mut edges) = (0..members.len())
        .into_par_iter()
        .fold(
            || (0u64, Vec::<Edge>::new()),
            |(mut visits, mut edges), left_index| {
                if connected.load(std::sync::atomic::Ordering::Acquire) {
                    return (visits, edges);
                }
                for right_index in left_index + 1..members.len() {
                    if connected.load(std::sync::atomic::Ordering::Acquire) {
                        break;
                    }
                    visits = visits.saturating_add(1);
                    if contracts_share_retained_token(
                        features,
                        members[left_index],
                        members[right_index],
                    ) {
                        continue;
                    }
                    if dsu.union(left_index as u32, right_index as u32) {
                        edges.push(Edge::new(members[left_index], members[right_index]));
                        let previous = components.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
                        if previous == 2 {
                            connected.store(true, std::sync::atomic::Ordering::Release);
                            break;
                        }
                    }
                }
                (visits, edges)
            },
        )
        .reduce(
            || (0u64, Vec::<Edge>::new()),
            |(left_visits, mut left_edges), (right_visits, mut right_edges)| {
                left_edges.append(&mut right_edges);
                (left_visits.saturating_add(right_visits), left_edges)
            },
        );
    edges.par_sort_unstable_by_key(|edge| (edge.left, edge.right));
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
    edge_batch: usize,
    dense_compaction_scratch_bytes: usize,
    bounded_mode: bool,
    hot_index_budget: usize,
}

struct CatalogParallelLaneState {
    batch: Vec<Edge>,
    pending_expansion: u64,
    compaction_scratch: ScopeCompactionScratch,
    expansion_scratch: Option<CatalogExpansionScratch>,
    bounded_expansion_scratch: Option<CatalogBoundedExpansionScratch>,
}

impl CatalogParallelLaneState {
    fn new(
        contract_count: usize,
        chain_count: usize,
        edge_batch: usize,
        dense_scratch_bytes: usize,
        bounded_mode: bool,
    ) -> Self {
        Self {
            batch: Vec::with_capacity(edge_batch),
            pending_expansion: 0,
            compaction_scratch: ScopeCompactionScratch::new(contract_count, dense_scratch_bytes),
            expansion_scratch: (!bounded_mode).then(|| CatalogExpansionScratch::new(chain_count)),
            bounded_expansion_scratch: bounded_mode
                .then(|| CatalogBoundedExpansionScratch::new(chain_count)),
        }
    }
}

struct CatalogExpansionScratch {
    left_by_chain: Vec<Vec<u32>>,
    right_by_chain: Vec<Vec<u32>>,
    retained_tokens: HashSet<u32>,
    retained_token_sort: Vec<u32>,
}

const MAX_CATALOG_TOKEN_SET_ENTRIES: usize = 131_072;
const BOUNDED_CATALOG_TOKEN_CHUNK_ENTRIES: usize = 16_384;

impl CatalogExpansionScratch {
    fn new(chain_count: usize) -> Self {
        Self {
            left_by_chain: (0..chain_count).map(|_| Vec::new()).collect(),
            right_by_chain: (0..chain_count).map(|_| Vec::new()).collect(),
            retained_tokens: HashSet::new(),
            retained_token_sort: Vec::new(),
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
        retained_token_sort: &mut Vec<u32>,
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
        if indexed_memberships <= MAX_CATALOG_TOKEN_SET_ENTRIES {
            retained_tokens.clear();
            retained_tokens.reserve(indexed_memberships);
            for &contract in indexed {
                retained_tokens.extend(contract_retained_tokens(features, contract));
            }
            return !scanned.iter().any(|&contract| {
                contract_retained_tokens(features, contract)
                    .iter()
                    .any(|token| retained_tokens.contains(token))
            });
        }
        retained_token_sort.clear();
        if retained_token_sort
            .try_reserve(indexed_memberships)
            .is_err()
        {
            return false;
        }
        for &contract in indexed {
            retained_token_sort.extend_from_slice(contract_retained_tokens(features, contract));
        }
        retained_token_sort.sort_unstable();
        retained_token_sort.dedup();
        !scanned.iter().any(|&contract| {
            contract_retained_tokens(features, contract)
                .iter()
                .any(|token| retained_token_sort.binary_search(token).is_ok())
        })
    }
}

struct CatalogBoundedExpansionScratch {
    token_chunk: Vec<u32>,
    left_chain_roots: Vec<Option<u32>>,
    right_chain_roots: Vec<Option<u32>>,
}

impl CatalogBoundedExpansionScratch {
    fn new(chain_count: usize) -> Self {
        Self {
            token_chunk: Vec::with_capacity(BOUNDED_CATALOG_TOKEN_CHUNK_ENTRIES),
            left_chain_roots: vec![None; chain_count],
            right_chain_roots: vec![None; chain_count],
        }
    }

    fn retained_tokens_disjoint(
        &mut self,
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
        let (indexed, scanned) = if left_memberships <= right_memberships {
            (left, right)
        } else {
            (right, left)
        };
        let mut indexed_tokens = indexed
            .iter()
            .flat_map(|&contract| contract_retained_tokens(features, contract).iter().copied());
        loop {
            self.token_chunk.clear();
            self.token_chunk.extend(
                indexed_tokens
                    .by_ref()
                    .take(BOUNDED_CATALOG_TOKEN_CHUNK_ENTRIES),
            );
            if self.token_chunk.is_empty() {
                return true;
            }
            self.token_chunk.sort_unstable();
            self.token_chunk.dedup();
            if scanned.iter().any(|&contract| {
                contract_retained_tokens(features, contract)
                    .iter()
                    .any(|token| self.token_chunk.binary_search(token).is_ok())
            }) {
                return false;
            }
        }
    }

    fn prepare_chain_roots(
        &mut self,
        features: &crate::encode::FeatureView,
        left: &[u32],
        right: &[u32],
    ) {
        self.left_chain_roots.fill(None);
        self.right_chain_roots.fill(None);
        for &contract in left {
            let chain = features.contract_chain[contract as usize] as usize;
            self.left_chain_roots[chain].get_or_insert(contract);
        }
        for &contract in right {
            let chain = features.contract_chain[contract as usize] as usize;
            self.right_chain_roots[chain].get_or_insert(contract);
        }
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
    let retained_token_hash = (MAX_CATALOG_TOKEN_SET_ENTRIES as u64)
        .checked_mul(32)
        .ok_or(crate::resource::MemoryError::Overflow)?;
    let features = snapshot.features();
    let max_atom_token_memberships = features.fallback_atom_offsets.windows(2).try_fold(
        0u64,
        |maximum, window| -> Result<u64, PipelineError> {
            let begin = usize::try_from(window[0]).map_err(|_| MemoryError::Overflow)?;
            let end = usize::try_from(window[1]).map_err(|_| MemoryError::Overflow)?;
            let memberships = features.fallback_atom_contracts[begin..end]
                .iter()
                .try_fold(0u64, |total, &contract| {
                    let contract = contract as usize;
                    total
                        .checked_add(
                            features.contract_token_offsets[contract + 1]
                                .saturating_sub(features.contract_token_offsets[contract]),
                        )
                        .ok_or(MemoryError::Overflow)
                })?;
            Ok(maximum.max(memberships))
        },
    )?;
    // The large-set path stores packed u32 token ids instead of falling off a
    // fixed HashSet cliff into a contract Cartesian product. Allow 2x capacity
    // slack while Vec grows.
    let retained_token_sort = max_atom_token_memberships
        .checked_mul(std::mem::size_of::<u32>() as u64)
        .and_then(|bytes| bytes.checked_mul(2))
        .ok_or(MemoryError::Overflow)?;
    let retained_token_index = retained_token_hash.max(retained_token_sort);
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
    let CatalogExecutionConfig {
        lanes,
        chain_count,
        edge_batch,
        dense_compaction_scratch_bytes,
        bounded_mode,
        hot_index_budget,
    } = execution;
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
    let index = if bounded_mode {
        ConservativeIndex::open_bounded(snapshot, hot_index_budget)
    } else {
        ConservativeIndex::open(snapshot)
    };
    let (sender, receiver) = std::sync::mpsc::sync_channel::<CatalogMessage>(lanes.max(1) * 2);
    let cancelled = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(|scope| -> Result<(IndexMetrics, u64), PipelineError> {
        let producer_sender = sender.clone();
        let producer_cancelled = &cancelled;
        let producer = scope.spawn(move || {
            pool.install(|| {
                plan.ordered_job_ids.par_iter().for_each_init(
                    || {
                        CatalogParallelLaneState::new(
                            snapshot.features().contract_chain.len(),
                            chain_count,
                            edge_batch,
                            dense_compaction_scratch_bytes,
                            bounded_mode,
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
                        const PARALLEL_MICRO_JOB_WORK: u64 = 1_000_000;
                        let proof_indexed = job.shape == JobShape::LeftTileFanout;
                        if !bounded_mode
                            && (proof_indexed || job.estimated_work >= PARALLEL_MICRO_JOB_WORK)
                        {
                            let metrics = index
                                .for_each_job_candidate_parallel_stateful_with_work_while(
                                    &job,
                                    || {
                                        CatalogParallelLaneState::new(
                                            snapshot.features().contract_chain.len(),
                                            chain_count,
                                            edge_batch,
                                            dense_compaction_scratch_bytes,
                                            bounded_mode,
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
                                            let expansion_scratch =
                                                state.expansion_scratch.as_mut().expect(
                                                    "resident catalog lane has expansion scratch",
                                                );
                                            expand_catalog_atom_pair_streaming(
                                                snapshot,
                                                a,
                                                b,
                                                proof_indexed,
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
                                        if state.batch.len() >= edge_batch {
                                            if let Err(error) = submit_catalog_lane_batch(
                                                state,
                                                snapshot,
                                                chain_count,
                                                collectors,
                                                edge_batch,
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
                                            edge_batch,
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
                                    &|| {
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
                            &job,
                            &mut |a, b| {
                                if send_failed.load(std::sync::atomic::Ordering::Acquire)
                                    || producer_cancelled.load(std::sync::atomic::Ordering::Acquire)
                                {
                                    return;
                                }
                                let expansion_result = if bounded_mode {
                                    expand_catalog_atom_pair_bounded(
                                        snapshot,
                                        a,
                                        b,
                                        proof_indexed,
                                        lane_state,
                                        collectors,
                                        chain_count,
                                        edge_batch,
                                    )
                                } else {
                                    let batch = &mut lane_state.batch;
                                    let expansion_scratch = lane_state
                                        .expansion_scratch
                                        .as_mut()
                                        .expect("resident catalog lane has expansion scratch");
                                    expand_catalog_atom_pair_streaming(
                                        snapshot,
                                        a,
                                        b,
                                        proof_indexed,
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
                                if lane_state.batch.len() >= edge_batch {
                                    if let Err(error) = submit_catalog_lane_batch(
                                        lane_state,
                                        snapshot,
                                        chain_count,
                                        collectors,
                                        edge_batch,
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
                                edge_batch,
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

fn resize_measured_memory_advisory(
    lease: &mut MemoryLease,
    measured_bytes: u64,
    label: &str,
    advisory: &mut impl FnMut(&str),
) -> Result<(), PipelineError> {
    match lease.resize(measured_bytes) {
        Ok(()) => Ok(()),
        Err(error @ MemoryError::Budget { .. }) => {
            advisory(&format!(
                "measured {label} require {measured_bytes} bytes beyond the accounting envelope \
                 ({error}); continuing because the state is already allocated and letting actual \
                 allocation or I/O report any real failure"
            ));
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
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
    let snapshot_validation_scratch_bytes =
        MetadataSnapshot::validation_scratch_bytes(features, blocking)?;
    let memory = MemoryBroker::new(config.host_total_memory, config.memory_hard_top)?;
    // Invariant validation uses ordinary heap scratch while the immutable
    // feature arrays are demand-paged mmaps. Admit validation first, then use
    // the remainder as a reclaimable resident hot window. Mandatory Match
    // work may shrink that window, but never invalidates the mappings.
    let validation_charge = snapshot_validation_scratch_bytes.min(memory.unreserved_bytes());
    let snapshot_validation_memory = memory.reserve(validation_charge)?;
    if validation_charge < snapshot_validation_scratch_bytes {
        advisory(&format!(
            "metadata snapshot validation scratch estimate exceeds the configured engine top: \
             estimated={} bytes, charged={} bytes, hard_top={} bytes; continuing the single \
             linear validation pass from required host headroom instead of terminating",
            snapshot_validation_scratch_bytes,
            validation_charge,
            memory.hard_top_bytes()
        ));
    }
    let initial_snapshot_hot_bytes = snapshot_verification_bytes.min(memory.unreserved_bytes());
    let mut snapshot_memory = memory.reserve_reclaimable(initial_snapshot_hot_bytes)?;
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
    drop(snapshot_validation_memory);
    let restored_snapshot_hot_bytes = snapshot_verification_bytes.min(
        snapshot_memory
            .bytes()
            .saturating_add(memory.unreserved_bytes()),
    );
    snapshot_memory.resize(restored_snapshot_hot_bytes)?;
    let mut snapshot_residency =
        SnapshotResidency::new(snapshot_memory, snapshot_verification_bytes);
    snapshot_residency.report_paging("snapshot open", &memory, &mut advisory);
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
    let component_root_scope_factor = chain_count.saturating_add(1).max(2) as u64;
    let component_bytes = (snapshot.contract_count() as u64)
        .checked_mul(std::mem::size_of::<u32>() as u64)
        .and_then(|bytes| bytes.checked_mul(component_root_scope_factor))
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
        .checked_mul(component_root_scope_factor)
        .and_then(|edges| edges.checked_mul(std::mem::size_of::<Edge>() as u64))
        .ok_or(crate::resource::MemoryError::Overflow)?;
    let edge_bytes = config.edge_bytes.min(forest_upper_bytes.max(64 * 1024));
    let catalog_budget = UniverseBudget {
        max_jobs: config.max_catalog_jobs.min(u32::MAX as u64),
        max_catalog_bytes: u64::MAX,
        cold_members_per_job: 262_144,
    };
    let catalog_job_count = WorkCatalog::descriptor_count(
        &snapshot,
        catalog_budget,
        crate::blocking::DEFAULT_MAX_ROUTING_BLOCK_MEMBERS as u64,
    )?;
    let catalog_bytes = catalog_job_count
        .checked_mul(24)
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
    let catalog_hot_bytes = catalog_bytes.min(memory.unreserved_bytes());
    let _catalog_memory = memory.reserve_reclaimable(catalog_hot_bytes)?;
    if catalog_hot_bytes < catalog_bytes {
        advisory(&format!(
            "catalog descriptors require {} bytes for {} jobs, but only {} bytes fit in the \
             resident hot window; descriptors are generated in two linear passes, stored as \
             checksummed fixed-width columns, and demand-paged by mmap instead of enforcing a \
             fixed resident job cap",
            catalog_bytes, catalog_job_count, catalog_hot_bytes
        ));
    }
    snapshot_residency.report_paging("catalog admission", &memory, &mut advisory);
    let catalog_blocks = snapshot.blocking().block_kinds.len() as u64;
    let index_dir = out.join("index-1");
    let catalog_dir = if persistence == MatchPersistence::Durable {
        index_dir.join("catalog")
    } else {
        out.join("catalog-descriptor-scratch")
    };
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
        WorkCatalog::build_external_with_progress(
            &catalog_dir,
            &snapshot,
            catalog_budget,
            crate::blocking::DEFAULT_MAX_ROUTING_BLOCK_MEMBERS as u64,
            true,
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
    let desired_evidence_partition_bytes = (evidence_resident_target / 3)
        .max(evidence_partition_floor)
        .min(
            config
                .exact_pair_work
                .saturating_mul(16)
                .max(evidence_partition_floor),
        );
    let evidence_resident_bytes = desired_evidence_partition_bytes.saturating_mul(3);
    // Three retained evidence partitions coexist with per-worker vectors and
    // shared-token routing scratch.  Reserve the conservative peak for their
    // full lifetime so later lane admission sees the real resident pressure.
    let evidence_peak_bytes = evidence_resident_bytes
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(desired_evidence_partition_bytes.saturating_mul(2)))
        .ok_or(crate::resource::MemoryError::Overflow)?;
    let evidence_memory = memory.reserve_up_to(evidence_peak_bytes)?;
    let evidence_partition_bytes = evidence_memory
        .bytes()
        .checked_div(8)
        .unwrap_or_default()
        .min(desired_evidence_partition_bytes);
    if evidence_memory.bytes() < evidence_peak_bytes {
        advisory(&format!(
            "exact-evidence resident peak reduced from {} to {} bytes because only {} bytes were \
             available; each partition keeps up to {} miss bytes resident, then spills sorted \
             checksummed runs and mmaps the merged result. Oversized shared-token routing groups \
             use conservative exact scoring without a group-local index instead of terminating",
            evidence_peak_bytes,
            evidence_memory.bytes(),
            memory
                .available_bytes()
                .saturating_add(evidence_memory.bytes()),
            evidence_partition_bytes
        ));
    }
    snapshot_residency.report_paging("exact-evidence admission", &memory, &mut advisory);
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
    for (label, miss_bytes) in [
        (
            "pair calibration",
            (exact.conservative_misses.len() as u64)
                .saturating_mul(std::mem::size_of::<crate::exact_islands::ExactMiss>() as u64),
        ),
        (
            "pair holdout",
            (pair_holdout_evidence.conservative_misses.len() as u64)
                .saturating_mul(std::mem::size_of::<crate::exact_islands::ExactMiss>() as u64),
        ),
        (
            "shared-token",
            (shared_token_exact_evidence
                .calibration_misses
                .len()
                .saturating_add(shared_token_exact_evidence.holdout_misses.len())
                as u64)
                .saturating_mul(
                    std::mem::size_of::<crate::exact_islands::SharedTokenExactMiss>() as u64,
                ),
        ),
    ] {
        if miss_bytes > evidence_partition_bytes {
            advisory(&format!(
                "{label} ExactEvidence misses require {miss_bytes} bytes, above the \
                 {evidence_partition_bytes}-byte resident target; sorted checksummed miss runs \
                 were externally merged and reopened by mmap instead of terminating"
            ));
        }
    }
    if !shared_token_exact_evidence
        .scratch_fallback_tokens
        .is_empty()
    {
        advisory(&format!(
            "{} shared-token ExactEvidence groups exceeded the admitted per-group routing \
             scratch; exact pair scoring continued without materializing those group indexes, \
             and exact matches were conservatively retained as rescue misses",
            shared_token_exact_evidence.scratch_fallback_tokens.len()
        ));
    }
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
    let node_count = snapshot.contract_count() as u32;
    let requested_edge_bytes = edge_bytes;
    let scope_collector_plan = build_scope_collector_memory_plan(
        &memory,
        node_count,
        chain_pair_count,
        requested_edge_bytes,
        config.threads,
    )?;
    let edge_bytes = scope_collector_plan.edge_bytes;
    let mut edge_memory = memory.reserve_up_to(scope_collector_plan.reserved_bytes)?;
    if edge_memory.bytes() < scope_collector_plan.reserved_bytes {
        advisory(&format!(
            "minimum exact edge collector requires {} bytes while {} bytes fit the accounting \
             envelope; continuing with the bounded sparse collector and letting actual \
             allocation or I/O report any real failure",
            scope_collector_plan.reserved_bytes,
            edge_memory.bytes()
        ));
    }
    snapshot_residency.report_paging("edge-buffer admission", &memory, &mut advisory);
    let requested_sink_workers = scope_collector_topology(chain_pair_count, config.threads).1;
    if scope_collector_plan.scratch_kind == EdgeCollectorScratchKind::Sparse
        || edge_bytes < requested_edge_bytes
        || scope_collector_plan.active_sink_workers < requested_sink_workers
    {
        advisory(&format!(
            "metadata scope collectors selected {:?} shared scratch: requested_edge_bytes={}, \
             effective_edge_bytes={}, requested_sink_workers={}, sink_workers={}, \
             scorer_lanes={}, max_buffer_bytes={}, buffer_pool_bytes={}, scratch_bytes={}, \
             merge_growth_bytes={}, \
             total_reserved_bytes={}; dense scratch is \
             used only when its full resident peak fits, otherwise exact touched-only sparse \
             reduction continues instead of terminating",
            scope_collector_plan.scratch_kind,
            requested_edge_bytes,
            edge_bytes,
            requested_sink_workers,
            scope_collector_plan.active_sink_workers,
            scope_collector_plan.scorer_lanes,
            scope_collector_plan.max_buffer_bytes,
            scope_collector_plan.buffer_pool_bytes,
            scope_collector_plan.scratch_bytes,
            scope_collector_plan.merge_growth_bytes,
            scope_collector_plan.reserved_bytes
        ));
    }
    let max_edge_count = edge_bytes / (std::mem::size_of::<Edge>() as u64);
    let scope_count = scope_count.max(1);
    let epoch_edge_cap = (max_edge_count / scope_count).clamp(1, 10_000_000);
    let budget = EdgeBudget {
        max_buffer_bytes: scope_collector_plan
            .max_buffer_bytes
            .min(epoch_edge_cap.saturating_mul(std::mem::size_of::<Edge>() as u64)),
        max_run_edges: epoch_edge_cap,
        max_total_bytes: edge_bytes,
    };
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
    let collector_spill_root = out.join(format!(
        ".connectivity-collector-spill-{}",
        crate::artifacts::new_artifact_run_id()
    ));
    let mut temporary_forest_spill = Some(TemporaryForestSpill::new(collector_spill_root.clone()));
    let mut forest_spill_reason = None;
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
                &out.join("rescue-stream-1"),
                &mut progress,
                &mut advisory,
            )?;
            let rescue_execution_plan = &rescue_execution.plan;
            let rescue_pair_visits = rescue_execution_plan.total_visits();
            let collectors = ScopeCollectorBroker::new(
                node_count,
                chain_pair_count,
                budget,
                scope_collector_plan.retained_runtime_bytes,
                scope_collector_plan,
                &collector_spill_root,
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
            let admitted_lanes = loop {
                let concurrent_hot_jobs = hot_job_count.min(lanes) as u64;
                let fixed_hot_bytes = concurrent_hot_jobs.saturating_mul(hot_index_bytes);
                let admitted = memory.active_lanes(lanes, fixed_hot_bytes, catalog_lane_bytes);
                if admitted >= lanes {
                    break admitted;
                }
                if admitted == 0 {
                    break 0;
                }
                lanes = admitted;
            };
            let (catalog_execution, scorer_memory) = if admitted_lanes != 0 {
                lanes = admitted_lanes;
                let fixed_hot_bytes =
                    (hot_job_count.min(lanes) as u64).saturating_mul(hot_index_bytes);
                let scorer_bytes = fixed_hot_bytes
                    .checked_add((lanes as u64).saturating_mul(catalog_lane_bytes))
                    .ok_or(crate::resource::MemoryError::Overflow)?;
                (
                    CatalogExecutionConfig {
                        lanes,
                        chain_count,
                        edge_batch: 32_768,
                        dense_compaction_scratch_bytes: 4 * 1024 * 1024,
                        bounded_mode: false,
                        hot_index_budget: 0,
                    },
                    memory.reserve(scorer_bytes)?,
                )
            } else {
                const MAX_BOUNDED_CATALOG_BYTES: u64 = 64 * 1024 * 1024;
                const MIN_BOUNDED_HOT_INDEX_BYTES: u64 = 64 * 1024;
                let available = memory.available_bytes();
                let lease = memory.reserve_up_to(MAX_BOUNDED_CATALOG_BYTES)?;
                let reserved = lease.bytes();
                let edge_batch =
                    usize::try_from((reserved / 128).clamp(256, 4_096)).unwrap_or(4_096);
                let hot_index_budget = usize::try_from(
                    reserved
                        .saturating_sub(2 * 1024 * 1024)
                        .max(MIN_BOUNDED_HOT_INDEX_BYTES),
                )
                .unwrap_or(usize::MAX);
                advisory(&format!(
                    "one resident catalog scoring lane does not fit: hot_index={} bytes, \
                     lane_scratch={} bytes, available={} bytes; switching to one bounded lane \
                     with direct contract expansion, edge_batch={}, hot_index_budget={} bytes \
                     (reserved={} bytes) instead of terminating",
                    hot_index_bytes,
                    catalog_lane_bytes,
                    available,
                    edge_batch,
                    hot_index_budget,
                    reserved
                ));
                (
                    CatalogExecutionConfig {
                        lanes: 1,
                        chain_count,
                        edge_batch,
                        dense_compaction_scratch_bytes: 0,
                        bounded_mode: true,
                        hot_index_budget,
                    },
                    lease,
                )
            };
            snapshot_residency.report_paging("catalog scoring", &memory, &mut advisory);
            let catalog_result = score_catalog_parallel(
                &snapshot,
                &catalog,
                &recall,
                catalog_execution,
                &collectors,
                &mut progress,
            );
            let (catalog_metrics, catalog_pair_visits) = catalog_result?;
            let metrics: SerializableIndexMetrics = catalog_metrics.into();
            let catalog_admitted_pair_visits = admitted_pair_visits
                .checked_add(catalog_pair_visits)
                .ok_or(crate::resource::MemoryError::Overflow)?;
            drop(scorer_memory);
            // Large shared-token scopes use group-local BaseEquivalent routing
            // while remaining source-context isolated.
            let shared_index_bytes =
                max_shared_group_index_bytes_with_progress(&snapshot, &mut progress)?;
            let shared_lane_bytes = shared_index_bytes
                .saturating_add(BASE_CATALOG_LANE_BYTES)
                .max(1);
            let requested_shared_lanes = collectors.scorer_lanes().max(1);
            let (resident_shared_lanes, resident_shared_memory) =
                memory.reserve_lanes(requested_shared_lanes, 0, shared_lane_bytes)?;
            const MAX_BOUNDED_SHARED_BYTES: u64 = 64 * 1024 * 1024;
            const ESTIMATED_SHARED_CACHE_ENTRY_BYTES: u64 = 64;
            let (
                shared_lanes,
                shared_group_index_budget,
                shared_cache_entries_per_lane,
                shared_index_memory,
            ) = if resident_shared_lanes != 0 {
                let cache_entries =
                    usize::try_from(BASE_CATALOG_LANE_BYTES / ESTIMATED_SHARED_CACHE_ENTRY_BYTES)
                        .unwrap_or(usize::MAX)
                        .min(MAX_RESCUE_PAYLOAD_CACHE_ENTRIES);
                (
                    resident_shared_lanes,
                    shared_index_bytes,
                    cache_entries,
                    resident_shared_memory,
                )
            } else {
                drop(resident_shared_memory);
                let available = memory.available_bytes();
                let lease = memory.reserve_up_to(MAX_BOUNDED_SHARED_BYTES)?;
                let reserved = lease.bytes();
                let group_index_budget = reserved.saturating_sub(BASE_CATALOG_LANE_BYTES);
                let cache_entries = usize::try_from(
                    reserved
                        .min(BASE_CATALOG_LANE_BYTES)
                        .checked_div(ESTIMATED_SHARED_CACHE_ENTRY_BYTES)
                        .unwrap_or_default(),
                )
                .unwrap_or(usize::MAX)
                .min(MAX_RESCUE_PAYLOAD_CACHE_ENTRIES);
                advisory(&format!(
                    "one resident shared-token scoring lane does not fit: largest_group_index={} \
                     bytes, lane_scratch={} bytes, available={} bytes; switching to one bounded \
                     lane with group_index_budget={} bytes and payload_cache_entries={} \
                     (reserved={} bytes). Oversized groups use exact pairwise scoring instead of \
                     terminating",
                    shared_index_bytes,
                    BASE_CATALOG_LANE_BYTES,
                    available,
                    group_index_budget,
                    cache_entries,
                    reserved
                ));
                (1, group_index_budget, cache_entries, lease)
            };
            snapshot_residency.report_paging("shared-token scoring", &memory, &mut advisory);
            let shared_result = append_shared_token_edges(
                &snapshot,
                shared_lanes,
                shared_group_index_budget,
                shared_cache_entries_per_lane,
                &collectors,
                chain_count,
                &mut progress,
            );
            let shared_pair_visits = shared_result?;
            drop(shared_index_memory);
            let candidate_pair_visits = catalog_admitted_pair_visits
                .checked_add(shared_pair_visits)
                .ok_or(crate::resource::MemoryError::Overflow)?;
            append_rescue_edges(
                &snapshot,
                rescue_execution_plan,
                &match_pool,
                &collectors,
                &memory,
                chain_count,
                &mut progress,
            )?;
            drop(rescue_execution);
            let accepted_edge_count = collectors.accepted_edges();
            let (intra_runs, cross_runs, pair_runs) =
                collectors.finish_with_progress(&match_pool, &mut progress)?;
            let persisted_edges = run_edge_count(&intra_runs)
                .saturating_add(run_edge_count(&cross_runs))
                .saturating_add(
                    pair_runs
                        .iter()
                        .map(|runs| run_edge_count(runs))
                        .sum::<usize>(),
                );
            let persisted_bytes =
                persisted_edges.saturating_mul(std::mem::size_of::<Edge>()) as u64;
            let resident_forest_bytes = run_edge_capacity_bytes(&intra_runs)
                .saturating_add(run_edge_capacity_bytes(&cross_runs))
                .saturating_add(
                    pair_runs
                        .iter()
                        .map(|runs| run_edge_capacity_bytes(runs))
                        .fold(0u64, u64::saturating_add),
                );
            if connectivity_mapped_run_count(&intra_runs, &cross_runs, &pair_runs) != 0 {
                forest_spill_reason = Some((persisted_bytes, resident_forest_bytes));
            }
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
    snapshot_residency.report_paging("candidate and rescue scoring", &memory, &mut advisory);
    let persisted_edges = run_edge_count(&intra_runs)
        .saturating_add(run_edge_count(&cross_runs))
        .saturating_add(
            pair_runs
                .iter()
                .map(|runs| run_edge_count(runs))
                .sum::<usize>(),
        );
    let persisted_bytes = persisted_edges.saturating_mul(std::mem::size_of::<Edge>()) as u64;
    let resident_forest_bytes = run_edge_capacity_bytes(&intra_runs)
        .saturating_add(run_edge_capacity_bytes(&cross_runs))
        .saturating_add(
            pair_runs
                .iter()
                .map(|runs| run_edge_capacity_bytes(runs))
                .fold(0u64, u64::saturating_add),
        );
    if let Some((forest_payload, resident_after_spill)) = forest_spill_reason {
        advisory(&format!(
            "metadata connectivity collection reached its resident edge envelope: \
             payload={forest_payload} bytes, resident_after_spill={resident_after_spill} bytes, \
             resident_limit={edge_bytes} bytes; bounded runs were committed as checksummed \
             interleaved edge arrays and reopened by mmap, so collection and reduction continue \
             without terminating or rematerializing spilled forests"
        ));
    } else if persisted_bytes > edge_bytes {
        advisory(&format!(
            "recovered metadata connectivity forests exceed the current resident edge envelope: \
             payload={persisted_bytes} bytes, resident_limit={edge_bytes} bytes; continuing from \
             checksummed mmap runs without materializing the forests"
        ));
    }
    resize_measured_memory_advisory(
        &mut edge_memory,
        resident_forest_bytes,
        "resident forest runs",
        &mut advisory,
    )?;
    let chain_index_estimate = (node_count as u64)
        .checked_mul((2 * std::mem::size_of::<u32>()) as u64)
        .and_then(|bytes| {
            bytes.checked_add(
                (chain_count.saturating_add(1) as u64)
                    .saturating_mul(2 * std::mem::size_of::<u32>() as u64),
            )
        })
        .ok_or(MemoryError::Overflow)?;
    let chain_index_spill_root = out.join(format!(
        ".component-chain-index-{}",
        crate::artifacts::new_artifact_run_id()
    ));
    let (mut chain_index, mut chain_index_memory) = match memory.reserve(chain_index_estimate) {
        Ok(mut lease) => {
            match ChainContractIndex::build(&snapshot.features().contract_chain, chain_count) {
                Ok(index) => match lease.resize(index.total_bytes()) {
                    Ok(()) => (index, Some(lease)),
                    Err(error @ MemoryError::Budget { .. }) => {
                        advisory(&format!(
                            "measured chain-local component index requires {} bytes beyond \
                                 its estimate ({error}); retaining the completed index and letting \
                                 actual allocation pressure determine whether execution can \
                                 continue",
                            index.total_bytes()
                        ));
                        (index, None)
                    }
                    Err(error) => return Err(error.into()),
                },
                Err(PipelineError::Allocation(error)) => {
                    drop(lease);
                    advisory(&format!(
                        "chain-local component index resident allocation failed after admission: \
                     {error}; rebuilding in linear-time checksummed typed arrays and demand-paged \
                     mmap instead of terminating"
                    ));
                    (
                        ChainContractIndex::build_external(
                            &snapshot.features().contract_chain,
                            chain_count,
                            &chain_index_spill_root,
                        )?,
                        None,
                    )
                }
                Err(error) => return Err(error),
            }
        }
        Err(MemoryError::Budget {
            requested,
            used,
            hard_top,
        }) => {
            advisory(&format!(
                "chain-local component index exceeds the remaining broker budget: \
                 requested={requested} bytes, used={used} bytes, hard_top={hard_top} bytes; \
                 rebuilding in linear-time checksummed typed arrays and demand-paged mmap instead \
                 of allocating outside the broker"
            ));
            (
                ChainContractIndex::build_external(
                    &snapshot.features().contract_chain,
                    chain_count,
                    &chain_index_spill_root,
                )?,
                None,
            )
        }
        Err(error) => return Err(error.into()),
    };
    snapshot_residency.report_paging("chain-local component index", &memory, &mut advisory);
    let pair_chains = chain_pairs(chain_count);

    let component_root = out.join("component-snapshots");
    let component_spill_root = out.join(format!(
        ".component-root-spill-{}",
        crate::artifacts::new_artifact_run_id()
    ));
    let mut scopes = Vec::with_capacity(pair_runs.len().saturating_add(2));
    let mut push_scope = |kind: ComponentScopeKind,
                          runs: Vec<ForestRunStorage>,
                          scope_node_count: u32|
     -> Result<(), PipelineError> {
        let directory = component_root.join(kind.directory_name());
        let identity = ComponentSnapshotIdentity {
            schema_revision: crate::scoring::MATCH_SEMANTICS_REVISION,
            snapshot_fingerprint: catalog.snapshot_fingerprint.clone(),
            connectivity_revision: COMPONENT_ROOT_LAYOUT_REVISION,
            connectivity_plan_digest: connectivity_plan_digest.clone(),
            scope_identity: kind.identity(),
            node_count: scope_node_count,
        };
        let mut committed = false;
        let roots = if persistence == MatchPersistence::Durable {
            match open_component_roots(&directory, &identity)? {
                Some(OpenComponentRoots::Mapped(roots)) => Some(mapped_root_storage(
                    roots,
                    identity.node_count as usize,
                    None,
                    None,
                )?),
                Some(OpenComponentRoots::Materialized(roots)) => {
                    commit_component_roots_dense(&directory, &identity, &roots, || {})?;
                    drop(roots);
                    let roots =
                        crate::format::map_u32_array(&component_dense_roots_path(&directory))?;
                    committed = true;
                    Some(mapped_root_storage(
                        roots,
                        identity.node_count as usize,
                        None,
                        None,
                    )?)
                }
                None => None,
            }
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
            committed,
        });
        Ok(())
    };
    push_scope(ComponentScopeKind::Intra, intra_runs, node_count)?;
    push_scope(ComponentScopeKind::Cross, cross_runs, node_count)?;
    let mut pair_runs = pair_runs.into_iter();
    for &(left, right) in &pair_chains {
        let runs = pair_runs
            .next()
            .ok_or_else(|| PipelineError::Invariant("missing chain-pair forest runs".into()))?;
        let pair_node_count = chain_index.pair_node_count(left as usize, right as usize)?;
        push_scope(
            ComponentScopeKind::Pair { left, right },
            runs,
            pair_node_count,
        )?;
    }
    if pair_runs.next().is_some() {
        return Err(PipelineError::Invariant(
            "unexpected extra chain-pair forest runs".into(),
        ));
    }
    release_reused_component_runs(&mut scopes, &worker_pool);
    let active_forest_bytes = scopes
        .iter()
        .map(|scope| run_edge_capacity_bytes(&scope.runs))
        .fold(0u64, u64::saturating_add);
    resize_measured_memory_advisory(
        &mut edge_memory,
        active_forest_bytes,
        "active forest runs",
        &mut advisory,
    )?;
    let retained_chain_index_bytes = chain_index.retained_bytes();
    let chain_contract_storage = std::mem::take(&mut chain_index.contracts);

    let root_node_counts = scopes
        .iter()
        .map(|scope| scope.identity.node_count)
        .collect::<Vec<_>>();
    let component_scope_count = scopes.len();
    let component_memory_plan = plan_component_memory(&memory, config.threads, &root_node_counts)?;
    debug_assert_eq!(component_memory_plan.total_root_bytes, component_bytes);
    let requested_component_scopes = config.threads.max(1).min(component_scope_count.max(1));
    let component_parallel_scopes = component_memory_plan.parallel_scopes.max(1);
    if component_memory_plan.root_mode == ComponentRootMode::Mapped {
        advisory(&format!(
            "component roots exceed the resident memory envelope; switching to budgeted waves \
             with immediate checksummed dense mmap instead of terminating: total_roots={} bytes, \
             working_set={} bytes per scope, requested_scopes={}, admitted_scopes={}, \
             broker_reserved={} bytes, host_headroom_fallback={} bytes, used={} bytes, \
             hard_top={} bytes",
            component_memory_plan.total_root_bytes,
            component_memory_plan
                .transient_bytes_per_scope
                .saturating_mul(2),
            requested_component_scopes,
            component_memory_plan.parallel_scopes,
            component_memory_plan.peak_bytes,
            component_memory_plan.host_headroom_bytes,
            memory.used_bytes(),
            memory.hard_top_bytes()
        ));
    } else if component_parallel_scopes < requested_component_scopes {
        advisory(&format!(
            "component scope concurrency reduced from {requested_component_scopes} to \
             {component_parallel_scopes}: resident roots={} bytes, transient per scope={} bytes, \
             host_headroom_fallback={} bytes, used={} bytes, hard_top={} bytes",
            component_memory_plan.total_root_bytes,
            component_memory_plan.transient_bytes_per_scope,
            component_memory_plan.host_headroom_bytes,
            memory.used_bytes(),
            memory.hard_top_bytes()
        ));
    }
    let component_reserved_bytes = match component_memory_plan.root_mode {
        ComponentRootMode::Resident if component_memory_plan.parallel_scopes == 0 => {
            component_memory_plan.total_root_bytes
        }
        _ => component_memory_plan.peak_bytes,
    };
    let mut component_memory = memory.reserve(component_reserved_bytes)?;
    snapshot_residency.report_paging("component reduction", &memory, &mut advisory);
    let reduce_total = scopes
        .iter()
        .filter(|scope| scope.roots.is_none())
        .try_fold(0u64, |total, scope| -> Result<u64, PipelineError> {
            total
                .checked_add(reduce_work(&scope.runs, scope.identity.node_count)?)
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
        ComponentReduceExecution {
            reduce_total,
            max_parallel_scopes: component_parallel_scopes,
            root_mode: component_memory_plan.root_mode,
            persistence,
            spill_root: &component_spill_root,
            source_node_count: node_count,
            chain_index: &chain_index,
            contract_chain: &snapshot.features().contract_chain,
        },
        &worker_pool,
        &mut progress,
    )?;
    for scope in &mut scopes {
        scope.runs = Vec::new();
    }
    edge_memory.resize(0)?;
    if let Some(spill) = temporary_forest_spill.take() {
        spill.cleanup()?;
    }
    chain_index.release_local_rank();
    if let Some(lease) = chain_index_memory.as_mut() {
        lease.resize(retained_chain_index_bytes)?;
    }
    component_memory.resize(match component_memory_plan.root_mode {
        ComponentRootMode::Resident => component_memory_plan.total_root_bytes,
        ComponentRootMode::Mapped => 0,
    })?;
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
    const COMPONENT_COMMIT_BYTES_PER_SCOPE: u64 = 128 * 1024;
    let requested_commit_scopes = config.threads.max(1).min(component_scope_count.max(1));
    let admitted_commit_scopes =
        memory.active_lanes(requested_commit_scopes, 0, COMPONENT_COMMIT_BYTES_PER_SCOPE);
    let component_commit_scopes = admitted_commit_scopes.max(1);
    if component_commit_scopes < requested_commit_scopes {
        advisory(&format!(
            "component commit concurrency reduced from {requested_commit_scopes} to \
             {component_commit_scopes}: streaming scratch={} bytes per scope, used={} bytes, \
             hard_top={} bytes",
            COMPONENT_COMMIT_BYTES_PER_SCOPE,
            memory.used_bytes(),
            memory.hard_top_bytes()
        ));
    }
    let commit_memory = if persistence != MatchPersistence::Ephemeral {
        let bytes = COMPONENT_COMMIT_BYTES_PER_SCOPE
            .checked_mul(admitted_commit_scopes as u64)
            .ok_or(MemoryError::Overflow)?;
        Some(memory.reserve(bytes)?)
    } else {
        None
    };
    snapshot_residency.report_paging("component commit", &memory, &mut advisory);
    if persistence != MatchPersistence::Ephemeral {
        commit_component_scopes_parallel(
            &scopes,
            component_total,
            component_commit_scopes,
            &worker_pool,
            &mut progress,
        )?;
    }
    drop(commit_memory);
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
            ComponentScopeKind::Pair { left, right } => {
                let left_contract_count = chain_index.chain_contract_count(left as usize)?;
                chain_pair_roots.push(ChainPairRoots {
                    left_chain: left,
                    right_chain: right,
                    left_contract_count,
                    roots,
                });
            }
        }
    }
    let chain_contract_offsets = std::mem::take(&mut chain_index.offsets);
    let scope_components = ScopeComponents {
        intra_roots: intra_roots
            .ok_or_else(|| PipelineError::Invariant("missing intra component scope".into()))?,
        cross_roots: cross_roots
            .ok_or_else(|| PipelineError::Invariant("missing cross component scope".into()))?,
        chain_pair_roots,
        chain_contract_offsets,
        chain_contracts: chain_contract_storage,
    };
    let primary_summary_bytes_per_scope = (node_count as u64)
        .checked_mul(std::mem::size_of::<SummaryEntry>() as u64)
        .ok_or(MemoryError::Overflow)?;
    let pair_summary_bytes_per_scope = scope_components
        .chain_pair_roots
        .iter()
        .map(|pair| pair.roots.len() as u64)
        .max()
        .unwrap_or_default()
        .checked_mul(std::mem::size_of::<SummaryEntry>() as u64)
        .ok_or(MemoryError::Overflow)?;
    let primary_summary_scopes = 1usize.saturating_add(usize::from(chain_count > 1));
    let pair_summary_scopes = scope_components.chain_pair_roots.len();
    let (summary_memory_plan, summary_memory) = reserve_summary_memory(
        &memory,
        config.threads,
        primary_summary_scopes,
        pair_summary_scopes,
        primary_summary_bytes_per_scope,
        pair_summary_bytes_per_scope,
    )?;
    snapshot_residency.report_paging("summary admission", &memory, &mut advisory);
    let primary_summary_lanes = summary_memory_plan.parallel_primary_scopes;
    let pair_summary_lanes = summary_memory_plan.parallel_pair_scopes;
    if primary_summary_lanes == 0 {
        advisory(&format!(
            "metadata summary resident scratch does not fit the current memory budget; \
             switching to bounded external merge instead of terminating: full scratch={} bytes \
             primary scope, pair scratch={} bytes per scope, stream scratch={} bytes, \
             reserved={} bytes, host-headroom fallback={} \
             bytes, used={} bytes, hard_top={} bytes",
            summary_memory_plan.primary_bytes_per_scope,
            summary_memory_plan.pair_bytes_per_scope,
            summary_memory_plan.stream_scratch_bytes,
            summary_memory_plan.peak_bytes,
            summary_memory_plan.stream_headroom_bytes,
            memory.used_bytes(),
            memory.hard_top_bytes()
        ));
    } else if primary_summary_lanes < primary_summary_scopes
        || (pair_summary_scopes != 0
            && pair_summary_lanes < pair_summary_scopes.min(config.threads))
    {
        advisory(&format!(
            "metadata summary concurrency reduced: primary {primary_summary_scopes}->{primary_summary_lanes}, \
             pair {}->{pair_summary_lanes}; primary scratch={} bytes, pair scratch={} bytes, \
             used={} bytes, hard_top={} bytes",
            pair_summary_scopes.min(config.threads.max(1)),
            summary_memory_plan.primary_bytes_per_scope,
            summary_memory_plan.pair_bytes_per_scope,
            memory.used_bytes(),
            memory.hard_top_bytes()
        ));
    }
    let summary_stream_scratch = out.join("metadata-summary-stream-scratch");
    let summary_rows = build_summary_rows_with_progress(
        &snapshot,
        &scope_components,
        chain_count,
        SummaryBuildPlan {
            memory: summary_memory_plan,
            stream_scratch_root: &summary_stream_scratch,
        },
        &worker_pool,
        &mut progress,
        &mut advisory,
    )?;
    drop(summary_memory);
    drop(evidence_memory);
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
            "summary_revision": 2,
            "schema_revision": result.schema_revision,
            "evidence_gate_revision": result.evidence_gate_revision,
            "snapshot_fingerprint": result.snapshot_fingerprint,
            "snapshot_atoms": result.snapshot_atoms,
            "index_metrics": result.index_metrics,
            "exact_evidence": {
                "pair_work": result.exact_evidence.pair_work,
                "exact_matches": result.exact_evidence.exact_matches,
                "conservative_miss_count": result.exact_evidence.conservative_misses.len(),
                "artifact": "../exact-islands/pair-calibration-1/ready",
            },
            "pair_holdout_evidence": {
                "pair_work": result.pair_holdout_evidence.pair_work,
                "exact_matches": result.pair_holdout_evidence.exact_matches,
                "conservative_miss_count": result.pair_holdout_evidence.conservative_misses.len(),
                "artifact": "../exact-islands/pair-holdout-1/ready",
            },
            "shared_token_exact_evidence": {
                "pair_work": result.shared_token_exact_evidence.pair_work,
                "exact_matches": result.shared_token_exact_evidence.exact_matches,
                "calibration_miss_count": result.shared_token_exact_evidence.calibration_misses.len(),
                "holdout_miss_count": result.shared_token_exact_evidence.holdout_misses.len(),
                "scratch_fallback_group_count": result.shared_token_exact_evidence.scratch_fallback_tokens.len(),
                "artifact": "../exact-islands/shared-token-1/ready",
            },
            "skipped_shared_token_evidence_groups": result.skipped_shared_token_evidence_groups,
            "rescue_plan": {
                "pair_atom_count": result.rescue_plan.pair_atoms.len(),
                "shared_contract_count": result.rescue_plan.shared_contracts.len(),
                "shared_edge_count": result.rescue_plan.shared_edges.len(),
                "artifact": "../rescue-plan-1/rescue-plan.ready",
            },
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
        crate::format::commit_ready_serialized(&summary_dir, "metadata-summary.ready", &ready)?;
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
    snapshot_residency.report_paging("artifact commit", &memory, &mut advisory);
    Ok(result)
}

fn reserve_pipeline_storage_advisory(
    storage: &mut StorageBroker,
    class: ArtifactClass,
    final_bytes: u64,
    partial_peak_bytes: u64,
    _label: &str,
    _advisory: &mut dyn FnMut(&str),
) -> Result<Option<StorageLease>, PipelineError> {
    storage
        .reserve(class, final_bytes, partial_peak_bytes)
        .map(Some)
        .map_err(Into::into)
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

fn resident_run_edge_capacity_bytes(runs: &[ForestRun]) -> u64 {
    runs.iter()
        .map(|run| (run.edges.capacity() as u64).saturating_mul(std::mem::size_of::<Edge>() as u64))
        .fold(0u64, u64::saturating_add)
}

fn run_edge_count(runs: &[ForestRunStorage]) -> usize {
    runs.iter().map(ForestRunStorage::edge_count).sum()
}

fn run_edge_capacity_bytes(runs: &[ForestRunStorage]) -> u64 {
    runs.iter()
        .map(ForestRunStorage::resident_capacity_bytes)
        .fold(0u64, u64::saturating_add)
}

fn connectivity_mapped_run_count(
    intra: &[ForestRunStorage],
    cross: &[ForestRunStorage],
    pairs: &[Vec<ForestRunStorage>],
) -> usize {
    intra
        .iter()
        .chain(cross)
        .chain(pairs.iter().flatten())
        .filter(|run| run.is_mapped())
        .count()
}

fn reduce_work(runs: &[ForestRunStorage], node_count: u32) -> Result<u64, PipelineError> {
    crate::reduce::planned_reduce_work(run_edge_count(runs) as u64, node_count)
        .map_err(PipelineError::from)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ComponentRootMode {
    Resident,
    Mapped,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ComponentMemoryPlan {
    root_mode: ComponentRootMode,
    parallel_scopes: usize,
    total_root_bytes: u64,
    transient_bytes_per_scope: u64,
    peak_bytes: u64,
    host_headroom_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SummaryMemoryPlan {
    parallel_primary_scopes: usize,
    parallel_pair_scopes: usize,
    primary_bytes_per_scope: u64,
    pair_bytes_per_scope: u64,
    peak_bytes: u64,
    stream_scratch_bytes: u64,
    stream_headroom_bytes: u64,
}

#[derive(Clone, Copy)]
struct SummaryBuildPlan<'a> {
    memory: SummaryMemoryPlan,
    stream_scratch_root: &'a Path,
}

fn reserve_summary_memory(
    memory: &MemoryBroker,
    requested_threads: usize,
    primary_scope_count: usize,
    pair_scope_count: usize,
    primary_bytes_per_scope: u64,
    pair_bytes_per_scope: u64,
) -> Result<(SummaryMemoryPlan, MemoryLease), PipelineError> {
    let requested_primary = requested_threads.max(1).min(primary_scope_count);
    let requested_pairs = requested_threads.max(1).min(pair_scope_count);
    let available = memory.available_bytes();
    let affordable_lanes = |bytes_per_scope: u64| {
        usize::try_from(available.checked_div(bytes_per_scope).unwrap_or(u64::MAX))
            .unwrap_or(usize::MAX)
    };
    let parallel_primary_scopes = requested_primary.min(affordable_lanes(primary_bytes_per_scope));
    let parallel_pair_scopes = requested_pairs.min(affordable_lanes(pair_bytes_per_scope));
    let resident_peak_bytes = primary_bytes_per_scope
        .checked_mul(parallel_primary_scopes as u64)
        .and_then(|primary| {
            pair_bytes_per_scope
                .checked_mul(parallel_pair_scopes as u64)
                .map(|pair| primary.max(pair))
        })
        .ok_or(MemoryError::Overflow)?;
    let streaming_primary_bytes =
        u64::from(primary_scope_count != 0 && parallel_primary_scopes == 0)
            .saturating_mul(primary_bytes_per_scope);
    let streaming_pair_bytes = u64::from(pair_scope_count != 0 && parallel_pair_scopes == 0)
        .saturating_mul(pair_bytes_per_scope);
    let desired_stream_scratch = streaming_primary_bytes.max(streaming_pair_bytes);
    let desired_stream_scratch = if desired_stream_scratch == 0 {
        0
    } else {
        desired_stream_scratch.clamp(
            SUMMARY_STREAM_MIN_SCRATCH_BYTES,
            SUMMARY_STREAM_MAX_SCRATCH_BYTES,
        )
    };
    let broker_stream_scratch = if available >= SUMMARY_STREAM_MIN_SCRATCH_BYTES {
        available.min(desired_stream_scratch)
    } else {
        0
    };
    let peak_bytes = resident_peak_bytes.max(broker_stream_scratch);
    let lease = memory.reserve(peak_bytes)?;
    let reserved_stream_scratch = lease.bytes().min(desired_stream_scratch);
    let stream_scratch_bytes = if desired_stream_scratch != 0
        && reserved_stream_scratch < SUMMARY_STREAM_MIN_SCRATCH_BYTES
    {
        desired_stream_scratch
    } else {
        reserved_stream_scratch
    };
    let stream_headroom_bytes = stream_scratch_bytes.saturating_sub(reserved_stream_scratch);
    Ok((
        SummaryMemoryPlan {
            parallel_primary_scopes,
            parallel_pair_scopes,
            primary_bytes_per_scope,
            pair_bytes_per_scope,
            peak_bytes,
            stream_scratch_bytes,
            stream_headroom_bytes,
        },
        lease,
    ))
}

fn plan_component_memory(
    memory: &MemoryBroker,
    requested_threads: usize,
    root_node_counts: &[u32],
) -> Result<ComponentMemoryPlan, PipelineError> {
    let scope_count = root_node_counts.len().max(1);
    let requested_scopes = requested_threads.max(1).min(scope_count);
    let mut total_root_bytes = 0u64;
    let mut largest_scope_bytes = BinaryHeap::<Reverse<u64>>::with_capacity(requested_scopes);
    for &nodes in root_node_counts {
        let bytes = u64::from(nodes)
            .checked_mul(std::mem::size_of::<u32>() as u64)
            .ok_or(MemoryError::Overflow)?;
        total_root_bytes = total_root_bytes
            .checked_add(bytes)
            .ok_or(MemoryError::Overflow)?;
        if largest_scope_bytes.len() < requested_scopes {
            largest_scope_bytes.push(Reverse(bytes));
        } else if largest_scope_bytes
            .peek()
            .is_some_and(|smallest| bytes > smallest.0)
        {
            largest_scope_bytes.pop();
            largest_scope_bytes.push(Reverse(bytes));
        }
    }
    let mut largest_scope_bytes = largest_scope_bytes
        .into_iter()
        .map(|Reverse(bytes)| bytes)
        .collect::<Vec<_>>();
    largest_scope_bytes.sort_unstable_by(|left, right| right.cmp(left));
    let mut largest_scope_prefix = Vec::with_capacity(largest_scope_bytes.len() + 1);
    largest_scope_prefix.push(0u64);
    for bytes in largest_scope_bytes {
        largest_scope_prefix.push(
            largest_scope_prefix
                .last()
                .copied()
                .unwrap_or_default()
                .checked_add(bytes)
                .ok_or(MemoryError::Overflow)?,
        );
    }
    let root_bytes_per_scope = largest_scope_prefix.get(1).copied().unwrap_or_default();
    let available = memory.available_bytes();
    let (root_mode, parallel_scopes, peak_bytes, host_headroom_bytes) =
        if total_root_bytes <= available {
            let mut parallel_scopes = 0usize;
            let mut peak_bytes = total_root_bytes;
            for (lanes, &transient_bytes) in largest_scope_prefix.iter().enumerate().skip(1) {
                let candidate = total_root_bytes
                    .checked_add(transient_bytes)
                    .ok_or(MemoryError::Overflow)?;
                if candidate > available {
                    break;
                }
                parallel_scopes = lanes;
                peak_bytes = candidate;
            }
            (
                ComponentRootMode::Resident,
                parallel_scopes,
                peak_bytes,
                usize::from(parallel_scopes == 0) as u64 * root_bytes_per_scope,
            )
        } else {
            // Each mapped-fallback lane holds one AtomicU32 parent plus one
            // final roots Vec. The Vec is checksummed, mapped, and released
            // before the next wave, so completed scopes do not accumulate.
            let mut parallel_scopes = 0usize;
            let mut peak_bytes = 0u64;
            for (lanes, &root_bytes) in largest_scope_prefix.iter().enumerate().skip(1) {
                let candidate = root_bytes.checked_mul(2).ok_or(MemoryError::Overflow)?;
                if candidate > available {
                    break;
                }
                parallel_scopes = lanes;
                peak_bytes = candidate;
            }
            let working_bytes_per_scope = root_bytes_per_scope
                .checked_mul(2)
                .ok_or(MemoryError::Overflow)?;
            (
                ComponentRootMode::Mapped,
                parallel_scopes,
                peak_bytes,
                if parallel_scopes == 0 {
                    working_bytes_per_scope
                } else {
                    0
                },
            )
        };
    Ok(ComponentMemoryPlan {
        root_mode,
        parallel_scopes,
        total_root_bytes,
        transient_bytes_per_scope: root_bytes_per_scope,
        peak_bytes,
        host_headroom_bytes,
    })
}

fn mapped_root_storage(
    roots: crate::format::MappedU32Array,
    expected_len: usize,
    cleanup_file: Option<PathBuf>,
    cleanup_dir: Option<PathBuf>,
) -> Result<RootStorage, PipelineError> {
    if roots.len() != expected_len
        || roots
            .iter()
            .any(|&root| usize::try_from(root).map_or(true, |root| root >= expected_len))
    {
        return Err(PipelineError::Invariant(
            "mapped component roots are outside their scope identity".into(),
        ));
    }
    Ok(RootStorage::mapped(roots, cleanup_file, cleanup_dir))
}

fn persist_mapped_component_roots(
    scope: &mut ComponentScopePlan,
    roots: Vec<u32>,
    persistence: MatchPersistence,
    spill_root: &Path,
) -> Result<RootStorage, PipelineError> {
    let expected_len = roots.len();
    if persistence == MatchPersistence::Ephemeral {
        std::fs::create_dir_all(spill_root)?;
        let path = spill_root.join(format!("{}.u32", scope.kind.directory_name()));
        let outcome = (|| {
            crate::format::write_u32_array(&path, crate::format::ArrayKind::U32, &roots)?;
            drop(roots);
            let mapped = crate::format::map_u32_array(&path)?;
            mapped_root_storage(
                mapped,
                expected_len,
                Some(path.clone()),
                Some(spill_root.to_path_buf()),
            )
        })();
        if outcome.is_err() {
            let _ = std::fs::remove_file(&path);
            let _ = std::fs::remove_dir(spill_root);
        }
        outcome
    } else {
        commit_component_roots_dense(&scope.directory, &scope.identity, &roots, || {})?;
        scope.committed = true;
        drop(roots);
        let mapped = crate::format::map_u32_array(&component_dense_roots_path(&scope.directory))?;
        mapped_root_storage(mapped, expected_len, None, None)
    }
}

#[derive(Clone, Copy)]
struct ComponentReduceExecution<'a> {
    reduce_total: u64,
    max_parallel_scopes: usize,
    root_mode: ComponentRootMode,
    persistence: MatchPersistence,
    spill_root: &'a Path,
    source_node_count: u32,
    chain_index: &'a ChainContractIndex,
    contract_chain: &'a [u32],
}

fn reduce_component_scopes_parallel(
    scopes: &mut [ComponentScopePlan],
    execution: ComponentReduceExecution<'_>,
    worker_pool: &rayon::ThreadPool,
    progress: &mut impl FnMut(ProgressEvent),
) -> Result<(), PipelineError> {
    let ComponentReduceExecution {
        reduce_total,
        max_parallel_scopes,
        root_mode,
        persistence,
        spill_root,
        source_node_count,
        chain_index,
        contract_chain,
    } = execution;
    let channel_capacity = worker_pool.current_num_threads().max(1).saturating_mul(2);
    let (sender, receiver) = std::sync::mpsc::sync_channel::<u64>(channel_capacity);
    std::thread::scope(|thread_scope| -> Result<(), PipelineError> {
        let producer_sender = sender.clone();
        let producer = thread_scope.spawn(move || {
            for wave in scopes.chunks_mut(max_parallel_scopes.max(1)) {
                worker_pool.install(|| {
                    wave.par_iter_mut()
                        .filter(|scope| scope.roots.is_none())
                        .try_for_each(|scope| -> Result<(), PipelineError> {
                            let mut previous = 0u64;
                            let node_count = scope.identity.node_count;
                            let mut report_progress = |completed: u64, _: u64| {
                                let delta = completed.saturating_sub(previous);
                                previous = completed;
                                if delta != 0 {
                                    let _ = producer_sender.send(delta);
                                }
                            };
                            let roots = match &scope.kind {
                                ComponentScopeKind::Pair { left, right } => {
                                    let left = *left as usize;
                                    let right = *right as usize;
                                    reduce_stored_components_with_progress(
                                        &scope.runs,
                                        source_node_count,
                                        node_count,
                                        |edge| {
                                            let local_left = chain_index
                                                .pair_local_contract(
                                                    contract_chain,
                                                    edge.left,
                                                    left,
                                                    right,
                                                )
                                                .map_err(|error| {
                                                    crate::reduce::ReduceError::SnapshotChain(
                                                        format!(
                                                            "pair endpoint localization failed: \
                                                             {error}"
                                                        ),
                                                    )
                                                })?;
                                            let local_right = chain_index
                                                .pair_local_contract(
                                                    contract_chain,
                                                    edge.right,
                                                    left,
                                                    right,
                                                )
                                                .map_err(|error| {
                                                    crate::reduce::ReduceError::SnapshotChain(
                                                        format!(
                                                            "pair endpoint localization failed: \
                                                             {error}"
                                                        ),
                                                    )
                                                })?;
                                            Ok(Edge::new(local_left, local_right))
                                        },
                                        &mut report_progress,
                                    )
                                }
                                ComponentScopeKind::Intra | ComponentScopeKind::Cross => {
                                    reduce_stored_components_with_progress(
                                        &scope.runs,
                                        source_node_count,
                                        node_count,
                                        Ok,
                                        &mut report_progress,
                                    )
                                }
                            }?;
                            scope.roots = Some(match root_mode {
                                ComponentRootMode::Resident => RootStorage::resident(roots),
                                ComponentRootMode::Mapped => persist_mapped_component_roots(
                                    scope,
                                    roots,
                                    persistence,
                                    spill_root,
                                )?,
                            });
                            scope.runs = Vec::new();
                            Ok(())
                        })
                })?;
            }
            Ok::<(), PipelineError>(())
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
        progress(ProgressEvent::determinate(
            ProgressPhase::ReduceScopes,
            reduced_work,
            reduced_work,
            WorkUnit::Items,
            ProgressCounters::default(),
        ));
        Ok(())
    })
}

fn commit_component_scopes_parallel(
    scopes: &[ComponentScopePlan],
    component_total: u64,
    max_parallel_scopes: usize,
    worker_pool: &rayon::ThreadPool,
    progress: &mut impl FnMut(ProgressEvent),
) -> Result<(), PipelineError> {
    let channel_capacity = worker_pool.current_num_threads().max(1).saturating_mul(2);
    let (sender, receiver) = std::sync::mpsc::sync_channel::<()>(channel_capacity);
    std::thread::scope(|thread_scope| -> Result<(), PipelineError> {
        let producer_sender = sender.clone();
        let producer = thread_scope.spawn(move || {
            for wave in scopes.chunks(max_parallel_scopes.max(1)) {
                worker_pool.install(|| {
                    wave.par_iter()
                        .filter(|scope| scope.needs_rebuild)
                        .try_for_each(|scope| -> Result<(), PipelineError> {
                            if scope.committed {
                                let _ = producer_sender.send(());
                                let _ = producer_sender.send(());
                                return Ok(());
                            }
                            let roots = scope.roots.as_deref().ok_or_else(|| {
                                PipelineError::Invariant("missing reduced roots".into())
                            })?;
                            commit_component_roots_dense(
                                &scope.directory,
                                &scope.identity,
                                roots,
                                || {
                                    let _ = producer_sender.send(());
                                },
                            )?;
                            Ok(())
                        })
                })?;
            }
            Ok::<(), PipelineError>(())
        });
        drop(sender);
        let mut committed = 0u64;
        for () in receiver {
            committed = committed.saturating_add(1);
            progress(ProgressEvent::determinate(
                ProgressPhase::CommitComponents,
                committed.min(component_total),
                component_total,
                WorkUnit::Files,
                ProgressCounters::default(),
            ));
        }
        producer
            .join()
            .map_err(|_| PipelineError::Parallel("component commit worker panicked".into()))??;
        progress(ProgressEvent::determinate(
            ProgressPhase::CommitComponents,
            committed,
            committed,
            WorkUnit::Files,
            ProgressCounters::default(),
        ));
        Ok(())
    })
}

fn commit_runs(
    runs: &[ForestRunStorage],
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

fn open_run_count(directory: &Path, count: u32) -> Result<Vec<ForestRunStorage>, PipelineError> {
    (0..count)
        .map(|run_id| ForestRunStorage::open_mapped(directory, run_id).map_err(PipelineError::from))
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

fn shared_member_index_bytes(
    features: &crate::encode::FeatureView,
    source: u32,
) -> Result<u64, PipelineError> {
    let payload = features.source_to_payload[source as usize] as usize;
    let terms = (features.payload_template_offsets[payload + 1]
        - features.payload_template_offsets[payload])
        .checked_add(
            features.payload_content_offsets[payload + 1]
                - features.payload_content_offsets[payload],
        )
        .ok_or(crate::resource::MemoryError::Overflow)?;
    Ok(terms.saturating_mul(8).saturating_add(256))
}

fn shared_group_index_bytes(
    features: &crate::encode::FeatureView,
    sources: &[u32],
) -> Result<u64, PipelineError> {
    if sources.len() < SHARED_LOCAL_ROUTING_MIN_MEMBERS {
        return Ok(0);
    }
    sources.iter().try_fold(0u64, |bytes, &source| {
        bytes
            .checked_add(shared_member_index_bytes(features, source)?)
            .ok_or(crate::resource::MemoryError::Overflow.into())
    })
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
        let indexed_group =
            window[1].saturating_sub(window[0]) as usize >= SHARED_LOCAL_ROUTING_MIN_MEMBERS;
        for member in window[0] as usize..window[1] as usize {
            if indexed_group {
                bytes = bytes
                    .checked_add(shared_member_index_bytes(
                        features,
                        features.token_member_sources[member],
                    )?)
                    .ok_or(crate::resource::MemoryError::Overflow)?;
            }
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
                    &mut scratch.retained_token_sort,
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

#[allow(clippy::too_many_arguments)]
fn expand_catalog_atom_pair_bounded(
    snapshot: &MetadataSnapshot,
    left_atom: u32,
    right_atom: u32,
    template_match_proven: bool,
    state: &mut CatalogParallelLaneState,
    collectors: &ScopeCollectorBroker,
    chain_count: usize,
    edge_batch: usize,
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
    const FOREST_FAST_PATH_MIN_PAIR_WORK: usize = 64;
    if left_contracts.len().saturating_mul(right_contracts.len()) >= FOREST_FAST_PATH_MIN_PAIR_WORK
    {
        let mut scratch = state
            .bounded_expansion_scratch
            .take()
            .expect("bounded catalog lane has bounded expansion scratch");
        let disjoint = scratch.retained_tokens_disjoint(features, left_contracts, right_contracts);
        if disjoint {
            scratch.prepare_chain_roots(features, left_contracts, right_contracts);
            let mut emit_error = None;
            emit_chain_scoped_complete_bipartite_forest(
                features,
                left_contracts,
                right_contracts,
                &scratch.left_chain_roots,
                &scratch.right_chain_roots,
                |left, right| {
                    if emit_error.is_some() || left == right {
                        return;
                    }
                    state.batch.push(Edge::new(left, right));
                    if state.batch.len() >= edge_batch.max(1) {
                        if let Err(error) = submit_catalog_lane_batch(
                            state,
                            snapshot,
                            chain_count,
                            collectors,
                            edge_batch.max(1),
                        ) {
                            emit_error = Some(error);
                        }
                    }
                },
            );
            state.bounded_expansion_scratch = Some(scratch);
            if let Some(error) = emit_error {
                return Err(error);
            }
            return Ok(work);
        }
        state.bounded_expansion_scratch = Some(scratch);
    }
    for &left in left_contracts {
        for &right in right_contracts {
            if left == right || contracts_share_retained_token(features, left, right) {
                continue;
            }
            state.batch.push(Edge::new(left, right));
            if state.batch.len() >= edge_batch {
                submit_catalog_lane_batch(state, snapshot, chain_count, collectors, edge_batch)?;
            }
        }
    }
    Ok(work)
}

fn emit_chain_scoped_complete_bipartite_forest(
    features: &crate::encode::FeatureView,
    left: &[u32],
    right: &[u32],
    left_chain_roots: &[Option<u32>],
    right_chain_roots: &[Option<u32>],
    mut emit: impl FnMut(u32, u32),
) {
    for &right_contract in right {
        for &left_root in left_chain_roots.iter().flatten() {
            emit(left_root, right_contract);
        }
    }
    for &left_contract in left {
        let left_chain = features.contract_chain[left_contract as usize] as usize;
        if left_chain_roots[left_chain] == Some(left_contract) {
            continue;
        }
        for &right_root in right_chain_roots.iter().flatten() {
            emit(left_contract, right_root);
        }
    }
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

type RescuePair = (u32, u32);

enum RescueFallbackStorage<'a> {
    InMemoryChunks {
        atom_chunks: Vec<RescueMatchChunk>,
        shared_chunks: Vec<RescueMatchChunk>,
        base_shared_edges: &'a [RescuePair],
    },
    Spilled {
        files: RescueSpillFiles,
        base_shared_edges: &'a [RescuePair],
    },
}

struct RescueExecutionPlan<'a> {
    atom_score_visits: u64,
    contract_expansion_visits: u64,
    shared_score_visits: u64,
    matched_atom_pairs: Vec<RescuePair>,
    matched_shared_edges: Vec<RescuePair>,
    fallback: Option<RescueFallbackStorage<'a>>,
    matched_shared_edge_count: u64,
    stream_buffer_bytes: u64,
}

impl RescueExecutionPlan<'_> {
    fn total_visits(&self) -> u64 {
        self.atom_score_visits
            .saturating_add(self.shared_score_visits)
            .saturating_add(self.contract_expansion_visits)
            .saturating_add(self.matched_shared_edge_count)
    }

    fn execution_work(&self) -> u64 {
        self.contract_expansion_visits
            .saturating_add(self.matched_shared_edge_count)
    }
}

struct AdmittedRescueExecutionPlan<'a> {
    plan: RescueExecutionPlan<'a>,
    _match_memory: Option<MemoryLease>,
    _stream_memory: Option<MemoryLease>,
}

const RESCUE_MATCH_CHUNK_PAIRS: usize = 4_096;
const RESCUE_SCORE_TILE: usize = 65_536;
const MAX_RESCUE_STREAM_IO_BYTES: u64 = 256 * 1024 * 1024;
const MAX_RESCUE_GLOBAL_SORT_PAIRS: u64 = 250_000_000;

struct RescueMatchChunk {
    pairs: Vec<RescuePair>,
    expansion_work: u64,
    _memory: Option<MemoryLease>,
}

impl RescueMatchChunk {
    fn retained_bytes() -> Result<u64, MemoryError> {
        (RESCUE_MATCH_CHUNK_PAIRS as u64)
            .checked_mul(std::mem::size_of::<(u32, u32)>() as u64)
            .and_then(|value| value.checked_add(std::mem::size_of::<RescueMatchChunk>() as u64))
            .ok_or(MemoryError::Overflow)
    }

    fn new() -> Result<Self, PipelineError> {
        let mut pairs = Vec::new();
        pairs
            .try_reserve_exact(RESCUE_MATCH_CHUNK_PAIRS)
            .map_err(|error| {
                PipelineError::Allocation(format!(
                    "unable to allocate a {}-pair rescue stream buffer: {error}",
                    RESCUE_MATCH_CHUNK_PAIRS
                ))
            })?;
        Ok(Self {
            pairs,
            expansion_work: 0,
            _memory: None,
        })
    }

    fn retain(&mut self, memory: &MemoryBroker) -> Result<(), MemoryError> {
        self._memory = Some(memory.reserve(Self::retained_bytes()?)?);
        Ok(())
    }
}

fn rescue_final_match_bytes(atom_count: u64, shared_count: u64) -> Result<u64, MemoryError> {
    atom_count
        .checked_add(shared_count)
        .and_then(|count| count.checked_mul(std::mem::size_of::<RescuePair>() as u64))
        .ok_or(MemoryError::Overflow)
}

fn rescue_stream_buffer_bytes(lanes: usize) -> Result<u64, MemoryError> {
    let slots = lanes
        .max(1)
        .checked_mul(3)
        .and_then(|slots| slots.checked_add(4))
        .ok_or(MemoryError::Overflow)?;
    (slots as u64)
        .checked_mul(RescueMatchChunk::retained_bytes()?)
        .ok_or(MemoryError::Overflow)
}

fn rescue_spill_batch_pairs(admitted_bytes: u64) -> usize {
    const PARALLEL_STREAM_BUFFERS: u64 = 4;
    let bytes_per_pair = (std::mem::size_of::<RescuePair>() as u64)
        .saturating_mul(PARALLEL_STREAM_BUFFERS)
        .max(1);
    let max_pairs = (MAX_RESCUE_STREAM_IO_BYTES / bytes_per_pair).max(1);
    let pairs = (admitted_bytes / bytes_per_pair).clamp(
        RESCUE_MATCH_CHUNK_PAIRS as u64,
        max_pairs.max(RESCUE_MATCH_CHUNK_PAIRS as u64),
    );
    let aligned = pairs - pairs % RESCUE_MATCH_CHUNK_PAIRS as u64;
    usize::try_from(aligned.max(RESCUE_MATCH_CHUNK_PAIRS as u64))
        .unwrap_or(RESCUE_MATCH_CHUNK_PAIRS)
}

struct RescueSpillWriter {
    directory: PathBuf,
    atom_path: PathBuf,
    shared_path: PathBuf,
    atom: Option<BufWriter<std::fs::File>>,
    shared: Option<BufWriter<std::fs::File>>,
    committed: bool,
}

impl RescueSpillWriter {
    fn create(directory: &Path) -> Result<Self, PipelineError> {
        std::fs::create_dir_all(directory)?;
        let atom_path = directory.join("atom-pairs.bin");
        let shared_path = directory.join("shared-edges.bin");
        let atom = BufWriter::with_capacity(
            RESCUE_MATCH_CHUNK_PAIRS * std::mem::size_of::<RescuePair>(),
            std::fs::File::create(&atom_path)?,
        );
        let shared = BufWriter::with_capacity(
            RESCUE_MATCH_CHUNK_PAIRS * std::mem::size_of::<RescuePair>(),
            std::fs::File::create(&shared_path)?,
        );
        Ok(Self {
            directory: directory.to_path_buf(),
            atom_path,
            shared_path,
            atom: Some(atom),
            shared: Some(shared),
            committed: false,
        })
    }

    fn write_chunk(&mut self, chunk: &RescueMatchChunk, shared: bool) -> Result<(), PipelineError> {
        let writer = if shared {
            self.shared.as_mut()
        } else {
            self.atom.as_mut()
        }
        .expect("rescue spill writer is open");
        let mut encoded = [0u8; 8 * 1_024];
        for pairs in chunk.pairs.chunks(1_024) {
            for (index, &(left, right)) in pairs.iter().enumerate() {
                let offset = index * 8;
                encoded[offset..offset + 4].copy_from_slice(&left.to_le_bytes());
                encoded[offset + 4..offset + 8].copy_from_slice(&right.to_le_bytes());
            }
            writer.write_all(&encoded[..pairs.len() * 8])?;
        }
        Ok(())
    }

    fn finish(mut self) -> Result<RescueSpillFiles, PipelineError> {
        self.atom
            .as_mut()
            .expect("rescue atom spill writer is open")
            .flush()?;
        self.shared
            .as_mut()
            .expect("rescue shared spill writer is open")
            .flush()?;
        drop(self.atom.take());
        drop(self.shared.take());
        self.committed = true;
        Ok(RescueSpillFiles {
            directory: self.directory.clone(),
            atom_path: self.atom_path.clone(),
            shared_path: self.shared_path.clone(),
        })
    }
}

impl Drop for RescueSpillWriter {
    fn drop(&mut self) {
        drop(self.atom.take());
        drop(self.shared.take());
        if !self.committed {
            let _ = std::fs::remove_file(&self.atom_path);
            let _ = std::fs::remove_file(&self.shared_path);
            let _ = std::fs::remove_dir(&self.directory);
        }
    }
}

struct RescueSpillFiles {
    directory: PathBuf,
    atom_path: PathBuf,
    shared_path: PathBuf,
}

impl Drop for RescueSpillFiles {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.atom_path);
        let _ = std::fs::remove_file(&self.shared_path);
        let _ = std::fs::remove_dir(&self.directory);
    }
}

#[allow(clippy::too_many_arguments)]
fn retain_or_spill_rescue_chunk(
    mut chunk: RescueMatchChunk,
    shared: bool,
    memory: &MemoryBroker,
    spill_directory: &Path,
    atom_chunks: &mut Vec<RescueMatchChunk>,
    shared_chunks: &mut Vec<RescueMatchChunk>,
    spill_writer: &mut Option<RescueSpillWriter>,
    advisory: &mut dyn FnMut(&str),
) -> Result<(), PipelineError> {
    if let Some(writer) = spill_writer.as_mut() {
        return writer.write_chunk(&chunk, shared);
    }
    match chunk.retain(memory) {
        Ok(()) => {
            if shared {
                shared_chunks.push(chunk);
            } else {
                atom_chunks.push(chunk);
            }
            Ok(())
        }
        Err(error @ MemoryError::Budget { .. }) => {
            advisory(&format!(
                "metadata rescue matched-pair corpus exceeded the resident memory budget \
                 ({error}); switching to bounded one-pass disk streaming in {}",
                spill_directory.display()
            ));
            let mut writer = RescueSpillWriter::create(spill_directory)?;
            for retained in std::mem::take(atom_chunks) {
                writer.write_chunk(&retained, false)?;
            }
            for retained in std::mem::take(shared_chunks) {
                writer.write_chunk(&retained, true)?;
            }
            writer.write_chunk(&chunk, shared)?;
            *spill_writer = Some(writer);
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

fn read_spilled_rescue_pair_batches(
    path: &Path,
    batch_pairs: usize,
    cancelled: &std::sync::atomic::AtomicBool,
    mut visit: impl FnMut(&[RescuePair]) -> Result<(), PipelineError>,
) -> Result<(), PipelineError> {
    const PAIR_BYTES: usize = std::mem::size_of::<RescuePair>();
    const READER_BUFFER_BYTES: usize = 1024 * 1024;
    let batch_pairs = batch_pairs.max(RESCUE_MATCH_CHUNK_PAIRS);
    let encoded_bytes = batch_pairs
        .checked_mul(PAIR_BYTES)
        .ok_or(MemoryError::Overflow)?;
    let mut reader = BufReader::with_capacity(READER_BUFFER_BYTES, std::fs::File::open(path)?);
    let mut encoded = Vec::new();
    encoded.try_reserve_exact(encoded_bytes).map_err(|error| {
        PipelineError::Allocation(format!(
            "unable to allocate a {encoded_bytes}-byte rescue spill input buffer: {error}"
        ))
    })?;
    encoded.resize(encoded_bytes, 0);
    let mut pairs = Vec::new();
    pairs.try_reserve_exact(batch_pairs).map_err(|error| {
        PipelineError::Allocation(format!(
            "unable to allocate a {batch_pairs}-pair rescue spill decode buffer: {error}"
        ))
    })?;
    loop {
        if cancelled.load(std::sync::atomic::Ordering::Acquire) {
            break;
        }
        let mut filled = 0usize;
        while filled < encoded.len() {
            if cancelled.load(std::sync::atomic::Ordering::Acquire) {
                return Ok(());
            }
            let read = reader.read(&mut encoded[filled..])?;
            if read == 0 {
                break;
            }
            filled += read;
        }
        if filled == 0 {
            break;
        }
        if !filled.is_multiple_of(PAIR_BYTES) {
            return Err(PipelineError::Invariant(format!(
                "truncated rescue spill file {}: {filled} trailing bytes",
                path.display()
            )));
        }
        pairs.clear();
        for pair in encoded[..filled].chunks_exact(PAIR_BYTES) {
            pairs.push((
                u32::from_le_bytes(pair[..4].try_into().expect("four-byte left id")),
                u32::from_le_bytes(pair[4..].try_into().expect("four-byte right id")),
            ));
        }
        visit(&pairs)?;
        if filled < encoded.len() {
            break;
        }
    }
    Ok(())
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
    pair: RescuePair,
    expansion_work: u64,
    sender: &std::sync::mpsc::SyncSender<RescuePlanMessage>,
    shared: bool,
) -> Result<(), PipelineError> {
    if chunk.is_none() {
        *chunk = Some(RescueMatchChunk::new()?);
    }
    let current = chunk.as_mut().expect("rescue match chunk initialized");
    current.pairs.push(pair);
    current.expansion_work = current
        .expansion_work
        .checked_add(expansion_work)
        .ok_or(MemoryError::Overflow)?;
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

fn build_rescue_execution_plan<'a>(
    snapshot: &MetadataSnapshot,
    rescue: &'a RescuePlan,
    lanes: usize,
    memory: &MemoryBroker,
    spill_directory: &Path,
    mut progress: impl FnMut(ProgressEvent),
    mut advisory: impl FnMut(&str),
) -> Result<AdmittedRescueExecutionPlan<'a>, PipelineError> {
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
        .checked_mul(std::mem::size_of::<u32>() as u64)
        .and_then(|bytes| bytes.checked_add(atom_count as u64))
        .ok_or(MemoryError::Overflow)?;
    let contract_count = snapshot.contract_count();
    let rescue_seed_index_bytes = (rescue.shared_contracts.len() as u64)
        .checked_mul(RESCUE_SEED_INDEX_ENTRY_BYTES)
        .and_then(|bytes| bytes.checked_mul(2))
        .and_then(|bytes| {
            (rescue.shared_edges.len() as u64)
                .checked_mul(std::mem::size_of::<RescuePair>() as u64)
                .and_then(|edge_bytes| bytes.checked_add(edge_bytes))
        })
        .ok_or(MemoryError::Overflow)?;
    let rescue_base_bytes = rescue_fixed_bytes
        .checked_add(contract_count as u64)
        .and_then(|bytes| bytes.checked_add(rescue_seed_index_bytes))
        .ok_or(MemoryError::Overflow)?;
    let chunk_bytes = RescueMatchChunk::retained_bytes()?;
    let admission_fixed_bytes = rescue_base_bytes
        .checked_add(chunk_bytes.checked_mul(4).ok_or(MemoryError::Overflow)?)
        .ok_or(MemoryError::Overflow)?;
    let stream_bytes_per_lane = chunk_bytes.checked_mul(3).ok_or(MemoryError::Overflow)?;
    let requested_lanes = lanes.max(1);
    let admitted_lanes = memory.active_lanes(
        requested_lanes,
        admission_fixed_bytes,
        stream_bytes_per_lane,
    );
    let lanes = admitted_lanes.max(1);
    let stream_buffer_bytes = rescue_stream_buffer_bytes(lanes)?;
    let cache_budget = memory
        .available_bytes()
        .saturating_sub(rescue_base_bytes)
        .saturating_sub(stream_buffer_bytes);
    let cache_entries_per_lane = cache_budget
        .checked_div(
            (lanes as u64)
                .checked_mul(RESCUE_PAYLOAD_CACHE_ENTRY_BYTES)
                .ok_or(MemoryError::Overflow)?
                .max(1),
        )
        .unwrap_or(0)
        .min(MAX_RESCUE_PAYLOAD_CACHE_ENTRIES as u64) as usize;
    if lanes < requested_lanes {
        advisory(&format!(
            "metadata rescue concurrency reduced from {requested_lanes} to {lanes} lanes: \
             resident base={admission_fixed_bytes} bytes, stream-per-lane={stream_bytes_per_lane} \
             bytes, used={} bytes, hard_top={} bytes",
            memory.used_bytes(),
            memory.hard_top_bytes()
        ));
    }
    if cache_entries_per_lane < MAX_RESCUE_PAYLOAD_CACHE_ENTRIES {
        advisory(&format!(
            "metadata rescue payload cache reduced from {} to {cache_entries_per_lane} entries \
             per lane to preserve bounded streaming concurrency",
            MAX_RESCUE_PAYLOAD_CACHE_ENTRIES
        ));
    }
    let rescue_cache_bytes = (lanes as u64)
        .checked_mul(cache_entries_per_lane as u64)
        .and_then(|entries| entries.checked_mul(RESCUE_PAYLOAD_CACHE_ENTRY_BYTES))
        .ok_or(MemoryError::Overflow)?;
    let rescue_reservation_bytes = rescue_base_bytes
        .checked_add(rescue_cache_bytes)
        .ok_or(MemoryError::Overflow)?;
    let _rescue_memory = match memory.reserve(rescue_reservation_bytes) {
        Ok(lease) => Some(lease),
        Err(error @ MemoryError::Budget { .. }) => {
            advisory(&format!(
                "metadata rescue measured base state requires {rescue_reservation_bytes} bytes \
                 beyond the accounting envelope ({error}); continuing with the minimum bounded \
                 rescue state and letting actual allocation or I/O report any real failure"
            ));
            None
        }
        Err(error) => return Err(error.into()),
    };
    // Producer-local chunks plus the bounded channel remain admitted even
    // after the retained corpus switches to disk-backed streaming.
    let stream_memory = match memory.reserve(stream_buffer_bytes) {
        Ok(lease) => Some(lease),
        Err(error @ MemoryError::Budget { .. }) => {
            advisory(&format!(
                "metadata rescue cannot admit its minimum {stream_buffer_bytes}-byte bounded \
                 stream buffer ({error}); continuing with one bounded lane from host headroom \
                 and spilling matched pairs immediately"
            ));
            None
        }
        Err(error) => return Err(error.into()),
    };
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
    progress(ProgressEvent::indeterminate(
        ProgressPhase::PrepareRescuePairs,
        prepare_completed,
        WorkUnit::Items,
        ProgressCounters {
            groups: shared_group_count,
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
        |scope| -> Result<AdmittedRescueExecutionPlan<'a>, PipelineError> {
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
                                    for (offset, right_atom) in
                                        (begin as u32..end as u32).enumerate()
                                    {
                                        if offset.is_multiple_of(CANCELLATION_CHECK_PAIRS)
                                            && producer_cancelled
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
                                            cache_entries_per_lane,
                                        );
                                        if exact_match {
                                            let expansion_work =
                                                (atom_contracts(snapshot, left_atom).len() as u64)
                                                    .checked_mul(
                                                        atom_contracts(snapshot, right_atom).len()
                                                            as u64,
                                                    )
                                                    .ok_or(MemoryError::Overflow);
                                            let expansion_work = match expansion_work {
                                                Ok(work) => work,
                                                Err(error) => {
                                                    producer_cancelled.store(
                                                        true,
                                                        std::sync::atomic::Ordering::Release,
                                                    );
                                                    let _ = producer_sender.send(
                                                        RescuePlanMessage::Error(error.into()),
                                                    );
                                                    return;
                                                }
                                            };
                                            if let Err(error) = record_rescue_match(
                                                &mut matches,
                                                (left_atom, right_atom),
                                                expansion_work,
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
                            (0..features.token_member_offsets.len().saturating_sub(1))
                                .into_par_iter()
                                .for_each(|token_id| {
                                    let begin = features.token_member_offsets[token_id] as usize;
                                    let end = features.token_member_offsets[token_id + 1] as usize;
                                    let contracts = &features.token_member_contracts[begin..end];
                                    if contracts.len() < SHARED_LOCAL_ROUTING_MIN_MEMBERS {
                                        return;
                                    }
                                    let sources = &features.token_member_sources[begin..end];
                                    contracts.par_iter().copied().enumerate().for_each(
                                        |(seed_index, seed_contract)| {
                                            if !shared_contract_mask[seed_contract as usize] {
                                                return;
                                            }
                                            let seed_payload = features.source_to_payload
                                                [sources[seed_index] as usize];
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
                                                    let mut payload_scores =
                                                        HashMap::<u32, bool>::new();
                                                    for (offset, (&contract, &source)) in
                                                        contracts.iter().zip(sources).enumerate()
                                                    {
                                                        if offset.is_multiple_of(
                                                            CANCELLATION_CHECK_PAIRS,
                                                        ) && producer_cancelled.load(
                                                            std::sync::atomic::Ordering::Acquire,
                                                        ) {
                                                            return;
                                                        }
                                                        if contract == seed_contract
                                                            || (shared_contract_mask
                                                                [contract as usize]
                                                                && contract < seed_contract)
                                                        {
                                                            continue;
                                                        }
                                                        let payload = features.source_to_payload
                                                            [source as usize];
                                                        let exact_match = bounded_payload_match(
                                                            &mut payload_scores,
                                                            features,
                                                            seed_payload,
                                                            payload,
                                                            cache_entries_per_lane,
                                                        );
                                                        if exact_match {
                                                            if let Err(error) = record_rescue_match(
                                                                &mut matches,
                                                                (seed_contract, contract),
                                                                0,
                                                                &producer_sender,
                                                                true,
                                                            ) {
                                                                producer_cancelled.store(
                                                            true,
                                                            std::sync::atomic::Ordering::Release,
                                                        );
                                                                let _ = producer_sender.send(
                                                                    RescuePlanMessage::Error(error),
                                                                );
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
                                                    let _ = producer_sender.send(
                                                        RescuePlanMessage::RowDone(
                                                            take_rescue_plan_work(
                                                                &mut pending_work,
                                                            ),
                                                        ),
                                                    );
                                                });
                                        },
                                    );
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
            let mut raw_contract_expansion_visits = 0u64;
            let mut spill_writer = None;
            let mut first_error = None;
            for message in receiver {
                match message {
                    RescuePlanMessage::Work(work) => {
                        completed = completed.saturating_add(work).min(score_visits);
                    }
                    RescuePlanMessage::AtomMatches(chunk) => {
                        matched_atom_count = matched_atom_count.saturating_add(chunk.pairs.len());
                        raw_contract_expansion_visits = raw_contract_expansion_visits
                            .checked_add(chunk.expansion_work)
                            .ok_or(MemoryError::Overflow)?;
                        if first_error.is_none() {
                            if let Err(error) = retain_or_spill_rescue_chunk(
                                chunk,
                                false,
                                memory,
                                spill_directory,
                                &mut atom_chunks,
                                &mut shared_chunks,
                                &mut spill_writer,
                                &mut advisory,
                            ) {
                                cancelled.store(true, std::sync::atomic::Ordering::Release);
                                first_error = Some(error);
                            }
                        }
                    }
                    RescuePlanMessage::SharedMatches(chunk) => {
                        matched_shared_count =
                            matched_shared_count.saturating_add(chunk.pairs.len());
                        if first_error.is_none() {
                            if let Err(error) = retain_or_spill_rescue_chunk(
                                chunk,
                                true,
                                memory,
                                spill_directory,
                                &mut atom_chunks,
                                &mut shared_chunks,
                                &mut spill_writer,
                                &mut advisory,
                            ) {
                                cancelled.store(true, std::sync::atomic::Ordering::Release);
                                first_error = Some(error);
                            }
                        }
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
            let matched_atom_pair_count =
                u64::try_from(matched_atom_count).map_err(|_| MemoryError::Overflow)?;
            let matched_shared_edge_count =
                u64::try_from(matched_shared_count).map_err(|_| MemoryError::Overflow)?;
            let raw_match_count = matched_atom_pair_count
                .checked_add(matched_shared_edge_count)
                .ok_or(MemoryError::Overflow)?;
            progress(ProgressEvent::indeterminate(
                ProgressPhase::FinalizeRescuePlan,
                0,
                WorkUnit::Items,
                ProgressCounters {
                    matched: raw_match_count,
                    ..ProgressCounters::default()
                },
            ));

            if let Some(writer) = spill_writer {
                let files = writer.finish()?;
                progress(ProgressEvent::indeterminate(
                    ProgressPhase::FinalizeRescuePlan,
                    raw_match_count,
                    WorkUnit::Items,
                    ProgressCounters {
                        matched: raw_match_count,
                        ..ProgressCounters::default()
                    },
                ));
                return Ok(AdmittedRescueExecutionPlan {
                    plan: RescueExecutionPlan {
                        atom_score_visits,
                        contract_expansion_visits: raw_contract_expansion_visits,
                        shared_score_visits,
                        matched_atom_pairs: Vec::new(),
                        matched_shared_edges: Vec::new(),
                        fallback: Some(RescueFallbackStorage::Spilled {
                            files,
                            base_shared_edges: &rescue.shared_edges,
                        }),
                        matched_shared_edge_count,
                        stream_buffer_bytes,
                    },
                    _match_memory: None,
                    _stream_memory: stream_memory,
                });
            }

            if raw_match_count > MAX_RESCUE_GLOBAL_SORT_PAIRS {
                // At production scale, flatten + comparison sort is both a
                // second full pair corpus and O(N log N).  Atom matches are
                // unique by construction; shared duplicates are connectivity
                // idempotent downstream.  Keep the hot chunks resident and
                // locally deduplicate shared chunks in parallel instead.
                pool.install(|| {
                    shared_chunks.par_iter_mut().for_each(|chunk| {
                        chunk.pairs.sort_unstable();
                        chunk.pairs.dedup();
                    });
                });
                let matched_shared_edge_count = (rescue.shared_edges.len() as u64)
                    .checked_add(
                        shared_chunks
                            .iter()
                            .map(|chunk| chunk.pairs.len() as u64)
                            .try_fold(0u64, |total, count| total.checked_add(count))
                            .ok_or(MemoryError::Overflow)?,
                    )
                    .ok_or(MemoryError::Overflow)?;
                let streamed_match_count = matched_atom_pair_count
                    .checked_add(matched_shared_edge_count)
                    .ok_or(MemoryError::Overflow)?;
                advisory(&format!(
                    "metadata rescue produced {raw_match_count} matched pairs; bypassing the \
                     O(N log N) global sort above {MAX_RESCUE_GLOBAL_SORT_PAIRS} pairs and \
                     consuming resident chunks directly"
                ));
                progress(ProgressEvent::indeterminate(
                    ProgressPhase::FinalizeRescuePlan,
                    streamed_match_count,
                    WorkUnit::Items,
                    ProgressCounters {
                        matched: streamed_match_count,
                        ..ProgressCounters::default()
                    },
                ));
                return Ok(AdmittedRescueExecutionPlan {
                    plan: RescueExecutionPlan {
                        atom_score_visits,
                        contract_expansion_visits: raw_contract_expansion_visits,
                        shared_score_visits,
                        matched_atom_pairs: Vec::new(),
                        matched_shared_edges: Vec::new(),
                        fallback: Some(RescueFallbackStorage::InMemoryChunks {
                            atom_chunks,
                            shared_chunks,
                            base_shared_edges: &rescue.shared_edges,
                        }),
                        matched_shared_edge_count,
                        stream_buffer_bytes,
                    },
                    _match_memory: None,
                    _stream_memory: stream_memory,
                });
            }

            // The source chunks already hold their own leases.  The destination
            // vectors together contain exactly one pair-width per result, so a
            // second multiplier here double-counts the destination and can
            // reject a plan that is safely below the hard top.
            let final_match_bytes =
                rescue_final_match_bytes(matched_atom_pair_count, matched_shared_edge_count)?;
            let match_memory = match memory.reserve(final_match_bytes) {
                Ok(lease) => Some(lease),
                Err(error @ MemoryError::Budget { .. }) => {
                    advisory(&format!(
                        "metadata rescue flatten needs {final_match_bytes} additional resident \
                         bytes but cannot be admitted ({error}); retaining the already admitted \
                         chunks and streaming them directly without a global sort"
                    ));
                    None
                }
                Err(error) => return Err(error.into()),
            };

            if match_memory.is_none() {
                progress(ProgressEvent::indeterminate(
                    ProgressPhase::FinalizeRescuePlan,
                    raw_match_count,
                    WorkUnit::Items,
                    ProgressCounters {
                        matched: raw_match_count,
                        ..ProgressCounters::default()
                    },
                ));
                return Ok(AdmittedRescueExecutionPlan {
                    plan: RescueExecutionPlan {
                        atom_score_visits,
                        contract_expansion_visits: raw_contract_expansion_visits,
                        shared_score_visits,
                        matched_atom_pairs: Vec::new(),
                        matched_shared_edges: Vec::new(),
                        fallback: Some(RescueFallbackStorage::InMemoryChunks {
                            atom_chunks,
                            shared_chunks,
                            base_shared_edges: &rescue.shared_edges,
                        }),
                        matched_shared_edge_count,
                        stream_buffer_bytes,
                    },
                    _match_memory: None,
                    _stream_memory: stream_memory,
                });
            }

            let allocation = (|| {
                let mut atom_pairs = Vec::new();
                atom_pairs.try_reserve_exact(matched_atom_count)?;
                let mut shared_edges = Vec::new();
                shared_edges.try_reserve_exact(matched_shared_count)?;
                Ok::<_, std::collections::TryReserveError>((atom_pairs, shared_edges))
            })();
            let (mut matched_atom_pairs, mut matched_shared_edges) = match allocation {
                Ok(vectors) => vectors,
                Err(error) => {
                    drop(match_memory);
                    advisory(&format!(
                        "metadata rescue flatten was admitted for {final_match_bytes} bytes but \
                         the contiguous allocation failed ({error}); retaining the admitted \
                         chunks and streaming them directly without a global sort"
                    ));
                    progress(ProgressEvent::indeterminate(
                        ProgressPhase::FinalizeRescuePlan,
                        raw_match_count,
                        WorkUnit::Items,
                        ProgressCounters {
                            matched: raw_match_count,
                            ..ProgressCounters::default()
                        },
                    ));
                    return Ok(AdmittedRescueExecutionPlan {
                        plan: RescueExecutionPlan {
                            atom_score_visits,
                            contract_expansion_visits: raw_contract_expansion_visits,
                            shared_score_visits,
                            matched_atom_pairs: Vec::new(),
                            matched_shared_edges: Vec::new(),
                            fallback: Some(RescueFallbackStorage::InMemoryChunks {
                                atom_chunks,
                                shared_chunks,
                                base_shared_edges: &rescue.shared_edges,
                            }),
                            matched_shared_edge_count,
                            stream_buffer_bytes,
                        },
                        _match_memory: None,
                        _stream_memory: stream_memory,
                    });
                }
            };
            for chunk in atom_chunks {
                matched_atom_pairs.extend(chunk.pairs);
            }
            matched_shared_edges.extend_from_slice(&rescue.shared_edges);
            for chunk in shared_chunks {
                matched_shared_edges.extend(chunk.pairs);
            }
            pool.install(|| {
                rayon::join(
                    || matched_atom_pairs.par_sort_unstable(),
                    || matched_shared_edges.par_sort_unstable(),
                )
            });
            matched_atom_pairs.dedup();
            matched_shared_edges.dedup();
            let matched_atom_pair_count =
                u64::try_from(matched_atom_pairs.len()).map_err(|_| MemoryError::Overflow)?;
            let matched_shared_edge_count =
                u64::try_from(matched_shared_edges.len()).map_err(|_| MemoryError::Overflow)?;
            let mut finalize_completed = matched_atom_pair_count
                .checked_add(matched_shared_edge_count)
                .ok_or(MemoryError::Overflow)?;
            progress(ProgressEvent::indeterminate(
                ProgressPhase::FinalizeRescuePlan,
                finalize_completed,
                WorkUnit::Items,
                ProgressCounters {
                    matched: finalize_completed,
                    ..ProgressCounters::default()
                },
            ));

            let contract_expansion_visits = pool.install(|| {
                matched_atom_pairs
                    .par_iter()
                    .try_fold(
                        || 0u64,
                        |total, &(left_atom, right_atom)| {
                            let work = (atom_contracts(snapshot, left_atom).len() as u64)
                                .checked_mul(atom_contracts(snapshot, right_atom).len() as u64)
                                .ok_or(MemoryError::Overflow)?;
                            total.checked_add(work).ok_or(MemoryError::Overflow)
                        },
                    )
                    .try_reduce(
                        || 0u64,
                        |left, right| left.checked_add(right).ok_or(MemoryError::Overflow),
                    )
            })?;
            finalize_completed = finalize_completed.saturating_add(matched_atom_pair_count);
            progress(ProgressEvent::indeterminate(
                ProgressPhase::FinalizeRescuePlan,
                finalize_completed,
                WorkUnit::Items,
                ProgressCounters {
                    matched: matched_atom_pair_count.saturating_add(matched_shared_edge_count),
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
                    fallback: None,
                    matched_shared_edge_count,
                    stream_buffer_bytes: 0,
                },
                _match_memory: match_memory,
                _stream_memory: None,
            })
        },
    )
}

fn bounded_payload_match(
    cache: &mut HashMap<u32, bool>,
    features: &crate::encode::FeatureView,
    left_payload: u32,
    right_payload: u32,
    max_cache_entries: usize,
) -> bool {
    if max_cache_entries == 0 {
        return score_pair(features, left_payload, right_payload) == PairScoreDecision::ExactMatch;
    }
    if let Some(&decision) = cache.get(&right_payload) {
        return decision;
    }
    let decision =
        score_pair(features, left_payload, right_payload) == PairScoreDecision::ExactMatch;
    if cache.len() < max_cache_entries {
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

#[allow(clippy::too_many_arguments)]
fn append_rescue_atom_pairs_parallel(
    pairs: &[RescuePair],
    snapshot: &MetadataSnapshot,
    collectors: &ScopeCollectorBroker,
    chain_count: usize,
    sender: &std::sync::mpsc::SyncSender<RescueExpansionMessage>,
    cancelled: &std::sync::atomic::AtomicBool,
) {
    const TILE_SIDE: usize = 256;
    const EDGE_BATCH: usize = 4_096;
    let features = snapshot.features();
    pairs.par_iter().for_each(|&(left_atom, right_atom)| {
        if cancelled.load(std::sync::atomic::Ordering::Acquire) {
            return;
        }
        let left_contracts = atom_contracts(snapshot, left_atom);
        let right_contracts = atom_contracts(snapshot, right_atom);
        let left_tiles = left_contracts.len().div_ceil(TILE_SIDE);
        let right_tiles = right_contracts.len().div_ceil(TILE_SIDE);
        let tile_count = left_tiles.saturating_mul(right_tiles);
        (0..tile_count).into_par_iter().for_each(|tile| {
            if cancelled.load(std::sync::atomic::Ordering::Acquire) {
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
                    if left != right && !contracts_share_retained_token(features, left, right) {
                        edges.push(Edge::new(left, right));
                    }
                }
            }
            if !edges.is_empty() {
                if let Err(error) =
                    collectors.push_edges_by_chain(&features.contract_chain, chain_count, edges)
                {
                    let _ = sender.send(RescueExpansionMessage::Error(error));
                    cancelled.store(true, std::sync::atomic::Ordering::Release);
                    return;
                }
            }
            let work = (left_end - left_begin).saturating_mul(right_end - right_begin) as u64;
            if sender.send(RescueExpansionMessage::Work(work)).is_err() {
                cancelled.store(true, std::sync::atomic::Ordering::Release);
            }
        });
    });
}

fn append_rescue_shared_pairs_parallel(
    pairs: &[RescuePair],
    features: &crate::encode::FeatureView,
    collectors: &ScopeCollectorBroker,
    chain_count: usize,
    sender: &std::sync::mpsc::SyncSender<RescueExpansionMessage>,
    cancelled: &std::sync::atomic::AtomicBool,
) {
    const EDGE_BATCH: usize = 4_096;
    pairs.par_chunks(EDGE_BATCH).for_each(|chunk| {
        if cancelled.load(std::sync::atomic::Ordering::Acquire) {
            return;
        }
        let edges = chunk
            .iter()
            .map(|&(left, right)| Edge::new(left, right))
            .collect::<Vec<_>>();
        if let Err(error) =
            collectors.push_edges_by_chain(&features.contract_chain, chain_count, edges)
        {
            let _ = sender.send(RescueExpansionMessage::Error(error));
            cancelled.store(true, std::sync::atomic::Ordering::Release);
            return;
        }
        if sender
            .send(RescueExpansionMessage::Work(chunk.len() as u64))
            .is_err()
        {
            cancelled.store(true, std::sync::atomic::Ordering::Release);
        }
    });
}

fn append_rescue_edges(
    snapshot: &MetadataSnapshot,
    plan: &RescueExecutionPlan<'_>,
    worker_pool: &rayon::ThreadPool,
    collectors: &ScopeCollectorBroker,
    memory: &MemoryBroker,
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
    let features = snapshot.features();
    let extra_stream_memory =
        if matches!(&plan.fallback, Some(RescueFallbackStorage::Spilled { .. })) {
            let bytes = memory.available_bytes().min(MAX_RESCUE_STREAM_IO_BYTES);
            if bytes == 0 {
                None
            } else {
                memory.reserve(bytes).ok()
            }
        } else {
            None
        };
    let spill_batch_pairs = rescue_spill_batch_pairs(
        plan.stream_buffer_bytes.saturating_add(
            extra_stream_memory
                .as_ref()
                .map(MemoryLease::bytes)
                .unwrap_or(0),
        ),
    );
    let cancelled = std::sync::atomic::AtomicBool::new(false);
    let (sender, receiver) = std::sync::mpsc::sync_channel::<RescueExpansionMessage>(
        worker_pool.current_num_threads().max(1) * 2,
    );
    std::thread::scope(|scope| -> Result<(), PipelineError> {
        let producer_sender = sender.clone();
        let producer_cancelled = &cancelled;
        let producer = scope.spawn(move || {
            worker_pool.install(|| match &plan.fallback {
                None => {
                    rayon::join(
                        || {
                            append_rescue_atom_pairs_parallel(
                                &plan.matched_atom_pairs,
                                snapshot,
                                collectors,
                                chain_count,
                                &producer_sender,
                                producer_cancelled,
                            )
                        },
                        || {
                            append_rescue_shared_pairs_parallel(
                                &plan.matched_shared_edges,
                                features,
                                collectors,
                                chain_count,
                                &producer_sender,
                                producer_cancelled,
                            )
                        },
                    );
                }
                Some(RescueFallbackStorage::InMemoryChunks {
                    atom_chunks,
                    shared_chunks,
                    base_shared_edges,
                }) => {
                    rayon::join(
                        || {
                            for chunk in atom_chunks {
                                append_rescue_atom_pairs_parallel(
                                    &chunk.pairs,
                                    snapshot,
                                    collectors,
                                    chain_count,
                                    &producer_sender,
                                    producer_cancelled,
                                );
                            }
                        },
                        || {
                            rayon::join(
                                || {
                                    append_rescue_shared_pairs_parallel(
                                        base_shared_edges,
                                        features,
                                        collectors,
                                        chain_count,
                                        &producer_sender,
                                        producer_cancelled,
                                    )
                                },
                                || {
                                    for chunk in shared_chunks {
                                        append_rescue_shared_pairs_parallel(
                                            &chunk.pairs,
                                            features,
                                            collectors,
                                            chain_count,
                                            &producer_sender,
                                            producer_cancelled,
                                        );
                                    }
                                },
                            );
                        },
                    );
                }
                Some(RescueFallbackStorage::Spilled {
                    files,
                    base_shared_edges,
                }) => {
                    rayon::join(
                        || {
                            if let Err(error) = read_spilled_rescue_pair_batches(
                                &files.atom_path,
                                spill_batch_pairs,
                                producer_cancelled,
                                |pairs| {
                                    append_rescue_atom_pairs_parallel(
                                        pairs,
                                        snapshot,
                                        collectors,
                                        chain_count,
                                        &producer_sender,
                                        producer_cancelled,
                                    );
                                    Ok(())
                                },
                            ) {
                                let _ = producer_sender.send(RescueExpansionMessage::Error(error));
                                producer_cancelled
                                    .store(true, std::sync::atomic::Ordering::Release);
                            }
                        },
                        || {
                            rayon::join(
                                || {
                                    append_rescue_shared_pairs_parallel(
                                        base_shared_edges,
                                        features,
                                        collectors,
                                        chain_count,
                                        &producer_sender,
                                        producer_cancelled,
                                    )
                                },
                                || {
                                    if let Err(error) = read_spilled_rescue_pair_batches(
                                        &files.shared_path,
                                        spill_batch_pairs,
                                        producer_cancelled,
                                        |pairs| {
                                            append_rescue_shared_pairs_parallel(
                                                pairs,
                                                features,
                                                collectors,
                                                chain_count,
                                                &producer_sender,
                                                producer_cancelled,
                                            );
                                            Ok(())
                                        },
                                    ) {
                                        let _ = producer_sender
                                            .send(RescueExpansionMessage::Error(error));
                                        producer_cancelled
                                            .store(true, std::sync::atomic::Ordering::Release);
                                    }
                                },
                            );
                        },
                    );
                }
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
        progress(ProgressEvent::determinate(
            ProgressPhase::RescuePairs,
            completed,
            completed,
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
    group_index_budget_bytes: u64,
    max_cache_entries_per_lane: usize,
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
                    let indexed_group = contracts.len() >= SHARED_LOCAL_ROUTING_MIN_MEMBERS;
                    let group_index_bytes = match shared_group_index_bytes(f, sources) {
                        Ok(bytes) => bytes,
                        Err(error) => {
                            let _ = worker_sender.send(SharedMessage::Error(error));
                            producer_cancelled.store(true, std::sync::atomic::Ordering::Release);
                            return;
                        }
                    };
                    if indexed_group && group_index_bytes <= group_index_budget_bytes {
                        const LOCAL_TILE_MEMBERS: usize = 256;
                        let member_payloads = sources
                            .iter()
                            .map(|&source| f.source_to_payload[source as usize])
                            .collect::<Vec<_>>();
                        let cache_payload_scores = payloads_have_duplicates(&member_payloads);
                        let sketches =
                            build_base_equivalent_atom_sketches_from_feature_view_parallel(
                                f,
                                &member_payloads,
                            );
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
                                let mut payload_scores = SharedPayloadScoreCache::new(
                                    cache_payload_scores,
                                    max_cache_entries_per_lane,
                                );
                                let _ = plan.visit_tile(&sketches, &tile, |i, j| {
                                    if failed {
                                        return false;
                                    }
                                    if pending_work.is_multiple_of(CANCELLATION_CHECK_PAIRS as u64)
                                        && producer_cancelled
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
                    let member_payloads = if indexed_group {
                        None
                    } else {
                        Some(
                            sources
                                .iter()
                                .map(|&source| f.source_to_payload[source as usize])
                                .collect::<Vec<_>>(),
                        )
                    };
                    let cache_payload_scores = member_payloads
                        .as_deref()
                        .map(payloads_have_duplicates)
                        .unwrap_or(max_cache_entries_per_lane != 0);
                    let mut edges = Vec::with_capacity(EDGE_BATCH);
                    let mut pending_work = 0u64;
                    let mut failed = false;
                    let mut payload_scores = SharedPayloadScoreCache::new(
                        cache_payload_scores,
                        max_cache_entries_per_lane,
                    );
                    let mut visit = |i: usize, j: usize| -> bool {
                        if failed {
                            return false;
                        }
                        pending_work = pending_work.saturating_add(1);
                        let (left_payload, right_payload) = member_payloads
                            .as_deref()
                            .map(|payloads| (payloads[i], payloads[j]))
                            .unwrap_or_else(|| {
                                (
                                    f.source_to_payload[sources[i] as usize],
                                    f.source_to_payload[sources[j] as usize],
                                )
                            });
                        if let Some(edge) = shared_pair_edge_for_payloads(
                            f,
                            contracts[i],
                            contracts[j],
                            left_payload,
                            right_payload,
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
                        !failed
                    };
                    'pairs: for i in 0..contracts.len() {
                        if producer_cancelled.load(std::sync::atomic::Ordering::Acquire) {
                            failed = true;
                            break;
                        }
                        for j in i + 1..contracts.len() {
                            if !visit(i, j) {
                                break 'pairs;
                            }
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
    payload_scores: &mut SharedPayloadScoreCache,
) -> Option<Edge> {
    shared_pair_edge_for_payloads(
        f,
        contracts[i],
        contracts[j],
        payloads[i],
        payloads[j],
        payload_scores,
    )
}

fn shared_pair_edge_for_payloads(
    f: &crate::encode::FeatureView,
    left: u32,
    right: u32,
    left_payload: u32,
    right_payload: u32,
    payload_scores: &mut SharedPayloadScoreCache,
) -> Option<Edge> {
    if left == right {
        return None;
    }
    let exact_match = payload_scores.exact_match(f, left_payload, right_payload);
    if exact_match {
        Some(Edge::new(left, right))
    } else {
        None
    }
}

struct SharedPayloadScoreCache {
    decisions: HashMap<u64, bool>,
    max_entries: usize,
}

impl SharedPayloadScoreCache {
    fn new(enabled: bool, max_entries: usize) -> Self {
        Self {
            decisions: HashMap::new(),
            max_entries: if enabled { max_entries } else { 0 },
        }
    }

    fn exact_match(
        &mut self,
        features: &crate::encode::FeatureView,
        left_payload: u32,
        right_payload: u32,
    ) -> bool {
        if self.max_entries == 0 {
            return score_pair(features, left_payload, right_payload)
                == PairScoreDecision::ExactMatch;
        }
        let key = payload_pair_key(left_payload, right_payload);
        if let Some(&decision) = self.decisions.get(&key) {
            return decision;
        }
        let decision =
            score_pair(features, left_payload, right_payload) == PairScoreDecision::ExactMatch;
        if self.decisions.len() < self.max_entries {
            self.decisions.insert(key, decision);
        }
        decision
    }
}

fn payloads_have_duplicates(payloads: &[u32]) -> bool {
    let mut seen = HashSet::new();
    if seen.try_reserve(payloads.len()).is_err() {
        return false;
    }
    payloads.iter().any(|payload| !seen.insert(*payload))
}

fn payload_pair_key(left: u32, right: u32) -> u64 {
    let (left, right) = (left.min(right), left.max(right));
    (u64::from(left) << 32) | u64::from(right)
}

fn chain_pair_index(left: usize, right: usize, chain_count: usize) -> usize {
    let (left, right) = (left.min(right), left.max(right));
    left * (2 * chain_count - left - 1) / 2 + (right - left - 1)
}

fn chain_pairs(chain_count: usize) -> Vec<(u32, u32)> {
    (0..chain_count)
        .flat_map(|left| (left + 1..chain_count).map(move |right| (left as u32, right as u32)))
        .collect()
}

fn release_reused_component_runs(
    scopes: &mut [ComponentScopePlan],
    worker_pool: &rayon::ThreadPool,
) {
    worker_pool.install(|| {
        scopes.par_iter_mut().for_each(|scope| {
            if !scope.needs_rebuild {
                scope.runs = Vec::new();
            }
        });
    });
}

fn build_summary_rows_with_progress(
    snapshot: &MetadataSnapshot,
    scopes: &ScopeComponents,
    chain_count: usize,
    build_plan: SummaryBuildPlan<'_>,
    worker_pool: &rayon::ThreadPool,
    progress: &mut impl FnMut(ProgressEvent),
    advisory: &mut impl FnMut(&str),
) -> Result<Vec<MetadataSummaryRow>, PipelineError> {
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
    let primary_outcomes = std::thread::scope(|thread_scope| {
        let producer_sender = sender.clone();
        let producer = thread_scope.spawn(move || {
            worker_pool.install(|| -> Result<_, PipelineError> {
                let intra_sender = producer_sender.clone();
                let summarize_intra = || {
                    summary_stats_by_chain(
                        snapshot,
                        &scopes.intra_roots,
                        chain_count,
                        false,
                        SummaryScopeExecution {
                            mode: SummaryExecutionMode::from_fast_lanes(
                                build_plan.memory.parallel_primary_scopes,
                            ),
                            stream: SummaryStreamContext {
                                scratch_root: build_plan.stream_scratch_root,
                                scope_label: "intra",
                                scratch_bytes: build_plan.memory.stream_scratch_bytes,
                            },
                        },
                        &|delta| {
                            let _ = intra_sender.send(delta);
                        },
                    )
                };
                let summarize_cross = || {
                    if chain_count > 1 {
                        summary_stats_by_chain(
                            snapshot,
                            &scopes.cross_roots,
                            chain_count,
                            true,
                            SummaryScopeExecution {
                                mode: SummaryExecutionMode::from_fast_lanes(
                                    build_plan.memory.parallel_primary_scopes,
                                ),
                                stream: SummaryStreamContext {
                                    scratch_root: build_plan.stream_scratch_root,
                                    scope_label: "cross",
                                    scratch_bytes: build_plan.memory.stream_scratch_bytes,
                                },
                            },
                            &|delta| {
                                let _ = producer_sender.send(delta);
                            },
                        )
                        .map(Some)
                    } else {
                        Ok(None)
                    }
                };
                if build_plan.memory.parallel_primary_scopes >= 2 && chain_count > 1 {
                    let (intra, cross) = rayon::join(summarize_intra, summarize_cross);
                    Ok((intra?, cross?))
                } else {
                    Ok((summarize_intra()?, summarize_cross()?))
                }
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
    let (intra_rows, cross_stats) = primary_outcomes?;
    if let Some(message) = intra_rows.advisory.as_deref() {
        advisory(message);
    }
    if let Some(message) = cross_stats
        .as_ref()
        .and_then(|outcome| outcome.advisory.as_deref())
    {
        advisory(message);
    }
    for chain in 0..chain_count {
        rows.push(summary_row_from_stats(
            snapshot,
            "intra_chain",
            chain,
            None,
            intra_rows.stats[chain],
        ));
        if let Some(outcome) = cross_stats.as_ref() {
            rows.push(summary_row_from_stats(
                snapshot,
                "cross_chain_summary",
                chain,
                None,
                outcome.stats[chain],
            ));
        }
    }
    let (sender, receiver) = std::sync::mpsc::sync_channel::<u64>(channel_capacity);
    let pair_rows = std::thread::scope(|thread_scope| {
        let producer_sender = sender.clone();
        let producer = thread_scope.spawn(move || {
            let mut pair_rows = Vec::with_capacity(scopes.chain_pair_roots.len());
            for wave in scopes
                .chain_pair_roots
                .chunks(build_plan.memory.parallel_pair_scopes.max(1))
            {
                let mut wave_rows = worker_pool.install(|| -> Result<_, PipelineError> {
                    wave.par_iter()
                        .map(|pair| {
                            let outcome = summary_stats_for_chain_pair(
                                snapshot,
                                scopes,
                                pair,
                                SummaryScopeExecution {
                                    mode: SummaryExecutionMode::from_fast_lanes(
                                        build_plan.memory.parallel_pair_scopes,
                                    ),
                                    stream: SummaryStreamContext {
                                        scratch_root: build_plan.stream_scratch_root,
                                        scope_label: &format!(
                                            "pair-{}-{}",
                                            pair.left_chain, pair.right_chain
                                        ),
                                        scratch_bytes: build_plan.memory.stream_scratch_bytes,
                                    },
                                },
                                &|delta| {
                                    let _ = producer_sender.send(delta);
                                },
                            )?;
                            Ok((
                                [
                                    summary_row_from_stats(
                                        snapshot,
                                        "chain_matrix",
                                        pair.left_chain as usize,
                                        Some(pair.right_chain as usize),
                                        outcome.stats[0],
                                    ),
                                    summary_row_from_stats(
                                        snapshot,
                                        "chain_matrix",
                                        pair.right_chain as usize,
                                        Some(pair.left_chain as usize),
                                        outcome.stats[1],
                                    ),
                                ],
                                outcome.advisory,
                            ))
                        })
                        .collect::<Result<Vec<_>, PipelineError>>()
                })?;
                pair_rows.append(&mut wave_rows);
            }
            Ok::<_, PipelineError>(pair_rows)
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
    for (pair, message) in pair_rows? {
        if let Some(message) = message.as_deref() {
            advisory(message);
        }
        for row in pair {
            rows.push(row);
        }
    }
    match std::fs::remove_dir(build_plan.stream_scratch_root) {
        Ok(()) => {}
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::DirectoryNotEmpty
            ) => {}
        Err(error) => return Err(error.into()),
    }
    Ok(rows)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SummaryEntry {
    root: u32,
    chain: u32,
    nfts: i64,
}

impl SummaryEntry {
    const EXCLUDED: Self = Self {
        root: u32::MAX,
        chain: u32::MAX,
        nfts: 0,
    };

    fn key(self) -> (u32, u32) {
        (self.root, self.chain)
    }
}

#[derive(Clone, Copy)]
enum SummaryExecutionMode {
    Fast,
    Streaming,
}

impl SummaryExecutionMode {
    fn from_fast_lanes(lanes: usize) -> Self {
        if lanes == 0 {
            Self::Streaming
        } else {
            Self::Fast
        }
    }
}

#[derive(Clone, Copy)]
struct SummaryStreamContext<'a> {
    scratch_root: &'a Path,
    scope_label: &'a str,
    scratch_bytes: u64,
}

#[derive(Clone, Copy)]
struct SummaryScopeExecution<'a> {
    mode: SummaryExecutionMode,
    stream: SummaryStreamContext<'a>,
}

#[derive(Clone, Copy)]
enum SummaryChainSelection {
    All(usize),
    Pair { left: usize, right: usize },
}

impl SummaryChainSelection {
    fn includes(self, chain: usize) -> bool {
        match self {
            Self::All(chain_count) => chain < chain_count,
            Self::Pair { left, right } => chain == left || chain == right,
        }
    }

    fn for_each(self, mut visit: impl FnMut(usize)) {
        match self {
            Self::All(chain_count) => {
                for chain in 0..chain_count {
                    visit(chain);
                }
            }
            Self::Pair { left, right } => {
                visit(left);
                if right != left {
                    visit(right);
                }
            }
        }
    }
}

#[derive(Clone, Copy)]
enum SummaryRootView<'a> {
    Global {
        roots: &'a [u32],
        selection: SummaryChainSelection,
    },
    PairLocal {
        roots: &'a [u32],
        left_chain: usize,
        right_chain: usize,
        left_contracts: &'a [u32],
        right_contracts: &'a [u32],
    },
}

impl<'a> SummaryRootView<'a> {
    fn global(roots: &'a [u32], selection: SummaryChainSelection) -> Self {
        Self::Global { roots, selection }
    }

    fn pair(
        roots: &'a [u32],
        left_chain: usize,
        right_chain: usize,
        left_contracts: &'a [u32],
        right_contracts: &'a [u32],
    ) -> Result<Self, PipelineError> {
        if roots.len() != left_contracts.len().saturating_add(right_contracts.len()) {
            return Err(PipelineError::Invariant(
                "pair-local roots do not match the two chain CSR slices".into(),
            ));
        }
        Ok(Self::PairLocal {
            roots,
            left_chain,
            right_chain,
            left_contracts,
            right_contracts,
        })
    }

    fn len(self) -> usize {
        match self {
            Self::Global { roots, .. } | Self::PairLocal { roots, .. } => roots.len(),
        }
    }

    fn selection(self) -> SummaryChainSelection {
        match self {
            Self::Global { selection, .. } => selection,
            Self::PairLocal {
                left_chain,
                right_chain,
                ..
            } => SummaryChainSelection::Pair {
                left: left_chain,
                right: right_chain,
            },
        }
    }

    fn entry(self, snapshot: &MetadataSnapshot, local: usize) -> Option<SummaryEntry> {
        let features = snapshot.features();
        match self {
            Self::Global { roots, selection } => {
                let &root = roots.get(local)?;
                let &chain = features.contract_chain.get(local)?;
                selection.includes(chain as usize).then(|| SummaryEntry {
                    root,
                    chain,
                    nfts: i64::try_from(features.contract_weight[local]).unwrap_or(i64::MAX),
                })
            }
            Self::PairLocal {
                roots,
                left_chain,
                right_chain,
                left_contracts,
                right_contracts,
            } => {
                let &root = roots.get(local)?;
                let (contract, chain) = if local < left_contracts.len() {
                    (left_contracts[local] as usize, left_chain)
                } else {
                    (
                        *right_contracts.get(local - left_contracts.len())? as usize,
                        right_chain,
                    )
                };
                Some(SummaryEntry {
                    root,
                    chain: chain as u32,
                    nfts: i64::try_from(*features.contract_weight.get(contract)?)
                        .unwrap_or(i64::MAX),
                })
            }
        }
    }
}

struct SummaryComputation<T> {
    stats: T,
    advisory: Option<String>,
}

#[derive(Clone, Copy)]
struct SummaryStreamReport {
    requested_chunk_entries: usize,
    actual_chunk_entries: usize,
    emergency_no_alloc: bool,
}

fn allocate_summary_entries(len: usize) -> Result<Vec<SummaryEntry>, String> {
    let mut entries = Vec::new();
    entries
        .try_reserve_exact(len)
        .map_err(|error| format!("unable to allocate {len} summary entries: {error}"))?;
    if entries.capacity() != len {
        return Err(format!(
            "allocator returned capacity {} for {len} admitted summary entries",
            entries.capacity()
        ));
    }
    entries.resize(len, SummaryEntry::EXCLUDED);
    Ok(entries)
}

fn summary_entries(
    snapshot: &MetadataSnapshot,
    roots: SummaryRootView<'_>,
    on_work: &(impl Fn(u64) + Sync),
) -> Result<Vec<SummaryEntry>, String> {
    const PROGRESS_CHUNK: u64 = 65_536;
    let mut entries = allocate_summary_entries(roots.len())?;
    entries
        .par_chunks_mut(PROGRESS_CHUNK as usize)
        .enumerate()
        .for_each(|(chunk_index, output)| {
            let begin = chunk_index.saturating_mul(PROGRESS_CHUNK as usize);
            for (offset, entry) in output.iter_mut().enumerate() {
                if let Some(value) = roots.entry(snapshot, begin.saturating_add(offset)) {
                    *entry = value;
                }
            }
            on_work(output.len() as u64);
        });
    entries.retain(|entry| *entry != SummaryEntry::EXCLUDED);
    Ok(entries)
}

fn summary_stats_by_chain(
    snapshot: &MetadataSnapshot,
    roots: &[u32],
    chain_count: usize,
    require_secondary: bool,
    execution: SummaryScopeExecution<'_>,
    on_work: &(impl Fn(u64) + Sync),
) -> Result<SummaryComputation<Vec<SummaryStats>>, PipelineError> {
    let mut stats = vec![SummaryStats::default(); chain_count];
    let selection = SummaryChainSelection::All(chain_count);
    let roots = SummaryRootView::global(roots, selection);
    let advisory = match execution.mode {
        SummaryExecutionMode::Fast => match summary_entries(snapshot, roots, on_work) {
            Ok(mut entries) => {
                entries.par_sort_unstable_by_key(|entry| entry.key());
                summarize_sorted_entries(&entries, require_secondary, |chain, summary| {
                    if let Some(target) = stats.get_mut(chain) {
                        accumulate_summary(target, summary);
                    }
                });
                None
            }
            Err(error) => {
                let report = summarize_entries_external(
                    snapshot,
                    roots,
                    require_secondary,
                    execution.stream,
                    on_work,
                    |chain, summary| {
                        if let Some(target) = stats.get_mut(chain) {
                            accumulate_summary(target, summary);
                        }
                    },
                )?;
                Some(summary_fallback_advisory(
                    execution.stream.scope_label,
                    &error,
                    report,
                ))
            }
        },
        SummaryExecutionMode::Streaming => {
            let report = summarize_entries_external(
                snapshot,
                roots,
                require_secondary,
                execution.stream,
                on_work,
                |chain, summary| {
                    if let Some(target) = stats.get_mut(chain) {
                        accumulate_summary(target, summary);
                    }
                },
            )?;
            summary_stream_report_advisory(execution.stream.scope_label, report)
        }
    };
    Ok(SummaryComputation { stats, advisory })
}

fn summary_stats_for_chain_pair(
    snapshot: &MetadataSnapshot,
    scopes: &ScopeComponents,
    pair: &ChainPairRoots,
    execution: SummaryScopeExecution<'_>,
    on_work: &(impl Fn(u64) + Sync),
) -> Result<SummaryComputation<[SummaryStats; 2]>, PipelineError> {
    let left_chain = pair.left_chain as usize;
    let right_chain = pair.right_chain as usize;
    let left_contracts = scopes
        .contracts_for_chain(pair.left_chain)
        .ok_or_else(|| PipelineError::Invariant("missing left chain CSR slice".into()))?;
    let right_contracts = scopes
        .contracts_for_chain(pair.right_chain)
        .ok_or_else(|| PipelineError::Invariant("missing right chain CSR slice".into()))?;
    if left_contracts.len() != pair.left_contract_count as usize {
        return Err(PipelineError::Invariant(
            "pair-local left chain cardinality mismatch".into(),
        ));
    }
    let roots = SummaryRootView::pair(
        &pair.roots,
        left_chain,
        right_chain,
        left_contracts,
        right_contracts,
    )?;
    let mut stats = [SummaryStats::default(), SummaryStats::default()];
    let advisory = {
        let mut accumulate = |chain, summary| {
            if chain == left_chain {
                accumulate_summary(&mut stats[0], summary);
            } else if chain == right_chain {
                accumulate_summary(&mut stats[1], summary);
            }
        };
        match execution.mode {
            SummaryExecutionMode::Fast => match summary_entries(snapshot, roots, on_work) {
                Ok(mut entries) => {
                    entries.par_sort_unstable_by_key(|entry| entry.key());
                    summarize_sorted_entries(&entries, true, &mut accumulate);
                    None
                }
                Err(error) => {
                    let report = summarize_entries_external(
                        snapshot,
                        roots,
                        true,
                        execution.stream,
                        on_work,
                        &mut accumulate,
                    )?;
                    Some(summary_fallback_advisory(
                        execution.stream.scope_label,
                        &error,
                        report,
                    ))
                }
            },
            SummaryExecutionMode::Streaming => {
                let report = summarize_entries_external(
                    snapshot,
                    roots,
                    true,
                    execution.stream,
                    on_work,
                    &mut accumulate,
                )?;
                summary_stream_report_advisory(execution.stream.scope_label, report)
            }
        }
    };
    Ok(SummaryComputation { stats, advisory })
}

fn summarize_sorted_entries(
    entries: &[SummaryEntry],
    require_secondary: bool,
    mut accumulate: impl FnMut(usize, SummaryStats),
) {
    let mut state = SortedSummaryAccumulator::new(require_secondary);
    for &entry in entries {
        state.push(entry, &mut accumulate);
    }
    state.finish(&mut accumulate);
}

#[derive(Clone, Copy, Default)]
struct SummaryChainGroup {
    chain: usize,
    count: i64,
    nfts: i64,
}

struct SortedSummaryAccumulator {
    require_secondary: bool,
    current_root: Option<u32>,
    current_chain: u32,
    current_count: i64,
    current_nfts: i64,
    root_total: i64,
    pending: [SummaryChainGroup; 2],
    pending_len: usize,
    direct_secondary: bool,
}

impl SortedSummaryAccumulator {
    fn new(require_secondary: bool) -> Self {
        Self {
            require_secondary,
            current_root: None,
            current_chain: 0,
            current_count: 0,
            current_nfts: 0,
            root_total: 0,
            pending: [SummaryChainGroup::default(); 2],
            pending_len: 0,
            direct_secondary: false,
        }
    }

    fn push(&mut self, entry: SummaryEntry, accumulate: &mut impl FnMut(usize, SummaryStats)) {
        if self.current_root != Some(entry.root) {
            if self.current_root.is_some() {
                self.finish_current_chain(accumulate);
                self.finish_root(accumulate);
            }
            self.current_root = Some(entry.root);
            self.current_chain = entry.chain;
        } else if self.current_chain != entry.chain {
            self.finish_current_chain(accumulate);
            self.current_chain = entry.chain;
        }
        self.current_count += 1;
        self.current_nfts = self.current_nfts.saturating_add(entry.nfts);
        self.root_total += 1;
    }

    fn finish(&mut self, accumulate: &mut impl FnMut(usize, SummaryStats)) {
        if self.current_root.is_some() {
            self.finish_current_chain(accumulate);
            self.finish_root(accumulate);
        }
    }

    fn finish_current_chain(&mut self, accumulate: &mut impl FnMut(usize, SummaryStats)) {
        if self.current_count == 0 {
            return;
        }
        let group = SummaryChainGroup {
            chain: self.current_chain as usize,
            count: self.current_count,
            nfts: self.current_nfts,
        };
        self.current_count = 0;
        self.current_nfts = 0;
        if !self.require_secondary {
            if group.count >= 2 {
                emit_summary_group(accumulate, group, group.count > 2);
            }
            return;
        }
        if self.direct_secondary {
            emit_summary_group(accumulate, group, true);
            return;
        }
        if self.pending_len == self.pending.len() {
            self.flush_pending(accumulate, true);
            emit_summary_group(accumulate, group, true);
            self.direct_secondary = true;
            return;
        }
        self.pending[self.pending_len] = group;
        self.pending_len += 1;
        if self.pending_len >= 2 && self.root_total > 2 {
            self.flush_pending(accumulate, true);
            self.direct_secondary = true;
        }
    }

    fn finish_root(&mut self, accumulate: &mut impl FnMut(usize, SummaryStats)) {
        if self.require_secondary && !self.direct_secondary && self.pending_len >= 2 {
            self.flush_pending(accumulate, self.root_total > 2);
        }
        self.current_root = None;
        self.root_total = 0;
        self.pending_len = 0;
        self.direct_secondary = false;
    }

    fn flush_pending(&mut self, accumulate: &mut impl FnMut(usize, SummaryStats), size_gt_2: bool) {
        for index in 0..self.pending_len {
            emit_summary_group(accumulate, self.pending[index], size_gt_2);
        }
        self.pending_len = 0;
    }
}

fn emit_summary_group(
    accumulate: &mut impl FnMut(usize, SummaryStats),
    group: SummaryChainGroup,
    size_gt_2: bool,
) {
    accumulate(
        group.chain,
        SummaryStats {
            group_count: 1,
            duplicate_contract_count: group.count,
            duplicate_nft_count: group.nfts,
            group_size_ge_2_count: 1,
            group_size_gt_2_count: i64::from(size_gt_2),
        },
    );
}

fn summary_stream_layout(scratch_bytes: u64) -> (usize, usize) {
    let scratch_bytes = usize::try_from(scratch_bytes)
        .unwrap_or(usize::MAX)
        .max(SUMMARY_ENTRY_ENCODED_BYTES * 4);
    let io_buffer_bytes = (scratch_bytes / 4).clamp(
        SUMMARY_ENTRY_ENCODED_BYTES,
        SUMMARY_STREAM_MAX_IO_BUFFER_BYTES,
    ) / SUMMARY_ENTRY_ENCODED_BYTES
        * SUMMARY_ENTRY_ENCODED_BYTES;
    let chunk_bytes = scratch_bytes.saturating_sub(io_buffer_bytes);
    (
        (chunk_bytes / std::mem::size_of::<SummaryEntry>()).max(1),
        io_buffer_bytes.max(SUMMARY_ENTRY_ENCODED_BYTES),
    )
}

fn allocate_summary_stream_chunk(
    requested_entries: usize,
) -> Result<(Vec<SummaryEntry>, usize), String> {
    let mut entries = requested_entries.max(1);
    let mut first_error = None;
    loop {
        let mut chunk = Vec::new();
        match chunk.try_reserve_exact(entries) {
            Ok(()) if chunk.capacity() <= requested_entries => {
                let capacity = chunk.capacity().max(1);
                return Ok((chunk, capacity));
            }
            Ok(()) if entries == 1 => {
                // A conforming allocator may round the backing allocation up,
                // but the emergency O(1)-heap path is only justified when even
                // one logical SummaryEntry cannot be allocated.
                return Ok((chunk, 1));
            }
            Ok(()) => {
                first_error.get_or_insert_with(|| {
                    format!(
                        "allocator returned capacity {} above the {requested_entries}-entry \
                         external-summary budget",
                        chunk.capacity()
                    )
                });
            }
            Err(error) => {
                first_error.get_or_insert_with(|| error.to_string());
            }
        }
        if entries == 1 {
            return Err(format!(
                "unable to allocate even one external-summary entry after the \
                 {requested_entries}-entry allocation failed: {}",
                first_error.unwrap_or_else(|| "unknown allocation failure".into())
            ));
        }
        entries = (entries / 2).max(1);
    }
}

fn summary_stream_scope_directory(
    scratch_root: &Path,
    scope_label: &str,
) -> Result<PathBuf, PipelineError> {
    static NEXT_SUMMARY_STREAM_ID: std::sync::atomic::AtomicU64 =
        std::sync::atomic::AtomicU64::new(0);
    std::fs::create_dir_all(scratch_root)?;
    let id = NEXT_SUMMARY_STREAM_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let directory = scratch_root.join(format!("{scope_label}-{}-{id}", std::process::id()));
    std::fs::create_dir(&directory)?;
    Ok(directory)
}

fn summary_run_path(directory: &Path, generation: usize, run: usize) -> PathBuf {
    directory.join(format!("run-{generation:06}-{run:012}.bin"))
}

fn write_summary_entry(writer: &mut impl Write, entry: SummaryEntry) -> Result<(), std::io::Error> {
    writer.write_all(&entry.root.to_le_bytes())?;
    writer.write_all(&entry.chain.to_le_bytes())?;
    writer.write_all(&entry.nfts.to_le_bytes())
}

fn read_summary_entry(reader: &mut impl Read) -> Result<Option<SummaryEntry>, std::io::Error> {
    let mut encoded = [0u8; SUMMARY_ENTRY_ENCODED_BYTES];
    if reader.read(&mut encoded[..1])? == 0 {
        return Ok(None);
    }
    reader.read_exact(&mut encoded[1..])?;
    Ok(Some(SummaryEntry {
        root: u32::from_le_bytes(encoded[0..4].try_into().expect("four root bytes")),
        chain: u32::from_le_bytes(encoded[4..8].try_into().expect("four chain bytes")),
        nfts: i64::from_le_bytes(encoded[8..16].try_into().expect("eight nft bytes")),
    }))
}

fn write_summary_run(
    path: &Path,
    entries: &[SummaryEntry],
    io_buffer_bytes: usize,
) -> Result<(), PipelineError> {
    let mut writer = BufWriter::with_capacity(io_buffer_bytes, std::fs::File::create(path)?);
    for &entry in entries {
        write_summary_entry(&mut writer, entry)?;
    }
    writer.flush()?;
    Ok(())
}

fn flush_summary_run(
    directory: &Path,
    run: usize,
    entries: &mut Vec<SummaryEntry>,
    io_buffer_bytes: usize,
) -> Result<(), PipelineError> {
    if entries.len() >= 16_384 {
        entries.par_sort_unstable_by_key(|entry| entry.key());
    } else {
        entries.sort_unstable_by_key(|entry| entry.key());
    }
    write_summary_run(
        &summary_run_path(directory, 0, run),
        entries,
        io_buffer_bytes,
    )?;
    entries.clear();
    Ok(())
}

fn merge_summary_runs(
    left_path: &Path,
    right_path: &Path,
    output_path: &Path,
    io_buffer_bytes: usize,
) -> Result<(), PipelineError> {
    let mut left = BufReader::with_capacity(io_buffer_bytes, std::fs::File::open(left_path)?);
    let mut right = BufReader::with_capacity(io_buffer_bytes, std::fs::File::open(right_path)?);
    let mut output = BufWriter::with_capacity(io_buffer_bytes, std::fs::File::create(output_path)?);
    let mut left_entry = read_summary_entry(&mut left)?;
    let mut right_entry = read_summary_entry(&mut right)?;
    while left_entry.is_some() || right_entry.is_some() {
        let take_left = match (left_entry, right_entry) {
            (Some(left), Some(right)) => left.key() <= right.key(),
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => break,
        };
        if take_left {
            write_summary_entry(&mut output, left_entry.expect("left entry is present"))?;
            left_entry = read_summary_entry(&mut left)?;
        } else {
            write_summary_entry(&mut output, right_entry.expect("right entry is present"))?;
            right_entry = read_summary_entry(&mut right)?;
        }
    }
    output.flush()?;
    drop(output);
    drop(left);
    drop(right);
    std::fs::remove_file(left_path)?;
    std::fs::remove_file(right_path)?;
    Ok(())
}

fn summarize_entries_external(
    snapshot: &MetadataSnapshot,
    roots: SummaryRootView<'_>,
    require_secondary: bool,
    stream: SummaryStreamContext<'_>,
    on_work: &(impl Fn(u64) + Sync),
    mut accumulate: impl FnMut(usize, SummaryStats),
) -> Result<SummaryStreamReport, PipelineError> {
    // Let M be the admitted chunk entries and R=ceil(N/M). Run generation is
    // O(N log M), pairwise merge is O(N log R), and peak heap is O(M). This
    // keeps one linear scan over the resident roots and trades bounded
    // sequential I/O for the full N*SummaryEntry resident allocation.
    const PROGRESS_CHUNK: usize = 65_536;
    let (requested_chunk_entries, io_buffer_bytes) = summary_stream_layout(stream.scratch_bytes);
    let (mut entries, actual_chunk_entries) =
        match allocate_summary_stream_chunk(requested_chunk_entries) {
            Ok(admitted) => admitted,
            Err(_) => {
                summarize_entries_without_allocation(
                    snapshot,
                    roots,
                    require_secondary,
                    on_work,
                    &mut accumulate,
                );
                return Ok(SummaryStreamReport {
                    requested_chunk_entries,
                    actual_chunk_entries: 0,
                    emergency_no_alloc: true,
                });
            }
        };
    let directory = summary_stream_scope_directory(stream.scratch_root, stream.scope_label)?;
    let mut run_count = 0usize;
    for begin in (0..roots.len()).step_by(PROGRESS_CHUNK) {
        let end = begin.saturating_add(PROGRESS_CHUNK).min(roots.len());
        for local in begin..end {
            let Some(entry) = roots.entry(snapshot, local) else {
                continue;
            };
            entries.push(entry);
            if entries.len() == actual_chunk_entries {
                flush_summary_run(&directory, run_count, &mut entries, io_buffer_bytes)?;
                run_count += 1;
            }
        }
        on_work((end - begin) as u64);
    }
    if !entries.is_empty() {
        flush_summary_run(&directory, run_count, &mut entries, io_buffer_bytes)?;
        run_count += 1;
    }
    drop(entries);
    if run_count == 0 {
        std::fs::remove_dir(&directory)?;
        return Ok(SummaryStreamReport {
            requested_chunk_entries,
            actual_chunk_entries,
            emergency_no_alloc: false,
        });
    }

    let mut generation = 0usize;
    while run_count > 1 {
        let next_generation = generation + 1;
        let mut next_run = 0usize;
        let mut input_run = 0usize;
        while input_run < run_count {
            let left = summary_run_path(&directory, generation, input_run);
            let output = summary_run_path(&directory, next_generation, next_run);
            if input_run + 1 < run_count {
                let right = summary_run_path(&directory, generation, input_run + 1);
                merge_summary_runs(&left, &right, &output, io_buffer_bytes)?;
            } else {
                std::fs::rename(&left, &output)?;
            }
            input_run += 2;
            next_run += 1;
        }
        generation = next_generation;
        run_count = next_run;
    }

    let final_run = summary_run_path(&directory, generation, 0);
    let mut reader = BufReader::with_capacity(io_buffer_bytes, std::fs::File::open(&final_run)?);
    let mut state = SortedSummaryAccumulator::new(require_secondary);
    while let Some(entry) = read_summary_entry(&mut reader)? {
        state.push(entry, &mut accumulate);
    }
    state.finish(&mut accumulate);
    drop(reader);
    std::fs::remove_file(final_run)?;
    std::fs::remove_dir(directory)?;
    Ok(SummaryStreamReport {
        requested_chunk_entries,
        actual_chunk_entries,
        emergency_no_alloc: false,
    })
}

fn summarize_entries_without_allocation(
    snapshot: &MetadataSnapshot,
    roots: SummaryRootView<'_>,
    require_secondary: bool,
    on_work: &(impl Fn(u64) + Sync),
    accumulate: &mut impl FnMut(usize, SummaryStats),
) {
    // Last-resort allocator failure path. It retains only scalar counters:
    // O(1) transient heap beyond the required output rows, at O(U*K*N) time
    // for U possible roots and K selected chains. Normal budget fallback uses
    // the external merge above; this path exists so allocation pressure warns
    // and degrades instead of terminating the pipeline.
    const PROGRESS_CHUNK: usize = 65_536;
    for begin in (0..roots.len()).step_by(PROGRESS_CHUNK) {
        on_work(
            begin
                .saturating_add(PROGRESS_CHUNK)
                .min(roots.len())
                .saturating_sub(begin) as u64,
        );
    }
    let selection = roots.selection();
    for root in 0..roots.len() {
        let root = root as u32;
        let mut total = 0i64;
        for local in 0..roots.len() {
            let Some(entry) = roots.entry(snapshot, local) else {
                continue;
            };
            if entry.root == root {
                total += 1;
            }
        }
        if total < 2 {
            continue;
        }
        selection.for_each(|chain| {
            let mut count = 0i64;
            let mut nfts = 0i64;
            for local in 0..roots.len() {
                let Some(entry) = roots.entry(snapshot, local) else {
                    continue;
                };
                if entry.root != root || entry.chain != chain as u32 {
                    continue;
                }
                count += 1;
                nfts = nfts.saturating_add(entry.nfts);
            }
            let qualifies = if require_secondary {
                count != 0 && total > count
            } else {
                count >= 2
            };
            if qualifies {
                emit_summary_group(
                    accumulate,
                    SummaryChainGroup { chain, count, nfts },
                    if require_secondary {
                        total > 2
                    } else {
                        count > 2
                    },
                );
            }
        });
    }
}

fn summary_fallback_advisory(
    scope_label: &str,
    allocation_error: &str,
    report: SummaryStreamReport,
) -> String {
    if report.emergency_no_alloc {
        format!(
            "metadata summary fast allocation failed for {scope_label}: {allocation_error}; \
             bounded external scratch allocation also failed, so the scope used the exact \
             constant-memory multi-pass fallback and the pipeline continued"
        )
    } else {
        format!(
            "metadata summary fast allocation failed for {scope_label}: {allocation_error}; \
             switched to bounded external merge with {} entries per chunk (requested {}) and \
             the pipeline continued",
            report.actual_chunk_entries, report.requested_chunk_entries
        )
    }
}

fn summary_stream_report_advisory(
    scope_label: &str,
    report: SummaryStreamReport,
) -> Option<String> {
    if report.emergency_no_alloc {
        Some(format!(
            "metadata summary external scratch allocation failed for {scope_label}; used the \
             exact constant-memory multi-pass fallback and the pipeline continued"
        ))
    } else if report.actual_chunk_entries < report.requested_chunk_entries {
        Some(format!(
            "metadata summary external chunk allocation for {scope_label} was reduced from {} \
             to {} entries; bounded merge continued",
            report.requested_chunk_entries, report.actual_chunk_entries
        ))
    } else {
        None
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

    fn test_scope_collector_plan(
        node_count: u32,
        chain_pair_count: usize,
        edge_bytes: u64,
        threads: usize,
    ) -> ScopeCollectorMemoryPlan {
        let memory = MemoryBroker::new(2 * crate::resource::GIB, crate::resource::GIB).unwrap();
        build_scope_collector_memory_plan(
            &memory,
            node_count,
            chain_pair_count,
            edge_bytes,
            threads,
        )
        .unwrap()
    }

    #[test]
    fn configured_worker_pool_uses_requested_thread_count() {
        let pool = build_metadata_worker_pool(2).unwrap();
        assert_eq!(pool.install(rayon::current_num_threads), 2);
    }

    #[test]
    fn payload_score_cache_is_only_enabled_for_repeated_payloads() {
        assert!(!payloads_have_duplicates(&[]));
        assert!(!payloads_have_duplicates(&[7]));
        assert!(!payloads_have_duplicates(&[9, 3, 7, 1]));
        assert!(payloads_have_duplicates(&[9, 3, 7, 3]));
    }

    #[test]
    fn chain_pair_local_nodes_sum_to_linear_storage_even_with_empty_chains() {
        let contract_chain = [4, 0, 2, 4, 2, 0, 4];
        let chain_count = 6;
        let index = ChainContractIndex::build(&contract_chain, chain_count).unwrap();
        let pair_nodes = chain_pairs(chain_count)
            .into_iter()
            .map(|(left, right)| {
                u64::from(
                    index
                        .pair_node_count(left as usize, right as usize)
                        .unwrap(),
                )
            })
            .sum::<u64>();

        assert_eq!(
            pair_nodes,
            (chain_count as u64 - 1) * contract_chain.len() as u64
        );
    }

    #[test]
    fn external_chain_contract_index_matches_resident_and_cleans_workspace() {
        let contract_chain = [4, 0, 2, 4, 2, 0, 4];
        let chain_count = 6;
        let resident = ChainContractIndex::build(&contract_chain, chain_count).unwrap();
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("chain-index");
        {
            let external =
                ChainContractIndex::build_external(&contract_chain, chain_count, &workspace)
                    .unwrap();
            assert!(external.offsets.is_mapped());
            assert!(external.contracts.is_mapped());
            assert!(external.local_rank.is_mapped());
            assert_eq!(external.offsets.as_ref(), resident.offsets.as_ref());
            assert_eq!(external.contracts.as_ref(), resident.contracts.as_ref());
            assert_eq!(external.local_rank.as_ref(), resident.local_rank.as_ref());
            for (left, right) in chain_pairs(chain_count) {
                assert_eq!(
                    external
                        .pair_node_count(left as usize, right as usize)
                        .unwrap(),
                    resident
                        .pair_node_count(left as usize, right as usize)
                        .unwrap()
                );
            }
        }
        assert!(!workspace.exists());
    }

    #[test]
    fn pair_local_reduction_expands_to_exact_global_reference_components() {
        let contract_chain = [1, 0, 2, 1, 0, 2];
        let node_count = contract_chain.len() as u32;
        let chain_count = 3;
        let budget = EdgeBudget {
            max_buffer_bytes: u64::MAX,
            max_run_edges: u64::MAX,
            max_total_bytes: u64::MAX,
        };
        let global_runs = vec![
            vec![ForestRun::from_edges(
                node_count,
                [Edge::new(1, 0), Edge::new(4, 3), Edge::new(1, 3)],
                budget,
            )
            .unwrap()],
            vec![ForestRun::from_edges(node_count, [Edge::new(4, 2)], budget).unwrap()],
            vec![ForestRun::from_edges(node_count, [Edge::new(3, 5)], budget).unwrap()],
        ];
        let pairs = chain_pairs(chain_count);
        let index = ChainContractIndex::build(&contract_chain, chain_count).unwrap();
        let temp = tempfile::tempdir().unwrap();
        let mut pair_roots = Vec::new();
        let mut expected_global = Vec::new();
        for (pair_index, (&(left, right), runs)) in pairs.iter().zip(&global_runs).enumerate() {
            let resident = runs
                .iter()
                .cloned()
                .map(ForestRunStorage::resident)
                .collect::<Vec<_>>();
            let directory = temp.path().join(format!("pair-{pair_index}"));
            for (run_id, run) in runs.iter().enumerate() {
                run.commit(&directory, run_id as u32).unwrap();
                let prefix = format!("run-{run_id:06}");
                assert!(directory.join(format!("{prefix}-edges.u32")).is_file());
                assert!(!directory.join(format!("{prefix}-left.u32")).exists());
                assert!(!directory.join(format!("{prefix}-right.u32")).exists());
            }
            let mapped = (0..runs.len())
                .map(|run_id| ForestRunStorage::open_mapped(&directory, run_id as u32).unwrap())
                .collect::<Vec<_>>();
            let pair_node_count = index
                .pair_node_count(left as usize, right as usize)
                .unwrap();
            let reduce = |stored: &[ForestRunStorage]| {
                crate::reduce::reduce_stored_components_with_progress(
                    stored,
                    node_count,
                    pair_node_count,
                    |edge| {
                        Ok(Edge::new(
                            index
                                .pair_local_contract(
                                    &contract_chain,
                                    edge.left,
                                    left as usize,
                                    right as usize,
                                )
                                .unwrap(),
                            index
                                .pair_local_contract(
                                    &contract_chain,
                                    edge.right,
                                    left as usize,
                                    right as usize,
                                )
                                .unwrap(),
                        ))
                    },
                    |_, _| {},
                )
                .unwrap()
            };
            let resident_roots = reduce(&resident);
            let mapped_roots = reduce(&mapped);
            assert_eq!(resident_roots, mapped_roots);
            assert!(mapped.iter().all(ForestRunStorage::is_mapped));
            pair_roots.push(ChainPairRoots {
                left_chain: left,
                right_chain: right,
                left_contract_count: index.chain_contract_count(left as usize).unwrap(),
                roots: RootStorage::resident(resident_roots),
            });
            expected_global.push(crate::reduce::reduce_components(runs, node_count).unwrap());
        }
        let scopes = ScopeComponents {
            intra_roots: RootStorage::default(),
            cross_roots: RootStorage::default(),
            chain_pair_roots: pair_roots,
            chain_contract_offsets: index.offsets.clone(),
            chain_contracts: index.contracts.clone(),
        };

        for (pair, global_roots) in scopes.chain_pair_roots.iter().zip(expected_global) {
            assert_eq!(pair.expand_global_roots(&scopes).unwrap(), global_roots);
        }
    }

    #[test]
    fn component_resume_releases_reused_forests_without_touching_pending_runs() {
        let budget = EdgeBudget {
            max_buffer_bytes: u64::MAX,
            max_run_edges: u64::MAX,
            max_total_bytes: u64::MAX,
        };
        let identity = |scope_identity: &str| ComponentSnapshotIdentity {
            schema_revision: 1,
            snapshot_fingerprint: "snapshot".into(),
            connectivity_revision: COMPONENT_ROOT_LAYOUT_REVISION,
            connectivity_plan_digest: "plan".into(),
            scope_identity: scope_identity.into(),
            node_count: 2,
        };
        let mut scopes = vec![
            ComponentScopePlan {
                kind: ComponentScopeKind::Pair { left: 0, right: 1 },
                directory: PathBuf::new(),
                identity: identity("pair:0:1"),
                runs: vec![ForestRunStorage::resident(
                    ForestRun::from_edges(3, [Edge::new(0, 2)], budget).unwrap(),
                )],
                roots: Some(RootStorage::resident(vec![0, 1])),
                needs_rebuild: false,
                committed: true,
            },
            ComponentScopePlan {
                kind: ComponentScopeKind::Pair { left: 1, right: 2 },
                directory: PathBuf::new(),
                identity: identity("pair:1:2"),
                runs: vec![ForestRunStorage::resident(
                    ForestRun::from_edges(3, [Edge::new(1, 2)], budget).unwrap(),
                )],
                roots: None,
                needs_rebuild: true,
                committed: false,
            },
        ];
        let pool = build_metadata_worker_pool(2).unwrap();

        release_reused_component_runs(&mut scopes, &pool);

        assert!(scopes[0].runs.is_empty());
        assert_eq!(scopes[1].runs[0].node_count(), 3);
        match &scopes[1].runs[0] {
            ForestRunStorage::Resident(run) => {
                assert_eq!(run.edges, vec![Edge::new(1, 2)]);
            }
            ForestRunStorage::Mapped(_) => panic!("pending test forest unexpectedly mapped"),
        }
    }

    #[test]
    fn component_memory_plan_charges_final_roots_once_plus_one_parent_per_lane() {
        let memory =
            MemoryBroker::new(512 * crate::resource::GIB, 448 * crate::resource::GIB).unwrap();
        let roots = vec![(crate::resource::GIB / 4) as u32; 8];
        let plan = plan_component_memory(&memory, 128, &roots).unwrap();

        assert_eq!(plan.root_mode, ComponentRootMode::Resident);
        assert_eq!(plan.parallel_scopes, 8);
        assert_eq!(plan.total_root_bytes, 8 * crate::resource::GIB);
        assert_eq!(plan.transient_bytes_per_scope, crate::resource::GIB);
        assert_eq!(plan.peak_bytes, 16 * crate::resource::GIB);
    }

    #[test]
    fn component_memory_plan_reduces_scope_concurrency_to_remaining_budget() {
        let memory =
            MemoryBroker::new(512 * crate::resource::GIB, 448 * crate::resource::GIB).unwrap();
        let _occupied = memory.reserve(437 * crate::resource::GIB).unwrap();
        let roots = vec![(crate::resource::GIB / 4) as u32; 8];
        let plan = plan_component_memory(&memory, 128, &roots).unwrap();

        assert_eq!(plan.parallel_scopes, 3);
        assert_eq!(plan.peak_bytes, 11 * crate::resource::GIB);
    }

    #[test]
    fn component_memory_plan_uses_actual_pair_sizes_for_unbalanced_parallelism() {
        let memory =
            MemoryBroker::new(512 * crate::resource::GIB, 448 * crate::resource::GIB).unwrap();
        let _occupied = memory.reserve(430 * crate::resource::GIB).unwrap();
        let mut roots = vec![(2 * 1024 * 1024) as u32; 128];
        roots[0] = (2 * crate::resource::GIB) as u32;
        let total_root_bytes = roots
            .iter()
            .map(|&nodes| u64::from(nodes) * std::mem::size_of::<u32>() as u64)
            .sum::<u64>();

        let plan = plan_component_memory(&memory, 128, &roots).unwrap();

        assert_eq!(plan.root_mode, ComponentRootMode::Resident);
        assert_eq!(plan.parallel_scopes, 128);
        assert_eq!(plan.total_root_bytes, total_root_bytes);
        assert_eq!(plan.peak_bytes, total_root_bytes * 2);
    }

    #[test]
    fn component_memory_plan_uses_mapped_waves_when_global_roots_do_not_fit() {
        let memory =
            MemoryBroker::new(512 * crate::resource::GIB, 448 * crate::resource::GIB).unwrap();
        let _occupied = memory.reserve(447 * crate::resource::GIB).unwrap();
        let roots = vec![(crate::resource::GIB / 2) as u32; 8];
        let plan = plan_component_memory(&memory, 128, &roots).unwrap();

        assert_eq!(plan.root_mode, ComponentRootMode::Mapped);
        assert_eq!(plan.parallel_scopes, 0);
        assert_eq!(plan.peak_bytes, 0);
        assert_eq!(plan.host_headroom_bytes, 4 * crate::resource::GIB);
    }

    #[test]
    fn summary_memory_plan_uses_bounded_headroom_when_no_full_lane_fits() {
        let memory =
            MemoryBroker::new(512 * crate::resource::GIB, 448 * crate::resource::GIB).unwrap();
        let _occupied = memory.reserve(448 * crate::resource::GIB).unwrap();
        let (plan, _summary_memory) = reserve_summary_memory(
            &memory,
            128,
            2,
            8,
            16 * crate::resource::GIB,
            8 * crate::resource::GIB,
        )
        .unwrap();

        assert_eq!(plan.parallel_primary_scopes, 0);
        assert_eq!(plan.parallel_pair_scopes, 0);
        assert_eq!(plan.peak_bytes, 0);
        assert_eq!(plan.stream_scratch_bytes, SUMMARY_STREAM_MAX_SCRATCH_BYTES);
        assert_eq!(plan.stream_headroom_bytes, SUMMARY_STREAM_MAX_SCRATCH_BYTES);
    }

    #[test]
    fn summary_memory_plan_reserves_available_bytes_for_external_merge() {
        let memory =
            MemoryBroker::new(512 * crate::resource::GIB, 448 * crate::resource::GIB).unwrap();
        let remaining = 32 * 1024 * 1024;
        let _occupied = memory
            .reserve(448 * crate::resource::GIB - remaining)
            .unwrap();
        let (plan, _summary_memory) = reserve_summary_memory(
            &memory,
            128,
            2,
            8,
            crate::resource::GIB,
            crate::resource::GIB / 2,
        )
        .unwrap();

        assert_eq!(plan.parallel_primary_scopes, 0);
        assert_eq!(plan.parallel_pair_scopes, 0);
        assert_eq!(plan.peak_bytes, remaining);
        assert_eq!(plan.stream_scratch_bytes, remaining);
        assert_eq!(plan.stream_headroom_bytes, 0);
    }

    #[test]
    fn summary_memory_plan_accounts_stream_scratch_when_only_primary_spills() {
        let memory =
            MemoryBroker::new(512 * crate::resource::GIB, 448 * crate::resource::GIB).unwrap();
        let remaining = 32 * 1024 * 1024;
        let _occupied = memory
            .reserve(448 * crate::resource::GIB - remaining)
            .unwrap();
        let (plan, summary_memory) =
            reserve_summary_memory(&memory, 128, 2, 2, 64 * 1024 * 1024, 16 * 1024 * 1024).unwrap();

        assert_eq!(plan.parallel_primary_scopes, 0);
        assert_eq!(plan.parallel_pair_scopes, 2);
        assert_eq!(plan.peak_bytes, remaining);
        assert_eq!(plan.stream_scratch_bytes, remaining);
        assert_eq!(plan.stream_headroom_bytes, 0);
        assert_eq!(summary_memory.bytes(), remaining);
    }

    #[test]
    fn scope_collector_plan_falls_back_to_exact_sparse_when_one_dense_lane_does_not_fit() {
        let mib = 1024 * 1024;
        let memory = MemoryBroker::new(32 * mib, 16 * mib).unwrap();
        let plan = build_scope_collector_memory_plan(&memory, 4_000_000, 3, mib, 4).unwrap();

        assert_eq!(plan.scratch_kind, EdgeCollectorScratchKind::Sparse);
        assert_eq!(plan.edge_bytes, mib);
        assert!(plan.reserved_bytes <= memory.available_bytes());
        assert!(plan.active_sink_workers > 0);
    }

    #[test]
    fn scope_collector_dense_scratch_scales_with_sink_workers_not_scope_count() {
        let memory =
            MemoryBroker::new(512 * crate::resource::GIB, 448 * crate::resource::GIB).unwrap();
        let few = build_scope_collector_memory_plan(&memory, 1_000_000, 1, 1024 * 1024, 4).unwrap();
        let many =
            build_scope_collector_memory_plan(&memory, 1_000_000, 120, 1024 * 1024, 4).unwrap();

        assert_eq!(few.scratch_kind, EdgeCollectorScratchKind::Dense);
        assert_eq!(many.scratch_kind, EdgeCollectorScratchKind::Dense);
        assert_eq!(few.active_sink_workers, many.active_sink_workers);
        assert!(many.scratch_bytes <= few.scratch_bytes);
    }

    #[test]
    fn oversized_summary_entry_allocation_is_recoverable() {
        assert!(allocate_summary_entries(usize::MAX).is_err());
    }

    #[test]
    fn scope_collector_broker_shards_scopes_within_thread_ceiling() {
        let budget = EdgeBudget {
            max_buffer_bytes: u64::MAX,
            max_run_edges: u64::MAX,
            max_total_bytes: u64::MAX,
        };
        let contract_chain = [0, 0, 1, 1, 2, 2];
        let plan = test_scope_collector_plan(6, 3, 1024 * 1024, 4);
        let spill = tempfile::tempdir().unwrap();
        let broker = ScopeCollectorBroker::new(6, 3, budget, u64::MAX, plan, spill.path()).unwrap();

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
            canonical_edge_components(6, &intra[0].materialized_edges()),
            vec![vec![0, 1]]
        );
        let cross_edges = cross
            .iter()
            .flat_map(ForestRunStorage::materialized_edges)
            .collect::<Vec<_>>();
        assert_eq!(
            canonical_edge_components(6, &cross_edges),
            vec![vec![0, 2, 4]]
        );
        assert_eq!(
            canonical_edge_components(6, &pairs[0][0].materialized_edges()),
            vec![vec![0, 2]]
        );
        assert_eq!(
            canonical_edge_components(6, &pairs[2][0].materialized_edges()),
            vec![vec![2, 4]]
        );
        assert!(pairs[1].is_empty());
    }

    #[test]
    fn scope_collector_broker_spills_on_retained_budget_overflow() {
        let budget = EdgeBudget {
            max_buffer_bytes: u64::MAX,
            max_run_edges: u64::MAX,
            max_total_bytes: u64::MAX,
        };
        let plan = test_scope_collector_plan(2, 0, 1024 * 1024, 2);
        let spill = tempfile::tempdir().unwrap();
        let broker = ScopeCollectorBroker::new(2, 0, budget, 0, plan, spill.path()).unwrap();

        broker
            .push_edges_by_chain(&[0, 0], 1, vec![Edge::new(0, 1)])
            .unwrap();
        let (intra, cross, pairs) = broker.finish().unwrap();

        assert_eq!(intra.len(), 1);
        assert!(intra[0].is_mapped());
        assert_eq!(intra[0].materialized_edges(), vec![Edge::new(0, 1)]);
        assert!(cross.is_empty());
        assert!(pairs.is_empty());
    }

    #[test]
    fn scope_collector_broker_preserves_scorer_capacity_with_many_scopes() {
        let budget = EdgeBudget {
            max_buffer_bytes: u64::MAX,
            max_run_edges: u64::MAX,
            max_total_bytes: u64::MAX,
        };
        let plan = test_scope_collector_plan(1, 120, 1024 * 1024, 128);
        let spill = tempfile::tempdir().unwrap();
        let broker =
            ScopeCollectorBroker::new(1, 120, budget, u64::MAX, plan, spill.path()).unwrap();

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
    fn fallback_atom_with_empty_retained_tokens_uses_linear_star_forest() {
        let dir = tempfile::tempdir().unwrap();
        let features = dir.path().join("features");
        let blocking = dir.path().join("blocking");
        let sources = vec![
            EncodeSourceRow {
                contract_id: 0,
                payload_id: 0,
                retained_token_ids: vec![1],
            },
            EncodeSourceRow {
                contract_id: 1,
                payload_id: 0,
                retained_token_ids: vec![1, 2],
            },
            EncodeSourceRow {
                contract_id: 2,
                payload_id: 0,
                retained_token_ids: vec![],
            },
            EncodeSourceRow {
                contract_id: 3,
                payload_id: 0,
                retained_token_ids: vec![3],
            },
        ];
        let contracts = (0..4)
            .map(|contract_id| EncodeContractRow {
                contract_id,
                chain_id: 0,
                source_doc_id: contract_id,
                payload_id: 0,
                weight: 1,
            })
            .collect::<Vec<_>>();
        write_encode_artifacts_with_contracts_and_atoms(
            &features,
            &sources,
            &[EncodePayloadRow {
                template_terms: vec![(1, 1)],
                content_terms: vec![(2, 1)],
            }],
            &contracts,
            &[vec![0, 1, 2, 3]],
        )
        .unwrap();
        compile_base_equivalent(
            &[AtomSketch {
                template_simhash: 0,
                content_simhash: 0,
                template_anchors: vec![1],
                content_anchors: vec![2],
                has_template_terms: true,
                has_content_terms: true,
            }],
            &BlockingCompileConfig {
                max_routing_block_members: 10,
            },
            &blocking,
        )
        .unwrap();
        commit_ready(
            &features,
            "features.ready",
            r#"{"schema_revision":3,"source_count":4,"payload_count":1,"chains":["x"],"chain_totals":[{"name":"x","contracts":4,"nfts":4}]}"#,
        )
        .unwrap();
        commit_ready(
            &blocking,
            "blocking.ready",
            r#"{"blocking_revision":3,"atom_count":1}"#,
        )
        .unwrap();
        let snapshot = MetadataSnapshot::open(&features, &blocking).unwrap();

        let (visits, edges) = fallback_atom_forest(&snapshot, 0).unwrap();
        let mut actual = edges
            .iter()
            .map(|edge| (edge.left, edge.right))
            .collect::<Vec<_>>();
        actual.sort_unstable();
        let mut expected = [0, 1, 3]
            .into_iter()
            .map(|contract| {
                let edge = Edge::new(2, contract);
                (edge.left, edge.right)
            })
            .collect::<Vec<_>>();
        expected.sort_unstable();

        assert_eq!(visits, 3);
        assert_eq!(actual, expected);
        assert!(edges.iter().all(|edge| !contracts_share_retained_token(
            snapshot.features(),
            edge.left,
            edge.right
        )));
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
                runs: vec![ForestRunStorage::resident(first)],
                roots: None,
                needs_rebuild: true,
                committed: false,
            },
            ComponentScopePlan {
                kind: ComponentScopeKind::Cross,
                directory: PathBuf::new(),
                identity: identity("cross"),
                runs: vec![ForestRunStorage::resident(second)],
                roots: None,
                needs_rebuild: true,
                committed: false,
            },
        ];
        let total = scopes
            .iter()
            .map(|scope| reduce_work(&scope.runs, 5).unwrap())
            .sum();
        let pool = build_metadata_worker_pool(2).unwrap();
        let mut events = Vec::new();
        let contract_chain = vec![0; 5];
        let chain_index = ChainContractIndex::build(&contract_chain, 1).unwrap();

        let spill = tempfile::tempdir().unwrap();
        reduce_component_scopes_parallel(
            &mut scopes,
            ComponentReduceExecution {
                reduce_total: total,
                max_parallel_scopes: 2,
                root_mode: ComponentRootMode::Resident,
                persistence: MatchPersistence::Ephemeral,
                spill_root: spill.path(),
                source_node_count: 5,
                chain_index: &chain_index,
                contract_chain: &contract_chain,
            },
            &pool,
            &mut |event| events.push(event),
        )
        .unwrap();

        assert_eq!(
            scopes[0].roots.as_ref().unwrap().as_slice(),
            &[0, 0, 0, 3, 4]
        );
        assert_eq!(
            scopes[1].roots.as_ref().unwrap().as_slice(),
            &[0, 1, 2, 3, 3]
        );
        assert_eq!(events.last().unwrap().completed, total);
    }

    #[test]
    fn resident_and_mapped_component_roots_are_semantically_identical_and_cleanup_spill() {
        let budget = EdgeBudget {
            max_buffer_bytes: u64::MAX,
            max_run_edges: u64::MAX,
            max_total_bytes: u64::MAX,
        };
        let make_scopes = || {
            let identity = |scope_identity: &str| ComponentSnapshotIdentity {
                schema_revision: 1,
                snapshot_fingerprint: "snapshot".into(),
                connectivity_revision: 1,
                connectivity_plan_digest: "plan".into(),
                scope_identity: scope_identity.into(),
                node_count: 6,
            };
            vec![
                ComponentScopePlan {
                    kind: ComponentScopeKind::Intra,
                    directory: PathBuf::new(),
                    identity: identity("intra"),
                    runs: vec![ForestRunStorage::resident(
                        ForestRun::from_edges(6, [Edge::new(0, 1), Edge::new(1, 2)], budget)
                            .unwrap(),
                    )],
                    roots: None,
                    needs_rebuild: true,
                    committed: false,
                },
                ComponentScopePlan {
                    kind: ComponentScopeKind::Cross,
                    directory: PathBuf::new(),
                    identity: identity("cross"),
                    runs: vec![ForestRunStorage::resident(
                        ForestRun::from_edges(6, [Edge::new(3, 4), Edge::new(4, 5)], budget)
                            .unwrap(),
                    )],
                    roots: None,
                    needs_rebuild: true,
                    committed: false,
                },
            ]
        };
        let pool = build_metadata_worker_pool(2).unwrap();
        let mut resident = make_scopes();
        let mut mapped = make_scopes();
        let total = resident
            .iter()
            .map(|scope| reduce_work(&scope.runs, 6).unwrap())
            .sum();
        let contract_chain = vec![0; 6];
        let chain_index = ChainContractIndex::build(&contract_chain, 1).unwrap();
        let temp = tempfile::tempdir().unwrap();
        let spill = temp.path().join("roots");

        reduce_component_scopes_parallel(
            &mut resident,
            ComponentReduceExecution {
                reduce_total: total,
                max_parallel_scopes: 2,
                root_mode: ComponentRootMode::Resident,
                persistence: MatchPersistence::Ephemeral,
                spill_root: &spill,
                source_node_count: 6,
                chain_index: &chain_index,
                contract_chain: &contract_chain,
            },
            &pool,
            &mut |_| {},
        )
        .unwrap();
        reduce_component_scopes_parallel(
            &mut mapped,
            ComponentReduceExecution {
                reduce_total: total,
                max_parallel_scopes: 2,
                root_mode: ComponentRootMode::Mapped,
                persistence: MatchPersistence::Ephemeral,
                spill_root: &spill,
                source_node_count: 6,
                chain_index: &chain_index,
                contract_chain: &contract_chain,
            },
            &pool,
            &mut |_| {},
        )
        .unwrap();

        assert!(mapped
            .iter()
            .all(|scope| scope.roots.as_ref().unwrap().is_mapped()));
        for (resident, mapped) in resident.iter().zip(&mapped) {
            assert_eq!(resident.roots, mapped.roots);
            assert_eq!(
                serde_json::to_value(resident.roots.as_ref().unwrap()).unwrap(),
                serde_json::to_value(mapped.roots.as_ref().unwrap()).unwrap()
            );
        }
        assert!(spill.is_dir());
        drop(mapped);
        assert!(!spill.exists());
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

    fn summary_test_snapshot() -> (tempfile::TempDir, MetadataSnapshot) {
        let dir = tempfile::tempdir().unwrap();
        let features = dir.path().join("features");
        let blocking = dir.path().join("blocking");
        let chains = [0u32, 0, 1, 2, 1, 0, 2];
        let weights = [10u64, 20, 30, 40, 50, 60, 70];
        let sources = (0..chains.len() as u32)
            .map(|contract_id| EncodeSourceRow {
                contract_id,
                payload_id: 0,
                retained_token_ids: vec![],
            })
            .collect::<Vec<_>>();
        let contracts = chains
            .iter()
            .copied()
            .zip(weights)
            .enumerate()
            .map(|(contract_id, (chain_id, weight))| EncodeContractRow {
                contract_id: contract_id as u32,
                chain_id,
                source_doc_id: contract_id as u32,
                payload_id: 0,
                weight,
            })
            .collect::<Vec<_>>();
        let payloads = [EncodePayloadRow {
            template_terms: vec![(1, 1)],
            content_terms: vec![(2, 1)],
        }];
        write_encode_artifacts_with_contracts_and_atoms(
            &features,
            &sources,
            &payloads,
            &contracts,
            &(0..chains.len() as u32)
                .map(|contract| vec![contract])
                .collect::<Vec<_>>(),
        )
        .unwrap();
        compile_base_equivalent(
            &(0..chains.len())
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
                max_routing_block_members: 16,
            },
            &blocking,
        )
        .unwrap();
        commit_ready(
            &features,
            "features.ready",
            r#"{"schema_revision":3,"source_count":7,"payload_count":1,"chains":["a","b","c"],"chain_totals":[{"name":"a","contracts":3,"nfts":90},{"name":"b","contracts":2,"nfts":80},{"name":"c","contracts":2,"nfts":110}]}"#,
        )
        .unwrap();
        commit_ready(
            &blocking,
            "blocking.ready",
            r#"{"blocking_revision":3,"atom_count":7}"#,
        )
        .unwrap();
        let snapshot = MetadataSnapshot::open(&features, &blocking).unwrap();
        (dir, snapshot)
    }

    #[test]
    fn external_and_constant_memory_summaries_match_in_memory_sort() {
        let (dir, snapshot) = summary_test_snapshot();
        let roots = [0, 0, 0, 3, 3, 5, 5];
        let cases = [
            (SummaryChainSelection::All(3), false),
            (SummaryChainSelection::All(3), true),
            (SummaryChainSelection::Pair { left: 0, right: 2 }, true),
        ];

        for (case, (selection, require_secondary)) in cases.into_iter().enumerate() {
            let roots = SummaryRootView::global(&roots, selection);
            let mut entries = summary_entries(&snapshot, roots, &|_| {}).unwrap();
            entries.sort_unstable_by_key(|entry| entry.key());
            let mut expected = vec![SummaryStats::default(); 3];
            summarize_sorted_entries(&entries, require_secondary, |chain, value| {
                accumulate_summary(&mut expected[chain], value);
            });

            let external_progress = std::sync::atomic::AtomicU64::new(0);
            let mut external = vec![SummaryStats::default(); 3];
            let scratch_root = dir.path().join("summary-scratch");
            let scope_label = format!("case-{case}");
            let report = summarize_entries_external(
                &snapshot,
                roots,
                require_secondary,
                SummaryStreamContext {
                    scratch_root: &scratch_root,
                    scope_label: &scope_label,
                    scratch_bytes: 64,
                },
                &|delta| {
                    external_progress.fetch_add(delta, std::sync::atomic::Ordering::Relaxed);
                },
                |chain, value| accumulate_summary(&mut external[chain], value),
            )
            .unwrap();
            assert!(!report.emergency_no_alloc);
            assert_eq!(report.actual_chunk_entries, 3);
            assert_eq!(
                external_progress.load(std::sync::atomic::Ordering::Relaxed),
                roots.len() as u64
            );
            assert_eq!(external, expected);

            let no_alloc_progress = std::sync::atomic::AtomicU64::new(0);
            let mut no_alloc = vec![SummaryStats::default(); 3];
            summarize_entries_without_allocation(
                &snapshot,
                roots,
                require_secondary,
                &|delta| {
                    no_alloc_progress.fetch_add(delta, std::sync::atomic::Ordering::Relaxed);
                },
                &mut |chain, value| accumulate_summary(&mut no_alloc[chain], value),
            );
            assert_eq!(
                no_alloc_progress.load(std::sync::atomic::Ordering::Relaxed),
                roots.len() as u64
            );
            assert_eq!(no_alloc, expected);
        }
        assert_eq!(
            std::fs::read_dir(dir.path().join("summary-scratch"))
                .unwrap()
                .count(),
            0
        );
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
    fn bounded_catalog_disjoint_fast_path_matches_chain_scoped_resident_forest() {
        let dir = tempfile::tempdir().unwrap();
        let features = dir.path().join("features");
        let blocking = dir.path().join("blocking");
        let sources = (0..32)
            .map(|contract_id| EncodeSourceRow {
                contract_id,
                payload_id: 0,
                retained_token_ids: vec![if contract_id < 16 {
                    contract_id
                } else {
                    1_000 + contract_id
                }],
            })
            .collect::<Vec<_>>();
        let contracts = (0..32)
            .map(|contract_id| EncodeContractRow {
                contract_id,
                chain_id: u32::from(contract_id >= 16),
                source_doc_id: contract_id,
                payload_id: 0,
                weight: 1,
            })
            .collect::<Vec<_>>();
        write_encode_artifacts_with_contracts_and_atoms(
            &features,
            &sources,
            &[EncodePayloadRow {
                template_terms: vec![(1, 1)],
                content_terms: vec![(2, 1)],
            }],
            &contracts,
            &[(0..16).collect(), (16..32).collect()],
        )
        .unwrap();
        let sketch = AtomSketch {
            template_simhash: 0,
            content_simhash: 0,
            template_anchors: vec![1],
            content_anchors: vec![2],
            has_template_terms: true,
            has_content_terms: true,
        };
        compile_base_equivalent(
            &[sketch.clone(), sketch],
            &BlockingCompileConfig {
                max_routing_block_members: 10,
            },
            &blocking,
        )
        .unwrap();
        commit_ready(
            &features,
            "features.ready",
            r#"{"schema_revision":3,"source_count":32,"payload_count":1,"chains":["a","b"],"chain_totals":[{"name":"a","contracts":16,"nfts":16},{"name":"b","contracts":16,"nfts":16}]}"#,
        )
        .unwrap();
        commit_ready(
            &blocking,
            "blocking.ready",
            r#"{"blocking_revision":3,"atom_count":2}"#,
        )
        .unwrap();
        let snapshot = MetadataSnapshot::open(&features, &blocking).unwrap();
        let mut resident = Vec::new();
        let work = expand_catalog_atom_pair(&snapshot, 0, 1, |left, right| {
            resident.push((left, right));
        })
        .unwrap();
        let left = atom_contracts(&snapshot, 0);
        let right = atom_contracts(&snapshot, 1);
        let mut scratch = CatalogBoundedExpansionScratch::new(2);

        assert!(scratch.retained_tokens_disjoint(snapshot.features(), left, right));
        scratch.prepare_chain_roots(snapshot.features(), left, right);
        let mut bounded = Vec::new();
        emit_chain_scoped_complete_bipartite_forest(
            snapshot.features(),
            left,
            right,
            &scratch.left_chain_roots,
            &scratch.right_chain_roots,
            |left, right| bounded.push((left, right)),
        );
        resident.sort_unstable();
        bounded.sort_unstable();

        assert_eq!(work, 256);
        assert_eq!(resident.len(), 16 + 16 - 1);
        assert_eq!(bounded, resident);
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
        let serial = build_rescue_execution_plan(
            &snapshot,
            &rescue,
            1,
            &memory,
            &dir.path().join("serial-spill"),
            |event| events.push(event),
            |_| {},
        )
        .unwrap();
        let parallel = build_rescue_execution_plan(
            &snapshot,
            &rescue,
            4,
            &memory,
            &dir.path().join("parallel-spill"),
            |_| {},
            |_| {},
        )
        .unwrap();
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

    fn three_atom_rescue_fixture() -> (tempfile::TempDir, MetadataSnapshot, RescuePlan) {
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
        (dir, snapshot, rescue)
    }

    #[test]
    fn rescue_execution_plan_counts_atom_scores_before_contract_expansion() {
        let (dir, snapshot, rescue) = three_atom_rescue_fixture();
        let mut events = Vec::new();

        let memory = MemoryBroker::new(512 << 30, 448 << 30).unwrap();
        let admitted = build_rescue_execution_plan(
            &snapshot,
            &rescue,
            1,
            &memory,
            &dir.path().join("spill"),
            |event| {
                events.push(event);
            },
            |_| {},
        )
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

    fn three_atom_rescue_memory_bytes() -> (u64, u64, u64) {
        let rescue_base_bytes = 3 * std::mem::size_of::<u32>() as u64 + 3 + 3;
        let full_cache_bytes =
            (MAX_RESCUE_PAYLOAD_CACHE_ENTRIES as u64) * RESCUE_PAYLOAD_CACHE_ENTRY_BYTES;
        let stream_bytes = rescue_stream_buffer_bytes(1).unwrap();
        (rescue_base_bytes, full_cache_bytes, stream_bytes)
    }

    #[test]
    fn rescue_flatten_regression_uses_one_pair_width_for_reported_failure() {
        const MATCHES: u64 = 17_180_694_886;
        const USED: u64 = 290_749_377_591;
        const HARD_TOP: u64 = 465_352_376_832;
        const EXPECTED_BYTES: u64 = 137_445_559_088;

        let bytes = rescue_final_match_bytes(MATCHES, 0).unwrap();
        assert_eq!(bytes, EXPECTED_BYTES);
        assert_eq!(HARD_TOP - USED - bytes, 37_157_440_153);

        let broker = MemoryBroker::new(512 * crate::resource::GIB, HARD_TOP).unwrap();
        let _used = broker.reserve(USED).unwrap();
        assert!(broker.reserve(bytes).is_ok());

        let old_broker = MemoryBroker::new(512 * crate::resource::GIB, HARD_TOP).unwrap();
        let _used = old_broker.reserve(USED).unwrap();
        assert!(matches!(
            old_broker.reserve(bytes * 2),
            Err(MemoryError::Budget { .. })
        ));
        assert_eq!(
            rescue_final_match_bytes(u64::MAX, 1),
            Err(MemoryError::Overflow)
        );
    }

    #[test]
    fn rescue_flatten_budget_shortage_warns_and_keeps_resident_chunks() {
        let (dir, snapshot, rescue) = three_atom_rescue_fixture();
        let (base, cache, stream) = three_atom_rescue_memory_bytes();
        let hard_top = base + cache + stream + RescueMatchChunk::retained_bytes().unwrap();
        let memory = MemoryBroker::new(512 * crate::resource::GIB, hard_top).unwrap();
        let mut advisories = Vec::new();

        let admitted = build_rescue_execution_plan(
            &snapshot,
            &rescue,
            1,
            &memory,
            &dir.path().join("chunk-fallback"),
            |_| {},
            |message| advisories.push(message.to_string()),
        )
        .unwrap();

        assert!(matches!(
            &admitted.plan.fallback,
            Some(RescueFallbackStorage::InMemoryChunks { .. })
        ));
        assert!(admitted.plan.matched_atom_pairs.is_empty());
        assert_eq!(admitted.plan.contract_expansion_visits, 2);
        assert_eq!(admitted.plan.execution_work(), 2);
        assert!(advisories
            .iter()
            .any(|message| message.contains("streaming them directly")));
    }

    #[test]
    fn rescue_corpus_budget_shortage_warns_and_spills_once() {
        let (dir, snapshot, rescue) = three_atom_rescue_fixture();
        let (base, cache, stream) = three_atom_rescue_memory_bytes();
        let hard_top = base + cache + stream;
        let memory = MemoryBroker::new(512 * crate::resource::GIB, hard_top).unwrap();
        let spill = dir.path().join("disk-fallback");
        let mut advisories = Vec::new();

        let admitted = build_rescue_execution_plan(
            &snapshot,
            &rescue,
            1,
            &memory,
            &spill,
            |_| {},
            |message| advisories.push(message.to_string()),
        )
        .unwrap();

        let files = match admitted.plan.fallback.as_ref().unwrap() {
            RescueFallbackStorage::Spilled { files, .. } => files,
            RescueFallbackStorage::InMemoryChunks { .. } => {
                panic!("expected disk-backed rescue fallback")
            }
        };
        let cancelled = std::sync::atomic::AtomicBool::new(false);
        let mut pairs = Vec::new();
        read_spilled_rescue_pair_batches(
            &files.atom_path,
            rescue_spill_batch_pairs(admitted.plan.stream_buffer_bytes),
            &cancelled,
            |batch| {
                pairs.extend_from_slice(batch);
                Ok(())
            },
        )
        .unwrap();

        pairs.sort_unstable();
        assert_eq!(pairs, vec![(0, 1), (0, 2)]);
        assert_eq!(std::fs::metadata(&files.atom_path).unwrap().len(), 16);
        assert_eq!(admitted.plan.contract_expansion_visits, 2);
        assert_eq!(admitted.plan.execution_work(), 2);
        assert_eq!(
            advisories
                .iter()
                .filter(|message| message.contains("disk streaming"))
                .count(),
            1
        );
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
    fn match_storage_reservation_does_not_preflight_physical_space() {
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

        assert!(lease.is_some());
        assert!(advisories.is_empty());
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
