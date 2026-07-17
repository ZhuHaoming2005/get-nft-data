//! Full-universe Pair ExactIsland oracle for frozen sampled left frontiers.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::marker::PhantomData;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::blocking::{
    build_base_equivalent_atom_sketches_from_feature_view_parallel, LocalRoutingPlan,
};
use crate::cascade::{score_pair, PairScoreDecision};
use crate::format;
use crate::index::candidate_owner;
use crate::progress::{ProgressCounters, ProgressEvent, ProgressPhase, WorkUnit};
use crate::snapshot::MetadataSnapshot;

const EVIDENCE_ARTIFACT_REVISION: u32 = 7;
const SHARED_PAIR_TILE_MEMBERS: usize = 512;
pub const SHARED_EXACT_TOTAL_PAIR_SAMPLE: u64 = 8_000_000;

#[derive(Deserialize)]
struct EvidenceRevision {
    artifact_revision: u32,
}

#[derive(Debug, Clone, Copy)]
pub struct ExactEvidenceBudget {
    pub max_lefts: u64,
    pub max_pair_work: u64,
    pub max_artifact_bytes: u64,
    pub max_lanes: usize,
}

/// Frozen pair-evidence work selected before any ExactIsland scan starts.
/// Half of the configured work envelope is intentionally retained for
/// shared-token evidence so a successful calibration scan cannot make the
/// later holdout/evidence stages impossible to admit.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExactEvidencePlan {
    pub calibration_lefts: u64,
    pub holdout_lefts: u64,
    pub pair_work: u64,
    pub remaining_pair_work: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SharedTokenEvidencePlan {
    pub calibration_tokens: Vec<u32>,
    pub holdout_tokens: Vec<u32>,
    pub skipped_tokens: Vec<u32>,
    pub pair_work: u64,
    pub skipped_pair_work: u64,
    pub considered_pair_work: u64,
    pub work_strata: Vec<SharedTokenWorkStratum>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct SharedTokenWorkStratum {
    pub log2_pair_work: u32,
    pub considered_pair_work: u64,
    pub skipped_pair_work: u64,
}

impl SharedTokenEvidencePlan {
    pub fn covers_all_active_groups(&self, token_member_offsets: &[u64]) -> bool {
        if u32::try_from(token_member_offsets.len().saturating_sub(1)).is_err() {
            return false;
        }
        let selected = self
            .calibration_tokens
            .iter()
            .chain(&self.holdout_tokens)
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        token_member_offsets
            .windows(2)
            .enumerate()
            .filter(|(_, window)| window[1].saturating_sub(window[0]) >= 2)
            .all(|(token, _)| selected.contains(&(token as u32)))
            && self.skipped_tokens.is_empty()
    }
}

pub fn plan_exact_evidence(
    universe_atoms: u64,
    requested_lefts_per_partition: u64,
    max_pair_work: u64,
) -> Result<ExactEvidencePlan, ExactIslandError> {
    let work_per_left = universe_atoms.saturating_sub(1);
    if requested_lefts_per_partition == 0 || universe_atoms < 2 || work_per_left == 0 {
        return Ok(ExactEvidencePlan {
            calibration_lefts: 0,
            holdout_lefts: 0,
            pair_work: 0,
            remaining_pair_work: max_pair_work,
        });
    }
    let pair_envelope = max_pair_work / 2;
    let two_frontiers = work_per_left
        .checked_mul(2)
        .ok_or(ExactIslandError::Budget {
            resource: "pair_evidence_plan",
            requested: u64::MAX,
            limit: max_pair_work,
        })?;
    let available_per_partition = universe_atoms / 2;
    let selected = requested_lefts_per_partition
        .min(available_per_partition)
        .min(pair_envelope / two_frontiers);
    let pair_work = selected
        .checked_mul(2)
        .and_then(|lefts| lefts.checked_mul(work_per_left))
        .ok_or(ExactIslandError::Budget {
            resource: "pair_evidence_plan",
            requested: u64::MAX,
            limit: max_pair_work,
        })?;
    Ok(ExactEvidencePlan {
        calibration_lefts: selected,
        holdout_lefts: selected,
        pair_work,
        remaining_pair_work: max_pair_work.saturating_sub(pair_work),
    })
}

pub fn plan_shared_token_evidence(
    token_member_offsets: &[u64],
    sampled_tokens: &[u32],
    max_tokens_per_partition: u64,
    max_pair_work: u64,
) -> Result<SharedTokenEvidencePlan, ExactIslandError> {
    let token_count = token_member_offsets.len().saturating_sub(1);
    crate::identity::checked_u32_identity("shared-token identities", token_count as u64)?;
    let mut calibration_tokens = Vec::new();
    let mut holdout_tokens = Vec::new();
    let mut skipped_tokens = Vec::new();
    let mut skipped_pair_work = 0u64;
    let mut considered_pair_work = 0u64;
    let mut work_strata = std::collections::BTreeMap::<u32, SharedTokenWorkStratum>::new();
    let mut seen_tokens = std::collections::BTreeSet::new();
    let mut sample_index = 0usize;
    for &token in sampled_tokens {
        if token as usize >= token_count {
            return Err(ExactIslandError::SampleOutOfRange(token));
        }
        if !seen_tokens.insert(token) {
            continue;
        }
        let members = token_member_offsets[token as usize + 1]
            .saturating_sub(token_member_offsets[token as usize]);
        let work = members
            .checked_mul(members.saturating_sub(1))
            .and_then(|value| value.checked_div(2))
            .ok_or(ExactIslandError::Budget {
                resource: "shared_token_evidence_plan",
                requested: u64::MAX,
                limit: max_pair_work,
            })?;
        considered_pair_work =
            considered_pair_work
                .checked_add(work)
                .ok_or(ExactIslandError::Budget {
                    resource: "shared_token_considered_pair_work",
                    requested: u64::MAX,
                    limit: max_pair_work,
                })?;
        let log2_pair_work = if work == 0 {
            0
        } else {
            63 - work.leading_zeros()
        };
        let stratum = work_strata
            .entry(log2_pair_work)
            .or_insert(SharedTokenWorkStratum {
                log2_pair_work,
                considered_pair_work: 0,
                skipped_pair_work: 0,
            });
        stratum.considered_pair_work =
            stratum
                .considered_pair_work
                .checked_add(work)
                .ok_or(ExactIslandError::Budget {
                    resource: "shared_token_stratum_considered_pair_work",
                    requested: u64::MAX,
                    limit: max_pair_work,
                })?;
        let target = if sample_index.is_multiple_of(2) {
            &mut calibration_tokens
        } else {
            &mut holdout_tokens
        };
        sample_index = sample_index.saturating_add(1);
        if target.len() as u64 >= max_tokens_per_partition {
            skipped_tokens.push(token);
            skipped_pair_work =
                skipped_pair_work
                    .checked_add(work)
                    .ok_or(ExactIslandError::Budget {
                        resource: "shared_token_skipped_pair_work",
                        requested: u64::MAX,
                        limit: max_pair_work,
                    })?;
            work_strata
                .get_mut(&log2_pair_work)
                .expect("work stratum was inserted above")
                .skipped_pair_work = work_strata[&log2_pair_work]
                .skipped_pair_work
                .checked_add(work)
                .ok_or(ExactIslandError::Budget {
                    resource: "shared_token_stratum_skipped_pair_work",
                    requested: u64::MAX,
                    limit: max_pair_work,
                })?;
            continue;
        }
        target.push(token);
    }
    let selected_pair_population =
        considered_pair_work
            .checked_sub(skipped_pair_work)
            .ok_or(ExactIslandError::Budget {
                resource: "shared_token_selected_pair_work",
                requested: skipped_pair_work,
                limit: considered_pair_work,
            })?;
    let pair_work = selected_pair_population
        .min(max_pair_work)
        .min(SHARED_EXACT_TOTAL_PAIR_SAMPLE);
    Ok(SharedTokenEvidencePlan {
        calibration_tokens,
        holdout_tokens,
        skipped_tokens,
        pair_work,
        skipped_pair_work,
        considered_pair_work,
        work_strata: work_strata.into_values().collect(),
    })
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[repr(C)]
pub struct ExactMiss {
    pub left_atom: u32,
    pub right_atom: u32,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[repr(C)]
pub struct SharedTokenExactMiss {
    pub token_id: u32,
    pub left_contract: u32,
    pub right_contract: u32,
}

#[doc(hidden)]
pub trait MissRecord:
    Copy + Ord + std::fmt::Debug + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static
{
    const WIDTH: usize;
    fn word(self, index: usize) -> u32;
}

impl MissRecord for ExactMiss {
    const WIDTH: usize = 2;

    fn word(self, index: usize) -> u32 {
        [self.left_atom, self.right_atom][index]
    }
}

impl MissRecord for SharedTokenExactMiss {
    const WIDTH: usize = 3;

    fn word(self, index: usize) -> u32 {
        [self.token_id, self.left_contract, self.right_contract][index]
    }
}

struct MissWorkspace {
    root: PathBuf,
    cleanup: bool,
}

impl Drop for MissWorkspace {
    fn drop(&mut self) {
        if self.cleanup {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }
}

enum MissRowsInner<T> {
    Resident(Arc<[T]>),
    Mapped {
        words: Arc<crate::format::MappedU32Array>,
        workspace: Arc<MissWorkspace>,
        marker: PhantomData<T>,
    },
}

pub struct MissRows<T> {
    inner: MissRowsInner<T>,
}

impl<T> Clone for MissRows<T> {
    fn clone(&self) -> Self {
        let inner = match &self.inner {
            MissRowsInner::Resident(rows) => MissRowsInner::Resident(rows.clone()),
            MissRowsInner::Mapped {
                words, workspace, ..
            } => MissRowsInner::Mapped {
                words: words.clone(),
                workspace: workspace.clone(),
                marker: PhantomData,
            },
        };
        Self { inner }
    }
}

impl<T> From<Vec<T>> for MissRows<T> {
    fn from(rows: Vec<T>) -> Self {
        Self {
            inner: MissRowsInner::Resident(rows.into()),
        }
    }
}

impl<T: MissRecord> MissRows<T> {
    fn mapped(
        path: &Path,
        row_count: usize,
        workspace: Arc<MissWorkspace>,
    ) -> Result<Self, format::FormatError> {
        let words = Arc::new(crate::format::map_u32_array(path)?);
        let expected = row_count
            .checked_mul(T::WIDTH)
            .ok_or(format::FormatError::PayloadLengthOverflow)?;
        if words.len() != expected
            || std::mem::size_of::<T>() != T::WIDTH * std::mem::size_of::<u32>()
            || std::mem::align_of::<T>() > std::mem::align_of::<u32>()
        {
            return Err(format::FormatError::InvalidHeader);
        }
        Ok(Self {
            inner: MissRowsInner::Mapped {
                words,
                workspace,
                marker: PhantomData,
            },
        })
    }

    #[cfg(test)]
    fn is_mapped(&self) -> bool {
        matches!(self.inner, MissRowsInner::Mapped { .. })
    }
}

impl<T: MissRecord> Deref for MissRows<T> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        match &self.inner {
            MissRowsInner::Resident(rows) => rows,
            MissRowsInner::Mapped { words, .. } => {
                // SAFETY: `MissRecord` is implemented only for `#[repr(C)]`
                // all-u32 records. `mapped` verifies exact width, size, and
                // alignment after the typed-array checksum has been validated.
                unsafe {
                    std::slice::from_raw_parts(words.as_ptr().cast::<T>(), words.len() / T::WIDTH)
                }
            }
        }
    }
}

impl<T: MissRecord> std::fmt::Debug for MissRows<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.deref().fmt(formatter)
    }
}

impl<T: MissRecord> PartialEq for MissRows<T> {
    fn eq(&self, other: &Self) -> bool {
        self.deref() == other.deref()
    }
}

impl<T: MissRecord> Eq for MissRows<T> {}

impl<T: MissRecord> Serialize for MissRows<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.deref().serialize(serializer)
    }
}

impl<'de, T: MissRecord> Deserialize<'de> for MissRows<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Vec::<T>::deserialize(deserializer).map(Into::into)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExactEvidenceCluster {
    pub id: u32,
    pub exact_matches: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PairExactEvidence {
    pub artifact_revision: u32,
    pub match_semantics_revision: u32,
    pub snapshot_fingerprint: String,
    pub sampling_policy_digest: String,
    pub universe_atoms: u64,
    pub sampled_lefts: Vec<u32>,
    pub pair_work: u64,
    pub exact_matches: u64,
    pub clusters: Vec<ExactEvidenceCluster>,
    pub conservative_misses: MissRows<ExactMiss>,
    pub frontier_build_micros: u64,
    pub full_universe_scan_micros: u64,
    pub posting_finalize_micros: u64,
    pub oracle_score_micros: u64,
    pub full_scan_equivalents_micros: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SharedTokenExactEvidence {
    pub artifact_revision: u32,
    pub match_semantics_revision: u32,
    pub snapshot_fingerprint: String,
    pub sampling_policy_digest: String,
    pub calibration_tokens: Vec<u32>,
    pub holdout_tokens: Vec<u32>,
    pub pair_work: u64,
    pub calibration_pair_work: u64,
    pub holdout_pair_work: u64,
    pub exact_matches: u64,
    pub calibration_exact_matches: u64,
    pub holdout_exact_matches: u64,
    pub calibration_clusters: Vec<ExactEvidenceCluster>,
    pub holdout_clusters: Vec<ExactEvidenceCluster>,
    pub scratch_fallback_tokens: Vec<u32>,
    pub calibration_misses: MissRows<SharedTokenExactMiss>,
    pub holdout_misses: MissRows<SharedTokenExactMiss>,
}

const PAIR_MISS_FILE: &str = "pair-misses.u32";
const SHARED_CALIBRATION_MISS_FILE: &str = "calibration-misses.u32";
const SHARED_HOLDOUT_MISS_FILE: &str = "holdout-misses.u32";

#[derive(Serialize)]
struct PairExactEvidenceReadyRef<'a> {
    artifact_revision: u32,
    match_semantics_revision: u32,
    snapshot_fingerprint: &'a str,
    sampling_policy_digest: &'a str,
    universe_atoms: u64,
    sampled_lefts: &'a [u32],
    pair_work: u64,
    exact_matches: u64,
    clusters: &'a [ExactEvidenceCluster],
    conservative_miss_count: usize,
    conservative_miss_file: &'static str,
    frontier_build_micros: u64,
    full_universe_scan_micros: u64,
    posting_finalize_micros: u64,
    oracle_score_micros: u64,
    full_scan_equivalents_micros: u64,
}

#[derive(Deserialize)]
struct PairExactEvidenceReady {
    artifact_revision: u32,
    match_semantics_revision: u32,
    snapshot_fingerprint: String,
    sampling_policy_digest: String,
    universe_atoms: u64,
    sampled_lefts: Vec<u32>,
    pair_work: u64,
    exact_matches: u64,
    clusters: Vec<ExactEvidenceCluster>,
    conservative_miss_count: usize,
    conservative_miss_file: String,
    frontier_build_micros: u64,
    full_universe_scan_micros: u64,
    posting_finalize_micros: u64,
    oracle_score_micros: u64,
    full_scan_equivalents_micros: u64,
}

#[derive(Serialize)]
struct SharedTokenExactEvidenceReadyRef<'a> {
    artifact_revision: u32,
    match_semantics_revision: u32,
    snapshot_fingerprint: &'a str,
    sampling_policy_digest: &'a str,
    calibration_tokens: &'a [u32],
    holdout_tokens: &'a [u32],
    pair_work: u64,
    calibration_pair_work: u64,
    holdout_pair_work: u64,
    exact_matches: u64,
    calibration_exact_matches: u64,
    holdout_exact_matches: u64,
    calibration_clusters: &'a [ExactEvidenceCluster],
    holdout_clusters: &'a [ExactEvidenceCluster],
    scratch_fallback_tokens: &'a [u32],
    calibration_miss_count: usize,
    calibration_miss_file: &'static str,
    holdout_miss_count: usize,
    holdout_miss_file: &'static str,
}

#[derive(Deserialize)]
struct SharedTokenExactEvidenceReady {
    artifact_revision: u32,
    match_semantics_revision: u32,
    snapshot_fingerprint: String,
    sampling_policy_digest: String,
    calibration_tokens: Vec<u32>,
    holdout_tokens: Vec<u32>,
    pair_work: u64,
    calibration_pair_work: u64,
    holdout_pair_work: u64,
    exact_matches: u64,
    calibration_exact_matches: u64,
    holdout_exact_matches: u64,
    calibration_clusters: Vec<ExactEvidenceCluster>,
    holdout_clusters: Vec<ExactEvidenceCluster>,
    scratch_fallback_tokens: Vec<u32>,
    calibration_miss_count: usize,
    calibration_miss_file: String,
    holdout_miss_count: usize,
    holdout_miss_file: String,
}

fn cluster_total(clusters: &[ExactEvidenceCluster]) -> Option<u64> {
    clusters.iter().try_fold(0u64, |total, cluster| {
        total.checked_add(cluster.exact_matches)
    })
}

fn pair_frontier_work(universe_atoms: u64, sampled_count: u64) -> Option<u64> {
    sampled_count
        .checked_mul(universe_atoms.saturating_sub(1))
        .and_then(|work| {
            sampled_count
                .checked_mul(sampled_count.saturating_sub(1))
                .map(|duplicates| work.saturating_sub(duplicates / 2))
        })
}

fn pair_evidence_is_consistent(evidence: &PairExactEvidence) -> bool {
    let cluster_ids_match = evidence.clusters.len() == evidence.sampled_lefts.len()
        && evidence
            .clusters
            .iter()
            .zip(&evidence.sampled_lefts)
            .all(|(cluster, &left)| cluster.id == left);
    let misses_are_canonical = evidence.conservative_misses.windows(2).all(|pair| {
        (pair[0].left_atom, pair[0].right_atom) < (pair[1].left_atom, pair[1].right_atom)
    }) && evidence.conservative_misses.iter().all(|miss| {
        miss.left_atom < miss.right_atom
            && u64::from(miss.right_atom) < evidence.universe_atoms
            && (evidence
                .sampled_lefts
                .binary_search(&miss.left_atom)
                .is_ok()
                || evidence
                    .sampled_lefts
                    .binary_search(&miss.right_atom)
                    .is_ok())
            && evidence.clusters.iter().any(|cluster| {
                (cluster.id == miss.left_atom || cluster.id == miss.right_atom)
                    && cluster.exact_matches != 0
            })
    });
    cluster_ids_match
        && evidence
            .sampled_lefts
            .windows(2)
            .all(|pair| pair[0] < pair[1])
        && evidence
            .sampled_lefts
            .last()
            .is_none_or(|&left| u64::from(left) < evidence.universe_atoms)
        && cluster_total(&evidence.clusters) == Some(evidence.exact_matches)
        && evidence.exact_matches <= evidence.pair_work
        && evidence.conservative_misses.len() as u64 <= evidence.exact_matches
        && pair_frontier_work(evidence.universe_atoms, evidence.sampled_lefts.len() as u64)
            == Some(evidence.pair_work)
        && misses_are_canonical
}

fn shared_partition_is_consistent(
    tokens: &[u32],
    clusters: &[ExactEvidenceCluster],
    misses: &[SharedTokenExactMiss],
    exact_matches: u64,
    contract_count: usize,
) -> bool {
    clusters.len() == tokens.len()
        && clusters
            .iter()
            .zip(tokens)
            .all(|(cluster, &token)| cluster.id == token)
        && cluster_total(clusters) == Some(exact_matches)
        && misses.len() as u64 <= exact_matches
        && misses.windows(2).all(|pair| {
            (
                pair[0].token_id,
                pair[0].left_contract,
                pair[0].right_contract,
            ) < (
                pair[1].token_id,
                pair[1].left_contract,
                pair[1].right_contract,
            )
        })
        && misses.iter().all(|miss| {
            miss.left_contract < miss.right_contract
                && (miss.right_contract as usize) < contract_count
                && tokens.binary_search(&miss.token_id).is_ok()
                && clusters
                    .iter()
                    .any(|cluster| cluster.id == miss.token_id && cluster.exact_matches != 0)
        })
}

fn shared_pair_work(snapshot: &MetadataSnapshot, tokens: &[u32]) -> Option<u64> {
    tokens.iter().try_fold(0u64, |total, &token| {
        let begin = *snapshot
            .features()
            .token_member_offsets
            .get(token as usize)?;
        let end = *snapshot
            .features()
            .token_member_offsets
            .get(token as usize + 1)?;
        let members = end.checked_sub(begin)?;
        let pairs = members.checked_mul(members.saturating_sub(1))? / 2;
        total.checked_add(pairs)
    })
}

fn shared_evidence_is_consistent(
    evidence: &SharedTokenExactEvidence,
    snapshot: &MetadataSnapshot,
) -> bool {
    let tokens_are_disjoint = evidence
        .calibration_tokens
        .iter()
        .all(|token| evidence.holdout_tokens.binary_search(token).is_err());
    let calibration_population = shared_pair_work(snapshot, &evidence.calibration_tokens);
    let holdout_population = shared_pair_work(snapshot, &evidence.holdout_tokens);
    let scratch_fallbacks_are_valid = evidence
        .scratch_fallback_tokens
        .windows(2)
        .all(|pair| pair[0] < pair[1])
        && evidence.scratch_fallback_tokens.iter().all(|token| {
            evidence.calibration_tokens.binary_search(token).is_ok()
                || evidence.holdout_tokens.binary_search(token).is_ok()
        });
    tokens_are_disjoint
        && scratch_fallbacks_are_valid
        && evidence
            .calibration_tokens
            .windows(2)
            .all(|pair| pair[0] < pair[1])
        && evidence
            .holdout_tokens
            .windows(2)
            .all(|pair| pair[0] < pair[1])
        && calibration_population.is_some_and(|work| evidence.calibration_pair_work <= work)
        && holdout_population.is_some_and(|work| evidence.holdout_pair_work <= work)
        && evidence
            .calibration_pair_work
            .checked_add(evidence.holdout_pair_work)
            == Some(evidence.pair_work)
        && evidence
            .calibration_exact_matches
            .checked_add(evidence.holdout_exact_matches)
            == Some(evidence.exact_matches)
        && evidence.calibration_exact_matches <= evidence.calibration_pair_work
        && evidence.holdout_exact_matches <= evidence.holdout_pair_work
        && evidence.exact_matches <= evidence.pair_work
        && shared_partition_is_consistent(
            &evidence.calibration_tokens,
            &evidence.calibration_clusters,
            &evidence.calibration_misses,
            evidence.calibration_exact_matches,
            snapshot.contract_count(),
        )
        && shared_partition_is_consistent(
            &evidence.holdout_tokens,
            &evidence.holdout_clusters,
            &evidence.holdout_misses,
            evidence.holdout_exact_matches,
            snapshot.contract_count(),
        )
}

#[derive(Debug, Error)]
pub enum ExactIslandError {
    #[error("stale ExactEvidence checkpoint: {0}")]
    StaleEvidence(String),
    #[error("parallel ExactEvidence execution failed: {0}")]
    Parallel(String),
    #[error("invalid ExactEvidence invariant: {0}")]
    InvalidEvidence(&'static str),
    #[error("ExactEvidence budget exceeded for {resource}: requested {requested}, limit {limit}")]
    Budget {
        resource: &'static str,
        requested: u64,
        limit: u64,
    },
    #[error("sample atom {0} outside snapshot universe")]
    SampleOutOfRange(u32),
    #[error(transparent)]
    Identity(#[from] crate::identity::IdentityOverflow),
    #[error(transparent)]
    Format(#[from] format::FormatError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

struct ResidentMissBudget {
    max_records: usize,
    used_records: Mutex<usize>,
}

impl ResidentMissBudget {
    fn for_record<T>(max_bytes: u64) -> Self {
        let record_bytes = std::mem::size_of::<T>().max(1) as u64;
        Self {
            max_records: usize::try_from(max_bytes / record_bytes).unwrap_or(usize::MAX),
            used_records: Mutex::new(0),
        }
    }

    fn try_reserve(&self, records: usize) -> bool {
        let mut used = self.used_records.lock().expect("resident miss budget");
        let Some(next) = used.checked_add(records) else {
            return false;
        };
        if next > self.max_records {
            return false;
        }
        *used = next;
        true
    }

    fn release(&self, records: usize) {
        let mut used = self.used_records.lock().expect("resident miss budget");
        *used = used.saturating_sub(records);
    }
}

static MISS_WORKSPACE_ID: AtomicU64 = AtomicU64::new(0);

fn miss_workspace(
    output_dir: Option<&Path>,
    label: &str,
) -> Result<Arc<MissWorkspace>, ExactIslandError> {
    let (root, cleanup) = if let Some(output_dir) = output_dir {
        (output_dir.to_path_buf(), false)
    } else {
        let id = MISS_WORKSPACE_ID.fetch_add(1, Ordering::Relaxed);
        (
            std::env::temp_dir().join(format!(
                "metadata-exact-{label}-{}-{id}",
                std::process::id()
            )),
            true,
        )
    };
    std::fs::create_dir_all(&root).map_err(format::FormatError::from)?;
    Ok(Arc::new(MissWorkspace { root, cleanup }))
}

struct MissSpoolState<T> {
    resident: Vec<T>,
    runs: Vec<PathBuf>,
    next_run: usize,
}

struct MissSpool<T> {
    state: Mutex<MissSpoolState<T>>,
    budget: Arc<ResidentMissBudget>,
    workspace: Arc<MissWorkspace>,
    prefix: &'static str,
}

impl<T: MissRecord> MissSpool<T> {
    fn new(
        budget: Arc<ResidentMissBudget>,
        workspace: Arc<MissWorkspace>,
        prefix: &'static str,
    ) -> Self {
        Self {
            state: Mutex::new(MissSpoolState {
                resident: Vec::new(),
                runs: Vec::new(),
                next_run: 0,
            }),
            budget,
            workspace,
            prefix,
        }
    }

    fn push_chunk(&self, mut chunk: Vec<T>) -> Result<(), ExactIslandError> {
        if chunk.is_empty() {
            return Ok(());
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| ExactIslandError::Parallel("miss spool lock poisoned".into()))?;
        if state.runs.is_empty() && self.budget.try_reserve(chunk.len()) {
            if state.resident.try_reserve_exact(chunk.len()).is_ok() {
                state.resident.append(&mut chunk);
                return Ok(());
            }
            self.budget.release(chunk.len());
        }
        if !state.resident.is_empty() {
            let resident = std::mem::take(&mut state.resident);
            self.budget.release(resident.len());
            self.write_run(&mut state, resident)?;
        }
        self.write_run(&mut state, chunk)
    }

    fn write_run(
        &self,
        state: &mut MissSpoolState<T>,
        mut rows: Vec<T>,
    ) -> Result<(), ExactIslandError> {
        rows.sort_unstable();
        rows.dedup();
        let path = self
            .workspace
            .root
            .join(format!("{}-run-{:06}.u32", self.prefix, state.next_run));
        state.next_run = state.next_run.saturating_add(1);
        write_miss_rows(&path, &rows)?;
        state.runs.push(path);
        Ok(())
    }

    fn finish(
        &self,
        final_name: &'static str,
        persist_resident: bool,
    ) -> Result<MissRows<T>, ExactIslandError> {
        let (mut resident, mut runs) = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| ExactIslandError::Parallel("miss spool lock poisoned".into()))?;
            (
                std::mem::take(&mut state.resident),
                std::mem::take(&mut state.runs),
            )
        };
        self.budget.release(resident.len());
        resident.sort_unstable();
        resident.dedup();
        if runs.is_empty() && !persist_resident {
            return Ok(resident.into());
        }
        if !resident.is_empty() {
            let path = self
                .workspace
                .root
                .join(format!("{}-run-final.u32", self.prefix));
            write_miss_rows(&path, &resident)?;
            runs.push(path);
        }
        let final_path = self.workspace.root.join(final_name);
        let row_count = if runs.is_empty() {
            write_miss_rows::<T>(&final_path, &[])?;
            0
        } else {
            merge_miss_runs::<T>(&runs, &final_path)?
        };
        let mapped = MissRows::mapped(&final_path, row_count, self.workspace.clone())?;
        for run in runs {
            let _ = std::fs::remove_file(run);
        }
        Ok(mapped)
    }
}

fn write_miss_rows<T: MissRecord>(path: &Path, rows: &[T]) -> Result<(), ExactIslandError> {
    let words = (rows.len() as u64)
        .checked_mul(T::WIDTH as u64)
        .ok_or(format::FormatError::PayloadLengthOverflow)?;
    crate::format::write_u32_iter(
        path,
        crate::format::ArrayKind::U32,
        words,
        rows.iter()
            .copied()
            .flat_map(|row| (0..T::WIDTH).map(move |index| row.word(index))),
    )?;
    Ok(())
}

fn mapped_miss_row<T: MissRecord>(words: &crate::format::MappedU32Array, row: usize) -> T {
    // SAFETY: run files are produced only by `write_miss_rows`, and callers
    // validate that the u32 element count is an exact multiple of T::WIDTH.
    unsafe { *words.as_ptr().add(row * T::WIDTH).cast::<T>() }
}

fn visit_merged_miss_runs<T: MissRecord>(
    runs: &[crate::format::MappedU32Array],
    mut visit: impl FnMut(T) -> Result<(), ExactIslandError>,
) -> Result<(), ExactIslandError> {
    let mut positions = vec![0usize; runs.len()];
    let mut heap = BinaryHeap::<Reverse<(T, usize)>>::new();
    for (run, words) in runs.iter().enumerate() {
        if !words.len().is_multiple_of(T::WIDTH) {
            return Err(format::FormatError::InvalidHeader.into());
        }
        if !words.is_empty() {
            heap.push(Reverse((mapped_miss_row::<T>(words, 0), run)));
        }
    }
    let mut previous = None;
    while let Some(Reverse((row, run))) = heap.pop() {
        if previous != Some(row) {
            visit(row)?;
            previous = Some(row);
        }
        positions[run] += 1;
        if positions[run] < runs[run].len() / T::WIDTH {
            heap.push(Reverse((
                mapped_miss_row::<T>(&runs[run], positions[run]),
                run,
            )));
        }
    }
    Ok(())
}

fn merge_miss_runs<T: MissRecord>(
    run_paths: &[PathBuf],
    final_path: &Path,
) -> Result<usize, ExactIslandError> {
    let runs = run_paths
        .iter()
        .map(|path| crate::format::map_u32_array(path))
        .collect::<Result<Vec<_>, _>>()?;
    let mut row_count = 0u64;
    visit_merged_miss_runs::<T>(&runs, |_| {
        row_count = row_count.checked_add(1).ok_or(ExactIslandError::Budget {
            resource: "external_miss_rows",
            requested: u64::MAX,
            limit: u64::MAX - 1,
        })?;
        Ok(())
    })?;
    let word_count = row_count
        .checked_mul(T::WIDTH as u64)
        .ok_or(format::FormatError::PayloadLengthOverflow)?;
    let mut sink = crate::format::TypedArraySink::create(
        final_path,
        crate::format::ArrayKind::U32,
        word_count,
    )?;
    visit_merged_miss_runs::<T>(&runs, |row| {
        for index in 0..T::WIDTH {
            sink.push_u32(row.word(index))?;
        }
        Ok(())
    })?;
    sink.finish()?;
    usize::try_from(row_count).map_err(|_| ExactIslandError::Budget {
        resource: "external_miss_rows",
        requested: row_count,
        limit: usize::MAX as u64,
    })
}

pub fn open_pair_exact_evidence(
    directory: &Path,
    snapshot: &MetadataSnapshot,
    sampled_lefts: &[u32],
) -> Result<Option<PairExactEvidence>, ExactIslandError> {
    let ready = directory.join("ready");
    if !ready.is_file() {
        return Ok(None);
    }
    let bytes = std::fs::read(&ready).map_err(format::FormatError::from)?;
    let revision: EvidenceRevision = serde_json::from_slice(&bytes)?;
    if revision.artifact_revision != EVIDENCE_ARTIFACT_REVISION {
        std::fs::remove_file(ready).map_err(format::FormatError::from)?;
        return Ok(None);
    }
    let ready_evidence: PairExactEvidenceReady = serde_json::from_slice(&bytes)?;
    if ready_evidence.conservative_miss_file != PAIR_MISS_FILE {
        std::fs::remove_file(ready).map_err(format::FormatError::from)?;
        return Ok(None);
    }
    let workspace = Arc::new(MissWorkspace {
        root: directory.to_path_buf(),
        cleanup: false,
    });
    let conservative_misses = MissRows::mapped(
        &directory.join(PAIR_MISS_FILE),
        ready_evidence.conservative_miss_count,
        workspace,
    )?;
    let evidence = PairExactEvidence {
        artifact_revision: ready_evidence.artifact_revision,
        match_semantics_revision: ready_evidence.match_semantics_revision,
        snapshot_fingerprint: ready_evidence.snapshot_fingerprint,
        sampling_policy_digest: ready_evidence.sampling_policy_digest,
        universe_atoms: ready_evidence.universe_atoms,
        sampled_lefts: ready_evidence.sampled_lefts,
        pair_work: ready_evidence.pair_work,
        exact_matches: ready_evidence.exact_matches,
        clusters: ready_evidence.clusters,
        conservative_misses,
        frontier_build_micros: ready_evidence.frontier_build_micros,
        full_universe_scan_micros: ready_evidence.full_universe_scan_micros,
        posting_finalize_micros: ready_evidence.posting_finalize_micros,
        oracle_score_micros: ready_evidence.oracle_score_micros,
        full_scan_equivalents_micros: ready_evidence.full_scan_equivalents_micros,
    };
    let mut expected = sampled_lefts.to_vec();
    expected.sort_unstable();
    expected.dedup();
    if evidence.match_semantics_revision != crate::scoring::MATCH_SEMANTICS_REVISION
        || evidence.snapshot_fingerprint != crate::scheduler::snapshot_fingerprint(snapshot)
        || evidence.sampling_policy_digest != pair_sampling_digest(&expected)
        || evidence.universe_atoms != snapshot.atom_count() as u64
        || evidence.sampled_lefts != expected
        || !pair_evidence_is_consistent(&evidence)
    {
        std::fs::remove_file(ready).map_err(format::FormatError::from)?;
        return Ok(None);
    }
    Ok(Some(evidence))
}

pub fn open_shared_token_exact_evidence(
    directory: &Path,
    snapshot: &MetadataSnapshot,
    calibration_tokens: &[u32],
    holdout_tokens: &[u32],
) -> Result<Option<SharedTokenExactEvidence>, ExactIslandError> {
    let ready = directory.join("ready");
    if !ready.is_file() {
        return Ok(None);
    }
    let bytes = std::fs::read(&ready).map_err(format::FormatError::from)?;
    let revision: EvidenceRevision = serde_json::from_slice(&bytes)?;
    if revision.artifact_revision != EVIDENCE_ARTIFACT_REVISION {
        std::fs::remove_file(ready).map_err(format::FormatError::from)?;
        return Ok(None);
    }
    let ready_evidence: SharedTokenExactEvidenceReady = serde_json::from_slice(&bytes)?;
    if ready_evidence.calibration_miss_file != SHARED_CALIBRATION_MISS_FILE
        || ready_evidence.holdout_miss_file != SHARED_HOLDOUT_MISS_FILE
    {
        std::fs::remove_file(ready).map_err(format::FormatError::from)?;
        return Ok(None);
    }
    let workspace = Arc::new(MissWorkspace {
        root: directory.to_path_buf(),
        cleanup: false,
    });
    let calibration_misses = MissRows::mapped(
        &directory.join(SHARED_CALIBRATION_MISS_FILE),
        ready_evidence.calibration_miss_count,
        workspace.clone(),
    )?;
    let holdout_misses = MissRows::mapped(
        &directory.join(SHARED_HOLDOUT_MISS_FILE),
        ready_evidence.holdout_miss_count,
        workspace,
    )?;
    let evidence = SharedTokenExactEvidence {
        artifact_revision: ready_evidence.artifact_revision,
        match_semantics_revision: ready_evidence.match_semantics_revision,
        snapshot_fingerprint: ready_evidence.snapshot_fingerprint,
        sampling_policy_digest: ready_evidence.sampling_policy_digest,
        calibration_tokens: ready_evidence.calibration_tokens,
        holdout_tokens: ready_evidence.holdout_tokens,
        pair_work: ready_evidence.pair_work,
        calibration_pair_work: ready_evidence.calibration_pair_work,
        holdout_pair_work: ready_evidence.holdout_pair_work,
        exact_matches: ready_evidence.exact_matches,
        calibration_exact_matches: ready_evidence.calibration_exact_matches,
        holdout_exact_matches: ready_evidence.holdout_exact_matches,
        calibration_clusters: ready_evidence.calibration_clusters,
        holdout_clusters: ready_evidence.holdout_clusters,
        scratch_fallback_tokens: ready_evidence.scratch_fallback_tokens,
        calibration_misses,
        holdout_misses,
    };
    let mut expected_calibration = calibration_tokens.to_vec();
    expected_calibration.sort_unstable();
    expected_calibration.dedup();
    let mut expected_holdout = holdout_tokens.to_vec();
    expected_holdout.sort_unstable();
    expected_holdout.dedup();
    expected_holdout.retain(|token| expected_calibration.binary_search(token).is_err());
    if evidence.match_semantics_revision != crate::scoring::MATCH_SEMANTICS_REVISION
        || evidence.snapshot_fingerprint != crate::scheduler::snapshot_fingerprint(snapshot)
        || evidence.sampling_policy_digest
            != shared_sampling_digest(&expected_calibration, &expected_holdout)
        || evidence.calibration_tokens != expected_calibration
        || evidence.holdout_tokens != expected_holdout
        || !shared_evidence_is_consistent(&evidence, snapshot)
    {
        std::fs::remove_file(ready).map_err(format::FormatError::from)?;
        return Ok(None);
    }
    Ok(Some(evidence))
}

pub fn run_shared_token_exact_islands(
    snapshot: &MetadataSnapshot,
    calibration_tokens: &[u32],
    holdout_tokens: &[u32],
    budget: ExactEvidenceBudget,
    output_dir: Option<&Path>,
) -> Result<SharedTokenExactEvidence, ExactIslandError> {
    run_shared_token_exact_islands_with_progress(
        snapshot,
        calibration_tokens,
        holdout_tokens,
        budget,
        output_dir,
        |_| {},
    )
}

fn proportional_partition_samples(
    token_member_offsets: &[u64],
    tokens: &[u32],
    sample_cap: u64,
) -> Result<HashMap<u32, u64>, ExactIslandError> {
    let mut population = 0u64;
    let mut rows = Vec::with_capacity(tokens.len());
    for &token in tokens {
        let begin = token_member_offsets[token as usize];
        let end = token_member_offsets[token as usize + 1];
        let members = end.saturating_sub(begin);
        let work = members
            .checked_mul(members.saturating_sub(1))
            .and_then(|value| value.checked_div(2))
            .ok_or(ExactIslandError::Budget {
                resource: "adaptive_shared_token_population",
                requested: u64::MAX,
                limit: sample_cap,
            })?;
        population = population
            .checked_add(work)
            .ok_or(ExactIslandError::Budget {
                resource: "adaptive_shared_token_population",
                requested: u64::MAX,
                limit: sample_cap,
            })?;
        rows.push((token, work, 0u64, 0u64));
    }
    let target = sample_cap.min(population);
    if population == 0 || target == population {
        return Ok(rows
            .into_iter()
            .map(|(token, work, _, _)| (token, work))
            .collect());
    }
    let mut allocated = 0u64;
    for (_, work, sample, remainder) in &mut rows {
        let scaled = u128::from(*work).saturating_mul(u128::from(target));
        *sample = (scaled / u128::from(population)) as u64;
        *remainder = (scaled % u128::from(population)) as u64;
        allocated = allocated.saturating_add(*sample);
    }
    let mut order = (0..rows.len()).collect::<Vec<_>>();
    order.sort_unstable_by(|&left, &right| {
        rows[right]
            .3
            .cmp(&rows[left].3)
            .then_with(|| rows[left].0.cmp(&rows[right].0))
    });
    let mut remaining = target.saturating_sub(allocated);
    for index in order {
        if remaining == 0 {
            break;
        }
        if rows[index].2 < rows[index].1 {
            rows[index].2 += 1;
            remaining -= 1;
        }
    }
    if remaining != 0 {
        return Err(ExactIslandError::Budget {
            resource: "adaptive_shared_token_apportionment",
            requested: target,
            limit: target.saturating_sub(remaining),
        });
    }
    let active_groups = rows.iter().filter(|row| row.1 != 0).count() as u64;
    if target >= active_groups {
        let zero_sample_groups = rows
            .iter()
            .enumerate()
            .filter_map(|(index, row)| (row.1 != 0 && row.2 == 0).then_some(index))
            .collect::<Vec<_>>();
        for zero in zero_sample_groups {
            let donor = rows
                .iter()
                .enumerate()
                .filter(|(_, row)| row.2 > 1)
                .max_by(|(_, left), (_, right)| {
                    left.2.cmp(&right.2).then_with(|| right.0.cmp(&left.0))
                })
                .map(|(index, _)| index)
                .ok_or(ExactIslandError::Budget {
                    resource: "adaptive_shared_token_minimum_group_sample",
                    requested: active_groups,
                    limit: target,
                })?;
            rows[donor].2 -= 1;
            rows[zero].2 = 1;
        }
    }
    Ok(rows
        .into_iter()
        .map(|(token, _, sample, _)| (token, sample))
        .collect())
}

pub fn run_shared_token_exact_islands_with_progress(
    snapshot: &MetadataSnapshot,
    calibration_tokens: &[u32],
    holdout_tokens: &[u32],
    budget: ExactEvidenceBudget,
    output_dir: Option<&Path>,
    mut progress: impl FnMut(ProgressEvent),
) -> Result<SharedTokenExactEvidence, ExactIslandError> {
    let token_count = snapshot
        .features()
        .token_member_offsets
        .len()
        .saturating_sub(1);
    crate::identity::checked_u32_identity("shared-token identities", token_count as u64)?;
    let mut calibration = normalized_tokens(calibration_tokens, token_count)?;
    let mut holdout = normalized_tokens(holdout_tokens, token_count)?;
    holdout.retain(|token| calibration.binary_search(token).is_err());
    checked(
        "shared_token_sample_groups",
        calibration.len().saturating_add(holdout.len()) as u64,
        budget.max_lefts,
    )?;

    let total_sample_cap = SHARED_EXACT_TOTAL_PAIR_SAMPLE.min(budget.max_pair_work);
    let (calibration_cap, holdout_cap) = match (calibration.is_empty(), holdout.is_empty()) {
        (false, false) => (
            total_sample_cap / 2,
            total_sample_cap - total_sample_cap / 2,
        ),
        (false, true) => (total_sample_cap, 0),
        (true, false) => (0, total_sample_cap),
        (true, true) => (0, 0),
    };
    let calibration_samples = proportional_partition_samples(
        &snapshot.features().token_member_offsets,
        &calibration,
        calibration_cap,
    )?;
    let holdout_samples = proportional_partition_samples(
        &snapshot.features().token_member_offsets,
        &holdout,
        holdout_cap,
    )?;
    let total_pair_work = calibration_samples
        .values()
        .chain(holdout_samples.values())
        .try_fold(0u64, |total, &work| {
            total.checked_add(work).ok_or(ExactIslandError::Budget {
                resource: "adaptive_shared_token_pair_work",
                requested: u64::MAX,
                limit: budget.max_pair_work,
            })
        })?;
    progress(ProgressEvent::determinate(
        ProgressPhase::SharedTokenExactIsland,
        0,
        total_pair_work,
        WorkUnit::Pairs,
        ProgressCounters::default(),
    ));

    let groups = calibration
        .iter()
        .copied()
        .map(|token| (token, true, calibration_samples[&token]))
        .chain(
            holdout
                .iter()
                .copied()
                .map(|token| (token, false, holdout_samples[&token])),
        )
        .collect::<Vec<_>>();
    let max_group_scratch_bytes =
        groups
            .iter()
            .try_fold(0u64, |maximum, &(token, _, sample_work)| {
                if sample_work == 0 {
                    Ok(maximum)
                } else {
                    shared_group_scratch_upper_bound(snapshot, token, sample_work)
                        .map(|bytes| maximum.max(bytes))
                }
            })?;
    // The caller reserves three artifact budgets for ExactEvidence. Misses may
    // use one third; the remaining two thirds bound concurrent routing/tile
    // scratch. Large groups still exploit the full Rayon pool internally.
    let scratch_bytes = budget.max_artifact_bytes.saturating_mul(2);
    let concurrent_group_lanes = scratch_bytes
        .checked_div(max_group_scratch_bytes)
        .map_or(budget.max_lanes.max(1), |lanes| {
            lanes.max(1).min(budget.max_lanes.max(1) as u64) as usize
        });
    let routing_scratch_budget_per_group = scratch_bytes
        .checked_div(concurrent_group_lanes.max(1) as u64)
        .unwrap_or_default();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(budget.max_lanes.max(1))
        .thread_name(|index| format!("metadata-shared-exact-{index}"))
        .build()
        .map_err(|error| ExactIslandError::Parallel(error.to_string()))?;
    let lanes = budget.max_lanes.max(1);
    enum SharedScanMessage {
        Work {
            work: u64,
            is_calibration: bool,
        },
        Done {
            token: u32,
            is_calibration: bool,
            result: Result<SharedTokenGroupScan, ExactIslandError>,
        },
    }
    let (sender, receiver) = std::sync::mpsc::sync_channel(lanes.saturating_mul(4).max(1));
    // Calibration and holdout share one resident envelope. Either partition
    // may use all available miss memory; overflow is sorted into checksummed
    // runs instead of cancelling the ExactEvidence scan.
    let shared_miss_workspace = miss_workspace(output_dir, "shared")?;
    let shared_miss_budget = Arc::new(ResidentMissBudget::for_record::<SharedTokenExactMiss>(
        budget.max_artifact_bytes,
    ));
    let calibration_miss_spool = Arc::new(MissSpool::<SharedTokenExactMiss>::new(
        shared_miss_budget.clone(),
        shared_miss_workspace.clone(),
        "calibration-misses",
    ));
    let holdout_miss_spool = Arc::new(MissSpool::<SharedTokenExactMiss>::new(
        shared_miss_budget,
        shared_miss_workspace,
        "holdout-misses",
    ));
    let (
        pair_work,
        calibration_pair_work,
        holdout_pair_work,
        calibration_exact_matches,
        holdout_exact_matches,
        mut calibration_clusters,
        mut holdout_clusters,
        mut scratch_fallback_tokens,
    ) = std::thread::scope(|scope| -> Result<_, ExactIslandError> {
        let worker_sender = sender.clone();
        let calibration_worker_spool = calibration_miss_spool.clone();
        let holdout_worker_spool = holdout_miss_spool.clone();
        let worker = scope.spawn(move || {
            pool.install(|| {
                for wave in groups.chunks(concurrent_group_lanes.max(1)) {
                    wave.par_iter()
                        .for_each(|&(token, is_calibration, sample_work)| {
                            let miss_spool = if is_calibration {
                                &calibration_worker_spool
                            } else {
                                &holdout_worker_spool
                            };
                            let result = scan_shared_token_group(
                                snapshot,
                                token,
                                sample_work,
                                budget,
                                routing_scratch_budget_per_group,
                                miss_spool,
                                |work| {
                                    let _ = worker_sender.send(SharedScanMessage::Work {
                                        work,
                                        is_calibration,
                                    });
                                },
                            );
                            let _ = worker_sender.send(SharedScanMessage::Done {
                                token,
                                is_calibration,
                                result,
                            });
                        });
                }
            });
        });
        drop(sender);
        let mut pair_work = 0u64;
        let mut calibration_pair_work = 0u64;
        let mut holdout_pair_work = 0u64;
        let mut calibration_exact_matches = 0u64;
        let mut holdout_exact_matches = 0u64;
        let mut calibration_clusters = Vec::new();
        let mut holdout_clusters = Vec::new();
        let mut scratch_fallback_tokens = Vec::new();
        let mut completed_groups = 0u64;
        for message in receiver {
            let (token, is_calibration, result) = match message {
                SharedScanMessage::Work {
                    work,
                    is_calibration,
                } => {
                    pair_work = pair_work.saturating_add(work).min(total_pair_work);
                    if is_calibration {
                        calibration_pair_work = calibration_pair_work.saturating_add(work);
                    } else {
                        holdout_pair_work = holdout_pair_work.saturating_add(work);
                    }
                    progress(ProgressEvent::determinate(
                        ProgressPhase::SharedTokenExactIsland,
                        pair_work,
                        total_pair_work,
                        WorkUnit::Pairs,
                        ProgressCounters {
                            groups: completed_groups,
                            matched: calibration_exact_matches
                                .saturating_add(holdout_exact_matches),
                            ..ProgressCounters::default()
                        },
                    ));
                    continue;
                }
                SharedScanMessage::Done {
                    token,
                    is_calibration,
                    result,
                } => (token, is_calibration, result),
            };
            let result = result?;
            if is_calibration {
                calibration_exact_matches =
                    calibration_exact_matches.saturating_add(result.exact_matches);
                calibration_clusters.push(ExactEvidenceCluster {
                    id: token,
                    exact_matches: result.exact_matches,
                });
            } else {
                holdout_exact_matches = holdout_exact_matches.saturating_add(result.exact_matches);
                holdout_clusters.push(ExactEvidenceCluster {
                    id: token,
                    exact_matches: result.exact_matches,
                });
            }
            if result.scratch_fallback {
                scratch_fallback_tokens.push(token);
            }
            completed_groups = completed_groups.saturating_add(1);
            progress(ProgressEvent::determinate(
                ProgressPhase::SharedTokenExactIsland,
                pair_work,
                total_pair_work,
                WorkUnit::Pairs,
                ProgressCounters {
                    groups: completed_groups,
                    matched: calibration_exact_matches.saturating_add(holdout_exact_matches),
                    ..ProgressCounters::default()
                },
            ));
        }
        worker
            .join()
            .map_err(|_| ExactIslandError::Parallel("worker panicked".into()))?;
        Ok((
            pair_work,
            calibration_pair_work,
            holdout_pair_work,
            calibration_exact_matches,
            holdout_exact_matches,
            calibration_clusters,
            holdout_clusters,
            scratch_fallback_tokens,
        ))
    })?;
    let finalize_total = 1 + u64::from(output_dir.is_some());
    progress(ProgressEvent::determinate(
        ProgressPhase::SharedTokenExactFinalize,
        0,
        finalize_total,
        WorkUnit::Items,
        ProgressCounters::default(),
    ));
    calibration.shrink_to_fit();
    holdout.shrink_to_fit();
    let calibration_misses =
        calibration_miss_spool.finish(SHARED_CALIBRATION_MISS_FILE, output_dir.is_some())?;
    let holdout_misses =
        holdout_miss_spool.finish(SHARED_HOLDOUT_MISS_FILE, output_dir.is_some())?;
    calibration_clusters.sort_unstable_by_key(|cluster| cluster.id);
    holdout_clusters.sort_unstable_by_key(|cluster| cluster.id);
    scratch_fallback_tokens.sort_unstable();
    scratch_fallback_tokens.dedup();
    let evidence = SharedTokenExactEvidence {
        artifact_revision: EVIDENCE_ARTIFACT_REVISION,
        match_semantics_revision: crate::scoring::MATCH_SEMANTICS_REVISION,
        snapshot_fingerprint: crate::scheduler::snapshot_fingerprint(snapshot),
        sampling_policy_digest: shared_sampling_digest(&calibration, &holdout),
        calibration_tokens: calibration,
        holdout_tokens: holdout,
        pair_work,
        calibration_pair_work,
        holdout_pair_work,
        exact_matches: calibration_exact_matches.saturating_add(holdout_exact_matches),
        calibration_exact_matches,
        holdout_exact_matches,
        calibration_clusters,
        holdout_clusters,
        scratch_fallback_tokens,
        calibration_misses,
        holdout_misses,
    };
    if !shared_evidence_is_consistent(&evidence, snapshot) {
        return Err(ExactIslandError::InvalidEvidence(
            "generated shared-token evidence is internally inconsistent",
        ));
    }
    progress(ProgressEvent::determinate(
        ProgressPhase::SharedTokenExactFinalize,
        1,
        finalize_total,
        WorkUnit::Items,
        ProgressCounters::default(),
    ));
    if let Some(dir) = output_dir {
        let ready = SharedTokenExactEvidenceReadyRef {
            artifact_revision: evidence.artifact_revision,
            match_semantics_revision: evidence.match_semantics_revision,
            snapshot_fingerprint: &evidence.snapshot_fingerprint,
            sampling_policy_digest: &evidence.sampling_policy_digest,
            calibration_tokens: &evidence.calibration_tokens,
            holdout_tokens: &evidence.holdout_tokens,
            pair_work: evidence.pair_work,
            calibration_pair_work: evidence.calibration_pair_work,
            holdout_pair_work: evidence.holdout_pair_work,
            exact_matches: evidence.exact_matches,
            calibration_exact_matches: evidence.calibration_exact_matches,
            holdout_exact_matches: evidence.holdout_exact_matches,
            calibration_clusters: &evidence.calibration_clusters,
            holdout_clusters: &evidence.holdout_clusters,
            scratch_fallback_tokens: &evidence.scratch_fallback_tokens,
            calibration_miss_count: evidence.calibration_misses.len(),
            calibration_miss_file: SHARED_CALIBRATION_MISS_FILE,
            holdout_miss_count: evidence.holdout_misses.len(),
            holdout_miss_file: SHARED_HOLDOUT_MISS_FILE,
        };
        crate::format::commit_ready_serialized(dir, "ready", &ready)?;
    }
    progress(ProgressEvent::determinate(
        ProgressPhase::SharedTokenExactFinalize,
        finalize_total,
        finalize_total,
        WorkUnit::Items,
        ProgressCounters::default(),
    ));
    Ok(evidence)
}

fn normalized_tokens(tokens: &[u32], token_count: usize) -> Result<Vec<u32>, ExactIslandError> {
    let mut values = tokens.to_vec();
    values.sort_unstable();
    values.dedup();
    if let Some(&bad) = values.iter().find(|&&token| token as usize >= token_count) {
        return Err(ExactIslandError::SampleOutOfRange(bad));
    }
    Ok(values)
}

struct SharedTokenGroupScan {
    exact_matches: u64,
    scratch_fallback: bool,
}

impl SharedTokenGroupScan {
    fn merge(mut self, other: Self) -> Self {
        self.exact_matches = self.exact_matches.saturating_add(other.exact_matches);
        self.scratch_fallback |= other.scratch_fallback;
        self
    }
}

#[derive(Clone, Copy)]
struct SharedTokenPairTile {
    left_begin: usize,
    left_end: usize,
    right_begin: usize,
    right_end: usize,
}

fn shared_group_scratch_upper_bound(
    snapshot: &MetadataSnapshot,
    token: u32,
    _sample_work: u64,
) -> Result<u64, ExactIslandError> {
    let features = snapshot.features();
    let begin = features.token_member_offsets[token as usize] as usize;
    let end = features.token_member_offsets[token as usize + 1] as usize;
    let sources = &features.token_member_sources[begin..end];
    if sources.len() < 256 {
        return Ok(0);
    }
    let term_memberships = sources.iter().try_fold(0u64, |total, &source| {
        let payload = features.source_to_payload[source as usize] as usize;
        let template = features.payload_template_offsets[payload + 1]
            .saturating_sub(features.payload_template_offsets[payload]);
        let content = features.payload_content_offsets[payload + 1]
            .saturating_sub(features.payload_content_offsets[payload]);
        total.checked_add(template.saturating_add(content))
    });
    let Some(term_memberships) = term_memberships else {
        return Err(ExactIslandError::Budget {
            resource: "shared_token_group_scratch",
            requested: u64::MAX,
            limit: u64::MAX - 1,
        });
    };
    let members = sources.len() as u64;
    members
        .checked_mul(384)
        .and_then(|bytes| bytes.checked_add(term_memberships.saturating_mul(8)))
        .ok_or(ExactIslandError::Budget {
            resource: "shared_token_group_scratch",
            requested: u64::MAX,
            limit: u64::MAX - 1,
        })
}

#[cfg(test)]
fn shared_token_pair_tiles(member_count: usize, tile_members: usize) -> Vec<SharedTokenPairTile> {
    let tile_members = tile_members.max(1);
    let side = member_count.div_ceil(tile_members);
    let mut tiles = Vec::with_capacity(side.saturating_mul(side.saturating_add(1)) / 2);
    for left_tile in 0..side {
        for right_tile in left_tile..side {
            let left_begin = left_tile * tile_members;
            let right_begin = right_tile * tile_members;
            tiles.push(SharedTokenPairTile {
                left_begin,
                left_end: left_begin.saturating_add(tile_members).min(member_count),
                right_begin,
                right_end: right_begin.saturating_add(tile_members).min(member_count),
            });
        }
    }
    tiles
}

fn scan_shared_token_group(
    snapshot: &MetadataSnapshot,
    token: u32,
    sample_work: u64,
    budget: ExactEvidenceBudget,
    routing_scratch_budget: u64,
    miss_spool: &MissSpool<SharedTokenExactMiss>,
    report_work: impl Fn(u64) + Sync,
) -> Result<SharedTokenGroupScan, ExactIslandError> {
    const MISS_BATCH: usize = 4_096;
    const MAX_PAYLOAD_CACHE_ENTRIES: usize = 65_536;
    const PROGRESS_CHUNK: u64 = 65_536;
    let features = snapshot.features();
    let begin = features.token_member_offsets[token as usize] as usize;
    let end = features.token_member_offsets[token as usize + 1] as usize;
    let contracts = &features.token_member_contracts[begin..end];
    let sources = &features.token_member_sources[begin..end];
    let members = contracts.len() as u64;
    let group_work = members
        .checked_mul(members.saturating_sub(1))
        .and_then(|value| value.checked_div(2))
        .ok_or(ExactIslandError::Budget {
            resource: "shared_token_pair_work",
            requested: u64::MAX,
            limit: budget.max_pair_work,
        })?;
    checked("adaptive_shared_token_pair_work", sample_work, group_work)?;
    if sample_work == 0 {
        return Ok(SharedTokenGroupScan {
            exact_matches: 0,
            scratch_fallback: false,
        });
    }

    let routing_scratch =
        shared_group_scratch_upper_bound(snapshot, token, sample_work).unwrap_or(u64::MAX);
    let scratch_fallback = contracts.len() >= 256 && routing_scratch > routing_scratch_budget;
    let member_payloads = (!scratch_fallback).then(|| {
        sources
            .iter()
            .map(|&source| features.source_to_payload[source as usize])
            .collect::<Vec<_>>()
    });
    let cache_payload_scores = member_payloads.as_ref().is_none_or(|payloads| {
        let mut unique_payloads = payloads.clone();
        unique_payloads.sort_unstable();
        unique_payloads.dedup();
        unique_payloads.len() < payloads.len()
    });
    let routing = if contracts.len() < 256 || scratch_fallback {
        None
    } else {
        let member_payloads = member_payloads
            .as_ref()
            .expect("routed shared group materializes member payload ids");
        let sketches = build_base_equivalent_atom_sketches_from_feature_view_parallel(
            features,
            member_payloads,
        );
        let plan = LocalRoutingPlan::build_parallel(&sketches);
        Some((sketches, plan))
    };
    let payload_at = |index: usize| {
        member_payloads.as_ref().map_or_else(
            || features.source_to_payload[sources[index] as usize],
            |payloads| payloads[index],
        )
    };
    let is_routing_miss = |left: usize, right: usize| {
        scratch_fallback
            || routing.as_ref().is_some_and(|(sketches, plan)| {
                !plan.routes_pair(sketches, left as u32, right as u32)
            })
    };
    let score_cached =
        |payload_scores: &mut HashMap<u64, bool>, left_payload: u32, right_payload: u32| {
            if !cache_payload_scores {
                return score_pair(features, left_payload, right_payload)
                    == PairScoreDecision::ExactMatch;
            }
            let key = payload_pair_key(left_payload, right_payload);
            if let Some(&decision) = payload_scores.get(&key) {
                return decision;
            }
            let decision =
                score_pair(features, left_payload, right_payload) == PairScoreDecision::ExactMatch;
            if payload_scores.len() < MAX_PAYLOAD_CACHE_ENTRIES {
                payload_scores.insert(key, decision);
            }
            decision
        };
    let push_miss = |misses: &mut Vec<SharedTokenExactMiss>,
                     left: usize,
                     right: usize|
     -> Result<(), ExactIslandError> {
        misses.push(SharedTokenExactMiss {
            token_id: token,
            left_contract: contracts[left].min(contracts[right]),
            right_contract: contracts[left].max(contracts[right]),
        });
        if misses.len() == MISS_BATCH {
            miss_spool.push_chunk(std::mem::replace(misses, Vec::with_capacity(MISS_BATCH)))?;
        }
        Ok(())
    };
    if sample_work < group_work {
        let start = adaptive_pair_seed(token, 0x9E37_79B9) % group_work;
        let step = coprime_pair_step(group_work, adaptive_pair_seed(token, 0x85EB_CA6B));
        let chunks = sample_work.div_ceil(PROGRESS_CHUNK);
        return (0..chunks)
            .into_par_iter()
            .map(|chunk| -> Result<SharedTokenGroupScan, ExactIslandError> {
                let first = chunk.saturating_mul(PROGRESS_CHUNK);
                let end = first.saturating_add(PROGRESS_CHUNK).min(sample_work);
                let mut result = SharedTokenGroupScan {
                    exact_matches: 0,
                    scratch_fallback,
                };
                let mut payload_scores = HashMap::<u64, bool>::new();
                let mut misses = Vec::with_capacity(MISS_BATCH);
                for sample in first..end {
                    let ordinal = (u128::from(start)
                        + u128::from(sample).saturating_mul(u128::from(step)))
                        % u128::from(group_work);
                    let (left, right) = triangular_pair(contracts.len(), ordinal as u64);
                    if score_cached(&mut payload_scores, payload_at(left), payload_at(right)) {
                        result.exact_matches = result.exact_matches.saturating_add(1);
                        if is_routing_miss(left, right) {
                            push_miss(&mut misses, left, right)?;
                        }
                    }
                }
                miss_spool.push_chunk(misses)?;
                report_work(end.saturating_sub(first));
                Ok(result)
            })
            .try_reduce(
                || SharedTokenGroupScan {
                    exact_matches: 0,
                    scratch_fallback,
                },
                |left, right| Ok(left.merge(right)),
            );
    }
    let side = contracts.len().div_ceil(SHARED_PAIR_TILE_MEMBERS);
    (0..side)
        .into_par_iter()
        .flat_map_iter(|left_tile| {
            (left_tile..side).map(move |right_tile| {
                let left_begin = left_tile * SHARED_PAIR_TILE_MEMBERS;
                let right_begin = right_tile * SHARED_PAIR_TILE_MEMBERS;
                SharedTokenPairTile {
                    left_begin,
                    left_end: left_begin
                        .saturating_add(SHARED_PAIR_TILE_MEMBERS)
                        .min(contracts.len()),
                    right_begin,
                    right_end: right_begin
                        .saturating_add(SHARED_PAIR_TILE_MEMBERS)
                        .min(contracts.len()),
                }
            })
        })
        .map(|tile| -> Result<SharedTokenGroupScan, ExactIslandError> {
            let mut result = SharedTokenGroupScan {
                exact_matches: 0,
                scratch_fallback,
            };
            let mut pending_work = 0u64;
            let mut payload_scores = HashMap::<u64, bool>::new();
            let mut misses = Vec::with_capacity(MISS_BATCH);
            for left in tile.left_begin..tile.left_end {
                let right_begin = tile.right_begin.max(left.saturating_add(1));
                for right in right_begin..tile.right_end {
                    pending_work = pending_work.saturating_add(1);
                    if pending_work >= PROGRESS_CHUNK {
                        report_work(pending_work);
                        pending_work = 0;
                    }
                    if score_cached(&mut payload_scores, payload_at(left), payload_at(right)) {
                        result.exact_matches = result.exact_matches.saturating_add(1);
                        if is_routing_miss(left, right) {
                            push_miss(&mut misses, left, right)?;
                        }
                    }
                }
            }
            if pending_work != 0 {
                report_work(pending_work);
            }
            miss_spool.push_chunk(misses)?;
            Ok(result)
        })
        .try_reduce(
            || SharedTokenGroupScan {
                exact_matches: 0,
                scratch_fallback,
            },
            |left, right| Ok(left.merge(right)),
        )
}

fn adaptive_pair_seed(token: u32, salt: u64) -> u64 {
    u64::from(token)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(salt)
}

fn greatest_common_divisor(mut left: u64, mut right: u64) -> u64 {
    while right != 0 {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    left
}

fn coprime_pair_step(modulus: u64, seed: u64) -> u64 {
    if modulus <= 1 {
        return 1;
    }
    let mut step = (seed % modulus).max(1);
    while greatest_common_divisor(step, modulus) != 1 {
        step = step.wrapping_add(1);
        if step >= modulus {
            step = 1;
        }
    }
    step
}

fn triangular_pair(members: usize, ordinal: u64) -> (usize, usize) {
    let members = members as u64;
    let prefix = |left: u64| {
        left.saturating_mul(
            members
                .saturating_mul(2)
                .saturating_sub(left)
                .saturating_sub(1),
        ) / 2
    };
    let mut low = 0u64;
    let mut high = members.saturating_sub(1);
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        if prefix(middle) <= ordinal {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    let left = low;
    let right = left + 1 + ordinal.saturating_sub(prefix(left));
    (left as usize, right as usize)
}

pub fn run_pair_exact_island(
    snapshot: &MetadataSnapshot,
    sampled_lefts: &[u32],
    budget: ExactEvidenceBudget,
    output_dir: Option<&Path>,
) -> Result<PairExactEvidence, ExactIslandError> {
    run_pair_exact_island_with_progress(snapshot, sampled_lefts, budget, output_dir, |_| {})
}

pub fn run_pair_exact_island_with_progress(
    snapshot: &MetadataSnapshot,
    sampled_lefts: &[u32],
    budget: ExactEvidenceBudget,
    output_dir: Option<&Path>,
    mut progress: impl FnMut(ProgressEvent),
) -> Result<PairExactEvidence, ExactIslandError> {
    let started = Instant::now();
    crate::identity::checked_u32_identity("exact-island atoms", snapshot.atom_count() as u64)?;
    let mut lefts = sampled_lefts.to_vec();
    lefts.sort_unstable();
    lefts.dedup();
    checked("sample_lefts", lefts.len() as u64, budget.max_lefts)?;
    if let Some(&bad) = lefts.iter().find(|&&a| a as usize >= snapshot.atom_count()) {
        return Err(ExactIslandError::SampleOutOfRange(bad));
    }
    let frontier_us = micros(started);
    let sampled_count = lefts.len() as u64;
    let pair_work = sampled_count
        .checked_mul(snapshot.atom_count().saturating_sub(1) as u64)
        .and_then(|work| {
            sampled_count
                .checked_mul(sampled_count.saturating_sub(1))
                .map(|duplicates| work.saturating_sub(duplicates / 2))
        })
        .ok_or(ExactIslandError::Budget {
            resource: "pair_work",
            requested: u64::MAX,
            limit: budget.max_pair_work,
        })?;
    checked("pair_work", pair_work, budget.max_pair_work)?;
    progress(ProgressEvent::determinate(
        ProgressPhase::PairExactIsland,
        0,
        pair_work,
        WorkUnit::Pairs,
        ProgressCounters::default(),
    ));

    let scan = Instant::now();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(budget.max_lanes.max(1))
        .thread_name(|index| format!("metadata-exact-{index}"))
        .build()
        .map_err(|error| ExactIslandError::Parallel(error.to_string()))?;
    let lanes = budget.max_lanes.max(1);
    enum ScanMessage {
        Work(u64),
        Done {
            left: u32,
            result: Result<u64, ExactIslandError>,
        },
    }
    let (sender, receiver) = std::sync::mpsc::sync_channel(lanes.saturating_mul(4).max(1));
    let pair_miss_workspace = miss_workspace(output_dir, "pair")?;
    let pair_miss_spool = Arc::new(MissSpool::<ExactMiss>::new(
        Arc::new(ResidentMissBudget::for_record::<ExactMiss>(
            budget.max_artifact_bytes,
        )),
        pair_miss_workspace,
        "pair-misses",
    ));
    let atom_payloads = (0..snapshot.atom_count() as u32)
        .map(|atom| atom_payload(snapshot, atom))
        .collect::<Vec<_>>();
    let mut matches = 0u64;
    let mut clusters = Vec::new();
    let mut completed = 0u64;
    let work_lefts = &lefts;
    std::thread::scope(|scope| -> Result<(), ExactIslandError> {
        let worker_sender = sender.clone();
        let worker_miss_spool = pair_miss_spool.clone();
        let producer = scope.spawn(move || {
            pool.install(|| {
                work_lefts.par_iter().for_each(|&left| {
                    let result = scan_pair_left(
                        snapshot,
                        &atom_payloads,
                        work_lefts,
                        left,
                        &worker_miss_spool,
                        |work| {
                            let _ = worker_sender.send(ScanMessage::Work(work));
                        },
                    );
                    let _ = worker_sender.send(ScanMessage::Done { left, result });
                });
            });
        });
        drop(sender);
        let mut first_error = None;
        for message in receiver {
            let (left, result) = match message {
                ScanMessage::Work(work) => {
                    completed = completed.saturating_add(work).min(pair_work);
                    progress(ProgressEvent::determinate(
                        ProgressPhase::PairExactIsland,
                        completed,
                        pair_work,
                        WorkUnit::Pairs,
                        ProgressCounters {
                            matched: matches,
                            ..ProgressCounters::default()
                        },
                    ));
                    continue;
                }
                ScanMessage::Done { left, result } => (left, result),
            };
            match result {
                Ok(left_matches) if first_error.is_none() => {
                    matches = matches.saturating_add(left_matches);
                    clusters.push(ExactEvidenceCluster {
                        id: left,
                        exact_matches: left_matches,
                    });
                }
                Err(error) if first_error.is_none() => first_error = Some(error),
                _ => {}
            }
            progress(ProgressEvent::determinate(
                ProgressPhase::PairExactIsland,
                completed,
                pair_work,
                WorkUnit::Pairs,
                ProgressCounters {
                    matched: matches,
                    ..ProgressCounters::default()
                },
            ));
        }
        producer
            .join()
            .map_err(|_| ExactIslandError::Parallel("worker panicked".into()))?;
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(())
    })?;
    let finalize_total = 1 + u64::from(output_dir.is_some());
    progress(ProgressEvent::determinate(
        ProgressPhase::PairExactFinalize,
        0,
        finalize_total,
        WorkUnit::Items,
        ProgressCounters::default(),
    ));
    let conservative_misses = pair_miss_spool.finish(PAIR_MISS_FILE, output_dir.is_some())?;
    clusters.sort_unstable_by_key(|cluster| cluster.id);
    progress(ProgressEvent::determinate(
        ProgressPhase::PairExactFinalize,
        1,
        finalize_total,
        WorkUnit::Items,
        ProgressCounters::default(),
    ));
    let scan_us = micros(scan);
    let finalize = Instant::now();
    let mut evidence = PairExactEvidence {
        artifact_revision: EVIDENCE_ARTIFACT_REVISION,
        match_semantics_revision: crate::scoring::MATCH_SEMANTICS_REVISION,
        snapshot_fingerprint: crate::scheduler::snapshot_fingerprint(snapshot),
        sampling_policy_digest: pair_sampling_digest(&lefts),
        universe_atoms: snapshot.atom_count() as u64,
        sampled_lefts: lefts,
        pair_work,
        exact_matches: matches,
        clusters,
        conservative_misses,
        frontier_build_micros: frontier_us,
        full_universe_scan_micros: scan_us,
        posting_finalize_micros: 0,
        oracle_score_micros: scan_us,
        full_scan_equivalents_micros: scan_us,
    };
    if !pair_evidence_is_consistent(&evidence) {
        return Err(ExactIslandError::InvalidEvidence(
            "generated pair evidence is internally inconsistent",
        ));
    }
    evidence.posting_finalize_micros = micros(finalize);
    if let Some(dir) = output_dir {
        let ready = PairExactEvidenceReadyRef {
            artifact_revision: evidence.artifact_revision,
            match_semantics_revision: evidence.match_semantics_revision,
            snapshot_fingerprint: &evidence.snapshot_fingerprint,
            sampling_policy_digest: &evidence.sampling_policy_digest,
            universe_atoms: evidence.universe_atoms,
            sampled_lefts: &evidence.sampled_lefts,
            pair_work: evidence.pair_work,
            exact_matches: evidence.exact_matches,
            clusters: &evidence.clusters,
            conservative_miss_count: evidence.conservative_misses.len(),
            conservative_miss_file: PAIR_MISS_FILE,
            frontier_build_micros: evidence.frontier_build_micros,
            full_universe_scan_micros: evidence.full_universe_scan_micros,
            posting_finalize_micros: evidence.posting_finalize_micros,
            oracle_score_micros: evidence.oracle_score_micros,
            full_scan_equivalents_micros: evidence.full_scan_equivalents_micros,
        };
        crate::format::commit_ready_serialized(dir, "ready", &ready)?;
    }
    progress(ProgressEvent::determinate(
        ProgressPhase::PairExactFinalize,
        finalize_total,
        finalize_total,
        WorkUnit::Items,
        ProgressCounters::default(),
    ));
    Ok(evidence)
}

fn pair_sampling_digest(sampled_lefts: &[u32]) -> String {
    sampling_digest("pair", sampled_lefts, &[])
}

fn shared_sampling_digest(calibration_tokens: &[u32], holdout_tokens: &[u32]) -> String {
    sampling_digest("shared-token", calibration_tokens, holdout_tokens)
}

fn sampling_digest(kind: &str, first: &[u32], second: &[u32]) -> String {
    let mut hash = Sha256::new();
    hash.update(kind.as_bytes());
    for values in [first, second] {
        hash.update((values.len() as u64).to_le_bytes());
        for &value in values {
            hash.update(value.to_le_bytes());
        }
    }
    hash.finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn scan_pair_left(
    snapshot: &MetadataSnapshot,
    atom_payloads: &[u32],
    sampled_lefts: &[u32],
    left: u32,
    miss_spool: &MissSpool<ExactMiss>,
    mut report_work: impl FnMut(u64),
) -> Result<u64, ExactIslandError> {
    const MISS_BATCH: usize = 4_096;
    const PROGRESS_CHUNK: u64 = 65_536;
    let mut matches = 0u64;
    let mut misses = Vec::with_capacity(MISS_BATCH);
    let mut pending_work = 0u64;
    let left_payload = atom_payloads[left as usize];
    let left_contracts = atom_contracts(snapshot, left);
    let mut sampled_cursor = 0usize;
    for right in 0..snapshot.atom_count() as u32 {
        while sampled_cursor < sampled_lefts.len() && sampled_lefts[sampled_cursor] < right {
            sampled_cursor += 1;
        }
        let right_is_sampled = sampled_lefts.get(sampled_cursor).copied() == Some(right);
        if left == right || (right < left && right_is_sampled) {
            continue;
        }
        pending_work += 1;
        if pending_work == PROGRESS_CHUNK {
            report_work(pending_work);
            pending_work = 0;
        }
        let right_payload = atom_payloads[right as usize];
        if score_pair(snapshot.features(), left_payload, right_payload)
            == PairScoreDecision::ExactMatch
            && has_token_disjoint_contract_pair(snapshot, left_contracts, right)
        {
            matches = matches.saturating_add(1);
            if candidate_owner(snapshot.blocking(), left, right).is_none() {
                misses.push(ExactMiss {
                    left_atom: left.min(right),
                    right_atom: left.max(right),
                });
                if misses.len() == MISS_BATCH {
                    miss_spool.push_chunk(std::mem::replace(
                        &mut misses,
                        Vec::with_capacity(MISS_BATCH),
                    ))?;
                }
            }
        }
    }
    if pending_work != 0 {
        report_work(pending_work);
    }
    miss_spool.push_chunk(misses)?;
    Ok(matches)
}

fn checked(resource: &'static str, requested: u64, limit: u64) -> Result<(), ExactIslandError> {
    if requested > limit {
        Err(ExactIslandError::Budget {
            resource,
            requested,
            limit,
        })
    } else {
        Ok(())
    }
}
fn micros(t: Instant) -> u64 {
    u64::try_from(t.elapsed().as_micros()).unwrap_or(u64::MAX)
}
fn atom_payload(s: &MetadataSnapshot, a: u32) -> u32 {
    let f = s.features();
    let c = f.fallback_atom_contracts[f.fallback_atom_offsets[a as usize] as usize];
    f.contract_payload[c as usize]
}
fn atom_contracts(s: &MetadataSnapshot, atom: u32) -> &[u32] {
    let f = s.features();
    &f.fallback_atom_contracts[f.fallback_atom_offsets[atom as usize] as usize
        ..f.fallback_atom_offsets[atom as usize + 1] as usize]
}

fn payload_pair_key(left: u32, right: u32) -> u64 {
    let (left, right) = (left.min(right), left.max(right));
    (u64::from(left) << 32) | u64::from(right)
}

fn has_token_disjoint_contract_pair(s: &MetadataSnapshot, left: &[u32], right_atom: u32) -> bool {
    let f = s.features();
    let right = atom_contracts(s, right_atom);
    left.iter().any(|&left_contract| {
        right.iter().any(|&right_contract| {
            !sorted_intersects(
                f.contract_tokens(left_contract),
                f.contract_tokens(right_contract),
            )
        })
    })
}

fn sorted_intersects(x: &[u32], y: &[u32]) -> bool {
    let (mut i, mut j) = (0, 0);
    while i < x.len() && j < y.len() {
        match x[i].cmp(&y[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => return true,
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::{
        coprime_pair_step, miss_workspace, pair_evidence_is_consistent,
        proportional_partition_samples, shared_token_pair_tiles, triangular_pair,
        ExactEvidenceCluster, ExactMiss, MissSpool, PairExactEvidence, ResidentMissBudget,
        SharedTokenExactEvidence, EVIDENCE_ARTIFACT_REVISION,
    };
    use crate::blocking::{AtomSketch, LocalRoutingPlan};

    #[test]
    fn adaptive_partition_gives_every_selected_group_one_observation_when_affordable() {
        let samples = proportional_partition_samples(&[0, 2, 1_000_002], &[0, 1], 2).unwrap();

        assert_eq!(samples[&0], 1);
        assert_eq!(samples[&1], 1);
    }

    #[test]
    fn local_route_queries_do_not_materialize_the_pair_universe() {
        let sketches = (0..300u64)
            .map(|value| AtomSketch {
                template_simhash: value.wrapping_mul(0x9e37_79b9_7f4a_7c15),
                content_simhash: value.wrapping_add(17).wrapping_mul(0xbf58_476d_1ce4_e5b9),
                template_anchors: vec![value as u32],
                content_anchors: vec![value as u32 + 10_000],
                has_template_terms: true,
                has_content_terms: true,
            })
            .collect::<Vec<_>>();

        let plan = LocalRoutingPlan::build(&sketches);
        let routed = (0..sketches.len() as u32)
            .flat_map(|left| (left + 1..sketches.len() as u32).map(move |right| (left, right)))
            .filter(|&(left, right)| plan.routes_pair(&sketches, left, right))
            .count();
        assert!(routed < 2_048);
    }

    #[test]
    fn shared_token_tiles_cover_every_unordered_pair_once() {
        let mut visits = Vec::new();
        for tile in shared_token_pair_tiles(19, 4) {
            for left in tile.left_begin..tile.left_end {
                let right_begin = tile.right_begin.max(left + 1);
                for right in right_begin..tile.right_end {
                    visits.push((left, right));
                }
            }
        }
        visits.sort_unstable();
        let expected = (0..19)
            .flat_map(|left| (left + 1..19).map(move |right| (left, right)))
            .collect::<Vec<_>>();
        assert_eq!(visits, expected);
    }

    #[test]
    fn miss_spool_spills_skewed_workers_without_losing_or_duplicating_rows() {
        let workspace = miss_workspace(None, "spool-test").unwrap();
        let budget = std::sync::Arc::new(ResidentMissBudget::for_record::<ExactMiss>(
            2 * std::mem::size_of::<ExactMiss>() as u64,
        ));
        let spool = MissSpool::new(budget, workspace, "pair");

        spool
            .push_chunk(vec![
                ExactMiss {
                    left_atom: 2,
                    right_atom: 4,
                },
                ExactMiss {
                    left_atom: 0,
                    right_atom: 1,
                },
            ])
            .unwrap();
        spool
            .push_chunk(vec![
                ExactMiss {
                    left_atom: 1,
                    right_atom: 3,
                },
                ExactMiss {
                    left_atom: 0,
                    right_atom: 1,
                },
            ])
            .unwrap();
        let rows = spool.finish("pair.u32", false).unwrap();

        assert!(rows.is_mapped());
        assert_eq!(
            &*rows,
            &[
                ExactMiss {
                    left_atom: 0,
                    right_atom: 1,
                },
                ExactMiss {
                    left_atom: 1,
                    right_atom: 3,
                },
                ExactMiss {
                    left_atom: 2,
                    right_atom: 4,
                },
            ]
        );
    }

    #[test]
    fn adaptive_pair_permutation_is_deterministic_and_without_replacement() {
        let members = 10usize;
        let pair_work = 45u64;
        let step = coprime_pair_step(pair_work, 17);
        let start = 11u64;
        let mut pairs = (0..pair_work)
            .map(|sample| triangular_pair(members, (start + sample * step) % pair_work))
            .collect::<Vec<_>>();
        pairs.sort_unstable();
        pairs.dedup();
        assert_eq!(pairs.len() as u64, pair_work);
        assert!(pairs
            .iter()
            .all(|&(left, right)| left < right && right < members));
    }

    #[test]
    fn adaptive_partition_sampling_is_proportional_to_pair_population() {
        // Token pair populations are 45 and 4_950. A 500-pair budget preserves
        // their inclusion rate up to one largest-remainder rounding unit.
        let samples = proportional_partition_samples(&[0, 10, 110], &[0, 1], 500).unwrap();
        assert_eq!(samples.values().sum::<u64>(), 500);
        let left_cross = samples[&0] * 4_950;
        let right_cross = samples[&1] * 45;
        assert!(left_cross.abs_diff(right_cross) <= 4_950);
    }

    #[test]
    fn shared_evidence_partition_metrics_are_mandatory() {
        let evidence = SharedTokenExactEvidence {
            artifact_revision: EVIDENCE_ARTIFACT_REVISION,
            match_semantics_revision: crate::scoring::MATCH_SEMANTICS_REVISION,
            snapshot_fingerprint: "snapshot".into(),
            sampling_policy_digest: "sampling".into(),
            calibration_tokens: vec![],
            holdout_tokens: vec![],
            pair_work: 0,
            calibration_pair_work: 0,
            holdout_pair_work: 0,
            exact_matches: 0,
            calibration_exact_matches: 0,
            holdout_exact_matches: 0,
            calibration_clusters: vec![],
            holdout_clusters: vec![],
            scratch_fallback_tokens: vec![],
            calibration_misses: vec![].into(),
            holdout_misses: vec![].into(),
        };
        let mut json = serde_json::to_value(evidence).unwrap();
        json.as_object_mut().unwrap().remove("holdout_pair_work");

        assert!(serde_json::from_value::<SharedTokenExactEvidence>(json).is_err());
    }

    #[test]
    fn pair_evidence_rejects_exact_matches_above_scanned_pair_work() {
        let evidence = PairExactEvidence {
            artifact_revision: EVIDENCE_ARTIFACT_REVISION,
            match_semantics_revision: crate::scoring::MATCH_SEMANTICS_REVISION,
            snapshot_fingerprint: "snapshot".into(),
            sampling_policy_digest: "sampling".into(),
            universe_atoms: 2,
            sampled_lefts: vec![0],
            pair_work: 1,
            exact_matches: 2,
            clusters: vec![ExactEvidenceCluster {
                id: 0,
                exact_matches: 2,
            }],
            conservative_misses: vec![].into(),
            frontier_build_micros: 0,
            full_universe_scan_micros: 0,
            posting_finalize_micros: 0,
            oracle_score_micros: 0,
            full_scan_equivalents_micros: 0,
        };

        assert!(!pair_evidence_is_consistent(&evidence));
    }
}
