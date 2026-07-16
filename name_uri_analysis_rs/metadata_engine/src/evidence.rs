use crate::exact_islands::{
    ExactEvidenceCluster, ExactMiss, SharedTokenExactMiss, SharedTokenWorkStratum,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use thiserror::Error;

pub const EVIDENCE_GATE_REVISION: u32 = 5;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct EvidenceGatePolicy {
    pub max_miss_rate: f64,
    pub confidence_z: f64,
    pub min_exact_matches: u64,
    pub max_skipped_pair_work_rate: f64,
}

impl EvidenceGatePolicy {
    pub const fn production() -> Self {
        Self {
            max_miss_rate: 0.01,
            confidence_z: 1.96,
            min_exact_matches: 30,
            max_skipped_pair_work_rate: 0.01,
        }
    }

    pub const fn permissive() -> Self {
        Self {
            max_miss_rate: 1.0,
            confidence_z: 1.96,
            min_exact_matches: 0,
            max_skipped_pair_work_rate: 1.0,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RescuePlan {
    pub pair_atoms: Vec<u32>,
    pub shared_contracts: Vec<u32>,
    pub shared_edges: Vec<(u32, u32)>,
}

impl RescuePlan {
    pub fn from_calibration(
        pair_misses: &[ExactMiss],
        shared_misses: &[SharedTokenExactMiss],
    ) -> Self {
        let mut pair_atoms = pair_misses
            .iter()
            .flat_map(|miss| [miss.left_atom, miss.right_atom])
            .collect::<Vec<_>>();
        pair_atoms.sort_unstable();
        pair_atoms.dedup();

        let mut shared_degrees = BTreeMap::<u32, u64>::new();
        for miss in shared_misses {
            let left = shared_degrees.entry(miss.left_contract).or_default();
            *left = left.saturating_add(1);
            let right = shared_degrees.entry(miss.right_contract).or_default();
            *right = right.saturating_add(1);
        }
        let mut shared_contracts = shared_misses
            .iter()
            .map(|miss| {
                let left_degree = shared_degrees[&miss.left_contract];
                let right_degree = shared_degrees[&miss.right_contract];
                if left_degree > right_degree {
                    miss.left_contract
                } else if right_degree > left_degree {
                    miss.right_contract
                } else {
                    miss.left_contract.min(miss.right_contract)
                }
            })
            .collect::<Vec<_>>();
        shared_contracts.sort_unstable();
        shared_contracts.dedup();
        let shared_edges = normalized_shared_edges(shared_misses);

        Self {
            pair_atoms,
            shared_contracts,
            shared_edges,
        }
    }

    pub fn from_holdout(pair_misses: &[ExactMiss], shared_misses: &[SharedTokenExactMiss]) -> Self {
        let mut plan = Self::from_calibration(pair_misses, &[]);
        plan.shared_edges = normalized_shared_edges(shared_misses);
        plan
    }

    pub fn merge(mut self, mut other: Self) -> Self {
        self.pair_atoms.append(&mut other.pair_atoms);
        self.pair_atoms.sort_unstable();
        self.pair_atoms.dedup();
        self.shared_contracts.append(&mut other.shared_contracts);
        self.shared_contracts.sort_unstable();
        self.shared_contracts.dedup();
        self.shared_edges.append(&mut other.shared_edges);
        for edge in &mut self.shared_edges {
            *edge = normalized_edge(edge.0, edge.1);
        }
        self.shared_edges.sort_unstable();
        self.shared_edges.dedup();
        self
    }

    fn covers_pair(&self, miss: &ExactMiss) -> bool {
        // Production rescue scans every selected atom against the full atom
        // universe. Therefore either endpoint is sufficient to guarantee that
        // this exact unordered pair is scored; requiring both would count
        // deterministically rescued pairs as residual misses.
        self.pair_atoms.binary_search(&miss.left_atom).is_ok()
            || self.pair_atoms.binary_search(&miss.right_atom).is_ok()
    }

    fn covers_shared(&self, miss: &SharedTokenExactMiss) -> bool {
        let edge = normalized_edge(miss.left_contract, miss.right_contract);
        self.shared_edges.binary_search(&edge).is_ok()
            || [miss.left_contract, miss.right_contract]
                .into_iter()
                .any(|contract_id| self.shared_contracts.binary_search(&contract_id).is_ok())
    }
}

fn normalized_shared_edges(misses: &[SharedTokenExactMiss]) -> Vec<(u32, u32)> {
    let mut edges = misses
        .iter()
        .map(|miss| normalized_edge(miss.left_contract, miss.right_contract))
        .collect::<Vec<_>>();
    edges.sort_unstable();
    edges.dedup();
    edges
}

fn normalized_edge(left: u32, right: u32) -> (u32, u32) {
    (left.min(right), left.max(right))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EvidenceGateReport {
    pub policy: EvidenceGatePolicy,
    pub passed: bool,
    pub sample_sufficient: bool,
    pub observed_misses: u64,
    pub evaluated_pair_work: u64,
    pub evidence_exhaustive: bool,
    pub exact_matches: u64,
    pub wilson_upper_bound: f64,
    pub pair_wilson_upper_bound: f64,
    pub shared_wilson_upper_bound: f64,
    pub pair_sample_sufficient: bool,
    pub shared_sample_sufficient: bool,
    pub skipped_shared_groups: Vec<u32>,
    pub skipped_pair_work_rate: f64,
    pub max_stratum_skipped_pair_work_rate: f64,
    pub pair_statistical_trials: u64,
    pub pair_statistical_misses: u64,
    pub shared_statistical_trials: u64,
    pub shared_statistical_misses: u64,
}

impl EvidenceGateReport {
    pub fn advisory_message(&self) -> Option<String> {
        (!self.passed).then(|| {
            format!(
                "ExactEvidence quality advisory did not pass: aggregate Wilson upper bound \
                 {:.6}, maximum residual miss rate {:.6}, sample_sufficient={} \
                 ({} residual misses / {} exact observations); diagnostics: pair {:.6} \
                 ({}/{}, sufficient={}), shared-token {:.6} ({}/{}, sufficient={}); \
                 skipped pair-work rate {:.6}, maximum stratum rate {:.6}, skip limit {:.6}",
                self.wilson_upper_bound,
                self.policy.max_miss_rate,
                self.sample_sufficient,
                self.observed_misses,
                self.exact_matches,
                self.pair_wilson_upper_bound,
                self.pair_statistical_misses,
                self.pair_statistical_trials,
                self.pair_sample_sufficient,
                self.shared_wilson_upper_bound,
                self.shared_statistical_misses,
                self.shared_statistical_trials,
                self.shared_sample_sufficient,
                self.skipped_pair_work_rate,
                self.max_stratum_skipped_pair_work_rate,
                self.policy.max_skipped_pair_work_rate,
            )
        })
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum EvidenceError {
    #[error("invalid max miss rate {0}; expected a finite value in [0, 1]")]
    InvalidMaxMissRate(f64),
    #[error("invalid confidence z-score {0}; expected a finite non-negative value")]
    InvalidConfidenceZ(f64),
    #[error("invalid skipped pair-work rate {0}; expected a finite value in [0, 1]")]
    InvalidSkippedPairWorkRate(f64),
    #[error("{kind} residual misses {misses} exceeds exact matches {exact_matches}")]
    MissesExceedExactMatches {
        kind: &'static str,
        misses: u64,
        exact_matches: u64,
    },
    #[error(
        "{kind} evidence cluster total {cluster_matches} does not equal exact matches {exact_matches}"
    )]
    ClusterTotalsMismatch {
        kind: &'static str,
        cluster_matches: u64,
        exact_matches: u64,
    },
    #[error("residual miss references an absent or empty evidence cluster")]
    MissingEvidenceCluster,
    #[error("duplicate {kind} evidence cluster id {id}")]
    DuplicateEvidenceCluster { kind: &'static str, id: u32 },
    #[error("evidence counter overflow while summing {0}")]
    CounterOverflow(&'static str),
    #[error("exact matches {exact_matches} exceeds evaluated pair work {evaluated_pair_work}")]
    ExactMatchesExceedEvaluatedWork {
        exact_matches: u64,
        evaluated_pair_work: u64,
    },
    #[error("pair misses are non-canonical, duplicated, or not strictly sorted")]
    InvalidPairMissOrder,
    #[error("shared-token misses are non-canonical, duplicated, or not strictly sorted")]
    InvalidSharedMissOrder,
    #[error("skipped shared-token pair work {skipped} exceeds considered work {considered}")]
    InvalidGlobalPairWork { skipped: u64, considered: u64 },
    #[error(
        "shared-token work strata total considered={strata_considered}, skipped={strata_skipped} does not match global considered={considered}, skipped={skipped}"
    )]
    StratumTotalsMismatch {
        strata_considered: u64,
        strata_skipped: u64,
        considered: u64,
        skipped: u64,
    },
    #[error(
        "shared-token work stratum {log2_pair_work} skipped work {skipped} exceeds considered work {considered}"
    )]
    InvalidStratumPairWork {
        log2_pair_work: u32,
        skipped: u64,
        considered: u64,
    },
}

pub struct HoldoutEvidence<'a> {
    pub evaluated_pair_work: u64,
    pub exhaustive: bool,
    pub pair_exact_matches: u64,
    pub pair_misses: &'a [ExactMiss],
    pub shared_exact_matches: u64,
    pub shared_misses: &'a [SharedTokenExactMiss],
    pub skipped_shared_groups: &'a [u32],
    pub skipped_shared_pair_work: u64,
    pub considered_shared_pair_work: u64,
    pub shared_work_strata: &'a [SharedTokenWorkStratum],
    pub pair_clusters: &'a [ExactEvidenceCluster],
    pub shared_clusters: &'a [ExactEvidenceCluster],
}

pub fn evaluate_holdout(
    evidence: HoldoutEvidence<'_>,
    rescue_plan: &RescuePlan,
    policy: EvidenceGatePolicy,
) -> Result<EvidenceGateReport, EvidenceError> {
    if !policy.max_miss_rate.is_finite() || !(0.0..=1.0).contains(&policy.max_miss_rate) {
        return Err(EvidenceError::InvalidMaxMissRate(policy.max_miss_rate));
    }
    if !policy.confidence_z.is_finite() || policy.confidence_z < 0.0 {
        return Err(EvidenceError::InvalidConfidenceZ(policy.confidence_z));
    }
    if !policy.max_skipped_pair_work_rate.is_finite()
        || !(0.0..=1.0).contains(&policy.max_skipped_pair_work_rate)
    {
        return Err(EvidenceError::InvalidSkippedPairWorkRate(
            policy.max_skipped_pair_work_rate,
        ));
    }

    let cluster_index = |kind, clusters: &[ExactEvidenceCluster]| {
        let mut index = BTreeMap::new();
        for cluster in clusters {
            if index.insert(cluster.id, cluster.exact_matches).is_some() {
                return Err(EvidenceError::DuplicateEvidenceCluster {
                    kind,
                    id: cluster.id,
                });
            }
        }
        Ok(index)
    };
    let pair_cluster_matches = cluster_index("pair", evidence.pair_clusters)?;
    let shared_cluster_matches = cluster_index("shared-token", evidence.shared_clusters)?;
    let checked_cluster_total = |kind, index: &BTreeMap<u32, u64>| {
        index.values().try_fold(0u64, |total, &matches| {
            total
                .checked_add(matches)
                .ok_or(EvidenceError::CounterOverflow(kind))
        })
    };
    let pair_cluster_total =
        checked_cluster_total("pair cluster exact matches", &pair_cluster_matches)?;
    let shared_cluster_total = checked_cluster_total(
        "shared-token cluster exact matches",
        &shared_cluster_matches,
    )?;
    if pair_cluster_total != evidence.pair_exact_matches {
        return Err(EvidenceError::ClusterTotalsMismatch {
            kind: "pair",
            cluster_matches: pair_cluster_total,
            exact_matches: evidence.pair_exact_matches,
        });
    }
    if shared_cluster_total != evidence.shared_exact_matches {
        return Err(EvidenceError::ClusterTotalsMismatch {
            kind: "shared-token",
            cluster_matches: shared_cluster_total,
            exact_matches: evidence.shared_exact_matches,
        });
    }
    let pair_raw_misses = evidence.pair_misses.len() as u64;
    let shared_raw_misses = evidence.shared_misses.len() as u64;
    if pair_raw_misses > evidence.pair_exact_matches {
        return Err(EvidenceError::MissesExceedExactMatches {
            kind: "pair",
            misses: pair_raw_misses,
            exact_matches: evidence.pair_exact_matches,
        });
    }
    if shared_raw_misses > evidence.shared_exact_matches {
        return Err(EvidenceError::MissesExceedExactMatches {
            kind: "shared-token",
            misses: shared_raw_misses,
            exact_matches: evidence.shared_exact_matches,
        });
    }
    if evidence
        .pair_misses
        .iter()
        .any(|miss| miss.left_atom >= miss.right_atom)
        || evidence.pair_misses.windows(2).any(|pair| {
            (pair[0].left_atom, pair[0].right_atom) >= (pair[1].left_atom, pair[1].right_atom)
        })
    {
        return Err(EvidenceError::InvalidPairMissOrder);
    }
    if evidence
        .shared_misses
        .iter()
        .any(|miss| miss.left_contract >= miss.right_contract)
        || evidence.shared_misses.windows(2).any(|pair| {
            (
                pair[0].token_id,
                pair[0].left_contract,
                pair[0].right_contract,
            ) >= (
                pair[1].token_id,
                pair[1].left_contract,
                pair[1].right_contract,
            )
        })
    {
        return Err(EvidenceError::InvalidSharedMissOrder);
    }
    let cluster_exists = |clusters: &BTreeMap<u32, u64>, id| {
        clusters
            .get(&id)
            .is_some_and(|&exact_matches| exact_matches != 0)
    };
    // ExactMiss stores canonical (min, max) endpoints, while its statistical
    // cluster is the sampled endpoint that caused the pair to be evaluated.
    // That endpoint can be the canonical right side when only the larger atom
    // was sampled. If both endpoints were sampled, scan ownership belongs to
    // the smaller endpoint, so checking left first preserves deterministic
    // assignment.
    let pair_miss_cluster = |miss: &ExactMiss| {
        if cluster_exists(&pair_cluster_matches, miss.left_atom) {
            Some(miss.left_atom)
        } else if cluster_exists(&pair_cluster_matches, miss.right_atom) {
            Some(miss.right_atom)
        } else {
            None
        }
    };
    for miss in evidence.pair_misses {
        if pair_miss_cluster(miss).is_none() {
            return Err(EvidenceError::MissingEvidenceCluster);
        }
    }
    for miss in evidence.shared_misses {
        if !cluster_exists(&shared_cluster_matches, miss.token_id) {
            return Err(EvidenceError::MissingEvidenceCluster);
        }
    }
    let pair_residual_miss_count = evidence
        .pair_misses
        .iter()
        .filter(|miss| !rescue_plan.covers_pair(miss))
        .count() as u64;
    let shared_residual_miss_count = evidence
        .shared_misses
        .iter()
        .filter(|miss| !rescue_plan.covers_shared(miss))
        .count() as u64;
    let observed_misses = pair_residual_miss_count
        .checked_add(shared_residual_miss_count)
        .ok_or(EvidenceError::CounterOverflow("residual misses"))?;
    let exact_matches = evidence
        .pair_exact_matches
        .checked_add(evidence.shared_exact_matches)
        .ok_or(EvidenceError::CounterOverflow("exact matches"))?;
    if exact_matches > evidence.evaluated_pair_work {
        return Err(EvidenceError::ExactMatchesExceedEvaluatedWork {
            exact_matches,
            evaluated_pair_work: evidence.evaluated_pair_work,
        });
    }
    // The configured policy is the aggregate residual-miss rate across all
    // exact observations, matching observed_misses/exact_matches in the report.
    // Keep per-domain bounds as diagnostics so concentration remains visible,
    // but do not silently reinterpret one aggregate policy as two stricter
    // domain policies. Sampling is deterministic, making these operational
    // audit margins rather than claims of independent random observations.
    let pair_wilson_upper_bound = wilson_upper_bound(
        pair_residual_miss_count,
        evidence.pair_exact_matches,
        policy.confidence_z,
    );
    let shared_wilson_upper_bound = wilson_upper_bound(
        shared_residual_miss_count,
        evidence.shared_exact_matches,
        policy.confidence_z,
    );
    let wilson_upper_bound =
        wilson_upper_bound(observed_misses, exact_matches, policy.confidence_z);
    let domain_sample_sufficient = |trials| trials == 0 || trials >= policy.min_exact_matches;
    let pair_sample_sufficient =
        evidence.exhaustive || domain_sample_sufficient(evidence.pair_exact_matches);
    let shared_sample_sufficient =
        evidence.exhaustive || domain_sample_sufficient(evidence.shared_exact_matches);
    let sample_sufficient = evidence.exhaustive || exact_matches >= policy.min_exact_matches;
    if evidence.skipped_shared_pair_work > evidence.considered_shared_pair_work {
        return Err(EvidenceError::InvalidGlobalPairWork {
            skipped: evidence.skipped_shared_pair_work,
            considered: evidence.considered_shared_pair_work,
        });
    }
    let skipped_pair_work_rate = if evidence.considered_shared_pair_work == 0 {
        0.0
    } else {
        evidence.skipped_shared_pair_work as f64 / evidence.considered_shared_pair_work as f64
    };
    let skipped_work_admitted = skipped_pair_work_rate <= policy.max_skipped_pair_work_rate;
    let mut max_stratum_skipped_pair_work_rate = 0.0f64;
    let mut strata_considered = 0u64;
    let mut strata_skipped = 0u64;
    for stratum in evidence.shared_work_strata {
        if stratum.skipped_pair_work > stratum.considered_pair_work {
            return Err(EvidenceError::InvalidStratumPairWork {
                log2_pair_work: stratum.log2_pair_work,
                skipped: stratum.skipped_pair_work,
                considered: stratum.considered_pair_work,
            });
        }
        strata_considered = strata_considered
            .checked_add(stratum.considered_pair_work)
            .ok_or(EvidenceError::CounterOverflow(
                "stratum considered pair work",
            ))?;
        strata_skipped = strata_skipped
            .checked_add(stratum.skipped_pair_work)
            .ok_or(EvidenceError::CounterOverflow("stratum skipped pair work"))?;
        let rate = if stratum.considered_pair_work == 0 {
            0.0
        } else {
            stratum.skipped_pair_work as f64 / stratum.considered_pair_work as f64
        };
        max_stratum_skipped_pair_work_rate = max_stratum_skipped_pair_work_rate.max(rate);
    }
    if strata_considered != evidence.considered_shared_pair_work
        || strata_skipped != evidence.skipped_shared_pair_work
    {
        return Err(EvidenceError::StratumTotalsMismatch {
            strata_considered,
            strata_skipped,
            considered: evidence.considered_shared_pair_work,
            skipped: evidence.skipped_shared_pair_work,
        });
    }
    let stratum_skips_admitted =
        max_stratum_skipped_pair_work_rate <= policy.max_skipped_pair_work_rate;
    let passed = if evidence.exhaustive {
        observed_misses == 0 && skipped_work_admitted && stratum_skips_admitted
    } else {
        sample_sufficient
            && wilson_upper_bound <= policy.max_miss_rate
            && skipped_work_admitted
            && stratum_skips_admitted
    };

    let mut skipped_shared_groups = evidence.skipped_shared_groups.to_vec();
    skipped_shared_groups.sort_unstable();
    skipped_shared_groups.dedup();
    Ok(EvidenceGateReport {
        policy,
        passed,
        sample_sufficient,
        observed_misses,
        evaluated_pair_work: evidence.evaluated_pair_work,
        evidence_exhaustive: evidence.exhaustive,
        exact_matches,
        wilson_upper_bound,
        pair_wilson_upper_bound,
        shared_wilson_upper_bound,
        pair_sample_sufficient,
        shared_sample_sufficient,
        skipped_shared_groups,
        skipped_pair_work_rate,
        max_stratum_skipped_pair_work_rate,
        pair_statistical_trials: evidence.pair_exact_matches,
        pair_statistical_misses: pair_residual_miss_count,
        shared_statistical_trials: evidence.shared_exact_matches,
        shared_statistical_misses: shared_residual_miss_count,
    })
}

fn wilson_upper_bound(misses: u64, trials: u64, z: f64) -> f64 {
    if trials == 0 {
        return 0.0;
    }
    let n = trials as f64;
    let p = misses as f64 / n;
    let z2 = z * z;
    let denominator = 1.0 + z2 / n;
    let centre = p + z2 / (2.0 * n);
    let radius = z * ((p * (1.0 - p) + z2 / (4.0 * n)) / n).sqrt();
    ((centre + radius) / denominator).clamp(0.0, 1.0)
}

#[cfg(test)]
mod tests {
    use super::{wilson_upper_bound, RescuePlan};
    use crate::exact_islands::{ExactMiss, SharedTokenExactMiss};

    #[test]
    fn rescue_coverage_matches_full_frontier_execution_semantics() {
        let plan = RescuePlan {
            pair_atoms: vec![3],
            shared_contracts: vec![11],
            shared_edges: vec![(20, 21)],
        };

        assert!(plan.covers_pair(&ExactMiss {
            left_atom: 3,
            right_atom: 99,
        }));
        assert!(plan.covers_pair(&ExactMiss {
            left_atom: 1,
            right_atom: 3,
        }));
        assert!(!plan.covers_pair(&ExactMiss {
            left_atom: 1,
            right_atom: 2,
        }));
        assert!(plan.covers_shared(&SharedTokenExactMiss {
            token_id: 8,
            left_contract: 11,
            right_contract: 12,
        }));
    }

    #[test]
    fn production_rescue_merge_deduplicates_both_evidence_partitions() {
        let merged = RescuePlan {
            pair_atoms: vec![1, 3],
            shared_contracts: vec![5, 7],
            shared_edges: vec![(10, 11), (12, 13)],
        }
        .merge(RescuePlan {
            pair_atoms: vec![2, 3],
            shared_contracts: vec![6, 7],
            shared_edges: vec![(11, 10), (14, 15)],
        });

        assert_eq!(merged.pair_atoms, vec![1, 2, 3]);
        assert_eq!(merged.shared_contracts, vec![5, 6, 7]);
        assert_eq!(merged.shared_edges, vec![(10, 11), (12, 13), (14, 15)]);
    }

    #[test]
    fn holdout_rescue_keeps_exact_edges_without_global_contract_generalization() {
        let plan = RescuePlan::from_holdout(
            &[],
            &[SharedTokenExactMiss {
                token_id: 9,
                left_contract: 12,
                right_contract: 4,
            }],
        );

        assert!(plan.shared_contracts.is_empty());
        assert_eq!(plan.shared_edges, vec![(4, 12)]);
    }

    #[test]
    fn wilson_pair_rate_regression_for_production_failure() {
        let upper = wilson_upper_bound(770_823, 211_407_756, 1.96);

        assert!((upper - 0.003_654_277_375).abs() < 1e-12);
        assert!(upper < 0.01);
    }
}
