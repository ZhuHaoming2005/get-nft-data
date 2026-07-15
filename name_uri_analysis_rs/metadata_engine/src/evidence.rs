use crate::exact_islands::{
    ExactEvidenceCluster, ExactMiss, SharedTokenExactMiss, SharedTokenWorkStratum,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use thiserror::Error;

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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct SharedRescueSeed {
    pub token_id: u32,
    pub contract_id: u32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RescuePlan {
    pub pair_atoms: Vec<u32>,
    pub shared_seeds: Vec<SharedRescueSeed>,
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

        let mut shared_seeds = shared_misses
            .iter()
            .flat_map(|miss| {
                [
                    SharedRescueSeed {
                        token_id: miss.token_id,
                        contract_id: miss.left_contract,
                    },
                    SharedRescueSeed {
                        token_id: miss.token_id,
                        contract_id: miss.right_contract,
                    },
                ]
            })
            .collect::<Vec<_>>();
        shared_seeds.sort_unstable();
        shared_seeds.dedup();

        Self {
            pair_atoms,
            shared_seeds,
        }
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
        [miss.left_contract, miss.right_contract]
            .into_iter()
            .any(|contract_id| {
                self.shared_seeds
                    .binary_search(&SharedRescueSeed {
                        token_id: miss.token_id,
                        contract_id,
                    })
                    .is_ok()
            })
    }
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
    pub skipped_shared_groups: Vec<u32>,
    pub skipped_pair_work_rate: f64,
    pub max_stratum_skipped_pair_work_rate: f64,
    pub statistical_trials: u64,
    pub statistical_misses: u64,
}

#[derive(Debug, Error, PartialEq)]
pub enum EvidenceError {
    #[error("invalid max miss rate {0}; expected a finite value in [0, 1]")]
    InvalidMaxMissRate(f64),
    #[error("invalid confidence z-score {0}; expected a finite non-negative value")]
    InvalidConfidenceZ(f64),
    #[error("invalid skipped pair-work rate {0}; expected a finite value in [0, 1]")]
    InvalidSkippedPairWorkRate(f64),
    #[error("residual misses {misses} exceeds exact matches {exact_matches}")]
    MissesExceedExactMatches { misses: u64, exact_matches: u64 },
    #[error(
        "evidence cluster totals {cluster_matches} do not equal exact matches {exact_matches}"
    )]
    ClusterTotalsMismatch {
        cluster_matches: u64,
        exact_matches: u64,
    },
    #[error("residual miss references an absent or empty evidence cluster")]
    MissingEvidenceCluster,
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

    let pair_residual_misses = evidence
        .pair_misses
        .iter()
        .filter(|miss| !rescue_plan.covers_pair(miss))
        .collect::<Vec<_>>();
    let shared_residual_misses = evidence
        .shared_misses
        .iter()
        .filter(|miss| !rescue_plan.covers_shared(miss))
        .collect::<Vec<_>>();
    let observed_misses =
        (pair_residual_misses.len() as u64).saturating_add(shared_residual_misses.len() as u64);
    let exact_matches = evidence
        .pair_exact_matches
        .saturating_add(evidence.shared_exact_matches);
    if observed_misses > exact_matches {
        return Err(EvidenceError::MissesExceedExactMatches {
            misses: observed_misses,
            exact_matches,
        });
    }
    let cluster_matches = evidence
        .pair_clusters
        .iter()
        .chain(evidence.shared_clusters)
        .fold(0u64, |total, cluster| {
            total.saturating_add(cluster.exact_matches)
        });
    if cluster_matches != exact_matches {
        return Err(EvidenceError::ClusterTotalsMismatch {
            cluster_matches,
            exact_matches,
        });
    }
    let pair_miss_clusters = pair_residual_misses
        .iter()
        .map(|miss| miss.left_atom)
        .collect::<BTreeSet<_>>();
    let shared_miss_clusters = shared_residual_misses
        .iter()
        .map(|miss| miss.token_id)
        .collect::<BTreeSet<_>>();
    let cluster_exists = |clusters: &[ExactEvidenceCluster], id| {
        clusters
            .iter()
            .any(|cluster| cluster.id == id && cluster.exact_matches != 0)
    };
    if pair_miss_clusters
        .iter()
        .any(|&id| !cluster_exists(evidence.pair_clusters, id))
        || shared_miss_clusters
            .iter()
            .any(|&id| !cluster_exists(evidence.shared_clusters, id))
    {
        return Err(EvidenceError::MissingEvidenceCluster);
    }
    let statistical_trials = evidence
        .pair_clusters
        .iter()
        .chain(evidence.shared_clusters)
        .filter(|cluster| cluster.exact_matches != 0)
        .count() as u64;
    let statistical_misses = pair_miss_clusters.len() as u64 + shared_miss_clusters.len() as u64;
    let wilson_upper_bound =
        wilson_upper_bound(statistical_misses, statistical_trials, policy.confidence_z);
    let sample_sufficient = evidence.exhaustive || exact_matches >= policy.min_exact_matches;
    let skipped_pair_work_rate = if evidence.considered_shared_pair_work == 0 {
        0.0
    } else {
        evidence.skipped_shared_pair_work as f64 / evidence.considered_shared_pair_work as f64
    };
    let skipped_work_admitted = skipped_pair_work_rate <= policy.max_skipped_pair_work_rate;
    let mut max_stratum_skipped_pair_work_rate = 0.0f64;
    for stratum in evidence.shared_work_strata {
        if stratum.skipped_pair_work > stratum.considered_pair_work {
            return Err(EvidenceError::InvalidStratumPairWork {
                log2_pair_work: stratum.log2_pair_work,
                skipped: stratum.skipped_pair_work,
                considered: stratum.considered_pair_work,
            });
        }
        let rate = if stratum.considered_pair_work == 0 {
            0.0
        } else {
            stratum.skipped_pair_work as f64 / stratum.considered_pair_work as f64
        };
        max_stratum_skipped_pair_work_rate = max_stratum_skipped_pair_work_rate.max(rate);
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
        skipped_shared_groups,
        skipped_pair_work_rate,
        max_stratum_skipped_pair_work_rate,
        statistical_trials,
        statistical_misses,
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
    use super::{RescuePlan, SharedRescueSeed};
    use crate::exact_islands::{ExactMiss, SharedTokenExactMiss};

    #[test]
    fn rescue_coverage_matches_full_frontier_execution_semantics() {
        let plan = RescuePlan {
            pair_atoms: vec![3],
            shared_seeds: vec![SharedRescueSeed {
                token_id: 7,
                contract_id: 11,
            }],
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
            token_id: 7,
            left_contract: 11,
            right_contract: 12,
        }));
    }
}
