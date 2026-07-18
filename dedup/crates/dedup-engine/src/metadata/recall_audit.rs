use super::{
    ContractAnchors, MetadataCandidate, PrefilterResult, TemplateFingerprint,
    fingerprint_bytes_equal, verify_metadata_pair,
};
use dedup_model::{ChainId, ContractId, DedupError, StageCounters};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug)]
pub struct StratifiedSampler {
    pub seed: u64,
    pub contracts_per_stratum: usize,
}

impl StratifiedSampler {
    pub fn sample(
        &self,
        contracts: &[ContractAnchors],
        templates: &[TemplateFingerprint],
    ) -> Vec<ContractId> {
        let low_information: BTreeMap<ContractId, bool> = templates
            .iter()
            .map(|template| (template.contract_id, template.low_information))
            .collect();
        let mut strata: BTreeMap<(ChainId, bool, usize), Vec<(u64, ContractId)>> = BTreeMap::new();
        for contract in contracts {
            let size_band = contract.anchors.len().next_power_of_two();
            let score = splitmix64(contract.contract_id.as_u64() ^ self.seed);
            strata
                .entry((
                    contract.chain_id,
                    low_information[&contract.contract_id],
                    size_band,
                ))
                .or_default()
                .push((score, contract.contract_id));
        }
        let mut sample = Vec::new();
        for members in strata.values_mut() {
            members.sort_unstable();
            sample.extend(
                members
                    .iter()
                    .take(self.contracts_per_stratum)
                    .map(|(_, contract)| *contract),
            );
        }
        sample.sort_unstable();
        sample
    }
}

#[derive(Clone, Debug, Default)]
pub struct ExhaustiveSharedTokenOracle;

impl ExhaustiveSharedTokenOracle {
    pub fn matches(
        &self,
        contracts: &[ContractAnchors],
        sample: &[ContractId],
        threshold: f64,
    ) -> Result<BTreeSet<MetadataCandidate>, DedupError> {
        let by_id: BTreeMap<ContractId, &ContractAnchors> = contracts
            .iter()
            .map(|contract| (contract.contract_id, contract))
            .collect();
        let mut matches = BTreeSet::new();
        for (position, left_id) in sample.iter().enumerate() {
            for right_id in &sample[position + 1..] {
                let Some(candidate) = MetadataCandidate::new(*left_id, *right_id) else {
                    continue;
                };
                if verify_metadata_pair(
                    by_id[left_id],
                    by_id[right_id],
                    threshold,
                    &mut StageCounters::default(),
                )?
                .matched
                {
                    matches.insert(candidate);
                }
            }
        }
        Ok(matches)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RecallBreakdown {
    pub true_positive_pairs: u64,
    pub retained_positive_pairs: u64,
    pub digest_bucket_cap_misses: u64,
    pub lsh_band_misses: u64,
    pub candidate_quota_misses: u64,
    pub low_information_guard_misses: u64,
}

impl RecallBreakdown {
    pub fn recall_ppm(&self) -> Option<u32> {
        (self.true_positive_pairs != 0).then(|| {
            u32::try_from(
                self.retained_positive_pairs.saturating_mul(1_000_000) / self.true_positive_pairs,
            )
            .unwrap_or(1_000_000)
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QualityDecision {
    Passed { recall_ppm: u32 },
}

pub fn audit_metadata_recall(
    oracle_matches: &BTreeSet<MetadataCandidate>,
    prefilter: &PrefilterResult,
    templates: &[TemplateFingerprint],
    minimum_positives: u64,
    required_recall: f64,
) -> Result<(RecallBreakdown, QualityDecision), DedupError> {
    let mut retained = BTreeSet::new();
    prefilter.candidates.visit(|candidate| {
        if oracle_matches.contains(&candidate) {
            retained.insert(candidate);
        }
        Ok(())
    })?;
    let template_by_id: BTreeMap<_, _> = templates
        .iter()
        .map(|template| (template.contract_id, template))
        .collect();
    let mut breakdown = RecallBreakdown {
        true_positive_pairs: u64::try_from(oracle_matches.len()).unwrap_or(u64::MAX),
        retained_positive_pairs: u64::try_from(oracle_matches.intersection(&retained).count())
            .unwrap_or(u64::MAX),
        ..RecallBreakdown::default()
    };
    if breakdown.true_positive_pairs < minimum_positives {
        return Err(DedupError::InsufficientPositives {
            actual: breakdown.true_positive_pairs,
            required: minimum_positives,
        });
    }
    for candidate in oracle_matches.difference(&retained) {
        let left = template_by_id[&candidate.left];
        let right = template_by_id[&candidate.right];
        if left.low_information || right.low_information {
            breakdown.low_information_guard_misses += 1;
        } else if prefilter
            .audit
            .generated_pairs_before_quota
            .contains(candidate)
        {
            breakdown.candidate_quota_misses += 1;
        } else if fingerprint_bytes_equal(left, right) {
            breakdown.digest_bucket_cap_misses += 1;
        } else {
            breakdown.lsh_band_misses += 1;
        }
    }
    let recall_ppm = breakdown
        .recall_ppm()
        .expect("minimum positives is non-zero after gate");
    let required_ppm = (required_recall.clamp(0.0, 1.0) * 1_000_000.0).round() as u32;
    if recall_ppm < required_ppm {
        return Err(DedupError::QualityGateFailed {
            recall_ppm,
            required_ppm,
        });
    }
    Ok((breakdown, QualityDecision::Passed { recall_ppm }))
}

const fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quality_gate_is_typed_and_exact() {
        let positives = BTreeSet::from([
            MetadataCandidate::new(ContractId::new(0), ContractId::new(1)).unwrap(),
            MetadataCandidate::new(ContractId::new(0), ContractId::new(2)).unwrap(),
        ]);
        let retained_candidate =
            MetadataCandidate::new(ContractId::new(0), ContractId::new(1)).unwrap();
        let prefilter = PrefilterResult {
            candidates: vec![retained_candidate].into(),
            audit: Default::default(),
        };
        let template = |id| TemplateFingerprint {
            contract_id: ContractId::new(id),
            feature_tokens: vec![vec![id as u8]],
            fingerprint_bytes: vec![id as u8].into(),
            template_digest: [id as u8; 32],
            low_information: false,
            discriminative_feature_count: 1,
        };
        assert!(matches!(
            audit_metadata_recall(
                &positives,
                &prefilter,
                &[template(0), template(1), template(2)],
                2,
                0.75
            ),
            Err(DedupError::QualityGateFailed { .. })
        ));
    }

    #[test]
    fn exact_pair_removed_by_quota_is_attributed_to_quota() {
        let candidate = MetadataCandidate::new(ContractId::new(0), ContractId::new(1)).unwrap();
        let positives = BTreeSet::from([candidate]);
        let prefilter = PrefilterResult {
            candidates: Vec::new().into(),
            audit: crate::metadata::PrefilterAudit {
                generated_pairs_before_quota: BTreeSet::from([candidate]),
                ..Default::default()
            },
        };
        let template = |id| TemplateFingerprint {
            contract_id: ContractId::new(id),
            feature_tokens: vec![b"shared".to_vec()],
            fingerprint_bytes: Vec::from(&b"same-fingerprint"[..]).into(),
            template_digest: [7; 32],
            low_information: false,
            discriminative_feature_count: 1,
        };

        let (breakdown, _) =
            audit_metadata_recall(&positives, &prefilter, &[template(0), template(1)], 1, 0.0)
                .unwrap();

        assert_eq!(breakdown.candidate_quota_misses, 1);
        assert_eq!(breakdown.digest_bucket_cap_misses, 0);
    }
}
