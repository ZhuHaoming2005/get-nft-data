use crate::model::{
    CandidateId, EvidenceBundle, RelationClassification, RelationLabel, SeedCandidateRelation,
};

pub fn classify_relations(
    candidate_id: CandidateId,
    relations: &[SeedCandidateRelation],
    evidence: &EvidenceBundle,
) -> Vec<RelationLabel> {
    let mut labels = relations
        .iter()
        .map(|relation| {
            let verification = evidence
                .relation_verifications
                .binary_search_by_key(&relation.seed_id, |verification| verification.seed_id)
                .ok()
                .map(|index| &evidence.relation_verifications[index]);
            let classification = match verification {
                Some(verification)
                    if verification.official_controller_continuity
                        || verification.authorized_reissue
                        || verification.verified_migration
                        || verification.official_collection_relation =>
                {
                    RelationClassification::LegitDuplicate {
                        evidence: verification.evidence_keys.clone(),
                    }
                }
                Some(verification) => RelationClassification::SuspectedDuplicate {
                    legit_verification_complete: verification.complete,
                },
                None => RelationClassification::SuspectedDuplicate {
                    legit_verification_complete: false,
                },
            };
            RelationLabel {
                seed_id: relation.seed_id,
                candidate_id,
                classification,
            }
        })
        .collect::<Vec<_>>();
    labels.sort_by_key(|label| label.seed_id);
    labels
}
