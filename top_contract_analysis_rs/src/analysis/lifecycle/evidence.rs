use super::*;

pub(super) fn build_address_evidence_features(
    attributions: &[AddressAttributionPayload],
) -> Vec<AddressEvidenceFeaturePayload> {
    let mut rows: Vec<_> = attributions
        .iter()
        .map(|item| {
            let related_tokens: BTreeSet<String> = item
                .evidence
                .iter()
                .filter(|evidence| !evidence.token_id.is_empty())
                .map(|evidence| evidence.token_id.clone())
                .collect();
            let related_txs: BTreeSet<String> = item
                .evidence
                .iter()
                .filter(|evidence| !evidence.tx_hash.is_empty())
                .map(|evidence| evidence.tx_hash.clone())
                .collect();
            AddressEvidenceFeaturePayload {
                contract_address: item.contract_address.clone(),
                address: item.address.clone(),
                observed_roles: item.observed_roles.clone(),
                attribution_label: if item.attribution_label.is_empty() {
                    "neutral_participant".into()
                } else {
                    item.attribution_label.clone()
                },
                operator_score: item.operator_score,
                colluder_score: item.colluder_score,
                victim_score: item.victim_score,
                corruption_score: item.corruption_score,
                neutral_score: item.neutral_score,
                confidence: item.confidence.clone(),
                operator_level: item.operator_level,
                operator_level_label: item.operator_level_label.clone(),
                evidence_count: item.evidence.len() as i64,
                related_token_count: related_tokens.len() as i64,
                related_tx_count: related_txs.len() as i64,
                evidence: item.evidence.clone(),
            }
        })
        .collect();
    rows.sort_by(|left, right| {
        (
            left.contract_address.as_str(),
            left.address.as_str(),
            left.attribution_label.as_str(),
        )
            .cmp(&(
                right.contract_address.as_str(),
                right.address.as_str(),
                right.attribution_label.as_str(),
            ))
    });
    rows
}

pub(super) fn build_content_similarity_edges(
    seed_contract: &SeedContractPayload,
    candidates: &[DuplicateCandidate],
) -> Vec<ContentSimilarityEdgePayload> {
    let mut rows: Vec<_> = candidates
        .iter()
        .filter(|candidate| !candidate.contract_address.is_empty())
        .map(|candidate| {
            let confidence = if candidate.confidence.trim().is_empty() {
                infer_similarity_confidence(&candidate.match_reasons)
            } else {
                candidate.confidence.clone()
            };
            ContentSimilarityEdgePayload {
                edge_id: format!(
                    "content:{}:{}:{}",
                    seed_contract.contract_address, candidate.contract_address, candidate.token_id
                ),
                seed_contract_address: seed_contract.contract_address.clone(),
                candidate_contract_address: candidate.contract_address.clone(),
                token_id: candidate.token_id.clone(),
                match_reasons: candidate.match_reasons.clone(),
                confidence,
                evidence_type: "content_similarity".into(),
            }
        })
        .collect();
    rows.sort_by(|left, right| {
        (
            left.candidate_contract_address.as_str(),
            left.token_id.as_str(),
        )
            .cmp(&(
                right.candidate_contract_address.as_str(),
                right.token_id.as_str(),
            ))
    });
    rows
}

fn infer_similarity_confidence(match_reasons: &[String]) -> String {
    if match_reasons.iter().any(|reason| {
        matches!(
            reason.as_str(),
            "token_uri_match" | "image_uri_match" | "metadata_match"
        )
    }) {
        "high".into()
    } else if match_reasons
        .iter()
        .any(|reason| reason.contains("name") || reason.contains("symbol"))
    {
        "medium".into()
    } else {
        "low".into()
    }
}

pub(super) fn has_strong_campaign_address_evidence(
    feature: &AddressEvidenceFeaturePayload,
) -> bool {
    feature.evidence.iter().any(|evidence| {
        matches!(
            evidence.evidence_type.as_str(),
            "wash_cycle"
                | "star_distribution"
                | "rapid_spread"
                | "corrupted_honest_resale"
                | "contract_value_withdrawal"
                | "multi_hop_cashout"
        )
    })
}

pub(super) fn has_strong_operator_address_evidence(
    features: &[AddressEvidenceFeaturePayload],
    contract_address: &str,
    address: &str,
) -> bool {
    features.iter().any(|feature| {
        normalize_chain_identity(&feature.contract_address)
            == normalize_chain_identity(contract_address)
            && normalize_chain_identity(&feature.address) == normalize_chain_identity(address)
            && matches!(
                feature.attribution_label.as_str(),
                "suspected_operator" | "suspected_colluder" | "corrupted_victim"
            )
            && has_strong_campaign_address_evidence(feature)
    })
}
