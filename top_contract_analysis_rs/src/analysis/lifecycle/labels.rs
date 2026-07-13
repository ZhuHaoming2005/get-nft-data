use super::metrics::{earliest_positive_time, is_outcome_lifecycle_event};
use super::*;

pub(super) fn build_weak_supervision_labels(
    lifecycle_metrics: &[ContractLifecycleMetricPayload],
    address_evidence_features: &[AddressEvidenceFeaturePayload],
    content_similarity_edges: &[ContentSimilarityEdgePayload],
) -> Vec<WeakSupervisionLabelPayload> {
    let mut labels = Vec::new();
    let content_confidence_by_contract = content_confidence_by_contract(content_similarity_edges);
    for metric in lifecycle_metrics {
        let content_confidence = content_confidence_by_contract
            .get(&metric.contract_address)
            .map(String::as_str)
            .unwrap_or("");
        if !content_confidence.is_empty()
            && (metric.sale_count > 0
                || metric.victim_count > 0
                || metric.operator_revenue_eth > 0.0
                || metric.operator_revenue_usd > 0.0)
        {
            labels.push(WeakSupervisionLabelPayload {
                entity_type: "contract".into(),
                contract_address: metric.contract_address.clone(),
                address: String::new(),
                label: "probable_infringement_campaign".into(),
                confidence: if content_confidence == "high" && metric.victim_count > 0 {
                    "high".into()
                } else {
                    "medium".into()
                },
                source: "rule_v1".into(),
                evidence_flags: compact_flags(vec![
                    format!("content_similarity:{content_confidence}"),
                    format!("sale_count:{}", metric.sale_count),
                    format!("victim_count:{}", metric.victim_count),
                    format!("operator_revenue_eth:{:.8}", metric.operator_revenue_eth),
                ]),
            });
        }
        if metric.early_detection_positive {
            labels.push(WeakSupervisionLabelPayload {
                entity_type: "contract".into(),
                contract_address: metric.contract_address.clone(),
                address: String::new(),
                label: "early_detection_positive".into(),
                confidence: "medium".into(),
                source: "rule_v1".into(),
                evidence_flags: vec![
                    format!("pre_sale_signal_count:{}", metric.pre_sale_signal_count),
                    format!(
                        "time_to_first_sale_seconds:{:?}",
                        metric.time_to_first_sale_seconds
                    ),
                ],
            });
        }
        if metric
            .top_value_recipient_share
            .map(|share| share >= 0.80)
            .unwrap_or(false)
            && (metric.operator_revenue_eth > 0.0 || metric.operator_revenue_usd > 0.0)
        {
            labels.push(WeakSupervisionLabelPayload {
                entity_type: "contract".into(),
                contract_address: metric.contract_address.clone(),
                address: metric.top_value_recipient_address.clone(),
                label: "concentrated_operator_revenue".into(),
                confidence: "medium".into(),
                source: "rule_v1".into(),
                evidence_flags: vec![
                    format!(
                        "top_value_recipient_share:{:.4}",
                        metric.top_value_recipient_share.unwrap_or_default()
                    ),
                    format!("operator_revenue_eth:{:.8}", metric.operator_revenue_eth),
                ],
            });
        }
    }

    for feature in address_evidence_features {
        let label = match feature.attribution_label.as_str() {
            "suspected_operator" => "operator_address_candidate",
            "suspected_colluder" => "colluder_address_candidate",
            "likely_victim" => "victim_address_candidate",
            "corrupted_victim" => "corrupted_victim_candidate",
            _ => continue,
        };
        labels.push(WeakSupervisionLabelPayload {
            entity_type: "address".into(),
            contract_address: feature.contract_address.clone(),
            address: feature.address.clone(),
            label: label.into(),
            confidence: feature.confidence.clone(),
            source: "address_attribution_rule_v1".into(),
            evidence_flags: feature
                .evidence
                .iter()
                .map(|evidence| evidence.evidence_type.clone())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
        });
    }

    labels.sort_by(|left, right| {
        (
            left.entity_type.as_str(),
            left.contract_address.as_str(),
            left.address.as_str(),
            left.label.as_str(),
        )
            .cmp(&(
                right.entity_type.as_str(),
                right.contract_address.as_str(),
                right.address.as_str(),
                right.label.as_str(),
            ))
    });
    labels
}

fn content_confidence_by_contract(
    content_similarity_edges: &[ContentSimilarityEdgePayload],
) -> BTreeMap<String, String> {
    let mut rows = BTreeMap::<String, String>::new();
    for edge in content_similarity_edges {
        rows.entry(edge.candidate_contract_address.clone())
            .and_modify(|existing| {
                if confidence_rank(&edge.confidence) > confidence_rank(existing.as_str()) {
                    *existing = edge.confidence.clone();
                }
            })
            .or_insert(edge.confidence.clone());
    }
    rows
}

fn confidence_rank(value: &str) -> i64 {
    match value {
        "high" => 3,
        "medium" => 2,
        "low" => 1,
        _ => 0,
    }
}

fn compact_flags(flags: Vec<String>) -> Vec<String> {
    flags
        .into_iter()
        .filter(|flag| !flag.trim().is_empty())
        .collect()
}

pub(super) fn build_early_detection_features(
    lifecycle_metrics: &[ContractLifecycleMetricPayload],
    lifecycle_events: &[ContractLifecycleEventPayload],
    value_flow_edges: &[ValueFlowEdgePayload],
) -> Vec<EarlyDetectionFeaturePayload> {
    let mut rows = Vec::new();
    for metric in lifecycle_metrics {
        if metric.deployment_time <= 0 {
            continue;
        }
        for window_seconds in [60_i64, 3_600_i64, 86_400_i64] {
            let window_end = metric.deployment_time + window_seconds;
            let contract_events: Vec<_> = lifecycle_events
                .iter()
                .filter(|event| event.contract_address == metric.contract_address)
                .filter(|event| event.block_time > 0 && event.block_time <= window_end)
                .collect();
            let contract_value_edges: Vec<_> = value_flow_edges
                .iter()
                .filter(|edge| edge.contract_address == metric.contract_address)
                .filter(|edge| edge.block_time > 0 && edge.block_time <= window_end)
                .collect();
            let sale_observed = metric.first_sale_time > 0 && metric.first_sale_time <= window_end;
            let victim_observed =
                metric.first_victim_time > 0 && metric.first_victim_time <= window_end;
            let future_positive =
                (metric.first_sale_time > window_end) || (metric.first_victim_time > window_end);
            let first_outcome_time =
                earliest_positive_time(metric.first_sale_time, metric.first_victim_time);
            rows.push(EarlyDetectionFeaturePayload {
                contract_address: metric.contract_address.clone(),
                observation_window_seconds: window_seconds,
                window_start_time: metric.deployment_time,
                window_end_time: window_end,
                content_similarity_count: contract_events
                    .iter()
                    .filter(|event| event.lifecycle_stage == "copy_preparation")
                    .count() as i64,
                mint_event_count: contract_events
                    .iter()
                    .filter(|event| event.event_type == "mint")
                    .count() as i64,
                value_flow_count: contract_value_edges.len() as i64,
                funding_edge_count: contract_value_edges
                    .iter()
                    .filter(|edge| edge.channel == "funding")
                    .count() as i64,
                withdrawal_edge_count: contract_value_edges
                    .iter()
                    .filter(|edge| edge.channel == "withdrawal")
                    .count() as i64,
                sale_event_count: contract_events
                    .iter()
                    .filter(|event| event.event_type == "sale")
                    .count() as i64,
                victim_signal_count: contract_events
                    .iter()
                    .filter(|event| event.lifecycle_stage == "victimization")
                    .count() as i64,
                pre_sale_signal_count: contract_events
                    .iter()
                    .filter(|event| !is_outcome_lifecycle_event(event))
                    .filter(|event| {
                        first_outcome_time <= 0
                            || (event.block_time > 0 && event.block_time < first_outcome_time)
                    })
                    .count() as i64,
                weak_label: if !sale_observed && !victim_observed && future_positive {
                    "positive_future_sale_or_victimization".into()
                } else if sale_observed || victim_observed {
                    "positive_observed_sale_or_victimization".into()
                } else {
                    "unlabeled".into()
                },
            });
        }
    }
    rows
}
