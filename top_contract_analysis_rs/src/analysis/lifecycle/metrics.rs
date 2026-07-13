use super::value_flow::{
    summarize_value_flows, value_flow_coverage_gaps, VALUE_FLOW_COVERAGE_SCOPE,
};
use super::*;

pub(super) fn build_lifecycle_metrics(
    seed_contract: &SeedContractPayload,
    propagation_paths: &BTreeMap<String, NftPropagationPathPayload>,
    address_evidence_features: &[AddressEvidenceFeaturePayload],
    value_flow_edges: &[ValueFlowEdgePayload],
    lifecycle_events: &[ContractLifecycleEventPayload],
) -> Vec<ContractLifecycleMetricPayload> {
    let mut contracts: BTreeSet<String> = propagation_paths.keys().cloned().collect();
    contracts.extend(
        lifecycle_events
            .iter()
            .map(|event| event.contract_address.clone())
            .filter(|value| !value.is_empty()),
    );
    contracts.remove(&seed_contract.contract_address);

    contracts
        .into_iter()
        .filter(|contract| !contract.is_empty())
        .map(|contract| {
            let deployment_time =
                first_stage_time(lifecycle_events, &contract, "replica_deployment");
            let first_mint_time = first_stage_time(lifecycle_events, &contract, "replica_mint");
            let first_transfer_time = earliest_positive_time(
                first_stage_time(lifecycle_events, &contract, "distribution"),
                first_stage_time(lifecycle_events, &contract, "monetization"),
            );
            let first_sale_time = first_stage_time(lifecycle_events, &contract, "monetization");
            let first_victim_time = earliest_positive_time(
                first_stage_time(lifecycle_events, &contract, "victimization"),
                first_paid_mint_victim_time(value_flow_edges, &contract),
            );
            let path_summary = propagation_paths.get(&contract).map(|path| &path.summary);
            let victim_count = address_evidence_features
                .iter()
                .filter(|feature| feature.contract_address == contract)
                .filter(|feature| {
                    matches!(
                        feature.attribution_label.as_str(),
                        "likely_victim" | "corrupted_victim"
                    )
                })
                .count() as i64;
            let revenue = summarize_value_flows(
                value_flow_edges
                    .iter()
                    .filter(|edge| edge.contract_address == contract),
                address_evidence_features,
            );
            let first_outcome_time = earliest_positive_time(first_sale_time, first_victim_time);
            let pre_sale_signal_count =
                pre_sale_signal_count(lifecycle_events, &contract, first_outcome_time);
            let sale_observed = first_outcome_time > 0;
            let early_detection_positive =
                sale_observed && deployment_time > 0 && pre_sale_signal_count >= 2;
            ContractLifecycleMetricPayload {
                contract_address: contract.clone(),
                deployment_time,
                first_mint_time,
                first_transfer_time,
                first_sale_time,
                first_victim_time,
                time_to_first_transfer_seconds: elapsed(deployment_time, first_transfer_time),
                time_to_first_sale_seconds: elapsed(deployment_time, first_sale_time),
                time_to_first_victim_seconds: elapsed(deployment_time, first_victim_time),
                cascade_node_count: path_summary.map(|summary| summary.node_count).unwrap_or(0),
                cascade_edge_count: path_summary.map(|summary| summary.edge_count).unwrap_or(0),
                victim_count,
                sale_count: path_summary
                    .map(|summary| summary.sale_edge_count)
                    .unwrap_or(0),
                gross_revenue_eth: revenue.gross_eth,
                gross_revenue_usd: revenue.gross_usd,
                operator_revenue_eth: revenue.operator_eth,
                operator_revenue_usd: revenue.operator_usd,
                marketplace_fee_eth: revenue.marketplace_fee_eth,
                marketplace_fee_usd: revenue.marketplace_fee_usd,
                funding_amount_eth: revenue.funding_amount_eth,
                funding_amount_usd: revenue.funding_amount_usd,
                withdrawal_amount_eth: revenue.withdrawal_amount_eth,
                withdrawal_amount_usd: revenue.withdrawal_amount_usd,
                funding_edge_count: revenue.funding_edge_count,
                withdrawal_edge_count: revenue.withdrawal_edge_count,
                revenue_backflow_edge_count: revenue.revenue_backflow_edge_count,
                value_flow_coverage_scope: VALUE_FLOW_COVERAGE_SCOPE.into(),
                value_flow_coverage_gaps: value_flow_coverage_gaps(),
                top_value_recipient_address: revenue.top_value_recipient_address,
                top_value_recipient_eth: revenue.top_value_recipient_eth,
                top_value_recipient_usd: revenue.top_value_recipient_usd,
                top_value_recipient_share: revenue.top_value_recipient_share,
                pre_sale_signal_count,
                early_detection_positive,
            }
        })
        .collect()
}

fn pre_sale_signal_count(
    lifecycle_events: &[ContractLifecycleEventPayload],
    contract_address: &str,
    first_outcome_time: i64,
) -> i64 {
    lifecycle_events
        .iter()
        .filter(|event| event.contract_address == contract_address)
        .filter(|event| event.lifecycle_stage != "stage_transition")
        .filter(|event| !is_outcome_lifecycle_event(event))
        .filter(|event| {
            event.block_time > 0
                && (first_outcome_time <= 0 || event.block_time < first_outcome_time)
        })
        .count() as i64
}

pub(super) fn earliest_positive_time(left: i64, right: i64) -> i64 {
    match (left > 0, right > 0) {
        (true, true) => left.min(right),
        (true, false) => left,
        (false, true) => right,
        (false, false) => 0,
    }
}

fn first_paid_mint_victim_time(
    value_flow_edges: &[ValueFlowEdgePayload],
    contract_address: &str,
) -> i64 {
    value_flow_edges
        .iter()
        .filter(|edge| edge.contract_address == contract_address)
        .filter(|edge| edge.channel == "mint_payment")
        .filter(|edge| edge.block_time > 0 && value_flow_has_positive_amount(edge))
        .map(|edge| edge.block_time)
        .min()
        .unwrap_or(0)
}

fn value_flow_has_positive_amount(edge: &ValueFlowEdgePayload) -> bool {
    edge.value_eth.unwrap_or(0.0) > 0.0 || edge.value_usd.unwrap_or(0.0) > 0.0
}

pub(super) fn is_outcome_lifecycle_event(event: &ContractLifecycleEventPayload) -> bool {
    matches!(
        event.lifecycle_stage.as_str(),
        "monetization" | "primary_monetization" | "victimization"
    ) || matches!(
        event.event_type.as_str(),
        "sale" | "mint_payment" | "secondary_sale_victim_acquisition"
    )
}

fn first_stage_time(
    lifecycle_events: &[ContractLifecycleEventPayload],
    contract_address: &str,
    stage: &str,
) -> i64 {
    lifecycle_events
        .iter()
        .filter(|event| event.contract_address == contract_address)
        .filter(|event| event.lifecycle_stage == stage)
        .filter_map(|event| (event.block_time > 0).then_some(event.block_time))
        .min()
        .unwrap_or_default()
}

fn elapsed(start: i64, end: i64) -> Option<i64> {
    (start > 0 && end >= start).then_some(end - start)
}
