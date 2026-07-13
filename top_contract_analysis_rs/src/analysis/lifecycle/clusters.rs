use super::evidence::has_strong_campaign_address_evidence;
use super::value_flow::{
    summarize_value_flows, value_flow_coverage_gaps, VALUE_FLOW_COVERAGE_SCOPE,
};
use super::*;

pub(super) fn build_campaign_clusters(
    seed_contract: &SeedContractPayload,
    duplicate_contracts: &[DuplicateContractPayload],
    address_evidence_features: &[AddressEvidenceFeaturePayload],
    value_flow_edges: &[ValueFlowEdgePayload],
    lifecycle_events: &[ContractLifecycleEventPayload],
) -> Vec<CampaignClusterPayload> {
    let seed_contract_address = seed_contract.contract_address.as_str();
    let campaign_events: Vec<_> = lifecycle_events
        .iter()
        .filter(|event| is_campaign_lifecycle_event(event, seed_contract_address))
        .collect();

    let mut contract_addresses: BTreeSet<String> = duplicate_contracts
        .iter()
        .filter(|item| {
            !item.contract_address.is_empty()
                && item.contract_address != seed_contract.contract_address
        })
        .map(|item| item.contract_address.clone())
        .collect();
    for event in &campaign_events {
        contract_addresses.insert(event.contract_address.clone());
    }

    if contract_addresses.is_empty() {
        return vec![];
    }

    let mut adjacency: BTreeMap<String, BTreeSet<String>> = contract_addresses
        .iter()
        .map(|contract| (contract.clone(), BTreeSet::new()))
        .collect();
    let mut shared_evidence_by_contract: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

    let mut contracts_by_deployer = BTreeMap::<String, BTreeSet<String>>::new();
    for duplicate_contract in duplicate_contracts {
        if contract_addresses.contains(&duplicate_contract.contract_address)
            && !duplicate_contract.contract_deployer.is_empty()
        {
            contracts_by_deployer
                .entry(duplicate_contract.contract_deployer.clone())
                .or_default()
                .insert(duplicate_contract.contract_address.clone());
        }
    }
    for (deployer, contracts) in contracts_by_deployer {
        connect_contract_group(
            &mut adjacency,
            &mut shared_evidence_by_contract,
            &contracts,
            format!("shared_deployer:{deployer}"),
        );
    }

    let mut contracts_by_attributed_address = BTreeMap::<String, BTreeSet<String>>::new();
    for feature in address_evidence_features {
        if !contract_addresses.contains(&feature.contract_address)
            || !matches!(
                feature.attribution_label.as_str(),
                "suspected_operator" | "suspected_colluder" | "corrupted_victim"
            )
            || !has_strong_campaign_address_evidence(feature)
        {
            continue;
        }
        contracts_by_attributed_address
            .entry(format!("{}:{}", feature.attribution_label, feature.address))
            .or_default()
            .insert(feature.contract_address.clone());
    }
    for (key, contracts) in contracts_by_attributed_address {
        connect_contract_group(
            &mut adjacency,
            &mut shared_evidence_by_contract,
            &contracts,
            format!("shared_address:{key}"),
        );
    }

    let mut contracts_by_value_recipient = BTreeMap::<String, BTreeSet<String>>::new();
    for edge in value_flow_edges {
        if !contract_addresses.contains(&edge.contract_address)
            || !edge.recipient_known
            || edge.to_address.is_empty()
            || !matches!(
                edge.channel.as_str(),
                "mint_payment" | "royalty_fee" | "withdrawal"
            )
        {
            continue;
        }
        contracts_by_value_recipient
            .entry(format!("{}:{}", edge.channel, edge.to_address))
            .or_default()
            .insert(edge.contract_address.clone());
    }
    for (key, contracts) in contracts_by_value_recipient {
        connect_contract_group(
            &mut adjacency,
            &mut shared_evidence_by_contract,
            &contracts,
            format!("shared_value_recipient:{key}"),
        );
    }

    let mut contracts_by_funding_source = BTreeMap::<String, BTreeSet<String>>::new();
    for edge in value_flow_edges {
        if !contract_addresses.contains(&edge.contract_address)
            || !edge.recipient_known
            || edge.from_address.is_empty()
            || edge.channel != "funding"
        {
            continue;
        }
        contracts_by_funding_source
            .entry(edge.from_address.clone())
            .or_default()
            .insert(edge.contract_address.clone());
    }
    for (source, contracts) in contracts_by_funding_source {
        connect_contract_group(
            &mut adjacency,
            &mut shared_evidence_by_contract,
            &contracts,
            format!("shared_funding_source:{source}"),
        );
    }

    let components = connected_contract_components(&contract_addresses, &adjacency);
    let mut clusters = Vec::new();
    for component in components {
        let component_set: BTreeSet<String> = component.iter().cloned().collect();
        let component_events: Vec<_> = campaign_events
            .iter()
            .filter(|event| component_set.contains(&event.contract_address))
            .copied()
            .collect();
        let component_value_edges: Vec<_> = value_flow_edges
            .iter()
            .filter(|edge| component_set.contains(&edge.contract_address))
            .collect();
        let mut shared_evidence = component
            .iter()
            .flat_map(|contract| {
                shared_evidence_by_contract
                    .get(contract)
                    .into_iter()
                    .flatten()
                    .cloned()
            })
            .collect::<BTreeSet<_>>();
        if shared_evidence.is_empty() {
            shared_evidence.insert("single_contract_candidate".into());
        }
        let shared_evidence: Vec<String> = shared_evidence.into_iter().collect();
        let lifecycle_stages = component_events
            .iter()
            .map(|event| event.lifecycle_stage.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let token_count = component_events
            .iter()
            .filter(|event| !event.token_id.is_empty())
            .map(|event| event.token_id.clone())
            .collect::<BTreeSet<_>>()
            .len() as i64;
        let sale_count = component_events
            .iter()
            .filter(|event| event.event_type == "sale")
            .count() as i64;
        let revenue = summarize_value_flows(
            component_value_edges.iter().copied(),
            address_evidence_features,
        );
        let blocks: Vec<i64> = component_events
            .iter()
            .filter_map(|event| (event.block_number > 0).then_some(event.block_number))
            .collect();
        let times: Vec<i64> = component_events
            .iter()
            .filter_map(|event| (event.block_time > 0).then_some(event.block_time))
            .collect();
        let value_flow_channels = component_value_edges
            .iter()
            .map(|edge| edge.channel.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let first_contract = component.first().cloned().unwrap_or_default();
        clusters.push(CampaignClusterPayload {
            cluster_id: format!(
                "campaign:{}:{}",
                seed_contract.contract_address, first_contract
            ),
            seed_contract_address: seed_contract.contract_address.clone(),
            contract_addresses: component,
            suspected_operator_addresses: addresses_by_label_for_contracts(
                address_evidence_features,
                "suspected_operator",
                &component_set,
            ),
            suspected_colluder_addresses: addresses_by_label_for_contracts(
                address_evidence_features,
                "suspected_colluder",
                &component_set,
            ),
            victim_addresses: victim_addresses_for_contracts(
                address_evidence_features,
                &component_set,
            ),
            corrupted_victim_addresses: addresses_by_label_for_contracts(
                address_evidence_features,
                "corrupted_victim",
                &component_set,
            ),
            lifecycle_stages,
            shared_evidence: shared_evidence.clone(),
            value_flow_channels,
            cluster_confidence: campaign_cluster_confidence(&shared_evidence).into(),
            token_count,
            sale_count,
            value_flow_count: component_value_edges.len() as i64,
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
            first_block_number: blocks.iter().min().copied().unwrap_or_default(),
            last_block_number: blocks.iter().max().copied().unwrap_or_default(),
            first_block_time: times.iter().min().copied().unwrap_or_default(),
            last_block_time: times.iter().max().copied().unwrap_or_default(),
        });
    }
    clusters.sort_by(|left, right| {
        (
            left.first_block_number,
            left.first_block_time,
            left.contract_addresses
                .first()
                .map(String::as_str)
                .unwrap_or(""),
        )
            .cmp(&(
                right.first_block_number,
                right.first_block_time,
                right
                    .contract_addresses
                    .first()
                    .map(String::as_str)
                    .unwrap_or(""),
            ))
    });
    clusters
}

fn is_campaign_lifecycle_event(
    event: &ContractLifecycleEventPayload,
    seed_contract_address: &str,
) -> bool {
    !event.contract_address.is_empty()
        && event.contract_address != seed_contract_address
        && event.lifecycle_stage != "reference_deployment"
}

fn addresses_by_label_for_contracts(
    features: &[AddressEvidenceFeaturePayload],
    label: &str,
    contracts: &BTreeSet<String>,
) -> Vec<String> {
    features
        .iter()
        .filter(|feature| contracts.contains(&feature.contract_address))
        .filter(|feature| feature.attribution_label == label)
        .map(|feature| feature.address.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn victim_addresses_for_contracts(
    features: &[AddressEvidenceFeaturePayload],
    contracts: &BTreeSet<String>,
) -> Vec<String> {
    features
        .iter()
        .filter(|feature| contracts.contains(&feature.contract_address))
        .filter(|feature| {
            matches!(
                feature.attribution_label.as_str(),
                "likely_victim" | "corrupted_victim"
            )
        })
        .map(|feature| feature.address.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn connect_contract_group(
    adjacency: &mut BTreeMap<String, BTreeSet<String>>,
    shared_evidence_by_contract: &mut BTreeMap<String, BTreeSet<String>>,
    contracts: &BTreeSet<String>,
    evidence: String,
) {
    if contracts.len() < 2 {
        return;
    }
    for contract in contracts {
        shared_evidence_by_contract
            .entry(contract.clone())
            .or_default()
            .insert(evidence.clone());
    }
    let contracts: Vec<_> = contracts.iter().cloned().collect();
    for left_index in 0..contracts.len() {
        for right_index in (left_index + 1)..contracts.len() {
            adjacency
                .entry(contracts[left_index].clone())
                .or_default()
                .insert(contracts[right_index].clone());
            adjacency
                .entry(contracts[right_index].clone())
                .or_default()
                .insert(contracts[left_index].clone());
        }
    }
}

fn connected_contract_components(
    contracts: &BTreeSet<String>,
    adjacency: &BTreeMap<String, BTreeSet<String>>,
) -> Vec<Vec<String>> {
    let mut visited = BTreeSet::new();
    let mut components = Vec::new();
    for contract in contracts {
        if visited.contains(contract) {
            continue;
        }
        let mut stack = vec![contract.clone()];
        let mut component = BTreeSet::new();
        while let Some(current) = stack.pop() {
            if !visited.insert(current.clone()) {
                continue;
            }
            component.insert(current.clone());
            if let Some(neighbors) = adjacency.get(&current) {
                for neighbor in neighbors {
                    if !visited.contains(neighbor) {
                        stack.push(neighbor.clone());
                    }
                }
            }
        }
        components.push(component.into_iter().collect());
    }
    components
}

fn campaign_cluster_confidence(shared_evidence: &[String]) -> &'static str {
    if shared_evidence.iter().any(|item| {
        item.starts_with("shared_deployer:") || item.starts_with("shared_value_recipient:")
    }) {
        "high"
    } else if shared_evidence
        .iter()
        .any(|item| item.starts_with("shared_address:"))
    {
        "medium"
    } else {
        "low"
    }
}
