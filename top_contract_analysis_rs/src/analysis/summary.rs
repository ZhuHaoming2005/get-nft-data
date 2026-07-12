use std::collections::{BTreeMap, BTreeSet};

use crate::models::{
    normalize_chain_identity, AddressAttributionPayload, ContractLevelSummaryPayload,
    ContractMetadata, DuplicateCandidate, DuplicateContractPayload, MaliciousAddressPayload,
    NftPropagationPathPayload, SecondarySaleVictimAddressPayload, SeedCollectionStatsPayload,
    SeedNft, ValueFlowEdgePayload, VictimAcquisitionAddressPayload,
};
use crate::normalize::{normalize_name, normalize_symbol, normalize_url};

use super::enrich_duplicate_contract_payload_with_metadata;

pub(super) fn build_contract_payload(
    contract_address: &str,
    contract_candidates: &[DuplicateCandidate],
    metadata: Option<&ContractMetadata>,
) -> DuplicateContractPayload {
    let mut match_reasons: BTreeSet<String> = BTreeSet::new();
    for item in contract_candidates {
        for reason in &item.match_reasons {
            match_reasons.insert(reason.clone());
        }
    }
    enrich_duplicate_contract_payload_with_metadata(
        DuplicateContractPayload {
            contract_address: contract_address.to_string(),
            candidate_count: contract_candidates.len() as i64,
            match_reasons: match_reasons.into_iter().collect(),
            mint_recipients: vec![],
            ..DuplicateContractPayload::default()
        },
        metadata,
    )
}

pub(super) fn build_duplicate_contract_payloads(
    expanded_candidates_by_contract: &BTreeMap<String, Vec<DuplicateCandidate>>,
    candidate_contract_metadata: &BTreeMap<String, ContractMetadata>,
) -> Vec<DuplicateContractPayload> {
    expanded_candidates_by_contract
        .iter()
        .map(|(contract_address, items)| {
            build_contract_payload(
                contract_address,
                items,
                candidate_contract_metadata.get(contract_address),
            )
        })
        .collect()
}

pub(super) fn build_seed_collection_stats(seed_nfts: &[SeedNft]) -> SeedCollectionStatsPayload {
    let unique_token_uri_count = seed_nfts
        .iter()
        .filter_map(|item| normalize_url(&item.token_uri))
        .collect::<BTreeSet<_>>()
        .len() as i64;
    let unique_image_uri_count = seed_nfts
        .iter()
        .filter_map(|item| normalize_url(&item.image_uri))
        .collect::<BTreeSet<_>>()
        .len() as i64;
    let unique_name_count = seed_nfts
        .iter()
        .map(|item| normalize_name(&item.name))
        .filter(|value| !value.is_empty())
        .collect::<BTreeSet<_>>()
        .len() as i64;
    let unique_symbol_count = seed_nfts
        .iter()
        .map(|item| normalize_symbol(&item.symbol))
        .filter(|value| !value.is_empty())
        .collect::<BTreeSet<_>>()
        .len() as i64;

    SeedCollectionStatsPayload {
        seed_nft_count: seed_nfts.len() as i64,
        unique_token_uri_count,
        unique_image_uri_count,
        unique_name_count,
        unique_symbol_count,
    }
}

pub(super) fn build_contract_level_summary(
    expanded_candidates_by_contract: &BTreeMap<String, Vec<DuplicateCandidate>>,
) -> BTreeMap<String, ContractLevelSummaryPayload> {
    expanded_candidates_by_contract
        .iter()
        .map(|(contract_address, items)| {
            (
                contract_address.clone(),
                ContractLevelSummaryPayload {
                    candidate_count: items.len() as i64,
                },
            )
        })
        .collect()
}

pub(super) fn malicious_address_set(
    malicious_addresses: &[MaliciousAddressPayload],
) -> BTreeSet<String> {
    malicious_addresses
        .iter()
        .map(|item| normalized_address(&item.address))
        .filter(|value| !value.is_empty())
        .collect()
}

pub(super) fn paid_mint_victim_address_set(
    address_attributions: &[AddressAttributionPayload],
) -> BTreeSet<String> {
    address_attributions
        .iter()
        .filter(|item| is_victim_attribution_label(&item.attribution_label))
        .filter(|item| {
            item.evidence
                .iter()
                .any(|evidence| evidence.evidence_type == "paid_mint_payment")
        })
        .map(|item| normalized_address(&item.address))
        .filter(|value| !value.is_empty())
        .collect()
}

#[cfg(test)]
pub(super) fn build_victim_acquisition_addresses(
    secondary_sale_victim_addresses: &[SecondarySaleVictimAddressPayload],
    address_attributions: &[AddressAttributionPayload],
    value_flow_edges: &[ValueFlowEdgePayload],
    propagation_paths: &BTreeMap<String, NftPropagationPathPayload>,
) -> Vec<VictimAcquisitionAddressPayload> {
    build_victim_acquisition_addresses_excluding_malicious(
        secondary_sale_victim_addresses,
        address_attributions,
        value_flow_edges,
        propagation_paths,
        &[],
    )
}

pub(super) fn build_victim_acquisition_addresses_excluding_malicious(
    secondary_sale_victim_addresses: &[SecondarySaleVictimAddressPayload],
    address_attributions: &[AddressAttributionPayload],
    value_flow_edges: &[ValueFlowEdgePayload],
    propagation_paths: &BTreeMap<String, NftPropagationPathPayload>,
    malicious_addresses: &[MaliciousAddressPayload],
) -> Vec<VictimAcquisitionAddressPayload> {
    let malicious_addresses = malicious_address_set(malicious_addresses);
    let paid_mint_victim_addresses = paid_mint_victim_address_set(address_attributions)
        .into_iter()
        .filter(|address| !malicious_addresses.contains(address))
        .collect::<BTreeSet<_>>();
    let mut labels_by_address = BTreeMap::<String, BTreeSet<String>>::new();
    for attribution in address_attributions {
        let address = normalized_address(&attribution.address);
        if address.is_empty() || !is_victim_attribution_label(&attribution.attribution_label) {
            continue;
        }
        labels_by_address
            .entry(address)
            .or_default()
            .insert(attribution.attribution_label.clone());
    }

    let mut rows = BTreeMap::<String, VictimAcquisitionAddressPayload>::new();
    let mut gas_extra_by_address = BTreeMap::<String, (f64, f64)>::new();
    for victim in secondary_sale_victim_addresses {
        let address = normalized_address(&victim.address);
        if address.is_empty() || malicious_addresses.contains(&address) {
            continue;
        }
        let row = rows
            .entry(address.clone())
            .or_insert_with(|| VictimAcquisitionAddressPayload {
                address: victim.address.clone(),
                ..VictimAcquisitionAddressPayload::default()
            });
        push_unique(&mut row.contract_addresses, &victim.contract_address);
        push_unique(&mut row.acquisition_channels, "secondary_sale");
        push_unique_many(&mut row.tx_hashes, &victim.buy_tx_hashes);
        row.secondary_sale_cost_eth += victim.buy_amount_eth;
        row.secondary_sale_cost_usd += victim.buy_amount_usd;
        row.secondary_sale_count += victim.buy_tx_hashes.len() as i64;
        if victim.is_stuck {
            row.secondary_sale_stuck_cost_eth += victim.last_buy_amount_eth.unwrap_or_default();
            row.secondary_sale_stuck_cost_usd += victim.last_buy_amount_usd.unwrap_or_default();
            row.is_stuck = true;
        }
    }

    for edge in value_flow_edges {
        if edge.channel != "mint_payment" {
            continue;
        }
        let payer = normalized_address(&edge.from_address);
        if payer.is_empty() || !paid_mint_victim_addresses.contains(&payer) {
            continue;
        }
        let row = rows
            .entry(payer)
            .or_insert_with(|| VictimAcquisitionAddressPayload {
                address: edge.from_address.clone(),
                ..VictimAcquisitionAddressPayload::default()
            });
        push_unique(&mut row.contract_addresses, &edge.contract_address);
        push_unique(&mut row.acquisition_channels, "paid_mint");
        push_unique(&mut row.tx_hashes, &edge.tx_hash);
        row.paid_mint_cost_eth += edge.value_eth.unwrap_or_default();
        row.paid_mint_cost_usd += edge.value_usd.unwrap_or_default();
        if row.buy_before_eth_balance.is_none() {
            row.buy_before_eth_balance = edge.from_before_eth_balance;
        }
        if row.buy_before_usd_balance.is_none() {
            row.buy_before_usd_balance = edge.from_before_usd_balance;
        }
        let gas_extra_eth = edge_sender_paid_gas_eth(edge);
        let gas_extra_usd = edge_sender_paid_gas_usd(edge);
        if gas_extra_eth > 0.0 || gas_extra_usd > 0.0 {
            let entry = gas_extra_by_address
                .entry(normalize_chain_identity(&row.address))
                .or_default();
            entry.0 += gas_extra_eth;
            entry.1 += gas_extra_usd;
        }
        row.paid_mint_edge_count += 1;
        let (stuck_token_count, total_token_count) =
            paid_mint_stuck_token_counts(edge, propagation_paths);
        row.paid_mint_token_count += total_token_count as i64;
        if stuck_token_count > 0 && total_token_count > 0 {
            let stuck_fraction = stuck_token_count as f64 / total_token_count as f64;
            row.paid_mint_stuck_token_count += stuck_token_count as i64;
            row.paid_mint_stuck_cost_eth += edge.value_eth.unwrap_or_default() * stuck_fraction;
            row.paid_mint_stuck_cost_usd += edge.value_usd.unwrap_or_default() * stuck_fraction;
            row.is_stuck = true;
        }
    }

    for (address, row) in &mut rows {
        if let Some(labels) = labels_by_address.get(address) {
            row.attribution_labels = labels.iter().cloned().collect();
            row.is_corrupted = labels.iter().any(|label| label == "corrupted_victim");
        }
        row.total_acquisition_cost_eth = row.secondary_sale_cost_eth + row.paid_mint_cost_eth;
        row.total_acquisition_cost_usd = row.secondary_sale_cost_usd + row.paid_mint_cost_usd;
        row.total_stuck_cost_eth = row.secondary_sale_stuck_cost_eth + row.paid_mint_stuck_cost_eth;
        row.total_stuck_cost_usd = row.secondary_sale_stuck_cost_usd + row.paid_mint_stuck_cost_usd;
        row.buy_asset_ratio = acquisition_ratio(
            row.total_acquisition_cost_usd,
            row.total_acquisition_cost_eth,
            row.buy_before_usd_balance,
            row.buy_before_eth_balance,
        )
        .or(row.buy_asset_ratio);
        let (gas_extra_eth, gas_extra_usd) = gas_extra_by_address
            .get(address)
            .copied()
            .unwrap_or_default();
        row.buy_asset_ratio_with_gas = acquisition_ratio(
            row.total_acquisition_cost_usd + gas_extra_usd,
            row.total_acquisition_cost_eth + gas_extra_eth,
            row.buy_before_usd_balance,
            row.buy_before_eth_balance,
        )
        .or(row.buy_asset_ratio_with_gas);
    }

    rows.into_values().collect()
}

pub(super) fn acquisition_ratio(
    acquisition_cost_usd: f64,
    acquisition_cost_eth: f64,
    before_usd_balance: Option<f64>,
    before_eth_balance: Option<f64>,
) -> Option<f64> {
    if let Some(balance) = before_usd_balance.filter(|value| *value > 0.0) {
        if acquisition_cost_usd > 0.0 {
            return Some(acquisition_cost_usd / balance);
        }
    }
    if let Some(balance) = before_eth_balance.filter(|value| *value > 0.0) {
        if acquisition_cost_eth > 0.0 {
            return Some(acquisition_cost_eth / balance);
        }
    }
    None
}

fn edge_sender_paid_gas(edge: &ValueFlowEdgePayload) -> bool {
    edge.gas_payer_address.trim().is_empty()
        || normalized_address(&edge.gas_payer_address) == normalized_address(&edge.from_address)
}

fn edge_sender_paid_gas_eth(edge: &ValueFlowEdgePayload) -> f64 {
    if !edge_sender_paid_gas(edge) {
        return 0.0;
    }
    edge.gas_eth.unwrap_or_else(|| {
        let value_with_gas = edge
            .value_with_gas_eth
            .unwrap_or_else(|| edge.value_eth.unwrap_or_default());
        (value_with_gas - edge.value_eth.unwrap_or_default()).max(0.0)
    })
}

fn edge_sender_paid_gas_usd(edge: &ValueFlowEdgePayload) -> f64 {
    if !edge_sender_paid_gas(edge) {
        return 0.0;
    }
    edge.gas_usd.unwrap_or_else(|| {
        let value_with_gas = edge
            .value_with_gas_usd
            .unwrap_or_else(|| edge.value_usd.unwrap_or_default());
        (value_with_gas - edge.value_usd.unwrap_or_default()).max(0.0)
    })
}

pub(super) fn push_unique(values: &mut Vec<String>, value: &str) {
    let value = value.trim();
    if value.is_empty() || values.iter().any(|existing| existing == value) {
        return;
    }
    values.push(value.to_string());
}

pub(super) fn push_unique_many(values: &mut Vec<String>, new_values: &[String]) {
    for value in new_values {
        push_unique(values, value);
    }
}

pub(super) fn is_victim_attribution_label(label: &str) -> bool {
    matches!(label, "likely_victim" | "corrupted_victim")
}

pub(super) fn normalized_address(address: &str) -> String {
    normalize_chain_identity(address)
}

pub(super) fn paid_mint_stuck_token_counts(
    edge: &ValueFlowEdgePayload,
    propagation_paths: &BTreeMap<String, NftPropagationPathPayload>,
) -> (usize, usize) {
    let token_ids = value_flow_token_ids(edge);
    if token_ids.is_empty() {
        return (0, 0);
    }
    let Some(path) = propagation_paths.get(&edge.contract_address).or_else(|| {
        propagation_paths.values().find(|path| {
            normalized_address(&path.contract_address) == normalized_address(&edge.contract_address)
        })
    }) else {
        return (0, token_ids.len());
    };
    let payer = normalized_address(&edge.from_address);
    let mut stuck_count = 0usize;
    for token_id in &token_ids {
        if path
            .token_paths
            .iter()
            .find(|token_path| token_path.token_id == *token_id)
            .map(|token_path| {
                token_path
                    .current_holder_addresses
                    .iter()
                    .any(|holder| normalized_address(holder) == payer)
            })
            .unwrap_or(false)
        {
            stuck_count += 1;
        }
    }
    (stuck_count, token_ids.len())
}

pub(super) fn value_flow_token_ids(edge: &ValueFlowEdgePayload) -> Vec<String> {
    let mut token_ids = Vec::new();
    for token_id in edge.token_id.split(',') {
        let token_id = token_id.trim();
        if !token_id.is_empty() {
            token_ids.push(token_id.to_string());
        }
    }
    token_ids.sort();
    token_ids.dedup();
    token_ids
}
