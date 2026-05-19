use std::collections::{BTreeMap, BTreeSet};

use crate::models::{
    AddressAttributionPayload, AddressSignalPayload, BatchReportSummary, BatchSeedReportPayload,
    ContractLevelSummaryPayload, ContractLifecycleMetricPayload, ContractMetadata,
    DuplicateCandidate, DuplicateContractPayload, HonestAddressPayload, InfringingTokenRecord,
    MaliciousAddressPayload, NftPropagationPathPayload, ReportSummary,
    SecondarySaleVictimAddressPayload, SeedCollectionStatsPayload, SeedNft, SingleReportPayload,
    ValueFlowEdgePayload, VictimAcquisitionAddressPayload,
};
use crate::normalize::{normalize_name, normalize_symbol, normalize_url};

use super::{enrich_duplicate_contract_payload_with_metadata, BatchSeedAggregate};

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

#[derive(Clone, Debug, Default)]
pub(super) struct AcquisitionCostStats {
    paid_mint_victim_cost_eth: f64,
    paid_mint_victim_cost_usd: f64,
    paid_mint_victim_edge_count: i64,
    paid_mint_victim_address_count: i64,
    paid_mint_stuck_cost_eth: f64,
    paid_mint_stuck_cost_usd: f64,
    paid_mint_stuck_edge_count: i64,
    paid_mint_stuck_token_count: i64,
    stablecoin_erc20_value_usd: f64,
    stablecoin_erc20_edge_count: i64,
    value_flow_priced_edge_count: i64,
    value_flow_unpriced_edge_count: i64,
}

pub(super) fn build_acquisition_cost_stats(
    address_attributions: &[AddressAttributionPayload],
    value_flow_edges: &[ValueFlowEdgePayload],
    propagation_paths: &BTreeMap<String, NftPropagationPathPayload>,
) -> AcquisitionCostStats {
    let paid_mint_victim_addresses = paid_mint_victim_address_set(address_attributions);
    let mut stats = AcquisitionCostStats {
        paid_mint_victim_address_count: paid_mint_victim_addresses.len() as i64,
        ..AcquisitionCostStats::default()
    };

    for edge in value_flow_edges {
        if edge.value_usd.unwrap_or_default() > 0.0 {
            stats.value_flow_priced_edge_count += 1;
        } else if edge.value_eth.unwrap_or_default() > 0.0 {
            stats.value_flow_unpriced_edge_count += 1;
        }
        if is_stablecoin_symbol(&edge.payment_token_symbol)
            && edge.value_usd.unwrap_or_default() > 0.0
        {
            stats.stablecoin_erc20_edge_count += 1;
            stats.stablecoin_erc20_value_usd += edge.value_usd.unwrap_or_default();
        }
        if edge.channel != "mint_payment" {
            continue;
        }
        let payer = normalized_address(&edge.from_address);
        if payer.is_empty() || !paid_mint_victim_addresses.contains(&payer) {
            continue;
        }
        stats.paid_mint_victim_edge_count += 1;
        stats.paid_mint_victim_cost_eth += edge.value_eth.unwrap_or_default();
        stats.paid_mint_victim_cost_usd += edge.value_usd.unwrap_or_default();

        let (stuck_token_count, total_token_count) =
            paid_mint_stuck_token_counts(edge, propagation_paths);
        if stuck_token_count > 0 && total_token_count > 0 {
            let stuck_fraction = stuck_token_count as f64 / total_token_count as f64;
            stats.paid_mint_stuck_edge_count += 1;
            stats.paid_mint_stuck_token_count += stuck_token_count as i64;
            stats.paid_mint_stuck_cost_eth += edge.value_eth.unwrap_or_default() * stuck_fraction;
            stats.paid_mint_stuck_cost_usd += edge.value_usd.unwrap_or_default() * stuck_fraction;
        }
    }

    stats
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

pub(super) fn build_victim_acquisition_addresses(
    secondary_sale_victim_addresses: &[SecondarySaleVictimAddressPayload],
    address_attributions: &[AddressAttributionPayload],
    value_flow_edges: &[ValueFlowEdgePayload],
    propagation_paths: &BTreeMap<String, NftPropagationPathPayload>,
) -> Vec<VictimAcquisitionAddressPayload> {
    let paid_mint_victim_addresses = paid_mint_victim_address_set(address_attributions);
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
        if address.is_empty() {
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
        if row.buy_before_eth_balance.is_none() {
            row.buy_before_eth_balance = victim.buy_before_eth_balance;
        }
        if row.buy_before_usd_balance.is_none() {
            row.buy_before_usd_balance = victim.buy_before_usd_balance;
        }
        if row.buy_asset_ratio.is_none() {
            row.buy_asset_ratio = victim.buy_asset_ratio;
        }
        if row.buy_asset_ratio_with_gas.is_none() {
            row.buy_asset_ratio_with_gas = victim.buy_asset_ratio_with_gas;
        }
        let gas_extra = ratio_gas_extra(
            victim.buy_asset_ratio,
            victim.buy_asset_ratio_with_gas,
            victim.buy_before_usd_balance,
            victim.buy_before_eth_balance,
        );
        if gas_extra.0 > 0.0 || gas_extra.1 > 0.0 {
            let entry = gas_extra_by_address.entry(address).or_default();
            entry.0 += gas_extra.0;
            entry.1 += gas_extra.1;
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
        let value_with_gas_eth = edge
            .value_with_gas_eth
            .unwrap_or_else(|| edge.value_eth.unwrap_or_default());
        let value_with_gas_usd = edge
            .value_with_gas_usd
            .unwrap_or_else(|| edge.value_usd.unwrap_or_default());
        let gas_extra_eth = (value_with_gas_eth - edge.value_eth.unwrap_or_default()).max(0.0);
        let gas_extra_usd = (value_with_gas_usd - edge.value_usd.unwrap_or_default()).max(0.0);
        if gas_extra_eth > 0.0 || gas_extra_usd > 0.0 {
            let entry = gas_extra_by_address
                .entry(row.address.to_lowercase())
                .or_default();
            entry.0 += gas_extra_eth;
            entry.1 += gas_extra_usd;
        }
        row.paid_mint_edge_count += 1;
        let (stuck_token_count, total_token_count) =
            paid_mint_stuck_token_counts(edge, propagation_paths);
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

pub(super) fn ratio_gas_extra(
    ratio_without_gas: Option<f64>,
    ratio_with_gas: Option<f64>,
    before_usd_balance: Option<f64>,
    before_eth_balance: Option<f64>,
) -> (f64, f64) {
    let Some(delta_ratio) = ratio_with_gas
        .zip(ratio_without_gas)
        .map(|(with_gas, without_gas)| with_gas - without_gas)
        .filter(|value| *value > 0.0)
    else {
        return (0.0, 0.0);
    };
    if let Some(balance) = before_usd_balance.filter(|value| *value > 0.0) {
        return (0.0, delta_ratio * balance);
    }
    if let Some(balance) = before_eth_balance.filter(|value| *value > 0.0) {
        return (delta_ratio * balance, 0.0);
    }
    (0.0, 0.0)
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

pub(super) fn is_neutral_attribution_label(label: &str) -> bool {
    label == "neutral_participant"
}

pub(super) fn normalized_address(address: &str) -> String {
    address.trim().to_lowercase()
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
            path.contract_address
                .eq_ignore_ascii_case(&edge.contract_address)
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

pub(super) fn is_stablecoin_symbol(symbol: &str) -> bool {
    matches!(
        symbol.trim().to_ascii_uppercase().as_str(),
        "USDC" | "USDT" | "DAI" | "USDS" | "PYUSD" | "FRAX" | "LUSD" | "TUSD"
    )
}

pub(super) struct ReportSummaryInput<'a> {
    pub(super) open_license: bool,
    pub(super) grouped: &'a BTreeMap<String, Vec<usize>>,
    pub(super) implausible_candidate_contract_count: i64,
    pub(super) legit_duplicates: &'a [DuplicateContractPayload],
    pub(super) infringing_tokens: &'a [InfringingTokenRecord],
    pub(super) malicious_addresses: &'a [MaliciousAddressPayload],
    pub(super) honest_addresses: &'a [HonestAddressPayload],
    pub(super) secondary_sale_victim_addresses: &'a [SecondarySaleVictimAddressPayload],
    pub(super) victim_acquisition_addresses: &'a [VictimAcquisitionAddressPayload],
    pub(super) address_signals: &'a BTreeMap<String, AddressSignalPayload>,
    pub(super) address_attributions: &'a [AddressAttributionPayload],
    pub(super) value_flow_edges: &'a [ValueFlowEdgePayload],
    pub(super) propagation_paths: &'a BTreeMap<String, NftPropagationPathPayload>,
    pub(super) lifecycle_metrics: &'a [ContractLifecycleMetricPayload],
}

pub(super) fn build_report_summary(input: ReportSummaryInput<'_>) -> ReportSummary {
    let ReportSummaryInput {
        open_license,
        grouped,
        implausible_candidate_contract_count,
        legit_duplicates,
        infringing_tokens,
        malicious_addresses,
        honest_addresses,
        secondary_sale_victim_addresses,
        victim_acquisition_addresses,
        address_signals,
        address_attributions,
        value_flow_edges,
        propagation_paths,
        lifecycle_metrics,
    } = input;
    let infringing_nft_count = infringing_tokens
        .iter()
        .map(|item| (item.contract_address.clone(), item.token_id.clone()))
        .collect::<BTreeSet<_>>()
        .len() as i64;
    let malicious_address_count = malicious_addresses
        .iter()
        .map(|item| item.address.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect::<BTreeSet<_>>()
        .len() as i64;
    let neutral_address_count = address_attributions
        .iter()
        .filter(|item| is_neutral_attribution_label(&item.attribution_label))
        .map(|item| normalized_address(&item.address))
        .filter(|value| !value.is_empty())
        .collect::<BTreeSet<_>>()
        .len() as i64;
    let repeat_infringing_address_count = infringing_tokens
        .iter()
        .filter(|item| !item.minter_address.is_empty() && !item.contract_address.is_empty())
        .fold(
            BTreeMap::<String, BTreeSet<String>>::new(),
            |mut acc, item| {
                acc.entry(item.minter_address.clone())
                    .or_default()
                    .insert(item.contract_address.clone());
                acc
            },
        )
        .values()
        .filter(|contracts| contracts.len() > 1)
        .count() as i64;
    let candidate_open_license_tokens: Vec<&InfringingTokenRecord> = infringing_tokens
        .iter()
        .filter(|item| item.candidate_open_license)
        .collect();
    let candidate_open_license_contract_count = candidate_open_license_tokens
        .iter()
        .map(|item| item.contract_address.clone())
        .collect::<BTreeSet<_>>()
        .len() as i64;
    let secondary_sale_victim_cost_eth = secondary_sale_victim_addresses
        .iter()
        .map(|item| item.buy_amount_eth)
        .sum::<f64>();
    let secondary_sale_victim_cost_usd = secondary_sale_victim_addresses
        .iter()
        .map(|item| item.buy_amount_usd)
        .sum::<f64>();
    let secondary_sale_stuck_cost_eth = secondary_sale_victim_addresses
        .iter()
        .filter(|item| item.is_stuck)
        .map(|item| item.last_buy_amount_eth.unwrap_or(0.0))
        .sum::<f64>();
    let secondary_sale_stuck_cost_usd = secondary_sale_victim_addresses
        .iter()
        .filter(|item| item.is_stuck)
        .map(|item| item.last_buy_amount_usd.unwrap_or(0.0))
        .sum::<f64>();
    let buy_ratio_values: Vec<f64> = victim_acquisition_addresses
        .iter()
        .filter_map(|item| item.buy_asset_ratio)
        .collect();
    let ratio_known_count = buy_ratio_values.len() as i64;
    let ratio_over_60_count = buy_ratio_values
        .iter()
        .filter(|value| **value > 0.6)
        .count() as i64;
    let ratio_over_80_count = buy_ratio_values
        .iter()
        .filter(|value| **value > 0.8)
        .count() as i64;
    let stuck_victim_address_count = victim_acquisition_addresses
        .iter()
        .filter(|item| item.is_stuck)
        .count() as i64;
    let corrupted_victim_address_count = honest_addresses
        .iter()
        .filter(|item| item.is_corrupted_address)
        .count() as i64;
    let corrupted_holding_values: Vec<f64> = honest_addresses
        .iter()
        .filter(|item| item.is_corrupted_address)
        .filter_map(|item| item.hold_duration_median_seconds)
        .collect();
    let deployment_to_neutral_holder_samples: Vec<f64> = honest_addresses
        .iter()
        .flat_map(|item| {
            item.deployment_to_neutral_holder_seconds_samples
                .iter()
                .filter_map(|sample| positive_seconds(*sample))
        })
        .collect();
    let deployment_to_first_transfer_values: Vec<f64> = lifecycle_metrics
        .iter()
        .filter_map(|metric| metric.time_to_first_transfer_seconds)
        .filter_map(positive_seconds)
        .collect();
    let unique_receiver_values: Vec<f64> = address_signals
        .values()
        .map(|signal| signal.unique_receiver_count as f64)
        .collect();
    let acquisition_stats =
        build_acquisition_cost_stats(address_attributions, value_flow_edges, propagation_paths);
    let secondary_sale_stuck_cost_ratio = if secondary_sale_victim_cost_usd > 0.0 {
        Some(secondary_sale_stuck_cost_usd / secondary_sale_victim_cost_usd)
    } else if secondary_sale_victim_cost_eth > 0.0 {
        Some(secondary_sale_stuck_cost_eth / secondary_sale_victim_cost_eth)
    } else {
        None
    };
    let victim_acquisition_total_eth =
        secondary_sale_victim_cost_eth + acquisition_stats.paid_mint_victim_cost_eth;
    let victim_acquisition_total_usd =
        secondary_sale_victim_cost_usd + acquisition_stats.paid_mint_victim_cost_usd;
    let victim_acquisition_stuck_cost_eth =
        secondary_sale_stuck_cost_eth + acquisition_stats.paid_mint_stuck_cost_eth;
    let victim_acquisition_stuck_cost_usd =
        secondary_sale_stuck_cost_usd + acquisition_stats.paid_mint_stuck_cost_usd;
    let victim_acquisition_stuck_cost_ratio = if victim_acquisition_total_usd > 0.0 {
        Some(victim_acquisition_stuck_cost_usd / victim_acquisition_total_usd)
    } else if victim_acquisition_total_eth > 0.0 {
        Some(victim_acquisition_stuck_cost_eth / victim_acquisition_total_eth)
    } else {
        None
    };

    ReportSummary {
        open_license_detected: open_license,
        candidate_contract_count: grouped.len() as i64,
        implausible_candidate_contract_count,
        infringing_nft_count,
        malicious_address_count,
        neutral_address_count,
        repeat_infringing_address_count,
        legit_duplicate_contract_count: legit_duplicates.len() as i64,
        candidate_open_license_token_count: candidate_open_license_tokens.len() as i64,
        candidate_open_license_contract_count,
        secondary_sale_victim_cost_eth,
        secondary_sale_victim_cost_usd,
        secondary_sale_victim_address_count: secondary_sale_victim_addresses.len() as i64,
        secondary_sale_stuck_cost_eth,
        secondary_sale_stuck_cost_usd,
        secondary_sale_stuck_cost_ratio,
        paid_mint_victim_cost_eth: acquisition_stats.paid_mint_victim_cost_eth,
        paid_mint_victim_cost_usd: acquisition_stats.paid_mint_victim_cost_usd,
        paid_mint_victim_edge_count: acquisition_stats.paid_mint_victim_edge_count,
        paid_mint_victim_address_count: acquisition_stats.paid_mint_victim_address_count,
        paid_mint_stuck_cost_eth: acquisition_stats.paid_mint_stuck_cost_eth,
        paid_mint_stuck_cost_usd: acquisition_stats.paid_mint_stuck_cost_usd,
        paid_mint_stuck_edge_count: acquisition_stats.paid_mint_stuck_edge_count,
        paid_mint_stuck_token_count: acquisition_stats.paid_mint_stuck_token_count,
        victim_acquisition_total_eth,
        victim_acquisition_total_usd,
        victim_acquisition_stuck_cost_eth,
        victim_acquisition_stuck_cost_usd,
        victim_acquisition_stuck_cost_ratio,
        victim_acquisition_address_count: victim_acquisition_addresses.len() as i64,
        stablecoin_erc20_value_usd: acquisition_stats.stablecoin_erc20_value_usd,
        stablecoin_erc20_edge_count: acquisition_stats.stablecoin_erc20_edge_count,
        value_flow_priced_edge_count: acquisition_stats.value_flow_priced_edge_count,
        value_flow_unpriced_edge_count: acquisition_stats.value_flow_unpriced_edge_count,
        buy_asset_ratio_known_address_count: ratio_known_count,
        ratio_over_60_address_count: ratio_over_60_count,
        ratio_over_60_address_ratio: if ratio_known_count > 0 {
            Some(ratio_over_60_count as f64 / ratio_known_count as f64)
        } else {
            None
        },
        ratio_over_80_address_count: ratio_over_80_count,
        ratio_over_80_address_ratio: if ratio_known_count > 0 {
            Some(ratio_over_80_count as f64 / ratio_known_count as f64)
        } else {
            None
        },
        stuck_victim_address_count,
        stuck_victim_address_ratio: if !victim_acquisition_addresses.is_empty() {
            Some(stuck_victim_address_count as f64 / victim_acquisition_addresses.len() as f64)
        } else {
            None
        },
        corrupted_victim_address_count,
        avg_corrupted_address_holding_seconds: mean_f64(&corrupted_holding_values),
        median_corrupted_address_holding_seconds: median_f64(&corrupted_holding_values),
        avg_deployment_to_neutral_holder_seconds: mean_f64(&deployment_to_neutral_holder_samples),
        median_deployment_to_neutral_holder_seconds: median_f64(
            &deployment_to_neutral_holder_samples,
        ),
        avg_deployment_to_first_transfer_seconds: mean_f64(&deployment_to_first_transfer_values),
        median_deployment_to_first_transfer_seconds: median_f64(
            &deployment_to_first_transfer_values,
        ),
        avg_unique_receiver_count: mean_f64(&unique_receiver_values),
    }
}

pub(super) fn mean_f64(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        None
    } else {
        Some(values.iter().sum::<f64>() / values.len() as f64)
    }
}

pub(super) fn median_f64(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|left, right| left.total_cmp(right));
    let mid = sorted.len() / 2;
    if sorted.len() % 2 == 1 {
        Some(sorted[mid])
    } else {
        Some((sorted[mid - 1] + sorted[mid]) / 2.0)
    }
}

pub(super) fn build_batch_report_summary(
    seed_reports: &[BatchSeedAggregate],
) -> BatchReportSummary {
    let distinct_chains: BTreeSet<String> = seed_reports
        .iter()
        .map(|item| item.report.seed_contract.chain.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect();
    let distinct_chains: Vec<String> = distinct_chains.into_iter().collect();
    let malicious_addresses: BTreeSet<String> = seed_reports
        .iter()
        .flat_map(|item| item.malicious_addresses.iter().cloned())
        .collect();
    let neutral_addresses: BTreeSet<String> = seed_reports
        .iter()
        .flat_map(|item| item.neutral_addresses.iter().cloned())
        .collect();
    let victim_acquisition_addresses: BTreeSet<String> = seed_reports
        .iter()
        .flat_map(|item| item.victim_acquisition_addresses.iter().cloned())
        .collect();
    let stuck_victim_addresses: BTreeSet<String> = seed_reports
        .iter()
        .flat_map(|item| item.stuck_victim_addresses.iter().cloned())
        .collect();
    let corrupted_victim_addresses: BTreeSet<String> = seed_reports
        .iter()
        .flat_map(|item| item.corrupted_victim_addresses.iter().cloned())
        .collect();
    let mut minter_infringing_contracts: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for seed_report in seed_reports {
        for (minter, contracts) in &seed_report.minter_infringing_contracts {
            minter_infringing_contracts
                .entry(minter.clone())
                .or_default()
                .extend(contracts.iter().cloned());
        }
    }
    let secondary_sale_victim_cost_eth_total: f64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.secondary_sale_victim_cost_eth)
        .sum();
    let secondary_sale_victim_cost_usd_total: f64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.secondary_sale_victim_cost_usd)
        .sum();
    let secondary_sale_victim_address_count_total: i64 = seed_reports
        .iter()
        .map(|item| {
            item.report
                .report_summary
                .secondary_sale_victim_address_count
        })
        .sum();
    let secondary_sale_stuck_cost_eth_total: f64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.secondary_sale_stuck_cost_eth)
        .sum();
    let secondary_sale_stuck_cost_usd_total: f64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.secondary_sale_stuck_cost_usd)
        .sum();
    let paid_mint_victim_cost_eth_total: f64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.paid_mint_victim_cost_eth)
        .sum();
    let paid_mint_victim_cost_usd_total: f64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.paid_mint_victim_cost_usd)
        .sum();
    let paid_mint_victim_edge_count_total: i64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.paid_mint_victim_edge_count)
        .sum();
    let paid_mint_victim_address_count_total: i64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.paid_mint_victim_address_count)
        .sum();
    let paid_mint_stuck_cost_eth_total: f64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.paid_mint_stuck_cost_eth)
        .sum();
    let paid_mint_stuck_cost_usd_total: f64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.paid_mint_stuck_cost_usd)
        .sum();
    let paid_mint_stuck_edge_count_total: i64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.paid_mint_stuck_edge_count)
        .sum();
    let paid_mint_stuck_token_count_total: i64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.paid_mint_stuck_token_count)
        .sum();
    let victim_acquisition_total_eth_total: f64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.victim_acquisition_total_eth)
        .sum();
    let victim_acquisition_total_usd_total: f64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.victim_acquisition_total_usd)
        .sum();
    let victim_acquisition_stuck_cost_eth_total: f64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.victim_acquisition_stuck_cost_eth)
        .sum();
    let victim_acquisition_stuck_cost_usd_total: f64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.victim_acquisition_stuck_cost_usd)
        .sum();
    let victim_acquisition_address_count_total: i64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.victim_acquisition_address_count)
        .sum();
    let stablecoin_erc20_value_usd_total: f64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.stablecoin_erc20_value_usd)
        .sum();
    let stablecoin_erc20_edge_count_total: i64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.stablecoin_erc20_edge_count)
        .sum();
    let value_flow_priced_edge_count_total: i64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.value_flow_priced_edge_count)
        .sum();
    let value_flow_unpriced_edge_count_total: i64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.value_flow_unpriced_edge_count)
        .sum();
    let buy_asset_ratio_known_address_count_total: i64 = seed_reports
        .iter()
        .map(|item| {
            item.report
                .report_summary
                .buy_asset_ratio_known_address_count
        })
        .sum();
    let ratio_over_60_address_count_total: i64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.ratio_over_60_address_count)
        .sum();
    let ratio_over_80_address_count_total: i64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.ratio_over_80_address_count)
        .sum();
    let stuck_victim_address_count_total: i64 = seed_reports
        .iter()
        .map(|item| item.report.report_summary.stuck_victim_address_count)
        .sum();
    let mean_corrupted_holding_values: Vec<f64> = seed_reports
        .iter()
        .filter_map(|item| {
            item.report
                .report_summary
                .avg_corrupted_address_holding_seconds
        })
        .collect();
    let median_corrupted_holding_values: Vec<f64> = seed_reports
        .iter()
        .filter_map(|item| {
            item.report
                .report_summary
                .median_corrupted_address_holding_seconds
        })
        .collect();
    let mean_neutral_holder_values: Vec<f64> = seed_reports
        .iter()
        .filter_map(|item| {
            item.report
                .report_summary
                .avg_deployment_to_neutral_holder_seconds
        })
        .collect();
    let median_neutral_holder_values: Vec<f64> = seed_reports
        .iter()
        .filter_map(|item| {
            item.report
                .report_summary
                .median_deployment_to_neutral_holder_seconds
        })
        .collect();
    let mean_first_transfer_values: Vec<f64> = seed_reports
        .iter()
        .filter_map(|item| {
            item.report
                .report_summary
                .avg_deployment_to_first_transfer_seconds
        })
        .filter(|value| *value > 0.0)
        .collect();
    let median_first_transfer_values: Vec<f64> = seed_reports
        .iter()
        .filter_map(|item| {
            item.report
                .report_summary
                .median_deployment_to_first_transfer_seconds
        })
        .filter(|value| *value > 0.0)
        .collect();
    let mean_unique_receiver_values: Vec<f64> = seed_reports
        .iter()
        .filter_map(|item| item.report.report_summary.avg_unique_receiver_count)
        .collect();
    BatchReportSummary {
        seed_report_count: seed_reports.len() as i64,
        chain: if distinct_chains.len() == 1 {
            distinct_chains[0].clone()
        } else {
            String::new()
        },
        chains: distinct_chains,
        open_license_detected_count: seed_reports
            .iter()
            .filter(|item| item.report.report_summary.open_license_detected)
            .count() as i64,
        candidate_contract_count_total: seed_reports
            .iter()
            .map(|item| item.report.report_summary.candidate_contract_count)
            .sum(),
        implausible_candidate_contract_count_total: seed_reports
            .iter()
            .map(|item| {
                item.report
                    .report_summary
                    .implausible_candidate_contract_count
            })
            .sum(),
        infringing_nft_count_total: seed_reports
            .iter()
            .map(|item| item.report.report_summary.infringing_nft_count)
            .sum(),
        malicious_address_count_total: malicious_addresses.len() as i64,
        neutral_address_count_total: neutral_addresses.len() as i64,
        repeat_infringing_address_count_total: seed_reports
            .iter()
            .map(|item| item.report.report_summary.repeat_infringing_address_count)
            .sum(),
        repeat_infringing_address_count_global: minter_infringing_contracts
            .values()
            .filter(|contracts| contracts.len() > 1)
            .count() as i64,
        legit_duplicate_contract_count_total: seed_reports
            .iter()
            .map(|item| item.report.report_summary.legit_duplicate_contract_count)
            .sum(),
        secondary_sale_victim_cost_eth_total,
        secondary_sale_victim_cost_usd_total,
        secondary_sale_victim_address_count_total,
        secondary_sale_stuck_cost_eth_total,
        secondary_sale_stuck_cost_usd_total,
        secondary_sale_stuck_cost_ratio_overall: if secondary_sale_victim_cost_usd_total > 0.0 {
            Some(secondary_sale_stuck_cost_usd_total / secondary_sale_victim_cost_usd_total)
        } else if secondary_sale_victim_cost_eth_total > 0.0 {
            Some(secondary_sale_stuck_cost_eth_total / secondary_sale_victim_cost_eth_total)
        } else {
            None
        },
        paid_mint_victim_cost_eth_total,
        paid_mint_victim_cost_usd_total,
        paid_mint_victim_edge_count_total,
        paid_mint_victim_address_count_total,
        paid_mint_stuck_cost_eth_total,
        paid_mint_stuck_cost_usd_total,
        paid_mint_stuck_edge_count_total,
        paid_mint_stuck_token_count_total,
        victim_acquisition_total_eth_total,
        victim_acquisition_total_usd_total,
        victim_acquisition_stuck_cost_eth_total,
        victim_acquisition_stuck_cost_usd_total,
        victim_acquisition_stuck_cost_ratio_overall: if victim_acquisition_total_usd_total > 0.0 {
            Some(victim_acquisition_stuck_cost_usd_total / victim_acquisition_total_usd_total)
        } else if victim_acquisition_total_eth_total > 0.0 {
            Some(victim_acquisition_stuck_cost_eth_total / victim_acquisition_total_eth_total)
        } else {
            None
        },
        victim_acquisition_address_count_total,
        victim_acquisition_address_count_distinct: victim_acquisition_addresses.len() as i64,
        stablecoin_erc20_value_usd_total,
        stablecoin_erc20_edge_count_total,
        value_flow_priced_edge_count_total,
        value_flow_unpriced_edge_count_total,
        buy_asset_ratio_known_address_count_total,
        ratio_over_60_address_count_total,
        ratio_over_60_address_ratio_overall: if buy_asset_ratio_known_address_count_total > 0 {
            Some(
                ratio_over_60_address_count_total as f64
                    / buy_asset_ratio_known_address_count_total as f64,
            )
        } else {
            None
        },
        ratio_over_80_address_count_total,
        ratio_over_80_address_ratio_overall: if buy_asset_ratio_known_address_count_total > 0 {
            Some(
                ratio_over_80_address_count_total as f64
                    / buy_asset_ratio_known_address_count_total as f64,
            )
        } else {
            None
        },
        stuck_victim_address_count_total,
        stuck_victim_address_ratio_overall: if victim_acquisition_address_count_total > 0 {
            Some(
                stuck_victim_address_count_total as f64
                    / victim_acquisition_address_count_total as f64,
            )
        } else {
            None
        },
        stuck_victim_address_count_distinct: stuck_victim_addresses.len() as i64,
        stuck_victim_address_ratio_distinct: if !victim_acquisition_addresses.is_empty() {
            Some(stuck_victim_addresses.len() as f64 / victim_acquisition_addresses.len() as f64)
        } else {
            None
        },
        corrupted_victim_address_count_total: seed_reports
            .iter()
            .map(|item| item.report.report_summary.corrupted_victim_address_count)
            .sum(),
        corrupted_victim_address_count_distinct: corrupted_victim_addresses.len() as i64,
        avg_corrupted_address_holding_seconds_mean: mean(&mean_corrupted_holding_values),
        median_corrupted_address_holding_seconds_median: median_f64(
            &median_corrupted_holding_values,
        ),
        avg_deployment_to_neutral_holder_seconds_mean: mean(&mean_neutral_holder_values),
        median_deployment_to_neutral_holder_seconds_median: median_f64(
            &median_neutral_holder_values,
        ),
        avg_deployment_to_first_transfer_seconds_mean: mean(&mean_first_transfer_values),
        median_deployment_to_first_transfer_seconds_median: median_f64(
            &median_first_transfer_values,
        ),
        avg_unique_receiver_count_mean: mean(&mean_unique_receiver_values),
        generated_at: chrono::Utc::now().to_rfc3339(),
    }
}

pub(super) fn build_batch_seed_aggregate(payload: SingleReportPayload) -> BatchSeedAggregate {
    let malicious_addresses: BTreeSet<String> = payload
        .malicious_addresses
        .iter()
        .map(|item| item.address.trim().to_lowercase())
        .filter(|value| !value.is_empty())
        .collect();
    let neutral_addresses: BTreeSet<String> = payload
        .address_attributions
        .iter()
        .filter(|item| is_neutral_attribution_label(&item.attribution_label))
        .map(|item| normalized_address(&item.address))
        .filter(|value| !value.is_empty())
        .collect();
    let victim_acquisition_addresses: BTreeSet<String> = payload
        .victim_acquisition_addresses
        .iter()
        .map(|item| normalized_address(&item.address))
        .filter(|value| !value.is_empty())
        .collect();
    let stuck_victim_addresses: BTreeSet<String> = payload
        .victim_acquisition_addresses
        .iter()
        .filter(|item| item.is_stuck)
        .map(|item| normalized_address(&item.address))
        .filter(|value| !value.is_empty())
        .collect();
    let corrupted_victim_addresses: BTreeSet<String> = payload
        .honest_addresses
        .iter()
        .filter(|item| item.is_corrupted_address)
        .map(|item| normalized_address(&item.address))
        .filter(|value| !value.is_empty())
        .collect();
    let minter_infringing_contracts = payload_minter_contracts(&payload.infringing_tokens);
    let report_summary = payload.report_summary.clone();

    BatchSeedAggregate {
        report: BatchSeedReportPayload {
            seed_contract: payload.seed_contract,
            report_summary,
            output_files: None,
        },
        malicious_addresses,
        neutral_addresses,
        victim_acquisition_addresses,
        stuck_victim_addresses,
        corrupted_victim_addresses,
        minter_infringing_contracts,
    }
}

pub(super) fn payload_minter_contracts(
    infringing_tokens: &[InfringingTokenRecord],
) -> BTreeMap<String, BTreeSet<String>> {
    let mut minter_contracts: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for token in infringing_tokens {
        let minter = token.minter_address.trim().to_lowercase();
        let contract = token.contract_address.trim().to_lowercase();
        if minter.is_empty() || contract.is_empty() {
            continue;
        }
        minter_contracts.entry(minter).or_default().insert(contract);
    }
    minter_contracts
}

pub(super) fn positive_seconds(value: i64) -> Option<f64> {
    (value > 0).then_some(value as f64)
}

pub(super) fn mean(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        None
    } else {
        Some(values.iter().sum::<f64>() / values.len() as f64)
    }
}
