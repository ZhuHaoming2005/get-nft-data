use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::models::{
    AddressAttributionPayload, AddressEvidencePayload, DuplicateCandidate, FraudTradeStatsPayload,
    HonestAddressPayload, HonestAddressStatsPayload, InfringingTokenRecord,
    MaliciousAddressPayload, NftSaleRecord, OwnerBalance, SecondarySaleVictimAddressPayload,
    TransferRecord, ValueFlowEdgePayload, VictimAcquisitionAddressPayload, ZERO_ADDRESS,
};

#[derive(Clone, Debug, Default, PartialEq)]
pub struct SaleMetricRecord {
    pub buy_before_eth_balance: Option<f64>,
    pub buy_before_usd_balance: Option<f64>,
    pub buy_asset_ratio: Option<f64>,
    pub buy_asset_ratio_with_gas: Option<f64>,
    pub ratio_status: String,
}

pub(crate) fn sale_metric_key(tx_hash: &str, buyer_address: &str) -> String {
    format!(
        "{}|{}",
        tx_hash.trim().to_lowercase(),
        buyer_address.trim().to_lowercase()
    )
}

pub(crate) struct PreparedContractActivity<'a> {
    owner_token_map: HashMap<String, HashSet<String>>,
    sorted_transfers: Vec<&'a TransferRecord>,
    sorted_sales: Vec<&'a NftSaleRecord>,
    latest_outgoing: HashMap<(String, String), (i64, i64, String)>,
}

fn transfer_sort_key(transfer: &TransferRecord) -> (i64, i64, &str) {
    (
        transfer.block_number,
        transfer.log_index,
        transfer.tx_hash.as_str(),
    )
}

fn sale_sort_key(sale: &NftSaleRecord) -> (i64, i64, i64, &str) {
    (
        sale.block_number,
        sale.log_index,
        sale.bundle_index,
        sale.tx_hash.as_str(),
    )
}

fn median_i64(values: &[i64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let mid = sorted.len() / 2;
    if sorted.len() % 2 == 1 {
        Some(sorted[mid] as f64)
    } else {
        Some((sorted[mid - 1] as f64 + sorted[mid] as f64) / 2.0)
    }
}

fn median_f64(values: &[f64]) -> Option<f64> {
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

fn mean_f64(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        None
    } else {
        Some(values.iter().sum::<f64>() / values.len() as f64)
    }
}

fn sale_usd_value(sale: &NftSaleRecord) -> Option<f64> {
    sale.price_usd
}

fn sale_has_positive_value(sale: &NftSaleRecord) -> bool {
    sale.price_eth.unwrap_or_default() > 0.0 || sale.price_usd.unwrap_or_default() > 0.0
}

fn value_flow_has_positive_value(edge: &ValueFlowEdgePayload) -> bool {
    edge.value_eth.unwrap_or_default() > 0.0 || edge.value_usd.unwrap_or_default() > 0.0
}

const WASH_CYCLE_L2_PROPAGATION_THRESHOLD: i64 = 3;
const WASH_CYCLE_L2_VALUE_USD_THRESHOLD: f64 = 1_000.0;
const WASH_CYCLE_L2_VALUE_ETH_THRESHOLD: f64 = 0.5;
const STAR_DISTRIBUTION_L1_THRESHOLD: i64 = 3;
const STAR_DISTRIBUTION_L2_THRESHOLD: i64 = 5;
const HIGH_VOLUME_SELLER_L1_THRESHOLD: i64 = 3;
const HIGH_VOLUME_SELLER_L2_THRESHOLD: i64 = 5;
const RAPID_SPREAD_L2_TOKEN_THRESHOLD: usize = 2;

struct OperatorLevelSignals {
    wash_cycle_count: i64,
    wash_cycle_value_eth: f64,
    wash_cycle_value_usd: f64,
    wash_cycle_has_usd: bool,
    star_out_degree: i64,
    sale_seller_count: i64,
    rapid_spread_token_count: usize,
    value_extraction_observed: bool,
}

#[derive(Clone, Debug, Default)]
struct WashCycleScope {
    token_ids: HashSet<String>,
    tx_hashes: HashSet<String>,
    participants: HashSet<String>,
}

#[derive(Clone, Debug)]
struct WashCycleTransferScope {
    token_id: String,
    tx_hash: String,
    from_address: String,
    to_address: String,
}

impl WashCycleTransferScope {
    fn from_transfer(transfer: &TransferRecord) -> Self {
        Self {
            token_id: transfer.token_id.trim().to_lowercase(),
            tx_hash: transfer.tx_hash.trim().to_lowercase(),
            from_address: transfer.from_address.trim().to_lowercase(),
            to_address: transfer.to_address.trim().to_lowercase(),
        }
    }
}

fn add_wash_cycle_transfer_scope(
    scopes: &mut HashMap<String, WashCycleScope>,
    address: &str,
    transfer: &WashCycleTransferScope,
) {
    let scope = scopes.entry(address.to_string()).or_default();
    if !transfer.token_id.is_empty() {
        scope.token_ids.insert(transfer.token_id.clone());
    }
    if !transfer.tx_hash.is_empty() {
        scope.tx_hashes.insert(transfer.tx_hash.clone());
    }
    if !transfer.from_address.is_empty() {
        scope.participants.insert(transfer.from_address.clone());
    }
    if !transfer.to_address.is_empty() {
        scope.participants.insert(transfer.to_address.clone());
    }
}

fn operator_level_label(level: i64) -> &'static str {
    match level {
        1 => "weak_behavioral_operator",
        2 => "likely_behavioral_operator",
        3 => "strong_value_control_operator",
        _ => "",
    }
}

fn classify_operator_level(signals: &OperatorLevelSignals) -> i64 {
    if signals.value_extraction_observed {
        return 3;
    }

    let has_wash_cycle = signals.wash_cycle_count > 0;
    let has_star_distribution = signals.star_out_degree >= STAR_DISTRIBUTION_L1_THRESHOLD;
    let has_high_volume_selling = signals.sale_seller_count >= HIGH_VOLUME_SELLER_L1_THRESHOLD;
    let has_rapid_spread = signals.rapid_spread_token_count > 0;
    let has_independent_rapid_spread = has_rapid_spread && !has_wash_cycle;
    let behavior_family_count = [
        has_wash_cycle,
        has_star_distribution,
        has_high_volume_selling,
        has_independent_rapid_spread,
    ]
    .into_iter()
    .filter(|value| *value)
    .count();

    if behavior_family_count == 0 {
        return 0;
    }

    let wash_value_reaches_l2 = if signals.wash_cycle_has_usd {
        signals.wash_cycle_value_usd >= WASH_CYCLE_L2_VALUE_USD_THRESHOLD
    } else {
        signals.wash_cycle_value_eth >= WASH_CYCLE_L2_VALUE_ETH_THRESHOLD
    };
    let severe_behavior = signals.wash_cycle_count >= WASH_CYCLE_L2_PROPAGATION_THRESHOLD
        || wash_value_reaches_l2
        || signals.star_out_degree >= STAR_DISTRIBUTION_L2_THRESHOLD
        || signals.sale_seller_count >= HIGH_VOLUME_SELLER_L2_THRESHOLD
        || signals.rapid_spread_token_count >= RAPID_SPREAD_L2_TOKEN_THRESHOLD
        || behavior_family_count >= 2;

    if severe_behavior {
        2
    } else {
        1
    }
}

fn value_flow_token_ids(edge: &ValueFlowEdgePayload) -> Vec<String> {
    edge.token_id
        .split(',')
        .map(str::trim)
        .filter(|token_id| !token_id.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn value_flow_precedes_sale(edge: &ValueFlowEdgePayload, sale: &NftSaleRecord) -> bool {
    if edge.block_number > 0 && sale.block_number > 0 {
        return edge.block_number <= sale.block_number;
    }
    false
}

fn nft_transfer_key(
    token_id: &str,
    tx_hash: &str,
    from_address: &str,
    to_address: &str,
) -> (String, String, String, String) {
    (
        token_id.trim().to_string(),
        tx_hash.trim().to_lowercase(),
        from_address.trim().to_lowercase(),
        to_address.trim().to_lowercase(),
    )
}

fn value_flow_scope_matches(
    edge: &ValueFlowEdgePayload,
    wash_cycle_scope: &WashCycleScope,
) -> bool {
    let tx_hash = edge.tx_hash.trim().to_lowercase();
    if !wash_cycle_scope.tx_hashes.contains(&tx_hash) {
        return false;
    }
    value_flow_token_ids(edge)
        .iter()
        .any(|token_id| wash_cycle_scope.token_ids.contains(token_id))
        && (wash_cycle_scope
            .participants
            .contains(&edge.from_address.trim().to_lowercase())
            || wash_cycle_scope
                .participants
                .contains(&edge.to_address.trim().to_lowercase()))
}

fn sale_scope_matches(sale: &NftSaleRecord, wash_cycle_scope: &WashCycleScope) -> bool {
    let tx_hash = sale.tx_hash.trim().to_lowercase();
    wash_cycle_scope.tx_hashes.contains(&tx_hash)
        && wash_cycle_scope
            .token_ids
            .contains(&sale.token_id.trim().to_lowercase())
        && (wash_cycle_scope
            .participants
            .contains(&sale.seller_address.trim().to_lowercase())
            || wash_cycle_scope
                .participants
                .contains(&sale.buyer_address.trim().to_lowercase()))
}

fn wash_cycle_value_for_address(
    address: &str,
    wash_cycle_scope: &WashCycleScope,
    activity: &PreparedContractActivity<'_>,
    value_flow_edges: &[ValueFlowEdgePayload],
) -> (f64, f64, bool) {
    let address = address.trim().to_lowercase();
    let mut value_eth = 0.0;
    let mut value_usd = 0.0;
    let mut has_usd = false;
    let mut seen_sales = HashSet::<(String, String, String, String)>::new();
    for sale in &activity.sorted_sales {
        if !sale_has_positive_value(sale)
            || !sale_scope_matches(sale, wash_cycle_scope)
            || (!sale.seller_address.eq_ignore_ascii_case(&address)
                && !sale.buyer_address.eq_ignore_ascii_case(&address))
        {
            continue;
        }
        let key = nft_transfer_key(
            &sale.token_id,
            &sale.tx_hash,
            &sale.seller_address,
            &sale.buyer_address,
        );
        if !seen_sales.insert(key) {
            continue;
        }
        value_eth += sale.price_eth.unwrap_or_default();
        if let Some(price_usd) = sale.price_usd {
            has_usd = true;
            value_usd += price_usd;
        }
    }

    let mut seen_edges = HashSet::<(String, String, String, String, String)>::new();
    for edge in value_flow_edges {
        if !value_flow_has_positive_value(edge)
            || !value_flow_scope_matches(edge, wash_cycle_scope)
            || (!edge.from_address.eq_ignore_ascii_case(&address)
                && !edge.to_address.eq_ignore_ascii_case(&address))
        {
            continue;
        }
        let key = (
            edge.tx_hash.trim().to_lowercase(),
            edge.token_id.trim().to_lowercase(),
            edge.from_address.trim().to_lowercase(),
            edge.to_address.trim().to_lowercase(),
            edge.channel.trim().to_lowercase(),
        );
        if !seen_edges.insert(key) {
            continue;
        }
        value_eth += edge.value_eth.unwrap_or_default();
        if let Some(edge_value_usd) = edge.value_usd {
            has_usd = true;
            value_usd += edge_value_usd;
        }
    }

    (value_eth, value_usd, has_usd)
}

fn is_service_value_flow_role(role: &str) -> bool {
    matches!(role, "cex" | "bridge" | "mixer")
}

fn value_flow_operator_address(
    address: &str,
    contract_address: &str,
    role: &str,
) -> Option<String> {
    let address = address.trim();
    if address.is_empty()
        || address.eq_ignore_ascii_case(ZERO_ADDRESS)
        || address.eq_ignore_ascii_case(contract_address)
        || is_service_value_flow_role(role)
    {
        None
    } else {
        Some(address.to_string())
    }
}

fn build_owner_token_map(owners: &[OwnerBalance]) -> HashMap<String, HashSet<String>> {
    let mut owner_token_map = HashMap::new();
    for owner in owners {
        if owner.owner_address.is_empty() || owner.owner_address == ZERO_ADDRESS {
            continue;
        }
        let held_tokens: HashSet<String> = owner
            .token_balances
            .iter()
            .filter(|(_, balance)| **balance > 0)
            .map(|(token_id, _)| token_id.clone())
            .collect();
        if !held_tokens.is_empty() {
            owner_token_map.insert(owner.owner_address.clone(), held_tokens);
        }
    }
    owner_token_map
}

pub(crate) fn prepare_contract_activity<'a>(
    transfers: &'a [TransferRecord],
    sales: &'a [NftSaleRecord],
    owners: &'a [OwnerBalance],
) -> PreparedContractActivity<'a> {
    let owner_token_map = build_owner_token_map(owners);

    let mut latest_outgoing = HashMap::new();
    let mut sorted_transfers: Vec<&TransferRecord> = transfers.iter().collect();
    sorted_transfers.sort_by(|left, right| transfer_sort_key(left).cmp(&transfer_sort_key(right)));
    for transfer in &sorted_transfers {
        if transfer.from_address.is_empty() || transfer.from_address == ZERO_ADDRESS {
            continue;
        }
        let key = (transfer.from_address.clone(), transfer.token_id.clone());
        let transfer_key = (
            transfer.block_number,
            transfer.log_index,
            transfer.tx_hash.clone(),
        );
        match latest_outgoing.get(&key) {
            Some(current) if current >= &transfer_key => {}
            _ => {
                latest_outgoing.insert(key, transfer_key);
            }
        }
    }

    let mut sorted_sales: Vec<&NftSaleRecord> = sales.iter().collect();
    sorted_sales.sort_by(|left, right| sale_sort_key(left).cmp(&sale_sort_key(right)));

    PreparedContractActivity {
        owner_token_map,
        sorted_transfers,
        sorted_sales,
        latest_outgoing,
    }
}

pub fn build_infringing_token_records(
    contract_address: &str,
    contract_candidates: &[DuplicateCandidate],
    transfers: &[TransferRecord],
) -> Vec<InfringingTokenRecord> {
    let candidate_refs: Vec<&DuplicateCandidate> = contract_candidates.iter().collect();
    build_infringing_token_records_with_context_refs(
        contract_address,
        &candidate_refs,
        transfers,
        &HashSet::new(),
        &HashMap::new(),
    )
}

pub fn build_infringing_token_records_with_context(
    contract_address: &str,
    contract_candidates: &[DuplicateCandidate],
    transfers: &[TransferRecord],
    official_addresses: &HashSet<String>,
    candidate_open_license_by_token: &HashMap<(String, String), bool>,
) -> Vec<InfringingTokenRecord> {
    let candidate_refs: Vec<&DuplicateCandidate> = contract_candidates.iter().collect();
    build_infringing_token_records_with_context_refs(
        contract_address,
        &candidate_refs,
        transfers,
        official_addresses,
        candidate_open_license_by_token,
    )
}

pub fn build_infringing_token_records_with_context_refs(
    contract_address: &str,
    contract_candidates: &[&DuplicateCandidate],
    transfers: &[TransferRecord],
    official_addresses: &HashSet<String>,
    candidate_open_license_by_token: &HashMap<(String, String), bool>,
) -> Vec<InfringingTokenRecord> {
    let mut transfers_by_token: HashMap<String, Vec<&TransferRecord>> = HashMap::new();
    for transfer in transfers {
        if transfer.contract_address != contract_address || transfer.token_id.is_empty() {
            continue;
        }
        transfers_by_token
            .entry(transfer.token_id.clone())
            .or_default()
            .push(transfer);
    }
    for token_transfers in transfers_by_token.values_mut() {
        token_transfers
            .sort_by(|left, right| transfer_sort_key(left).cmp(&transfer_sort_key(right)));
    }

    let mut rows: Vec<InfringingTokenRecord> = contract_candidates
        .iter()
        .map(|candidate| {
            let token_transfers = transfers_by_token.get(&candidate.token_id);
            let mint_transfer = token_transfers.and_then(|rows| {
                rows.iter()
                    .find(|row| row.from_address == ZERO_ADDRESS)
                    .copied()
            });
            let first_non_mint_transfer = token_transfers.and_then(|rows| {
                rows.iter()
                    .find(|row| row.from_address != ZERO_ADDRESS)
                    .copied()
            });
            let first_transfer = token_transfers.and_then(|rows| rows.first().copied());
            let (minter_address, mint_tx_hash, mint_block, first_transfer_time) =
                if let Some(mint_transfer) = mint_transfer {
                    (
                        mint_transfer.to_address.clone(),
                        mint_transfer.tx_hash.clone(),
                        mint_transfer.block_number,
                        first_non_mint_transfer
                            .map(|transfer| transfer.block_time)
                            .unwrap_or(0),
                    )
                } else if let Some(first_transfer) = first_transfer {
                    (
                        first_transfer.to_address.clone(),
                        first_transfer.tx_hash.clone(),
                        first_transfer.block_number,
                        first_transfer.block_time,
                    )
                } else {
                    (String::new(), String::new(), 0, 0)
                };

            let official_or_legit_reissue =
                !minter_address.is_empty() && official_addresses.contains(&minter_address);

            InfringingTokenRecord {
                contract_address: contract_address.to_string(),
                token_id: candidate.token_id.clone(),
                mint_tx_hash,
                mint_block,
                minter_address,
                first_transfer_time,
                history_window: "full".to_string(),
                match_reasons: candidate.match_reasons.clone(),
                candidate_open_license: candidate_open_license_by_token
                    .get(&(contract_address.to_string(), candidate.token_id.clone()))
                    .copied()
                    .unwrap_or(false),
                official_or_legit_reissue,
            }
        })
        .collect();

    rows.sort_by(|left, right| {
        (&left.token_id, &left.contract_address).cmp(&(&right.token_id, &right.contract_address))
    });
    rows
}

pub fn build_malicious_address_records(
    contract_address: &str,
    transfers: &[TransferRecord],
    infringing_tokens: &[InfringingTokenRecord],
    mint_payment_edges: &[ValueFlowEdgePayload],
) -> Vec<MaliciousAddressPayload> {
    let activity =
        prepare_contract_activity(transfers, &[] as &[NftSaleRecord], &[] as &[OwnerBalance]);
    build_malicious_address_records_from_activity(
        contract_address,
        &activity,
        infringing_tokens,
        mint_payment_edges,
    )
}

pub(crate) fn build_malicious_address_records_from_activity(
    contract_address: &str,
    activity: &PreparedContractActivity<'_>,
    infringing_tokens: &[InfringingTokenRecord],
    mint_payment_edges: &[ValueFlowEdgePayload],
) -> Vec<MaliciousAddressPayload> {
    let relevant_token_ids: HashSet<String> = infringing_tokens
        .iter()
        .filter(|item| !item.token_id.is_empty())
        .map(|item| item.token_id.clone())
        .collect();
    let mint_addresses: HashSet<String> = infringing_tokens
        .iter()
        .filter(|item| !item.minter_address.is_empty())
        .map(|item| item.minter_address.clone())
        .collect();

    let mut outgoing: HashMap<String, HashSet<String>> = HashMap::new();
    let mut incoming: HashMap<String, HashSet<String>> = HashMap::new();
    let mut cycle_counts: HashMap<String, i64> = HashMap::new();
    let mut wash_cycle_scopes: HashMap<String, WashCycleScope> = HashMap::new();
    let mut sale_seller_counts: HashMap<String, i64> = HashMap::new();
    let mut seen_pairs: HashSet<(String, String)> = HashSet::new();
    let mut seen_transfer_scopes_by_pair: HashMap<(String, String), Vec<WashCycleTransferScope>> =
        HashMap::new();
    let mut rapid_addresses: HashSet<String> = HashSet::new();
    let mut rapid_token_ids_by_address: HashMap<String, HashSet<String>> = HashMap::new();
    let mut mint_times: HashMap<String, i64> = HashMap::new();
    let mut receiver_candidates: HashSet<String> = HashSet::new();
    let paid_mint_payers: HashSet<String> = mint_payment_edges
        .iter()
        .filter(|edge| {
            edge.channel == "mint_payment"
                && edge.contract_address.eq_ignore_ascii_case(contract_address)
                && value_flow_has_positive_value(edge)
        })
        .map(|edge| edge.from_address.clone())
        .filter(|address| !address.is_empty())
        .collect();
    let mut secondary_market_resellers: HashSet<String> = HashSet::new();
    let mut prior_paid_buyers_by_token: HashMap<String, HashSet<String>> = HashMap::new();
    let mut paid_acquisition_addresses: HashSet<String> = paid_mint_payers.clone();
    let sale_transfer_keys: HashSet<(String, String, String, String)> = activity
        .sorted_sales
        .iter()
        .filter(|sale| {
            sale_has_positive_value(sale)
                && (relevant_token_ids.is_empty() || relevant_token_ids.contains(&sale.token_id))
        })
        .map(|sale| {
            nft_transfer_key(
                &sale.token_id,
                &sale.tx_hash,
                &sale.seller_address,
                &sale.buyer_address,
            )
        })
        .collect();

    for transfer in &activity.sorted_transfers {
        if !relevant_token_ids.is_empty() && !relevant_token_ids.contains(&transfer.token_id) {
            continue;
        }
        if !transfer.to_address.is_empty() {
            receiver_candidates.insert(transfer.to_address.clone());
        }
        if transfer.from_address == ZERO_ADDRESS {
            if !transfer.to_address.is_empty() {
                mint_times.insert(transfer.token_id.clone(), transfer.block_time);
            }
            continue;
        }
        if !transfer.from_address.is_empty() && !transfer.to_address.is_empty() {
            outgoing
                .entry(transfer.from_address.clone())
                .or_default()
                .insert(transfer.to_address.clone());
            let transfer_key = nft_transfer_key(
                &transfer.token_id,
                &transfer.tx_hash,
                &transfer.from_address,
                &transfer.to_address,
            );
            if !sale_transfer_keys.contains(&transfer_key) {
                incoming
                    .entry(transfer.to_address.clone())
                    .or_default()
                    .insert(transfer.from_address.clone());
            }
            let pair = (transfer.from_address.clone(), transfer.to_address.clone());
            let reverse = (transfer.to_address.clone(), transfer.from_address.clone());
            let transfer_scope = WashCycleTransferScope::from_transfer(transfer);
            if seen_pairs.contains(&reverse) {
                *cycle_counts
                    .entry(transfer.from_address.clone())
                    .or_insert(0) += 1;
                *cycle_counts.entry(transfer.to_address.clone()).or_insert(0) += 1;
                add_wash_cycle_transfer_scope(
                    &mut wash_cycle_scopes,
                    &transfer.from_address,
                    &transfer_scope,
                );
                add_wash_cycle_transfer_scope(
                    &mut wash_cycle_scopes,
                    &transfer.to_address,
                    &transfer_scope,
                );
                if let Some(reverse_scopes) = seen_transfer_scopes_by_pair.get(&reverse) {
                    for reverse_scope in reverse_scopes {
                        add_wash_cycle_transfer_scope(
                            &mut wash_cycle_scopes,
                            &transfer.from_address,
                            reverse_scope,
                        );
                        add_wash_cycle_transfer_scope(
                            &mut wash_cycle_scopes,
                            &transfer.to_address,
                            reverse_scope,
                        );
                    }
                }
            }
            seen_pairs.insert(pair.clone());
            seen_transfer_scopes_by_pair
                .entry(pair)
                .or_default()
                .push(transfer_scope);
        }
        let mint_time = *mint_times.get(&transfer.token_id).unwrap_or(&0);
        if mint_time > 0
            && transfer.block_time > 0
            && transfer.block_time - mint_time <= 24 * 3600
            && !transfer.from_address.is_empty()
        {
            rapid_addresses.insert(transfer.from_address.clone());
            rapid_token_ids_by_address
                .entry(transfer.from_address.clone())
                .or_default()
                .insert(transfer.token_id.clone());
        }
    }

    for sale in &activity.sorted_sales {
        if !relevant_token_ids.is_empty() && !relevant_token_ids.contains(&sale.token_id) {
            continue;
        }
        if sale_has_positive_value(sale) {
            let prior_paid_buyers = prior_paid_buyers_by_token
                .entry(sale.token_id.clone())
                .or_default();
            if prior_paid_buyers.contains(&sale.seller_address) {
                secondary_market_resellers.insert(sale.seller_address.clone());
            }
            if !sale.buyer_address.is_empty() {
                paid_acquisition_addresses.insert(sale.buyer_address.clone());
                prior_paid_buyers.insert(sale.buyer_address.clone());
            }
        }
        if !sale.seller_address.is_empty() {
            *sale_seller_counts
                .entry(sale.seller_address.clone())
                .or_insert(0) += 1;
        }
        if !sale.buyer_address.is_empty() {
            receiver_candidates.insert(sale.buyer_address.clone());
        }
    }

    let mut withdrawal_edge_counts: HashMap<String, i64> = HashMap::new();
    let mut cashout_edge_counts: HashMap<String, i64> = HashMap::new();
    for edge in mint_payment_edges {
        if !edge.contract_address.eq_ignore_ascii_case(contract_address)
            || !value_flow_has_positive_value(edge)
        {
            continue;
        }
        match edge.channel.as_str() {
            "withdrawal" => {
                if let Some(address) =
                    value_flow_operator_address(&edge.to_address, contract_address, &edge.to_role)
                {
                    *withdrawal_edge_counts.entry(address).or_insert(0) += 1;
                }
            }
            "cashout_hop" => {
                if let Some(address) = value_flow_operator_address(
                    &edge.from_address,
                    contract_address,
                    &edge.from_role,
                ) {
                    *cashout_edge_counts.entry(address).or_insert(0) += 1;
                }
                if let Some(address) =
                    value_flow_operator_address(&edge.to_address, contract_address, &edge.to_role)
                {
                    *cashout_edge_counts.entry(address).or_insert(0) += 1;
                }
            }
            _ => {}
        }
    }

    let mut candidate_addresses: Vec<String> = outgoing
        .keys()
        .cloned()
        .chain(incoming.keys().cloned())
        .chain(sale_seller_counts.keys().cloned())
        .chain(withdrawal_edge_counts.keys().cloned())
        .chain(cashout_edge_counts.keys().cloned())
        .chain(receiver_candidates)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    candidate_addresses.sort();

    let mut rows = Vec::new();
    for address in candidate_addresses {
        if address.is_empty() {
            continue;
        }
        let mint_activity_observed = mint_addresses.contains(&address);
        let wash_cycle_count = *cycle_counts.get(&address).unwrap_or(&0);
        let (wash_cycle_value_eth, wash_cycle_value_usd, wash_cycle_has_usd) =
            if let Some(wash_cycle_scope) = wash_cycle_scopes.get(&address) {
                wash_cycle_value_for_address(
                    &address,
                    wash_cycle_scope,
                    activity,
                    mint_payment_edges,
                )
            } else {
                (0.0, 0.0, false)
            };
        let star_out_degree = outgoing.get(&address).map(|value| value.len()).unwrap_or(0) as i64;
        let is_star_distributor = star_out_degree >= 3;
        let withdrawal_edge_count = *withdrawal_edge_counts.get(&address).unwrap_or(&0);
        let cashout_edge_count = *cashout_edge_counts.get(&address).unwrap_or(&0);
        let value_extraction_observed = withdrawal_edge_count > 0 || cashout_edge_count > 0;
        let sale_seller_count = *sale_seller_counts.get(&address).unwrap_or(&0);
        let rapid_spread = rapid_addresses.contains(&address);
        let rapid_spread_token_count = rapid_token_ids_by_address
            .get(&address)
            .map(|tokens| tokens.len())
            .unwrap_or(0);
        let acquired_victim_like = (paid_acquisition_addresses.contains(&address)
            || secondary_market_resellers.contains(&address))
            && wash_cycle_count == 0
            && !is_star_distributor
            && !value_extraction_observed
            && sale_seller_count < 3;
        if acquired_victim_like {
            continue;
        }
        let high_volume_seller = sale_seller_count >= 3;
        let operator_level = classify_operator_level(&OperatorLevelSignals {
            wash_cycle_count,
            wash_cycle_value_eth,
            wash_cycle_value_usd,
            wash_cycle_has_usd,
            star_out_degree,
            sale_seller_count,
            rapid_spread_token_count,
            value_extraction_observed,
        });
        if wash_cycle_count == 0
            && !is_star_distributor
            && !value_extraction_observed
            && !high_volume_seller
            && !rapid_spread
        {
            continue;
        }
        if operator_level == 0 {
            continue;
        }
        rows.push(MaliciousAddressPayload {
            address: address.clone(),
            mint_activity_observed,
            wash_cycle_count,
            wash_cycle_propagation_count: wash_cycle_count,
            wash_cycle_value_eth,
            wash_cycle_value_usd,
            star_out_degree,
            sale_seller_count,
            withdrawal_edge_count,
            cashout_edge_count,
            operator_level,
            operator_level_label: operator_level_label(operator_level).into(),
            rapid_spread_contracts: if rapid_spread {
                vec![contract_address.to_string()]
            } else {
                vec![]
            },
            evidence_contracts: vec![contract_address.to_string()],
        });
    }
    rows
}

#[derive(Default)]
struct AttributionAccumulator {
    roles: BTreeSet<String>,
    attacker_score: f64,
    operator_score: f64,
    colluder_score: f64,
    victim_score: f64,
    corruption_score: f64,
    neutral_score: f64,
    operator_level: i64,
    operator_level_label: String,
    evidence: Vec<AddressEvidencePayload>,
}

fn attribution_entry<'a>(
    rows: &'a mut BTreeMap<String, AttributionAccumulator>,
    address: &str,
) -> Option<&'a mut AttributionAccumulator> {
    let address = address.trim().to_lowercase();
    if address.is_empty() || address == ZERO_ADDRESS {
        return None;
    }
    Some(rows.entry(address).or_default())
}

#[derive(Clone, Copy)]
enum EvidenceBucket {
    Operator,
    Colluder,
    Victim,
    Corruption,
    Neutral,
}

struct EvidenceInput<'a> {
    contract_address: &'a str,
    address: &'a str,
    role: &'a str,
    evidence_type: &'a str,
    token_id: &'a str,
    tx_hash: &'a str,
    weight: f64,
    detail: &'a str,
    bucket: EvidenceBucket,
}

fn add_attribution_evidence(
    rows: &mut BTreeMap<String, AttributionAccumulator>,
    input: EvidenceInput<'_>,
) {
    let Some(entry) = attribution_entry(rows, input.address) else {
        return;
    };
    entry.roles.insert(input.role.to_string());
    match input.bucket {
        EvidenceBucket::Operator => {
            entry.operator_score += input.weight;
            entry.attacker_score += input.weight;
        }
        EvidenceBucket::Colluder => {
            entry.colluder_score += input.weight;
            entry.attacker_score += input.weight;
        }
        EvidenceBucket::Victim => {
            entry.victim_score += input.weight;
        }
        EvidenceBucket::Corruption => {
            entry.corruption_score += input.weight;
        }
        EvidenceBucket::Neutral => {
            entry.neutral_score += input.weight;
        }
    }
    entry.evidence.push(AddressEvidencePayload {
        evidence_type: input.evidence_type.to_string(),
        contract_address: input.contract_address.to_string(),
        token_id: input.token_id.to_string(),
        tx_hash: input.tx_hash.to_string(),
        weight: input.weight,
        detail: input.detail.to_string(),
    });
}

fn apply_operator_level(entry: &mut AttributionAccumulator, level: i64, label: &str) {
    if level > entry.operator_level {
        entry.operator_level = level;
        entry.operator_level_label = label.to_string();
    }
}

fn attribution_confidence(
    operator_score: f64,
    colluder_score: f64,
    victim_score: f64,
    corruption_score: f64,
) -> String {
    let best = operator_score
        .max(colluder_score)
        .max(victim_score)
        .max(corruption_score);
    let second = [
        operator_score,
        colluder_score,
        victim_score,
        corruption_score,
    ]
    .into_iter()
    .filter(|value| *value < best)
    .max_by(|left, right| left.total_cmp(right))
    .unwrap_or(0.0);
    let margin = best - second;
    if best >= 0.75 && margin >= 0.25 {
        "high".into()
    } else if best >= 0.45 {
        "medium".into()
    } else {
        "low".into()
    }
}

fn attribution_label(
    operator_score: f64,
    colluder_score: f64,
    victim_score: f64,
    corruption_score: f64,
    neutral_score: f64,
) -> String {
    if corruption_score >= 0.40 && victim_score >= 0.20 {
        return "corrupted_victim".into();
    }
    if operator_score >= 0.25 && operator_score >= victim_score && operator_score >= colluder_score
    {
        return "suspected_operator".into();
    }
    if colluder_score >= 0.25 && colluder_score >= victim_score {
        return "suspected_colluder".into();
    }
    if victim_score >= 0.45 && victim_score >= operator_score && victim_score >= colluder_score {
        return "likely_victim".into();
    }
    if neutral_score >= 0.20 {
        return "neutral_participant".into();
    }
    "neutral_participant".into()
}

pub fn build_address_attribution_records(
    contract_address: &str,
    infringing_tokens: &[InfringingTokenRecord],
    sales: &[NftSaleRecord],
    mint_payment_edges: &[ValueFlowEdgePayload],
    malicious_addresses: &[MaliciousAddressPayload],
    honest_addresses: &[HonestAddressPayload],
    secondary_sale_victim_addresses: &[SecondarySaleVictimAddressPayload],
) -> Vec<AddressAttributionPayload> {
    let relevant_token_ids: HashSet<String> = infringing_tokens
        .iter()
        .filter(|item| !item.token_id.is_empty())
        .map(|item| item.token_id.clone())
        .collect();
    let mut rows = BTreeMap::<String, AttributionAccumulator>::new();

    for token in infringing_tokens {
        add_attribution_evidence(
            &mut rows,
            EvidenceInput {
                contract_address,
                address: &token.minter_address,
                role: "mint_recipient",
                evidence_type: "mint_recipient",
                token_id: &token.token_id,
                tx_hash: &token.mint_tx_hash,
                weight: 0.10,
                detail: "mint recipient is weak evidence only; paid mints may be victims",
                bucket: EvidenceBucket::Neutral,
            },
        );
        if token.official_or_legit_reissue {
            if let Some(entry) = attribution_entry(&mut rows, &token.minter_address) {
                entry.roles.insert("official_reissue".into());
            }
        }
    }

    for edge in mint_payment_edges {
        if edge.channel != "mint_payment"
            || !edge.contract_address.eq_ignore_ascii_case(contract_address)
            || edge.from_address.is_empty()
            || (edge.value_eth.unwrap_or(0.0) <= 0.0 && edge.value_usd.unwrap_or(0.0) <= 0.0)
        {
            continue;
        }
        let paid_to_controlled_recipient =
            matches!(
                edge.to_role.as_str(),
                "mint_contract"
                    | "contract_deployer"
                    | "contract_owner"
                    | "contract_admin"
                    | "proxy_admin"
                    | "operator_wallet"
            ) || edge.to_address.eq_ignore_ascii_case(contract_address);
        if !paid_to_controlled_recipient {
            continue;
        }
        add_attribution_evidence(
            &mut rows,
            EvidenceInput {
                contract_address,
                address: &edge.from_address,
                role: "paid_minter",
                evidence_type: "paid_mint_payment",
                token_id: &edge.token_id,
                tx_hash: &edge.tx_hash,
                weight: 0.45,
                detail: "address paid native or priced value to mint a copied NFT without independent operator evidence",
                bucket: EvidenceBucket::Victim,
            },
        );
    }

    for item in malicious_addresses {
        if let Some(entry) = attribution_entry(&mut rows, &item.address) {
            apply_operator_level(entry, item.operator_level, &item.operator_level_label);
        }
        if item.wash_cycle_count > 0 {
            add_attribution_evidence(
                &mut rows,
                EvidenceInput {
                    contract_address,
                    address: &item.address,
                    role: "wash_cycle",
                    evidence_type: "wash_cycle",
                    token_id: "",
                    tx_hash: "",
                    weight: 0.35,
                    detail: "address participates in reciprocal transfer cycles",
                    bucket: EvidenceBucket::Operator,
                },
            );
        }
        if item.star_out_degree >= 3 {
            add_attribution_evidence(
                &mut rows,
                EvidenceInput {
                    contract_address,
                    address: &item.address,
                    role: "star_distributor",
                    evidence_type: "star_distribution",
                    token_id: "",
                    tx_hash: "",
                    weight: 0.30,
                    detail: "address distributes copied NFTs to many unique receivers",
                    bucket: EvidenceBucket::Operator,
                },
            );
        }
        if item.sale_seller_count >= HIGH_VOLUME_SELLER_L1_THRESHOLD {
            add_attribution_evidence(
                &mut rows,
                EvidenceInput {
                    contract_address,
                    address: &item.address,
                    role: "high_volume_seller",
                    evidence_type: "high_volume_sale_seller",
                    token_id: "",
                    tx_hash: "",
                    weight: 0.25,
                    detail: "address sells copied NFTs across repeated sale events",
                    bucket: EvidenceBucket::Operator,
                },
            );
        }
        if item.withdrawal_edge_count > 0 {
            add_attribution_evidence(
                &mut rows,
                EvidenceInput {
                    contract_address,
                    address: &item.address,
                    role: "withdrawal_recipient",
                    evidence_type: "contract_value_withdrawal",
                    token_id: "",
                    tx_hash: "",
                    weight: 0.35,
                    detail: "address receives native or priced value withdrawn from the copied NFT contract",
                    bucket: EvidenceBucket::Operator,
                },
            );
        }
        if item.cashout_edge_count > 0 {
            add_attribution_evidence(
                &mut rows,
                EvidenceInput {
                    contract_address,
                    address: &item.address,
                    role: "cashout_intermediate",
                    evidence_type: "multi_hop_cashout",
                    token_id: "",
                    tx_hash: "",
                    weight: 0.25,
                    detail:
                        "address appears as an intermediate wallet in same-block cashout tracing",
                    bucket: EvidenceBucket::Colluder,
                },
            );
        }
        if !item.rapid_spread_contracts.is_empty() {
            add_attribution_evidence(
                &mut rows,
                EvidenceInput {
                    contract_address,
                    address: &item.address,
                    role: "rapid_spreader",
                    evidence_type: "rapid_spread",
                    token_id: "",
                    tx_hash: "",
                    weight: 0.20,
                    detail: "address appears in propagation within 24 hours of mint",
                    bucket: EvidenceBucket::Colluder,
                },
            );
        }
    }

    for sale in sales {
        if !sale.contract_address.eq_ignore_ascii_case(contract_address) {
            continue;
        }
        if !relevant_token_ids.is_empty() && !relevant_token_ids.contains(&sale.token_id) {
            continue;
        }
        let seller_key = sale.seller_address.trim().to_lowercase();
        let seller_context = rows.get(&seller_key).map(|entry| {
            if entry.operator_score > 0.0 {
                Some(EvidenceBucket::Operator)
            } else if entry.colluder_score > 0.0 {
                Some(EvidenceBucket::Colluder)
            } else {
                None
            }
        });
        let (seller_bucket, seller_weight, seller_detail) =
            if let Some(Some(bucket)) = seller_context {
                (
                bucket,
                0.10,
                "address sold copied NFT and already has independent operator or colluder evidence",
            )
            } else {
                (
                EvidenceBucket::Neutral,
                0.10,
                "sale alone is weak evidence; ordinary paid minters or resellers may be victims",
            )
            };
        add_attribution_evidence(
            &mut rows,
            EvidenceInput {
                contract_address,
                address: &sale.seller_address,
                role: "seller",
                evidence_type: "infringing_sale_seller",
                token_id: &sale.token_id,
                tx_hash: &sale.tx_hash,
                weight: seller_weight,
                detail: seller_detail,
                bucket: seller_bucket,
            },
        );
        add_attribution_evidence(
            &mut rows,
            EvidenceInput {
                contract_address,
                address: &sale.buyer_address,
                role: "buyer",
                evidence_type: "marketplace_purchase",
                token_id: &sale.token_id,
                tx_hash: &sale.tx_hash,
                weight: if sale.price_eth.unwrap_or(0.0) > 0.0
                    || sale.price_usd.unwrap_or(0.0) > 0.0
                {
                    0.50
                } else {
                    0.30
                },
                detail: "address bought the copied NFT through a sale event",
                bucket: EvidenceBucket::Victim,
            },
        );
    }

    for item in secondary_sale_victim_addresses {
        add_attribution_evidence(
            &mut rows,
            EvidenceInput {
                contract_address,
                address: &item.address,
                role: "victim_candidate",
                evidence_type: "secondary_sale_victim_profile",
                token_id: "",
                tx_hash: &item.last_buy_tx_hash,
                weight: 0.20,
                detail: "address has a purchase profile in the victim candidate set",
                bucket: EvidenceBucket::Victim,
            },
        );
        if item.is_stuck {
            add_attribution_evidence(
                &mut rows,
                EvidenceInput {
                    contract_address,
                    address: &item.address,
                    role: "stuck_victim",
                    evidence_type: "stuck_holder",
                    token_id: "",
                    tx_hash: &item.last_buy_tx_hash,
                    weight: 0.25,
                    detail: "address still holds the copied NFT after purchase with no later outgoing transfer",
                    bucket: EvidenceBucket::Victim,
                },
            );
        }
        if item
            .buy_asset_ratio
            .map(|ratio| ratio >= 0.60)
            .unwrap_or(false)
        {
            add_attribution_evidence(
                &mut rows,
                EvidenceInput {
                    contract_address,
                    address: &item.address,
                    role: "high_exposure_buyer",
                    evidence_type: "high_purchase_balance_ratio",
                    token_id: "",
                    tx_hash: &item.last_buy_tx_hash,
                    weight: 0.15,
                    detail: "purchase consumed a high share of the observed wallet balance",
                    bucket: EvidenceBucket::Victim,
                },
            );
        }
    }

    for item in honest_addresses {
        if let Some(entry) = attribution_entry(&mut rows, &item.address) {
            entry.roles.insert("honest_holder".into());
            entry.neutral_score += 0.20;
        }
        if item.is_corrupted_address {
            add_attribution_evidence(
                &mut rows,
                EvidenceInput {
                    contract_address,
                    address: &item.address,
                    role: "corrupted_honest",
                    evidence_type: "corrupted_honest_resale",
                    token_id: "",
                    tx_hash: "",
                    weight: 0.45,
                    detail:
                        "address otherwise looks like a participant but also propagates copied NFTs",
                    bucket: EvidenceBucket::Corruption,
                },
            );
        }
    }

    rows.into_iter()
        .map(|(address, row)| {
            let operator_score = row.operator_score.clamp(0.0, 1.0);
            let colluder_score = row.colluder_score.clamp(0.0, 1.0);
            let attacker_score = row.attacker_score.clamp(0.0, 1.0);
            let victim_score = row.victim_score.clamp(0.0, 1.0);
            let corruption_score = row.corruption_score.clamp(0.0, 1.0);
            let neutral_score = row.neutral_score.clamp(0.0, 1.0);
            AddressAttributionPayload {
                contract_address: contract_address.to_string(),
                address,
                observed_roles: row.roles.into_iter().collect(),
                attribution_label: attribution_label(
                    operator_score,
                    colluder_score,
                    victim_score,
                    corruption_score,
                    neutral_score,
                ),
                operator_score,
                colluder_score,
                attacker_score,
                victim_score,
                corruption_score,
                neutral_score,
                confidence: attribution_confidence(
                    operator_score,
                    colluder_score,
                    victim_score,
                    corruption_score,
                ),
                operator_level: row.operator_level,
                operator_level_label: row.operator_level_label,
                evidence: row.evidence,
            }
        })
        .collect()
}

pub fn add_acquisition_exposure_attribution_evidence(
    address_attributions: Vec<AddressAttributionPayload>,
    victim_acquisition_addresses: &[VictimAcquisitionAddressPayload],
) -> Vec<AddressAttributionPayload> {
    let mut rows: BTreeMap<(String, String), AddressAttributionPayload> = address_attributions
        .into_iter()
        .map(|item| {
            (
                (
                    item.contract_address.to_lowercase(),
                    item.address.to_lowercase(),
                ),
                item,
            )
        })
        .collect();

    for acquisition in victim_acquisition_addresses {
        if !acquisition
            .buy_asset_ratio
            .map(|ratio| ratio >= 0.60)
            .unwrap_or(false)
        {
            continue;
        }
        for contract_address in &acquisition.contract_addresses {
            let contract_address = contract_address.trim().to_lowercase();
            let address = acquisition.address.trim().to_lowercase();
            if contract_address.is_empty() || address.is_empty() || address == ZERO_ADDRESS {
                continue;
            }
            let entry = rows
                .entry((contract_address.clone(), address.clone()))
                .or_insert_with(|| AddressAttributionPayload {
                    contract_address: contract_address.clone(),
                    address: address.clone(),
                    attribution_label: "likely_victim".into(),
                    confidence: "low".into(),
                    ..AddressAttributionPayload::default()
                });
            if !entry
                .observed_roles
                .iter()
                .any(|role| role == "high_exposure_acquirer")
            {
                entry.observed_roles.push("high_exposure_acquirer".into());
                entry.observed_roles.sort();
            }
            entry.victim_score = (entry.victim_score + 0.15).clamp(0.0, 1.0);
            if !entry
                .evidence
                .iter()
                .any(|evidence| evidence.evidence_type == "high_acquisition_balance_ratio")
            {
                entry.evidence.push(AddressEvidencePayload {
                    evidence_type: "high_acquisition_balance_ratio".into(),
                    contract_address: contract_address.clone(),
                    token_id: String::new(),
                    tx_hash: acquisition.tx_hashes.first().cloned().unwrap_or_default(),
                    weight: 0.15,
                    detail:
                        "total acquisition cost consumed a high share of the observed pre-acquisition ETH balance"
                            .into(),
                });
            }
            entry.attribution_label = attribution_label(
                entry.operator_score,
                entry.colluder_score,
                entry.victim_score,
                entry.corruption_score,
                entry.neutral_score,
            );
            entry.confidence = attribution_confidence(
                entry.operator_score,
                entry.colluder_score,
                entry.victim_score,
                entry.corruption_score,
            );
        }
    }

    rows.into_values().collect()
}

pub fn build_secondary_sale_victim_address_records(
    contract_address: &str,
    sales: &[NftSaleRecord],
    transfers: &[TransferRecord],
    owners: &[OwnerBalance],
    sale_metrics_by_tx: &BTreeMap<String, SaleMetricRecord>,
) -> Vec<SecondarySaleVictimAddressPayload> {
    let activity = prepare_contract_activity(transfers, sales, owners);
    build_secondary_sale_victim_address_records_from_activity(
        contract_address,
        &activity,
        sale_metrics_by_tx,
    )
}

pub(crate) fn build_secondary_sale_victim_address_records_from_activity(
    contract_address: &str,
    activity: &PreparedContractActivity<'_>,
    sale_metrics_by_tx: &BTreeMap<String, SaleMetricRecord>,
) -> Vec<SecondarySaleVictimAddressPayload> {
    build_secondary_sale_victim_address_records_excluding_malicious_from_activity(
        contract_address,
        activity,
        sale_metrics_by_tx,
        &[],
    )
}

pub(crate) fn build_secondary_sale_victim_address_records_excluding_malicious_from_activity(
    contract_address: &str,
    activity: &PreparedContractActivity<'_>,
    sale_metrics_by_tx: &BTreeMap<String, SaleMetricRecord>,
    malicious_addresses: &[MaliciousAddressPayload],
) -> Vec<SecondarySaleVictimAddressPayload> {
    let malicious_buyers: HashSet<String> = malicious_addresses
        .iter()
        .map(|item| item.address.trim().to_lowercase())
        .filter(|value| !value.is_empty())
        .collect();
    let mut grouped: BTreeMap<String, SecondarySaleVictimAddressPayload> = BTreeMap::new();
    let mut last_buy_key: HashMap<String, (i64, i64, i64, String)> = HashMap::new();

    for sale in &activity.sorted_sales {
        let buyer_address = sale.buyer_address.trim().to_lowercase();
        if buyer_address.is_empty() || malicious_buyers.contains(&buyer_address) {
            continue;
        }
        let metric_key = sale_metric_key(&sale.tx_hash, &sale.buyer_address);
        let metrics = sale_metrics_by_tx
            .get(&metric_key)
            .or_else(|| sale_metrics_by_tx.get(&sale.tx_hash))
            .cloned()
            .unwrap_or_default();
        let later_transfer_out = activity
            .latest_outgoing
            .get(&(sale.buyer_address.clone(), sale.token_id.clone()))
            .map(|transfer_key| {
                transfer_key > &(sale.block_number, sale.log_index, sale.tx_hash.clone())
            })
            .unwrap_or(false);
        let is_stuck = activity
            .owner_token_map
            .get(&sale.buyer_address)
            .map(|held_tokens| held_tokens.contains(&sale.token_id))
            .unwrap_or(false)
            && !later_transfer_out;

        let entry = grouped
            .entry(sale.buyer_address.clone())
            .or_insert_with(|| SecondarySaleVictimAddressPayload {
                contract_address: contract_address.to_string(),
                address: sale.buyer_address.clone(),
                ratio_status: "unavailable".into(),
                ..SecondarySaleVictimAddressPayload::default()
            });
        entry.buy_tx_hashes.push(sale.tx_hash.clone());
        if sale.price_eth.is_some() {
            entry.buy_amount_eth += sale.price_eth.unwrap_or(0.0);
        }
        if let Some(amount_usd) = sale_usd_value(sale) {
            entry.buy_amount_usd += amount_usd;
        }
        let current_key = (
            sale.block_number,
            sale.log_index,
            sale.bundle_index,
            sale.tx_hash.clone(),
        );
        let should_update_last = last_buy_key
            .get(&sale.buyer_address)
            .map(|existing| &current_key >= existing)
            .unwrap_or(true);
        if should_update_last {
            last_buy_key.insert(sale.buyer_address.clone(), current_key);
            entry.last_buy_tx_hash = sale.tx_hash.clone();
            entry.last_buy_amount_eth = sale.price_eth;
            entry.last_buy_amount_usd = sale_usd_value(sale);
            entry.buy_before_eth_balance = metrics.buy_before_eth_balance;
            entry.buy_before_usd_balance = metrics.buy_before_usd_balance;
            entry.buy_asset_ratio = metrics.buy_asset_ratio;
            entry.buy_asset_ratio_with_gas = metrics.buy_asset_ratio_with_gas;
            entry.ratio_status = if metrics.ratio_status.is_empty() {
                "unavailable".into()
            } else {
                metrics.ratio_status
            };
        }
        entry.is_stuck = entry.is_stuck || is_stuck;
    }

    grouped.into_values().collect()
}

pub struct HonestAddressRecordInput<'a> {
    pub contract_address: &'a str,
    pub transfers: &'a [TransferRecord],
    pub sales: &'a [NftSaleRecord],
    pub owners: &'a [OwnerBalance],
    pub infringing_tokens: &'a [InfringingTokenRecord],
    pub malicious_addresses: &'a [MaliciousAddressPayload],
    pub mint_payment_edges: &'a [ValueFlowEdgePayload],
    pub deployment_time: i64,
    pub analysis_timestamp: i64,
}

pub fn build_honest_address_records(
    input: HonestAddressRecordInput<'_>,
) -> Vec<HonestAddressPayload> {
    let HonestAddressRecordInput {
        contract_address,
        transfers,
        sales,
        owners,
        infringing_tokens,
        malicious_addresses,
        mint_payment_edges,
        deployment_time,
        analysis_timestamp,
    } = input;
    let activity = prepare_contract_activity(transfers, sales, owners);
    build_honest_address_records_from_activity(
        contract_address,
        &activity,
        infringing_tokens,
        malicious_addresses,
        mint_payment_edges,
        deployment_time,
        analysis_timestamp,
    )
}

pub(crate) fn build_honest_address_records_from_activity(
    contract_address: &str,
    activity: &PreparedContractActivity<'_>,
    infringing_tokens: &[InfringingTokenRecord],
    malicious_addresses: &[MaliciousAddressPayload],
    mint_payment_edges: &[ValueFlowEdgePayload],
    deployment_time: i64,
    analysis_timestamp: i64,
) -> Vec<HonestAddressPayload> {
    let cutoff_time = analysis_timestamp.max(0);
    let deployment_time = deployment_time.max(0);
    let relevant_token_ids: HashSet<String> = infringing_tokens
        .iter()
        .filter(|item| !item.token_id.is_empty())
        .map(|item| item.token_id.clone())
        .collect();
    let malicious_set: HashSet<String> = malicious_addresses
        .iter()
        .filter(|item| !item.address.is_empty())
        .map(|item| item.address.clone())
        .collect();

    let owner_token_map: HashMap<String, HashSet<String>> = activity
        .owner_token_map
        .iter()
        .filter_map(|(owner_address, held_tokens)| {
            let filtered: HashSet<String> = held_tokens
                .iter()
                .filter(|token_id| {
                    relevant_token_ids.is_empty() || relevant_token_ids.contains(*token_id)
                })
                .cloned()
                .collect();
            (!filtered.is_empty()).then(|| (owner_address.clone(), filtered))
        })
        .collect();

    let relevant_transfers: Vec<&TransferRecord> = activity
        .sorted_transfers
        .iter()
        .copied()
        .filter(|transfer| {
            relevant_token_ids.is_empty() || relevant_token_ids.contains(&transfer.token_id)
        })
        .collect();

    let relevant_sales: Vec<&NftSaleRecord> = activity
        .sorted_sales
        .iter()
        .copied()
        .filter(|sale| relevant_token_ids.is_empty() || relevant_token_ids.contains(&sale.token_id))
        .collect();

    let mut all_addresses: HashSet<String> = HashSet::new();
    let mut non_mint_transfer_participants: HashSet<String> = HashSet::new();
    let mut sale_participants: HashSet<String> = HashSet::new();
    for transfer in &relevant_transfers {
        if !transfer.from_address.is_empty() && transfer.from_address != ZERO_ADDRESS {
            all_addresses.insert(transfer.from_address.clone());
            non_mint_transfer_participants.insert(transfer.from_address.clone());
        }
        if !transfer.to_address.is_empty() && transfer.to_address != ZERO_ADDRESS {
            all_addresses.insert(transfer.to_address.clone());
            if transfer.from_address != ZERO_ADDRESS {
                non_mint_transfer_participants.insert(transfer.to_address.clone());
            }
        }
    }
    for sale in &relevant_sales {
        if !sale.buyer_address.is_empty() {
            all_addresses.insert(sale.buyer_address.clone());
            sale_participants.insert(sale.buyer_address.clone());
        }
        if !sale.seller_address.is_empty() {
            all_addresses.insert(sale.seller_address.clone());
            sale_participants.insert(sale.seller_address.clone());
        }
    }
    for address in owner_token_map.keys() {
        all_addresses.insert(address.clone());
    }

    let mut honest_addresses: Vec<String> = all_addresses
        .into_iter()
        .filter(|address| {
            !address.is_empty()
                && !malicious_set.contains(address)
                && (owner_token_map.contains_key(address)
                    || non_mint_transfer_participants.contains(address)
                    || sale_participants.contains(address))
        })
        .collect();
    honest_addresses.sort();
    let honest_set: HashSet<String> = honest_addresses.iter().cloned().collect();

    let mut transfers_by_token: HashMap<String, Vec<&TransferRecord>> = HashMap::new();
    for transfer in relevant_transfers {
        transfers_by_token
            .entry(transfer.token_id.clone())
            .or_default()
            .push(transfer);
    }

    let mut token_interactions_by_address: HashMap<String, HashSet<String>> = HashMap::new();
    let mut durations_by_address: HashMap<String, Vec<i64>> = HashMap::new();
    let mut deployment_to_neutral_holder_samples_by_address: HashMap<String, Vec<i64>> =
        HashMap::new();
    let mut victim_resale_count: HashMap<String, i64> = HashMap::new();
    let mut corrupted_addresses: HashSet<String> = HashSet::new();

    let mut paid_mint_acquisitions_by_token: HashMap<
        String,
        HashMap<String, Vec<&ValueFlowEdgePayload>>,
    > = HashMap::new();
    for edge in mint_payment_edges {
        if edge.channel != "mint_payment"
            || !edge.contract_address.eq_ignore_ascii_case(contract_address)
            || !value_flow_has_positive_value(edge)
        {
            continue;
        }
        let payer = &edge.from_address;
        if !honest_set.contains(payer) {
            continue;
        }
        for token_id in value_flow_token_ids(edge) {
            if relevant_token_ids.is_empty() || relevant_token_ids.contains(&token_id) {
                paid_mint_acquisitions_by_token
                    .entry(token_id)
                    .or_default()
                    .entry(payer.clone())
                    .or_default()
                    .push(edge);
            }
        }
    }

    let mut prior_paid_buyers_by_token: HashMap<String, HashSet<String>> = HashMap::new();
    for sale in &relevant_sales {
        if !sale_has_positive_value(sale) {
            continue;
        }
        let seller = &sale.seller_address;
        let prior_paid_buyers = prior_paid_buyers_by_token
            .entry(sale.token_id.clone())
            .or_default();
        let has_paid_mint_acquisition = paid_mint_acquisitions_by_token
            .get(&sale.token_id)
            .and_then(|by_address| by_address.get(seller))
            .map(|edges| {
                edges
                    .iter()
                    .any(|edge| value_flow_precedes_sale(edge, sale))
            })
            .unwrap_or(false);
        if honest_set.contains(seller)
            && (prior_paid_buyers.contains(seller) || has_paid_mint_acquisition)
        {
            corrupted_addresses.insert(seller.clone());
            *victim_resale_count.entry(seller.clone()).or_insert(0) += 1;
        }
        if honest_set.contains(&sale.buyer_address) {
            prior_paid_buyers.insert(sale.buyer_address.clone());
        }
    }

    for (token_id, token_transfers) in transfers_by_token {
        let mut first_honest_recorded = false;
        let mut open_holds: HashMap<String, i64> = HashMap::new();

        for transfer in &token_transfers {
            if honest_set.contains(&transfer.from_address) {
                token_interactions_by_address
                    .entry(transfer.from_address.clone())
                    .or_default()
                    .insert(token_id.clone());
                if let Some(start_time) = open_holds.remove(&transfer.from_address) {
                    if transfer.block_time >= start_time {
                        durations_by_address
                            .entry(transfer.from_address.clone())
                            .or_default()
                            .push(transfer.block_time - start_time);
                    }
                }
            }
            if honest_set.contains(&transfer.to_address) {
                token_interactions_by_address
                    .entry(transfer.to_address.clone())
                    .or_default()
                    .insert(token_id.clone());
                if transfer.block_time > 0 {
                    open_holds.insert(transfer.to_address.clone(), transfer.block_time);
                    if deployment_time > 0 && !first_honest_recorded {
                        deployment_to_neutral_holder_samples_by_address
                            .entry(transfer.to_address.clone())
                            .or_default()
                            .push((transfer.block_time - deployment_time).max(0));
                        first_honest_recorded = true;
                    }
                }
            }
        }

        for (address, start_time) in open_holds {
            if !owner_token_map
                .get(&address)
                .map(|held_tokens| held_tokens.contains(&token_id))
                .unwrap_or(false)
            {
                continue;
            }
            if cutoff_time >= start_time {
                durations_by_address
                    .entry(address)
                    .or_default()
                    .push(cutoff_time - start_time);
            }
        }
    }

    honest_addresses
        .into_iter()
        .map(|address| {
            let current_tokens = owner_token_map.get(&address).cloned().unwrap_or_default();
            let interacted_tokens = token_interactions_by_address
                .get(&address)
                .cloned()
                .unwrap_or_default();
            let mut union_tokens = interacted_tokens;
            union_tokens.extend(current_tokens.iter().cloned());
            let hold_durations = durations_by_address
                .get(&address)
                .cloned()
                .unwrap_or_default();

            HonestAddressPayload {
                contract_address: contract_address.to_string(),
                address: address.clone(),
                interacted_token_count: union_tokens.len() as i64,
                currently_holding_token_count: current_tokens.len() as i64,
                hold_duration_median_seconds: median_i64(&hold_durations),
                hold_duration_count: hold_durations.len() as i64,
                is_corrupted_address: corrupted_addresses.contains(&address),
                victim_resale_count: *victim_resale_count.get(&address).unwrap_or(&0),
                deployment_to_neutral_holder_seconds_samples:
                    deployment_to_neutral_holder_samples_by_address
                        .get(&address)
                        .cloned()
                        .unwrap_or_default(),
            }
        })
        .collect()
}

pub fn build_honest_address_stats(
    contract_address: &str,
    honest_addresses: &[HonestAddressPayload],
) -> BTreeMap<String, HonestAddressStatsPayload> {
    let corrupted_addresses: Vec<String> = honest_addresses
        .iter()
        .filter(|item| item.is_corrupted_address)
        .map(|item| item.address.clone())
        .collect();
    let holding_medians: Vec<f64> = honest_addresses
        .iter()
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

    BTreeMap::from([(
        contract_address.to_string(),
        HonestAddressStatsPayload {
            honest_address_count: honest_addresses.len() as i64,
            corrupted_address_count: corrupted_addresses.len() as i64,
            victim_resale_count: honest_addresses
                .iter()
                .map(|item| item.victim_resale_count)
                .sum(),
            median_holding_seconds: median_f64(&holding_medians),
            avg_deployment_to_neutral_holder_seconds: mean_f64(
                &deployment_to_neutral_holder_samples,
            ),
            corrupted_addresses,
        },
    )])
}

fn positive_seconds(value: i64) -> Option<f64> {
    (value > 0).then_some(value as f64)
}

pub fn build_fraud_trade_stats(
    contract_address: &str,
    sales: &[NftSaleRecord],
    secondary_sale_victim_addresses: &[SecondarySaleVictimAddressPayload],
) -> BTreeMap<String, FraudTradeStatsPayload> {
    let contract_sales: Vec<&NftSaleRecord> = sales
        .iter()
        .filter(|sale| sale.contract_address.eq_ignore_ascii_case(contract_address))
        .collect();
    let native_sales: Vec<&NftSaleRecord> = contract_sales
        .iter()
        .copied()
        .filter(|sale| sale.is_native_eth && sale.price_eth.is_some())
        .collect();
    let eth_priced_sales: Vec<&NftSaleRecord> = contract_sales
        .iter()
        .copied()
        .filter(|sale| sale.price_eth.is_some())
        .collect();
    let usd_priced_sales: Vec<&NftSaleRecord> = contract_sales
        .iter()
        .copied()
        .filter(|sale| sale_usd_value(sale).is_some())
        .collect();

    BTreeMap::from([(
        contract_address.to_string(),
        FraudTradeStatsPayload {
            unique_buyers: sales
                .iter()
                .filter(|sale| sale.contract_address.eq_ignore_ascii_case(contract_address))
                .filter(|sale| !sale.buyer_address.is_empty())
                .map(|sale| sale.buyer_address.clone())
                .collect::<BTreeSet<_>>()
                .len() as i64,
            native_eth_sale_count: Some(native_sales.len() as i64),
            native_eth_volume: Some(
                native_sales
                    .iter()
                    .map(|sale| sale.price_eth.unwrap_or(0.0))
                    .sum(),
            ),
            usd_priced_sale_count: Some(usd_priced_sales.len() as i64),
            usd_priced_volume: Some(
                usd_priced_sales
                    .iter()
                    .map(|sale| sale_usd_value(sale).unwrap_or(0.0))
                    .sum(),
            ),
            eth_priced_sale_count: Some(eth_priced_sales.len() as i64),
            eth_priced_volume: Some(
                eth_priced_sales
                    .iter()
                    .map(|sale| sale.price_eth.unwrap_or(0.0))
                    .sum(),
            ),
            stuck_wallet_count: secondary_sale_victim_addresses
                .iter()
                .filter(|item| item.is_stuck)
                .count() as i64,
            stuck_cost_eth: secondary_sale_victim_addresses
                .iter()
                .filter(|item| item.is_stuck)
                .map(|item| item.last_buy_amount_eth.unwrap_or(0.0))
                .sum(),
            stuck_cost_usd: secondary_sale_victim_addresses
                .iter()
                .filter(|item| item.is_stuck)
                .map(|item| item.last_buy_amount_usd.unwrap_or(0.0))
                .sum(),
        },
    )])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sale(symbol: &str, amount_eth_equivalent: f64) -> NftSaleRecord {
        NftSaleRecord {
            contract_address: "0xdup".into(),
            token_id: "1".into(),
            tx_hash: format!("0x{symbol}"),
            block_number: 10,
            log_index: 1,
            buyer_address: format!("0xbuyer{symbol}"),
            seller_address: "0xseller".into(),
            payment_token_symbol: symbol.into(),
            price_eth: Some(amount_eth_equivalent),
            price_usd: Some(amount_eth_equivalent),
            is_native_eth: symbol == "ETH",
            ..NftSaleRecord::default()
        }
    }

    fn transfer(
        tx_hash: &str,
        block_time: i64,
        from_address: &str,
        to_address: &str,
    ) -> TransferRecord {
        transfer_token("1", tx_hash, block_time, from_address, to_address)
    }

    fn transfer_token(
        token_id: &str,
        tx_hash: &str,
        block_time: i64,
        from_address: &str,
        to_address: &str,
    ) -> TransferRecord {
        TransferRecord {
            contract_address: "0xdup".into(),
            token_id: token_id.into(),
            tx_hash: tx_hash.into(),
            block_number: block_time,
            block_time,
            from_address: from_address.into(),
            to_address: to_address.into(),
            event_type: "erc721".into(),
            source: "test".into(),
            ..TransferRecord::default()
        }
    }

    fn infringing_token() -> InfringingTokenRecord {
        infringing_token_minted_by("0xoperator")
    }

    fn infringing_token_minted_by(minter_address: &str) -> InfringingTokenRecord {
        infringing_token_id_minted_by("1", minter_address)
    }

    fn infringing_token_id_minted_by(
        token_id: &str,
        minter_address: &str,
    ) -> InfringingTokenRecord {
        InfringingTokenRecord {
            contract_address: "0xdup".into(),
            token_id: token_id.into(),
            minter_address: minter_address.into(),
            ..InfringingTokenRecord::default()
        }
    }

    fn operator_address() -> MaliciousAddressPayload {
        MaliciousAddressPayload {
            address: "0xoperator".into(),
            mint_activity_observed: true,
            ..MaliciousAddressPayload::default()
        }
    }

    fn sale_between(
        tx_hash: &str,
        block_time: i64,
        seller_address: &str,
        buyer_address: &str,
        price_eth: f64,
    ) -> NftSaleRecord {
        sale_between_token(
            "1",
            tx_hash,
            block_time,
            seller_address,
            buyer_address,
            price_eth,
        )
    }

    fn sale_between_token(
        token_id: &str,
        tx_hash: &str,
        block_time: i64,
        seller_address: &str,
        buyer_address: &str,
        price_eth: f64,
    ) -> NftSaleRecord {
        NftSaleRecord {
            contract_address: "0xdup".into(),
            token_id: token_id.into(),
            tx_hash: tx_hash.into(),
            block_number: block_time,
            log_index: 0,
            buyer_address: buyer_address.into(),
            seller_address: seller_address.into(),
            payment_token_symbol: "ETH".into(),
            price_eth: Some(price_eth),
            price_usd: Some(price_eth),
            is_native_eth: true,
            ..NftSaleRecord::default()
        }
    }

    fn mint_payment_edge(from_address: &str) -> ValueFlowEdgePayload {
        ValueFlowEdgePayload {
            contract_address: "0xdup".into(),
            from_address: from_address.into(),
            to_address: "0xdup".into(),
            tx_hash: "0xmint".into(),
            block_number: 100,
            block_time: 100,
            token_id: "1".into(),
            value_eth: Some(0.08),
            value_usd: Some(160.0),
            channel: "mint_payment".into(),
            evidence_type: "same_tx_mint_payment".into(),
            ..ValueFlowEdgePayload::default()
        }
    }

    #[test]
    fn fraud_trade_stats_include_weth_and_stablecoin_eth_equivalent_amounts() {
        let sales = vec![sale("ETH", 1.0), sale("WETH", 2.0), sale("USDC", 0.05)];

        let stats =
            build_fraud_trade_stats("0xdup", &sales, &[] as &[SecondarySaleVictimAddressPayload]);
        let stats = &stats["0xdup"];

        assert_eq!(stats.native_eth_sale_count, Some(1));
        assert_eq!(stats.native_eth_volume, Some(1.0));
        assert_eq!(stats.eth_priced_sale_count, Some(3));
        assert_eq!(stats.eth_priced_volume, Some(3.05));
        assert_eq!(stats.usd_priced_sale_count, Some(3));
        assert_eq!(stats.usd_priced_volume, Some(3.05));
    }

    #[test]
    fn fraud_trade_stats_ignore_sales_from_other_contracts() {
        let mut matching = sale("USDC", 5.0);
        matching.contract_address = "0xdup".into();
        let mut unrelated = sale("USDC", 7.0);
        unrelated.contract_address = "0xother".into();

        let stats = build_fraud_trade_stats("0xdup", &[matching, unrelated], &[]);
        let stats = &stats["0xdup"];

        assert_eq!(stats.unique_buyers, 1);
        assert_eq!(stats.usd_priced_sale_count, Some(1));
        assert_eq!(stats.usd_priced_volume, Some(5.0));
    }

    #[test]
    fn fraud_trade_stats_do_not_count_eth_amounts_as_usd_when_rate_is_missing() {
        let mut eth_sale = sale("ETH", 1.25);
        eth_sale.price_usd = None;

        let stats = build_fraud_trade_stats("0xdup", &[eth_sale], &[]);
        let stats = &stats["0xdup"];

        assert_eq!(stats.eth_priced_sale_count, Some(1));
        assert_eq!(stats.eth_priced_volume, Some(1.25));
        assert_eq!(stats.usd_priced_sale_count, Some(0));
        assert_eq!(stats.usd_priced_volume, Some(0.0));
    }

    #[test]
    fn victim_records_do_not_count_eth_amounts_as_usd_when_rate_is_missing() {
        let mut eth_sale = sale("ETH", 1.25);
        eth_sale.price_usd = None;
        let owners = vec![OwnerBalance {
            owner_address: "0xbuyerETH".into(),
            token_balances: BTreeMap::from([("1".into(), 1)]),
        }];
        let sales = vec![eth_sale];
        let activity = prepare_contract_activity(&[], &sales, &owners);

        let victims = build_secondary_sale_victim_address_records_from_activity(
            "0xdup",
            &activity,
            &BTreeMap::new(),
        );

        assert_eq!(victims.len(), 1);
        assert_eq!(victims[0].buy_amount_eth, 1.25);
        assert_eq!(victims[0].buy_amount_usd, 0.0);
        assert_eq!(victims[0].last_buy_amount_eth, Some(1.25));
        assert_eq!(victims[0].last_buy_amount_usd, None);
    }

    #[test]
    fn victim_records_include_stablecoin_eth_equivalent_amounts() {
        let sales = vec![sale("USDT", 0.1)];
        let owners = vec![OwnerBalance {
            owner_address: "0xbuyerUSDT".into(),
            token_balances: BTreeMap::from([("1".into(), 1)]),
        }];
        let activity = prepare_contract_activity(&[], &sales, &owners);

        let victims = build_secondary_sale_victim_address_records_from_activity(
            "0xdup",
            &activity,
            &BTreeMap::new(),
        );

        assert_eq!(victims.len(), 1);
        assert_eq!(victims[0].buy_amount_eth, 0.1);
        assert_eq!(victims[0].buy_amount_usd, 0.1);
        assert_eq!(victims[0].last_buy_amount_eth, Some(0.1));
        assert_eq!(victims[0].last_buy_amount_usd, Some(0.1));
        assert!(victims[0].is_stuck);
    }

    #[test]
    fn victim_records_exclude_malicious_secondary_sale_buyers() {
        let sales = vec![
            sale_between("0xvictim_buy", 120, "0xminter", "0xvictim", 1.0),
            sale_between("0xwash_buy", 130, "0xvictim", "0xoperator", 10.0),
        ];
        let owners = vec![
            OwnerBalance {
                owner_address: "0xvictim".into(),
                token_balances: BTreeMap::from([("1".into(), 1)]),
            },
            OwnerBalance {
                owner_address: "0xoperator".into(),
                token_balances: BTreeMap::from([("1".into(), 1)]),
            },
        ];
        let activity = prepare_contract_activity(&[], &sales, &owners);

        let victims = build_secondary_sale_victim_address_records_excluding_malicious_from_activity(
            "0xdup",
            &activity,
            &BTreeMap::new(),
            &[MaliciousAddressPayload {
                address: "0xoperator".into(),
                wash_cycle_count: 1,
                ..MaliciousAddressPayload::default()
            }],
        );

        assert_eq!(victims.len(), 1);
        assert_eq!(victims[0].address, "0xvictim");
        assert_eq!(victims[0].buy_amount_eth, 1.0);
    }

    #[test]
    fn honest_records_do_not_mark_free_transfer_after_purchase_as_corrupted() {
        let transfers = vec![
            transfer("0xmint", 100, ZERO_ADDRESS, "0xoperator"),
            transfer("0xbuy", 120, "0xoperator", "0xvictim"),
            transfer("0xgift", 140, "0xvictim", "0xrecipient"),
        ];
        let sales = vec![sale_between("0xbuy", 120, "0xoperator", "0xvictim", 1.0)];
        let owners = vec![OwnerBalance {
            owner_address: "0xrecipient".into(),
            token_balances: BTreeMap::from([("1".into(), 1)]),
        }];

        let rows = build_honest_address_records(HonestAddressRecordInput {
            contract_address: "0xdup",
            transfers: &transfers,
            sales: &sales,
            owners: &owners,
            infringing_tokens: &[infringing_token()],
            malicious_addresses: &[operator_address()],
            mint_payment_edges: &[],
            deployment_time: 90,
            analysis_timestamp: 200,
        });
        let victim = rows
            .iter()
            .find(|row| row.address == "0xvictim")
            .expect("victim row");

        assert!(!victim.is_corrupted_address);
        assert_eq!(victim.victim_resale_count, 0);
        assert_eq!(
            build_honest_address_stats("0xdup", &rows)["0xdup"].corrupted_address_count,
            0
        );
    }

    #[test]
    fn honest_records_mark_paid_resale_after_purchase_as_corrupted() {
        let transfers = vec![
            transfer("0xmint", 100, ZERO_ADDRESS, "0xoperator"),
            transfer("0xbuy", 120, "0xoperator", "0xvictim"),
            transfer("0xresale", 140, "0xvictim", "0xrecipient"),
        ];
        let sales = vec![
            sale_between("0xbuy", 120, "0xoperator", "0xvictim", 1.0),
            sale_between("0xresale", 140, "0xvictim", "0xrecipient", 0.4),
        ];
        let owners = vec![OwnerBalance {
            owner_address: "0xrecipient".into(),
            token_balances: BTreeMap::from([("1".into(), 1)]),
        }];

        let rows = build_honest_address_records(HonestAddressRecordInput {
            contract_address: "0xdup",
            transfers: &transfers,
            sales: &sales,
            owners: &owners,
            infringing_tokens: &[infringing_token()],
            malicious_addresses: &[operator_address()],
            mint_payment_edges: &[],
            deployment_time: 90,
            analysis_timestamp: 200,
        });
        let victim = rows
            .iter()
            .find(|row| row.address == "0xvictim")
            .expect("victim row");

        assert!(victim.is_corrupted_address);
        assert_eq!(victim.victim_resale_count, 1);
        assert_eq!(
            build_honest_address_stats("0xdup", &rows)["0xdup"].corrupted_address_count,
            1
        );
    }

    #[test]
    fn honest_records_mark_paid_mint_resale_as_corrupted() {
        let transfers = vec![
            transfer("0xmint", 100, ZERO_ADDRESS, "0xvictim"),
            transfer("0xresale", 140, "0xvictim", "0xrecipient"),
        ];
        let sales = vec![sale_between(
            "0xresale",
            140,
            "0xvictim",
            "0xrecipient",
            0.4,
        )];
        let owners = vec![OwnerBalance {
            owner_address: "0xrecipient".into(),
            token_balances: BTreeMap::from([("1".into(), 1)]),
        }];

        let rows = build_honest_address_records(HonestAddressRecordInput {
            contract_address: "0xdup",
            transfers: &transfers,
            sales: &sales,
            owners: &owners,
            infringing_tokens: &[infringing_token()],
            malicious_addresses: &[operator_address()],
            mint_payment_edges: &[mint_payment_edge("0xvictim")],
            deployment_time: 90,
            analysis_timestamp: 200,
        });
        let victim = rows
            .iter()
            .find(|row| row.address == "0xvictim")
            .expect("paid minter resale row");

        assert!(victim.is_corrupted_address);
        assert_eq!(victim.victim_resale_count, 1);
    }

    #[test]
    fn paid_mint_reseller_is_not_malicious_when_only_weak_mint_sale_behavior() {
        let transfers = vec![
            transfer("0xmint", 100, ZERO_ADDRESS, "0xvictim"),
            transfer("0xresale", 140, "0xvictim", "0xrecipient"),
        ];
        let sales = vec![sale_between(
            "0xresale",
            140,
            "0xvictim",
            "0xrecipient",
            0.4,
        )];
        let owners = vec![OwnerBalance {
            owner_address: "0xrecipient".into(),
            token_balances: BTreeMap::from([("1".into(), 1)]),
        }];
        let mint_payment_edges = vec![mint_payment_edge("0xvictim")];
        let infringing_tokens = vec![infringing_token_minted_by("0xvictim")];
        let activity = prepare_contract_activity(&transfers, &sales, &owners);

        let malicious = build_malicious_address_records_from_activity(
            "0xdup",
            &activity,
            &infringing_tokens,
            &mint_payment_edges,
        );

        assert!(malicious.iter().all(|row| row.address != "0xvictim"));
        let honest = build_honest_address_records_from_activity(
            "0xdup",
            &activity,
            &infringing_tokens,
            &malicious,
            &mint_payment_edges,
            90,
            200,
        );
        let victim = honest
            .iter()
            .find(|row| row.address == "0xvictim")
            .expect("paid mint reseller remains honest");
        assert!(victim.is_corrupted_address);
    }

    #[test]
    fn secondary_market_reseller_is_not_malicious_when_only_weak_rapid_sale_behavior() {
        let transfers = vec![
            transfer("0xmint", 100, ZERO_ADDRESS, "0xoperator"),
            transfer("0xbuy", 110, "0xoperator", "0xvictim"),
            transfer("0xresale", 120, "0xvictim", "0xrecipient"),
        ];
        let sales = vec![
            sale_between("0xbuy", 110, "0xoperator", "0xvictim", 1.0),
            sale_between("0xresale", 120, "0xvictim", "0xrecipient", 0.4),
        ];
        let owners = vec![OwnerBalance {
            owner_address: "0xrecipient".into(),
            token_balances: BTreeMap::from([("1".into(), 1)]),
        }];
        let infringing_tokens = vec![infringing_token()];
        let activity = prepare_contract_activity(&transfers, &sales, &owners);

        let malicious = build_malicious_address_records_from_activity(
            "0xdup",
            &activity,
            &infringing_tokens,
            &[],
        );

        assert!(malicious.iter().all(|row| row.address != "0xvictim"));
        let honest = build_honest_address_records_from_activity(
            "0xdup",
            &activity,
            &infringing_tokens,
            &malicious,
            &[],
            90,
            200,
        );
        let victim = honest
            .iter()
            .find(|row| row.address == "0xvictim")
            .expect("secondary market reseller remains honest");
        assert!(victim.is_corrupted_address);
    }

    #[test]
    fn paid_mint_high_volume_reseller_remains_malicious() {
        let transfers = vec![
            transfer("0xmint1", 100, ZERO_ADDRESS, "0xoperator"),
            transfer("0xresale1", 120, "0xoperator", "0xrecipient1"),
            transfer("0xresale2", 130, "0xoperator", "0xrecipient2"),
            transfer("0xresale3", 140, "0xoperator", "0xrecipient3"),
        ];
        let sales = vec![
            sale_between("0xresale1", 120, "0xoperator", "0xrecipient1", 0.4),
            sale_between("0xresale2", 130, "0xoperator", "0xrecipient2", 0.4),
            sale_between("0xresale3", 140, "0xoperator", "0xrecipient3", 0.4),
        ];
        let owners = Vec::<OwnerBalance>::new();
        let mint_payment_edges = vec![mint_payment_edge("0xoperator")];
        let infringing_tokens = vec![infringing_token_minted_by("0xoperator")];
        let activity = prepare_contract_activity(&transfers, &sales, &owners);

        let malicious = build_malicious_address_records_from_activity(
            "0xdup",
            &activity,
            &infringing_tokens,
            &mint_payment_edges,
        );

        assert!(malicious.iter().any(|row| row.address == "0xoperator"));
    }

    #[test]
    fn aggregation_receiver_is_malicious_signal() {
        let transfers = vec![
            transfer_token("1", "0xmint1", 100, ZERO_ADDRESS, "0xsource1"),
            transfer_token("2", "0xmint2", 100, ZERO_ADDRESS, "0xsource2"),
            transfer_token("3", "0xmint3", 100, ZERO_ADDRESS, "0xsource3"),
            transfer_token("1", "0xagg1", 120, "0xsource1", "0xcollector"),
            transfer_token("2", "0xagg2", 121, "0xsource2", "0xcollector"),
            transfer_token("3", "0xagg3", 122, "0xsource3", "0xcollector"),
        ];
        let infringing_tokens = vec![
            infringing_token_id_minted_by("1", "0xsource1"),
            infringing_token_id_minted_by("2", "0xsource2"),
            infringing_token_id_minted_by("3", "0xsource3"),
        ];
        let activity = prepare_contract_activity(&transfers, &[], &[]);

        let malicious = build_malicious_address_records_from_activity(
            "0xdup",
            &activity,
            &infringing_tokens,
            &[],
        );

        assert!(malicious.iter().all(|row| row.address != "0xcollector"));
    }

    #[test]
    fn wash_cycle_operator_levels_use_count_and_value_thresholds() {
        let low_transfers = vec![
            transfer("0xmint", 100, ZERO_ADDRESS, "0xoperator"),
            transfer("0xout", 110, "0xoperator", "0xpeer"),
            transfer("0xback", 120, "0xpeer", "0xoperator"),
        ];
        let low_sales = vec![sale_between("0xout", 115, "0xoperator", "0xpeer", 0.1)];
        let low_activity = prepare_contract_activity(&low_transfers, &low_sales, &[]);

        let low_rows = build_malicious_address_records_from_activity(
            "0xdup",
            &low_activity,
            &[infringing_token()],
            &[],
        );
        let low_operator = low_rows
            .iter()
            .find(|row| row.address == "0xoperator")
            .expect("low wash-cycle operator");
        assert_eq!(low_operator.operator_level, 1);
        assert_eq!(
            low_operator.operator_level_label,
            "weak_behavioral_operator"
        );
        assert_eq!(low_operator.wash_cycle_propagation_count, 1);

        let unrelated_value_edges = vec![ValueFlowEdgePayload {
            contract_address: "0xdup".into(),
            from_address: "0xoperator".into(),
            to_address: "0xdup".into(),
            tx_hash: "0xunrelated_mint_payment".into(),
            token_id: "1".into(),
            value_eth: Some(2.0),
            value_usd: Some(2_000.0),
            channel: "mint_payment".into(),
            ..ValueFlowEdgePayload::default()
        }];
        let unrelated_value_rows = build_malicious_address_records_from_activity(
            "0xdup",
            &low_activity,
            &[infringing_token()],
            &unrelated_value_edges,
        );
        let unrelated_value_operator = unrelated_value_rows
            .iter()
            .find(|row| row.address == "0xoperator")
            .expect("wash-cycle operator with unrelated value flow");
        assert_eq!(unrelated_value_operator.operator_level, 1);
        assert_eq!(unrelated_value_operator.wash_cycle_value_usd, 0.1);

        let count_transfers = vec![
            transfer_token("1", "0xmint1", 100, ZERO_ADDRESS, "0xoperator"),
            transfer_token("1", "0xout1", 110, "0xoperator", "0xpeer1"),
            transfer_token("1", "0xback1", 120, "0xpeer1", "0xoperator"),
            transfer_token("2", "0xmint2", 100, ZERO_ADDRESS, "0xoperator"),
            transfer_token("2", "0xout2", 130, "0xoperator", "0xpeer2"),
            transfer_token("2", "0xback2", 140, "0xpeer2", "0xoperator"),
            transfer_token("3", "0xmint3", 100, ZERO_ADDRESS, "0xoperator"),
            transfer_token("3", "0xout3", 150, "0xoperator", "0xpeer3"),
            transfer_token("3", "0xback3", 160, "0xpeer3", "0xoperator"),
        ];
        let count_activity = prepare_contract_activity(&count_transfers, &[], &[]);
        let count_rows = build_malicious_address_records_from_activity(
            "0xdup",
            &count_activity,
            &[
                infringing_token_id_minted_by("1", "0xoperator"),
                infringing_token_id_minted_by("2", "0xoperator"),
                infringing_token_id_minted_by("3", "0xoperator"),
            ],
            &[],
        );
        let count_operator = count_rows
            .iter()
            .find(|row| row.address == "0xoperator")
            .expect("count-threshold wash-cycle operator");
        assert_eq!(count_operator.operator_level, 2);
        assert_eq!(
            count_operator.operator_level_label,
            "likely_behavioral_operator"
        );
        assert_eq!(count_operator.wash_cycle_propagation_count, 3);

        let value_transfers = vec![
            transfer("0xmint", 100, ZERO_ADDRESS, "0xoperator"),
            transfer("0xout", 110, "0xoperator", "0xpeer"),
            transfer("0xback", 120, "0xpeer", "0xoperator"),
        ];
        let value_sales = vec![sale_between("0xout", 115, "0xoperator", "0xpeer", 1_200.0)];
        let value_activity = prepare_contract_activity(&value_transfers, &value_sales, &[]);
        let value_rows = build_malicious_address_records_from_activity(
            "0xdup",
            &value_activity,
            &[infringing_token()],
            &[],
        );
        let value_operator = value_rows
            .iter()
            .find(|row| row.address == "0xoperator")
            .expect("value-threshold wash-cycle operator");
        assert_eq!(value_operator.operator_level, 2);
        assert_eq!(value_operator.wash_cycle_value_usd, 1_200.0);

        let eth_fallback_transfers = vec![
            transfer("0xmint", 100, ZERO_ADDRESS, "0xoperator"),
            transfer("0xout", 110, "0xoperator", "0xpeer"),
            transfer("0xback", 120, "0xpeer", "0xoperator"),
        ];
        let mut eth_fallback_sale = sale_between("0xout", 115, "0xoperator", "0xpeer", 0.6);
        eth_fallback_sale.price_usd = None;
        let eth_fallback_sales = vec![eth_fallback_sale];
        let eth_fallback_activity =
            prepare_contract_activity(&eth_fallback_transfers, &eth_fallback_sales, &[]);
        let eth_fallback_rows = build_malicious_address_records_from_activity(
            "0xdup",
            &eth_fallback_activity,
            &[infringing_token()],
            &[],
        );
        let eth_fallback_operator = eth_fallback_rows
            .iter()
            .find(|row| row.address == "0xoperator")
            .expect("eth-fallback wash-cycle operator");
        assert_eq!(eth_fallback_operator.operator_level, 2);
        assert_eq!(eth_fallback_operator.wash_cycle_value_eth, 0.6);
        assert_eq!(eth_fallback_operator.wash_cycle_value_usd, 0.0);
    }

    #[test]
    fn paid_multi_purchase_buyer_is_not_aggregation_operator() {
        let transfers = vec![
            transfer_token("1", "0xmint1", 100, ZERO_ADDRESS, "0xseller1"),
            transfer_token("2", "0xmint2", 100, ZERO_ADDRESS, "0xseller2"),
            transfer_token("3", "0xmint3", 100, ZERO_ADDRESS, "0xseller3"),
            transfer_token("1", "0xbuy1", 120, "0xseller1", "0xbuyer"),
            transfer_token("2", "0xbuy2", 121, "0xseller2", "0xbuyer"),
            transfer_token("3", "0xbuy3", 122, "0xseller3", "0xbuyer"),
        ];
        let sales = vec![
            sale_between_token("1", "0xbuy1", 120, "0xseller1", "0xbuyer", 0.4),
            sale_between_token("2", "0xbuy2", 121, "0xseller2", "0xbuyer", 0.5),
            sale_between_token("3", "0xbuy3", 122, "0xseller3", "0xbuyer", 0.6),
        ];
        let infringing_tokens = vec![
            infringing_token_id_minted_by("1", "0xseller1"),
            infringing_token_id_minted_by("2", "0xseller2"),
            infringing_token_id_minted_by("3", "0xseller3"),
        ];
        let activity = prepare_contract_activity(&transfers, &sales, &[]);

        let malicious = build_malicious_address_records_from_activity(
            "0xdup",
            &activity,
            &infringing_tokens,
            &[],
        );

        assert!(malicious.iter().all(|row| row.address != "0xbuyer"));
    }

    #[test]
    fn withdrawal_recipient_is_malicious_signal() {
        let activity = prepare_contract_activity(&[], &[], &[]);
        let mint_payment_edges = vec![ValueFlowEdgePayload {
            contract_address: "0xdup".into(),
            from_address: "0xdup".into(),
            to_address: "0xoperator".into(),
            tx_hash: "0xwithdraw".into(),
            block_number: 120,
            block_time: 120,
            token_id: "1".into(),
            value_eth: Some(0.5),
            channel: "withdrawal".into(),
            to_role: "external_wallet".into(),
            evidence_flags: vec!["same_tx_contract_withdrawal".into()],
            ..ValueFlowEdgePayload::default()
        }];

        let malicious = build_malicious_address_records_from_activity(
            "0xdup",
            &activity,
            &[infringing_token()],
            &mint_payment_edges,
        );
        let operator = malicious
            .iter()
            .find(|row| row.address == "0xoperator")
            .expect("withdrawal recipient malicious signal");

        assert_eq!(operator.withdrawal_edge_count, 1);
        assert_eq!(operator.cashout_edge_count, 0);
        assert_eq!(operator.operator_level, 3);
        assert_eq!(
            operator.operator_level_label,
            "strong_value_control_operator"
        );
    }

    #[test]
    fn attribution_records_explain_withdrawal_operator_level() {
        let malicious = vec![MaliciousAddressPayload {
            address: "0xoperator".into(),
            withdrawal_edge_count: 1,
            operator_level: 3,
            operator_level_label: "strong_value_control_operator".into(),
            evidence_contracts: vec!["0xdup".into()],
            ..MaliciousAddressPayload::default()
        }];

        let rows = build_address_attribution_records("0xdup", &[], &[], &[], &malicious, &[], &[]);
        let operator = rows
            .iter()
            .find(|row| row.address == "0xoperator")
            .expect("operator attribution");

        assert_eq!(operator.attribution_label, "suspected_operator");
        assert_eq!(operator.operator_level, 3);
        assert_eq!(
            operator.operator_level_label,
            "strong_value_control_operator"
        );
        assert!(operator
            .evidence
            .iter()
            .any(|evidence| evidence.evidence_type == "contract_value_withdrawal"));
    }

    #[test]
    fn honest_records_do_not_mark_malicious_paid_mint_resale_as_corrupted() {
        let transfers = vec![
            transfer("0xmint", 100, ZERO_ADDRESS, "0xoperator"),
            transfer("0xresale", 140, "0xoperator", "0xrecipient"),
        ];
        let sales = vec![sale_between(
            "0xresale",
            140,
            "0xoperator",
            "0xrecipient",
            0.4,
        )];
        let owners = vec![OwnerBalance {
            owner_address: "0xrecipient".into(),
            token_balances: BTreeMap::from([("1".into(), 1)]),
        }];

        let rows = build_honest_address_records(HonestAddressRecordInput {
            contract_address: "0xdup",
            transfers: &transfers,
            sales: &sales,
            owners: &owners,
            infringing_tokens: &[infringing_token()],
            malicious_addresses: &[operator_address()],
            mint_payment_edges: &[mint_payment_edge("0xoperator")],
            deployment_time: 90,
            analysis_timestamp: 200,
        });

        assert!(rows.iter().all(|row| row.address != "0xoperator"));
        assert_eq!(
            build_honest_address_stats("0xdup", &rows)["0xdup"].corrupted_address_count,
            0
        );
    }

    #[test]
    fn acquisition_exposure_evidence_uses_total_victim_acquisition_ratio() {
        let rows = add_acquisition_exposure_attribution_evidence(
            vec![AddressAttributionPayload {
                contract_address: "0xdup".into(),
                address: "0xvictim".into(),
                attribution_label: "likely_victim".into(),
                victim_score: 0.45,
                confidence: "medium".into(),
                ..AddressAttributionPayload::default()
            }],
            &[VictimAcquisitionAddressPayload {
                address: "0xvictim".into(),
                contract_addresses: vec!["0xdup".into()],
                tx_hashes: vec!["0xbuy".into(), "0xmint".into()],
                buy_asset_ratio: Some(0.7),
                ..VictimAcquisitionAddressPayload::default()
            }],
        );

        let victim = rows
            .iter()
            .find(|item| item.address == "0xvictim")
            .expect("victim attribution");
        assert!(victim
            .observed_roles
            .iter()
            .any(|role| role == "high_exposure_acquirer"));
        assert!(victim
            .evidence
            .iter()
            .any(|evidence| evidence.evidence_type == "high_acquisition_balance_ratio"));
    }
}
