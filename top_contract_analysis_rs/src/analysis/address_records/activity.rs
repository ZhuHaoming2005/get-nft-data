use super::*;

pub(crate) struct PreparedContractActivity<'a> {
    pub(super) owner_token_map: HashMap<String, HashSet<String>>,
    pub(super) sorted_transfers: Vec<&'a TransferRecord>,
    pub(super) sorted_sales: Vec<&'a NftSaleRecord>,
    pub(super) latest_outgoing: HashMap<(String, String), (i64, i64, String)>,
}

pub(super) fn transfer_sort_key(transfer: &TransferRecord) -> (i64, i64, &str) {
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

pub(super) fn median_i64(values: &[i64]) -> Option<f64> {
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

pub(super) fn sale_usd_value(sale: &NftSaleRecord) -> Option<f64> {
    sale.price_usd
}

pub(super) fn sale_has_positive_value(sale: &NftSaleRecord) -> bool {
    sale.price_eth.unwrap_or_default() > 0.0 || sale.price_usd.unwrap_or_default() > 0.0
}

pub(super) fn identities_equal(left: &str, right: &str) -> bool {
    normalize_chain_identity(left) == normalize_chain_identity(right)
}

pub(super) fn value_flow_has_positive_value(edge: &ValueFlowEdgePayload) -> bool {
    edge.value_eth.unwrap_or_default() > 0.0 || edge.value_usd.unwrap_or_default() > 0.0
}

pub(super) const WASH_CYCLE_L2_PROPAGATION_THRESHOLD: i64 = 3;
pub(super) const WASH_CYCLE_L2_VALUE_USD_THRESHOLD: f64 = 1_000.0;
pub(super) const WASH_CYCLE_L2_VALUE_ETH_THRESHOLD: f64 = 0.5;
pub(super) const STAR_DISTRIBUTION_L1_THRESHOLD: i64 = 3;
pub(super) const STAR_DISTRIBUTION_L2_THRESHOLD: i64 = 5;
pub(super) const HIGH_VOLUME_SELLER_L1_THRESHOLD: i64 = 3;
pub(super) const HIGH_VOLUME_SELLER_L2_THRESHOLD: i64 = 5;
pub(super) const RAPID_SPREAD_L2_TOKEN_THRESHOLD: usize = 2;

pub(super) struct OperatorLevelSignals {
    pub(super) wash_cycle_count: i64,
    pub(super) wash_cycle_value_eth: f64,
    pub(super) wash_cycle_value_usd: f64,
    pub(super) wash_cycle_has_usd: bool,
    pub(super) star_out_degree: i64,
    pub(super) sale_seller_count: i64,
    pub(super) rapid_spread_token_count: usize,
    pub(super) value_extraction_observed: bool,
}

#[derive(Clone, Debug, Default)]
pub(super) struct WashCycleScope {
    pub(super) token_ids: HashSet<String>,
    pub(super) tx_hashes: HashSet<String>,
    pub(super) participants: HashSet<String>,
}

#[derive(Clone, Debug)]
pub(super) struct WashCycleTransferScope {
    pub(super) token_id: String,
    pub(super) tx_hash: String,
    pub(super) from_address: String,
    pub(super) to_address: String,
}

impl WashCycleTransferScope {
    pub(super) fn from_transfer(transfer: &TransferRecord) -> Self {
        Self {
            token_id: normalize_chain_identity(&transfer.token_id),
            tx_hash: normalize_chain_identity(&transfer.tx_hash),
            from_address: normalize_chain_identity(&transfer.from_address),
            to_address: normalize_chain_identity(&transfer.to_address),
        }
    }
}

pub(super) fn add_wash_cycle_transfer_scope(
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

pub(super) fn operator_level_label(level: i64) -> &'static str {
    match level {
        1 => "weak_behavioral_operator",
        2 => "likely_behavioral_operator",
        3 => "strong_value_control_operator",
        _ => "",
    }
}

pub(super) fn classify_operator_level(signals: &OperatorLevelSignals) -> i64 {
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

pub(super) fn value_flow_token_ids(edge: &ValueFlowEdgePayload) -> Vec<String> {
    edge.token_id
        .split(',')
        .map(str::trim)
        .filter(|token_id| !token_id.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

pub(super) fn value_flow_precedes_sale(edge: &ValueFlowEdgePayload, sale: &NftSaleRecord) -> bool {
    if edge.block_number > 0 && sale.block_number > 0 {
        return edge.block_number <= sale.block_number;
    }
    false
}

pub(super) fn nft_transfer_key(
    token_id: &str,
    tx_hash: &str,
    from_address: &str,
    to_address: &str,
) -> (String, String, String, String) {
    (
        token_id.trim().to_string(),
        normalize_chain_identity(tx_hash),
        normalize_chain_identity(from_address),
        normalize_chain_identity(to_address),
    )
}

fn value_flow_scope_matches(
    edge: &ValueFlowEdgePayload,
    wash_cycle_scope: &WashCycleScope,
) -> bool {
    let tx_hash = normalize_chain_identity(&edge.tx_hash);
    if !wash_cycle_scope.tx_hashes.contains(&tx_hash) {
        return false;
    }
    value_flow_token_ids(edge)
        .iter()
        .any(|token_id| wash_cycle_scope.token_ids.contains(token_id))
        && (wash_cycle_scope
            .participants
            .contains(&normalize_chain_identity(&edge.from_address))
            || wash_cycle_scope
                .participants
                .contains(&normalize_chain_identity(&edge.to_address)))
}

fn sale_scope_matches(sale: &NftSaleRecord, wash_cycle_scope: &WashCycleScope) -> bool {
    let tx_hash = normalize_chain_identity(&sale.tx_hash);
    wash_cycle_scope.tx_hashes.contains(&tx_hash)
        && wash_cycle_scope
            .token_ids
            .contains(&normalize_chain_identity(&sale.token_id))
        && (wash_cycle_scope
            .participants
            .contains(&normalize_chain_identity(&sale.seller_address))
            || wash_cycle_scope
                .participants
                .contains(&normalize_chain_identity(&sale.buyer_address)))
}

pub(super) fn wash_cycle_value_for_address(
    address: &str,
    wash_cycle_scope: &WashCycleScope,
    activity: &PreparedContractActivity<'_>,
    value_flow_edges: &[ValueFlowEdgePayload],
) -> (f64, f64, bool) {
    let address = normalize_chain_identity(address);
    let mut value_eth = 0.0;
    let mut value_usd = 0.0;
    let mut has_usd = false;
    let mut seen_sales = HashSet::<(String, String, String, String)>::new();
    for sale in &activity.sorted_sales {
        if !sale_has_positive_value(sale)
            || !sale_scope_matches(sale, wash_cycle_scope)
            || (!identities_equal(&sale.seller_address, &address)
                && !identities_equal(&sale.buyer_address, &address))
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
            || (!identities_equal(&edge.from_address, &address)
                && !identities_equal(&edge.to_address, &address))
        {
            continue;
        }
        let key = (
            normalize_chain_identity(&edge.tx_hash),
            normalize_chain_identity(&edge.token_id),
            normalize_chain_identity(&edge.from_address),
            normalize_chain_identity(&edge.to_address),
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

pub(super) fn value_flow_operator_address(
    address: &str,
    contract_address: &str,
    role: &str,
) -> Option<String> {
    let address = address.trim();
    if address.is_empty()
        || identities_equal(address, ZERO_ADDRESS)
        || identities_equal(address, contract_address)
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
