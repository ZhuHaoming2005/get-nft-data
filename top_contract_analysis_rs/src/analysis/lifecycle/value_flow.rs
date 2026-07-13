use super::evidence::has_strong_operator_address_evidence;
use super::*;

pub(super) const VALUE_FLOW_COVERAGE_SCOPE: &str =
    "same_block_native_eth_and_stablecoin_erc20_with_value_constrained_cashout";
const VALUE_FLOW_COVERAGE_GAPS: [&str; 3] = [
    "later_withdrawals_not_exhaustive",
    "cashout_trace_same_block_value_constrained",
    "known_cex_bridge_mixer_labels_incomplete",
];

pub(super) fn build_value_flow_edges(
    propagation_paths: &BTreeMap<String, NftPropagationPathPayload>,
    mint_payment_edges: &[ValueFlowEdgePayload],
) -> Vec<ValueFlowEdgePayload> {
    let mut rows = mint_payment_edges.to_vec();
    for path in propagation_paths.values() {
        for edge in &path.edges {
            if edge.channel != "sale" {
                continue;
            }
            rows.push(sale_value_edge(SaleValueEdgeInput {
                edge,
                channel: "sale_payment",
                to_address: &edge.from_address,
                evidence_type: "marketplace_sale",
                from_role: "buyer",
                to_role: "seller",
                recipient_known: true,
                value_eth: edge.seller_fee_eth.or(edge.price_eth),
                value_usd: edge.seller_fee_usd.or(edge.price_usd),
            }));
            if edge.protocol_fee_eth.unwrap_or(0.0) > 0.0
                || edge.protocol_fee_usd.unwrap_or(0.0) > 0.0
            {
                rows.push(sale_value_edge(SaleValueEdgeInput {
                    edge,
                    channel: "protocol_fee",
                    to_address: &unknown_value_recipient("protocol_fee", &edge.marketplace),
                    evidence_type: "marketplace_protocol_fee",
                    from_role: "buyer",
                    to_role: "marketplace_protocol",
                    recipient_known: false,
                    value_eth: edge.protocol_fee_eth,
                    value_usd: edge.protocol_fee_usd,
                }));
            }
            if edge.royalty_fee_eth.unwrap_or(0.0) > 0.0
                || edge.royalty_fee_usd.unwrap_or(0.0) > 0.0
            {
                let royalty_recipient = edge.royalty_recipient_address.trim();
                let royalty_recipient_known = !royalty_recipient.is_empty();
                let royalty_recipient_address = if royalty_recipient_known {
                    royalty_recipient.to_string()
                } else {
                    unknown_value_recipient("royalty_recipient", &edge.contract_address)
                };
                rows.push(sale_value_edge(SaleValueEdgeInput {
                    edge,
                    channel: "royalty_fee",
                    to_address: &royalty_recipient_address,
                    evidence_type: "marketplace_royalty_fee",
                    from_role: "buyer",
                    to_role: "royalty_recipient",
                    recipient_known: royalty_recipient_known,
                    value_eth: edge.royalty_fee_eth,
                    value_usd: edge.royalty_fee_usd,
                }));
            }
        }
    }
    rows.sort_by(|left, right| {
        (
            left.block_number,
            left.block_time,
            left.tx_hash.as_str(),
            left.token_id.as_str(),
        )
            .cmp(&(
                right.block_number,
                right.block_time,
                right.tx_hash.as_str(),
                right.token_id.as_str(),
            ))
    });
    rows
}

struct SaleValueEdgeInput<'a> {
    edge: &'a NftPropagationEdgePayload,
    channel: &'a str,
    to_address: &'a str,
    evidence_type: &'a str,
    from_role: &'a str,
    to_role: &'a str,
    recipient_known: bool,
    value_eth: Option<f64>,
    value_usd: Option<f64>,
}

fn sale_value_edge(input: SaleValueEdgeInput<'_>) -> ValueFlowEdgePayload {
    let SaleValueEdgeInput {
        edge,
        channel,
        to_address,
        evidence_type,
        from_role,
        to_role,
        recipient_known,
        value_eth,
        value_usd,
    } = input;
    ValueFlowEdgePayload {
        edge_id: format!("value:{}:{}", channel, edge.edge_id),
        contract_address: edge.contract_address.clone(),
        from_address: edge.to_address.clone(),
        to_address: to_address.to_string(),
        tx_hash: edge.tx_hash.clone(),
        block_number: edge.block_number,
        block_time: edge.block_time,
        token_id: edge.token_id.clone(),
        value_eth,
        value_usd,
        value_with_gas_eth: value_eth,
        value_with_gas_usd: value_usd,
        gas_payer_address: String::new(),
        gas_eth: None,
        gas_usd: None,
        from_before_eth_balance: None,
        from_before_usd_balance: None,
        payment_token_symbol: edge.payment_token_symbol.clone(),
        payment_token_address: edge.payment_token_address.clone(),
        channel: channel.into(),
        marketplace: edge.marketplace.clone(),
        evidence_type: evidence_type.into(),
        from_role: from_role.into(),
        to_role: to_role.into(),
        recipient_known,
        evidence_flags: vec![
            "secondary_sale".into(),
            channel.into(),
            evidence_type.into(),
        ],
    }
}

fn unknown_value_recipient(role: &str, scope: &str) -> String {
    format!("unknown:{role}:{scope}")
}

#[derive(Clone, Debug, Default)]
pub(super) struct ValueFlowBreakdown {
    pub(super) gross_eth: f64,
    pub(super) gross_usd: f64,
    pub(super) operator_eth: f64,
    pub(super) operator_usd: f64,
    pub(super) marketplace_fee_eth: f64,
    pub(super) marketplace_fee_usd: f64,
    pub(super) funding_amount_eth: f64,
    pub(super) funding_amount_usd: f64,
    pub(super) withdrawal_amount_eth: f64,
    pub(super) withdrawal_amount_usd: f64,
    pub(super) funding_edge_count: i64,
    pub(super) withdrawal_edge_count: i64,
    pub(super) revenue_backflow_edge_count: i64,
    pub(super) top_value_recipient_address: String,
    pub(super) top_value_recipient_eth: f64,
    pub(super) top_value_recipient_usd: f64,
    pub(super) top_value_recipient_share: Option<f64>,
}

pub(super) fn summarize_value_flows<'a>(
    edges: impl IntoIterator<Item = &'a ValueFlowEdgePayload>,
    address_evidence_features: &[AddressEvidenceFeaturePayload],
) -> ValueFlowBreakdown {
    let mut breakdown = ValueFlowBreakdown::default();
    let mut operator_recipient_eth = BTreeMap::<String, f64>::new();
    let mut operator_recipient_usd = BTreeMap::<String, f64>::new();
    let mut funding_sources = BTreeSet::<String>::new();
    let mut funding_source_edges = Vec::<String>::new();
    let mut operator_recipients = BTreeSet::<String>::new();
    let mut withdrawal_recipients = Vec::<String>::new();
    for edge in edges {
        if edge.channel == "funding" {
            breakdown.funding_edge_count += 1;
            if let Some(value) = edge.value_eth {
                breakdown.funding_amount_eth += value;
            }
            if let Some(value) = edge.value_usd {
                breakdown.funding_amount_usd += value;
            }
            if !edge.from_address.is_empty() {
                funding_sources.insert(edge.from_address.clone());
                funding_source_edges.push(edge.from_address.clone());
            }
        }
        if edge.channel == "withdrawal" {
            breakdown.withdrawal_edge_count += 1;
            if let Some(value) = edge.value_eth {
                breakdown.withdrawal_amount_eth += value;
            }
            if let Some(value) = edge.value_usd {
                breakdown.withdrawal_amount_usd += value;
            }
            if !edge.to_address.is_empty() {
                withdrawal_recipients.push(edge.to_address.clone());
            }
        }
        let operator_revenue = is_operator_revenue_edge(edge, address_evidence_features);
        if let Some(value) = edge.value_eth {
            if is_gross_revenue_edge(edge) {
                breakdown.gross_eth += value;
            }
            if edge.channel == "protocol_fee" {
                breakdown.marketplace_fee_eth += value;
            }
            if operator_revenue {
                breakdown.operator_eth += value;
                if !edge.to_address.is_empty() {
                    operator_recipients.insert(edge.to_address.clone());
                    *operator_recipient_eth
                        .entry(edge.to_address.clone())
                        .or_insert(0.0) += value;
                }
            }
        }
        if let Some(value) = edge.value_usd {
            if is_gross_revenue_edge(edge) {
                breakdown.gross_usd += value;
            }
            if edge.channel == "protocol_fee" {
                breakdown.marketplace_fee_usd += value;
            }
            if operator_revenue {
                breakdown.operator_usd += value;
                if !edge.to_address.is_empty() {
                    operator_recipients.insert(edge.to_address.clone());
                    *operator_recipient_usd
                        .entry(edge.to_address.clone())
                        .or_insert(0.0) += value;
                }
            }
        }
    }
    if let Some((address, eth)) = operator_recipient_eth
        .iter()
        .max_by(|left, right| left.1.total_cmp(right.1).then_with(|| right.0.cmp(left.0)))
    {
        breakdown.top_value_recipient_address = address.clone();
        breakdown.top_value_recipient_eth = *eth;
        breakdown.top_value_recipient_usd = operator_recipient_usd
            .get(address)
            .copied()
            .unwrap_or_default();
        breakdown.top_value_recipient_share =
            (breakdown.operator_eth > 0.0).then_some(*eth / breakdown.operator_eth);
    } else if let Some((address, usd)) = operator_recipient_usd
        .iter()
        .max_by(|left, right| left.1.total_cmp(right.1).then_with(|| right.0.cmp(left.0)))
    {
        breakdown.top_value_recipient_address = address.clone();
        breakdown.top_value_recipient_usd = *usd;
        breakdown.top_value_recipient_share =
            (breakdown.operator_usd > 0.0).then_some(*usd / breakdown.operator_usd);
    }
    let mut backflow_addresses = BTreeSet::new();
    for address in funding_source_edges
        .iter()
        .filter(|address| operator_recipients.contains(*address))
    {
        backflow_addresses.insert(address.clone());
    }
    for address in withdrawal_recipients
        .iter()
        .filter(|address| funding_sources.contains(*address))
    {
        backflow_addresses.insert(address.clone());
    }
    breakdown.revenue_backflow_edge_count = backflow_addresses.len() as i64;
    breakdown
}

fn is_gross_revenue_edge(edge: &ValueFlowEdgePayload) -> bool {
    matches!(
        edge.channel.as_str(),
        "mint_payment" | "sale_payment" | "royalty_fee" | "protocol_fee"
    )
}

fn is_operator_revenue_edge(
    edge: &ValueFlowEdgePayload,
    address_evidence_features: &[AddressEvidenceFeaturePayload],
) -> bool {
    match edge.channel.as_str() {
        "mint_payment" => {
            matches!(
                edge.to_role.as_str(),
                "mint_contract"
                    | "contract_deployer"
                    | "contract_owner"
                    | "contract_admin"
                    | "proxy_admin"
            ) || normalize_chain_identity(&edge.to_address)
                == normalize_chain_identity(&edge.contract_address)
        }
        "sale_payment" | "royalty_fee" => {
            edge.recipient_known
                && (matches!(
                    edge.to_role.as_str(),
                    "contract_deployer" | "contract_owner" | "contract_admin" | "proxy_admin"
                ) || has_strong_operator_address_evidence(
                    address_evidence_features,
                    &edge.contract_address,
                    &edge.to_address,
                ))
        }
        "withdrawal" => false,
        _ => false,
    }
}

pub(super) fn value_flow_coverage_gaps() -> Vec<String> {
    VALUE_FLOW_COVERAGE_GAPS
        .iter()
        .map(|value| (*value).to_string())
        .collect()
}
