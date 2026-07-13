use super::merge::{add_attacker_cost_payload, merge_f64_map};
use super::*;

#[derive(Default)]
pub(super) struct AttackerCostBuild {
    pub(super) payload: PaperAttackerCostPayload,
    pub(super) details: Vec<PaperAttackerCostDetailPayload>,
    pub(super) by_contract_usd: BTreeMap<String, f64>,
}

struct AttackerCostCandidate {
    stage: AttackerCostStage,
    detail: PaperAttackerCostDetailPayload,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum AttackerCostStage {
    Setup,
    Lure,
    Exit,
}

#[derive(Default)]
pub(super) struct OutputInputRatioBuild {
    pub(super) summary: PaperOutputInputSummaryPayload,
    pub(super) rows: Vec<PaperOutputInputRatioRowPayload>,
}

pub(super) fn explicit_malicious_address_set(
    malicious_addresses: &[MaliciousAddressPayload],
) -> BTreeSet<String> {
    malicious_addresses
        .iter()
        .map(|item| normalized_address(&item.address))
        .filter(|address| is_participant_address(address))
        .collect()
}

pub(super) fn build_attacker_cost(
    config: PaperStatsConfig,
    value_flow_edges: &[ValueFlowEdgePayload],
    address_sets: &AddressSets,
    explicit_malicious_addresses: &BTreeSet<String>,
    contribution_contract_count: usize,
) -> AttackerCostBuild {
    let mut candidates = BTreeMap::<(String, String), AttackerCostCandidate>::new();
    for edge in value_flow_edges {
        let gas_eth = edge.gas_eth.unwrap_or_else(|| {
            edge.value_with_gas_eth.unwrap_or_default() - edge.value_eth.unwrap_or_default()
        });
        let gas_usd = edge.gas_usd.unwrap_or_else(|| {
            edge.value_with_gas_usd.unwrap_or_default() - edge.value_usd.unwrap_or_default()
        });
        let gas_eth = gas_eth.max(0.0);
        let gas_usd = gas_usd.max(0.0);
        let Some(stage) = attacker_cost_stage(edge, address_sets, explicit_malicious_addresses)
        else {
            continue;
        };
        let contract = normalized_contract(&edge.contract_address);
        let detail = PaperAttackerCostDetailPayload {
            contract_address: contract.clone(),
            stage: attacker_cost_stage_label(stage).into(),
            channel: edge.channel.clone(),
            tx_hash: normalize_chain_identity(&edge.tx_hash),
            gas_payer_address: attacker_cost_payer(edge),
            gas_eth,
            gas_usd,
            from_role: edge.from_role.clone(),
            to_role: edge.to_role.clone(),
            evidence_type: edge.evidence_type.clone(),
        };
        let key = attacker_cost_edge_key(&contract, edge, stage, gas_eth, gas_usd);
        merge_attacker_cost_candidate(
            &mut candidates,
            key,
            AttackerCostCandidate { stage, detail },
        );
    }
    finalize_attacker_cost_build(config, candidates, contribution_contract_count)
}

pub(super) fn build_attacker_cost_from_details(
    config: PaperStatsConfig,
    details: &[PaperAttackerCostDetailPayload],
    contribution_contract_count: usize,
) -> AttackerCostBuild {
    let mut candidates = BTreeMap::<(String, String), AttackerCostCandidate>::new();
    for detail in details {
        let Some((key, candidate)) = attacker_cost_candidate_from_detail(detail) else {
            continue;
        };
        merge_attacker_cost_candidate(&mut candidates, key, candidate);
    }
    finalize_attacker_cost_build(config, candidates, contribution_contract_count)
}

fn finalize_attacker_cost_build(
    config: PaperStatsConfig,
    candidates: BTreeMap<(String, String), AttackerCostCandidate>,
    contribution_contract_count: usize,
) -> AttackerCostBuild {
    let mut build = AttackerCostBuild::default();
    for candidate in candidates.into_values() {
        let gas_eth = candidate.detail.gas_eth.max(0.0);
        let gas_usd = candidate.detail.gas_usd.max(0.0);
        match candidate.stage {
            AttackerCostStage::Setup => {
                build.payload.setup_gas_eth += gas_eth;
                build.payload.setup_gas_usd += gas_usd;
            }
            AttackerCostStage::Lure => {
                build.payload.lure_gas_eth += gas_eth;
                build.payload.lure_gas_usd += gas_usd;
            }
            AttackerCostStage::Exit => {
                build.payload.exit_gas_eth += gas_eth;
                build.payload.exit_gas_usd += gas_usd;
            }
        }
        if gas_usd > 0.0 {
            *build
                .by_contract_usd
                .entry(candidate.detail.contract_address.clone())
                .or_default() += gas_usd;
        }
        if gas_eth > 0.0 || gas_usd > 0.0 {
            build.details.push(candidate.detail);
        }
    }
    build.payload.total_gas_eth =
        build.payload.setup_gas_eth + build.payload.lure_gas_eth + build.payload.exit_gas_eth;
    build.payload.total_gas_usd =
        build.payload.setup_gas_usd + build.payload.lure_gas_usd + build.payload.exit_gas_usd;
    build.payload.top_contract_contribution_denominator = build.payload.total_gas_usd;
    build.payload.top_contract_contribution_numerator =
        top_contribution_numerator(&build.by_contract_usd, config, contribution_contract_count);
    build.payload.top_contract_contribution_ratio = ratio_f64(
        build.payload.top_contract_contribution_numerator,
        build.payload.top_contract_contribution_denominator,
    );
    sort_attacker_cost_details(&mut build.details);
    build
}

pub(super) fn add_legacy_attacker_cost_summary(
    build: &mut AttackerCostBuild,
    legacy_payload: &PaperAttackerCostPayload,
    legacy_by_contract_usd: &BTreeMap<String, f64>,
    config: PaperStatsConfig,
    contribution_contract_count: usize,
) {
    add_attacker_cost_payload(&mut build.payload, legacy_payload);
    merge_f64_map(&mut build.by_contract_usd, legacy_by_contract_usd);
    build.payload.top_contract_contribution_denominator = build.payload.total_gas_usd;
    build.payload.top_contract_contribution_numerator =
        top_contribution_numerator(&build.by_contract_usd, config, contribution_contract_count);
    build.payload.top_contract_contribution_ratio = ratio_f64(
        build.payload.top_contract_contribution_numerator,
        build.payload.top_contract_contribution_denominator,
    );
}

pub(super) fn build_operator_output_by_contract(
    value_flow_edges: &[ValueFlowEdgePayload],
) -> BTreeMap<String, f64> {
    let mut by_edge = BTreeMap::<(String, String), f64>::new();
    for edge in value_flow_edges {
        let contract = normalized_contract(&edge.contract_address);
        if contract == "unknown" {
            continue;
        }
        let Some(output_usd) = operator_output_usd(edge) else {
            continue;
        };
        let key = operator_output_edge_key(&contract, edge, output_usd);
        by_edge
            .entry(key)
            .and_modify(|existing| *existing = existing.max(output_usd))
            .or_insert(output_usd);
    }

    let mut by_contract = BTreeMap::<String, f64>::new();
    for ((contract, _), output_usd) in by_edge {
        *by_contract.entry(contract).or_default() += output_usd;
    }
    by_contract
}

fn operator_output_usd(edge: &ValueFlowEdgePayload) -> Option<f64> {
    if !is_operator_output_edge(edge) {
        return None;
    }
    let output_usd = edge
        .value_usd
        .filter(|value| *value > 0.0)
        .or_else(|| {
            edge.value_eth
                .filter(|value| *value > 0.0)
                .map(|value| value * FALLBACK_ETH_USD_RATE)
        })
        .unwrap_or_default()
        .max(0.0);
    (output_usd > 0.0).then_some(output_usd)
}

fn is_operator_output_edge(edge: &ValueFlowEdgePayload) -> bool {
    match edge.channel.as_str() {
        "mint_payment" => is_operator_output_recipient(edge),
        "sale_payment" | "royalty_fee" => {
            edge.recipient_known && is_operator_output_recipient(edge)
        }
        "exit_payment" => is_operator_output_sender(edge),
        _ => false,
    }
}

fn is_operator_output_recipient(edge: &ValueFlowEdgePayload) -> bool {
    normalized_address(&edge.to_address) == normalized_contract(&edge.contract_address)
        || is_operator_revenue_role(&edge.to_role)
}

fn is_operator_output_sender(edge: &ValueFlowEdgePayload) -> bool {
    is_operator_revenue_role(&edge.from_role)
}

fn is_operator_revenue_role(role: &str) -> bool {
    matches!(
        role,
        "attacker"
            | "malicious"
            | "operator_wallet"
            | "contract_deployer"
            | "contract_owner"
            | "contract_admin"
            | "proxy_admin"
            | "mint_contract"
    )
}

fn operator_output_edge_key(
    contract: &str,
    edge: &ValueFlowEdgePayload,
    output_usd: f64,
) -> (String, String) {
    let edge_id = edge.edge_id.trim();
    if !edge_id.is_empty() {
        return (
            contract.to_string(),
            format!("edge:{}", edge_id.to_lowercase()),
        );
    }
    let tx_hash = edge.tx_hash.trim();
    if !tx_hash.is_empty() {
        return (
            contract.to_string(),
            format!(
                "tx:{}:{}:{}:{}:{:.6}",
                edge.channel.trim().to_lowercase(),
                normalize_chain_identity(tx_hash),
                normalized_address(&edge.from_address),
                normalized_address(&edge.to_address),
                output_usd
            ),
        );
    }
    (
        contract.to_string(),
        format!(
            "synthetic:{}:{}:{}:{:.6}",
            edge.channel.trim().to_lowercase(),
            normalized_address(&edge.from_address),
            normalized_address(&edge.to_address),
            output_usd
        ),
    )
}

pub(super) fn build_output_input_ratio(
    output_by_contract_usd: &BTreeMap<String, f64>,
    input_by_contract_usd: &BTreeMap<String, f64>,
) -> OutputInputRatioBuild {
    let mut rows = output_by_contract_usd
        .iter()
        .filter_map(|(contract, output_usd)| {
            let output_usd = output_usd.max(0.0);
            if output_usd <= 0.0 {
                return None;
            }
            let input_usd = input_by_contract_usd
                .get(contract)
                .copied()
                .unwrap_or_default()
                .max(0.0);
            Some(PaperOutputInputRatioRowPayload {
                contract_address: contract.clone(),
                output_usd,
                input_usd,
                output_input_ratio: ratio_f64(output_usd, input_usd),
                output_input_ratio_numerator: output_usd,
                output_input_ratio_denominator: input_usd,
            })
        })
        .collect::<Vec<_>>();
    rows.sort_by(compare_output_input_ratio_rows);

    let total_output_usd = rows.iter().map(|row| row.output_usd).sum::<f64>();
    let total_input_usd = rows.iter().map(|row| row.input_usd).sum::<f64>();
    let ratio_denominator = rows.iter().filter(|row| row.input_usd > 0.0).count() as i64;
    let ratio_gte_one_count = rows
        .iter()
        .filter(|row| output_input_ratio_gte_one(row))
        .count() as i64;
    let ratio_lt_one_count = rows
        .iter()
        .filter(|row| output_input_ratio_lt_one(row))
        .count() as i64;

    OutputInputRatioBuild {
        summary: PaperOutputInputSummaryPayload {
            total_output_usd,
            total_input_usd,
            total_output_input_ratio: ratio_f64(total_output_usd, total_input_usd),
            total_output_input_ratio_numerator: total_output_usd,
            total_output_input_ratio_denominator: total_input_usd,
            ratio_gte_one_count,
            ratio_gte_one_ratio: ratio_i64(ratio_gte_one_count, ratio_denominator),
            ratio_gte_one_ratio_numerator: ratio_gte_one_count,
            ratio_gte_one_ratio_denominator: ratio_denominator,
            ratio_lt_one_count,
            ratio_lt_one_ratio: ratio_i64(ratio_lt_one_count, ratio_denominator),
            ratio_lt_one_ratio_numerator: ratio_lt_one_count,
            ratio_lt_one_ratio_denominator: ratio_denominator,
        },
        rows,
    }
}

fn output_input_ratio_gte_one(row: &PaperOutputInputRatioRowPayload) -> bool {
    row.input_usd > 0.0 && row.output_usd >= row.input_usd
}

fn output_input_ratio_lt_one(row: &PaperOutputInputRatioRowPayload) -> bool {
    row.input_usd > 0.0 && row.output_usd < row.input_usd
}

fn compare_output_input_ratio_rows(
    left: &PaperOutputInputRatioRowPayload,
    right: &PaperOutputInputRatioRowPayload,
) -> Ordering {
    compare_f64_desc(
        output_input_sort_ratio(left),
        output_input_sort_ratio(right),
    )
    .then_with(|| compare_f64_desc(left.output_usd, right.output_usd))
    .then_with(|| compare_f64_desc(left.input_usd, right.input_usd))
    .then_with(|| left.contract_address.cmp(&right.contract_address))
}

fn output_input_sort_ratio(row: &PaperOutputInputRatioRowPayload) -> f64 {
    row.output_input_ratio.unwrap_or({
        if row.output_usd > 0.0 {
            f64::INFINITY
        } else {
            0.0
        }
    })
}

fn merge_attacker_cost_candidate(
    candidates: &mut BTreeMap<(String, String), AttackerCostCandidate>,
    key: (String, String),
    mut candidate: AttackerCostCandidate,
) {
    if let Some(existing) = candidates.get_mut(&key) {
        let gas_eth = existing.detail.gas_eth.max(candidate.detail.gas_eth);
        let gas_usd = existing.detail.gas_usd.max(candidate.detail.gas_usd);
        if attacker_cost_candidate_preferred(&candidate, existing) {
            candidate.detail.gas_eth = gas_eth;
            candidate.detail.gas_usd = gas_usd;
            *existing = candidate;
        } else {
            existing.detail.gas_eth = gas_eth;
            existing.detail.gas_usd = gas_usd;
        }
    } else {
        candidates.insert(key, candidate);
    }
}

fn attacker_cost_candidate_preferred(
    candidate: &AttackerCostCandidate,
    existing: &AttackerCostCandidate,
) -> bool {
    attacker_cost_stage_priority(candidate.stage)
        .cmp(&attacker_cost_stage_priority(existing.stage))
        .then_with(|| {
            candidate
                .detail
                .gas_usd
                .partial_cmp(&existing.detail.gas_usd)
                .unwrap_or(Ordering::Equal)
        })
        .then_with(|| {
            candidate
                .detail
                .gas_eth
                .partial_cmp(&existing.detail.gas_eth)
                .unwrap_or(Ordering::Equal)
        })
        == Ordering::Greater
}

fn attacker_cost_stage_priority(stage: AttackerCostStage) -> i32 {
    match stage {
        AttackerCostStage::Setup => 0,
        AttackerCostStage::Lure => 1,
        AttackerCostStage::Exit => 2,
    }
}

fn attacker_cost_edge_key(
    contract: &str,
    edge: &ValueFlowEdgePayload,
    stage: AttackerCostStage,
    gas_eth: f64,
    gas_usd: f64,
) -> (String, String) {
    let tx_hash = edge.tx_hash.trim();
    if !tx_hash.is_empty() {
        return (
            contract.to_string(),
            format!("tx:{}", normalize_chain_identity(tx_hash)),
        );
    }
    let edge_id = edge.edge_id.trim();
    if !edge_id.is_empty() {
        return (
            contract.to_string(),
            format!("edge:{}", edge_id.to_lowercase()),
        );
    }
    (
        contract.to_string(),
        format!(
            "synthetic:{}:{}:{}:{}:{}:{:.18}:{:.6}",
            attacker_cost_stage_label(stage),
            edge.channel.trim().to_lowercase(),
            normalized_address(&edge.from_address),
            normalized_address(&edge.to_address),
            attacker_cost_payer(edge),
            gas_eth,
            gas_usd
        ),
    )
}

fn attacker_cost_candidate_from_detail(
    detail: &PaperAttackerCostDetailPayload,
) -> Option<((String, String), AttackerCostCandidate)> {
    let stage = attacker_cost_stage_from_label(&detail.stage)?;
    let contract = normalized_contract(&detail.contract_address);
    let mut detail = detail.clone();
    detail.contract_address = contract.clone();
    detail.stage = attacker_cost_stage_label(stage).into();
    detail.tx_hash = normalize_chain_identity(&detail.tx_hash);
    detail.gas_payer_address = normalized_address(&detail.gas_payer_address);
    detail.gas_eth = detail.gas_eth.max(0.0);
    detail.gas_usd = detail.gas_usd.max(0.0);
    let key = attacker_cost_detail_key(&contract, &detail, stage);
    Some((key, AttackerCostCandidate { stage, detail }))
}

fn attacker_cost_detail_key(
    contract: &str,
    detail: &PaperAttackerCostDetailPayload,
    stage: AttackerCostStage,
) -> (String, String) {
    if !detail.tx_hash.trim().is_empty() {
        return (
            contract.to_string(),
            format!("tx:{}", normalize_chain_identity(&detail.tx_hash)),
        );
    }
    (
        contract.to_string(),
        format!(
            "detail:{}:{}:{}:{}:{}:{}:{:.18}:{:.6}",
            attacker_cost_stage_label(stage),
            detail.channel.trim().to_lowercase(),
            normalize_chain_identity(&detail.gas_payer_address),
            detail.from_role.trim().to_lowercase(),
            detail.to_role.trim().to_lowercase(),
            detail.evidence_type.trim().to_lowercase(),
            detail.gas_eth,
            detail.gas_usd
        ),
    )
}

fn attacker_cost_stage_from_label(stage: &str) -> Option<AttackerCostStage> {
    let stage = stage.trim().to_lowercase();
    match stage.as_str() {
        "setup" => Some(AttackerCostStage::Setup),
        "lure" => Some(AttackerCostStage::Lure),
        "exit" => Some(AttackerCostStage::Exit),
        _ => None,
    }
}

fn attacker_cost_stage_label(stage: AttackerCostStage) -> &'static str {
    match stage {
        AttackerCostStage::Setup => "setup",
        AttackerCostStage::Lure => "lure",
        AttackerCostStage::Exit => "exit",
    }
}

pub(super) fn sort_attacker_cost_details(details: &mut [PaperAttackerCostDetailPayload]) {
    details.sort_by(|left, right| {
        compare_f64_desc(left.gas_usd, right.gas_usd)
            .then_with(|| compare_f64_desc(left.gas_eth, right.gas_eth))
            .then_with(|| left.stage.cmp(&right.stage))
            .then_with(|| left.contract_address.cmp(&right.contract_address))
            .then_with(|| left.tx_hash.cmp(&right.tx_hash))
    });
}

fn compare_f64_desc(left: f64, right: f64) -> Ordering {
    right.partial_cmp(&left).unwrap_or(Ordering::Equal)
}

fn attacker_cost_stage(
    edge: &ValueFlowEdgePayload,
    address_sets: &AddressSets,
    explicit_malicious_addresses: &BTreeSet<String>,
) -> Option<AttackerCostStage> {
    match edge.channel.as_str() {
        "deployment" | "contract_deploy" => {
            is_attacker_fee_payer(edge, address_sets, explicit_malicious_addresses)
                .then_some(AttackerCostStage::Setup)
        }
        "mint_payment" => is_attacker_fee_payer(edge, address_sets, explicit_malicious_addresses)
            .then_some(AttackerCostStage::Lure),
        "funding" => is_attacker_fee_payer(edge, address_sets, explicit_malicious_addresses)
            .then_some(AttackerCostStage::Setup),
        "sale_payment" | "lure_payment" => {
            is_attacker_fee_payer(edge, address_sets, explicit_malicious_addresses)
                .then_some(AttackerCostStage::Lure)
        }
        "withdrawal" | "cashout_hop" | "exit_payment" => {
            is_attacker_fee_payer(edge, address_sets, explicit_malicious_addresses)
                .then_some(AttackerCostStage::Exit)
        }
        _ => None,
    }
}

fn is_attacker_fee_payer(
    edge: &ValueFlowEdgePayload,
    address_sets: &AddressSets,
    explicit_malicious_addresses: &BTreeSet<String>,
) -> bool {
    let payer = attacker_cost_payer(edge);
    if address_sets.honest.contains(&payer) {
        return false;
    }
    let from = normalized_address(&edge.from_address);
    explicit_malicious_addresses.contains(&payer)
        || (payer == from && is_operator_role(&edge.from_role))
}

fn attacker_cost_payer(edge: &ValueFlowEdgePayload) -> String {
    if edge.gas_payer_address.trim().is_empty() {
        normalized_address(&edge.from_address)
    } else {
        normalized_address(&edge.gas_payer_address)
    }
}

fn is_operator_role(role: &str) -> bool {
    matches!(
        role,
        "attacker"
            | "malicious"
            | "operator_wallet"
            | "contract_deployer"
            | "contract_owner"
            | "contract_admin"
            | "proxy_admin"
            | "external_funder"
            | "paid_minter"
            | "mint_contract"
            | "cashout_intermediate"
    )
}

pub(super) fn top_contribution_numerator(
    values_by_contract: &BTreeMap<String, f64>,
    config: PaperStatsConfig,
    contribution_contract_count: usize,
) -> f64 {
    let mut values = values_by_contract
        .values()
        .copied()
        .filter(|value| *value > 0.0)
        .collect::<Vec<_>>();
    values.sort_by(|left, right| right.partial_cmp(left).unwrap_or(Ordering::Equal));
    let contract_count = contribution_contract_count.max(values_by_contract.len());
    values
        .into_iter()
        .take(config.top_contract_count(contract_count))
        .sum()
}
