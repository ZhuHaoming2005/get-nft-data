use super::*;

#[derive(Default)]
pub(super) struct HonestLossAccumulator {
    pub(super) stuck_nft_count: i64,
    pub(super) stuck_nft_denominator: i64,
    pub(super) stuck_time_numerator: f64,
    pub(super) stuck_time_denominator: f64,
    pub(super) secondary_sale_loss_eth: f64,
    pub(super) secondary_sale_loss_usd: f64,
    pub(super) paid_mint_loss_eth: f64,
    pub(super) paid_mint_loss_usd: f64,
}

#[derive(Default)]
pub(super) struct HonestLossBuild {
    pub(super) payload: PaperHonestLossPayload,
    pub(super) total_loss_by_contract_usd: BTreeMap<String, f64>,
    pub(super) stuck_time_numerator_by_contract: BTreeMap<String, f64>,
    pub(super) stuck_time_denominator_by_contract: BTreeMap<String, f64>,
}

struct LossRowInput {
    stuck_nft_count: i64,
    total_nft_count: i64,
    stuck_time_numerator: f64,
    stuck_time_denominator: f64,
    secondary_sale_loss_eth: f64,
    secondary_sale_loss_usd: f64,
    paid_mint_loss_eth: f64,
    paid_mint_loss_usd: f64,
    top_loss_numerator: f64,
    top_loss_denominator: f64,
}

fn loss_row(input: LossRowInput) -> PaperHonestLossPayload {
    let total_loss_eth = input.secondary_sale_loss_eth + input.paid_mint_loss_eth;
    let total_loss_usd = input.secondary_sale_loss_usd + input.paid_mint_loss_usd;
    PaperHonestLossPayload {
        stuck_nft_count: input.stuck_nft_count,
        stuck_nft_ratio: ratio_i64(input.stuck_nft_count, input.total_nft_count),
        stuck_nft_ratio_numerator: input.stuck_nft_count,
        stuck_nft_ratio_denominator: input.total_nft_count,
        stuck_time_ratio: ratio_f64(input.stuck_time_numerator, input.stuck_time_denominator),
        stuck_time_ratio_numerator: input.stuck_time_numerator,
        stuck_time_ratio_denominator: input.stuck_time_denominator,
        secondary_sale_loss_eth: input.secondary_sale_loss_eth,
        secondary_sale_loss_usd: input.secondary_sale_loss_usd,
        paid_mint_loss_eth: input.paid_mint_loss_eth,
        paid_mint_loss_usd: input.paid_mint_loss_usd,
        total_loss_eth,
        total_loss_usd,
        top_contract_loss_contribution_numerator: input.top_loss_numerator,
        top_contract_loss_contribution_denominator: input.top_loss_denominator,
        top_contract_loss_contribution_ratio: ratio_f64(
            input.top_loss_numerator,
            input.top_loss_denominator,
        ),
    }
}

pub(super) fn build_honest_loss(
    config: PaperStatsConfig,
    address_sets: &AddressSets,
    victim_acquisition_addresses: &[VictimAcquisitionAddressPayload],
    nft_propagation_paths: &BTreeMap<String, NftPropagationPathPayload>,
    all_fake_nft_count: i64,
    contribution_contract_count: usize,
) -> HonestLossBuild {
    let secondary_total_nft_count: i64 = victim_acquisition_addresses
        .iter()
        .map(|item| item.secondary_sale_count)
        .sum();
    let secondary_stuck_nft_count: i64 = victim_acquisition_addresses
        .iter()
        .filter(|item| item.is_stuck)
        .map(|item| item.secondary_sale_count)
        .sum();
    let paid_mint_stuck_nft_count: i64 = victim_acquisition_addresses
        .iter()
        .map(|item| item.paid_mint_stuck_token_count)
        .sum();
    let paid_mint_total_nft_count: i64 = victim_acquisition_addresses
        .iter()
        .map(paid_mint_observed_token_count)
        .sum();

    let secondary_sale_loss_eth: f64 = victim_acquisition_addresses
        .iter()
        .map(|item| item.secondary_sale_stuck_cost_eth)
        .sum();
    let secondary_sale_loss_usd: f64 = victim_acquisition_addresses
        .iter()
        .map(|item| item.secondary_sale_stuck_cost_usd)
        .sum();
    let paid_mint_loss_eth: f64 = victim_acquisition_addresses
        .iter()
        .map(|item| item.paid_mint_stuck_cost_eth)
        .sum();
    let paid_mint_loss_usd: f64 = victim_acquisition_addresses
        .iter()
        .map(|item| item.paid_mint_stuck_cost_usd)
        .sum();
    let fallback_total_stuck_eth: f64 = victim_acquisition_addresses
        .iter()
        .map(|item| item.total_stuck_cost_eth)
        .sum();
    let fallback_total_stuck_usd: f64 = victim_acquisition_addresses
        .iter()
        .map(|item| item.total_stuck_cost_usd)
        .sum();

    let total_secondary_eth = secondary_sale_loss_eth;
    let total_secondary_usd = secondary_sale_loss_usd;
    let total_paid_mint_eth = if paid_mint_loss_eth > 0.0 {
        paid_mint_loss_eth
    } else {
        (fallback_total_stuck_eth - total_secondary_eth).max(0.0)
    };
    let total_paid_mint_usd = if paid_mint_loss_usd > 0.0 {
        paid_mint_loss_usd
    } else {
        (fallback_total_stuck_usd - total_secondary_usd).max(0.0)
    };
    let total_stuck_nft_count = secondary_stuck_nft_count + paid_mint_stuck_nft_count;
    let observed_victim_nft_count = secondary_total_nft_count + paid_mint_total_nft_count;
    let total_nft_count = if all_fake_nft_count > 0 {
        all_fake_nft_count
    } else {
        observed_victim_nft_count
    };

    let total_loss_by_contract_usd = loss_by_contract(victim_acquisition_addresses, |item| {
        item.total_stuck_cost_usd
    });

    let (stuck_time_numerator_by_contract, stuck_time_denominator_by_contract) =
        stuck_time_by_contract(config, address_sets, nft_propagation_paths);
    let stuck_time_numerator: f64 = stuck_time_numerator_by_contract.values().sum();
    let stuck_time_denominator: f64 = stuck_time_denominator_by_contract.values().sum();

    HonestLossBuild {
        payload: loss_row(LossRowInput {
            stuck_nft_count: total_stuck_nft_count,
            total_nft_count,
            stuck_time_numerator,
            stuck_time_denominator,
            secondary_sale_loss_eth: total_secondary_eth,
            secondary_sale_loss_usd: total_secondary_usd,
            paid_mint_loss_eth: total_paid_mint_eth,
            paid_mint_loss_usd: total_paid_mint_usd,
            top_loss_numerator: top_contribution_numerator(
                &total_loss_by_contract_usd,
                config,
                contribution_contract_count,
            ),
            top_loss_denominator: total_secondary_usd + total_paid_mint_usd,
        }),
        total_loss_by_contract_usd,
        stuck_time_numerator_by_contract,
        stuck_time_denominator_by_contract,
    }
}

fn loss_by_contract(
    victim_acquisition_addresses: &[VictimAcquisitionAddressPayload],
    loss: impl Fn(&VictimAcquisitionAddressPayload) -> f64,
) -> BTreeMap<String, f64> {
    let mut by_contract = BTreeMap::<String, f64>::new();
    for item in victim_acquisition_addresses {
        let amount = loss(item);
        if amount <= 0.0 {
            continue;
        }
        let contracts = item
            .contract_addresses
            .iter()
            .map(|contract| normalized_contract(contract))
            .collect::<BTreeSet<_>>();
        let contracts = if contracts.is_empty() {
            BTreeSet::from(["unknown".to_string()])
        } else {
            contracts
        };
        let share = amount / contracts.len() as f64;
        for contract in contracts {
            *by_contract.entry(contract).or_default() += share;
        }
    }
    by_contract
}

fn stuck_time_by_contract(
    config: PaperStatsConfig,
    address_sets: &AddressSets,
    nft_propagation_paths: &BTreeMap<String, NftPropagationPathPayload>,
) -> (BTreeMap<String, f64>, BTreeMap<String, f64>) {
    if config.analysis_timestamp <= 0 {
        return (BTreeMap::new(), BTreeMap::new());
    }
    let mut numerators = BTreeMap::<String, f64>::new();
    let mut denominators = BTreeMap::<String, f64>::new();
    for (contract_key, path) in nft_propagation_paths {
        let contract = if path.contract_address.trim().is_empty() {
            normalized_contract(contract_key)
        } else {
            normalized_contract(&path.contract_address)
        };
        let first_time = if path.summary.first_block_time > 0 {
            path.summary.first_block_time
        } else {
            path.edges
                .iter()
                .filter_map(|edge| (edge.block_time > 0).then_some(edge.block_time))
                .min()
                .unwrap_or_default()
        };
        if first_time <= 0 {
            continue;
        }
        for edge in &path.edges {
            if edge.channel != "sale" {
                continue;
            }
            let buyer = normalized_address(&edge.to_address);
            if !address_sets.honest.contains(&buyer) {
                continue;
            }
            if edge.block_time <= first_time || config.analysis_timestamp <= edge.block_time {
                continue;
            }
            *numerators.entry(contract.clone()).or_default() +=
                (config.analysis_timestamp - edge.block_time) as f64;
            *denominators.entry(contract.clone()).or_default() +=
                (edge.block_time - first_time) as f64;
        }
    }
    (numerators, denominators)
}
