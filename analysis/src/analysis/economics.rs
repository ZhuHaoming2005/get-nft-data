use crate::analysis::attribution::AddressRole;
use crate::model::{
    EconomicFacts, EventKind, GasCostRecord, GasEvidenceKind, GasStage, NftKey, NormalizedEvent,
    ValueChannel,
};
use ahash::{AHashMap, AHashSet};
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Default)]
struct TransactionGas {
    stage: Option<GasStage>,
    channel: Option<ValueChannel>,
    event_index: u32,
    transaction: Option<Arc<str>>,
    payer: Option<Arc<str>>,
    from_role: Option<AddressRole>,
    to_role: Option<AddressRole>,
    native: i128,
    usd_micros: i128,
}

pub struct EconomicAnalysis {
    pub facts: EconomicFacts,
    pub gas_cost_records: Vec<GasCostRecord>,
}

pub fn compute_economics(
    events: &[NormalizedEvent],
    roles: &BTreeMap<Arc<str>, AddressRole>,
    holders: &[(NftKey, Arc<str>)],
) -> EconomicFacts {
    compute_economics_detailed_iter(events.iter(), roles, holders.iter(), None, None).facts
}

pub fn compute_economics_iter<'a>(
    events: impl Iterator<Item = &'a NormalizedEvent>,
    roles: &BTreeMap<Arc<str>, AddressRole>,
    holders: impl Iterator<Item = &'a (NftKey, Arc<str>)>,
) -> EconomicFacts {
    compute_economics_detailed_iter(events, roles, holders, None, None).facts
}

pub fn compute_economics_detailed<'a>(
    events: &'a [NormalizedEvent],
    roles: &BTreeMap<Arc<str>, AddressRole>,
    holders: &'a [(NftKey, Arc<str>)],
) -> EconomicAnalysis {
    compute_economics_detailed_iter(events.iter(), roles, holders.iter(), None, None)
}

pub fn compute_economics_detailed_at<'a>(
    events: &'a [NormalizedEvent],
    roles: &BTreeMap<Arc<str>, AddressRole>,
    holders: &'a [(NftKey, Arc<str>)],
    first_activity_timestamp: Option<i64>,
    analysis_timestamp: i64,
) -> EconomicAnalysis {
    compute_economics_detailed_iter(
        events.iter(),
        roles,
        holders.iter(),
        first_activity_timestamp.map(|first| (first, analysis_timestamp)),
        None,
    )
}

pub fn compute_candidate_economics_detailed_at<'a>(
    candidate_contract: &str,
    events: &'a [NormalizedEvent],
    roles: &BTreeMap<Arc<str>, AddressRole>,
    holders: &'a [(NftKey, Arc<str>)],
    first_activity_timestamp: Option<i64>,
    analysis_timestamp: i64,
) -> EconomicAnalysis {
    compute_economics_detailed_iter(
        events.iter(),
        roles,
        holders.iter(),
        first_activity_timestamp.map(|first| (first, analysis_timestamp)),
        Some(candidate_contract),
    )
}

pub fn compute_economics_iter_at<'a>(
    events: impl Iterator<Item = &'a NormalizedEvent>,
    roles: &BTreeMap<Arc<str>, AddressRole>,
    holders: impl Iterator<Item = &'a (NftKey, Arc<str>)>,
    first_activity_timestamp: Option<i64>,
    analysis_timestamp: i64,
) -> EconomicFacts {
    compute_economics_detailed_iter(
        events,
        roles,
        holders,
        first_activity_timestamp.map(|first| (first, analysis_timestamp)),
        None,
    )
    .facts
}

pub fn compute_candidate_economics_iter_at<'a>(
    candidate_contract: &str,
    events: impl Iterator<Item = &'a NormalizedEvent>,
    roles: &BTreeMap<Arc<str>, AddressRole>,
    holders: impl Iterator<Item = &'a (NftKey, Arc<str>)>,
    first_activity_timestamp: Option<i64>,
    analysis_timestamp: i64,
) -> EconomicFacts {
    compute_economics_detailed_iter(
        events,
        roles,
        holders,
        first_activity_timestamp.map(|first| (first, analysis_timestamp)),
        Some(candidate_contract),
    )
    .facts
}

fn compute_economics_detailed_iter<'a>(
    events: impl Iterator<Item = &'a NormalizedEvent>,
    roles: &BTreeMap<Arc<str>, AddressRole>,
    holders: impl Iterator<Item = &'a (NftKey, Arc<str>)>,
    stuck_window: Option<(i64, i64)>,
    candidate_contract: Option<&str>,
) -> EconomicAnalysis {
    let operators = roles
        .iter()
        .filter(|(_, role)| {
            matches!(
                role,
                AddressRole::SuspectedOperator | AddressRole::SuspectedColluder
            )
        })
        .map(|(address, _)| address.as_ref())
        .collect::<AHashSet<_>>();
    // Honest loss only counts LikelyVictim addresses that still hold the NFT
    // they paid for (design: 诚实买家购买后仍持有). Corrupted victims and
    // unlabeled non-operator holders are excluded.
    let honest_buyers = roles
        .iter()
        .filter(|(_, role)| matches!(role, AddressRole::LikelyVictim))
        .map(|(address, _)| address.as_ref())
        .collect::<AHashSet<_>>();
    let honest_holders = holders
        .filter(|(_, address)| honest_buyers.contains(address.as_ref()))
        .map(|(nft, address)| (nft, address.as_ref()))
        .collect::<AHashSet<_>>();
    let mut output = EconomicFacts::default();
    let mut gas_by_transaction = AHashMap::<_, TransactionGas>::new();
    let mut stuck_nfts = AHashSet::<&NftKey>::new();
    for event in events {
        let channel = event.value_channel();
        let operator_paid = event
            .fee_payer
            .as_deref()
            .is_some_and(|address| operators.contains(address));
        if operator_paid {
            let stage = gas_stage(event.kind, channel);
            let transaction = gas_by_transaction
                .entry((event.chain, event.tx_id.as_ref()))
                .or_default();
            if transaction.stage.is_none_or(|current| {
                stage > current || (stage == current && event.event_index < transaction.event_index)
            }) {
                transaction.stage = Some(stage);
                transaction.channel = Some(channel);
                transaction.event_index = event.event_index;
                transaction.transaction = Some(event.tx_id.clone());
                transaction.payer = event.fee_payer.clone();
                transaction.from_role = event
                    .from
                    .as_ref()
                    .and_then(|address| roles.get(address))
                    .copied();
                transaction.to_role = event
                    .to
                    .as_ref()
                    .and_then(|address| roles.get(address))
                    .copied();
            }
            transaction.native = transaction.native.max(event.gas_native.unwrap_or(0).max(0));
            transaction.usd_micros = transaction
                .usd_micros
                .max(event.gas_usd_micros.unwrap_or(0).max(0));
        }
        let payment_recipient = event.payment_recipient.as_deref().or(match channel {
            ValueChannel::SalePayment | ValueChannel::MintPayment => event.from.as_deref(),
            ValueChannel::RoyaltyFee => None,
            _ => event.to.as_deref(),
        });
        let payment_payer = event.payment_payer.as_deref().or(event.to.as_deref());
        if matches!(
            channel,
            ValueChannel::MintPayment | ValueChannel::SalePayment
        ) {
            output.gross_revenue_native = output
                .gross_revenue_native
                .saturating_add(event.native_amount.unwrap_or(0).max(0));
            output.gross_revenue_usd_micros = output
                .gross_revenue_usd_micros
                .saturating_add(event.usd_micros.unwrap_or(0).max(0));
            output.marketplace_fee_native = output
                .marketplace_fee_native
                .saturating_add(event.marketplace_fee_native.unwrap_or(0).max(0));
            output.marketplace_fee_usd_micros = output
                .marketplace_fee_usd_micros
                .saturating_add(event.marketplace_fee_usd_micros.unwrap_or(0).max(0));
        }
        let operator_receipt = payment_recipient.is_some_and(|address| {
            operators.contains(address)
                || (channel == ValueChannel::MintPayment && candidate_contract == Some(address))
        });
        if operator_receipt
            && matches!(
                channel,
                ValueChannel::MintPayment | ValueChannel::SalePayment | ValueChannel::RoyaltyFee
            )
        {
            output.operator_output_native = output
                .operator_output_native
                .saturating_add(event.native_amount.unwrap_or(0).max(0));
            output.operator_output_usd_micros = output
                .operator_output_usd_micros
                .saturating_add(event.usd_micros.unwrap_or(0).max(0));
        }
        if event.nft.as_ref().is_some_and(|nft| {
            payment_payer.is_some_and(|address| honest_holders.contains(&(nft, address)))
        }) && matches!(
            channel,
            ValueChannel::MintPayment | ValueChannel::SalePayment
        ) {
            let native = event.native_amount.unwrap_or(0).max(0);
            let usd = event.usd_micros.unwrap_or(0).max(0);
            output.honest_loss_native = output.honest_loss_native.saturating_add(native);
            output.honest_loss_usd_micros = output.honest_loss_usd_micros.saturating_add(usd);
            match channel {
                ValueChannel::MintPayment => {
                    output.paid_mint_loss_native =
                        output.paid_mint_loss_native.saturating_add(native);
                    output.paid_mint_loss_usd_micros =
                        output.paid_mint_loss_usd_micros.saturating_add(usd);
                }
                ValueChannel::SalePayment => {
                    output.secondary_sale_loss_native =
                        output.secondary_sale_loss_native.saturating_add(native);
                    output.secondary_sale_loss_usd_micros =
                        output.secondary_sale_loss_usd_micros.saturating_add(usd);
                }
                _ => unreachable!("honest loss is restricted to mint and sale"),
            }
            if let Some(nft) = &event.nft {
                stuck_nfts.insert(nft);
            }
            // The stuck-time ratio is defined over secondary-sale edges only;
            // paid mints still contribute to loss and the unique stuck-NFT set.
            if channel == ValueChannel::SalePayment {
                if let (Some((first_activity, analysis_timestamp)), Some(purchase)) =
                    (stuck_window, event.timestamp)
                {
                    if let (Some(held), Some(lure)) = (
                        analysis_timestamp
                            .checked_sub(purchase)
                            .filter(|value| *value >= 0),
                        purchase
                            .checked_sub(first_activity)
                            .filter(|value| *value >= 0),
                    ) {
                        output.stuck_time_numerator_seconds = output
                            .stuck_time_numerator_seconds
                            .saturating_add(i128::from(held));
                        output.stuck_time_denominator_seconds = output
                            .stuck_time_denominator_seconds
                            .saturating_add(i128::from(lure));
                    }
                }
            }
        }
        match event.kind {
            EventKind::Funding
                if event
                    .to
                    .as_deref()
                    .is_some_and(|address| operators.contains(address)) =>
            {
                output.funding_native = output
                    .funding_native
                    .saturating_add(event.native_amount.unwrap_or(0).max(0));
                output.funding_usd_micros = output
                    .funding_usd_micros
                    .saturating_add(event.usd_micros.unwrap_or(0).max(0));
            }
            EventKind::Withdrawal | EventKind::Cashout
                if event
                    .from
                    .as_deref()
                    .is_some_and(|address| operators.contains(address)) =>
            {
                let native = event.native_amount.unwrap_or(0).max(0);
                let usd = event.usd_micros.unwrap_or(0).max(0);
                output.withdrawal_native = output.withdrawal_native.saturating_add(native);
                output.withdrawal_usd_micros = output.withdrawal_usd_micros.saturating_add(usd);
                // These events move already received value, so counting them
                // again as operator output would duplicate prior mint/sale
                // revenue. A true exit sale remains represented as Sale.
                if event
                    .to
                    .as_deref()
                    .is_some_and(|address| operators.contains(address))
                {
                    output.revenue_backflow_native =
                        output.revenue_backflow_native.saturating_add(native);
                    output.revenue_backflow_usd_micros =
                        output.revenue_backflow_usd_micros.saturating_add(usd);
                }
            }
            _ => {}
        }
        if channel == ValueChannel::ExitPayment
            && event
                .from
                .as_deref()
                .is_some_and(|address| operators.contains(address))
        {
            output.operator_output_native = output
                .operator_output_native
                .saturating_add(event.native_amount.unwrap_or(0).max(0));
            output.operator_output_usd_micros = output
                .operator_output_usd_micros
                .saturating_add(event.usd_micros.unwrap_or(0).max(0));
        }
    }
    output.stuck_nft_count = stuck_nfts.len() as u64;
    let mut gas_cost_records = Vec::with_capacity(gas_by_transaction.len());
    for ((chain, _), transaction) in gas_by_transaction {
        let stage = transaction
            .stage
            .expect("gas transaction always has an assigned stage");
        match stage {
            GasStage::Setup => {
                output.setup_gas_native =
                    output.setup_gas_native.saturating_add(transaction.native);
                output.setup_gas_usd_micros = output
                    .setup_gas_usd_micros
                    .saturating_add(transaction.usd_micros);
            }
            GasStage::Lure => {
                output.lure_gas_native = output.lure_gas_native.saturating_add(transaction.native);
                output.lure_gas_usd_micros = output
                    .lure_gas_usd_micros
                    .saturating_add(transaction.usd_micros);
            }
            GasStage::Exit => {
                output.exit_gas_native = output.exit_gas_native.saturating_add(transaction.native);
                output.exit_gas_usd_micros = output
                    .exit_gas_usd_micros
                    .saturating_add(transaction.usd_micros);
            }
        }
        gas_cost_records.push(GasCostRecord {
            chain,
            stage,
            channel: transaction
                .channel
                .expect("gas transaction always has an assigned channel"),
            transaction: transaction
                .transaction
                .expect("gas transaction always retains its transaction identity"),
            gas_payer: transaction
                .payer
                .expect("operator-paid gas transaction always has a fee payer"),
            gas_native: transaction.native,
            gas_usd_micros: transaction.usd_micros,
            from_role: transaction.from_role,
            to_role: transaction.to_role,
            evidence_type: GasEvidenceKind::AttributedOperatorFeePayer,
        });
    }
    gas_cost_records.sort_by(|left, right| {
        right
            .gas_usd_micros
            .cmp(&left.gas_usd_micros)
            .then_with(|| right.gas_native.cmp(&left.gas_native))
            .then_with(|| left.stage.cmp(&right.stage))
            .then_with(|| left.chain.cmp(&right.chain))
            .then_with(|| left.transaction.cmp(&right.transaction))
    });
    EconomicAnalysis {
        facts: output,
        gas_cost_records,
    }
}

fn gas_stage(kind: EventKind, channel: ValueChannel) -> GasStage {
    if channel == ValueChannel::ExitPayment {
        return GasStage::Exit;
    }
    match kind {
        EventKind::Withdrawal | EventKind::Cashout => GasStage::Exit,
        EventKind::Mint | EventKind::Sale | EventKind::Listing => GasStage::Lure,
        EventKind::Deploy | EventKind::Funding | EventKind::Transfer => GasStage::Setup,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::ChainId;
    use std::sync::Arc;

    fn event(index: u32, kind: EventKind, from: &str, to: &str) -> NormalizedEvent {
        NormalizedEvent {
            chain: ChainId::Ethereum,
            tx_id: Arc::from("tx"),
            event_index: index,
            timestamp: Some(i64::from(index)),
            block_number: Some(u64::from(index)),
            kind,
            channel: None,
            from: Some(Arc::from(from)),
            to: Some(Arc::from(to)),
            fee_payer: None,
            payment_payer: None,
            payment_recipient: None,
            nft: None,
            native_amount: None,
            usd_micros: None,
            gas_native: None,
            gas_usd_micros: None,
            marketplace_fee_native: None,
            marketplace_fee_usd_micros: None,
        }
    }

    #[test]
    fn gas_uses_independent_currency_fields_and_highest_stage() {
        let mut setup = event(0, EventKind::Deploy, "operator", "contract");
        setup.fee_payer = Some(Arc::from("operator"));
        setup.gas_native = Some(10);
        setup.gas_usd_micros = Some(20);
        let mut exit = event(1, EventKind::Cashout, "operator", "wallet");
        exit.fee_payer = Some(Arc::from("operator"));
        exit.gas_native = Some(10);
        exit.gas_usd_micros = Some(20);
        let roles = BTreeMap::from([(Arc::from("operator"), AddressRole::SuspectedOperator)]);
        let analysis = compute_economics_detailed(&[setup, exit], &roles, &[]);
        assert_eq!(analysis.facts.exit_gas_native, 10);
        assert_eq!(analysis.facts.exit_gas_usd_micros, 20);
        assert_eq!(analysis.facts.setup_gas_native, 0);
        assert_eq!(analysis.gas_cost_records.len(), 1);
        let record = &analysis.gas_cost_records[0];
        assert_eq!(record.stage, GasStage::Exit);
        assert_eq!(record.channel, ValueChannel::CashoutHop);
        assert_eq!(record.transaction.as_ref(), "tx");
        assert_eq!(record.gas_payer.as_ref(), "operator");
        assert_eq!(record.from_role, Some(AddressRole::SuspectedOperator));
        assert_eq!(
            record.evidence_type,
            GasEvidenceKind::AttributedOperatorFeePayer
        );
    }

    #[test]
    fn honest_loss_requires_the_purchased_nft_to_still_be_held() {
        let held = NftKey {
            chain: ChainId::Ethereum,
            contract_address: Arc::from("contract"),
            token_id: Arc::from("1"),
        };
        let other = NftKey {
            token_id: Arc::from("2"),
            ..held.clone()
        };
        let mut sale = event(0, EventKind::Sale, "seller", "buyer");
        sale.tx_id = Arc::from("sale");
        sale.nft = Some(other);
        sale.native_amount = Some(7);
        sale.usd_micros = Some(11);
        let roles = BTreeMap::from([(Arc::from("buyer"), AddressRole::LikelyVictim)]);
        let holders = vec![(held, Arc::from("buyer"))];
        let facts = compute_economics(&[sale], &roles, &holders);
        assert_eq!(facts.honest_loss_native, 0);
        assert_eq!(facts.honest_loss_usd_micros, 0);
    }

    #[test]
    fn corrupted_victim_holders_are_excluded_from_honest_loss() {
        let nft = NftKey {
            chain: ChainId::Ethereum,
            contract_address: Arc::from("contract"),
            token_id: Arc::from("1"),
        };
        let mut sale = event(0, EventKind::Sale, "seller", "buyer");
        sale.nft = Some(nft.clone());
        sale.native_amount = Some(7);
        sale.usd_micros = Some(11);
        let roles = BTreeMap::from([(Arc::from("buyer"), AddressRole::CorruptedVictim)]);
        let holders = vec![(nft, Arc::from("buyer"))];
        let facts = compute_economics(&[sale], &roles, &holders);
        assert_eq!(facts.honest_loss_native, 0);
        assert_eq!(facts.honest_loss_usd_micros, 0);
        assert_eq!(facts.stuck_nft_count, 0);
    }

    #[test]
    fn sale_payment_flows_from_buyer_to_operator_seller() {
        let nft = NftKey {
            chain: ChainId::Ethereum,
            contract_address: Arc::from("contract"),
            token_id: Arc::from("1"),
        };
        let mut sale = event(0, EventKind::Sale, "operator", "buyer");
        sale.nft = Some(nft.clone());
        sale.native_amount = Some(7);
        sale.usd_micros = Some(11);
        let roles = BTreeMap::from([
            (Arc::from("operator"), AddressRole::SuspectedOperator),
            (Arc::from("buyer"), AddressRole::LikelyVictim),
        ]);
        let holders = vec![(nft, Arc::from("buyer"))];
        let facts = compute_economics(&[sale], &roles, &holders);
        assert_eq!(facts.operator_output_native, 7);
        assert_eq!(facts.operator_output_usd_micros, 11);
        assert_eq!(facts.honest_loss_native, 7);
        assert_eq!(facts.honest_loss_usd_micros, 11);
        assert_eq!(facts.secondary_sale_loss_native, 7);
        assert_eq!(facts.secondary_sale_loss_usd_micros, 11);
        assert_eq!(facts.paid_mint_loss_native, 0);
        assert_eq!(facts.paid_mint_loss_usd_micros, 0);
        assert_eq!(facts.stuck_nft_count, 1);
    }

    #[test]
    fn candidate_contract_receipt_counts_only_for_mint_output() {
        let mut mint = event(0, EventKind::Mint, "zero", "buyer");
        mint.payment_recipient = Some(Arc::from("contract"));
        mint.native_amount = Some(7);
        let mut sale = event(1, EventKind::Sale, "seller", "buyer");
        sale.payment_recipient = Some(Arc::from("contract"));
        sale.native_amount = Some(11);
        let events = [mint, sale];
        let facts = compute_candidate_economics_detailed_at(
            "contract",
            &events,
            &BTreeMap::new(),
            &[],
            None,
            10,
        )
        .facts;
        assert_eq!(facts.operator_output_native, 7);
        assert_eq!(facts.gross_revenue_native, 18);
    }

    #[test]
    fn honest_loss_splits_paid_mints_and_deduplicates_stuck_nfts() {
        let nft = NftKey {
            chain: ChainId::Ethereum,
            contract_address: Arc::from("contract"),
            token_id: Arc::from("1"),
        };
        let mut mint = event(1, EventKind::Mint, "operator", "buyer");
        mint.tx_id = Arc::from("mint");
        mint.nft = Some(nft.clone());
        mint.payment_payer = Some(Arc::from("buyer"));
        mint.payment_recipient = Some(Arc::from("operator"));
        mint.native_amount = Some(3);
        mint.usd_micros = Some(5);
        let mut sale = event(2, EventKind::Sale, "operator", "buyer");
        sale.tx_id = Arc::from("sale");
        sale.nft = Some(nft.clone());
        sale.payment_payer = Some(Arc::from("buyer"));
        sale.payment_recipient = Some(Arc::from("operator"));
        sale.native_amount = Some(7);
        sale.usd_micros = Some(11);
        let holders = vec![(nft, Arc::from("buyer"))];
        let roles = BTreeMap::from([(Arc::from("buyer"), AddressRole::LikelyVictim)]);
        let facts = compute_economics(&[mint, sale], &roles, &holders);
        assert_eq!(facts.paid_mint_loss_native, 3);
        assert_eq!(facts.paid_mint_loss_usd_micros, 5);
        assert_eq!(facts.secondary_sale_loss_native, 7);
        assert_eq!(facts.secondary_sale_loss_usd_micros, 11);
        assert_eq!(facts.honest_loss_native, 10);
        assert_eq!(facts.honest_loss_usd_micros, 16);
        assert_eq!(facts.stuck_nft_count, 1);
    }

    #[test]
    fn stuck_time_uses_secondary_sales_only() {
        let sale_nft = NftKey {
            chain: ChainId::Ethereum,
            contract_address: Arc::from("contract"),
            token_id: Arc::from("sale"),
        };
        let mint_nft = NftKey {
            token_id: Arc::from("mint"),
            ..sale_nft.clone()
        };
        let mut sale = event(0, EventKind::Sale, "operator", "buyer");
        sale.tx_id = Arc::from("sale");
        sale.timestamp = Some(40);
        sale.nft = Some(sale_nft.clone());
        sale.payment_payer = Some(Arc::from("buyer"));
        let mut mint = event(1, EventKind::Mint, "operator", "buyer");
        mint.tx_id = Arc::from("mint");
        mint.timestamp = Some(60);
        mint.nft = Some(mint_nft.clone());
        mint.payment_payer = Some(Arc::from("buyer"));
        let holders = [
            (sale_nft, Arc::from("buyer")),
            (mint_nft, Arc::from("buyer")),
        ];
        let events = [sale, mint];
        let roles = BTreeMap::from([(Arc::from("buyer"), AddressRole::LikelyVictim)]);
        let facts = compute_economics_detailed_at(&events, &roles, &holders, Some(10), 100).facts;
        assert_eq!(facts.stuck_time_numerator_seconds, 60);
        assert_eq!(facts.stuck_time_denominator_seconds, 30);
    }

    #[test]
    fn withdrawal_and_cashout_do_not_double_count_operator_output() {
        let mut withdrawal = event(0, EventKind::Withdrawal, "operator", "wallet");
        withdrawal.native_amount = Some(7);
        withdrawal.usd_micros = Some(11);
        let roles = BTreeMap::from([(Arc::from("operator"), AddressRole::SuspectedOperator)]);
        let facts = compute_economics(&[withdrawal], &roles, &[]);
        assert_eq!(facts.withdrawal_native, 7);
        assert_eq!(facts.withdrawal_usd_micros, 11);
        assert_eq!(facts.operator_output_native, 0);
        assert_eq!(facts.operator_output_usd_micros, 0);

        let mut exit = event(1, EventKind::Cashout, "operator", "offramp");
        exit.channel = Some(ValueChannel::ExitPayment);
        exit.native_amount = Some(5);
        exit.usd_micros = Some(9);
        let facts = compute_economics(&[exit], &roles, &[]);
        assert_eq!(facts.operator_output_native, 5);
        assert_eq!(facts.operator_output_usd_micros, 9);
    }

    #[test]
    fn royalty_output_is_not_a_second_sale_or_honest_loss() {
        let nft = NftKey {
            chain: ChainId::Ethereum,
            contract_address: Arc::from("contract"),
            token_id: Arc::from("1"),
        };
        let mut royalty = event(0, EventKind::Sale, "market", "operator");
        royalty.channel = Some(ValueChannel::RoyaltyFee);
        royalty.nft = Some(nft.clone());
        royalty.payment_payer = Some(Arc::from("buyer"));
        royalty.payment_recipient = Some(Arc::from("operator"));
        royalty.native_amount = Some(3);
        royalty.usd_micros = Some(5);
        let roles = BTreeMap::from([(Arc::from("operator"), AddressRole::SuspectedOperator)]);
        let holders = [(nft, Arc::from("buyer"))];
        let facts = compute_economics(&[royalty], &roles, &holders);
        assert_eq!(facts.gross_revenue_native, 0);
        assert_eq!(facts.honest_loss_native, 0);
        assert_eq!(facts.operator_output_native, 3);
        assert_eq!(facts.operator_output_usd_micros, 5);
    }
}
