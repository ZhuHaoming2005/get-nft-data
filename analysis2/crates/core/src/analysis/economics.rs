//! Attacker economics: Setup/Lure/Exit gas, output ratios, honest loss.

use std::collections::BTreeMap;

use ahash::{AHashMap, AHashSet};
use serde::{Deserialize, Serialize};

use super::attribution::AddressRole;
use super::lifecycle::LifecycleFacts;
use crate::enrich::{EvidenceBundle, EvidenceStatus, ValueFlowKind};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct EconomicFacts {
    pub gross_revenue_native: f64,
    pub gross_revenue_usd: f64,
    pub setup_gas_native: f64,
    pub lure_gas_native: f64,
    pub exit_gas_native: f64,
    pub total_gas_native: f64,
    pub operator_output_native: f64,
    pub operator_output_usd: f64,
    /// Same-unit output/input: USD/USD when `attacker_input_usd` is set, else native/native.
    pub output_input_ratio: Option<f64>,
    /// True when `output_input_ratio` is USD/USD (gas priced via spot).
    #[serde(default)]
    pub output_input_ratio_is_usd: bool,
    /// Attacker gas cost in USD when a spot rate is available for the chain.
    #[serde(default)]
    pub attacker_input_usd: Option<f64>,
    pub secondary_sale_loss_native: f64,
    pub secondary_sale_loss_usd: f64,
    pub paid_mint_loss_native: f64,
    pub paid_mint_loss_usd: f64,
    pub honest_loss_native: f64,
    pub honest_loss_usd: f64,
    pub stuck_nft_count: u64,
    /// Native funding into operators (from `value_flows` Funding edges).
    pub funding_native: f64,
    pub funding_usd: f64,
    /// Native withdrawal / cashout out of operators.
    pub withdrawal_native: f64,
    pub withdrawal_usd: f64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct EconomicsQuality {
    pub gas: EvidenceStatus,
    pub value_flows: EvidenceStatus,
    pub notes: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum GasStage {
    Setup = 1,
    Lure = 2,
    Exit = 3,
}

fn value_flows_usable(status: EvidenceStatus) -> bool {
    matches!(status, EvidenceStatus::Complete | EvidenceStatus::Truncated)
}

pub fn compute_economics(
    evidence: &EvidenceBundle,
    roles: &BTreeMap<String, AddressRole>,
    analysis_timestamp: i64,
    lifecycle: &LifecycleFacts,
) -> (EconomicFacts, EconomicsQuality) {
    let mut quality = EconomicsQuality {
        gas: evidence.quality.gas,
        value_flows: evidence.quality.value_flows,
        notes: Vec::new(),
    };
    if matches!(
        evidence.quality.gas,
        EvidenceStatus::NotRequested | EvidenceStatus::Failed
    ) {
        quality.notes.push(
            "gas evidence incomplete; Setup/Lure/Exit costs reported as honest zeros".into(),
        );
    }
    if matches!(
        evidence.quality.value_flows,
        EvidenceStatus::NotRequested | EvidenceStatus::Failed
    ) {
        quality
            .notes
            .push("value_flows evidence incomplete; funding/withdrawal aggregates unavailable".into());
    }

    let operators = roles
        .iter()
        .filter(|(_, role)| {
            matches!(
                role,
                AddressRole::SuspectedOperator | AddressRole::SuspectedColluder
            )
        })
        .map(|(address, _)| address.as_str())
        .collect::<AHashSet<_>>();
    let honest_buyers = roles
        .iter()
        .filter(|(_, role)| matches!(role, AddressRole::LikelyVictim))
        .map(|(address, _)| address.as_str())
        .collect::<AHashSet<_>>();
    let honest_holders = evidence
        .holders
        .iter()
        .filter(|holder| honest_buyers.contains(holder.owner.as_str()))
        .map(|holder| (holder.token_id.as_str(), holder.owner.as_str()))
        .collect::<AHashSet<_>>();

    let mut facts = EconomicFacts::default();
    let mut stuck_nfts = AHashSet::new();
    let mut gas_by_tx = AHashMap::<&str, (GasStage, f64)>::new();
    let mut gas_native_by_tx = AHashMap::<&str, f64>::new();

    for sale in &evidence.sales {
        let native = sale.native_amount.unwrap_or(0.0).max(0.0);
        let usd = sale.usd_amount.unwrap_or(0.0).max(0.0);
        facts.gross_revenue_native += native;
        facts.gross_revenue_usd += usd;
        if operators.contains(sale.seller.as_str()) {
            facts.operator_output_native += native;
            facts.operator_output_usd += usd;
        }
        if honest_holders.contains(&(sale.token_id.as_str(), sale.buyer.as_str()))
            && (native > 0.0 || usd > 0.0)
        {
            facts.secondary_sale_loss_native += native;
            facts.secondary_sale_loss_usd += usd;
            facts.honest_loss_native += native;
            facts.honest_loss_usd += usd;
            stuck_nfts.insert(sale.token_id.as_str());
        }
        // Sale gas attributed as Lure when quality allows and seller/operator paid.
        if evidence.quality.gas == EvidenceStatus::Complete {
            // Sales themselves rarely carry gas; transfers below may.
        }
        let _ = analysis_timestamp;
        let _ = lifecycle;
    }

    for transfer in &evidence.transfers {
        if transfer.is_mint
            && honest_holders.contains(&(transfer.token_id.as_str(), transfer.to.as_str()))
        {
            let native = transfer.mint_payment_native.unwrap_or(0.0).max(0.0);
            let usd = transfer.mint_payment_usd.unwrap_or(0.0).max(0.0);
            if native > 0.0 || usd > 0.0 {
                facts.paid_mint_loss_native += native;
                facts.paid_mint_loss_usd += usd;
                facts.honest_loss_native += native;
                facts.honest_loss_usd += usd;
                stuck_nfts.insert(transfer.token_id.as_str());
            }
        }
        if let Some(gas) = transfer.gas_native.filter(|value| *value > 0.0) {
            let entry = gas_native_by_tx
                .entry(transfer.tx_hash.as_str())
                .or_insert(0.0);
            *entry = entry.max(gas);
        }
        if evidence.quality.gas == EvidenceStatus::Complete {
            if let Some(gas) = transfer.gas_native.filter(|value| *value > 0.0) {
                let payer = transfer
                    .fee_payer
                    .as_deref()
                    .unwrap_or(if transfer.is_mint {
                        transfer.to.as_str()
                    } else {
                        transfer.from.as_str()
                    });
                if operators.contains(payer) {
                    let stage = if transfer.is_mint {
                        GasStage::Setup
                    } else {
                        GasStage::Lure
                    };
                    let entry = gas_by_tx.entry(transfer.tx_hash.as_str()).or_insert((stage, 0.0));
                    if stage > entry.0 {
                        entry.0 = stage;
                    }
                    entry.1 = entry.1.max(gas);
                }
            }
        }
    }

    if value_flows_usable(evidence.quality.value_flows) {
        for edge in &evidence.value_flows {
            let native = edge.native_amount.unwrap_or(0.0).max(0.0);
            let usd = edge.usd_amount.unwrap_or(0.0).max(0.0);
            match edge.kind {
                ValueFlowKind::Funding | ValueFlowKind::RevenueBackflow => {
                    facts.funding_native += native;
                    facts.funding_usd += usd;
                }
                ValueFlowKind::Withdrawal | ValueFlowKind::Cashout => {
                    facts.withdrawal_native += native;
                    facts.withdrawal_usd += usd;
                    if let Some(&gas) = gas_native_by_tx.get(edge.tx_hash.as_str()) {
                        let entry = gas_by_tx
                            .entry(edge.tx_hash.as_str())
                            .or_insert((GasStage::Exit, 0.0));
                        if GasStage::Exit > entry.0 {
                            entry.0 = GasStage::Exit;
                        }
                        entry.1 = entry.1.max(gas);
                    }
                }
            }
        }
    }

    for (_, (stage, gas)) in gas_by_tx {
        match stage {
            GasStage::Setup => facts.setup_gas_native += gas,
            GasStage::Lure => facts.lure_gas_native += gas,
            GasStage::Exit => facts.exit_gas_native += gas,
        }
        facts.total_gas_native += gas;
    }

    facts.stuck_nft_count = stuck_nfts.len() as u64;

    // Price attacker gas to USD when spot rate is present (same chain).
    let gas_usd = spot_gas_usd(
        facts.total_gas_native,
        evidence.chain.as_str(),
        &evidence.prices,
    );
    facts.attacker_input_usd = gas_usd;
    let (ratio, is_usd) = same_unit_output_input_ratio(
        facts.operator_output_usd,
        facts.operator_output_native,
        gas_usd,
        facts.total_gas_native,
    );
    facts.output_input_ratio = ratio;
    facts.output_input_ratio_is_usd = is_usd;

    (facts, quality)
}

fn spot_gas_usd(gas_native: f64, chain: &str, prices: &[crate::enrich::PriceBucket]) -> Option<f64> {
    if gas_native <= 0.0 {
        return None;
    }
    prices
        .iter()
        .find(|p| p.chain.eq_ignore_ascii_case(chain) && p.usd_per_native > 0.0)
        .map(|p| gas_native * p.usd_per_native)
        .filter(|v| v.is_finite() && *v > 0.0)
}

/// Prefer USD/USD when gas USD available; else native/native; else None.
/// Returns `(ratio, is_usd)`.
fn same_unit_output_input_ratio(
    output_usd: f64,
    output_native: f64,
    gas_usd: Option<f64>,
    gas_native: f64,
) -> (Option<f64>, bool) {
    if let Some(gas_usd) = gas_usd.filter(|value| *value > 0.0) {
        return (Some(output_usd / gas_usd), true);
    }
    if gas_native > 0.0 {
        return (Some(output_native / gas_native), false);
    }
    (None, false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::enrich::{
        EvidenceQuality, EvidenceStatus, TransferEvent, ValueFlowEdge, ValueFlowKind,
    };

    fn roles_op(op: &str) -> BTreeMap<String, AddressRole> {
        let mut roles = BTreeMap::new();
        roles.insert(op.to_owned(), AddressRole::SuspectedOperator);
        roles
    }

    fn edge(
        tx: &str,
        from: &str,
        to: &str,
        kind: ValueFlowKind,
        native: f64,
        usd: f64,
    ) -> ValueFlowEdge {
        ValueFlowEdge {
            tx_hash: tx.into(),
            from: from.into(),
            to: to.into(),
            kind,
            native_amount: Some(native),
            usd_amount: Some(usd),
            timestamp: Some(1),
        }
    }

    #[test]
    fn value_flows_complete_aggregates_funding_withdrawal_and_exit_gas() {
        let mut evidence = EvidenceBundle::empty(1, "ethereum", "0xcand");
        evidence.controllers = vec!["0xop".into()];
        evidence.transfers = vec![TransferEvent {
            tx_hash: "cashout-tx".into(),
            token_id: "1".into(),
            from: "0xop".into(),
            to: "0xex".into(),
            timestamp: Some(10),
            block_number: Some(10),
            is_mint: false,
            gas_native: Some(0.05),
            fee_payer: Some("0xop".into()),
            mint_payment_native: None,
            mint_payment_usd: None,
        }];
        evidence.value_flows = vec![
            edge("fund-tx", "0xfunder", "0xop", ValueFlowKind::Funding, 1.0, 200.0),
            edge(
                "cashout-tx",
                "0xop",
                "0xex",
                ValueFlowKind::Cashout,
                0.8,
                160.0,
            ),
            edge(
                "wd-tx",
                "0xop",
                "0xcex",
                ValueFlowKind::Withdrawal,
                0.2,
                40.0,
            ),
        ];
        evidence.quality = EvidenceQuality {
            transfers: EvidenceStatus::Complete,
            gas: EvidenceStatus::Complete,
            value_flows: EvidenceStatus::Complete,
            ..EvidenceQuality::default()
        };

        let (facts, quality) = compute_economics(
            &evidence,
            &roles_op("0xop"),
            100,
            &LifecycleFacts::default(),
        );

        assert_eq!(quality.value_flows, EvidenceStatus::Complete);
        assert!(quality.notes.is_empty());
        assert_eq!(facts.funding_native, 1.0);
        assert_eq!(facts.funding_usd, 200.0);
        assert_eq!(facts.withdrawal_native, 1.0);
        assert_eq!(facts.withdrawal_usd, 200.0);
        assert_eq!(facts.exit_gas_native, 0.05);
        assert_eq!(facts.total_gas_native, 0.05);
    }

    #[test]
    fn value_flows_not_requested_keeps_zero_aggregates_and_note() {
        let mut evidence = EvidenceBundle::empty(1, "ethereum", "0xcand");
        evidence.value_flows = vec![edge(
            "fund-tx",
            "0xfunder",
            "0xop",
            ValueFlowKind::Funding,
            9.0,
            900.0,
        )];
        evidence.quality.value_flows = EvidenceStatus::NotRequested;

        let (facts, quality) = compute_economics(
            &evidence,
            &roles_op("0xop"),
            100,
            &LifecycleFacts::default(),
        );

        assert_eq!(facts.funding_native, 0.0);
        assert_eq!(facts.funding_usd, 0.0);
        assert_eq!(facts.withdrawal_native, 0.0);
        assert_eq!(facts.withdrawal_usd, 0.0);
        assert_eq!(facts.exit_gas_native, 0.0);
        assert!(quality.notes.iter().any(|n| n.contains("value_flows")));
    }

    #[test]
    fn paid_mint_loss_counts_when_honest_holder_has_payment() {
        use crate::enrich::HolderRecord;

        let mut evidence = EvidenceBundle::empty(1, "ethereum", "0xcand");
        evidence.transfers = vec![TransferEvent {
            tx_hash: "mint-tx".into(),
            token_id: "1".into(),
            from: "0x0000000000000000000000000000000000000000".into(),
            to: "0xvictim".into(),
            timestamp: Some(10),
            block_number: Some(10),
            is_mint: true,
            gas_native: None,
            fee_payer: None,
            mint_payment_native: Some(0.08),
            mint_payment_usd: Some(160.0),
        }];
        evidence.holders = vec![HolderRecord {
            token_id: "1".into(),
            owner: "0xvictim".into(),
            balance: Some(1),
        }];
        evidence.quality.transfers = EvidenceStatus::Complete;
        evidence.quality.holders = EvidenceStatus::Complete;

        let mut roles = BTreeMap::new();
        roles.insert("0xvictim".into(), AddressRole::LikelyVictim);

        let (facts, _) = compute_economics(
            &evidence,
            &roles,
            100,
            &LifecycleFacts::default(),
        );
        assert_eq!(facts.paid_mint_loss_native, 0.08);
        assert_eq!(facts.paid_mint_loss_usd, 160.0);
        assert_eq!(facts.honest_loss_native, 0.08);
        assert_eq!(facts.honest_loss_usd, 160.0);
        assert_eq!(facts.stuck_nft_count, 1);
    }

    #[test]
    fn free_mint_does_not_add_paid_mint_loss() {
        use crate::enrich::HolderRecord;

        let mut evidence = EvidenceBundle::empty(1, "ethereum", "0xcand");
        evidence.transfers = vec![TransferEvent {
            tx_hash: "mint-tx".into(),
            token_id: "1".into(),
            from: "0x0000000000000000000000000000000000000000".into(),
            to: "0xvictim".into(),
            timestamp: Some(10),
            block_number: Some(10),
            is_mint: true,
            gas_native: None,
            fee_payer: None,
            mint_payment_native: None,
            mint_payment_usd: None,
        }];
        evidence.holders = vec![HolderRecord {
            token_id: "1".into(),
            owner: "0xvictim".into(),
            balance: Some(1),
        }];
        let mut roles = BTreeMap::new();
        roles.insert("0xvictim".into(), AddressRole::LikelyVictim);
        let (facts, _) = compute_economics(
            &evidence,
            &roles,
            100,
            &LifecycleFacts::default(),
        );
        assert_eq!(facts.paid_mint_loss_native, 0.0);
        assert_eq!(facts.paid_mint_loss_usd, 0.0);
    }

    #[test]
    fn value_flows_truncated_still_computes_available_edges() {
        let mut evidence = EvidenceBundle::empty(1, "ethereum", "0xcand");
        evidence.value_flows = vec![edge(
            "fund-tx",
            "0xfunder",
            "0xop",
            ValueFlowKind::Funding,
            2.5,
            500.0,
        )];
        evidence.quality.value_flows = EvidenceStatus::Truncated;

        let (facts, quality) = compute_economics(
            &evidence,
            &roles_op("0xop"),
            100,
            &LifecycleFacts::default(),
        );

        assert_eq!(quality.value_flows, EvidenceStatus::Truncated);
        assert_eq!(facts.funding_native, 2.5);
        assert_eq!(facts.funding_usd, 500.0);
        assert!(!quality.notes.iter().any(|n| n.contains("value_flows")));
    }
}
