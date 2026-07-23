//! Lifecycle timelines and value-flow aggregates.

use std::collections::BTreeSet;

use ahash::AHashSet;
use serde::{Deserialize, Serialize};

use super::attribution::AddressRole;
use crate::enrich::EvidenceBundle;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LifecycleFacts {
    pub deployment_timestamp: Option<i64>,
    pub first_activity_timestamp: Option<i64>,
    pub first_mint_timestamp: Option<i64>,
    pub first_transfer_timestamp: Option<i64>,
    pub first_sale_timestamp: Option<i64>,
    pub first_victim_timestamp: Option<i64>,
    pub deployment_to_first_transfer_seconds: Option<i64>,
    pub deployment_to_first_sale_seconds: Option<i64>,
    pub deployment_to_first_victim_seconds: Option<i64>,
    pub first_activity_to_first_victim_seconds: Option<i64>,
    pub first_victim_holding_seconds: Option<i64>,
    pub early_signal_categories: Vec<String>,
    pub early_signal_positive: Option<bool>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ValueFlowFacts {
    pub mint_edge_count: u64,
    pub transfer_edge_count: u64,
    pub sale_edge_count: u64,
    pub nft_count: u64,
    pub address_count: u64,
    pub gross_revenue_native: f64,
    pub gross_revenue_usd: f64,
    pub operator_revenue_native: f64,
    pub operator_revenue_usd: f64,
    pub malicious_address_count: u64,
    pub victim_address_count: u64,
    pub currently_holding_victim_address_count: u64,
    pub max_value_receiver: Option<String>,
    pub max_value_receiver_usd: f64,
    pub max_value_receiver_share: Option<f64>,
}

pub fn build_lifecycle(evidence: &EvidenceBundle, analysis_timestamp: i64) -> LifecycleFacts {
    let deployment_timestamp = evidence.deployment_timestamp;
    let mut first_activity_timestamp = deployment_timestamp;
    let mut first_mint_timestamp = None;
    let mut first_transfer_timestamp = None;
    let mut first_sale_timestamp = None;
    let mut first_victim_timestamp = None;
    let mut relevant_timing_complete = true;

    for transfer in &evidence.transfers {
        first_activity_timestamp = minimum_time(first_activity_timestamp, transfer.timestamp);
        if transfer.is_mint {
            first_mint_timestamp = minimum_time(first_mint_timestamp, transfer.timestamp);
        } else {
            first_transfer_timestamp = minimum_time(first_transfer_timestamp, transfer.timestamp);
        }
        if transfer.timestamp.is_none() {
            relevant_timing_complete = false;
        }
    }
    for sale in &evidence.sales {
        first_activity_timestamp = minimum_time(first_activity_timestamp, sale.timestamp);
        first_sale_timestamp = minimum_time(first_sale_timestamp, sale.timestamp);
        let paid = sale.native_amount.unwrap_or(0.0) > 0.0 || sale.usd_amount.unwrap_or(0.0) > 0.0;
        if paid {
            first_victim_timestamp = minimum_time(first_victim_timestamp, sale.timestamp);
        }
        if sale.timestamp.is_none() {
            relevant_timing_complete = false;
        }
    }

    let first_result = [first_sale_timestamp, first_victim_timestamp]
        .into_iter()
        .flatten()
        .min();
    let mut categories = BTreeSet::new();
    if let Some(result) = first_result {
        for transfer in evidence
            .transfers
            .iter()
            .filter(|event| event.timestamp.is_some_and(|time| time < result))
        {
            if transfer.is_mint {
                categories.insert("coordinated_mint_or_distribution".to_owned());
            } else {
                categories.insert("coordinated_mint_or_distribution".to_owned());
            }
        }
        if evidence
            .duplicate_content_timestamp
            .is_some_and(|timestamp| timestamp < result)
        {
            categories.insert("content_copy".to_owned());
        }
    }

    let history_complete = matches!(
        evidence.quality.histories,
        crate::enrich::EvidenceStatus::Complete | crate::enrich::EvidenceStatus::Empty
    );
    let early_signal_positive = match (deployment_timestamp, first_result) {
        (Some(deployment), Some(result))
            if result >= deployment
                && history_complete
                && relevant_timing_complete
                && (categories.len() >= 2 || evidence.duplicate_content_timestamp.is_some()) =>
        {
            Some(categories.len() >= 2)
        }
        _ => None,
    };

    LifecycleFacts {
        deployment_timestamp,
        first_activity_timestamp,
        first_mint_timestamp,
        first_transfer_timestamp,
        first_sale_timestamp,
        first_victim_timestamp,
        deployment_to_first_transfer_seconds: elapsed(
            deployment_timestamp,
            first_transfer_timestamp,
        ),
        deployment_to_first_sale_seconds: elapsed(deployment_timestamp, first_sale_timestamp),
        deployment_to_first_victim_seconds: elapsed(deployment_timestamp, first_victim_timestamp),
        first_activity_to_first_victim_seconds: elapsed(
            first_activity_timestamp,
            first_victim_timestamp,
        ),
        first_victim_holding_seconds: elapsed(first_victim_timestamp, Some(analysis_timestamp)),
        early_signal_categories: categories.into_iter().collect(),
        early_signal_positive,
    }
}

pub fn build_value_flow(
    evidence: &EvidenceBundle,
    roles: &BTreeMap<String, AddressRole>,
) -> ValueFlowFacts {
    let malicious = roles
        .iter()
        .filter(|(_, role)| {
            matches!(
                role,
                AddressRole::SuspectedOperator | AddressRole::SuspectedColluder
            )
        })
        .map(|(address, _)| address.as_str())
        .collect::<AHashSet<_>>();
    let victims = roles
        .iter()
        .filter(|(_, role)| {
            matches!(
                role,
                AddressRole::LikelyVictim | AddressRole::CorruptedVictim
            )
        })
        .map(|(address, _)| address.as_str())
        .collect::<AHashSet<_>>();
    let holding_victims = evidence
        .holders
        .iter()
        .filter(|holder| victims.contains(holder.owner.as_str()))
        .map(|holder| holder.owner.as_str())
        .collect::<AHashSet<_>>()
        .len() as u64;

    let mut facts = ValueFlowFacts {
        malicious_address_count: malicious.len() as u64,
        victim_address_count: victims.len() as u64,
        currently_holding_victim_address_count: holding_victims,
        ..Default::default()
    };

    let mut nfts = AHashSet::new();
    let mut addresses = AHashSet::new();
    let mut receiver_usd = BTreeMap::<String, f64>::new();
    let mut total_usd = 0.0_f64;

    for transfer in &evidence.transfers {
        nfts.insert(transfer.token_id.as_str());
        addresses.insert(transfer.from.as_str());
        addresses.insert(transfer.to.as_str());
        if transfer.is_mint {
            facts.mint_edge_count += 1;
        } else {
            facts.transfer_edge_count += 1;
        }
    }
    for sale in &evidence.sales {
        nfts.insert(sale.token_id.as_str());
        addresses.insert(sale.seller.as_str());
        addresses.insert(sale.buyer.as_str());
        facts.sale_edge_count += 1;
        let native = sale.native_amount.unwrap_or(0.0).max(0.0);
        let usd = sale.usd_amount.unwrap_or(0.0).max(0.0);
        facts.gross_revenue_native += native;
        facts.gross_revenue_usd += usd;
        if malicious.contains(sale.seller.as_str()) {
            facts.operator_revenue_native += native;
            facts.operator_revenue_usd += usd;
            *receiver_usd.entry(sale.seller.clone()).or_default() += usd;
            total_usd += usd;
        }
    }
    for holder in &evidence.holders {
        nfts.insert(holder.token_id.as_str());
        addresses.insert(holder.owner.as_str());
    }

    facts.nft_count = nfts.len() as u64;
    facts.address_count = addresses.len() as u64;
    if let Some((receiver, usd)) = receiver_usd
        .into_iter()
        .max_by(|left, right| {
            left.1
                .partial_cmp(&right.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| right.0.cmp(&left.0))
        })
    {
        facts.max_value_receiver = Some(receiver);
        facts.max_value_receiver_usd = usd;
        facts.max_value_receiver_share = (total_usd > 0.0).then_some(usd / total_usd);
    }
    facts
}

fn minimum_time(left: Option<i64>, right: Option<i64>) -> Option<i64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn elapsed(start: Option<i64>, end: Option<i64>) -> Option<i64> {
    start
        .zip(end)
        .and_then(|(start, end)| end.checked_sub(start))
        .filter(|duration| *duration >= 0)
}
