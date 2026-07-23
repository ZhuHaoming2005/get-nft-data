//! Address role attribution with weighted evidence.

use std::collections::{BTreeMap, BTreeSet};

use ahash::{AHashMap, AHashSet};
use serde::{Deserialize, Serialize};

use super::graph::AddressGraph;
use crate::enrich::EvidenceBundle;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AddressRole {
    SuspectedOperator,
    SuspectedColluder,
    LikelyVictim,
    CorruptedVictim,
    Neutral,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AddressEvidenceKind {
    ControllerOrAuthority,
    CurrentHolder,
    EventSender,
    EventRecipient,
    MintRecipient,
    PaidAcquisition,
    SubsequentPropagation,
    MaliciousSaleCycle,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AddressEvidence {
    pub evidence_type: AddressEvidenceKind,
    pub token_id: Option<String>,
    pub transaction: Option<String>,
    pub weight: f64,
    pub confidence: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AddressAttribution {
    pub role: AddressRole,
    pub evidence: Vec<AddressEvidence>,
}

pub struct AttributionResult {
    pub roles: BTreeMap<String, AddressRole>,
    pub records: BTreeMap<String, AddressAttribution>,
}

pub fn attribute_addresses(
    evidence: &EvidenceBundle,
    _transfer_graph: &AddressGraph,
) -> AttributionResult {
    let controller_set = evidence
        .controllers
        .iter()
        .cloned()
        .collect::<AHashSet<_>>();
    let holder_set = evidence
        .holders
        .iter()
        .map(|holder| holder.owner.clone())
        .collect::<AHashSet<_>>();

    let mut paid_buyers = AHashSet::new();
    let mut propagators = AHashSet::new();
    let mut operator_evidence = controller_set.clone();

    for sale in &evidence.sales {
        let paid = sale.native_amount.unwrap_or(0.0) > 0.0 || sale.usd_amount.unwrap_or(0.0) > 0.0;
        if paid {
            paid_buyers.insert(sale.buyer.clone());
        }
        propagators.insert(sale.seller.clone());
    }
    for transfer in &evidence.transfers {
        if transfer.is_mint {
            // Free/paid mint recipient is often an operator seed when also a controller;
            // otherwise mint alone does not imply operator.
            if controller_set.contains(&transfer.to) {
                operator_evidence.insert(transfer.to.clone());
            }
        } else {
            propagators.insert(transfer.from.clone());
        }
    }

    let mut all = BTreeSet::new();
    all.extend(evidence.controllers.iter().cloned());
    all.extend(holder_set.iter().cloned());
    for sale in &evidence.sales {
        all.insert(sale.seller.clone());
        all.insert(sale.buyer.clone());
    }
    for transfer in &evidence.transfers {
        all.insert(transfer.from.clone());
        all.insert(transfer.to.clone());
    }

    let mut roles = all
        .into_iter()
        .filter(|address| !address.is_empty())
        .map(|address| {
            let role = if operator_evidence.contains(&address) {
                AddressRole::SuspectedOperator
            } else if paid_buyers.contains(&address) && propagators.contains(&address) {
                AddressRole::CorruptedVictim
            } else if paid_buyers.contains(&address) && holder_set.contains(&address) {
                AddressRole::LikelyVictim
            } else {
                AddressRole::Neutral
            };
            (address, role)
        })
        .collect::<BTreeMap<_, _>>();

    let sale_graph = AddressGraph::from_sales(&evidence.sales);
    let sale_components = sale_graph.strongly_connected_components();
    for component in &sale_components {
        if component.len() < 2 {
            continue;
        }
        let has_operator = component
            .iter()
            .any(|&vertex| operator_evidence.contains(&sale_graph.addresses[vertex]));
        if !has_operator {
            continue;
        }
        for &vertex in component {
            let address = &sale_graph.addresses[vertex];
            if let Some(role) = roles.get_mut(address) {
                // Cycle participants are colluders; prefer over neutral / corrupted-victim.
                if matches!(
                    *role,
                    AddressRole::Neutral | AddressRole::CorruptedVictim | AddressRole::LikelyVictim
                ) {
                    *role = AddressRole::SuspectedColluder;
                }
            }
        }
    }

    let mut evidence_rows = roles
        .keys()
        .cloned()
        .map(|address| (address, Vec::new()))
        .collect::<BTreeMap<_, Vec<AddressEvidence>>>();

    for controller in &evidence.controllers {
        push_evidence(
            &mut evidence_rows,
            controller,
            AddressEvidenceKind::ControllerOrAuthority,
            None,
            None,
            1.0,
            1.0,
        );
    }
    for holder in &evidence.holders {
        push_evidence(
            &mut evidence_rows,
            &holder.owner,
            AddressEvidenceKind::CurrentHolder,
            Some(holder.token_id.clone()),
            None,
            0.5,
            0.75,
        );
    }
    for transfer in &evidence.transfers {
        push_evidence(
            &mut evidence_rows,
            &transfer.from,
            AddressEvidenceKind::EventSender,
            Some(transfer.token_id.clone()),
            Some(transfer.tx_hash.clone()),
            0.1,
            0.25,
        );
        push_evidence(
            &mut evidence_rows,
            &transfer.to,
            AddressEvidenceKind::EventRecipient,
            Some(transfer.token_id.clone()),
            Some(transfer.tx_hash.clone()),
            0.1,
            0.25,
        );
        if transfer.is_mint {
            push_evidence(
                &mut evidence_rows,
                &transfer.to,
                AddressEvidenceKind::MintRecipient,
                Some(transfer.token_id.clone()),
                Some(transfer.tx_hash.clone()),
                0.6,
                0.7,
            );
        } else {
            push_evidence(
                &mut evidence_rows,
                &transfer.from,
                AddressEvidenceKind::SubsequentPropagation,
                Some(transfer.token_id.clone()),
                Some(transfer.tx_hash.clone()),
                0.7,
                0.8,
            );
        }
    }
    for sale in &evidence.sales {
        push_evidence(
            &mut evidence_rows,
            &sale.seller,
            AddressEvidenceKind::EventSender,
            Some(sale.token_id.clone()),
            Some(sale.tx_hash.clone()),
            0.1,
            0.25,
        );
        push_evidence(
            &mut evidence_rows,
            &sale.buyer,
            AddressEvidenceKind::EventRecipient,
            Some(sale.token_id.clone()),
            Some(sale.tx_hash.clone()),
            0.1,
            0.25,
        );
        let paid = sale.native_amount.unwrap_or(0.0) > 0.0 || sale.usd_amount.unwrap_or(0.0) > 0.0;
        if paid {
            push_evidence(
                &mut evidence_rows,
                &sale.buyer,
                AddressEvidenceKind::PaidAcquisition,
                Some(sale.token_id.clone()),
                Some(sale.tx_hash.clone()),
                0.8,
                0.9,
            );
        }
        push_evidence(
            &mut evidence_rows,
            &sale.seller,
            AddressEvidenceKind::SubsequentPropagation,
            Some(sale.token_id.clone()),
            Some(sale.tx_hash.clone()),
            0.7,
            0.8,
        );
    }

    let mut malicious_cycle_by_address = AHashMap::<&str, usize>::new();
    for (component_id, component) in sale_components.iter().enumerate() {
        if component.len() >= 2
            && component
                .iter()
                .any(|&vertex| operator_evidence.contains(&sale_graph.addresses[vertex]))
        {
            for &vertex in component {
                malicious_cycle_by_address
                    .insert(sale_graph.addresses[vertex].as_str(), component_id);
            }
        }
    }
    for sale in &evidence.sales {
        let same_cycle = malicious_cycle_by_address
            .get(sale.seller.as_str())
            .zip(malicious_cycle_by_address.get(sale.buyer.as_str()))
            .is_some_and(|(left, right)| left == right);
        if same_cycle {
            for address in [&sale.seller, &sale.buyer] {
                push_evidence(
                    &mut evidence_rows,
                    address,
                    AddressEvidenceKind::MaliciousSaleCycle,
                    Some(sale.token_id.clone()),
                    Some(sale.tx_hash.clone()),
                    0.9,
                    0.95,
                );
            }
        }
    }

    let records = roles
        .iter()
        .map(|(address, role)| {
            let mut rows = evidence_rows.remove(address).unwrap_or_default();
            rows.sort_by(|left, right| {
                (
                    left.evidence_type,
                    left.token_id.as_deref(),
                    left.transaction.as_deref(),
                )
                    .cmp(&(
                        right.evidence_type,
                        right.token_id.as_deref(),
                        right.transaction.as_deref(),
                    ))
            });
            rows.dedup_by(|left, right| {
                left.evidence_type == right.evidence_type
                    && left.token_id == right.token_id
                    && left.transaction == right.transaction
            });
            (
                address.clone(),
                AddressAttribution {
                    role: *role,
                    evidence: rows,
                },
            )
        })
        .collect();

    AttributionResult { roles, records }
}

fn push_evidence(
    evidence: &mut BTreeMap<String, Vec<AddressEvidence>>,
    address: &str,
    evidence_type: AddressEvidenceKind,
    token_id: Option<String>,
    transaction: Option<String>,
    weight: f64,
    confidence: f64,
) {
    if address.is_empty() {
        return;
    }
    evidence.entry(address.to_owned()).or_default().push(AddressEvidence {
        evidence_type,
        token_id,
        transaction,
        weight,
        confidence,
    });
}
