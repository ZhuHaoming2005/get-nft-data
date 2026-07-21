use crate::model::{
    AddressAttribution, AddressEvidence, AddressEvidenceKind, ContractKey, EventKind, NftKey,
    NormalizedEvent,
};
use ahash::{AHashMap, AHashSet};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

pub use crate::model::AddressRoleKind as AddressRole;

pub struct AttributionResult {
    pub roles: BTreeMap<Arc<str>, AddressRole>,
    pub records: BTreeMap<Arc<str>, AddressAttribution>,
}

pub fn attribute_addresses(
    candidate: &ContractKey,
    events: &[NormalizedEvent],
    controllers: &[Arc<str>],
    holders: &[(NftKey, Arc<str>)],
) -> BTreeMap<Arc<str>, AddressRole> {
    attribute_addresses_detailed(candidate, events, controllers, holders).roles
}

pub fn attribute_addresses_detailed(
    candidate: &ContractKey,
    events: &[NormalizedEvent],
    controllers: &[Arc<str>],
    holders: &[(NftKey, Arc<str>)],
) -> AttributionResult {
    let controller_set = controllers.iter().cloned().collect::<AHashSet<_>>();
    let holder_set = holders
        .iter()
        .map(|(_, address)| address.clone())
        .collect::<AHashSet<_>>();
    let mut paid_buyers = AHashSet::new();
    let mut propagators = AHashSet::new();
    let mut operator_evidence = controller_set.clone();
    for event in events {
        match event.kind {
            EventKind::Deploy => operator_evidence.extend(event.from.iter().cloned()),
            EventKind::Funding => {
                operator_evidence.extend(event.to.iter().cloned());
            }
            EventKind::Withdrawal | EventKind::Cashout => {
                operator_evidence.extend(event.from.iter().cloned());
                operator_evidence.extend(event.to.iter().cloned());
            }
            _ => {}
        }
        if (matches!(event.kind, EventKind::Mint) || event.is_nft_sale())
            && (event.native_amount.unwrap_or(0) > 0 || event.usd_micros.unwrap_or(0) > 0)
        {
            paid_buyers.extend(
                event
                    .payment_payer
                    .iter()
                    .cloned()
                    .chain(event.to.iter().cloned()),
            );
        }
        if event.is_nft_sale() || event.kind == EventKind::Transfer {
            propagators.extend(event.from.iter().cloned());
        }
    }
    let mut all = BTreeSet::new();
    all.extend(
        events
            .iter()
            .flat_map(|event| {
                [
                    event.from.clone(),
                    event.to.clone(),
                    event.fee_payer.clone(),
                    event.payment_payer.clone(),
                    event.payment_recipient.clone(),
                ]
            })
            .flatten(),
    );
    all.extend(controllers.iter().cloned());
    all.extend(holder_set.iter().cloned());
    let mut roles = all
        .into_iter()
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
    let sale_graph =
        crate::analysis::propagation::PropagationGraph::build_filtered(events, |event| {
            event.is_nft_sale()
        });
    let sale_components = sale_graph.strongly_connected_components();
    for component in &sale_components {
        if component.len() < 2
            || !component
                .iter()
                .any(|&vertex| operator_evidence.contains(&sale_graph.addresses[vertex]))
        {
            continue;
        }
        for vertex in component {
            let role = roles
                .get_mut(&sale_graph.addresses[*vertex])
                .expect("graph address is present in role map");
            if *role == AddressRole::Neutral {
                *role = AddressRole::SuspectedColluder;
            }
        }
    }
    let mut evidence = roles
        .keys()
        .cloned()
        .map(|address| (address, Vec::new()))
        .collect::<BTreeMap<_, Vec<AddressEvidence>>>();
    for controller in controllers {
        push_evidence(
            &mut evidence,
            controller,
            candidate,
            AddressEvidenceKind::ControllerOrAuthority,
            None,
            None,
            1.0,
            1.0,
        );
    }
    for (nft, holder) in holders {
        push_evidence(
            &mut evidence,
            holder,
            candidate,
            AddressEvidenceKind::CurrentHolder,
            Some(nft.clone()),
            None,
            0.5,
            0.75,
        );
    }
    for event in events {
        if let Some(from) = &event.from {
            push_evidence(
                &mut evidence,
                from,
                candidate,
                AddressEvidenceKind::EventSender,
                event.nft.clone(),
                Some(event.tx_id.clone()),
                0.1,
                0.25,
            );
        }
        if let Some(to) = &event.to {
            push_evidence(
                &mut evidence,
                to,
                candidate,
                AddressEvidenceKind::EventRecipient,
                event.nft.clone(),
                Some(event.tx_id.clone()),
                0.1,
                0.25,
            );
        }
        match event.kind {
            EventKind::Deploy => {
                if let Some(address) = &event.from {
                    push_evidence(
                        &mut evidence,
                        address,
                        candidate,
                        AddressEvidenceKind::Deployment,
                        event.nft.clone(),
                        Some(event.tx_id.clone()),
                        1.0,
                        1.0,
                    );
                }
            }
            EventKind::Funding => {
                if let Some(address) = &event.to {
                    push_evidence(
                        &mut evidence,
                        address,
                        candidate,
                        AddressEvidenceKind::FundingReceived,
                        event.nft.clone(),
                        Some(event.tx_id.clone()),
                        0.9,
                        0.9,
                    );
                }
            }
            EventKind::Withdrawal | EventKind::Cashout => {
                for address in event.from.iter().chain(event.to.iter()) {
                    push_evidence(
                        &mut evidence,
                        address,
                        candidate,
                        AddressEvidenceKind::WithdrawalOrCashout,
                        event.nft.clone(),
                        Some(event.tx_id.clone()),
                        0.9,
                        0.9,
                    );
                }
            }
            _ if (event.kind == EventKind::Mint || event.is_nft_sale())
                && (event.native_amount.unwrap_or(0) > 0 || event.usd_micros.unwrap_or(0) > 0) =>
            {
                for address in event.payment_payer.iter().chain(event.to.iter()) {
                    push_evidence(
                        &mut evidence,
                        address,
                        candidate,
                        AddressEvidenceKind::PaidAcquisition,
                        event.nft.clone(),
                        Some(event.tx_id.clone()),
                        0.8,
                        0.9,
                    );
                }
            }
            _ => {}
        }
        if event.is_nft_sale() || event.kind == EventKind::Transfer {
            if let Some(address) = &event.from {
                push_evidence(
                    &mut evidence,
                    address,
                    candidate,
                    AddressEvidenceKind::SubsequentPropagation,
                    event.nft.clone(),
                    Some(event.tx_id.clone()),
                    0.7,
                    0.8,
                );
            }
        }
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
                    .insert(sale_graph.addresses[vertex].as_ref(), component_id);
            }
        }
    }
    for event in events.iter().filter(|event| event.is_nft_sale()) {
        let same_cycle = event
            .from
            .as_deref()
            .and_then(|address| malicious_cycle_by_address.get(address))
            .zip(
                event
                    .to
                    .as_deref()
                    .and_then(|address| malicious_cycle_by_address.get(address)),
            )
            .is_some_and(|(left, right)| left == right);
        if same_cycle {
            for address in event.from.iter().chain(event.to.iter()) {
                push_evidence(
                    &mut evidence,
                    address,
                    candidate,
                    AddressEvidenceKind::MaliciousSaleCycle,
                    event.nft.clone(),
                    Some(event.tx_id.clone()),
                    0.9,
                    0.95,
                );
            }
        }
    }
    let records = roles
        .iter()
        .map(|(address, role)| {
            let mut evidence = evidence.remove(address).unwrap_or_default();
            evidence.sort_by(|left, right| {
                (left.evidence_type, &left.token, left.transaction.as_deref()).cmp(&(
                    right.evidence_type,
                    &right.token,
                    right.transaction.as_deref(),
                ))
            });
            evidence.dedup_by(|left, right| {
                left.evidence_type == right.evidence_type
                    && left.token == right.token
                    && left.transaction == right.transaction
            });
            (
                address.clone(),
                AddressAttribution {
                    role: *role,
                    evidence,
                },
            )
        })
        .collect();
    AttributionResult { roles, records }
}

#[allow(clippy::too_many_arguments)]
fn push_evidence(
    evidence: &mut BTreeMap<Arc<str>, Vec<AddressEvidence>>,
    address: &Arc<str>,
    candidate: &ContractKey,
    evidence_type: AddressEvidenceKind,
    token: Option<NftKey>,
    transaction: Option<Arc<str>>,
    weight: f64,
    confidence: f64,
) {
    evidence
        .entry(address.clone())
        .or_default()
        .push(AddressEvidence {
            evidence_type,
            related_contract: candidate.clone(),
            token,
            transaction,
            weight,
            confidence,
        });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::ChainId;

    #[test]
    fn address_labels_keep_self_contained_weighted_evidence() {
        let candidate = ContractKey::new(ChainId::Ethereum, "0xcopy");
        let nft = NftKey {
            chain: candidate.chain,
            contract_address: candidate.contract_address.clone(),
            token_id: Arc::from("1"),
        };
        let sale = NormalizedEvent {
            chain: candidate.chain,
            tx_id: Arc::from("0xtx"),
            event_index: 0,
            timestamp: Some(1),
            block_number: Some(1),
            kind: EventKind::Sale,
            channel: None,
            from: Some(Arc::from("0xoperator")),
            to: Some(Arc::from("0xbuyer")),
            fee_payer: None,
            payment_payer: Some(Arc::from("0xbuyer")),
            payment_recipient: Some(Arc::from("0xoperator")),
            nft: Some(nft.clone()),
            native_amount: Some(10),
            usd_micros: Some(20),
            gas_native: None,
            gas_usd_micros: None,
            marketplace_fee_native: None,
            marketplace_fee_usd_micros: None,
        };
        let result = attribute_addresses_detailed(
            &candidate,
            &[sale],
            &[Arc::from("0xoperator")],
            &[(nft.clone(), Arc::from("0xbuyer"))],
        );
        assert_eq!(result.roles["0xoperator"], AddressRole::SuspectedOperator);
        assert_eq!(result.roles["0xbuyer"], AddressRole::LikelyVictim);
        let buyer = &result.records["0xbuyer"];
        assert!(buyer.evidence.iter().any(|evidence| {
            evidence.evidence_type == AddressEvidenceKind::PaidAcquisition
                && evidence.token.as_ref() == Some(&nft)
                && evidence.transaction.as_deref() == Some("0xtx")
                && evidence.weight > 0.0
                && evidence.confidence > 0.0
        }));
    }
}
