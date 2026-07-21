pub mod attribution;
pub mod behavior;
pub mod economics;
pub mod legit_duplicate;
pub mod lifecycle;
pub mod propagation;
pub mod quality;
pub mod relation_projector;

use crate::model::{CandidateFacts, EventKind, EvidenceBundle, NormalizedEvent};
use ahash::AHashSet;
use std::sync::Arc;

type NftMovementKey = (
    crate::model::ChainId,
    Arc<str>,
    crate::model::NftKey,
    Arc<str>,
    Arc<str>,
);

pub fn normalize_evidence(bundle: &mut EvidenceBundle) -> crate::Result<()> {
    canonicalize_evidence_identities(bundle);
    reconcile_duplicate_events(&mut bundle.events);
    intern_evidence_strings(bundle);
    bundle.events.sort_by(|left, right| {
        (left.chain, &left.tx_id, left.event_index).cmp(&(
            right.chain,
            &right.tx_id,
            right.event_index,
        ))
    });
    for duplicate in bundle.events.windows(2) {
        if (
            duplicate[0].chain,
            duplicate[0].tx_id.as_ref(),
            duplicate[0].event_index,
        ) == (
            duplicate[1].chain,
            duplicate[1].tx_id.as_ref(),
            duplicate[1].event_index,
        ) && duplicate[0] != duplicate[1]
        {
            return Err(crate::AnalysisError::Provider(format!(
                "conflicting normalized event identity {}:{}:{}",
                duplicate[0].chain, duplicate[0].tx_id, duplicate[0].event_index
            )));
        }
    }
    bundle.events.dedup_by(|left, right| {
        (left.chain, left.tx_id.as_ref(), left.event_index)
            == (right.chain, right.tx_id.as_ref(), right.event_index)
    });
    bundle.holders.sort();
    bundle.holders.dedup();
    bundle.controllers.sort();
    bundle.controllers.dedup();
    for verification in &mut bundle.relation_verifications {
        verification.evidence_keys.sort();
        verification.evidence_keys.dedup();
        verification.failures.sort();
        verification.failures.dedup();
    }
    bundle
        .relation_verifications
        .sort_by_key(|verification| verification.seed_id);
    let mut verifications = Vec::<crate::model::RelationVerification>::with_capacity(
        bundle.relation_verifications.len(),
    );
    for verification in bundle.relation_verifications.drain(..) {
        if let Some(existing) = verifications
            .last_mut()
            .filter(|existing| existing.seed_id == verification.seed_id)
        {
            existing.official_controller_continuity |= verification.official_controller_continuity;
            existing.authorized_reissue |= verification.authorized_reissue;
            existing.verified_migration |= verification.verified_migration;
            existing.official_collection_relation |= verification.official_collection_relation;
            existing.complete &= verification.complete;
            existing.evidence_keys.extend(verification.evidence_keys);
            existing.evidence_keys.sort();
            existing.evidence_keys.dedup();
            existing.failures.extend(verification.failures);
            existing.failures.sort();
            existing.failures.dedup();
        } else {
            verifications.push(verification);
        }
    }
    bundle.relation_verifications = verifications;
    bundle.provenance.sort();
    bundle.provenance.dedup();
    bundle.quality.failures.sort();
    bundle.quality.failures.dedup();
    Ok(())
}

fn reconcile_duplicate_events(events: &mut Vec<NormalizedEvent>) {
    let mut exact = AHashSet::with_capacity(events.len());
    events.retain(|event| {
        let mut identity_free = event.clone();
        identity_free.event_index = 0;
        exact.insert(identity_free)
    });

    // Transfer providers and marketplace providers commonly describe the same
    // NFT movement independently. Keep the sale because it carries the payment
    // attribution; retaining both would double the propagation and gas facts.
    let sales = events
        .iter()
        .filter(|event| event.is_nft_sale())
        .filter_map(nft_movement_key)
        .collect::<AHashSet<_>>();
    events.retain(|event| {
        event.kind != EventKind::Transfer
            || nft_movement_key(event).is_none_or(|key| !sales.contains(&key))
    });
}

fn nft_movement_key(event: &NormalizedEvent) -> Option<NftMovementKey> {
    Some((
        event.chain,
        event.tx_id.clone(),
        event.nft.clone()?,
        event.from.clone()?,
        event.to.clone()?,
    ))
}

pub(crate) fn canonicalize_evidence_identities(bundle: &mut EvidenceBundle) {
    canonicalize_arc(
        bundle.candidate.chain,
        &mut bundle.candidate.contract_address,
        false,
    );
    for event in &mut bundle.events {
        canonicalize_arc(event.chain, &mut event.tx_id, false);
        for address in [
            &mut event.from,
            &mut event.to,
            &mut event.fee_payer,
            &mut event.payment_payer,
            &mut event.payment_recipient,
        ]
        .into_iter()
        .flatten()
        {
            canonicalize_arc(event.chain, address, false);
        }
        if let Some(nft) = &mut event.nft {
            canonicalize_nft(nft);
        }
    }
    for (nft, holder) in &mut bundle.holders {
        canonicalize_nft(nft);
        canonicalize_arc(nft.chain, holder, false);
    }
    for controller in &mut bundle.controllers {
        canonicalize_arc(bundle.candidate.chain, controller, false);
    }
}

fn canonicalize_nft(nft: &mut crate::model::NftKey) {
    canonicalize_arc(nft.chain, &mut nft.contract_address, false);
    canonicalize_arc(nft.chain, &mut nft.token_id, true);
}

fn canonicalize_arc(chain: crate::model::ChainId, value: &mut Arc<str>, token_id: bool) {
    if !chain.is_evm() {
        let trimmed = value.trim();
        if trimmed.len() != value.len() {
            *value = Arc::from(trimmed);
        }
        return;
    }
    let trimmed = value.trim();
    if token_id && !trimmed.is_empty() && trimmed.bytes().all(|byte| byte.is_ascii_digit()) {
        let digits = trimmed.trim_start_matches('0');
        let normalized = if digits.is_empty() { "0" } else { digits };
        if normalized != value.as_ref() {
            *value = Arc::from(normalized);
        }
        return;
    }
    if trimmed.len() == value.len() && !trimmed.bytes().any(|byte| byte.is_ascii_uppercase()) {
        return;
    }
    *value = Arc::from(trimmed.to_ascii_lowercase());
}

fn intern_evidence_strings(bundle: &mut EvidenceBundle) {
    let mut values = AHashSet::<Arc<str>>::new();
    intern_arc(&mut bundle.candidate.contract_address, &mut values);
    for event in &mut bundle.events {
        intern_arc(&mut event.tx_id, &mut values);
        if let Some(address) = &mut event.from {
            intern_arc(address, &mut values);
        }
        if let Some(address) = &mut event.to {
            intern_arc(address, &mut values);
        }
        if let Some(address) = &mut event.fee_payer {
            intern_arc(address, &mut values);
        }
        if let Some(address) = &mut event.payment_payer {
            intern_arc(address, &mut values);
        }
        if let Some(address) = &mut event.payment_recipient {
            intern_arc(address, &mut values);
        }
        if let Some(nft) = &mut event.nft {
            intern_arc(&mut nft.contract_address, &mut values);
            intern_arc(&mut nft.token_id, &mut values);
        }
    }
    for (nft, holder) in &mut bundle.holders {
        intern_arc(&mut nft.contract_address, &mut values);
        intern_arc(&mut nft.token_id, &mut values);
        intern_arc(holder, &mut values);
    }
    for controller in &mut bundle.controllers {
        intern_arc(controller, &mut values);
    }
    for verification in &mut bundle.relation_verifications {
        for evidence_key in &mut verification.evidence_keys {
            intern_arc(evidence_key, &mut values);
        }
    }
}

fn intern_arc(value: &mut Arc<str>, values: &mut AHashSet<Arc<str>>) {
    if let Some(existing) = values.get(value.as_ref()) {
        *value = existing.clone();
    } else {
        values.insert(value.clone());
    }
}

pub fn analyze_candidate(bundle: &EvidenceBundle, analysis_timestamp: i64) -> CandidateFacts {
    let events = &bundle.events;
    let lifecycle = lifecycle::build_lifecycle(
        bundle.deployment_timestamp,
        bundle.duplicate_content_timestamp,
        events,
        matches!(
            bundle.quality.histories,
            Some(crate::model::EvidenceStatus::Complete | crate::model::EvidenceStatus::Empty)
        ),
        analysis_timestamp,
    );
    let graph =
        propagation::PropagationGraph::build_filtered(events, |event| event.is_nft_propagation());
    let components = graph.strongly_connected_components();
    let attribution = attribution::attribute_addresses_detailed(
        &bundle.candidate,
        events,
        &bundle.controllers,
        &bundle.holders,
    );
    let propagation = propagation::analyze(events, &attribution.roles, &bundle.holders);
    let behavior_analysis =
        behavior::detect_behaviors(events, &graph, &attribution.roles, &components);
    let economics_analysis = economics::compute_candidate_economics_detailed_at(
        &bundle.candidate.contract_address,
        events,
        &attribution.roles,
        &bundle.holders,
        lifecycle.first_activity_timestamp,
        analysis_timestamp,
    );
    let economics = economics_analysis.facts;
    let honest_buyers = build_honest_buyers(
        events,
        &bundle.holders,
        &attribution.roles,
        &behavior_analysis.instances,
        lifecycle.first_activity_timestamp,
        analysis_timestamp,
    );
    CandidateFacts {
        candidate: bundle.candidate.clone(),
        analysis_timestamp,
        lifecycle,
        propagation: propagation.facts,
        economics,
        gas_cost_records: economics_analysis.gas_cost_records,
        behaviors: behavior_analysis.facts,
        behavior_instances: behavior_analysis.instances,
        address_count: attribution.roles.len() as u64,
        nft_count: propagation.nft_count,
        transaction_count: propagation.transaction_count,
        event_count: events.len() as u64,
        event_kind_counts: propagation.event_kind_counts,
        address_attributions: attribution.records,
        honest_buyers,
        quality: bundle.quality.clone(),
    }
}

fn build_honest_buyers(
    events: &[crate::model::NormalizedEvent],
    holders: &[(crate::model::NftKey, Arc<str>)],
    roles: &std::collections::BTreeMap<Arc<str>, attribution::AddressRole>,
    instances: &[crate::model::BehaviorInstance],
    first_activity_timestamp: Option<i64>,
    analysis_timestamp: i64,
) -> Vec<crate::model::HonestBuyerFact> {
    #[derive(Default)]
    struct Acquisition {
        transactions: Vec<Arc<str>>,
        paid_native: i128,
        paid_usd_micros: i128,
        first_purchase_timestamp: Option<i64>,
    }

    let honest_roles = roles
        .iter()
        .filter(|(_, role)| {
            matches!(
                role,
                attribution::AddressRole::LikelyVictim | attribution::AddressRole::CorruptedVictim
            )
        })
        .map(|(address, role)| (address.clone(), *role))
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut held = std::collections::BTreeMap::<Arc<str>, Vec<crate::model::NftKey>>::new();
    let mut held_pairs = ahash::AHashSet::<(&str, &crate::model::NftKey)>::new();
    for (nft, address) in holders {
        if honest_roles.contains_key(address) {
            held.entry(address.clone()).or_default().push(nft.clone());
            held_pairs.insert((address.as_ref(), nft));
        }
    }
    let mut acquisitions = std::collections::BTreeMap::<Arc<str>, Acquisition>::new();
    for event in events.iter().filter(|event| {
        (event.kind == crate::model::EventKind::Mint || event.is_nft_sale())
            && (event.native_amount.unwrap_or(0) > 0 || event.usd_micros.unwrap_or(0) > 0)
    }) {
        let Some(nft) = &event.nft else {
            continue;
        };
        let Some(address) = event.payment_payer.as_ref().or(event.to.as_ref()) else {
            continue;
        };
        if !held_pairs.contains(&(address.as_ref(), nft)) {
            continue;
        }
        let acquisition = acquisitions.entry(address.clone()).or_default();
        acquisition.transactions.push(event.tx_id.clone());
        acquisition.paid_native = acquisition
            .paid_native
            .saturating_add(event.native_amount.unwrap_or(0).max(0));
        acquisition.paid_usd_micros = acquisition
            .paid_usd_micros
            .saturating_add(event.usd_micros.unwrap_or(0).max(0));
        acquisition.first_purchase_timestamp = acquisition
            .first_purchase_timestamp
            .into_iter()
            .chain(event.timestamp)
            .min();
    }
    let mut linked = std::collections::BTreeMap::<
        Arc<str>,
        std::collections::BTreeSet<crate::model::BehaviorKind>,
    >::new();
    for instance in instances {
        for address in instance
            .linked_buyers
            .iter()
            .chain(instance.addresses.iter())
            .filter(|address| honest_roles.contains_key(*address))
        {
            linked
                .entry(address.clone())
                .or_default()
                .insert(instance.kind);
        }
    }
    honest_roles
        .into_iter()
        .filter_map(|(address, role)| {
            let mut held_nfts = held.remove(&address)?;
            let mut acquisition = acquisitions.remove(&address).unwrap_or_default();
            held_nfts.sort();
            held_nfts.dedup();
            acquisition.transactions.sort();
            acquisition.transactions.dedup();
            let first_purchase = acquisition.first_purchase_timestamp;
            Some(crate::model::HonestBuyerFact {
                address: address.clone(),
                role,
                held_nfts,
                acquisition_transactions: acquisition.transactions,
                paid_native: acquisition.paid_native,
                paid_usd_micros: acquisition.paid_usd_micros,
                first_purchase_timestamp: first_purchase,
                first_activity_to_first_purchase_seconds: first_activity_timestamp
                    .zip(first_purchase)
                    .and_then(|(first, purchase)| purchase.checked_sub(first))
                    .filter(|value| *value >= 0),
                holding_seconds: first_purchase
                    .and_then(|purchase| analysis_timestamp.checked_sub(purchase))
                    .filter(|value| *value >= 0),
                linked_behaviors: linked
                    .remove(&address)
                    .unwrap_or_default()
                    .into_iter()
                    .collect(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ChainId, NftKey, ValueChannel};

    #[test]
    fn canonical_identity_fast_path_reuses_normalized_arc() {
        let original: Arc<str> = Arc::from("0xabcdef");
        let mut normalized = original.clone();
        canonicalize_arc(ChainId::Ethereum, &mut normalized, false);
        assert!(Arc::ptr_eq(&original, &normalized));

        let mut mixed: Arc<str> = Arc::from(" 0xAbCd ");
        canonicalize_arc(ChainId::Ethereum, &mut mixed, false);
        assert_eq!(mixed.as_ref(), "0xabcd");

        let mut token: Arc<str> = Arc::from("00042");
        canonicalize_arc(ChainId::Ethereum, &mut token, true);
        assert_eq!(token.as_ref(), "42");
    }

    #[test]
    fn reconciliation_deduplicates_provider_copies_and_prefers_sale() {
        let nft = NftKey {
            chain: ChainId::Ethereum,
            contract_address: Arc::from("0xcollection"),
            token_id: Arc::from("7"),
        };
        let event = |event_index, kind, channel| NormalizedEvent {
            chain: ChainId::Ethereum,
            tx_id: Arc::from("0xtx"),
            event_index,
            timestamp: Some(10),
            block_number: Some(20),
            kind,
            channel: Some(channel),
            from: Some(Arc::from("0xfrom")),
            to: Some(Arc::from("0xto")),
            fee_payer: None,
            payment_payer: None,
            payment_recipient: None,
            nft: Some(nft.clone()),
            native_amount: None,
            usd_micros: None,
            gas_native: None,
            gas_usd_micros: None,
            marketplace_fee_native: None,
            marketplace_fee_usd_micros: None,
        };
        let mut events = vec![
            event(1, EventKind::Transfer, ValueChannel::Transfer),
            event(2, EventKind::Transfer, ValueChannel::Transfer),
            event(3, EventKind::Sale, ValueChannel::SalePayment),
        ];

        reconcile_duplicate_events(&mut events);

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, EventKind::Sale);
    }
}
