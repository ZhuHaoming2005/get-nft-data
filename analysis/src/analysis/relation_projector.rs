use crate::model::{
    AddressRoleKind, BehaviorFacts, BehaviorInstance, CandidateFacts, CandidateId, ChainId,
    EconomicFacts, EvidenceBundle, NftKey, NftSelection, RelationClassification, RelationDelta,
    RelationLabel, SeedCandidateRelation,
};
use ahash::{AHashMap, AHashSet};
use std::collections::BTreeMap;
use std::sync::Arc;

pub struct RelationProjection {
    pub deltas: Vec<RelationDelta>,
    pub suspected_economics: EconomicFacts,
    pub suspected_behaviors: BehaviorFacts,
    pub suspected_behavior_instances: Vec<BehaviorInstance>,
    pub suspected_address_roles: BTreeMap<Arc<str>, AddressRoleKind>,
    /// `(seed_chain, candidate_chain)` → facts for the cell's selection union.
    /// When the candidate is mixed/suspected, every matched cell is elevated and
    /// includes all relations in that cell (not only originally suspected labels).
    pub matrix_suspected: BTreeMap<(ChainId, ChainId), ProjectedFacts>,
}

#[derive(Clone, Default)]
pub struct ProjectedFacts {
    pub economics: EconomicFacts,
    pub behaviors: BehaviorFacts,
    pub behavior_instances: Vec<BehaviorInstance>,
    pub address_roles: BTreeMap<Arc<str>, AddressRoleKind>,
}

pub fn project_relations(
    candidate_id: CandidateId,
    facts: &CandidateFacts,
    relations: &[SeedCandidateRelation],
    labels: &[RelationLabel],
    evidence: Option<&EvidenceBundle>,
) -> RelationProjection {
    let labels = labels
        .iter()
        .map(|label| (label.seed_id, &label.classification))
        .collect::<BTreeMap<_, _>>();
    let mut cache = ProjectionCache::new(facts, evidence);
    let mut output = relations
        .iter()
        .filter(|relation| !relation.incomplete)
        .map(|relation| {
            let suspected = matches!(
                labels.get(&relation.seed_id),
                Some(RelationClassification::SuspectedDuplicate { .. })
            );
            let (economics, behaviors) = cache.summary(suspected, &relation.selection);
            RelationDelta {
                seed_id: relation.seed_id,
                seed_chain: relation.seed.chain,
                candidate_id,
                candidate: facts.candidate.clone(),
                selection: relation.selection.clone(),
                suspected,
                economics,
                behaviors,
            }
        })
        .collect::<Vec<_>>();
    output.sort_by_key(|delta| delta.seed_id);
    let candidate_suspected = output.iter().any(|delta| delta.suspected);
    let mut suspected_selection: Option<NftSelection> = None;
    for delta in output.iter().filter(|delta| delta.suspected) {
        match &mut suspected_selection {
            Some(selection) => selection.union_assign(delta.selection.clone()),
            None => suspected_selection = Some(delta.selection.clone()),
        }
    }
    let suspected = suspected_selection
        .as_mut()
        .map(|selection| {
            selection.normalize();
            cache.full(true, selection)
        })
        .unwrap_or_default();

    let mut matrix_groups = BTreeMap::<(ChainId, ChainId), NftSelection>::new();
    if candidate_suspected {
        for delta in &output {
            let key = (delta.seed_chain, delta.candidate.chain);
            matrix_groups
                .entry(key)
                .and_modify(|selection| selection.union_assign(delta.selection.clone()))
                .or_insert_with(|| delta.selection.clone());
        }
    }
    let mut matrix_suspected = BTreeMap::new();
    for (key, mut selection) in matrix_groups {
        selection.normalize();
        matrix_suspected.insert(key, cache.full(true, &selection));
    }

    RelationProjection {
        deltas: output,
        suspected_economics: suspected.economics,
        suspected_behaviors: suspected.behaviors,
        suspected_behavior_instances: suspected.behavior_instances,
        suspected_address_roles: suspected.address_roles,
        matrix_suspected,
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum SelectionKey {
    AllInContract,
    Explicit(Vec<NftKey>),
}

impl SelectionKey {
    fn normalized(selection: &NftSelection) -> Self {
        match selection {
            NftSelection::AllInContract { .. } => Self::AllInContract,
            NftSelection::Explicit { nfts } => {
                let mut nfts = nfts.clone();
                nfts.sort_unstable();
                nfts.dedup();
                Self::Explicit(nfts)
            }
        }
    }
}

/// Candidate-local memoization for both historical projection semantics.
/// Relation summaries reuse candidate-wide roles and behavior instances, while
/// union/matrix views recompute the full selected-evidence graph. Each semantic
/// branch computes a normalized selection at most once.
struct ProjectionCache<'a> {
    facts: &'a CandidateFacts,
    evidence: Option<&'a EvidenceBundle>,
    summaries: AHashMap<SelectionKey, (EconomicFacts, BehaviorFacts)>,
    full: AHashMap<SelectionKey, ProjectedFacts>,
    #[cfg(test)]
    summary_computations: usize,
    #[cfg(test)]
    full_computations: usize,
}

impl<'a> ProjectionCache<'a> {
    fn new(facts: &'a CandidateFacts, evidence: Option<&'a EvidenceBundle>) -> Self {
        Self {
            facts,
            evidence,
            summaries: AHashMap::new(),
            full: AHashMap::new(),
            #[cfg(test)]
            summary_computations: 0,
            #[cfg(test)]
            full_computations: 0,
        }
    }

    fn summary(
        &mut self,
        suspected: bool,
        selection: &NftSelection,
    ) -> (EconomicFacts, BehaviorFacts) {
        let Some(evidence) = self.evidence.filter(|_| suspected) else {
            return Default::default();
        };
        let key = SelectionKey::normalized(selection);
        if !self.summaries.contains_key(&key) {
            let summary = compute_projected_summary(self.facts, evidence, &key);
            self.summaries.insert(key.clone(), summary);
            #[cfg(test)]
            {
                self.summary_computations += 1;
            }
        }
        self.summaries
            .get(&key)
            .expect("selection summary inserted above")
            .clone()
    }

    fn full(&mut self, suspected: bool, selection: &NftSelection) -> ProjectedFacts {
        let Some(evidence) = self.evidence.filter(|_| suspected) else {
            return ProjectedFacts::default();
        };
        let key = SelectionKey::normalized(selection);
        if !self.full.contains_key(&key) {
            let projected = compute_projected_facts(self.facts, evidence, &key);
            self.full.insert(key.clone(), projected);
            #[cfg(test)]
            {
                self.full_computations += 1;
            }
        }
        self.full
            .get(&key)
            .expect("full selection projection inserted above")
            .clone()
    }
}

/// Preserve the historical relation-delta semantics: roles and behavior
/// instances are classified once for the complete candidate, then filtered to
/// the relation's NFT set. This is intentionally distinct from the full
/// union/matrix projection, which re-runs attribution and graph analysis on the
/// selected evidence itself.
fn compute_projected_summary(
    facts: &CandidateFacts,
    evidence: &EvidenceBundle,
    selection: &SelectionKey,
) -> (EconomicFacts, BehaviorFacts) {
    let SelectionKey::Explicit(nfts) = selection else {
        return (facts.economics.clone(), facts.behaviors.clone());
    };
    let selected = nfts.iter().collect::<AHashSet<_>>();
    let roles = facts
        .address_attributions
        .iter()
        .filter(|(_, attribution)| {
            attribution.evidence.iter().any(|record| {
                record
                    .token
                    .as_ref()
                    .is_none_or(|nft| selected.contains(nft))
            })
        })
        .map(|(address, attribution)| (address.clone(), attribution.role))
        .collect::<BTreeMap<_, _>>();
    let behaviors = projected_behavior_facts(facts.behavior_instances.iter().filter(|instance| {
        instance.nfts.is_empty() || instance.nfts.iter().any(|nft| selected.contains(nft))
    }));
    let economics = crate::analysis::economics::compute_candidate_economics_iter_at(
        &facts.candidate.contract_address,
        evidence
            .events
            .iter()
            .filter(|event| event.nft.as_ref().is_none_or(|nft| selected.contains(nft))),
        &roles,
        evidence
            .holders
            .iter()
            .filter(|(nft, _)| selected.contains(nft)),
        facts.lifecycle.first_activity_timestamp,
        facts.analysis_timestamp,
    );
    (economics, behaviors)
}

fn projected_behavior_facts<'a>(
    instances: impl Iterator<Item = &'a BehaviorInstance>,
) -> BehaviorFacts {
    let mut facts = BehaviorFacts::default();
    for instance in instances {
        match instance.kind {
            crate::model::BehaviorKind::WashTrading => facts.wash_cycles += 1,
            crate::model::BehaviorKind::PumpAndExit => facts.pump_and_exit += 1,
            crate::model::BehaviorKind::SybilDistribution => facts.sybil_distribution += 1,
            crate::model::BehaviorKind::FraudRevenue => facts.fraud_revenue += 1,
            crate::model::BehaviorKind::Poisoning => facts.poisoning += 1,
            crate::model::BehaviorKind::LayeredTransfer => facts.layered_transfer += 1,
            crate::model::BehaviorKind::InventoryConcentration => {
                facts.inventory_concentration += 1;
            }
        }
    }
    facts
}

fn compute_projected_facts(
    facts: &CandidateFacts,
    evidence: &EvidenceBundle,
    selection: &SelectionKey,
) -> ProjectedFacts {
    let SelectionKey::Explicit(nfts) = selection else {
        return ProjectedFacts {
            economics: facts.economics.clone(),
            behaviors: facts.behaviors.clone(),
            behavior_instances: facts.behavior_instances.clone(),
            address_roles: facts
                .address_attributions
                .iter()
                .map(|(address, attribution)| (address.clone(), attribution.role))
                .collect(),
        };
    };
    let selected = nfts.iter().collect::<AHashSet<_>>();
    let events = evidence
        .events
        .iter()
        .filter(|event| event.nft.as_ref().is_none_or(|nft| selected.contains(nft)))
        .cloned()
        .collect::<Vec<_>>();
    let holders = evidence
        .holders
        .iter()
        .filter(|(nft, _)| selected.contains(nft))
        .cloned()
        .collect::<Vec<_>>();
    let roles = crate::analysis::attribution::attribute_addresses(
        &facts.candidate,
        &events,
        &evidence.controllers,
        &holders,
    );
    let graph = crate::analysis::propagation::PropagationGraph::build_filtered(&events, |event| {
        event.is_nft_propagation()
    });
    let components = graph.strongly_connected_components();
    let analysis =
        crate::analysis::behavior::detect_behaviors(&events, &graph, &roles, &components);
    let economics = crate::analysis::economics::compute_candidate_economics_detailed_at(
        &facts.candidate.contract_address,
        &events,
        &roles,
        &holders,
        facts.lifecycle.first_activity_timestamp,
        facts.analysis_timestamp,
    )
    .facts;
    ProjectedFacts {
        economics,
        behaviors: analysis.facts,
        behavior_instances: analysis.instances,
        address_roles: roles,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{
        ChainId, ContractKey, EventKind, EvidenceQuality, EvidenceStatus, NormalizedEvent,
        RelationClassification, SeedId,
    };
    use std::sync::Arc;

    #[test]
    fn explicit_relation_projects_only_selected_nft_economics() {
        let candidate = ContractKey::new(ChainId::Ethereum, "copy");
        let nft = |token: &str| NftKey {
            chain: candidate.chain,
            contract_address: candidate.contract_address.clone(),
            token_id: Arc::from(token),
        };
        let sale = |index, nft: NftKey, amount| NormalizedEvent {
            chain: candidate.chain,
            tx_id: Arc::from(format!("tx-{index}")),
            event_index: 0,
            timestamp: Some(index),
            block_number: Some(index as u64),
            kind: EventKind::Sale,
            channel: None,
            from: Some(Arc::from("operator")),
            to: Some(Arc::from(format!("buyer-{index}"))),
            fee_payer: Some(Arc::from("operator")),
            payment_payer: Some(Arc::from(format!("buyer-{index}"))),
            payment_recipient: Some(Arc::from("operator")),
            nft: Some(nft),
            native_amount: Some(amount),
            usd_micros: Some(amount),
            gas_native: None,
            gas_usd_micros: None,
            marketplace_fee_native: None,
            marketplace_fee_usd_micros: None,
        };
        let selected = nft("1");
        let excluded = nft("2");
        let evidence = EvidenceBundle {
            candidate: candidate.clone(),
            deployment_timestamp: None,
            duplicate_content_timestamp: None,
            events: vec![sale(1, selected.clone(), 10), sale(2, excluded.clone(), 20)],
            holders: vec![
                (selected.clone(), Arc::from("buyer-1")),
                (excluded.clone(), Arc::from("buyer-2")),
            ],
            controllers: vec![Arc::from("operator")],
            relation_verifications: Vec::new(),
            provenance: Vec::new(),
            quality: EvidenceQuality {
                histories: Some(EvidenceStatus::Complete),
                ..Default::default()
            },
        };
        let facts = crate::analysis::analyze_candidate(&evidence, 3);
        assert_eq!(facts.economics.operator_output_native, 30);
        assert_eq!(facts.honest_buyers.len(), 2);
        assert_eq!(facts.honest_buyers[0].held_nfts.len(), 1);
        assert_eq!(facts.honest_buyers[0].paid_native, 10);
        assert_eq!(facts.honest_buyers[0].first_purchase_timestamp, Some(1));
        assert_eq!(
            facts.honest_buyers[0].first_activity_to_first_purchase_seconds,
            Some(0)
        );
        assert_eq!(facts.honest_buyers[0].holding_seconds, Some(2));
        let mut cache = ProjectionCache::new(&facts, Some(&evidence));
        cache.summary(
            true,
            &NftSelection::Explicit {
                nfts: vec![excluded.clone(), selected.clone(), selected.clone()],
            },
        );
        cache.summary(
            true,
            &NftSelection::Explicit {
                nfts: vec![selected.clone(), excluded.clone()],
            },
        );
        cache.full(
            true,
            &NftSelection::Explicit {
                nfts: vec![excluded.clone(), selected.clone(), selected.clone()],
            },
        );
        cache.full(
            true,
            &NftSelection::Explicit {
                nfts: vec![selected.clone(), excluded.clone()],
            },
        );
        assert_eq!(cache.summary_computations, 1);
        assert_eq!(cache.full_computations, 1);
        let relation = SeedCandidateRelation {
            seed_id: SeedId(0),
            seed: ContractKey::new(ChainId::Ethereum, "seed"),
            candidate_id: CandidateId(1),
            candidate,
            dimensions: crate::model::Dimension::TokenUri.bit(),
            selection: NftSelection::Explicit {
                nfts: vec![selected],
            },
            evidence: Vec::new(),
            incomplete: false,
        };
        let labels = vec![RelationLabel {
            seed_id: SeedId(0),
            candidate_id: CandidateId(1),
            classification: RelationClassification::SuspectedDuplicate {
                legit_verification_complete: true,
            },
        }];
        let projection = project_relations(
            CandidateId(1),
            &facts,
            std::slice::from_ref(&relation),
            &labels,
            Some(&evidence),
        );
        assert_eq!(projection.deltas[0].economics.operator_output_native, 10);
        assert_eq!(projection.deltas[0].economics.honest_loss_native, 10);
        assert_eq!(projection.suspected_economics.operator_output_native, 10);
        assert!(!projection.suspected_address_roles.contains_key("buyer-2"));

        let second = SeedCandidateRelation {
            seed_id: SeedId(1),
            selection: NftSelection::Explicit {
                nfts: vec![excluded],
            },
            ..relation.clone()
        };
        let union = project_relations(
            CandidateId(1),
            &facts,
            &[relation, second],
            &[
                labels[0].clone(),
                RelationLabel {
                    seed_id: SeedId(1),
                    candidate_id: CandidateId(1),
                    classification: RelationClassification::SuspectedDuplicate {
                        legit_verification_complete: true,
                    },
                },
            ],
            Some(&evidence),
        );
        assert_eq!(union.deltas[1].economics.operator_output_native, 20);
        assert_eq!(union.suspected_economics.operator_output_native, 30);
    }

    #[test]
    fn relation_summary_preserves_candidate_context_while_union_reprojects_selection() {
        let candidate = ContractKey::new(ChainId::Ethereum, "copy");
        let nft = |token: &str| NftKey {
            chain: candidate.chain,
            contract_address: candidate.contract_address.clone(),
            token_id: Arc::from(token),
        };
        let selected = nft("selected");
        let excluded = nft("excluded");
        let event = |index: u32,
                     kind: EventKind,
                     from: &str,
                     to: &str,
                     nft: NftKey,
                     amount: Option<i128>| NormalizedEvent {
            chain: candidate.chain,
            tx_id: Arc::from(format!("tx-{index}")),
            event_index: 0,
            timestamp: Some(i64::from(index)),
            block_number: Some(u64::from(index)),
            kind,
            channel: None,
            from: Some(Arc::from(from)),
            to: Some(Arc::from(to)),
            fee_payer: None,
            payment_payer: (kind == EventKind::Sale).then(|| Arc::from(to)),
            payment_recipient: (kind == EventKind::Sale).then(|| Arc::from(from)),
            nft: Some(nft),
            native_amount: amount,
            usd_micros: amount,
            gas_native: None,
            gas_usd_micros: None,
            marketplace_fee_native: None,
            marketplace_fee_usd_micros: None,
        };
        let evidence = EvidenceBundle {
            candidate: candidate.clone(),
            deployment_timestamp: None,
            duplicate_content_timestamp: None,
            events: vec![
                event(
                    1,
                    EventKind::Sale,
                    "operator",
                    "buyer",
                    selected.clone(),
                    Some(10),
                ),
                event(
                    2,
                    EventKind::Transfer,
                    "buyer",
                    "sink",
                    excluded.clone(),
                    None,
                ),
            ],
            holders: vec![
                (selected.clone(), Arc::from("buyer")),
                (excluded, Arc::from("sink")),
            ],
            controllers: vec![Arc::from("operator")],
            relation_verifications: Vec::new(),
            provenance: Vec::new(),
            quality: EvidenceQuality {
                histories: Some(EvidenceStatus::Complete),
                ..Default::default()
            },
        };
        let facts = crate::analysis::analyze_candidate(&evidence, 3);
        assert_eq!(
            facts.address_attributions["buyer"].role,
            AddressRoleKind::CorruptedVictim
        );
        let relation = SeedCandidateRelation {
            seed_id: SeedId(0),
            seed: ContractKey::new(ChainId::Ethereum, "seed"),
            candidate_id: CandidateId(1),
            candidate,
            dimensions: crate::model::Dimension::TokenUri.bit(),
            selection: NftSelection::Explicit {
                nfts: vec![selected],
            },
            evidence: Vec::new(),
            incomplete: false,
        };
        let projection = project_relations(
            CandidateId(1),
            &facts,
            &[relation],
            &[RelationLabel {
                seed_id: SeedId(0),
                candidate_id: CandidateId(1),
                classification: RelationClassification::SuspectedDuplicate {
                    legit_verification_complete: true,
                },
            }],
            Some(&evidence),
        );

        // Historical relation deltas retain the candidate-wide CorruptedVictim
        // role and therefore do not classify this payment as honest loss.
        assert_eq!(projection.deltas[0].economics.honest_loss_native, 0);
        // The suspected union historically re-attributed the selected evidence,
        // where the buyer is a LikelyVictim still holding the selected NFT.
        assert_eq!(projection.suspected_economics.honest_loss_native, 10);
        assert_eq!(
            projection.suspected_address_roles["buyer"],
            AddressRoleKind::LikelyVictim
        );
    }

    #[test]
    fn explicit_relation_projects_common_behavior_instances_by_nft() {
        let candidate = ContractKey::new(ChainId::Ethereum, "copy");
        let nft = |token: &str| NftKey {
            chain: candidate.chain,
            contract_address: candidate.contract_address.clone(),
            token_id: Arc::from(token),
        };
        let sale = |index: u32, from: &str, to: &str, nft: NftKey| NormalizedEvent {
            chain: candidate.chain,
            tx_id: Arc::from(format!("tx-{index}")),
            event_index: 0,
            timestamp: Some(i64::from(index)),
            block_number: Some(u64::from(index)),
            kind: EventKind::Sale,
            channel: None,
            from: Some(Arc::from(from)),
            to: Some(Arc::from(to)),
            fee_payer: None,
            payment_payer: Some(Arc::from(to)),
            payment_recipient: Some(Arc::from(from)),
            nft: Some(nft),
            native_amount: None,
            usd_micros: None,
            gas_native: None,
            gas_usd_micros: None,
            marketplace_fee_native: None,
            marketplace_fee_usd_micros: None,
        };
        let selected = nft("1");
        let excluded = nft("2");
        let evidence = EvidenceBundle {
            candidate: candidate.clone(),
            deployment_timestamp: None,
            duplicate_content_timestamp: None,
            events: vec![
                sale(1, "a", "b", selected.clone()),
                sale(2, "b", "a", selected.clone()),
                sale(3, "c", "d", excluded.clone()),
                sale(4, "d", "c", excluded),
            ],
            holders: Vec::new(),
            controllers: vec![Arc::from("a"), Arc::from("c")],
            relation_verifications: Vec::new(),
            provenance: Vec::new(),
            quality: EvidenceQuality {
                histories: Some(EvidenceStatus::Complete),
                ..Default::default()
            },
        };
        let facts = crate::analysis::analyze_candidate(&evidence, 5);
        assert_eq!(facts.behaviors.wash_cycles, 2);
        let relation = SeedCandidateRelation {
            seed_id: SeedId(0),
            seed: ContractKey::new(ChainId::Ethereum, "seed"),
            candidate_id: CandidateId(1),
            candidate,
            dimensions: crate::model::Dimension::TokenUri.bit(),
            selection: NftSelection::Explicit {
                nfts: vec![selected],
            },
            evidence: Vec::new(),
            incomplete: false,
        };
        let projection = project_relations(
            CandidateId(1),
            &facts,
            &[relation],
            &[RelationLabel {
                seed_id: SeedId(0),
                candidate_id: CandidateId(1),
                classification: RelationClassification::SuspectedDuplicate {
                    legit_verification_complete: true,
                },
            }],
            Some(&evidence),
        );
        assert_eq!(projection.deltas[0].behaviors.wash_cycles, 1);
        assert_eq!(projection.suspected_behaviors.wash_cycles, 1);
        assert_eq!(
            projection.suspected_behavior_instances.len(),
            projection.suspected_behaviors.wash_cycles as usize
        );
    }
}
