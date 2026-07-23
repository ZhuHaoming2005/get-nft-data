//! Deep analysis for enriched candidates (Task 12).

mod attribution;
mod behavior;
mod economics;
mod graph;
mod legit;
mod lifecycle;

pub use attribution::{AddressAttribution, AddressEvidence, AddressEvidenceKind, AddressRole};
pub use behavior::{BehaviorFacts, BehaviorInstance, BehaviorKind};
pub use economics::{EconomicFacts, EconomicsQuality};
pub use graph::AddressGraph;
pub use legit::LegitClassification;
pub use lifecycle::{LifecycleFacts, ValueFlowFacts};

use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use crate::Analysis2Error;
use crate::enrich::EvidenceBundle;
use crate::entity::{ContractId, ResidentStore};

const PARALLEL_CANDIDATE_EVENT_THRESHOLD: usize = 2_048;

/// Paper / CLI knobs for behavior detectors.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PaperConfig {
    /// Minimum SCC size for a wash cycle (default 2).
    pub min_cycle_size: usize,
    /// Minimum distinct addresses on a layered transfer path (default 3).
    pub layered_path_addresses: usize,
    /// Minimum DAG fan-out for star centers (default 3).
    pub fan_out: usize,
    /// Top-fraction concentration threshold for aggregate reporting (default 0.10).
    pub top_concentration_fraction: f64,
    /// Unix timestamp used for holding-time windows.
    pub analysis_timestamp: i64,
}

impl Default for PaperConfig {
    fn default() -> Self {
        Self {
            min_cycle_size: 2,
            layered_path_addresses: 3,
            fan_out: 3,
            top_concentration_fraction: 0.10,
            analysis_timestamp: chrono::Utc::now().timestamp(),
        }
    }
}

/// Per-candidate deep analysis product.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CandidateAnalysis {
    pub contract_id: ContractId,
    pub chain: String,
    pub address: String,
    pub legit: LegitClassification,
    /// Per-seed relation classification keyed by `"chain:address"`.
    #[serde(default)]
    pub legit_by_seed: std::collections::BTreeMap<String, LegitClassification>,
    pub attribution: Vec<(String, AddressAttribution)>,
    pub lifecycle: LifecycleFacts,
    pub value_flow: ValueFlowFacts,
    pub behaviors: BehaviorFacts,
    pub behavior_instances: Vec<BehaviorInstance>,
    pub economics: EconomicFacts,
    pub economics_quality: EconomicsQuality,
    pub analysis_timestamp: i64,
}

/// Run deep analysis for one candidate using resident identity + evidence bundle.
pub fn analyze_candidate(
    store: &ResidentStore,
    contract: ContractId,
    evidence: &EvidenceBundle,
    cfg: &PaperConfig,
) -> Result<CandidateAnalysis, Analysis2Error> {
    let Some(contract_row) = store.contracts.get(contract as usize) else {
        return Err(Analysis2Error::invalid(format!(
            "unknown contract id {contract}"
        )));
    };
    if evidence.contract_id != contract {
        return Err(Analysis2Error::invalid(format!(
            "evidence contract_id {} != requested {contract}",
            evidence.contract_id
        )));
    }

    let legit = legit::classify(&evidence.legit);
    let legit_by_seed: std::collections::BTreeMap<String, LegitClassification> = evidence
        .relation_legit
        .iter()
        .map(|(k, v)| (k.clone(), legit::classify(v)))
        .collect();
    let transfer_graph = graph::AddressGraph::from_transfers(&evidence.transfers);
    let event_work = evidence
        .transfers
        .len()
        .saturating_add(evidence.sales.len())
        .saturating_add(evidence.holders.len())
        .saturating_add(evidence.value_flows.len());
    let parallel =
        event_work >= PARALLEL_CANDIDATE_EVENT_THRESHOLD && rayon::current_num_threads() > 1;
    let (transfer_sccs, attribution, lifecycle) = if parallel {
        let (transfer_sccs, (attribution, lifecycle)) = rayon::join(
            || transfer_graph.strongly_connected_components(),
            || {
                rayon::join(
                    || attribution::attribute_addresses(evidence, &transfer_graph),
                    || lifecycle::build_lifecycle(evidence, cfg.analysis_timestamp),
                )
            },
        );
        (transfer_sccs, attribution, lifecycle)
    } else {
        (
            transfer_graph.strongly_connected_components(),
            attribution::attribute_addresses(evidence, &transfer_graph),
            lifecycle::build_lifecycle(evidence, cfg.analysis_timestamp),
        )
    };
    let (value_flow, detected, economics, economics_quality) = if parallel {
        let (value_flow, (detected, (economics, economics_quality))) = rayon::join(
            || lifecycle::build_value_flow(evidence, &attribution.roles),
            || {
                rayon::join(
                    || {
                        behavior::detect_behaviors(
                            evidence,
                            &transfer_graph,
                            &transfer_sccs,
                            &attribution.roles,
                            cfg,
                        )
                    },
                    || {
                        economics::compute_economics(
                            evidence,
                            &attribution.roles,
                            cfg.analysis_timestamp,
                            &lifecycle,
                        )
                    },
                )
            },
        );
        (value_flow, detected, economics, economics_quality)
    } else {
        let value_flow = lifecycle::build_value_flow(evidence, &attribution.roles);
        let detected = behavior::detect_behaviors(
            evidence,
            &transfer_graph,
            &transfer_sccs,
            &attribution.roles,
            cfg,
        );
        let (economics, economics_quality) = economics::compute_economics(
            evidence,
            &attribution.roles,
            cfg.analysis_timestamp,
            &lifecycle,
        );
        (value_flow, detected, economics, economics_quality)
    };

    let mut attribution_rows: Vec<(String, AddressAttribution)> = attribution
        .records
        .into_iter()
        .collect();
    if attribution_rows.len() >= PARALLEL_CANDIDATE_EVENT_THRESHOLD {
        attribution_rows.par_sort_by(|left, right| left.0.cmp(&right.0));
    } else {
        attribution_rows.sort_by(|left, right| left.0.cmp(&right.0));
    }

    Ok(CandidateAnalysis {
        contract_id: contract,
        chain: store.chain_name(contract_row.chain_id).to_owned(),
        address: contract_row.address.clone(),
        legit,
        legit_by_seed,
        attribution: attribution_rows,
        lifecycle,
        value_flow,
        behaviors: detected.facts,
        behavior_instances: detected.instances,
        economics,
        economics_quality,
        analysis_timestamp: cfg.analysis_timestamp,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::enrich::{
        EvidenceQuality, EvidenceStatus, HolderRecord, LegitSignals, SaleEvent, TransferEvent,
        ValueFlowEdge, ValueFlowKind,
    };
    use crate::entity::{IdentityRow, SourceOrder};

    fn store_with_contract(chain: &str, address: &str) -> (ResidentStore, ContractId) {
        let mut store = ResidentStore::new();
        store
            .ingest_identity_row(IdentityRow {
                chain: chain.to_owned(),
                contract_address: address.to_owned(),
                token_id: "1".into(),
                name_norm: String::new(),
                token_uri_norm: String::new(),
                image_uri_norm: String::new(),
                source_order: SourceOrder {
                    file_ordinal: 0,
                    file_row_number: 0,
                },
            })
            .unwrap();
        let chain_id = *store.chain_ids.get(chain).unwrap();
        let contract = *store
            .contract_index
            .get(&(chain_id, address.to_owned()))
            .unwrap();
        (store, contract)
    }

    fn sale(tx: &str, token: &str, seller: &str, buyer: &str, ts: i64, usd: f64) -> SaleEvent {
        SaleEvent {
            tx_hash: tx.into(),
            token_id: token.into(),
            seller: seller.into(),
            buyer: buyer.into(),
            timestamp: Some(ts),
            block_number: Some(ts as u64),
            marketplace: None,
            native_amount: Some(usd),
            usd_amount: Some(usd),
            currency_symbol: Some("ETH".into()),
        }
    }

    fn transfer(
        tx: &str,
        token: &str,
        from: &str,
        to: &str,
        ts: i64,
        is_mint: bool,
    ) -> TransferEvent {
        TransferEvent {
            tx_hash: tx.into(),
            token_id: token.into(),
            from: from.into(),
            to: to.into(),
            timestamp: Some(ts),
            block_number: Some(ts as u64),
            is_mint,
            gas_native: None,
            fee_payer: None,
            mint_payment_native: None,
            mint_payment_usd: None,
        }
    }

    #[test]
    fn wash_cycle_two_node_scc_from_reciprocal_malicious_sales() {
        let (store, contract) = store_with_contract("ethereum", "0xcand");
        let mut evidence = EvidenceBundle::empty(contract, "ethereum", "0xcand");
        evidence.controllers = vec!["0xa".into()];
        evidence.sales = vec![
            sale("tx-0", "1", "0xa", "0xb", 10, 1.0),
            sale("tx-1", "1", "0xb", "0xa", 20, 1.0),
        ];
        evidence.quality.sales = EvidenceStatus::Complete;
        evidence.quality.transfers = EvidenceStatus::Empty;
        evidence.quality.holders = EvidenceStatus::Empty;

        let analysis = analyze_candidate(
            &store,
            contract,
            &evidence,
            &PaperConfig {
                analysis_timestamp: 100,
                ..PaperConfig::default()
            },
        )
        .unwrap();

        assert_eq!(analysis.behaviors.wash_cycles, 1);
        let wash = analysis
            .behavior_instances
            .iter()
            .find(|instance| instance.kind == BehaviorKind::WashTrading)
            .expect("wash instance");
        assert_eq!(wash.addresses, vec!["0xa".to_owned(), "0xb".to_owned()]);
        assert_eq!(wash.edge_count, 2);
        assert!(matches!(
            analysis
                .attribution
                .iter()
                .find(|(addr, _)| addr == "0xb")
                .map(|(_, row)| row.role),
            Some(AddressRole::SuspectedColluder)
        ));
    }

    #[test]
    fn legit_duplicate_excludes_from_malicious_flag() {
        let (store, contract) = store_with_contract("ethereum", "0xlegit");
        let mut evidence = EvidenceBundle::empty(contract, "ethereum", "0xlegit");
        evidence.legit = LegitSignals {
            verified_migration: true,
            evidence_keys: vec!["migration:official".into()],
            verification_complete: true,
            ..LegitSignals::default()
        };
        let analysis = analyze_candidate(
            &store,
            contract,
            &evidence,
            &PaperConfig {
                analysis_timestamp: 1,
                ..PaperConfig::default()
            },
        )
        .unwrap();
        assert!(analysis.legit.is_legit_duplicate);
        assert_eq!(
            analysis.legit.evidence_keys,
            vec!["migration:official".to_owned()]
        );
    }

    #[test]
    fn economics_marks_gas_not_requested_without_fake_complete() {
        let (store, contract) = store_with_contract("ethereum", "0xecon");
        let mut evidence = EvidenceBundle::empty(contract, "ethereum", "0xecon");
        evidence.controllers = vec!["0xop".into()];
        evidence.sales = vec![sale("tx-s", "1", "0xop", "0xv", 50, 5.0)];
        evidence.holders = vec![HolderRecord {
            token_id: "1".into(),
            owner: "0xv".into(),
            balance: Some(1),
        }];
        evidence.quality = EvidenceQuality {
            sales: EvidenceStatus::Complete,
            holders: EvidenceStatus::Complete,
            transfers: EvidenceStatus::Empty,
            gas: EvidenceStatus::NotRequested,
            value_flows: EvidenceStatus::NotRequested,
            ..EvidenceQuality::default()
        };

        let analysis = analyze_candidate(
            &store,
            contract,
            &evidence,
            &PaperConfig {
                analysis_timestamp: 100,
                ..PaperConfig::default()
            },
        )
        .unwrap();

        assert_eq!(analysis.economics.setup_gas_native, 0.0);
        assert_eq!(analysis.economics.lure_gas_native, 0.0);
        assert_eq!(analysis.economics.exit_gas_native, 0.0);
        assert_eq!(analysis.economics_quality.gas, EvidenceStatus::NotRequested);
        assert_eq!(
            analysis.economics_quality.value_flows,
            EvidenceStatus::NotRequested
        );
        assert!(analysis.economics.honest_loss_usd > 0.0);
    }

    #[test]
    fn layered_and_sybil_detectors_fire_on_synthetic_graphs() {
        let (store, contract) = store_with_contract("ethereum", "0xstar");
        let mut evidence = EvidenceBundle::empty(contract, "ethereum", "0xstar");
        evidence.controllers = vec!["0xop".into()];
        // layered path op -> a -> b (3 addresses)
        evidence.transfers = vec![
            transfer("t0", "1", "0xop", "0xa", 1, false),
            transfer("t1", "1", "0xa", "0xb", 2, false),
            // star fan-out from op to three leaves
            transfer("t2", "2", "0xop", "0xc", 3, false),
            transfer("t3", "3", "0xop", "0xd", 4, false),
            transfer("t4", "4", "0xop", "0xe", 5, false),
            // leaves do not propagate further → poisoning / fraud depending on value
        ];
        evidence.sales = vec![
            sale("s0", "2", "0xop", "0xc", 10, 2.0),
            sale("s1", "3", "0xop", "0xd", 11, 2.0),
            sale("s2", "4", "0xop", "0xe", 12, 2.0),
        ];
        evidence.quality.transfers = EvidenceStatus::Complete;
        evidence.quality.sales = EvidenceStatus::Complete;

        let analysis = analyze_candidate(
            &store,
            contract,
            &evidence,
            &PaperConfig {
                analysis_timestamp: 100,
                ..PaperConfig::default()
            },
        )
        .unwrap();

        assert!(analysis.behaviors.layered_transfer >= 1);
        assert!(
            analysis.behaviors.fraud_revenue
                + analysis.behaviors.sybil_distribution
                + analysis.behaviors.poisoning
                >= 1
        );
    }

    #[test]
    fn attribution_marks_paid_holder_as_likely_victim() {
        let (store, contract) = store_with_contract("ethereum", "0xattr");
        let mut evidence = EvidenceBundle::empty(contract, "ethereum", "0xattr");
        evidence.controllers = vec!["0xop".into()];
        evidence.sales = vec![sale("tx", "9", "0xop", "0xv", 5, 3.0)];
        evidence.holders = vec![HolderRecord {
            token_id: "9".into(),
            owner: "0xv".into(),
            balance: Some(1),
        }];
        evidence.quality.sales = EvidenceStatus::Complete;
        evidence.quality.holders = EvidenceStatus::Complete;

        let analysis = analyze_candidate(
            &store,
            contract,
            &evidence,
            &PaperConfig {
                analysis_timestamp: 20,
                ..PaperConfig::default()
            },
        )
        .unwrap();

        let victim = analysis
            .attribution
            .iter()
            .find(|(addr, _)| addr == "0xv")
            .unwrap();
        assert_eq!(victim.1.role, AddressRole::LikelyVictim);
        assert_eq!(
            analysis
                .attribution
                .iter()
                .find(|(addr, _)| addr == "0xop")
                .unwrap()
                .1
                .role,
            AddressRole::SuspectedOperator
        );
    }

    #[test]
    fn output_input_ratio_uses_same_unit_native_when_gas_complete() {
        let (store, contract) = store_with_contract("ethereum", "0xratio");
        let mut evidence = EvidenceBundle::empty(contract, "ethereum", "0xratio");
        evidence.controllers = vec!["0xop".into()];
        evidence.transfers = vec![TransferEvent {
            tx_hash: "mint".into(),
            token_id: "1".into(),
            from: String::new(),
            to: "0xop".into(),
            timestamp: Some(1),
            block_number: Some(1),
            is_mint: true,
            gas_native: Some(0.1),
            fee_payer: None,
            mint_payment_native: None,
            mint_payment_usd: None,
        }];
        evidence.sales = vec![SaleEvent {
            tx_hash: "sale".into(),
            token_id: "1".into(),
            seller: "0xop".into(),
            buyer: "0xv".into(),
            timestamp: Some(2),
            block_number: Some(2),
            marketplace: None,
            native_amount: Some(2.0),
            usd_amount: Some(400.0),
            currency_symbol: Some("ETH".into()),
        }];
        evidence.quality.transfers = EvidenceStatus::Complete;
        evidence.quality.sales = EvidenceStatus::Complete;
        evidence.quality.gas = EvidenceStatus::Complete;

        let analysis = analyze_candidate(
            &store,
            contract,
            &evidence,
            &PaperConfig {
                analysis_timestamp: 10,
                ..PaperConfig::default()
            },
        )
        .unwrap();

        assert_eq!(analysis.economics.total_gas_native, 0.1);
        assert_eq!(analysis.economics.operator_output_native, 2.0);
        assert_eq!(analysis.economics.operator_output_usd, 400.0);
        // Same-unit native/native — not mixed 400 / 0.1.
        assert_eq!(analysis.economics.output_input_ratio, Some(20.0));
    }

    #[test]
    fn secondary_sale_honest_loss_counts_native_only_amounts() {
        let (store, contract) = store_with_contract("ethereum", "0xnative");
        let mut evidence = EvidenceBundle::empty(contract, "ethereum", "0xnative");
        evidence.controllers = vec!["0xop".into()];
        evidence.sales = vec![SaleEvent {
            tx_hash: "sale".into(),
            token_id: "7".into(),
            seller: "0xop".into(),
            buyer: "0xv".into(),
            timestamp: Some(5),
            block_number: Some(5),
            marketplace: None,
            native_amount: Some(1.5),
            usd_amount: None,
            currency_symbol: Some("ETH".into()),
        }];
        evidence.holders = vec![HolderRecord {
            token_id: "7".into(),
            owner: "0xv".into(),
            balance: Some(1),
        }];
        evidence.quality.sales = EvidenceStatus::Complete;
        evidence.quality.holders = EvidenceStatus::Complete;

        let analysis = analyze_candidate(
            &store,
            contract,
            &evidence,
            &PaperConfig {
                analysis_timestamp: 20,
                ..PaperConfig::default()
            },
        )
        .unwrap();

        assert_eq!(analysis.economics.secondary_sale_loss_native, 1.5);
        assert_eq!(analysis.economics.secondary_sale_loss_usd, 0.0);
        assert_eq!(analysis.economics.honest_loss_native, 1.5);
        assert_eq!(analysis.economics.honest_loss_usd, 0.0);
        assert_eq!(analysis.economics.stuck_nft_count, 1);
    }

    /// Regression: enrich-depth fields (gas Complete + Withdrawal/Cashout edges with
    /// `gas_native`) flow through `analyze_candidate` into Setup/Lure/Exit economics.
    #[test]
    fn analyze_candidate_setup_lure_exit_when_gas_and_value_flows_complete() {
        let (store, contract) = store_with_contract("ethereum", "0xe5econ");
        let mut evidence = EvidenceBundle::empty(contract, "ethereum", "0xe5econ");
        evidence.controllers = vec!["0xop".into()];
        evidence.transfers = vec![
            TransferEvent {
                tx_hash: "mint-tx".into(),
                token_id: "1".into(),
                from: String::new(),
                to: "0xop".into(),
                timestamp: Some(1),
                block_number: Some(1),
                is_mint: true,
                gas_native: Some(0.01),
                fee_payer: Some("0xop".into()),
                mint_payment_native: None,
                mint_payment_usd: None,
            },
            TransferEvent {
                tx_hash: "lure-tx".into(),
                token_id: "1".into(),
                from: "0xop".into(),
                to: "0xv".into(),
                timestamp: Some(2),
                block_number: Some(2),
                is_mint: false,
                gas_native: Some(0.02),
                fee_payer: Some("0xop".into()),
                mint_payment_native: None,
                mint_payment_usd: None,
            },
            TransferEvent {
                tx_hash: "cashout-tx".into(),
                token_id: "1".into(),
                from: "0xop".into(),
                to: "0xex".into(),
                timestamp: Some(3),
                block_number: Some(3),
                is_mint: false,
                gas_native: Some(0.05),
                fee_payer: Some("0xop".into()),
                mint_payment_native: None,
                mint_payment_usd: None,
            },
        ];
        evidence.value_flows = vec![
            ValueFlowEdge {
                tx_hash: "cashout-tx".into(),
                from: "0xop".into(),
                to: "0xex".into(),
                kind: ValueFlowKind::Cashout,
                native_amount: Some(0.8),
                usd_amount: Some(160.0),
                timestamp: Some(3),
            },
            ValueFlowEdge {
                tx_hash: "wd-tx".into(),
                from: "0xop".into(),
                to: "0xcex".into(),
                kind: ValueFlowKind::Withdrawal,
                native_amount: Some(0.2),
                usd_amount: Some(40.0),
                timestamp: Some(4),
            },
        ];
        evidence.quality = EvidenceQuality {
            transfers: EvidenceStatus::Complete,
            gas: EvidenceStatus::Complete,
            value_flows: EvidenceStatus::Complete,
            ..EvidenceQuality::default()
        };

        let analysis = analyze_candidate(
            &store,
            contract,
            &evidence,
            &PaperConfig {
                analysis_timestamp: 100,
                ..PaperConfig::default()
            },
        )
        .unwrap();

        assert_eq!(analysis.economics_quality.gas, EvidenceStatus::Complete);
        assert_eq!(
            analysis.economics_quality.value_flows,
            EvidenceStatus::Complete
        );
        assert_eq!(analysis.economics.setup_gas_native, 0.01);
        assert_eq!(analysis.economics.lure_gas_native, 0.02);
        // cashout-tx gas upgraded Setup/Lure → Exit via Cashout edge.
        assert_eq!(analysis.economics.exit_gas_native, 0.05);
        assert_eq!(analysis.economics.total_gas_native, 0.08);
        assert_eq!(analysis.economics.withdrawal_native, 1.0);
        assert_eq!(analysis.economics.withdrawal_usd, 200.0);
        assert!(analysis.economics_quality.notes.is_empty());
    }
}
