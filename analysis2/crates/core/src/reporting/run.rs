//! Full `run` report builders: candidate JSON, seed analysis sections, summary aggregates.

use std::path::Path;

use ahash::{AHashMap, AHashSet};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::analysis::{
    AddressRole, BehaviorKind, CandidateAnalysis, EconomicFacts,
};
use crate::dedup::candidates::CandidateRegistry;
use crate::entity::{ContractId, ResidentStore};
use crate::error::Analysis2Error;

use super::json::{
    seed_dir_name, write_json, DedupRunParams, SeedDedupReport, SeedRecord,
};
use super::layout::{
    ensure_output_layout, intermediate_path, seed_report_dir, DETAIL_CANDIDATES_REL,
    SCOPE_CHAIN_MATRIX, SCOPE_INTRA_CHAIN, SCOPE_LABEL_ALL_CHAINS, SCOPE_LABEL_CROSS_CHAIN,
};
use super::manifest::{
    count_failed_seeds, write_failures_jsonl, FailureRecord, RunManifest, RunManifestSeeds,
};
use super::markdown;

/// Per-seed analysis rollup attached to the seed report after deep analysis.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SeedAnalysisRollup {
    pub analyzed_candidate_count: u64,
    pub suspected_duplicate_contract_count: u64,
    pub legit_duplicate_contract_count: u64,
    pub infringing_nft_count: u64,
    pub malicious_address_count: u64,
    pub honest_address_count: u64,
    pub economics_usd: EconomicsUsdRollup,
    pub candidate_refs: Vec<CandidateRef>,
}

/// Cross-chain / multi-candidate economics rollup.
///
/// Monetary fields that are summed across chains are **USD only**. Gas stages are
/// summed as native units per-candidate (not mixed-chain ETH totals for display
/// when multi-chain; still useful as aggregate gas accounting in native units).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct EconomicsUsdRollup {
    pub operator_output_usd: f64,
    pub honest_loss_usd: f64,
    pub secondary_sale_loss_usd: f64,
    pub paid_mint_loss_usd: f64,
    pub gross_revenue_usd: f64,
    pub setup_gas_native: f64,
    pub lure_gas_native: f64,
    pub exit_gas_native: f64,
    pub total_gas_native: f64,
    pub stuck_nft_count: u64,
    /// Contracts with a defined same-unit `output_input_ratio`.
    pub output_input_ratio_count: u64,
    pub output_input_ratio_ge1_count: u64,
    pub output_input_ratio_lt1_count: u64,
    /// Sum of per-contract input USD inferred as `output_usd / ratio` when ratio > 0.
    pub inferred_input_usd: f64,
}

/// Pointer from a seed report to a streamed candidate artifact.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CandidateRef {
    pub chain: String,
    pub address: String,
    pub is_legit_duplicate: bool,
    pub path: String,
}

/// Full per-seed report written by `run` (dedup + analysis).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SeedFullReport {
    #[serde(flatten)]
    pub dedup: SeedDedupReport,
    /// True when dedup completed for all configured secondary chains (four-scope complete).
    pub scopes_complete: bool,
    /// True when every related candidate finished analysis successfully.
    pub analysis_complete: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub analysis: Option<SeedAnalysisRollup>,
}

impl SeedFullReport {
    /// Formal summary denominators only include seeds with complete scopes + analysis.
    pub fn is_formal(&self) -> bool {
        self.scopes_complete && self.analysis_complete
    }
}

/// Whether this seed's chain-matrix covers every non-primary chain in the store.
pub fn scopes_complete_for_seed(store: &ResidentStore, report: &SeedDedupReport) -> bool {
    let primary = report.seed.chain.as_str();
    let expected: AHashSet<&str> = store
        .chains
        .iter()
        .map(String::as_str)
        .filter(|c| *c != primary)
        .collect();
    let present: AHashSet<&str> = report
        .duplicate_scale
        .chain_matrix
        .iter()
        .map(|b| b.secondary_chain.as_str())
        .collect();
    expected == present
}

pub fn candidate_file_name(chain: &str, address: &str) -> String {
    format!("{chain}__{address}.json")
}

/// Relative path for one candidate analysis JSON under `detail/candidates/`.
pub fn candidate_json_rel_path(chain: &str, address: &str) -> String {
    format!(
        "{DETAIL_CANDIDATES_REL}/{}",
        candidate_file_name(chain, address)
    )
}

/// Write one candidate analysis JSON under `output_dir/detail/candidates/`.
pub fn write_candidate_json(
    output_dir: &Path,
    analysis: &CandidateAnalysis,
) -> Result<String, Analysis2Error> {
    let rel = candidate_json_rel_path(&analysis.chain, &analysis.address);
    let path = output_dir.join(&rel);
    write_json(&path, analysis)?;
    Ok(rel)
}

/// Serialize candidate analysis to compact JSON bytes (CPU work; safe on Rayon).
pub fn serialize_candidate_json(analysis: &CandidateAnalysis) -> Result<Vec<u8>, Analysis2Error> {
    serde_json::to_vec(analysis)
        .map_err(|e| Analysis2Error::invalid(format!("serialize candidate analysis: {e}")))
}

/// Write pre-serialized candidate JSON bytes to `output_dir/rel`.
pub fn write_candidate_json_bytes(
    output_dir: &Path,
    rel: &str,
    body: &[u8],
) -> Result<(), Analysis2Error> {
    let path = output_dir.join(rel);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, body)?;
    Ok(())
}

fn economics_usd_from(facts: &EconomicFacts) -> EconomicsUsdRollup {
    let mut roll = EconomicsUsdRollup {
        operator_output_usd: facts.operator_output_usd,
        honest_loss_usd: facts.honest_loss_usd,
        secondary_sale_loss_usd: facts.secondary_sale_loss_usd,
        paid_mint_loss_usd: facts.paid_mint_loss_usd,
        gross_revenue_usd: facts.gross_revenue_usd,
        setup_gas_native: facts.setup_gas_native,
        lure_gas_native: facts.lure_gas_native,
        exit_gas_native: facts.exit_gas_native,
        total_gas_native: facts.total_gas_native,
        stuck_nft_count: facts.stuck_nft_count,
        output_input_ratio_count: 0,
        output_input_ratio_ge1_count: 0,
        output_input_ratio_lt1_count: 0,
        inferred_input_usd: 0.0,
    };
    // Paper table is USD-centric: only count ge1/lt1 and sum input when the
    // per-contract ratio is USD/USD (priced gas). Never invent USD input from a
    // native/native ratio (that produced 17x aggregate with 100% "<1" rows).
    if facts.output_input_ratio_is_usd {
        if let Some(ratio) = facts.output_input_ratio {
            roll.output_input_ratio_count = 1;
            if ratio >= 1.0 {
                roll.output_input_ratio_ge1_count = 1;
            } else {
                roll.output_input_ratio_lt1_count = 1;
            }
        }
        if let Some(input_usd) = facts.attacker_input_usd.filter(|v| v.is_finite() && *v > 0.0) {
            roll.inferred_input_usd = input_usd;
        } else if let Some(ratio) = facts.output_input_ratio.filter(|r| *r > 0.0) {
            roll.inferred_input_usd = facts.operator_output_usd / ratio;
        }
    }
    roll
}

fn merge_usd(dst: &mut EconomicsUsdRollup, src: &EconomicsUsdRollup) {
    dst.operator_output_usd += src.operator_output_usd;
    dst.honest_loss_usd += src.honest_loss_usd;
    dst.secondary_sale_loss_usd += src.secondary_sale_loss_usd;
    dst.paid_mint_loss_usd += src.paid_mint_loss_usd;
    dst.gross_revenue_usd += src.gross_revenue_usd;
    dst.setup_gas_native += src.setup_gas_native;
    dst.lure_gas_native += src.lure_gas_native;
    dst.exit_gas_native += src.exit_gas_native;
    dst.total_gas_native += src.total_gas_native;
    dst.stuck_nft_count += src.stuck_nft_count;
    dst.output_input_ratio_count += src.output_input_ratio_count;
    dst.output_input_ratio_ge1_count += src.output_input_ratio_ge1_count;
    dst.output_input_ratio_lt1_count += src.output_input_ratio_lt1_count;
    dst.inferred_input_usd += src.inferred_input_usd;
}

fn role_is_malicious(role: AddressRole) -> bool {
    matches!(
        role,
        AddressRole::SuspectedOperator | AddressRole::SuspectedColluder
    )
}

fn role_is_honest(role: AddressRole) -> bool {
    matches!(role, AddressRole::LikelyVictim)
}

/// Build the analysis rollup for one seed from its related candidate analyses.
pub fn build_seed_analysis_rollup(
    registry: &CandidateRegistry,
    seed_contract: ContractId,
    seed_chain: &str,
    seed_address: &str,
    analyses: &AHashMap<ContractId, CandidateAnalysis>,
    output_dir_label: &str,
) -> (SeedAnalysisRollup, bool) {
    let seed_key = format!("{seed_chain}:{seed_address}");
    let relations = registry.relations_for_seed(seed_contract);
    let mut analyzed = 0u64;
    let mut suspected = 0u64;
    let mut legit = 0u64;
    let mut infringing_nfts = 0u64;
    let mut malicious = AHashSet::new();
    let mut honest = AHashSet::new();
    let mut economics = EconomicsUsdRollup::default();
    let mut refs = Vec::new();
    let mut all_ok = true;

    for rel in relations {
        let Some(analysis) = analyses.get(&rel.candidate_contract) else {
            all_ok = false;
            continue;
        };
        analyzed += 1;
        let is_legit = analysis
            .legit_by_seed
            .get(&seed_key)
            .map(|c| c.is_legit_duplicate)
            .unwrap_or(analysis.legit.is_legit_duplicate);
        if is_legit {
            legit += 1;
        } else {
            suspected += 1;
            infringing_nfts += rel.nft_ids.len() as u64;
            for (addr, attr) in &analysis.attribution {
                if role_is_malicious(attr.role) {
                    malicious.insert(addr.clone());
                }
                if role_is_honest(attr.role) {
                    honest.insert(addr.clone());
                }
            }
            merge_usd(&mut economics, &economics_usd_from(&analysis.economics));
        }
        let path = format!(
            "{output_dir_label}/{}",
            candidate_file_name(&analysis.chain, &analysis.address)
        );
        refs.push(CandidateRef {
            chain: analysis.chain.clone(),
            address: analysis.address.clone(),
            is_legit_duplicate: is_legit,
            path,
        });
    }

    (
        SeedAnalysisRollup {
            analyzed_candidate_count: analyzed,
            suspected_duplicate_contract_count: suspected,
            legit_duplicate_contract_count: legit,
            infringing_nft_count: infringing_nfts,
            malicious_address_count: malicious.len() as u64,
            honest_address_count: honest.len() as u64,
            economics_usd: economics,
            candidate_refs: refs,
        },
        all_ok,
    )
}

fn row_has_duplicate(rows: &[super::aggregate::DuplicateScaleRow]) -> bool {
    rows.iter()
        .find(|r| r.category == "total")
        .map(|r| r.duplicate_nft_count > 0 || r.duplicate_contract_count > 0)
        .unwrap_or(false)
}

fn behavior_kind_key(kind: BehaviorKind) -> &'static str {
    match kind {
        BehaviorKind::WashTrading => "wash_trading",
        BehaviorKind::PumpAndExit => "pump_and_exit",
        BehaviorKind::SybilDistribution => "sybil_distribution",
        BehaviorKind::FraudRevenue => "fraud_revenue",
        BehaviorKind::Poisoning => "poisoning",
        BehaviorKind::LayeredTransfer => "layered_transfer",
        BehaviorKind::InventoryConcentration => "inventory_concentration",
    }
}

/// Aggregate behavior / address / economics summary keys for formal seeds only.
pub fn build_run_summary(
    selected: &[SeedRecord],
    formal: &[&SeedFullReport],
    incomplete: &[&SeedFullReport],
    failures: &[FailureRecord],
    analyses: &[&CandidateAnalysis],
) -> Value {
    let selected_n = selected.len() as u64;
    let formal_n = formal.len() as u64;
    let incomplete_n = incomplete.len() as u64;
    let with_dup = formal
        .iter()
        .filter(|r| {
            row_has_duplicate(&r.dedup.duplicate_scale.intra_chain)
                || row_has_duplicate(&r.dedup.duplicate_scale.cross_chain_summary)
        })
        .count() as u64;

    let mut candidate_contracts = AHashSet::new();
    let mut suspected = AHashSet::new();
    let mut legit_contracts = AHashSet::new();
    let mut malicious_addrs = AHashSet::new();
    let mut honest_addrs = AHashSet::new();
    let mut addr_to_suspect_contracts: AHashMap<String, AHashSet<String>> = AHashMap::new();

    // Per-seed views keep their own rollups; summary contract sets de-dupe by candidate key.
    for report in formal {
        if let Some(a) = &report.analysis {
            for r in &a.candidate_refs {
                let key = format!("{}:{}", r.chain, r.address);
                candidate_contracts.insert(key.clone());
                if r.is_legit_duplicate {
                    legit_contracts.insert(key);
                } else {
                    suspected.insert(key);
                }
            }
        }
    }

    // Address / behavior / economics / infringing aggregates over unique analyses in formal seeds.
    let formal_cand_keys: AHashSet<String> = formal
        .iter()
        .filter_map(|r| r.analysis.as_ref())
        .flat_map(|a| {
            a.candidate_refs
                .iter()
                .map(|c| format!("{}:{}", c.chain, c.address))
        })
        .collect();

    // Formal seed keys that touch each candidate (for all-relations-legit gate).
    let mut cand_formal_seeds: AHashMap<String, AHashSet<String>> = AHashMap::new();
    for report in formal {
        let seed_key = format!("{}:{}", report.dedup.seed.chain, report.dedup.seed.address);
        if let Some(a) = &report.analysis {
            for r in &a.candidate_refs {
                let cand_key = format!("{}:{}", r.chain, r.address);
                cand_formal_seeds
                    .entry(cand_key)
                    .or_default()
                    .insert(seed_key.clone());
            }
        }
    }

    // Mixed seed views: prefer suspected over legit for unique contract sets.
    for key in &suspected {
        legit_contracts.remove(key);
    }

    // Relation NFT identity keys keyed by unique candidate (union across seeds).
    // Prefer union of per-relation `nft_ids`; fall back to max(`nft_count`) when ids absent.
    let mut cand_nft_ids: AHashMap<String, AHashSet<u32>> = AHashMap::new();
    let mut cand_nft_max: AHashMap<String, u64> = AHashMap::new();
    for report in formal {
        for rel in &report.dedup.relations {
            let key = format!("{}:{}", rel.candidate_chain, rel.candidate_address);
            let emax = cand_nft_max.entry(key.clone()).or_insert(0);
            *emax = (*emax).max(rel.nft_count);
            if !rel.nft_ids.is_empty() {
                cand_nft_ids
                    .entry(key)
                    .or_default()
                    .extend(rel.nft_ids.iter().copied());
            }
        }
    }

    let mut behavior_map: AHashMap<&'static str, BehaviorAgg> = AHashMap::new();
    for key in [
        "wash_trading",
        "pump_and_exit",
        "sybil_distribution",
        "fraud_revenue",
        "poisoning",
        "layered_transfer",
        "inventory_concentration",
    ] {
        behavior_map.insert(key, BehaviorAgg::default());
    }

    let mut infringing_nfts = 0u64;
    let mut economics = EconomicsUsdRollup::default();
    let mut total_instances = 0u64;
    // Wash cycle node-size buckets (2 / 3 / 4 / 5+) from instance address sets.
    let mut wash_cycle_2 = 0u64;
    let mut wash_cycle_3 = 0u64;
    let mut wash_cycle_4 = 0u64;
    let mut wash_cycle_5p = 0u64;
    let mut contracts_with_behavior = AHashSet::new();
    // Per-contract total gas for concentration (top share of total_gas_native).
    let mut gas_by_contract: Vec<(String, f64)> = Vec::new();

    for analysis in analyses {
        let key = format!("{}:{}", analysis.chain, analysis.address);
        if !formal_cand_keys.contains(&key) {
            continue;
        }
        // Exclude from unique numerators only when every formal seed relation is legit.
        let fully_legit = cand_formal_seeds
            .get(&key)
            .map(|seeds| {
                !seeds.is_empty()
                    && seeds.iter().all(|sk| {
                        analysis
                            .legit_by_seed
                            .get(sk)
                            .map(|c| c.is_legit_duplicate)
                            .unwrap_or(analysis.legit.is_legit_duplicate)
                    })
            })
            .unwrap_or(analysis.legit.is_legit_duplicate);
        if fully_legit {
            continue;
        }
        let econ = economics_usd_from(&analysis.economics);
        merge_usd(&mut economics, &econ);
        if analysis.economics.total_gas_native > 0.0 {
            gas_by_contract.push((key.clone(), analysis.economics.total_gas_native));
        }
        let from_ids = cand_nft_ids
            .get(&key)
            .map(|s| s.len() as u64)
            .unwrap_or(0);
        let from_max = cand_nft_max.get(&key).copied().unwrap_or(0);
        infringing_nfts += if from_ids > 0 { from_ids } else { from_max };
        for (addr, attr) in &analysis.attribution {
            if role_is_malicious(attr.role) {
                malicious_addrs.insert(addr.clone());
                addr_to_suspect_contracts
                    .entry(addr.clone())
                    .or_default()
                    .insert(key.clone());
            }
            if role_is_honest(attr.role) {
                honest_addrs.insert(addr.clone());
            }
        }
        let mut kinds_seen = AHashSet::new();
        for inst in &analysis.behavior_instances {
            total_instances += 1;
            let bk = behavior_kind_key(inst.kind);
            let agg = behavior_map.get_mut(bk).unwrap();
            agg.instance_count += 1;
            for a in &inst.addresses {
                agg.addresses.insert(a.clone());
            }
            for n in &inst.nfts {
                agg.nfts.insert(n.clone());
            }
            for b in &inst.linked_buyers {
                agg.linked_buyers.insert(b.clone());
            }
            agg.linked_loss_usd += inst.linked_loss_usd;
            kinds_seen.insert(bk);
            if matches!(inst.kind, BehaviorKind::WashTrading) {
                match inst.addresses.len() {
                    0 | 1 => {}
                    2 => wash_cycle_2 += 1,
                    3 => wash_cycle_3 += 1,
                    4 => wash_cycle_4 += 1,
                    _ => wash_cycle_5p += 1,
                }
            }
        }
        if !kinds_seen.is_empty() {
            contracts_with_behavior.insert(key.clone());
        }
        for bk in kinds_seen {
            behavior_map.get_mut(bk).unwrap().contracts.insert(key.clone());
        }
    }

    let wash_total = wash_cycle_2 + wash_cycle_3 + wash_cycle_4 + wash_cycle_5p;
    let wash_cycle_size_distribution = json!([
        {
            "node_count_bucket": "2",
            "cycle_count": wash_cycle_2,
            "cycle_ratio": if wash_total == 0 { None } else { Some(wash_cycle_2 as f64 / wash_total as f64) },
            "cycle_ratio_numerator": wash_cycle_2,
            "cycle_ratio_denominator": wash_total,
        },
        {
            "node_count_bucket": "3",
            "cycle_count": wash_cycle_3,
            "cycle_ratio": if wash_total == 0 { None } else { Some(wash_cycle_3 as f64 / wash_total as f64) },
            "cycle_ratio_numerator": wash_cycle_3,
            "cycle_ratio_denominator": wash_total,
        },
        {
            "node_count_bucket": "4",
            "cycle_count": wash_cycle_4,
            "cycle_ratio": if wash_total == 0 { None } else { Some(wash_cycle_4 as f64 / wash_total as f64) },
            "cycle_ratio_numerator": wash_cycle_4,
            "cycle_ratio_denominator": wash_total,
        },
        {
            "node_count_bucket": "5+",
            "cycle_count": wash_cycle_5p,
            "cycle_ratio": if wash_total == 0 { None } else { Some(wash_cycle_5p as f64 / wash_total as f64) },
            "cycle_ratio_numerator": wash_cycle_5p,
            "cycle_ratio_denominator": wash_total,
        },
    ]);

    // Top-10% contract gas concentration (by total_gas_native among positive-gas contracts).
    gas_by_contract.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    let gas_total: f64 = gas_by_contract.iter().map(|(_, g)| *g).sum();
    let top_k = ((gas_by_contract.len() as f64) * 0.10).ceil() as usize;
    let top_k = top_k.max(if gas_by_contract.is_empty() { 0 } else { 1 }).min(gas_by_contract.len());
    let top_gas: f64 = gas_by_contract.iter().take(top_k).map(|(_, g)| *g).sum();
    let gas_concentration = if gas_total > 0.0 {
        Some(top_gas / gas_total)
    } else {
        None
    };

    let repeat_malicious = addr_to_suspect_contracts
        .values()
        .filter(|cs| cs.len() >= 2)
        .count() as u64;
    let total_address_count = malicious_addrs
        .union(&honest_addrs)
        .count() as u64;

    let suspected_n = suspected.len() as u64;
    let mut behaviors = serde_json::Map::new();
    for (k, agg) in &behavior_map {
        behaviors.insert(
            (*k).into(),
            json!({
                "contract_count": agg.contracts.len() as u64,
                "contract_coverage_ratio": if suspected_n == 0 {
                    None
                } else {
                    Some(agg.contracts.len() as f64 / suspected_n as f64)
                },
                "instance_count": agg.instance_count,
                "instance_ratio": if total_instances == 0 {
                    None
                } else {
                    Some(agg.instance_count as f64 / total_instances as f64)
                },
                "address_count": agg.addresses.len() as u64,
                "nft_count": agg.nfts.len() as u64,
                "linked_buyer_count": agg.linked_buyers.len() as u64,
                "linked_loss_usd": agg.linked_loss_usd,
            }),
        );
    }
    let mut total_behavior_addresses = AHashSet::new();
    let mut total_behavior_nfts = AHashSet::new();
    let mut total_linked_buyers = AHashSet::new();
    let mut total_linked_loss_usd = 0.0;
    for agg in behavior_map.values() {
        total_behavior_addresses.extend(agg.addresses.iter().cloned());
        total_behavior_nfts.extend(agg.nfts.iter().cloned());
        total_linked_buyers.extend(agg.linked_buyers.iter().cloned());
        total_linked_loss_usd += agg.linked_loss_usd;
    }
    behaviors.insert(
        "total".into(),
        json!({
            "contract_count": suspected_n,
            "instance_count": total_instances,
            "address_count": total_behavior_addresses.len() as u64,
            "nft_count": total_behavior_nfts.len() as u64,
            "linked_buyer_count": total_linked_buyers.len() as u64,
            "linked_loss_usd": total_linked_loss_usd,
        }),
    );

    let mut quality_complete = 0u64;
    let mut quality_empty = 0u64;
    let mut quality_failed = 0u64;
    let mut quality_truncated = 0u64;
    let mut quality_not_requested = 0u64;
    for analysis in analyses {
        let key = format!("{}:{}", analysis.chain, analysis.address);
        if !formal_cand_keys.contains(&key) {
            continue;
        }
        // economics_quality mirrors evidence gaps from Task 11/12.
        match analysis.economics_quality.gas {
            crate::enrich::EvidenceStatus::Complete => quality_complete += 1,
            crate::enrich::EvidenceStatus::Empty => quality_empty += 1,
            crate::enrich::EvidenceStatus::Failed => quality_failed += 1,
            crate::enrich::EvidenceStatus::Truncated => quality_truncated += 1,
            crate::enrich::EvidenceStatus::NotRequested => quality_not_requested += 1,
        }
    }

    json!({
        "selected_seed_count": selected_n,
        "analyzed_seed_count": formal_n,
        "incomplete_seed_count": incomplete_n,
        "failed_seed_count": count_failed_seeds(failures),
        "seed_completion_ratio": if selected_n == 0 { None } else { Some(formal_n as f64 / selected_n as f64) },
        "seed_with_duplicate_count": with_dup,
        "seed_duplicate_ratio": if formal_n == 0 { None } else { Some(with_dup as f64 / formal_n as f64) },
        "representative_candidate_count": candidate_contracts.len() as u64,
        "candidate_contract_count": candidate_contracts.len() as u64,
        "suspected_duplicate_contract_count": suspected_n,
        "legit_duplicate_contract_count": legit_contracts.len() as u64,
        "infringing_nft_count": infringing_nfts,
        "address_classification": {
            "malicious_address_count": malicious_addrs.len() as u64,
            "repeat_infringing_malicious_address_count": repeat_malicious,
            "honest_address_count": honest_addrs.len() as u64,
            "total_address_count": total_address_count,
        },
        "behaviors": behaviors,
        "behavior_contract_count": contracts_with_behavior.len() as u64,
        "wash_cycle_size_distribution": wash_cycle_size_distribution,
        "economics": {
            "operator_output_usd": economics.operator_output_usd,
            "honest_loss_usd": economics.honest_loss_usd,
            "secondary_sale_loss_usd": economics.secondary_sale_loss_usd,
            "paid_mint_loss_usd": economics.paid_mint_loss_usd,
            "gross_revenue_usd": economics.gross_revenue_usd,
            "setup_gas_native": economics.setup_gas_native,
            "lure_gas_native": economics.lure_gas_native,
            "exit_gas_native": economics.exit_gas_native,
            "total_gas_native": economics.total_gas_native,
            "stuck_nft_count": economics.stuck_nft_count,
            "stuck_nft_ratio": if infringing_nfts == 0 {
                None
            } else {
                Some(economics.stuck_nft_count as f64 / infringing_nfts as f64)
            },
            "output_input_ratio": if economics.inferred_input_usd > 0.0 {
                Some(economics.operator_output_usd / economics.inferred_input_usd)
            } else {
                None
            },
            "output_input_ratio_count": economics.output_input_ratio_count,
            "output_input_ratio_ge1_count": economics.output_input_ratio_ge1_count,
            "output_input_ratio_lt1_count": economics.output_input_ratio_lt1_count,
            "output_input_ratio_ge1_share": if economics.output_input_ratio_count == 0 {
                None
            } else {
                Some(
                    economics.output_input_ratio_ge1_count as f64
                        / economics.output_input_ratio_count as f64,
                )
            },
            "output_input_ratio_lt1_share": if economics.output_input_ratio_count == 0 {
                None
            } else {
                Some(
                    economics.output_input_ratio_lt1_count as f64
                        / economics.output_input_ratio_count as f64,
                )
            },
            "inferred_input_usd": economics.inferred_input_usd,
            "top_contract_gas_contribution_ratio": gas_concentration,
            "top_contract_gas_contribution_numerator": top_gas,
            "top_contract_gas_contribution_denominator": gas_total,
            "top_contract_gas_count": top_k as u64,
        },
        "data_quality": {
            "representative_candidate_count": candidate_contracts.len() as u64,
            "candidate_contract_count": candidate_contracts.len() as u64,
            "suspected_duplicate_contract_count": suspected_n,
            "legit_duplicate_contract_count": legit_contracts.len() as u64,
            "infringing_nft_count": infringing_nfts,
            "gas_evidence_complete": quality_complete,
            "gas_evidence_empty": quality_empty,
            "gas_evidence_failed": quality_failed,
            "gas_evidence_truncated": quality_truncated,
            "gas_evidence_not_requested": quality_not_requested,
            "failure_record_count": failures.len() as u64,
        },
        "all_chains": {
            "formal_seed_count": formal_n,
            "candidate_contract_count": candidate_contracts.len() as u64,
            "economics_usd": economics,
        },
        "seeds": formal.iter().map(|r| json!({
            "chain": r.dedup.seed.chain,
            "address": r.dedup.seed.address,
            "candidate_contract_count": r.dedup.candidate_contract_count,
            "hit_edge_count": r.dedup.hit_edge_count,
            "scopes_complete": r.scopes_complete,
            "analysis_complete": r.analysis_complete,
        })).collect::<Vec<_>>(),
    })
}

#[derive(Default)]
struct BehaviorAgg {
    contracts: AHashSet<String>,
    instance_count: u64,
    addresses: AHashSet<String>,
    nfts: AHashSet<String>,
    linked_buyers: AHashSet<String>,
    linked_loss_usd: f64,
}

/// Write full `run` artifacts under `output_dir`.
///
/// Layout: `intermediate/` (manifest, failures), `detail/` (seeds + candidates),
/// `summary/` (intra_chain / chain_matrix / cross_chain / all_chains).
pub fn write_run_outputs(
    output_dir: &Path,
    params: &DedupRunParams,
    store: &ResidentStore,
    selected_seeds: &[SeedRecord],
    analyzed: &[Result<(SeedRecord, SeedFullReport), FailureRecord>],
    analyses: &[CandidateAnalysis],
    extra_failures: &[FailureRecord],
) -> Result<(), Analysis2Error> {
    ensure_output_layout(output_dir).map_err(Analysis2Error::from)?;

    let mut failures = extra_failures.to_vec();
    let mut ok_reports: Vec<&SeedFullReport> = Vec::new();
    for item in analyzed {
        match item {
            Ok((_seed, report)) => ok_reports.push(report),
            Err(fail) => failures.push(fail.clone()),
        }
    }

    let formal: Vec<&SeedFullReport> = ok_reports.iter().copied().filter(|r| r.is_formal()).collect();
    let incomplete: Vec<&SeedFullReport> =
        ok_reports.iter().copied().filter(|r| !r.is_formal()).collect();

    for report in &ok_reports {
        let dir = seed_report_dir(output_dir, &seed_dir_name(&report.dedup.seed));
        write_json(&dir.join("report.json"), report)?;
        markdown::write_seed_full_report_md(&dir.join("report.md"), report)?;
    }

    // Four scopes share the same paper tables; each has its own set-union scale.
    let dedup_refs: Vec<&SeedDedupReport> = ok_reports.iter().map(|r| &r.dedup).collect();
    let analysis_refs: Vec<&CandidateAnalysis> = analyses.iter().collect();
    let summary = build_run_summary(
        selected_seeds,
        &formal,
        &incomplete,
        &failures,
        &analysis_refs,
    );
    super::json::write_four_scope_paper_summaries_public(
        output_dir,
        store,
        &dedup_refs,
        &summary,
    )?;

    let manifest = RunManifest {
        status: if failures.is_empty() && incomplete.is_empty() {
            "complete".into()
        } else {
            "complete_with_failures".into()
        },
        command: params.command.clone(),
        params: params.clone(),
        snapshot: json!({
            "inputs": params.inputs,
            "rows_loaded": store.rows_loaded,
            "chains": store.chains,
            "contracts": store.snapshot_contract_count().max(store.contracts.len() as u64),
            "nfts": store.snapshot_nft_count(),
        }),
        seeds: RunManifestSeeds {
            selected: selected_seeds.len() as u64,
            analyzed: formal.len() as u64,
            failed: count_failed_seeds(&failures),
        },
        completeness: json!({
            "seed_completion_ratio": if selected_seeds.is_empty() {
                None
            } else {
                Some(formal.len() as f64 / selected_seeds.len() as f64)
            },
            "incomplete_seed_count": incomplete.len() as u64,
            "formal_denominator_excludes_incomplete": true,
        }),
        pricing_policy: "alchemy_spot_runtime_usd_only_cross_chain".into(),
        stage_timings: json!([]),
        output_layout: json!({
            "intermediate": super::layout::INTERMEDIATE_DIR,
            "detail": super::layout::DETAIL_DIR,
            "summary": super::layout::SUMMARY_DIR,
            "scopes": [
                SCOPE_INTRA_CHAIN,
                SCOPE_CHAIN_MATRIX,
                SCOPE_LABEL_CROSS_CHAIN,
                SCOPE_LABEL_ALL_CHAINS,
            ],
        }),
    };
    write_json(
        &intermediate_path(output_dir, "run_manifest.json"),
        &manifest,
    )?;
    write_failures_jsonl(&intermediate_path(output_dir, "failures.jsonl"), &failures)?;
    Ok(())
}



#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::{
        BehaviorFacts, EconomicsQuality, LegitClassification, LifecycleFacts, ValueFlowFacts,
    };
    use crate::enrich::EvidenceStatus;
    use crate::reporting::aggregate::SeedDuplicateScale;
    use crate::reporting::json::SeedRelationJson;

    fn empty_analysis(chain: &str, address: &str, cid: ContractId) -> CandidateAnalysis {
        CandidateAnalysis {
            contract_id: cid,
            chain: chain.into(),
            address: address.into(),
            legit: LegitClassification {
                is_legit_duplicate: false,
                verification_complete: false,
                evidence_keys: vec![],
                reasons: vec![],
            },
            legit_by_seed: Default::default(),
            attribution: vec![],
            lifecycle: LifecycleFacts::default(),
            value_flow: ValueFlowFacts::default(),
            behaviors: BehaviorFacts::default(),
            behavior_instances: vec![],
            economics: EconomicFacts {
                operator_output_usd: 10.0,
                honest_loss_usd: 3.0,
                gross_revenue_usd: 12.0,
                ..EconomicFacts::default()
            },
            economics_quality: EconomicsQuality {
                gas: EvidenceStatus::NotRequested,
                value_flows: EvidenceStatus::NotRequested,
                notes: vec![],
            },
            analysis_timestamp: 0,
        }
    }

    #[test]
    fn summary_has_rewrite_design_keys_and_usd_only_economics() {
        let seed = SeedRecord {
            chain: "ethereum".into(),
            address: "0xseed".into(),
            rank: Some(1),
        };
        let report = SeedFullReport {
            dedup: SeedDedupReport {
                seed: seed.clone(),
                hit_edge_count: 1,
                candidate_contract_count: 1,
                relations: vec![SeedRelationJson {
                    candidate_chain: "base".into(),
                    candidate_address: "0xcand".into(),
                    dimensions: vec!["token_uri".into()],
                    nft_count: 1,
                    nft_ids: vec![1],
                }],
                duplicate_scale: SeedDuplicateScale::default(),
            },
            scopes_complete: true,
            analysis_complete: true,
            analysis: Some(SeedAnalysisRollup {
                analyzed_candidate_count: 1,
                suspected_duplicate_contract_count: 1,
                legit_duplicate_contract_count: 0,
                infringing_nft_count: 1,
                malicious_address_count: 0,
                honest_address_count: 0,
                economics_usd: EconomicsUsdRollup {
                    operator_output_usd: 10.0,
                    honest_loss_usd: 3.0,
                    secondary_sale_loss_usd: 3.0,
                    paid_mint_loss_usd: 0.0,
                    gross_revenue_usd: 12.0,
                    ..Default::default()
                },
                candidate_refs: vec![CandidateRef {
                    chain: "base".into(),
                    address: "0xcand".into(),
                    is_legit_duplicate: false,
                    path: "detail/candidates/base__0xcand.json".into(),
                }],
            }),
        };
        let analysis = empty_analysis("base", "0xcand", 1);
        let summary = build_run_summary(
            &[seed],
            &[&report],
            &[],
            &[],
            &[&analysis],
        );
        for key in [
            "selected_seed_count",
            "analyzed_seed_count",
            "incomplete_seed_count",
            "failed_seed_count",
            "seed_completion_ratio",
            "seed_with_duplicate_count",
            "seed_duplicate_ratio",
            "representative_candidate_count",
            "candidate_contract_count",
            "suspected_duplicate_contract_count",
            "legit_duplicate_contract_count",
            "infringing_nft_count",
            "address_classification",
            "behaviors",
            "behavior_contract_count",
            "wash_cycle_size_distribution",
            "economics",
            "data_quality",
            "all_chains",
        ] {
            assert!(summary.get(key).is_some(), "missing summary key {key}");
        }
        let econ = &summary["economics"];
        assert!(econ.get("operator_output_usd").is_some());
        assert!(econ.get("honest_loss_usd").is_some());
        assert!(econ.get("setup_gas_native").is_some());
        assert!(econ.get("stuck_nft_count").is_some());
        assert!(econ.get("operator_output_native").is_none());
        assert!(econ.get("honest_loss_native").is_none());
        assert_eq!(econ["operator_output_usd"], 10.0);
        assert_eq!(summary["address_classification"]["malicious_address_count"], 0);
        assert!(summary["behaviors"].get("wash_trading").is_some());
        assert!(summary["behaviors"].get("total").is_some());
        assert!(summary["wash_cycle_size_distribution"].as_array().is_some());
    }

    fn formal_seed_sharing_candidate(
        seed_chain: &str,
        seed_addr: &str,
        cand_chain: &str,
        cand_addr: &str,
        economics: EconomicsUsdRollup,
        infringing_nft_count: u64,
        nft_ids: Vec<u32>,
        dimensions: Vec<String>,
        is_legit: bool,
    ) -> SeedFullReport {
        SeedFullReport {
            dedup: SeedDedupReport {
                seed: SeedRecord {
                    chain: seed_chain.into(),
                    address: seed_addr.into(),
                    rank: Some(1),
                },
                hit_edge_count: 1,
                candidate_contract_count: 1,
                relations: vec![SeedRelationJson {
                    candidate_chain: cand_chain.into(),
                    candidate_address: cand_addr.into(),
                    dimensions,
                    nft_count: infringing_nft_count,
                    nft_ids,
                }],
                duplicate_scale: SeedDuplicateScale::default(),
            },
            scopes_complete: true,
            analysis_complete: true,
            analysis: Some(SeedAnalysisRollup {
                analyzed_candidate_count: 1,
                suspected_duplicate_contract_count: if is_legit { 0 } else { 1 },
                legit_duplicate_contract_count: if is_legit { 1 } else { 0 },
                infringing_nft_count: if is_legit { 0 } else { infringing_nft_count },
                malicious_address_count: 0,
                honest_address_count: 0,
                economics_usd: economics.clone(),
                candidate_refs: vec![CandidateRef {
                    chain: cand_chain.into(),
                    address: cand_addr.into(),
                    is_legit_duplicate: is_legit,
                    path: format!("detail/candidates/{cand_chain}__{cand_addr}.json"),
                }],
            }),
        }
    }

    #[test]
    fn summary_economics_count_shared_candidate_once() {
        let econ = EconomicsUsdRollup {
            operator_output_usd: 10.0,
            honest_loss_usd: 3.0,
            secondary_sale_loss_usd: 3.0,
            paid_mint_loss_usd: 0.0,
            gross_revenue_usd: 12.0,
            ..Default::default()
        };
        // Two formal seeds share one candidate; per-seed rollups each carry the full USD.
        let report_a = formal_seed_sharing_candidate(
            "ethereum",
            "0xseed_a",
            "base",
            "0xcand",
            econ.clone(),
            2,
            vec![10, 11],
            vec!["token_uri".into()],
            false,
        );
        let report_b = formal_seed_sharing_candidate(
            "ethereum",
            "0xseed_b",
            "base",
            "0xcand",
            econ,
            2,
            vec![10, 11],
            vec!["token_uri".into()],
            false,
        );
        let analysis = empty_analysis("base", "0xcand", 1);
        let selected = vec![
            report_a.dedup.seed.clone(),
            report_b.dedup.seed.clone(),
        ];
        let summary = build_run_summary(
            &selected,
            &[&report_a, &report_b],
            &[],
            &[],
            &[&analysis],
        );
        let economics = &summary["economics"];
        // Must equal the unique CandidateAnalysis once — not 2× per-seed rollups.
        assert_eq!(economics["operator_output_usd"], 10.0);
        assert_eq!(economics["honest_loss_usd"], 3.0);
        assert_eq!(economics["gross_revenue_usd"], 12.0);
        assert_eq!(summary["all_chains"]["economics_usd"]["operator_output_usd"], 10.0);
        assert_eq!(summary["infringing_nft_count"], 2);
        assert_eq!(summary["candidate_contract_count"], 1);
    }

    #[test]
    fn summary_excludes_legit_duplicate_from_economics_attribution_behavior() {
        use crate::analysis::{
            AddressAttribution, AddressEvidence, AddressEvidenceKind, AddressRole, BehaviorInstance,
            BehaviorKind,
        };
        use crate::enrich::{finalize_legit_signals, EvidenceBundle, LegitSignals};

        // Plumbing: future enrich can set flags; classify → summary must exclude.
        let mut bundle = EvidenceBundle::empty(2, "base", "0xlegit");
        bundle.legit = LegitSignals {
            verified_migration: true,
            evidence_keys: vec!["migration:test".into()],
            verification_complete: true,
            ..LegitSignals::default()
        };
        finalize_legit_signals(&mut bundle);
        assert!(bundle.legit.is_legit_duplicate());

        let econ = EconomicsUsdRollup {
            operator_output_usd: 10.0,
            honest_loss_usd: 3.0,
            secondary_sale_loss_usd: 3.0,
            paid_mint_loss_usd: 0.0,
            gross_revenue_usd: 12.0,
            ..Default::default()
        };
        let report = formal_seed_sharing_candidate(
            "ethereum",
            "0xseed",
            "base",
            "0xlegit",
            econ,
            4,
            vec![1, 2, 3, 4],
            vec!["name".into()],
            true,
        );
        let mut analysis = empty_analysis("base", "0xlegit", 2);
        analysis.legit = LegitClassification {
            is_legit_duplicate: true,
            verification_complete: true,
            evidence_keys: vec!["migration:test".into()],
            reasons: vec!["verified_migration".into()],
        };
        analysis.economics.operator_output_usd = 99.0;
        analysis.economics.honest_loss_usd = 50.0;
        analysis.economics.gross_revenue_usd = 99.0;
        analysis.attribution = vec![(
            "0xop".into(),
            AddressAttribution {
                role: AddressRole::SuspectedOperator,
                evidence: vec![AddressEvidence {
                    evidence_type: AddressEvidenceKind::ControllerOrAuthority,
                    token_id: None,
                    transaction: None,
                    weight: 1.0,
                    confidence: 1.0,
                }],
            },
        )];
        analysis.behavior_instances = vec![BehaviorInstance {
            kind: BehaviorKind::WashTrading,
            addresses: vec!["0xop".into()],
            nfts: vec!["1".into()],
            linked_loss_usd: 7.0,
            ..BehaviorInstance::default()
        }];

        let summary = build_run_summary(
            &[report.dedup.seed.clone()],
            &[&report],
            &[],
            &[],
            &[&analysis],
        );
        assert_eq!(summary["legit_duplicate_contract_count"], 1);
        assert_eq!(summary["suspected_duplicate_contract_count"], 0);
        assert_eq!(summary["infringing_nft_count"], 0);
        assert_eq!(summary["economics"]["operator_output_usd"], 0.0);
        assert_eq!(summary["economics"]["honest_loss_usd"], 0.0);
        assert_eq!(summary["address_classification"]["malicious_address_count"], 0);
        assert_eq!(summary["behaviors"]["total"]["instance_count"], 0);
        assert_eq!(summary["behaviors"]["wash_trading"]["instance_count"], 0);
    }

    #[test]
    fn summary_infringing_nft_unions_uri_narrow_and_name_wide_hits() {
        let econ = EconomicsUsdRollup::default();
        // Seed A: URI-narrow hit on NFTs {10, 11}
        let report_a = formal_seed_sharing_candidate(
            "ethereum",
            "0xseed_uri",
            "base",
            "0xcand",
            econ.clone(),
            2,
            vec![10, 11],
            vec!["token_uri".into()],
            false,
        );
        // Seed B: Name-wide hit expands to {10, 11, 12, 13, 14}
        let report_b = formal_seed_sharing_candidate(
            "ethereum",
            "0xseed_name",
            "base",
            "0xcand",
            econ,
            5,
            vec![10, 11, 12, 13, 14],
            vec!["name".into()],
            false,
        );
        let analysis = empty_analysis("base", "0xcand", 1);
        let selected = vec![
            report_a.dedup.seed.clone(),
            report_b.dedup.seed.clone(),
        ];
        let summary = build_run_summary(
            &selected,
            &[&report_a, &report_b],
            &[],
            &[],
            &[&analysis],
        );
        // Union of identity keys = 5, not first-wins (2) and not sum (7).
        assert_eq!(summary["infringing_nft_count"], 5);
        assert_eq!(summary["candidate_contract_count"], 1);
    }

    #[test]
    fn failed_seed_count_counts_unique_seed_scope_not_candidate_rows() {
        let failures = vec![
            FailureRecord::seed_stage("ethereum", "0xfail", "resolve_seed", "missing"),
            FailureRecord::candidate_stage("base", "0xc1", "analyze_candidate", "boom"),
            FailureRecord::candidate_stage("base", "0xc2", "analyze_candidate", "boom"),
            FailureRecord::seed_stage("ethereum", "0xfail", "dedup_query", "again"),
        ];
        assert_eq!(count_failed_seeds(&failures), 1);
        assert_eq!(failures.len(), 4);

        let seed = SeedRecord {
            chain: "ethereum".into(),
            address: "0xok".into(),
            rank: Some(1),
        };
        let report = formal_seed_sharing_candidate(
            "ethereum",
            "0xok",
            "base",
            "0xcand",
            EconomicsUsdRollup::default(),
            1,
            vec![1],
            vec!["token_uri".into()],
            false,
        );
        let analysis = empty_analysis("base", "0xcand", 1);
        let summary = build_run_summary(
            &[seed, SeedRecord {
                chain: "ethereum".into(),
                address: "0xfail".into(),
                rank: Some(2),
            }],
            &[&report],
            &[],
            &failures,
            &[&analysis],
        );
        assert_eq!(summary["failed_seed_count"], 1);
        assert_eq!(summary["data_quality"]["failure_record_count"], 4);
    }
}
