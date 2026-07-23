//! JSON writers and seed-report payloads for offline dedup runs.

use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::dedup::candidates::CandidateRegistry;
use crate::dedup::hits::{Dimension, HitGraph};
use crate::entity::{ContractId, NftId, ResidentStore};
use crate::error::Analysis2Error;

use super::aggregate::{
    build_seed_duplicate_scale, DuplicateScaleRow, SeedDuplicateScale,
};
use super::manifest::{count_failed_seeds, FailureRecord, RunManifest, RunManifestSeeds};

/// Minimal seed entry accepted by `--seeds` before `select-seeds` lands.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeedRecord {
    pub chain: String,
    pub address: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rank: Option<u32>,
}

/// CLI/run parameters echoed into `run_manifest.json`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DedupRunParams {
    pub command: String,
    pub inputs: Vec<String>,
    pub chains: Vec<String>,
    pub evm_chains: Vec<String>,
    pub name_threshold: f64,
    pub metadata_threshold: f64,
    pub metadata_anchors: usize,
}

/// One seed→candidate relation in the per-seed JSON report.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SeedRelationJson {
    pub candidate_chain: String,
    pub candidate_address: String,
    pub dimensions: Vec<String>,
    pub nft_count: u64,
    /// Resident-store NFT ids for this relation; summary unions these across seeds.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub nft_ids: Vec<u32>,
}

/// Per-seed dedup report (JSON body for `report.json`).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SeedDedupReport {
    pub seed: SeedRecord,
    pub hit_edge_count: u64,
    pub candidate_contract_count: u64,
    pub relations: Vec<SeedRelationJson>,
    pub duplicate_scale: SeedDuplicateScale,
}

fn dimension_label(d: Dimension) -> &'static str {
    match d {
        Dimension::Name => "name",
        Dimension::TokenUri => "token_uri",
        Dimension::ImageUri => "image_uri",
        Dimension::Metadata => "metadata",
    }
}

/// Load a JSON array of `{chain, address, rank?}`.
pub fn load_seeds_json(path: &Path) -> Result<Vec<SeedRecord>, Analysis2Error> {
    let text = fs::read_to_string(path)?;
    let seeds: Vec<SeedRecord> = serde_json::from_str(&text).map_err(|e| {
        Analysis2Error::invalid(format!("invalid seeds JSON {}: {e}", path.display()))
    })?;
    if seeds.is_empty() {
        return Err(Analysis2Error::invalid("seeds JSON is empty"));
    }
    let mut normalized = Vec::with_capacity(seeds.len());
    for mut seed in seeds {
        seed.chain = seed.chain.trim().to_ascii_lowercase();
        seed.address = seed.address.trim().to_owned();
        if seed.chain.is_empty() || seed.address.is_empty() {
            return Err(Analysis2Error::invalid(
                "each seed requires non-empty chain and address",
            ));
        }
        normalized.push(seed);
    }
    Ok(normalized)
}

pub fn seed_dir_name(seed: &SeedRecord) -> String {
    format!("{}__{}", seed.chain, seed.address)
}

pub fn resolve_seed_contract(
    store: &ResidentStore,
    seed: &SeedRecord,
) -> Result<ContractId, Analysis2Error> {
    let chain_id = store
        .chain_ids
        .get(&seed.chain)
        .copied()
        .ok_or_else(|| Analysis2Error::invalid(format!("unknown seed chain {}", seed.chain)))?;
    store
        .contract_index
        .get(&(chain_id, seed.address.clone()))
        .copied()
        .ok_or_else(|| {
            Analysis2Error::invalid(format!(
                "seed contract not in snapshot: {} / {}",
                seed.chain, seed.address
            ))
        })
}

/// Build the per-seed dedup report payload from a populated HitGraph.
pub fn build_seed_dedup_report(
    store: &ResidentStore,
    seed: &SeedRecord,
    seed_id: ContractId,
    graph: &HitGraph,
    registry: &CandidateRegistry,
    contract_nfts: &ahash::AHashMap<ContractId, Vec<NftId>>,
) -> SeedDedupReport {
    let relations: Vec<SeedRelationJson> = registry
        .relations_for_seed(seed_id)
        .into_iter()
        .map(|rel| {
            let cand = &store.contracts[rel.candidate_contract as usize];
            SeedRelationJson {
                candidate_chain: store.chain_name(cand.chain_id).to_owned(),
                candidate_address: cand.address.clone(),
                dimensions: rel
                    .dimensions
                    .iter()
                    .copied()
                    .map(dimension_label)
                    .map(str::to_owned)
                    .collect(),
                nft_count: rel.nft_ids.len() as u64,
                nft_ids: rel.nft_ids.clone(),
            }
        })
        .collect();

    SeedDedupReport {
        seed: seed.clone(),
        hit_edge_count: graph
            .edges()
            .iter()
            .filter(|e| e.seed_contract == seed_id)
            .count() as u64,
        candidate_contract_count: relations.len() as u64,
        relations,
        duplicate_scale: build_seed_duplicate_scale(store, graph, seed_id, contract_nfts),
    }
}

pub(crate) fn write_json(path: &Path, value: &impl Serialize) -> Result<(), Analysis2Error> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let body = serde_json::to_vec_pretty(value)
        .map_err(|e| Analysis2Error::invalid(format!("json encode {}: {e}", path.display())))?;
    fs::write(path, body)?;
    Ok(())
}

fn row_has_duplicate(rows: &[DuplicateScaleRow]) -> bool {
    rows.iter()
        .find(|r| r.category == "total")
        .map(|r| r.duplicate_nft_count > 0 || r.duplicate_contract_count > 0)
        .unwrap_or(false)
}

/// Write all offline dedup artifacts under `output_dir`.
pub fn write_dedup_outputs(
    output_dir: &Path,
    params: &DedupRunParams,
    store: &ResidentStore,
    selected_seeds: &[SeedRecord],
    analyzed: &[Result<(SeedRecord, SeedDedupReport), FailureRecord>],
    extra_failures: &[FailureRecord],
) -> Result<(), Analysis2Error> {
    fs::create_dir_all(output_dir)?;

    let mut failures = extra_failures.to_vec();
    let mut ok_reports: Vec<&SeedDedupReport> = Vec::new();
    for item in analyzed {
        match item {
            Ok((_seed, report)) => ok_reports.push(report),
            Err(fail) => failures.push(fail.clone()),
        }
    }

    for report in &ok_reports {
        let dir = output_dir.join("seeds").join(seed_dir_name(&report.seed));
        write_json(&dir.join("report.json"), report)?;
        super::markdown::write_seed_report_md(&dir.join("report.md"), report)?;
    }

    write_scope_rollups(output_dir, &ok_reports)?;
    write_summary(output_dir, selected_seeds, &ok_reports, &failures)?;

    let manifest = RunManifest {
        status: if failures.is_empty() {
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
            "contracts": store.contracts.len(),
            "nfts": store.nfts.len(),
        }),
        seeds: RunManifestSeeds {
            selected: selected_seeds.len() as u64,
            analyzed: ok_reports.len() as u64,
            failed: count_failed_seeds(&failures),
        },
        completeness: json!({
            "seed_completion_ratio": if selected_seeds.is_empty() {
                None
            } else {
                Some(ok_reports.len() as f64 / selected_seeds.len() as f64)
            }
        }),
        pricing_policy: "not_applicable".into(),
        stage_timings: json!([]),
    };
    write_json(&output_dir.join("run_manifest.json"), &manifest)?;
    super::manifest::write_failures_jsonl(&output_dir.join("failures.jsonl"), &failures)?;
    Ok(())
}

pub(crate) fn write_scope_rollups_public(
    output_dir: &Path,
    reports: &[&SeedDedupReport],
) -> Result<(), Analysis2Error> {
    write_scope_rollups(output_dir, reports)
}

fn write_scope_rollups(
    output_dir: &Path,
    reports: &[&SeedDedupReport],
) -> Result<(), Analysis2Error> {
    let mut intra = Vec::new();
    let mut matrix = Vec::new();
    let mut cross = Vec::new();
    for report in reports {
        intra.push(json!({
            "seed_chain": report.seed.chain,
            "seed_address": report.seed.address,
            "rows": report.duplicate_scale.intra_chain,
        }));
        for block in &report.duplicate_scale.chain_matrix {
            matrix.push(json!({
                "seed_chain": report.seed.chain,
                "seed_address": report.seed.address,
                "secondary_chain": block.secondary_chain,
                "rows": block.rows,
            }));
        }
        cross.push(json!({
            "seed_chain": report.seed.chain,
            "seed_address": report.seed.address,
            "rows": report.duplicate_scale.cross_chain_summary,
        }));
    }
    write_json(
        &output_dir.join("intra_chain.json"),
        &json!({ "scope": "intra_chain", "seeds": intra }),
    )?;
    write_json(
        &output_dir.join("chain_matrix.json"),
        &json!({ "scope": "chain_matrix", "seeds": matrix }),
    )?;
    write_json(
        &output_dir.join("cross_chain.json"),
        &json!({ "scope": "cross_chain_summary", "seeds": cross }),
    )?;
    super::markdown::write_scope_md(
        &output_dir.join("intra_chain.md"),
        "intra_chain",
        reports,
        |r| &r.duplicate_scale.intra_chain,
    )?;
    super::markdown::write_matrix_md(&output_dir.join("chain_matrix.md"), reports)?;
    super::markdown::write_scope_md(
        &output_dir.join("cross_chain.md"),
        "cross_chain_summary",
        reports,
        |r| &r.duplicate_scale.cross_chain_summary,
    )?;
    Ok(())
}

fn write_summary(
    output_dir: &Path,
    selected: &[SeedRecord],
    reports: &[&SeedDedupReport],
    failures: &[FailureRecord],
) -> Result<(), Analysis2Error> {
    let with_dup = reports
        .iter()
        .filter(|r| {
            row_has_duplicate(&r.duplicate_scale.intra_chain)
                || row_has_duplicate(&r.duplicate_scale.cross_chain_summary)
        })
        .count() as u64;
    let analyzed = reports.len() as u64;
    let selected_n = selected.len() as u64;
    let body = json!({
        "selected_seed_count": selected_n,
        "analyzed_seed_count": analyzed,
        "failed_seed_count": count_failed_seeds(failures),
        "seed_completion_ratio": if selected_n == 0 { None } else { Some(analyzed as f64 / selected_n as f64) },
        "seed_with_duplicate_count": with_dup,
        "seed_duplicate_ratio": if analyzed == 0 { None } else { Some(with_dup as f64 / analyzed as f64) },
        "seeds": reports.iter().map(|r| json!({
            "chain": r.seed.chain,
            "address": r.seed.address,
            "candidate_contract_count": r.candidate_contract_count,
            "hit_edge_count": r.hit_edge_count,
        })).collect::<Vec<_>>(),
    });
    write_json(&output_dir.join("summary.json"), &body)?;
    super::markdown::write_summary_md(&output_dir.join("summary.md"), &body)?;
    Ok(())
}
