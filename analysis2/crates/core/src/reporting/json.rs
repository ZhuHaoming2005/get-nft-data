//! JSON writers and seed-report payloads for offline dedup runs.

use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::dedup::candidates::CandidateRegistry;
use crate::dedup::hits::{Dimension, HitGraph};
use crate::entity::{ContractId, NftId, ResidentStore};
use crate::error::Analysis2Error;

use super::aggregate::{
    build_all_chains_duplicate_scale, build_scope_duplicate_scale, build_seed_duplicate_scale,
    matrix_secondary_chains, AllChainsRelationRef, DuplicateScaleRow, ScopeScaleFilter,
    SeedDuplicateScale,
};
use super::layout::{
    ensure_output_layout, intermediate_path, seed_report_dir, summary_scope_path,
    SCOPE_ALL_CHAINS, SCOPE_CHAIN_MATRIX, SCOPE_CROSS_CHAIN, SCOPE_INTRA_CHAIN,
    SCOPE_LABEL_ALL_CHAINS, SCOPE_LABEL_CROSS_CHAIN,
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
    pub name_threshold: Option<f64>,
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
    if !store.chain_ids.contains_key(&seed.chain) {
        return Err(Analysis2Error::invalid(format!(
            "unknown seed chain {}",
            seed.chain
        )));
    }
    store.contract_id(&seed.chain, &seed.address).ok_or_else(|| {
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
///
/// Layout: `intermediate/` (manifest, failures), `detail/seeds/` (per-seed),
/// `summary/` (intra_chain / chain_matrix / cross_chain / all_chains).
pub fn write_dedup_outputs(
    output_dir: &Path,
    params: &DedupRunParams,
    store: &ResidentStore,
    selected_seeds: &[SeedRecord],
    analyzed: &[Result<(SeedRecord, SeedDedupReport), FailureRecord>],
    extra_failures: &[FailureRecord],
) -> Result<(), Analysis2Error> {
    ensure_output_layout(output_dir).map_err(Analysis2Error::from)?;

    let mut failures = extra_failures.to_vec();
    let mut ok_reports: Vec<&SeedDedupReport> = Vec::new();
    for item in analyzed {
        match item {
            Ok((_seed, report)) => ok_reports.push(report),
            Err(fail) => failures.push(fail.clone()),
        }
    }

    for report in &ok_reports {
        let dir = seed_report_dir(output_dir, &seed_dir_name(&report.seed));
        write_json(&dir.join("report.json"), report)?;
        super::markdown::write_seed_report_md(&dir.join("report.md"), report)?;
    }

    write_scope_rollups(output_dir, store, selected_seeds, &ok_reports, &failures)?;

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
            "contracts": store.snapshot_contract_count().max(store.contracts.len() as u64),
            "nfts": store.snapshot_nft_count().max(store.nfts.len() as u64),
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
        output_layout: output_layout_manifest(),
    };
    write_json(
        &intermediate_path(output_dir, "run_manifest.json"),
        &manifest,
    )?;
    super::manifest::write_failures_jsonl(
        &intermediate_path(output_dir, "failures.jsonl"),
        &failures,
    )?;
    Ok(())
}

pub(crate) fn write_four_scope_paper_summaries_public(
    output_dir: &Path,
    store: &ResidentStore,
    reports: &[&SeedDedupReport],
    paper_summary: &serde_json::Value,
) -> Result<(), Analysis2Error> {
    write_four_scope_paper_summaries(output_dir, store, reports, paper_summary)
}

fn output_layout_manifest() -> serde_json::Value {
    json!({
        "intermediate": super::layout::INTERMEDIATE_DIR,
        "detail": super::layout::DETAIL_DIR,
        "summary": super::layout::SUMMARY_DIR,
        "scopes": [
            SCOPE_INTRA_CHAIN,
            SCOPE_CHAIN_MATRIX,
            SCOPE_LABEL_CROSS_CHAIN,
            SCOPE_LABEL_ALL_CHAINS,
        ],
    })
}

fn scope_relations<'a>(reports: &[&'a SeedDedupReport]) -> Vec<AllChainsRelationRef<'a>> {
    let mut out = Vec::new();
    for report in reports {
        for rel in &report.relations {
            out.push(AllChainsRelationRef {
                seed_chain: report.seed.chain.as_str(),
                seed_address: report.seed.address.as_str(),
                candidate_chain: rel.candidate_chain.as_str(),
                candidate_address: rel.candidate_address.as_str(),
                dimensions: rel.dimensions.as_slice(),
                nft_ids: rel.nft_ids.as_slice(),
            });
        }
    }
    out
}

fn seed_index_json(reports: &[&SeedDedupReport]) -> Vec<serde_json::Value> {
    reports
        .iter()
        .map(|r| {
            json!({
                "chain": r.seed.chain,
                "address": r.seed.address,
                "candidate_contract_count": r.candidate_contract_count,
                "hit_edge_count": r.hit_edge_count,
            })
        })
        .collect()
}

fn per_seed_scale_detail(reports: &[&SeedDedupReport]) -> (Vec<Value>, Vec<Value>, Vec<Value>) {
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
    (intra, matrix, cross)
}

/// Write all four scopes with the same paper-table layout as `all_chains`.
///
/// `paper_summary` is the batch analysis rollup (may be a thin dedup-only body).
/// Each scope gets its own aggregated `duplicate_scale` plus optional per-seed detail.
pub fn write_four_scope_paper_summaries(
    output_dir: &Path,
    store: &ResidentStore,
    reports: &[&SeedDedupReport],
    paper_summary: &Value,
) -> Result<(), Analysis2Error> {
    let rels = scope_relations(reports);
    let (intra_detail, matrix_detail, cross_detail) = per_seed_scale_detail(reports);

    let intra_scale =
        build_scope_duplicate_scale(store, rels.iter().copied(), ScopeScaleFilter::Intra);
    let cross_scale =
        build_scope_duplicate_scale(store, rels.iter().copied(), ScopeScaleFilter::Cross);
    let all_scale = build_all_chains_duplicate_scale(store, rels.iter().copied());

    let secondaries = matrix_secondary_chains(rels.iter().copied());
    let mut matrix_blocks = Vec::new();
    for sec in &secondaries {
        let rows = build_scope_duplicate_scale(
            store,
            rels.iter().copied(),
            ScopeScaleFilter::MatrixSecondary(sec.as_str()),
        );
        matrix_blocks.push(json!({
            "secondary_chain": sec,
            "rows": rows,
        }));
    }
    // Overall matrix scale = all cross-chain relations (same numerators as cross).
    let matrix_scale = cross_scale.clone();

    write_one_scope_paper(
        output_dir,
        SCOPE_INTRA_CHAIN,
        SCOPE_INTRA_CHAIN,
        paper_summary,
        &intra_scale,
        json!({ "seeds": intra_detail }),
    )?;
    write_one_scope_paper(
        output_dir,
        SCOPE_CHAIN_MATRIX,
        SCOPE_CHAIN_MATRIX,
        paper_summary,
        &matrix_scale,
        json!({
            "matrix_blocks": matrix_blocks,
            "seeds": matrix_detail,
        }),
    )?;
    write_one_scope_paper(
        output_dir,
        SCOPE_CROSS_CHAIN,
        SCOPE_LABEL_CROSS_CHAIN,
        paper_summary,
        &cross_scale,
        json!({ "seeds": cross_detail }),
    )?;
    write_one_scope_paper(
        output_dir,
        SCOPE_ALL_CHAINS,
        SCOPE_LABEL_ALL_CHAINS,
        paper_summary,
        &all_scale,
        json!({ "seeds": seed_index_json(reports) }),
    )?;
    Ok(())
}

fn write_one_scope_paper(
    output_dir: &Path,
    file_stem: &str,
    scope_label: &str,
    paper_summary: &Value,
    scale: &[DuplicateScaleRow],
    extra: Value,
) -> Result<(), Analysis2Error> {
    let mut body = paper_summary.clone();
    if let Some(obj) = body.as_object_mut() {
        obj.insert("scope".into(), json!(scope_label));
        obj.insert("duplicate_scale".into(), json!(scale));
        if let Some(extra_obj) = extra.as_object() {
            for (k, v) in extra_obj {
                obj.insert(k.clone(), v.clone());
            }
        }
    }
    write_json(&summary_scope_path(output_dir, file_stem, "json"), &body)?;
    super::markdown::write_all_chains_md(
        &summary_scope_path(output_dir, file_stem, "md"),
        &body,
        scale,
    )?;
    Ok(())
}

fn write_scope_rollups(
    output_dir: &Path,
    store: &ResidentStore,
    selected: &[SeedRecord],
    reports: &[&SeedDedupReport],
    failures: &[FailureRecord],
) -> Result<(), Analysis2Error> {
    // Offline dedup path: paper fields are seed/dedup counts only (no deep analysis).
    let with_dup = reports
        .iter()
        .filter(|r| {
            row_has_duplicate(&r.duplicate_scale.intra_chain)
                || row_has_duplicate(&r.duplicate_scale.cross_chain_summary)
        })
        .count() as u64;
    let analyzed = reports.len() as u64;
    let selected_n = selected.len() as u64;
    let paper = json!({
        "selected_seed_count": selected_n,
        "analyzed_seed_count": analyzed,
        "incomplete_seed_count": 0,
        "failed_seed_count": count_failed_seeds(failures),
        "seed_completion_ratio": if selected_n == 0 { None } else { Some(analyzed as f64 / selected_n as f64) },
        "seed_with_duplicate_count": with_dup,
        "seed_duplicate_ratio": if analyzed == 0 { None } else { Some(with_dup as f64 / analyzed as f64) },
        "representative_candidate_count": 0,
        "candidate_contract_count": 0,
        "suspected_duplicate_contract_count": 0,
        "legit_duplicate_contract_count": 0,
        "infringing_nft_count": 0,
        "address_classification": {
            "malicious_address_count": 0,
            "repeat_infringing_malicious_address_count": 0,
            "honest_address_count": 0,
            "total_address_count": 0,
        },
        "behaviors": {},
        "behavior_contract_count": 0,
        "wash_cycle_size_distribution": [],
        "economics": {},
        "data_quality": {
            "failure_record_count": failures.len() as u64,
        },
    });
    write_four_scope_paper_summaries(output_dir, store, reports, &paper)
}


