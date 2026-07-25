//! JSON writers and seed-report payloads for offline dedup runs.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::dedup::candidates::CandidateRegistry;
use crate::dedup::hits::{Dimension, HitGraph};
use crate::entity::{ContractId, NftId, ResidentStore};
use crate::error::Analysis2Error;

use super::aggregate::{
    AllChainsRelationRef, DuplicateScaleRow, ScopeScaleFilter, SeedDuplicateScale,
    build_scope_duplicate_scale_for_chains, build_seed_duplicate_scale,
};
use super::layout::{
    SCOPE_ALL_CHAINS, SCOPE_CHAIN_MATRIX, SCOPE_CROSS_CHAIN, SCOPE_INTRA_CHAIN,
    SCOPE_LABEL_ALL_CHAINS, SCOPE_LABEL_CROSS_CHAIN, ensure_output_layout, intermediate_path,
    seed_report_dir, summary_scope_path,
};
use super::manifest::{FailureRecord, RunManifest, RunManifestSeeds, count_failed_seeds};

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
    store
        .contract_id(&seed.chain, &seed.address)
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
    all_summary: &serde_json::Value,
    intra_summary: &serde_json::Value,
    cross_summary: &serde_json::Value,
    matrix_summaries: &BTreeMap<(String, String), serde_json::Value>,
) -> Result<(), Analysis2Error> {
    write_four_scope_paper_summaries(
        output_dir,
        store,
        reports,
        all_summary,
        intra_summary,
        cross_summary,
        matrix_summaries,
    )
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
    all_summary: &Value,
    intra_summary: &Value,
    cross_summary: &Value,
    matrix_summaries: &BTreeMap<(String, String), Value>,
) -> Result<(), Analysis2Error> {
    let rels = scope_relations(reports);
    let (intra_detail, matrix_detail, cross_detail) = per_seed_scale_detail(reports);
    let primary_chains: ahash::AHashSet<String> = reports
        .iter()
        .map(|report| report.seed.chain.to_ascii_lowercase())
        .collect();

    let intra_scale = build_scope_duplicate_scale_for_chains(
        store,
        rels.iter().copied(),
        ScopeScaleFilter::Intra,
        &primary_chains,
    );
    let cross_scale = build_scope_duplicate_scale_for_chains(
        store,
        rels.iter().copied(),
        ScopeScaleFilter::Cross,
        &primary_chains,
    );
    let all_scale = build_scope_duplicate_scale_for_chains(
        store,
        rels.iter().copied(),
        ScopeScaleFilter::All,
        &primary_chains,
    );

    let mut matrix_blocks = Vec::new();
    let mut primaries: Vec<_> = primary_chains.iter().cloned().collect();
    primaries.sort();
    for primary in &primaries {
        for secondary in &store.chains {
            if primary.eq_ignore_ascii_case(secondary) {
                continue;
            }
            let rows = build_scope_duplicate_scale_for_chains(
                store,
                rels.iter().copied(),
                ScopeScaleFilter::Matrix {
                    primary_chain: primary,
                    secondary_chain: secondary,
                },
                &primary_chains,
            );
            matrix_blocks.push(json!({
                "primary_chain": primary,
                "secondary_chain": secondary,
                "rows": rows,
                "summary": matrix_summaries.get(&(primary.clone(), secondary.to_ascii_lowercase())),
            }));
        }
    }
    // Overall matrix scale = all cross-chain relations (same numerators as cross).
    let matrix_scale = cross_scale.clone();

    write_one_scope_paper(
        output_dir,
        SCOPE_INTRA_CHAIN,
        SCOPE_INTRA_CHAIN,
        intra_summary,
        &intra_scale,
        json!({ "seed_scale_detail": intra_detail }),
    )?;
    write_one_scope_paper(
        output_dir,
        SCOPE_CHAIN_MATRIX,
        SCOPE_CHAIN_MATRIX,
        cross_summary,
        &matrix_scale,
        json!({
            "matrix_blocks": matrix_blocks,
            "seed_scale_detail": matrix_detail,
        }),
    )?;
    write_one_scope_paper(
        output_dir,
        SCOPE_CROSS_CHAIN,
        SCOPE_LABEL_CROSS_CHAIN,
        cross_summary,
        &cross_scale,
        json!({ "seed_scale_detail": cross_detail }),
    )?;
    write_one_scope_paper(
        output_dir,
        SCOPE_ALL_CHAINS,
        SCOPE_LABEL_ALL_CHAINS,
        all_summary,
        &all_scale,
        json!({ "seed_index": seed_index_json(reports) }),
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
    let paper_for = |filter: ScopeScaleFilter<'_>| {
        let mut candidates = ahash::AHashSet::new();
        let mut candidates_with_ids = ahash::AHashSet::new();
        let mut nft_ids = ahash::AHashSet::new();
        let mut fallback_by_candidate = ahash::AHashMap::<String, u64>::new();
        let mut with_dup = 0u64;
        for report in reports {
            let mut seed_has_duplicate = false;
            for relation in &report.relations {
                let same = report
                    .seed
                    .chain
                    .eq_ignore_ascii_case(&relation.candidate_chain);
                let matches = match filter {
                    ScopeScaleFilter::All => true,
                    ScopeScaleFilter::Intra => same,
                    ScopeScaleFilter::Cross => !same,
                    ScopeScaleFilter::Matrix { .. } => false,
                };
                if !matches {
                    continue;
                }
                seed_has_duplicate = true;
                let key = format!(
                    "{}:{}",
                    relation.candidate_chain, relation.candidate_address
                );
                candidates.insert(key.clone());
                nft_ids.extend(relation.nft_ids.iter().copied());
                if !relation.nft_ids.is_empty() {
                    candidates_with_ids.insert(key.clone());
                }
                fallback_by_candidate
                    .entry(key)
                    .and_modify(|count| *count = (*count).max(relation.nft_count))
                    .or_insert(relation.nft_count);
            }
            with_dup += u64::from(seed_has_duplicate);
        }
        let missing_id_nfts: u64 = fallback_by_candidate
            .iter()
            .filter(|(candidate, _)| !candidates_with_ids.contains(*candidate))
            .map(|(_, count)| *count)
            .sum();
        let analyzed = reports.len() as u64;
        let selected_n = selected.len() as u64;
        let representative_nfts = nft_ids.len() as u64 + missing_id_nfts;
        json!({
            "analysis_available": false,
            "selected_seed_count": selected_n,
            "analyzed_seed_count": analyzed,
            "incomplete_seed_count": 0,
            "failed_seed_count": count_failed_seeds(failures),
            "seed_completion_ratio": (selected_n > 0).then_some(analyzed as f64 / selected_n as f64),
            "seed_with_duplicate_count": with_dup,
            "seed_duplicate_ratio": (analyzed > 0).then_some(with_dup as f64 / analyzed as f64),
            "representative_candidate_count": representative_nfts,
            "representative_candidate_nft_count": representative_nfts,
            "candidate_contract_count": candidates.len() as u64,
            "suspected_duplicate_contract_count": Value::Null,
            "legit_duplicate_contract_count": Value::Null,
            "infringing_nft_count": Value::Null,
            "data_quality": {
                "analysis_available": false,
                "failure_record_count": failures.len() as u64,
                "failure_record_count_scope": "global_run",
            },
        })
    };
    let all_paper = paper_for(ScopeScaleFilter::All);
    let intra_paper = paper_for(ScopeScaleFilter::Intra);
    let cross_paper = paper_for(ScopeScaleFilter::Cross);
    write_four_scope_paper_summaries(
        output_dir,
        store,
        reports,
        &all_paper,
        &intra_paper,
        &cross_paper,
        &BTreeMap::new(),
    )
}
