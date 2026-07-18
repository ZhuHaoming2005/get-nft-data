use dedup_core::{
    Dimension, EntityStore, PrefilterStats, ScopeKind, SummaryAccumulator,
};
use serde::Serialize;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::time::Duration;

#[derive(Serialize)]
struct RunManifest {
    inputs: Vec<String>,
    chains: Vec<String>,
    evm_chains: Vec<String>,
    rows_loaded: u64,
    contracts: usize,
    elapsed_secs: f64,
    name_threshold: f64,
    metadata_threshold: f64,
    metadata_anchors: usize,
    metadata_prefilter: Option<PrefilterStats>,
}

pub fn write_reports(
    output_dir: &Path,
    store: &EntityStore,
    acc: &SummaryAccumulator,
    inputs: &[std::path::PathBuf],
    chains: &[String],
    evm_chains: &[String],
    name_threshold: f64,
    metadata_threshold: f64,
    metadata_anchors: usize,
    metadata_prefilter: Option<PrefilterStats>,
    elapsed: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(output_dir)?;
    write_summary(output_dir, store, acc)?;
    write_matrix(output_dir, store, acc)?;
    let manifest = RunManifest {
        inputs: inputs
            .iter()
            .map(|p| p.display().to_string())
            .collect(),
        chains: chains.to_vec(),
        evm_chains: evm_chains.to_vec(),
        rows_loaded: store.rows_loaded,
        contracts: store.contracts.len(),
        elapsed_secs: elapsed.as_secs_f64(),
        name_threshold,
        metadata_threshold,
        metadata_anchors,
        metadata_prefilter,
    };
    let mut file = File::create(output_dir.join("run_manifest.json"))?;
    serde_json::to_writer_pretty(&mut file, &manifest)?;
    file.write_all(b"\n")?;
    Ok(())
}

fn write_summary(
    output_dir: &Path,
    store: &EntityStore,
    acc: &SummaryAccumulator,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut writer = csv::Writer::from_path(output_dir.join("summary.csv"))?;
    writer.write_record([
        "primary_chain",
        "scope",
        "dimension",
        "duplicate_contract_count",
        "duplicate_nft_count",
        "total_contracts",
        "total_nfts",
    ])?;
    for (key, counts) in acc.counts() {
        if key.kind == ScopeKind::ChainMatrix {
            continue;
        }
        let chain = store.chain_name(key.primary_chain);
        let totals = store.totals.get(&key.primary_chain).cloned().unwrap_or_default();
        writer.write_record([
            chain,
            scope_name(&key.kind),
            dimension_name(key.dimension),
            &counts.duplicate_contract_count.to_string(),
            &counts.duplicate_nft_count.to_string(),
            &totals.contracts.to_string(),
            &totals.nfts.to_string(),
        ])?;
    }
    writer.flush()?;
    Ok(())
}

fn write_matrix(
    output_dir: &Path,
    store: &EntityStore,
    acc: &SummaryAccumulator,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut writer = csv::Writer::from_path(output_dir.join("chain_matrix.csv"))?;
    writer.write_record([
        "primary_chain",
        "secondary_chain",
        "dimension",
        "duplicate_contract_count",
        "duplicate_nft_count",
        "total_contracts",
        "total_nfts",
    ])?;
    for (key, counts) in acc.counts() {
        if key.kind != ScopeKind::ChainMatrix {
            continue;
        }
        let primary = store.chain_name(key.primary_chain);
        let secondary = key
            .secondary_chain
            .map(|id| store.chain_name(id))
            .unwrap_or("");
        let totals = store.totals.get(&key.primary_chain).cloned().unwrap_or_default();
        writer.write_record([
            primary,
            secondary,
            dimension_name(key.dimension),
            &counts.duplicate_contract_count.to_string(),
            &counts.duplicate_nft_count.to_string(),
            &totals.contracts.to_string(),
            &totals.nfts.to_string(),
        ])?;
    }
    writer.flush()?;
    Ok(())
}

fn scope_name(kind: &ScopeKind) -> &'static str {
    match kind {
        ScopeKind::IntraChain => "intra_chain",
        ScopeKind::CrossChainSummary => "cross_chain_summary",
        ScopeKind::ChainMatrix => "chain_matrix",
    }
}

fn dimension_name(dimension: Dimension) -> &'static str {
    match dimension {
        Dimension::Name => "name",
        Dimension::TokenUri => "token_uri",
        Dimension::ImageUri => "image_uri",
        Dimension::Metadata => "metadata",
    }
}
