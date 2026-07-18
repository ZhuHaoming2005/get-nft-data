use dedup_core::{Dimension, EntityStore, PrefilterStats, ScopeKind, SummaryAccumulator};
use serde::Serialize;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::thread;
use std::time::Duration;

type ReportError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Serialize)]
struct RunManifest {
    inputs: Vec<String>,
    chains: Vec<String>,
    evm_chains: Vec<String>,
    rows_loaded: u64,
    contracts: usize,
    chain_totals: Vec<ChainTotalRow>,
    elapsed_secs: f64,
    name_threshold: f64,
    metadata_threshold: f64,
    metadata_anchors: usize,
    metadata_prefilter: Option<PrefilterStats>,
}

#[derive(Serialize)]
struct ChainTotalRow {
    chain: String,
    contracts: u64,
    nfts: u64,
}

pub struct ReportRequest<'a> {
    pub store: &'a EntityStore,
    pub accumulator: &'a SummaryAccumulator,
    pub inputs: &'a [std::path::PathBuf],
    pub chains: &'a [String],
    pub evm_chains: &'a [String],
    pub name_threshold: f64,
    pub metadata_threshold: f64,
    pub metadata_anchors: usize,
    pub metadata_prefilter: Option<PrefilterStats>,
    pub elapsed: Duration,
}

pub fn write_reports(output_dir: &Path, request: ReportRequest<'_>) -> Result<(), ReportError> {
    fs::create_dir_all(output_dir)?;
    let mut chain_totals: Vec<ChainTotalRow> = request
        .store
        .totals
        .iter()
        .map(|(id, totals)| ChainTotalRow {
            chain: request.store.chain_name(*id).to_owned(),
            contracts: totals.contracts,
            nfts: totals.nfts,
        })
        .collect();
    chain_totals.sort_by(|a, b| a.chain.cmp(&b.chain));
    let manifest = RunManifest {
        inputs: request
            .inputs
            .iter()
            .map(|path| path.display().to_string())
            .collect(),
        chains: request.chains.to_vec(),
        evm_chains: request.evm_chains.to_vec(),
        rows_loaded: request.store.rows_loaded,
        contracts: request.store.contracts.len(),
        chain_totals,
        elapsed_secs: request.elapsed.as_secs_f64(),
        name_threshold: request.name_threshold,
        metadata_threshold: request.metadata_threshold,
        metadata_anchors: request.metadata_anchors,
        metadata_prefilter: request.metadata_prefilter,
    };
    thread::scope(|scope| -> Result<(), ReportError> {
        let summary = scope.spawn(|| write_summary(output_dir, request.store, request.accumulator));
        let matrix = scope.spawn(|| write_matrix(output_dir, request.store, request.accumulator));
        let manifest = scope.spawn(|| write_manifest(output_dir, &manifest));
        summary
            .join()
            .map_err(|_| std::io::Error::other("summary writer panicked"))??;
        matrix
            .join()
            .map_err(|_| std::io::Error::other("matrix writer panicked"))??;
        manifest
            .join()
            .map_err(|_| std::io::Error::other("manifest writer panicked"))??;
        Ok(())
    })
}

fn write_manifest(output_dir: &Path, manifest: &RunManifest) -> Result<(), ReportError> {
    let mut file = File::create(output_dir.join("run_manifest.json"))?;
    serde_json::to_writer_pretty(&mut file, manifest)?;
    file.write_all(b"\n")?;
    Ok(())
}

fn write_summary(
    output_dir: &Path,
    store: &EntityStore,
    acc: &SummaryAccumulator,
) -> Result<(), ReportError> {
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
    let mut rows: Vec<_> = acc
        .counts()
        .iter()
        .filter(|(key, _)| key.kind != ScopeKind::ChainMatrix)
        .collect();
    rows.sort_by(|(a, _), (b, _)| {
        store
            .chain_name(a.primary_chain)
            .cmp(store.chain_name(b.primary_chain))
            .then(scope_name(&a.kind).cmp(scope_name(&b.kind)))
            .then(dimension_name(a.dimension).cmp(dimension_name(b.dimension)))
    });
    for (key, counts) in rows {
        let chain = store.chain_name(key.primary_chain);
        let totals = store
            .totals
            .get(&key.primary_chain)
            .cloned()
            .unwrap_or_default();
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
) -> Result<(), ReportError> {
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
    let mut rows: Vec<_> = acc
        .counts()
        .iter()
        .filter(|(key, _)| key.kind == ScopeKind::ChainMatrix)
        .collect();
    rows.sort_by(|(a, _), (b, _)| {
        let a_sec = a
            .secondary_chain
            .map(|id| store.chain_name(id))
            .unwrap_or("");
        let b_sec = b
            .secondary_chain
            .map(|id| store.chain_name(id))
            .unwrap_or("");
        store
            .chain_name(a.primary_chain)
            .cmp(store.chain_name(b.primary_chain))
            .then(a_sec.cmp(b_sec))
            .then(dimension_name(a.dimension).cmp(dimension_name(b.dimension)))
    });
    for (key, counts) in rows {
        let primary = store.chain_name(key.primary_chain);
        let secondary = key
            .secondary_chain
            .map(|id| store.chain_name(id))
            .unwrap_or("");
        let totals = store
            .totals
            .get(&key.primary_chain)
            .cloned()
            .unwrap_or_default();
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
