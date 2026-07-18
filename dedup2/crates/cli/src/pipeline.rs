use crate::progress::ProgressReporter;
use crate::report::write_reports;
use dedup_core::{
    load_entities, run_metadata, run_name, run_uri, DedupError, PrefilterConfig,
    ProgressObserver, SummaryAccumulator,
};
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Instant;

#[derive(Clone, Debug)]
pub struct RunConfig {
    pub inputs: Vec<PathBuf>,
    pub output_dir: PathBuf,
    pub chains: Vec<String>,
    pub evm_chains: Vec<String>,
    pub name_threshold: f64,
    pub metadata_threshold: f64,
    pub metadata_anchors: usize,
    pub template_jaccard_threshold: f64,
    pub lsh_bands: u32,
    pub lsh_rows_per_band: u32,
    pub max_outgoing_candidates_per_contract: usize,
    pub max_candidates_per_target_chain: usize,
    pub neighbors_per_target_chain: usize,
    pub run_name: bool,
    pub run_uri: bool,
    pub run_metadata: bool,
}

pub fn run(config: RunConfig, progress: &ProgressReporter) -> Result<(), DedupError> {
    let started = Instant::now();
    let mut store = load_entities(&config.inputs, progress)?;

    let allowed: BTreeSet<String> = config
        .chains
        .iter()
        .map(|c| c.trim().to_ascii_lowercase())
        .filter(|c| !c.is_empty())
        .collect();
    if !allowed.is_empty() {
        store.retain_chains(&allowed);
    }

    let mut acc = SummaryAccumulator::default();
    let name_threshold = config.name_threshold / 100.0;
    if config.run_name {
        run_name(&store, name_threshold, &mut acc, progress)?;
    }
    if config.run_uri {
        run_uri(&store, &mut acc, progress)?;
    }
    let mut metadata_stats = None;
    if config.run_metadata {
        let evm: std::collections::HashSet<String> = config
            .evm_chains
            .iter()
            .map(|c| c.trim().to_ascii_lowercase())
            .collect();
        let prefilter = PrefilterConfig {
            template_jaccard_threshold: config.template_jaccard_threshold,
            lsh_bands: config.lsh_bands,
            lsh_rows_per_band: config.lsh_rows_per_band,
            max_outgoing_candidates_per_contract: config.max_outgoing_candidates_per_contract,
            max_candidates_per_target_chain: config.max_candidates_per_target_chain,
            neighbors_per_target_chain: config.neighbors_per_target_chain,
            bucket_pair_cap: 10_000,
        };
        let result = run_metadata(
            &store,
            &evm,
            config.metadata_anchors,
            config.metadata_threshold,
            prefilter,
            &mut acc,
            progress,
        )?;
        metadata_stats = Some(result.stats);
    }

    progress.set_stage("report");
    progress.set_phase("write");
    progress.set_total(Some(3));
    write_reports(
        &config.output_dir,
        &store,
        &acc,
        &config.inputs,
        &config.chains,
        &config.evm_chains,
        config.name_threshold,
        config.metadata_threshold,
        config.metadata_anchors,
        metadata_stats,
        started.elapsed(),
    )
    .map_err(|error| DedupError::Message(error.to_string()))?;
    progress.add_completed(3);
    Ok(())
}
