mod bm25;
mod canonical_json;
mod direct;

pub(crate) use canonical_json::canonicalize_json as canonicalize_json_strict;
pub use direct::MetadataStats;

use crate::entity::EntityStore;
use crate::error::DedupError;
use crate::progress::ProgressObserver;
use crate::sampling::DuplicatePairSamples;
use crate::stats::SummaryAccumulator;

pub struct MetadataRunResult {
    pub stats: MetadataStats,
    pub samples: DuplicatePairSamples,
}

pub fn run_metadata(
    store: &mut EntityStore,
    evm_chains: &std::collections::HashSet<String>,
    anchors_k: Option<usize>,
    content_threshold: f64,
    acc: &mut SummaryAccumulator,
    progress: &dyn ProgressObserver,
) -> Result<MetadataRunResult, DedupError> {
    progress.set_stage("metadata");
    let stats = direct::run_direct_releasing(
        store,
        evm_chains,
        anchors_k,
        content_threshold,
        acc,
        progress,
    )?;

    Ok(MetadataRunResult {
        stats,
        samples: DuplicatePairSamples::default(),
    })
}

pub fn run_metadata_with_samples(
    store: &mut EntityStore,
    evm_chains: &std::collections::HashSet<String>,
    anchors_k: Option<usize>,
    content_threshold: f64,
    acc: &mut SummaryAccumulator,
    progress: &dyn ProgressObserver,
    sample_size: usize,
) -> Result<MetadataRunResult, DedupError> {
    progress.set_stage("metadata");
    let (stats, samples) = direct::run_direct_releasing_with_samples(
        store,
        evm_chains,
        anchors_k,
        content_threshold,
        acc,
        progress,
        sample_size,
    )?;
    Ok(MetadataRunResult { stats, samples })
}
