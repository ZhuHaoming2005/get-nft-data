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
    pub image_samples: Vec<MetadataImagePairSample>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataImagePairSample {
    pub contract_a_chain: String,
    pub contract_a_address: String,
    pub token_id_a: String,
    pub image_uri_a: String,
    pub metadata_json_a: String,
    pub contract_b_chain: String,
    pub contract_b_address: String,
    pub token_id_b: String,
    pub image_uri_b: String,
    pub metadata_json_b: String,
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
        image_samples: Vec::new(),
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
    let (stats, samples, image_samples) = direct::run_direct_releasing_with_samples(
        store,
        evm_chains,
        anchors_k,
        content_threshold,
        acc,
        progress,
        sample_size,
    )?;
    Ok(MetadataRunResult {
        stats,
        samples,
        image_samples,
    })
}
