mod bm25;
mod canonical_json;
mod direct;

pub(crate) use canonical_json::canonicalize_json as canonicalize_json_strict;
pub use direct::MetadataStats;

use crate::entity::EntityStore;
use crate::error::DedupError;
use crate::progress::ProgressObserver;
use crate::stats::SummaryAccumulator;

pub struct MetadataRunResult {
    pub stats: MetadataStats,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetadataSamplePool {
    IntraChain,
    CrossChain,
}

pub struct MetadataFastSampleResult {
    pub intra_chain: Vec<MetadataImagePairSample>,
    pub cross_chain: Vec<MetadataImagePairSample>,
    pub scored_candidate_tasks: u64,
    pub visited_profiles: u64,
    pub total_profiles: u64,
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

    Ok(MetadataRunResult { stats })
}

#[allow(clippy::too_many_arguments)]
pub fn sample_metadata<F>(
    store: &mut EntityStore,
    evm_chains: &std::collections::HashSet<String>,
    anchors_k: Option<usize>,
    content_threshold: f64,
    target_per_pool: usize,
    progress: &dyn ProgressObserver,
    mut accept: F,
) -> Result<MetadataFastSampleResult, DedupError>
where
    F: FnMut(&[(MetadataSamplePool, MetadataImagePairSample)]) -> Result<Vec<bool>, DedupError>,
{
    progress.set_stage("sample_metadata");
    direct::sample_direct_releasing(
        store,
        evm_chains,
        anchors_k,
        content_threshold,
        target_per_pool,
        progress,
        &mut accept,
    )
}
