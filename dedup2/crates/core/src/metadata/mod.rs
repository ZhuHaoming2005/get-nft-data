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

pub fn run_metadata(
    store: &EntityStore,
    evm_chains: &std::collections::HashSet<String>,
    anchors_k: usize,
    content_threshold: f64,
    acc: &mut SummaryAccumulator,
    progress: &dyn ProgressObserver,
) -> Result<MetadataRunResult, DedupError> {
    progress.set_stage("metadata");
    let stats = direct::run_direct(
        store,
        evm_chains,
        anchors_k,
        content_threshold,
        acc,
        progress,
    )?;

    Ok(MetadataRunResult { stats })
}
