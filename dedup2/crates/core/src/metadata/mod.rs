mod anchors;
mod bm25;
mod canonical_json;
mod prefilter;
mod template;
mod verify;

pub use prefilter::{PrefilterConfig, PrefilterStats};

use crate::entity::{Dimension, EntityStore};
use crate::error::DedupError;
use crate::progress::ProgressObserver;
use crate::stats::SummaryAccumulator;
use anchors::select_anchors;
use prefilter::generate_candidates;
use template::fingerprint_anchors;
use verify::pair_matches;
use ahash::AHashMap;

pub struct MetadataRunResult {
    pub stats: PrefilterStats,
}

pub fn run_metadata(
    store: &EntityStore,
    evm_chains: &std::collections::HashSet<String>,
    anchors_k: usize,
    content_threshold: f64,
    mut prefilter: PrefilterConfig,
    acc: &mut SummaryAccumulator,
    progress: &dyn ProgressObserver,
) -> Result<MetadataRunResult, DedupError> {
    progress.set_stage("metadata");
    progress.set_phase("anchors");
    prefilter.resolve_lsh();

    let mut anchor_map = AHashMap::new();
    let mut fingerprints = Vec::new();
    for contract in &store.contracts {
        let chain_name = store.chain_name(contract.chain_id);
        let is_evm = evm_chains.contains(chain_name);
        let Some(anchors) = select_anchors(contract, anchors_k, is_evm) else {
            continue;
        };
        let fp = fingerprint_anchors(&anchors);
        fingerprints.push((contract.id, contract.chain_id, fp));
        anchor_map.insert(contract.id, anchors);
    }

    progress.set_phase("prefilter");
    let (candidates, stats) = generate_candidates(&fingerprints, &prefilter);
    progress.set_total(Some(candidates.len() as u64));
    progress.set_phase("verify");

    for pair in &candidates {
        progress.check_cancelled()?;
        let Some(left) = anchor_map.get(&pair.left) else {
            progress.add_completed(1);
            continue;
        };
        let Some(right) = anchor_map.get(&pair.right) else {
            progress.add_completed(1);
            continue;
        };
        if pair_matches(left, right, content_threshold) {
            acc.mark_contract_duplicate(store, pair.left, Dimension::Metadata, right.chain_id);
            acc.mark_contract_duplicate(store, pair.right, Dimension::Metadata, left.chain_id);
        }
        progress.add_completed(1);
    }

    Ok(MetadataRunResult { stats })
}
