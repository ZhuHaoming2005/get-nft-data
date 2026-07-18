mod anchors;
mod bm25;
mod canonical_json;
mod prefilter;
mod template;
mod verify;

pub(crate) use canonical_json::canonicalize_json as canonicalize_json_strict;
pub use prefilter::{PrefilterConfig, PrefilterStats};

use crate::entity::{ContractId, Dimension, EntityStore};
use crate::error::DedupError;
use crate::progress::ProgressObserver;
use crate::stats::SummaryAccumulator;
use ahash::AHashSet;
use anchors::select_anchors;
use prefilter::generate_candidates;
use rayon::prelude::*;
use std::sync::atomic::{AtomicBool, Ordering};
use template::fingerprint_anchors;
use verify::pair_matches;

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
    prefilter.resolve_lsh();

    progress.begin_phase("anchors", Some(store.contracts.len() as u64));
    const BATCH: usize = 4096;
    let prepared_chunks: Vec<Result<Vec<_>, DedupError>> = store
        .contracts
        .par_chunks(BATCH)
        .map(|contracts| {
            progress.check_cancelled()?;
            let prepared = contracts
                .iter()
                .filter_map(|contract| {
                    let chain_name = store.chain_name(contract.chain_id);
                    let is_evm = evm_chains.contains(chain_name);
                    let anchors = select_anchors(contract, anchors_k, is_evm)?;
                    let fingerprint = fingerprint_anchors(&anchors);
                    Some((contract.id, contract.chain_id, anchors, fingerprint))
                })
                .collect();
            progress.add_completed(contracts.len() as u64);
            Ok(prepared)
        })
        .collect();
    let mut anchor_map = vec![None; store.contracts.len()];
    let mut fingerprints = Vec::new();
    for chunk in prepared_chunks {
        for (contract_id, chain_id, anchors, fingerprint) in chunk? {
            anchor_map[contract_id as usize] = Some(anchors);
            fingerprints.push((contract_id, chain_id, fingerprint));
        }
    }

    let (candidates, stats) = generate_candidates(fingerprints, &prefilter, progress)?;

    progress.begin_phase("verify", Some(candidates.len() as u64));
    let cancelled = AtomicBool::new(false);
    let matched: AHashSet<(ContractId, crate::entity::ChainId)> = candidates
        .par_chunks(BATCH)
        .map(|chunk| {
            let mut hits = AHashSet::new();
            if progress.check_cancelled().is_err() {
                cancelled.store(true, Ordering::Relaxed);
                return hits;
            }
            for pair in chunk {
                let left = anchor_map[pair.left as usize].as_ref();
                let right = anchor_map[pair.right as usize].as_ref();
                if let (Some(left), Some(right)) = (left, right)
                    && pair_matches(left, right, content_threshold)
                {
                    let left_chain = store.contracts[pair.left as usize].chain_id;
                    let right_chain = store.contracts[pair.right as usize].chain_id;
                    hits.insert((pair.left, right_chain));
                    hits.insert((pair.right, left_chain));
                }
            }
            progress.add_completed(chunk.len() as u64);
            hits
        })
        .reduce(AHashSet::new, |mut left, mut right| {
            if left.len() < right.len() {
                std::mem::swap(&mut left, &mut right);
            }
            left.extend(right);
            left
        });
    if cancelled.load(Ordering::Relaxed) {
        return Err(DedupError::Interrupted);
    }

    for (contract_id, peer_chain) in matched {
        acc.mark_contract_duplicate(store, contract_id, Dimension::Metadata, peer_chain);
    }

    Ok(MetadataRunResult { stats })
}
