use crate::dedup::DedupHit;
use crate::model::{Dimension, MatchEvidence, NftSelection};
use crate::resident::candidate_bounds::CandidateBounds;
use crate::resident::{
    NameFeatureStore, NameIndex, PreparedNameQuery, ResidentBaseStore, SeedRawQuery,
};
use ahash::AHashMap;
use rapidfuzz::distance::jaro_winkler::{Args, BatchComparator};
use std::sync::Arc;

pub fn query_name_shard(
    store: &ResidentBaseStore,
    features: &NameFeatureStore,
    index: &NameIndex,
    seed: &SeedRawQuery,
    shard: usize,
    threshold: f64,
) -> Vec<DedupHit> {
    let mut hits = Vec::new();
    if let Some(seed_name) = seed.name_value {
        let query = index.prepare_query(seed_name, threshold);
        visit_name_shard(
            store,
            features,
            index,
            seed,
            shard,
            threshold,
            &query,
            |hit| hits.push(hit),
        );
    }
    hits
}

pub fn query_name_shard_into(
    store: &ResidentBaseStore,
    features: &NameFeatureStore,
    index: &NameIndex,
    seed: &SeedRawQuery,
    shard: usize,
    threshold: f64,
    output: &mut crate::dedup::RelationAccumulator<'_>,
) {
    let Some(seed_name) = seed.name_value else {
        return;
    };
    let query = index.prepare_query(seed_name, threshold);
    visit_name_shard(
        store,
        features,
        index,
        seed,
        shard,
        threshold,
        &query,
        |hit| output.push_hit(hit),
    );
}

/// Same as `query_name_shard_into`, but reuses a `PreparedNameQuery` computed
/// once per seed (see `PreparedNamePlan::build`) instead of
/// recomputing the safe token prefix on every one of the 128 owner-shard
/// queries for that seed.
#[allow(clippy::too_many_arguments)]
pub fn query_name_shard_with_plan_into(
    store: &ResidentBaseStore,
    features: &NameFeatureStore,
    index: &NameIndex,
    seed: &SeedRawQuery,
    shard: usize,
    threshold: f64,
    query: &PreparedNameQuery,
    output: &mut crate::dedup::RelationAccumulator<'_>,
) {
    if seed.name_value.is_none() {
        return;
    }
    visit_name_shard(
        store,
        features,
        index,
        seed,
        shard,
        threshold,
        query,
        |hit| output.push_hit(hit),
    );
}

#[allow(clippy::too_many_arguments)]
fn visit_name_shard(
    store: &ResidentBaseStore,
    features: &NameFeatureStore,
    index: &NameIndex,
    seed: &SeedRawQuery,
    shard: usize,
    threshold: f64,
    query: &PreparedNameQuery,
    mut emit: impl FnMut(DedupHit),
) {
    let Some(seed_name) = seed.name_value else {
        return;
    };
    crate::dedup::scratch::with_worker_scratch(|scratch| {
        let left = index.characters(seed_name);
        let comparator = BatchComparator::new(left.iter().copied());
        let left_text: Arc<str> = Arc::from(features.values.get(seed_name.0));
        let args = Args::default().score_cutoff(threshold);
        let threshold_pct = threshold * 100.0;
        scratch.name_left_counts.clear();
        for &character in left {
            *scratch.name_left_counts.entry(character).or_insert(0_u32) += 1;
        }
        index.candidates_into(shard, query, &mut scratch.name_candidates);
        for &candidate_name in &scratch.name_candidates {
            let right = index.characters(candidate_name);
            if !CandidateBounds::lengths_can_reach(left.len(), right.len(), threshold_pct) {
                continue;
            }
            let overlap = multiset_overlap(
                &scratch.name_left_counts,
                right,
                &mut scratch.name_right_counts,
            );
            if overlap
                < CandidateBounds::minimum_multiset_overlap(left.len(), right.len(), threshold_pct)
            {
                continue;
            }
            let score = if candidate_name == seed_name {
                1.0
            } else {
                let Some(score) = comparator.similarity_with_args(right.iter().copied(), &args)
                else {
                    continue;
                };
                score
            };
            let right_text: Arc<str> = Arc::from(features.values.get(candidate_name.0));
            for &contract_id in index.members(candidate_name) {
                if contract_id == seed.contract_id {
                    continue;
                }
                let contract = store.contracts.key(contract_id);
                let nft_count = store.contracts.contracts[contract_id.index()].nft_count;
                emit(DedupHit {
                    seed_id: seed.seed_id,
                    seed_contract: seed.contract_id,
                    candidate_contract: contract_id,
                    dimension: Dimension::Name,
                    selection: NftSelection::AllInContract {
                        contract,
                        nft_count,
                    },
                    evidence: MatchEvidence::Name {
                        left: left_text.clone(),
                        right: right_text.clone(),
                        similarity: score,
                        threshold,
                    },
                });
            }
        }
        scratch.trim_oversized(1 << 20);
    });
}

fn multiset_overlap(
    left: &AHashMap<char, u32>,
    right: &[char],
    right_counts: &mut AHashMap<char, u32>,
) -> usize {
    right_counts.clear();
    for &character in right {
        *right_counts.entry(character).or_insert(0) += 1;
    }
    right_counts
        .iter()
        .map(|(character, right)| {
            usize::try_from((*right).min(left.get(character).copied().unwrap_or(0)))
                .unwrap_or(usize::MAX)
        })
        .sum()
}
