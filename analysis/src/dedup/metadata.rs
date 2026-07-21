use crate::dedup::DedupHit;
use crate::model::{ChainId, Dimension, MatchEvidence, NftSelection, ProfileId};
use crate::resident::{
    MetadataFeatureStore, MetadataIndex, PreparedMetadataQuery, ResidentBaseStore, SeedRawQuery,
};
use std::sync::Arc;

pub fn query_metadata_shard(
    store: &ResidentBaseStore,
    features: &MetadataFeatureStore,
    index: &MetadataIndex,
    seed: &SeedRawQuery,
    shard: usize,
    shard_count: usize,
    threshold: f64,
) -> Vec<DedupHit> {
    let prepared = index.prepare_query(features, seed.metadata_profile);
    query_metadata_shard_with_plan(
        store,
        features,
        index,
        seed,
        shard,
        shard_count,
        threshold,
        &prepared,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn query_metadata_shard_with_plan(
    store: &ResidentBaseStore,
    features: &MetadataFeatureStore,
    index: &MetadataIndex,
    seed: &SeedRawQuery,
    shard: usize,
    shard_count: usize,
    threshold: f64,
    prepared: &PreparedMetadataQuery,
) -> Vec<DedupHit> {
    let mut hits = Vec::new();
    visit_metadata_exact_shard(
        store,
        features,
        index,
        seed,
        shard,
        shard_count,
        prepared,
        |hit| hits.push(hit),
    );
    visit_metadata_bm25_shard(
        store,
        features,
        index,
        seed,
        shard,
        shard_count,
        threshold,
        prepared,
        |hit| hits.push(hit),
    );
    hits
}

pub fn query_metadata_exact_shard(
    store: &ResidentBaseStore,
    features: &MetadataFeatureStore,
    index: &MetadataIndex,
    seed: &SeedRawQuery,
    shard: usize,
    shard_count: usize,
) -> Vec<DedupHit> {
    let prepared = index.prepare_query(features, seed.metadata_profile);
    let mut hits = Vec::new();
    visit_metadata_exact_shard(
        store,
        features,
        index,
        seed,
        shard,
        shard_count,
        &prepared,
        |hit| hits.push(hit),
    );
    hits
}

#[allow(clippy::too_many_arguments)]
pub fn query_metadata_exact_shard_into(
    store: &ResidentBaseStore,
    features: &MetadataFeatureStore,
    index: &MetadataIndex,
    seed: &SeedRawQuery,
    shard: usize,
    shard_count: usize,
    output: &mut crate::dedup::RelationAccumulator<'_>,
) {
    let prepared = index.prepare_query(features, seed.metadata_profile);
    query_metadata_exact_shard_with_plan_into(
        store,
        features,
        index,
        seed,
        shard,
        shard_count,
        &prepared,
        output,
    );
}

#[allow(clippy::too_many_arguments)]
pub fn query_metadata_exact_shard_with_plan_into(
    store: &ResidentBaseStore,
    features: &MetadataFeatureStore,
    index: &MetadataIndex,
    seed: &SeedRawQuery,
    shard: usize,
    shard_count: usize,
    prepared: &PreparedMetadataQuery,
    output: &mut crate::dedup::RelationAccumulator<'_>,
) {
    visit_metadata_exact_shard(
        store,
        features,
        index,
        seed,
        shard,
        shard_count,
        prepared,
        |hit| output.push_hit(hit),
    );
}

#[allow(clippy::too_many_arguments)]
fn visit_metadata_exact_shard(
    store: &ResidentBaseStore,
    features: &MetadataFeatureStore,
    index: &MetadataIndex,
    seed: &SeedRawQuery,
    shard: usize,
    shard_count: usize,
    prepared: &PreparedMetadataQuery,
    mut emit: impl FnMut(DedupHit),
) {
    let Some(_seed_profile_id) = seed.metadata_profile else {
        return;
    };
    let shard_index = &index.shards[shard];
    crate::dedup::scratch::with_worker_scratch(|scratch| {
        scratch.sparse_seen.clear();
        scratch.metadata_candidates.clear();
        for digest in &prepared.exact_digests {
            if let Some(postings) = shard_index.exact_postings.get(digest) {
                for &profile in postings {
                    if scratch.sparse_seen.insert(profile.0) {
                        scratch.metadata_candidates.push(profile);
                    }
                }
            }
        }
        scratch.metadata_candidates.sort_unstable();
        score_metadata_candidates(
            MetadataScoreContext {
                store,
                features,
                index,
                seed,
                shard,
                shard_count,
                threshold: 1.0,
            },
            &scratch.metadata_candidates,
            MetadataWave::Exact,
            &mut emit,
        )
    });
}

pub fn query_metadata_bm25_shard(
    store: &ResidentBaseStore,
    features: &MetadataFeatureStore,
    index: &MetadataIndex,
    seed: &SeedRawQuery,
    shard: usize,
    shard_count: usize,
    threshold: f64,
) -> Vec<DedupHit> {
    let prepared = index.prepare_query(features, seed.metadata_profile);
    let mut hits = Vec::new();
    visit_metadata_bm25_shard(
        store,
        features,
        index,
        seed,
        shard,
        shard_count,
        threshold,
        &prepared,
        |hit| hits.push(hit),
    );
    hits
}

#[allow(clippy::too_many_arguments)]
pub fn query_metadata_bm25_shard_into(
    store: &ResidentBaseStore,
    features: &MetadataFeatureStore,
    index: &MetadataIndex,
    seed: &SeedRawQuery,
    shard: usize,
    shard_count: usize,
    threshold: f64,
    output: &mut crate::dedup::RelationAccumulator<'_>,
) {
    let prepared = index.prepare_query(features, seed.metadata_profile);
    query_metadata_bm25_shard_with_plan_into(
        store,
        features,
        index,
        seed,
        shard,
        shard_count,
        threshold,
        &prepared,
        output,
    );
}

#[allow(clippy::too_many_arguments)]
pub fn query_metadata_bm25_shard_with_plan_into(
    store: &ResidentBaseStore,
    features: &MetadataFeatureStore,
    index: &MetadataIndex,
    seed: &SeedRawQuery,
    shard: usize,
    shard_count: usize,
    threshold: f64,
    prepared: &PreparedMetadataQuery,
    output: &mut crate::dedup::RelationAccumulator<'_>,
) {
    visit_metadata_bm25_shard(
        store,
        features,
        index,
        seed,
        shard,
        shard_count,
        threshold,
        prepared,
        |hit| output.push_hit(hit),
    );
}

#[allow(clippy::too_many_arguments)]
fn visit_metadata_bm25_shard(
    store: &ResidentBaseStore,
    features: &MetadataFeatureStore,
    index: &MetadataIndex,
    seed: &SeedRawQuery,
    shard: usize,
    shard_count: usize,
    threshold: f64,
    prepared: &PreparedMetadataQuery,
    mut emit: impl FnMut(DedupHit),
) {
    let Some(_seed_profile_id) = seed.metadata_profile else {
        return;
    };
    let shard_index = &index.shards[shard];
    crate::dedup::scratch::with_worker_scratch(|scratch| {
        scratch.sparse_seen.clear();
        scratch.metadata_candidates.clear();
        if threshold <= 0.0 {
            scratch
                .metadata_candidates
                .extend(shard_index.profiles.iter().copied());
        } else {
            // Probe the rarest (token_id, term) and term keys first. Keys
            // that a single profile owns anywhere in the whole corpus were
            // already dropped from every shard's postings by
            // `prune_singleton_postings`, so a miss in `index.token_term_rarity`
            // / `index.term_rarity` means the lookup is guaranteed empty and
            // is skipped instead of touching the (possibly large) postings map.
            for &key in &prepared.token_term_probes {
                if let Some(postings) = shard_index.token_term_postings.get(&key) {
                    for &profile in postings {
                        if scratch.sparse_seen.insert(profile.0) {
                            scratch.metadata_candidates.push(profile);
                        }
                    }
                }
            }
            for &term in &prepared.term_probes {
                if let Some(postings) = shard_index.term_postings.get(&term) {
                    for &profile in postings {
                        if scratch.sparse_seen.insert(profile.0) {
                            scratch.metadata_candidates.push(profile);
                        }
                    }
                }
            }
        }
        scratch.metadata_candidates.sort_unstable();
        score_metadata_candidates(
            MetadataScoreContext {
                store,
                features,
                index,
                seed,
                shard,
                shard_count,
                threshold,
            },
            &scratch.metadata_candidates,
            MetadataWave::Bm25,
            &mut emit,
        )
    });
}

#[derive(Clone, Copy)]
enum MetadataWave {
    Exact,
    Bm25,
}

struct MetadataScoreContext<'a> {
    store: &'a ResidentBaseStore,
    features: &'a MetadataFeatureStore,
    index: &'a MetadataIndex,
    seed: &'a SeedRawQuery,
    shard: usize,
    shard_count: usize,
    threshold: f64,
}

fn score_metadata_candidates(
    context: MetadataScoreContext<'_>,
    candidates: &[ProfileId],
    wave: MetadataWave,
    emit: &mut impl FnMut(DedupHit),
) {
    let MetadataScoreContext {
        store,
        features,
        index,
        seed,
        shard,
        shard_count,
        threshold,
    } = context;
    let seed_profile_id = seed
        .metadata_profile
        .expect("metadata candidates require a seed profile");
    let seed_anchors = features.profile_anchors(seed_profile_id);
    let seed_chain = store.chain(seed.contract_id);
    for &candidate_profile_id in candidates {
        debug_assert_eq!(
            crate::model::owner_shard(candidate_profile_id.0, shard_count),
            shard
        );
        let candidate_members = features.profile_members(candidate_profile_id);
        if candidate_members
            .iter()
            .all(|contract_id| *contract_id == seed.contract_id)
        {
            continue;
        }
        let candidate_anchors = features.profile_anchors(candidate_profile_id);
        let candidate_chain = store.chain(candidate_members[0]);
        let (seed_anchor, candidate_anchor) = select_anchors(
            seed_anchors,
            seed_chain,
            candidate_anchors,
            candidate_chain,
            features,
        );
        let left = &index.documents[seed_anchor.metadata_id.index()];
        let right = &index.documents[candidate_anchor.metadata_id.index()];
        let exact = left.digest == right.digest
            && features.documents.get(seed_anchor.metadata_id.0)
                == features.documents.get(candidate_anchor.metadata_id.0);
        let (matched, score) = match wave {
            MetadataWave::Exact => (exact, 1.0),
            MetadataWave::Bm25 if exact => (false, 1.0),
            MetadataWave::Bm25 => index.similarity(
                seed_anchor.metadata_id,
                candidate_anchor.metadata_id,
                threshold,
            ),
        };
        if !matched {
            continue;
        }
        let seed_token: Arc<str> = Arc::from(features.anchor_tokens.get(seed_anchor.token_id_id.0));
        let candidate_token: Arc<str> =
            Arc::from(features.anchor_tokens.get(candidate_anchor.token_id_id.0));
        let seed_digest: Arc<str> = Arc::from(crate::resident::hex_encode(&left.digest));
        let candidate_digest: Arc<str> = Arc::from(crate::resident::hex_encode(&right.digest));
        for &contract_id in candidate_members {
            if contract_id == seed.contract_id {
                continue;
            }
            let contract = store.contracts.key(contract_id);
            let nft_count = store.contracts.contracts[contract_id.index()].nft_count;
            emit(DedupHit {
                seed_id: seed.seed_id,
                seed_contract: seed.contract_id,
                candidate_contract: contract_id,
                dimension: Dimension::Metadata,
                selection: NftSelection::AllInContract {
                    contract,
                    nft_count,
                },
                evidence: MatchEvidence::Metadata {
                    seed_token_id: seed_token.clone(),
                    candidate_token_id: candidate_token.clone(),
                    seed_digest: seed_digest.clone(),
                    candidate_digest: candidate_digest.clone(),
                    similarity: score,
                    threshold,
                },
            });
        }
    }
}

fn select_anchors<'a>(
    left: &'a [crate::model::MetadataAnchor],
    left_chain: ChainId,
    right: &'a [crate::model::MetadataAnchor],
    right_chain: ChainId,
    features: &MetadataFeatureStore,
) -> (
    &'a crate::model::MetadataAnchor,
    &'a crate::model::MetadataAnchor,
) {
    let mut selected = None;
    for left_anchor in left {
        let left_token = token_match_key(
            left_chain,
            features.anchor_tokens.get(left_anchor.token_id_id.0),
        );
        for right_anchor in right {
            let right_token = token_match_key(
                right_chain,
                features.anchor_tokens.get(right_anchor.token_id_id.0),
            );
            if left_token == right_token {
                // Left anchors are already in the chain-specific token order,
                // so overwriting selects its largest common anchor without a
                // lexicographic comparison of arbitrary-precision EVM IDs.
                selected = Some((left_anchor, right_anchor));
                break;
            }
        }
    }
    selected.unwrap_or_else(|| {
        (
            left.last().expect("profiles are nonempty"),
            right.last().expect("profiles are nonempty"),
        )
    })
}

fn token_match_key(chain: ChainId, token: &str) -> &str {
    if chain.is_evm() {
        let trimmed = token.trim_start_matches('0');
        if trimmed.is_empty() {
            "0"
        } else {
            trimmed
        }
    } else {
        token
    }
}
