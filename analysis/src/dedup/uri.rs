use crate::dedup::DedupHit;
use crate::model::{Dimension, MatchEvidence, NftId, NftSelection};
use crate::resident::{
    PreparedUriPosting, PreparedUriShardQuery, ResidentBaseStore, SeedRawQuery, UriFeatureStore,
    UriIndex, UriNftIdentityStore,
};
use std::sync::Arc;

pub fn query_uri_shard(
    store: &ResidentBaseStore,
    identities: &UriNftIdentityStore,
    features: &UriFeatureStore,
    index: &UriIndex,
    seed: &SeedRawQuery,
    shard: usize,
    shard_count: usize,
) -> Vec<DedupHit> {
    let (token_postings, image_postings) = prepare_shard_postings(index, seed, shard);
    let mut hits = Vec::new();
    visit_uri_shard(
        store,
        identities,
        features,
        index,
        seed,
        shard,
        shard_count,
        &token_postings,
        &image_postings,
        |hit| hits.push(hit),
    );
    hits
}

#[allow(clippy::too_many_arguments)]
pub fn query_uri_shard_into(
    store: &ResidentBaseStore,
    identities: &UriNftIdentityStore,
    features: &UriFeatureStore,
    index: &UriIndex,
    seed: &SeedRawQuery,
    shard: usize,
    shard_count: usize,
    output: &mut crate::dedup::RelationAccumulator<'_>,
) {
    let (token_postings, image_postings) = prepare_shard_postings(index, seed, shard);
    visit_uri_shard(
        store,
        identities,
        features,
        index,
        seed,
        shard,
        shard_count,
        &token_postings,
        &image_postings,
        |hit| output.push_hit(hit),
    );
}

#[allow(clippy::too_many_arguments)]
pub fn query_uri_shard_with_plan_into(
    store: &ResidentBaseStore,
    identities: &UriNftIdentityStore,
    features: &UriFeatureStore,
    index: &UriIndex,
    seed: &SeedRawQuery,
    prepared: &PreparedUriShardQuery,
    shard_count: usize,
    output: &mut crate::dedup::RelationAccumulator<'_>,
) {
    visit_uri_shard(
        store,
        identities,
        features,
        index,
        seed,
        prepared.shard,
        shard_count,
        &prepared.token_postings,
        &prepared.image_postings,
        |hit| output.push_hit(hit),
    );
}

#[allow(clippy::too_many_arguments)]
fn visit_uri_shard(
    store: &ResidentBaseStore,
    identities: &UriNftIdentityStore,
    features: &UriFeatureStore,
    index: &UriIndex,
    seed: &SeedRawQuery,
    shard: usize,
    shard_count: usize,
    token_postings: &[PreparedUriPosting],
    image_postings: &[PreparedUriPosting],
    mut emit: impl FnMut(DedupHit),
) {
    crate::dedup::scratch::with_worker_scratch(|scratch| {
        scratch.sparse_seen.clear();
        for probe in token_postings {
            let posting = probe.posting;
            debug_assert_eq!(posting.shard, shard);
            let uri = posting.uri;
            let postings = index.token_postings(posting);
            let uri_text: Arc<str> = Arc::from(features.values.get(uri.0));
            for &candidate_nft in postings {
                debug_assert_eq!(
                    crate::model::owner_shard(candidate_nft.0, shard_count),
                    shard
                );
                let identity = identities.nfts[candidate_nft.index()];
                if identity.contract_id == seed.contract_id {
                    continue;
                }
                scratch.sparse_seen.insert(candidate_nft.0);
                emit(uri_hit(
                    store,
                    seed,
                    candidate_nft,
                    probe.seed_nft.clone(),
                    uri_text.clone(),
                    Dimension::TokenUri,
                ));
            }
        }
        for probe in image_postings {
            let posting = probe.posting;
            debug_assert_eq!(posting.shard, shard);
            let uri = posting.uri;
            let postings = index.image_postings(posting);
            let uri_text: Arc<str> = Arc::from(features.values.get(uri.0));
            for &candidate_nft in postings {
                debug_assert_eq!(
                    crate::model::owner_shard(candidate_nft.0, shard_count),
                    shard
                );
                if scratch.sparse_seen.contains(&candidate_nft.0) {
                    continue;
                }
                let identity = identities.nfts[candidate_nft.index()];
                if identity.contract_id == seed.contract_id {
                    continue;
                }
                emit(uri_hit(
                    store,
                    seed,
                    candidate_nft,
                    probe.seed_nft.clone(),
                    uri_text.clone(),
                    Dimension::ImageUri,
                ));
            }
        }
    });
}

fn prepare_shard_postings(
    index: &UriIndex,
    seed: &SeedRawQuery,
    shard: usize,
) -> (Vec<PreparedUriPosting>, Vec<PreparedUriPosting>) {
    let token_postings = seed
        .token_uri_values
        .iter()
        .flat_map(|&uri| index.token_posting_refs(uri))
        .filter(|posting| posting.shard == shard)
        .map(|&posting| PreparedUriPosting {
            posting,
            seed_nft: seed_uri_evidence(&seed.token_uri_evidence, posting.uri).clone(),
        })
        .collect();
    let image_postings = seed
        .image_uri_values
        .iter()
        .flat_map(|&uri| index.image_posting_refs(uri))
        .filter(|posting| posting.shard == shard)
        .map(|&posting| PreparedUriPosting {
            posting,
            seed_nft: seed_uri_evidence(&seed.image_uri_evidence, posting.uri).clone(),
        })
        .collect();
    (token_postings, image_postings)
}

fn seed_uri_evidence(
    values: &[(crate::model::UriValueId, crate::model::NftKey)],
    uri: crate::model::UriValueId,
) -> &crate::model::NftKey {
    &values[values
        .binary_search_by_key(&uri, |(candidate, _)| *candidate)
        .expect("seed URI evidence is retained")]
    .1
}

fn uri_hit(
    store: &ResidentBaseStore,
    seed: &SeedRawQuery,
    candidate_nft: NftId,
    seed_key: crate::model::NftKey,
    uri: Arc<str>,
    dimension: Dimension,
) -> DedupHit {
    let candidate_key = store
        .nft_key(candidate_nft)
        .expect("URI stage retains identities");
    let candidate_contract =
        store.uri_identity.as_ref().unwrap().nfts[candidate_nft.index()].contract_id;
    DedupHit {
        seed_id: seed.seed_id,
        seed_contract: seed.contract_id,
        candidate_contract,
        dimension,
        selection: NftSelection::Explicit {
            nfts: vec![candidate_key.clone()],
        },
        evidence: MatchEvidence::Uri {
            dimension,
            uri,
            seed_nft: seed_key,
            candidate_nft: candidate_key,
        },
    }
}
