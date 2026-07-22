use crate::error::{AnalysisError, Result};
use crate::model::{ContractId, MetadataId, NameValueId, NftKey, ProfileId, SeedId, UriValueId};
use crate::resident::{
    MetadataFeatureStore, MetadataIndex, NameIndex, PreparedMetadataQuery, PreparedNameQuery,
    ResidentBaseStore, UriIndex, UriPostingRef,
};
use crate::seed::SeedManifest;
use ahash::AHashMap;
use std::collections::BTreeSet;

#[derive(Clone, Debug)]
pub struct SeedRawQuery {
    pub seed_id: SeedId,
    pub contract_id: ContractId,
    pub name_value: Option<NameValueId>,
    pub token_uri_values: Vec<UriValueId>,
    pub image_uri_values: Vec<UriValueId>,
    pub token_uri_evidence: Vec<(UriValueId, NftKey)>,
    pub image_uri_evidence: Vec<(UriValueId, NftKey)>,
    pub metadata_profile: Option<ProfileId>,
    pub metadata_documents: Vec<MetadataId>,
}

#[derive(Clone, Debug)]
pub struct SeedRawPlan {
    pub seeds: Vec<SeedRawQuery>,
    pub missing_seed_ids: BTreeSet<SeedId>,
}

impl SeedRawPlan {
    pub fn build(store: &ResidentBaseStore, manifest: &SeedManifest) -> Result<Self> {
        let identities = store
            .uri_identity
            .as_ref()
            .ok_or_else(|| AnalysisError::State("URI identities already released".into()))?;
        let uri_features = store
            .uri_features
            .as_ref()
            .ok_or_else(|| AnalysisError::State("URI features already released".into()))?;
        let name_features = store
            .name_features
            .as_ref()
            .ok_or_else(|| AnalysisError::State("Name features already released".into()))?;
        let metadata_features = store
            .metadata_features
            .as_ref()
            .ok_or_else(|| AnalysisError::State("Metadata features already released".into()))?;
        let mut seeds = Vec::with_capacity(manifest.seeds.len());
        let mut missing_seed_ids = BTreeSet::new();
        for definition in &manifest.seeds {
            let key = definition.contract_key();
            let Some(contract_id) = store.contracts.find(&key) else {
                missing_seed_ids.insert(definition.id);
                continue;
            };
            let mut token_uri_values = Vec::new();
            let mut image_uri_values = Vec::new();
            let mut token_uri_evidence = AHashMap::new();
            let mut image_uri_evidence = AHashMap::new();
            let nft_start = identities.contract_offsets[contract_id.index()] as usize;
            let nft_end = identities.contract_offsets[contract_id.index() + 1] as usize;
            for nft_index in nft_start..nft_end {
                let feature = uri_features.features[nft_index];
                token_uri_values.extend(feature.token_uri);
                image_uri_values.extend(feature.image_uri);
                if let Some(uri) = feature.token_uri {
                    token_uri_evidence.entry(uri).or_insert_with(|| {
                        store
                            .nft_key(crate::model::NftId(nft_index as u32))
                            .expect("seed NFT identity is retained")
                    });
                }
                if let Some(uri) = feature.image_uri {
                    image_uri_evidence.entry(uri).or_insert_with(|| {
                        store
                            .nft_key(crate::model::NftId(nft_index as u32))
                            .expect("seed NFT identity is retained")
                    });
                }
            }
            token_uri_values.sort_unstable();
            token_uri_values.dedup();
            image_uri_values.sort_unstable();
            image_uri_values.dedup();
            let mut token_uri_evidence = token_uri_evidence.into_iter().collect::<Vec<_>>();
            token_uri_evidence.sort_unstable_by_key(|(uri, _)| *uri);
            let mut image_uri_evidence = image_uri_evidence.into_iter().collect::<Vec<_>>();
            image_uri_evidence.sort_unstable_by_key(|(uri, _)| *uri);
            let metadata_profile = metadata_features.contract_profiles[contract_id.index()];
            let metadata_documents = metadata_profile
                .map(|profile| {
                    metadata_features
                        .profile_anchors(profile)
                        .iter()
                        .map(|anchor| anchor.metadata_id)
                        .collect()
                })
                .unwrap_or_default();
            seeds.push(SeedRawQuery {
                seed_id: definition.id,
                contract_id,
                name_value: name_features.contract_names[contract_id.index()],
                token_uri_values,
                image_uri_values,
                token_uri_evidence,
                image_uri_evidence,
                metadata_profile,
                metadata_documents,
            });
        }
        Ok(Self {
            seeds,
            missing_seed_ids,
        })
    }
}

#[derive(Clone, Debug)]
pub struct PreparedUriPosting {
    pub posting: UriPostingRef,
    pub seed_nft: NftKey,
}

#[derive(Clone, Debug, Default)]
pub struct PreparedUriShardQuery {
    pub shard: usize,
    pub token_postings: Vec<PreparedUriPosting>,
    pub image_postings: Vec<PreparedUriPosting>,
}

#[derive(Clone, Debug, Default)]
pub struct PreparedUriQuery {
    pub shards: Vec<PreparedUriShardQuery>,
}

impl PreparedUriQuery {
    fn build(seed: &SeedRawQuery, index: &UriIndex, shard_count: usize) -> Self {
        let mut token_by_shard = vec![Vec::new(); shard_count];
        let mut image_by_shard = vec![Vec::new(); shard_count];
        for &uri in &seed.token_uri_values {
            let seed_nft = seed_uri_evidence(&seed.token_uri_evidence, uri);
            for &posting in index.token_posting_refs(uri) {
                token_by_shard[posting.shard].push(PreparedUriPosting {
                    posting,
                    seed_nft: seed_nft.clone(),
                });
            }
        }
        for &uri in &seed.image_uri_values {
            let seed_nft = seed_uri_evidence(&seed.image_uri_evidence, uri);
            for &posting in index.image_posting_refs(uri) {
                image_by_shard[posting.shard].push(PreparedUriPosting {
                    posting,
                    seed_nft: seed_nft.clone(),
                });
            }
        }
        let shards = token_by_shard
            .into_iter()
            .zip(image_by_shard)
            .enumerate()
            .filter_map(|(shard, (token_postings, image_postings))| {
                (!token_postings.is_empty() || !image_postings.is_empty()).then_some(
                    PreparedUriShardQuery {
                        shard,
                        token_postings,
                        image_postings,
                    },
                )
            })
            .collect();
        Self { shards }
    }
}

fn seed_uri_evidence(values: &[(UriValueId, NftKey)], uri: UriValueId) -> &NftKey {
    &values[values
        .binary_search_by_key(&uri, |(candidate, _)| *candidate)
        .expect("seed URI evidence is retained")]
    .1
}

#[derive(Clone, Debug, Default)]
pub struct PreparedUriPlan {
    pub queries: Vec<PreparedUriQuery>,
}

impl PreparedUriPlan {
    pub fn build(raw_plan: &SeedRawPlan, index: &UriIndex, shard_count: usize) -> Self {
        Self {
            queries: raw_plan
                .seeds
                .iter()
                .map(|seed| PreparedUriQuery::build(seed, index, shard_count))
                .collect(),
        }
    }
}

/// Safe rarity-sorted Name candidate prefixes, prepared once per seed and
/// reused by every owner-shard query.
#[derive(Clone, Debug, Default)]
pub struct PreparedNamePlan {
    pub queries: Vec<PreparedNameQuery>,
}

impl PreparedNamePlan {
    pub fn build(raw_plan: &SeedRawPlan, index: &NameIndex, threshold: f64) -> Self {
        let queries = raw_plan
            .seeds
            .iter()
            .map(|seed| match seed.name_value {
                Some(name) => index.prepare_query(name, threshold),
                None => PreparedNameQuery::direct_verification(),
            })
            .collect();
        Self { queries }
    }
}

/// Exact digests and lossless BM25 posting probes, prepared once per seed
/// after global rarity has been computed and reused by all owner shards.
#[derive(Clone, Debug, Default)]
pub struct PreparedMetadataPlan {
    pub queries: Vec<PreparedMetadataQuery>,
}

impl PreparedMetadataPlan {
    pub fn build(
        raw_plan: &SeedRawPlan,
        features: &MetadataFeatureStore,
        index: &MetadataIndex,
    ) -> Self {
        Self {
            queries: raw_plan
                .seeds
                .iter()
                .map(|seed| index.prepare_query(features, seed.metadata_profile))
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ChainId, InputRow, SourceOrder};
    use crate::resident::ResidentBuilder;
    use crate::seed::SeedDefinition;

    fn seed(id: SeedId, chain: ChainId, contract_address: &str, rank: u8) -> SeedDefinition {
        let collected_at = chrono::Utc::now();
        SeedDefinition {
            id,
            chain,
            contract_address: contract_address.to_owned(),
            rank,
            collection_name: contract_address.to_owned(),
            stable_identifier: contract_address.to_owned(),
            ranking_metric: "total_volume".to_owned(),
            ranking_value: 1.0,
            ranking_window: "all_time".to_owned(),
            source: "opensea".to_owned(),
            collected_at,
        }
    }

    #[test]
    fn missing_snapshot_seed_is_recorded_without_aborting_present_seeds() {
        let mut builder = ResidentBuilder::default();
        builder
            .push(InputRow {
                chain: ChainId::Base,
                contract_address: "present".to_owned(),
                token_id: "1".to_owned(),
                name_norm: Some("present name".to_owned()),
                token_uri_norm: Some("ipfs://present-token".to_owned()),
                image_uri_norm: Some("ipfs://present-image".to_owned()),
                metadata_json: Some(r#"{"name":"present"}"#.to_owned()),
                source_order: SourceOrder {
                    file_ordinal: 0,
                    file_row_number: 0,
                },
            })
            .unwrap();
        let store = builder.finish(8, 128).unwrap();
        let generated_at = chrono::Utc::now();
        let manifest = SeedManifest {
            generated_at,
            seeds: vec![
                seed(SeedId(0), ChainId::Base, "present", 1),
                seed(
                    SeedId(1),
                    ChainId::Base,
                    "0x7d093830fcee68724e81a483def9966f5fac163a",
                    2,
                ),
            ],
        };

        let plan = SeedRawPlan::build(&store, &manifest).unwrap();

        assert_eq!(plan.seeds.len(), 1);
        assert_eq!(plan.seeds[0].seed_id, SeedId(0));
        assert_eq!(plan.missing_seed_ids, BTreeSet::from([SeedId(1)]));
    }
}
