use analysis::config::{NumaMode, ProviderConcurrency, RunConfig};
use analysis::dedup::{
    query_metadata_shard, query_metadata_shard_with_plan, query_name_shard, query_uri_shard,
};
use analysis::model::{ChainId, ContractId, InputRow, SeedId, SourceOrder};
use analysis::resident::{MetadataIndex, NameIndex, ResidentBuilder, SeedRawQuery, UriIndex};
use analysis::seed::{SeedDefinition, SeedManifest};
use rapidfuzz::distance::jaro_winkler;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

fn row(
    identity: (ChainId, &str, &str),
    content: (&str, &str, &str, &str),
    row_number: u64,
) -> InputRow {
    let (chain, contract, token) = identity;
    let (name, token_uri, image_uri, metadata) = content;
    InputRow {
        chain,
        contract_address: contract.to_owned(),
        token_id: token.to_owned(),
        name_norm: Some(name.to_owned()),
        token_uri_norm: Some(token_uri.to_owned()),
        image_uri_norm: Some(image_uri.to_owned()),
        metadata_json: Some(metadata.to_owned()),
        source_order: SourceOrder {
            file_ordinal: 0,
            file_row_number: row_number,
        },
    }
}

fn fixture() -> analysis::resident::ResidentBaseStore {
    let mut builder = ResidentBuilder::default();
    for (index, input) in [
        row(
            (ChainId::Base, "base-seed", "1"),
            (
                "alpha collection",
                "ipfs://shared-token",
                "ipfs://seed-image",
                r#"{"name":"alpha","attributes":[{"trait_type":"class","value":"one"}]}"#,
            ),
            0,
        ),
        row(
            (ChainId::Ethereum, "eth-copy", "1"),
            (
                "alpha collection",
                "ipfs://shared-token",
                "ipfs://other-image",
                r#"{"name":"alpha","attributes":[{"trait_type":"class","value":"one"}]}"#,
            ),
            1,
        ),
        row(
            (ChainId::Polygon, "polygon-image", "7"),
            (
                "unrelated polygon",
                "ipfs://different-token",
                "ipfs://seed-image",
                r#"{"name":"unrelated polygon"}"#,
            ),
            2,
        ),
        row(
            (ChainId::Solana, "solana-copy", "mint-a"),
            (
                "alpha collections",
                "ipfs://sol-token",
                "ipfs://sol-image",
                r#"{"name":"alpha","attributes":[{"trait_type":"class","value":"one"}]}"#,
            ),
            3,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let _ = index;
        builder.push(input).unwrap();
    }
    builder.finish(8, 128).unwrap()
}

fn seed_query(store: &analysis::resident::ResidentBaseStore) -> SeedRawQuery {
    let contract_id = ContractId(0);
    let uri = store.uri_features.as_ref().unwrap();
    let name = store.name_features.as_ref().unwrap();
    let metadata = store.metadata_features.as_ref().unwrap();
    let token_uri = uri.features[0].token_uri.unwrap();
    let image_uri = uri.features[0].image_uri.unwrap();
    let profile = metadata.contract_profiles[0].unwrap();
    let evidence_nft = store.nft_key(analysis::model::NftId(0)).unwrap();
    SeedRawQuery {
        seed_id: SeedId(0),
        contract_id,
        name_value: name.contract_names[0],
        token_uri_values: vec![token_uri],
        image_uri_values: vec![image_uri],
        token_uri_evidence: vec![(token_uri, evidence_nft.clone())],
        image_uri_evidence: vec![(image_uri, evidence_nft)],
        metadata_profile: Some(profile),
        metadata_documents: metadata
            .profile_anchors(profile)
            .iter()
            .map(|anchor| anchor.metadata_id)
            .collect(),
    }
}

#[test]
fn indexed_name_candidates_equal_exhaustive_oracle() {
    let store = fixture();
    let seed = seed_query(&store);
    let features = store.name_features.as_ref().unwrap();
    let index = NameIndex::build(features, &[seed.name_value.unwrap()], 128);
    let indexed = (0..128)
        .flat_map(|shard| query_name_shard(&store, features, &index, &seed, shard, 0.98))
        .map(|hit| hit.candidate_contract)
        .collect::<BTreeSet<_>>();
    let seed_name = features.values.get(seed.name_value.expect("seed name").0);
    let exhaustive = features
        .contract_names
        .iter()
        .enumerate()
        .filter_map(|(contract, candidate)| {
            let candidate = (*candidate)?;
            if contract == seed.contract_id.index() {
                return None;
            }
            let score = jaro_winkler::similarity(
                seed_name.chars(),
                features.values.get(candidate.0).chars(),
            );
            (score >= 0.98).then_some(ContractId(contract as u32))
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(indexed, exhaustive);
    assert!(indexed.contains(&ContractId(1)));
    assert!(indexed.contains(&ContractId(3)));
}

#[test]
fn uri_priority_and_all_four_chains_are_preserved() {
    let store = fixture();
    let seed = seed_query(&store);
    let identities = store.uri_identity.as_ref().unwrap();
    let features = store.uri_features.as_ref().unwrap();
    let index = UriIndex::build(
        identities,
        features,
        &seed.token_uri_values,
        &seed.image_uri_values,
        128,
    );
    let hits = (0..128)
        .flat_map(|shard| query_uri_shard(&store, identities, features, &index, &seed, shard, 128))
        .collect::<Vec<_>>();
    assert!(hits.iter().any(|hit| {
        hit.candidate_contract == ContractId(1)
            && hit.dimension == analysis::model::Dimension::TokenUri
    }));
    assert!(hits.iter().any(|hit| {
        hit.candidate_contract == ContractId(2)
            && hit.dimension == analysis::model::Dimension::ImageUri
    }));
    assert!(!hits.iter().any(|hit| {
        hit.candidate_contract == ContractId(1)
            && hit.dimension == analysis::model::Dimension::ImageUri
    }));
}

#[test]
fn metadata_index_covers_exhaustive_selected_anchor_matches() {
    let store = fixture();
    let seed = seed_query(&store);
    let features = store.metadata_features.as_ref().unwrap();
    let index = MetadataIndex::build(features, &seed.metadata_documents, 128);
    let indexed = (0..128)
        .flat_map(|shard| query_metadata_shard(&store, features, &index, &seed, shard, 128, 0.6))
        .map(|hit| hit.candidate_contract)
        .collect::<BTreeSet<_>>();
    let prepared = index.prepare_query(features, seed.metadata_profile);
    let prepared_indexed = (0..128)
        .flat_map(|shard| {
            query_metadata_shard_with_plan(
                &store, features, &index, &seed, shard, 128, 0.6, &prepared,
            )
        })
        .map(|hit| hit.candidate_contract)
        .collect::<BTreeSet<_>>();
    assert_eq!(prepared_indexed, indexed);
    let seed_anchor = features
        .profile_anchors(seed.metadata_profile.unwrap())
        .last()
        .unwrap();
    let exhaustive = features
        .contract_profiles
        .iter()
        .enumerate()
        .filter_map(|(contract, profile)| {
            if contract == seed.contract_id.index() {
                return None;
            }
            let candidate_anchor = features.profile_anchors((*profile)?).last().unwrap();
            let exact = features.documents.get(seed_anchor.metadata_id.0)
                == features.documents.get(candidate_anchor.metadata_id.0);
            let matched = exact
                || index
                    .similarity(seed_anchor.metadata_id, candidate_anchor.metadata_id, 0.6)
                    .0;
            matched.then_some(ContractId(contract as u32))
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(indexed, exhaustive);
    assert!(indexed.contains(&ContractId(1)));
    assert!(indexed.contains(&ContractId(3)));
}

#[test]
fn randomized_name_and_metadata_indexes_cover_exhaustive_matches() {
    let mut builder = ResidentBuilder::default();
    let mut state = 0xa076_1d64_78bd_642f_u64;
    let vocabulary = [
        "alpha", "beta", "gamma", "delta", "epsilon", "pixel", "dragon", "forest", "future",
        "orbit", "neon", "stone",
    ];
    let chains = [
        ChainId::Ethereum,
        ChainId::Polygon,
        ChainId::Base,
        ChainId::Solana,
    ];
    for index in 0..256_u64 {
        let chain = chains[index as usize % chains.len()];
        let words = generated_words(&vocabulary, &mut state, 3 + index as usize % 6);
        let name = if index % 17 == 0 {
            "shared alpha collection".to_owned()
        } else {
            words.join(" ")
        };
        let metadata = if index % 19 == 0 {
            r#"{"description":"shared alpha dragon forest","name":"shared"}"#.to_owned()
        } else {
            format!(
                r#"{{"description":"{}","name":"item {}"}}"#,
                words.join(" "),
                index % 13
            )
        };
        builder
            .push(row(
                (chain, &format!("contract-{index:03}"), &index.to_string()),
                (
                    &name,
                    &format!("ipfs://token-{index}"),
                    &format!("ipfs://image-{index}"),
                    &metadata,
                ),
                index,
            ))
            .unwrap();
    }
    let store = builder.finish(8, 128).unwrap();
    let name_features = store.name_features.as_ref().unwrap();
    let metadata_features = store.metadata_features.as_ref().unwrap();
    let seed_contracts = [0_usize, 17, 38, 73, 119, 191, 255]
        .into_iter()
        .map(|ordinal| {
            store
                .contracts
                .find(&analysis::model::ContractKey::new(
                    chains[ordinal % chains.len()],
                    format!("contract-{ordinal:03}"),
                ))
                .unwrap()
        })
        .collect::<Vec<_>>();
    let seed_names = seed_contracts
        .iter()
        .filter_map(|contract| name_features.contract_names[contract.index()])
        .collect::<Vec<_>>();
    let seed_documents = seed_contracts
        .iter()
        .filter_map(|contract| metadata_features.contract_profiles[contract.index()])
        .flat_map(|profile| metadata_features.profile_anchors(profile))
        .map(|anchor| anchor.metadata_id)
        .collect::<Vec<_>>();
    let name_index = NameIndex::build(name_features, &seed_names, 128);
    let metadata_index = MetadataIndex::build(metadata_features, &seed_documents, 128);

    for (seed_ordinal, &contract_id) in seed_contracts.iter().enumerate() {
        let seed = seed_query_for_contract(&store, contract_id, SeedId(seed_ordinal as u16));
        let indexed_names = (0..128)
            .flat_map(|shard| {
                query_name_shard(&store, name_features, &name_index, &seed, shard, 0.98)
            })
            .map(|hit| hit.candidate_contract)
            .collect::<BTreeSet<_>>();
        let seed_name = seed.name_value.unwrap();
        let exhaustive_names = name_features
            .contract_names
            .iter()
            .enumerate()
            .filter_map(|(contract, candidate)| {
                let candidate = (*candidate)?;
                if contract == contract_id.index() {
                    return None;
                }
                let score = jaro_winkler::similarity(
                    name_features.values.get(seed_name.0).chars(),
                    name_features.values.get(candidate.0).chars(),
                );
                (score >= 0.98).then_some(ContractId(contract as u32))
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(
            indexed_names, exhaustive_names,
            "name index diverged for seed {seed_ordinal}"
        );

        let indexed_metadata = (0..128)
            .flat_map(|shard| {
                query_metadata_shard(
                    &store,
                    metadata_features,
                    &metadata_index,
                    &seed,
                    shard,
                    128,
                    0.6,
                )
            })
            .map(|hit| hit.candidate_contract)
            .collect::<BTreeSet<_>>();
        let seed_anchor = metadata_features
            .profile_anchors(seed.metadata_profile.unwrap())
            .last()
            .unwrap();
        let exhaustive_metadata = metadata_features
            .contract_profiles
            .iter()
            .enumerate()
            .filter_map(|(contract, profile)| {
                if contract == contract_id.index() {
                    return None;
                }
                let candidate_anchor = metadata_features
                    .profile_anchors((*profile)?)
                    .last()
                    .unwrap();
                let exact = metadata_features.documents.get(seed_anchor.metadata_id.0)
                    == metadata_features
                        .documents
                        .get(candidate_anchor.metadata_id.0);
                let similar = metadata_index
                    .similarity(seed_anchor.metadata_id, candidate_anchor.metadata_id, 0.6)
                    .0;
                (exact || similar).then_some(ContractId(contract as u32))
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(
            indexed_metadata, exhaustive_metadata,
            "metadata index diverged for seed {seed_ordinal}"
        );
    }
}

#[test]
fn metadata_exact_prefetch_is_emitted_before_frozen_relations() {
    let store = fixture();
    let seed_key = store.contracts.key(ContractId(0));
    let now = chrono::DateTime::parse_from_rfc3339("2026-07-20T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let manifest = SeedManifest {
        generated_at: now,
        seeds: vec![SeedDefinition {
            id: SeedId(0),
            chain: seed_key.chain,
            contract_address: seed_key.contract_address.to_string(),
            rank: 1,
            collection_name: "seed".to_owned(),
            stable_identifier: "seed".to_owned(),
            ranking_metric: "fixture".to_owned(),
            ranking_value: 1.0,
            ranking_window: "fixture".to_owned(),
            source: "fixture".to_owned(),
            collected_at: now,
        }],
    };
    let config = RunConfig {
        snapshot_files: vec![PathBuf::from("fixture.parquet")],
        seed_manifest: PathBuf::from("seed.json"),
        output_dir: PathBuf::from("result"),
        memory_limit: 464 * 1024 * 1024 * 1024,
        cpu_workers: 4,
        index_shards: 128,
        seed_batch_size: 1,
        seed_top: Default::default(),
        numa_mode: NumaMode::Auto,
        tokio_worker_threads: 1,
        cpu_queue_capacity: 4,
        network_queue_capacity: 4,
        analysis_queue_capacity: 4,
        compression_concurrency: 1,
        writer_threads: 1,
        writer_queue_bytes: 1024,
        next_dimension_overlap: false,
        provider_timeout_ms: 1000,
        candidate_timeout_ms: 10_000,
        api_keys: Default::default(),
        provider_endpoints: Default::default(),
        provider_concurrency: ProviderConcurrency {
            alchemy: 1,
            helius: 1,
            other: 1,
        },
        provider_page_limits: BTreeMap::new(),
        provider_retry_count: 0,
        name_threshold: 0.98,
        metadata_threshold: 0.6,
        metadata_anchor_count: 8,
        analysis_timestamp: now,
    };
    let executor = analysis::pipeline::CpuExecutor::new(4).unwrap();
    let progress = analysis::progress::Progress::default();
    let (sender, mut receiver) = tokio::sync::mpsc::channel(32);
    let dedup =
        analysis::pipeline::execute_dedup(store, &manifest, &config, &executor, &progress, sender)
            .unwrap();
    assert!(dedup.store.uri_identity.is_none());
    assert!(dedup.store.uri_features.is_none());
    assert!(dedup.store.name_features.is_none());
    assert!(dedup.store.metadata_features.is_none());
    let catalog_bytes = dedup.store.contracts.contracts.len() as u64
        * std::mem::size_of::<analysis::model::ContractRecord>() as u64
        + dedup
            .store
            .contracts
            .contracts
            .iter()
            .map(|contract| contract.address.len() as u64)
            .sum::<u64>();
    assert_eq!(dedup.store.logical_bytes(), catalog_bytes);
    let mut events = Vec::new();
    while let Ok(event) = receiver.try_recv() {
        events.push(event);
    }
    let first_prefetch = events
        .iter()
        .position(|event| match event {
            analysis::pipeline::CandidateRelationsEvent::Prefetch(relations) => {
                relations.iter().any(|relation| {
                    relation.dimensions & analysis::model::Dimension::Metadata.bit() != 0
                })
            }
            _ => false,
        })
        .expect("fixture must produce an exact metadata prefetch");
    let first_frozen = events
        .iter()
        .position(|event| {
            matches!(
                event,
                analysis::pipeline::CandidateRelationsEvent::Frozen(_)
            )
        })
        .expect("fixture must produce frozen relations");
    assert!(first_prefetch < first_frozen);
    let prefetched = events
        .iter()
        .filter_map(|event| match event {
            analysis::pipeline::CandidateRelationsEvent::Prefetch(relations) => {
                Some(relations.iter().map(|relation| relation.candidate_id))
            }
            _ => None,
        })
        .flatten()
        .collect::<BTreeSet<_>>();
    let frozen = events
        .iter()
        .filter_map(|event| match event {
            analysis::pipeline::CandidateRelationsEvent::Frozen(relations) => {
                Some(relations.iter().map(|relation| relation.candidate_id))
            }
            _ => None,
        })
        .flatten()
        .collect::<BTreeSet<_>>();
    assert!(prefetched.is_subset(&frozen));

    let mut overlapped = config.clone();
    overlapped.next_dimension_overlap = true;
    let (sender, mut receiver) = tokio::sync::mpsc::channel(32);
    analysis::pipeline::execute_dedup(
        fixture(),
        &manifest,
        &overlapped,
        &executor,
        &progress,
        sender,
    )
    .unwrap();
    let mut overlap_events = Vec::new();
    while let Ok(event) = receiver.try_recv() {
        overlap_events.push(event);
    }
    assert_eq!(
        event_fingerprints(&overlap_events),
        event_fingerprints(&events),
        "tail overlap must preserve externally visible relations and ordering"
    );
}

fn event_fingerprints(
    events: &[analysis::pipeline::CandidateRelationsEvent],
) -> Vec<(u8, Vec<u8>)> {
    let mut values = events
        .iter()
        .map(|event| match event {
            analysis::pipeline::CandidateRelationsEvent::Prefetch(relations) => {
                (0, serde_json::to_vec(relations).unwrap())
            }
            analysis::pipeline::CandidateRelationsEvent::Frozen(relations) => {
                (1, serde_json::to_vec(relations).unwrap())
            }
        })
        .collect::<Vec<_>>();
    values.sort();
    values
}

fn seed_query_for_contract(
    store: &analysis::resident::ResidentBaseStore,
    contract_id: ContractId,
    seed_id: SeedId,
) -> SeedRawQuery {
    let uri = store.uri_features.as_ref().unwrap();
    let identity = store.uri_identity.as_ref().unwrap();
    let name = store.name_features.as_ref().unwrap();
    let metadata = store.metadata_features.as_ref().unwrap();
    let start = identity.contract_offsets[contract_id.index()] as usize;
    let end = identity.contract_offsets[contract_id.index() + 1] as usize;
    let nft_ids = (start..end)
        .map(|index| analysis::model::NftId(index as u32))
        .collect::<Vec<_>>();
    let mut token_uri_evidence = nft_ids
        .iter()
        .filter_map(|&nft| {
            uri.features[nft.index()]
                .token_uri
                .map(|value| (value, store.nft_key(nft).unwrap()))
        })
        .collect::<Vec<_>>();
    token_uri_evidence.sort_unstable_by_key(|(value, _)| *value);
    token_uri_evidence.dedup_by_key(|(value, _)| *value);
    let mut image_uri_evidence = nft_ids
        .iter()
        .filter_map(|&nft| {
            uri.features[nft.index()]
                .image_uri
                .map(|value| (value, store.nft_key(nft).unwrap()))
        })
        .collect::<Vec<_>>();
    image_uri_evidence.sort_unstable_by_key(|(value, _)| *value);
    image_uri_evidence.dedup_by_key(|(value, _)| *value);
    let profile = metadata.contract_profiles[contract_id.index()];
    SeedRawQuery {
        seed_id,
        contract_id,
        name_value: name.contract_names[contract_id.index()],
        token_uri_values: token_uri_evidence.iter().map(|(value, _)| *value).collect(),
        image_uri_values: image_uri_evidence.iter().map(|(value, _)| *value).collect(),
        token_uri_evidence,
        image_uri_evidence,
        metadata_profile: profile,
        metadata_documents: profile
            .into_iter()
            .flat_map(|profile| metadata.profile_anchors(profile))
            .map(|anchor| anchor.metadata_id)
            .collect(),
    }
}

fn generated_words<'a>(vocabulary: &'a [&'a str], state: &mut u64, count: usize) -> Vec<&'a str> {
    (0..count)
        .map(|_| {
            *state = state
                .wrapping_mul(2_862_933_555_777_941_757)
                .wrapping_add(3_037_000_493);
            vocabulary[(*state >> 32) as usize % vocabulary.len()]
        })
        .collect()
}
