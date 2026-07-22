//! Focused coverage for the `next_dimension_overlap` path in
//! `pipeline::coordinator::execute_dedup`: while the URI tail (last 1/8 of
//! shard work) runs across the NUMA lanes, the Name index build runs
//! concurrently in the sibling `rayon::join` branch. This test asserts that
//! path actually engages (tail_start < uri_work) and that it produces the
//! exact same candidate relations, in the same order, as the fully serial
//! path, with no seed marked `incomplete` by the dimension-level
//! `ShardWorkTracker`s wired around the Name/URI folds.

use analysis::config::{NumaMode, ProviderConcurrency, RunConfig};
use analysis::model::{ChainId, InputRow, SeedId, SourceOrder};
use analysis::pipeline::{CandidateRelationsEvent, CpuExecutor};
use analysis::progress::Progress;
use analysis::resident::ResidentBuilder;
use analysis::seed::{SeedDefinition, SeedManifest};
use std::collections::BTreeMap;
use std::path::PathBuf;

fn test_executor() -> CpuExecutor {
    #[cfg(not(target_os = "linux"))]
    {
        use analysis::platform::WorkerPlacement;
        CpuExecutor::new_numa_bounded(
            4,
            16,
            &[
                WorkerPlacement {
                    cpu: 0,
                    numa_node: Some(0),
                },
                WorkerPlacement {
                    cpu: 2,
                    numa_node: Some(1),
                },
                WorkerPlacement {
                    cpu: 1,
                    numa_node: Some(0),
                },
                WorkerPlacement {
                    cpu: 3,
                    numa_node: Some(1),
                },
            ],
        )
        .unwrap()
    }
    #[cfg(target_os = "linux")]
    CpuExecutor::new(4).unwrap()
}

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

/// Small four-chain fixture with a seed (`seed-collection`) and three
/// candidates that duplicate it by name, token URI, and metadata
/// respectively, plus one unrelated contract so shard work is non-trivial.
fn fixture() -> analysis::resident::ResidentBaseStore {
    let mut builder = ResidentBuilder::default();
    for input in [
        row(
            (ChainId::Ethereum, "seed-collection", "1"),
            (
                "overlap seed collection",
                "ipfs://overlap-token",
                "ipfs://overlap-image",
                r#"{"name":"overlap","attributes":[{"trait_type":"class","value":"seed"}]}"#,
            ),
            0,
        ),
        row(
            (ChainId::Base, "name-copy", "1"),
            (
                "overlap seed collection",
                "ipfs://different-token-a",
                "ipfs://different-image-a",
                r#"{"name":"unrelated-a"}"#,
            ),
            1,
        ),
        row(
            (ChainId::Polygon, "uri-copy", "9"),
            (
                "totally different name",
                "ipfs://overlap-token",
                "ipfs://different-image-b",
                r#"{"name":"unrelated-b"}"#,
            ),
            2,
        ),
        row(
            (ChainId::Solana, "metadata-copy", "mint-1"),
            (
                "another different name",
                "ipfs://different-token-c",
                "ipfs://different-image-c",
                r#"{"name":"overlap","attributes":[{"trait_type":"class","value":"seed"}]}"#,
            ),
            3,
        ),
        row(
            (ChainId::Ethereum, "unrelated", "1"),
            (
                "no relation here",
                "ipfs://no-relation-token",
                "ipfs://no-relation-image",
                r#"{"name":"no-relation"}"#,
            ),
            4,
        ),
    ]
    .into_iter()
    {
        builder.push(input).unwrap();
    }
    builder.finish(8, 128).unwrap()
}

fn base_config(now: chrono::DateTime<chrono::Utc>) -> RunConfig {
    RunConfig {
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
    }
}

fn manifest(
    store: &analysis::resident::ResidentBaseStore,
    now: chrono::DateTime<chrono::Utc>,
) -> SeedManifest {
    let seed_key = store.contracts.key(analysis::model::ContractId(0));
    SeedManifest {
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
    }
}

fn drain(
    mut receiver: tokio::sync::mpsc::Receiver<CandidateRelationsEvent>,
) -> Vec<CandidateRelationsEvent> {
    let mut events = Vec::new();
    while let Ok(event) = receiver.try_recv() {
        events.push(event);
    }
    events
}

fn event_fingerprints(events: &[CandidateRelationsEvent]) -> Vec<(u8, Vec<u8>)> {
    let mut values = events
        .iter()
        .map(|event| match event {
            CandidateRelationsEvent::Prefetch(relations) => {
                (0, serde_json::to_vec(relations).unwrap())
            }
            CandidateRelationsEvent::Frozen(relations) => {
                (1, serde_json::to_vec(relations).unwrap())
            }
        })
        .collect::<Vec<_>>();
    values.sort();
    values
}

fn assert_no_relation_marked_incomplete(events: &[CandidateRelationsEvent]) {
    for event in events {
        let relations = match event {
            CandidateRelationsEvent::Prefetch(relations)
            | CandidateRelationsEvent::Frozen(relations) => relations,
        };
        assert!(
            relations.iter().all(|relation| !relation.incomplete),
            "a clean fixture run must never soft-fail a seed via the dimension shard trackers"
        );
    }
}

/// With 128 index shards and a single seed, `uri_work == 128` and the
/// overlap tail starts at `128 * 7 / 8 == 112`, so the overlap branch in
/// `execute_dedup` is guaranteed to run: the last 16 URI shard queries
/// execute concurrently with the NUMA-aware Name index build.
#[test]
fn overlap_path_engages_and_matches_serial_dimension_ordering() {
    let now = chrono::DateTime::parse_from_rfc3339("2026-07-20T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let executor = test_executor();
    let progress = Progress::default();

    let serial_manifest = manifest(&fixture(), now);
    let mut serial_config = base_config(now);
    serial_config.next_dimension_overlap = false;
    let uri_work = {
        // seeds.len() * index_shards, matching coordinator::execute_dedup.
        serial_manifest.seeds.len() * serial_config.index_shards
    };
    let tail_start = uri_work.saturating_mul(7) / 8;
    assert!(
        tail_start < uri_work,
        "fixture must be large enough for the overlap branch to engage \
         (tail_start={tail_start}, uri_work={uri_work})"
    );

    let (serial_tx, serial_rx) = tokio::sync::mpsc::channel(64);
    analysis::pipeline::execute_dedup(
        fixture(),
        &serial_manifest,
        &serial_config,
        &executor,
        &progress,
        serial_tx,
    )
    .unwrap();
    let serial_events = drain(serial_rx);
    assert!(
        !serial_events.is_empty(),
        "fixture must produce candidate relations"
    );
    assert_no_relation_marked_incomplete(&serial_events);

    let mut overlap_config = serial_config.clone();
    overlap_config.next_dimension_overlap = true;
    let (overlap_tx, overlap_rx) = tokio::sync::mpsc::channel(64);
    analysis::pipeline::execute_dedup(
        fixture(),
        &serial_manifest,
        &overlap_config,
        &executor,
        &progress,
        overlap_tx,
    )
    .unwrap();
    let overlap_events = drain(overlap_rx);
    assert_no_relation_marked_incomplete(&overlap_events);

    assert_eq!(
        event_fingerprints(&overlap_events),
        event_fingerprints(&serial_events),
        "overlapping the Name index build with the URI tail must not change \
         the externally observed candidate relations"
    );
}
