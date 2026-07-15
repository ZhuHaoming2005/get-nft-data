use metadata_engine::blocking::{compile_base_equivalent, AtomSketch, BlockingCompileConfig};
use metadata_engine::cascade::{score_pair, PairScoreDecision};
use metadata_engine::encode::{
    write_encode_artifacts, write_encode_artifacts_with_contracts,
    write_encode_artifacts_with_contracts_and_atoms, EncodeContractRow, EncodePayloadRow,
    EncodeSourceRow,
};
use metadata_engine::evidence::{
    evaluate_holdout, EvidenceGatePolicy, HoldoutEvidence, RescuePlan, SharedRescueSeed,
};
use metadata_engine::exact_islands::{
    open_pair_exact_evidence, open_shared_token_exact_evidence, plan_exact_evidence,
    plan_shared_token_evidence, run_pair_exact_island, run_pair_exact_island_with_progress,
    run_shared_token_exact_islands, ExactEvidenceBudget, ExactEvidenceCluster,
    SharedTokenWorkStratum,
};
use metadata_engine::format::commit_ready;
use metadata_engine::identity::checked_u32_identity;
use metadata_engine::index::ConservativeIndex;
use metadata_engine::progress::{ProgressPhase, TotalKind, WorkClass, WorkUnit};
use metadata_engine::reduce::{
    build_component_snapshot_chain, commit_component_snapshot_chain, open_component_snapshot_chain,
    recover_component_snapshots, reduce_components, reduce_components_with_progress,
    ComponentSnapshot, ComponentSnapshotIdentity, Edge, EdgeBudget, EdgeCollector, ForestRun,
    SnapshotCadence,
};
use metadata_engine::resource::{required_host_headroom, MemoryBroker, GIB, MATCH_HARD_TOP};
use metadata_engine::scheduler::{
    job_routing_pair_work, CoverageCertificate, JobShape, RecallPlan, SchedulerError,
    UniverseBudget, WorkCatalog,
};
use metadata_engine::snapshot::MetadataSnapshot;

fn snapshot_fixture() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    let d = tempfile::tempdir().unwrap();
    let f = d.path().join("e");
    let b = d.path().join("b");
    write_encode_artifacts(
        &f,
        &[
            EncodeSourceRow {
                contract_id: 0,
                payload_id: 0,
                retained_token_ids: vec![],
            },
            EncodeSourceRow {
                contract_id: 1,
                payload_id: 1,
                retained_token_ids: vec![],
            },
        ],
        &[
            EncodePayloadRow {
                template_terms: vec![(1, 1)],
                content_terms: vec![(2, 2)],
            },
            EncodePayloadRow {
                template_terms: vec![(1, 1)],
                content_terms: vec![(2, 2)],
            },
        ],
    )
    .unwrap();
    compile_base_equivalent(
        &[
            AtomSketch {
                template_simhash: 0,
                content_simhash: 0,
                template_anchors: vec![10],
                content_anchors: vec![20],
                has_template_terms: true,
                has_content_terms: true,
            },
            AtomSketch {
                template_simhash: u64::MAX,
                content_simhash: u64::MAX,
                template_anchors: vec![11],
                content_anchors: vec![21],
                has_template_terms: true,
                has_content_terms: true,
            },
        ],
        &BlockingCompileConfig {
            max_routing_block_members: 10,
        },
        &b,
    )
    .unwrap();
    commit_ready(
        &f,
        "features.ready",
        r#"{"schema_revision":3,"source_count":2,"payload_count":2,"chains":["x"],"chain_totals":[{"name":"x","contracts":2,"nfts":2}]}"#,
    )
    .unwrap();
    commit_ready(
        &b,
        "blocking.ready",
        r#"{"blocking_revision":3,"atom_count":2}"#,
    )
    .unwrap();
    (d, f, b)
}

fn expanded_atom_snapshot_fixture() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    let d = tempfile::tempdir().unwrap();
    let f = d.path().join("e");
    let b = d.path().join("b");
    let sources = (0..4)
        .map(|contract_id| EncodeSourceRow {
            contract_id,
            payload_id: contract_id / 2,
            retained_token_ids: vec![],
        })
        .collect::<Vec<_>>();
    let contracts = (0..4)
        .map(|contract_id| EncodeContractRow {
            contract_id,
            chain_id: 0,
            source_doc_id: contract_id,
            payload_id: contract_id / 2,
            weight: 1,
        })
        .collect::<Vec<_>>();
    let payloads = vec![
        EncodePayloadRow {
            template_terms: vec![(1, 1)],
            content_terms: vec![(2, 1)],
        },
        EncodePayloadRow {
            template_terms: vec![(1, 1)],
            content_terms: vec![(2, 1)],
        },
    ];
    write_encode_artifacts_with_contracts_and_atoms(
        &f,
        &sources,
        &payloads,
        &contracts,
        &[vec![0, 1], vec![2, 3]],
    )
    .unwrap();
    let identical = AtomSketch {
        template_simhash: 0,
        content_simhash: 0,
        template_anchors: vec![10],
        content_anchors: vec![20],
        has_template_terms: true,
        has_content_terms: true,
    };
    compile_base_equivalent(
        &[identical.clone(), identical],
        &BlockingCompileConfig {
            max_routing_block_members: 10,
        },
        &b,
    )
    .unwrap();
    commit_ready(
        &f,
        "features.ready",
        r#"{"schema_revision":3,"source_count":4,"payload_count":2,"chains":["x"],"chain_totals":[{"name":"x","contracts":4,"nfts":4}]}"#,
    )
    .unwrap();
    commit_ready(
        &b,
        "blocking.ready",
        r#"{"blocking_revision":3,"atom_count":2}"#,
    )
    .unwrap();
    (d, f, b)
}

fn hot_pruning_snapshot_fixture() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    let directory = tempfile::tempdir().unwrap();
    let features = directory.path().join("features");
    let blocking = directory.path().join("blocking");
    let sources = (0..4)
        .map(|contract_id| EncodeSourceRow {
            contract_id,
            payload_id: contract_id,
            retained_token_ids: vec![],
        })
        .collect::<Vec<_>>();
    let payloads = vec![
        EncodePayloadRow {
            template_terms: vec![(1, 1)],
            content_terms: vec![(10, 1)],
        },
        EncodePayloadRow {
            template_terms: vec![(1, 1)],
            content_terms: vec![(10, 1)],
        },
        EncodePayloadRow {
            template_terms: vec![(1, 1)],
            content_terms: vec![(20, 1)],
        },
        EncodePayloadRow {
            template_terms: vec![(2, 1)],
            content_terms: vec![(10, 1)],
        },
    ];
    write_encode_artifacts(&features, &sources, &payloads).unwrap();
    let identical = AtomSketch {
        template_simhash: 0,
        content_simhash: 0,
        template_anchors: vec![100],
        content_anchors: vec![200],
        has_template_terms: true,
        has_content_terms: true,
    };
    compile_base_equivalent(
        &vec![identical; 4],
        &BlockingCompileConfig {
            max_routing_block_members: 1,
        },
        &blocking,
    )
    .unwrap();
    commit_ready(
        &features,
        "features.ready",
        r#"{"schema_revision":3,"source_count":4,"payload_count":4,"chains":["x"],"chain_totals":[{"name":"x","contracts":4,"nfts":4}]}"#,
    )
    .unwrap();
    commit_ready(
        &blocking,
        "blocking.ready",
        r#"{"blocking_revision":3,"atom_count":4}"#,
    )
    .unwrap();
    (directory, features, blocking)
}

#[test]
fn exact_island_scans_full_universe_and_finds_out_of_block_match() {
    let (_d, f, b) = snapshot_fixture();
    let s = MetadataSnapshot::open(&f, &b).unwrap();
    let e = run_pair_exact_island(
        &s,
        &[0],
        ExactEvidenceBudget {
            max_lefts: 1,
            max_pair_work: 2,
            max_artifact_bytes: 1_000_000,
            max_lanes: 1,
        },
        None,
    )
    .unwrap();
    assert_eq!(e.pair_work, 1);
    assert_eq!(e.conservative_misses.len(), 1);
    assert_eq!(
        (
            e.conservative_misses[0].left_atom,
            e.conservative_misses[0].right_atom
        ),
        (0, 1)
    );
}

#[test]
fn exact_island_reports_monotonic_unordered_pair_work() {
    let (_d, f, b) = snapshot_fixture();
    let snapshot = MetadataSnapshot::open(&f, &b).unwrap();
    let mut events = Vec::new();
    run_pair_exact_island_with_progress(
        &snapshot,
        &[0, 1],
        ExactEvidenceBudget {
            max_lefts: 2,
            max_pair_work: 2,
            max_artifact_bytes: 1_000_000,
            max_lanes: 1,
        },
        None,
        |event| events.push(event),
    )
    .unwrap();
    let scan_positions = events
        .iter()
        .filter(|event| event.phase == ProgressPhase::PairExactIsland)
        .map(|event| event.completed)
        .collect::<Vec<_>>();
    assert_eq!(scan_positions.first(), Some(&0));
    assert_eq!(scan_positions.last(), Some(&1));
    assert!(scan_positions.windows(2).all(|pair| pair[0] <= pair[1]));
    assert!(events.iter().all(|event| {
        matches!(
            event.phase,
            ProgressPhase::PairExactIsland | ProgressPhase::PairExactFinalize
        )
    }));
    let scan_terminal = events
        .iter()
        .rfind(|event| event.phase == ProgressPhase::PairExactIsland)
        .unwrap();
    assert_eq!(scan_terminal.total, Some(1));
    assert_eq!(scan_terminal.unit, WorkUnit::Pairs);
    let finalize = events.last().unwrap();
    assert_eq!(finalize.phase, ProgressPhase::PairExactFinalize);
    assert_eq!(finalize.completed, finalize.total.unwrap());
}

#[test]
fn catalog_recall_plan_and_coverage_are_deterministic_and_fail_closed() {
    let (_d, f, b) = snapshot_fixture();
    let s = MetadataSnapshot::open(&f, &b).unwrap();
    let budget = UniverseBudget {
        max_jobs: 100,
        max_catalog_bytes: 100_000,
        cold_members_per_job: 16,
    };
    let c = WorkCatalog::build(&s, budget, 8).unwrap();
    let p = RecallPlan::freeze(&c, vec![1, 0, 1], vec![0]);
    assert!(p.frozen);
    assert_eq!(p.sampled_lefts, vec![0, 1]);
    let cert = CoverageCertificate::issue(&c, 0, &[0, 1]);
    assert!(cert.validate(&c, &[0, 1]).is_ok());
    assert!(cert.validate(&c, &[1]).is_err());
    let index = ConservativeIndex::open(&s);
    let mut direct = Vec::new();
    index.for_each_candidate(|a, b| direct.push((a, b)));
    let mut scheduled = Vec::new();
    index.for_each_catalog_candidate(&c, &p, |a, b| scheduled.push((a, b)));
    direct.sort_unstable();
    scheduled.sort_unstable();
    assert_eq!(direct, scheduled);
}

#[test]
fn catalog_estimated_work_is_a_contract_expanded_upper_bound() {
    let (_d, f, b) = snapshot_fixture();
    let snapshot = MetadataSnapshot::open(&f, &b).unwrap();
    let catalog = WorkCatalog::build(
        &snapshot,
        UniverseBudget {
            max_jobs: 100,
            max_catalog_bytes: 100_000,
            cold_members_per_job: u64::MAX,
        },
        u64::MAX,
    )
    .unwrap();
    assert_eq!(catalog.jobs.len(), 1, "fixture must form one MicroBatch");
    let plan = RecallPlan::freeze(&catalog, Vec::new(), Vec::new());
    let actual = ConservativeIndex::open(&snapshot)
        .for_each_catalog_candidate(&catalog, &plan, |_, _| {})
        .contract_pair_visits;
    assert!(catalog.jobs[0].estimated_work >= actual);
}

#[test]
fn catalog_exposes_one_checked_total_for_budget_metrics_and_progress() {
    let (_d, f, b) = snapshot_fixture();
    let snapshot = MetadataSnapshot::open(&f, &b).unwrap();
    let catalog = WorkCatalog::build(
        &snapshot,
        UniverseBudget {
            max_jobs: 10_000,
            max_catalog_bytes: 1_000_000,
            cold_members_per_job: 1,
        },
        u64::MAX,
    )
    .unwrap();
    let summed = catalog
        .jobs
        .iter()
        .map(|job| job.estimated_work)
        .sum::<u64>();
    assert_eq!(catalog.estimated_work().unwrap(), summed);
}

#[test]
fn production_scale_hot_blocks_use_one_lazy_catalog_descriptor_each() {
    const ATOMS: u32 = 1_025;
    let directory = tempfile::tempdir().unwrap();
    let features = directory.path().join("features");
    let blocking = directory.path().join("blocking");
    let sources = (0..ATOMS)
        .map(|contract_id| EncodeSourceRow {
            contract_id,
            payload_id: 0,
            retained_token_ids: vec![],
        })
        .collect::<Vec<_>>();
    write_encode_artifacts(
        &features,
        &sources,
        &[EncodePayloadRow {
            template_terms: vec![(1, 1)],
            content_terms: vec![(2, 1)],
        }],
    )
    .unwrap();
    let identical = AtomSketch {
        template_simhash: 0,
        content_simhash: 0,
        template_anchors: vec![10],
        content_anchors: vec![20],
        has_template_terms: true,
        has_content_terms: true,
    };
    compile_base_equivalent(
        &vec![identical; ATOMS as usize],
        &BlockingCompileConfig {
            max_routing_block_members: 1_024,
        },
        &blocking,
    )
    .unwrap();
    commit_ready(
        &features,
        "features.ready",
        &format!(
            r#"{{"schema_revision":3,"source_count":{ATOMS},"payload_count":1,"chains":["x"],"chain_totals":[{{"name":"x","contracts":{ATOMS},"nfts":{ATOMS}}}]}}"#
        ),
    )
    .unwrap();
    commit_ready(
        &blocking,
        "blocking.ready",
        &format!(r#"{{"blocking_revision":3,"atom_count":{ATOMS}}}"#),
    )
    .unwrap();
    let snapshot = MetadataSnapshot::open(&features, &blocking).unwrap();
    let hot_blocks = snapshot
        .blocking()
        .block_atom_offsets
        .windows(2)
        .filter(|offsets| offsets[1] - offsets[0] > 1_024)
        .count();
    assert!(hot_blocks > 0);

    let catalog = WorkCatalog::build(
        &snapshot,
        UniverseBudget {
            max_jobs: 1_000,
            max_catalog_bytes: 1_000_000,
            cold_members_per_job: u64::MAX,
        },
        1_024,
    )
    .unwrap();
    let hot_jobs = catalog
        .jobs
        .iter()
        .filter(|job| job.shape == JobShape::LeftTileFanout)
        .collect::<Vec<_>>();

    assert_eq!(hot_jobs.len(), hot_blocks);
    assert!(hot_jobs
        .iter()
        .all(|job| job.block_count == 1 && job.tile_row == 0 && job.tile_col == 0));

    let routing_total = catalog.jobs.iter().try_fold(0u64, |total, job| {
        total
            .checked_add(job_routing_pair_work(&snapshot, job).unwrap())
            .ok_or(())
    });
    let expected_routing_total = snapshot
        .blocking()
        .block_atom_offsets
        .windows(2)
        .map(|offsets| {
            let members = offsets[1] - offsets[0];
            members * members.saturating_sub(1) / 2
        })
        .sum::<u64>();
    assert_eq!(routing_total.unwrap(), expected_routing_total);

    let error = WorkCatalog::build(
        &snapshot,
        UniverseBudget {
            max_jobs: 0,
            max_catalog_bytes: 0,
            cold_members_per_job: u64::MAX,
        },
        1_024,
    )
    .unwrap_err();
    assert!(matches!(error, SchedulerError::Budget { jobs: 1, .. }));
}

#[test]
fn hot_block_proof_index_matches_exhaustive_exact_scoring() {
    let (_directory, features, blocking) = hot_pruning_snapshot_fixture();
    let snapshot = MetadataSnapshot::open(&features, &blocking).unwrap();
    let catalog = WorkCatalog::build(
        &snapshot,
        UniverseBudget {
            max_jobs: 1_000,
            max_catalog_bytes: 1_000_000,
            cold_members_per_job: 16,
        },
        1,
    )
    .unwrap();
    assert!(catalog
        .jobs
        .iter()
        .any(|job| job.shape == JobShape::LeftTileFanout));
    let plan = RecallPlan::freeze(&catalog, Vec::new(), Vec::new());
    let index = ConservativeIndex::open(&snapshot);

    let mut exhaustive_matches = Vec::new();
    index.for_each_candidate(|left, right| {
        let left_payload = snapshot.features().contract_payload[left as usize];
        let right_payload = snapshot.features().contract_payload[right as usize];
        if score_pair(snapshot.features(), left_payload, right_payload)
            == PairScoreDecision::ExactMatch
        {
            exhaustive_matches.push((left, right));
        }
    });
    exhaustive_matches.sort_unstable();
    exhaustive_matches.dedup();

    let mut scheduled_candidates = Vec::new();
    let metrics = index.for_each_catalog_candidate(&catalog, &plan, |left, right| {
        scheduled_candidates.push((left, right));
    });
    let mut scheduled_matches = scheduled_candidates
        .iter()
        .copied()
        .filter(|&(left, right)| {
            let left_payload = snapshot.features().contract_payload[left as usize];
            let right_payload = snapshot.features().contract_payload[right as usize];
            score_pair(snapshot.features(), left_payload, right_payload)
                == PairScoreDecision::ExactMatch
        })
        .collect::<Vec<_>>();
    scheduled_matches.sort_unstable();
    scheduled_matches.dedup();

    assert_eq!(scheduled_matches, exhaustive_matches);
    assert_eq!(scheduled_matches, vec![(0, 1)]);
    assert!(scheduled_candidates.len() < metrics.block_pair_visits as usize);
}

#[test]
fn work_catalog_reopens_only_for_the_same_snapshot() {
    let (dir, features, blocking) = snapshot_fixture();
    let snapshot = MetadataSnapshot::open(&features, &blocking).unwrap();
    let catalog = WorkCatalog::build(
        &snapshot,
        UniverseBudget {
            max_jobs: 100,
            max_catalog_bytes: 100_000,
            cold_members_per_job: 10,
        },
        10,
    )
    .unwrap();
    let out = dir.path().join("catalog-recovery");
    catalog.commit(&out).unwrap();
    assert_eq!(WorkCatalog::open(&out, &snapshot).unwrap(), catalog);
}

#[test]
fn stale_work_catalog_is_retired_and_rebuilt_for_the_current_snapshot() {
    let (dir, features, blocking) = snapshot_fixture();
    let snapshot = MetadataSnapshot::open(&features, &blocking).unwrap();
    let out = dir.path().join("catalog-stale-recovery");
    let budget = UniverseBudget {
        max_jobs: 100,
        max_catalog_bytes: 100_000,
        cold_members_per_job: 10,
    };
    let catalog = WorkCatalog::build(&snapshot, budget, 10).unwrap();
    catalog.commit(&out).unwrap();
    let ready = out.join("catalog.ready");
    let mut json: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&ready).unwrap()).unwrap();
    json["snapshot_fingerprint"] = "stale-snapshot".into();
    std::fs::write(&ready, serde_json::to_vec(&json).unwrap()).unwrap();

    let rebuilt =
        WorkCatalog::open_or_rebuild_with_progress(&out, &snapshot, budget, 10, |_, _, _| {})
            .unwrap();

    assert_eq!(WorkCatalog::open(&out, &snapshot).unwrap(), rebuilt);
}

#[test]
fn catalog_progress_advances_by_completed_jobs_without_using_candidate_hits() {
    let (_d, f, b) = snapshot_fixture();
    let identical = AtomSketch {
        template_simhash: 0,
        content_simhash: 0,
        template_anchors: vec![10],
        content_anchors: vec![20],
        has_template_terms: true,
        has_content_terms: true,
    };
    compile_base_equivalent(
        &[identical.clone(), identical],
        &BlockingCompileConfig {
            max_routing_block_members: 10,
        },
        &b,
    )
    .unwrap();
    commit_ready(
        &b,
        "blocking.ready",
        r#"{"blocking_revision":3,"atom_count":2}"#,
    )
    .unwrap();
    let snapshot = MetadataSnapshot::open(&f, &b).unwrap();
    let catalog = WorkCatalog::build(
        &snapshot,
        UniverseBudget {
            max_jobs: 10_000,
            max_catalog_bytes: 1_000_000,
            cold_members_per_job: 2,
        },
        u64::MAX,
    )
    .unwrap();
    let plan = RecallPlan::freeze(&catalog, Vec::new(), Vec::new());
    let mut events = Vec::new();
    let mut candidates = 0u64;
    let metrics = ConservativeIndex::open(&snapshot)
        .for_each_catalog_candidate_with_progress(
            &catalog,
            &plan,
            |_, _| candidates += 1,
            |event| events.push(event),
        )
        .unwrap();
    assert_eq!(metrics.block_pair_visits, catalog.estimated_work().unwrap());
    assert_eq!(events.len(), catalog.jobs.len() + 1);
    assert_eq!(events.first().unwrap().completed, 0);
    let terminal = events.last().unwrap();
    assert_eq!(terminal.phase, ProgressPhase::CatalogPairs);
    assert_eq!(terminal.unit, WorkUnit::Pairs);
    assert_eq!(terminal.completed, metrics.block_pair_visits);
    assert_eq!(terminal.total, Some(metrics.block_pair_visits));
    assert_eq!(terminal.work_class, WorkClass::CatalogRoutes);
    assert!(terminal.counters.candidates <= terminal.completed);
    assert!(candidates <= terminal.completed);
}

#[test]
fn catalog_budget_is_contract_expanded_while_progress_counts_routing_pairs() {
    let (_d, f, b) = expanded_atom_snapshot_fixture();
    let snapshot = MetadataSnapshot::open(&f, &b).unwrap();
    let catalog = WorkCatalog::build(
        &snapshot,
        UniverseBudget {
            max_jobs: 100,
            max_catalog_bytes: 100_000,
            cold_members_per_job: 100,
        },
        100,
    )
    .unwrap();
    let plan = RecallPlan::freeze(&catalog, Vec::new(), Vec::new());
    let mut events = Vec::new();
    let metrics = ConservativeIndex::open(&snapshot)
        .for_each_catalog_candidate_with_progress(
            &catalog,
            &plan,
            |_, _| {},
            |event| events.push(event),
        )
        .unwrap();

    assert_eq!(metrics.routed_pairs, 1);
    assert!(metrics.block_pair_visits > metrics.routed_pairs);
    assert_eq!(metrics.contract_pair_visits, 4);
    assert!(catalog.estimated_work().unwrap() >= metrics.contract_pair_visits);
    let terminal = events.last().unwrap();
    assert_eq!(terminal.total, Some(metrics.block_pair_visits));
    assert_eq!(terminal.work_class, WorkClass::CatalogRoutes);
    assert_eq!(terminal.completed, terminal.total.unwrap());
}

#[test]
fn catalog_job_traversal_honors_global_cancellation_predicate() {
    let (_dir, features, blocking) = expanded_atom_snapshot_fixture();
    let snapshot = MetadataSnapshot::open(&features, &blocking).unwrap();
    let catalog = WorkCatalog::build(
        &snapshot,
        UniverseBudget {
            max_jobs: 100,
            max_catalog_bytes: 100_000,
            cold_members_per_job: 100,
        },
        100,
    )
    .unwrap();
    let mut checks = 0u64;
    let mut reported = 0u64;

    let metrics = ConservativeIndex::open(&snapshot).for_each_job_candidate_with_work_while(
        &catalog.jobs[0],
        |_, _| {},
        |work| reported = reported.saturating_add(work),
        || {
            checks = checks.saturating_add(1);
            checks <= 2
        },
    );

    assert_eq!(metrics.block_pair_visits, 2);
    assert_eq!(reported, 2);
}

#[test]
fn forest_runs_preserve_components_and_minimum_roots() {
    let b = EdgeBudget {
        max_buffer_bytes: 1024,
        max_run_edges: 100,
        max_total_bytes: 1024,
    };
    let raw = vec![
        Edge::new(2, 3),
        Edge::new(0, 1),
        Edge::new(1, 2),
        Edge::new(0, 3),
        Edge::new(1, 2),
    ];
    let run = ForestRun::from_edges(5, raw, b).unwrap();
    assert_eq!(run.edges.len(), 3);
    assert_eq!(reduce_components(&[run], 5).unwrap(), vec![0, 0, 0, 0, 4]);
}

#[test]
fn component_snapshot_recovery_rejects_a_broken_delta_chain() {
    let full = ComponentSnapshot {
        revision: 1,
        epoch: 3,
        base_epoch: None,
        roots: vec![0, 0],
    };
    let good = ComponentSnapshot {
        revision: 1,
        epoch: 4,
        base_epoch: Some(3),
        roots: vec![0, 0],
    };
    assert_eq!(
        recover_component_snapshots(&[full.clone(), good])
            .unwrap()
            .epoch,
        4
    );
    let broken = ComponentSnapshot {
        revision: 1,
        epoch: 5,
        base_epoch: Some(1),
        roots: vec![0, 0],
    };
    assert!(recover_component_snapshots(&[full, broken]).is_err());
}

#[test]
fn component_snapshot_cadence_is_multi_epoch_and_restarts_from_latest_full() {
    let budget = EdgeBudget {
        max_buffer_bytes: 1_000_000,
        max_run_edges: 100,
        max_total_bytes: 1_000_000,
    };
    let runs = (0..5)
        .map(|left| ForestRun::from_edges(6, [Edge::new(left, left + 1)], budget).unwrap())
        .collect::<Vec<_>>();
    let snapshots = build_component_snapshot_chain(
        &runs,
        6,
        SnapshotCadence {
            max_epoch_edges: 1,
            full_every_epochs: 3,
            max_replay_epochs: 2,
            max_replay_bytes: 48,
        },
    )
    .unwrap();

    assert_eq!(
        snapshots.len(),
        2,
        "superseded full/delta history is retired"
    );
    assert_eq!(
        snapshots
            .iter()
            .map(|snapshot| snapshot.epoch)
            .collect::<Vec<_>>(),
        vec![3, 4]
    );
    assert_eq!(
        snapshots
            .iter()
            .map(|snapshot| snapshot.base_epoch)
            .collect::<Vec<_>>(),
        vec![None, Some(3)]
    );
    assert_eq!(
        recover_component_snapshots(&snapshots).unwrap().roots,
        vec![0; 6]
    );
}

#[test]
fn component_snapshot_cadence_rejects_one_oversized_run() {
    let budget = EdgeBudget {
        max_buffer_bytes: 1_000_000,
        max_run_edges: 100,
        max_total_bytes: 1_000_000,
    };
    let run = ForestRun::from_edges(4, [Edge::new(0, 1), Edge::new(2, 3)], budget).unwrap();
    let error = build_component_snapshot_chain(
        &[run],
        4,
        SnapshotCadence {
            max_epoch_edges: 1,
            full_every_epochs: 8,
            max_replay_epochs: 8,
            max_replay_bytes: 1_000_000,
        },
    )
    .unwrap_err();
    assert!(error.to_string().contains("run has 2 edges"), "{error}");
}

#[test]
fn forest_and_component_snapshots_reopen_from_checksummed_artifacts() {
    let dir = tempfile::tempdir().unwrap();
    let budget = EdgeBudget {
        max_buffer_bytes: 1024,
        max_run_edges: 10,
        max_total_bytes: 1024,
    };
    let run = ForestRun::from_edges(3, [Edge::new(0, 1), Edge::new(1, 2)], budget).unwrap();
    run.commit(dir.path(), 7).unwrap();
    assert_eq!(ForestRun::open(dir.path(), 7).unwrap().edges, run.edges);
    let snapshot = ComponentSnapshot::full(4, std::slice::from_ref(&run), 3).unwrap();
    snapshot.commit(dir.path()).unwrap();
    assert_eq!(ComponentSnapshot::open(dir.path(), 4).unwrap(), snapshot);
}

#[test]
fn component_snapshot_chain_reopens_only_for_the_bound_pipeline_scope() {
    let dir = tempfile::tempdir().unwrap();
    let budget = EdgeBudget {
        max_buffer_bytes: 1024,
        max_run_edges: 10,
        max_total_bytes: 1024,
    };
    let runs = vec![
        ForestRun::from_edges(3, [Edge::new(0, 1)], budget).unwrap(),
        ForestRun::from_edges(3, [Edge::new(1, 2)], budget).unwrap(),
    ];
    let snapshots = build_component_snapshot_chain(
        &runs,
        3,
        SnapshotCadence {
            max_epoch_edges: 1,
            full_every_epochs: 8,
            max_replay_epochs: 8,
            max_replay_bytes: 1024,
        },
    )
    .unwrap();
    let identity = ComponentSnapshotIdentity {
        schema_revision: 7,
        snapshot_fingerprint: "snapshot-a".into(),
        connectivity_revision: 2,
        connectivity_plan_digest: "plan-a".into(),
        scope_identity: "intra".into(),
        node_count: 3,
    };
    commit_component_snapshot_chain(dir.path(), &identity, &snapshots, || {}).unwrap();

    let reopened = open_component_snapshot_chain(dir.path(), &identity)
        .unwrap()
        .expect("same identity must reuse the complete chain");
    assert_eq!(reopened, snapshots);

    for stale in [
        ComponentSnapshotIdentity {
            snapshot_fingerprint: "snapshot-b".into(),
            ..identity.clone()
        },
        ComponentSnapshotIdentity {
            connectivity_plan_digest: "plan-b".into(),
            ..identity.clone()
        },
        ComponentSnapshotIdentity {
            scope_identity: "cross".into(),
            ..identity.clone()
        },
    ] {
        assert!(
            open_component_snapshot_chain(dir.path(), &stale)
                .unwrap()
                .is_none(),
            "stale identity must be rebuilt, never reused"
        );
    }
}

#[test]
fn component_snapshot_chain_with_matching_identity_fails_closed_when_corrupt() {
    let dir = tempfile::tempdir().unwrap();
    let run = ForestRun::from_edges(
        2,
        [Edge::new(0, 1)],
        EdgeBudget {
            max_buffer_bytes: 1024,
            max_run_edges: 10,
            max_total_bytes: 1024,
        },
    )
    .unwrap();
    let snapshots = vec![ComponentSnapshot::full(0, &[run], 2).unwrap()];
    let identity = ComponentSnapshotIdentity {
        schema_revision: 7,
        snapshot_fingerprint: "snapshot-a".into(),
        connectivity_revision: 2,
        connectivity_plan_digest: "plan-a".into(),
        scope_identity: "intra".into(),
        node_count: 2,
    };
    commit_component_snapshot_chain(dir.path(), &identity, &snapshots, || {}).unwrap();
    std::fs::write(dir.path().join("component-roots-000000.u32"), b"corrupt").unwrap();

    let error = open_component_snapshot_chain(dir.path(), &identity).unwrap_err();
    assert!(error.to_string().contains("component snapshot"), "{error}");
}

#[test]
fn high_degree_edge_collector_flushes_forests_without_changing_components() {
    let budget = EdgeBudget {
        max_buffer_bytes: 1024,
        max_run_edges: 100,
        max_total_bytes: 1024,
    };
    let mut collector = EdgeCollector::new(20, budget, 4);
    for right in 1..20 {
        collector.push(Edge::new(0, right)).unwrap();
    }
    let runs = collector.finish().unwrap();
    assert!(runs.len() > 1);
    assert_eq!(reduce_components(&runs, 20).unwrap(), vec![0; 20]);
}

#[test]
fn edge_collector_reports_all_retained_buffer_and_forest_bytes() {
    let budget = EdgeBudget {
        max_buffer_bytes: 16,
        max_run_edges: 100,
        max_total_bytes: 1024,
    };
    let mut collector = EdgeCollector::new(8, budget, 100);
    collector.push(Edge::new(0, 1)).unwrap();
    assert_eq!(collector.retained_bytes(), 8);
    collector.push(Edge::new(2, 3)).unwrap();
    collector.push(Edge::new(4, 5)).unwrap();
    assert!(collector.retained_bytes() <= 24);
}

#[test]
fn edge_collector_recompacts_runs_before_rejecting_a_compressible_graph() {
    let budget = EdgeBudget {
        max_buffer_bytes: 64,
        max_run_edges: 100,
        max_total_bytes: 160,
    };
    let mut collector = EdgeCollector::new(20, budget, 4);
    for left in 0..20 {
        for right in left + 1..20 {
            collector.push(Edge::new(left, right)).unwrap();
        }
    }
    let runs = collector.finish().unwrap();
    assert_eq!(reduce_components(&runs, 20).unwrap(), vec![0; 20]);
    assert!(
        runs.iter().map(|run| run.edges.len()).sum::<usize>() <= 160 / std::mem::size_of::<Edge>()
    );
}

#[test]
fn component_reduce_reports_chunk_progress_inside_a_scope() {
    let run = ForestRun::from_edges(
        20_000,
        (1..20_000).map(|right| Edge::new(0, right)),
        EdgeBudget {
            max_buffer_bytes: 1_000_000,
            max_run_edges: 20_000,
            max_total_bytes: 1_000_000,
        },
    )
    .unwrap();
    let mut observed = Vec::new();
    let roots = reduce_components_with_progress(&[run], 20_000, |completed, total| {
        observed.push((completed, total));
    })
    .unwrap();

    assert_eq!(roots, vec![0; 20_000]);
    assert!(observed.len() > 2);
    let edge_work = 19_999u64;
    assert_eq!(observed[1].0, 16_384);
    assert_eq!(observed[1].1, edge_work + 20_000);
    assert!(observed.windows(2).all(|pair| pair[0].0 <= pair[1].0));
    assert_eq!(observed.last().unwrap().0, observed.last().unwrap().1);
}

#[test]
fn component_snapshot_can_reuse_already_reduced_roots() {
    let roots = vec![0, 0, 2, 2];
    let snapshot = ComponentSnapshot::from_reduced_roots(7, roots.clone()).unwrap();

    assert_eq!(snapshot.epoch, 7);
    assert_eq!(snapshot.base_epoch, None);
    assert_eq!(snapshot.roots, roots);
}

#[test]
fn memory_gate_fails_before_allocation() {
    let broker = MemoryBroker::new(512 * GIB, MATCH_HARD_TOP).unwrap();
    let lease = broker.reserve(128 * GIB).unwrap();
    assert!(broker.reserve(300 * GIB).is_err());
    drop(lease);
    assert!(broker.reserve(300 * GIB).is_ok());
}

#[test]
fn persisted_identity_cardinality_fails_closed_above_u32() {
    assert_eq!(
        checked_u32_identity("atoms", u32::MAX as u64).unwrap(),
        u32::MAX
    );
    let error = checked_u32_identity("atoms", u32::MAX as u64 + 1).unwrap_err();
    assert!(error.to_string().contains("atoms"));
    assert!(error.to_string().contains("u32"));
}

#[test]
fn pipeline_rejects_snapshot_memory_before_opening_any_mmap() {
    let (dir, features, blocking) = snapshot_fixture();
    let snapshot_bytes = MetadataSnapshot::verification_bytes(&features, &blocking).unwrap();
    let mut events = Vec::new();

    let error = metadata_engine::pipeline::run_metadata_pipeline_with_progress(
        &features,
        &blocking,
        &dir.path().join("snapshot-memory-rejected"),
        &metadata_engine::pipeline::MetadataPipelineConfig {
            storage_work_directory: dir.path().to_path_buf(),
            memory_hard_top: snapshot_bytes.saturating_sub(1),
            host_total_memory: 512 * GIB,
            threads: 1,
            max_catalog_jobs: 100,
            max_candidate_pair_visits: 1_000_000,
            exact_sample_lefts: 1,
            exact_pair_work: 10,
            evidence_gate_policy: EvidenceGatePolicy::permissive(),
            edge_bytes: 1_000_000,
        },
        |event| events.push(event),
    )
    .unwrap_err();

    assert!(
        error.to_string().contains("memory budget exceeded"),
        "{error}"
    );
    assert!(
        events.is_empty(),
        "snapshot admission must happen before mmap verification starts"
    );
}

#[test]
fn snapshot_lease_remains_charged_when_component_memory_is_admitted() {
    let (dir, features, blocking) = snapshot_fixture();
    let snapshot_bytes = MetadataSnapshot::verification_bytes(&features, &blocking).unwrap();
    let max_catalog_jobs = 100u64;
    let catalog_bytes =
        max_catalog_jobs * std::mem::size_of::<metadata_engine::scheduler::JobDescriptor>() as u64;
    let edge_bytes = 64 * 1024u64;
    let component_peak_bytes = 2 * 4 * 2 * 10u64;
    let hard_top = snapshot_bytes
        .checked_add(catalog_bytes)
        .and_then(|bytes| bytes.checked_add(edge_bytes))
        .and_then(|bytes| bytes.checked_add(component_peak_bytes - 1))
        .unwrap();

    let error = metadata_engine::pipeline::run_metadata_pipeline(
        &features,
        &blocking,
        &dir.path().join("cumulative-memory-rejected"),
        &metadata_engine::pipeline::MetadataPipelineConfig {
            storage_work_directory: dir.path().to_path_buf(),
            memory_hard_top: hard_top,
            host_total_memory: 512 * GIB,
            threads: 1,
            max_catalog_jobs,
            max_candidate_pair_visits: 1_000_000,
            exact_sample_lefts: 1,
            exact_pair_work: 10,
            evidence_gate_policy: EvidenceGatePolicy::permissive(),
            edge_bytes: 1_000_000,
        },
    )
    .unwrap_err();

    let expected_used = snapshot_bytes + catalog_bytes + edge_bytes;
    assert!(
        error.to_string().contains(&format!(
            "requested {component_peak_bytes}, used {expected_used}"
        )),
        "snapshot, catalog, and edge leases must be cumulative: {error}"
    );
}

#[test]
fn host_headroom_scales_down_on_non_target_machines() {
    assert_eq!(required_host_headroom(64 * GIB), 8 * GIB);
    assert_eq!(required_host_headroom(512 * GIB), 64 * GIB);
}

#[test]
fn exact_evidence_plan_scales_both_pair_partitions_to_the_joint_budget() {
    let plan = plan_exact_evidence(13_500_000, 1_024, 20_000_000_000).unwrap();

    assert!(plan.calibration_lefts > 0);
    assert_eq!(plan.calibration_lefts, plan.holdout_lefts);
    assert!(plan.pair_work <= 20_000_000_000);
    assert_eq!(
        plan.pair_work,
        (plan.calibration_lefts + plan.holdout_lefts) * 13_499_999
    );
}

#[test]
fn match_semantics_revision_tracks_clustered_evidence_and_admission_contracts() {
    assert_eq!(metadata_engine::scoring::MATCH_SEMANTICS_REVISION, 6);
}

#[test]
fn shared_token_evidence_skips_unaffordable_groups_before_execution() {
    let plan = plan_shared_token_evidence(
        &[0, 1_000_000, 1_000_002, 1_000_004, 1_000_006],
        &[0, 1, 2, 3],
        2,
        10,
    )
    .unwrap();

    assert_eq!(plan.skipped_tokens, vec![0]);
    assert_eq!(plan.calibration_tokens, vec![2]);
    assert_eq!(plan.holdout_tokens, vec![1, 3]);
    assert_eq!(plan.pair_work, 3);
    assert_eq!(plan.skipped_pair_work, 499_999_500_000);
    assert_eq!(plan.considered_pair_work, 499_999_500_003);
}

#[test]
fn shared_token_exhaustive_requires_every_active_token_identity() {
    let mut offsets = vec![0u64; 1_002];
    for value in offsets.iter_mut().skip(1) {
        *value = 1;
    }
    *offsets.last_mut().unwrap() = 3;
    let plan = metadata_engine::exact_islands::SharedTokenEvidencePlan {
        calibration_tokens: vec![0],
        holdout_tokens: vec![],
        skipped_tokens: vec![],
        pair_work: 0,
        skipped_pair_work: 0,
        considered_pair_work: 0,
        work_strata: vec![],
    };

    assert!(!plan.covers_all_active_groups(&offsets));
}

#[test]
fn evidence_gate_rejects_excessive_skipped_pair_work_separately_from_misses() {
    let report = evaluate_holdout(
        HoldoutEvidence {
            evaluated_pair_work: 15_000,
            exhaustive: false,
            pair_exact_matches: 10_000,
            pair_misses: &[],
            shared_exact_matches: 5_000,
            shared_misses: &[],
            skipped_shared_groups: &[7, 11],
            skipped_shared_pair_work: 5_000,
            considered_shared_pair_work: 20_000,
            shared_work_strata: &[],
            pair_clusters: &[ExactEvidenceCluster {
                id: 1,
                exact_matches: 10_000,
            }],
            shared_clusters: &[ExactEvidenceCluster {
                id: 2,
                exact_matches: 5_000,
            }],
        },
        &RescuePlan::default(),
        EvidenceGatePolicy {
            max_miss_rate: 0.01,
            confidence_z: 1.96,
            min_exact_matches: 30,
            max_skipped_pair_work_rate: 0.10,
        },
    )
    .unwrap();

    assert!(!report.passed);
    assert_eq!(report.observed_misses, 0);
    assert_eq!(report.skipped_shared_groups, vec![7, 11]);
    assert_eq!(report.skipped_pair_work_rate, 0.25);
}

#[test]
fn shared_evidence_plan_tracks_budget_skips_per_pair_work_stratum() {
    // Group sizes 2, 3, and 9 have pair work 1, 3, and 36 respectively.
    let plan = plan_shared_token_evidence(&[0, 2, 5, 14], &[0, 1, 2], 8, 4).unwrap();

    assert_eq!(plan.pair_work, 4);
    assert_eq!(plan.skipped_pair_work, 36);
    assert_eq!(plan.work_strata.len(), 3);
    assert_eq!(plan.work_strata[0].considered_pair_work, 1);
    assert_eq!(plan.work_strata[0].skipped_pair_work, 0);
    assert_eq!(plan.work_strata[1].considered_pair_work, 3);
    assert_eq!(plan.work_strata[1].skipped_pair_work, 0);
    assert_eq!(plan.work_strata[2].considered_pair_work, 36);
    assert_eq!(plan.work_strata[2].skipped_pair_work, 36);
}

#[test]
fn evidence_gate_rejects_a_skipped_work_stratum_hidden_by_the_global_rate() {
    let report = evaluate_holdout(
        HoldoutEvidence {
            evaluated_pair_work: 10_000,
            exhaustive: false,
            pair_exact_matches: 10_000,
            pair_misses: &[],
            shared_exact_matches: 0,
            shared_misses: &[],
            skipped_shared_groups: &[9],
            skipped_shared_pair_work: 1,
            considered_shared_pair_work: 10_000,
            shared_work_strata: &[
                SharedTokenWorkStratum {
                    log2_pair_work: 0,
                    considered_pair_work: 1,
                    skipped_pair_work: 1,
                },
                SharedTokenWorkStratum {
                    log2_pair_work: 12,
                    considered_pair_work: 9_999,
                    skipped_pair_work: 0,
                },
            ],
            pair_clusters: &[ExactEvidenceCluster {
                id: 1,
                exact_matches: 10_000,
            }],
            shared_clusters: &[],
        },
        &RescuePlan::default(),
        EvidenceGatePolicy {
            max_miss_rate: 1.0,
            confidence_z: 0.0,
            min_exact_matches: 0,
            max_skipped_pair_work_rate: 0.10,
        },
    )
    .unwrap();

    assert_eq!(report.skipped_pair_work_rate, 0.0001);
    assert_eq!(report.max_stratum_skipped_pair_work_rate, 1.0);
    assert!(!report.passed);
}

#[test]
fn calibration_rescue_is_deterministic_and_filters_independent_holdout() {
    let pair_miss = metadata_engine::exact_islands::ExactMiss {
        left_atom: 9,
        right_atom: 3,
    };
    let shared_miss = metadata_engine::exact_islands::SharedTokenExactMiss {
        token_id: 5,
        left_contract: 8,
        right_contract: 2,
    };
    let plan = RescuePlan::from_calibration(
        std::slice::from_ref(&pair_miss),
        std::slice::from_ref(&shared_miss),
    );
    assert_eq!(plan.pair_atoms, vec![3, 9]);
    assert_eq!(
        plan.shared_seeds,
        vec![
            SharedRescueSeed {
                token_id: 5,
                contract_id: 2,
            },
            SharedRescueSeed {
                token_id: 5,
                contract_id: 8,
            },
        ]
    );

    let report = evaluate_holdout(
        HoldoutEvidence {
            evaluated_pair_work: 2_000,
            exhaustive: false,
            pair_exact_matches: 1_000,
            pair_misses: &[pair_miss],
            shared_exact_matches: 1_000,
            shared_misses: &[shared_miss],
            skipped_shared_groups: &[],
            skipped_shared_pair_work: 0,
            considered_shared_pair_work: 0,
            shared_work_strata: &[],
            pair_clusters: &[ExactEvidenceCluster {
                id: 9,
                exact_matches: 1_000,
            }],
            shared_clusters: &[ExactEvidenceCluster {
                id: 5,
                exact_matches: 1_000,
            }],
        },
        &plan,
        EvidenceGatePolicy::permissive(),
    )
    .unwrap();
    assert!(report.passed);
    assert_eq!(report.observed_misses, 0);
}

#[test]
fn evidence_gate_rejects_a_wilson_upper_bound_above_policy() {
    let misses = (0..20)
        .map(|right_atom| metadata_engine::exact_islands::ExactMiss {
            left_atom: 1,
            right_atom,
        })
        .collect::<Vec<_>>();
    let report = evaluate_holdout(
        HoldoutEvidence {
            evaluated_pair_work: 1_000,
            exhaustive: false,
            pair_exact_matches: 1_000,
            pair_misses: &misses,
            shared_exact_matches: 0,
            shared_misses: &[],
            skipped_shared_groups: &[],
            skipped_shared_pair_work: 0,
            considered_shared_pair_work: 0,
            shared_work_strata: &[],
            pair_clusters: &[ExactEvidenceCluster {
                id: 1,
                exact_matches: 1_000,
            }],
            shared_clusters: &[],
        },
        &RescuePlan::default(),
        EvidenceGatePolicy {
            max_miss_rate: 0.01,
            confidence_z: 1.96,
            min_exact_matches: 30,
            max_skipped_pair_work_rate: 0.0,
        },
    )
    .unwrap();

    assert!(!report.passed);
    assert!(report.wilson_upper_bound > 0.01);
}

#[test]
fn evidence_gate_uses_independent_clusters_for_the_confidence_bound() {
    let report = evaluate_holdout(
        HoldoutEvidence {
            evaluated_pair_work: 10_000,
            exhaustive: false,
            pair_exact_matches: 1_000,
            pair_misses: &[],
            shared_exact_matches: 0,
            shared_misses: &[],
            skipped_shared_groups: &[],
            skipped_shared_pair_work: 0,
            considered_shared_pair_work: 0,
            shared_work_strata: &[],
            pair_clusters: &[ExactEvidenceCluster {
                id: 7,
                exact_matches: 1_000,
            }],
            shared_clusters: &[],
        },
        &RescuePlan::default(),
        EvidenceGatePolicy {
            max_miss_rate: 0.10,
            confidence_z: 1.96,
            min_exact_matches: 30,
            max_skipped_pair_work_rate: 0.0,
        },
    )
    .unwrap();

    assert!(!report.passed);
    assert_eq!(report.statistical_trials, 1);
    assert!(report.wilson_upper_bound > 0.10);
}

#[test]
fn evidence_gate_rejects_an_impossible_miss_count() {
    let error = evaluate_holdout(
        HoldoutEvidence {
            evaluated_pair_work: 1,
            exhaustive: false,
            pair_exact_matches: 0,
            pair_misses: &[metadata_engine::exact_islands::ExactMiss {
                left_atom: 1,
                right_atom: 2,
            }],
            shared_exact_matches: 0,
            shared_misses: &[],
            skipped_shared_groups: &[],
            skipped_shared_pair_work: 0,
            considered_shared_pair_work: 0,
            shared_work_strata: &[],
            pair_clusters: &[],
            shared_clusters: &[],
        },
        &RescuePlan::default(),
        EvidenceGatePolicy::production(),
    )
    .unwrap_err();

    assert!(error.to_string().contains("exceeds exact matches"));
}

#[test]
fn evidence_gate_fails_closed_when_exact_match_sample_is_too_small() {
    let report = evaluate_holdout(
        HoldoutEvidence {
            evaluated_pair_work: 10,
            exhaustive: false,
            pair_exact_matches: 10,
            pair_misses: &[],
            shared_exact_matches: 0,
            shared_misses: &[],
            skipped_shared_groups: &[],
            skipped_shared_pair_work: 0,
            considered_shared_pair_work: 0,
            shared_work_strata: &[],
            pair_clusters: &[ExactEvidenceCluster {
                id: 0,
                exact_matches: 10,
            }],
            shared_clusters: &[],
        },
        &RescuePlan::default(),
        EvidenceGatePolicy {
            max_miss_rate: 0.01,
            confidence_z: 1.96,
            min_exact_matches: 30,
            max_skipped_pair_work_rate: 0.0,
        },
    )
    .unwrap();

    assert!(!report.passed);
    assert_eq!(report.exact_matches, 10);
}

#[test]
fn evidence_gate_accepts_a_vacuous_universe_with_no_pair_work() {
    let report = evaluate_holdout(
        HoldoutEvidence {
            evaluated_pair_work: 0,
            exhaustive: true,
            pair_exact_matches: 0,
            pair_misses: &[],
            shared_exact_matches: 0,
            shared_misses: &[],
            skipped_shared_groups: &[],
            skipped_shared_pair_work: 0,
            considered_shared_pair_work: 0,
            shared_work_strata: &[],
            pair_clusters: &[],
            shared_clusters: &[],
        },
        &RescuePlan::default(),
        EvidenceGatePolicy::production(),
    )
    .unwrap();

    assert!(report.passed);
    assert!(report.sample_sufficient);
}

#[test]
fn exhaustive_evidence_with_zero_residual_misses_does_not_use_wilson_sampling() {
    let report = evaluate_holdout(
        HoldoutEvidence {
            evaluated_pair_work: 1,
            exhaustive: true,
            pair_exact_matches: 1,
            pair_misses: &[],
            shared_exact_matches: 0,
            shared_misses: &[],
            skipped_shared_groups: &[],
            skipped_shared_pair_work: 0,
            considered_shared_pair_work: 0,
            shared_work_strata: &[],
            pair_clusters: &[ExactEvidenceCluster {
                id: 0,
                exact_matches: 1,
            }],
            shared_clusters: &[],
        },
        &RescuePlan::default(),
        EvidenceGatePolicy::production(),
    )
    .unwrap();

    assert!(report.passed);
}

#[test]
fn frozen_calibration_rescue_repairs_production_components() {
    let (dir, features, blocking) = snapshot_fixture();
    let result = metadata_engine::pipeline::run_metadata_pipeline(
        &features,
        &blocking,
        &dir.path().join("evidence-only-match"),
        &metadata_engine::pipeline::MetadataPipelineConfig {
            storage_work_directory: dir.path().to_path_buf(),
            memory_hard_top: MATCH_HARD_TOP,
            host_total_memory: 512 * GIB,
            threads: 1,
            max_catalog_jobs: 100,
            max_candidate_pair_visits: 1_000_000,
            exact_sample_lefts: 1,
            exact_pair_work: 10,
            evidence_gate_policy: EvidenceGatePolicy::permissive(),
            edge_bytes: 1_000_000,
        },
    )
    .unwrap();

    assert!(!result.exact_evidence.conservative_misses.is_empty());
    assert!(!result.rescue_plan.pair_atoms.is_empty());
    assert_eq!(result.scope_components.intra_roots, vec![0, 0]);
}

#[test]
fn insufficient_exact_holdout_gate_fails_closed_without_publishing_summary() {
    let (dir, features, blocking) = snapshot_fixture();
    let out = dir.path().join("strict-evidence-match");
    let error = metadata_engine::pipeline::run_metadata_pipeline(
        &features,
        &blocking,
        &out,
        &metadata_engine::pipeline::MetadataPipelineConfig {
            storage_work_directory: dir.path().to_path_buf(),
            memory_hard_top: MATCH_HARD_TOP,
            host_total_memory: 512 * GIB,
            threads: 2,
            max_catalog_jobs: 100,
            max_candidate_pair_visits: 1_000_000,
            exact_sample_lefts: 0,
            exact_pair_work: 0,
            evidence_gate_policy: EvidenceGatePolicy {
                max_miss_rate: 0.0,
                confidence_z: 1.96,
                min_exact_matches: 1,
                max_skipped_pair_work_rate: 0.0,
            },
            edge_bytes: 1_000_000,
        },
    )
    .unwrap_err();

    assert!(error.to_string().contains("Wilson upper bound"), "{error}");
    assert!(!out
        .join("metadata-summary-1/metadata-summary.ready")
        .is_file());
}

#[test]
fn shared_token_local_routing_is_admitted_by_routed_work() {
    let dir = tempfile::tempdir().unwrap();
    let features = dir.path().join("shared-features");
    let blocking = dir.path().join("shared-blocking");
    let sources = (0..300u32)
        .map(|contract_id| EncodeSourceRow {
            contract_id,
            payload_id: contract_id,
            retained_token_ids: vec![7],
        })
        .collect::<Vec<_>>();
    let payloads = (0..300u32)
        .map(|term| EncodePayloadRow {
            template_terms: vec![(term + 1, 1)],
            content_terms: vec![(term + 10_000, 1)],
        })
        .collect::<Vec<_>>();
    write_encode_artifacts(&features, &sources, &payloads).unwrap();
    let sketches = (0..300u64)
        .map(|value| AtomSketch {
            template_simhash: value.wrapping_mul(0x9e37_79b9_7f4a_7c15),
            content_simhash: value.wrapping_add(17).wrapping_mul(0xbf58_476d_1ce4_e5b9),
            template_anchors: vec![value as u32],
            content_anchors: vec![value as u32 + 10_000],
            has_template_terms: true,
            has_content_terms: true,
        })
        .collect::<Vec<_>>();
    compile_base_equivalent(
        &sketches,
        &BlockingCompileConfig {
            max_routing_block_members: 10_000,
        },
        &blocking,
    )
    .unwrap();
    commit_ready(
        &features,
        "features.ready",
        r#"{"schema_revision":3,"source_count":300,"payload_count":300,"chains":["x"],"chain_totals":[{"name":"x","contracts":300,"nfts":300}]}"#,
    )
    .unwrap();
    commit_ready(
        &blocking,
        "blocking.ready",
        r#"{"blocking_revision":3,"atom_count":300}"#,
    )
    .unwrap();

    metadata_engine::pipeline::run_metadata_pipeline(
        &features,
        &blocking,
        &dir.path().join("shared-match"),
        &metadata_engine::pipeline::MetadataPipelineConfig {
            storage_work_directory: dir.path().to_path_buf(),
            memory_hard_top: MATCH_HARD_TOP,
            host_total_memory: 512 * GIB,
            threads: 1,
            max_catalog_jobs: 100_000,
            max_candidate_pair_visits: 40_000,
            exact_sample_lefts: 0,
            exact_pair_work: 0,
            evidence_gate_policy: EvidenceGatePolicy::permissive(),
            edge_bytes: 1_000_000,
        },
    )
    .unwrap();
}

#[test]
fn catalog_parallelism_preserves_deterministic_components_and_metrics() {
    let (dir, features, blocking) = expanded_atom_snapshot_fixture();
    let run = |name: &str, threads: usize| {
        metadata_engine::pipeline::run_metadata_pipeline(
            &features,
            &blocking,
            &dir.path().join(name),
            &metadata_engine::pipeline::MetadataPipelineConfig {
                storage_work_directory: dir.path().to_path_buf(),
                memory_hard_top: MATCH_HARD_TOP,
                host_total_memory: 512 * GIB,
                threads,
                max_catalog_jobs: 100,
                max_candidate_pair_visits: 1_000_000,
                exact_sample_lefts: 0,
                exact_pair_work: 0,
                evidence_gate_policy: EvidenceGatePolicy::permissive(),
                edge_bytes: 1_000_000,
            },
        )
        .unwrap()
    };
    let serial = run("serial-match", 1);
    let parallel = run("parallel-match", 4);
    let visible_threads = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(4);
    let visible_max = run("visible-max-match", visible_threads);

    assert_eq!(
        serial.scope_components.intra_roots,
        parallel.scope_components.intra_roots
    );
    assert_eq!(
        serial.scope_components.cross_roots,
        parallel.scope_components.cross_roots
    );
    assert_eq!(
        serial.index_metrics.block_pair_visits,
        parallel.index_metrics.block_pair_visits
    );
    assert_eq!(
        serial.index_metrics.routed_pairs,
        parallel.index_metrics.routed_pairs
    );
    assert_eq!(serial.scope_components, visible_max.scope_components);
    assert_eq!(serial.summary_rows, visible_max.summary_rows);
    assert_eq!(serial.index_metrics, visible_max.index_metrics);
}

#[test]
fn catalog_candidate_visit_limit_is_ignored() {
    let (dir, features, blocking) = expanded_atom_snapshot_fixture();
    let result = metadata_engine::pipeline::run_metadata_pipeline(
        &features,
        &blocking,
        &dir.path().join("dynamic-catalog-budget"),
        &metadata_engine::pipeline::MetadataPipelineConfig {
            storage_work_directory: dir.path().to_path_buf(),
            memory_hard_top: MATCH_HARD_TOP,
            host_total_memory: 512 * GIB,
            threads: 4,
            max_catalog_jobs: 100,
            max_candidate_pair_visits: 10,
            exact_sample_lefts: 0,
            exact_pair_work: 0,
            evidence_gate_policy: EvidenceGatePolicy::permissive(),
            edge_bytes: 1_000_000,
        },
    )
    .unwrap();

    assert!(result.planned_candidate_pair_visits <= 10);

    let below_actual_limit = metadata_engine::pipeline::run_metadata_pipeline(
        &features,
        &blocking,
        &dir.path().join("dynamic-catalog-budget-rejected"),
        &metadata_engine::pipeline::MetadataPipelineConfig {
            storage_work_directory: dir.path().to_path_buf(),
            memory_hard_top: MATCH_HARD_TOP,
            host_total_memory: 512 * GIB,
            threads: 4,
            max_catalog_jobs: 100,
            max_candidate_pair_visits: 6,
            exact_sample_lefts: 0,
            exact_pair_work: 0,
            evidence_gate_policy: EvidenceGatePolicy::permissive(),
            edge_bytes: 1_000_000,
        },
    )
    .unwrap();
    assert_eq!(result.scope_components, below_actual_limit.scope_components);
    assert_eq!(result.edge_count, below_actual_limit.edge_count);
}

#[test]
fn pair_exact_parallelism_preserves_deterministic_evidence() {
    let (_dir, features, blocking) = expanded_atom_snapshot_fixture();
    let snapshot = MetadataSnapshot::open(&features, &blocking).unwrap();
    let run = |max_lanes| {
        run_pair_exact_island(
            &snapshot,
            &[0, 1],
            ExactEvidenceBudget {
                max_lefts: 2,
                max_pair_work: 2,
                max_artifact_bytes: 1_000_000,
                max_lanes,
            },
            None,
        )
        .unwrap()
    };
    let serial = run(1);
    let parallel = run(4);
    assert_eq!(serial.pair_work, parallel.pair_work);
    assert_eq!(serial.exact_matches, parallel.exact_matches);
    assert_eq!(serial.conservative_misses, parallel.conservative_misses);
}

#[test]
fn pair_exact_counts_two_sampled_endpoints_as_one_unordered_pair() {
    let (_dir, features, blocking) = expanded_atom_snapshot_fixture();
    let snapshot = MetadataSnapshot::open(&features, &blocking).unwrap();

    let evidence = run_pair_exact_island(
        &snapshot,
        &[0, 1],
        ExactEvidenceBudget {
            max_lefts: 2,
            max_pair_work: 1,
            max_artifact_bytes: 1_000_000,
            max_lanes: 2,
        },
        None,
    )
    .unwrap();

    assert_eq!(evidence.pair_work, 1);
    assert!(evidence
        .conservative_misses
        .iter()
        .all(|miss| miss.left_atom < miss.right_atom));
}

#[test]
fn shared_token_exact_parallelism_preserves_deterministic_evidence() {
    let dir = tempfile::tempdir().unwrap();
    let features = dir.path().join("shared-features");
    let blocking = dir.path().join("shared-blocking");
    let sources = (0..4)
        .map(|contract_id| EncodeSourceRow {
            contract_id,
            payload_id: contract_id % 2,
            retained_token_ids: vec![1],
        })
        .collect::<Vec<_>>();
    write_encode_artifacts(
        &features,
        &sources,
        &[
            EncodePayloadRow {
                template_terms: vec![(1, 1)],
                content_terms: vec![(2, 1)],
            },
            EncodePayloadRow {
                template_terms: vec![(1, 1)],
                content_terms: vec![(2, 1)],
            },
        ],
    )
    .unwrap();
    let sketch = AtomSketch {
        template_simhash: 0,
        content_simhash: 0,
        template_anchors: vec![1],
        content_anchors: vec![2],
        has_template_terms: true,
        has_content_terms: true,
    };
    compile_base_equivalent(
        &[sketch.clone(), sketch.clone(), sketch.clone(), sketch],
        &BlockingCompileConfig {
            max_routing_block_members: 10,
        },
        &blocking,
    )
    .unwrap();
    commit_ready(
        &features,
        "features.ready",
        r#"{"schema_revision":3,"source_count":4,"payload_count":2,"chains":["x"],"chain_totals":[{"name":"x","contracts":4,"nfts":4}]}"#,
    )
    .unwrap();
    commit_ready(
        &blocking,
        "blocking.ready",
        r#"{"blocking_revision":3,"atom_count":4}"#,
    )
    .unwrap();
    let snapshot = MetadataSnapshot::open(&features, &blocking).unwrap();
    let run = |max_lanes, output_dir| {
        run_shared_token_exact_islands(
            &snapshot,
            &[1],
            &[],
            ExactEvidenceBudget {
                max_lefts: 1,
                max_pair_work: 6,
                max_artifact_bytes: 1_000_000,
                max_lanes,
            },
            output_dir,
        )
        .unwrap()
    };

    let serial = run(1, None);
    let evidence_dir = dir.path().join("shared-evidence");
    let parallel = run(4, Some(&evidence_dir));
    assert_eq!(serial, parallel);
    assert_eq!(
        open_shared_token_exact_evidence(&evidence_dir, &snapshot, &[1], &[])
            .unwrap()
            .unwrap(),
        parallel
    );
    assert!(
        open_shared_token_exact_evidence(&evidence_dir, &snapshot, &[], &[1])
            .unwrap()
            .is_none()
    );
}

#[test]
fn pair_exact_evidence_reopens_only_for_the_same_frontier() {
    let (dir, features, blocking) = snapshot_fixture();
    let snapshot = MetadataSnapshot::open(&features, &blocking).unwrap();
    let evidence_dir = dir.path().join("pair-evidence");
    let written = run_pair_exact_island(
        &snapshot,
        &[0],
        ExactEvidenceBudget {
            max_lefts: 1,
            max_pair_work: 1,
            max_artifact_bytes: 1_000_000,
            max_lanes: 1,
        },
        Some(&evidence_dir),
    )
    .unwrap();

    let reopened = open_pair_exact_evidence(&evidence_dir, &snapshot, &[0])
        .unwrap()
        .unwrap();
    assert_eq!(written.pair_work, reopened.pair_work);
    assert!(open_pair_exact_evidence(&evidence_dir, &snapshot, &[1])
        .unwrap()
        .is_none());
}

#[test]
fn stale_exact_evidence_identity_is_retired_and_recomputed() {
    let (dir, features, blocking) = snapshot_fixture();
    let out = dir.path().join("stale-exact-identity");
    let config = metadata_engine::pipeline::MetadataPipelineConfig {
        storage_work_directory: dir.path().to_path_buf(),
        memory_hard_top: MATCH_HARD_TOP,
        host_total_memory: 512 * GIB,
        threads: 1,
        max_catalog_jobs: 100,
        max_candidate_pair_visits: 1_000_000,
        exact_sample_lefts: 1,
        exact_pair_work: 10,
        evidence_gate_policy: EvidenceGatePolicy::permissive(),
        edge_bytes: 1_000_000,
    };
    let first = metadata_engine::pipeline::run_metadata_pipeline_durable(
        &features, &blocking, &out, &config,
    )
    .unwrap();
    let ready = out.join("exact-islands/pair-calibration-1/ready");
    let mut json: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&ready).unwrap()).unwrap();
    json["artifact_revision"] = 0.into();
    json["snapshot_fingerprint"] = "stale-snapshot".into();
    json["pair_work"] = 999.into();
    std::fs::write(&ready, serde_json::to_vec_pretty(&json).unwrap()).unwrap();
    std::fs::remove_dir_all(out.join("connectivity-runs")).unwrap();
    std::fs::remove_dir_all(out.join("component-snapshots")).unwrap();
    std::fs::remove_dir_all(out.join("metadata-summary-1")).unwrap();

    let rerun = metadata_engine::pipeline::run_metadata_pipeline_durable(
        &features, &blocking, &out, &config,
    )
    .unwrap();

    assert_eq!(
        rerun.exact_evidence.pair_work,
        first.exact_evidence.pair_work
    );
    assert_ne!(rerun.exact_evidence.pair_work, 999);
}

#[test]
fn snapshot_only_pipeline_commits_recoverable_products() {
    let (dir, features, blocking) = snapshot_fixture();
    let out = dir.path().join("match");
    let result = metadata_engine::pipeline::run_metadata_pipeline_durable(
        &features,
        &blocking,
        &out,
        &metadata_engine::pipeline::MetadataPipelineConfig {
            storage_work_directory: dir.path().to_path_buf(),
            memory_hard_top: MATCH_HARD_TOP,
            host_total_memory: 512 * GIB,
            threads: 1,
            max_catalog_jobs: 100,
            max_candidate_pair_visits: 1_000_000,
            exact_sample_lefts: 1,
            exact_pair_work: 10,
            evidence_gate_policy: EvidenceGatePolicy::permissive(),
            edge_bytes: 1_000_000,
        },
    )
    .unwrap();
    let result_json = serde_json::to_value(&result).unwrap();
    assert!(result_json.get("production_ready").is_none());
    assert!(result_json.get("blockers").is_none());
    assert_eq!(result.exact_evidence.sampled_lefts, vec![0]);
    assert_eq!(result.pair_holdout_evidence.sampled_lefts, vec![1]);
    assert!(!result.pair_holdout_evidence.conservative_misses.is_empty());
    assert!(serde_json::to_value(&result.index_metrics)
        .unwrap()
        .get("retained_token_rejects")
        .is_none());
    assert_eq!(result.scope_components.intra_roots, vec![0, 0]);
    assert!(result.evidence_gate_report.passed);
    assert!(out.join("rescue-plan-1/rescue-plan.ready").is_file());
    assert!(out.join("index-1/index.ready").is_file());
    assert!(out.join("recall-plan-1/recall-plan.ready").is_file());
    assert!(out
        .join("component-snapshots/intra/component-snapshot-000000.ready")
        .is_file());
    assert!(out
        .join("metadata-summary-1/metadata-summary.ready")
        .is_file());
    let ledger: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.path().join("storage-ledger.json")).unwrap())
            .unwrap();
    let artifacts = ledger["artifacts"].as_object().unwrap();
    for class in [
        "index",
        "exact_evidence",
        "recall_plan",
        "connectivity_run",
        "component_snapshot",
        "summary",
    ] {
        assert!(artifacts.values().any(|artifact| {
            artifact["class"] == class
                && artifact["pins"]
                    .as_array()
                    .is_some_and(|pins| pins.iter().any(|pin| pin == "metadata_complete"))
        }));
    }
}

#[test]
fn committed_connectivity_runs_resume_without_rescoring_candidates() {
    let (dir, features, blocking) = snapshot_fixture();
    let out = dir.path().join("resume-match");
    let config = metadata_engine::pipeline::MetadataPipelineConfig {
        storage_work_directory: dir.path().to_path_buf(),
        memory_hard_top: MATCH_HARD_TOP,
        host_total_memory: 512 * GIB,
        threads: 2,
        max_catalog_jobs: 100,
        max_candidate_pair_visits: 1_000_000,
        exact_sample_lefts: 1,
        exact_pair_work: 10,
        evidence_gate_policy: EvidenceGatePolicy::permissive(),
        edge_bytes: 1_000_000,
    };
    let first = metadata_engine::pipeline::run_metadata_pipeline_durable(
        &features, &blocking, &out, &config,
    )
    .unwrap();
    std::fs::remove_dir_all(out.join("component-snapshots")).unwrap();
    std::fs::remove_dir_all(out.join("metadata-summary-1")).unwrap();

    let mut events = Vec::new();
    let resumed = metadata_engine::pipeline::run_metadata_pipeline_with_progress_and_persistence(
        &features,
        &blocking,
        &out,
        &config,
        metadata_engine::pipeline::MatchPersistence::Durable,
        |event| events.push(event),
    )
    .unwrap();

    assert_eq!(
        first.scope_components.intra_roots,
        resumed.scope_components.intra_roots
    );
    assert!(out.join("connectivity-runs/connectivity.ready").is_file());
    let catalog = events
        .iter()
        .filter(|event| event.phase == ProgressPhase::CatalogPairs)
        .collect::<Vec<_>>();
    assert_eq!(catalog.len(), 1);
    assert_eq!(catalog[0].total, Some(0));
}

#[test]
fn default_pipeline_persists_only_final_grouping_and_summary_artifacts() {
    let (dir, features, blocking) = snapshot_fixture();
    let out = dir.path().join("memory-first-match");
    let config = metadata_engine::pipeline::MetadataPipelineConfig {
        storage_work_directory: dir.path().to_path_buf(),
        memory_hard_top: MATCH_HARD_TOP,
        host_total_memory: 512 * GIB,
        threads: 1,
        max_catalog_jobs: 100,
        max_candidate_pair_visits: 1_000_000,
        exact_sample_lefts: 1,
        exact_pair_work: 10,
        evidence_gate_policy: EvidenceGatePolicy::permissive(),
        edge_bytes: 1_000_000,
    };

    metadata_engine::pipeline::run_metadata_pipeline_durable(&features, &blocking, &out, &config)
        .unwrap();
    assert!(out.join("index-1/index.ready").is_file());

    let mut events = Vec::new();
    metadata_engine::pipeline::run_metadata_pipeline_with_progress(
        &features,
        &blocking,
        &out,
        &config,
        |event| events.push(event),
    )
    .unwrap();

    assert!(out
        .join("metadata-summary-1/metadata-summary.ready")
        .is_file());
    assert!(out.join("component-snapshots").is_dir());
    for recovery_only in [
        "index-1",
        "exact-islands",
        "rescue-plan-1",
        "recall-plan-1",
        "connectivity-runs",
    ] {
        assert!(
            !out.join(recovery_only).exists(),
            "default memory-first Match must not persist {recovery_only}"
        );
    }
    assert!(events
        .iter()
        .all(|event| event.phase != ProgressPhase::CommitConnectivityRuns));
    assert!(events
        .iter()
        .all(|event| event.phase != ProgressPhase::BuildRecoveryChain));
    assert!(events
        .iter()
        .any(|event| event.phase == ProgressPhase::FinalizeComponents));
}

#[test]
fn committed_component_chains_resume_without_reducing_or_rewriting_scopes() {
    let (dir, features, blocking) = snapshot_fixture();
    let out = dir.path().join("resume-components");
    let config = metadata_engine::pipeline::MetadataPipelineConfig {
        storage_work_directory: dir.path().to_path_buf(),
        memory_hard_top: MATCH_HARD_TOP,
        host_total_memory: 512 * GIB,
        threads: 2,
        max_catalog_jobs: 100,
        max_candidate_pair_visits: 1_000_000,
        exact_sample_lefts: 1,
        exact_pair_work: 10,
        evidence_gate_policy: EvidenceGatePolicy::permissive(),
        edge_bytes: 1_000_000,
    };
    let first = metadata_engine::pipeline::run_metadata_pipeline_durable(
        &features, &blocking, &out, &config,
    )
    .unwrap();
    std::fs::remove_dir_all(out.join("metadata-summary-1")).unwrap();

    let mut events = Vec::new();
    let resumed = metadata_engine::pipeline::run_metadata_pipeline_with_progress_and_persistence(
        &features,
        &blocking,
        &out,
        &config,
        metadata_engine::pipeline::MatchPersistence::Durable,
        |event| events.push(event),
    )
    .unwrap();

    assert_eq!(
        serde_json::to_value(&resumed.scope_components).unwrap(),
        serde_json::to_value(&first.scope_components).unwrap(),
        "recovered roots must be deterministic"
    );
    assert_eq!(
        serde_json::to_value(&resumed.summary_rows).unwrap(),
        serde_json::to_value(&first.summary_rows).unwrap()
    );
    for phase in [ProgressPhase::ReduceScopes, ProgressPhase::CommitComponents] {
        let terminal = events
            .iter()
            .rfind(|event| event.phase == phase)
            .expect("resume must report skipped component work");
        assert_eq!(terminal.total, Some(0), "{phase:?} must be fully reused");
        assert_eq!(terminal.completed, 0);
    }
    let recovery = events
        .iter()
        .filter(|event| event.phase == ProgressPhase::BuildRecoveryChain)
        .collect::<Vec<_>>();
    assert_eq!(recovery.first().unwrap().completed, 0);
    assert_eq!(recovery.last().unwrap().completed, 2);
    assert_eq!(recovery.last().unwrap().total, Some(2));
}

#[test]
fn stale_component_identity_rebuilds_only_that_scope() {
    let (dir, features, blocking) = snapshot_fixture();
    let out = dir.path().join("stale-component-scope");
    let config = metadata_engine::pipeline::MetadataPipelineConfig {
        storage_work_directory: dir.path().to_path_buf(),
        memory_hard_top: MATCH_HARD_TOP,
        host_total_memory: 512 * GIB,
        threads: 1,
        max_catalog_jobs: 100,
        max_candidate_pair_visits: 1_000_000,
        exact_sample_lefts: 1,
        exact_pair_work: 10,
        evidence_gate_policy: EvidenceGatePolicy::permissive(),
        edge_bytes: 1_000_000,
    };
    let first = metadata_engine::pipeline::run_metadata_pipeline_durable(
        &features, &blocking, &out, &config,
    )
    .unwrap();
    std::fs::remove_dir_all(out.join("metadata-summary-1")).unwrap();
    let ready = out.join("component-snapshots/cross/component-chain.ready");
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&ready).unwrap()).unwrap();
    manifest["identity"]["scope_identity"] = "stale-cross".into();
    std::fs::write(&ready, serde_json::to_vec_pretty(&manifest).unwrap()).unwrap();

    let mut events = Vec::new();
    let resumed = metadata_engine::pipeline::run_metadata_pipeline_with_progress_and_persistence(
        &features,
        &blocking,
        &out,
        &config,
        metadata_engine::pipeline::MatchPersistence::Durable,
        |event| events.push(event),
    )
    .unwrap();

    assert_eq!(
        serde_json::to_value(&resumed.scope_components).unwrap(),
        serde_json::to_value(&first.scope_components).unwrap()
    );
    let reduce = events
        .iter()
        .rfind(|event| event.phase == ProgressPhase::ReduceScopes)
        .unwrap();
    assert_eq!(
        reduce.total,
        Some(2),
        "only the edge-free two-node cross scope should reduce"
    );
    let recovery = events
        .iter()
        .rfind(|event| event.phase == ProgressPhase::BuildRecoveryChain)
        .unwrap();
    assert_eq!(recovery.counters.matched, 1, "one scope was reused");
    assert_eq!(recovery.counters.groups, 1, "one scope was rebuilt");
    let committed = events
        .iter()
        .rfind(|event| event.phase == ProgressPhase::CommitComponents)
        .unwrap();
    assert_eq!(
        committed.total,
        Some(2),
        "one full snapshot plus its chain manifest"
    );
}

#[test]
fn matching_component_chain_corruption_fails_the_pipeline_closed() {
    let (dir, features, blocking) = snapshot_fixture();
    let out = dir.path().join("corrupt-components");
    let config = metadata_engine::pipeline::MetadataPipelineConfig {
        storage_work_directory: dir.path().to_path_buf(),
        memory_hard_top: MATCH_HARD_TOP,
        host_total_memory: 512 * GIB,
        threads: 1,
        max_catalog_jobs: 100,
        max_candidate_pair_visits: 1_000_000,
        exact_sample_lefts: 1,
        exact_pair_work: 10,
        evidence_gate_policy: EvidenceGatePolicy::permissive(),
        edge_bytes: 1_000_000,
    };
    metadata_engine::pipeline::run_metadata_pipeline_durable(&features, &blocking, &out, &config)
        .unwrap();
    std::fs::remove_dir_all(out.join("metadata-summary-1")).unwrap();
    std::fs::write(
        out.join("component-snapshots/intra/component-roots-000000.u32"),
        b"corrupt",
    )
    .unwrap();

    let error = metadata_engine::pipeline::run_metadata_pipeline_durable(
        &features, &blocking, &out, &config,
    )
    .unwrap_err();
    assert!(error.to_string().contains("component snapshot"), "{error}");
    assert!(!out
        .join("metadata-summary-1/metadata-summary.ready")
        .is_file());
}

#[test]
fn recovered_connectivity_ignores_a_lower_legacy_candidate_cap() {
    let dir = tempfile::tempdir().unwrap();
    let features = dir.path().join("e");
    let blocking = dir.path().join("b");
    let sources = (0..3)
        .map(|contract_id| EncodeSourceRow {
            contract_id,
            payload_id: 0,
            retained_token_ids: vec![1],
        })
        .collect::<Vec<_>>();
    let contracts = (0..3)
        .map(|contract_id| EncodeContractRow {
            contract_id,
            chain_id: 0,
            source_doc_id: contract_id,
            payload_id: 0,
            weight: 1,
        })
        .collect::<Vec<_>>();
    write_encode_artifacts_with_contracts_and_atoms(
        &features,
        &sources,
        &[EncodePayloadRow {
            template_terms: vec![(1, 1)],
            content_terms: vec![(2, 1)],
        }],
        &contracts,
        &[vec![0, 1, 2]],
    )
    .unwrap();
    compile_base_equivalent(
        &[AtomSketch {
            template_simhash: 0,
            content_simhash: 0,
            template_anchors: vec![1],
            content_anchors: vec![2],
            has_template_terms: true,
            has_content_terms: true,
        }],
        &BlockingCompileConfig {
            max_routing_block_members: 10,
        },
        &blocking,
    )
    .unwrap();
    commit_ready(
        &features,
        "features.ready",
        r#"{"schema_revision":3,"source_count":3,"payload_count":1,"chains":["x"],"chain_totals":[{"name":"x","contracts":3,"nfts":3}]}"#,
    )
    .unwrap();
    commit_ready(
        &blocking,
        "blocking.ready",
        r#"{"blocking_revision":3,"atom_count":1}"#,
    )
    .unwrap();
    let out = dir.path().join("resume-lower-cap");
    let mut config = metadata_engine::pipeline::MetadataPipelineConfig {
        storage_work_directory: dir.path().to_path_buf(),
        memory_hard_top: MATCH_HARD_TOP,
        host_total_memory: 512 * GIB,
        threads: 1,
        max_catalog_jobs: 100,
        max_candidate_pair_visits: 100,
        exact_sample_lefts: 0,
        exact_pair_work: 0,
        evidence_gate_policy: EvidenceGatePolicy::permissive(),
        edge_bytes: 1_000_000,
    };
    metadata_engine::pipeline::run_metadata_pipeline_durable(&features, &blocking, &out, &config)
        .unwrap();
    std::fs::remove_dir_all(out.join("component-snapshots")).unwrap();
    std::fs::remove_dir_all(out.join("metadata-summary-1")).unwrap();
    config.max_candidate_pair_visits = 5;

    let recovered = metadata_engine::pipeline::run_metadata_pipeline_durable(
        &features, &blocking, &out, &config,
    )
    .unwrap();
    assert_eq!(recovered.scope_components.intra_roots, vec![0, 0, 0]);
}

#[test]
fn shared_small_group_ignores_legacy_candidate_visit_limit() {
    let dir = tempfile::tempdir().unwrap();
    let features = dir.path().join("e");
    let blocking = dir.path().join("b");
    let sources = (0..3)
        .map(|contract_id| EncodeSourceRow {
            contract_id,
            payload_id: 0,
            retained_token_ids: vec![1],
        })
        .collect::<Vec<_>>();
    let contracts = (0..3)
        .map(|contract_id| EncodeContractRow {
            contract_id,
            chain_id: 0,
            source_doc_id: contract_id,
            payload_id: 0,
            weight: 1,
        })
        .collect::<Vec<_>>();
    write_encode_artifacts_with_contracts_and_atoms(
        &features,
        &sources,
        &[EncodePayloadRow {
            template_terms: vec![(1, 1)],
            content_terms: vec![(2, 1)],
        }],
        &contracts,
        &[vec![0, 1, 2]],
    )
    .unwrap();
    compile_base_equivalent(
        &[AtomSketch {
            template_simhash: 0,
            content_simhash: 0,
            template_anchors: vec![1],
            content_anchors: vec![2],
            has_template_terms: true,
            has_content_terms: true,
        }],
        &BlockingCompileConfig {
            max_routing_block_members: 10,
        },
        &blocking,
    )
    .unwrap();
    commit_ready(
        &features,
        "features.ready",
        r#"{"schema_revision":3,"source_count":3,"payload_count":1,"chains":["x"],"chain_totals":[{"name":"x","contracts":3,"nfts":3}]}"#,
    )
    .unwrap();
    commit_ready(
        &blocking,
        "blocking.ready",
        r#"{"blocking_revision":3,"atom_count":1}"#,
    )
    .unwrap();
    let mut events = Vec::new();
    let result = metadata_engine::pipeline::run_metadata_pipeline_with_progress(
        &features,
        &blocking,
        &dir.path().join("small-group-budget"),
        &metadata_engine::pipeline::MetadataPipelineConfig {
            storage_work_directory: dir.path().to_path_buf(),
            memory_hard_top: MATCH_HARD_TOP,
            host_total_memory: 512 * GIB,
            threads: 2,
            max_catalog_jobs: 100,
            max_candidate_pair_visits: 5,
            exact_sample_lefts: 0,
            exact_pair_work: 0,
            evidence_gate_policy: EvidenceGatePolicy::permissive(),
            edge_bytes: 1_000_000,
        },
        |event| events.push(event),
    )
    .unwrap();
    assert_eq!(result.scope_components.intra_roots, vec![0, 0, 0]);
    assert!(events
        .iter()
        .filter(|event| event.phase == ProgressPhase::SharedTokenPairs)
        .any(|event| event.completed > 0));
}

#[test]
fn pipeline_reports_monotonic_pair_work_with_stable_terminal_plans() {
    let (dir, features, blocking) = snapshot_fixture();
    let out = dir.path().join("progress-match");
    let mut events = Vec::new();
    metadata_engine::pipeline::run_metadata_pipeline_with_progress(
        &features,
        &blocking,
        &out,
        &metadata_engine::pipeline::MetadataPipelineConfig {
            storage_work_directory: dir.path().to_path_buf(),
            memory_hard_top: MATCH_HARD_TOP,
            host_total_memory: 512 * GIB,
            threads: 1,
            max_catalog_jobs: 100,
            max_candidate_pair_visits: 1_000_000,
            exact_sample_lefts: 1,
            exact_pair_work: 10,
            evidence_gate_policy: EvidenceGatePolicy::permissive(),
            edge_bytes: 1_000_000,
        },
        |event| events.push(event),
    )
    .unwrap();

    let exact = events
        .iter()
        .filter(|event| event.phase == ProgressPhase::PairExactIsland)
        .collect::<Vec<_>>();
    assert!(!exact.is_empty());
    assert!(exact
        .windows(2)
        .all(|window| window[0].completed <= window[1].completed));
    let terminal = exact.last().unwrap();
    assert_eq!(terminal.unit, WorkUnit::Pairs);
    assert_eq!(terminal.total, Some(1));
    assert_eq!(terminal.completed, 1);
    let exact_finalize = events
        .iter()
        .rfind(|event| event.phase == ProgressPhase::PairExactFinalize)
        .expect("exact sorting and persistence must have a separate phase");
    assert_eq!(exact_finalize.completed, exact_finalize.total.unwrap());

    let catalog_terminal = events
        .iter()
        .rfind(|event| event.phase == ProgressPhase::CatalogPairs)
        .expect("pipeline must expose catalog traversal progress");
    assert_eq!(catalog_terminal.unit, WorkUnit::Pairs);
    assert_eq!(catalog_terminal.work_class, WorkClass::Generic);
    assert!(catalog_terminal.total.is_some());
    assert_eq!(catalog_terminal.total_kind, TotalKind::UpperBound);
    assert!(catalog_terminal.completed <= catalog_terminal.total.unwrap());

    let shared_token_events = events
        .iter()
        .filter(|event| event.phase == ProgressPhase::SharedTokenPairs)
        .collect::<Vec<_>>();
    assert!(!shared_token_events.is_empty());
    let shared_token_limit = shared_token_events[0].total;
    assert!(shared_token_limit.is_some());
    assert!(shared_token_events.iter().all(|event| {
        event.total == shared_token_limit && event.total_kind == TotalKind::UpperBound
    }));
    let shared_token_terminal = shared_token_events.last().unwrap();
    assert!(shared_token_terminal.completed <= shared_token_terminal.total.unwrap());
    for phase in [
        ProgressPhase::CommitComponents,
        ProgressPhase::BuildSummary,
        ProgressPhase::CommitArtifacts,
    ] {
        let phase_events = events
            .iter()
            .filter(|event| event.phase == phase)
            .collect::<Vec<_>>();
        assert!(!phase_events.is_empty(), "missing {phase:?}");
        assert_eq!(phase_events.first().unwrap().completed, 0);
        assert_eq!(
            phase_events.last().unwrap().completed,
            phase_events.last().unwrap().total.unwrap()
        );
    }
    let recovery = events
        .iter()
        .filter(|event| event.phase == ProgressPhase::FinalizeComponents)
        .collect::<Vec<_>>();
    assert!(!recovery.is_empty());
    assert!(recovery.iter().all(|event| event.total.is_some()));
    assert_eq!(
        recovery.last().unwrap().completed,
        recovery.last().unwrap().total.unwrap()
    );
    let catalog_build = events
        .iter()
        .filter(|event| event.phase == ProgressPhase::BuildCatalog)
        .collect::<Vec<_>>();
    assert!(
        catalog_build.len() > 2,
        "catalog construction must report progress within the build"
    );

    for phase in [
        ProgressPhase::OpenSnapshot,
        ProgressPhase::BuildCatalog,
        ProgressPhase::PairExactIsland,
        ProgressPhase::SharedTokenExactIsland,
        ProgressPhase::SharedTokenExactFinalize,
        ProgressPhase::FallbackPairs,
        ProgressPhase::CatalogPairs,
        ProgressPhase::SharedTokenPairs,
        ProgressPhase::FinalizeEdgeCollectors,
        ProgressPhase::EdgeDispatch,
        ProgressPhase::ReduceScopes,
        ProgressPhase::FinalizeComponents,
        ProgressPhase::CommitArtifacts,
    ] {
        let phase_events = events
            .iter()
            .filter(|event| event.phase == phase)
            .collect::<Vec<_>>();
        assert!(!phase_events.is_empty(), "missing {phase:?} progress");
        let terminal = phase_events.last().unwrap();
        if matches!(
            phase,
            ProgressPhase::CatalogPairs | ProgressPhase::SharedTokenPairs
        ) {
            assert_eq!(terminal.total_kind, TotalKind::UpperBound);
            assert!(terminal.completed <= terminal.total.unwrap());
        } else {
            assert_eq!(terminal.completed, terminal.total.unwrap(), "{phase:?}");
        }
        if phase == ProgressPhase::ReduceScopes {
            assert!(
                terminal.total.unwrap() >= 4,
                "two scopes over two nodes require at least four reduce work units"
            );
        }
    }
    let finalizer_index = events
        .iter()
        .rposition(|event| event.phase == ProgressPhase::FinalizeEdgeCollectors)
        .unwrap();
    let dispatch_index = events
        .iter()
        .rposition(|event| event.phase == ProgressPhase::EdgeDispatch)
        .unwrap();
    assert!(finalizer_index < dispatch_index);
    assert!(events
        .iter()
        .all(|event| event.phase != ProgressPhase::CommitConnectivityRuns));
}

#[test]
fn determinate_engine_progress_cannot_publish_more_than_total_work() {
    let event = metadata_engine::progress::ProgressEvent::determinate(
        ProgressPhase::EdgeDispatch,
        11,
        10,
        WorkUnit::Edges,
        metadata_engine::progress::ProgressCounters::default(),
    );
    assert_eq!(event.completed, 11);
    assert_eq!(event.exact_total_overrun(), Some(1));
}

#[test]
fn catalog_progress_uses_a_stable_combined_work_upper_bound() {
    let dir = tempfile::tempdir().unwrap();
    let features = dir.path().join("e");
    let blocking = dir.path().join("b");
    let sources = (0..40u32)
        .map(|contract_id| EncodeSourceRow {
            contract_id,
            payload_id: u32::from(contract_id >= 20),
            retained_token_ids: vec![contract_id],
        })
        .collect::<Vec<_>>();
    let contracts = (0..40u32)
        .map(|contract_id| EncodeContractRow {
            contract_id,
            chain_id: 0,
            source_doc_id: contract_id,
            payload_id: u32::from(contract_id >= 20),
            weight: 1,
        })
        .collect::<Vec<_>>();
    write_encode_artifacts_with_contracts_and_atoms(
        &features,
        &sources,
        &[
            EncodePayloadRow {
                template_terms: vec![(1, 1)],
                content_terms: vec![(2, 1)],
            },
            EncodePayloadRow {
                template_terms: vec![(3, 1)],
                content_terms: vec![(4, 1)],
            },
        ],
        &contracts,
        &[(0..20).collect(), (20..40).collect()],
    )
    .unwrap();
    compile_base_equivalent(
        &[
            AtomSketch {
                template_simhash: 0,
                content_simhash: 0,
                template_anchors: vec![10],
                content_anchors: vec![20],
                has_template_terms: true,
                has_content_terms: true,
            },
            AtomSketch {
                template_simhash: 0,
                content_simhash: 0,
                template_anchors: vec![10],
                content_anchors: vec![20],
                has_template_terms: true,
                has_content_terms: true,
            },
        ],
        &BlockingCompileConfig {
            max_routing_block_members: 10,
        },
        &blocking,
    )
    .unwrap();
    commit_ready(
        &features,
        "features.ready",
        r#"{"schema_revision":3,"source_count":40,"payload_count":2,"chains":["x"],"chain_totals":[{"name":"x","contracts":40,"nfts":40}]}"#,
    )
    .unwrap();
    commit_ready(
        &blocking,
        "blocking.ready",
        r#"{"blocking_revision":3,"atom_count":2}"#,
    )
    .unwrap();

    let mut events = Vec::new();
    metadata_engine::pipeline::run_metadata_pipeline_with_progress(
        &features,
        &blocking,
        &dir.path().join("m"),
        &metadata_engine::pipeline::MetadataPipelineConfig {
            storage_work_directory: dir.path().to_path_buf(),
            memory_hard_top: MATCH_HARD_TOP,
            host_total_memory: 512 * GIB,
            threads: 2,
            max_catalog_jobs: 100,
            max_candidate_pair_visits: 1_000_000,
            exact_sample_lefts: 0,
            exact_pair_work: 0,
            evidence_gate_policy: EvidenceGatePolicy::permissive(),
            edge_bytes: 1_000_000,
        },
        |event| events.push(event),
    )
    .unwrap();

    let terminal = events
        .iter()
        .rfind(|event| event.phase == ProgressPhase::CatalogPairs)
        .unwrap();
    let catalog_events = events
        .iter()
        .filter(|event| event.phase == ProgressPhase::CatalogPairs)
        .collect::<Vec<_>>();
    let catalog_upper_bound = catalog_events[0].total;
    assert!(catalog_upper_bound.is_some());
    assert!(catalog_events.iter().all(|event| {
        event.work_class == WorkClass::Generic
            && event.unit == WorkUnit::Pairs
            && event.total == catalog_upper_bound
            && event.total_kind == TotalKind::UpperBound
    }));
    assert!(terminal.completed <= terminal.total.unwrap());
    assert!(terminal.total.unwrap() > terminal.counters.scored);
    assert!(
        terminal.completed >= terminal.counters.expanded,
        "catalog wall-work progress must include completed conditional expansion"
    );
}

#[test]
fn zero_edge_run_still_commits_and_registers_empty_connectivity() {
    let (dir, features, blocking) = snapshot_fixture();
    let out = dir.path().join("zero-edge-match");
    let result = metadata_engine::pipeline::run_metadata_pipeline_durable(
        &features,
        &blocking,
        &out,
        &metadata_engine::pipeline::MetadataPipelineConfig {
            storage_work_directory: dir.path().to_path_buf(),
            memory_hard_top: MATCH_HARD_TOP,
            host_total_memory: 512 * GIB,
            threads: 1,
            max_catalog_jobs: 100,
            max_candidate_pair_visits: 1_000_000,
            exact_sample_lefts: 0,
            exact_pair_work: 0,
            evidence_gate_policy: EvidenceGatePolicy::permissive(),
            edge_bytes: 1_000_000,
        },
    )
    .unwrap();
    assert_eq!(result.edge_count, 0);
    assert!(out.join("connectivity-runs").is_dir());
    assert!(out
        .join("metadata-summary-1/metadata-summary.ready")
        .is_file());
}

#[test]
fn chain_local_representative_atom_connects_only_token_disjoint_members() {
    let dir = tempfile::tempdir().unwrap();
    let features = dir.path().join("e");
    let blocking = dir.path().join("b");
    let sources = vec![
        EncodeSourceRow {
            contract_id: 0,
            payload_id: 0,
            retained_token_ids: vec![1],
        },
        EncodeSourceRow {
            contract_id: 1,
            payload_id: 0,
            retained_token_ids: vec![1],
        },
        EncodeSourceRow {
            contract_id: 2,
            payload_id: 0,
            retained_token_ids: vec![2],
        },
    ];
    let contracts = (0..3)
        .map(|id| EncodeContractRow {
            contract_id: id,
            chain_id: 0,
            source_doc_id: id,
            payload_id: 0,
            weight: 1,
        })
        .collect::<Vec<_>>();
    write_encode_artifacts_with_contracts_and_atoms(
        &features,
        &sources,
        &[EncodePayloadRow {
            template_terms: vec![(1, 1)],
            content_terms: vec![(2, 1)],
        }],
        &contracts,
        &[vec![0, 1, 2]],
    )
    .unwrap();
    compile_base_equivalent(
        &[AtomSketch {
            template_simhash: 1,
            content_simhash: 2,
            template_anchors: vec![1],
            content_anchors: vec![2],
            has_template_terms: true,
            has_content_terms: true,
        }],
        &BlockingCompileConfig {
            max_routing_block_members: 10,
        },
        &blocking,
    )
    .unwrap();
    commit_ready(
        &features,
        "features.ready",
        r#"{"schema_revision":3,"source_count":3,"payload_count":1,"chains":["x"],"chain_totals":[{"name":"x","contracts":3,"nfts":3}]}"#,
    )
    .unwrap();
    commit_ready(
        &blocking,
        "blocking.ready",
        r#"{"blocking_revision":3,"atom_count":1}"#,
    )
    .unwrap();
    let result = metadata_engine::pipeline::run_metadata_pipeline(
        &features,
        &blocking,
        &dir.path().join("fallback-budget-rejected"),
        &metadata_engine::pipeline::MetadataPipelineConfig {
            storage_work_directory: dir.path().to_path_buf(),
            memory_hard_top: MATCH_HARD_TOP,
            host_total_memory: 512 * GIB,
            threads: 1,
            max_catalog_jobs: 100,
            max_candidate_pair_visits: 2,
            exact_sample_lefts: 0,
            exact_pair_work: 0,
            evidence_gate_policy: EvidenceGatePolicy::permissive(),
            edge_bytes: 1_000_000,
        },
    )
    .unwrap();
    assert_eq!(result.scope_components.intra_roots, vec![0, 0, 0]);
    assert_eq!(result.edge_count, 2);
}

#[test]
fn scope_forests_do_not_leak_transitive_edges_between_chain_pairs() {
    let dir = tempfile::tempdir().unwrap();
    let features = dir.path().join("e");
    let blocking = dir.path().join("b");
    let sources = vec![
        EncodeSourceRow {
            contract_id: 0,
            payload_id: 0,
            retained_token_ids: vec![1],
        },
        EncodeSourceRow {
            contract_id: 1,
            payload_id: 0,
            retained_token_ids: vec![1, 2],
        },
        EncodeSourceRow {
            contract_id: 2,
            payload_id: 0,
            retained_token_ids: vec![2],
        },
    ];
    let contracts = (0..3)
        .map(|id| EncodeContractRow {
            contract_id: id,
            chain_id: id,
            source_doc_id: id,
            payload_id: 0,
            weight: 1,
        })
        .collect::<Vec<_>>();
    write_encode_artifacts_with_contracts(
        &features,
        &sources,
        &[EncodePayloadRow {
            template_terms: vec![(1, 1)],
            content_terms: vec![(2, 1)],
        }],
        &contracts,
    )
    .unwrap();
    compile_base_equivalent(
        &[
            AtomSketch {
                template_simhash: 0,
                content_simhash: 0,
                template_anchors: vec![10],
                content_anchors: vec![20],
                has_template_terms: true,
                has_content_terms: true,
            },
            AtomSketch {
                template_simhash: 0x5555_5555_5555_5555,
                content_simhash: 0x5555_5555_5555_5555,
                template_anchors: vec![11],
                content_anchors: vec![21],
                has_template_terms: true,
                has_content_terms: true,
            },
            AtomSketch {
                template_simhash: u64::MAX,
                content_simhash: u64::MAX,
                template_anchors: vec![12],
                content_anchors: vec![22],
                has_template_terms: true,
                has_content_terms: true,
            },
        ],
        &BlockingCompileConfig {
            max_routing_block_members: 10,
        },
        &blocking,
    )
    .unwrap();
    commit_ready(
        &features,
        "features.ready",
        r#"{"schema_revision":3,"source_count":3,"payload_count":1,"chains":["a","b","c"],"chain_totals":[{"name":"a","contracts":10,"nfts":100},{"name":"b","contracts":20,"nfts":200},{"name":"c","contracts":30,"nfts":300}]}"#,
    )
    .unwrap();
    commit_ready(
        &blocking,
        "blocking.ready",
        r#"{"blocking_revision":3,"atom_count":3}"#,
    )
    .unwrap();
    let result = metadata_engine::pipeline::run_metadata_pipeline(
        &features,
        &blocking,
        &dir.path().join("m"),
        &metadata_engine::pipeline::MetadataPipelineConfig {
            storage_work_directory: dir.path().to_path_buf(),
            memory_hard_top: MATCH_HARD_TOP,
            host_total_memory: 512 * GIB,
            threads: 1,
            max_catalog_jobs: 100,
            max_candidate_pair_visits: 1_000_000,
            exact_sample_lefts: 0,
            exact_pair_work: 0,
            evidence_gate_policy: EvidenceGatePolicy::permissive(),
            edge_bytes: 1_000_000,
        },
    )
    .unwrap();
    assert_eq!(result.edge_count, 4);
    assert_eq!(result.scope_components.intra_roots, vec![0, 1, 2]);
    assert_eq!(result.scope_components.cross_roots, vec![0, 0, 0]);
    let pairs = &result.scope_components.chain_pair_roots;
    assert_eq!(pairs[0].roots, vec![0, 0, 2]);
    assert_eq!(pairs[1].roots, vec![0, 1, 2]);
    assert_eq!(pairs[2].roots, vec![0, 1, 1]);
    let cross_a = result
        .summary_rows
        .iter()
        .find(|row| row.scope == "cross_chain_summary" && row.primary_chain == "a")
        .unwrap();
    assert_eq!(
        (
            cross_a.total_contracts,
            cross_a.group_count,
            cross_a.duplicate_contract_count
        ),
        (10, 1, 1)
    );
    let matrix_ac = result
        .summary_rows
        .iter()
        .find(|row| {
            row.scope == "chain_matrix" && row.primary_chain == "a" && row.secondary_chain == "c"
        })
        .unwrap();
    assert_eq!(matrix_ac.group_count, 0);
}

#[test]
fn summary_preserves_selected_chains_without_eligible_metadata_contracts() {
    let dir = tempfile::tempdir().unwrap();
    let features = dir.path().join("e");
    let blocking = dir.path().join("b");
    write_encode_artifacts_with_contracts_and_atoms(
        &features,
        &[EncodeSourceRow {
            contract_id: 0,
            payload_id: 0,
            retained_token_ids: vec![1],
        }],
        &[EncodePayloadRow {
            template_terms: vec![(1, 1)],
            content_terms: vec![(2, 1)],
        }],
        &[EncodeContractRow {
            contract_id: 0,
            chain_id: 0,
            source_doc_id: 0,
            payload_id: 0,
            weight: 1,
        }],
        &[vec![0]],
    )
    .unwrap();
    compile_base_equivalent(
        &[AtomSketch {
            template_simhash: 1,
            content_simhash: 2,
            template_anchors: vec![1],
            content_anchors: vec![2],
            has_template_terms: true,
            has_content_terms: true,
        }],
        &BlockingCompileConfig {
            max_routing_block_members: 10,
        },
        &blocking,
    )
    .unwrap();
    commit_ready(
        &features,
        "features.ready",
        r#"{"schema_revision":3,"source_count":1,"payload_count":1,"chains":["a","b","c"],"chain_totals":[{"name":"a","contracts":1,"nfts":1},{"name":"b","contracts":20,"nfts":200},{"name":"c","contracts":30,"nfts":300}]}"#,
    )
    .unwrap();
    commit_ready(
        &blocking,
        "blocking.ready",
        r#"{"blocking_revision":3,"atom_count":1}"#,
    )
    .unwrap();

    let result = metadata_engine::pipeline::run_metadata_pipeline(
        &features,
        &blocking,
        &dir.path().join("m"),
        &metadata_engine::pipeline::MetadataPipelineConfig {
            storage_work_directory: dir.path().to_path_buf(),
            memory_hard_top: MATCH_HARD_TOP,
            host_total_memory: 512 * GIB,
            threads: 1,
            max_catalog_jobs: 100,
            max_candidate_pair_visits: 100,
            exact_sample_lefts: 0,
            exact_pair_work: 0,
            evidence_gate_policy: EvidenceGatePolicy::permissive(),
            edge_bytes: 1_000_000,
        },
    )
    .unwrap();

    for (chain, contracts, nfts) in [("a", 1, 1), ("b", 20, 200), ("c", 30, 300)] {
        let row = result
            .summary_rows
            .iter()
            .find(|row| row.scope == "cross_chain_summary" && row.primary_chain == chain)
            .unwrap_or_else(|| panic!("missing summary row for selected chain {chain}"));
        assert_eq!((row.total_contracts, row.total_nfts), (contracts, nfts));
    }
    assert_eq!(result.scope_components.chain_pair_roots.len(), 3);
}
