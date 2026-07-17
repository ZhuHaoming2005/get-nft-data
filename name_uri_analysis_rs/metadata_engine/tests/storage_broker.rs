#[test]
fn reserve_accounts_for_partial_peak_overlap() {
    let dir = tempfile::tempdir().unwrap();
    let mut broker = metadata_engine::storage::StorageBroker::open(dir.path()).unwrap();
    let lease = broker
        .reserve(
            metadata_engine::storage::ArtifactClass::Feature,
            1_000,
            2_000, // partial peak
        )
        .unwrap();
    assert_eq!(broker.snapshot().committed_partial_peak_bytes, 2_000);
    drop(lease);
}

#[test]
fn batch_registration_and_pinning_commit_one_consistent_ledger() {
    use metadata_engine::storage::{ArtifactClass, ArtifactRegistration};

    let dir = tempfile::tempdir().unwrap();
    let first = dir.path().join("first.bin");
    let second = dir.path().join("second.bin");
    std::fs::write(&first, b"a").unwrap();
    std::fs::write(&second, b"b").unwrap();
    let mut broker = metadata_engine::storage::StorageBroker::open(dir.path()).unwrap();
    broker
        .register_batch(vec![
            ArtifactRegistration::new(first.clone(), ArtifactClass::Feature, 1, 0, vec![]),
            ArtifactRegistration::new(second.clone(), ArtifactClass::Blocking, 1, 0, vec![]),
        ])
        .unwrap();
    let pins = broker
        .pin_batch(&[first, second], "metadata_encode_complete")
        .unwrap();
    assert_eq!(pins.len(), 2);
    let snapshot = broker.snapshot();
    assert_eq!(snapshot.committed_bytes, 2);
    assert_eq!(snapshot.pinned_bytes, 2);
}

#[test]
fn pin_blocks_eviction_until_owner_checkpoint_releases() {
    let dir = tempfile::tempdir().unwrap();
    let mut broker = metadata_engine::storage::StorageBroker::open(dir.path()).unwrap();
    let path = dir.path().join("features.bin");
    std::fs::write(&path, b"x").unwrap();
    broker
        .register(
            &path,
            metadata_engine::storage::ArtifactClass::Feature,
            1,
            1,
            &[],
        )
        .unwrap();
    let _pin = broker.pin(&path, "metadata_encode_complete").unwrap();
    broker.mark_evictable(&path, "test").unwrap();
    let plan = broker.plan_evict(1).unwrap();
    assert!(plan.paths.is_empty(), "pinned artifacts must stay");
}

#[test]
fn registered_dependency_blocks_eviction_even_when_dependency_is_marked_evictable() {
    let dir = tempfile::tempdir().unwrap();
    let feature = dir.path().join("feature.bin");
    let index = dir.path().join("index.bin");
    std::fs::write(&feature, b"f").unwrap();
    std::fs::write(&index, b"i").unwrap();
    let mut broker = metadata_engine::storage::StorageBroker::open(dir.path()).unwrap();
    broker
        .register(
            &feature,
            metadata_engine::storage::ArtifactClass::Feature,
            1,
            0,
            &[],
        )
        .unwrap();
    broker
        .register(
            &index,
            metadata_engine::storage::ArtifactClass::Index,
            1,
            0,
            &[&feature.to_string_lossy()],
        )
        .unwrap();
    broker.mark_evictable(&feature, "test").unwrap();
    assert!(broker.plan_evict(1).unwrap().paths.is_empty());
    broker
        .commit_evict(&metadata_engine::storage::EvictionPlan {
            paths: vec![feature.clone()],
        })
        .unwrap();
    assert!(feature.exists(), "forced plans must respect dependencies");
}

#[test]
fn encode_checkpoint_does_not_evict_match_inputs() {
    // Register an evictable payload CAS without declaring Match independence.
    // plan_evict / commit_evict must not delete or include that CAS path.
    // After an explicit declaration, eviction of CAS may be allowed.
    let dir = tempfile::tempdir().unwrap();
    let mut broker = metadata_engine::storage::StorageBroker::open(dir.path()).unwrap();

    let cas_path = dir.path().join("payload_cas").join("pack-000.bin");
    std::fs::create_dir_all(cas_path.parent().unwrap()).unwrap();
    std::fs::write(&cas_path, b"cas-bytes").unwrap();

    broker
        .register(
            &cas_path,
            metadata_engine::storage::ArtifactClass::PayloadCas,
            9,
            0,
            &[],
        )
        .unwrap();
    broker
        .mark_evictable(&cas_path, "encode_marked_rebuildable")
        .unwrap();

    let plan = broker.plan_evict(1).unwrap();
    assert!(
        plan.paths.is_empty(),
        "CAS must not enter an eviction plan without Match independence"
    );

    let available = broker.commit_evict(&plan).unwrap();
    let _ = available;
    assert!(
        cas_path.exists(),
        "commit_evict must leave active Match CAS inputs on disk"
    );

    broker.declare_match_independence(&cas_path).unwrap();
    let plan_after = broker.plan_evict(1).unwrap();
    assert_eq!(
        plan_after.paths,
        vec![cas_path.clone()],
        "CAS may enter eviction plan only after Match independence"
    );
    broker.commit_evict(&plan_after).unwrap();
    assert!(
        !cas_path.exists(),
        "CAS may be deleted only after Match independence"
    );
}

#[test]
fn reserve_reduces_available_and_cas_without_independence_survives_forced_commit() {
    let dir = tempfile::tempdir().unwrap();
    let mut broker =
        metadata_engine::storage::StorageBroker::open_with_physical_free(dir.path(), 10_000)
            .unwrap();
    assert_eq!(broker.available_after_evict(), 10_000);

    let lease = broker
        .reserve(
            metadata_engine::storage::ArtifactClass::Feature,
            1_000,
            2_000,
        )
        .unwrap();
    // physical 10000 − committed 1000 − safety 0 − partial 2000 = 7000
    assert_eq!(broker.available_after_evict(), 7_000);
    assert_eq!(broker.snapshot().committed_bytes, 1_000);
    assert_eq!(broker.snapshot().committed_partial_peak_bytes, 2_000);
    drop(lease);
    assert_eq!(
        broker.available_after_evict(),
        10_000,
        "lease Drop must restore available"
    );

    let cas_path = dir.path().join("payload_cas").join("pack-forced.bin");
    std::fs::create_dir_all(cas_path.parent().unwrap()).unwrap();
    std::fs::write(&cas_path, b"keep-me").unwrap();
    broker
        .register(
            &cas_path,
            metadata_engine::storage::ArtifactClass::PayloadCas,
            7,
            0,
            &[],
        )
        .unwrap();
    // Registered bytes remain visible in ledger metrics but are already part of
    // the filesystem free-space reading and therefore are not subtracted again.
    assert_eq!(broker.available_after_evict(), 10_000);
    broker.mark_evictable(&cas_path, "encode_wants_gc").unwrap();
    assert_eq!(
        broker.available_after_evict(),
        10_000,
        "mark_evictable alone must not free bytes"
    );
    assert_eq!(broker.snapshot().reclaimable_bytes, 0);

    let forged = metadata_engine::storage::EvictionPlan {
        paths: vec![cas_path.clone()],
    };
    let after = broker.commit_evict(&forged).unwrap();
    assert!(
        cas_path.exists(),
        "commit_evict must not delete CAS without Match independence"
    );
    assert_eq!(
        after, 10_000,
        "forced plan must leave CAS committed and available unchanged"
    );
    assert_eq!(broker.available_after_evict(), 10_000);
}

#[test]
fn registered_artifacts_are_not_subtracted_from_current_filesystem_free_twice() {
    let dir = tempfile::tempdir().unwrap();
    let artifact = dir.path().join("already-on-disk.bin");
    std::fs::write(&artifact, vec![0u8; 128]).unwrap();
    let mut broker =
        metadata_engine::storage::StorageBroker::open_with_physical_free(dir.path(), 10_000)
            .unwrap();

    broker
        .register(
            &artifact,
            metadata_engine::storage::ArtifactClass::Feature,
            128,
            0,
            &[],
        )
        .unwrap();

    assert_eq!(
        broker.available_after_evict(),
        10_000,
        "the free-space reading already reflects registered on-disk artifacts"
    );
    let lease = broker
        .reserve(metadata_engine::storage::ArtifactClass::Index, 1_000, 500)
        .unwrap();
    assert_eq!(broker.available_after_evict(), 8_500);
    drop(lease);
    assert_eq!(broker.available_after_evict(), 10_000);
}

#[test]
fn checkpoint_pin_survives_process_lease_drop_until_explicit_release() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("features");
    std::fs::create_dir_all(&path).unwrap();
    std::fs::write(path.join("data.bin"), b"feature").unwrap();

    let mut broker = metadata_engine::storage::StorageBroker::open(dir.path()).unwrap();
    broker
        .register(
            &path,
            metadata_engine::storage::ArtifactClass::Feature,
            7,
            0,
            &[],
        )
        .unwrap();
    let pin = broker.pin(&path, "metadata_encode_complete").unwrap();
    pin.persist().unwrap();
    drop(broker);

    let mut reopened = metadata_engine::storage::StorageBroker::open(dir.path()).unwrap();
    reopened.mark_evictable(&path, "test").unwrap();
    assert!(reopened.plan_evict(1).unwrap().paths.is_empty());

    reopened
        .release_pin(&path, "metadata_encode_complete")
        .unwrap();
    assert_eq!(reopened.plan_evict(1).unwrap().paths, vec![path]);
}

#[test]
fn retiring_checkpoint_unpins_and_marks_products_evictable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("index-1");
    std::fs::create_dir_all(&path).unwrap();
    let mut broker = metadata_engine::storage::StorageBroker::open(dir.path()).unwrap();
    broker
        .register(
            &path,
            metadata_engine::storage::ArtifactClass::Index,
            1,
            0,
            &[],
        )
        .unwrap();
    broker
        .pin(&path, "metadata_complete")
        .unwrap()
        .persist()
        .unwrap();
    assert_eq!(
        broker
            .retire_checkpoint_artifacts("metadata_complete", "revision changed")
            .unwrap(),
        1
    );
    assert_eq!(broker.plan_evict(1).unwrap().paths, vec![path]);
}

#[test]
fn recovery_can_register_the_same_artifact_idempotently() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("features");
    std::fs::create_dir_all(&path).unwrap();

    let mut broker = metadata_engine::storage::StorageBroker::open(dir.path()).unwrap();
    broker
        .register(
            &path,
            metadata_engine::storage::ArtifactClass::Feature,
            10,
            2,
            &[],
        )
        .unwrap();
    broker
        .register(
            &path,
            metadata_engine::storage::ArtifactClass::Feature,
            12,
            3,
            &[],
        )
        .unwrap();

    assert_eq!(broker.snapshot().committed_bytes, 12);
}

#[test]
fn commit_evict_removes_registered_artifact_directories() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("blocking-1");
    std::fs::create_dir_all(&path).unwrap();
    std::fs::write(path.join("members.u32"), b"data").unwrap();

    let mut broker = metadata_engine::storage::StorageBroker::open(dir.path()).unwrap();
    broker
        .register(
            &path,
            metadata_engine::storage::ArtifactClass::Blocking,
            4,
            0,
            &[],
        )
        .unwrap();
    broker.mark_evictable(&path, "superseded").unwrap();
    let plan = broker.plan_evict(1).unwrap();
    broker.commit_evict(&plan).unwrap();

    assert!(!path.exists());
}

#[test]
fn reservation_does_not_preflight_physical_space() {
    let dir = tempfile::tempdir().unwrap();
    let mut broker =
        metadata_engine::storage::StorageBroker::open_with_physical_free(dir.path(), 1_000)
            .unwrap();

    let lease = broker
        .reserve(metadata_engine::storage::ArtifactClass::Feature, 800, 300)
        .unwrap();
    assert_eq!(broker.snapshot().committed_bytes, 800);
    drop(lease);
}

#[test]
fn reserve_keeps_safe_evictable_artifacts_without_space_preflight() {
    let dir = tempfile::tempdir().unwrap();
    let stale = dir.path().join("stale-index.bin");
    std::fs::write(&stale, vec![7u8; 600]).unwrap();
    let mut broker =
        metadata_engine::storage::StorageBroker::open_with_physical_free(dir.path(), 1_000)
            .unwrap();
    broker
        .register(
            &stale,
            metadata_engine::storage::ArtifactClass::Index,
            600,
            0,
            &[],
        )
        .unwrap();
    broker.mark_evictable(&stale, "superseded").unwrap();

    let lease = broker
        .reserve(metadata_engine::storage::ArtifactClass::Summary, 1_100, 0)
        .unwrap();

    assert!(stale.exists());
    assert_eq!(broker.snapshot().committed_bytes, 1_700);
    drop(lease);
}

#[test]
fn reserve_accepts_large_request_without_preflight_and_keeps_evictable_cache() {
    let dir = tempfile::tempdir().unwrap();
    let stale = dir.path().join("small-stale-index.bin");
    std::fs::write(&stale, vec![3u8; 100]).unwrap();
    let mut broker =
        metadata_engine::storage::StorageBroker::open_with_physical_free(dir.path(), 1_000)
            .unwrap();
    broker
        .register(
            &stale,
            metadata_engine::storage::ArtifactClass::Index,
            100,
            0,
            &[],
        )
        .unwrap();
    broker.mark_evictable(&stale, "superseded").unwrap();

    let lease = broker
        .reserve(metadata_engine::storage::ArtifactClass::Summary, 1_500, 0)
        .unwrap();
    assert!(stale.exists());
    drop(lease);
}

#[test]
fn production_broker_does_not_probe_filesystem_free_space() {
    let dir = tempfile::tempdir().unwrap();
    let broker = metadata_engine::storage::StorageBroker::open(dir.path()).unwrap();
    let before = broker.snapshot().physical_free_bytes;
    std::fs::write(dir.path().join("ordinary-write.bin"), b"actual write").unwrap();
    let after = broker.snapshot().physical_free_bytes;

    assert_eq!(before, 0);
    assert_eq!(after, 0);
}

#[test]
fn reopening_broker_reclaims_reservations_left_by_a_crashed_process() {
    let dir = tempfile::tempdir().unwrap();
    let mut broker =
        metadata_engine::storage::StorageBroker::open_with_physical_free(dir.path(), 10_000)
            .unwrap();
    let lease = broker
        .reserve(
            metadata_engine::storage::ArtifactClass::Feature,
            4_000,
            1_000,
        )
        .unwrap();
    std::mem::forget(lease); // model process termination: Drop never executes
    drop(broker);

    let reopened =
        metadata_engine::storage::StorageBroker::open_with_physical_free(dir.path(), 10_000)
            .unwrap();
    assert_eq!(reopened.snapshot().committed_bytes, 0);
    assert_eq!(reopened.snapshot().committed_partial_peak_bytes, 0);
}

#[test]
fn invalidating_checkpoint_releases_all_of_its_durable_pins() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("features.bin");
    std::fs::write(&path, b"x").unwrap();
    let mut broker = metadata_engine::storage::StorageBroker::open(dir.path()).unwrap();
    broker
        .register(
            &path,
            metadata_engine::storage::ArtifactClass::Feature,
            1,
            0,
            &[],
        )
        .unwrap();
    broker
        .pin(&path, "metadata_encode_complete")
        .unwrap()
        .persist()
        .unwrap();
    assert_eq!(broker.snapshot().pinned_bytes, 1);
    assert_eq!(
        broker
            .release_checkpoint_pins("metadata_encode_complete")
            .unwrap(),
        1
    );
    assert_eq!(broker.snapshot().pinned_bytes, 0);
}

#[test]
fn registration_refuses_an_artifact_outside_the_work_root() {
    let temp = tempfile::tempdir().unwrap();
    let work = temp.path().join("work");
    let outside = temp.path().join("outside.bin");
    std::fs::write(&outside, b"do-not-delete").unwrap();

    let mut broker = metadata_engine::storage::StorageBroker::open(&work).unwrap();
    let error = broker
        .register(
            &outside,
            metadata_engine::storage::ArtifactClass::Index,
            13,
            0,
            &[],
        )
        .unwrap_err();

    assert!(error.to_string().contains("outside storage work root"));
    assert_eq!(std::fs::read(&outside).unwrap(), b"do-not-delete");
}

#[test]
fn commit_evict_refuses_an_outside_path_in_a_forged_ledger() {
    let temp = tempfile::tempdir().unwrap();
    let work = temp.path().join("work");
    let inside = work.join("artifact.bin");
    let outside = temp.path().join("outside.bin");
    std::fs::create_dir_all(&work).unwrap();
    std::fs::write(&inside, b"inside").unwrap();
    std::fs::write(&outside, b"do-not-delete").unwrap();
    let mut broker = metadata_engine::storage::StorageBroker::open(&work).unwrap();
    broker
        .register(
            &inside,
            metadata_engine::storage::ArtifactClass::Index,
            6,
            0,
            &[],
        )
        .unwrap();
    drop(broker);

    let ledger_path = work.join("storage-ledger.json");
    let mut ledger: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&ledger_path).unwrap()).unwrap();
    let artifacts = ledger["artifacts"].as_object_mut().unwrap();
    let inside_key = artifacts.keys().next().unwrap().clone();
    let mut forged = artifacts.remove(&inside_key).unwrap();
    forged["evictable"] = true.into();
    forged["evict_reason"] = "forged".into();
    artifacts.insert(outside.to_string_lossy().into_owned(), forged);
    std::fs::write(&ledger_path, serde_json::to_vec_pretty(&ledger).unwrap()).unwrap();

    let mut broker = metadata_engine::storage::StorageBroker::open(&work).unwrap();
    let error = broker
        .commit_evict(&metadata_engine::storage::EvictionPlan {
            paths: vec![outside.clone()],
        })
        .unwrap_err();

    assert!(error.to_string().contains("outside storage work root"));
    assert_eq!(std::fs::read(&outside).unwrap(), b"do-not-delete");
}

#[test]
fn reopening_finishes_an_eviction_that_crashed_after_the_tombstone_rename() {
    let temp = tempfile::tempdir().unwrap();
    let work = temp.path().join("work");
    let artifact = work.join("artifacts/stale-index.bin");
    std::fs::create_dir_all(artifact.parent().unwrap()).unwrap();
    std::fs::write(&artifact, b"gone").unwrap();

    let mut broker = metadata_engine::storage::StorageBroker::open(&work).unwrap();
    broker
        .register(
            &artifact,
            metadata_engine::storage::ArtifactClass::Index,
            4,
            0,
            &[],
        )
        .unwrap();
    broker.mark_evictable(&artifact, "stale").unwrap();
    drop(broker);

    let transaction_dir = work.join(".storage-evictions");
    std::fs::create_dir_all(&transaction_dir).unwrap();
    let tombstone = transaction_dir.join("pending.tombstone");
    std::fs::rename(&artifact, &tombstone).unwrap();
    let journal = transaction_dir.join("pending.json");
    std::fs::write(
        &journal,
        serde_json::to_vec(&serde_json::json!({
            "revision": 1,
            "ledger_key": artifact.to_string_lossy(),
            "original": artifact,
            "tombstone": tombstone,
            "logical_bytes": 4
        }))
        .unwrap(),
    )
    .unwrap();

    let reopened = metadata_engine::storage::StorageBroker::open(&work).unwrap();

    assert!(!journal.exists());
    assert!(!tombstone.exists());
    assert_eq!(reopened.snapshot().committed_bytes, 0);
}

#[test]
fn reopening_rolls_back_an_eviction_that_crashed_before_the_tombstone_rename() {
    let temp = tempfile::tempdir().unwrap();
    let work = temp.path().join("work");
    let artifact = work.join("artifacts/kept-index.bin");
    std::fs::create_dir_all(artifact.parent().unwrap()).unwrap();
    std::fs::write(&artifact, b"keep").unwrap();

    let mut broker = metadata_engine::storage::StorageBroker::open(&work).unwrap();
    broker
        .register(
            &artifact,
            metadata_engine::storage::ArtifactClass::Index,
            4,
            0,
            &[],
        )
        .unwrap();
    broker.mark_evictable(&artifact, "stale").unwrap();
    drop(broker);

    let transaction_dir = work.join(".storage-evictions");
    std::fs::create_dir_all(&transaction_dir).unwrap();
    let tombstone = transaction_dir.join("pending.tombstone");
    let journal = transaction_dir.join("pending.json");
    std::fs::write(
        &journal,
        serde_json::to_vec(&serde_json::json!({
            "revision": 1,
            "ledger_key": artifact.to_string_lossy(),
            "original": artifact,
            "tombstone": tombstone,
            "logical_bytes": 4
        }))
        .unwrap(),
    )
    .unwrap();

    let reopened = metadata_engine::storage::StorageBroker::open(&work).unwrap();

    assert!(!journal.exists());
    assert!(artifact.exists());
    assert_eq!(reopened.snapshot().committed_bytes, 4);
}
