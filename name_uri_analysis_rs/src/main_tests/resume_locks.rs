use super::*;

#[test]
fn metadata_stage_revision_tracks_conservative_fallback_semantics() {
    assert_eq!(StageRevisions::current().metadata, 4);
    assert_eq!(StageRevisions::current().prepare, 1);
    assert_eq!(StageRevisions::current().name, 1);
}

#[test]
fn output_directory_cannot_be_deleted_with_work_directory() {
    let directory = tempfile::tempdir().unwrap();
    let work = directory.path().join("work");

    let error = validate_directory_layout(&work, &work.join("output")).unwrap_err();

    assert!(error.to_string().contains("inside --work-directory"));
}

#[test]
fn output_containment_normalizes_parent_components() {
    let directory = tempfile::tempdir().unwrap();
    let work = directory.path().join("work");
    fs::create_dir_all(&work).unwrap();
    let disguised_child = directory
        .path()
        .join("other")
        .join("..")
        .join("work")
        .join("output");

    let error = validate_directory_layout(&work, &disguised_child).unwrap_err();

    assert!(error.to_string().contains("inside --work-directory"));
}

#[cfg(windows)]
#[test]
fn output_containment_resolves_directory_symlinks() {
    let directory = tempfile::tempdir().unwrap();
    let work = directory.path().join("work");
    let alias = directory.path().join("work-alias");
    fs::create_dir_all(&work).unwrap();
    std::os::windows::fs::symlink_dir(&work, &alias).unwrap();

    let error = validate_directory_layout(&work, &alias.join("output")).unwrap_err();

    assert!(error.to_string().contains("inside --work-directory"));
}

#[test]
fn controller_lock_rejects_a_concurrent_owner_and_releases_on_drop() {
    let temp = tempfile::tempdir().unwrap();
    let work = temp.path().join("work");
    let first = ControllerLock::acquire(&work).unwrap();

    let error = ControllerLock::acquire(&work).unwrap_err();
    assert!(error.to_string().contains("already controlled"));

    drop(first);
    ControllerLock::acquire(&work).unwrap();
}

#[test]
fn controller_lock_reuses_stale_metadata_without_replacing_the_file() {
    let temp = tempfile::tempdir().unwrap();
    let work = temp.path().join("work");
    let lock = temp.path().join(".work.name-uri-analysis.lock");
    let alias = temp.path().join("controller-lock-alias");
    fs::write(&lock, format!("{} 0", u32::MAX)).unwrap();
    fs::hard_link(&lock, &alias).unwrap();

    let acquired = ControllerLock::acquire(&work).unwrap();

    assert!(lock.is_file());
    drop(acquired);
    assert_eq!(fs::read(&lock).unwrap(), fs::read(&alias).unwrap());
    assert!(lock.is_file());
}

#[test]
fn phase_lock_blocks_controller_probe_until_the_phase_releases() {
    let temp = tempfile::tempdir().unwrap();
    let work = temp.path().join("work");
    let phase = PhaseLock::acquire(&work).unwrap();

    let error = ensure_phase_idle(&work).unwrap_err();
    assert!(error.to_string().contains("analysis phase is still active"));

    drop(phase);
    ensure_phase_idle(&work).unwrap();
}

#[test]
fn controller_phase_lease_hands_work_to_a_child_and_reclaims_it() {
    let temp = tempfile::tempdir().unwrap();
    let work = temp.path().join("work");
    let mut lease = ControllerPhaseLease::acquire(&work).unwrap();

    assert!(ensure_phase_idle(&work).is_err());
    lease.release_for_child().unwrap();
    let child_phase = PhaseLock::acquire(&work).unwrap();
    drop(child_phase);
    lease.reclaim_after_child().unwrap();

    assert!(ensure_phase_idle(&work).is_err());
}

#[test]
fn phase_generation_rejects_a_stale_waiter_after_controller_replacement() {
    let temp = tempfile::tempdir().unwrap();
    let work = temp.path().join("work");
    let stale_generation = {
        let lease = ControllerPhaseLease::acquire(&work).unwrap();
        lease.generation().to_string()
    };
    let mut current = ControllerPhaseLease::acquire(&work).unwrap();
    let current_generation = current.generation().to_string();
    assert_ne!(stale_generation, current_generation);

    current.release_for_child().unwrap();
    let stale_waiter = PhaseLock::acquire_blocking(&work).unwrap();
    let error = validate_phase_generation(&work, &stale_generation).unwrap_err();
    assert!(error
        .to_string()
        .contains("stale internal phase generation"));
    drop(stale_waiter);

    let current_child = PhaseLock::acquire_blocking(&work).unwrap();
    validate_phase_generation(&work, &current_generation).unwrap();
    drop(current_child);
    current.reclaim_after_child().unwrap();
}

#[test]
fn internal_phase_waits_for_the_controller_to_release_its_lease() {
    let temp = tempfile::tempdir().unwrap();
    let work = temp.path().join("work");
    let controller_phase = PhaseLock::acquire(&work).unwrap();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (acquired_tx, acquired_rx) = std::sync::mpsc::channel();
    let child_work = work.clone();
    let child = thread::spawn(move || {
        started_tx.send(()).unwrap();
        let phase = PhaseLock::acquire_blocking(&child_work).unwrap();
        acquired_tx.send(()).unwrap();
        phase
    });

    started_rx.recv().unwrap();
    assert!(acquired_rx.recv_timeout(Duration::from_millis(50)).is_err());
    drop(controller_phase);
    acquired_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    drop(child.join().unwrap());
}

#[test]
fn parent_liveness_watcher_invokes_callback_on_eof() {
    let callbacks = std::cell::Cell::new(0usize);

    watch_parent_liveness(std::io::Cursor::new(b"parent-alive"), || {
        callbacks.set(callbacks.get() + 1);
    });

    assert_eq!(callbacks.get(), 1);
}

#[test]
fn parent_liveness_watcher_invokes_callback_on_read_error() {
    struct FailedReader;

    impl Read for FailedReader {
        fn read(&mut self, _buffer: &mut [u8]) -> std::io::Result<usize> {
            Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "test disconnect",
            ))
        }
    }

    let disconnected = std::cell::Cell::new(false);
    watch_parent_liveness(FailedReader, || disconnected.set(true));

    assert!(disconnected.get());
}

#[cfg(windows)]
#[test]
fn output_containment_comparison_is_case_insensitive_on_windows() {
    assert!(path_is_same_or_descendant(
        Path::new(r"C:\DATA\WORK\output"),
        Path::new(r"c:\data\work")
    ));
}

#[test]
fn manifest_compatibility_allows_resource_tuning_but_not_semantic_changes() {
    let temp = tempfile::tempdir().unwrap();
    let expected = sample_manifest(temp.path());
    let mut existing = expected.clone();
    existing.stages.get_mut("name_complete").unwrap().complete = true;
    assert!(manifests_have_same_inputs_and_options(&existing, &expected));

    existing.binary_version = "new-compatible-binary".to_string();
    assert!(manifests_have_same_inputs_and_options(&existing, &expected));

    existing.options.threads = 128;
    existing.options.memory_limit = "384GiB".to_string();
    existing.options.analysis_memory_limit = Some("384GiB".to_string());
    existing.options.duckdb_memory_limit = "320GiB".to_string();
    assert!(manifests_have_same_inputs_and_options(&existing, &expected));

    existing.inputs[0].row_count += 1;
    assert!(!manifests_have_same_inputs_and_options(
        &existing, &expected
    ));
    existing = expected.clone();
    existing.options.name_threshold = 96.0;
    assert!(!manifests_have_same_inputs_and_options(
        &existing, &expected
    ));
}

#[test]
fn resume_rebinds_a_stage_compatible_manifest_to_the_current_binary() {
    let temp = tempfile::tempdir().unwrap();
    let work = temp.path().join("work");
    fs::create_dir_all(&work).unwrap();
    let mut existing = sample_manifest(&work);
    existing.binary_version = "old-binary".to_string();
    existing
        .stages
        .get_mut("prepare_complete")
        .unwrap()
        .complete = true;
    let manifest_path = work.join("manifest.json");
    fs::write(&manifest_path, serde_json::to_vec(&existing).unwrap()).unwrap();
    let mut expected = existing.clone();
    expected.binary_version = "new-binary".to_string();
    expected.options.threads = 128;
    expected.options.memory_limit = "384GiB".to_string();
    expected.options.analysis_memory_limit = Some("384GiB".to_string());
    expected.options.duckdb_memory_limit = "320GiB".to_string();

    let (_, rebound) = prepare_work_directory(&work, expected, true).unwrap();
    let persisted: PipelineManifest =
        serde_json::from_slice(&fs::read(manifest_path).unwrap()).unwrap();

    assert_eq!(rebound.binary_version, "new-binary");
    assert_eq!(persisted.binary_version, "new-binary");
    assert_eq!(persisted.options.threads, 128);
    assert_eq!(
        persisted.options.analysis_memory_limit.as_deref(),
        Some("384GiB")
    );
    assert!(persisted.stages["prepare_complete"].complete);
}

#[test]
fn changing_metadata_recall_mode_invalidates_only_metadata_and_finalizer() {
    let temp = tempfile::tempdir().unwrap();
    let work = temp.path().join("work");
    fs::create_dir_all(&work).unwrap();
    let mut existing = sample_manifest(&work);
    existing.options.metadata_recall_mode = MetadataRecallMode::Exact;
    for checkpoint in existing.stages.values_mut() {
        checkpoint.complete = true;
    }
    fs::write(
        work.join("manifest.json"),
        serde_json::to_vec(&existing).unwrap(),
    )
    .unwrap();
    let mut expected = existing.clone();
    expected.options.metadata_recall_mode = MetadataRecallMode::Conservative;

    let (_, resumed) = prepare_work_directory(&work, expected, true).unwrap();

    assert!(resumed.stages["prepare_complete"].complete);
    assert!(resumed.stages["name_complete"].complete);
    assert!(!resumed.stages["metadata_complete"].complete);
    assert!(!resumed.stages["finalized"].complete);
}

#[test]
fn resume_stage_revision_changes_follow_the_dependency_graph() {
    struct Case {
        revisions: serde_json::Value,
        invalidated_stages: &'static [&'static str],
        invalidated_ready_phases: &'static [&'static str],
    }

    let cases = [
        Case {
            revisions: serde_json::json!({
                "prepare": 0,
                "name": 1,
                "metadata": 4,
                "finalizer": 1,
            }),
            invalidated_stages: &[
                "contracts_ready",
                "uri_complete",
                "metadata_compact_ready",
                "prepare_complete",
                "name_complete",
                "metadata_complete",
                "finalized",
            ],
            invalidated_ready_phases: &["prepare", "name", "metadata"],
        },
        Case {
            revisions: serde_json::json!({
                "prepare": 1,
                "name": 0,
                "metadata": 4,
                "finalizer": 1,
            }),
            invalidated_stages: &["name_complete", "finalized"],
            invalidated_ready_phases: &["name"],
        },
        Case {
            revisions: serde_json::json!({
                "prepare": 1,
                "name": 1,
                "metadata": 3,
                "finalizer": 1,
            }),
            invalidated_stages: &["metadata_complete", "finalized"],
            invalidated_ready_phases: &["metadata"],
        },
        Case {
            revisions: serde_json::json!({
                "prepare": 1,
                "name": 1,
                "metadata": 4,
                "finalizer": 0,
            }),
            invalidated_stages: &["finalized"],
            invalidated_ready_phases: &[],
        },
    ];

    for (case_index, case) in cases.into_iter().enumerate() {
        let temp = tempfile::tempdir().unwrap();
        let work = temp.path().join(format!("work-{case_index}"));
        let checkpoints = work.join("checkpoints");
        fs::create_dir_all(&checkpoints).unwrap();
        let mut existing = sample_manifest(&work);
        for checkpoint in existing.stages.values_mut() {
            checkpoint.complete = true;
        }
        let mut serialized = serde_json::to_value(existing).unwrap();
        serialized["stage_revisions"] = case.revisions;
        fs::write(
            work.join("manifest.json"),
            serde_json::to_vec(&serialized).unwrap(),
        )
        .unwrap();
        for phase in ["prepare", "name", "metadata"] {
            fs::write(
                checkpoints.join(format!("{phase}.ready.json")),
                b"stale-ready",
            )
            .unwrap();
        }

        let (_, rebound) = prepare_work_directory(&work, sample_manifest(&work), true).unwrap();

        for (stage, checkpoint) in &rebound.stages {
            let should_be_complete = !case.invalidated_stages.contains(&stage.as_str());
            assert_eq!(
                checkpoint.complete, should_be_complete,
                "unexpected {stage:?} state for case {case_index}"
            );
            if !should_be_complete {
                assert!(checkpoint.artifacts.is_empty());
            }
        }
        for phase in ["prepare", "name", "metadata"] {
            let should_exist = !case.invalidated_ready_phases.contains(&phase);
            assert_eq!(
                checkpoints.join(format!("{phase}.ready.json")).exists(),
                should_exist,
                "unexpected {phase:?} ready checkpoint state for case {case_index}"
            );
        }
    }
}

#[test]
fn legacy_manifest_without_stage_revisions_is_safely_invalidated_and_upgraded() {
    let temp = tempfile::tempdir().unwrap();
    let work = temp.path().join("work");
    fs::create_dir_all(&work).unwrap();
    let mut legacy = sample_manifest(&work);
    for checkpoint in legacy.stages.values_mut() {
        checkpoint.complete = true;
    }
    let mut serialized = serde_json::to_value(legacy).unwrap();
    serialized
        .as_object_mut()
        .unwrap()
        .remove("stage_revisions");
    let manifest_path = work.join("manifest.json");
    fs::write(&manifest_path, serde_json::to_vec(&serialized).unwrap()).unwrap();

    let (_, rebound) = prepare_work_directory(&work, sample_manifest(&work), true).unwrap();
    let persisted: serde_json::Value =
        serde_json::from_slice(&fs::read(manifest_path).unwrap()).unwrap();

    assert!(rebound.stages["input_validated"].complete);
    for stage in [
        "contracts_ready",
        "uri_complete",
        "metadata_compact_ready",
        "prepare_complete",
        "name_complete",
        "metadata_complete",
        "finalized",
    ] {
        assert!(!rebound.stages[stage].complete, "legacy stage {stage:?}");
    }
    assert!(persisted["stage_revisions"].is_object());
}

#[test]
fn completed_checkpoint_rejects_tampered_artifact() {
    let temp = tempfile::tempdir().unwrap();
    let artifact_path = temp.path().join("partial.json");
    fs::write(&artifact_path, b"original").unwrap();
    let mut manifest = sample_manifest(temp.path());
    manifest.stages.insert(
        "name_complete".to_string(),
        StageCheckpoint {
            complete: true,
            artifacts: vec![fingerprint_artifact(&artifact_path).unwrap()],
        },
    );
    assert!(checkpoint_is_complete_and_valid(&manifest, "name_complete", temp.path()).unwrap());

    fs::write(&artifact_path, b"tampered").unwrap();
    let error =
        checkpoint_is_complete_and_valid(&manifest, "name_complete", temp.path()).unwrap_err();
    assert!(error.to_string().contains("changed"));
}

#[test]
fn resume_rejects_missing_database_table_needed_by_next_phase() {
    let temp = tempfile::tempdir().unwrap();
    let mut manifest = sample_manifest(temp.path());
    manifest
        .stages
        .get_mut("prepare_complete")
        .unwrap()
        .complete = true;
    let conn = Connection::open(&manifest.options.database_path).unwrap();
    conn.execute_batch(
        "CREATE TABLE analysis_contracts(id INTEGER);
         CREATE TABLE metadata_rows(id INTEGER);
         CREATE TABLE metadata_contract_token_rows(id INTEGER);
         CREATE TABLE metadata_token_stats(id INTEGER);
         CREATE TABLE selected_chains(chain VARCHAR);",
    )
    .unwrap();
    drop(conn);

    let error = validate_resume_database_for_downstream(&manifest, "prepare_complete").unwrap_err();

    assert!(error.to_string().contains("name_atoms"));
}

#[test]
fn ready_checkpoint_promotes_phase_after_controller_restart() {
    let temp = tempfile::tempdir().unwrap();
    fs::create_dir_all(temp.path().join("partial")).unwrap();
    fs::create_dir_all(temp.path().join("checkpoints")).unwrap();
    let partial = temp.path().join("partial/name-summary.json");
    fs::write(&partial, br#"{"summary_rows":[]}"#).unwrap();
    let fingerprint = fingerprint_artifact(&partial).unwrap();
    let ready = PhaseReady {
        phase: "name".to_string(),
        partial_file: "name-summary.json".to_string(),
        size: fingerprint.size,
        sha256: fingerprint.sha256,
    };
    fs::write(
        temp.path().join("checkpoints/name.ready.json"),
        serde_json::to_vec(&ready).unwrap(),
    )
    .unwrap();
    let mut manifest = sample_manifest(temp.path());

    assert!(promote_ready_phase(
        &mut manifest,
        InternalPhase::Name,
        "name-summary.json",
        temp.path(),
    )
    .unwrap());
    let checkpoint = manifest.stages.get("name_complete").unwrap();
    assert!(checkpoint.complete);
    assert_eq!(checkpoint.artifacts.len(), 1);
}
