#[test]
fn controller_does_not_scan_parquet_before_prepare_phase() {
    let source = include_str!("../main.rs");
    let obsolete_call = ["inspect_selected_", "chains("].concat();
    assert!(!source.contains(&obsolete_call));
}

#[test]
fn controller_holds_phase_lease_before_reading_pipeline_state() {
    let source = include_str!("../main.rs");
    let controller = source.find("let _controller_lock").unwrap();
    let lease = source[controller..]
        .find("ControllerPhaseLease::acquire(&work_directory)")
        .map(|offset| controller + offset)
        .unwrap();
    let fingerprint = source[controller..]
        .find("fingerprint_inputs(&args.parquet_inputs)")
        .map(|offset| controller + offset)
        .unwrap();

    assert!(controller < lease && lease < fingerprint);
}

#[test]
fn internal_phase_locks_work_state_before_reading_the_manifest() {
    let source = include_str!("../controller_child.rs");
    let start = source.find("fn run_internal_phase").unwrap();
    let end = source[start..].find("fn run_child_phase").unwrap() + start;
    let body = &source[start..end];
    let phase_lock = body
        .find("PhaseLock::acquire_blocking(work_directory)")
        .unwrap();
    let generation = body.find("validate_phase_generation_from_env(").unwrap();
    let manifest_read = body.find("fs::read(config_path)").unwrap();

    assert!(phase_lock < generation && generation < manifest_read);
}

#[test]
fn child_phase_keeps_a_private_parent_liveness_pipe_until_wait_finishes() {
    let source = include_str!("../controller_child.rs");
    let start = source.find("fn run_child_phase").unwrap();
    let end = source[start..].find("fn directory_size").unwrap() + start;
    let body = &source[start..end];
    let body = body
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect::<String>();

    assert!(body.contains(".stdin(Stdio::piped())"));
    assert!(body.contains(".env(PARENT_LIVENESS_ENV,\"1\")"));
    assert!(body.contains(".env(PHASE_GENERATION_ENV,phase_lease.generation())"));
    let take = body.find("child.stdin.take()").unwrap();
    let wait = body.find("child.wait()").unwrap();
    let release = body.find("drop(parent_liveness)").unwrap();
    assert!(take < wait && wait < release);
}

#[test]
fn child_phase_hands_off_and_reclaims_the_controller_phase_lease() {
    let source = include_str!("../controller_child.rs");
    let start = source.find("fn run_child_phase").unwrap();
    let end = source[start..].find("fn directory_size").unwrap() + start;
    let body = source[start..end]
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect::<String>();

    let spawn = body.find("command.spawn()?").unwrap();
    let release = body.find("phase_lease.release_for_child()?").unwrap();
    let wait = body.find("child.wait()").unwrap();
    let reclaim = body.find("phase_lease.reclaim_after_child()").unwrap();
    assert!(spawn < release && release < wait && wait < reclaim);
}

#[test]
fn internal_phase_starts_parent_watchdog_before_acquiring_phase_lock() {
    let source = include_str!("../controller_child.rs");
    let start = source.find("fn run_internal_phase").unwrap();
    let end = source[start..].find("fn run_child_phase").unwrap() + start;
    let body = &source[start..end];
    let watchdog = body.find("start_parent_liveness_watchdog()?").unwrap();
    let phase_lock = body
        .find("PhaseLock::acquire_blocking(work_directory)")
        .unwrap();

    assert!(watchdog < phase_lock);
}

#[test]
fn manifest_replacement_never_deletes_the_last_durable_copy_first() {
    let source = include_str!("../controller_manifest.rs");
    let start = source.find("fn write_manifest_atomically").unwrap();
    let end = source[start..].find("fn write_metric_atomically").unwrap() + start;
    let writer = &source[start..end];
    let atomic = include_str!("../atomic_file.rs");
    let atomic_production = atomic.split("#[cfg(test)]").next().unwrap();

    assert!(writer.contains("write_json_atomically"));
    assert!(!writer.contains("remove_file(destination)"));
    assert!(atomic_production.contains("fn replace_file_atomically"));
    assert!(!atomic_production.contains("remove_file(destination)"));
}
