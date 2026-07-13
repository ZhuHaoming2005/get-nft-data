use super::*;

#[test]
fn disabled_progress_tracker_is_noop() {
    let progress = ProgressTracker::for_pipeline_stage(PipelineStage::Prepare, false);

    progress.start_stage("phase", 1);
    progress.add_work(1);
    progress.step_stage("step");
    progress.set_message("message");
    progress.finish_stage("done");
    progress.start_task("task", Some(1), "rows");
    progress.advance_task(1, ProgressCounters::default());
    progress.finish_task("task done");
    progress.fail("ignored");
    progress.finish();
}

#[test]
fn hierarchical_progress_tracks_pipeline_stage_and_task_independently() {
    let progress = ProgressTracker::for_pipeline_stage(PipelineStage::Metadata, true);

    let ProgressTracker::Enabled {
        pipeline,
        stage,
        task,
        metrics,
        ..
    } = &progress
    else {
        panic!("progress must be enabled");
    };
    assert_eq!(pipeline.length(), Some(4));
    assert_eq!(pipeline.position(), 2);
    assert_eq!(pipeline.message(), "metadata");

    progress.start_stage("shared-token matching", 6);
    assert_eq!(pipeline.message(), "metadata");
    progress.step_stage("metadata documents loaded");
    assert_eq!(stage.length(), Some(6));
    assert_eq!(stage.position(), 1);

    progress.start_task("shared-token memberships", Some(100), "rows");
    progress.advance_task(
        25,
        ProgressCounters {
            groups: 2,
            candidates: 300,
            scored: 40,
            matched: 7,
        },
    );
    assert_eq!(task.length(), Some(100));
    assert_eq!(task.position(), 25);
    assert_eq!(task.message(), "shared-token memberships");
    assert!(metrics.message().contains("groups 2"));
    assert!(metrics.message().contains("candidates 300"));
    assert!(metrics.message().contains("scored 40"));
    assert!(metrics.message().contains("matched 7"));

    progress.finish_task("shared-token matching complete");
    progress.finish_stage("metadata complete");
    progress.finish_pipeline_stage("metadata complete");
    assert_eq!(pipeline.position(), 3);
}

#[test]
fn progress_layout_keeps_fixed_bars_separate_from_long_metrics() {
    assert!(pipeline_bar_template().contains("{bar:24"));
    assert!(stage_bar_template().contains("{bar:28"));
    assert!(task_bar_template().contains("{bar:32"));
    assert!(!pipeline_bar_template().contains("wide_bar"));
    assert!(!stage_bar_template().contains("wide_bar"));
    assert!(!task_bar_template().contains("wide_bar"));
    assert!(metrics_template().contains("metrics"));
}

#[test]
fn hierarchical_progress_can_move_to_finalize_without_recreating_state() {
    let progress = ProgressTracker::for_pipeline_stage(PipelineStage::Prepare, true);
    progress.set_pipeline_stage(PipelineStage::Finalize);

    let ProgressTracker::Enabled { pipeline, .. } = &progress else {
        panic!("progress must be enabled");
    };
    assert_eq!(pipeline.position(), 3);
    assert_eq!(pipeline.message(), "finalize outputs");
}

#[test]
fn task_progress_message_uses_stable_units_for_throughput_and_eta() {
    let message = format_task_progress_message(&TaskProgressSnapshot {
        label: "shared-token memberships",
        position: 25,
        total: Some(100),
        unit: "rows",
        counters: ProgressCounters {
            groups: 2,
            candidates: 300,
            scored: 40,
            matched: 7,
        },
        elapsed: std::time::Duration::from_secs(2),
    });

    assert_eq!(
        message,
        "shared-token memberships; 25/100 rows; 12.5 rows/s; ETA 6s; groups 2; candidates 300; scored 40; matched 7"
    );
}

#[test]
fn task_progress_message_keeps_unknown_work_indeterminate() {
    let message = format_task_progress_message(&TaskProgressSnapshot {
        label: "building metadata index",
        position: 9,
        total: None,
        unit: "docs",
        counters: ProgressCounters::default(),
        elapsed: std::time::Duration::from_secs(3),
    });

    assert_eq!(
        message,
        "building metadata index; 9 docs; 3.0 docs/s; ETA n/a"
    );
}

#[test]
fn hierarchical_progress_finishes_all_levels_with_failure_context() {
    let progress = ProgressTracker::for_pipeline_stage(PipelineStage::Metadata, true);
    progress.start_stage("shared-token matching", 1);
    progress.start_task("membership rows", Some(10), "rows");
    progress.fail("metadata query failed");

    let ProgressTracker::Enabled {
        pipeline,
        stage,
        task,
        metrics,
        ..
    } = &progress
    else {
        panic!("progress must be enabled");
    };
    assert!(pipeline.is_finished());
    assert!(stage.is_finished());
    assert!(task.is_finished());
    assert!(metrics.is_finished());
    assert!(pipeline.message().contains("FAILED"));
    assert!(pipeline.message().contains("metadata query failed"));
}
