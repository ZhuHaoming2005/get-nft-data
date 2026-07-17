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
    let progress = ProgressTracker::for_pipeline_stage(PipelineStage::MetadataMatch, true);

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
    assert_eq!(pipeline.length(), Some(5));
    assert_eq!(pipeline.position(), 3);
    assert_eq!(pipeline.message(), "metadata match");

    progress.start_stage("shared-token matching", 6);
    assert_eq!(pipeline.message(), "metadata match");
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
            expanded: 80,
            matched: 7,
            selected: 0,
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
    assert_eq!(pipeline.position(), 4);
}

#[test]
fn metadata_match_engine_events_use_a_dynamic_stage_spinner() {
    use metadata_engine::progress::{
        ProgressCounters as EngineCounters, ProgressEvent, ProgressPhase, WorkUnit,
    };

    let progress = ProgressTracker::for_pipeline_stage(PipelineStage::MetadataMatch, true);
    progress.observe_engine_event(ProgressEvent::determinate(
        ProgressPhase::OpenSnapshot,
        0,
        10,
        WorkUnit::Bytes,
        EngineCounters::default(),
    ));

    let ProgressTracker::Enabled { stage, .. } = &progress else {
        panic!("progress must be enabled");
    };
    assert_eq!(stage.length(), None);
    assert_eq!(stage.position(), 0);
    assert_eq!(stage.message(), "open snapshot");

    progress.observe_engine_event(ProgressEvent::determinate(
        ProgressPhase::BuildCatalog,
        0,
        2,
        WorkUnit::Items,
        EngineCounters::default(),
    ));
    assert_eq!(stage.length(), None);
    assert_eq!(stage.position(), 0);
    assert_eq!(stage.message(), "build catalog");
}

#[test]
fn match_elapsed_offset_uses_the_controller_wall_clock_origin() {
    assert_eq!(
        match_elapsed_offset(Some(1_000), Some(3_500)),
        std::time::Duration::from_millis(2_500)
    );
    assert_eq!(
        match_elapsed_offset(Some(3_500), Some(1_000)),
        std::time::Duration::ZERO
    );
    assert_eq!(
        match_elapsed_offset(None, Some(1_000)),
        std::time::Duration::ZERO
    );
}

#[test]
fn match_forecast_parser_accepts_only_the_shared_controller_schema() {
    let current = format!(
        r#"{{"schema_version":{},"sample_count":8,"lower_total_millis":1000,"upper_total_millis":2000}}"#,
        MATCH_ETA_FORECAST_SCHEMA_VERSION
    );
    assert!(parse_match_eta_forecast(&current).is_some());

    let stale = r#"{"schema_version":2,"sample_count":8,"lower_total_millis":1000,"upper_total_millis":2000}"#;
    assert!(parse_match_eta_forecast(stale).is_none());
}

#[test]
fn progress_layout_keeps_fixed_bars_separate_from_long_metrics() {
    assert!(pipeline_bar_template().contains("{bar:20"));
    assert!(pipeline_bar_template().contains("phases {pos}/{len}"));
    assert!(stage_bar_template().contains("{bar:24"));
    assert!(stage_bar_template().contains("steps {pos}/{len}"));
    assert!(!stage_spinner_template().contains("{percent"));
    assert!(!stage_spinner_template().contains("{bar"));
    assert!(task_bar_template().contains("{bar:32"));
    assert!(task_bar_template().contains("{human_pos}/{human_len}"));
    assert!(!pipeline_bar_template().contains("{spinner"));
    assert!(!stage_bar_template().contains("{spinner"));
    assert!(!task_bar_template().contains("{spinner"));
    assert!(!metrics_template().contains("{spinner"));
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
    assert_eq!(pipeline.position(), 4);
    assert_eq!(pipeline.message(), "finalize outputs");
}

#[test]
fn task_progress_message_uses_stable_units_for_throughput_and_eta() {
    let message = format_task_progress_message(&TaskProgressSnapshot {
        position: 25,
        total: Some(100),
        unit: "rows",
        counters: ProgressCounters {
            groups: 2,
            candidates: 300,
            scored: 40,
            expanded: 80,
            matched: 7,
            selected: 0,
        },
        rate: Some(12.5),
        show_match_eta: false,
        total_kind: metadata_engine::progress::TotalKind::Exact,
    });

    assert_eq!(
        message,
        "12.5 rows/s · ETA 6s · groups 2 · candidates 300 · scored 40 · expanded 80 · matched 7"
    );
}

#[test]
fn upper_bound_progress_labels_eta_as_a_ceiling() {
    let message = format_task_progress_message(&TaskProgressSnapshot {
        position: 25,
        total: Some(100),
        unit: "pairs",
        counters: ProgressCounters::default(),
        rate: Some(12.5),
        show_match_eta: false,
        total_kind: metadata_engine::progress::TotalKind::UpperBound,
    });

    assert_eq!(message, "12.5 pairs/s · ETA ≤ 6s");
}

#[test]
fn estimated_progress_labels_eta_as_approximate() {
    let message = format_task_progress_message(&TaskProgressSnapshot {
        position: 25,
        total: Some(100),
        unit: "pairs",
        counters: ProgressCounters::default(),
        rate: Some(12.5),
        show_match_eta: false,
        total_kind: metadata_engine::progress::TotalKind::Estimate,
    });

    assert_eq!(message, "12.5 pairs/s · ETA ~ 6s");
}

#[test]
fn upper_bound_phase_eta_is_not_used_as_a_match_lower_bound() {
    let snapshot = TaskProgressSnapshot {
        position: 25,
        total: Some(100),
        unit: "pairs",
        counters: ProgressCounters::default(),
        rate: Some(12.5),
        show_match_eta: true,
        total_kind: metadata_engine::progress::TotalKind::UpperBound,
    };
    let forecast = MatchEtaForecast {
        schema_version: MATCH_ETA_FORECAST_SCHEMA_VERSION,
        sample_count: 2,
        lower_total_millis: None,
        upper_total_millis: None,
    };

    let message = format_task_progress_message_with_match_forecast(
        &snapshot,
        Some(&forecast),
        std::time::Duration::from_secs(1),
    );

    assert!(message.contains("ETA ≤ 6s"), "{message}");
    assert!(message.contains("match ETA lower n/a"), "{message}");
    assert!(!message.contains("match remaining >="), "{message}");
}

#[test]
fn task_progress_message_keeps_unknown_work_indeterminate() {
    let message = format_task_progress_message(&TaskProgressSnapshot {
        position: 9,
        total: None,
        unit: "docs",
        counters: ProgressCounters::default(),
        rate: Some(3.0),
        show_match_eta: false,
        total_kind: metadata_engine::progress::TotalKind::Unknown,
    });

    assert_eq!(message, "9 docs · 3.0 docs/s · ETA n/a (total unknown)");
}

#[test]
fn unknown_task_immediately_explains_that_eta_is_unavailable() {
    let progress = ProgressTracker::for_pipeline_stage(PipelineStage::Prepare, true);
    progress.start_task("DuckDB aggregation", None, "rows");

    let ProgressTracker::Enabled { metrics, .. } = &progress else {
        panic!("progress must be enabled");
    };
    assert_eq!(metrics.message(), "ETA n/a (work total not observable)");
}

#[test]
fn empty_exact_phase_is_reported_as_skipped_without_rate_or_eta_noise() {
    let message = format_task_progress_message_with_match_forecast(
        &TaskProgressSnapshot {
            position: 0,
            total: Some(0),
            unit: "files",
            counters: ProgressCounters::default(),
            rate: None,
            show_match_eta: true,
            total_kind: metadata_engine::progress::TotalKind::Exact,
        },
        Some(&MatchEtaForecast {
            schema_version: MATCH_ETA_FORECAST_SCHEMA_VERSION,
            sample_count: 8,
            lower_total_millis: Some(10_000),
            upper_total_millis: Some(20_000),
        }),
        std::time::Duration::from_secs(4),
    );

    assert_eq!(message, "skipped (0 files)");
}

#[test]
fn task_rate_estimator_uses_the_full_warmup_window() {
    let mut estimator = TaskRateEstimator::default();
    assert_eq!(estimator.sample(0, std::time::Duration::ZERO), None);
    assert_eq!(
        estimator.sample(10, std::time::Duration::from_millis(500)),
        None
    );
    assert_eq!(
        estimator.sample(10, std::time::Duration::from_secs(1)),
        None
    );
    assert_eq!(
        estimator.sample(20, std::time::Duration::from_millis(1500)),
        None
    );
    assert_eq!(
        estimator.sample(30, std::time::Duration::from_secs(2)),
        Some(15.0)
    );
}

#[test]
fn task_rate_estimator_uses_recent_work_instead_of_lifetime_average() {
    let mut estimator = TaskRateEstimator::default();
    assert_eq!(estimator.sample(0, std::time::Duration::ZERO), None);
    assert_eq!(
        estimator.sample(20, std::time::Duration::from_secs(2)),
        Some(10.0)
    );
    let rate = estimator
        .sample(120, std::time::Duration::from_secs(3))
        .unwrap();
    assert!((rate - 21.646).abs() < 0.01, "{rate}");
}

#[test]
fn task_rate_estimator_decays_during_observed_stalls() {
    let mut estimator = TaskRateEstimator::default();
    assert_eq!(estimator.sample(0, std::time::Duration::ZERO), None);
    assert_eq!(
        estimator.sample(20, std::time::Duration::from_secs(2)),
        Some(10.0)
    );
    let stalled = estimator
        .sample(20, std::time::Duration::from_secs(3))
        .unwrap();
    assert!(stalled < 10.0, "{stalled}");
}

#[test]
fn task_progress_message_uses_the_sampled_rate_for_eta() {
    let message = format_task_progress_message(&TaskProgressSnapshot {
        position: 50,
        total: Some(100),
        unit: "pairs",
        counters: ProgressCounters::default(),
        rate: Some(25.0),
        show_match_eta: false,
        total_kind: metadata_engine::progress::TotalKind::Exact,
    });
    assert_eq!(message, "25.0 pairs/s · ETA 2s");
}

#[test]
fn task_metrics_use_compact_rates_and_grouped_diagnostic_counts() {
    let message = format_task_progress_message(&TaskProgressSnapshot {
        position: 22_097_544,
        total: Some(44_752_896),
        unit: "token groups",
        counters: ProgressCounters {
            selected: 21_830_112,
            ..ProgressCounters::default()
        },
        rate: Some(308_400.0),
        show_match_eta: false,
        total_kind: metadata_engine::progress::TotalKind::Exact,
    });

    assert_eq!(
        message,
        "308.4K token groups/s · ETA 1m 14s · selected 21,830,112 sources"
    );
}

#[test]
fn task_progress_distinguishes_phase_eta_from_uncalibrated_match_eta() {
    let message = format_task_progress_message(&TaskProgressSnapshot {
        position: 50,
        total: Some(100),
        unit: "pairs",
        counters: ProgressCounters::default(),
        rate: Some(25.0),
        show_match_eta: true,
        total_kind: metadata_engine::progress::TotalKind::Exact,
    });

    assert!(message.contains("ETA 2s"));
    assert!(message.contains("match remaining >= 2s"));
    assert!(message.contains("upper n/a (uncalibrated)"));
}

#[test]
fn match_forecast_never_invents_a_bound_for_unknown_phase_work() {
    let message = format_task_progress_message(&TaskProgressSnapshot {
        position: 50,
        total: None,
        unit: "pairs",
        counters: ProgressCounters::default(),
        rate: Some(25.0),
        show_match_eta: true,
        total_kind: metadata_engine::progress::TotalKind::Unknown,
    });

    assert!(message.contains("ETA n/a (total unknown)"));
    assert!(message.contains("match ETA n/a (uncalibrated)"));
    assert!(!message.contains("match remaining >="));
}

#[test]
fn calibrated_match_forecast_is_a_central_historical_range_not_a_claimed_bound() {
    let snapshot = TaskProgressSnapshot {
        position: 50,
        total: Some(100),
        unit: "pairs",
        counters: ProgressCounters::default(),
        rate: Some(25.0),
        show_match_eta: true,
        total_kind: metadata_engine::progress::TotalKind::Exact,
    };
    let forecast = MatchEtaForecast {
        schema_version: MATCH_ETA_FORECAST_SCHEMA_VERSION,
        sample_count: 8,
        lower_total_millis: Some(10_000),
        upper_total_millis: Some(20_000),
    };

    let message = format_task_progress_message_with_match_forecast(
        &snapshot,
        Some(&forecast),
        std::time::Duration::from_secs(4),
    );

    assert!(message.contains("ETA 2s"));
    assert!(message.contains("match ETA central 6s..16s (historical P20-P80; n=8)"));
}

#[test]
fn warming_match_forecast_keeps_only_the_current_exact_phase_lower_bound() {
    let snapshot = TaskProgressSnapshot {
        position: 50,
        total: Some(100),
        unit: "pairs",
        counters: ProgressCounters::default(),
        rate: Some(25.0),
        show_match_eta: true,
        total_kind: metadata_engine::progress::TotalKind::Exact,
    };
    let forecast = MatchEtaForecast {
        schema_version: MATCH_ETA_FORECAST_SCHEMA_VERSION,
        sample_count: 7,
        lower_total_millis: None,
        upper_total_millis: None,
    };

    let message = format_task_progress_message_with_match_forecast(
        &snapshot,
        Some(&forecast),
        std::time::Duration::from_secs(4),
    );

    assert!(message.contains("match remaining >= 2s"));
    assert!(message.contains("upper n/a (calibrating 7/8)"));
}

#[test]
fn elapsed_history_overrun_never_turns_a_phase_lower_bound_into_an_upper_bound() {
    let snapshot = TaskProgressSnapshot {
        position: 50,
        total: Some(100),
        unit: "pairs",
        counters: ProgressCounters::default(),
        rate: Some(25.0),
        show_match_eta: true,
        total_kind: metadata_engine::progress::TotalKind::Exact,
    };
    let forecast = MatchEtaForecast {
        schema_version: MATCH_ETA_FORECAST_SCHEMA_VERSION,
        sample_count: 8,
        lower_total_millis: Some(1_000),
        upper_total_millis: Some(3_000),
    };

    let message = format_task_progress_message_with_match_forecast(
        &snapshot,
        Some(&forecast),
        std::time::Duration::from_secs(4),
    );

    assert!(message.contains("match remaining >= 2s"));
    assert!(message.contains("upper n/a (history overrun; n=8)"));
    assert!(!message.contains("match ETA central"));
}

#[test]
fn phase_lower_over_history_upper_never_fabricates_an_observed_interval() {
    let snapshot = TaskProgressSnapshot {
        position: 50,
        total: Some(100),
        unit: "pairs",
        counters: ProgressCounters::default(),
        rate: Some(5.0),
        show_match_eta: true,
        total_kind: metadata_engine::progress::TotalKind::Exact,
    };
    let forecast = MatchEtaForecast {
        schema_version: MATCH_ETA_FORECAST_SCHEMA_VERSION,
        sample_count: 8,
        lower_total_millis: Some(1_000),
        upper_total_millis: Some(8_000),
    };

    let message = format_task_progress_message_with_match_forecast(
        &snapshot,
        Some(&forecast),
        std::time::Duration::from_secs(1),
    );

    assert!(message.contains("match remaining >= 10s"), "{message}");
    assert!(
        message.contains("upper n/a (phase lower exceeds history; n=8)"),
        "{message}"
    );
    assert!(!message.contains("match ETA central"), "{message}");
}

#[test]
fn determinate_task_progress_clamps_only_the_bar_and_preserves_plan_overrun() {
    let progress = ProgressTracker::for_pipeline_stage(PipelineStage::MetadataMatch, true);
    progress.start_task("catalog pairs", Some(10), "pairs");
    progress.advance_task(15, ProgressCounters::default());

    let ProgressTracker::Enabled {
        task, task_state, ..
    } = &progress
    else {
        panic!("progress must be enabled");
    };
    assert_eq!(task.position(), 10);
    assert_eq!(task_state.lock().unwrap().as_ref().unwrap().position, 15);
}

#[test]
fn task_rendering_coalesces_updates_but_completion_flushes_immediately() {
    let progress = ProgressTracker::for_pipeline_stage(PipelineStage::Name, true);
    progress.start_task("canonical names", Some(100), "names");
    progress.advance_task(10, ProgressCounters::default());

    let ProgressTracker::Enabled {
        task, task_state, ..
    } = &progress
    else {
        panic!("progress must be enabled");
    };
    assert_eq!(task.position(), 10);

    progress.advance_task(20, ProgressCounters::default());
    assert_eq!(task.position(), 10, "bar update should remain coalesced");
    assert_eq!(task_state.lock().unwrap().as_ref().unwrap().position, 30);

    progress.advance_task(70, ProgressCounters::default());
    assert_eq!(task.position(), 100, "completion must bypass throttling");
}

#[test]
fn engine_progress_events_drive_absolute_task_position_and_reset_by_phase() {
    use metadata_engine::progress::{
        ProgressCounters as EngineCounters, ProgressEvent, ProgressPhase, WorkUnit,
    };

    let progress = ProgressTracker::for_pipeline_stage(PipelineStage::MetadataMatch, true);
    progress.observe_engine_event(ProgressEvent::determinate(
        ProgressPhase::PairExactIsland,
        25,
        100,
        WorkUnit::Pairs,
        EngineCounters {
            matched: 3,
            ..EngineCounters::default()
        },
    ));

    let ProgressTracker::Enabled { task, metrics, .. } = &progress else {
        panic!("progress must be enabled");
    };
    assert_eq!(task.length(), Some(100));
    assert_eq!(task.position(), 25);
    assert!(task.message().contains("pair exact island"));
    assert!(metrics.message().contains("matched 3"));

    progress.observe_engine_event(ProgressEvent::determinate(
        ProgressPhase::CatalogPairs,
        5,
        50,
        WorkUnit::Pairs,
        EngineCounters {
            candidates: 2,
            ..EngineCounters::default()
        },
    ));
    assert_eq!(task.length(), Some(50));
    assert_eq!(task.position(), 5);
    assert!(task.message().contains("catalog pairs"));
}

#[test]
fn upper_bound_engine_task_marks_the_bar_as_an_upper_bound() {
    use metadata_engine::progress::{
        ProgressCounters as EngineCounters, ProgressEvent, ProgressPhase, TotalKind, WorkClass,
        WorkUnit,
    };

    let progress = ProgressTracker::for_pipeline_stage(PipelineStage::MetadataMatch, true);
    progress.observe_engine_event(
        ProgressEvent::determinate(
            ProgressPhase::CatalogPairs,
            25,
            100,
            WorkUnit::Pairs,
            EngineCounters::default(),
        )
        .with_plan(WorkClass::Generic, TotalKind::UpperBound),
    );

    let ProgressTracker::Enabled { task, .. } = &progress else {
        panic!("progress must be enabled");
    };
    assert_eq!(task.message(), "catalog pairs (upper bound)");
}

#[test]
fn metadata_encode_engine_events_do_not_show_match_eta() {
    use metadata_engine::progress::{
        ProgressCounters as EngineCounters, ProgressEvent, ProgressPhase, WorkUnit,
    };

    let progress = ProgressTracker::for_pipeline_stage(PipelineStage::MetadataEncode, true);
    progress.observe_engine_event(ProgressEvent::determinate(
        ProgressPhase::EncodeTokenSources,
        1,
        2,
        WorkUnit::Items,
        EngineCounters::default(),
    ));

    let ProgressTracker::Enabled { metrics, .. } = &progress else {
        panic!("progress must be enabled");
    };
    assert!(
        !metrics.message().contains("match ETA"),
        "{}",
        metrics.message()
    );
}

#[test]
fn hierarchical_progress_finishes_all_levels_with_failure_context() {
    let progress = ProgressTracker::for_pipeline_stage(PipelineStage::MetadataMatch, true);
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
    assert!(stage.message().is_empty());
    assert!(task.message().is_empty());
    assert!(metrics.message().is_empty());
}
