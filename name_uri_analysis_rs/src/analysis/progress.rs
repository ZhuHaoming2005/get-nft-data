use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use super::*;

const PIPELINE_STAGE_COUNT: u64 = 5;
pub(crate) const PROGRESS_REFRESH_INTERVAL: Duration = Duration::from_millis(50);
const PROGRESS_REFRESH_HZ: u8 = 20;
const RATE_WARMUP: Duration = Duration::from_secs(1);
const RATE_EWMA_ALPHA: f64 = 0.25;
#[derive(Debug, Clone, serde::Deserialize, PartialEq, Eq)]
pub(crate) struct MatchEtaForecast {
    pub(crate) schema_version: u32,
    pub(crate) sample_count: usize,
    pub(crate) lower_total_millis: Option<u64>,
    pub(crate) upper_total_millis: Option<u64>,
}

fn match_eta_forecast_from_env() -> Option<MatchEtaForecast> {
    let value = std::env::var(MATCH_ETA_FORECAST_ENV).ok()?;
    parse_match_eta_forecast(&value)
}

pub(crate) fn parse_match_eta_forecast(value: &str) -> Option<MatchEtaForecast> {
    let forecast = serde_json::from_str::<MatchEtaForecast>(value).ok()?;
    if forecast.schema_version != MATCH_ETA_FORECAST_SCHEMA_VERSION {
        return None;
    }
    match (forecast.lower_total_millis, forecast.upper_total_millis) {
        (Some(lower), Some(upper)) if lower <= upper && forecast.sample_count >= 8 => {}
        (None, None) if forecast.sample_count < 8 => {}
        _ => return None,
    }
    Some(forecast)
}

fn match_elapsed_offset_from_env() -> Duration {
    let started = std::env::var(MATCH_ETA_STARTED_UNIX_MILLIS_ENV)
        .ok()
        .and_then(|value| value.parse::<u128>().ok());
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_millis());
    match_elapsed_offset(started, now)
}

pub(crate) fn match_elapsed_offset(started: Option<u128>, now: Option<u128>) -> Duration {
    let (Some(started), Some(now)) = (started, now) else {
        return Duration::ZERO;
    };
    Duration::from_millis(u64::try_from(now.saturating_sub(started)).unwrap_or(u64::MAX))
}

#[derive(Debug)]
pub(crate) struct TaskRateEstimator {
    last_position: Option<u64>,
    last_elapsed: Duration,
    rate: Option<f64>,
}

impl Default for TaskRateEstimator {
    fn default() -> Self {
        Self {
            last_position: None,
            last_elapsed: Duration::ZERO,
            rate: None,
        }
    }
}

impl TaskRateEstimator {
    pub(crate) fn sample(&mut self, position: u64, elapsed: Duration) -> Option<f64> {
        let Some(last_position) = self.last_position.replace(position) else {
            self.last_elapsed = elapsed;
            return None;
        };
        let delta = position.saturating_sub(last_position);
        let sample_elapsed = elapsed.saturating_sub(self.last_elapsed);
        self.last_elapsed = elapsed;
        if delta == 0 || sample_elapsed.is_zero() || elapsed < RATE_WARMUP {
            return self.rate;
        }
        let instantaneous = delta as f64 / sample_elapsed.as_secs_f64();
        let smoothed = self.rate.map_or(instantaneous, |previous| {
            RATE_EWMA_ALPHA * instantaneous + (1.0 - RATE_EWMA_ALPHA) * previous
        });
        self.rate = Some(smoothed);
        self.rate
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PipelineStage {
    Prepare,
    MetadataEncode,
    Name,
    MetadataMatch,
    Finalize,
}

impl PipelineStage {
    const fn position(self) -> u64 {
        match self {
            Self::Prepare => 0,
            Self::MetadataEncode => 1,
            Self::Name => 2,
            Self::MetadataMatch => 3,
            Self::Finalize => 4,
        }
    }

    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Prepare => "prepare + URI",
            Self::MetadataEncode => "metadata encode",
            Self::Name => "name",
            Self::MetadataMatch => "metadata match",
            Self::Finalize => "finalize outputs",
        }
    }
}

impl From<AnalysisPhase> for PipelineStage {
    fn from(value: AnalysisPhase) -> Self {
        match value {
            AnalysisPhase::Prepare => Self::Prepare,
            AnalysisPhase::MetadataEncode => Self::MetadataEncode,
            AnalysisPhase::Name => Self::Name,
            AnalysisPhase::MetadataMatch => Self::MetadataMatch,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ProgressCounters {
    pub(crate) groups: u64,
    pub(crate) candidates: u64,
    pub(crate) scored: u64,
    pub(crate) expanded: u64,
    pub(crate) matched: u64,
    pub(crate) selected: u64,
}

pub(crate) struct TaskProgressSnapshot<'a> {
    pub(crate) position: u64,
    pub(crate) total: Option<u64>,
    pub(crate) unit: &'a str,
    pub(crate) counters: ProgressCounters,
    pub(crate) rate: Option<f64>,
    pub(crate) show_match_eta: bool,
    pub(crate) total_kind: metadata_engine::progress::TotalKind,
}

pub(crate) struct TaskProgressState {
    label: String,
    total: Option<u64>,
    unit: String,
    pub(crate) position: u64,
    counters: ProgressCounters,
    started: Instant,
    last_refresh: Option<Instant>,
    rate: TaskRateEstimator,
    show_match_eta: bool,
    total_kind: metadata_engine::progress::TotalKind,
    work_class: metadata_engine::progress::WorkClass,
}

pub(crate) enum ProgressTracker {
    Enabled {
        _multi: MultiProgress,
        pipeline: ProgressBar,
        stage: ProgressBar,
        task: ProgressBar,
        metrics: ProgressBar,
        task_state: Box<Mutex<Option<TaskProgressState>>>,
        match_started: Instant,
        match_elapsed_offset: Duration,
        match_forecast: Box<Option<MatchEtaForecast>>,
        match_eta_enabled: AtomicBool,
    },
    Disabled,
}

impl ProgressTracker {
    pub(crate) fn for_pipeline_stage(stage: PipelineStage, enabled: bool) -> Self {
        let tracker = Self::build(PIPELINE_STAGE_COUNT, enabled);
        tracker.set_pipeline_stage(stage);
        tracker
    }

    pub(crate) fn set_pipeline_stage(&self, pipeline_stage: PipelineStage) {
        if let Self::Enabled {
            pipeline,
            stage,
            match_eta_enabled,
            ..
        } = self
        {
            pipeline.set_position(pipeline_stage.position());
            pipeline.set_message(pipeline_stage.label());
            match_eta_enabled.store(
                pipeline_stage == PipelineStage::MetadataMatch,
                Ordering::Relaxed,
            );
            if pipeline_stage == PipelineStage::MetadataMatch {
                stage.reset();
                stage.unset_length();
                stage.set_style(stage_spinner_style());
                stage.set_message("waiting for engine progress");
                stage.enable_steady_tick(PROGRESS_REFRESH_INTERVAL);
            } else {
                stage.set_style(stage_bar_style());
            }
        }
    }

    fn build(total_phases: u64, enabled: bool) -> Self {
        if !enabled {
            return Self::Disabled;
        }
        let multi = MultiProgress::with_draw_target(ProgressDrawTarget::stderr_with_hz(
            PROGRESS_REFRESH_HZ,
        ));
        let pipeline = multi.add(ProgressBar::new(total_phases));
        pipeline.set_style(
            ProgressStyle::with_template(pipeline_bar_template())
                .unwrap()
                .progress_chars("#>-"),
        );
        let stage = multi.add(ProgressBar::new(0));
        stage.set_style(stage_bar_style());
        let task = multi.add(ProgressBar::new_spinner());
        task.set_style(task_spinner_style());
        let metrics = multi.add(ProgressBar::new_spinner());
        metrics.set_style(ProgressStyle::with_template(metrics_template()).unwrap());
        Self::Enabled {
            _multi: multi,
            pipeline,
            stage,
            task,
            metrics,
            task_state: Box::new(Mutex::new(None)),
            match_started: Instant::now(),
            match_elapsed_offset: match_elapsed_offset_from_env(),
            match_forecast: Box::new(match_eta_forecast_from_env()),
            match_eta_enabled: AtomicBool::new(false),
        }
    }

    pub(crate) fn start_stage(&self, message: impl Into<String>, work_units: u64) {
        let Self::Enabled { stage, .. } = self else {
            return;
        };
        let message = message.into();
        stage.reset();
        stage.set_length(work_units);
        stage.set_position(0);
        stage.set_message(message);
    }

    pub(crate) fn step_stage(&self, message: impl Into<String>) {
        if let Self::Enabled { stage, .. } = self {
            stage.set_message(message.into());
            stage.inc(1);
        }
    }

    pub(crate) fn finish_stage(&self, message: impl Into<String>) {
        if let Self::Enabled {
            stage,
            task,
            metrics,
            ..
        } = self
        {
            let message = message.into();
            task.finish_and_clear();
            metrics.finish_and_clear();
            stage.finish_with_message(message);
        }
    }

    pub(crate) fn finish_pipeline_stage(&self, message: impl Into<String>) {
        if let Self::Enabled { pipeline, .. } = self {
            pipeline.inc(1);
            pipeline.set_message(message.into());
        }
    }

    pub(crate) fn start_task(
        &self,
        label: impl Into<String>,
        total: Option<u64>,
        unit: impl Into<String>,
    ) {
        let total_kind = if total.is_some() {
            metadata_engine::progress::TotalKind::Exact
        } else {
            metadata_engine::progress::TotalKind::Unknown
        };
        self.start_task_with_context(
            label,
            total,
            unit,
            false,
            metadata_engine::progress::WorkClass::Generic,
            total_kind,
        );
    }

    fn start_task_with_context(
        &self,
        label: impl Into<String>,
        total: Option<u64>,
        unit: impl Into<String>,
        show_match_eta: bool,
        work_class: metadata_engine::progress::WorkClass,
        total_kind: metadata_engine::progress::TotalKind,
    ) {
        let Self::Enabled {
            task,
            metrics,
            task_state,
            ..
        } = self
        else {
            return;
        };
        let label = label.into();
        let unit = unit.into();
        task.reset();
        if let Some(total) = total {
            task.set_length(total);
            task.set_style(task_bar_style());
        } else {
            task.unset_length();
            task.set_style(task_spinner_style());
        }
        task.set_position(0);
        task.set_message(label.clone());
        task.enable_steady_tick(PROGRESS_REFRESH_INTERVAL);
        metrics.reset();
        if total_kind == metadata_engine::progress::TotalKind::Unknown {
            metrics.set_message("ETA n/a (work total not observable)");
        } else {
            metrics.set_message("waiting for progress sample");
        }
        metrics.enable_steady_tick(PROGRESS_REFRESH_INTERVAL);
        *task_state.lock().expect("task progress mutex poisoned") = Some(TaskProgressState {
            label,
            total,
            unit,
            position: 0,
            counters: ProgressCounters::default(),
            started: Instant::now(),
            last_refresh: None,
            rate: TaskRateEstimator::default(),
            show_match_eta,
            total_kind,
            work_class,
        });
    }

    pub(crate) fn advance_task(&self, delta: u64, counters: ProgressCounters) {
        let Self::Enabled {
            stage,
            task,
            metrics,
            task_state,
            match_started,
            match_elapsed_offset,
            match_forecast,
            ..
        } = self
        else {
            return;
        };
        let now = Instant::now();
        let mut guard = task_state.lock().expect("task progress mutex poisoned");
        let Some(state) = guard.as_mut() else {
            stage.inc(delta);
            return;
        };
        let next = state.position.saturating_add(delta);
        state.position = next;
        state.counters = counters;
        task.set_position(
            state
                .total
                .map_or(state.position, |total| state.position.min(total)),
        );
        if state.last_refresh.is_some_and(|last_refresh| {
            now.duration_since(last_refresh) < PROGRESS_REFRESH_INTERVAL
        }) {
            return;
        }
        state.last_refresh = Some(now);
        let elapsed = now.duration_since(state.started);
        let rate = state.rate.sample(state.position, elapsed);
        metrics.set_message(format_task_progress_message_with_match_forecast(
            &TaskProgressSnapshot {
                position: state.position,
                total: state.total,
                unit: &state.unit,
                counters: state.counters,
                rate,
                show_match_eta: state.show_match_eta,
                total_kind: state.total_kind,
            },
            match_forecast.as_ref().as_ref(),
            match_elapsed_offset.saturating_add(now.duration_since(*match_started)),
        ));
    }

    pub(crate) fn observe_engine_event(&self, event: metadata_engine::progress::ProgressEvent) {
        let Self::Enabled {
            stage,
            task_state,
            match_eta_enabled,
            ..
        } = self
        else {
            return;
        };
        if match_eta_enabled.load(Ordering::Relaxed) {
            stage.set_message(event.phase.label());
        }
        let label = if event.total_kind == metadata_engine::progress::TotalKind::UpperBound {
            format!("{} (upper bound)", event.phase.label())
        } else {
            event.phase.label().to_string()
        };
        let unit = event.unit.label();
        let (restart, previous) = {
            let guard = task_state.lock().expect("task progress mutex poisoned");
            match guard.as_ref() {
                Some(state)
                    if state.label == label
                        && state.total == event.total
                        && state.unit == unit
                        && state.work_class == event.work_class
                        && state.total_kind == event.total_kind
                        && state.position <= event.completed =>
                {
                    (false, state.position)
                }
                _ => (true, 0),
            }
        };
        if restart {
            self.start_task_with_context(
                label,
                event.total,
                unit,
                match_eta_enabled.load(Ordering::Relaxed),
                event.work_class,
                event.total_kind,
            );
        }
        self.advance_task(
            event.completed.saturating_sub(previous),
            ProgressCounters {
                groups: event.counters.groups,
                candidates: event.counters.candidates,
                scored: event.counters.scored,
                expanded: event.counters.expanded,
                matched: event.counters.matched,
                selected: event.counters.selected,
            },
        );
    }

    #[cfg(test)]
    pub(crate) fn update_task_label(&self, label: impl Into<String>) {
        let Self::Enabled { task_state, .. } = self else {
            return;
        };
        if let Some(state) = task_state
            .lock()
            .expect("task progress mutex poisoned")
            .as_mut()
        {
            state.label = label.into();
        }
    }

    pub(crate) fn finish_task(&self, message: impl Into<String>) {
        let Self::Enabled {
            task,
            metrics,
            task_state,
            ..
        } = self
        else {
            return;
        };
        let final_message = message.into();
        if let Some(state) = task_state
            .lock()
            .expect("task progress mutex poisoned")
            .take()
        {
            if let Some(total) = state.total {
                task.set_position(total);
            }
            task.finish_with_message(state.label);
        } else {
            task.finish_and_clear();
        }
        metrics.finish_with_message(final_message);
    }

    // Compatibility methods for the existing stage-level progress call sites.
    pub(crate) fn add_work(&self, units: u64) {
        if let Self::Enabled { stage, .. } = self {
            stage.inc_length(units);
        }
    }

    pub(crate) fn set_message(&self, message: impl Into<String>) {
        if let Self::Enabled { stage, .. } = self {
            stage.set_message(message.into());
        }
    }

    #[cfg(test)]
    pub(crate) fn finish(&self) {
        self.finish_display("analysis complete; writing outputs finished");
    }

    pub(crate) fn finish_display(&self, message: impl Into<String>) {
        if let Self::Enabled {
            pipeline,
            stage,
            task,
            metrics,
            ..
        } = self
        {
            let message = message.into();
            task.finish_and_clear();
            metrics.finish_and_clear();
            stage.finish_and_clear();
            pipeline.finish_with_message(message);
        }
    }

    pub(crate) fn fail(&self, message: impl Into<String>) {
        if let Self::Enabled {
            pipeline,
            stage,
            task,
            metrics,
            ..
        } = self
        {
            let message = format!("FAILED: {}", message.into());
            task.abandon_with_message(message.clone());
            metrics.abandon_with_message(message.clone());
            stage.abandon_with_message(message.clone());
            pipeline.abandon_with_message(message);
        }
    }
}

#[cfg(test)]
pub(crate) fn format_task_progress_message(snapshot: &TaskProgressSnapshot<'_>) -> String {
    format_task_progress_message_with_match_forecast(snapshot, None, Duration::ZERO)
}

pub(crate) fn format_task_progress_message_with_match_forecast(
    snapshot: &TaskProgressSnapshot<'_>,
    match_forecast: Option<&MatchEtaForecast>,
    match_elapsed: Duration,
) -> String {
    if snapshot.total_kind == metadata_engine::progress::TotalKind::Exact
        && snapshot.total == Some(0)
        && snapshot.position == 0
    {
        return format!("skipped (0 {})", snapshot.unit);
    }
    let mut metrics = Vec::new();
    if snapshot.total.is_none() {
        metrics.push(format!(
            "{} {}",
            format_progress_count(snapshot.position),
            snapshot.unit
        ));
    }
    if snapshot.total_kind == metadata_engine::progress::TotalKind::Exact {
        if let Some(total) = snapshot.total {
            if snapshot.position > total {
                metrics.push(format!(
                    "PLAN OVERRUN +{} {}",
                    format_progress_count(snapshot.position - total),
                    snapshot.unit
                ));
            }
        }
    }
    let rate = snapshot.rate;
    match rate {
        Some(rate) => metrics.push(format!(
            "{} {}/s",
            format_progress_rate(rate),
            snapshot.unit
        )),
        None => metrics.push(format!("n/a {}/s", snapshot.unit)),
    }
    let phase_eta = match (snapshot.total_kind, snapshot.total, rate) {
        (
            metadata_engine::progress::TotalKind::Exact
            | metadata_engine::progress::TotalKind::UpperBound,
            Some(total),
            Some(rate),
        ) if rate > 0.0 && snapshot.position <= total => {
            let remaining = total.saturating_sub(snapshot.position) as f64;
            Some(Duration::from_secs_f64((remaining / rate).ceil()))
        }
        _ => None,
    };
    match phase_eta {
        Some(eta) if snapshot.total_kind == metadata_engine::progress::TotalKind::UpperBound => {
            metrics.push(format!("ETA ≤ {}", format_progress_duration(eta)));
        }
        Some(eta) => metrics.push(format!("ETA {}", format_progress_duration(eta))),
        None if snapshot.total_kind == metadata_engine::progress::TotalKind::Unknown => {
            metrics.push("ETA n/a (total unknown)".to_string());
        }
        None => metrics.push("ETA n/a".to_string()),
    }
    let phase_lower_bound_eta = (snapshot.total_kind
        == metadata_engine::progress::TotalKind::Exact)
        .then_some(phase_eta)
        .flatten();
    if snapshot.show_match_eta {
        match match_forecast {
            Some(MatchEtaForecast {
                sample_count,
                lower_total_millis: Some(lower_total_millis),
                upper_total_millis: Some(upper_total_millis),
                ..
            }) => {
                if match_elapsed >= Duration::from_millis(*upper_total_millis) {
                    if let Some(phase_eta) = phase_lower_bound_eta {
                        metrics.push(format!(
                            "match remaining >= {}; upper n/a (history overrun; n={sample_count})",
                            format_progress_duration(phase_eta)
                        ));
                    } else {
                        metrics.push(format!(
                            "match ETA lower n/a; upper n/a (history overrun; n={sample_count})"
                        ));
                    }
                } else {
                    let historical_lower =
                        Duration::from_millis(*lower_total_millis).saturating_sub(match_elapsed);
                    let historical_upper =
                        Duration::from_millis(*upper_total_millis).saturating_sub(match_elapsed);
                    let lower = phase_lower_bound_eta
                        .map_or(historical_lower, |phase| phase.max(historical_lower));
                    if lower > historical_upper {
                        metrics.push(format!(
                            "match remaining >= {}; upper n/a (phase lower exceeds history; n={sample_count})",
                            format_progress_duration(lower)
                        ));
                    } else {
                        metrics.push(format!(
                            "match ETA observed {}..{} (n={sample_count})",
                            format_progress_duration(lower),
                            format_progress_duration(historical_upper)
                        ));
                    }
                }
            }
            Some(forecast) => {
                if let Some(phase_eta) = phase_lower_bound_eta {
                    metrics.push(format!(
                        "match remaining >= {}; upper n/a (calibrating {}/8)",
                        format_progress_duration(phase_eta),
                        forecast.sample_count
                    ));
                } else {
                    metrics.push(format!(
                        "match ETA lower n/a; upper n/a (calibrating {}/8)",
                        forecast.sample_count
                    ));
                }
            }
            None => {
                if let Some(phase_eta) = phase_eta {
                    metrics.push(format!(
                        "match remaining >= {}; upper n/a (uncalibrated)",
                        format_progress_duration(phase_eta)
                    ));
                } else {
                    metrics.push("match ETA n/a (uncalibrated)".to_string());
                }
            }
        }
    }
    if snapshot.counters.groups != 0 {
        metrics.push(format!(
            "groups {}",
            format_progress_count(snapshot.counters.groups)
        ));
    }
    if snapshot.counters.candidates != 0 {
        metrics.push(format!(
            "candidates {}",
            format_progress_count(snapshot.counters.candidates)
        ));
    }
    if snapshot.counters.scored != 0 {
        metrics.push(format!(
            "scored {}",
            format_progress_count(snapshot.counters.scored)
        ));
    }
    if snapshot.counters.expanded != 0 {
        metrics.push(format!(
            "expanded {}",
            format_progress_count(snapshot.counters.expanded)
        ));
    }
    if snapshot.counters.matched != 0 {
        metrics.push(format!(
            "matched {}",
            format_progress_count(snapshot.counters.matched)
        ));
    }
    if snapshot.counters.selected != 0 {
        metrics.push(format!(
            "selected {} sources",
            format_progress_count(snapshot.counters.selected)
        ));
    }
    metrics.join(" · ")
}

fn format_progress_count(count: u64) -> String {
    indicatif::HumanCount(count).to_string()
}

fn format_progress_rate(rate: f64) -> String {
    for (scale, suffix) in [(1_000_000_000.0, "G"), (1_000_000.0, "M"), (1_000.0, "K")] {
        if rate >= scale {
            return format!("{:.1}{suffix}", rate / scale);
        }
    }
    format!("{rate:.1}")
}

fn format_progress_duration(duration: Duration) -> String {
    let seconds = duration.as_secs();
    if seconds < 60 {
        return format!("{seconds}s");
    }
    let minutes = seconds / 60;
    let remaining_seconds = seconds % 60;
    if minutes < 60 {
        return format!("{minutes}m {remaining_seconds:02}s");
    }
    let hours = minutes / 60;
    let remaining_minutes = minutes % 60;
    format!("{hours}h {remaining_minutes:02}m")
}

fn task_bar_style() -> ProgressStyle {
    ProgressStyle::with_template(task_bar_template())
        .unwrap()
        .progress_chars("#>-")
}

fn stage_bar_style() -> ProgressStyle {
    ProgressStyle::with_template(stage_bar_template())
        .unwrap()
        .progress_chars("#>-")
}

fn stage_spinner_style() -> ProgressStyle {
    ProgressStyle::with_template(stage_spinner_template()).unwrap()
}

fn task_spinner_style() -> ProgressStyle {
    ProgressStyle::with_template("    {spinner:.yellow} task [{elapsed_precise}] {msg}").unwrap()
}

pub(crate) const fn pipeline_bar_template() -> &'static str {
    "{spinner:.green} pipeline [{elapsed_precise}] [{bar:24.cyan/blue}] {pos}/{len} {msg}"
}

pub(crate) const fn stage_bar_template() -> &'static str {
    "  {spinner:.blue} stage [{elapsed_precise}] [{bar:28.magenta/blue}] {pos}/{len} {percent:>3}% {msg}"
}

pub(crate) const fn stage_spinner_template() -> &'static str {
    "  {spinner:.blue} stage [{elapsed_precise}] {msg}"
}

pub(crate) const fn task_bar_template() -> &'static str {
    "    {spinner:.yellow} task [{elapsed_precise}] [{bar:32.yellow/blue}] {human_pos}/{human_len} {percent:>3}% {msg}"
}

pub(crate) const fn metrics_template() -> &'static str {
    "      {spinner:.white} metrics {msg}"
}

#[cfg(test)]
mod throttle_tests {
    use super::*;

    #[test]
    fn task_label_updates_preserve_the_refresh_throttle() {
        let tracker = ProgressTracker::for_pipeline_stage(PipelineStage::Name, true);
        tracker.start_task("initial", Some(1), "items");
        tracker.advance_task(0, ProgressCounters::default());
        let before = match &tracker {
            ProgressTracker::Enabled { task_state, .. } => {
                task_state.lock().unwrap().as_ref().unwrap().last_refresh
            }
            ProgressTracker::Disabled => panic!("progress must be enabled"),
        };

        tracker.update_task_label("changed");

        let (label, after) = match &tracker {
            ProgressTracker::Enabled { task_state, .. } => {
                let guard = task_state.lock().unwrap();
                let state = guard.as_ref().unwrap();
                (state.label.clone(), state.last_refresh)
            }
            ProgressTracker::Disabled => panic!("progress must be enabled"),
        };
        assert_eq!(label, "changed");
        assert_eq!(after, before);
    }
}
