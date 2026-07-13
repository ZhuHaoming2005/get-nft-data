use std::sync::Mutex;
use std::time::{Duration, Instant};

use super::*;

const PIPELINE_STAGE_COUNT: u64 = 4;
const PROGRESS_REFRESH_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PipelineStage {
    Prepare,
    Name,
    Metadata,
    Finalize,
}

impl PipelineStage {
    const fn position(self) -> u64 {
        match self {
            Self::Prepare => 0,
            Self::Name => 1,
            Self::Metadata => 2,
            Self::Finalize => 3,
        }
    }

    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Prepare => "prepare + URI",
            Self::Name => "name",
            Self::Metadata => "metadata",
            Self::Finalize => "finalize outputs",
        }
    }
}

impl From<AnalysisPhase> for PipelineStage {
    fn from(value: AnalysisPhase) -> Self {
        match value {
            AnalysisPhase::Prepare => Self::Prepare,
            AnalysisPhase::Name => Self::Name,
            AnalysisPhase::Metadata => Self::Metadata,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ProgressCounters {
    pub(crate) groups: u64,
    pub(crate) candidates: u64,
    pub(crate) scored: u64,
    pub(crate) matched: u64,
}

pub(crate) struct TaskProgressSnapshot<'a> {
    pub(crate) label: &'a str,
    pub(crate) position: u64,
    pub(crate) total: Option<u64>,
    pub(crate) unit: &'a str,
    pub(crate) counters: ProgressCounters,
    pub(crate) elapsed: Duration,
}

pub(crate) struct TaskProgressState {
    label: String,
    total: Option<u64>,
    unit: String,
    position: u64,
    counters: ProgressCounters,
    started: Instant,
    last_refresh: Option<Instant>,
}

pub(crate) enum ProgressTracker {
    Enabled {
        _multi: MultiProgress,
        pipeline: ProgressBar,
        stage: ProgressBar,
        task: ProgressBar,
        task_state: Box<Mutex<Option<TaskProgressState>>>,
    },
    Disabled,
}

impl ProgressTracker {
    #[cfg(test)]
    pub(crate) fn new(total_phases: u64, enabled: bool) -> Self {
        let tracker = Self::build(total_phases, enabled);
        if let Self::Enabled { pipeline, .. } = &tracker {
            pipeline.set_message("analysis");
        }
        tracker
    }

    pub(crate) fn for_pipeline_stage(stage: PipelineStage, enabled: bool) -> Self {
        let tracker = Self::build(PIPELINE_STAGE_COUNT, enabled);
        tracker.set_pipeline_stage(stage);
        tracker
    }

    pub(crate) fn set_pipeline_stage(&self, pipeline_stage: PipelineStage) {
        if let Self::Enabled { pipeline, .. } = self {
            pipeline.set_position(pipeline_stage.position());
            pipeline.set_message(pipeline_stage.label());
        }
    }

    fn build(total_phases: u64, enabled: bool) -> Self {
        if !enabled {
            return Self::Disabled;
        }
        let multi = MultiProgress::new();
        let pipeline = multi.add(ProgressBar::new(total_phases));
        pipeline.set_style(
            ProgressStyle::with_template(
                "{spinner:.green} pipeline [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} {msg}",
            )
            .unwrap()
            .progress_chars("#>-"),
        );
        let stage = multi.add(ProgressBar::new(0));
        stage.set_style(
            ProgressStyle::with_template(
                "  {spinner:.blue} stage [{elapsed_precise}] [{wide_bar:.magenta/blue}] {pos}/{len} {percent}% {msg}",
            )
            .unwrap()
            .progress_chars("#>-"),
        );
        let task = multi.add(ProgressBar::new_spinner());
        task.set_style(task_spinner_style());
        Self::Enabled {
            _multi: multi,
            pipeline,
            stage,
            task,
            task_state: Box::new(Mutex::new(None)),
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
        if let Self::Enabled { stage, task, .. } = self {
            let message = message.into();
            task.finish_and_clear();
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
        let Self::Enabled {
            task, task_state, ..
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
        *task_state.lock().expect("task progress mutex poisoned") = Some(TaskProgressState {
            label,
            total,
            unit,
            position: 0,
            counters: ProgressCounters::default(),
            started: Instant::now(),
            last_refresh: None,
        });
    }

    pub(crate) fn advance_task(&self, delta: u64, counters: ProgressCounters) {
        let Self::Enabled {
            stage,
            task,
            task_state,
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
        state.position = state.position.saturating_add(delta);
        state.counters = counters;
        task.set_position(state.position);
        if state.last_refresh.is_some_and(|last_refresh| {
            now.duration_since(last_refresh) < PROGRESS_REFRESH_INTERVAL
        }) {
            return;
        }
        state.last_refresh = Some(now);
        task.set_message(format_task_progress_message(&TaskProgressSnapshot {
            label: &state.label,
            position: state.position,
            total: state.total,
            unit: &state.unit,
            counters: state.counters,
            elapsed: now.duration_since(state.started),
        }));
    }

    pub(crate) fn finish_task(&self, message: impl Into<String>) {
        let Self::Enabled {
            task, task_state, ..
        } = self
        else {
            return;
        };
        if let Some(state) = task_state
            .lock()
            .expect("task progress mutex poisoned")
            .take()
        {
            if let Some(total) = state.total {
                task.set_position(total);
            }
        }
        task.finish_with_message(message.into());
    }

    // Compatibility methods for the existing stage-level progress call sites.
    pub(crate) fn start_phase(&self, message: impl Into<String>, work_units: u64) {
        self.start_stage(message, work_units);
    }

    pub(crate) fn add_work(&self, units: u64) {
        if let Self::Enabled { stage, .. } = self {
            stage.inc_length(units);
        }
    }

    pub(crate) fn step(&self, message: impl Into<String>) {
        self.step_stage(message);
    }

    #[cfg(test)]
    pub(crate) fn inc(&self, units: u64) {
        if let Self::Enabled { stage, .. } = self {
            stage.inc(units);
        }
    }

    pub(crate) fn set_message(&self, message: impl Into<String>) {
        if let Self::Enabled { stage, .. } = self {
            stage.set_message(message.into());
        }
    }

    pub(crate) fn finish_phase(&self, message: impl Into<String>) {
        self.finish_stage(message);
    }

    pub(crate) fn finish(&self) {
        self.finish_display("analysis complete; writing outputs finished");
    }

    pub(crate) fn finish_display(&self, message: impl Into<String>) {
        if let Self::Enabled {
            pipeline,
            stage,
            task,
            ..
        } = self
        {
            let message = message.into();
            task.finish_and_clear();
            stage.finish_and_clear();
            pipeline.finish_with_message(message);
        }
    }

    pub(crate) fn fail(&self, message: impl Into<String>) {
        if let Self::Enabled {
            pipeline,
            stage,
            task,
            ..
        } = self
        {
            let message = format!("FAILED: {}", message.into());
            task.abandon_with_message(message.clone());
            stage.abandon_with_message(message.clone());
            pipeline.abandon_with_message(message);
        }
    }
}

pub(crate) fn format_task_progress_message(snapshot: &TaskProgressSnapshot<'_>) -> String {
    let mut message = snapshot.label.to_string();
    match snapshot.total {
        Some(total) => message.push_str(&format!(
            "; {}/{} {}",
            snapshot.position, total, snapshot.unit
        )),
        None => message.push_str(&format!("; {} {}", snapshot.position, snapshot.unit)),
    }
    let rate = progress_rate(snapshot.position, snapshot.elapsed);
    match rate {
        Some(rate) => message.push_str(&format!("; {rate:.1} {}/s", snapshot.unit)),
        None => message.push_str(&format!("; n/a {}/s", snapshot.unit)),
    }
    let eta = match (snapshot.total, rate) {
        (Some(total), Some(rate)) if rate > 0.0 => {
            let remaining = total.saturating_sub(snapshot.position) as f64;
            format_progress_duration(Duration::from_secs_f64((remaining / rate).ceil()))
        }
        _ => "n/a".to_string(),
    };
    message.push_str(&format!("; ETA {eta}"));
    if snapshot.counters.groups != 0 {
        message.push_str(&format!("; groups {}", snapshot.counters.groups));
    }
    if snapshot.counters.candidates != 0 {
        message.push_str(&format!("; candidates {}", snapshot.counters.candidates));
    }
    if snapshot.counters.scored != 0 {
        message.push_str(&format!("; scored {}", snapshot.counters.scored));
    }
    if snapshot.counters.matched != 0 {
        message.push_str(&format!("; matched {}", snapshot.counters.matched));
    }
    message
}

fn progress_rate(position: u64, elapsed: Duration) -> Option<f64> {
    if position == 0 || elapsed.is_zero() {
        return None;
    }
    Some(position as f64 / elapsed.as_secs_f64())
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
    ProgressStyle::with_template(
        "    {spinner:.yellow} task [{elapsed_precise}] [{wide_bar:.yellow/blue}] {pos}/{len} {percent}% {msg}",
    )
    .unwrap()
    .progress_chars("#>-")
}

fn task_spinner_style() -> ProgressStyle {
    ProgressStyle::with_template("    {spinner:.yellow} task [{elapsed_precise}] {msg}").unwrap()
}
