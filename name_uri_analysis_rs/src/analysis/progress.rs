use super::*;

pub(crate) enum ProgressTracker {
    Enabled {
        _multi: MultiProgress,
        overall: ProgressBar,
        detail: ProgressBar,
    },
    Disabled,
}

impl ProgressTracker {
    pub(crate) fn new(total_phases: u64, enabled: bool) -> Self {
        if !enabled {
            return Self::Disabled;
        }
        let multi = MultiProgress::new();
        let overall = multi.add(ProgressBar::new(total_phases));
        overall.set_style(
            ProgressStyle::with_template(
                "{spinner:.green} overall [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} {msg}",
            )
            .unwrap()
            .progress_chars("#>-"),
        );
        let detail = multi.add(ProgressBar::new(0));
        detail.set_style(
            ProgressStyle::with_template(
                "  {spinner:.blue} current [{elapsed_precise}] [{wide_bar:.magenta/blue}] {pos}/{len} {percent}% {msg}",
            )
            .unwrap()
            .progress_chars("#>-"),
        );
        Self::Enabled {
            _multi: multi,
            overall,
            detail,
        }
    }

    pub(crate) fn start_phase(&self, message: impl Into<String>, work_units: u64) {
        let Self::Enabled {
            overall, detail, ..
        } = self
        else {
            return;
        };
        let message = message.into();
        overall.set_message(message.clone());
        detail.reset();
        detail.set_length(work_units);
        detail.set_position(0);
        detail.set_message(message);
    }

    pub(crate) fn add_work(&self, units: u64) {
        if let Self::Enabled { detail, .. } = self {
            detail.inc_length(units);
        }
    }

    pub(crate) fn step(&self, message: impl Into<String>) {
        if let Self::Enabled { detail, .. } = self {
            detail.set_message(message.into());
            detail.inc(1);
        }
    }

    pub(crate) fn inc(&self, units: u64) {
        if let Self::Enabled { detail, .. } = self {
            detail.inc(units);
        }
    }

    pub(crate) fn set_message(&self, message: impl Into<String>) {
        if let Self::Enabled { detail, .. } = self {
            detail.set_message(message.into());
        }
    }

    pub(crate) fn finish_phase(&self, message: impl Into<String>) {
        let Self::Enabled {
            overall, detail, ..
        } = self
        else {
            return;
        };
        let message = message.into();
        detail.finish_with_message(message.clone());
        overall.inc(1);
        overall.set_message(message);
    }

    pub(crate) fn finish(&self) {
        if let Self::Enabled {
            overall, detail, ..
        } = self
        {
            detail.finish_with_message("analysis complete; writing outputs finished");
            overall.finish_with_message("analysis complete; writing outputs finished");
        }
    }
}

