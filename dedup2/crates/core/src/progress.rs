/// Progress observer implemented by the CLI reporter.
pub trait ProgressObserver: Send + Sync {
    fn set_stage(&self, stage: &str);
    fn set_phase(&self, phase: &str);
    fn set_total(&self, total: Option<u64>);
    fn add_completed(&self, delta: u64);
    fn check_cancelled(&self) -> Result<(), crate::DedupError> {
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct NoopProgress;

impl ProgressObserver for NoopProgress {
    fn set_stage(&self, _stage: &str) {}
    fn set_phase(&self, _phase: &str) {}
    fn set_total(&self, _total: Option<u64>) {}
    fn add_completed(&self, _delta: u64) {}
}

/// EWMA throughput helper shared by the CLI reporter.
#[derive(Clone, Debug)]
pub struct EwmaEta {
    alpha: f64,
    rate: Option<f64>,
    positive_samples: u32,
}

impl EwmaEta {
    pub fn new(alpha: f64) -> Self {
        Self {
            alpha,
            rate: None,
            positive_samples: 0,
        }
    }

    pub fn observe(&mut self, items_per_sec: f64) {
        if !items_per_sec.is_finite() || items_per_sec <= 0.0 {
            return;
        }
        self.rate = Some(match self.rate {
            Some(prev) => self.alpha * items_per_sec + (1.0 - self.alpha) * prev,
            None => items_per_sec,
        });
        self.positive_samples = self.positive_samples.saturating_add(1);
    }

    pub fn confident(&self) -> bool {
        self.positive_samples >= 3
    }

    pub fn eta_secs(&self, remaining: u64) -> Option<f64> {
        let rate = self.rate?;
        if rate <= 0.0 {
            return None;
        }
        Some(remaining as f64 / rate)
    }

    pub fn rate(&self) -> Option<f64> {
        self.rate
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ewma_needs_three_samples_for_confidence() {
        let mut eta = EwmaEta::new(0.25);
        eta.observe(100.0);
        eta.observe(100.0);
        assert!(!eta.confident());
        eta.observe(100.0);
        assert!(eta.confident());
        let secs = eta.eta_secs(200).unwrap();
        assert!((secs - 2.0).abs() < 0.01);
    }
}
