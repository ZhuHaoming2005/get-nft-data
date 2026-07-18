/// Low-overhead work reporting contract shared by storage and engine crates.
///
/// Progress is deliberately expressed in processed work units rather than hits.
/// Implementations may sample or batch updates, so callers must not depend on
/// callbacks being rendered synchronously.
pub trait ProgressObserver: Send + Sync {
    fn begin_phase(&self, phase: &'static str, total: Option<u64>);

    fn set_total(&self, total: u64);

    fn advance(&self, amount: u64);

    fn is_cancelled(&self) -> bool {
        false
    }

    fn check_cancelled(&self, stage: &'static str) -> Result<(), crate::DedupError> {
        if self.is_cancelled() {
            Err(crate::DedupError::Interrupted { stage })
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NoopProgress;

impl ProgressObserver for NoopProgress {
    fn begin_phase(&self, _phase: &'static str, _total: Option<u64>) {}

    fn set_total(&self, _total: u64) {}

    fn advance(&self, _amount: u64) {}
}
