use crate::PlatformError;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::thread::JoinHandle;
#[cfg(unix)]
use std::{thread, time::Duration};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum LifecycleState {
    #[default]
    Running,
    Draining,
    ControlledShutdown,
    ImmediateExit,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Signal {
    Term,
    Int,
    Hup,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SignalAction {
    None,
    ReloadLogLevel,
    StopIntakeAndCheckpoint,
    ControlledShutdown,
    ImmediateExit,
}

pub trait SignalSource {
    fn pending(&mut self) -> Result<Vec<Signal>, PlatformError>;
}

#[cfg(unix)]
pub struct NativeSignalSource {
    signals: signal_hook::iterator::Signals,
}

#[cfg(unix)]
impl NativeSignalSource {
    pub fn register() -> Result<Self, PlatformError> {
        use signal_hook::consts::signal::{SIGHUP, SIGINT, SIGTERM};
        signal_hook::iterator::Signals::new([SIGTERM, SIGINT, SIGHUP])
            .map(|signals| Self { signals })
            .map_err(|error| PlatformError::Io(error.to_string()))
    }
}

#[cfg(unix)]
impl SignalSource for NativeSignalSource {
    fn pending(&mut self) -> Result<Vec<Signal>, PlatformError> {
        use signal_hook::consts::signal::{SIGHUP, SIGINT, SIGTERM};
        self.signals
            .pending()
            .map(|signal| match signal {
                SIGTERM => Ok(Signal::Term),
                SIGINT => Ok(Signal::Int),
                SIGHUP => Ok(Signal::Hup),
                value => Err(PlatformError::InvalidData {
                    field: "signal",
                    value: value.to_string(),
                }),
            })
            .collect()
    }
}

#[cfg(not(unix))]
pub struct NativeSignalSource;

#[cfg(not(unix))]
impl NativeSignalSource {
    pub fn register() -> Result<Self, PlatformError> {
        Err(PlatformError::Missing("Unix signal handling"))
    }
}

#[cfg(not(unix))]
impl SignalSource for NativeSignalSource {
    fn pending(&mut self) -> Result<Vec<Signal>, PlatformError> {
        Err(PlatformError::Missing("Unix signal handling"))
    }
}

#[derive(Default)]
pub struct MockSignalSource {
    pending: std::collections::VecDeque<Signal>,
}

impl MockSignalSource {
    pub fn new(signals: impl IntoIterator<Item = Signal>) -> Self {
        Self {
            pending: signals.into_iter().collect(),
        }
    }
}

impl SignalSource for MockSignalSource {
    fn pending(&mut self) -> Result<Vec<Signal>, PlatformError> {
        Ok(self.pending.drain(..).collect())
    }
}

#[derive(Clone, Debug, Default)]
pub struct SignalStateMachine {
    state: LifecycleState,
}

#[derive(Clone, Debug)]
pub struct LifecycleHandle {
    state: Arc<AtomicU8>,
    log_reload_epoch: Arc<AtomicU64>,
}

impl LifecycleHandle {
    #[must_use]
    pub fn state(&self) -> LifecycleState {
        decode_state(self.state.load(Ordering::Acquire))
    }

    #[must_use]
    pub fn should_stop(&self) -> bool {
        matches!(
            self.state(),
            LifecycleState::ControlledShutdown | LifecycleState::ImmediateExit
        )
    }

    #[must_use]
    pub fn should_stop_intake(&self) -> bool {
        self.state() != LifecycleState::Running
    }

    #[must_use]
    pub fn log_reload_epoch(&self) -> u64 {
        self.log_reload_epoch.load(Ordering::Acquire)
    }
}

pub struct LifecycleMonitor {
    handle: LifecycleHandle,
    stopping: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl LifecycleMonitor {
    /// Installs the native Unix signal monitor. Non-Unix diagnostic builds
    /// receive an inactive handle.
    ///
    /// # Errors
    ///
    /// Returns a platform error when native Unix handlers cannot be registered
    /// or the monitor thread cannot be created.
    pub fn install_native() -> Result<Self, PlatformError> {
        install_native_monitor()
    }

    #[must_use]
    pub fn handle(&self) -> LifecycleHandle {
        self.handle.clone()
    }
}

impl Drop for LifecycleMonitor {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[cfg(unix)]
fn install_native_monitor() -> Result<LifecycleMonitor, PlatformError> {
    let mut source = NativeSignalSource::register()?;
    let state = Arc::new(AtomicU8::new(encode_state(LifecycleState::Running)));
    let log_reload_epoch = Arc::new(AtomicU64::new(0));
    let stopping = Arc::new(AtomicBool::new(false));
    let worker_state = Arc::clone(&state);
    let worker_reload = Arc::clone(&log_reload_epoch);
    let worker_stopping = Arc::clone(&stopping);
    let worker = thread::Builder::new()
        .name("dedup-signals".to_owned())
        .spawn(move || {
            let mut machine = SignalStateMachine::default();
            while !worker_stopping.load(Ordering::Acquire) {
                match source.pending() {
                    Ok(signals) => {
                        for signal in signals {
                            let action = machine.apply(signal);
                            worker_state.store(encode_state(machine.state()), Ordering::Release);
                            match action {
                                SignalAction::ReloadLogLevel => {
                                    worker_reload.fetch_add(1, Ordering::AcqRel);
                                }
                                SignalAction::ImmediateExit => std::process::exit(130),
                                SignalAction::None
                                | SignalAction::StopIntakeAndCheckpoint
                                | SignalAction::ControlledShutdown => {}
                            }
                        }
                    }
                    Err(_) => {
                        worker_state.store(
                            encode_state(LifecycleState::ControlledShutdown),
                            Ordering::Release,
                        );
                        break;
                    }
                }
                thread::sleep(Duration::from_millis(50));
            }
        })
        .map_err(|error| PlatformError::Io(error.to_string()))?;
    Ok(LifecycleMonitor {
        handle: LifecycleHandle {
            state,
            log_reload_epoch,
        },
        stopping,
        worker: Some(worker),
    })
}

#[cfg(not(unix))]
fn install_native_monitor() -> Result<LifecycleMonitor, PlatformError> {
    let state = Arc::new(AtomicU8::new(encode_state(LifecycleState::Running)));
    let log_reload_epoch = Arc::new(AtomicU64::new(0));
    Ok(LifecycleMonitor {
        handle: LifecycleHandle {
            state,
            log_reload_epoch,
        },
        stopping: Arc::new(AtomicBool::new(false)),
        worker: None,
    })
}

const fn encode_state(state: LifecycleState) -> u8 {
    match state {
        LifecycleState::Running => 0,
        LifecycleState::Draining => 1,
        LifecycleState::ControlledShutdown => 2,
        LifecycleState::ImmediateExit => 3,
    }
}

const fn decode_state(state: u8) -> LifecycleState {
    match state {
        1 => LifecycleState::Draining,
        2 => LifecycleState::ControlledShutdown,
        3 => LifecycleState::ImmediateExit,
        _ => LifecycleState::Running,
    }
}

impl SignalStateMachine {
    pub fn state(&self) -> LifecycleState {
        self.state
    }

    pub fn apply(&mut self, signal: Signal) -> SignalAction {
        match signal {
            Signal::Hup => SignalAction::ReloadLogLevel,
            Signal::Term if self.state == LifecycleState::Running => {
                self.state = LifecycleState::Draining;
                SignalAction::StopIntakeAndCheckpoint
            }
            Signal::Int if self.state != LifecycleState::ControlledShutdown => {
                self.state = LifecycleState::ControlledShutdown;
                SignalAction::ControlledShutdown
            }
            Signal::Int => {
                self.state = LifecycleState::ImmediateExit;
                SignalAction::ImmediateExit
            }
            Signal::Term => SignalAction::None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn second_sigint_exits_immediately_and_hup_has_no_semantic_effect() {
        let mut machine = SignalStateMachine::default();
        assert_eq!(machine.apply(Signal::Hup), SignalAction::ReloadLogLevel);
        assert_eq!(machine.state(), LifecycleState::Running);
        assert_eq!(machine.apply(Signal::Int), SignalAction::ControlledShutdown);
        assert_eq!(machine.apply(Signal::Int), SignalAction::ImmediateExit);
    }

    #[test]
    fn mock_source_drives_all_lifecycle_transitions() {
        let mut source =
            MockSignalSource::new([Signal::Term, Signal::Hup, Signal::Int, Signal::Int]);
        let mut machine = SignalStateMachine::default();
        let actions: Vec<_> = source
            .pending()
            .unwrap()
            .into_iter()
            .map(|signal| machine.apply(signal))
            .collect();
        assert_eq!(
            actions,
            [
                SignalAction::StopIntakeAndCheckpoint,
                SignalAction::ReloadLogLevel,
                SignalAction::ControlledShutdown,
                SignalAction::ImmediateExit,
            ]
        );
    }

    #[test]
    fn lifecycle_handle_exposes_atomic_shutdown_state() {
        let state = Arc::new(AtomicU8::new(encode_state(LifecycleState::Running)));
        let handle = LifecycleHandle {
            state: Arc::clone(&state),
            log_reload_epoch: Arc::new(AtomicU64::new(0)),
        };
        assert!(!handle.should_stop());
        assert!(!handle.should_stop_intake());
        state.store(encode_state(LifecycleState::Draining), Ordering::Release);
        assert!(!handle.should_stop());
        assert!(handle.should_stop_intake());
        state.store(
            encode_state(LifecycleState::ControlledShutdown),
            Ordering::Release,
        );
        assert!(handle.should_stop());
        assert_eq!(handle.state(), LifecycleState::ControlledShutdown);
    }
}
