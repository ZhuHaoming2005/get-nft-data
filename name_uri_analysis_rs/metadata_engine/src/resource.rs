//! Checked resident/transient memory admission for metadata phases.

use std::sync::{Arc, Mutex};
use thiserror::Error;

pub const GIB: u64 = 1024 * 1024 * 1024;
pub const ENCODE_HARD_TOP: u64 = 288 * GIB;
pub const MATCH_HARD_TOP: u64 = 384 * GIB;
pub const REQUIRED_HOST_HEADROOM: u64 = 64 * GIB;

pub const fn required_host_headroom(host_total: u64) -> u64 {
    let proportional = host_total / 8;
    if proportional < REQUIRED_HOST_HEADROOM {
        proportional
    } else {
        REQUIRED_HOST_HEADROOM
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum MemoryError {
    #[error("host headroom insufficient: total {host_total}, engine top {hard_top}, required {required}")]
    Headroom {
        host_total: u64,
        hard_top: u64,
        required: u64,
    },
    #[error("memory reservation overflow")]
    Overflow,
    #[error("memory budget exceeded: requested {requested}, used {used}, hard top {hard_top}")]
    Budget {
        requested: u64,
        used: u64,
        hard_top: u64,
    },
}

struct Inner {
    hard_top: u64,
    used: u64,
}
#[derive(Clone)]
pub struct MemoryBroker {
    inner: Arc<Mutex<Inner>>,
}
pub struct MemoryLease {
    inner: Arc<Mutex<Inner>>,
    bytes: u64,
}

impl MemoryBroker {
    pub fn new(host_total: u64, hard_top: u64) -> Result<Self, MemoryError> {
        let required = required_host_headroom(host_total);
        if host_total.saturating_sub(hard_top) < required {
            return Err(MemoryError::Headroom {
                host_total,
                hard_top,
                required,
            });
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(Inner { hard_top, used: 0 })),
        })
    }
    pub fn reserve(&self, bytes: u64) -> Result<MemoryLease, MemoryError> {
        let mut g = self.inner.lock().expect("memory broker lock");
        let next = g.used.checked_add(bytes).ok_or(MemoryError::Overflow)?;
        if next > g.hard_top {
            return Err(MemoryError::Budget {
                requested: bytes,
                used: g.used,
                hard_top: g.hard_top,
            });
        }
        g.used = next;
        Ok(MemoryLease {
            inner: self.inner.clone(),
            bytes,
        })
    }
    /// Deterministic lane cap derived from an admitted per-lane fixed-width layout.
    pub fn active_lanes(
        &self,
        requested_threads: usize,
        fixed_bytes: u64,
        bytes_per_lane: u64,
    ) -> usize {
        if requested_threads == 0 || bytes_per_lane == 0 {
            return usize::from(requested_threads > 0);
        }
        let g = self.inner.lock().expect("memory broker lock");
        let available = g
            .hard_top
            .saturating_sub(g.used)
            .saturating_sub(fixed_bytes);
        requested_threads.min((available / bytes_per_lane) as usize)
    }
}

impl MemoryLease {
    pub fn resize(&mut self, new_bytes: u64) -> Result<(), MemoryError> {
        if new_bytes == self.bytes {
            return Ok(());
        }
        let mut g = self.inner.lock().expect("memory broker lock");
        if new_bytes > self.bytes {
            let add = new_bytes - self.bytes;
            let next = g.used.checked_add(add).ok_or(MemoryError::Overflow)?;
            if next > g.hard_top {
                return Err(MemoryError::Budget {
                    requested: add,
                    used: g.used,
                    hard_top: g.hard_top,
                });
            }
            g.used = next;
        } else {
            let sub = self.bytes - new_bytes;
            g.used -= sub;
        }
        self.bytes = new_bytes;
        Ok(())
    }
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}
impl Drop for MemoryLease {
    fn drop(&mut self) {
        let mut g = self.inner.lock().expect("memory broker lock");
        g.used = g.used.saturating_sub(self.bytes);
    }
}
