//! Checked resident/transient memory admission for metadata phases.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex, Weak,
};
use thiserror::Error;

pub const GIB: u64 = 1024 * 1024 * 1024;
pub const ENCODE_HARD_TOP: u64 = 288 * GIB;
pub const MATCH_HARD_TOP: u64 = 448 * GIB;
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
    reclaimable: Vec<Weak<AtomicU64>>,
    reclaimed: u64,
}
#[derive(Clone)]
pub struct MemoryBroker {
    inner: Arc<Mutex<Inner>>,
}
pub struct MemoryLease {
    inner: Arc<Mutex<Inner>>,
    bytes: Arc<AtomicU64>,
}

impl MemoryBroker {
    pub fn new(_host_total: u64, hard_top: u64) -> Result<Self, MemoryError> {
        Ok(Self {
            inner: Arc::new(Mutex::new(Inner {
                hard_top,
                used: 0,
                reclaimable: Vec::new(),
                reclaimed: 0,
            })),
        })
    }

    pub fn reserve(&self, bytes: u64) -> Result<MemoryLease, MemoryError> {
        self.reserve_inner(bytes, false)
    }

    /// Reserve a best-effort resident cache whose accounting may be reclaimed
    /// by later non-reclaimable work. The underlying mmap remains valid and
    /// naturally degrades to operating-system demand paging.
    pub fn reserve_reclaimable(&self, bytes: u64) -> Result<MemoryLease, MemoryError> {
        self.reserve_inner(bytes, true)
    }

    /// Atomically reserve as much mandatory scratch as currently fits, capped
    /// by `maximum`. Reclaimable mmap accounting is released first when needed.
    ///
    /// This is the admission primitive for bounded fallbacks: unlike
    /// `available_bytes()` followed by `reserve()`, it cannot lose a
    /// check-then-reserve race and turn a recoverable low-memory condition into
    /// a pipeline error.
    pub fn reserve_up_to(&self, maximum: u64) -> Result<MemoryLease, MemoryError> {
        let mut g = self.inner.lock().expect("memory broker lock");
        let bytes = maximum.min(effective_available_locked(&g));
        let mut next = g.used.checked_add(bytes).ok_or(MemoryError::Overflow)?;
        if next > g.hard_top {
            let excess = next - g.hard_top;
            reclaim_locked(&mut g, excess, None);
            next = g.used.checked_add(bytes).ok_or(MemoryError::Overflow)?;
        }
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
            bytes: Arc::new(AtomicU64::new(bytes)),
        })
    }

    fn reserve_inner(&self, bytes: u64, reclaimable: bool) -> Result<MemoryLease, MemoryError> {
        let mut g = self.inner.lock().expect("memory broker lock");
        let mut next = g.used.checked_add(bytes).ok_or(MemoryError::Overflow)?;
        if next > g.hard_top && !reclaimable {
            if bytes > effective_available_locked(&g) {
                return Err(MemoryError::Budget {
                    requested: bytes,
                    used: g.used,
                    hard_top: g.hard_top,
                });
            }
            let required = next - g.hard_top;
            reclaim_locked(&mut g, required, None);
            next = g.used.checked_add(bytes).ok_or(MemoryError::Overflow)?;
        }
        if next > g.hard_top {
            return Err(MemoryError::Budget {
                requested: bytes,
                used: g.used,
                hard_top: g.hard_top,
            });
        }
        g.used = next;
        let lease = MemoryLease {
            inner: self.inner.clone(),
            bytes: Arc::new(AtomicU64::new(bytes)),
        };
        if reclaimable {
            g.reclaimable.push(Arc::downgrade(&lease.bytes));
        }
        Ok(lease)
    }

    pub fn used_bytes(&self) -> u64 {
        self.inner.lock().expect("memory broker lock").used
    }
    pub fn hard_top_bytes(&self) -> u64 {
        self.inner.lock().expect("memory broker lock").hard_top
    }

    /// Bytes not currently assigned to any lease. Unlike `available_bytes`,
    /// this excludes reclaimable mmap residency and is used when sizing the
    /// reclaimable lease itself.
    pub fn unreserved_bytes(&self) -> u64 {
        let g = self.inner.lock().expect("memory broker lock");
        g.hard_top.saturating_sub(g.used)
    }

    /// Bytes available to mandatory work, including resident mmap cache that
    /// can be converted to demand-paged access.
    pub fn available_bytes(&self) -> u64 {
        let g = self.inner.lock().expect("memory broker lock");
        effective_available_locked(&g)
    }

    pub fn reclaimed_bytes(&self) -> u64 {
        self.inner.lock().expect("memory broker lock").reclaimed
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
        let available = effective_available_locked(&g).saturating_sub(fixed_bytes);
        requested_threads.min((available / bytes_per_lane) as usize)
    }

    /// Atomically choose and reserve the largest admitted lane count.
    ///
    /// This closes the check-then-reserve gap of `active_lanes` followed by
    /// `reserve`, which matters when other workers retain memory between those
    /// two operations. A zero-lane result carries a zero-byte lease; callers
    /// must select an explicit streaming/spill fallback rather than forcing
    /// one unbudgeted lane.
    pub fn reserve_lanes(
        &self,
        requested_threads: usize,
        fixed_bytes: u64,
        bytes_per_lane: u64,
    ) -> Result<(usize, MemoryLease), MemoryError> {
        let mut g = self.inner.lock().expect("memory broker lock");
        let available = effective_available_locked(&g);
        let lanes = if requested_threads == 0 || fixed_bytes > available {
            0
        } else {
            requested_threads.min(
                usize::try_from(
                    available
                        .saturating_sub(fixed_bytes)
                        .checked_div(bytes_per_lane)
                        .unwrap_or(u64::MAX),
                )
                .unwrap_or(usize::MAX),
            )
        };
        let bytes = if lanes == 0 {
            0
        } else {
            fixed_bytes
                .checked_add(
                    bytes_per_lane
                        .checked_mul(lanes as u64)
                        .ok_or(MemoryError::Overflow)?,
                )
                .ok_or(MemoryError::Overflow)?
        };
        let mut next = g.used.checked_add(bytes).ok_or(MemoryError::Overflow)?;
        if next > g.hard_top {
            let required = next - g.hard_top;
            reclaim_locked(&mut g, required, None);
            next = g.used.checked_add(bytes).ok_or(MemoryError::Overflow)?;
        }
        if next > g.hard_top {
            return Err(MemoryError::Budget {
                requested: bytes,
                used: g.used,
                hard_top: g.hard_top,
            });
        }
        g.used = next;
        Ok((
            lanes,
            MemoryLease {
                inner: self.inner.clone(),
                bytes: Arc::new(AtomicU64::new(bytes)),
            },
        ))
    }
}

impl MemoryLease {
    pub fn resize(&mut self, new_bytes: u64) -> Result<(), MemoryError> {
        let mut g = self.inner.lock().expect("memory broker lock");
        let current = self.bytes.load(Ordering::Relaxed);
        if new_bytes == current {
            return Ok(());
        }
        if new_bytes > current {
            let add = new_bytes - current;
            let mut next = g.used.checked_add(add).ok_or(MemoryError::Overflow)?;
            if next > g.hard_top {
                let available = g
                    .hard_top
                    .saturating_sub(g.used)
                    .saturating_add(reclaimable_bytes_locked(&g, Some(&self.bytes)));
                if add > available {
                    return Err(MemoryError::Budget {
                        requested: add,
                        used: g.used,
                        hard_top: g.hard_top,
                    });
                }
                let required = next - g.hard_top;
                reclaim_locked(&mut g, required, Some(&self.bytes));
                next = g.used.checked_add(add).ok_or(MemoryError::Overflow)?;
            }
            if next > g.hard_top {
                return Err(MemoryError::Budget {
                    requested: add,
                    used: g.used,
                    hard_top: g.hard_top,
                });
            }
            g.used = next;
        } else {
            let sub = current - new_bytes;
            g.used = g.used.saturating_sub(sub);
        }
        self.bytes.store(new_bytes, Ordering::Relaxed);
        Ok(())
    }

    pub fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }
}

impl Drop for MemoryLease {
    fn drop(&mut self) {
        let mut g = self.inner.lock().expect("memory broker lock");
        let bytes = self.bytes.swap(0, Ordering::Relaxed);
        g.used = g.used.saturating_sub(bytes);
    }
}

fn reclaimable_bytes_locked(inner: &Inner, excluded: Option<&Arc<AtomicU64>>) -> u64 {
    inner
        .reclaimable
        .iter()
        .filter_map(Weak::upgrade)
        .filter(|bytes| !excluded.is_some_and(|excluded| Arc::ptr_eq(excluded, bytes)))
        .map(|bytes| bytes.load(Ordering::Relaxed))
        .fold(0u64, u64::saturating_add)
}

fn effective_available_locked(inner: &Inner) -> u64 {
    inner
        .hard_top
        .saturating_sub(inner.used)
        .saturating_add(reclaimable_bytes_locked(inner, None))
}

fn reclaim_locked(inner: &mut Inner, requested: u64, excluded: Option<&Arc<AtomicU64>>) -> u64 {
    let mut remaining = requested;
    let mut reclaimed = 0u64;
    let reclaimable = std::mem::take(&mut inner.reclaimable);
    inner.reclaimable.reserve(reclaimable.len());
    for weak in reclaimable {
        let Some(bytes) = weak.upgrade() else {
            continue;
        };
        let is_excluded = excluded.is_some_and(|excluded| Arc::ptr_eq(excluded, &bytes));
        if !is_excluded && remaining > 0 {
            let current = bytes.load(Ordering::Relaxed);
            let release = current.min(remaining);
            if release > 0 {
                bytes.store(current - release, Ordering::Relaxed);
                inner.used = inner.used.saturating_sub(release);
                inner.reclaimed = inner.reclaimed.saturating_add(release);
                reclaimed = reclaimed.saturating_add(release);
                remaining -= release;
            }
        }
        inner.reclaimable.push(Arc::downgrade(&bytes));
    }
    reclaimed
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserve_lanes_atomically_caps_and_charges_the_selected_layout() {
        let broker = MemoryBroker::new(512 * GIB, 448 * GIB).unwrap();
        let _occupied = broker.reserve(400 * GIB).unwrap();

        let (lanes, lease) = broker.reserve_lanes(8, 8 * GIB, 10 * GIB).unwrap();

        assert_eq!(lanes, 4);
        assert_eq!(lease.bytes(), 48 * GIB);
        assert_eq!(broker.used_bytes(), 448 * GIB);
    }

    #[test]
    fn reserve_lanes_returns_zero_without_charging_fixed_bytes() {
        let broker = MemoryBroker::new(512 * GIB, 448 * GIB).unwrap();
        let _occupied = broker.reserve(448 * GIB).unwrap();

        let (lanes, lease) = broker.reserve_lanes(8, GIB, GIB).unwrap();

        assert_eq!(lanes, 0);
        assert_eq!(lease.bytes(), 0);
        assert_eq!(broker.used_bytes(), 448 * GIB);
    }

    #[test]
    fn reserve_up_to_uses_remaining_capacity_without_budget_failure() {
        let broker = MemoryBroker::new(512 * GIB, 448 * GIB).unwrap();
        let _occupied = broker.reserve(447 * GIB).unwrap();

        let lease = broker.reserve_up_to(64 * GIB).unwrap();

        assert_eq!(lease.bytes(), GIB);
        assert_eq!(broker.used_bytes(), 448 * GIB);
    }

    #[test]
    fn reserve_up_to_reclaims_mmap_residency_before_bounded_fallback() {
        let broker = MemoryBroker::new(512 * GIB, 448 * GIB).unwrap();
        let mmap = broker.reserve_reclaimable(32 * GIB).unwrap();
        let _occupied = broker.reserve(416 * GIB).unwrap();

        let lease = broker.reserve_up_to(16 * GIB).unwrap();

        assert_eq!(lease.bytes(), 16 * GIB);
        assert_eq!(mmap.bytes(), 16 * GIB);
        assert_eq!(broker.used_bytes(), 448 * GIB);
    }

    #[test]
    fn mandatory_reservation_reclaims_demand_paged_residency() {
        let broker = MemoryBroker::new(512 * GIB, 448 * GIB).unwrap();
        let snapshot = broker.reserve_reclaimable(400 * GIB).unwrap();

        let work = broker.reserve(100 * GIB).unwrap();

        assert_eq!(snapshot.bytes(), 348 * GIB);
        assert_eq!(work.bytes(), 100 * GIB);
        assert_eq!(broker.used_bytes(), 448 * GIB);
        assert_eq!(broker.reclaimed_bytes(), 52 * GIB);
        drop(work);
        assert_eq!(broker.used_bytes(), 348 * GIB);
    }

    #[test]
    fn lane_admission_counts_reclaimable_snapshot_pages() {
        let broker = MemoryBroker::new(512 * GIB, 448 * GIB).unwrap();
        let snapshot = broker.reserve_reclaimable(448 * GIB).unwrap();

        let (lanes, work) = broker.reserve_lanes(8, 8 * GIB, 10 * GIB).unwrap();

        assert_eq!(lanes, 8);
        assert_eq!(work.bytes(), 88 * GIB);
        assert_eq!(snapshot.bytes(), 360 * GIB);
        assert_eq!(broker.used_bytes(), 448 * GIB);
    }

    #[test]
    fn rejected_oversized_work_does_not_evict_snapshot_pages() {
        let broker = MemoryBroker::new(512 * GIB, 448 * GIB).unwrap();
        let snapshot = broker.reserve_reclaimable(400 * GIB).unwrap();

        let error = broker
            .reserve(449 * GIB)
            .err()
            .expect("oversized work must be rejected");

        assert!(matches!(error, MemoryError::Budget { .. }));
        assert_eq!(snapshot.bytes(), 400 * GIB);
        assert_eq!(broker.used_bytes(), 400 * GIB);
        assert_eq!(broker.reclaimed_bytes(), 0);
    }
}
