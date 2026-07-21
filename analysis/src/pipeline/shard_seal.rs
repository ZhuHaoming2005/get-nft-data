use crate::model::SeedId;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Debug)]
pub struct ShardWorkTracker {
    producer_closed: AtomicBool,
    seed_batches_inflight: AtomicUsize,
    failed_seed_bitmap: [AtomicU64; 2],
    sealed: AtomicBool,
}

impl Default for ShardWorkTracker {
    fn default() -> Self {
        Self {
            producer_closed: AtomicBool::new(false),
            seed_batches_inflight: AtomicUsize::new(0),
            failed_seed_bitmap: [AtomicU64::new(0), AtomicU64::new(0)],
            sealed: AtomicBool::new(false),
        }
    }
}

impl ShardWorkTracker {
    pub fn register_seed_batch(self: &Arc<Self>, seed: SeedId) -> WorkGuard {
        self.seed_batches_inflight.fetch_add(1, Ordering::AcqRel);
        WorkGuard::new(self.clone(), seed)
    }

    pub fn close_producer(&self) {
        self.producer_closed.store(true, Ordering::Release);
    }

    pub fn try_seal(&self) -> Option<DimensionShardSeal> {
        let quiescent = self.producer_closed.load(Ordering::Acquire)
            && self.seed_batches_inflight.load(Ordering::Acquire) == 0;
        if !quiescent
            || self
                .sealed
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
        {
            return None;
        }
        Some(DimensionShardSeal {
            failed_seed_bitmap: [
                self.failed_seed_bitmap[0].load(Ordering::Acquire),
                self.failed_seed_bitmap[1].load(Ordering::Acquire),
            ],
        })
    }

    fn complete(&self) {
        let previous = self.seed_batches_inflight.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0);
    }

    fn fail(&self, seed: SeedId) {
        let word = seed.index() / 64;
        let bit = seed.index() % 64;
        self.failed_seed_bitmap[word].fetch_or(1_u64 << bit, Ordering::AcqRel);
    }
}

#[derive(Debug)]
pub struct WorkGuard {
    tracker: Arc<ShardWorkTracker>,
    seed: SeedId,
    succeeded: bool,
}

impl WorkGuard {
    fn new(tracker: Arc<ShardWorkTracker>, seed: SeedId) -> Self {
        Self {
            tracker,
            seed,
            succeeded: false,
        }
    }

    pub fn succeed(mut self) {
        self.succeeded = true;
    }
}

impl Drop for WorkGuard {
    fn drop(&mut self) {
        if !self.succeeded {
            self.tracker.fail(self.seed);
        }
        self.tracker.complete();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DimensionShardSeal {
    pub failed_seed_bitmap: [u64; 2],
}

impl DimensionShardSeal {
    pub fn seed_failed(self, seed: SeedId) -> bool {
        self.failed_seed_bitmap[seed.index() / 64] & (1_u64 << (seed.index() % 64)) != 0
    }

    pub fn failed_seed_ids(self) -> impl Iterator<Item = SeedId> {
        self.failed_seed_bitmap
            .into_iter()
            .enumerate()
            .flat_map(|(word_index, word)| {
                (0..64)
                    .filter(move |&bit| word & (1_u64 << bit) != 0)
                    .map(move |bit| {
                        SeedId(
                            u16::try_from(word_index * 64 + bit)
                                .expect("failed seed bitmap index fits SeedId"),
                        )
                    })
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inflight_seed_batch_prevents_early_seal() {
        let tracker = Arc::new(ShardWorkTracker::default());
        let batch = tracker.register_seed_batch(SeedId(2));
        tracker.close_producer();
        assert!(tracker.try_seal().is_none());
        batch.succeed();
        assert_eq!(tracker.try_seal().unwrap().failed_seed_bitmap, [0, 0]);
    }

    #[test]
    fn dropped_failed_task_marks_exact_seed() {
        let tracker = Arc::new(ShardWorkTracker::default());
        drop(tracker.register_seed_batch(SeedId(3)));
        drop(tracker.register_seed_batch(SeedId(70)));
        tracker.close_producer();
        let seal = tracker.try_seal().unwrap();
        assert!(seal.seed_failed(SeedId(3)));
        assert!(seal.seed_failed(SeedId(70)));
        assert!(!seal.seed_failed(SeedId(4)));
        assert_eq!(
            seal.failed_seed_ids().collect::<Vec<_>>(),
            [SeedId(3), SeedId(70)]
        );
    }
}
