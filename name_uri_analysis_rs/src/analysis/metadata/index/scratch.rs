#[cfg(test)]
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;

use super::super::{metadata_doc_index_to_usize, MetadataDocIndex};

use super::*;

#[cfg(test)]
impl MetadataHitPermits {
    pub(in super::super) fn new(remaining: usize) -> Self {
        Self {
            remaining: AtomicUsize::new(remaining),
            exceeded: AtomicBool::new(false),
        }
    }

    pub(in super::super) fn exceeded(&self) -> bool {
        self.exceeded.load(Ordering::Relaxed)
    }

    pub(in super::super) fn try_acquire(&self) -> bool {
        if self.exceeded() {
            return false;
        }
        if self
            .remaining
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            true
        } else {
            self.exceeded.store(true, Ordering::Relaxed);
            false
        }
    }
}

impl MetadataCandidateScratch {
    pub(in super::super) fn new(doc_count: usize) -> Self {
        Self {
            seen_generation: vec![0; doc_count],
            generation: 0,
            candidates: Vec::new(),
            secondary_seen_generation: vec![0; doc_count],
            secondary_generation: 0,
            secondary_candidates: Vec::new(),
            posting_plan: MetadataCandidatePostingPlan::default(),
            raw_candidate_count: 0,
        }
    }

    pub(in super::super) fn clear_for_next_left(&mut self) {
        self.candidates.clear();
        self.raw_candidate_count = 0;
        self.generation = self.generation.wrapping_add(1);
        if self.generation == 0 {
            self.seen_generation.fill(0);
            self.generation = 1;
        }
    }

    pub(in super::super) fn push_once(&mut self, index: MetadataDocIndex) {
        let index_usize = metadata_doc_index_to_usize(index);
        if self.seen_generation[index_usize] == self.generation {
            return;
        }
        self.seen_generation[index_usize] = self.generation;
        self.candidates.push(index);
    }

    pub(in super::super) fn prepare_secondary_generation(&mut self) {
        std::mem::swap(
            &mut self.seen_generation,
            &mut self.secondary_seen_generation,
        );
        std::mem::swap(&mut self.generation, &mut self.secondary_generation);
        std::mem::swap(&mut self.candidates, &mut self.secondary_candidates);
        self.clear_for_next_left();
    }

    pub(in super::super) fn retain_secondary_intersection(&mut self) {
        let secondary_generation = self.secondary_generation;
        let secondary_seen_generation = &self.secondary_seen_generation;
        self.candidates.retain(|&index| {
            secondary_seen_generation[metadata_doc_index_to_usize(index)] == secondary_generation
        });
    }
}

impl MetadataCandidateScratchPool {
    pub(in super::super) fn new(doc_count: usize) -> Self {
        Self {
            doc_count,
            scratches: Mutex::new(Vec::new()),
        }
    }

    pub(in super::super) fn take(&self) -> MetadataCandidateScratchLease<'_> {
        let scratch = {
            self.scratches
                .lock()
                .expect("metadata candidate scratch pool lock poisoned")
                .pop()
        };
        let scratch = scratch.unwrap_or_else(|| MetadataCandidateScratch::new(self.doc_count));
        MetadataCandidateScratchLease {
            pool: self,
            scratch: Some(scratch),
        }
    }
}

impl std::ops::Deref for MetadataCandidateScratchLease<'_> {
    type Target = MetadataCandidateScratch;

    fn deref(&self) -> &Self::Target {
        self.scratch
            .as_ref()
            .expect("metadata candidate scratch lease is empty")
    }
}

impl std::ops::DerefMut for MetadataCandidateScratchLease<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.scratch
            .as_mut()
            .expect("metadata candidate scratch lease is empty")
    }
}

impl Drop for MetadataCandidateScratchLease<'_> {
    fn drop(&mut self) {
        let Some(scratch) = self.scratch.take() else {
            return;
        };
        self.pool
            .scratches
            .lock()
            .expect("metadata candidate scratch pool lock poisoned")
            .push(scratch);
    }
}
