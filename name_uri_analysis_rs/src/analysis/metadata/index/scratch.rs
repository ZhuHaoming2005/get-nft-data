#[cfg(test)]
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex;

use super::super::{metadata_doc_index_from_usize, metadata_doc_index_to_usize, MetadataDocIndex};

use super::*;

impl MetadataCandidateSet {
    pub(in super::super) fn from_sparse(
        candidates: Vec<MetadataDocIndex>,
        universe_len: usize,
    ) -> Self {
        let dense_threshold = universe_len
            .saturating_div(METADATA_DENSE_CANDIDATE_UNIVERSE_DIVISOR)
            .max(METADATA_DENSE_CANDIDATE_MIN_COUNT);
        Self::from_sparse_at_threshold(candidates, universe_len, dense_threshold, None)
    }

    #[cfg(test)]
    pub(in super::super) fn from_sparse_with_threshold(
        candidates: Vec<MetadataDocIndex>,
        universe_len: usize,
        dense_threshold: usize,
    ) -> Self {
        Self::from_sparse_at_threshold(candidates, universe_len, dense_threshold, None)
    }

    #[cfg(test)]
    pub(in super::super) fn from_pooled_sparse_with_threshold(
        candidates: Vec<MetadataDocIndex>,
        universe_len: usize,
        dense_threshold: usize,
        pool: Arc<MetadataCandidateBufferPool>,
    ) -> Self {
        Self::from_sparse_at_threshold(candidates, universe_len, dense_threshold, Some(pool))
    }

    pub(in super::super) fn from_pooled_sparse(
        candidates: Vec<MetadataDocIndex>,
        universe_len: usize,
        pool: Arc<MetadataCandidateBufferPool>,
    ) -> Self {
        let dense_threshold = universe_len
            .saturating_div(METADATA_DENSE_CANDIDATE_UNIVERSE_DIVISOR)
            .max(METADATA_DENSE_CANDIDATE_MIN_COUNT);
        Self::from_sparse_at_threshold(candidates, universe_len, dense_threshold, Some(pool))
    }

    fn from_sparse_at_threshold(
        candidates: Vec<MetadataDocIndex>,
        universe_len: usize,
        dense_threshold: usize,
        pool: Option<Arc<MetadataCandidateBufferPool>>,
    ) -> Self {
        if candidates.len() <= dense_threshold {
            return Self::Sparse(MetadataSparseCandidateBuffer { candidates, pool });
        }
        let len = candidates.len();
        let (mut words, mut touched_words) = pool
            .as_ref()
            .map(|pool| pool.take_dense())
            .unwrap_or_else(|| (vec![0u64; universe_len.saturating_add(63) / 64], Vec::new()));
        for candidate in &candidates {
            let index = metadata_doc_index_to_usize(*candidate);
            debug_assert!(index < universe_len);
            let word_index = index / 64;
            if words[word_index] == 0 {
                touched_words.push(word_index);
            }
            words[word_index] |= 1u64 << (index % 64);
        }
        if let Some(pool) = &pool {
            pool.release_sparse(candidates);
        }
        Self::Dense(MetadataDenseCandidateBitmap {
            words,
            touched_words,
            len,
            pool,
        })
    }

    pub(in super::super) fn iter(&self) -> MetadataCandidateSetIter<'_> {
        match self {
            Self::Sparse(buffer) => {
                MetadataCandidateSetIter::Sparse(buffer.candidates.iter().copied())
            }
            Self::Dense(bitmap) => MetadataCandidateSetIter::Dense(bitmap.iter()),
        }
    }

    pub(in super::super) fn len(&self) -> usize {
        match self {
            Self::Sparse(buffer) => buffer.candidates.len(),
            Self::Dense(bitmap) => bitmap.len,
        }
    }

    pub(in super::super) fn is_dense(&self) -> bool {
        matches!(self, Self::Dense(_))
    }
}

impl MetadataCandidateBufferPool {
    pub(in super::super) fn new(universe_len: usize, maximum_retained: usize) -> Self {
        Self {
            universe_len,
            maximum_retained: maximum_retained.max(1),
            sparse: Mutex::new(Vec::new()),
            dense: Mutex::new(Vec::new()),
        }
    }

    pub(in super::super) fn take_sparse(&self) -> Vec<MetadataDocIndex> {
        self.sparse
            .try_lock()
            .ok()
            .and_then(|mut buffers| buffers.pop())
            .unwrap_or_default()
    }

    pub(in super::super) fn release_sparse(&self, mut candidates: Vec<MetadataDocIndex>) {
        candidates.clear();
        if let Ok(mut buffers) = self.sparse.try_lock() {
            if buffers.len() < self.maximum_retained {
                buffers.push(candidates);
            }
        }
    }

    pub(in super::super) fn take_dense(&self) -> (Vec<u64>, Vec<usize>) {
        self.dense
            .try_lock()
            .ok()
            .and_then(|mut buffers| buffers.pop())
            .unwrap_or_else(|| {
                (
                    vec![0; self.universe_len.saturating_add(63) / 64],
                    Vec::new(),
                )
            })
    }

    fn release_dense(&self, words: Vec<u64>, mut touched_words: Vec<usize>) {
        debug_assert!(touched_words.iter().all(|&index| words[index] == 0));
        touched_words.clear();
        if let Ok(mut buffers) = self.dense.try_lock() {
            if buffers.len() < self.maximum_retained {
                buffers.push((words, touched_words));
            }
        }
    }
}

impl Drop for MetadataSparseCandidateBuffer {
    fn drop(&mut self) {
        let Some(pool) = self.pool.take() else {
            return;
        };
        pool.release_sparse(std::mem::take(&mut self.candidates));
    }
}

impl Drop for MetadataDenseCandidateBitmap {
    fn drop(&mut self) {
        let Some(pool) = self.pool.take() else {
            return;
        };
        for &word_index in &self.touched_words {
            self.words[word_index] = 0;
        }
        pool.release_dense(
            std::mem::take(&mut self.words),
            std::mem::take(&mut self.touched_words),
        );
    }
}

impl MetadataDenseCandidateBitmap {
    fn iter(&self) -> MetadataDenseCandidateBitmapIter<'_> {
        MetadataDenseCandidateBitmapIter {
            words: &self.words,
            word_index: 0,
            remaining_word: self.words.first().copied().unwrap_or(0),
        }
    }
}

impl Iterator for MetadataCandidateSetIter<'_> {
    type Item = MetadataDocIndex;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Sparse(iter) => iter.next(),
            Self::Dense(iter) => iter.next(),
        }
    }
}

impl Iterator for MetadataDenseCandidateBitmapIter<'_> {
    type Item = MetadataDocIndex;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.remaining_word != 0 {
                let bit = self.remaining_word.trailing_zeros() as usize;
                self.remaining_word &= self.remaining_word - 1;
                return Some(metadata_doc_index_from_usize(
                    self.word_index.saturating_mul(64).saturating_add(bit),
                ));
            }
            self.word_index = self.word_index.saturating_add(1);
            self.remaining_word = *self.words.get(self.word_index)?;
        }
    }
}

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
            visited_posting_entries: 0,
            fallback_token_exclusion: MetadataFallbackTokenExclusionScratch::new(doc_count),
        }
    }

    pub(in super::super) fn clear_for_next_left(&mut self) {
        self.candidates.clear();
        self.raw_candidate_count = 0;
        self.visited_posting_entries = 0;
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

    pub(in super::super) fn record_posting_visits(&mut self, visits: usize) {
        self.visited_posting_entries = self.visited_posting_entries.saturating_add(visits as u64);
    }

    pub(in super::super) fn prepare_secondary_generation(&mut self) {
        let visited_posting_entries = self.visited_posting_entries;
        std::mem::swap(
            &mut self.seen_generation,
            &mut self.secondary_seen_generation,
        );
        std::mem::swap(&mut self.generation, &mut self.secondary_generation);
        std::mem::swap(&mut self.candidates, &mut self.secondary_candidates);
        self.clear_for_next_left();
        self.visited_posting_entries = visited_posting_entries;
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
