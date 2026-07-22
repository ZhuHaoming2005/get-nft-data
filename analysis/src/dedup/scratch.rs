use crate::model::{NameValueId, ProfileId};
use ahash::AHashSet;
use std::cell::RefCell;

#[derive(Default)]
pub struct WorkerScratch {
    pub name_candidates: Vec<NameValueId>,
    pub metadata_candidates: Vec<ProfileId>,
    pub sparse_seen: AHashSet<u32>,
}

thread_local! {
    static WORKER_SCRATCH: RefCell<WorkerScratch> = RefCell::new(WorkerScratch::default());
}

pub fn with_worker_scratch<T>(operation: impl FnOnce(&mut WorkerScratch) -> T) -> T {
    WORKER_SCRATCH.with(|scratch| operation(&mut scratch.borrow_mut()))
}

pub fn release_uri_scratch() {
    WORKER_SCRATCH.with(|scratch| {
        scratch.borrow_mut().sparse_seen = AHashSet::new();
    });
}

pub fn release_name_scratch() {
    WORKER_SCRATCH.with(|scratch| {
        let mut scratch = scratch.borrow_mut();
        scratch.name_candidates = Vec::new();
    });
}

pub fn release_metadata_scratch() {
    WORKER_SCRATCH.with(|scratch| {
        let mut scratch = scratch.borrow_mut();
        scratch.metadata_candidates = Vec::new();
        scratch.sparse_seen = AHashSet::new();
    });
}

impl WorkerScratch {
    pub fn trim_oversized(&mut self, max_capacity: usize) {
        if self.name_candidates.capacity() > max_capacity {
            self.name_candidates = Vec::with_capacity(max_capacity);
        }
        if self.metadata_candidates.capacity() > max_capacity {
            self.metadata_candidates = Vec::with_capacity(max_capacity);
        }
    }
}
