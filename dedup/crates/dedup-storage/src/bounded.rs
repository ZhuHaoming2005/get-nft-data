use dedup_model::{DedupError, ErrorContext};
use std::collections::VecDeque;

pub type WorkerQueue<T> = BoundedQueue<T>;
pub type WriterQueue<T> = BoundedQueue<T>;
pub type CandidateBuffer<T> = BoundedBuffer<T>;
pub type PairReducerBuffer<T> = BoundedBuffer<T>;
pub type PerWorkerArena = BoundedBuffer<u8>;
pub type LshProbeAccumulator<T> = BoundedBuffer<T>;
pub type SpillFileSet<T> = BoundedBuffer<T>;

#[derive(Clone, Debug)]
pub struct BoundedQueue<T> {
    capacity: usize,
    values: VecDeque<T>,
}

impl<T> BoundedQueue<T> {
    pub fn new(capacity: usize) -> Result<Self, DedupError> {
        if capacity == 0 {
            return Err(DedupError::InvalidInput {
                context: ErrorContext::stage("bounded_queue"),
                message: "capacity must be positive".to_owned(),
            });
        }
        Ok(Self {
            capacity,
            values: VecDeque::with_capacity(capacity),
        })
    }

    pub fn push(&mut self, value: T) -> Result<(), T> {
        if self.values.len() == self.capacity {
            return Err(value);
        }
        self.values.push_back(value);
        Ok(())
    }

    pub fn pop(&mut self) -> Option<T> {
        self.values.pop_front()
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

#[derive(Clone, Debug)]
pub struct BoundedBuffer<T> {
    capacity: usize,
    values: Vec<T>,
}

impl<T> BoundedBuffer<T> {
    pub fn new(capacity: usize) -> Result<Self, DedupError> {
        if capacity == 0 {
            return Err(DedupError::InvalidInput {
                context: ErrorContext::stage("bounded_buffer"),
                message: "capacity must be positive".to_owned(),
            });
        }
        Ok(Self {
            capacity,
            values: Vec::new(),
        })
    }

    pub fn push(&mut self, value: T) -> Result<(), T> {
        if self.values.len() == self.capacity {
            return Err(value);
        }
        self.values.push(value);
        Ok(())
    }

    pub fn drain(&mut self) -> impl Iterator<Item = T> + '_ {
        self.values.drain(..)
    }

    pub fn as_slice(&self) -> &[T] {
        &self.values
    }

    pub fn as_mut_slice(&mut self) -> &mut [T] {
        &mut self.values
    }

    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        self.values.get_mut(index)
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut T> {
        self.values.iter_mut()
    }

    pub fn clear(&mut self) {
        self.values.clear();
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}
