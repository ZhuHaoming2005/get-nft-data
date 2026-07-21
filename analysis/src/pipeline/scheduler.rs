use crate::pipeline::CpuTaskKind;
use std::collections::VecDeque;

/// A bounded weighted queue. The payload is stored directly in the selected
/// priority lane, avoiding an ID-to-job side table and a second lookup on pop.
pub struct WeightedScheduler<T> {
    capacity: usize,
    queues: [VecDeque<T>; 4],
    cursor: usize,
    owner_shards_open: bool,
}

impl<T> WeightedScheduler<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            queues: std::array::from_fn(|_| VecDeque::new()),
            cursor: 0,
            owner_shards_open: true,
        }
    }

    pub fn set_owner_shards_open(&mut self, open: bool) {
        self.owner_shards_open = open;
    }

    pub fn try_push(&mut self, kind: CpuTaskKind, task: T) -> std::result::Result<(), T> {
        if self.len() >= self.capacity {
            return Err(task);
        }
        self.queues[kind_index(kind)].push_back(task);
        Ok(())
    }

    pub fn pop(&mut self) -> Option<T> {
        const OPEN: [CpuTaskKind; 8] = [
            CpuTaskKind::Dedup,
            CpuTaskKind::Dedup,
            CpuTaskKind::Dedup,
            CpuTaskKind::ResponseDecode,
            CpuTaskKind::ResponseDecode,
            CpuTaskKind::Analysis,
            CpuTaskKind::Compress,
            CpuTaskKind::Dedup,
        ];
        const CLOSED: [CpuTaskKind; 8] = [
            CpuTaskKind::Analysis,
            CpuTaskKind::Analysis,
            CpuTaskKind::Analysis,
            CpuTaskKind::ResponseDecode,
            CpuTaskKind::ResponseDecode,
            CpuTaskKind::Compress,
            CpuTaskKind::Analysis,
            CpuTaskKind::Compress,
        ];
        let weights = if self.owner_shards_open {
            &OPEN
        } else {
            &CLOSED
        };
        for _ in 0..weights.len() {
            let kind = weights[self.cursor % weights.len()];
            self.cursor = self.cursor.wrapping_add(1);
            if let Some(task) = self.queues[kind_index(kind)].pop_front() {
                return Some(task);
            }
        }
        self.queues.iter_mut().find_map(VecDeque::pop_front)
    }

    pub fn len(&self) -> usize {
        self.queues.iter().map(VecDeque::len).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.queues.iter().all(VecDeque::is_empty)
    }
}

fn kind_index(kind: CpuTaskKind) -> usize {
    match kind {
        CpuTaskKind::Dedup => 0,
        CpuTaskKind::ResponseDecode => 1,
        CpuTaskKind::Analysis => 2,
        CpuTaskKind::Compress => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_queue_backpressures_without_dropping_existing_tasks() {
        let mut scheduler = WeightedScheduler::new(1);
        scheduler.try_push(CpuTaskKind::Dedup, 1).unwrap();
        assert_eq!(scheduler.try_push(CpuTaskKind::Analysis, 2), Err(2));
        assert_eq!(scheduler.pop(), Some(1));
    }

    #[test]
    fn open_owner_shards_follow_the_documented_weighted_round_robin() {
        let mut scheduler = WeightedScheduler::new(8);
        let kinds = [
            CpuTaskKind::Dedup,
            CpuTaskKind::Dedup,
            CpuTaskKind::Dedup,
            CpuTaskKind::Dedup,
            CpuTaskKind::ResponseDecode,
            CpuTaskKind::ResponseDecode,
            CpuTaskKind::Analysis,
            CpuTaskKind::Compress,
        ];
        for kind in kinds {
            scheduler.try_push(kind, kind).unwrap();
        }
        let popped = (0..8).map(|_| scheduler.pop().unwrap()).collect::<Vec<_>>();
        assert_eq!(
            popped,
            [
                CpuTaskKind::Dedup,
                CpuTaskKind::Dedup,
                CpuTaskKind::Dedup,
                CpuTaskKind::ResponseDecode,
                CpuTaskKind::ResponseDecode,
                CpuTaskKind::Analysis,
                CpuTaskKind::Compress,
                CpuTaskKind::Dedup,
            ]
        );
    }
}
