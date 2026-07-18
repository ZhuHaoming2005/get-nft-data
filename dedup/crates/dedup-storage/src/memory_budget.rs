use dedup_model::{DedupError, ErrorContext};
use std::sync::{Arc, Condvar, Mutex};

#[derive(Clone, Debug)]
pub struct MemoryBudget {
    inner: Arc<BudgetState>,
}

#[derive(Debug)]
struct BudgetState {
    available: u64,
    stage_limit: u64,
    in_memory_admission_limit: u64,
    used: Mutex<u64>,
    released: Condvar,
}

#[derive(Debug)]
pub struct MemoryLease {
    state: Arc<BudgetState>,
    bytes: u64,
}

#[derive(Clone, Debug)]
pub struct NodeMemoryBudget {
    central: MemoryBudget,
    state: Arc<NodeBudgetState>,
}

#[derive(Debug)]
struct NodeBudgetState {
    limit: u64,
    used: Mutex<u64>,
}

#[derive(Debug)]
pub struct NodeMemoryLease {
    central: MemoryLease,
    state: Arc<NodeBudgetState>,
    bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaseDecision {
    Wait,
    Spill,
}

impl MemoryBudget {
    pub fn new(physical_memory: u64, cgroup_memory_limit: u64) -> Self {
        let available = physical_memory.min(cgroup_memory_limit);
        let stage_limit = available.saturating_mul(75) / 100;
        let in_memory_admission_limit = stage_limit / 2;
        Self {
            inner: Arc::new(BudgetState {
                available,
                stage_limit,
                in_memory_admission_limit,
                used: Mutex::new(0),
                released: Condvar::new(),
            }),
        }
    }

    pub fn available(&self) -> u64 {
        self.inner.available
    }

    pub fn stage_limit(&self) -> u64 {
        self.inner.stage_limit
    }

    pub fn in_memory_admission_limit(&self) -> u64 {
        self.inner.in_memory_admission_limit
    }

    pub fn used(&self) -> u64 {
        *self.inner.used.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn try_lease(&self, bytes: u64) -> Result<MemoryLease, LeaseDecision> {
        let mut used = self.inner.used.lock().unwrap_or_else(|e| e.into_inner());
        let Some(next) = used.checked_add(bytes) else {
            return Err(LeaseDecision::Spill);
        };
        if next > self.inner.stage_limit {
            return Err(if bytes <= self.inner.stage_limit {
                LeaseDecision::Wait
            } else {
                LeaseDecision::Spill
            });
        }
        *used = next;
        Ok(MemoryLease {
            state: Arc::clone(&self.inner),
            bytes,
        })
    }

    pub fn require_lease(&self, bytes: u64) -> Result<MemoryLease, DedupError> {
        self.try_lease(bytes)
            .map_err(|_| DedupError::ResourceBudgetExceeded {
                context: ErrorContext::stage("memory_budget"),
                requested: bytes,
            })
    }

    pub fn split_node_budgets(&self, weights: &[u64]) -> Result<Vec<NodeMemoryBudget>, DedupError> {
        if weights.is_empty() || weights.contains(&0) {
            return Err(DedupError::InvalidInput {
                context: ErrorContext::stage("memory_budget"),
                message: "node budget weights must be non-empty and positive".to_owned(),
            });
        }
        let total_weight = weights.iter().try_fold(0_u64, |total, weight| {
            total
                .checked_add(*weight)
                .ok_or(DedupError::CounterOverflow {
                    counter: "node_budget_weights",
                })
        })?;
        let mut assigned = 0_u64;
        let mut budgets = Vec::with_capacity(weights.len());
        for (index, weight) in weights.iter().enumerate() {
            let limit = if index + 1 == weights.len() {
                self.stage_limit().saturating_sub(assigned)
            } else {
                let limit = self.stage_limit().saturating_mul(*weight) / total_weight;
                assigned = assigned.saturating_add(limit);
                limit
            };
            budgets.push(NodeMemoryBudget {
                central: self.clone(),
                state: Arc::new(NodeBudgetState {
                    limit,
                    used: Mutex::new(0),
                }),
            });
        }
        Ok(budgets)
    }
}

impl MemoryLease {
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl Drop for MemoryLease {
    fn drop(&mut self) {
        let mut used = self.state.used.lock().unwrap_or_else(|e| e.into_inner());
        *used = used.saturating_sub(self.bytes);
        self.state.released.notify_one();
    }
}

impl NodeMemoryBudget {
    pub fn limit(&self) -> u64 {
        self.state.limit
    }

    pub fn used(&self) -> u64 {
        *self
            .state
            .used
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    pub fn try_lease(&self, bytes: u64) -> Result<NodeMemoryLease, LeaseDecision> {
        let mut used = self
            .state
            .used
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Some(next) = used.checked_add(bytes) else {
            return Err(LeaseDecision::Spill);
        };
        if next > self.state.limit {
            return Err(if bytes <= self.state.limit {
                LeaseDecision::Wait
            } else {
                LeaseDecision::Spill
            });
        }
        let central = self.central.try_lease(bytes)?;
        *used = next;
        Ok(NodeMemoryLease {
            central,
            state: Arc::clone(&self.state),
            bytes,
        })
    }
}

impl NodeMemoryLease {
    pub fn bytes(&self) -> u64 {
        self.central.bytes()
    }
}

impl Drop for NodeMemoryLease {
    fn drop(&mut self) {
        let mut used = self
            .state
            .used
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        *used = used.saturating_sub(self.bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limits_and_raii_are_exact() {
        let budget = MemoryBudget::new(1_000, 800);
        assert_eq!(budget.available(), 800);
        assert_eq!(budget.stage_limit(), 600);
        assert_eq!(budget.in_memory_admission_limit(), 300);
        let lease = budget.require_lease(400).unwrap();
        assert_eq!(lease.bytes(), 400);
        assert_eq!(budget.used(), 400);
        assert_eq!(budget.try_lease(300).unwrap_err(), LeaseDecision::Wait);
        drop(lease);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn oversize_allocation_spills_without_retry() {
        let budget = MemoryBudget::new(1_000, 1_000);
        assert_eq!(budget.try_lease(751).unwrap_err(), LeaseDecision::Spill);
    }

    #[test]
    fn node_sub_budgets_share_and_never_exceed_central_budget() {
        let central = MemoryBudget::new(1_000, 1_000);
        let nodes = central.split_node_budgets(&[1, 2]).unwrap();
        assert_eq!(nodes.iter().map(NodeMemoryBudget::limit).sum::<u64>(), 750);
        let first = nodes[0].try_lease(250).unwrap();
        let second = nodes[1].try_lease(500).unwrap();
        assert_eq!(central.used(), 750);
        assert!(nodes[0].try_lease(1).is_err());
        drop((first, second));
        assert_eq!(central.used(), 0);
        assert_eq!(nodes[0].used(), 0);
    }
}
