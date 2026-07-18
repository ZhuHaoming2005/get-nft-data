use crate::{HardwareTopology, NativePlatformController, PlatformController, PlatformError};
use rayon::prelude::*;
use rayon::{ThreadPool, ThreadPoolBuilder};
use serde::Serialize;
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::sync_channel;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct WorkerPlacement {
    pub worker_index: usize,
    pub logical_cpu: u32,
    pub numa_node: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct NumaNodeExecutionMetrics {
    pub numa_node: u32,
    pub workers: usize,
    pub queue_capacity: usize,
    pub max_queue_depth: u64,
    pub scheduled_chunks: u64,
    pub data_locality_verified: bool,
    pub remote_chunks: Option<u64>,
}

struct NodeWorkerPool {
    pool: ThreadPool,
    metrics: Arc<NodeMetrics>,
}

#[derive(Debug)]
struct NodeMetrics {
    numa_node: u32,
    workers: usize,
    queue_capacity: usize,
    scheduled_chunks: AtomicU64,
    max_queue_depth: AtomicU64,
}

#[derive(Clone, Debug)]
pub struct NumaMetricsHandle {
    nodes: Vec<Arc<NodeMetrics>>,
}

impl NumaMetricsHandle {
    #[must_use]
    pub fn snapshot(&self) -> Vec<NumaNodeExecutionMetrics> {
        self.nodes
            .iter()
            .map(|node| NumaNodeExecutionMetrics {
                numa_node: node.numa_node,
                workers: node.workers,
                queue_capacity: node.queue_capacity,
                max_queue_depth: node.max_queue_depth.load(Ordering::Relaxed),
                scheduled_chunks: node.scheduled_chunks.load(Ordering::Relaxed),
                data_locality_verified: false,
                remote_chunks: None,
            })
            .collect()
    }
}

#[derive(Debug)]
pub enum NumaExecutionError<E> {
    Platform(PlatformError),
    Task(E),
}

struct BoundedTaskQueue {
    capacity: usize,
    values: VecDeque<usize>,
}

impl BoundedTaskQueue {
    fn new(capacity: usize) -> Result<Self, PlatformError> {
        if capacity == 0 {
            return Err(PlatformError::InvalidData {
                field: "worker_queue_capacity",
                value: "0".to_owned(),
            });
        }
        Ok(Self {
            capacity,
            values: VecDeque::with_capacity(capacity),
        })
    }

    fn push(&mut self, value: usize) -> Result<(), PlatformError> {
        if self.values.len() == self.capacity {
            return Err(PlatformError::InvalidData {
                field: "worker_queue_depth",
                value: self.values.len().to_string(),
            });
        }
        self.values.push_back(value);
        Ok(())
    }

    fn pop(&mut self) -> Option<usize> {
        self.values.pop_front()
    }

    fn len(&self) -> usize {
        self.values.len()
    }
}

pub struct NumaWorkerPool {
    node_pools: Vec<NodeWorkerPool>,
    placements: Vec<WorkerPlacement>,
    binding_enforced: bool,
    queue_capacity: usize,
}

impl NumaWorkerPool {
    #[must_use]
    pub fn placements(&self) -> &[WorkerPlacement] {
        &self.placements
    }

    #[must_use]
    pub fn binding_enforced(&self) -> bool {
        self.binding_enforced
    }

    #[must_use]
    pub fn node_count(&self) -> usize {
        self.node_pools.len()
    }

    #[must_use]
    pub fn execution_metrics(&self) -> Vec<NumaNodeExecutionMetrics> {
        self.metrics_handle().snapshot()
    }

    #[must_use]
    pub fn metrics_handle(&self) -> NumaMetricsHandle {
        NumaMetricsHandle {
            nodes: self
                .node_pools
                .iter()
                .map(|node| Arc::clone(&node.metrics))
                .collect(),
        }
    }

    #[must_use]
    pub fn worker_count(&self) -> usize {
        self.placements.len()
    }

    pub fn map_chunks<T, R, F, E>(
        &self,
        items: &[T],
        chunk_size: usize,
        map: F,
    ) -> Result<Vec<R>, NumaExecutionError<E>>
    where
        T: Sync,
        R: Send,
        E: Send,
        F: Fn(&[T]) -> Result<R, E> + Send + Sync,
    {
        if chunk_size == 0 {
            return Err(NumaExecutionError::Platform(PlatformError::InvalidData {
                field: "chunk_size",
                value: "0".to_owned(),
            }));
        }
        if items.is_empty() {
            return Ok(Vec::new());
        }
        let chunk_count = items.len().div_ceil(chunk_size);
        let mut next_chunk = 0_usize;
        let mut indexed_results = Vec::with_capacity(chunk_count);
        while next_chunk < chunk_count {
            let mut queues = (0..self.node_pools.len())
                .map(|_| BoundedTaskQueue::new(self.queue_capacity))
                .collect::<Result<Vec<_>, _>>()
                .map_err(NumaExecutionError::Platform)?;
            let wave_capacity = self
                .queue_capacity
                .checked_mul(queues.len())
                .ok_or_else(|| {
                    NumaExecutionError::Platform(PlatformError::InvalidData {
                        field: "worker_queue_capacity",
                        value: "overflow".to_owned(),
                    })
                })?;
            let wave_end = next_chunk.saturating_add(wave_capacity).min(chunk_count);
            while next_chunk < wave_end {
                let node_index = next_chunk % queues.len();
                queues[node_index]
                    .push(next_chunk)
                    .map_err(NumaExecutionError::Platform)?;
                next_chunk += 1;
            }
            let node_tasks = queues
                .into_iter()
                .map(|mut queue| {
                    let mut tasks = Vec::with_capacity(queue.len());
                    while let Some(task) = queue.pop() {
                        tasks.push(task);
                    }
                    tasks
                })
                .collect::<Vec<_>>();
            for (node, tasks) in self.node_pools.iter().zip(&node_tasks) {
                node.metrics.max_queue_depth.fetch_max(
                    u64::try_from(tasks.len()).unwrap_or(u64::MAX),
                    Ordering::Relaxed,
                );
            }
            let wave_results = std::thread::scope(|scope| {
                let mut handles = Vec::with_capacity(self.node_pools.len());
                for (node, tasks) in self.node_pools.iter().zip(node_tasks) {
                    if tasks.is_empty() {
                        continue;
                    }
                    let map_ref = &map;
                    handles.push(scope.spawn(move || {
                        let task_count = u64::try_from(tasks.len()).unwrap_or(u64::MAX);
                        let values = node.pool.install(|| {
                            tasks
                                .into_par_iter()
                                .map(|chunk_index| {
                                    let start = chunk_index.saturating_mul(chunk_size);
                                    let end = start.saturating_add(chunk_size).min(items.len());
                                    (chunk_index, map_ref(&items[start..end]))
                                })
                                .collect::<Vec<_>>()
                        });
                        node.metrics
                            .scheduled_chunks
                            .fetch_add(task_count, Ordering::Relaxed);
                        values
                    }));
                }
                handles
                    .into_iter()
                    .map(|handle| {
                        handle.join().map_err(|_| {
                            NumaExecutionError::Platform(PlatformError::Io(
                                "NUMA worker coordinator panicked".to_owned(),
                            ))
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()
            })?;
            for node_results in wave_results {
                for (chunk_index, result) in node_results {
                    indexed_results.push((chunk_index, result.map_err(NumaExecutionError::Task)?));
                }
            }
        }
        indexed_results.sort_unstable_by_key(|(chunk_index, _)| *chunk_index);
        Ok(indexed_results
            .into_iter()
            .map(|(_, result)| result)
            .collect())
    }
}

pub fn plan_worker_placements(
    topology: &HardwareTopology,
    workers: usize,
) -> Result<Vec<WorkerPlacement>, PlatformError> {
    if workers == 0 {
        return Err(PlatformError::InvalidData {
            field: "worker_count",
            value: "0".to_owned(),
        });
    }
    if topology.allowed_logical_cpus.is_empty() {
        return Err(PlatformError::Missing("allowed logical CPUs"));
    }
    let node_cpus: Vec<(u32, Vec<u32>)> = topology
        .numa_nodes
        .iter()
        .filter_map(|node| {
            let cpus: Vec<_> = node
                .logical_cpus
                .iter()
                .copied()
                .filter(|cpu| topology.allowed_logical_cpus.contains(cpu))
                .collect();
            (!cpus.is_empty()).then_some((node.id, cpus))
        })
        .collect();
    if node_cpus.is_empty() {
        return Err(PlatformError::Missing("allowed NUMA node CPUs"));
    }
    let mut placements = Vec::with_capacity(workers);
    for worker_index in 0..workers {
        let (numa_node, cpus) = &node_cpus[worker_index % node_cpus.len()];
        let logical_cpu = cpus[(worker_index / node_cpus.len()) % cpus.len()];
        if topology.cpu_to_numa_node.get(&logical_cpu) != Some(numa_node) {
            return Err(PlatformError::InvalidData {
                field: "cpu_to_numa_node",
                value: logical_cpu.to_string(),
            });
        }
        placements.push(WorkerPlacement {
            worker_index,
            logical_cpu,
            numa_node: *numa_node,
        });
    }
    Ok(placements)
}

pub fn build_numa_worker_pool(
    topology: &HardwareTopology,
    workers: usize,
    queue_capacity: usize,
    thread_prefix: &str,
    require_binding: bool,
) -> Result<NumaWorkerPool, PlatformError> {
    if queue_capacity == 0 {
        return Err(PlatformError::InvalidData {
            field: "worker_queue_capacity",
            value: "0".to_owned(),
        });
    }
    let placements = plan_worker_placements(topology, workers)?;
    if require_binding && !crate::is_linux_platform() {
        return Err(PlatformError::Missing("Linux NUMA worker binding"));
    }
    let mut by_node = BTreeMap::<u32, Vec<WorkerPlacement>>::new();
    for placement in &placements {
        by_node
            .entry(placement.numa_node)
            .or_default()
            .push(*placement);
    }
    let mut node_pools = Vec::with_capacity(by_node.len());
    for (numa_node, node_placements) in by_node {
        let pool = build_node_pool(numa_node, &node_placements, thread_prefix, require_binding)?;
        node_pools.push(NodeWorkerPool {
            pool,
            metrics: Arc::new(NodeMetrics {
                numa_node,
                workers: node_placements.len(),
                queue_capacity,
                scheduled_chunks: AtomicU64::new(0),
                max_queue_depth: AtomicU64::new(0),
            }),
        });
    }
    Ok(NumaWorkerPool {
        node_pools,
        placements,
        binding_enforced: require_binding,
        queue_capacity,
    })
}

fn build_node_pool(
    numa_node: u32,
    placements: &[WorkerPlacement],
    thread_prefix: &str,
    require_binding: bool,
) -> Result<ThreadPool, PlatformError> {
    let shared = Arc::new(placements.to_vec());
    let name_placements = Arc::clone(&shared);
    let prefix = thread_prefix.to_owned();
    let builder = ThreadPoolBuilder::new()
        .num_threads(placements.len())
        .thread_name(move |index| {
            format!(
                "{prefix}-node{numa_node}-{}",
                name_placements[index].worker_index
            )
        });
    let pool = if require_binding {
        builder
            .spawn_handler(move |thread| {
                let placement = shared[thread.index()];
                let (sender, receiver) = sync_channel(1);
                std::thread::Builder::new()
                    .name(thread.name().unwrap_or("dedup-worker").to_owned())
                    .spawn(move || {
                        let controller = NativePlatformController;
                        let setup = controller
                            .set_current_thread_affinity(&[placement.logical_cpu])
                            .and_then(|()| controller.set_preferred_numa_node(placement.numa_node))
                            .map_err(|error| error.to_string());
                        let runnable = setup.is_ok();
                        let _ = sender.send(setup);
                        if runnable {
                            thread.run();
                        }
                    })?;
                match receiver.recv() {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(message)) => Err(std::io::Error::other(message)),
                    Err(error) => Err(std::io::Error::other(error)),
                }
            })
            .build()
    } else {
        builder.build()
    };
    pool.map_err(|error| PlatformError::Io(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NumaNode;

    fn topology() -> HardwareTopology {
        HardwareTopology {
            allowed_logical_cpus: vec![2, 4, 6, 8],
            physical_cores: 4,
            numa_nodes: vec![
                NumaNode {
                    id: 0,
                    logical_cpus: vec![2, 4],
                    memory_bytes: 1024,
                },
                NumaNode {
                    id: 1,
                    logical_cpus: vec![6, 8],
                    memory_bytes: 1024,
                },
            ],
            cpu_to_numa_node: BTreeMap::from([(2, 0), (4, 0), (6, 1), (8, 1)]),
            physical_memory: 2048,
            cgroup_memory_limit: 2048,
            cpu_quota_parallelism: None,
        }
    }

    #[test]
    fn placements_cover_allowed_cpus_and_their_nodes_deterministically() {
        let placements = plan_worker_placements(&topology(), 6).unwrap();
        assert_eq!(
            placements,
            vec![
                WorkerPlacement {
                    worker_index: 0,
                    logical_cpu: 2,
                    numa_node: 0,
                },
                WorkerPlacement {
                    worker_index: 1,
                    logical_cpu: 6,
                    numa_node: 1,
                },
                WorkerPlacement {
                    worker_index: 2,
                    logical_cpu: 4,
                    numa_node: 0,
                },
                WorkerPlacement {
                    worker_index: 3,
                    logical_cpu: 8,
                    numa_node: 1,
                },
                WorkerPlacement {
                    worker_index: 4,
                    logical_cpu: 2,
                    numa_node: 0,
                },
                WorkerPlacement {
                    worker_index: 5,
                    logical_cpu: 6,
                    numa_node: 1,
                },
            ]
        );
    }

    #[test]
    fn independent_node_pools_execute_bounded_local_queues_deterministically() {
        let pool = build_numa_worker_pool(&topology(), 4, 2, "test-pool", false).unwrap();
        let input = (0_u64..29).collect::<Vec<_>>();
        let output = pool
            .map_chunks(&input, 3, |chunk| Ok::<_, ()>(chunk.iter().sum::<u64>()))
            .unwrap();
        let expected = input
            .chunks(3)
            .map(|chunk| chunk.iter().sum::<u64>())
            .collect::<Vec<_>>();
        assert_eq!(output, expected);
        assert_eq!(pool.node_count(), 2);
        assert!(!pool.binding_enforced());
        let metrics = pool.execution_metrics();
        assert_eq!(
            metrics
                .iter()
                .map(|node| node.scheduled_chunks)
                .sum::<u64>(),
            10
        );
        assert!(metrics.iter().all(|node| !node.data_locality_verified));
        assert!(metrics.iter().all(|node| node.remote_chunks.is_none()));
        assert!(metrics.iter().all(|node| node.max_queue_depth <= 2));
    }

    #[test]
    fn topology_changes_do_not_change_output_order() {
        let one_node = HardwareTopology {
            numa_nodes: vec![NumaNode {
                id: 7,
                logical_cpus: vec![2, 4, 6, 8],
                memory_bytes: 2048,
            }],
            cpu_to_numa_node: BTreeMap::from([(2, 7), (4, 7), (6, 7), (8, 7)]),
            ..topology()
        };
        let input = (0_u64..37).collect::<Vec<_>>();
        let run = |topology: &HardwareTopology| {
            build_numa_worker_pool(topology, 4, 3, "stable", false)
                .unwrap()
                .map_chunks(&input, 4, |chunk| Ok::<_, ()>(chunk.to_vec()))
                .unwrap()
        };
        assert_eq!(run(&topology()), run(&one_node));
    }
}
