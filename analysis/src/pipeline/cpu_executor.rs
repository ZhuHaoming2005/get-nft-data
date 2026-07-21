use crate::error::{AnalysisError, Result};
use crate::platform::WorkerPlacement;
use parking_lot::{Condvar, Mutex};
use rayon::{ThreadPool, ThreadPoolBuilder};
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub struct CpuExecutor {
    lanes: Vec<CpuLane>,
    workers: usize,
    admission: Arc<Admission>,
    active: Arc<AtomicUsize>,
    next_lane: AtomicUsize,
}

struct CpuLane {
    pool: Arc<ThreadPool>,
    dispatcher: Arc<Dispatcher>,
}

impl CpuExecutor {
    pub fn new(workers: usize) -> Result<Self> {
        Self::build(workers, workers.saturating_mul(4).max(1), None)
    }

    pub fn new_bounded(workers: usize, queue_capacity: usize) -> Result<Self> {
        Self::build(workers, queue_capacity, None)
    }

    pub fn new_pinned(workers: usize, cpus: &[u32]) -> Result<Self> {
        Self::new_pinned_bounded(workers, workers.saturating_mul(4).max(1), cpus)
    }

    pub fn new_pinned_bounded(workers: usize, queue_capacity: usize, cpus: &[u32]) -> Result<Self> {
        if cpus.len() < workers {
            return Err(AnalysisError::Platform(format!(
                "received {} CPU assignments for {workers} workers",
                cpus.len()
            )));
        }
        let placements = cpus
            .iter()
            .copied()
            .map(|cpu| WorkerPlacement {
                cpu,
                numa_node: None,
            })
            .collect::<Vec<_>>();
        Self::build(workers, queue_capacity, Some(&placements))
    }

    pub fn new_numa_bounded(
        workers: usize,
        queue_capacity: usize,
        placements: &[WorkerPlacement],
    ) -> Result<Self> {
        if placements.len() < workers {
            return Err(AnalysisError::Platform(format!(
                "received {} NUMA assignments for {workers} workers",
                placements.len()
            )));
        }
        Self::build(workers, queue_capacity, Some(placements))
    }

    fn build(
        workers: usize,
        queue_capacity: usize,
        placements: Option<&[WorkerPlacement]>,
    ) -> Result<Self> {
        if workers == 0 || queue_capacity == 0 {
            return Err(AnalysisError::Config(
                "CpuExecutor requires positive worker and queue capacities".into(),
            ));
        }
        let lane_placements = partition_placements(workers, placements);
        let active = Arc::new(AtomicUsize::new(0));
        let scheduler_capacity = workers.saturating_add(queue_capacity);
        let mut lanes = Vec::with_capacity(lane_placements.len());
        for (lane_index, assignments) in lane_placements.into_iter().enumerate() {
            let lane_workers = assignments
                .as_ref()
                .map_or(workers, |assignments| assignments.len());
            let builder = ThreadPoolBuilder::new()
                .num_threads(lane_workers)
                .thread_name(move |index| format!("analysis-numa-{lane_index:02}-cpu-{index:03}"));
            #[cfg(target_os = "linux")]
            let (builder, startup_rx) = if let Some(assignments) = assignments {
                let (startup_tx, startup_rx) = std::sync::mpsc::channel();
                let builder = builder.start_handler(move |index| {
                    let placement = assignments[index];
                    let affinity = pin_current_thread(placement.cpu);
                    let numa = placement.numa_node.map_or(Ok(()), prefer_numa_node);
                    let _ = startup_tx.send((placement, affinity, numa));
                });
                (builder, Some(startup_rx))
            } else {
                (builder, None)
            };
            #[cfg(not(target_os = "linux"))]
            let _ = assignments;
            let pool = Arc::new(
                builder
                    .build()
                    .map_err(|error| AnalysisError::Platform(error.to_string()))?,
            );
            #[cfg(target_os = "linux")]
            if let Some(startup_rx) = startup_rx {
                for _ in 0..lane_workers {
                    let (placement, affinity, numa) = startup_rx.recv().map_err(|_| {
                        AnalysisError::Platform(
                            "NUMA worker exited before reporting startup state".into(),
                        )
                    })?;
                    if let Err(code) = affinity {
                        return Err(AnalysisError::Platform(format!(
                            "pthread_setaffinity_np failed for CPU {} with errno {code}",
                            placement.cpu
                        )));
                    }
                    if let Err(code) = numa {
                        return Err(AnalysisError::Platform(format!(
                            "set_mempolicy(MPOL_PREFERRED) failed for NUMA node {:?} with errno {code}",
                            placement.numa_node
                        )));
                    }
                }
            }
            lanes.push(CpuLane {
                pool,
                dispatcher: Arc::new(Dispatcher::new(
                    lane_workers,
                    scheduler_capacity,
                    active.clone(),
                )),
            });
        }
        Ok(Self {
            lanes,
            workers,
            admission: Arc::new(Admission::new(scheduler_capacity)),
            active,
            next_lane: AtomicUsize::new(0),
        })
    }

    pub fn workers(&self) -> usize {
        self.workers
    }

    pub fn utilization(&self) -> (usize, usize) {
        let active = self.active.load(Ordering::Relaxed);
        let inflight = self.admission.inflight();
        (active, inflight.saturating_sub(active))
    }

    pub fn numa_pool_count(&self) -> usize {
        self.lanes.len()
    }

    fn next_lane(&self) -> &CpuLane {
        let lane = self.next_lane.fetch_add(1, Ordering::Relaxed) % self.lanes.len();
        &self.lanes[lane]
    }

    fn lane_for<T: Hash>(&self, route: &T) -> &CpuLane {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        route.hash(&mut hasher);
        &self.lanes[hasher.finish() as usize % self.lanes.len()]
    }

    pub fn submit_kind_routed<T, F, R>(
        &self,
        kind: crate::pipeline::CpuTaskKind,
        route: R,
        task: F,
    ) -> SubmittedTask
    where
        T: Send + 'static,
        R: Hash,
        F: FnOnce() -> T + Send + 'static,
    {
        let lane = self.lane_for(&route);
        self.submit_kind_to_lane(lane, kind, task)
    }

    pub fn submit_kind_on_lane<T, F>(
        &self,
        kind: crate::pipeline::CpuTaskKind,
        lane: usize,
        task: F,
    ) -> SubmittedTask
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        self.submit_kind_to_lane(&self.lanes[lane % self.lanes.len()], kind, task)
    }

    fn submit_kind_to_lane<T, F>(
        &self,
        lane: &CpuLane,
        kind: crate::pipeline::CpuTaskKind,
        task: F,
    ) -> SubmittedTask
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        let permit = self.admission.acquire_blocking();
        lane.dispatcher.enqueue(
            &lane.pool,
            kind,
            Box::new(move || {
                let _permit = permit;
                drop(task());
            }),
        );
        SubmittedTask
    }

    pub fn install_on_all<T, F>(&self, operation: F) -> Vec<T>
    where
        T: Send,
        F: Fn(usize, usize) -> T + Send + Sync,
    {
        let lane_count = self.lanes.len();
        if lane_count == 1 {
            return vec![self.lanes[0].pool.install(|| operation(0, 1))];
        }
        let results = (0..lane_count)
            .map(|_| Mutex::new(None))
            .collect::<Vec<_>>();
        self.lanes[0].pool.scope(|scope| {
            for (lane, result) in results.iter().enumerate().skip(1) {
                let operation = &operation;
                let pool = &self.lanes[lane].pool;
                scope.spawn(move |_| {
                    *result.lock() = Some(pool.install(|| operation(lane, lane_count)));
                });
            }
            *results[0].lock() = Some(operation(0, lane_count));
        });
        results
            .into_iter()
            .map(|result| {
                result
                    .into_inner()
                    .expect("every NUMA lane publishes its install result")
            })
            .collect()
    }

    /// Executes a cleanup/maintenance hook once on every Rayon worker in every
    /// NUMA lane. This is intended for worker-local scratch that cannot be
    /// reached by one coordinator task per lane.
    pub fn broadcast(&self, operation: impl Fn() + Send + Sync) {
        for lane in &self.lanes {
            lane.pool.broadcast(|_| operation());
        }
    }

    pub fn submit_with_notification_kind_routed<T, F, R>(
        &self,
        kind: crate::pipeline::CpuTaskKind,
        route: R,
        notify: tokio::sync::mpsc::UnboundedSender<T>,
        task: F,
    ) where
        T: Send + 'static,
        R: Hash,
        F: FnOnce() -> T + Send + 'static,
    {
        let lane = self.lane_for(&route);
        self.submit_with_notification_kind_to_lane(lane, kind, notify, task);
    }

    fn submit_with_notification_kind_to_lane<T, F>(
        &self,
        lane: &CpuLane,
        kind: crate::pipeline::CpuTaskKind,
        notify: tokio::sync::mpsc::UnboundedSender<T>,
        task: F,
    ) where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        let permit = self.admission.acquire_blocking();
        lane.dispatcher.enqueue(
            &lane.pool,
            kind,
            Box::new(move || {
                let _permit = permit;
                let _ = notify.send(task());
            }),
        );
    }

    pub async fn execute_async_kind<T, F>(
        &self,
        kind: crate::pipeline::CpuTaskKind,
        task: F,
    ) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T> + Send + 'static,
    {
        let permit = self.admission.acquire_async().await;
        let (notify, receive) = tokio::sync::oneshot::channel();
        let lane = self.next_lane();
        lane.dispatcher.enqueue(
            &lane.pool,
            kind,
            Box::new(move || {
                let _permit = permit;
                let _ = notify.send(task());
            }),
        );
        receive
            .await
            .map_err(|_| AnalysisError::State("CPU completion channel closed".into()))?
    }

    pub fn install<T: Send>(&self, operation: impl FnOnce() -> T + Send) -> T {
        self.next_lane().pool.install(operation)
    }

    /// Runs a coordinator operation on one explicit NUMA lane. Cross-lane
    /// operations use lane zero so a single-worker remote lane is never held
    /// while waiting for work that must run on that same lane.
    pub(crate) fn install_on_lane<T: Send>(
        &self,
        lane: usize,
        operation: impl FnOnce() -> T + Send,
    ) -> T {
        self.lanes[lane % self.lanes.len()].pool.install(operation)
    }

    pub fn scope<'scope, T, F>(&self, operation: F) -> T
    where
        T: Send,
        F: FnOnce(&rayon::Scope<'scope>) -> T + Send,
    {
        self.next_lane().pool.scope(operation)
    }

    pub fn set_owner_shards_open(&self, open: bool) {
        for lane in &self.lanes {
            lane.dispatcher.set_owner_shards_open(open);
        }
    }
}

fn partition_placements(
    workers: usize,
    placements: Option<&[WorkerPlacement]>,
) -> Vec<Option<Vec<WorkerPlacement>>> {
    let Some(placements) = placements else {
        return vec![None];
    };
    if placements[..workers]
        .iter()
        .any(|placement| placement.numa_node.is_none())
    {
        return vec![Some(placements[..workers].to_vec())];
    }
    let mut by_node = BTreeMap::<u32, Vec<WorkerPlacement>>::new();
    for &placement in &placements[..workers] {
        by_node
            .entry(placement.numa_node.expect("checked above"))
            .or_default()
            .push(placement);
    }
    if by_node.len() > 1 {
        by_node.into_values().map(Some).collect()
    } else {
        vec![Some(placements[..workers].to_vec())]
    }
}

type ScheduledJob = Box<dyn FnOnce() + Send + 'static>;

struct Dispatcher {
    state: Mutex<DispatchState>,
    workers: usize,
    active: Arc<AtomicUsize>,
}

struct DispatchState {
    scheduler: crate::pipeline::WeightedScheduler<ScheduledJob>,
    running: usize,
}

impl Dispatcher {
    fn new(workers: usize, capacity: usize, active: Arc<AtomicUsize>) -> Self {
        Self {
            state: Mutex::new(DispatchState {
                scheduler: crate::pipeline::WeightedScheduler::new(capacity),
                running: 0,
            }),
            workers,
            active,
        }
    }

    fn enqueue(
        self: &Arc<Self>,
        pool: &Arc<ThreadPool>,
        kind: crate::pipeline::CpuTaskKind,
        job: ScheduledJob,
    ) {
        let jobs = {
            let mut state = self.state.lock();
            if state.scheduler.try_push(kind, job).is_err() {
                panic!("CPU admission and scheduler capacity diverged");
            }
            state.take_ready(self.workers)
        };
        self.spawn_jobs(pool, jobs);
    }

    fn complete(self: &Arc<Self>, pool: &Arc<ThreadPool>) {
        let jobs = {
            let mut state = self.state.lock();
            state.running = state
                .running
                .checked_sub(1)
                .expect("completed CPU task was not running");
            state.take_ready(self.workers)
        };
        self.spawn_jobs(pool, jobs);
    }

    fn spawn_jobs(self: &Arc<Self>, pool: &Arc<ThreadPool>, jobs: Vec<ScheduledJob>) {
        for job in jobs {
            let dispatcher = self.clone();
            let pool_for_completion = pool.clone();
            let active = self.active.clone();
            pool.spawn(move || {
                let _completion = DispatchCompletion {
                    dispatcher,
                    pool: pool_for_completion,
                };
                let _active = ActiveTask::new(active);
                job();
            });
        }
    }

    fn set_owner_shards_open(&self, open: bool) {
        self.state.lock().scheduler.set_owner_shards_open(open);
    }
}

impl DispatchState {
    fn take_ready(&mut self, workers: usize) -> Vec<ScheduledJob> {
        let mut jobs = Vec::new();
        while self.running < workers {
            let Some(job) = self.scheduler.pop() else {
                break;
            };
            self.running += 1;
            jobs.push(job);
        }
        jobs
    }
}

struct DispatchCompletion {
    dispatcher: Arc<Dispatcher>,
    pool: Arc<ThreadPool>,
}

impl Drop for DispatchCompletion {
    fn drop(&mut self) {
        self.dispatcher.complete(&self.pool);
    }
}

struct Admission {
    limit: usize,
    inflight: Mutex<usize>,
    wake_blocking: Condvar,
    wake_async: tokio::sync::Notify,
}

impl Admission {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            inflight: Mutex::new(0),
            wake_blocking: Condvar::new(),
            wake_async: tokio::sync::Notify::new(),
        }
    }

    fn acquire_blocking(self: &Arc<Self>) -> AdmissionPermit {
        let mut inflight = self.inflight.lock();
        while *inflight >= self.limit {
            self.wake_blocking.wait(&mut inflight);
        }
        *inflight += 1;
        AdmissionPermit {
            admission: self.clone(),
        }
    }

    async fn acquire_async(self: &Arc<Self>) -> AdmissionPermit {
        loop {
            let notified = self.wake_async.notified();
            {
                let mut inflight = self.inflight.lock();
                if *inflight < self.limit {
                    *inflight += 1;
                    return AdmissionPermit {
                        admission: self.clone(),
                    };
                }
            }
            notified.await;
        }
    }

    fn inflight(&self) -> usize {
        *self.inflight.lock()
    }
}

struct AdmissionPermit {
    admission: Arc<Admission>,
}

impl Drop for AdmissionPermit {
    fn drop(&mut self) {
        *self.admission.inflight.lock() -= 1;
        self.admission.wake_blocking.notify_one();
        self.admission.wake_async.notify_one();
    }
}

struct ActiveTask {
    active: Arc<AtomicUsize>,
}

impl ActiveTask {
    fn new(active: Arc<AtomicUsize>) -> Self {
        active.fetch_add(1, Ordering::Relaxed);
        Self { active }
    }
}

impl Drop for ActiveTask {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(target_os = "linux")]
fn pin_current_thread(cpu: u32) -> std::result::Result<(), i32> {
    if cpu as usize >= libc::CPU_SETSIZE as usize {
        return Err(libc::EINVAL);
    }
    let mut set = unsafe { std::mem::zeroed::<libc::cpu_set_t>() };
    unsafe {
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(cpu as usize, &mut set);
        let result = libc::pthread_setaffinity_np(
            libc::pthread_self(),
            std::mem::size_of::<libc::cpu_set_t>(),
            &set,
        );
        if result == 0 {
            Ok(())
        } else {
            Err(result)
        }
    }
}

#[cfg(target_os = "linux")]
fn prefer_numa_node(node: u32) -> std::result::Result<(), i32> {
    const MPOL_PREFERRED: libc::c_int = 1;
    let word_bits = usize::BITS as usize;
    let word_index = node as usize / word_bits;
    let mut mask = vec![0_usize; word_index + 1];
    mask[word_index] |= 1_usize << (node as usize % word_bits);
    let result = unsafe {
        libc::syscall(
            libc::SYS_set_mempolicy,
            MPOL_PREFERRED,
            mask.as_ptr(),
            libc::c_ulong::from(node) + 1,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or(libc::EINVAL))
    }
}

/// Zero-cost acknowledgement that a task was admitted. Results that matter are
/// returned through the caller-owned completion channel, not a second slot.
pub struct SubmittedTask;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_numa_topology_creates_node_local_worker_groups() {
        let placements = [
            WorkerPlacement {
                cpu: 0,
                numa_node: Some(0),
            },
            WorkerPlacement {
                cpu: 2,
                numa_node: Some(1),
            },
            WorkerPlacement {
                cpu: 1,
                numa_node: Some(0),
            },
            WorkerPlacement {
                cpu: 3,
                numa_node: Some(1),
            },
        ];
        let groups = partition_placements(4, Some(&placements));
        assert_eq!(groups.len(), 2);
        assert_eq!(
            groups[0]
                .as_ref()
                .unwrap()
                .iter()
                .map(|item| item.cpu)
                .collect::<Vec<_>>(),
            [0, 1]
        );
        assert_eq!(
            groups[1]
                .as_ref()
                .unwrap()
                .iter()
                .map(|item| item.cpu)
                .collect::<Vec<_>>(),
            [2, 3]
        );
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn install_on_all_executes_once_per_numa_lane() {
        let placements = [
            WorkerPlacement {
                cpu: 0,
                numa_node: Some(0),
            },
            WorkerPlacement {
                cpu: 2,
                numa_node: Some(1),
            },
            WorkerPlacement {
                cpu: 1,
                numa_node: Some(0),
            },
            WorkerPlacement {
                cpu: 3,
                numa_node: Some(1),
            },
        ];
        let executor = CpuExecutor::new_numa_bounded(4, 8, &placements).unwrap();
        assert_eq!(executor.install_on_all(|lane, _| lane), [0, 1]);
        let (left, right) = executor.install(|| {
            rayon::join(
                || executor.install_on_all(|lane, _| lane + 10),
                || executor.install_on_all(|lane, _| lane + 20),
            )
        });
        assert_eq!(left, [10, 11]);
        assert_eq!(right, [20, 21]);

        let constrained = CpuExecutor::new_numa_bounded(2, 4, &placements[..2]).unwrap();
        let results =
            constrained.install_on_lane(0, || constrained.install_on_all(|lane, _| lane + 30));
        assert_eq!(results, [30, 31]);
    }
}
