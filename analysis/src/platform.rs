use crate::config::NumaMode;
use crate::error::{AnalysisError, Result};
use serde::Serialize;
use std::collections::BTreeSet;
#[cfg(target_os = "linux")]
use std::fs;
#[cfg(any(target_os = "linux", test))]
use std::path::{Component, Path, PathBuf};
#[cfg(target_os = "linux")]
use std::sync::OnceLock;

#[cfg(target_os = "linux")]
const GIB: u64 = 1024 * 1024 * 1024;
#[cfg(target_os = "linux")]
const SYSTEM_RESERVE: u64 = 48 * GIB;
#[cfg(target_os = "linux")]
const REQUIRED_EFFECTIVE: u64 = 464 * GIB;
#[cfg(target_os = "linux")]
const CGROUP_V2_MOUNT: &str = "/sys/fs/cgroup";
#[cfg(target_os = "linux")]
static CURRENT_CGROUP_V2: OnceLock<PathBuf> = OnceLock::new();

#[derive(Clone, Debug, Serialize)]
pub struct PlatformResources {
    pub allowed_cpus: Vec<u32>,
    pub physical_memory: u64,
    pub cgroup_memory_limit: Option<u64>,
    pub effective_memory_limit: u64,
    pub numa_nodes: Vec<NumaNode>,
}

#[derive(Clone, Debug, Serialize)]
pub struct NumaNode {
    pub id: u32,
    pub cpus: Vec<u32>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerPlacement {
    pub cpu: u32,
    pub numa_node: Option<u32>,
}

impl PlatformResources {
    pub fn worker_cpus(&self, workers: usize) -> Result<Vec<u32>> {
        select_worker_cpus(&self.allowed_cpus, &self.numa_nodes, workers)
    }

    pub fn worker_placements(
        &self,
        mode: NumaMode,
        workers: usize,
    ) -> Result<Vec<WorkerPlacement>> {
        match mode {
            NumaMode::Auto => {
                select_worker_placements(&self.allowed_cpus, &self.numa_nodes, workers)
            }
        }
    }
}

pub fn select_worker_cpus(
    allowed_cpus: &[u32],
    numa_nodes: &[NumaNode],
    workers: usize,
) -> Result<Vec<u32>> {
    Ok(select_worker_placements(allowed_cpus, numa_nodes, workers)?
        .into_iter()
        .map(|placement| placement.cpu)
        .collect())
}

pub fn select_worker_placements(
    allowed_cpus: &[u32],
    numa_nodes: &[NumaNode],
    workers: usize,
) -> Result<Vec<WorkerPlacement>> {
    if allowed_cpus.len() < workers {
        return Err(AnalysisError::Platform(format!(
            "cpuset exposes {} CPUs; {workers} required",
            allowed_cpus.len()
        )));
    }
    let allowed = allowed_cpus.iter().copied().collect::<BTreeSet<_>>();
    let per_node = numa_nodes
        .iter()
        .map(|node| {
            (
                node.id,
                node.cpus
                    .iter()
                    .copied()
                    .filter(|cpu| allowed.contains(cpu))
                    .collect::<Vec<_>>(),
            )
        })
        .filter(|(_, cpus)| !cpus.is_empty())
        .collect::<Vec<_>>();
    let mut selected = Vec::with_capacity(workers);
    if per_node.len() > 1 {
        let mut cursor = vec![0_usize; per_node.len()];
        while selected.len() < workers {
            let mut advanced = false;
            for (node_index, (node_id, cpus)) in per_node.iter().enumerate() {
                if cursor[node_index] < cpus.len() {
                    selected.push(WorkerPlacement {
                        cpu: cpus[cursor[node_index]],
                        numa_node: Some(*node_id),
                    });
                    cursor[node_index] += 1;
                    advanced = true;
                    if selected.len() == workers {
                        break;
                    }
                }
            }
            if !advanced {
                break;
            }
        }
    }
    let selected_set = selected
        .iter()
        .map(|placement| placement.cpu)
        .collect::<BTreeSet<_>>();
    selected.extend(
        allowed_cpus
            .iter()
            .copied()
            .filter(|cpu| !selected_set.contains(cpu))
            .map(|cpu| WorkerPlacement {
                cpu,
                numa_node: None,
            })
            .take(workers.saturating_sub(selected.len())),
    );
    selected.truncate(workers);
    Ok(selected)
}

pub fn inspect_production_platform() -> Result<PlatformResources> {
    #[cfg(not(target_os = "linux"))]
    {
        Err(AnalysisError::Platform(
            "production run requires Linux cgroup v2; development tests may run elsewhere".into(),
        ))
    }
    #[cfg(target_os = "linux")]
    {
        let cgroup = current_cgroup_v2_dir()?;
        let cpuset = read_cpuset(&cgroup.join("cpuset.cpus.effective"))?;
        let affinity = current_process_affinity()?;
        let affinity = affinity.into_iter().collect::<BTreeSet<_>>();
        let allowed_cpus = cpuset
            .into_iter()
            .filter(|cpu| affinity.contains(cpu))
            .collect::<Vec<_>>();
        if allowed_cpus.len() < 128 {
            return Err(AnalysisError::Platform(format!(
                "effective cgroup cpuset/process affinity exposes {} CPUs; 128 required",
                allowed_cpus.len()
            )));
        }
        let physical_memory = physical_memory()?;
        let cgroup_memory_limit = read_effective_memory_max(cgroup, Path::new(CGROUP_V2_MOUNT))?;
        let available = cgroup_memory_limit
            .map(|limit| limit.min(physical_memory))
            .unwrap_or(physical_memory);
        let effective_memory_limit = available.saturating_sub(SYSTEM_RESERVE);
        if effective_memory_limit < REQUIRED_EFFECTIVE {
            return Err(AnalysisError::Platform(format!(
                "effective memory is {} GiB; at least 464 GiB required",
                effective_memory_limit / GIB
            )));
        }
        Ok(PlatformResources {
            allowed_cpus,
            physical_memory,
            cgroup_memory_limit,
            effective_memory_limit,
            numa_nodes: read_numa_nodes().unwrap_or_default(),
        })
    }
}

pub fn current_memory_usage() -> Result<Option<u64>> {
    #[cfg(not(target_os = "linux"))]
    {
        Ok(None)
    }
    #[cfg(target_os = "linux")]
    {
        let cgroup_current = current_cgroup_v2_dir()?.join("memory.current");
        if cgroup_current.is_file() {
            let raw = fs::read_to_string(&cgroup_current)?;
            return raw.trim().parse::<u64>().map(Some).map_err(|_| {
                AnalysisError::Platform(format!("invalid memory.current `{}`", raw.trim()))
            });
        }
        let raw = fs::read_to_string("/proc/self/statm")?;
        let resident_pages = raw
            .split_whitespace()
            .nth(1)
            .ok_or_else(|| AnalysisError::Platform("invalid /proc/self/statm".into()))?
            .parse::<u64>()
            .map_err(|_| AnalysisError::Platform("invalid resident page count".into()))?;
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if page_size <= 0 {
            return Err(AnalysisError::Platform(
                "sysconf could not read page size".into(),
            ));
        }
        Ok(Some(resident_pages.saturating_mul(page_size as u64)))
    }
}

pub fn parse_cpu_list(raw: &str) -> Result<Vec<u32>> {
    let mut cpus = Vec::new();
    for part in raw.trim().split(',').filter(|part| !part.is_empty()) {
        if let Some((start, end)) = part.split_once('-') {
            let start = start
                .parse::<u32>()
                .map_err(|_| AnalysisError::Platform(format!("invalid CPU range `{part}`")))?;
            let end = end
                .parse::<u32>()
                .map_err(|_| AnalysisError::Platform(format!("invalid CPU range `{part}`")))?;
            if start > end {
                return Err(AnalysisError::Platform(format!(
                    "descending CPU range `{part}`"
                )));
            }
            cpus.extend(start..=end);
        } else {
            cpus.push(
                part.parse::<u32>()
                    .map_err(|_| AnalysisError::Platform(format!("invalid CPU `{part}`")))?,
            );
        }
    }
    cpus.sort_unstable();
    cpus.dedup();
    Ok(cpus)
}

#[cfg(any(target_os = "linux", test))]
fn resolve_cgroup_v2_path(contents: &str, mount: &Path) -> Result<PathBuf> {
    let relative = contents
        .lines()
        .find_map(|line| {
            let mut fields = line.splitn(3, ':');
            let _hierarchy = fields.next()?;
            let controllers = fields.next()?;
            let path = fields.next()?;
            controllers.is_empty().then_some(path)
        })
        .ok_or_else(|| {
            AnalysisError::Platform("/proc/self/cgroup has no cgroup v2 entry".into())
        })?;
    let mut resolved = mount.to_path_buf();
    for component in Path::new(relative).components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::Normal(component) => resolved.push(component),
            Component::ParentDir | Component::Prefix(_) => {
                return Err(AnalysisError::Platform(format!(
                    "unsafe cgroup v2 path `{relative}`"
                )));
            }
        }
    }
    Ok(resolved)
}

#[cfg(target_os = "linux")]
fn current_cgroup_v2_dir() -> Result<&'static Path> {
    if let Some(path) = CURRENT_CGROUP_V2.get() {
        return Ok(path.as_path());
    }
    let contents = fs::read_to_string("/proc/self/cgroup")?;
    let path = resolve_cgroup_v2_path(&contents, Path::new(CGROUP_V2_MOUNT))?;
    let _ = CURRENT_CGROUP_V2.set(path);
    CURRENT_CGROUP_V2
        .get()
        .map(PathBuf::as_path)
        .ok_or_else(|| AnalysisError::Platform("could not cache current cgroup v2 path".into()))
}

#[cfg(target_os = "linux")]
fn read_cpuset(path: &Path) -> Result<Vec<u32>> {
    parse_cpu_list(&fs::read_to_string(path)?)
}

#[cfg(target_os = "linux")]
fn current_process_affinity() -> Result<Vec<u32>> {
    let mut set = unsafe { std::mem::zeroed::<libc::cpu_set_t>() };
    let status = unsafe {
        libc::sched_getaffinity(
            0,
            std::mem::size_of::<libc::cpu_set_t>(),
            std::ptr::addr_of_mut!(set),
        )
    };
    if status != 0 {
        return Err(AnalysisError::Platform(format!(
            "sched_getaffinity failed with errno {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok((0..libc::CPU_SETSIZE as usize)
        .filter(|cpu| unsafe { libc::CPU_ISSET(*cpu, &set) })
        .map(|cpu| cpu as u32)
        .collect())
}

#[cfg(target_os = "linux")]
fn physical_memory() -> Result<u64> {
    let pages = unsafe { libc::sysconf(libc::_SC_PHYS_PAGES) };
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if pages <= 0 || page_size <= 0 {
        return Err(AnalysisError::Platform(
            "sysconf could not read physical memory".into(),
        ));
    }
    Ok((pages as u64).saturating_mul(page_size as u64))
}

#[cfg(target_os = "linux")]
fn read_memory_max(path: &Path) -> Result<Option<u64>> {
    let raw = fs::read_to_string(path)?;
    let raw = raw.trim();
    if raw == "max" {
        Ok(None)
    } else {
        raw.parse::<u64>()
            .map(Some)
            .map_err(|_| AnalysisError::Platform(format!("invalid memory.max `{raw}`")))
    }
}

#[cfg(target_os = "linux")]
fn read_effective_memory_max(group: &Path, mount: &Path) -> Result<Option<u64>> {
    let mut current = group;
    let mut effective = None;
    loop {
        let memory_max = current.join("memory.max");
        if memory_max.is_file() {
            if let Some(limit) = read_memory_max(&memory_max)? {
                effective = Some(effective.map_or(limit, |known: u64| known.min(limit)));
            }
        }
        if current == mount {
            break;
        }
        current = current.parent().ok_or_else(|| {
            AnalysisError::Platform(format!(
                "cgroup path `{}` is outside `{}`",
                group.display(),
                mount.display()
            ))
        })?;
        if !current.starts_with(mount) {
            return Err(AnalysisError::Platform(format!(
                "cgroup path `{}` is outside `{}`",
                group.display(),
                mount.display()
            )));
        }
    }
    Ok(effective)
}

#[cfg(target_os = "linux")]
fn read_numa_nodes() -> Result<Vec<NumaNode>> {
    let mut nodes = Vec::new();
    for entry in fs::read_dir("/sys/devices/system/node")? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Some(id) = name.strip_prefix("node").and_then(|id| id.parse().ok()) else {
            continue;
        };
        let cpus = parse_cpu_list(&fs::read_to_string(entry.path().join("cpulist"))?)?;
        nodes.push(NumaNode { id, cpus });
    }
    nodes.sort_by_key(|node| node.id);
    Ok(nodes)
}

#[derive(Clone, Copy, Debug)]
pub struct MemoryPlan {
    pub long_lived: u64,
    pub current_dimension: u64,
    pub next_dimension: u64,
    pub worker_scratch: u64,
    pub candidate_inflight: u64,
    pub writer_queue: u64,
    pub allocator_reserve: u64,
}

impl MemoryPlan {
    pub fn choose_overlap(self, limit: u64, requested: bool) -> Result<bool> {
        let without_overlap = self
            .long_lived
            .saturating_add(self.current_dimension)
            .saturating_add(self.worker_scratch)
            .saturating_add(self.candidate_inflight)
            .saturating_add(self.writer_queue)
            .saturating_add(self.allocator_reserve);
        if without_overlap > limit {
            return Err(AnalysisError::MemoryBudget {
                required: without_overlap,
                limit,
            });
        }
        Ok(requested && without_overlap.saturating_add(self.next_dimension) <= limit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_linux_cpu_lists() {
        assert_eq!(parse_cpu_list("0-2,4,6-7").unwrap(), [0, 1, 2, 4, 6, 7]);
    }

    #[test]
    fn resolves_nested_cgroup_v2_path_without_escaping_mount() {
        let mount = Path::new("/sys/fs/cgroup");
        assert_eq!(
            resolve_cgroup_v2_path("0::/system.slice/analysis.service\n", mount).unwrap(),
            mount.join("system.slice/analysis.service")
        );
        assert!(resolve_cgroup_v2_path("0::/../../tmp\n", mount).is_err());
    }

    #[test]
    fn overlap_is_disabled_before_failing() {
        let plan = MemoryPlan {
            long_lived: 40,
            current_dimension: 20,
            next_dimension: 50,
            worker_scratch: 10,
            candidate_inflight: 10,
            writer_queue: 5,
            allocator_reserve: 5,
        };
        assert!(!plan.choose_overlap(100, true).unwrap());
    }

    #[test]
    fn numa_cpu_selection_interleaves_nodes_and_falls_back() {
        let allowed = [0, 1, 2, 3];
        let nodes = [
            NumaNode {
                id: 0,
                cpus: vec![0, 1],
            },
            NumaNode {
                id: 1,
                cpus: vec![2, 3],
            },
        ];
        assert_eq!(
            select_worker_cpus(&allowed, &nodes, 4).unwrap(),
            [0, 2, 1, 3]
        );
        assert_eq!(
            select_worker_placements(&allowed, &nodes, 4).unwrap(),
            [
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
            ]
        );
        assert_eq!(select_worker_cpus(&allowed, &[], 3).unwrap(), [0, 1, 2]);
        assert!(select_worker_placements(&allowed, &[], 3)
            .unwrap()
            .iter()
            .all(|placement| placement.numa_node.is_none()));
        assert!(select_worker_cpus(&allowed, &nodes, 5).is_err());
    }
}
