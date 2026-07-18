use crate::{PlatformError, PlatformReader, parse_cpuset, parse_memory_limit, read_cgroup_v2};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NumaNode {
    pub id: u32,
    pub logical_cpus: Vec<u32>,
    pub memory_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardwareTopology {
    pub allowed_logical_cpus: Vec<u32>,
    pub physical_cores: u32,
    pub numa_nodes: Vec<NumaNode>,
    pub cpu_to_numa_node: BTreeMap<u32, u32>,
    pub physical_memory: u64,
    pub cgroup_memory_limit: u64,
    pub cpu_quota_parallelism: Option<u64>,
}

impl HardwareTopology {
    pub fn available_memory(&self) -> u64 {
        self.physical_memory.min(self.cgroup_memory_limit)
    }
}

pub fn read_basic_topology(
    reader: &impl PlatformReader,
    physical_memory: u64,
    physical_cores: u32,
) -> Result<HardwareTopology, PlatformError> {
    let cpus = parse_cpuset(&reader.read_to_string("/sys/fs/cgroup/cpuset.cpus.effective")?)?;
    let memory_limit = parse_memory_limit(&reader.read_to_string("/sys/fs/cgroup/memory.max")?)?
        .unwrap_or(physical_memory);
    Ok(HardwareTopology {
        allowed_logical_cpus: cpus.clone(),
        physical_cores,
        numa_nodes: vec![NumaNode {
            id: 0,
            logical_cpus: cpus.clone(),
            memory_bytes: physical_memory.min(memory_limit),
        }],
        cpu_to_numa_node: cpus.into_iter().map(|cpu| (cpu, 0)).collect(),
        physical_memory,
        cgroup_memory_limit: memory_limit,
        cpu_quota_parallelism: None,
    })
}

pub fn read_hardware_topology(
    reader: &impl PlatformReader,
) -> Result<HardwareTopology, PlatformError> {
    let cgroup = read_cgroup_v2(reader)?;
    let physical_memory = parse_memtotal(&reader.read_to_string("/proc/meminfo")?)?;
    let node_ids = parse_cpuset(&reader.read_to_string("/sys/devices/system/node/online")?)?;
    let allowed: std::collections::BTreeSet<_> = cgroup.allowed_cpus.iter().copied().collect();
    let mut numa_nodes = Vec::new();
    let mut cpu_to_numa_node = BTreeMap::new();
    for node_id in node_ids {
        let logical_cpus: Vec<u32> = parse_cpuset(
            &reader.read_to_string(&format!("/sys/devices/system/node/node{node_id}/cpulist"))?,
        )?
        .into_iter()
        .filter(|cpu| allowed.contains(cpu))
        .collect();
        if logical_cpus.is_empty() {
            continue;
        }
        let memory_bytes = parse_numa_memtotal(
            &reader.read_to_string(&format!("/sys/devices/system/node/node{node_id}/meminfo"))?,
        )?;
        for cpu in &logical_cpus {
            cpu_to_numa_node.insert(*cpu, node_id);
        }
        numa_nodes.push(NumaNode {
            id: node_id,
            logical_cpus,
            memory_bytes,
        });
    }
    if numa_nodes.is_empty() {
        return Err(PlatformError::Missing("NUMA topology"));
    }
    let mut physical_core_keys = std::collections::BTreeSet::new();
    for cpu in &cgroup.allowed_cpus {
        let core = parse_single_u32(
            "core_id",
            &reader.read_to_string(&format!(
                "/sys/devices/system/cpu/cpu{cpu}/topology/core_id"
            ))?,
        )?;
        let package = parse_single_u32(
            "physical_package_id",
            &reader.read_to_string(&format!(
                "/sys/devices/system/cpu/cpu{cpu}/topology/physical_package_id"
            ))?,
        )?;
        physical_core_keys.insert((package, core));
    }
    Ok(HardwareTopology {
        allowed_logical_cpus: cgroup.allowed_cpus,
        physical_cores: u32::try_from(physical_core_keys.len()).map_err(|_| {
            PlatformError::InvalidData {
                field: "physical_cores",
                value: physical_core_keys.len().to_string(),
            }
        })?,
        numa_nodes,
        cpu_to_numa_node,
        physical_memory,
        cgroup_memory_limit: cgroup.memory_limit.unwrap_or(physical_memory),
        cpu_quota_parallelism: cgroup.cpu_quota.maximum_parallelism(),
    })
}

pub fn parse_memtotal(input: &str) -> Result<u64, PlatformError> {
    parse_kib_field(input, "MemTotal:")
}

pub fn parse_numa_memtotal(input: &str) -> Result<u64, PlatformError> {
    let line = input
        .lines()
        .find(|line| line.split_whitespace().any(|field| field == "MemTotal:"))
        .ok_or_else(|| PlatformError::InvalidData {
            field: "numa_meminfo",
            value: input.to_owned(),
        })?;
    let fields: Vec<_> = line.split_whitespace().collect();
    let position = fields
        .iter()
        .position(|field| *field == "MemTotal:")
        .ok_or_else(|| PlatformError::InvalidData {
            field: "numa_meminfo",
            value: input.to_owned(),
        })?;
    parse_kib(fields.get(position + 1).copied(), "numa_meminfo", input)
}

fn parse_kib_field(input: &str, field_name: &'static str) -> Result<u64, PlatformError> {
    let line = input
        .lines()
        .find(|line| line.starts_with(field_name))
        .ok_or_else(|| PlatformError::InvalidData {
            field: field_name,
            value: input.to_owned(),
        })?;
    parse_kib(line.split_whitespace().nth(1), field_name, input)
}

fn parse_kib(
    value: Option<&str>,
    field_name: &'static str,
    input: &str,
) -> Result<u64, PlatformError> {
    value
        .and_then(|value| value.parse::<u64>().ok())
        .and_then(|value| value.checked_mul(1024))
        .ok_or_else(|| PlatformError::InvalidData {
            field: field_name,
            value: input.to_owned(),
        })
}

fn parse_single_u32(field: &'static str, input: &str) -> Result<u32, PlatformError> {
    input
        .trim()
        .parse()
        .map_err(|_| PlatformError::InvalidData {
            field,
            value: input.to_owned(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    struct FixtureReader(BTreeMap<String, String>);

    impl PlatformReader for FixtureReader {
        fn read_to_string(&self, path: &str) -> Result<String, PlatformError> {
            self.0
                .get(path)
                .cloned()
                .ok_or(PlatformError::Missing("fixture path"))
        }
    }

    #[test]
    fn cgroup_cpuset_filters_numa_and_core_topology() {
        let mut files = BTreeMap::from([
            (
                "/sys/fs/cgroup/cpuset.cpus.effective".to_owned(),
                "0-3".to_owned(),
            ),
            ("/sys/fs/cgroup/memory.max".to_owned(), "4096".to_owned()),
            (
                "/sys/fs/cgroup/cpu.max".to_owned(),
                "200000 100000".to_owned(),
            ),
            ("/sys/fs/cgroup/io.max".to_owned(), String::new()),
            ("/proc/meminfo".to_owned(), "MemTotal: 8 kB\n".to_owned()),
            (
                "/sys/devices/system/node/online".to_owned(),
                "0-1".to_owned(),
            ),
            (
                "/sys/devices/system/node/node0/cpulist".to_owned(),
                "0-1".to_owned(),
            ),
            (
                "/sys/devices/system/node/node1/cpulist".to_owned(),
                "2-3".to_owned(),
            ),
            (
                "/sys/devices/system/node/node0/meminfo".to_owned(),
                "Node 0 MemTotal: 4 kB".to_owned(),
            ),
            (
                "/sys/devices/system/node/node1/meminfo".to_owned(),
                "Node 1 MemTotal: 4 kB".to_owned(),
            ),
        ]);
        for cpu in 0..4 {
            files.insert(
                format!("/sys/devices/system/cpu/cpu{cpu}/topology/core_id"),
                (cpu / 2).to_string(),
            );
            files.insert(
                format!("/sys/devices/system/cpu/cpu{cpu}/topology/physical_package_id"),
                "0".to_owned(),
            );
        }
        let topology = read_hardware_topology(&FixtureReader(files)).unwrap();
        assert_eq!(topology.physical_cores, 2);
        assert_eq!(topology.numa_nodes.len(), 2);
        assert_eq!(topology.available_memory(), 4096);
        assert_eq!(topology.cpu_quota_parallelism, Some(2));
    }
}
