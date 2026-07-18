#![allow(unsafe_code)]

use crate::{HardwareTopology, PlatformError};

pub const MIN_OPEN_FILE_LIMIT: u64 = 1024;

#[must_use]
pub const fn is_linux_platform() -> bool {
    cfg!(target_os = "linux")
}

/// Injectable operating-system controls used by the CLI hardware quality gate.
pub trait PlatformController {
    fn set_current_thread_affinity(&self, logical_cpus: &[u32]) -> Result<(), PlatformError>;
    fn set_preferred_numa_node(&self, node: u32) -> Result<(), PlatformError>;
    fn file_descriptor_limit(&self) -> Result<u64, PlatformError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NativePlatformController;

pub fn enforce_hardware_quality_gate(
    controller: &impl PlatformController,
    topology: &HardwareTopology,
) -> Result<(), PlatformError> {
    controller.set_current_thread_affinity(&topology.allowed_logical_cpus)?;
    let first_node = topology
        .numa_nodes
        .first()
        .ok_or(PlatformError::Missing("NUMA node"))?;
    controller.set_preferred_numa_node(first_node.id)?;
    let descriptor_limit = controller.file_descriptor_limit()?;
    if descriptor_limit < MIN_OPEN_FILE_LIMIT {
        return Err(PlatformError::InvalidData {
            field: "RLIMIT_NOFILE",
            value: format!("{descriptor_limit} < {MIN_OPEN_FILE_LIMIT}"),
        });
    }
    Ok(())
}

#[cfg(target_os = "linux")]
impl PlatformController for NativePlatformController {
    fn set_current_thread_affinity(&self, logical_cpus: &[u32]) -> Result<(), PlatformError> {
        if logical_cpus.is_empty() {
            return Err(PlatformError::InvalidData {
                field: "affinity",
                value: "empty CPU set".to_owned(),
            });
        }
        // SAFETY: cpu_set_t is initialized before use; CPU indexes are checked against
        // CPU_SETSIZE; pthread_self refers to the calling thread.
        unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            libc::CPU_ZERO(&mut set);
            for cpu in logical_cpus {
                let cpu = usize::try_from(*cpu).map_err(|_| PlatformError::InvalidData {
                    field: "affinity",
                    value: cpu.to_string(),
                })?;
                if cpu >= libc::CPU_SETSIZE as usize {
                    return Err(PlatformError::InvalidData {
                        field: "affinity",
                        value: cpu.to_string(),
                    });
                }
                libc::CPU_SET(cpu, &mut set);
            }
            let result = libc::pthread_setaffinity_np(
                libc::pthread_self(),
                std::mem::size_of::<libc::cpu_set_t>(),
                &set,
            );
            if result != 0 {
                return Err(PlatformError::Io(format!(
                    "pthread_setaffinity_np failed with errno {result}"
                )));
            }
        }
        Ok(())
    }

    fn set_preferred_numa_node(&self, node: u32) -> Result<(), PlatformError> {
        let bits_per_word = usize::BITS as usize;
        let node_index = usize::try_from(node).map_err(|_| PlatformError::InvalidData {
            field: "numa_node",
            value: node.to_string(),
        })?;
        let word_index = node_index / bits_per_word;
        let mut mask = vec![0_usize; word_index + 1];
        mask[word_index] |= 1_usize << (node_index % bits_per_word);
        let max_node = node_index
            .checked_add(1)
            .ok_or(PlatformError::InvalidData {
                field: "numa_node",
                value: node.to_string(),
            })?;
        // SAFETY: the nodemask points to `mask`, which remains live for the syscall,
        // and max_node names only initialized bits in that allocation.
        let result = unsafe {
            libc::syscall(
                libc::SYS_set_mempolicy,
                1_i32, // MPOL_PREFERRED
                mask.as_ptr(),
                max_node,
            )
        };
        if result != 0 {
            return Err(PlatformError::Io(
                "set_mempolicy(MPOL_PREFERRED) failed".to_owned(),
            ));
        }
        Ok(())
    }

    fn file_descriptor_limit(&self) -> Result<u64, PlatformError> {
        // SAFETY: getrlimit initializes the provided rlimit on success.
        let mut limit: libc::rlimit = unsafe { std::mem::zeroed() };
        // SAFETY: `limit` is a valid writable rlimit pointer.
        if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
            return Err(PlatformError::Io(
                "getrlimit(RLIMIT_NOFILE) failed".to_owned(),
            ));
        }
        Ok(limit.rlim_cur)
    }
}

#[cfg(not(target_os = "linux"))]
impl PlatformController for NativePlatformController {
    fn set_current_thread_affinity(&self, _logical_cpus: &[u32]) -> Result<(), PlatformError> {
        Err(PlatformError::Missing("Linux CPU affinity"))
    }

    fn set_preferred_numa_node(&self, _node: u32) -> Result<(), PlatformError> {
        Err(PlatformError::Missing("Linux NUMA allocation policy"))
    }

    fn file_descriptor_limit(&self) -> Result<u64, PlatformError> {
        Err(PlatformError::Missing("Linux RLIMIT_NOFILE"))
    }
}

#[derive(Debug)]
pub struct MockPlatformController {
    pub affinity_result: Result<(), PlatformError>,
    pub numa_result: Result<(), PlatformError>,
    pub descriptor_limit: Result<u64, PlatformError>,
}

impl Default for MockPlatformController {
    fn default() -> Self {
        Self {
            affinity_result: Ok(()),
            numa_result: Ok(()),
            descriptor_limit: Ok(4096),
        }
    }
}

impl PlatformController for MockPlatformController {
    fn set_current_thread_affinity(&self, _logical_cpus: &[u32]) -> Result<(), PlatformError> {
        clone_platform_result(&self.affinity_result)
    }

    fn set_preferred_numa_node(&self, _node: u32) -> Result<(), PlatformError> {
        clone_platform_result(&self.numa_result)
    }

    fn file_descriptor_limit(&self) -> Result<u64, PlatformError> {
        match &self.descriptor_limit {
            Ok(limit) => Ok(*limit),
            Err(error) => Err(clone_platform_error(error)),
        }
    }
}

fn clone_platform_result(result: &Result<(), PlatformError>) -> Result<(), PlatformError> {
    match result {
        Ok(()) => Ok(()),
        Err(error) => Err(clone_platform_error(error)),
    }
}

fn clone_platform_error(error: &PlatformError) -> PlatformError {
    match error {
        PlatformError::InvalidData { field, value } => PlatformError::InvalidData {
            field,
            value: value.clone(),
        },
        PlatformError::Missing(capability) => PlatformError::Missing(capability),
        PlatformError::Io(message) => PlatformError::Io(message.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn platform_controls_are_fully_injectable() {
        let controller = MockPlatformController {
            affinity_result: Err(PlatformError::Io("affinity denied".to_owned())),
            numa_result: Ok(()),
            descriptor_limit: Ok(1024),
        };
        assert!(controller.set_current_thread_affinity(&[0]).is_err());
        assert!(controller.set_preferred_numa_node(0).is_ok());
        assert_eq!(controller.file_descriptor_limit().unwrap(), 1024);
    }

    #[test]
    fn quality_gate_rejects_low_descriptor_limit() {
        let controller = MockPlatformController {
            descriptor_limit: Ok(MIN_OPEN_FILE_LIMIT - 1),
            ..MockPlatformController::default()
        };
        let topology = HardwareTopology {
            allowed_logical_cpus: vec![0],
            physical_cores: 1,
            numa_nodes: vec![crate::NumaNode {
                id: 0,
                logical_cpus: vec![0],
                memory_bytes: 1024,
            }],
            cpu_to_numa_node: [(0, 0)].into_iter().collect(),
            physical_memory: 1024,
            cgroup_memory_limit: 1024,
            cpu_quota_parallelism: Some(1),
        };
        assert!(matches!(
            enforce_hardware_quality_gate(&controller, &topology),
            Err(PlatformError::InvalidData {
                field: "RLIMIT_NOFILE",
                ..
            })
        ));
    }
}
