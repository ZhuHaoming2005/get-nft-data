use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PlatformError {
    #[error("invalid platform data for {field}: {value:?}")]
    InvalidData { field: &'static str, value: String },
    #[error("platform capability missing: {0}")]
    Missing(&'static str),
    #[error("platform I/O error: {0}")]
    Io(String),
}

pub trait PlatformReader {
    fn read_to_string(&self, path: &str) -> Result<String, PlatformError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SystemPlatformReader;

impl PlatformReader for SystemPlatformReader {
    fn read_to_string(&self, path: &str) -> Result<String, PlatformError> {
        std::fs::read_to_string(path).map_err(|error| PlatformError::Io(format!("{path}: {error}")))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CpuQuota {
    pub quota_micros: Option<u64>,
    pub period_micros: u64,
}

impl CpuQuota {
    pub fn maximum_parallelism(self) -> Option<u64> {
        self.quota_micros
            .map(|quota| quota.div_ceil(self.period_micros))
    }
}

pub fn parse_memory_limit(value: &str) -> Result<Option<u64>, PlatformError> {
    let trimmed = value.trim();
    if trimmed == "max" {
        return Ok(None);
    }
    trimmed
        .parse()
        .map(Some)
        .map_err(|_| PlatformError::InvalidData {
            field: "memory.max",
            value: trimmed.to_owned(),
        })
}

pub fn parse_cpuset(value: &str) -> Result<Vec<u32>, PlatformError> {
    let mut cpus = Vec::new();
    for part in value.trim().split(',').filter(|part| !part.is_empty()) {
        if let Some((start, end)) = part.split_once('-') {
            let start: u32 = start.parse().map_err(|_| PlatformError::InvalidData {
                field: "cpuset",
                value: value.to_owned(),
            })?;
            let end: u32 = end.parse().map_err(|_| PlatformError::InvalidData {
                field: "cpuset",
                value: value.to_owned(),
            })?;
            if end < start {
                return Err(PlatformError::InvalidData {
                    field: "cpuset",
                    value: value.to_owned(),
                });
            }
            cpus.extend(start..=end);
        } else {
            cpus.push(part.parse().map_err(|_| PlatformError::InvalidData {
                field: "cpuset",
                value: value.to_owned(),
            })?);
        }
    }
    cpus.sort_unstable();
    cpus.dedup();
    if cpus.is_empty() {
        return Err(PlatformError::InvalidData {
            field: "cpuset",
            value: value.to_owned(),
        });
    }
    Ok(cpus)
}

pub fn parse_cpu_max(value: &str) -> Result<CpuQuota, PlatformError> {
    let mut fields = value.split_whitespace();
    let quota = fields.next().ok_or_else(|| PlatformError::InvalidData {
        field: "cpu.max",
        value: value.to_owned(),
    })?;
    let period = fields
        .next()
        .ok_or_else(|| PlatformError::InvalidData {
            field: "cpu.max",
            value: value.to_owned(),
        })?
        .parse::<u64>()
        .map_err(|_| PlatformError::InvalidData {
            field: "cpu.max",
            value: value.to_owned(),
        })?;
    if period == 0 || fields.next().is_some() {
        return Err(PlatformError::InvalidData {
            field: "cpu.max",
            value: value.to_owned(),
        });
    }
    let quota_micros = if quota == "max" {
        None
    } else {
        Some(
            quota
                .parse::<u64>()
                .map_err(|_| PlatformError::InvalidData {
                    field: "cpu.max",
                    value: value.to_owned(),
                })?,
        )
    };
    Ok(CpuQuota {
        quota_micros,
        period_micros: period,
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CgroupResources {
    pub allowed_cpus: Vec<u32>,
    pub memory_limit: Option<u64>,
    pub cpu_quota: CpuQuota,
    pub io_max: String,
}

pub fn read_cgroup_v2(reader: &impl PlatformReader) -> Result<CgroupResources, PlatformError> {
    let unified_path = reader
        .read_to_string("/proc/self/cgroup")
        .ok()
        .and_then(|value| parse_unified_cgroup_path(&value));
    let allowed_cpus = reader
        .read_to_string("/proc/self/status")
        .ok()
        .and_then(|value| parse_status_allowed_cpus(&value).ok())
        .or_else(|| {
            read_cgroup_file(reader, unified_path.as_deref(), "cpuset.cpus.effective")
                .and_then(|value| parse_cpuset(&value).ok())
        })
        .or_else(|| {
            read_first(
                reader,
                &[
                    "/sys/fs/cgroup/cpuset/cpuset.effective_cpus",
                    "/sys/fs/cgroup/cpuset/cpuset.cpus",
                ],
            )
            .and_then(|value| parse_cpuset(&value).ok())
        })
        .ok_or(PlatformError::Missing("effective CPU affinity"))?;
    let memory_limit = read_cgroup_file(reader, unified_path.as_deref(), "memory.max")
        .and_then(|value| parse_memory_limit(&value).ok())
        .flatten()
        .or_else(|| {
            reader
                .read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes")
                .ok()
                .and_then(|value| value.trim().parse::<u64>().ok())
        });
    let cpu_quota = read_cgroup_file(reader, unified_path.as_deref(), "cpu.max")
        .and_then(|value| parse_cpu_max(&value).ok())
        .or_else(|| read_cgroup_v1_cpu_quota(reader))
        .unwrap_or(CpuQuota {
            quota_micros: None,
            period_micros: 100_000,
        });
    let io_max = read_cgroup_file(reader, unified_path.as_deref(), "io.max").unwrap_or_default();
    Ok(CgroupResources {
        allowed_cpus,
        memory_limit,
        cpu_quota,
        io_max,
    })
}

fn read_cgroup_file(
    reader: &impl PlatformReader,
    unified_path: Option<&str>,
    file: &str,
) -> Option<String> {
    let nested = unified_path
        .filter(|path| *path != "/")
        .map(|path| format!("/sys/fs/cgroup/{}/{file}", path.trim_matches('/')));
    nested
        .as_deref()
        .and_then(|path| reader.read_to_string(path).ok())
        .or_else(|| {
            reader
                .read_to_string(&format!("/sys/fs/cgroup/{file}"))
                .ok()
        })
}

fn read_first(reader: &impl PlatformReader, paths: &[&str]) -> Option<String> {
    paths
        .iter()
        .find_map(|path| reader.read_to_string(path).ok())
}

fn read_cgroup_v1_cpu_quota(reader: &impl PlatformReader) -> Option<CpuQuota> {
    let quota = read_first(
        reader,
        &[
            "/sys/fs/cgroup/cpu/cpu.cfs_quota_us",
            "/sys/fs/cgroup/cpu,cpuacct/cpu.cfs_quota_us",
        ],
    )?;
    let period = read_first(
        reader,
        &[
            "/sys/fs/cgroup/cpu/cpu.cfs_period_us",
            "/sys/fs/cgroup/cpu,cpuacct/cpu.cfs_period_us",
        ],
    )?
    .trim()
    .parse::<u64>()
    .ok()?;
    if period == 0 {
        return None;
    }
    let quota = quota.trim().parse::<i64>().ok()?;
    Some(CpuQuota {
        quota_micros: u64::try_from(quota).ok(),
        period_micros: period,
    })
}

fn parse_unified_cgroup_path(input: &str) -> Option<String> {
    input.lines().find_map(|line| {
        let mut fields = line.splitn(3, ':');
        (fields.next()? == "0" && fields.next()?.is_empty())
            .then(|| fields.next().unwrap_or("/").to_owned())
    })
}

pub fn parse_status_allowed_cpus(input: &str) -> Result<Vec<u32>, PlatformError> {
    let value = input
        .lines()
        .find_map(|line| line.strip_prefix("Cpus_allowed_list:"))
        .ok_or_else(|| PlatformError::InvalidData {
            field: "Cpus_allowed_list",
            value: input.to_owned(),
        })?;
    parse_cpuset(value)
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
    fn pure_cgroup_parsers_cover_limits_and_ranges() {
        assert_eq!(parse_memory_limit("max\n").unwrap(), None);
        assert_eq!(parse_memory_limit("1024\n").unwrap(), Some(1024));
        assert_eq!(parse_cpuset("0-2,4,6-7").unwrap(), vec![0, 1, 2, 4, 6, 7]);
        assert_eq!(
            parse_cpu_max("150000 100000")
                .unwrap()
                .maximum_parallelism(),
            Some(2)
        );
        assert_eq!(parse_cpu_max("max 100000").unwrap().quota_micros, None);
    }

    #[test]
    fn nested_cgroup_without_io_controller_uses_process_affinity() {
        let reader = FixtureReader(BTreeMap::from([
            (
                "/proc/self/cgroup".to_owned(),
                "0::/tenant/job\n".to_owned(),
            ),
            (
                "/proc/self/status".to_owned(),
                "Name:\tdedup\nCpus_allowed_list:\t2-3\n".to_owned(),
            ),
            (
                "/sys/fs/cgroup/tenant/job/memory.max".to_owned(),
                "2048\n".to_owned(),
            ),
            (
                "/sys/fs/cgroup/tenant/job/cpu.max".to_owned(),
                "150000 100000\n".to_owned(),
            ),
        ]));
        let resources = read_cgroup_v2(&reader).unwrap();
        assert_eq!(resources.allowed_cpus, [2, 3]);
        assert_eq!(resources.memory_limit, Some(2048));
        assert_eq!(resources.cpu_quota.maximum_parallelism(), Some(2));
        assert!(resources.io_max.is_empty());
    }
}
