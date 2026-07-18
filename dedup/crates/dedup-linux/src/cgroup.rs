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
        std::fs::read_to_string(path).map_err(|error| PlatformError::Io(error.to_string()))
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
    Ok(CgroupResources {
        allowed_cpus: parse_cpuset(
            &reader.read_to_string("/sys/fs/cgroup/cpuset.cpus.effective")?,
        )?,
        memory_limit: parse_memory_limit(&reader.read_to_string("/sys/fs/cgroup/memory.max")?)?,
        cpu_quota: parse_cpu_max(&reader.read_to_string("/sys/fs/cgroup/cpu.max")?)?,
        io_max: reader.read_to_string("/sys/fs/cgroup/io.max")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
