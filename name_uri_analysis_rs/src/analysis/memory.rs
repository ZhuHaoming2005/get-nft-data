use super::*;
use sysinfo::System;

pub(crate) fn disk_fallback_disabled() -> bool {
    std::env::var_os("NAME_URI_ANALYSIS_NO_DISK_FALLBACK").is_some()
}

#[derive(Debug)]
pub(crate) struct MemoryPlan {
    pub(crate) analysis_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EncodeProcessMemoryPlan {
    pub(crate) envelope_bytes: u64,
    pub(crate) duckdb_bytes: u64,
    pub(crate) rust_hard_top_bytes: u64,
}

pub(crate) fn encode_process_memory_plan(
    duckdb_memory_limit: &str,
    rust_user_budget: usize,
    estimated_rust_resident: u64,
    host_total: u64,
    host_available: u64,
) -> Result<EncodeProcessMemoryPlan, AnalysisError> {
    use metadata_engine::resource::GIB;

    let envelope_bytes = process_memory_envelope_bytes(host_total, host_available);
    let configured_duckdb =
        parse_byte_size(&resolve_duckdb_memory_limit(duckdb_memory_limit)?)? as u64;
    if disk_fallback_disabled() {
        return Ok(EncodeProcessMemoryPlan {
            envelope_bytes,
            duckdb_bytes: configured_duckdb,
            rust_hard_top_bytes: u64::MAX / 4,
        });
    }
    // Encode can spill DuckDB operators, while its Rust interner/CSR state is
    // substantially more expensive to reconstruct. Preserve a useful DuckDB
    // floor, then let large datasets borrow the rest of the current shared
    // process envelope instead of failing at the old fixed 288 GiB Rust ceiling.
    const MIN_ENCODE_DUCKDB_BYTES: u64 = 8 * GIB;
    let duckdb_floor = configured_duckdb.min(MIN_ENCODE_DUCKDB_BYTES);
    let max_rust = (rust_user_budget as u64).min(envelope_bytes.saturating_sub(duckdb_floor));
    let max_rust = max_rust.max(1);
    let desired_rust = estimated_rust_resident
        .saturating_add(64 * GIB)
        .max((128 * GIB).min(max_rust))
        .min(max_rust);
    let duckdb_bytes = configured_duckdb.min(envelope_bytes.saturating_sub(desired_rust));
    let rust_hard_top_bytes =
        (rust_user_budget as u64).min(envelope_bytes.saturating_sub(duckdb_bytes));
    Ok(EncodeProcessMemoryPlan {
        envelope_bytes,
        duckdb_bytes,
        rust_hard_top_bytes,
    })
}

pub(crate) fn name_analysis_memory_plan(
    memory_limit: &str,
    analysis_memory_limit: Option<&str>,
    resident_analysis_bytes: usize,
) -> Result<MemoryPlan, AnalysisError> {
    if disk_fallback_disabled() {
        return Ok(MemoryPlan {
            analysis_bytes: usize::MAX / 4,
        });
    }
    let total_budget = total_memory_budget_bytes(memory_limit)?;
    if let Some(value) = analysis_memory_limit
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if value.eq_ignore_ascii_case("auto") {
            return auto_balanced_memory_plan(total_budget, resident_analysis_bytes);
        }
        let analysis_bytes = parse_byte_size(value)?;
        return explicit_analysis_memory_plan(
            total_budget,
            analysis_bytes,
            resident_analysis_bytes,
        );
    }

    auto_balanced_memory_plan(total_budget, resident_analysis_bytes)
}

pub(crate) fn explicit_analysis_memory_plan(
    total_budget: usize,
    analysis_bytes: usize,
    resident_analysis_bytes: usize,
) -> Result<MemoryPlan, AnalysisError> {
    let _ = resident_analysis_bytes;
    Ok(MemoryPlan {
        analysis_bytes: analysis_bytes.min(total_budget),
    })
}

pub(crate) fn auto_balanced_memory_plan(
    total_budget: usize,
    resident_analysis_bytes: usize,
) -> Result<MemoryPlan, AnalysisError> {
    let _ = resident_analysis_bytes;
    Ok(MemoryPlan {
        analysis_bytes: total_budget,
    })
}

pub(crate) fn total_memory_budget_bytes(value: &str) -> Result<usize, AnalysisError> {
    let value = value.trim();
    if value.eq_ignore_ascii_case("auto") {
        Ok(auto_memory_budget_bytes())
    } else {
        parse_byte_size(value)
    }
}

pub(crate) fn auto_memory_budget_bytes() -> usize {
    let capacity = effective_memory_capacity_bytes();
    usize::try_from(
        capacity.saturating_sub(metadata_engine::resource::required_host_headroom(capacity)),
    )
    .unwrap_or(usize::MAX)
}

pub fn effective_available_memory_bytes() -> u64 {
    effective_memory_snapshot_bytes().1
}

pub fn effective_memory_capacity_bytes() -> u64 {
    effective_memory_snapshot_bytes().0
}

pub(crate) fn effective_memory_snapshot_bytes() -> (u64, u64) {
    let mut system = System::new();
    system.refresh_memory();
    effective_memory_values(&system)
}

pub(crate) fn process_memory_envelope_bytes(host_total: u64, _host_available: u64) -> u64 {
    host_total.saturating_sub(metadata_engine::resource::required_host_headroom(
        host_total,
    ))
}

pub(crate) fn duckdb_buffer_cap_bytes(process_envelope: u64) -> u64 {
    process_envelope.saturating_mul(3).saturating_div(4)
}

fn effective_memory_values(system: &System) -> (u64, u64) {
    let host_total = system.total_memory();
    let host_available = system.available_memory();
    let cgroup = system
        .cgroup_limits()
        .map(|limits| (limits.total_memory, limits.free_memory));
    effective_memory_limits(host_total, host_available, cgroup)
}

fn effective_memory_limits(
    host_total: u64,
    host_available: u64,
    cgroup: Option<(u64, u64)>,
) -> (u64, u64) {
    cgroup.map_or((host_total, host_available), |(total, available)| {
        (host_total.min(total), host_available.min(available))
    })
}

pub(crate) fn engine_memory_hard_top_bytes(
    user_budget: usize,
    engine_cap: u64,
    host_total: u64,
    _host_available: u64,
) -> Result<u64, AnalysisError> {
    if disk_fallback_disabled() {
        return Ok(u64::MAX / 4);
    }
    let configured_top = (user_budget as u64).min(engine_cap).max(1);
    let host_capacity = process_memory_envelope_bytes(host_total, host_total);
    Ok(if host_capacity == 0 {
        configured_top
    } else {
        configured_top.min(host_capacity)
    })
}

pub(crate) fn format_byte_size(bytes: usize) -> String {
    let mib = 1024usize * 1024;
    if bytes >= mib {
        format!("{}MB", bytes / mib)
    } else {
        format!("{bytes}B")
    }
}

pub(crate) fn parse_byte_size(value: &str) -> Result<usize, AnalysisError> {
    let trimmed = value.trim();
    let split_at = trimmed
        .find(|ch: char| !(ch.is_ascii_digit() || ch == '.'))
        .unwrap_or(trimmed.len());
    let (number, unit) = trimmed.split_at(split_at);
    let number = number.trim().parse::<f64>().map_err(|_| {
        AnalysisError::InvalidData(format!("invalid analysis memory limit: {value}"))
    })?;
    if !number.is_finite() || number <= 0.0 {
        return Err(AnalysisError::InvalidData(format!(
            "invalid analysis memory limit: {value}"
        )));
    }

    let multiplier = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1.0,
        "k" | "kb" | "kib" => 1024.0,
        "m" | "mb" | "mib" => 1024.0 * 1024.0,
        "g" | "gb" | "gib" => 1024.0 * 1024.0 * 1024.0,
        "t" | "tb" | "tib" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        _ => {
            return Err(AnalysisError::InvalidData(format!(
                "invalid analysis memory limit unit: {value}"
            )))
        }
    };
    Ok((number * multiplier) as usize)
}

#[cfg(test)]
mod effective_memory_tests {
    use super::effective_memory_limits;

    #[test]
    fn cgroup_limits_bound_host_capacity_and_availability() {
        assert_eq!(
            effective_memory_limits(512, 400, Some((256, 120))),
            (256, 120)
        );
    }

    #[test]
    fn unrestricted_or_looser_cgroup_preserves_host_values() {
        assert_eq!(effective_memory_limits(512, 400, None), (512, 400));
        assert_eq!(
            effective_memory_limits(512, 400, Some((1024, 900))),
            (512, 400)
        );
    }
}
