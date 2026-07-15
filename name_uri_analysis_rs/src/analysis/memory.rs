use super::*;

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
) -> Result<EncodeProcessMemoryPlan, AnalysisError> {
    use metadata_engine::resource::{required_host_headroom, ENCODE_HARD_TOP, GIB};

    let envelope_bytes = host_total.saturating_sub(required_host_headroom(host_total));
    let configured_duckdb =
        parse_byte_size(&resolve_duckdb_memory_limit(duckdb_memory_limit)?)? as u64;
    let desired_rust = estimated_rust_resident
        .saturating_add(64 * GIB)
        .clamp(128 * GIB, ENCODE_HARD_TOP)
        .min(rust_user_budget as u64);
    if desired_rust == 0 || desired_rust > envelope_bytes {
        return Err(AnalysisError::InvalidData(
            "Encode has no resident memory inside the shared process envelope".into(),
        ));
    }
    let duckdb_bytes = configured_duckdb.min(envelope_bytes.saturating_sub(desired_rust));
    let rust_hard_top_bytes = (rust_user_budget as u64)
        .min(ENCODE_HARD_TOP)
        .min(envelope_bytes.saturating_sub(duckdb_bytes));
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
    if analysis_bytes > total_budget {
        return Err(AnalysisError::InvalidData(format!(
            "--analysis-memory-limit {} exceeds total --memory-limit {}",
            format_byte_size(analysis_bytes),
            format_byte_size(total_budget)
        )));
    }
    if resident_analysis_bytes > analysis_bytes {
        return Err(AnalysisError::InvalidData(format!(
            "resident name state needs about {}, exceeding --analysis-memory-limit {}",
            format_byte_size(resident_analysis_bytes),
            format_byte_size(analysis_bytes)
        )));
    }

    Ok(MemoryPlan { analysis_bytes })
}

pub(crate) fn auto_balanced_memory_plan(
    total_budget: usize,
    resident_analysis_bytes: usize,
) -> Result<MemoryPlan, AnalysisError> {
    if resident_analysis_bytes > total_budget {
        return Err(AnalysisError::InvalidData(format!(
            "loaded name atoms need about {}, exceeding available Rust budget under --memory-limit {}",
            format_byte_size(resident_analysis_bytes),
            format_byte_size(total_budget)
        )));
    }
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
    let mut system = System::new();
    system.refresh_memory();
    system.available_memory() as usize
}

pub(crate) fn physical_memory_bytes() -> u64 {
    let mut system = System::new();
    system.refresh_memory();
    system.total_memory()
}

pub(crate) fn engine_memory_hard_top_bytes(
    user_budget: usize,
    engine_cap: u64,
    host_total: u64,
) -> Result<u64, AnalysisError> {
    let host_capacity = host_total.saturating_sub(
        metadata_engine::resource::required_host_headroom(host_total),
    );
    let hard_top = (user_budget as u64).min(engine_cap).min(host_capacity);
    if hard_top == 0 {
        return Err(AnalysisError::InvalidData(
            "no memory remains after reserving physical host headroom".into(),
        ));
    }
    Ok(hard_top)
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
