#[derive(Debug)]
struct MemoryPlan {
    duckdb_bytes: usize,
    analysis_bytes: usize,
}

const DUCKDB_MIN_MEMORY_LIMIT_BYTES: usize = 64 * 1024 * 1024;

struct MemoryGuard {
    total_budget: usize,
    pid: Option<Pid>,
    system: System,
}

impl MemoryGuard {
    fn new(total_budget: usize) -> Self {
        Self {
            total_budget,
            pid: get_current_pid().ok(),
            system: System::new(),
        }
    }

    fn current_rss_bytes(&mut self) -> Option<usize> {
        let pid = self.pid?;
        self.system
            .refresh_processes(ProcessesToUpdate::Some(&[pid]), false);
        self.system
            .process(pid)
            .map(|process| process.memory() as usize)
    }

    fn next_threshold_batch_size(
        &mut self,
        remaining_thresholds: usize,
        budget_capacity: usize,
        per_threshold_bytes: usize,
    ) -> usize {
        let current_rss = self.current_rss_bytes().unwrap_or(0);
        adaptive_threshold_batch_size(
            remaining_thresholds,
            budget_capacity,
            per_threshold_bytes,
            self.total_budget,
            current_rss,
        )
    }
}

fn set_duckdb_memory_limit(conn: &Connection, bytes: usize) -> Result<(), AnalysisError> {
    let bytes = duckdb_effective_memory_limit_bytes(bytes);
    conn.execute(
        &format!(
            "PRAGMA memory_limit='{}'",
            sql_string(&format_byte_size(bytes))
        ),
        [],
    )?;
    Ok(())
}

fn duckdb_effective_memory_limit_bytes(bytes: usize) -> usize {
    if bytes == 0 {
        DUCKDB_MIN_MEMORY_LIMIT_BYTES
    } else {
        bytes
    }
}

fn set_duckdb_memory_limit_for_process_budget(
    conn: &Connection,
    memory_guard: &mut MemoryGuard,
    desired_duckdb_bytes: usize,
) -> Result<(), AnalysisError> {
    let current_rss = memory_guard.current_rss_bytes().unwrap_or(0);
    let bytes = duckdb_memory_limit_from_process_budget(
        memory_guard.total_budget,
        current_rss,
        desired_duckdb_bytes,
    )?;
    set_duckdb_memory_limit(conn, bytes)
}

fn duckdb_memory_limit_from_process_budget(
    total_budget: usize,
    current_rss: usize,
    desired_duckdb_bytes: usize,
) -> Result<usize, AnalysisError> {
    if total_budget <= current_rss {
        return Err(AnalysisError::InvalidData(format!(
            "process RSS {} already reached --memory-limit {}; cannot safely start another DuckDB batch",
            format_byte_size(current_rss),
            format_byte_size(total_budget)
        )));
    }
    Ok(desired_duckdb_bytes.min(total_budget - current_rss))
}

fn name_analysis_memory_plan(
    thresholds: &[f64],
    atom_count: usize,
    chain_count: usize,
    memory_limit: &str,
    analysis_memory_limit: Option<&str>,
    resident_analysis_bytes: usize,
    chain_matrix_reuse_bytes: usize,
) -> Result<MemoryPlan, AnalysisError> {
    let total_budget = total_memory_budget_bytes(memory_limit)?;
    if let Some(value) = analysis_memory_limit
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if value.eq_ignore_ascii_case("auto") {
            return auto_balanced_memory_plan(
                total_budget,
                thresholds.len(),
                atom_count,
                chain_count,
                resident_analysis_bytes,
                chain_matrix_reuse_bytes,
            );
        }
        let analysis_bytes = parse_byte_size(value)?;
        return explicit_analysis_memory_plan(
            total_budget,
            analysis_bytes,
            resident_analysis_bytes,
        );
    }

    auto_balanced_memory_plan(
        total_budget,
        thresholds.len(),
        atom_count,
        chain_count,
        resident_analysis_bytes,
        chain_matrix_reuse_bytes,
    )
}

fn explicit_analysis_memory_plan(
    total_budget: usize,
    analysis_bytes: usize,
    resident_analysis_bytes: usize,
) -> Result<MemoryPlan, AnalysisError> {
    let requested_analysis = analysis_bytes.max(resident_analysis_bytes);
    if requested_analysis > total_budget {
        return Err(AnalysisError::InvalidData(format!(
            "--analysis-memory-limit {} exceeds total --memory-limit {}",
            format_byte_size(analysis_bytes),
            format_byte_size(total_budget)
        )));
    }

    Ok(MemoryPlan {
        duckdb_bytes: total_budget.saturating_sub(requested_analysis),
        analysis_bytes: requested_analysis,
    })
}

fn auto_balanced_memory_plan(
    total_budget: usize,
    threshold_count: usize,
    atom_count: usize,
    chain_count: usize,
    resident_analysis_bytes: usize,
    chain_matrix_reuse_bytes: usize,
) -> Result<MemoryPlan, AnalysisError> {
    if resident_analysis_bytes > total_budget {
        return Err(AnalysisError::InvalidData(format!(
            "loaded name atoms need about {}, exceeding available Rust budget under --memory-limit {}",
            format_byte_size(resident_analysis_bytes),
            format_byte_size(total_budget)
        )));
    }
    let desired_analysis = desired_analysis_budget(
        threshold_count,
        atom_count,
        chain_count,
        resident_analysis_bytes,
        chain_matrix_reuse_bytes,
    );
    let duckdb_bytes = total_budget.saturating_sub(desired_analysis.min(total_budget));

    Ok(MemoryPlan {
        duckdb_bytes,
        analysis_bytes: total_budget,
    })
}

fn desired_analysis_budget(
    threshold_count: usize,
    atom_count: usize,
    chain_count: usize,
    resident_analysis_bytes: usize,
    chain_matrix_reuse_bytes: usize,
) -> usize {
    let thresholds = threshold_count.max(1);
    let per_threshold_bytes =
        threshold_state_bytes(atom_count, chain_count).saturating_add(chain_matrix_reuse_bytes);
    resident_analysis_bytes.saturating_add(per_threshold_bytes.saturating_mul(thresholds))
}

fn total_memory_budget_bytes(value: &str) -> Result<usize, AnalysisError> {
    let value = value.trim();
    if value.eq_ignore_ascii_case("auto") {
        Ok(auto_memory_budget_bytes())
    } else {
        parse_byte_size(value)
    }
}

fn auto_memory_budget_bytes() -> usize {
    let mut system = System::new();
    system.refresh_memory();
    system.available_memory() as usize
}

fn format_byte_size(bytes: usize) -> String {
    let mib = 1024usize * 1024;
    if bytes >= mib {
        format!("{}MB", bytes / mib)
    } else {
        format!("{bytes}B")
    }
}

fn parse_byte_size(value: &str) -> Result<usize, AnalysisError> {
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
