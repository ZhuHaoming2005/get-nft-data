use super::*;

pub(crate) fn summary_row(spec: SummarySpec<'_>, groups: GroupSummary) -> SummaryRow {
    SummaryRow {
        field_name: spec.field_name.to_string(),
        scope: spec.scope.to_string(),
        primary_chain: spec.primary_chain.to_string(),
        secondary_chain: spec.secondary_chain.to_string(),
        threshold: spec.threshold,
        match_mode: spec.match_mode.to_string(),
        metric: spec.metric.to_string(),
        total_contracts: spec.total_contracts,
        total_nfts: spec.total_nfts,
        group_count: groups.group_count,
        duplicate_contract_count: groups.duplicate_contract_count,
        duplicate_nft_count: groups.duplicate_nft_count,
        duplicate_contract_ratio: pct(groups.duplicate_contract_count, spec.total_contracts),
        duplicate_nft_ratio: pct(groups.duplicate_nft_count, spec.total_nfts),
        group_size_ge_2_count: groups.group_size_ge_2_count,
        group_size_gt_2_count: groups.group_size_gt_2_count,
    }
}

pub(crate) fn write_outputs(report: &AnalysisReport, output_dir: &Path) -> Result<(), AnalysisError> {
    let json_path = output_dir.join("summary.json");
    let json_file = fs::File::create(&json_path)?;
    serde_json::to_writer_pretty(json_file, report)?;

    let csv_path = output_dir.join("summary.csv");
    let mut file = fs::File::create(csv_path)?;
    writeln!(
        file,
        "field_name,scope,primary_chain,secondary_chain,threshold,match_mode,metric,total_contracts,total_nfts,group_count,duplicate_contract_count,duplicate_nft_count,duplicate_contract_ratio,duplicate_nft_ratio,group_size_ge_2_count,group_size_gt_2_count"
    )?;
    for row in &report.summary_rows {
        writeln!(
            file,
            "{},{},{},{},{},{},{},{},{},{},{},{},{:.6},{:.6},{},{}",
            csv_cell(&row.field_name),
            csv_cell(&row.scope),
            csv_cell(&row.primary_chain),
            csv_cell(&row.secondary_chain),
            row.threshold
                .map(|value| format!("{value:.6}"))
                .unwrap_or_default(),
            csv_cell(&row.match_mode),
            csv_cell(&row.metric),
            row.total_contracts,
            row.total_nfts,
            row.group_count,
            row.duplicate_contract_count,
            row.duplicate_nft_count,
            row.duplicate_contract_ratio,
            row.duplicate_nft_ratio,
            row.group_size_ge_2_count,
            row.group_size_gt_2_count,
        )?;
    }
    Ok(())
}

pub(crate) fn pct(part: i64, total: i64) -> f64 {
    if total == 0 {
        0.0
    } else {
        part as f64 * 100.0 / total as f64
    }
}

pub(crate) fn parquet_input_sql(paths: &[PathBuf]) -> String {
    if paths.len() == 1 {
        format!(
            "'{}'",
            sql_string(&paths[0].display().to_string().replace('\\', "/"))
        )
    } else {
        let values = paths
            .iter()
            .map(|path| {
                format!(
                    "'{}'",
                    sql_string(&path.display().to_string().replace('\\', "/"))
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!("[{values}]")
    }
}

pub(crate) fn sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

pub(crate) fn csv_cell(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

