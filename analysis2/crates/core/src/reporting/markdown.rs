//! Markdown report writers for offline dedup outputs.

use std::fs;
use std::path::Path;

use serde_json::Value;

use crate::error::Analysis2Error;

use super::aggregate::DuplicateScaleRow;
use super::json::SeedDedupReport;

fn write_text(path: &Path, body: &str) -> Result<(), Analysis2Error> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, body)?;
    Ok(())
}

fn scale_table(rows: &[DuplicateScaleRow]) -> String {
    let mut out = String::from(
        "| category | dup_nfts | nft_ratio | dup_contracts | contract_ratio |\n|---|---:|---:|---:|---:|\n",
    );
    for row in rows {
        let nft_ratio = row
            .duplicate_nft_ratio
            .map(|v| format!("{v:.6}"))
            .unwrap_or_else(|| "null".into());
        let contract_ratio = row
            .duplicate_contract_ratio
            .map(|v| format!("{v:.6}"))
            .unwrap_or_else(|| "null".into());
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} |\n",
            row.category,
            row.duplicate_nft_count,
            nft_ratio,
            row.duplicate_contract_count,
            contract_ratio
        ));
    }
    out
}

pub fn write_seed_report_md(path: &Path, report: &SeedDedupReport) -> Result<(), Analysis2Error> {
    write_text(path, &seed_dedup_md_body(report))
}

fn seed_dedup_md_body(report: &SeedDedupReport) -> String {
    let mut body = format!(
        "# Seed {} / {}\n\n- hit edges: {}\n- candidate contracts: {}\n\n",
        report.seed.chain,
        report.seed.address,
        report.hit_edge_count,
        report.candidate_contract_count
    );
    body.push_str("## Intra-chain\n\n");
    body.push_str(&scale_table(&report.duplicate_scale.intra_chain));
    body.push_str("\n## Cross-chain summary\n\n");
    body.push_str(&scale_table(&report.duplicate_scale.cross_chain_summary));
    for block in &report.duplicate_scale.chain_matrix {
        body.push_str(&format!(
            "\n## Chain matrix → {}\n\n",
            block.secondary_chain
        ));
        body.push_str(&scale_table(&block.rows));
    }
    if !report.relations.is_empty() {
        body.push_str(
            "\n## Candidates\n\n| chain | address | dimensions | nfts |\n|---|---|---|---:|\n",
        );
        for rel in &report.relations {
            body.push_str(&format!(
                "| {} | {} | {} | {} |\n",
                rel.candidate_chain,
                rel.candidate_address,
                rel.dimensions.join(","),
                rel.nft_count
            ));
        }
    }
    body
}

pub fn write_seed_full_report_md(
    path: &Path,
    report: &super::run::SeedFullReport,
) -> Result<(), Analysis2Error> {
    let mut body = seed_dedup_md_body(&report.dedup);
    body.push_str(&format!(
        "\n## Analysis\n\n- scopes_complete: {}\n- analysis_complete: {}\n",
        report.scopes_complete, report.analysis_complete
    ));
    if let Some(a) = &report.analysis {
        body.push_str(&format!(
            "- suspected_duplicate_contract_count: {}\n- legit_duplicate_contract_count: {}\n- infringing_nft_count: {}\n- honest_loss_usd: {}\n- operator_output_usd: {}\n",
            a.suspected_duplicate_contract_count,
            a.legit_duplicate_contract_count,
            a.infringing_nft_count,
            a.economics_usd.honest_loss_usd,
            a.economics_usd.operator_output_usd,
        ));
    }
    write_text(path, &body)
}

pub fn write_scope_md(
    path: &Path,
    scope: &str,
    reports: &[&SeedDedupReport],
    rows_of: impl Fn(&SeedDedupReport) -> &Vec<DuplicateScaleRow>,
) -> Result<(), Analysis2Error> {
    let mut body = format!("# {scope}\n\n");
    for report in reports {
        body.push_str(&format!(
            "## {} / {}\n\n",
            report.seed.chain, report.seed.address
        ));
        body.push_str(&scale_table(rows_of(report)));
        body.push('\n');
    }
    write_text(path, &body)
}

pub fn write_matrix_md(path: &Path, reports: &[&SeedDedupReport]) -> Result<(), Analysis2Error> {
    let mut body = String::from("# chain_matrix\n\n");
    for report in reports {
        for block in &report.duplicate_scale.chain_matrix {
            body.push_str(&format!(
                "## {} / {} → {}\n\n",
                report.seed.chain, report.seed.address, block.secondary_chain
            ));
            body.push_str(&scale_table(&block.rows));
            body.push('\n');
        }
    }
    write_text(path, &body)
}

pub fn write_summary_md(path: &Path, summary: &Value) -> Result<(), Analysis2Error> {
    let body = format!(
        "# Summary\n\n- selected: {}\n- analyzed (formal): {}\n- incomplete: {}\n- failed: {}\n- seed_with_duplicate_count: {}\n- seed_duplicate_ratio: {}\n- candidate_contract_count: {}\n- honest_loss_usd: {}\n- operator_output_usd: {}\n",
        summary["selected_seed_count"],
        summary["analyzed_seed_count"],
        summary.get("incomplete_seed_count").unwrap_or(&Value::Null),
        summary["failed_seed_count"],
        summary["seed_with_duplicate_count"],
        summary["seed_duplicate_ratio"],
        summary.get("candidate_contract_count").unwrap_or(&Value::Null),
        summary.pointer("/economics/honest_loss_usd").unwrap_or(&Value::Null),
        summary.pointer("/economics/operator_output_usd").unwrap_or(&Value::Null),
    );
    write_text(path, &body)
}
