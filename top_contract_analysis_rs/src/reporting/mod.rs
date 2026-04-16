use once_cell::sync::Lazy;
use regex::Regex;
use unicode_normalization::UnicodeNormalization;

use crate::models::{BatchSummaryPayload, SingleReportPayload};

static SLUG_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"[^0-9a-zA-Z\u{4e00}-\u{9fff}]+").unwrap());

fn slugify(value: &str) -> String {
    let normalized = value.nfkc().collect::<String>();
    let lowered = normalized.trim().to_lowercase();
    let slug = SLUG_RE.replace_all(&lowered, "_");
    let slug = slug.trim_matches('_');
    if slug.is_empty() {
        "unknown_collection".into()
    } else {
        slug.to_owned()
    }
}

pub fn default_output_basename(payload: &SingleReportPayload) -> String {
    let seed_name = if payload.seed_contract.name.trim().is_empty() {
        let contract_address = payload.seed_contract.contract_address.trim();
        if contract_address.is_empty() {
            "unknown_collection"
        } else {
            contract_address
        }
    } else {
        payload.seed_contract.name.trim()
    };

    format!("top_contract_analysis__{}", slugify(seed_name))
}

pub fn render_human_readable_report(payload: &SingleReportPayload) -> String {
    format!(
        "# Top NFT 合约分析报告\n\n- Seed: {}\n- 高置信: {}\n- 低置信: {}\n",
        payload.seed_contract.contract_address,
        payload.report_summary.high_confidence_contract_count,
        payload.report_summary.low_confidence_contract_count,
    )
}

pub fn render_batch_human_readable_report(payload: &BatchSummaryPayload) -> String {
    let summary = &payload.batch_summary;
    let mut lines = vec![
        "# Top NFT 合约批量分析总报告".to_string(),
        String::new(),
        "## 汇总".to_string(),
        format!("- 种子合约报告数: {}", summary.seed_report_count),
        format!(
            "- 链: {}",
            if !summary.chain.trim().is_empty() {
                summary.chain.clone()
            } else if !summary.chains.is_empty() {
                summary.chains.join(", ")
            } else {
                "unknown".into()
            }
        ),
        format!(
            "- 重复候选合约总数: {}",
            summary.candidate_contract_count_total
        ),
        format!(
            "- 高置信疑似侵权合约总数: {}",
            summary.high_confidence_contract_count_total
        ),
        format!(
            "- 低置信疑似侵权合约总数: {}",
            summary.low_confidence_contract_count_total
        ),
        format!("- 疑似侵权 NFT 总数: {}", summary.infringing_nft_count_total),
        String::new(),
        "## Seed 报告索引".to_string(),
    ];

    if payload.seed_reports.is_empty() {
        lines.push("- 无".to_string());
    } else {
        for report in &payload.seed_reports {
            let seed = &report.seed_contract;
            let seed_name = if seed.name.trim().is_empty() {
                seed.contract_address.trim()
            } else {
                seed.name.trim()
            };
            lines.push(format!(
                "- {} ({}) | 高置信={} | 低置信={} | 侵权NFT={}",
                seed_name,
                seed.contract_address,
                report.report_summary.high_confidence_contract_count,
                report.report_summary.low_confidence_contract_count,
                report.report_summary.infringing_nft_count
            ));
        }
    }

    lines.join("\n") + "\n"
}
