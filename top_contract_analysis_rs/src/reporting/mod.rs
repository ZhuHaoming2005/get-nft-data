use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use once_cell::sync::Lazy;
use regex::Regex;
use unicode_normalization::UnicodeNormalization;

use crate::error::AppError;
use crate::models::{BatchSummaryPayload, SingleReportPayload};

static SLUG_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"[^0-9a-zA-Z\u{4e00}-\u{9fff}]+").unwrap());

fn slugify(value: &str) -> String {
    let normalized = value.nfkc().collect::<String>();
    let lowered = normalized.trim().to_lowercase().replace('ß', "ss");
    let slug = SLUG_RE.replace_all(&lowered, "_");
    let slug = slug.trim_matches('_');
    if slug.is_empty() {
        "unknown_collection".into()
    } else {
        slug.to_owned()
    }
}

fn format_ratio(value: Option<f64>) -> String {
    value
        .map(|ratio| format!("{:.2}%", ratio * 100.0))
        .unwrap_or_else(|| "n/a".into())
}

fn format_scalar(value: Option<f64>) -> String {
    value
        .map(|number| number.to_string())
        .unwrap_or_else(|| "n/a".into())
}

fn format_reason_counts(reasons: BTreeMap<String, i64>) -> String {
    if reasons.is_empty() {
        "无".into()
    } else {
        reasons
            .into_iter()
            .map(|(reason, count)| format!("{reason}={count}"))
            .collect::<Vec<_>>()
            .join(", ")
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

pub fn dump_results(payload: &SingleReportPayload, output_path: &Path) -> Result<(), AppError> {
    let text = serde_json::to_string_pretty(payload)?;
    std::fs::write(output_path, text)?;
    Ok(())
}

pub fn write_default_outputs(
    payload: &SingleReportPayload,
    output_path: &str,
) -> Result<(PathBuf, PathBuf), AppError> {
    if output_path.trim().is_empty() {
        let result_dir = std::env::current_dir()?.join("result");
        write_outputs_to_directory(payload, &result_dir)
    } else {
        let json_path = PathBuf::from(output_path);
        if let Some(parent) = json_path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let md_path = json_path.with_extension("md");
        dump_results(payload, &json_path)?;
        std::fs::write(&md_path, render_human_readable_report(payload))?;
        Ok((json_path, md_path))
    }
}

pub fn write_outputs_to_directory(
    payload: &SingleReportPayload,
    output_dir: &Path,
) -> Result<(PathBuf, PathBuf), AppError> {
    std::fs::create_dir_all(output_dir)?;
    let json_path = output_dir.join(format!("{}.json", default_output_basename(payload)));
    let md_path = json_path.with_extension("md");
    dump_results(payload, &json_path)?;
    std::fs::write(&md_path, render_human_readable_report(payload))?;
    Ok((json_path, md_path))
}

pub fn write_batch_summary_outputs(
    payload: &BatchSummaryPayload,
    output_dir: &Path,
) -> Result<(PathBuf, PathBuf), AppError> {
    std::fs::create_dir_all(output_dir)?;
    let json_path = output_dir.join("top_contract_analysis__summary.json");
    let md_path = output_dir.join("top_contract_analysis__summary.md");
    let text = serde_json::to_string_pretty(payload)?;
    std::fs::write(&json_path, text)?;
    std::fs::write(&md_path, render_batch_human_readable_report(payload))?;
    Ok((json_path, md_path))
}

pub fn render_human_readable_report(payload: &SingleReportPayload) -> String {
    let seed = &payload.seed_contract;
    let seed_stats = &payload.seed_collection_stats;
    let summary = &payload.report_summary;

    let mut lines = vec![
        "# Top NFT 合约重复样本分析报告".to_string(),
        String::new(),
        "## 种子合约".to_string(),
        format!("- 链: {}", seed.chain),
        format!("- 合约地址: {}", seed.contract_address),
        format!("- 名称: {}", seed.name),
        format!("- 符号: {}", seed.symbol),
        format!("- Token 类型: {}", seed.token_type),
        format!(
            "- 合约部署者: {}",
            if seed.contract_deployer.is_empty() {
                "unknown"
            } else {
                &seed.contract_deployer
            }
        ),
        format!("- 部署区块: {}", seed.deployed_block_number),
        String::new(),
        "## 摘要".to_string(),
        format!(
            "- 检测到开放许可: {}",
            if summary.open_license_detected {
                "是"
            } else {
                "否"
            }
        ),
        format!("- 重复候选合约数: {}", summary.candidate_contract_count),
        format!("- 疑似侵权 NFT 总数: {}", summary.infringing_nft_count),
        format!("- 恶意地址数: {}", summary.malicious_address_count),
        format!("- 诚实地址数: {}", summary.honest_address_count),
        format!(
            "- 多次侵权地址数: {}",
            summary.repeat_infringing_address_count
        ),
        format!(
            "- 归为官方参与型重复的合约数: {}",
            summary.legit_duplicate_contract_count
        ),
        format!(
            "- 候选侧开放许可 token 数: {}",
            summary.candidate_open_license_token_count
        ),
        format!(
            "- 候选侧开放许可合约数: {}",
            summary.candidate_open_license_contract_count
        ),
        format!(
            "- 诚实地址购买总金额(USD): {}",
            summary.honest_purchase_total_usd
        ),
        format!(
            "- 套牢资金(USD): {} / {}",
            summary.stuck_cost_usd,
            format_ratio(summary.stuck_cost_ratio)
        ),
        format!(
            "- 可计算买入占比的诚实地址数: {}",
            summary.buy_asset_ratio_known_address_count
        ),
        format!(
            "- 买入金额占钱包总额 >60% 的地址数/占比: {} / {}",
            summary.ratio_over_60_address_count,
            format_ratio(summary.ratio_over_60_address_ratio)
        ),
        format!(
            "- 买入金额占钱包总额 >80% 的地址数/占比: {} / {}",
            summary.ratio_over_80_address_count,
            format_ratio(summary.ratio_over_80_address_ratio)
        ),
        format!(
            "- 购买后无法再次售出的诚实节点数/占比: {} / {}",
            summary.stuck_honest_address_count,
            format_ratio(summary.stuck_honest_address_ratio)
        ),
        format!(
            "- 被腐化的地址数: {}",
            summary.corrupted_honest_address_count
        ),
        format!(
            "- Mint 到被诚实节点购买平均时间: {} 秒",
            format_scalar(summary.avg_seconds_to_honest_holder)
        ),
        format!(
            "- 传播时间中位数: {} 秒",
            format_scalar(summary.median_seconds_to_honest_holder)
        ),
        format!(
            "- Mint 到首次转手平均时间: {} 秒",
            format_scalar(summary.avg_mint_to_first_transfer_seconds)
        ),
        format!(
            "- Mint 到首次转手中位数: {} 秒",
            format_scalar(summary.median_mint_to_first_transfer_seconds)
        ),
        format!(
            "- 候选合约平均唯一接收钱包数: {}",
            format_scalar(summary.avg_unique_receiver_count)
        ),
        String::new(),
        "## 种子集合统计".to_string(),
        format!("- 拉取到的种子 NFT 数: {}", seed_stats.seed_nft_count),
        format!("- 唯一 token URI 数: {}", seed_stats.unique_token_uri_count),
        format!("- 唯一 image URI 数: {}", seed_stats.unique_image_uri_count),
        format!("- 唯一规范化名称数: {}", seed_stats.unique_name_count),
        format!("- 唯一规范化符号数: {}", seed_stats.unique_symbol_count),
    ];

    let duplicate_candidate_total = payload
        .duplicate_contracts
        .iter()
        .map(|item| item.candidate_count)
        .sum::<i64>();
    let legit_candidate_total = payload
        .legit_duplicates
        .iter()
        .map(|item| item.candidate_count)
        .sum::<i64>();
    let mut match_reason_counts = BTreeMap::<String, i64>::new();
    for item in &payload.duplicate_contracts {
        for reason in &item.match_reasons {
            *match_reason_counts.entry(reason.clone()).or_default() += 1;
        }
    }
    let mut official_reason_counts = BTreeMap::<String, i64>::new();
    for item in &payload.legit_duplicates {
        if !item.mint_recipients.is_empty() {
            *official_reason_counts
                .entry("mint 接收地址命中官方地址规则".into())
                .or_default() += 1;
        }
        for reason in &item.exclusion_reasons {
            *official_reason_counts.entry(reason.clone()).or_default() += 1;
        }
    }
    let total_usd_priced_sale_count = payload
        .fraud_trade_stats
        .values()
        .map(|stats| stats.usd_priced_sale_count.unwrap_or_default())
        .sum::<i64>();
    let total_usd_priced_volume = payload
        .fraud_trade_stats
        .values()
        .map(|stats| stats.usd_priced_volume.unwrap_or_default())
        .sum::<f64>();
    let total_unique_buyers = payload
        .fraud_trade_stats
        .values()
        .map(|stats| stats.unique_buyers)
        .sum::<i64>();

    lines.extend([
        String::new(),
        "## 合约分类摘要".to_string(),
        format!("- 疑似重复合约数: {}", payload.duplicate_contracts.len()),
        format!("- 疑似重复 NFT 数: {}", duplicate_candidate_total),
        format!(
            "- 命中原因分布: {}",
            format_reason_counts(match_reason_counts)
        ),
        format!("- 官方参与型重复合约数: {}", payload.legit_duplicates.len()),
        format!("- 官方参与型重复 NFT 数: {}", legit_candidate_total),
        format!(
            "- 官方参与型判定原因分布: {}",
            format_reason_counts(official_reason_counts)
        ),
        String::new(),
        "## 资金与交易摘要".to_string(),
        format!("- 有定价销售记录数: {}", total_usd_priced_sale_count),
        format!("- 有定价销售额(USD): {}", total_usd_priced_volume),
        format!("- 唯一买家计数合计: {}", total_unique_buyers),
        format!("- 套牢钱包数: {}", summary.stuck_honest_address_count),
        format!("- 套牢资金(USD): {}", summary.stuck_cost_usd),
    ]);

    lines.join("\n") + "\n"
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
            "- 检测到开放许可的 seed 数: {}",
            summary.open_license_detected_count
        ),
        format!(
            "- 重复候选合约总数: {}",
            summary.candidate_contract_count_total
        ),
        format!(
            "- 疑似侵权 NFT 总数: {}",
            summary.infringing_nft_count_total
        ),
        format!("- 恶意地址总数: {}", summary.malicious_address_count_total),
        format!("- 诚实地址总数: {}", summary.honest_address_count_total),
        format!(
            "- 多次侵权地址总数(按 seed 求和): {}",
            summary.repeat_infringing_address_count_total
        ),
        format!(
            "- 多次侵权地址总数(跨批次全局去重): {}",
            summary.repeat_infringing_address_count_global
        ),
        format!(
            "- 官方参与型重复合约总数: {}",
            summary.legit_duplicate_contract_count_total
        ),
        format!(
            "- 诚实地址购买总金额(USD)汇总: {}",
            summary.honest_purchase_total_usd_total
        ),
        format!(
            "- 套牢资金(USD)汇总: {} / {}",
            summary.stuck_cost_usd_total,
            format_ratio(summary.stuck_cost_ratio_overall)
        ),
        format!(
            "- 可计算买入占比的诚实地址总数: {}",
            summary.buy_asset_ratio_known_address_count_total
        ),
        format!(
            "- 买入金额占钱包总额 >60% 的地址数/总体占比: {} / {}",
            summary.ratio_over_60_address_count_total,
            format_ratio(summary.ratio_over_60_address_ratio_overall)
        ),
        format!(
            "- 买入金额占钱包总额 >80% 的地址数/总体占比: {} / {}",
            summary.ratio_over_80_address_count_total,
            format_ratio(summary.ratio_over_80_address_ratio_overall)
        ),
        format!(
            "- 购买后无法再次售出的诚实节点数/总体占比: {} / {}",
            summary.stuck_honest_address_count_total,
            format_ratio(summary.stuck_honest_address_ratio_overall)
        ),
        format!(
            "- 被腐化的诚实地址总数: {}",
            summary.corrupted_honest_address_count_total
        ),
        format!(
            "- Mint 到被诚实节点购买平均时间(跨 seed 均值): {} 秒",
            format_scalar(summary.avg_seconds_to_honest_holder_mean)
        ),
        format!(
            "- 传播时间中位数(跨 seed 中位数): {} 秒",
            format_scalar(summary.median_seconds_to_honest_holder_median)
        ),
        format!(
            "- Mint 到首次转手平均时间(跨 seed 均值): {} 秒",
            format_scalar(summary.avg_mint_to_first_transfer_seconds_mean)
        ),
        format!(
            "- Mint 到首次转手中位数(跨 seed 中位数): {} 秒",
            format_scalar(summary.median_mint_to_first_transfer_seconds_median)
        ),
        format!(
            "- 候选合约平均唯一接收钱包数(跨 seed 均值): {}",
            format_scalar(summary.avg_unique_receiver_count_mean)
        ),
        format!("- 生成时间(UTC): {}", summary.generated_at),
        String::new(),
        "## Seed 报告索引".to_string(),
    ];

    if payload.seed_reports.is_empty() {
        lines.push("- 无".to_string());
    } else {
        for item in &payload.seed_reports {
            let seed = &item.seed_contract;
            let report_summary = &item.report_summary;
            let output_files = item.output_files.as_ref();
            let seed_name = if seed.name.is_empty() {
                if seed.contract_address.is_empty() {
                    "unknown"
                } else {
                    &seed.contract_address
                }
            } else {
                &seed.name
            };

            lines.push(format!(
                "- {} ({}) | 重复合约={} | 侵权NFT={} | 恶意地址={} | 诚实地址={} | 多次侵权地址={} | 官方参与={} | 诚实购买额(USD)={} | 套牢资金(USD)={}/{} | >60%={}/{} | 套牢={}/{} | 被腐化={} | 诚实购买时长={}秒 | 传播中位数={}秒 | 首次转手中位数={}秒 | JSON={} | MD={}",
                seed_name,
                seed.contract_address,
                report_summary.candidate_contract_count,
                report_summary.infringing_nft_count,
                report_summary.malicious_address_count,
                report_summary.honest_address_count,
                report_summary.repeat_infringing_address_count,
                report_summary.legit_duplicate_contract_count,
                report_summary.honest_purchase_total_usd,
                report_summary.stuck_cost_usd,
                format_ratio(report_summary.stuck_cost_ratio),
                report_summary.ratio_over_60_address_count,
                format_ratio(report_summary.ratio_over_60_address_ratio),
                report_summary.stuck_honest_address_count,
                format_ratio(report_summary.stuck_honest_address_ratio),
                report_summary.corrupted_honest_address_count,
                format_scalar(report_summary.avg_seconds_to_honest_holder),
                format_scalar(report_summary.median_seconds_to_honest_holder),
                format_scalar(report_summary.median_mint_to_first_transfer_seconds),
                output_files.map(|files| files.json.as_str()).unwrap_or(""),
                output_files
                    .map(|files| files.markdown.as_str())
                    .unwrap_or("")
            ));
        }
    }

    lines.join("\n") + "\n"
}
