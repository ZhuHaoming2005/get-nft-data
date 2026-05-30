use std::cmp::Ordering;
use std::path::{Path, PathBuf};

use once_cell::sync::Lazy;
use regex::Regex;
use unicode_normalization::UnicodeNormalization;

use crate::error::AppError;
use crate::models::{
    BatchSummaryPayload, PaperContractBehaviorStatsPayload, PaperStatsPayload,
    PaperWashCycleSizeRowPayload, SingleReportPayload,
};

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

fn format_multiple(value: Option<f64>) -> String {
    value
        .map(|multiple| format!("{}x", format_number(multiple)))
        .unwrap_or_else(|| "n/a".into())
}

fn format_number(value: f64) -> String {
    if value == 0.0 {
        return "0".into();
    }
    let formatted = format!("{value:.6}");
    formatted
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_string()
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

pub fn write_batch_paper_stats_outputs(
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

    let mut lines = vec![
        "# NFT 论文统计单合约报告".to_string(),
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
    ];

    append_paper_stats_sections(&mut lines, &payload.paper_stats);

    lines.join("\n") + "\n"
}

pub fn render_batch_human_readable_report(payload: &BatchSummaryPayload) -> String {
    let mut lines = vec!["# NFT 论文统计汇总报告".to_string(), String::new()];

    append_paper_stats_summary_sections(&mut lines, &payload.paper_stats);

    lines.join("\n") + "\n"
}

fn append_paper_stats_sections(lines: &mut Vec<String>, stats: &PaperStatsPayload) {
    append_paper_stats_summary_sections(lines, stats);
    append_contract_behavior_details(lines, stats);
}

fn append_paper_stats_summary_sections(lines: &mut Vec<String>, stats: &PaperStatsPayload) {
    append_duplicate_scale(lines, stats);
    append_address_classification(lines, stats);
    append_attacker_cost(lines, stats);
    append_honest_loss(lines, stats);
    append_behavior_summary(lines, stats);
    append_wash_cycle_size_distribution(lines, stats);
    append_data_quality(lines, stats);
}

fn append_duplicate_scale(lines: &mut Vec<String>, stats: &PaperStatsPayload) {
    lines.extend([
        String::new(),
        "## 重复规模".to_string(),
        "| 类别 | 重复 NFT 数 | NFT 占比 | 重复合约数 | 合约占比 |".to_string(),
        "| --- | ---: | ---: | ---: | ---: |".to_string(),
    ]);
    for row in &stats.duplicate_scale {
        lines.push(format!(
            "| {} | {} | {} ({}/{}) | {} | {} ({}/{}) |",
            row.category,
            row.duplicate_nft_count,
            format_ratio(row.duplicate_nft_ratio),
            row.duplicate_nft_ratio_numerator,
            row.duplicate_nft_ratio_denominator,
            row.duplicate_contract_count,
            format_ratio(row.duplicate_contract_ratio),
            row.duplicate_contract_ratio_numerator,
            row.duplicate_contract_ratio_denominator
        ));
    }
}

fn append_address_classification(lines: &mut Vec<String>, stats: &PaperStatsPayload) {
    let address = &stats.address_classification;
    lines.extend([
        String::new(),
        "## 地址分类".to_string(),
        "| 类别 | 恶意地址数量 | 多次侵权地址数 | 诚实地址数量 | 地址总数 |".to_string(),
        "| --- | ---: | ---: | ---: | ---: |".to_string(),
        format!(
            "| all | {} | {} | {} | {} |",
            address.malicious_address_count,
            address.repeat_infringing_malicious_address_count,
            address.honest_address_count,
            address.total_address_count
        ),
    ]);
}

fn append_attacker_cost(lines: &mut Vec<String>, stats: &PaperStatsPayload) {
    let cost = &stats.attacker_cost;
    lines.extend([
        String::new(),
        "## 攻击者成本".to_string(),
        "| cost | Setup Gas ETH/USD | Lure Gas ETH/USD | Exit Gas ETH/USD | Total Gas ETH/USD | 攻击投入集中度 |".to_string(),
        "| --- | ---: | ---: | ---: | ---: | ---: |".to_string(),
        format!(
            "| gas | {} / {} | {} / {} | {} / {} | {} / {} | {} ({}/{}) |",
            format_number(cost.setup_gas_eth),
            format_number(cost.setup_gas_usd),
            format_number(cost.lure_gas_eth),
            format_number(cost.lure_gas_usd),
            format_number(cost.exit_gas_eth),
            format_number(cost.exit_gas_usd),
            format_number(cost.total_gas_eth),
            format_number(cost.total_gas_usd),
            format_ratio(cost.top_contract_contribution_ratio),
            format_number(cost.top_contract_contribution_numerator),
            format_number(cost.top_contract_contribution_denominator)
        ),
    ]);
}

fn append_honest_loss(lines: &mut Vec<String>, stats: &PaperStatsPayload) {
    lines.extend([
        String::new(),
        "## 诚实买家损失".to_string(),
        "| 套牢 NFT | NFT 套牢占比 | 套牢时间倍数 | 二级市场损失 ETH/USD | 付费 mint 损失 ETH/USD | 总损失 ETH/USD | 损失集中度 |"
            .to_string(),
        "| ---: | ---: | ---: | ---: | ---: | ---: | ---: |".to_string(),
    ]);
    let row = &stats.honest_loss;
    lines.push(format!(
        "| {} | {} ({}/{}) | {} ({}/{}) | {} / {} | {} / {} | {} / {} | {} ({}/{}) |",
        row.stuck_nft_count,
        format_ratio(row.stuck_nft_ratio),
        row.stuck_nft_ratio_numerator,
        row.stuck_nft_ratio_denominator,
        format_multiple(row.stuck_time_ratio),
        format_number(row.stuck_time_ratio_numerator),
        format_number(row.stuck_time_ratio_denominator),
        format_number(row.secondary_sale_loss_eth),
        format_number(row.secondary_sale_loss_usd),
        format_number(row.paid_mint_loss_eth),
        format_number(row.paid_mint_loss_usd),
        format_number(row.total_loss_eth),
        format_number(row.total_loss_usd),
        format_ratio(row.top_contract_loss_contribution_ratio),
        format_number(row.top_contract_loss_contribution_numerator),
        format_number(row.top_contract_loss_contribution_denominator)
    ));
}

fn append_behavior_summary(lines: &mut Vec<String>, stats: &PaperStatsPayload) {
    lines.extend([
        String::new(),
        "## 恶意行为汇总".to_string(),
        format!(
            "- 有合约级行为统计的合约数: {}",
            stats.contract_behavior_stats.len()
        ),
    ]);
    if stats.malicious_behavior_summary.is_empty() {
        lines.push("- 行为实例: 0".to_string());
        return;
    }

    lines.extend([
        "| 行为 | 合约数 | 覆盖率 | 实例数 | 行为占比 | 地址数 | NFT 数 | 关联买家 | 关联损失 USD |"
            .to_string(),
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |".to_string(),
    ]);
    for row in &stats.malicious_behavior_summary {
        lines.push(format!(
            "| {} | {} | {} ({}/{}) | {} | {} ({}/{}) | {} | {} | {} | {} |",
            row.behavior_type,
            row.contract_count,
            format_ratio(row.contract_coverage_ratio),
            row.contract_coverage_numerator,
            row.contract_coverage_denominator,
            row.instance_count,
            format_ratio(row.instance_ratio),
            row.instance_ratio_numerator,
            row.instance_ratio_denominator,
            row.address_count,
            row.nft_count,
            row.linked_buyer_count,
            format_number(row.linked_loss_usd)
        ));
    }
}

fn append_wash_cycle_size_distribution(lines: &mut Vec<String>, stats: &PaperStatsPayload) {
    if stats.wash_cycle_size_distribution.is_empty() {
        return;
    }
    lines.extend([
        String::new(),
        "## Wash Cycle 节点规模".to_string(),
        "| 节点数 | 循环数 | 循环占比 |".to_string(),
        "| --- | ---: | ---: |".to_string(),
    ]);
    for row in &stats.wash_cycle_size_distribution {
        lines.push(format!(
            "| {} | {} | {} ({}/{}) |",
            row.node_count_bucket,
            row.cycle_count,
            format_ratio(row.cycle_ratio),
            row.cycle_ratio_numerator,
            row.cycle_ratio_denominator
        ));
    }
}

fn append_contract_behavior_details(lines: &mut Vec<String>, stats: &PaperStatsPayload) {
    if stats.contract_behavior_stats.is_empty() {
        return;
    }
    let rows = sorted_contract_behavior_rows(stats);

    lines.extend([
        String::new(),
        "## 合约行为明细".to_string(),
        "| contract | Wash | Pump-Exit | Star | Layered | Inventory | Honest buyers | Fake NFT | Paid USD | Behavior value USD | Linked loss USD |"
            .to_string(),
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |".to_string(),
    ]);
    for row in &rows {
        let fake_nft_count: i64 = row
            .honest_buyers
            .iter()
            .map(|buyer| buyer.fake_nft_bought)
            .sum();
        let paid_usd: f64 = row
            .honest_buyers
            .iter()
            .map(|buyer| buyer.total_paid_usd)
            .sum();
        lines.push(format!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
            short_identifier(&row.contract_address),
            row.wash_trading.len(),
            row.pump_and_exit.len(),
            row.star_behaviors.len(),
            row.layered_transfers.len(),
            row.inventory_concentration.len(),
            row.honest_buyers.len(),
            fake_nft_count,
            format_number(paid_usd),
            format_number(contract_behavior_value_usd(row)),
            format_number(contract_linked_loss_usd(row))
        ));
    }

    lines.extend([
        String::new(),
        "## Wash Cycle 节点规模（按合约）".to_string(),
        "| contract | 2 nodes | 3 nodes | 4 nodes | 5+ nodes | Total |".to_string(),
        "| --- | ---: | ---: | ---: | ---: | ---: |".to_string(),
    ]);
    for (contract_address, distribution) in contract_wash_cycle_size_rows(stats, &rows) {
        let total = cycle_size_distribution_total(&distribution);
        lines.push(format!(
            "| {} | {} | {} | {} | {} | {} |",
            short_identifier(&contract_address),
            format_cycle_size_cell(&distribution, "2"),
            format_cycle_size_cell(&distribution, "3"),
            format_cycle_size_cell(&distribution, "4"),
            format_cycle_size_cell(&distribution, "5+"),
            total
        ));
    }
}

fn sorted_contract_behavior_rows(
    stats: &PaperStatsPayload,
) -> Vec<&PaperContractBehaviorStatsPayload> {
    let mut rows = stats.contract_behavior_stats.iter().collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        compare_desc(
            contract_behavior_impact_usd(left),
            contract_behavior_impact_usd(right),
        )
        .then_with(|| {
            contract_behavior_instance_count(right).cmp(&contract_behavior_instance_count(left))
        })
        .then_with(|| left.contract_address.cmp(&right.contract_address))
    });
    rows
}

fn short_identifier(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.len() > 18 && trimmed.starts_with("0x") {
        format!("{}...{}", &trimmed[..6], &trimmed[trimmed.len() - 6..])
    } else {
        trimmed.to_string()
    }
}

fn contract_wash_cycle_size_distribution(
    row: &PaperContractBehaviorStatsPayload,
) -> Vec<PaperWashCycleSizeRowPayload> {
    if !row.wash_cycle_size_distribution.is_empty() {
        return row.wash_cycle_size_distribution.clone();
    }
    wash_cycle_size_distribution_from_counts(
        row.wash_trading
            .iter()
            .map(|cycle| cycle.participant_node_count),
    )
}

fn contract_wash_cycle_size_rows(
    stats: &PaperStatsPayload,
    behavior_rows: &[&PaperContractBehaviorStatsPayload],
) -> Vec<(String, Vec<PaperWashCycleSizeRowPayload>)> {
    let mut rows = if stats.wash_cycle_size_by_contract.is_empty() {
        behavior_rows
            .iter()
            .map(|row| {
                (
                    row.contract_address.clone(),
                    contract_wash_cycle_size_distribution(row),
                )
            })
            .collect::<Vec<_>>()
    } else {
        stats
            .wash_cycle_size_by_contract
            .iter()
            .map(|row| (row.contract_address.clone(), row.distribution.clone()))
            .collect::<Vec<_>>()
    };
    rows.sort_by(|(left_contract, left), (right_contract, right)| {
        cycle_size_distribution_total(right)
            .cmp(&cycle_size_distribution_total(left))
            .then_with(|| left_contract.cmp(right_contract))
    });
    rows
}

fn wash_cycle_size_distribution_from_counts(
    counts: impl IntoIterator<Item = i64>,
) -> Vec<PaperWashCycleSizeRowPayload> {
    let mut two_node_count = 0;
    let mut three_node_count = 0;
    let mut four_node_count = 0;
    let mut five_plus_node_count = 0;
    for count in counts {
        match count {
            2 => two_node_count += 1,
            3 => three_node_count += 1,
            4 => four_node_count += 1,
            count if count >= 5 => five_plus_node_count += 1,
            _ => {}
        }
    }
    let total = two_node_count + three_node_count + four_node_count + five_plus_node_count;
    [
        ("2", two_node_count),
        ("3", three_node_count),
        ("4", four_node_count),
        ("5+", five_plus_node_count),
    ]
    .into_iter()
    .map(|(bucket, count)| PaperWashCycleSizeRowPayload {
        node_count_bucket: bucket.to_string(),
        cycle_count: count,
        cycle_ratio: (total > 0).then_some(count as f64 / total as f64),
        cycle_ratio_numerator: count,
        cycle_ratio_denominator: total,
    })
    .collect()
}

fn format_cycle_size_cell(rows: &[PaperWashCycleSizeRowPayload], bucket: &str) -> String {
    rows.iter()
        .find(|row| row.node_count_bucket == bucket)
        .map(|row| format!("{} ({})", row.cycle_count, format_ratio(row.cycle_ratio)))
        .unwrap_or_else(|| "0 (n/a)".into())
}

fn cycle_size_distribution_total(rows: &[PaperWashCycleSizeRowPayload]) -> i64 {
    rows.iter().map(|row| row.cycle_count).sum()
}

fn contract_behavior_value_usd(row: &PaperContractBehaviorStatsPayload) -> f64 {
    let wash_volume: f64 = row
        .wash_trading
        .iter()
        .map(|item| item.fake_volume_usd)
        .sum();
    let star_value: f64 = row
        .star_behaviors
        .iter()
        .map(|item| item.total_value_usd)
        .sum();
    let layered_value: f64 = row
        .layered_transfers
        .iter()
        .map(|item| item.total_value_usd)
        .sum();
    let inventory_value: f64 = row
        .inventory_concentration
        .iter()
        .map(|item| item.value_collected_usd)
        .sum();
    wash_volume + star_value + layered_value + inventory_value
}

fn contract_linked_loss_usd(row: &PaperContractBehaviorStatsPayload) -> f64 {
    row.pump_and_exit
        .iter()
        .map(|item| item.linked_loss_usd)
        .sum()
}

fn contract_behavior_impact_usd(row: &PaperContractBehaviorStatsPayload) -> f64 {
    let wash_volume: f64 = row
        .wash_trading
        .iter()
        .map(|item| item.fake_volume_usd)
        .sum();
    let pump_loss: f64 = row
        .pump_and_exit
        .iter()
        .map(|item| item.linked_loss_usd)
        .sum();
    let star_value: f64 = row
        .star_behaviors
        .iter()
        .map(|item| item.total_value_usd)
        .sum();
    let layered_value: f64 = row
        .layered_transfers
        .iter()
        .map(|item| item.total_value_usd)
        .sum();
    let inventory_value: f64 = row
        .inventory_concentration
        .iter()
        .map(|item| item.value_collected_usd)
        .sum();
    let honest_paid: f64 = row
        .honest_buyers
        .iter()
        .map(|item| item.total_paid_usd)
        .sum();
    wash_volume + pump_loss + star_value + layered_value + inventory_value + honest_paid
}

fn contract_behavior_instance_count(row: &PaperContractBehaviorStatsPayload) -> usize {
    row.wash_trading.len()
        + row.pump_and_exit.len()
        + row.star_behaviors.len()
        + row.layered_transfers.len()
        + row.inventory_concentration.len()
        + row.honest_buyers.len()
}

fn compare_desc(left: f64, right: f64) -> Ordering {
    right.partial_cmp(&left).unwrap_or(Ordering::Equal)
}

fn append_data_quality(lines: &mut Vec<String>, stats: &PaperStatsPayload) {
    let quality = &stats.data_quality;
    lines.extend([
        String::new(),
        "## 数据质量".to_string(),
        format!(
            "- 代表候选样本数: {}",
            quality.representative_candidate_count
        ),
        format!("- 候选合约数: {}", quality.candidate_contract_count),
        format!(
            "- 疑似重复合约数: {}",
            quality.suspected_duplicate_contract_count
        ),
        format!("- 疑似侵权 NFT 数: {}", quality.infringing_nft_count),
        format!(
            "- 可解析销售价格: {} / {} ({})",
            quality.sale_price_parseable_ratio_numerator,
            quality.sale_price_parseable_ratio_denominator,
            format_ratio(quality.sale_price_parseable_ratio)
        ),
        format!(
            "- 官方参与型重复合约数: {}",
            quality.legit_duplicate_contract_count
        ),
    ]);
}
