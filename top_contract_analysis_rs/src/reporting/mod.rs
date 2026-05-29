use std::cmp::Ordering;
use std::path::{Path, PathBuf};

use once_cell::sync::Lazy;
use regex::Regex;
use unicode_normalization::UnicodeNormalization;

use crate::error::AppError;
use crate::models::{
    BatchSummaryPayload, PaperContractBehaviorStatsPayload, PaperHonestBuyerRowPayload,
    PaperStatsPayload, SingleReportPayload,
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
        "# NFT 论文统计报告".to_string(),
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
    let mut lines = vec!["# NFT 论文统计批量报告".to_string(), String::new()];

    append_paper_stats_sections(&mut lines, &payload.paper_stats);

    lines.join("\n") + "\n"
}

fn append_paper_stats_sections(lines: &mut Vec<String>, stats: &PaperStatsPayload) {
    append_duplicate_scale(lines, stats);
    append_address_classification(lines, stats);
    append_attacker_cost(lines, stats);
    append_honest_loss(lines, stats);
    append_behavior_summary(lines, stats);
    append_data_quality(lines, stats);
    append_contract_behavior_details(lines, stats);
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
        format!("- 恶意地址数: {}", address.malicious_address_count),
        format!(
            "- 跨合约重复侵权恶意地址数: {}",
            address.repeat_infringing_malicious_address_count
        ),
        format!("- 诚实买家地址数: {}", address.honest_address_count),
        format!("- 地址总数: {}", address.total_address_count),
    ]);
}

fn append_attacker_cost(lines: &mut Vec<String>, stats: &PaperStatsPayload) {
    let cost = &stats.attacker_cost;
    lines.extend([
        String::new(),
        "## 攻击者成本".to_string(),
        "| 阶段 | Gas ETH | Gas USD |".to_string(),
        "| --- | ---: | ---: |".to_string(),
        format!(
            "| setup | {} | {} |",
            format_number(cost.setup_gas_eth),
            format_number(cost.setup_gas_usd)
        ),
        format!(
            "| lure | {} | {} |",
            format_number(cost.lure_gas_eth),
            format_number(cost.lure_gas_usd)
        ),
        format!(
            "| exit | {} | {} |",
            format_number(cost.exit_gas_eth),
            format_number(cost.exit_gas_usd)
        ),
        format!(
            "| total | {} | {} |",
            format_number(cost.total_gas_eth),
            format_number(cost.total_gas_usd)
        ),
        format!(
            "- Top 合约成本贡献占比: {} ({}/{})",
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
        "| 类别 | 套牢 NFT | NFT 套牢占比 | 二级市场损失 USD | 付费 mint 损失 USD | 总损失 USD |"
            .to_string(),
        "| --- | ---: | ---: | ---: | ---: | ---: |".to_string(),
    ]);
    for row in &stats.honest_loss {
        lines.push(format!(
            "| {} | {} | {} ({}/{}) | {} | {} | {} |",
            row.category,
            row.stuck_nft_count,
            format_ratio(row.stuck_nft_ratio),
            row.stuck_nft_ratio_numerator,
            row.stuck_nft_ratio_denominator,
            format_number(row.secondary_sale_loss_usd),
            format_number(row.paid_mint_loss_usd),
            format_number(row.total_loss_usd)
        ));
    }
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
        "| 行为 | 合约数 | 覆盖率 | 实例数 | 地址数 | NFT 数 | 关联买家 | 关联损失 USD |"
            .to_string(),
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |".to_string(),
    ]);
    for row in &stats.malicious_behavior_summary {
        lines.push(format!(
            "| {} | {} | {} ({}/{}) | {} | {} | {} | {} | {} |",
            row.behavior_type,
            row.contract_count,
            format_ratio(row.contract_coverage_ratio),
            row.contract_coverage_numerator,
            row.contract_coverage_denominator,
            row.instance_count,
            row.address_count,
            row.nft_count,
            row.linked_buyer_count,
            format_number(row.linked_loss_usd)
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
        "| contract_address | Wash Trading | Pump-and-Exit | Star | Layered | Inventory | Honest buyers | Impact USD |".to_string(),
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |".to_string(),
    ]);
    for row in &rows {
        lines.push(format!(
            "| {} | {} | {} | {} | {} | {} | {} | {} |",
            row.contract_address,
            row.wash_trading.len(),
            row.pump_and_exit.len(),
            row.star_behaviors.len(),
            row.layered_transfers.len(),
            row.inventory_concentration.len(),
            row.honest_buyers_top.len(),
            format_number(contract_behavior_impact_usd(row))
        ));
    }

    let buyers = sorted_honest_buyers(&rows);
    if buyers.is_empty() {
        return;
    }

    lines.extend([
        String::new(),
        "### 诚实买家".to_string(),
        "| contract_address | buyer | source_pattern | fake NFT | paid USD | still holding |"
            .to_string(),
        "| --- | --- | --- | ---: | ---: | --- |".to_string(),
    ]);
    for (contract_address, buyer) in buyers {
        lines.push(format!(
            "| {} | {} | {} | {} | {} | {} |",
            contract_address,
            buyer.honest_buyer,
            buyer.source_pattern,
            buyer.fake_nft_bought,
            format_number(buyer.total_paid_usd),
            buyer.still_holding
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

fn sorted_honest_buyers<'a>(
    rows: &[&'a PaperContractBehaviorStatsPayload],
) -> Vec<(&'a str, &'a PaperHonestBuyerRowPayload)> {
    let mut buyers = rows
        .iter()
        .flat_map(|row| {
            row.honest_buyers_top
                .iter()
                .map(|buyer| (row.contract_address.as_str(), buyer))
        })
        .collect::<Vec<_>>();
    buyers.sort_by(|(left_contract, left), (right_contract, right)| {
        compare_desc(left.total_paid_usd, right.total_paid_usd)
            .then_with(|| right.fake_nft_bought.cmp(&left.fake_nft_bought))
            .then_with(|| left_contract.cmp(right_contract))
            .then_with(|| left.honest_buyer.cmp(&right.honest_buyer))
    });
    buyers
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
        .honest_buyers_top
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
        + row.honest_buyers_top.len()
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
            "- 可解析销售价格: {} / {} ({})",
            quality.sale_price_parseable_count,
            quality.sale_price_total_count,
            format_ratio(quality.sale_price_parseable_ratio)
        ),
        format!(
            "- 官方参与型重复合约数: {}",
            quality.legit_duplicate_contract_count
        ),
    ]);
}
