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
        "| 套牢 NFT | NFT 套牢占比 | 套牢时间比 | 二级市场损失 ETH/USD | 付费 mint 损失 ETH/USD | 总损失 ETH/USD | 损失集中度 |"
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
        format_ratio(row.stuck_time_ratio),
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

fn append_contract_behavior_details(lines: &mut Vec<String>, stats: &PaperStatsPayload) {
    if stats.contract_behavior_stats.is_empty() {
        return;
    }
    let rows = sorted_contract_behavior_rows(stats);

    lines.extend([
        String::new(),
        "## 合约行为明细".to_string(),
        "| contract_address | Wash Trading | Pump-and-Exit | Star | Layered | Inventory | Honest buyers |".to_string(),
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |".to_string(),
    ]);
    for row in &rows {
        lines.push(format!(
            "| {} | {} | {} | {} | {} | {} | {} |",
            row.contract_address,
            row.wash_trading.len(),
            row.pump_and_exit.len(),
            row.star_behaviors.len(),
            row.layered_transfers.len(),
            row.inventory_concentration.len(),
            row.honest_buyers.len()
        ));
    }

    append_wash_trading_details(lines, &rows);
    append_pump_exit_details(lines, &rows);
    append_star_behavior_details(lines, &rows);
    append_layered_transfer_details(lines, &rows);
    append_inventory_concentration_details(lines, &rows);

    let buyers = sorted_honest_buyers(&rows);
    if buyers.is_empty() {
        return;
    }

    lines.extend([
        String::new(),
        "### 诚实买家".to_string(),
        "| contract_address | buyer | source_pattern | fake NFT | paid ETH/USD | time_to_purchase_seconds | still holding | holding_seconds |"
            .to_string(),
        "| --- | --- | --- | ---: | ---: | ---: | --- | ---: |".to_string(),
    ]);
    for (contract_address, buyer) in buyers {
        lines.push(format!(
            "| {} | {} | {} | {} | {} / {} | {} | {} | {} |",
            contract_address,
            buyer.honest_buyer,
            buyer.source_pattern,
            buyer.fake_nft_bought,
            format_number(buyer.total_paid_eth),
            format_number(buyer.total_paid_usd),
            format_optional_i64(buyer.time_to_purchase_seconds),
            buyer.still_holding,
            format_optional_i64(buyer.holding_seconds)
        ));
    }
}

fn append_wash_trading_details(
    lines: &mut Vec<String>,
    rows: &[&PaperContractBehaviorStatsPayload],
) {
    let mut details = rows
        .iter()
        .flat_map(|row| {
            row.wash_trading
                .iter()
                .map(|item| (row.contract_address.as_str(), item))
        })
        .collect::<Vec<_>>();
    if details.is_empty() {
        return;
    }
    details.sort_by(|(left_contract, left), (right_contract, right)| {
        compare_desc(left.fake_volume_usd, right.fake_volume_usd)
            .then_with(|| compare_desc(left.fake_volume_eth, right.fake_volume_eth))
            .then_with(|| {
                right
                    .participant_node_count
                    .cmp(&left.participant_node_count)
            })
            .then_with(|| left_contract.cmp(right_contract))
            .then_with(|| left.cycle_id.cmp(&right.cycle_id))
    });
    lines.extend([
        String::new(),
        "### Wash Trading".to_string(),
        "| contract_address | cycle_id | nodes | token_gini | avg_cycle_blocks | fake_volume ETH/USD |".to_string(),
        "| --- | --- | ---: | ---: | ---: | ---: |".to_string(),
    ]);
    for (contract_address, row) in details {
        lines.push(format!(
            "| {} | {} | {} | {} | {} | {} / {} |",
            contract_address,
            row.cycle_id,
            row.participant_node_count,
            format_optional_f64(row.token_gini),
            format_optional_f64(row.avg_cycle_blocks),
            format_number(row.fake_volume_eth),
            format_number(row.fake_volume_usd)
        ));
    }
}

fn append_pump_exit_details(lines: &mut Vec<String>, rows: &[&PaperContractBehaviorStatsPayload]) {
    let mut details = rows
        .iter()
        .flat_map(|row| {
            row.pump_and_exit
                .iter()
                .map(|item| (row.contract_address.as_str(), item))
        })
        .collect::<Vec<_>>();
    if details.is_empty() {
        return;
    }
    details.sort_by(|(left_contract, left), (right_contract, right)| {
        compare_desc(left.linked_loss_usd, right.linked_loss_usd)
            .then_with(|| {
                compare_desc(
                    left.exit_price_premium.unwrap_or_default(),
                    right.exit_price_premium.unwrap_or_default(),
                )
            })
            .then_with(|| {
                right
                    .linked_honest_buyer_count
                    .cmp(&left.linked_honest_buyer_count)
            })
            .then_with(|| left_contract.cmp(right_contract))
            .then_with(|| left.cycle_id.cmp(&right.cycle_id))
    });
    lines.extend([
        String::new(),
        "### Pump-and-Exit".to_string(),
        "| contract_address | cycle_id | exit_delay_seconds | exit_price_premium | exit_ratio | linked_buyers | linked_loss ETH/USD |".to_string(),
        "| --- | --- | ---: | ---: | ---: | ---: | ---: |".to_string(),
    ]);
    for (contract_address, row) in details {
        lines.push(format!(
            "| {} | {} | {} | {} ({}/{}) | {} ({}/{}) | {} | {} / {} |",
            contract_address,
            row.cycle_id,
            format_optional_i64(row.exit_delay_seconds),
            format_optional_f64(row.exit_price_premium),
            format_number(row.exit_price_premium_numerator),
            format_number(row.exit_price_premium_denominator),
            format_ratio(row.exit_ratio),
            row.exit_ratio_numerator,
            row.exit_ratio_denominator,
            row.linked_honest_buyer_count,
            format_number(row.linked_loss_eth),
            format_number(row.linked_loss_usd)
        ));
    }
}

fn append_star_behavior_details(
    lines: &mut Vec<String>,
    rows: &[&PaperContractBehaviorStatsPayload],
) {
    let mut details = rows
        .iter()
        .flat_map(|row| {
            row.star_behaviors
                .iter()
                .map(|item| (row.contract_address.as_str(), item))
        })
        .collect::<Vec<_>>();
    if details.is_empty() {
        return;
    }
    details.sort_by(|(left_contract, left), (right_contract, right)| {
        compare_desc(left.total_value_usd, right.total_value_usd)
            .then_with(|| right.edges.cmp(&left.edges))
            .then_with(|| left.behavior.cmp(&right.behavior))
            .then_with(|| left_contract.cmp(right_contract))
    });
    lines.extend([
        String::new(),
        "### Sybil/Fraud/Poisoning".to_string(),
        "| contract_address | behavior | centers | edges | wallets | tokens | avg_fan_out | median_holding_seconds | total_value ETH/USD |".to_string(),
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |".to_string(),
    ]);
    for (contract_address, row) in details {
        lines.push(format!(
            "| {} | {} | {} | {} | {} | {} | {} ({}/{}) | {} | {} / {} |",
            contract_address,
            row.behavior,
            row.centers,
            row.edges,
            row.wallets,
            row.tokens,
            format_optional_f64(row.avg_fan_out),
            row.avg_fan_out_numerator,
            row.avg_fan_out_denominator,
            format_optional_f64(row.median_holding_seconds),
            format_number(row.total_value_eth),
            format_number(row.total_value_usd)
        ));
    }
}

fn append_layered_transfer_details(
    lines: &mut Vec<String>,
    rows: &[&PaperContractBehaviorStatsPayload],
) {
    let mut details = rows
        .iter()
        .flat_map(|row| {
            row.layered_transfers
                .iter()
                .map(|item| (row.contract_address.as_str(), item))
        })
        .collect::<Vec<_>>();
    if details.is_empty() {
        return;
    }
    details.sort_by(|(left_contract, left), (right_contract, right)| {
        compare_desc(left.total_value_usd, right.total_value_usd)
            .then_with(|| right.length.cmp(&left.length))
            .then_with(|| left_contract.cmp(right_contract))
            .then_with(|| left.path_id.cmp(&right.path_id))
    });
    lines.extend([
        String::new(),
        "### Layered Transfer".to_string(),
        "| contract_address | path_id | tokens | length | wallets | zero/low-value hops | duration_seconds | total_value ETH/USD |".to_string(),
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |".to_string(),
    ]);
    for (contract_address, row) in details {
        lines.push(format!(
            "| {} | {} | {} | {} | {} | {} | {} | {} / {} |",
            contract_address,
            row.path_id,
            row.tokens,
            row.length,
            row.wallets,
            row.zero_or_low_value_hops,
            format_optional_i64(row.total_path_duration_seconds),
            format_number(row.total_value_eth),
            format_number(row.total_value_usd)
        ));
    }
}

fn append_inventory_concentration_details(
    lines: &mut Vec<String>,
    rows: &[&PaperContractBehaviorStatsPayload],
) {
    let mut details = rows
        .iter()
        .flat_map(|row| {
            row.inventory_concentration
                .iter()
                .map(|item| (row.contract_address.as_str(), item))
        })
        .collect::<Vec<_>>();
    if details.is_empty() {
        return;
    }
    details.sort_by(|(left_contract, left), (right_contract, right)| {
        compare_desc(left.value_collected_usd, right.value_collected_usd)
            .then_with(|| right.inbound_txns.cmp(&left.inbound_txns))
            .then_with(|| left_contract.cmp(right_contract))
            .then_with(|| left.hub_address.cmp(&right.hub_address))
    });
    lines.extend([
        String::new(),
        "### Inventory Concentration".to_string(),
        "| contract_address | hub_address | source_wallets | inbound_txns | token_share | value_collected ETH/USD | value_share | collection_window_seconds |".to_string(),
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |".to_string(),
    ]);
    for (contract_address, row) in details {
        lines.push(format!(
            "| {} | {} | {} | {} | {} ({}/{}) | {} / {} | {} ({}/{}) | {} |",
            contract_address,
            row.hub_address,
            row.source_wallets,
            row.inbound_txns,
            format_ratio(row.token_share),
            row.token_share_numerator,
            row.token_share_denominator,
            format_number(row.value_collected_eth),
            format_number(row.value_collected_usd),
            format_ratio(row.value_share),
            format_number(row.value_share_numerator),
            format_number(row.value_share_denominator),
            format_optional_i64(row.collection_window_seconds)
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
            row.honest_buyers
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
        .honest_buyers
        .iter()
        .map(|item| item.total_paid_usd)
        .sum();
    wash_volume + pump_loss + star_value + layered_value + inventory_value + honest_paid
}

fn format_optional_i64(value: Option<i64>) -> String {
    value.map(|value| value.to_string()).unwrap_or_default()
}

fn format_optional_f64(value: Option<f64>) -> String {
    value.map(format_number).unwrap_or_default()
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
