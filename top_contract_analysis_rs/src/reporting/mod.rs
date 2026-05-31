use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use once_cell::sync::Lazy;
use regex::Regex;
use unicode_normalization::UnicodeNormalization;

use crate::error::AppError;
use crate::models::{
    BatchSummaryPayload, PaperContractBehaviorStatsPayload, PaperOutputInputRatioRowPayload,
    PaperStatsPayload, PaperWashCycleSizeRowPayload, SingleReportPayload,
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

fn format_optional_number(value: Option<f64>) -> String {
    value.map(format_number).unwrap_or_else(|| "n/a".into())
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

fn format_seconds_i64(value: Option<i64>) -> String {
    value
        .map(|seconds| format!("{seconds}s"))
        .unwrap_or_else(|| "n/a".into())
}

fn format_bool(value: bool) -> &'static str {
    if value {
        "是"
    } else {
        "否"
    }
}

fn markdown_cell(value: &str) -> String {
    value.replace('|', "\\|").replace(['\r', '\n'], " ")
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
    append_output_input_ratio(lines, stats);
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

fn append_output_input_ratio(lines: &mut Vec<String>, stats: &PaperStatsPayload) {
    let summary = &stats.output_input_summary;
    lines.extend([
        String::new(),
        "## 产出投入比".to_string(),
        "| scope | 产出 USD | 投入 USD | 产出/投入 | >=1 数量占比 | <1 数量占比 |".to_string(),
        "| --- | ---: | ---: | ---: | ---: | ---: |".to_string(),
        format!(
            "| total | {} | {} | {} ({}/{}) | {} ({}/{}) | {} ({}/{}) |",
            format_number(summary.total_output_usd),
            format_number(summary.total_input_usd),
            format_multiple(summary.total_output_input_ratio),
            format_number(summary.total_output_input_ratio_numerator),
            format_number(summary.total_output_input_ratio_denominator),
            format_ratio(summary.ratio_gte_one_ratio),
            summary.ratio_gte_one_ratio_numerator,
            summary.ratio_gte_one_ratio_denominator,
            format_ratio(summary.ratio_lt_one_ratio),
            summary.ratio_lt_one_ratio_numerator,
            summary.ratio_lt_one_ratio_denominator
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
    if stats.contract_behavior_stats.is_empty()
        && stats.wash_cycle_size_by_contract.is_empty()
        && stats.output_input_ratio_by_contract.is_empty()
    {
        return;
    }
    let rows = sorted_match_contract_behavior_rows(stats);

    lines.extend([String::new(), "## 合约行为明细".to_string()]);
    for row in &rows {
        let output_input = output_input_ratio_for_contract(stats, &row.contract_address);
        append_match_contract_behavior_group(lines, row, output_input);
    }
}

fn append_match_contract_behavior_group(
    lines: &mut Vec<String>,
    row: &PaperContractBehaviorStatsPayload,
    output_input: Option<&PaperOutputInputRatioRowPayload>,
) {
    lines.extend([
        String::new(),
        format!("### Match 合约 {}", short_identifier(&row.contract_address)),
    ]);

    let table_count = [
        !row.wash_trading.is_empty(),
        !row.pump_and_exit.is_empty(),
        !row.star_behaviors.is_empty(),
        !row.layered_transfers.is_empty(),
        !row.inventory_concentration.is_empty(),
        row.honest_buyers
            .iter()
            .any(|buyer| buyer.source_pattern != "unattributed_sale"),
        cycle_size_distribution_total(&contract_wash_cycle_size_distribution(row)) > 0,
        output_input.is_some(),
    ]
    .into_iter()
    .filter(|has_table| *has_table)
    .count();

    if table_count == 0 {
        lines.push("- 无可展示行为明细".to_string());
        return;
    }

    append_match_contract_wash_trading(lines, row);
    append_match_contract_pump_exit(lines, row);
    append_match_contract_star_behaviors(lines, row);
    append_match_contract_layered_transfers(lines, row);
    append_match_contract_inventory_concentration(lines, row);
    append_match_contract_honest_buyers(lines, row);
    append_match_contract_wash_cycle_size_distribution(lines, row);
    append_match_contract_output_input_ratio(lines, output_input);
}

fn append_match_contract_wash_trading(
    lines: &mut Vec<String>,
    row: &PaperContractBehaviorStatsPayload,
) {
    if row.wash_trading.is_empty() {
        return;
    }
    let mut rows = row.wash_trading.iter().collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        compare_desc(left.fake_volume_usd, right.fake_volume_usd)
            .then_with(|| {
                right
                    .participant_node_count
                    .cmp(&left.participant_node_count)
            })
            .then_with(|| left.cycle_id.cmp(&right.cycle_id))
    });

    lines.extend([
        String::new(),
        "#### Wash Trading".to_string(),
        "| 环 ID | 参与节点数 | token 基尼系数 | 平均周期（区块数） | 虚假交易额 ETH/USD |"
            .to_string(),
        "| --- | ---: | ---: | ---: | ---: |".to_string(),
    ]);
    for item in rows {
        lines.push(format!(
            "| {} | {} | {} | {} | {} / {} |",
            short_identifier(&item.cycle_id),
            item.participant_node_count,
            format_optional_number(item.token_gini),
            format_optional_number(item.avg_cycle_blocks),
            format_number(item.fake_volume_eth),
            format_number(item.fake_volume_usd)
        ));
    }
}

fn append_match_contract_pump_exit(
    lines: &mut Vec<String>,
    row: &PaperContractBehaviorStatsPayload,
) {
    if row.pump_and_exit.is_empty() {
        return;
    }
    let mut rows = row.pump_and_exit.iter().collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        compare_desc(left.linked_loss_usd, right.linked_loss_usd)
            .then_with(|| {
                right
                    .linked_honest_buyer_count
                    .cmp(&left.linked_honest_buyer_count)
            })
            .then_with(|| left.cycle_id.cmp(&right.cycle_id))
    });

    lines.extend([
        String::new(),
        "#### Pump-and-Exit".to_string(),
        "| 环 ID | 出货延迟时间 | 出货价格溢价 | 出货比例 | 关联买家数 | 关联损失 ETH/USD |"
            .to_string(),
        "| --- | ---: | ---: | ---: | ---: | ---: |".to_string(),
    ]);
    for item in rows {
        lines.push(format!(
            "| {} | {} | {} ({}/{}) | {} ({}/{}) | {} | {} / {} |",
            short_identifier(&item.cycle_id),
            format_seconds_i64(item.exit_delay_seconds),
            format_multiple(item.exit_price_premium),
            format_number(item.exit_price_premium_numerator),
            format_number(item.exit_price_premium_denominator),
            format_ratio(item.exit_ratio),
            item.exit_ratio_numerator,
            item.exit_ratio_denominator,
            item.linked_honest_buyer_count,
            format_number(item.linked_loss_eth),
            format_number(item.linked_loss_usd)
        ));
    }
}

fn append_match_contract_star_behaviors(
    lines: &mut Vec<String>,
    row: &PaperContractBehaviorStatsPayload,
) {
    if row.star_behaviors.is_empty() {
        return;
    }
    let mut rows = row.star_behaviors.iter().collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        compare_desc(left.total_value_usd, right.total_value_usd)
            .then_with(|| right.edges.cmp(&left.edges))
            .then_with(|| left.behavior.cmp(&right.behavior))
    });

    lines.extend([
        String::new(),
        "#### 星型行为".to_string(),
        "| behavior | centers | edges | wallets | tokens | avg fan-out | total value ETH/USD |"
            .to_string(),
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |".to_string(),
    ]);
    for item in rows {
        lines.push(format!(
            "| {} | {} | {} | {} | {} | {} ({}/{}) | {} / {} |",
            markdown_cell(&item.behavior),
            item.centers,
            item.edges,
            item.wallets,
            item.tokens,
            format_optional_number(item.avg_fan_out),
            item.avg_fan_out_numerator,
            item.avg_fan_out_denominator,
            format_number(item.total_value_eth),
            format_number(item.total_value_usd)
        ));
    }
}

fn append_match_contract_layered_transfers(
    lines: &mut Vec<String>,
    row: &PaperContractBehaviorStatsPayload,
) {
    if row.layered_transfers.is_empty() {
        return;
    }
    let mut rows = row.layered_transfers.iter().collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        compare_desc(left.total_value_usd, right.total_value_usd)
            .then_with(|| right.length.cmp(&left.length))
            .then_with(|| left.path_id.cmp(&right.path_id))
    });

    lines.extend([
        String::new(),
        "#### Layered Transfer".to_string(),
        "| path ID | tokens | length | wallets | zero/low-value hops | total path duration | total value ETH/USD |"
            .to_string(),
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |".to_string(),
    ]);
    for item in rows {
        lines.push(format!(
            "| {} | {} | {} | {} | {} | {} | {} / {} |",
            short_identifier(&item.path_id),
            item.tokens,
            item.length,
            item.wallets,
            item.zero_or_low_value_hops,
            format_seconds_i64(item.total_path_duration_seconds),
            format_number(item.total_value_eth),
            format_number(item.total_value_usd)
        ));
    }
}

fn append_match_contract_inventory_concentration(
    lines: &mut Vec<String>,
    row: &PaperContractBehaviorStatsPayload,
) {
    if row.inventory_concentration.is_empty() {
        return;
    }
    let mut rows = row.inventory_concentration.iter().collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        compare_desc(left.value_collected_usd, right.value_collected_usd)
            .then_with(|| right.inbound_txns.cmp(&left.inbound_txns))
            .then_with(|| left.hub_address.cmp(&right.hub_address))
    });

    lines.extend([
        String::new(),
        "#### Inventory Concentration".to_string(),
        "| hub address | source wallets | inbound txns | token share | value collected ETH/USD | value share | collection window |"
            .to_string(),
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |".to_string(),
    ]);
    for item in rows {
        lines.push(format!(
            "| {} | {} | {} | {} ({}/{}) | {} / {} | {} ({}/{}) | {} |",
            short_identifier(&item.hub_address),
            item.source_wallets,
            item.inbound_txns,
            format_ratio(item.token_share),
            item.token_share_numerator,
            item.token_share_denominator,
            format_number(item.value_collected_eth),
            format_number(item.value_collected_usd),
            format_ratio(item.value_share),
            format_number(item.value_share_numerator),
            format_number(item.value_share_denominator),
            format_seconds_i64(item.collection_window_seconds)
        ));
    }
}

fn append_match_contract_honest_buyers(
    lines: &mut Vec<String>,
    row: &PaperContractBehaviorStatsPayload,
) {
    let mut rows = row
        .honest_buyers
        .iter()
        .filter(|buyer| buyer.source_pattern != "unattributed_sale")
        .collect::<Vec<_>>();
    if rows.is_empty() {
        return;
    }
    rows.sort_by(|left, right| {
        compare_desc(left.total_paid_usd, right.total_paid_usd)
            .then_with(|| right.fake_nft_bought.cmp(&left.fake_nft_bought))
            .then_with(|| left.honest_buyer.cmp(&right.honest_buyer))
    });

    lines.extend([
        String::new(),
        "#### 诚实买家".to_string(),
        "| honest buyer | fake NFT bought | total paid ETH/USD | source pattern | time-to-purchase | still holding | holding time |"
            .to_string(),
        "| --- | ---: | ---: | --- | ---: | --- | ---: |".to_string(),
    ]);
    for item in rows {
        lines.push(format!(
            "| {} | {} | {} / {} | {} | {} | {} | {} |",
            short_identifier(&item.honest_buyer),
            item.fake_nft_bought,
            format_number(item.total_paid_eth),
            format_number(item.total_paid_usd),
            markdown_cell(&item.source_pattern),
            format_seconds_i64(item.time_to_purchase_seconds),
            format_bool(item.still_holding),
            format_seconds_i64(item.holding_seconds)
        ));
    }
}

fn append_match_contract_wash_cycle_size_distribution(
    lines: &mut Vec<String>,
    row: &PaperContractBehaviorStatsPayload,
) {
    let distribution = contract_wash_cycle_size_distribution(row);
    if cycle_size_distribution_total(&distribution) == 0 {
        return;
    }

    lines.extend([
        String::new(),
        "#### Wash Cycle 节点规模".to_string(),
        "| 节点数 | 循环数 | 循环占比 |".to_string(),
        "| --- | ---: | ---: |".to_string(),
    ]);
    for item in distribution {
        lines.push(format!(
            "| {} | {} | {} ({}/{}) |",
            item.node_count_bucket,
            item.cycle_count,
            format_ratio(item.cycle_ratio),
            item.cycle_ratio_numerator,
            item.cycle_ratio_denominator
        ));
    }
}

fn append_match_contract_output_input_ratio(
    lines: &mut Vec<String>,
    row: Option<&PaperOutputInputRatioRowPayload>,
) {
    let Some(row) = row else {
        return;
    };

    lines.extend([
        String::new(),
        "#### 产出投入比".to_string(),
        "| 产出 USD | 投入 USD | 产出/投入 |".to_string(),
        "| ---: | ---: | ---: |".to_string(),
        format!(
            "| {} | {} | {} ({}/{}) |",
            format_number(row.output_usd),
            format_number(row.input_usd),
            format_multiple(row.output_input_ratio),
            format_number(row.output_input_ratio_numerator),
            format_number(row.output_input_ratio_denominator)
        ),
    ]);
}

fn sorted_match_contract_behavior_rows(
    stats: &PaperStatsPayload,
) -> Vec<PaperContractBehaviorStatsPayload> {
    let mut rows = stats.contract_behavior_stats.clone();
    let mut seen_contracts = rows
        .iter()
        .map(|row| row.contract_address.clone())
        .collect::<BTreeSet<_>>();
    for wash_row in &stats.wash_cycle_size_by_contract {
        if seen_contracts.insert(wash_row.contract_address.clone()) {
            rows.push(PaperContractBehaviorStatsPayload {
                contract_address: wash_row.contract_address.clone(),
                wash_cycle_size_distribution: wash_row.distribution.clone(),
                ..PaperContractBehaviorStatsPayload::default()
            });
        }
    }
    for ratio_row in &stats.output_input_ratio_by_contract {
        if seen_contracts.insert(ratio_row.contract_address.clone()) {
            rows.push(PaperContractBehaviorStatsPayload {
                contract_address: ratio_row.contract_address.clone(),
                ..PaperContractBehaviorStatsPayload::default()
            });
        }
    }
    rows.sort_by(|left, right| {
        compare_desc(
            contract_behavior_impact_usd(left) + output_input_impact_usd(stats, left),
            contract_behavior_impact_usd(right) + output_input_impact_usd(stats, right),
        )
        .then_with(|| {
            contract_behavior_instance_count(right).cmp(&contract_behavior_instance_count(left))
        })
        .then_with(|| left.contract_address.cmp(&right.contract_address))
    });
    rows
}

fn output_input_ratio_for_contract<'a>(
    stats: &'a PaperStatsPayload,
    contract_address: &str,
) -> Option<&'a PaperOutputInputRatioRowPayload> {
    stats
        .output_input_ratio_by_contract
        .iter()
        .find(|row| row.contract_address.eq_ignore_ascii_case(contract_address))
}

fn output_input_impact_usd(
    stats: &PaperStatsPayload,
    row: &PaperContractBehaviorStatsPayload,
) -> f64 {
    output_input_ratio_for_contract(stats, &row.contract_address)
        .map(|item| item.output_usd)
        .unwrap_or_default()
}

fn short_identifier(value: &str) -> String {
    let trimmed = value.trim();
    if let Some((address, suffix)) = trimmed.split_once(':') {
        if address.len() > 18 && address.starts_with("0x") {
            return format!("{}:{suffix}", short_identifier(address));
        }
    }
    if trimmed.len() > 18 && trimmed.starts_with("0x") {
        format!("{}...{}", &trimmed[..6], &trimmed[trimmed.len() - 6..])
    } else if trimmed.chars().count() > 32 {
        let start = trimmed.chars().take(14).collect::<String>();
        let end = trimmed
            .chars()
            .rev()
            .take(8)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect::<String>();
        format!("{start}...{end}")
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

fn cycle_size_distribution_total(rows: &[PaperWashCycleSizeRowPayload]) -> i64 {
    rows.iter().map(|row| row.cycle_count).sum()
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
