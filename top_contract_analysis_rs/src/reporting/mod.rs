use once_cell::sync::Lazy;
use regex::Regex;
use unicode_normalization::UnicodeNormalization;

use crate::models::{BatchSummaryPayload, SingleReportPayload};

static SLUG_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"[^0-9a-zA-Z\u{4e00}-\u{9fff}]+").unwrap());

fn slugify(value: &str) -> String {
    let normalized = value.nfkc().collect::<String>();
    let lowered = normalized
        .trim()
        .to_lowercase()
        .replace('ß', "ss");
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

fn format_python_optional_scalar(value: Option<f64>) -> String {
    value
        .map(|number| number.to_string())
        .unwrap_or_else(|| "None".into())
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
        format!(
            "- 高置信疑似侵权合约数: {}",
            summary.high_confidence_contract_count
        ),
        format!(
            "- 低置信疑似侵权合约数: {}",
            summary.low_confidence_contract_count
        ),
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
            "- 诚实地址购买总金额(ETH/WETH): {}",
            summary.honest_purchase_total_eth
        ),
        format!(
            "- 套牢资金(ETH/WETH): {} / {}",
            summary.stuck_cost_eth,
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
        String::new(),
        "## 高置信疑似侵权合约".to_string(),
    ];

    if payload
        .suspected_infringing_duplicates_high_confidence
        .is_empty()
    {
        lines.push("- 无".to_string());
    } else {
        for item in &payload.suspected_infringing_duplicates_high_confidence {
            lines.push(format!(
                "- {}: {} 个重复 NFT | 命中原因={}",
                item.contract_address,
                item.candidate_count,
                item.match_reasons.join(", ")
            ));
        }
    }

    lines.extend([String::new(), "## 低置信疑似侵权合约".to_string()]);
    if payload.suspected_infringing_duplicates_low_confidence.is_empty() {
        lines.push("- 无".to_string());
    } else {
        for item in &payload.suspected_infringing_duplicates_low_confidence {
            lines.push(format!(
                "- {}: {} 个重复 NFT | 命中原因={}",
                item.contract_address,
                item.candidate_count,
                item.match_reasons.join(", ")
            ));
        }
    }

    lines.extend([
        String::new(),
        "## 被算法归为官方参与型重复的合约".to_string(),
        "- 说明: 该分组仅表示 mint 接收地址与官方地址集合存在交集。".to_string(),
    ]);
    if payload.legit_duplicates.is_empty() {
        lines.push("- 无".to_string());
    } else {
        for item in &payload.legit_duplicates {
            lines.push(format!(
                "- {}: {} 个重复 NFT | mint 接收地址(命中官方地址规则)={}",
                item.contract_address,
                item.candidate_count,
                item.mint_recipients.join(", ")
            ));
        }
    }

    lines.extend([String::new(), "## 地址行为信号".to_string()]);
    if payload.address_signals.is_empty() {
        lines.push("- 无".to_string());
    } else {
        for (contract, signal) in &payload.address_signals {
            lines.extend([
                format!("### {contract}"),
                format!("- Mint 地址数: {}", signal.mint_address_count),
                format!("- Mint 交易数: {}", signal.mint_count),
                format!("- 唯一接收地址数: {}", signal.unique_receiver_count),
                format!("- 循环交易边数: {}", signal.cycle_edge_count),
                format!("- 星状扩散中心数: {}", signal.star_distributor_count),
                format!(
                    "- Mint 到首次转手时间: {} 秒",
                    signal.mint_to_first_transfer_seconds
                ),
                format!(
                    "- 快速扩散: {}",
                    if signal.fast_spread { "是" } else { "否" }
                ),
            ]);
        }
    }

    lines.extend([String::new(), "## 受害者信号".to_string()]);
    if payload.victim_signals.is_empty() {
        lines.push("- 无".to_string());
    } else {
        for (contract, signal) in &payload.victim_signals {
            lines.extend([
                format!("### {contract}"),
                format!("- 当前持有地址数: {}", signal.owner_count),
                format!("- 套牢地址数: {}", signal.stuck_holder_count),
                format!(
                    "- 套牢地址占比: {}",
                    format_ratio(signal.stuck_holder_ratio)
                ),
                format!("- 疑似受害地址数: {}", signal.victim_wallet_count),
            ]);
        }
    }

    lines.extend([String::new(), "## 诚实地址画像".to_string()]);
    if payload.honest_address_stats.is_empty() {
        lines.push("- 无".to_string());
    } else {
        for (contract, stats) in &payload.honest_address_stats {
            lines.extend([
                format!("### {contract}"),
                format!("- 诚实地址数: {}", stats.honest_address_count),
                format!("- 被腐化地址数: {}", stats.corrupted_address_count),
                format!(
                    "- 诚实地址之间转售次数: {}",
                    stats.honest_to_honest_transfer_count
                ),
                format!(
                    "- 持有时长中位数: {} 秒",
                    format_python_optional_scalar(stats.median_holding_seconds)
                ),
                format!(
                    "- Mint 到诚实地址平均时间: {} 秒",
                    format_python_optional_scalar(stats.avg_seconds_to_honest_holder)
                ),
            ]);
        }
        for item in &payload.honest_addresses {
            lines.push(format!(
                "- {}:{}: interacted_token_count={} | currently_holding_token_count={} | hold_duration_median_seconds={} | 被腐化={} | honest_sale_to_honest_count={}",
                item.contract_address,
                item.address,
                item.interacted_token_count,
                item.currently_holding_token_count,
                format_python_optional_scalar(item.hold_duration_median_seconds),
                if item.is_corrupted_address { "是" } else { "否" },
                item.honest_sale_to_honest_count
            ));
        }
    }

    lines.extend([String::new(), "## 被骗地址画像".to_string()]);
    if payload.victim_addresses.is_empty() {
        lines.push("- 无".to_string());
    } else {
        for item in &payload.victim_addresses {
            lines.push(format!(
                "- {}: buy_tx_count={} | 买入金额(ETH/WETH)={} | 最后一次买入金额(ETH/WETH)={} | 买入前 ETH 余额: {} | 买入占比={} | 套牢={} | last_buy_tx={}",
                item.address,
                item.buy_tx_hashes.len(),
                item.buy_amount_eth,
                format_python_optional_scalar(item.last_buy_amount_eth),
                format_python_optional_scalar(item.buy_before_eth_balance),
                format_ratio(item.buy_asset_ratio),
                if item.is_stuck { "是" } else { "否" },
                if item.last_buy_tx_hash.is_empty() {
                    "n/a".to_string()
                } else {
                    item.last_buy_tx_hash.clone()
                }
            ));
        }
    }

    lines.extend([String::new(), "## 被骗交易与套牢资金".to_string()]);
    if payload.fraud_trade_stats.is_empty() {
        lines.push("- 无".to_string());
    } else {
        for (contract, stats) in &payload.fraud_trade_stats {
            lines.push(format!(
                "- {}: unique_buyers={} | eth_priced_sale_count={} | eth_priced_volume={} | stuck_wallet_count={} | stuck_cost_eth={}",
                contract,
                stats.unique_buyers,
                stats.eth_priced_sale_count,
                stats.eth_priced_volume,
                stats.stuck_wallet_count,
                stats.stuck_cost_eth
            ));
        }
    }

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
            "- 高置信疑似侵权合约总数: {}",
            summary.high_confidence_contract_count_total
        ),
        format!(
            "- 低置信疑似侵权合约总数: {}",
            summary.low_confidence_contract_count_total
        ),
        format!("- 疑似侵权 NFT 总数: {}", summary.infringing_nft_count_total),
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
            "- 诚实地址购买总金额(ETH/WETH)汇总: {}",
            summary.honest_purchase_total_eth_total
        ),
        format!(
            "- 套牢资金(ETH/WETH)汇总: {} / {}",
            summary.stuck_cost_eth_total,
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
                "- {} ({}) | 高置信={} | 低置信={} | 侵权NFT={} | 恶意地址={} | 诚实地址={} | 多次侵权地址={} | 官方参与={} | 诚实购买额={} | 套牢资金={}/{} | >60%={}/{} | 套牢={}/{} | 被腐化={} | 诚实购买时长={}秒 | 传播中位数={}秒 | 首次转手中位数={}秒 | JSON={} | MD={}",
                seed_name,
                seed.contract_address,
                report_summary.high_confidence_contract_count,
                report_summary.low_confidence_contract_count,
                report_summary.infringing_nft_count,
                report_summary.malicious_address_count,
                report_summary.honest_address_count,
                report_summary.repeat_infringing_address_count,
                report_summary.legit_duplicate_contract_count,
                report_summary.honest_purchase_total_eth,
                report_summary.stuck_cost_eth,
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
