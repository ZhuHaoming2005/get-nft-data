use crate::error::Result;
use crate::reporting::contract_writer::atomic_write_deferred;
use crate::reporting::AggregateSnapshot;
use std::fmt::Write as _;
use std::path::Path;

pub fn write_all_chains(path: &Path, summary: &AggregateSnapshot) -> Result<()> {
    let mut markdown = format!(
        "# 四链 NFT 重复分析\n\n\
         - 已持久化候选：{}\n\
         - 已分析 seed：{}\n\
         - 候选合约或 collection：{}\n\
         - 疑似重复合约或 collection：{}\n\
         - 合法重复合约或 collection：{}\n\
         - 疑似侵权 NFT：{}\n\
         - 恶意地址：{}\n\
         - 诚实地址：{}\n\
         - 攻击者 gas 投入（USD micros）：{}\n\
         - 攻击者产出（USD micros）：{}\n\
         - 诚实损失（USD micros）：{}\n\
         - 二级市场诚实损失（USD micros）：{}\n\
         - 付费 mint 诚实损失（USD micros）：{}\n\
         - 套牢 NFT：{}\n\
         - 套牢 NFT 比例：{}\n\
         - 套牢时间比例：{}\n\
         - 证据失败记录：{}\n\
         - 计价口径：事件当日 UTC 日桶 Alchemy 历史价（同日价格）；跨链仅汇总 USD\n\
         - CopyMint 口径：查重命中 − legit_duplicate（见 dedup_scopes CSV 的 matched/copymint 字段）\n\
         - 统计作用域：intra_chain / chain_matrix / cross_chain_summary / all_chains（查重与分析指标均分作用域输出）\n",
        summary.persisted_candidate_count,
        summary.analyzed_seed_count,
        summary.candidate_contract_count,
        summary.suspected_duplicate_contract_count,
        summary.legit_duplicate_contract_count,
        summary.infringing_nft_count,
        summary.malicious_address_count,
        summary.honest_address_count,
        summary.economics_derived.attacker_gas_usd_micros,
        summary.economics.operator_output_usd_micros,
        summary.economics.honest_loss_usd_micros,
        summary.economics.secondary_sale_loss_usd_micros,
        summary.economics.paid_mint_loss_usd_micros,
        summary.economics.stuck_nft_count,
        optional_ratio(summary.economics_derived.stuck_nft_ratio),
        optional_ratio(summary.economics_derived.stuck_time_ratio),
        summary.data_quality.failure_records,
    );
    markdown.push_str(
        "\n## 行为汇总\n\n\
         | 行为 | 合约数 | 覆盖率 | 实例数 | 实例占比 | 地址数 | NFT 数 | 关联买家 | 关联损失（USD micros） |\n\
         |---|---:|---:|---:|---:|---:|---:|---:|---:|\n",
    );
    for (name, metric) in &summary.behaviors {
        writeln!(
            markdown,
            "| {name} | {} | {} | {} | {} | {} | {} | {} | {} |",
            metric.contract_count,
            optional_ratio(metric.contract_coverage_ratio),
            metric.instance_count,
            optional_ratio(metric.instance_ratio),
            metric.address_count,
            metric.nft_count,
            metric.linked_buyer_count,
            metric.linked_loss_usd_micros,
        )
        .expect("writing to String cannot fail");
    }
    markdown.push_str("\n## 数据质量\n\n");
    writeln!(
        markdown,
        "- 候选资产覆盖率：{}\n- 截断候选合约：{}\n- 交易覆盖率：{}\n- 成交价格解析率：{}\n- 历史请求：{}\n- 历史成功：{}\n- 历史完整：{}\n- 历史失败：{}\n- 历史未请求：{}\n- 历史截断：{}\n",
        optional_ratio(summary.data_quality.candidate_asset_coverage),
        summary.data_quality.candidate_asset_truncated_contracts,
        optional_ratio(summary.data_quality.transaction_coverage),
        optional_ratio(summary.data_quality.sale_price_parse_ratio),
        summary.data_quality.history_assets_requested,
        summary.data_quality.history_assets_succeeded,
        summary.data_quality.history_assets_complete,
        summary.data_quality.history_assets_failed,
        summary.data_quality.history_assets_not_requested,
        summary.data_quality.history_assets_truncated,
    )
    .expect("writing to String cannot fail");
    atomic_write_deferred(path, markdown.as_bytes())
}

fn optional_ratio(value: Option<f64>) -> String {
    value.map_or_else(|| "unknown".to_owned(), |value| format!("{value:.6}"))
}
