//! Markdown report writers for offline dedup outputs and paper-style summaries.

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

fn f64_cell(v: &Value) -> String {
    match v {
        Value::Number(n) => n
            .as_f64()
            .map(|x| {
                if x == 0.0 {
                    "0.000000".into()
                } else {
                    format!("{x:.6}")
                }
            })
            .unwrap_or_else(|| n.to_string()),
        Value::Null => "null".into(),
        other => other.to_string(),
    }
}

fn u64_cell(v: &Value) -> String {
    match v {
        Value::Number(n) => n
            .as_u64()
            .map(|x| x.to_string())
            .or_else(|| n.as_i64().map(|x| x.to_string()))
            .unwrap_or_else(|| n.to_string()),
        Value::Null => "0".into(),
        other => other.to_string(),
    }
}

fn percent_value(ratio: f64) -> String {
    let percent = ratio * 100.0;
    if percent == 0.0 || percent.abs() >= 0.01 {
        format!("{percent:.2}%")
    } else {
        format!("{percent:.6}%")
    }
}

fn pct_cell(ratio: &Value, numer: &Value, denom: &Value) -> String {
    match ratio.as_f64() {
        Some(r) if denom.as_u64().unwrap_or(0) > 0 || denom.as_f64().unwrap_or(0.0) > 0.0 => {
            format!(
                "{} ({}/{})",
                percent_value(r),
                u64_cell(numer),
                u64_cell(denom)
            )
        }
        Some(r) => percent_value(r),
        None => "null".into(),
    }
}

fn scale_table(rows: &[DuplicateScaleRow]) -> String {
    let mut out = String::from(
        "| 类别 | 重复 NFT 数 | NFT 占比 | 重复合约数 | 合约占比 |\n| --- | ---: | ---: | ---: | ---: |\n",
    );
    for row in rows {
        let nft_ratio = row
            .duplicate_nft_ratio
            .map(|v| {
                format!(
                    "{} ({}/{})",
                    percent_value(v),
                    row.duplicate_nft_ratio_numerator,
                    row.duplicate_nft_ratio_denominator
                )
            })
            .unwrap_or_else(|| "null".into());
        let contract_ratio = row
            .duplicate_contract_ratio
            .map(|v| {
                format!(
                    "{} ({}/{})",
                    percent_value(v),
                    row.duplicate_contract_ratio_numerator,
                    row.duplicate_contract_ratio_denominator
                )
            })
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

fn behavior_label(key: &str) -> String {
    match key {
        "wash_trading" => "Wash Trading".into(),
        "pump_and_exit" => "Pump-and-Exit".into(),
        "sybil_distribution" => "Sybil Distribution".into(),
        "fraud_revenue" => "Fraud Revenue".into(),
        "poisoning" => "Poisoning".into(),
        "layered_transfer" => "Layered Transfer".into(),
        "inventory_concentration" => "Inventory Concentration".into(),
        "total" => "total".into(),
        other => other.to_owned(),
    }
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
            "- suspected_duplicate_contract_count: {}\n- legit_duplicate_contract_count: {}\n- infringing_nft_count: {}\n- honest_paid_exposure_usd: {}\n- operator_output_usd: {}\n",
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
    // Keep a thin alias for offline-dedup-only summary; full paper tables live in all_chains.
    write_all_chains_md(path, summary, &[])
}

/// Paper-style markdown for any of the four scopes (intra / matrix / cross / all_chains).
pub fn write_all_chains_md(
    path: &Path,
    summary: &Value,
    scale: &[DuplicateScaleRow],
) -> Result<(), Analysis2Error> {
    let scope = summary
        .get("scope")
        .and_then(|v| v.as_str())
        .unwrap_or("all_chains");
    let mut body = format!("# NFT 论文统计汇总报告（scope = {scope}）\n\n");

    // Header counts. API/data-quality problems are documented separately and
    // never remove otherwise available seed results from this report.
    body.push_str(&format!(
        "- selected seeds: {}\n- included seed reports: {}\n- excluded seeds: {}\n- seed_with_duplicate: {} / {} ({})\n\n",
        summary["selected_seed_count"],
        summary["included_seed_report_count"],
        summary["excluded_seed_count"],
        summary["seed_with_duplicate_count"],
        summary["seed_duplicate_ratio_denominator"],
        f64_cell(&summary["seed_duplicate_ratio"]),
    ));

    // ## 重复规模
    body.push_str("## 重复规模\n\n");
    if scale.is_empty() {
        // Prefer embedded duplicate_scale from JSON when caller passes empty slice.
        if let Some(rows) = summary.get("duplicate_scale").and_then(|v| v.as_array()) {
            body.push_str("| 类别 | 重复 NFT 数 | NFT 占比 | 重复合约数 | 合约占比 |\n| --- | ---: | ---: | ---: | ---: |\n");
            for row in rows {
                let nft_n = u64_cell(&row["duplicate_nft_count"]);
                let c_n = u64_cell(&row["duplicate_contract_count"]);
                let nft_ratio = match row["duplicate_nft_ratio"].as_f64() {
                    Some(r) => format!(
                        "{} ({}/{})",
                        percent_value(r),
                        u64_cell(&row["duplicate_nft_ratio_numerator"]),
                        u64_cell(&row["duplicate_nft_ratio_denominator"])
                    ),
                    None => "null".into(),
                };
                let c_ratio = match row["duplicate_contract_ratio"].as_f64() {
                    Some(r) => format!(
                        "{} ({}/{})",
                        percent_value(r),
                        u64_cell(&row["duplicate_contract_ratio_numerator"]),
                        u64_cell(&row["duplicate_contract_ratio_denominator"])
                    ),
                    None => "null".into(),
                };
                body.push_str(&format!(
                    "| {} | {} | {} | {} | {} |\n",
                    row["category"].as_str().unwrap_or("?"),
                    nft_n,
                    nft_ratio,
                    c_n,
                    c_ratio
                ));
            }
        } else {
            body.push_str("_无重复规模数据_\n");
        }
    } else {
        body.push_str(&scale_table(scale));
    }

    // Matrix: additional per-secondary-chain scale tables.
    if let Some(blocks) = summary.get("matrix_blocks").and_then(|v| v.as_array()) {
        for block in blocks {
            let primary = block
                .get("primary_chain")
                .and_then(|v| v.as_str())
                .unwrap_or("?");
            let sec = block
                .get("secondary_chain")
                .and_then(|v| v.as_str())
                .unwrap_or("?");
            body.push_str(&format!("\n### Matrix {primary} → {sec}\n\n"));
            if let Some(rows) = block.get("rows").and_then(|v| v.as_array()) {
                body.push_str("| 类别 | 重复 NFT 数 | NFT 占比 | 重复合约数 | 合约占比 |\n| --- | ---: | ---: | ---: | ---: |\n");
                for row in rows {
                    let nft_ratio = match row["duplicate_nft_ratio"].as_f64() {
                        Some(r) => format!(
                            "{} ({}/{})",
                            percent_value(r),
                            u64_cell(&row["duplicate_nft_ratio_numerator"]),
                            u64_cell(&row["duplicate_nft_ratio_denominator"])
                        ),
                        None => "null".into(),
                    };
                    let c_ratio = match row["duplicate_contract_ratio"].as_f64() {
                        Some(r) => format!(
                            "{} ({}/{})",
                            percent_value(r),
                            u64_cell(&row["duplicate_contract_ratio_numerator"]),
                            u64_cell(&row["duplicate_contract_ratio_denominator"])
                        ),
                        None => "null".into(),
                    };
                    body.push_str(&format!(
                        "| {} | {} | {} | {} | {} |\n",
                        row["category"].as_str().unwrap_or("?"),
                        u64_cell(&row["duplicate_nft_count"]),
                        nft_ratio,
                        u64_cell(&row["duplicate_contract_count"]),
                        c_ratio
                    ));
                }
            }
            if let Some(direction) = block.get("summary") {
                let econ = &direction["economics"];
                body.push_str("\n| 候选合约 | 疑似合约 | 侵权 NFT | 行为合约 | 攻击者产出 USD | 买家损失 USD | Gas USD |\n| ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n");
                body.push_str(&format!(
                    "| {} | {} | {} | {} | {} | {} | {} |\n",
                    u64_cell(&direction["candidate_contract_count"]),
                    u64_cell(&direction["suspected_duplicate_contract_count"]),
                    u64_cell(&direction["infringing_nft_count"]),
                    u64_cell(&direction["behavior_contract_count"]),
                    f64_cell(&econ["operator_output_usd"]),
                    f64_cell(&econ["honest_paid_exposure_usd"]),
                    f64_cell(&econ["total_gas_usd"]),
                ));
            }
        }
    }

    if summary["analysis_available"].as_bool() == Some(false) {
        body.push_str(
            "\n## 深度分析\n\n_该产物来自 run-dedup，仅包含去重规模与候选数量；合法性、行为、地址和经济统计未执行，因此不以 0 代替缺失结果。_\n",
        );
        return write_text(path, &body);
    }

    // ## 地址分类
    let addr = &summary["address_classification"];
    body.push_str("\n## 地址分类\n\n");
    body.push_str("| 类别 | 恶意地址数量 | 多次侵权地址数 | 诚实地址数量 | 地址总数 |\n| --- | ---: | ---: | ---: | ---: |\n");
    body.push_str(&format!(
        "| all | {} | {} | {} | {} |\n",
        u64_cell(&addr["malicious_address_count"]),
        u64_cell(&addr["repeat_infringing_malicious_address_count"]),
        u64_cell(&addr["honest_address_count"]),
        u64_cell(&addr["total_address_count"]),
    ));

    // ## 攻击者成本（all monetary fields use run-time USD prices）
    let econ = &summary["economics"];
    body.push_str("\n## 攻击者成本\n\n");
    body.push_str(
        "| cost | Setup Gas (USD) | Lure Gas (USD) | Exit Gas (USD) | Total Gas (USD) | 攻击投入集中度 |\n| --- | ---: | ---: | ---: | ---: | ---: |\n",
    );
    let conc = match econ["top_contract_gas_contribution_ratio"].as_f64() {
        Some(r) => format!(
            "{} ({}/{})",
            percent_value(r),
            f64_cell(&econ["top_contract_gas_contribution_numerator_usd"]),
            f64_cell(&econ["top_contract_gas_contribution_denominator_usd"])
        ),
        None => "null".into(),
    };
    body.push_str(&format!(
        "| gas | {} | {} | {} | {} | {} |\n",
        f64_cell(&econ["setup_gas_usd"]),
        f64_cell(&econ["lure_gas_usd"]),
        f64_cell(&econ["exit_gas_usd"]),
        f64_cell(&econ["total_gas_usd"]),
        conc,
    ));
    body.push_str(
        "\n> 说明：所有金额均按程序执行时取得的现价换算为 USD；无法定价的支付不会按 0 USD 计入。\n",
    );
    if econ["evidence_coverage_complete"].as_bool() == Some(false) {
        body.push_str(
            "\n> 警告：本范围的链上证据覆盖不完整；金额仅代表已取得证据，不应解释为完整总体。\n",
        );
    }
    if econ["observed_usd_pricing_complete"].as_bool() == Some(false) {
        body.push_str(
            "\n> 警告：已取得的交易、资金流、mint 支付或 Gas 中存在无法可靠换算 USD 的记录；金额仅代表已定价子集。\n",
        );
    }
    if econ["operator_output_attribution_complete"].as_bool() == Some(false) {
        body.push_str(
            "\n> 警告：部分销售、版税或 mint 收款方无法完成操作者归因；操作者产出不是完整总体。\n",
        );
    }

    body.push_str("\n## 成交与操作者产出\n\n");
    body.push_str(
        "| 成交总额 USD | 市场协议费 USD | 创作者版税 USD | 操作者收到的版税 USD | 操作者总产出 USD |\n| ---: | ---: | ---: | ---: | ---: |\n",
    );
    body.push_str(&format!(
        "| {} | {} | {} | {} | {} |\n",
        f64_cell(&econ["gross_sales_volume_usd"]),
        f64_cell(&econ["marketplace_fee_usd"]),
        f64_cell(&econ["royalty_fee_usd"]),
        f64_cell(&econ["operator_royalty_usd"]),
        f64_cell(&econ["operator_output_usd"]),
    ));

    // ## 产出投入比 (USD/USD only; contracts without priced gas excluded from counts)
    body.push_str("\n## 产出投入比\n\n");
    body.push_str(
        "| scope | 可比样本产出 USD | 可比样本投入 USD (gas×spot) | 产出/投入 | >=1 数量占比 | <1 数量占比 |\n| --- | ---: | ---: | ---: | ---: | ---: |\n",
    );
    let ratio_s = match econ["output_input_ratio"].as_f64() {
        Some(r) => format!("{r:.5}x"),
        None => "null".into(),
    };
    let ge1 = match econ["output_input_ratio_ge1_share"].as_f64() {
        Some(r) => format!(
            "{} ({}/{})",
            percent_value(r),
            u64_cell(&econ["output_input_ratio_ge1_count"]),
            u64_cell(&econ["output_input_ratio_count"])
        ),
        None => "null".into(),
    };
    let lt1 = match econ["output_input_ratio_lt1_share"].as_f64() {
        Some(r) => format!(
            "{} ({}/{})",
            percent_value(r),
            u64_cell(&econ["output_input_ratio_lt1_count"]),
            u64_cell(&econ["output_input_ratio_count"])
        ),
        None => "null".into(),
    };
    body.push_str(&format!(
        "| total | {} | {} | {} | {} | {} |\n",
        f64_cell(
            econ.get("ratio_eligible_operator_output_usd")
                .unwrap_or(&econ["ratio_operator_output_usd"])
        ),
        f64_cell(
            econ.get("ratio_eligible_attacker_input_usd")
                .unwrap_or(&econ["attacker_input_usd"])
        ),
        ratio_s,
        ge1,
        lt1,
    ));
    body.push_str(&format!(
        "\n> 口径：总比值与 >=1/<1 分布均只使用同一批 {} 个可比合约；样本覆盖全部疑似合约：{}。全部已观察操作者产出为 {} USD。\n",
        u64_cell(econ.get("ratio_eligible_contract_count").unwrap_or(&econ["output_input_ratio_count"])),
        econ["ratio_sample_complete"],
        f64_cell(econ.get("all_observed_operator_output_usd").unwrap_or(&econ["operator_output_usd"])),
    ));
    body.push_str(&format!(
        "\n- candidate/operator funding_usd: {}\n- operator_internal_backflow_usd: {}\n- candidate/operator withdrawal_usd: {}\n",
        f64_cell(&econ["funding_usd"]),
        f64_cell(&econ["revenue_backflow_usd"]),
        f64_cell(&econ["withdrawal_usd"])
    ));

    // ## 诚实买家付费暴露
    let hit_contract_nfts = u64_cell(&econ["hit_contract_nft_count"]);
    let stuck = u64_cell(&econ["stuck_nft_count"]);
    let stuck_ratio = match econ["stuck_nft_ratio"].as_f64() {
        Some(r) => format!("{} ({stuck}/{hit_contract_nfts})", percent_value(r)),
        None => format!("n/a ({stuck}/{hit_contract_nfts})"),
    };
    body.push_str("\n## 诚实买家付费暴露\n\n");
    body.push_str(
        "| 套牢 NFT | NFT 套牢占比 | 二级市场付费暴露 USD | 付费 mint 暴露 USD | 总付费暴露 USD |\n| ---: | ---: | ---: | ---: | ---: |\n",
    );
    body.push_str(&format!(
        "| {stuck} | {stuck_ratio} | {} | {} | {} |\n",
        f64_cell(&econ["secondary_sale_paid_exposure_usd"]),
        f64_cell(&econ["paid_mint_exposure_usd"]),
        f64_cell(&econ["honest_paid_exposure_usd"]),
    ));

    // ## 恶意行为汇总
    body.push_str("\n## 恶意行为汇总\n\n");
    body.push_str(&format!(
        "- 有合约级行为统计的合约数: {}\n",
        u64_cell(
            summary
                .get("behavior_contract_count")
                .unwrap_or(&Value::Null)
        )
    ));
    body.push_str(
        "| 行为 | 合约数 | 覆盖率 | 实例数 | 行为占比 | 地址数 | NFT 数 | 关联买家 | 关联付费暴露 USD |\n| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n",
    );
    let suspected = summary["suspected_duplicate_contract_count"]
        .as_u64()
        .unwrap_or(0);
    let order = [
        "wash_trading",
        "pump_and_exit",
        "sybil_distribution",
        "fraud_revenue",
        "poisoning",
        "layered_transfer",
        "inventory_concentration",
        "total",
    ];
    if let Some(behaviors) = summary.get("behaviors").and_then(|v| v.as_object()) {
        for key in order {
            let Some(row) = behaviors.get(key) else {
                continue;
            };
            let contracts = u64_cell(&row["contract_count"]);
            let coverage = match row.get("contract_coverage_ratio").and_then(|v| v.as_f64()) {
                Some(r) => format!("{} ({contracts}/{suspected})", percent_value(r)),
                None if key == "total" => format!("n/a ({contracts}/{suspected})"),
                None => "null".into(),
            };
            let instances = u64_cell(&row["instance_count"]);
            let inst_ratio = match row.get("instance_ratio").and_then(|v| v.as_f64()) {
                Some(r) => percent_value(r),
                None if key == "total" => "100.00%".into(),
                None => "null".into(),
            };
            body.push_str(&format!(
                "| {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
                behavior_label(key),
                contracts,
                coverage,
                instances,
                inst_ratio,
                u64_cell(&row["address_count"]),
                u64_cell(&row["nft_count"]),
                u64_cell(&row["linked_buyer_count"]),
                f64_cell(&row["linked_paid_exposure_usd"]),
            ));
        }
    }

    // ## Wash Cycle 节点规模
    body.push_str("\n## Wash Cycle 节点规模\n\n");
    body.push_str("| 节点数 | 循环数 | 循环占比 |\n| --- | ---: | ---: |\n");
    if let Some(rows) = summary
        .get("wash_cycle_size_distribution")
        .and_then(|v| v.as_array())
    {
        for row in rows {
            let ratio = pct_cell(
                &row["cycle_ratio"],
                &row["cycle_ratio_numerator"],
                &row["cycle_ratio_denominator"],
            );
            body.push_str(&format!(
                "| {} | {} | {} |\n",
                row["node_count_bucket"].as_str().unwrap_or("?"),
                u64_cell(&row["cycle_count"]),
                ratio
            ));
        }
    } else {
        body.push_str("| — | 0 | null |\n");
    }

    // ## 数据质量
    let dq = &summary["data_quality"];
    let gas = &dq["evidence"]["gas"];
    let pricing = &dq["pricing"];
    let dimensions = &dq["dedup_dimensions"];
    body.push_str("\n## 数据质量\n\n");
    body.push_str(&format!(
        "- 代表候选 NFT 数: {}\n- 命中合约全部 NFT 数: {}（完整: {}）\n- 候选合约数: {}\n- 疑似重复合约数: {}\n- 官方参与型重复合约数: {}\n- 疑似侵权 NFT 数: {}\n- 合法关系验证 Complete/Incomplete: {} / {}\n- gas 证据 Complete/Empty/Failed/Truncated/NotRequested: {} / {} / {} / {} / {}\n- 销售定价 Priced/Unpriced/Amountless/AssumedPeg/Total: {} / {} / {} / {} / {}\n- 操作者销售净收入 Priced/Unpriced/Unknown/Total: {} / {} / {} / {}\n- 版税接收方 Unknown: {}\n- 操作者付费 mint 收入 Priced/Unpriced/Operator/AllPaid/UnknownReceiver: {} / {} / {} / {} / {}\n- 诚实买家付费 mint 损失定价 Priced/Unpriced/Total: {} / {} / {}\n- Gas 成本定价 Priced/Unpriced/Total: {} / {} / {}\n- 证据覆盖完整: {}\n- 已观察金额 USD 定价完整: {}\n- 操作者归因完整: {}\n- 操作者产出完整: {}\n- USD 估值完整: {}\n- 产出/投入可比样本完整: {}\n- 去重维度 token_uri/image_uri/metadata/name: {} / {} / {} / {}\n",
        u64_cell(dq.get("representative_candidate_nft_count").unwrap_or(&summary["representative_candidate_nft_count"])),
        u64_cell(&dq["hit_contract_nft_count"]),
        dq["hit_contract_nft_count_complete"],
        u64_cell(dq.get("candidate_contract_count").unwrap_or(&summary["candidate_contract_count"])),
        u64_cell(dq.get("suspected_duplicate_contract_count").unwrap_or(&summary["suspected_duplicate_contract_count"])),
        u64_cell(dq.get("legit_duplicate_contract_count").unwrap_or(&summary["legit_duplicate_contract_count"])),
        u64_cell(dq.get("infringing_nft_count").unwrap_or(&summary["infringing_nft_count"])),
        u64_cell(&dq["legit_relation_verification_complete"]),
        u64_cell(&dq["legit_relation_verification_incomplete"]),
        u64_cell(&gas["complete"]),
        u64_cell(&gas["empty"]),
        u64_cell(&gas["failed"]),
        u64_cell(&gas["truncated"]),
        u64_cell(&gas["not_requested"]),
        u64_cell(&pricing["priced_sale_count"]),
        u64_cell(&pricing["unpriced_sale_count"]),
        u64_cell(&pricing["amountless_sale_count"]),
        u64_cell(&pricing["assumed_stablecoin_peg_sale_count"]),
        u64_cell(&pricing["sale_count"]),
        u64_cell(&pricing["priced_operator_sale_proceeds_count"]),
        u64_cell(&pricing["unpriced_operator_sale_proceeds_count"]),
        u64_cell(&pricing["unknown_operator_sale_proceeds_count"]),
        u64_cell(&pricing["operator_sale_count"]),
        u64_cell(&pricing["unknown_royalty_recipient_count"]),
        u64_cell(&pricing["priced_operator_paid_mint_payment_count"]),
        u64_cell(&pricing["unpriced_operator_paid_mint_payment_count"]),
        u64_cell(&pricing["operator_paid_mint_payment_count"]),
        u64_cell(&pricing["paid_mint_payment_count"]),
        u64_cell(&pricing["unknown_paid_mint_receiver_count"]),
        u64_cell(&pricing["priced_honest_paid_mint_exposure_count"]),
        u64_cell(&pricing["unpriced_honest_paid_mint_exposure_count"]),
        u64_cell(&pricing["honest_paid_mint_exposure_count"]),
        u64_cell(&pricing["priced_gas_cost_contract_count"]),
        u64_cell(&pricing["unpriced_gas_cost_contract_count"]),
        u64_cell(&pricing["gas_cost_contract_count"]),
        pricing["evidence_coverage_complete"],
        pricing["observed_usd_pricing_complete"],
        pricing["operator_output_attribution_complete"],
        pricing["operator_output_complete"],
        pricing["usd_valuation_complete"],
        econ["ratio_sample_complete"],
        dimensions["token_uri_enabled"],
        dimensions["image_uri_enabled"],
        dimensions["metadata_enabled"],
        dimensions["name_enabled"],
    ));

    write_text(path, &body)
}
