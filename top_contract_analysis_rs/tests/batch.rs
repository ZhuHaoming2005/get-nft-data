use top_contract_analysis_rs::models::{
    BatchReportSummary, BatchSeedReportPayload, BatchSummaryPayload, OutputFilesPayload,
    ReportSummary, SeedContractPayload,
};
use top_contract_analysis_rs::reporting::render_batch_human_readable_report;

#[test]
fn batch_markdown_preserves_reference_summary_and_output_index_lines() {
    let payload = BatchSummaryPayload {
        batch_summary: BatchReportSummary {
            seed_report_count: 2,
            chain: "ethereum".into(),
            chains: vec!["ethereum".into()],
            open_license_detected_count: 1,
            candidate_contract_count_total: 10,
            high_confidence_contract_count_total: 4,
            low_confidence_contract_count_total: 6,
            infringing_nft_count_total: 11,
            malicious_address_count_total: 7,
            honest_address_count_total: 8,
            repeat_infringing_address_count_total: 3,
            repeat_infringing_address_count_global: 2,
            legit_duplicate_contract_count_total: 1,
            honest_purchase_total_eth_total: 12.5,
            stuck_cost_eth_total: 5.0,
            stuck_cost_ratio_overall: Some(0.4),
            buy_asset_ratio_known_address_count_total: 8,
            ratio_over_60_address_count_total: 3,
            ratio_over_60_address_ratio_overall: Some(0.375),
            ratio_over_80_address_count_total: 1,
            ratio_over_80_address_ratio_overall: Some(0.125),
            stuck_honest_address_count_total: 2,
            stuck_honest_address_ratio_overall: Some(0.25),
            corrupted_honest_address_count_total: 1,
            avg_seconds_to_honest_holder_mean: Some(12.5),
            median_seconds_to_honest_holder_median: Some(10.0),
            avg_mint_to_first_transfer_seconds_mean: Some(8.0),
            median_mint_to_first_transfer_seconds_median: Some(7.0),
            avg_unique_receiver_count_mean: Some(4.0),
            generated_at: "2026-04-17T00:00:00+00:00".into(),
        },
        seed_reports: vec![BatchSeedReportPayload {
            seed_contract: SeedContractPayload {
                name: "Azuki".into(),
                contract_address: "0xseed".into(),
                ..Default::default()
            },
            report_summary: ReportSummary {
                high_confidence_contract_count: 2,
                low_confidence_contract_count: 3,
                infringing_nft_count: 4,
                malicious_address_count: 5,
                honest_address_count: 6,
                repeat_infringing_address_count: 1,
                legit_duplicate_contract_count: 1,
                honest_purchase_total_eth: 7.5,
                stuck_cost_eth: 2.5,
                stuck_cost_ratio: Some(1.0 / 3.0),
                ratio_over_60_address_count: 2,
                ratio_over_60_address_ratio: Some(0.5),
                stuck_honest_address_count: 1,
                stuck_honest_address_ratio: Some(0.25),
                corrupted_honest_address_count: 1,
                avg_seconds_to_honest_holder: Some(10.0),
                median_seconds_to_honest_holder: Some(9.0),
                median_mint_to_first_transfer_seconds: Some(8.0),
                ..Default::default()
            },
            output_files: Some(OutputFilesPayload {
                json: "result/top_contract_analysis__azuki.json".into(),
                markdown: "result/top_contract_analysis__azuki.md".into(),
            }),
        }],
    };

    let markdown = render_batch_human_readable_report(&payload);

    assert!(markdown.contains("# Top NFT 合约批量分析总报告"));
    assert!(markdown.contains("- 检测到开放许可的 seed 数: 1"));
    assert!(markdown.contains("- 恶意地址总数: 7"));
    assert!(markdown.contains("- 诚实地址购买总金额(ETH/WETH)汇总: 12.5"));
    assert!(markdown.contains("- 套牢资金(ETH/WETH)汇总: 5 / 40.00%"));
    assert!(markdown.contains("- 买入金额占钱包总额 >60% 的地址数/总体占比: 3 / 37.50%"));
    assert!(markdown.contains("- 生成时间(UTC): 2026-04-17T00:00:00+00:00"));
    assert!(markdown.contains("## Seed 报告索引"));
    assert!(markdown.contains(
        "- Azuki (0xseed) | 高置信=2 | 低置信=3 | 侵权NFT=4 | 恶意地址=5 | 诚实地址=6 | 多次侵权地址=1 | 官方参与=1 | 诚实购买额=7.5 | 套牢资金=2.5/33.33% | >60%=2/50.00% | 套牢=1/25.00% | 被腐化=1 | 诚实购买时长=10秒 | 传播中位数=9秒 | 首次转手中位数=8秒 | JSON=result/top_contract_analysis__azuki.json | MD=result/top_contract_analysis__azuki.md"
    ));
}
