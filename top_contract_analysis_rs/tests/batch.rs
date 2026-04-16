use top_contract_analysis_rs::models::BatchSummaryPayload;
use top_contract_analysis_rs::reporting::render_batch_human_readable_report;

#[test]
fn batch_markdown_contains_summary_header() {
    let markdown = render_batch_human_readable_report(&BatchSummaryPayload::default());
    assert!(markdown.contains("# Top NFT 合约批量分析总报告"));
}
