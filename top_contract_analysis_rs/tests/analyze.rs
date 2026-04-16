use top_contract_analysis_rs::models::{
    ReportSummary, SeedContractPayload, SingleReportPayload,
};
use top_contract_analysis_rs::reporting::{
    default_output_basename, render_human_readable_report,
};

#[test]
fn default_output_basename_matches_existing_prefix() {
    let payload = SingleReportPayload {
        seed_contract: SeedContractPayload {
            name: "Azuki".into(),
            contract_address: "0xseed".into(),
            ..Default::default()
        },
        ..Default::default()
    };

    assert_eq!(default_output_basename(&payload), "top_contract_analysis__azuki");
}

#[test]
fn single_report_markdown_contains_seed_and_summary_counts() {
    let payload = SingleReportPayload {
        seed_contract: SeedContractPayload {
            contract_address: "0xseed".into(),
            ..Default::default()
        },
        report_summary: ReportSummary {
            high_confidence_contract_count: 2,
            low_confidence_contract_count: 1,
            ..Default::default()
        },
    };

    let markdown = render_human_readable_report(&payload);

    assert!(markdown.contains("# Top NFT 合约分析报告"));
    assert!(markdown.contains("- Seed: 0xseed"));
    assert!(markdown.contains("- 高置信: 2"));
    assert!(markdown.contains("- 低置信: 1"));
}
