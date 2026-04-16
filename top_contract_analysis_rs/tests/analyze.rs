use std::collections::BTreeMap;

use top_contract_analysis_rs::models::{
    AddressSignalPayload, DuplicateContractPayload, FraudTradeStatsPayload,
    HonestAddressPayload, HonestAddressStatsPayload, ReportSummary,
    SeedCollectionStatsPayload, SeedContractPayload, SingleReportPayload,
    VictimAddressPayload, VictimSignalPayload,
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
fn default_output_basename_casefolds_non_ascii_more_like_python() {
    let payload = SingleReportPayload {
        seed_contract: SeedContractPayload {
            name: "Straße".into(),
            contract_address: "0xseed".into(),
            ..Default::default()
        },
        ..Default::default()
    };

    assert_eq!(default_output_basename(&payload), "top_contract_analysis__strasse");
}

#[test]
fn single_report_markdown_preserves_reference_sections_and_summary_lines() {
    let payload = SingleReportPayload {
        seed_contract: SeedContractPayload {
            chain: "ethereum".into(),
            contract_address: "0xseed".into(),
            name: "Azuki".into(),
            symbol: "AZUKI".into(),
            token_type: "erc721".into(),
            contract_deployer: "0xdeployer".into(),
            deployed_block_number: 12345,
        },
        seed_collection_stats: SeedCollectionStatsPayload {
            seed_nft_count: 10,
            unique_token_uri_count: 8,
            unique_image_uri_count: 7,
            unique_name_count: 6,
            unique_symbol_count: 1,
        },
        report_summary: ReportSummary {
            open_license_detected: true,
            candidate_contract_count: 9,
            high_confidence_contract_count: 2,
            low_confidence_contract_count: 3,
            infringing_nft_count: 11,
            malicious_address_count: 4,
            honest_address_count: 5,
            repeat_infringing_address_count: 2,
            legit_duplicate_contract_count: 1,
            candidate_open_license_token_count: 6,
            candidate_open_license_contract_count: 2,
            honest_purchase_total_eth: 10.0,
            stuck_cost_eth: 6.5,
            stuck_cost_ratio: Some(0.65),
            buy_asset_ratio_known_address_count: 5,
            ratio_over_60_address_count: 3,
            ratio_over_60_address_ratio: Some(0.6),
            ratio_over_80_address_count: 1,
            ratio_over_80_address_ratio: Some(0.2),
            stuck_honest_address_count: 2,
            stuck_honest_address_ratio: Some(0.4),
            corrupted_honest_address_count: 1,
            avg_seconds_to_honest_holder: Some(12.5),
            median_seconds_to_honest_holder: Some(10.0),
            avg_mint_to_first_transfer_seconds: Some(8.0),
            median_mint_to_first_transfer_seconds: Some(7.0),
            avg_unique_receiver_count: Some(4.0),
        },
        suspected_infringing_duplicates_high_confidence: vec![DuplicateContractPayload {
            contract_address: "0xhigh".into(),
            candidate_count: 2,
            match_reasons: vec!["token_uri_match".into(), "name_match".into()],
            ..Default::default()
        }],
        suspected_infringing_duplicates_low_confidence: vec![DuplicateContractPayload {
            contract_address: "0xlow".into(),
            candidate_count: 1,
            match_reasons: vec!["symbol_match".into()],
            ..Default::default()
        }],
        legit_duplicates: vec![DuplicateContractPayload {
            contract_address: "0xlegit".into(),
            candidate_count: 1,
            mint_recipients: vec!["0xofficial".into()],
            ..Default::default()
        }],
        address_signals: BTreeMap::from([(
            "0xhigh".into(),
            AddressSignalPayload {
                mint_address_count: 2,
                mint_count: 3,
                unique_receiver_count: 4,
                cycle_edge_count: 1,
                star_distributor_count: 1,
                mint_to_first_transfer_seconds: 8,
                fast_spread: true,
            },
        )]),
        victim_signals: BTreeMap::from([(
            "0xhigh".into(),
            VictimSignalPayload {
                owner_count: 3,
                stuck_holder_count: 1,
                stuck_holder_ratio: Some(1.0 / 3.0),
                victim_wallet_count: 2,
            },
        )]),
        honest_address_stats: BTreeMap::from([(
            "0xhigh".into(),
            HonestAddressStatsPayload {
                honest_address_count: 2,
                corrupted_address_count: 1,
                honest_to_honest_transfer_count: 3,
                median_holding_seconds: Some(44.0),
                avg_seconds_to_honest_holder: Some(12.5),
            },
        )]),
        honest_addresses: vec![HonestAddressPayload {
            contract_address: "0xhigh".into(),
            address: "0xhonest".into(),
            interacted_token_count: 2,
            currently_holding_token_count: 1,
            hold_duration_median_seconds: Some(44.0),
            is_corrupted_address: true,
            honest_sale_to_honest_count: 1,
        }],
        victim_addresses: vec![VictimAddressPayload {
            address: "0xvictim".into(),
            buy_tx_hashes: vec!["0xbuy1".into(), "0xbuy2".into()],
            buy_amount_eth: 3.5,
            last_buy_amount_eth: Some(2.0),
            buy_before_eth_balance: Some(4.0),
            buy_asset_ratio: Some(0.5),
            is_stuck: true,
            last_buy_tx_hash: "0xbuy2".into(),
        }],
        fraud_trade_stats: BTreeMap::from([(
            "0xhigh".into(),
            FraudTradeStatsPayload {
                unique_buyers: 2,
                eth_priced_sale_count: 2,
                eth_priced_volume: 5.5,
                stuck_wallet_count: 1,
                stuck_cost_eth: 2.0,
            },
        )]),
        ..Default::default()
    };

    let markdown = render_human_readable_report(&payload);

    assert!(markdown.contains("# Top NFT 合约重复样本分析报告"));
    assert!(markdown.contains("## 种子合约"));
    assert!(markdown.contains("- 合约地址: 0xseed"));
    assert!(markdown.contains("- 合约部署者: 0xdeployer"));
    assert!(markdown.contains("## 摘要"));
    assert!(markdown.contains("- 检测到开放许可: 是"));
    assert!(markdown.contains("- 恶意地址数: 4"));
    assert!(markdown.contains("- 候选侧开放许可 token 数: 6"));
    assert!(markdown.contains("- 套牢资金(ETH/WETH): 6.5 / 65.00%"));
    assert!(markdown.contains("- 买入金额占钱包总额 >60% 的地址数/占比: 3 / 60.00%"));
    assert!(markdown.contains("## 种子集合统计"));
    assert!(markdown.contains("- 拉取到的种子 NFT 数: 10"));
    assert!(markdown.contains("## 高置信疑似侵权合约"));
    assert!(markdown.contains("- 0xhigh: 2 个重复 NFT | 命中原因=token_uri_match, name_match"));
    assert!(markdown.contains("## 被算法归为官方参与型重复的合约"));
    assert!(markdown.contains("- 0xlegit: 1 个重复 NFT | mint 接收地址(命中官方地址规则)=0xofficial"));
    assert!(markdown.contains("## 地址行为信号"));
    assert!(markdown.contains("### 0xhigh"));
    assert!(markdown.contains("- 快速扩散: 是"));
    assert!(markdown.contains("## 受害者信号"));
    assert!(markdown.contains("- 套牢地址占比: 33.33%"));
    assert!(markdown.contains("## 诚实地址画像"));
    assert!(markdown.contains(
        "- 0xhigh:0xhonest: interacted_token_count=2 | currently_holding_token_count=1 | hold_duration_median_seconds=44 | 被腐化=是 | honest_sale_to_honest_count=1"
    ));
    assert!(markdown.contains("## 被骗地址画像"));
    assert!(markdown.contains(
        "- 0xvictim: buy_tx_count=2 | 买入金额(ETH/WETH)=3.5 | 最后一次买入金额(ETH/WETH)=2 | 买入前 ETH 余额: 4 | 买入占比=50.00% | 套牢=是 | last_buy_tx=0xbuy2"
    ));
    assert!(markdown.contains("## 被骗交易与套牢资金"));
    assert!(markdown.contains(
        "- 0xhigh: unique_buyers=2 | eth_priced_sale_count=2 | eth_priced_volume=5.5 | stuck_wallet_count=1 | stuck_cost_eth=2"
    ));
}

#[test]
fn single_report_detailed_sections_keep_python_none_rendering_for_missing_leaf_values() {
    let payload = SingleReportPayload {
        honest_address_stats: BTreeMap::from([(
            "0xdup".into(),
            HonestAddressStatsPayload {
                honest_address_count: 1,
                corrupted_address_count: 0,
                honest_to_honest_transfer_count: 0,
                median_holding_seconds: None,
                avg_seconds_to_honest_holder: None,
            },
        )]),
        honest_addresses: vec![HonestAddressPayload {
            contract_address: "0xdup".into(),
            address: "0xhonest".into(),
            interacted_token_count: 1,
            currently_holding_token_count: 0,
            hold_duration_median_seconds: None,
            is_corrupted_address: false,
            honest_sale_to_honest_count: 0,
        }],
        victim_addresses: vec![VictimAddressPayload {
            address: "0xvictim".into(),
            buy_tx_hashes: vec!["0xbuy".into()],
            buy_amount_eth: 1.0,
            last_buy_amount_eth: None,
            buy_before_eth_balance: None,
            buy_asset_ratio: None,
            is_stuck: false,
            last_buy_tx_hash: String::new(),
        }],
        ..Default::default()
    };

    let markdown = render_human_readable_report(&payload);

    assert!(markdown.contains("- 持有时长中位数: None 秒"));
    assert!(markdown.contains("- Mint 到诚实地址平均时间: None 秒"));
    assert!(markdown.contains(
        "- 0xdup:0xhonest: interacted_token_count=1 | currently_holding_token_count=0 | hold_duration_median_seconds=None | 被腐化=否 | honest_sale_to_honest_count=0"
    ));
    assert!(markdown.contains(
        "- 0xvictim: buy_tx_count=1 | 买入金额(ETH/WETH)=1 | 最后一次买入金额(ETH/WETH)=None | 买入前 ETH 余额: None | 买入占比=n/a | 套牢=否 | last_buy_tx=n/a"
    ));
}
