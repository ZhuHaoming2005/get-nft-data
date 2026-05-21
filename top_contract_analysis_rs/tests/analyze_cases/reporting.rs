use super::*;

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

    assert_eq!(
        default_output_basename(&payload),
        "top_contract_analysis__azuki"
    );
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

    assert_eq!(
        default_output_basename(&payload),
        "top_contract_analysis__strasse"
    );
}

#[test]
fn single_report_payload_serializes_current_python_top_level_shape() {
    let payload = SingleReportPayload {
        seed_contract: SeedContractPayload {
            contract_address: "0xseed".into(),
            ..Default::default()
        },
        seed_collection_stats: SeedCollectionStatsPayload {
            seed_nft_count: 1,
            unique_token_uri_count: 1,
            unique_image_uri_count: 1,
            unique_name_count: 1,
            unique_symbol_count: 1,
        },
        duplicate_candidates: vec![top_contract_analysis_rs::models::DuplicateCandidate {
            contract_address: "0xdup".into(),
            token_id: "1".into(),
            confidence: "high".into(),
            ..Default::default()
        }],
        contract_level_summary: BTreeMap::from([(
            "0xdup".into(),
            top_contract_analysis_rs::models::ContractLevelSummaryPayload { candidate_count: 1 },
        )]),
        infringing_tokens: vec![top_contract_analysis_rs::models::InfringingTokenRecord {
            contract_address: "0xdup".into(),
            token_id: "1".into(),
            minter_address: "0xminter".into(),
            ..Default::default()
        }],
        malicious_addresses: vec![MaliciousAddressPayload {
            address: "0xsybil".into(),
            ..Default::default()
        }],
        honest_addresses: vec![HonestAddressPayload {
            contract_address: "0xdup".into(),
            address: "0xholder".into(),
            hold_duration_count: 2,
            is_corrupted_address: true,
            deployment_to_neutral_holder_seconds_samples: vec![15, 30],
            ..Default::default()
        }],
        honest_address_stats: BTreeMap::from([(
            "0xdup".into(),
            HonestAddressStatsPayload {
                honest_address_count: 1,
                corrupted_address_count: 1,
                victim_resale_count: 1,
                avg_deployment_to_neutral_holder_seconds: Some(22.0),
                corrupted_addresses: vec!["0xholder".into()],
                ..Default::default()
            },
        )]),
        fraud_trade_stats: BTreeMap::from([(
            "0xdup".into(),
            FraudTradeStatsPayload {
                native_eth_sale_count: Some(4),
                native_eth_volume: Some(6.25),
                ..Default::default()
            },
        )]),
        ..Default::default()
    };

    let serialized = serde_json::to_value(&payload).unwrap();
    let object = serialized.as_object().unwrap();
    let keys: BTreeSet<_> = object.keys().map(String::as_str).collect();

    assert_eq!(
        keys,
        BTreeSet::from([
            "seed_contract",
            "seed_collection_stats",
            "legit_duplicates",
            "duplicate_contracts",
            "contract_level_summary",
            "address_signals",
            "victim_signals",
            "infringing_tokens",
            "malicious_addresses",
            "neutral_addresses",
            "neutral_address_stats",
            "secondary_sale_victim_addresses",
            "victim_acquisition_addresses",
            "address_evidence_features",
            "contract_lifecycle_events",
            "value_flow_edges",
            "content_similarity_edges",
            "campaign_clusters",
            "lifecycle_metrics",
            "weak_supervision_labels",
            "early_detection_features",
            "market_events",
            "fraud_trade_stats",
            "nft_propagation_paths",
            "report_summary",
        ])
    );
    assert!(!object.contains_key("output_files"));
    assert!(!object.contains_key("duplicate_candidates"));
    assert_eq!(
        serialized["contract_level_summary"]["0xdup"]["candidate_count"],
        1
    );
    assert_eq!(
        serialized["infringing_tokens"][0]["minter_address"],
        "0xminter"
    );
    assert_eq!(serialized["malicious_addresses"][0]["address"], "0xsybil");
    assert_eq!(serialized["neutral_addresses"][0]["hold_duration_count"], 2);
    assert_eq!(
        serialized["neutral_addresses"][0]["deployment_to_neutral_holder_seconds_samples"],
        serde_json::json!([15, 30])
    );
    assert_eq!(
        serialized["neutral_addresses"][0]["is_corrupted_victim"],
        true
    );
    assert_eq!(
        serialized["neutral_address_stats"]["0xdup"]["neutral_address_count"],
        1
    );
    assert_eq!(
        serialized["neutral_address_stats"]["0xdup"]["corrupted_victim_address_count"],
        1
    );
    assert_eq!(
        serialized["neutral_address_stats"]["0xdup"]["avg_deployment_to_neutral_holder_seconds"],
        22.0
    );
    assert_eq!(
        serialized["neutral_address_stats"]["0xdup"]["corrupted_victim_addresses"],
        serde_json::json!(["0xholder"])
    );
    assert!(serialized["honest_addresses"].is_null());
    assert!(serialized["honest_address_stats"].is_null());
    assert_eq!(
        serialized["fraud_trade_stats"]["0xdup"]["native_eth_sale_count"],
        4
    );
    assert_eq!(
        serialized["fraud_trade_stats"]["0xdup"]["native_eth_volume"],
        6.25
    );
}

#[test]
fn single_report_markdown_preserves_summary_sections_only() {
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
            infringing_nft_count: 11,
            malicious_address_count: 4,
            neutral_address_count: 5,
            repeat_infringing_address_count: 2,
            legit_duplicate_contract_count: 1,
            candidate_open_license_token_count: 6,
            candidate_open_license_contract_count: 2,
            secondary_sale_victim_cost_eth: 10.0,
            secondary_sale_victim_cost_usd: 10.0,
            secondary_sale_stuck_cost_eth: 6.5,
            secondary_sale_stuck_cost_usd: 6.5,
            secondary_sale_stuck_cost_ratio: Some(0.65),
            victim_acquisition_total_eth: 10.0,
            victim_acquisition_total_usd: 10.0,
            victim_acquisition_stuck_cost_eth: 6.5,
            victim_acquisition_stuck_cost_usd: 6.5,
            victim_acquisition_stuck_cost_ratio: Some(0.65),
            buy_asset_ratio_known_address_count: 5,
            ratio_over_60_address_count: 3,
            ratio_over_60_address_ratio: Some(0.6),
            ratio_over_80_address_count: 1,
            ratio_over_80_address_ratio: Some(0.2),
            stuck_victim_address_count: 2,
            stuck_victim_address_ratio: Some(0.4),
            corrupted_victim_address_count: 1,
            avg_deployment_to_neutral_holder_seconds: Some(12.5),
            median_deployment_to_neutral_holder_seconds: Some(10.0),
            avg_deployment_to_first_transfer_seconds: Some(8.0),
            median_deployment_to_first_transfer_seconds: Some(7.0),
            avg_unique_receiver_count: Some(4.0),
            ..ReportSummary::default()
        },
        duplicate_contracts: vec![
            DuplicateContractPayload {
                contract_address: "0xhigh".into(),
                candidate_count: 2,
                match_reasons: vec!["token_uri_match".into(), "name_match".into()],
                ..Default::default()
            },
            DuplicateContractPayload {
                contract_address: "0xlow".into(),
                candidate_count: 1,
                match_reasons: vec!["image_uri_match".into()],
                ..Default::default()
            },
        ],
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
                first_transfer_delay_seconds: 8,
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
                victim_resale_count: 3,
                median_holding_seconds: Some(44.0),
                avg_deployment_to_neutral_holder_seconds: Some(12.5),
                corrupted_addresses: vec!["0xhonest".into()],
            },
        )]),
        honest_addresses: vec![HonestAddressPayload {
            contract_address: "0xhigh".into(),
            address: "0xhonest".into(),
            interacted_token_count: 2,
            currently_holding_token_count: 1,
            hold_duration_median_seconds: Some(44.0),
            hold_duration_count: 2,
            is_corrupted_address: true,
            victim_resale_count: 1,
            deployment_to_neutral_holder_seconds_samples: vec![12, 13],
        }],
        secondary_sale_victim_addresses: vec![SecondarySaleVictimAddressPayload {
            contract_address: "0xhigh".into(),
            address: "0xvictim".into(),
            buy_tx_hashes: vec!["0xbuy1".into(), "0xbuy2".into()],
            buy_amount_eth: 3.5,
            buy_amount_usd: 3.5,
            last_buy_amount_eth: Some(2.0),
            last_buy_amount_usd: Some(2.0),
            buy_before_eth_balance: Some(4.0),
            buy_before_usd_balance: Some(4.0),
            buy_asset_ratio: Some(0.5),
            buy_asset_ratio_with_gas: Some(0.55),
            is_stuck: true,
            last_buy_tx_hash: "0xbuy2".into(),
            ratio_status: "ok".into(),
        }],
        fraud_trade_stats: BTreeMap::from([(
            "0xhigh".into(),
            FraudTradeStatsPayload {
                unique_buyers: 2,
                eth_priced_sale_count: Some(2),
                usd_priced_sale_count: Some(2),
                eth_priced_volume: Some(5.5),
                usd_priced_volume: Some(5.5),
                native_eth_sale_count: Some(2),
                native_eth_volume: Some(5.5),
                stuck_wallet_count: 1,
                stuck_cost_eth: 2.0,
                stuck_cost_usd: 2.0,
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
    assert!(markdown.contains("- 疑似操作者地址数: 4"));
    assert!(markdown.contains("- 中性地址数: 5"));
    assert!(markdown.contains("- 受害者数: 0"));
    assert!(markdown.contains("- 候选侧开放许可 token 数: 6"));
    assert!(markdown.contains("- 受害者套牢成本合计(USD): 6.5 / 65.00%"));
    assert!(markdown.contains("- 二级市场受害者成本(USD): 10 / addresses=0"));
    assert!(markdown.contains("- 获取成本占购买前 ETH 余额估算 >60% 的受害者数/占比: 3 / 60.00%"));
    assert!(markdown.contains("- 套牢受害者数/占比: 2 / 40.00%"));
    assert!(markdown.contains("- 被腐化受害者数: 1"));
    assert!(markdown.contains("- 部署到中性地址首次接收平均时间: 12.5 秒"));
    assert!(markdown.contains("## 种子集合统计"));
    assert!(markdown.contains("- 拉取到的种子 NFT 数: 10"));
    assert!(markdown.contains("## 合约分类摘要"));
    assert!(markdown.contains("- 疑似重复合约数: 2"));
    assert!(markdown.contains("- 疑似重复 NFT 数: 3"));
    assert!(markdown.contains("- 命中原因分布: image_uri_match=1, name_match=1, token_uri_match=1"));
    assert!(markdown.contains("- 官方参与型重复合约数: 1"));
    assert!(markdown.contains("- 官方参与型重复 NFT 数: 1"));
    assert!(markdown.contains("- 官方参与型判定原因分布: mint 接收地址命中官方地址规则=1"));
    assert!(markdown.contains("## 资金与交易摘要"));
    assert!(markdown.contains("- 有定价销售记录数: 2"));
    assert!(markdown.contains("- 有定价销售额(USD): 5.5"));
    assert!(markdown.contains("- 唯一买家计数合计: 2"));
    assert!(!markdown.contains("## 地址行为信号"));
    assert!(!markdown.contains("## 受害者信号"));
    assert!(!markdown.contains("## 诚实地址画像"));
    assert!(!markdown.contains("诚实地址数"));
    assert!(!markdown.contains("诚实节点"));
    assert!(!markdown.contains("诚实购买"));
    assert!(!markdown.contains("## 被骗地址画像"));
    assert!(!markdown.contains("## 被骗交易与套牢资金"));
    assert!(!markdown.contains("0xhonest"));
    assert!(!markdown.contains("0xvictim"));
    assert!(!markdown.contains("0xhigh:"));
}

#[test]
fn single_report_markdown_omits_detailed_address_sections() {
    let payload = SingleReportPayload {
        honest_address_stats: BTreeMap::from([(
            "0xdup".into(),
            HonestAddressStatsPayload {
                honest_address_count: 1,
                corrupted_address_count: 0,
                victim_resale_count: 0,
                median_holding_seconds: None,
                avg_deployment_to_neutral_holder_seconds: None,
                corrupted_addresses: vec![],
            },
        )]),
        honest_addresses: vec![HonestAddressPayload {
            contract_address: "0xdup".into(),
            address: "0xhonest".into(),
            interacted_token_count: 1,
            currently_holding_token_count: 0,
            hold_duration_median_seconds: None,
            hold_duration_count: 0,
            is_corrupted_address: false,
            victim_resale_count: 0,
            deployment_to_neutral_holder_seconds_samples: vec![],
        }],
        secondary_sale_victim_addresses: vec![SecondarySaleVictimAddressPayload {
            contract_address: "0xdup".into(),
            address: "0xvictim".into(),
            buy_tx_hashes: vec!["0xbuy".into()],
            buy_amount_eth: 1.0,
            buy_amount_usd: 1.0,
            last_buy_amount_eth: None,
            last_buy_amount_usd: None,
            buy_before_eth_balance: None,
            buy_before_usd_balance: None,
            buy_asset_ratio: None,
            buy_asset_ratio_with_gas: None,
            is_stuck: false,
            last_buy_tx_hash: String::new(),
            ratio_status: "unavailable".into(),
        }],
        ..Default::default()
    };

    let markdown = render_human_readable_report(&payload);

    assert!(markdown.contains("## 摘要"));
    assert!(markdown.contains("## 合约分类摘要"));
    assert!(!markdown.contains("## 诚实地址画像"));
    assert!(!markdown.contains("## 被骗地址画像"));
    assert!(!markdown.contains("0xhonest"));
    assert!(!markdown.contains("0xvictim"));
    assert!(!markdown.contains("buy_tx_count"));
    assert!(!markdown.contains("hold_duration_median_seconds"));
}

#[test]
fn single_report_markdown_omits_victim_address_rows() {
    let payload = SingleReportPayload {
        secondary_sale_victim_addresses: vec![
            SecondarySaleVictimAddressPayload {
                address: "0xvictim".into(),
                buy_tx_hashes: vec!["0xbuy1".into()],
                buy_amount_eth: 0.0,
                buy_amount_usd: 5.0,
                last_buy_amount_eth: Some(0.0),
                last_buy_amount_usd: Some(5.0),
                buy_before_usd_balance: Some(20.0),
                buy_asset_ratio: Some(0.25),
                is_stuck: false,
                last_buy_tx_hash: "0xbuy1".into(),
                ratio_status: "ok".into(),
                ..Default::default()
            },
            SecondarySaleVictimAddressPayload {
                address: "0xvictim".into(),
                buy_tx_hashes: vec!["0xbuy2".into()],
                buy_amount_eth: 0.0,
                buy_amount_usd: 7.0,
                last_buy_amount_eth: Some(0.0),
                last_buy_amount_usd: Some(7.0),
                buy_before_usd_balance: Some(30.0),
                buy_asset_ratio: Some(7.0 / 30.0),
                is_stuck: true,
                last_buy_tx_hash: "0xbuy2".into(),
                ratio_status: "ok".into(),
                ..Default::default()
            },
        ],
        ..Default::default()
    };

    let markdown = render_human_readable_report(&payload);

    assert!(markdown.contains("## 资金与交易摘要"));
    assert_eq!(markdown.matches("0xvictim").count(), 0);
    assert!(!markdown.contains("buy_tx_count"));
    assert!(!markdown.contains("last_buy_tx"));
}

#[test]
fn single_report_fraud_trade_stats_do_not_fall_back_to_native_eth_fields_for_usd_output() {
    let payload = SingleReportPayload {
        fraud_trade_stats: BTreeMap::from([(
            "0xdup".into(),
            FraudTradeStatsPayload {
                unique_buyers: 3,
                native_eth_sale_count: Some(4),
                native_eth_volume: Some(6.25),
                stuck_wallet_count: 2,
                stuck_cost_eth: 1.5,
                stuck_cost_usd: 0.0,
                ..Default::default()
            },
        )]),
        ..Default::default()
    };

    let markdown = render_human_readable_report(&payload);

    assert!(markdown.contains("- 有定价销售记录数: 0"));
    assert!(markdown.contains("- 有定价销售额(USD): 0"));
    assert!(markdown.contains("- 唯一买家计数合计: 3"));
    assert!(!markdown.contains("native_eth_volume"));
    assert!(!markdown.contains("0xdup:"));
}

#[test]
fn single_report_fraud_trade_stats_preserve_explicit_zero_eth_priced_values() {
    let payload = SingleReportPayload {
        fraud_trade_stats: BTreeMap::from([(
            "0xdup".into(),
            FraudTradeStatsPayload {
                unique_buyers: 3,
                eth_priced_sale_count: Some(0),
                usd_priced_sale_count: Some(0),
                eth_priced_volume: Some(0.0),
                usd_priced_volume: Some(0.0),
                native_eth_sale_count: Some(4),
                native_eth_volume: Some(6.25),
                stuck_wallet_count: 2,
                stuck_cost_eth: 1.5,
                stuck_cost_usd: 1.5,
            },
        )]),
        ..Default::default()
    };

    let markdown = render_human_readable_report(&payload);

    assert!(markdown.contains("- 有定价销售记录数: 0"));
    assert!(markdown.contains("- 有定价销售额(USD): 0"));
    assert!(markdown.contains("- 唯一买家计数合计: 3"));
    assert!(!markdown.contains("0xdup:"));
}

#[test]
fn single_report_does_not_display_eth_values_in_usd_fields() {
    let payload = SingleReportPayload {
        secondary_sale_victim_addresses: vec![SecondarySaleVictimAddressPayload {
            address: "0xvictim".into(),
            buy_tx_hashes: vec!["0xbuy".into()],
            buy_amount_eth: 1.25,
            buy_amount_usd: 0.0,
            last_buy_amount_eth: Some(1.25),
            last_buy_amount_usd: None,
            is_stuck: true,
            last_buy_tx_hash: "0xbuy".into(),
            ratio_status: "unavailable".into(),
            ..Default::default()
        }],
        fraud_trade_stats: BTreeMap::from([(
            "0xdup".into(),
            FraudTradeStatsPayload {
                unique_buyers: 1,
                eth_priced_sale_count: Some(1),
                eth_priced_volume: Some(1.25),
                usd_priced_sale_count: Some(0),
                usd_priced_volume: Some(0.0),
                stuck_wallet_count: 1,
                stuck_cost_eth: 1.25,
                stuck_cost_usd: 0.0,
                ..Default::default()
            },
        )]),
        ..Default::default()
    };

    let markdown = render_human_readable_report(&payload);

    assert!(markdown.contains("- 有定价销售记录数: 0"));
    assert!(markdown.contains("- 有定价销售额(USD): 0"));
    assert!(markdown.contains("- 唯一买家计数合计: 1"));
    assert!(!markdown.contains("0xvictim"));
    assert!(!markdown.contains("0xdup:"));
    assert!(!markdown.contains("1.25"));
}
