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
        ..Default::default()
    };

    let serialized = serde_json::to_value(&payload).unwrap();
    let object = serialized.as_object().unwrap();
    let keys: BTreeSet<_> = object.keys().map(String::as_str).collect();

    assert_eq!(
        keys,
        BTreeSet::from(["report_type", "seed_contract", "paper_stats"])
    );
    assert_eq!(serialized["report_type"], "single_seed");
    assert_eq!(serialized["seed_contract"]["contract_address"], "0xseed");
    assert!(serialized["paper_stats"].is_object());
    assert!(!object.contains_key("output_files"));
    assert!(!object.contains_key("duplicate_candidates"));
    assert!(!object.contains_key("report_summary"));
    assert!(!object.contains_key("fraud_trade_stats"));
    assert!(!object.contains_key("neutral_addresses"));
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
            is_stuck: true,
            last_buy_tx_hash: "0xbuy2".into(),
        }],
        ..Default::default()
    };

    let markdown = render_human_readable_report(&payload);

    assert!(markdown.contains("# NFT 论文统计单合约报告"));
    assert!(markdown.contains("## 种子合约"));
    assert!(markdown.contains("- 合约地址: 0xseed"));
    assert!(markdown.contains("- 合约部署者: 0xdeployer"));
    assert!(markdown.contains("## 重复规模"));
    assert!(markdown.contains("## 地址分类"));
    assert!(markdown.contains("## 攻击者成本"));
    assert!(markdown.contains("## 诚实买家损失"));
    assert!(markdown.contains("## 恶意行为汇总"));
    assert!(markdown.contains("## 数据质量"));
    assert!(!markdown.contains("## 摘要"));
    assert!(!markdown.contains("## 种子集合统计"));
    assert!(!markdown.contains("## 合约分类摘要"));
    assert!(!markdown.contains("## 资金与交易摘要"));
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
            is_stuck: false,
            last_buy_tx_hash: String::new(),
        }],
        ..Default::default()
    };

    let markdown = render_human_readable_report(&payload);

    assert!(markdown.contains("## 重复规模"));
    assert!(markdown.contains("## 地址分类"));
    assert!(!markdown.contains("## 摘要"));
    assert!(!markdown.contains("## 合约分类摘要"));
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
                is_stuck: false,
                last_buy_tx_hash: "0xbuy1".into(),
                ..Default::default()
            },
            SecondarySaleVictimAddressPayload {
                address: "0xvictim".into(),
                buy_tx_hashes: vec!["0xbuy2".into()],
                buy_amount_eth: 0.0,
                buy_amount_usd: 7.0,
                last_buy_amount_eth: Some(0.0),
                last_buy_amount_usd: Some(7.0),
                is_stuck: true,
                last_buy_tx_hash: "0xbuy2".into(),
                ..Default::default()
            },
        ],
        ..Default::default()
    };

    let markdown = render_human_readable_report(&payload);

    assert!(markdown.contains("## 诚实买家损失"));
    assert!(!markdown.contains("## 资金与交易摘要"));
    assert_eq!(markdown.matches("0xvictim").count(), 0);
    assert!(!markdown.contains("buy_tx_count"));
    assert!(!markdown.contains("last_buy_tx"));
}

#[test]
fn single_report_data_quality_does_not_fall_back_to_native_eth_fields_for_usd_output() {
    let payload = SingleReportPayload::default();

    let markdown = render_human_readable_report(&payload);

    assert!(markdown.contains("## 数据质量"));
    assert!(markdown.contains("- 可解析销售价格: 0 / 0 (n/a)"));
    assert!(!markdown.contains("- 有定价销售记录数"));
    assert!(!markdown.contains("- 唯一买家计数合计"));
    assert!(!markdown.contains("native_eth_volume"));
    assert!(!markdown.contains("0xdup:"));
}

#[test]
fn single_report_data_quality_preserves_explicit_zero_eth_priced_values() {
    let payload = SingleReportPayload::default();

    let markdown = render_human_readable_report(&payload);

    assert!(markdown.contains("## 数据质量"));
    assert!(markdown.contains("- 可解析销售价格: 0 / 0 (n/a)"));
    assert!(!markdown.contains("- 有定价销售记录数"));
    assert!(!markdown.contains("- 唯一买家计数合计"));
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
            ..Default::default()
        }],
        ..Default::default()
    };

    let markdown = render_human_readable_report(&payload);

    assert!(markdown.contains("## 数据质量"));
    assert!(markdown.contains("- 可解析销售价格: 0 / 0 (n/a)"));
    assert!(!markdown.contains("- 有定价销售记录数"));
    assert!(!markdown.contains("- 唯一买家计数合计"));
    assert!(!markdown.contains("0xvictim"));
    assert!(!markdown.contains("0xdup:"));
    assert!(!markdown.contains("1.25"));
}
