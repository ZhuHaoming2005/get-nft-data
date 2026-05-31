use std::collections::BTreeMap;

use top_contract_analysis_rs::analysis::paper_stats::{
    build_paper_stats, merge_paper_stats, PaperStatsConfig, PaperStatsInput,
};
use top_contract_analysis_rs::models::{
    BatchSummaryPayload, DuplicateCandidate, DuplicateContractPayload, InfringingTokenRecord,
    MaliciousAddressPayload, NftPropagationEdgePayload, NftPropagationNodePayload,
    NftPropagationPathPayload, NftPropagationSummaryPayload, PaperContractBehaviorStatsPayload,
    PaperDataQualityPayload, PaperDuplicateScaleRowPayload, PaperHonestBuyerRowPayload,
    PaperInventoryConcentrationRowPayload, PaperLayeredTransferRowPayload, PaperPumpExitRowPayload,
    PaperStarBehaviorRowPayload, PaperStatsPayload, PaperWashCycleSizeRowPayload,
    PaperWashTradingRowPayload, SeedCollectionStatsPayload, SingleReportPayload,
    ValueFlowEdgePayload, VictimAcquisitionAddressPayload,
};
use top_contract_analysis_rs::reporting::{
    render_batch_human_readable_report, render_human_readable_report,
};

#[test]
fn single_report_serializes_new_paper_stats_contract_without_legacy_summary() {
    let payload = SingleReportPayload::default();
    let json = serde_json::to_value(&payload).unwrap();

    assert_eq!(json["report_type"], "single_seed");
    assert!(json.get("paper_stats").is_some());
    assert!(json.get("report_summary").is_none());
    assert!(json.get("neutral_addresses").is_none());
    assert!(json.get("honest_addresses").is_none());
    assert!(json.get("weak_supervision_labels").is_none());
    assert!(json.get("early_detection_features").is_none());
    assert!(json.get("campaign_clusters").is_none());
}

#[test]
fn batch_report_serializes_new_paper_stats_contract_without_legacy_summary() {
    let payload = BatchSummaryPayload::default();
    let json = serde_json::to_value(&payload).unwrap();

    assert_eq!(json["report_type"], "batch_summary");
    assert!(json.get("paper_stats").is_some());
    assert!(json.get("batch_summary").is_none());
    assert!(json.get("seed_reports").is_none());
}

#[test]
fn markdown_reports_render_paper_stats_without_legacy_sections() {
    let paper_stats = PaperStatsPayload {
        duplicate_scale: vec![PaperDuplicateScaleRowPayload {
            category: "token_uri".into(),
            duplicate_nft_count: 2,
            duplicate_nft_ratio: Some(0.2),
            duplicate_nft_ratio_numerator: 2,
            duplicate_nft_ratio_denominator: 10,
            duplicate_contract_count: 1,
            duplicate_contract_ratio: Some(0.5),
            duplicate_contract_ratio_numerator: 1,
            duplicate_contract_ratio_denominator: 2,
        }],
        ..PaperStatsPayload::default()
    };
    let single = SingleReportPayload {
        paper_stats: paper_stats.clone(),
        ..SingleReportPayload::default()
    };
    let batch = BatchSummaryPayload {
        paper_stats,
        ..BatchSummaryPayload::default()
    };

    let single_markdown = render_human_readable_report(&single);
    let batch_markdown = render_batch_human_readable_report(&batch);

    assert!(single_markdown.contains("## 重复规模"));
    assert!(single_markdown.contains("| token_uri | 2 | 20.00% (2/10) | 1 | 50.00% (1/2) |"));
    assert!(!single_markdown.contains("## Seed 报告索引"));
    assert!(!single_markdown.contains("report_summary"));

    assert!(batch_markdown.contains("# NFT 论文统计汇总报告"));
    assert!(batch_markdown.contains("## 重复规模"));
    assert!(!batch_markdown.contains("## Seed 报告索引"));
    assert!(!batch_markdown.contains("batch_summary"));
}

#[test]
fn duplicate_scale_uses_expanded_infringing_tokens_before_representative_candidates() {
    let seed_stats = SeedCollectionStatsPayload {
        seed_nft_count: 10_000,
        ..SeedCollectionStatsPayload::default()
    };
    let duplicate_candidates = vec![
        DuplicateCandidate {
            contract_address: "0xdup1".into(),
            token_id: "representative-1".into(),
            match_reasons: vec!["token_uri_match".into()],
            ..DuplicateCandidate::default()
        },
        DuplicateCandidate {
            contract_address: "0xdup2".into(),
            token_id: "representative-2".into(),
            match_reasons: vec!["image_uri_match".into()],
            ..DuplicateCandidate::default()
        },
    ];
    let duplicate_contracts = vec![
        DuplicateContractPayload {
            contract_address: "0xdup1".into(),
            candidate_count: 3,
            ..DuplicateContractPayload::default()
        },
        DuplicateContractPayload {
            contract_address: "0xdup2".into(),
            candidate_count: 2,
            ..DuplicateContractPayload::default()
        },
    ];
    let infringing_tokens = vec![
        InfringingTokenRecord {
            contract_address: "0xdup1".into(),
            token_id: "1".into(),
            match_reasons: vec!["token_uri_match".into(), "name_match".into()],
            ..InfringingTokenRecord::default()
        },
        InfringingTokenRecord {
            contract_address: "0xdup1".into(),
            token_id: "2".into(),
            match_reasons: vec!["token_uri_match".into(), "name_match".into()],
            ..InfringingTokenRecord::default()
        },
        InfringingTokenRecord {
            contract_address: "0xdup1".into(),
            token_id: "3".into(),
            match_reasons: vec!["token_uri_match".into()],
            ..InfringingTokenRecord::default()
        },
        InfringingTokenRecord {
            contract_address: "0xdup2".into(),
            token_id: "10".into(),
            match_reasons: vec!["image_uri_match".into()],
            ..InfringingTokenRecord::default()
        },
        InfringingTokenRecord {
            contract_address: "0xdup2".into(),
            token_id: "11".into(),
            match_reasons: vec!["image_uri_match".into()],
            ..InfringingTokenRecord::default()
        },
        InfringingTokenRecord {
            contract_address: "0xlegit".into(),
            token_id: "99".into(),
            match_reasons: vec!["token_uri_match".into()],
            official_or_legit_reissue: true,
            ..InfringingTokenRecord::default()
        },
    ];

    let stats = build_paper_stats(PaperStatsInput {
        config: PaperStatsConfig::default(),
        seed_collection_stats: &seed_stats,
        duplicate_candidates: &duplicate_candidates,
        duplicate_contracts: &duplicate_contracts,
        legit_duplicates: &[],
        infringing_tokens: &infringing_tokens,
        malicious_addresses: &[],
        victim_acquisition_addresses: &[],
        value_flow_edges: &[],
        nft_propagation_paths: &Default::default(),
    });

    let total = stats
        .duplicate_scale
        .iter()
        .find(|row| row.category == "total")
        .unwrap();
    assert_eq!(total.duplicate_nft_count, 5);
    assert_eq!(total.duplicate_nft_ratio, Some(1.0));
    assert_eq!(total.duplicate_nft_ratio_denominator, 5);
    assert_eq!(total.duplicate_contract_count, 2);

    let token_uri = stats
        .duplicate_scale
        .iter()
        .find(|row| row.category == "token_uri")
        .unwrap();
    assert_eq!(token_uri.duplicate_nft_count, 3);
    assert_eq!(token_uri.duplicate_nft_ratio, Some(0.6));
    assert_eq!(token_uri.duplicate_nft_ratio_denominator, 5);
    assert_eq!(token_uri.duplicate_contract_count, 1);

    let quality = &stats.data_quality;
    assert_eq!(quality.representative_candidate_count, 2);
    assert_eq!(quality.candidate_contract_count, 2);
    assert_eq!(quality.suspected_duplicate_contract_count, 2);
    assert_eq!(quality.infringing_nft_count, 5);
}

#[test]
fn data_quality_duplicate_counts_match_duplicate_scale_when_using_candidate_fallback() {
    let duplicate_candidates = vec![
        DuplicateCandidate {
            contract_address: "0xdup1".into(),
            token_id: "1".into(),
            match_reasons: vec!["token_uri_match".into()],
            ..DuplicateCandidate::default()
        },
        DuplicateCandidate {
            contract_address: "0xdup2".into(),
            token_id: "2".into(),
            match_reasons: vec!["image_uri_match".into()],
            ..DuplicateCandidate::default()
        },
    ];
    let duplicate_contracts = vec![
        DuplicateContractPayload {
            contract_address: "0xdup1".into(),
            candidate_count: 1,
            ..DuplicateContractPayload::default()
        },
        DuplicateContractPayload {
            contract_address: "0xdup2".into(),
            candidate_count: 1,
            ..DuplicateContractPayload::default()
        },
    ];

    let stats = build_paper_stats(PaperStatsInput {
        config: PaperStatsConfig::default(),
        seed_collection_stats: &SeedCollectionStatsPayload {
            seed_nft_count: 10,
            ..SeedCollectionStatsPayload::default()
        },
        duplicate_candidates: &duplicate_candidates,
        duplicate_contracts: &duplicate_contracts,
        legit_duplicates: &[],
        infringing_tokens: &[],
        malicious_addresses: &[],
        victim_acquisition_addresses: &[],
        value_flow_edges: &[],
        nft_propagation_paths: &Default::default(),
    });

    let total = stats
        .duplicate_scale
        .iter()
        .find(|row| row.category == "total")
        .unwrap();
    assert_eq!(total.duplicate_nft_count, 2);
    assert_eq!(total.duplicate_contract_count, 2);
    assert_eq!(
        stats.data_quality.infringing_nft_count,
        total.duplicate_nft_count
    );
    assert_eq!(
        stats.data_quality.suspected_duplicate_contract_count,
        total.duplicate_contract_count
    );
}

#[test]
fn markdown_reports_use_contract_address_without_extra_chain_identifiers() {
    let paper_stats = PaperStatsPayload {
        contract_behavior_stats: vec![PaperContractBehaviorStatsPayload {
            contract_address: "0xcopy".into(),
            wash_trading: vec![PaperWashTradingRowPayload {
                cycle_id: "0xcopy:wash:1".into(),
                participant_node_count: 2,
                fake_volume_usd: 10.0,
                ..PaperWashTradingRowPayload::default()
            }],
            honest_buyers: vec![PaperHonestBuyerRowPayload {
                honest_buyer: "0xbuyer".into(),
                total_paid_usd: 10.0,
                source_pattern: "Pump-and-Exit".into(),
                still_holding: true,
                holding_seconds: Some(86_400),
                ..PaperHonestBuyerRowPayload::default()
            }],
            ..PaperContractBehaviorStatsPayload::default()
        }],
        ..PaperStatsPayload::default()
    };
    let batch_markdown = render_batch_human_readable_report(&BatchSummaryPayload {
        paper_stats: paper_stats.clone(),
        ..BatchSummaryPayload::default()
    });
    let single_markdown = render_human_readable_report(&SingleReportPayload {
        paper_stats,
        ..SingleReportPayload::default()
    });

    assert!(!batch_markdown.contains("## 合约行为明细"));
    assert!(!batch_markdown.contains("contract_address"));
    assert!(!batch_markdown.contains("0xbuyer"));
    assert!(single_markdown.contains("## 合约行为明细"));
    assert!(single_markdown.contains("### Match 合约 0xcopy"));
    assert!(single_markdown.contains("#### Wash Trading"));
    assert!(single_markdown.contains("| 0xcopy:wash:1 | 2 | n/a | n/a | 0 / 10 |"));
    assert!(single_markdown.contains("#### 诚实买家"));
    assert!(
        single_markdown.contains("| 0xbuyer | 0 | 0 / 10 | Pump-and-Exit | n/a | 是 | 86400s |")
    );
    assert!(!batch_markdown.contains("Impact USD"));
    assert!(!single_markdown.contains("### 诚实买家 Top"));
    assert!(!batch_markdown.contains("chain_id"));
    assert!(!batch_markdown.contains("seed_contract_address"));
    assert!(!batch_markdown.contains("copy_contract_address"));
    assert!(!single_markdown.contains("chain_id"));
    assert!(!single_markdown.contains("seed_contract_address"));
    assert!(!single_markdown.contains("copy_contract_address"));
}

#[test]
fn single_markdown_lists_attributed_honest_buyers_per_match_contract() {
    let paper_stats = PaperStatsPayload {
        contract_behavior_stats: vec![PaperContractBehaviorStatsPayload {
            contract_address: "0xcopy".into(),
            honest_buyers: vec![
                PaperHonestBuyerRowPayload {
                    honest_buyer: "0xunattributed".into(),
                    total_paid_usd: 100.0,
                    source_pattern: "unattributed_sale".into(),
                    still_holding: true,
                    ..PaperHonestBuyerRowPayload::default()
                },
                PaperHonestBuyerRowPayload {
                    honest_buyer: "0xlinked".into(),
                    total_paid_usd: 50.0,
                    source_pattern: "Pump-and-Exit".into(),
                    still_holding: true,
                    ..PaperHonestBuyerRowPayload::default()
                },
            ],
            ..PaperContractBehaviorStatsPayload::default()
        }],
        ..PaperStatsPayload::default()
    };

    let markdown = render_human_readable_report(&SingleReportPayload {
        paper_stats,
        ..SingleReportPayload::default()
    });

    assert!(markdown.contains("## 合约行为明细"));
    assert!(markdown.contains("### Match 合约 0xcopy"));
    assert!(markdown.contains("#### 诚实买家"));
    assert!(markdown.contains("| 0xlinked | 0 | 0 / 50 | Pump-and-Exit | n/a | 是 | n/a |"));
    assert!(markdown.contains("0xlinked"));
    assert!(!markdown.contains("0xunattributed"));
    assert!(!markdown.contains("unattributed_sale"));
}

#[test]
fn single_markdown_truncates_contract_address_labels_in_details() {
    let long_contract = "0x1234567890abcdef1234567890abcdef12345678";
    let paper_stats = PaperStatsPayload {
        contract_behavior_stats: vec![PaperContractBehaviorStatsPayload {
            contract_address: long_contract.into(),
            wash_trading: vec![PaperWashTradingRowPayload {
                fake_volume_usd: 10.0,
                ..PaperWashTradingRowPayload::default()
            }],
            ..PaperContractBehaviorStatsPayload::default()
        }],
        ..PaperStatsPayload::default()
    };

    let markdown = render_human_readable_report(&SingleReportPayload {
        paper_stats,
        ..SingleReportPayload::default()
    });

    assert!(markdown.contains("0x1234...345678"));
    assert!(!markdown.contains(long_contract));
}

#[test]
fn honest_loss_ratio_uses_all_fake_nfts_as_denominator() {
    let infringing_tokens = (1..=5)
        .map(|token_id| InfringingTokenRecord {
            contract_address: "0xdup".into(),
            token_id: token_id.to_string(),
            match_reasons: vec!["token_uri_match".into()],
            ..InfringingTokenRecord::default()
        })
        .collect::<Vec<_>>();
    let victim = VictimAcquisitionAddressPayload {
        address: "0xhonest".into(),
        contract_addresses: vec!["0xdup".into()],
        secondary_sale_count: 2,
        secondary_sale_stuck_cost_usd: 20.0,
        total_stuck_cost_usd: 20.0,
        is_stuck: true,
        ..VictimAcquisitionAddressPayload::default()
    };

    let stats = build_paper_stats(PaperStatsInput {
        config: PaperStatsConfig::default(),
        seed_collection_stats: &SeedCollectionStatsPayload::default(),
        duplicate_candidates: &[],
        duplicate_contracts: &[],
        legit_duplicates: &[],
        infringing_tokens: &infringing_tokens,
        malicious_addresses: &[],
        victim_acquisition_addresses: &[victim],
        value_flow_edges: &[],
        nft_propagation_paths: &Default::default(),
    });

    assert_eq!(stats.honest_loss.stuck_nft_ratio_numerator, 2);
    assert_eq!(stats.honest_loss.stuck_nft_ratio_denominator, 5);
    assert_eq!(stats.honest_loss.stuck_nft_ratio, Some(0.4));
}

#[test]
fn behavior_summary_only_counts_loss_when_buyers_are_linked() {
    let path = NftPropagationPathPayload {
        contract_address: "0xfraud".into(),
        edges: vec![
            propagation_edge(
                ("0xfraud", "0xcenter", "0xleaf1"),
                "1",
                "sale",
                100,
                (Some(0.01), Some(10.0)),
            ),
            propagation_edge(
                ("0xfraud", "0xcenter", "0xleaf2"),
                "2",
                "sale",
                101,
                (Some(0.02), Some(20.0)),
            ),
        ],
        ..NftPropagationPathPayload::default()
    };

    let stats = build_paper_stats(PaperStatsInput {
        config: PaperStatsConfig {
            center_fanout_threshold: 2,
            ..PaperStatsConfig::default()
        },
        seed_collection_stats: &SeedCollectionStatsPayload::default(),
        duplicate_candidates: &[],
        duplicate_contracts: &[],
        legit_duplicates: &[],
        infringing_tokens: &[],
        malicious_addresses: &[],
        victim_acquisition_addresses: &[],
        value_flow_edges: &[],
        nft_propagation_paths: &BTreeMap::from([("0xfraud".into(), path)]),
    });

    let fraud = stats
        .malicious_behavior_summary
        .iter()
        .find(|row| row.behavior_type == "Fraud Revenue")
        .unwrap();
    assert_eq!(fraud.linked_buyer_count, 0);
    assert_eq!(fraud.linked_loss_usd, 0.0);
    assert_eq!(
        stats.contract_behavior_stats[0].star_behaviors[0].total_value_usd,
        30.0
    );
}

#[test]
fn markdown_honest_loss_renders_stuck_time_as_multiple() {
    let paper_stats = PaperStatsPayload {
        honest_loss: top_contract_analysis_rs::models::PaperHonestLossPayload {
            stuck_nft_count: 2,
            stuck_nft_ratio: Some(0.25),
            stuck_nft_ratio_numerator: 2,
            stuck_nft_ratio_denominator: 8,
            stuck_time_ratio: Some(2.5),
            stuck_time_ratio_numerator: 50.0,
            stuck_time_ratio_denominator: 20.0,
            total_loss_usd: 10.0,
            top_contract_loss_contribution_ratio: Some(1.0),
            top_contract_loss_contribution_numerator: 10.0,
            top_contract_loss_contribution_denominator: 10.0,
            ..top_contract_analysis_rs::models::PaperHonestLossPayload::default()
        },
        ..PaperStatsPayload::default()
    };

    let markdown = render_human_readable_report(&SingleReportPayload {
        paper_stats,
        ..SingleReportPayload::default()
    });

    assert!(markdown.contains("套牢时间倍数"));
    assert!(markdown.contains("| 2 | 25.00% (2/8) | 2.5x (50/20) |"));
    assert!(!markdown.contains("250.00% (50/20)"));
}

#[test]
fn honest_loss_exports_single_total_object_and_markdown_row_without_category() {
    let victim = VictimAcquisitionAddressPayload {
        address: "0xhonest".into(),
        contract_addresses: vec!["0xdup".into()],
        secondary_sale_count: 2,
        secondary_sale_stuck_cost_eth: 0.03,
        secondary_sale_stuck_cost_usd: 30.0,
        paid_mint_edge_count: 3,
        paid_mint_token_count: 3,
        paid_mint_stuck_token_count: 2,
        paid_mint_stuck_cost_eth: 0.04,
        paid_mint_stuck_cost_usd: 40.0,
        total_stuck_cost_eth: 0.07,
        total_stuck_cost_usd: 70.0,
        is_stuck: true,
        ..VictimAcquisitionAddressPayload::default()
    };
    let stats = build_paper_stats(PaperStatsInput {
        config: PaperStatsConfig::default(),
        seed_collection_stats: &SeedCollectionStatsPayload::default(),
        duplicate_candidates: &[],
        duplicate_contracts: &[],
        legit_duplicates: &[],
        infringing_tokens: &[],
        malicious_addresses: &[],
        victim_acquisition_addresses: &[victim],
        value_flow_edges: &[],
        nft_propagation_paths: &Default::default(),
    });

    let json = serde_json::to_value(&stats).unwrap();
    let honest_loss = json
        .get("honest_loss")
        .expect("honest_loss should be exported");
    assert!(honest_loss.is_object());
    assert!(honest_loss.get("category").is_none());
    assert_eq!(honest_loss["stuck_nft_count"], 4);
    assert_eq!(honest_loss["stuck_nft_ratio_numerator"], 4);
    assert_eq!(honest_loss["stuck_nft_ratio_denominator"], 5);
    assert_eq!(honest_loss["secondary_sale_loss_usd"], 30.0);
    assert_eq!(honest_loss["paid_mint_loss_usd"], 40.0);
    assert_eq!(honest_loss["total_loss_usd"], 70.0);

    let markdown = render_human_readable_report(&SingleReportPayload {
        paper_stats: stats,
        ..SingleReportPayload::default()
    });
    assert!(markdown.contains(
        "| 套牢 NFT | NFT 套牢占比 | 套牢时间倍数 | 二级市场损失 ETH/USD | 付费 mint 损失 ETH/USD | 总损失 ETH/USD | 损失集中度 |"
    ));
    assert!(!markdown.contains("| 类别 | 套牢 NFT |"));
    assert!(markdown.contains(
        "| 4 | 80.00% (4/5) | n/a (0/0) | 0.03 / 30 | 0.04 / 40 | 0.07 / 70 | 100.00% (70/70) |"
    ));
}

#[test]
fn markdown_summary_tables_include_experiment_fields() {
    let stats = PaperStatsPayload {
        address_classification:
            top_contract_analysis_rs::models::PaperAddressClassificationPayload {
                malicious_address_count: 3,
                repeat_infringing_malicious_address_count: 1,
                honest_address_count: 2,
                total_address_count: 5,
            },
        attacker_cost: top_contract_analysis_rs::models::PaperAttackerCostPayload {
            setup_gas_eth: 0.01,
            setup_gas_usd: 20.0,
            lure_gas_eth: 0.02,
            lure_gas_usd: 40.0,
            exit_gas_eth: 0.03,
            exit_gas_usd: 60.0,
            total_gas_eth: 0.06,
            total_gas_usd: 120.0,
            top_contract_contribution_ratio: Some(0.25),
            top_contract_contribution_numerator: 30.0,
            top_contract_contribution_denominator: 120.0,
        },
        honest_loss: top_contract_analysis_rs::models::PaperHonestLossPayload {
            stuck_nft_count: 4,
            stuck_nft_ratio: Some(0.8),
            stuck_nft_ratio_numerator: 4,
            stuck_nft_ratio_denominator: 5,
            stuck_time_ratio: Some(2.0),
            stuck_time_ratio_numerator: 20.0,
            stuck_time_ratio_denominator: 10.0,
            secondary_sale_loss_eth: 0.03,
            secondary_sale_loss_usd: 30.0,
            paid_mint_loss_eth: 0.04,
            paid_mint_loss_usd: 40.0,
            total_loss_eth: 0.07,
            total_loss_usd: 70.0,
            top_contract_loss_contribution_ratio: Some(0.5),
            top_contract_loss_contribution_numerator: 35.0,
            top_contract_loss_contribution_denominator: 70.0,
        },
        ..PaperStatsPayload::default()
    };

    let markdown = render_human_readable_report(&SingleReportPayload {
        paper_stats: stats,
        ..SingleReportPayload::default()
    });

    assert!(markdown.contains("| 类别 | 恶意地址数量 | 多次侵权地址数 | 诚实地址数量 | 地址总数 |"));
    assert!(markdown.contains("| all | 3 | 1 | 2 | 5 |"));
    assert!(markdown.contains(
        "| cost | Setup Gas ETH/USD | Lure Gas ETH/USD | Exit Gas ETH/USD | Total Gas ETH/USD | 攻击投入集中度 |"
    ));
    assert!(markdown
        .contains("| gas | 0.01 / 20 | 0.02 / 40 | 0.03 / 60 | 0.06 / 120 | 25.00% (30/120) |"));
    assert!(markdown.contains(
        "| 套牢 NFT | NFT 套牢占比 | 套牢时间倍数 | 二级市场损失 ETH/USD | 付费 mint 损失 ETH/USD | 总损失 ETH/USD | 损失集中度 |"
    ));
    assert!(markdown.contains(
        "| 4 | 80.00% (4/5) | 2x (20/10) | 0.03 / 30 | 0.04 / 40 | 0.07 / 70 | 50.00% (35/70) |"
    ));
}

#[test]
fn paper_stats_summarizes_wash_cycle_node_size_distribution() {
    let contract = "0xcyclesizes";
    let empty_contract = "0xemptycycles";
    let mut edges = Vec::new();
    edges.extend(cycle_edges(contract, "2", &["0x2a", "0x2b"], 100));
    edges.extend(cycle_edges(contract, "3", &["0x3a", "0x3b", "0x3c"], 200));
    edges.extend(cycle_edges(
        contract,
        "4",
        &["0x4a", "0x4b", "0x4c", "0x4d"],
        300,
    ));
    edges.extend(cycle_edges(
        contract,
        "5",
        &["0x5a", "0x5b", "0x5c", "0x5d", "0x5e"],
        400,
    ));
    edges.extend(cycle_edges(
        contract,
        "6",
        &["0x6a", "0x6b", "0x6c", "0x6d", "0x6e", "0x6f"],
        500,
    ));
    let path = NftPropagationPathPayload {
        contract_address: contract.into(),
        edges,
        ..NftPropagationPathPayload::default()
    };

    let stats = build_paper_stats(PaperStatsInput {
        config: PaperStatsConfig::default(),
        seed_collection_stats: &SeedCollectionStatsPayload::default(),
        duplicate_candidates: &[],
        duplicate_contracts: &[
            DuplicateContractPayload {
                contract_address: contract.into(),
                ..DuplicateContractPayload::default()
            },
            DuplicateContractPayload {
                contract_address: empty_contract.into(),
                ..DuplicateContractPayload::default()
            },
        ],
        legit_duplicates: &[],
        infringing_tokens: &[],
        malicious_addresses: &[],
        victim_acquisition_addresses: &[],
        value_flow_edges: &[],
        nft_propagation_paths: &BTreeMap::from([(contract.into(), path)]),
    });

    let rows = stats
        .wash_cycle_size_distribution
        .iter()
        .map(|row| {
            (
                row.node_count_bucket.as_str(),
                row.cycle_count,
                row.cycle_ratio,
                row.cycle_ratio_numerator,
                row.cycle_ratio_denominator,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        vec![
            ("2", 1, Some(0.2), 1, 5),
            ("3", 1, Some(0.2), 1, 5),
            ("4", 1, Some(0.2), 1, 5),
            ("5+", 2, Some(0.4), 2, 5),
        ]
    );
    let contract_rows = &stats.contract_behavior_stats[0].wash_cycle_size_distribution;
    assert_eq!(&stats.wash_cycle_size_distribution, contract_rows);
    assert_eq!(stats.wash_cycle_size_by_contract.len(), 2);
    let empty_contract_row = stats
        .wash_cycle_size_by_contract
        .iter()
        .find(|row| row.contract_address == empty_contract)
        .unwrap();
    assert_eq!(empty_contract_row.distribution[0].cycle_count, 0);
    assert_eq!(
        empty_contract_row.distribution[0].cycle_ratio_denominator,
        0
    );

    let markdown = render_human_readable_report(&SingleReportPayload {
        paper_stats: stats,
        ..SingleReportPayload::default()
    });
    assert!(markdown.contains("## Wash Cycle 节点规模"));
    assert!(markdown.contains("| 节点数 | 循环数 | 循环占比 |"));
    assert!(markdown.contains("| 5+ | 2 | 40.00% (2/5) |"));
    assert!(markdown.contains("### Match 合约 0xcyclesizes"));
    assert!(markdown.contains("#### Wash Cycle 节点规模"));
    assert!(markdown.contains("| 2 | 1 | 20.00% (1/5) |"));
    assert!(markdown.contains("| 5+ | 2 | 40.00% (2/5) |"));
    assert!(markdown.contains("### Match 合约 0xemptycycles"));
    assert!(markdown.contains("- 无可展示行为明细"));
}

#[test]
fn batch_markdown_renders_wash_cycle_size_distribution_summary() {
    let stats = PaperStatsPayload {
        wash_cycle_size_distribution: vec![
            PaperWashCycleSizeRowPayload {
                node_count_bucket: "2".into(),
                cycle_count: 3,
                cycle_ratio: Some(0.75),
                cycle_ratio_numerator: 3,
                cycle_ratio_denominator: 4,
            },
            PaperWashCycleSizeRowPayload {
                node_count_bucket: "5+".into(),
                cycle_count: 1,
                cycle_ratio: Some(0.25),
                cycle_ratio_numerator: 1,
                cycle_ratio_denominator: 4,
            },
        ],
        ..PaperStatsPayload::default()
    };

    let markdown = render_batch_human_readable_report(&BatchSummaryPayload {
        paper_stats: stats,
        ..BatchSummaryPayload::default()
    });

    assert!(markdown.contains("## Wash Cycle 节点规模"));
    assert!(markdown.contains("| 2 | 3 | 75.00% (3/4) |"));
    assert!(markdown.contains("| 5+ | 1 | 25.00% (1/4) |"));
    assert!(!markdown.contains("## 合约行为明细"));
}

#[test]
fn attacker_cost_excludes_honest_buyer_mint_gas_and_ignored_channels() {
    let victim = VictimAcquisitionAddressPayload {
        address: "0xhonest".into(),
        is_stuck: true,
        ..VictimAcquisitionAddressPayload::default()
    };
    let value_flow_edges = vec![
        ValueFlowEdgePayload {
            contract_address: "0xdup".into(),
            from_address: "0xhonest".into(),
            channel: "mint_payment".into(),
            value_usd: Some(100.0),
            value_with_gas_usd: Some(600.0),
            value_eth: Some(0.05),
            value_with_gas_eth: Some(0.06),
            ..ValueFlowEdgePayload::default()
        },
        ValueFlowEdgePayload {
            contract_address: "0xignored".into(),
            from_address: "0xhonest".into(),
            channel: "unrelated_transfer".into(),
            value_usd: Some(0.0),
            value_with_gas_usd: Some(1_000.0),
            value_eth: Some(0.0),
            value_with_gas_eth: Some(1.0),
            ..ValueFlowEdgePayload::default()
        },
        ValueFlowEdgePayload {
            contract_address: "0xdup".into(),
            from_address: "0xdeployer".into(),
            channel: "contract_deploy".into(),
            from_role: "contract_deployer".into(),
            value_usd: Some(0.0),
            value_with_gas_usd: Some(10.0),
            value_eth: Some(0.0),
            value_with_gas_eth: Some(0.01),
            ..ValueFlowEdgePayload::default()
        },
    ];

    let stats = build_paper_stats(PaperStatsInput {
        config: PaperStatsConfig::default(),
        seed_collection_stats: &SeedCollectionStatsPayload::default(),
        duplicate_candidates: &[],
        duplicate_contracts: &[],
        legit_duplicates: &[],
        infringing_tokens: &[],
        malicious_addresses: &[],
        victim_acquisition_addresses: &[victim],
        value_flow_edges: &value_flow_edges,
        nft_propagation_paths: &Default::default(),
    });

    assert_eq!(stats.attacker_cost.setup_gas_usd, 10.0);
    assert_eq!(stats.attacker_cost.setup_gas_eth, 0.01);
    assert_eq!(stats.attacker_cost.total_gas_usd, 10.0);
    assert_eq!(
        stats.attacker_cost.top_contract_contribution_numerator,
        10.0
    );
    assert_eq!(
        stats.attacker_cost.top_contract_contribution_denominator,
        10.0
    );
    assert_eq!(
        stats.attacker_cost.top_contract_contribution_ratio,
        Some(1.0)
    );
}

#[test]
fn concentration_uses_all_suspected_duplicate_contracts() {
    let config = PaperStatsConfig {
        concentration_top_pct: 0.5,
        ..PaperStatsConfig::default()
    };
    let duplicate_contracts = (1..=6)
        .map(|index| DuplicateContractPayload {
            contract_address: format!("0xdup{index}"),
            candidate_count: 1,
            ..DuplicateContractPayload::default()
        })
        .collect::<Vec<_>>();
    let value_flow_edges = [
        ("0xdup1", 60.0),
        ("0xdup2", 30.0),
        ("0xdup3", 10.0),
        ("0xdup4", 1.0),
    ]
    .into_iter()
    .map(|(contract, gas_usd)| ValueFlowEdgePayload {
        contract_address: contract.into(),
        from_address: format!("{contract}deployer"),
        channel: "contract_deploy".into(),
        from_role: "contract_deployer".into(),
        gas_usd: Some(gas_usd),
        ..ValueFlowEdgePayload::default()
    })
    .collect::<Vec<_>>();
    let victims = [
        ("0xdup1", "0xbuyer1", 60.0),
        ("0xdup2", "0xbuyer2", 30.0),
        ("0xdup3", "0xbuyer3", 10.0),
        ("0xdup4", "0xbuyer4", 1.0),
    ]
    .into_iter()
    .map(
        |(contract, buyer, loss_usd)| VictimAcquisitionAddressPayload {
            address: buyer.into(),
            contract_addresses: vec![contract.into()],
            secondary_sale_count: 1,
            secondary_sale_stuck_cost_usd: loss_usd,
            total_stuck_cost_usd: loss_usd,
            is_stuck: true,
            ..VictimAcquisitionAddressPayload::default()
        },
    )
    .collect::<Vec<_>>();

    let stats = build_paper_stats(PaperStatsInput {
        config,
        seed_collection_stats: &SeedCollectionStatsPayload::default(),
        duplicate_candidates: &[],
        duplicate_contracts: &duplicate_contracts,
        legit_duplicates: &[],
        infringing_tokens: &[],
        malicious_addresses: &[],
        victim_acquisition_addresses: &victims,
        value_flow_edges: &value_flow_edges,
        nft_propagation_paths: &Default::default(),
    });

    assert_eq!(
        stats.attacker_cost.top_contract_contribution_numerator,
        100.0
    );
    assert_eq!(
        stats.honest_loss.top_contract_loss_contribution_numerator,
        100.0
    );
}

#[test]
fn malicious_mint_payment_gas_counts_as_lure_not_setup() {
    let value_flow_edges = vec![
        ValueFlowEdgePayload {
            contract_address: "0xdup".into(),
            from_address: "0xdeployer".into(),
            tx_hash: "0xdeploy".into(),
            channel: "contract_deploy".into(),
            from_role: "contract_deployer".into(),
            gas_usd: Some(3.0),
            ..ValueFlowEdgePayload::default()
        },
        ValueFlowEdgePayload {
            contract_address: "0xdup".into(),
            from_address: "0xminter".into(),
            tx_hash: "0xmint".into(),
            channel: "mint_payment".into(),
            from_role: "paid_minter".into(),
            gas_usd: Some(5.0),
            ..ValueFlowEdgePayload::default()
        },
    ];

    let stats = build_paper_stats(PaperStatsInput {
        config: PaperStatsConfig::default(),
        seed_collection_stats: &SeedCollectionStatsPayload::default(),
        duplicate_candidates: &[],
        duplicate_contracts: &[],
        legit_duplicates: &[],
        infringing_tokens: &[],
        malicious_addresses: &[MaliciousAddressPayload {
            address: "0xminter".into(),
            ..MaliciousAddressPayload::default()
        }],
        victim_acquisition_addresses: &[],
        value_flow_edges: &value_flow_edges,
        nft_propagation_paths: &Default::default(),
    });

    assert_eq!(stats.attacker_cost.setup_gas_usd, 3.0);
    assert_eq!(stats.attacker_cost.lure_gas_usd, 5.0);
    let mint_detail = stats
        .attacker_cost_details
        .iter()
        .find(|detail| detail.tx_hash == "0xmint")
        .unwrap();
    assert_eq!(mint_detail.stage, "lure");
}

#[test]
fn wash_trading_avg_cycle_blocks_uses_block_numbers_not_timestamps() {
    let path = NftPropagationPathPayload {
        contract_address: "0xdup".into(),
        summary: NftPropagationSummaryPayload {
            token_count: 1,
            first_block_time: 1_000,
            ..NftPropagationSummaryPayload::default()
        },
        edges: vec![
            NftPropagationEdgePayload {
                edge_id: "0xdup:0xa:0xb:1".into(),
                contract_address: "0xdup".into(),
                token_id: "1".into(),
                from_address: "0xa".into(),
                to_address: "0xb".into(),
                tx_hash: "0xcycle1".into(),
                block_number: 10,
                block_time: 1_000,
                channel: "sale".into(),
                price_eth: Some(1.0),
                price_usd: Some(2_000.0),
                aggregate_count: 1,
                token_ids: vec!["1".into()],
                ..NftPropagationEdgePayload::default()
            },
            NftPropagationEdgePayload {
                edge_id: "0xdup:0xb:0xa:1".into(),
                contract_address: "0xdup".into(),
                token_id: "1".into(),
                from_address: "0xb".into(),
                to_address: "0xa".into(),
                tx_hash: "0xcycle2".into(),
                block_number: 12,
                block_time: 4_600,
                channel: "sale".into(),
                price_eth: Some(1.5),
                price_usd: Some(3_000.0),
                aggregate_count: 1,
                token_ids: vec!["1".into()],
                ..NftPropagationEdgePayload::default()
            },
        ],
        ..NftPropagationPathPayload::default()
    };

    let stats = build_paper_stats(PaperStatsInput {
        config: PaperStatsConfig::default(),
        seed_collection_stats: &SeedCollectionStatsPayload::default(),
        duplicate_candidates: &[],
        duplicate_contracts: &[],
        legit_duplicates: &[],
        infringing_tokens: &[],
        malicious_addresses: &[],
        victim_acquisition_addresses: &[],
        value_flow_edges: &[],
        nft_propagation_paths: &BTreeMap::from([("0xdup".into(), path)]),
    });

    let wash = &stats.contract_behavior_stats[0].wash_trading[0];
    assert_eq!(wash.avg_cycle_blocks, Some(2.0));
}

#[test]
fn ratio_like_fields_export_reproducible_numerators_and_denominators() {
    let path = NftPropagationPathPayload {
        contract_address: "0xdup".into(),
        summary: NftPropagationSummaryPayload {
            token_count: 1,
            first_block_time: 100,
            ..NftPropagationSummaryPayload::default()
        },
        edges: vec![
            propagation_edge(
                ("0xdup", "0xa", "0xb"),
                "1",
                "sale",
                100,
                (Some(1.0), Some(100.0)),
            ),
            propagation_edge(
                ("0xdup", "0xb", "0xa"),
                "1",
                "sale",
                110,
                (Some(1.0), Some(100.0)),
            ),
            propagation_edge(
                ("0xdup", "0xa", "0xhonest"),
                "1",
                "sale",
                130,
                (Some(3.0), Some(300.0)),
            ),
            propagation_edge(("0xdup", "0xa", "0xpeer"), "1", "sale", 140, (None, None)),
        ],
        ..NftPropagationPathPayload::default()
    };
    let victim = VictimAcquisitionAddressPayload {
        address: "0xhonest".into(),
        contract_addresses: vec!["0xdup".into()],
        secondary_sale_count: 1,
        secondary_sale_stuck_cost_eth: 3.0,
        secondary_sale_stuck_cost_usd: 300.0,
        total_stuck_cost_eth: 3.0,
        total_stuck_cost_usd: 300.0,
        is_stuck: true,
        ..VictimAcquisitionAddressPayload::default()
    };

    let stats = build_paper_stats(PaperStatsInput {
        config: PaperStatsConfig::default(),
        seed_collection_stats: &SeedCollectionStatsPayload::default(),
        duplicate_candidates: &[],
        duplicate_contracts: &[],
        legit_duplicates: &[],
        infringing_tokens: &[],
        malicious_addresses: &[],
        victim_acquisition_addresses: &[victim],
        value_flow_edges: &[],
        nft_propagation_paths: &BTreeMap::from([("0xdup".into(), path)]),
    });
    let json = serde_json::to_value(&stats).unwrap();
    let pump = &json["contract_behavior_stats"][0]["pump_and_exit"][0];
    assert_eq!(pump["exit_price_premium"], 3.0);
    assert_eq!(pump["exit_price_premium_numerator"], 300.0);
    assert_eq!(pump["exit_price_premium_denominator"], 100.0);
    assert_eq!(json["data_quality"]["sale_price_parseable_ratio"], 0.75);
    assert_eq!(
        json["data_quality"]["sale_price_parseable_ratio_numerator"],
        3
    );
    assert_eq!(
        json["data_quality"]["sale_price_parseable_ratio_denominator"],
        4
    );
}

#[test]
fn attacker_cost_counts_all_stages_from_malicious_gas_and_deduplicates_transactions() {
    let victim = VictimAcquisitionAddressPayload {
        address: "0xhonest".into(),
        is_stuck: true,
        ..VictimAcquisitionAddressPayload::default()
    };
    let value_flow_edges = vec![
        ValueFlowEdgePayload {
            contract_address: "0xdup".into(),
            from_address: "0xfunder".into(),
            tx_hash: "0xsetup".into(),
            channel: "funding".into(),
            from_role: "external_funder".into(),
            value_usd: Some(100.0),
            value_with_gas_usd: Some(103.0),
            value_eth: Some(0.05),
            value_with_gas_eth: Some(0.051),
            ..ValueFlowEdgePayload::default()
        },
        ValueFlowEdgePayload {
            contract_address: "0xdup".into(),
            from_address: "0xbot".into(),
            tx_hash: "0xlure".into(),
            channel: "sale_payment".into(),
            from_role: "buyer".into(),
            value_usd: Some(200.0),
            value_with_gas_usd: Some(205.0),
            value_eth: Some(0.1),
            value_with_gas_eth: Some(0.102),
            ..ValueFlowEdgePayload::default()
        },
        ValueFlowEdgePayload {
            contract_address: "0xdup".into(),
            from_address: "0xbot".into(),
            tx_hash: "0xlure".into(),
            channel: "sale_payment".into(),
            from_role: "buyer".into(),
            value_usd: Some(1.0),
            value_with_gas_usd: Some(6.0),
            value_eth: Some(0.001),
            value_with_gas_eth: Some(0.003),
            ..ValueFlowEdgePayload::default()
        },
        ValueFlowEdgePayload {
            contract_address: "0xdup".into(),
            from_address: "0xdup".into(),
            tx_hash: "0xexit".into(),
            channel: "withdrawal".into(),
            from_role: "mint_contract".into(),
            value_usd: Some(50.0),
            value_with_gas_usd: Some(57.0),
            value_eth: Some(0.025),
            value_with_gas_eth: Some(0.028),
            ..ValueFlowEdgePayload::default()
        },
        ValueFlowEdgePayload {
            contract_address: "0xdup".into(),
            from_address: "0xhonest".into(),
            tx_hash: "0xhonest-lure".into(),
            channel: "sale_payment".into(),
            from_role: "buyer".into(),
            value_usd: Some(300.0),
            value_with_gas_usd: Some(333.0),
            value_eth: Some(0.15),
            value_with_gas_eth: Some(0.16),
            ..ValueFlowEdgePayload::default()
        },
    ];

    let stats = build_paper_stats(PaperStatsInput {
        config: PaperStatsConfig::default(),
        seed_collection_stats: &SeedCollectionStatsPayload::default(),
        duplicate_candidates: &[],
        duplicate_contracts: &[],
        legit_duplicates: &[],
        infringing_tokens: &[],
        malicious_addresses: &[MaliciousAddressPayload {
            address: "0xbot".into(),
            ..MaliciousAddressPayload::default()
        }],
        victim_acquisition_addresses: &[victim],
        value_flow_edges: &value_flow_edges,
        nft_propagation_paths: &Default::default(),
    });

    assert_eq!(stats.attacker_cost.setup_gas_usd, 3.0);
    assert_eq!(stats.attacker_cost.lure_gas_usd, 5.0);
    assert_eq!(stats.attacker_cost.exit_gas_usd, 7.0);
    assert_eq!(stats.attacker_cost.total_gas_usd, 15.0);
    assert_eq!(stats.attacker_cost_details.len(), 3);
    assert_eq!(stats.attacker_cost_details[0].stage, "exit");
    assert_eq!(stats.attacker_cost_details[0].channel, "withdrawal");
    assert_eq!(stats.attacker_cost_details[0].tx_hash, "0xexit");
    assert_eq!(stats.attacker_cost_details[0].gas_payer_address, "0xdup");
    assert_eq!(stats.attacker_cost_details[0].gas_usd, 7.0);
    assert_eq!(stats.attacker_cost_details[1].stage, "lure");
    assert_eq!(stats.attacker_cost_details[1].tx_hash, "0xlure");
    assert_eq!(stats.attacker_cost_details[1].gas_usd, 5.0);
    assert_eq!(stats.attacker_cost_details[2].stage, "setup");
    assert_eq!(stats.attacker_cost_details[2].tx_hash, "0xsetup");
    assert_eq!(stats.attacker_cost_details[2].gas_usd, 3.0);
    assert_eq!(
        stats.attacker_cost.top_contract_contribution_numerator,
        15.0
    );
    assert_eq!(
        stats.attacker_cost.top_contract_contribution_denominator,
        15.0
    );
}

#[test]
fn attacker_cost_counts_same_transaction_gas_once_across_stages() {
    let value_flow_edges = vec![
        ValueFlowEdgePayload {
            contract_address: "0xdup".into(),
            from_address: "0xfunder".into(),
            gas_payer_address: "0xminter".into(),
            tx_hash: "0xmint".into(),
            channel: "funding".into(),
            from_role: "external_funder".into(),
            gas_eth: Some(0.002),
            gas_usd: Some(4.0),
            ..ValueFlowEdgePayload::default()
        },
        ValueFlowEdgePayload {
            contract_address: "0xdup".into(),
            from_address: "0xminter".into(),
            gas_payer_address: "0xminter".into(),
            tx_hash: "0xmint".into(),
            channel: "mint_payment".into(),
            from_role: "paid_minter".into(),
            gas_eth: Some(0.002),
            gas_usd: Some(4.0),
            ..ValueFlowEdgePayload::default()
        },
    ];

    let stats = build_paper_stats(PaperStatsInput {
        config: PaperStatsConfig::default(),
        seed_collection_stats: &SeedCollectionStatsPayload::default(),
        duplicate_candidates: &[],
        duplicate_contracts: &[],
        legit_duplicates: &[],
        infringing_tokens: &[],
        malicious_addresses: &[MaliciousAddressPayload {
            address: "0xminter".into(),
            ..MaliciousAddressPayload::default()
        }],
        victim_acquisition_addresses: &[],
        value_flow_edges: &value_flow_edges,
        nft_propagation_paths: &Default::default(),
    });

    assert_eq!(stats.attacker_cost.setup_gas_usd, 0.0);
    assert_eq!(stats.attacker_cost.lure_gas_usd, 4.0);
    assert_eq!(stats.attacker_cost.total_gas_usd, 4.0);
    assert_eq!(stats.attacker_cost_details.len(), 1);
    assert_eq!(stats.attacker_cost_details[0].stage, "lure");
    assert_eq!(stats.attacker_cost_details[0].channel, "mint_payment");
}

#[test]
fn paper_stats_merge_deduplicates_attacker_cost_details_by_contract_transaction() {
    let first = paper_stats_for_attacker_gas_transaction("0xdup", "0xattacker", "0xgas", 5.0);
    let second = paper_stats_for_attacker_gas_transaction("0xdup", "0xattacker", "0xgas", 5.0);

    let merged = merge_paper_stats([&first, &second], PaperStatsConfig::default());

    assert_eq!(merged.attacker_cost.lure_gas_usd, 5.0);
    assert_eq!(merged.attacker_cost.total_gas_usd, 5.0);
    assert_eq!(
        merged.attacker_cost_by_contract_usd.get("0xdup"),
        Some(&5.0)
    );
    assert_eq!(merged.attacker_cost_details.len(), 1);
    assert_eq!(merged.attacker_cost_details[0].tx_hash, "0xgas");
}

#[test]
fn paper_stats_merge_deduplicates_current_attacker_cost_when_legacy_cost_has_no_details() {
    let first = paper_stats_for_attacker_gas_transaction("0xdup", "0xattacker", "0xgas", 5.0);
    let second = paper_stats_for_attacker_gas_transaction("0xdup", "0xattacker", "0xgas", 5.0);
    let legacy = legacy_attacker_cost_without_details("0xlegacy", 2.0);

    let merged = merge_paper_stats([&first, &second, &legacy], PaperStatsConfig::default());

    assert_eq!(merged.attacker_cost.setup_gas_usd, 2.0);
    assert_eq!(merged.attacker_cost.lure_gas_usd, 5.0);
    assert_eq!(merged.attacker_cost.total_gas_usd, 7.0);
    assert_eq!(
        merged.attacker_cost_by_contract_usd.get("0xdup"),
        Some(&5.0)
    );
    assert_eq!(
        merged.attacker_cost_by_contract_usd.get("0xlegacy"),
        Some(&2.0)
    );
    assert_eq!(merged.attacker_cost_details.len(), 1);
    assert_eq!(merged.attacker_cost_details[0].tx_hash, "0xgas");
}

#[test]
fn attacker_cost_does_not_count_gas_payer_without_attacker_evidence() {
    let value_flow_edges = vec![
        ValueFlowEdgePayload {
            contract_address: "0xdup".into(),
            from_address: "0xsender".into(),
            gas_payer_address: "0xrelayer".into(),
            tx_hash: "0xrelayed".into(),
            channel: "sale_payment".into(),
            from_role: "buyer".into(),
            gas_eth: Some(0.01),
            gas_usd: Some(20.0),
            ..ValueFlowEdgePayload::default()
        },
        ValueFlowEdgePayload {
            contract_address: "0xdup".into(),
            from_address: "0xsender".into(),
            gas_payer_address: "0xexplicit".into(),
            tx_hash: "0xexplicit".into(),
            channel: "sale_payment".into(),
            from_role: "buyer".into(),
            gas_eth: Some(0.02),
            gas_usd: Some(40.0),
            ..ValueFlowEdgePayload::default()
        },
    ];

    let stats = build_paper_stats(PaperStatsInput {
        config: PaperStatsConfig::default(),
        seed_collection_stats: &SeedCollectionStatsPayload::default(),
        duplicate_candidates: &[],
        duplicate_contracts: &[],
        legit_duplicates: &[],
        infringing_tokens: &[],
        malicious_addresses: &[MaliciousAddressPayload {
            address: "0xexplicit".into(),
            ..MaliciousAddressPayload::default()
        }],
        victim_acquisition_addresses: &[],
        value_flow_edges: &value_flow_edges,
        nft_propagation_paths: &Default::default(),
    });

    assert_eq!(stats.attacker_cost.lure_gas_usd, 40.0);
    assert_eq!(stats.attacker_cost.total_gas_usd, 40.0);
    assert_eq!(stats.attacker_cost_details.len(), 1);
    assert_eq!(
        stats.attacker_cost_details[0].gas_payer_address,
        "0xexplicit"
    );
}

#[test]
fn attacker_cost_does_not_count_deployment_without_operator_evidence() {
    let value_flow_edges = vec![ValueFlowEdgePayload {
        contract_address: "0xdup".into(),
        from_address: "0xunknown".into(),
        tx_hash: "0xdeploy".into(),
        channel: "contract_deploy".into(),
        gas_eth: Some(0.01),
        gas_usd: Some(20.0),
        ..ValueFlowEdgePayload::default()
    }];

    let stats = build_paper_stats(PaperStatsInput {
        config: PaperStatsConfig::default(),
        seed_collection_stats: &SeedCollectionStatsPayload::default(),
        duplicate_candidates: &[],
        duplicate_contracts: &[],
        legit_duplicates: &[],
        infringing_tokens: &[],
        malicious_addresses: &[],
        victim_acquisition_addresses: &[],
        value_flow_edges: &value_flow_edges,
        nft_propagation_paths: &Default::default(),
    });

    assert_eq!(stats.attacker_cost.total_gas_eth, 0.0);
    assert_eq!(stats.attacker_cost.total_gas_usd, 0.0);
    assert!(stats.attacker_cost_details.is_empty());
}

#[test]
fn paper_stats_builds_output_input_ratio_rows_and_skips_zero_output_contracts() {
    let stats = build_output_input_ratio_stats();

    assert_eq!(stats.output_input_ratio_by_contract.len(), 2);
    let profit = stats
        .output_input_ratio_by_contract
        .iter()
        .find(|row| row.contract_address == "0xprofit")
        .expect("profit contract ratio row");
    assert_eq!(profit.output_usd, 100.0);
    assert_eq!(profit.input_usd, 25.0);
    assert_eq!(profit.output_input_ratio, Some(4.0));
    assert_eq!(profit.output_input_ratio_numerator, 100.0);
    assert_eq!(profit.output_input_ratio_denominator, 25.0);

    let loss = stats
        .output_input_ratio_by_contract
        .iter()
        .find(|row| row.contract_address == "0xloss")
        .expect("loss contract ratio row");
    assert_eq!(loss.output_usd, 10.0);
    assert_eq!(loss.input_usd, 20.0);
    assert_eq!(loss.output_input_ratio, Some(0.5));

    assert!(!stats
        .output_input_ratio_by_contract
        .iter()
        .any(|row| row.contract_address == "0xzero"));
    assert_eq!(stats.output_input_summary.total_output_usd, 110.0);
    assert_eq!(stats.output_input_summary.total_input_usd, 45.0);
    assert_eq!(
        stats.output_input_summary.total_output_input_ratio,
        Some(110.0 / 45.0)
    );
    assert_eq!(stats.output_input_summary.ratio_gte_one_count, 1);
    assert_eq!(stats.output_input_summary.ratio_gte_one_ratio, Some(0.5));
    assert_eq!(stats.output_input_summary.ratio_lt_one_count, 1);
    assert_eq!(stats.output_input_summary.ratio_lt_one_ratio, Some(0.5));
}

#[test]
fn paper_stats_merge_recomputes_output_input_ratio_summary() {
    let first = output_input_ratio_stats_for_contract("0xprofit", 100.0, 25.0);
    let second = output_input_ratio_stats_for_contract("0xloss", 10.0, 20.0);
    let zero_output = output_input_ratio_stats_for_contract("0xzero", 0.0, 40.0);

    let merged = merge_paper_stats([&first, &second, &zero_output], PaperStatsConfig::default());

    assert_eq!(merged.output_input_ratio_by_contract.len(), 2);
    assert!(merged
        .output_input_ratio_by_contract
        .iter()
        .any(|row| row.contract_address == "0xprofit" && row.output_input_ratio == Some(4.0)));
    assert!(merged
        .output_input_ratio_by_contract
        .iter()
        .any(|row| row.contract_address == "0xloss" && row.output_input_ratio == Some(0.5)));
    assert!(!merged
        .output_input_ratio_by_contract
        .iter()
        .any(|row| row.contract_address == "0xzero"));
    assert_eq!(merged.output_input_summary.total_output_usd, 110.0);
    assert_eq!(merged.output_input_summary.total_input_usd, 45.0);
    assert_eq!(
        merged.output_input_summary.total_output_input_ratio,
        Some(110.0 / 45.0)
    );
    assert_eq!(merged.output_input_summary.ratio_gte_one_count, 1);
    assert_eq!(merged.output_input_summary.ratio_gte_one_ratio, Some(0.5));
    assert_eq!(merged.output_input_summary.ratio_lt_one_count, 1);
    assert_eq!(merged.output_input_summary.ratio_lt_one_ratio, Some(0.5));
}

#[test]
fn markdown_omits_attacker_cost_details_while_json_keeps_them() {
    let paper_stats = PaperStatsPayload {
        attacker_cost: top_contract_analysis_rs::models::PaperAttackerCostPayload {
            setup_gas_eth: 0.01,
            setup_gas_usd: 20.0,
            total_gas_eth: 0.01,
            total_gas_usd: 20.0,
            top_contract_contribution_ratio: Some(1.0),
            top_contract_contribution_numerator: 20.0,
            top_contract_contribution_denominator: 20.0,
            ..top_contract_analysis_rs::models::PaperAttackerCostPayload::default()
        },
        attacker_cost_details: vec![
            top_contract_analysis_rs::models::PaperAttackerCostDetailPayload {
                contract_address: "0xdup".into(),
                stage: "setup".into(),
                channel: "contract_deploy".into(),
                tx_hash: "0xdeploy".into(),
                gas_payer_address: "0xdeployer".into(),
                gas_eth: 0.01,
                gas_usd: 20.0,
                from_role: "contract_deployer".into(),
                to_role: "mint_contract".into(),
                evidence_type: "deployment_receipt_gas".into(),
            },
        ],
        ..PaperStatsPayload::default()
    };
    let json = serde_json::to_value(&SingleReportPayload {
        paper_stats: paper_stats.clone(),
        ..SingleReportPayload::default()
    })
    .unwrap();

    let markdown = render_human_readable_report(&SingleReportPayload {
        paper_stats,
        ..SingleReportPayload::default()
    });

    assert_eq!(
        json["paper_stats"]["attacker_cost_details"][0]["tx_hash"],
        "0xdeploy"
    );
    assert!(markdown.contains("## 攻击者成本"));
    assert!(markdown.contains("| gas | 0.01 / 20 | 0 / 0 | 0 / 0 | 0.01 / 20 | 100.00% (20/20) |"));
    assert!(!markdown.contains("### 攻击者成本明细"));
    assert!(!markdown.contains(
        "| contract_address | stage | channel | tx_hash | gas_payer | gas ETH/USD | from_role | to_role | evidence |"
    ));
    assert!(!markdown.contains(
        "| 0xdup | setup | contract_deploy | 0xdeploy | 0xdeployer | 0.01 / 20 | contract_deployer | mint_contract | deployment_receipt_gas |"
    ));
}

#[test]
fn paper_stats_json_keeps_audit_detail_fields() {
    let paper_stats = PaperStatsPayload {
        malicious_addresses: vec!["0xmalicious".into()],
        honest_addresses: vec!["0xhonest".into()],
        repeat_infringing_malicious_addresses: vec!["0xrepeat".into()],
        attacker_cost_by_contract_usd: BTreeMap::from([("0xdup".into(), 20.0)]),
        honest_loss_by_contract_usd: BTreeMap::from([("0xdup".into(), 30.0)]),
        stuck_time_numerator_by_contract: BTreeMap::from([("0xdup".into(), 100.0)]),
        stuck_time_denominator_by_contract: BTreeMap::from([("0xdup".into(), 10.0)]),
        behavior_contract_denominator: 2,
        behavior_contract_denominator_keys: vec!["0xdup".into()],
        duplicate_nft_keys_by_category: BTreeMap::from([("total".into(), vec!["0xdup:1".into()])]),
        duplicate_contract_keys_by_category: BTreeMap::from([(
            "total".into(),
            vec!["0xdup".into()],
        )]),
        duplicate_contract_denominator_keys: vec!["0xdup".into()],
        behavior_contracts_by_type: BTreeMap::from([("Wash Trading".into(), vec!["0xdup".into()])]),
        behavior_addresses_by_type: BTreeMap::from([(
            "Wash Trading".into(),
            vec!["0xmalicious".into()],
        )]),
        behavior_nfts_by_type: BTreeMap::from([("Wash Trading".into(), vec!["0xdup:1".into()])]),
        behavior_buyers_by_type: BTreeMap::from([(
            "Pump-and-Exit".into(),
            vec!["0xhonest".into()],
        )]),
        ..PaperStatsPayload::default()
    };

    let json = serde_json::to_value(&paper_stats).unwrap();

    assert_eq!(json["malicious_addresses"][0], "0xmalicious");
    assert_eq!(json["honest_addresses"][0], "0xhonest");
    assert_eq!(json["repeat_infringing_malicious_addresses"][0], "0xrepeat");
    assert_eq!(json["attacker_cost_by_contract_usd"]["0xdup"], 20.0);
    assert_eq!(json["honest_loss_by_contract_usd"]["0xdup"], 30.0);
    assert_eq!(json["stuck_time_numerator_by_contract"]["0xdup"], 100.0);
    assert_eq!(json["stuck_time_denominator_by_contract"]["0xdup"], 10.0);
    assert_eq!(json["behavior_contract_denominator"], 2);
    assert_eq!(json["behavior_contract_denominator_keys"][0], "0xdup");
    assert_eq!(
        json["duplicate_nft_keys_by_category"]["total"][0],
        "0xdup:1"
    );
    assert_eq!(
        json["duplicate_contract_keys_by_category"]["total"][0],
        "0xdup"
    );
    assert_eq!(json["duplicate_contract_denominator_keys"][0], "0xdup");
    assert_eq!(
        json["behavior_contracts_by_type"]["Wash Trading"][0],
        "0xdup"
    );
    assert_eq!(
        json["behavior_addresses_by_type"]["Wash Trading"][0],
        "0xmalicious"
    );
    assert_eq!(json["behavior_nfts_by_type"]["Wash Trading"][0], "0xdup:1");
    assert_eq!(
        json["behavior_buyers_by_type"]["Pump-and-Exit"][0],
        "0xhonest"
    );
}

#[test]
fn markdown_places_large_tables_last_and_sorts_by_key_metrics() {
    let paper_stats = PaperStatsPayload {
        data_quality: PaperDataQualityPayload {
            sale_price_parseable_count: 1,
            sale_price_total_count: 2,
            sale_price_parseable_ratio: Some(0.5),
            legit_duplicate_contract_count: 0,
            ..PaperDataQualityPayload::default()
        },
        contract_behavior_stats: vec![
            PaperContractBehaviorStatsPayload {
                contract_address: "0xlow".into(),
                wash_trading: vec![PaperWashTradingRowPayload {
                    fake_volume_usd: 10.0,
                    ..PaperWashTradingRowPayload::default()
                }],
                honest_buyers: vec![PaperHonestBuyerRowPayload {
                    honest_buyer: "0xsmallbuyer".into(),
                    total_paid_usd: 50.0,
                    still_holding: true,
                    ..PaperHonestBuyerRowPayload::default()
                }],
                ..PaperContractBehaviorStatsPayload::default()
            },
            PaperContractBehaviorStatsPayload {
                contract_address: "0xhigh".into(),
                pump_and_exit: vec![PaperPumpExitRowPayload {
                    linked_loss_usd: 2_000.0,
                    ..PaperPumpExitRowPayload::default()
                }],
                honest_buyers: vec![PaperHonestBuyerRowPayload {
                    honest_buyer: "0xbigbuyer".into(),
                    total_paid_usd: 3_000.0,
                    still_holding: true,
                    ..PaperHonestBuyerRowPayload::default()
                }],
                ..PaperContractBehaviorStatsPayload::default()
            },
        ],
        ..PaperStatsPayload::default()
    };

    let markdown = render_human_readable_report(&SingleReportPayload {
        paper_stats,
        ..SingleReportPayload::default()
    });

    assert!(markdown.find("## 数据质量").unwrap() < markdown.find("## 合约行为明细").unwrap());
    assert!(
        markdown.find("### Match 合约 0xhigh").unwrap()
            < markdown.find("### Match 合约 0xlow").unwrap(),
        "match contract groups should be sorted by descending internal sort score"
    );
    assert!(!markdown.contains("Impact USD"));
    assert!(markdown.contains("0xbigbuyer"));
    assert!(markdown.contains("0xsmallbuyer"));
}

#[test]
fn markdown_contract_behavior_details_include_experiment_rows() {
    let paper_stats = PaperStatsPayload {
        contract_behavior_stats: vec![PaperContractBehaviorStatsPayload {
            contract_address: "0xcopy".into(),
            wash_trading: vec![PaperWashTradingRowPayload {
                cycle_id: "wash1".into(),
                participant_node_count: 3,
                token_gini: Some(0.4),
                avg_cycle_blocks: Some(12.0),
                fake_volume_eth: 1.2,
                fake_volume_usd: 2_400.0,
            }],
            wash_cycle_size_distribution: vec![],
            pump_and_exit: vec![PaperPumpExitRowPayload {
                cycle_id: "pump1".into(),
                exit_delay_seconds: Some(60),
                exit_price_premium: Some(1.5),
                exit_price_premium_numerator: 300.0,
                exit_price_premium_denominator: 200.0,
                exit_ratio: Some(0.5),
                exit_ratio_numerator: 1,
                exit_ratio_denominator: 2,
                linked_honest_buyer_count: 2,
                linked_loss_eth: 1.0,
                linked_loss_usd: 2_000.0,
            }],
            star_behaviors: vec![PaperStarBehaviorRowPayload {
                behavior: "Sybil Distribution".into(),
                centers: 1,
                edges: 6,
                wallets: 7,
                tokens: 4,
                avg_fan_out: Some(3.0),
                avg_fan_out_numerator: 6,
                avg_fan_out_denominator: 2,
                median_holding_seconds: Some(120.0),
                total_value_eth: 0.6,
                total_value_usd: 1_200.0,
            }],
            layered_transfers: vec![PaperLayeredTransferRowPayload {
                path_id: "path1".into(),
                tokens: 2,
                length: 4,
                wallets: 5,
                zero_or_low_value_hops: 2,
                total_path_duration_seconds: Some(3600),
                total_value_eth: 0.4,
                total_value_usd: 800.0,
            }],
            inventory_concentration: vec![PaperInventoryConcentrationRowPayload {
                hub_address: "0xhub".into(),
                source_wallets: 4,
                inbound_txns: 9,
                token_share: Some(0.25),
                token_share_numerator: 5,
                token_share_denominator: 20,
                value_collected_eth: 0.3,
                value_collected_usd: 600.0,
                value_share: Some(0.4),
                value_share_numerator: 600.0,
                value_share_denominator: 1_500.0,
                collection_window_seconds: Some(7200),
            }],
            honest_buyers: vec![PaperHonestBuyerRowPayload {
                honest_buyer: "0xbuyer".into(),
                fake_nft_bought: 2,
                total_paid_eth: 0.7,
                total_paid_usd: 1_400.0,
                source_pattern: "Pump-and-Exit".into(),
                time_to_purchase_seconds: Some(900),
                still_holding: true,
                holding_seconds: Some(86_400),
            }],
        }],
        ..PaperStatsPayload::default()
    };

    let markdown = render_human_readable_report(&SingleReportPayload {
        paper_stats,
        ..SingleReportPayload::default()
    });

    assert!(markdown.contains("### Match 合约 0xcopy"));
    assert!(markdown.contains("#### Wash Trading"));
    assert!(markdown.contains("| wash1 | 3 | 0.4 | 12 | 1.2 / 2400 |"));
    assert!(markdown.contains("#### Pump-and-Exit"));
    assert!(markdown.contains("| pump1 | 60s | 1.5x (300/200) | 50.00% (1/2) | 2 | 1 / 2000 |"));
    assert!(markdown.contains("#### 星型行为"));
    assert!(
        markdown.contains("| Sybil Distribution | 1 | 6 | 7 | 4 | 3 (6/2) | 120s | 0.6 / 1200 |")
    );
    assert!(markdown.contains("#### Layered Transfer"));
    assert!(markdown.contains("| path1 | 2 | 4 | 5 | 2 | 3600s | 0.4 / 800 |"));
    assert!(markdown.contains("#### Inventory Concentration"));
    assert!(markdown
        .contains("| 0xhub | 4 | 9 | 25.00% (5/20) | 0.3 / 600 | 40.00% (600/1500) | 7200s |"));
    assert!(markdown.contains("#### 诚实买家"));
    assert!(markdown.contains("| 0xbuyer | 2 | 0.7 / 1400 | Pump-and-Exit | 900s | 是 | 86400s |"));
    assert!(!markdown.contains("hub_address"));
}

#[test]
fn paper_stats_tracks_ratio_numerators_and_address_roles() {
    let seed_stats = SeedCollectionStatsPayload {
        seed_nft_count: 10,
        ..SeedCollectionStatsPayload::default()
    };
    let duplicate_candidates = vec![
        DuplicateCandidate {
            contract_address: "0xdup1".into(),
            token_id: "1".into(),
            match_reasons: vec!["token_uri_match".into(), "name_match".into()],
            ..DuplicateCandidate::default()
        },
        DuplicateCandidate {
            contract_address: "0xdup1".into(),
            token_id: "2".into(),
            match_reasons: vec!["token_uri_match".into()],
            ..DuplicateCandidate::default()
        },
        DuplicateCandidate {
            contract_address: "0xdup2".into(),
            token_id: "3".into(),
            match_reasons: vec!["image_uri_match".into()],
            ..DuplicateCandidate::default()
        },
        DuplicateCandidate {
            contract_address: "0xdup3".into(),
            token_id: "4".into(),
            match_reasons: vec!["metadata_match".into()],
            ..DuplicateCandidate::default()
        },
    ];
    let duplicate_contracts = vec![
        DuplicateContractPayload {
            contract_address: "0xdup1".into(),
            candidate_count: 2,
            ..DuplicateContractPayload::default()
        },
        DuplicateContractPayload {
            contract_address: "0xdup2".into(),
            candidate_count: 1,
            ..DuplicateContractPayload::default()
        },
        DuplicateContractPayload {
            contract_address: "0xdup3".into(),
            candidate_count: 1,
            ..DuplicateContractPayload::default()
        },
    ];
    let malicious_addresses = vec![
        MaliciousAddressPayload {
            address: "0xoperator".into(),
            ..MaliciousAddressPayload::default()
        },
        MaliciousAddressPayload {
            address: "0xrepeat".into(),
            ..MaliciousAddressPayload::default()
        },
    ];
    let infringing_tokens = vec![
        InfringingTokenRecord {
            contract_address: "0xdup1".into(),
            minter_address: "0xrepeat".into(),
            token_id: "1".into(),
            match_reasons: vec!["token_uri_match".into()],
            ..InfringingTokenRecord::default()
        },
        InfringingTokenRecord {
            contract_address: "0xdup2".into(),
            minter_address: "0xrepeat".into(),
            token_id: "2".into(),
            match_reasons: vec!["image_uri_match".into()],
            ..InfringingTokenRecord::default()
        },
    ];
    let victim_acquisition_addresses = vec![VictimAcquisitionAddressPayload {
        address: "0xhonest".into(),
        total_acquisition_cost_eth: 3.0,
        total_acquisition_cost_usd: 6_000.0,
        total_stuck_cost_eth: 2.0,
        total_stuck_cost_usd: 4_000.0,
        paid_mint_stuck_token_count: 2,
        is_stuck: true,
        ..VictimAcquisitionAddressPayload::default()
    }];

    let stats = build_paper_stats(PaperStatsInput {
        config: PaperStatsConfig::default(),
        seed_collection_stats: &seed_stats,
        duplicate_candidates: &duplicate_candidates,
        duplicate_contracts: &duplicate_contracts,
        legit_duplicates: &[],
        infringing_tokens: &infringing_tokens,
        malicious_addresses: &malicious_addresses,
        victim_acquisition_addresses: &victim_acquisition_addresses,
        value_flow_edges: &[],
        nft_propagation_paths: &Default::default(),
    });

    let token_uri = stats
        .duplicate_scale
        .iter()
        .find(|row| row.category == "token_uri")
        .unwrap();
    assert_eq!(token_uri.duplicate_nft_ratio_numerator, 1);
    assert_eq!(token_uri.duplicate_nft_ratio_denominator, 2);
    assert_eq!(token_uri.duplicate_contract_ratio_numerator, 1);
    assert_eq!(token_uri.duplicate_contract_ratio_denominator, 3);

    assert_eq!(stats.address_classification.malicious_address_count, 2);
    assert_eq!(
        stats
            .address_classification
            .repeat_infringing_malicious_address_count,
        1
    );
    assert_eq!(stats.address_classification.honest_address_count, 1);
    assert_eq!(stats.address_classification.total_address_count, 3);

    let total_loss = &stats.honest_loss;
    assert_eq!(total_loss.stuck_nft_ratio_numerator, 2);
    assert_eq!(total_loss.stuck_nft_ratio_denominator, 2);
    assert_eq!(total_loss.total_loss_usd, 4_000.0);
}

#[test]
fn paper_stats_uses_participant_universe_and_extracts_behavior_patterns() {
    let config = PaperStatsConfig {
        min_cycle_size: 2,
        min_path_length: 3,
        center_fanout_threshold: 1,
        concentration_top_pct: 0.5,
        analysis_timestamp: 250,
    };
    let path = NftPropagationPathPayload {
        contract_address: "0xdup".into(),
        summary: NftPropagationSummaryPayload {
            token_count: 4,
            first_block_time: 100,
            ..NftPropagationSummaryPayload::default()
        },
        nodes: BTreeMap::from([
            (
                "0xa".into(),
                NftPropagationNodePayload {
                    roles: vec!["malicious".into()],
                    sent_transfer_count: 2,
                    received_transfer_count: 1,
                    ..NftPropagationNodePayload::default()
                },
            ),
            (
                "0xb".into(),
                NftPropagationNodePayload {
                    roles: vec!["malicious".into()],
                    sent_transfer_count: 2,
                    received_transfer_count: 1,
                    ..NftPropagationNodePayload::default()
                },
            ),
            (
                "0xcenter".into(),
                NftPropagationNodePayload {
                    sent_transfer_count: 3,
                    ..NftPropagationNodePayload::default()
                },
            ),
            (
                "0xleaf1".into(),
                NftPropagationNodePayload {
                    sent_transfer_count: 1,
                    received_transfer_count: 1,
                    ..NftPropagationNodePayload::default()
                },
            ),
            (
                "0xleaf2".into(),
                NftPropagationNodePayload {
                    received_transfer_count: 1,
                    ..NftPropagationNodePayload::default()
                },
            ),
            (
                "0xleaf3".into(),
                NftPropagationNodePayload {
                    received_transfer_count: 1,
                    ..NftPropagationNodePayload::default()
                },
            ),
            (
                "0xhonest".into(),
                NftPropagationNodePayload {
                    roles: vec!["victim_buyer".into()],
                    bought_token_count: 1,
                    current_holding_token_count: 1,
                    total_buy_eth: 5.0,
                    total_buy_usd: 10_000.0,
                    is_stuck_victim: true,
                    ..NftPropagationNodePayload::default()
                },
            ),
        ]),
        edges: vec![
            propagation_edge(
                ("0xdup", "0xa", "0xb"),
                "1",
                "sale",
                110,
                (Some(1.0), Some(2_000.0)),
            ),
            propagation_edge(
                ("0xdup", "0xb", "0xa"),
                "1",
                "sale",
                120,
                (Some(2.0), Some(4_000.0)),
            ),
            propagation_edge(
                ("0xdup", "0xb", "0xhonest"),
                "1",
                "sale",
                150,
                (Some(5.0), Some(10_000.0)),
            ),
            propagation_edge(
                ("0xdup", "0xcenter", "0xleaf1"),
                "2",
                "transfer",
                130,
                (None, None),
            ),
            propagation_edge(
                ("0xdup", "0xcenter", "0xleaf2"),
                "3",
                "transfer",
                131,
                (None, None),
            ),
            propagation_edge(
                ("0xdup", "0xleaf1", "0xleaf3"),
                "2",
                "transfer",
                140,
                (None, None),
            ),
            propagation_edge(
                ("0xdup", "0xleaf2", "0xcenter"),
                "3",
                "transfer",
                180,
                (None, None),
            ),
        ],
        ..NftPropagationPathPayload::default()
    };
    let victim_acquisition_addresses = vec![VictimAcquisitionAddressPayload {
        address: "0xhonest".into(),
        contract_addresses: vec!["0xdup".into()],
        secondary_sale_count: 1,
        secondary_sale_stuck_cost_eth: 5.0,
        secondary_sale_stuck_cost_usd: 10_000.0,
        total_stuck_cost_eth: 5.0,
        total_stuck_cost_usd: 10_000.0,
        is_stuck: true,
        ..VictimAcquisitionAddressPayload::default()
    }];
    let stats = build_paper_stats(PaperStatsInput {
        config,
        seed_collection_stats: &SeedCollectionStatsPayload {
            seed_nft_count: 4,
            ..SeedCollectionStatsPayload::default()
        },
        duplicate_candidates: &[],
        duplicate_contracts: &[],
        legit_duplicates: &[],
        infringing_tokens: &[],
        malicious_addresses: &[MaliciousAddressPayload {
            address: "0xa".into(),
            ..MaliciousAddressPayload::default()
        }],
        victim_acquisition_addresses: &victim_acquisition_addresses,
        value_flow_edges: &[],
        nft_propagation_paths: &BTreeMap::from([("0xdup".into(), path)]),
    });

    assert!(
        stats.address_classification.malicious_address_count > 1,
        "all non-honest graph participants should count as malicious"
    );
    let contract_stats = stats
        .contract_behavior_stats
        .iter()
        .find(|row| row.contract_address == "0xdup")
        .unwrap();
    assert!(!contract_stats.wash_trading.is_empty());
    assert!(!contract_stats.pump_and_exit.is_empty());
    assert!(!contract_stats.star_behaviors.is_empty());
    assert!(!contract_stats.layered_transfers.is_empty());
    assert!(!contract_stats.inventory_concentration.is_empty());
    assert_eq!(
        contract_stats.inventory_concentration[0].token_share_numerator,
        1
    );
    assert_eq!(
        contract_stats.inventory_concentration[0].token_share_denominator,
        4
    );
    assert_eq!(
        contract_stats.inventory_concentration[0].value_share_numerator,
        contract_stats.inventory_concentration[0].value_collected_usd
    );
    assert_eq!(
        contract_stats.inventory_concentration[0].value_share_denominator,
        16_000.0
    );
    assert_eq!(contract_stats.honest_buyers[0].honest_buyer, "0xhonest");
    assert!(stats
        .malicious_behavior_summary
        .iter()
        .any(|row| row.behavior_type == "Wash Trading" && row.instance_count > 0));
}

#[test]
fn paper_stats_detects_multi_node_wash_cycle_and_exit() {
    let config = PaperStatsConfig {
        min_cycle_size: 3,
        analysis_timestamp: 260,
        ..PaperStatsConfig::default()
    };
    let path = NftPropagationPathPayload {
        contract_address: "0xcycle".into(),
        summary: NftPropagationSummaryPayload {
            token_count: 1,
            first_block_time: 100,
            ..NftPropagationSummaryPayload::default()
        },
        nodes: BTreeMap::from([
            ("0xa".into(), NftPropagationNodePayload::default()),
            ("0xb".into(), NftPropagationNodePayload::default()),
            ("0xc".into(), NftPropagationNodePayload::default()),
            (
                "0xhonest".into(),
                NftPropagationNodePayload {
                    roles: vec!["victim_buyer".into()],
                    current_holding_token_count: 1,
                    is_stuck_victim: true,
                    ..NftPropagationNodePayload::default()
                },
            ),
        ]),
        edges: vec![
            propagation_edge(
                ("0xcycle", "0xa", "0xb"),
                "1",
                "sale",
                110,
                (Some(1.0), Some(2_000.0)),
            ),
            propagation_edge(
                ("0xcycle", "0xb", "0xc"),
                "1",
                "sale",
                120,
                (Some(2.0), Some(4_000.0)),
            ),
            propagation_edge(
                ("0xcycle", "0xc", "0xa"),
                "1",
                "sale",
                130,
                (Some(3.0), Some(6_000.0)),
            ),
            propagation_edge(
                ("0xcycle", "0xc", "0xhonest"),
                "1",
                "sale",
                160,
                (Some(8.0), Some(16_000.0)),
            ),
        ],
        ..NftPropagationPathPayload::default()
    };
    let victim = VictimAcquisitionAddressPayload {
        address: "0xhonest".into(),
        contract_addresses: vec!["0xcycle".into()],
        secondary_sale_count: 1,
        secondary_sale_stuck_cost_usd: 16_000.0,
        total_stuck_cost_usd: 16_000.0,
        is_stuck: true,
        ..VictimAcquisitionAddressPayload::default()
    };

    let stats = build_paper_stats(PaperStatsInput {
        config,
        seed_collection_stats: &SeedCollectionStatsPayload::default(),
        duplicate_candidates: &[],
        duplicate_contracts: &[],
        legit_duplicates: &[],
        infringing_tokens: &[],
        malicious_addresses: &[],
        victim_acquisition_addresses: &[victim],
        value_flow_edges: &[],
        nft_propagation_paths: &BTreeMap::from([("0xcycle".into(), path)]),
    });

    let contract_stats = stats
        .contract_behavior_stats
        .iter()
        .find(|row| row.contract_address == "0xcycle")
        .unwrap();
    assert_eq!(contract_stats.wash_trading[0].participant_node_count, 3);
    assert_eq!(contract_stats.pump_and_exit[0].linked_honest_buyer_count, 1);
    assert_eq!(contract_stats.pump_and_exit[0].exit_ratio_numerator, 1);
    assert_eq!(contract_stats.pump_and_exit[0].exit_ratio_denominator, 1);
    assert_eq!(
        contract_stats.honest_buyers[0].source_pattern,
        "Pump-and-Exit"
    );
}

#[test]
fn paper_stats_classifies_star_behaviors_on_scc_dag() {
    let config = PaperStatsConfig {
        center_fanout_threshold: 2,
        ..PaperStatsConfig::default()
    };
    let path = NftPropagationPathPayload {
        contract_address: "0xsccdag".into(),
        nodes: BTreeMap::from([
            ("0xa".into(), NftPropagationNodePayload::default()),
            ("0xb".into(), NftPropagationNodePayload::default()),
            ("0xleaf1".into(), NftPropagationNodePayload::default()),
            ("0xleaf2".into(), NftPropagationNodePayload::default()),
        ]),
        edges: vec![
            propagation_edge(
                ("0xsccdag", "0xa", "0xb"),
                "1",
                "transfer",
                100,
                (None, None),
            ),
            propagation_edge(
                ("0xsccdag", "0xb", "0xa"),
                "1",
                "transfer",
                101,
                (None, None),
            ),
            propagation_edge(
                ("0xsccdag", "0xa", "0xleaf1"),
                "2",
                "transfer",
                110,
                (None, None),
            ),
            propagation_edge(
                ("0xsccdag", "0xb", "0xleaf2"),
                "3",
                "transfer",
                111,
                (None, None),
            ),
        ],
        ..NftPropagationPathPayload::default()
    };

    let stats = build_paper_stats(PaperStatsInput {
        config,
        seed_collection_stats: &SeedCollectionStatsPayload::default(),
        duplicate_candidates: &[],
        duplicate_contracts: &[],
        legit_duplicates: &[],
        infringing_tokens: &[],
        malicious_addresses: &[],
        victim_acquisition_addresses: &[],
        value_flow_edges: &[],
        nft_propagation_paths: &BTreeMap::from([("0xsccdag".into(), path)]),
    });

    let contract_stats = stats
        .contract_behavior_stats
        .iter()
        .find(|row| row.contract_address == "0xsccdag")
        .unwrap();
    assert_eq!(contract_stats.star_behaviors.len(), 1);
    assert_eq!(contract_stats.star_behaviors[0].behavior, "Poisoning");
    assert_eq!(contract_stats.star_behaviors[0].centers, 1);
    assert_eq!(contract_stats.star_behaviors[0].edges, 2);
    assert_eq!(contract_stats.star_behaviors[0].avg_fan_out_numerator, 2);
    assert_eq!(contract_stats.star_behaviors[0].avg_fan_out_denominator, 1);
}

#[test]
fn paper_stats_does_not_truncate_honest_buyer_rows() {
    let path = NftPropagationPathPayload {
        contract_address: "0xallbuyers".into(),
        ..NftPropagationPathPayload::default()
    };
    let victims = (0..11)
        .map(|index| VictimAcquisitionAddressPayload {
            address: format!("0xbuyer{index}"),
            contract_addresses: vec!["0xallbuyers".into()],
            secondary_sale_count: 1,
            total_acquisition_cost_usd: index as f64,
            total_stuck_cost_usd: index as f64,
            is_stuck: true,
            ..VictimAcquisitionAddressPayload::default()
        })
        .collect::<Vec<_>>();

    let stats = build_paper_stats(PaperStatsInput {
        config: PaperStatsConfig::default(),
        seed_collection_stats: &SeedCollectionStatsPayload::default(),
        duplicate_candidates: &[],
        duplicate_contracts: &[],
        legit_duplicates: &[],
        infringing_tokens: &[],
        malicious_addresses: &[],
        victim_acquisition_addresses: &victims,
        value_flow_edges: &[],
        nft_propagation_paths: &BTreeMap::from([("0xallbuyers".into(), path)]),
    });

    let contract_stats = stats
        .contract_behavior_stats
        .iter()
        .find(|row| row.contract_address == "0xallbuyers")
        .unwrap();
    assert_eq!(contract_stats.honest_buyers.len(), 11);
    let json = serde_json::to_value(&stats).unwrap();
    assert!(json["contract_behavior_stats"][0]
        .get("honest_buyers")
        .is_some());
    assert!(json["contract_behavior_stats"][0]
        .get("honest_buyers_top")
        .is_none());
}

#[test]
fn honest_buyer_fake_nft_count_matches_stuck_nft_count() {
    let path = NftPropagationPathPayload {
        contract_address: "0xpaidmint".into(),
        ..NftPropagationPathPayload::default()
    };
    let victim = VictimAcquisitionAddressPayload {
        address: "0xbuyer".into(),
        contract_addresses: vec!["0xpaidmint".into()],
        paid_mint_edge_count: 1,
        paid_mint_token_count: 4,
        paid_mint_stuck_token_count: 2,
        paid_mint_cost_usd: 400.0,
        paid_mint_stuck_cost_usd: 200.0,
        total_acquisition_cost_usd: 400.0,
        total_stuck_cost_usd: 200.0,
        is_stuck: true,
        ..VictimAcquisitionAddressPayload::default()
    };

    let stats = build_paper_stats(PaperStatsInput {
        config: PaperStatsConfig::default(),
        seed_collection_stats: &SeedCollectionStatsPayload::default(),
        duplicate_candidates: &[],
        duplicate_contracts: &[],
        legit_duplicates: &[],
        infringing_tokens: &[],
        malicious_addresses: &[],
        victim_acquisition_addresses: &[victim],
        value_flow_edges: &[],
        nft_propagation_paths: &BTreeMap::from([("0xpaidmint".into(), path)]),
    });

    assert_eq!(stats.honest_loss.stuck_nft_ratio_numerator, 2);
    assert_eq!(stats.honest_loss.stuck_nft_ratio_denominator, 4);
    assert_eq!(
        stats.contract_behavior_stats[0].honest_buyers[0].fake_nft_bought,
        2
    );
}

#[test]
fn paid_mint_only_honest_buyer_uses_value_flow_time_for_holding_columns() {
    let path = NftPropagationPathPayload {
        contract_address: "0xpaidmint".into(),
        summary: NftPropagationSummaryPayload {
            first_block_time: 1_000,
            ..NftPropagationSummaryPayload::default()
        },
        ..NftPropagationPathPayload::default()
    };
    let victim = VictimAcquisitionAddressPayload {
        address: "0xbuyer".into(),
        contract_addresses: vec!["0xpaidmint".into()],
        paid_mint_edge_count: 1,
        paid_mint_token_count: 1,
        paid_mint_stuck_token_count: 1,
        paid_mint_cost_usd: 100.0,
        paid_mint_stuck_cost_usd: 100.0,
        total_acquisition_cost_usd: 100.0,
        total_stuck_cost_usd: 100.0,
        is_stuck: true,
        ..VictimAcquisitionAddressPayload::default()
    };
    let value_flow_edges = vec![ValueFlowEdgePayload {
        contract_address: "0xpaidmint".into(),
        from_address: "0xbuyer".into(),
        tx_hash: "0xmint".into(),
        block_time: 1_100,
        channel: "mint_payment".into(),
        value_usd: Some(100.0),
        ..ValueFlowEdgePayload::default()
    }];

    let stats = build_paper_stats(PaperStatsInput {
        config: PaperStatsConfig {
            analysis_timestamp: 2_000,
            ..PaperStatsConfig::default()
        },
        seed_collection_stats: &SeedCollectionStatsPayload::default(),
        duplicate_candidates: &[],
        duplicate_contracts: &[],
        legit_duplicates: &[],
        infringing_tokens: &[],
        malicious_addresses: &[],
        victim_acquisition_addresses: &[victim],
        value_flow_edges: &value_flow_edges,
        nft_propagation_paths: &BTreeMap::from([("0xpaidmint".into(), path)]),
    });

    let buyer = &stats.contract_behavior_stats[0].honest_buyers[0];
    assert_eq!(buyer.time_to_purchase_seconds, Some(100));
    assert_eq!(buyer.holding_seconds, Some(900));
}

#[test]
fn paper_stats_requires_exit_price_premium_for_pump_and_exit() {
    let path = NftPropagationPathPayload {
        contract_address: "0xdiscount".into(),
        nodes: BTreeMap::from([
            ("0xa".into(), NftPropagationNodePayload::default()),
            ("0xb".into(), NftPropagationNodePayload::default()),
            (
                "0xhonest".into(),
                NftPropagationNodePayload {
                    roles: vec!["victim_buyer".into()],
                    current_holding_token_count: 1,
                    is_stuck_victim: true,
                    ..NftPropagationNodePayload::default()
                },
            ),
        ]),
        edges: vec![
            propagation_edge(
                ("0xdiscount", "0xa", "0xb"),
                "1",
                "sale",
                100,
                (Some(2.0), Some(4_000.0)),
            ),
            propagation_edge(
                ("0xdiscount", "0xb", "0xa"),
                "1",
                "sale",
                110,
                (Some(3.0), Some(6_000.0)),
            ),
            propagation_edge(
                ("0xdiscount", "0xa", "0xhonest"),
                "1",
                "sale",
                150,
                (Some(1.0), Some(2_000.0)),
            ),
        ],
        ..NftPropagationPathPayload::default()
    };
    let victim = VictimAcquisitionAddressPayload {
        address: "0xhonest".into(),
        contract_addresses: vec!["0xdiscount".into()],
        is_stuck: true,
        total_stuck_cost_usd: 2_000.0,
        ..VictimAcquisitionAddressPayload::default()
    };

    let stats = build_paper_stats(PaperStatsInput {
        config: PaperStatsConfig::default(),
        seed_collection_stats: &SeedCollectionStatsPayload::default(),
        duplicate_candidates: &[],
        duplicate_contracts: &[],
        legit_duplicates: &[],
        infringing_tokens: &[],
        malicious_addresses: &[],
        victim_acquisition_addresses: &[victim],
        value_flow_edges: &[],
        nft_propagation_paths: &BTreeMap::from([("0xdiscount".into(), path)]),
    });

    let contract_stats = stats
        .contract_behavior_stats
        .iter()
        .find(|row| row.contract_address == "0xdiscount")
        .unwrap();
    assert!(contract_stats.pump_and_exit.is_empty());
    assert_eq!(
        contract_stats.honest_buyers[0].source_pattern,
        "unattributed_sale"
    );
}

#[test]
fn paper_stats_merges_pump_exit_linked_buyers_with_global_deduplication() {
    let first = pump_exit_stats("0xpump", "0xsharedbuyer");
    let second = pump_exit_stats("0xpump", "0xsharedbuyer");

    let merged = merge_paper_stats([&first, &second], PaperStatsConfig::default());

    let pump = merged
        .malicious_behavior_summary
        .iter()
        .find(|row| row.behavior_type == "Pump-and-Exit")
        .unwrap();
    assert_eq!(pump.instance_count, 2);
    assert_eq!(pump.linked_buyer_count, 1);
}

#[test]
fn paper_stats_merges_behavior_coverage_denominator_with_global_contract_deduplication() {
    let first = two_node_cycle_stats("0xsharedcontract");
    let second = two_node_cycle_stats("0xsharedcontract");

    let merged = merge_paper_stats([&first, &second], PaperStatsConfig::default());

    let wash = merged
        .malicious_behavior_summary
        .iter()
        .find(|row| row.behavior_type == "Wash Trading")
        .unwrap();
    assert_eq!(wash.contract_count, 1);
    assert_eq!(wash.contract_coverage_denominator, 1);
    assert_eq!(wash.contract_coverage_ratio, Some(1.0));
}

#[test]
fn paper_stats_computes_global_deduplication_and_concentration() {
    let config = PaperStatsConfig {
        concentration_top_pct: 0.5,
        analysis_timestamp: 250,
        ..PaperStatsConfig::default()
    };
    let first = paper_stats_for_contract_loss_and_cost("0xdup1", "0xshared", "0xhonest", 90.0, 9.0);
    let second =
        paper_stats_for_contract_loss_and_cost("0xdup2", "0xshared", "0xhonest", 10.0, 1.0);

    let merged = merge_paper_stats([&first, &second], config);

    assert_eq!(merged.address_classification.malicious_address_count, 1);
    assert_eq!(merged.address_classification.honest_address_count, 1);
    assert_eq!(merged.address_classification.total_address_count, 2);
    assert_eq!(
        merged.attacker_cost.top_contract_contribution_ratio,
        Some(0.9)
    );
    let total_loss = &merged.honest_loss;
    assert_eq!(total_loss.top_contract_loss_contribution_ratio, Some(0.9));
    assert_eq!(total_loss.stuck_time_ratio, Some(2.0));
}

#[test]
fn paper_stats_merges_duplicate_scale_with_global_contract_token_deduplication() {
    let first = duplicate_scale_stats("0xdup", "1", 10);
    let second = duplicate_scale_stats("0xdup", "1", 12);

    let merged = merge_paper_stats([&first, &second], PaperStatsConfig::default());

    let token_uri = merged
        .duplicate_scale
        .iter()
        .find(|row| row.category == "token_uri")
        .unwrap();
    assert_eq!(token_uri.duplicate_nft_count, 1);
    assert_eq!(token_uri.duplicate_nft_ratio_denominator, 1);
    assert_eq!(token_uri.duplicate_contract_count, 1);
    assert_eq!(token_uri.duplicate_contract_ratio_denominator, 1);
    assert_eq!(merged.data_quality.suspected_duplicate_contract_count, 1);
    assert_eq!(merged.data_quality.infringing_nft_count, 1);
}

#[test]
fn paper_stats_merges_behavior_summary_with_global_address_deduplication() {
    let first = two_node_cycle_stats("0xcontract1");
    let second = two_node_cycle_stats("0xcontract1");

    let merged = merge_paper_stats([&first, &second], PaperStatsConfig::default());

    let wash = merged
        .malicious_behavior_summary
        .iter()
        .find(|row| row.behavior_type == "Wash Trading")
        .unwrap();
    assert_eq!(wash.instance_count, 2);
    assert_eq!(wash.address_count, 2);
    assert_eq!(wash.nft_count, 1);
}

fn duplicate_scale_stats(contract: &str, token_id: &str, seed_nft_count: i64) -> PaperStatsPayload {
    let seed_stats = SeedCollectionStatsPayload {
        seed_nft_count,
        ..SeedCollectionStatsPayload::default()
    };
    let duplicate_candidates = vec![DuplicateCandidate {
        contract_address: contract.into(),
        token_id: token_id.into(),
        match_reasons: vec!["token_uri_match".into()],
        ..DuplicateCandidate::default()
    }];
    let duplicate_contracts = vec![DuplicateContractPayload {
        contract_address: contract.into(),
        candidate_count: 1,
        ..DuplicateContractPayload::default()
    }];

    build_paper_stats(PaperStatsInput {
        config: PaperStatsConfig::default(),
        seed_collection_stats: &seed_stats,
        duplicate_candidates: &duplicate_candidates,
        duplicate_contracts: &duplicate_contracts,
        legit_duplicates: &[],
        infringing_tokens: &[],
        malicious_addresses: &[],
        victim_acquisition_addresses: &[],
        value_flow_edges: &[],
        nft_propagation_paths: &Default::default(),
    })
}

fn two_node_cycle_stats(contract: &str) -> PaperStatsPayload {
    let path = NftPropagationPathPayload {
        contract_address: contract.into(),
        summary: NftPropagationSummaryPayload {
            token_count: 1,
            first_block_time: 100,
            ..NftPropagationSummaryPayload::default()
        },
        nodes: BTreeMap::from([
            ("0xshareda".into(), NftPropagationNodePayload::default()),
            ("0xsharedb".into(), NftPropagationNodePayload::default()),
        ]),
        edges: vec![
            propagation_edge(
                (contract, "0xshareda", "0xsharedb"),
                "1",
                "sale",
                110,
                (Some(1.0), Some(2_000.0)),
            ),
            propagation_edge(
                (contract, "0xsharedb", "0xshareda"),
                "1",
                "sale",
                120,
                (Some(1.2), Some(2_400.0)),
            ),
        ],
        ..NftPropagationPathPayload::default()
    };

    build_paper_stats(PaperStatsInput {
        config: PaperStatsConfig::default(),
        seed_collection_stats: &SeedCollectionStatsPayload::default(),
        duplicate_candidates: &[],
        duplicate_contracts: &[],
        legit_duplicates: &[],
        infringing_tokens: &[],
        malicious_addresses: &[],
        victim_acquisition_addresses: &[],
        value_flow_edges: &[],
        nft_propagation_paths: &BTreeMap::from([(contract.into(), path)]),
    })
}

fn pump_exit_stats(contract: &str, buyer: &str) -> PaperStatsPayload {
    let path = NftPropagationPathPayload {
        contract_address: contract.into(),
        nodes: BTreeMap::from([
            ("0xa".into(), NftPropagationNodePayload::default()),
            ("0xb".into(), NftPropagationNodePayload::default()),
            (
                buyer.into(),
                NftPropagationNodePayload {
                    roles: vec!["victim_buyer".into()],
                    current_holding_token_count: 1,
                    is_stuck_victim: true,
                    ..NftPropagationNodePayload::default()
                },
            ),
        ]),
        edges: vec![
            propagation_edge(
                (contract, "0xa", "0xb"),
                "1",
                "sale",
                100,
                (Some(1.0), Some(2_000.0)),
            ),
            propagation_edge(
                (contract, "0xb", "0xa"),
                "1",
                "sale",
                110,
                (Some(1.2), Some(2_400.0)),
            ),
            propagation_edge(
                (contract, "0xa", buyer),
                "1",
                "sale",
                150,
                (Some(5.0), Some(10_000.0)),
            ),
        ],
        ..NftPropagationPathPayload::default()
    };
    let victim = VictimAcquisitionAddressPayload {
        address: buyer.into(),
        contract_addresses: vec![contract.into()],
        secondary_sale_count: 1,
        secondary_sale_stuck_cost_usd: 10_000.0,
        total_stuck_cost_usd: 10_000.0,
        is_stuck: true,
        ..VictimAcquisitionAddressPayload::default()
    };

    build_paper_stats(PaperStatsInput {
        config: PaperStatsConfig::default(),
        seed_collection_stats: &SeedCollectionStatsPayload::default(),
        duplicate_candidates: &[],
        duplicate_contracts: &[],
        legit_duplicates: &[],
        infringing_tokens: &[],
        malicious_addresses: &[],
        victim_acquisition_addresses: &[victim],
        value_flow_edges: &[],
        nft_propagation_paths: &BTreeMap::from([(contract.into(), path)]),
    })
}

fn paper_stats_for_contract_loss_and_cost(
    contract: &str,
    malicious: &str,
    honest: &str,
    loss_usd: f64,
    gas_usd: f64,
) -> PaperStatsPayload {
    let config = PaperStatsConfig {
        concentration_top_pct: 0.5,
        analysis_timestamp: 250,
        ..PaperStatsConfig::default()
    };
    let victim = VictimAcquisitionAddressPayload {
        address: honest.into(),
        contract_addresses: vec![contract.into()],
        secondary_sale_count: 1,
        secondary_sale_stuck_cost_usd: loss_usd,
        total_stuck_cost_usd: loss_usd,
        is_stuck: true,
        ..VictimAcquisitionAddressPayload::default()
    };
    let path = NftPropagationPathPayload {
        contract_address: contract.into(),
        summary: NftPropagationSummaryPayload {
            first_block_time: 100,
            ..NftPropagationSummaryPayload::default()
        },
        nodes: BTreeMap::from([
            (malicious.into(), NftPropagationNodePayload::default()),
            (
                honest.into(),
                NftPropagationNodePayload {
                    roles: vec!["victim_buyer".into()],
                    is_stuck_victim: true,
                    ..NftPropagationNodePayload::default()
                },
            ),
        ]),
        edges: vec![propagation_edge(
            (contract, malicious, honest),
            "1",
            "sale",
            150,
            (Some(loss_usd / 2_000.0), Some(loss_usd)),
        )],
        ..NftPropagationPathPayload::default()
    };
    build_paper_stats(PaperStatsInput {
        config,
        seed_collection_stats: &SeedCollectionStatsPayload::default(),
        duplicate_candidates: &[],
        duplicate_contracts: &[],
        legit_duplicates: &[],
        infringing_tokens: &[],
        malicious_addresses: &[MaliciousAddressPayload {
            address: malicious.into(),
            ..MaliciousAddressPayload::default()
        }],
        victim_acquisition_addresses: &[victim],
        value_flow_edges: &[ValueFlowEdgePayload {
            contract_address: contract.into(),
            from_address: malicious.into(),
            channel: "mint_payment".into(),
            value_usd: Some(0.0),
            value_with_gas_usd: Some(gas_usd),
            ..ValueFlowEdgePayload::default()
        }],
        nft_propagation_paths: &BTreeMap::from([(contract.into(), path)]),
    })
}

fn paper_stats_for_attacker_gas_transaction(
    contract: &str,
    malicious: &str,
    tx_hash: &str,
    gas_usd: f64,
) -> PaperStatsPayload {
    build_paper_stats(PaperStatsInput {
        config: PaperStatsConfig::default(),
        seed_collection_stats: &SeedCollectionStatsPayload::default(),
        duplicate_candidates: &[],
        duplicate_contracts: &[],
        legit_duplicates: &[],
        infringing_tokens: &[],
        malicious_addresses: &[MaliciousAddressPayload {
            address: malicious.into(),
            ..MaliciousAddressPayload::default()
        }],
        victim_acquisition_addresses: &[],
        value_flow_edges: &[ValueFlowEdgePayload {
            contract_address: contract.into(),
            from_address: malicious.into(),
            gas_payer_address: malicious.into(),
            tx_hash: tx_hash.into(),
            channel: "mint_payment".into(),
            from_role: "paid_minter".into(),
            gas_usd: Some(gas_usd),
            ..ValueFlowEdgePayload::default()
        }],
        nft_propagation_paths: &BTreeMap::new(),
    })
}

fn build_output_input_ratio_stats() -> PaperStatsPayload {
    let contracts = [
        duplicate_contract("0xprofit", 1),
        duplicate_contract("0xloss", 1),
        duplicate_contract("0xzero", 1),
    ];
    let value_flow_edges = vec![
        operator_output_edge("0xprofit", "0xprofit_mint", 100.0),
        attacker_input_edge("0xprofit", "0xprofit_deploy", 25.0),
        operator_output_edge("0xloss", "0xloss_mint", 10.0),
        attacker_input_edge("0xloss", "0xloss_deploy", 20.0),
        operator_output_edge("0xzero", "0xzero_mint", 0.0),
        attacker_input_edge("0xzero", "0xzero_deploy", 40.0),
    ];

    build_paper_stats(PaperStatsInput {
        config: PaperStatsConfig::default(),
        seed_collection_stats: &SeedCollectionStatsPayload::default(),
        duplicate_candidates: &[],
        duplicate_contracts: &contracts,
        legit_duplicates: &[],
        infringing_tokens: &[],
        malicious_addresses: &[],
        victim_acquisition_addresses: &[],
        value_flow_edges: &value_flow_edges,
        nft_propagation_paths: &BTreeMap::new(),
    })
}

fn output_input_ratio_stats_for_contract(
    contract: &str,
    output_usd: f64,
    input_usd: f64,
) -> PaperStatsPayload {
    let contracts = [duplicate_contract(contract, 1)];
    let value_flow_edges = vec![
        operator_output_edge(contract, &format!("{contract}_mint"), output_usd),
        attacker_input_edge(contract, &format!("{contract}_deploy"), input_usd),
    ];

    build_paper_stats(PaperStatsInput {
        config: PaperStatsConfig::default(),
        seed_collection_stats: &SeedCollectionStatsPayload::default(),
        duplicate_candidates: &[],
        duplicate_contracts: &contracts,
        legit_duplicates: &[],
        infringing_tokens: &[],
        malicious_addresses: &[],
        victim_acquisition_addresses: &[],
        value_flow_edges: &value_flow_edges,
        nft_propagation_paths: &BTreeMap::new(),
    })
}

fn operator_output_edge(contract: &str, tx_hash: &str, output_usd: f64) -> ValueFlowEdgePayload {
    ValueFlowEdgePayload {
        edge_id: format!("value:mint_payment:{tx_hash}:0xbuyer:{contract}"),
        contract_address: contract.into(),
        from_address: "0xbuyer".into(),
        to_address: contract.into(),
        tx_hash: tx_hash.into(),
        value_usd: Some(output_usd),
        channel: "mint_payment".into(),
        from_role: "paid_minter".into(),
        to_role: "mint_contract".into(),
        recipient_known: true,
        ..ValueFlowEdgePayload::default()
    }
}

fn attacker_input_edge(contract: &str, tx_hash: &str, gas_usd: f64) -> ValueFlowEdgePayload {
    ValueFlowEdgePayload {
        edge_id: format!("value:contract_deploy:{contract}:{tx_hash}"),
        contract_address: contract.into(),
        from_address: "0xoperator".into(),
        to_address: contract.into(),
        tx_hash: tx_hash.into(),
        gas_payer_address: "0xoperator".into(),
        gas_usd: Some(gas_usd),
        channel: "contract_deploy".into(),
        from_role: "contract_deployer".into(),
        to_role: "mint_contract".into(),
        ..ValueFlowEdgePayload::default()
    }
}

fn duplicate_contract(contract: &str, candidate_count: i64) -> DuplicateContractPayload {
    DuplicateContractPayload {
        contract_address: contract.into(),
        candidate_count,
        match_reasons: vec!["metadata_match".into()],
        ..DuplicateContractPayload::default()
    }
}

fn legacy_attacker_cost_without_details(contract: &str, gas_usd: f64) -> PaperStatsPayload {
    PaperStatsPayload {
        attacker_cost: top_contract_analysis_rs::models::PaperAttackerCostPayload {
            setup_gas_usd: gas_usd,
            total_gas_usd: gas_usd,
            top_contract_contribution_numerator: gas_usd,
            top_contract_contribution_denominator: gas_usd,
            top_contract_contribution_ratio: Some(1.0),
            ..top_contract_analysis_rs::models::PaperAttackerCostPayload::default()
        },
        attacker_cost_by_contract_usd: BTreeMap::from([(contract.into(), gas_usd)]),
        attacker_cost_details: vec![],
        ..PaperStatsPayload::default()
    }
}

fn propagation_edge(
    endpoints: (&str, &str, &str),
    token: &str,
    channel: &str,
    block_time: i64,
    prices: (Option<f64>, Option<f64>),
) -> NftPropagationEdgePayload {
    let (contract, from, to) = endpoints;
    let (price_eth, price_usd) = prices;
    NftPropagationEdgePayload {
        edge_id: format!("{contract}:{from}:{to}:{token}:{block_time}"),
        contract_address: contract.into(),
        token_id: token.into(),
        from_address: from.into(),
        to_address: to.into(),
        tx_hash: format!("0xtx{block_time}"),
        block_number: block_time,
        block_time,
        channel: channel.into(),
        price_eth,
        price_usd,
        aggregate_count: 1,
        token_ids: vec![token.into()],
        ..NftPropagationEdgePayload::default()
    }
}

fn cycle_edges(
    contract: &str,
    token: &str,
    addresses: &[&str],
    start_block_time: i64,
) -> Vec<NftPropagationEdgePayload> {
    addresses
        .iter()
        .enumerate()
        .map(|(index, from)| {
            let to = addresses[(index + 1) % addresses.len()];
            propagation_edge(
                (contract, from, to),
                token,
                "sale",
                start_block_time + index as i64,
                (Some(1.0), Some(2_000.0)),
            )
        })
        .collect()
}
