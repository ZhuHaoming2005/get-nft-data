use std::collections::BTreeMap;

use top_contract_analysis_rs::analysis::paper_stats::{
    build_paper_stats, merge_paper_stats, PaperStatsConfig, PaperStatsInput,
};
use top_contract_analysis_rs::models::{
    BatchSummaryPayload, DuplicateCandidate, DuplicateContractPayload, InfringingTokenRecord,
    MaliciousAddressPayload, NftPropagationEdgePayload, NftPropagationNodePayload,
    NftPropagationPathPayload, NftPropagationSummaryPayload, PaperContractBehaviorStatsPayload,
    PaperDataQualityPayload, PaperDuplicateScaleRowPayload, PaperHonestBuyerRowPayload,
    PaperPumpExitRowPayload, PaperStatsPayload, PaperWashTradingRowPayload,
    SeedCollectionStatsPayload, SingleReportPayload, ValueFlowEdgePayload,
    VictimAcquisitionAddressPayload,
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
    assert_eq!(total.duplicate_contract_count, 2);

    let token_uri = stats
        .duplicate_scale
        .iter()
        .find(|row| row.category == "token_uri")
        .unwrap();
    assert_eq!(token_uri.duplicate_nft_count, 3);
    assert_eq!(token_uri.duplicate_contract_count, 1);

    let quality = &stats.data_quality;
    assert_eq!(quality.representative_candidate_count, 2);
    assert_eq!(quality.candidate_contract_count, 2);
    assert_eq!(quality.suspected_duplicate_contract_count, 2);
    assert_eq!(quality.infringing_nft_count, 5);
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
            honest_buyers_top: vec![PaperHonestBuyerRowPayload {
                honest_buyer: "0xbuyer".into(),
                total_paid_usd: 10.0,
                source_pattern: "Pump-and-Exit".into(),
                still_holding: true,
                ..PaperHonestBuyerRowPayload::default()
            }],
            ..PaperContractBehaviorStatsPayload::default()
        }],
        ..PaperStatsPayload::default()
    };
    let batch_markdown = render_batch_human_readable_report(&BatchSummaryPayload {
        paper_stats,
        ..BatchSummaryPayload::default()
    });

    assert!(batch_markdown.contains("## 合约行为明细"));
    assert!(batch_markdown.contains("contract_address"));
    assert!(batch_markdown.contains("### 诚实买家"));
    assert!(!batch_markdown.contains("### 诚实买家 Top"));
    assert!(!batch_markdown.contains("chain_id"));
    assert!(!batch_markdown.contains("seed_contract_address"));
    assert!(!batch_markdown.contains("copy_contract_address"));
    assert!(batch_markdown.contains("| 0xcopy | 1 | 0 | 0 | 0 | 0 | 1 |"));
    assert!(batch_markdown.contains("| 0xcopy | 0xbuyer | Pump-and-Exit |"));
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
                honest_buyers_top: vec![PaperHonestBuyerRowPayload {
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
                honest_buyers_top: vec![PaperHonestBuyerRowPayload {
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

    let markdown = render_batch_human_readable_report(&BatchSummaryPayload {
        paper_stats,
        ..BatchSummaryPayload::default()
    });

    assert!(markdown.find("## 数据质量").unwrap() < markdown.find("## 合约行为明细").unwrap());
    assert!(
        markdown.find("| 0xhigh |").unwrap() < markdown.find("| 0xlow |").unwrap(),
        "contract behavior rows should be sorted by descending impact"
    );
    assert!(
        markdown.find("| 0xhigh | 0xbigbuyer |").unwrap()
            < markdown.find("| 0xlow | 0xsmallbuyer |").unwrap(),
        "honest buyer rows should be sorted by descending paid USD"
    );
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
    assert_eq!(token_uri.duplicate_nft_ratio_denominator, 10);
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

    let total_loss = stats
        .honest_loss
        .iter()
        .find(|row| row.category == "total")
        .unwrap();
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
    assert_eq!(contract_stats.honest_buyers_top[0].honest_buyer, "0xhonest");
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
        contract_stats.honest_buyers_top[0].source_pattern,
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
    assert_eq!(contract_stats.honest_buyers_top.len(), 11);
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
        contract_stats.honest_buyers_top[0].source_pattern,
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
    let total_loss = merged
        .honest_loss
        .iter()
        .find(|row| row.category == "total")
        .unwrap();
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
    assert_eq!(token_uri.duplicate_nft_ratio_denominator, 22);
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
            channel: "mint_payment".into(),
            value_usd: Some(0.0),
            value_with_gas_usd: Some(gas_usd),
            ..ValueFlowEdgePayload::default()
        }],
        nft_propagation_paths: &BTreeMap::from([(contract.into(), path)]),
    })
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
