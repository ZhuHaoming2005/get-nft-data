use super::*;
use crate::models::{AddressEvidencePayload, NftTokenPropagationPayload};
use crate::models::{ContractLifecycleMetricPayload, EthTransferRecord};

fn payload_median_deployment_to_neutral_holder_seconds(
    payload: &SingleReportPayload,
) -> Option<f64> {
    let values: Vec<f64> = payload
        .honest_addresses
        .iter()
        .flat_map(|item| {
            item.deployment_to_neutral_holder_seconds_samples
                .iter()
                .copied()
        })
        .filter_map(positive_seconds)
        .collect();
    median_f64(&values)
}

fn payload_median_deployment_to_first_transfer_seconds(
    payload: &SingleReportPayload,
) -> Option<f64> {
    let values: Vec<f64> = payload
        .lifecycle_metrics
        .iter()
        .filter_map(|metric| metric.time_to_first_transfer_seconds)
        .filter_map(positive_seconds)
        .collect();
    median_f64(&values)
}

#[test]
fn report_summary_uses_deployment_to_first_transfer_samples() {
    let lifecycle_metrics = vec![
        ContractLifecycleMetricPayload {
            contract_address: "0xdeployonly".into(),
            time_to_first_transfer_seconds: Some(0),
            ..ContractLifecycleMetricPayload::default()
        },
        ContractLifecycleMetricPayload {
            contract_address: "0xfast".into(),
            time_to_first_transfer_seconds: Some(8),
            ..ContractLifecycleMetricPayload::default()
        },
        ContractLifecycleMetricPayload {
            contract_address: "0xslow".into(),
            time_to_first_transfer_seconds: Some(20),
            ..ContractLifecycleMetricPayload::default()
        },
    ];

    let summary = build_report_summary(ReportSummaryInput {
        open_license: false,
        grouped: &BTreeMap::new(),
        implausible_candidate_contract_count: 0,
        legit_duplicates: &[],
        infringing_tokens: &[],
        malicious_addresses: &[],
        honest_addresses: &[],
        secondary_sale_victim_addresses: &[],
        victim_acquisition_addresses: &[],
        address_signals: &BTreeMap::new(),
        address_attributions: &[],
        value_flow_edges: &[],
        propagation_paths: &BTreeMap::new(),
        lifecycle_metrics: &lifecycle_metrics,
    });

    assert_eq!(summary.avg_deployment_to_first_transfer_seconds, Some(14.0));
    assert_eq!(
        summary.median_deployment_to_first_transfer_seconds,
        Some(14.0)
    );
}

#[test]
fn report_summary_ignores_zero_deployment_to_neutral_holder_samples() {
    let honest_addresses = vec![
        HonestAddressPayload {
            address: "0xmintvictim".into(),
            deployment_to_neutral_holder_seconds_samples: vec![0],
            ..HonestAddressPayload::default()
        },
        HonestAddressPayload {
            address: "0xpropagated1".into(),
            deployment_to_neutral_holder_seconds_samples: vec![12],
            ..HonestAddressPayload::default()
        },
        HonestAddressPayload {
            address: "0xpropagated2".into(),
            deployment_to_neutral_holder_seconds_samples: vec![20],
            ..HonestAddressPayload::default()
        },
    ];

    let summary = build_report_summary(ReportSummaryInput {
        open_license: false,
        grouped: &BTreeMap::new(),
        implausible_candidate_contract_count: 0,
        legit_duplicates: &[],
        infringing_tokens: &[],
        malicious_addresses: &[],
        honest_addresses: &honest_addresses,
        secondary_sale_victim_addresses: &[],
        victim_acquisition_addresses: &[],
        address_signals: &BTreeMap::new(),
        address_attributions: &[],
        value_flow_edges: &[],
        propagation_paths: &BTreeMap::new(),
        lifecycle_metrics: &[],
    });

    assert_eq!(summary.avg_deployment_to_neutral_holder_seconds, Some(16.0));
    assert_eq!(
        summary.median_deployment_to_neutral_holder_seconds,
        Some(16.0)
    );
}

#[test]
fn report_summary_tracks_corrupted_address_holding_duration_stats() {
    let honest_addresses = vec![
        HonestAddressPayload {
            address: "0xcorrupted-fast".into(),
            is_corrupted_address: true,
            hold_duration_median_seconds: Some(12.0),
            ..HonestAddressPayload::default()
        },
        HonestAddressPayload {
            address: "0xcorrupted-slow".into(),
            is_corrupted_address: true,
            hold_duration_median_seconds: Some(30.0),
            ..HonestAddressPayload::default()
        },
        HonestAddressPayload {
            address: "0xvictim-no-duration".into(),
            is_corrupted_address: true,
            hold_duration_median_seconds: None,
            ..HonestAddressPayload::default()
        },
        HonestAddressPayload {
            address: "0xplain-victim".into(),
            is_corrupted_address: false,
            hold_duration_median_seconds: Some(100.0),
            ..HonestAddressPayload::default()
        },
    ];

    let summary = build_report_summary(ReportSummaryInput {
        open_license: false,
        grouped: &BTreeMap::new(),
        implausible_candidate_contract_count: 0,
        legit_duplicates: &[],
        infringing_tokens: &[],
        malicious_addresses: &[],
        honest_addresses: &honest_addresses,
        secondary_sale_victim_addresses: &[],
        victim_acquisition_addresses: &[],
        address_signals: &BTreeMap::new(),
        address_attributions: &[],
        value_flow_edges: &[],
        propagation_paths: &BTreeMap::new(),
        lifecycle_metrics: &[],
    });

    assert_eq!(summary.corrupted_victim_address_count, 3);
    assert_eq!(summary.avg_corrupted_address_holding_seconds, Some(21.0));
    assert_eq!(summary.median_corrupted_address_holding_seconds, Some(21.0));
}

#[test]
fn cached_payload_median_ignores_zero_deployment_to_first_transfer_samples() {
    let payload = SingleReportPayload {
        lifecycle_metrics: vec![
            ContractLifecycleMetricPayload {
                time_to_first_transfer_seconds: Some(0),
                ..ContractLifecycleMetricPayload::default()
            },
            ContractLifecycleMetricPayload {
                time_to_first_transfer_seconds: Some(12),
                ..ContractLifecycleMetricPayload::default()
            },
        ],
        ..SingleReportPayload::default()
    };

    assert_eq!(
        payload_median_deployment_to_first_transfer_seconds(&payload),
        Some(12.0)
    );
}

#[test]
fn cached_payload_median_ignores_zero_deployment_to_neutral_holder_samples() {
    let payload = SingleReportPayload {
        honest_addresses: vec![
            HonestAddressPayload {
                deployment_to_neutral_holder_seconds_samples: vec![0],
                ..HonestAddressPayload::default()
            },
            HonestAddressPayload {
                deployment_to_neutral_holder_seconds_samples: vec![12, 20],
                ..HonestAddressPayload::default()
            },
        ],
        ..SingleReportPayload::default()
    };

    assert_eq!(
        payload_median_deployment_to_neutral_holder_seconds(&payload),
        Some(16.0)
    );
}

#[test]
fn report_summary_separates_secondary_sale_and_paid_mint_victim_costs() {
    let secondary_sale_victim_addresses = vec![SecondarySaleVictimAddressPayload {
        contract_address: "0xdup".into(),
        address: "0xsalevictim".into(),
        buy_amount_eth: 0.5,
        buy_amount_usd: 1_000.0,
        last_buy_amount_eth: Some(0.25),
        last_buy_amount_usd: Some(500.0),
        is_stuck: true,
        ..SecondarySaleVictimAddressPayload::default()
    }];
    let address_attributions = vec![AddressAttributionPayload {
        contract_address: "0xdup".into(),
        address: "0xpaidvictim".into(),
        attribution_label: "likely_victim".into(),
        victim_score: 0.45,
        evidence: vec![AddressEvidencePayload {
            evidence_type: "paid_mint_payment".into(),
            contract_address: "0xdup".into(),
            token_id: "1,2".into(),
            tx_hash: "0xmint".into(),
            weight: 0.45,
            detail: "paid mint victim evidence".into(),
        }],
        ..AddressAttributionPayload::default()
    }];
    let value_flow_edges = vec![ValueFlowEdgePayload {
        edge_id: "value:mint_payment:0xmint".into(),
        contract_address: "0xdup".into(),
        from_address: "0xpaidvictim".into(),
        to_address: "0xdup".into(),
        tx_hash: "0xmint".into(),
        token_id: "1,2".into(),
        value_eth: Some(2.0),
        value_usd: Some(4_000.0),
        payment_token_symbol: "ETH".into(),
        channel: "mint_payment".into(),
        to_role: "mint_contract".into(),
        ..ValueFlowEdgePayload::default()
    }];
    let propagation_paths = BTreeMap::from([(
        "0xdup".into(),
        NftPropagationPathPayload {
            contract_address: "0xdup".into(),
            token_paths: vec![
                NftTokenPropagationPayload {
                    token_id: "1".into(),
                    current_holder_addresses: vec!["0xpaidvictim".into()],
                    ..NftTokenPropagationPayload::default()
                },
                NftTokenPropagationPayload {
                    token_id: "2".into(),
                    current_holder_addresses: vec!["0xother".into()],
                    ..NftTokenPropagationPayload::default()
                },
            ],
            ..NftPropagationPathPayload::default()
        },
    )]);
    let victim_acquisition_addresses = build_victim_acquisition_addresses(
        &secondary_sale_victim_addresses,
        &address_attributions,
        &value_flow_edges,
        &propagation_paths,
    );

    let summary = build_report_summary(ReportSummaryInput {
        open_license: false,
        grouped: &BTreeMap::new(),
        implausible_candidate_contract_count: 0,
        legit_duplicates: &[],
        infringing_tokens: &[],
        malicious_addresses: &[],
        honest_addresses: &[],
        secondary_sale_victim_addresses: &secondary_sale_victim_addresses,
        victim_acquisition_addresses: &victim_acquisition_addresses,
        address_signals: &BTreeMap::new(),
        address_attributions: &address_attributions,
        value_flow_edges: &value_flow_edges,
        propagation_paths: &propagation_paths,
        lifecycle_metrics: &[],
    });

    assert_eq!(summary.secondary_sale_victim_cost_eth, 0.5);
    assert_eq!(summary.secondary_sale_stuck_cost_eth, 0.25);
    assert_eq!(summary.paid_mint_victim_cost_eth, 2.0);
    assert_eq!(summary.paid_mint_victim_cost_usd, 4_000.0);
    assert_eq!(summary.paid_mint_stuck_cost_eth, 1.0);
    assert_eq!(summary.paid_mint_stuck_cost_usd, 2_000.0);
    assert_eq!(summary.victim_acquisition_total_eth, 2.5);
    assert_eq!(summary.victim_acquisition_stuck_cost_eth, 1.25);
    assert_eq!(summary.victim_acquisition_address_count, 2);
}

#[test]
fn victim_acquisition_ratio_uses_total_cost_for_all_acquisition_channels() {
    let secondary_sale_victim_addresses = vec![SecondarySaleVictimAddressPayload {
        contract_address: "0xdup".into(),
        address: "0xvictim".into(),
        buy_tx_hashes: vec!["0xbuy".into()],
        buy_amount_eth: 4.0,
        buy_amount_usd: 4_000.0,
        buy_before_eth_balance: Some(10.0),
        buy_before_usd_balance: Some(10_000.0),
        buy_asset_ratio: Some(0.4),
        ..SecondarySaleVictimAddressPayload::default()
    }];
    let address_attributions = vec![AddressAttributionPayload {
        contract_address: "0xdup".into(),
        address: "0xvictim".into(),
        attribution_label: "likely_victim".into(),
        evidence: vec![AddressEvidencePayload {
            evidence_type: "paid_mint_payment".into(),
            contract_address: "0xdup".into(),
            token_id: "1".into(),
            tx_hash: "0xmint".into(),
            weight: 0.45,
            detail: "paid mint victim evidence".into(),
        }],
        ..AddressAttributionPayload::default()
    }];
    let value_flow_edges = vec![ValueFlowEdgePayload {
        edge_id: "value:mint_payment:0xmint".into(),
        contract_address: "0xdup".into(),
        from_address: "0xvictim".into(),
        tx_hash: "0xmint".into(),
        token_id: "1".into(),
        value_eth: Some(3.0),
        value_usd: Some(3_000.0),
        channel: "mint_payment".into(),
        ..ValueFlowEdgePayload::default()
    }];
    let victim_acquisition_addresses = build_victim_acquisition_addresses(
        &secondary_sale_victim_addresses,
        &address_attributions,
        &value_flow_edges,
        &BTreeMap::new(),
    );

    assert_eq!(victim_acquisition_addresses.len(), 1);
    assert_eq!(
        victim_acquisition_addresses[0].total_acquisition_cost_eth,
        7.0
    );
    assert_eq!(victim_acquisition_addresses[0].buy_asset_ratio, Some(0.7));

    let summary = build_report_summary(ReportSummaryInput {
        open_license: false,
        grouped: &BTreeMap::new(),
        implausible_candidate_contract_count: 0,
        legit_duplicates: &[],
        infringing_tokens: &[],
        malicious_addresses: &[],
        honest_addresses: &[],
        secondary_sale_victim_addresses: &secondary_sale_victim_addresses,
        victim_acquisition_addresses: &victim_acquisition_addresses,
        address_signals: &BTreeMap::new(),
        address_attributions: &address_attributions,
        value_flow_edges: &value_flow_edges,
        propagation_paths: &BTreeMap::new(),
        lifecycle_metrics: &[],
    });

    assert_eq!(summary.buy_asset_ratio_known_address_count, 1);
    assert_eq!(summary.ratio_over_60_address_count, 1);
    assert_eq!(summary.ratio_over_60_address_ratio, Some(1.0));
}

#[test]
fn victim_acquisition_ratio_with_gas_preserves_secondary_sale_gas_delta() {
    let secondary_sale_victim_addresses = vec![SecondarySaleVictimAddressPayload {
        contract_address: "0xdup".into(),
        address: "0xvictim".into(),
        buy_tx_hashes: vec!["0xbuy".into()],
        buy_amount_eth: 4.0,
        buy_amount_usd: 4_000.0,
        buy_before_eth_balance: Some(10.0),
        buy_before_usd_balance: Some(10_000.0),
        buy_asset_ratio: Some(0.4),
        buy_asset_ratio_with_gas: Some(0.45),
        ..SecondarySaleVictimAddressPayload::default()
    }];
    let address_attributions = vec![AddressAttributionPayload {
        contract_address: "0xdup".into(),
        address: "0xvictim".into(),
        attribution_label: "likely_victim".into(),
        evidence: vec![AddressEvidencePayload {
            evidence_type: "paid_mint_payment".into(),
            contract_address: "0xdup".into(),
            token_id: "1".into(),
            tx_hash: "0xmint".into(),
            weight: 0.45,
            detail: "paid mint victim evidence".into(),
        }],
        ..AddressAttributionPayload::default()
    }];
    let value_flow_edges = vec![ValueFlowEdgePayload {
        edge_id: "value:mint_payment:0xmint".into(),
        contract_address: "0xdup".into(),
        from_address: "0xvictim".into(),
        tx_hash: "0xmint".into(),
        token_id: "1".into(),
        value_eth: Some(3.0),
        value_usd: Some(3_000.0),
        channel: "mint_payment".into(),
        ..ValueFlowEdgePayload::default()
    }];

    let rows = build_victim_acquisition_addresses(
        &secondary_sale_victim_addresses,
        &address_attributions,
        &value_flow_edges,
        &BTreeMap::new(),
    );

    assert_eq!(rows[0].buy_asset_ratio, Some(0.7));
    assert_eq!(rows[0].buy_asset_ratio_with_gas, Some(0.75));
}

#[test]
fn paid_mint_only_victim_ratio_uses_observed_pre_mint_eth_balance() {
    let address_attributions = vec![AddressAttributionPayload {
        contract_address: "0xdup".into(),
        address: "0xvictim".into(),
        attribution_label: "likely_victim".into(),
        evidence: vec![AddressEvidencePayload {
            evidence_type: "paid_mint_payment".into(),
            contract_address: "0xdup".into(),
            token_id: "1".into(),
            tx_hash: "0xmint".into(),
            weight: 0.45,
            detail: "paid mint victim evidence".into(),
        }],
        ..AddressAttributionPayload::default()
    }];
    let value_flow_edges = vec![ValueFlowEdgePayload {
        edge_id: "value:mint_payment:0xmint".into(),
        contract_address: "0xdup".into(),
        from_address: "0xvictim".into(),
        tx_hash: "0xmint".into(),
        token_id: "1".into(),
        value_eth: Some(7.0),
        value_usd: Some(7_000.0),
        from_before_eth_balance: Some(10.0),
        from_before_usd_balance: Some(10_000.0),
        channel: "mint_payment".into(),
        ..ValueFlowEdgePayload::default()
    }];

    let victim_acquisition_addresses = build_victim_acquisition_addresses(
        &[],
        &address_attributions,
        &value_flow_edges,
        &BTreeMap::new(),
    );
    let summary = build_report_summary(ReportSummaryInput {
        open_license: false,
        grouped: &BTreeMap::new(),
        implausible_candidate_contract_count: 0,
        legit_duplicates: &[],
        infringing_tokens: &[],
        malicious_addresses: &[],
        honest_addresses: &[],
        secondary_sale_victim_addresses: &[],
        victim_acquisition_addresses: &victim_acquisition_addresses,
        address_signals: &BTreeMap::new(),
        address_attributions: &address_attributions,
        value_flow_edges: &value_flow_edges,
        propagation_paths: &BTreeMap::new(),
        lifecycle_metrics: &[],
    });

    assert_eq!(victim_acquisition_addresses[0].buy_asset_ratio, Some(0.7));
    assert_eq!(summary.buy_asset_ratio_known_address_count, 1);
    assert_eq!(summary.ratio_over_60_address_count, 1);
}

#[test]
fn mint_value_flow_does_not_classify_erc20_same_tx_transfers_to_minter_as_funding() {
    let lookup = MintPaymentLookup {
        tx_hash: "0xmint".into(),
        block_number: 100,
        block_time: 1_700_000_000,
        minter_address: "0xpaidvictim".into(),
        token_ids: vec!["1".into()],
    };
    let erc20_transfer = EthTransferRecord {
        tx_hash: "0xmint".into(),
        block_number: 100,
        from_address: "0xrouter".into(),
        to_address: "0xpaidvictim".into(),
        value_eth: 134.0,
        value_usd: Some(300_000.0),
        payment_token_symbol: "WETH".into(),
        category: "erc20".into(),
        ..EthTransferRecord::default()
    };
    let native_transfer = EthTransferRecord {
        category: "external".into(),
        value_eth: 0.5,
        value_usd: Some(1_000.0),
        ..erc20_transfer.clone()
    };

    assert!(classify_mint_value_flow_transfer(&erc20_transfer, &lookup, "0xdup", None).is_none());
    let classified =
        classify_mint_value_flow_transfer(&native_transfer, &lookup, "0xdup", None).unwrap();
    assert_eq!(classified.0, "funding");
}
