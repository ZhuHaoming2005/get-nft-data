use super::*;
use std::collections::BTreeMap;

#[test]
fn lifecycle_metric_pre_sale_signals_exclude_victim_outcome() {
    let seed_contract = SeedContractPayload {
        contract_address: "0xseed".into(),
        ..SeedContractPayload::default()
    };
    let lifecycle_events = vec![
        ContractLifecycleEventPayload {
            contract_address: "0xdup".into(),
            lifecycle_stage: "replica_deployment".into(),
            event_type: "candidate_contract_deployed".into(),
            block_time: 80,
            ..ContractLifecycleEventPayload::default()
        },
        ContractLifecycleEventPayload {
            contract_address: "0xdup".into(),
            lifecycle_stage: "replica_mint".into(),
            event_type: "mint".into(),
            block_time: 100,
            ..ContractLifecycleEventPayload::default()
        },
        ContractLifecycleEventPayload {
            contract_address: "0xdup".into(),
            lifecycle_stage: "victimization".into(),
            event_type: "secondary_sale_victim_acquisition".into(),
            block_time: 150,
            ..ContractLifecycleEventPayload::default()
        },
    ];

    let metrics = build_lifecycle_metrics(
        &seed_contract,
        &BTreeMap::new(),
        &[],
        &[],
        &lifecycle_events,
    );
    let metric = metrics
        .iter()
        .find(|metric| metric.contract_address == "0xdup")
        .expect("contract metric");

    assert_eq!(metric.first_sale_time, 0);
    assert_eq!(metric.deployment_time, 80);
    assert_eq!(metric.first_victim_time, 150);
    assert_eq!(metric.pre_sale_signal_count, 2);
    assert!(metric.early_detection_positive);
}

#[test]
fn lifecycle_metric_uses_paid_mint_as_victim_outcome_time() {
    let seed_contract = SeedContractPayload {
        contract_address: "0xseed".into(),
        ..SeedContractPayload::default()
    };
    let lifecycle_events = vec![
        ContractLifecycleEventPayload {
            contract_address: "0xdup".into(),
            lifecycle_stage: "replica_deployment".into(),
            event_type: "candidate_contract_deployed".into(),
            block_time: 80,
            ..ContractLifecycleEventPayload::default()
        },
        ContractLifecycleEventPayload {
            contract_address: "0xdup".into(),
            lifecycle_stage: "replica_mint".into(),
            event_type: "mint".into(),
            block_time: 100,
            ..ContractLifecycleEventPayload::default()
        },
    ];
    let value_flow_edges = vec![ValueFlowEdgePayload {
        contract_address: "0xdup".into(),
        block_time: 120,
        channel: "mint_payment".into(),
        value_eth: Some(0.08),
        ..ValueFlowEdgePayload::default()
    }];

    let metrics = build_lifecycle_metrics(
        &seed_contract,
        &BTreeMap::new(),
        &[],
        &value_flow_edges,
        &lifecycle_events,
    );
    let metric = metrics
        .iter()
        .find(|metric| metric.contract_address == "0xdup")
        .expect("contract metric");

    assert_eq!(metric.first_sale_time, 0);
    assert_eq!(metric.deployment_time, 80);
    assert_eq!(metric.first_victim_time, 120);
    assert_eq!(metric.time_to_first_victim_seconds, Some(40));
    assert_eq!(metric.pre_sale_signal_count, 2);

    let rows = build_early_detection_features(&metrics, &lifecycle_events, &value_flow_edges);
    let first_window = rows
        .iter()
        .find(|row| row.observation_window_seconds == 60)
        .expect("first window");
    assert_eq!(
        first_window.weak_label,
        "positive_observed_sale_or_victimization"
    );
}

#[test]
fn early_detection_window_pre_sale_signals_exclude_victim_outcome() {
    let metrics = vec![ContractLifecycleMetricPayload {
        contract_address: "0xdup".into(),
        deployment_time: 80,
        first_mint_time: 100,
        first_sale_time: 0,
        first_victim_time: 150,
        ..ContractLifecycleMetricPayload::default()
    }];
    let lifecycle_events = vec![
        ContractLifecycleEventPayload {
            contract_address: "0xdup".into(),
            lifecycle_stage: "replica_deployment".into(),
            event_type: "candidate_contract_deployed".into(),
            block_time: 80,
            ..ContractLifecycleEventPayload::default()
        },
        ContractLifecycleEventPayload {
            contract_address: "0xdup".into(),
            lifecycle_stage: "replica_mint".into(),
            event_type: "mint".into(),
            block_time: 100,
            ..ContractLifecycleEventPayload::default()
        },
        ContractLifecycleEventPayload {
            contract_address: "0xdup".into(),
            lifecycle_stage: "victimization".into(),
            event_type: "secondary_sale_victim_acquisition".into(),
            block_time: 150,
            ..ContractLifecycleEventPayload::default()
        },
    ];

    let rows = build_early_detection_features(&metrics, &lifecycle_events, &[]);
    let first_window = rows
        .iter()
        .find(|row| row.observation_window_seconds == 60)
        .expect("first window");

    assert_eq!(first_window.window_start_time, 80);
    assert_eq!(first_window.victim_signal_count, 0);
    assert_eq!(first_window.pre_sale_signal_count, 2);
    let long_window = rows
        .iter()
        .find(|row| row.observation_window_seconds == 3_600)
        .expect("long window");
    assert_eq!(long_window.victim_signal_count, 1);
    assert_eq!(
        long_window.weak_label,
        "positive_observed_sale_or_victimization"
    );
}

#[test]
fn lifecycle_metric_treats_sale_as_first_transfer_when_no_distribution_edge_exists() {
    let seed_contract = SeedContractPayload {
        contract_address: "0xseed".into(),
        ..SeedContractPayload::default()
    };
    let lifecycle_events = vec![
        ContractLifecycleEventPayload {
            contract_address: "0xdup".into(),
            lifecycle_stage: "replica_deployment".into(),
            block_time: 80,
            ..ContractLifecycleEventPayload::default()
        },
        ContractLifecycleEventPayload {
            contract_address: "0xdup".into(),
            lifecycle_stage: "monetization".into(),
            event_type: "sale".into(),
            block_time: 150,
            ..ContractLifecycleEventPayload::default()
        },
    ];

    let metrics = build_lifecycle_metrics(
        &seed_contract,
        &BTreeMap::new(),
        &[],
        &[],
        &lifecycle_events,
    );
    let metric = metrics
        .iter()
        .find(|item| item.contract_address == "0xdup")
        .expect("contract metric");

    assert_eq!(metric.first_transfer_time, 150);
    assert_eq!(metric.time_to_first_transfer_seconds, Some(70));
}
