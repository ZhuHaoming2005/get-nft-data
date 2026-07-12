use super::*;
use crate::models::EthTransferRecord;
use crate::models::{AddressEvidencePayload, NftTokenPropagationPayload};

#[test]
fn missing_mint_pre_balance_quality_count_deduplicates_transaction_wallet_pairs() {
    let missing = ValueFlowEdgePayload {
        channel: "mint_payment".into(),
        tx_hash: "signature".into(),
        from_address: "wallet".into(),
        from_before_eth_balance: None,
        ..ValueFlowEdgePayload::default()
    };
    let available = ValueFlowEdgePayload {
        tx_hash: "other".into(),
        from_before_eth_balance: Some(1.0),
        ..missing.clone()
    };

    assert_eq!(
        count_missing_mint_pre_balances(&[missing.clone(), missing, available]),
        1
    );
}

#[test]
fn seed_duplicate_matching_uses_single_contract_level_name() {
    let seed_contract = ContractMetadata {
        chain: "ethereum".into(),
        contract_address: "0xseed".into(),
        name: "Azuki".into(),
        symbol: "AZUKI".into(),
        ..ContractMetadata::default()
    };
    let seed_nfts = vec![
        SeedNft {
            token_id: "1".into(),
            name: "Azuki #1".into(),
            metadata_json: r#"{"name":"Azuki #1"}"#.into(),
            ..SeedNft::default()
        },
        SeedNft {
            token_id: "2".into(),
            name: "Azuki #2".into(),
            metadata_json: r#"{"name":"Azuki #2"}"#.into(),
            ..SeedNft::default()
        },
    ];

    let dedup_seed_nfts = seed_nfts_for_duplicate_matching(&seed_nfts, &seed_contract);
    let non_empty_names = dedup_seed_nfts
        .iter()
        .filter(|item| !item.name.is_empty())
        .map(|item| item.name.as_str())
        .collect::<Vec<_>>();

    assert_eq!(non_empty_names, vec!["Azuki"]);
    assert_eq!(dedup_seed_nfts[0].metadata_json, seed_nfts[0].metadata_json);
    assert_eq!(dedup_seed_nfts[1].token_id, "2");

    let name_only_seed_nfts = seed_nfts_for_duplicate_matching(&[], &seed_contract);
    assert_eq!(name_only_seed_nfts.len(), 1);
    assert_eq!(name_only_seed_nfts[0].contract_address, "0xseed");
    assert_eq!(name_only_seed_nfts[0].name, "Azuki");
}

#[test]
fn victim_acquisition_separates_secondary_sale_and_paid_mint_costs() {
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

    assert_eq!(victim_acquisition_addresses.len(), 2);
    assert_eq!(
        victim_acquisition_addresses
            .iter()
            .map(|item| item.secondary_sale_cost_eth)
            .sum::<f64>(),
        0.5
    );
    assert_eq!(
        victim_acquisition_addresses
            .iter()
            .map(|item| item.secondary_sale_stuck_cost_eth)
            .sum::<f64>(),
        0.25
    );
    assert_eq!(
        victim_acquisition_addresses
            .iter()
            .map(|item| item.paid_mint_cost_eth)
            .sum::<f64>(),
        2.0
    );
    assert_eq!(
        victim_acquisition_addresses
            .iter()
            .map(|item| item.paid_mint_cost_usd)
            .sum::<f64>(),
        4_000.0
    );
    assert_eq!(
        victim_acquisition_addresses
            .iter()
            .map(|item| item.paid_mint_token_count)
            .sum::<i64>(),
        2
    );
    assert_eq!(
        victim_acquisition_addresses
            .iter()
            .map(|item| item.paid_mint_stuck_cost_eth)
            .sum::<f64>(),
        1.0
    );
    assert_eq!(
        victim_acquisition_addresses
            .iter()
            .map(|item| item.paid_mint_stuck_cost_usd)
            .sum::<f64>(),
        2_000.0
    );
}

#[test]
fn victim_acquisition_ratio_uses_total_cost_for_all_acquisition_channels() {
    let secondary_sale_victim_addresses = vec![SecondarySaleVictimAddressPayload {
        contract_address: "0xdup".into(),
        address: "0xvictim".into(),
        buy_tx_hashes: vec!["0xbuy".into()],
        buy_amount_eth: 4.0,
        buy_amount_usd: 4_000.0,
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
        from_before_eth_balance: Some(10.0),
        from_before_usd_balance: Some(10_000.0),
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

    let buy_ratios: Vec<f64> = victim_acquisition_addresses
        .iter()
        .filter_map(|item| item.buy_asset_ratio)
        .collect();
    assert_eq!(buy_ratios.len(), 1);
    assert_eq!(buy_ratios.iter().filter(|value| **value > 0.6).count(), 1);
}

#[test]
fn victim_acquisition_ratio_with_gas_uses_paid_mint_gas_delta() {
    let secondary_sale_victim_addresses = vec![SecondarySaleVictimAddressPayload {
        contract_address: "0xdup".into(),
        address: "0xvictim".into(),
        buy_tx_hashes: vec!["0xbuy".into()],
        buy_amount_eth: 4.0,
        buy_amount_usd: 4_000.0,
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
        value_with_gas_eth: Some(3.5),
        value_with_gas_usd: Some(3_500.0),
        from_before_eth_balance: Some(10.0),
        from_before_usd_balance: Some(10_000.0),
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
fn victim_acquisition_ratio_with_gas_excludes_third_party_paid_gas() {
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
        value_with_gas_eth: Some(3.5),
        value_with_gas_usd: Some(3_500.0),
        gas_payer_address: "0xrelayer".into(),
        gas_eth: Some(0.5),
        gas_usd: Some(500.0),
        from_before_eth_balance: Some(10.0),
        from_before_usd_balance: Some(10_000.0),
        channel: "mint_payment".into(),
        ..ValueFlowEdgePayload::default()
    }];

    let rows = build_victim_acquisition_addresses(
        &[],
        &address_attributions,
        &value_flow_edges,
        &BTreeMap::new(),
    );

    assert_eq!(rows[0].buy_asset_ratio, Some(0.3));
    assert_eq!(rows[0].buy_asset_ratio_with_gas, Some(0.3));
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
    assert_eq!(victim_acquisition_addresses[0].buy_asset_ratio, Some(0.7));
    assert_eq!(
        victim_acquisition_addresses
            .iter()
            .filter(|item| item.buy_asset_ratio.is_some())
            .count(),
        1
    );
    assert_eq!(
        victim_acquisition_addresses
            .iter()
            .filter_map(|item| item.buy_asset_ratio)
            .filter(|value| *value > 0.6)
            .count(),
        1
    );
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
