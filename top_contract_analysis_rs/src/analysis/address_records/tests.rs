use super::*;

fn sale(symbol: &str, amount_eth_equivalent: f64) -> NftSaleRecord {
    NftSaleRecord {
        contract_address: "0xdup".into(),
        token_id: "1".into(),
        tx_hash: format!("0x{symbol}"),
        block_number: 10,
        log_index: 1,
        buyer_address: format!("0xbuyer{symbol}"),
        seller_address: "0xseller".into(),
        payment_token_symbol: symbol.into(),
        price_eth: Some(amount_eth_equivalent),
        price_usd: Some(amount_eth_equivalent),
        is_native_eth: symbol == "ETH",
        ..NftSaleRecord::default()
    }
}

fn transfer(
    tx_hash: &str,
    block_time: i64,
    from_address: &str,
    to_address: &str,
) -> TransferRecord {
    transfer_token("1", tx_hash, block_time, from_address, to_address)
}

fn transfer_token(
    token_id: &str,
    tx_hash: &str,
    block_time: i64,
    from_address: &str,
    to_address: &str,
) -> TransferRecord {
    TransferRecord {
        contract_address: "0xdup".into(),
        token_id: token_id.into(),
        tx_hash: tx_hash.into(),
        block_number: block_time,
        block_time,
        from_address: from_address.into(),
        to_address: to_address.into(),
        event_type: "erc721".into(),
        source: "test".into(),
        ..TransferRecord::default()
    }
}

fn infringing_token() -> InfringingTokenRecord {
    infringing_token_minted_by("0xoperator")
}

fn infringing_token_minted_by(minter_address: &str) -> InfringingTokenRecord {
    infringing_token_id_minted_by("1", minter_address)
}

fn infringing_token_id_minted_by(token_id: &str, minter_address: &str) -> InfringingTokenRecord {
    InfringingTokenRecord {
        contract_address: "0xdup".into(),
        token_id: token_id.into(),
        minter_address: minter_address.into(),
        ..InfringingTokenRecord::default()
    }
}

fn operator_address() -> MaliciousAddressPayload {
    MaliciousAddressPayload {
        address: "0xoperator".into(),
        mint_activity_observed: true,
        ..MaliciousAddressPayload::default()
    }
}

fn sale_between(
    tx_hash: &str,
    block_time: i64,
    seller_address: &str,
    buyer_address: &str,
    price_eth: f64,
) -> NftSaleRecord {
    sale_between_token(
        "1",
        tx_hash,
        block_time,
        seller_address,
        buyer_address,
        price_eth,
    )
}

fn sale_between_token(
    token_id: &str,
    tx_hash: &str,
    block_time: i64,
    seller_address: &str,
    buyer_address: &str,
    price_eth: f64,
) -> NftSaleRecord {
    NftSaleRecord {
        contract_address: "0xdup".into(),
        token_id: token_id.into(),
        tx_hash: tx_hash.into(),
        block_number: block_time,
        log_index: 0,
        buyer_address: buyer_address.into(),
        seller_address: seller_address.into(),
        payment_token_symbol: "ETH".into(),
        price_eth: Some(price_eth),
        price_usd: Some(price_eth),
        is_native_eth: true,
        ..NftSaleRecord::default()
    }
}

fn mint_payment_edge(from_address: &str) -> ValueFlowEdgePayload {
    ValueFlowEdgePayload {
        contract_address: "0xdup".into(),
        from_address: from_address.into(),
        to_address: "0xdup".into(),
        tx_hash: "0xmint".into(),
        block_number: 100,
        block_time: 100,
        token_id: "1".into(),
        value_eth: Some(0.08),
        value_usd: Some(160.0),
        channel: "mint_payment".into(),
        evidence_type: "same_tx_mint_payment".into(),
        ..ValueFlowEdgePayload::default()
    }
}

#[test]
fn victim_records_do_not_count_eth_amounts_as_usd_when_rate_is_missing() {
    let mut eth_sale = sale("ETH", 1.25);
    eth_sale.price_usd = None;
    let owners = vec![OwnerBalance {
        owner_address: "0xbuyerETH".into(),
        token_balances: BTreeMap::from([("1".into(), 1)]),
    }];
    let sales = vec![eth_sale];
    let activity = prepare_contract_activity(&[], &sales, &owners);

    let victims = build_secondary_sale_victim_address_records_from_activity("0xdup", &activity);

    assert_eq!(victims.len(), 1);
    assert_eq!(victims[0].buy_amount_eth, 1.25);
    assert_eq!(victims[0].buy_amount_usd, 0.0);
    assert_eq!(victims[0].last_buy_amount_eth, Some(1.25));
    assert_eq!(victims[0].last_buy_amount_usd, None);
}

#[test]
fn victim_records_include_stablecoin_eth_equivalent_amounts() {
    let sales = vec![sale("USDT", 0.1)];
    let owners = vec![OwnerBalance {
        owner_address: "0xbuyerUSDT".into(),
        token_balances: BTreeMap::from([("1".into(), 1)]),
    }];
    let activity = prepare_contract_activity(&[], &sales, &owners);

    let victims = build_secondary_sale_victim_address_records_from_activity("0xdup", &activity);

    assert_eq!(victims.len(), 1);
    assert_eq!(victims[0].buy_amount_eth, 0.1);
    assert_eq!(victims[0].buy_amount_usd, 0.1);
    assert_eq!(victims[0].last_buy_amount_eth, Some(0.1));
    assert_eq!(victims[0].last_buy_amount_usd, Some(0.1));
    assert!(victims[0].is_stuck);
}

#[test]
fn victim_records_exclude_malicious_secondary_sale_buyers() {
    let sales = vec![
        sale_between("0xvictim_buy", 120, "0xminter", "0xvictim", 1.0),
        sale_between("0xwash_buy", 130, "0xvictim", "0xoperator", 10.0),
    ];
    let owners = vec![
        OwnerBalance {
            owner_address: "0xvictim".into(),
            token_balances: BTreeMap::from([("1".into(), 1)]),
        },
        OwnerBalance {
            owner_address: "0xoperator".into(),
            token_balances: BTreeMap::from([("1".into(), 1)]),
        },
    ];
    let activity = prepare_contract_activity(&[], &sales, &owners);

    let victims = build_secondary_sale_victim_address_records_excluding_malicious_from_activity(
        "0xdup",
        &activity,
        &[MaliciousAddressPayload {
            address: "0xoperator".into(),
            wash_cycle_count: 1,
            ..MaliciousAddressPayload::default()
        }],
    );

    assert_eq!(victims.len(), 1);
    assert_eq!(victims[0].address, "0xvictim");
    assert_eq!(victims[0].buy_amount_eth, 1.0);
}

#[test]
fn honest_records_do_not_mark_free_transfer_after_purchase_as_corrupted() {
    let transfers = vec![
        transfer("0xmint", 100, ZERO_ADDRESS, "0xoperator"),
        transfer("0xbuy", 120, "0xoperator", "0xvictim"),
        transfer("0xgift", 140, "0xvictim", "0xrecipient"),
    ];
    let sales = vec![sale_between("0xbuy", 120, "0xoperator", "0xvictim", 1.0)];
    let owners = vec![OwnerBalance {
        owner_address: "0xrecipient".into(),
        token_balances: BTreeMap::from([("1".into(), 1)]),
    }];

    let rows = build_honest_address_records(HonestAddressRecordInput {
        contract_address: "0xdup",
        transfers: &transfers,
        sales: &sales,
        owners: &owners,
        infringing_tokens: &[infringing_token()],
        malicious_addresses: &[operator_address()],
        mint_payment_edges: &[],
        deployment_time: 90,
        analysis_timestamp: 200,
    });
    let victim = rows
        .iter()
        .find(|row| row.address == "0xvictim")
        .expect("victim row");

    assert!(!victim.is_corrupted_address);
    assert_eq!(victim.victim_resale_count, 0);
    assert_eq!(
        rows.iter().filter(|row| row.is_corrupted_address).count(),
        0
    );
}

#[test]
fn honest_records_mark_paid_resale_after_purchase_as_corrupted() {
    let transfers = vec![
        transfer("0xmint", 100, ZERO_ADDRESS, "0xoperator"),
        transfer("0xbuy", 120, "0xoperator", "0xvictim"),
        transfer("0xresale", 140, "0xvictim", "0xrecipient"),
    ];
    let sales = vec![
        sale_between("0xbuy", 120, "0xoperator", "0xvictim", 1.0),
        sale_between("0xresale", 140, "0xvictim", "0xrecipient", 0.4),
    ];
    let owners = vec![OwnerBalance {
        owner_address: "0xrecipient".into(),
        token_balances: BTreeMap::from([("1".into(), 1)]),
    }];

    let rows = build_honest_address_records(HonestAddressRecordInput {
        contract_address: "0xdup",
        transfers: &transfers,
        sales: &sales,
        owners: &owners,
        infringing_tokens: &[infringing_token()],
        malicious_addresses: &[operator_address()],
        mint_payment_edges: &[],
        deployment_time: 90,
        analysis_timestamp: 200,
    });
    let victim = rows
        .iter()
        .find(|row| row.address == "0xvictim")
        .expect("victim row");

    assert!(victim.is_corrupted_address);
    assert_eq!(victim.victim_resale_count, 1);
    assert_eq!(
        rows.iter().filter(|row| row.is_corrupted_address).count(),
        1
    );
}

#[test]
fn honest_records_mark_paid_mint_resale_as_corrupted() {
    let transfers = vec![
        transfer("0xmint", 100, ZERO_ADDRESS, "0xvictim"),
        transfer("0xresale", 140, "0xvictim", "0xrecipient"),
    ];
    let sales = vec![sale_between(
        "0xresale",
        140,
        "0xvictim",
        "0xrecipient",
        0.4,
    )];
    let owners = vec![OwnerBalance {
        owner_address: "0xrecipient".into(),
        token_balances: BTreeMap::from([("1".into(), 1)]),
    }];

    let rows = build_honest_address_records(HonestAddressRecordInput {
        contract_address: "0xdup",
        transfers: &transfers,
        sales: &sales,
        owners: &owners,
        infringing_tokens: &[infringing_token()],
        malicious_addresses: &[operator_address()],
        mint_payment_edges: &[mint_payment_edge("0xvictim")],
        deployment_time: 90,
        analysis_timestamp: 200,
    });
    let victim = rows
        .iter()
        .find(|row| row.address == "0xvictim")
        .expect("paid minter resale row");

    assert!(victim.is_corrupted_address);
    assert_eq!(victim.victim_resale_count, 1);
}

#[test]
fn paid_mint_reseller_is_not_malicious_when_only_weak_mint_sale_behavior() {
    let transfers = vec![
        transfer("0xmint", 100, ZERO_ADDRESS, "0xvictim"),
        transfer("0xresale", 140, "0xvictim", "0xrecipient"),
    ];
    let sales = vec![sale_between(
        "0xresale",
        140,
        "0xvictim",
        "0xrecipient",
        0.4,
    )];
    let owners = vec![OwnerBalance {
        owner_address: "0xrecipient".into(),
        token_balances: BTreeMap::from([("1".into(), 1)]),
    }];
    let mint_payment_edges = vec![mint_payment_edge("0xvictim")];
    let infringing_tokens = vec![infringing_token_minted_by("0xvictim")];
    let activity = prepare_contract_activity(&transfers, &sales, &owners);

    let malicious = build_malicious_address_records_from_activity(
        "0xdup",
        &activity,
        &infringing_tokens,
        &mint_payment_edges,
    );

    assert!(malicious.iter().all(|row| row.address != "0xvictim"));
    let honest = build_honest_address_records_from_activity(
        "0xdup",
        &activity,
        &infringing_tokens,
        &malicious,
        &mint_payment_edges,
        90,
        200,
    );
    let victim = honest
        .iter()
        .find(|row| row.address == "0xvictim")
        .expect("paid mint reseller remains honest");
    assert!(victim.is_corrupted_address);
}

#[test]
fn secondary_market_reseller_is_not_malicious_when_only_weak_rapid_sale_behavior() {
    let transfers = vec![
        transfer("0xmint", 100, ZERO_ADDRESS, "0xoperator"),
        transfer("0xbuy", 110, "0xoperator", "0xvictim"),
        transfer("0xresale", 120, "0xvictim", "0xrecipient"),
    ];
    let sales = vec![
        sale_between("0xbuy", 110, "0xoperator", "0xvictim", 1.0),
        sale_between("0xresale", 120, "0xvictim", "0xrecipient", 0.4),
    ];
    let owners = vec![OwnerBalance {
        owner_address: "0xrecipient".into(),
        token_balances: BTreeMap::from([("1".into(), 1)]),
    }];
    let infringing_tokens = vec![infringing_token()];
    let activity = prepare_contract_activity(&transfers, &sales, &owners);

    let malicious =
        build_malicious_address_records_from_activity("0xdup", &activity, &infringing_tokens, &[]);

    assert!(malicious.iter().all(|row| row.address != "0xvictim"));
    let honest = build_honest_address_records_from_activity(
        "0xdup",
        &activity,
        &infringing_tokens,
        &malicious,
        &[],
        90,
        200,
    );
    let victim = honest
        .iter()
        .find(|row| row.address == "0xvictim")
        .expect("secondary market reseller remains honest");
    assert!(victim.is_corrupted_address);
}

#[test]
fn paid_mint_high_volume_reseller_remains_malicious() {
    let transfers = vec![
        transfer("0xmint1", 100, ZERO_ADDRESS, "0xoperator"),
        transfer("0xresale1", 120, "0xoperator", "0xrecipient1"),
        transfer("0xresale2", 130, "0xoperator", "0xrecipient2"),
        transfer("0xresale3", 140, "0xoperator", "0xrecipient3"),
    ];
    let sales = vec![
        sale_between("0xresale1", 120, "0xoperator", "0xrecipient1", 0.4),
        sale_between("0xresale2", 130, "0xoperator", "0xrecipient2", 0.4),
        sale_between("0xresale3", 140, "0xoperator", "0xrecipient3", 0.4),
    ];
    let owners = Vec::<OwnerBalance>::new();
    let mint_payment_edges = vec![mint_payment_edge("0xoperator")];
    let infringing_tokens = vec![infringing_token_minted_by("0xoperator")];
    let activity = prepare_contract_activity(&transfers, &sales, &owners);

    let malicious = build_malicious_address_records_from_activity(
        "0xdup",
        &activity,
        &infringing_tokens,
        &mint_payment_edges,
    );

    assert!(malicious.iter().any(|row| row.address == "0xoperator"));
}

#[test]
fn aggregation_receiver_is_malicious_signal() {
    let transfers = vec![
        transfer_token("1", "0xmint1", 100, ZERO_ADDRESS, "0xsource1"),
        transfer_token("2", "0xmint2", 100, ZERO_ADDRESS, "0xsource2"),
        transfer_token("3", "0xmint3", 100, ZERO_ADDRESS, "0xsource3"),
        transfer_token("1", "0xagg1", 120, "0xsource1", "0xcollector"),
        transfer_token("2", "0xagg2", 121, "0xsource2", "0xcollector"),
        transfer_token("3", "0xagg3", 122, "0xsource3", "0xcollector"),
    ];
    let infringing_tokens = vec![
        infringing_token_id_minted_by("1", "0xsource1"),
        infringing_token_id_minted_by("2", "0xsource2"),
        infringing_token_id_minted_by("3", "0xsource3"),
    ];
    let activity = prepare_contract_activity(&transfers, &[], &[]);

    let malicious =
        build_malicious_address_records_from_activity("0xdup", &activity, &infringing_tokens, &[]);

    assert!(malicious.iter().all(|row| row.address != "0xcollector"));
}

#[test]
fn wash_cycle_operator_levels_use_count_and_value_thresholds() {
    let low_transfers = vec![
        transfer("0xmint", 100, ZERO_ADDRESS, "0xoperator"),
        transfer("0xout", 110, "0xoperator", "0xpeer"),
        transfer("0xback", 120, "0xpeer", "0xoperator"),
    ];
    let low_sales = vec![sale_between("0xout", 115, "0xoperator", "0xpeer", 0.1)];
    let low_activity = prepare_contract_activity(&low_transfers, &low_sales, &[]);

    let low_rows = build_malicious_address_records_from_activity(
        "0xdup",
        &low_activity,
        &[infringing_token()],
        &[],
    );
    let low_operator = low_rows
        .iter()
        .find(|row| row.address == "0xoperator")
        .expect("low wash-cycle operator");
    assert_eq!(low_operator.operator_level, 1);
    assert_eq!(
        low_operator.operator_level_label,
        "weak_behavioral_operator"
    );
    assert_eq!(low_operator.wash_cycle_propagation_count, 1);

    let unrelated_value_edges = vec![ValueFlowEdgePayload {
        contract_address: "0xdup".into(),
        from_address: "0xoperator".into(),
        to_address: "0xdup".into(),
        tx_hash: "0xunrelated_mint_payment".into(),
        token_id: "1".into(),
        value_eth: Some(2.0),
        value_usd: Some(2_000.0),
        channel: "mint_payment".into(),
        ..ValueFlowEdgePayload::default()
    }];
    let unrelated_value_rows = build_malicious_address_records_from_activity(
        "0xdup",
        &low_activity,
        &[infringing_token()],
        &unrelated_value_edges,
    );
    let unrelated_value_operator = unrelated_value_rows
        .iter()
        .find(|row| row.address == "0xoperator")
        .expect("wash-cycle operator with unrelated value flow");
    assert_eq!(unrelated_value_operator.operator_level, 1);
    assert_eq!(unrelated_value_operator.wash_cycle_value_usd, 0.1);

    let count_transfers = vec![
        transfer_token("1", "0xmint1", 100, ZERO_ADDRESS, "0xoperator"),
        transfer_token("1", "0xout1", 110, "0xoperator", "0xpeer1"),
        transfer_token("1", "0xback1", 120, "0xpeer1", "0xoperator"),
        transfer_token("2", "0xmint2", 100, ZERO_ADDRESS, "0xoperator"),
        transfer_token("2", "0xout2", 130, "0xoperator", "0xpeer2"),
        transfer_token("2", "0xback2", 140, "0xpeer2", "0xoperator"),
        transfer_token("3", "0xmint3", 100, ZERO_ADDRESS, "0xoperator"),
        transfer_token("3", "0xout3", 150, "0xoperator", "0xpeer3"),
        transfer_token("3", "0xback3", 160, "0xpeer3", "0xoperator"),
    ];
    let count_activity = prepare_contract_activity(&count_transfers, &[], &[]);
    let count_rows = build_malicious_address_records_from_activity(
        "0xdup",
        &count_activity,
        &[
            infringing_token_id_minted_by("1", "0xoperator"),
            infringing_token_id_minted_by("2", "0xoperator"),
            infringing_token_id_minted_by("3", "0xoperator"),
        ],
        &[],
    );
    let count_operator = count_rows
        .iter()
        .find(|row| row.address == "0xoperator")
        .expect("count-threshold wash-cycle operator");
    assert_eq!(count_operator.operator_level, 2);
    assert_eq!(
        count_operator.operator_level_label,
        "likely_behavioral_operator"
    );
    assert_eq!(count_operator.wash_cycle_propagation_count, 3);

    let value_transfers = vec![
        transfer("0xmint", 100, ZERO_ADDRESS, "0xoperator"),
        transfer("0xout", 110, "0xoperator", "0xpeer"),
        transfer("0xback", 120, "0xpeer", "0xoperator"),
    ];
    let value_sales = vec![sale_between("0xout", 115, "0xoperator", "0xpeer", 1_200.0)];
    let value_activity = prepare_contract_activity(&value_transfers, &value_sales, &[]);
    let value_rows = build_malicious_address_records_from_activity(
        "0xdup",
        &value_activity,
        &[infringing_token()],
        &[],
    );
    let value_operator = value_rows
        .iter()
        .find(|row| row.address == "0xoperator")
        .expect("value-threshold wash-cycle operator");
    assert_eq!(value_operator.operator_level, 2);
    assert_eq!(value_operator.wash_cycle_value_usd, 1_200.0);

    let eth_fallback_transfers = vec![
        transfer("0xmint", 100, ZERO_ADDRESS, "0xoperator"),
        transfer("0xout", 110, "0xoperator", "0xpeer"),
        transfer("0xback", 120, "0xpeer", "0xoperator"),
    ];
    let mut eth_fallback_sale = sale_between("0xout", 115, "0xoperator", "0xpeer", 0.6);
    eth_fallback_sale.price_usd = None;
    let eth_fallback_sales = vec![eth_fallback_sale];
    let eth_fallback_activity =
        prepare_contract_activity(&eth_fallback_transfers, &eth_fallback_sales, &[]);
    let eth_fallback_rows = build_malicious_address_records_from_activity(
        "0xdup",
        &eth_fallback_activity,
        &[infringing_token()],
        &[],
    );
    let eth_fallback_operator = eth_fallback_rows
        .iter()
        .find(|row| row.address == "0xoperator")
        .expect("eth-fallback wash-cycle operator");
    assert_eq!(eth_fallback_operator.operator_level, 2);
    assert_eq!(eth_fallback_operator.wash_cycle_value_eth, 0.6);
    assert_eq!(eth_fallback_operator.wash_cycle_value_usd, 0.0);
}

#[test]
fn paid_multi_purchase_buyer_is_not_aggregation_operator() {
    let transfers = vec![
        transfer_token("1", "0xmint1", 100, ZERO_ADDRESS, "0xseller1"),
        transfer_token("2", "0xmint2", 100, ZERO_ADDRESS, "0xseller2"),
        transfer_token("3", "0xmint3", 100, ZERO_ADDRESS, "0xseller3"),
        transfer_token("1", "0xbuy1", 120, "0xseller1", "0xbuyer"),
        transfer_token("2", "0xbuy2", 121, "0xseller2", "0xbuyer"),
        transfer_token("3", "0xbuy3", 122, "0xseller3", "0xbuyer"),
    ];
    let sales = vec![
        sale_between_token("1", "0xbuy1", 120, "0xseller1", "0xbuyer", 0.4),
        sale_between_token("2", "0xbuy2", 121, "0xseller2", "0xbuyer", 0.5),
        sale_between_token("3", "0xbuy3", 122, "0xseller3", "0xbuyer", 0.6),
    ];
    let infringing_tokens = vec![
        infringing_token_id_minted_by("1", "0xseller1"),
        infringing_token_id_minted_by("2", "0xseller2"),
        infringing_token_id_minted_by("3", "0xseller3"),
    ];
    let activity = prepare_contract_activity(&transfers, &sales, &[]);

    let malicious =
        build_malicious_address_records_from_activity("0xdup", &activity, &infringing_tokens, &[]);

    assert!(malicious.iter().all(|row| row.address != "0xbuyer"));
}

#[test]
fn withdrawal_recipient_is_malicious_signal() {
    let activity = prepare_contract_activity(&[], &[], &[]);
    let mint_payment_edges = vec![ValueFlowEdgePayload {
        contract_address: "0xdup".into(),
        from_address: "0xdup".into(),
        to_address: "0xoperator".into(),
        tx_hash: "0xwithdraw".into(),
        block_number: 120,
        block_time: 120,
        token_id: "1".into(),
        value_eth: Some(0.5),
        channel: "withdrawal".into(),
        to_role: "external_wallet".into(),
        evidence_flags: vec!["same_tx_contract_withdrawal".into()],
        ..ValueFlowEdgePayload::default()
    }];

    let malicious = build_malicious_address_records_from_activity(
        "0xdup",
        &activity,
        &[infringing_token()],
        &mint_payment_edges,
    );
    let operator = malicious
        .iter()
        .find(|row| row.address == "0xoperator")
        .expect("withdrawal recipient malicious signal");

    assert_eq!(operator.withdrawal_edge_count, 1);
    assert_eq!(operator.cashout_edge_count, 0);
    assert_eq!(operator.operator_level, 3);
    assert_eq!(
        operator.operator_level_label,
        "strong_value_control_operator"
    );
}

#[test]
fn attribution_records_explain_withdrawal_operator_level() {
    let malicious = vec![MaliciousAddressPayload {
        address: "0xoperator".into(),
        withdrawal_edge_count: 1,
        operator_level: 3,
        operator_level_label: "strong_value_control_operator".into(),
        evidence_contracts: vec!["0xdup".into()],
        ..MaliciousAddressPayload::default()
    }];

    let rows = build_address_attribution_records("0xdup", &[], &[], &[], &malicious, &[], &[]);
    let operator = rows
        .iter()
        .find(|row| row.address == "0xoperator")
        .expect("operator attribution");

    assert_eq!(operator.attribution_label, "suspected_operator");
    assert_eq!(operator.operator_level, 3);
    assert_eq!(
        operator.operator_level_label,
        "strong_value_control_operator"
    );
    assert!(operator
        .evidence
        .iter()
        .any(|evidence| evidence.evidence_type == "contract_value_withdrawal"));
}

#[test]
fn honest_records_do_not_mark_malicious_paid_mint_resale_as_corrupted() {
    let transfers = vec![
        transfer("0xmint", 100, ZERO_ADDRESS, "0xoperator"),
        transfer("0xresale", 140, "0xoperator", "0xrecipient"),
    ];
    let sales = vec![sale_between(
        "0xresale",
        140,
        "0xoperator",
        "0xrecipient",
        0.4,
    )];
    let owners = vec![OwnerBalance {
        owner_address: "0xrecipient".into(),
        token_balances: BTreeMap::from([("1".into(), 1)]),
    }];

    let rows = build_honest_address_records(HonestAddressRecordInput {
        contract_address: "0xdup",
        transfers: &transfers,
        sales: &sales,
        owners: &owners,
        infringing_tokens: &[infringing_token()],
        malicious_addresses: &[operator_address()],
        mint_payment_edges: &[mint_payment_edge("0xoperator")],
        deployment_time: 90,
        analysis_timestamp: 200,
    });

    assert!(rows.iter().all(|row| row.address != "0xoperator"));
    assert_eq!(
        rows.iter().filter(|row| row.is_corrupted_address).count(),
        0
    );
}

#[test]
fn acquisition_exposure_evidence_uses_total_victim_acquisition_ratio() {
    let rows = add_acquisition_exposure_attribution_evidence(
        vec![AddressAttributionPayload {
            contract_address: "0xdup".into(),
            address: "0xvictim".into(),
            attribution_label: "likely_victim".into(),
            victim_score: 0.45,
            confidence: "medium".into(),
            ..AddressAttributionPayload::default()
        }],
        &[VictimAcquisitionAddressPayload {
            address: "0xvictim".into(),
            contract_addresses: vec!["0xdup".into()],
            tx_hashes: vec!["0xbuy".into(), "0xmint".into()],
            buy_asset_ratio: Some(0.7),
            ..VictimAcquisitionAddressPayload::default()
        }],
    );

    let victim = rows
        .iter()
        .find(|item| item.address == "0xvictim")
        .expect("victim attribution");
    assert!(victim
        .observed_roles
        .iter()
        .any(|role| role == "high_exposure_acquirer"));
    assert!(victim
        .evidence
        .iter()
        .any(|evidence| evidence.evidence_type == "high_acquisition_balance_ratio"));
}
