use super::*;

pub fn build_malicious_address_records(
    contract_address: &str,
    transfers: &[TransferRecord],
    infringing_tokens: &[InfringingTokenRecord],
    mint_payment_edges: &[ValueFlowEdgePayload],
) -> Vec<MaliciousAddressPayload> {
    let activity =
        prepare_contract_activity(transfers, &[] as &[NftSaleRecord], &[] as &[OwnerBalance]);
    build_malicious_address_records_from_activity(
        contract_address,
        &activity,
        infringing_tokens,
        mint_payment_edges,
    )
}

pub(crate) fn build_malicious_address_records_from_activity(
    contract_address: &str,
    activity: &PreparedContractActivity<'_>,
    infringing_tokens: &[InfringingTokenRecord],
    mint_payment_edges: &[ValueFlowEdgePayload],
) -> Vec<MaliciousAddressPayload> {
    let relevant_token_ids: HashSet<String> = infringing_tokens
        .iter()
        .filter(|item| !item.token_id.is_empty())
        .map(|item| item.token_id.clone())
        .collect();
    let mint_addresses: HashSet<String> = infringing_tokens
        .iter()
        .filter(|item| !item.minter_address.is_empty())
        .map(|item| item.minter_address.clone())
        .collect();

    let mut outgoing: HashMap<String, HashSet<String>> = HashMap::new();
    let mut incoming: HashMap<String, HashSet<String>> = HashMap::new();
    let mut cycle_counts: HashMap<String, i64> = HashMap::new();
    let mut wash_cycle_scopes: HashMap<String, WashCycleScope> = HashMap::new();
    let mut sale_seller_counts: HashMap<String, i64> = HashMap::new();
    let mut seen_pairs: HashSet<(String, String)> = HashSet::new();
    let mut seen_transfer_scopes_by_pair: HashMap<(String, String), Vec<WashCycleTransferScope>> =
        HashMap::new();
    let mut rapid_addresses: HashSet<String> = HashSet::new();
    let mut rapid_token_ids_by_address: HashMap<String, HashSet<String>> = HashMap::new();
    let mut mint_times: HashMap<String, i64> = HashMap::new();
    let mut receiver_candidates: HashSet<String> = HashSet::new();
    let paid_mint_payers: HashSet<String> = mint_payment_edges
        .iter()
        .filter(|edge| {
            edge.channel == "mint_payment"
                && identities_equal(&edge.contract_address, contract_address)
                && value_flow_has_positive_value(edge)
        })
        .map(|edge| edge.from_address.clone())
        .filter(|address| !address.is_empty())
        .collect();
    let mut secondary_market_resellers: HashSet<String> = HashSet::new();
    let mut prior_paid_buyers_by_token: HashMap<String, HashSet<String>> = HashMap::new();
    let mut paid_acquisition_addresses: HashSet<String> = paid_mint_payers.clone();
    let sale_transfer_keys: HashSet<(String, String, String, String)> = activity
        .sorted_sales
        .iter()
        .filter(|sale| {
            sale_has_positive_value(sale)
                && (relevant_token_ids.is_empty() || relevant_token_ids.contains(&sale.token_id))
        })
        .map(|sale| {
            nft_transfer_key(
                &sale.token_id,
                &sale.tx_hash,
                &sale.seller_address,
                &sale.buyer_address,
            )
        })
        .collect();

    for transfer in &activity.sorted_transfers {
        if !relevant_token_ids.is_empty() && !relevant_token_ids.contains(&transfer.token_id) {
            continue;
        }
        if !transfer.to_address.is_empty() {
            receiver_candidates.insert(transfer.to_address.clone());
        }
        if transfer.from_address == ZERO_ADDRESS {
            if !transfer.to_address.is_empty() {
                mint_times.insert(transfer.token_id.clone(), transfer.block_time);
            }
            continue;
        }
        if !transfer.from_address.is_empty() && !transfer.to_address.is_empty() {
            outgoing
                .entry(transfer.from_address.clone())
                .or_default()
                .insert(transfer.to_address.clone());
            let transfer_key = nft_transfer_key(
                &transfer.token_id,
                &transfer.tx_hash,
                &transfer.from_address,
                &transfer.to_address,
            );
            if !sale_transfer_keys.contains(&transfer_key) {
                incoming
                    .entry(transfer.to_address.clone())
                    .or_default()
                    .insert(transfer.from_address.clone());
            }
            let pair = (transfer.from_address.clone(), transfer.to_address.clone());
            let reverse = (transfer.to_address.clone(), transfer.from_address.clone());
            let transfer_scope = WashCycleTransferScope::from_transfer(transfer);
            if seen_pairs.contains(&reverse) {
                *cycle_counts
                    .entry(transfer.from_address.clone())
                    .or_insert(0) += 1;
                *cycle_counts.entry(transfer.to_address.clone()).or_insert(0) += 1;
                add_wash_cycle_transfer_scope(
                    &mut wash_cycle_scopes,
                    &transfer.from_address,
                    &transfer_scope,
                );
                add_wash_cycle_transfer_scope(
                    &mut wash_cycle_scopes,
                    &transfer.to_address,
                    &transfer_scope,
                );
                if let Some(reverse_scopes) = seen_transfer_scopes_by_pair.get(&reverse) {
                    for reverse_scope in reverse_scopes {
                        add_wash_cycle_transfer_scope(
                            &mut wash_cycle_scopes,
                            &transfer.from_address,
                            reverse_scope,
                        );
                        add_wash_cycle_transfer_scope(
                            &mut wash_cycle_scopes,
                            &transfer.to_address,
                            reverse_scope,
                        );
                    }
                }
            }
            seen_pairs.insert(pair.clone());
            seen_transfer_scopes_by_pair
                .entry(pair)
                .or_default()
                .push(transfer_scope);
        }
        let mint_time = *mint_times.get(&transfer.token_id).unwrap_or(&0);
        if mint_time > 0
            && transfer.block_time > 0
            && transfer.block_time - mint_time <= 24 * 3600
            && !transfer.from_address.is_empty()
        {
            rapid_addresses.insert(transfer.from_address.clone());
            rapid_token_ids_by_address
                .entry(transfer.from_address.clone())
                .or_default()
                .insert(transfer.token_id.clone());
        }
    }

    for sale in &activity.sorted_sales {
        if !relevant_token_ids.is_empty() && !relevant_token_ids.contains(&sale.token_id) {
            continue;
        }
        if sale_has_positive_value(sale) {
            let prior_paid_buyers = prior_paid_buyers_by_token
                .entry(sale.token_id.clone())
                .or_default();
            if prior_paid_buyers.contains(&sale.seller_address) {
                secondary_market_resellers.insert(sale.seller_address.clone());
            }
            if !sale.buyer_address.is_empty() {
                paid_acquisition_addresses.insert(sale.buyer_address.clone());
                prior_paid_buyers.insert(sale.buyer_address.clone());
            }
        }
        if !sale.seller_address.is_empty() {
            *sale_seller_counts
                .entry(sale.seller_address.clone())
                .or_insert(0) += 1;
        }
        if !sale.buyer_address.is_empty() {
            receiver_candidates.insert(sale.buyer_address.clone());
        }
    }

    let mut withdrawal_edge_counts: HashMap<String, i64> = HashMap::new();
    let mut cashout_edge_counts: HashMap<String, i64> = HashMap::new();
    for edge in mint_payment_edges {
        if !identities_equal(&edge.contract_address, contract_address)
            || !value_flow_has_positive_value(edge)
        {
            continue;
        }
        match edge.channel.as_str() {
            "withdrawal" => {
                if let Some(address) =
                    value_flow_operator_address(&edge.to_address, contract_address, &edge.to_role)
                {
                    *withdrawal_edge_counts.entry(address).or_insert(0) += 1;
                }
            }
            "cashout_hop" => {
                if let Some(address) = value_flow_operator_address(
                    &edge.from_address,
                    contract_address,
                    &edge.from_role,
                ) {
                    *cashout_edge_counts.entry(address).or_insert(0) += 1;
                }
                if let Some(address) =
                    value_flow_operator_address(&edge.to_address, contract_address, &edge.to_role)
                {
                    *cashout_edge_counts.entry(address).or_insert(0) += 1;
                }
            }
            _ => {}
        }
    }

    let mut candidate_addresses: Vec<String> = outgoing
        .keys()
        .cloned()
        .chain(incoming.keys().cloned())
        .chain(sale_seller_counts.keys().cloned())
        .chain(withdrawal_edge_counts.keys().cloned())
        .chain(cashout_edge_counts.keys().cloned())
        .chain(receiver_candidates)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    candidate_addresses.sort();

    let mut rows = Vec::new();
    for address in candidate_addresses {
        if address.is_empty() {
            continue;
        }
        let mint_activity_observed = mint_addresses.contains(&address);
        let wash_cycle_count = *cycle_counts.get(&address).unwrap_or(&0);
        let (wash_cycle_value_eth, wash_cycle_value_usd, wash_cycle_has_usd) =
            if let Some(wash_cycle_scope) = wash_cycle_scopes.get(&address) {
                wash_cycle_value_for_address(
                    &address,
                    wash_cycle_scope,
                    activity,
                    mint_payment_edges,
                )
            } else {
                (0.0, 0.0, false)
            };
        let star_out_degree = outgoing.get(&address).map(|value| value.len()).unwrap_or(0) as i64;
        let is_star_distributor = star_out_degree >= 3;
        let withdrawal_edge_count = *withdrawal_edge_counts.get(&address).unwrap_or(&0);
        let cashout_edge_count = *cashout_edge_counts.get(&address).unwrap_or(&0);
        let value_extraction_observed = withdrawal_edge_count > 0 || cashout_edge_count > 0;
        let sale_seller_count = *sale_seller_counts.get(&address).unwrap_or(&0);
        let rapid_spread = rapid_addresses.contains(&address);
        let rapid_spread_token_count = rapid_token_ids_by_address
            .get(&address)
            .map(|tokens| tokens.len())
            .unwrap_or(0);
        let acquired_victim_like = (paid_acquisition_addresses.contains(&address)
            || secondary_market_resellers.contains(&address))
            && wash_cycle_count == 0
            && !is_star_distributor
            && !value_extraction_observed
            && sale_seller_count < 3;
        if acquired_victim_like {
            continue;
        }
        let high_volume_seller = sale_seller_count >= 3;
        let operator_level = classify_operator_level(&OperatorLevelSignals {
            wash_cycle_count,
            wash_cycle_value_eth,
            wash_cycle_value_usd,
            wash_cycle_has_usd,
            star_out_degree,
            sale_seller_count,
            rapid_spread_token_count,
            value_extraction_observed,
        });
        if wash_cycle_count == 0
            && !is_star_distributor
            && !value_extraction_observed
            && !high_volume_seller
            && !rapid_spread
        {
            continue;
        }
        if operator_level == 0 {
            continue;
        }
        rows.push(MaliciousAddressPayload {
            address: address.clone(),
            mint_activity_observed,
            wash_cycle_count,
            wash_cycle_propagation_count: wash_cycle_count,
            wash_cycle_value_eth,
            wash_cycle_value_usd,
            star_out_degree,
            sale_seller_count,
            withdrawal_edge_count,
            cashout_edge_count,
            operator_level,
            operator_level_label: operator_level_label(operator_level).into(),
            rapid_spread_contracts: if rapid_spread {
                vec![contract_address.to_string()]
            } else {
                vec![]
            },
            evidence_contracts: vec![contract_address.to_string()],
        });
    }
    rows
}
