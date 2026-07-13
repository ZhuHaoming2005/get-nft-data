use super::*;

pub struct HonestAddressRecordInput<'a> {
    pub contract_address: &'a str,
    pub transfers: &'a [TransferRecord],
    pub sales: &'a [NftSaleRecord],
    pub owners: &'a [OwnerBalance],
    pub infringing_tokens: &'a [InfringingTokenRecord],
    pub malicious_addresses: &'a [MaliciousAddressPayload],
    pub mint_payment_edges: &'a [ValueFlowEdgePayload],
    pub deployment_time: i64,
    pub analysis_timestamp: i64,
}

pub fn build_honest_address_records(
    input: HonestAddressRecordInput<'_>,
) -> Vec<HonestAddressPayload> {
    let HonestAddressRecordInput {
        contract_address,
        transfers,
        sales,
        owners,
        infringing_tokens,
        malicious_addresses,
        mint_payment_edges,
        deployment_time,
        analysis_timestamp,
    } = input;
    let activity = prepare_contract_activity(transfers, sales, owners);
    build_honest_address_records_from_activity(
        contract_address,
        &activity,
        infringing_tokens,
        malicious_addresses,
        mint_payment_edges,
        deployment_time,
        analysis_timestamp,
    )
}

pub(crate) fn build_honest_address_records_from_activity(
    contract_address: &str,
    activity: &PreparedContractActivity<'_>,
    infringing_tokens: &[InfringingTokenRecord],
    malicious_addresses: &[MaliciousAddressPayload],
    mint_payment_edges: &[ValueFlowEdgePayload],
    deployment_time: i64,
    analysis_timestamp: i64,
) -> Vec<HonestAddressPayload> {
    let cutoff_time = analysis_timestamp.max(0);
    let deployment_time = deployment_time.max(0);
    let relevant_token_ids: HashSet<String> = infringing_tokens
        .iter()
        .filter(|item| !item.token_id.is_empty())
        .map(|item| item.token_id.clone())
        .collect();
    let malicious_set: HashSet<String> = malicious_addresses
        .iter()
        .filter(|item| !item.address.is_empty())
        .map(|item| item.address.clone())
        .collect();

    let owner_token_map: HashMap<String, HashSet<String>> = activity
        .owner_token_map
        .iter()
        .filter_map(|(owner_address, held_tokens)| {
            let filtered: HashSet<String> = held_tokens
                .iter()
                .filter(|token_id| {
                    relevant_token_ids.is_empty() || relevant_token_ids.contains(*token_id)
                })
                .cloned()
                .collect();
            (!filtered.is_empty()).then(|| (owner_address.clone(), filtered))
        })
        .collect();

    let relevant_transfers: Vec<&TransferRecord> = activity
        .sorted_transfers
        .iter()
        .copied()
        .filter(|transfer| {
            relevant_token_ids.is_empty() || relevant_token_ids.contains(&transfer.token_id)
        })
        .collect();

    let relevant_sales: Vec<&NftSaleRecord> = activity
        .sorted_sales
        .iter()
        .copied()
        .filter(|sale| relevant_token_ids.is_empty() || relevant_token_ids.contains(&sale.token_id))
        .collect();

    let mut all_addresses: HashSet<String> = HashSet::new();
    let mut non_mint_transfer_participants: HashSet<String> = HashSet::new();
    let mut sale_participants: HashSet<String> = HashSet::new();
    for transfer in &relevant_transfers {
        if !transfer.from_address.is_empty() && transfer.from_address != ZERO_ADDRESS {
            all_addresses.insert(transfer.from_address.clone());
            non_mint_transfer_participants.insert(transfer.from_address.clone());
        }
        if !transfer.to_address.is_empty() && transfer.to_address != ZERO_ADDRESS {
            all_addresses.insert(transfer.to_address.clone());
            if transfer.from_address != ZERO_ADDRESS {
                non_mint_transfer_participants.insert(transfer.to_address.clone());
            }
        }
    }
    for sale in &relevant_sales {
        if !sale.buyer_address.is_empty() {
            all_addresses.insert(sale.buyer_address.clone());
            sale_participants.insert(sale.buyer_address.clone());
        }
        if !sale.seller_address.is_empty() {
            all_addresses.insert(sale.seller_address.clone());
            sale_participants.insert(sale.seller_address.clone());
        }
    }
    for address in owner_token_map.keys() {
        all_addresses.insert(address.clone());
    }

    let mut honest_addresses: Vec<String> = all_addresses
        .into_iter()
        .filter(|address| {
            !address.is_empty()
                && !malicious_set.contains(address)
                && (owner_token_map.contains_key(address)
                    || non_mint_transfer_participants.contains(address)
                    || sale_participants.contains(address))
        })
        .collect();
    honest_addresses.sort();
    let honest_set: HashSet<String> = honest_addresses.iter().cloned().collect();

    let mut transfers_by_token: HashMap<String, Vec<&TransferRecord>> = HashMap::new();
    for transfer in relevant_transfers {
        transfers_by_token
            .entry(transfer.token_id.clone())
            .or_default()
            .push(transfer);
    }

    let mut token_interactions_by_address: HashMap<String, HashSet<String>> = HashMap::new();
    let mut durations_by_address: HashMap<String, Vec<i64>> = HashMap::new();
    let mut deployment_to_neutral_holder_samples_by_address: HashMap<String, Vec<i64>> =
        HashMap::new();
    let mut victim_resale_count: HashMap<String, i64> = HashMap::new();
    let mut corrupted_addresses: HashSet<String> = HashSet::new();

    let mut paid_mint_acquisitions_by_token: HashMap<
        String,
        HashMap<String, Vec<&ValueFlowEdgePayload>>,
    > = HashMap::new();
    for edge in mint_payment_edges {
        if edge.channel != "mint_payment"
            || !identities_equal(&edge.contract_address, contract_address)
            || !value_flow_has_positive_value(edge)
        {
            continue;
        }
        let payer = &edge.from_address;
        if !honest_set.contains(payer) {
            continue;
        }
        for token_id in value_flow_token_ids(edge) {
            if relevant_token_ids.is_empty() || relevant_token_ids.contains(&token_id) {
                paid_mint_acquisitions_by_token
                    .entry(token_id)
                    .or_default()
                    .entry(payer.clone())
                    .or_default()
                    .push(edge);
            }
        }
    }

    let mut prior_paid_buyers_by_token: HashMap<String, HashSet<String>> = HashMap::new();
    for sale in &relevant_sales {
        if !sale_has_positive_value(sale) {
            continue;
        }
        let seller = &sale.seller_address;
        let prior_paid_buyers = prior_paid_buyers_by_token
            .entry(sale.token_id.clone())
            .or_default();
        let has_paid_mint_acquisition = paid_mint_acquisitions_by_token
            .get(&sale.token_id)
            .and_then(|by_address| by_address.get(seller))
            .map(|edges| {
                edges
                    .iter()
                    .any(|edge| value_flow_precedes_sale(edge, sale))
            })
            .unwrap_or(false);
        if honest_set.contains(seller)
            && (prior_paid_buyers.contains(seller) || has_paid_mint_acquisition)
        {
            corrupted_addresses.insert(seller.clone());
            *victim_resale_count.entry(seller.clone()).or_insert(0) += 1;
        }
        if honest_set.contains(&sale.buyer_address) {
            prior_paid_buyers.insert(sale.buyer_address.clone());
        }
    }

    for (token_id, token_transfers) in transfers_by_token {
        let mut first_honest_recorded = false;
        let mut open_holds: HashMap<String, i64> = HashMap::new();

        for transfer in &token_transfers {
            if honest_set.contains(&transfer.from_address) {
                token_interactions_by_address
                    .entry(transfer.from_address.clone())
                    .or_default()
                    .insert(token_id.clone());
                if let Some(start_time) = open_holds.remove(&transfer.from_address) {
                    if transfer.block_time >= start_time {
                        durations_by_address
                            .entry(transfer.from_address.clone())
                            .or_default()
                            .push(transfer.block_time - start_time);
                    }
                }
            }
            if honest_set.contains(&transfer.to_address) {
                token_interactions_by_address
                    .entry(transfer.to_address.clone())
                    .or_default()
                    .insert(token_id.clone());
                if transfer.block_time > 0 {
                    open_holds.insert(transfer.to_address.clone(), transfer.block_time);
                    if deployment_time > 0 && !first_honest_recorded {
                        deployment_to_neutral_holder_samples_by_address
                            .entry(transfer.to_address.clone())
                            .or_default()
                            .push((transfer.block_time - deployment_time).max(0));
                        first_honest_recorded = true;
                    }
                }
            }
        }

        for (address, start_time) in open_holds {
            if !owner_token_map
                .get(&address)
                .map(|held_tokens| held_tokens.contains(&token_id))
                .unwrap_or(false)
            {
                continue;
            }
            if cutoff_time >= start_time {
                durations_by_address
                    .entry(address)
                    .or_default()
                    .push(cutoff_time - start_time);
            }
        }
    }

    honest_addresses
        .into_iter()
        .map(|address| {
            let current_tokens = owner_token_map.get(&address).cloned().unwrap_or_default();
            let interacted_tokens = token_interactions_by_address
                .get(&address)
                .cloned()
                .unwrap_or_default();
            let mut union_tokens = interacted_tokens;
            union_tokens.extend(current_tokens.iter().cloned());
            let hold_durations = durations_by_address
                .get(&address)
                .cloned()
                .unwrap_or_default();

            HonestAddressPayload {
                contract_address: contract_address.to_string(),
                address: address.clone(),
                interacted_token_count: union_tokens.len() as i64,
                currently_holding_token_count: current_tokens.len() as i64,
                hold_duration_median_seconds: median_i64(&hold_durations),
                hold_duration_count: hold_durations.len() as i64,
                is_corrupted_address: corrupted_addresses.contains(&address),
                victim_resale_count: *victim_resale_count.get(&address).unwrap_or(&0),
                deployment_to_neutral_holder_seconds_samples:
                    deployment_to_neutral_holder_samples_by_address
                        .get(&address)
                        .cloned()
                        .unwrap_or_default(),
            }
        })
        .collect()
}
