use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::models::{
    DuplicateCandidate, FraudTradeStatsPayload, HonestAddressPayload, HonestAddressStatsPayload,
    InfringingTokenRecord, MaliciousAddressPayload, NftSaleRecord, OwnerBalance, TransferRecord,
    VictimAddressPayload, ZERO_ADDRESS,
};

#[derive(Clone, Debug, Default, PartialEq)]
pub struct SaleMetricRecord {
    pub buy_before_eth_balance: Option<f64>,
    pub buy_before_usd_balance: Option<f64>,
    pub buy_asset_ratio: Option<f64>,
    pub buy_asset_ratio_with_gas: Option<f64>,
    pub ratio_status: String,
}

pub(crate) struct PreparedContractActivity<'a> {
    owner_token_map: HashMap<String, HashSet<String>>,
    sorted_transfers: Vec<&'a TransferRecord>,
    sorted_sales: Vec<&'a NftSaleRecord>,
    latest_outgoing: HashMap<(String, String), (i64, i64, String)>,
}

fn transfer_sort_key(transfer: &TransferRecord) -> (i64, i64, &str) {
    (
        transfer.block_number,
        transfer.log_index,
        transfer.tx_hash.as_str(),
    )
}

fn sale_sort_key(sale: &NftSaleRecord) -> (i64, i64, i64, &str) {
    (
        sale.block_number,
        sale.log_index,
        sale.bundle_index,
        sale.tx_hash.as_str(),
    )
}

fn median_i64(values: &[i64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let mid = sorted.len() / 2;
    if sorted.len() % 2 == 1 {
        Some(sorted[mid] as f64)
    } else {
        Some((sorted[mid - 1] as f64 + sorted[mid] as f64) / 2.0)
    }
}

fn median_f64(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|left, right| left.total_cmp(right));
    let mid = sorted.len() / 2;
    if sorted.len() % 2 == 1 {
        Some(sorted[mid])
    } else {
        Some((sorted[mid - 1] + sorted[mid]) / 2.0)
    }
}

fn mean_f64(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        None
    } else {
        Some(values.iter().sum::<f64>() / values.len() as f64)
    }
}

fn sale_usd_value(sale: &NftSaleRecord) -> Option<f64> {
    sale.price_usd
}

fn build_owner_token_map(owners: &[OwnerBalance]) -> HashMap<String, HashSet<String>> {
    let mut owner_token_map = HashMap::new();
    for owner in owners {
        if owner.owner_address.is_empty() || owner.owner_address == ZERO_ADDRESS {
            continue;
        }
        let held_tokens: HashSet<String> = owner
            .token_balances
            .iter()
            .filter_map(|(token_id, balance)| (*balance > 0).then(|| token_id.clone()))
            .collect();
        if !held_tokens.is_empty() {
            owner_token_map.insert(owner.owner_address.clone(), held_tokens);
        }
    }
    owner_token_map
}

pub(crate) fn prepare_contract_activity<'a>(
    transfers: &'a [TransferRecord],
    sales: &'a [NftSaleRecord],
    owners: &'a [OwnerBalance],
) -> PreparedContractActivity<'a> {
    let owner_token_map = build_owner_token_map(owners);

    let mut latest_outgoing = HashMap::new();
    let mut sorted_transfers: Vec<&TransferRecord> = transfers.iter().collect();
    sorted_transfers.sort_by(|left, right| transfer_sort_key(left).cmp(&transfer_sort_key(right)));
    for transfer in &sorted_transfers {
        if transfer.from_address.is_empty() || transfer.from_address == ZERO_ADDRESS {
            continue;
        }
        let key = (transfer.from_address.clone(), transfer.token_id.clone());
        let transfer_key = (
            transfer.block_number,
            transfer.log_index,
            transfer.tx_hash.clone(),
        );
        match latest_outgoing.get(&key) {
            Some(current) if current >= &transfer_key => {}
            _ => {
                latest_outgoing.insert(key, transfer_key);
            }
        }
    }

    let mut sorted_sales: Vec<&NftSaleRecord> = sales.iter().collect();
    sorted_sales.sort_by(|left, right| sale_sort_key(left).cmp(&sale_sort_key(right)));

    PreparedContractActivity {
        owner_token_map,
        sorted_transfers,
        sorted_sales,
        latest_outgoing,
    }
}

pub fn build_infringing_token_records(
    contract_address: &str,
    contract_candidates: &[DuplicateCandidate],
    transfers: &[TransferRecord],
) -> Vec<InfringingTokenRecord> {
    let candidate_refs: Vec<&DuplicateCandidate> = contract_candidates.iter().collect();
    build_infringing_token_records_with_context_refs(
        contract_address,
        &candidate_refs,
        transfers,
        &HashSet::new(),
        &HashMap::new(),
    )
}

pub fn build_infringing_token_records_with_context(
    contract_address: &str,
    contract_candidates: &[DuplicateCandidate],
    transfers: &[TransferRecord],
    official_addresses: &HashSet<String>,
    candidate_open_license_by_token: &HashMap<(String, String), bool>,
) -> Vec<InfringingTokenRecord> {
    let candidate_refs: Vec<&DuplicateCandidate> = contract_candidates.iter().collect();
    build_infringing_token_records_with_context_refs(
        contract_address,
        &candidate_refs,
        transfers,
        official_addresses,
        candidate_open_license_by_token,
    )
}

pub fn build_infringing_token_records_with_context_refs(
    contract_address: &str,
    contract_candidates: &[&DuplicateCandidate],
    transfers: &[TransferRecord],
    official_addresses: &HashSet<String>,
    candidate_open_license_by_token: &HashMap<(String, String), bool>,
) -> Vec<InfringingTokenRecord> {
    let mut transfers_by_token: HashMap<String, Vec<&TransferRecord>> = HashMap::new();
    for transfer in transfers {
        if transfer.contract_address != contract_address || transfer.token_id.is_empty() {
            continue;
        }
        transfers_by_token
            .entry(transfer.token_id.clone())
            .or_default()
            .push(transfer);
    }
    for token_transfers in transfers_by_token.values_mut() {
        token_transfers
            .sort_by(|left, right| transfer_sort_key(left).cmp(&transfer_sort_key(right)));
    }

    let mut rows: Vec<InfringingTokenRecord> = contract_candidates
        .iter()
        .map(|candidate| {
            let token_transfers = transfers_by_token.get(&candidate.token_id);
            let mint_transfer = token_transfers.and_then(|rows| {
                rows.iter()
                    .find(|row| row.from_address == ZERO_ADDRESS)
                    .copied()
            });
            let first_non_mint_transfer = token_transfers.and_then(|rows| {
                rows.iter()
                    .find(|row| row.from_address != ZERO_ADDRESS)
                    .copied()
            });
            let first_transfer = token_transfers.and_then(|rows| rows.first().copied());
            let (minter_address, mint_tx_hash, mint_block, first_transfer_time) =
                if let Some(mint_transfer) = mint_transfer {
                    (
                        mint_transfer.to_address.clone(),
                        mint_transfer.tx_hash.clone(),
                        mint_transfer.block_number,
                        first_non_mint_transfer
                            .map(|transfer| transfer.block_time)
                            .unwrap_or(0),
                    )
                } else if let Some(first_transfer) = first_transfer {
                    (
                        first_transfer.to_address.clone(),
                        first_transfer.tx_hash.clone(),
                        first_transfer.block_number,
                        first_transfer.block_time,
                    )
                } else {
                    (String::new(), String::new(), 0, 0)
                };

            let official_or_legit_reissue =
                !minter_address.is_empty() && official_addresses.contains(&minter_address);

            InfringingTokenRecord {
                contract_address: contract_address.to_string(),
                token_id: candidate.token_id.clone(),
                mint_tx_hash,
                mint_block,
                minter_address,
                first_transfer_time,
                history_window: "full".to_string(),
                match_reasons: candidate.match_reasons.clone(),
                candidate_open_license: candidate_open_license_by_token
                    .get(&(contract_address.to_string(), candidate.token_id.clone()))
                    .copied()
                    .unwrap_or(false),
                official_or_legit_reissue,
            }
        })
        .collect();

    rows.sort_by(|left, right| {
        (&left.token_id, &left.contract_address).cmp(&(&right.token_id, &right.contract_address))
    });
    rows
}

pub fn build_malicious_address_records(
    contract_address: &str,
    transfers: &[TransferRecord],
    infringing_tokens: &[InfringingTokenRecord],
) -> Vec<MaliciousAddressPayload> {
    let activity =
        prepare_contract_activity(transfers, &[] as &[NftSaleRecord], &[] as &[OwnerBalance]);
    build_malicious_address_records_from_activity(contract_address, &activity, infringing_tokens)
}

pub(crate) fn build_malicious_address_records_from_activity(
    contract_address: &str,
    activity: &PreparedContractActivity<'_>,
    infringing_tokens: &[InfringingTokenRecord],
) -> Vec<MaliciousAddressPayload> {
    let relevant_token_ids: HashSet<String> = infringing_tokens
        .iter()
        .filter_map(|item| (!item.token_id.is_empty()).then(|| item.token_id.clone()))
        .collect();
    let mint_addresses: HashSet<String> = infringing_tokens
        .iter()
        .filter_map(|item| (!item.minter_address.is_empty()).then(|| item.minter_address.clone()))
        .collect();

    let mut outgoing: HashMap<String, HashSet<String>> = HashMap::new();
    let mut cycle_counts: HashMap<String, i64> = HashMap::new();
    let mut seen_pairs: HashSet<(String, String)> = HashSet::new();
    let mut rapid_addresses: HashSet<String> = HashSet::new();
    let mut mint_times: HashMap<String, i64> = HashMap::new();
    let mut receiver_candidates: HashSet<String> = HashSet::new();

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
            let pair = (transfer.from_address.clone(), transfer.to_address.clone());
            let reverse = (transfer.to_address.clone(), transfer.from_address.clone());
            if seen_pairs.contains(&reverse) {
                *cycle_counts
                    .entry(transfer.from_address.clone())
                    .or_insert(0) += 1;
                *cycle_counts.entry(transfer.to_address.clone()).or_insert(0) += 1;
            }
            seen_pairs.insert(pair);
        }
        let mint_time = *mint_times.get(&transfer.token_id).unwrap_or(&0);
        if mint_time > 0 && transfer.block_time > 0 && transfer.block_time - mint_time <= 24 * 3600
        {
            if !transfer.from_address.is_empty() {
                rapid_addresses.insert(transfer.from_address.clone());
            }
            if !transfer.to_address.is_empty() {
                rapid_addresses.insert(transfer.to_address.clone());
            }
        }
    }

    let mut candidate_addresses: Vec<String> = mint_addresses
        .iter()
        .cloned()
        .chain(outgoing.keys().cloned())
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
        let mint_role = mint_addresses.contains(&address);
        let wash_cycle_count = *cycle_counts.get(&address).unwrap_or(&0);
        let star_out_degree = outgoing.get(&address).map(|value| value.len()).unwrap_or(0) as i64;
        let is_star_distributor = star_out_degree >= 3;
        if !mint_role && wash_cycle_count == 0 && !is_star_distributor {
            continue;
        }
        rows.push(MaliciousAddressPayload {
            address: address.clone(),
            mint_role,
            wash_cycle_count,
            star_out_degree,
            rapid_spread_contracts: if rapid_addresses.contains(&address) {
                vec![contract_address.to_string()]
            } else {
                vec![]
            },
            evidence_contracts: vec![contract_address.to_string()],
        });
    }
    rows
}

pub fn build_victim_address_records(
    sales: &[NftSaleRecord],
    transfers: &[TransferRecord],
    owners: &[OwnerBalance],
    sale_metrics_by_tx: &BTreeMap<String, SaleMetricRecord>,
) -> Vec<VictimAddressPayload> {
    let activity = prepare_contract_activity(transfers, sales, owners);
    build_victim_address_records_from_activity(&activity, sale_metrics_by_tx)
}

pub(crate) fn build_victim_address_records_from_activity(
    activity: &PreparedContractActivity<'_>,
    sale_metrics_by_tx: &BTreeMap<String, SaleMetricRecord>,
) -> Vec<VictimAddressPayload> {
    let mut grouped: BTreeMap<String, VictimAddressPayload> = BTreeMap::new();
    let mut last_buy_key: HashMap<String, (i64, i64, i64, String)> = HashMap::new();

    for sale in &activity.sorted_sales {
        if sale.buyer_address.is_empty() {
            continue;
        }
        let metrics = sale_metrics_by_tx
            .get(&sale.tx_hash)
            .cloned()
            .unwrap_or_default();
        let later_transfer_out = activity
            .latest_outgoing
            .get(&(sale.buyer_address.clone(), sale.token_id.clone()))
            .map(|transfer_key| {
                transfer_key > &(sale.block_number, sale.log_index, sale.tx_hash.clone())
            })
            .unwrap_or(false);
        let is_stuck = activity
            .owner_token_map
            .get(&sale.buyer_address)
            .map(|held_tokens| held_tokens.contains(&sale.token_id))
            .unwrap_or(false)
            && !later_transfer_out;

        let entry = grouped
            .entry(sale.buyer_address.clone())
            .or_insert_with(|| VictimAddressPayload {
                address: sale.buyer_address.clone(),
                ratio_status: "unavailable".into(),
                ..VictimAddressPayload::default()
            });
        entry.buy_tx_hashes.push(sale.tx_hash.clone());
        if sale.price_eth.is_some() {
            entry.buy_amount_eth += sale.price_eth.unwrap_or(0.0);
        }
        if let Some(amount_usd) = sale_usd_value(sale) {
            entry.buy_amount_usd += amount_usd;
        }
        let current_key = (
            sale.block_number,
            sale.log_index,
            sale.bundle_index,
            sale.tx_hash.clone(),
        );
        let should_update_last = last_buy_key
            .get(&sale.buyer_address)
            .map(|existing| &current_key >= existing)
            .unwrap_or(true);
        if should_update_last {
            last_buy_key.insert(sale.buyer_address.clone(), current_key);
            entry.last_buy_tx_hash = sale.tx_hash.clone();
            entry.last_buy_amount_eth = sale.price_eth;
            entry.last_buy_amount_usd = sale_usd_value(sale);
            entry.buy_before_eth_balance = metrics.buy_before_eth_balance;
            entry.buy_before_usd_balance = metrics.buy_before_usd_balance;
            entry.buy_asset_ratio = metrics.buy_asset_ratio;
            entry.buy_asset_ratio_with_gas = metrics.buy_asset_ratio_with_gas;
            entry.ratio_status = if metrics.ratio_status.is_empty() {
                "unavailable".into()
            } else {
                metrics.ratio_status
            };
        }
        entry.is_stuck = entry.is_stuck || is_stuck;
    }

    grouped.into_values().collect()
}

pub fn build_honest_address_records(
    contract_address: &str,
    transfers: &[TransferRecord],
    sales: &[NftSaleRecord],
    owners: &[OwnerBalance],
    infringing_tokens: &[InfringingTokenRecord],
    malicious_addresses: &[MaliciousAddressPayload],
    analysis_timestamp: i64,
) -> Vec<HonestAddressPayload> {
    let activity = prepare_contract_activity(transfers, sales, owners);
    build_honest_address_records_from_activity(
        contract_address,
        &activity,
        infringing_tokens,
        malicious_addresses,
        analysis_timestamp,
    )
}

pub(crate) fn build_honest_address_records_from_activity(
    contract_address: &str,
    activity: &PreparedContractActivity<'_>,
    infringing_tokens: &[InfringingTokenRecord],
    malicious_addresses: &[MaliciousAddressPayload],
    analysis_timestamp: i64,
) -> Vec<HonestAddressPayload> {
    let cutoff_time = analysis_timestamp.max(0);
    let relevant_token_ids: HashSet<String> = infringing_tokens
        .iter()
        .filter_map(|item| (!item.token_id.is_empty()).then(|| item.token_id.clone()))
        .collect();
    let malicious_set: HashSet<String> = malicious_addresses
        .iter()
        .filter_map(|item| (!item.address.is_empty()).then(|| item.address.clone()))
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
    for transfer in &relevant_transfers {
        if !transfer.from_address.is_empty() && transfer.from_address != ZERO_ADDRESS {
            all_addresses.insert(transfer.from_address.clone());
        }
        if !transfer.to_address.is_empty() && transfer.to_address != ZERO_ADDRESS {
            all_addresses.insert(transfer.to_address.clone());
        }
    }
    for sale in &relevant_sales {
        if !sale.buyer_address.is_empty() {
            all_addresses.insert(sale.buyer_address.clone());
        }
        if !sale.seller_address.is_empty() {
            all_addresses.insert(sale.seller_address.clone());
        }
    }
    for address in owner_token_map.keys() {
        all_addresses.insert(address.clone());
    }

    let mut honest_addresses: Vec<String> = all_addresses
        .into_iter()
        .filter(|address| !address.is_empty() && !malicious_set.contains(address))
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
    let mut mint_to_honest_samples_by_address: HashMap<String, Vec<i64>> = HashMap::new();
    let mut honest_to_honest_count: HashMap<String, i64> = HashMap::new();
    let mut corrupted_addresses: HashSet<String> = HashSet::new();

    for (token_id, token_transfers) in transfers_by_token {
        let mut mint_time = 0_i64;
        let mut first_honest_recorded = false;
        let mut open_holds: HashMap<String, i64> = HashMap::new();

        for transfer in &token_transfers {
            if transfer.from_address == ZERO_ADDRESS && transfer.block_time > 0 {
                mint_time = transfer.block_time;
            }
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
            if honest_set.contains(&transfer.from_address)
                && honest_set.contains(&transfer.to_address)
            {
                corrupted_addresses.insert(transfer.from_address.clone());
                *honest_to_honest_count
                    .entry(transfer.from_address.clone())
                    .or_insert(0) += 1;
            }
            if honest_set.contains(&transfer.to_address) {
                token_interactions_by_address
                    .entry(transfer.to_address.clone())
                    .or_default()
                    .insert(token_id.clone());
                if transfer.block_time > 0 {
                    open_holds.insert(transfer.to_address.clone(), transfer.block_time);
                    if mint_time > 0 && !first_honest_recorded {
                        mint_to_honest_samples_by_address
                            .entry(transfer.to_address.clone())
                            .or_default()
                            .push((transfer.block_time - mint_time).max(0));
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
                honest_sale_to_honest_count: *honest_to_honest_count.get(&address).unwrap_or(&0),
                mint_to_honest_seconds_samples: mint_to_honest_samples_by_address
                    .get(&address)
                    .cloned()
                    .unwrap_or_default(),
            }
        })
        .collect()
}

pub fn build_honest_address_stats(
    contract_address: &str,
    honest_addresses: &[HonestAddressPayload],
) -> BTreeMap<String, HonestAddressStatsPayload> {
    let corrupted_addresses: Vec<String> = honest_addresses
        .iter()
        .filter(|item| item.is_corrupted_address)
        .map(|item| item.address.clone())
        .collect();
    let holding_medians: Vec<f64> = honest_addresses
        .iter()
        .filter_map(|item| item.hold_duration_median_seconds)
        .collect();
    let mint_to_honest_samples: Vec<f64> = honest_addresses
        .iter()
        .flat_map(|item| {
            item.mint_to_honest_seconds_samples
                .iter()
                .map(|sample| *sample as f64)
        })
        .collect();

    BTreeMap::from([(
        contract_address.to_string(),
        HonestAddressStatsPayload {
            honest_address_count: honest_addresses.len() as i64,
            corrupted_address_count: corrupted_addresses.len() as i64,
            honest_to_honest_transfer_count: honest_addresses
                .iter()
                .map(|item| item.honest_sale_to_honest_count)
                .sum(),
            median_holding_seconds: median_f64(&holding_medians),
            avg_seconds_to_honest_holder: mean_f64(&mint_to_honest_samples),
            corrupted_addresses,
        },
    )])
}

pub fn build_fraud_trade_stats(
    contract_address: &str,
    sales: &[NftSaleRecord],
    victim_addresses: &[VictimAddressPayload],
) -> BTreeMap<String, FraudTradeStatsPayload> {
    let contract_sales: Vec<&NftSaleRecord> = sales
        .iter()
        .filter(|sale| sale.contract_address.eq_ignore_ascii_case(contract_address))
        .collect();
    let native_sales: Vec<&NftSaleRecord> = contract_sales
        .iter()
        .copied()
        .filter(|sale| sale.is_native_eth && sale.price_eth.is_some())
        .collect();
    let eth_priced_sales: Vec<&NftSaleRecord> = contract_sales
        .iter()
        .copied()
        .filter(|sale| sale.price_eth.is_some())
        .collect();
    let usd_priced_sales: Vec<&NftSaleRecord> = contract_sales
        .iter()
        .copied()
        .filter(|sale| sale_usd_value(sale).is_some())
        .collect();

    BTreeMap::from([(
        contract_address.to_string(),
        FraudTradeStatsPayload {
            unique_buyers: sales
                .iter()
                .filter(|sale| sale.contract_address.eq_ignore_ascii_case(contract_address))
                .filter_map(|sale| {
                    (!sale.buyer_address.is_empty()).then(|| sale.buyer_address.clone())
                })
                .collect::<BTreeSet<_>>()
                .len() as i64,
            native_eth_sale_count: Some(native_sales.len() as i64),
            native_eth_volume: Some(
                native_sales
                    .iter()
                    .map(|sale| sale.price_eth.unwrap_or(0.0))
                    .sum(),
            ),
            usd_priced_sale_count: Some(usd_priced_sales.len() as i64),
            usd_priced_volume: Some(
                usd_priced_sales
                    .iter()
                    .map(|sale| sale_usd_value(sale).unwrap_or(0.0))
                    .sum(),
            ),
            eth_priced_sale_count: Some(eth_priced_sales.len() as i64),
            eth_priced_volume: Some(
                eth_priced_sales
                    .iter()
                    .map(|sale| sale.price_eth.unwrap_or(0.0))
                    .sum(),
            ),
            stuck_wallet_count: victim_addresses.iter().filter(|item| item.is_stuck).count() as i64,
            stuck_cost_eth: victim_addresses
                .iter()
                .filter(|item| item.is_stuck)
                .map(|item| item.last_buy_amount_eth.unwrap_or(0.0))
                .sum(),
            stuck_cost_usd: victim_addresses
                .iter()
                .filter(|item| item.is_stuck)
                .map(|item| item.last_buy_amount_usd.unwrap_or(0.0))
                .sum(),
        },
    )])
}

#[cfg(test)]
mod tests {
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

    #[test]
    fn fraud_trade_stats_include_weth_and_stablecoin_eth_equivalent_amounts() {
        let sales = vec![sale("ETH", 1.0), sale("WETH", 2.0), sale("USDC", 0.05)];

        let stats = build_fraud_trade_stats("0xdup", &sales, &[] as &[VictimAddressPayload]);
        let stats = &stats["0xdup"];

        assert_eq!(stats.native_eth_sale_count, Some(1));
        assert_eq!(stats.native_eth_volume, Some(1.0));
        assert_eq!(stats.eth_priced_sale_count, Some(3));
        assert_eq!(stats.eth_priced_volume, Some(3.05));
        assert_eq!(stats.usd_priced_sale_count, Some(3));
        assert_eq!(stats.usd_priced_volume, Some(3.05));
    }

    #[test]
    fn fraud_trade_stats_ignore_sales_from_other_contracts() {
        let mut matching = sale("USDC", 5.0);
        matching.contract_address = "0xdup".into();
        let mut unrelated = sale("USDC", 7.0);
        unrelated.contract_address = "0xother".into();

        let stats = build_fraud_trade_stats("0xdup", &[matching, unrelated], &[]);
        let stats = &stats["0xdup"];

        assert_eq!(stats.unique_buyers, 1);
        assert_eq!(stats.usd_priced_sale_count, Some(1));
        assert_eq!(stats.usd_priced_volume, Some(5.0));
    }

    #[test]
    fn fraud_trade_stats_do_not_count_eth_amounts_as_usd_when_rate_is_missing() {
        let mut eth_sale = sale("ETH", 1.25);
        eth_sale.price_usd = None;

        let stats = build_fraud_trade_stats("0xdup", &[eth_sale], &[]);
        let stats = &stats["0xdup"];

        assert_eq!(stats.eth_priced_sale_count, Some(1));
        assert_eq!(stats.eth_priced_volume, Some(1.25));
        assert_eq!(stats.usd_priced_sale_count, Some(0));
        assert_eq!(stats.usd_priced_volume, Some(0.0));
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

        let victims = build_victim_address_records_from_activity(&activity, &BTreeMap::new());

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

        let victims = build_victim_address_records_from_activity(&activity, &BTreeMap::new());

        assert_eq!(victims.len(), 1);
        assert_eq!(victims[0].buy_amount_eth, 0.1);
        assert_eq!(victims[0].buy_amount_usd, 0.1);
        assert_eq!(victims[0].last_buy_amount_eth, Some(0.1));
        assert_eq!(victims[0].last_buy_amount_usd, Some(0.1));
        assert!(victims[0].is_stuck);
    }
}
