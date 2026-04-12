use crate::common::ZERO_ADDRESS;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};

#[derive(Default)]
struct VictimAddressRow {
    address: String,
    buy_tx_hashes: Vec<String>,
    buy_amount_eth: f64,
    last_buy_amount_eth: Option<f64>,
    buy_before_eth_balance: Option<f64>,
    buy_asset_ratio: Option<f64>,
    buy_asset_ratio_with_gas: Option<f64>,
    is_stuck: bool,
    last_buy_tx_hash: String,
    ratio_status: String,
}

#[derive(Clone)]
struct VictimSaleInput {
    token_id: String,
    tx_hash: String,
    block_number: i64,
    log_index: i64,
    bundle_index: i64,
    buyer_address: String,
    is_eth_priced: bool,
    price_eth: Option<f64>,
    buy_before_eth_balance: Option<f64>,
    buy_asset_ratio: Option<f64>,
    buy_asset_ratio_with_gas: Option<f64>,
    ratio_status: String,
}

#[derive(Clone)]
struct HonestTransferInput {
    token_id: String,
    tx_hash: String,
    block_number: i64,
    log_index: i64,
    block_time: i64,
    from_address: String,
    to_address: String,
}

#[derive(Default)]
struct HonestAddressRow {
    contract_address: String,
    address: String,
    interacted_token_count: usize,
    currently_holding_token_count: usize,
    hold_duration_median_seconds: Option<f64>,
    hold_duration_count: usize,
    is_corrupted_address: bool,
    honest_sale_to_honest_count: usize,
    mint_to_honest_seconds_samples: Vec<i64>,
}

#[derive(Default)]
struct TokenHonestStats {
    token_id: String,
    interacted_addresses: HashSet<String>,
    durations_by_address: HashMap<String, Vec<i64>>,
    mint_to_honest_samples_by_address: HashMap<String, Vec<i64>>,
    honest_to_honest_count: HashMap<String, usize>,
    corrupted_addresses: HashSet<String>,
}

#[derive(Clone)]
struct MaliciousTransferInput {
    token_id: String,
    tx_hash: String,
    block_number: i64,
    log_index: i64,
    block_time: i64,
    from_address: String,
    to_address: String,
}

#[derive(Default)]
struct MaliciousAddressRow {
    address: String,
    mint_role: bool,
    wash_cycle_count: usize,
    star_out_degree: usize,
    rapid_spread_contracts: Vec<String>,
    evidence_contracts: Vec<String>,
}

#[derive(Clone)]
struct InfringingTokenTransferInput {
    token_id: String,
    tx_hash: String,
    block_number: i64,
    log_index: i64,
    block_time: i64,
    from_address: String,
    to_address: String,
}

#[derive(Clone)]
struct InfringingCandidateInput {
    token_id: String,
    match_reasons: Vec<String>,
}

#[derive(Default)]
struct InfringingTokenRow {
    contract_address: String,
    token_id: String,
    mint_tx_hash: String,
    mint_block: i64,
    minter_address: String,
    first_transfer_time: i64,
    history_window: String,
    match_reasons: Vec<String>,
    candidate_open_license: bool,
    official_or_legit_reissue: bool,
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

fn build_victim_address_records_internal(
    sales: Vec<VictimSaleInput>,
    transfers: Vec<(String, String, i64, i64, String)>,
    owners: Vec<(String, Vec<String>)>,
) -> Vec<VictimAddressRow> {
    let mut owner_token_map: HashMap<String, HashSet<String>> = HashMap::new();
    for (owner_address, held_tokens) in owners.into_iter() {
        if held_tokens.is_empty() {
            continue;
        }
        owner_token_map.insert(owner_address, held_tokens.into_iter().collect());
    }

    let mut latest_outgoing: HashMap<(String, String), (i64, i64, String)> = HashMap::new();
    for (token_id, tx_hash, block_number, log_index, from_address) in transfers.into_iter() {
        if from_address.is_empty() || from_address == ZERO_ADDRESS {
            continue;
        }
        let key = (from_address, token_id);
        let transfer_key = (block_number, log_index, tx_hash);
        match latest_outgoing.get(&key) {
            Some(current) if current >= &transfer_key => {}
            _ => {
                latest_outgoing.insert(key, transfer_key);
            }
        }
    }

    let mut sorted_sales = sales;
    sorted_sales.sort_by(|left, right| {
        (
            left.block_number,
            left.log_index,
            left.bundle_index,
            &left.tx_hash,
        )
            .cmp(&(
                right.block_number,
                right.log_index,
                right.bundle_index,
                &right.tx_hash,
            ))
    });

    let mut grouped: HashMap<String, VictimAddressRow> = HashMap::new();
    let mut last_buy_key: HashMap<String, (i64, i64, i64, String)> = HashMap::new();

    for sale in sorted_sales.into_iter() {
        if sale.buyer_address.is_empty() {
            continue;
        }

        let later_transfer_out = latest_outgoing
            .get(&(sale.buyer_address.clone(), sale.token_id.clone()))
            .map(|transfer_key| transfer_key > &(sale.block_number, sale.log_index, sale.tx_hash.clone()))
            .unwrap_or(false);

        let is_stuck = owner_token_map
            .get(&sale.buyer_address)
            .map(|held_tokens| held_tokens.contains(&sale.token_id))
            .unwrap_or(false)
            && !later_transfer_out;

        let entry = grouped
            .entry(sale.buyer_address.clone())
            .or_insert_with(|| VictimAddressRow {
                address: sale.buyer_address.clone(),
                ratio_status: "unavailable".to_string(),
                ..VictimAddressRow::default()
            });
        entry.buy_tx_hashes.push(sale.tx_hash.clone());
        if sale.is_eth_priced {
            entry.buy_amount_eth += sale.price_eth.unwrap_or(0.0);
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
            entry.last_buy_amount_eth = if sale.is_eth_priced { sale.price_eth } else { None };
            entry.buy_before_eth_balance = sale.buy_before_eth_balance;
            entry.buy_asset_ratio = sale.buy_asset_ratio;
            entry.buy_asset_ratio_with_gas = sale.buy_asset_ratio_with_gas;
            entry.ratio_status = sale.ratio_status.clone();
        }
        entry.is_stuck = entry.is_stuck || is_stuck;
    }

    let mut rows: Vec<VictimAddressRow> = grouped.into_values().collect();
    rows.sort_by(|left, right| left.address.cmp(&right.address));
    rows
}

fn build_honest_address_records_internal(
    contract_address: String,
    transfers: Vec<HonestTransferInput>,
    sales: Vec<(String, String, String)>,
    owners: Vec<(String, Vec<String>)>,
    infringing_token_ids: Vec<String>,
    malicious_addresses: Vec<String>,
    analysis_timestamp: i64,
) -> Vec<HonestAddressRow> {
    let cutoff_time = if analysis_timestamp > 0 { analysis_timestamp } else { 0 };
    let relevant_token_ids: HashSet<String> = infringing_token_ids
        .into_iter()
        .filter(|token_id| !token_id.is_empty())
        .collect();
    let malicious_set: HashSet<String> = malicious_addresses
        .into_iter()
        .filter(|address| !address.is_empty())
        .collect();

    let mut owner_token_map: HashMap<String, HashSet<String>> = HashMap::new();
    for (owner_address, held_tokens) in owners.into_iter() {
        let filtered: HashSet<String> = held_tokens
            .into_iter()
            .filter(|token_id| relevant_token_ids.is_empty() || relevant_token_ids.contains(token_id))
            .collect();
        if !filtered.is_empty() {
            owner_token_map.insert(owner_address, filtered);
        }
    }

    let mut relevant_transfers: Vec<HonestTransferInput> = transfers
        .into_iter()
        .filter(|transfer| relevant_token_ids.is_empty() || relevant_token_ids.contains(&transfer.token_id))
        .collect();
    relevant_transfers.sort_by(|left, right| {
        (left.block_number, left.log_index, &left.tx_hash).cmp(&(right.block_number, right.log_index, &right.tx_hash))
    });

    let relevant_sales: Vec<(String, String, String)> = sales
        .into_iter()
        .filter(|(token_id, _buyer_address, _seller_address)| {
            relevant_token_ids.is_empty() || relevant_token_ids.contains(token_id)
        })
        .collect();

    let mut all_addresses: HashSet<String> = HashSet::new();
    for transfer in relevant_transfers.iter() {
        if !transfer.from_address.is_empty() && transfer.from_address != ZERO_ADDRESS {
            all_addresses.insert(transfer.from_address.clone());
        }
        if !transfer.to_address.is_empty() && transfer.to_address != ZERO_ADDRESS {
            all_addresses.insert(transfer.to_address.clone());
        }
    }
    for (_token_id, buyer_address, seller_address) in relevant_sales.iter() {
        if !buyer_address.is_empty() {
            all_addresses.insert(buyer_address.clone());
        }
        if !seller_address.is_empty() {
            all_addresses.insert(seller_address.clone());
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

    let mut transfers_by_token: HashMap<String, Vec<HonestTransferInput>> = HashMap::new();
    for transfer in relevant_transfers.into_iter() {
        transfers_by_token
            .entry(transfer.token_id.clone())
            .or_default()
            .push(transfer);
    }

    let mut token_interactions_by_address: HashMap<String, HashSet<String>> = HashMap::new();
    let mut durations_by_address: HashMap<String, Vec<i64>> = HashMap::new();
    let mut mint_to_honest_samples_by_address: HashMap<String, Vec<i64>> = HashMap::new();
    let mut honest_to_honest_count: HashMap<String, usize> = HashMap::new();
    let mut corrupted_addresses: HashSet<String> = HashSet::new();

    for (token_id, token_transfers) in transfers_by_token.into_iter() {
        let mut mint_time = 0_i64;
        let mut first_honest_recorded = false;
        let mut open_holds: HashMap<String, i64> = HashMap::new();
        let mut token_stats = TokenHonestStats {
            token_id: token_id.clone(),
            ..TokenHonestStats::default()
        };

        for transfer in token_transfers.iter() {
            if transfer.from_address == ZERO_ADDRESS && transfer.block_time > 0 {
                mint_time = transfer.block_time;
            }
            if honest_set.contains(&transfer.from_address) {
                token_stats.interacted_addresses.insert(transfer.from_address.clone());
                if let Some(start_time) = open_holds.remove(&transfer.from_address) {
                    if transfer.block_time >= start_time {
                        token_stats
                            .durations_by_address
                            .entry(transfer.from_address.clone())
                            .or_default()
                            .push(transfer.block_time - start_time);
                    }
                }
            }
            if honest_set.contains(&transfer.from_address) && honest_set.contains(&transfer.to_address) {
                token_stats.corrupted_addresses.insert(transfer.from_address.clone());
                *token_stats
                    .honest_to_honest_count
                    .entry(transfer.from_address.clone())
                    .or_insert(0) += 1;
            }
            if honest_set.contains(&transfer.to_address) {
                token_stats.interacted_addresses.insert(transfer.to_address.clone());
                if transfer.block_time > 0 {
                    open_holds.insert(transfer.to_address.clone(), transfer.block_time);
                    if mint_time > 0 && !first_honest_recorded {
                        token_stats
                            .mint_to_honest_samples_by_address
                            .entry(transfer.to_address.clone())
                            .or_default()
                            .push(std::cmp::max(0_i64, transfer.block_time - mint_time));
                        first_honest_recorded = true;
                    }
                }
            }
        }

        for (address, start_time) in open_holds.into_iter() {
            if !owner_token_map
                .get(&address)
                .map(|held_tokens| held_tokens.contains(&token_id))
                .unwrap_or(false)
            {
                continue;
            }
            if cutoff_time >= start_time {
                token_stats
                    .durations_by_address
                    .entry(address)
                    .or_default()
                    .push(cutoff_time - start_time);
            }
        }

        for address in token_stats.interacted_addresses.into_iter() {
            token_interactions_by_address
                .entry(address)
                .or_default()
                .insert(token_stats.token_id.clone());
        }
        for (address, durations) in token_stats.durations_by_address.into_iter() {
            durations_by_address.entry(address).or_default().extend(durations);
        }
        for (address, samples) in token_stats.mint_to_honest_samples_by_address.into_iter() {
            mint_to_honest_samples_by_address
                .entry(address)
                .or_default()
                .extend(samples);
        }
        for (address, count) in token_stats.honest_to_honest_count.into_iter() {
            *honest_to_honest_count.entry(address).or_insert(0) += count;
        }
        corrupted_addresses.extend(token_stats.corrupted_addresses.into_iter());
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
            let hold_durations = durations_by_address.get(&address).cloned().unwrap_or_default();

            HonestAddressRow {
                contract_address: contract_address.clone(),
                address: address.clone(),
                interacted_token_count: union_tokens.len(),
                currently_holding_token_count: current_tokens.len(),
                hold_duration_median_seconds: median_i64(&hold_durations),
                hold_duration_count: hold_durations.len(),
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

fn build_malicious_address_records_internal(
    contract_address: String,
    transfers: Vec<MaliciousTransferInput>,
    infringing_tokens: Vec<(String, String)>,
) -> Vec<MaliciousAddressRow> {
    let relevant_token_ids: HashSet<String> = infringing_tokens
        .iter()
        .filter_map(|(token_id, _)| if token_id.is_empty() { None } else { Some(token_id.clone()) })
        .collect();
    let mint_addresses: HashSet<String> = infringing_tokens
        .into_iter()
        .filter_map(|(_token_id, minter_address)| {
            if minter_address.is_empty() {
                None
            } else {
                Some(minter_address)
            }
        })
        .collect();

    let mut sorted_transfers = transfers;
    sorted_transfers.sort_by(|left, right| {
        (left.block_number, left.log_index, &left.tx_hash).cmp(&(right.block_number, right.log_index, &right.tx_hash))
    });

    let mut outgoing: HashMap<String, HashSet<String>> = HashMap::new();
    let mut cycle_counts: HashMap<String, usize> = HashMap::new();
    let mut seen_pairs: HashSet<(String, String)> = HashSet::new();
    let mut rapid_addresses: HashSet<String> = HashSet::new();
    let mut mint_times: HashMap<String, i64> = HashMap::new();
    let mut receiver_candidates: HashSet<String> = HashSet::new();

    for transfer in sorted_transfers.iter() {
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
                *cycle_counts.entry(transfer.from_address.clone()).or_insert(0) += 1;
                *cycle_counts.entry(transfer.to_address.clone()).or_insert(0) += 1;
            }
            seen_pairs.insert(pair);
        }
        let mint_time = *mint_times.get(&transfer.token_id).unwrap_or(&0);
        if mint_time > 0 && transfer.block_time > 0 && transfer.block_time - mint_time <= 24 * 3600 {
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
        .chain(receiver_candidates.into_iter())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    candidate_addresses.sort();

    let mut rows = Vec::new();
    for address in candidate_addresses.into_iter() {
        if address.is_empty() {
            continue;
        }
        let mint_role = mint_addresses.contains(&address);
        let wash_cycle_count = *cycle_counts.get(&address).unwrap_or(&0);
        let star_out_degree = outgoing.get(&address).map(|v| v.len()).unwrap_or(0);
        let is_star_distributor = star_out_degree >= 3;
        if !mint_role && wash_cycle_count == 0 && !is_star_distributor {
            continue;
        }
        rows.push(MaliciousAddressRow {
            address,
            mint_role,
            wash_cycle_count,
            star_out_degree,
            rapid_spread_contracts: Vec::new(),
            evidence_contracts: vec![contract_address.clone()],
        });
    }

    for row in rows.iter_mut() {
        if rapid_addresses.contains(&row.address) {
            row.rapid_spread_contracts = vec![contract_address.clone()];
        }
    }
    rows
}

fn build_infringing_token_records_internal(
    contract_address: String,
    contract_candidates: Vec<InfringingCandidateInput>,
    transfers: Vec<InfringingTokenTransferInput>,
    official_addresses: Vec<String>,
    candidate_open_license: Vec<(String, bool)>,
) -> Vec<InfringingTokenRow> {
    let official_set: HashSet<String> = official_addresses.into_iter().collect();
    let open_license_map: HashMap<String, bool> = candidate_open_license.into_iter().collect();
    let mut transfers_by_token: HashMap<String, Vec<InfringingTokenTransferInput>> = HashMap::new();
    for transfer in transfers.into_iter() {
        if transfer.token_id.is_empty() {
            continue;
        }
        transfers_by_token
            .entry(transfer.token_id.clone())
            .or_default()
            .push(transfer);
    }
    for token_transfers in transfers_by_token.values_mut() {
        token_transfers.sort_by(|left, right| {
            (left.block_number, left.log_index, &left.tx_hash).cmp(&(right.block_number, right.log_index, &right.tx_hash))
        });
    }

    let mut rows: Vec<InfringingTokenRow> = if contract_candidates.len() >= 512 {
        contract_candidates
            .par_iter()
            .map(|candidate| {
                let token_transfers = transfers_by_token.get(&candidate.token_id);
                let mint_transfer = token_transfers.and_then(|rows| rows.iter().find(|row| row.from_address == ZERO_ADDRESS));
                let first_transfer = token_transfers.and_then(|rows| rows.first());
                let (minter_address, mint_tx_hash, mint_block, first_transfer_time) =
                    if let Some(mint_transfer) = mint_transfer {
                        (
                            mint_transfer.to_address.clone(),
                            mint_transfer.tx_hash.clone(),
                            mint_transfer.block_number,
                            mint_transfer.block_time,
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
                InfringingTokenRow {
                    contract_address: contract_address.clone(),
                    token_id: candidate.token_id.clone(),
                    mint_tx_hash,
                    mint_block,
                    minter_address: minter_address.clone(),
                    first_transfer_time,
                    history_window: "full".to_string(),
                    match_reasons: candidate.match_reasons.clone(),
                    candidate_open_license: *open_license_map.get(&candidate.token_id).unwrap_or(&false),
                    official_or_legit_reissue: !minter_address.is_empty() && official_set.contains(&minter_address),
                }
            })
            .collect()
    } else {
        contract_candidates
            .iter()
            .map(|candidate| {
                let token_transfers = transfers_by_token.get(&candidate.token_id);
                let mint_transfer = token_transfers.and_then(|rows| rows.iter().find(|row| row.from_address == ZERO_ADDRESS));
                let first_transfer = token_transfers.and_then(|rows| rows.first());
                let (minter_address, mint_tx_hash, mint_block, first_transfer_time) =
                    if let Some(mint_transfer) = mint_transfer {
                        (
                            mint_transfer.to_address.clone(),
                            mint_transfer.tx_hash.clone(),
                            mint_transfer.block_number,
                            mint_transfer.block_time,
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
                InfringingTokenRow {
                    contract_address: contract_address.clone(),
                    token_id: candidate.token_id.clone(),
                    mint_tx_hash,
                    mint_block,
                    minter_address: minter_address.clone(),
                    first_transfer_time,
                    history_window: "full".to_string(),
                    match_reasons: candidate.match_reasons.clone(),
                    candidate_open_license: *open_license_map.get(&candidate.token_id).unwrap_or(&false),
                    official_or_legit_reissue: !minter_address.is_empty() && official_set.contains(&minter_address),
                }
            })
            .collect()
    };
    rows.sort_by(|left, right| (&left.token_id, &left.contract_address).cmp(&(&right.token_id, &right.contract_address)));
    rows
}

#[pyfunction]
pub fn build_victim_address_records(
    py: Python<'_>,
    sales: Vec<(
        String,
        String,
        i64,
        i64,
        i64,
        String,
        bool,
        Option<f64>,
        Option<f64>,
        Option<f64>,
        Option<f64>,
        String,
    )>,
    transfers: Vec<(String, String, i64, i64, String)>,
    owners: Vec<(String, Vec<String>)>,
) -> PyResult<Vec<PyObject>> {
    let sales: Vec<VictimSaleInput> = sales
        .into_iter()
        .map(
            |(
                token_id,
                tx_hash,
                block_number,
                log_index,
                bundle_index,
                buyer_address,
                is_eth_priced,
                price_eth,
                buy_before_eth_balance,
                buy_asset_ratio,
                buy_asset_ratio_with_gas,
                ratio_status,
            )| VictimSaleInput {
                token_id,
                tx_hash,
                block_number,
                log_index,
                bundle_index,
                buyer_address,
                is_eth_priced,
                price_eth,
                buy_before_eth_balance,
                buy_asset_ratio,
                buy_asset_ratio_with_gas,
                ratio_status,
            },
        )
        .collect();

    let rows = py.allow_threads(|| build_victim_address_records_internal(sales, transfers, owners));
    let mut output = Vec::with_capacity(rows.len());
    for row in rows.into_iter() {
        let result = PyDict::new_bound(py);
        result.set_item("address", row.address)?;
        result.set_item("buy_tx_hashes", row.buy_tx_hashes)?;
        result.set_item("buy_amount_eth", row.buy_amount_eth)?;
        result.set_item("last_buy_amount_eth", row.last_buy_amount_eth)?;
        result.set_item("buy_before_eth_balance", row.buy_before_eth_balance)?;
        result.set_item("buy_asset_ratio", row.buy_asset_ratio)?;
        result.set_item("buy_asset_ratio_with_gas", row.buy_asset_ratio_with_gas)?;
        result.set_item("is_stuck", row.is_stuck)?;
        result.set_item("last_buy_tx_hash", row.last_buy_tx_hash)?;
        result.set_item("ratio_status", row.ratio_status)?;
        output.push(result.into_any().unbind());
    }
    Ok(output)
}

#[pyfunction]
pub fn build_honest_address_records(
    py: Python<'_>,
    contract_address: String,
    transfers: Vec<(String, String, i64, i64, i64, String, String)>,
    sales: Vec<(String, String, String)>,
    owners: Vec<(String, Vec<String>)>,
    infringing_token_ids: Vec<String>,
    malicious_addresses: Vec<String>,
    analysis_timestamp: i64,
) -> PyResult<Vec<PyObject>> {
    let transfers: Vec<HonestTransferInput> = transfers
        .into_iter()
        .map(
            |(
                token_id,
                tx_hash,
                block_number,
                log_index,
                block_time,
                from_address,
                to_address,
            )| HonestTransferInput {
                token_id,
                tx_hash,
                block_number,
                log_index,
                block_time,
                from_address,
                to_address,
            },
        )
        .collect();

    let rows = py.allow_threads(|| {
        build_honest_address_records_internal(
            contract_address,
            transfers,
            sales,
            owners,
            infringing_token_ids,
            malicious_addresses,
            analysis_timestamp,
        )
    });

    let mut output = Vec::with_capacity(rows.len());
    for row in rows.into_iter() {
        let result = PyDict::new_bound(py);
        result.set_item("contract_address", row.contract_address)?;
        result.set_item("address", row.address)?;
        result.set_item("interacted_token_count", row.interacted_token_count)?;
        result.set_item("currently_holding_token_count", row.currently_holding_token_count)?;
        result.set_item("hold_duration_median_seconds", row.hold_duration_median_seconds)?;
        result.set_item("hold_duration_count", row.hold_duration_count)?;
        result.set_item("is_corrupted_address", row.is_corrupted_address)?;
        result.set_item("honest_sale_to_honest_count", row.honest_sale_to_honest_count)?;
        result.set_item(
            "mint_to_honest_seconds_samples",
            row.mint_to_honest_seconds_samples,
        )?;
        output.push(result.into_any().unbind());
    }
    Ok(output)
}

#[pyfunction]
pub fn build_malicious_address_records(
    py: Python<'_>,
    contract_address: String,
    transfers: Vec<(String, String, i64, i64, i64, String, String)>,
    infringing_tokens: Vec<(String, String)>,
) -> PyResult<Vec<PyObject>> {
    let transfers: Vec<MaliciousTransferInput> = transfers
        .into_iter()
        .map(
            |(
                token_id,
                tx_hash,
                block_number,
                log_index,
                block_time,
                from_address,
                to_address,
            )| MaliciousTransferInput {
                token_id,
                tx_hash,
                block_number,
                log_index,
                block_time,
                from_address,
                to_address,
            },
        )
        .collect();

    let rows = py.allow_threads(|| {
        build_malicious_address_records_internal(contract_address, transfers, infringing_tokens)
    });
    let mut output = Vec::with_capacity(rows.len());
    for row in rows.into_iter() {
        let result = PyDict::new_bound(py);
        result.set_item("address", row.address)?;
        result.set_item("mint_role", row.mint_role)?;
        result.set_item("wash_cycle_count", row.wash_cycle_count)?;
        result.set_item("star_out_degree", row.star_out_degree)?;
        result.set_item("rapid_spread_contracts", row.rapid_spread_contracts)?;
        result.set_item("evidence_contracts", row.evidence_contracts)?;
        output.push(result.into_any().unbind());
    }
    Ok(output)
}

#[pyfunction]
pub fn build_infringing_token_records(
    py: Python<'_>,
    contract_address: String,
    contract_candidates: Vec<(String, Vec<String>)>,
    transfers: Vec<(String, String, i64, i64, i64, String, String)>,
    official_addresses: Vec<String>,
    candidate_open_license: Vec<(String, bool)>,
) -> PyResult<Vec<PyObject>> {
    let candidates: Vec<InfringingCandidateInput> = contract_candidates
        .into_iter()
        .map(|(token_id, match_reasons)| InfringingCandidateInput {
            token_id,
            match_reasons,
        })
        .collect();
    let transfers: Vec<InfringingTokenTransferInput> = transfers
        .into_iter()
        .map(
            |(
                token_id,
                tx_hash,
                block_number,
                log_index,
                block_time,
                from_address,
                to_address,
            )| InfringingTokenTransferInput {
                token_id,
                tx_hash,
                block_number,
                log_index,
                block_time,
                from_address,
                to_address,
            },
        )
        .collect();

    let rows = py.allow_threads(|| {
        build_infringing_token_records_internal(
            contract_address,
            candidates,
            transfers,
            official_addresses,
            candidate_open_license,
        )
    });
    let mut output = Vec::with_capacity(rows.len());
    for row in rows.into_iter() {
        let result = PyDict::new_bound(py);
        result.set_item("contract_address", row.contract_address)?;
        result.set_item("token_id", row.token_id)?;
        result.set_item("mint_tx_hash", row.mint_tx_hash)?;
        result.set_item("mint_block", row.mint_block)?;
        result.set_item("minter_address", row.minter_address)?;
        result.set_item("first_transfer_time", row.first_transfer_time)?;
        result.set_item("history_window", row.history_window)?;
        result.set_item("match_reasons", row.match_reasons)?;
        result.set_item("candidate_open_license", row.candidate_open_license)?;
        result.set_item("official_or_legit_reissue", row.official_or_legit_reissue)?;
        output.push(result.into_any().unbind());
    }
    Ok(output)
}
