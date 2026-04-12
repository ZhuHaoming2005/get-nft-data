use crate::common::{canonical_pair, ZERO_ADDRESS};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::collections::{HashMap, HashSet};

fn analyze_transfer_signals_internal(
    transfers: Vec<(String, String, i64)>,
) -> (usize, usize, usize, usize, usize, i64, bool) {
    let mut mint_recipients: HashSet<String> = HashSet::new();
    let mut receiver_addresses: HashSet<String> = HashSet::new();
    let mut seen_pairs: HashSet<(String, String)> = HashSet::new();
    let mut cycle_pairs: HashSet<(String, String)> = HashSet::new();
    let mut outgoing: HashMap<String, HashSet<String>> = HashMap::new();
    let mut incoming: HashMap<String, usize> = HashMap::new();
    let mut mint_count: usize = 0;
    let mut first_mint_time: i64 = 0;
    let mut first_non_mint_time: i64 = 0;

    for (from_address, to_address, block_time) in transfers.iter() {
        if !to_address.is_empty() && to_address != ZERO_ADDRESS {
            receiver_addresses.insert(to_address.clone());
        }
        if from_address == ZERO_ADDRESS {
            mint_count += 1;
            if !to_address.is_empty() {
                mint_recipients.insert(to_address.clone());
            }
            if *block_time > 0 && (first_mint_time == 0 || *block_time < first_mint_time) {
                first_mint_time = *block_time;
            }
            continue;
        }

        if *block_time > 0 && (first_non_mint_time == 0 || *block_time < first_non_mint_time) {
            first_non_mint_time = *block_time;
        }
        if to_address != ZERO_ADDRESS {
            outgoing
                .entry(from_address.clone())
                .or_default()
                .insert(to_address.clone());
            *incoming.entry(to_address.clone()).or_insert(0) += 1;
            let pair = (from_address.clone(), to_address.clone());
            let reverse = (to_address.clone(), from_address.clone());
            if seen_pairs.contains(&reverse) {
                cycle_pairs.insert(canonical_pair(from_address, to_address));
            }
            seen_pairs.insert(pair);
        }
    }

    let star_distributor_count = outgoing
        .iter()
        .filter(|(sender, recipients)| recipients.len() >= 3 && *incoming.get(*sender).unwrap_or(&0) <= 1)
        .count();
    let mut first_transfer_delay = 0_i64;
    if first_mint_time > 0 && first_non_mint_time >= first_mint_time {
        first_transfer_delay = first_non_mint_time - first_mint_time;
    }
    let fast_spread = first_transfer_delay > 0 && first_transfer_delay <= 24 * 3600;

    (
        mint_recipients.len(),
        mint_count,
        receiver_addresses.len(),
        cycle_pairs.len(),
        star_distributor_count,
        first_transfer_delay,
        fast_spread,
    )
}

fn analyze_victim_signals_internal(
    transfers: Vec<(String, String, i64)>,
    owners: Vec<(String, bool)>,
) -> (usize, usize, f64, usize) {
    let active_sellers: HashSet<String> = transfers
        .into_iter()
        .filter_map(|(from_address, _to_address, _block_time)| {
            if !from_address.is_empty() && from_address != ZERO_ADDRESS {
                Some(from_address)
            } else {
                None
            }
        })
        .collect();

    let mut owner_count: usize = 0;
    let mut stuck_holder_count: usize = 0;
    for (owner_address, has_positive_balance) in owners.into_iter() {
        if !has_positive_balance {
            continue;
        }
        owner_count += 1;
        if !active_sellers.contains(&owner_address) {
            stuck_holder_count += 1;
        }
    }
    let stuck_holder_ratio = if owner_count == 0 {
        0.0
    } else {
        stuck_holder_count as f64 / owner_count as f64
    };

    (
        owner_count,
        stuck_holder_count,
        stuck_holder_ratio,
        stuck_holder_count,
    )
}

#[pyfunction]
pub fn analyze_transfer_signals(
    py: Python<'_>,
    transfers: Vec<(String, String, i64)>,
) -> PyResult<PyObject> {
    let (
        mint_address_count,
        mint_count,
        unique_receiver_count,
        cycle_edge_count,
        star_distributor_count,
        first_transfer_delay,
        fast_spread,
    ) = py.allow_threads(|| analyze_transfer_signals_internal(transfers));

    let result = PyDict::new_bound(py);
    result.set_item("mint_address_count", mint_address_count)?;
    result.set_item("mint_count", mint_count)?;
    result.set_item("unique_receiver_count", unique_receiver_count)?;
    result.set_item("cycle_edge_count", cycle_edge_count)?;
    result.set_item("star_distributor_count", star_distributor_count)?;
    result.set_item("mint_to_first_transfer_seconds", first_transfer_delay)?;
    result.set_item("fast_spread", fast_spread)?;
    Ok(result.into_any().unbind())
}

#[pyfunction]
pub fn analyze_victim_signals(
    py: Python<'_>,
    transfers: Vec<(String, String, i64)>,
    owners: Vec<(String, bool)>,
) -> PyResult<PyObject> {
    let (owner_count, stuck_holder_count, stuck_holder_ratio, victim_wallet_count) =
        py.allow_threads(|| analyze_victim_signals_internal(transfers, owners));

    let result = PyDict::new_bound(py);
    result.set_item("owner_count", owner_count)?;
    result.set_item("stuck_holder_count", stuck_holder_count)?;
    result.set_item("stuck_holder_ratio", stuck_holder_ratio)?;
    result.set_item("victim_wallet_count", victim_wallet_count)?;
    Ok(result.into_any().unbind())
}
