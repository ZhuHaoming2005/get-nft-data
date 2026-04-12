use once_cell::sync::Lazy;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use rayon::prelude::*;
use regex::Regex;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use strsim::{jaro_winkler, normalized_levenshtein};
use unicode_normalization::UnicodeNormalization;

const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

static TRAILING_PATTERNS: Lazy<Vec<Regex>> = Lazy::new(|| {
    vec![
        Regex::new(r"\s*#\s*[0-9a-fA-FxX]+\s*$").unwrap(),
        Regex::new(r"\s*#\s*\d+\s*$").unwrap(),
        Regex::new(r"\s*-\s*\d+\s*$").unwrap(),
        Regex::new(r"\s*:\s*\d+\s*$").unwrap(),
        Regex::new(r"\s*\(\s*\d+\s*\)\s*$").unwrap(),
        Regex::new(r"\s*\[\s*\d+\s*\]\s*$").unwrap(),
        Regex::new(r"\s*/\s*\d+\s*$").unwrap(),
        Regex::new(r"\s+No\.?\s*\d+\s*$").unwrap(),
        Regex::new(r"\s+nr\.?\s*\d+\s*$").unwrap(),
        Regex::new(r"\s+\d{1,12}\s*$").unwrap(),
    ]
});

static WHITESPACE_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\s+").unwrap());
static TOKEN_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"[\p{L}\p{N}_]+").unwrap());

fn normalize_nfkc(raw: &str) -> String {
    raw.nfkc().collect::<String>()
}

fn strip_trailing_number_suffix(raw: &str) -> String {
    let mut text = normalize_nfkc(raw).trim().to_string();
    let mut changed = true;
    let mut guard = 0;
    while changed && guard < 20 {
        changed = false;
        guard += 1;
        for pattern in TRAILING_PATTERNS.iter() {
            let updated = pattern.replace(&text, "").trim().to_string();
            if updated != text {
                text = updated;
                changed = true;
                break;
            }
        }
    }
    WHITESPACE_RE.replace_all(&text, " ").trim().to_string()
}

fn normalize_name(raw: &str) -> String {
    strip_trailing_number_suffix(raw).to_lowercase()
}

fn normalize_text(raw: &str) -> String {
    let text = normalize_nfkc(raw).to_lowercase();
    WHITESPACE_RE.replace_all(text.trim(), " ").to_string()
}

fn flatten_metadata(value: &Value, parts: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            for (key, item) in map.iter() {
                let key_norm = key.to_lowercase();
                if matches!(
                    key_norm.as_str(),
                    "description"
                        | "trait_type"
                        | "value"
                        | "display_type"
                        | "image"
                        | "image_url"
                        | "animation_url"
                        | "external_url"
                        | "attributes"
                        | "metadata"
                        | "rawmetadata"
                        | "raw"
                ) {
                    flatten_metadata(item, parts);
                }
            }
        }
        Value::Array(items) => {
            for item in items.iter() {
                flatten_metadata(item, parts);
            }
        }
        Value::String(text) => {
            if !text.trim().is_empty() {
                parts.push(text.trim().to_string());
            }
        }
        _ => {}
    }
}

fn metadata_document(raw: &str) -> String {
    if raw.trim().is_empty() {
        return String::new();
    }
    match serde_json::from_str::<Value>(raw) {
        Ok(value) => {
            let mut parts = Vec::new();
            flatten_metadata(&value, &mut parts);
            normalize_text(&parts.join(" "))
        }
        Err(_) => normalize_text(raw),
    }
}

fn tokenize(document: &str) -> HashSet<String> {
    TOKEN_RE
        .find_iter(document)
        .map(|m| m.as_str().to_lowercase())
        .filter(|token| token.len() >= 2)
        .collect()
}

fn metadata_keywords_internal(document: &str, limit: usize) -> Vec<String> {
    let mut counts: HashMap<String, usize> = HashMap::new();
    for token in TOKEN_RE.find_iter(document) {
        let normalized = token.as_str().to_lowercase();
        if normalized.len() < 4 {
            continue;
        }
        *counts.entry(normalized).or_insert(0) += 1;
    }
    let mut ranked: Vec<(String, usize)> = counts.into_iter().collect();
    ranked.sort_by(|left, right| {
        right
            .1
            .cmp(&left.1)
            .then_with(|| right.0.len().cmp(&left.0.len()))
            .then_with(|| left.0.cmp(&right.0))
    });
    ranked.into_iter().take(limit).map(|(token, _)| token).collect()
}

fn name_score(left: &str, right: &str) -> f64 {
    let left_norm = normalize_name(left);
    let right_norm = normalize_name(right);
    if left_norm.is_empty() || right_norm.is_empty() {
        return 0.0;
    }
    if left_norm == right_norm {
        return 100.0;
    }
    let jaro = jaro_winkler(&left_norm, &right_norm);
    let levenshtein = normalized_levenshtein(&left_norm, &right_norm);
    ((jaro * 0.65) + (levenshtein * 0.35)) * 100.0
}

fn metadata_score(left: &str, right: &str) -> f64 {
    let left_doc = metadata_document(left);
    let right_doc = metadata_document(right);
    metadata_score_from_documents(&left_doc, &right_doc)
}

fn metadata_score_from_documents(left: &str, right: &str) -> f64 {
    let left_doc = normalize_text(left);
    let right_doc = normalize_text(right);
    if left_doc.is_empty() || right_doc.is_empty() {
        return 0.0;
    }
    let left_tokens = tokenize(&left_doc);
    let right_tokens = tokenize(&right_doc);
    let union = left_tokens.union(&right_tokens).count();
    let overlap = left_tokens.intersection(&right_tokens).count();
    let jaccard = if union == 0 {
        0.0
    } else {
        overlap as f64 / union as f64
    };
    let similarity = jaro_winkler(&left_doc, &right_doc);
    (jaccard * 0.45) + (similarity * 0.55)
}

fn canonical_pair(left: &str, right: &str) -> (String, String) {
    if left <= right {
        (left.to_string(), right.to_string())
    } else {
        (right.to_string(), left.to_string())
    }
}

#[pyfunction]
fn score_name_pairs(py: Python<'_>, left: Vec<String>, right: Vec<String>) -> PyResult<Vec<f64>> {
    if left.len() != right.len() {
        return Err(PyValueError::new_err(
            "left and right sequences must have identical lengths",
        ));
    }
    Ok(py.allow_threads(|| {
        left.par_iter()
            .zip(right.par_iter())
            .map(|(l, r)| name_score(l, r))
            .collect()
    }))
}

#[pyfunction]
fn score_metadata_pairs(py: Python<'_>, left: Vec<String>, right: Vec<String>) -> PyResult<Vec<f64>> {
    if left.len() != right.len() {
        return Err(PyValueError::new_err(
            "left and right sequences must have identical lengths",
        ));
    }
    Ok(py.allow_threads(|| {
        left.par_iter()
            .zip(right.par_iter())
            .map(|(l, r)| metadata_score(l, r))
            .collect()
    }))
}

#[pyfunction]
fn score_metadata_documents(
    py: Python<'_>,
    left: Vec<String>,
    right: Vec<String>,
) -> PyResult<Vec<f64>> {
    if left.len() != right.len() {
        return Err(PyValueError::new_err(
            "left and right sequences must have identical lengths",
        ));
    }
    Ok(py.allow_threads(|| {
        left.par_iter()
            .zip(right.par_iter())
            .map(|(l, r)| metadata_score_from_documents(l, r))
            .collect()
    }))
}

#[pyfunction]
fn metadata_document_from_json(py: Python<'_>, raw: String) -> PyResult<String> {
    Ok(py.allow_threads(|| metadata_document(&raw)))
}

#[pyfunction(signature = (document, limit=8))]
fn metadata_keywords(py: Python<'_>, document: String, limit: usize) -> PyResult<Vec<String>> {
    Ok(py.allow_threads(|| metadata_keywords_internal(&document, limit)))
}

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
            .map(|transfer_key| {
                transfer_key
                    > &(sale.block_number, sale.log_index, sale.tx_hash.clone())
            })
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
            entry.last_buy_amount_eth = if sale.is_eth_priced {
                sale.price_eth
            } else {
                None
            };
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
    let cutoff_time = if analysis_timestamp > 0 {
        analysis_timestamp
    } else {
        0
    };
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
        (
            left.block_number,
            left.log_index,
            &left.tx_hash,
        )
            .cmp(&(
                right.block_number,
                right.log_index,
                &right.tx_hash,
            ))
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

#[pyfunction]
fn analyze_transfer_signals(
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
fn analyze_victim_signals(
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

#[pyfunction]
fn build_victim_address_records(
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
fn build_honest_address_records(
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

#[pymodule]
fn top_contract_analysis_rust(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(score_name_pairs, m)?)?;
    m.add_function(wrap_pyfunction!(score_metadata_pairs, m)?)?;
    m.add_function(wrap_pyfunction!(score_metadata_documents, m)?)?;
    m.add_function(wrap_pyfunction!(metadata_document_from_json, m)?)?;
    m.add_function(wrap_pyfunction!(metadata_keywords, m)?)?;
    m.add_function(wrap_pyfunction!(analyze_transfer_signals, m)?)?;
    m.add_function(wrap_pyfunction!(analyze_victim_signals, m)?)?;
    m.add_function(wrap_pyfunction!(build_victim_address_records, m)?)?;
    m.add_function(wrap_pyfunction!(build_honest_address_records, m)?)?;
    Ok(())
}
