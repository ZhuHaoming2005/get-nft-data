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

#[pymodule]
fn top_contract_analysis_rust(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(score_name_pairs, m)?)?;
    m.add_function(wrap_pyfunction!(score_metadata_pairs, m)?)?;
    m.add_function(wrap_pyfunction!(score_metadata_documents, m)?)?;
    m.add_function(wrap_pyfunction!(metadata_document_from_json, m)?)?;
    m.add_function(wrap_pyfunction!(metadata_keywords, m)?)?;
    m.add_function(wrap_pyfunction!(analyze_transfer_signals, m)?)?;
    m.add_function(wrap_pyfunction!(analyze_victim_signals, m)?)?;
    Ok(())
}
