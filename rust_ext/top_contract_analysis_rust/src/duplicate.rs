use crate::common::{
    metadata_document, metadata_keywords_internal, metadata_score_normalized_documents,
    name_similarity_normalized, normalize_name, normalize_symbol, normalize_text, normalize_url,
};
use pyo3::prelude::*;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};

#[derive(Clone)]
struct DuplicateSeedInput {
    contract_address: String,
    name: String,
    symbol: String,
    token_uri: String,
    image_uri: String,
    metadata_json: String,
    metadata_doc: String,
}

#[derive(Clone)]
struct SnapshotNftInput {
    contract_address: String,
    token_id: String,
    name: String,
    symbol: String,
    token_uri: String,
    image_uri: String,
    metadata_json: String,
    metadata_doc: String,
}

fn candidate_length_bounds(length: usize, threshold: f64) -> (usize, usize) {
    if length == 0 {
        return (1, 0);
    }
    let safe_threshold = if threshold <= 0.0 { 1.0 } else { threshold };
    let factor = (200.0 - safe_threshold) / safe_threshold;
    let min_length = std::cmp::max(1, ((length as f64) / factor).ceil() as usize);
    let max_length = ((length as f64) * factor) as usize;
    (min_length, max_length)
}

fn has_name_match(
    row_name_norm: &str,
    name_threshold: f64,
    seed_names_by_length: &HashMap<usize, Vec<String>>,
    sorted_seed_name_lengths: &[usize],
) -> bool {
    if row_name_norm.is_empty() {
        return false;
    }
    let (min_length, max_length) = candidate_length_bounds(row_name_norm.chars().count(), name_threshold);
    for length in sorted_seed_name_lengths.iter().copied() {
        if length < min_length {
            continue;
        }
        if length > max_length {
            break;
        }
        if let Some(candidates) = seed_names_by_length.get(&length) {
            for candidate in candidates.iter() {
                if name_similarity_normalized(row_name_norm, candidate) >= name_threshold {
                    return true;
                }
            }
        }
    }
    false
}

fn build_duplicate_candidates_internal(
    seed_nfts: Vec<DuplicateSeedInput>,
    snapshot_rows: Vec<SnapshotNftInput>,
    name_threshold: f64,
    metadata_threshold: f64,
) -> Vec<(String, String, Vec<String>, String, String, String, String, String)> {
    let seed_contracts: HashSet<String> = seed_nfts
        .iter()
        .map(|item| item.contract_address.to_lowercase())
        .collect();
    let seed_token_uri_keys: HashSet<String> = seed_nfts
        .iter()
        .filter_map(|item| normalize_url(&item.token_uri))
        .collect();
    let seed_image_uri_keys: HashSet<String> = seed_nfts
        .iter()
        .filter_map(|item| normalize_url(&item.image_uri))
        .collect();
    let seed_symbol_norms: HashSet<String> = seed_nfts
        .iter()
        .map(|item| normalize_symbol(&item.symbol))
        .filter(|symbol| !symbol.is_empty())
        .collect();

    let mut seed_names_by_length: HashMap<usize, Vec<String>> = HashMap::new();
    let mut sorted_seed_name_lengths: Vec<usize> = Vec::new();
    for item in seed_nfts.iter() {
        let name_norm = normalize_name(&item.name);
        if name_norm.is_empty() {
            continue;
        }
        let len = name_norm.chars().count();
        let bucket = seed_names_by_length.entry(len).or_default();
        if bucket.is_empty() {
            sorted_seed_name_lengths.push(len);
        }
        bucket.push(name_norm);
    }
    sorted_seed_name_lengths.sort_unstable();
    sorted_seed_name_lengths.dedup();

    let seed_metadata_docs_and_keywords: Vec<(String, HashSet<String>)> = seed_nfts
        .iter()
        .filter_map(|item| {
            let seed_doc = if !item.metadata_doc.is_empty() {
                normalize_text(&item.metadata_doc)
            } else {
                metadata_document(&item.metadata_json)
            };
            if seed_doc.is_empty() {
                None
            } else {
                Some((seed_doc.clone(), metadata_keywords_internal(&seed_doc, 12).into_iter().collect()))
            }
        })
        .collect();
    let seed_metadata_keyword_union: HashSet<String> = seed_metadata_docs_and_keywords
        .iter()
        .flat_map(|(_, keywords)| keywords.iter().cloned())
        .collect();

    let mut rows: Vec<(String, String, Vec<String>, String, String, String, String, String)> = snapshot_rows
        .par_iter()
        .filter(|row| !seed_contracts.contains(&row.contract_address.to_lowercase()))
        .filter_map(|row| {
            let token_key = normalize_url(&row.token_uri);
            let image_key = normalize_url(&row.image_uri);
            let symbol_norm = normalize_symbol(&row.symbol);
            let row_name_norm = normalize_name(&row.name);
            let row_doc = if !row.metadata_doc.is_empty() {
                normalize_text(&row.metadata_doc)
            } else {
                metadata_document(&row.metadata_json)
            };
            let row_keywords: HashSet<String> = if row_doc.is_empty() {
                HashSet::new()
            } else {
                metadata_keywords_internal(&row_doc, 12).into_iter().collect()
            };

            let mut reasons: Vec<String> = Vec::new();
            if token_key
                .as_ref()
                .map(|value| seed_token_uri_keys.contains(value))
                .unwrap_or(false)
            {
                reasons.push("token_uri_match".to_string());
            }
            if image_key
                .as_ref()
                .map(|value| seed_image_uri_keys.contains(value))
                .unwrap_or(false)
            {
                reasons.push("image_uri_match".to_string());
            }
            if !symbol_norm.is_empty() && seed_symbol_norms.contains(&symbol_norm) {
                reasons.push("symbol_match".to_string());
            }
            if has_name_match(
                &row_name_norm,
                name_threshold,
                &seed_names_by_length,
                &sorted_seed_name_lengths,
            ) {
                reasons.push("name_match".to_string());
            }
            if !row_doc.is_empty()
                && (!seed_metadata_keyword_union.is_empty()
                    && !row_keywords.is_empty()
                    && !row_keywords.is_disjoint(&seed_metadata_keyword_union))
                && seed_metadata_docs_and_keywords.iter().any(|(seed_doc, seed_keywords)| {
                    if !row_keywords.is_empty()
                        && !seed_keywords.is_empty()
                        && row_keywords.is_disjoint(seed_keywords)
                    {
                        return false;
                    }
                    metadata_score_normalized_documents(seed_doc, &row_doc) >= metadata_threshold
                })
            {
                reasons.push("metadata_match".to_string());
            }
            if reasons.is_empty() {
                return None;
            }
            reasons.sort();
            reasons.dedup();
            let confidence = if reasons.iter().any(|reason| {
                matches!(
                    reason.as_str(),
                    "token_uri_match" | "image_uri_match" | "metadata_match"
                )
            }) || (reasons.contains(&"name_match".to_string()) && reasons.contains(&"symbol_match".to_string()))
            {
                "high".to_string()
            } else {
                "low".to_string()
            };
            Some((
                row.contract_address.clone(),
                row.token_id.clone(),
                reasons,
                confidence,
                row.token_uri.clone(),
                row.image_uri.clone(),
                row.name.clone(),
                row.symbol.clone(),
            ))
        })
        .collect();
    rows.sort_by(|left, right| (&left.0, &left.1).cmp(&(&right.0, &right.1)));
    rows
}

#[pyfunction]
pub fn build_duplicate_candidates(
    py: Python<'_>,
    seed_nfts: Vec<(String, String, String, String, String, String, String, String)>,
    snapshot_rows: Vec<(String, String, String, String, String, String, String, String)>,
    name_threshold: f64,
    metadata_threshold: f64,
) -> PyResult<Vec<(String, String, Vec<String>, String, String, String, String, String)>> {
    let seed_nfts: Vec<DuplicateSeedInput> = seed_nfts
        .into_iter()
        .map(
            |(
                contract_address,
                _token_id,
                name,
                symbol,
                token_uri,
                image_uri,
                metadata_json,
                metadata_doc,
            )| DuplicateSeedInput {
                contract_address,
                name,
                symbol,
                token_uri,
                image_uri,
                metadata_json,
                metadata_doc,
            },
        )
        .collect();
    let snapshot_rows: Vec<SnapshotNftInput> = snapshot_rows
        .into_iter()
        .map(
            |(
                contract_address,
                token_id,
                name,
                symbol,
                token_uri,
                image_uri,
                metadata_json,
                metadata_doc,
            )| SnapshotNftInput {
                contract_address,
                token_id,
                name,
                symbol,
                token_uri,
                image_uri,
                metadata_json,
                metadata_doc,
            },
        )
        .collect();

    Ok(py.allow_threads(|| {
        build_duplicate_candidates_internal(seed_nfts, snapshot_rows, name_threshold, metadata_threshold)
    }))
}
