use std::collections::{HashMap, HashSet};

use once_cell::sync::Lazy;
use regex::Regex;

use crate::analysis::scoring::{metadata_document_from_json, score_metadata_documents, score_name_pairs};
use crate::models::{DatabaseNftRecord, DuplicateCandidate, SeedNft};
use crate::normalize::{normalize_name, normalize_symbol, normalize_text, normalize_url};

static TOKEN_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"[\p{L}\p{N}_]+").unwrap());

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

fn metadata_keywords(document: &str, limit: usize) -> Vec<String> {
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
    ranked
        .into_iter()
        .take(limit)
        .map(|(token, _)| token)
        .collect()
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
    let (min_length, max_length) =
        candidate_length_bounds(row_name_norm.chars().count(), name_threshold);
    for length in sorted_seed_name_lengths.iter().copied() {
        if length < min_length {
            continue;
        }
        if length > max_length {
            break;
        }
        if let Some(candidates) = seed_names_by_length.get(&length) {
            let left = vec![row_name_norm.to_string(); candidates.len()];
            let right = candidates.clone();
            if score_name_pairs(&left, &right)
                .map(|scores| scores.into_iter().any(|score| score >= name_threshold))
                .unwrap_or(false)
            {
                return true;
            }
        }
    }
    false
}

pub fn build_duplicate_candidates(
    seed_nfts: &[SeedNft],
    snapshot_rows: &[DatabaseNftRecord],
    name_threshold: f64,
    metadata_threshold: f64,
) -> Vec<DuplicateCandidate> {
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
    let mut sorted_seed_name_lengths = Vec::new();
    for item in seed_nfts {
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
                metadata_document_from_json(&item.metadata_json)
            };
            if seed_doc.is_empty() {
                None
            } else {
                Some((
                    seed_doc.clone(),
                    metadata_keywords(&seed_doc, 12).into_iter().collect(),
                ))
            }
        })
        .collect();
    let seed_metadata_keyword_union: HashSet<String> = seed_metadata_docs_and_keywords
        .iter()
        .flat_map(|(_, keywords)| keywords.iter().cloned())
        .collect();

    let mut rows = Vec::new();
    for row in snapshot_rows {
        if seed_contracts.contains(&row.contract_address.to_lowercase()) {
            continue;
        }

        let token_key = normalize_url(&row.token_uri);
        let image_key = normalize_url(&row.image_uri);
        let symbol_norm = normalize_symbol(&row.symbol);
        let row_name_norm = normalize_name(&row.name);
        let row_doc = if !row.metadata_doc.is_empty() {
            normalize_text(&row.metadata_doc)
        } else {
            metadata_document_from_json(&row.metadata_json)
        };
        let row_keywords: HashSet<String> = if row_doc.is_empty() {
            HashSet::new()
        } else {
            metadata_keywords(&row_doc, 12).into_iter().collect()
        };

        let mut reasons = Vec::new();
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
                score_metadata_documents(&[seed_doc.clone()], &[row_doc.clone()])
                    .map(|scores| scores[0] >= metadata_threshold)
                    .unwrap_or(false)
            })
        {
            reasons.push("metadata_match".to_string());
        }

        if reasons.is_empty() {
            continue;
        }
        reasons.sort();
        reasons.dedup();
        let has_high_reason = reasons.iter().any(|reason| {
            matches!(
                reason.as_str(),
                "token_uri_match" | "image_uri_match" | "metadata_match"
            )
        });
        let has_name_and_symbol =
            reasons.iter().any(|reason| reason == "name_match")
                && reasons.iter().any(|reason| reason == "symbol_match");
        let confidence = if has_high_reason || has_name_and_symbol {
            "high"
        } else {
            "low"
        };

        rows.push(DuplicateCandidate {
            contract_address: row.contract_address.clone(),
            token_id: row.token_id.clone(),
            match_reasons: reasons,
            confidence: confidence.to_string(),
            token_uri: row.token_uri.clone(),
            image_uri: row.image_uri.clone(),
            name: row.name.clone(),
            symbol: row.symbol.clone(),
        });
    }

    rows.sort_by(|left, right| {
        (&left.contract_address, &left.token_id).cmp(&(&right.contract_address, &right.token_id))
    });
    rows
}
