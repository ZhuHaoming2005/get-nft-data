use once_cell::sync::Lazy;
use regex::Regex;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use strsim::{jaro_winkler, normalized_levenshtein};
use unicode_normalization::UnicodeNormalization;

pub(crate) const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

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
static IPFS_HTTP_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)^https?://[^/]+/ipfs/([A-Za-z0-9][^?#\s]*)").unwrap());
static ARWEAVE_HTTP_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)^https?://(?:[^/]+\.)?arweave\.net/([A-Za-z0-9_-]{43}(?:/[^?#\s]*)?)")
        .unwrap()
});

pub(crate) fn normalize_nfkc(raw: &str) -> String {
    raw.nfkc().collect::<String>()
}

pub(crate) fn strip_trailing_number_suffix(raw: &str) -> String {
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

pub(crate) fn normalize_name(raw: &str) -> String {
    strip_trailing_number_suffix(raw).to_lowercase()
}

pub(crate) fn normalize_symbol(raw: &str) -> String {
    normalize_nfkc(raw).trim().to_lowercase()
}

pub(crate) fn normalize_text(raw: &str) -> String {
    let text = normalize_nfkc(raw).to_lowercase();
    WHITESPACE_RE.replace_all(text.trim(), " ").to_string()
}

pub(crate) fn normalize_url(raw: &str) -> Option<String> {
    let text = raw.trim();
    if text.is_empty() {
        return None;
    }
    let lowered = text.to_lowercase();
    if matches!(
        lowered.as_str(),
        "nano" | "null" | "none" | "undefined" | "n/a" | "na" | "-" | "." | "false" | "true" | "0"
    ) {
        return None;
    }
    if lowered.starts_with("data:") {
        return None;
    }
    if lowered.starts_with("ipfs://") {
        let mut tail = text[7..].to_string();
        if tail.to_lowercase().starts_with("ipfs/") {
            tail = tail[5..].to_string();
        }
        let cid_path = tail
            .split('?')
            .next()
            .unwrap_or("")
            .split('#')
            .next()
            .unwrap_or("")
            .trim_matches('/')
            .to_string();
        return if cid_path.is_empty() {
            None
        } else {
            Some(format!("ipfs:{}", cid_path))
        };
    }
    if lowered.starts_with("ar://") {
        let tx_path = text[5..]
            .split('?')
            .next()
            .unwrap_or("")
            .split('#')
            .next()
            .unwrap_or("")
            .trim_matches('/')
            .to_string();
        return if tx_path.is_empty() {
            None
        } else {
            Some(format!("ar:{}", tx_path))
        };
    }
    if let Some(captures) = IPFS_HTTP_RE.captures(text) {
        let cid_path = captures
            .get(1)
            .map(|value| value.as_str())
            .unwrap_or("")
            .split('?')
            .next()
            .unwrap_or("")
            .split('#')
            .next()
            .unwrap_or("")
            .trim_end_matches('/')
            .to_string();
        return if cid_path.is_empty() {
            None
        } else {
            Some(format!("ipfs:{}", cid_path))
        };
    }
    if let Some(captures) = ARWEAVE_HTTP_RE.captures(text) {
        let tx_path = captures
            .get(1)
            .map(|value| value.as_str())
            .unwrap_or("")
            .split('?')
            .next()
            .unwrap_or("")
            .split('#')
            .next()
            .unwrap_or("")
            .trim_end_matches('/')
            .to_string();
        return if tx_path.is_empty() {
            None
        } else {
            Some(format!("ar:{}", tx_path))
        };
    }
    Some(lowered.trim_end_matches('/').to_string())
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

pub(crate) fn metadata_document(raw: &str) -> String {
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

pub(crate) fn tokenize(document: &str) -> HashSet<String> {
    TOKEN_RE
        .find_iter(document)
        .map(|m| m.as_str().to_lowercase())
        .filter(|token| token.len() >= 2)
        .collect()
}

pub(crate) fn metadata_keywords_internal(document: &str, limit: usize) -> Vec<String> {
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

pub(crate) fn name_similarity_normalized(left_norm: &str, right_norm: &str) -> f64 {
    if left_norm.is_empty() || right_norm.is_empty() {
        return 0.0;
    }
    if left_norm == right_norm {
        return 100.0;
    }
    let jaro = jaro_winkler(left_norm, right_norm);
    let levenshtein = normalized_levenshtein(left_norm, right_norm);
    ((jaro * 0.65) + (levenshtein * 0.35)) * 100.0
}

pub(crate) fn name_score(left: &str, right: &str) -> f64 {
    let left_norm = normalize_name(left);
    let right_norm = normalize_name(right);
    name_similarity_normalized(&left_norm, &right_norm)
}

pub(crate) fn metadata_score(left: &str, right: &str) -> f64 {
    let left_doc = metadata_document(left);
    let right_doc = metadata_document(right);
    metadata_score_from_documents(&left_doc, &right_doc)
}

pub(crate) fn metadata_score_normalized_documents(left_doc: &str, right_doc: &str) -> f64 {
    if left_doc.is_empty() || right_doc.is_empty() {
        return 0.0;
    }
    let left_tokens = tokenize(left_doc);
    let right_tokens = tokenize(right_doc);
    let union = left_tokens.union(&right_tokens).count();
    let overlap = left_tokens.intersection(&right_tokens).count();
    let jaccard = if union == 0 {
        0.0
    } else {
        overlap as f64 / union as f64
    };
    let similarity = jaro_winkler(left_doc, right_doc);
    (jaccard * 0.45) + (similarity * 0.55)
}

pub(crate) fn metadata_score_from_documents(left: &str, right: &str) -> f64 {
    let left_doc = normalize_text(left);
    let right_doc = normalize_text(right);
    metadata_score_normalized_documents(&left_doc, &right_doc)
}

pub(crate) fn canonical_pair(left: &str, right: &str) -> (String, String) {
    if left <= right {
        (left.to_string(), right.to_string())
    } else {
        (right.to_string(), left.to_string())
    }
}
