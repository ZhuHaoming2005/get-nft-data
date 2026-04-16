use std::collections::HashSet;

use serde_json::Value;

use crate::normalize::{normalize_name, normalize_text};

fn flatten_metadata(value: &Value, parts: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            for (key, item) in map {
                let key = key.to_lowercase();
                if matches!(
                    key.as_str(),
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
            for item in items {
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

fn levenshtein(left: &str, right: &str) -> usize {
    let left: Vec<char> = left.chars().collect();
    let right: Vec<char> = right.chars().collect();

    if left.is_empty() {
        return right.len();
    }
    if right.is_empty() {
        return left.len();
    }

    let mut prev: Vec<usize> = (0..=right.len()).collect();
    let mut curr = vec![0; right.len() + 1];

    for (i, left_ch) in left.iter().enumerate() {
        curr[0] = i + 1;
        for (j, right_ch) in right.iter().enumerate() {
            let cost = usize::from(left_ch != right_ch);
            curr[j + 1] = (prev[j + 1] + 1)
                .min(curr[j] + 1)
                .min(prev[j] + cost);
        }
        prev.clone_from(&curr);
    }

    prev[right.len()]
}

fn normalized_levenshtein(left: &str, right: &str) -> f64 {
    let max_len = left.chars().count().max(right.chars().count());
    if max_len == 0 {
        return 1.0;
    }
    1.0 - (levenshtein(left, right) as f64 / max_len as f64)
}

fn jaro_similarity(left: &str, right: &str) -> f64 {
    let left: Vec<char> = left.chars().collect();
    let right: Vec<char> = right.chars().collect();
    let left_len = left.len();
    let right_len = right.len();

    if left_len == 0 && right_len == 0 {
        return 1.0;
    }
    if left_len == 0 || right_len == 0 {
        return 0.0;
    }

    let match_distance = (left_len.max(right_len) / 2).saturating_sub(1);
    let mut left_matches = vec![false; left_len];
    let mut right_matches = vec![false; right_len];
    let mut matches = 0usize;

    for i in 0..left_len {
        let start = i.saturating_sub(match_distance);
        let end = (i + match_distance + 1).min(right_len);
        for j in start..end {
            if right_matches[j] || left[i] != right[j] {
                continue;
            }
            left_matches[i] = true;
            right_matches[j] = true;
            matches += 1;
            break;
        }
    }

    if matches == 0 {
        return 0.0;
    }

    let mut transpositions = 0usize;
    let mut right_index = 0usize;
    for i in 0..left_len {
        if !left_matches[i] {
            continue;
        }
        while right_index < right_len && !right_matches[right_index] {
            right_index += 1;
        }
        if right_index < right_len && left[i] != right[right_index] {
            transpositions += 1;
        }
        right_index += 1;
    }

    let matches = matches as f64;
    ((matches / left_len as f64)
        + (matches / right_len as f64)
        + ((matches - (transpositions as f64 / 2.0)) / matches))
        / 3.0
}

fn jaro_winkler(left: &str, right: &str) -> f64 {
    let jaro = jaro_similarity(left, right);
    let prefix = left
        .chars()
        .zip(right.chars())
        .take_while(|(l, r)| l == r)
        .take(4)
        .count() as f64;
    jaro + (prefix * 0.1 * (1.0 - jaro))
}

fn tokenize(document: &str) -> HashSet<String> {
    document
        .split(|ch: char| !(ch.is_alphanumeric() || ch == '_'))
        .filter(|token| token.len() >= 2)
        .map(|token| token.to_lowercase())
        .collect()
}

fn metadata_score_from_documents(left: &str, right: &str) -> f64 {
    let left_doc = normalize_text(left);
    let right_doc = normalize_text(right);

    if left_doc.is_empty() || right_doc.is_empty() {
        return 0.0;
    }

    let left_tokens = tokenize(&left_doc);
    let right_tokens = tokenize(&right_doc);
    if !left_tokens.is_empty() && left_tokens == right_tokens {
        return 1.0;
    }
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

pub fn metadata_document_from_json(raw: &str) -> String {
    metadata_document(raw)
}

pub fn score_name_pairs(left: &[String], right: &[String]) -> Vec<f64> {
    left.iter()
        .zip(right.iter())
        .map(|(l, r)| {
            let left_norm = normalize_name(l);
            let right_norm = normalize_name(r);
            if left_norm.is_empty() || right_norm.is_empty() {
                0.0
            } else if left_norm == right_norm {
                100.0
            } else {
                ((jaro_winkler(&left_norm, &right_norm) * 0.65)
                    + (normalized_levenshtein(&left_norm, &right_norm) * 0.35))
                    * 100.0
            }
        })
        .collect()
}

pub fn score_metadata_documents(left: &[String], right: &[String]) -> Vec<f64> {
    left.iter()
        .zip(right.iter())
        .map(|(l, r)| metadata_score_from_documents(l, r))
        .collect()
}
