use std::collections::HashSet;

use once_cell::sync::Lazy;
use regex::Regex;
use serde_json::Value;
use strsim::{jaro_winkler, normalized_levenshtein};
use thiserror::Error;

use crate::normalize::{normalize_name, normalize_text};

static TOKEN_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"[\p{L}\p{N}_]+").unwrap());

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ScoringError {
    #[error("left and right sequences must have identical lengths")]
    MismatchedInputLengths,
}

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

fn tokenize(document: &str) -> HashSet<String> {
    TOKEN_RE
        .find_iter(document)
        .map(|m| m.as_str().to_lowercase())
        .filter(|token| token.len() >= 2)
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

pub fn score_name_pairs(left: &[String], right: &[String]) -> Result<Vec<f64>, ScoringError> {
    if left.len() != right.len() {
        return Err(ScoringError::MismatchedInputLengths);
    }
    Ok(left
        .iter()
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
        .collect())
}

pub fn score_metadata_documents(
    left: &[String],
    right: &[String],
) -> Result<Vec<f64>, ScoringError> {
    if left.len() != right.len() {
        return Err(ScoringError::MismatchedInputLengths);
    }
    Ok(left
        .iter()
        .zip(right.iter())
        .map(|(l, r)| metadata_score_from_documents(l, r))
        .collect())
}
