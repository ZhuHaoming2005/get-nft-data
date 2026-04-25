use std::collections::{HashMap, HashSet};

use once_cell::sync::Lazy;
use regex::Regex;
use serde_json::Value;
use strsim::jaro_winkler;
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

fn tokenize(document: &str) -> Vec<String> {
    TOKEN_RE
        .find_iter(document)
        .map(|m| m.as_str().to_lowercase())
        .filter(|token| token.len() >= 2)
        .collect()
}

pub fn metadata_bm25_tokens(document: &str) -> Vec<String> {
    tokenize(&normalize_text(document))
}

#[derive(Debug, Clone)]
pub struct MetadataBm25Corpus {
    total_docs: usize,
    avg_doc_len: f64,
    doc_freqs: HashMap<String, usize>,
}

impl MetadataBm25Corpus {
    pub fn from_documents(documents: &[String]) -> Self {
        let mut total_docs = 0usize;
        let mut total_terms = 0usize;
        let mut doc_freqs = HashMap::new();

        for document in documents {
            let tokens = metadata_bm25_tokens(document);
            if tokens.is_empty() {
                continue;
            }
            total_docs += 1;
            total_terms += tokens.len();
            for token in tokens.into_iter().collect::<HashSet<_>>() {
                *doc_freqs.entry(token).or_insert(0) += 1;
            }
        }

        let avg_doc_len = if total_docs == 0 {
            0.0
        } else {
            total_terms as f64 / total_docs as f64
        };

        Self {
            total_docs,
            avg_doc_len,
            doc_freqs,
        }
    }
}

fn bm25_score_tokens(
    query_tokens: &[String],
    doc_tokens: &[String],
    corpus: &MetadataBm25Corpus,
) -> f64 {
    if query_tokens.is_empty()
        || doc_tokens.is_empty()
        || corpus.total_docs == 0
        || corpus.avg_doc_len <= 0.0
    {
        return 0.0;
    }

    let mut term_freqs = HashMap::new();
    for token in doc_tokens {
        *term_freqs.entry(token).or_insert(0usize) += 1;
    }

    let k1 = 1.2;
    let b = 0.75;
    let doc_len = doc_tokens.len() as f64;
    let norm = k1 * (1.0 - b + b * doc_len / corpus.avg_doc_len);

    query_tokens
        .iter()
        .map(|token| {
            let tf = *term_freqs.get(token).unwrap_or(&0) as f64;
            if tf == 0.0 {
                return 0.0;
            }
            let df = *corpus.doc_freqs.get(token).unwrap_or(&0) as f64;
            let idf = ((corpus.total_docs as f64 - df + 0.5) / (df + 0.5) + 1.0).ln();
            idf * (tf * (k1 + 1.0)) / (tf + norm)
        })
        .sum()
}

fn metadata_bm25_score_from_documents(left: &str, right: &str, corpus: &MetadataBm25Corpus) -> f64 {
    let left_doc = normalize_text(left);
    let right_doc = normalize_text(right);

    if left_doc.is_empty() || right_doc.is_empty() {
        return 0.0;
    }

    let query_tokens = metadata_bm25_tokens(&left_doc);
    let doc_tokens = metadata_bm25_tokens(&right_doc);
    let self_score = bm25_score_tokens(&query_tokens, &query_tokens, corpus);
    let denominator = if self_score > 0.0 { self_score } else { 1.0 };
    (bm25_score_tokens(&query_tokens, &doc_tokens, corpus) / denominator).clamp(0.0, 1.0)
}

pub fn metadata_document_from_json(raw: &str) -> String {
    metadata_document(raw)
}

pub fn score_name_pair(left: &str, right: &str) -> f64 {
    let left_norm = normalize_name(left);
    let right_norm = normalize_name(right);
    if left_norm.is_empty() || right_norm.is_empty() {
        0.0
    } else if left_norm == right_norm {
        100.0
    } else {
        jaro_winkler(&left_norm, &right_norm) * 100.0
    }
}

pub fn score_name_pairs(left: &[String], right: &[String]) -> Result<Vec<f64>, ScoringError> {
    if left.len() != right.len() {
        return Err(ScoringError::MismatchedInputLengths);
    }
    Ok(left
        .iter()
        .zip(right.iter())
        .map(|(l, r)| score_name_pair(l, r))
        .collect())
}

pub fn score_metadata_document_pair(left: &str, right: &str) -> f64 {
    let corpus = MetadataBm25Corpus::from_documents(&[right.to_string()]);
    metadata_bm25_score_from_documents(left, right, &corpus)
}

pub fn score_metadata_document_pair_with_corpus(
    left: &str,
    right: &str,
    corpus: &MetadataBm25Corpus,
) -> f64 {
    metadata_bm25_score_from_documents(left, right, corpus)
}

pub fn score_metadata_documents(
    left: &[String],
    right: &[String],
) -> Result<Vec<f64>, ScoringError> {
    if left.len() != right.len() {
        return Err(ScoringError::MismatchedInputLengths);
    }
    let corpus = MetadataBm25Corpus::from_documents(right);
    Ok(left
        .iter()
        .zip(right.iter())
        .map(|(l, r)| score_metadata_document_pair_with_corpus(l, r, &corpus))
        .collect())
}
