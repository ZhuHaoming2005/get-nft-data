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

pub const METADATA_BM25_K1: f64 = 1.2;
pub const METADATA_BM25_B: f64 = 0.75;

#[derive(Debug, Clone)]
pub struct MetadataBm25Document {
    tokens: Vec<String>,
    term_freqs: HashMap<String, usize>,
}

impl MetadataBm25Document {
    pub fn from_text(document: &str) -> Option<Self> {
        Self::from_tokens(metadata_bm25_tokens(document))
    }

    pub(crate) fn from_tokens(tokens: Vec<String>) -> Option<Self> {
        if tokens.is_empty() {
            return None;
        }

        let mut term_freqs = HashMap::new();
        for token in &tokens {
            *term_freqs.entry(token.clone()).or_insert(0usize) += 1;
        }

        Some(Self { tokens, term_freqs })
    }

    pub fn tokens(&self) -> &[String] {
        &self.tokens
    }

    fn len(&self) -> usize {
        self.tokens.len()
    }

    pub fn term_frequency(&self, token: &str) -> usize {
        *self.term_freqs.get(token).unwrap_or(&0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn indexed_metadata_documents_reuse_cached_tokens_and_term_frequencies() {
        let query = MetadataBm25Document::from_text("gold dragon gold").unwrap();
        let doc = MetadataBm25Document::from_text("rare gold dragon").unwrap();
        let corpus = MetadataBm25Corpus::from_indexed_documents(std::slice::from_ref(&doc));

        assert_eq!(query.term_frequency("gold"), 2);
        assert_eq!(doc.tokens(), &["rare", "gold", "dragon"]);

        let indexed_score = score_metadata_indexed_pair_with_corpus(&query, &doc, &corpus);
        let string_score = score_metadata_document_pair_with_corpus(
            "gold dragon gold",
            "rare gold dragon",
            &corpus,
        );
        assert!((indexed_score - string_score).abs() < 1e-9);
    }

    #[test]
    fn metadata_bm25_uses_common_okapi_defaults() {
        assert_eq!(METADATA_BM25_K1, 1.2);
        assert_eq!(METADATA_BM25_B, 0.75);
    }
}

#[derive(Debug, Clone)]
pub struct MetadataBm25Corpus {
    total_docs: usize,
    avg_doc_len: f64,
    doc_freqs: HashMap<String, usize>,
}

#[derive(Debug, Default)]
pub(crate) struct MetadataBm25CorpusBuilder {
    total_docs: usize,
    total_terms: usize,
    doc_freqs: HashMap<String, usize>,
}

impl MetadataBm25CorpusBuilder {
    pub(crate) fn add_tokens(&mut self, tokens: &[String]) {
        if tokens.is_empty() {
            return;
        }
        self.total_docs += 1;
        self.total_terms += tokens.len();
        for token in tokens.iter().collect::<HashSet<_>>() {
            *self.doc_freqs.entry((*token).clone()).or_insert(0) += 1;
        }
    }

    pub(crate) fn finish(self) -> MetadataBm25Corpus {
        let avg_doc_len = if self.total_docs == 0 {
            0.0
        } else {
            self.total_terms as f64 / self.total_docs as f64
        };

        MetadataBm25Corpus {
            total_docs: self.total_docs,
            avg_doc_len,
            doc_freqs: self.doc_freqs,
        }
    }
}

impl MetadataBm25Corpus {
    pub fn from_documents(documents: &[String]) -> Self {
        let indexed_documents: Vec<_> = documents
            .iter()
            .filter_map(|document| MetadataBm25Document::from_text(document))
            .collect();
        Self::from_indexed_documents(&indexed_documents)
    }

    pub fn from_indexed_documents(documents: &[MetadataBm25Document]) -> Self {
        let mut builder = MetadataBm25CorpusBuilder::default();
        for document in documents {
            builder.add_tokens(document.tokens());
        }
        builder.finish()
    }

    #[cfg(test)]
    pub(crate) fn total_docs(&self) -> usize {
        self.total_docs
    }
}

fn bm25_score_tokens(
    query_tokens: &[String],
    doc: &MetadataBm25Document,
    corpus: &MetadataBm25Corpus,
) -> f64 {
    if query_tokens.is_empty()
        || doc.len() == 0
        || corpus.total_docs == 0
        || corpus.avg_doc_len <= 0.0
    {
        return 0.0;
    }

    let doc_len = doc.len() as f64;
    let norm =
        METADATA_BM25_K1 * (1.0 - METADATA_BM25_B + METADATA_BM25_B * doc_len / corpus.avg_doc_len);

    query_tokens
        .iter()
        .map(|token| {
            let tf = doc.term_frequency(token) as f64;
            if tf == 0.0 {
                return 0.0;
            }
            let df = *corpus.doc_freqs.get(token).unwrap_or(&0) as f64;
            let idf = ((corpus.total_docs as f64 - df + 0.5) / (df + 0.5) + 1.0).ln();
            idf * (tf * (METADATA_BM25_K1 + 1.0)) / (tf + norm)
        })
        .sum()
}

pub fn score_metadata_indexed_pair_with_corpus(
    left: &MetadataBm25Document,
    right: &MetadataBm25Document,
    corpus: &MetadataBm25Corpus,
) -> f64 {
    let self_score = bm25_score_tokens(left.tokens(), left, corpus);
    let denominator = if self_score > 0.0 { self_score } else { 1.0 };
    (bm25_score_tokens(left.tokens(), right, corpus) / denominator).clamp(0.0, 1.0)
}

fn metadata_bm25_score_from_documents(left: &str, right: &str, corpus: &MetadataBm25Corpus) -> f64 {
    let Some(left_doc) = MetadataBm25Document::from_text(left) else {
        return 0.0;
    };
    let Some(right_doc) = MetadataBm25Document::from_text(right) else {
        return 0.0;
    };
    score_metadata_indexed_pair_with_corpus(&left_doc, &right_doc, corpus)
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
    let mut right_doc_indexes = Vec::with_capacity(right.len());
    let mut corpus_docs = Vec::new();
    for document in right {
        if let Some(document) = MetadataBm25Document::from_text(document) {
            right_doc_indexes.push(Some(corpus_docs.len()));
            corpus_docs.push(document);
        } else {
            right_doc_indexes.push(None);
        }
    }
    let corpus = MetadataBm25Corpus::from_indexed_documents(&corpus_docs);
    Ok(left
        .iter()
        .zip(right_doc_indexes.iter())
        .map(|(left_doc, right_doc_index)| {
            let Some(left_doc) = MetadataBm25Document::from_text(left_doc) else {
                return 0.0;
            };
            let Some(right_doc_index) = right_doc_index else {
                return 0.0;
            };
            let right_doc = &corpus_docs[*right_doc_index];
            score_metadata_indexed_pair_with_corpus(&left_doc, right_doc, &corpus)
        })
        .collect())
}
