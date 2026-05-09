use std::collections::{BTreeSet, HashMap, HashSet};

use once_cell::sync::Lazy;
use regex::Regex;
use serde_json::Value;
use strsim::jaro_winkler;
use thiserror::Error;

use crate::normalize::{normalize_name, normalize_text};

static TOKEN_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"[\p{L}\p{N}_]+").unwrap());
pub const MAX_METADATA_BYTES_FOR_DEDUP: usize = 64 * 1024;

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

pub fn metadata_prefilter_document_from_json(raw: &str) -> String {
    if raw.trim().is_empty() {
        return String::new();
    }

    match serde_json::from_str::<Value>(raw) {
        Ok(value) => {
            let mut parts = BTreeSet::new();
            collect_metadata_prefilter_parts(&value, &mut parts);
            parts.into_iter().collect::<Vec<_>>().join(" ")
        }
        Err(_) => normalize_text(raw),
    }
}

pub fn metadata_recall_document(metadata_doc: &str, metadata_json: &str) -> String {
    if !metadata_is_dedup_eligible(metadata_doc, metadata_json) {
        return String::new();
    }
    metadata_prefilter_document_from_json(metadata_json)
}

pub fn metadata_is_dedup_eligible(metadata_doc: &str, metadata_json: &str) -> bool {
    let _ = metadata_doc;
    let metadata_json = metadata_json.trim();
    !metadata_json.is_empty()
        && metadata_json.len() <= MAX_METADATA_BYTES_FOR_DEDUP
        && matches!(metadata_json.chars().next(), Some('{') | Some('['))
}

pub fn metadata_recall_keywords(document: &str, limit: usize) -> Vec<String> {
    let mut counts: HashMap<String, usize> = HashMap::new();
    for token in TOKEN_RE.find_iter(document) {
        let normalized = token.as_str().to_lowercase();
        if normalized.len() < 2 {
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

fn collect_metadata_prefilter_parts(value: &Value, parts: &mut BTreeSet<String>) {
    match value {
        Value::Object(map) => {
            for (key, item) in map {
                let key_norm = normalize_text(key);
                if key_norm.is_empty() {
                    continue;
                }
                if is_structure_wrapper_key(&key_norm) {
                    collect_metadata_prefilter_parts(item, parts);
                } else if key_norm == "trait_type" {
                    push_metadata_prefilter_part(parts, &key_norm);
                    if let Some(text) = item.as_str() {
                        push_metadata_prefilter_part(parts, text);
                    }
                } else if metadata_prefilter_includes_value(&key_norm) {
                    push_metadata_prefilter_part(parts, &key_norm);
                    collect_metadata_prefilter_values(item, parts);
                } else {
                    push_metadata_prefilter_part(parts, &key_norm);
                    collect_metadata_prefilter_parts(item, parts);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_metadata_prefilter_parts(item, parts);
            }
        }
        _ => {}
    }
}

fn collect_metadata_prefilter_values(value: &Value, parts: &mut BTreeSet<String>) {
    match value {
        Value::String(text) => push_metadata_prefilter_part(parts, text),
        Value::Number(number) => push_metadata_prefilter_part(parts, &number.to_string()),
        Value::Bool(value) => push_metadata_prefilter_part(parts, &value.to_string()),
        Value::Array(items) => {
            for item in items {
                collect_metadata_prefilter_values(item, parts);
            }
        }
        Value::Object(map) => {
            for (key, item) in map {
                push_metadata_prefilter_part(parts, key);
                collect_metadata_prefilter_values(item, parts);
            }
        }
        Value::Null => {}
    }
}

fn metadata_prefilter_includes_value(key: &str) -> bool {
    is_description_key(key) || is_platform_key(key)
}

fn push_metadata_prefilter_part(parts: &mut BTreeSet<String>, raw: &str) {
    let text = normalize_text(raw);
    if !text.is_empty() {
        parts.insert(text);
    }
}

fn is_structure_wrapper_key(key: &str) -> bool {
    matches!(key, "metadata" | "rawmetadata" | "raw")
}

fn is_description_key(key: &str) -> bool {
    matches!(
        key,
        "description" | "bio" | "story" | "lore" | "summary" | "about"
    )
}

fn is_platform_key(key: &str) -> bool {
    matches!(
        key,
        "seller_fee_basis_points"
            | "fee_recipient"
            | "royalty"
            | "royalties"
            | "creator"
            | "creators"
            | "compiler"
            | "license"
            | "collection"
            | "marketplace"
            | "contract"
            | "chain"
    )
}

fn tokenize(document: &str) -> Vec<String> {
    TOKEN_RE
        .find_iter(document)
        .map(|m| m.as_str().to_string())
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
    fn prepared_metadata_query_matches_indexed_pair_score() {
        let query = MetadataBm25Document::from_text("gold dragon gold").unwrap();
        let doc = MetadataBm25Document::from_text("rare gold dragon").unwrap();
        let corpus = MetadataBm25Corpus::from_indexed_documents(std::slice::from_ref(&doc));

        let prepared_query = MetadataBm25Query::new(&query, &corpus);
        let prepared_score = score_metadata_indexed_pair_with_query(&prepared_query, &doc);
        let indexed_score = score_metadata_indexed_pair_with_corpus(&query, &doc, &corpus);

        assert!((prepared_score - indexed_score).abs() < 1e-9);
    }

    #[test]
    fn prepared_metadata_query_detects_term_overlap() {
        let query = MetadataBm25Document::from_text("gold dragon gold").unwrap();
        let overlap_doc = MetadataBm25Document::from_text("rare gold").unwrap();
        let miss_doc = MetadataBm25Document::from_text("silver cat").unwrap();
        let corpus =
            MetadataBm25Corpus::from_indexed_documents(&[overlap_doc.clone(), miss_doc.clone()]);

        let prepared_query = MetadataBm25Query::new(&query, &corpus);

        assert!(prepared_query.has_term_overlap(&overlap_doc));
        assert!(!prepared_query.has_term_overlap(&miss_doc));
    }

    #[test]
    fn single_document_metadata_pair_score_matches_corpus_path() {
        let query = MetadataBm25Document::from_text("gold dragon").unwrap();
        let doc = MetadataBm25Document::from_text("rare gold dragon").unwrap();
        let corpus = MetadataBm25Corpus::from_indexed_documents(std::slice::from_ref(&doc));

        let single_doc_score = score_metadata_single_document_pair(&query, &doc);
        let corpus_score = score_metadata_indexed_pair_with_corpus(&query, &doc, &corpus);

        assert!((single_doc_score - corpus_score).abs() < 1e-9);
    }

    #[test]
    fn prepared_single_document_query_reuses_terms_without_changing_score() {
        let query = MetadataBm25Document::from_text("gold dragon gold").unwrap();
        let doc = MetadataBm25Document::from_text("rare gold dragon").unwrap();

        let prepared_query = MetadataBm25SingleDocumentQuery::new(query.clone());
        let prepared_score = prepared_query.score(&doc);
        let pair_score = score_metadata_single_document_pair(&query, &doc);

        assert!((prepared_score - pair_score).abs() < 1e-9);
    }

    #[test]
    fn prepared_single_document_query_detects_term_overlap() {
        let query = MetadataBm25Document::from_text("gold dragon").unwrap();
        let overlap_doc = MetadataBm25Document::from_text("dragon scale").unwrap();
        let miss_doc = MetadataBm25Document::from_text("silver cat").unwrap();

        let prepared_query = MetadataBm25SingleDocumentQuery::new(query);

        assert!(prepared_query.has_term_overlap(&overlap_doc));
        assert!(!prepared_query.has_term_overlap(&miss_doc));
    }

    #[test]
    fn metadata_bm25_uses_common_okapi_defaults() {
        assert_eq!(METADATA_BM25_K1, 1.2);
        assert_eq!(METADATA_BM25_B, 0.75);
    }

    #[test]
    fn metadata_prefilter_document_keeps_insensitive_values_but_only_sensitive_keys() {
        let json = r#"{"name":"Seed #1","description":"Shared Story","attributes":[{"trait_type":"Background","value":"Red"}],"image":"ipfs://seed/1.png"}"#;

        let text = metadata_prefilter_document_from_json(json);

        assert!(text.contains("description"));
        assert!(text.contains("shared story"));
        assert!(text.contains("background"));
        assert!(text.contains("name"));
        assert!(text.contains("image"));
        let tokens = text.split_whitespace().collect::<Vec<_>>();
        assert!(!tokens.contains(&"seed"));
        assert!(!tokens.contains(&"red"));
        assert!(!tokens.contains(&"ipfs"));
    }

    #[test]
    fn metadata_recall_document_rejects_non_json_and_overlong_raw_metadata() {
        assert_eq!(
            metadata_recall_document("gold dragon", r#"{"description":"Gold Dragon"}"#),
            "description gold dragon"
        );
        assert_eq!(
            metadata_recall_document("gold dragon", "not json metadata"),
            ""
        );
        assert_eq!(metadata_recall_document("gold dragon", ""), "");

        let overlong_json = format!(
            r#"{{"description":"{}"}}"#,
            "x".repeat(MAX_METADATA_BYTES_FOR_DEDUP)
        );
        assert_eq!(metadata_recall_document("gold dragon", &overlong_json), "");
    }
}

#[derive(Debug, Clone)]
pub struct MetadataBm25Corpus {
    total_docs: usize,
    avg_doc_len: f64,
    doc_freqs: HashMap<String, usize>,
}

#[derive(Debug)]
pub(crate) struct MetadataBm25Query<'a> {
    terms: Vec<(String, usize)>,
    denominator: f64,
    corpus: &'a MetadataBm25Corpus,
}

#[derive(Debug, Clone)]
pub(crate) struct MetadataBm25SingleDocumentQuery {
    document: MetadataBm25Document,
    terms: Vec<(String, usize)>,
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

impl<'a> MetadataBm25Query<'a> {
    pub(crate) fn new(query: &MetadataBm25Document, corpus: &'a MetadataBm25Corpus) -> Self {
        let terms = query_terms_from_tokens(query.tokens());
        let self_score = bm25_score_terms(&terms, query, corpus);
        let denominator = if self_score > 0.0 { self_score } else { 1.0 };
        Self {
            terms,
            denominator,
            corpus,
        }
    }

    pub(crate) fn has_term_overlap(&self, document: &MetadataBm25Document) -> bool {
        query_terms_overlap_document(&self.terms, document)
    }
}

impl MetadataBm25SingleDocumentQuery {
    pub(crate) fn new(document: MetadataBm25Document) -> Self {
        let terms = query_terms_from_tokens(document.tokens());
        Self { document, terms }
    }

    pub(crate) fn document(&self) -> &MetadataBm25Document {
        &self.document
    }

    pub(crate) fn score(&self, right: &MetadataBm25Document) -> f64 {
        let self_score =
            bm25_score_terms_with_single_document_corpus(&self.terms, &self.document, right);
        let denominator = if self_score > 0.0 { self_score } else { 1.0 };
        (bm25_score_terms_with_single_document_corpus(&self.terms, right, right) / denominator)
            .clamp(0.0, 1.0)
    }

    pub(crate) fn has_term_overlap(&self, document: &MetadataBm25Document) -> bool {
        query_terms_overlap_document(&self.terms, document)
    }
}

fn query_terms_from_tokens(query_tokens: &[String]) -> Vec<(String, usize)> {
    let mut query_terms = HashMap::<String, usize>::new();
    for token in query_tokens {
        *query_terms.entry(token.clone()).or_insert(0) += 1;
    }
    let mut query_terms = query_terms.into_iter().collect::<Vec<_>>();
    query_terms.sort_by(|left, right| left.0.cmp(&right.0));
    query_terms
}

fn query_terms_overlap_document(
    query_terms: &[(String, usize)],
    document: &MetadataBm25Document,
) -> bool {
    query_terms
        .iter()
        .any(|(token, _)| document.term_frequency(token) > 0)
}

fn bm25_score_terms(
    query_terms: &[(String, usize)],
    doc: &MetadataBm25Document,
    corpus: &MetadataBm25Corpus,
) -> f64 {
    if query_terms.is_empty()
        || doc.len() == 0
        || corpus.total_docs == 0
        || corpus.avg_doc_len <= 0.0
    {
        return 0.0;
    }

    let doc_len = doc.len() as f64;
    let norm =
        METADATA_BM25_K1 * (1.0 - METADATA_BM25_B + METADATA_BM25_B * doc_len / corpus.avg_doc_len);

    query_terms
        .iter()
        .map(|(token, query_tf)| {
            let tf = doc.term_frequency(token) as f64;
            if tf == 0.0 {
                return 0.0;
            }
            let df = *corpus.doc_freqs.get(token).unwrap_or(&0) as f64;
            let idf = ((corpus.total_docs as f64 - df + 0.5) / (df + 0.5) + 1.0).ln();
            *query_tf as f64 * idf * (tf * (METADATA_BM25_K1 + 1.0)) / (tf + norm)
        })
        .sum()
}

fn bm25_score_terms_with_single_document_corpus(
    query_terms: &[(String, usize)],
    doc: &MetadataBm25Document,
    corpus_doc: &MetadataBm25Document,
) -> f64 {
    if query_terms.is_empty() || doc.len() == 0 || corpus_doc.len() == 0 {
        return 0.0;
    }

    let doc_len = doc.len() as f64;
    let avg_doc_len = corpus_doc.len() as f64;
    let norm = METADATA_BM25_K1 * (1.0 - METADATA_BM25_B + METADATA_BM25_B * doc_len / avg_doc_len);

    query_terms
        .iter()
        .map(|(token, query_tf)| {
            let tf = doc.term_frequency(token) as f64;
            if tf == 0.0 {
                return 0.0;
            }
            let df = if corpus_doc.term_frequency(token) > 0 {
                1.0
            } else {
                0.0
            };
            let idf = ((1.0_f64 - df + 0.5) / (df + 0.5) + 1.0).ln();
            *query_tf as f64 * idf * (tf * (METADATA_BM25_K1 + 1.0)) / (tf + norm)
        })
        .sum()
}

pub(crate) fn score_metadata_indexed_pair_with_query(
    query: &MetadataBm25Query<'_>,
    right: &MetadataBm25Document,
) -> f64 {
    (bm25_score_terms(&query.terms, right, query.corpus) / query.denominator).clamp(0.0, 1.0)
}

#[cfg(test)]
pub(crate) fn score_metadata_single_document_pair(
    left: &MetadataBm25Document,
    right: &MetadataBm25Document,
) -> f64 {
    MetadataBm25SingleDocumentQuery::new(left.clone()).score(right)
}

pub fn score_metadata_indexed_pair_with_corpus(
    left: &MetadataBm25Document,
    right: &MetadataBm25Document,
    corpus: &MetadataBm25Corpus,
) -> f64 {
    let query = MetadataBm25Query::new(left, corpus);
    score_metadata_indexed_pair_with_query(&query, right)
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
