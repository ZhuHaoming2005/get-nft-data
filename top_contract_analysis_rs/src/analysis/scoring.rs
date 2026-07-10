use std::collections::{BTreeSet, HashMap};
#[cfg(test)]
use std::collections::HashSet;

use once_cell::sync::Lazy;
use rapidfuzz::distance::jaro_winkler;
use regex::Regex;
use serde_json::Value;
use thiserror::Error;

use crate::normalize::{normalize_name, normalize_text};

// Keep in sync with name_uri_analysis_rs metadata TOKEN_RE / metadata_bm25_tokens
static TOKEN_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"[\p{L}\p{N}_]+").unwrap());
pub const MAX_METADATA_BYTES_FOR_DEDUP: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ScoringError {
    #[error("left and right sequences must have identical lengths")]
    MismatchedInputLengths,
}

#[derive(Debug, Default)]
pub(crate) struct MetadataDocuments {
    pub(crate) prefilter: String,
    pub(crate) content: String,
}

// Keep in sync with name_uri_analysis_rs::analysis::name_scoring::PreparedNameQuery
// (rapidfuzz Jaro–Winkler BatchComparator + score_cutoff percent API).
pub(crate) struct PreparedNameQuery {
    scorer: jaro_winkler::BatchComparator<char>,
}

impl PreparedNameQuery {
    pub(crate) fn new(name: &str) -> Self {
        Self {
            scorer: jaro_winkler::BatchComparator::new(name.chars()),
        }
    }

    pub(crate) fn score_percent(&self, right: &str, threshold: f64) -> Option<f64> {
        if threshold.is_nan() || threshold > 100.0 {
            return None;
        }
        let args = jaro_winkler::Args::default().score_cutoff((threshold / 100.0).clamp(0.0, 1.0));
        self.scorer
            .normalized_similarity_with_args(right.chars(), &args)
            .map(|score| score * 100.0)
    }
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
        Value::String(text) if !text.trim().is_empty() => parts.push(text.trim().to_string()),
        _ => {}
    }
}

pub(crate) fn metadata_documents_from_json(raw: &str) -> MetadataDocuments {
    if raw.trim().is_empty() {
        return MetadataDocuments::default();
    }

    match serde_json::from_str::<Value>(raw) {
        Ok(value) => {
            let mut prefilter_parts = BTreeSet::new();
            collect_metadata_prefilter_parts(&value, &mut prefilter_parts);
            let prefilter = prefilter_parts.into_iter().collect::<Vec<_>>().join(" ");
            let content = if metadata_is_dedup_eligible(raw) {
                let mut content_parts = Vec::new();
                flatten_metadata(&value, &mut content_parts);
                normalize_text(&content_parts.join(" "))
            } else {
                String::new()
            };
            MetadataDocuments { prefilter, content }
        }
        Err(_) => {
            let normalized = normalize_text(raw);
            MetadataDocuments {
                prefilter: normalized.clone(),
                content: if metadata_is_dedup_eligible(raw) {
                    normalized
                } else {
                    String::new()
                },
            }
        }
    }
}

pub fn metadata_prefilter_document_from_json(raw: &str) -> String {
    metadata_documents_from_json(raw).prefilter
}

pub fn metadata_recall_document(metadata_json: &str) -> String {
    if !metadata_is_dedup_eligible(metadata_json) {
        return String::new();
    }
    metadata_prefilter_document_from_json(metadata_json)
}

pub fn metadata_is_dedup_eligible(metadata_json: &str) -> bool {
    // Keep in sync with name_uri_analysis_rs metadata_is_dedup_eligible
    // and sql_metadata_json_eligible_predicate / metadata_json_eligible_predicate:
    // trim, non-empty, len<=64KiB, starts with { or [
    let metadata_json = metadata_json.trim();
    !metadata_json.is_empty()
        && metadata_json.len() <= MAX_METADATA_BYTES_FOR_DEDUP
        && matches!(metadata_json.chars().next(), Some('{') | Some('['))
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

pub(crate) fn metadata_bm25_has_terms(document: &str) -> bool {
    // Normalize like `metadata_bm25_tokens`/`MetadataBm25Document::from_text` so
    // `has_terms(x) == from_text(x).is_some()` for *any* input, not only
    // pre-normalized text. NFKC can change byte lengths (e.g. "²" -> "2"), which
    // would otherwise make a length>=2 check disagree with `from_text`.
    let normalized = normalize_text(document);
    TOKEN_RE
        .find_iter(&normalized)
        .any(|token| token.as_str().len() >= 2)
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
    fn metadata_is_dedup_eligible_accepts_leading_whitespace_json() {
        assert!(metadata_is_dedup_eligible("  {\"a\":1}"));
        assert!(metadata_is_dedup_eligible("\n[1]"));
        assert!(!metadata_is_dedup_eligible("  x{}"));
    }

    #[test]
    fn normalized_name_pair_scoring_assumes_inputs_are_already_normalized() {
        assert_eq!(score_normalized_name_pair("azuki", "azuki"), 100.0);
        assert_eq!(score_name_pair("Azuki #123", "azuki"), 100.0);
        assert!(score_normalized_name_pair("Azuki #123", "azuki") < 100.0);
    }

    #[test]
    fn prepared_name_query_preserves_unicode_scores_and_cutoff() {
        let query = PreparedNameQuery::new("金色 dragon");

        assert_eq!(query.score_percent("金色 dragon", 95.0), Some(100.0));
        assert_eq!(query.score_percent("silver cat", 95.0), None);
        assert_eq!(query.score_percent("金色 dragon", 101.0), None);

        let expected = score_normalized_name_pair("金色 dragon", "金色 dragons");
        let actual = query
            .score_percent("金色 dragons", 0.0)
            .expect("zero cutoff must return a score");
        assert!((actual - expected).abs() < 1e-9);
    }

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
    fn compact_metadata_bm25_matches_string_reference_with_unknown_query_terms() {
        let query = MetadataBm25Document::from_text("gold dragon gold unknown_seed_term").unwrap();
        let documents = vec![
            MetadataBm25Document::from_text("rare gold dragon").unwrap(),
            MetadataBm25Document::from_text("silver cat").unwrap(),
        ];
        let string_corpus = MetadataBm25Corpus::from_indexed_documents(&documents);
        let (compact_corpus, compact_documents) =
            CompactMetadataBm25Corpus::from_indexed_documents(&documents);
        let compact_query = CompactMetadataBm25Query::new(&query, &compact_corpus);

        for (string_document, compact_document) in documents.iter().zip(compact_documents.iter()) {
            let expected =
                score_metadata_indexed_pair_with_corpus(&query, string_document, &string_corpus);
            let actual =
                score_compact_metadata_indexed_pair_with_query(&compact_query, compact_document);
            assert!((actual - expected).abs() < 1e-9);
        }
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
            metadata_recall_document(r#"{"description":"Gold Dragon"}"#),
            "description gold dragon"
        );
        assert_eq!(metadata_recall_document("not json metadata"), "");
        assert_eq!(metadata_recall_document(""), "");

        let overlong_json = format!(
            r#"{{"description":"{}"}}"#,
            "x".repeat(MAX_METADATA_BYTES_FOR_DEDUP)
        );
        assert_eq!(metadata_recall_document(&overlong_json), "");
    }

    #[test]
    fn metadata_bm25_has_terms_matches_from_text_eligibility() {
        // has_terms must agree with MetadataBm25Document::from_text (which
        // tokenizes normalized text) for arbitrary input, not only pre-normalized
        // text. NFKC can change byte lengths (e.g. "²" -> "2"), so has_terms must
        // normalize exactly like from_text does.
        for raw in [
            "",
            "  ",
            "a b c",
            "x",
            "gold dragon",
            "GOLD  Dragon!!",
            "{}",
            "²",
            "ﬃ",
            "金色",
        ] {
            let has_terms = metadata_bm25_has_terms(raw);
            let from_text_some = MetadataBm25Document::from_text(raw).is_some();
            assert_eq!(has_terms, from_text_some, "raw={raw:?}");
        }
    }

    #[test]
    fn metadata_documents_parse_once_and_preserve_both_semantics() {
        let raw = r#"{"name":"Seed #1","description":"Gold Dragon","attributes":[{"trait_type":"Background","value":"Red"}],"image":"ipfs://seed/1.png"}"#;

        let documents = metadata_documents_from_json(raw);

        assert_eq!(
            documents.prefilter,
            metadata_prefilter_document_from_json(raw)
        );
        assert_eq!(documents.content, metadata_document_from_json(raw));
        assert!(documents.prefilter.contains("description"));
        assert!(documents.content.contains("gold dragon"));
    }
}

// String-keyed BM25 corpus/query path is a test-only oracle for Compact*.
#[cfg(test)]
#[derive(Debug, Clone)]
pub struct MetadataBm25Corpus {
    total_docs: usize,
    avg_doc_len: f64,
    doc_freqs: HashMap<String, usize>,
}

#[derive(Debug, Clone)]
pub(crate) struct CompactMetadataBm25Document {
    len: usize,
    terms: Vec<(u32, usize)>,
}

#[derive(Debug)]
pub(crate) struct CompactMetadataBm25Corpus {
    token_ids: HashMap<String, u32>,
    total_docs: usize,
    avg_doc_len: f64,
    doc_freqs: Vec<usize>,
}

#[derive(Debug, Default)]
pub(crate) struct CompactMetadataBm25CorpusBuilder {
    token_ids: HashMap<String, u32>,
    total_docs: usize,
    total_terms: usize,
    doc_freqs: Vec<usize>,
}

#[derive(Debug)]
pub(crate) struct CompactMetadataBm25Query<'a> {
    terms: Vec<(Option<u32>, usize)>,
    denominator: f64,
    corpus: &'a CompactMetadataBm25Corpus,
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct MetadataBm25Query<'a> {
    terms: Vec<(String, usize)>,
    denominator: f64,
    corpus: &'a MetadataBm25Corpus,
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub(crate) struct MetadataBm25SingleDocumentQuery {
    document: MetadataBm25Document,
    terms: Vec<(String, usize)>,
}

#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct MetadataBm25CorpusBuilder {
    total_docs: usize,
    total_terms: usize,
    doc_freqs: HashMap<String, usize>,
}

#[cfg(test)]
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

impl CompactMetadataBm25CorpusBuilder {
    pub(crate) fn add_tokens(&mut self, tokens: &[String]) {
        if tokens.is_empty() {
            return;
        }
        self.total_docs += 1;
        self.total_terms += tokens.len();
        let mut unique_ids = Vec::with_capacity(tokens.len());
        for token in tokens {
            let token_id = match self.token_ids.get(token).copied() {
                Some(token_id) => token_id,
                None => {
                    let token_id = u32::try_from(self.token_ids.len())
                        .expect("metadata token dictionary exceeds u32 indexes");
                    self.token_ids.insert(token.clone(), token_id);
                    self.doc_freqs.push(0);
                    token_id
                }
            };
            unique_ids.push(token_id);
        }
        unique_ids.sort_unstable();
        unique_ids.dedup();
        for token_id in unique_ids {
            self.doc_freqs[token_id as usize] += 1;
        }
    }

    pub(crate) fn finish(self) -> CompactMetadataBm25Corpus {
        let avg_doc_len = if self.total_docs == 0 {
            0.0
        } else {
            self.total_terms as f64 / self.total_docs as f64
        };
        CompactMetadataBm25Corpus {
            token_ids: self.token_ids,
            total_docs: self.total_docs,
            avg_doc_len,
            doc_freqs: self.doc_freqs,
        }
    }
}

impl CompactMetadataBm25Corpus {
    pub(crate) fn from_indexed_documents(
        documents: &[MetadataBm25Document],
    ) -> (Self, Vec<CompactMetadataBm25Document>) {
        let mut builder = CompactMetadataBm25CorpusBuilder::default();
        for document in documents {
            builder.add_tokens(document.tokens());
        }
        let corpus = builder.finish();
        let compact_documents = documents
            .iter()
            .map(|document| corpus.compact_document(document))
            .collect();
        (corpus, compact_documents)
    }

    pub(crate) fn compact_document(
        &self,
        document: &MetadataBm25Document,
    ) -> CompactMetadataBm25Document {
        let mut terms = document
            .term_freqs
            .iter()
            .filter_map(|(token, term_frequency)| {
                self.token_ids
                    .get(token)
                    .copied()
                    .map(|token_id| (token_id, *term_frequency))
            })
            .collect::<Vec<_>>();
        terms.sort_unstable_by_key(|(token_id, _)| *token_id);
        CompactMetadataBm25Document {
            len: document.len(),
            terms,
        }
    }

    pub(crate) fn total_docs(&self) -> usize {
        self.total_docs
    }

    pub(crate) fn token_doc_freq(&self, token: &str) -> Option<usize> {
        self.token_ids
            .get(token)
            .map(|token_id| self.doc_freqs[*token_id as usize])
    }

    pub(crate) fn contains_token(&self, token: &str) -> bool {
        self.token_ids.contains_key(token)
    }
}

impl<'a> CompactMetadataBm25Query<'a> {
    pub(crate) fn new(query: &MetadataBm25Document, corpus: &'a CompactMetadataBm25Corpus) -> Self {
        let terms = query_terms_from_tokens(query.tokens())
            .into_iter()
            .map(|(token, term_frequency)| (corpus.token_ids.get(&token).copied(), term_frequency))
            .collect::<Vec<_>>();
        let self_score = compact_bm25_query_self_score(&terms, query.len(), corpus);
        let denominator = if self_score > 0.0 { self_score } else { 1.0 };
        Self {
            terms,
            denominator,
            corpus,
        }
    }

    pub(crate) fn has_term_overlap(&self, document: &CompactMetadataBm25Document) -> bool {
        self.terms.iter().any(|(token_id, _)| {
            token_id.is_some_and(|token_id| compact_term_frequency(document, token_id) > 0)
        })
    }
}

#[cfg(test)]
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
}

fn compact_term_frequency(document: &CompactMetadataBm25Document, token_id: u32) -> usize {
    document
        .terms
        .binary_search_by_key(&token_id, |(document_token_id, _)| *document_token_id)
        .ok()
        .map_or(0, |index| document.terms[index].1)
}

fn compact_bm25_query_self_score(
    query_terms: &[(Option<u32>, usize)],
    query_len: usize,
    corpus: &CompactMetadataBm25Corpus,
) -> f64 {
    if query_terms.is_empty()
        || query_len == 0
        || corpus.total_docs == 0
        || corpus.avg_doc_len <= 0.0
    {
        return 0.0;
    }
    let doc_len = query_len as f64;
    let norm =
        METADATA_BM25_K1 * (1.0 - METADATA_BM25_B + METADATA_BM25_B * doc_len / corpus.avg_doc_len);
    query_terms
        .iter()
        .map(|(token_id, query_tf)| {
            let tf = *query_tf as f64;
            let df = token_id
                .map(|token_id| corpus.doc_freqs[token_id as usize])
                .unwrap_or(0) as f64;
            let idf = ((corpus.total_docs as f64 - df + 0.5) / (df + 0.5) + 1.0).ln();
            *query_tf as f64 * idf * (tf * (METADATA_BM25_K1 + 1.0)) / (tf + norm)
        })
        .sum()
}

fn compact_bm25_score_terms(
    query_terms: &[(Option<u32>, usize)],
    document: &CompactMetadataBm25Document,
    corpus: &CompactMetadataBm25Corpus,
) -> f64 {
    if query_terms.is_empty()
        || document.len == 0
        || corpus.total_docs == 0
        || corpus.avg_doc_len <= 0.0
    {
        return 0.0;
    }
    let doc_len = document.len as f64;
    let norm =
        METADATA_BM25_K1 * (1.0 - METADATA_BM25_B + METADATA_BM25_B * doc_len / corpus.avg_doc_len);
    query_terms
        .iter()
        .map(|(token_id, query_tf)| {
            let Some(token_id) = token_id else {
                return 0.0;
            };
            let tf = compact_term_frequency(document, *token_id) as f64;
            if tf == 0.0 {
                return 0.0;
            }
            let df = corpus.doc_freqs[*token_id as usize] as f64;
            let idf = ((corpus.total_docs as f64 - df + 0.5) / (df + 0.5) + 1.0).ln();
            *query_tf as f64 * idf * (tf * (METADATA_BM25_K1 + 1.0)) / (tf + norm)
        })
        .sum()
}

pub(crate) fn score_compact_metadata_indexed_pair_with_query(
    query: &CompactMetadataBm25Query<'_>,
    document: &CompactMetadataBm25Document,
) -> f64 {
    (compact_bm25_score_terms(&query.terms, document, query.corpus) / query.denominator)
        .clamp(0.0, 1.0)
}

#[cfg(test)]
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

#[cfg(test)]
impl MetadataBm25SingleDocumentQuery {
    pub(crate) fn new(document: MetadataBm25Document) -> Self {
        let terms = query_terms_from_tokens(document.tokens());
        Self { document, terms }
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

#[cfg(test)]
fn query_terms_overlap_document(
    query_terms: &[(String, usize)],
    document: &MetadataBm25Document,
) -> bool {
    query_terms
        .iter()
        .any(|(token, _)| document.term_frequency(token) > 0)
}

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
pub fn score_metadata_indexed_pair_with_corpus(
    left: &MetadataBm25Document,
    right: &MetadataBm25Document,
    corpus: &MetadataBm25Corpus,
) -> f64 {
    let query = MetadataBm25Query::new(left, corpus);
    score_metadata_indexed_pair_with_query(&query, right)
}

#[cfg(test)]
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
    metadata_documents_from_json(raw).content
}

pub fn score_name_pair(left: &str, right: &str) -> f64 {
    let left_norm = normalize_name(left);
    let right_norm = normalize_name(right);
    score_normalized_name_pair(&left_norm, &right_norm)
}

pub fn score_normalized_name_pair(left_norm: &str, right_norm: &str) -> f64 {
    if left_norm.is_empty() || right_norm.is_empty() {
        0.0
    } else if left_norm == right_norm {
        100.0
    } else {
        jaro_winkler::normalized_similarity(left_norm.chars(), right_norm.chars()) * 100.0
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

/// Thin public wrapper over CompactMetadataBm25Corpus (string BM25 is test-only).
pub fn score_metadata_document_pair(left: &str, right: &str) -> f64 {
    match score_metadata_documents(&[left.to_string()], &[right.to_string()]) {
        Ok(scores) => scores.into_iter().next().unwrap_or(0.0),
        Err(_) => 0.0,
    }
}

#[cfg(test)]
pub fn score_metadata_document_pair_with_corpus(
    left: &str,
    right: &str,
    corpus: &MetadataBm25Corpus,
) -> f64 {
    metadata_bm25_score_from_documents(left, right, corpus)
}

/// Public batch API implemented via CompactMetadataBm25Corpus.
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
    let (compact_corpus, compact_documents) =
        CompactMetadataBm25Corpus::from_indexed_documents(&corpus_docs);
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
            let compact_query = CompactMetadataBm25Query::new(&left_doc, &compact_corpus);
            score_compact_metadata_indexed_pair_with_query(
                &compact_query,
                &compact_documents[*right_doc_index],
            )
        })
        .collect())
}
