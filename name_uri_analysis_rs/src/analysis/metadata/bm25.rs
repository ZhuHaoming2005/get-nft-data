use std::collections::HashMap;
use std::sync::Arc;

use super::parse::metadata_bm25_tokens;
use super::{MetadataContractIndex, MetadataDocIndex, METADATA_THRESHOLD};

pub(super) const METADATA_BM25_K1: f64 = 1.2;
pub(super) const METADATA_BM25_B: f64 = 0.75;

#[derive(Debug, Clone)]
pub(crate) struct MetadataBm25Document {
    pub(super) tokens: Vec<String>,
    pub(super) unique_tokens: Vec<String>,
    pub(super) term_freqs: HashMap<String, usize>,
}

#[derive(Debug)]
pub(super) struct MetadataContentRecord {
    pub(super) contract_index: MetadataContractIndex,
    pub(super) doc: Arc<MetadataBm25Document>,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct CompactMetadataContentDocument {
    pub(super) len: usize,
    pub(super) terms: Vec<(u32, usize)>,
}

pub(super) struct CompactMetadataContentSet {
    pub(super) docs: Vec<CompactMetadataContentDocument>,
}

#[derive(Debug)]
pub(super) struct InternedMetadataDoc {
    pub(super) unique_tokens: Vec<usize>,
}

#[derive(Debug)]
pub(super) struct InternedMetadataSourceDoc {
    pub(super) tokens: Vec<usize>,
    pub(super) term_freqs: HashMap<usize, usize>,
    pub(super) unique_tokens: Vec<usize>,
}

#[derive(Debug)]
pub(super) struct InternedMetadataCorpus {
    pub(super) total_docs: usize,
    pub(super) avg_doc_len: f64,
    pub(super) doc_freqs: Vec<usize>,
}

#[derive(Debug)]
pub(super) struct PreparedInternedMetadataQuery {
    pub(super) terms: Vec<(usize, usize)>,
    pub(super) denominator: f64,
    pub(super) candidate_tokens: Vec<usize>,
}

#[derive(Debug)]
pub(super) struct PreparedInternedMetadataDoc {
    pub(super) token_weights: Vec<(usize, f64)>,
}


/// Shared Okapi BM25 term contribution used by interned and compact scorers.
#[inline]
pub(super) fn bm25_term_score(
    query_tf: f64,
    tf: f64,
    idf: f64,
    norm: f64,
    k1: f64,
) -> f64 {
    if tf == 0.0 {
        return 0.0;
    }
    query_tf * idf * (tf * (k1 + 1.0)) / (tf + norm)
}

impl MetadataBm25Document {
    pub(super) fn from_text(document: &str) -> Option<Self> {
        let mut tokens = metadata_bm25_tokens(document);
        if tokens.is_empty() {
            return None;
        }
        tokens.sort_unstable();
        let mut term_freqs = HashMap::new();
        for token in &tokens {
            *term_freqs.entry(token.clone()).or_insert(0usize) += 1;
        }
        let mut unique_tokens = tokens.clone();
        unique_tokens.sort_unstable();
        unique_tokens.dedup();
        Some(Self {
            tokens,
            unique_tokens,
            term_freqs,
        })
    }
}

impl InternedMetadataDoc {
    pub(super) fn from_source_doc(doc: InternedMetadataSourceDoc) -> Self {
        Self {
            unique_tokens: doc.unique_tokens,
        }
    }

    #[cfg(test)]
    pub(super) fn unique_tokens(&self) -> &[usize] {
        &self.unique_tokens
    }
}

pub(super) fn metadata_token_id(token: &str, token_ids: &HashMap<String, usize>) -> usize {
    *token_ids
        .get(token)
        .expect("metadata token must be present in the lexical token id map")
}

impl InternedMetadataSourceDoc {
    pub(super) fn from_metadata_doc(
        doc: &MetadataBm25Document,
        token_ids: &HashMap<String, usize>,
    ) -> Self {
        let mut tokens = Vec::with_capacity(doc.tokens.len());
        let mut term_freqs = HashMap::new();
        for token in &doc.tokens {
            let token_id = metadata_token_id(token, token_ids);
            tokens.push(token_id);
            *term_freqs.entry(token_id).or_insert(0usize) += 1;
        }
        let mut unique_tokens = term_freqs.keys().copied().collect::<Vec<_>>();
        unique_tokens.sort_unstable();
        Self {
            tokens,
            term_freqs,
            unique_tokens,
        }
    }

    pub(super) fn len(&self) -> usize {
        self.tokens.len()
    }

    pub(super) fn term_frequency(&self, token: usize) -> usize {
        *self.term_freqs.get(&token).unwrap_or(&0)
    }

    pub(super) fn unique_tokens(&self) -> &[usize] {
        &self.unique_tokens
    }
}

impl InternedMetadataCorpus {
    pub(super) fn from_doc_weights(
        doc_weights: &[usize],
        docs: &[InternedMetadataSourceDoc],
        token_count: usize,
    ) -> Self {
        let mut total_docs = 0usize;
        let mut total_terms = 0usize;
        let mut doc_freqs = vec![0; token_count];
        for (&weight, doc) in doc_weights.iter().zip(docs) {
            if weight == 0 {
                continue;
            }
            total_docs += weight;
            total_terms += doc.len() * weight;
            for &token in doc.unique_tokens() {
                doc_freqs[token] += weight;
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

impl PreparedInternedMetadataQuery {
    pub(super) fn new(
        query: &InternedMetadataSourceDoc,
        corpus: &InternedMetadataCorpus,
        max_token_weights: &[f64],
        postings: &[Vec<MetadataDocIndex>],
    ) -> Self {
        let terms = query_terms_from_token_ids(&query.tokens);
        let self_score = bm25_score_terms(&terms, query, corpus);
        let denominator = if self_score > 0.0 { self_score } else { 1.0 };
        let candidate_tokens = metadata_bm25_candidate_prefix(
            &terms,
            denominator,
            max_token_weights,
            postings,
            METADATA_THRESHOLD,
        );
        Self {
            terms,
            denominator,
            candidate_tokens,
        }
    }
}

pub(super) fn metadata_bm25_candidate_prefix(
    terms: &[(usize, usize)],
    denominator: f64,
    max_token_weights: &[f64],
    postings: &[Vec<MetadataDocIndex>],
    threshold: f64,
) -> Vec<usize> {
    let mut candidates = terms
        .iter()
        .filter_map(|&(token, query_tf)| {
            let max_weight = max_token_weights.get(token).copied().unwrap_or(0.0);
            let upper_bound = query_tf as f64 * max_weight;
            (upper_bound > 0.0).then_some((token, upper_bound, postings[token].len()))
        })
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return Vec::new();
    }

    candidates.sort_unstable_by(|left, right| {
        let left_cost = left.2 as f64 / left.1;
        let right_cost = right.2 as f64 / right.1;
        left_cost
            .total_cmp(&right_cost)
            .then_with(|| left.2.cmp(&right.2))
            .then_with(|| left.0.cmp(&right.0))
    });
    let mut remaining_upper_bound = candidates
        .iter()
        .map(|(_, upper_bound, _)| upper_bound)
        .sum::<f64>();
    let required_score = threshold * denominator;
    let tolerance = f64::EPSILON
        * (remaining_upper_bound.abs() + required_score.abs() + 1.0)
        * candidates.len() as f64
        * 8.0;
    let mut prefix = Vec::new();
    for (token, upper_bound, _) in candidates {
        prefix.push(token);
        remaining_upper_bound = (remaining_upper_bound - upper_bound).max(0.0);
        if remaining_upper_bound + tolerance < required_score {
            break;
        }
    }
    prefix
}

impl PreparedInternedMetadataDoc {
    pub(super) fn new(doc: &InternedMetadataSourceDoc, corpus: &InternedMetadataCorpus) -> Self {
        if doc.len() == 0 || corpus.total_docs == 0 || corpus.avg_doc_len <= 0.0 {
            return Self {
                token_weights: Vec::new(),
            };
        }

        let doc_len = doc.len() as f64;
        let norm = METADATA_BM25_K1
            * (1.0 - METADATA_BM25_B + METADATA_BM25_B * doc_len / corpus.avg_doc_len);
        let token_weights = doc
            .unique_tokens()
            .iter()
            .filter_map(|&token| {
                let tf = doc.term_frequency(token) as f64;
                if tf == 0.0 {
                    return None;
                }
                let df = corpus.doc_freqs.get(token).copied().unwrap_or(0) as f64;
                let idf = ((corpus.total_docs as f64 - df + 0.5) / (df + 0.5) + 1.0).ln();
                let weight = bm25_term_score(1.0, tf, idf, norm, METADATA_BM25_K1);
                Some((token, weight))
            })
            .collect();
        Self { token_weights }
    }
}

pub(super) fn score_metadata_with_prepared_doc(
    query: &PreparedInternedMetadataQuery,
    right: &PreparedInternedMetadataDoc,
) -> f64 {
    if query.terms.is_empty() || right.token_weights.is_empty() {
        return 0.0;
    }
    (bm25_score_prepared_terms(&query.terms, &right.token_weights) / query.denominator)
        .clamp(0.0, 1.0)
}

pub(super) fn bm25_score_prepared_terms(
    query_terms: &[(usize, usize)],
    doc_token_weights: &[(usize, f64)],
) -> f64 {
    let mut score = 0.0;
    let mut query_index = 0usize;
    let mut doc_index = 0usize;
    while query_index < query_terms.len() && doc_index < doc_token_weights.len() {
        let (query_token, query_tf) = query_terms[query_index];
        let (doc_token, doc_weight) = doc_token_weights[doc_index];
        match query_token.cmp(&doc_token) {
            std::cmp::Ordering::Less => query_index += 1,
            std::cmp::Ordering::Greater => doc_index += 1,
            std::cmp::Ordering::Equal => {
                score += query_tf as f64 * doc_weight;
                query_index += 1;
                doc_index += 1;
            }
        }
    }
    score
}

pub(super) fn query_terms_from_token_ids(query_tokens: &[usize]) -> Vec<(usize, usize)> {
    if query_tokens.is_empty() {
        return Vec::new();
    }
    let mut tokens = query_tokens.to_vec();
    tokens.sort_unstable();
    let mut terms = Vec::new();
    let mut iter = tokens.into_iter();
    let Some(mut current) = iter.next() else {
        return Vec::new();
    };
    let mut count = 1usize;
    for token in iter {
        if token == current {
            count += 1;
        } else {
            terms.push((current, count));
            current = token;
            count = 1;
        }
    }
    terms.push((current, count));
    terms
}

pub(super) fn bm25_score_terms(
    query_terms: &[(usize, usize)],
    doc: &InternedMetadataSourceDoc,
    corpus: &InternedMetadataCorpus,
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
            let tf = doc.term_frequency(*token) as f64;
            if tf == 0.0 {
                return 0.0;
            }
            let df = corpus.doc_freqs.get(*token).copied().unwrap_or(0) as f64;
            let idf = ((corpus.total_docs as f64 - df + 0.5) / (df + 0.5) + 1.0).ln();
            bm25_term_score(*query_tf as f64, tf, idf, norm, METADATA_BM25_K1)
        })
        .sum()
}

impl CompactMetadataContentSet {
    pub(super) fn from_records(records: &[MetadataContentRecord]) -> Self {
        let mut token_ids = HashMap::<&str, u32>::new();
        for record in records {
            for token in &record.doc.unique_tokens {
                if token_ids.contains_key(token.as_str()) {
                    continue;
                }
                let token_id = u32::try_from(token_ids.len())
                    .expect("metadata content token dictionary exceeds u32 indexes");
                token_ids.insert(token, token_id);
            }
        }
        let docs = records
            .iter()
            .map(|record| {
                let mut terms = record
                    .doc
                    .term_freqs
                    .iter()
                    .map(|(token, term_frequency)| {
                        (token_ids[token.as_str()], *term_frequency)
                    })
                    .collect::<Vec<_>>();
                terms.sort_unstable_by_key(|(token_id, _)| *token_id);
                CompactMetadataContentDocument {
                    len: record.doc.tokens.len(),
                    terms,
                }
            })
            .collect();
        Self { docs }
    }
}

pub(super) fn compact_metadata_content_pair_score(
    left: &CompactMetadataContentDocument,
    right: &CompactMetadataContentDocument,
) -> f64 {
    compact_metadata_single_document_score(left, right)
        .max(compact_metadata_single_document_score(right, left))
}

pub(super) fn compact_metadata_single_document_score(
    query: &CompactMetadataContentDocument,
    right: &CompactMetadataContentDocument,
) -> f64 {
    if !compact_metadata_content_docs_share_token(query, right) {
        return 0.0;
    }
    let numerator =
        compact_metadata_single_corpus_bm25_score(query, right, right);
    let denominator =
        compact_metadata_single_corpus_bm25_score(query, query, right);
    if denominator <= 0.0 {
        0.0
    } else {
        (numerator / denominator).clamp(0.0, 1.0)
    }
}

pub(super) fn compact_metadata_single_corpus_bm25_score(
    query: &CompactMetadataContentDocument,
    document: &CompactMetadataContentDocument,
    corpus_document: &CompactMetadataContentDocument,
) -> f64 {
    if query.len == 0 || document.len == 0 || corpus_document.len == 0 {
        return 0.0;
    }
    let doc_len = document.len as f64;
    let avg_doc_len = corpus_document.len as f64;
    let norm = METADATA_BM25_K1
        * (1.0 - METADATA_BM25_B
            + METADATA_BM25_B * doc_len / avg_doc_len);
    query
        .terms
        .iter()
        .map(|(token_id, query_tf)| {
            let tf = compact_metadata_content_term_frequency(
                document,
                *token_id,
            ) as f64;
            if tf == 0.0 {
                return 0.0;
            }
            let doc_freq = f64::from(
                compact_metadata_content_term_frequency(
                    corpus_document,
                    *token_id,
                ) > 0,
            );
            let idf =
                ((1.0 - doc_freq + 0.5) / (doc_freq + 0.5) + 1.0).ln();
            bm25_term_score(*query_tf as f64, tf, idf, norm, METADATA_BM25_K1)
        })
        .sum()
}

pub(super) fn compact_metadata_content_term_frequency(
    document: &CompactMetadataContentDocument,
    token_id: u32,
) -> usize {
    document
        .terms
        .binary_search_by_key(&token_id, |(document_token_id, _)| {
            *document_token_id
        })
        .ok()
        .map_or(0, |index| document.terms[index].1)
}

pub(super) fn compact_metadata_content_docs_share_token(
    left: &CompactMetadataContentDocument,
    right: &CompactMetadataContentDocument,
) -> bool {
    let mut left_index = 0usize;
    let mut right_index = 0usize;
    while left_index < left.terms.len() && right_index < right.terms.len() {
        match left.terms[left_index].0.cmp(&right.terms[right_index].0) {
            std::cmp::Ordering::Equal => return true,
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
        }
    }
    false
}

#[cfg(test)]
pub(super) fn metadata_content_pair_score(
    left: &MetadataBm25Document,
    right: &MetadataBm25Document,
) -> f64 {
    metadata_single_document_score(left, right)
        .max(metadata_single_document_score(right, left))
}

#[cfg(test)]
pub(super) fn metadata_single_document_score(
    query: &MetadataBm25Document,
    right: &MetadataBm25Document,
) -> f64 {
    if !metadata_string_docs_share_token(query, right) {
        return 0.0;
    }
    let numerator = metadata_single_corpus_bm25_score(query, right, right);
    let denominator = metadata_single_corpus_bm25_score(query, query, right);
    if denominator <= 0.0 {
        0.0
    } else {
        (numerator / denominator).clamp(0.0, 1.0)
    }
}

#[cfg(test)]
pub(super) fn metadata_single_corpus_bm25_score(
    query: &MetadataBm25Document,
    doc: &MetadataBm25Document,
    corpus_doc: &MetadataBm25Document,
) -> f64 {
    if query.tokens.is_empty() || doc.tokens.is_empty() || corpus_doc.tokens.is_empty() {
        return 0.0;
    }
    let doc_len = doc.tokens.len() as f64;
    let avg_doc_len = corpus_doc.tokens.len() as f64;
    let norm =
        METADATA_BM25_K1 * (1.0 - METADATA_BM25_B + METADATA_BM25_B * doc_len / avg_doc_len);
    query
        .term_freqs
        .iter()
        .map(|(token, query_tf)| {
            let tf = doc.term_freqs.get(token).copied().unwrap_or(0) as f64;
            if tf == 0.0 {
                return 0.0;
            }
            let doc_freq = f64::from(corpus_doc.term_freqs.contains_key(token));
            let idf = ((1.0 - doc_freq + 0.5) / (doc_freq + 0.5) + 1.0).ln();
            bm25_term_score(*query_tf as f64, tf, idf, norm, METADATA_BM25_K1)
        })
        .sum()
}

#[cfg(test)]
pub(super) fn metadata_string_docs_share_token(
    left: &MetadataBm25Document,
    right: &MetadataBm25Document,
) -> bool {
    let mut left_index = 0usize;
    let mut right_index = 0usize;
    while left_index < left.unique_tokens.len() && right_index < right.unique_tokens.len() {
        match left.unique_tokens[left_index].cmp(&right.unique_tokens[right_index]) {
            std::cmp::Ordering::Equal => return true,
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
        }
    }
    false
}
