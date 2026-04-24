use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};

use once_cell::sync::Lazy;
use rayon::prelude::*;
use regex::Regex;
use serde::Serialize;
use strsim::{damerau_levenshtein, jaro_winkler};
use top_contract_analysis_rs::normalize::{normalize_name, normalize_text};

use crate::decision_rules::duplicate_score_rule;
use crate::sample::BenchmarkSample;
use crate::store::FeatureRow;

static TOKEN_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"[\p{L}\p{N}_]+").unwrap());

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AlgorithmField {
    Name,
    Metadata,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct CandidateScore {
    pub rank: usize,
    pub contract_address: String,
    pub token_id: String,
    pub name: String,
    pub score: f64,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct NameContractDuplicate {
    pub contract_address: String,
    pub name: String,
    pub metadata_doc: String,
    pub max_score: f64,
    pub duplicate_token_count: usize,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct MetadataDuplicate {
    pub contract_address: String,
    pub metadata_doc: String,
    pub score: f64,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct NameAlgorithmReport {
    pub algorithm_id: String,
    pub field: AlgorithmField,
    pub decision_rule: String,
    pub repeat: usize,
    pub runs_ms: Vec<f64>,
    pub avg_ms: f64,
    pub min_ms: f64,
    pub duplicate_count: usize,
    pub duplicates: Vec<NameContractDuplicate>,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct MetadataAlgorithmReport {
    pub algorithm_id: String,
    pub field: AlgorithmField,
    pub decision_rule: String,
    pub repeat: usize,
    pub runs_ms: Vec<f64>,
    pub avg_ms: f64,
    pub min_ms: f64,
    pub duplicate_count: usize,
    pub duplicates: Vec<MetadataDuplicate>,
}

#[derive(Clone, Copy)]
pub struct TimingAlgorithmDefinition {
    pub id: &'static str,
    pub field: AlgorithmField,
    pub scorer: fn(&BenchmarkSample, &FeatureRow) -> f64,
}

pub fn metadata_duplicate_doc_scorer(algorithm_id: &str) -> Result<fn(&str, &str) -> f64, String> {
    match algorithm_id {
        "metadata_token_cosine" => Ok(token_cosine),
        "metadata_soft_tfidf" => Ok(soft_tfidf),
        "metadata_weighted_jaccard" => Ok(weighted_jaccard),
        "metadata_bm25" => Err("metadata_bm25 requires corpus-aware scoring".to_string()),
        _ => Err(format!("unknown metadata algorithm id: {algorithm_id}")),
    }
}

pub fn timing_algorithms() -> Vec<TimingAlgorithmDefinition> {
    vec![
        TimingAlgorithmDefinition {
            id: "name_exact_normalized",
            field: AlgorithmField::Name,
            scorer: score_name_exact_normalized_raw,
        },
        TimingAlgorithmDefinition {
            id: "name_jaro_winkler",
            field: AlgorithmField::Name,
            scorer: score_name_jaro_winkler_raw,
        },
        TimingAlgorithmDefinition {
            id: "name_damerau_levenshtein",
            field: AlgorithmField::Name,
            scorer: score_name_damerau_levenshtein_raw,
        },
        TimingAlgorithmDefinition {
            id: "name_monge_elkan",
            field: AlgorithmField::Name,
            scorer: score_name_monge_elkan_raw,
        },
        TimingAlgorithmDefinition {
            id: "metadata_bm25",
            field: AlgorithmField::Metadata,
            scorer: score_metadata_bm25_raw,
        },
        TimingAlgorithmDefinition {
            id: "metadata_token_cosine",
            field: AlgorithmField::Metadata,
            scorer: score_metadata_token_cosine_raw,
        },
        TimingAlgorithmDefinition {
            id: "metadata_soft_tfidf",
            field: AlgorithmField::Metadata,
            scorer: score_metadata_soft_tfidf_raw,
        },
        TimingAlgorithmDefinition {
            id: "metadata_weighted_jaccard",
            field: AlgorithmField::Metadata,
            scorer: score_metadata_weighted_jaccard_raw,
        },
    ]
}

pub fn metadata_keywords(document: &str, limit: usize) -> Vec<String> {
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for token in TOKEN_RE.find_iter(document) {
        let token = token.as_str().to_lowercase();
        if token.len() < 4 {
            continue;
        }
        *counts.entry(token).or_insert(0) += 1;
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

pub fn tokenize(document: &str) -> Vec<String> {
    TOKEN_RE
        .find_iter(document)
        .map(|token| token.as_str().to_lowercase())
        .filter(|token| token.len() >= 2)
        .collect()
}

#[cfg(test)]
fn token_jaccard(left: &str, right: &str) -> f64 {
    let left_tokens: HashSet<String> = tokenize(&normalize_text(left)).into_iter().collect();
    let right_tokens: HashSet<String> = tokenize(&normalize_text(right)).into_iter().collect();
    token_jaccard_from_sets(&left_tokens, &right_tokens)
}

#[cfg(test)]
fn token_jaccard_from_sets(left: &HashSet<String>, right: &HashSet<String>) -> f64 {
    if left.is_empty() || right.is_empty() {
        return 0.0;
    }
    let union = left.union(right).count();
    let overlap = left.intersection(right).count();
    if union == 0 {
        0.0
    } else {
        overlap as f64 / union as f64
    }
}

fn token_cosine(left: &str, right: &str) -> f64 {
    let mut left_counts = BTreeMap::<String, f64>::new();
    let mut right_counts = BTreeMap::<String, f64>::new();
    for token in tokenize(&normalize_text(left)) {
        *left_counts.entry(token).or_insert(0.0) += 1.0;
    }
    for token in tokenize(&normalize_text(right)) {
        *right_counts.entry(token).or_insert(0.0) += 1.0;
    }
    if left_counts.is_empty() || right_counts.is_empty() {
        return 0.0;
    }
    token_cosine_from_counts(&left_counts, &right_counts)
}

fn token_cosine_from_counts(left: &BTreeMap<String, f64>, right: &BTreeMap<String, f64>) -> f64 {
    if left.is_empty() || right.is_empty() {
        return 0.0;
    }
    let dot = left
        .iter()
        .map(|(token, left_value)| left_value * right.get(token).unwrap_or(&0.0))
        .sum::<f64>();
    let left_norm = left.values().map(|value| value * value).sum::<f64>().sqrt();
    let right_norm = right
        .values()
        .map(|value| value * value)
        .sum::<f64>()
        .sqrt();
    if left_norm == 0.0 || right_norm == 0.0 {
        0.0
    } else {
        dot / (left_norm * right_norm)
    }
}

fn tokenize_name(document: &str) -> Vec<String> {
    tokenize(document)
        .into_iter()
        .filter(|token| !token.chars().all(|ch| ch.is_ascii_digit()))
        .collect()
}

fn monge_elkan(left: &str, right: &str) -> f64 {
    let left_tokens = tokenize_name(&normalize_text(left));
    let right_tokens = tokenize_name(&normalize_text(right));
    if left_tokens.is_empty() || right_tokens.is_empty() {
        return 0.0;
    }

    let left_to_right = monge_elkan_direction(&left_tokens, &right_tokens);
    let right_to_left = monge_elkan_direction(&right_tokens, &left_tokens);
    (left_to_right + right_to_left) / 2.0
}

fn monge_elkan_direction(left_tokens: &[String], right_tokens: &[String]) -> f64 {
    let sum = left_tokens
        .iter()
        .map(|left| {
            right_tokens
                .iter()
                .map(|right| jaro_winkler(left, right))
                .fold(0.0, f64::max)
        })
        .sum::<f64>();
    sum / left_tokens.len() as f64
}

fn token_counts(document: &str) -> BTreeMap<String, f64> {
    let mut counts = BTreeMap::<String, f64>::new();
    for token in tokenize(&normalize_text(document)) {
        *counts.entry(token).or_insert(0.0) += 1.0;
    }
    counts
}

fn inverse_document_frequencies(
    left: &BTreeMap<String, f64>,
    right: &BTreeMap<String, f64>,
) -> BTreeMap<String, f64> {
    let mut doc_freqs = BTreeMap::<String, f64>::new();
    for token in left.keys() {
        *doc_freqs.entry(token.clone()).or_insert(0.0) += 1.0;
    }
    for token in right.keys() {
        *doc_freqs.entry(token.clone()).or_insert(0.0) += 1.0;
    }

    doc_freqs
        .into_iter()
        .map(|(token, df)| {
            let idf = ((2.0 - df + 0.5) / (df + 0.5) + 1.0).ln().max(0.1);
            (token, idf)
        })
        .collect()
}

fn weighted_jaccard(left: &str, right: &str) -> f64 {
    let left_counts = token_counts(left);
    let right_counts = token_counts(right);
    if left_counts.is_empty() || right_counts.is_empty() {
        return 0.0;
    }

    let weights = inverse_document_frequencies(&left_counts, &right_counts);
    let mut all_tokens: HashSet<String> = HashSet::new();
    all_tokens.extend(left_counts.keys().cloned());
    all_tokens.extend(right_counts.keys().cloned());

    let mut numerator = 0.0;
    let mut denominator = 0.0;
    for token in all_tokens {
        let left_value = *left_counts.get(&token).unwrap_or(&0.0);
        let right_value = *right_counts.get(&token).unwrap_or(&0.0);
        let weight = *weights.get(&token).unwrap_or(&1.0);
        numerator += left_value.min(right_value) * weight;
        denominator += left_value.max(right_value) * weight;
    }

    if denominator == 0.0 {
        0.0
    } else {
        numerator / denominator
    }
}

fn soft_tfidf(left: &str, right: &str) -> f64 {
    let left_counts = token_counts(left);
    let right_counts = token_counts(right);
    if left_counts.is_empty() || right_counts.is_empty() {
        return 0.0;
    }

    let weights = inverse_document_frequencies(&left_counts, &right_counts);
    let left_vector = tfidf_vector(&left_counts, &weights);
    let right_vector = tfidf_vector(&right_counts, &weights);

    let mut dot = 0.0;
    for (left_token, left_weight) in &left_vector {
        let best = right_vector
            .iter()
            .map(|(right_token, right_weight)| {
                let similarity = if left_token == right_token {
                    1.0
                } else {
                    jaro_winkler(left_token, right_token)
                };
                if similarity >= 0.9 {
                    left_weight * right_weight * similarity
                } else {
                    0.0
                }
            })
            .fold(0.0, f64::max);
        dot += best;
    }

    let left_norm = left_vector
        .values()
        .map(|value| value * value)
        .sum::<f64>()
        .sqrt();
    let right_norm = right_vector
        .values()
        .map(|value| value * value)
        .sum::<f64>()
        .sqrt();
    if left_norm == 0.0 || right_norm == 0.0 {
        0.0
    } else {
        (dot / (left_norm * right_norm)).clamp(0.0, 1.0)
    }
}

fn tfidf_vector(
    counts: &BTreeMap<String, f64>,
    weights: &BTreeMap<String, f64>,
) -> BTreeMap<String, f64> {
    counts
        .iter()
        .map(|(token, tf)| {
            (
                token.clone(),
                tf * weights.get(token).copied().unwrap_or(1.0),
            )
        })
        .collect()
}

pub fn score_name_exact_normalized_raw(sample: &BenchmarkSample, row: &FeatureRow) -> f64 {
    if sample.name_norm.is_empty() || row.name_norm.is_empty() {
        0.0
    } else if sample.name_norm == row.name_norm {
        100.0
    } else {
        0.0
    }
}

pub fn score_name_jaro_winkler_raw(sample: &BenchmarkSample, row: &FeatureRow) -> f64 {
    if sample.name_norm.is_empty() || row.name_norm.is_empty() {
        0.0
    } else {
        jaro_winkler(&sample.name_norm, &row.name_norm) * 100.0
    }
}

pub fn score_name_damerau_levenshtein_raw(sample: &BenchmarkSample, row: &FeatureRow) -> f64 {
    if sample.name_norm.is_empty() || row.name_norm.is_empty() {
        0.0
    } else {
        normalized_damerau_levenshtein(&sample.name_norm, &row.name_norm) * 100.0
    }
}

pub fn score_name_monge_elkan_raw(sample: &BenchmarkSample, row: &FeatureRow) -> f64 {
    if sample.name_norm.is_empty() || row.name_norm.is_empty() {
        0.0
    } else {
        monge_elkan(&sample.name_norm, &row.name_norm) * 100.0
    }
}

pub fn score_metadata_token_cosine_raw(sample: &BenchmarkSample, row: &FeatureRow) -> f64 {
    best_metadata_doc_score_raw(sample, row, token_cosine)
}

pub fn score_metadata_soft_tfidf_raw(sample: &BenchmarkSample, row: &FeatureRow) -> f64 {
    best_metadata_doc_score_raw(sample, row, soft_tfidf)
}

pub fn score_metadata_weighted_jaccard_raw(sample: &BenchmarkSample, row: &FeatureRow) -> f64 {
    best_metadata_doc_score_raw(sample, row, weighted_jaccard)
}

pub fn score_metadata_bm25_raw(_sample: &BenchmarkSample, _row: &FeatureRow) -> f64 {
    unreachable!("metadata_bm25 requires corpus-aware scoring")
}

fn normalized_damerau_levenshtein(left: &str, right: &str) -> f64 {
    let left_chars = left.chars().count();
    let right_chars = right.chars().count();
    let max_len = left_chars.max(right_chars);
    if max_len == 0 {
        1.0
    } else {
        let distance = damerau_levenshtein(left, right) as f64;
        (1.0 - distance / max_len as f64).clamp(0.0, 1.0)
    }
}

struct Bm25CorpusStats {
    total_docs: usize,
    avg_doc_len: f64,
    doc_freqs: BTreeMap<String, usize>,
}

fn bm25_corpus_stats(rows: &[FeatureRow]) -> Bm25CorpusStats {
    let mut total_docs = 0usize;
    let mut total_terms = 0usize;
    let mut doc_freqs = BTreeMap::<String, usize>::new();

    for row in rows {
        for metadata_doc in &row.metadata_docs {
            let tokens = tokenize(&normalize_text(metadata_doc));
            if tokens.is_empty() {
                continue;
            }
            total_docs += 1;
            total_terms += tokens.len();
            let unique_tokens: HashSet<String> = tokens.into_iter().collect();
            for token in unique_tokens {
                *doc_freqs.entry(token).or_insert(0) += 1;
            }
        }
    }

    let avg_doc_len = if total_docs == 0 {
        0.0
    } else {
        total_terms as f64 / total_docs as f64
    };

    Bm25CorpusStats {
        total_docs,
        avg_doc_len,
        doc_freqs,
    }
}

fn bm25_score_tokens(
    query_tokens: &[String],
    doc_tokens: &[String],
    stats: &Bm25CorpusStats,
) -> f64 {
    if query_tokens.is_empty()
        || doc_tokens.is_empty()
        || stats.total_docs == 0
        || stats.avg_doc_len <= 0.0
    {
        return 0.0;
    }

    let mut term_freqs = BTreeMap::<String, usize>::new();
    for token in doc_tokens {
        *term_freqs.entry(token.clone()).or_insert(0) += 1;
    }

    let k1 = 1.2;
    let b = 0.75;
    let doc_len = doc_tokens.len() as f64;
    let norm = k1 * (1.0 - b + b * doc_len / stats.avg_doc_len);

    query_tokens
        .iter()
        .map(|token| {
            let tf = *term_freqs.get(token).unwrap_or(&0) as f64;
            if tf == 0.0 {
                return 0.0;
            }
            let df = *stats.doc_freqs.get(token).unwrap_or(&0) as f64;
            let idf = ((stats.total_docs as f64 - df + 0.5) / (df + 0.5) + 1.0).ln();
            idf * (tf * (k1 + 1.0)) / (tf + norm)
        })
        .sum()
}

fn bm25_self_score(query_tokens: &[String], stats: &Bm25CorpusStats) -> f64 {
    bm25_score_tokens(query_tokens, query_tokens, stats)
}

pub fn score_metadata_bm25_all_rows_raw(sample: &BenchmarkSample, rows: &[FeatureRow]) -> Vec<f64> {
    let stats = bm25_corpus_stats(rows);
    let query_tokens = tokenize(&normalize_text(&sample.metadata_doc));
    let self_score = bm25_self_score(&query_tokens, &stats);
    let denominator = if self_score > 0.0 { self_score } else { 1.0 };

    rows.par_iter()
        .map(|row| {
            row.metadata_docs
                .iter()
                .map(|metadata_doc| {
                    let tokens = tokenize(&normalize_text(metadata_doc));
                    bm25_score_tokens(&query_tokens, &tokens, &stats) / denominator
                })
                .fold(0.0, f64::max)
                .clamp(0.0, 1.0)
        })
        .collect()
}

pub fn build_algorithm_duplicates_raw_from_scores(
    algorithm_id: &str,
    rows: &[FeatureRow],
    scored: &[f64],
) -> Result<(usize, Vec<CandidateScore>), String> {
    debug_assert_eq!(rows.len(), scored.len());
    let rule = duplicate_score_rule(algorithm_id)?;
    let mut duplicates: Vec<(usize, f64)> = scored
        .iter()
        .enumerate()
        .filter_map(|(index, score)| (*score >= rule.threshold).then_some((index, *score)))
        .collect();
    duplicates.sort_by(|left, right| {
        right
            .1
            .partial_cmp(&left.1)
            .unwrap_or(Ordering::Equal)
            .then_with(|| {
                rows[left.0]
                    .contract_address
                    .cmp(&rows[right.0].contract_address)
            })
            .then_with(|| rows[left.0].token_id.cmp(&rows[right.0].token_id))
    });
    let total = duplicates.len();
    let duplicates = duplicates
        .into_iter()
        .enumerate()
        .map(|(rank, (index, score))| CandidateScore {
            rank: rank + 1,
            contract_address: rows[index].contract_address.clone(),
            token_id: rows[index].token_id.clone(),
            name: rows[index].name.clone(),
            score,
        })
        .collect();
    Ok((total, duplicates))
}

pub fn name_duplicates_from_candidates(
    rows: &[FeatureRow],
    duplicates: Vec<CandidateScore>,
) -> Vec<NameContractDuplicate> {
    let row_lookup: BTreeMap<String, &FeatureRow> = rows
        .iter()
        .map(|row| (row.contract_address.clone(), row))
        .collect();

    duplicates
        .into_iter()
        .filter_map(|candidate| {
            row_lookup
                .get(&candidate.contract_address)
                .map(|row| NameContractDuplicate {
                    contract_address: candidate.contract_address,
                    name: row.name.clone(),
                    metadata_doc: row.metadata_display_doc.clone(),
                    max_score: candidate.score,
                    duplicate_token_count: row.token_count,
                })
        })
        .collect()
}

fn best_metadata_doc_score_raw(
    sample: &BenchmarkSample,
    row: &FeatureRow,
    per_doc: fn(&str, &str) -> f64,
) -> f64 {
    row.metadata_docs
        .iter()
        .map(|metadata_doc| per_doc(&sample.metadata_doc, metadata_doc))
        .fold(0.0, f64::max)
}

fn best_metadata_doc_for_row(
    sample: &BenchmarkSample,
    row: &FeatureRow,
    per_doc: fn(&str, &str) -> f64,
) -> Option<(String, f64)> {
    row.metadata_docs
        .iter()
        .zip(row.metadata_display_docs.iter())
        .fold(None, |best, (metadata_doc, metadata_display_doc)| {
            let score = per_doc(&sample.metadata_doc, metadata_doc);
            match best {
                Some((_, best_score)) if best_score >= score => best,
                _ => Some((metadata_display_doc.clone(), score)),
            }
        })
}

fn best_metadata_doc_for_row_bm25(
    sample: &BenchmarkSample,
    row: &FeatureRow,
    stats: &Bm25CorpusStats,
) -> Option<(String, f64)> {
    let query_tokens = tokenize(&normalize_text(&sample.metadata_doc));
    let self_score = bm25_self_score(&query_tokens, stats);
    let denominator = if self_score > 0.0 { self_score } else { 1.0 };

    row.metadata_docs
        .iter()
        .zip(row.metadata_display_docs.iter())
        .fold(None, |best, (metadata_doc, metadata_display_doc)| {
            let doc_tokens = tokenize(&normalize_text(metadata_doc));
            let score = (bm25_score_tokens(&query_tokens, &doc_tokens, stats) / denominator)
                .clamp(0.0, 1.0);
            match best {
                Some((_, best_score)) if best_score >= score => best,
                _ => Some((metadata_display_doc.clone(), score)),
            }
        })
}

pub fn metadata_duplicates_from_candidates(
    sample: &BenchmarkSample,
    rows: &[FeatureRow],
    duplicates: Vec<CandidateScore>,
    algorithm_id: &str,
) -> Vec<MetadataDuplicate> {
    let row_lookup: BTreeMap<String, &FeatureRow> = rows
        .iter()
        .map(|row| (row.contract_address.clone(), row))
        .collect();
    let bm25_stats = (algorithm_id == "metadata_bm25").then(|| bm25_corpus_stats(rows));
    let per_doc = metadata_duplicate_doc_scorer(algorithm_id).ok();

    duplicates
        .into_iter()
        .filter_map(|candidate| {
            let contract_address = candidate.contract_address;
            row_lookup
                .get(&contract_address)
                .and_then(|row| {
                    if let Some(stats) = bm25_stats.as_ref() {
                        best_metadata_doc_for_row_bm25(sample, row, stats)
                    } else {
                        per_doc.and_then(|scorer| best_metadata_doc_for_row(sample, row, scorer))
                    }
                })
                .map(|(metadata_doc, score)| MetadataDuplicate {
                    contract_address,
                    metadata_doc,
                    score,
                })
        })
        .collect()
}

pub fn score_rows_parallel_raw(
    sample: &BenchmarkSample,
    rows: &[FeatureRow],
    scorer: fn(&BenchmarkSample, &FeatureRow) -> f64,
) -> Vec<f64> {
    rows.par_iter().map(|row| scorer(sample, row)).collect()
}

pub fn derive_name_norm(name: &str) -> String {
    normalize_name(name)
}

pub fn parse_keywords(raw: &str, metadata_doc: &str) -> Vec<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("null") {
        return metadata_keywords(metadata_doc, 8);
    }
    if let Ok(parsed) = serde_json::from_str::<Vec<String>>(trimmed) {
        return parsed;
    }
    if trimmed.starts_with('[') && trimmed.ends_with(']') {
        let inner = &trimmed[1..trimmed.len() - 1];
        let values: Vec<String> = inner
            .split(',')
            .map(|value| value.trim().trim_matches('"').trim_matches('\''))
            .filter(|value| !value.is_empty())
            .map(|value| value.to_lowercase())
            .collect();
        if !values.is_empty() {
            return values;
        }
    }
    metadata_keywords(metadata_doc, 8)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::FeatureRow;

    fn sample_raw() -> BenchmarkSample {
        BenchmarkSample {
            chain: "ethereum".into(),
            contract_address: String::new(),
            token_id: String::new(),
            token_uri: String::new(),
            image_uri: String::new(),
            name: "Azuki #1".into(),
            name_norm: normalize_name("Azuki #1"),
            metadata_json: "{\"description\":\"gold dragon rare\"}".into(),
            metadata_doc: "gold dragon rare".into(),
            metadata_display_doc: "gold dragon rare".into(),
            metadata_keywords: vec!["dragon".into(), "gold".into(), "rare".into()],
        }
    }

    fn row_raw() -> FeatureRow {
        FeatureRow {
            contract_address: "0xdup".into(),
            token_id: "1".into(),
            token_uri: String::new(),
            image_uri: String::new(),
            name: "Azuki".into(),
            name_norm: normalize_name("Azuki"),
            metadata_doc: "rare dragon gold".into(),
            metadata_display_doc: "rare dragon gold".into(),
            metadata_docs: vec!["rare dragon gold".into()],
            metadata_display_docs: vec!["rare dragon gold".into()],
            token_uris: Vec::new(),
            image_uris: Vec::new(),
            metadata_keywords: vec!["dragon".into(), "gold".into(), "rare".into()],
            token_count: 1,
        }
    }

    fn second_row_raw() -> FeatureRow {
        FeatureRow {
            contract_address: "0xdup2".into(),
            token_id: "2".into(),
            token_uri: String::new(),
            image_uri: String::new(),
            name: "Azuki Mirror #2".into(),
            name_norm: normalize_name("Azuki Mirror #2"),
            metadata_doc: "rare dragon silver".into(),
            metadata_display_doc: "rare dragon silver".into(),
            metadata_docs: vec!["rare dragon silver".into()],
            metadata_display_docs: vec!["rare dragon silver".into()],
            token_uris: Vec::new(),
            image_uris: Vec::new(),
            metadata_keywords: vec!["dragon".into(), "rare".into(), "silver".into()],
            token_count: 1,
        }
    }

    fn third_row_raw() -> FeatureRow {
        FeatureRow {
            contract_address: "0xdup3".into(),
            token_id: "3".into(),
            token_uri: String::new(),
            image_uri: String::new(),
            name: "Azuki Variant #3".into(),
            name_norm: normalize_name("Azuki Variant #3"),
            metadata_doc: "gold dragon ultra rare".into(),
            metadata_display_doc: "gold dragon ultra rare".into(),
            metadata_docs: vec!["gold dragon ultra rare".into()],
            metadata_display_docs: vec!["gold dragon ultra rare".into()],
            token_uris: Vec::new(),
            image_uris: Vec::new(),
            metadata_keywords: vec!["dragon".into(), "gold".into(), "rare".into()],
            token_count: 1,
        }
    }

    #[test]
    fn damerau_name_score_is_high_for_close_names() {
        assert!(score_name_damerau_levenshtein_raw(&sample_raw(), &row_raw()) > 80.0);
    }

    #[test]
    fn bm25_metadata_score_is_high_for_identical_doc() {
        let scores = score_metadata_bm25_all_rows_raw(&sample_raw(), &[row_raw()]);
        assert_eq!(scores.len(), 1);
        assert!(scores[0] > 0.7);
    }

    #[test]
    fn bm25_parallel_row_scoring_matches_sequential_scoring() {
        let sample = sample_raw();
        let rows = vec![row_raw(), second_row_raw(), third_row_raw()];
        let stats = bm25_corpus_stats(&rows);
        let query_tokens = tokenize(&normalize_text(&sample.metadata_doc));
        let self_score = bm25_self_score(&query_tokens, &stats);
        let denominator = if self_score > 0.0 { self_score } else { 1.0 };

        let sequential: Vec<f64> = rows
            .iter()
            .map(|row| {
                row.metadata_docs
                    .iter()
                    .map(|metadata_doc| {
                        let tokens = tokenize(&normalize_text(metadata_doc));
                        bm25_score_tokens(&query_tokens, &tokens, &stats) / denominator
                    })
                    .fold(0.0, f64::max)
                    .clamp(0.0, 1.0)
            })
            .collect();

        let parallel = score_metadata_bm25_all_rows_raw(&sample, &rows);
        assert_eq!(parallel, sequential);
    }

    #[test]
    fn monge_elkan_name_score_is_high_for_tokenwise_close_names() {
        let sample = BenchmarkSample {
            name: "Azuki Dragon".into(),
            name_norm: normalize_name("Azuki Dragon"),
            ..sample_raw()
        };
        let row = FeatureRow {
            name: "Azuki Dragons".into(),
            name_norm: normalize_name("Azuki Dragons"),
            ..row_raw()
        };

        assert!(score_name_monge_elkan_raw(&sample, &row) >= 85.0);
    }

    #[test]
    fn token_cosine_handles_repeated_tokens() {
        let score = token_cosine("gold gold dragon", "gold dragon");
        assert!(score > 0.9);
        assert!(score <= 1.0);
    }

    #[test]
    fn weighted_jaccard_rewards_rare_overlap_more_than_plain_jaccard() {
        let left = "gold dragon ultra";
        let right = "gold dragon";

        let weighted = weighted_jaccard(left, right);
        let plain = token_jaccard(left, right);
        assert!(weighted > 0.0);
        assert!(weighted != plain);
    }

    #[test]
    fn soft_tfidf_gives_credit_for_similar_tokens() {
        let score = soft_tfidf("gold dragon", "golden dragon");
        assert!(score >= 0.75);
    }

    #[test]
    fn parallel_raw_row_scoring_matches_sequential_scoring() {
        let sample = sample_raw();
        let rows = vec![row_raw(), second_row_raw()];
        let sequential: Vec<f64> = rows
            .iter()
            .map(|row| score_name_damerau_levenshtein_raw(&sample, row))
            .collect();
        let parallel = score_rows_parallel_raw(&sample, &rows, score_name_damerau_levenshtein_raw);

        assert_eq!(parallel, sequential);
    }

    #[test]
    fn ordinary_duplicate_filtering_respects_per_algorithm_threshold() {
        let rows = vec![row_raw(), second_row_raw()];
        let scores = vec![95.0, 94.99];

        let (duplicate_count, duplicates) =
            build_algorithm_duplicates_raw_from_scores("name_jaro_winkler", &rows, &scores)
                .unwrap();

        assert_eq!(duplicate_count, 1);
        assert_eq!(duplicates.len(), 1);
        assert_eq!(duplicates[0].contract_address, "0xdup");
        assert_eq!(duplicates[0].score, 95.0);
    }

    #[test]
    fn ordinary_duplicates_are_not_top_k_truncated() {
        let rows = vec![row_raw(), second_row_raw(), third_row_raw()];
        let scores = vec![100.0, 99.0, 98.0];

        let (duplicate_count, duplicates) =
            build_algorithm_duplicates_raw_from_scores("name_damerau_levenshtein", &rows, &scores)
                .unwrap();

        assert_eq!(duplicate_count, 3);
        assert_eq!(duplicates.len(), 3);
        assert_eq!(
            duplicates
                .iter()
                .map(|candidate| candidate.contract_address.as_str())
                .collect::<Vec<_>>(),
            vec!["0xdup", "0xdup2", "0xdup3"]
        );
    }

    #[test]
    fn name_duplicates_are_grouped_by_contract() {
        let duplicates = vec![
            CandidateScore {
                rank: 1,
                contract_address: "0xdup".into(),
                token_id: "1".into(),
                name: "Azuki".into(),
                score: 100.0,
            },
            CandidateScore {
                rank: 2,
                contract_address: "0xother".into(),
                token_id: "1".into(),
                name: "Azuki Alt".into(),
                score: 98.0,
            },
        ];

        let rows = vec![
            FeatureRow {
                contract_address: "0xdup".into(),
                token_id: "1".into(),
                token_uri: String::new(),
                image_uri: String::new(),
                name: "Azuki".into(),
                name_norm: normalize_name("Azuki"),
                metadata_doc: "rare dragon gold".into(),
                metadata_display_doc: "rare dragon gold".into(),
                metadata_docs: vec!["rare dragon gold".into(), "blue tiger".into()],
                metadata_display_docs: vec!["rare dragon gold".into(), "blue tiger".into()],
                token_uris: Vec::new(),
                image_uris: Vec::new(),
                metadata_keywords: vec!["dragon".into()],
                token_count: 2,
            },
            FeatureRow {
                contract_address: "0xother".into(),
                token_id: "1".into(),
                token_uri: String::new(),
                image_uri: String::new(),
                name: "Azuki Alt".into(),
                name_norm: normalize_name("Azuki Alt"),
                metadata_doc: "rare dragon gold".into(),
                metadata_display_doc: "rare dragon gold".into(),
                metadata_docs: vec!["rare dragon gold".into()],
                metadata_display_docs: vec!["rare dragon gold".into()],
                token_uris: Vec::new(),
                image_uris: Vec::new(),
                metadata_keywords: vec!["dragon".into()],
                token_count: 1,
            },
        ];

        let grouped = name_duplicates_from_candidates(&rows, duplicates);

        assert_eq!(grouped.len(), 2);
        assert_eq!(grouped[0].contract_address, "0xdup");
        assert_eq!(grouped[0].name, "Azuki");
        assert_eq!(grouped[0].metadata_doc, "rare dragon gold");
        assert_eq!(grouped[0].max_score, 100.0);
        assert_eq!(grouped[0].duplicate_token_count, 2);
    }

    #[test]
    fn metadata_duplicates_use_metadata_doc_in_output() {
        let rows = vec![row_raw()];
        let duplicates = vec![CandidateScore {
            rank: 1,
            contract_address: "0xdup".into(),
            token_id: "1".into(),
            name: "Azuki".into(),
            score: 0.9,
        }];

        let metadata_duplicates = metadata_duplicates_from_candidates(
            &sample_raw(),
            &rows,
            duplicates,
            "metadata_token_cosine",
        );

        assert_eq!(metadata_duplicates.len(), 1);
        assert_eq!(metadata_duplicates[0].metadata_doc, "rare dragon gold");
    }
}
