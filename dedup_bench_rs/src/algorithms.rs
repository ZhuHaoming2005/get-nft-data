use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashSet};

use once_cell::sync::Lazy;
use rayon::prelude::*;
use regex::Regex;
use serde::Serialize;
use strsim::{jaro_winkler, normalized_levenshtein};
use top_contract_analysis_rs::analysis::scoring::{score_metadata_document_pair, score_name_pair};
use top_contract_analysis_rs::normalize::{normalize_name, normalize_text};

use crate::sample::BenchmarkSample;
use crate::store::FeatureRow;

static TOKEN_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"[\p{L}\p{N}_]+").unwrap());

pub const DEFAULT_NAME_THRESHOLD: f64 = 95.0;
pub const DEFAULT_METADATA_THRESHOLD: f64 = 0.55;

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AlgorithmField {
    Name,
    Metadata,
    Reference,
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
pub struct ReferenceCandidateScore {
    pub rank: usize,
    pub contract_address: String,
    pub token_id: String,
    pub name: String,
    pub combined_score: f64,
    pub name_score: f64,
    pub metadata_score: f64,
    pub match_reasons: Vec<String>,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct AlgorithmReport {
    pub algorithm_id: String,
    pub field: AlgorithmField,
    pub repeat: usize,
    pub runs_ms: Vec<f64>,
    pub avg_ms: f64,
    pub min_ms: f64,
    pub candidate_count: usize,
    pub top_candidates: Vec<CandidateScore>,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct ReferenceReport {
    pub algorithm_id: String,
    pub field: AlgorithmField,
    pub repeat: usize,
    pub runs_ms: Vec<f64>,
    pub avg_ms: f64,
    pub min_ms: f64,
    pub candidate_count: usize,
    pub top_candidates: Vec<ReferenceCandidateScore>,
}

#[derive(Clone, Copy)]
pub struct AlgorithmDefinition {
    pub id: &'static str,
    pub field: AlgorithmField,
    pub scorer: fn(&PreparedSample, &PreparedFeatureRow) -> f64,
}

#[derive(Clone, Copy)]
pub struct TimingAlgorithmDefinition {
    pub id: &'static str,
    pub field: AlgorithmField,
    pub scorer: fn(&BenchmarkSample, &FeatureRow) -> f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PreparedText {
    pub normalized: String,
    pub trigram_set: BTreeSet<String>,
    pub token_set: HashSet<String>,
    pub token_counts: BTreeMap<String, f64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PreparedSample {
    pub raw: BenchmarkSample,
    pub name_prefix: Option<String>,
    pub name_text: PreparedText,
    pub metadata_text: PreparedText,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PreparedFeatureRow {
    pub raw: FeatureRow,
    pub name_text: PreparedText,
    pub metadata_text: PreparedText,
}

pub fn name_algorithms() -> Vec<AlgorithmDefinition> {
    vec![
        AlgorithmDefinition {
            id: "name_exact_normalized",
            field: AlgorithmField::Name,
            scorer: score_name_exact_normalized,
        },
        AlgorithmDefinition {
            id: "name_jaro_winkler",
            field: AlgorithmField::Name,
            scorer: score_name_jaro_winkler,
        },
        AlgorithmDefinition {
            id: "name_normalized_levenshtein",
            field: AlgorithmField::Name,
            scorer: score_name_normalized_levenshtein,
        },
        AlgorithmDefinition {
            id: "name_trigram_jaccard",
            field: AlgorithmField::Name,
            scorer: score_name_trigram_jaccard,
        },
        AlgorithmDefinition {
            id: "name_current_hybrid",
            field: AlgorithmField::Name,
            scorer: score_name_current_hybrid,
        },
    ]
}

pub fn metadata_algorithms() -> Vec<AlgorithmDefinition> {
    vec![
        AlgorithmDefinition {
            id: "metadata_token_jaccard",
            field: AlgorithmField::Metadata,
            scorer: score_metadata_token_jaccard,
        },
        AlgorithmDefinition {
            id: "metadata_jaro_winkler_doc",
            field: AlgorithmField::Metadata,
            scorer: score_metadata_jaro_winkler_doc,
        },
        AlgorithmDefinition {
            id: "metadata_trigram_jaccard_doc",
            field: AlgorithmField::Metadata,
            scorer: score_metadata_trigram_jaccard_doc,
        },
        AlgorithmDefinition {
            id: "metadata_token_cosine",
            field: AlgorithmField::Metadata,
            scorer: score_metadata_token_cosine,
        },
        AlgorithmDefinition {
            id: "metadata_current_hybrid",
            field: AlgorithmField::Metadata,
            scorer: score_metadata_current_hybrid,
        },
    ]
}

pub fn all_algorithms() -> Vec<AlgorithmDefinition> {
    let mut algorithms = name_algorithms();
    algorithms.extend(metadata_algorithms());
    algorithms
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
            id: "name_normalized_levenshtein",
            field: AlgorithmField::Name,
            scorer: score_name_normalized_levenshtein_raw,
        },
        TimingAlgorithmDefinition {
            id: "name_trigram_jaccard",
            field: AlgorithmField::Name,
            scorer: score_name_trigram_jaccard_raw,
        },
        TimingAlgorithmDefinition {
            id: "name_current_hybrid",
            field: AlgorithmField::Name,
            scorer: score_name_current_hybrid_raw,
        },
        TimingAlgorithmDefinition {
            id: "metadata_token_jaccard",
            field: AlgorithmField::Metadata,
            scorer: score_metadata_token_jaccard_raw,
        },
        TimingAlgorithmDefinition {
            id: "metadata_jaro_winkler_doc",
            field: AlgorithmField::Metadata,
            scorer: score_metadata_jaro_winkler_doc_raw,
        },
        TimingAlgorithmDefinition {
            id: "metadata_trigram_jaccard_doc",
            field: AlgorithmField::Metadata,
            scorer: score_metadata_trigram_jaccard_doc_raw,
        },
        TimingAlgorithmDefinition {
            id: "metadata_token_cosine",
            field: AlgorithmField::Metadata,
            scorer: score_metadata_token_cosine_raw,
        },
        TimingAlgorithmDefinition {
            id: "metadata_current_hybrid",
            field: AlgorithmField::Metadata,
            scorer: score_metadata_current_hybrid_raw,
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

fn trigram_set(document: &str) -> BTreeSet<String> {
    let chars: Vec<char> = document.chars().collect();
    if chars.is_empty() {
        return BTreeSet::new();
    }
    if chars.len() < 3 {
        return BTreeSet::from([document.to_string()]);
    }
    chars
        .windows(3)
        .map(|window| window.iter().collect::<String>())
        .collect()
}

fn trigram_jaccard(left: &str, right: &str) -> f64 {
    let left = normalize_text(left);
    let right = normalize_text(right);
    if left.is_empty() || right.is_empty() {
        return 0.0;
    }
    let left_set = trigram_set(&left);
    let right_set = trigram_set(&right);
    trigram_jaccard_from_sets(&left_set, &right_set)
}

fn trigram_jaccard_from_sets(left: &BTreeSet<String>, right: &BTreeSet<String>) -> f64 {
    let union = left.union(right).count();
    let overlap = left.intersection(right).count();
    if union == 0 {
        0.0
    } else {
        overlap as f64 / union as f64
    }
}

fn token_jaccard(left: &str, right: &str) -> f64 {
    let left_tokens: HashSet<String> = tokenize(&normalize_text(left)).into_iter().collect();
    let right_tokens: HashSet<String> = tokenize(&normalize_text(right)).into_iter().collect();
    token_jaccard_from_sets(&left_tokens, &right_tokens)
}

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
    let right_norm = right.values().map(|value| value * value).sum::<f64>().sqrt();
    if left_norm == 0.0 || right_norm == 0.0 {
        0.0
    } else {
        dot / (left_norm * right_norm)
    }
}

fn token_counts(document: &str) -> BTreeMap<String, f64> {
    let mut counts = BTreeMap::<String, f64>::new();
    for token in tokenize(document) {
        *counts.entry(token).or_insert(0.0) += 1.0;
    }
    counts
}

fn prepare_text(normalized: String) -> PreparedText {
    PreparedText {
        trigram_set: trigram_set(&normalized),
        token_set: tokenize(&normalized).into_iter().collect(),
        token_counts: token_counts(&normalized),
        normalized,
    }
}

pub fn prepare_sample(sample: BenchmarkSample) -> PreparedSample {
    PreparedSample {
        name_prefix: if sample.name_norm.is_empty() {
            None
        } else {
            Some(sample.name_norm.chars().take(8).collect())
        },
        name_text: prepare_text(sample.name_norm.clone()),
        metadata_text: prepare_text(normalize_text(&sample.metadata_doc)),
        raw: sample,
    }
}

pub fn prepare_rows(rows: Vec<FeatureRow>) -> Vec<PreparedFeatureRow> {
    rows.into_iter()
        .map(|row| PreparedFeatureRow {
            name_text: prepare_text(row.name_norm.clone()),
            metadata_text: prepare_text(normalize_text(&row.metadata_doc)),
            raw: row,
        })
        .collect()
}

pub fn score_name_exact_normalized(sample: &PreparedSample, row: &PreparedFeatureRow) -> f64 {
    if sample.raw.name_norm.is_empty() || row.raw.name_norm.is_empty() {
        0.0
    } else if sample.raw.name_norm == row.raw.name_norm {
        100.0
    } else {
        0.0
    }
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

pub fn score_name_jaro_winkler(sample: &PreparedSample, row: &PreparedFeatureRow) -> f64 {
    if sample.name_text.normalized.is_empty() || row.name_text.normalized.is_empty() {
        0.0
    } else {
        jaro_winkler(&sample.name_text.normalized, &row.name_text.normalized) * 100.0
    }
}

pub fn score_name_jaro_winkler_raw(sample: &BenchmarkSample, row: &FeatureRow) -> f64 {
    if sample.name_norm.is_empty() || row.name_norm.is_empty() {
        0.0
    } else {
        jaro_winkler(&sample.name_norm, &row.name_norm) * 100.0
    }
}

pub fn score_name_normalized_levenshtein(
    sample: &PreparedSample,
    row: &PreparedFeatureRow,
) -> f64 {
    if sample.name_text.normalized.is_empty() || row.name_text.normalized.is_empty() {
        0.0
    } else {
        normalized_levenshtein(&sample.name_text.normalized, &row.name_text.normalized) * 100.0
    }
}

pub fn score_name_normalized_levenshtein_raw(sample: &BenchmarkSample, row: &FeatureRow) -> f64 {
    if sample.name_norm.is_empty() || row.name_norm.is_empty() {
        0.0
    } else {
        normalized_levenshtein(&sample.name_norm, &row.name_norm) * 100.0
    }
}

pub fn score_name_trigram_jaccard(sample: &PreparedSample, row: &PreparedFeatureRow) -> f64 {
    trigram_jaccard_from_sets(&sample.name_text.trigram_set, &row.name_text.trigram_set) * 100.0
}

pub fn score_name_trigram_jaccard_raw(sample: &BenchmarkSample, row: &FeatureRow) -> f64 {
    trigram_jaccard(&sample.name_norm, &row.name_norm) * 100.0
}

pub fn score_name_current_hybrid(sample: &PreparedSample, row: &PreparedFeatureRow) -> f64 {
    score_name_pair(&sample.raw.name, &row.raw.name)
}

pub fn score_name_current_hybrid_raw(sample: &BenchmarkSample, row: &FeatureRow) -> f64 {
    score_name_pair(&sample.name, &row.name)
}

pub fn score_metadata_token_jaccard(sample: &PreparedSample, row: &PreparedFeatureRow) -> f64 {
    token_jaccard_from_sets(&sample.metadata_text.token_set, &row.metadata_text.token_set)
}

pub fn score_metadata_token_jaccard_raw(sample: &BenchmarkSample, row: &FeatureRow) -> f64 {
    token_jaccard(&sample.metadata_doc, &row.metadata_doc)
}

pub fn score_metadata_jaro_winkler_doc(
    sample: &PreparedSample,
    row: &PreparedFeatureRow,
) -> f64 {
    if sample.metadata_text.normalized.is_empty() || row.metadata_text.normalized.is_empty() {
        0.0
    } else {
        jaro_winkler(&sample.metadata_text.normalized, &row.metadata_text.normalized)
    }
}

pub fn score_metadata_jaro_winkler_doc_raw(sample: &BenchmarkSample, row: &FeatureRow) -> f64 {
    let left = normalize_text(&sample.metadata_doc);
    let right = normalize_text(&row.metadata_doc);
    if left.is_empty() || right.is_empty() {
        0.0
    } else {
        jaro_winkler(&left, &right)
    }
}

pub fn score_metadata_trigram_jaccard_doc(
    sample: &PreparedSample,
    row: &PreparedFeatureRow,
) -> f64 {
    trigram_jaccard_from_sets(
        &sample.metadata_text.trigram_set,
        &row.metadata_text.trigram_set,
    )
}

pub fn score_metadata_trigram_jaccard_doc_raw(
    sample: &BenchmarkSample,
    row: &FeatureRow,
) -> f64 {
    trigram_jaccard(&sample.metadata_doc, &row.metadata_doc)
}

pub fn score_metadata_token_cosine(sample: &PreparedSample, row: &PreparedFeatureRow) -> f64 {
    token_cosine_from_counts(&sample.metadata_text.token_counts, &row.metadata_text.token_counts)
}

pub fn score_metadata_token_cosine_raw(sample: &BenchmarkSample, row: &FeatureRow) -> f64 {
    token_cosine(&sample.metadata_doc, &row.metadata_doc)
}

pub fn score_metadata_current_hybrid(sample: &PreparedSample, row: &PreparedFeatureRow) -> f64 {
    score_metadata_document_pair(&sample.raw.metadata_doc, &row.raw.metadata_doc)
}

pub fn score_metadata_current_hybrid_raw(sample: &BenchmarkSample, row: &FeatureRow) -> f64 {
    score_metadata_document_pair(&sample.metadata_doc, &row.metadata_doc)
}

pub fn sort_algorithm_candidates(
    rows: &[PreparedFeatureRow],
    scored: &[f64],
    top_k: usize,
) -> (usize, Vec<CandidateScore>) {
    let mut candidates: Vec<(usize, f64)> = scored
        .iter()
        .enumerate()
        .filter_map(|(index, score)| (*score > 0.0).then_some((index, *score)))
        .collect();
    candidates.sort_by(|left, right| {
        right
            .1
            .partial_cmp(&left.1)
            .unwrap_or(Ordering::Equal)
            .then_with(|| {
                rows[left.0]
                    .raw
                    .contract_address
                    .cmp(&rows[right.0].raw.contract_address)
            })
            .then_with(|| rows[left.0].raw.token_id.cmp(&rows[right.0].raw.token_id))
    });
    let total = candidates.len();
    let top_candidates = candidates
        .into_iter()
        .take(top_k)
        .enumerate()
        .map(|(rank, (index, score))| CandidateScore {
            rank: rank + 1,
            contract_address: rows[index].raw.contract_address.clone(),
            token_id: rows[index].raw.token_id.clone(),
            name: rows[index].raw.name.clone(),
            score,
        })
        .collect();
    (total, top_candidates)
}

pub fn sort_algorithm_candidates_raw(
    rows: &[FeatureRow],
    scored: &[f64],
    top_k: usize,
) -> (usize, Vec<CandidateScore>) {
    let mut candidates: Vec<(usize, f64)> = scored
        .iter()
        .enumerate()
        .filter_map(|(index, score)| (*score > 0.0).then_some((index, *score)))
        .collect();
    candidates.sort_by(|left, right| {
        right
            .1
            .partial_cmp(&left.1)
            .unwrap_or(Ordering::Equal)
            .then_with(|| rows[left.0].contract_address.cmp(&rows[right.0].contract_address))
            .then_with(|| rows[left.0].token_id.cmp(&rows[right.0].token_id))
    });
    let total = candidates.len();
    let top_candidates = candidates
        .into_iter()
        .take(top_k)
        .enumerate()
        .map(|(rank, (index, score))| CandidateScore {
            rank: rank + 1,
            contract_address: rows[index].contract_address.clone(),
            token_id: rows[index].token_id.clone(),
            name: rows[index].name.clone(),
            score,
        })
        .collect();
    (total, top_candidates)
}

pub fn score_rows_parallel(
    sample: &PreparedSample,
    rows: &[PreparedFeatureRow],
    scorer: fn(&PreparedSample, &PreparedFeatureRow) -> f64,
) -> Vec<f64> {
    rows.par_iter().map(|row| scorer(sample, row)).collect()
}

pub fn score_rows_parallel_raw(
    sample: &BenchmarkSample,
    rows: &[FeatureRow],
    scorer: fn(&BenchmarkSample, &FeatureRow) -> f64,
) -> Vec<f64> {
    rows.par_iter().map(|row| scorer(sample, row)).collect()
}

pub fn build_reference_candidates_from_scores(
    rows: &[PreparedFeatureRow],
    name_scores: &[f64],
    metadata_scores: &[f64],
    top_k: usize,
) -> (usize, Vec<ReferenceCandidateScore>) {
    debug_assert_eq!(rows.len(), name_scores.len());
    debug_assert_eq!(rows.len(), metadata_scores.len());
    let mut candidates: Vec<(&PreparedFeatureRow, f64, f64, f64, Vec<String>)> = rows
        .iter()
        .zip(name_scores.iter().copied())
        .zip(metadata_scores.iter().copied())
        .filter_map(|((row, name_score), metadata_score)| {
            let mut reasons = Vec::new();
            if name_score >= DEFAULT_NAME_THRESHOLD {
                reasons.push("name_match".to_string());
            }
            if metadata_score >= DEFAULT_METADATA_THRESHOLD {
                reasons.push("metadata_match".to_string());
            }
            if reasons.is_empty() {
                return None;
            }
            let combined_score = (name_score / 100.0).max(metadata_score);
            Some((row, combined_score, name_score, metadata_score, reasons))
        })
        .collect();
    candidates.sort_by(|left, right| {
        right
            .1
            .partial_cmp(&left.1)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.0.raw.contract_address.cmp(&right.0.raw.contract_address))
            .then_with(|| left.0.raw.token_id.cmp(&right.0.raw.token_id))
    });
    let total = candidates.len();
    let top_candidates = candidates
        .into_iter()
        .take(top_k)
        .enumerate()
        .map(
            |(rank, (row, combined_score, name_score, metadata_score, match_reasons))| {
                ReferenceCandidateScore {
                    rank: rank + 1,
                    contract_address: row.raw.contract_address.clone(),
                    token_id: row.raw.token_id.clone(),
                    name: row.raw.name.clone(),
                    combined_score,
                    name_score,
                    metadata_score,
                    match_reasons,
                }
            },
        )
        .collect();
    (total, top_candidates)
}

pub fn build_reference_candidates(
    sample: &PreparedSample,
    rows: &[PreparedFeatureRow],
    top_k: usize,
) -> (usize, Vec<ReferenceCandidateScore>) {
    let name_scores = score_rows_parallel(sample, rows, score_name_current_hybrid);
    let metadata_scores = score_rows_parallel(sample, rows, score_metadata_current_hybrid);
    build_reference_candidates_from_scores(rows, &name_scores, &metadata_scores, top_k)
}

pub fn build_reference_candidates_raw(
    sample: &BenchmarkSample,
    rows: &[FeatureRow],
    top_k: usize,
) -> (usize, Vec<ReferenceCandidateScore>) {
    let mut candidates = Vec::new();
    for row in rows {
        let name_score = score_name_current_hybrid_raw(sample, row);
        let metadata_score = score_metadata_current_hybrid_raw(sample, row);
        let mut reasons = Vec::new();
        if name_score >= DEFAULT_NAME_THRESHOLD {
            reasons.push("name_match".to_string());
        }
        if metadata_score >= DEFAULT_METADATA_THRESHOLD {
            reasons.push("metadata_match".to_string());
        }
        if reasons.is_empty() {
            continue;
        }
        let combined_score = (name_score / 100.0).max(metadata_score);
        candidates.push((row, combined_score, name_score, metadata_score, reasons));
    }
    candidates.sort_by(|left, right| {
        right
            .1
            .partial_cmp(&left.1)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.0.contract_address.cmp(&right.0.contract_address))
            .then_with(|| left.0.token_id.cmp(&right.0.token_id))
    });
    let total = candidates.len();
    let top_candidates = candidates
        .into_iter()
        .take(top_k)
        .enumerate()
        .map(
            |(rank, (row, combined_score, name_score, metadata_score, match_reasons))| {
                ReferenceCandidateScore {
                    rank: rank + 1,
                    contract_address: row.contract_address.clone(),
                    token_id: row.token_id.clone(),
                    name: row.name.clone(),
                    combined_score,
                    name_score,
                    metadata_score,
                    match_reasons,
                }
            },
        )
        .collect();
    (total, top_candidates)
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
            name: "Azuki #1".into(),
            name_norm: normalize_name("Azuki #1"),
            metadata_json: "{\"description\":\"gold dragon rare\"}".into(),
            metadata_doc: "gold dragon rare".into(),
            metadata_keywords: vec!["dragon".into(), "gold".into(), "rare".into()],
        }
    }

    fn sample() -> PreparedSample {
        prepare_sample(sample_raw())
    }

    fn row_raw() -> FeatureRow {
        FeatureRow {
            contract_address: "0xdup".into(),
            token_id: "1".into(),
            name: "Azuki".into(),
            name_norm: normalize_name("Azuki"),
            metadata_doc: "rare dragon gold".into(),
            metadata_keywords: vec!["dragon".into(), "gold".into(), "rare".into()],
        }
    }

    fn row() -> PreparedFeatureRow {
        prepare_rows(vec![row_raw()]).pop().unwrap()
    }

    fn second_row_raw() -> FeatureRow {
        FeatureRow {
            contract_address: "0xdup2".into(),
            token_id: "2".into(),
            name: "Azuki Mirror #2".into(),
            name_norm: normalize_name("Azuki Mirror #2"),
            metadata_doc: "rare dragon silver".into(),
            metadata_keywords: vec!["dragon".into(), "rare".into(), "silver".into()],
        }
    }

    fn second_row() -> PreparedFeatureRow {
        prepare_rows(vec![second_row_raw()]).pop().unwrap()
    }

    #[test]
    fn current_name_hybrid_matches_existing_logic() {
        let sample = sample();
        let row = row();
        assert_eq!(
            score_name_current_hybrid(&sample, &row),
            score_name_pair(&sample.raw.name, &row.raw.name)
        );
    }

    #[test]
    fn current_metadata_hybrid_matches_existing_logic() {
        let sample = sample();
        let row = row();
        assert_eq!(
            score_metadata_current_hybrid(&sample, &row),
            score_metadata_document_pair(&sample.raw.metadata_doc, &row.raw.metadata_doc)
        );
    }

    #[test]
    fn trigram_jaccard_is_stable_for_identical_and_disjoint_inputs() {
        assert_eq!(trigram_jaccard("abc", "abc"), 1.0);
        assert_eq!(trigram_jaccard("abc", "xyz"), 0.0);
    }

    #[test]
    fn token_cosine_handles_repeated_tokens() {
        let score = token_cosine("gold gold dragon", "gold dragon");
        assert!(score > 0.9);
        assert!(score <= 1.0);
    }

    #[test]
    fn parallel_row_scoring_matches_sequential_scoring() {
        let sample = sample();
        let rows = vec![row(), second_row()];
        let sequential: Vec<f64> = rows
            .iter()
            .map(|row| score_name_current_hybrid(&sample, row))
            .collect();
        let parallel = score_rows_parallel(&sample, &rows, score_name_current_hybrid);

        assert_eq!(parallel, sequential);
        assert_eq!(
            sort_algorithm_candidates(&rows, &parallel, 10),
            sort_algorithm_candidates(&rows, &sequential, 10)
        );
    }

    #[test]
    fn parallel_reference_candidates_keep_same_ranking_rules() {
        let sample = sample();
        let rows = vec![row(), second_row()];
        let actual = build_reference_candidates(&sample, &rows, 10);

        let mut expected_candidates = Vec::new();
        for row in &rows {
            let name_score = score_name_current_hybrid(&sample, row);
            let metadata_score = score_metadata_current_hybrid(&sample, row);
            let mut reasons = Vec::new();
            if name_score >= DEFAULT_NAME_THRESHOLD {
                reasons.push("name_match".to_string());
            }
            if metadata_score >= DEFAULT_METADATA_THRESHOLD {
                reasons.push("metadata_match".to_string());
            }
            if reasons.is_empty() {
                continue;
            }
            let combined_score = (name_score / 100.0).max(metadata_score);
            expected_candidates.push((row, combined_score, name_score, metadata_score, reasons));
        }
        expected_candidates.sort_by(|left, right| {
            right
                .1
                .partial_cmp(&left.1)
                .unwrap_or(Ordering::Equal)
                .then_with(|| left.0.raw.contract_address.cmp(&right.0.raw.contract_address))
                .then_with(|| left.0.raw.token_id.cmp(&right.0.raw.token_id))
        });
        let expected = (
            expected_candidates.len(),
            expected_candidates
                .into_iter()
                .enumerate()
                .map(
                    |(rank, (row, combined_score, name_score, metadata_score, match_reasons))| {
                        ReferenceCandidateScore {
                            rank: rank + 1,
                            contract_address: row.raw.contract_address.clone(),
                            token_id: row.raw.token_id.clone(),
                            name: row.raw.name.clone(),
                            combined_score,
                            name_score,
                            metadata_score,
                            match_reasons,
                        }
                    },
                )
                .collect(),
        );

        assert_eq!(actual, expected);
    }

    #[test]
    fn prepared_scoring_matches_unprepared_scoring() {
        let sample_raw = sample_raw();
        let row_raw = row_raw();
        let sample = prepare_sample(sample_raw.clone());
        let row = prepare_rows(vec![row_raw.clone()]).pop().unwrap();

        assert_eq!(
            score_name_jaro_winkler(&sample, &row),
            jaro_winkler(&sample_raw.name_norm, &row_raw.name_norm) * 100.0
        );
        assert_eq!(
            score_name_normalized_levenshtein(&sample, &row),
            normalized_levenshtein(&sample_raw.name_norm, &row_raw.name_norm) * 100.0
        );
        assert_eq!(
            score_metadata_token_jaccard(&sample, &row),
            token_jaccard(&sample_raw.metadata_doc, &row_raw.metadata_doc)
        );
        assert_eq!(
            score_metadata_token_cosine(&sample, &row),
            token_cosine(&sample_raw.metadata_doc, &row_raw.metadata_doc)
        );
    }

    #[test]
    fn reference_candidates_from_precomputed_scores_match_direct_build() {
        let sample = sample();
        let rows = vec![row(), second_row()];
        let name_scores = score_rows_parallel(&sample, &rows, score_name_current_hybrid);
        let metadata_scores = score_rows_parallel(&sample, &rows, score_metadata_current_hybrid);

        assert_eq!(
            build_reference_candidates(&sample, &rows, 10),
            build_reference_candidates_from_scores(&rows, &name_scores, &metadata_scores, 10)
        );
    }
}
