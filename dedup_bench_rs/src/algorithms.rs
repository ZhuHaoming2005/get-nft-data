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
    pub avg_ms: f64,
    pub min_ms: f64,
    pub candidate_count: usize,
    pub top_candidates: Vec<ReferenceCandidateScore>,
}

#[derive(Clone, Copy)]
pub struct AlgorithmDefinition {
    pub id: &'static str,
    pub field: AlgorithmField,
    pub scorer: fn(&BenchmarkSample, &FeatureRow) -> f64,
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
    let union = left_set.union(&right_set).count();
    let overlap = left_set.intersection(&right_set).count();
    if union == 0 {
        0.0
    } else {
        overlap as f64 / union as f64
    }
}

fn token_jaccard(left: &str, right: &str) -> f64 {
    let left_tokens: HashSet<String> = tokenize(&normalize_text(left)).into_iter().collect();
    let right_tokens: HashSet<String> = tokenize(&normalize_text(right)).into_iter().collect();
    if left_tokens.is_empty() || right_tokens.is_empty() {
        return 0.0;
    }
    let union = left_tokens.union(&right_tokens).count();
    let overlap = left_tokens.intersection(&right_tokens).count();
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

    let dot = left_counts
        .iter()
        .map(|(token, left_value)| left_value * right_counts.get(token).unwrap_or(&0.0))
        .sum::<f64>();
    let left_norm = left_counts.values().map(|value| value * value).sum::<f64>().sqrt();
    let right_norm = right_counts
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

pub fn score_name_exact_normalized(sample: &BenchmarkSample, row: &FeatureRow) -> f64 {
    if sample.name_norm.is_empty() || row.name_norm.is_empty() {
        0.0
    } else if sample.name_norm == row.name_norm {
        100.0
    } else {
        0.0
    }
}

pub fn score_name_jaro_winkler(sample: &BenchmarkSample, row: &FeatureRow) -> f64 {
    if sample.name_norm.is_empty() || row.name_norm.is_empty() {
        0.0
    } else {
        jaro_winkler(&sample.name_norm, &row.name_norm) * 100.0
    }
}

pub fn score_name_normalized_levenshtein(sample: &BenchmarkSample, row: &FeatureRow) -> f64 {
    if sample.name_norm.is_empty() || row.name_norm.is_empty() {
        0.0
    } else {
        normalized_levenshtein(&sample.name_norm, &row.name_norm) * 100.0
    }
}

pub fn score_name_trigram_jaccard(sample: &BenchmarkSample, row: &FeatureRow) -> f64 {
    trigram_jaccard(&sample.name_norm, &row.name_norm) * 100.0
}

pub fn score_name_current_hybrid(sample: &BenchmarkSample, row: &FeatureRow) -> f64 {
    score_name_pair(&sample.name, &row.name)
}

pub fn score_metadata_token_jaccard(sample: &BenchmarkSample, row: &FeatureRow) -> f64 {
    token_jaccard(&sample.metadata_doc, &row.metadata_doc)
}

pub fn score_metadata_jaro_winkler_doc(sample: &BenchmarkSample, row: &FeatureRow) -> f64 {
    let left = normalize_text(&sample.metadata_doc);
    let right = normalize_text(&row.metadata_doc);
    if left.is_empty() || right.is_empty() {
        0.0
    } else {
        jaro_winkler(&left, &right)
    }
}

pub fn score_metadata_trigram_jaccard_doc(sample: &BenchmarkSample, row: &FeatureRow) -> f64 {
    trigram_jaccard(&sample.metadata_doc, &row.metadata_doc)
}

pub fn score_metadata_token_cosine(sample: &BenchmarkSample, row: &FeatureRow) -> f64 {
    token_cosine(&sample.metadata_doc, &row.metadata_doc)
}

pub fn score_metadata_current_hybrid(sample: &BenchmarkSample, row: &FeatureRow) -> f64 {
    score_metadata_document_pair(&sample.metadata_doc, &row.metadata_doc)
}

pub fn sort_algorithm_candidates(
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
    sample: &BenchmarkSample,
    rows: &[FeatureRow],
    scorer: fn(&BenchmarkSample, &FeatureRow) -> f64,
) -> Vec<f64> {
    rows.par_iter().map(|row| scorer(sample, row)).collect()
}

pub fn build_reference_candidates(
    sample: &BenchmarkSample,
    rows: &[FeatureRow],
    top_k: usize,
) -> (usize, Vec<ReferenceCandidateScore>) {
    let mut candidates: Vec<(&FeatureRow, f64, f64, f64, Vec<String>)> = rows
        .par_iter()
        .filter_map(|row| {
            let name_score = score_name_current_hybrid(sample, row);
            let metadata_score = score_metadata_current_hybrid(sample, row);
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

    fn sample() -> BenchmarkSample {
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

    fn row() -> FeatureRow {
        FeatureRow {
            contract_address: "0xdup".into(),
            token_id: "1".into(),
            name: "Azuki".into(),
            name_norm: normalize_name("Azuki"),
            metadata_doc: "rare dragon gold".into(),
            metadata_keywords: vec!["dragon".into(), "gold".into(), "rare".into()],
        }
    }

    fn second_row() -> FeatureRow {
        FeatureRow {
            contract_address: "0xdup2".into(),
            token_id: "2".into(),
            name: "Azuki Mirror #2".into(),
            name_norm: normalize_name("Azuki Mirror #2"),
            metadata_doc: "rare dragon silver".into(),
            metadata_keywords: vec!["dragon".into(), "rare".into(), "silver".into()],
        }
    }

    #[test]
    fn current_name_hybrid_matches_existing_logic() {
        let sample = sample();
        let row = row();
        assert_eq!(
            score_name_current_hybrid(&sample, &row),
            score_name_pair(&sample.name, &row.name)
        );
    }

    #[test]
    fn current_metadata_hybrid_matches_existing_logic() {
        let sample = sample();
        let row = row();
        assert_eq!(
            score_metadata_current_hybrid(&sample, &row),
            score_metadata_document_pair(&sample.metadata_doc, &row.metadata_doc)
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
                .then_with(|| left.0.contract_address.cmp(&right.0.contract_address))
                .then_with(|| left.0.token_id.cmp(&right.0.token_id))
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
                .collect(),
        );

        assert_eq!(actual, expected);
    }
}
