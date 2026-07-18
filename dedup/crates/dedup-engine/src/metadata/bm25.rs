use super::RawContentVector;
use dedup_model::{DedupError, ErrorContext, Q16_16};
use std::collections::BTreeMap;

pub const BM25_K1: f64 = 1.2;
pub const BM25_B: f64 = 0.75;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WeightedContentVector {
    pub terms: Vec<(u32, u32)>,
}

#[derive(Clone, Debug)]
pub struct Bm25Corpus {
    term_ids: BTreeMap<Vec<u8>, u32>,
    document_frequency: Vec<u64>,
    document_count: u64,
    average_document_length: f64,
}

impl Bm25Corpus {
    pub fn build(documents: &[RawContentVector]) -> Result<Self, DedupError> {
        Self::build_from_refs(documents.iter())
    }

    pub fn build_from_refs<'a>(
        documents: impl IntoIterator<Item = &'a RawContentVector>,
    ) -> Result<Self, DedupError> {
        let documents: Vec<_> = documents.into_iter().collect();
        if documents.is_empty() {
            return Err(DedupError::InvalidInput {
                context: ErrorContext::stage("bm25"),
                message: "BM25 corpus cannot be empty".to_owned(),
            });
        }
        let mut term_ids = BTreeMap::<Vec<u8>, u32>::new();
        for document in &documents {
            for term in document.term_frequencies.keys() {
                term_ids.entry(term.clone()).or_default();
            }
        }
        for (index, term_id) in term_ids.values_mut().enumerate() {
            *term_id = u32::try_from(index).map_err(|_| DedupError::InvalidInput {
                context: ErrorContext::stage("bm25"),
                message: "term ID capacity exceeded".to_owned(),
            })?;
        }
        let mut document_frequency = vec![0_u64; term_ids.len()];
        let mut total_document_length = 0_u64;
        for document in &documents {
            total_document_length = total_document_length
                .checked_add(u64::from(document.document_length))
                .ok_or(DedupError::CounterOverflow {
                    counter: "bm25_total_document_length",
                })?;
            for term in document.term_frequencies.keys() {
                let index = usize::try_from(term_ids[term]).map_err(|_| {
                    DedupError::InvariantViolation {
                        context: ErrorContext::stage("bm25"),
                        message: "term ID does not fit usize".to_owned(),
                    }
                })?;
                document_frequency[index] = document_frequency[index].checked_add(1).ok_or(
                    DedupError::CounterOverflow {
                        counter: "bm25_document_frequency",
                    },
                )?;
            }
        }
        let document_count =
            u64::try_from(documents.len()).map_err(|_| DedupError::CounterOverflow {
                counter: "bm25_document_count",
            })?;
        Ok(Self {
            term_ids,
            document_frequency,
            document_count,
            average_document_length: total_document_length as f64 / document_count as f64,
        })
    }

    pub fn weight(&self, document: &RawContentVector) -> Result<WeightedContentVector, DedupError> {
        let mut terms = Vec::with_capacity(document.term_frequencies.len());
        for (term, frequency) in &document.term_frequencies {
            let term_id = self.term_ids[term];
            let document_frequency =
                self.document_frequency[usize::try_from(term_id).map_err(|_| {
                    DedupError::InvariantViolation {
                        context: ErrorContext::stage("bm25"),
                        message: "term ID does not fit usize".to_owned(),
                    }
                })?];
            let idf = (1.0
                + (self.document_count as f64 - document_frequency as f64 + 0.5)
                    / (document_frequency as f64 + 0.5))
                .ln();
            let tf = f64::from(*frequency);
            let length_ratio =
                f64::from(document.document_length) / self.average_document_length.max(1.0);
            let weight = idf * (tf * (BM25_K1 + 1.0))
                / (tf + BM25_K1 * (1.0 - BM25_B + BM25_B * length_ratio));
            let quantized = Q16_16::from_unit((weight / (1.0 + weight)).clamp(0.0, 1.0))
                .ok_or_else(|| DedupError::InvariantViolation {
                    context: ErrorContext::stage("bm25"),
                    message: "BM25 weight is not finite".to_owned(),
                })?;
            terms.push((term_id, quantized.raw()));
        }
        Ok(WeightedContentVector { terms })
    }
}

pub fn cosine_similarity(
    left: &WeightedContentVector,
    right: &WeightedContentVector,
) -> Result<(f64, u64), DedupError> {
    let mut left_index = 0;
    let mut right_index = 0;
    let mut dot = 0_u128;
    let mut left_norm = 0_u128;
    let mut right_norm = 0_u128;
    let mut comparisons = 0_u64;
    for (_, weight) in &left.terms {
        left_norm = checked_square_add(left_norm, *weight)?;
    }
    for (_, weight) in &right.terms {
        right_norm = checked_square_add(right_norm, *weight)?;
    }
    while left_index < left.terms.len() && right_index < right.terms.len() {
        comparisons = comparisons
            .checked_add(1)
            .ok_or(DedupError::CounterOverflow {
                counter: "bm25_term_comparisons",
            })?;
        let left_term = left.terms[left_index];
        let right_term = right.terms[right_index];
        match left_term.0.cmp(&right_term.0) {
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
            std::cmp::Ordering::Equal => {
                dot = dot
                    .checked_add(u128::from(left_term.1) * u128::from(right_term.1))
                    .ok_or_else(|| DedupError::InvariantViolation {
                        context: ErrorContext::stage("bm25"),
                        message: "BM25 dot product overflow".to_owned(),
                    })?;
                left_index += 1;
                right_index += 1;
            }
        }
    }
    if left_norm == 0 || right_norm == 0 {
        return Ok((0.0, comparisons));
    }
    let similarity = dot as f64 / ((left_norm as f64) * (right_norm as f64)).sqrt();
    Ok((similarity, comparisons))
}

fn checked_square_add(total: u128, weight: u32) -> Result<u128, DedupError> {
    total
        .checked_add(u128::from(weight) * u128::from(weight))
        .ok_or_else(|| DedupError::InvariantViolation {
            context: ErrorContext::stage("bm25"),
            message: "BM25 norm overflow".to_owned(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{canonicalize_json, vectorize_content};

    #[test]
    fn identical_vectors_have_unit_similarity() {
        let raw = vectorize_content(&canonicalize_json(r#"{"name":"one"}"#).unwrap()).unwrap();
        let corpus = Bm25Corpus::build(&[raw.clone(), raw.clone()]).unwrap();
        let vector = corpus.weight(&raw).unwrap();
        let (score, comparisons) = cosine_similarity(&vector, &vector).unwrap();
        assert!((score - 1.0).abs() < 1e-12);
        assert!(comparisons > 0);
    }
}
