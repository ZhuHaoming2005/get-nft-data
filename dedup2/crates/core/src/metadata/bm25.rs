const K1: f64 = 1.2;
const B: f64 = 0.75;
const IDF_ONCE: f64 = std::f64::consts::LN_2;
const IDF_SHARED: f64 = 0.182_321_556_793_954_6;
const IDF_RATIO_SQUARED: f64 = (IDF_SHARED / IDF_ONCE) * (IDF_SHARED / IDF_ONCE);
const UPPER_BOUND_EPSILON: f64 = 1e-12;
const INLINE_FREQUENCIES: usize = 4;
const MIN_LENGTH_NORM: f64 = K1 * (1.0 - B);
const MAX_LENGTH_NORM: f64 = K1 * (1.0 - B + 2.0 * B);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ThresholdDecision {
    pub matched: bool,
    pub zero_overlap_pruned: bool,
    pub upper_bound_pruned: bool,
}

#[derive(Clone, Debug)]
pub struct PreparedDocument {
    term_start: u32,
    term_len: u32,
    frequency_histogram: FrequencyHistogram,
    term_mask: [u64; 2],
    length: u32,
}

#[cfg(test)]
pub(crate) struct PreparedDocumentParts {
    pub document: PreparedDocument,
    pub terms: Vec<(u32, u32)>,
}

#[derive(Clone, Debug)]
enum FrequencyHistogram {
    Inline {
        len: u8,
        values: [(u32, u32); INLINE_FREQUENCIES],
    },
    Heap(Box<[(u32, u32)]>),
}

impl FrequencyHistogram {
    fn from_terms(terms: &[(u32, u32)]) -> Self {
        let mut inline = [(0_u32, 0_u32); INLINE_FREQUENCIES];
        let mut inline_len = 0;
        let mut heap = None::<Vec<(u32, u32)>>;
        for &(_, frequency) in terms {
            if let Some(values) = &mut heap {
                if let Some((_, count)) = values
                    .iter_mut()
                    .find(|(candidate, _)| *candidate == frequency)
                {
                    *count += 1;
                } else {
                    values.push((frequency, 1));
                }
                continue;
            }
            if let Some((_, count)) = inline[..inline_len]
                .iter_mut()
                .find(|(candidate, _)| *candidate == frequency)
            {
                *count += 1;
            } else if inline_len < INLINE_FREQUENCIES {
                inline[inline_len] = (frequency, 1);
                inline_len += 1;
            } else {
                let mut values = inline.to_vec();
                values.push((frequency, 1));
                heap = Some(values);
            }
        }
        if let Some(mut values) = heap {
            values.sort_unstable_by_key(|(frequency, _)| *frequency);
            Self::Heap(values.into_boxed_slice())
        } else {
            inline[..inline_len].sort_unstable_by_key(|(frequency, _)| *frequency);
            Self::Inline {
                len: inline_len as u8,
                values: inline,
            }
        }
    }

    fn as_slice(&self) -> &[(u32, u32)] {
        match self {
            Self::Inline { len, values } => &values[..usize::from(*len)],
            Self::Heap(values) => values,
        }
    }
}

impl PreparedDocument {
    #[cfg(test)]
    pub fn try_new<'a, E>(
        canonical: &'a str,
        mut intern: impl FnMut(&'a str) -> Result<u32, E>,
    ) -> Result<PreparedDocumentParts, E> {
        let mut scratch = Vec::new();
        let mut terms = Vec::new();
        let document = Self::try_new_into(canonical, &mut intern, &mut scratch, &mut terms)?;
        Ok(PreparedDocumentParts { document, terms })
    }

    pub(crate) fn try_new_into<'a, E>(
        canonical: &'a str,
        mut intern: impl FnMut(&'a str) -> Result<u32, E>,
        scratch: &mut Vec<u32>,
        terms: &mut Vec<(u32, u32)>,
    ) -> Result<Self, E> {
        scratch.clear();
        if canonical.is_ascii() {
            let bytes = canonical.as_bytes();
            let mut start = 0;
            while start < bytes.len() {
                while start < bytes.len() && !bytes[start].is_ascii_alphanumeric() {
                    start += 1;
                }
                let mut end = start;
                while end < bytes.len() && bytes[end].is_ascii_alphanumeric() {
                    end += 1;
                }
                if start < end {
                    scratch.push(intern(&canonical[start..end])?);
                }
                start = end;
            }
        } else {
            for token in canonical
                .split(|character: char| !character.is_alphanumeric())
                .filter(|token| !token.is_empty())
            {
                scratch.push(intern(token)?);
            }
        }
        scratch.sort_unstable();
        let length = scratch.len() as u32;
        let term_start = terms.len();
        let mut term_mask = [0_u64; 2];
        let mut index = 0;
        while index < scratch.len() {
            let mut end = index + 1;
            while end < scratch.len() && scratch[end] == scratch[index] {
                end += 1;
            }
            terms.push((scratch[index], (end - index) as u32));
            let mixed = u64::from(scratch[index]).wrapping_mul(0x9e37_79b9_7f4a_7c15);
            let bit = (mixed >> 57) as usize;
            term_mask[bit / 64] |= 1_u64 << (bit % 64);
            index = end;
        }
        let compact_terms = &terms[term_start..];
        let frequency_histogram = FrequencyHistogram::from_terms(compact_terms);
        Ok(Self {
            term_start: 0,
            term_len: compact_terms.len() as u32,
            frequency_histogram,
            term_mask,
            length,
        })
    }

    pub(crate) fn set_term_start(&mut self, start: u32) {
        self.term_start = start;
    }

    pub(crate) fn terms<'a>(&self, terms: &'a [(u32, u32)]) -> &'a [(u32, u32)] {
        let start = self.term_start as usize;
        &terms[start..start + self.term_len as usize]
    }

    #[cfg(test)]
    pub(crate) fn term_range(&self) -> std::ops::Range<usize> {
        let start = self.term_start as usize;
        start..start + self.term_len as usize
    }
}

pub(crate) fn lossless_prefix_len(frequencies_in_rarity_order: &[u32], threshold: f64) -> usize {
    if frequencies_in_rarity_order.is_empty() || threshold.is_nan() || threshold > 1.0 {
        return 0;
    }
    if threshold <= 0.0 {
        return frequencies_in_rarity_order.len();
    }
    let rounding_margin = 64.0 * f64::EPSILON * frequencies_in_rarity_order.len() as f64;

    let mut suffix_shared_max = frequencies_in_rarity_order
        .iter()
        .map(|&frequency| {
            let weighted = weight(frequency, MIN_LENGTH_NORM, IDF_SHARED);
            weighted * weighted
        })
        .sum::<f64>();
    let mut prefix_unique_min = 0.0;
    for (index, &frequency) in frequencies_in_rarity_order.iter().enumerate() {
        let shared = weight(frequency, MIN_LENGTH_NORM, IDF_SHARED);
        suffix_shared_max = (suffix_shared_max - shared * shared).max(0.0);
        let unique = weight(frequency, MAX_LENGTH_NORM, IDF_ONCE);
        prefix_unique_min += unique * unique;
        let denominator = prefix_unique_min + suffix_shared_max;
        let upper_bound = if denominator <= 0.0 {
            0.0
        } else {
            (suffix_shared_max / denominator).sqrt()
        };
        if upper_bound + UPPER_BOUND_EPSILON + rounding_margin < threshold {
            return index + 1;
        }
    }
    frequencies_in_rarity_order.len()
}

#[cfg(test)]
pub fn cosine_similarity(
    left: &PreparedDocument,
    left_terms: &[(u32, u32)],
    right: &PreparedDocument,
    right_terms: &[(u32, u32)],
) -> f64 {
    let weights = PairWeights::new(left, right);
    let mut shared = SharedWeights::default();
    for_each_shared_term(left_terms, right_terms, |left_tf, right_tf| {
        shared.add(weights.left_once(left_tf), weights.right_once(right_tf));
    });
    weights.score(shared)
}

pub fn similarity_at_least(
    left: &PreparedDocument,
    left_terms: &[(u32, u32)],
    right: &PreparedDocument,
    right_terms: &[(u32, u32)],
    threshold: f64,
) -> ThresholdDecision {
    if threshold <= 0.0 {
        return ThresholdDecision {
            matched: true,
            zero_overlap_pruned: false,
            upper_bound_pruned: false,
        };
    }
    if !may_share_term(left, left_terms, right, right_terms) {
        return ThresholdDecision {
            matched: false,
            zero_overlap_pruned: true,
            upper_bound_pruned: false,
        };
    }
    let weights = PairWeights::new(left, right);
    if left_terms.len().saturating_mul(4) < right_terms.len()
        || right_terms.len().saturating_mul(4) < left_terms.len()
    {
        let mut shared = SharedWeights::default();
        for_each_shared_term(left_terms, right_terms, |left_tf, right_tf| {
            shared.add(weights.left_once(left_tf), weights.right_once(right_tf));
        });
        return decision(weights.score(shared), threshold, shared.count == 0, false);
    }

    let mut left_pos = 0;
    let mut right_pos = 0;
    let mut processed_left_norm = 0.0;
    let mut processed_right_norm = 0.0;
    let mut shared = SharedWeights::default();
    let mut comparisons = 0_u32;
    while left_pos < left_terms.len() && right_pos < right_terms.len() {
        let (left_term, left_tf) = &left_terms[left_pos];
        let (right_term, right_tf) = &right_terms[right_pos];
        match left_term.cmp(right_term) {
            std::cmp::Ordering::Equal => {
                let left_once = weights.left_once(*left_tf);
                let right_once = weights.right_once(*right_tf);
                let left_squared = left_once * left_once;
                let right_squared = right_once * right_once;
                processed_left_norm += left_squared;
                processed_right_norm += right_squared;
                shared.add_with_norms(left_once, right_once, left_squared, right_squared);
                left_pos += 1;
                right_pos += 1;
            }
            std::cmp::Ordering::Less => {
                let left_once = weights.left_once(*left_tf);
                processed_left_norm += left_once * left_once;
                left_pos += 1;
            }
            std::cmp::Ordering::Greater => {
                let right_once = weights.right_once(*right_tf);
                processed_right_norm += right_once * right_once;
                right_pos += 1;
            }
        }
        comparisons += 1;
        if comparisons.is_multiple_of(8)
            && weights.upper_bound(shared, processed_left_norm, processed_right_norm)
                + UPPER_BOUND_EPSILON
                < threshold
        {
            return ThresholdDecision {
                matched: false,
                zero_overlap_pruned: false,
                upper_bound_pruned: true,
            };
        }
    }
    decision(weights.score(shared), threshold, shared.count == 0, false)
}

pub(crate) fn may_share_term(
    left: &PreparedDocument,
    left_terms: &[(u32, u32)],
    right: &PreparedDocument,
    right_terms: &[(u32, u32)],
) -> bool {
    let term_ranges_are_disjoint = match (
        left_terms.first(),
        left_terms.last(),
        right_terms.first(),
        right_terms.last(),
    ) {
        (
            Some((left_first, _)),
            Some((left_last, _)),
            Some((right_first, _)),
            Some((right_last, _)),
        ) => left_last < right_first || right_last < left_first,
        _ => true,
    };
    if term_ranges_are_disjoint {
        return false;
    }
    !left
        .term_mask
        .iter()
        .zip(right.term_mask)
        .all(|(left, right)| left & right == 0)
}

#[derive(Clone, Copy, Debug, Default)]
struct SharedWeights {
    left_norm: f64,
    right_norm: f64,
    dot: f64,
    count: u32,
}

impl SharedWeights {
    fn add(&mut self, left_once: f64, right_once: f64) {
        self.add_with_norms(
            left_once,
            right_once,
            left_once * left_once,
            right_once * right_once,
        );
    }

    fn add_with_norms(
        &mut self,
        left_once: f64,
        right_once: f64,
        left_squared: f64,
        right_squared: f64,
    ) {
        self.left_norm += left_squared;
        self.right_norm += right_squared;
        self.dot += left_once * right_once;
        self.count += 1;
    }
}

struct PairWeights {
    left_length_norm: f64,
    right_length_norm: f64,
    left_unit_weight: f64,
    right_unit_weight: f64,
    left_base_norm: f64,
    right_base_norm: f64,
}

impl PairWeights {
    fn new(left: &PreparedDocument, right: &PreparedDocument) -> Self {
        let avgdl = f64::from(left.length + right.length) / 2.0;
        let left_length_norm = length_norm(left.length, avgdl);
        let right_length_norm = length_norm(right.length, avgdl);
        let left_unit_weight = weight(1, left_length_norm, IDF_ONCE);
        let right_unit_weight = weight(1, right_length_norm, IDF_ONCE);
        Self {
            left_length_norm,
            right_length_norm,
            left_unit_weight,
            right_unit_weight,
            left_base_norm: histogram_norm(
                left.frequency_histogram.as_slice(),
                left_length_norm,
                left_unit_weight,
            ),
            right_base_norm: histogram_norm(
                right.frequency_histogram.as_slice(),
                right_length_norm,
                right_unit_weight,
            ),
        }
    }

    fn left_once(&self, frequency: u32) -> f64 {
        if frequency == 1 {
            self.left_unit_weight
        } else {
            weight(frequency, self.left_length_norm, IDF_ONCE)
        }
    }

    fn right_once(&self, frequency: u32) -> f64 {
        if frequency == 1 {
            self.right_unit_weight
        } else {
            weight(frequency, self.right_length_norm, IDF_ONCE)
        }
    }

    fn score(&self, shared: SharedWeights) -> f64 {
        if shared.count == 0 {
            return 0.0;
        }
        let reduction = 1.0 - IDF_RATIO_SQUARED;
        let left_norm_squared = (self.left_base_norm - reduction * shared.left_norm).max(0.0);
        let right_norm_squared = (self.right_base_norm - reduction * shared.right_norm).max(0.0);
        let denominator = (left_norm_squared * right_norm_squared).sqrt();
        if denominator <= 0.0 {
            0.0
        } else {
            IDF_RATIO_SQUARED * shared.dot / denominator
        }
    }

    fn upper_bound(
        &self,
        shared: SharedWeights,
        processed_left_norm: f64,
        processed_right_norm: f64,
    ) -> f64 {
        let remaining_left = (self.left_base_norm - processed_left_norm).max(0.0);
        let remaining_right = (self.right_base_norm - processed_right_norm).max(0.0);
        let maximum_dot = shared.dot + (remaining_left * remaining_right).sqrt();
        let reduction = 1.0 - IDF_RATIO_SQUARED;
        let minimum_left_norm =
            (self.left_base_norm - reduction * (shared.left_norm + remaining_left)).max(0.0);
        let minimum_right_norm =
            (self.right_base_norm - reduction * (shared.right_norm + remaining_right)).max(0.0);
        let minimum_denominator = (minimum_left_norm * minimum_right_norm).sqrt();
        if minimum_denominator <= 0.0 {
            1.0
        } else {
            (IDF_RATIO_SQUARED * maximum_dot / minimum_denominator).min(1.0)
        }
    }
}

fn decision(
    score: f64,
    threshold: f64,
    zero_overlap: bool,
    upper_bound_pruned: bool,
) -> ThresholdDecision {
    if zero_overlap && threshold <= 0.0 {
        return ThresholdDecision {
            matched: true,
            zero_overlap_pruned: false,
            upper_bound_pruned: false,
        };
    }
    ThresholdDecision {
        matched: score >= threshold,
        zero_overlap_pruned: zero_overlap && threshold > 0.0,
        upper_bound_pruned,
    }
}

#[cfg(test)]
fn legacy_cosine_similarity(
    left: &PreparedDocument,
    left_terms: &[(u32, u32)],
    right: &PreparedDocument,
    right_terms: &[(u32, u32)],
) -> f64 {
    let avgdl = f64::from(left.length + right.length) / 2.0;
    let mut left_pos = 0;
    let mut right_pos = 0;
    let mut dot = 0.0;
    let mut left_norm_squared = 0.0;
    let mut right_norm_squared = 0.0;
    while left_pos < left_terms.len() || right_pos < right_terms.len() {
        match (left_terms.get(left_pos), right_terms.get(right_pos)) {
            (Some((left_term, left_tf)), Some((right_term, right_tf))) => {
                match left_term.cmp(right_term) {
                    std::cmp::Ordering::Equal => {
                        let left_weight = legacy_weight(*left_tf, left.length, avgdl, 2);
                        let right_weight = legacy_weight(*right_tf, right.length, avgdl, 2);
                        dot += left_weight * right_weight;
                        left_norm_squared += left_weight * left_weight;
                        right_norm_squared += right_weight * right_weight;
                        left_pos += 1;
                        right_pos += 1;
                    }
                    std::cmp::Ordering::Less => {
                        let left_weight = legacy_weight(*left_tf, left.length, avgdl, 1);
                        left_norm_squared += left_weight * left_weight;
                        left_pos += 1;
                    }
                    std::cmp::Ordering::Greater => {
                        let right_weight = legacy_weight(*right_tf, right.length, avgdl, 1);
                        right_norm_squared += right_weight * right_weight;
                        right_pos += 1;
                    }
                }
            }
            (Some((_, left_tf)), None) => {
                let left_weight = legacy_weight(*left_tf, left.length, avgdl, 1);
                left_norm_squared += left_weight * left_weight;
                left_pos += 1;
            }
            (None, Some((_, right_tf))) => {
                let right_weight = legacy_weight(*right_tf, right.length, avgdl, 1);
                right_norm_squared += right_weight * right_weight;
                right_pos += 1;
            }
            (None, None) => break,
        }
    }
    if dot == 0.0 {
        return 0.0;
    }
    let left_norm = left_norm_squared.sqrt();
    let right_norm = right_norm_squared.sqrt();
    if left_norm <= 0.0 || right_norm <= 0.0 {
        0.0
    } else {
        dot / (left_norm * right_norm)
    }
}

#[cfg(test)]
fn legacy_weight(frequency: u32, document_length: u32, avgdl: f64, document_frequency: u32) -> f64 {
    let frequency = f64::from(frequency);
    let document_length = f64::from(document_length);
    let document_frequency = f64::from(document_frequency);
    let idf = ((2.0 - document_frequency + 0.5) / (document_frequency + 0.5) + 1.0).ln();
    let tf_norm = (frequency * (K1 + 1.0))
        / (frequency + K1 * (1.0 - B + B * document_length / avgdl.max(1.0)));
    idf * tf_norm
}

fn for_each_shared_term(
    left: &[(u32, u32)],
    right: &[(u32, u32)],
    mut visit: impl FnMut(u32, u32),
) {
    let (shorter, longer, swapped) = if left.len() <= right.len() {
        (left, right, false)
    } else {
        (right, left, true)
    };
    if shorter.len().saturating_mul(4) < longer.len() {
        for (term, short_tf) in shorter {
            if let Ok(position) = longer.binary_search_by(|(candidate, _)| candidate.cmp(term)) {
                let long_tf = longer[position].1;
                if swapped {
                    visit(long_tf, *short_tf);
                } else {
                    visit(*short_tf, long_tf);
                }
            }
        }
        return;
    }

    let mut left_pos = 0;
    let mut right_pos = 0;
    while left_pos < left.len() && right_pos < right.len() {
        let (left_term, left_tf) = &left[left_pos];
        let (right_term, right_tf) = &right[right_pos];
        match left_term.cmp(right_term) {
            std::cmp::Ordering::Equal => {
                visit(*left_tf, *right_tf);
                left_pos += 1;
                right_pos += 1;
            }
            std::cmp::Ordering::Less => left_pos += 1,
            std::cmp::Ordering::Greater => right_pos += 1,
        }
    }
}

fn histogram_norm(histogram: &[(u32, u32)], length_norm: f64, unit_weight: f64) -> f64 {
    histogram
        .iter()
        .map(|(frequency, count)| {
            let weighted = if *frequency == 1 {
                unit_weight
            } else {
                weight(*frequency, length_norm, IDF_ONCE)
            };
            weighted * weighted * f64::from(*count)
        })
        .sum()
}

fn length_norm(document_length: u32, avgdl: f64) -> f64 {
    K1 * (1.0 - B + B * f64::from(document_length) / avgdl.max(1.0))
}

fn weight(frequency: u32, length_norm: f64, idf: f64) -> f64 {
    let frequency = f64::from(frequency);
    let tf_norm = (frequency * (K1 + 1.0)) / (frequency + length_norm);
    idf * tf_norm
}

#[cfg(test)]
mod tests {
    use super::*;
    use ahash::AHashMap;

    fn prepare(texts: &[&str]) -> Vec<PreparedDocumentParts> {
        let mut term_ids: AHashMap<String, u32> = AHashMap::new();
        texts
            .iter()
            .map(|text| {
                PreparedDocument::try_new(text, |term| {
                    if let Some(&id) = term_ids.get(term) {
                        return Ok::<_, std::convert::Infallible>(id);
                    }
                    let id = term_ids.len() as u32;
                    term_ids.insert(term.to_owned(), id);
                    Ok(id)
                })
                .unwrap()
            })
            .collect()
    }

    fn similarity(left: &PreparedDocumentParts, right: &PreparedDocumentParts) -> f64 {
        cosine_similarity(&left.document, &left.terms, &right.document, &right.terms)
    }

    fn legacy_similarity(left: &PreparedDocumentParts, right: &PreparedDocumentParts) -> f64 {
        legacy_cosine_similarity(&left.document, &left.terms, &right.document, &right.terms)
    }

    fn threshold_decision(
        left: &PreparedDocumentParts,
        right: &PreparedDocumentParts,
        threshold: f64,
    ) -> ThresholdDecision {
        similarity_at_least(
            &left.document,
            &left.terms,
            &right.document,
            &right.terms,
            threshold,
        )
    }

    #[test]
    fn identical_texts_score_high() {
        let documents = prepare(&["hello world collection", "hello world collection"]);
        let left = &documents[0];
        let right = &documents[1];
        assert!(similarity(left, right) > 0.99);
    }

    #[test]
    fn repeated_terms_allocate_only_unique_entries() {
        let document = prepare(&["name name name collection collection"])
            .pop()
            .unwrap();
        assert_eq!(document.document.length, 5);
        assert_eq!(document.terms.len(), 2);
        assert_eq!(
            document
                .terms
                .iter()
                .map(|(_, frequency)| *frequency)
                .collect::<Vec<_>>(),
            vec![3, 2]
        );
        assert_eq!(
            document.document.frequency_histogram.as_slice(),
            [(2, 1), (3, 1)]
        );
    }

    #[test]
    fn ascii_fast_tokenizer_matches_the_original_character_split() {
        let text = r#"{"name":"Alpha-42_beta","url":"https://example.com/a1"}"#;
        let expected = text
            .split(|character: char| !character.is_alphanumeric())
            .filter(|token| !token.is_empty())
            .collect::<Vec<_>>();
        let mut actual = Vec::new();
        PreparedDocument::try_new(text, |term| {
            actual.push(term);
            Ok::<_, std::convert::Infallible>(actual.len() as u32)
        })
        .unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn unicode_tokenizer_preserves_non_ascii_alphanumeric_terms() {
        let text = "猫 名称 café_42";
        let mut actual = Vec::new();
        PreparedDocument::try_new(text, |term| {
            actual.push(term);
            Ok::<_, std::convert::Infallible>(actual.len() as u32)
        })
        .unwrap();
        assert_eq!(actual, ["猫", "名称", "café", "42"]);
    }

    #[test]
    fn prepared_documents_read_terms_from_nonzero_csr_offsets() {
        let mut documents = prepare(&["alpha beta beta", "alpha gamma"]);
        let mut terms = vec![(u32::MAX, u32::MAX)];
        for parts in &mut documents {
            parts.document.set_term_start(terms.len() as u32);
            terms.extend_from_slice(&parts.terms);
        }
        let left_terms = documents[0].document.terms(&terms);
        let right_terms = documents[1].document.terms(&terms);
        assert_eq!(left_terms, documents[0].terms);
        assert_eq!(right_terms, documents[1].terms);
        assert_eq!(
            cosine_similarity(
                &documents[0].document,
                left_terms,
                &documents[1].document,
                right_terms,
            ),
            similarity(&documents[0], &documents[1])
        );
    }

    #[test]
    fn frequency_histogram_falls_back_without_changing_sorted_counts() {
        let document = prepare(&["a b b c c c d d d d e e e e e"]).pop().unwrap();
        assert!(matches!(
            &document.document.frequency_histogram,
            FrequencyHistogram::Heap(_)
        ));
        assert_eq!(
            document.document.frequency_histogram.as_slice(),
            [(1, 1), (2, 1), (3, 1), (4, 1), (5, 1)]
        );
    }

    #[test]
    fn disjoint_documents_score_zero() {
        let documents = prepare(&["alpha beta gamma", "delta epsilon zeta"]);
        let left = &documents[0];
        let right = &documents[1];
        assert_eq!(similarity(left, right), 0.0);
    }

    #[test]
    fn imbalanced_documents_use_the_same_symmetric_score() {
        let documents = prepare(&[
            "shared",
            "a b c d e f g h i j k l m n o p q r s t u v shared",
        ]);
        let left = &documents[0];
        let right = &documents[1];
        assert_eq!(similarity(left, right), similarity(right, left));
    }

    #[test]
    fn optimized_score_matches_legacy_score() {
        let documents = [
            "alpha beta gamma",
            "alpha alpha beta delta epsilon",
            "gamma delta zeta eta theta iota",
            "shared",
            "a b c d e f g h i j k l m n o p q r s t u v shared",
            "completely unrelated document terms",
        ];
        let documents = prepare(&documents);
        for left in &documents {
            for right in &documents {
                let optimized = similarity(left, right);
                let legacy = legacy_similarity(left, right);
                assert!(
                    (optimized - legacy).abs() <= 1e-12,
                    "optimized={optimized}, legacy={legacy}"
                );
                for threshold in [0.0, 0.2, 0.6, 0.95, 1.01] {
                    assert_eq!(
                        threshold_decision(left, right, threshold).matched,
                        legacy >= threshold
                    );
                }
            }
        }
    }

    #[test]
    fn upper_bound_prunes_low_overlap_documents() {
        let documents = prepare(&[
            "shared a01 a02 a03 a04 a05 a06 a07 a08 a09 a10 a11 a12 a13 a14 a15 a16",
            "shared z01 z02 z03 z04 z05 z06 z07 z08 z09 z10 z11 z12 z13 z14 z15 z16",
        ]);
        let left = &documents[0];
        let right = &documents[1];
        let decision = threshold_decision(left, right, 0.6);
        assert!(!decision.matched);
        assert!(decision.upper_bound_pruned || decision.zero_overlap_pruned);
        assert!(legacy_similarity(left, right) < 0.6);
    }

    #[test]
    fn threshold_pruning_matches_exhaustive_generated_oracle() {
        let vocabulary = [
            "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "theta",
        ];
        let texts = (1_u32..128)
            .map(|mask| {
                let mut words = Vec::new();
                for (index, word) in vocabulary.iter().enumerate() {
                    if mask & (1 << index) != 0 {
                        words.push(*word);
                        if (mask as usize + index).is_multiple_of(3) {
                            words.push(*word);
                        }
                    }
                }
                words.join(" ")
            })
            .collect::<Vec<_>>();
        let documents = prepare(&texts.iter().map(String::as_str).collect::<Vec<_>>());
        for left in &documents {
            for right in &documents {
                let legacy = legacy_similarity(left, right);
                for threshold in [0.2, 0.4, 0.6, 0.8, 0.95] {
                    assert_eq!(
                        threshold_decision(left, right, threshold).matched,
                        legacy >= threshold,
                        "legacy={legacy}, threshold={threshold}"
                    );
                }
            }
        }
    }

    #[test]
    fn lossless_prefix_contains_a_witness_for_every_threshold_match() {
        let texts = [
            "alpha beta gamma delta epsilon",
            "alpha beta gamma delta zeta",
            "alpha alpha beta gamma",
            "alpha alpha beta theta",
            "collection name attributes image",
            "collection title properties animation",
            "entirely unrelated vocabulary",
        ];
        let documents = prepare(&texts);
        let term_count = documents
            .iter()
            .flat_map(|document| document.terms.iter().map(|(term, _)| *term as usize + 1))
            .max()
            .unwrap_or(0);
        let mut document_frequency = vec![0_u32; term_count];
        for document in &documents {
            for &(term, _) in &document.terms {
                document_frequency[term as usize] += 1;
            }
        }
        let mut ordered_terms = document_frequency
            .iter()
            .enumerate()
            .map(|(term, frequency)| (*frequency, term as u32))
            .collect::<Vec<_>>();
        ordered_terms.sort_unstable();
        let mut rank = vec![0_u32; term_count];
        for (position, &(_, term)) in ordered_terms.iter().enumerate() {
            rank[term as usize] = position as u32;
        }
        let prefixes = documents
            .iter()
            .map(|document| {
                let mut terms = document.terms.clone();
                terms.sort_unstable_by_key(|(term, _)| rank[*term as usize]);
                let frequencies = terms
                    .iter()
                    .map(|(_, frequency)| *frequency)
                    .collect::<Vec<_>>();
                let len = lossless_prefix_len(&frequencies, 0.6);
                terms[..len]
                    .iter()
                    .map(|(term, _)| *term)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        for (left_id, left) in documents.iter().enumerate() {
            for (right_id, right) in documents.iter().enumerate() {
                if legacy_similarity(left, right) < 0.6 {
                    continue;
                }
                assert!(
                    prefixes[left_id].iter().any(|term| {
                        right
                            .terms
                            .binary_search_by_key(term, |(candidate, _)| *candidate)
                            .is_ok()
                    }),
                    "left prefix has no witness for matching pair {left_id}/{right_id}"
                );
            }
        }
    }
}
