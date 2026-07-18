const K1: f64 = 1.2;
const B: f64 = 0.75;

#[derive(Clone, Debug)]
pub struct PreparedDocument<'a> {
    pub canonical: &'a str,
    terms: Vec<(String, u32)>,
    length: u32,
}

impl<'a> PreparedDocument<'a> {
    pub fn new(canonical: &'a str) -> Self {
        let mut terms = canonical
            .split(|character: char| !character.is_alphanumeric())
            .filter(|token| !token.is_empty())
            .collect::<Vec<_>>();
        terms.sort_unstable();
        let length = terms.len() as u32;
        let mut counts = Vec::new();
        let mut index = 0;
        while index < terms.len() {
            let mut end = index + 1;
            while end < terms.len() && terms[end] == terms[index] {
                end += 1;
            }
            counts.push((terms[index].to_owned(), (end - index) as u32));
            index = end;
        }
        Self {
            canonical,
            terms: counts,
            length,
        }
    }
}

pub fn cosine_similarity(left: &PreparedDocument<'_>, right: &PreparedDocument<'_>) -> f64 {
    let avgdl = f64::from(left.length + right.length) / 2.0;
    let mut left_pos = 0;
    let mut right_pos = 0;
    let mut dot = 0.0;
    let mut left_norm_squared = 0.0;
    let mut right_norm_squared = 0.0;
    while left_pos < left.terms.len() || right_pos < right.terms.len() {
        match (left.terms.get(left_pos), right.terms.get(right_pos)) {
            (Some((left_term, left_tf)), Some((right_term, right_tf))) => {
                match left_term.cmp(right_term) {
                    std::cmp::Ordering::Equal => {
                        let left_weight = weight(*left_tf, left.length, avgdl, 2);
                        let right_weight = weight(*right_tf, right.length, avgdl, 2);
                        dot += left_weight * right_weight;
                        left_norm_squared += left_weight * left_weight;
                        right_norm_squared += right_weight * right_weight;
                        left_pos += 1;
                        right_pos += 1;
                    }
                    std::cmp::Ordering::Less => {
                        let left_weight = weight(*left_tf, left.length, avgdl, 1);
                        left_norm_squared += left_weight * left_weight;
                        left_pos += 1;
                    }
                    std::cmp::Ordering::Greater => {
                        let right_weight = weight(*right_tf, right.length, avgdl, 1);
                        right_norm_squared += right_weight * right_weight;
                        right_pos += 1;
                    }
                }
            }
            (Some((_, left_tf)), None) => {
                let left_weight = weight(*left_tf, left.length, avgdl, 1);
                left_norm_squared += left_weight * left_weight;
                left_pos += 1;
            }
            (None, Some((_, right_tf))) => {
                let right_weight = weight(*right_tf, right.length, avgdl, 1);
                right_norm_squared += right_weight * right_weight;
                right_pos += 1;
            }
            (None, None) => break,
        }
    }
    let left_norm = left_norm_squared.sqrt();
    let right_norm = right_norm_squared.sqrt();
    if left_norm <= 0.0 || right_norm <= 0.0 {
        0.0
    } else {
        dot / (left_norm * right_norm)
    }
}

fn weight(frequency: u32, document_length: u32, avgdl: f64, document_frequency: u32) -> f64 {
    let frequency = f64::from(frequency);
    let document_length = f64::from(document_length);
    let document_frequency = f64::from(document_frequency);
    let idf = ((2.0 - document_frequency + 0.5) / (document_frequency + 0.5) + 1.0).ln();
    let tf_norm = (frequency * (K1 + 1.0))
        / (frequency + K1 * (1.0 - B + B * document_length / avgdl.max(1.0)));
    idf * tf_norm
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_texts_score_high() {
        let left = PreparedDocument::new("hello world collection");
        let right = PreparedDocument::new("hello world collection");
        assert!(cosine_similarity(&left, &right) > 0.99);
    }

    #[test]
    fn repeated_terms_allocate_only_unique_entries() {
        let document = PreparedDocument::new("name name name collection collection");
        assert_eq!(document.length, 5);
        assert_eq!(
            document.terms,
            vec![("collection".to_owned(), 2), ("name".to_owned(), 3)]
        );
    }
}
