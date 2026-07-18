const K1: f64 = 1.2;
const B: f64 = 0.75;

#[derive(Clone, Debug)]
pub struct PreparedDocument {
    pub canonical: String,
    terms: Vec<(String, u32)>,
    length: u32,
}

impl PreparedDocument {
    pub fn new(canonical: String) -> Self {
        let mut terms = canonical
            .split(|character: char| !character.is_alphanumeric())
            .filter(|token| !token.is_empty())
            .map(str::to_owned)
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
            counts.push((terms[index].clone(), (end - index) as u32));
            index = end;
        }
        Self {
            canonical,
            terms: counts,
            length,
        }
    }
}

pub fn cosine_similarity(left: &PreparedDocument, right: &PreparedDocument) -> f64 {
    let avgdl = f64::from(left.length + right.length) / 2.0;
    let mut left_weights = Vec::with_capacity(left.terms.len());
    let mut right_weights = Vec::with_capacity(right.terms.len());
    let mut left_pos = 0;
    let mut right_pos = 0;
    while left_pos < left.terms.len() || right_pos < right.terms.len() {
        match (left.terms.get(left_pos), right.terms.get(right_pos)) {
            (Some((left_term, left_tf)), Some((right_term, right_tf))) => {
                match left_term.cmp(right_term) {
                    std::cmp::Ordering::Equal => {
                        left_weights.push(weight(*left_tf, left.length, avgdl, 2));
                        right_weights.push(weight(*right_tf, right.length, avgdl, 2));
                        left_pos += 1;
                        right_pos += 1;
                    }
                    std::cmp::Ordering::Less => {
                        left_weights.push(weight(*left_tf, left.length, avgdl, 1));
                        right_weights.push(0.0);
                        left_pos += 1;
                    }
                    std::cmp::Ordering::Greater => {
                        left_weights.push(0.0);
                        right_weights.push(weight(*right_tf, right.length, avgdl, 1));
                        right_pos += 1;
                    }
                }
            }
            (Some((_, left_tf)), None) => {
                left_weights.push(weight(*left_tf, left.length, avgdl, 1));
                right_weights.push(0.0);
                left_pos += 1;
            }
            (None, Some((_, right_tf))) => {
                left_weights.push(0.0);
                right_weights.push(weight(*right_tf, right.length, avgdl, 1));
                right_pos += 1;
            }
            (None, None) => break,
        }
    }
    let dot = left_weights
        .iter()
        .zip(&right_weights)
        .map(|(left, right)| left * right)
        .sum::<f64>();
    let left_norm = left_weights
        .iter()
        .map(|value| value * value)
        .sum::<f64>()
        .sqrt();
    let right_norm = right_weights
        .iter()
        .map(|value| value * value)
        .sum::<f64>()
        .sqrt();
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
        let left = PreparedDocument::new("hello world collection".to_owned());
        let right = PreparedDocument::new("hello world collection".to_owned());
        assert!(cosine_similarity(&left, &right) > 0.99);
    }
}
