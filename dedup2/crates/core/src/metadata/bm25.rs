use ahash::AHashMap;

const K1: f64 = 1.2;
const B: f64 = 0.75;

#[derive(Clone, Debug)]
pub struct Bm25Vector {
    weights: Vec<(u32, f64)>,
    norm: f64,
}

pub fn cosine_similarity(left: &Bm25Vector, right: &Bm25Vector) -> f64 {
    if left.norm <= 0.0 || right.norm <= 0.0 {
        return 0.0;
    }
    let mut i = 0;
    let mut j = 0;
    let mut dot = 0.0;
    while i < left.weights.len() && j < right.weights.len() {
        match left.weights[i].0.cmp(&right.weights[j].0) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                dot += left.weights[i].1 * right.weights[j].1;
                i += 1;
                j += 1;
            }
        }
    }
    dot / (left.norm * right.norm)
}

pub fn build_pair_vectors(left_text: &str, right_text: &str) -> (Bm25Vector, Bm25Vector) {
    let left_terms = tokenize(left_text);
    let right_terms = tokenize(right_text);
    let mut df: AHashMap<&str, u32> = AHashMap::new();
    for term in left_terms.keys() {
        *df.entry(*term).or_default() += 1;
    }
    for term in right_terms.keys() {
        *df.entry(*term).or_default() += 1;
    }
    let avgdl = ((left_terms.values().sum::<u32>() + right_terms.values().sum::<u32>()) as f64) / 2.0;
    let n_docs = 2.0;
    let mut term_ids: AHashMap<&str, u32> = AHashMap::new();
    let mut next_id = 0_u32;
    for term in df.keys() {
        term_ids.insert(*term, next_id);
        next_id += 1;
    }
    (
        to_vector(&left_terms, &df, &term_ids, avgdl, n_docs),
        to_vector(&right_terms, &df, &term_ids, avgdl, n_docs),
    )
}

fn tokenize(text: &str) -> AHashMap<&str, u32> {
    let mut counts = AHashMap::new();
    for token in text.split(|c: char| !c.is_alphanumeric()) {
        if token.is_empty() {
            continue;
        }
        *counts.entry(token).or_default() += 1;
    }
    counts
}

fn to_vector(
    tf: &AHashMap<&str, u32>,
    df: &AHashMap<&str, u32>,
    term_ids: &AHashMap<&str, u32>,
    avgdl: f64,
    n_docs: f64,
) -> Bm25Vector {
    let doc_len = tf.values().sum::<u32>() as f64;
    let mut weights = Vec::new();
    for (term, &freq) in tf {
        let id = term_ids[term];
        let document_freq = df[term] as f64;
        let idf = ((n_docs - document_freq + 0.5) / (document_freq + 0.5) + 1.0).ln();
        let tf_norm = (freq as f64 * (K1 + 1.0))
            / (freq as f64 + K1 * (1.0 - B + B * doc_len / avgdl.max(1.0)));
        let w = idf * tf_norm;
        if w.is_finite() && w != 0.0 {
            weights.push((id, w));
        }
    }
    weights.sort_by_key(|(id, _)| *id);
    let norm = weights.iter().map(|(_, w)| w * w).sum::<f64>().sqrt();
    Bm25Vector { weights, norm }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_texts_score_high() {
        let (a, b) = build_pair_vectors("hello world collection", "hello world collection");
        assert!(cosine_similarity(&a, &b) > 0.99);
    }
}
