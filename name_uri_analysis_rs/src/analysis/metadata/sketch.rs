use super::bm25::{InternedMetadataCorpus, InternedMetadataSourceDoc};

pub(super) const METADATA_SKETCH_ANCHOR_COUNT: usize = 16;
pub(super) const METADATA_SKETCH_SOURCE_HAMMING_THRESHOLD: u32 = 32;
pub(super) const METADATA_SKETCH_HIGH_FREQ_MIN_DOCS: usize = 32;
pub(super) const METADATA_SKETCH_HIGH_FREQ_DIVISOR: usize = 5;

#[derive(Clone, Debug, Default)]
pub(super) struct MetadataSketch {
    pub(super) simhash: u64,
    pub(super) anchors: Vec<usize>,
}

pub(super) fn sorted_metadata_anchors_intersect(left: &[usize], right: &[usize]) -> bool {
    let mut left_index = 0;
    let mut right_index = 0;
    while left_index < left.len() && right_index < right.len() {
        match left[left_index].cmp(&right[right_index]) {
            std::cmp::Ordering::Equal => return true,
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
        }
    }
    false
}

pub(super) fn metadata_sketch_source_match(
    left: &MetadataSketch,
    right: &MetadataSketch,
    hamming_threshold: u32,
) -> bool {
    if (left.simhash == 0 && left.anchors.is_empty())
        || (right.simhash == 0 && right.anchors.is_empty())
    {
        return false;
    }
    if !left.anchors.is_empty()
        && sorted_metadata_anchors_intersect(&left.anchors, &right.anchors)
    {
        return true;
    }
    (left.simhash ^ right.simhash).count_ones() <= hamming_threshold
}

pub(super) fn stable_metadata_token_hash(token: &str) -> u64 {
    let mut value = 0xcbf2_9ce4_8422_2325u64;
    for byte in token.as_bytes() {
        value ^= u64::from(*byte);
        value = value.wrapping_mul(0x0000_0100_0000_01b3);
    }
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

pub(super) fn metadata_token_idf(total_docs: usize, doc_freq: usize) -> f64 {
    (((total_docs + 1) as f64) / ((doc_freq + 1) as f64)).ln() + 1.0
}

pub(super) fn metadata_token_is_high_frequency(total_docs: usize, doc_freq: usize) -> bool {
    doc_freq >= METADATA_SKETCH_HIGH_FREQ_MIN_DOCS
        && doc_freq.saturating_mul(METADATA_SKETCH_HIGH_FREQ_DIVISOR) > total_docs
}

pub(super) fn metadata_sketch_from_interned_document(
    document: &InternedMetadataSourceDoc,
    corpus: &InternedMetadataCorpus,
    token_hashes: &[u64],
) -> MetadataSketch {
    let mut weights = [0.0f64; 64];
    let mut anchor_candidates = Vec::new();
    for &token in document.unique_tokens() {
        let doc_freq = corpus.doc_freqs.get(token).copied().unwrap_or(0);
        let idf = metadata_token_idf(corpus.total_docs, doc_freq);
        let token_hash = token_hashes.get(token).copied().unwrap_or(0);
        for (bit, weight) in weights.iter_mut().enumerate() {
            if ((token_hash >> bit) & 1) == 1 {
                *weight += idf;
            } else {
                *weight -= idf;
            }
        }
        if !metadata_token_is_high_frequency(corpus.total_docs, doc_freq) {
            anchor_candidates.push((token, doc_freq));
        }
    }
    anchor_candidates.sort_unstable_by(|left, right| {
        left.1
            .cmp(&right.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    let mut anchors = anchor_candidates
        .into_iter()
        .take(METADATA_SKETCH_ANCHOR_COUNT)
        .map(|(token, _)| token)
        .collect::<Vec<_>>();
    anchors.sort_unstable();
    let mut simhash = 0u64;
    for (bit, weight) in weights.into_iter().enumerate() {
        if weight >= 0.0 {
            simhash |= 1u64 << bit;
        }
    }
    MetadataSketch { simhash, anchors }
}
