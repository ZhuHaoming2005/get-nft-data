//! Proof-safe L0/L2 cascade. Signature AND rejects have only false positives
//! (never false negatives), then CSR intersection confirms exact overlap.

use crate::encode::FeatureView;

pub const PAYLOAD_TERM_SIG_BYTES: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CascadeDecision {
    RejectL0TemplateNoOverlap,
    RejectL2ContentNoOverlap,
    ScoreExactL1L3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PairScoreDecision {
    RejectL0TemplateNoOverlap,
    RejectL2ContentNoOverlap,
    ExactMatch,
    ExactMiss,
}

pub fn score_pair(view: &FeatureView, left: u32, right: u32) -> PairScoreDecision {
    match decide(view, left, right) {
        CascadeDecision::RejectL0TemplateNoOverlap => PairScoreDecision::RejectL0TemplateNoOverlap,
        CascadeDecision::RejectL2ContentNoOverlap => PairScoreDecision::RejectL2ContentNoOverlap,
        CascadeDecision::ScoreExactL1L3 => {
            if crate::scoring::template_matches(view, left, right)
                && crate::scoring::content_matches(view, left, right)
            {
                PairScoreDecision::ExactMatch
            } else {
                PairScoreDecision::ExactMiss
            }
        }
    }
}

pub fn decide(view: &FeatureView, left_payload: u32, right_payload: u32) -> CascadeDecision {
    // Proof-safe bloom signatures: AND==0 ⇒ no shared term IDs (no FN).
    if !signatures_may_overlap(
        view.payload_template_sig(left_payload),
        view.payload_template_sig(right_payload),
    ) {
        return CascadeDecision::RejectL0TemplateNoOverlap;
    }
    let lt = terms(
        &view.payload_template_offsets,
        &view.payload_template_terms,
        left_payload,
    );
    let rt = terms(
        &view.payload_template_offsets,
        &view.payload_template_terms,
        right_payload,
    );
    if !intersects(lt, rt) {
        return CascadeDecision::RejectL0TemplateNoOverlap;
    }
    if !signatures_may_overlap(
        view.payload_content_sig(left_payload),
        view.payload_content_sig(right_payload),
    ) {
        return CascadeDecision::RejectL2ContentNoOverlap;
    }
    let lc = terms(
        &view.payload_content_offsets,
        &view.payload_content_terms,
        left_payload,
    );
    let rc = terms(
        &view.payload_content_offsets,
        &view.payload_content_terms,
        right_payload,
    );
    if !intersects(lc, rc) {
        return CascadeDecision::RejectL2ContentNoOverlap;
    }
    CascadeDecision::ScoreExactL1L3
}

/// Build a 256-bit signature from sorted unique term IDs. Each term sets two
/// bits so AND-zero rejects remain false-negative free.
pub fn term_id_signature(term_ids: impl IntoIterator<Item = u32>) -> [u8; PAYLOAD_TERM_SIG_BYTES] {
    let mut sig = [0u8; PAYLOAD_TERM_SIG_BYTES];
    for term in term_ids {
        let mixed = term.wrapping_mul(0x9E37_79B9).wrapping_add(0x85EB_CA6B);
        set_sig_bit(&mut sig, mixed);
        set_sig_bit(&mut sig, mixed.rotate_left(11) ^ 0xC2B2_AE35);
    }
    sig
}

fn set_sig_bit(sig: &mut [u8; PAYLOAD_TERM_SIG_BYTES], hash: u32) {
    let bit = (hash as usize) % (PAYLOAD_TERM_SIG_BYTES * 8);
    sig[bit / 8] |= 1 << (bit % 8);
}

fn signatures_may_overlap(left: &[u8], right: &[u8]) -> bool {
    left.iter().zip(right.iter()).any(|(a, b)| (*a & *b) != 0)
}

/// A directed-rounding-safe upper envelope for the current zero-overlap proof.
/// It is intentionally 1 for any overlapping pair, so it cannot false-reject.
pub fn content_upper_safe(view: &FeatureView, left: u32, right: u32) -> f64 {
    let a = terms(
        &view.payload_content_offsets,
        &view.payload_content_terms,
        left,
    );
    let b = terms(
        &view.payload_content_offsets,
        &view.payload_content_terms,
        right,
    );
    if intersects(a, b) {
        1.0
    } else {
        0.0
    }
}

fn terms<'a>(o: &[u64], v: &'a [u32], i: u32) -> &'a [u32] {
    let i = i as usize;
    if i + 1 >= o.len() {
        return &[];
    }
    &v[o[i] as usize..o[i + 1] as usize]
}

fn intersects(a: &[u32], b: &[u32]) -> bool {
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => return true,
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signature_and_zero_only_when_terms_disjoint_without_bit_collision() {
        let left = term_id_signature([1u32, 2]);
        let same = term_id_signature([1u32, 2]);
        assert!(signatures_may_overlap(&left, &same));
        // Empty signatures never overlap.
        let empty = term_id_signature([]);
        assert!(!signatures_may_overlap(&empty, &left));
    }
}
