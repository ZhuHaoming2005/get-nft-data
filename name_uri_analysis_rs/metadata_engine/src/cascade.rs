//! Proof-safe L0/L2 cascade. Initial revision only hard-rejects zero-overlap;
//! tighter numeric bounds remain inactive until separately evidenced.

use crate::encode::FeatureView;

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
