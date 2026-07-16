//! Exact f64 scorers owned by Metadata Engine.

use crate::encode::FeatureView;

pub const METADATA_THRESHOLD: f64 = 0.6;
pub const MATCH_SEMANTICS_REVISION: u32 = 7;
const K1: f64 = 1.2;
const B: f64 = 0.75;
const PRESENT_IDF: f64 = 0.287_682_072_451_780_85;
const ABSENT_IDF: f64 = 1.386_294_361_119_890_6;

/// Both directional normalized template BM25 scores for two payloads.
pub fn template_score_bidirectional(view: &FeatureView, left: u32, right: u32) -> (f64, f64) {
    let l = template_parts(view, left as usize);
    let r = template_parts(view, right as usize);
    let (Some((lt, lf, lw, ld)), Some((rt, rf, rw, rd))) = (l, r) else {
        return (0.0, 0.0);
    };
    let mut ls = 0.0;
    let mut rs = 0.0;
    let (mut i, mut j) = (0, 0);
    while i < lt.len() && j < rt.len() {
        match lt[i].cmp(&rt[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                ls += f64::from(lf[i]) * rw[j];
                rs += f64::from(rf[j]) * lw[i];
                i += 1;
                j += 1;
            }
        }
    }
    ((ls / ld).clamp(0.0, 1.0), (rs / rd).clamp(0.0, 1.0))
}

pub fn template_matches(view: &FeatureView, left: u32, right: u32) -> bool {
    if left == right {
        return true;
    }
    let (Some((lt, lf, lw, ld)), Some((rt, rf, rw, rd))) = (
        template_parts(view, left as usize),
        template_parts(view, right as usize),
    ) else {
        return false;
    };
    if !(ld > 0.0 && rd > 0.0) {
        let (a, b) = template_score_bidirectional(view, left, right);
        return a >= METADATA_THRESHOLD || b >= METADATA_THRESHOLD;
    }
    let left_target = METADATA_THRESHOLD * ld;
    let right_target = METADATA_THRESHOLD * rd;
    let (mut left_score, mut right_score) = (0.0, 0.0);
    let (mut left_index, mut right_index) = (0usize, 0usize);
    while left_index < lt.len() && right_index < rt.len() {
        match lt[left_index].cmp(&rt[right_index]) {
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
            std::cmp::Ordering::Equal => {
                left_score += f64::from(lf[left_index]) * rw[right_index];
                right_score += f64::from(rf[right_index]) * lw[left_index];
                if left_score >= left_target || right_score >= right_target {
                    return true;
                }
                left_index += 1;
                right_index += 1;
            }
        }
    }
    false
}

/// Exact legacy-compatible content pair score (max of both directions).
pub fn content_pair_score(view: &FeatureView, left: u32, right: u32) -> f64 {
    let Some((lt, lf, llen)) = content_parts(view, left as usize) else {
        return 0.0;
    };
    let Some((rt, rf, rlen)) = content_parts(view, right as usize) else {
        return 0.0;
    };
    if llen == 0 || rlen == 0 || lt.is_empty() || rt.is_empty() {
        return 0.0;
    }
    let lnorm = K1 * (1.0 - B + B * f64::from(llen) / f64::from(rlen));
    let rnorm = K1 * (1.0 - B + B * f64::from(rlen) / f64::from(llen));
    let (mut ln, mut ld, mut rn, mut rd) = (0.0, 0.0, 0.0, 0.0);
    let (mut i, mut j) = (0, 0);
    while i < lt.len() && j < rt.len() {
        match lt[i].cmp(&rt[j]) {
            std::cmp::Ordering::Less => {
                let f = f64::from(lf[i]);
                ld += term(f, f, ABSENT_IDF, lnorm);
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                let f = f64::from(rf[j]);
                rd += term(f, f, ABSENT_IDF, rnorm);
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                let a = f64::from(lf[i]);
                let b = f64::from(rf[j]);
                ln += term(a, b, PRESENT_IDF, K1);
                ld += term(a, a, PRESENT_IDF, lnorm);
                rn += term(b, a, PRESENT_IDF, K1);
                rd += term(b, b, PRESENT_IDF, rnorm);
                i += 1;
                j += 1;
            }
        }
    }
    for &f in &lf[i..] {
        let f = f64::from(f);
        ld += term(f, f, ABSENT_IDF, lnorm);
    }
    for &f in &rf[j..] {
        let f = f64::from(f);
        rd += term(f, f, ABSENT_IDF, rnorm);
    }
    let a = if ld > 0.0 {
        (ln / ld).clamp(0.0, 1.0)
    } else {
        0.0
    };
    let b = if rd > 0.0 {
        (rn / rd).clamp(0.0, 1.0)
    } else {
        0.0
    };
    a.max(b)
}

pub fn content_matches(view: &FeatureView, left: u32, right: u32) -> bool {
    content_pair_score(view, left, right) >= METADATA_THRESHOLD
}

fn term(q: f64, tf: f64, idf: f64, norm: f64) -> f64 {
    if tf == 0.0 {
        0.0
    } else {
        q * idf * (tf * (K1 + 1.0)) / (tf + norm)
    }
}

fn range(offsets: &[u64], i: usize) -> Option<std::ops::Range<usize>> {
    (i + 1 < offsets.len()).then(|| offsets[i] as usize..offsets[i + 1] as usize)
}

type TemplateParts<'a> = (&'a [u32], &'a [u32], &'a [f64], f64);
fn template_parts(view: &FeatureView, i: usize) -> Option<TemplateParts<'_>> {
    let r = range(&view.payload_template_offsets, i)?;
    let w = range(&view.prepared_weight_offsets, i)?;
    if r.len() != w.len() {
        return None;
    }
    Some((
        &view.payload_template_terms[r.clone()],
        &view.payload_template_freqs[r],
        &view.prepared_weights[w],
        view.query_denominators[i],
    ))
}

type ContentParts<'a> = (&'a [u32], &'a [u32], u32);
fn content_parts(view: &FeatureView, i: usize) -> Option<ContentParts<'_>> {
    let r = range(&view.payload_content_offsets, i)?;
    Some((
        &view.payload_content_terms[r.clone()],
        &view.payload_content_freqs[r],
        view.payload_lengths[i],
    ))
}
