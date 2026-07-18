use crate::metadata::anchors::{largest_shared_token, max_anchor, ContractAnchors};
use crate::metadata::bm25::{build_pair_vectors, cosine_similarity};
use crate::metadata::canonical_json::canonicalize_json;

pub fn pair_matches(
    left: &ContractAnchors,
    right: &ContractAnchors,
    threshold: f64,
) -> bool {
    let (left_json, right_json) = if let Some((_, lj, rj)) = largest_shared_token(left, right) {
        (lj.to_owned(), rj.to_owned())
    } else {
        let Some(la) = max_anchor(left) else {
            return false;
        };
        let Some(ra) = max_anchor(right) else {
            return false;
        };
        (la.json.clone(), ra.json.clone())
    };

    let Some(lc) = canonicalize_json(&left_json) else {
        return false;
    };
    let Some(rc) = canonicalize_json(&right_json) else {
        return false;
    };
    if lc == rc {
        return true;
    }
    let (lv, rv) = build_pair_vectors(&lc, &rc);
    cosine_similarity(&lv, &rv) >= threshold
}
