use crate::metadata::anchors::{ContractAnchors, largest_shared_anchors, max_anchor};
use crate::metadata::bm25::cosine_similarity;

pub fn pair_matches(
    left: &ContractAnchors<'_>,
    right: &ContractAnchors<'_>,
    threshold: f64,
) -> bool {
    let (left_document, right_document) =
        if let Some((left_anchor, right_anchor)) = largest_shared_anchors(left, right) {
            (&left_anchor.prepared, &right_anchor.prepared)
        } else {
            let Some(la) = max_anchor(left) else {
                return false;
            };
            let Some(ra) = max_anchor(right) else {
                return false;
            };
            (&la.prepared, &ra.prepared)
        };
    if left_document.canonical == right_document.canonical {
        return true;
    }
    cosine_similarity(left_document, right_document) >= threshold
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::anchors::{AnchorRecord, ContractAnchors};

    fn anchor<'a>(token_id: &'a str, canonical: &'a str) -> AnchorRecord<'a> {
        AnchorRecord::new(token_id, canonical, canonical, true)
    }

    #[test]
    fn evm_uses_largest_shared_token() {
        let left = ContractAnchors {
            contract_id: 0,
            anchors: vec![
                anchor("1", r#"{"name":"different"}"#),
                anchor("2", r#"{"name":"same"}"#),
            ],
            is_evm: true,
        };
        let right = ContractAnchors {
            contract_id: 1,
            anchors: vec![
                anchor("1", r#"{"name":"other"}"#),
                anchor("2", r#"{"name":"same"}"#),
            ],
            is_evm: true,
        };
        assert!(pair_matches(&left, &right, 0.99));
    }

    #[test]
    fn solana_uses_each_side_largest_anchor() {
        let left = ContractAnchors {
            contract_id: 0,
            anchors: vec![
                anchor("A", r#"{"name":"old"}"#),
                anchor("Z", r#"{"name":"same"}"#),
            ],
            is_evm: false,
        };
        let right = ContractAnchors {
            contract_id: 1,
            anchors: vec![
                anchor("B", r#"{"name":"old2"}"#),
                anchor("Y", r#"{"name":"same"}"#),
            ],
            is_evm: false,
        };
        assert!(pair_matches(&left, &right, 0.99));
    }
}
