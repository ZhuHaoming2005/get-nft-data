use crate::entity::{Contract, ContractId, compare_token_ids};
use crate::metadata::bm25::PreparedDocument;

#[derive(Clone, Debug)]
pub struct AnchorRecord<'a> {
    pub token_id: &'a str,
    pub json: &'a str,
    numeric_token_id: Option<&'a str>,
    pub prepared: PreparedDocument<'a>,
}

impl<'a> AnchorRecord<'a> {
    pub(crate) fn new(
        token_id: &'a str,
        json: &'a str,
        canonical_json: &'a str,
        is_evm: bool,
    ) -> Self {
        Self {
            token_id,
            json,
            numeric_token_id: if is_evm {
                decimal_magnitude(token_id)
            } else {
                None
            },
            prepared: PreparedDocument::new(canonical_json),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ContractAnchors<'a> {
    #[allow(dead_code)]
    pub contract_id: ContractId,
    pub anchors: Vec<AnchorRecord<'a>>,
    pub is_evm: bool,
}

pub fn select_anchors(contract: &Contract, k: usize, is_evm: bool) -> Option<ContractAnchors<'_>> {
    if contract.metadata_by_token.is_empty() || k == 0 {
        return None;
    }
    let anchors = contract
        .metadata_by_token
        .iter()
        .take(k)
        .map(|record| {
            AnchorRecord::new(
                &record.token_id,
                &record.json,
                &record.canonical_json,
                is_evm,
            )
        })
        .collect();
    Some(ContractAnchors {
        contract_id: contract.id,
        anchors,
        is_evm,
    })
}

pub fn largest_shared_anchors<'left, 'right, 'data>(
    left: &'left ContractAnchors<'data>,
    right: &'right ContractAnchors<'data>,
) -> Option<(&'left AnchorRecord<'data>, &'right AnchorRecord<'data>)> {
    if !left.is_evm || !right.is_evm {
        return None;
    }
    let mut left_pos = left.anchors.len();
    let mut right_pos = right.anchors.len();
    while left_pos > 0 && right_pos > 0 {
        let left_anchor = &left.anchors[left_pos - 1];
        let right_anchor = &right.anchors[right_pos - 1];
        match compare_anchor_token_ids(left_anchor, right_anchor) {
            std::cmp::Ordering::Equal => return Some((left_anchor, right_anchor)),
            std::cmp::Ordering::Greater => left_pos -= 1,
            std::cmp::Ordering::Less => right_pos -= 1,
        }
    }
    None
}

fn compare_anchor_token_ids(
    left: &AnchorRecord<'_>,
    right: &AnchorRecord<'_>,
) -> std::cmp::Ordering {
    match (&left.numeric_token_id, &right.numeric_token_id) {
        (Some(left), Some(right)) => left.len().cmp(&right.len()).then(left.cmp(right)),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => compare_token_ids(left.token_id, right.token_id, false),
    }
}

fn decimal_magnitude(token_id: &str) -> Option<&str> {
    let trimmed = token_id.trim();
    if trimmed.is_empty() || !trimmed.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    Some(trimmed.trim_start_matches('0'))
}

pub fn max_anchor<'borrow, 'data>(
    anchors: &'borrow ContractAnchors<'data>,
) -> Option<&'borrow AnchorRecord<'data>> {
    anchors.anchors.last()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::{MetadataRecord, SourceOrder};

    fn source_order() -> SourceOrder {
        SourceOrder {
            file_ordinal: 0,
            file_row_number: 0,
        }
    }

    #[test]
    fn selected_anchors_borrow_store_strings_and_cache_numeric_token() {
        let contract = Contract {
            id: 0,
            chain_id: 0,
            address: "0x1".to_owned(),
            name_norm: None,
            nft_count: 1,
            metadata_by_token: vec![MetadataRecord {
                token_id: "340282366920938463463374607431768211456".to_owned(),
                json: r#"{"name":"Alpha"}"#.to_owned(),
                canonical_json: r#"{"name":"alpha"}"#.to_owned(),
                source_order: source_order(),
            }],
        };
        let anchors = select_anchors(&contract, 1, true).unwrap();
        let selected = &anchors.anchors[0];
        let stored = &contract.metadata_by_token[0];
        assert!(std::ptr::eq(
            selected.token_id.as_ptr(),
            stored.token_id.as_ptr()
        ));
        assert!(std::ptr::eq(selected.json.as_ptr(), stored.json.as_ptr()));
        assert!(std::ptr::eq(
            selected.prepared.canonical.as_ptr(),
            stored.canonical_json.as_ptr()
        ));
        assert!(selected.numeric_token_id.is_some());
    }

    #[test]
    fn cached_evm_comparison_matches_original_ordering() {
        let canonical = "{}";
        let values = [
            "2",
            "10",
            "00010",
            "  10 ",
            "0",
            "000",
            "340282366920938463463374607431768211456",
            "invalid",
            "z-invalid",
        ];
        for left in values {
            for right in values {
                let left_anchor = AnchorRecord::new(left, canonical, canonical, true);
                let right_anchor = AnchorRecord::new(right, canonical, canonical, true);
                assert_eq!(
                    compare_anchor_token_ids(&left_anchor, &right_anchor),
                    compare_token_ids(left, right, true)
                );
            }
        }
    }
}
