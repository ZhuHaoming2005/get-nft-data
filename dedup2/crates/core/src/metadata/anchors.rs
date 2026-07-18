use crate::entity::{Contract, ContractId, compare_token_ids};
use crate::metadata::bm25::PreparedDocument;

#[derive(Clone, Debug)]
pub struct AnchorRecord {
    pub token_id: String,
    pub json: String,
    pub prepared: PreparedDocument,
}

#[derive(Clone, Debug)]
pub struct ContractAnchors {
    #[allow(dead_code)]
    pub contract_id: ContractId,
    pub anchors: Vec<AnchorRecord>,
    pub is_evm: bool,
}

pub fn select_anchors(contract: &Contract, k: usize, is_evm: bool) -> Option<ContractAnchors> {
    if contract.metadata_by_token.is_empty() || k == 0 {
        return None;
    }
    let anchors = contract
        .metadata_by_token
        .iter()
        .take(k)
        .map(|record| AnchorRecord {
            token_id: record.token_id.clone(),
            json: record.json.clone(),
            prepared: PreparedDocument::new(record.canonical_json.clone()),
        })
        .collect();
    Some(ContractAnchors {
        contract_id: contract.id,
        anchors,
        is_evm,
    })
}

pub fn largest_shared_token<'a>(
    left: &'a ContractAnchors,
    right: &'a ContractAnchors,
) -> Option<(&'a str, &'a str, &'a str)> {
    // Returns (token_id, left_json, right_json) for largest shared token.
    if !left.is_evm || !right.is_evm {
        return None;
    }
    let mut left_pos = left.anchors.len();
    let mut right_pos = right.anchors.len();
    while left_pos > 0 && right_pos > 0 {
        let left_anchor = &left.anchors[left_pos - 1];
        let right_anchor = &right.anchors[right_pos - 1];
        match compare_token_ids(&left_anchor.token_id, &right_anchor.token_id, true) {
            std::cmp::Ordering::Equal => {
                return Some((
                    left_anchor.token_id.as_str(),
                    left_anchor.json.as_str(),
                    right_anchor.json.as_str(),
                ));
            }
            std::cmp::Ordering::Greater => left_pos -= 1,
            std::cmp::Ordering::Less => right_pos -= 1,
        }
    }
    None
}

pub fn max_anchor(anchors: &ContractAnchors) -> Option<&AnchorRecord> {
    anchors
        .anchors
        .iter()
        .max_by(|a, b| compare_token_ids(&a.token_id, &b.token_id, anchors.is_evm))
}
