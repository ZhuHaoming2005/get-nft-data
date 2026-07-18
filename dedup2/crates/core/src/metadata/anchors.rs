use crate::entity::{Contract, ContractId};
use num_bigint::BigUint;
use std::cmp::Ordering;
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub struct AnchorRecord {
    pub token_id: String,
    pub json: String,
}

#[derive(Clone, Debug)]
pub struct ContractAnchors {
    #[allow(dead_code)]
    pub contract_id: ContractId,
    pub chain_id: crate::entity::ChainId,
    pub anchors: Vec<AnchorRecord>,
    pub is_evm: bool,
}

pub fn select_anchors(
    contract: &Contract,
    k: usize,
    is_evm: bool,
) -> Option<ContractAnchors> {
    if contract.metadata_by_token.is_empty() || k == 0 {
        return None;
    }
    let mut items: Vec<(String, String)> = contract
        .metadata_by_token
        .iter()
        .map(|(t, j)| (t.clone(), j.clone()))
        .collect();
    items.sort_by(|(a, _), (b, _)| compare_token_ids(a, b, is_evm));
    items.truncate(k);
    Some(ContractAnchors {
        contract_id: contract.id,
        chain_id: contract.chain_id,
        anchors: items
            .into_iter()
            .map(|(token_id, json)| AnchorRecord { token_id, json })
            .collect(),
        is_evm,
    })
}

pub fn compare_token_ids(left: &str, right: &str, is_evm: bool) -> Ordering {
    if is_evm {
        match (parse_evm_token(left), parse_evm_token(right)) {
            (Some(a), Some(b)) => a.cmp(&b),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => left.cmp(right),
        }
    } else {
        left.cmp(right)
    }
}

fn parse_evm_token(token: &str) -> Option<BigUint> {
    let trimmed = token.trim();
    if trimmed.is_empty() {
        return None;
    }
    BigUint::parse_bytes(trimmed.as_bytes(), 10)
}

pub fn largest_shared_token<'a>(
    left: &'a ContractAnchors,
    right: &'a ContractAnchors,
) -> Option<(&'a str, &'a str, &'a str)> {
    // Returns (token_id, left_json, right_json) for largest shared token.
    if !left.is_evm || !right.is_evm {
        return None;
    }
    let right_map: BTreeMap<&str, &str> = right
        .anchors
        .iter()
        .map(|a| (a.token_id.as_str(), a.json.as_str()))
        .collect();
    let mut best: Option<(&str, &str, &str)> = None;
    for anchor in &left.anchors {
        if let Some(rj) = right_map.get(anchor.token_id.as_str()) {
            let replace = match best {
                None => true,
                Some((prev, _, _)) => {
                    compare_token_ids(anchor.token_id.as_str(), prev, true) == Ordering::Greater
                }
            };
            if replace {
                best = Some((anchor.token_id.as_str(), anchor.json.as_str(), rj));
            }
        }
    }
    best
}

pub fn max_anchor(anchors: &ContractAnchors) -> Option<&AnchorRecord> {
    anchors.anchors.iter().max_by(|a, b| {
        compare_token_ids(&a.token_id, &b.token_id, anchors.is_evm)
    })
}
