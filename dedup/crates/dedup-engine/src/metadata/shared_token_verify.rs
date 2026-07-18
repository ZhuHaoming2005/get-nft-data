use super::{ContractAnchors, MetadataAnchor, cosine_similarity};
use dedup_model::{DedupError, StageCounters};
use num_bigint::BigUint;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TokenSelection {
    Shared(String),
    MaxFallback { left: String, right: String },
}

#[derive(Clone, Debug, PartialEq)]
pub struct VerificationResult {
    pub matched: bool,
    pub selection: TokenSelection,
    pub byte_identical: bool,
    pub similarity: f64,
}

pub fn verify_metadata_pair(
    left: &ContractAnchors,
    right: &ContractAnchors,
    threshold: f64,
    counters: &mut StageCounters,
) -> Result<VerificationResult, DedupError> {
    let (left_anchor, right_anchor, selection) = select_comparison(left, right, counters)?;
    if left_anchor.canonical_digest == right_anchor.canonical_digest
        && left_anchor.canonical_bytes == right_anchor.canonical_bytes
    {
        return Ok(VerificationResult {
            matched: true,
            selection,
            byte_identical: true,
            similarity: 1.0,
        });
    }
    let (similarity, comparisons) =
        cosine_similarity(&left_anchor.content_vector, &right_anchor.content_vector)?;
    counters.bm25_term_comparisons(comparisons)?;
    Ok(VerificationResult {
        matched: similarity >= threshold,
        selection,
        byte_identical: false,
        similarity,
    })
}

fn select_comparison<'a>(
    left: &'a ContractAnchors,
    right: &'a ContractAnchors,
    counters: &mut StageCounters,
) -> Result<(&'a MetadataAnchor, &'a MetadataAnchor, TokenSelection), DedupError> {
    let left_max = left
        .anchors
        .last()
        .expect("contracts contain at least one anchor");
    let right_max = right
        .anchors
        .last()
        .expect("contracts contain at least one anchor");
    if !(left.is_evm && right.is_evm) {
        return Ok((
            left_max,
            right_max,
            TokenSelection::MaxFallback {
                left: left_max.token_id.clone(),
                right: right_max.token_id.clone(),
            },
        ));
    }
    let mut left_index = 0;
    let mut right_index = 0;
    let mut shared = None;
    while left_index < left.anchors.len() && right_index < right.anchors.len() {
        counters.token_id_comparisons(1)?;
        let left_id = left.anchors[left_index]
            .token_id
            .parse::<BigUint>()
            .expect("validated EVM token IDs");
        let right_id = right.anchors[right_index]
            .token_id
            .parse::<BigUint>()
            .expect("validated EVM token IDs");
        match left_id.cmp(&right_id) {
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
            std::cmp::Ordering::Equal => {
                shared = Some((left_index, right_index));
                left_index += 1;
                right_index += 1;
            }
        }
    }
    if let Some((left_index, right_index)) = shared {
        let token_id = left.anchors[left_index].token_id.clone();
        Ok((
            &left.anchors[left_index],
            &right.anchors[right_index],
            TokenSelection::Shared(token_id),
        ))
    } else {
        Ok((
            left_max,
            right_max,
            TokenSelection::MaxFallback {
                left: left_max.token_id.clone(),
                right: right_max.token_id.clone(),
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{MetadataRecord, select_anchors};
    use dedup_model::{ChainId, ContractId};
    use std::collections::BTreeSet;

    fn contracts(left_tokens: &[&str], right_tokens: &[&str], evm: bool) -> Vec<ContractAnchors> {
        let records = [left_tokens, right_tokens]
            .into_iter()
            .enumerate()
            .flat_map(|(contract, tokens)| {
                tokens.iter().map(move |token| MetadataRecord {
                    doc_id: dedup_model::MetadataDocId::new(dedup_model::EntityId::from(
                        contract as u32,
                    )),
                    contract_id: ContractId::new(dedup_model::EntityId::from(contract as u32)),
                    chain_id: ChainId::new(contract as u16),
                    token_id: (*token).to_owned(),
                    content: format!(r#"{{"collection":"same","token":"{token}"}}"#),
                })
            })
            .collect();
        select_anchors(
            records,
            &if evm {
                BTreeSet::from([ChainId::new(0), ChainId::new(1)])
            } else {
                BTreeSet::new()
            },
            8,
            &mut StageCounters::default(),
        )
        .unwrap()
    }

    #[test]
    fn largest_shared_evm_token_is_selected_numerically() {
        let contracts = contracts(&["2", "10"], &["2", "10"], true);
        let result = verify_metadata_pair(
            &contracts[0],
            &contracts[1],
            0.6,
            &mut StageCounters::default(),
        )
        .unwrap();
        assert_eq!(result.selection, TokenSelection::Shared("10".to_owned()));
    }

    #[test]
    fn solana_never_builds_an_intersection() {
        let contracts = contracts(&["a", "z"], &["a", "y"], false);
        let mut counters = StageCounters::default();
        let result =
            verify_metadata_pair(&contracts[0], &contracts[1], 0.6, &mut counters).unwrap();
        assert_eq!(
            result.selection,
            TokenSelection::MaxFallback {
                left: "z".to_owned(),
                right: "y".to_owned()
            }
        );
        assert_eq!(counters.token_id_comparisons, 0);
    }

    #[test]
    fn evm_without_shared_token_uses_each_contract_maximum() {
        let contracts = contracts(&["2", "10"], &["3", "11"], true);
        let mut counters = StageCounters::default();
        let result =
            verify_metadata_pair(&contracts[0], &contracts[1], 0.6, &mut counters).unwrap();

        assert_eq!(
            result.selection,
            TokenSelection::MaxFallback {
                left: "10".to_owned(),
                right: "11".to_owned(),
            }
        );
        assert!(counters.token_id_comparisons > 0);
    }
}
