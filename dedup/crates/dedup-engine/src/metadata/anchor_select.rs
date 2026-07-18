use super::{
    Bm25Corpus, RawContentVector, WeightedContentVector, canonicalize_json,
    is_collection_stable_path, scalar_paths, stable_value, structural_features, vectorize_content,
};
use dedup_model::{
    ChainId, ContractId, DedupError, ErrorContext, MetadataDocId, NoopProgress, ProgressObserver,
    StageCounters,
};
use num_bigint::BigUint;
use sha2::{Digest, Sha256};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataRecord {
    pub doc_id: MetadataDocId,
    pub contract_id: ContractId,
    pub chain_id: ChainId,
    pub token_id: String,
    pub content: String,
}

#[derive(Clone, Copy, Debug)]
pub struct BorrowedMetadataRecord<'a> {
    pub doc_id: MetadataDocId,
    pub contract_id: ContractId,
    pub chain_id: ChainId,
    pub token_id: &'a str,
    pub content: &'a str,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataAnchor {
    pub doc_id: MetadataDocId,
    pub token_id: String,
    pub canonical_bytes: Vec<u8>,
    pub canonical_digest: [u8; 32],
    pub raw_vector: RawContentVector,
    pub content_vector: WeightedContentVector,
    pub template_structure: Vec<Vec<u8>>,
    pub template_stable_values: Vec<(Vec<String>, String)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContractAnchors {
    pub contract_id: ContractId,
    pub chain_id: ChainId,
    pub is_evm: bool,
    pub anchors: Vec<MetadataAnchor>,
}

/// Releases template-only parse trees and unweighted terms after fingerprints
/// have been built. Verification retains canonical bytes and weighted vectors.
pub fn release_template_scratch(contracts: &mut [ContractAnchors]) {
    for anchor in contracts
        .iter_mut()
        .flat_map(|contract| &mut contract.anchors)
    {
        anchor.raw_vector = RawContentVector::default();
        anchor.template_structure.clear();
        anchor.template_structure.shrink_to_fit();
        anchor.template_stable_values.clear();
        anchor.template_stable_values.shrink_to_fit();
    }
}

pub fn select_anchors(
    records: Vec<MetadataRecord>,
    evm_chains: &BTreeSet<ChainId>,
    anchor_count: usize,
    counters: &mut StageCounters,
) -> Result<Vec<ContractAnchors>, DedupError> {
    select_anchors_with_progress(records, evm_chains, anchor_count, counters, &NoopProgress)
}

pub fn select_anchors_with_progress(
    records: Vec<MetadataRecord>,
    evm_chains: &BTreeSet<ChainId>,
    anchor_count: usize,
    counters: &mut StageCounters,
    progress: &dyn ProgressObserver,
) -> Result<Vec<ContractAnchors>, DedupError> {
    if anchor_count == 0 {
        return Err(DedupError::InvalidInput {
            context: ErrorContext::stage("metadata_anchor"),
            message: "metadata_anchor_tokens must be positive".to_owned(),
        });
    }
    let mut grouped: BTreeMap<(ContractId, ChainId), Vec<MetadataRecord>> = BTreeMap::new();
    progress.begin_phase("group_metadata_records", u64::try_from(records.len()).ok());
    let mut grouped_work = 0_u64;
    for record in records {
        grouped_work = grouped_work.saturating_add(1);
        if grouped_work == 1_024 {
            progress.advance(grouped_work);
            progress.check_cancelled("metadata_anchor")?;
            grouped_work = 0;
        }
        grouped
            .entry((record.contract_id, record.chain_id))
            .or_default()
            .push(record);
    }
    progress.advance(grouped_work);
    progress.check_cancelled("metadata_anchor")?;
    progress.begin_phase(
        "canonicalize_metadata_anchors",
        u64::try_from(grouped.len()).ok(),
    );
    let mut pending = Vec::new();
    for ((contract_id, chain_id), mut records) in grouped {
        let is_evm = evm_chains.contains(&chain_id);
        records.sort_by(|left, right| compare_token_ids(&left.token_id, &right.token_id, is_evm));
        let mut anchors = Vec::with_capacity(anchor_count.min(records.len()));
        for record in records {
            let canonical_value = match canonicalize_json(&record.content) {
                Ok(value) => value,
                Err(DedupError::InvalidMetadata { .. }) => continue,
                Err(error) => return Err(error),
            };
            let canonical_bytes = canonical_value.canonical_bytes();
            let canonical_digest = Sha256::digest(&canonical_bytes).into();
            let raw_vector = vectorize_content(&canonical_value)?;
            let template_structure = structural_features(&canonical_value).into_iter().collect();
            let template_stable_values = scalar_paths(&canonical_value)
                .into_iter()
                .filter(|(path, _)| is_collection_stable_path(path))
                .map(|(path, value)| (path, stable_value(&value)))
                .collect();
            anchors.push((
                record.doc_id,
                record.token_id,
                canonical_bytes,
                canonical_digest,
                raw_vector,
                template_structure,
                template_stable_values,
            ));
            counters.metadata_anchor_documents(1)?;
            if anchors.len() == anchor_count {
                break;
            }
        }
        if !anchors.is_empty() {
            pending.push((contract_id, chain_id, is_evm, anchors));
        }
        progress.advance(1);
        progress.check_cancelled("metadata_anchor")?;
    }
    finalize_pending_anchors(pending)
}

pub fn select_anchors_from_sorted_records<'a>(
    records: impl IntoIterator<Item = Result<BorrowedMetadataRecord<'a>, DedupError>>,
    record_count: u64,
    evm_chains: &BTreeSet<ChainId>,
    anchor_count: usize,
    counters: &mut StageCounters,
    progress: &dyn ProgressObserver,
) -> Result<Vec<ContractAnchors>, DedupError> {
    if anchor_count == 0 {
        return Err(DedupError::InvalidInput {
            context: ErrorContext::stage("metadata_anchor"),
            message: "metadata_anchor_tokens must be positive".to_owned(),
        });
    }
    progress.begin_phase("select_bounded_metadata_anchors", Some(record_count));
    let mut pending = Vec::new();
    let mut current_key: Option<(ContractId, ChainId)> = None;
    let mut current_is_evm = false;
    let mut current_anchors = Vec::with_capacity(anchor_count);
    let mut previous_token = String::new();
    let mut work = 0_u64;
    for record in records {
        let record = record?;
        work = work.saturating_add(1);
        if work == 1_024 {
            progress.advance(work);
            progress.check_cancelled("metadata_anchor")?;
            work = 0;
        }
        let key = (record.contract_id, record.chain_id);
        if current_key != Some(key) {
            if let Some(previous_key) = current_key {
                if key <= previous_key {
                    return Err(DedupError::InvariantViolation {
                        context: ErrorContext::stage("metadata_anchor"),
                        message: "metadata records are not ordered by ContractId".to_owned(),
                    });
                }
                if !current_anchors.is_empty() {
                    pending.push((
                        previous_key.0,
                        previous_key.1,
                        current_is_evm,
                        std::mem::take(&mut current_anchors),
                    ));
                }
            }
            current_key = Some(key);
            current_is_evm = evm_chains.contains(&record.chain_id);
            previous_token.clear();
        } else if !previous_token.is_empty()
            && compare_token_ids(&previous_token, record.token_id, current_is_evm)
                == Ordering::Greater
        {
            return Err(DedupError::InvariantViolation {
                context: ErrorContext::stage("metadata_anchor"),
                message: "metadata records are not ordered by token id".to_owned(),
            });
        }
        previous_token.clear();
        previous_token.push_str(record.token_id);
        if current_anchors.len() >= anchor_count {
            continue;
        }
        let canonical_value = match canonicalize_json(record.content) {
            Ok(value) => value,
            Err(DedupError::InvalidMetadata { .. }) => continue,
            Err(error) => return Err(error),
        };
        let canonical_bytes = canonical_value.canonical_bytes();
        let canonical_digest = Sha256::digest(&canonical_bytes).into();
        let raw_vector = vectorize_content(&canonical_value)?;
        let template_structure = structural_features(&canonical_value).into_iter().collect();
        let template_stable_values = scalar_paths(&canonical_value)
            .into_iter()
            .filter(|(path, _)| is_collection_stable_path(path))
            .map(|(path, value)| (path, stable_value(&value)))
            .collect();
        current_anchors.push((
            record.doc_id,
            record.token_id.to_owned(),
            canonical_bytes,
            canonical_digest,
            raw_vector,
            template_structure,
            template_stable_values,
        ));
        counters.metadata_anchor_documents(1)?;
    }
    progress.advance(work);
    progress.check_cancelled("metadata_anchor")?;
    if let Some(key) = current_key
        && !current_anchors.is_empty()
    {
        pending.push((key.0, key.1, current_is_evm, current_anchors));
    }
    finalize_pending_anchors(pending)
}

type PendingAnchor = (
    MetadataDocId,
    String,
    Vec<u8>,
    [u8; 32],
    RawContentVector,
    Vec<Vec<u8>>,
    Vec<(Vec<String>, String)>,
);
type PendingContractAnchors = (ContractId, ChainId, bool, Vec<PendingAnchor>);

fn finalize_pending_anchors(
    pending: Vec<PendingContractAnchors>,
) -> Result<Vec<ContractAnchors>, DedupError> {
    if pending.iter().all(|(_, _, _, anchors)| anchors.is_empty()) {
        return Ok(Vec::new());
    }
    let corpus = Bm25Corpus::build_from_refs(
        pending
            .iter()
            .flat_map(|(_, _, _, anchors)| anchors.iter().map(|anchor| &anchor.4)),
    )?;
    pending
        .into_iter()
        .map(|(contract_id, chain_id, is_evm, anchors)| {
            let anchors = anchors
                .into_iter()
                .map(
                    |(
                        doc_id,
                        token_id,
                        canonical_bytes,
                        canonical_digest,
                        raw_vector,
                        template_structure,
                        template_stable_values,
                    )| {
                        let content_vector = corpus.weight(&raw_vector)?;
                        Ok(MetadataAnchor {
                            doc_id,
                            token_id,
                            canonical_bytes,
                            canonical_digest,
                            raw_vector,
                            content_vector,
                            template_structure,
                            template_stable_values,
                        })
                    },
                )
                .collect::<Result<_, DedupError>>()?;
            Ok(ContractAnchors {
                contract_id,
                chain_id,
                is_evm,
                anchors,
            })
        })
        .collect()
}

pub fn compare_token_ids(left: &str, right: &str, is_evm: bool) -> Ordering {
    if is_evm {
        match (left.parse::<BigUint>(), right.parse::<BigUint>()) {
            (Ok(left), Ok(right)) => left.cmp(&right),
            (Ok(_), Err(_)) => Ordering::Less,
            (Err(_), Ok(_)) => Ordering::Greater,
            (Err(_), Err(_)) => left.cmp(right),
        }
    } else {
        left.cmp(right)
    }
}

pub fn require_valid_evm_token_ids(contracts: &[ContractAnchors]) -> Result<(), DedupError> {
    for contract in contracts.iter().filter(|contract| contract.is_evm) {
        for anchor in &contract.anchors {
            anchor
                .token_id
                .parse::<BigUint>()
                .map_err(|_| DedupError::InvalidInput {
                    context: ErrorContext {
                        stage: "metadata_anchor",
                        partition: None,
                        stable_object_id: Some(contract.contract_id.as_u64()),
                    },
                    message: format!("invalid EVM token id {:?}", anchor.token_id),
                })?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evm_anchor_order_is_arbitrary_precision_numeric() {
        let records = ["10", "2", "999999999999999999999999999999"]
            .into_iter()
            .enumerate()
            .map(|(index, token_id)| MetadataRecord {
                doc_id: MetadataDocId::new(dedup_model::EntityId::try_from(index).unwrap()),
                contract_id: ContractId::new(0),
                chain_id: ChainId::new(0),
                token_id: token_id.to_owned(),
                content: format!(r#"{{"token":"{token_id}"}}"#),
            })
            .collect();
        let mut counters = StageCounters::default();
        let result = select_anchors(
            records,
            &BTreeSet::from([ChainId::new(0)]),
            2,
            &mut counters,
        )
        .unwrap();
        assert_eq!(result[0].anchors[0].token_id, "2");
        assert_eq!(result[0].anchors[1].token_id, "10");
        assert_eq!(counters.metadata_anchor_documents, 2);
    }

    #[test]
    fn canonicalization_work_is_bounded_by_k_times_contracts() {
        let contract_count = 3_u32;
        let anchors_per_contract = 4_usize;
        let records = (0..contract_count)
            .flat_map(|contract| {
                let record_count = if contract == 0 { 10_000 } else { 100 };
                (0..record_count).map(move |token| MetadataRecord {
                    doc_id: MetadataDocId::new(dedup_model::EntityId::try_from(token).unwrap()),
                    contract_id: ContractId::new(dedup_model::EntityId::from(contract)),
                    chain_id: ChainId::new(0),
                    token_id: token.to_string(),
                    content: format!(r#"{{"collection":"stable","token":{token}}}"#),
                })
            })
            .collect();
        let mut counters = StageCounters::default();
        let result = select_anchors(
            records,
            &BTreeSet::from([ChainId::new(0)]),
            anchors_per_contract,
            &mut counters,
        )
        .unwrap();
        assert_eq!(result.len(), usize::try_from(contract_count).unwrap());
        assert_eq!(
            counters.metadata_anchor_documents,
            u64::from(contract_count) * u64::try_from(anchors_per_contract).unwrap()
        );
        assert!(
            result
                .iter()
                .all(|contract| contract.anchors.len() == anchors_per_contract)
        );
    }
}
