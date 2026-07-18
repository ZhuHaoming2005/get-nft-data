use super::{ContractAnchors, encode_term};
use dedup_model::{
    ContractId, DedupError, ErrorContext, NoopProgress, ProgressObserver, StageCounters,
};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

pub const VARIABLE_VALUE_PATHS_VERSION: u32 = 1;
pub const COLLECTION_VALUE_PATHS_VERSION: u32 = 2;
pub const TEMPLATE_SCHEMA_VERSION: u32 = 2;

#[derive(Clone, Copy, Debug)]
pub struct TemplateGuard {
    pub min_anchor_documents: usize,
    pub stable_value_min_anchors: usize,
    pub stable_value_support_ratio: f64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TemplateFingerprint {
    pub contract_id: ContractId,
    pub feature_tokens: Vec<Vec<u8>>,
    pub fingerprint_bytes: Arc<[u8]>,
    pub template_digest: [u8; 32],
    pub low_information: bool,
    pub discriminative_feature_count: usize,
}

pub fn build_template_fingerprints(
    contracts: &[ContractAnchors],
    guard: TemplateGuard,
    counters: &mut StageCounters,
) -> Result<Vec<TemplateFingerprint>, DedupError> {
    build_template_fingerprints_with_progress(contracts, guard, counters, &NoopProgress)
}

pub fn build_template_fingerprints_with_progress(
    contracts: &[ContractAnchors],
    guard: TemplateGuard,
    counters: &mut StageCounters,
    progress: &dyn ProgressObserver,
) -> Result<Vec<TemplateFingerprint>, DedupError> {
    if guard.min_anchor_documents == 0
        || guard.stable_value_min_anchors == 0
        || !(0.0..=1.0).contains(&guard.stable_value_support_ratio)
    {
        return Err(DedupError::InvalidInput {
            context: ErrorContext::stage("metadata_template"),
            message: "invalid metadata guard configuration".to_owned(),
        });
    }
    progress.begin_phase(
        "build_metadata_templates",
        u64::try_from(contracts.len()).ok(),
    );
    let templates = contracts
        .iter()
        .enumerate()
        .map(|(index, contract)| {
            if index > 0 && index % 256 == 0 {
                progress.advance(256);
                progress.check_cancelled("metadata_template")?;
            }
            let mut structure = BTreeSet::new();
            let mut stable_counts: BTreeMap<(Vec<String>, String), usize> = BTreeMap::new();
            for anchor in &contract.anchors {
                structure.extend(anchor.template_structure.iter().cloned());
                for value in anchor
                    .template_stable_values
                    .iter()
                    .cloned()
                    .collect::<BTreeSet<_>>()
                {
                    *stable_counts.entry(value).or_default() += 1;
                }
            }
            let required_by_ratio =
                (contract.anchors.len() as f64 * guard.stable_value_support_ratio).ceil() as usize;
            let required = guard
                .stable_value_min_anchors
                .max(required_by_ratio)
                .min(contract.anchors.len());
            let stable_features: BTreeSet<Vec<u8>> = stable_counts
                .into_iter()
                .filter(|(_, count)| *count >= required)
                .map(|((path, value), _)| encode_term(&path, b"stable", value.as_bytes()))
                .collect();
            let all_identical = contract
                .anchors
                .windows(2)
                .all(|pair| pair[0].canonical_bytes == pair[1].canonical_bytes);
            let has_enough_anchors = contract.anchors.len() >= guard.min_anchor_documents;
            let low_information =
                !has_enough_anchors || stable_features.is_empty() || all_identical;
            let discriminative_feature_count = stable_features.len();
            let mut feature_tokens: Vec<Vec<u8>> =
                structure.into_iter().chain(stable_features).collect();
            feature_tokens.sort();
            feature_tokens.dedup();
            let fingerprint_bytes: Arc<[u8]> = encode_fingerprint(&feature_tokens).into();
            let template_digest = Sha256::digest(&fingerprint_bytes).into();
            counters.metadata_template_features(u64::try_from(feature_tokens.len()).map_err(
                |_| DedupError::CounterOverflow {
                    counter: "metadata_template_features",
                },
            )?)?;
            if low_information {
                counters.metadata_low_information_contracts(1)?;
            }
            Ok::<_, DedupError>(TemplateFingerprint {
                contract_id: contract.contract_id,
                feature_tokens,
                fingerprint_bytes,
                template_digest,
                low_information,
                discriminative_feature_count,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    progress.advance(u64::try_from(contracts.len() % 256).unwrap_or(0));
    progress.check_cancelled("metadata_template")?;
    Ok(templates)
}

pub(crate) fn is_collection_stable_path(path: &[String]) -> bool {
    let Some(last) = path.last().map(String::as_str) else {
        return false;
    };
    let nested_creator_address = matches!(last, "address" | "creator")
        && path[..path.len() - 1]
            .iter()
            .any(|component| matches!(component.as_str(), "creator" | "creators"));
    matches!(
        last,
        "collection"
            | "symbol"
            | "description"
            | "creator"
            | "creators"
            | "royalty"
            | "seller_fee_basis_points"
            | "license"
            | "external_url"
            | "image"
            | "animation_url"
    ) || nested_creator_address
        || (last == "name" && path.len() > 1 && path[path.len() - 2] == "collection")
}

pub(crate) fn stable_value(value: &str) -> String {
    if value.starts_with("ipfs://") {
        let mut parts = value.split('/');
        let scheme = parts.next().unwrap_or_default();
        let empty = parts.next().unwrap_or_default();
        let cid = parts.next().unwrap_or_default();
        return format!("{scheme}/{empty}/{cid}");
    }
    if let Some(scheme_end) = value.find("://") {
        let authority_start = scheme_end + 3;
        if let Some(path_end) = value[authority_start..].find('/') {
            let authority_end = authority_start + path_end;
            let path = value[authority_end + 1..]
                .split(['?', '#'])
                .next()
                .unwrap_or_default();
            let segments: Vec<_> = path
                .split('/')
                .filter(|segment| !segment.is_empty())
                .take(2)
                .collect();
            if segments.is_empty() {
                return value[..authority_end].to_owned();
            }
            return format!("{}/{}", &value[..authority_end], segments.join("/"));
        }
    }
    value.to_owned()
}

fn encode_fingerprint(features: &[Vec<u8>]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&TEMPLATE_SCHEMA_VERSION.to_le_bytes());
    bytes.extend_from_slice(&VARIABLE_VALUE_PATHS_VERSION.to_le_bytes());
    bytes.extend_from_slice(&COLLECTION_VALUE_PATHS_VERSION.to_le_bytes());
    for feature in features {
        bytes.extend_from_slice(&(feature.len() as u64).to_le_bytes());
        bytes.extend_from_slice(feature);
    }
    bytes
}

pub fn fingerprint_bytes_equal(left: &TemplateFingerprint, right: &TemplateFingerprint) -> bool {
    left.template_digest == right.template_digest
        && left.fingerprint_bytes == right.fingerprint_bytes
}

pub fn template_jaccard(left: &TemplateFingerprint, right: &TemplateFingerprint) -> f64 {
    let left: BTreeSet<_> = left.feature_tokens.iter().collect();
    let right: BTreeSet<_> = right.feature_tokens.iter().collect();
    let union = left.union(&right).count();
    if union == 0 {
        0.0
    } else {
        left.intersection(&right).count() as f64 / union as f64
    }
}

pub fn assert_template_contract_order(templates: &[TemplateFingerprint]) -> Result<(), DedupError> {
    if templates
        .windows(2)
        .any(|pair| pair[0].contract_id >= pair[1].contract_id)
    {
        return Err(DedupError::InvariantViolation {
            context: ErrorContext::stage("metadata_template"),
            message: "template fingerprints are not in ContractId order".to_owned(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{MetadataRecord, select_anchors};
    use dedup_model::{ChainId, StageCounters};

    fn fingerprints(contents: &[&str]) -> Vec<TemplateFingerprint> {
        let records = contents
            .iter()
            .enumerate()
            .map(|(index, content)| MetadataRecord {
                doc_id: dedup_model::MetadataDocId::new(
                    dedup_model::EntityId::try_from(index).unwrap(),
                ),
                contract_id: ContractId::new(0),
                chain_id: ChainId::new(0),
                token_id: index.to_string(),
                content: (*content).to_owned(),
            })
            .collect();
        let mut counters = StageCounters::default();
        let anchors = select_anchors(
            records,
            &BTreeSet::from([ChainId::new(0)]),
            contents.len(),
            &mut counters,
        )
        .unwrap();
        build_template_fingerprints(
            &anchors,
            TemplateGuard {
                min_anchor_documents: 2,
                stable_value_min_anchors: 2,
                stable_value_support_ratio: 0.8,
            },
            &mut counters,
        )
        .unwrap()
    }

    #[test]
    fn stable_collection_value_is_discriminative() {
        let result = fingerprints(&[
            r#"{"collection":{"name":"cats"},"name":"cat #1","image":"ipfs://cid/1"}"#,
            r#"{"collection":{"name":"cats"},"name":"cat #2","image":"ipfs://cid/2"}"#,
        ]);
        assert!(!result[0].low_information);
        assert!(result[0].discriminative_feature_count > 0);
    }

    #[test]
    fn structure_only_and_identical_placeholders_are_low_information() {
        let result = fingerprints(&[
            r#"{"name":"placeholder","attributes":[]}"#,
            r#"{"name":"placeholder","attributes":[]}"#,
        ]);
        assert!(result[0].low_information);
    }

    #[test]
    fn nested_creator_address_is_a_stable_collection_value() {
        let result = fingerprints(&[
            r#"{"name":"one","creators":[{"address":"0xabc","share":100}]}"#,
            r#"{"name":"two","creators":[{"address":"0xabc","share":100}]}"#,
        ]);
        assert!(!result[0].low_information);
        assert!(result[0].discriminative_feature_count >= 1);
    }

    #[test]
    fn http_base_keeps_discriminative_path_prefix() {
        assert_eq!(
            stable_value("https://cdn.example/collections/cats/1.json"),
            "https://cdn.example/collections/cats"
        );
        assert_ne!(
            stable_value("https://cdn.example/collections/cats/1.json"),
            stable_value("https://cdn.example/collections/dogs/1.json")
        );
    }
}
