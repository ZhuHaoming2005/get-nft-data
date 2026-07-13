use std::collections::{BTreeMap, BTreeSet};

use crate::models::{
    normalize_chain_identity, AddressAttributionPayload, AddressEvidenceFeaturePayload,
    CampaignClusterPayload, ContentSimilarityEdgePayload, ContractLifecycleEventPayload,
    ContractLifecycleMetricPayload, DuplicateCandidate, DuplicateContractPayload,
    EarlyDetectionFeaturePayload, NftPropagationEdgePayload, NftPropagationPathPayload,
    SeedContractPayload, ValueFlowEdgePayload, WeakSupervisionLabelPayload,
};

mod clusters;
mod events;
mod evidence;
mod labels;
mod metrics;
mod value_flow;

#[cfg(test)]
mod tests;

use clusters::build_campaign_clusters;
use events::build_contract_lifecycle_events;
use evidence::{build_address_evidence_features, build_content_similarity_edges};
use labels::{build_early_detection_features, build_weak_supervision_labels};
use metrics::build_lifecycle_metrics;
use value_flow::build_value_flow_edges;
pub struct LifecycleModelInput<'a> {
    pub seed_contract: &'a SeedContractPayload,
    pub duplicate_candidates: &'a [DuplicateCandidate],
    pub duplicate_contracts: &'a [DuplicateContractPayload],
    pub address_attributions: &'a [AddressAttributionPayload],
    pub nft_propagation_paths: &'a BTreeMap<String, NftPropagationPathPayload>,
    pub mint_payment_edges: &'a [ValueFlowEdgePayload],
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct LifecycleModelOutputs {
    pub contract_lifecycle_events: Vec<ContractLifecycleEventPayload>,
    pub address_evidence_features: Vec<AddressEvidenceFeaturePayload>,
    pub value_flow_edges: Vec<ValueFlowEdgePayload>,
    pub content_similarity_edges: Vec<ContentSimilarityEdgePayload>,
    pub campaign_clusters: Vec<CampaignClusterPayload>,
    pub lifecycle_metrics: Vec<ContractLifecycleMetricPayload>,
    pub weak_supervision_labels: Vec<WeakSupervisionLabelPayload>,
    pub early_detection_features: Vec<EarlyDetectionFeaturePayload>,
}

pub fn build_lifecycle_model_outputs(input: LifecycleModelInput<'_>) -> LifecycleModelOutputs {
    let address_evidence_features = build_address_evidence_features(input.address_attributions);
    let content_similarity_edges =
        build_content_similarity_edges(input.seed_contract, input.duplicate_candidates);
    let value_flow_edges =
        build_value_flow_edges(input.nft_propagation_paths, input.mint_payment_edges);
    let contract_lifecycle_events = build_contract_lifecycle_events(
        input.seed_contract,
        input.duplicate_contracts,
        &content_similarity_edges,
        &address_evidence_features,
        input.nft_propagation_paths,
        &value_flow_edges,
    );
    let lifecycle_metrics = build_lifecycle_metrics(
        input.seed_contract,
        input.nft_propagation_paths,
        &address_evidence_features,
        &value_flow_edges,
        &contract_lifecycle_events,
    );
    let campaign_clusters = build_campaign_clusters(
        input.seed_contract,
        input.duplicate_contracts,
        &address_evidence_features,
        &value_flow_edges,
        &contract_lifecycle_events,
    );
    let weak_supervision_labels = build_weak_supervision_labels(
        &lifecycle_metrics,
        &address_evidence_features,
        &content_similarity_edges,
    );
    let early_detection_features = build_early_detection_features(
        &lifecycle_metrics,
        &contract_lifecycle_events,
        &value_flow_edges,
    );

    LifecycleModelOutputs {
        contract_lifecycle_events,
        address_evidence_features,
        value_flow_edges,
        content_similarity_edges,
        campaign_clusters,
        lifecycle_metrics,
        weak_supervision_labels,
        early_detection_features,
    }
}
