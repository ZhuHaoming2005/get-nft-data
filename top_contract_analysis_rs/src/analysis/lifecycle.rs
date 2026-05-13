use std::collections::{BTreeMap, BTreeSet};

use crate::models::{
    AddressAttributionPayload, AddressEvidenceFeaturePayload, CampaignClusterPayload,
    ContentSimilarityEdgePayload, ContractLifecycleEventPayload, ContractLifecycleMetricPayload,
    DuplicateCandidate, DuplicateContractPayload, EarlyDetectionFeaturePayload,
    NftMarketEventRecord, NftPropagationEdgePayload, NftPropagationPathPayload,
    SeedContractPayload, ValueFlowEdgePayload, WeakSupervisionLabelPayload,
};

const VALUE_FLOW_COVERAGE_SCOPE: &str = "same_tx_native_eth_and_stablecoin_erc20";
const VALUE_FLOW_COVERAGE_GAPS: [&str; 3] = [
    "later_withdrawals_not_traced",
    "multi_hop_cashout_not_traced",
    "cex_bridge_mixer_not_classified",
];

pub struct LifecycleModelInput<'a> {
    pub seed_contract: &'a SeedContractPayload,
    pub duplicate_candidates: &'a [DuplicateCandidate],
    pub duplicate_contracts: &'a [DuplicateContractPayload],
    pub address_attributions: &'a [AddressAttributionPayload],
    pub nft_propagation_paths: &'a BTreeMap<String, NftPropagationPathPayload>,
    pub mint_payment_edges: &'a [ValueFlowEdgePayload],
    pub market_events: &'a [NftMarketEventRecord],
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
        input.market_events,
    );
    let lifecycle_metrics = build_lifecycle_metrics(
        input.seed_contract,
        input.nft_propagation_paths,
        input.market_events,
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

fn build_address_evidence_features(
    attributions: &[AddressAttributionPayload],
) -> Vec<AddressEvidenceFeaturePayload> {
    let mut rows: Vec<_> = attributions
        .iter()
        .map(|item| {
            let related_tokens: BTreeSet<String> = item
                .evidence
                .iter()
                .filter(|evidence| !evidence.token_id.is_empty())
                .map(|evidence| evidence.token_id.clone())
                .collect();
            let related_txs: BTreeSet<String> = item
                .evidence
                .iter()
                .filter(|evidence| !evidence.tx_hash.is_empty())
                .map(|evidence| evidence.tx_hash.clone())
                .collect();
            AddressEvidenceFeaturePayload {
                contract_address: item.contract_address.clone(),
                address: item.address.clone(),
                observed_roles: item.observed_roles.clone(),
                attribution_label: if item.attribution_label.is_empty() {
                    "neutral_participant".into()
                } else {
                    item.attribution_label.clone()
                },
                operator_score: item.operator_score,
                colluder_score: item.colluder_score,
                victim_score: item.victim_score,
                corruption_score: item.corruption_score,
                neutral_score: item.neutral_score,
                confidence: item.confidence.clone(),
                evidence_count: item.evidence.len() as i64,
                related_token_count: related_tokens.len() as i64,
                related_tx_count: related_txs.len() as i64,
                evidence: item.evidence.clone(),
            }
        })
        .collect();
    rows.sort_by(|left, right| {
        (
            left.contract_address.as_str(),
            left.address.as_str(),
            left.attribution_label.as_str(),
        )
            .cmp(&(
                right.contract_address.as_str(),
                right.address.as_str(),
                right.attribution_label.as_str(),
            ))
    });
    rows
}

fn build_content_similarity_edges(
    seed_contract: &SeedContractPayload,
    candidates: &[DuplicateCandidate],
) -> Vec<ContentSimilarityEdgePayload> {
    let mut rows: Vec<_> = candidates
        .iter()
        .filter(|candidate| !candidate.contract_address.is_empty())
        .map(|candidate| {
            let confidence = if candidate.confidence.trim().is_empty() {
                infer_similarity_confidence(&candidate.match_reasons)
            } else {
                candidate.confidence.clone()
            };
            ContentSimilarityEdgePayload {
                edge_id: format!(
                    "content:{}:{}:{}",
                    seed_contract.contract_address, candidate.contract_address, candidate.token_id
                ),
                seed_contract_address: seed_contract.contract_address.clone(),
                candidate_contract_address: candidate.contract_address.clone(),
                token_id: candidate.token_id.clone(),
                match_reasons: candidate.match_reasons.clone(),
                confidence,
                evidence_type: "content_similarity".into(),
            }
        })
        .collect();
    rows.sort_by(|left, right| {
        (
            left.candidate_contract_address.as_str(),
            left.token_id.as_str(),
        )
            .cmp(&(
                right.candidate_contract_address.as_str(),
                right.token_id.as_str(),
            ))
    });
    rows
}

fn infer_similarity_confidence(match_reasons: &[String]) -> String {
    if match_reasons.iter().any(|reason| {
        matches!(
            reason.as_str(),
            "token_uri_match" | "image_uri_match" | "metadata_match"
        )
    }) {
        "high".into()
    } else if match_reasons
        .iter()
        .any(|reason| reason.contains("name") || reason.contains("symbol"))
    {
        "medium".into()
    } else {
        "low".into()
    }
}

fn build_value_flow_edges(
    propagation_paths: &BTreeMap<String, NftPropagationPathPayload>,
    mint_payment_edges: &[ValueFlowEdgePayload],
) -> Vec<ValueFlowEdgePayload> {
    let mut rows = mint_payment_edges.to_vec();
    for path in propagation_paths.values() {
        for edge in &path.edges {
            if edge.channel != "sale" {
                continue;
            }
            rows.push(sale_value_edge(SaleValueEdgeInput {
                edge,
                channel: "sale_payment",
                to_address: &edge.from_address,
                evidence_type: "marketplace_sale",
                from_role: "buyer",
                to_role: "seller",
                recipient_known: true,
                value_eth: edge.seller_fee_eth.or(edge.price_eth),
                value_usd: edge.seller_fee_usd.or(edge.price_usd),
            }));
            if edge.protocol_fee_eth.unwrap_or(0.0) > 0.0
                || edge.protocol_fee_usd.unwrap_or(0.0) > 0.0
            {
                rows.push(sale_value_edge(SaleValueEdgeInput {
                    edge,
                    channel: "protocol_fee",
                    to_address: &unknown_value_recipient("protocol_fee", &edge.marketplace),
                    evidence_type: "marketplace_protocol_fee",
                    from_role: "buyer",
                    to_role: "marketplace_protocol",
                    recipient_known: false,
                    value_eth: edge.protocol_fee_eth,
                    value_usd: edge.protocol_fee_usd,
                }));
            }
            if edge.royalty_fee_eth.unwrap_or(0.0) > 0.0
                || edge.royalty_fee_usd.unwrap_or(0.0) > 0.0
            {
                let royalty_recipient = edge.royalty_recipient_address.trim();
                let royalty_recipient_known = !royalty_recipient.is_empty();
                let royalty_recipient_address = if royalty_recipient_known {
                    royalty_recipient.to_string()
                } else {
                    unknown_value_recipient("royalty_recipient", &edge.contract_address)
                };
                rows.push(sale_value_edge(SaleValueEdgeInput {
                    edge,
                    channel: "royalty_fee",
                    to_address: &royalty_recipient_address,
                    evidence_type: "marketplace_royalty_fee",
                    from_role: "buyer",
                    to_role: "royalty_recipient",
                    recipient_known: royalty_recipient_known,
                    value_eth: edge.royalty_fee_eth,
                    value_usd: edge.royalty_fee_usd,
                }));
            }
        }
    }
    rows.sort_by(|left, right| {
        (
            left.block_number,
            left.block_time,
            left.tx_hash.as_str(),
            left.token_id.as_str(),
        )
            .cmp(&(
                right.block_number,
                right.block_time,
                right.tx_hash.as_str(),
                right.token_id.as_str(),
            ))
    });
    rows
}

struct SaleValueEdgeInput<'a> {
    edge: &'a NftPropagationEdgePayload,
    channel: &'a str,
    to_address: &'a str,
    evidence_type: &'a str,
    from_role: &'a str,
    to_role: &'a str,
    recipient_known: bool,
    value_eth: Option<f64>,
    value_usd: Option<f64>,
}

fn sale_value_edge(input: SaleValueEdgeInput<'_>) -> ValueFlowEdgePayload {
    let SaleValueEdgeInput {
        edge,
        channel,
        to_address,
        evidence_type,
        from_role,
        to_role,
        recipient_known,
        value_eth,
        value_usd,
    } = input;
    ValueFlowEdgePayload {
        edge_id: format!("value:{}:{}", channel, edge.edge_id),
        contract_address: edge.contract_address.clone(),
        from_address: edge.to_address.clone(),
        to_address: to_address.to_string(),
        tx_hash: edge.tx_hash.clone(),
        block_number: edge.block_number,
        block_time: edge.block_time,
        token_id: edge.token_id.clone(),
        value_eth,
        value_usd,
        value_with_gas_eth: value_eth,
        value_with_gas_usd: value_usd,
        from_before_eth_balance: None,
        from_before_usd_balance: None,
        payment_token_symbol: edge.payment_token_symbol.clone(),
        payment_token_address: edge.payment_token_address.clone(),
        channel: channel.into(),
        marketplace: edge.marketplace.clone(),
        evidence_type: evidence_type.into(),
        from_role: from_role.into(),
        to_role: to_role.into(),
        recipient_known,
        evidence_flags: vec![
            "secondary_sale".into(),
            channel.into(),
            evidence_type.into(),
        ],
    }
}

fn unknown_value_recipient(role: &str, scope: &str) -> String {
    format!("unknown:{role}:{scope}")
}

fn build_contract_lifecycle_events(
    seed_contract: &SeedContractPayload,
    duplicate_contracts: &[DuplicateContractPayload],
    content_similarity_edges: &[ContentSimilarityEdgePayload],
    address_evidence_features: &[AddressEvidenceFeaturePayload],
    propagation_paths: &BTreeMap<String, NftPropagationPathPayload>,
    value_flow_edges: &[ValueFlowEdgePayload],
    market_events: &[NftMarketEventRecord],
) -> Vec<ContractLifecycleEventPayload> {
    let victim_addresses: BTreeSet<String> = address_evidence_features
        .iter()
        .filter(|feature| {
            matches!(
                feature.attribution_label.as_str(),
                "likely_victim" | "corrupted_victim"
            )
        })
        .map(|feature| feature.address.clone())
        .collect();
    let mut rows = Vec::new();

    if seed_contract.deployed_block_number > 0 {
        rows.push(ContractLifecycleEventPayload {
            event_id: format!("deploy:{}", seed_contract.contract_address),
            contract_address: seed_contract.contract_address.clone(),
            lifecycle_stage: "reference_deployment".into(),
            event_type: "seed_contract_deployed".into(),
            block_number: seed_contract.deployed_block_number,
            actor_address: seed_contract.contract_deployer.clone(),
            evidence_type: "contract_metadata".into(),
            evidence_flags: vec!["reference_contract".into(), "contract_metadata".into()],
            confidence: "high".into(),
            detail: "seed contract deployment block from contract metadata".into(),
            ..ContractLifecycleEventPayload::default()
        });
    }

    for duplicate_contract in duplicate_contracts {
        if duplicate_contract.contract_address.is_empty()
            || duplicate_contract.contract_address == seed_contract.contract_address
            || (duplicate_contract.deployed_block_number <= 0
                && duplicate_contract.contract_deployer.is_empty())
        {
            continue;
        }
        let confidence = if duplicate_contract.deployed_block_number > 0
            && !duplicate_contract.contract_deployer.is_empty()
        {
            "high"
        } else {
            "medium"
        };
        rows.push(ContractLifecycleEventPayload {
            event_id: format!("deploy:{}", duplicate_contract.contract_address),
            contract_address: duplicate_contract.contract_address.clone(),
            lifecycle_stage: "replica_deployment".into(),
            event_type: "candidate_contract_deployed".into(),
            block_number: duplicate_contract.deployed_block_number,
            block_time: duplicate_contract.deployed_block_time,
            actor_address: duplicate_contract.contract_deployer.clone(),
            evidence_type: "contract_metadata".into(),
            evidence_flags: vec!["candidate_contract".into(), "contract_metadata".into()],
            confidence: confidence.into(),
            detail: "candidate contract deployment metadata".into(),
            ..ContractLifecycleEventPayload::default()
        });
    }

    for edge in content_similarity_edges {
        rows.push(ContractLifecycleEventPayload {
            event_id: format!("content:{}", edge.edge_id),
            contract_address: edge.candidate_contract_address.clone(),
            lifecycle_stage: "copy_preparation".into(),
            event_type: "content_similarity_match".into(),
            token_id: edge.token_id.clone(),
            evidence_type: edge.evidence_type.clone(),
            evidence_flags: edge.match_reasons.clone(),
            confidence: edge.confidence.clone(),
            detail: edge.match_reasons.join(","),
            ..ContractLifecycleEventPayload::default()
        });
    }

    for path in propagation_paths.values() {
        for edge in &path.edges {
            rows.push(edge_lifecycle_event(edge));
            if edge.channel == "sale" && victim_addresses.contains(&edge.to_address) {
                rows.push(ContractLifecycleEventPayload {
                    event_id: format!("victim:{}", edge.edge_id),
                    contract_address: edge.contract_address.clone(),
                    lifecycle_stage: "victimization".into(),
                    event_type: "secondary_sale_victim_acquisition".into(),
                    block_number: edge.block_number,
                    block_time: edge.block_time,
                    tx_hash: edge.tx_hash.clone(),
                    actor_address: edge.to_address.clone(),
                    counterparty_address: edge.from_address.clone(),
                    token_id: edge.token_id.clone(),
                    value_eth: edge.price_eth,
                    value_usd: edge.price_usd,
                    evidence_type: "victim_sale_attribution".into(),
                    evidence_flags: vec![
                        "victim_score".into(),
                        "marketplace_purchase".into(),
                        "secondary_sale".into(),
                    ],
                    confidence: "medium".into(),
                    detail: "sale buyer is classified as a victim candidate".into(),
                });
            }
        }
    }
    for edge in value_flow_edges {
        if matches!(
            edge.channel.as_str(),
            "mint_payment" | "royalty_fee" | "protocol_fee" | "funding" | "withdrawal"
        ) {
            rows.push(value_flow_lifecycle_event(edge));
        }
    }
    for event in market_events {
        rows.push(market_lifecycle_event(event));
    }
    let transition_events = build_stage_transition_events(&rows);
    rows.extend(transition_events);

    rows.sort_by(|left, right| {
        (
            left.block_number,
            left.block_time,
            left.lifecycle_stage.as_str(),
            left.event_type.as_str(),
            left.contract_address.as_str(),
            left.token_id.as_str(),
        )
            .cmp(&(
                right.block_number,
                right.block_time,
                right.lifecycle_stage.as_str(),
                right.event_type.as_str(),
                right.contract_address.as_str(),
                right.token_id.as_str(),
            ))
    });
    rows
}

fn build_stage_transition_events(
    events: &[ContractLifecycleEventPayload],
) -> Vec<ContractLifecycleEventPayload> {
    let stage_order = [
        "copy_preparation",
        "replica_deployment",
        "replica_mint",
        "primary_monetization",
        "market_exposure",
        "victimization",
        "monetization",
        "exit_or_cleanup",
    ];
    let mut earliest_by_contract_stage =
        BTreeMap::<(String, String), &ContractLifecycleEventPayload>::new();
    for event in events {
        if event.contract_address.is_empty() || event.lifecycle_stage == "stage_transition" {
            continue;
        }
        if !stage_order.contains(&event.lifecycle_stage.as_str()) {
            continue;
        }
        let key = (
            event.contract_address.clone(),
            event.lifecycle_stage.clone(),
        );
        earliest_by_contract_stage
            .entry(key)
            .and_modify(|existing| {
                if lifecycle_event_time_key(event) < lifecycle_event_time_key(existing) {
                    *existing = event;
                }
            })
            .or_insert(event);
    }

    let mut transitions = Vec::new();
    let contracts: BTreeSet<String> = earliest_by_contract_stage
        .keys()
        .map(|(contract, _)| contract.clone())
        .collect();
    for contract in contracts {
        let observed: Vec<_> = stage_order
            .iter()
            .filter_map(|stage| {
                earliest_by_contract_stage
                    .get(&(contract.clone(), (*stage).to_string()))
                    .map(|event| (*stage, *event))
            })
            .collect();
        for pair in observed.windows(2) {
            let (from_stage, from_event) = pair[0];
            let (to_stage, to_event) = pair[1];
            if !is_forward_lifecycle_transition(from_event, to_event) {
                continue;
            }
            let mut evidence_flags = BTreeSet::new();
            evidence_flags.insert("stage_transition".to_string());
            evidence_flags.insert(format!("from:{from_stage}"));
            evidence_flags.insert(format!("to:{to_stage}"));
            for flag in &from_event.evidence_flags {
                evidence_flags.insert(flag.clone());
            }
            for flag in &to_event.evidence_flags {
                evidence_flags.insert(flag.clone());
            }
            let evidence_flags: Vec<String> = evidence_flags.into_iter().collect();
            transitions.push(ContractLifecycleEventPayload {
                event_id: format!("transition:{contract}:{from_stage}:{to_stage}"),
                contract_address: contract.clone(),
                lifecycle_stage: "stage_transition".into(),
                event_type: format!("{from_stage}_to_{to_stage}"),
                block_number: to_event.block_number,
                block_time: to_event.block_time,
                tx_hash: to_event.tx_hash.clone(),
                actor_address: to_event.actor_address.clone(),
                counterparty_address: to_event.counterparty_address.clone(),
                token_id: to_event.token_id.clone(),
                value_eth: to_event.value_eth,
                value_usd: to_event.value_usd,
                evidence_type: "evidence_accumulated_transition".into(),
                evidence_flags: evidence_flags.clone(),
                confidence: transition_confidence(&evidence_flags).into(),
                detail: format!(
                    "stage transition from {from_stage} to {to_stage}; evidence_flags={}",
                    evidence_flags.join(",")
                ),
            });
        }
    }
    transitions
}

fn is_forward_lifecycle_transition(
    from_event: &ContractLifecycleEventPayload,
    to_event: &ContractLifecycleEventPayload,
) -> bool {
    if from_event.block_time > 0 && to_event.block_time > 0 {
        return to_event.block_time >= from_event.block_time;
    }
    if from_event.block_number > 0 && to_event.block_number > 0 {
        return to_event.block_number >= from_event.block_number;
    }
    lifecycle_event_time_key(to_event) >= lifecycle_event_time_key(from_event)
}

fn lifecycle_event_time_key(event: &ContractLifecycleEventPayload) -> (i64, i64, &str, &str) {
    (
        event.block_number,
        event.block_time,
        event.tx_hash.as_str(),
        event.event_id.as_str(),
    )
}

fn transition_confidence(evidence_flags: &[String]) -> &'static str {
    let modality_count = [
        "token_uri_match",
        "image_uri_match",
        "metadata_match",
        "name_match",
        "mint",
        "sale",
        "marketplace_purchase",
        "paid_mint",
        "opensea_order",
        "opensea_cancel",
    ]
    .iter()
    .filter(|flag| evidence_flags.iter().any(|item| item.contains(**flag)))
    .count();
    if modality_count >= 3 {
        "high"
    } else if modality_count >= 2 {
        "medium"
    } else {
        "low"
    }
}

fn value_flow_lifecycle_event(edge: &ValueFlowEdgePayload) -> ContractLifecycleEventPayload {
    let (stage, event_type, confidence, detail) = match edge.channel.as_str() {
        "mint_payment" => (
            "primary_monetization",
            "mint_payment",
            "medium",
            "native ETH transfer in the same transaction as copied NFT mint",
        ),
        "royalty_fee" => (
            "monetization",
            "royalty_fee",
            "medium",
            "marketplace sale reports royalty value for copied NFT",
        ),
        "protocol_fee" => (
            "market_fee",
            "protocol_fee",
            "medium",
            "marketplace sale reports protocol fee value for copied NFT",
        ),
        "funding" => (
            "copy_preparation",
            "funding",
            "medium",
            "same-transaction ETH inflow funds a copied NFT mint actor",
        ),
        "withdrawal" => (
            "exit_or_cleanup",
            "withdrawal",
            "medium",
            "same-transaction ETH outflow from copied NFT contract",
        ),
        _ => (
            "value_flow",
            edge.channel.as_str(),
            "low",
            "value flow evidence for copied NFT activity",
        ),
    };
    ContractLifecycleEventPayload {
        event_id: format!("{stage}:{}", edge.edge_id),
        contract_address: edge.contract_address.clone(),
        lifecycle_stage: stage.into(),
        event_type: event_type.into(),
        block_number: edge.block_number,
        block_time: edge.block_time,
        tx_hash: edge.tx_hash.clone(),
        actor_address: edge.from_address.clone(),
        counterparty_address: edge.to_address.clone(),
        token_id: edge.token_id.clone(),
        value_eth: edge.value_eth,
        value_usd: edge.value_usd,
        evidence_type: edge.evidence_type.clone(),
        evidence_flags: edge.evidence_flags.clone(),
        confidence: confidence.into(),
        detail: detail.into(),
    }
}

fn market_lifecycle_event(event: &NftMarketEventRecord) -> ContractLifecycleEventPayload {
    let (stage, event_type, confidence, detail) = match event.event_type.as_str() {
        "order" | "listing" | "item_listed" => (
            "market_exposure",
            if event.order_type.is_empty() {
                "market_listing"
            } else {
                event.order_type.as_str()
            },
            "medium",
            "OpenSea order event for copied NFT",
        ),
        "cancel" | "order_cancelled" | "item_cancelled" => (
            "exit_or_cleanup",
            "order_cancel",
            "medium",
            "OpenSea cancellation event for copied NFT order",
        ),
        "transfer" => (
            "distribution",
            "market_transfer",
            "medium",
            "OpenSea observed NFT transfer event",
        ),
        "sale" => (
            "monetization",
            "sale",
            "high",
            "OpenSea observed sale event",
        ),
        other => (
            "market_activity",
            other,
            "low",
            "OpenSea market activity event",
        ),
    };
    let block_time = if event.block_time > 0 {
        event.block_time
    } else {
        event.event_timestamp
    };

    ContractLifecycleEventPayload {
        event_id: format!(
            "market:{}:{}:{}:{}",
            stage, event.contract_address, event.token_id, event.order_hash
        ),
        contract_address: event.contract_address.clone(),
        lifecycle_stage: stage.into(),
        event_type: event_type.into(),
        block_number: event.block_number,
        block_time,
        tx_hash: event.tx_hash.clone(),
        actor_address: event.actor_address.clone(),
        counterparty_address: if event.to_address.is_empty() {
            event.taker_address.clone()
        } else {
            event.to_address.clone()
        },
        token_id: event.token_id.clone(),
        value_eth: event.price_eth,
        value_usd: event.price_usd,
        evidence_type: format!("opensea_{}", event.event_type),
        evidence_flags: market_event_flags(event),
        confidence: confidence.into(),
        detail: detail.into(),
    }
}

fn market_event_flags(event: &NftMarketEventRecord) -> Vec<String> {
    let mut flags = vec![format!("opensea_{}", event.event_type)];
    if !event.order_type.is_empty() {
        flags.push(format!("order_type:{}", event.order_type));
    }
    if !event.marketplace.is_empty() {
        flags.push(format!("marketplace:{}", event.marketplace));
    }
    flags
}

fn build_lifecycle_metrics(
    seed_contract: &SeedContractPayload,
    propagation_paths: &BTreeMap<String, NftPropagationPathPayload>,
    market_events: &[NftMarketEventRecord],
    address_evidence_features: &[AddressEvidenceFeaturePayload],
    value_flow_edges: &[ValueFlowEdgePayload],
    lifecycle_events: &[ContractLifecycleEventPayload],
) -> Vec<ContractLifecycleMetricPayload> {
    let mut contracts: BTreeSet<String> = propagation_paths.keys().cloned().collect();
    contracts.extend(
        market_events
            .iter()
            .map(|event| event.contract_address.clone()),
    );
    contracts.extend(
        lifecycle_events
            .iter()
            .map(|event| event.contract_address.clone())
            .filter(|value| !value.is_empty()),
    );
    contracts.remove(&seed_contract.contract_address);

    contracts
        .into_iter()
        .filter(|contract| !contract.is_empty())
        .map(|contract| {
            let deployment_time =
                first_stage_time(lifecycle_events, &contract, "replica_deployment");
            let first_mint_time = first_stage_time(lifecycle_events, &contract, "replica_mint");
            let first_transfer_time = earliest_positive_time(
                first_stage_time(lifecycle_events, &contract, "distribution"),
                first_stage_time(lifecycle_events, &contract, "monetization"),
            );
            let first_listing_time =
                first_stage_time(lifecycle_events, &contract, "market_exposure");
            let first_sale_time = first_stage_time(lifecycle_events, &contract, "monetization");
            let first_victim_time = earliest_positive_time(
                first_stage_time(lifecycle_events, &contract, "victimization"),
                first_paid_mint_victim_time(value_flow_edges, &contract),
            );
            let path_summary = propagation_paths.get(&contract).map(|path| &path.summary);
            let victim_count = address_evidence_features
                .iter()
                .filter(|feature| feature.contract_address == contract)
                .filter(|feature| {
                    matches!(
                        feature.attribution_label.as_str(),
                        "likely_victim" | "corrupted_victim"
                    )
                })
                .count() as i64;
            let revenue = summarize_value_flows(
                value_flow_edges
                    .iter()
                    .filter(|edge| edge.contract_address == contract),
                address_evidence_features,
            );
            let first_outcome_time = earliest_positive_time(first_sale_time, first_victim_time);
            let pre_sale_signal_count =
                pre_sale_signal_count(lifecycle_events, &contract, first_outcome_time);
            let sale_observed = first_outcome_time > 0;
            let early_detection_positive =
                sale_observed && deployment_time > 0 && pre_sale_signal_count >= 2;
            ContractLifecycleMetricPayload {
                contract_address: contract.clone(),
                deployment_time,
                first_mint_time,
                first_transfer_time,
                first_listing_time,
                first_sale_time,
                first_victim_time,
                time_to_first_transfer_seconds: elapsed(deployment_time, first_transfer_time),
                time_to_first_listing_seconds: elapsed(deployment_time, first_listing_time),
                time_to_first_sale_seconds: elapsed(deployment_time, first_sale_time),
                time_to_first_victim_seconds: elapsed(deployment_time, first_victim_time),
                cascade_node_count: path_summary.map(|summary| summary.node_count).unwrap_or(0),
                cascade_edge_count: path_summary.map(|summary| summary.edge_count).unwrap_or(0),
                victim_count,
                sale_count: path_summary
                    .map(|summary| summary.sale_edge_count)
                    .unwrap_or(0),
                market_event_count: market_events
                    .iter()
                    .filter(|event| event.contract_address == contract)
                    .count() as i64,
                gross_revenue_eth: revenue.gross_eth,
                gross_revenue_usd: revenue.gross_usd,
                operator_revenue_eth: revenue.operator_eth,
                operator_revenue_usd: revenue.operator_usd,
                marketplace_fee_eth: revenue.marketplace_fee_eth,
                marketplace_fee_usd: revenue.marketplace_fee_usd,
                funding_amount_eth: revenue.funding_amount_eth,
                funding_amount_usd: revenue.funding_amount_usd,
                withdrawal_amount_eth: revenue.withdrawal_amount_eth,
                withdrawal_amount_usd: revenue.withdrawal_amount_usd,
                funding_edge_count: revenue.funding_edge_count,
                withdrawal_edge_count: revenue.withdrawal_edge_count,
                revenue_backflow_edge_count: revenue.revenue_backflow_edge_count,
                value_flow_coverage_scope: VALUE_FLOW_COVERAGE_SCOPE.into(),
                value_flow_coverage_gaps: value_flow_coverage_gaps(),
                top_value_recipient_address: revenue.top_value_recipient_address,
                top_value_recipient_eth: revenue.top_value_recipient_eth,
                top_value_recipient_usd: revenue.top_value_recipient_usd,
                top_value_recipient_share: revenue.top_value_recipient_share,
                pre_sale_signal_count,
                early_detection_positive,
            }
        })
        .collect()
}

#[derive(Clone, Debug, Default)]
struct ValueFlowBreakdown {
    gross_eth: f64,
    gross_usd: f64,
    operator_eth: f64,
    operator_usd: f64,
    marketplace_fee_eth: f64,
    marketplace_fee_usd: f64,
    funding_amount_eth: f64,
    funding_amount_usd: f64,
    withdrawal_amount_eth: f64,
    withdrawal_amount_usd: f64,
    funding_edge_count: i64,
    withdrawal_edge_count: i64,
    revenue_backflow_edge_count: i64,
    top_value_recipient_address: String,
    top_value_recipient_eth: f64,
    top_value_recipient_usd: f64,
    top_value_recipient_share: Option<f64>,
}

fn summarize_value_flows<'a>(
    edges: impl IntoIterator<Item = &'a ValueFlowEdgePayload>,
    address_evidence_features: &[AddressEvidenceFeaturePayload],
) -> ValueFlowBreakdown {
    let mut breakdown = ValueFlowBreakdown::default();
    let mut operator_recipient_eth = BTreeMap::<String, f64>::new();
    let mut operator_recipient_usd = BTreeMap::<String, f64>::new();
    let mut funding_sources = BTreeSet::<String>::new();
    let mut funding_source_edges = Vec::<String>::new();
    let mut operator_recipients = BTreeSet::<String>::new();
    let mut withdrawal_recipients = Vec::<String>::new();
    for edge in edges {
        if edge.channel == "funding" {
            breakdown.funding_edge_count += 1;
            if let Some(value) = edge.value_eth {
                breakdown.funding_amount_eth += value;
            }
            if let Some(value) = edge.value_usd {
                breakdown.funding_amount_usd += value;
            }
            if !edge.from_address.is_empty() {
                funding_sources.insert(edge.from_address.clone());
                funding_source_edges.push(edge.from_address.clone());
            }
        }
        if edge.channel == "withdrawal" {
            breakdown.withdrawal_edge_count += 1;
            if let Some(value) = edge.value_eth {
                breakdown.withdrawal_amount_eth += value;
            }
            if let Some(value) = edge.value_usd {
                breakdown.withdrawal_amount_usd += value;
            }
            if !edge.to_address.is_empty() {
                withdrawal_recipients.push(edge.to_address.clone());
            }
        }
        let operator_revenue = is_operator_revenue_edge(edge, address_evidence_features);
        if let Some(value) = edge.value_eth {
            if is_gross_revenue_edge(edge) {
                breakdown.gross_eth += value;
            }
            if edge.channel == "protocol_fee" {
                breakdown.marketplace_fee_eth += value;
            }
            if operator_revenue {
                breakdown.operator_eth += value;
                if !edge.to_address.is_empty() {
                    operator_recipients.insert(edge.to_address.clone());
                    *operator_recipient_eth
                        .entry(edge.to_address.clone())
                        .or_insert(0.0) += value;
                }
            }
        }
        if let Some(value) = edge.value_usd {
            if is_gross_revenue_edge(edge) {
                breakdown.gross_usd += value;
            }
            if edge.channel == "protocol_fee" {
                breakdown.marketplace_fee_usd += value;
            }
            if operator_revenue {
                breakdown.operator_usd += value;
                if !edge.to_address.is_empty() {
                    operator_recipients.insert(edge.to_address.clone());
                    *operator_recipient_usd
                        .entry(edge.to_address.clone())
                        .or_insert(0.0) += value;
                }
            }
        }
    }
    if let Some((address, eth)) = operator_recipient_eth
        .iter()
        .max_by(|left, right| left.1.total_cmp(right.1).then_with(|| right.0.cmp(left.0)))
    {
        breakdown.top_value_recipient_address = address.clone();
        breakdown.top_value_recipient_eth = *eth;
        breakdown.top_value_recipient_usd = operator_recipient_usd
            .get(address)
            .copied()
            .unwrap_or_default();
        breakdown.top_value_recipient_share =
            (breakdown.operator_eth > 0.0).then_some(*eth / breakdown.operator_eth);
    } else if let Some((address, usd)) = operator_recipient_usd
        .iter()
        .max_by(|left, right| left.1.total_cmp(right.1).then_with(|| right.0.cmp(left.0)))
    {
        breakdown.top_value_recipient_address = address.clone();
        breakdown.top_value_recipient_usd = *usd;
        breakdown.top_value_recipient_share =
            (breakdown.operator_usd > 0.0).then_some(*usd / breakdown.operator_usd);
    }
    let mut backflow_addresses = BTreeSet::new();
    for address in funding_source_edges
        .iter()
        .filter(|address| operator_recipients.contains(*address))
    {
        backflow_addresses.insert(address.clone());
    }
    for address in withdrawal_recipients
        .iter()
        .filter(|address| funding_sources.contains(*address))
    {
        backflow_addresses.insert(address.clone());
    }
    breakdown.revenue_backflow_edge_count = backflow_addresses.len() as i64;
    breakdown
}

fn is_gross_revenue_edge(edge: &ValueFlowEdgePayload) -> bool {
    matches!(
        edge.channel.as_str(),
        "mint_payment" | "sale_payment" | "royalty_fee" | "protocol_fee"
    )
}

fn is_operator_revenue_edge(
    edge: &ValueFlowEdgePayload,
    address_evidence_features: &[AddressEvidenceFeaturePayload],
) -> bool {
    match edge.channel.as_str() {
        "mint_payment" => {
            matches!(
                edge.to_role.as_str(),
                "mint_contract"
                    | "contract_deployer"
                    | "contract_owner"
                    | "contract_admin"
                    | "proxy_admin"
            ) || edge
                .to_address
                .eq_ignore_ascii_case(edge.contract_address.as_str())
        }
        "sale_payment" | "royalty_fee" => {
            edge.recipient_known
                && (matches!(
                    edge.to_role.as_str(),
                    "contract_deployer" | "contract_owner" | "contract_admin" | "proxy_admin"
                ) || has_strong_operator_address_evidence(
                    address_evidence_features,
                    &edge.contract_address,
                    &edge.to_address,
                ))
        }
        "withdrawal" => false,
        _ => false,
    }
}

fn pre_sale_signal_count(
    lifecycle_events: &[ContractLifecycleEventPayload],
    contract_address: &str,
    first_outcome_time: i64,
) -> i64 {
    lifecycle_events
        .iter()
        .filter(|event| event.contract_address == contract_address)
        .filter(|event| event.lifecycle_stage != "stage_transition")
        .filter(|event| !is_outcome_lifecycle_event(event))
        .filter(|event| {
            event.block_time > 0
                && (first_outcome_time <= 0 || event.block_time < first_outcome_time)
        })
        .count() as i64
}

fn earliest_positive_time(left: i64, right: i64) -> i64 {
    match (left > 0, right > 0) {
        (true, true) => left.min(right),
        (true, false) => left,
        (false, true) => right,
        (false, false) => 0,
    }
}

fn first_paid_mint_victim_time(
    value_flow_edges: &[ValueFlowEdgePayload],
    contract_address: &str,
) -> i64 {
    value_flow_edges
        .iter()
        .filter(|edge| edge.contract_address == contract_address)
        .filter(|edge| edge.channel == "mint_payment")
        .filter(|edge| edge.block_time > 0 && value_flow_has_positive_amount(edge))
        .map(|edge| edge.block_time)
        .min()
        .unwrap_or(0)
}

fn value_flow_has_positive_amount(edge: &ValueFlowEdgePayload) -> bool {
    edge.value_eth.unwrap_or(0.0) > 0.0 || edge.value_usd.unwrap_or(0.0) > 0.0
}

fn is_outcome_lifecycle_event(event: &ContractLifecycleEventPayload) -> bool {
    matches!(
        event.lifecycle_stage.as_str(),
        "monetization" | "primary_monetization" | "victimization"
    ) || matches!(
        event.event_type.as_str(),
        "sale" | "mint_payment" | "secondary_sale_victim_acquisition"
    )
}

fn has_strong_operator_address_evidence(
    features: &[AddressEvidenceFeaturePayload],
    contract_address: &str,
    address: &str,
) -> bool {
    features.iter().any(|feature| {
        feature
            .contract_address
            .eq_ignore_ascii_case(contract_address)
            && feature.address.eq_ignore_ascii_case(address)
            && matches!(
                feature.attribution_label.as_str(),
                "suspected_operator" | "suspected_colluder" | "corrupted_victim"
            )
            && has_strong_campaign_address_evidence(feature)
    })
}

fn first_stage_time(
    lifecycle_events: &[ContractLifecycleEventPayload],
    contract_address: &str,
    stage: &str,
) -> i64 {
    lifecycle_events
        .iter()
        .filter(|event| event.contract_address == contract_address)
        .filter(|event| event.lifecycle_stage == stage)
        .filter_map(|event| (event.block_time > 0).then_some(event.block_time))
        .min()
        .unwrap_or_default()
}

fn elapsed(start: i64, end: i64) -> Option<i64> {
    (start > 0 && end >= start).then_some(end - start)
}

fn build_weak_supervision_labels(
    lifecycle_metrics: &[ContractLifecycleMetricPayload],
    address_evidence_features: &[AddressEvidenceFeaturePayload],
    content_similarity_edges: &[ContentSimilarityEdgePayload],
) -> Vec<WeakSupervisionLabelPayload> {
    let mut labels = Vec::new();
    let content_confidence_by_contract = content_confidence_by_contract(content_similarity_edges);
    for metric in lifecycle_metrics {
        let content_confidence = content_confidence_by_contract
            .get(&metric.contract_address)
            .map(String::as_str)
            .unwrap_or("");
        if !content_confidence.is_empty()
            && (metric.sale_count > 0
                || metric.victim_count > 0
                || metric.operator_revenue_eth > 0.0
                || metric.operator_revenue_usd > 0.0)
        {
            labels.push(WeakSupervisionLabelPayload {
                entity_type: "contract".into(),
                contract_address: metric.contract_address.clone(),
                address: String::new(),
                label: "probable_infringement_campaign".into(),
                confidence: if content_confidence == "high" && metric.victim_count > 0 {
                    "high".into()
                } else {
                    "medium".into()
                },
                source: "rule_v1".into(),
                evidence_flags: compact_flags(vec![
                    format!("content_similarity:{content_confidence}"),
                    format!("sale_count:{}", metric.sale_count),
                    format!("victim_count:{}", metric.victim_count),
                    format!("operator_revenue_eth:{:.8}", metric.operator_revenue_eth),
                ]),
            });
        }
        if metric.early_detection_positive {
            labels.push(WeakSupervisionLabelPayload {
                entity_type: "contract".into(),
                contract_address: metric.contract_address.clone(),
                address: String::new(),
                label: "early_detection_positive".into(),
                confidence: "medium".into(),
                source: "rule_v1".into(),
                evidence_flags: vec![
                    format!("pre_sale_signal_count:{}", metric.pre_sale_signal_count),
                    format!(
                        "time_to_first_sale_seconds:{:?}",
                        metric.time_to_first_sale_seconds
                    ),
                ],
            });
        }
        if metric
            .top_value_recipient_share
            .map(|share| share >= 0.80)
            .unwrap_or(false)
            && (metric.operator_revenue_eth > 0.0 || metric.operator_revenue_usd > 0.0)
        {
            labels.push(WeakSupervisionLabelPayload {
                entity_type: "contract".into(),
                contract_address: metric.contract_address.clone(),
                address: metric.top_value_recipient_address.clone(),
                label: "concentrated_operator_revenue".into(),
                confidence: "medium".into(),
                source: "rule_v1".into(),
                evidence_flags: vec![
                    format!(
                        "top_value_recipient_share:{:.4}",
                        metric.top_value_recipient_share.unwrap_or_default()
                    ),
                    format!("operator_revenue_eth:{:.8}", metric.operator_revenue_eth),
                ],
            });
        }
    }

    for feature in address_evidence_features {
        let label = match feature.attribution_label.as_str() {
            "suspected_operator" => "operator_address_candidate",
            "suspected_colluder" => "colluder_address_candidate",
            "likely_victim" => "victim_address_candidate",
            "corrupted_victim" => "corrupted_victim_candidate",
            _ => continue,
        };
        labels.push(WeakSupervisionLabelPayload {
            entity_type: "address".into(),
            contract_address: feature.contract_address.clone(),
            address: feature.address.clone(),
            label: label.into(),
            confidence: feature.confidence.clone(),
            source: "address_attribution_rule_v1".into(),
            evidence_flags: feature
                .evidence
                .iter()
                .map(|evidence| evidence.evidence_type.clone())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
        });
    }

    labels.sort_by(|left, right| {
        (
            left.entity_type.as_str(),
            left.contract_address.as_str(),
            left.address.as_str(),
            left.label.as_str(),
        )
            .cmp(&(
                right.entity_type.as_str(),
                right.contract_address.as_str(),
                right.address.as_str(),
                right.label.as_str(),
            ))
    });
    labels
}

fn content_confidence_by_contract(
    content_similarity_edges: &[ContentSimilarityEdgePayload],
) -> BTreeMap<String, String> {
    let mut rows = BTreeMap::<String, String>::new();
    for edge in content_similarity_edges {
        rows.entry(edge.candidate_contract_address.clone())
            .and_modify(|existing| {
                if confidence_rank(&edge.confidence) > confidence_rank(existing.as_str()) {
                    *existing = edge.confidence.clone();
                }
            })
            .or_insert(edge.confidence.clone());
    }
    rows
}

fn confidence_rank(value: &str) -> i64 {
    match value {
        "high" => 3,
        "medium" => 2,
        "low" => 1,
        _ => 0,
    }
}

fn compact_flags(flags: Vec<String>) -> Vec<String> {
    flags
        .into_iter()
        .filter(|flag| !flag.trim().is_empty())
        .collect()
}

fn build_early_detection_features(
    lifecycle_metrics: &[ContractLifecycleMetricPayload],
    lifecycle_events: &[ContractLifecycleEventPayload],
    value_flow_edges: &[ValueFlowEdgePayload],
) -> Vec<EarlyDetectionFeaturePayload> {
    let mut rows = Vec::new();
    for metric in lifecycle_metrics {
        if metric.deployment_time <= 0 {
            continue;
        }
        for window_seconds in [60_i64, 3_600_i64, 86_400_i64] {
            let window_end = metric.deployment_time + window_seconds;
            let contract_events: Vec<_> = lifecycle_events
                .iter()
                .filter(|event| event.contract_address == metric.contract_address)
                .filter(|event| event.block_time > 0 && event.block_time <= window_end)
                .collect();
            let contract_value_edges: Vec<_> = value_flow_edges
                .iter()
                .filter(|edge| edge.contract_address == metric.contract_address)
                .filter(|edge| edge.block_time > 0 && edge.block_time <= window_end)
                .collect();
            let sale_observed = metric.first_sale_time > 0 && metric.first_sale_time <= window_end;
            let victim_observed =
                metric.first_victim_time > 0 && metric.first_victim_time <= window_end;
            let future_positive =
                (metric.first_sale_time > window_end) || (metric.first_victim_time > window_end);
            let first_outcome_time =
                earliest_positive_time(metric.first_sale_time, metric.first_victim_time);
            rows.push(EarlyDetectionFeaturePayload {
                contract_address: metric.contract_address.clone(),
                observation_window_seconds: window_seconds,
                window_start_time: metric.deployment_time,
                window_end_time: window_end,
                content_similarity_count: contract_events
                    .iter()
                    .filter(|event| event.lifecycle_stage == "copy_preparation")
                    .count() as i64,
                mint_event_count: contract_events
                    .iter()
                    .filter(|event| event.event_type == "mint")
                    .count() as i64,
                market_event_count: contract_events
                    .iter()
                    .filter(|event| event.lifecycle_stage == "market_exposure")
                    .count() as i64,
                value_flow_count: contract_value_edges.len() as i64,
                funding_edge_count: contract_value_edges
                    .iter()
                    .filter(|edge| edge.channel == "funding")
                    .count() as i64,
                withdrawal_edge_count: contract_value_edges
                    .iter()
                    .filter(|edge| edge.channel == "withdrawal")
                    .count() as i64,
                sale_event_count: contract_events
                    .iter()
                    .filter(|event| event.event_type == "sale")
                    .count() as i64,
                victim_signal_count: contract_events
                    .iter()
                    .filter(|event| event.lifecycle_stage == "victimization")
                    .count() as i64,
                pre_sale_signal_count: contract_events
                    .iter()
                    .filter(|event| !is_outcome_lifecycle_event(event))
                    .filter(|event| {
                        first_outcome_time <= 0
                            || (event.block_time > 0 && event.block_time < first_outcome_time)
                    })
                    .count() as i64,
                weak_label: if !sale_observed && !victim_observed && future_positive {
                    "positive_future_sale_or_victimization".into()
                } else if sale_observed || victim_observed {
                    "positive_observed_sale_or_victimization".into()
                } else {
                    "unlabeled".into()
                },
            });
        }
    }
    rows
}

fn edge_lifecycle_event(edge: &NftPropagationEdgePayload) -> ContractLifecycleEventPayload {
    let (stage, event_type, actor, counterparty, confidence, detail) = match edge.channel.as_str() {
        "mint" => (
            "replica_mint",
            "mint",
            edge.to_address.clone(),
            edge.from_address.clone(),
            "medium",
            "copied NFT mint or first observed mint-like transfer",
        ),
        "sale" => (
            "monetization",
            "sale",
            edge.from_address.clone(),
            edge.to_address.clone(),
            "high",
            "marketplace sale of copied NFT",
        ),
        _ => (
            "distribution",
            "transfer",
            edge.from_address.clone(),
            edge.to_address.clone(),
            "medium",
            "post-mint NFT transfer propagation",
        ),
    };

    ContractLifecycleEventPayload {
        event_id: format!("{}:{}", stage, edge.edge_id),
        contract_address: edge.contract_address.clone(),
        lifecycle_stage: stage.into(),
        event_type: event_type.into(),
        block_number: edge.block_number,
        block_time: edge.block_time,
        tx_hash: edge.tx_hash.clone(),
        actor_address: actor,
        counterparty_address: counterparty,
        token_id: edge.token_id.clone(),
        value_eth: edge.price_eth,
        value_usd: edge.price_usd,
        evidence_type: edge.channel.clone(),
        evidence_flags: propagation_event_flags(edge),
        confidence: confidence.into(),
        detail: detail.into(),
    }
}

fn propagation_event_flags(edge: &NftPropagationEdgePayload) -> Vec<String> {
    let mut flags = edge.underlying_channels.clone();
    if flags.is_empty() {
        flags.push(edge.channel.clone());
    }
    if edge.merged_transfer {
        flags.push("sale_transfer_merged".into());
    }
    if edge.aggregate_count > 1 {
        flags.push("aggregated_edge".into());
    }
    flags
}

fn has_strong_campaign_address_evidence(feature: &AddressEvidenceFeaturePayload) -> bool {
    feature.evidence.iter().any(|evidence| {
        matches!(
            evidence.evidence_type.as_str(),
            "wash_cycle" | "star_distribution" | "rapid_spread" | "corrupted_honest_resale"
        )
    })
}

fn build_campaign_clusters(
    seed_contract: &SeedContractPayload,
    duplicate_contracts: &[DuplicateContractPayload],
    address_evidence_features: &[AddressEvidenceFeaturePayload],
    value_flow_edges: &[ValueFlowEdgePayload],
    lifecycle_events: &[ContractLifecycleEventPayload],
) -> Vec<CampaignClusterPayload> {
    let seed_contract_address = seed_contract.contract_address.as_str();
    let campaign_events: Vec<_> = lifecycle_events
        .iter()
        .filter(|event| is_campaign_lifecycle_event(event, seed_contract_address))
        .collect();

    let mut contract_addresses: BTreeSet<String> = duplicate_contracts
        .iter()
        .filter(|item| {
            !item.contract_address.is_empty()
                && item.contract_address != seed_contract.contract_address
        })
        .map(|item| item.contract_address.clone())
        .collect();
    for event in &campaign_events {
        contract_addresses.insert(event.contract_address.clone());
    }

    if contract_addresses.is_empty() {
        return vec![];
    }

    let mut adjacency: BTreeMap<String, BTreeSet<String>> = contract_addresses
        .iter()
        .map(|contract| (contract.clone(), BTreeSet::new()))
        .collect();
    let mut shared_evidence_by_contract: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

    let mut contracts_by_deployer = BTreeMap::<String, BTreeSet<String>>::new();
    for duplicate_contract in duplicate_contracts {
        if contract_addresses.contains(&duplicate_contract.contract_address)
            && !duplicate_contract.contract_deployer.is_empty()
        {
            contracts_by_deployer
                .entry(duplicate_contract.contract_deployer.clone())
                .or_default()
                .insert(duplicate_contract.contract_address.clone());
        }
    }
    for (deployer, contracts) in contracts_by_deployer {
        connect_contract_group(
            &mut adjacency,
            &mut shared_evidence_by_contract,
            &contracts,
            format!("shared_deployer:{deployer}"),
        );
    }

    let mut contracts_by_attributed_address = BTreeMap::<String, BTreeSet<String>>::new();
    for feature in address_evidence_features {
        if !contract_addresses.contains(&feature.contract_address)
            || !matches!(
                feature.attribution_label.as_str(),
                "suspected_operator" | "suspected_colluder" | "corrupted_victim"
            )
            || !has_strong_campaign_address_evidence(feature)
        {
            continue;
        }
        contracts_by_attributed_address
            .entry(format!("{}:{}", feature.attribution_label, feature.address))
            .or_default()
            .insert(feature.contract_address.clone());
    }
    for (key, contracts) in contracts_by_attributed_address {
        connect_contract_group(
            &mut adjacency,
            &mut shared_evidence_by_contract,
            &contracts,
            format!("shared_address:{key}"),
        );
    }

    let mut contracts_by_value_recipient = BTreeMap::<String, BTreeSet<String>>::new();
    for edge in value_flow_edges {
        if !contract_addresses.contains(&edge.contract_address)
            || !edge.recipient_known
            || edge.to_address.is_empty()
            || !matches!(
                edge.channel.as_str(),
                "mint_payment" | "royalty_fee" | "withdrawal"
            )
        {
            continue;
        }
        contracts_by_value_recipient
            .entry(format!("{}:{}", edge.channel, edge.to_address))
            .or_default()
            .insert(edge.contract_address.clone());
    }
    for (key, contracts) in contracts_by_value_recipient {
        connect_contract_group(
            &mut adjacency,
            &mut shared_evidence_by_contract,
            &contracts,
            format!("shared_value_recipient:{key}"),
        );
    }

    let mut contracts_by_funding_source = BTreeMap::<String, BTreeSet<String>>::new();
    for edge in value_flow_edges {
        if !contract_addresses.contains(&edge.contract_address)
            || !edge.recipient_known
            || edge.from_address.is_empty()
            || edge.channel != "funding"
        {
            continue;
        }
        contracts_by_funding_source
            .entry(edge.from_address.clone())
            .or_default()
            .insert(edge.contract_address.clone());
    }
    for (source, contracts) in contracts_by_funding_source {
        connect_contract_group(
            &mut adjacency,
            &mut shared_evidence_by_contract,
            &contracts,
            format!("shared_funding_source:{source}"),
        );
    }

    let components = connected_contract_components(&contract_addresses, &adjacency);
    let mut clusters = Vec::new();
    for component in components {
        let component_set: BTreeSet<String> = component.iter().cloned().collect();
        let component_events: Vec<_> = campaign_events
            .iter()
            .filter(|event| component_set.contains(&event.contract_address))
            .copied()
            .collect();
        let component_value_edges: Vec<_> = value_flow_edges
            .iter()
            .filter(|edge| component_set.contains(&edge.contract_address))
            .collect();
        let mut shared_evidence = component
            .iter()
            .flat_map(|contract| {
                shared_evidence_by_contract
                    .get(contract)
                    .into_iter()
                    .flatten()
                    .cloned()
            })
            .collect::<BTreeSet<_>>();
        if shared_evidence.is_empty() {
            shared_evidence.insert("single_contract_candidate".into());
        }
        let shared_evidence: Vec<String> = shared_evidence.into_iter().collect();
        let lifecycle_stages = component_events
            .iter()
            .map(|event| event.lifecycle_stage.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let token_count = component_events
            .iter()
            .filter(|event| !event.token_id.is_empty())
            .map(|event| event.token_id.clone())
            .collect::<BTreeSet<_>>()
            .len() as i64;
        let sale_count = component_events
            .iter()
            .filter(|event| event.event_type == "sale")
            .count() as i64;
        let revenue = summarize_value_flows(
            component_value_edges.iter().copied(),
            address_evidence_features,
        );
        let blocks: Vec<i64> = component_events
            .iter()
            .filter_map(|event| (event.block_number > 0).then_some(event.block_number))
            .collect();
        let times: Vec<i64> = component_events
            .iter()
            .filter_map(|event| (event.block_time > 0).then_some(event.block_time))
            .collect();
        let value_flow_channels = component_value_edges
            .iter()
            .map(|edge| edge.channel.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let first_contract = component.first().cloned().unwrap_or_default();
        clusters.push(CampaignClusterPayload {
            cluster_id: format!(
                "campaign:{}:{}",
                seed_contract.contract_address, first_contract
            ),
            seed_contract_address: seed_contract.contract_address.clone(),
            contract_addresses: component,
            suspected_operator_addresses: addresses_by_label_for_contracts(
                address_evidence_features,
                "suspected_operator",
                &component_set,
            ),
            suspected_colluder_addresses: addresses_by_label_for_contracts(
                address_evidence_features,
                "suspected_colluder",
                &component_set,
            ),
            victim_addresses: victim_addresses_for_contracts(
                address_evidence_features,
                &component_set,
            ),
            corrupted_victim_addresses: addresses_by_label_for_contracts(
                address_evidence_features,
                "corrupted_victim",
                &component_set,
            ),
            lifecycle_stages,
            shared_evidence: shared_evidence.clone(),
            value_flow_channels,
            cluster_confidence: campaign_cluster_confidence(&shared_evidence).into(),
            token_count,
            sale_count,
            value_flow_count: component_value_edges.len() as i64,
            gross_revenue_eth: revenue.gross_eth,
            gross_revenue_usd: revenue.gross_usd,
            operator_revenue_eth: revenue.operator_eth,
            operator_revenue_usd: revenue.operator_usd,
            marketplace_fee_eth: revenue.marketplace_fee_eth,
            marketplace_fee_usd: revenue.marketplace_fee_usd,
            funding_amount_eth: revenue.funding_amount_eth,
            funding_amount_usd: revenue.funding_amount_usd,
            withdrawal_amount_eth: revenue.withdrawal_amount_eth,
            withdrawal_amount_usd: revenue.withdrawal_amount_usd,
            funding_edge_count: revenue.funding_edge_count,
            withdrawal_edge_count: revenue.withdrawal_edge_count,
            revenue_backflow_edge_count: revenue.revenue_backflow_edge_count,
            value_flow_coverage_scope: VALUE_FLOW_COVERAGE_SCOPE.into(),
            value_flow_coverage_gaps: value_flow_coverage_gaps(),
            first_block_number: blocks.iter().min().copied().unwrap_or_default(),
            last_block_number: blocks.iter().max().copied().unwrap_or_default(),
            first_block_time: times.iter().min().copied().unwrap_or_default(),
            last_block_time: times.iter().max().copied().unwrap_or_default(),
        });
    }
    clusters.sort_by(|left, right| {
        (
            left.first_block_number,
            left.first_block_time,
            left.contract_addresses
                .first()
                .map(String::as_str)
                .unwrap_or(""),
        )
            .cmp(&(
                right.first_block_number,
                right.first_block_time,
                right
                    .contract_addresses
                    .first()
                    .map(String::as_str)
                    .unwrap_or(""),
            ))
    });
    clusters
}

fn is_campaign_lifecycle_event(
    event: &ContractLifecycleEventPayload,
    seed_contract_address: &str,
) -> bool {
    !event.contract_address.is_empty()
        && event.contract_address != seed_contract_address
        && event.lifecycle_stage != "reference_deployment"
}

fn addresses_by_label_for_contracts(
    features: &[AddressEvidenceFeaturePayload],
    label: &str,
    contracts: &BTreeSet<String>,
) -> Vec<String> {
    features
        .iter()
        .filter(|feature| contracts.contains(&feature.contract_address))
        .filter(|feature| feature.attribution_label == label)
        .map(|feature| feature.address.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn victim_addresses_for_contracts(
    features: &[AddressEvidenceFeaturePayload],
    contracts: &BTreeSet<String>,
) -> Vec<String> {
    features
        .iter()
        .filter(|feature| contracts.contains(&feature.contract_address))
        .filter(|feature| {
            matches!(
                feature.attribution_label.as_str(),
                "likely_victim" | "corrupted_victim"
            )
        })
        .map(|feature| feature.address.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn connect_contract_group(
    adjacency: &mut BTreeMap<String, BTreeSet<String>>,
    shared_evidence_by_contract: &mut BTreeMap<String, BTreeSet<String>>,
    contracts: &BTreeSet<String>,
    evidence: String,
) {
    if contracts.len() < 2 {
        return;
    }
    for contract in contracts {
        shared_evidence_by_contract
            .entry(contract.clone())
            .or_default()
            .insert(evidence.clone());
    }
    let contracts: Vec<_> = contracts.iter().cloned().collect();
    for left_index in 0..contracts.len() {
        for right_index in (left_index + 1)..contracts.len() {
            adjacency
                .entry(contracts[left_index].clone())
                .or_default()
                .insert(contracts[right_index].clone());
            adjacency
                .entry(contracts[right_index].clone())
                .or_default()
                .insert(contracts[left_index].clone());
        }
    }
}

fn connected_contract_components(
    contracts: &BTreeSet<String>,
    adjacency: &BTreeMap<String, BTreeSet<String>>,
) -> Vec<Vec<String>> {
    let mut visited = BTreeSet::new();
    let mut components = Vec::new();
    for contract in contracts {
        if visited.contains(contract) {
            continue;
        }
        let mut stack = vec![contract.clone()];
        let mut component = BTreeSet::new();
        while let Some(current) = stack.pop() {
            if !visited.insert(current.clone()) {
                continue;
            }
            component.insert(current.clone());
            if let Some(neighbors) = adjacency.get(&current) {
                for neighbor in neighbors {
                    if !visited.contains(neighbor) {
                        stack.push(neighbor.clone());
                    }
                }
            }
        }
        components.push(component.into_iter().collect());
    }
    components
}

fn campaign_cluster_confidence(shared_evidence: &[String]) -> &'static str {
    if shared_evidence.iter().any(|item| {
        item.starts_with("shared_deployer:") || item.starts_with("shared_value_recipient:")
    }) {
        "high"
    } else if shared_evidence
        .iter()
        .any(|item| item.starts_with("shared_address:"))
    {
        "medium"
    } else {
        "low"
    }
}

fn value_flow_coverage_gaps() -> Vec<String> {
    VALUE_FLOW_COVERAGE_GAPS
        .iter()
        .map(|value| (*value).to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn lifecycle_metric_pre_sale_signals_exclude_victim_outcome() {
        let seed_contract = SeedContractPayload {
            contract_address: "0xseed".into(),
            ..SeedContractPayload::default()
        };
        let lifecycle_events = vec![
            ContractLifecycleEventPayload {
                contract_address: "0xdup".into(),
                lifecycle_stage: "replica_deployment".into(),
                event_type: "candidate_contract_deployed".into(),
                block_time: 80,
                ..ContractLifecycleEventPayload::default()
            },
            ContractLifecycleEventPayload {
                contract_address: "0xdup".into(),
                lifecycle_stage: "replica_mint".into(),
                event_type: "mint".into(),
                block_time: 100,
                ..ContractLifecycleEventPayload::default()
            },
            ContractLifecycleEventPayload {
                contract_address: "0xdup".into(),
                lifecycle_stage: "victimization".into(),
                event_type: "secondary_sale_victim_acquisition".into(),
                block_time: 150,
                ..ContractLifecycleEventPayload::default()
            },
        ];

        let metrics = build_lifecycle_metrics(
            &seed_contract,
            &BTreeMap::new(),
            &[],
            &[],
            &[],
            &lifecycle_events,
        );
        let metric = metrics
            .iter()
            .find(|metric| metric.contract_address == "0xdup")
            .expect("contract metric");

        assert_eq!(metric.first_sale_time, 0);
        assert_eq!(metric.deployment_time, 80);
        assert_eq!(metric.first_victim_time, 150);
        assert_eq!(metric.pre_sale_signal_count, 2);
        assert!(metric.early_detection_positive);
    }

    #[test]
    fn lifecycle_metric_uses_paid_mint_as_victim_outcome_time() {
        let seed_contract = SeedContractPayload {
            contract_address: "0xseed".into(),
            ..SeedContractPayload::default()
        };
        let lifecycle_events = vec![
            ContractLifecycleEventPayload {
                contract_address: "0xdup".into(),
                lifecycle_stage: "replica_deployment".into(),
                event_type: "candidate_contract_deployed".into(),
                block_time: 80,
                ..ContractLifecycleEventPayload::default()
            },
            ContractLifecycleEventPayload {
                contract_address: "0xdup".into(),
                lifecycle_stage: "replica_mint".into(),
                event_type: "mint".into(),
                block_time: 100,
                ..ContractLifecycleEventPayload::default()
            },
        ];
        let value_flow_edges = vec![ValueFlowEdgePayload {
            contract_address: "0xdup".into(),
            block_time: 120,
            channel: "mint_payment".into(),
            value_eth: Some(0.08),
            ..ValueFlowEdgePayload::default()
        }];

        let metrics = build_lifecycle_metrics(
            &seed_contract,
            &BTreeMap::new(),
            &[],
            &[],
            &value_flow_edges,
            &lifecycle_events,
        );
        let metric = metrics
            .iter()
            .find(|metric| metric.contract_address == "0xdup")
            .expect("contract metric");

        assert_eq!(metric.first_sale_time, 0);
        assert_eq!(metric.deployment_time, 80);
        assert_eq!(metric.first_victim_time, 120);
        assert_eq!(metric.time_to_first_victim_seconds, Some(40));
        assert_eq!(metric.pre_sale_signal_count, 2);

        let rows = build_early_detection_features(&metrics, &lifecycle_events, &value_flow_edges);
        let first_window = rows
            .iter()
            .find(|row| row.observation_window_seconds == 60)
            .expect("first window");
        assert_eq!(
            first_window.weak_label,
            "positive_observed_sale_or_victimization"
        );
    }

    #[test]
    fn early_detection_window_pre_sale_signals_exclude_victim_outcome() {
        let metrics = vec![ContractLifecycleMetricPayload {
            contract_address: "0xdup".into(),
            deployment_time: 80,
            first_mint_time: 100,
            first_sale_time: 0,
            first_victim_time: 150,
            ..ContractLifecycleMetricPayload::default()
        }];
        let lifecycle_events = vec![
            ContractLifecycleEventPayload {
                contract_address: "0xdup".into(),
                lifecycle_stage: "replica_deployment".into(),
                event_type: "candidate_contract_deployed".into(),
                block_time: 80,
                ..ContractLifecycleEventPayload::default()
            },
            ContractLifecycleEventPayload {
                contract_address: "0xdup".into(),
                lifecycle_stage: "replica_mint".into(),
                event_type: "mint".into(),
                block_time: 100,
                ..ContractLifecycleEventPayload::default()
            },
            ContractLifecycleEventPayload {
                contract_address: "0xdup".into(),
                lifecycle_stage: "victimization".into(),
                event_type: "secondary_sale_victim_acquisition".into(),
                block_time: 150,
                ..ContractLifecycleEventPayload::default()
            },
        ];

        let rows = build_early_detection_features(&metrics, &lifecycle_events, &[]);
        let first_window = rows
            .iter()
            .find(|row| row.observation_window_seconds == 60)
            .expect("first window");

        assert_eq!(first_window.window_start_time, 80);
        assert_eq!(first_window.victim_signal_count, 0);
        assert_eq!(first_window.pre_sale_signal_count, 2);
        let long_window = rows
            .iter()
            .find(|row| row.observation_window_seconds == 3_600)
            .expect("long window");
        assert_eq!(long_window.victim_signal_count, 1);
        assert_eq!(
            long_window.weak_label,
            "positive_observed_sale_or_victimization"
        );
    }

    #[test]
    fn lifecycle_metric_treats_sale_as_first_transfer_when_no_distribution_edge_exists() {
        let seed_contract = SeedContractPayload {
            contract_address: "0xseed".into(),
            ..SeedContractPayload::default()
        };
        let lifecycle_events = vec![
            ContractLifecycleEventPayload {
                contract_address: "0xdup".into(),
                lifecycle_stage: "replica_deployment".into(),
                block_time: 80,
                ..ContractLifecycleEventPayload::default()
            },
            ContractLifecycleEventPayload {
                contract_address: "0xdup".into(),
                lifecycle_stage: "monetization".into(),
                event_type: "sale".into(),
                block_time: 150,
                ..ContractLifecycleEventPayload::default()
            },
        ];

        let metrics = build_lifecycle_metrics(
            &seed_contract,
            &BTreeMap::new(),
            &[],
            &[],
            &[],
            &lifecycle_events,
        );
        let metric = metrics
            .iter()
            .find(|item| item.contract_address == "0xdup")
            .expect("contract metric");

        assert_eq!(metric.first_transfer_time, 150);
        assert_eq!(metric.time_to_first_transfer_seconds, Some(70));
    }
}
