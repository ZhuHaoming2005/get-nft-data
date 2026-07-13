use super::*;

pub(super) fn build_contract_lifecycle_events(
    seed_contract: &SeedContractPayload,
    duplicate_contracts: &[DuplicateContractPayload],
    content_similarity_edges: &[ContentSimilarityEdgePayload],
    address_evidence_features: &[AddressEvidenceFeaturePayload],
    propagation_paths: &BTreeMap<String, NftPropagationPathPayload>,
    value_flow_edges: &[ValueFlowEdgePayload],
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
            "mint_payment"
                | "royalty_fee"
                | "protocol_fee"
                | "funding"
                | "withdrawal"
                | "cashout_hop"
        ) {
            rows.push(value_flow_lifecycle_event(edge));
        }
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
            "same-block value outflow from copied NFT contract",
        ),
        "cashout_hop" => (
            "exit_or_cleanup",
            "cashout_hop",
            "medium",
            "same-block multi-hop cashout after copied NFT contract withdrawal",
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
