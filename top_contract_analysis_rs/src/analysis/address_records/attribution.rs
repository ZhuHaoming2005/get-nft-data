use super::*;

#[derive(Default)]
struct AttributionAccumulator {
    roles: BTreeSet<String>,
    attacker_score: f64,
    operator_score: f64,
    colluder_score: f64,
    victim_score: f64,
    corruption_score: f64,
    neutral_score: f64,
    operator_level: i64,
    operator_level_label: String,
    evidence: Vec<AddressEvidencePayload>,
}

fn attribution_entry<'a>(
    rows: &'a mut BTreeMap<String, AttributionAccumulator>,
    address: &str,
) -> Option<&'a mut AttributionAccumulator> {
    let address = normalize_chain_identity(address);
    if address.is_empty() || address == ZERO_ADDRESS {
        return None;
    }
    Some(rows.entry(address).or_default())
}

#[derive(Clone, Copy)]
enum EvidenceBucket {
    Operator,
    Colluder,
    Victim,
    Corruption,
    Neutral,
}

struct EvidenceInput<'a> {
    contract_address: &'a str,
    address: &'a str,
    role: &'a str,
    evidence_type: &'a str,
    token_id: &'a str,
    tx_hash: &'a str,
    weight: f64,
    detail: &'a str,
    bucket: EvidenceBucket,
}

fn add_attribution_evidence(
    rows: &mut BTreeMap<String, AttributionAccumulator>,
    input: EvidenceInput<'_>,
) {
    let Some(entry) = attribution_entry(rows, input.address) else {
        return;
    };
    entry.roles.insert(input.role.to_string());
    match input.bucket {
        EvidenceBucket::Operator => {
            entry.operator_score += input.weight;
            entry.attacker_score += input.weight;
        }
        EvidenceBucket::Colluder => {
            entry.colluder_score += input.weight;
            entry.attacker_score += input.weight;
        }
        EvidenceBucket::Victim => {
            entry.victim_score += input.weight;
        }
        EvidenceBucket::Corruption => {
            entry.corruption_score += input.weight;
        }
        EvidenceBucket::Neutral => {
            entry.neutral_score += input.weight;
        }
    }
    entry.evidence.push(AddressEvidencePayload {
        evidence_type: input.evidence_type.to_string(),
        contract_address: input.contract_address.to_string(),
        token_id: input.token_id.to_string(),
        tx_hash: input.tx_hash.to_string(),
        weight: input.weight,
        detail: input.detail.to_string(),
    });
}

fn apply_operator_level(entry: &mut AttributionAccumulator, level: i64, label: &str) {
    if level > entry.operator_level {
        entry.operator_level = level;
        entry.operator_level_label = label.to_string();
    }
}

fn attribution_confidence(
    operator_score: f64,
    colluder_score: f64,
    victim_score: f64,
    corruption_score: f64,
) -> String {
    let best = operator_score
        .max(colluder_score)
        .max(victim_score)
        .max(corruption_score);
    let second = [
        operator_score,
        colluder_score,
        victim_score,
        corruption_score,
    ]
    .into_iter()
    .filter(|value| *value < best)
    .max_by(|left, right| left.total_cmp(right))
    .unwrap_or(0.0);
    let margin = best - second;
    if best >= 0.75 && margin >= 0.25 {
        "high".into()
    } else if best >= 0.45 {
        "medium".into()
    } else {
        "low".into()
    }
}

fn attribution_label(
    operator_score: f64,
    colluder_score: f64,
    victim_score: f64,
    corruption_score: f64,
    neutral_score: f64,
) -> String {
    if corruption_score >= 0.40 && victim_score >= 0.20 {
        return "corrupted_victim".into();
    }
    if operator_score >= 0.25 && operator_score >= victim_score && operator_score >= colluder_score
    {
        return "suspected_operator".into();
    }
    if colluder_score >= 0.25 && colluder_score >= victim_score {
        return "suspected_colluder".into();
    }
    if victim_score >= 0.45 && victim_score >= operator_score && victim_score >= colluder_score {
        return "likely_victim".into();
    }
    if neutral_score >= 0.20 {
        return "neutral_participant".into();
    }
    "neutral_participant".into()
}

pub fn build_address_attribution_records(
    contract_address: &str,
    infringing_tokens: &[InfringingTokenRecord],
    sales: &[NftSaleRecord],
    mint_payment_edges: &[ValueFlowEdgePayload],
    malicious_addresses: &[MaliciousAddressPayload],
    honest_addresses: &[HonestAddressPayload],
    secondary_sale_victim_addresses: &[SecondarySaleVictimAddressPayload],
) -> Vec<AddressAttributionPayload> {
    let relevant_token_ids: HashSet<String> = infringing_tokens
        .iter()
        .filter(|item| !item.token_id.is_empty())
        .map(|item| item.token_id.clone())
        .collect();
    let mut rows = BTreeMap::<String, AttributionAccumulator>::new();

    for token in infringing_tokens {
        add_attribution_evidence(
            &mut rows,
            EvidenceInput {
                contract_address,
                address: &token.minter_address,
                role: "mint_recipient",
                evidence_type: "mint_recipient",
                token_id: &token.token_id,
                tx_hash: &token.mint_tx_hash,
                weight: 0.10,
                detail: "mint recipient is weak evidence only; paid mints may be victims",
                bucket: EvidenceBucket::Neutral,
            },
        );
        if token.official_or_legit_reissue {
            if let Some(entry) = attribution_entry(&mut rows, &token.minter_address) {
                entry.roles.insert("official_reissue".into());
            }
        }
    }

    for edge in mint_payment_edges {
        if edge.channel != "mint_payment"
            || !identities_equal(&edge.contract_address, contract_address)
            || edge.from_address.is_empty()
            || (edge.value_eth.unwrap_or(0.0) <= 0.0 && edge.value_usd.unwrap_or(0.0) <= 0.0)
        {
            continue;
        }
        let paid_to_controlled_recipient =
            matches!(
                edge.to_role.as_str(),
                "mint_contract"
                    | "contract_deployer"
                    | "contract_owner"
                    | "contract_admin"
                    | "proxy_admin"
                    | "operator_wallet"
            ) || identities_equal(&edge.to_address, contract_address);
        if !paid_to_controlled_recipient {
            continue;
        }
        add_attribution_evidence(
            &mut rows,
            EvidenceInput {
                contract_address,
                address: &edge.from_address,
                role: "paid_minter",
                evidence_type: "paid_mint_payment",
                token_id: &edge.token_id,
                tx_hash: &edge.tx_hash,
                weight: 0.45,
                detail: "address paid native or priced value to mint a copied NFT without independent operator evidence",
                bucket: EvidenceBucket::Victim,
            },
        );
    }

    for item in malicious_addresses {
        if let Some(entry) = attribution_entry(&mut rows, &item.address) {
            apply_operator_level(entry, item.operator_level, &item.operator_level_label);
        }
        if item.wash_cycle_count > 0 {
            add_attribution_evidence(
                &mut rows,
                EvidenceInput {
                    contract_address,
                    address: &item.address,
                    role: "wash_cycle",
                    evidence_type: "wash_cycle",
                    token_id: "",
                    tx_hash: "",
                    weight: 0.35,
                    detail: "address participates in reciprocal transfer cycles",
                    bucket: EvidenceBucket::Operator,
                },
            );
        }
        if item.star_out_degree >= 3 {
            add_attribution_evidence(
                &mut rows,
                EvidenceInput {
                    contract_address,
                    address: &item.address,
                    role: "star_distributor",
                    evidence_type: "star_distribution",
                    token_id: "",
                    tx_hash: "",
                    weight: 0.30,
                    detail: "address distributes copied NFTs to many unique receivers",
                    bucket: EvidenceBucket::Operator,
                },
            );
        }
        if item.sale_seller_count >= HIGH_VOLUME_SELLER_L1_THRESHOLD {
            add_attribution_evidence(
                &mut rows,
                EvidenceInput {
                    contract_address,
                    address: &item.address,
                    role: "high_volume_seller",
                    evidence_type: "high_volume_sale_seller",
                    token_id: "",
                    tx_hash: "",
                    weight: 0.25,
                    detail: "address sells copied NFTs across repeated sale events",
                    bucket: EvidenceBucket::Operator,
                },
            );
        }
        if item.withdrawal_edge_count > 0 {
            add_attribution_evidence(
                &mut rows,
                EvidenceInput {
                    contract_address,
                    address: &item.address,
                    role: "withdrawal_recipient",
                    evidence_type: "contract_value_withdrawal",
                    token_id: "",
                    tx_hash: "",
                    weight: 0.35,
                    detail: "address receives native or priced value withdrawn from the copied NFT contract",
                    bucket: EvidenceBucket::Operator,
                },
            );
        }
        if item.cashout_edge_count > 0 {
            add_attribution_evidence(
                &mut rows,
                EvidenceInput {
                    contract_address,
                    address: &item.address,
                    role: "cashout_intermediate",
                    evidence_type: "multi_hop_cashout",
                    token_id: "",
                    tx_hash: "",
                    weight: 0.25,
                    detail:
                        "address appears as an intermediate wallet in same-block cashout tracing",
                    bucket: EvidenceBucket::Colluder,
                },
            );
        }
        if !item.rapid_spread_contracts.is_empty() {
            add_attribution_evidence(
                &mut rows,
                EvidenceInput {
                    contract_address,
                    address: &item.address,
                    role: "rapid_spreader",
                    evidence_type: "rapid_spread",
                    token_id: "",
                    tx_hash: "",
                    weight: 0.20,
                    detail: "address appears in propagation within 24 hours of mint",
                    bucket: EvidenceBucket::Colluder,
                },
            );
        }
    }

    for sale in sales {
        if !identities_equal(&sale.contract_address, contract_address) {
            continue;
        }
        if !relevant_token_ids.is_empty() && !relevant_token_ids.contains(&sale.token_id) {
            continue;
        }
        let seller_key = normalize_chain_identity(&sale.seller_address);
        let seller_context = rows.get(&seller_key).map(|entry| {
            if entry.operator_score > 0.0 {
                Some(EvidenceBucket::Operator)
            } else if entry.colluder_score > 0.0 {
                Some(EvidenceBucket::Colluder)
            } else {
                None
            }
        });
        let (seller_bucket, seller_weight, seller_detail) =
            if let Some(Some(bucket)) = seller_context {
                (
                bucket,
                0.10,
                "address sold copied NFT and already has independent operator or colluder evidence",
            )
            } else {
                (
                EvidenceBucket::Neutral,
                0.10,
                "sale alone is weak evidence; ordinary paid minters or resellers may be victims",
            )
            };
        add_attribution_evidence(
            &mut rows,
            EvidenceInput {
                contract_address,
                address: &sale.seller_address,
                role: "seller",
                evidence_type: "infringing_sale_seller",
                token_id: &sale.token_id,
                tx_hash: &sale.tx_hash,
                weight: seller_weight,
                detail: seller_detail,
                bucket: seller_bucket,
            },
        );
        add_attribution_evidence(
            &mut rows,
            EvidenceInput {
                contract_address,
                address: &sale.buyer_address,
                role: "buyer",
                evidence_type: "marketplace_purchase",
                token_id: &sale.token_id,
                tx_hash: &sale.tx_hash,
                weight: if sale.price_eth.unwrap_or(0.0) > 0.0
                    || sale.price_usd.unwrap_or(0.0) > 0.0
                {
                    0.50
                } else {
                    0.30
                },
                detail: "address bought the copied NFT through a sale event",
                bucket: EvidenceBucket::Victim,
            },
        );
    }

    for item in secondary_sale_victim_addresses {
        add_attribution_evidence(
            &mut rows,
            EvidenceInput {
                contract_address,
                address: &item.address,
                role: "victim_candidate",
                evidence_type: "secondary_sale_victim_profile",
                token_id: "",
                tx_hash: &item.last_buy_tx_hash,
                weight: 0.20,
                detail: "address has a purchase profile in the victim candidate set",
                bucket: EvidenceBucket::Victim,
            },
        );
        if item.is_stuck {
            add_attribution_evidence(
                &mut rows,
                EvidenceInput {
                    contract_address,
                    address: &item.address,
                    role: "stuck_victim",
                    evidence_type: "stuck_holder",
                    token_id: "",
                    tx_hash: &item.last_buy_tx_hash,
                    weight: 0.25,
                    detail: "address still holds the copied NFT after purchase with no later outgoing transfer",
                    bucket: EvidenceBucket::Victim,
                },
            );
        }
    }

    for item in honest_addresses {
        if let Some(entry) = attribution_entry(&mut rows, &item.address) {
            entry.roles.insert("honest_holder".into());
            entry.neutral_score += 0.20;
        }
        if item.is_corrupted_address {
            add_attribution_evidence(
                &mut rows,
                EvidenceInput {
                    contract_address,
                    address: &item.address,
                    role: "corrupted_honest",
                    evidence_type: "corrupted_honest_resale",
                    token_id: "",
                    tx_hash: "",
                    weight: 0.45,
                    detail:
                        "address otherwise looks like a participant but also propagates copied NFTs",
                    bucket: EvidenceBucket::Corruption,
                },
            );
        }
    }

    rows.into_iter()
        .map(|(address, row)| {
            let operator_score = row.operator_score.clamp(0.0, 1.0);
            let colluder_score = row.colluder_score.clamp(0.0, 1.0);
            let attacker_score = row.attacker_score.clamp(0.0, 1.0);
            let victim_score = row.victim_score.clamp(0.0, 1.0);
            let corruption_score = row.corruption_score.clamp(0.0, 1.0);
            let neutral_score = row.neutral_score.clamp(0.0, 1.0);
            AddressAttributionPayload {
                contract_address: contract_address.to_string(),
                address,
                observed_roles: row.roles.into_iter().collect(),
                attribution_label: attribution_label(
                    operator_score,
                    colluder_score,
                    victim_score,
                    corruption_score,
                    neutral_score,
                ),
                operator_score,
                colluder_score,
                attacker_score,
                victim_score,
                corruption_score,
                neutral_score,
                confidence: attribution_confidence(
                    operator_score,
                    colluder_score,
                    victim_score,
                    corruption_score,
                ),
                operator_level: row.operator_level,
                operator_level_label: row.operator_level_label,
                evidence: row.evidence,
            }
        })
        .collect()
}

pub fn add_acquisition_exposure_attribution_evidence(
    address_attributions: Vec<AddressAttributionPayload>,
    victim_acquisition_addresses: &[VictimAcquisitionAddressPayload],
) -> Vec<AddressAttributionPayload> {
    let mut rows: BTreeMap<(String, String), AddressAttributionPayload> = address_attributions
        .into_iter()
        .map(|item| {
            (
                (
                    normalize_chain_identity(&item.contract_address),
                    normalize_chain_identity(&item.address),
                ),
                item,
            )
        })
        .collect();

    for acquisition in victim_acquisition_addresses {
        if !acquisition
            .buy_asset_ratio
            .map(|ratio| ratio >= 0.60)
            .unwrap_or(false)
        {
            continue;
        }
        for contract_address in &acquisition.contract_addresses {
            let contract_address = normalize_chain_identity(contract_address);
            let address = normalize_chain_identity(&acquisition.address);
            if contract_address.is_empty() || address.is_empty() || address == ZERO_ADDRESS {
                continue;
            }
            let entry = rows
                .entry((contract_address.clone(), address.clone()))
                .or_insert_with(|| AddressAttributionPayload {
                    contract_address: contract_address.clone(),
                    address: address.clone(),
                    attribution_label: "likely_victim".into(),
                    confidence: "low".into(),
                    ..AddressAttributionPayload::default()
                });
            if !entry
                .observed_roles
                .iter()
                .any(|role| role == "high_exposure_acquirer")
            {
                entry.observed_roles.push("high_exposure_acquirer".into());
                entry.observed_roles.sort();
            }
            entry.victim_score = (entry.victim_score + 0.15).clamp(0.0, 1.0);
            if !entry
                .evidence
                .iter()
                .any(|evidence| evidence.evidence_type == "high_acquisition_balance_ratio")
            {
                entry.evidence.push(AddressEvidencePayload {
                    evidence_type: "high_acquisition_balance_ratio".into(),
                    contract_address: contract_address.clone(),
                    token_id: String::new(),
                    tx_hash: acquisition.tx_hashes.first().cloned().unwrap_or_default(),
                    weight: 0.15,
                    detail:
                        "total acquisition cost consumed a high share of the observed pre-acquisition ETH balance"
                            .into(),
                });
            }
            entry.attribution_label = attribution_label(
                entry.operator_score,
                entry.colluder_score,
                entry.victim_score,
                entry.corruption_score,
                entry.neutral_score,
            );
            entry.confidence = attribution_confidence(
                entry.operator_score,
                entry.colluder_score,
                entry.victim_score,
                entry.corruption_score,
            );
        }
    }

    rows.into_values().collect()
}
