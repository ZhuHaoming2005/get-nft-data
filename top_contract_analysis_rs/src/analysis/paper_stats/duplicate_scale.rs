use super::*;

#[derive(Default)]
pub(super) struct DuplicateScaleAccumulator {
    pub(super) duplicate_nft_count: i64,
    pub(super) duplicate_nft_denominator: i64,
    pub(super) duplicate_contract_count: i64,
    pub(super) duplicate_contract_denominator: i64,
}

#[derive(Default)]
pub(super) struct DuplicateScaleBuild {
    pub(super) rows: Vec<PaperDuplicateScaleRowPayload>,
    pub(super) nft_keys_by_category: BTreeMap<String, BTreeSet<String>>,
    pub(super) contract_keys_by_category: BTreeMap<String, BTreeSet<String>>,
    pub(super) contract_denominator_keys: BTreeSet<String>,
}

#[derive(Clone)]
pub(super) struct DuplicateEvidenceItem {
    pub(super) contract_address: String,
    pub(super) token_id: String,
    match_reasons: Vec<String>,
}

pub(super) fn duplicate_evidence_item(
    contract_address: &str,
    token_id: &str,
    match_reasons: &[String],
) -> Option<DuplicateEvidenceItem> {
    let contract_address = normalized_contract(contract_address);
    let token_id = token_id.trim();
    if contract_address == "unknown" || token_id.is_empty() {
        return None;
    }
    Some(DuplicateEvidenceItem {
        contract_address,
        token_id: token_id.to_string(),
        match_reasons: match_reasons.to_vec(),
    })
}

pub(super) fn duplicate_evidence_items(input: &PaperStatsInput<'_>) -> Vec<DuplicateEvidenceItem> {
    let infringing_items = input
        .infringing_tokens
        .iter()
        .filter(|token| !token.official_or_legit_reissue)
        .filter_map(|token| {
            duplicate_evidence_item(
                &token.contract_address,
                &token.token_id,
                &token.match_reasons,
            )
        })
        .collect::<Vec<_>>();
    if !infringing_items.is_empty() {
        return infringing_items;
    }

    input
        .duplicate_candidates
        .iter()
        .filter_map(|candidate| {
            duplicate_evidence_item(
                &candidate.contract_address,
                &candidate.token_id,
                &candidate.match_reasons,
            )
        })
        .collect()
}

fn evidence_matches_category(item: &DuplicateEvidenceItem, category: &str) -> bool {
    match_reasons_match_category(&item.match_reasons, category)
}

pub(super) fn duplicate_contract_key_set(
    input: &PaperStatsInput<'_>,
    evidence_items: &[DuplicateEvidenceItem],
) -> BTreeSet<String> {
    let mut keys = input
        .duplicate_contracts
        .iter()
        .map(|contract| normalized_contract(&contract.contract_address))
        .filter(|contract| contract != "unknown")
        .collect::<BTreeSet<_>>();
    keys.extend(
        evidence_items
            .iter()
            .map(|item| item.contract_address.clone())
            .filter(|contract| contract != "unknown"),
    );
    keys
}

pub(super) fn build_duplicate_scale(input: &PaperStatsInput<'_>) -> DuplicateScaleBuild {
    let categories = ["token_uri", "image_uri", "metadata", "name", "total"];
    let evidence_items = duplicate_evidence_items(input);
    let contract_denominator_keys = duplicate_contract_key_set(input, &evidence_items);
    let duplicate_contract_denominator = contract_denominator_keys.len() as i64;
    let duplicate_nft_denominator = evidence_items
        .iter()
        .map(|item| format!("{}:{}", item.contract_address, item.token_id.trim()))
        .collect::<BTreeSet<_>>()
        .len() as i64;

    let rows = categories
        .par_iter()
        .map(|category| {
            let nft_keys = evidence_items
                .iter()
                .filter(|item| evidence_matches_category(item, category))
                .map(|candidate| {
                    format!(
                        "{}:{}",
                        normalized_contract(&candidate.contract_address),
                        candidate.token_id.trim()
                    )
                })
                .collect::<BTreeSet<_>>();
            let contract_keys = if *category == "total" {
                contract_denominator_keys.clone()
            } else {
                evidence_items
                    .iter()
                    .filter(|item| evidence_matches_category(item, category))
                    .map(|item| normalized_contract(&item.contract_address))
                    .filter(|contract| contract != "unknown")
                    .collect::<BTreeSet<_>>()
            };
            let duplicate_nft_count = nft_keys.len() as i64;
            let duplicate_contract_count = contract_keys.len() as i64;

            (
                (*category).to_string(),
                nft_keys,
                contract_keys,
                PaperDuplicateScaleRowPayload {
                    category: (*category).to_string(),
                    duplicate_nft_count,
                    duplicate_nft_ratio: ratio_i64(duplicate_nft_count, duplicate_nft_denominator),
                    duplicate_nft_ratio_numerator: duplicate_nft_count,
                    duplicate_nft_ratio_denominator: duplicate_nft_denominator,
                    duplicate_contract_count,
                    duplicate_contract_ratio: ratio_i64(
                        duplicate_contract_count,
                        duplicate_contract_denominator,
                    ),
                    duplicate_contract_ratio_numerator: duplicate_contract_count,
                    duplicate_contract_ratio_denominator: duplicate_contract_denominator,
                },
            )
        })
        .collect::<Vec<_>>();

    DuplicateScaleBuild {
        rows: rows.iter().map(|(_, _, _, row)| row.clone()).collect(),
        nft_keys_by_category: rows
            .iter()
            .map(|(category, nft_keys, _, _)| (category.clone(), nft_keys.clone()))
            .collect(),
        contract_keys_by_category: rows
            .iter()
            .map(|(category, _, contract_keys, _)| (category.clone(), contract_keys.clone()))
            .collect(),
        contract_denominator_keys,
    }
}

pub(super) fn behavior_contract_denominator_keys(input: &PaperStatsInput<'_>) -> BTreeSet<String> {
    let mut keys = input
        .duplicate_contracts
        .iter()
        .map(|contract| normalized_contract(&contract.contract_address))
        .filter(|contract| contract != "unknown")
        .collect::<BTreeSet<_>>();
    for (contract_key, path) in input.nft_propagation_paths {
        let contract = if path.contract_address.trim().is_empty() {
            normalized_contract(contract_key)
        } else {
            normalized_contract(&path.contract_address)
        };
        if contract != "unknown" {
            keys.insert(contract);
        }
    }
    keys
}

pub(super) fn total_duplicate_nft_count(duplicate_scale: &DuplicateScaleBuild) -> i64 {
    duplicate_scale
        .rows
        .iter()
        .find(|row| row.category == "total")
        .map(|row| row.duplicate_nft_count)
        .unwrap_or_default()
}

pub(super) fn build_address_sets(input: &PaperStatsInput<'_>) -> AddressSets {
    let mut participants = BTreeSet::<String>::new();
    let mut honest_addresses = BTreeSet::<String>::new();
    let mut address_contracts = BTreeMap::<String, BTreeSet<String>>::new();

    for address in input
        .malicious_addresses
        .iter()
        .map(|item| normalized_address(&item.address))
    {
        if is_participant_address(&address) {
            participants.insert(address);
        }
    }
    for item in input.malicious_addresses {
        let address = normalized_address(&item.address);
        if !is_participant_address(&address) {
            continue;
        }
        for contract in &item.evidence_contracts {
            let contract = normalized_contract(contract);
            address_contracts
                .entry(address.clone())
                .or_default()
                .insert(contract);
        }
    }

    for item in input.victim_acquisition_addresses {
        let address = normalized_address(&item.address);
        if !is_participant_address(&address) {
            continue;
        }
        participants.insert(address.clone());
        if item.is_stuck
            || item.total_stuck_cost_eth > 0.0
            || item.total_stuck_cost_usd > 0.0
            || item.secondary_sale_stuck_cost_eth > 0.0
            || item.secondary_sale_stuck_cost_usd > 0.0
            || item.paid_mint_stuck_cost_eth > 0.0
            || item.paid_mint_stuck_cost_usd > 0.0
            || item.paid_mint_stuck_token_count > 0
        {
            honest_addresses.insert(address.clone());
        }
        for contract in &item.contract_addresses {
            let contract = normalized_contract(contract);
            address_contracts
                .entry(address.clone())
                .or_default()
                .insert(contract);
        }
    }

    for (contract_key, path) in input.nft_propagation_paths {
        let contract = if path.contract_address.trim().is_empty() {
            normalized_contract(contract_key)
        } else {
            normalized_contract(&path.contract_address)
        };
        for (node_key, node) in &path.nodes {
            let address = if node.address.trim().is_empty() {
                normalized_address(node_key)
            } else {
                normalized_address(&node.address)
            };
            if !is_participant_address(&address) {
                continue;
            }
            participants.insert(address.clone());
            address_contracts
                .entry(address.clone())
                .or_default()
                .insert(contract.clone());
            if node.is_stuck_victim
                || node
                    .roles
                    .iter()
                    .any(|role| role == "victim_buyer" || role == "honest")
                    && node.current_holding_token_count > 0
            {
                honest_addresses.insert(address);
            }
        }
        for edge in &path.edges {
            for address in [
                normalized_address(&edge.from_address),
                normalized_address(&edge.to_address),
            ] {
                if !is_participant_address(&address) {
                    continue;
                }
                participants.insert(address.clone());
                address_contracts
                    .entry(address)
                    .or_default()
                    .insert(contract.clone());
            }
        }
    }

    for edge in input.value_flow_edges {
        let contract = normalized_contract(&edge.contract_address);
        for address in [
            normalized_address(&edge.from_address),
            normalized_address(&edge.to_address),
        ] {
            if !is_participant_address(&address) {
                continue;
            }
            participants.insert(address.clone());
            address_contracts
                .entry(address)
                .or_default()
                .insert(contract.clone());
        }
    }

    for token in input.infringing_tokens {
        let minter = normalized_address(&token.minter_address);
        let contract = normalized_contract(&token.contract_address);
        if is_participant_address(&minter) {
            participants.insert(minter.clone());
            address_contracts
                .entry(minter)
                .or_default()
                .insert(contract);
        }
    }

    let malicious = participants
        .difference(&honest_addresses)
        .cloned()
        .collect::<BTreeSet<_>>();
    let repeat_infringing_malicious = address_contracts
        .into_iter()
        .filter_map(|(address, contracts)| {
            (contracts.len() > 1 && malicious.contains(&address)).then_some(address)
        })
        .collect();

    AddressSets {
        malicious,
        honest: honest_addresses,
        repeat_infringing_malicious,
    }
}

pub(super) fn build_address_classification(
    address_sets: &AddressSets,
) -> PaperAddressClassificationPayload {
    PaperAddressClassificationPayload {
        malicious_address_count: address_sets.malicious.len() as i64,
        repeat_infringing_malicious_address_count: address_sets.repeat_infringing_malicious.len()
            as i64,
        honest_address_count: address_sets.honest.len() as i64,
        total_address_count: address_sets.malicious.union(&address_sets.honest).count() as i64,
    }
}
