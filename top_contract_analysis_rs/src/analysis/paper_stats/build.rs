use super::merge::{merge_set_maps, sets_to_vecs};
use super::*;

pub fn build_paper_stats(input: PaperStatsInput<'_>) -> PaperStatsPayload {
    let address_sets = build_address_sets(&input);
    let duplicate_scale = build_duplicate_scale(&input);
    let behavior_contract_denominator_keys = behavior_contract_denominator_keys(&input);
    let contract_behavior_builds = build_contract_behavior_stats(&input, &address_sets);
    let mut behavior_contracts_by_type = BTreeMap::<String, BTreeSet<String>>::new();
    let mut behavior_addresses_by_type = BTreeMap::<String, BTreeSet<String>>::new();
    let mut behavior_nfts_by_type = BTreeMap::<String, BTreeSet<String>>::new();
    let mut behavior_buyers_by_type = BTreeMap::<String, BTreeSet<String>>::new();
    let contract_behavior_stats = contract_behavior_builds
        .into_iter()
        .map(|build| {
            merge_set_maps(&mut behavior_contracts_by_type, build.behavior_contracts);
            merge_set_maps(&mut behavior_addresses_by_type, build.behavior_addresses);
            merge_set_maps(&mut behavior_nfts_by_type, build.behavior_nfts);
            merge_set_maps(&mut behavior_buyers_by_type, build.behavior_buyers);
            build.stats
        })
        .collect::<Vec<_>>();
    let contract_denominator = behavior_contract_denominator_keys
        .len()
        .max(input.nft_propagation_paths.len())
        .max(input.duplicate_contracts.len());
    let malicious_behavior_summary = build_behavior_summary(
        &contract_behavior_stats,
        contract_denominator,
        &behavior_contracts_by_type,
        &behavior_addresses_by_type,
        &behavior_nfts_by_type,
        &behavior_buyers_by_type,
    );
    let wash_cycle_size_distribution =
        wash_cycle_size_distribution_for_contracts(&contract_behavior_stats);
    let wash_cycle_size_by_contract = wash_cycle_size_by_contract(&input, &contract_behavior_stats);
    let explicit_malicious_addresses = explicit_malicious_address_set(input.malicious_addresses);
    let duplicate_contract_count = duplicate_scale.contract_denominator_keys.len();
    let attacker_cost = build_attacker_cost(
        input.config,
        input.value_flow_edges,
        &address_sets,
        &explicit_malicious_addresses,
        duplicate_contract_count,
    );
    let honest_loss = build_honest_loss(
        input.config,
        &address_sets,
        input.victim_acquisition_addresses,
        input.nft_propagation_paths,
        total_duplicate_nft_count(&duplicate_scale),
        duplicate_contract_count,
    );
    let operator_output_by_contract_usd = build_operator_output_by_contract(input.value_flow_edges);
    let output_input_ratio = build_output_input_ratio(
        &operator_output_by_contract_usd,
        &attacker_cost.by_contract_usd,
    );

    PaperStatsPayload {
        duplicate_scale: duplicate_scale.rows,
        address_classification: build_address_classification(&address_sets),
        contract_behavior_stats,
        malicious_behavior_summary,
        wash_cycle_size_distribution,
        wash_cycle_size_by_contract,
        attacker_cost: attacker_cost.payload,
        attacker_cost_details: attacker_cost.details,
        honest_loss: honest_loss.payload,
        output_input_summary: output_input_ratio.summary,
        output_input_ratio_by_contract: output_input_ratio.rows,
        data_quality: build_data_quality(&input),
        malicious_addresses: address_sets.malicious.into_iter().collect(),
        honest_addresses: address_sets.honest.into_iter().collect(),
        repeat_infringing_malicious_addresses: address_sets
            .repeat_infringing_malicious
            .into_iter()
            .collect(),
        attacker_cost_by_contract_usd: attacker_cost.by_contract_usd,
        operator_output_by_contract_usd,
        honest_loss_by_contract_usd: honest_loss.total_loss_by_contract_usd,
        stuck_time_numerator_by_contract: honest_loss.stuck_time_numerator_by_contract,
        stuck_time_denominator_by_contract: honest_loss.stuck_time_denominator_by_contract,
        behavior_contract_denominator: contract_denominator as i64,
        behavior_contract_denominator_keys: behavior_contract_denominator_keys
            .into_iter()
            .collect(),
        duplicate_nft_keys_by_category: sets_to_vecs(duplicate_scale.nft_keys_by_category),
        duplicate_contract_keys_by_category: sets_to_vecs(
            duplicate_scale.contract_keys_by_category,
        ),
        duplicate_contract_denominator_keys: duplicate_scale
            .contract_denominator_keys
            .into_iter()
            .collect(),
        behavior_contracts_by_type: sets_to_vecs(behavior_contracts_by_type),
        behavior_addresses_by_type: sets_to_vecs(behavior_addresses_by_type),
        behavior_nfts_by_type: sets_to_vecs(behavior_nfts_by_type),
        behavior_buyers_by_type: sets_to_vecs(behavior_buyers_by_type),
    }
}

fn build_data_quality(input: &PaperStatsInput<'_>) -> PaperDataQualityPayload {
    let mut sale_price_total_count = 0_i64;
    let mut sale_price_parseable_count = 0_i64;
    for path in input.nft_propagation_paths.values() {
        for edge in &path.edges {
            if edge.channel != "sale" {
                continue;
            }
            sale_price_total_count += 1;
            if edge.price_usd.filter(|value| *value > 0.0).is_some()
                || edge.price_eth.filter(|value| *value > 0.0).is_some()
            {
                sale_price_parseable_count += 1;
            }
        }
    }
    let representative_candidate_count = input
        .duplicate_candidates
        .iter()
        .filter_map(|candidate| {
            duplicate_evidence_item(
                &candidate.contract_address,
                &candidate.token_id,
                &candidate.match_reasons,
            )
        })
        .map(|item| format!("{}:{}", item.contract_address, item.token_id))
        .collect::<BTreeSet<_>>()
        .len() as i64;
    let candidate_contract_count = input
        .duplicate_candidates
        .iter()
        .map(|candidate| normalized_contract(&candidate.contract_address))
        .filter(|contract| contract != "unknown")
        .collect::<BTreeSet<_>>()
        .len() as i64;
    let evidence_items = duplicate_evidence_items(input);
    let suspected_duplicate_contract_count =
        duplicate_contract_key_set(input, &evidence_items).len() as i64;
    let infringing_nft_count = evidence_items
        .iter()
        .map(|item| format!("{}:{}", item.contract_address, item.token_id))
        .collect::<BTreeSet<_>>()
        .len() as i64;

    PaperDataQualityPayload {
        representative_candidate_count,
        candidate_contract_count,
        suspected_duplicate_contract_count,
        infringing_nft_count,
        sale_price_parseable_count,
        sale_price_total_count,
        sale_price_parseable_ratio: ratio_i64(sale_price_parseable_count, sale_price_total_count),
        sale_price_parseable_ratio_numerator: sale_price_parseable_count,
        sale_price_parseable_ratio_denominator: sale_price_total_count,
        legit_duplicate_contract_count: input.legit_duplicates.len() as i64,
        ..PaperDataQualityPayload::default()
    }
}
