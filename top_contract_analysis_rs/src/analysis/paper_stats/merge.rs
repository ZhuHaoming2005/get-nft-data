use super::*;

pub fn merge_paper_stats<I, S>(seed_stats: I, config: PaperStatsConfig) -> PaperStatsPayload
where
    I: IntoIterator<Item = S>,
    S: std::borrow::Borrow<PaperStatsPayload>,
{
    let mut duplicate_rows = BTreeMap::<String, DuplicateScaleAccumulator>::new();
    let mut honest_loss = HonestLossAccumulator::default();
    let mut duplicate_nft_keys = BTreeMap::<String, BTreeSet<String>>::new();
    let mut duplicate_contract_keys = BTreeMap::<String, BTreeSet<String>>::new();
    let mut duplicate_contract_denominator_keys = BTreeSet::<String>::new();
    let mut behavior_contracts = BTreeMap::<String, BTreeSet<String>>::new();
    let mut behavior_addresses = BTreeMap::<String, BTreeSet<String>>::new();
    let mut behavior_nfts = BTreeMap::<String, BTreeSet<String>>::new();
    let mut behavior_buyers = BTreeMap::<String, BTreeSet<String>>::new();
    let mut behavior_contract_denominator_keys = BTreeSet::<String>::new();
    let mut merged = PaperStatsPayload::default();
    let mut has_positive_attacker_cost_without_details = false;
    let mut legacy_attacker_cost = PaperAttackerCostPayload::default();
    let mut legacy_attacker_cost_by_contract_usd = BTreeMap::<String, f64>::new();
    let mut saw_history_quality = false;
    let mut history_complete = true;

    for stats in seed_stats {
        let stats = stats.borrow();
        for row in &stats.duplicate_scale {
            let entry = duplicate_rows.entry(row.category.clone()).or_default();
            entry.duplicate_nft_count += row.duplicate_nft_count;
            entry.duplicate_nft_denominator += row.duplicate_nft_ratio_denominator;
            entry.duplicate_contract_count += row.duplicate_contract_count;
            entry.duplicate_contract_denominator += row.duplicate_contract_ratio_denominator;
        }
        merge_vec_map_as_sets(
            &mut duplicate_nft_keys,
            &stats.duplicate_nft_keys_by_category,
        );
        merge_vec_map_as_sets(
            &mut duplicate_contract_keys,
            &stats.duplicate_contract_keys_by_category,
        );
        duplicate_contract_denominator_keys.extend(
            stats
                .duplicate_contract_denominator_keys
                .iter()
                .map(|contract| normalized_contract(contract)),
        );
        merge_vec_map_as_sets(&mut behavior_contracts, &stats.behavior_contracts_by_type);
        merge_vec_map_as_sets(&mut behavior_addresses, &stats.behavior_addresses_by_type);
        merge_vec_map_as_sets(&mut behavior_nfts, &stats.behavior_nfts_by_type);
        merge_vec_map_as_sets(&mut behavior_buyers, &stats.behavior_buyers_by_type);
        behavior_contract_denominator_keys.extend(
            stats
                .behavior_contract_denominator_keys
                .iter()
                .map(|contract| normalized_contract(contract))
                .filter(|contract| contract != "unknown"),
        );

        merged
            .malicious_addresses
            .extend(stats.malicious_addresses.iter().cloned());
        merged
            .honest_addresses
            .extend(stats.honest_addresses.iter().cloned());
        merged
            .repeat_infringing_malicious_addresses
            .extend(stats.repeat_infringing_malicious_addresses.iter().cloned());
        merged
            .contract_behavior_stats
            .extend(stats.contract_behavior_stats.iter().cloned());
        if stats.behavior_contract_denominator_keys.is_empty() {
            merged.behavior_contract_denominator += stats.behavior_contract_denominator;
        }

        merged.attacker_cost.setup_gas_eth += stats.attacker_cost.setup_gas_eth;
        merged.attacker_cost.setup_gas_usd += stats.attacker_cost.setup_gas_usd;
        merged.attacker_cost.lure_gas_eth += stats.attacker_cost.lure_gas_eth;
        merged.attacker_cost.lure_gas_usd += stats.attacker_cost.lure_gas_usd;
        merged.attacker_cost.exit_gas_eth += stats.attacker_cost.exit_gas_eth;
        merged.attacker_cost.exit_gas_usd += stats.attacker_cost.exit_gas_usd;
        merged.attacker_cost.total_gas_eth += stats.attacker_cost.total_gas_eth;
        merged.attacker_cost.total_gas_usd += stats.attacker_cost.total_gas_usd;
        let has_legacy_attacker_cost = stats.attacker_cost_details.is_empty()
            && (stats.attacker_cost.total_gas_eth > 0.0 || stats.attacker_cost.total_gas_usd > 0.0);
        if has_legacy_attacker_cost {
            has_positive_attacker_cost_without_details = true;
            add_attacker_cost_payload(&mut legacy_attacker_cost, &stats.attacker_cost);
            merge_f64_map(
                &mut legacy_attacker_cost_by_contract_usd,
                &stats.attacker_cost_by_contract_usd,
            );
        }
        merged
            .attacker_cost_details
            .extend(stats.attacker_cost_details.iter().cloned());

        merge_f64_map(
            &mut merged.attacker_cost_by_contract_usd,
            &stats.attacker_cost_by_contract_usd,
        );
        if stats.operator_output_by_contract_usd.is_empty() {
            for row in &stats.output_input_ratio_by_contract {
                if row.output_usd > 0.0 {
                    *merged
                        .operator_output_by_contract_usd
                        .entry(normalized_contract(&row.contract_address))
                        .or_default() += row.output_usd;
                }
            }
        } else {
            merge_f64_map(
                &mut merged.operator_output_by_contract_usd,
                &stats.operator_output_by_contract_usd,
            );
        }
        merge_f64_map(
            &mut merged.honest_loss_by_contract_usd,
            &stats.honest_loss_by_contract_usd,
        );
        merge_f64_map(
            &mut merged.stuck_time_numerator_by_contract,
            &stats.stuck_time_numerator_by_contract,
        );
        merge_f64_map(
            &mut merged.stuck_time_denominator_by_contract,
            &stats.stuck_time_denominator_by_contract,
        );

        let row = &stats.honest_loss;
        honest_loss.stuck_nft_count += row.stuck_nft_count;
        honest_loss.stuck_nft_denominator += row.stuck_nft_ratio_denominator;
        honest_loss.stuck_time_numerator += row.stuck_time_ratio_numerator;
        honest_loss.stuck_time_denominator += row.stuck_time_ratio_denominator;
        honest_loss.secondary_sale_loss_eth += row.secondary_sale_loss_eth;
        honest_loss.secondary_sale_loss_usd += row.secondary_sale_loss_usd;
        honest_loss.paid_mint_loss_eth += row.paid_mint_loss_eth;
        honest_loss.paid_mint_loss_usd += row.paid_mint_loss_usd;

        merged.data_quality.sale_price_parseable_count +=
            stats.data_quality.sale_price_parseable_count;
        merged.data_quality.sale_price_total_count += stats.data_quality.sale_price_total_count;
        merged.data_quality.representative_candidate_count +=
            stats.data_quality.representative_candidate_count;
        merged.data_quality.candidate_contract_count += stats.data_quality.candidate_contract_count;
        merged.data_quality.suspected_duplicate_contract_count +=
            stats.data_quality.suspected_duplicate_contract_count;
        merged.data_quality.infringing_nft_count += stats.data_quality.infringing_nft_count;
        merged.data_quality.legit_duplicate_contract_count +=
            stats.data_quality.legit_duplicate_contract_count;
        merged.data_quality.asset_listing_analyzed_count +=
            stats.data_quality.asset_listing_analyzed_count;
        merged.data_quality.asset_listing_total_count +=
            stats.data_quality.asset_listing_total_count;
        merged.data_quality.asset_listing_truncated_contract_count +=
            stats.data_quality.asset_listing_truncated_contract_count;
        merged
            .data_quality
            .asset_listing_unknown_total_contract_count += stats
            .data_quality
            .asset_listing_unknown_total_contract_count;
        merged.data_quality.history_failed_asset_count +=
            stats.data_quality.history_failed_asset_count;
        merged.data_quality.history_requested_asset_count +=
            stats.data_quality.history_requested_asset_count;
        merged.data_quality.history_successful_asset_count +=
            stats.data_quality.history_successful_asset_count;
        merged.data_quality.history_complete_asset_count +=
            stats.data_quality.history_complete_asset_count;
        merged.data_quality.history_unrequested_asset_count +=
            stats.data_quality.history_unrequested_asset_count;
        merged.data_quality.history_truncated_asset_count +=
            stats.data_quality.history_truncated_asset_count;
        merged.data_quality.history_fetched_transaction_count +=
            stats.data_quality.history_fetched_transaction_count;
        merged.data_quality.history_reported_transaction_count +=
            stats.data_quality.history_reported_transaction_count;
        merged.data_quality.history_failed_transaction_count +=
            stats.data_quality.history_failed_transaction_count;
        merged
            .data_quality
            .history_signature_discovery_failure_count +=
            stats.data_quality.history_signature_discovery_failure_count;
        merged.data_quality.history_transaction_detail_failure_count +=
            stats.data_quality.history_transaction_detail_failure_count;
        merged
            .data_quality
            .history_unattributed_sol_transaction_count += stats
            .data_quality
            .history_unattributed_sol_transaction_count;
        merged.data_quality.history_unresolved_compressed_mint_count +=
            stats.data_quality.history_unresolved_compressed_mint_count;
        merged.data_quality.mint_pre_balance_unavailable_count +=
            stats.data_quality.mint_pre_balance_unavailable_count;
        merged.data_quality.collection_authority_missing_count +=
            stats.data_quality.collection_authority_missing_count;
        if stats.data_quality.asset_listing_analyzed_count > 0
            || stats.data_quality.history_requested_asset_count > 0
        {
            saw_history_quality = true;
            history_complete &= stats.data_quality.history_complete;
        }
        merged.data_quality.supplemental_provider_failure_count +=
            stats.data_quality.supplemental_provider_failure_count;
        merged.data_quality.provider_quality_lookup_failure_count +=
            stats.data_quality.provider_quality_lookup_failure_count;
    }

    merged.malicious_addresses = dedup_strings(merged.malicious_addresses);
    merged.honest_addresses = dedup_strings(merged.honest_addresses);
    merged.repeat_infringing_malicious_addresses =
        dedup_strings(merged.repeat_infringing_malicious_addresses);
    merged.address_classification = PaperAddressClassificationPayload {
        malicious_address_count: merged.malicious_addresses.len() as i64,
        repeat_infringing_malicious_address_count: merged
            .repeat_infringing_malicious_addresses
            .len() as i64,
        honest_address_count: merged.honest_addresses.len() as i64,
        total_address_count: merged
            .malicious_addresses
            .iter()
            .chain(merged.honest_addresses.iter())
            .collect::<BTreeSet<_>>()
            .len() as i64,
    };

    let duplicate_nft_denominator = duplicate_nft_keys
        .get("total")
        .map(|keys| keys.len() as i64)
        .or_else(|| {
            duplicate_rows
                .get("total")
                .map(|row| row.duplicate_nft_count)
        })
        .unwrap_or_default();
    merged.duplicate_scale = duplicate_rows
        .into_iter()
        .map(|(category, row)| {
            let duplicate_nft_count = duplicate_nft_keys
                .get(&category)
                .map(|keys| keys.len() as i64)
                .unwrap_or(row.duplicate_nft_count);
            let duplicate_contract_count = duplicate_contract_keys
                .get(&category)
                .map(|keys| keys.len() as i64)
                .unwrap_or(row.duplicate_contract_count);
            let duplicate_contract_denominator = if duplicate_contract_denominator_keys.is_empty() {
                row.duplicate_contract_denominator
            } else {
                duplicate_contract_denominator_keys.len() as i64
            };
            PaperDuplicateScaleRowPayload {
                category,
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
            }
        })
        .collect();

    let total_loss_usd = honest_loss.secondary_sale_loss_usd + honest_loss.paid_mint_loss_usd;
    let contribution_contract_count = if duplicate_contract_denominator_keys.is_empty() {
        merged
            .attacker_cost_by_contract_usd
            .len()
            .max(merged.honest_loss_by_contract_usd.len())
    } else {
        duplicate_contract_denominator_keys.len()
    };
    let top_loss_numerator = top_contribution_numerator(
        &merged.honest_loss_by_contract_usd,
        config,
        contribution_contract_count,
    );
    let stuck_nft_denominator = if duplicate_nft_denominator > 0 {
        duplicate_nft_denominator
    } else {
        honest_loss.stuck_nft_denominator
    };
    merged.honest_loss = PaperHonestLossPayload {
        stuck_nft_count: honest_loss.stuck_nft_count,
        stuck_nft_ratio: ratio_i64(honest_loss.stuck_nft_count, stuck_nft_denominator),
        stuck_nft_ratio_numerator: honest_loss.stuck_nft_count,
        stuck_nft_ratio_denominator: stuck_nft_denominator,
        stuck_time_ratio: ratio_f64(
            honest_loss.stuck_time_numerator,
            honest_loss.stuck_time_denominator,
        ),
        stuck_time_ratio_numerator: honest_loss.stuck_time_numerator,
        stuck_time_ratio_denominator: honest_loss.stuck_time_denominator,
        secondary_sale_loss_eth: honest_loss.secondary_sale_loss_eth,
        secondary_sale_loss_usd: honest_loss.secondary_sale_loss_usd,
        paid_mint_loss_eth: honest_loss.paid_mint_loss_eth,
        paid_mint_loss_usd: honest_loss.paid_mint_loss_usd,
        total_loss_eth: honest_loss.secondary_sale_loss_eth + honest_loss.paid_mint_loss_eth,
        total_loss_usd,
        top_contract_loss_contribution_ratio: ratio_f64(top_loss_numerator, total_loss_usd),
        top_contract_loss_contribution_numerator: top_loss_numerator,
        top_contract_loss_contribution_denominator: total_loss_usd,
    };

    if !merged.attacker_cost_details.is_empty() {
        let mut attacker_cost = build_attacker_cost_from_details(
            config,
            &merged.attacker_cost_details,
            contribution_contract_count,
        );
        if has_positive_attacker_cost_without_details {
            add_legacy_attacker_cost_summary(
                &mut attacker_cost,
                &legacy_attacker_cost,
                &legacy_attacker_cost_by_contract_usd,
                config,
                contribution_contract_count,
            );
        }
        merged.attacker_cost = attacker_cost.payload;
        merged.attacker_cost_details = attacker_cost.details;
        merged.attacker_cost_by_contract_usd = attacker_cost.by_contract_usd;
    } else {
        merged.attacker_cost.top_contract_contribution_numerator = top_contribution_numerator(
            &merged.attacker_cost_by_contract_usd,
            config,
            contribution_contract_count,
        );
        merged.attacker_cost.top_contract_contribution_denominator =
            merged.attacker_cost.total_gas_usd;
        merged.attacker_cost.top_contract_contribution_ratio = ratio_f64(
            merged.attacker_cost.top_contract_contribution_numerator,
            merged.attacker_cost.top_contract_contribution_denominator,
        );
        sort_attacker_cost_details(&mut merged.attacker_cost_details);
    }
    let output_input_ratio = build_output_input_ratio(
        &merged.operator_output_by_contract_usd,
        &merged.attacker_cost_by_contract_usd,
    );
    merged.output_input_summary = output_input_ratio.summary;
    merged.output_input_ratio_by_contract = output_input_ratio.rows;

    merged.data_quality.sale_price_parseable_ratio = ratio_i64(
        merged.data_quality.sale_price_parseable_count,
        merged.data_quality.sale_price_total_count,
    );
    merged.data_quality.sale_price_parseable_ratio_numerator =
        merged.data_quality.sale_price_parseable_count;
    merged.data_quality.sale_price_parseable_ratio_denominator =
        merged.data_quality.sale_price_total_count;
    merged.data_quality.asset_listing_coverage_ratio = (merged
        .data_quality
        .asset_listing_unknown_total_contract_count
        == 0)
        .then(|| {
            ratio_i64(
                merged.data_quality.asset_listing_analyzed_count,
                merged.data_quality.asset_listing_total_count,
            )
        })
        .flatten();
    merged.data_quality.history_asset_coverage_ratio = ratio_i64(
        merged.data_quality.history_successful_asset_count,
        merged.data_quality.history_requested_asset_count,
    );
    merged.data_quality.history_complete = saw_history_quality
        && history_complete
        && merged.data_quality.provider_quality_lookup_failure_count == 0;
    merged.data_quality.history_transaction_coverage_ratio =
        (merged.data_quality.history_failed_asset_count == 0)
            .then(|| {
                ratio_i64(
                    merged.data_quality.history_fetched_transaction_count,
                    merged.data_quality.history_reported_transaction_count,
                )
            })
            .flatten();
    if !duplicate_contract_denominator_keys.is_empty() {
        merged.data_quality.suspected_duplicate_contract_count =
            duplicate_contract_denominator_keys.len() as i64;
    }
    if let Some(total_duplicate_nft_keys) = duplicate_nft_keys.get("total") {
        merged.data_quality.infringing_nft_count = total_duplicate_nft_keys.len() as i64;
    }
    let behavior_contract_denominator = if !behavior_contract_denominator_keys.is_empty() {
        behavior_contract_denominator_keys.len()
    } else if merged.behavior_contract_denominator > 0 {
        merged.behavior_contract_denominator as usize
    } else {
        merged.contract_behavior_stats.len()
    };
    merged.behavior_contract_denominator = behavior_contract_denominator as i64;
    merged.malicious_behavior_summary = build_behavior_summary(
        &merged.contract_behavior_stats,
        behavior_contract_denominator,
        &behavior_contracts,
        &behavior_addresses,
        &behavior_nfts,
        &behavior_buyers,
    );
    merged.wash_cycle_size_distribution =
        wash_cycle_size_distribution_for_contracts(&merged.contract_behavior_stats);
    merged.wash_cycle_size_by_contract =
        wash_cycle_size_by_contract_from_stats(&merged.contract_behavior_stats);
    merged.duplicate_nft_keys_by_category = sets_to_vecs(duplicate_nft_keys);
    merged.duplicate_contract_keys_by_category = sets_to_vecs(duplicate_contract_keys);
    merged.duplicate_contract_denominator_keys =
        duplicate_contract_denominator_keys.into_iter().collect();
    merged.behavior_contract_denominator_keys =
        behavior_contract_denominator_keys.into_iter().collect();
    merged.behavior_contracts_by_type = sets_to_vecs(behavior_contracts);
    merged.behavior_addresses_by_type = sets_to_vecs(behavior_addresses);
    merged.behavior_nfts_by_type = sets_to_vecs(behavior_nfts);
    merged.behavior_buyers_by_type = sets_to_vecs(behavior_buyers);

    merged
}

pub(super) fn merge_f64_map(target: &mut BTreeMap<String, f64>, source: &BTreeMap<String, f64>) {
    for (key, value) in source {
        *target.entry(key.clone()).or_default() += value;
    }
}

pub(super) fn add_attacker_cost_payload(
    target: &mut PaperAttackerCostPayload,
    source: &PaperAttackerCostPayload,
) {
    target.setup_gas_eth += source.setup_gas_eth;
    target.setup_gas_usd += source.setup_gas_usd;
    target.lure_gas_eth += source.lure_gas_eth;
    target.lure_gas_usd += source.lure_gas_usd;
    target.exit_gas_eth += source.exit_gas_eth;
    target.exit_gas_usd += source.exit_gas_usd;
    target.total_gas_eth += source.total_gas_eth;
    target.total_gas_usd += source.total_gas_usd;
}

pub(super) fn merge_set_maps(
    target: &mut BTreeMap<String, BTreeSet<String>>,
    source: BTreeMap<String, BTreeSet<String>>,
) {
    for (key, values) in source {
        target.entry(key).or_default().extend(values);
    }
}

fn merge_vec_map_as_sets(
    target: &mut BTreeMap<String, BTreeSet<String>>,
    source: &BTreeMap<String, Vec<String>>,
) {
    for (key, values) in source {
        target
            .entry(key.clone())
            .or_default()
            .extend(values.iter().cloned());
    }
}

pub(super) fn sets_to_vecs(
    source: BTreeMap<String, BTreeSet<String>>,
) -> BTreeMap<String, Vec<String>> {
    source
        .into_iter()
        .map(|(key, values)| (key, values.into_iter().collect()))
        .collect()
}

fn dedup_strings(values: Vec<String>) -> Vec<String> {
    values
        .into_iter()
        .map(|value| normalized_address(&value))
        .filter(|value| is_participant_address(value))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}
