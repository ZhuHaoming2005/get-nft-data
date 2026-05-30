use std::cmp::Ordering;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};

use rayon::prelude::*;

use crate::models::{
    DuplicateCandidate, DuplicateContractPayload, InfringingTokenRecord, MaliciousAddressPayload,
    NftPropagationEdgePayload, NftPropagationPathPayload, PaperAddressClassificationPayload,
    PaperAttackerCostPayload, PaperBehaviorSummaryRowPayload, PaperContractBehaviorStatsPayload,
    PaperDataQualityPayload, PaperDuplicateScaleRowPayload, PaperHonestBuyerRowPayload,
    PaperHonestLossPayload, PaperInventoryConcentrationRowPayload, PaperLayeredTransferRowPayload,
    PaperPumpExitRowPayload, PaperStarBehaviorRowPayload, PaperStatsPayload,
    PaperWashTradingRowPayload, SeedCollectionStatsPayload, ValueFlowEdgePayload,
    VictimAcquisitionAddressPayload, ZERO_ADDRESS,
};

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PaperStatsConfig {
    pub min_cycle_size: usize,
    pub min_path_length: usize,
    pub center_fanout_threshold: usize,
    pub concentration_top_pct: f64,
    pub analysis_timestamp: i64,
}

impl Default for PaperStatsConfig {
    fn default() -> Self {
        Self {
            min_cycle_size: 2,
            min_path_length: 3,
            center_fanout_threshold: 3,
            concentration_top_pct: 0.1,
            analysis_timestamp: 0,
        }
    }
}

impl PaperStatsConfig {
    fn min_cycle_size(self) -> usize {
        self.min_cycle_size.max(2)
    }

    fn min_path_length(self) -> usize {
        self.min_path_length.max(2)
    }

    fn fanout_threshold(self) -> usize {
        self.center_fanout_threshold.max(1)
    }

    fn top_contract_count(self, total_contract_count: usize) -> usize {
        if total_contract_count == 0 {
            return 0;
        }
        let pct = if self.concentration_top_pct.is_finite() {
            self.concentration_top_pct.clamp(0.0, 1.0)
        } else {
            PaperStatsConfig::default().concentration_top_pct
        };
        ((total_contract_count as f64 * pct).ceil() as usize)
            .max(1)
            .min(total_contract_count)
    }
}

pub struct PaperStatsInput<'a> {
    pub config: PaperStatsConfig,
    pub seed_collection_stats: &'a SeedCollectionStatsPayload,
    pub duplicate_candidates: &'a [DuplicateCandidate],
    pub duplicate_contracts: &'a [DuplicateContractPayload],
    pub legit_duplicates: &'a [DuplicateContractPayload],
    pub infringing_tokens: &'a [InfringingTokenRecord],
    pub malicious_addresses: &'a [MaliciousAddressPayload],
    pub victim_acquisition_addresses: &'a [VictimAcquisitionAddressPayload],
    pub value_flow_edges: &'a [ValueFlowEdgePayload],
    pub nft_propagation_paths: &'a BTreeMap<String, NftPropagationPathPayload>,
}

#[derive(Default)]
struct DuplicateScaleAccumulator {
    duplicate_nft_count: i64,
    duplicate_nft_denominator: i64,
    duplicate_contract_count: i64,
    duplicate_contract_denominator: i64,
}

#[derive(Default)]
struct DuplicateScaleBuild {
    rows: Vec<PaperDuplicateScaleRowPayload>,
    nft_keys_by_category: BTreeMap<String, BTreeSet<String>>,
    contract_keys_by_category: BTreeMap<String, BTreeSet<String>>,
    contract_denominator_keys: BTreeSet<String>,
}

#[derive(Default)]
struct HonestLossAccumulator {
    stuck_nft_count: i64,
    stuck_nft_denominator: i64,
    stuck_time_numerator: f64,
    stuck_time_denominator: f64,
    secondary_sale_loss_eth: f64,
    secondary_sale_loss_usd: f64,
    paid_mint_loss_eth: f64,
    paid_mint_loss_usd: f64,
}

#[derive(Default)]
struct AddressSets {
    malicious: BTreeSet<String>,
    honest: BTreeSet<String>,
    repeat_infringing_malicious: BTreeSet<String>,
}

#[derive(Default)]
struct AttackerCostBuild {
    payload: PaperAttackerCostPayload,
    by_contract_usd: BTreeMap<String, f64>,
}

#[derive(Default)]
struct HonestLossBuild {
    rows: Vec<PaperHonestLossPayload>,
    total_loss_by_contract_usd: BTreeMap<String, f64>,
    stuck_time_numerator_by_contract: BTreeMap<String, f64>,
    stuck_time_denominator_by_contract: BTreeMap<String, f64>,
}

struct LossRowInput<'a> {
    category: &'a str,
    stuck_nft_count: i64,
    total_nft_count: i64,
    stuck_time_numerator: f64,
    stuck_time_denominator: f64,
    secondary_sale_loss_eth: f64,
    secondary_sale_loss_usd: f64,
    paid_mint_loss_eth: f64,
    paid_mint_loss_usd: f64,
    top_loss_numerator: f64,
    top_loss_denominator: f64,
}

#[derive(Default)]
struct BehaviorMeasure {
    instance_count: i64,
    address_count: i64,
    nft_count: i64,
    buyer_count: i64,
    linked_loss_eth: f64,
    linked_loss_usd: f64,
}

#[derive(Default)]
struct ContractBehaviorBuild {
    stats: PaperContractBehaviorStatsPayload,
    behavior_contracts: BTreeMap<String, BTreeSet<String>>,
    behavior_addresses: BTreeMap<String, BTreeSet<String>>,
    behavior_nfts: BTreeMap<String, BTreeSet<String>>,
    behavior_buyers: BTreeMap<String, BTreeSet<String>>,
}

struct PumpExitPattern {
    row: PaperPumpExitRowPayload,
    addresses: BTreeSet<String>,
    token_ids: BTreeSet<String>,
    buyers: BTreeSet<String>,
}

struct StarBehaviorPattern {
    row: PaperStarBehaviorRowPayload,
    addresses: BTreeSet<String>,
    token_ids: BTreeSet<String>,
}

#[derive(Default)]
struct StarBehaviorBuild {
    centers: BTreeSet<usize>,
    edge_count: i64,
    wallets: BTreeSet<String>,
    token_ids: BTreeSet<String>,
    fanout_total: usize,
    total_value_eth: f64,
    total_value_usd: f64,
}

struct LayeredTransferPattern {
    row: PaperLayeredTransferRowPayload,
    addresses: BTreeSet<String>,
    token_ids: BTreeSet<String>,
}

struct InventoryConcentrationPattern {
    row: PaperInventoryConcentrationRowPayload,
    addresses: BTreeSet<String>,
    token_ids: BTreeSet<String>,
}

struct CyclePattern {
    row: PaperWashTradingRowPayload,
    participants: BTreeSet<String>,
    token_ids: BTreeSet<String>,
    max_block_time: i64,
    avg_price_eth: Option<f64>,
    avg_price_usd: Option<f64>,
}

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
    let attacker_cost = build_attacker_cost(input.config, input.value_flow_edges);
    let honest_loss = build_honest_loss_rows(
        input.config,
        &address_sets,
        input.victim_acquisition_addresses,
        input.nft_propagation_paths,
    );

    PaperStatsPayload {
        duplicate_scale: duplicate_scale.rows,
        address_classification: build_address_classification(&address_sets),
        contract_behavior_stats,
        malicious_behavior_summary,
        attacker_cost: attacker_cost.payload,
        honest_loss: honest_loss.rows,
        data_quality: build_data_quality(&input),
        malicious_addresses: address_sets.malicious.into_iter().collect(),
        honest_addresses: address_sets.honest.into_iter().collect(),
        repeat_infringing_malicious_addresses: address_sets
            .repeat_infringing_malicious
            .into_iter()
            .collect(),
        attacker_cost_by_contract_usd: attacker_cost.by_contract_usd,
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

pub fn merge_paper_stats<'a>(
    seed_stats: impl IntoIterator<Item = &'a PaperStatsPayload>,
    config: PaperStatsConfig,
) -> PaperStatsPayload {
    let mut duplicate_rows = BTreeMap::<String, DuplicateScaleAccumulator>::new();
    let mut honest_loss_rows = BTreeMap::<String, HonestLossAccumulator>::new();
    let mut duplicate_nft_keys = BTreeMap::<String, BTreeSet<String>>::new();
    let mut duplicate_contract_keys = BTreeMap::<String, BTreeSet<String>>::new();
    let mut duplicate_contract_denominator_keys = BTreeSet::<String>::new();
    let mut behavior_contracts = BTreeMap::<String, BTreeSet<String>>::new();
    let mut behavior_addresses = BTreeMap::<String, BTreeSet<String>>::new();
    let mut behavior_nfts = BTreeMap::<String, BTreeSet<String>>::new();
    let mut behavior_buyers = BTreeMap::<String, BTreeSet<String>>::new();
    let mut behavior_contract_denominator_keys = BTreeSet::<String>::new();
    let mut merged = PaperStatsPayload::default();

    for stats in seed_stats {
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

        merge_f64_map(
            &mut merged.attacker_cost_by_contract_usd,
            &stats.attacker_cost_by_contract_usd,
        );
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

        for row in &stats.honest_loss {
            let entry = honest_loss_rows.entry(row.category.clone()).or_default();
            entry.stuck_nft_count += row.stuck_nft_count;
            entry.stuck_nft_denominator += row.stuck_nft_ratio_denominator;
            entry.stuck_time_numerator += row.stuck_time_ratio_numerator;
            entry.stuck_time_denominator += row.stuck_time_ratio_denominator;
            entry.secondary_sale_loss_eth += row.secondary_sale_loss_eth;
            entry.secondary_sale_loss_usd += row.secondary_sale_loss_usd;
            entry.paid_mint_loss_eth += row.paid_mint_loss_eth;
            entry.paid_mint_loss_usd += row.paid_mint_loss_usd;
        }

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

    merged.honest_loss = honest_loss_rows
        .into_iter()
        .map(|(category, row)| {
            let total_loss_usd = row.secondary_sale_loss_usd + row.paid_mint_loss_usd;
            let top_loss_numerator = if category == "total" {
                top_contribution_numerator(&merged.honest_loss_by_contract_usd, config)
            } else {
                total_loss_usd
            };
            PaperHonestLossPayload {
                category,
                stuck_nft_count: row.stuck_nft_count,
                stuck_nft_ratio: ratio_i64(row.stuck_nft_count, row.stuck_nft_denominator),
                stuck_nft_ratio_numerator: row.stuck_nft_count,
                stuck_nft_ratio_denominator: row.stuck_nft_denominator,
                stuck_time_ratio: ratio_f64(row.stuck_time_numerator, row.stuck_time_denominator),
                stuck_time_ratio_numerator: row.stuck_time_numerator,
                stuck_time_ratio_denominator: row.stuck_time_denominator,
                secondary_sale_loss_eth: row.secondary_sale_loss_eth,
                secondary_sale_loss_usd: row.secondary_sale_loss_usd,
                paid_mint_loss_eth: row.paid_mint_loss_eth,
                paid_mint_loss_usd: row.paid_mint_loss_usd,
                total_loss_eth: row.secondary_sale_loss_eth + row.paid_mint_loss_eth,
                total_loss_usd,
                top_contract_loss_contribution_ratio: ratio_f64(top_loss_numerator, total_loss_usd),
                top_contract_loss_contribution_numerator: top_loss_numerator,
                top_contract_loss_contribution_denominator: total_loss_usd,
            }
        })
        .collect();

    merged.attacker_cost.top_contract_contribution_numerator =
        top_contribution_numerator(&merged.attacker_cost_by_contract_usd, config);
    merged.attacker_cost.top_contract_contribution_denominator = merged.attacker_cost.total_gas_usd;
    merged.attacker_cost.top_contract_contribution_ratio = ratio_f64(
        merged.attacker_cost.top_contract_contribution_numerator,
        merged.attacker_cost.top_contract_contribution_denominator,
    );
    merged.data_quality.sale_price_parseable_ratio = ratio_i64(
        merged.data_quality.sale_price_parseable_count,
        merged.data_quality.sale_price_total_count,
    );
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

fn merge_f64_map(target: &mut BTreeMap<String, f64>, source: &BTreeMap<String, f64>) {
    for (key, value) in source {
        *target.entry(key.clone()).or_default() += value;
    }
}

fn merge_set_maps(
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

fn sets_to_vecs(source: BTreeMap<String, BTreeSet<String>>) -> BTreeMap<String, Vec<String>> {
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

fn ratio_i64(numerator: i64, denominator: i64) -> Option<f64> {
    (denominator > 0).then_some(numerator as f64 / denominator as f64)
}

fn ratio_f64(numerator: f64, denominator: f64) -> Option<f64> {
    (denominator > 0.0).then_some(numerator / denominator)
}

fn normalized_address(address: &str) -> String {
    address.trim().to_lowercase()
}

fn normalized_contract(contract: &str) -> String {
    let contract = normalized_address(contract);
    if contract.is_empty() {
        "unknown".into()
    } else {
        contract
    }
}

fn is_participant_address(address: &str) -> bool {
    !address.is_empty() && address != ZERO_ADDRESS
}

fn category_reason(category: &str) -> Option<&'static str> {
    match category {
        "token_uri" => Some("token_uri_match"),
        "image_uri" => Some("image_uri_match"),
        "metadata" => Some("metadata_match"),
        "name" => Some("name_match"),
        "total" => None,
        _ => None,
    }
}

fn match_reasons_match_category(match_reasons: &[String], category: &str) -> bool {
    category_reason(category)
        .map(|reason| match_reasons.iter().any(|item| item == reason))
        .unwrap_or(true)
}

#[derive(Clone)]
struct DuplicateEvidenceItem {
    contract_address: String,
    token_id: String,
    match_reasons: Vec<String>,
}

fn duplicate_evidence_item(
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

fn duplicate_evidence_items(input: &PaperStatsInput<'_>) -> Vec<DuplicateEvidenceItem> {
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

fn duplicate_contract_key_set(
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

fn build_duplicate_scale(input: &PaperStatsInput<'_>) -> DuplicateScaleBuild {
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

fn behavior_contract_denominator_keys(input: &PaperStatsInput<'_>) -> BTreeSet<String> {
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

fn build_address_sets(input: &PaperStatsInput<'_>) -> AddressSets {
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

fn build_address_classification(address_sets: &AddressSets) -> PaperAddressClassificationPayload {
    PaperAddressClassificationPayload {
        malicious_address_count: address_sets.malicious.len() as i64,
        repeat_infringing_malicious_address_count: address_sets.repeat_infringing_malicious.len()
            as i64,
        honest_address_count: address_sets.honest.len() as i64,
        total_address_count: address_sets.malicious.union(&address_sets.honest).count() as i64,
    }
}

fn build_attacker_cost(
    config: PaperStatsConfig,
    value_flow_edges: &[ValueFlowEdgePayload],
) -> AttackerCostBuild {
    let mut build = AttackerCostBuild::default();
    for edge in value_flow_edges {
        let gas_eth =
            edge.value_with_gas_eth.unwrap_or_default() - edge.value_eth.unwrap_or_default();
        let gas_usd =
            edge.value_with_gas_usd.unwrap_or_default() - edge.value_usd.unwrap_or_default();
        let gas_eth = gas_eth.max(0.0);
        let gas_usd = gas_usd.max(0.0);
        match edge.channel.as_str() {
            "mint_payment" | "funding" | "deployment" | "contract_deploy" => {
                build.payload.setup_gas_eth += gas_eth;
                build.payload.setup_gas_usd += gas_usd;
            }
            "sale_payment" | "lure_payment" => {
                build.payload.lure_gas_eth += gas_eth;
                build.payload.lure_gas_usd += gas_usd;
            }
            "withdrawal" | "cashout_hop" | "exit_payment" => {
                build.payload.exit_gas_eth += gas_eth;
                build.payload.exit_gas_usd += gas_usd;
            }
            _ => {}
        }
        if gas_usd > 0.0 {
            *build
                .by_contract_usd
                .entry(normalized_contract(&edge.contract_address))
                .or_default() += gas_usd;
        }
    }
    build.payload.total_gas_eth =
        build.payload.setup_gas_eth + build.payload.lure_gas_eth + build.payload.exit_gas_eth;
    build.payload.total_gas_usd =
        build.payload.setup_gas_usd + build.payload.lure_gas_usd + build.payload.exit_gas_usd;
    build.payload.top_contract_contribution_denominator = build.payload.total_gas_usd;
    build.payload.top_contract_contribution_numerator =
        top_contribution_numerator(&build.by_contract_usd, config);
    build.payload.top_contract_contribution_ratio = ratio_f64(
        build.payload.top_contract_contribution_numerator,
        build.payload.top_contract_contribution_denominator,
    );
    build
}

fn top_contribution_numerator(
    values_by_contract: &BTreeMap<String, f64>,
    config: PaperStatsConfig,
) -> f64 {
    let mut values = values_by_contract
        .values()
        .copied()
        .filter(|value| *value > 0.0)
        .collect::<Vec<_>>();
    values.sort_by(|left, right| right.partial_cmp(left).unwrap_or(Ordering::Equal));
    values
        .into_iter()
        .take(config.top_contract_count(values_by_contract.len()))
        .sum()
}

fn loss_row(input: LossRowInput<'_>) -> PaperHonestLossPayload {
    let total_loss_eth = input.secondary_sale_loss_eth + input.paid_mint_loss_eth;
    let total_loss_usd = input.secondary_sale_loss_usd + input.paid_mint_loss_usd;
    PaperHonestLossPayload {
        category: input.category.to_string(),
        stuck_nft_count: input.stuck_nft_count,
        stuck_nft_ratio: ratio_i64(input.stuck_nft_count, input.total_nft_count),
        stuck_nft_ratio_numerator: input.stuck_nft_count,
        stuck_nft_ratio_denominator: input.total_nft_count,
        stuck_time_ratio: ratio_f64(input.stuck_time_numerator, input.stuck_time_denominator),
        stuck_time_ratio_numerator: input.stuck_time_numerator,
        stuck_time_ratio_denominator: input.stuck_time_denominator,
        secondary_sale_loss_eth: input.secondary_sale_loss_eth,
        secondary_sale_loss_usd: input.secondary_sale_loss_usd,
        paid_mint_loss_eth: input.paid_mint_loss_eth,
        paid_mint_loss_usd: input.paid_mint_loss_usd,
        total_loss_eth,
        total_loss_usd,
        top_contract_loss_contribution_numerator: input.top_loss_numerator,
        top_contract_loss_contribution_denominator: input.top_loss_denominator,
        top_contract_loss_contribution_ratio: ratio_f64(
            input.top_loss_numerator,
            input.top_loss_denominator,
        ),
    }
}

fn build_honest_loss_rows(
    config: PaperStatsConfig,
    address_sets: &AddressSets,
    victim_acquisition_addresses: &[VictimAcquisitionAddressPayload],
    nft_propagation_paths: &BTreeMap<String, NftPropagationPathPayload>,
) -> HonestLossBuild {
    let secondary_total_nft_count: i64 = victim_acquisition_addresses
        .iter()
        .map(|item| item.secondary_sale_count)
        .sum();
    let secondary_stuck_nft_count: i64 = victim_acquisition_addresses
        .iter()
        .filter(|item| item.is_stuck)
        .map(|item| item.secondary_sale_count)
        .sum();
    let paid_mint_stuck_nft_count: i64 = victim_acquisition_addresses
        .iter()
        .map(|item| item.paid_mint_stuck_token_count)
        .sum();
    let paid_mint_total_nft_count: i64 = victim_acquisition_addresses
        .iter()
        .map(|item| {
            item.paid_mint_stuck_token_count
                .max(item.paid_mint_edge_count)
        })
        .sum();

    let secondary_sale_loss_eth: f64 = victim_acquisition_addresses
        .iter()
        .map(|item| item.secondary_sale_stuck_cost_eth)
        .sum();
    let secondary_sale_loss_usd: f64 = victim_acquisition_addresses
        .iter()
        .map(|item| item.secondary_sale_stuck_cost_usd)
        .sum();
    let paid_mint_loss_eth: f64 = victim_acquisition_addresses
        .iter()
        .map(|item| item.paid_mint_stuck_cost_eth)
        .sum();
    let paid_mint_loss_usd: f64 = victim_acquisition_addresses
        .iter()
        .map(|item| item.paid_mint_stuck_cost_usd)
        .sum();
    let fallback_total_stuck_eth: f64 = victim_acquisition_addresses
        .iter()
        .map(|item| item.total_stuck_cost_eth)
        .sum();
    let fallback_total_stuck_usd: f64 = victim_acquisition_addresses
        .iter()
        .map(|item| item.total_stuck_cost_usd)
        .sum();

    let total_secondary_eth = secondary_sale_loss_eth;
    let total_secondary_usd = secondary_sale_loss_usd;
    let total_paid_mint_eth = if paid_mint_loss_eth > 0.0 {
        paid_mint_loss_eth
    } else {
        (fallback_total_stuck_eth - total_secondary_eth).max(0.0)
    };
    let total_paid_mint_usd = if paid_mint_loss_usd > 0.0 {
        paid_mint_loss_usd
    } else {
        (fallback_total_stuck_usd - total_secondary_usd).max(0.0)
    };
    let total_stuck_nft_count = secondary_stuck_nft_count + paid_mint_stuck_nft_count;
    let total_nft_count = secondary_total_nft_count + paid_mint_total_nft_count;

    let secondary_loss_by_contract_usd = loss_by_contract(victim_acquisition_addresses, |item| {
        item.secondary_sale_stuck_cost_usd
    });
    let paid_mint_loss_by_contract_usd = loss_by_contract(victim_acquisition_addresses, |item| {
        item.paid_mint_stuck_cost_usd
    });
    let total_loss_by_contract_usd = loss_by_contract(victim_acquisition_addresses, |item| {
        item.total_stuck_cost_usd
    });

    let (stuck_time_numerator_by_contract, stuck_time_denominator_by_contract) =
        stuck_time_by_contract(config, address_sets, nft_propagation_paths);
    let stuck_time_numerator: f64 = stuck_time_numerator_by_contract.values().sum();
    let stuck_time_denominator: f64 = stuck_time_denominator_by_contract.values().sum();

    HonestLossBuild {
        rows: vec![
            loss_row(LossRowInput {
                category: "secondary_sale",
                stuck_nft_count: secondary_stuck_nft_count,
                total_nft_count: secondary_total_nft_count,
                stuck_time_numerator,
                stuck_time_denominator,
                secondary_sale_loss_eth,
                secondary_sale_loss_usd,
                paid_mint_loss_eth: 0.0,
                paid_mint_loss_usd: 0.0,
                top_loss_numerator: top_contribution_numerator(
                    &secondary_loss_by_contract_usd,
                    config,
                ),
                top_loss_denominator: secondary_sale_loss_usd,
            }),
            loss_row(LossRowInput {
                category: "paid_mint",
                stuck_nft_count: paid_mint_stuck_nft_count,
                total_nft_count: paid_mint_total_nft_count,
                stuck_time_numerator: 0.0,
                stuck_time_denominator: 0.0,
                secondary_sale_loss_eth: 0.0,
                secondary_sale_loss_usd: 0.0,
                paid_mint_loss_eth: total_paid_mint_eth,
                paid_mint_loss_usd: total_paid_mint_usd,
                top_loss_numerator: top_contribution_numerator(
                    &paid_mint_loss_by_contract_usd,
                    config,
                ),
                top_loss_denominator: total_paid_mint_usd,
            }),
            loss_row(LossRowInput {
                category: "total",
                stuck_nft_count: total_stuck_nft_count,
                total_nft_count,
                stuck_time_numerator,
                stuck_time_denominator,
                secondary_sale_loss_eth: total_secondary_eth,
                secondary_sale_loss_usd: total_secondary_usd,
                paid_mint_loss_eth: total_paid_mint_eth,
                paid_mint_loss_usd: total_paid_mint_usd,
                top_loss_numerator: top_contribution_numerator(&total_loss_by_contract_usd, config),
                top_loss_denominator: total_secondary_usd + total_paid_mint_usd,
            }),
        ],
        total_loss_by_contract_usd,
        stuck_time_numerator_by_contract,
        stuck_time_denominator_by_contract,
    }
}

fn loss_by_contract(
    victim_acquisition_addresses: &[VictimAcquisitionAddressPayload],
    loss: impl Fn(&VictimAcquisitionAddressPayload) -> f64,
) -> BTreeMap<String, f64> {
    let mut by_contract = BTreeMap::<String, f64>::new();
    for item in victim_acquisition_addresses {
        let amount = loss(item);
        if amount <= 0.0 {
            continue;
        }
        let contracts = item
            .contract_addresses
            .iter()
            .map(|contract| normalized_contract(contract))
            .collect::<BTreeSet<_>>();
        let contracts = if contracts.is_empty() {
            BTreeSet::from(["unknown".to_string()])
        } else {
            contracts
        };
        let share = amount / contracts.len() as f64;
        for contract in contracts {
            *by_contract.entry(contract).or_default() += share;
        }
    }
    by_contract
}

fn stuck_time_by_contract(
    config: PaperStatsConfig,
    address_sets: &AddressSets,
    nft_propagation_paths: &BTreeMap<String, NftPropagationPathPayload>,
) -> (BTreeMap<String, f64>, BTreeMap<String, f64>) {
    if config.analysis_timestamp <= 0 {
        return (BTreeMap::new(), BTreeMap::new());
    }
    let mut numerators = BTreeMap::<String, f64>::new();
    let mut denominators = BTreeMap::<String, f64>::new();
    for (contract_key, path) in nft_propagation_paths {
        let contract = if path.contract_address.trim().is_empty() {
            normalized_contract(contract_key)
        } else {
            normalized_contract(&path.contract_address)
        };
        let first_time = if path.summary.first_block_time > 0 {
            path.summary.first_block_time
        } else {
            path.edges
                .iter()
                .filter_map(|edge| (edge.block_time > 0).then_some(edge.block_time))
                .min()
                .unwrap_or_default()
        };
        if first_time <= 0 {
            continue;
        }
        for edge in &path.edges {
            if edge.channel != "sale" {
                continue;
            }
            let buyer = normalized_address(&edge.to_address);
            if !address_sets.honest.contains(&buyer) {
                continue;
            }
            if edge.block_time <= first_time || config.analysis_timestamp <= edge.block_time {
                continue;
            }
            *numerators.entry(contract.clone()).or_default() +=
                (config.analysis_timestamp - edge.block_time) as f64;
            *denominators.entry(contract.clone()).or_default() +=
                (edge.block_time - first_time) as f64;
        }
    }
    (numerators, denominators)
}

fn build_contract_behavior_stats(
    input: &PaperStatsInput<'_>,
    address_sets: &AddressSets,
) -> Vec<ContractBehaviorBuild> {
    input
        .nft_propagation_paths
        .par_iter()
        .filter_map(|(contract_key, path)| {
            let contract_address = if path.contract_address.trim().is_empty() {
                normalized_contract(contract_key)
            } else {
                normalized_contract(&path.contract_address)
            };
            let cycles = detect_wash_trading(&contract_address, path, address_sets, input.config);
            let mut behavior_contracts = BTreeMap::<String, BTreeSet<String>>::new();
            let mut behavior_addresses = BTreeMap::<String, BTreeSet<String>>::new();
            let mut behavior_nfts = BTreeMap::<String, BTreeSet<String>>::new();
            let mut behavior_buyers = BTreeMap::<String, BTreeSet<String>>::new();
            for cycle in &cycles {
                insert_behavior_keys(
                    &contract_address,
                    &mut behavior_contracts,
                    &mut behavior_addresses,
                    &mut behavior_nfts,
                    "Wash Trading",
                    cycle.participants.iter().cloned(),
                    cycle.token_ids.iter().cloned(),
                );
            }
            let wash_trading = cycles.iter().map(|cycle| cycle.row.clone()).collect();
            let pump_and_exit_patterns = detect_pump_and_exit(path, address_sets, &cycles);
            let mut source_patterns_by_buyer = BTreeMap::<String, BTreeSet<String>>::new();
            for pattern in &pump_and_exit_patterns {
                insert_behavior_keys(
                    &contract_address,
                    &mut behavior_contracts,
                    &mut behavior_addresses,
                    &mut behavior_nfts,
                    "Pump-and-Exit",
                    pattern.addresses.iter().cloned(),
                    pattern.token_ids.iter().cloned(),
                );
                insert_behavior_buyers(
                    &mut behavior_buyers,
                    "Pump-and-Exit",
                    pattern.buyers.iter().cloned(),
                );
                for buyer in &pattern.buyers {
                    source_patterns_by_buyer
                        .entry(buyer.clone())
                        .or_default()
                        .insert("Pump-and-Exit".into());
                }
            }
            let pump_and_exit = pump_and_exit_patterns
                .iter()
                .map(|pattern| pattern.row.clone())
                .collect();
            let star_behavior_patterns = detect_star_behaviors(path, address_sets, input.config);
            for pattern in &star_behavior_patterns {
                insert_behavior_keys(
                    &contract_address,
                    &mut behavior_contracts,
                    &mut behavior_addresses,
                    &mut behavior_nfts,
                    &pattern.row.behavior,
                    pattern.addresses.iter().cloned(),
                    pattern.token_ids.iter().cloned(),
                );
            }
            let star_behaviors = star_behavior_patterns
                .iter()
                .map(|pattern| pattern.row.clone())
                .collect();
            let layered_transfer_patterns =
                detect_layered_transfers(&contract_address, path, input.config);
            for pattern in &layered_transfer_patterns {
                insert_behavior_keys(
                    &contract_address,
                    &mut behavior_contracts,
                    &mut behavior_addresses,
                    &mut behavior_nfts,
                    "Layered Transfer",
                    pattern.addresses.iter().cloned(),
                    pattern.token_ids.iter().cloned(),
                );
            }
            let layered_transfers = layered_transfer_patterns
                .iter()
                .map(|pattern| pattern.row.clone())
                .collect();
            let inventory_concentration =
                detect_inventory_concentration(path, address_sets, input.config);
            for pattern in &inventory_concentration {
                insert_behavior_keys(
                    &contract_address,
                    &mut behavior_contracts,
                    &mut behavior_addresses,
                    &mut behavior_nfts,
                    "Inventory Concentration",
                    pattern.addresses.iter().cloned(),
                    pattern.token_ids.iter().cloned(),
                );
            }
            let inventory_concentration = inventory_concentration
                .iter()
                .map(|pattern| pattern.row.clone())
                .collect();
            let honest_buyers_top =
                honest_buyers_top(input, &contract_address, path, &source_patterns_by_buyer);

            let stats = PaperContractBehaviorStatsPayload {
                contract_address,
                wash_trading,
                pump_and_exit,
                star_behaviors,
                layered_transfers,
                inventory_concentration,
                honest_buyers_top,
            };
            (!stats.wash_trading.is_empty()
                || !stats.pump_and_exit.is_empty()
                || !stats.star_behaviors.is_empty()
                || !stats.layered_transfers.is_empty()
                || !stats.inventory_concentration.is_empty()
                || !stats.honest_buyers_top.is_empty())
            .then_some(ContractBehaviorBuild {
                stats,
                behavior_contracts,
                behavior_addresses,
                behavior_nfts,
                behavior_buyers,
            })
        })
        .collect()
}

fn detect_wash_trading(
    contract_address: &str,
    path: &NftPropagationPathPayload,
    address_sets: &AddressSets,
    config: PaperStatsConfig,
) -> Vec<CyclePattern> {
    let mut adjacency = BTreeMap::<String, BTreeSet<String>>::new();
    let mut sale_edges = Vec::<&NftPropagationEdgePayload>::new();
    for edge in path.edges.iter().filter(|edge| edge.channel == "sale") {
        let from = normalized_address(&edge.from_address);
        let to = normalized_address(&edge.to_address);
        if !address_sets.malicious.contains(&from) || !address_sets.malicious.contains(&to) {
            continue;
        }
        if !is_participant_address(&from) || !is_participant_address(&to) {
            continue;
        }
        adjacency
            .entry(from.clone())
            .or_default()
            .insert(to.clone());
        adjacency.entry(to).or_default();
        sale_edges.push(edge);
    }

    let components = strongly_connected_components(&adjacency);
    let mut component_by_address = BTreeMap::<String, usize>::new();
    for (index, component) in components.iter().enumerate() {
        for address in component {
            component_by_address.insert(address.clone(), index);
        }
    }

    let mut edges_by_component = BTreeMap::<usize, Vec<&NftPropagationEdgePayload>>::new();
    for edge in sale_edges {
        let from = normalized_address(&edge.from_address);
        let to = normalized_address(&edge.to_address);
        let (Some(from_component), Some(to_component)) = (
            component_by_address.get(&from).copied(),
            component_by_address.get(&to).copied(),
        ) else {
            continue;
        };
        if from_component == to_component {
            edges_by_component
                .entry(from_component)
                .or_default()
                .push(edge);
        }
    }

    let mut cycles = Vec::new();
    for (component_index, all_edges) in edges_by_component {
        let component = &components[component_index];
        if component.len() < config.min_cycle_size() {
            continue;
        }
        if all_edges.is_empty() {
            continue;
        }
        let token_ids = all_edges
            .iter()
            .flat_map(|edge| edge_token_ids(edge))
            .collect::<BTreeSet<_>>();
        let block_times = all_edges
            .iter()
            .map(|edge| edge.block_time)
            .filter(|block_time| *block_time > 0)
            .collect::<Vec<_>>();
        let fake_volume_eth: f64 = all_edges
            .iter()
            .map(|edge| edge.price_eth.unwrap_or_default())
            .sum();
        let fake_volume_usd: f64 = all_edges
            .iter()
            .map(|edge| edge.price_usd.unwrap_or_default())
            .sum();
        let avg_price_eth = average_optional(all_edges.iter().filter_map(|edge| edge.price_eth));
        let avg_price_usd = average_optional(all_edges.iter().filter_map(|edge| edge.price_usd));
        let token_counts = all_edges.iter().flat_map(|edge| edge_token_ids(edge)).fold(
            BTreeMap::<String, i64>::new(),
            |mut counts, token| {
                *counts.entry(token).or_default() += 1;
                counts
            },
        );
        let max_block_time = block_times.iter().max().copied().unwrap_or_default();
        let avg_cycle_blocks = match (block_times.iter().min(), block_times.iter().max()) {
            (Some(first), Some(last)) => Some((*last - *first).max(0) as f64),
            _ => None,
        };
        cycles.push(CyclePattern {
            row: PaperWashTradingRowPayload {
                cycle_id: format!("{contract_address}:wash:{}", cycles.len() + 1),
                participant_node_count: component.len() as i64,
                token_gini: gini(token_counts.values().map(|value| *value as f64).collect()),
                avg_cycle_blocks,
                fake_volume_eth,
                fake_volume_usd,
            },
            participants: component.clone(),
            token_ids,
            max_block_time,
            avg_price_eth,
            avg_price_usd,
        });
    }
    cycles
}

fn insert_behavior_keys(
    contract_address: &str,
    behavior_contracts: &mut BTreeMap<String, BTreeSet<String>>,
    behavior_addresses: &mut BTreeMap<String, BTreeSet<String>>,
    behavior_nfts: &mut BTreeMap<String, BTreeSet<String>>,
    behavior: &str,
    addresses: impl IntoIterator<Item = String>,
    token_ids: impl IntoIterator<Item = String>,
) {
    behavior_contracts
        .entry(behavior.to_string())
        .or_default()
        .insert(normalized_contract(contract_address));
    behavior_addresses
        .entry(behavior.to_string())
        .or_default()
        .extend(
            addresses
                .into_iter()
                .map(|address| normalized_address(&address))
                .filter(|address| is_participant_address(address)),
        );
    behavior_nfts
        .entry(behavior.to_string())
        .or_default()
        .extend(
            token_ids
                .into_iter()
                .filter_map(|token_id| behavior_nft_key(contract_address, &token_id)),
        );
}

fn insert_behavior_buyers(
    behavior_buyers: &mut BTreeMap<String, BTreeSet<String>>,
    behavior: &str,
    buyers: impl IntoIterator<Item = String>,
) {
    behavior_buyers
        .entry(behavior.to_string())
        .or_default()
        .extend(
            buyers
                .into_iter()
                .map(|address| normalized_address(&address))
                .filter(|address| is_participant_address(address)),
        );
}

fn behavior_nft_key(contract_address: &str, token_id: &str) -> Option<String> {
    let token_id = token_id.trim();
    (!token_id.is_empty()).then(|| format!("{}:{token_id}", normalized_contract(contract_address)))
}

fn strongly_connected_components(
    adjacency: &BTreeMap<String, BTreeSet<String>>,
) -> Vec<BTreeSet<String>> {
    let nodes = adjacency
        .iter()
        .flat_map(|(node, neighbors)| {
            std::iter::once(node.clone()).chain(neighbors.iter().cloned())
        })
        .collect::<BTreeSet<_>>();
    let mut visited = BTreeSet::<String>::new();
    let mut order = Vec::<String>::new();

    for node in &nodes {
        if visited.contains(node) {
            continue;
        }
        let mut stack = vec![(node.clone(), false)];
        while let Some((current, expanded)) = stack.pop() {
            if expanded {
                order.push(current);
                continue;
            }
            if !visited.insert(current.clone()) {
                continue;
            }
            stack.push((current.clone(), true));
            if let Some(neighbors) = adjacency.get(&current) {
                for neighbor in neighbors.iter().rev() {
                    if !visited.contains(neighbor) {
                        stack.push((neighbor.clone(), false));
                    }
                }
            }
        }
    }

    let mut reverse = BTreeMap::<String, BTreeSet<String>>::new();
    for node in &nodes {
        reverse.entry(node.clone()).or_default();
    }
    for (from, neighbors) in adjacency {
        for to in neighbors {
            reverse.entry(to.clone()).or_default().insert(from.clone());
        }
    }

    let mut assigned = BTreeSet::<String>::new();
    let mut components = Vec::new();
    while let Some(node) = order.pop() {
        if assigned.contains(&node) {
            continue;
        }
        let mut component = BTreeSet::<String>::new();
        let mut stack = vec![node];
        while let Some(current) = stack.pop() {
            if !assigned.insert(current.clone()) {
                continue;
            }
            component.insert(current.clone());
            if let Some(neighbors) = reverse.get(&current) {
                for neighbor in neighbors.iter().rev() {
                    if !assigned.contains(neighbor) {
                        stack.push(neighbor.clone());
                    }
                }
            }
        }
        components.push(component);
    }

    components
}

fn detect_pump_and_exit(
    path: &NftPropagationPathPayload,
    address_sets: &AddressSets,
    cycles: &[CyclePattern],
) -> Vec<PumpExitPattern> {
    let mut rows = Vec::new();
    for cycle in cycles {
        let exit_edges = path
            .edges
            .iter()
            .filter(|edge| {
                edge.channel == "sale"
                    && edge.block_time >= cycle.max_block_time
                    && cycle
                        .participants
                        .contains(&normalized_address(&edge.from_address))
                    && address_sets
                        .honest
                        .contains(&normalized_address(&edge.to_address))
            })
            .collect::<Vec<_>>();
        if exit_edges.is_empty() {
            continue;
        }
        let linked_buyers = exit_edges
            .iter()
            .map(|edge| normalized_address(&edge.to_address))
            .collect::<BTreeSet<_>>();
        let exit_tokens = exit_edges
            .iter()
            .flat_map(|edge| edge_token_ids(edge))
            .collect::<BTreeSet<_>>();
        let first_exit_time = exit_edges
            .iter()
            .map(|edge| edge.block_time)
            .filter(|block_time| *block_time > 0)
            .min();
        let exit_price_premium =
            average_optional(exit_edges.iter().filter_map(|edge| edge.price_usd))
                .and_then(|avg_exit| {
                    cycle
                        .avg_price_usd
                        .filter(|value| *value > 0.0)
                        .map(|avg| avg_exit / avg)
                })
                .or_else(|| {
                    average_optional(exit_edges.iter().filter_map(|edge| edge.price_eth)).and_then(
                        |avg_exit| {
                            cycle
                                .avg_price_eth
                                .filter(|value| *value > 0.0)
                                .map(|avg| avg_exit / avg)
                        },
                    )
                });
        if exit_price_premium
            .filter(|premium| *premium > 1.0)
            .is_none()
        {
            continue;
        }
        let mut addresses = cycle.participants.clone();
        addresses.extend(linked_buyers.iter().cloned());
        let mut token_ids = cycle.token_ids.clone();
        token_ids.extend(exit_tokens.iter().cloned());
        rows.push(PumpExitPattern {
            row: PaperPumpExitRowPayload {
                cycle_id: cycle.row.cycle_id.clone(),
                exit_delay_seconds: first_exit_time
                    .map(|time| (time - cycle.max_block_time).max(0)),
                exit_price_premium,
                exit_ratio: ratio_i64(exit_tokens.len() as i64, cycle.token_ids.len() as i64),
                exit_ratio_numerator: exit_tokens.len() as i64,
                exit_ratio_denominator: cycle.token_ids.len() as i64,
                linked_honest_buyer_count: linked_buyers.len() as i64,
                linked_loss_eth: exit_edges
                    .iter()
                    .map(|edge| edge.price_eth.unwrap_or_default())
                    .sum(),
                linked_loss_usd: exit_edges
                    .iter()
                    .map(|edge| edge.price_usd.unwrap_or_default())
                    .sum(),
            },
            addresses,
            token_ids,
            buyers: linked_buyers,
        });
    }
    rows
}

fn detect_star_behaviors(
    path: &NftPropagationPathPayload,
    address_sets: &AddressSets,
    config: PaperStatsConfig,
) -> Vec<StarBehaviorPattern> {
    let mut adjacency = BTreeMap::<String, BTreeSet<String>>::new();
    let mut graph_edges = Vec::<&NftPropagationEdgePayload>::new();
    for edge in &path.edges {
        let from = normalized_address(&edge.from_address);
        let to = normalized_address(&edge.to_address);
        if !is_participant_address(&from) || !is_participant_address(&to) {
            continue;
        }
        adjacency
            .entry(from.clone())
            .or_default()
            .insert(to.clone());
        adjacency.entry(to).or_default();
        graph_edges.push(edge);
    }

    let components = strongly_connected_components(&adjacency);
    let mut component_by_address = BTreeMap::<String, usize>::new();
    for (index, component) in components.iter().enumerate() {
        for address in component {
            component_by_address.insert(address.clone(), index);
        }
    }

    let mut dag_targets = BTreeMap::<usize, BTreeSet<usize>>::new();
    let mut center_edges = BTreeMap::<usize, Vec<&NftPropagationEdgePayload>>::new();
    for edge in graph_edges {
        let from = normalized_address(&edge.from_address);
        let to = normalized_address(&edge.to_address);
        let (Some(from_component), Some(to_component)) = (
            component_by_address.get(&from).copied(),
            component_by_address.get(&to).copied(),
        ) else {
            continue;
        };
        if from_component == to_component {
            continue;
        }
        dag_targets
            .entry(from_component)
            .or_default()
            .insert(to_component);
        if components[from_component]
            .iter()
            .any(|address| address_sets.malicious.contains(address))
        {
            center_edges.entry(from_component).or_default().push(edge);
        }
    }

    let mut by_behavior = BTreeMap::<String, StarBehaviorBuild>::new();
    for (center_component, edges) in center_edges {
        let targets = dag_targets
            .get(&center_component)
            .cloned()
            .unwrap_or_default();
        if targets.len() < config.fanout_threshold() {
            continue;
        }
        let has_downstream = targets.iter().any(|target| {
            dag_targets
                .get(target)
                .map(|downstream| !downstream.is_empty())
                .unwrap_or(false)
        });
        let has_value = edges.iter().any(|edge| {
            edge.channel == "sale"
                || edge.price_eth.unwrap_or_default() > 0.0
                || edge.price_usd.unwrap_or_default() > 0.0
        });
        let behavior = if has_downstream {
            "Sybil Distribution"
        } else if has_value {
            "Fraud Revenue"
        } else {
            "Poisoning"
        };
        let entry = by_behavior.entry(behavior.to_string()).or_default();
        entry.centers.insert(center_component);
        entry.fanout_total += targets.len();
        entry.edge_count += edges.len() as i64;
        entry
            .wallets
            .extend(components[center_component].iter().cloned());
        for target in targets {
            entry.wallets.extend(components[target].iter().cloned());
        }
        for edge in edges {
            entry.token_ids.extend(edge_token_ids(edge));
            entry.total_value_eth += edge.price_eth.unwrap_or_default();
            entry.total_value_usd += edge.price_usd.unwrap_or_default();
        }
    }

    by_behavior
        .into_iter()
        .map(|(behavior, build)| StarBehaviorPattern {
            row: PaperStarBehaviorRowPayload {
                behavior,
                centers: build.centers.len() as i64,
                edges: build.edge_count,
                wallets: build.wallets.len() as i64,
                tokens: build.token_ids.len() as i64,
                avg_fan_out: ratio_i64(build.fanout_total as i64, build.centers.len() as i64),
                avg_fan_out_numerator: build.fanout_total as i64,
                avg_fan_out_denominator: build.centers.len() as i64,
                median_holding_seconds: None,
                total_value_eth: build.total_value_eth,
                total_value_usd: build.total_value_usd,
            },
            addresses: build.wallets,
            token_ids: build.token_ids,
        })
        .collect()
}

fn detect_layered_transfers(
    contract_address: &str,
    path: &NftPropagationPathPayload,
    config: PaperStatsConfig,
) -> Vec<LayeredTransferPattern> {
    let mut adjacency = BTreeMap::<String, Vec<&NftPropagationEdgePayload>>::new();
    for edge in &path.edges {
        let from = normalized_address(&edge.from_address);
        let to = normalized_address(&edge.to_address);
        if is_participant_address(&from) && is_participant_address(&to) {
            adjacency.entry(from).or_default().push(edge);
        }
    }

    let mut rows = Vec::new();
    for start in adjacency.keys() {
        let mut visited = BTreeSet::from([start.clone()]);
        let mut current_edges = Vec::new();
        if find_layered_path(
            &adjacency,
            start,
            &mut visited,
            &mut current_edges,
            config.min_path_length(),
        ) {
            let wallets = current_edges
                .iter()
                .flat_map(|edge: &&NftPropagationEdgePayload| {
                    [
                        normalized_address(&edge.from_address),
                        normalized_address(&edge.to_address),
                    ]
                })
                .collect::<BTreeSet<_>>();
            let tokens = current_edges
                .iter()
                .flat_map(|edge| edge_token_ids(edge))
                .collect::<BTreeSet<_>>();
            let block_times = current_edges
                .iter()
                .map(|edge| edge.block_time)
                .filter(|block_time| *block_time > 0)
                .collect::<Vec<_>>();
            rows.push(LayeredTransferPattern {
                row: PaperLayeredTransferRowPayload {
                    path_id: format!("{contract_address}:path:{}", rows.len() + 1),
                    tokens: tokens.len() as i64,
                    length: wallets.len() as i64,
                    wallets: wallets.len() as i64,
                    zero_or_low_value_hops: current_edges
                        .iter()
                        .filter(|edge| {
                            edge.price_usd.unwrap_or_default() <= 1.0
                                && edge.price_eth.unwrap_or_default() <= 0.001
                        })
                        .count() as i64,
                    total_path_duration_seconds: match (
                        block_times.iter().min(),
                        block_times.iter().max(),
                    ) {
                        (Some(first), Some(last)) => Some((*last - *first).max(0)),
                        _ => None,
                    },
                    total_value_eth: current_edges
                        .iter()
                        .map(|edge| edge.price_eth.unwrap_or_default())
                        .sum(),
                    total_value_usd: current_edges
                        .iter()
                        .map(|edge| edge.price_usd.unwrap_or_default())
                        .sum(),
                },
                addresses: wallets,
                token_ids: tokens,
            });
            break;
        }
    }
    rows
}

fn find_layered_path<'a>(
    adjacency: &BTreeMap<String, Vec<&'a NftPropagationEdgePayload>>,
    current: &str,
    visited: &mut BTreeSet<String>,
    current_edges: &mut Vec<&'a NftPropagationEdgePayload>,
    min_wallet_count: usize,
) -> bool {
    if visited.len() >= min_wallet_count {
        return true;
    }
    let Some(edges) = adjacency.get(current) else {
        return false;
    };
    for edge in edges {
        let next = normalized_address(&edge.to_address);
        if visited.contains(&next) {
            continue;
        }
        visited.insert(next.clone());
        current_edges.push(*edge);
        if find_layered_path(adjacency, &next, visited, current_edges, min_wallet_count) {
            return true;
        }
        current_edges.pop();
        visited.remove(&next);
    }
    false
}

fn detect_inventory_concentration(
    path: &NftPropagationPathPayload,
    address_sets: &AddressSets,
    config: PaperStatsConfig,
) -> Vec<InventoryConcentrationPattern> {
    let total_tokens = if path.summary.token_count > 0 {
        path.summary.token_count
    } else {
        path.edges
            .iter()
            .flat_map(edge_token_ids)
            .collect::<BTreeSet<_>>()
            .len() as i64
    };
    let total_value_eth: f64 = path
        .edges
        .iter()
        .map(|edge| edge.price_eth.unwrap_or_default())
        .sum();
    let total_value_usd: f64 = path
        .edges
        .iter()
        .map(|edge| edge.price_usd.unwrap_or_default())
        .sum();
    let mut inbound = BTreeMap::<String, Vec<&NftPropagationEdgePayload>>::new();
    let mut outgoing = BTreeMap::<String, BTreeSet<String>>::new();
    for edge in &path.edges {
        let from = normalized_address(&edge.from_address);
        let to = normalized_address(&edge.to_address);
        if !is_participant_address(&from) || !is_participant_address(&to) {
            continue;
        }
        inbound.entry(to.clone()).or_default().push(edge);
        outgoing.entry(from).or_default().insert(to);
    }

    let mut rows = inbound
        .into_iter()
        .filter_map(|(hub, edges)| {
            if !address_sets.malicious.contains(&hub) {
                return None;
            }
            let sources = edges
                .iter()
                .map(|edge| normalized_address(&edge.from_address))
                .collect::<BTreeSet<_>>();
            let outgoing_fanout = outgoing.get(&hub).map(BTreeSet::len).unwrap_or_default();
            let enough_sources = sources.len() >= config.fanout_threshold();
            let fanout_center_returned_inventory =
                outgoing_fanout >= config.fanout_threshold() && !edges.is_empty();
            if !enough_sources && !fanout_center_returned_inventory {
                return None;
            }
            let tokens = edges
                .iter()
                .flat_map(|edge| edge_token_ids(edge))
                .collect::<BTreeSet<_>>();
            let block_times = edges
                .iter()
                .map(|edge| edge.block_time)
                .filter(|block_time| *block_time > 0)
                .collect::<Vec<_>>();
            let value_collected_eth: f64 = edges
                .iter()
                .map(|edge| edge.price_eth.unwrap_or_default())
                .sum();
            let value_collected_usd: f64 = edges
                .iter()
                .map(|edge| edge.price_usd.unwrap_or_default())
                .sum();
            let (value_share, value_share_numerator, value_share_denominator) =
                if total_value_usd > 0.0 {
                    (
                        ratio_f64(value_collected_usd, total_value_usd),
                        value_collected_usd,
                        total_value_usd,
                    )
                } else {
                    (
                        ratio_f64(value_collected_eth, total_value_eth),
                        value_collected_eth,
                        total_value_eth,
                    )
                };
            let mut addresses = sources.clone();
            addresses.insert(hub.clone());
            Some(InventoryConcentrationPattern {
                row: PaperInventoryConcentrationRowPayload {
                    hub_address: hub,
                    source_wallets: sources.len() as i64,
                    inbound_txns: edges.len() as i64,
                    token_share: ratio_i64(tokens.len() as i64, total_tokens),
                    token_share_numerator: tokens.len() as i64,
                    token_share_denominator: total_tokens,
                    value_collected_eth,
                    value_collected_usd,
                    value_share,
                    value_share_numerator,
                    value_share_denominator,
                    collection_window_seconds: match (
                        block_times.iter().min(),
                        block_times.iter().max(),
                    ) {
                        (Some(first), Some(last)) => Some((*last - *first).max(0)),
                        _ => None,
                    },
                },
                addresses,
                token_ids: tokens,
            })
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|pattern| Reverse(pattern.row.inbound_txns));
    rows
}

fn honest_buyers_top(
    input: &PaperStatsInput<'_>,
    contract_address: &str,
    path: &NftPropagationPathPayload,
    source_patterns_by_buyer: &BTreeMap<String, BTreeSet<String>>,
) -> Vec<PaperHonestBuyerRowPayload> {
    let first_time = if path.summary.first_block_time > 0 {
        path.summary.first_block_time
    } else {
        path.edges
            .iter()
            .filter_map(|edge| (edge.block_time > 0).then_some(edge.block_time))
            .min()
            .unwrap_or_default()
    };
    let first_sale_by_buyer = path
        .edges
        .iter()
        .filter(|edge| edge.channel == "sale")
        .fold(BTreeMap::<String, i64>::new(), |mut map, edge| {
            let buyer = normalized_address(&edge.to_address);
            if is_participant_address(&buyer) && edge.block_time > 0 {
                map.entry(buyer)
                    .and_modify(|time| *time = (*time).min(edge.block_time))
                    .or_insert(edge.block_time);
            }
            map
        });

    let mut rows = input
        .victim_acquisition_addresses
        .iter()
        .filter(|item| {
            item.contract_addresses
                .iter()
                .map(|contract| normalized_contract(contract))
                .any(|contract| contract == contract_address)
        })
        .filter(|item| {
            item.is_stuck || item.total_stuck_cost_eth > 0.0 || item.total_stuck_cost_usd > 0.0
        })
        .map(|item| {
            let buyer = normalized_address(&item.address);
            let first_buy_time = first_sale_by_buyer.get(&buyer).copied();
            let source_pattern = source_patterns_by_buyer
                .get(&buyer)
                .map(|patterns| patterns.iter().cloned().collect::<Vec<_>>().join("+"))
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| "unattributed_sale".into());
            PaperHonestBuyerRowPayload {
                honest_buyer: buyer,
                fake_nft_bought: item.secondary_sale_count + item.paid_mint_stuck_token_count,
                total_paid_eth: item
                    .total_acquisition_cost_eth
                    .max(item.total_stuck_cost_eth),
                total_paid_usd: item
                    .total_acquisition_cost_usd
                    .max(item.total_stuck_cost_usd),
                source_pattern,
                time_to_purchase_seconds: first_buy_time
                    .and_then(|time| (first_time > 0).then_some((time - first_time).max(0))),
                still_holding: item.is_stuck,
                holding_seconds: first_buy_time.and_then(|time| {
                    (input.config.analysis_timestamp > time)
                        .then_some(input.config.analysis_timestamp - time)
                }),
            }
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        right
            .total_paid_usd
            .partial_cmp(&left.total_paid_usd)
            .unwrap_or(Ordering::Equal)
    });
    rows
}

fn build_behavior_summary(
    contract_stats: &[PaperContractBehaviorStatsPayload],
    contract_denominator: usize,
    behavior_contracts: &BTreeMap<String, BTreeSet<String>>,
    behavior_addresses: &BTreeMap<String, BTreeSet<String>>,
    behavior_nfts: &BTreeMap<String, BTreeSet<String>>,
    behavior_buyers: &BTreeMap<String, BTreeSet<String>>,
) -> Vec<PaperBehaviorSummaryRowPayload> {
    let mut rows = Vec::new();
    let mut total = PaperBehaviorSummaryRowPayload {
        behavior_type: "total".into(),
        contract_coverage_denominator: contract_denominator as i64,
        ..PaperBehaviorSummaryRowPayload::default()
    };

    if let Some(mut row) = build_behavior_row(
        "Wash Trading",
        contract_stats,
        contract_denominator,
        |stats| BehaviorMeasure {
            instance_count: stats.wash_trading.len() as i64,
            address_count: stats
                .wash_trading
                .iter()
                .map(|row| row.participant_node_count)
                .sum(),
            nft_count: stats.wash_trading.len() as i64,
            ..BehaviorMeasure::default()
        },
    ) {
        apply_behavior_dedup(
            &mut row,
            behavior_contracts,
            behavior_addresses,
            behavior_nfts,
            behavior_buyers,
        );
        rows.push(row);
    }
    if let Some(mut row) = build_behavior_row(
        "Pump-and-Exit",
        contract_stats,
        contract_denominator,
        |stats| BehaviorMeasure {
            instance_count: stats.pump_and_exit.len() as i64,
            address_count: stats
                .pump_and_exit
                .iter()
                .map(|row| row.linked_honest_buyer_count)
                .sum(),
            nft_count: stats.pump_and_exit.len() as i64,
            buyer_count: stats
                .pump_and_exit
                .iter()
                .map(|row| row.linked_honest_buyer_count)
                .sum(),
            linked_loss_eth: stats
                .pump_and_exit
                .iter()
                .map(|row| row.linked_loss_eth)
                .sum(),
            linked_loss_usd: stats
                .pump_and_exit
                .iter()
                .map(|row| row.linked_loss_usd)
                .sum(),
        },
    ) {
        apply_behavior_dedup(
            &mut row,
            behavior_contracts,
            behavior_addresses,
            behavior_nfts,
            behavior_buyers,
        );
        rows.push(row);
    }
    for behavior in ["Sybil Distribution", "Fraud Revenue", "Poisoning"] {
        if let Some(mut row) =
            build_behavior_row(behavior, contract_stats, contract_denominator, |stats| {
                BehaviorMeasure {
                    instance_count: stats
                        .star_behaviors
                        .iter()
                        .filter(|row| row.behavior == behavior)
                        .map(|row| row.centers)
                        .sum(),
                    address_count: stats
                        .star_behaviors
                        .iter()
                        .filter(|row| row.behavior == behavior)
                        .map(|row| row.wallets)
                        .sum(),
                    nft_count: stats
                        .star_behaviors
                        .iter()
                        .filter(|row| row.behavior == behavior)
                        .map(|row| row.tokens)
                        .sum(),
                    linked_loss_eth: stats
                        .star_behaviors
                        .iter()
                        .filter(|row| row.behavior == behavior)
                        .map(|row| row.total_value_eth)
                        .sum(),
                    linked_loss_usd: stats
                        .star_behaviors
                        .iter()
                        .filter(|row| row.behavior == behavior)
                        .map(|row| row.total_value_usd)
                        .sum(),
                    ..BehaviorMeasure::default()
                }
            })
        {
            apply_behavior_dedup(
                &mut row,
                behavior_contracts,
                behavior_addresses,
                behavior_nfts,
                behavior_buyers,
            );
            rows.push(row);
        }
    }
    if let Some(mut row) = build_behavior_row(
        "Layered Transfer",
        contract_stats,
        contract_denominator,
        |stats| BehaviorMeasure {
            instance_count: stats.layered_transfers.len() as i64,
            address_count: stats.layered_transfers.iter().map(|row| row.wallets).sum(),
            nft_count: stats.layered_transfers.iter().map(|row| row.tokens).sum(),
            linked_loss_eth: stats
                .layered_transfers
                .iter()
                .map(|row| row.total_value_eth)
                .sum(),
            linked_loss_usd: stats
                .layered_transfers
                .iter()
                .map(|row| row.total_value_usd)
                .sum(),
            ..BehaviorMeasure::default()
        },
    ) {
        apply_behavior_dedup(
            &mut row,
            behavior_contracts,
            behavior_addresses,
            behavior_nfts,
            behavior_buyers,
        );
        rows.push(row);
    }
    if let Some(mut row) = build_behavior_row(
        "Inventory Concentration",
        contract_stats,
        contract_denominator,
        |stats| BehaviorMeasure {
            instance_count: stats.inventory_concentration.len() as i64,
            address_count: stats
                .inventory_concentration
                .iter()
                .map(|row| row.source_wallets + 1)
                .sum(),
            nft_count: stats
                .inventory_concentration
                .iter()
                .map(|row| row.inbound_txns)
                .sum(),
            linked_loss_eth: stats
                .inventory_concentration
                .iter()
                .map(|row| row.value_collected_eth)
                .sum(),
            linked_loss_usd: stats
                .inventory_concentration
                .iter()
                .map(|row| row.value_collected_usd)
                .sum(),
            ..BehaviorMeasure::default()
        },
    ) {
        apply_behavior_dedup(
            &mut row,
            behavior_contracts,
            behavior_addresses,
            behavior_nfts,
            behavior_buyers,
        );
        rows.push(row);
    }

    let instance_denominator: i64 = rows.iter().map(|row| row.instance_count).sum();
    for row in &mut rows {
        row.instance_ratio_denominator = instance_denominator;
        row.instance_ratio = ratio_i64(row.instance_ratio_numerator, instance_denominator);
        total.contract_count += row.contract_count;
        total.instance_count += row.instance_count;
        total.instance_ratio_numerator += row.instance_ratio_numerator;
        total.address_count += row.address_count;
        total.nft_count += row.nft_count;
        total.linked_buyer_count += row.linked_buyer_count;
        total.linked_loss_eth += row.linked_loss_eth;
        total.linked_loss_usd += row.linked_loss_usd;
    }
    total.contract_count = contract_stats
        .iter()
        .filter(|stats| {
            !stats.wash_trading.is_empty()
                || !stats.pump_and_exit.is_empty()
                || !stats.star_behaviors.is_empty()
                || !stats.layered_transfers.is_empty()
                || !stats.inventory_concentration.is_empty()
        })
        .count() as i64;
    if !behavior_contracts.is_empty() {
        total.contract_count = union_count(behavior_contracts) as i64;
    }
    if !behavior_addresses.is_empty() {
        total.address_count = union_count(behavior_addresses) as i64;
    }
    if !behavior_nfts.is_empty() {
        total.nft_count = union_count(behavior_nfts) as i64;
    }
    if !behavior_buyers.is_empty() {
        total.linked_buyer_count = union_count(behavior_buyers) as i64;
    }
    total.contract_coverage_numerator = total.contract_count;
    total.contract_coverage_ratio = ratio_i64(
        total.contract_coverage_numerator,
        total.contract_coverage_denominator,
    );
    total.instance_ratio_denominator = instance_denominator;
    total.instance_ratio = ratio_i64(total.instance_ratio_numerator, instance_denominator);
    if instance_denominator > 0 {
        rows.push(total);
    }
    rows
}

fn apply_behavior_dedup(
    row: &mut PaperBehaviorSummaryRowPayload,
    behavior_contracts: &BTreeMap<String, BTreeSet<String>>,
    behavior_addresses: &BTreeMap<String, BTreeSet<String>>,
    behavior_nfts: &BTreeMap<String, BTreeSet<String>>,
    behavior_buyers: &BTreeMap<String, BTreeSet<String>>,
) {
    if let Some(contracts) = behavior_contracts.get(&row.behavior_type) {
        row.contract_count = contracts.len() as i64;
        row.contract_coverage_numerator = row.contract_count;
        row.contract_coverage_ratio = ratio_i64(
            row.contract_coverage_numerator,
            row.contract_coverage_denominator,
        );
    }
    if let Some(addresses) = behavior_addresses.get(&row.behavior_type) {
        row.address_count = addresses.len() as i64;
    }
    if let Some(nfts) = behavior_nfts.get(&row.behavior_type) {
        row.nft_count = nfts.len() as i64;
    }
    if let Some(buyers) = behavior_buyers.get(&row.behavior_type) {
        row.linked_buyer_count = buyers.len() as i64;
    }
}

fn union_count(source: &BTreeMap<String, BTreeSet<String>>) -> usize {
    source
        .values()
        .flat_map(|values| values.iter().cloned())
        .collect::<BTreeSet<_>>()
        .len()
}

fn build_behavior_row(
    behavior_type: &str,
    contract_stats: &[PaperContractBehaviorStatsPayload],
    contract_denominator: usize,
    measure: impl Fn(&PaperContractBehaviorStatsPayload) -> BehaviorMeasure,
) -> Option<PaperBehaviorSummaryRowPayload> {
    let mut row = PaperBehaviorSummaryRowPayload {
        behavior_type: behavior_type.into(),
        contract_coverage_denominator: contract_denominator as i64,
        ..PaperBehaviorSummaryRowPayload::default()
    };
    for stats in contract_stats {
        let measure = measure(stats);
        if measure.instance_count <= 0 {
            continue;
        }
        row.contract_count += 1;
        row.instance_count += measure.instance_count;
        row.instance_ratio_numerator += measure.instance_count;
        row.address_count += measure.address_count;
        row.nft_count += measure.nft_count;
        row.linked_buyer_count += measure.buyer_count;
        row.linked_loss_eth += measure.linked_loss_eth;
        row.linked_loss_usd += measure.linked_loss_usd;
    }
    if row.instance_count <= 0 {
        return None;
    }
    row.contract_coverage_numerator = row.contract_count;
    row.contract_coverage_ratio = ratio_i64(
        row.contract_coverage_numerator,
        row.contract_coverage_denominator,
    );
    Some(row)
}

fn average_optional(values: impl Iterator<Item = f64>) -> Option<f64> {
    let values = values.filter(|value| *value > 0.0).collect::<Vec<_>>();
    (!values.is_empty()).then_some(values.iter().sum::<f64>() / values.len() as f64)
}

fn gini(mut values: Vec<f64>) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal));
    let sum: f64 = values.iter().sum();
    if sum <= 0.0 {
        return Some(0.0);
    }
    let weighted_sum: f64 = values
        .iter()
        .enumerate()
        .map(|(index, value)| (index as f64 + 1.0) * value)
        .sum();
    Some(
        (2.0 * weighted_sum) / (values.len() as f64 * sum)
            - (values.len() as f64 + 1.0) / values.len() as f64,
    )
}

fn edge_token_ids(edge: &NftPropagationEdgePayload) -> Vec<String> {
    if edge.token_ids.is_empty() {
        if edge.token_id.trim().is_empty() {
            Vec::new()
        } else {
            vec![edge.token_id.clone()]
        }
    } else {
        edge.token_ids.clone()
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
        legit_duplicate_contract_count: input.legit_duplicates.len() as i64,
    }
}
