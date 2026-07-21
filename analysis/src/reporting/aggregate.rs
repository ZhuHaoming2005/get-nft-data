use crate::error::{AnalysisError, Result};
use crate::model::{
    AggregateDelta, BehaviorFacts, BehaviorKind, ChainId, EconomicFacts, EvidenceQuality,
    EvidenceStatus, GlobalAddressId, GlobalNftId, GlobalTxId, NftKey, NftSelection,
};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Default)]
pub struct AggregateState {
    // Completed worker deltas are merged only by the coordinator event loop.
    inner: AggregateInner,
}

#[derive(Default)]
struct AggregateInner {
    merged: DenseIdSet,
    seed_ids: BTreeSet<crate::model::SeedId>,
    /// Directional `(primary_seed_chain, candidate_chain)` cells.
    matrix: BTreeMap<(ChainId, ChainId), ScopeBucket>,
    /// Four-chain total: each candidate attributed once.
    all_chains: ScopeBucket,
    /// Same-chain native-safe economics keyed by candidate chain (once per candidate).
    economics_by_chain: BTreeMap<ChainId, EconomicFacts>,
    observed_nfts: BTreeSet<GlobalNftId>,
    observed_transactions: BTreeSet<GlobalTxId>,
    /// `(seed_id, candidate_id) -> suspected` (true = CopyMint / not legit).
    relation_suspected: BTreeMap<(crate::model::SeedId, crate::model::CandidateId), bool>,
}

/// Per-scope counters for contracts, economics, behaviors, addresses, and quality.
#[derive(Default)]
struct ScopeBucket {
    attributed: DenseIdSet,
    suspected_candidates: DenseIdSet,
    behavior_analyzed_suspected_candidates: DenseIdSet,
    legit_candidates: DenseIdSet,
    economics: EconomicFacts,
    quality: QualityAccumulator,
    representative_candidate_count: u64,
    infringing_nft_count: u64,
    malicious_addresses: BTreeSet<GlobalAddressId>,
    honest_addresses: BTreeSet<GlobalAddressId>,
    malicious_candidate_counts: BTreeMap<GlobalAddressId, u32>,
    behavior_contracts: BTreeMap<String, u64>,
    behavior_instances: BTreeMap<String, u64>,
    behavior_entities: BTreeMap<BehaviorKind, BehaviorEntities>,
    all_behavior_entities: BehaviorEntities,
    any_behavior_contracts: u64,
    gas_by_candidate: Vec<i128>,
    loss_by_candidate: Vec<i128>,
    output_input_at_least_one: u64,
    output_input_below_one: u64,
}

#[derive(Default)]
struct BehaviorEntities {
    addresses: DenseIdSet,
    nfts: DenseIdSet,
    linked_buyers: DenseIdSet,
    linked_loss_native: i128,
    linked_loss_usd_micros: i128,
}

impl BehaviorEntities {
    fn merge(&mut self, delta: &crate::model::BehaviorEntityDelta, include_native: bool) {
        for address in &delta.addresses {
            self.addresses.insert(address.0);
        }
        for nft in &delta.nfts {
            self.nfts.insert(nft.0);
        }
        for buyer in &delta.linked_buyers {
            self.linked_buyers.insert(buyer.0);
        }
        if include_native {
            self.linked_loss_native = self
                .linked_loss_native
                .saturating_add(delta.linked_loss_native);
        }
        self.linked_loss_usd_micros = self
            .linked_loss_usd_micros
            .saturating_add(delta.linked_loss_usd_micros);
    }

    fn merge_from(&mut self, other: &Self) {
        self.addresses.union_from(&other.addresses);
        self.nfts.union_from(&other.nfts);
        self.linked_buyers.union_from(&other.linked_buyers);
        self.linked_loss_native = self
            .linked_loss_native
            .saturating_add(other.linked_loss_native);
        self.linked_loss_usd_micros = self
            .linked_loss_usd_micros
            .saturating_add(other.linked_loss_usd_micros);
    }
}

#[derive(Default)]
struct DenseIdSet {
    words: Vec<u64>,
    len: u64,
}

impl DenseIdSet {
    fn insert(&mut self, id: u32) -> bool {
        let word = id as usize / 64;
        if self.words.len() <= word {
            self.words.resize(word + 1, 0);
        }
        let mask = 1_u64 << (id % 64);
        if self.words[word] & mask != 0 {
            return false;
        }
        self.words[word] |= mask;
        self.len += 1;
        true
    }

    const fn len(&self) -> u64 {
        self.len
    }

    fn union_from(&mut self, other: &Self) {
        if self.words.len() < other.words.len() {
            self.words.resize(other.words.len(), 0);
        }
        for (left, right) in self.words.iter_mut().zip(&other.words) {
            self.len = self
                .len
                .saturating_add((right & !*left).count_ones() as u64);
            *left |= right;
        }
    }
}

impl ScopeBucket {
    /// Cross-chain rows are a disjoint union of matrix cells for one primary
    /// chain. Deriving them here avoids repeating all candidate accounting in
    /// the ingestion hot path.
    fn merge_cross_cell(&mut self, other: &Self) {
        self.attributed.union_from(&other.attributed);
        self.suspected_candidates
            .union_from(&other.suspected_candidates);
        self.behavior_analyzed_suspected_candidates
            .union_from(&other.behavior_analyzed_suspected_candidates);
        self.legit_candidates.union_from(&other.legit_candidates);
        self.quality.merge_from(&other.quality);
        self.representative_candidate_count = self
            .representative_candidate_count
            .saturating_add(other.representative_candidate_count);
        self.infringing_nft_count = self
            .infringing_nft_count
            .saturating_add(other.infringing_nft_count);
        accumulate_economics_cross_chain(&mut self.economics, &other.economics);
        self.malicious_addresses
            .extend(other.malicious_addresses.iter().copied());
        self.honest_addresses
            .extend(other.honest_addresses.iter().copied());
        for (address, count) in &other.malicious_candidate_counts {
            let entry = self.malicious_candidate_counts.entry(*address).or_default();
            *entry = entry.saturating_add(*count);
        }
        merge_counts(&mut self.behavior_contracts, &other.behavior_contracts);
        merge_counts(&mut self.behavior_instances, &other.behavior_instances);
        for (kind, entities) in &other.behavior_entities {
            self.behavior_entities
                .entry(*kind)
                .or_default()
                .merge_from(entities);
        }
        self.all_behavior_entities
            .merge_from(&other.all_behavior_entities);
        self.any_behavior_contracts = self
            .any_behavior_contracts
            .saturating_add(other.any_behavior_contracts);
        self.gas_by_candidate
            .extend(other.gas_by_candidate.iter().copied());
        self.loss_by_candidate
            .extend(other.loss_by_candidate.iter().copied());
        self.output_input_at_least_one = self
            .output_input_at_least_one
            .saturating_add(other.output_input_at_least_one);
        self.output_input_below_one = self
            .output_input_below_one
            .saturating_add(other.output_input_below_one);
    }
}

fn merge_counts<K: Ord + Clone>(left: &mut BTreeMap<K, u64>, right: &BTreeMap<K, u64>) {
    for (key, count) in right {
        let entry = left.entry(key.clone()).or_default();
        *entry = entry.saturating_add(*count);
    }
}

impl AggregateState {
    pub fn relation_suspected(
        &self,
    ) -> BTreeMap<(crate::model::SeedId, crate::model::CandidateId), bool> {
        self.inner.relation_suspected.clone()
    }

    pub fn take_relation_suspected(
        &mut self,
    ) -> BTreeMap<(crate::model::SeedId, crate::model::CandidateId), bool> {
        std::mem::take(&mut self.inner.relation_suspected)
    }

    pub fn merge_once(&mut self, delta: AggregateDelta) -> Result<()> {
        let inner = &mut self.inner;
        if !inner.merged.insert(delta.candidate_id.0) {
            return Err(AnalysisError::State(format!(
                "candidate {} aggregate merged more than once",
                delta.candidate_id.0
            )));
        }
        inner
            .observed_nfts
            .extend(delta.global_nft_ids.iter().copied());
        inner
            .observed_transactions
            .extend(delta.global_transaction_ids.iter().copied());

        for relation in &delta.relation_deltas {
            inner.seed_ids.insert(relation.seed_id);
            inner.relation_suspected.insert(
                (relation.seed_id, relation.candidate_id),
                relation.suspected,
            );
        }

        let candidate_chain = delta
            .relation_deltas
            .first()
            .map(|relation| relation.candidate.chain);

        // Candidate-level mutual exclusion: any suspected → suspected everywhere.
        let any_suspected = delta
            .relation_deltas
            .iter()
            .any(|relation| relation.suspected);
        let only_legit = !any_suspected && !delta.relation_deltas.is_empty();

        // all_chains: once per candidate (USD only). Elevated infringing = all matched NFTs.
        absorb_candidate(
            &mut inner.all_chains,
            &delta,
            any_suspected,
            only_legit,
            /* include_native */ false,
            selection_count(delta.relation_deltas.iter()),
            if any_suspected {
                selection_count(delta.relation_deltas.iter())
            } else {
                0
            },
            &delta.suspected_economics,
            &delta.suspected_behaviors,
            &delta.behavior_entities,
            &delta.global_address_roles,
        );
        if any_suspected {
            if let Some(chain) = candidate_chain {
                accumulate_economics(
                    inner.economics_by_chain.entry(chain).or_default(),
                    &delta.suspected_economics,
                );
            }
        }

        let mut matrix_keys = BTreeSet::<(ChainId, ChainId)>::new();
        for relation in &delta.relation_deltas {
            let primary = relation.seed_chain;
            let candidate = relation.candidate.chain;
            matrix_keys.insert((primary, candidate));
        }

        let empty_bundle = crate::model::ScopedSuspectedBundle::default();
        for (primary, candidate) in matrix_keys {
            let cell_relations = delta
                .relation_deltas
                .iter()
                .filter(|relation| {
                    relation.seed_chain == primary && relation.candidate.chain == candidate
                })
                .collect::<Vec<_>>();
            let include_native = primary == candidate;
            let bundle = delta
                .matrix_suspected
                .get(&(primary, candidate))
                .unwrap_or(&empty_bundle);
            let bucket = inner.matrix.entry((primary, candidate)).or_default();
            absorb_candidate(
                bucket,
                &delta,
                any_suspected,
                only_legit,
                include_native,
                selection_count(cell_relations.iter().copied()),
                if any_suspected {
                    selection_count(cell_relations.iter().copied())
                } else {
                    0
                },
                &bundle.economics,
                &bundle.behaviors,
                &bundle.behavior_entities,
                &bundle.address_roles,
            );
        }

        // Failure scope annotations on all_chains quality (by chain / seed).
        let failure_count = delta.candidate_quality.failures.len() as u64;
        if failure_count > 0 {
            if let Some(chain) = candidate_chain {
                let count = inner
                    .all_chains
                    .quality
                    .snapshot
                    .failure_records_by_chain
                    .entry(chain)
                    .or_default();
                *count = count.saturating_add(failure_count);
            }
            for seed_id in delta
                .relation_deltas
                .iter()
                .map(|relation| relation.seed_id)
                .collect::<BTreeSet<_>>()
            {
                let count = inner
                    .all_chains
                    .quality
                    .snapshot
                    .failure_records_by_seed
                    .entry(seed_id)
                    .or_default();
                *count = count.saturating_add(failure_count);
            }
        }
        Ok(())
    }

    pub fn snapshot(&self) -> AggregateSnapshot {
        let inner = &self.inner;
        let mut scopes = Vec::new();
        for primary in ChainId::ALL {
            for candidate in ChainId::ALL {
                let empty = ScopeBucket::default();
                let bucket = inner.matrix.get(&(primary, candidate)).unwrap_or(&empty);
                scopes.push(scoped_metric(
                    "chain_matrix",
                    Some(primary),
                    Some(candidate),
                    bucket,
                ));
            }
        }
        for row in scopes
            .iter()
            .filter(|row| row.scope == "chain_matrix" && row.primary_chain == row.candidate_chain)
            .cloned()
            .collect::<Vec<_>>()
        {
            let mut intra = row;
            intra.scope = "intra_chain";
            scopes.push(intra);
        }
        for primary in ChainId::ALL {
            let mut bucket = ScopeBucket::default();
            for ((cell_primary, candidate), cell) in &inner.matrix {
                if *cell_primary == primary && *candidate != primary {
                    bucket.merge_cross_cell(cell);
                }
            }
            scopes.push(scoped_metric(
                "cross_chain_summary",
                Some(primary),
                None,
                &bucket,
            ));
        }
        let all = scoped_metric("all_chains", None, None, &inner.all_chains);
        let mut all = all;
        all.analyzed_seed_count = Some(inner.seed_ids.len() as u64);
        all.persisted_candidate_count = Some(inner.merged.len());
        all.observed_unique_nft_count = Some(inner.observed_nfts.len() as u64);
        all.observed_unique_transaction_count = Some(inner.observed_transactions.len() as u64);
        scopes.push(all.clone());

        AggregateSnapshot {
            persisted_candidate_count: inner.merged.len(),
            analyzed_seed_count: inner.seed_ids.len() as u64,
            representative_candidate_count: all.representative_candidate_count,
            candidate_contract_count: inner.merged.len(),
            suspected_duplicate_contract_count: all.suspected_duplicate_contract_count,
            behavior_analyzed_suspected_contract_count: all
                .behavior_analyzed_suspected_contract_count,
            infringing_nft_count: all.infringing_nft_count,
            legit_duplicate_contract_count: all.legit_duplicate_contract_count,
            malicious_address_count: all.malicious_address_count,
            repeat_infringing_malicious_address_count: all
                .repeat_infringing_malicious_address_count,
            honest_address_count: all.honest_address_count,
            total_classified_address_count: all.total_classified_address_count,
            observed_unique_nft_count: inner.observed_nfts.len() as u64,
            observed_unique_transaction_count: inner.observed_transactions.len() as u64,
            behaviors: all.behaviors.clone(),
            economics: all.economics.clone(),
            economics_by_chain: inner.economics_by_chain.clone(),
            economics_derived: all.economics_derived.clone(),
            data_quality: all.data_quality.clone(),
            scopes,
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn absorb_candidate(
    bucket: &mut ScopeBucket,
    delta: &AggregateDelta,
    is_suspected: bool,
    is_legit_only: bool,
    include_native: bool,
    representative_nfts: u64,
    infringing_nfts: u64,
    economics: &EconomicFacts,
    behaviors: &BehaviorFacts,
    behavior_entities: &[crate::model::BehaviorEntityDelta],
    address_roles: &[(GlobalAddressId, crate::model::AddressRoleKind)],
) {
    if !bucket.attributed.insert(delta.candidate_id.0) {
        return;
    }
    bucket.quality.add(&delta.candidate_quality);
    bucket.representative_candidate_count = bucket
        .representative_candidate_count
        .saturating_add(representative_nfts);

    if is_suspected {
        bucket.suspected_candidates.insert(delta.candidate_id.0);
        if delta.analysis_complete {
            bucket
                .behavior_analyzed_suspected_candidates
                .insert(delta.candidate_id.0);
        }
        bucket.infringing_nft_count = bucket.infringing_nft_count.saturating_add(infringing_nfts);
        if include_native {
            accumulate_economics(&mut bucket.economics, economics);
        } else {
            accumulate_economics_cross_chain(&mut bucket.economics, economics);
        }
        let gas = economics
            .setup_gas_usd_micros
            .saturating_add(economics.lure_gas_usd_micros)
            .saturating_add(economics.exit_gas_usd_micros);
        bucket.gas_by_candidate.push(gas);
        bucket
            .loss_by_candidate
            .push(economics.honest_loss_usd_micros);
        if gas > 0 && economics.operator_output_usd_micros > 0 {
            if economics.operator_output_usd_micros >= gas {
                bucket.output_input_at_least_one += 1;
            } else {
                bucket.output_input_below_one += 1;
            }
        }
        for (address, role) in address_roles {
            match role {
                crate::model::AddressRoleKind::SuspectedOperator
                | crate::model::AddressRoleKind::SuspectedColluder => {
                    bucket.malicious_addresses.insert(*address);
                    *bucket
                        .malicious_candidate_counts
                        .entry(*address)
                        .or_default() += 1;
                }
                crate::model::AddressRoleKind::LikelyVictim
                | crate::model::AddressRoleKind::CorruptedVictim => {
                    bucket.honest_addresses.insert(*address);
                }
                _ => {}
            }
        }
        if delta.analysis_complete {
            let mut any = false;
            for (name, count) in behavior_values(behaviors) {
                if count == 0 {
                    continue;
                }
                any = true;
                *bucket
                    .behavior_contracts
                    .entry(name.to_owned())
                    .or_default() += 1;
                let entry = bucket
                    .behavior_instances
                    .entry(name.to_owned())
                    .or_default();
                *entry = entry.saturating_add(count);
            }
            for entities in behavior_entities {
                bucket
                    .behavior_entities
                    .entry(entities.kind)
                    .or_default()
                    .merge(entities, include_native);
                bucket.all_behavior_entities.merge(entities, include_native);
            }
            bucket.any_behavior_contracts += u64::from(any);
        }
    } else if is_legit_only {
        bucket.legit_candidates.insert(delta.candidate_id.0);
    }
}

fn scoped_metric(
    scope: &'static str,
    primary_chain: Option<ChainId>,
    candidate_chain: Option<ChainId>,
    bucket: &ScopeBucket,
) -> ScopedAggregateMetric {
    let honest_address_count = bucket
        .honest_addresses
        .difference(&bucket.malicious_addresses)
        .count() as u64;
    let total_classified_address_count = bucket
        .malicious_addresses
        .union(&bucket.honest_addresses)
        .count() as u64;
    let total_behavior_instances = bucket.behavior_instances.values().copied().sum::<u64>();
    let behaviors = behavior_values(&BehaviorFacts::default())
        .map(|(name, _)| {
            let contracts = bucket.behavior_contracts.get(name).copied().unwrap_or(0);
            let instances = bucket.behavior_instances.get(name).copied().unwrap_or(0);
            let entities = behavior_kind(name).and_then(|kind| bucket.behavior_entities.get(&kind));
            (
                name.to_owned(),
                BehaviorAggregateMetric {
                    contract_count: contracts,
                    contract_coverage_ratio: ratio(
                        contracts,
                        bucket.behavior_analyzed_suspected_candidates.len(),
                    ),
                    instance_count: instances,
                    instance_ratio: ratio(instances, total_behavior_instances),
                    address_count: entities.map_or(0, |values| values.addresses.len()),
                    nft_count: entities.map_or(0, |values| values.nfts.len()),
                    linked_buyer_count: entities.map_or(0, |values| values.linked_buyers.len()),
                    linked_loss_native: entities.map_or(0, |values| values.linked_loss_native),
                    linked_loss_usd_micros: entities
                        .map_or(0, |values| values.linked_loss_usd_micros),
                },
            )
        })
        .chain(std::iter::once((
            "total".to_owned(),
            BehaviorAggregateMetric {
                contract_count: bucket.any_behavior_contracts,
                contract_coverage_ratio: ratio(
                    bucket.any_behavior_contracts,
                    bucket.behavior_analyzed_suspected_candidates.len(),
                ),
                instance_count: total_behavior_instances,
                instance_ratio: ratio(total_behavior_instances, total_behavior_instances),
                address_count: bucket.all_behavior_entities.addresses.len(),
                nft_count: bucket.all_behavior_entities.nfts.len(),
                linked_buyer_count: bucket.all_behavior_entities.linked_buyers.len(),
                linked_loss_native: bucket.all_behavior_entities.linked_loss_native,
                linked_loss_usd_micros: bucket.all_behavior_entities.linked_loss_usd_micros,
            },
        )))
        .collect();
    let attacker_gas_usd_micros = bucket
        .economics
        .setup_gas_usd_micros
        .saturating_add(bucket.economics.lure_gas_usd_micros)
        .saturating_add(bucket.economics.exit_gas_usd_micros);
    let economics_derived = EconomicAggregateDerived {
        attacker_gas_usd_micros,
        output_input_ratio: amount_ratio(
            bucket.economics.operator_output_usd_micros,
            attacker_gas_usd_micros,
        ),
        output_input_at_least_one: bucket.output_input_at_least_one,
        output_input_below_one: bucket.output_input_below_one,
        top_ten_percent_gas_contribution_ratio: top_ten_percent_ratio(
            bucket.gas_by_candidate.clone(),
        ),
        top_ten_percent_loss_contribution_ratio: top_ten_percent_ratio(
            bucket.loss_by_candidate.clone(),
        ),
        stuck_nft_ratio: ratio(
            bucket.economics.stuck_nft_count,
            bucket.infringing_nft_count,
        ),
        stuck_time_ratio: amount_ratio(
            bucket.economics.stuck_time_numerator_seconds,
            bucket.economics.stuck_time_denominator_seconds,
        ),
    };
    ScopedAggregateMetric {
        scope,
        primary_chain,
        candidate_chain,
        representative_candidate_count: bucket.representative_candidate_count,
        candidate_contract_count: bucket.attributed.len(),
        suspected_duplicate_contract_count: bucket.suspected_candidates.len(),
        behavior_analyzed_suspected_contract_count: bucket
            .behavior_analyzed_suspected_candidates
            .len(),
        infringing_nft_count: bucket.infringing_nft_count,
        legit_duplicate_contract_count: bucket.legit_candidates.len(),
        malicious_address_count: bucket.malicious_addresses.len() as u64,
        repeat_infringing_malicious_address_count: bucket
            .malicious_candidate_counts
            .values()
            .filter(|count| **count >= 2)
            .count() as u64,
        honest_address_count,
        total_classified_address_count,
        analyzed_seed_count: None,
        persisted_candidate_count: None,
        observed_unique_nft_count: None,
        observed_unique_transaction_count: None,
        behaviors,
        economics: bucket.economics.clone(),
        economics_derived,
        data_quality: bucket.quality.clone().snapshot(),
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct AggregateSnapshot {
    pub persisted_candidate_count: u64,
    pub analyzed_seed_count: u64,
    pub representative_candidate_count: u64,
    pub candidate_contract_count: u64,
    pub suspected_duplicate_contract_count: u64,
    pub behavior_analyzed_suspected_contract_count: u64,
    pub infringing_nft_count: u64,
    pub legit_duplicate_contract_count: u64,
    pub malicious_address_count: u64,
    pub repeat_infringing_malicious_address_count: u64,
    pub honest_address_count: u64,
    pub total_classified_address_count: u64,
    pub observed_unique_nft_count: u64,
    pub observed_unique_transaction_count: u64,
    pub behaviors: BTreeMap<String, BehaviorAggregateMetric>,
    pub economics: EconomicFacts,
    pub economics_by_chain: BTreeMap<ChainId, EconomicFacts>,
    pub economics_derived: EconomicAggregateDerived,
    pub data_quality: QualityAggregateSnapshot,
    /// All analysis metrics at `intra_chain` / `chain_matrix` / `cross_chain_summary` / `all_chains`.
    pub scopes: Vec<ScopedAggregateMetric>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ScopedAggregateMetric {
    pub scope: &'static str,
    pub primary_chain: Option<ChainId>,
    pub candidate_chain: Option<ChainId>,
    pub representative_candidate_count: u64,
    pub candidate_contract_count: u64,
    pub suspected_duplicate_contract_count: u64,
    pub behavior_analyzed_suspected_contract_count: u64,
    pub infringing_nft_count: u64,
    pub legit_duplicate_contract_count: u64,
    pub malicious_address_count: u64,
    pub repeat_infringing_malicious_address_count: u64,
    pub honest_address_count: u64,
    pub total_classified_address_count: u64,
    /// Populated only on `all_chains`.
    pub analyzed_seed_count: Option<u64>,
    pub persisted_candidate_count: Option<u64>,
    pub observed_unique_nft_count: Option<u64>,
    pub observed_unique_transaction_count: Option<u64>,
    pub behaviors: BTreeMap<String, BehaviorAggregateMetric>,
    pub economics: EconomicFacts,
    pub economics_derived: EconomicAggregateDerived,
    pub data_quality: QualityAggregateSnapshot,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct BehaviorAggregateMetric {
    pub contract_count: u64,
    pub contract_coverage_ratio: Option<f64>,
    pub instance_count: u64,
    pub instance_ratio: Option<f64>,
    pub address_count: u64,
    pub nft_count: u64,
    pub linked_buyer_count: u64,
    pub linked_loss_native: i128,
    pub linked_loss_usd_micros: i128,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct EconomicAggregateDerived {
    pub attacker_gas_usd_micros: i128,
    pub output_input_ratio: Option<f64>,
    pub output_input_at_least_one: u64,
    pub output_input_below_one: u64,
    pub top_ten_percent_gas_contribution_ratio: Option<f64>,
    pub top_ten_percent_loss_contribution_ratio: Option<f64>,
    pub stuck_nft_ratio: Option<f64>,
    pub stuck_time_ratio: Option<f64>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct QualityAggregateSnapshot {
    pub status_counts: BTreeMap<String, BTreeMap<String, u64>>,
    pub sale_prices_parsed: u64,
    pub sale_prices_total: u64,
    pub sale_price_parse_ratio: Option<f64>,
    pub candidate_assets_analyzed: u64,
    pub candidate_assets_total: u64,
    pub candidate_asset_coverage: Option<f64>,
    pub candidate_asset_truncated_contracts: u64,
    pub history_assets_requested: u64,
    pub history_assets_succeeded: u64,
    pub history_assets_complete: u64,
    pub history_assets_failed: u64,
    pub history_assets_not_requested: u64,
    pub history_assets_truncated: u64,
    pub transactions_fetched: u64,
    pub transactions_provider_reported: u64,
    pub transactions_failed: u64,
    pub transaction_coverage: Option<f64>,
    pub signature_discovery_failures: u64,
    pub transaction_detail_failures: u64,
    pub unattributed_solana_transactions: u64,
    pub unresolved_compressed_mints: u64,
    pub missing_mint_pre_balances: u64,
    pub missing_collection_authorities: u64,
    pub supplemental_query_failures: u64,
    pub failure_records: u64,
    pub failure_records_by_chain: BTreeMap<ChainId, u64>,
    pub failure_records_by_seed: BTreeMap<crate::model::SeedId, u64>,
}

#[derive(Clone)]
struct QualityAccumulator {
    snapshot: QualityAggregateSnapshot,
    all_assets_complete: bool,
    all_transactions_complete: bool,
    all_prices_complete: bool,
}

impl Default for QualityAccumulator {
    fn default() -> Self {
        Self {
            snapshot: QualityAggregateSnapshot::default(),
            all_assets_complete: true,
            all_transactions_complete: true,
            all_prices_complete: true,
        }
    }
}

impl QualityAccumulator {
    fn add(&mut self, quality: &EvidenceQuality) {
        for (category, status) in [
            ("assets", quality.assets),
            ("histories", quality.histories),
            ("transactions", quality.transactions),
            ("prices", quality.prices),
            ("authority", quality.authority),
        ] {
            *self
                .snapshot
                .status_counts
                .entry(category.to_owned())
                .or_default()
                .entry(status_name(status).to_owned())
                .or_default() += 1;
        }
        self.all_assets_complete &= terminal_complete(quality.assets);
        self.all_transactions_complete &= terminal_complete(quality.transactions);
        self.all_prices_complete &= terminal_complete(quality.prices);
        macro_rules! add {
            ($field:ident) => {
                self.snapshot.$field = self.snapshot.$field.saturating_add(quality.$field);
            };
        }
        add!(sale_prices_parsed);
        add!(sale_prices_total);
        add!(candidate_assets_analyzed);
        add!(candidate_assets_total);
        add!(history_assets_requested);
        add!(history_assets_succeeded);
        add!(history_assets_complete);
        add!(history_assets_failed);
        add!(history_assets_not_requested);
        add!(history_assets_truncated);
        add!(transactions_fetched);
        add!(transactions_provider_reported);
        add!(transactions_failed);
        add!(signature_discovery_failures);
        add!(transaction_detail_failures);
        add!(unattributed_solana_transactions);
        add!(unresolved_compressed_mints);
        add!(missing_mint_pre_balances);
        add!(missing_collection_authorities);
        add!(supplemental_query_failures);
        self.snapshot.failure_records = self
            .snapshot
            .failure_records
            .saturating_add(quality.failures.len() as u64);
        self.snapshot.candidate_asset_truncated_contracts = self
            .snapshot
            .candidate_asset_truncated_contracts
            .saturating_add(u64::from(quality.assets == Some(EvidenceStatus::Truncated)));
    }

    fn merge_from(&mut self, other: &Self) {
        for (category, statuses) in &other.snapshot.status_counts {
            let target = self
                .snapshot
                .status_counts
                .entry(category.clone())
                .or_default();
            merge_counts(target, statuses);
        }
        macro_rules! add {
            ($($field:ident),+ $(,)?) => {
                $(
                    self.snapshot.$field = self.snapshot.$field
                        .saturating_add(other.snapshot.$field);
                )+
            };
        }
        add!(
            sale_prices_parsed,
            sale_prices_total,
            candidate_assets_analyzed,
            candidate_assets_total,
            candidate_asset_truncated_contracts,
            history_assets_requested,
            history_assets_succeeded,
            history_assets_complete,
            history_assets_failed,
            history_assets_not_requested,
            history_assets_truncated,
            transactions_fetched,
            transactions_provider_reported,
            transactions_failed,
            signature_discovery_failures,
            transaction_detail_failures,
            unattributed_solana_transactions,
            unresolved_compressed_mints,
            missing_mint_pre_balances,
            missing_collection_authorities,
            supplemental_query_failures,
            failure_records,
        );
        merge_counts(
            &mut self.snapshot.failure_records_by_chain,
            &other.snapshot.failure_records_by_chain,
        );
        merge_counts(
            &mut self.snapshot.failure_records_by_seed,
            &other.snapshot.failure_records_by_seed,
        );
        self.all_assets_complete &= other.all_assets_complete;
        self.all_transactions_complete &= other.all_transactions_complete;
        self.all_prices_complete &= other.all_prices_complete;
    }

    fn snapshot(mut self) -> QualityAggregateSnapshot {
        self.snapshot.sale_price_parse_ratio = complete_ratio(
            self.snapshot.sale_prices_parsed,
            self.snapshot.sale_prices_total,
            self.all_prices_complete,
        );
        self.snapshot.candidate_asset_coverage = complete_ratio(
            self.snapshot.candidate_assets_analyzed,
            self.snapshot.candidate_assets_total,
            self.all_assets_complete,
        );
        self.snapshot.transaction_coverage = complete_ratio(
            self.snapshot.transactions_fetched,
            self.snapshot.transactions_provider_reported,
            self.all_transactions_complete,
        );
        self.snapshot
    }
}

fn status_name(status: Option<EvidenceStatus>) -> &'static str {
    match status {
        None => "unknown",
        Some(EvidenceStatus::NotRequested) => "not_requested",
        Some(EvidenceStatus::Requested) => "requested",
        Some(EvidenceStatus::Complete) => "complete",
        Some(EvidenceStatus::Empty) => "empty",
        Some(EvidenceStatus::Truncated) => "truncated",
        Some(EvidenceStatus::Failed) => "failed",
    }
}

fn terminal_complete(status: Option<EvidenceStatus>) -> bool {
    matches!(
        status,
        Some(EvidenceStatus::Complete | EvidenceStatus::Empty)
    )
}

fn complete_ratio(numerator: u64, denominator: u64, complete: bool) -> Option<f64> {
    (complete && denominator != 0 && numerator <= denominator)
        .then(|| numerator as f64 / denominator as f64)
}

fn selection_count<'a>(relations: impl Iterator<Item = &'a crate::model::RelationDelta>) -> u64 {
    let mut all = None;
    let mut explicit = BTreeSet::<NftKey>::new();
    for relation in relations {
        match &relation.selection {
            NftSelection::AllInContract { nft_count, .. } => {
                all = Some(all.unwrap_or(0_u64).max(*nft_count));
            }
            NftSelection::Explicit { nfts } => explicit.extend(nfts.iter().cloned()),
        }
    }
    all.unwrap_or(explicit.len() as u64)
}

fn accumulate_economics(total: &mut EconomicFacts, candidate: &EconomicFacts) {
    accumulate_economics_usd(total, candidate);
    accumulate_economics_native(total, candidate);
}

/// Cross-chain / multi-chain totals must not sum native base units.
fn accumulate_economics_cross_chain(total: &mut EconomicFacts, candidate: &EconomicFacts) {
    accumulate_economics_usd(total, candidate);
}

fn accumulate_economics_usd(total: &mut EconomicFacts, candidate: &EconomicFacts) {
    macro_rules! accumulate {
        ($($field:ident),+ $(,)?) => {
            $(
                total.$field = total.$field.saturating_add(candidate.$field);
            )+
        };
    }
    accumulate!(
        gross_revenue_usd_micros,
        marketplace_fee_usd_micros,
        funding_usd_micros,
        withdrawal_usd_micros,
        revenue_backflow_usd_micros,
        setup_gas_usd_micros,
        lure_gas_usd_micros,
        exit_gas_usd_micros,
        operator_output_usd_micros,
        secondary_sale_loss_usd_micros,
        paid_mint_loss_usd_micros,
        honest_loss_usd_micros,
        stuck_nft_count,
        stuck_time_numerator_seconds,
        stuck_time_denominator_seconds,
    );
}

fn accumulate_economics_native(total: &mut EconomicFacts, candidate: &EconomicFacts) {
    macro_rules! accumulate {
        ($($field:ident),+ $(,)?) => {
            $(
                total.$field = total.$field.saturating_add(candidate.$field);
            )+
        };
    }
    accumulate!(
        gross_revenue_native,
        marketplace_fee_native,
        funding_native,
        withdrawal_native,
        revenue_backflow_native,
        setup_gas_native,
        lure_gas_native,
        exit_gas_native,
        operator_output_native,
        secondary_sale_loss_native,
        paid_mint_loss_native,
        honest_loss_native,
    );
}

fn behavior_values(behaviors: &BehaviorFacts) -> impl Iterator<Item = (&'static str, u64)> {
    [
        ("wash_trading", behaviors.wash_cycles),
        ("pump_and_exit", behaviors.pump_and_exit),
        ("sybil_distribution", behaviors.sybil_distribution),
        ("fraud_revenue", behaviors.fraud_revenue),
        ("poisoning", behaviors.poisoning),
        ("layered_transfer", behaviors.layered_transfer),
        ("inventory_concentration", behaviors.inventory_concentration),
    ]
    .into_iter()
}

fn behavior_kind(name: &str) -> Option<BehaviorKind> {
    match name {
        "wash_trading" => Some(BehaviorKind::WashTrading),
        "pump_and_exit" => Some(BehaviorKind::PumpAndExit),
        "sybil_distribution" => Some(BehaviorKind::SybilDistribution),
        "fraud_revenue" => Some(BehaviorKind::FraudRevenue),
        "poisoning" => Some(BehaviorKind::Poisoning),
        "layered_transfer" => Some(BehaviorKind::LayeredTransfer),
        "inventory_concentration" => Some(BehaviorKind::InventoryConcentration),
        _ => None,
    }
}

fn ratio(numerator: u64, denominator: u64) -> Option<f64> {
    (denominator != 0).then(|| numerator as f64 / denominator as f64)
}

fn amount_ratio(numerator: i128, denominator: i128) -> Option<f64> {
    (denominator > 0).then(|| numerator as f64 / denominator as f64)
}

fn top_ten_percent_ratio(mut values: Vec<i128>) -> Option<f64> {
    values.retain(|value| *value >= 0);
    if values.is_empty() {
        return None;
    }
    values.sort_unstable_by(|left, right| right.cmp(left));
    let total = values
        .iter()
        .fold(0_i128, |sum, value| sum.saturating_add(*value));
    if total == 0 {
        return None;
    }
    let top_count = values.len().div_ceil(10);
    let top = values[..top_count]
        .iter()
        .fold(0_i128, |sum, value| sum.saturating_add(*value));
    Some(top as f64 / total as f64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{
        BehaviorEntityDelta, BehaviorFacts, BehaviorKind, CandidateId, ContractKey,
        EvidenceQuality, EvidenceStatus, GlobalAddressId, GlobalNftId, NftSelection, RelationDelta,
        SeedId,
    };

    fn relation(
        seed_id: u16,
        seed_chain: ChainId,
        candidate_id: u32,
        candidate: ContractKey,
        suspected: bool,
    ) -> RelationDelta {
        RelationDelta {
            seed_id: SeedId(seed_id),
            seed_chain,
            candidate_id: CandidateId(candidate_id),
            candidate,
            selection: NftSelection::Explicit { nfts: Vec::new() },
            suspected,
            economics: EconomicFacts::default(),
            behaviors: BehaviorFacts::default(),
        }
    }

    #[test]
    fn duplicate_completion_cannot_merge_twice() {
        let mut state = AggregateState::default();
        let delta = AggregateDelta {
            candidate_id: CandidateId(1),
            analysis_complete: true,
            relation_deltas: Vec::new(),
            suspected_economics: EconomicFacts::default(),
            suspected_behaviors: BehaviorFacts::default(),
            behavior_entities: Vec::new(),
            matrix_suspected: BTreeMap::new(),
            candidate_quality: EvidenceQuality::default(),
            global_address_roles: Vec::new(),
            global_nft_ids: Vec::new(),
            global_transaction_ids: Vec::new(),
        };
        state.merge_once(delta.clone()).unwrap();
        assert!(state.merge_once(delta).is_err());
    }

    #[test]
    fn candidate_economics_are_not_multiplied_by_seed_relations() {
        let mut state = AggregateState::default();
        let candidate = ContractKey::new(ChainId::Ethereum, "0x1");
        let economics = EconomicFacts {
            honest_loss_usd_micros: 10,
            honest_loss_native: 7,
            ..Default::default()
        };
        state
            .merge_once(AggregateDelta {
                candidate_id: CandidateId(1),
                analysis_complete: true,
                relation_deltas: vec![
                    relation(0, ChainId::Ethereum, 1, candidate.clone(), true),
                    relation(1, ChainId::Ethereum, 1, candidate, true),
                ],
                suspected_economics: economics,
                suspected_behaviors: BehaviorFacts::default(),
                behavior_entities: Vec::new(),
                matrix_suspected: BTreeMap::new(),
                candidate_quality: EvidenceQuality::default(),
                global_address_roles: Vec::new(),
                global_nft_ids: Vec::new(),
                global_transaction_ids: Vec::new(),
            })
            .unwrap();
        let snapshot = state.snapshot();
        assert_eq!(snapshot.economics.honest_loss_usd_micros, 10);
        // all_chains never sums native.
        assert_eq!(snapshot.economics.honest_loss_native, 0);
    }

    #[test]
    fn mixed_suspected_and_legit_relations_count_as_suspected_only() {
        let mut state = AggregateState::default();
        let candidate = ContractKey::new(ChainId::Base, "0xmixed");
        state
            .merge_once(AggregateDelta {
                candidate_id: CandidateId(9),
                analysis_complete: true,
                relation_deltas: vec![
                    relation(0, ChainId::Base, 9, candidate.clone(), true),
                    relation(1, ChainId::Ethereum, 9, candidate, false),
                ],
                suspected_economics: EconomicFacts::default(),
                suspected_behaviors: BehaviorFacts::default(),
                behavior_entities: Vec::new(),
                matrix_suspected: BTreeMap::new(),
                candidate_quality: EvidenceQuality::default(),
                global_address_roles: Vec::new(),
                global_nft_ids: Vec::new(),
                global_transaction_ids: Vec::new(),
            })
            .unwrap();
        let snapshot = state.snapshot();
        assert_eq!(snapshot.suspected_duplicate_contract_count, 1);
        assert_eq!(snapshot.legit_duplicate_contract_count, 0);
        let eth_base = snapshot
            .scopes
            .iter()
            .find(|row| {
                row.scope == "chain_matrix"
                    && row.primary_chain == Some(ChainId::Ethereum)
                    && row.candidate_chain == Some(ChainId::Base)
            })
            .expect("eth→base cell");
        assert_eq!(eth_base.legit_duplicate_contract_count, 0);
        assert_eq!(eth_base.suspected_duplicate_contract_count, 1);
    }

    #[test]
    fn cross_chain_and_all_chains_exclude_native_amounts() {
        let mut state = AggregateState::default();
        let eth = ContractKey::new(ChainId::Ethereum, "0xeth");
        let sol = ContractKey::new(ChainId::Solana, "solcol");
        state
            .merge_once(AggregateDelta {
                candidate_id: CandidateId(1),
                analysis_complete: true,
                relation_deltas: vec![relation(0, ChainId::Ethereum, 1, eth, true)],
                suspected_economics: EconomicFacts {
                    honest_loss_native: 100,
                    honest_loss_usd_micros: 1_000,
                    ..Default::default()
                },
                suspected_behaviors: BehaviorFacts {
                    wash_cycles: 1,
                    ..Default::default()
                },
                behavior_entities: vec![BehaviorEntityDelta {
                    kind: BehaviorKind::WashTrading,
                    addresses: vec![GlobalAddressId(1)],
                    nfts: vec![GlobalNftId(1)],
                    linked_buyers: vec![GlobalAddressId(2)],
                    linked_loss_native: 50,
                    linked_loss_usd_micros: 500,
                }],
                matrix_suspected: BTreeMap::from([(
                    (ChainId::Ethereum, ChainId::Ethereum),
                    crate::model::ScopedSuspectedBundle {
                        economics: EconomicFacts {
                            honest_loss_native: 100,
                            honest_loss_usd_micros: 1_000,
                            ..Default::default()
                        },
                        behaviors: BehaviorFacts {
                            wash_cycles: 1,
                            ..Default::default()
                        },
                        behavior_entities: vec![BehaviorEntityDelta {
                            kind: BehaviorKind::WashTrading,
                            addresses: vec![GlobalAddressId(1)],
                            nfts: vec![GlobalNftId(1)],
                            linked_buyers: vec![GlobalAddressId(2)],
                            linked_loss_native: 50,
                            linked_loss_usd_micros: 500,
                        }],
                        address_roles: Vec::new(),
                    },
                )]),
                candidate_quality: EvidenceQuality::default(),
                global_address_roles: Vec::new(),
                global_nft_ids: Vec::new(),
                global_transaction_ids: Vec::new(),
            })
            .unwrap();
        state
            .merge_once(AggregateDelta {
                candidate_id: CandidateId(2),
                analysis_complete: true,
                relation_deltas: vec![relation(1, ChainId::Ethereum, 2, sol, true)],
                suspected_economics: EconomicFacts {
                    honest_loss_native: 200,
                    honest_loss_usd_micros: 2_000,
                    ..Default::default()
                },
                suspected_behaviors: BehaviorFacts::default(),
                behavior_entities: Vec::new(),
                matrix_suspected: BTreeMap::from([(
                    (ChainId::Ethereum, ChainId::Solana),
                    crate::model::ScopedSuspectedBundle {
                        economics: EconomicFacts {
                            honest_loss_native: 200,
                            honest_loss_usd_micros: 2_000,
                            ..Default::default()
                        },
                        behaviors: BehaviorFacts::default(),
                        behavior_entities: Vec::new(),
                        address_roles: Vec::new(),
                    },
                )]),
                candidate_quality: EvidenceQuality::default(),
                global_address_roles: Vec::new(),
                global_nft_ids: Vec::new(),
                global_transaction_ids: Vec::new(),
            })
            .unwrap();
        let snapshot = state.snapshot();
        assert_eq!(snapshot.economics.honest_loss_usd_micros, 3_000);
        assert_eq!(snapshot.economics.honest_loss_native, 0);
        assert_eq!(snapshot.behaviors["wash_trading"].linked_loss_native, 0);
        assert_eq!(
            snapshot.behaviors["wash_trading"].linked_loss_usd_micros,
            500
        );

        let intra_eth = snapshot
            .scopes
            .iter()
            .find(|row| {
                row.scope == "intra_chain"
                    && row.primary_chain == Some(ChainId::Ethereum)
                    && row.candidate_chain == Some(ChainId::Ethereum)
            })
            .expect("intra eth");
        assert_eq!(intra_eth.economics.honest_loss_native, 100);
        assert_eq!(intra_eth.behaviors["wash_trading"].linked_loss_native, 50);

        let cross = snapshot
            .scopes
            .iter()
            .find(|row| {
                row.scope == "cross_chain_summary" && row.primary_chain == Some(ChainId::Ethereum)
            })
            .expect("cross eth");
        assert_eq!(cross.economics.honest_loss_usd_micros, 2_000);
        assert_eq!(cross.economics.honest_loss_native, 0);
    }

    #[test]
    fn scopes_cover_intra_matrix_cross_and_all_chains() {
        let snapshot = AggregateState::default().snapshot();
        let scopes: BTreeSet<_> = snapshot.scopes.iter().map(|row| row.scope).collect();
        assert!(scopes.contains("intra_chain"));
        assert!(scopes.contains("chain_matrix"));
        assert!(scopes.contains("cross_chain_summary"));
        assert!(scopes.contains("all_chains"));
        assert_eq!(
            snapshot
                .scopes
                .iter()
                .filter(|row| row.scope == "chain_matrix")
                .count(),
            16
        );
    }

    #[test]
    fn failed_analysis_is_excluded_from_behavior_coverage_denominator() {
        let mut state = AggregateState::default();
        let candidate = ContractKey::new(ChainId::Ethereum, "0x2");
        state
            .merge_once(AggregateDelta {
                candidate_id: CandidateId(2),
                analysis_complete: false,
                relation_deltas: vec![relation(0, ChainId::Ethereum, 2, candidate, true)],
                suspected_economics: EconomicFacts::default(),
                suspected_behaviors: BehaviorFacts::default(),
                behavior_entities: Vec::new(),
                matrix_suspected: BTreeMap::new(),
                candidate_quality: EvidenceQuality::default(),
                global_address_roles: Vec::new(),
                global_nft_ids: Vec::new(),
                global_transaction_ids: Vec::new(),
            })
            .unwrap();
        let snapshot = state.snapshot();
        assert_eq!(snapshot.suspected_duplicate_contract_count, 1);
        assert_eq!(snapshot.behavior_analyzed_suspected_contract_count, 0);
        assert_eq!(
            snapshot.behaviors["wash_trading"].contract_coverage_ratio,
            None
        );
    }

    #[test]
    fn quality_summary_preserves_truncation_history_and_failure_scope() {
        let mut state = AggregateState::default();
        let candidate = ContractKey::new(ChainId::Polygon, "0x4");
        state
            .merge_once(AggregateDelta {
                candidate_id: CandidateId(4),
                analysis_complete: false,
                relation_deltas: vec![relation(9, ChainId::Polygon, 4, candidate, true)],
                suspected_economics: EconomicFacts::default(),
                suspected_behaviors: BehaviorFacts::default(),
                behavior_entities: Vec::new(),
                matrix_suspected: BTreeMap::new(),
                candidate_quality: EvidenceQuality {
                    assets: Some(EvidenceStatus::Truncated),
                    history_assets_requested: 3,
                    history_assets_succeeded: 2,
                    history_assets_complete: 1,
                    history_assets_failed: 1,
                    history_assets_truncated: 1,
                    failures: vec!["history failed".into(), "authority failed".into()],
                    ..EvidenceQuality::default()
                },
                global_address_roles: Vec::new(),
                global_nft_ids: Vec::new(),
                global_transaction_ids: Vec::new(),
            })
            .unwrap();

        let quality = state.snapshot().data_quality;
        assert_eq!(quality.candidate_asset_truncated_contracts, 1);
        assert_eq!(quality.history_assets_requested, 3);
        assert_eq!(quality.history_assets_succeeded, 2);
        assert_eq!(quality.history_assets_complete, 1);
        assert_eq!(quality.history_assets_failed, 1);
        assert_eq!(quality.history_assets_truncated, 1);
        assert_eq!(quality.failure_records, 2);
        assert_eq!(quality.failure_records_by_chain[&ChainId::Polygon], 2);
        assert_eq!(quality.failure_records_by_seed[&SeedId(9)], 2);
    }

    #[test]
    fn invalid_coverage_counts_do_not_emit_a_ratio_above_one() {
        assert_eq!(complete_ratio(2, 1, true), None);
        assert_eq!(complete_ratio(1, 2, true), Some(0.5));
    }

    #[test]
    fn behavior_entities_are_deduplicated_in_kind_and_total_summaries() {
        let mut state = AggregateState::default();
        let candidate = ContractKey::new(ChainId::Ethereum, "0x3");
        state
            .merge_once(AggregateDelta {
                candidate_id: CandidateId(3),
                analysis_complete: true,
                relation_deltas: vec![relation(0, ChainId::Ethereum, 3, candidate, true)],
                suspected_economics: EconomicFacts::default(),
                suspected_behaviors: BehaviorFacts {
                    wash_cycles: 2,
                    ..Default::default()
                },
                behavior_entities: vec![
                    BehaviorEntityDelta {
                        kind: BehaviorKind::WashTrading,
                        addresses: vec![GlobalAddressId(1), GlobalAddressId(2)],
                        nfts: vec![GlobalNftId(4)],
                        linked_buyers: vec![GlobalAddressId(8)],
                        linked_loss_native: 10,
                        linked_loss_usd_micros: 20,
                    },
                    BehaviorEntityDelta {
                        kind: BehaviorKind::WashTrading,
                        addresses: vec![GlobalAddressId(2), GlobalAddressId(3)],
                        nfts: vec![GlobalNftId(4)],
                        linked_buyers: vec![GlobalAddressId(8)],
                        linked_loss_native: 30,
                        linked_loss_usd_micros: 40,
                    },
                ],
                matrix_suspected: BTreeMap::from([(
                    (ChainId::Ethereum, ChainId::Ethereum),
                    crate::model::ScopedSuspectedBundle {
                        economics: EconomicFacts::default(),
                        behaviors: BehaviorFacts {
                            wash_cycles: 2,
                            ..Default::default()
                        },
                        behavior_entities: vec![
                            BehaviorEntityDelta {
                                kind: BehaviorKind::WashTrading,
                                addresses: vec![GlobalAddressId(1), GlobalAddressId(2)],
                                nfts: vec![GlobalNftId(4)],
                                linked_buyers: vec![GlobalAddressId(8)],
                                linked_loss_native: 10,
                                linked_loss_usd_micros: 20,
                            },
                            BehaviorEntityDelta {
                                kind: BehaviorKind::WashTrading,
                                addresses: vec![GlobalAddressId(2), GlobalAddressId(3)],
                                nfts: vec![GlobalNftId(4)],
                                linked_buyers: vec![GlobalAddressId(8)],
                                linked_loss_native: 30,
                                linked_loss_usd_micros: 40,
                            },
                        ],
                        address_roles: Vec::new(),
                    },
                )]),
                candidate_quality: EvidenceQuality::default(),
                global_address_roles: Vec::new(),
                global_nft_ids: Vec::new(),
                global_transaction_ids: Vec::new(),
            })
            .unwrap();
        let snapshot = state.snapshot();
        let wash = &snapshot.behaviors["wash_trading"];
        assert_eq!(wash.address_count, 3);
        assert_eq!(wash.nft_count, 1);
        assert_eq!(wash.linked_buyer_count, 1);
        // all_chains excludes native
        assert_eq!(wash.linked_loss_native, 0);
        assert_eq!(wash.linked_loss_usd_micros, 60);
        assert_eq!(snapshot.behaviors["total"].address_count, 3);
        let intra = snapshot
            .scopes
            .iter()
            .find(|row| row.scope == "intra_chain" && row.primary_chain == Some(ChainId::Ethereum))
            .expect("intra");
        assert_eq!(intra.behaviors["wash_trading"].linked_loss_native, 40);
    }

    #[test]
    fn matrix_cells_use_scoped_economics_not_full_candidate_union() {
        let mut state = AggregateState::default();
        let candidate = ContractKey::new(ChainId::Ethereum, "0xsplit");
        state
            .merge_once(AggregateDelta {
                candidate_id: CandidateId(11),
                analysis_complete: true,
                relation_deltas: vec![
                    relation(0, ChainId::Ethereum, 11, candidate.clone(), true),
                    relation(1, ChainId::Base, 11, candidate, true),
                ],
                suspected_economics: EconomicFacts {
                    honest_loss_usd_micros: 30,
                    ..Default::default()
                },
                suspected_behaviors: BehaviorFacts::default(),
                behavior_entities: Vec::new(),
                matrix_suspected: BTreeMap::from([
                    (
                        (ChainId::Ethereum, ChainId::Ethereum),
                        crate::model::ScopedSuspectedBundle {
                            economics: EconomicFacts {
                                honest_loss_usd_micros: 10,
                                honest_loss_native: 10,
                                ..Default::default()
                            },
                            ..Default::default()
                        },
                    ),
                    (
                        (ChainId::Base, ChainId::Ethereum),
                        crate::model::ScopedSuspectedBundle {
                            economics: EconomicFacts {
                                honest_loss_usd_micros: 20,
                                honest_loss_native: 20,
                                ..Default::default()
                            },
                            ..Default::default()
                        },
                    ),
                ]),
                candidate_quality: EvidenceQuality::default(),
                global_address_roles: Vec::new(),
                global_nft_ids: Vec::new(),
                global_transaction_ids: Vec::new(),
            })
            .unwrap();
        let snapshot = state.snapshot();
        assert_eq!(snapshot.economics.honest_loss_usd_micros, 30);
        let eth_eth = snapshot
            .scopes
            .iter()
            .find(|row| {
                row.scope == "chain_matrix"
                    && row.primary_chain == Some(ChainId::Ethereum)
                    && row.candidate_chain == Some(ChainId::Ethereum)
            })
            .expect("eth→eth");
        let base_eth = snapshot
            .scopes
            .iter()
            .find(|row| {
                row.scope == "chain_matrix"
                    && row.primary_chain == Some(ChainId::Base)
                    && row.candidate_chain == Some(ChainId::Ethereum)
            })
            .expect("base→eth");
        assert_eq!(eth_eth.economics.honest_loss_usd_micros, 10);
        assert_eq!(eth_eth.economics.honest_loss_native, 10);
        assert_eq!(base_eth.economics.honest_loss_usd_micros, 20);
        assert_eq!(base_eth.economics.honest_loss_native, 0);
    }

    #[test]
    fn infringing_matches_copymint_when_all_relations_suspected() {
        use crate::model::NftKey;
        use std::sync::Arc;

        let mut state = AggregateState::default();
        let candidate = ContractKey::new(ChainId::Ethereum, "0xcopy");
        let nft = NftKey {
            chain: ChainId::Ethereum,
            contract_address: Arc::from("0xcopy"),
            token_id: Arc::from("1"),
        };
        state
            .merge_once(AggregateDelta {
                candidate_id: CandidateId(5),
                analysis_complete: true,
                relation_deltas: vec![RelationDelta {
                    seed_id: SeedId(0),
                    seed_chain: ChainId::Ethereum,
                    candidate_id: CandidateId(5),
                    candidate,
                    selection: NftSelection::Explicit {
                        nfts: vec![nft.clone()],
                    },
                    suspected: true,
                    economics: EconomicFacts::default(),
                    behaviors: BehaviorFacts::default(),
                }],
                suspected_economics: EconomicFacts::default(),
                suspected_behaviors: BehaviorFacts::default(),
                behavior_entities: Vec::new(),
                matrix_suspected: BTreeMap::new(),
                candidate_quality: EvidenceQuality::default(),
                global_address_roles: Vec::new(),
                global_nft_ids: Vec::new(),
                global_transaction_ids: Vec::new(),
            })
            .unwrap();
        let snapshot = state.snapshot();
        assert_eq!(snapshot.infringing_nft_count, 1);
        let labels = state.relation_suspected();
        assert_eq!(labels.get(&(SeedId(0), CandidateId(5))), Some(&true));
    }
}
