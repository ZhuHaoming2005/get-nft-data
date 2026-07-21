use crate::model::{
    CandidateId, ChainId, ContractKey, Dimension, MatchEvidence, NftKey, NftSelection,
    SeedCandidateRelation, SeedId,
};
use crate::resident::ContractCatalog;
use crate::seed::SeedManifest;
use ahash::AHashSet;
use serde::Serialize;
use std::collections::BTreeMap;

const TOTAL_DIMENSION: &str = "total";

/// The report-only projection of a dedup relation. Candidate artifacts retain
/// the complete public relation/evidence schema; the run-level report keeps
/// only fields that contribute to its counters.
#[derive(Clone, Debug)]
pub struct ScopeRelation {
    pub seed_id: SeedId,
    pub candidate_id: CandidateId,
    pub candidate: ContractKey,
    pub dimensions: u8,
    pub selection: NftSelection,
    pub token_uri_nfts: Vec<NftKey>,
    pub image_uri_nfts: Vec<NftKey>,
    pub incomplete: bool,
}

impl From<&SeedCandidateRelation> for ScopeRelation {
    fn from(relation: &SeedCandidateRelation) -> Self {
        let mut token_uri_nfts = Vec::new();
        let mut image_uri_nfts = Vec::new();
        for evidence in &relation.evidence {
            let MatchEvidence::Uri {
                dimension,
                candidate_nft,
                ..
            } = evidence
            else {
                continue;
            };
            match dimension {
                Dimension::TokenUri => token_uri_nfts.push(candidate_nft.clone()),
                Dimension::ImageUri => image_uri_nfts.push(candidate_nft.clone()),
                Dimension::Name | Dimension::Metadata => {}
            }
        }
        token_uri_nfts.sort_unstable();
        token_uri_nfts.dedup();
        image_uri_nfts.sort_unstable();
        image_uri_nfts.dedup();
        Self {
            seed_id: relation.seed_id,
            candidate_id: relation.candidate_id,
            candidate: relation.candidate.clone(),
            dimensions: relation.dimensions,
            selection: relation.selection.clone(),
            token_uri_nfts,
            image_uri_nfts,
            incomplete: relation.incomplete,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ScopeMetric {
    pub scope: &'static str,
    pub primary_chain: Option<ChainId>,
    pub candidate_chain: Option<ChainId>,
    pub dimension: &'static str,
    pub duplicate_nft_count: u64,
    pub duplicate_contract_count: u64,
    pub nft_denominator: u64,
    pub contract_denominator: u64,
    pub duplicate_nft_ratio: Option<f64>,
    pub duplicate_contract_ratio: Option<f64>,
    /// 查重命中的 NFT 去重总数（含合法重复），与 `duplicate_nft_count` 同值。
    pub matched_nft_count: u64,
    /// CopyMint = 查重命中 − legit_duplicate。
    pub copymint_nft_count: u64,
    /// `copymint_nft_count / matched_nft_count`。
    pub copymint_nft_ratio: Option<f64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct SeedDedupMetric {
    pub seed_id: SeedId,
    pub primary_chain: ChainId,
    pub relation_count: u64,
    pub duplicate_contract_count: u64,
    pub duplicate_nft_count: u64,
    pub incomplete: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct DedupScopeReport {
    pub selected_seed_count: u64,
    pub analyzed_seed_count: u64,
    pub failed_seed_count: u64,
    pub seed_completion_ratio: f64,
    pub seed_with_duplicate_count: u64,
    pub seed_duplicate_ratio: f64,
    pub metrics: Vec<ScopeMetric>,
    pub seeds: Vec<SeedDedupMetric>,
}

#[derive(Clone, Debug)]
enum Coverage<'a> {
    All(u64),
    Explicit(AHashSet<&'a NftKey>),
}

impl<'a> Coverage<'a> {
    fn add_selection(&mut self, selection: &'a NftSelection) {
        match selection {
            NftSelection::AllInContract { nft_count, .. } => *self = Self::All(*nft_count),
            NftSelection::Explicit { nfts } => {
                if let Self::Explicit(values) = self {
                    values.extend(nfts);
                }
            }
        }
    }

    fn add_nft(&mut self, nft: &'a NftKey) {
        if let Self::Explicit(values) = self {
            values.insert(nft);
        }
    }

    fn merge(&mut self, other: &Self) {
        match (&mut *self, other) {
            (Self::All(left), Self::All(right)) => *left = (*left).max(*right),
            (Self::All(_), Self::Explicit(_)) => {}
            (slot, Self::All(count)) => *slot = Self::All(*count),
            (Self::Explicit(left), Self::Explicit(right)) => {
                left.extend(right.iter().copied());
            }
        }
    }

    fn count(&self) -> u64 {
        match self {
            Self::All(count) => *count,
            Self::Explicit(values) => values.len() as u64,
        }
    }
}

type CoverageMap<'a> = BTreeMap<CandidateId, Coverage<'a>>;
type DirectionalKey = (ChainId, ChainId, &'static str);

pub fn build_scope_report(
    manifest: &SeedManifest,
    catalog: &ContractCatalog,
    relations: &[ScopeRelation],
    suspected_relations: &BTreeMap<(SeedId, CandidateId), bool>,
    failed_seeds: &std::collections::BTreeSet<SeedId>,
) -> DedupScopeReport {
    let seed_chains = manifest
        .seeds
        .iter()
        .map(|seed| (seed.id, seed.chain))
        .collect::<BTreeMap<_, _>>();
    // Candidate is CopyMint if any relation is suspected/unlabeled; then all matched
    // NFTs for that candidate count as CopyMint (mixed elevates to suspected).
    let mut candidate_is_copymint = BTreeMap::<CandidateId, bool>::new();
    for relation in relations {
        if relation.incomplete || failed_seeds.contains(&relation.seed_id) {
            continue;
        }
        let relation_copymint = suspected_relations
            .get(&(relation.seed_id, relation.candidate_id))
            .copied()
            .unwrap_or(true);
        *candidate_is_copymint
            .entry(relation.candidate_id)
            .or_insert(false) |= relation_copymint;
    }
    let mut matched = BTreeMap::<DirectionalKey, CoverageMap<'_>>::new();
    let mut copymint = BTreeMap::<DirectionalKey, CoverageMap<'_>>::new();
    for relation in relations {
        if relation.incomplete || failed_seeds.contains(&relation.seed_id) {
            continue;
        }
        let primary = seed_chains[&relation.seed_id];
        add_relation_to_directional(&mut matched, primary, relation);
        if candidate_is_copymint
            .get(&relation.candidate_id)
            .copied()
            .unwrap_or(true)
        {
            add_relation_to_directional(&mut copymint, primary, relation);
        }
    }
    let denominators = chain_denominators(catalog);
    let mut metrics = Vec::new();
    for primary in ChainId::ALL {
        for candidate in ChainId::ALL {
            for dimension in dimension_names() {
                metrics.push(direct_metric(
                    "chain_matrix",
                    Some(primary),
                    Some(candidate),
                    dimension,
                    matched
                        .get(&(primary, candidate, dimension))
                        .into_iter()
                        .flat_map(BTreeMap::iter),
                    copymint
                        .get(&(primary, candidate, dimension))
                        .into_iter()
                        .flat_map(BTreeMap::iter),
                    denominators[&candidate],
                ));
            }
        }
    }
    metrics.extend(
        metrics
            .iter()
            .filter(|row| row.scope == "chain_matrix" && row.primary_chain == row.candidate_chain)
            .cloned()
            .map(|mut row| {
                row.scope = "intra_chain";
                row
            })
            .collect::<Vec<_>>(),
    );
    for primary in ChainId::ALL {
        for dimension in dimension_names() {
            metrics.push(direct_metric(
                "cross_chain_summary",
                Some(primary),
                None,
                dimension,
                matched
                    .iter()
                    .filter(|((left, right, candidate_dimension), _)| {
                        *left == primary && *right != primary && *candidate_dimension == dimension
                    })
                    .flat_map(|(_, values)| values.iter()),
                copymint
                    .iter()
                    .filter(|((left, right, candidate_dimension), _)| {
                        *left == primary && *right != primary && *candidate_dimension == dimension
                    })
                    .flat_map(|(_, values)| values.iter()),
                combined_denominator(&denominators, |chain| chain != primary),
            ));
        }
    }
    for dimension in dimension_names() {
        metrics.push(metric(
            "all_chains",
            None,
            None,
            dimension,
            matched
                .iter()
                .filter(|((_, _, candidate_dimension), _)| *candidate_dimension == dimension)
                .flat_map(|(_, values)| values.iter()),
            copymint
                .iter()
                .filter(|((_, _, candidate_dimension), _)| *candidate_dimension == dimension)
                .flat_map(|(_, values)| values.iter()),
            combined_denominator(&denominators, |_| true),
        ));
    }

    let mut relations_by_seed = BTreeMap::<SeedId, Vec<&ScopeRelation>>::new();
    for relation in relations {
        relations_by_seed
            .entry(relation.seed_id)
            .or_default()
            .push(relation);
    }
    let seeds = manifest
        .seeds
        .iter()
        .map(|seed| {
            let seed_relations = relations_by_seed
                .get(&seed.id)
                .map(Vec::as_slice)
                .unwrap_or_default();
            let incomplete = failed_seeds.contains(&seed.id)
                || seed_relations.iter().any(|relation| relation.incomplete);
            let formal_relations = if incomplete { &[][..] } else { seed_relations };
            let mut coverage = CoverageMap::new();
            for relation in formal_relations {
                add_relation_coverage(&mut coverage, relation.candidate_id, &relation.selection);
            }
            SeedDedupMetric {
                seed_id: seed.id,
                primary_chain: seed.chain,
                relation_count: formal_relations.len() as u64,
                duplicate_contract_count: coverage.len() as u64,
                duplicate_nft_count: coverage.values().map(Coverage::count).sum(),
                incomplete,
            }
        })
        .collect::<Vec<_>>();
    let failed_seed_count = seeds.iter().filter(|seed| seed.incomplete).count() as u64;
    let analyzed_seed_count = seeds.len() as u64 - failed_seed_count;
    let seed_with_duplicate_count = seeds
        .iter()
        .filter(|seed| !seed.incomplete && seed.relation_count > 0)
        .count() as u64;
    DedupScopeReport {
        selected_seed_count: seeds.len() as u64,
        analyzed_seed_count,
        failed_seed_count,
        seed_completion_ratio: ratio(analyzed_seed_count, seeds.len() as u64).unwrap_or(0.0),
        seed_with_duplicate_count,
        seed_duplicate_ratio: ratio(seed_with_duplicate_count, analyzed_seed_count).unwrap_or(0.0),
        metrics,
        seeds,
    }
}

fn add_relation_to_directional<'a>(
    directional: &mut BTreeMap<DirectionalKey, CoverageMap<'a>>,
    primary: ChainId,
    relation: &'a ScopeRelation,
) {
    add_relation_coverage(
        directional
            .entry((primary, relation.candidate.chain, TOTAL_DIMENSION))
            .or_default(),
        relation.candidate_id,
        &relation.selection,
    );
    for dimension in Dimension::ALL {
        if relation.dimensions & dimension.bit() == 0 {
            continue;
        }
        let coverage = directional
            .entry((primary, relation.candidate.chain, dimension.as_str()))
            .or_default();
        match dimension {
            Dimension::Name | Dimension::Metadata => {
                add_relation_coverage(coverage, relation.candidate_id, &relation.selection);
            }
            Dimension::TokenUri | Dimension::ImageUri => {
                let nfts = match dimension {
                    Dimension::TokenUri => &relation.token_uri_nfts,
                    Dimension::ImageUri => &relation.image_uri_nfts,
                    Dimension::Name | Dimension::Metadata => unreachable!(),
                };
                let candidate = coverage
                    .entry(relation.candidate_id)
                    .or_insert_with(|| Coverage::Explicit(AHashSet::new()));
                for nft in nfts {
                    candidate.add_nft(nft);
                }
            }
        }
    }
}

fn add_relation_coverage<'a>(
    coverage: &mut CoverageMap<'a>,
    candidate_id: CandidateId,
    selection: &'a NftSelection,
) {
    coverage
        .entry(candidate_id)
        .or_insert_with(|| Coverage::Explicit(AHashSet::new()))
        .add_selection(selection);
}

fn direct_metric<'a, 'b>(
    scope: &'static str,
    primary_chain: Option<ChainId>,
    candidate_chain: Option<ChainId>,
    dimension: &'static str,
    matched: impl Iterator<Item = (&'b CandidateId, &'b Coverage<'a>)>,
    copymint: impl Iterator<Item = (&'b CandidateId, &'b Coverage<'a>)>,
    denominator: (u64, u64),
) -> ScopeMetric
where
    'a: 'b,
{
    let (duplicate_contract_count, duplicate_nft_count) =
        matched.fold((0_u64, 0_u64), |(contracts, nfts), (_, coverage)| {
            (
                contracts.saturating_add(1),
                nfts.saturating_add(coverage.count()),
            )
        });
    let copymint_nft_count = copymint.fold(0_u64, |nfts, (_, coverage)| {
        nfts.saturating_add(coverage.count())
    });
    ScopeMetric {
        scope,
        primary_chain,
        candidate_chain,
        dimension,
        duplicate_nft_count,
        duplicate_contract_count,
        nft_denominator: denominator.0,
        contract_denominator: denominator.1,
        duplicate_nft_ratio: ratio(duplicate_nft_count, denominator.0),
        duplicate_contract_ratio: ratio(duplicate_contract_count, denominator.1),
        matched_nft_count: duplicate_nft_count,
        copymint_nft_count,
        copymint_nft_ratio: ratio(copymint_nft_count, duplicate_nft_count),
    }
}

fn metric<'a, 'b>(
    scope: &'static str,
    primary_chain: Option<ChainId>,
    candidate_chain: Option<ChainId>,
    dimension: &'static str,
    matched: impl Iterator<Item = (&'b CandidateId, &'b Coverage<'a>)>,
    copymint: impl Iterator<Item = (&'b CandidateId, &'b Coverage<'a>)>,
    denominator: (u64, u64),
) -> ScopeMetric
where
    'a: 'b,
{
    let mut matched_merged = CoverageMap::new();
    for (&candidate_id, coverage) in matched {
        matched_merged
            .entry(candidate_id)
            .and_modify(|current| current.merge(coverage))
            .or_insert_with(|| coverage.clone());
    }
    let mut copymint_merged = CoverageMap::new();
    for (&candidate_id, coverage) in copymint {
        copymint_merged
            .entry(candidate_id)
            .and_modify(|current| current.merge(coverage))
            .or_insert_with(|| coverage.clone());
    }
    let duplicate_nft_count = matched_merged.values().map(Coverage::count).sum();
    let duplicate_contract_count = matched_merged.len() as u64;
    let copymint_nft_count = copymint_merged.values().map(Coverage::count).sum();
    ScopeMetric {
        scope,
        primary_chain,
        candidate_chain,
        dimension,
        duplicate_nft_count,
        duplicate_contract_count,
        nft_denominator: denominator.0,
        contract_denominator: denominator.1,
        duplicate_nft_ratio: ratio(duplicate_nft_count, denominator.0),
        duplicate_contract_ratio: ratio(duplicate_contract_count, denominator.1),
        matched_nft_count: duplicate_nft_count,
        copymint_nft_count,
        copymint_nft_ratio: ratio(copymint_nft_count, duplicate_nft_count),
    }
}

fn chain_denominators(catalog: &ContractCatalog) -> BTreeMap<ChainId, (u64, u64)> {
    let mut values = ChainId::ALL
        .into_iter()
        .map(|chain| (chain, (0, 0)))
        .collect::<BTreeMap<_, _>>();
    for contract in &catalog.contracts {
        let entry = values.get_mut(&contract.chain).expect("known chain");
        entry.0 += contract.nft_count;
        entry.1 += 1;
    }
    values
}

fn combined_denominator(
    values: &BTreeMap<ChainId, (u64, u64)>,
    include: impl Fn(ChainId) -> bool,
) -> (u64, u64) {
    ChainId::ALL
        .into_iter()
        .filter(|chain| include(*chain))
        .map(|chain| values[&chain])
        .fold((0_u64, 0_u64), |left, right| {
            (left.0 + right.0, left.1 + right.1)
        })
}

fn dimension_names() -> impl Iterator<Item = &'static str> {
    Dimension::ALL
        .into_iter()
        .map(Dimension::as_str)
        .chain(std::iter::once(TOTAL_DIMENSION))
}

fn ratio(numerator: u64, denominator: u64) -> Option<f64> {
    (denominator != 0).then(|| numerator as f64 / denominator as f64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{CandidateId, ContractKey, ContractRecord};
    use crate::seed::SeedDefinition;
    use chrono::Utc;
    use std::collections::BTreeSet;
    use std::sync::Arc;

    #[test]
    fn dimension_counts_preserve_uri_granularity_and_contract_union() {
        let now = Utc::now();
        let manifest = SeedManifest {
            generated_at: now,
            seeds: vec![SeedDefinition {
                id: SeedId(0),
                chain: ChainId::Base,
                contract_address: "seed".to_owned(),
                rank: 1,
                collection_name: "seed".to_owned(),
                stable_identifier: "seed".to_owned(),
                ranking_metric: "volume".to_owned(),
                ranking_value: 1.0,
                ranking_window: "all".to_owned(),
                source: "fixture".to_owned(),
                collected_at: now,
            }],
        };
        let candidate = ContractKey::new(ChainId::Ethereum, "copy");
        let nft = NftKey {
            chain: ChainId::Ethereum,
            contract_address: Arc::from("copy"),
            token_id: Arc::from("1"),
        };
        let relation = SeedCandidateRelation {
            seed_id: SeedId(0),
            seed: ContractKey::new(ChainId::Ethereum, "seed"),
            candidate_id: CandidateId(0),
            candidate: candidate.clone(),
            dimensions: Dimension::Name.bit() | Dimension::TokenUri.bit(),
            selection: NftSelection::AllInContract {
                contract: candidate,
                nft_count: 10,
            },
            evidence: vec![MatchEvidence::Uri {
                dimension: Dimension::TokenUri,
                uri: Arc::from("ipfs://same"),
                seed_nft: NftKey {
                    chain: ChainId::Base,
                    contract_address: Arc::from("seed"),
                    token_id: Arc::from("1"),
                },
                candidate_nft: nft,
            }],
            incomplete: false,
        };
        let catalog = ContractCatalog {
            contracts: vec![ContractRecord {
                chain: ChainId::Ethereum,
                address: Arc::from("copy"),
                nft_count: 10,
                name_value_id: None,
                metadata_profile_id: None,
                name_owner_shard: None,
                metadata_owner_shard: None,
            }],
        };
        let scope_relation = ScopeRelation::from(&relation);
        let report = build_scope_report(
            &manifest,
            &catalog,
            std::slice::from_ref(&scope_relation),
            &BTreeMap::new(),
            &BTreeSet::new(),
        );
        let row = |dimension| {
            report
                .metrics
                .iter()
                .find(|row| {
                    row.scope == "chain_matrix"
                        && row.primary_chain == Some(ChainId::Base)
                        && row.candidate_chain == Some(ChainId::Ethereum)
                        && row.dimension == dimension
                })
                .unwrap()
        };
        assert_eq!(row("name").duplicate_nft_count, 10);
        assert_eq!(row("token_uri").duplicate_nft_count, 1);
        assert_eq!(row("total").duplicate_nft_count, 10);
        assert_eq!(row("total").matched_nft_count, 10);
        assert_eq!(row("total").copymint_nft_count, 10);
        assert_eq!(row("total").copymint_nft_ratio, Some(1.0));

        let mut suspected = BTreeMap::new();
        suspected.insert((SeedId(0), CandidateId(0)), false);
        let filtered = build_scope_report(
            &manifest,
            &catalog,
            &[scope_relation],
            &suspected,
            &BTreeSet::new(),
        );
        let filtered_total = filtered
            .metrics
            .iter()
            .find(|row| {
                row.scope == "chain_matrix"
                    && row.primary_chain == Some(ChainId::Base)
                    && row.candidate_chain == Some(ChainId::Ethereum)
                    && row.dimension == "total"
            })
            .unwrap();
        assert_eq!(filtered_total.matched_nft_count, 10);
        assert_eq!(filtered_total.copymint_nft_count, 0);
        assert_eq!(filtered_total.copymint_nft_ratio, Some(0.0));
    }

    #[test]
    fn mixed_candidate_elevates_all_matched_nfts_to_copymint() {
        let now = Utc::now();
        let manifest = SeedManifest {
            generated_at: now,
            seeds: vec![
                SeedDefinition {
                    id: SeedId(0),
                    chain: ChainId::Ethereum,
                    contract_address: "seed-eth".to_owned(),
                    rank: 1,
                    collection_name: "seed".to_owned(),
                    stable_identifier: "seed-eth".to_owned(),
                    ranking_metric: "volume".to_owned(),
                    ranking_value: 1.0,
                    ranking_window: "all".to_owned(),
                    source: "fixture".to_owned(),
                    collected_at: now,
                },
                SeedDefinition {
                    id: SeedId(1),
                    chain: ChainId::Base,
                    contract_address: "seed-base".to_owned(),
                    rank: 1,
                    collection_name: "seed".to_owned(),
                    stable_identifier: "seed-base".to_owned(),
                    ranking_metric: "volume".to_owned(),
                    ranking_value: 1.0,
                    ranking_window: "all".to_owned(),
                    source: "fixture".to_owned(),
                    collected_at: now,
                },
            ],
        };
        let candidate = ContractKey::new(ChainId::Polygon, "copy");
        let relation = |seed_id, seed_chain, count| SeedCandidateRelation {
            seed_id: SeedId(seed_id),
            seed: ContractKey::new(seed_chain, format!("seed-{seed_id}")),
            candidate_id: CandidateId(0),
            candidate: candidate.clone(),
            dimensions: Dimension::Name.bit(),
            selection: NftSelection::AllInContract {
                contract: candidate.clone(),
                nft_count: count,
            },
            evidence: Vec::new(),
            incomplete: false,
        };
        let relations = [
            relation(0, ChainId::Ethereum, 3),
            relation(1, ChainId::Base, 5),
        ];
        let mut labels = BTreeMap::new();
        labels.insert((SeedId(0), CandidateId(0)), true);
        labels.insert((SeedId(1), CandidateId(0)), false);
        let catalog = ContractCatalog {
            contracts: vec![ContractRecord {
                chain: ChainId::Polygon,
                address: Arc::from("copy"),
                nft_count: 8,
                name_value_id: None,
                metadata_profile_id: None,
                name_owner_shard: None,
                metadata_owner_shard: None,
            }],
        };
        let scope_relations = relations
            .iter()
            .map(ScopeRelation::from)
            .collect::<Vec<_>>();
        let report = build_scope_report(
            &manifest,
            &catalog,
            &scope_relations,
            &labels,
            &BTreeSet::new(),
        );
        let base_poly = report
            .metrics
            .iter()
            .find(|row| {
                row.scope == "chain_matrix"
                    && row.primary_chain == Some(ChainId::Base)
                    && row.candidate_chain == Some(ChainId::Polygon)
                    && row.dimension == "total"
            })
            .unwrap();
        assert_eq!(base_poly.matched_nft_count, 5);
        // Legit-labeled edge still counts as CopyMint after candidate elevation.
        assert_eq!(base_poly.copymint_nft_count, 5);
    }

    #[test]
    fn failed_seed_without_relations_is_counted_and_incomplete_relations_are_excluded() {
        let now = Utc::now();
        let manifest = SeedManifest {
            generated_at: now,
            seeds: vec![
                SeedDefinition {
                    id: SeedId(0),
                    chain: ChainId::Ethereum,
                    contract_address: "seed-0".to_owned(),
                    rank: 1,
                    collection_name: "seed".to_owned(),
                    stable_identifier: "seed-0".to_owned(),
                    ranking_metric: "volume".to_owned(),
                    ranking_value: 1.0,
                    ranking_window: "all".to_owned(),
                    source: "fixture".to_owned(),
                    collected_at: now,
                },
                SeedDefinition {
                    id: SeedId(1),
                    chain: ChainId::Base,
                    contract_address: "seed-1".to_owned(),
                    rank: 2,
                    collection_name: "seed".to_owned(),
                    stable_identifier: "seed-1".to_owned(),
                    ranking_metric: "volume".to_owned(),
                    ranking_value: 1.0,
                    ranking_window: "all".to_owned(),
                    source: "fixture".to_owned(),
                    collected_at: now,
                },
            ],
        };
        let candidate = ContractKey::new(ChainId::Polygon, "copy");
        let relation = SeedCandidateRelation {
            seed_id: SeedId(1),
            seed: ContractKey::new(ChainId::Base, "seed-1"),
            candidate_id: CandidateId(0),
            candidate: candidate.clone(),
            dimensions: Dimension::Name.bit(),
            selection: NftSelection::AllInContract {
                contract: candidate,
                nft_count: 7,
            },
            evidence: Vec::new(),
            incomplete: true,
        };
        let catalog = ContractCatalog {
            contracts: vec![ContractRecord {
                chain: ChainId::Polygon,
                address: Arc::from("copy"),
                nft_count: 7,
                name_value_id: None,
                metadata_profile_id: None,
                name_owner_shard: None,
                metadata_owner_shard: None,
            }],
        };
        let scope_relation = ScopeRelation::from(&relation);
        let report = build_scope_report(
            &manifest,
            &catalog,
            &[scope_relation],
            &BTreeMap::new(),
            &BTreeSet::from([SeedId(0)]),
        );

        assert_eq!(report.selected_seed_count, 2);
        assert_eq!(report.analyzed_seed_count, 0);
        assert_eq!(report.failed_seed_count, 2);
        assert_eq!(report.seed_completion_ratio, 0.0);
        assert!(report
            .seeds
            .iter()
            .all(|seed| seed.incomplete && seed.relation_count == 0));
        assert!(report
            .metrics
            .iter()
            .all(|metric| metric.duplicate_nft_count == 0 && metric.duplicate_contract_count == 0));
    }
}
