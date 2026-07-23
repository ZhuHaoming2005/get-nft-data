//! Scope counters and duplicate-scale rows for offline reports.

use ahash::{AHashMap, AHashSet};
use serde::{Deserialize, Serialize};

use crate::dedup::hits::{Dimension, HitGraph, ScopeKind};
use crate::entity::{ChainId, ContractId, NftId, ResidentStore};

/// Per-dimension and union NFT numerator counts for one seed scope.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ScopeNftCounts {
    pub name: u64,
    pub token_uri: u64,
    pub image_uri: u64,
    pub metadata: u64,
    /// Four-dimension set-union size (not the sum of the rows above).
    pub total: u64,
}

impl ScopeNftCounts {
    pub fn from_sets(
        name: usize,
        token_uri: usize,
        image_uri: usize,
        metadata: usize,
        total: usize,
    ) -> Self {
        Self {
            name: name as u64,
            token_uri: token_uri as u64,
            image_uri: image_uri as u64,
            metadata: metadata as u64,
            total: total as u64,
        }
    }
}

/// One duplicate-scale category row (`token_uri` / `image_uri` / `metadata` / `name` / `total`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DuplicateScaleRow {
    pub category: String,
    pub duplicate_nft_count: u64,
    pub duplicate_nft_ratio: Option<f64>,
    pub duplicate_nft_ratio_numerator: u64,
    pub duplicate_nft_ratio_denominator: u64,
    pub duplicate_contract_count: u64,
    pub duplicate_contract_ratio: Option<f64>,
    pub duplicate_contract_ratio_numerator: u64,
    pub duplicate_contract_ratio_denominator: u64,
}

/// Directional chain-matrix block for one secondary chain.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ChainMatrixBlock {
    pub secondary_chain: String,
    pub rows: Vec<DuplicateScaleRow>,
}

/// Per-seed duplicate-scale sections.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SeedDuplicateScale {
    pub intra_chain: Vec<DuplicateScaleRow>,
    pub chain_matrix: Vec<ChainMatrixBlock>,
    pub cross_chain_summary: Vec<DuplicateScaleRow>,
}

/// Build `contract_id → nft_ids` map used by HitGraph expansion helpers.
pub fn build_contract_nft_map(store: &ResidentStore) -> AHashMap<ContractId, Vec<NftId>> {
    let mut map: AHashMap<ContractId, Vec<NftId>> = AHashMap::new();
    for nft in &store.nfts {
        map.entry(nft.contract_id).or_default().push(nft.id);
    }
    for ids in map.values_mut() {
        ids.sort_unstable();
    }
    map
}

/// Count NFT numerators for one `seed` under `scope`: per-dimension rows + set-union `total`.
///
/// Only edges with `edge.seed_contract == seed` are counted. `image_uri` excludes NFTs already
/// counted under `token_uri` for the same seed/scope (other seeds must not contaminate).
pub fn count_scope_nfts(
    graph: &HitGraph,
    seed: ContractId,
    scope: ScopeKind,
    primary_chain: ChainId,
    matrix_secondary: Option<ChainId>,
    contract_nfts: &AHashMap<ContractId, Vec<NftId>>,
) -> ScopeNftCounts {
    let name = graph.dimension_candidate_nfts(
        seed,
        Dimension::Name,
        scope,
        primary_chain,
        matrix_secondary,
        contract_nfts,
    );
    let token_uri = graph.dimension_candidate_nfts(
        seed,
        Dimension::TokenUri,
        scope,
        primary_chain,
        matrix_secondary,
        contract_nfts,
    );
    let image_uri = graph.dimension_candidate_nfts(
        seed,
        Dimension::ImageUri,
        scope,
        primary_chain,
        matrix_secondary,
        contract_nfts,
    );
    let metadata = graph.dimension_candidate_nfts(
        seed,
        Dimension::Metadata,
        scope,
        primary_chain,
        matrix_secondary,
        contract_nfts,
    );
    let total =
        graph.union_candidate_nfts(seed, scope, primary_chain, matrix_secondary, contract_nfts);
    ScopeNftCounts::from_sets(
        name.len(),
        token_uri.len(),
        image_uri.len(),
        metadata.len(),
        total.len(),
    )
}

fn ratio(numer: u64, denom: u64) -> Option<f64> {
    if denom == 0 {
        None
    } else {
        Some(numer as f64 / denom as f64)
    }
}

fn scale_row(category: &str, nft: u64, contract: u64, nft_denom: u64, contract_denom: u64) -> DuplicateScaleRow {
    DuplicateScaleRow {
        category: category.to_owned(),
        duplicate_nft_count: nft,
        duplicate_nft_ratio: ratio(nft, nft_denom),
        duplicate_nft_ratio_numerator: nft,
        duplicate_nft_ratio_denominator: nft_denom,
        duplicate_contract_count: contract,
        duplicate_contract_ratio: ratio(contract, contract_denom),
        duplicate_contract_ratio_numerator: contract,
        duplicate_contract_ratio_denominator: contract_denom,
    }
}

fn contracts_for_nfts(
    nfts: &AHashSet<NftId>,
    store: &ResidentStore,
) -> AHashSet<ContractId> {
    let mut out = AHashSet::new();
    for &nft in nfts {
        if let Some(rec) = store.nfts.get(nft as usize) {
            out.insert(rec.contract_id);
        }
    }
    out
}

fn dimension_sets(
    graph: &HitGraph,
    seed: ContractId,
    scope: ScopeKind,
    primary: ChainId,
    secondary: Option<ChainId>,
    contract_nfts: &AHashMap<ContractId, Vec<NftId>>,
) -> [(Dimension, AHashSet<NftId>); 4] {
    [
        (
            Dimension::TokenUri,
            graph.dimension_candidate_nfts(
                seed,
                Dimension::TokenUri,
                scope,
                primary,
                secondary,
                contract_nfts,
            ),
        ),
        (
            Dimension::ImageUri,
            graph.dimension_candidate_nfts(
                seed,
                Dimension::ImageUri,
                scope,
                primary,
                secondary,
                contract_nfts,
            ),
        ),
        (
            Dimension::Metadata,
            graph.dimension_candidate_nfts(
                seed,
                Dimension::Metadata,
                scope,
                primary,
                secondary,
                contract_nfts,
            ),
        ),
        (
            Dimension::Name,
            graph.dimension_candidate_nfts(
                seed,
                Dimension::Name,
                scope,
                primary,
                secondary,
                contract_nfts,
            ),
        ),
    ]
}

fn category_name(dim: Dimension) -> &'static str {
    match dim {
        Dimension::TokenUri => "token_uri",
        Dimension::ImageUri => "image_uri",
        Dimension::Metadata => "metadata",
        Dimension::Name => "name",
    }
}

/// Build five-category duplicate-scale rows for one seed/scope.
pub fn build_duplicate_scale_rows(
    store: &ResidentStore,
    graph: &HitGraph,
    seed: ContractId,
    scope: ScopeKind,
    primary_chain: ChainId,
    matrix_secondary: Option<ChainId>,
    contract_nfts: &AHashMap<ContractId, Vec<NftId>>,
) -> Vec<DuplicateScaleRow> {
    let totals = store
        .totals
        .get(&primary_chain)
        .cloned()
        .unwrap_or_default();
    let nft_denom = totals.nfts;
    let contract_denom = totals.contracts;

    let dims = dimension_sets(
        graph,
        seed,
        scope,
        primary_chain,
        matrix_secondary,
        contract_nfts,
    );
    let mut rows = Vec::with_capacity(5);
    let mut union_nfts = AHashSet::new();
    for (dim, nfts) in &dims {
        for &nft in nfts {
            union_nfts.insert(nft);
        }
        let contracts = contracts_for_nfts(nfts, store);
        rows.push(scale_row(
            category_name(*dim),
            nfts.len() as u64,
            contracts.len() as u64,
            nft_denom,
            contract_denom,
        ));
    }
    let union_contracts = contracts_for_nfts(&union_nfts, store);
    rows.push(scale_row(
        "total",
        union_nfts.len() as u64,
        union_contracts.len() as u64,
        nft_denom,
        contract_denom,
    ));
    rows
}

/// Build intra / matrix / cross duplicate-scale sections for one seed.
pub fn build_seed_duplicate_scale(
    store: &ResidentStore,
    graph: &HitGraph,
    seed: ContractId,
    contract_nfts: &AHashMap<ContractId, Vec<NftId>>,
) -> SeedDuplicateScale {
    let primary = store.contracts[seed as usize].chain_id;
    let intra_chain = build_duplicate_scale_rows(
        store,
        graph,
        seed,
        ScopeKind::IntraChain,
        primary,
        None,
        contract_nfts,
    );

    let mut chain_matrix = Vec::new();
    for (idx, _name) in store.chains.iter().enumerate() {
        let secondary = idx as ChainId;
        if secondary == primary {
            continue;
        }
        let rows = build_duplicate_scale_rows(
            store,
            graph,
            seed,
            ScopeKind::ChainMatrix,
            primary,
            Some(secondary),
            contract_nfts,
        );
        chain_matrix.push(ChainMatrixBlock {
            secondary_chain: store.chain_name(secondary).to_owned(),
            rows,
        });
    }
    chain_matrix.sort_by(|a, b| a.secondary_chain.cmp(&b.secondary_chain));

    let cross_chain_summary = build_duplicate_scale_rows(
        store,
        graph,
        seed,
        ScopeKind::CrossChainSummary,
        primary,
        None,
        contract_nfts,
    );

    SeedDuplicateScale {
        intra_chain,
        chain_matrix,
        cross_chain_summary,
    }
}
