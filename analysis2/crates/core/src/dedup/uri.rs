//! Seed-scoped URI exact-match query against ResidentStore CSR postings.

use ahash::{AHashMap, AHashSet};

use crate::dedup::hits::{Dimension, HitEdge, HitGraph};
use crate::entity::{ChainId, ContractId, NftId, ResidentStore};
use crate::error::Analysis2Error;
use crate::progress::ProgressObserver;

/// Scope keys used for token→image fallback per seed NFT.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum UriScope {
    Intra,
    /// Cross-chain summary / matrix peer chain.
    Cross(ChainId),
}

/// Query token_uri then image_uri for `seed`, posting HitEdges into `graph`.
///
/// Intra-chain requires ≥2 distinct contracts sharing the URI on the seed chain.
/// Cross-chain requires the URI on both primary and secondary chains.
/// Seed self-hits are excluded by [`HitGraph::push`].
///
/// For each seed NFT / scope, `image_uri` is tried only when `token_uri` missed.
/// Image eligibility also excludes NFTs already hit via `token_uri` in that scope.
pub fn query_uri_for_seed(
    store: &ResidentStore,
    seed: ContractId,
    graph: &mut HitGraph,
    progress: &dyn ProgressObserver,
) -> Result<(), Analysis2Error> {
    progress.set_stage("uri");
    progress.check_cancelled()?;

    let seed_usize = seed as usize;
    if seed_usize >= store.contracts.len() {
        return Err(Analysis2Error::invalid(format!(
            "unknown seed contract id {seed}"
        )));
    }
    let seed_chain = store.contracts[seed_usize].chain_id;

    let seed_nfts = store.contract_nft_csr.values_for(seed).unwrap_or(&[]);

    let mut token_hit_scopes: AHashMap<NftId, AHashSet<UriScope>> = AHashMap::new();
    let mut token_hit_candidates: AHashMap<UriScope, AHashSet<NftId>> = AHashMap::new();

    let mut token_queries: AHashMap<crate::entity::StringId, Vec<NftId>> = AHashMap::new();
    let mut token_missing = 0_u64;
    for &seed_nft in seed_nfts {
        match store.nfts[seed_nft as usize].token_uri_id {
            Some(uri_id) => token_queries.entry(uri_id).or_default().push(seed_nft),
            None => token_missing += 1,
        }
    }

    let mut token_queries: Vec<_> = token_queries.into_iter().collect();
    token_queries.sort_unstable_by_key(|(uri_id, _)| *uri_id);
    progress.begin_phase("token_uri", Some(seed_nfts.len() as u64));
    for (uri_id, source_nfts) in token_queries {
        progress.check_cancelled()?;
        if let Some(members) = store.token_uri_csr.values_for(uri_id) {
            let by_chain = group_members_by_chain(store, members);
            emit_uri_hits(
                store,
                seed,
                seed_chain,
                &source_nfts,
                Dimension::TokenUri,
                &by_chain,
                /*skip_scopes=*/ None,
                /*exclude_candidates=*/ None,
                graph,
                Some(&mut token_hit_scopes),
                Some(&mut token_hit_candidates),
            );
        }
        progress.add_completed(source_nfts.len() as u64);
    }
    progress.add_completed(token_missing);

    progress.begin_phase("image_uri", Some(seed_nfts.len() as u64));
    for &seed_nft in seed_nfts {
        progress.check_cancelled()?;
        if let Some(uri_id) = store.nfts[seed_nft as usize].image_uri_id {
            if let Some(members) = store.image_uri_csr.values_for(uri_id) {
                let by_chain = group_members_by_chain(store, members);
                let skip = token_hit_scopes.get(&seed_nft);
                emit_uri_hits(
                    store,
                    seed,
                    seed_chain,
                    std::slice::from_ref(&seed_nft),
                    Dimension::ImageUri,
                    &by_chain,
                    skip,
                    Some(&token_hit_candidates),
                    graph,
                    None,
                    None,
                );
            }
        }
        progress.add_completed(1);
    }

    Ok(())
}

fn group_members_by_chain(store: &ResidentStore, members: &[NftId]) -> Vec<Vec<NftId>> {
    let mut by_chain = vec![Vec::new(); store.chains.len()];
    for &nft_id in members {
        let contract_id = store.nfts[nft_id as usize].contract_id;
        let chain_id = store.contracts[contract_id as usize].chain_id;
        by_chain[chain_id as usize].push(nft_id);
    }
    by_chain
}

fn emit_uri_hits(
    store: &ResidentStore,
    seed: ContractId,
    seed_chain: ChainId,
    seed_nfts: &[NftId],
    dimension: Dimension,
    by_chain: &[Vec<NftId>],
    skip_scopes: Option<&AHashSet<UriScope>>,
    exclude_candidates: Option<&AHashMap<UriScope, AHashSet<NftId>>>,
    graph: &mut HitGraph,
    mut hit_scopes_out: Option<&mut AHashMap<NftId, AHashSet<UriScope>>>,
    mut hit_candidates_out: Option<&mut AHashMap<UriScope, AHashSet<NftId>>>,
) {
    let skip = |scope: UriScope| skip_scopes.is_some_and(|s| s.contains(&scope));
    let excluded = |scope: UriScope| exclude_candidates.and_then(|m| m.get(&scope));

    // Intra-chain: URI on ≥2 distinct contracts (after optional token-hit exclusion).
    if !skip(UriScope::Intra) {
        if let Some(primary_members) = by_chain.get(seed_chain as usize) {
            let excl = excluded(UriScope::Intra);
            if has_seed_and_other_contract(store, primary_members, excl, seed) {
                let mut any = false;
                for &nft_id in primary_members {
                    if excl.is_some_and(|s| s.contains(&nft_id)) {
                        continue;
                    }
                    let cand = store.nfts[nft_id as usize].contract_id;
                    if cand == seed {
                        continue;
                    }
                    if graph.push(HitEdge {
                        seed_contract: seed,
                        candidate_contract: cand,
                        candidate_nft: Some(nft_id),
                        dimension,
                        score: 1.0,
                        primary_chain: seed_chain,
                        secondary_chain: seed_chain,
                    }) {
                        if let Some(out) = hit_candidates_out.as_deref_mut() {
                            out.entry(UriScope::Intra).or_default().insert(nft_id);
                        }
                        any = true;
                    }
                }
                if any {
                    if let Some(out) = hit_scopes_out.as_deref_mut() {
                        for &seed_nft in seed_nfts {
                            out.entry(seed_nft).or_default().insert(UriScope::Intra);
                        }
                    }
                }
            }
        }
    }

    // Cross-chain: URI must appear on seed chain and peer chain.
    if by_chain.get(seed_chain as usize).is_none_or(Vec::is_empty) {
        return;
    }

    for (peer_index, peer_members) in by_chain.iter().enumerate() {
        let peer = peer_index as ChainId;
        if peer == seed_chain || peer_members.is_empty() {
            continue;
        }
        let scope = UriScope::Cross(peer);
        if skip(scope) {
            continue;
        }
        // Peer side must still have at least one non-excluded NFT.
        let excl = excluded(scope);
        let peer_alive = peer_members
            .iter()
            .any(|nft_id| !excl.is_some_and(|s| s.contains(nft_id)));
        if !peer_alive {
            continue;
        }

        // Seed chain must also retain a participating posting after exclusion
        // (seed NFT itself always participates when we reach here from its URI).
        let Some(primary_members) = by_chain.get(seed_chain as usize) else {
            continue;
        };
        let primary_alive = primary_members
            .iter()
            .any(|&nft_id| !excl.is_some_and(|s| s.contains(&nft_id)));
        if !primary_alive {
            continue;
        }

        let mut any = false;
        for &nft_id in peer_members {
            if excl.is_some_and(|s| s.contains(&nft_id)) {
                continue;
            }
            let cand = store.nfts[nft_id as usize].contract_id;
            if cand == seed {
                continue;
            }
            if graph.push(HitEdge {
                seed_contract: seed,
                candidate_contract: cand,
                candidate_nft: Some(nft_id),
                dimension,
                score: 1.0,
                primary_chain: seed_chain,
                secondary_chain: peer,
            }) {
                if let Some(out) = hit_candidates_out.as_deref_mut() {
                    out.entry(scope).or_default().insert(nft_id);
                }
                any = true;
            }
        }
        if any {
            if let Some(out) = hit_scopes_out.as_deref_mut() {
                for &seed_nft in seed_nfts {
                    out.entry(seed_nft).or_default().insert(scope);
                }
            }
        }
    }
}

fn has_seed_and_other_contract(
    store: &ResidentStore,
    members: &[NftId],
    exclude_nfts: Option<&AHashSet<NftId>>,
    seed: ContractId,
) -> bool {
    let mut has_seed = false;
    let mut has_other = false;
    for &nft_id in members {
        if exclude_nfts.is_some_and(|s| s.contains(&nft_id)) {
            continue;
        }
        if store.nfts[nft_id as usize].contract_id == seed {
            has_seed = true;
        } else {
            has_other = true;
        }
        if has_seed && has_other {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dedup::hits::ScopeKind;
    use crate::entity::{IdentityRow, SourceOrder};
    use crate::progress::NoopProgress;
    use crate::reporting::count_scope_nfts;

    fn row(
        chain: &str,
        contract: &str,
        token: &str,
        token_uri: &str,
        image_uri: &str,
        row_number: u64,
    ) -> IdentityRow {
        IdentityRow {
            chain: chain.to_owned(),
            contract_address: contract.to_owned(),
            token_id: token.to_owned(),
            name_norm: String::new(),
            token_uri_norm: token_uri.to_owned(),
            image_uri_norm: image_uri.to_owned(),
            source_order: SourceOrder {
                file_ordinal: 0,
                file_row_number: row_number,
            },
        }
    }

    fn prepared(rows: impl IntoIterator<Item = IdentityRow>) -> ResidentStore {
        let mut store = ResidentStore::new();
        for r in rows {
            store.ingest_identity_row(r).unwrap();
        }
        store.rebuild_uri_csr();
        store
    }

    fn cid(store: &ResidentStore, chain: &str, address: &str) -> ContractId {
        let chain_id = store.chain_ids[chain];
        store.contract_index[&(chain_id, address.to_owned())]
    }

    fn nft_map(store: &ResidentStore) -> AHashMap<ContractId, Vec<NftId>> {
        let mut map: AHashMap<ContractId, Vec<NftId>> = AHashMap::new();
        for nft in &store.nfts {
            map.entry(nft.contract_id).or_default().push(nft.id);
        }
        map
    }

    #[test]
    fn intra_requires_two_contracts() {
        let store = prepared([
            row("ethereum", "a", "1", "ipfs://x", "", 1),
            row("ethereum", "b", "1", "ipfs://x", "", 2),
        ]);
        let seed = cid(&store, "ethereum", "a");
        let mut graph = HitGraph::new();
        query_uri_for_seed(&store, seed, &mut graph, &NoopProgress).unwrap();
        let eth = store.chain_ids["ethereum"];
        let counts = count_scope_nfts(
            &graph,
            seed,
            ScopeKind::IntraChain,
            eth,
            None,
            &nft_map(&store),
        );
        assert_eq!(counts.token_uri, 1);
        assert!(!graph.edges().iter().any(|e| e.candidate_contract == seed));
    }

    #[test]
    fn empty_uri_skipped() {
        let store = prepared([
            row("ethereum", "a", "1", "", "", 1),
            row("ethereum", "b", "1", "", "", 2),
        ]);
        let seed = cid(&store, "ethereum", "a");
        let mut graph = HitGraph::new();
        query_uri_for_seed(&store, seed, &mut graph, &NoopProgress).unwrap();
        assert!(graph.is_empty());
    }

    #[test]
    fn repeated_seed_uri_is_queried_once_and_marks_all_fallback_scopes() {
        let store = prepared([
            row("ethereum", "a", "1", "ipfs://token", "ipfs://image", 1),
            row("ethereum", "a", "2", "ipfs://token", "ipfs://image", 2),
            row("ethereum", "b", "1", "ipfs://token", "ipfs://image", 3),
        ]);
        let seed = cid(&store, "ethereum", "a");
        let mut graph = HitGraph::new();
        query_uri_for_seed(&store, seed, &mut graph, &NoopProgress).unwrap();

        assert_eq!(
            graph
                .edges()
                .iter()
                .filter(|edge| edge.dimension == Dimension::TokenUri)
                .count(),
            1,
            "identical seed-side URI queries must not duplicate candidate edges"
        );
        assert!(
            graph
                .edges()
                .iter()
                .all(|edge| edge.dimension != Dimension::ImageUri),
            "token hit must suppress image fallback for every seed NFT sharing the URI"
        );
    }
}
