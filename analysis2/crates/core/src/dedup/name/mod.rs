//! Name query-to-index engine (EVM representative + Solana NFT→collection).

mod bounds;
mod representative;

pub use bounds::CandidateBounds;
pub use representative::{is_usable_name, select_evm_representatives};

use ahash::AHashSet;
use rapidfuzz::distance::jaro_winkler::{Args, BatchComparator};

use crate::dedup::hits::{Dimension, HitEdge, HitGraph};
use crate::entity::{ChainId, ContractId, ResidentStore, StringId};
use crate::error::Analysis2Error;
use crate::progress::ProgressObserver;

use self::representative::is_usable_name as usable;

/// Default JW similarity threshold (fraction).
pub const DEFAULT_NAME_THRESHOLD: f64 = 0.98;

/// Build EVM representatives, Solana NFT name CSR, and length-sorted name keys.
pub fn finalize_name_index(store: &mut ResidentStore) -> Result<(), Analysis2Error> {
    for contract in &mut store.contracts {
        contract.name_id = None;
    }

    let reps = select_evm_representatives(store);
    for &(contract_id, name_id) in &reps {
        store.contracts[contract_id as usize].name_id = Some(name_id);
    }

    let mut contract_pairs: Vec<(u32, u32)> = reps
        .iter()
        .map(|&(contract_id, name_id)| (name_id, contract_id))
        .collect();
    contract_pairs.sort_unstable();
    store.name_contract_csr = crate::entity::CsrIndex::from_sorted_pairs(&contract_pairs);

    let mut nft_pairs: Vec<(u32, u32)> = Vec::new();
    for nft in &store.nfts {
        let Some(name_id) = nft.name_id else {
            continue;
        };
        let contract = &store.contracts[nft.contract_id as usize];
        let chain = store.chain_name(contract.chain_id);
        if store.is_evm_chain(chain) {
            continue;
        }
        if !usable(store.string(name_id)) {
            continue;
        }
        nft_pairs.push((name_id, nft.id));
    }
    nft_pairs.sort_unstable();
    store.name_nft_csr = crate::entity::CsrIndex::from_sorted_pairs(&nft_pairs);

    // Unique name keys from both CSRs, sorted by char length then text for windows.
    let mut keys: AHashSet<StringId> = AHashSet::new();
    for &key in &store.name_contract_csr.keys {
        keys.insert(key);
    }
    for &key in &store.name_nft_csr.keys {
        keys.insert(key);
    }
    let mut keyed: Vec<(usize, String, StringId)> = keys
        .into_iter()
        .map(|id| {
            let text = store.string(id).to_owned();
            (text.chars().count(), text, id)
        })
        .collect();
    keyed.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    store.name_keys_by_len = keyed.into_iter().map(|(_, _, id)| id).collect();
    Ok(())
}

/// Query Name for `seed` against the finalized index; emit whole-collection edges.
///
/// - EVM seed: one representative string query.
/// - Solana seed: each NFT name; any hit marks the whole seed→candidate collection
///   (`candidate_nft: None` so HitGraph expands all candidate NFTs).
pub fn query_name_for_seed(
    store: &ResidentStore,
    seed: ContractId,
    threshold: f64,
    graph: &mut HitGraph,
    progress: &dyn ProgressObserver,
) -> Result<(), Analysis2Error> {
    progress.set_stage("name");
    progress.check_cancelled()?;

    let seed_usize = seed as usize;
    if seed_usize >= store.contracts.len() {
        return Err(Analysis2Error::invalid(format!(
            "unknown seed contract id {seed}"
        )));
    }
    let seed_chain = store.contracts[seed_usize].chain_id;
    let seed_chain_name = store.chain_name(seed_chain);
    let seed_is_evm = store.is_evm_chain(seed_chain_name);

    let queries: Vec<(StringId, String)> = if seed_is_evm {
        match store.contracts[seed_usize].name_id {
            Some(name_id) => vec![(name_id, store.string(name_id).to_owned())],
            None => Vec::new(),
        }
    } else {
        store
            .nfts
            .iter()
            .filter(|nft| nft.contract_id == seed)
            .filter_map(|nft| {
                let name_id = nft.name_id?;
                let text = store.string(name_id);
                if !usable(text) {
                    return None;
                }
                Some((name_id, text.to_owned()))
            })
            .collect()
    };

    progress.begin_phase("name_query", Some(queries.len() as u64));
    let mut seen_candidates: AHashSet<(ContractId, ChainId)> = AHashSet::new();

    for (query_id, query_text) in &queries {
        progress.check_cancelled()?;
        emit_for_query(
            store,
            seed,
            seed_chain,
            *query_id,
            query_text,
            threshold,
            graph,
            &mut seen_candidates,
        );
        progress.add_completed(1);
    }
    Ok(())
}

fn emit_for_query(
    store: &ResidentStore,
    seed: ContractId,
    seed_chain: ChainId,
    query_id: StringId,
    query_text: &str,
    threshold: f64,
    graph: &mut HitGraph,
    seen: &mut AHashSet<(ContractId, ChainId)>,
) {
    let query_len = query_text.chars().count();
    let query_chars: Vec<char> = query_text.chars().collect();
    let mut query_sorted = query_chars.clone();
    query_sorted.sort_unstable();

    // Byte-equal short circuit via CSR exact key.
    if let Some(contracts) = store.name_contract_csr.values_for(query_id) {
        for &cand in contracts {
            push_collection_edge(store, seed, seed_chain, cand, 1.0, graph, seen);
        }
    }
    if let Some(nfts) = store.name_nft_csr.values_for(query_id) {
        for &nft_id in nfts {
            let cand = store.nfts[nft_id as usize].contract_id;
            push_collection_edge(store, seed, seed_chain, cand, 1.0, graph, seen);
        }
    }

    if threshold > 1.0 || threshold.is_nan() {
        return;
    }
    if store.name_keys_by_len.is_empty() {
        return;
    }

    let args = Args::default().score_cutoff(threshold.clamp(0.0, 1.0));
    let prepared = BatchComparator::new(query_chars.iter().copied());

    // Length window over sorted unique names.
    let keys = &store.name_keys_by_len;
    let mut lo = 0usize;
    while lo < keys.len() {
        let cand_len = store.string(keys[lo]).chars().count();
        if CandidateBounds::lengths_can_reach(query_len, cand_len, threshold) {
            break;
        }
        lo += 1;
    }
    let mut hi = lo;
    while hi < keys.len() {
        let cand_len = store.string(keys[hi]).chars().count();
        if !CandidateBounds::lengths_can_reach(query_len, cand_len, threshold) {
            break;
        }
        hi += 1;
    }

    for &name_id in &keys[lo..hi] {
        if name_id == query_id {
            continue; // exact already emitted
        }
        let cand_text = store.string(name_id);
        let cand_len = cand_text.chars().count();
        if !CandidateBounds::lengths_can_reach(query_len, cand_len, threshold) {
            continue;
        }
        let required =
            CandidateBounds::minimum_multiset_overlap(query_len, cand_len, threshold);
        if required > 0 {
            let mut cand_sorted: Vec<char> = cand_text.chars().collect();
            cand_sorted.sort_unstable();
            if !sorted_overlap_at_least(&query_sorted, &cand_sorted, required) {
                continue;
            }
        }
        let Some(score) = prepared.similarity_with_args(cand_text.chars(), &args) else {
            continue;
        };
        if let Some(contracts) = store.name_contract_csr.values_for(name_id) {
            for &cand in contracts {
                push_collection_edge(store, seed, seed_chain, cand, score, graph, seen);
            }
        }
        if let Some(nfts) = store.name_nft_csr.values_for(name_id) {
            for &nft_id in nfts {
                let cand = store.nfts[nft_id as usize].contract_id;
                push_collection_edge(store, seed, seed_chain, cand, score, graph, seen);
            }
        }
    }
}

fn push_collection_edge(
    store: &ResidentStore,
    seed: ContractId,
    seed_chain: ChainId,
    candidate: ContractId,
    score: f64,
    graph: &mut HitGraph,
    seen: &mut AHashSet<(ContractId, ChainId)>,
) {
    if candidate == seed {
        return;
    }
    let secondary = store.contracts[candidate as usize].chain_id;
    if !seen.insert((candidate, secondary)) {
        return;
    }
    graph.push(HitEdge {
        seed_contract: seed,
        candidate_contract: candidate,
        candidate_nft: None, // whole-collection Name hit
        dimension: Dimension::Name,
        score,
        primary_chain: seed_chain,
        secondary_chain: secondary,
    });
}

fn sorted_overlap_at_least(left: &[char], right: &[char], required: usize) -> bool {
    if required == 0 {
        return true;
    }
    let mut i = 0usize;
    let mut j = 0usize;
    let mut overlap = 0usize;
    while i < left.len() && j < right.len() {
        if overlap + (left.len() - i).min(right.len() - j) < required {
            return false;
        }
        match left[i].cmp(&right[j]) {
            std::cmp::Ordering::Equal => {
                overlap += 1;
                if overlap >= required {
                    return true;
                }
                i += 1;
                j += 1;
            }
            std::cmp::Ordering::Less => {
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                j += 1;
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dedup::hits::ScopeKind;
    use crate::entity::{IdentityRow, NftId, SourceOrder};
    use crate::progress::NoopProgress;
    use crate::reporting::count_scope_nfts;
    use ahash::{AHashMap, AHashSet};

    fn row(chain: &str, contract: &str, token: &str, name: &str, n: u64) -> IdentityRow {
        IdentityRow {
            chain: chain.to_owned(),
            contract_address: contract.to_owned(),
            token_id: token.to_owned(),
            name_norm: name.to_owned(),
            token_uri_norm: String::new(),
            image_uri_norm: String::new(),
            source_order: SourceOrder {
                file_ordinal: 0,
                file_row_number: n,
            },
        }
    }

    fn prepared(
        evm: &[&str],
        rows: impl IntoIterator<Item = IdentityRow>,
    ) -> ResidentStore {
        let evm_set = evm
            .iter()
            .map(|c| (*c).to_owned())
            .collect::<AHashSet<_>>();
        let mut store = ResidentStore::with_options(8, &evm_set);
        for r in rows {
            store.ingest_identity_row(r).unwrap();
        }
        finalize_name_index(&mut store).unwrap();
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
    fn solana_one_nft_hit_marks_whole_candidate_collection() {
        // Seed collection A has one NFT name matching one NFT in collection B;
        // B has 3 NFTs total → Name count expands to all 3.
        let store = prepared(
            &["ethereum"],
            [
                row("solana", "col-a", "mint-1", "SharedName", 1),
                row("solana", "col-a", "mint-2", "OtherUnique", 2),
                row("solana", "col-b", "mint-x", "SharedName", 3),
                row("solana", "col-b", "mint-y", "NoiseOne", 4),
                row("solana", "col-b", "mint-z", "NoiseTwo", 5),
            ],
        );
        let seed = cid(&store, "solana", "col-a");
        let cand = cid(&store, "solana", "col-b");
        let mut graph = HitGraph::new();
        query_name_for_seed(
            &store,
            seed,
            DEFAULT_NAME_THRESHOLD,
            &mut graph,
            &NoopProgress,
        )
        .unwrap();

        let edge = graph
            .edges()
            .iter()
            .find(|e| e.candidate_contract == cand)
            .expect("candidate collection edge");
        assert_eq!(edge.candidate_nft, None);
        assert_eq!(edge.dimension, Dimension::Name);
        assert_eq!(edge.score, 1.0);

        let sol = store.chain_ids["solana"];
        let counts = count_scope_nfts(
            &graph,
            seed,
            ScopeKind::IntraChain,
            sol,
            None,
            &nft_map(&store),
        );
        assert_eq!(counts.name, 3, "whole candidate collection NFT count");
    }

    #[test]
    fn solana_jw_near_match_also_expands_collection() {
        // "CoolCatzzz" vs "CoolCatzzz!" — high JW; one NFT hit → all candidate NFTs.
        let store = prepared(
            &["ethereum"],
            [
                row("solana", "col-a", "m1", "CoolCatCollection", 1),
                row("solana", "col-b", "m1", "CoolCatCollectiom", 2), // last char differs
                row("solana", "col-b", "m2", "UnrelatedNameXYZ", 3),
            ],
        );
        let seed = cid(&store, "solana", "col-a");
        let cand = cid(&store, "solana", "col-b");
        let mut graph = HitGraph::new();
        query_name_for_seed(&store, seed, 0.90, &mut graph, &NoopProgress).unwrap();

        assert!(
            graph
                .edges()
                .iter()
                .any(|e| e.candidate_contract == cand && e.candidate_nft.is_none()),
            "expected whole-collection Name edge"
        );
        let sol = store.chain_ids["solana"];
        let counts = count_scope_nfts(
            &graph,
            seed,
            ScopeKind::IntraChain,
            sol,
            None,
            &nft_map(&store),
        );
        assert_eq!(counts.name, 2);
    }

    #[test]
    fn evm_representative_exact_cross_chain() {
        let store = prepared(
            &["ethereum", "base"],
            [
                row("ethereum", "0xa", "1", "Alpha", 1),
                row("ethereum", "0xa", "2", "Alpha", 2),
                row("base", "0xb", "1", "Alpha", 3),
                row("base", "0xb", "2", "Alpha", 4),
            ],
        );
        let seed = cid(&store, "ethereum", "0xa");
        let cand = cid(&store, "base", "0xb");
        assert_eq!(
            store.string(store.contracts[seed as usize].name_id.unwrap()),
            "Alpha"
        );
        let mut graph = HitGraph::new();
        query_name_for_seed(
            &store,
            seed,
            DEFAULT_NAME_THRESHOLD,
            &mut graph,
            &NoopProgress,
        )
        .unwrap();
        let eth = store.chain_ids["ethereum"];
        let base = store.chain_ids["base"];
        let counts = count_scope_nfts(
            &graph,
            seed,
            ScopeKind::ChainMatrix,
            eth,
            Some(base),
            &nft_map(&store),
        );
        assert_eq!(counts.name, 2);
        assert!(graph.edges().iter().any(|e| {
            e.candidate_contract == cand && e.candidate_nft.is_none() && e.score == 1.0
        }));
    }

    #[test]
    fn self_hit_excluded() {
        let store = prepared(
            &["ethereum"],
            [
                row("ethereum", "0xa", "1", "Solo", 1),
                row("ethereum", "0xa", "2", "Solo", 2),
            ],
        );
        let seed = cid(&store, "ethereum", "0xa");
        let mut graph = HitGraph::new();
        query_name_for_seed(
            &store,
            seed,
            DEFAULT_NAME_THRESHOLD,
            &mut graph,
            &NoopProgress,
        )
        .unwrap();
        assert!(graph.is_empty());
    }
}
