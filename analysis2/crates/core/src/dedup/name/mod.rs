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
    if store.contract_nft_csr.is_empty() && !store.nfts.is_empty() {
        store.rebuild_contract_nft_csr();
    }
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
    let mut keyed: Vec<(usize, StringId)> = keys
        .into_iter()
        .map(|id| (store.string(id).chars().count(), id))
        .collect();
    keyed.sort_unstable_by(|a, b| {
        a.0.cmp(&b.0)
            .then_with(|| store.string(a.1).cmp(store.string(b.1)))
    });

    store.name_keys_by_len.clear();
    store.name_key_char_lens.clear();
    store.name_sorted_char_offsets.clear();
    store.name_sorted_chars.clear();
    store.name_keys_by_len.reserve(keyed.len());
    store.name_key_char_lens.reserve(keyed.len());
    store.name_sorted_char_offsets.reserve(keyed.len() + 1);
    store.name_sorted_char_offsets.push(0);

    let mut sorted_chars = Vec::new();
    for (char_len, id) in keyed {
        store.name_keys_by_len.push(id);
        store.name_key_char_lens.push(char_len as u32);
        sorted_chars.clear();
        sorted_chars.extend(store.string(id).chars());
        sorted_chars.sort_unstable();
        store.name_sorted_chars.extend_from_slice(&sorted_chars);
        store
            .name_sorted_char_offsets
            .push(store.name_sorted_chars.len() as u64);
    }
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

    let queries: Vec<StringId> = if seed_is_evm {
        match store.contracts[seed_usize].name_id {
            Some(name_id) => vec![name_id],
            None => Vec::new(),
        }
    } else {
        store
            .contract_nft_csr
            .values_for(seed)
            .unwrap_or(&[])
            .iter()
            .filter_map(|&nft_id| {
                let nft = &store.nfts[nft_id as usize];
                let name_id = nft.name_id?;
                let text = store.string(name_id);
                if !usable(text) {
                    return None;
                }
                Some(name_id)
            })
            .collect()
    };

    progress.begin_phase("name_query", Some(queries.len() as u64));
    let mut seen_candidates: AHashSet<(ContractId, ChainId)> = AHashSet::new();

    for &query_id in &queries {
        progress.check_cancelled()?;
        emit_for_query(
            store,
            seed,
            seed_chain,
            query_id,
            store.string(query_id),
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

    // Allocation-free length window over sorted unique names.
    let keys = &store.name_keys_by_len;
    let lengths = &store.name_key_char_lens;
    debug_assert_eq!(keys.len(), lengths.len());
    let lo = lengths.partition_point(|&cand_len| {
        let cand_len = cand_len as usize;
        cand_len < query_len && !CandidateBounds::lengths_can_reach(query_len, cand_len, threshold)
    });
    let hi = lengths.partition_point(|&cand_len| {
        let cand_len = cand_len as usize;
        cand_len <= query_len || CandidateBounds::lengths_can_reach(query_len, cand_len, threshold)
    });

    for key_index in lo..hi {
        let name_id = keys[key_index];
        if name_id == query_id {
            continue; // exact already emitted
        }
        let cand_text = store.string(name_id);
        let cand_len = lengths[key_index] as usize;
        if !CandidateBounds::lengths_can_reach(query_len, cand_len, threshold) {
            continue;
        }
        let required = CandidateBounds::minimum_multiset_overlap(query_len, cand_len, threshold);
        if required > 0 {
            let start = store.name_sorted_char_offsets[key_index] as usize;
            let end = store.name_sorted_char_offsets[key_index + 1] as usize;
            let cand_sorted = &store.name_sorted_chars[start..end];
            if !sorted_overlap_at_least(&query_sorted, cand_sorted, required) {
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

    fn prepared(evm: &[&str], rows: impl IntoIterator<Item = IdentityRow>) -> ResidentStore {
        let evm_set = evm.iter().map(|c| (*c).to_owned()).collect::<AHashSet<_>>();
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

    #[test]
    fn cached_length_window_matches_exhaustive_jaro_winkler() {
        let names = [
            "a",
            "ab",
            "abc",
            "abcd",
            "abcde",
            "abcdf",
            "xbcde",
            "abcdefghij",
            "abcdefghijk",
            "zzzzzzzzzzzzzzzz",
            "αβγδε",
        ];
        let rows = names.iter().enumerate().map(|(index, name)| {
            row(
                "ethereum",
                &format!("0x{index:02x}"),
                "1",
                name,
                index as u64,
            )
        });
        let store = prepared(&["ethereum"], rows);
        let seed = cid(&store, "ethereum", "0x04");

        assert_eq!(
            store.name_sorted_char_offsets.len(),
            store.name_keys_by_len.len() + 1
        );
        assert_eq!(store.name_key_char_lens.len(), store.name_keys_by_len.len());

        for threshold in [0.8, 0.98, 1.0] {
            let mut graph = HitGraph::new();
            query_name_for_seed(&store, seed, threshold, &mut graph, &NoopProgress).unwrap();
            let actual: AHashSet<ContractId> = graph
                .edges()
                .iter()
                .map(|edge| edge.candidate_contract)
                .collect();
            let expected: AHashSet<ContractId> = names
                .iter()
                .enumerate()
                .filter(|(index, name)| {
                    *index != 4
                        && rapidfuzz::distance::jaro_winkler::similarity(
                            "abcde".chars(),
                            name.chars(),
                        ) >= threshold
                })
                .map(|(index, _)| cid(&store, "ethereum", &format!("0x{index:02x}")))
                .collect();
            assert_eq!(actual, expected, "threshold={threshold}");
        }
    }
}
