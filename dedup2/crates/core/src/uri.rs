use crate::entity::{Dimension, EntityStore, UriPosting};
use crate::error::DedupError;
use crate::progress::ProgressObserver;
use crate::stats::SummaryAccumulator;
use ahash::AHashMap;

pub fn run_uri(
    store: &EntityStore,
    acc: &mut SummaryAccumulator,
    progress: &dyn ProgressObserver,
) -> Result<(), DedupError> {
    progress.set_stage("uri");
    progress.set_phase("token_uri");
    accumulate_uri_dimension(store, acc, &store.token_uri_postings, Dimension::TokenUri, progress)?;
    progress.set_phase("image_uri");
    // image_uri only when token_uri did not already mark the same NFT rows for
    // that contract+uri group — approximate by running image groups independently
    // and letting contract-once semantics handle overlap at contract level.
    // Spec: image metric when token URI did not match. Track token-matched NFT
    // keys per contract for exclusion.
    let token_matched = token_matched_contracts(store, acc);
    accumulate_image_uri(store, acc, &token_matched, progress)?;
    Ok(())
}

fn token_matched_contracts(
    store: &EntityStore,
    _acc: &SummaryAccumulator,
) -> ahash::AHashSet<crate::entity::ContractId> {
    // Recompute which contracts have any token_uri duplicate in any scope by
    // re-deriving from postings (same logic as accumulate).
    let mut matched = ahash::AHashSet::new();
    let groups = group_postings(&store.token_uri_postings);
    for members in groups.values() {
        if is_duplicate_group(members) {
            for member in members {
                matched.insert(member.contract_id);
            }
        }
    }
    matched
}

fn accumulate_uri_dimension(
    store: &EntityStore,
    acc: &mut SummaryAccumulator,
    postings: &[UriPosting],
    dimension: Dimension,
    progress: &dyn ProgressObserver,
) -> Result<(), DedupError> {
    let groups = group_postings(postings);
    progress.set_total(Some(groups.len() as u64));
    for members in groups.values() {
        progress.check_cancelled()?;
        if is_duplicate_group(members) {
            emit_group(store, acc, members, dimension);
        }
        progress.add_completed(1);
    }
    Ok(())
}

fn accumulate_image_uri(
    store: &EntityStore,
    acc: &mut SummaryAccumulator,
    token_matched: &ahash::AHashSet<crate::entity::ContractId>,
    progress: &dyn ProgressObserver,
) -> Result<(), DedupError> {
    let groups = group_postings(&store.image_uri_postings);
    progress.set_total(Some(groups.len() as u64));
    for members in groups.values() {
        progress.check_cancelled()?;
        if !is_duplicate_group(members) {
            progress.add_completed(1);
            continue;
        }
        // Only count image hits for contracts not already token-uri duplicated
        // within this image group's peer set — simplified: skip contracts that
        // had any token_uri duplicate.
        let filtered: Vec<&UriPosting> = members
            .iter()
            .copied()
            .filter(|m| !token_matched.contains(&m.contract_id))
            .collect();
        if is_duplicate_group(&filtered) {
            emit_group(store, acc, &filtered, Dimension::ImageUri);
        }
        progress.add_completed(1);
    }
    Ok(())
}

fn group_postings(postings: &[UriPosting]) -> AHashMap<&str, Vec<&UriPosting>> {
    let mut groups: AHashMap<&str, Vec<&UriPosting>> = AHashMap::new();
    for posting in postings {
        groups.entry(posting.uri.as_str()).or_default().push(posting);
    }
    groups
}

fn is_duplicate_group(members: &[&UriPosting]) -> bool {
    if members.len() < 2 {
        return false;
    }
    let mut chains = ahash::AHashSet::new();
    let mut contracts_per_chain: AHashMap<crate::entity::ChainId, ahash::AHashSet<_>> =
        AHashMap::new();
    for member in members {
        chains.insert(member.chain_id);
        contracts_per_chain
            .entry(member.chain_id)
            .or_default()
            .insert(member.contract_id);
    }
    if chains.len() > 1 {
        return true;
    }
    contracts_per_chain.values().any(|set| set.len() >= 2)
}

fn emit_group(
    store: &EntityStore,
    acc: &mut SummaryAccumulator,
    members: &[&UriPosting],
    dimension: Dimension,
) {
    // For each member, peer chains are every other chain present; for intra,
    // peer is same chain when ≥2 contracts on that chain.
    let mut by_chain: AHashMap<crate::entity::ChainId, Vec<&UriPosting>> = AHashMap::new();
    for member in members {
        by_chain.entry(member.chain_id).or_default().push(*member);
    }
    let chains: Vec<_> = by_chain.keys().copied().collect();
    for (&chain, chain_members) in &by_chain {
        let intra = chain_members
            .iter()
            .map(|m| m.contract_id)
            .collect::<ahash::AHashSet<_>>()
            .len()
            >= 2;
        if intra {
            for member in chain_members {
                acc.mark_uri_hit(store, member.contract_id, member.nft_count, dimension, chain);
            }
        }
        for &other in &chains {
            if other == chain {
                continue;
            }
            for member in chain_members {
                acc.mark_uri_hit(store, member.contract_id, member.nft_count, dimension, other);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::{InputRow, SourceOrder};
    use crate::progress::NoopProgress;

    fn row(chain: &str, contract: &str, token: &str, token_uri: &str) -> InputRow {
        InputRow {
            chain: chain.to_owned(),
            contract_address: contract.to_owned(),
            token_id: token.to_owned(),
            name_norm: String::new(),
            token_uri_norm: token_uri.to_owned(),
            image_uri_norm: String::new(),
            metadata_json: String::new(),
            source_order: SourceOrder {
                file_ordinal: 0,
                file_row_number: 0,
            },
        }
    }

    #[test]
    fn intra_chain_token_uri_counts_two_contracts() {
        let mut store = EntityStore::default();
        store.ingest_row(row("ethereum", "a", "1", "ipfs://x"));
        store.ingest_row(row("ethereum", "b", "1", "ipfs://x"));
        let mut acc = SummaryAccumulator::default();
        run_uri(&store, &mut acc, &NoopProgress).unwrap();
        let eth = *store.chain_ids.get("ethereum").unwrap();
        let key = crate::scope::ScopeKey {
            kind: crate::entity::ScopeKind::IntraChain,
            primary_chain: eth,
            secondary_chain: None,
            dimension: Dimension::TokenUri,
        };
        let counts = acc.counts().get(&key).unwrap();
        assert_eq!(counts.duplicate_contract_count, 2);
        assert_eq!(counts.duplicate_nft_count, 2);
    }
}
