use crate::entity::{ContractId, Dimension, EntityStore, UriPosting};
use crate::error::DedupError;
use crate::progress::ProgressObserver;
use crate::stats::SummaryAccumulator;
use ahash::{AHashMap, AHashSet};

pub fn run_uri(
    store: &EntityStore,
    acc: &mut SummaryAccumulator,
    progress: &dyn ProgressObserver,
) -> Result<(), DedupError> {
    progress.set_stage("uri");
    progress.set_phase("token_uri");
    let token_matched_nfts = accumulate_token_uri(store, acc, progress)?;
    progress.set_phase("image_uri");
    accumulate_image_uri(store, acc, &token_matched_nfts, progress)?;
    Ok(())
}

fn accumulate_token_uri(
    store: &EntityStore,
    acc: &mut SummaryAccumulator,
    progress: &dyn ProgressObserver,
) -> Result<AHashSet<(ContractId, String)>, DedupError> {
    let groups = group_postings(&store.token_uri_postings);
    progress.set_total(Some(groups.len() as u64));
    let mut matched_nfts = AHashSet::new();
    for members in groups.values() {
        progress.check_cancelled()?;
        if is_duplicate_group(members) {
            for member in members {
                for token_id in &member.token_ids {
                    matched_nfts.insert((member.contract_id, token_id.clone()));
                }
            }
            emit_group(store, acc, members, Dimension::TokenUri, None);
        }
        progress.add_completed(1);
    }
    Ok(matched_nfts)
}

fn accumulate_image_uri(
    store: &EntityStore,
    acc: &mut SummaryAccumulator,
    token_matched_nfts: &AHashSet<(ContractId, String)>,
    progress: &dyn ProgressObserver,
) -> Result<(), DedupError> {
    let groups = group_postings(&store.image_uri_postings);
    progress.set_total(Some(groups.len() as u64));
    for members in groups.values() {
        progress.check_cancelled()?;
        // NFT-level AND-NOT: drop tokens already counted via token_uri.
        let filtered: Vec<UriPosting> = members
            .iter()
            .filter_map(|member| {
                let token_ids: Vec<String> = member
                    .token_ids
                    .iter()
                    .filter(|tid| !token_matched_nfts.contains(&(member.contract_id, (*tid).clone())))
                    .cloned()
                    .collect();
                if token_ids.is_empty() {
                    return None;
                }
                Some(UriPosting {
                    contract_id: member.contract_id,
                    chain_id: member.chain_id,
                    uri: member.uri.clone(),
                    token_ids,
                })
            })
            .collect();
        let refs: Vec<&UriPosting> = filtered.iter().collect();
        if is_duplicate_group(&refs) {
            emit_group(store, acc, &refs, Dimension::ImageUri, None);
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
    let mut chains = AHashSet::new();
    let mut contracts_per_chain: AHashMap<crate::entity::ChainId, AHashSet<_>> = AHashMap::new();
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
    _unused: Option<()>,
) {
    let mut by_chain: AHashMap<crate::entity::ChainId, Vec<&UriPosting>> = AHashMap::new();
    for member in members {
        by_chain.entry(member.chain_id).or_default().push(*member);
    }
    let chains: Vec<_> = by_chain.keys().copied().collect();
    for (&chain, chain_members) in &by_chain {
        let intra = chain_members
            .iter()
            .map(|m| m.contract_id)
            .collect::<AHashSet<_>>()
            .len()
            >= 2;
        if intra {
            for member in chain_members {
                acc.mark_uri_hit(
                    store,
                    member.contract_id,
                    &member.uri,
                    member.nft_count(),
                    dimension,
                    chain,
                );
            }
        }
        for &other in &chains {
            if other == chain {
                continue;
            }
            for member in chain_members {
                acc.mark_uri_hit(
                    store,
                    member.contract_id,
                    &member.uri,
                    member.nft_count(),
                    dimension,
                    other,
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::{InputRow, SourceOrder};
    use crate::progress::NoopProgress;

    fn row(
        chain: &str,
        contract: &str,
        token: &str,
        token_uri: &str,
        image_uri: &str,
    ) -> InputRow {
        InputRow {
            chain: chain.to_owned(),
            contract_address: contract.to_owned(),
            token_id: token.to_owned(),
            name_norm: String::new(),
            token_uri_norm: token_uri.to_owned(),
            image_uri_norm: image_uri.to_owned(),
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
        store.ingest_row(row("ethereum", "a", "1", "ipfs://x", ""));
        store.ingest_row(row("ethereum", "b", "1", "ipfs://x", ""));
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

    #[test]
    fn cross_summary_nft_not_double_counted_across_peers() {
        let mut store = EntityStore::default();
        store.ingest_row(row("ethereum", "a", "1", "ipfs://x", ""));
        store.ingest_row(row("base", "b", "1", "ipfs://x", ""));
        store.ingest_row(row("polygon", "c", "1", "ipfs://x", ""));
        let mut acc = SummaryAccumulator::default();
        run_uri(&store, &mut acc, &NoopProgress).unwrap();
        let eth = *store.chain_ids.get("ethereum").unwrap();
        let key = crate::scope::ScopeKey {
            kind: crate::entity::ScopeKind::CrossChainSummary,
            primary_chain: eth,
            secondary_chain: None,
            dimension: Dimension::TokenUri,
        };
        assert_eq!(acc.counts().get(&key).unwrap().duplicate_nft_count, 1);
    }

    #[test]
    fn image_excluded_only_for_token_matched_nfts() {
        let mut store = EntityStore::default();
        // token duplicate on token 1; token 2 only shares image
        store.ingest_row(row("ethereum", "a", "1", "ipfs://tok", "ipfs://img"));
        store.ingest_row(row("ethereum", "b", "1", "ipfs://tok", "ipfs://other"));
        store.ingest_row(row("ethereum", "a", "2", "ipfs://unique-a", "ipfs://img"));
        store.ingest_row(row("ethereum", "c", "2", "ipfs://unique-c", "ipfs://img"));
        let mut acc = SummaryAccumulator::default();
        run_uri(&store, &mut acc, &NoopProgress).unwrap();
        let eth = *store.chain_ids.get("ethereum").unwrap();
        let image_key = crate::scope::ScopeKey {
            kind: crate::entity::ScopeKind::IntraChain,
            primary_chain: eth,
            secondary_chain: None,
            dimension: Dimension::ImageUri,
        };
        // a#2 and c#2 share image and were not token-matched
        assert_eq!(
            acc.counts().get(&image_key).unwrap().duplicate_nft_count,
            2
        );
    }

    #[test]
    fn interleaved_uri_rows_merge_postings() {
        let mut store = EntityStore::default();
        store.ingest_row(row("ethereum", "a", "1", "ipfs://x", ""));
        store.ingest_row(row("ethereum", "a", "2", "ipfs://y", ""));
        store.ingest_row(row("ethereum", "a", "3", "ipfs://x", ""));
        store.ingest_row(row("ethereum", "b", "1", "ipfs://x", ""));
        assert_eq!(store.token_uri_postings.len(), 3); // (a,x), (a,y), (b,x)
        let ax = store
            .token_uri_postings
            .iter()
            .find(|p| p.contract_id == 0 && p.uri == "ipfs://x")
            .unwrap();
        assert_eq!(ax.nft_count(), 2);
    }
}
