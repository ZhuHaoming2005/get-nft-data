use crate::entity::{
    ChainId, ContractId, Dimension, EntityStore, NftId, ScopeKind, StringId, UriPosting,
};
use crate::error::DedupError;
use crate::progress::ProgressObserver;
use crate::stats::SummaryAccumulator;
use ahash::{AHashMap, AHashSet};
use rayon::prelude::*;

#[derive(Clone)]
struct UriScopeHit {
    contract_id: ContractId,
    uri_id: StringId,
    nft_ids: Vec<NftId>,
    dimension: Dimension,
    kind: ScopeKind,
    secondary_chain: Option<ChainId>,
}

struct TokenHits {
    intra: DenseNftSet,
    cross_summary: DenseNftSet,
    matrix: AHashSet<(NftId, ChainId)>,
}

struct DenseNftSet {
    words: Vec<u64>,
}

impl DenseNftSet {
    fn with_capacity(nft_count: usize) -> Self {
        Self {
            words: vec![0; nft_count.div_ceil(64)],
        }
    }

    fn insert(&mut self, nft_id: NftId) {
        let nft_id = nft_id as usize;
        self.words[nft_id / 64] |= 1_u64 << (nft_id % 64);
    }

    fn contains(&self, nft_id: NftId) -> bool {
        let nft_id = nft_id as usize;
        self.words
            .get(nft_id / 64)
            .is_some_and(|word| word & (1_u64 << (nft_id % 64)) != 0)
    }
}

impl TokenHits {
    fn new(nft_count: usize) -> Self {
        Self {
            intra: DenseNftSet::with_capacity(nft_count),
            cross_summary: DenseNftSet::with_capacity(nft_count),
            matrix: AHashSet::new(),
        }
    }
}

pub fn run_uri(
    store: &EntityStore,
    acc: &mut SummaryAccumulator,
    progress: &dyn ProgressObserver,
) -> Result<(), DedupError> {
    progress.set_stage("uri");
    let token_ranges = group_ranges(&store.token_uri_postings);
    progress.begin_phase("token_uri", Some(token_ranges.len() as u64));
    let token_results: Vec<Result<Vec<UriScopeHit>, DedupError>> = token_ranges
        .par_iter()
        .map(|range| {
            progress.check_cancelled()?;
            let hits = token_scope_hits(&store.token_uri_postings[range.clone()]);
            progress.add_completed(1);
            Ok(hits)
        })
        .collect();
    let mut token_hits = TokenHits::new(store.nfts.len());
    for result in token_results {
        for hit in result? {
            remember_token_hit(&mut token_hits, &hit);
            apply_hit(store, acc, hit);
        }
    }

    let image_ranges = group_ranges(&store.image_uri_postings);
    progress.begin_phase("image_uri", Some(image_ranges.len() as u64));
    let image_results: Vec<Result<Vec<UriScopeHit>, DedupError>> = image_ranges
        .par_iter()
        .map(|range| {
            progress.check_cancelled()?;
            let hits = image_scope_hits(&store.image_uri_postings[range.clone()], &token_hits);
            progress.add_completed(1);
            Ok(hits)
        })
        .collect();
    for result in image_results {
        for hit in result? {
            apply_hit(store, acc, hit);
        }
    }
    Ok(())
}

fn group_ranges(postings: &[UriPosting]) -> Vec<std::ops::Range<usize>> {
    let mut ranges = Vec::new();
    let mut start = 0;
    while start < postings.len() {
        let uri_id = postings[start].uri_id;
        let mut end = start + 1;
        while end < postings.len() && postings[end].uri_id == uri_id {
            end += 1;
        }
        ranges.push(start..end);
        start = end;
    }
    ranges
}

fn postings_by_chain(members: &[UriPosting]) -> AHashMap<ChainId, Vec<&UriPosting>> {
    let mut by_chain: AHashMap<ChainId, Vec<&UriPosting>> = AHashMap::new();
    for member in members {
        by_chain.entry(member.chain_id).or_default().push(member);
    }
    by_chain
}

fn token_scope_hits(members: &[UriPosting]) -> Vec<UriScopeHit> {
    let by_chain = postings_by_chain(members);
    let chains: Vec<ChainId> = by_chain.keys().copied().collect();
    let mut hits = Vec::new();
    for (&chain, postings) in &by_chain {
        if postings.len() >= 2 {
            hits.extend(postings.iter().map(|posting| {
                scope_hit(posting, Dimension::TokenUri, ScopeKind::IntraChain, None)
            }));
        }
        if chains.len() >= 2 {
            hits.extend(postings.iter().map(|posting| {
                scope_hit(
                    posting,
                    Dimension::TokenUri,
                    ScopeKind::CrossChainSummary,
                    None,
                )
            }));
        }
        for &other_chain in &chains {
            if other_chain == chain {
                continue;
            }
            hits.extend(postings.iter().map(|posting| {
                scope_hit(
                    posting,
                    Dimension::TokenUri,
                    ScopeKind::ChainMatrix,
                    Some(other_chain),
                )
            }));
        }
    }
    hits
}

fn image_scope_hits(members: &[UriPosting], token_hits: &TokenHits) -> Vec<UriScopeHit> {
    let by_chain = postings_by_chain(members);
    let chains: Vec<ChainId> = by_chain.keys().copied().collect();
    let mut hits = Vec::new();

    for (&chain, postings) in &by_chain {
        let intra = filtered_postings(postings, |nft_id| !token_hits.intra.contains(nft_id));
        if intra.len() >= 2 {
            hits.extend(intra.into_iter().map(|(posting, nft_ids)| UriScopeHit {
                contract_id: posting.contract_id,
                uri_id: posting.uri_id,
                nft_ids,
                dimension: Dimension::ImageUri,
                kind: ScopeKind::IntraChain,
                secondary_chain: None,
            }));
        }

        let primary_cross = filtered_postings(postings, |nft_id| {
            !token_hits.cross_summary.contains(nft_id)
        });
        let has_other_cross = chains.iter().any(|&other_chain| {
            other_chain != chain
                && by_chain[&other_chain].iter().any(|posting| {
                    posting
                        .nft_ids
                        .iter()
                        .any(|&nft_id| !token_hits.cross_summary.contains(nft_id))
                })
        });
        if !primary_cross.is_empty() && has_other_cross {
            hits.extend(
                primary_cross
                    .into_iter()
                    .map(|(posting, nft_ids)| UriScopeHit {
                        contract_id: posting.contract_id,
                        uri_id: posting.uri_id,
                        nft_ids,
                        dimension: Dimension::ImageUri,
                        kind: ScopeKind::CrossChainSummary,
                        secondary_chain: None,
                    }),
            );
        }

        for &other_chain in &chains {
            if other_chain == chain {
                continue;
            }
            let primary_matrix = filtered_postings(postings, |nft_id| {
                !token_hits.matrix.contains(&(nft_id, other_chain))
            });
            let other_has_match = by_chain[&other_chain].iter().any(|posting| {
                posting
                    .nft_ids
                    .iter()
                    .any(|nft_id| !token_hits.matrix.contains(&(*nft_id, chain)))
            });
            if !primary_matrix.is_empty() && other_has_match {
                hits.extend(
                    primary_matrix
                        .into_iter()
                        .map(|(posting, nft_ids)| UriScopeHit {
                            contract_id: posting.contract_id,
                            uri_id: posting.uri_id,
                            nft_ids,
                            dimension: Dimension::ImageUri,
                            kind: ScopeKind::ChainMatrix,
                            secondary_chain: Some(other_chain),
                        }),
                );
            }
        }
    }
    hits
}

fn filtered_postings<'a>(
    postings: &[&'a UriPosting],
    keep: impl Fn(NftId) -> bool,
) -> Vec<(&'a UriPosting, Vec<NftId>)> {
    postings
        .iter()
        .filter_map(|posting| {
            let nft_ids = posting
                .nft_ids
                .iter()
                .copied()
                .filter(|&nft_id| keep(nft_id))
                .collect::<Vec<_>>();
            (!nft_ids.is_empty()).then_some((*posting, nft_ids))
        })
        .collect()
}

fn scope_hit(
    posting: &UriPosting,
    dimension: Dimension,
    kind: ScopeKind,
    secondary_chain: Option<ChainId>,
) -> UriScopeHit {
    UriScopeHit {
        contract_id: posting.contract_id,
        uri_id: posting.uri_id,
        nft_ids: posting.nft_ids.clone(),
        dimension,
        kind,
        secondary_chain,
    }
}

fn remember_token_hit(token_hits: &mut TokenHits, hit: &UriScopeHit) {
    match hit.kind {
        ScopeKind::IntraChain => {
            for &nft_id in &hit.nft_ids {
                token_hits.intra.insert(nft_id);
            }
        }
        ScopeKind::CrossChainSummary => {
            for &nft_id in &hit.nft_ids {
                token_hits.cross_summary.insert(nft_id);
            }
        }
        ScopeKind::ChainMatrix => {
            let secondary = hit.secondary_chain.expect("matrix hit has secondary chain");
            token_hits
                .matrix
                .extend(hit.nft_ids.iter().map(|&nft_id| (nft_id, secondary)));
        }
    }
}

fn apply_hit(store: &EntityStore, acc: &mut SummaryAccumulator, hit: UriScopeHit) {
    acc.mark_uri_scope_hit(
        store,
        hit.contract_id,
        hit.uri_id,
        hit.nft_ids.len() as u64,
        hit.dimension,
        (hit.kind, hit.secondary_chain),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::{InputRow, SourceOrder};
    use crate::progress::NoopProgress;

    fn row(chain: &str, contract: &str, token: &str, token_uri: &str, image_uri: &str) -> InputRow {
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
                file_row_number: token.parse().unwrap_or(0),
            },
        }
    }

    fn prepared(rows: impl IntoIterator<Item = InputRow>) -> EntityStore {
        let mut store = EntityStore::default();
        for row in rows {
            store.ingest_row(row);
        }
        store.rebuild_uri_postings();
        store
    }

    fn counts(
        store: &EntityStore,
        acc: &SummaryAccumulator,
        chain: &str,
        kind: ScopeKind,
        secondary: Option<&str>,
        dimension: Dimension,
    ) -> u64 {
        let primary = store.chain_ids[chain];
        let key = crate::scope::ScopeKey {
            kind,
            primary_chain: primary,
            secondary_chain: secondary.map(|name| store.chain_ids[name]),
            dimension,
        };
        acc.counts()
            .get(&key)
            .map(|value| value.duplicate_nft_count)
            .unwrap_or(0)
    }

    #[test]
    fn intra_chain_token_uri_counts_two_contracts() {
        let store = prepared([
            row("ethereum", "a", "1", "ipfs://x", ""),
            row("ethereum", "b", "1", "ipfs://x", ""),
        ]);
        let mut acc = SummaryAccumulator::default();
        run_uri(&store, &mut acc, &NoopProgress).unwrap();
        assert_eq!(
            counts(
                &store,
                &acc,
                "ethereum",
                ScopeKind::IntraChain,
                None,
                Dimension::TokenUri
            ),
            2
        );
    }

    #[test]
    fn cross_summary_nft_not_double_counted_across_peers() {
        let store = prepared([
            row("ethereum", "a", "1", "ipfs://x", ""),
            row("base", "b", "1", "ipfs://x", ""),
            row("polygon", "c", "1", "ipfs://x", ""),
        ]);
        let mut acc = SummaryAccumulator::default();
        run_uri(&store, &mut acc, &NoopProgress).unwrap();
        assert_eq!(
            counts(
                &store,
                &acc,
                "ethereum",
                ScopeKind::CrossChainSummary,
                None,
                Dimension::TokenUri
            ),
            1
        );
    }

    #[test]
    fn image_and_not_is_scope_specific() {
        let store = prepared([
            row("ethereum", "a", "1", "token://same", "image://same"),
            row("ethereum", "b", "1", "token://same", "image://other"),
            row("base", "c", "1", "token://base-only", "image://same"),
        ]);
        let mut acc = SummaryAccumulator::default();
        run_uri(&store, &mut acc, &NoopProgress).unwrap();
        assert_eq!(
            counts(
                &store,
                &acc,
                "ethereum",
                ScopeKind::IntraChain,
                None,
                Dimension::ImageUri
            ),
            0
        );
        assert_eq!(
            counts(
                &store,
                &acc,
                "ethereum",
                ScopeKind::ChainMatrix,
                Some("base"),
                Dimension::ImageUri
            ),
            1
        );
    }

    #[test]
    fn interleaved_uri_rows_merge_postings() {
        let store = prepared([
            row("ethereum", "a", "1", "ipfs://x", ""),
            row("ethereum", "a", "2", "ipfs://y", ""),
            row("ethereum", "a", "3", "ipfs://x", ""),
            row("ethereum", "b", "1", "ipfs://x", ""),
        ]);
        assert_eq!(store.token_uri_postings.len(), 3);
        let x = store.string_id("ipfs://x").unwrap();
        let posting = store
            .token_uri_postings
            .iter()
            .find(|posting| posting.contract_id == 0 && posting.uri_id == x)
            .unwrap();
        assert_eq!(posting.nft_count(), 2);
    }
}
