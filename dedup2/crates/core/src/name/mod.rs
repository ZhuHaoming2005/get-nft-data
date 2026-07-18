mod candidate_bounds;

pub use candidate_bounds::CandidateBounds;

use crate::entity::{ChainId, ContractId, Dimension, EntityStore};
use crate::error::DedupError;
use crate::progress::ProgressObserver;
use crate::stats::SummaryAccumulator;
use ahash::AHashMap;
use rapidfuzz::distance::jaro_winkler::{Args, BatchComparator};
use rayon::prelude::*;

#[derive(Clone, Debug)]
struct NameAtom {
    chain_id: ChainId,
    contract_ids: Vec<ContractId>,
    nft_count: u64,
}

#[derive(Clone, Debug)]
struct CanonicalName {
    text: String,
    characters: Vec<char>,
    character_counts: Vec<(char, u32)>,
    atoms: Vec<NameAtom>,
}

pub fn run_name(
    store: &EntityStore,
    threshold: f64,
    acc: &mut SummaryAccumulator,
    progress: &dyn ProgressObserver,
) -> Result<(), DedupError> {
    if !(0.0..=1.0).contains(&threshold) {
        return Err(DedupError::invalid(
            "name",
            "name threshold must be in [0, 1] (pass CLI value/100)",
        ));
    }
    progress.set_stage("name");
    progress.set_phase("atomize");
    let mut names = atomize(store);
    progress.set_phase("identical");
    emit_identical(&names, store, acc);

    progress.set_phase("candidates");
    let candidates = if threshold >= 0.95 {
        posting_candidates(&names, progress)?
    } else {
        exhaustive_candidates(names.len(), progress)?
    };

    progress.set_phase("score");
    progress.set_total(Some(candidates.len() as u64));
    let matches = score_candidates(&names, &candidates, threshold, progress)?;
    progress.set_phase("emit");
    for (left, right) in matches {
        emit_pair(&names[left], &names[right], store, acc);
    }
    let _ = &mut names;
    Ok(())
}

fn atomize(store: &EntityStore) -> Vec<CanonicalName> {
    let mut by_text: AHashMap<String, Vec<NameAtom>> = AHashMap::new();
    let mut atom_index: AHashMap<(ChainId, String), usize> = AHashMap::new();

    for contract in &store.contracts {
        let Some(name) = contract.name_norm.as_ref() else {
            continue;
        };
        if name.is_empty() {
            continue;
        }
        let key = (contract.chain_id, name.clone());
        if let Some(&atom_pos) = atom_index.get(&key) {
            let atoms = by_text.get_mut(name).unwrap();
            atoms[atom_pos].contract_ids.push(contract.id);
            atoms[atom_pos].nft_count += contract.nft_count;
        } else {
            let atoms = by_text.entry(name.clone()).or_default();
            let pos = atoms.len();
            atoms.push(NameAtom {
                chain_id: contract.chain_id,
                contract_ids: vec![contract.id],
                nft_count: contract.nft_count,
            });
            atom_index.insert(key, pos);
        }
    }

    let mut names: Vec<CanonicalName> = by_text
        .into_iter()
        .map(|(text, atoms)| {
            let characters: Vec<char> = text.chars().collect();
            let mut counts: AHashMap<char, u32> = AHashMap::new();
            for ch in &characters {
                *counts.entry(*ch).or_default() += 1;
            }
            let mut character_counts: Vec<(char, u32)> = counts.into_iter().collect();
            character_counts.sort_by_key(|(ch, _)| *ch);
            CanonicalName {
                text,
                characters,
                character_counts,
                atoms,
            }
        })
        .collect();
    names.sort_by(|a, b| {
        a.characters
            .len()
            .cmp(&b.characters.len())
            .then_with(|| a.text.cmp(&b.text))
    });
    names
}

fn emit_identical(names: &[CanonicalName], store: &EntityStore, acc: &mut SummaryAccumulator) {
    for name in names {
        if name.atoms.len() < 2 {
            let Some(atom) = name.atoms.first() else {
                continue;
            };
            if atom.contract_ids.len() >= 2 {
                for &cid in &atom.contract_ids {
                    for &peer in &atom.contract_ids {
                        if cid != peer {
                            acc.mark_contract_duplicate(store, cid, Dimension::Name, atom.chain_id);
                        }
                    }
                }
            }
            continue;
        }
        for left in &name.atoms {
            for right in &name.atoms {
                for &cid in &left.contract_ids {
                    acc.mark_contract_duplicate(store, cid, Dimension::Name, right.chain_id);
                }
            }
        }
    }
}

fn exhaustive_candidates(
    n: usize,
    progress: &dyn ProgressObserver,
) -> Result<Vec<(usize, usize)>, DedupError> {
    let mut out = Vec::new();
    progress.set_total(Some(n as u64));
    for i in 0..n {
        progress.check_cancelled()?;
        for j in (i + 1)..n {
            out.push((i, j));
        }
        progress.add_completed(1);
    }
    Ok(out)
}

fn posting_candidates(
    names: &[CanonicalName],
    progress: &dyn ProgressObserver,
) -> Result<Vec<(usize, usize)>, DedupError> {
    // Resident occurrence-token postings with length + multiset filter.
    type Token = (char, u32);
    let mut postings: AHashMap<Token, Vec<usize>> = AHashMap::new();
    for (idx, name) in names.iter().enumerate() {
        let mut occ: AHashMap<char, u32> = AHashMap::new();
        for ch in &name.characters {
            let rank = *occ.entry(*ch).or_default();
            *occ.get_mut(ch).unwrap() += 1;
            postings.entry((*ch, rank)).or_default().push(idx);
        }
    }
    let mut pairs: AHashMap<(usize, usize), ()> = AHashMap::new();
    progress.set_total(Some(names.len() as u64));
    for (left, name) in names.iter().enumerate() {
        progress.check_cancelled()?;
        let min_len = name.characters.len().saturating_mul(3).div_ceil(4);
        let max_len = name.characters.len().saturating_mul(4) / 3;
        let mut token_freq: Vec<(Token, usize)> = {
            let mut occ: AHashMap<char, u32> = AHashMap::new();
            let mut tokens = Vec::new();
            for ch in &name.characters {
                let rank = *occ.entry(*ch).or_default();
                *occ.get_mut(ch).unwrap() += 1;
                let token = (*ch, rank);
                let freq = postings.get(&token).map(|v| v.len()).unwrap_or(0);
                tokens.push((token, freq));
            }
            tokens.sort_by_key(|(_, freq)| *freq);
            tokens
        };
        // Probe rare-token prefix (up to half the tokens, at least 1).
        let probe_n = (token_freq.len() / 2).max(1).min(token_freq.len());
        token_freq.truncate(probe_n);
        for (token, _) in token_freq {
            let Some(list) = postings.get(&token) else {
                continue;
            };
            for &right in list {
                if right <= left {
                    continue;
                }
                let right_len = names[right].characters.len();
                if right_len < min_len || right_len > max_len {
                    continue;
                }
                if !CandidateBounds::can_pair_lengths(name.characters.len(), right_len) {
                    continue;
                }
                if !passes_overlap(name, &names[right]) {
                    continue;
                }
                pairs.insert((left, right), ());
            }
        }
        progress.add_completed(1);
    }
    let mut out: Vec<(usize, usize)> = pairs.into_keys().collect();
    out.sort_unstable();
    Ok(out)
}

fn passes_overlap(left: &CanonicalName, right: &CanonicalName) -> bool {
    let common_prefix = left
        .characters
        .iter()
        .zip(right.characters.iter())
        .take_while(|(a, b)| a == b)
        .count();
    let bounds = CandidateBounds::for_lengths_and_prefix(
        left.characters.len(),
        right.characters.len(),
        common_prefix,
    );
    let overlap = multiset_overlap(&left.character_counts, &right.character_counts);
    overlap >= bounds.minimum_multiset_overlap
}

fn multiset_overlap(left: &[(char, u32)], right: &[(char, u32)]) -> usize {
    let mut i = 0;
    let mut j = 0;
    let mut total = 0_u32;
    while i < left.len() && j < right.len() {
        match left[i].0.cmp(&right[j].0) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                total += left[i].1.min(right[j].1);
                i += 1;
                j += 1;
            }
        }
    }
    total as usize
}

fn score_candidates(
    names: &[CanonicalName],
    candidates: &[(usize, usize)],
    threshold: f64,
    progress: &dyn ProgressObserver,
) -> Result<Vec<(usize, usize)>, DedupError> {
    let args = Args::default().score_cutoff(threshold);
    let matches: Vec<(usize, usize)> = candidates
        .par_iter()
        .filter_map(|&(left, right)| {
            let prepared = BatchComparator::new(names[left].characters.iter().copied());
            prepared
                .similarity_with_args(names[right].characters.iter().copied(), &args)
                .map(|_| (left, right))
        })
        .collect();
    progress.add_completed(candidates.len() as u64);
    Ok(matches)
}

fn emit_pair(
    left: &CanonicalName,
    right: &CanonicalName,
    store: &EntityStore,
    acc: &mut SummaryAccumulator,
) {
    for la in &left.atoms {
        for ra in &right.atoms {
            for &cid in &la.contract_ids {
                acc.mark_contract_duplicate(store, cid, Dimension::Name, ra.chain_id);
            }
            for &cid in &ra.contract_ids {
                acc.mark_contract_duplicate(store, cid, Dimension::Name, la.chain_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::{InputRow, SourceOrder};
    use crate::progress::NoopProgress;

    fn named(chain: &str, contract: &str, name: &str) -> InputRow {
        InputRow {
            chain: chain.to_owned(),
            contract_address: contract.to_owned(),
            token_id: "1".to_owned(),
            name_norm: name.to_owned(),
            token_uri_norm: String::new(),
            image_uri_norm: String::new(),
            metadata_json: String::new(),
            source_order: SourceOrder {
                file_ordinal: 0,
                file_row_number: 0,
            },
        }
    }

    #[test]
    fn identical_names_count_without_jw() {
        let mut store = EntityStore::default();
        store.ingest_row(named("ethereum", "a", "collection"));
        store.ingest_row(named("base", "b", "collection"));
        let mut acc = SummaryAccumulator::default();
        run_name(&store, 0.95, &mut acc, &NoopProgress).unwrap();
        let eth = *store.chain_ids.get("ethereum").unwrap();
        let base = *store.chain_ids.get("base").unwrap();
        let key = crate::scope::ScopeKey {
            kind: crate::entity::ScopeKind::ChainMatrix,
            primary_chain: eth,
            secondary_chain: Some(base),
            dimension: Dimension::Name,
        };
        assert_eq!(
            acc.counts().get(&key).unwrap().duplicate_contract_count,
            1
        );
    }
}
