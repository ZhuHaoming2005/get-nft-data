//! Name dedup — resident path aligned with `name_uri_analysis_rs`
//! (`ResidentNameCandidateIndex` + length windows + rare-prefix probe + JW).
//! Full in-memory only; no spill / external / budget machinery.

mod candidate_bounds;

use crate::entity::{ChainId, ContractId, Dimension, EntityStore, NftId};
use crate::error::DedupError;
use crate::progress::ProgressObserver;
use crate::radix::{sort_u32_pairs_while, sort_u32_triples_while, sort_u64_while};
use crate::stats::SummaryAccumulator;
use ahash::{AHashMap, AHashSet};
use candidate_bounds::CandidateBounds;
use rapidfuzz::distance::jaro_winkler::{Args, BatchComparator};
use rayon::prelude::*;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

const PROGRESS_BATCH: u64 = 4096;
const SCORE_PROGRESS_BATCH: u64 = 64;
const SCORE_SCHEDULING_CHUNK: usize = 8;
const DENSE_SEEN_BUDGET_BYTES: usize = 4 * 1024 * 1024 * 1024;
type NameHits = AHashSet<NameHit>;

#[derive(Clone, Debug)]
struct NameAtom {
    chain_id: ChainId,
    allow_intra: bool,
    members: Vec<NameMember>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct NameMember {
    contract_id: ContractId,
    nft_id: Option<NftId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct NameHit {
    member: NameMember,
    peer_chain: ChainId,
}

#[derive(Clone, Debug)]
struct CanonicalName {
    text: String,
    characters: Vec<char>,
    atoms: Vec<NameAtom>,
    query_side: bool,
}

impl CanonicalName {
    fn is_passive_solana(&self) -> bool {
        !self.query_side
    }
}

/// Resident candidate index (name_uri `ResidentNameCandidateIndex`).
/// Full token postings + per-document prefix (freq-sorted) and sorted tokens.
struct ResidentNameIndex {
    document_offsets: Vec<usize>,
    prefix_tokens: Vec<u32>,
    sorted_tokens: Vec<u32>,
    posting_offsets: Vec<usize>,
    posting_names: Vec<u32>,
}

impl ResidentNameIndex {
    fn build(names: &[CanonicalName], progress: &dyn ProgressObserver) -> Result<Self, DedupError> {
        const INDEX_CHUNK: usize = 4096;
        if names.len() > u32::MAX as usize {
            return Err(DedupError::invalid(
                "name",
                "canonical name count exceeds the u32 resident-index limit",
            ));
        }
        progress.begin_phase("build_name_documents", Some(names.len() as u64));
        let raw_document_chunks = names
            .par_chunks(INDEX_CHUNK)
            .map(|chunk| {
                progress.check_cancelled()?;
                let documents = chunk
                    .iter()
                    .map(|name| {
                        let mut occurrences: AHashMap<char, u32> = AHashMap::new();
                        name.characters
                            .iter()
                            .map(|&character| {
                                let rank = occurrences.entry(character).or_default();
                                let key = (u64::from(character as u32) << 32) | u64::from(*rank);
                                *rank += 1;
                                key
                            })
                            .collect()
                    })
                    .collect::<Vec<Vec<u64>>>();
                progress.add_completed(chunk.len() as u64);
                Ok::<_, DedupError>(documents)
            })
            .collect::<Vec<_>>();
        let mut raw_documents = Vec::with_capacity(names.len());
        for chunk in raw_document_chunks {
            raw_documents.extend(chunk?);
        }

        let mut document_offsets = Vec::with_capacity(raw_documents.len() + 1);
        document_offsets.push(0);
        let token_occurrences = raw_documents.iter().map(Vec::len).sum();
        let mut raw_tokens = Vec::with_capacity(token_occurrences);
        for document in raw_documents {
            raw_tokens.extend(document);
            document_offsets.push(raw_tokens.len());
        }

        let mut unique_tokens = raw_tokens.clone();
        let token_sort_passes = if unique_tokens.len() > 1 { 6 } else { 0 };
        progress.begin_phase("sort_name_tokens", Some(token_sort_passes));
        if !sort_u64_while(&mut unique_tokens, || {
            progress.add_completed(1);
            progress.check_cancelled().is_ok()
        }) {
            return Err(DedupError::Interrupted);
        }
        unique_tokens.dedup();
        if unique_tokens.len() > u32::MAX as usize {
            return Err(DedupError::invalid(
                "name",
                "occurrence-token count exceeds the u32 resident-index limit",
            ));
        }
        let token_ids: AHashMap<u64, u32> = unique_tokens
            .into_iter()
            .enumerate()
            .map(|(index, token)| (token, index as u32))
            .collect();
        let token_count = token_ids.len();

        progress.begin_phase("encode_name_documents", Some(raw_tokens.len() as u64));
        let document_chunks = raw_tokens
            .par_chunks(INDEX_CHUNK)
            .map(|chunk| {
                progress.check_cancelled()?;
                let encoded = chunk
                    .iter()
                    .map(|token| token_ids[token])
                    .collect::<Vec<_>>();
                progress.add_completed(chunk.len() as u64);
                Ok::<_, DedupError>(encoded)
            })
            .collect::<Vec<_>>();
        let mut documents = Vec::with_capacity(raw_tokens.len());
        for chunk in document_chunks {
            documents.extend(chunk?);
        }
        drop(raw_tokens);
        drop(token_ids);

        progress.begin_phase("fill_name_postings", Some(names.len() as u64));
        let posting_chunks = names
            .par_chunks(INDEX_CHUNK)
            .enumerate()
            .map(|(chunk_id, chunk)| {
                progress.check_cancelled()?;
                let first_name = chunk_id * INDEX_CHUNK;
                let mut pairs = Vec::new();
                for name_id in first_name..first_name + chunk.len() {
                    let start = document_offsets[name_id];
                    let end = document_offsets[name_id + 1];
                    pairs.extend(
                        documents[start..end]
                            .iter()
                            .map(|&token_id| (token_id, name_id as u32)),
                    );
                }
                progress.add_completed(chunk.len() as u64);
                Ok::<_, DedupError>(pairs)
            })
            .collect::<Vec<_>>();
        let mut posting_pairs = Vec::with_capacity(token_occurrences);
        for chunk in posting_chunks {
            posting_pairs.extend(chunk?);
        }
        let posting_sort_passes = if posting_pairs.len() > 1 { 6 } else { 0 };
        progress.begin_phase("sort_name_postings", Some(posting_sort_passes));
        if !sort_u32_pairs_while(&mut posting_pairs, || {
            progress.add_completed(1);
            progress.check_cancelled().is_ok()
        }) {
            return Err(DedupError::Interrupted);
        }

        let mut posting_counts = vec![0usize; token_count];
        for &(token_id, _) in &posting_pairs {
            posting_counts[token_id as usize] += 1;
        }
        let mut posting_offsets = Vec::with_capacity(token_count + 1);
        posting_offsets.push(0);
        for count in posting_counts {
            posting_offsets.push(posting_offsets.last().copied().unwrap_or(0) + count);
        }
        let posting_names = posting_pairs
            .into_iter()
            .map(|(_, name_id)| name_id)
            .collect();

        progress.begin_phase("build_name_prefix", Some(names.len() as u64));
        let mut sorted_tokens = documents;
        let mut prefix_tokens = sorted_tokens.clone();
        let mut document_slices = Vec::with_capacity(names.len());
        let mut sorted_rest = sorted_tokens.as_mut_slice();
        let mut prefix_rest = prefix_tokens.as_mut_slice();
        for offsets in document_offsets.windows(2) {
            let len = offsets[1] - offsets[0];
            let (sorted, next_sorted) = sorted_rest.split_at_mut(len);
            let (prefix, next_prefix) = prefix_rest.split_at_mut(len);
            document_slices.push((sorted, prefix));
            sorted_rest = next_sorted;
            prefix_rest = next_prefix;
        }
        document_slices
            .par_chunks_mut(INDEX_CHUNK)
            .try_for_each(|chunk| {
                progress.check_cancelled()?;
                for (sorted, prefix) in &mut *chunk {
                    prefix.sort_unstable_by(|&a, &b| {
                        let a = a as usize;
                        let b = b as usize;
                        let a_len = posting_offsets[a + 1] - posting_offsets[a];
                        let b_len = posting_offsets[b + 1] - posting_offsets[b];
                        a_len.cmp(&b_len).then_with(|| a.cmp(&b))
                    });
                    sorted.sort_unstable();
                }
                progress.add_completed(chunk.len() as u64);
                Ok::<_, DedupError>(())
            })?;

        Ok(Self {
            document_offsets,
            prefix_tokens,
            sorted_tokens,
            posting_offsets,
            posting_names,
        })
    }

    fn prefix(&self, name: usize) -> &[u32] {
        &self.prefix_tokens[self.document_offsets[name]..self.document_offsets[name + 1]]
    }

    fn sorted(&self, name: usize) -> &[u32] {
        &self.sorted_tokens[self.document_offsets[name]..self.document_offsets[name + 1]]
    }

    fn posting(&self, token: u32) -> &[u32] {
        let token = token as usize;
        &self.posting_names[self.posting_offsets[token]..self.posting_offsets[token + 1]]
    }
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
    // name_uri_analysis_rs uses percent-scale JW thresholds (0..=100).
    let threshold_pct = threshold * 100.0;

    progress.set_stage("name");
    let names = atomize(store, progress)?;
    progress.begin_phase("identical", Some(names.len() as u64));
    emit_identical(&names, store, acc, progress)?;

    if names.len() < 2 {
        return Ok(());
    }

    let index = ResidentNameIndex::build(&names, progress)?;
    let hits = score_resident_index(&index, &names, threshold_pct, progress)?;

    progress.begin_phase("emit", Some(hits.len() as u64));
    let mut completed = 0_u64;
    for hit in hits {
        progress.check_cancelled()?;
        emit_hit(store, acc, hit);
        completed += 1;
        flush_progress(&mut completed, progress)?;
    }
    flush_remaining(&mut completed, progress);
    Ok(())
}

fn atomize(
    store: &EntityStore,
    progress: &dyn ProgressObserver,
) -> Result<Vec<CanonicalName>, DedupError> {
    const CHUNK: usize = 4096;
    progress.begin_phase("atomize_collect", Some(store.nfts.len() as u64));
    let (mut assignments, mut solana_members) = store
        .nfts
        .par_chunks(CHUNK)
        .map(|nfts| {
            progress.check_cancelled()?;
            let mut contract_assignments = Vec::new();
            let mut nft_members = Vec::new();
            for nft in nfts {
                let Some(name_id) = nft.name_id else {
                    continue;
                };
                let contract = &store.contracts[nft.contract_id as usize];
                if !is_usable_contract_name(store.string(name_id)) {
                    continue;
                }
                if store.is_solana_chain(contract.chain_id) {
                    nft_members.push((nft.id, contract.id, name_id));
                } else {
                    contract_assignments.push((contract.id, name_id));
                }
            }
            progress.add_completed(nfts.len() as u64);
            Ok::<_, DedupError>((contract_assignments, nft_members))
        })
        .try_reduce(
            || (Vec::new(), Vec::new()),
            |(mut left_contracts, mut left_nfts), (mut right_contracts, mut right_nfts)| {
                if left_contracts.len() < right_contracts.len() {
                    std::mem::swap(&mut left_contracts, &mut right_contracts);
                }
                if left_nfts.len() < right_nfts.len() {
                    std::mem::swap(&mut left_nfts, &mut right_nfts);
                }
                left_contracts.append(&mut right_contracts);
                left_nfts.append(&mut right_nfts);
                Ok((left_contracts, left_nfts))
            },
        )?;
    solana_members.sort_unstable_by_key(|&(nft_id, _, _)| nft_id);

    let assignment_sort_passes = if assignments.len() > 1 { 6 } else { 0 };
    progress.begin_phase("atomize_sort_assignments", Some(assignment_sort_passes));
    if !sort_u32_pairs_while(&mut assignments, || {
        progress.add_completed(1);
        progress.check_cancelled().is_ok()
    }) {
        return Err(DedupError::Interrupted);
    }

    progress.begin_phase("atomize_select", Some(assignments.len() as u64));
    let mut representatives = Vec::new();
    let mut index = 0;
    let mut completed = 0_u64;
    while index < assignments.len() {
        let contract_id = assignments[index].0;
        let mut selected_name = assignments[index].1;
        let mut selected_count = 0_u64;
        while index < assignments.len() && assignments[index].0 == contract_id {
            let name_id = assignments[index].1;
            let start = index;
            index += 1;
            while index < assignments.len()
                && assignments[index].0 == contract_id
                && assignments[index].1 == name_id
            {
                index += 1;
            }
            let count = (index - start) as u64;
            if count > selected_count
                || (count == selected_count && store.string(name_id) < store.string(selected_name))
            {
                selected_name = name_id;
                selected_count = count;
            }
            completed += count;
            flush_progress(&mut completed, progress)?;
        }
        representatives.push((contract_id, selected_name));
    }
    flush_remaining(&mut completed, progress);
    drop(assignments);

    let mut logical_members = Vec::with_capacity(representatives.len() + solana_members.len());
    logical_members.extend(representatives.iter().map(|&(contract_id, name_id)| {
        (
            name_id,
            store.contracts[contract_id as usize].chain_id,
            NameMember {
                contract_id,
                nft_id: None,
            },
        )
    }));
    logical_members.extend(
        solana_members
            .drain(..)
            .map(|(nft_id, contract_id, name_id)| {
                (
                    name_id,
                    store.contracts[contract_id as usize].chain_id,
                    NameMember {
                        contract_id,
                        nft_id: Some(nft_id),
                    },
                )
            }),
    );
    drop(representatives);

    progress.begin_phase("atomize_materialize", Some(logical_members.len() as u64));
    let selected_chunks = logical_members
        .par_chunks(CHUNK)
        .enumerate()
        .map(|(chunk_id, chunk)| {
            progress.check_cancelled()?;
            let base = chunk_id * CHUNK;
            let selected = chunk
                .iter()
                .enumerate()
                .map(|(offset, &(name_id, chain_id, _))| {
                    Ok((
                        name_id,
                        u32::from(chain_id),
                        u32::try_from(base + offset).map_err(|_| {
                            DedupError::invalid("name", "name member index exceeds u32")
                        })?,
                    ))
                })
                .collect::<Result<Vec<_>, DedupError>>()?;
            progress.add_completed(chunk.len() as u64);
            Ok::<_, DedupError>(selected)
        })
        .collect::<Vec<_>>();
    let mut selected = Vec::with_capacity(logical_members.len());
    for chunk in selected_chunks {
        selected.extend(chunk?);
    }

    let selected_sort_passes = if selected.len() > 1 { 9 } else { 0 };
    progress.begin_phase("atomize_sort_selected", Some(selected_sort_passes));
    if !sort_u32_triples_while(&mut selected, || {
        progress.add_completed(1);
        progress.check_cancelled().is_ok()
    }) {
        return Err(DedupError::Interrupted);
    }

    progress.begin_phase("atomize_group", Some(selected.len() as u64));
    let mut names = Vec::new();
    let mut selected_index = 0;
    let mut grouped = 0_u64;
    while selected_index < selected.len() {
        let name_id = selected[selected_index].0;
        let mut atoms = Vec::new();
        while selected_index < selected.len() && selected[selected_index].0 == name_id {
            let chain_id = ChainId::try_from(selected[selected_index].1)
                .map_err(|_| DedupError::invalid("name", "chain id exceeds ChainId"))?;
            let start = selected_index;
            selected_index += 1;
            while selected_index < selected.len()
                && selected[selected_index].0 == name_id
                && selected[selected_index].1 == u32::from(chain_id)
            {
                selected_index += 1;
            }
            atoms.push(NameAtom {
                chain_id,
                allow_intra: !store.is_solana_chain(chain_id),
                members: selected[start..selected_index]
                    .iter()
                    .map(|entry| logical_members[entry.2 as usize].2)
                    .collect(),
            });
            grouped += (selected_index - start) as u64;
            flush_progress(&mut grouped, progress)?;
        }
        let text = store.string(name_id).to_owned();
        let characters = text.chars().collect();
        names.push(CanonicalName {
            text,
            characters,
            query_side: atoms.iter().any(|atom| atom.allow_intra),
            atoms,
        });
    }
    flush_remaining(&mut grouped, progress);

    // Length-sorted: required for monotone right-range windows.
    names.sort_by(|a, b| {
        a.characters
            .len()
            .cmp(&b.characters.len())
            .then_with(|| a.text.cmp(&b.text))
    });
    Ok(names)
}

fn is_usable_contract_name(name: &str) -> bool {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return false;
    }
    const NULL_LIKE: [&str; 11] = [
        "none",
        "null",
        "nil",
        "undefined",
        "n/a",
        "na",
        "n.a.",
        "nan",
        "-",
        "--",
        ".",
    ];
    if NULL_LIKE
        .iter()
        .any(|null_like| trimmed.eq_ignore_ascii_case(null_like))
    {
        return false;
    }
    !(trimmed.len() == 1 && trimmed.as_bytes()[0].is_ascii_digit())
}

fn emit_identical(
    names: &[CanonicalName],
    store: &EntityStore,
    acc: &mut SummaryAccumulator,
    progress: &dyn ProgressObserver,
) -> Result<(), DedupError> {
    const CHUNK: usize = 4096;
    let partials: Vec<Result<NameHits, DedupError>> = names
        .par_chunks(CHUNK)
        .map(|chunk| {
            progress.check_cancelled()?;
            let mut hits = AHashSet::new();
            for name in chunk {
                for atom in &name.atoms {
                    if atom.allow_intra && atom.members.len() >= 2 {
                        record_atom_hits(atom, atom.chain_id, &mut hits);
                    }
                }
                for (position, left) in name.atoms.iter().enumerate() {
                    for right in &name.atoms[position + 1..] {
                        record_atom_hits(left, right.chain_id, &mut hits);
                        record_atom_hits(right, left.chain_id, &mut hits);
                    }
                }
            }
            progress.add_completed(chunk.len() as u64);
            Ok(hits)
        })
        .collect();
    let mut hits = AHashSet::new();
    for partial in partials {
        hits.extend(partial?);
    }
    for hit in hits {
        emit_hit(store, acc, hit);
    }
    Ok(())
}

fn emit_hit(store: &EntityStore, acc: &mut SummaryAccumulator, hit: NameHit) {
    if let Some(nft_id) = hit.member.nft_id {
        acc.mark_nft_duplicate(store, nft_id, Dimension::Name, hit.peer_chain);
    } else {
        acc.mark_contract_duplicate(
            store,
            hit.member.contract_id,
            Dimension::Name,
            hit.peer_chain,
        );
    }
}

fn score_resident_index(
    index: &ResidentNameIndex,
    names: &[CanonicalName],
    threshold_pct: f64,
    progress: &dyn ProgressObserver,
) -> Result<NameHits, DedupError> {
    let query_names = query_name_ids(names);
    let left_count = query_names.len();
    progress.begin_phase("score_name", Some(left_count as u64));
    if left_count == 0 {
        return Ok(AHashSet::new());
    }

    let left_starts = build_left_name_range_starts(names, threshold_pct);
    let right_ends = build_right_name_range_ends(names, threshold_pct);
    let cancelled = AtomicU64::new(0);
    let score_cutoff = (threshold_pct / 100.0).clamp(0.0, 1.0);
    let args = Args::default().score_cutoff(score_cutoff);

    enum CandidateSeen {
        Dense {
            generations: Vec<u16>,
            generation: u16,
        },
        Sparse(AHashSet<u32>),
    }

    impl CandidateSeen {
        fn new(name_count: usize, dense: bool) -> Self {
            if dense {
                Self::Dense {
                    generations: vec![0; name_count],
                    generation: 0,
                }
            } else {
                Self::Sparse(AHashSet::new())
            }
        }

        fn begin_name(&mut self) {
            match self {
                Self::Dense {
                    generations,
                    generation,
                } => {
                    *generation = generation.wrapping_add(1);
                    if *generation == 0 {
                        generations.fill(0);
                        *generation = 1;
                    }
                }
                Self::Sparse(seen) => seen.clear(),
            }
        }

        fn insert(&mut self, candidate: u32) -> bool {
            match self {
                Self::Dense {
                    generations,
                    generation,
                } => {
                    let slot = &mut generations[candidate as usize];
                    if *slot == *generation {
                        false
                    } else {
                        *slot = *generation;
                        true
                    }
                }
                Self::Sparse(seen) => seen.insert(candidate),
            }
        }
    }

    struct Worker {
        candidates: Vec<u32>,
        seen: CandidateSeen,
        hits: NameHits,
    }

    let lane_count = rayon::current_num_threads().min(left_count).max(1);
    let dense_bytes = names
        .len()
        .checked_mul(std::mem::size_of::<u16>())
        .and_then(|bytes| bytes.checked_mul(lane_count));
    let use_dense_seen = dense_bytes.is_some_and(|bytes| bytes <= DENSE_SEEN_BUDGET_BYTES);

    let next_left = AtomicUsize::new(0);
    let worker = (0..lane_count)
        .into_par_iter()
        .map(|_| {
            let mut worker = Worker {
                candidates: Vec::new(),
                seen: CandidateSeen::new(names.len(), use_dense_seen),
                hits: AHashSet::new(),
            };
            let mut pending = 0_u64;
            loop {
                let start = next_left.fetch_add(SCORE_SCHEDULING_CHUNK, Ordering::Relaxed);
                if start >= left_count || cancelled.load(Ordering::Relaxed) != 0 {
                    break;
                }
                for &left in query_names
                    .iter()
                    .take((start + SCORE_SCHEDULING_CHUNK).min(left_count))
                    .skip(start)
                {
                    let left = left as usize;
                    worker.candidates.clear();
                    worker.seen.begin_name();
                    let mut collect_range =
                        |range_start: usize, range_end: usize, passive_only: bool| {
                            if range_start >= range_end {
                                return;
                            }
                            let minimum_overlap = CandidateBounds::minimum_multiset_overlap(
                                names[left].characters.len(),
                                names[range_start].characters.len(),
                                threshold_pct,
                            );

                            if minimum_overlap == 0 {
                                worker.candidates.extend(
                                    (range_start..range_end)
                                        .filter(|&right| {
                                            !passive_only || names[right].is_passive_solana()
                                        })
                                        .map(|right| right as u32),
                                );
                            } else {
                                let prefix = index.prefix(left);
                                let prefix_len = prefix
                                    .len()
                                    .saturating_sub(minimum_overlap)
                                    .saturating_add(1)
                                    .min(prefix.len());
                                let compact_start = range_start as u32;
                                let compact_end = range_end as u32;
                                for &token_id in &prefix[..prefix_len] {
                                    let posting = index.posting(token_id);
                                    let lo = posting.partition_point(|&a| a < compact_start);
                                    let hi = posting.partition_point(|&a| a < compact_end);
                                    for &candidate in &posting[lo..hi] {
                                        if (!passive_only
                                            || names[candidate as usize].is_passive_solana())
                                            && worker.seen.insert(candidate)
                                        {
                                            worker.candidates.push(candidate);
                                        }
                                    }
                                }
                            }
                        };

                    collect_range(left_starts[left], left, true);
                    let right_start = left + 1;
                    let right_end = right_ends.get(left).copied().unwrap_or(names.len());
                    collect_range(right_start, right_end, false);

                    worker.candidates.retain(|&right| {
                        resident_candidate_passes_overlap(
                            index,
                            names,
                            left,
                            right as usize,
                            threshold_pct,
                        )
                    });

                    if !worker.candidates.is_empty() {
                        let prepared = BatchComparator::new(names[left].characters.iter().copied());
                        for &right in &worker.candidates {
                            let right = right as usize;
                            if prepared
                                .similarity_with_args(
                                    names[right].characters.iter().copied(),
                                    &args,
                                )
                                .is_some()
                            {
                                record_pair_hits(&names[left], &names[right], &mut worker.hits);
                            }
                        }
                    }

                    pending += 1;
                    if pending >= SCORE_PROGRESS_BATCH {
                        progress.add_completed(pending);
                        if progress.check_cancelled().is_err() {
                            cancelled.store(1, Ordering::Relaxed);
                        }
                        pending = 0;
                    }
                }
            }
            if pending > 0 {
                progress.add_completed(pending);
            }
            worker
        })
        .reduce(
            || Worker {
                candidates: Vec::new(),
                seen: CandidateSeen::new(0, false),
                hits: AHashSet::new(),
            },
            |mut left, mut right| {
                if left.hits.len() < right.hits.len() {
                    std::mem::swap(&mut left.hits, &mut right.hits);
                }
                left.hits.extend(right.hits);
                left
            },
        );

    if cancelled.load(Ordering::Relaxed) != 0 {
        return Err(DedupError::Interrupted);
    }
    Ok(worker.hits)
}

fn query_name_ids(names: &[CanonicalName]) -> Vec<u32> {
    let mut passive_before = false;
    let mut query_names = Vec::new();
    for (name_id, name) in names.iter().enumerate() {
        if name.query_side {
            if name_id + 1 < names.len() || passive_before {
                query_names.push(name_id as u32);
            }
        } else {
            passive_before = true;
        }
    }
    query_names
}

fn record_pair_hits(left: &CanonicalName, right: &CanonicalName, hits: &mut NameHits) {
    for left_atom in &left.atoms {
        for right_atom in &right.atoms {
            if left_atom.chain_id == right_atom.chain_id
                && (!left_atom.allow_intra || !right_atom.allow_intra)
            {
                continue;
            }
            record_atom_hits(left_atom, right_atom.chain_id, hits);
            record_atom_hits(right_atom, left_atom.chain_id, hits);
        }
    }
}

fn record_atom_hits(atom: &NameAtom, peer_chain: ChainId, hits: &mut NameHits) {
    hits.extend(
        atom.members
            .iter()
            .map(|&member| NameHit { member, peer_chain }),
    );
}

fn build_right_name_range_ends(names: &[CanonicalName], threshold_pct: f64) -> Vec<usize> {
    let left_count = names.len().saturating_sub(1);
    let mut ends = Vec::with_capacity(left_count);
    let mut right = 1usize;
    for left in 0..left_count {
        right = right.max(left + 1);
        while right < names.len()
            && CandidateBounds::lengths_can_reach(
                names[left].characters.len(),
                names[right].characters.len(),
                threshold_pct,
            )
        {
            right += 1;
        }
        ends.push(right);
    }
    ends
}

fn build_left_name_range_starts(names: &[CanonicalName], threshold_pct: f64) -> Vec<usize> {
    let mut starts = Vec::with_capacity(names.len());
    let mut left = 0usize;
    for right in 0..names.len() {
        while left < right
            && !CandidateBounds::lengths_can_reach(
                names[left].characters.len(),
                names[right].characters.len(),
                threshold_pct,
            )
        {
            left += 1;
        }
        starts.push(left);
    }
    starts
}

fn resident_candidate_passes_overlap(
    index: &ResidentNameIndex,
    names: &[CanonicalName],
    left: usize,
    right: usize,
    threshold_pct: f64,
) -> bool {
    let required = CandidateBounds::minimum_multiset_overlap(
        names[left].characters.len(),
        names[right].characters.len(),
        threshold_pct,
    );
    required
        <= names[left]
            .characters
            .len()
            .min(names[right].characters.len())
        && sorted_name_token_overlap_at_least(index.sorted(left), index.sorted(right), required)
}

fn sorted_name_token_overlap_at_least(left: &[u32], right: &[u32], required: usize) -> bool {
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
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
        }
    }
    false
}

fn flush_progress(completed: &mut u64, progress: &dyn ProgressObserver) -> Result<(), DedupError> {
    if *completed >= PROGRESS_BATCH {
        progress.add_completed(*completed);
        progress.check_cancelled()?;
        *completed = 0;
    }
    Ok(())
}

fn flush_remaining(completed: &mut u64, progress: &dyn ProgressObserver) {
    if *completed > 0 {
        progress.add_completed(*completed);
        *completed = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::{InputRow, SourceOrder};
    use crate::progress::{NoopProgress, ProgressObserver};
    use std::sync::Mutex;

    #[derive(Default)]
    struct ScoreProgress {
        phase: Mutex<String>,
        score_total: Mutex<Option<u64>>,
        score_deltas: Mutex<Vec<u64>>,
    }

    impl ProgressObserver for ScoreProgress {
        fn set_stage(&self, _stage: &str) {}

        fn begin_phase(&self, phase: &str, total: Option<u64>) {
            *self.phase.lock().unwrap() = phase.to_owned();
            if phase == "score_name" {
                *self.score_total.lock().unwrap() = total;
            }
        }

        fn set_total(&self, _total: Option<u64>) {}

        fn add_completed(&self, delta: u64) {
            if *self.phase.lock().unwrap() == "score_name" {
                self.score_deltas.lock().unwrap().push(delta);
            }
        }
    }

    fn named(chain: &str, contract: &str, name: &str) -> InputRow {
        named_token(chain, contract, "1", name)
    }

    fn named_token(chain: &str, contract: &str, token: &str, name: &str) -> InputRow {
        InputRow {
            chain: chain.to_owned(),
            contract_address: contract.to_owned(),
            token_id: token.to_owned(),
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

    fn scope_counts<'a>(
        store: &EntityStore,
        acc: &'a SummaryAccumulator,
        chain: &str,
    ) -> &'a crate::scope::ScopeCounts {
        let chain_id = *store.chain_ids.get(chain).unwrap();
        let key = crate::scope::ScopeKey {
            kind: crate::entity::ScopeKind::IntraChain,
            primary_chain: chain_id,
            secondary_chain: None,
            dimension: Dimension::Name,
        };
        acc.counts().get(&key).unwrap()
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
        assert_eq!(acc.counts().get(&key).unwrap().duplicate_contract_count, 1);
        let intra = crate::scope::ScopeKey {
            kind: crate::entity::ScopeKind::IntraChain,
            primary_chain: eth,
            secondary_chain: None,
            dimension: Dimension::Name,
        };
        assert!(acc.counts().get(&intra).is_none());
    }

    #[test]
    fn fuzzy_near_duplicate_matches() {
        let mut store = EntityStore::default();
        store.ingest_row(named("ethereum", "a", "collection"));
        store.ingest_row(named("ethereum", "b", "collectiom"));
        let mut acc = SummaryAccumulator::default();
        run_name(&store, 0.95, &mut acc, &NoopProgress).unwrap();
        let eth = *store.chain_ids.get("ethereum").unwrap();
        let key = crate::scope::ScopeKey {
            kind: crate::entity::ScopeKind::IntraChain,
            primary_chain: eth,
            secondary_chain: None,
            dimension: Dimension::Name,
        };
        assert_eq!(acc.counts().get(&key).unwrap().duplicate_contract_count, 2);
    }

    #[test]
    fn evm_contract_uses_most_common_usable_nft_name() {
        let evm = ["ethereum".to_owned()].into_iter().collect();
        let mut store = EntityStore::with_options(8, &evm);
        for (token, name) in [
            ("1", "none"),
            ("2", "NONE"),
            ("3", "7"),
            ("4", "winner"),
            ("5", "winner"),
            ("6", "loser"),
        ] {
            store.ingest_row(named_token("ethereum", "a", token, name));
        }
        store.ingest_row(named_token("ethereum", "b", "1", "winner"));

        let mut acc = SummaryAccumulator::default();
        run_name(&store, 0.95, &mut acc, &NoopProgress).unwrap();

        let counts = scope_counts(&store, &acc, "ethereum");
        assert_eq!(counts.duplicate_contract_count, 2);
        assert_eq!(counts.duplicate_nft_count, 7);
    }

    #[test]
    fn evm_contract_name_tie_uses_lexicographically_smallest_name() {
        let evm = ["ethereum".to_owned()].into_iter().collect();
        let mut store = EntityStore::with_options(8, &evm);
        store.ingest_row(named_token("ethereum", "a", "1", "zeta"));
        store.ingest_row(named_token("ethereum", "a", "2", "alpha"));
        store.ingest_row(named_token("ethereum", "b", "1", "alpha"));

        let mut acc = SummaryAccumulator::default();
        run_name(&store, 1.0, &mut acc, &NoopProgress).unwrap();

        let counts = scope_counts(&store, &acc, "ethereum");
        assert_eq!(counts.duplicate_contract_count, 2);
        assert_eq!(counts.duplicate_nft_count, 3);
    }

    #[test]
    fn solana_participates_cross_chain_per_nft_but_not_intra_chain() {
        let mut store = EntityStore::default();
        store.ingest_row(named_token("solana", "collection-a", "mint-1", "shared"));
        store.ingest_row(named_token("solana", "collection-a", "mint-2", "unique"));
        store.ingest_row(named_token("solana", "collection-b", "mint-3", "shared"));
        store.ingest_row(named_token("ethereum", "evm-a", "1", "shared"));
        store.ingest_row(named_token("ethereum", "evm-b", "1", "shared"));

        let mut acc = SummaryAccumulator::default();
        run_name(&store, 1.0, &mut acc, &NoopProgress).unwrap();

        let counts = scope_counts(&store, &acc, "ethereum");
        assert_eq!(counts.duplicate_contract_count, 2);
        assert_eq!(counts.duplicate_nft_count, 2);
        let solana = *store.chain_ids.get("solana").unwrap();
        let ethereum = *store.chain_ids.get("ethereum").unwrap();
        let scope = |kind, primary, secondary| crate::scope::ScopeKey {
            kind,
            primary_chain: primary,
            secondary_chain: secondary,
            dimension: Dimension::Name,
        };
        assert!(
            acc.counts()
                .get(&scope(crate::entity::ScopeKind::IntraChain, solana, None))
                .is_none()
        );
        let solana_cross = acc
            .counts()
            .get(&scope(
                crate::entity::ScopeKind::ChainMatrix,
                solana,
                Some(ethereum),
            ))
            .unwrap();
        assert_eq!(solana_cross.duplicate_contract_count, 2);
        assert_eq!(solana_cross.duplicate_nft_count, 2);
        let ethereum_cross = acc
            .counts()
            .get(&scope(
                crate::entity::ScopeKind::ChainMatrix,
                ethereum,
                Some(solana),
            ))
            .unwrap();
        assert_eq!(ethereum_cross.duplicate_contract_count, 2);
        assert_eq!(ethereum_cross.duplicate_nft_count, 2);
    }

    #[test]
    fn solana_fuzzy_name_matches_cross_chain_without_intra_chain_hits() {
        let mut store = EntityStore::default();
        store.ingest_row(named_token(
            "solana",
            "collection-a",
            "mint-1",
            "collection",
        ));
        store.ingest_row(named_token(
            "solana",
            "collection-a",
            "mint-2",
            "collectiom",
        ));
        store.ingest_row(named_token("ethereum", "evm-a", "1", "collection"));

        let mut acc = SummaryAccumulator::default();
        run_name(&store, 0.95, &mut acc, &NoopProgress).unwrap();

        let solana = store.chain_ids["solana"];
        let ethereum = store.chain_ids["ethereum"];
        let scope = |kind, primary, secondary| crate::scope::ScopeKey {
            kind,
            primary_chain: primary,
            secondary_chain: secondary,
            dimension: Dimension::Name,
        };
        assert!(
            acc.counts()
                .get(&scope(crate::entity::ScopeKind::IntraChain, solana, None))
                .is_none()
        );
        let cross = acc
            .counts()
            .get(&scope(
                crate::entity::ScopeKind::ChainMatrix,
                solana,
                Some(ethereum),
            ))
            .unwrap();
        assert_eq!(cross.duplicate_contract_count, 1);
        assert_eq!(cross.duplicate_nft_count, 2);
    }

    #[test]
    fn solana_only_names_are_passive_fuzzy_index_entries() {
        let mut store = EntityStore::default();
        store.ingest_row(named_token(
            "solana",
            "collection-a",
            "mint-1",
            "collection",
        ));
        store.ingest_row(named_token(
            "solana",
            "collection-b",
            "mint-2",
            "collectiom",
        ));

        let names = atomize(&store, &NoopProgress).unwrap();
        assert!(names.iter().all(CanonicalName::is_passive_solana));
        assert!(query_name_ids(&names).is_empty());
        let index = ResidentNameIndex::build(&names, &NoopProgress).unwrap();
        let progress = ScoreProgress::default();
        let hits = score_resident_index(&index, &names, 95.0, &progress).unwrap();
        assert!(hits.is_empty());
        assert_eq!(*progress.score_total.lock().unwrap(), Some(0));
        assert!(progress.score_deltas.lock().unwrap().is_empty());
    }

    #[test]
    fn evm_query_finds_an_earlier_passive_solana_name() {
        let mut store = EntityStore::default();
        store.ingest_row(named_token(
            "solana",
            "collection-a",
            "mint-1",
            "collectiom",
        ));
        store.ingest_row(named("ethereum", "evm-a", "collection"));

        let names = atomize(&store, &NoopProgress).unwrap();
        assert_eq!(names.len(), 2);
        assert!(names[0].is_passive_solana());
        assert!(names[1].query_side);
        assert_eq!(query_name_ids(&names), vec![1]);
        let index = ResidentNameIndex::build(&names, &NoopProgress).unwrap();
        let hits = score_resident_index(&index, &names, 95.0, &NoopProgress).unwrap();
        assert_eq!(hits.len(), 2);
    }

    #[test]
    fn cross_chain_match_counts_each_contracts_logical_nfts() {
        let evm = ["ethereum".to_owned(), "base".to_owned()]
            .into_iter()
            .collect();
        let mut store = EntityStore::with_options(8, &evm);
        store.ingest_row(named_token("ethereum", "evm-a", "1", "shared"));
        store.ingest_row(named_token("ethereum", "evm-a", "2", "shared"));
        store.ingest_row(named_token("base", "base-a", "1", "shared"));

        let mut acc = SummaryAccumulator::default();
        run_name(&store, 1.0, &mut acc, &NoopProgress).unwrap();

        let ethereum = *store.chain_ids.get("ethereum").unwrap();
        let base = *store.chain_ids.get("base").unwrap();
        let matrix = |primary, secondary| crate::scope::ScopeKey {
            kind: crate::entity::ScopeKind::ChainMatrix,
            primary_chain: primary,
            secondary_chain: Some(secondary),
            dimension: Dimension::Name,
        };
        let evm_counts = acc.counts().get(&matrix(ethereum, base)).unwrap();
        assert_eq!(evm_counts.duplicate_contract_count, 1);
        assert_eq!(evm_counts.duplicate_nft_count, 2);
        let base_counts = acc.counts().get(&matrix(base, ethereum)).unwrap();
        assert_eq!(base_counts.duplicate_contract_count, 1);
        assert_eq!(base_counts.duplicate_nft_count, 1);
    }

    #[test]
    fn overlap_bounds_match_name_uri_scale() {
        assert!(CandidateBounds::minimum_multiset_overlap(10, 10, 95.0) <= 10);
        assert!(CandidateBounds::lengths_can_reach(75, 100, 95.0));
        assert!(!CandidateBounds::lengths_can_reach(74, 100, 95.0));
    }

    #[test]
    fn resident_candidate_pipeline_matches_exhaustive_jw() {
        let mut values = Vec::new();
        for len in 1..=6 {
            for bits in 0..(1usize << len) {
                values.push(
                    (0..len)
                        .map(|position| {
                            if bits & (1 << position) == 0 {
                                'a'
                            } else {
                                'b'
                            }
                        })
                        .collect::<String>(),
                );
            }
        }
        let mut store = EntityStore::default();
        for (index, value) in values.iter().enumerate() {
            let chain = if index % 3 == 0 { "solana" } else { "ethereum" };
            store.ingest_row(named(chain, &format!("contract-{index}"), value));
        }
        let names = atomize(&store, &NoopProgress).unwrap();
        let index = ResidentNameIndex::build(&names, &NoopProgress).unwrap();
        for threshold_pct in [0.0, 50.0, 80.0, 95.0, 100.0] {
            let actual =
                score_resident_index(&index, &names, threshold_pct, &NoopProgress).unwrap();
            let mut expected = AHashSet::new();
            for left in 0..names.len() {
                for right in (left + 1)..names.len() {
                    let score = rapidfuzz::distance::jaro_winkler::similarity(
                        names[left].characters.iter().copied(),
                        names[right].characters.iter().copied(),
                    );
                    if score * 100.0 >= threshold_pct {
                        record_pair_hits(&names[left], &names[right], &mut expected);
                    }
                }
            }
            assert_eq!(actual, expected, "threshold={threshold_pct}");
        }
    }

    #[test]
    fn score_progress_flushes_each_lane_without_waiting_for_global_reduce() {
        let mut store = EntityStore::default();
        for index in 0..257 {
            store.ingest_row(named(
                "ethereum",
                &format!("contract-{index}"),
                &format!("distinct-name-{index:04}"),
            ));
        }
        let progress = ScoreProgress::default();
        let names = atomize(&store, &progress).unwrap();
        let candidate_index = ResidentNameIndex::build(&names, &progress).unwrap();
        score_resident_index(&candidate_index, &names, 100.0, &progress).unwrap();

        let deltas = progress.score_deltas.lock().unwrap();
        assert!(!deltas.is_empty());
        assert!(deltas.iter().all(|&delta| delta <= SCORE_PROGRESS_BATCH));
        assert_eq!(
            deltas.iter().copied().sum::<u64>(),
            names.len().saturating_sub(1) as u64
        );
    }
}
