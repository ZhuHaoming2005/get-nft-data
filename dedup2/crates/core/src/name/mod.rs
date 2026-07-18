//! Name dedup — resident path aligned with `name_uri_analysis_rs`
//! (`ResidentNameCandidateIndex` + length windows + rare-prefix probe + JW).
//! Full in-memory only; no spill / external / budget machinery.

mod candidate_bounds;

use crate::entity::{ChainId, ContractId, Dimension, EntityStore};
use crate::error::DedupError;
use crate::progress::ProgressObserver;
use crate::radix::{sort_u32_pairs, sort_u64};
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
type NameAtomMap = AHashMap<(String, ChainId), NameAtom>;
type NameHits = AHashSet<(ContractId, ChainId)>;

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
    atoms: Vec<NameAtom>,
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
        if names.len() > u32::MAX as usize {
            return Err(DedupError::invalid(
                "name",
                "canonical name count exceeds the u32 resident-index limit",
            ));
        }
        progress.begin_phase("build_name_documents", Some(names.len() as u64));
        let raw_documents: Vec<Vec<u64>> = names
            .par_iter()
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
            .collect();
        progress.add_completed(names.len() as u64);
        progress.check_cancelled()?;

        let mut document_offsets = Vec::with_capacity(raw_documents.len() + 1);
        document_offsets.push(0);
        let token_occurrences = raw_documents.iter().map(Vec::len).sum();
        let mut raw_tokens = Vec::with_capacity(token_occurrences);
        for document in raw_documents {
            raw_tokens.extend(document);
            document_offsets.push(raw_tokens.len());
        }

        let mut unique_tokens = raw_tokens.clone();
        sort_u64(&mut unique_tokens);
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
        let documents: Vec<u32> = raw_tokens
            .into_par_iter()
            .map(|token| token_ids[&token])
            .collect();
        let token_count = token_ids.len();
        drop(token_ids);

        progress.begin_phase("fill_name_postings", Some(names.len() as u64));
        let mut posting_pairs: Vec<(u32, u32)> = (0..names.len())
            .into_par_iter()
            .flat_map_iter(|name_id| {
                let name_id = name_id as u32;
                let start = document_offsets[name_id as usize];
                let end = document_offsets[name_id as usize + 1];
                documents[start..end]
                    .iter()
                    .map(move |&token_id| (token_id, name_id))
            })
            .collect();
        progress.add_completed(names.len() as u64);
        progress.begin_phase("sort_name_postings", Some(posting_pairs.len() as u64));
        sort_u32_pairs(&mut posting_pairs);
        progress.add_completed(posting_pairs.len() as u64);
        progress.check_cancelled()?;

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
            .into_par_iter()
            .for_each(|(sorted, prefix)| {
                prefix.sort_unstable_by(|&a, &b| {
                    let a = a as usize;
                    let b = b as usize;
                    let a_len = posting_offsets[a + 1] - posting_offsets[a];
                    let b_len = posting_offsets[b + 1] - posting_offsets[b];
                    a_len.cmp(&b_len).then_with(|| a.cmp(&b))
                });
                sorted.sort_unstable();
            });
        progress.add_completed(names.len() as u64);
        progress.check_cancelled()?;

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
    progress.begin_phase("atomize", Some(store.contracts.len() as u64));
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
    for (contract_id, peer_chain) in hits {
        progress.check_cancelled()?;
        acc.mark_contract_duplicate(store, contract_id, Dimension::Name, peer_chain);
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
    let partials: Vec<Result<NameAtomMap, DedupError>> = store
        .contracts
        .par_chunks(CHUNK)
        .map(|contracts| {
            progress.check_cancelled()?;
            let mut atoms = NameAtomMap::new();
            for contract in contracts {
                if let Some(name) = contract.name_norm.as_ref().filter(|name| !name.is_empty()) {
                    let atom = atoms
                        .entry((name.clone(), contract.chain_id))
                        .or_insert_with(|| NameAtom {
                            chain_id: contract.chain_id,
                            contract_ids: Vec::new(),
                            nft_count: 0,
                        });
                    atom.contract_ids.push(contract.id);
                    atom.nft_count += contract.nft_count;
                }
            }
            progress.add_completed(contracts.len() as u64);
            Ok(atoms)
        })
        .collect();
    let mut by_atom = NameAtomMap::new();
    for partial in partials {
        for (key, mut atom) in partial? {
            let combined = by_atom.entry(key).or_insert_with(|| NameAtom {
                chain_id: atom.chain_id,
                contract_ids: Vec::new(),
                nft_count: 0,
            });
            combined.contract_ids.append(&mut atom.contract_ids);
            combined.nft_count += atom.nft_count;
        }
    }
    let mut by_text: AHashMap<String, Vec<NameAtom>> = AHashMap::new();
    for ((text, _), atom) in by_atom {
        by_text.entry(text).or_default().push(atom);
    }

    let mut names: Vec<CanonicalName> = by_text
        .into_iter()
        .map(|(text, mut atoms)| {
            atoms.sort_unstable_by_key(|atom| atom.chain_id);
            let characters: Vec<char> = text.chars().collect();
            CanonicalName {
                text,
                characters,
                atoms,
            }
        })
        .collect();
    // Length-sorted: required for monotone right-range windows.
    names.sort_by(|a, b| {
        a.characters
            .len()
            .cmp(&b.characters.len())
            .then_with(|| a.text.cmp(&b.text))
    });
    Ok(names)
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
                    if atom.contract_ids.len() >= 2 {
                        hits.extend(
                            atom.contract_ids
                                .iter()
                                .map(|&contract_id| (contract_id, atom.chain_id)),
                        );
                    }
                }
                for (position, left) in name.atoms.iter().enumerate() {
                    for right in &name.atoms[position + 1..] {
                        hits.extend(
                            left.contract_ids
                                .iter()
                                .map(|&contract_id| (contract_id, right.chain_id)),
                        );
                        hits.extend(
                            right
                                .contract_ids
                                .iter()
                                .map(|&contract_id| (contract_id, left.chain_id)),
                        );
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
    for (contract_id, peer_chain) in hits {
        acc.mark_contract_duplicate(store, contract_id, Dimension::Name, peer_chain);
    }
    Ok(())
}

fn score_resident_index(
    index: &ResidentNameIndex,
    names: &[CanonicalName],
    threshold_pct: f64,
    progress: &dyn ProgressObserver,
) -> Result<AHashSet<(ContractId, ChainId)>, DedupError> {
    let left_count = names.len().saturating_sub(1);
    progress.begin_phase("score_name", Some(left_count as u64));
    if left_count == 0 {
        return Ok(AHashSet::new());
    }

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
        hits: AHashSet<(ContractId, ChainId)>,
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
                for left in start..(start + SCORE_SCHEDULING_CHUNK).min(left_count) {
                    worker.candidates.clear();
                    worker.seen.begin_name();
                    let right_start = left + 1;
                    let right_end = right_ends[left];
                    if right_start < right_end {
                        let right_min_len = names[right_start].characters.len();
                        let minimum_overlap = CandidateBounds::minimum_multiset_overlap(
                            names[left].characters.len(),
                            right_min_len,
                            threshold_pct,
                        );

                        if minimum_overlap == 0 {
                            worker
                                .candidates
                                .extend((right_start..right_end).map(|right| right as u32));
                        } else {
                            let prefix = index.prefix(left);
                            let prefix_len = prefix
                                .len()
                                .saturating_sub(minimum_overlap)
                                .saturating_add(1)
                                .min(prefix.len());
                            let compact_start = right_start as u32;
                            let compact_end = right_end as u32;
                            for &token_id in &prefix[..prefix_len] {
                                let posting = index.posting(token_id);
                                let lo = posting.partition_point(|&a| a < compact_start);
                                let hi = posting.partition_point(|&a| a < compact_end);
                                for &candidate in &posting[lo..hi] {
                                    if worker.seen.insert(candidate) {
                                        worker.candidates.push(candidate);
                                    }
                                }
                            }
                        }

                        worker.candidates.retain(|&right| {
                            resident_candidate_passes_overlap(
                                index,
                                names,
                                left,
                                right as usize,
                                threshold_pct,
                            )
                        });

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

fn record_pair_hits(
    left: &CanonicalName,
    right: &CanonicalName,
    hits: &mut AHashSet<(ContractId, ChainId)>,
) {
    for left_atom in &left.atoms {
        for right_atom in &right.atoms {
            hits.extend(
                left_atom
                    .contract_ids
                    .iter()
                    .map(|&contract_id| (contract_id, right_atom.chain_id)),
            );
            hits.extend(
                right_atom
                    .contract_ids
                    .iter()
                    .map(|&contract_id| (contract_id, left_atom.chain_id)),
            );
        }
    }
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
        score_deltas: Mutex<Vec<u64>>,
    }

    impl ProgressObserver for ScoreProgress {
        fn set_stage(&self, _stage: &str) {}

        fn begin_phase(&self, phase: &str, _total: Option<u64>) {
            *self.phase.lock().unwrap() = phase.to_owned();
        }

        fn set_total(&self, _total: Option<u64>) {}

        fn add_completed(&self, delta: u64) {
            if *self.phase.lock().unwrap() == "score_name" {
                self.score_deltas.lock().unwrap().push(delta);
            }
        }
    }

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
            store.ingest_row(named("ethereum", &format!("contract-{index}"), value));
        }
        let names = atomize(&store, &NoopProgress).unwrap();
        let index = ResidentNameIndex::build(&names, &NoopProgress).unwrap();
        let actual = score_resident_index(&index, &names, 95.0, &NoopProgress).unwrap();
        let mut expected = AHashSet::new();
        for left in 0..names.len() {
            for right in (left + 1)..names.len() {
                let score = rapidfuzz::distance::jaro_winkler::similarity(
                    names[left].characters.iter().copied(),
                    names[right].characters.iter().copied(),
                );
                if score >= 0.95 {
                    record_pair_hits(&names[left], &names[right], &mut expected);
                }
            }
        }
        assert_eq!(actual, expected);
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
