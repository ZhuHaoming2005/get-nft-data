use super::*;

use rapidfuzz::distance::jaro_winkler;

pub(crate) type NameTokenId = u32;
pub(crate) type NameAtomIndex = u32;

pub(crate) struct IndexedNameDocument {
    prefix_tokens: Vec<NameTokenId>,
    sorted_tokens: Vec<NameTokenId>,
}

pub(crate) struct NameCandidateIndex {
    pub(crate) documents: Vec<IndexedNameDocument>,
    pub(crate) postings: Vec<Vec<NameAtomIndex>>,
}

/// Per-worker scratch space for candidate generation. The dedup backend is
/// chosen at construction time: a dense generation array for small atom sets
/// (O(1) push, O(1) clear via a generation counter) and a `HashSet` for large
/// ones (O(candidates) resident memory instead of O(atom_count), which matters
/// when `name_uri_analysis_rs` processes large atom sets under a memory budget).
pub(crate) struct NameCandidateScratch {
    candidates: Vec<NameAtomIndex>,
    dedup: NameDedup,
}

pub(crate) enum NameDedup {
    Dense {
        seen_generation: Vec<u32>,
        generation: u32,
    },
    Sparse {
        seen: HashSet<NameAtomIndex>,
    },
}

/// Above this atom count the per-worker dense generation array
/// (`atom_count * size_of::<u32>()`) is replaced with a sparse `HashSet` to
/// keep resident scratch memory bounded. Below it the dense array is faster
/// (no hashing, O(1) clear). The threshold is conservative: production name
/// tasks are sharded well below this, so the dense path is used unless an
/// unusually large atom set is analyzed in one batch.
pub(crate) const SPARSE_DEDUP_ATOM_THRESHOLD: usize = 1 << 20; // ~1M atoms -> ~4 MB dense array

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NameScratchMode {
    Dense,
    Sparse,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct NameScratchPlan {
    pub(crate) mode: NameScratchMode,
    pub(crate) reserved_bytes: usize,
}

// Keep in sync with top_contract_analysis_rs::analysis::scoring::PreparedNameQuery
// (rapidfuzz Jaro–Winkler BatchComparator + score_cutoff percent API).
pub(crate) struct PreparedNameQuery {
    scorer: jaro_winkler::BatchComparator<char>,
}

pub(crate) fn name_scratch_plan(
    atom_count: usize,
    threads: usize,
    available_bytes: usize,
) -> NameScratchPlan {
    let worker_count = threads.max(1).min(atom_count.saturating_sub(1));
    let dense_bytes = atom_count
        .saturating_mul(std::mem::size_of::<u32>())
        .saturating_mul(worker_count);
    if atom_count <= SPARSE_DEDUP_ATOM_THRESHOLD && dense_bytes <= available_bytes {
        NameScratchPlan {
            mode: NameScratchMode::Dense,
            reserved_bytes: dense_bytes,
        }
    } else {
        NameScratchPlan {
            mode: NameScratchMode::Sparse,
            reserved_bytes: 0,
        }
    }
}

impl NameCandidateScratch {
    #[cfg(test)]
    pub(crate) fn new(atom_count: usize) -> Self {
        let mode = if atom_count > SPARSE_DEDUP_ATOM_THRESHOLD {
            NameScratchMode::Sparse
        } else {
            NameScratchMode::Dense
        };
        Self::with_mode(atom_count, mode)
    }

    pub(crate) fn with_mode(atom_count: usize, mode: NameScratchMode) -> Self {
        let dedup = match mode {
            NameScratchMode::Dense => NameDedup::Dense {
                seen_generation: vec![0; atom_count],
                generation: 0,
            },
            NameScratchMode::Sparse => NameDedup::Sparse {
                seen: HashSet::new(),
            },
        };
        Self {
            candidates: Vec::new(),
            dedup,
        }
    }

    pub(crate) fn clear(&mut self) {
        self.candidates.clear();
        match &mut self.dedup {
            NameDedup::Dense {
                seen_generation,
                generation,
            } => {
                *generation = generation.wrapping_add(1);
                if *generation == 0 {
                    seen_generation.fill(0);
                    *generation = 1;
                }
            }
            NameDedup::Sparse { seen } => {
                seen.clear();
            }
        }
    }

    pub(crate) fn push_once(&mut self, atom_index: NameAtomIndex) {
        let novel = match &mut self.dedup {
            NameDedup::Dense {
                seen_generation,
                generation,
            } => {
                let slot = &mut seen_generation[atom_index as usize];
                if *slot == *generation {
                    false
                } else {
                    *slot = *generation;
                    true
                }
            }
            NameDedup::Sparse { seen } => seen.insert(atom_index),
        };
        if novel {
            self.candidates.push(atom_index);
        }
    }
}

impl NameCandidateIndex {
    pub(crate) fn new(atoms: &[NameAtom]) -> Self {
        let mut token_ids = HashMap::<(char, u32), NameTokenId>::new();
        let mut postings = Vec::<Vec<NameAtomIndex>>::new();
        let mut raw_documents = Vec::with_capacity(atoms.len());
        for (atom_index, atom) in atoms.iter().enumerate() {
            let compact_atom_index =
                u32::try_from(atom_index).expect("name atom index exceeds u32 indexes");
            let mut char_occurrences = HashMap::<char, u32>::new();
            let mut tokens = Vec::with_capacity(atom.char_len);
            for character in atom.name_norm.chars() {
                let occurrence = char_occurrences.entry(character).or_default();
                let token_key = (character, *occurrence);
                *occurrence += 1;
                let token_id = match token_ids.get(&token_key).copied() {
                    Some(token_id) => token_id,
                    None => {
                        let token_id = u32::try_from(token_ids.len())
                            .expect("name token dictionary exceeds u32 indexes");
                        token_ids.insert(token_key, token_id);
                        postings.push(Vec::new());
                        token_id
                    }
                };
                postings[token_id as usize].push(compact_atom_index);
                tokens.push(token_id);
            }
            raw_documents.push(tokens);
        }

        let documents = raw_documents
            .into_par_iter()
            .map(|tokens| {
                let mut prefix_tokens = tokens.clone();
                prefix_tokens.sort_unstable_by(|left, right| {
                    postings[*left as usize]
                        .len()
                        .cmp(&postings[*right as usize].len())
                        .then_with(|| left.cmp(right))
                });
                let mut sorted_tokens = tokens;
                sorted_tokens.sort_unstable();
                IndexedNameDocument {
                    prefix_tokens,
                    sorted_tokens,
                }
            })
            .collect::<Vec<_>>();
        Self {
            documents,
            postings,
        }
    }

    pub(crate) fn memory_bytes(&self) -> usize {
        self.documents
            .capacity()
            .saturating_mul(std::mem::size_of::<IndexedNameDocument>())
            .saturating_add(
                self.postings
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Vec<NameAtomIndex>>()),
            )
            .saturating_add(
                self.documents
            .iter()
            .map(|document| {
                document.prefix_tokens.capacity() * std::mem::size_of::<NameTokenId>()
                    + document.sorted_tokens.capacity() * std::mem::size_of::<NameTokenId>()
            })
                    .sum::<usize>(),
            )
            .saturating_add(
                self.postings
                    .iter()
                    .map(|posting| posting.capacity() * std::mem::size_of::<NameAtomIndex>())
                    .sum::<usize>(),
            )
    }

    pub(crate) fn candidates_for_left<'a>(
        &self,
        atoms: &[NameAtom],
        left: usize,
        right_min_len: Option<usize>,
        threshold: f64,
        scratch: &'a mut NameCandidateScratch,
    ) -> &'a [NameAtomIndex] {
        scratch.clear();
        // `right_min_len` is the char length of the shortest atom in the
        // caller's right-set (atoms are length-sorted, so the first in-range
        // right is the shortest). `minimum_name_char_overlap` is non-decreasing
        // in right_len for right_len >= left_len, so the shortest right yields
        // the minimum required char overlap across the whole right-set. This
        // two-sided bound is >= the old universal (left-only) bound, so the
        // prefix is no longer and the large common-character postings (sorted
        // last in `prefix_tokens`) are probed less often. It stays a valid
        // prefix-filter lower bound: every in-range true match has
        // `overlap >= required_overlap(left, right) >= this bound`.
        let Some(right_min_len) = right_min_len else {
            return &scratch.candidates;
        };
        let minimum_overlap =
            minimum_name_char_overlap(atoms[left].char_len, right_min_len, threshold);
        if minimum_overlap == 0 {
            for atom_index in 0..atoms.len() {
                if atom_index != left {
                    scratch.push_once(
                        u32::try_from(atom_index).expect("name atom index exceeds u32 indexes"),
                    );
                }
            }
        } else {
            let document = &self.documents[left];
            let prefix_len = document
                .prefix_tokens
                .len()
                .saturating_sub(minimum_overlap)
                .saturating_add(1)
                .min(document.prefix_tokens.len());
            for &token_id in &document.prefix_tokens[..prefix_len] {
                for &atom_index in &self.postings[token_id as usize] {
                    if atom_index as usize != left {
                        scratch.push_once(atom_index);
                    }
                }
            }
        }

        scratch.candidates.sort_unstable();
        let left_document = &self.documents[left].sorted_tokens;
        scratch.candidates.retain(|&right| {
            let right = right as usize;
            let required_overlap = minimum_name_char_overlap(
                atoms[left].char_len,
                atoms[right].char_len,
                threshold,
            );
            required_overlap <= atoms[left].char_len.min(atoms[right].char_len)
                && sorted_name_token_overlap(
                    left_document,
                    &self.documents[right].sorted_tokens,
                ) >= required_overlap
        });
        &scratch.candidates
    }
}

impl PreparedNameQuery {
    pub(crate) fn new(name: &str) -> Self {
        Self {
            scorer: jaro_winkler::BatchComparator::new(name.chars()),
        }
    }

    pub(crate) fn score_percent(&self, right: &str, threshold: f64) -> Option<f64> {
        if threshold.is_nan() || threshold > 100.0 {
            return None;
        }
        let args =
            jaro_winkler::Args::default().score_cutoff((threshold / 100.0).clamp(0.0, 1.0));
        self.scorer
            .normalized_similarity_with_args(right.chars(), &args)
            .map(|score| score * 100.0)
    }
}

pub(crate) fn union_full_name_pairs(
    atoms: &[NameAtom],
    candidate_index: &NameCandidateIndex,
    scratch_mode: NameScratchMode,
    states: &mut [ThresholdUnionState],
    chain_count: usize,
    progress: &ProgressTracker,
) {
    if atoms.len() < 2 || states.is_empty() {
        return;
    }
    let min_threshold = states
        .iter()
        .map(|state| state.threshold)
        .fold(f64::INFINITY, f64::min);

    let mut pending_progress = 0;
    for left_batch_start in (0..atoms.len() - 1).step_by(LEFT_SCORE_BATCH_SIZE) {
        let left_batch_end = (left_batch_start + LEFT_SCORE_BATCH_SIZE).min(atoms.len() - 1);
        let scored_lefts = (left_batch_start..left_batch_end)
            .into_par_iter()
            .map_init(
                || NameCandidateScratch::with_mode(atoms.len(), scratch_mode),
                |scratch, left| {
                    let right_end = right_name_range_end_for_left(atoms, left, min_threshold);
                    let matching_rights = score_indexed_name_pairs_for_left(
                        atoms,
                        candidate_index,
                        left,
                        left + 1..right_end,
                        min_threshold,
                        scratch,
                    );
                    (left, matching_rights)
                },
            )
            .collect::<Vec<_>>();
        for (left, matching_rights) in scored_lefts {
            apply_matching_name_pairs(atoms, states, left, &matching_rights, chain_count);
            pending_progress += 1;
            flush_chunk_progress(progress, &mut pending_progress);
        }
    }
    flush_remaining_progress(progress, &mut pending_progress);
}

pub(crate) fn right_name_range_end_for_left(atoms: &[NameAtom], left: usize, threshold: f64) -> usize {
    if left + 1 >= atoms.len() {
        return atoms.len();
    }

    let left_len = atoms[left].char_len;
    let mut low = left + 1;
    let mut high = atoms.len();
    while low < high {
        let middle = low + (high - low) / 2;
        if name_pair_lengths_can_reach_threshold(left_len, atoms[middle].char_len, threshold) {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    low
}

pub(crate) fn score_indexed_name_pairs_for_left(
    atoms: &[NameAtom],
    candidate_index: &NameCandidateIndex,
    left: usize,
    right_range: std::ops::Range<usize>,
    threshold: f64,
    scratch: &mut NameCandidateScratch,
) -> Vec<ScoredRight> {
    let query = PreparedNameQuery::new(&atoms[left].name_norm);
    let right_min_len = if right_range.is_empty() {
        None
    } else {
        Some(atoms[right_range.start].char_len)
    };
    candidate_index
        .candidates_for_left(atoms, left, right_min_len, threshold, scratch)
        .iter()
        .map(|&right| right as usize)
        .filter(|right| right_range.contains(right))
        .filter_map(|right| {
            let right_name = atoms[right].name_norm.as_str();
            query
                .score_percent(right_name, threshold)
                .map(|score| ScoredRight { right, score })
        })
        .collect()
}

#[cfg(test)]
pub(crate) fn score_name_pairs_for_left_chunk(
    atoms: &[NameAtom],
    left: usize,
    chunk_start: usize,
    chunk_end: usize,
    threshold: f64,
) -> Vec<ScoredRight> {
    let candidate_index = NameCandidateIndex::new(atoms);
    let mut scratch = NameCandidateScratch::new(atoms.len());
    score_indexed_name_pairs_for_left(
        atoms,
        &candidate_index,
        left,
        chunk_start..chunk_end,
        threshold,
        &mut scratch,
    )
}

pub(crate) fn minimum_name_char_overlap(left_len: usize, right_len: usize, threshold: f64) -> usize {
    if threshold.is_nan() || threshold > 100.0 {
        return left_len.min(right_len).saturating_add(1);
    }
    if threshold <= 0.0 {
        return 0;
    }
    let max_overlap = left_len.min(right_len);
    let mut low = 0usize;
    let mut high = max_overlap.saturating_add(1);
    while low < high {
        let middle = low + (high - low) / 2;
        if optimistic_jaro_winkler_from_overlap(left_len, right_len, middle) >= threshold {
            high = middle;
        } else {
            low = middle + 1;
        }
    }
    low
}

pub(crate) fn optimistic_jaro_winkler_from_overlap(
    left_len: usize,
    right_len: usize,
    overlap: usize,
) -> f64 {
    if left_len == 0 || right_len == 0 || overlap == 0 {
        return 0.0;
    }
    let overlap = overlap.min(left_len).min(right_len) as f64;
    let jaro =
        (overlap / left_len as f64 + overlap / right_len as f64 + 1.0) / 3.0;
    let prefix = overlap.min(left_len.min(right_len).min(4) as f64);
    let similarity = if jaro > 0.7 {
        jaro + 0.1 * prefix * (1.0 - jaro)
    } else {
        jaro
    };
    similarity.min(1.0) * 100.0
}

pub(crate) fn sorted_name_token_overlap(left: &[NameTokenId], right: &[NameTokenId]) -> usize {
    let mut left_index = 0usize;
    let mut right_index = 0usize;
    let mut overlap = 0usize;
    while left_index < left.len() && right_index < right.len() {
        match left[left_index].cmp(&right[right_index]) {
            std::cmp::Ordering::Equal => {
                overlap += 1;
                left_index += 1;
                right_index += 1;
            }
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
        }
    }
    overlap
}

#[cfg(test)]
pub(crate) fn name_pair_score_from_names(left_name: &str, right_name: &str) -> f64 {
    PreparedNameQuery::new(left_name)
        .score_percent(right_name, 0.0)
        .expect("zero cutoff must return a Jaro-Winkler score")
}

#[cfg(test)]
pub(crate) fn name_pair_can_reach_threshold(left_name: &str, right_name: &str, threshold: f64) -> bool {
    left_name == right_name
        || name_pair_lengths_can_reach_threshold(
            left_name.chars().count(),
            right_name.chars().count(),
            threshold,
        )
}

#[cfg(test)]
pub(crate) fn jaro_winkler_upper_bound(left_name: &str, right_name: &str) -> f64 {
    jaro_winkler_upper_bound_from_lengths(left_name.chars().count(), right_name.chars().count())
}

pub(crate) fn name_pair_lengths_can_reach_threshold(
    left_len: usize,
    right_len: usize,
    threshold: f64,
) -> bool {
    jaro_winkler_upper_bound_from_lengths(left_len, right_len) >= threshold
}

pub(crate) fn jaro_winkler_upper_bound_from_lengths(left_len: usize, right_len: usize) -> f64 {
    if left_len == 0 || right_len == 0 {
        return if left_len == right_len { 100.0 } else { 0.0 };
    }

    let shorter = left_len.min(right_len) as f64;
    let longer = left_len.max(right_len) as f64;
    let max_jaro = (1.0 + shorter / longer + 1.0) / 3.0;
    let max_prefix = left_len.min(right_len).min(4) as f64;
    let max_winkler = max_jaro + 0.1 * max_prefix * (1.0 - max_jaro);
    max_winkler.min(1.0) * 100.0
}

pub(crate) fn apply_matching_name_pairs(
    atoms: &[NameAtom],
    states: &mut [ThresholdUnionState],
    left: usize,
    matching_rights: &[ScoredRight],
    chain_count: usize,
) {
    let left_chain = atoms[left].chain_index;
    for hit in matching_rights {
        let right_chain = atoms[hit.right].chain_index;
        for state in states.iter_mut() {
            if hit.score < state.threshold {
                break;
            }
            if left_chain == right_chain {
                state.intra.union(left, hit.right);
            } else {
                if let Some(cross) = &mut state.cross {
                    cross.union(left, hit.right);
                }
                if let Some(matrix) = &mut state.chain_matrix {
                    let (primary_chain, secondary_chain) = if left_chain < right_chain {
                        (left_chain, right_chain)
                    } else {
                        (right_chain, left_chain)
                    };
                    let pair_index = chain_pair_index(primary_chain, secondary_chain, chain_count);
                    matrix[pair_index].union(left, hit.right);
                }
            }
        }
    }
}
