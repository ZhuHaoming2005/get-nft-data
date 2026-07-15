use super::*;

pub(crate) const NAME_EDGE_CHUNK_SIZE: usize = 8 * 1024;
const NAME_PROGRESS_LEFT_CHUNK: u64 = 64;
const SPARSE_HASH_ENTRY_BUDGET_BYTES: usize = 24;

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub(crate) struct NameScoringStats {
    pub(crate) candidate_pairs: u64,
    pub(crate) scored_pairs: u64,
    pub(crate) matched_pairs: u64,
}

impl NameScoringStats {
    fn merge(&mut self, other: Self) {
        self.candidate_pairs = self.candidate_pairs.saturating_add(other.candidate_pairs);
        self.scored_pairs = self.scored_pairs.saturating_add(other.scored_pairs);
        self.matched_pairs = self.matched_pairs.saturating_add(other.matched_pairs);
    }

    fn is_empty(self) -> bool {
        self.candidate_pairs == 0 && self.scored_pairs == 0 && self.matched_pairs == 0
    }
}

struct NameEdgeBatch {
    edges: Vec<(usize, ScoredRight)>,
    processed_lefts: u64,
    stats: NameScoringStats,
}

struct SequentialCanonicalScoreSpec<'a> {
    original_atoms: &'a [NameAtom],
    canonical: &'a CanonicalNameValues,
    candidate_index: &'a NameCandidateIndex,
    scratch_mode: NameScratchMode,
    chain_count: usize,
    threshold: f64,
}

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

#[derive(Clone, Copy, Debug)]
pub(crate) struct NameCandidateIndexEstimate {
    pub(crate) resident_bytes: usize,
    pub(crate) peak_build_bytes: usize,
}

/// Conservative capacity-based estimate used before allocating the candidate
/// index. Postings and hash-table allocations are intentionally overestimated
/// so the analysis budget remains a hard preflight guard at production scale.
pub(crate) fn estimate_name_candidate_index_bytes(
    atoms: &[NameAtom],
) -> NameCandidateIndexEstimate {
    let token_occurrences = atoms
        .iter()
        .map(|atom| atom.char_len)
        .fold(0usize, usize::saturating_add);
    let mut posting_lengths = HashMap::<(char, u32), usize>::new();
    let mut longest_distinct_name = 0usize;
    for atom in atoms {
        let mut occurrences = HashMap::<char, u32>::new();
        for character in atom.name_norm.chars() {
            let occurrence = occurrences.entry(character).or_default();
            let token_key = (character, *occurrence);
            *occurrence = occurrence.saturating_add(1);
            posting_lengths
                .entry(token_key)
                .and_modify(|length| *length = length.saturating_add(1))
                .or_insert(1);
        }
        longest_distinct_name = longest_distinct_name.max(occurrences.len());
    }
    let token_count = posting_lengths.len();
    let document_headers =
        pushed_vec_capacity(atoms.len()).saturating_mul(std::mem::size_of::<IndexedNameDocument>());
    let document_tokens = token_occurrences
        .saturating_mul(2)
        .saturating_mul(std::mem::size_of::<NameTokenId>());
    let posting_headers =
        pushed_vec_capacity(token_count).saturating_mul(std::mem::size_of::<Vec<NameAtomIndex>>());
    let posting_values = posting_lengths
        .values()
        .copied()
        .map(pushed_vec_capacity)
        .fold(0usize, usize::saturating_add)
        .saturating_mul(std::mem::size_of::<NameAtomIndex>());
    let resident_bytes = document_headers
        .saturating_add(document_tokens)
        .saturating_add(posting_headers)
        .saturating_add(posting_values);
    let build_only_bytes = token_count
        .saturating_mul(48)
        .saturating_add(
            atoms
                .len()
                .saturating_mul(std::mem::size_of::<Vec<NameTokenId>>()),
        )
        .saturating_add(longest_distinct_name.saturating_mul(32));
    NameCandidateIndexEstimate {
        resident_bytes,
        peak_build_bytes: resident_bytes.saturating_add(build_only_bytes),
    }
}

fn pushed_vec_capacity(length: usize) -> usize {
    if length == 0 {
        0
    } else {
        length.max(4).next_power_of_two()
    }
}

/// Per-worker scratch space for candidate generation. The preflight memory
/// plan chooses between a dense generation array (O(1) push and clear) and a
/// sparse `HashSet`; the decision is based on the configured budget and both
/// backends' conservative worst-case allocations, not an atom-count cutoff.
pub(crate) struct NameCandidateScratch {
    candidates: Vec<NameAtomIndex>,
    dedup: NameDedup,
}

pub(crate) enum NameDedup {
    Dense {
        seen_generation: Vec<u16>,
        generation: u16,
    },
    Sparse {
        seen: HashSet<NameAtomIndex>,
    },
}

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
    let candidate_bytes = pushed_vec_capacity(atom_count)
        .saturating_mul(std::mem::size_of::<NameAtomIndex>())
        .saturating_mul(worker_count);
    let edge_pipeline_bytes = NAME_EDGE_CHUNK_SIZE
        .saturating_mul(std::mem::size_of::<(usize, ScoredRight)>())
        // One batch can be actively filled, one can be executing, one can be
        // blocked in the bounded channel, and Rayon may already have created
        // the replacement batch for the worker.
        .saturating_mul(worker_count.saturating_mul(4));
    let common_bytes = candidate_bytes.saturating_add(edge_pipeline_bytes);
    let dense_bytes = common_bytes.saturating_add(
        atom_count
            .saturating_mul(std::mem::size_of::<u16>())
            .saturating_mul(worker_count),
    );
    let sparse_bytes = common_bytes.saturating_add(
        atom_count
            .saturating_mul(SPARSE_HASH_ENTRY_BUDGET_BYTES)
            .saturating_mul(worker_count),
    );
    if dense_bytes <= sparse_bytes && dense_bytes <= available_bytes {
        NameScratchPlan {
            mode: NameScratchMode::Dense,
            reserved_bytes: dense_bytes,
        }
    } else {
        NameScratchPlan {
            mode: NameScratchMode::Sparse,
            reserved_bytes: sparse_bytes,
        }
    }
}

impl NameCandidateScratch {
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
    #[cfg(test)]
    pub(crate) fn new(atoms: &[NameAtom]) -> Self {
        Self::new_with_progress(atoms, || {})
    }

    pub(crate) fn new_with_progress(
        atoms: &[NameAtom],
        on_unit_completed: impl Fn() + Sync,
    ) -> Self {
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
            on_unit_completed();
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
                let document = IndexedNameDocument {
                    prefix_tokens,
                    sorted_tokens,
                };
                on_unit_completed();
                document
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
        right_range: std::ops::Range<usize>,
        threshold: f64,
        scratch: &'a mut NameCandidateScratch,
    ) -> &'a [NameAtomIndex] {
        scratch.clear();
        let right_end = right_range.end.min(atoms.len());
        if right_range.start >= right_end {
            return &scratch.candidates;
        }
        let right_range = right_range.start..right_end;
        // The first atom in `right_range` is the shortest atom in the
        // caller's right-set (atoms are length-sorted, so the first in-range
        // right is the shortest). `minimum_name_char_overlap` is non-decreasing
        // in right_len for right_len >= left_len, so the shortest right yields
        // the minimum required char overlap across the whole right-set. This
        // two-sided bound is >= the old universal (left-only) bound, so the
        // prefix is no longer and the large common-character postings (sorted
        // last in `prefix_tokens`) are probed less often. It stays a valid
        // prefix-filter lower bound: every in-range true match has
        // `overlap >= required_overlap(left, right) >= this bound`.
        let right_min_len = atoms[right_range.start].char_len;
        let minimum_overlap =
            minimum_name_char_overlap(atoms[left].char_len, right_min_len, threshold);
        if minimum_overlap == 0 {
            for atom_index in right_range.clone() {
                if atom_index != left {
                    scratch.push_once(
                        u32::try_from(atom_index).expect("name atom index exceeds u32 indexes"),
                    );
                }
            }
        } else {
            let document = &self.documents[left];
            let compact_right_start =
                u32::try_from(right_range.start).expect("name candidate range exceeds u32 indexes");
            let compact_right_end =
                u32::try_from(right_range.end).expect("name candidate range exceeds u32 indexes");
            let prefix_len = document
                .prefix_tokens
                .len()
                .saturating_sub(minimum_overlap)
                .saturating_add(1)
                .min(document.prefix_tokens.len());
            for &token_id in &document.prefix_tokens[..prefix_len] {
                let posting = &self.postings[token_id as usize];
                let posting_start =
                    posting.partition_point(|&atom_index| atom_index < compact_right_start);
                let posting_end =
                    posting.partition_point(|&atom_index| atom_index < compact_right_end);
                for &atom_index in &posting[posting_start..posting_end] {
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
            let required_overlap =
                minimum_name_char_overlap(atoms[left].char_len, atoms[right].char_len, threshold);
            required_overlap <= atoms[left].char_len.min(atoms[right].char_len)
                && sorted_name_token_overlap(left_document, &self.documents[right].sorted_tokens)
                    >= required_overlap
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
        let args = jaro_winkler::Args::default().score_cutoff((threshold / 100.0).clamp(0.0, 1.0));
        self.scorer
            .normalized_similarity_with_args(right.chars(), &args)
            .map(|score| score * 100.0)
    }
}

pub(crate) fn union_canonical_name_pairs(
    original_atoms: &[NameAtom],
    canonical: &CanonicalNameValues,
    candidate_index: &NameCandidateIndex,
    scratch_mode: NameScratchMode,
    state: &mut ThresholdUnionState,
    chain_count: usize,
    progress: &ProgressTracker,
) -> NameScoringStats {
    if canonical.atoms.is_empty() {
        return NameScoringStats::default();
    }

    for members in &canonical.members {
        for (left_position, &left) in members.iter().enumerate() {
            for &right in &members[left_position + 1..] {
                apply_matching_name_pairs(
                    original_atoms,
                    state,
                    left,
                    &[ScoredRight {
                        right,
                        score: 100.0,
                    }],
                    chain_count,
                );
            }
        }
    }
    if canonical.atoms.len() < 2 {
        return NameScoringStats::default();
    }

    let min_threshold = state.threshold;
    if rayon::current_num_threads() == 1 {
        return score_canonical_names_sequential(
            SequentialCanonicalScoreSpec {
                original_atoms,
                canonical,
                candidate_index,
                scratch_mode,
                chain_count,
                threshold: min_threshold,
            },
            state,
            progress,
        );
    }

    let mut scoring_stats = NameScoringStats::default();
    let queue_capacity = rayon::current_num_threads().saturating_mul(2).max(1);
    let (sender, receiver) = std::sync::mpsc::sync_channel::<NameEdgeBatch>(queue_capacity);
    rayon::scope(|scope| {
        let producer = sender.clone();
        scope.spawn(move |_| {
            (0..canonical.atoms.len() - 1)
                .into_par_iter()
                .fold(
                    || {
                        (
                            NameCandidateScratch::with_mode(canonical.atoms.len(), scratch_mode),
                            Vec::<(usize, ScoredRight)>::with_capacity(NAME_EDGE_CHUNK_SIZE),
                            0u64,
                            NameScoringStats::default(),
                        )
                    },
                    |(mut scratch, mut edges, mut processed, mut stats), left| {
                        let right_end =
                            right_name_range_end_for_left(&canonical.atoms, left, min_threshold);
                        let left_stats = visit_indexed_name_pairs_for_left(
                            &canonical.atoms,
                            candidate_index,
                            left,
                            left + 1..right_end,
                            min_threshold,
                            &mut scratch,
                            |hit| {
                                edges.push((left, hit));
                                if edges.len() >= NAME_EDGE_CHUNK_SIZE {
                                    producer
                                        .send(NameEdgeBatch {
                                            edges: std::mem::replace(
                                                &mut edges,
                                                Vec::with_capacity(NAME_EDGE_CHUNK_SIZE),
                                            ),
                                            processed_lefts: processed,
                                            stats: NameScoringStats::default(),
                                        })
                                        .expect("name edge consumer must remain alive");
                                    processed = 0;
                                }
                            },
                        );
                        stats.merge(left_stats);
                        processed += 1;
                        if processed >= NAME_PROGRESS_LEFT_CHUNK && edges.is_empty() {
                            producer
                                .send(NameEdgeBatch {
                                    edges: Vec::new(),
                                    processed_lefts: processed,
                                    stats: NameScoringStats::default(),
                                })
                                .expect("name edge consumer must remain alive");
                            processed = 0;
                        }
                        (scratch, edges, processed, stats)
                    },
                )
                .for_each(|(_, edges, processed_lefts, stats)| {
                    if !edges.is_empty() || processed_lefts > 0 || !stats.is_empty() {
                        producer
                            .send(NameEdgeBatch {
                                edges,
                                processed_lefts,
                                stats,
                            })
                            .expect("name edge consumer must remain alive");
                    }
                });
            drop(producer);
        });
        drop(sender);
        for batch in receiver {
            apply_canonical_edge_batch(original_atoms, canonical, state, chain_count, batch.edges);
            scoring_stats.merge(batch.stats);
            progress.advance_task(
                batch.processed_lefts,
                ProgressCounters {
                    candidates: scoring_stats.candidate_pairs,
                    scored: scoring_stats.scored_pairs,
                    matched: scoring_stats.matched_pairs,
                    ..ProgressCounters::default()
                },
            );
        }
    });
    scoring_stats
}

fn apply_canonical_edge_batch(
    original_atoms: &[NameAtom],
    canonical: &CanonicalNameValues,
    state: &mut ThresholdUnionState,
    chain_count: usize,
    edges: Vec<(usize, ScoredRight)>,
) {
    for (canonical_left, matching) in edges {
        apply_canonical_edge(
            original_atoms,
            canonical,
            state,
            chain_count,
            canonical_left,
            matching,
        );
    }
}

fn apply_canonical_edge(
    original_atoms: &[NameAtom],
    canonical: &CanonicalNameValues,
    state: &mut ThresholdUnionState,
    chain_count: usize,
    canonical_left: usize,
    matching: ScoredRight,
) {
    for &original_left in &canonical.members[canonical_left] {
        for &original_right in &canonical.members[matching.right] {
            apply_matching_name_pairs(
                original_atoms,
                state,
                original_left,
                &[ScoredRight {
                    right: original_right,
                    score: matching.score,
                }],
                chain_count,
            );
        }
    }
}

fn score_canonical_names_sequential(
    spec: SequentialCanonicalScoreSpec<'_>,
    state: &mut ThresholdUnionState,
    progress: &ProgressTracker,
) -> NameScoringStats {
    let mut scratch =
        NameCandidateScratch::with_mode(spec.canonical.atoms.len(), spec.scratch_mode);
    let mut stats = NameScoringStats::default();
    for left in 0..spec.canonical.atoms.len() - 1 {
        let right_end = right_name_range_end_for_left(&spec.canonical.atoms, left, spec.threshold);
        let left_stats = visit_indexed_name_pairs_for_left(
            &spec.canonical.atoms,
            spec.candidate_index,
            left,
            left + 1..right_end,
            spec.threshold,
            &mut scratch,
            |hit| {
                apply_canonical_edge(
                    spec.original_atoms,
                    spec.canonical,
                    state,
                    spec.chain_count,
                    left,
                    hit,
                );
            },
        );
        stats.merge(left_stats);
        progress.advance_task(
            1,
            ProgressCounters {
                candidates: stats.candidate_pairs,
                scored: stats.scored_pairs,
                matched: stats.matched_pairs,
                ..ProgressCounters::default()
            },
        );
    }
    stats
}

pub(crate) fn right_name_range_end_for_left(
    atoms: &[NameAtom],
    left: usize,
    threshold: f64,
) -> usize {
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

fn visit_indexed_name_pairs_for_left(
    atoms: &[NameAtom],
    candidate_index: &NameCandidateIndex,
    left: usize,
    right_range: std::ops::Range<usize>,
    threshold: f64,
    scratch: &mut NameCandidateScratch,
    mut visit_match: impl FnMut(ScoredRight),
) -> NameScoringStats {
    let query = PreparedNameQuery::new(&atoms[left].name_norm);
    let mut scored_pairs = 0u64;
    let mut matched_pairs = 0u64;
    for right in candidate_index
        .candidates_for_left(atoms, left, right_range, threshold, scratch)
        .iter()
        .map(|&right| right as usize)
    {
        scored_pairs = scored_pairs.saturating_add(1);
        let right_name = atoms[right].name_norm.as_str();
        if let Some(score) = query.score_percent(right_name, threshold) {
            matched_pairs = matched_pairs.saturating_add(1);
            visit_match(ScoredRight { right, score });
        }
    }
    NameScoringStats {
        candidate_pairs: scored_pairs,
        scored_pairs,
        matched_pairs,
    }
}

pub(crate) fn minimum_name_char_overlap(
    left_len: usize,
    right_len: usize,
    threshold: f64,
) -> usize {
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
    let jaro = (overlap / left_len as f64 + overlap / right_len as f64 + 1.0) / 3.0;
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
    state: &mut ThresholdUnionState,
    left: usize,
    matching_rights: &[ScoredRight],
    chain_count: usize,
) {
    let left_chain = atoms[left].chain_index;
    for hit in matching_rights {
        let right_chain = atoms[hit.right].chain_index;
        if hit.score >= state.threshold {
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
