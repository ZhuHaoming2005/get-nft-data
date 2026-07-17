#![cfg_attr(not(test), allow(dead_code))]

use super::*;
use memmap2::{Mmap, MmapOptions};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

pub(crate) const NAME_EDGE_CHUNK_SIZE: usize = 8 * 1024;
const NAME_PROGRESS_LEFT_CHUNK: u64 = 4 * 1024;
const SPARSE_HASH_ENTRY_BUDGET_BYTES: usize = 24;
const NAME_INDEX_FIXED_ALLOCATION_HEADROOM_BYTES: usize = 64 * 1024;
const NAME_INDEX_PER_COLLECTION_HEADROOM_BYTES: usize = 8;
pub(crate) const EXTERNAL_POSTING_RECORD_BYTES: usize = 12;
const EXTERNAL_POSTING_HEAP_ENTRY_BYTES: usize = std::mem::size_of::<Reverse<(u32, usize)>>();
const EXTERNAL_POSTING_CURSOR_BYTES: usize = std::mem::size_of::<ExternalPostingCursor>();
const EXTERNAL_INDEX_MERGE_BUFFER_BUDGET_BYTES: usize = 256 * 1024 * 1024;
const EXTERNAL_INDEX_MIN_MERGE_BUFFER_BYTES: usize = 4 * 1024;
const EXTERNAL_INDEX_MAX_MERGE_BUFFER_BYTES: usize = 1024 * 1024;
const EXTERNAL_INDEX_MAX_MERGE_FAN_IN: usize = 128;
pub(crate) const EXTERNAL_INDEX_MIN_RECORDS_PER_RUN: usize = 4 * 1024;
const EXTERNAL_INDEX_MAX_RUN_BYTES: usize = 4 * 1024 * 1024 * 1024;
const UNICODE_SCALAR_SLOT_COUNT: usize = 0x11_0000;
const NAME_TOKEN_OCCURRENCE_BITS: u32 = 43;
const NAME_TOKEN_OCCURRENCE_MASK: u64 = (1u64 << NAME_TOKEN_OCCURRENCE_BITS) - 1;
pub(crate) const EXTERNAL_UNICODE_COUNTER_BYTES: usize =
    UNICODE_SCALAR_SLOT_COUNT * (std::mem::size_of::<u64>() + std::mem::size_of::<u16>());
pub(crate) const EXTERNAL_NAME_INDEX_RESIDENT_HEADROOM_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub(crate) struct NameScoringStats {
    pub(crate) candidate_pairs: u64,
    pub(crate) scored_pairs: u64,
    pub(crate) matched_pairs: u64,
    /// Number of original-member pairs represented by canonical exact/fuzzy
    /// matches. This preserves the old Cartesian expansion's logical work
    /// metric without executing that expansion.
    pub(crate) logical_member_pairs: u64,
    /// State-specific spanning union operations actually emitted across intra,
    /// global-cross, and chain-pair scopes.
    pub(crate) spanning_union_operations: u64,
}

impl NameScoringStats {
    fn merge(&mut self, other: Self) {
        self.candidate_pairs = self.candidate_pairs.saturating_add(other.candidate_pairs);
        self.scored_pairs = self.scored_pairs.saturating_add(other.scored_pairs);
        self.matched_pairs = self.matched_pairs.saturating_add(other.matched_pairs);
        self.logical_member_pairs = self
            .logical_member_pairs
            .saturating_add(other.logical_member_pairs);
        self.spanning_union_operations = self
            .spanning_union_operations
            .saturating_add(other.spanning_union_operations);
    }

    fn is_empty(self) -> bool {
        self.candidate_pairs == 0
            && self.scored_pairs == 0
            && self.matched_pairs == 0
            && self.logical_member_pairs == 0
            && self.spanning_union_operations == 0
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct NameUnionWork {
    logical_member_pairs: u64,
    spanning_union_operations: u64,
}

impl NameUnionWork {
    fn merge(&mut self, other: Self) {
        self.logical_member_pairs = self
            .logical_member_pairs
            .saturating_add(other.logical_member_pairs);
        self.spanning_union_operations = self
            .spanning_union_operations
            .saturating_add(other.spanning_union_operations);
    }
}

struct NameEdgeBatch {
    edges: Vec<(usize, ScoredRight)>,
    processed_lefts: u64,
    stats: NameScoringStats,
}

struct SequentialCanonicalScoreSpec<'a, A: NameAtomStore + ?Sized> {
    original_atoms: &'a A,
    canonical: &'a CanonicalNameValues,
    candidate_index: &'a NameCandidateIndex,
    right_range_ends: Option<&'a [u32]>,
    scratch_mode: NameScratchMode,
    chain_count: usize,
    threshold: f64,
}

use rapidfuzz::distance::jaro_winkler;

pub(crate) type NameTokenId = u32;
pub(crate) type NameAtomIndex = u32;
#[cfg(test)]
type ResidentNameCandidateParts<'a> = (
    &'a [u64],
    &'a [NameTokenId],
    &'a [NameTokenId],
    &'a [u64],
    &'a [NameAtomIndex],
);

pub(crate) struct NameCandidateIndex {
    storage: NameCandidateStorage,
}

enum NameCandidateStorage {
    Resident(ResidentNameCandidateIndex),
    External(ExternalNameCandidateIndex),
}

struct ResidentNameCandidateIndex {
    document_offsets: Box<[u64]>,
    prefix_tokens: Box<[NameTokenId]>,
    sorted_tokens: Box<[NameTokenId]>,
    posting_offsets: Box<[u64]>,
    posting_atoms: Box<[NameAtomIndex]>,
}

impl ResidentNameCandidateIndex {
    fn len(&self) -> usize {
        self.document_offsets.len().saturating_sub(1)
    }

    fn document_range(&self, index: usize) -> std::ops::Range<usize> {
        self.document_offsets[index] as usize..self.document_offsets[index + 1] as usize
    }

    fn prefix_tokens(&self, index: usize) -> &[NameTokenId] {
        &self.prefix_tokens[self.document_range(index)]
    }

    fn sorted_tokens(&self, index: usize) -> &[NameTokenId] {
        &self.sorted_tokens[self.document_range(index)]
    }

    fn posting(&self, token_id: NameTokenId) -> &[NameAtomIndex] {
        let token_id = token_id as usize;
        let range =
            self.posting_offsets[token_id] as usize..self.posting_offsets[token_id + 1] as usize;
        &self.posting_atoms[range]
    }

    fn memory_bytes(&self) -> usize {
        self.document_offsets
            .len()
            .saturating_mul(std::mem::size_of::<u64>())
            .saturating_add(
                self.prefix_tokens
                    .len()
                    .saturating_mul(std::mem::size_of::<NameTokenId>()),
            )
            .saturating_add(
                self.sorted_tokens
                    .len()
                    .saturating_mul(std::mem::size_of::<NameTokenId>()),
            )
            .saturating_add(
                self.posting_offsets
                    .len()
                    .saturating_mul(std::mem::size_of::<u64>()),
            )
            .saturating_add(
                self.posting_atoms
                    .len()
                    .saturating_mul(std::mem::size_of::<NameAtomIndex>()),
            )
    }
}

struct ExternalNameCandidateIndex {
    postings: Option<Mmap>,
    atom_count: usize,
    record_count: usize,
    backing_bytes: u64,
    _cleanup: NameCandidateSpillDirectory,
}

struct NameCandidateSpillDirectory {
    path: PathBuf,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ExternalPostingRecord {
    token_key: u64,
    atom_index: u32,
}

#[derive(Clone, Copy, Debug)]
struct ExternalPostingCursor {
    token_key: u64,
    next_record: usize,
    end_record: usize,
}

struct ExternalUnicodeCounter {
    counts: Vec<u64>,
    generations: Vec<u16>,
    generation: u16,
}

struct ExternalCandidateScratch {
    counter: ExternalUnicodeCounter,
    postings: Vec<ExternalPostingCursor>,
    heap: BinaryHeap<Reverse<(u32, usize)>>,
}

#[derive(Clone, Copy, Debug)]
struct ExternalBuildRange {
    atom_start: usize,
    atom_end: usize,
    token_count: usize,
}

#[derive(Clone, Copy)]
struct MemberChainGroup<'a> {
    chain_index: usize,
    members: &'a [u32],
}

struct MemberChainGroups<'a, A: NameAtomStore + ?Sized> {
    atoms: &'a A,
    members: &'a [u32],
    position: usize,
}

pub(crate) struct NameCandidateIndexBuildPlan {
    document_offsets: Vec<u64>,
    raw_tokens: Vec<NameTokenId>,
    posting_offsets: Vec<u64>,
    posting_atoms: Vec<NameAtomIndex>,
    estimate: NameCandidateIndexEstimate,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct NameCandidateIndexEstimate {
    pub(crate) resident_bytes: usize,
    pub(crate) peak_build_bytes: usize,
}

/// Fast conservative estimate used before allocating the candidate index.
///
/// `NameAtom::char_len` is computed from the normalized string while loading
/// and preserved by canonicalization. Reusing it avoids a second Unicode scan
/// and avoids inflating CJK/non-ASCII names to their UTF-8 byte length. Treating
/// every occurrence as a distinct token and posting still remains a hard
/// capacity guard. The exact token shape is computed once by
/// [`NameCandidateIndex::prepare_with_progress`] and retained for construction.
pub(crate) fn estimate_name_candidate_index_bytes<V: NameValueStore + ?Sized>(
    atoms: &V,
) -> NameCandidateIndexEstimate {
    let (token_occurrences, longest_distinct_name) =
        (0..atoms.len()).fold((0usize, 0usize), |(total, longest), index| {
            let token_capacity = atoms.char_len(index);
            (
                total.saturating_add(token_capacity),
                longest.max(token_capacity),
            )
        });
    // Before tokenization, every occurrence could introduce a distinct
    // (character, occurrence) token. Using T for K therefore upper-bounds
    // dictionary counts, posting offsets, and the exact posting CSR.
    let token_count = token_occurrences;
    name_candidate_index_estimate_from_shape(
        atoms.len(),
        token_occurrences,
        token_count,
        longest_distinct_name,
    )
}

fn name_candidate_index_estimate_from_shape(
    atom_count: usize,
    token_occurrences: usize,
    token_count: usize,
    longest_distinct_name: usize,
) -> NameCandidateIndexEstimate {
    let csr_offsets = atom_count
        .saturating_add(token_count)
        .saturating_add(2)
        .saturating_mul(std::mem::size_of::<u64>());
    // Prefix tokens, value-sorted tokens, and posting atoms each contain one
    // compact u32 per token occurrence.
    let csr_values = token_occurrences
        .saturating_mul(3)
        .saturating_mul(std::mem::size_of::<u32>());
    // The final index owns five contiguous allocations instead of A+K small
    // boxes. Retain fixed allocator/alignment headroom without charging a
    // fictitious per-document or per-token-list header.
    let allocation_headroom = 5usize
        .saturating_mul(NAME_INDEX_PER_COLLECTION_HEADROOM_BYTES)
        .saturating_add(NAME_INDEX_FIXED_ALLOCATION_HEADROOM_BYTES);
    let resident_bytes = csr_offsets
        .saturating_add(csr_values)
        .saturating_add(allocation_headroom);
    let document_offset_bytes = atom_count
        .saturating_add(1)
        .saturating_mul(std::mem::size_of::<u64>());
    let raw_token_bytes = token_occurrences.saturating_mul(std::mem::size_of::<NameTokenId>());
    let posting_count_capacity_bytes =
        pushed_vec_capacity(token_count).saturating_mul(std::mem::size_of::<u64>());
    let posting_offset_bytes = token_count
        .saturating_add(1)
        .saturating_mul(std::mem::size_of::<u64>());
    let tokenization_peak = document_offset_bytes
        .saturating_add(raw_token_bytes)
        // Compact u64 counts grow with the token dictionary.
        .saturating_add(posting_count_capacity_bytes)
        // Conservative hash-table buckets/control bytes for
        // HashMap<(char, occurrence), u32>.
        .saturating_add(token_count.saturating_mul(48))
        // Per-name occurrence map is cleared and reused; only its longest
        // retained capacity contributes.
        .saturating_add(longest_distinct_name.saturating_mul(32))
        .saturating_add(allocation_headroom);
    let posting_offsets_peak = document_offset_bytes
        .saturating_add(raw_token_bytes)
        .saturating_add(posting_count_capacity_bytes)
        .saturating_add(posting_offset_bytes)
        .saturating_add(allocation_headroom);
    let posting_fill_peak = document_offset_bytes
        .saturating_add(raw_token_bytes)
        .saturating_add(posting_offset_bytes)
        .saturating_add(token_count.saturating_mul(std::mem::size_of::<u64>()))
        .saturating_add(token_occurrences.saturating_mul(std::mem::size_of::<NameAtomIndex>()))
        .saturating_add(allocation_headroom);
    NameCandidateIndexEstimate {
        resident_bytes,
        peak_build_bytes: resident_bytes
            .max(tokenization_peak)
            .max(posting_offsets_peak)
            .max(posting_fill_peak),
    }
}

fn pushed_vec_capacity(length: usize) -> usize {
    if length == 0 {
        0
    } else {
        length
            .max(4)
            .checked_next_power_of_two()
            .unwrap_or(usize::MAX)
    }
}

fn compact_name_identity(value: usize, label: &str) -> Result<u32, AnalysisError> {
    u32::try_from(value).map_err(|_| {
        AnalysisError::InvalidData(format!("{label} exceeds compact u32 identity space"))
    })
}

/// Per-worker scratch space for candidate generation. The preflight memory
/// plan chooses between a dense generation array (O(1) push and clear) and a
/// sparse `HashSet`; the decision is based on the configured budget and both
/// backends' conservative worst-case allocations, not an atom-count cutoff.
pub(crate) struct NameCandidateScratch {
    candidates: Vec<NameAtomIndex>,
    dedup: NameDedup,
    external: Option<ExternalCandidateScratch>,
}

pub(crate) enum NameDedup {
    Dense {
        seen_generation: Vec<u16>,
        generation: u16,
    },
    Sparse {
        seen: HashSet<NameAtomIndex>,
    },
    Scan,
    ExternalMerge,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NameScratchMode {
    Dense,
    Sparse,
    Scan,
    ExternalMerge,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NameCandidateScratchProfile {
    Resident {
        atom_count: usize,
    },
    External {
        atom_count: usize,
        max_name_char_len: usize,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct NameScratchPlan {
    pub(crate) mode: NameScratchMode,
    pub(crate) requested_workers: usize,
    pub(crate) admitted_workers: usize,
    pub(crate) scratch_and_queue_bytes: usize,
    pub(crate) worker_stack_bytes: usize,
    pub(crate) reserved_bytes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct NameScoringExecution<'a> {
    pub(crate) scratch_mode: NameScratchMode,
    pub(crate) worker_count: usize,
    pub(crate) right_range_ends: Option<&'a [u32]>,
}

// Keep in sync with top_contract_analysis_rs::analysis::scoring::PreparedNameQuery
// (rapidfuzz Jaro–Winkler BatchComparator + score_cutoff percent API).
pub(crate) struct PreparedNameQuery {
    scorer: jaro_winkler::BatchComparator<char>,
}

#[cfg(test)]
pub(crate) fn name_scratch_plan(
    atom_count: usize,
    threads: usize,
    available_bytes: usize,
) -> NameScratchPlan {
    name_scratch_plan_for_profile(
        NameCandidateScratchProfile::Resident { atom_count },
        threads,
        available_bytes,
    )
}

pub(crate) fn name_scratch_plan_for_profile(
    profile: NameCandidateScratchProfile,
    threads: usize,
    available_bytes: usize,
) -> NameScratchPlan {
    if let NameCandidateScratchProfile::External {
        atom_count,
        max_name_char_len,
    } = profile
    {
        let requested_workers = threads.max(1).min(atom_count.saturating_sub(1).max(1));
        if atom_count < 2 {
            return NameScratchPlan {
                mode: NameScratchMode::ExternalMerge,
                requested_workers,
                admitted_workers: 1,
                scratch_and_queue_bytes: 0,
                worker_stack_bytes: 0,
                reserved_bytes: 0,
            };
        }
        let posting_capacity = pushed_vec_capacity(max_name_char_len);
        let external_bytes_per_worker = EXTERNAL_UNICODE_COUNTER_BYTES
            .saturating_add(posting_capacity.saturating_mul(EXTERNAL_POSTING_CURSOR_BYTES))
            .saturating_add(posting_capacity.saturating_mul(EXTERNAL_POSTING_HEAP_ENTRY_BYTES));
        return scratch_plan_for_mode(
            NameScratchMode::ExternalMerge,
            requested_workers,
            available_bytes,
            external_bytes_per_worker,
            edge_pipeline_bytes_per_worker(),
        )
        .unwrap_or(NameScratchPlan {
            mode: NameScratchMode::ExternalMerge,
            requested_workers,
            admitted_workers: 1,
            scratch_and_queue_bytes: external_bytes_per_worker,
            worker_stack_bytes: 0,
            reserved_bytes: external_bytes_per_worker,
        });
    }
    let NameCandidateScratchProfile::Resident { atom_count } = profile else {
        unreachable!("external profile returned above");
    };
    let left_count = atom_count.saturating_sub(1);
    let requested_workers = threads.max(1).min(left_count.max(1));
    if left_count == 0 {
        return NameScratchPlan {
            mode: NameScratchMode::Dense,
            requested_workers,
            admitted_workers: 1,
            scratch_and_queue_bytes: 0,
            worker_stack_bytes: 0,
            reserved_bytes: 0,
        };
    }

    let candidate_bytes_per_worker =
        pushed_vec_capacity(atom_count).saturating_mul(std::mem::size_of::<NameAtomIndex>());
    let dense_bytes_per_worker = candidate_bytes_per_worker
        .saturating_add(atom_count.saturating_mul(std::mem::size_of::<u16>()));
    let sparse_bytes_per_worker = candidate_bytes_per_worker
        .saturating_add(atom_count.saturating_mul(SPARSE_HASH_ENTRY_BUDGET_BYTES));
    let edge_pipeline_bytes_per_worker = edge_pipeline_bytes_per_worker();

    if let Some(plan) = scratch_plan_for_mode(
        NameScratchMode::Dense,
        requested_workers,
        available_bytes,
        dense_bytes_per_worker,
        edge_pipeline_bytes_per_worker,
    ) {
        return plan;
    }

    // Dense generations are both faster and strictly smaller under the
    // conservative worst-case model. Retain the sparse calculation as a
    // correctness guard if those representation costs ever change.
    if let Some(plan) = scratch_plan_for_mode(
        NameScratchMode::Sparse,
        requested_workers,
        available_bytes,
        sparse_bytes_per_worker,
        edge_pipeline_bytes_per_worker,
    ) {
        return plan;
    }

    // The resident index still makes a zero-linear-scratch exact path
    // possible: scan the bounded right range and apply the same token-overlap
    // predicate that follows prefix candidate generation. Prefer as many
    // parallel scan lanes as the edge queues and dedicated stacks admit; a
    // single caller-thread lane needs neither.
    scratch_plan_for_mode(
        NameScratchMode::Scan,
        requested_workers,
        available_bytes,
        0,
        edge_pipeline_bytes_per_worker,
    )
    .expect("a caller-thread scan lane has zero reserved bytes")
}

fn edge_pipeline_bytes_per_worker() -> usize {
    NAME_EDGE_CHUNK_SIZE
        .saturating_mul(std::mem::size_of::<(usize, ScoredRight)>())
        // One batch can be actively filled, one can be executing, one can be
        // blocked in the bounded channel, and Rayon may already have created
        // the replacement batch for the worker.
        .saturating_mul(4)
}

fn scratch_plan_for_mode(
    mode: NameScratchMode,
    requested_workers: usize,
    available_bytes: usize,
    scratch_bytes_per_worker: usize,
    edge_pipeline_bytes_per_worker: usize,
) -> Option<NameScratchPlan> {
    let single_scratch_bytes = scratch_bytes_per_worker;
    if single_scratch_bytes > available_bytes {
        return None;
    }

    let parallel_bytes_per_worker = scratch_bytes_per_worker
        .saturating_add(edge_pipeline_bytes_per_worker)
        .saturating_add(NAME_ANALYSIS_WORKER_STACK_BYTES);
    let admitted_workers = if requested_workers <= 1 || parallel_bytes_per_worker == 0 {
        1
    } else {
        (available_bytes / parallel_bytes_per_worker)
            .min(requested_workers)
            .max(1)
    };
    let (scratch_and_queue_bytes, worker_stack_bytes) = if admitted_workers == 1 {
        (single_scratch_bytes, 0)
    } else {
        (
            scratch_bytes_per_worker
                .saturating_add(edge_pipeline_bytes_per_worker)
                .saturating_mul(admitted_workers),
            NAME_ANALYSIS_WORKER_STACK_BYTES.saturating_mul(admitted_workers),
        )
    };
    let reserved_bytes = scratch_and_queue_bytes.saturating_add(worker_stack_bytes);
    debug_assert!(reserved_bytes <= available_bytes);
    Some(NameScratchPlan {
        mode,
        requested_workers,
        admitted_workers,
        scratch_and_queue_bytes,
        worker_stack_bytes,
        reserved_bytes,
    })
}

impl NameCandidateScratch {
    pub(crate) fn with_mode(atom_count: usize, mode: NameScratchMode) -> Self {
        let (dedup, external) = match mode {
            NameScratchMode::Dense => (
                NameDedup::Dense {
                    seen_generation: vec![0; atom_count],
                    generation: 0,
                },
                None,
            ),
            NameScratchMode::Sparse => (
                NameDedup::Sparse {
                    seen: HashSet::new(),
                },
                None,
            ),
            NameScratchMode::Scan => (NameDedup::Scan, None),
            NameScratchMode::ExternalMerge => (
                NameDedup::ExternalMerge,
                Some(ExternalCandidateScratch::new()),
            ),
        };
        Self {
            candidates: Vec::new(),
            dedup,
            external,
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
            NameDedup::Scan | NameDedup::ExternalMerge => {}
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
            NameDedup::Scan => {
                debug_assert!(false, "scan mode does not materialize candidates");
                false
            }
            NameDedup::ExternalMerge => {
                debug_assert!(false, "external merge mode streams candidates");
                false
            }
        };
        if novel {
            self.candidates.push(atom_index);
        }
    }

    fn is_scan(&self) -> bool {
        matches!(self.dedup, NameDedup::Scan)
    }

    fn is_external_merge(&self) -> bool {
        matches!(self.dedup, NameDedup::ExternalMerge)
    }
}

impl ExternalUnicodeCounter {
    fn new() -> Self {
        Self {
            counts: vec![0; UNICODE_SCALAR_SLOT_COUNT],
            generations: vec![0; UNICODE_SCALAR_SLOT_COUNT],
            generation: 0,
        }
    }

    fn begin(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        if self.generation == 0 {
            self.generations.fill(0);
            self.generation = 1;
        }
    }

    fn next_occurrence(&mut self, character: char) -> u64 {
        let slot = character as usize;
        if self.generations[slot] != self.generation {
            self.generations[slot] = self.generation;
            self.counts[slot] = 0;
        }
        let occurrence = self.counts[slot];
        self.counts[slot] = occurrence.saturating_add(1);
        occurrence
    }

    fn multiset_overlap(&mut self, left: &str, right: &str) -> usize {
        self.begin();
        for character in left.chars() {
            let slot = character as usize;
            if self.generations[slot] != self.generation {
                self.generations[slot] = self.generation;
                self.counts[slot] = 0;
            }
            self.counts[slot] = self.counts[slot].saturating_add(1);
        }
        let mut overlap = 0usize;
        for character in right.chars() {
            let slot = character as usize;
            if self.generations[slot] == self.generation && self.counts[slot] > 0 {
                self.counts[slot] -= 1;
                overlap = overlap.saturating_add(1);
            }
        }
        overlap
    }
}

impl ExternalCandidateScratch {
    fn new() -> Self {
        Self {
            counter: ExternalUnicodeCounter::new(),
            postings: Vec::new(),
            heap: BinaryHeap::new(),
        }
    }

    fn prepare_prefix(
        &mut self,
        index: &ExternalNameCandidateIndex,
        left_name: &str,
        prefix_len: usize,
        right_range: std::ops::Range<usize>,
    ) {
        self.postings.clear();
        self.heap.clear();
        self.counter.begin();
        for character in left_name.chars() {
            let occurrence = self.counter.next_occurrence(character);
            let token_key = external_name_token_key(character, occurrence);
            let (start_record, end_record) = index.token_record_range(token_key);
            self.postings.push(ExternalPostingCursor {
                token_key,
                next_record: start_record,
                end_record,
            });
        }
        self.postings.sort_unstable_by(|left, right| {
            left.end_record
                .saturating_sub(left.next_record)
                .cmp(&right.end_record.saturating_sub(right.next_record))
                .then_with(|| left.token_key.cmp(&right.token_key))
        });
        self.postings.truncate(prefix_len.min(self.postings.len()));

        let compact_start = u32::try_from(right_range.start).unwrap_or(u32::MAX);
        let compact_end = u32::try_from(right_range.end).unwrap_or(u32::MAX);
        for (cursor_index, posting) in self.postings.iter_mut().enumerate() {
            posting.next_record =
                index.lower_bound_atom(posting.next_record, posting.end_record, compact_start);
            posting.end_record =
                index.lower_bound_atom(posting.next_record, posting.end_record, compact_end);
            if posting.next_record < posting.end_record {
                self.heap.push(Reverse((
                    index.posting_atom(posting.next_record),
                    cursor_index,
                )));
                posting.next_record += 1;
            }
        }
    }

    fn pop_candidate(&mut self, index: &ExternalNameCandidateIndex) -> Option<u32> {
        let Reverse((atom_index, cursor_index)) = self.heap.pop()?;
        let posting = &mut self.postings[cursor_index];
        if posting.next_record < posting.end_record {
            self.heap.push(Reverse((
                index.posting_atom(posting.next_record),
                cursor_index,
            )));
            posting.next_record += 1;
        }
        Some(atom_index)
    }
}

impl NameCandidateSpillDirectory {
    fn create(root: &Path) -> Result<Self, AnalysisError> {
        static NONCE: AtomicU64 = AtomicU64::new(0);
        let parent = root.join("name-candidate-index");
        fs::create_dir_all(&parent)?;
        for _ in 0..1_000 {
            let nonce = NONCE.fetch_add(1, AtomicOrdering::Relaxed);
            let path = parent.join(format!("spill-{}-{nonce}", std::process::id()));
            match fs::create_dir(&path) {
                Ok(()) => return Ok(Self { path }),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(AnalysisError::InvalidData(
            "could not allocate a unique name candidate spill directory".into(),
        ))
    }
}

impl Drop for NameCandidateSpillDirectory {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.path) {
            eprintln!(
                "warning: could not remove name candidate spill directory {}: {error}",
                self.path.display()
            );
        }
    }
}

fn external_name_token_key(character: char, occurrence: u64) -> u64 {
    debug_assert!(occurrence <= NAME_TOKEN_OCCURRENCE_MASK);
    ((character as u64) << NAME_TOKEN_OCCURRENCE_BITS) | occurrence.min(NAME_TOKEN_OCCURRENCE_MASK)
}

impl NameCandidateIndex {
    #[cfg(test)]
    pub(crate) fn new<V: NameValueStore + ?Sized>(atoms: &V) -> Self {
        Self::new_with_progress(atoms, || {}).expect("test name index fits compact identities")
    }

    pub(crate) fn prepare_with_progress<V: NameValueStore + ?Sized>(
        atoms: &V,
        on_unit_completed: impl Fn() + Sync,
    ) -> Result<NameCandidateIndexBuildPlan, AnalysisError> {
        NameCandidateIndexBuildPlan::prepare_with_progress(atoms, on_unit_completed)
    }

    #[cfg(test)]
    pub(crate) fn new_with_progress<V: NameValueStore + ?Sized>(
        atoms: &V,
        on_unit_completed: impl Fn() + Sync,
    ) -> Result<Self, AnalysisError> {
        let plan = Self::prepare_with_progress(atoms, &on_unit_completed)?;
        plan.build_with_progress(on_unit_completed)
    }

    pub(crate) fn build_external_with_progress<V: NameValueStore + ?Sized>(
        atoms: &V,
        scratch_directory: &Path,
        threads: usize,
        available_build_bytes: usize,
        on_unit_completed: impl Fn() + Sync,
    ) -> Result<Self, AnalysisError> {
        let cleanup = NameCandidateSpillDirectory::create(scratch_directory)?;
        let total_tokens = (0..atoms.len()).try_fold(0usize, |total, index| {
            total.checked_add(atoms.char_len(index)).ok_or_else(|| {
                AnalysisError::InvalidData(
                    "name candidate token occurrence count exceeds usize".into(),
                )
            })
        })?;
        let requested_workers = threads.max(1).min(atoms.len().max(1));
        let fixed_workers = (available_build_bytes / EXTERNAL_UNICODE_COUNTER_BYTES)
            .max(1)
            .min(requested_workers);
        let fixed_bytes = fixed_workers.saturating_mul(EXTERNAL_UNICODE_COUNTER_BYTES);
        let record_budget = available_build_bytes.saturating_sub(fixed_bytes);
        let record_size = std::mem::size_of::<ExternalPostingRecord>();
        let target_records = (record_budget / fixed_workers.max(1) / record_size)
            .max(EXTERNAL_INDEX_MIN_RECORDS_PER_RUN)
            .min(EXTERNAL_INDEX_MAX_RUN_BYTES / record_size)
            .min(total_tokens.max(1));
        let ranges = external_build_ranges(atoms, target_records);
        let mut run_paths = Vec::with_capacity(ranges.len());
        if ranges.len() == 1 {
            let path = cleanup.path.join("run-0.bin");
            build_external_posting_run(atoms, ranges[0], &path, true, &on_unit_completed)?;
            run_paths.push(path);
        } else {
            for batch_start in (0..ranges.len()).step_by(fixed_workers.max(1)) {
                let batch_end = batch_start.saturating_add(fixed_workers).min(ranges.len());
                let batch_paths = ranges[batch_start..batch_end]
                    .par_iter()
                    .enumerate()
                    .map(|(offset, &range)| {
                        let run_index = batch_start + offset;
                        let path = cleanup.path.join(format!("run-{run_index}.bin"));
                        build_external_posting_run(atoms, range, &path, false, &on_unit_completed)?;
                        Ok::<_, AnalysisError>(path)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                run_paths.extend(batch_paths);
            }
        }

        let postings_path = cleanup.path.join("postings.bin");
        merge_external_posting_runs(&run_paths, &postings_path)?;
        for run_path in run_paths {
            if run_path != postings_path {
                match fs::remove_file(run_path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
            }
        }
        let postings_file = File::open(&postings_path)?;
        let backing_bytes = postings_file.metadata()?.len();
        if backing_bytes % EXTERNAL_POSTING_RECORD_BYTES as u64 != 0 {
            return Err(AnalysisError::InvalidData(
                "external name posting file has a truncated record".into(),
            ));
        }
        let record_count = usize::try_from(backing_bytes / EXTERNAL_POSTING_RECORD_BYTES as u64)
            .map_err(|_| {
                AnalysisError::InvalidData(
                    "external name posting record count exceeds usize".into(),
                )
            })?;
        let postings = if backing_bytes == 0 {
            None
        } else {
            // The file is immutable for the lifetime of the mapping and the
            // cleanup guard is declared after the mapping, so Windows also
            // unmaps it before recursive spill cleanup.
            Some(unsafe { MmapOptions::new().map(&postings_file)? })
        };
        Ok(Self {
            storage: NameCandidateStorage::External(ExternalNameCandidateIndex {
                postings,
                atom_count: atoms.len(),
                record_count,
                backing_bytes,
                _cleanup: cleanup,
            }),
        })
    }

    pub(crate) fn memory_bytes(&self) -> usize {
        match &self.storage {
            NameCandidateStorage::Resident(index) => index.memory_bytes(),
            NameCandidateStorage::External(_) => std::mem::size_of::<ExternalNameCandidateIndex>(),
        }
    }

    pub(crate) fn backing_bytes(&self) -> u64 {
        match &self.storage {
            NameCandidateStorage::Resident(_) => 0,
            NameCandidateStorage::External(index) => index.backing_bytes,
        }
    }

    pub(crate) fn len(&self) -> usize {
        match &self.storage {
            NameCandidateStorage::Resident(index) => index.len(),
            NameCandidateStorage::External(index) => index.atom_count,
        }
    }

    pub(crate) fn is_external(&self) -> bool {
        matches!(self.storage, NameCandidateStorage::External(_))
    }

    #[cfg(test)]
    pub(crate) fn resident_parts(&self) -> Option<ResidentNameCandidateParts<'_>> {
        match &self.storage {
            NameCandidateStorage::Resident(index) => Some((
                &index.document_offsets,
                &index.prefix_tokens,
                &index.sorted_tokens,
                &index.posting_offsets,
                &index.posting_atoms,
            )),
            NameCandidateStorage::External(_) => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn external_records_are_sorted(&self) -> bool {
        let NameCandidateStorage::External(index) = &self.storage else {
            return false;
        };
        (1..index.record_count).all(|record_index| {
            (
                index.posting_key(record_index - 1),
                index.posting_atom(record_index - 1),
            ) <= (
                index.posting_key(record_index),
                index.posting_atom(record_index),
            )
        })
    }

    pub(crate) fn candidates_for_left<'a, V: NameValueStore + ?Sized>(
        &self,
        atoms: &V,
        left: usize,
        right_range: std::ops::Range<usize>,
        threshold: f64,
        scratch: &'a mut NameCandidateScratch,
    ) -> &'a [NameAtomIndex] {
        debug_assert!(!scratch.is_scan() && !scratch.is_external_merge());
        scratch.clear();
        let NameCandidateStorage::Resident(index) = &self.storage else {
            return &scratch.candidates;
        };
        if left >= atoms.len() || left >= index.len() {
            return &scratch.candidates;
        }
        let right_end = right_range.end.min(atoms.len()).min(index.len());
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
        let right_min_len = atoms.char_len(right_range.start);
        let minimum_overlap =
            minimum_name_char_overlap(atoms.char_len(left), right_min_len, threshold);
        if minimum_overlap == 0 {
            for atom_index in right_range.clone() {
                if atom_index != left {
                    debug_assert!(u32::try_from(atom_index).is_ok());
                    scratch.push_once(atom_index as u32);
                }
            }
        } else {
            let prefix_tokens = index.prefix_tokens(left);
            debug_assert!(u32::try_from(right_range.end).is_ok());
            let compact_right_start = right_range.start as u32;
            let compact_right_end = right_range.end as u32;
            let prefix_len = prefix_tokens
                .len()
                .saturating_sub(minimum_overlap)
                .saturating_add(1)
                .min(prefix_tokens.len());
            for &token_id in &prefix_tokens[..prefix_len] {
                let posting = index.posting(token_id);
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
        scratch.candidates.retain(|&right| {
            resident_candidate_passes_overlap_filter(index, atoms, left, right as usize, threshold)
        });
        &scratch.candidates
    }

    fn candidate_passes_overlap_filter<V: NameValueStore + ?Sized>(
        &self,
        atoms: &V,
        left: usize,
        right: usize,
        threshold: f64,
    ) -> bool {
        let NameCandidateStorage::Resident(index) = &self.storage else {
            return false;
        };
        resident_candidate_passes_overlap_filter(index, atoms, left, right, threshold)
    }

    fn visit_external_candidates_for_left<V: NameValueStore + ?Sized>(
        &self,
        atoms: &V,
        left: usize,
        right_range: std::ops::Range<usize>,
        threshold: f64,
        scratch: &mut NameCandidateScratch,
        mut visit: impl FnMut(usize),
    ) {
        let NameCandidateStorage::External(index) = &self.storage else {
            return;
        };
        debug_assert!(scratch.is_external_merge());
        if left >= atoms.len() || left >= index.atom_count {
            return;
        }
        let right_end = right_range.end.min(atoms.len()).min(index.atom_count);
        let right_start = right_range.start.min(right_end);
        if right_start >= right_end {
            return;
        }
        let right_range = right_start..right_end;
        let minimum_overlap = minimum_name_char_overlap(
            atoms.char_len(left),
            atoms.char_len(right_range.start),
            threshold,
        );
        if minimum_overlap == 0 {
            for right in right_range {
                if right != left {
                    visit(right);
                }
            }
            return;
        }
        let prefix_len = atoms
            .char_len(left)
            .saturating_sub(minimum_overlap)
            .saturating_add(1)
            .min(atoms.char_len(left));
        let external = scratch
            .external
            .as_mut()
            .expect("external merge scratch must own its posting heap");
        external.prepare_prefix(index, atoms.normalized_name(left), prefix_len, right_range);
        let mut previous = None;
        while let Some(compact_right) = external.pop_candidate(index) {
            if previous == Some(compact_right) {
                continue;
            }
            previous = Some(compact_right);
            let right = compact_right as usize;
            if right == left {
                continue;
            }
            let required_overlap =
                minimum_name_char_overlap(atoms.char_len(left), atoms.char_len(right), threshold);
            if required_overlap <= atoms.char_len(left).min(atoms.char_len(right))
                && external
                    .counter
                    .multiset_overlap(atoms.normalized_name(left), atoms.normalized_name(right))
                    >= required_overlap
            {
                visit(right);
            }
        }
    }
}

fn resident_candidate_passes_overlap_filter<V: NameValueStore + ?Sized>(
    index: &ResidentNameCandidateIndex,
    atoms: &V,
    left: usize,
    right: usize,
    threshold: f64,
) -> bool {
    let required_overlap =
        minimum_name_char_overlap(atoms.char_len(left), atoms.char_len(right), threshold);
    required_overlap <= atoms.char_len(left).min(atoms.char_len(right))
        && sorted_name_token_overlap(index.sorted_tokens(left), index.sorted_tokens(right))
            >= required_overlap
}

impl ExternalNameCandidateIndex {
    fn posting_key(&self, record_index: usize) -> u64 {
        let offset = record_index.saturating_mul(EXTERNAL_POSTING_RECORD_BYTES);
        let bytes = &self
            .postings
            .as_ref()
            .expect("non-empty posting index must be mapped")
            [offset..offset + std::mem::size_of::<u64>()];
        u64::from_le_bytes(bytes.try_into().expect("posting key width is fixed"))
    }

    fn posting_atom(&self, record_index: usize) -> u32 {
        let offset = record_index
            .saturating_mul(EXTERNAL_POSTING_RECORD_BYTES)
            .saturating_add(std::mem::size_of::<u64>());
        let bytes = &self
            .postings
            .as_ref()
            .expect("non-empty posting index must be mapped")
            [offset..offset + std::mem::size_of::<u32>()];
        u32::from_le_bytes(bytes.try_into().expect("posting atom width is fixed"))
    }

    fn token_record_range(&self, token_key: u64) -> (usize, usize) {
        let start = self.lower_bound_token(token_key);
        let mut low = start;
        let mut high = self.record_count;
        while low < high {
            let middle = low + (high - low) / 2;
            if self.posting_key(middle) <= token_key {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        (start, low)
    }

    fn lower_bound_token(&self, token_key: u64) -> usize {
        let mut low = 0usize;
        let mut high = self.record_count;
        while low < high {
            let middle = low + (high - low) / 2;
            if self.posting_key(middle) < token_key {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        low
    }

    fn lower_bound_atom(&self, mut low: usize, mut high: usize, atom_index: u32) -> usize {
        while low < high {
            let middle = low + (high - low) / 2;
            if self.posting_atom(middle) < atom_index {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        low
    }
}

fn external_build_ranges<V: NameValueStore + ?Sized>(
    atoms: &V,
    target_records: usize,
) -> Vec<ExternalBuildRange> {
    if atoms.is_empty() {
        return Vec::new();
    }
    let target_records = target_records.max(1);
    let mut ranges = Vec::new();
    let mut atom_start = 0usize;
    let mut token_count = 0usize;
    for atom_index in 0..atoms.len() {
        if token_count > 0
            && token_count.saturating_add(atoms.char_len(atom_index)) > target_records
        {
            ranges.push(ExternalBuildRange {
                atom_start,
                atom_end: atom_index,
                token_count,
            });
            atom_start = atom_index;
            token_count = 0;
        }
        token_count = token_count.saturating_add(atoms.char_len(atom_index));
    }
    ranges.push(ExternalBuildRange {
        atom_start,
        atom_end: atoms.len(),
        token_count,
    });
    ranges
}

fn build_external_posting_run<V: NameValueStore + ?Sized>(
    atoms: &V,
    range: ExternalBuildRange,
    path: &Path,
    parallel_sort: bool,
    on_unit_completed: &(impl Fn() + Sync),
) -> Result<(), AnalysisError> {
    let mut records = Vec::with_capacity(range.token_count);
    let mut counter = ExternalUnicodeCounter::new();
    for atom_index in range.atom_start..range.atom_end {
        let compact_atom_index = compact_name_identity(atom_index, "name atom index")?;
        counter.begin();
        for character in atoms.normalized_name(atom_index).chars() {
            let occurrence = counter.next_occurrence(character);
            records.push(ExternalPostingRecord {
                token_key: external_name_token_key(character, occurrence),
                atom_index: compact_atom_index,
            });
        }
        on_unit_completed();
    }
    if parallel_sort {
        records.par_sort_unstable();
    } else {
        records.sort_unstable();
    }
    for _ in range.atom_start..range.atom_end {
        on_unit_completed();
    }
    let file = File::create(path)?;
    let mut writer = BufWriter::with_capacity(8 * 1024 * 1024, file);
    for record in records {
        write_external_posting_record(&mut writer, record)?;
    }
    writer.flush()?;
    Ok(())
}

fn write_external_posting_record(
    writer: &mut impl Write,
    record: ExternalPostingRecord,
) -> std::io::Result<()> {
    writer.write_all(&record.token_key.to_le_bytes())?;
    writer.write_all(&record.atom_index.to_le_bytes())
}

fn read_external_posting_record(
    reader: &mut impl Read,
) -> Result<Option<ExternalPostingRecord>, AnalysisError> {
    let mut bytes = [0u8; EXTERNAL_POSTING_RECORD_BYTES];
    let mut filled = 0usize;
    while filled < bytes.len() {
        match reader.read(&mut bytes[filled..])? {
            0 if filled == 0 => return Ok(None),
            0 => {
                return Err(AnalysisError::InvalidData(
                    "external name posting run has a truncated record".into(),
                ));
            }
            read => filled += read,
        }
    }
    Ok(Some(ExternalPostingRecord {
        token_key: u64::from_le_bytes(
            bytes[..8]
                .try_into()
                .expect("external posting key width is fixed"),
        ),
        atom_index: u32::from_le_bytes(
            bytes[8..]
                .try_into()
                .expect("external posting atom width is fixed"),
        ),
    }))
}

pub(crate) fn merge_external_posting_runs(
    run_paths: &[PathBuf],
    output_path: &Path,
) -> Result<(), AnalysisError> {
    if run_paths.is_empty() {
        File::create(output_path)?;
        return Ok(());
    }
    let merge_directory = output_path.parent().ok_or_else(|| {
        AnalysisError::InvalidData("external name posting output has no parent directory".into())
    })?;
    let mut current = run_paths.to_vec();
    let mut pass = 0usize;
    while current.len() > EXTERNAL_INDEX_MAX_MERGE_FAN_IN {
        let mut next = Vec::with_capacity(current.len().div_ceil(EXTERNAL_INDEX_MAX_MERGE_FAN_IN));
        for (group_index, group) in current.chunks(EXTERNAL_INDEX_MAX_MERGE_FAN_IN).enumerate() {
            let merged_path = merge_directory.join(format!("merge-{pass}-{group_index}.bin"));
            merge_external_posting_run_group(group, &merged_path)?;
            remove_external_posting_inputs(group, &merged_path)?;
            next.push(merged_path);
        }
        current = next;
        pass += 1;
    }
    merge_external_posting_run_group(&current, output_path)?;
    remove_external_posting_inputs(&current, output_path)
}

fn merge_external_posting_run_group(
    run_paths: &[PathBuf],
    output_path: &Path,
) -> Result<(), AnalysisError> {
    if run_paths.len() == 1 {
        fs::rename(&run_paths[0], output_path)?;
        return Ok(());
    }
    let buffer_bytes = (EXTERNAL_INDEX_MERGE_BUFFER_BUDGET_BYTES / run_paths.len().max(1)).clamp(
        EXTERNAL_INDEX_MIN_MERGE_BUFFER_BYTES,
        EXTERNAL_INDEX_MAX_MERGE_BUFFER_BYTES,
    );
    let mut readers = run_paths
        .iter()
        .map(|path| {
            File::open(path)
                .map(|file| BufReader::with_capacity(buffer_bytes, file))
                .map_err(AnalysisError::from)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut heap = BinaryHeap::<Reverse<(ExternalPostingRecord, usize)>>::new();
    for (run_index, reader) in readers.iter_mut().enumerate() {
        if let Some(record) = read_external_posting_record(reader)? {
            heap.push(Reverse((record, run_index)));
        }
    }
    let output = File::create(output_path)?;
    let mut writer = BufWriter::with_capacity(8 * 1024 * 1024, output);
    while let Some(Reverse((record, run_index))) = heap.pop() {
        write_external_posting_record(&mut writer, record)?;
        if let Some(next) = read_external_posting_record(&mut readers[run_index])? {
            heap.push(Reverse((next, run_index)));
        }
    }
    writer.flush()?;
    Ok(())
}

fn remove_external_posting_inputs(
    input_paths: &[PathBuf],
    output_path: &Path,
) -> Result<(), AnalysisError> {
    for input_path in input_paths {
        if input_path == output_path {
            continue;
        }
        match fs::remove_file(input_path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

impl NameCandidateIndexBuildPlan {
    fn prepare_with_progress<V: NameValueStore + ?Sized>(
        atoms: &V,
        on_unit_completed: impl Fn() + Sync,
    ) -> Result<Self, AnalysisError> {
        let token_occurrences = (0..atoms.len()).try_fold(0usize, |total, index| {
            total.checked_add(atoms.char_len(index)).ok_or_else(|| {
                AnalysisError::InvalidData(
                    "name candidate token occurrence count exceeds usize".into(),
                )
            })
        })?;
        let mut token_ids = HashMap::<(char, u32), NameTokenId>::new();
        let mut posting_counts = Vec::<u64>::new();
        let mut document_offsets = Vec::<u64>::new();
        document_offsets
            .try_reserve_exact(atoms.len().saturating_add(1))
            .map_err(|error| {
                AnalysisError::InvalidData(format!(
                    "could not reserve name document offsets: {error}"
                ))
            })?;
        document_offsets.push(0);
        let mut raw_tokens = Vec::<NameTokenId>::new();
        raw_tokens
            .try_reserve_exact(token_occurrences)
            .map_err(|error| {
                AnalysisError::InvalidData(format!(
                    "could not reserve continuous raw name tokens: {error}"
                ))
            })?;
        let mut char_occurrences = HashMap::<char, u32>::new();
        let mut longest_distinct_name = 0usize;
        for atom_index in 0..atoms.len() {
            char_occurrences.clear();
            char_occurrences
                .try_reserve(atoms.char_len(atom_index))
                .map_err(|error| {
                    AnalysisError::InvalidData(format!(
                        "could not reserve per-name occurrence dictionary: {error}"
                    ))
                })?;
            // char_len is computed from this exact normalized string by the
            // loader and survives canonicalization unchanged.
            for character in atoms.normalized_name(atom_index).chars() {
                let occurrence = char_occurrences.entry(character).or_default();
                let token_key = (character, *occurrence);
                *occurrence += 1;
                let token_id = match token_ids.get(&token_key).copied() {
                    Some(token_id) => token_id,
                    None => {
                        let token_id =
                            compact_name_identity(token_ids.len(), "name token dictionary")?;
                        token_ids.try_reserve(1).map_err(|error| {
                            AnalysisError::InvalidData(format!(
                                "could not grow name token dictionary: {error}"
                            ))
                        })?;
                        posting_counts.try_reserve(1).map_err(|error| {
                            AnalysisError::InvalidData(format!(
                                "could not grow name posting counts: {error}"
                            ))
                        })?;
                        token_ids.insert(token_key, token_id);
                        posting_counts.push(0);
                        token_id
                    }
                };
                posting_counts[token_id as usize] = posting_counts[token_id as usize]
                    .checked_add(1)
                    .ok_or_else(|| {
                        AnalysisError::InvalidData(
                            "one name token posting count exceeds u64".into(),
                        )
                    })?;
                if raw_tokens.len() >= token_occurrences {
                    return Err(AnalysisError::InvalidData(
                        "name character count exceeded the pre-sized token CSR".into(),
                    ));
                }
                raw_tokens.push(token_id);
            }
            longest_distinct_name = longest_distinct_name.max(char_occurrences.len());
            document_offsets.push(u64::try_from(raw_tokens.len()).map_err(|_| {
                AnalysisError::InvalidData("name document token offset exceeds u64".into())
            })?);
            on_unit_completed();
        }
        if raw_tokens.len() != token_occurrences {
            return Err(AnalysisError::InvalidData(format!(
                "name character counts changed while building candidates: expected={}, loaded={}",
                token_occurrences,
                raw_tokens.len()
            )));
        }

        let estimate = name_candidate_index_estimate_from_shape(
            atoms.len(),
            token_occurrences,
            posting_counts.len(),
            longest_distinct_name,
        );
        drop(token_ids);
        drop(char_occurrences);

        let mut posting_offsets = Vec::<u64>::new();
        posting_offsets
            .try_reserve_exact(posting_counts.len().saturating_add(1))
            .map_err(|error| {
                AnalysisError::InvalidData(format!(
                    "could not reserve name posting offsets: {error}"
                ))
            })?;
        posting_offsets.push(0);
        let mut posting_total = 0u64;
        for &count in &posting_counts {
            posting_total = posting_total.checked_add(count).ok_or_else(|| {
                AnalysisError::InvalidData("name posting entry total exceeds u64".into())
            })?;
            posting_offsets.push(posting_total);
        }
        if usize::try_from(posting_total).ok() != Some(token_occurrences) {
            return Err(AnalysisError::InvalidData(
                "name posting total differs from document token total".into(),
            ));
        }
        drop(posting_counts);

        let mut write_offsets = Vec::<u64>::new();
        write_offsets
            .try_reserve_exact(posting_offsets.len().saturating_sub(1))
            .map_err(|error| {
                AnalysisError::InvalidData(format!(
                    "could not reserve name posting write cursors: {error}"
                ))
            })?;
        write_offsets.extend_from_slice(&posting_offsets[..posting_offsets.len() - 1]);
        let mut posting_atoms = try_zeroed_name_ids(token_occurrences, "name posting-atom CSR")?;
        for atom_index in 0..atoms.len() {
            let compact_atom_index = compact_name_identity(atom_index, "name atom index")?;
            let start = usize::try_from(document_offsets[atom_index]).map_err(|_| {
                AnalysisError::InvalidData("name document offset exceeds usize".into())
            })?;
            let end = usize::try_from(document_offsets[atom_index + 1]).map_err(|_| {
                AnalysisError::InvalidData("name document offset exceeds usize".into())
            })?;
            for &token_id in &raw_tokens[start..end] {
                let cursor = &mut write_offsets[token_id as usize];
                let write_index = usize::try_from(*cursor).map_err(|_| {
                    AnalysisError::InvalidData("name posting offset exceeds usize".into())
                })?;
                posting_atoms[write_index] = compact_atom_index;
                *cursor = (*cursor).checked_add(1).ok_or_else(|| {
                    AnalysisError::InvalidData("name posting write offset exceeds u64".into())
                })?;
            }
        }
        debug_assert!(write_offsets
            .iter()
            .zip(&posting_offsets[1..])
            .all(|(written, expected)| written == expected));
        drop(write_offsets);

        Ok(Self {
            document_offsets,
            raw_tokens,
            posting_offsets,
            posting_atoms,
            estimate,
        })
    }

    pub(crate) fn estimate(&self) -> NameCandidateIndexEstimate {
        self.estimate
    }

    pub(crate) fn build_with_progress(
        self,
        on_unit_completed: impl Fn() + Sync,
    ) -> Result<NameCandidateIndex, AnalysisError> {
        let Self {
            document_offsets,
            mut raw_tokens,
            posting_offsets,
            posting_atoms,
            estimate: _,
        } = self;
        let mut prefix_tokens = try_zeroed_name_ids(raw_tokens.len(), "name prefix-token CSR")?;
        populate_name_document_csr(
            &mut raw_tokens,
            0,
            document_offsets.len().saturating_sub(1),
            &document_offsets,
            &mut prefix_tokens,
            &posting_offsets,
            &on_unit_completed,
        );

        Ok(NameCandidateIndex {
            storage: NameCandidateStorage::Resident(ResidentNameCandidateIndex {
                document_offsets: document_offsets.into_boxed_slice(),
                prefix_tokens: prefix_tokens.into_boxed_slice(),
                sorted_tokens: raw_tokens.into_boxed_slice(),
                posting_offsets: posting_offsets.into_boxed_slice(),
                posting_atoms: posting_atoms.into_boxed_slice(),
            }),
        })
    }
}

#[allow(clippy::slow_vector_initialization)]
fn try_zeroed_name_ids(len: usize, label: &str) -> Result<Vec<u32>, AnalysisError> {
    let mut values = Vec::new();
    values.try_reserve_exact(len).map_err(|error| {
        AnalysisError::InvalidData(format!("could not reserve {label} values: {error}"))
    })?;
    values.resize(len, 0);
    Ok(values)
}

fn populate_name_document_csr(
    sorted_tokens: &mut [NameTokenId],
    document_start: usize,
    document_count: usize,
    offsets: &[u64],
    prefix_tokens: &mut [NameTokenId],
    posting_offsets: &[u64],
    on_unit_completed: &(impl Fn() + Sync),
) {
    const PARALLEL_DOCUMENT_GRAIN: usize = 64;
    if document_count <= PARALLEL_DOCUMENT_GRAIN {
        let base_offset = offsets[document_start] as usize;
        for local_document in 0..document_count {
            let global_document = document_start + local_document;
            let token_start = offsets[global_document] as usize - base_offset;
            let token_end = offsets[global_document + 1] as usize - base_offset;
            let sorted = &mut sorted_tokens[token_start..token_end];
            let prefix = &mut prefix_tokens[token_start..token_end];
            prefix.copy_from_slice(sorted);
            prefix.sort_unstable_by(|left, right| {
                let left = *left as usize;
                let right = *right as usize;
                let left_len = posting_offsets[left + 1] - posting_offsets[left];
                let right_len = posting_offsets[right + 1] - posting_offsets[right];
                left_len.cmp(&right_len).then_with(|| left.cmp(&right))
            });
            sorted.sort_unstable();
            on_unit_completed();
        }
        return;
    }

    let document_mid = document_count / 2;
    let global_mid = document_start + document_mid;
    let left_token_count = (offsets[global_mid] - offsets[document_start]) as usize;
    let (left_sorted, right_sorted) = sorted_tokens.split_at_mut(left_token_count);
    let (left_prefix, right_prefix) = prefix_tokens.split_at_mut(left_token_count);
    rayon::join(
        || {
            populate_name_document_csr(
                left_sorted,
                document_start,
                document_mid,
                offsets,
                left_prefix,
                posting_offsets,
                on_unit_completed,
            )
        },
        || {
            populate_name_document_csr(
                right_sorted,
                global_mid,
                document_count - document_mid,
                offsets,
                right_prefix,
                posting_offsets,
                on_unit_completed,
            )
        },
    );
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

impl<'a, A: NameAtomStore + ?Sized> Iterator for MemberChainGroups<'a, A> {
    type Item = MemberChainGroup<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let start = self.position;
        let &first = self.members.get(start)?;
        let chain_index = self.atoms.chain_index(first as usize);
        let mut end = start + 1;
        while let Some(&member) = self.members.get(end) {
            if self.atoms.chain_index(member as usize) != chain_index {
                break;
            }
            end += 1;
        }
        self.position = end;
        Some(MemberChainGroup {
            chain_index,
            members: &self.members[start..end],
        })
    }
}

fn member_chain_groups<'a, A: NameAtomStore + ?Sized>(
    atoms: &'a A,
    members: &'a [u32],
) -> MemberChainGroups<'a, A> {
    debug_assert!(members.windows(2).all(|pair| {
        atoms.chain_index(pair[0] as usize) <= atoms.chain_index(pair[1] as usize)
    }));
    MemberChainGroups {
        atoms,
        members,
        position: 0,
    }
}

pub(crate) fn union_canonical_name_pairs<A: NameAtomStore + ?Sized>(
    original_atoms: &A,
    canonical: &CanonicalNameValues,
    candidate_index: &NameCandidateIndex,
    execution: NameScoringExecution<'_>,
    state: &mut ThresholdUnionState,
    chain_count: usize,
    progress: &ProgressTracker,
) -> NameScoringStats {
    if canonical.atoms.is_empty() {
        return NameScoringStats::default();
    }

    let mut scoring_stats = NameScoringStats::default();
    for members in &canonical.members {
        let work = connect_identical_canonical_members(original_atoms, members, state, chain_count);
        scoring_stats.logical_member_pairs = scoring_stats
            .logical_member_pairs
            .saturating_add(work.logical_member_pairs);
        scoring_stats.spanning_union_operations = scoring_stats
            .spanning_union_operations
            .saturating_add(work.spanning_union_operations);
    }
    if canonical.atoms.len() < 2 {
        return scoring_stats;
    }
    let right_range_ends = execution.right_range_ends;
    debug_assert!(
        right_range_ends.is_none_or(|ends| ends.len() == canonical.atoms.len().saturating_sub(1))
    );

    let min_threshold = state.threshold;
    if execution.worker_count <= 1 {
        let mut sequential = score_canonical_names_sequential(
            SequentialCanonicalScoreSpec {
                original_atoms,
                canonical,
                candidate_index,
                right_range_ends,
                scratch_mode: execution.scratch_mode,
                chain_count,
                threshold: min_threshold,
            },
            state,
            progress,
        );
        sequential.merge(scoring_stats);
        return sequential;
    }

    let queue_capacity = execution.worker_count.saturating_mul(2).max(1);
    let (sender, receiver) = std::sync::mpsc::sync_channel::<NameEdgeBatch>(queue_capacity);
    rayon::scope(|scope| {
        let producer = sender.clone();
        scope.spawn(move |_| {
            (0..canonical.atoms.len() - 1)
                .into_par_iter()
                .fold(
                    || {
                        (
                            NameCandidateScratch::with_mode(
                                canonical.atoms.len(),
                                execution.scratch_mode,
                            ),
                            Vec::<(usize, ScoredRight)>::with_capacity(NAME_EDGE_CHUNK_SIZE),
                            0u64,
                            NameScoringStats::default(),
                        )
                    },
                    |(mut scratch, mut edges, mut processed, mut stats), left| {
                        let right_end = right_range_ends.map_or_else(
                            || right_name_range_end_for_left(canonical, left, min_threshold),
                            |ends| ends[left] as usize,
                        );
                        let left_stats = visit_indexed_name_pairs_for_left(
                            canonical,
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
            let work = apply_canonical_edge_batch(
                original_atoms,
                canonical,
                state,
                chain_count,
                batch.edges,
            );
            scoring_stats.merge(batch.stats);
            scoring_stats.logical_member_pairs = scoring_stats
                .logical_member_pairs
                .saturating_add(work.logical_member_pairs);
            scoring_stats.spanning_union_operations = scoring_stats
                .spanning_union_operations
                .saturating_add(work.spanning_union_operations);
            progress.advance_task(
                batch.processed_lefts,
                ProgressCounters {
                    candidates: scoring_stats.candidate_pairs,
                    scored: scoring_stats.scored_pairs,
                    expanded: scoring_stats.logical_member_pairs,
                    matched: scoring_stats.matched_pairs,
                    ..ProgressCounters::default()
                },
            );
        }
    });
    scoring_stats
}

pub(crate) fn score_canonical_name_pairs_pairwise<A: NameAtomStore + ?Sized>(
    original_atoms: &A,
    canonical: &CanonicalNameValues,
    candidate_index: &NameCandidateIndex,
    execution: NameScoringExecution<'_>,
    state: &mut PairwiseNameState,
    chain_count: usize,
    progress: &ProgressTracker,
) -> NameScoringStats {
    if canonical.atoms.is_empty() {
        return NameScoringStats::default();
    }

    let mut scoring_stats = NameScoringStats::default();
    for members in &canonical.members {
        let work = record_identical_canonical_members_pairwise(
            original_atoms,
            members,
            state,
            chain_count,
        );
        scoring_stats.logical_member_pairs = scoring_stats
            .logical_member_pairs
            .saturating_add(work.logical_member_pairs);
        scoring_stats.spanning_union_operations = scoring_stats
            .spanning_union_operations
            .saturating_add(work.spanning_union_operations);
    }
    if canonical.atoms.len() < 2 {
        return scoring_stats;
    }

    let right_range_ends = execution.right_range_ends;
    debug_assert!(
        right_range_ends.is_none_or(|ends| ends.len() == canonical.atoms.len().saturating_sub(1))
    );
    let threshold = state.threshold;
    if execution.worker_count <= 1 {
        let mut scratch =
            NameCandidateScratch::with_mode(canonical.atoms.len(), execution.scratch_mode);
        let mut pending_progress = 0u64;
        for left in 0..canonical.atoms.len() - 1 {
            let right_end = right_range_ends.map_or_else(
                || right_name_range_end_for_left(canonical, left, threshold),
                |ends| ends[left] as usize,
            );
            let mut pair_work = NameUnionWork::default();
            let left_stats = visit_indexed_name_pairs_for_left(
                canonical,
                candidate_index,
                left,
                left + 1..right_end,
                threshold,
                &mut scratch,
                |hit| {
                    pair_work.merge(record_canonical_pairwise_match(
                        original_atoms,
                        canonical,
                        state,
                        chain_count,
                        left,
                        hit,
                    ));
                },
            );
            scoring_stats.merge(left_stats);
            scoring_stats.logical_member_pairs = scoring_stats
                .logical_member_pairs
                .saturating_add(pair_work.logical_member_pairs);
            scoring_stats.spanning_union_operations = scoring_stats
                .spanning_union_operations
                .saturating_add(pair_work.spanning_union_operations);
            pending_progress = pending_progress.saturating_add(1);
            if pending_progress >= NAME_PROGRESS_LEFT_CHUNK || left + 2 == canonical.atoms.len() {
                progress.advance_task(
                    pending_progress,
                    ProgressCounters {
                        candidates: scoring_stats.candidate_pairs,
                        scored: scoring_stats.scored_pairs,
                        expanded: scoring_stats.logical_member_pairs,
                        matched: scoring_stats.matched_pairs,
                        ..ProgressCounters::default()
                    },
                );
                pending_progress = 0;
            }
        }
        return scoring_stats;
    }

    let queue_capacity = execution.worker_count.saturating_mul(2).max(1);
    let (sender, receiver) = std::sync::mpsc::sync_channel::<NameEdgeBatch>(queue_capacity);
    rayon::scope(|scope| {
        let producer = sender.clone();
        scope.spawn(move |_| {
            (0..canonical.atoms.len() - 1)
                .into_par_iter()
                .fold(
                    || {
                        (
                            NameCandidateScratch::with_mode(
                                canonical.atoms.len(),
                                execution.scratch_mode,
                            ),
                            Vec::<(usize, ScoredRight)>::with_capacity(NAME_EDGE_CHUNK_SIZE),
                            0u64,
                            NameScoringStats::default(),
                        )
                    },
                    |(mut scratch, mut edges, mut processed, mut stats), left| {
                        let right_end = right_range_ends.map_or_else(
                            || right_name_range_end_for_left(canonical, left, threshold),
                            |ends| ends[left] as usize,
                        );
                        let left_stats = visit_indexed_name_pairs_for_left(
                            canonical,
                            candidate_index,
                            left,
                            left + 1..right_end,
                            threshold,
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
                                        .expect("name pair consumer must remain alive");
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
                                .expect("name pair consumer must remain alive");
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
                            .expect("name pair consumer must remain alive");
                    }
                });
            drop(producer);
        });
        drop(sender);
        for batch in receiver {
            let mut pair_work = NameUnionWork::default();
            for (left, hit) in batch.edges {
                pair_work.merge(record_canonical_pairwise_match(
                    original_atoms,
                    canonical,
                    state,
                    chain_count,
                    left,
                    hit,
                ));
            }
            scoring_stats.merge(batch.stats);
            scoring_stats.logical_member_pairs = scoring_stats
                .logical_member_pairs
                .saturating_add(pair_work.logical_member_pairs);
            scoring_stats.spanning_union_operations = scoring_stats
                .spanning_union_operations
                .saturating_add(pair_work.spanning_union_operations);
            progress.advance_task(
                batch.processed_lefts,
                ProgressCounters {
                    candidates: scoring_stats.candidate_pairs,
                    scored: scoring_stats.scored_pairs,
                    expanded: scoring_stats.logical_member_pairs,
                    matched: scoring_stats.matched_pairs,
                    ..ProgressCounters::default()
                },
            );
        }
    });
    scoring_stats
}

fn record_identical_canonical_members_pairwise<A: NameAtomStore + ?Sized>(
    atoms: &A,
    members: &[u32],
    state: &mut PairwiseNameState,
    chain_count: usize,
) -> NameUnionWork {
    let mut work = NameUnionWork::default();
    for &member in members {
        let atom = member as usize;
        let contracts = atoms.contract_count(atom).max(0);
        let pairs = contracts.saturating_mul(contracts.saturating_sub(1)) / 2;
        if pairs > 0 {
            state.mark_intra(atoms.chain_index(atom), &[atom], pairs);
            work.spanning_union_operations = work.spanning_union_operations.saturating_add(1);
        }
    }
    for left_index in 0..members.len() {
        for right_index in left_index + 1..members.len() {
            work.logical_member_pairs = work.logical_member_pairs.saturating_add(1);
            record_pairwise_atom_match(
                atoms,
                state,
                members[left_index] as usize,
                members[right_index] as usize,
                chain_count,
            );
            work.spanning_union_operations = work.spanning_union_operations.saturating_add(1);
        }
    }
    work
}

fn record_canonical_pairwise_match<A: NameAtomStore + ?Sized>(
    atoms: &A,
    canonical: &CanonicalNameValues,
    state: &mut PairwiseNameState,
    chain_count: usize,
    canonical_left: usize,
    matching: ScoredRight,
) -> NameUnionWork {
    debug_assert!(matching.score >= state.threshold);
    let left_members = &canonical.members[canonical_left];
    let right_members = &canonical.members[matching.right];
    let mut work = NameUnionWork {
        logical_member_pairs: (left_members.len() as u64)
            .saturating_mul(right_members.len() as u64),
        spanning_union_operations: 0,
    };
    for &left in left_members {
        for &right in right_members {
            record_pairwise_atom_match(atoms, state, left as usize, right as usize, chain_count);
            work.spanning_union_operations = work.spanning_union_operations.saturating_add(1);
        }
    }
    work
}

fn record_pairwise_atom_match<A: NameAtomStore + ?Sized>(
    atoms: &A,
    state: &mut PairwiseNameState,
    left: usize,
    right: usize,
    chain_count: usize,
) {
    let left_chain = atoms.chain_index(left);
    let right_chain = atoms.chain_index(right);
    let pairs = atoms
        .contract_count(left)
        .max(0)
        .saturating_mul(atoms.contract_count(right).max(0));
    if pairs == 0 {
        return;
    }
    if left_chain == right_chain {
        state.mark_intra(left_chain, &[left, right], pairs);
        return;
    }
    let (first_chain, second_chain) = if left_chain < right_chain {
        (left_chain, right_chain)
    } else {
        (right_chain, left_chain)
    };
    let pair_index = chain_pair_index(first_chain, second_chain, chain_count);
    state.mark_cross(left_chain, right_chain, left, right, pair_index, pairs);
}

fn apply_canonical_edge_batch<A: NameAtomStore + ?Sized>(
    original_atoms: &A,
    canonical: &CanonicalNameValues,
    state: &mut ThresholdUnionState,
    chain_count: usize,
    edges: Vec<(usize, ScoredRight)>,
) -> NameUnionWork {
    let mut work = NameUnionWork::default();
    for (canonical_left, matching) in edges {
        work.merge(apply_canonical_edge(
            original_atoms,
            canonical,
            state,
            chain_count,
            canonical_left,
            matching,
        ));
    }
    work
}

fn apply_canonical_edge<A: NameAtomStore + ?Sized>(
    original_atoms: &A,
    canonical: &CanonicalNameValues,
    state: &mut ThresholdUnionState,
    chain_count: usize,
    canonical_left: usize,
    matching: ScoredRight,
) -> NameUnionWork {
    debug_assert!(matching.score >= state.threshold);
    let left_members = &canonical.members[canonical_left];
    let right_members = &canonical.members[matching.right];
    let mut work = NameUnionWork {
        logical_member_pairs: (left_members.len() as u64)
            .saturating_mul(right_members.len() as u64),
        spanning_union_operations: 0,
    };

    // Exact-name preprocessing has already connected every same-chain bucket
    // inside each canonical value. A single bridge per shared chain therefore
    // preserves the complete bipartite intra-chain expansion.
    let mut left_groups = member_chain_groups(original_atoms, left_members).peekable();
    let mut right_groups = member_chain_groups(original_atoms, right_members).peekable();
    while let (Some(left), Some(right)) = (left_groups.peek(), right_groups.peek()) {
        match left.chain_index.cmp(&right.chain_index) {
            std::cmp::Ordering::Less => {
                left_groups.next();
            }
            std::cmp::Ordering::Greater => {
                right_groups.next();
            }
            std::cmp::Ordering::Equal => {
                work.spanning_union_operations =
                    work.spanning_union_operations
                        .saturating_add(union_intra_edge(
                            state,
                            left.members[0] as usize,
                            right.members[0] as usize,
                        ));
                left_groups.next();
                right_groups.next();
            }
        }
    }

    work.spanning_union_operations =
        work.spanning_union_operations
            .saturating_add(connect_cross_bipartite_spanning(
                original_atoms,
                left_members,
                right_members,
                state,
            ));

    if state.chain_matrix.is_some() {
        for left_group in member_chain_groups(original_atoms, left_members) {
            for right_group in member_chain_groups(original_atoms, right_members) {
                if left_group.chain_index == right_group.chain_index {
                    continue;
                }
                work.spanning_union_operations = work.spanning_union_operations.saturating_add(
                    connect_matrix_bipartite_spanning(
                        original_atoms,
                        left_group,
                        right_group,
                        state,
                        chain_count,
                    ),
                );
            }
        }
    }
    work
}

fn connect_identical_canonical_members<A: NameAtomStore + ?Sized>(
    atoms: &A,
    members: &[u32],
    state: &mut ThresholdUnionState,
    chain_count: usize,
) -> NameUnionWork {
    let mut work = NameUnionWork {
        logical_member_pairs: (members.len() as u64)
            .saturating_mul(members.len().saturating_sub(1) as u64)
            / 2,
        spanning_union_operations: 0,
    };
    if members.len() < 2 {
        return work;
    }

    // Replace each same-chain clique with one star.
    for group in member_chain_groups(atoms, members) {
        let anchor = group.members[0] as usize;
        for &member in &group.members[1..] {
            work.spanning_union_operations = work
                .spanning_union_operations
                .saturating_add(union_intra_edge(state, anchor, member as usize));
        }
    }

    work.spanning_union_operations = work
        .spanning_union_operations
        .saturating_add(connect_cross_multipartite_spanning(atoms, members, state));

    // Each chain-pair scope sees a complete bipartite graph. A bipartite
    // spanning tree is exact and emits |left| + |right| - 1 edges.
    if state.chain_matrix.is_some() {
        for left_group in member_chain_groups(atoms, members) {
            for right_group in member_chain_groups(atoms, members) {
                if left_group.chain_index >= right_group.chain_index {
                    continue;
                }
                work.spanning_union_operations = work.spanning_union_operations.saturating_add(
                    connect_matrix_bipartite_spanning(
                        atoms,
                        left_group,
                        right_group,
                        state,
                        chain_count,
                    ),
                );
            }
        }
    }
    work
}

fn connect_cross_multipartite_spanning<A: NameAtomStore + ?Sized>(
    atoms: &A,
    members: &[u32],
    state: &mut ThresholdUnionState,
) -> u64 {
    if state.cross.is_none() {
        return 0;
    }
    let Some((&first, rest)) = members.split_first() else {
        return 0;
    };
    let first = first as usize;
    let first_chain = atoms.chain_index(first);
    let Some(&second) = rest
        .iter()
        .find(|&&member| atoms.chain_index(member as usize) != first_chain)
    else {
        return 0;
    };
    let second = second as usize;
    let mut operations = union_cross_edge(state, first, second);
    for &member in rest {
        let member = member as usize;
        if member == second {
            continue;
        }
        let anchor = if atoms.chain_index(member) == first_chain {
            second
        } else {
            first
        };
        operations = operations.saturating_add(union_cross_edge(state, anchor, member));
    }
    operations
}

/// Emit a spanning forest for the colored bipartite graph whose allowed edges
/// are exactly `(left_chain != right_chain)`. The construction uses at most
/// `left.len() + right.len() - 1` edges and preserves isolated same-color
/// vertices and the two-component two-color case exactly.
pub(crate) fn connect_cross_bipartite_spanning<A: NameAtomStore + ?Sized>(
    atoms: &A,
    left_members: &[u32],
    right_members: &[u32],
    state: &mut ThresholdUnionState,
) -> u64 {
    if state.cross.is_none() || left_members.is_empty() || right_members.is_empty() {
        return 0;
    }
    let first_left = left_members[0] as usize;
    let first_right = right_members[0] as usize;
    let anchor = right_members
        .iter()
        .copied()
        .find(|&right| atoms.chain_index(first_left) != atoms.chain_index(right as usize))
        .map(|right| (first_left, right as usize))
        .or_else(|| {
            left_members.iter().copied().find_map(|left| {
                let left = left as usize;
                (atoms.chain_index(left) != atoms.chain_index(first_right))
                    .then_some((left, first_right))
            })
        });
    let Some((left_anchor, right_anchor)) = anchor else {
        return 0;
    };
    let left_anchor_chain = atoms.chain_index(left_anchor);
    let right_anchor_chain = atoms.chain_index(right_anchor);
    let mut operations = union_cross_edge(state, left_anchor, right_anchor);

    for &left in left_members {
        let left = left as usize;
        if left != left_anchor && atoms.chain_index(left) != right_anchor_chain {
            operations = operations.saturating_add(union_cross_edge(state, left, right_anchor));
        }
    }
    for &right in right_members {
        let right = right as usize;
        if right != right_anchor && atoms.chain_index(right) != left_anchor_chain {
            operations = operations.saturating_add(union_cross_edge(state, left_anchor, right));
        }
    }

    let right_alternative = right_members
        .iter()
        .copied()
        .find_map(|right| {
            let right = right as usize;
            let chain = atoms.chain_index(right);
            (chain != right_anchor_chain && chain != left_anchor_chain).then_some(right)
        })
        .or_else(|| {
            right_members.iter().copied().find_map(|right| {
                let right = right as usize;
                (atoms.chain_index(right) != right_anchor_chain).then_some(right)
            })
        });
    if let Some(right_alternative) = right_alternative {
        for &left in left_members {
            let left = left as usize;
            if atoms.chain_index(left) == right_anchor_chain {
                operations =
                    operations.saturating_add(union_cross_edge(state, left, right_alternative));
            }
        }
    }

    let left_alternative = left_members
        .iter()
        .copied()
        .find_map(|left| {
            let left = left as usize;
            let chain = atoms.chain_index(left);
            (chain != left_anchor_chain && chain != right_anchor_chain).then_some(left)
        })
        .or_else(|| {
            left_members.iter().copied().find_map(|left| {
                let left = left as usize;
                (atoms.chain_index(left) != left_anchor_chain).then_some(left)
            })
        });
    if let Some(left_alternative) = left_alternative {
        for &right in right_members {
            let right = right as usize;
            if atoms.chain_index(right) != left_anchor_chain {
                continue;
            }
            let duplicate_secondary_anchor = right_alternative == Some(right)
                && atoms.chain_index(left_alternative) == right_anchor_chain;
            if !duplicate_secondary_anchor {
                operations =
                    operations.saturating_add(union_cross_edge(state, left_alternative, right));
            }
        }
    }

    operations
}

fn connect_matrix_bipartite_spanning<A: NameAtomStore + ?Sized>(
    atoms: &A,
    left_group: MemberChainGroup<'_>,
    right_group: MemberChainGroup<'_>,
    state: &mut ThresholdUnionState,
    chain_count: usize,
) -> u64 {
    debug_assert_ne!(left_group.chain_index, right_group.chain_index);
    let left_anchor = left_group.members[0] as usize;
    let right_anchor = right_group.members[0] as usize;
    let mut operations = union_matrix_edge(atoms, state, left_anchor, right_anchor, chain_count);
    for &left in &left_group.members[1..] {
        operations = operations.saturating_add(union_matrix_edge(
            atoms,
            state,
            left as usize,
            right_anchor,
            chain_count,
        ));
    }
    for &right in &right_group.members[1..] {
        operations = operations.saturating_add(union_matrix_edge(
            atoms,
            state,
            left_anchor,
            right as usize,
            chain_count,
        ));
    }
    operations
}

fn union_intra_edge(state: &mut ThresholdUnionState, left: usize, right: usize) -> u64 {
    state.intra.union(left, right);
    1
}

fn union_cross_edge(state: &mut ThresholdUnionState, left: usize, right: usize) -> u64 {
    let Some(cross) = &mut state.cross else {
        return 0;
    };
    cross.union(left, right);
    1
}

fn union_matrix_edge<A: NameAtomStore + ?Sized>(
    atoms: &A,
    state: &mut ThresholdUnionState,
    left: usize,
    right: usize,
    chain_count: usize,
) -> u64 {
    let Some(matrix) = &mut state.chain_matrix else {
        return 0;
    };
    let left_chain = atoms.chain_index(left);
    let right_chain = atoms.chain_index(right);
    debug_assert_ne!(left_chain, right_chain);
    let (primary_chain, secondary_chain) = if left_chain < right_chain {
        (left_chain, right_chain)
    } else {
        (right_chain, left_chain)
    };
    let pair_index = chain_pair_index(primary_chain, secondary_chain, chain_count);
    match matrix {
        ChainMatrixState::Resident(matrix) => matrix[pair_index].union(left, right),
        ChainMatrixState::Spill(spill) => spill.record_edge(pair_index, left, right, atoms),
    }
    1
}

fn score_canonical_names_sequential<A: NameAtomStore + ?Sized>(
    spec: SequentialCanonicalScoreSpec<'_, A>,
    state: &mut ThresholdUnionState,
    progress: &ProgressTracker,
) -> NameScoringStats {
    let mut scratch =
        NameCandidateScratch::with_mode(spec.canonical.atoms.len(), spec.scratch_mode);
    let mut stats = NameScoringStats::default();
    let mut pending_progress = 0u64;
    for left in 0..spec.canonical.atoms.len() - 1 {
        let right_end = spec.right_range_ends.map_or_else(
            || right_name_range_end_for_left(spec.canonical, left, spec.threshold),
            |ends| ends[left] as usize,
        );
        let mut union_work = NameUnionWork::default();
        let left_stats = visit_indexed_name_pairs_for_left(
            spec.canonical,
            spec.candidate_index,
            left,
            left + 1..right_end,
            spec.threshold,
            &mut scratch,
            |hit| {
                union_work.merge(apply_canonical_edge(
                    spec.original_atoms,
                    spec.canonical,
                    state,
                    spec.chain_count,
                    left,
                    hit,
                ));
            },
        );
        stats.merge(left_stats);
        stats.logical_member_pairs = stats
            .logical_member_pairs
            .saturating_add(union_work.logical_member_pairs);
        stats.spanning_union_operations = stats
            .spanning_union_operations
            .saturating_add(union_work.spanning_union_operations);
        pending_progress = pending_progress.saturating_add(1);
        if pending_progress >= NAME_PROGRESS_LEFT_CHUNK || left + 2 == spec.canonical.atoms.len() {
            progress.advance_task(
                pending_progress,
                ProgressCounters {
                    candidates: stats.candidate_pairs,
                    scored: stats.scored_pairs,
                    expanded: stats.logical_member_pairs,
                    matched: stats.matched_pairs,
                    ..ProgressCounters::default()
                },
            );
            pending_progress = 0;
        }
    }
    stats
}

pub(crate) fn right_name_range_end_for_left<V: NameValueStore + ?Sized>(
    atoms: &V,
    left: usize,
    threshold: f64,
) -> usize {
    if left + 1 >= atoms.len() {
        return atoms.len();
    }

    let left_len = atoms.char_len(left);
    let mut low = left + 1;
    let mut high = atoms.len();
    while low < high {
        let middle = low + (high - low) / 2;
        if name_pair_lengths_can_reach_threshold(left_len, atoms.char_len(middle), threshold) {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    low
}

pub(crate) fn right_name_range_index_bytes(atom_count: usize) -> usize {
    atom_count
        .saturating_sub(1)
        .saturating_mul(std::mem::size_of::<u32>())
}

/// Precompute every length-pruned right endpoint in one monotone pass.
///
/// Canonical atoms retain the source atom ordering by nondecreasing character
/// length. For a fixed left length, the Jaro-Winkler length upper bound only
/// decreases as the right length grows; the largest admissible right length is
/// also monotone as left lengths grow. The shared cursor therefore turns A
/// independent binary searches into O(A) total work.
pub(crate) fn build_right_name_range_ends<V: NameValueStore + ?Sized>(
    atoms: &V,
    threshold: f64,
) -> Box<[u32]> {
    debug_assert!(
        (1..atoms.len()).all(|index| { atoms.char_len(index - 1) <= atoms.char_len(index) })
    );
    let left_count = atoms.len().saturating_sub(1);
    let mut ends = Vec::with_capacity(left_count);
    let mut right = 1usize;
    for left in 0..left_count {
        right = right.max(left + 1);
        while right < atoms.len()
            && name_pair_lengths_can_reach_threshold(
                atoms.char_len(left),
                atoms.char_len(right),
                threshold,
            )
        {
            right += 1;
        }
        debug_assert_eq!(right, right_name_range_end_for_left(atoms, left, threshold));
        ends.push(u32::try_from(right).expect("name right-range endpoint exceeds u32"));
    }
    ends.into_boxed_slice()
}

pub(crate) fn visit_indexed_name_pairs_for_left<V: NameValueStore + ?Sized>(
    atoms: &V,
    candidate_index: &NameCandidateIndex,
    left: usize,
    right_range: std::ops::Range<usize>,
    threshold: f64,
    scratch: &mut NameCandidateScratch,
    mut visit_match: impl FnMut(ScoredRight),
) -> NameScoringStats {
    let query = PreparedNameQuery::new(atoms.normalized_name(left));
    let mut scored_pairs = 0u64;
    let mut matched_pairs = 0u64;
    let mut score_right = |right: usize| {
        scored_pairs = scored_pairs.saturating_add(1);
        let right_name = atoms.normalized_name(right);
        if let Some(score) = query.score_percent(right_name, threshold) {
            matched_pairs = matched_pairs.saturating_add(1);
            visit_match(ScoredRight { right, score });
        }
    };
    if scratch.is_external_merge() {
        candidate_index.visit_external_candidates_for_left(
            atoms,
            left,
            right_range,
            threshold,
            scratch,
            score_right,
        );
    } else if scratch.is_scan() {
        let right_end = right_range.end.min(atoms.len()).min(candidate_index.len());
        for right in right_range.start.min(right_end)..right_end {
            if right != left
                && candidate_index.candidate_passes_overlap_filter(atoms, left, right, threshold)
            {
                score_right(right);
            }
        }
    } else {
        for right in candidate_index
            .candidates_for_left(atoms, left, right_range, threshold, scratch)
            .iter()
            .map(|&right| right as usize)
        {
            score_right(right);
        }
    }
    NameScoringStats {
        candidate_pairs: scored_pairs,
        scored_pairs,
        matched_pairs,
        ..NameScoringStats::default()
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

#[cfg(test)]
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
                    match matrix {
                        ChainMatrixState::Resident(matrix) => {
                            matrix[pair_index].union(left, hit.right);
                        }
                        ChainMatrixState::Spill(spill) => {
                            spill.record_edge(pair_index, left, hit.right, atoms);
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod identity_guard_tests {
    use super::{compact_name_identity, pushed_vec_capacity};

    #[test]
    fn vector_capacity_estimate_saturates_instead_of_panicking() {
        assert_eq!(pushed_vec_capacity(usize::MAX), usize::MAX);
    }

    #[test]
    fn compact_name_identity_rejects_values_above_u32() {
        let Some(overflow) = usize::try_from(u64::from(u32::MAX) + 1).ok() else {
            return;
        };

        let error = compact_name_identity(overflow, "name token dictionary").unwrap_err();

        assert!(error
            .to_string()
            .contains("name token dictionary exceeds compact u32 identity space"));
    }
}
