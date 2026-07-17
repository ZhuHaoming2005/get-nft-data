use super::*;
use duckdb::arrow::array::{Array, Int64Array, StringArray, StringViewArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};

pub(crate) const NAME_ANALYSIS_WORKER_STACK_BYTES: usize = 4 * 1024 * 1024;
const NAME_ATOM_CONVERT_CHUNK: usize = 16 * 1024;
const NAME_MAPPED_STRING_WORKING_SET_BYTES: usize = 64 * 1024 * 1024;
const NAME_MAPPED_ATOM_WORKING_SET_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const NAME_SUMMARY_MIN_ALLOCATION_HEADROOM_BYTES: usize = 8 * 1024 * 1024;
const NAME_SUMMARY_HEADROOM_DIVISOR: usize = 64;
pub(crate) const INTRA_SUMMARY_LIVE_VEC_HEADERS: usize = 3;
pub(crate) const CROSS_SUMMARY_LIVE_VEC_HEADERS: usize = 9;

fn name_string_resident_bytes(mode: NameStringStorageMode, arena_bytes: usize) -> usize {
    match mode {
        NameStringStorageMode::Resident => arena_bytes,
        NameStringStorageMode::Mapped => arena_bytes.min(NAME_MAPPED_STRING_WORKING_SET_BYTES),
    }
}

#[cfg(test)]
pub(crate) fn select_name_string_storage_mode(
    resident_load_peak_bytes: usize,
    analysis_budget_bytes: usize,
) -> NameStringStorageMode {
    if resident_load_peak_bytes <= analysis_budget_bytes {
        NameStringStorageMode::Resident
    } else {
        NameStringStorageMode::Mapped
    }
}

fn name_load_allocation_headroom(projected_live_bytes: usize) -> usize {
    (8 * 1024 * 1024).max(projected_live_bytes / 64)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NameCrossStateStrategy {
    Sparse,
    Dense,
    Deferred,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NameChainMatrixStrategy {
    Resident,
    Spill,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct NameUnionStatePlan {
    pub(crate) cross_strategy: Option<NameCrossStateStrategy>,
    pub(crate) chain_matrix_strategy: Option<NameChainMatrixStrategy>,
    pub(crate) intra_bytes: usize,
    pub(crate) cross_bytes: usize,
    pub(crate) resident_chain_matrix_bytes: usize,
    pub(crate) spill_chain_matrix_bytes: usize,
    pub(crate) total_bytes: usize,
}

pub(crate) fn name_union_state_plan(
    atom_count: usize,
    atoms_by_chain: &[Vec<u32>],
    available_bytes: usize,
) -> NameUnionStatePlan {
    let chain_count = atoms_by_chain.len();
    let intra_bytes = dense_union_find_bytes(atom_count);
    if chain_count <= 1 {
        return NameUnionStatePlan {
            cross_strategy: None,
            chain_matrix_strategy: None,
            intra_bytes,
            cross_bytes: 0,
            resident_chain_matrix_bytes: 0,
            spill_chain_matrix_bytes: 0,
            total_bytes: intra_bytes,
        };
    }

    let sparse_cross_bytes = sparse_union_find_bytes(atom_count);
    let dense_cross_bytes = dense_union_find_bytes(atom_count);
    let resident_chain_matrix_bytes = chain_matrix_reuse_state_bytes(atoms_by_chain);
    let spill_chain_matrix_bytes = chain_matrix_spill_reserve_bytes(chain_count);
    // Preserve the existing sparse global cross state whenever its worst-case
    // footprint plus the minimum spill representation fits. Otherwise a dense
    // global DSU is both faster per union and predictably ~9 bytes per atom.
    let cross_strategy = if intra_bytes
        .saturating_add(sparse_cross_bytes)
        .saturating_add(spill_chain_matrix_bytes)
        <= available_bytes
    {
        NameCrossStateStrategy::Sparse
    } else if intra_bytes
        .saturating_add(dense_cross_bytes)
        .saturating_add(spill_chain_matrix_bytes)
        <= available_bytes
    {
        NameCrossStateStrategy::Dense
    } else {
        NameCrossStateStrategy::Deferred
    };
    let cross_bytes = match cross_strategy {
        NameCrossStateStrategy::Sparse => sparse_cross_bytes,
        NameCrossStateStrategy::Dense => dense_cross_bytes,
        NameCrossStateStrategy::Deferred => 0,
    };
    let resident_total = intra_bytes
        .saturating_add(cross_bytes)
        .saturating_add(resident_chain_matrix_bytes);
    let chain_matrix_strategy = if cross_strategy != NameCrossStateStrategy::Deferred
        && resident_total <= available_bytes
    {
        NameChainMatrixStrategy::Resident
    } else {
        NameChainMatrixStrategy::Spill
    };
    let chain_matrix_bytes = match chain_matrix_strategy {
        NameChainMatrixStrategy::Resident => resident_chain_matrix_bytes,
        NameChainMatrixStrategy::Spill => spill_chain_matrix_bytes,
    };
    NameUnionStatePlan {
        cross_strategy: Some(cross_strategy),
        chain_matrix_strategy: Some(chain_matrix_strategy),
        intra_bytes,
        cross_bytes,
        resident_chain_matrix_bytes,
        spill_chain_matrix_bytes,
        total_bytes: intra_bytes
            .saturating_add(cross_bytes)
            .saturating_add(chain_matrix_bytes),
    }
}

#[cfg(test)]
pub(crate) fn select_name_union_and_scratch_plan(
    atom_count: usize,
    canonical_atom_count: usize,
    atoms_by_chain: &[Vec<u32>],
    threads: usize,
    available_bytes: usize,
) -> (NameUnionStatePlan, NameScratchPlan) {
    select_name_union_and_scratch_plan_for_profile(
        atom_count,
        atoms_by_chain,
        threads,
        available_bytes,
        NameCandidateScratchProfile::Resident {
            atom_count: canonical_atom_count,
        },
    )
}

pub(crate) fn select_name_union_and_scratch_plan_for_profile(
    atom_count: usize,
    atoms_by_chain: &[Vec<u32>],
    threads: usize,
    available_bytes: usize,
    scratch_profile: NameCandidateScratchProfile,
) -> (NameUnionStatePlan, NameScratchPlan) {
    if atoms_by_chain.len() <= 1 {
        let state = name_union_state_plan(atom_count, atoms_by_chain, available_bytes);
        let scratch = name_scratch_plan_for_profile(
            scratch_profile,
            threads,
            available_bytes.saturating_sub(state.total_bytes),
        );
        return (state, scratch);
    }

    let intra_bytes = dense_union_find_bytes(atom_count);
    let resident_chain_matrix_bytes = chain_matrix_reuse_state_bytes(atoms_by_chain);
    let spill_chain_matrix_bytes = chain_matrix_spill_reserve_bytes(atoms_by_chain.len());
    let candidates = [
        (
            NameCrossStateStrategy::Sparse,
            sparse_union_find_bytes(atom_count),
            NameChainMatrixStrategy::Resident,
            resident_chain_matrix_bytes,
        ),
        (
            NameCrossStateStrategy::Dense,
            dense_union_find_bytes(atom_count),
            NameChainMatrixStrategy::Resident,
            resident_chain_matrix_bytes,
        ),
        (
            NameCrossStateStrategy::Sparse,
            sparse_union_find_bytes(atom_count),
            NameChainMatrixStrategy::Spill,
            spill_chain_matrix_bytes,
        ),
        (
            NameCrossStateStrategy::Dense,
            dense_union_find_bytes(atom_count),
            NameChainMatrixStrategy::Spill,
            spill_chain_matrix_bytes,
        ),
        (
            NameCrossStateStrategy::Deferred,
            0,
            NameChainMatrixStrategy::Spill,
            spill_chain_matrix_bytes,
        ),
    ];
    let mut best: Option<(NameUnionStatePlan, NameScratchPlan)> = None;
    for (cross_strategy, cross_bytes, chain_matrix_strategy, chain_matrix_bytes) in candidates {
        let total_bytes = intra_bytes
            .saturating_add(cross_bytes)
            .saturating_add(chain_matrix_bytes);
        if total_bytes > available_bytes {
            continue;
        }
        let state = NameUnionStatePlan {
            cross_strategy: Some(cross_strategy),
            chain_matrix_strategy: Some(chain_matrix_strategy),
            intra_bytes,
            cross_bytes,
            resident_chain_matrix_bytes,
            spill_chain_matrix_bytes,
            total_bytes,
        };
        let scratch = name_scratch_plan_for_profile(
            scratch_profile,
            threads,
            available_bytes.saturating_sub(total_bytes),
        );
        let replace = best.as_ref().is_none_or(|(best_state, best_scratch)| {
            name_execution_plan_rank(state, scratch)
                > name_execution_plan_rank(*best_state, *best_scratch)
        });
        if replace {
            best = Some((state, scratch));
        }
    }
    best.unwrap_or_else(|| {
        let state = name_union_state_plan(atom_count, atoms_by_chain, available_bytes);
        let scratch = name_scratch_plan_for_profile(scratch_profile, threads, 0);
        (state, scratch)
    })
}

fn name_execution_plan_rank(
    state: NameUnionStatePlan,
    scratch: NameScratchPlan,
) -> (u8, usize, u8, u8) {
    (
        u8::from(!matches!(scratch.mode, NameScratchMode::Scan)),
        scratch.admitted_workers,
        u8::from(state.chain_matrix_strategy == Some(NameChainMatrixStrategy::Resident)),
        match state.cross_strategy {
            Some(NameCrossStateStrategy::Sparse) => 2,
            Some(NameCrossStateStrategy::Dense) => 1,
            Some(NameCrossStateStrategy::Deferred) | None => 0,
        },
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NameSummaryStrategy {
    DenseOnePass,
    LowMemory,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct NameSummaryMemoryShape {
    pub(crate) analysis_budget_bytes: usize,
    pub(crate) base_resident_bytes: usize,
    pub(crate) atom_count: usize,
    pub(crate) max_chain_atom_count: usize,
    pub(crate) cross_atom_count: usize,
    pub(crate) max_cross_chain_atom_count: usize,
    pub(crate) chain_count: usize,
    pub(crate) intra_state_bytes: usize,
    pub(crate) cross_state_bytes: usize,
    pub(crate) chain_matrix_state_bytes: usize,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct NameSummaryScratchPlan {
    pub(crate) intra_strategy: NameSummaryStrategy,
    pub(crate) cross_strategy: NameSummaryStrategy,
    pub(crate) intra_fast_resident_bytes: usize,
    pub(crate) cross_fast_resident_bytes: usize,
    pub(crate) intra_allocation_headroom_bytes: usize,
    pub(crate) cross_allocation_headroom_bytes: usize,
    pub(crate) intra_fast_scratch_bytes: usize,
    pub(crate) cross_fast_scratch_bytes: usize,
    pub(crate) intra_fast_peak_bytes: usize,
    pub(crate) cross_fast_peak_bytes: usize,
    pub(crate) low_memory_heap_scratch_bytes: usize,
    pub(crate) max_chain_atom_count: usize,
    pub(crate) max_cross_chain_atom_count: usize,
}

pub(crate) fn name_summary_scratch_plan(shape: NameSummaryMemoryShape) -> NameSummaryScratchPlan {
    let summary_vector_bytes = shape
        .chain_count
        .saturating_mul(std::mem::size_of::<GroupSummary>());
    let vec_header_bytes = std::mem::size_of::<Vec<u8>>();
    let intra_fast_resident_bytes = shape
        .base_resident_bytes
        .saturating_add(shape.intra_state_bytes)
        .saturating_add(shape.cross_state_bytes)
        .saturating_add(shape.chain_matrix_state_bytes);
    let intra_exact_scratch_bytes =
        dense_component_scratch_bytes(shape.atom_count, shape.max_chain_atom_count)
            .saturating_add(summary_vector_bytes)
            .saturating_add(INTRA_SUMMARY_LIVE_VEC_HEADERS.saturating_mul(vec_header_bytes));
    let intra_allocation_headroom_bytes = name_summary_allocation_headroom(
        intra_fast_resident_bytes.saturating_add(intra_exact_scratch_bytes),
    );
    let intra_fast_scratch_bytes =
        intra_exact_scratch_bytes.saturating_add(intra_allocation_headroom_bytes);
    let intra_fast_peak_bytes = intra_fast_resident_bytes.saturating_add(intra_fast_scratch_bytes);

    // The dense intra DSU is released before cross-chain summarization.
    let cross_fast_resident_bytes = shape
        .base_resident_bytes
        .saturating_add(shape.cross_state_bytes)
        .saturating_add(shape.chain_matrix_state_bytes);
    let (cross_fast_scratch_bytes, cross_allocation_headroom_bytes) = if shape.chain_count > 1 {
        let exact = summary_vector_bytes
            .saturating_add(dense_component_scratch_bytes(
                shape.cross_atom_count,
                shape.max_cross_chain_atom_count,
            ))
            .saturating_add(sparse_all_chain_summary_workspace_bytes(
                shape.cross_atom_count,
                shape.chain_count,
            ))
            .saturating_add(CROSS_SUMMARY_LIVE_VEC_HEADERS.saturating_mul(vec_header_bytes));
        let headroom =
            name_summary_allocation_headroom(cross_fast_resident_bytes.saturating_add(exact));
        (exact.saturating_add(headroom), headroom)
    } else {
        (0, 0)
    };
    let cross_fast_peak_bytes = cross_fast_resident_bytes.saturating_add(cross_fast_scratch_bytes);

    NameSummaryScratchPlan {
        intra_strategy: if intra_fast_peak_bytes <= shape.analysis_budget_bytes {
            NameSummaryStrategy::DenseOnePass
        } else {
            NameSummaryStrategy::LowMemory
        },
        cross_strategy: if cross_fast_peak_bytes <= shape.analysis_budget_bytes {
            NameSummaryStrategy::DenseOnePass
        } else {
            NameSummaryStrategy::LowMemory
        },
        intra_fast_resident_bytes,
        cross_fast_resident_bytes,
        intra_allocation_headroom_bytes,
        cross_allocation_headroom_bytes,
        intra_fast_scratch_bytes,
        cross_fast_scratch_bytes,
        intra_fast_peak_bytes,
        cross_fast_peak_bytes,
        low_memory_heap_scratch_bytes: summary_vector_bytes
            .saturating_mul(2)
            .saturating_add(2usize.saturating_mul(vec_header_bytes))
            // The destructive cross-chain fallbacks sort compact local/global
            // u32 identities by (root, chain). This is the only atom-sized
            // auxiliary allocation retained by the low-memory path.
            .saturating_add(
                shape
                    .cross_atom_count
                    .saturating_mul(std::mem::size_of::<u32>()),
            ),
        max_chain_atom_count: shape.max_chain_atom_count,
        max_cross_chain_atom_count: shape.max_cross_chain_atom_count,
    }
}

fn name_summary_allocation_headroom(projected_live_bytes: usize) -> usize {
    NAME_SUMMARY_MIN_ALLOCATION_HEADROOM_BYTES.max(
        projected_live_bytes
            .checked_div(NAME_SUMMARY_HEADROOM_DIVISOR)
            .unwrap_or(usize::MAX),
    )
}

pub(crate) fn name_worker_stack_reserve_bytes(threads: usize) -> usize {
    threads
        .max(1)
        .saturating_mul(NAME_ANALYSIS_WORKER_STACK_BYTES)
}

pub(crate) fn admitted_name_right_range_index_bytes(
    atom_count: usize,
    scoring_peak_without_index: usize,
    analysis_budget_bytes: usize,
) -> usize {
    let index_bytes = right_name_range_index_bytes(atom_count);
    if index_bytes > 0
        && index_bytes <= analysis_budget_bytes.saturating_sub(scoring_peak_without_index)
    {
        index_bytes
    } else {
        0
    }
}

pub(crate) struct NameAnalysisSpec<'a> {
    pub(crate) chains: &'a [String],
    pub(crate) totals: &'a HashMap<String, NameTotals>,
    pub(crate) threshold: f64,
    pub(crate) threads: usize,
    pub(crate) memory_limit: &'a str,
    pub(crate) analysis_memory_limit: Option<&'a str>,
    pub(crate) scratch_directory: &'a Path,
}

pub(crate) struct CanonicalNameValues {
    #[cfg(test)]
    pub(crate) atoms: Vec<CanonicalNameAtom>,
    #[cfg(not(test))]
    pub(crate) atoms: CanonicalNameAtoms,
    pub(crate) members: CanonicalNameMembers,
    #[cfg(not(test))]
    pub(crate) strings: NameStringArena,
}

#[cfg(test)]
#[derive(Clone, Debug)]
pub(crate) struct CanonicalNameAtom {
    pub(crate) name_norm: NameText,
    pub(crate) char_len: usize,
}

#[cfg(not(test))]
struct MappedCanonicalNameAtoms {
    mmap: Option<memmap2::MmapMut>,
    directory: PathBuf,
}

#[cfg(not(test))]
impl Drop for MappedCanonicalNameAtoms {
    fn drop(&mut self) {
        drop(self.mmap.take());
        if self.directory.exists() {
            let _ = fs::remove_dir_all(&self.directory);
        }
    }
}

#[cfg(not(test))]
enum CanonicalNameAtomBacking {
    Resident(Box<[u64]>),
    Mapped(MappedCanonicalNameAtoms),
}

#[cfg(not(test))]
pub(crate) struct CanonicalNameAtoms {
    backing: CanonicalNameAtomBacking,
    len: usize,
    capacity: usize,
    char_lengths_offset: usize,
    byte_len: usize,
}

#[cfg(not(test))]
impl CanonicalNameAtoms {
    #[allow(clippy::slow_vector_initialization)]
    fn new(
        mode: NameAtomStorageMode,
        capacity: usize,
        scratch_directory: &Path,
    ) -> Result<Self, AnalysisError> {
        let char_lengths_offset = capacity
            .checked_mul(std::mem::size_of::<u64>())
            .ok_or_else(|| {
                AnalysisError::InvalidData("canonical name atom SoA size overflow".into())
            })?;
        let byte_len = char_lengths_offset
            .checked_add(
                capacity
                    .checked_mul(std::mem::size_of::<u32>())
                    .ok_or_else(|| {
                        AnalysisError::InvalidData("canonical name atom SoA size overflow".into())
                    })?,
            )
            .ok_or_else(|| {
                AnalysisError::InvalidData("canonical name atom SoA size overflow".into())
            })?;
        let mapped = || -> Result<CanonicalNameAtomBacking, AnalysisError> {
            if byte_len == 0 {
                return Ok(CanonicalNameAtomBacking::Resident(Box::new([])));
            }
            let directory = scratch_directory.join("name-atom-columns");
            if directory.exists() {
                fs::remove_dir_all(&directory)?;
            }
            fs::create_dir_all(&directory)?;
            let file = std::fs::File::create(directory.join("canonical.soa"))?;
            file.set_len(u64::try_from(byte_len).map_err(|_| {
                AnalysisError::InvalidData(
                    "canonical name atom mmap exceeds file offset space".into(),
                )
            })?)?;
            // SAFETY: the file is exclusively owned and its two typed columns
            // are naturally aligned from the page-aligned mapping base.
            let mmap = unsafe { memmap2::MmapOptions::new().len(byte_len).map_mut(&file)? };
            Ok(CanonicalNameAtomBacking::Mapped(MappedCanonicalNameAtoms {
                mmap: Some(mmap),
                directory,
            }))
        };
        let backing = match mode {
            NameAtomStorageMode::Mapped => mapped()?,
            NameAtomStorageMode::Resident => {
                let word_count = byte_len.saturating_add(7) / 8;
                let mut words = Vec::<u64>::new();
                match words.try_reserve_exact(word_count) {
                    Ok(()) => {
                        words.resize(word_count, 0);
                        CanonicalNameAtomBacking::Resident(words.into_boxed_slice())
                    }
                    Err(error) => {
                        eprintln!(
                            "warning: resident canonical name atom SoA allocation of {} failed \
                             ({error}); retrying with file-backed mmap under {}",
                            format_byte_size(byte_len),
                            scratch_directory.join("name-atom-columns").display(),
                        );
                        mapped()?
                    }
                }
            }
        };
        Ok(Self {
            backing,
            len: 0,
            capacity,
            char_lengths_offset,
            byte_len,
        })
    }

    #[inline]
    fn base_ptr(&self) -> *const u8 {
        match &self.backing {
            CanonicalNameAtomBacking::Resident(words) => words.as_ptr().cast(),
            CanonicalNameAtomBacking::Mapped(mapped) => mapped
                .mmap
                .as_ref()
                .map_or(std::ptr::NonNull::<u8>::dangling().as_ptr(), |mmap| {
                    mmap.as_ptr()
                }),
        }
    }

    #[inline]
    fn base_mut_ptr(&mut self) -> *mut u8 {
        match &mut self.backing {
            CanonicalNameAtomBacking::Resident(words) => words.as_mut_ptr().cast(),
            CanonicalNameAtomBacking::Mapped(mapped) => mapped
                .mmap
                .as_mut()
                .map_or(std::ptr::NonNull::<u8>::dangling().as_ptr(), |mmap| {
                    mmap.as_mut_ptr()
                }),
        }
    }

    fn push(&mut self, name_offset: u64, char_len: u32) -> Result<(), AnalysisError> {
        if self.len >= self.capacity {
            return Err(AnalysisError::InvalidData(
                "canonical name atom count exceeded the pre-sized SoA".into(),
            ));
        }
        let index = self.len;
        // SAFETY: both columns are sized for capacity entries and this slot is
        // initialized exactly once before len is published.
        unsafe {
            self.base_mut_ptr()
                .cast::<u64>()
                .add(index)
                .write(name_offset);
            self.base_mut_ptr()
                .add(self.char_lengths_offset)
                .cast::<u32>()
                .add(index)
                .write(char_len);
        }
        self.len += 1;
        Ok(())
    }

    #[inline]
    fn name_offset(&self, index: usize) -> u64 {
        debug_assert!(index < self.len);
        // SAFETY: index is in the initialized prefix.
        unsafe { self.base_ptr().cast::<u64>().add(index).read() }
    }

    #[inline]
    fn char_len(&self, index: usize) -> usize {
        debug_assert!(index < self.len);
        // SAFETY: index is in the initialized prefix.
        unsafe {
            self.base_ptr()
                .add(self.char_lengths_offset)
                .cast::<u32>()
                .add(index)
                .read() as usize
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn resident_bytes(&self) -> usize {
        match &self.backing {
            CanonicalNameAtomBacking::Resident(words) => {
                words.len().saturating_mul(std::mem::size_of::<u64>())
            }
            CanonicalNameAtomBacking::Mapped(_) => {
                self.byte_len.min(NAME_MAPPED_ATOM_WORKING_SET_BYTES)
            }
        }
    }

    fn flush_mapped_async(&self) {
        if let CanonicalNameAtomBacking::Mapped(mapped) = &self.backing {
            if let Some(mmap) = &mapped.mmap {
                let _ = mmap.flush_async();
            }
        }
    }
}

#[cfg(test)]
impl NameValue for CanonicalNameAtom {
    #[inline]
    fn normalized_name(&self) -> &str {
        self.name_norm.as_ref()
    }

    #[inline]
    fn char_len(&self) -> usize {
        self.char_len
    }
}

impl NameValueStore for CanonicalNameValues {
    fn len(&self) -> usize {
        self.atoms.len()
    }

    fn normalized_name(&self, index: usize) -> &str {
        #[cfg(test)]
        {
            self.atoms[index].name_norm.as_ref()
        }
        #[cfg(not(test))]
        {
            self.strings.get(self.atoms.name_offset(index))
        }
    }

    fn char_len(&self, index: usize) -> usize {
        #[cfg(test)]
        {
            self.atoms[index].char_len
        }
        #[cfg(not(test))]
        {
            self.atoms.char_len(index)
        }
    }
}

pub(crate) struct CanonicalNameMembers {
    offsets: Box<[u32]>,
    members: Box<[u32]>,
}

impl CanonicalNameMembers {
    fn new(offsets: Vec<u32>, members: Vec<u32>) -> Self {
        Self {
            offsets: offsets.into_boxed_slice(),
            members: members.into_boxed_slice(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    pub(crate) fn memory_bytes(&self) -> usize {
        self.offsets
            .len()
            .saturating_mul(std::mem::size_of::<u32>())
            .saturating_add(
                self.members
                    .len()
                    .saturating_mul(std::mem::size_of::<u32>()),
            )
    }

    fn range(&self, index: usize) -> std::ops::Range<usize> {
        self.offsets[index] as usize..self.offsets[index + 1] as usize
    }

    pub(crate) fn iter(&self) -> CanonicalNameMemberIter<'_> {
        CanonicalNameMemberIter {
            members: self,
            index: 0,
        }
    }
}

impl std::ops::Index<usize> for CanonicalNameMembers {
    type Output = [u32];

    fn index(&self, index: usize) -> &Self::Output {
        &self.members[self.range(index)]
    }
}

pub(crate) struct CanonicalNameMemberIter<'a> {
    members: &'a CanonicalNameMembers,
    index: usize,
}

impl<'a> Iterator for CanonicalNameMemberIter<'a> {
    type Item = &'a [u32];

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.members.len() {
            return None;
        }
        let members = &self.members[self.index];
        self.index += 1;
        Some(members)
    }
}

impl<'a> IntoIterator for &'a CanonicalNameMembers {
    type Item = &'a [u32];
    type IntoIter = CanonicalNameMemberIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct NameAlgorithmMetrics {
    input_atoms: u64,
    canonical_names: u64,
    candidate_pairs: u64,
    scored_pairs: u64,
    matched_pairs: u64,
    logical_member_pairs: u64,
    spanning_union_operations: u64,
    atom_column_bytes: u64,
    atom_column_resident_bytes: u64,
    atom_sort_permutation_bytes: u64,
    source_string_arena_bytes: u64,
    string_arena_bytes: u64,
    string_arena_resident_bytes: u64,
    canonical_member_csr_bytes: u64,
    candidate_index_bytes: u64,
    right_range_index_bytes: u64,
    scratch_and_queue_bytes: u64,
    dsu_and_chain_state_bytes: u64,
}

pub(crate) struct NameAnalysisResult {
    pub(crate) rows: Vec<SummaryRow>,
    pub(crate) metrics: NameAlgorithmMetrics,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct NameAtomLoadEstimate {
    pub(crate) rows: u64,
    pub(crate) atom_storage_bytes: usize,
    pub(crate) string_arena_bytes: usize,
    pub(crate) sort_scratch_bytes: usize,
}

#[cfg(test)]
pub(crate) fn canonical_name_values(
    atoms: &mut [NameAtom],
) -> Result<CanonicalNameValues, AnalysisError> {
    if atoms.len() > u32::MAX as usize {
        return Err(AnalysisError::InvalidData(
            "name atom count exceeds compact u32 indexes".into(),
        ));
    }
    if !atoms.windows(2).all(|pair| {
        (
            pair[0].char_len,
            pair[0].name_norm.as_ref(),
            pair[0].chain_index,
        ) <= (
            pair[1].char_len,
            pair[1].name_norm.as_ref(),
            pair[1].chain_index,
        )
    }) {
        atoms.sort_unstable_by(|left, right| {
            left.char_len
                .cmp(&right.char_len)
                .then_with(|| left.name_norm.cmp(&right.name_norm))
                .then_with(|| left.chain_index.cmp(&right.chain_index))
        });
    }
    let canonical_count = atoms
        .iter()
        .enumerate()
        .filter(|(index, atom)| {
            *index == 0 || atoms[*index - 1].name_norm.as_ref() != atom.name_norm.as_ref()
        })
        .count();
    let mut canonical_atoms = Vec::<CanonicalNameAtom>::new();
    canonical_atoms
        .try_reserve_exact(canonical_count)
        .map_err(|error| {
            AnalysisError::InvalidData(format!(
                "could not reserve compact canonical name atoms: {error}"
            ))
        })?;
    let mut offsets = Vec::new();
    offsets
        .try_reserve_exact(canonical_count.saturating_add(1))
        .map_err(|error| {
            AnalysisError::InvalidData(format!(
                "could not reserve canonical name CSR offsets: {error}"
            ))
        })?;
    offsets.push(0u32);
    let mut members = Vec::new();
    members.try_reserve_exact(atoms.len()).map_err(|error| {
        AnalysisError::InvalidData(format!(
            "could not reserve canonical name CSR members: {error}"
        ))
    })?;
    let mut run_start = 0usize;
    while run_start < atoms.len() {
        let name = atoms[run_start].name_norm.clone();
        let char_len = atoms[run_start].char_len;
        let mut run_end = run_start + 1;
        while run_end < atoms.len() && atoms[run_end].name_norm.as_ref() == name.as_ref() {
            debug_assert_eq!(atoms[run_end].char_len, char_len);
            run_end += 1;
        }
        canonical_atoms.push(CanonicalNameAtom {
            name_norm: name,
            char_len,
        });
        members.extend((run_start..run_end).map(|index| index as u32));
        offsets.push(run_end as u32);
        #[cfg(test)]
        {
            let shared_name = &canonical_atoms
                .last()
                .expect("canonical run was pushed")
                .name_norm;
            for atom in &mut atoms[run_start..run_end] {
                atom.name_norm = shared_name.clone();
            }
        }
        run_start = run_end;
    }
    debug_assert_eq!(canonical_atoms.len(), canonical_count);
    Ok(CanonicalNameValues {
        atoms: canonical_atoms,
        members: CanonicalNameMembers::new(offsets, members),
    })
}

#[cfg(test)]
fn canonical_loaded_name_values(
    atoms: &mut LoadedNameAtoms,
    _scratch_directory: &Path,
) -> Result<CanonicalNameValues, AnalysisError> {
    canonical_name_values(&mut atoms.atoms.atoms)
}

#[cfg(not(test))]
fn canonical_loaded_name_values(
    atoms: &mut LoadedNameAtoms,
    scratch_directory: &Path,
) -> Result<CanonicalNameValues, AnalysisError> {
    if atoms.len() > u32::MAX as usize {
        return Err(AnalysisError::InvalidData(
            "name atom count exceeds compact u32 indexes".into(),
        ));
    }
    let source_strings = atoms.strings.as_ref().ok_or_else(|| {
        AnalysisError::InvalidData("loaded name atoms lost their source string arena".into())
    })?;
    debug_assert!((1..atoms.len()).all(|index| {
        (
            atoms.char_len(index - 1),
            source_strings.get(atoms.name_offset(index - 1)),
            atoms.chain_index(index - 1),
        ) <= (
            atoms.char_len(index),
            source_strings.get(atoms.name_offset(index)),
            atoms.chain_index(index),
        )
    }));
    let mut canonical_count = 0usize;
    let mut canonical_arena_bytes = 0usize;
    let mut cursor = 0usize;
    while cursor < atoms.len() {
        let name = source_strings.get(atoms.name_offset(cursor));
        canonical_count = canonical_count.saturating_add(1);
        canonical_arena_bytes = canonical_arena_bytes
            .checked_add(std::mem::size_of::<u32>())
            .and_then(|bytes| bytes.checked_add(name.len()))
            .ok_or_else(|| {
                AnalysisError::InvalidData("canonical name arena byte count overflow".into())
            })?;
        cursor += 1;
        while cursor < atoms.len() && source_strings.get(atoms.name_offset(cursor)) == name {
            cursor += 1;
        }
    }
    let mut offsets = Vec::new();
    offsets
        .try_reserve_exact(canonical_count.saturating_add(1))
        .map_err(|error| {
            AnalysisError::InvalidData(format!(
                "could not reserve canonical name CSR offsets: {error}"
            ))
        })?;
    offsets.push(0u32);
    let mut members = Vec::new();
    members.try_reserve_exact(atoms.len()).map_err(|error| {
        AnalysisError::InvalidData(format!(
            "could not reserve canonical name CSR members: {error}"
        ))
    })?;
    let canonical_scratch = scratch_directory.join("name-canonical-storage");
    let mut canonical_atoms =
        CanonicalNameAtoms::new(atoms.atoms.mode(), canonical_count, &canonical_scratch)?;
    let mut canonical_strings = NameStringArenaBuilder::new(
        source_strings.mode(),
        canonical_arena_bytes,
        &canonical_scratch,
    )?;
    let mut run_start = 0usize;
    while run_start < atoms.len() {
        let name = source_strings.get(atoms.name_offset(run_start));
        let char_len = atoms.char_len(run_start);
        let mut run_end = run_start + 1;
        while run_end < atoms.len() && source_strings.get(atoms.name_offset(run_end)) == name {
            debug_assert_eq!(atoms.char_len(run_end), char_len);
            run_end += 1;
        }
        canonical_atoms.push(
            canonical_strings.push(name)?,
            u32::try_from(char_len).map_err(|_| {
                AnalysisError::InvalidData(
                    "canonical name character length exceeds compact u32 space".into(),
                )
            })?,
        )?;
        members.extend((run_start..run_end).map(|index| index as u32));
        offsets.push(run_end as u32);
        run_start = run_end;
    }
    canonical_atoms.flush_mapped_async();
    let strings = canonical_strings.finish()?;
    drop(atoms.strings.take());
    Ok(CanonicalNameValues {
        atoms: canonical_atoms,
        members: CanonicalNameMembers::new(offsets, members),
        strings,
    })
}

pub(crate) fn run_name_analysis(
    conn: &Connection,
    spec: NameAnalysisSpec<'_>,
    progress: &ProgressTracker,
) -> Result<NameAnalysisResult, AnalysisError> {
    let chains = spec.chains;
    let totals = spec.totals;
    let threshold = spec.threshold;
    progress.start_stage("analyzing name duplicates", 6);
    progress.step_stage("loaded name totals");
    let load_estimate = estimate_name_atom_load(conn)?;
    let name_atom_total = load_estimate.rows;
    let worker_stack_bytes = name_worker_stack_reserve_bytes(spec.threads);
    let initial_analysis_budget =
        name_analysis_memory_plan(spec.memory_limit, spec.analysis_memory_limit, 0)?.analysis_bytes;
    let load_live_bytes = |atom_mode: NameAtomStorageMode, string_mode: NameStringStorageMode| {
        let atom_bytes = match atom_mode {
            NameAtomStorageMode::Resident => load_estimate.atom_storage_bytes,
            NameAtomStorageMode::Mapped => load_estimate
                .atom_storage_bytes
                .min(NAME_MAPPED_ATOM_WORKING_SET_BYTES),
        };
        let sort_bytes = match atom_mode {
            NameAtomStorageMode::Resident => load_estimate.sort_scratch_bytes,
            NameAtomStorageMode::Mapped => load_estimate
                .sort_scratch_bytes
                .min(NAME_MAPPED_ATOM_WORKING_SET_BYTES),
        };
        atom_bytes
            .saturating_add(name_string_resident_bytes(
                string_mode,
                load_estimate.string_arena_bytes,
            ))
            .saturating_add(sort_bytes)
    };
    let load_peak_bytes = |atom_mode: NameAtomStorageMode, string_mode: NameStringStorageMode| {
        let live = load_live_bytes(atom_mode, string_mode);
        live.saturating_add(name_load_allocation_headroom(live))
            .saturating_add(worker_stack_bytes)
    };
    // Keep canonical/scoring strings resident before preserving the colder
    // count columns: fuzzy scoring repeatedly reads strings, while the base
    // atom SoA is mostly scanned sequentially during expansion and summary.
    let load_modes = [
        (
            NameAtomStorageMode::Resident,
            NameStringStorageMode::Resident,
        ),
        (NameAtomStorageMode::Mapped, NameStringStorageMode::Resident),
        (NameAtomStorageMode::Resident, NameStringStorageMode::Mapped),
        (NameAtomStorageMode::Mapped, NameStringStorageMode::Mapped),
    ];
    let (atom_storage_mode, string_storage_mode) = load_modes
        .into_iter()
        .find(|&(atom_mode, string_mode)| {
            load_peak_bytes(atom_mode, string_mode) <= initial_analysis_budget
        })
        .unwrap_or((NameAtomStorageMode::Mapped, NameStringStorageMode::Mapped));
    let resident_load_preflight_bytes = load_peak_bytes(
        NameAtomStorageMode::Resident,
        NameStringStorageMode::Resident,
    );
    let load_preflight_bytes = load_peak_bytes(atom_storage_mode, string_storage_mode);
    if load_preflight_bytes > initial_analysis_budget {
        progress.warn(format!(
            "even the demand-paged name load estimate {} is above the {} analysis budget; \
             continuing with mmap-backed atom/string stores instead of terminating. The only \
             atom-sized sort state is the compact {} u32 permutation, which is also mmap-backed \
             in this mode",
            format_byte_size(load_preflight_bytes),
            format_byte_size(initial_analysis_budget),
            format_byte_size(load_estimate.sort_scratch_bytes),
        ));
    };
    if load_preflight_bytes <= initial_analysis_budget {
        name_analysis_memory_plan(
            spec.memory_limit,
            spec.analysis_memory_limit,
            load_preflight_bytes,
        )?;
    }
    if atom_storage_mode == NameAtomStorageMode::Mapped {
        progress.warn(format!(
            "resident name atom columns would contribute {} to a full load peak near {}, above \
             the {} analysis budget; storing offset/chain/rank/length/count columns in one \
             file-backed SoA mmap under {}. The layout uses about 36 bytes per atom and hot \
             column reads do not materialize 48-byte records",
            format_byte_size(load_estimate.atom_storage_bytes),
            format_byte_size(resident_load_preflight_bytes),
            format_byte_size(initial_analysis_budget),
            spec.scratch_directory
                .join("name-atom-storage")
                .join("name-atom-array")
                .display(),
        ));
    }
    if string_storage_mode == NameStringStorageMode::Mapped {
        progress.warn(format!(
            "resident name strings would make the load peak near {}, above the {} analysis \
             budget; storing the {} contiguous UTF-8 arena in a file-backed mmap under {}. \
             Scoring still uses direct in-process string views, while the operating system can \
             reclaim cold pages instead of aborting",
            format_byte_size(resident_load_preflight_bytes),
            format_byte_size(initial_analysis_budget),
            format_byte_size(load_estimate.string_arena_bytes),
            spec.scratch_directory
                .join("name-source-storage")
                .join("name-string-arena")
                .display(),
        ));
    }
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(spec.threads.max(1))
        .thread_name(|index| format!("name-{index}"))
        .stack_size(NAME_ANALYSIS_WORKER_STACK_BYTES)
        .build()
        .map_err(|err| AnalysisError::InvalidData(err.to_string()))?;
    progress.start_task("loading name atoms", Some(name_atom_total), "rows");
    let expected_name_atoms = usize::try_from(name_atom_total)
        .map_err(|_| AnalysisError::InvalidData("name atom row count exceeds usize".into()))?;
    let mut atoms = load_all_name_atoms(
        conn,
        NameAtomLoadSpec {
            chains,
            pool: &pool,
            expected_rows: expected_name_atoms,
            expected_string_arena_bytes: load_estimate.string_arena_bytes,
            string_storage_mode,
            atom_storage_mode,
            scratch_directory: spec.scratch_directory,
        },
        |delta| {
            progress.advance_task(delta, ProgressCounters::default());
        },
    )?;
    if atoms.len() > u32::MAX as usize {
        return Err(AnalysisError::InvalidData(
            "name atom count exceeds compact u32 indexes".to_string(),
        ));
    }
    progress.finish_task(format!("loaded {} name atoms", atoms.len()));
    progress.step_stage(format!("loaded {} name atoms", atoms.len()));
    let actual_atom_storage_mode = atoms.atoms.mode();
    #[cfg(not(test))]
    let actual_source_string_mode = atoms
        .strings
        .as_ref()
        .expect("loaded source strings are present")
        .mode();
    #[cfg(test)]
    let actual_source_string_mode = NameStringStorageMode::Resident;
    if actual_atom_storage_mode != atom_storage_mode {
        progress.warn(format!(
            "resident name atom allocation failed after preflight; continuing with the \
             file-backed SoA fallback under {}",
            spec.scratch_directory
                .join("name-atom-storage")
                .join("name-atom-array")
                .display(),
        ));
    }
    #[cfg(test)]
    let canonical_structural_peak = canonical_name_build_peak_bytes(&atoms.atoms.atoms);
    #[cfg(not(test))]
    let canonical_structural_peak = canonical_name_build_peak_bytes(&atoms);
    let canonical_peak_bytes = canonical_structural_peak
        .saturating_add(
            name_string_resident_bytes(actual_source_string_mode, load_estimate.string_arena_bytes)
                .saturating_mul(2),
        )
        .saturating_add(worker_stack_bytes)
        .saturating_add(8 * 1024 * 1024);
    if canonical_peak_bytes <= initial_analysis_budget {
        name_analysis_memory_plan(
            spec.memory_limit,
            spec.analysis_memory_limit,
            canonical_peak_bytes,
        )?;
    } else {
        progress.warn(format!(
            "canonical-name build is conservatively estimated near {}, above the {} analysis \
             budget; continuing with compact/mmap-capable 12-byte canonical columns and \
             releasing the duplicate source string arena immediately after collapse",
            format_byte_size(canonical_peak_bytes),
            format_byte_size(initial_analysis_budget),
        ));
    }
    let canonical = canonical_loaded_name_values(&mut atoms, spec.scratch_directory)?;
    #[cfg(not(test))]
    let canonical_string_arena_bytes = canonical.strings.bytes();
    #[cfg(test)]
    let canonical_string_arena_bytes = load_estimate.string_arena_bytes;
    #[cfg(not(test))]
    let canonical_string_storage_mode = canonical.strings.mode();
    #[cfg(test)]
    let canonical_string_storage_mode = string_storage_mode;
    let canonical_name_count = canonical.atoms.len();
    progress.set_message(format!(
        "collapsed {} chain/name atoms to {} canonical names",
        atoms.len(),
        canonical.atoms.len()
    ));
    let canonical_members_bytes = canonical.members.memory_bytes();
    let mut atoms_by_chain = atoms_by_chain(&atoms, chains.len());
    let atoms_by_chain_bytes = atoms_by_chain
        .capacity()
        .saturating_mul(std::mem::size_of::<Vec<u32>>())
        .saturating_add(
            atoms_by_chain
                .iter()
                .map(|indexes| {
                    indexes
                        .capacity()
                        .saturating_mul(std::mem::size_of::<u32>())
                })
                .fold(0usize, usize::saturating_add),
        );
    #[cfg(test)]
    let atom_set_bytes = name_atom_sets_memory_bytes(&atoms.atoms.atoms, &canonical.atoms);
    #[cfg(not(test))]
    let atom_set_bytes = name_atom_sets_memory_bytes(&atoms, &canonical);
    #[cfg(test)]
    let canonical_atom_resident_bytes = name_atoms_memory_bytes(&canonical.atoms);
    #[cfg(not(test))]
    let canonical_atom_resident_bytes = canonical.atoms.resident_bytes();
    #[cfg(test)]
    let canonical_string_resident_bytes = 0usize;
    #[cfg(not(test))]
    let canonical_string_resident_bytes =
        name_string_resident_bytes(canonical_string_storage_mode, canonical_string_arena_bytes);
    let base_atom_bytes = atom_set_bytes
        .saturating_add(canonical_members_bytes)
        .saturating_add(atoms_by_chain_bytes)
        .saturating_add(canonical_string_resident_bytes);
    let summary_base_resident_bytes = base_atom_bytes
        .saturating_sub(canonical_atom_resident_bytes)
        .saturating_sub(canonical_members_bytes)
        .saturating_sub(canonical_string_resident_bytes);
    let index_preflight = estimate_name_candidate_index_bytes(&canonical);
    let initial_memory_plan =
        name_analysis_memory_plan(spec.memory_limit, spec.analysis_memory_limit, 0)?;
    let minimum_union_state_bytes =
        dense_union_find_bytes(atoms.len()).saturating_add(if chains.len() > 1 {
            chain_matrix_spill_reserve_bytes(chains.len())
        } else {
            0
        });
    let resident_build_peak = base_atom_bytes
        .saturating_add(index_preflight.peak_build_bytes)
        .saturating_add(worker_stack_bytes);
    let resident_scoring_floor = base_atom_bytes
        .saturating_add(index_preflight.resident_bytes)
        .saturating_add(minimum_union_state_bytes);
    let use_external_index = resident_build_peak > initial_memory_plan.analysis_bytes
        || resident_scoring_floor > initial_memory_plan.analysis_bytes;
    progress.start_task(
        format!(
            "building candidate index for {} canonical names",
            canonical.atoms.len()
        ),
        Some((canonical.atoms.len() as u64).saturating_mul(2)),
        "build units",
    );
    let completed_build_units = std::sync::atomic::AtomicU64::new(0);
    let report_build_unit = || {
        let completed = completed_build_units
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            .saturating_add(1);
        if completed.is_multiple_of(16_384) {
            progress.advance_task(16_384, ProgressCounters::default());
        }
    };
    let candidate_plan =
        if use_external_index {
            None
        } else {
            Some(pool.install(|| {
                NameCandidateIndex::prepare_with_progress(&canonical, report_build_unit)
            })?)
        };
    let index_estimate = candidate_plan
        .as_ref()
        .map(NameCandidateIndexBuildPlan::estimate);
    let candidate_index_resident_bytes = index_estimate
        .map(|estimate| estimate.resident_bytes)
        .unwrap_or(EXTERNAL_NAME_INDEX_RESIDENT_HEADROOM_BYTES);
    let scratch_profile = if use_external_index {
        NameCandidateScratchProfile::External {
            atom_count: canonical.atoms.len(),
            max_name_char_len: (0..canonical.len())
                .map(|index| canonical.char_len(index))
                .max()
                .unwrap_or(0),
        }
    } else {
        NameCandidateScratchProfile::Resident {
            atom_count: canonical.atoms.len(),
        }
    };
    let fixed_scoring_resident_bytes =
        base_atom_bytes.saturating_add(candidate_index_resident_bytes);
    let (state_plan, scratch_plan) = select_name_union_and_scratch_plan_for_profile(
        atoms.len(),
        &atoms_by_chain,
        spec.threads,
        initial_memory_plan
            .analysis_bytes
            .saturating_sub(fixed_scoring_resident_bytes),
        scratch_profile,
    );
    let state_bytes = state_plan.total_bytes;
    let scoring_resident_bytes = base_atom_bytes
        .saturating_add(candidate_index_resident_bytes)
        .saturating_add(state_bytes);
    let scoring_peak_without_right_ranges =
        scoring_resident_bytes.saturating_add(scratch_plan.reserved_bytes);
    let right_range_index_bytes = right_name_range_index_bytes(canonical.atoms.len());
    let admitted_right_range_index_bytes = admitted_name_right_range_index_bytes(
        canonical.atoms.len(),
        scoring_peak_without_right_ranges,
        initial_memory_plan.analysis_bytes,
    );
    let use_right_range_index = admitted_right_range_index_bytes > 0;
    let scoring_peak_bytes =
        scoring_peak_without_right_ranges.saturating_add(admitted_right_range_index_bytes);
    let memory_plan = name_analysis_memory_plan(
        spec.memory_limit,
        spec.analysis_memory_limit,
        scoring_peak_bytes,
    )?;
    if use_external_index {
        progress.warn(format!(
            "name resident candidate index would peak near {} while building and retain about {}; \
             with {} of base atoms and mandatory union state this exceeds the {} analysis budget. \
             Falling back to exact disk-backed sorted postings under {}; Match uses mmap-backed \
             postings and per-left k-way merge/dedup instead of a global O(A²) scan",
            format_byte_size(resident_build_peak),
            format_byte_size(index_preflight.resident_bytes),
            format_byte_size(base_atom_bytes),
            format_byte_size(memory_plan.analysis_bytes),
            spec.scratch_directory
                .join("name-candidate-index")
                .display(),
        ));
    }
    if state_plan.cross_strategy == Some(NameCrossStateStrategy::Dense) {
        progress.warn(format!(
            "name global cross-chain sparse DSU worst case {} does not fit with the minimum \
             chain-matrix representation inside the {} analysis budget; using a predictable \
             dense DSU of about {} instead",
            format_byte_size(sparse_union_find_bytes(atoms.len())),
            format_byte_size(memory_plan.analysis_bytes),
            format_byte_size(state_plan.cross_bytes),
        ));
    }
    if state_plan.cross_strategy == Some(NameCrossStateStrategy::Deferred) {
        progress.warn(
            "name global cross-chain DSU cannot coexist with intra-chain state inside the \
             analysis budget; deferring exact global cross reconstruction to the pair spill \
             files after intra-chain summary releases its dense DSU",
        );
    }
    if state_plan.chain_matrix_strategy == Some(NameChainMatrixStrategy::Spill) {
        progress.warn(format!(
            "name chain-matrix all-resident state would need about {}, above the remaining {} \
             analysis budget; spilling matched edges by chain pair with at most {} buffered \
             memory under {} and rebuilding one pair at a time",
            format_byte_size(state_plan.resident_chain_matrix_bytes),
            format_byte_size(memory_plan.analysis_bytes),
            format_byte_size(state_plan.spill_chain_matrix_bytes),
            spec.scratch_directory
                .join("name-chain-matrix-spill")
                .display(),
        ));
    }
    if scratch_plan.admitted_workers < scratch_plan.requested_workers {
        progress.warn(format!(
            "name scoring requested {} workers, but the {} analysis budget admits {} after \
             reserving resident state and per-worker scratch; using a dedicated bounded scoring \
             pool instead of failing on the full-thread scratch estimate",
            scratch_plan.requested_workers,
            format_byte_size(memory_plan.analysis_bytes),
            scratch_plan.admitted_workers,
        ));
    }
    if scratch_plan.mode == NameScratchMode::Scan {
        progress.warn(format!(
            "name scoring cannot admit even one dense or sparse candidate-dedup lane inside the \
             {} analysis budget; using the exact low-memory token-overlap scan with {} worker(s) \
             and no atom-count-sized candidate scratch",
            format_byte_size(memory_plan.analysis_bytes),
            scratch_plan.admitted_workers,
        ));
    }
    if scratch_plan.mode == NameScratchMode::ExternalMerge {
        progress.warn(format!(
            "name scoring is using the external postings merge path with {} worker(s), reserving \
             {} for bounded posting cursors, Unicode overlap counters, edge queues, and stacks; \
             postings remain demand-paged and hot pages are retained by the operating-system cache",
            scratch_plan.admitted_workers,
            format_byte_size(scratch_plan.reserved_bytes),
        ));
    }
    if right_range_index_bytes > 0 && !use_right_range_index {
        progress.warn(format!(
            "name monotone right-range index would need {}, but the selected resident state and \
             scoring lanes leave only {} inside the {} analysis budget; retaining exact per-left \
             binary range lookup",
            format_byte_size(right_range_index_bytes),
            format_byte_size(
                memory_plan
                    .analysis_bytes
                    .saturating_sub(scoring_peak_without_right_ranges)
            ),
            format_byte_size(memory_plan.analysis_bytes),
        ));
    }
    progress.step_stage(format!(
        "Rust analysis memory budget {}; name scoring admits {} worker(s), reserving {} scratch \
         and queues plus {} dedicated stacks; union state reserves {} intra + {} cross/matrix; \
         monotone right ranges reserve {}",
        format_byte_size(memory_plan.analysis_bytes),
        scratch_plan.admitted_workers,
        format_byte_size(scratch_plan.scratch_and_queue_bytes),
        format_byte_size(scratch_plan.worker_stack_bytes),
        format_byte_size(state_plan.intra_bytes),
        format_byte_size(
            state_plan
                .total_bytes
                .saturating_sub(state_plan.intra_bytes)
        ),
        format_byte_size(admitted_right_range_index_bytes),
    ));
    let candidate_index = if let Some(candidate_plan) = candidate_plan {
        pool.install(|| candidate_plan.build_with_progress(report_build_unit))?
    } else {
        let available_build_bytes = memory_plan
            .analysis_bytes
            .saturating_sub(base_atom_bytes)
            .saturating_sub(worker_stack_bytes);
        pool.install(|| {
            NameCandidateIndex::build_external_with_progress(
                &canonical,
                spec.scratch_directory,
                spec.threads,
                available_build_bytes,
                report_build_unit,
            )
        })?
    };
    let build_total = (canonical.atoms.len() as u64).saturating_mul(2);
    progress.advance_task(build_total % 16_384, ProgressCounters::default());
    if candidate_index.is_external() {
        progress.finish_task(format!(
            "external name candidate index ready; {} disk-backed postings",
            format_byte_size(
                usize::try_from(candidate_index.backing_bytes()).unwrap_or(usize::MAX)
            ),
        ));
    } else {
        progress.finish_task("name candidate index ready");
    }
    progress.step_stage("built name candidate index");
    let actual_index_bytes = candidate_index.memory_bytes();
    if index_estimate.is_some_and(|estimate| actual_index_bytes > estimate.resident_bytes) {
        let estimate = index_estimate.expect("resident estimate checked above");
        progress.warn(format!(
            "name candidate index used {}, exceeding conservative preflight estimate {}; \
             continuing with the measured resident index",
            format_byte_size(actual_index_bytes),
            format_byte_size(estimate.resident_bytes)
        ));
    }
    drop(pool);
    let right_range_ends = use_right_range_index.then(|| {
        progress.set_message(format!(
            "precomputing {} monotone name right ranges",
            canonical.atoms.len().saturating_sub(1)
        ));
        build_right_name_range_ends(&canonical, threshold)
    });
    let mut rows = Vec::new();
    progress.start_task(
        "scoring canonical name left nodes",
        Some(canonical.atoms.len().saturating_sub(1) as u64),
        "names",
    );
    let mut state = ThresholdUnionState {
        threshold,
        intra: UnionFind::new(atoms.len()),
        cross: match state_plan.cross_strategy {
            Some(NameCrossStateStrategy::Sparse) => {
                Some(CrossUnionState::Sparse(SparseUnionFind::default()))
            }
            Some(NameCrossStateStrategy::Dense) => {
                Some(CrossUnionState::Dense(UnionFind::new(atoms.len())))
            }
            Some(NameCrossStateStrategy::Deferred) => Some(CrossUnionState::Deferred),
            None => None,
        },
        chain_matrix: match state_plan.chain_matrix_strategy {
            Some(NameChainMatrixStrategy::Resident) => Some(ChainMatrixState::Resident(
                new_chain_matrix_reuse_states(chain_pair_count(chains.len())),
            )),
            Some(NameChainMatrixStrategy::Spill) => {
                Some(ChainMatrixState::Spill(ChainMatrixSpill::new(
                    spec.scratch_directory.join("name-chain-matrix-spill"),
                    chain_pair_atom_capacities(&atoms_by_chain),
                )?))
            }
            None => None,
        },
    };
    let mut score = || {
        union_canonical_name_pairs(
            &atoms,
            &canonical,
            &candidate_index,
            NameScoringExecution {
                scratch_mode: scratch_plan.mode,
                worker_count: scratch_plan.admitted_workers,
                right_range_ends: right_range_ends.as_deref(),
            },
            &mut state,
            chains.len(),
            progress,
        )
    };
    let scoring = if scratch_plan.admitted_workers <= 1 {
        score()
    } else {
        let scoring_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(scratch_plan.admitted_workers)
            .thread_name(|index| format!("name-score-{index}"))
            .stack_size(NAME_ANALYSIS_WORKER_STACK_BYTES)
            .build()
            .map_err(|err| AnalysisError::InvalidData(err.to_string()))?;
        scoring_pool.install(score)
    };
    progress.finish_task(format!(
        "name scoring complete; canonical candidates {}; scored {}; matched {}; represented {} \
         original-member pairs with {} scope-specific spanning unions",
        scoring.candidate_pairs,
        scoring.scored_pairs,
        scoring.matched_pairs,
        scoring.logical_member_pairs,
        scoring.spanning_union_operations,
    ));
    progress.step_stage("scored canonical names");
    drop(right_range_ends);
    if let Some(ChainMatrixState::Spill(spill)) = &mut state.chain_matrix {
        spill.finish_writes()?;
    }
    drop(candidate_index);
    drop(canonical);
    let summary_plan = name_summary_plan_for_state(
        memory_plan.analysis_bytes,
        summary_base_resident_bytes,
        &atoms,
        &atoms_by_chain,
        &state,
        chains.len(),
    );
    if summary_plan.intra_strategy == NameSummaryStrategy::LowMemory {
        progress.warn(format!(
            "name intra-chain summary fast path would peak near {} (resident {} + scratch {}, \
             including {} allocation headroom), above the {} analysis budget; using in-place \
             root ordering with about {} heap scratch instead",
            format_byte_size(summary_plan.intra_fast_peak_bytes),
            format_byte_size(summary_plan.intra_fast_resident_bytes),
            format_byte_size(summary_plan.intra_fast_scratch_bytes),
            format_byte_size(summary_plan.intra_allocation_headroom_bytes),
            format_byte_size(memory_plan.analysis_bytes),
            format_byte_size(summary_plan.low_memory_heap_scratch_bytes),
        ));
    }
    if summary_plan.cross_strategy == NameSummaryStrategy::LowMemory && state.cross.is_some() {
        progress.warn(format!(
            "name cross-chain summary fast path would peak near {} (resident {} + scratch {}, \
             including {} allocation headroom), above the {} analysis budget; using destructive \
             sparse-root ordering with about {} heap scratch instead",
            format_byte_size(summary_plan.cross_fast_peak_bytes),
            format_byte_size(summary_plan.cross_fast_resident_bytes),
            format_byte_size(summary_plan.cross_fast_scratch_bytes),
            format_byte_size(summary_plan.cross_allocation_headroom_bytes),
            format_byte_size(memory_plan.analysis_bytes),
            format_byte_size(summary_plan.low_memory_heap_scratch_bytes),
        ));
    }
    let summary_units = chains.len() as u64 + chain_pair_count(chains.len()) as u64 * 2;
    progress.start_task(
        "summarizing name components",
        Some(summary_units),
        "summaries",
    );
    push_name_summary_rows(
        &mut rows,
        &atoms,
        &mut atoms_by_chain,
        chains,
        totals,
        &mut state,
        summary_plan,
    )?;
    progress.advance_task(chains.len() as u64, ProgressCounters::default());
    state.intra = UnionFind::new(0);
    state.cross = None;
    if chains.len() > 1 {
        push_reused_chain_matrix_rows(
            &mut rows,
            &atoms,
            &mut atoms_by_chain,
            chains,
            totals,
            &mut state,
        )?;
        progress.advance_task(
            chain_pair_count(chains.len()) as u64 * 2,
            ProgressCounters::default(),
        );
    }
    drop(atoms_by_chain);
    progress.finish_task("name component summaries ready");
    progress.step_stage("summarized name components");
    progress.finish_stage("name analysis complete");
    Ok(NameAnalysisResult {
        rows,
        metrics: NameAlgorithmMetrics {
            input_atoms: atoms.len() as u64,
            canonical_names: canonical_name_count as u64,
            candidate_pairs: scoring.candidate_pairs,
            scored_pairs: scoring.scored_pairs,
            matched_pairs: scoring.matched_pairs,
            logical_member_pairs: scoring.logical_member_pairs,
            spanning_union_operations: scoring.spanning_union_operations,
            atom_column_bytes: load_estimate.atom_storage_bytes as u64,
            atom_column_resident_bytes: atoms.atoms.resident_bytes() as u64,
            atom_sort_permutation_bytes: load_estimate.sort_scratch_bytes as u64,
            source_string_arena_bytes: load_estimate.string_arena_bytes as u64,
            string_arena_bytes: canonical_string_arena_bytes as u64,
            string_arena_resident_bytes: name_string_resident_bytes(
                canonical_string_storage_mode,
                canonical_string_arena_bytes,
            ) as u64,
            canonical_member_csr_bytes: canonical_members_bytes as u64,
            candidate_index_bytes: actual_index_bytes as u64,
            right_range_index_bytes: admitted_right_range_index_bytes as u64,
            scratch_and_queue_bytes: scratch_plan.scratch_and_queue_bytes as u64,
            dsu_and_chain_state_bytes: state_bytes as u64,
        },
    })
}

pub(crate) fn estimate_name_atom_load(
    conn: &Connection,
) -> Result<NameAtomLoadEstimate, AnalysisError> {
    let (rows, string_bytes): (u64, u64) = conn.query_row(
        "SELECT count(*)::UBIGINT,
                coalesce(sum(octet_length(encode(name_norm))), 0)::UBIGINT
         FROM name_atoms",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let row_count = usize::try_from(rows)
        .map_err(|_| AnalysisError::InvalidData("name atom row count exceeds usize".into()))?;
    #[cfg(test)]
    let atom_struct_bytes = row_count
        .checked_mul(std::mem::size_of::<NameAtom>())
        .ok_or_else(|| AnalysisError::InvalidData("name load estimate overflow".into()))?;
    #[cfg(not(test))]
    let atom_struct_bytes = NameAtomSoaLayout::new(row_count)?.byte_len;
    let sort_scratch_bytes = row_count
        .checked_mul(std::mem::size_of::<u32>())
        .ok_or_else(|| AnalysisError::InvalidData("name sort estimate overflow".into()))?;
    let string_headers = rows
        .checked_mul(std::mem::size_of::<u32>() as u64)
        .ok_or_else(|| AnalysisError::InvalidData("name load estimate overflow".into()))?;
    let arena_bytes = string_bytes
        .checked_add(string_headers)
        .ok_or_else(|| AnalysisError::InvalidData("name load estimate overflow".into()))?;
    let string_arena_bytes = usize::try_from(arena_bytes)
        .map_err(|_| AnalysisError::InvalidData("name load estimate exceeds usize".into()))?;
    Ok(NameAtomLoadEstimate {
        rows,
        atom_storage_bytes: atom_struct_bytes,
        string_arena_bytes,
        sort_scratch_bytes,
    })
}

#[cfg(test)]
pub(crate) fn count_all_name_atoms(conn: &Connection) -> Result<u64, AnalysisError> {
    estimate_name_atom_load(conn).map(|estimate| estimate.rows)
}

pub(crate) struct NameAtomLoadSpec<'a> {
    pub(crate) chains: &'a [String],
    pub(crate) pool: &'a rayon::ThreadPool,
    pub(crate) expected_rows: usize,
    pub(crate) expected_string_arena_bytes: usize,
    pub(crate) string_storage_mode: NameStringStorageMode,
    pub(crate) atom_storage_mode: NameAtomStorageMode,
    pub(crate) scratch_directory: &'a Path,
}

#[cfg(not(test))]
#[derive(Clone, Copy)]
struct NameAtomRow {
    name_offset: u64,
    chain_index: u32,
    chain_local_rank: u32,
    char_len: u32,
    contract_count: i64,
    nft_count: i64,
}

#[cfg(not(test))]
#[derive(Clone, Copy)]
struct NameAtomSoaLayout {
    name_offsets: usize,
    chain_indexes: usize,
    chain_local_ranks: usize,
    char_lengths: usize,
    contract_counts: usize,
    nft_counts: usize,
    byte_len: usize,
}

#[cfg(not(test))]
impl NameAtomSoaLayout {
    fn new(capacity: usize) -> Result<Self, AnalysisError> {
        fn column_end<T>(offset: usize, capacity: usize) -> Option<usize> {
            offset.checked_add(capacity.checked_mul(std::mem::size_of::<T>())?)
        }

        let name_offsets = 0usize;
        let chain_indexes = column_end::<u64>(name_offsets, capacity)
            .ok_or_else(|| AnalysisError::InvalidData("name atom SoA size overflow".into()))?;
        let chain_local_ranks = column_end::<u32>(chain_indexes, capacity)
            .ok_or_else(|| AnalysisError::InvalidData("name atom SoA size overflow".into()))?;
        let char_lengths = column_end::<u32>(chain_local_ranks, capacity)
            .ok_or_else(|| AnalysisError::InvalidData("name atom SoA size overflow".into()))?;
        let contract_unaligned = column_end::<u32>(char_lengths, capacity)
            .ok_or_else(|| AnalysisError::InvalidData("name atom SoA size overflow".into()))?;
        let contract_counts = contract_unaligned
            .checked_add(std::mem::align_of::<i64>() - 1)
            .map(|offset| offset & !(std::mem::align_of::<i64>() - 1))
            .ok_or_else(|| AnalysisError::InvalidData("name atom SoA size overflow".into()))?;
        let nft_counts = column_end::<i64>(contract_counts, capacity)
            .ok_or_else(|| AnalysisError::InvalidData("name atom SoA size overflow".into()))?;
        let byte_len = column_end::<i64>(nft_counts, capacity)
            .ok_or_else(|| AnalysisError::InvalidData("name atom SoA size overflow".into()))?;
        Ok(Self {
            name_offsets,
            chain_indexes,
            chain_local_ranks,
            char_lengths,
            contract_counts,
            nft_counts,
            byte_len,
        })
    }
}

#[cfg(not(test))]
struct MappedNameAtomColumns {
    mmap: Option<memmap2::MmapMut>,
    directory: PathBuf,
}

#[cfg(not(test))]
impl Drop for MappedNameAtomColumns {
    fn drop(&mut self) {
        drop(self.mmap.take());
        if self.directory.exists() {
            let _ = fs::remove_dir_all(&self.directory);
        }
    }
}

#[cfg(not(test))]
enum NameAtomColumnBacking {
    // u64 words give every typed column its required base alignment. The
    // layout itself keeps the i64 columns aligned for odd atom counts.
    Resident(Box<[u64]>),
    Mapped(MappedNameAtomColumns),
}

#[cfg(not(test))]
struct MappedNameSortOrder {
    mmap: Option<memmap2::MmapMut>,
    directory: PathBuf,
}

#[cfg(not(test))]
impl Drop for MappedNameSortOrder {
    fn drop(&mut self) {
        drop(self.mmap.take());
        if self.directory.exists() {
            let _ = fs::remove_dir_all(&self.directory);
        }
    }
}

#[cfg(not(test))]
enum NameSortOrder {
    Resident(Vec<u32>),
    Mapped(MappedNameSortOrder),
}

#[cfg(not(test))]
impl NameSortOrder {
    fn new(
        mode: NameAtomStorageMode,
        len: usize,
        scratch_directory: &Path,
    ) -> Result<Self, AnalysisError> {
        let mapped = || -> Result<Self, AnalysisError> {
            if len == 0 {
                return Ok(Self::Resident(Vec::new()));
            }
            let byte_len = len.checked_mul(std::mem::size_of::<u32>()).ok_or_else(|| {
                AnalysisError::InvalidData("name sort permutation size overflow".into())
            })?;
            let directory = scratch_directory.join("name-sort-permutation");
            if directory.exists() {
                fs::remove_dir_all(&directory)?;
            }
            fs::create_dir_all(&directory)?;
            let file = std::fs::File::create(directory.join("order.bin"))?;
            file.set_len(u64::try_from(byte_len).map_err(|_| {
                AnalysisError::InvalidData("name sort permutation exceeds file offset space".into())
            })?)?;
            // SAFETY: the scratch file is exclusively owned, fully sized for
            // u32 identities, and a file mapping is page aligned.
            let mmap = unsafe { memmap2::MmapOptions::new().len(byte_len).map_mut(&file)? };
            Ok(Self::Mapped(MappedNameSortOrder {
                mmap: Some(mmap),
                directory,
            }))
        };
        let mut order = match mode {
            NameAtomStorageMode::Mapped => mapped()?,
            NameAtomStorageMode::Resident => {
                let mut order = Vec::new();
                match order.try_reserve_exact(len) {
                    Ok(()) => Self::Resident(order),
                    Err(error) => {
                        eprintln!(
                            "warning: resident name sort permutation allocation of {} failed \
                             ({error}); retrying with file-backed mmap under {}",
                            format_byte_size(len.saturating_mul(std::mem::size_of::<u32>())),
                            scratch_directory.join("name-sort-permutation").display(),
                        );
                        mapped()?
                    }
                }
            }
        };
        match &mut order {
            Self::Resident(values) => values.extend((0..len).map(|index| index as u32)),
            Self::Mapped(mapped) => {
                let mmap = mapped
                    .mmap
                    .as_mut()
                    .expect("non-empty sort permutation mmap is open");
                // SAFETY: the mapping contains exactly len aligned u32 slots.
                let values =
                    unsafe { std::slice::from_raw_parts_mut(mmap.as_mut_ptr().cast::<u32>(), len) };
                values
                    .iter_mut()
                    .enumerate()
                    .for_each(|(index, slot)| *slot = index as u32);
            }
        }
        Ok(order)
    }

    fn as_mut_slice(&mut self) -> &mut [u32] {
        match self {
            Self::Resident(values) => values,
            Self::Mapped(mapped) => {
                let mmap = mapped.mmap.as_mut().expect("sort permutation mmap is open");
                // SAFETY: this mapping was created for exactly mmap.len()/4
                // u32 slots and the exclusive borrow prevents aliases.
                unsafe {
                    std::slice::from_raw_parts_mut(
                        mmap.as_mut_ptr().cast::<u32>(),
                        mmap.len() / std::mem::size_of::<u32>(),
                    )
                }
            }
        }
    }
}

#[cfg(not(test))]
struct NameAtomRecords {
    backing: NameAtomColumnBacking,
    layout: NameAtomSoaLayout,
    len: usize,
    capacity: usize,
}

#[cfg(not(test))]
impl NameAtomRecords {
    #[allow(clippy::slow_vector_initialization)]
    fn new(
        mode: NameAtomStorageMode,
        capacity: usize,
        scratch_directory: &Path,
    ) -> Result<Self, AnalysisError> {
        let layout = NameAtomSoaLayout::new(capacity)?;
        let mapped = || -> Result<NameAtomColumnBacking, AnalysisError> {
            if layout.byte_len == 0 {
                return Ok(NameAtomColumnBacking::Resident(Box::new([])));
            }
            let directory = scratch_directory.join("name-atom-array");
            if directory.exists() {
                fs::remove_dir_all(&directory)?;
            }
            fs::create_dir_all(&directory)?;
            let file = std::fs::File::create(directory.join("atoms.soa"))?;
            file.set_len(u64::try_from(layout.byte_len).map_err(|_| {
                AnalysisError::InvalidData("name atom mmap exceeds file offset space".into())
            })?)?;
            // SAFETY: the scratch file is exclusively owned, fully sized, and
            // all typed columns are aligned by NameAtomSoaLayout.
            let mmap = unsafe {
                memmap2::MmapOptions::new()
                    .len(layout.byte_len)
                    .map_mut(&file)?
            };
            Ok(NameAtomColumnBacking::Mapped(MappedNameAtomColumns {
                mmap: Some(mmap),
                directory,
            }))
        };
        let backing = match mode {
            NameAtomStorageMode::Mapped => mapped()?,
            NameAtomStorageMode::Resident => {
                let word_count = layout.byte_len.saturating_add(7) / 8;
                let mut words = Vec::<u64>::new();
                match words.try_reserve_exact(word_count) {
                    Ok(()) => {
                        words.resize(word_count, 0);
                        NameAtomColumnBacking::Resident(words.into_boxed_slice())
                    }
                    Err(error) => {
                        eprintln!(
                            "warning: resident name atom SoA allocation of {} failed ({error}); \
                             retrying with file-backed mmap under {}",
                            format_byte_size(layout.byte_len),
                            scratch_directory.join("name-atom-array").display(),
                        );
                        mapped()?
                    }
                }
            }
        };
        Ok(Self {
            backing,
            layout,
            len: 0,
            capacity,
        })
    }

    #[inline]
    fn base_ptr(&self) -> *const u8 {
        match &self.backing {
            NameAtomColumnBacking::Resident(words) => words.as_ptr().cast(),
            NameAtomColumnBacking::Mapped(mapped) => mapped
                .mmap
                .as_ref()
                .map_or(std::ptr::NonNull::<u8>::dangling().as_ptr(), |mmap| {
                    mmap.as_ptr()
                }),
        }
    }

    #[inline]
    fn base_mut_ptr(&mut self) -> *mut u8 {
        match &mut self.backing {
            NameAtomColumnBacking::Resident(words) => words.as_mut_ptr().cast(),
            NameAtomColumnBacking::Mapped(mapped) => mapped
                .mmap
                .as_mut()
                .map_or(std::ptr::NonNull::<u8>::dangling().as_ptr(), |mmap| {
                    mmap.as_mut_ptr()
                }),
        }
    }

    #[inline]
    fn read<T: Copy>(&self, column_offset: usize, index: usize) -> T {
        debug_assert!(index < self.len);
        // SAFETY: every column is sized for capacity elements, the layout
        // aligns T, and callers only read the initialized prefix.
        unsafe {
            self.base_ptr()
                .add(column_offset)
                .cast::<T>()
                .add(index)
                .read()
        }
    }

    #[inline]
    fn write<T: Copy>(&mut self, column_offset: usize, index: usize, value: T) {
        debug_assert!(index < self.capacity);
        // SAFETY: every column is sized for capacity elements, the layout
        // aligns T, and this exclusive borrow prevents concurrent writes.
        unsafe {
            self.base_mut_ptr()
                .add(column_offset)
                .cast::<T>()
                .add(index)
                .write(value);
        }
    }

    #[inline]
    fn row(&self, index: usize) -> NameAtomRow {
        NameAtomRow {
            name_offset: self.read(self.layout.name_offsets, index),
            chain_index: self.read(self.layout.chain_indexes, index),
            chain_local_rank: self.read(self.layout.chain_local_ranks, index),
            char_len: self.read(self.layout.char_lengths, index),
            contract_count: self.read(self.layout.contract_counts, index),
            nft_count: self.read(self.layout.nft_counts, index),
        }
    }

    #[inline]
    fn write_row(&mut self, index: usize, row: NameAtomRow) {
        self.write(self.layout.name_offsets, index, row.name_offset);
        self.write(self.layout.chain_indexes, index, row.chain_index);
        self.write(self.layout.chain_local_ranks, index, row.chain_local_rank);
        self.write(self.layout.char_lengths, index, row.char_len);
        self.write(self.layout.contract_counts, index, row.contract_count);
        self.write(self.layout.nft_counts, index, row.nft_count);
    }

    fn push(&mut self, row: NameAtomRow) -> Result<(), AnalysisError> {
        if self.len >= self.capacity {
            return Err(AnalysisError::InvalidData(
                "name atom row count exceeded the pre-sized SoA".into(),
            ));
        }
        let index = self.len;
        self.write_row(index, row);
        self.len += 1;
        Ok(())
    }

    fn sort_by_name(
        &mut self,
        strings: &NameStringArena,
        pool: &rayon::ThreadPool,
        scratch_directory: &Path,
    ) -> Result<(), AnalysisError> {
        let mut order = NameSortOrder::new(self.mode(), self.len, scratch_directory)?;
        let order = order.as_mut_slice();
        pool.install(|| {
            order.par_sort_unstable_by(|&left, &right| {
                let left = left as usize;
                let right = right as usize;
                self.char_len(left)
                    .cmp(&self.char_len(right))
                    .then_with(|| {
                        strings
                            .get(self.name_offset(left))
                            .cmp(strings.get(self.name_offset(right)))
                    })
                    .then_with(|| self.chain_index(left).cmp(&self.chain_index(right)))
            });
        });

        // `order[destination] = source`. Move every permutation cycle with
        // one stack row, rewriting the permutation to identity as its visited
        // marker. This keeps reorder scratch at exactly 4 bytes per atom.
        for start in 0..order.len() {
            if order[start] as usize == start {
                continue;
            }
            let saved = self.row(start);
            let mut destination = start;
            loop {
                let source = order[destination] as usize;
                order[destination] = destination as u32;
                if source == start {
                    self.write_row(destination, saved);
                    break;
                }
                let row = self.row(source);
                self.write_row(destination, row);
                destination = source;
            }
        }
        Ok(())
    }

    fn assign_chain_local_ranks(&mut self, chain_count: usize) -> Result<(), AnalysisError> {
        let mut next_rank = vec![0u32; chain_count];
        for index in 0..self.len {
            let chain_index = self.chain_index(index);
            let rank = next_rank[chain_index];
            self.write(self.layout.chain_local_ranks, index, rank);
            next_rank[chain_index] = rank.checked_add(1).ok_or_else(|| {
                AnalysisError::InvalidData(
                    "name chain-local atom rank exceeds compact u32 space".into(),
                )
            })?;
        }
        Ok(())
    }

    fn mode(&self) -> NameAtomStorageMode {
        match &self.backing {
            NameAtomColumnBacking::Resident(_) => NameAtomStorageMode::Resident,
            NameAtomColumnBacking::Mapped(_) => NameAtomStorageMode::Mapped,
        }
    }

    fn resident_bytes(&self) -> usize {
        match &self.backing {
            NameAtomColumnBacking::Resident(words) => {
                words.len().saturating_mul(std::mem::size_of::<u64>())
            }
            NameAtomColumnBacking::Mapped(_) => {
                self.layout.byte_len.min(NAME_MAPPED_ATOM_WORKING_SET_BYTES)
            }
        }
    }

    fn flush_mapped_async(&self) {
        if let NameAtomColumnBacking::Mapped(mapped) = &self.backing {
            if let Some(mmap) = &mapped.mmap {
                let _ = mmap.flush_async();
            }
        }
    }
}

#[cfg(test)]
#[derive(Debug)]
struct NameAtomRecords {
    atoms: Vec<NameAtom>,
}

#[cfg(test)]
impl NameAtomRecords {
    fn new(
        _mode: NameAtomStorageMode,
        capacity: usize,
        _scratch_directory: &Path,
    ) -> Result<Self, AnalysisError> {
        let mut atoms = Vec::new();
        atoms.try_reserve_exact(capacity).map_err(|error| {
            AnalysisError::InvalidData(format!("could not reserve name atoms: {error}"))
        })?;
        Ok(Self { atoms })
    }

    fn push(&mut self, atom: NameAtom) -> Result<(), AnalysisError> {
        if self.atoms.len() >= self.atoms.capacity() {
            return Err(AnalysisError::InvalidData(
                "name atom row count exceeded the reserved resident capacity".into(),
            ));
        }
        self.atoms.push(atom);
        Ok(())
    }

    fn mode(&self) -> NameAtomStorageMode {
        NameAtomStorageMode::Resident
    }

    fn resident_bytes(&self) -> usize {
        name_atoms_memory_bytes(&self.atoms)
    }

    fn flush_mapped_async(&self) {}
}

#[cfg(test)]
impl NameAtomStore for NameAtomRecords {
    fn len(&self) -> usize {
        self.atoms.len()
    }

    fn chain_index(&self, index: usize) -> usize {
        self.atoms[index].chain_index
    }

    fn chain_local_rank(&self, index: usize) -> u32 {
        self.atoms.as_slice().chain_local_rank(index)
    }

    fn contract_count(&self, index: usize) -> i64 {
        self.atoms[index].contract_count
    }

    fn nft_count(&self, index: usize) -> i64 {
        self.atoms[index].nft_count
    }
}

#[cfg_attr(test, derive(Debug))]
pub(crate) struct LoadedNameAtoms {
    atoms: NameAtomRecords,
    #[cfg(not(test))]
    pub(crate) strings: Option<NameStringArena>,
}

#[cfg(test)]
impl std::ops::Deref for LoadedNameAtoms {
    type Target = [NameAtom];

    fn deref(&self) -> &Self::Target {
        &self.atoms.atoms
    }
}

#[cfg(test)]
impl std::ops::DerefMut for LoadedNameAtoms {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.atoms.atoms
    }
}

impl NameAtomStore for LoadedNameAtoms {
    fn len(&self) -> usize {
        #[cfg(test)]
        {
            self.atoms.atoms.len()
        }
        #[cfg(not(test))]
        {
            self.atoms.len
        }
    }

    fn chain_index(&self, index: usize) -> usize {
        #[cfg(test)]
        {
            self.atoms.atoms[index].chain_index
        }
        #[cfg(not(test))]
        {
            self.atoms.chain_index(index)
        }
    }

    fn chain_local_rank(&self, index: usize) -> u32 {
        #[cfg(test)]
        {
            self.atoms.atoms.as_slice().chain_local_rank(index)
        }
        #[cfg(not(test))]
        {
            self.atoms.chain_local_rank(index)
        }
    }

    #[cfg(not(test))]
    fn name_offset(&self, index: usize) -> u64 {
        self.atoms.name_offset(index)
    }

    #[cfg(not(test))]
    fn char_len(&self, index: usize) -> usize {
        self.atoms.char_len(index)
    }

    fn contract_count(&self, index: usize) -> i64 {
        #[cfg(test)]
        {
            self.atoms.atoms[index].contract_count
        }
        #[cfg(not(test))]
        {
            self.atoms.contract_count(index)
        }
    }

    fn nft_count(&self, index: usize) -> i64 {
        #[cfg(test)]
        {
            self.atoms.atoms[index].nft_count
        }
        #[cfg(not(test))]
        {
            self.atoms.nft_count(index)
        }
    }
}

#[cfg(not(test))]
impl NameAtomStore for NameAtomRecords {
    fn len(&self) -> usize {
        self.len
    }

    #[inline]
    fn chain_index(&self, index: usize) -> usize {
        self.read::<u32>(self.layout.chain_indexes, index) as usize
    }

    #[inline]
    fn chain_local_rank(&self, index: usize) -> u32 {
        self.read(self.layout.chain_local_ranks, index)
    }

    #[inline]
    fn name_offset(&self, index: usize) -> u64 {
        self.read(self.layout.name_offsets, index)
    }

    #[inline]
    fn char_len(&self, index: usize) -> usize {
        self.read::<u32>(self.layout.char_lengths, index) as usize
    }

    #[inline]
    fn contract_count(&self, index: usize) -> i64 {
        self.read(self.layout.contract_counts, index)
    }

    #[inline]
    fn nft_count(&self, index: usize) -> i64 {
        self.read(self.layout.nft_counts, index)
    }
}

pub(crate) fn load_all_name_atoms(
    conn: &Connection,
    spec: NameAtomLoadSpec<'_>,
    mut on_rows_loaded: impl FnMut(u64),
) -> Result<LoadedNameAtoms, AnalysisError> {
    if spec.expected_rows > u32::MAX as usize {
        return Err(AnalysisError::InvalidData(
            "name atom count exceeds compact u32 indexes".into(),
        ));
    }
    #[derive(Clone, Copy)]
    struct PreparedNameAtom {
        chain_index: usize,
        char_len: usize,
        contract_count: i64,
        nft_count: i64,
    }

    #[cfg(not(test))]
    let source_string_scratch = spec.scratch_directory.join("name-source-storage");
    #[cfg(not(test))]
    let mut string_arena = NameStringArenaBuilder::new(
        spec.string_storage_mode,
        spec.expected_string_arena_bytes,
        &source_string_scratch,
    )?;
    #[cfg(test)]
    let _ = (
        spec.expected_string_arena_bytes,
        spec.string_storage_mode,
        spec.scratch_directory,
    );
    let atom_scratch = spec.scratch_directory.join("name-atom-storage");
    let mut atoms =
        NameAtomRecords::new(spec.atom_storage_mode, spec.expected_rows, &atom_scratch)?;
    let chain_indexes = spec
        .chains
        .iter()
        .enumerate()
        .map(|(index, chain)| (chain.as_str(), index))
        .collect::<HashMap<_, _>>();
    let mut stmt =
        conn.prepare("SELECT chain, name_norm, contract_count, nft_count FROM name_atoms")?;
    let batches = stmt.stream_arrow(
        [],
        std::sync::Arc::new(Schema::new(vec![
            Field::new("chain", DataType::Utf8, false),
            Field::new("name_norm", DataType::Utf8, false),
            Field::new("contract_count", DataType::Int64, false),
            Field::new("nft_count", DataType::Int64, false),
        ])),
    )?;
    for batch in batches {
        let row_count = batch.num_rows();
        let chains = NameStringColumn::new(batch.column(0).as_ref(), "chain")?;
        let names = NameStringColumn::new(batch.column(1).as_ref(), "name_norm")?;
        let contract_counts = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| AnalysisError::InvalidData("name contract_count is not INT64".into()))?;
        let nft_counts = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| AnalysisError::InvalidData("name nft_count is not INT64".into()))?;
        for chunk_start in (0..row_count).step_by(NAME_ATOM_CONVERT_CHUNK) {
            let chunk_end = chunk_start
                .saturating_add(NAME_ATOM_CONVERT_CHUNK)
                .min(row_count);
            let chunk_atoms = spec.pool.install(|| {
                (chunk_start..chunk_end)
                    .into_par_iter()
                    .map(|index| -> Result<PreparedNameAtom, AnalysisError> {
                        if chains.is_null(index)
                            || names.is_null(index)
                            || contract_counts.is_null(index)
                            || nft_counts.is_null(index)
                        {
                            return Err(AnalysisError::InvalidData(
                                "name atom row contains NULL".into(),
                            ));
                        }
                        let chain = chains.value(index);
                        let chain_index = chain_indexes.get(chain).copied().ok_or_else(|| {
                            AnalysisError::InvalidData(format!(
                                "name atom references unselected chain {chain:?}"
                            ))
                        })?;
                        let name = names.value(index);
                        Ok(PreparedNameAtom {
                            chain_index,
                            char_len: name.chars().count(),
                            contract_count: contract_counts.value(index),
                            nft_count: nft_counts.value(index),
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()
            })?;
            for (index, prepared) in (chunk_start..chunk_end).zip(chunk_atoms) {
                let name = names.value(index);
                #[cfg(test)]
                atoms.push(NameAtom {
                    chain_index: prepared.chain_index,
                    name_norm: std::sync::Arc::from(name),
                    char_len: prepared.char_len,
                    contract_count: prepared.contract_count,
                    nft_count: prepared.nft_count,
                })?;
                #[cfg(not(test))]
                atoms.push(NameAtomRow {
                    chain_index: u32::try_from(prepared.chain_index).map_err(|_| {
                        AnalysisError::InvalidData(
                            "name chain identity exceeds compact u32 space".into(),
                        )
                    })?,
                    chain_local_rank: 0,
                    name_offset: string_arena.push(name)?,
                    char_len: u32::try_from(prepared.char_len).map_err(|_| {
                        AnalysisError::InvalidData(
                            "normalized name character length exceeds compact u32 space".into(),
                        )
                    })?,
                    contract_count: prepared.contract_count,
                    nft_count: prepared.nft_count,
                })?;
            }
            on_rows_loaded((chunk_end - chunk_start) as u64);
        }
    }
    if NameAtomStore::len(&atoms) != spec.expected_rows {
        return Err(AnalysisError::InvalidData(format!(
            "name atom row count changed while loading: expected={}, loaded={}",
            spec.expected_rows,
            NameAtomStore::len(&atoms)
        )));
    }
    #[cfg(not(test))]
    let strings = string_arena.finish()?;
    #[cfg(test)]
    spec.pool.install(|| {
        atoms.atoms.par_sort_unstable_by(|left, right| {
            left.char_len
                .cmp(&right.char_len)
                .then_with(|| left.name_norm.cmp(&right.name_norm))
                .then_with(|| left.chain_index.cmp(&right.chain_index))
        });
    });
    #[cfg(not(test))]
    {
        atoms.sort_by_name(&strings, spec.pool, &atom_scratch)?;
        atoms.assign_chain_local_ranks(spec.chains.len())?;
    }
    atoms.flush_mapped_async();
    Ok(LoadedNameAtoms {
        atoms,
        #[cfg(not(test))]
        strings: Some(strings),
    })
}

#[derive(Clone, Copy)]
enum NameStringColumn<'a> {
    String(&'a StringArray),
    View(&'a StringViewArray),
}

impl<'a> NameStringColumn<'a> {
    fn new(array: &'a dyn Array, name: &str) -> Result<Self, AnalysisError> {
        if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
            return Ok(Self::String(values));
        }
        if let Some(values) = array.as_any().downcast_ref::<StringViewArray>() {
            return Ok(Self::View(values));
        }
        Err(AnalysisError::InvalidData(format!(
            "name column {name} is not UTF8"
        )))
    }

    fn is_null(self, index: usize) -> bool {
        match self {
            Self::String(values) => values.is_null(index),
            Self::View(values) => values.is_null(index),
        }
    }

    fn value(self, index: usize) -> &'a str {
        match self {
            Self::String(values) => values.value(index),
            Self::View(values) => values.value(index),
        }
    }
}

pub(crate) fn name_summary_plan_for_state<A: NameAtomStore + ?Sized>(
    analysis_budget_bytes: usize,
    base_resident_bytes: usize,
    atoms: &A,
    atoms_by_chain: &[Vec<u32>],
    state: &ThresholdUnionState,
    chain_count: usize,
) -> NameSummaryScratchPlan {
    let max_chain_atom_count = atoms_by_chain.iter().map(Vec::len).max().unwrap_or(0);
    let (cross_atom_count, max_cross_chain_atom_count) =
        state.cross.as_ref().map_or((0, 0), |cross| match cross {
            CrossUnionState::Sparse(cross) => {
                let mut counts = vec![0usize; chain_count];
                for local_index in 0..cross.atom_count() {
                    let chain_index = atoms.chain_index(cross.atom_at(local_index));
                    counts[chain_index] = counts[chain_index].saturating_add(1);
                }
                (cross.atom_count(), counts.into_iter().max().unwrap_or(0))
            }
            CrossUnionState::Dense(_) => (atoms.len(), max_chain_atom_count),
            CrossUnionState::Deferred => (atoms.len(), max_chain_atom_count),
        });
    let intra_state_bytes = union_find_resident_bytes(&state.intra);
    let cross_state_bytes = state
        .cross
        .as_ref()
        .map(|cross| match cross {
            CrossUnionState::Sparse(cross) => sparse_union_find_resident_bytes(cross),
            CrossUnionState::Dense(cross) => union_find_resident_bytes(cross),
            // Deferred reconstructs this dense DSU after intra summarization.
            // The intra allocation is released first, but the replayed DSU
            // remains live together with cross-summary scratch.
            CrossUnionState::Deferred => dense_union_find_bytes(atoms.len()),
        })
        .unwrap_or(0);
    let chain_matrix_state_bytes = state
        .chain_matrix
        .as_ref()
        .map(|matrix| match matrix {
            ChainMatrixState::Resident(matrix) => {
                let resident = matrix
                    .capacity()
                    .saturating_mul(std::mem::size_of::<SparseUnionFind>())
                    .saturating_add(
                        matrix
                            .iter()
                            .map(sparse_union_find_resident_bytes)
                            .fold(0usize, usize::saturating_add),
                    );
                // Pair summaries destructively flatten one sparse DSU and sort
                // compact local identities. All other pair states remain live,
                // so include the largest such u32 index vector in the peak.
                let max_pair_summary_scratch = matrix
                    .iter()
                    .map(|union_find| {
                        union_find
                            .atom_count()
                            .saturating_mul(std::mem::size_of::<u32>())
                    })
                    .max()
                    .unwrap_or(0);
                resident.saturating_add(max_pair_summary_scratch)
            }
            // Pair replay happens only after intra/cross DSUs are released;
            // count its largest dense pair allocation explicitly anyway so
            // summary admission never hides the fallback's peak.
            ChainMatrixState::Spill(spill) => spill
                .resident_bytes()
                .saturating_add(spill.max_pair_replay_bytes()),
        })
        .unwrap_or(0);

    name_summary_scratch_plan(NameSummaryMemoryShape {
        analysis_budget_bytes,
        base_resident_bytes,
        atom_count: atoms.len(),
        max_chain_atom_count,
        cross_atom_count,
        max_cross_chain_atom_count,
        chain_count,
        intra_state_bytes,
        cross_state_bytes,
        chain_matrix_state_bytes,
    })
}

pub(crate) fn union_find_resident_bytes(union_find: &UnionFind) -> usize {
    union_find
        .parent
        .capacity()
        .saturating_mul(std::mem::size_of::<u32>())
        .saturating_add(
            union_find
                .rank
                .capacity()
                .saturating_mul(std::mem::size_of::<u8>()),
        )
}

fn hash_map_bucket_count(capacity: usize) -> usize {
    if capacity == 0 {
        return 0;
    }
    capacity
        .saturating_mul(8)
        .saturating_add(6)
        .checked_div(7)
        .unwrap_or(usize::MAX)
        .max(4)
        .checked_next_power_of_two()
        .unwrap_or(usize::MAX)
}

pub(crate) fn sparse_union_find_resident_bytes(union_find: &SparseUnionFind) -> usize {
    let buckets = hash_map_bucket_count(union_find.index_by_atom.capacity());
    let hash_map_bytes = if buckets == 0 {
        0
    } else {
        buckets
            .saturating_mul(std::mem::size_of::<(u32, u32)>())
            .saturating_add(buckets)
            // hashbrown keeps one SIMD control group mirrored past the bucket array.
            .saturating_add(16)
    };
    hash_map_bytes
        .saturating_add(
            union_find
                .atoms
                .capacity()
                .saturating_mul(std::mem::size_of::<u32>()),
        )
        .saturating_add(
            union_find
                .parent
                .capacity()
                .saturating_mul(std::mem::size_of::<u32>()),
        )
        .saturating_add(
            union_find
                .rank
                .capacity()
                .saturating_mul(std::mem::size_of::<u8>()),
        )
}

pub(crate) fn push_name_summary_rows<A: NameAtomStore + ?Sized>(
    rows: &mut Vec<SummaryRow>,
    atoms: &A,
    atoms_by_chain: &mut [Vec<u32>],
    chains: &[String],
    totals: &HashMap<String, NameTotals>,
    state: &mut ThresholdUnionState,
    plan: NameSummaryScratchPlan,
) -> Result<(), AnalysisError> {
    let intra_summaries = match plan.intra_strategy {
        NameSummaryStrategy::DenseOnePass => {
            let mut summaries = Vec::with_capacity(chains.len());
            let mut dense_scratch = DenseComponentScratch::with_touched_capacity(
                atoms.len(),
                plan.max_chain_atom_count,
            );
            for primary_atoms in atoms_by_chain.iter() {
                summaries.push(summarize_components_for_primary_with_scratch(
                    atoms,
                    primary_atoms,
                    &mut state.intra,
                    &mut dense_scratch,
                ));
            }
            summaries
        }
        NameSummaryStrategy::LowMemory => {
            let intra = std::mem::replace(&mut state.intra, UnionFind::new(0));
            summarize_components_by_chain_low_memory(atoms, atoms_by_chain, intra)
        }
    };
    // Cross-chain summarization never needs the dense intra-chain DSU.
    drop(std::mem::replace(&mut state.intra, UnionFind::new(0)));

    let cross = match state.cross.take() {
        Some(CrossUnionState::Deferred) => {
            let Some(ChainMatrixState::Spill(spill)) = &mut state.chain_matrix else {
                return Err(AnalysisError::InvalidData(
                    "deferred name cross summary requires chain-matrix spill files".to_string(),
                ));
            };
            Some(CrossUnionState::Dense(
                spill.replay_global_dense(atoms.len(), atoms_by_chain)?,
            ))
        }
        cross => cross,
    };
    let cross_summaries = cross.map(|cross| match (cross, plan.cross_strategy) {
        (CrossUnionState::Sparse(mut cross), NameSummaryStrategy::DenseOnePass) => {
            let mut dense_scratch = DenseComponentScratch::with_touched_capacity(
                cross.atom_count(),
                plan.max_cross_chain_atom_count,
            );
            summarize_sparse_components_by_chain(
                atoms,
                &mut cross,
                chains.len(),
                &mut dense_scratch,
            )
        }
        (CrossUnionState::Sparse(cross), NameSummaryStrategy::LowMemory) => {
            summarize_sparse_components_by_chain_low_memory(atoms, cross, chains.len())
        }
        (CrossUnionState::Dense(mut cross), NameSummaryStrategy::DenseOnePass) => {
            let mut dense_scratch = DenseComponentScratch::with_touched_capacity(
                atoms.len(),
                plan.max_cross_chain_atom_count,
            );
            summarize_dense_cross_components_by_chain(
                atoms,
                &mut cross,
                chains.len(),
                &mut dense_scratch,
            )
        }
        (CrossUnionState::Dense(cross), NameSummaryStrategy::LowMemory) => {
            summarize_dense_cross_components_by_chain_low_memory(atoms, cross, chains.len())
        }
        (CrossUnionState::Deferred, _) => {
            unreachable!("deferred cross state was reconstructed before summarization")
        }
    });

    for (chain_index, primary) in chains.iter().enumerate() {
        let total = totals.get(primary).copied().unwrap_or(NameTotals {
            contracts: 0,
            nfts: 0,
        });
        rows.push(summary_row(
            SummarySpec {
                field_name: "name",
                scope: "intra_chain",
                primary_chain: primary,
                secondary_chain: "",
                threshold: Some(state.threshold),
                match_mode: "jaro_winkler",
                metric: "duplicate_group",
                total_contracts: total.contracts,
                total_nfts: total.nfts,
            },
            intra_summaries[chain_index],
        ));

        if let Some(cross_summaries) = &cross_summaries {
            rows.push(summary_row(
                SummarySpec {
                    field_name: "name",
                    scope: "cross_chain_summary",
                    primary_chain: primary,
                    secondary_chain: "",
                    threshold: Some(state.threshold),
                    match_mode: "jaro_winkler",
                    metric: "duplicate_group",
                    total_contracts: total.contracts,
                    total_nfts: total.nfts,
                },
                cross_summaries[chain_index],
            ));
        }
    }
    Ok(())
}

pub(crate) fn chain_matrix_reuse_state_bytes(atoms_by_chain: &[Vec<u32>]) -> usize {
    let mut bytes = chain_pair_count(atoms_by_chain.len())
        .saturating_mul(std::mem::size_of::<SparseUnionFind>());
    for left in 0..atoms_by_chain.len() {
        for right in left + 1..atoms_by_chain.len() {
            bytes = bytes.saturating_add(sparse_union_find_bytes(
                atoms_by_chain[left].len() + atoms_by_chain[right].len(),
            ));
        }
    }
    bytes
}

pub(crate) fn new_chain_matrix_reuse_states(pair_count: usize) -> Vec<SparseUnionFind> {
    std::iter::repeat_with(SparseUnionFind::default)
        .take(pair_count)
        .collect()
}

pub(crate) fn chain_pair_count(chain_count: usize) -> usize {
    chain_count.saturating_mul(chain_count.saturating_sub(1)) / 2
}

pub(crate) fn chain_pair_index(left: usize, right: usize, chain_count: usize) -> usize {
    debug_assert!(left < right);
    left * (2 * chain_count - left - 1) / 2 + (right - left - 1)
}

pub(crate) fn chain_pair_from_index(mut index: usize, chain_count: usize) -> (usize, usize) {
    for left in 0..chain_count {
        let row_width = chain_count - left - 1;
        if index < row_width {
            return (left, left + index + 1);
        }
        index -= row_width;
    }
    unreachable!("chain pair index out of range")
}

pub(crate) fn dense_union_find_bytes(atom_count: usize) -> usize {
    atom_count.saturating_mul(std::mem::size_of::<u32>() + std::mem::size_of::<u8>())
}

pub(crate) fn sparse_union_find_bytes(atom_count: usize) -> usize {
    let buckets = hash_map_bucket_count(atom_count);
    let hash_map_bytes = if buckets == 0 {
        0
    } else {
        buckets
            .saturating_mul(std::mem::size_of::<(u32, u32)>())
            .saturating_add(buckets)
            .saturating_add(16)
    };
    let identity_vector_capacity = if atom_count == 0 {
        0
    } else {
        atom_count
            .max(4)
            .checked_next_power_of_two()
            .unwrap_or(usize::MAX)
    };
    // RawVec's minimum non-zero allocation is eight elements for one-byte
    // values and four for wider values. Model the u8 rank vector separately
    // so preflight covers the first sparse insertion exactly.
    let rank_vector_capacity = if atom_count == 0 {
        0
    } else {
        atom_count
            .max(8)
            .checked_next_power_of_two()
            .unwrap_or(usize::MAX)
    };
    hash_map_bytes
        .saturating_add(
            identity_vector_capacity
                .saturating_mul(std::mem::size_of::<u32>() + std::mem::size_of::<u32>()),
        )
        .saturating_add(rank_vector_capacity.saturating_mul(std::mem::size_of::<u8>()))
}

#[cfg(test)]
pub(crate) fn name_atoms_memory_bytes<T: NameValue>(atoms: &Vec<T>) -> usize {
    let struct_bytes = atoms.capacity().saturating_mul(std::mem::size_of::<T>());
    let string_bytes = atoms
        .iter()
        .map(|atom| {
            atom.normalized_name()
                .len()
                .saturating_add(2 * std::mem::size_of::<usize>())
        })
        .sum::<usize>();
    struct_bytes.saturating_add(string_bytes)
}

#[cfg(test)]
pub(crate) fn canonical_name_build_peak_bytes(atoms: &Vec<NameAtom>) -> usize {
    let atom_count = atoms.len();
    name_atoms_memory_bytes(atoms)
        .saturating_add(atom_count.saturating_mul(std::mem::size_of::<CanonicalNameAtom>()))
        .saturating_add(
            atom_count
                .saturating_mul(2)
                .saturating_add(1)
                .saturating_mul(std::mem::size_of::<u32>()),
        )
}

#[cfg(not(test))]
pub(crate) fn canonical_name_build_peak_bytes(atoms: &LoadedNameAtoms) -> usize {
    let atom_count = atoms.len();
    let canonical_column_bytes =
        atom_count.saturating_mul(std::mem::size_of::<u64>() + std::mem::size_of::<u32>());
    let canonical_resident_bytes = match atoms.atoms.mode() {
        NameAtomStorageMode::Resident => canonical_column_bytes.saturating_add(7) / 8 * 8,
        NameAtomStorageMode::Mapped => {
            canonical_column_bytes.min(NAME_MAPPED_ATOM_WORKING_SET_BYTES)
        }
    };
    atoms
        .atoms
        .resident_bytes()
        .saturating_add(canonical_resident_bytes)
        .saturating_add(
            atom_count
                .saturating_mul(2)
                .saturating_add(1)
                .saturating_mul(std::mem::size_of::<u32>()),
        )
}

#[cfg(test)]
pub(crate) fn name_atom_sets_memory_bytes(
    original: &Vec<NameAtom>,
    canonical: &Vec<CanonicalNameAtom>,
) -> usize {
    let structural = original
        .capacity()
        .saturating_mul(std::mem::size_of::<NameAtom>())
        .saturating_add(
            canonical
                .capacity()
                .saturating_mul(std::mem::size_of::<CanonicalNameAtom>()),
        );
    let shared_name_allocations = canonical
        .iter()
        .map(|atom| {
            atom.name_norm
                .len()
                .saturating_add(2 * std::mem::size_of::<usize>())
        })
        .sum::<usize>();
    structural.saturating_add(shared_name_allocations)
}

#[cfg(not(test))]
pub(crate) fn name_atom_sets_memory_bytes(
    original: &LoadedNameAtoms,
    canonical: &CanonicalNameValues,
) -> usize {
    original
        .atoms
        .resident_bytes()
        .saturating_add(canonical.atoms.resident_bytes())
}
