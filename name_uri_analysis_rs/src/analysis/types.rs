#![cfg_attr(not(test), allow(dead_code))]

use super::*;

#[cfg(test)]
pub(crate) type NameText = std::sync::Arc<str>;

#[cfg(not(test))]
#[derive(Clone)]
pub(crate) struct NameStringArena {
    storage: std::sync::Arc<NameStringStorage>,
    mode: NameStringStorageMode,
}

#[cfg(not(test))]
impl NameStringArena {
    #[inline]
    pub(crate) fn get(&self, offset: u64) -> &str {
        self.storage.string_at(offset as usize)
    }

    pub(crate) fn mode(&self) -> NameStringStorageMode {
        self.mode
    }

    pub(crate) fn bytes(&self) -> usize {
        self.storage.len
    }
}

#[cfg(not(test))]
enum NameStringStorageBytes {
    Resident(Box<[u8]>),
    Mapped(MappedNameStrings),
}

#[cfg(not(test))]
struct MappedNameStrings {
    mmap: Option<memmap2::MmapMut>,
    directory: PathBuf,
}

#[cfg(not(test))]
impl Drop for MappedNameStrings {
    fn drop(&mut self) {
        drop(self.mmap.take());
        if self.directory.exists() {
            let _ = fs::remove_dir_all(&self.directory);
        }
    }
}

#[cfg(not(test))]
struct NameStringStorage {
    bytes: NameStringStorageBytes,
    base: std::ptr::NonNull<u8>,
    len: usize,
}

// The arena builder is the only writer and appends into disjoint, previously
// unpublished byte ranges. Once loading finishes, all accesses are immutable.
// NameStringArena keeps the allocation alive while scoring shares immutable
// views across Rayon workers.
#[cfg(not(test))]
unsafe impl Send for NameStringStorage {}
#[cfg(not(test))]
unsafe impl Sync for NameStringStorage {}

#[cfg(not(test))]
impl NameStringStorage {
    #[inline]
    fn string_at(&self, offset: usize) -> &str {
        let header_end = offset + std::mem::size_of::<u32>();
        debug_assert!(header_end <= self.len);
        // SAFETY: the offset was validated while constructing NameText; the
        // four-byte length header is written before NameText is published.
        let byte_len = unsafe {
            u32::from_le(std::ptr::read_unaligned(
                self.base.as_ptr().add(offset).cast::<u32>(),
            )) as usize
        };
        let value_end = header_end + byte_len;
        debug_assert!(value_end <= self.len);
        // SAFETY: the loader copies bytes from a valid UTF-8 Arrow string into
        // this immutable range and the Arc storage outlives the returned view.
        unsafe {
            std::str::from_utf8_unchecked(std::slice::from_raw_parts(
                self.base.as_ptr().add(header_end),
                byte_len,
            ))
        }
    }

    fn flush_mapped_async(&self) {
        if let NameStringStorageBytes::Mapped(mapped) = &self.bytes {
            let _ = mapped
                .mmap
                .as_ref()
                .expect("mapped name arena remains open")
                .flush_async();
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NameStringStorageMode {
    Resident,
    Mapped,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NameAtomStorageMode {
    Resident,
    Mapped,
}

#[cfg(not(test))]
pub(crate) struct NameStringArenaBuilder {
    storage: std::sync::Arc<NameStringStorage>,
    written: usize,
}

#[cfg(not(test))]
fn mapped_name_string_storage(
    arena_bytes: usize,
    scratch_directory: &Path,
) -> Result<NameStringStorageBytes, AnalysisError> {
    if arena_bytes == 0 {
        return Ok(NameStringStorageBytes::Resident(Box::new([])));
    }
    let directory = scratch_directory.join("name-string-arena");
    if directory.exists() {
        fs::remove_dir_all(&directory)?;
    }
    fs::create_dir_all(&directory)?;
    let path = directory.join("strings.bin");
    let file = std::fs::File::create(path)?;
    file.set_len(u64::try_from(arena_bytes).map_err(|_| {
        AnalysisError::InvalidData("name string arena exceeds file offset space".into())
    })?)?;
    // SAFETY: the file is exclusively owned by this arena, has exactly
    // arena_bytes bytes, and remains mapped until all NameText references are
    // dropped.
    let mmap = unsafe {
        memmap2::MmapOptions::new()
            .len(arena_bytes)
            .map_mut(&file)?
    };
    Ok(NameStringStorageBytes::Mapped(MappedNameStrings {
        mmap: Some(mmap),
        directory,
    }))
}

#[cfg(not(test))]
impl NameStringArenaBuilder {
    // `try_reserve_exact` is required here: the usual `vec![0; n]` aborts the
    // process on allocation failure and cannot retry the mmap fallback.
    #[allow(clippy::slow_vector_initialization)]
    pub(crate) fn new(
        mode: NameStringStorageMode,
        arena_bytes: usize,
        scratch_directory: &Path,
    ) -> Result<Self, AnalysisError> {
        let bytes = match mode {
            NameStringStorageMode::Resident => {
                let mut resident = Vec::new();
                match resident.try_reserve_exact(arena_bytes) {
                    Ok(()) => {
                        resident.resize(arena_bytes, 0);
                        NameStringStorageBytes::Resident(resident.into_boxed_slice())
                    }
                    Err(error) => {
                        if disk_fallback_disabled() {
                            std::alloc::handle_alloc_error(
                                std::alloc::Layout::array::<u8>(arena_bytes)
                                    .unwrap_or_else(|_| std::alloc::Layout::new::<u8>()),
                            );
                        }
                        eprintln!(
                            "warning: resident name string arena allocation of {} failed ({error}); \
                             retrying with file-backed mmap under {}",
                            format_byte_size(arena_bytes),
                            scratch_directory.join("name-string-arena").display(),
                        );
                        mapped_name_string_storage(arena_bytes, scratch_directory)?
                    }
                }
            }
            NameStringStorageMode::Mapped => {
                mapped_name_string_storage(arena_bytes, scratch_directory)?
            }
        };
        let (base, len) = match &bytes {
            NameStringStorageBytes::Resident(bytes) => (
                std::ptr::NonNull::new(bytes.as_ptr().cast_mut())
                    .unwrap_or_else(std::ptr::NonNull::dangling),
                bytes.len(),
            ),
            NameStringStorageBytes::Mapped(mapped) => {
                let mmap = mapped.mmap.as_ref().expect("mapped name arena is open");
                (
                    std::ptr::NonNull::new(mmap.as_ptr().cast_mut())
                        .unwrap_or_else(std::ptr::NonNull::dangling),
                    mmap.len(),
                )
            }
        };
        Ok(Self {
            storage: std::sync::Arc::new(NameStringStorage { bytes, base, len }),
            written: 0,
        })
    }

    pub(crate) fn push(&mut self, value: &str) -> Result<u64, AnalysisError> {
        let byte_len = u32::try_from(value.len())
            .map_err(|_| AnalysisError::InvalidData("one normalized name exceeds 4 GiB".into()))?;
        let offset = self.written;
        let header_end = offset
            .checked_add(std::mem::size_of::<u32>())
            .ok_or_else(|| AnalysisError::InvalidData("name string arena overflow".into()))?;
        let value_end = header_end
            .checked_add(value.len())
            .ok_or_else(|| AnalysisError::InvalidData("name string arena overflow".into()))?;
        if value_end > self.storage.len {
            return Err(AnalysisError::InvalidData(
                "name string bytes changed while loading the stable atom snapshot".into(),
            ));
        }
        // SAFETY: the builder advances monotonically, so this range is
        // disjoint from every previously published NameText. The fixed backing
        // allocation never moves.
        unsafe {
            std::ptr::copy_nonoverlapping(
                byte_len.to_le_bytes().as_ptr(),
                self.storage.base.as_ptr().add(offset),
                std::mem::size_of::<u32>(),
            );
            std::ptr::copy_nonoverlapping(
                value.as_ptr(),
                self.storage.base.as_ptr().add(header_end),
                value.len(),
            );
        }
        self.written = value_end;
        Ok(offset as u64)
    }

    pub(crate) fn finish(self) -> Result<NameStringArena, AnalysisError> {
        if self.written != self.storage.len {
            return Err(AnalysisError::InvalidData(format!(
                "name string byte total changed while loading: expected={}, loaded={}",
                self.storage.len, self.written
            )));
        }
        self.storage.flush_mapped_async();
        let mode = match &self.storage.bytes {
            NameStringStorageBytes::Resident(_) => NameStringStorageMode::Resident,
            NameStringStorageBytes::Mapped(_) => NameStringStorageMode::Mapped,
        };
        Ok(NameStringArena {
            storage: self.storage,
            mode,
        })
    }
}

#[derive(Debug, Error)]
pub enum AnalysisError {
    #[error("at least one parquet input is required")]
    MissingParquetInput,
    #[error("invalid data: {0}")]
    InvalidData(String),
    #[error(transparent)]
    DuckDb(#[from] duckdb::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
}

#[derive(Clone, Debug, Serialize, serde::Deserialize, PartialEq)]
pub struct AnalysisOptions {
    pub database_path: PathBuf,
    pub parquet_inputs: Vec<PathBuf>,
    pub output_dir: PathBuf,
    pub name_threshold: f64,
    pub threads: usize,
    /// Total analysis memory budget string (DuckDB-oriented / overall planning).
    pub memory_limit: String,
    /// Optional hard Rust-side analysis cap. The CLI sets both this and
    /// `memory_limit` to the same `--analysis-memory-limit` value.
    pub analysis_memory_limit: Option<String>,
    pub duckdb_memory_limit: String,
    pub temp_directory: Option<PathBuf>,
    pub progress: bool,
}

#[derive(Clone, Debug, Serialize, serde::Deserialize, PartialEq)]
pub struct SummaryRow {
    pub field_name: String,
    pub scope: String,
    pub primary_chain: String,
    pub secondary_chain: String,
    pub threshold: Option<f64>,
    pub match_mode: String,
    pub metric: String,
    pub total_contracts: i64,
    pub total_nfts: i64,
    pub group_count: i64,
    pub duplicate_contract_count: i64,
    pub duplicate_nft_count: i64,
    pub duplicate_contract_ratio: f64,
    pub duplicate_nft_ratio: f64,
    pub group_size_ge_2_count: i64,
    pub group_size_gt_2_count: i64,
}

#[derive(Clone, Debug, Serialize, serde::Deserialize, PartialEq)]
pub struct AnalysisReport {
    pub summary_rows: Vec<SummaryRow>,
}

#[cfg(test)]
#[derive(Clone, Debug)]
pub(crate) struct NameAtom {
    pub(crate) chain_index: usize,
    pub(crate) name_norm: NameText,
    pub(crate) char_len: usize,
    pub(crate) contract_count: i64,
    pub(crate) nft_count: i64,
}

pub(crate) trait NameAtomStore: Sync {
    fn len(&self) -> usize;
    fn chain_index(&self, index: usize) -> usize;
    fn chain_local_rank(&self, index: usize) -> u32;
    #[cfg(not(test))]
    fn name_offset(&self, index: usize) -> u64;
    #[cfg(not(test))]
    fn char_len(&self, index: usize) -> usize;
    fn contract_count(&self, index: usize) -> i64;
    fn nft_count(&self, index: usize) -> i64;
}

#[cfg(test)]
impl NameAtomStore for [NameAtom] {
    fn len(&self) -> usize {
        <[NameAtom]>::len(self)
    }

    fn chain_index(&self, index: usize) -> usize {
        self[index].chain_index
    }

    fn chain_local_rank(&self, index: usize) -> u32 {
        u32::try_from(
            self[..index]
                .iter()
                .filter(|candidate| candidate.chain_index == self[index].chain_index)
                .count(),
        )
        .expect("test chain-local atom rank exceeds u32")
    }

    fn contract_count(&self, index: usize) -> i64 {
        self[index].contract_count
    }

    fn nft_count(&self, index: usize) -> i64 {
        self[index].nft_count
    }
}

#[cfg(test)]
impl NameAtomStore for Vec<NameAtom> {
    fn len(&self) -> usize {
        Vec::len(self)
    }

    fn chain_index(&self, index: usize) -> usize {
        self.as_slice().chain_index(index)
    }

    fn chain_local_rank(&self, index: usize) -> u32 {
        self.as_slice().chain_local_rank(index)
    }

    fn contract_count(&self, index: usize) -> i64 {
        self.as_slice().contract_count(index)
    }

    fn nft_count(&self, index: usize) -> i64 {
        self.as_slice().nft_count(index)
    }
}

pub(crate) trait NameValue: Sync {
    fn normalized_name(&self) -> &str;
    fn char_len(&self) -> usize;
}

pub(crate) trait NameValueStore: Sync {
    fn len(&self) -> usize;
    fn normalized_name(&self, index: usize) -> &str;
    fn char_len(&self, index: usize) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T: NameValue> NameValueStore for Vec<T> {
    fn len(&self) -> usize {
        Vec::len(self)
    }

    fn normalized_name(&self, index: usize) -> &str {
        self[index].normalized_name()
    }

    fn char_len(&self, index: usize) -> usize {
        self[index].char_len()
    }
}

impl<T: NameValue> NameValueStore for [T] {
    fn len(&self) -> usize {
        <[T]>::len(self)
    }

    fn normalized_name(&self, index: usize) -> &str {
        self[index].normalized_name()
    }

    fn char_len(&self, index: usize) -> usize {
        self[index].char_len()
    }
}

#[cfg(test)]
impl NameValue for NameAtom {
    #[inline]
    fn normalized_name(&self) -> &str {
        self.name_norm.as_ref()
    }

    #[inline]
    fn char_len(&self) -> usize {
        self.char_len
    }
}

#[derive(Clone, Copy)]
pub(crate) struct ScoredRight {
    pub(crate) right: usize,
    pub(crate) score: f64,
}

#[derive(Clone, Copy)]
pub(crate) struct NameTotals {
    pub(crate) contracts: i64,
    pub(crate) nfts: i64,
}

#[derive(Clone, Copy, Default)]
pub(crate) struct UriCounts {
    pub(crate) v1_nfts: i64,
    pub(crate) v1_contracts: i64,
    pub(crate) v2_nfts: i64,
    pub(crate) v2_contracts: i64,
    pub(crate) v3_nfts: i64,
    pub(crate) v3_contracts: i64,
}

#[derive(Clone, Copy, Default)]
pub(crate) struct UriContractCounts {
    pub(crate) intra_chain: UriCounts,
    pub(crate) cross_chain: UriCounts,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) struct GroupSummary {
    pub(crate) group_count: i64,
    pub(crate) duplicate_contract_count: i64,
    pub(crate) duplicate_nft_count: i64,
    pub(crate) group_size_ge_2_count: i64,
    pub(crate) group_size_gt_2_count: i64,
}

pub(crate) struct SummarySpec<'a> {
    pub(crate) field_name: &'a str,
    pub(crate) scope: &'a str,
    pub(crate) primary_chain: &'a str,
    pub(crate) secondary_chain: &'a str,
    pub(crate) threshold: Option<f64>,
    pub(crate) match_mode: &'a str,
    pub(crate) metric: &'a str,
    pub(crate) total_contracts: i64,
    pub(crate) total_nfts: i64,
}

pub(crate) struct ChainMatrixRowSpec<'a> {
    pub(crate) chains: &'a [String],
    pub(crate) totals: &'a HashMap<String, NameTotals>,
    pub(crate) primary_index: usize,
    pub(crate) secondary_index: usize,
    pub(crate) threshold: f64,
}

#[derive(Clone, Copy, Default)]
pub(crate) struct PairComponentAccumulator {
    pub(crate) left_contract_count: i64,
    pub(crate) left_nft_count: i64,
    pub(crate) right_contract_count: i64,
    pub(crate) right_nft_count: i64,
    pub(crate) total_contract_count: i64,
}

pub(crate) struct UnionFind {
    pub(crate) parent: Vec<u32>,
    pub(crate) rank: Vec<u8>,
}

impl UnionFind {
    pub(crate) fn new(size: usize) -> Self {
        assert!(
            size <= u32::MAX as usize,
            "union-find size exceeds compact u32 identity space"
        );
        Self {
            parent: (0..size as u32).collect(),
            rank: vec![0; size],
        }
    }

    pub(crate) fn find(&mut self, node: usize) -> usize {
        let mut current = node;
        loop {
            let parent = self.parent[current] as usize;
            if parent == current {
                return current;
            }
            let grandparent = self.parent[parent];
            self.parent[current] = grandparent;
            current = grandparent as usize;
        }
    }

    pub(crate) fn union(&mut self, left: usize, right: usize) {
        let left_root = self.find(left);
        let right_root = self.find(right);
        if left_root == right_root {
            return;
        }
        if self.rank[left_root] < self.rank[right_root] {
            self.parent[left_root] = right_root as u32;
        } else if self.rank[left_root] > self.rank[right_root] {
            self.parent[right_root] = left_root as u32;
        } else {
            self.parent[right_root] = left_root as u32;
            self.rank[left_root] += 1;
        }
    }
}

pub(crate) struct ThresholdUnionState {
    pub(crate) threshold: f64,
    pub(crate) intra: UnionFind,
    pub(crate) cross: Option<CrossUnionState>,
    pub(crate) chain_matrix: Option<ChainMatrixState>,
}

pub(crate) struct PairwiseNameState {
    pub(crate) threshold: f64,
    pub(crate) intra_matched_atoms: Vec<bool>,
    pub(crate) cross_matched_atoms: Option<Vec<bool>>,
    pub(crate) chain_matrix_matched_atoms: Option<Vec<Vec<bool>>>,
    pub(crate) intra_pair_counts: Vec<i64>,
    pub(crate) cross_pair_counts: Option<Vec<i64>>,
    pub(crate) chain_matrix_pair_counts: Option<Vec<i64>>,
}

impl PairwiseNameState {
    pub(crate) fn new(atom_count: usize, chain_count: usize, threshold: f64) -> Self {
        let pair_count = chain_count.saturating_mul(chain_count.saturating_sub(1)) / 2;
        let has_cross_chain = chain_count > 1;
        Self {
            threshold,
            intra_matched_atoms: vec![false; atom_count],
            cross_matched_atoms: has_cross_chain.then(|| vec![false; atom_count]),
            chain_matrix_matched_atoms: has_cross_chain
                .then(|| (0..pair_count).map(|_| vec![false; atom_count]).collect()),
            intra_pair_counts: vec![0; chain_count],
            cross_pair_counts: has_cross_chain.then(|| vec![0; chain_count]),
            chain_matrix_pair_counts: has_cross_chain.then(|| vec![0; pair_count]),
        }
    }

    pub(crate) fn mark_intra(&mut self, chain: usize, atoms: &[usize], pairs: i64) {
        for &atom in atoms {
            self.intra_matched_atoms[atom] = true;
        }
        self.intra_pair_counts[chain] = self.intra_pair_counts[chain].saturating_add(pairs);
    }

    pub(crate) fn mark_cross(
        &mut self,
        left_chain: usize,
        right_chain: usize,
        left_atom: usize,
        right_atom: usize,
        pair_index: usize,
        pairs: i64,
    ) {
        let cross_atoms = self
            .cross_matched_atoms
            .as_mut()
            .expect("cross-chain pair requires cross match state");
        cross_atoms[left_atom] = true;
        cross_atoms[right_atom] = true;
        let cross_counts = self
            .cross_pair_counts
            .as_mut()
            .expect("cross-chain pair requires cross counters");
        cross_counts[left_chain] = cross_counts[left_chain].saturating_add(pairs);
        cross_counts[right_chain] = cross_counts[right_chain].saturating_add(pairs);

        let matrix_atoms = self
            .chain_matrix_matched_atoms
            .as_mut()
            .expect("cross-chain pair requires chain-matrix state");
        matrix_atoms[pair_index][left_atom] = true;
        matrix_atoms[pair_index][right_atom] = true;
        let matrix_counts = self
            .chain_matrix_pair_counts
            .as_mut()
            .expect("cross-chain pair requires chain-matrix counters");
        matrix_counts[pair_index] = matrix_counts[pair_index].saturating_add(pairs);
    }
}

pub(crate) enum CrossUnionState {
    Sparse(SparseUnionFind),
    Dense(UnionFind),
    Deferred,
}

impl CrossUnionState {
    pub(crate) fn union(&mut self, left: usize, right: usize) {
        match self {
            Self::Sparse(union_find) => union_find.union(left, right),
            Self::Dense(union_find) => union_find.union(left, right),
            Self::Deferred => {}
        }
    }

    #[cfg(test)]
    pub(crate) fn connected(&mut self, left: usize, right: usize) -> bool {
        match self {
            Self::Sparse(union_find) => union_find.connected(left, right),
            Self::Dense(union_find) => union_find.find(left) == union_find.find(right),
            Self::Deferred => false,
        }
    }
}

pub(crate) enum ChainMatrixState {
    Resident(Vec<SparseUnionFind>),
    Spill(ChainMatrixSpill),
}

pub(crate) struct ChainMatrixSpill {
    pub(crate) directory: PathBuf,
    pub(crate) writers: Vec<Option<std::io::BufWriter<std::fs::File>>>,
    pub(crate) pair_layouts: Vec<ChainPairAtomLayout>,
    pub(crate) first_error: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ChainPairAtomLayout {
    pub(crate) primary_count: u32,
    pub(crate) total_count: u32,
}

#[derive(Default)]
pub(crate) struct SparseUnionFind {
    pub(crate) index_by_atom: HashMap<u32, u32>,
    pub(crate) atoms: Vec<u32>,
    pub(crate) parent: Vec<u32>,
    pub(crate) rank: Vec<u8>,
}

impl SparseUnionFind {
    pub(crate) fn get_or_insert(&mut self, atom: usize) -> usize {
        let atom = u32::try_from(atom).expect("name atom identity exceeds u32");
        if let Some(index) = self.index_by_atom.get(&atom).copied() {
            return index as usize;
        }

        let index = u32::try_from(self.atoms.len()).expect("sparse union local index exceeds u32");
        self.index_by_atom.insert(atom, index);
        self.atoms.push(atom);
        self.parent.push(index);
        self.rank.push(0);
        index as usize
    }

    pub(crate) fn find_local(&mut self, node: usize) -> usize {
        let mut current = node;
        loop {
            let parent = self.parent[current] as usize;
            if parent == current {
                return current;
            }
            let grandparent = self.parent[parent];
            self.parent[current] = grandparent;
            current = grandparent as usize;
        }
    }

    pub(crate) fn union(&mut self, left: usize, right: usize) {
        let left = self.get_or_insert(left);
        let right = self.get_or_insert(right);

        let left_root = self.find_local(left);
        let right_root = self.find_local(right);
        if left_root == right_root {
            return;
        }

        let left_rank = self.rank[left_root];
        let right_rank = self.rank[right_root];
        if left_rank < right_rank {
            self.parent[left_root] = right_root as u32;
        } else if left_rank > right_rank {
            self.parent[right_root] = left_root as u32;
        } else {
            self.parent[right_root] = left_root as u32;
            self.rank[left_root] += 1;
        }
    }

    #[cfg(test)]
    pub(crate) fn connected(&mut self, left: usize, right: usize) -> bool {
        let Ok(left) = u32::try_from(left) else {
            return false;
        };
        let Ok(right) = u32::try_from(right) else {
            return false;
        };
        let Some(left) = self.index_by_atom.get(&left).copied() else {
            return false;
        };
        let Some(right) = self.index_by_atom.get(&right).copied() else {
            return false;
        };
        self.find_local(left as usize) == self.find_local(right as usize)
    }

    pub(crate) fn atom_count(&self) -> usize {
        self.atoms.len()
    }

    pub(crate) fn atom_at(&self, local_index: usize) -> usize {
        self.atoms[local_index] as usize
    }
}
