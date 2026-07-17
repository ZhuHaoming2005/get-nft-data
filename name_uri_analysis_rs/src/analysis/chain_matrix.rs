use super::*;
use std::io::{BufReader, Read, Write};

pub(crate) const NAME_MATRIX_SPILL_BUFFER_BYTES: usize = 16 * 1024;
const NAME_MATRIX_SPILL_READ_BUFFER_BYTES: usize = 1024 * 1024;
const NAME_MATRIX_SPILL_FIXED_HEADROOM_BYTES: usize = 1024 * 1024;
const NAME_MATRIX_EDGE_BYTES: u64 = 2 * std::mem::size_of::<u32>() as u64;

fn pair_local_atom_index<A: NameAtomStore + ?Sized>(
    atoms: &A,
    global_index: usize,
    primary_chain: usize,
    secondary_chain: usize,
    layout: ChainPairAtomLayout,
) -> Result<u32, std::io::Error> {
    let rank = atoms.chain_local_rank(global_index);
    let chain_index = atoms.chain_index(global_index);
    let local = if chain_index == primary_chain {
        if rank >= layout.primary_count {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "name chain-matrix primary endpoint exceeds pair layout",
            ));
        }
        rank
    } else if chain_index == secondary_chain {
        if rank >= layout.total_count.saturating_sub(layout.primary_count) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "name chain-matrix secondary endpoint exceeds pair layout",
            ));
        }
        layout.primary_count.checked_add(rank).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "name chain-matrix local endpoint overflow",
            )
        })?
    } else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "name chain-matrix endpoint belongs to the wrong chain pair",
        ));
    };
    if local >= layout.total_count {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "name chain-matrix local endpoint exceeds pair layout",
        ));
    }
    Ok(local)
}

fn pair_global_atom_index(
    local: u32,
    layout: ChainPairAtomLayout,
    primary_atoms: &[u32],
    secondary_atoms: &[u32],
) -> Result<usize, AnalysisError> {
    if local < layout.primary_count {
        primary_atoms
            .get(local as usize)
            .map(|&index| index as usize)
    } else {
        secondary_atoms
            .get(local.saturating_sub(layout.primary_count) as usize)
            .map(|&index| index as usize)
    }
    .ok_or_else(|| {
        AnalysisError::InvalidData(
            "name chain-matrix spill contains an invalid pair-local endpoint".into(),
        )
    })
}

pub(crate) fn atoms_by_chain<A: NameAtomStore + ?Sized>(
    atoms: &A,
    chain_count: usize,
) -> Vec<Vec<u32>> {
    let mut indexes = vec![Vec::new(); chain_count];
    for index in 0..atoms.len() {
        indexes[atoms.chain_index(index)].push(index as u32);
    }
    indexes
}

pub(crate) fn chain_pair_atom_capacities(atoms_by_chain: &[Vec<u32>]) -> Vec<ChainPairAtomLayout> {
    let mut layouts = Vec::with_capacity(chain_pair_count(atoms_by_chain.len()));
    for left in 0..atoms_by_chain.len() {
        for right in left + 1..atoms_by_chain.len() {
            let primary_count = u32::try_from(atoms_by_chain[left].len())
                .expect("name chain atom count exceeds compact u32 identity space");
            let total_count = u32::try_from(
                atoms_by_chain[left]
                    .len()
                    .saturating_add(atoms_by_chain[right].len()),
            )
            .expect("name chain-pair atom count exceeds compact u32 identity space");
            layouts.push(ChainPairAtomLayout {
                primary_count,
                total_count,
            });
        }
    }
    layouts
}

pub(crate) fn chain_matrix_spill_reserve_bytes(chain_count: usize) -> usize {
    let pair_count = chain_pair_count(chain_count);
    NAME_MATRIX_SPILL_FIXED_HEADROOM_BYTES.saturating_add(
        pair_count.saturating_mul(
            NAME_MATRIX_SPILL_BUFFER_BYTES
                .saturating_add(std::mem::size_of::<Option<std::io::BufWriter<std::fs::File>>>())
                .saturating_add(std::mem::size_of::<ChainPairAtomLayout>())
                .saturating_add(128),
        ),
    )
}

impl ChainMatrixSpill {
    pub(crate) fn new(
        directory: PathBuf,
        pair_layouts: Vec<ChainPairAtomLayout>,
    ) -> Result<Self, AnalysisError> {
        if directory.exists() {
            fs::remove_dir_all(&directory)?;
        }
        fs::create_dir_all(&directory)?;
        let pair_count = pair_layouts.len();
        Ok(Self {
            directory,
            writers: std::iter::repeat_with(|| None).take(pair_count).collect(),
            pair_layouts,
            first_error: None,
        })
    }

    pub(crate) fn record_edge<A: NameAtomStore + ?Sized>(
        &mut self,
        pair_index: usize,
        left: usize,
        right: usize,
        atoms: &A,
    ) {
        if self.first_error.is_some() {
            return;
        }
        let result = (|| -> Result<(), std::io::Error> {
            if self.writers[pair_index].is_none() {
                let file = std::fs::File::create(self.pair_path(pair_index))?;
                self.writers[pair_index] = Some(std::io::BufWriter::with_capacity(
                    NAME_MATRIX_SPILL_BUFFER_BYTES,
                    file,
                ));
            }
            let layout = self.pair_layouts[pair_index];
            let left_chain = atoms.chain_index(left);
            let right_chain = atoms.chain_index(right);
            let primary_chain = left_chain.min(right_chain);
            let secondary_chain = left_chain.max(right_chain);
            let left = pair_local_atom_index(atoms, left, primary_chain, secondary_chain, layout)?;
            let right =
                pair_local_atom_index(atoms, right, primary_chain, secondary_chain, layout)?;
            let writer = self.writers[pair_index]
                .as_mut()
                .expect("chain-matrix writer was initialized");
            writer.write_all(&left.to_le_bytes())?;
            writer.write_all(&right.to_le_bytes())
        })();
        if let Err(error) = result {
            self.first_error = Some(format!(
                "name chain-matrix spill pair {pair_index}: {error}"
            ));
        }
    }

    pub(crate) fn finish_writes(&mut self) -> Result<(), AnalysisError> {
        for (pair_index, writer) in self.writers.iter_mut().enumerate() {
            if let Some(mut writer) = writer.take() {
                if let Err(error) = writer.flush() {
                    if self.first_error.is_none() {
                        self.first_error = Some(format!(
                            "name chain-matrix spill pair {pair_index}: {error}"
                        ));
                    }
                }
            }
        }
        self.check_error()
    }

    pub(crate) fn take_pair_union_find(
        &mut self,
        pair_index: usize,
    ) -> Result<UnionFind, AnalysisError> {
        if let Some(mut writer) = self.writers[pair_index].take() {
            writer.flush()?;
        }
        self.check_error()?;
        let path = self.pair_path(pair_index);
        let layout = self.pair_layouts[pair_index];
        if !path.exists() {
            return Ok(UnionFind::new(layout.total_count as usize));
        }
        let file = std::fs::File::open(&path)?;
        let bytes = file.metadata()?.len();
        if bytes % NAME_MATRIX_EDGE_BYTES != 0 {
            return Err(AnalysisError::InvalidData(format!(
                "name chain-matrix spill pair {pair_index} has truncated edge data"
            )));
        }
        let edge_count = usize::try_from(bytes / NAME_MATRIX_EDGE_BYTES).map_err(|_| {
            AnalysisError::InvalidData(
                "name chain-matrix spill edge count exceeds usize".to_string(),
            )
        })?;
        let pair_atom_count = layout.total_count as usize;
        let mut union_find = UnionFind::new(pair_atom_count);
        let mut reader = BufReader::with_capacity(NAME_MATRIX_SPILL_READ_BUFFER_BYTES, file);
        let mut record = [0u8; NAME_MATRIX_EDGE_BYTES as usize];
        for _ in 0..edge_count {
            reader.read_exact(&mut record)?;
            let left_local =
                u32::from_le_bytes(record[..4].try_into().expect("four-byte left atom")) as usize;
            let right_local =
                u32::from_le_bytes(record[4..].try_into().expect("four-byte right atom")) as usize;
            if left_local >= pair_atom_count || right_local >= pair_atom_count {
                return Err(AnalysisError::InvalidData(format!(
                    "name chain-matrix spill pair {pair_index} has an invalid chain-local endpoint"
                )));
            }
            union_find.union(left_local, right_local);
        }
        drop(reader);
        let _ = fs::remove_file(path);
        Ok(union_find)
    }

    pub(crate) fn replay_global_dense(
        &mut self,
        atom_count: usize,
        atoms_by_chain: &[Vec<u32>],
    ) -> Result<UnionFind, AnalysisError> {
        self.finish_writes()?;
        let mut union_find = UnionFind::new(atom_count);
        for pair_index in 0..self.pair_layouts.len() {
            let path = self.pair_path(pair_index);
            if !path.exists() {
                continue;
            }
            let file = std::fs::File::open(path)?;
            let bytes = file.metadata()?.len();
            if bytes % NAME_MATRIX_EDGE_BYTES != 0 {
                return Err(AnalysisError::InvalidData(format!(
                    "name chain-matrix spill pair {pair_index} has truncated edge data"
                )));
            }
            let edge_count = usize::try_from(bytes / NAME_MATRIX_EDGE_BYTES).map_err(|_| {
                AnalysisError::InvalidData(
                    "name chain-matrix spill edge count exceeds usize".to_string(),
                )
            })?;
            let (primary_chain, secondary_chain) =
                chain_pair_from_index(pair_index, atoms_by_chain.len());
            let layout = self.pair_layouts[pair_index];
            let primary_atoms = &atoms_by_chain[primary_chain];
            let secondary_atoms = &atoms_by_chain[secondary_chain];
            let mut reader = BufReader::with_capacity(NAME_MATRIX_SPILL_READ_BUFFER_BYTES, file);
            let mut record = [0u8; NAME_MATRIX_EDGE_BYTES as usize];
            for _ in 0..edge_count {
                reader.read_exact(&mut record)?;
                let left_local =
                    u32::from_le_bytes(record[..4].try_into().expect("four-byte left atom"));
                let right_local =
                    u32::from_le_bytes(record[4..].try_into().expect("four-byte right atom"));
                let left =
                    pair_global_atom_index(left_local, layout, primary_atoms, secondary_atoms)?;
                let right =
                    pair_global_atom_index(right_local, layout, primary_atoms, secondary_atoms)?;
                debug_assert!(left < atom_count && right < atom_count);
                union_find.union(left, right);
            }
        }
        Ok(union_find)
    }

    pub(crate) fn resident_bytes(&self) -> usize {
        self.directory
            .capacity()
            .saturating_add(
                self.writers.capacity().saturating_mul(std::mem::size_of::<
                    Option<std::io::BufWriter<std::fs::File>>,
                >()),
            )
            .saturating_add(
                self.pair_layouts
                    .capacity()
                    .saturating_mul(std::mem::size_of::<ChainPairAtomLayout>()),
            )
            .saturating_add(
                self.writers
                    .iter()
                    .filter(|writer| writer.is_some())
                    .count()
                    .saturating_mul(NAME_MATRIX_SPILL_BUFFER_BYTES),
            )
    }

    pub(crate) fn max_pair_replay_bytes(&self) -> usize {
        self.pair_layouts
            .iter()
            .map(|layout| layout.total_count as usize)
            .max()
            .map(|count| {
                dense_union_find_bytes(count)
                    .saturating_add(count.saturating_mul(std::mem::size_of::<u32>()))
            })
            .unwrap_or(0)
    }

    fn pair_path(&self, pair_index: usize) -> PathBuf {
        self.directory.join(format!("pair-{pair_index:08}.edges"))
    }

    fn check_error(&self) -> Result<(), AnalysisError> {
        self.first_error.as_ref().map_or(Ok(()), |error| {
            Err(AnalysisError::InvalidData(error.clone()))
        })
    }
}

impl Drop for ChainMatrixSpill {
    fn drop(&mut self) {
        self.writers.clear();
        if self.directory.exists() {
            let _ = fs::remove_dir_all(&self.directory);
        }
    }
}

pub(crate) fn push_reused_chain_matrix_rows<A: NameAtomStore + ?Sized>(
    rows: &mut Vec<SummaryRow>,
    atoms: &A,
    atoms_by_chain: &mut [Vec<u32>],
    chains: &[String],
    totals: &HashMap<String, NameTotals>,
    state: &mut ThresholdUnionState,
) -> Result<(), AnalysisError> {
    let Some(matrix) = state.chain_matrix.take() else {
        return Ok(());
    };
    match matrix {
        ChainMatrixState::Resident(mut matrix) => {
            for (pair_index, slot) in matrix.iter_mut().enumerate() {
                push_chain_matrix_pair(
                    rows,
                    atoms,
                    chains,
                    totals,
                    state.threshold,
                    pair_index,
                    std::mem::take(slot),
                );
            }
        }
        ChainMatrixState::Spill(mut spill) => {
            spill.finish_writes()?;
            for pair_index in 0..spill.pair_layouts.len() {
                let (primary_index, secondary_index) =
                    chain_pair_from_index(pair_index, chains.len());
                let union_find = spill.take_pair_union_find(pair_index)?;
                push_dense_chain_matrix_pair(
                    rows,
                    atoms,
                    chains,
                    totals,
                    state.threshold,
                    primary_index,
                    secondary_index,
                    &atoms_by_chain[primary_index],
                    &atoms_by_chain[secondary_index],
                    union_find,
                );
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn push_dense_chain_matrix_pair<A: NameAtomStore + ?Sized>(
    rows: &mut Vec<SummaryRow>,
    atoms: &A,
    chains: &[String],
    totals: &HashMap<String, NameTotals>,
    threshold: f64,
    primary_index: usize,
    secondary_index: usize,
    primary_atoms: &[u32],
    secondary_atoms: &[u32],
    union_find: UnionFind,
) {
    let spec = ChainMatrixRowSpec {
        chains,
        totals,
        primary_index,
        secondary_index,
        threshold,
    };
    let (primary_summary, secondary_summary) = summarize_dense_components_for_chain_pair(
        atoms,
        union_find,
        primary_atoms,
        secondary_atoms,
    );
    push_chain_matrix_summary_row(rows, &spec, primary_index, secondary_index, primary_summary);
    push_chain_matrix_summary_row(
        rows,
        &spec,
        secondary_index,
        primary_index,
        secondary_summary,
    );
}

fn push_chain_matrix_pair<A: NameAtomStore + ?Sized>(
    rows: &mut Vec<SummaryRow>,
    atoms: &A,
    chains: &[String],
    totals: &HashMap<String, NameTotals>,
    threshold: f64,
    pair_index: usize,
    union_find: SparseUnionFind,
) {
    let (primary_index, secondary_index) = chain_pair_from_index(pair_index, chains.len());
    push_chain_matrix_rows(
        rows,
        atoms,
        ChainMatrixRowSpec {
            chains,
            totals,
            primary_index,
            secondary_index,
            threshold,
        },
        union_find,
    );
}

pub(crate) fn push_chain_matrix_rows<A: NameAtomStore + ?Sized>(
    rows: &mut Vec<SummaryRow>,
    atoms: &A,
    spec: ChainMatrixRowSpec<'_>,
    union_find: SparseUnionFind,
) {
    let (primary_summary, secondary_summary) = summarize_sparse_components_for_chain_pair(
        atoms,
        union_find,
        spec.primary_index,
        spec.secondary_index,
    );
    push_chain_matrix_summary_row(
        rows,
        &spec,
        spec.primary_index,
        spec.secondary_index,
        primary_summary,
    );
    push_chain_matrix_summary_row(
        rows,
        &spec,
        spec.secondary_index,
        spec.primary_index,
        secondary_summary,
    );
}

pub(crate) fn push_chain_matrix_summary_row(
    rows: &mut Vec<SummaryRow>,
    spec: &ChainMatrixRowSpec<'_>,
    primary_index: usize,
    secondary_index: usize,
    summary: GroupSummary,
) {
    let primary = &spec.chains[primary_index];
    let total = spec.totals.get(primary).copied().unwrap_or(NameTotals {
        contracts: 0,
        nfts: 0,
    });
    rows.push(summary_row(
        SummarySpec {
            field_name: "name",
            scope: "chain_matrix",
            primary_chain: primary,
            secondary_chain: &spec.chains[secondary_index],
            threshold: Some(spec.threshold),
            match_mode: "jaro_winkler",
            metric: "duplicate_group",
            total_contracts: total.contracts,
            total_nfts: total.nfts,
        },
        summary,
    ));
}
