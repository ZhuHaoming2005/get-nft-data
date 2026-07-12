use super::*;

pub(crate) fn atoms_by_chain(atoms: &[NameAtom], chain_count: usize) -> Vec<Vec<usize>> {
    let mut indexes = vec![Vec::new(); chain_count];
    for (index, atom) in atoms.iter().enumerate() {
        indexes[atom.chain_index].push(index);
    }
    indexes
}

#[cfg(test)]
pub(crate) fn chain_pair_candidate_chunk_count(
    atoms: &[NameAtom],
    left_atoms: &[usize],
    right_atoms: &[usize],
    threshold: f64,
) -> u64 {
    if left_atoms.is_empty() || right_atoms.is_empty() {
        return 0;
    }
    left_atoms
        .iter()
        .map(|&left| {
            right_atom_range_for_left(atoms, right_atoms, left, threshold)
                .len()
                .div_ceil(RIGHT_SCORE_CHUNK_SIZE) as u64
        })
        .sum()
}

#[cfg(test)]
pub(crate) fn right_atom_range_for_left(
    atoms: &[NameAtom],
    right_atoms: &[usize],
    left: usize,
    threshold: f64,
) -> std::ops::Range<usize> {
    if right_atoms.is_empty() {
        return 0..0;
    }
    let left_len = atoms[left].char_len;
    let max_right_len = atoms[*right_atoms.last().expect("right_atoms is not empty")].char_len;
    let Some((min_len, max_len)) =
        right_length_window_for_threshold(left_len, max_right_len, threshold)
    else {
        return 0..0;
    };
    let start = lower_bound_right_atom_len(atoms, right_atoms, min_len);
    let end = upper_bound_right_atom_len(atoms, right_atoms, max_len);
    start..end
}

#[cfg(test)]
pub(crate) fn right_length_window_for_threshold(
    left_len: usize,
    max_right_len: usize,
    threshold: f64,
) -> Option<(usize, usize)> {
    let pivot = left_len.min(max_right_len);
    if !name_pair_lengths_can_reach_threshold(left_len, pivot, threshold) {
        return None;
    }

    let mut low = 0usize;
    let mut high = pivot;
    while low < high {
        let middle = low + (high - low) / 2;
        if name_pair_lengths_can_reach_threshold(left_len, middle, threshold) {
            high = middle;
        } else {
            low = middle + 1;
        }
    }
    let min_len = low;

    let max_len = if max_right_len <= left_len {
        max_right_len
    } else {
        let mut low = left_len;
        let mut high = max_right_len.saturating_add(1);
        while low < high {
            let middle = low + (high - low) / 2;
            if name_pair_lengths_can_reach_threshold(left_len, middle, threshold) {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        low.saturating_sub(1)
    };

    Some((min_len, max_len))
}

#[cfg(test)]
pub(crate) fn lower_bound_right_atom_len(
    atoms: &[NameAtom],
    right_atoms: &[usize],
    min_len: usize,
) -> usize {
    let mut low = 0usize;
    let mut high = right_atoms.len();
    while low < high {
        let middle = low + (high - low) / 2;
        if atoms[right_atoms[middle]].char_len < min_len {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    low
}

#[cfg(test)]
pub(crate) fn upper_bound_right_atom_len(
    atoms: &[NameAtom],
    right_atoms: &[usize],
    max_len: usize,
) -> usize {
    let mut low = 0usize;
    let mut high = right_atoms.len();
    while low < high {
        let middle = low + (high - low) / 2;
        if atoms[right_atoms[middle]].char_len <= max_len {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    low
}

#[cfg(test)]
pub(crate) fn flush_chunk_progress(progress: &ProgressTracker, pending: &mut u64) {
    if *pending >= PROGRESS_FLUSH_CHUNKS {
        flush_remaining_progress(progress, pending);
    }
}

#[cfg(test)]
pub(crate) fn flush_remaining_progress(progress: &ProgressTracker, pending: &mut u64) {
    if *pending > 0 {
        progress.inc(*pending);
        *pending = 0;
    }
}

pub(crate) fn push_reused_chain_matrix_rows(
    rows: &mut Vec<SummaryRow>,
    atoms: &[NameAtom],
    chains: &[String],
    totals: &HashMap<String, NameTotals>,
    state: &mut ThresholdUnionState,
) {
    let Some(matrix) = &mut state.chain_matrix else {
        return;
    };
    for (pair_index, union_find) in matrix.iter_mut().enumerate() {
        let (primary_index, secondary_index) = chain_pair_from_index(pair_index, chains.len());
        push_chain_matrix_rows(
            rows,
            atoms,
            ChainMatrixRowSpec {
                chains,
                totals,
                primary_index,
                secondary_index,
                threshold: state.threshold,
            },
            union_find,
        );
    }
}

pub(crate) fn push_chain_matrix_rows(
    rows: &mut Vec<SummaryRow>,
    atoms: &[NameAtom],
    spec: ChainMatrixRowSpec<'_>,
    union_find: &mut SparseUnionFind,
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
