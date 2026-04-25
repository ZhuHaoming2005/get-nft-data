fn run_chain_matrix_analysis(
    atoms: &[NameAtom],
    atoms_by_chain: &[Vec<usize>],
    chains: &[String],
    spec: ChainMatrixAnalysisSpec<'_>,
    pool: &rayon::ThreadPool,
    progress: &ProgressTracker,
) -> Result<Vec<SummaryRow>, AnalysisError> {
    let mut memory_guard = MemoryGuard::new(spec.total_memory_budget);
    let mut rows = Vec::new();

    for left_chain in 0..chains.len() {
        for right_chain in left_chain + 1..chains.len() {
            let pair_atom_count =
                atoms_by_chain[left_chain].len() + atoms_by_chain[right_chain].len();
            let per_threshold_bytes = sparse_union_find_bytes(pair_atom_count);
            let pair_capacity = matrix_threshold_batch_capacity(
                spec.thresholds.len(),
                pair_atom_count,
                spec.analysis_budget,
            );
            let mut threshold_start = 0;
            while threshold_start < spec.thresholds.len() {
                let batch_size = memory_guard.next_threshold_batch_size(
                    spec.thresholds.len() - threshold_start,
                    pair_capacity,
                    per_threshold_bytes,
                );
                let threshold_batch =
                    spec.thresholds[threshold_start..threshold_start + batch_size].to_vec();
                threshold_start += batch_size;
                progress.set_message(format!(
                    "chain matrix {}-{} batch {} threshold(s), RSS {}",
                    chains[left_chain],
                    chains[right_chain],
                    threshold_batch.len(),
                    memory_guard
                        .current_rss_bytes()
                        .map(format_byte_size)
                        .unwrap_or_else(|| "unknown".to_string())
                ));
                progress.add_work(chain_pair_chunk_count(
                    atoms_by_chain[left_chain].len(),
                    atoms_by_chain[right_chain].len(),
                ));
                let mut states = threshold_batch
                    .iter()
                    .copied()
                    .map(|threshold| MatrixUnionState {
                        threshold,
                        union_find: SparseUnionFind::default(),
                    })
                    .collect::<Vec<_>>();
                sort_matrix_states_for_apply(&mut states);
                pool.install(|| {
                    union_chain_pair_name_pairs(
                        atoms,
                        &atoms_by_chain[left_chain],
                        &atoms_by_chain[right_chain],
                        &mut states,
                        progress,
                    )
                });
                progress.add_work(states.len() as u64 * 2);
                for state in &mut states {
                    push_chain_matrix_rows(
                        &mut rows,
                        atoms,
                        ChainMatrixRowSpec {
                            chains,
                            totals: spec.totals,
                            primary_index: left_chain,
                            secondary_index: right_chain,
                            threshold: state.threshold,
                        },
                        &mut state.union_find,
                    );
                    progress.inc(2);
                }
            }
        }
    }

    Ok(rows)
}

fn atoms_by_chain(atoms: &[NameAtom], chain_count: usize) -> Vec<Vec<usize>> {
    let mut indexes = vec![Vec::new(); chain_count];
    for (index, atom) in atoms.iter().enumerate() {
        indexes[atom.chain_index].push(index);
    }
    indexes
}

fn union_chain_pair_name_pairs(
    atoms: &[NameAtom],
    left_atoms: &[usize],
    right_atoms: &[usize],
    states: &mut [MatrixUnionState],
    progress: &ProgressTracker,
) {
    if left_atoms.is_empty() || right_atoms.is_empty() || states.is_empty() {
        return;
    }
    let min_threshold = states
        .iter()
        .map(|state| state.threshold)
        .fold(f64::INFINITY, f64::min);

    let mut pending_progress = 0;
    for &left in left_atoms {
        for right_chunk in right_atoms.chunks(RIGHT_SCORE_CHUNK_SIZE) {
            let matching_rights: Vec<ScoredRight> = right_chunk
                .par_iter()
                .copied()
                .filter_map(|right| {
                    let left_name = atoms[left].name_norm.as_str();
                    let right_name = atoms[right].name_norm.as_str();
                    if !name_pair_can_reach_threshold(left_name, right_name, min_threshold) {
                        return None;
                    }
                    let score = name_pair_score_from_names(left_name, right_name);
                    (score >= min_threshold).then_some(ScoredRight { right, score })
                })
                .collect();
            for hit in matching_rights {
                for state in states.iter_mut() {
                    if hit.score < state.threshold {
                        break;
                    }
                    state.union_find.union(left, hit.right);
                }
            }
            pending_progress += 1;
            flush_chunk_progress(progress, &mut pending_progress);
        }
    }
    flush_remaining_progress(progress, &mut pending_progress);
}

fn flush_chunk_progress(progress: &ProgressTracker, pending: &mut u64) {
    if *pending >= PROGRESS_FLUSH_CHUNKS {
        flush_remaining_progress(progress, pending);
    }
}

fn flush_remaining_progress(progress: &ProgressTracker, pending: &mut u64) {
    if *pending > 0 {
        progress.inc(*pending);
        *pending = 0;
    }
}

fn push_reused_chain_matrix_rows(
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

fn push_chain_matrix_rows(
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

fn push_chain_matrix_summary_row(
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
