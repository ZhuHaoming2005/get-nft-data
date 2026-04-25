fn union_full_name_pairs(
    atoms: &[NameAtom],
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
    for left in 0..atoms.len() - 1 {
        for chunk_start in (left + 1..atoms.len()).step_by(RIGHT_SCORE_CHUNK_SIZE) {
            let chunk_end = (chunk_start + RIGHT_SCORE_CHUNK_SIZE).min(atoms.len());
            let matching_rights =
                score_name_pairs_for_left_chunk(atoms, left, chunk_start, chunk_end, min_threshold);
            apply_matching_name_pairs(atoms, states, left, &matching_rights, chain_count);
            pending_progress += 1;
            flush_chunk_progress(progress, &mut pending_progress);
        }
    }
    flush_remaining_progress(progress, &mut pending_progress);
}

fn score_name_pairs_for_left_chunk(
    atoms: &[NameAtom],
    left: usize,
    chunk_start: usize,
    chunk_end: usize,
    threshold: f64,
) -> Vec<ScoredRight> {
    (chunk_start..chunk_end)
        .into_par_iter()
        .filter_map(|right| {
            let score = name_pair_score(atoms, left, right);
            (score >= threshold).then_some(ScoredRight { right, score })
        })
        .collect()
}

fn name_pair_score(atoms: &[NameAtom], left: usize, right: usize) -> f64 {
    let left_name = atoms[left].name_norm.as_str();
    let right_name = atoms[right].name_norm.as_str();
    if left_name == right_name {
        100.0
    } else {
        jaro_winkler(left_name, right_name) * 100.0
    }
}

fn apply_matching_name_pairs(
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

