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
            let left_name = atoms[left].name_norm.as_str();
            let right_name = atoms[right].name_norm.as_str();
            if !name_pair_can_reach_threshold(left_name, right_name, threshold) {
                return None;
            }
            let score = name_pair_score_from_names(left_name, right_name);
            (score >= threshold).then_some(ScoredRight { right, score })
        })
        .collect()
}

fn name_pair_score_from_names(left_name: &str, right_name: &str) -> f64 {
    if left_name == right_name {
        100.0
    } else {
        jaro_winkler(left_name, right_name) * 100.0
    }
}

fn name_pair_can_reach_threshold(left_name: &str, right_name: &str, threshold: f64) -> bool {
    left_name == right_name || jaro_winkler_upper_bound(left_name, right_name) >= threshold
}

fn jaro_winkler_upper_bound(left_name: &str, right_name: &str) -> f64 {
    let left_len = left_name.chars().count();
    let right_len = right_name.chars().count();
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
