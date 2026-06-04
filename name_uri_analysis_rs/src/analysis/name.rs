fn run_name_analysis(
    conn: &Connection,
    chains: &[String],
    thresholds: &[f64],
    threads: usize,
    memory_limit: &str,
    analysis_memory_limit: Option<&str>,
    progress: &ProgressTracker,
) -> Result<Vec<SummaryRow>, AnalysisError> {
    progress.start_phase("analyzing name duplicates", 3);
    let totals = load_name_totals(conn, chains)?;
    progress.step("loaded name totals");
    let atoms = load_all_name_atoms(conn, chains)?;
    progress.step(format!("loaded {} name atoms", atoms.len()));
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads.max(1))
        .build()
        .map_err(|err| AnalysisError::InvalidData(err.to_string()))?;

    let mut rows = Vec::new();
    let atom_bytes = name_atoms_memory_bytes(&atoms);
    let atoms_by_chain = atoms_by_chain(&atoms, chains.len());
    let chain_matrix_state_bytes = chain_matrix_reuse_state_bytes(&atoms_by_chain);
    let total_memory_budget = total_memory_budget_bytes(memory_limit)?;
    let memory_plan = name_analysis_memory_plan(
        thresholds,
        atoms.len(),
        chains.len(),
        memory_limit,
        analysis_memory_limit,
        atom_bytes,
        chain_matrix_state_bytes,
    )?;
    progress.step(format!(
        "Rust analysis memory budget {}",
        format_byte_size(memory_plan.analysis_bytes)
    ));
    let thresholds = unique_thresholds(thresholds);
    let analysis_work_budget = memory_plan.analysis_bytes.saturating_sub(atom_bytes);
    let per_threshold_bytes = threshold_state_bytes(atoms.len(), chains.len());
    let chain_matrix_reuse =
        chain_matrix_reuse_plan(&atoms_by_chain, analysis_work_budget, per_threshold_bytes);
    let scoring_per_threshold_bytes = per_threshold_bytes.saturating_add(
        chain_matrix_reuse
            .as_ref()
            .map_or(0, |plan| plan.per_threshold_bytes),
    );
    let threshold_budget_capacity = threshold_batch_capacity_for_state_bytes(
        thresholds.len(),
        scoring_per_threshold_bytes.max(1),
        analysis_work_budget,
    );
    let mut memory_guard = MemoryGuard::new(total_memory_budget);
    let mut threshold_start = 0;
    while threshold_start < thresholds.len() {
        let batch_size = memory_guard.next_threshold_batch_size(
            thresholds.len() - threshold_start,
            threshold_budget_capacity,
            scoring_per_threshold_bytes,
        );
        let threshold_batch = thresholds[threshold_start..threshold_start + batch_size].to_vec();
        threshold_start += batch_size;
        progress.set_message(format!(
            "name threshold batch {} threshold(s), RSS {}, chain_matrix {}",
            threshold_batch.len(),
            memory_guard
                .current_rss_bytes()
                .map(format_byte_size)
                .unwrap_or_else(|| "unknown".to_string()),
            if chain_matrix_reuse.is_some() {
                "reuse"
            } else {
                "fallback"
            }
        ));
        let min_threshold = threshold_batch
            .iter()
            .copied()
            .fold(f64::INFINITY, f64::min);
        progress.add_work(candidate_name_chunk_count(&atoms, min_threshold));
        let mut states = threshold_batch
            .iter()
            .copied()
            .map(|threshold| ThresholdUnionState {
                threshold,
                intra: UnionFind::new(atoms.len()),
                cross: (chains.len() > 1).then(SparseUnionFind::default),
                chain_matrix: chain_matrix_reuse
                    .as_ref()
                    .map(|plan| new_chain_matrix_reuse_states(plan.pair_count)),
            })
            .collect::<Vec<_>>();
        sort_threshold_states_for_apply(&mut states);
        pool.install(|| union_full_name_pairs(&atoms, &mut states, chains.len(), progress));

        let chain_matrix_summary_work = if chain_matrix_reuse.is_some() {
            states.len() as u64 * chain_pair_count(chains.len()) as u64 * 2
        } else {
            0
        };
        progress.add_work(states.len() as u64 * chains.len() as u64 + chain_matrix_summary_work);
        for state in &mut states {
            push_name_summary_rows(&mut rows, &atoms, &atoms_by_chain, chains, &totals, state);
            progress.inc(chains.len() as u64);
            if chain_matrix_reuse.is_some() {
                push_reused_chain_matrix_rows(&mut rows, &atoms, chains, &totals, state);
                progress.inc(chain_pair_count(chains.len()) as u64 * 2);
            }
        }
    }
    if chains.len() > 1 && chain_matrix_reuse.is_none() {
        rows.extend(run_chain_matrix_analysis(
            &atoms,
            &atoms_by_chain,
            chains,
            ChainMatrixAnalysisSpec {
                thresholds: &thresholds,
                analysis_budget: analysis_work_budget,
                total_memory_budget,
                totals: &totals,
            },
            &pool,
            progress,
        )?);
    }
    progress.finish_phase("name analysis complete");
    Ok(rows)
}

fn load_name_totals(
    conn: &Connection,
    chains: &[String],
) -> Result<HashMap<String, NameTotals>, AnalysisError> {
    let mut totals = HashMap::new();
    let mut stmt = conn.prepare(
        "
        SELECT count(*)::BIGINT, coalesce(sum(nft_count), 0)::BIGINT
        FROM contract_names
        WHERE chain = ?
        ",
    )?;
    for chain in chains {
        let total = stmt.query_row(params![chain], |row| {
            Ok(NameTotals {
                contracts: row.get(0)?,
                nfts: row.get(1)?,
            })
        })?;
        totals.insert(chain.clone(), total);
    }
    Ok(totals)
}

fn load_all_name_atoms(
    conn: &Connection,
    chains: &[String],
) -> Result<Vec<NameAtom>, AnalysisError> {
    let chain_indexes = chains
        .iter()
        .enumerate()
        .map(|(index, chain)| (chain.as_str(), index))
        .collect::<HashMap<_, _>>();
    let mut stmt = conn.prepare(
        "
        SELECT chain, name_norm, contract_count, nft_count
        FROM name_atoms
        ORDER BY chain, name_norm
        ",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, i64>(3)?,
        ))
    })?;
    let mut atoms = Vec::new();
    for row in rows {
        let (chain, name_norm, contract_count, nft_count) = row?;
        if let Some(chain_index) = chain_indexes.get(chain.as_str()).copied() {
            let char_len = name_norm.chars().count();
            atoms.push(NameAtom {
                chain_index,
                name_norm,
                char_len,
                contract_count,
                nft_count,
            });
        }
    }
    atoms.sort_by(|left, right| {
        left.char_len
            .cmp(&right.char_len)
            .then_with(|| left.chain_index.cmp(&right.chain_index))
            .then_with(|| left.name_norm.cmp(&right.name_norm))
    });
    Ok(atoms)
}

fn push_name_summary_rows(
    rows: &mut Vec<SummaryRow>,
    atoms: &[NameAtom],
    atoms_by_chain: &[Vec<usize>],
    chains: &[String],
    totals: &HashMap<String, NameTotals>,
    state: &mut ThresholdUnionState,
) {
    let mut dense_scratch = DenseComponentScratch::new(atoms.len());
    for (chain_index, primary) in chains.iter().enumerate() {
        let total = totals.get(primary).copied().unwrap_or(NameTotals {
            contracts: 0,
            nfts: 0,
        });
        let intra = summarize_components_for_primary_with_scratch(
            atoms,
            &atoms_by_chain[chain_index],
            &mut state.intra,
            &mut dense_scratch,
        );
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
            intra,
        ));

        if let Some(cross) = &mut state.cross {
            let cross_summary = summarize_sparse_components_for_primary(atoms, cross, chain_index);
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
                cross_summary,
            ));
        }
    }
}

fn unique_thresholds(thresholds: &[f64]) -> Vec<f64> {
    let mut unique = thresholds.to_vec();
    unique.sort_by(|left, right| right.partial_cmp(left).unwrap_or(std::cmp::Ordering::Equal));
    unique.dedup_by(|left, right| left.to_bits() == right.to_bits());
    unique
}

fn sort_threshold_states_for_apply(states: &mut [ThresholdUnionState]) {
    states.sort_by(|left, right| {
        left.threshold
            .partial_cmp(&right.threshold)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
}

fn sort_matrix_states_for_apply(states: &mut [MatrixUnionState]) {
    states.sort_by(|left, right| {
        left.threshold
            .partial_cmp(&right.threshold)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
}

fn chain_matrix_reuse_plan(
    atoms_by_chain: &[Vec<usize>],
    analysis_work_budget: usize,
    global_per_threshold_bytes: usize,
) -> Option<ChainMatrixReusePlan> {
    let pair_count = chain_pair_count(atoms_by_chain.len());
    if pair_count == 0 {
        return None;
    }
    let per_threshold_bytes = chain_matrix_reuse_state_bytes(atoms_by_chain);
    if per_threshold_bytes == 0 {
        return None;
    }
    let combined_threshold_bytes = global_per_threshold_bytes.saturating_add(per_threshold_bytes);
    (combined_threshold_bytes <= analysis_work_budget).then_some(ChainMatrixReusePlan {
        per_threshold_bytes,
        pair_count,
    })
}

fn chain_matrix_reuse_state_bytes(atoms_by_chain: &[Vec<usize>]) -> usize {
    let mut bytes = 0usize;
    for left in 0..atoms_by_chain.len() {
        for right in left + 1..atoms_by_chain.len() {
            bytes = bytes.saturating_add(sparse_union_find_bytes(
                atoms_by_chain[left].len() + atoms_by_chain[right].len(),
            ));
        }
    }
    bytes
}

fn new_chain_matrix_reuse_states(pair_count: usize) -> Vec<SparseUnionFind> {
    std::iter::repeat_with(SparseUnionFind::default)
        .take(pair_count)
        .collect()
}

fn chain_pair_count(chain_count: usize) -> usize {
    chain_count.saturating_mul(chain_count.saturating_sub(1)) / 2
}

fn chain_pair_index(left: usize, right: usize, chain_count: usize) -> usize {
    debug_assert!(left < right);
    left * (2 * chain_count - left - 1) / 2 + (right - left - 1)
}

fn chain_pair_from_index(mut index: usize, chain_count: usize) -> (usize, usize) {
    for left in 0..chain_count {
        let row_width = chain_count - left - 1;
        if index < row_width {
            return (left, left + index + 1);
        }
        index -= row_width;
    }
    unreachable!("chain pair index out of range")
}

#[cfg(test)]
fn threshold_batches(
    thresholds: &[f64],
    atom_count: usize,
    chain_count: usize,
    analysis_budget: usize,
) -> Vec<Vec<f64>> {
    let thresholds = unique_thresholds(thresholds);
    let batch_size =
        threshold_batch_capacity(thresholds.len(), atom_count, chain_count, analysis_budget);
    thresholds.chunks(batch_size).map(<[f64]>::to_vec).collect()
}

#[cfg(test)]
fn threshold_batch_capacity(
    threshold_count: usize,
    atom_count: usize,
    chain_count: usize,
    analysis_budget: usize,
) -> usize {
    let state_bytes = threshold_state_bytes(atom_count, chain_count).max(1);
    threshold_batch_capacity_for_state_bytes(threshold_count, state_bytes, analysis_budget)
}

fn matrix_threshold_batch_capacity(
    threshold_count: usize,
    atom_count: usize,
    analysis_budget: usize,
) -> usize {
    let state_bytes = sparse_union_find_bytes(atom_count).max(1);
    threshold_batch_capacity_for_state_bytes(threshold_count, state_bytes, analysis_budget)
}

fn threshold_batch_capacity_for_state_bytes(
    threshold_count: usize,
    state_bytes: usize,
    analysis_budget: usize,
) -> usize {
    (analysis_budget / state_bytes)
        .max(1)
        .min(threshold_count.max(1))
}

fn adaptive_threshold_batch_size(
    remaining_thresholds: usize,
    budget_capacity: usize,
    per_threshold_bytes: usize,
    total_budget: usize,
    current_rss: usize,
) -> usize {
    let capacity = remaining_thresholds.max(1).min(budget_capacity.max(1));
    if current_rss == 0 || total_budget == 0 {
        return capacity;
    }

    let headroom_capacity = if per_threshold_bytes == 0 {
        capacity
    } else {
        total_budget
            .saturating_sub(current_rss)
            .saturating_div(per_threshold_bytes)
            .max(1)
    };
    capacity.min(headroom_capacity)
}

#[cfg(test)]
fn full_name_chunk_count(atom_count: usize) -> u64 {
    if atom_count < 2 {
        return 0;
    }
    triangular_chunk_count(atom_count - 1)
}

fn candidate_name_chunk_count(atoms: &[NameAtom], threshold: f64) -> u64 {
    if atoms.len() < 2 {
        return 0;
    }
    (0..atoms.len() - 1)
        .map(|left| {
            right_name_range_end_for_left(atoms, left, threshold)
                .saturating_sub(left + 1)
                .div_ceil(RIGHT_SCORE_CHUNK_SIZE) as u64
        })
        .sum()
}

#[cfg(test)]
fn triangular_chunk_count(max_right_count: usize) -> u64 {
    let chunk = RIGHT_SCORE_CHUNK_SIZE as u128;
    let count = max_right_count as u128;
    let full_groups = count / chunk;
    let remainder = count % chunk;
    let total = chunk
        .saturating_mul(full_groups)
        .saturating_mul(full_groups + 1)
        .saturating_div(2)
        .saturating_add(remainder.saturating_mul(full_groups + 1));
    total.min(u64::MAX as u128) as u64
}

#[cfg(test)]
fn chain_pair_chunk_count(left_count: usize, right_count: usize) -> u64 {
    if left_count == 0 || right_count == 0 {
        return 0;
    }
    let chunks_per_left = right_count.div_ceil(RIGHT_SCORE_CHUNK_SIZE);
    (left_count as u64).saturating_mul(chunks_per_left as u64)
}

fn threshold_state_bytes(atom_count: usize, chain_count: usize) -> usize {
    let dense = dense_union_find_bytes(atom_count);
    if chain_count > 1 {
        dense.saturating_add(sparse_union_find_bytes(atom_count))
    } else {
        dense
    }
}

fn dense_union_find_bytes(atom_count: usize) -> usize {
    atom_count.saturating_mul(std::mem::size_of::<usize>() + std::mem::size_of::<u8>())
}

fn sparse_union_find_bytes(atom_count: usize) -> usize {
    atom_count.saturating_mul(SPARSE_UNION_NODE_BYTES)
}

fn name_atoms_memory_bytes(atoms: &[NameAtom]) -> usize {
    let struct_bytes = atoms.len().saturating_mul(std::mem::size_of::<NameAtom>());
    let string_bytes = atoms
        .iter()
        .map(|atom| atom.name_norm.capacity().max(atom.name_norm.len()))
        .sum::<usize>();
    struct_bytes.saturating_add(string_bytes)
}
