use super::*;

pub(crate) struct NameAnalysisSpec<'a> {
    pub(crate) chains: &'a [String],
    pub(crate) totals: &'a HashMap<String, NameTotals>,
    pub(crate) threshold: f64,
    pub(crate) threads: usize,
    pub(crate) memory_limit: &'a str,
    pub(crate) analysis_memory_limit: Option<&'a str>,
}

pub(crate) struct CanonicalNameValues {
    pub(crate) atoms: Vec<NameAtom>,
    pub(crate) members: Vec<Vec<usize>>,
}

#[derive(Debug, Serialize)]
pub(crate) struct NameAlgorithmMetrics {
    input_atoms: u64,
    canonical_names: u64,
    candidate_pairs: u64,
    scored_pairs: u64,
    matched_pairs: u64,
    candidate_index_bytes: u64,
    scratch_and_queue_bytes: u64,
    dsu_and_chain_state_bytes: u64,
}

pub(crate) struct NameAnalysisResult {
    pub(crate) rows: Vec<SummaryRow>,
    pub(crate) metrics: NameAlgorithmMetrics,
}

pub(crate) fn canonical_name_values(atoms: &[NameAtom]) -> CanonicalNameValues {
    let mut index_by_name = HashMap::<&str, usize>::new();
    let mut canonical_atoms = Vec::<NameAtom>::new();
    let mut members = Vec::<Vec<usize>>::new();
    for (atom_index, atom) in atoms.iter().enumerate() {
        if let Some(&canonical_index) = index_by_name.get(atom.name_norm.as_str()) {
            let canonical = &mut canonical_atoms[canonical_index];
            canonical.contract_count = canonical.contract_count.saturating_add(atom.contract_count);
            canonical.nft_count = canonical.nft_count.saturating_add(atom.nft_count);
            members[canonical_index].push(atom_index);
            continue;
        }
        let canonical_index = canonical_atoms.len();
        index_by_name.insert(atom.name_norm.as_str(), canonical_index);
        canonical_atoms.push(NameAtom {
            chain_index: atom.chain_index,
            name_norm: atom.name_norm.clone(),
            char_len: atom.char_len,
            contract_count: atom.contract_count,
            nft_count: atom.nft_count,
        });
        members.push(vec![atom_index]);
    }
    CanonicalNameValues {
        atoms: canonical_atoms,
        members,
    }
}

pub(crate) fn run_name_analysis(
    conn: &Connection,
    spec: NameAnalysisSpec<'_>,
    progress: &ProgressTracker,
) -> Result<NameAnalysisResult, AnalysisError> {
    let chains = spec.chains;
    let totals = spec.totals;
    let threshold = spec.threshold;
    progress.start_phase("analyzing name duplicates", 3);
    progress.step("loaded name totals");
    let atoms = load_all_name_atoms(conn, chains)?;
    if atoms.len() > u32::MAX as usize {
        return Err(AnalysisError::InvalidData(
            "name atom count exceeds compact u32 indexes".to_string(),
        ));
    }
    progress.step(format!("loaded {} name atoms", atoms.len()));
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(spec.threads.max(1))
        .build()
        .map_err(|err| AnalysisError::InvalidData(err.to_string()))?;
    let canonical = canonical_name_values(&atoms);
    let canonical_name_count = canonical.atoms.len();
    progress.set_message(format!(
        "collapsed {} chain/name atoms to {} canonical names",
        atoms.len(),
        canonical.atoms.len()
    ));
    let canonical_members_bytes = canonical
        .members
        .capacity()
        .saturating_mul(std::mem::size_of::<Vec<usize>>())
        .saturating_add(
            canonical
                .members
                .iter()
                .map(|members| {
                    members
                        .capacity()
                        .saturating_mul(std::mem::size_of::<usize>())
                })
                .sum::<usize>(),
        );
    let atoms_by_chain = atoms_by_chain(&atoms, chains.len());
    let atoms_by_chain_bytes = atoms_by_chain
        .capacity()
        .saturating_mul(std::mem::size_of::<Vec<usize>>())
        .saturating_add(
            atoms_by_chain
                .iter()
                .map(|indexes| {
                    indexes
                        .capacity()
                        .saturating_mul(std::mem::size_of::<usize>())
                })
                .fold(0usize, usize::saturating_add),
        );
    let base_atom_bytes = name_atoms_memory_bytes(&atoms)
        .saturating_add(name_atoms_memory_bytes(&canonical.atoms))
        .saturating_add(canonical_members_bytes)
        .saturating_add(atoms_by_chain_bytes);
    let index_estimate = estimate_name_candidate_index_bytes(&canonical.atoms);
    let chain_matrix_state_bytes = chain_matrix_reuse_state_bytes(&atoms_by_chain);
    let state_bytes =
        threshold_state_bytes(atoms.len(), chains.len()).saturating_add(chain_matrix_state_bytes);
    let initial_memory_plan = name_analysis_memory_plan(
        spec.memory_limit,
        spec.analysis_memory_limit,
        base_atom_bytes.saturating_add(index_estimate.peak_build_bytes),
    )?;
    let scoring_resident_bytes = base_atom_bytes
        .saturating_add(index_estimate.resident_bytes)
        .saturating_add(state_bytes);
    let scratch_plan = name_scratch_plan(
        canonical.atoms.len(),
        spec.threads,
        initial_memory_plan
            .analysis_bytes
            .saturating_sub(scoring_resident_bytes),
    );
    let scoring_peak_bytes = scoring_resident_bytes.saturating_add(scratch_plan.reserved_bytes);
    let memory_plan = name_analysis_memory_plan(
        spec.memory_limit,
        spec.analysis_memory_limit,
        scoring_peak_bytes,
    )?;
    progress.step(format!(
        "Rust analysis memory budget {}",
        format_byte_size(memory_plan.analysis_bytes)
    ));
    let candidate_index = pool.install(|| NameCandidateIndex::new(&canonical.atoms));
    let actual_index_bytes = candidate_index.memory_bytes();
    if actual_index_bytes > index_estimate.resident_bytes {
        return Err(AnalysisError::InvalidData(format!(
            "name candidate index used {}, exceeding conservative preflight estimate {}",
            format_byte_size(actual_index_bytes),
            format_byte_size(index_estimate.resident_bytes)
        )));
    }
    let mut rows = Vec::new();
    progress.set_message("name single-threshold global scoring with chain-matrix reuse");
    progress.add_work(canonical.atoms.len().saturating_sub(1) as u64);
    let mut state = ThresholdUnionState {
        threshold,
        intra: UnionFind::new(atoms.len()),
        cross: (chains.len() > 1).then(SparseUnionFind::default),
        chain_matrix: (chains.len() > 1)
            .then(|| new_chain_matrix_reuse_states(chain_pair_count(chains.len()))),
    };
    let scoring = pool.install(|| {
        union_canonical_name_pairs(
            &atoms,
            &canonical,
            &candidate_index,
            scratch_plan.mode,
            &mut state,
            chains.len(),
            progress,
        )
    });
    drop(candidate_index);
    drop(canonical);
    drop(pool);
    progress.add_work(chains.len() as u64 + chain_pair_count(chains.len()) as u64 * 2);
    push_name_summary_rows(
        &mut rows,
        &atoms,
        &atoms_by_chain,
        chains,
        totals,
        &mut state,
    );
    progress.inc(chains.len() as u64);
    drop(atoms_by_chain);
    state.intra = UnionFind::new(0);
    state.cross = None;
    if chains.len() > 1 {
        push_reused_chain_matrix_rows(&mut rows, &atoms, chains, totals, &mut state);
        progress.inc(chain_pair_count(chains.len()) as u64 * 2);
    }
    progress.finish_phase("name analysis complete");
    Ok(NameAnalysisResult {
        rows,
        metrics: NameAlgorithmMetrics {
            input_atoms: atoms.len() as u64,
            canonical_names: canonical_name_count as u64,
            candidate_pairs: scoring.candidate_pairs,
            scored_pairs: scoring.scored_pairs,
            matched_pairs: scoring.matched_pairs,
            candidate_index_bytes: actual_index_bytes as u64,
            scratch_and_queue_bytes: scratch_plan.reserved_bytes as u64,
            dsu_and_chain_state_bytes: state_bytes as u64,
        },
    })
}

pub(crate) fn load_all_name_atoms(
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

pub(crate) fn push_name_summary_rows(
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

pub(crate) fn chain_matrix_reuse_state_bytes(atoms_by_chain: &[Vec<usize>]) -> usize {
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

#[cfg(test)]
pub(crate) fn full_name_chunk_count(atom_count: usize) -> u64 {
    if atom_count < 2 {
        return 0;
    }
    triangular_chunk_count(atom_count - 1)
}

#[cfg(test)]
pub(crate) fn candidate_name_chunk_count(atoms: &[NameAtom], threshold: f64) -> u64 {
    let candidate_index = NameCandidateIndex::new(atoms);
    let mut scratch = NameCandidateScratch::new(atoms.len());
    (0..atoms.len().saturating_sub(1))
        .map(|left| {
            let right_end = right_name_range_end_for_left(atoms, left, threshold);
            candidate_index
                .candidates_for_left(atoms, left, left + 1..right_end, threshold, &mut scratch)
                .iter()
                .count()
                .div_ceil(RIGHT_SCORE_CHUNK_SIZE) as u64
        })
        .sum()
}

#[cfg(test)]
pub(crate) fn triangular_chunk_count(max_right_count: usize) -> u64 {
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
pub(crate) fn chain_pair_chunk_count(left_count: usize, right_count: usize) -> u64 {
    if left_count == 0 || right_count == 0 {
        return 0;
    }
    let chunks_per_left = right_count.div_ceil(RIGHT_SCORE_CHUNK_SIZE);
    (left_count as u64).saturating_mul(chunks_per_left as u64)
}

pub(crate) fn threshold_state_bytes(atom_count: usize, chain_count: usize) -> usize {
    let dense = dense_union_find_bytes(atom_count);
    if chain_count > 1 {
        dense.saturating_add(sparse_union_find_bytes(atom_count))
    } else {
        dense
    }
}

pub(crate) fn dense_union_find_bytes(atom_count: usize) -> usize {
    atom_count.saturating_mul(std::mem::size_of::<usize>() + std::mem::size_of::<u8>())
}

pub(crate) fn sparse_union_find_bytes(atom_count: usize) -> usize {
    atom_count.saturating_mul(SPARSE_UNION_NODE_BYTES)
}

pub(crate) fn name_atoms_memory_bytes(atoms: &Vec<NameAtom>) -> usize {
    let struct_bytes = atoms
        .capacity()
        .saturating_mul(std::mem::size_of::<NameAtom>());
    let string_bytes = atoms
        .iter()
        .map(|atom| atom.name_norm.capacity().max(atom.name_norm.len()))
        .sum::<usize>();
    struct_bytes.saturating_add(string_bytes)
}
