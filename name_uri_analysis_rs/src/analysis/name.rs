use super::*;
use duckdb::arrow::array::{Array, Int64Array, StringArray, StringViewArray};

pub(crate) const NAME_ANALYSIS_WORKER_STACK_BYTES: usize = 4 * 1024 * 1024;

pub(crate) fn name_worker_stack_reserve_bytes(threads: usize) -> usize {
    threads
        .max(1)
        .saturating_mul(NAME_ANALYSIS_WORKER_STACK_BYTES)
}

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
    progress.start_stage("analyzing name duplicates", 6);
    progress.step_stage("loaded name totals");
    let (name_atom_total, estimated_name_load_bytes) = estimate_name_atom_load(conn)?;
    let worker_stack_bytes = name_worker_stack_reserve_bytes(spec.threads);
    let load_preflight_bytes = estimated_name_load_bytes
        .saturating_add(estimated_name_load_bytes / 4)
        .saturating_add(8 * 1024 * 1024)
        .saturating_add(worker_stack_bytes);
    name_analysis_memory_plan(
        spec.memory_limit,
        spec.analysis_memory_limit,
        load_preflight_bytes,
    )?;
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(spec.threads.max(1))
        .thread_name(|index| format!("name-{index}"))
        .stack_size(NAME_ANALYSIS_WORKER_STACK_BYTES)
        .build()
        .map_err(|err| AnalysisError::InvalidData(err.to_string()))?;
    progress.start_task("loading name atoms", Some(name_atom_total), "rows");
    let atoms = load_all_name_atoms(conn, chains, &pool, |delta| {
        progress.advance_task(delta, ProgressCounters::default());
    })?;
    if atoms.len() > u32::MAX as usize {
        return Err(AnalysisError::InvalidData(
            "name atom count exceeds compact u32 indexes".to_string(),
        ));
    }
    progress.finish_task(format!("loaded {} name atoms", atoms.len()));
    progress.step_stage(format!("loaded {} name atoms", atoms.len()));
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
        base_atom_bytes
            .saturating_add(index_estimate.peak_build_bytes)
            .saturating_add(worker_stack_bytes),
    )?;
    let scoring_resident_bytes = base_atom_bytes
        .saturating_add(index_estimate.resident_bytes)
        .saturating_add(state_bytes)
        .saturating_add(worker_stack_bytes);
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
    progress.step_stage(format!(
        "Rust analysis memory budget {}",
        format_byte_size(memory_plan.analysis_bytes)
    ));
    progress.start_task(
        format!(
            "building candidate index for {} canonical names",
            canonical.atoms.len()
        ),
        Some((canonical.atoms.len() as u64).saturating_mul(2)),
        "build units",
    );
    let completed_build_units = std::sync::atomic::AtomicU64::new(0);
    let candidate_index = pool.install(|| {
        NameCandidateIndex::new_with_progress(&canonical.atoms, || {
            let completed = completed_build_units
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                .saturating_add(1);
            if completed.is_multiple_of(16_384) {
                progress.advance_task(16_384, ProgressCounters::default());
            }
        })
    });
    let build_total = (canonical.atoms.len() as u64).saturating_mul(2);
    progress.advance_task(build_total % 16_384, ProgressCounters::default());
    progress.finish_task("name candidate index ready");
    progress.step_stage("built name candidate index");
    let actual_index_bytes = candidate_index.memory_bytes();
    if actual_index_bytes > index_estimate.resident_bytes {
        return Err(AnalysisError::InvalidData(format!(
            "name candidate index used {}, exceeding conservative preflight estimate {}",
            format_byte_size(actual_index_bytes),
            format_byte_size(index_estimate.resident_bytes)
        )));
    }
    let mut rows = Vec::new();
    progress.start_task(
        "scoring canonical name left nodes",
        Some(canonical.atoms.len().saturating_sub(1) as u64),
        "names",
    );
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
    progress.finish_task(format!(
        "name scoring complete; candidates {}; scored {}; matched {}",
        scoring.candidate_pairs, scoring.scored_pairs, scoring.matched_pairs
    ));
    progress.step_stage("scored canonical names");
    drop(candidate_index);
    drop(canonical);
    drop(pool);
    let summary_units = chains.len() as u64 + chain_pair_count(chains.len()) as u64 * 2;
    progress.start_task(
        "summarizing name components",
        Some(summary_units),
        "summaries",
    );
    push_name_summary_rows(
        &mut rows,
        &atoms,
        &atoms_by_chain,
        chains,
        totals,
        &mut state,
    );
    progress.advance_task(chains.len() as u64, ProgressCounters::default());
    drop(atoms_by_chain);
    state.intra = UnionFind::new(0);
    state.cross = None;
    if chains.len() > 1 {
        push_reused_chain_matrix_rows(&mut rows, &atoms, chains, totals, &mut state);
        progress.advance_task(
            chain_pair_count(chains.len()) as u64 * 2,
            ProgressCounters::default(),
        );
    }
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
            candidate_index_bytes: actual_index_bytes as u64,
            scratch_and_queue_bytes: scratch_plan.reserved_bytes as u64,
            dsu_and_chain_state_bytes: state_bytes as u64,
        },
    })
}

fn estimate_name_atom_load(conn: &Connection) -> Result<(u64, usize), AnalysisError> {
    let (rows, string_bytes): (u64, u64) = conn.query_row(
        "SELECT count(*)::UBIGINT,
                (coalesce(sum(length(name_norm)), 0) * 4)::UBIGINT
         FROM name_atoms",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let struct_bytes = rows
        .checked_mul(std::mem::size_of::<NameAtom>() as u64)
        .ok_or_else(|| AnalysisError::InvalidData("name load estimate overflow".into()))?;
    let total = struct_bytes
        .checked_add(string_bytes)
        .ok_or_else(|| AnalysisError::InvalidData("name load estimate overflow".into()))?;
    let total = usize::try_from(total)
        .map_err(|_| AnalysisError::InvalidData("name load estimate exceeds usize".into()))?;
    Ok((rows, total))
}

#[cfg(test)]
pub(crate) fn count_all_name_atoms(conn: &Connection) -> Result<u64, AnalysisError> {
    estimate_name_atom_load(conn).map(|(rows, _)| rows)
}

pub(crate) fn load_all_name_atoms(
    conn: &Connection,
    chains: &[String],
    pool: &rayon::ThreadPool,
    mut on_rows_loaded: impl FnMut(u64),
) -> Result<Vec<NameAtom>, AnalysisError> {
    let chain_indexes = chains
        .iter()
        .enumerate()
        .map(|(index, chain)| (chain.as_str(), index))
        .collect::<HashMap<_, _>>();
    let mut stmt =
        conn.prepare("SELECT chain, name_norm, contract_count, nft_count FROM name_atoms")?;
    let batches = stmt.query_arrow([])?;
    let mut atoms = Vec::new();
    for batch in batches {
        let row_count = batch.num_rows();
        let chains = batch.column(0).as_ref();
        let names = batch.column(1).as_ref();
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
        let mut batch_atoms = pool.install(|| {
            (0..row_count)
                .into_par_iter()
                .map(|index| -> Result<Option<NameAtom>, AnalysisError> {
                    if chains.is_null(index)
                        || names.is_null(index)
                        || contract_counts.is_null(index)
                        || nft_counts.is_null(index)
                    {
                        return Err(AnalysisError::InvalidData(
                            "name atom row contains NULL".into(),
                        ));
                    }
                    let chain = name_arrow_string(chains, index)?;
                    let Some(chain_index) = chain_indexes.get(chain).copied() else {
                        return Ok(None);
                    };
                    let name = name_arrow_string(names, index)?;
                    Ok(Some(NameAtom {
                        chain_index,
                        name_norm: name.to_owned(),
                        char_len: name.chars().count(),
                        contract_count: contract_counts.value(index),
                        nft_count: nft_counts.value(index),
                    }))
                })
                .collect::<Result<Vec<_>, _>>()
        })?;
        atoms.extend(batch_atoms.drain(..).flatten());
        on_rows_loaded(row_count as u64);
    }
    pool.install(|| {
        atoms.par_sort_unstable_by(|left, right| {
            left.char_len
                .cmp(&right.char_len)
                .then_with(|| left.chain_index.cmp(&right.chain_index))
                .then_with(|| left.name_norm.cmp(&right.name_norm))
        });
    });
    Ok(atoms)
}

fn name_arrow_string(array: &dyn Array, index: usize) -> Result<&str, AnalysisError> {
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(values.value(index));
    }
    if let Some(values) = array.as_any().downcast_ref::<StringViewArray>() {
        return Ok(values.value(index));
    }
    Err(AnalysisError::InvalidData(
        "name text column is not UTF8".into(),
    ))
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
