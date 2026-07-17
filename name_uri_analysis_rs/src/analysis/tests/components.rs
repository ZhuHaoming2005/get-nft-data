use super::*;

#[test]
fn dense_summary_scratch_can_be_reused() {
    let atoms = vec![
        NameAtom {
            chain_index: 0,
            name_norm: "azuki".into(),
            char_len: 5,
            contract_count: 1,
            nft_count: 2,
        },
        NameAtom {
            chain_index: 0,
            name_norm: "azukis".into(),
            char_len: 6,
            contract_count: 1,
            nft_count: 3,
        },
    ];
    let primary_atoms = vec![0, 1];
    let mut union_find = UnionFind::new(atoms.len());
    union_find.union(0, 1);
    let mut scratch = DenseComponentScratch::new(atoms.len());

    let first = summarize_components_for_primary_with_scratch(
        &atoms,
        &primary_atoms,
        &mut union_find,
        &mut scratch,
    );
    let second = summarize_components_for_primary_with_scratch(
        &atoms,
        &primary_atoms,
        &mut union_find,
        &mut scratch,
    );

    assert_eq!(first.duplicate_contract_count, 2);
    assert_eq!(first.duplicate_nft_count, 5);
    assert_eq!(first, second);
}

#[test]
fn sparse_union_find_reports_only_existing_connections() {
    let mut union_find = SparseUnionFind::default();

    assert!(!union_find.connected(1, 2));
    assert_eq!(union_find.atom_count(), 0);
    union_find.union(1, 2);
    assert!(union_find.connected(1, 2));
    assert!(!union_find.connected(1, 3));
    assert_eq!(union_find.atom_count(), 2);
}

#[test]
fn chain_pair_indexes_round_trip() {
    let chain_count = 5;
    let mut seen = Vec::new();

    for left in 0..chain_count {
        for right in left + 1..chain_count {
            let index = chain_pair_index(left, right, chain_count);
            seen.push(index);
            assert_eq!(chain_pair_from_index(index, chain_count), (left, right));
        }
    }

    seen.sort_unstable();
    assert_eq!(seen, (0..chain_pair_count(chain_count)).collect::<Vec<_>>());
}

#[test]
fn destructive_chain_matrix_summary_preserves_pair_row_order_and_releases_state() {
    let atoms = (0..3)
        .map(|chain_index| NameAtom {
            chain_index,
            name_norm: format!("chain-{chain_index}").into(),
            char_len: 7,
            contract_count: 1,
            nft_count: 10,
        })
        .collect::<Vec<_>>();
    let chains = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    let mut matrix = new_chain_matrix_reuse_states(chain_pair_count(chains.len()));
    matrix[chain_pair_index(0, 1, chains.len())].union(0, 1);
    matrix[chain_pair_index(0, 2, chains.len())].union(0, 2);
    matrix[chain_pair_index(1, 2, chains.len())].union(1, 2);
    let mut state = ThresholdUnionState {
        threshold: 95.0,
        intra: UnionFind::new(atoms.len()),
        cross: None,
        chain_matrix: Some(ChainMatrixState::Resident(matrix)),
    };
    let mut rows = Vec::new();

    let mut atoms_by_chain = atoms_by_chain(&atoms, chains.len());
    push_reused_chain_matrix_rows(
        &mut rows,
        &atoms,
        &mut atoms_by_chain,
        &chains,
        &HashMap::new(),
        &mut state,
    )
    .unwrap();

    assert_eq!(
        rows.iter()
            .map(|row| (row.primary_chain.as_str(), row.secondary_chain.as_str()))
            .collect::<Vec<_>>(),
        vec![
            ("a", "b"),
            ("b", "a"),
            ("a", "c"),
            ("c", "a"),
            ("b", "c"),
            ("c", "b"),
        ]
    );
    assert!(state.chain_matrix.is_none());
}

#[test]
fn spilled_chain_matrix_matches_resident_rows_and_cleans_scratch() {
    let atoms = (0..3)
        .map(|chain_index| NameAtom {
            chain_index,
            name_norm: format!("chain-{chain_index}").into(),
            char_len: 7,
            contract_count: chain_index as i64 + 1,
            nft_count: (chain_index as i64 + 1) * 10,
        })
        .collect::<Vec<_>>();
    let chains = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    let totals = HashMap::new();
    let mut resident_matrix = new_chain_matrix_reuse_states(chain_pair_count(chains.len()));
    resident_matrix[chain_pair_index(0, 1, chains.len())].union(0, 1);
    resident_matrix[chain_pair_index(0, 2, chains.len())].union(0, 2);
    resident_matrix[chain_pair_index(1, 2, chains.len())].union(1, 2);
    let mut resident_state = ThresholdUnionState {
        threshold: 95.0,
        intra: UnionFind::new(atoms.len()),
        cross: None,
        chain_matrix: Some(ChainMatrixState::Resident(resident_matrix)),
    };
    let mut resident_atoms_by_chain = atoms_by_chain(&atoms, chains.len());
    let mut resident_rows = Vec::new();
    push_reused_chain_matrix_rows(
        &mut resident_rows,
        &atoms,
        &mut resident_atoms_by_chain,
        &chains,
        &totals,
        &mut resident_state,
    )
    .unwrap();

    let temp = tempfile::tempdir().unwrap();
    let spill_directory = temp.path().join("name-chain-matrix-spill");
    let mut spill = ChainMatrixSpill::new(
        spill_directory.clone(),
        chain_pair_atom_capacities(&resident_atoms_by_chain),
    )
    .unwrap();
    spill.record_edge(chain_pair_index(0, 1, chains.len()), 0, 1, &atoms);
    spill.record_edge(chain_pair_index(0, 2, chains.len()), 0, 2, &atoms);
    spill.record_edge(chain_pair_index(1, 2, chains.len()), 1, 2, &atoms);
    let mut spill_state = ThresholdUnionState {
        threshold: 95.0,
        intra: UnionFind::new(atoms.len()),
        cross: None,
        chain_matrix: Some(ChainMatrixState::Spill(spill)),
    };
    let mut spill_atoms_by_chain = atoms_by_chain(&atoms, chains.len());
    let mut spill_rows = Vec::new();
    push_reused_chain_matrix_rows(
        &mut spill_rows,
        &atoms,
        &mut spill_atoms_by_chain,
        &chains,
        &totals,
        &mut spill_state,
    )
    .unwrap();

    assert_eq!(spill_rows, resident_rows);
    assert!(!spill_directory.exists());
    assert!(spill_state.chain_matrix.is_none());
}

#[test]
fn truncated_chain_matrix_spill_fails_closed_and_cleans_scratch() {
    let atoms = vec![
        NameAtom {
            chain_index: 0,
            name_norm: "a".into(),
            char_len: 1,
            contract_count: 1,
            nft_count: 1,
        },
        NameAtom {
            chain_index: 1,
            name_norm: "b".into(),
            char_len: 1,
            contract_count: 1,
            nft_count: 1,
        },
    ];
    let chains = vec!["a".to_string(), "b".to_string()];
    let mut atoms_by_chain = atoms_by_chain(&atoms, chains.len());
    let temp = tempfile::tempdir().unwrap();
    let spill_directory = temp.path().join("name-chain-matrix-spill");
    let mut spill = ChainMatrixSpill::new(
        spill_directory.clone(),
        chain_pair_atom_capacities(&atoms_by_chain),
    )
    .unwrap();
    spill.finish_writes().unwrap();
    fs::write(spill_directory.join("pair-00000000.edges"), [1u8]).unwrap();
    let mut state = ThresholdUnionState {
        threshold: 95.0,
        intra: UnionFind::new(atoms.len()),
        cross: None,
        chain_matrix: Some(ChainMatrixState::Spill(spill)),
    };

    let error = push_reused_chain_matrix_rows(
        &mut Vec::new(),
        &atoms,
        &mut atoms_by_chain,
        &chains,
        &HashMap::new(),
        &mut state,
    )
    .unwrap_err();

    assert!(error.to_string().contains("truncated edge data"));
    assert!(!spill_directory.exists());
    assert!(state.chain_matrix.is_none());
}

#[test]
fn name_union_state_plan_spills_then_uses_dense_cross_at_exact_boundaries() {
    let chain_count = 32usize;
    let atom_count = 100_000usize;
    let mut atoms_by_chain = vec![Vec::new(); chain_count];
    for index in 0..atom_count {
        atoms_by_chain[index % chain_count].push(index as u32);
    }
    let unconstrained = name_union_state_plan(atom_count, &atoms_by_chain, usize::MAX);
    let sparse_spill_budget = unconstrained
        .intra_bytes
        .saturating_add(sparse_union_find_bytes(atom_count))
        .saturating_add(unconstrained.spill_chain_matrix_bytes);
    let sparse_spill = name_union_state_plan(atom_count, &atoms_by_chain, sparse_spill_budget);
    let dense_spill_budget = unconstrained
        .intra_bytes
        .saturating_add(dense_union_find_bytes(atom_count))
        .saturating_add(unconstrained.spill_chain_matrix_bytes);
    let dense_spill = name_union_state_plan(atom_count, &atoms_by_chain, dense_spill_budget);

    assert_eq!(
        unconstrained.chain_matrix_strategy,
        Some(NameChainMatrixStrategy::Resident)
    );
    assert_eq!(
        sparse_spill.cross_strategy,
        Some(NameCrossStateStrategy::Sparse)
    );
    assert_eq!(
        sparse_spill.chain_matrix_strategy,
        Some(NameChainMatrixStrategy::Spill)
    );
    assert_eq!(sparse_spill.total_bytes, sparse_spill_budget);
    assert_eq!(
        dense_spill.cross_strategy,
        Some(NameCrossStateStrategy::Dense)
    );
    assert_eq!(
        dense_spill.chain_matrix_strategy,
        Some(NameChainMatrixStrategy::Spill)
    );
    assert_eq!(dense_spill.total_bytes, dense_spill_budget);
}

#[test]
fn time_first_state_selection_spills_before_collapsing_scoring_to_scan() {
    let chain_count = 32usize;
    let atom_count = 100_000usize;
    let mut atoms_by_chain = vec![Vec::new(); chain_count];
    for index in 0..atom_count {
        atoms_by_chain[index % chain_count].push(index as u32);
    }
    let resident = name_union_state_plan(atom_count, &atoms_by_chain, usize::MAX);
    let one_dense_lane = name_scratch_plan(atom_count, 1, usize::MAX);
    let available = resident
        .total_bytes
        .saturating_add(one_dense_lane.reserved_bytes.saturating_sub(1));

    let (selected_state, selected_scratch) =
        select_name_union_and_scratch_plan(atom_count, atom_count, &atoms_by_chain, 8, available);

    assert_eq!(
        selected_state.chain_matrix_strategy,
        Some(NameChainMatrixStrategy::Spill)
    );
    assert_eq!(selected_scratch.mode, NameScratchMode::Dense);
    assert!(selected_scratch.admitted_workers > 1);
}

#[test]
fn low_memory_summaries_match_dense_one_pass_and_preserve_row_order() {
    let atoms = vec![
        NameAtom {
            chain_index: 0,
            name_norm: "alpha".into(),
            char_len: 5,
            contract_count: 1,
            nft_count: 10,
        },
        NameAtom {
            chain_index: 0,
            name_norm: "alphas".into(),
            char_len: 6,
            contract_count: 2,
            nft_count: 20,
        },
        NameAtom {
            chain_index: 1,
            name_norm: "beta".into(),
            char_len: 4,
            contract_count: 3,
            nft_count: 30,
        },
        NameAtom {
            chain_index: 1,
            name_norm: "betas".into(),
            char_len: 5,
            contract_count: 4,
            nft_count: 40,
        },
    ];
    let chains = vec!["base".to_string(), "ethereum".to_string()];
    let totals = HashMap::from([
        (
            "base".to_string(),
            NameTotals {
                contracts: 3,
                nfts: 30,
            },
        ),
        (
            "ethereum".to_string(),
            NameTotals {
                contracts: 7,
                nfts: 70,
            },
        ),
    ]);
    let build_state = || {
        let mut intra = UnionFind::new(atoms.len());
        intra.union(0, 1);
        intra.union(2, 3);
        let mut cross = SparseUnionFind::default();
        cross.union(0, 2);
        cross.union(1, 3);
        ThresholdUnionState {
            threshold: 95.0,
            intra,
            cross: Some(CrossUnionState::Sparse(cross)),
            chain_matrix: None,
        }
    };
    let shape = NameSummaryMemoryShape {
        analysis_budget_bytes: usize::MAX,
        base_resident_bytes: 0,
        atom_count: atoms.len(),
        max_chain_atom_count: 2,
        cross_atom_count: atoms.len(),
        max_cross_chain_atom_count: 2,
        chain_count: chains.len(),
        intra_state_bytes: 0,
        cross_state_bytes: 0,
        chain_matrix_state_bytes: 0,
    };
    let fast_plan = name_summary_scratch_plan(shape);
    let low_plan = name_summary_scratch_plan(NameSummaryMemoryShape {
        analysis_budget_bytes: 0,
        ..shape
    });

    let mut fast_rows = Vec::new();
    let mut fast_atoms_by_chain = atoms_by_chain(&atoms, chains.len());
    let mut fast_state = build_state();
    push_name_summary_rows(
        &mut fast_rows,
        &atoms,
        &mut fast_atoms_by_chain,
        &chains,
        &totals,
        &mut fast_state,
        fast_plan,
    )
    .unwrap();

    let mut low_rows = Vec::new();
    let mut low_atoms_by_chain = atoms_by_chain(&atoms, chains.len());
    let mut low_state = build_state();
    push_name_summary_rows(
        &mut low_rows,
        &atoms,
        &mut low_atoms_by_chain,
        &chains,
        &totals,
        &mut low_state,
        low_plan,
    )
    .unwrap();

    assert_eq!(fast_rows, low_rows);
    assert_eq!(
        fast_rows
            .iter()
            .map(|row| row.scope.as_str())
            .collect::<Vec<_>>(),
        vec![
            "intra_chain",
            "cross_chain_summary",
            "intra_chain",
            "cross_chain_summary"
        ]
    );
    assert!(fast_state.intra.parent.is_empty());
    assert!(fast_state.cross.is_none());
    assert!(low_state.intra.parent.is_empty());
    assert!(low_state.cross.is_none());
}

#[test]
fn name_summary_scratch_plan_accounts_for_every_dense_heap_vector() {
    let shape = NameSummaryMemoryShape {
        analysis_budget_bytes: usize::MAX,
        base_resident_bytes: 1_000,
        atom_count: 100,
        max_chain_atom_count: 60,
        cross_atom_count: 80,
        max_cross_chain_atom_count: 50,
        chain_count: 3,
        intra_state_bytes: 200,
        cross_state_bytes: 300,
        chain_matrix_state_bytes: 400,
    };
    let plan = name_summary_scratch_plan(shape);
    let summaries = shape.chain_count * std::mem::size_of::<GroupSummary>();
    let vec_header = std::mem::size_of::<Vec<u8>>();
    let expected_intra =
        dense_component_scratch_bytes(shape.atom_count, shape.max_chain_atom_count)
            + summaries
            + INTRA_SUMMARY_LIVE_VEC_HEADERS * vec_header
            + plan.intra_allocation_headroom_bytes;
    let expected_cross = summaries
        + dense_component_scratch_bytes(shape.cross_atom_count, shape.max_cross_chain_atom_count)
        + sparse_all_chain_summary_workspace_bytes(shape.cross_atom_count, shape.chain_count)
        + CROSS_SUMMARY_LIVE_VEC_HEADERS * vec_header
        + plan.cross_allocation_headroom_bytes;

    assert_eq!(plan.intra_fast_resident_bytes, 1_900);
    assert_eq!(plan.cross_fast_resident_bytes, 1_700);
    assert!(plan.intra_allocation_headroom_bytes >= NAME_SUMMARY_MIN_ALLOCATION_HEADROOM_BYTES);
    assert!(plan.cross_allocation_headroom_bytes >= NAME_SUMMARY_MIN_ALLOCATION_HEADROOM_BYTES);
    assert_eq!(plan.intra_fast_scratch_bytes, expected_intra);
    assert_eq!(plan.cross_fast_scratch_bytes, expected_cross);
    assert_eq!(
        plan.intra_fast_peak_bytes,
        plan.intra_fast_resident_bytes + expected_intra
    );
    assert_eq!(
        plan.cross_fast_peak_bytes,
        plan.cross_fast_resident_bytes + expected_cross
    );
    assert_eq!(
        plan.low_memory_heap_scratch_bytes,
        summaries * 2 + 2 * vec_header + shape.cross_atom_count * std::mem::size_of::<u32>()
    );
}

#[test]
fn name_right_range_index_is_admitted_at_the_exact_budget_boundary() {
    let atom_count = 1_001usize;
    let base_peak = 10_000usize;
    let index_bytes = right_name_range_index_bytes(atom_count);

    assert_eq!(
        admitted_name_right_range_index_bytes(atom_count, base_peak, base_peak + index_bytes),
        index_bytes
    );
    assert_eq!(
        admitted_name_right_range_index_bytes(atom_count, base_peak, base_peak + index_bytes - 1),
        0
    );
    assert_eq!(
        admitted_name_right_range_index_bytes(1, base_peak, usize::MAX),
        0
    );
}

#[test]
fn name_string_arena_switches_to_mmap_only_above_the_budget_boundary() {
    let resident_peak = 1_000_000usize;

    assert_eq!(
        select_name_string_storage_mode(resident_peak, resident_peak),
        NameStringStorageMode::Resident
    );
    assert_eq!(
        select_name_string_storage_mode(resident_peak, resident_peak - 1),
        NameStringStorageMode::Mapped
    );
}

#[test]
fn compact_union_find_capacity_estimates_cover_growth_boundaries() {
    for atom_count in [0usize, 1, 3, 4, 7, 8, 14, 15, 28, 29, 56, 57, 1_000] {
        let dense = UnionFind::new(atom_count);
        assert_eq!(
            union_find_resident_bytes(&dense),
            dense_union_find_bytes(atom_count),
            "dense atom_count={atom_count}"
        );

        let mut sparse = SparseUnionFind::default();
        for atom in 0..atom_count {
            sparse.get_or_insert(atom);
        }
        assert!(
            sparse_union_find_resident_bytes(&sparse) <= sparse_union_find_bytes(atom_count),
            "sparse atom_count={atom_count}, actual={}, estimate={}",
            sparse_union_find_resident_bytes(&sparse),
            sparse_union_find_bytes(atom_count),
        );
    }
}

#[test]
fn iterative_path_halving_handles_deep_dense_and_sparse_trees() {
    let atom_count = 100_000usize;
    let mut dense = UnionFind::new(atom_count);
    for index in 0..atom_count - 1 {
        dense.parent[index] = (index + 1) as u32;
    }
    assert_eq!(dense.find(0), atom_count - 1);
    assert_eq!(dense.find(0), atom_count - 1);

    let mut sparse = SparseUnionFind {
        index_by_atom: HashMap::new(),
        atoms: (0..atom_count as u32).collect(),
        parent: (0..atom_count as u32).collect(),
        rank: vec![0; atom_count],
    };
    for index in 0..atom_count - 1 {
        sparse.parent[index] = (index + 1) as u32;
    }
    assert_eq!(sparse.find_local(0), atom_count - 1);
    assert_eq!(sparse.find_local(0), atom_count - 1);
}

#[test]
fn name_summary_scratch_plan_falls_back_at_the_budget_boundary() {
    let shape = NameSummaryMemoryShape {
        analysis_budget_bytes: usize::MAX,
        base_resident_bytes: 1_000,
        atom_count: 100,
        max_chain_atom_count: 60,
        cross_atom_count: 80,
        max_cross_chain_atom_count: 50,
        chain_count: 3,
        intra_state_bytes: 200,
        cross_state_bytes: 300,
        chain_matrix_state_bytes: 400,
    };
    let unconstrained = name_summary_scratch_plan(shape);
    let intra_boundary = name_summary_scratch_plan(NameSummaryMemoryShape {
        analysis_budget_bytes: unconstrained.intra_fast_peak_bytes,
        ..shape
    });
    let cross_boundary = name_summary_scratch_plan(NameSummaryMemoryShape {
        analysis_budget_bytes: unconstrained.cross_fast_peak_bytes,
        ..shape
    });
    let intra_below = name_summary_scratch_plan(NameSummaryMemoryShape {
        analysis_budget_bytes: unconstrained.intra_fast_peak_bytes.saturating_sub(1),
        ..shape
    });
    let cross_below = name_summary_scratch_plan(NameSummaryMemoryShape {
        analysis_budget_bytes: unconstrained.cross_fast_peak_bytes.saturating_sub(1),
        ..shape
    });

    assert_eq!(
        intra_boundary.intra_strategy,
        NameSummaryStrategy::DenseOnePass
    );
    assert_eq!(
        cross_boundary.cross_strategy,
        NameSummaryStrategy::DenseOnePass
    );
    assert_eq!(intra_below.intra_strategy, NameSummaryStrategy::LowMemory);
    assert_eq!(cross_below.cross_strategy, NameSummaryStrategy::LowMemory);
}

#[test]
fn deferred_cross_replay_dense_state_is_counted_at_the_summary_boundary() {
    let atoms = vec![
        NameAtom {
            chain_index: 0,
            name_norm: "a".into(),
            char_len: 1,
            contract_count: 1,
            nft_count: 1,
        },
        NameAtom {
            chain_index: 1,
            name_norm: "b".into(),
            char_len: 1,
            contract_count: 1,
            nft_count: 1,
        },
    ];
    let atoms_by_chain = atoms_by_chain(&atoms, 2);
    let state = ThresholdUnionState {
        threshold: 95.0,
        intra: UnionFind::new(0),
        cross: Some(CrossUnionState::Deferred),
        chain_matrix: None,
    };
    let base_resident_bytes = 1_000usize;
    let unconstrained = name_summary_plan_for_state(
        usize::MAX,
        base_resident_bytes,
        &atoms,
        &atoms_by_chain,
        &state,
        2,
    );
    let at_boundary = name_summary_plan_for_state(
        unconstrained.cross_fast_peak_bytes,
        base_resident_bytes,
        &atoms,
        &atoms_by_chain,
        &state,
        2,
    );
    let below_boundary = name_summary_plan_for_state(
        unconstrained.cross_fast_peak_bytes.saturating_sub(1),
        base_resident_bytes,
        &atoms,
        &atoms_by_chain,
        &state,
        2,
    );

    assert_eq!(
        unconstrained.cross_fast_resident_bytes,
        base_resident_bytes + dense_union_find_bytes(atoms.len())
    );
    assert_eq!(
        at_boundary.cross_strategy,
        NameSummaryStrategy::DenseOnePass
    );
    assert_eq!(
        below_boundary.cross_strategy,
        NameSummaryStrategy::LowMemory
    );
}

#[test]
fn resident_chain_matrix_summary_counts_compact_pair_sort_scratch() {
    let atoms = vec![
        NameAtom {
            chain_index: 0,
            name_norm: "a".into(),
            char_len: 1,
            contract_count: 1,
            nft_count: 1,
        },
        NameAtom {
            chain_index: 1,
            name_norm: "b".into(),
            char_len: 1,
            contract_count: 1,
            nft_count: 1,
        },
    ];
    let atoms_by_chain = atoms_by_chain(&atoms, 2);
    let mut pair = SparseUnionFind::default();
    pair.union(0, 1);
    let matrix = vec![pair];
    let expected_matrix_peak = matrix
        .capacity()
        .saturating_mul(std::mem::size_of::<SparseUnionFind>())
        .saturating_add(
            matrix
                .iter()
                .map(sparse_union_find_resident_bytes)
                .sum::<usize>(),
        )
        .saturating_add(
            matrix[0]
                .atom_count()
                .saturating_mul(std::mem::size_of::<u32>()),
        );
    let state = ThresholdUnionState {
        threshold: 95.0,
        intra: UnionFind::new(0),
        cross: None,
        chain_matrix: Some(ChainMatrixState::Resident(matrix)),
    };

    let plan = name_summary_plan_for_state(usize::MAX, 0, &atoms, &atoms_by_chain, &state, 2);

    assert_eq!(plan.intra_fast_resident_bytes, expected_matrix_peak);
    assert_eq!(plan.cross_fast_resident_bytes, expected_matrix_peak);
}
