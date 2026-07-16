use super::*;

#[test]
fn full_name_pair_scoring_keeps_only_threshold_matches() {
    let atoms = vec![
        NameAtom {
            chain_index: 0,
            name_norm: "azuki".into(),
            char_len: 5,
            contract_count: 1,
            nft_count: 1,
        },
        NameAtom {
            chain_index: 1,
            name_norm: "azuki".into(),
            char_len: 5,
            contract_count: 1,
            nft_count: 1,
        },
        NameAtom {
            chain_index: 1,
            name_norm: "moonbirds".into(),
            char_len: 9,
            contract_count: 1,
            nft_count: 1,
        },
    ];
    let candidate_index = NameCandidateIndex::new(&atoms);
    let mut scratch = NameCandidateScratch::with_mode(atoms.len(), NameScratchMode::Dense);

    let hits = scored_rights_for_left(
        &atoms,
        &candidate_index,
        0,
        1..atoms.len(),
        90.0,
        &mut scratch,
    );

    assert_eq!(hits, vec![1]);
}

#[test]
fn name_candidate_index_memory_includes_vector_headers() {
    let atoms = vec![NameAtom {
        chain_index: 0,
        name_norm: "a".into(),
        char_len: 1,
        contract_count: 1,
        nft_count: 1,
    }];
    let candidate_index = NameCandidateIndex::new(&atoms);
    let minimum_structural_bytes = candidate_index.documents.capacity()
        * std::mem::size_of::<IndexedNameDocument>()
        + candidate_index.postings.capacity() * std::mem::size_of::<Vec<NameAtomIndex>>();

    assert!(candidate_index.memory_bytes() >= minimum_structural_bytes);
}

#[test]
fn name_candidate_index_estimate_covers_resident_allocation() {
    let atoms = vec![
        NameAtom {
            chain_index: 0,
            name_norm: "alpha".into(),
            char_len: 5,
            contract_count: 1,
            nft_count: 1,
        },
        NameAtom {
            chain_index: 0,
            name_norm: "alphabet".into(),
            char_len: 8,
            contract_count: 1,
            nft_count: 1,
        },
        NameAtom {
            chain_index: 1,
            name_norm: "金色dragon".into(),
            char_len: 8,
            contract_count: 1,
            nft_count: 1,
        },
    ];
    let estimate = estimate_name_candidate_index_bytes(&atoms);
    let actual = NameCandidateIndex::new(&atoms).memory_bytes();

    assert!(estimate.resident_bytes >= actual);
    assert!(estimate.peak_build_bytes >= estimate.resident_bytes);
}

#[test]
fn name_candidate_index_reports_both_build_passes() {
    use std::sync::atomic::{AtomicU64, Ordering};

    let atoms = vec![
        NameAtom {
            chain_index: 0,
            name_norm: "alpha".into(),
            char_len: 5,
            contract_count: 1,
            nft_count: 1,
        },
        NameAtom {
            chain_index: 0,
            name_norm: "beta".into(),
            char_len: 4,
            contract_count: 1,
            nft_count: 1,
        },
        NameAtom {
            chain_index: 1,
            name_norm: "gamma".into(),
            char_len: 5,
            contract_count: 1,
            nft_count: 1,
        },
    ];
    let completed = AtomicU64::new(0);

    let index = NameCandidateIndex::new_with_progress(&atoms, || {
        completed.fetch_add(1, Ordering::Relaxed);
    });

    assert_eq!(index.documents.len(), atoms.len());
    assert_eq!(completed.load(Ordering::Relaxed), 2 * atoms.len() as u64);
}

#[test]
fn name_atom_loader_reports_the_exact_scanned_row_total() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE name_atoms(
             chain VARCHAR,
             name_norm VARCHAR,
             contract_count BIGINT,
             nft_count BIGINT
         );
         INSERT INTO name_atoms VALUES
             ('base', 'alpha', 1, 2),
             ('ethereum', 'beta', 3, 4),
             ('ethereum', 'gamma', 5, 6);",
    )
    .unwrap();
    let chains = vec!["base".to_string(), "ethereum".to_string()];
    let mut scanned = 0u64;

    let total = count_all_name_atoms(&conn).unwrap();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .unwrap();
    let atoms = load_all_name_atoms(&conn, &chains, &pool, |delta| scanned += delta).unwrap();

    assert_eq!(total, 3);
    assert_eq!(scanned, total);
    assert_eq!(atoms.len() as u64, total);
}

#[test]
fn name_worker_stack_reservation_scales_per_worker() {
    assert_eq!(
        name_worker_stack_reserve_bytes(4) - name_worker_stack_reserve_bytes(1),
        3 * NAME_ANALYSIS_WORKER_STACK_BYTES
    );
}

#[test]
fn name_atom_memory_counts_reserved_vector_capacity() {
    let mut atoms = Vec::with_capacity(32);
    atoms.push(NameAtom {
        chain_index: 0,
        name_norm: String::with_capacity(64),
        char_len: 0,
        contract_count: 1,
        nft_count: 1,
    });

    let expected =
        atoms.capacity() * std::mem::size_of::<NameAtom>() + atoms[0].name_norm.capacity();

    assert_eq!(name_atoms_memory_bytes(&atoms), expected);
}

#[test]
fn name_candidates_never_escape_the_requested_right_range() {
    let atoms = ["aaaaa", "aaaab", "aaaba", "aabaa", "abaaa"]
        .into_iter()
        .map(|name| NameAtom {
            chain_index: 0,
            name_norm: name.to_string(),
            char_len: name.chars().count(),
            contract_count: 1,
            nft_count: 1,
        })
        .collect::<Vec<_>>();
    let index = NameCandidateIndex::new(&atoms);
    let mut scratch = NameCandidateScratch::with_mode(atoms.len(), NameScratchMode::Dense);

    let candidates = index
        .candidates_for_left(&atoms, 0, 2..4, 80.0, &mut scratch)
        .to_vec();

    assert_eq!(candidates, vec![2, 3]);
}

#[test]
fn name_scratch_plan_uses_dense_mode_only_when_all_workers_fit() {
    let atom_count = 1_000_000usize;
    let threads = 96;
    let workers = threads.min(atom_count - 1);
    let candidate_capacity = atom_count.max(4).next_power_of_two();
    let common_bytes = candidate_capacity * std::mem::size_of::<NameAtomIndex>() * workers
        + NAME_EDGE_CHUNK_SIZE * std::mem::size_of::<(usize, ScoredRight)>() * workers * 4;
    let dense_bytes = common_bytes + atom_count * std::mem::size_of::<u16>() * workers;

    let dense = name_scratch_plan(atom_count, threads, dense_bytes);
    let sparse = name_scratch_plan(atom_count, threads, dense_bytes - 1);

    assert_eq!(dense.mode, NameScratchMode::Dense);
    assert_eq!(dense.reserved_bytes, dense_bytes);
    assert_eq!(sparse.mode, NameScratchMode::Sparse);
    assert!(sparse.reserved_bytes > common_bytes);
}

#[test]
fn name_scratch_plan_prefers_budgeted_dense_mode_above_the_old_atom_threshold() {
    let atom_count = (1 << 20) + 1;
    let plan = name_scratch_plan(atom_count, 2, usize::MAX);

    assert_eq!(plan.mode, NameScratchMode::Dense);
}

#[test]
fn name_scratch_plan_reserves_the_candidate_vectors_actual_worst_capacity() {
    let atom_count = 5usize;
    let workers = 1usize;
    let candidate_capacity = atom_count.max(4).next_power_of_two();
    let expected = candidate_capacity * std::mem::size_of::<NameAtomIndex>() * workers
        + NAME_EDGE_CHUNK_SIZE * std::mem::size_of::<(usize, ScoredRight)>() * workers * 4
        + atom_count * std::mem::size_of::<u16>() * workers;

    let plan = name_scratch_plan(atom_count, workers, usize::MAX);

    assert_eq!(plan.mode, NameScratchMode::Dense);
    assert_eq!(plan.reserved_bytes, expected);
}

#[test]
fn dense_name_scratch_uses_u16_generations_and_resets_after_wrap() {
    let atoms = vec![
        NameAtom {
            chain_index: 0,
            name_norm: "azuki".into(),
            char_len: 5,
            contract_count: 1,
            nft_count: 1,
        },
        NameAtom {
            chain_index: 1,
            name_norm: "azuki".into(),
            char_len: 5,
            contract_count: 1,
            nft_count: 1,
        },
    ];
    let index = NameCandidateIndex::new(&atoms);
    let mut scratch = NameCandidateScratch::with_mode(2, NameScratchMode::Dense);

    for _ in 0..=u16::MAX {
        let candidates = index.candidates_for_left(&atoms, 0, 1..2, 90.0, &mut scratch);
        assert_eq!(candidates, [1]);
    }
}

#[test]
fn prepared_name_query_preserves_unicode_scores_and_cutoff() {
    let query = PreparedNameQuery::new("金色 dragon");

    assert_eq!(query.score_percent("金色 dragon", 95.0), Some(100.0));
    assert_eq!(query.score_percent("silver cat", 95.0), None);
    assert_eq!(query.score_percent("金色 dragon", 101.0), None);

    let expected = PreparedNameQuery::new("金色 dragon")
        .score_percent("金色 dragons", 0.0)
        .expect("zero cutoff must return a Jaro-Winkler score");
    let actual = query
        .score_percent("金色 dragons", 0.0)
        .expect("zero cutoff must return a score");
    assert!((actual - expected).abs() < 1e-9);
    assert!(query.score_percent("金色 dragons", expected).is_some());
    assert!(query
        .score_percent("金色 dragons", expected + 1e-9)
        .is_none());
}

#[test]
fn jaro_winkler_upper_bound_filters_impossible_thresholds() {
    let upper_bound = jaro_winkler_upper_bound_from_lengths(5, 26);

    assert!(upper_bound < 90.0);
    assert!(name_pair_lengths_can_reach_threshold(5, 6, 90.0));
    assert_eq!(
        upper_bound,
        jaro_winkler_upper_bound_from_lengths(
            "azuki".chars().count(),
            "a-very-long-unrelated-name".chars().count()
        )
    );
}

#[test]
fn sorted_name_lengths_bound_candidate_chunks() {
    let atoms = vec![
        NameAtom {
            chain_index: 0,
            name_norm: "azuki".into(),
            char_len: 5,
            contract_count: 1,
            nft_count: 1,
        },
        NameAtom {
            chain_index: 0,
            name_norm: "azukis".into(),
            char_len: 6,
            contract_count: 1,
            nft_count: 1,
        },
        NameAtom {
            chain_index: 0,
            name_norm: "a-very-long-unrelated-name".into(),
            char_len: 26,
            contract_count: 1,
            nft_count: 1,
        },
    ];
    let candidate_index = NameCandidateIndex::new(&atoms);
    let mut scratch = NameCandidateScratch::with_mode(atoms.len(), NameScratchMode::Dense);
    let right_end = right_name_range_end_for_left(&atoms, 0, 95.0);

    assert_eq!(right_end, 2);
    assert_eq!(
        candidate_index
            .candidates_for_left(&atoms, 0, 1..right_end, 95.0, &mut scratch)
            .len(),
        1
    );
}

#[test]
fn name_candidate_index_skips_same_length_names_without_shared_characters() {
    let atoms = vec![
        NameAtom {
            chain_index: 0,
            name_norm: "aaaaa".into(),
            char_len: 5,
            contract_count: 1,
            nft_count: 1,
        },
        NameAtom {
            chain_index: 0,
            name_norm: "bbbbb".into(),
            char_len: 5,
            contract_count: 1,
            nft_count: 1,
        },
        NameAtom {
            chain_index: 0,
            name_norm: "ccccc".into(),
            char_len: 5,
            contract_count: 1,
            nft_count: 1,
        },
    ];
    let candidate_index = NameCandidateIndex::new(&atoms);
    let mut scratch = NameCandidateScratch::with_mode(atoms.len(), NameScratchMode::Dense);

    assert!(candidate_index
        .candidates_for_left(&atoms, 0, 1..atoms.len(), 95.0, &mut scratch)
        .is_empty());
}

#[test]
fn name_candidate_index_preserves_brute_force_threshold_hits() {
    let mut atoms = [
        "azuki",
        "azukii",
        "azkui",
        "aaaaab",
        "aaaaba",
        "金色dragon",
        "金色dragons",
        "silvercat",
    ]
    .into_iter()
    .enumerate()
    .map(|(chain_index, name)| NameAtom {
        chain_index: chain_index % 2,
        name_norm: name.to_string(),
        char_len: name.chars().count(),
        contract_count: 1,
        nft_count: 1,
    })
    .collect::<Vec<_>>();
    atoms.sort_by(|left, right| {
        left.char_len
            .cmp(&right.char_len)
            .then_with(|| left.name_norm.cmp(&right.name_norm))
    });
    let candidate_index = NameCandidateIndex::new(&atoms);
    let mut scratch = NameCandidateScratch::with_mode(atoms.len(), NameScratchMode::Dense);

    for threshold in [90.0, 95.0, 100.0] {
        for left in 0..atoms.len().saturating_sub(1) {
            let actual = scored_rights_for_left(
                &atoms,
                &candidate_index,
                left,
                left + 1..atoms.len(),
                threshold,
                &mut scratch,
            );
            let expected = (left + 1..atoms.len())
                .filter(|&right| {
                    PreparedNameQuery::new(&atoms[left].name_norm)
                        .score_percent(&atoms[right].name_norm, threshold)
                        .is_some()
                })
                .collect::<Vec<_>>();

            assert_eq!(actual, expected, "left={left}, threshold={threshold}");
        }
    }
}

#[test]
fn name_candidate_index_preserves_brute_force_hits_below_winkler_boost() {
    // Thresholds <= 70 used to take the "push all atoms" path because the
    // old length-unaware overlap bound short-circuited to 0. The
    // length-aware bound now yields a real prefix, so exercise that path.
    let mut atoms = [
        "azuki",
        "azukii",
        "azkui",
        "aaaaab",
        "aaaaba",
        "金色dragon",
        "金色dragons",
        "silvercat",
        "cat",
        "cats",
    ]
    .into_iter()
    .enumerate()
    .map(|(chain_index, name)| NameAtom {
        chain_index: chain_index % 2,
        name_norm: name.to_string(),
        char_len: name.chars().count(),
        contract_count: 1,
        nft_count: 1,
    })
    .collect::<Vec<_>>();
    atoms.sort_by(|left, right| {
        left.char_len
            .cmp(&right.char_len)
            .then_with(|| left.name_norm.cmp(&right.name_norm))
    });
    let candidate_index = NameCandidateIndex::new(&atoms);
    let mut scratch = NameCandidateScratch::with_mode(atoms.len(), NameScratchMode::Dense);

    for threshold in [50.0, 60.0, 70.0, 80.0] {
        for left in 0..atoms.len().saturating_sub(1) {
            let actual = scored_rights_for_left(
                &atoms,
                &candidate_index,
                left,
                left + 1..atoms.len(),
                threshold,
                &mut scratch,
            );
            let expected = (left + 1..atoms.len())
                .filter(|&right| {
                    PreparedNameQuery::new(&atoms[left].name_norm)
                        .score_percent(&atoms[right].name_norm, threshold)
                        .is_some()
                })
                .collect::<Vec<_>>();
            assert_eq!(actual, expected, "left={left}, threshold={threshold}");
        }
    }
}

#[test]
fn name_candidate_index_matches_brute_force_on_random_atoms() {
    let mut state: u64 = 0x9e3779b97f4a7c15;
    let alphabet = ['a', 'a', 'b', 'c', 'd', 'e', '金', 'x'];

    for _ in 0..16 {
        let atom_count = 3 + (xorshift(&mut state) % 25) as usize;
        let mut atoms = (0..atom_count)
            .map(|_| {
                let len = 1 + (xorshift(&mut state) % 9) as usize;
                let name: String = (0..len)
                    .map(|_| {
                        let idx = (xorshift(&mut state) as usize) % alphabet.len();
                        alphabet[idx]
                    })
                    .collect();
                NameAtom {
                    chain_index: (xorshift(&mut state) % 2) as usize,
                    name_norm: name.clone(),
                    char_len: name.chars().count(),
                    contract_count: 1,
                    nft_count: 1,
                }
            })
            .collect::<Vec<_>>();
        atoms.sort_by(|left, right| {
            left.char_len
                .cmp(&right.char_len)
                .then_with(|| left.name_norm.cmp(&right.name_norm))
        });
        let candidate_index = NameCandidateIndex::new(&atoms);
        let mut scratch = NameCandidateScratch::with_mode(atoms.len(), NameScratchMode::Dense);
        let threshold = 50.0 + 10.0 * (xorshift(&mut state) % 6) as f64;

        for left in 0..atoms.len().saturating_sub(1) {
            let right_end = right_name_range_end_for_left(&atoms, left, threshold);
            let actual = scored_rights_for_left(
                &atoms,
                &candidate_index,
                left,
                left + 1..right_end,
                threshold,
                &mut scratch,
            );
            // Oracle uses the same rapidfuzz scorer as the index, so this
            // isolates prefix + retain filter correctness with no
            // cross-library float noise, compared against brute force over
            // the same length-restricted right range.
            let expected = (left + 1..right_end)
                .filter(|&right| {
                    PreparedNameQuery::new(&atoms[left].name_norm)
                        .score_percent(&atoms[right].name_norm, threshold)
                        .is_some()
                })
                .collect::<Vec<_>>();
            assert_eq!(
                actual,
                expected,
                "left={left}, threshold={threshold}, atoms={:?}",
                atoms
                    .iter()
                    .map(|atom| atom.name_norm.as_str())
                    .collect::<Vec<_>>()
            );
        }
    }
}

#[test]
fn canonical_name_values_collapse_identical_names_across_chains() {
    let atoms = vec![
        NameAtom {
            chain_index: 0,
            name_norm: "azuki".into(),
            char_len: 5,
            contract_count: 2,
            nft_count: 20,
        },
        NameAtom {
            chain_index: 1,
            name_norm: "azuki".into(),
            char_len: 5,
            contract_count: 3,
            nft_count: 30,
        },
        NameAtom {
            chain_index: 1,
            name_norm: "azukis".into(),
            char_len: 6,
            contract_count: 1,
            nft_count: 10,
        },
    ];

    let canonical = canonical_name_values(&atoms);

    assert_eq!(canonical.atoms.len(), 2);
    assert_eq!(canonical.members, vec![vec![0, 1], vec![2]]);
    assert_eq!(canonical.atoms[0].chain_index, 0);
    assert_eq!(canonical.atoms[0].name_norm, "azuki");
    assert_eq!(canonical.atoms[0].contract_count, 5);
    assert_eq!(canonical.atoms[0].nft_count, 50);
}

#[test]
fn canonical_name_scoring_expands_matches_to_original_atoms() {
    let atoms = vec![
        NameAtom {
            chain_index: 0,
            name_norm: "azuki".into(),
            char_len: 5,
            contract_count: 1,
            nft_count: 1,
        },
        NameAtom {
            chain_index: 1,
            name_norm: "azuki".into(),
            char_len: 5,
            contract_count: 1,
            nft_count: 1,
        },
        NameAtom {
            chain_index: 1,
            name_norm: "azukii".into(),
            char_len: 6,
            contract_count: 1,
            nft_count: 1,
        },
    ];
    let canonical = canonical_name_values(&atoms);
    let index = NameCandidateIndex::new(&canonical.atoms);
    let mut states = [ThresholdUnionState {
        threshold: 80.0,
        intra: UnionFind::new(atoms.len()),
        cross: Some(SparseUnionFind::default()),
        chain_matrix: Some(new_chain_matrix_reuse_states(1)),
    }];

    let progress = ProgressTracker::for_pipeline_stage(PipelineStage::Name, true);
    progress.start_stage("canonical name scoring", 0);
    progress.add_work(canonical.atoms.len().saturating_sub(1) as u64);
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap();
    let stats = pool.install(|| {
        union_canonical_name_pairs(
            &atoms,
            &canonical,
            &index,
            NameScratchMode::Dense,
            &mut states[0],
            2,
            &progress,
        )
    });

    assert_eq!(stats.candidate_pairs, 1);
    assert_eq!(stats.scored_pairs, 1);
    assert_eq!(stats.matched_pairs, 1);
    let ProgressTracker::Enabled { stage, .. } = &progress else {
        panic!("progress must be enabled");
    };
    assert_eq!(stage.position(), 1);
    assert_eq!(states[0].intra.find(1), states[0].intra.find(2));
    assert!(states[0].cross.as_mut().unwrap().connected(0, 1));
    assert!(states[0].cross.as_mut().unwrap().connected(0, 2));
}

#[test]
fn canonical_name_progress_and_stats_include_unmatched_candidates() {
    let atoms = vec![
        NameAtom {
            chain_index: 0,
            name_norm: "abcd".into(),
            char_len: 4,
            contract_count: 1,
            nft_count: 1,
        },
        NameAtom {
            chain_index: 0,
            name_norm: "abdc".into(),
            char_len: 4,
            contract_count: 1,
            nft_count: 1,
        },
    ];
    let canonical = canonical_name_values(&atoms);
    let index = NameCandidateIndex::new(&canonical.atoms);
    let mut state = ThresholdUnionState {
        threshold: 95.0,
        intra: UnionFind::new(atoms.len()),
        cross: None,
        chain_matrix: None,
    };
    let progress = ProgressTracker::for_pipeline_stage(PipelineStage::Name, true);
    progress.start_stage("canonical name scoring", 0);
    progress.add_work(1);
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap();

    let stats = pool.install(|| {
        union_canonical_name_pairs(
            &atoms,
            &canonical,
            &index,
            NameScratchMode::Dense,
            &mut state,
            1,
            &progress,
        )
    });

    assert_eq!(stats.candidate_pairs, 1);
    assert_eq!(stats.scored_pairs, 1);
    assert_eq!(stats.matched_pairs, 0);
    let ProgressTracker::Enabled { stage, .. } = &progress else {
        panic!("progress must be enabled");
    };
    assert_eq!(stage.position(), 1);
    assert_ne!(state.intra.find(0), state.intra.find(1));
}

#[test]
fn name_candidate_scratch_dense_and_sparse_backends_agree() {
    let mut atoms = [
        "azuki",
        "azukii",
        "azkui",
        "aaaaba",
        "金色dragon",
        "金色dragons",
    ]
    .into_iter()
    .enumerate()
    .map(|(chain_index, name)| NameAtom {
        chain_index: chain_index % 2,
        name_norm: name.to_string(),
        char_len: name.chars().count(),
        contract_count: 1,
        nft_count: 1,
    })
    .collect::<Vec<_>>();
    atoms.sort_by(|left, right| {
        left.char_len
            .cmp(&right.char_len)
            .then_with(|| left.name_norm.cmp(&right.name_norm))
    });
    let candidate_index = NameCandidateIndex::new(&atoms);

    for threshold in [50.0, 70.0, 85.0, 95.0] {
        for left in 0..atoms.len().saturating_sub(1) {
            let right_range = left + 1..atoms.len();
            let mut dense = NameCandidateScratch::with_mode(atoms.len(), NameScratchMode::Dense);
            let dense_cands = candidate_index
                .candidates_for_left(&atoms, left, right_range.clone(), threshold, &mut dense)
                .to_vec();
            let mut sparse = NameCandidateScratch::with_mode(atoms.len(), NameScratchMode::Sparse);
            let sparse_cands = candidate_index
                .candidates_for_left(&atoms, left, right_range, threshold, &mut sparse)
                .to_vec();
            assert_eq!(
                dense_cands, sparse_cands,
                "left={left}, threshold={threshold}"
            );
        }
    }
}
