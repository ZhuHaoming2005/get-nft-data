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
fn name_candidate_index_uses_exact_contiguous_csr_storage() {
    let atoms = vec![NameAtom {
        chain_index: 0,
        name_norm: "a".into(),
        char_len: 1,
        contract_count: 1,
        nft_count: 1,
    }];
    let candidate_index = NameCandidateIndex::new(&atoms);
    let (document_offsets, prefix_tokens, sorted_tokens, posting_offsets, posting_atoms) =
        candidate_index
            .resident_parts()
            .expect("test index must be resident");
    let exact_structural_bytes = std::mem::size_of_val(document_offsets)
        + std::mem::size_of_val(prefix_tokens)
        + std::mem::size_of_val(sorted_tokens)
        + std::mem::size_of_val(posting_offsets)
        + std::mem::size_of_val(posting_atoms);

    assert_eq!(candidate_index.memory_bytes(), exact_structural_bytes);
    assert_eq!(document_offsets.len(), atoms.len() + 1);
    assert_eq!(prefix_tokens.len(), sorted_tokens.len());
    assert_eq!(
        posting_offsets.last().copied(),
        Some(posting_atoms.len() as u64)
    );
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
    let preflight = estimate_name_candidate_index_bytes(&atoms);
    let plan = NameCandidateIndex::prepare_with_progress(&atoms, || {}).unwrap();
    let refined = plan.estimate();
    let actual = plan.build_with_progress(|| {}).unwrap().memory_bytes();

    assert!(preflight.resident_bytes >= refined.resident_bytes);
    assert!(preflight.peak_build_bytes >= refined.peak_build_bytes);
    assert!(refined.resident_bytes >= actual);
    assert!(refined.peak_build_bytes >= refined.resident_bytes);
}

#[test]
fn name_candidate_index_build_plan_reuses_tokens_and_reports_both_passes() {
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

    let plan = NameCandidateIndex::prepare_with_progress(&atoms, || {
        completed.fetch_add(1, Ordering::Relaxed);
    })
    .unwrap();
    assert_eq!(completed.load(Ordering::Relaxed), atoms.len() as u64);

    let estimate = plan.estimate();
    let index = plan
        .build_with_progress(|| {
            completed.fetch_add(1, Ordering::Relaxed);
        })
        .unwrap();

    assert_eq!(index.len(), atoms.len());
    assert_eq!(completed.load(Ordering::Relaxed), 2 * atoms.len() as u64);
    assert!(estimate.resident_bytes >= index.memory_bytes());

    let wrapper_completed = AtomicU64::new(0);
    NameCandidateIndex::new_with_progress(&atoms, || {
        wrapper_completed.fetch_add(1, Ordering::Relaxed);
    })
    .unwrap();
    assert_eq!(
        wrapper_completed.load(Ordering::Relaxed),
        2 * atoms.len() as u64
    );
}

#[test]
fn name_candidate_index_preflight_reuses_char_lengths_without_utf8_inflation() {
    let unicode = vec![NameAtom {
        chain_index: 0,
        name_norm: "金色dragon".into(),
        char_len: "金色dragon".chars().count(),
        contract_count: 1,
        nft_count: 1,
    }];
    let ascii = vec![NameAtom {
        chain_index: 0,
        name_norm: "abdragon".into(),
        char_len: "abdragon".chars().count(),
        contract_count: 1,
        nft_count: 1,
    }];

    let preflight = estimate_name_candidate_index_bytes(&unicode);
    assert_eq!(
        preflight.resident_bytes,
        estimate_name_candidate_index_bytes(&ascii).resident_bytes
    );
    assert_eq!(
        preflight.peak_build_bytes,
        estimate_name_candidate_index_bytes(&ascii).peak_build_bytes
    );
    let plan = NameCandidateIndex::prepare_with_progress(&unicode, || {}).unwrap();
    let refined = plan.estimate();
    let index = plan.build_with_progress(|| {}).unwrap();

    assert!(preflight.resident_bytes >= refined.resident_bytes);
    assert!(preflight.peak_build_bytes >= refined.peak_build_bytes);
    assert!(preflight.resident_bytes >= index.memory_bytes());
}

#[test]
fn external_name_candidate_index_matches_resident_and_cleans_spill() {
    use std::sync::atomic::{AtomicU64, Ordering};

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
        name_norm: name.into(),
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
    let resident = NameCandidateIndex::new(&atoms);
    let temp = tempfile::tempdir().unwrap();
    let completed = AtomicU64::new(0);
    let external = NameCandidateIndex::build_external_with_progress(
        &atoms,
        temp.path(),
        4,
        64 * 1024 * 1024,
        || {
            completed.fetch_add(1, Ordering::Relaxed);
        },
    )
    .unwrap();

    assert!(external.is_external());
    assert_eq!(external.len(), atoms.len());
    assert_eq!(completed.load(Ordering::Relaxed), 2 * atoms.len() as u64);
    assert_eq!(
        external.backing_bytes(),
        atoms.iter().map(|atom| atom.char_len as u64).sum::<u64>()
            * EXTERNAL_POSTING_RECORD_BYTES as u64
    );
    for threshold in [50.0, 70.0, 90.0, 100.0] {
        let mut resident_scratch =
            NameCandidateScratch::with_mode(atoms.len(), NameScratchMode::Dense);
        let mut external_scratch =
            NameCandidateScratch::with_mode(atoms.len(), NameScratchMode::ExternalMerge);
        for left in 0..atoms.len().saturating_sub(1) {
            let right_end = right_name_range_end_for_left(&atoms, left, threshold);
            assert_eq!(
                scored_rights_for_left(
                    &atoms,
                    &external,
                    left,
                    left + 1..right_end,
                    threshold,
                    &mut external_scratch,
                ),
                scored_rights_for_left(
                    &atoms,
                    &resident,
                    left,
                    left + 1..right_end,
                    threshold,
                    &mut resident_scratch,
                ),
                "left={left}, threshold={threshold}",
            );
        }
    }

    let spill_parent = temp.path().join("name-candidate-index");
    assert_eq!(fs::read_dir(&spill_parent).unwrap().count(), 1);
    drop(external);
    assert_eq!(fs::read_dir(spill_parent).unwrap().count(), 0);
}

#[test]
fn external_name_candidate_index_merges_multiple_sorted_runs() {
    let atoms = (0usize..320)
        .map(|atom_index| {
            let name = format!(
                "{:04x}{:04x}{:04x}{:04x}{:04x}",
                atom_index,
                atom_index.wrapping_mul(3),
                atom_index.wrapping_mul(5),
                atom_index.wrapping_mul(7),
                atom_index.wrapping_mul(11),
            );
            NameAtom {
                chain_index: atom_index % 2,
                char_len: name.chars().count(),
                name_norm: name.into(),
                contract_count: 1,
                nft_count: 1,
            }
        })
        .collect::<Vec<_>>();
    assert!(
        atoms.iter().map(|atom| atom.char_len).sum::<usize>() > EXTERNAL_INDEX_MIN_RECORDS_PER_RUN
    );
    let temp = tempfile::tempdir().unwrap();
    let external = NameCandidateIndex::build_external_with_progress(
        &atoms,
        temp.path(),
        1,
        EXTERNAL_UNICODE_COUNTER_BYTES,
        || {},
    )
    .unwrap();
    assert!(external.external_records_are_sorted());
}

#[test]
fn external_name_posting_merge_bounds_fan_in_across_passes() {
    let temp = tempfile::tempdir().unwrap();
    let mut runs = Vec::new();
    for run_index in 0u32..130 {
        let path = temp.path().join(format!("run-{run_index}.bin"));
        let token_key = u64::from(129 - run_index);
        let mut bytes = token_key.to_le_bytes().to_vec();
        bytes.extend_from_slice(&run_index.to_le_bytes());
        fs::write(&path, bytes).unwrap();
        runs.push(path);
    }
    let output = temp.path().join("postings.bin");

    merge_external_posting_runs(&runs, &output).unwrap();

    let bytes = fs::read(output).unwrap();
    assert_eq!(bytes.len(), 130 * EXTERNAL_POSTING_RECORD_BYTES);
    let keys = bytes
        .chunks_exact(EXTERNAL_POSTING_RECORD_BYTES)
        .map(|record| u64::from_le_bytes(record[..8].try_into().unwrap()))
        .collect::<Vec<_>>();
    assert!(keys.windows(2).all(|pair| pair[0] <= pair[1]));
    assert!(runs.iter().all(|path| !path.exists()));
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

    let estimate = estimate_name_atom_load(&conn).unwrap();
    let total = count_all_name_atoms(&conn).unwrap();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .unwrap();
    let atoms = load_all_name_atoms(
        &conn,
        NameAtomLoadSpec {
            chains: &chains,
            pool: &pool,
            expected_rows: total as usize,
            expected_string_arena_bytes: 0,
            string_storage_mode: NameStringStorageMode::Resident,
            atom_storage_mode: NameAtomStorageMode::Resident,
            scratch_directory: Path::new("."),
        },
        |delta| {
            scanned += delta;
        },
    )
    .unwrap();

    assert_eq!(total, 3);
    assert_eq!(estimate.string_arena_bytes, 5 + 4 + 5 + 3 * 4);
    assert_eq!(scanned, total);
    assert_eq!(atoms.len() as u64, total);
}

#[test]
fn name_atom_loader_rejects_unselected_chain_rows() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE name_atoms(
             chain VARCHAR,
             name_norm VARCHAR,
             contract_count BIGINT,
             nft_count BIGINT
         );
         INSERT INTO name_atoms VALUES ('polygon', 'alpha', 1, 2);",
    )
    .unwrap();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .build()
        .unwrap();

    let error = load_all_name_atoms(
        &conn,
        NameAtomLoadSpec {
            chains: &["ethereum".to_string()],
            pool: &pool,
            expected_rows: 1,
            expected_string_arena_bytes: 0,
            string_storage_mode: NameStringStorageMode::Resident,
            atom_storage_mode: NameAtomStorageMode::Resident,
            scratch_directory: Path::new("."),
        },
        |_| {},
    )
    .unwrap_err();

    assert!(error
        .to_string()
        .contains("name atom references unselected chain"));
}

#[cfg(target_pointer_width = "64")]
#[test]
fn name_atom_loader_rejects_u32_identity_overflow_before_allocating() {
    let conn = Connection::open_in_memory().unwrap();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .build()
        .unwrap();
    let error = load_all_name_atoms(
        &conn,
        NameAtomLoadSpec {
            chains: &[],
            pool: &pool,
            expected_rows: u32::MAX as usize + 1,
            expected_string_arena_bytes: 0,
            string_storage_mode: NameStringStorageMode::Resident,
            atom_storage_mode: NameAtomStorageMode::Resident,
            scratch_directory: Path::new("."),
        },
        |_| {},
    )
    .unwrap_err();

    assert!(error
        .to_string()
        .contains("name atom count exceeds compact u32 indexes"));
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
        name_norm: "reserved-name".into(),
        char_len: 13,
        contract_count: 1,
        nft_count: 1,
    });

    let expected = atoms.capacity() * std::mem::size_of::<NameAtom>()
        + atoms[0].name_norm.len()
        + 2 * std::mem::size_of::<usize>();

    assert_eq!(name_atoms_memory_bytes(&atoms), expected);
    assert!(canonical_name_build_peak_bytes(&atoms) > expected);
}

#[test]
fn name_candidates_never_escape_the_requested_right_range() {
    let atoms = ["aaaaa", "aaaab", "aaaba", "aabaa", "abaaa"]
        .into_iter()
        .map(|name| NameAtom {
            chain_index: 0,
            name_norm: name.into(),
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
fn name_scratch_plan_reduces_dense_workers_before_considering_larger_sparse_scratch() {
    let atom_count = 1_000_000usize;
    let threads = 96;
    let full = name_scratch_plan(atom_count, threads, usize::MAX);
    let reduced = name_scratch_plan(atom_count, threads, full.reserved_bytes - 1);

    assert_eq!(full.mode, NameScratchMode::Dense);
    assert_eq!(full.requested_workers, threads);
    assert_eq!(full.admitted_workers, threads);
    assert_eq!(reduced.mode, NameScratchMode::Dense);
    assert_eq!(reduced.requested_workers, threads);
    assert!(reduced.admitted_workers < threads);
    assert!(reduced.admitted_workers >= 1);
    assert!(reduced.reserved_bytes < full.reserved_bytes);
}

#[test]
fn name_scratch_plan_prefers_budgeted_dense_mode_above_the_old_atom_threshold() {
    let atom_count = (1 << 20) + 1;
    let plan = name_scratch_plan(atom_count, 2, usize::MAX);

    assert_eq!(plan.mode, NameScratchMode::Dense);
    assert_eq!(plan.admitted_workers, 2);
}

#[test]
fn name_scratch_plan_reserves_the_candidate_vectors_actual_worst_capacity() {
    let atom_count = 5usize;
    let workers = 1usize;
    let candidate_capacity = atom_count.max(4).next_power_of_two();
    let expected = candidate_capacity * std::mem::size_of::<NameAtomIndex>() * workers
        + atom_count * std::mem::size_of::<u16>() * workers;

    let plan = name_scratch_plan(atom_count, workers, usize::MAX);

    assert_eq!(plan.mode, NameScratchMode::Dense);
    assert_eq!(plan.admitted_workers, 1);
    assert_eq!(plan.worker_stack_bytes, 0);
    assert_eq!(plan.scratch_and_queue_bytes, expected);
    assert_eq!(plan.reserved_bytes, expected);
}

#[test]
fn name_scratch_plan_uses_zero_linear_scratch_scan_below_the_single_dense_lane_boundary() {
    let atom_count = 1_000_000usize;
    let one_dense_lane = name_scratch_plan(atom_count, 1, usize::MAX);
    let dense_boundary = name_scratch_plan(atom_count, 128, one_dense_lane.reserved_bytes);
    let scan = name_scratch_plan(
        atom_count,
        128,
        one_dense_lane.reserved_bytes.saturating_sub(1),
    );

    assert_eq!(dense_boundary.mode, NameScratchMode::Dense);
    assert_eq!(dense_boundary.admitted_workers, 1);
    assert_eq!(scan.mode, NameScratchMode::Scan);
    assert!(scan.admitted_workers >= 1);
    assert!(scan.reserved_bytes < one_dense_lane.reserved_bytes);
}

#[test]
fn external_name_scratch_is_bounded_by_name_length_instead_of_atom_count() {
    let atom_count = 100_000_000usize;
    let external = name_scratch_plan_for_profile(
        NameCandidateScratchProfile::External {
            atom_count,
            max_name_char_len: 64,
        },
        128,
        usize::MAX,
    );
    let resident = name_scratch_plan(atom_count, 128, usize::MAX);

    assert_eq!(external.mode, NameScratchMode::ExternalMerge);
    assert_eq!(external.admitted_workers, 128);
    assert!(external.reserved_bytes < resident.reserved_bytes);
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
fn monotone_right_range_index_matches_every_binary_endpoint() {
    let atoms = (0usize..=64)
        .flat_map(|char_len| {
            (0..3).map(move |chain_index| NameAtom {
                chain_index,
                name_norm: "x".repeat(char_len).into(),
                char_len,
                contract_count: 1,
                nft_count: 1,
            })
        })
        .collect::<Vec<_>>();

    for threshold in [0.0, 50.0, 70.0, 85.0, 95.0, 100.0, 101.0] {
        let ends = build_right_name_range_ends(&atoms, threshold);
        assert_eq!(ends.len(), atoms.len() - 1);
        assert_eq!(
            std::mem::size_of_val(ends.as_ref()),
            right_name_range_index_bytes(atoms.len())
        );
        assert!(ends.windows(2).all(|pair| pair[0] <= pair[1]));
        for left in 0..atoms.len() - 1 {
            assert_eq!(
                ends[left] as usize,
                right_name_range_end_for_left(&atoms, left, threshold),
                "left={left}, threshold={threshold}"
            );
        }
    }
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
        name_norm: name.into(),
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
        name_norm: name.into(),
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
                    name_norm: name.clone().into(),
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
                    .map(|atom| atom.name_norm.as_ref())
                    .collect::<Vec<_>>()
            );
        }
    }
}

#[test]
fn canonical_name_values_collapse_identical_names_across_chains() {
    let mut atoms = vec![
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

    let canonical = canonical_name_values(&mut atoms).unwrap();

    assert_eq!(canonical.atoms.len(), 2);
    assert_eq!(canonical.members[0].as_ref(), &[0, 1]);
    assert_eq!(canonical.members[1].as_ref(), &[2]);
    assert_eq!(
        canonical.members.memory_bytes(),
        (canonical.atoms.len() + 1 + atoms.len()) * std::mem::size_of::<u32>()
    );
    assert_eq!(canonical.atoms[0].name_norm.as_ref(), "azuki");
    assert_eq!(
        canonical.members[0]
            .iter()
            .map(|&index| atoms[index as usize].contract_count)
            .sum::<i64>(),
        5
    );
    assert_eq!(
        canonical.members[0]
            .iter()
            .map(|&index| atoms[index as usize].nft_count)
            .sum::<i64>(),
        50
    );
    assert!(std::sync::Arc::ptr_eq(
        &atoms[0].name_norm,
        &atoms[1].name_norm
    ));
    assert!(std::sync::Arc::ptr_eq(
        &atoms[0].name_norm,
        &canonical.atoms[0].name_norm
    ));
    assert!(
        name_atom_sets_memory_bytes(&atoms, &canonical.atoms)
            < name_atoms_memory_bytes(&atoms) + name_atoms_memory_bytes(&canonical.atoms)
    );
}

#[test]
fn canonical_name_scoring_expands_matches_to_original_atoms() {
    let mut atoms = vec![
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
    let canonical = canonical_name_values(&mut atoms).unwrap();
    let index = NameCandidateIndex::new(&canonical.atoms);
    let mut states = [ThresholdUnionState {
        threshold: 80.0,
        intra: UnionFind::new(atoms.len()),
        cross: Some(CrossUnionState::Sparse(SparseUnionFind::default())),
        chain_matrix: Some(ChainMatrixState::Resident(new_chain_matrix_reuse_states(1))),
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
            NameScoringExecution {
                scratch_mode: NameScratchMode::Dense,
                worker_count: 4,
                right_range_ends: None,
            },
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
    let mut atoms = vec![
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
    let canonical = canonical_name_values(&mut atoms).unwrap();
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
            NameScoringExecution {
                scratch_mode: NameScratchMode::Dense,
                worker_count: 4,
                right_range_ends: None,
            },
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
        name_norm: name.into(),
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

#[test]
fn low_memory_scan_scoring_matches_dense_candidate_dedup() {
    let mut atoms = [
        (0, "azuki"),
        (1, "azukii"),
        (0, "azkui"),
        (1, "aaaaba"),
        (0, "金色dragon"),
        (1, "金色dragons"),
    ]
    .into_iter()
    .map(|(chain_index, name)| NameAtom {
        chain_index,
        name_norm: name.into(),
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
    let canonical = canonical_name_values(&mut atoms).unwrap();
    let candidate_index = NameCandidateIndex::new(&canonical.atoms);
    let right_range_ends = build_right_name_range_ends(&canonical.atoms, 70.0);
    let build_state = || ThresholdUnionState {
        threshold: 70.0,
        intra: UnionFind::new(atoms.len()),
        cross: Some(CrossUnionState::Sparse(SparseUnionFind::default())),
        chain_matrix: Some(ChainMatrixState::Resident(new_chain_matrix_reuse_states(1))),
    };
    let progress = ProgressTracker::for_pipeline_stage(PipelineStage::Name, false);
    let mut dense_state = build_state();
    let dense_stats = union_canonical_name_pairs(
        &atoms,
        &canonical,
        &candidate_index,
        NameScoringExecution {
            scratch_mode: NameScratchMode::Dense,
            worker_count: 1,
            right_range_ends: Some(&right_range_ends),
        },
        &mut dense_state,
        2,
        &progress,
    );
    let mut scan_state = build_state();
    let scan_stats = union_canonical_name_pairs(
        &atoms,
        &canonical,
        &candidate_index,
        NameScoringExecution {
            scratch_mode: NameScratchMode::Scan,
            worker_count: 1,
            right_range_ends: None,
        },
        &mut scan_state,
        2,
        &progress,
    );

    assert_eq!(scan_stats, dense_stats);
    for left in 0..atoms.len() {
        for right in left + 1..atoms.len() {
            assert_eq!(
                scan_state.intra.find(left) == scan_state.intra.find(right),
                dense_state.intra.find(left) == dense_state.intra.find(right),
                "intra mismatch for ({left}, {right})"
            );
            assert_eq!(
                scan_state.cross.as_mut().unwrap().connected(left, right),
                dense_state.cross.as_mut().unwrap().connected(left, right),
                "cross mismatch for ({left}, {right})"
            );
            let scan_connected = match scan_state.chain_matrix.as_mut().unwrap() {
                ChainMatrixState::Resident(matrix) => matrix[0].connected(left, right),
                ChainMatrixState::Spill(_) => panic!("test state must remain resident"),
            };
            let dense_connected = match dense_state.chain_matrix.as_mut().unwrap() {
                ChainMatrixState::Resident(matrix) => matrix[0].connected(left, right),
                ChainMatrixState::Spill(_) => panic!("test state must remain resident"),
            };
            assert_eq!(
                scan_connected, dense_connected,
                "matrix mismatch for ({left}, {right})"
            );
        }
    }
}

fn resident_name_union_state(atom_count: usize, chain_count: usize) -> ThresholdUnionState {
    ThresholdUnionState {
        threshold: 70.0,
        intra: UnionFind::new(atom_count),
        cross: Some(CrossUnionState::Dense(UnionFind::new(atom_count))),
        chain_matrix: (chain_count > 1).then(|| {
            ChainMatrixState::Resident(new_chain_matrix_reuse_states(chain_pair_count(chain_count)))
        }),
    }
}

fn apply_full_member_cartesian_reference(
    atoms: &[NameAtom],
    canonical: &CanonicalNameValues,
    state: &mut ThresholdUnionState,
    chain_count: usize,
) {
    for members in &canonical.members {
        for (left_position, &left) in members.iter().enumerate() {
            for &right in &members[left_position + 1..] {
                apply_matching_name_pairs(
                    atoms,
                    state,
                    left as usize,
                    &[ScoredRight {
                        right: right as usize,
                        score: 100.0,
                    }],
                    chain_count,
                );
            }
        }
    }
    for left in 0..canonical.atoms.len() {
        let query = PreparedNameQuery::new(&canonical.atoms[left].name_norm);
        for right in left + 1..canonical.atoms.len() {
            let Some(score) =
                query.score_percent(&canonical.atoms[right].name_norm, state.threshold)
            else {
                continue;
            };
            for &original_left in &canonical.members[left] {
                for &original_right in &canonical.members[right] {
                    apply_matching_name_pairs(
                        atoms,
                        state,
                        original_left as usize,
                        &[ScoredRight {
                            right: original_right as usize,
                            score,
                        }],
                        chain_count,
                    );
                }
            }
        }
    }
}

fn assert_resident_name_states_equivalent(
    atoms: &[NameAtom],
    chain_count: usize,
    actual: &mut ThresholdUnionState,
    expected: &mut ThresholdUnionState,
) {
    for left in 0..atoms.len() {
        for right in left + 1..atoms.len() {
            assert_eq!(
                actual.intra.find(left) == actual.intra.find(right),
                expected.intra.find(left) == expected.intra.find(right),
                "intra mismatch for ({left}, {right})",
            );
            assert_eq!(
                actual.cross.as_mut().unwrap().connected(left, right),
                expected.cross.as_mut().unwrap().connected(left, right),
                "global cross mismatch for ({left}, {right})",
            );
        }
    }
    let ChainMatrixState::Resident(actual_matrix) =
        actual.chain_matrix.as_mut().expect("actual matrix")
    else {
        panic!("actual matrix must be resident");
    };
    let ChainMatrixState::Resident(expected_matrix) =
        expected.chain_matrix.as_mut().expect("expected matrix")
    else {
        panic!("expected matrix must be resident");
    };
    assert_eq!(actual_matrix.len(), chain_pair_count(chain_count));
    for pair_index in 0..actual_matrix.len() {
        for left in 0..atoms.len() {
            for right in left + 1..atoms.len() {
                assert_eq!(
                    actual_matrix[pair_index].connected(left, right),
                    expected_matrix[pair_index].connected(left, right),
                    "matrix {pair_index} mismatch for ({left}, {right})",
                );
            }
        }
    }
}

#[test]
fn identical_member_cliques_use_scope_equivalent_spanning_unions() {
    let chain_counts = [100usize, 120, 80];
    let mut atoms = chain_counts
        .iter()
        .enumerate()
        .flat_map(|(chain_index, &count)| {
            (0..count).map(move |_| NameAtom {
                chain_index,
                name_norm: "same-name".into(),
                char_len: 9,
                contract_count: 1,
                nft_count: 1,
            })
        })
        .collect::<Vec<_>>();
    let canonical = canonical_name_values(&mut atoms).unwrap();
    let index = NameCandidateIndex::new(&canonical.atoms);
    let mut actual = resident_name_union_state(atoms.len(), chain_counts.len());
    let mut expected = resident_name_union_state(atoms.len(), chain_counts.len());
    let progress = ProgressTracker::for_pipeline_stage(PipelineStage::Name, false);

    let stats = union_canonical_name_pairs(
        &atoms,
        &canonical,
        &index,
        NameScoringExecution {
            scratch_mode: NameScratchMode::Dense,
            worker_count: 1,
            right_range_ends: None,
        },
        &mut actual,
        chain_counts.len(),
        &progress,
    );
    apply_full_member_cartesian_reference(&atoms, &canonical, &mut expected, chain_counts.len());

    assert_resident_name_states_equivalent(&atoms, chain_counts.len(), &mut actual, &mut expected);
    let member_count = atoms.len() as u64;
    assert_eq!(
        stats.logical_member_pairs,
        member_count * (member_count - 1) / 2
    );
    let intra_edges = chain_counts.iter().map(|count| count - 1).sum::<usize>();
    let cross_edges = atoms.len() - 1;
    let matrix_edges = (chain_counts[0] + chain_counts[1] - 1)
        + (chain_counts[0] + chain_counts[2] - 1)
        + (chain_counts[1] + chain_counts[2] - 1);
    assert_eq!(
        stats.spanning_union_operations,
        (intra_edges + cross_edges + matrix_edges) as u64,
    );
    assert!(stats.spanning_union_operations * 10 < stats.logical_member_pairs);
}

#[test]
fn spanning_expansion_matches_full_cartesian_reference_on_random_groups() {
    let mut random = 0x517c_c1b7_2722_0a95u64;
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap();
    for _ in 0..24 {
        let names = ["aaaaa", "aaaab", "aaaba"];
        let chain_count = 3usize;
        let mut atoms = Vec::new();
        for &name in &names {
            let mut group_members = 0usize;
            for chain_index in 0..chain_count {
                let count = (xorshift(&mut random) % 4) as usize;
                group_members += count;
                for _ in 0..count {
                    atoms.push(NameAtom {
                        chain_index,
                        name_norm: name.into(),
                        char_len: name.chars().count(),
                        contract_count: 1,
                        nft_count: 1,
                    });
                }
            }
            if group_members == 0 {
                atoms.push(NameAtom {
                    chain_index: (xorshift(&mut random) % chain_count as u64) as usize,
                    name_norm: name.into(),
                    char_len: name.chars().count(),
                    contract_count: 1,
                    nft_count: 1,
                });
            }
        }
        atoms.sort_by(|left, right| {
            left.char_len
                .cmp(&right.char_len)
                .then_with(|| left.chain_index.cmp(&right.chain_index))
                .then_with(|| left.name_norm.cmp(&right.name_norm))
        });
        let canonical = canonical_name_values(&mut atoms).unwrap();
        let index = NameCandidateIndex::new(&canonical.atoms);
        let mut actual = resident_name_union_state(atoms.len(), chain_count);
        let mut expected = resident_name_union_state(atoms.len(), chain_count);
        let progress = ProgressTracker::for_pipeline_stage(PipelineStage::Name, false);

        let stats = pool.install(|| {
            union_canonical_name_pairs(
                &atoms,
                &canonical,
                &index,
                NameScoringExecution {
                    scratch_mode: NameScratchMode::Dense,
                    worker_count: 4,
                    right_range_ends: None,
                },
                &mut actual,
                chain_count,
                &progress,
            )
        });
        apply_full_member_cartesian_reference(&atoms, &canonical, &mut expected, chain_count);

        assert_resident_name_states_equivalent(&atoms, chain_count, &mut actual, &mut expected);
        let exact_pairs = canonical
            .members
            .iter()
            .map(|members| {
                (members.len() as u64).saturating_mul(members.len().saturating_sub(1) as u64) / 2
            })
            .sum::<u64>();
        let mut fuzzy_pairs = 0u64;
        for left in 0..canonical.atoms.len() {
            let query = PreparedNameQuery::new(&canonical.atoms[left].name_norm);
            for right in left + 1..canonical.atoms.len() {
                if query
                    .score_percent(&canonical.atoms[right].name_norm, 70.0)
                    .is_some()
                {
                    fuzzy_pairs = fuzzy_pairs.saturating_add(
                        (canonical.members[left].len() as u64)
                            .saturating_mul(canonical.members[right].len() as u64),
                    );
                }
            }
        }
        assert_eq!(stats.logical_member_pairs, exact_pairs + fuzzy_pairs);
        assert!(stats.spanning_union_operations <= 3 * stats.logical_member_pairs);
    }
}

#[test]
fn cross_bipartite_spanning_forest_matches_all_allowed_edges() {
    let mut random = 0xa076_1d64_78bd_642fu64;
    for _ in 0..128 {
        let left_len = 1 + (xorshift(&mut random) % 8) as usize;
        let right_len = 1 + (xorshift(&mut random) % 8) as usize;
        let atoms = (0..left_len + right_len)
            .map(|_| NameAtom {
                chain_index: (xorshift(&mut random) % 4) as usize,
                name_norm: "x".into(),
                char_len: 1,
                contract_count: 1,
                nft_count: 1,
            })
            .collect::<Vec<_>>();
        // The direct cross helper does not require chain-group ordering.
        let left_members = (0..left_len).map(|index| index as u32).collect::<Vec<_>>();
        let right_members = (left_len..atoms.len())
            .map(|index| index as u32)
            .collect::<Vec<_>>();
        let mut actual = ThresholdUnionState {
            threshold: 0.0,
            intra: UnionFind::new(atoms.len()),
            cross: Some(CrossUnionState::Dense(UnionFind::new(atoms.len()))),
            chain_matrix: None,
        };
        let mut expected = ThresholdUnionState {
            threshold: 0.0,
            intra: UnionFind::new(atoms.len()),
            cross: Some(CrossUnionState::Dense(UnionFind::new(atoms.len()))),
            chain_matrix: None,
        };

        let operations =
            connect_cross_bipartite_spanning(&atoms, &left_members, &right_members, &mut actual);
        for &left in &left_members {
            for &right in &right_members {
                if atoms[left as usize].chain_index != atoms[right as usize].chain_index {
                    expected
                        .cross
                        .as_mut()
                        .unwrap()
                        .union(left as usize, right as usize);
                }
            }
        }

        for left in 0..atoms.len() {
            for right in left + 1..atoms.len() {
                assert_eq!(
                    actual.cross.as_mut().unwrap().connected(left, right),
                    expected.cross.as_mut().unwrap().connected(left, right),
                    "left colors {:?}, right colors {:?}, pair ({left}, {right})",
                    left_members
                        .iter()
                        .map(|&index| atoms[index as usize].chain_index)
                        .collect::<Vec<_>>(),
                    right_members
                        .iter()
                        .map(|&index| atoms[index as usize].chain_index)
                        .collect::<Vec<_>>(),
                );
            }
        }
        assert!(
            operations <= (left_len + right_len - 1) as u64,
            "operations={operations}, left={:?}, right={:?}",
            left_members
                .iter()
                .map(|&index| atoms[index as usize].chain_index)
                .collect::<Vec<_>>(),
            right_members
                .iter()
                .map(|&index| atoms[index as usize].chain_index)
                .collect::<Vec<_>>(),
        );
    }
}

#[test]
fn spanning_unions_preserve_spill_matrix_and_deferred_global_replay() {
    let mut atoms = Vec::new();
    for &(name, base_count) in &[("aaaaa", 5usize), ("aaaab", 4usize)] {
        for chain_index in 0..3usize {
            for _ in 0..base_count + chain_index {
                atoms.push(NameAtom {
                    chain_index,
                    name_norm: name.into(),
                    char_len: name.chars().count(),
                    contract_count: 1,
                    nft_count: 1,
                });
            }
        }
    }
    atoms.sort_by(|left, right| {
        left.char_len
            .cmp(&right.char_len)
            .then_with(|| left.chain_index.cmp(&right.chain_index))
            .then_with(|| left.name_norm.cmp(&right.name_norm))
    });
    let canonical = canonical_name_values(&mut atoms).unwrap();
    let index = NameCandidateIndex::new(&canonical.atoms);
    let mut expected = resident_name_union_state(atoms.len(), 3);
    apply_full_member_cartesian_reference(&atoms, &canonical, &mut expected, 3);
    let by_chain = atoms_by_chain(&atoms, 3);
    let temp = tempfile::tempdir().unwrap();
    let spill = ChainMatrixSpill::new(
        temp.path().join("name-spanning-spill"),
        chain_pair_atom_capacities(&by_chain),
    )
    .unwrap();
    let mut actual = ThresholdUnionState {
        threshold: 70.0,
        intra: UnionFind::new(atoms.len()),
        cross: Some(CrossUnionState::Deferred),
        chain_matrix: Some(ChainMatrixState::Spill(spill)),
    };
    let progress = ProgressTracker::for_pipeline_stage(PipelineStage::Name, false);

    union_canonical_name_pairs(
        &atoms,
        &canonical,
        &index,
        NameScoringExecution {
            scratch_mode: NameScratchMode::Dense,
            worker_count: 1,
            right_range_ends: None,
        },
        &mut actual,
        3,
        &progress,
    );

    for left in 0..atoms.len() {
        for right in left + 1..atoms.len() {
            assert_eq!(
                actual.intra.find(left) == actual.intra.find(right),
                expected.intra.find(left) == expected.intra.find(right),
            );
        }
    }
    let ChainMatrixState::Spill(mut spill) = actual.chain_matrix.take().expect("spill matrix")
    else {
        panic!("matrix must remain spilled");
    };
    let mut replayed_cross = spill.replay_global_dense(atoms.len(), &by_chain).unwrap();
    for left in 0..atoms.len() {
        for right in left + 1..atoms.len() {
            assert_eq!(
                replayed_cross.find(left) == replayed_cross.find(right),
                expected.cross.as_mut().unwrap().connected(left, right),
                "deferred global replay mismatch for ({left}, {right})",
            );
        }
    }
    let ChainMatrixState::Resident(expected_matrix) =
        expected.chain_matrix.as_mut().expect("expected matrix")
    else {
        panic!("reference matrix must be resident");
    };
    let mut pair_index = 0usize;
    for primary_chain in 0..3 {
        for secondary_chain in primary_chain + 1..3 {
            let primary_atoms = &by_chain[primary_chain];
            let secondary_atoms = &by_chain[secondary_chain];
            let mut replayed = spill.take_pair_union_find(pair_index).unwrap();
            let pair_atoms = primary_atoms
                .iter()
                .chain(secondary_atoms)
                .copied()
                .collect::<Vec<_>>();
            for left_local in 0..pair_atoms.len() {
                for right_local in left_local + 1..pair_atoms.len() {
                    assert_eq!(
                        replayed.find(left_local) == replayed.find(right_local),
                        expected_matrix[pair_index].connected(
                            pair_atoms[left_local] as usize,
                            pair_atoms[right_local] as usize,
                        ),
                        "spill matrix {pair_index} mismatch for locals \
                         ({left_local}, {right_local})",
                    );
                }
            }
            pair_index += 1;
        }
    }
}
