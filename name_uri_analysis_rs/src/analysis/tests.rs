use super::*;

#[test]
fn analysis_connection_uses_persistent_work_database() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("stage.duckdb");

    let connection = open_analysis_connection(&path).unwrap();
    connection
        .execute_batch("CREATE TABLE persisted(value INTEGER);")
        .unwrap();
    drop(connection);

    assert!(path.exists());
}

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

    let hits = score_name_pairs_for_left_chunk(&atoms, 0, 1, atoms.len(), 90.0);

    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].right, 1);
    assert_eq!(hits[0].score, 100.0);
}

#[test]
fn name_progress_counts_scored_candidates_even_when_none_match() {
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
    let candidate_index = NameCandidateIndex::new(&atoms);
    let mut states = [ThresholdUnionState {
        threshold: 95.0,
        intra: UnionFind::new(atoms.len()),
        cross: None,
        chain_matrix: None,
    }];
    let progress = ProgressTracker::new(1, true);
    progress.start_phase("name scoring", 0);
    progress.add_work(1);

    union_full_name_pairs(
        &atoms,
        &candidate_index,
        NameScratchMode::Dense,
        &mut states[0],
        1,
        &progress,
    );

    let ProgressTracker::Enabled { stage, .. } = &progress else {
        panic!("progress must be enabled");
    };
    assert_eq!(stage.position(), 1);
    assert_ne!(states[0].intra.find(0), states[0].intra.find(1));
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
fn name_candidate_postings_are_sliced_to_the_requested_right_range() {
    let source = include_str!("name_scoring.rs");

    assert!(source.contains("posting.partition_point"));
    assert!(source.contains("right_range.start"));
    assert!(source.contains("right_range.end"));
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
    let mut scratch = NameCandidateScratch::new(atoms.len());

    let candidates = index
        .candidates_for_left(&atoms, 0, 2..4, 80.0, &mut scratch)
        .to_vec();

    assert_eq!(candidates, vec![2, 3]);
}

#[test]
fn name_candidate_index_is_budgeted_before_construction() {
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

    assert!(
        estimate.resident_bytes >= actual,
        "estimate={} actual={actual}",
        estimate.resident_bytes
    );
    assert!(estimate.peak_build_bytes >= estimate.resident_bytes);

    let source = include_str!("name.rs");
    let budget = source.find("estimate_name_candidate_index_bytes").unwrap();
    let build = source.find("NameCandidateIndex::new").unwrap();
    assert!(budget < build);
}

#[test]
fn name_scratch_plan_uses_dense_mode_only_when_all_workers_fit() {
    let atom_count = 1_000_000usize;
    let threads = 96;
    let workers = threads.min(atom_count - 1);
    let candidate_capacity = atom_count.max(4).next_power_of_two();
    let common_bytes = candidate_capacity * std::mem::size_of::<NameAtomIndex>() * workers
        + NAME_EDGE_CHUNK_SIZE * std::mem::size_of::<(usize, ScoredRight)>() * workers * 3;
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
        + NAME_EDGE_CHUNK_SIZE * std::mem::size_of::<(usize, ScoredRight)>() * workers * 3
        + atom_count * std::mem::size_of::<u16>() * workers;

    let plan = name_scratch_plan(atom_count, workers, usize::MAX);

    assert_eq!(plan.mode, NameScratchMode::Dense);
    assert_eq!(plan.reserved_bytes, expected);
}

#[test]
fn dense_name_scratch_uses_u16_generations_and_resets_after_wrap() {
    let source = include_str!("name_scoring.rs");
    assert!(source.contains("seen_generation: Vec<u16>"));
    assert!(source.contains("generation: u16"));

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

    let expected = name_pair_score_from_names("金色 dragon", "金色 dragons");
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
fn auto_memory_plan_exposes_full_total_budget_to_rust_batching() {
    let total_budget = 512 * 1024 * 1024;
    let plan = name_analysis_memory_plan("512MB", None, 0).unwrap();

    assert_eq!(plan.analysis_bytes, total_budget);
}

#[test]
fn auto_memory_plan_uses_full_budget_without_duckdb_reservation() {
    let atoms_by_chain = vec![vec![0; 10_000], vec![0; 10_000], vec![0; 10_000]];
    let atom_count = atoms_by_chain.iter().map(Vec::len).sum();
    let global_bytes = threshold_state_bytes(atom_count, atoms_by_chain.len());
    let matrix_bytes = chain_matrix_reuse_state_bytes(&atoms_by_chain);
    let total_budget = global_bytes
        .saturating_add(matrix_bytes)
        .saturating_mul(3)
        .saturating_mul(2);
    let memory_limit = format_byte_size(total_budget);
    let parsed_budget = total_memory_budget_bytes(&memory_limit).unwrap();

    let plan = name_analysis_memory_plan(&memory_limit, None, 0).unwrap();

    assert_eq!(plan.analysis_bytes, parsed_budget);
}

#[test]
fn default_memory_budget_is_fully_available_to_rust_batching() {
    let small = name_analysis_memory_plan("10GB", None, 0).unwrap();
    let large = name_analysis_memory_plan("10GB", None, 0).unwrap();

    assert_eq!(small.analysis_bytes, 10 * 1024 * 1024 * 1024);
    assert_eq!(large.analysis_bytes, 10 * 1024 * 1024 * 1024);
}

#[test]
fn explicit_analysis_memory_limit_stays_inside_total_budget() {
    let plan = name_analysis_memory_plan("10GB", Some("16KB"), 0).unwrap();

    assert_eq!(plan.analysis_bytes, 16 * 1024);
}

#[test]
fn explicit_analysis_memory_limit_rejects_over_budget_value() {
    let error = name_analysis_memory_plan("1GB", Some("2GB"), 0).unwrap_err();

    assert!(error.to_string().contains("exceeds total --memory-limit"));
}

#[test]
fn explicit_analysis_memory_limit_is_a_hard_resident_limit() {
    let error = name_analysis_memory_plan("10GB", Some("16KB"), 32 * 1024).unwrap_err();

    assert!(error.to_string().contains("resident name state"));
    assert!(error.to_string().contains("16384B"));
}

#[test]
fn analysis_memory_auto_uses_total_budget_auto_balance() {
    let default_plan = name_analysis_memory_plan("4GB", None, 0).unwrap();
    let auto_plan = name_analysis_memory_plan("4GB", Some("auto"), 0).unwrap();

    assert_eq!(auto_plan.analysis_bytes, default_plan.analysis_bytes);
}

#[test]
fn metadata_memory_budget_accepts_auto() {
    assert!(metadata::metadata_memory_budget_bytes("auto").unwrap() > 0);
}

#[test]
fn controller_memory_validation_accepts_auto_and_rejects_invalid_static_limits() {
    validate_static_memory_options("auto", Some("auto"), "auto").unwrap();

    let analysis_error =
        validate_static_memory_options("unbounded", Some("1GiB"), "1GiB").unwrap_err();
    assert!(analysis_error
        .to_string()
        .contains("invalid analysis memory limit"));

    let duckdb_error =
        validate_static_memory_options("1GiB", Some("auto"), "automatic").unwrap_err();
    assert!(duckdb_error
        .to_string()
        .contains("invalid analysis memory limit"));
}

#[test]
fn diagnostic_environment_flag_is_explicit() {
    assert!(!diagnostics_requested(None));
    assert!(!diagnostics_requested(Some(std::ffi::OsStr::new("0"))));
    assert!(diagnostics_requested(Some(std::ffi::OsStr::new("1"))));
    assert!(diagnostics_requested(Some(std::ffi::OsStr::new("true"))));
}

#[test]
fn duckdb_configuration_does_not_parse_memory_limit() {
    let conn = Connection::open_in_memory().unwrap();
    let options = AnalysisOptions {
        database_path: PathBuf::from(":memory:"),
        parquet_inputs: Vec::new(),
        output_dir: PathBuf::from("unused"),
        name_threshold: 95.0,
        threads: 1,
        memory_limit: "not-a-size".into(),
        analysis_memory_limit: None,
        duckdb_memory_limit: "1GB".into(),
        temp_directory: None,
        progress: false,
    };

    configure_duckdb(&conn, &options).unwrap();
}

#[test]
fn duckdb_threads_are_capped_at_the_64_physical_core_target() {
    let conn = Connection::open_in_memory().unwrap();
    let options = AnalysisOptions {
        database_path: PathBuf::from(":memory:"),
        parquet_inputs: Vec::new(),
        output_dir: PathBuf::from("unused"),
        name_threshold: 95.0,
        threads: 128,
        memory_limit: "384GiB".into(),
        analysis_memory_limit: Some("384GiB".into()),
        duckdb_memory_limit: "320GiB".into(),
        temp_directory: None,
        progress: false,
    };

    configure_duckdb(&conn, &options).unwrap();

    let threads = conn
        .query_row("SELECT current_setting('threads')::UBIGINT", [], |row| {
            row.get::<_, u64>(0)
        })
        .unwrap();
    assert_eq!(threads, 64);
}

#[test]
fn prepare_only_uri_tables_are_released_before_metadata_compaction() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TEMP TABLE contract_dim(value INTEGER);
         CREATE TEMP TABLE uri_rows(value INTEGER);
         CREATE TEMP TABLE uri_key_contracts(value INTEGER);
         CREATE TEMP TABLE uri_duplicate_key_stats(value INTEGER);
         CREATE TEMP TABLE uri_cross_chain_keys(value INTEGER);
         CREATE TEMP TABLE uri_contract_flags(value INTEGER);
         CREATE TEMP TABLE uri_chain_pair_contract_flags(value INTEGER);",
    )
    .unwrap();

    drop_prepare_only_uri_tables(&conn).unwrap();

    for table in [
        "contract_dim",
        "uri_rows",
        "uri_key_contracts",
        "uri_duplicate_key_stats",
        "uri_cross_chain_keys",
        "uri_contract_flags",
        "uri_chain_pair_contract_flags",
    ] {
        let exists: bool = conn
            .query_row(
                "SELECT count(*) > 0 FROM duckdb_tables() WHERE table_name = ?",
                [table],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!exists, "temporary table still present: {table}");
    }
}

#[test]
fn rust_heavy_phases_clamp_duckdb_without_raising_smaller_limits() {
    let mut options = AnalysisOptions {
        database_path: PathBuf::from(":memory:"),
        parquet_inputs: Vec::new(),
        output_dir: PathBuf::from("unused"),
        name_threshold: 95.0,
        threads: 1,
        memory_limit: "192GiB".into(),
        analysis_memory_limit: Some("192GiB".into()),
        duckdb_memory_limit: "160GiB".into(),
        temp_directory: None,
        progress: false,
    };
    assert_eq!(
        phase_duckdb_memory_limit(&options, NAME_DUCKDB_MEMORY_CAP).unwrap(),
        "8GiB"
    );
    assert_eq!(
        phase_duckdb_memory_limit(&options, METADATA_DUCKDB_MEMORY_CAP).unwrap(),
        "32GiB"
    );

    options.duckdb_memory_limit = "4GiB".to_string();
    assert_eq!(
        phase_duckdb_memory_limit(&options, NAME_DUCKDB_MEMORY_CAP).unwrap(),
        "4GiB"
    );
}

#[test]
fn jaro_winkler_upper_bound_filters_impossible_thresholds() {
    let upper_bound = jaro_winkler_upper_bound("azuki", "a-very-long-unrelated-name");

    assert!(upper_bound < 90.0);
    assert!(name_pair_can_reach_threshold("azuki", "azukii", 90.0));
}

#[test]
fn cached_lengths_drive_jaro_winkler_upper_bound() {
    let upper_bound = jaro_winkler_upper_bound_from_lengths(5, 26);

    assert_eq!(
        upper_bound,
        jaro_winkler_upper_bound("azuki", "a-very-long-unrelated-name")
    );
    assert!(name_pair_lengths_can_reach_threshold(5, 6, 90.0));
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

    assert_eq!(right_name_range_end_for_left(&atoms, 0, 95.0), 2);
    assert_eq!(candidate_name_chunk_count(&atoms, 95.0), 1);
    assert_eq!(full_name_chunk_count(atoms.len()), 2);
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

    assert_eq!(candidate_name_chunk_count(&atoms, 95.0), 0);
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
    let mut scratch = NameCandidateScratch::new(atoms.len());

    for threshold in [90.0, 95.0, 100.0] {
        for left in 0..atoms.len().saturating_sub(1) {
            let actual = score_indexed_name_pairs_for_left(
                &atoms,
                &candidate_index,
                left,
                left + 1..atoms.len(),
                threshold,
                &mut scratch,
            )
            .into_iter()
            .map(|hit| hit.right)
            .collect::<Vec<_>>();
            let expected = (left + 1..atoms.len())
                .filter(|&right| {
                    name_pair_score_from_names(&atoms[left].name_norm, &atoms[right].name_norm)
                        >= threshold
                })
                .collect::<Vec<_>>();

            assert_eq!(actual, expected, "left={left}, threshold={threshold}");
        }
    }
}

#[test]
fn chain_pair_length_window_excludes_unreachable_right_atoms() {
    let atoms = vec![
        NameAtom {
            chain_index: 1,
            name_norm: "cat".into(),
            char_len: 3,
            contract_count: 1,
            nft_count: 1,
        },
        NameAtom {
            chain_index: 0,
            name_norm: "azuki".into(),
            char_len: 5,
            contract_count: 1,
            nft_count: 1,
        },
        NameAtom {
            chain_index: 1,
            name_norm: "azukis".into(),
            char_len: 6,
            contract_count: 1,
            nft_count: 1,
        },
        NameAtom {
            chain_index: 1,
            name_norm: "a-very-long-unrelated-name".into(),
            char_len: 26,
            contract_count: 1,
            nft_count: 1,
        },
    ];
    let left_atoms = vec![1];
    let right_atoms = vec![0, 2, 3];

    assert_eq!(
        right_atom_range_for_left(&atoms, &right_atoms, left_atoms[0], 95.0),
        1..2
    );
    assert_eq!(
        chain_pair_candidate_chunk_count(&atoms, &left_atoms, &right_atoms, 95.0),
        1
    );
}

#[test]
fn metadata_projection_keeps_token_id_for_verification() {
    let sql = build_metadata_rows_sql("'sample.parquet'", "metadata_json");

    assert!(sql.contains(" AS token_id,"));
    assert!(!sql.contains("token_uri_norm"));
    assert!(!sql.contains("image_uri_norm"));
    assert!(sql.contains("metadata_json"));
    assert!(sql.contains("filename = true"));
    assert!(sql.contains("file_row_number = true"));
    assert!(sql.contains("source_file"));
    assert!(sql.contains("source_row_number"));
}

#[test]
fn metadata_source_ids_follow_cli_file_order() {
    let paths = [PathBuf::from("z.parquet"), PathBuf::from("a.parquet")];
    let expression = source_file_id_projection_expr(&paths);

    assert!(expression.contains("WHEN 'z.parquet' THEN 0::UINTEGER"));
    assert!(expression.contains("WHEN 'a.parquet' THEN 1::UINTEGER"));
    assert!(expression.contains("error("));
}

#[test]
fn domain_projections_preserve_solana_case_only() {
    let projections = [
        build_core_rows_sql("'sample.parquet'"),
        build_uri_rows_sql("'sample.parquet'"),
        build_metadata_rows_sql("'sample.parquet'", "metadata_json"),
    ];

    for sql in projections {
        assert!(sql.contains("WHEN lower(trim(CAST(chain AS VARCHAR))) = 'solana'"));
        assert!(sql.contains("THEN trim(CAST(contract_address AS VARCHAR))"));
        assert!(sql.contains("ELSE lower(trim(CAST(contract_address AS VARCHAR)))"));
    }
}

#[test]
fn domain_projections_do_not_materialize_a_wide_analysis_table() {
    let core = build_core_rows_sql("'sample.parquet'");
    let uri = build_uri_rows_sql("'sample.parquet'");
    let metadata = build_metadata_rows_sql("'sample.parquet'", "metadata_json");

    assert!(!core.contains("metadata_json"));
    assert!(!core.contains("token_uri_norm"));
    assert!(!uri.contains("metadata_json"));
    assert!(!uri.contains("name_norm"));
    assert!(!metadata.contains("token_uri_norm"));
    assert!(!metadata.contains("name_norm"));
    assert!(!analysis_contracts_sql().contains("analysis_rows"));
    assert!(core.contains("CREATE OR REPLACE TEMP TABLE contract_dim"));
    assert!(uri.contains("CREATE OR REPLACE TEMP TABLE uri_rows"));
}

#[test]
fn metadata_rows_materialize_eligibility_once() {
    let sql = build_metadata_rows_sql("'sample.parquet'", "metadata_json");

    assert!(sql.contains("AS metadata_eligible"));
    assert!(analysis_contracts_sql().contains("WHERE metadata_eligible"));
    assert!(!analysis_contracts_sql().contains("FILTER (WHERE metadata_eligible)"));
}

#[test]
fn metadata_sql_eligibility_uses_utf8_bytes_not_characters() {
    let conn = duckdb::Connection::open_in_memory().unwrap();
    let metadata = format!(r#"{{"description":"{}"}}"#, "界".repeat(22_000));
    assert!(metadata.chars().count() <= MAX_METADATA_BYTES_FOR_DEDUP);
    assert!(metadata.len() > MAX_METADATA_BYTES_FOR_DEDUP);
    let sql = format!("SELECT {}", metadata_json_eligible_predicate("?1"));

    let eligible = conn
        .query_row(&sql, [&metadata], |row| row.get::<_, bool>(0))
        .unwrap();

    assert!(!eligible);
}

#[test]
fn analysis_contracts_sql_selects_one_representative_per_contract() {
    let conn = duckdb::Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
            CREATE TEMP TABLE source_rows AS
            SELECT * FROM (
                VALUES
                ('ethereum', '0xaaa', '1', '', '{"description":"first available"}'),
                ('ethereum', '0xaaa', '2', '', '{"description":"later repeated"}'),
                ('ethereum', '0xaaa', '3', '', '{"description":"later repeated"}'),
                ('ethereum', '0xbbb', '0', '', ''),
                ('ethereum', '0xbbb', '1', '', '{"description":"first usable for b"}'),
                ('ethereum', '0xbbb', '2', '', '{"description":"later b"}')
            ) AS t(chain, contract_address, token_id, name_norm, metadata_json);
            CREATE TEMP TABLE contract_dim AS
            SELECT (row_number() OVER (ORDER BY chain, contract_address) - 1)::UINTEGER AS contract_id,
                   chain, contract_address, count(*)::BIGINT AS nft_count,
                   min(nullif(name_norm, '')) AS name_norm
            FROM source_rows
            GROUP BY chain, contract_address;
            CREATE TEMP TABLE metadata_rows AS
            SELECT contracts.contract_id, token_id, metadata_json,
                   0::UINTEGER AS source_file,
                   row_number() OVER ()::UBIGINT AS source_row_number,
                   metadata_json <> '' AS metadata_eligible
            FROM source_rows
            JOIN contract_dim contracts USING (chain, contract_address);
        "#,
    )
    .unwrap();

    conn.execute_batch(&analysis_contracts_sql()).unwrap();
    let mut stmt = conn
        .prepare(
            "
            SELECT contracts.contract_address, rows.metadata_json, contracts.nft_count
            FROM analysis_contracts contracts
            JOIN metadata_rows rows
              ON rows.source_file = contracts.metadata_source_file
             AND rows.source_row_number = contracts.metadata_source_row_number
            ORDER BY contracts.contract_address
            ",
        )
        .unwrap();
    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|(contract, metadata, nft_count)| {
        contract == "0xaaa" && metadata == r#"{"description":"first available"}"# && *nft_count == 3
    }));
    assert!(rows.iter().any(|(contract, metadata, nft_count)| {
        contract == "0xbbb"
            && metadata == r#"{"description":"first usable for b"}"#
            && *nft_count == 3
    }));
}

#[test]
fn analysis_contracts_sql_uses_grouped_stable_representatives() {
    let sql = analysis_contracts_sql();

    assert!(!sql.contains("GROUP BY chain, contract_address, metadata_json"));
    assert!(sql.contains("GROUP BY contract_id"));
    assert!(sql.contains("arg_min("));
    assert!(sql.contains("row(token_id, source_file, source_row_number)"));
    assert!(sql.contains("WHERE metadata_eligible"));
    assert!(!sql.contains("metadata_contract_lookup"));
    assert!(!sql.contains("FULL OUTER JOIN"));
}

#[test]
fn analysis_contracts_aggregates_only_the_representative_row_id() {
    let sql = analysis_contracts_sql();

    assert_eq!(sql.matches("arg_min(").count(), 1);
    assert!(sql.contains("struct_pack("));
    assert!(sql.contains("metadata_source.file_id"));
    assert!(sql.contains("metadata_source.row_number"));
    assert!(sql.contains("indexed_metadata_sources"));
    assert!(!sql.contains("row_number() OVER (ORDER BY contract_id)"));
    assert!(!sql.contains("count(*) FILTER"));
    assert!(!sql.contains("representative.metadata_json"));
    assert!(!sql.contains("JOIN metadata_rows representative"));
}

#[test]
fn persisted_metadata_sources_use_stable_file_and_row_ids() {
    let contracts = analysis_contracts_sql();
    let load_source = include_str!("metadata/load.rs");
    let start = load_source
        .find("pub(super) fn metadata_contract_token_rows_sql")
        .unwrap();
    let tokens = &load_source[start..];

    assert!(!contracts.contains("rowid"));
    assert!(!tokens.contains("rowid"));
    for sql in [contracts.as_str(), tokens] {
        assert!(sql.contains("metadata_source_file"));
        assert!(sql.contains("metadata_source_row_number"));
    }
}

#[test]
fn cross_process_stage_tables_are_persistent() {
    let contracts = analysis_contracts_sql().to_ascii_uppercase();
    assert!(contracts.contains("CREATE OR REPLACE TABLE ANALYSIS_CONTRACTS"));
    assert!(!contracts.contains("CREATE TEMP TABLE ANALYSIS_CONTRACTS"));

    let source = include_str!("duckdb_prep.rs").to_ascii_uppercase();
    for table in ["SELECTED_CHAINS", "NAME_ATOMS"] {
        assert!(
            source.contains(&format!("CREATE OR REPLACE TABLE {table}")),
            "{table} must survive the prepare child process"
        );
    }
}

#[test]
fn final_outputs_use_partial_files_before_atomic_rename() {
    let source = include_str!("output.rs");
    assert!(source.contains("summary.json.partial"));
    assert!(source.contains("summary.csv.partial"));
    assert!(source.contains("replace_file_atomically"));
    assert!(!source.contains("remove_file"));
}

fn output_generation_report(metric: &str, duplicate_contract_count: i64) -> AnalysisReport {
    AnalysisReport {
        summary_rows: vec![SummaryRow {
            field_name: "name".to_string(),
            scope: "intra_chain".to_string(),
            primary_chain: "ethereum".to_string(),
            secondary_chain: String::new(),
            threshold: Some(95.0),
            match_mode: "jaro_winkler".to_string(),
            metric: metric.to_string(),
            total_contracts: 10,
            total_nfts: 100,
            group_count: 1,
            duplicate_contract_count,
            duplicate_nft_count: 20,
            duplicate_contract_ratio: 20.0,
            duplicate_nft_ratio: 20.0,
            group_size_ge_2_count: 1,
            group_size_gt_2_count: 0,
        }],
    }
}

#[test]
fn output_generation_is_valid_only_after_manifest_publication() {
    let directory = tempfile::tempdir().unwrap();
    let report = output_generation_report("old", 2);
    fs::write(
        directory.path().join("summary.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    fs::write(directory.path().join("summary.csv"), b"old csv\n").unwrap();

    let before_publication = validate_output_generation(directory.path()).unwrap_err();
    assert!(before_publication
        .to_string()
        .contains("summary.manifest.json"));

    write_outputs(&report, directory.path()).unwrap();

    validate_output_generation(directory.path()).unwrap();
    assert!(directory.path().join("summary.manifest.json").is_file());
}

#[test]
fn output_generation_rejects_a_mixed_summary_pair() {
    let directory = tempfile::tempdir().unwrap();
    let first = output_generation_report("first", 2);
    write_outputs(&first, directory.path()).unwrap();
    validate_output_generation(directory.path()).unwrap();

    let second = output_generation_report("second", 3);
    fs::write(
        directory.path().join("summary.json"),
        serde_json::to_vec_pretty(&second).unwrap(),
    )
    .unwrap();

    let error = validate_output_generation(directory.path()).unwrap_err();
    assert!(error.to_string().contains("summary.json"));
}

#[test]
fn metadata_raw_rows_read_precomputed_contract_rows() {
    let sql = metadata_raw_rows_sql();

    assert_eq!(sql.matches("analysis_contracts").count(), 1);
    assert_eq!(sql.matches("metadata_rows").count(), 1);
    assert!(sql.contains("rows.source_file = contracts.metadata_source_file"));
    assert!(sql.contains("rows.source_row_number = contracts.metadata_source_row_number"));
    assert_eq!(sql.matches("analysis_rows").count(), 0);
    assert!(!sql.contains("GROUP BY"));
    assert!(!sql.contains("arg_min("));
}

#[test]
fn chain_totals_query_aggregates_all_chains_once() {
    let sql = chain_totals_sql();

    assert!(sql.contains("GROUP BY chain"));
    assert!(!sql.contains('?'));
}

#[test]
fn arrow_columns_preserve_utf8_integers_nulls_and_order() {
    let conn = duckdb::Connection::open_in_memory().unwrap();
    let mut stmt = conn
        .prepare(
            "
            SELECT * FROM (
                VALUES
                    (0::BIGINT, '金色 dragon'::VARCHAR),
                    (1::BIGINT, NULL::VARCHAR)
            ) rows(row_index, text_value)
            ORDER BY row_index
            ",
        )
        .unwrap();
    let batches = stmt.query_arrow([]).unwrap().collect::<Vec<_>>();

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    let indexes = arrow_i64_column(batch, 0, "row_index").unwrap();
    let texts = arrow_string_column(batch, 1, "text_value").unwrap();
    assert_eq!(indexes.value(0), 0);
    assert_eq!(indexes.value(1), 1);
    assert_eq!(texts.value(0), "金色 dragon");
    assert!(duckdb::arrow::array::Array::is_null(texts, 1));
}

#[test]
fn analysis_contracts_sql_selects_lowest_token_id_representative() {
    let conn = duckdb::Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
            CREATE TEMP TABLE source_rows AS
            SELECT * FROM (
                VALUES
                ('ethereum', '0xaaa', '2', '', '{"description":"rowid first"}'),
                ('ethereum', '0xaaa', '1', '', '{"description":"token id first"}')
            ) AS t(chain, contract_address, token_id, name_norm, metadata_json);
            CREATE TEMP TABLE contract_dim AS
            SELECT 0::UINTEGER AS contract_id,
                   chain, contract_address, count(*)::BIGINT AS nft_count,
                   min(nullif(name_norm, '')) AS name_norm
            FROM source_rows
            GROUP BY chain, contract_address;
            CREATE TEMP TABLE metadata_rows AS
            SELECT 0::UINTEGER AS contract_id, token_id, metadata_json,
                   0::UINTEGER AS source_file,
                   row_number() OVER ()::UBIGINT AS source_row_number,
                   metadata_json <> '' AS metadata_eligible
            FROM source_rows;
        "#,
    )
    .unwrap();

    conn.execute_batch(&analysis_contracts_sql()).unwrap();
    let metadata = conn
        .query_row(
            "SELECT rows.metadata_json
             FROM analysis_contracts contracts
             JOIN metadata_rows rows
               ON rows.source_file = contracts.metadata_source_file
              AND rows.source_row_number = contracts.metadata_source_row_number",
            [],
            |row| row.get::<_, String>(0),
        )
        .unwrap();

    assert_eq!(metadata, r#"{"description":"token id first"}"#);
}

#[test]
fn uri_duplicate_sql_skips_full_stats_tables() {
    let duplicate_sql = build_uri_duplicate_key_stats_sql();

    assert!(!duplicate_sql.contains("uri_key_stats"));
    assert!(duplicate_sql.contains("HAVING count(*) >= 2"));
    assert!(!duplicate_sql.contains("nft_count"));
}

#[test]
fn uri_key_contract_sql_aggregates_key_kinds_before_union() {
    let sql = build_uri_key_contracts_sql();

    assert_eq!(sql.matches("uri_rows").count(), 2);
    assert!(!sql.contains("CROSS JOIN LATERAL"));
    assert!(sql.contains("UNION ALL"));
    assert!(sql.contains("0::UTINYINT AS key_kind"));
    assert!(sql.contains("1::UTINYINT AS key_kind"));
    assert!(sql.contains("rows.chain_index"));
    assert!(!sql.contains("contract_dim"));
    assert!(!sql.contains("strict_token"));
    assert!(!sql.contains("nft_count"));
}

#[test]
fn single_chain_uri_flags_skip_cross_chain_key_tables() {
    let sql = build_uri_contract_flags_sql(false);

    assert!(!sql.contains("uri_key_chain_counts"));
    assert!(!sql.contains("uri_duplicate_key_chain_counts"));
    assert!(!sql.contains("uri_cross_chain_keys"));
    assert!(!sql.contains("norm_cross_chain"));
    assert!(!sql.contains("norm_token_chain"));
    assert!(!sql.contains("norm_image_chain"));
    assert!(sql.contains("norm_contract_v1_nfts"));
}

#[test]
fn multi_chain_uri_flags_include_cross_chain_tables_and_metrics() {
    let key_sql = build_uri_cross_chain_keys_sql();
    let flags_sql = build_uri_contract_flags_sql(true);
    let pair_sql = build_uri_chain_pair_contract_flags_sql();

    assert!(key_sql.contains("bit_count(chain_mask) >= 2"));
    assert!(!key_sql.contains("count(DISTINCT"));
    assert!(key_sql.contains("chain_mask"));
    assert!(!key_sql.contains("JOIN selected_chains"));
    assert!(flags_sql.contains("nt.chain_index = r.chain_index"));
    assert!(flags_sql.contains("nt.key_kind = 0"));
    assert!(flags_sql.contains("SELECT chain_index"));
    assert!(!flags_sql.contains("chains.chain"));
    assert!(flags_sql.contains("uri_cross_chain_keys"));
    assert!(flags_sql.contains("norm_cross_chain_v1_nfts"));
    assert!(pair_sql.contains("uri_cross_chain_keys"));
    assert!(pair_sql.contains("chain_mask"));
    assert!(pair_sql.contains("primary_chain"));
    assert!(pair_sql.contains("secondary_chain"));
    assert!(pair_sql.contains("norm_chain_v3_contracts"));
    assert!(pair_sql.contains("rowid AS uri_row_id"));
    assert!(pair_sql.contains("UNION ALL"));
    assert!(!pair_sql.contains("CROSS JOIN selected_chains"));
    assert!(pair_sql.contains("primary_chain_index"));
    assert!(pair_sql.contains("secondary_chain_index"));
    assert!(!pair_sql.contains("count(*)::BIGINT AS total_nfts"));
}

#[test]
fn cross_chain_uri_keys_use_compact_chain_masks() {
    let keys = build_uri_cross_chain_keys_sql();
    let flags = build_uri_contract_flags_sql(true);

    assert!(keys.contains("bit_or"));
    assert!(keys.contains("chain_mask"));
    assert!(flags.contains("chain_mask"));
    assert!(!flags.contains("uri_key_chain_presence"));
}

#[test]
fn chain_masks_reject_more_than_64_chains() {
    assert!(validate_chain_mask_capacity(64).is_ok());
    assert!(validate_chain_mask_capacity(65).is_err());
}

#[test]
fn chain_pair_count_query_aggregates_all_pairs_at_once() {
    let sql = uri_chain_pair_counts_sql();

    assert!(sql.contains("GROUP BY primary_chain_index, secondary_chain_index"));
    assert!(sql.contains("selected_chains"));
    assert!(!sql.contains('?'));
}

#[test]
fn contract_count_query_aggregates_all_chains_and_scopes_at_once() {
    let sql = uri_contract_counts_sql(true);

    assert!(sql.contains("GROUP BY chain"));
    assert!(sql.contains("selected_chains"));
    assert!(sql.contains("norm_contract_v1_nfts"));
    assert!(sql.contains("norm_cross_chain_v1_nfts"));
    assert!(!sql.contains('?'));
}

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
fn disabled_progress_tracker_is_noop() {
    let progress = ProgressTracker::new(1, false);

    progress.start_phase("phase", 1);
    progress.add_work(1);
    progress.step("step");
    progress.inc(1);
    progress.set_message("message");
    progress.finish_phase("done");
    progress.start_task("task", Some(1), "rows");
    progress.advance_task(1, ProgressCounters::default());
    progress.finish_task("task done");
    progress.fail("ignored");
    progress.finish();
}

#[test]
fn hierarchical_progress_tracks_pipeline_stage_and_task_independently() {
    let progress = ProgressTracker::for_pipeline_stage(PipelineStage::Metadata, true);

    let ProgressTracker::Enabled {
        pipeline,
        stage,
        task,
        ..
    } = &progress
    else {
        panic!("progress must be enabled");
    };
    assert_eq!(pipeline.length(), Some(4));
    assert_eq!(pipeline.position(), 2);
    assert_eq!(pipeline.message(), "metadata");

    progress.start_stage("shared-token matching", 6);
    assert_eq!(pipeline.message(), "metadata");
    progress.step_stage("metadata documents loaded");
    assert_eq!(stage.length(), Some(6));
    assert_eq!(stage.position(), 1);

    progress.start_task("shared-token memberships", Some(100), "rows");
    progress.advance_task(
        25,
        ProgressCounters {
            groups: 2,
            candidates: 300,
            scored: 40,
            matched: 7,
        },
    );
    assert_eq!(task.length(), Some(100));
    assert_eq!(task.position(), 25);
    assert!(task.message().contains("groups 2"));
    assert!(task.message().contains("candidates 300"));
    assert!(task.message().contains("scored 40"));
    assert!(task.message().contains("matched 7"));

    progress.finish_task("shared-token matching complete");
    progress.finish_stage("metadata complete");
    progress.finish_pipeline_stage("metadata complete");
    assert_eq!(pipeline.position(), 3);
}

#[test]
fn hierarchical_progress_can_move_to_finalize_without_recreating_state() {
    let progress = ProgressTracker::for_pipeline_stage(PipelineStage::Prepare, true);
    progress.set_pipeline_stage(PipelineStage::Finalize);

    let ProgressTracker::Enabled { pipeline, .. } = &progress else {
        panic!("progress must be enabled");
    };
    assert_eq!(pipeline.position(), 3);
    assert_eq!(pipeline.message(), "finalize outputs");
}

#[test]
fn task_progress_message_uses_stable_units_for_throughput_and_eta() {
    let message = format_task_progress_message(&TaskProgressSnapshot {
        label: "shared-token memberships",
        position: 25,
        total: Some(100),
        unit: "rows",
        counters: ProgressCounters {
            groups: 2,
            candidates: 300,
            scored: 40,
            matched: 7,
        },
        elapsed: std::time::Duration::from_secs(2),
    });

    assert_eq!(
        message,
        "shared-token memberships; 25/100 rows; 12.5 rows/s; ETA 6s; groups 2; candidates 300; scored 40; matched 7"
    );
}

#[test]
fn task_progress_message_keeps_unknown_work_indeterminate() {
    let message = format_task_progress_message(&TaskProgressSnapshot {
        label: "building metadata index",
        position: 9,
        total: None,
        unit: "docs",
        counters: ProgressCounters::default(),
        elapsed: std::time::Duration::from_secs(3),
    });

    assert_eq!(
        message,
        "building metadata index; 9 docs; 3.0 docs/s; ETA n/a"
    );
}

#[test]
fn hierarchical_progress_finishes_all_levels_with_failure_context() {
    let progress = ProgressTracker::for_pipeline_stage(PipelineStage::Metadata, true);
    progress.start_stage("shared-token matching", 1);
    progress.start_task("membership rows", Some(10), "rows");
    progress.fail("metadata query failed");

    let ProgressTracker::Enabled {
        pipeline,
        stage,
        task,
        ..
    } = &progress
    else {
        panic!("progress must be enabled");
    };
    assert!(pipeline.is_finished());
    assert!(stage.is_finished());
    assert!(task.is_finished());
    assert!(pipeline.message().contains("FAILED"));
    assert!(pipeline.message().contains("metadata query failed"));
}

#[test]
fn auto_memory_plan_rejects_resident_atoms_over_budget() {
    let error = name_analysis_memory_plan("1GB", None, 2 * 1024 * 1024 * 1024).unwrap_err();

    assert!(error.to_string().contains("loaded name atoms need"));
}

#[test]
fn chunk_count_matches_nested_loop_chunks() {
    let atom_count = RIGHT_SCORE_CHUNK_SIZE + 3;
    let mut expected = 0;
    for left in 0..atom_count - 1 {
        let right_count = atom_count - left - 1;
        expected += right_count.div_ceil(RIGHT_SCORE_CHUNK_SIZE);
    }

    assert_eq!(full_name_chunk_count(atom_count), expected as u64);
    assert_eq!(chain_pair_chunk_count(3, RIGHT_SCORE_CHUNK_SIZE + 1), 6);
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
    let mut scratch = NameCandidateScratch::new(atoms.len());

    for threshold in [50.0, 60.0, 70.0, 80.0] {
        for left in 0..atoms.len().saturating_sub(1) {
            let actual = score_indexed_name_pairs_for_left(
                &atoms,
                &candidate_index,
                left,
                left + 1..atoms.len(),
                threshold,
                &mut scratch,
            )
            .into_iter()
            .map(|hit| hit.right)
            .collect::<Vec<_>>();
            let expected = (left + 1..atoms.len())
                .filter(|&right| {
                    name_pair_score_from_names(&atoms[left].name_norm, &atoms[right].name_norm)
                        >= threshold
                })
                .collect::<Vec<_>>();
            assert_eq!(actual, expected, "left={left}, threshold={threshold}");
        }
    }
}

// Deterministic xorshift PRNG so the randomized test is reproducible
// without pulling in the `rand` crate.
fn xorshift(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
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
        let mut scratch = NameCandidateScratch::new(atoms.len());
        let threshold = 50.0 + 10.0 * (xorshift(&mut state) % 6) as f64;

        for left in 0..atoms.len().saturating_sub(1) {
            let right_end = right_name_range_end_for_left(&atoms, left, threshold);
            let actual = score_indexed_name_pairs_for_left(
                &atoms,
                &candidate_index,
                left,
                left + 1..right_end,
                threshold,
                &mut scratch,
            )
            .into_iter()
            .map(|hit| hit.right)
            .collect::<Vec<_>>();
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
fn canonical_name_lookup_borrows_source_names() {
    let source = include_str!("name.rs");

    assert!(source.contains("HashMap::<&str, usize>::new()"));
    assert!(!source.contains("HashMap::<String, usize>::new()"));
}

#[test]
fn name_scoring_releases_candidate_state_before_summary_building() {
    let source = include_str!("name.rs");
    let drop_index = source.find("drop(candidate_index)").unwrap();
    let drop_canonical = source.find("drop(canonical)").unwrap();
    let build_summary = source.find("push_name_summary_rows(").unwrap();

    assert!(drop_index < build_summary);
    assert!(drop_canonical < build_summary);
}

#[test]
fn name_summary_releases_chain_index_before_matrix_summary() {
    let source = include_str!("name.rs");
    let drop_chain_index = source.find("drop(atoms_by_chain)").unwrap();
    let build_matrix_summary = source.find("push_reused_chain_matrix_rows(").unwrap();

    assert!(drop_chain_index < build_matrix_summary);
}

#[test]
fn name_summary_releases_global_dsu_before_matrix_summary() {
    let source = include_str!("name.rs");
    let release_intra = source.find("state.intra = UnionFind::new(0)").unwrap();
    let release_cross = source.find("state.cross = None").unwrap();
    let build_matrix_summary = source.find("push_reused_chain_matrix_rows(").unwrap();

    assert!(release_intra < build_matrix_summary);
    assert!(release_cross < build_matrix_summary);
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

    let progress = ProgressTracker::new(1, true);
    progress.start_phase("canonical name scoring", 0);
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
    let progress = ProgressTracker::new(1, true);
    progress.start_phase("canonical name scoring", 0);
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
fn production_name_scoring_streams_matches_into_bounded_edge_batches() {
    let source = include_str!("name_scoring.rs");
    let start = source
        .find("pub(crate) fn union_canonical_name_pairs(")
        .unwrap();
    let end = source[start..]
        .find("pub(crate) fn right_name_range_end_for_left(")
        .map(|offset| start + offset)
        .unwrap();
    let production_scoring = &source[start..end];

    assert!(!production_scoring.contains("Vec::<ScoredRight>::new()"));
    assert!(!production_scoring.contains("let mut hits"));
    assert!(production_scoring.contains("visit_indexed_name_pairs_for_left("));
}

#[test]
fn parallel_name_scoring_reduces_fold_local_stats_without_shared_atomics() {
    let source = include_str!("name_scoring.rs");
    let start = source
        .find("pub(crate) fn union_canonical_name_pairs(")
        .unwrap();
    let end = source[start..]
        .find("fn apply_canonical_edge_batch(")
        .map(|offset| start + offset)
        .unwrap();
    let parallel_scoring = &source[start..end];

    assert!(!parallel_scoring.contains("AtomicU64"));
    assert!(!parallel_scoring.contains("fetch_add"));
    assert!(source.contains("stats: NameScoringStats"));
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
            let mut dense = NameCandidateScratch::new(atoms.len());
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
