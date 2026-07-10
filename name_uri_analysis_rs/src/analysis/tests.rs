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
    let mut states = vec![ThresholdUnionState {
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
        &mut states,
        1,
        &progress,
    );

    let ProgressTracker::Enabled { detail, .. } = &progress else {
        panic!("progress must be enabled");
    };
    assert_eq!(detail.position(), 1);
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
        + candidate_index.postings.capacity()
            * std::mem::size_of::<Vec<NameAtomIndex>>();

    assert!(candidate_index.memory_bytes() >= minimum_structural_bytes);
}

#[test]
fn name_scratch_plan_uses_dense_mode_only_when_all_workers_fit() {
    let atom_count = 1_000_000;
    let threads = 96;
    let dense_bytes = atom_count
        * std::mem::size_of::<u32>()
        * threads;

    let dense = name_scratch_plan(atom_count, threads, dense_bytes);
    let sparse = name_scratch_plan(atom_count, threads, dense_bytes - 1);

    assert_eq!(dense.mode, NameScratchMode::Dense);
    assert_eq!(dense.reserved_bytes, dense_bytes);
    assert_eq!(sparse.mode, NameScratchMode::Sparse);
    assert_eq!(sparse.reserved_bytes, 0);
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
    assert!(query
        .score_percent("金色 dragons", expected)
        .is_some());
    assert!(query
        .score_percent("金色 dragons", expected + 1e-9)
        .is_none());
}

#[test]
fn threshold_batches_reuse_memory_limit_by_default() {
    let plan =
        name_analysis_memory_plan(&[90.0, 95.0, 98.0], 1_000, 1, "1MB", None, 0, 0).unwrap();
    let batches = threshold_batches(&[90.0, 95.0, 98.0], 1_000, 1, plan.analysis_bytes);

    assert_eq!(batches, vec![vec![98.0, 95.0, 90.0]]);
}

#[test]
fn threshold_batches_honor_analysis_memory_override() {
    let plan =
        name_analysis_memory_plan(&[90.0, 95.0, 98.0], 1_000, 2, "1GB", Some("16KB"), 0, 0)
            .unwrap();
    let batches = threshold_batches(&[90.0, 95.0, 98.0], 1_000, 2, plan.analysis_bytes);

    assert_eq!(batches, vec![vec![98.0], vec![95.0], vec![90.0]]);
}

#[test]
fn threshold_batches_use_available_analysis_budget_aggressively() {
    let state_bytes = threshold_state_bytes(10_000, 2);
    let analysis_budget = state_bytes.saturating_mul(3);

    let batches = threshold_batches(&[90.0, 95.0, 98.0], 10_000, 2, analysis_budget);

    assert_eq!(batches, vec![vec![98.0, 95.0, 90.0]]);
}

#[test]
fn auto_memory_plan_prefers_name_analysis_when_many_thresholds_can_fit() {
    let state_bytes = threshold_state_bytes(50_000, 2);
    let total_budget = state_bytes.saturating_mul(6);
    let memory_limit = format_byte_size(total_budget);

    let plan =
        name_analysis_memory_plan(&[90.0, 95.0, 98.0], 50_000, 2, &memory_limit, None, 0, 0)
            .unwrap();

    assert!(plan.analysis_bytes >= state_bytes.saturating_mul(3));
}

#[test]
fn auto_memory_plan_exposes_full_total_budget_to_rust_batching() {
    let total_budget = 512 * 1024 * 1024;
    let plan =
        name_analysis_memory_plan(&[90.0, 95.0, 98.0], 10_000, 2, "512MB", None, 0, 0).unwrap();

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

    let plan = name_analysis_memory_plan(
        &[90.0, 95.0, 98.0],
        atom_count,
        atoms_by_chain.len(),
        &memory_limit,
        None,
        0,
        matrix_bytes,
    )
    .unwrap();

    assert_eq!(plan.analysis_bytes, parsed_budget);
}

#[test]
fn default_memory_budget_is_fully_available_to_rust_batching() {
    let small =
        name_analysis_memory_plan(&[90.0, 95.0, 98.0], 1_000, 1, "10GB", None, 0, 0).unwrap();
    let large =
        name_analysis_memory_plan(&[90.0, 95.0, 98.0], 20_000_000, 2, "10GB", None, 0, 0)
            .unwrap();

    assert_eq!(small.analysis_bytes, 10 * 1024 * 1024 * 1024);
    assert_eq!(large.analysis_bytes, 10 * 1024 * 1024 * 1024);
}

#[test]
fn explicit_analysis_memory_limit_stays_inside_total_budget() {
    let plan =
        name_analysis_memory_plan(&[90.0, 95.0, 98.0], 1_000, 2, "10GB", Some("16KB"), 0, 0)
            .unwrap();

    assert_eq!(plan.analysis_bytes, 16 * 1024);
}

#[test]
fn explicit_analysis_memory_limit_rejects_over_budget_value() {
    let error =
        name_analysis_memory_plan(&[90.0], 1_000, 2, "1GB", Some("2GB"), 0, 0).unwrap_err();

    assert!(error.to_string().contains("exceeds total --memory-limit"));
}

#[test]
fn analysis_memory_auto_uses_total_budget_auto_balance() {
    let default_plan =
        name_analysis_memory_plan(&[90.0, 95.0, 98.0], 10_000, 2, "4GB", None, 0, 0).unwrap();
    let auto_plan =
        name_analysis_memory_plan(&[90.0, 95.0, 98.0], 10_000, 2, "4GB", Some("auto"), 0, 0)
            .unwrap();

    assert_eq!(auto_plan.analysis_bytes, default_plan.analysis_bytes);
}

#[test]
fn duckdb_configuration_does_not_parse_memory_limit() {
    let conn = Connection::open_in_memory().unwrap();
    let options = AnalysisOptions {
        database_path: PathBuf::from(":memory:"),
        parquet_inputs: Vec::new(),
        output_dir: PathBuf::from("unused"),
        thresholds: vec![95.0],
        threads: 1,
        memory_limit: "not-a-size".into(),
        analysis_memory_limit: None,
        duckdb_memory_limit: "1GB".into(),
        temp_directory: None,
        progress: false,
        persist_prepared: false,
        reuse_prepared: false,
    };

    configure_duckdb(&conn, &options).unwrap();
}

#[test]
fn adaptive_threshold_batch_size_shrinks_when_rss_is_high() {
    let batch_size = adaptive_threshold_batch_size(3, 3, 1_000, 10_000, 9_200);

    assert_eq!(batch_size, 1);
}

#[test]
fn adaptive_threshold_batch_size_keeps_capacity_when_rss_is_low() {
    let batch_size = adaptive_threshold_batch_size(3, 3, 1_000, 10_000, 4_000);

    assert_eq!(batch_size, 3);
}

#[test]
fn adaptive_threshold_batch_size_uses_remaining_headroom() {
    let batch_size = adaptive_threshold_batch_size(5, 5, 2_000, 10_000, 6_000);

    assert_eq!(batch_size, 2);
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

    assert_eq!(upper_bound, jaro_winkler_upper_bound("azuki", "a-very-long-unrelated-name"));
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
                    name_pair_score_from_names(
                        &atoms[left].name_norm,
                        &atoms[right].name_norm,
                    ) >= threshold
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
fn analysis_rows_projection_keeps_token_id_for_metadata_verification() {
    let sql = build_analysis_rows_sql("'sample.parquet'", "metadata_json");

    assert!(sql.contains(" AS token_id,"));
    assert!(!sql.contains(" AS token_uri,"));
    assert!(!sql.contains(" AS image_uri,"));
    assert!(sql.contains("token_uri_norm"));
    assert!(sql.contains("image_uri_norm"));
    assert!(sql.contains("metadata_json"));
}

#[test]
fn analysis_rows_projection_preserves_solana_case_only() {
    let sql = build_analysis_rows_sql("'sample.parquet'", "metadata_json");

    assert!(sql.contains("WHEN lower(trim(CAST(chain AS VARCHAR))) = 'solana'"));
    assert!(sql.contains("THEN trim(CAST(contract_address AS VARCHAR))"));
    assert!(sql.contains("ELSE lower(trim(CAST(contract_address AS VARCHAR)))"));
}

#[test]
fn analysis_contracts_sql_selects_one_representative_per_contract() {
    let conn = duckdb::Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
            CREATE TEMP TABLE analysis_rows AS
            SELECT * FROM (
                VALUES
                ('ethereum', '0xaaa', '1', '', '{"description":"first available"}'),
                ('ethereum', '0xaaa', '2', '', '{"description":"later repeated"}'),
                ('ethereum', '0xaaa', '3', '', '{"description":"later repeated"}'),
                ('ethereum', '0xbbb', '0', '', ''),
                ('ethereum', '0xbbb', '1', '', '{"description":"first usable for b"}'),
                ('ethereum', '0xbbb', '2', '', '{"description":"later b"}')
            ) AS t(chain, contract_address, token_id, name_norm, metadata_json);
        "#,
    )
    .unwrap();

    conn.execute_batch(&analysis_contracts_sql()).unwrap();
    let mut stmt = conn
        .prepare(
            "
            SELECT contract_address, metadata_json, nft_count
            FROM analysis_contracts
            WHERE metadata_json IS NOT NULL
            ORDER BY contract_address
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
        contract == "0xaaa"
            && metadata == r#"{"description":"first available"}"#
            && *nft_count == 3
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
    assert!(sql.contains("GROUP BY chain, contract_address"));
    assert!(sql.contains("arg_min("));
    assert!(sql.contains("row(token_id, rowid)"));
    assert!(sql.contains("FILTER"));
    assert!(!sql.contains("metadata_contract_lookup"));
    assert!(!sql.contains("FULL OUTER JOIN"));
}

#[test]
fn metadata_raw_rows_read_precomputed_contract_rows() {
    let sql = metadata_raw_rows_sql();

    assert_eq!(sql.matches("analysis_contracts").count(), 1);
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
            CREATE TEMP TABLE analysis_rows AS
            SELECT * FROM (
                VALUES
                ('ethereum', '0xaaa', '2', '', '{"description":"rowid first"}'),
                ('ethereum', '0xaaa', '1', '', '{"description":"token id first"}')
            ) AS t(chain, contract_address, token_id, name_norm, metadata_json);
        "#,
    )
    .unwrap();

    conn.execute_batch(&analysis_contracts_sql()).unwrap();
    let metadata = conn
        .query_row(
            "SELECT metadata_json FROM analysis_contracts",
            [],
            |row| row.get::<_, String>(0),
        )
        .unwrap();

    assert_eq!(metadata, r#"{"description":"token id first"}"#);
}

#[test]
fn uri_duplicate_sql_skips_full_stats_tables() {
    let duplicate_sql = build_uri_duplicate_key_stats_sql(false);

    assert!(!duplicate_sql.contains("uri_key_stats"));
    assert!(duplicate_sql.contains("HAVING"));
}

#[test]
fn uri_key_contract_sql_expands_keys_in_one_scan() {
    let sql = build_uri_key_contracts_sql(false);

    assert_eq!(sql.matches("analysis_rows").count(), 1);
    assert!(sql.contains("CROSS JOIN LATERAL"));
    assert!(sql.contains("norm_token"));
    assert!(sql.contains("norm_image"));
    assert!(!sql.contains("strict_token"));
}

#[test]
fn single_chain_uri_flags_skip_cross_chain_key_tables() {
    let sql = build_uri_contract_flags_sql(false, false);

    assert!(!sql.contains("uri_key_chain_counts"));
    assert!(!sql.contains("uri_duplicate_key_chain_counts"));
    assert!(!sql.contains("uri_cross_chain_keys"));
    assert!(!sql.contains("norm_cross_chain"));
    assert!(!sql.contains("_chain"));
    assert!(sql.contains("norm_contract_v1_nfts"));
}

#[test]
fn multi_chain_uri_flags_include_cross_chain_tables_and_metrics() {
    let key_sql = build_uri_cross_chain_keys_sql(false);
    let presence_sql = build_uri_key_chain_presence_sql(false);
    let flags_sql = build_uri_contract_flags_sql(true, false);
    let pair_sql = build_uri_chain_pair_contract_flags_sql(false);

    assert!(key_sql.contains("count(DISTINCT chain) >= 2"));
    assert!(presence_sql.contains("JOIN uri_cross_chain_keys"));
    assert!(flags_sql.contains("uri_cross_chain_keys"));
    assert!(flags_sql.contains("norm_cross_chain_v1_nfts"));
    assert!(pair_sql.contains("uri_key_chain_presence"));
    assert!(pair_sql.contains("primary_chain"));
    assert!(pair_sql.contains("secondary_chain"));
    assert!(pair_sql.contains("norm_chain_v3_contracts"));
    assert!(pair_sql.contains("rowid AS uri_row_id"));
    assert!(pair_sql.contains("UNION ALL"));
    assert!(!pair_sql.contains("CROSS JOIN selected_chains"));
    assert!(!pair_sql.contains("count(*)::BIGINT AS total_nfts"));
}

#[test]
fn chain_pair_count_query_aggregates_all_pairs_at_once() {
    let sql = uri_chain_pair_counts_sql();

    assert!(sql.contains("GROUP BY primary_chain, secondary_chain"));
    assert!(!sql.contains('?'));
}

#[test]
fn contract_count_query_aggregates_all_chains_and_scopes_at_once() {
    let sql = uri_contract_counts_sql(true);

    assert!(sql.contains("GROUP BY chain"));
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
fn chain_matrix_capacity_uses_sparse_state_estimate() {
    let atom_count = 1_000;
    let budget = sparse_union_find_bytes(atom_count).saturating_mul(3);

    let global_capacity = threshold_batch_capacity(5, atom_count, 2, budget);
    let matrix_capacity = matrix_threshold_batch_capacity(5, atom_count, budget);

    assert!(matrix_capacity > global_capacity);
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
fn chain_matrix_reuse_plan_requires_combined_state_budget() {
    let atoms_by_chain = vec![vec![0; 10], vec![0; 20], vec![0; 30]];
    let matrix_bytes = chain_matrix_reuse_state_bytes(&atoms_by_chain);
    let global_bytes = threshold_state_bytes(60, 3);

    assert!(chain_matrix_reuse_plan(
        &atoms_by_chain,
        global_bytes + matrix_bytes - 1,
        global_bytes,
    )
    .is_none());
    assert!(chain_matrix_reuse_plan(
        &atoms_by_chain,
        global_bytes + matrix_bytes,
        global_bytes,
    )
    .is_some());
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
    progress.finish();
}

#[test]
fn auto_memory_plan_rejects_resident_atoms_over_budget() {
    let error =
        name_analysis_memory_plan(&[90.0], 1_000, 2, "1GB", None, 2 * 1024 * 1024 * 1024, 0)
            .unwrap_err();

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
                    name_pair_score_from_names(
                        &atoms[left].name_norm,
                        &atoms[right].name_norm,
                    ) >= threshold
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
fn name_candidate_scratch_dense_and_sparse_backends_agree() {
    let mut atoms = ["azuki", "azukii", "azkui", "aaaaba", "金色dragon", "金色dragons"]
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
            let right_min_len = Some(atoms[left + 1].char_len);
            let mut dense = NameCandidateScratch::new(atoms.len());
            let dense_cands = candidate_index
                .candidates_for_left(&atoms, left, right_min_len, threshold, &mut dense)
                .to_vec();
            // Force the sparse HashSet backend without allocating a huge
            // dense array: an atom_count above the threshold selects Sparse.
            let mut sparse = NameCandidateScratch::new(SPARSE_DEDUP_ATOM_THRESHOLD + 1);
            let sparse_cands = candidate_index
                .candidates_for_left(&atoms, left, right_min_len, threshold, &mut sparse)
                .to_vec();
            assert_eq!(
                dense_cands, sparse_cands,
                "left={left}, threshold={threshold}"
            );
        }
    }
}
