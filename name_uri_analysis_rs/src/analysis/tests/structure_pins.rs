use super::*;

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

    let source = include_str!("../name.rs");
    let budget = source.find("estimate_name_candidate_index_bytes").unwrap();
    let build = source.find("NameCandidateIndex::new").unwrap();
    assert!(budget < build);
}

#[test]
fn metadata_recall_mode_is_forwarded_to_both_analysis_entry_paths() {
    let source = include_str!("../../analysis.rs");
    assert!(
        source.contains("fn metadata_analysis_spec"),
        "both entry paths must share one metadata spec builder"
    );
    assert!(
        source.contains("recall_mode: options.metadata_recall_mode"),
        "metadata spec builder must forward the configured recall mode"
    );
    assert_eq!(
        source.matches("metadata_analysis_spec(").count(),
        2,
        "isolated and in-process metadata phases must use the shared builder"
    );
}

#[test]
fn persisted_metadata_sources_use_stable_file_and_row_ids() {
    let contracts = analysis_contracts_sql();
    let load_source = include_str!("../metadata/load.rs");
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

    let source = include_str!("../duckdb_prep.rs").to_ascii_uppercase();
    for table in ["SELECTED_CHAINS", "NAME_ATOMS"] {
        assert!(
            source.contains(&format!("CREATE OR REPLACE TABLE {table}")),
            "{table} must survive the prepare child process"
        );
    }
}

#[test]
fn final_outputs_use_partial_files_before_atomic_rename() {
    let source = include_str!("../output.rs");
    assert!(source.contains("summary.json.partial"));
    assert!(source.contains("summary.csv.partial"));
    assert!(source.contains("replace_file_atomically"));
    assert!(!source.contains("remove_file"));
}

#[test]
fn progress_text_refreshes_at_least_twenty_times_per_second() {
    assert!(PROGRESS_REFRESH_INTERVAL <= std::time::Duration::from_millis(50));
    let source = include_str!("../progress.rs");
    assert!(source.contains("enable_steady_tick(PROGRESS_REFRESH_INTERVAL)"));
}

#[test]
fn name_worker_stacks_are_reserved_inside_the_analysis_budget() {
    assert_eq!(
        name_worker_stack_reserve_bytes(4) - name_worker_stack_reserve_bytes(1),
        3 * NAME_ANALYSIS_WORKER_STACK_BYTES
    );
    let source = include_str!("../name.rs");
    let final_plan = source
        .rfind("let memory_plan = name_analysis_memory_plan")
        .unwrap();
    let pool_build = source.find("rayon::ThreadPoolBuilder::new()").unwrap();
    assert!(final_plan < pool_build);
    assert!(source.contains(".stack_size(NAME_ANALYSIS_WORKER_STACK_BYTES)"));
}

#[test]
fn canonical_name_lookup_borrows_source_names() {
    let source = include_str!("../name.rs");

    assert!(source.contains("HashMap::<&str, usize>::new()"));
    assert!(!source.contains("HashMap::<String, usize>::new()"));
}

#[test]
fn name_scoring_releases_candidate_state_before_summary_building() {
    let source = include_str!("../name.rs");
    let drop_index = source.find("drop(candidate_index)").unwrap();
    let drop_canonical = source.find("drop(canonical)").unwrap();
    let build_summary = source.find("push_name_summary_rows(").unwrap();

    assert!(drop_index < build_summary);
    assert!(drop_canonical < build_summary);
}

#[test]
fn name_summary_releases_chain_index_before_matrix_summary() {
    let source = include_str!("../name.rs");
    let drop_chain_index = source.find("drop(atoms_by_chain)").unwrap();
    let build_matrix_summary = source.find("push_reused_chain_matrix_rows(").unwrap();

    assert!(drop_chain_index < build_matrix_summary);
}

#[test]
fn name_summary_releases_global_dsu_before_matrix_summary() {
    let source = include_str!("../name.rs");
    let release_intra = source.find("state.intra = UnionFind::new(0)").unwrap();
    let release_cross = source.find("state.cross = None").unwrap();
    let build_matrix_summary = source.find("push_reused_chain_matrix_rows(").unwrap();

    assert!(release_intra < build_matrix_summary);
    assert!(release_cross < build_matrix_summary);
}

#[test]
fn production_name_scoring_streams_matches_into_bounded_edge_batches() {
    let source = include_str!("../name_scoring.rs");
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
    let source = include_str!("../name_scoring.rs");
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
fn name_candidate_postings_are_sliced_to_the_requested_right_range() {
    let source = include_str!("../name_scoring.rs");

    assert!(source.contains("posting.partition_point"));
    assert!(source.contains("right_range.start"));
    assert!(source.contains("right_range.end"));
}
