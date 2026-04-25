#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_name_pair_scoring_keeps_only_threshold_matches() {
        let atoms = vec![
            NameAtom {
                chain_index: 0,
                name_norm: "azuki".into(),
                contract_count: 1,
                nft_count: 1,
            },
            NameAtom {
                chain_index: 1,
                name_norm: "azuki".into(),
                contract_count: 1,
                nft_count: 1,
            },
            NameAtom {
                chain_index: 1,
                name_norm: "moonbirds".into(),
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
    fn auto_memory_plan_reserves_duckdb_only_after_chain_matrix_reuse_need() {
        let atoms_by_chain = vec![vec![0; 10_000], vec![0; 10_000], vec![0; 10_000]];
        let atom_count = atoms_by_chain.iter().map(Vec::len).sum();
        let threshold_count = 3;
        let global_bytes = threshold_state_bytes(atom_count, atoms_by_chain.len());
        let matrix_bytes = chain_matrix_reuse_state_bytes(&atoms_by_chain);
        let state_need = global_bytes.saturating_add(matrix_bytes);
        let total_budget = state_need.saturating_mul(threshold_count).saturating_mul(2);
        let memory_limit = format_byte_size(total_budget);

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

        assert!(plan.duckdb_bytes <= total_budget.saturating_sub(state_need * threshold_count));
    }

    #[test]
    fn default_memory_budget_is_auto_balanced_between_duckdb_and_rust() {
        let small =
            name_analysis_memory_plan(&[90.0, 95.0, 98.0], 1_000, 1, "10GB", None, 0, 0).unwrap();
        let large =
            name_analysis_memory_plan(&[90.0, 95.0, 98.0], 20_000_000, 2, "10GB", None, 0, 0)
                .unwrap();

        assert_eq!(small.analysis_bytes, 10 * 1024 * 1024 * 1024);
        assert_eq!(large.analysis_bytes, 10 * 1024 * 1024 * 1024);
        assert!(large.duckdb_bytes < small.duckdb_bytes);
    }

    #[test]
    fn explicit_analysis_memory_limit_stays_inside_total_budget() {
        let plan =
            name_analysis_memory_plan(&[90.0, 95.0, 98.0], 1_000, 2, "10GB", Some("16KB"), 0, 0)
                .unwrap();

        assert!(plan.duckdb_bytes < 10 * 1024 * 1024 * 1024);
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

        assert_eq!(auto_plan.duckdb_bytes, default_plan.duckdb_bytes);
        assert_eq!(auto_plan.analysis_bytes, default_plan.analysis_bytes);
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
    fn duckdb_limit_is_capped_by_process_headroom() {
        let limit = duckdb_memory_limit_from_process_budget(10_000, 4_000, 9_000).unwrap();

        assert_eq!(limit, 6_000);
    }

    #[test]
    fn duckdb_limit_rejects_exhausted_process_budget() {
        let error = duckdb_memory_limit_from_process_budget(10_000, 10_000, 9_000).unwrap_err();

        assert!(error.to_string().contains("process RSS"));
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
}
