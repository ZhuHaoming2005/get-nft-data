use super::*;

#[test]
fn representative_fallback_uses_requested_recall_mode() {
    let source = include_str!("../mod.rs");
    let start = source
        .find("let maximum_fallback_working_bytes")
        .expect("representative fallback setup");
    let end = source[start..]
        .find("progress.step_stage(\"matched representative metadata fallback\")")
        .map(|offset| start + offset)
        .expect("representative fallback completion");
    let fallback = &source[start..end];

    assert!(fallback.contains("recall_mode: metadata_recall_mode"));
    assert!(!fallback.contains("recall_mode: MetadataRecallMode::Exact"));
}

#[test]
fn normalized_metadata_bm25_path_skips_redundant_normalization() {
    let raw = "  ＧＯＬＤ\tDragon  gold ";
    let normalized = normalize_metadata_text(raw);
    let ordinary = MetadataBm25Document::from_text(raw).unwrap();
    let pre_normalized = MetadataBm25Document::from_normalized_text(&normalized).unwrap();

    assert_eq!(ordinary.len(), pre_normalized.len());
    assert_eq!(ordinary.unique_len(), pre_normalized.unique_len());
    assert_eq!(ordinary.term_frequency("gold"), 2);
    assert_eq!(ordinary.term_frequency("dragon"), 1);
    assert_eq!(pre_normalized.term_frequency("gold"), 2);
    assert_eq!(pre_normalized.term_frequency("dragon"), 1);

    let source = include_str!("../parse.rs");
    let start = source
        .find("pub(super) fn metadata_bm25_tokens_from_normalized")
        .expect("normalized tokenizer");
    let end = source[start..]
        .find("\n}")
        .map(|offset| start + offset)
        .expect("normalized tokenizer end");
    assert!(!source[start..end].contains("normalize_metadata_text"));
}

#[test]
fn metadata_bm25_document_stores_one_sorted_term_entry_per_unique_token() {
    let document = MetadataBm25Document::from_text("gold dragon gold ＧＯＬＤ").unwrap();

    assert_eq!(document.len(), 4);
    assert_eq!(document.unique_len(), 2);
    assert_eq!(
        document.terms(),
        &[("dragon".to_string(), 1), ("gold".to_string(), 3)]
    );

    let source = include_str!("../bm25.rs");
    let start = source
        .find("pub(crate) struct MetadataBm25Document")
        .unwrap();
    let end = source[start..]
        .find("\n}")
        .map(|offset| start + offset)
        .unwrap();
    let declaration = &source[start..end];
    assert!(declaration.contains("len: usize"));
    assert!(declaration.contains("terms: Vec<(String, usize)>"));
    assert!(!declaration.contains("tokens:"));
    assert!(!declaration.contains("unique_tokens:"));
    assert!(!declaration.contains("HashMap"));
}

#[test]
fn interned_metadata_source_doc_keeps_one_compact_sorted_term_vector() {
    let source = include_str!("../bm25.rs");
    let start = source
        .find("pub(super) struct InternedMetadataSourceDoc")
        .unwrap();
    let end = source[start..]
        .find("\n}")
        .map(|offset| start + offset)
        .unwrap();
    let declaration = &source[start..end];

    assert!(declaration.contains("len: usize"));
    assert!(declaration.contains("terms: Vec<(u32, u32)>"));
    assert!(!declaration.contains("HashMap"));
    assert!(!declaration.contains("unique_tokens"));
}

#[test]
fn compact_metadata_content_document_uses_u32_term_frequencies() {
    let source = include_str!("../bm25.rs");
    let start = source
        .find("pub(super) struct CompactMetadataContentDocument")
        .unwrap();
    let end = source[start..]
        .find("\n}")
        .map(|offset| start + offset)
        .unwrap();
    let declaration = &source[start..end];

    assert!(declaration.contains("terms: Vec<(u32, u32)>"));
    assert!(!declaration.contains("terms: Vec<(u32, usize)>"));
}

#[test]
fn retained_metadata_tokens_use_csr_without_a_global_sql_sort() {
    let load_source = include_str!("../load.rs");
    let load_start = load_source
        .find("pub(super) fn load_metadata_contract_tokens")
        .unwrap();
    let loader = &load_source[load_start..];
    assert!(!loader.contains("ORDER BY contract_index, token_index"));
    assert!(loader.contains("counts_and_cursors"));
    assert!(loader.contains("sort_compact_contract_token_slices"));
    assert!(loader.contains("rayon::join"));
    assert!(!loader.contains("Vec<Vec<u32>>"));

    let index_source = INDEX_SOURCE;
    let union_start = index_source
        .find("pub(in super::super) fn union_metadata_token_content_matches")
        .unwrap();
    let union_end = index_source[union_start..]
        .find("pub(in super::super) fn metadata_token_content_rows_sql")
        .map(|offset| union_start + offset)
        .unwrap();
    assert!(!index_source[union_start..union_end].contains("WITH shared_tokens"));
}

#[test]
fn metadata_shared_token_groups_prepare_in_bounded_parallel_batches() {
    let source = INDEX_SOURCE;
    let prepare_start = source
        .find("fn prepare_metadata_token_group_batch")
        .expect("parallel token-group preparation helper");
    let prepare_end = source[prepare_start..]
        .find("pub(in super::super) fn union_metadata_token_content_matches")
        .map(|offset| prepare_start + offset)
        .expect("token-group stream follows preparation helper");
    let prepare = &source[prepare_start..prepare_end];

    assert!(prepare.contains("par_iter_mut()"));
    assert!(prepare.contains("flush_raw(context"));
    assert!(prepare.contains("for group in groups.drain(..)"));
    assert!(prepare.contains("group.union_with_budget"));
    assert!(prepare.contains("remaining_prepared_bytes"));
    assert!(prepare.contains("saturating_sub(remaining_prepared_bytes)"));

    let stream_start = prepare_end;
    let stream_end = source[stream_start..]
        .find("pub(in super::super) fn metadata_token_content_rows_sql")
        .map(|offset| stream_start + offset)
        .expect("token content SQL follows stream");
    let stream = &source[stream_start..stream_end];
    assert!(stream.contains("METADATA_TOKEN_GROUP_BATCH_MULTIPLIER"));
    assert!(stream.contains("parallel_prepare_bytes"));
}

#[test]
fn large_shared_token_groups_generate_candidates_in_bounded_parallel_waves() {
    let source = INDEX_SOURCE;
    let collector_start = source
        .find("fn collect_metadata_left_candidate_wave")
        .unwrap();
    let collector_end = source[collector_start..]
        .find("fn consume_metadata_left_candidate_wave")
        .map(|offset| collector_start + offset)
        .unwrap();
    let collector = &source[collector_start..collector_end];
    assert!(collector.contains("par_iter()"));
    assert!(collector.contains(".copied()"));
    assert!(collector.contains("map_init("));

    let core_start = source
        .find("fn union_metadata_shared_token_atom_core")
        .unwrap();
    let core_end = source[core_start..]
        .find("pub(in super::super) fn union_metadata_no_common_content_candidates")
        .map(|offset| core_start + offset)
        .unwrap();
    let core = &source[core_start..core_end];
    assert!(core.contains("METADATA_PARALLEL_LEFT_WAVE_MULTIPLIER"));
    assert!(core.contains("collect_metadata_left_candidate_wave"));
    assert!(core.contains("consume_metadata_left_candidate_wave"));
}

#[test]
fn compact_template_bidirectional_score_matches_two_directional_scores() {
    let index = InternedMetadataIndex::from_source_doc_entries(vec![
        metadata_doc_entry("gold dragon alpha omega"),
        metadata_doc_entry("dragon gold alpha"),
        metadata_doc_entry("silver dragon alpha beta gamma"),
        metadata_doc_entry("unrelated isolated metadata"),
    ]);

    for left in 0..index.doc_count() {
        for right in 0..index.doc_count() {
            let expected = (
                index.scoring.score(left, right),
                index.scoring.score(right, left),
            );
            let actual = index.scoring.score_bidirectional(left, right);
            assert_eq!(actual, expected, "template pair {left}-{right}");
        }
    }

    let source = INDEX_SOURCE;
    let start = source
        .find("impl<'a> MetadataTemplateCompatibility<'a>")
        .unwrap();
    let end = source[start..]
        .find("\n}\n")
        .map(|offset| start + offset)
        .unwrap();
    let implementation = &source[start..end];
    assert!(implementation.contains("score_bidirectional"));
    assert!(!implementation.contains("scoring.score(left, right)"));
    assert!(!implementation.contains("scoring.score(right, left)"));
}

#[test]
fn metadata_cross_connection_check_short_circuits_before_matrix_lookup() {
    let source = INDEX_SOURCE;
    let start = source
        .find("pub(in super::super) fn metadata_pair_already_connected")
        .unwrap();
    let end = source[start..]
        .find("pub(in super::super) fn lexical_metadata_token_ids")
        .map(|offset| start + offset)
        .unwrap();
    let implementation = &source[start..end];
    assert!(implementation.contains("state.intra.connected(left, right)"));
    assert!(!implementation.contains("state.intra.find(left) == state.intra.find(right)"));
    let cross = implementation.find("let cross_connected").unwrap();
    let early_return = implementation
        .find("if !cross_connected")
        .expect("cross=false early return");
    let matrix = implementation.find("let matrix_connected").unwrap();
    assert!(cross < early_return && early_return < matrix);
}

#[test]
fn shared_token_candidate_generation_overlaps_the_next_wave_with_consumption() {
    let source = INDEX_SOURCE;
    let start = source
        .find("fn union_metadata_shared_token_atom_core")
        .unwrap();
    let end = source[start..]
        .find("pub(in super::super) fn union_metadata_no_common_content_candidates")
        .map(|offset| start + offset)
        .unwrap();
    let implementation = &source[start..end];

    assert!(implementation.contains("rayon::join"));
    assert!(implementation.contains("next_left_batches"));
}

#[test]
fn production_metadata_path_does_not_materialize_global_template_match_pairs() {
    let source = include_str!("../mod.rs");
    let analysis_start = source.find("pub(super) fn run_metadata_analysis").unwrap();
    let analysis_end = source[analysis_start..]
        .find("fn scalar_u64")
        .map(|offset| analysis_start + offset)
        .unwrap();
    let analysis = &source[analysis_start..analysis_end];

    assert!(!analysis.contains("collect_metadata_template_matches"));
    assert!(!analysis.contains("MetadataTemplateMatches"));
    assert!(!analysis.contains("metadata_template_match_pair_budget"));
}

#[test]
fn metadata_analysis_workers_have_an_explicit_production_stack_budget() {
    let types_source = include_str!("../types.rs");
    let source = include_str!("../mod.rs");
    let analysis_start = source.find("pub(super) fn run_metadata_analysis").unwrap();
    let analysis_end = source[analysis_start..]
        .find("fn scalar_u64")
        .map(|offset| analysis_start + offset)
        .unwrap();
    let analysis = &source[analysis_start..analysis_end];

    assert!(types_source
        .contains("const METADATA_ANALYSIS_WORKER_STACK_BYTES: usize = 16 * 1024 * 1024;"));
    assert!(analysis.contains(".stack_size(METADATA_ANALYSIS_WORKER_STACK_BYTES)"));
    assert!(analysis.contains(".thread_name(|index| format!(\"metadata-{index}\"))"));
}

#[test]
fn template_compatibility_is_scored_inside_parallel_content_batches() {
    let source = INDEX_SOURCE;
    let shared_start = source
        .find("fn union_metadata_shared_token_atom_core")
        .unwrap();
    let fallback_end = source[shared_start..]
        .find("pub(in super::super) fn union_metadata_content_candidates")
        .map(|offset| shared_start + offset)
        .unwrap();
    let union_loops = &source[shared_start..fallback_end];
    assert!(!union_loops.contains("template_compatibility.matches("));

    let batch_start = source
        .find("fn score_and_apply_metadata_atom_pair_batch")
        .unwrap();
    let batch_end = source[batch_start..]
        .find("fn score_and_apply_metadata_fallback_atom_pair_batch")
        .map(|offset| batch_start + offset)
        .unwrap();
    assert!(source[batch_start..batch_end].contains("template_compatibility"));
}

#[test]
fn production_shared_atom_unions_do_not_merge_member_vectors() {
    let source = INDEX_SOURCE;
    let start = source
        .find("pub(in super::super) fn score_and_apply_metadata_atom_pair_batch")
        .unwrap();
    let end = source[start..]
        .find("pub(in super::super) fn score_and_apply_metadata_fallback_atom_pair_batch")
        .map(|offset| start + offset)
        .unwrap();
    let shared_batch = &source[start..end];

    assert!(!shared_batch.contains("Vec::with_capacity"));
    assert!(!shared_batch.contains("extend_from_slice"));
    assert!(shared_batch.contains("apply_metadata_atom_pair_union"));
}

#[test]
fn template_score_cache_pool_is_reused_across_shared_token_groups() {
    let source = INDEX_SOURCE;
    let stream_start = source
        .find("pub(in super::super) fn union_metadata_token_content_matches")
        .unwrap();
    let stream_end = source[stream_start..]
        .find("pub(in super::super) fn metadata_token_content_rows_sql")
        .map(|offset| stream_start + offset)
        .unwrap();
    assert!(source[stream_start..stream_end].contains("MetadataTemplateScoreCachePool::default()"));

    let core_start = source
        .find("fn union_metadata_shared_token_atom_core")
        .unwrap();
    let core_end = source[core_start..]
        .find("fn union_metadata_no_common_atom_core")
        .map(|offset| core_start + offset)
        .unwrap();
    assert!(!source[core_start..core_end].contains("MetadataTemplateScoreCachePool::default()"));
}

#[test]
fn template_score_cache_is_symmetric_and_avoids_repeat_bm25_calls() {
    let index = InternedMetadataIndex::from_source_doc_entries(vec![
        metadata_doc_entry("gold dragon rare"),
        metadata_doc_entry("gold dragon common"),
    ]);
    let compatibility = MetadataTemplateCompatibility::Scored(&index.scoring);
    let mut cache = MetadataTemplateScoreCache::default();

    let first = cache.evaluate(0, 1, compatibility);
    let repeated = cache.evaluate(0, 1, compatibility);
    let reversed = cache.evaluate(1, 0, compatibility);
    let identical = cache.evaluate(0, 0, compatibility);

    assert!(first.1 > 0);
    assert!(!first.2);
    assert_eq!(repeated, (first.0, first.1, true));
    assert_eq!(reversed, (first.0, first.1, true));
    assert_eq!(identical, (true, 0, false));

    let source = INDEX_SOURCE;
    let evaluation_start = source
        .find("fn collect_metadata_template_pair_evaluations")
        .unwrap();
    let evaluation_end = source[evaluation_start..]
        .find("impl MetadataValidatedPairBatch")
        .map(|offset| evaluation_start + offset)
        .unwrap();
    let evaluation = &source[evaluation_start..evaluation_end];
    assert!(evaluation.contains("cache.evaluate"));
    assert!(evaluation.contains("template_cache_pool.take()"));
    let collect_start = source
        .find("fn collect_metadata_validated_atom_pair_hits")
        .unwrap();
    let collect_end = source[collect_start..]
        .find("pub(in super::super) fn score_and_apply_metadata_atom_pair_batch")
        .map(|offset| collect_start + offset)
        .unwrap();
    assert!(source[collect_start..collect_end].contains("template_evaluations"));
}

#[test]
fn hot_candidate_buffer_pool_never_waits_on_a_contended_lock() {
    let source = INDEX_SOURCE;
    let start = source
        .find("impl MetadataCandidateBufferPool")
        .expect("candidate buffer pool implementation");
    let end = source[start..]
        .find("impl Drop for MetadataSparseCandidateBuffer")
        .map(|offset| start + offset)
        .expect("candidate buffer pool implementation end");
    let implementation = &source[start..end];

    assert!(implementation.contains("try_lock()"));
    assert!(!implementation.contains(".lock()"));
}

#[test]
fn template_candidate_postings_use_sparse_csr_and_reuse_planned_ranges() {
    let source = INDEX_SOURCE;
    let declaration_start = source
        .find("pub(super) struct MetadataTemplateCandidateIndex")
        .unwrap();
    let declaration_end = source[declaration_start..]
        .find("\n}")
        .map(|offset| declaration_start + offset)
        .unwrap();
    let declaration = &source[declaration_start..declaration_end];
    assert!(declaration.contains("MetadataSparseCandidatePostings"));
    assert!(!declaration.contains("Vec<(u32, MetadataDocIndex)>"));

    let implementation_start = source.find("impl MetadataLocalCandidateIndex").unwrap();
    let implementation_end = source[implementation_start..]
        .find("impl MetadataCandidateScratch")
        .map(|offset| implementation_start + offset)
        .unwrap();
    let implementation = &source[implementation_start..implementation_end];
    assert!(implementation.contains("plan_candidates_after"));
    assert!(implementation.contains("append_planned_candidates"));
    assert!(!implementation.contains("scan_cost_after"));
}

#[test]
fn production_candidates_prune_incompatible_templates_before_content_pairs() {
    let templates = [
        "alphaone uniquexa",
        "betatwo uniquexb",
        "gammathree uniquexc",
        "deltafour uniquexd",
    ];
    let contents = [
        "https ipfs common contenta",
        "https ipfs common contentb",
        "https ipfs common contentc",
        "https ipfs common contentd",
    ];
    let mut builder = MetadataDataBuilder::new(1);
    for (template, content) in templates.into_iter().zip(contents) {
        builder.merge_indexed_row(IndexedMetadataRow {
            chain_index: 0,
            nft_count: 1,
            content_doc: MetadataBm25Document::from_text(content).map(Arc::new),
            doc: MetadataBm25Document::from_text(template).unwrap().into(),
            doc_key: metadata_document_key(template),
        });
    }
    let data = builder.finish();
    let records = contents
        .into_iter()
        .enumerate()
        .map(|(contract_index, content)| MetadataContentRecord {
            contract_index: metadata_contract_index_from_usize(contract_index),
            doc: MetadataBm25Document::from_text(content).unwrap().into(),
        })
        .collect::<Vec<_>>();
    let contract_tokens = CompactContractTokens::from_nested(vec![vec![1]; records.len()]);
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap();
    let context = MetadataContentUnionContext {
        data: &data,
        template_compatibility: MetadataTemplateCompatibility::Scored(&data.metadata_index.scoring),
        contract_tokens: &contract_tokens,
        chain_count: 1,
        pool: &pool,
        recall_mode: MetadataRecallMode::Exact,
    };
    let mut state = MetadataUnionState {
        intra: UnionFind::new(records.len()),
        cross: None,
        chain_matrix: None,
    };

    let stats = union_metadata_content_candidates(
        &records,
        MetadataContentScope::SharedToken,
        &context,
        &mut state,
    );

    assert_eq!(stats.candidate_pairs, 0);
    assert_eq!(stats.template_candidate_pairs, 0);
    let source = INDEX_SOURCE;
    assert!(source.contains("MetadataTemplateCandidateIndex"));
}

#[test]
fn two_atom_groups_bypass_adaptive_index_construction() {
    let source = INDEX_SOURCE;
    for (start_marker, end_marker) in [
        (
            "fn union_metadata_shared_token_atom_core",
            "pub(in super::super) fn union_metadata_no_common_content_candidates",
        ),
        (
            "fn union_metadata_no_common_atom_core",
            "pub(in super::super) fn union_metadata_content_candidates",
        ),
    ] {
        let start = source.find(start_marker).unwrap();
        let end = source[start..]
            .find(end_marker)
            .map(|offset| start + offset)
            .unwrap();
        let implementation = &source[start..end];
        let direct_pair = implementation
            .find("atoms.len() == METADATA_DIRECT_ATOM_GROUP_SIZE")
            .expect("two-atom direct path");
        let adaptive_index = implementation
            .find("MetadataLocalCandidateIndex::from_atoms")
            .expect("adaptive index path");
        assert!(direct_pair < adaptive_index);
    }
}

#[test]
fn compact_metadata_pair_score_uses_one_linear_term_merge() {
    let source = include_str!("../bm25.rs");
    let start = source
        .find("pub(super) fn compact_metadata_content_pair_score")
        .unwrap();
    let end = source[start..]
        .find("\n}\n")
        .map(|offset| start + offset)
        .unwrap();
    let implementation = &source[start..end];

    assert!(implementation.contains("while left_index < left.terms.len()"));
    assert!(!implementation.contains("compact_metadata_single_document_score"));
    assert!(!implementation.contains("compact_metadata_content_term_frequency"));
    assert!(!implementation.contains("binary_search"));
    assert!(!implementation.contains(".ln()"));
}

#[test]
fn reused_metadata_cache_budget_does_not_rescan_all_entries_per_insert() {
    let source = include_str!("../load.rs");
    let start = source.find("fn append_reused_metadata_documents(").unwrap();
    let end = source[start..]
        .find("pub(super) fn load_metadata_data(")
        .map(|offset| start + offset)
        .unwrap();
    let loader = &source[start..end];
    let normalized = loader
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect::<String>();

    assert!(!loader.contains("reused_metadata_documents_memory_bytes(&documents)"));
    assert!(loader.contains("retained_payload_bytes"));
    assert!(!loader.contains("budget_exhausted"));
    assert!(normalized.contains("ifmaximum_cache_bytes==Some(0)||maximum_transient_bytes==0"));
    assert!(normalized
        .contains("ifprojected_bytes>maximum{documents.shrink_to_fit();returnOk(false);}"));
}

#[test]
fn metadata_content_candidate_index_uses_compact_csr_postings() {
    let records = vec![
        MetadataContentRecord {
            contract_index: 0,
            doc: MetadataBm25Document::from_text("gold dragon")
                .unwrap()
                .into(),
        },
        MetadataContentRecord {
            contract_index: 1,
            doc: MetadataBm25Document::from_text("gold cat").unwrap().into(),
        },
    ];
    let compact = CompactMetadataContentSet::from_records(&records);
    let index = MetadataContentCandidateIndex::new(&compact.docs);
    let entry_count = compact
        .docs
        .iter()
        .map(|doc| doc.terms.len())
        .sum::<usize>();
    let token_count = compact
        .docs
        .iter()
        .flat_map(|doc| doc.terms.iter().map(|&(token_id, _)| token_id as usize + 1))
        .max()
        .unwrap_or(0);

    assert_eq!(index.len(), entry_count);
    assert_eq!(index.offset_count(), token_count + 1);
    assert!(
        index.memory_bytes()
            <= entry_count * std::mem::size_of::<MetadataDocIndex>()
                + (token_count + 1) * std::mem::size_of::<u64>()
    );

    let source = INDEX_SOURCE;
    let scoring_peak = source
        .split("fn scoring_peak_bytes")
        .nth(1)
        .and_then(|tail| tail.split("fn ensure_within_memory_budget").next())
        .expect("metadata scoring peak implementation");
    assert!(scoring_peak.contains("2usize.saturating_mul(std::mem::size_of::<u64>())"));
}

#[test]
fn representative_fallback_builds_compact_atoms_without_owned_record_vector() {
    let source = INDEX_SOURCE;
    let start = source
        .find("pub(in super::super) fn union_metadata_representative_content_fallback")
        .unwrap();
    let end = source[start..]
        .find("pub(in super::super) fn apply_metadata_contract_pair_union")
        .map(|offset| start + offset)
        .unwrap();
    let implementation = &source[start..end];

    assert!(implementation.contains("CompactMetadataContentGroupBuilder"));
    assert!(!implementation.contains("collect::<Vec<_>>()"));
    assert!(!implementation.contains("METADATA_CONTENT_BUDGET_CHECK_INTERVAL"));
    let normalized = implementation
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect::<String>();
    assert!(normalized.contains(
        "builder.push_document(metadata_contract_index_from_usize(contract_index),document.as_ref(),context.data,Some(context.contract_tokens),);builder.ensure_within_memory_budget(0,maximum_working_bytes,context.pool.current_num_threads(),context.recall_mode,)?;"
    ));
}

#[test]
fn metadata_scoring_peak_accounts_for_fallback_acceleration_buffers() {
    let source = INDEX_SOURCE;
    let scoring_peak = source
        .split("fn scoring_peak_bytes")
        .nth(1)
        .and_then(|tail| tail.split("fn ensure_within_memory_budget").next())
        .expect("metadata scoring peak implementation");

    assert!(scoring_peak.contains("fallback_exclusion_index"));
    assert!(scoring_peak.contains("fallback_exclusion_scratch"));
    assert!(scoring_peak.contains("joint_candidate_index"));
    assert!(scoring_peak.contains("family_positions"));
    assert!(scoring_peak.contains("difficult_first_order"));
    assert!(scoring_peak.contains("calibration_graph_scratch"));
    assert!(scoring_peak.contains("exact_rescue_mask"));
}

#[test]
fn joint_band_base_probe_uses_direct_per_atom_posting_positions() {
    let source = INDEX_SOURCE;

    assert!(source.contains("posting_positions_by_atom"));
    assert!(source.contains("posting_range_after_own_bucket"));
    assert!(source.contains("template_probe == template_value"));
    assert!(source.contains("content_probe == content_value"));
}

#[test]
fn metadata_left_root_cache_is_invalidated_after_each_union_batch() {
    let source = INDEX_SOURCE;
    let start = source
        .find("impl MetadataLeftCandidateBatchConsumer")
        .expect("metadata left candidate consumer");
    let consumer = &source[start..];

    assert!(consumer.contains("connected_with_left_root"));
    assert!(consumer.contains("intra_left_root = None"));
}

#[test]
fn metadata_candidate_scratch_pool_releases_lock_before_cold_allocation() {
    let source = INDEX_SOURCE;
    let take_start = source
        .find("pub(in super::super) fn take(&self) -> MetadataCandidateScratchLease<'_>")
        .unwrap();
    let take_end = source[take_start..]
        .find("impl std::ops::Deref for MetadataCandidateScratchLease")
        .map(|offset| take_start + offset)
        .unwrap();
    let take = &source[take_start..take_end];

    let pop = take.find(".pop()").unwrap();
    let cold_allocation = take.find("MetadataCandidateScratch::new").unwrap();
    assert!(take[pop..cold_allocation].contains("};"));
}

#[test]
fn parallel_left_waves_reuse_candidate_scratch_pool_across_calls() {
    let source = INDEX_SOURCE;
    let wave_start = source
        .find("fn collect_metadata_left_candidate_wave")
        .unwrap();
    let wave_end = source[wave_start..]
        .find("fn consume_metadata_left_candidate_wave")
        .map(|offset| wave_start + offset)
        .unwrap();
    let wave = &source[wave_start..wave_end];
    assert!(wave.contains("scratch_pool: &MetadataCandidateScratchPool"));
    assert!(wave.contains("|| scratch_pool.take()"));
    assert!(!wave.contains("MetadataCandidateScratch::new(atoms.len())"));

    let core_start = source
        .find("fn union_metadata_shared_token_atom_core")
        .unwrap();
    let core = &source[core_start..];
    assert!(core
        .contains("let candidate_scratch_pool = MetadataCandidateScratchPool::new(atoms.len())"));
    assert!(core.matches("&candidate_scratch_pool").count() >= 2);
}

#[test]
fn conservative_calibration_streams_candidate_sets_into_one_reused_score_batch() {
    let source = INDEX_SOURCE;
    let start = source
        .find("fn calibrate_metadata_conservative_recall")
        .expect("conservative calibration implementation");
    let end = source[start..]
        .find("impl MetadataSharedTokenGroupProgress")
        .map(|offset| start + offset)
        .expect("conservative calibration implementation end");
    let implementation = &source[start..end];

    assert!(!implementation.contains("candidates.iter().collect::<Vec<_>>()"));
    assert!(!implementation.contains("estimated_posting_visits_by_left: None"));
    assert!(implementation.contains("let mut score_pairs = Vec::with_capacity"));
    assert!(implementation.matches("&mut score_pairs").count() >= 2);
}

#[test]
fn shared_token_collection_skips_fallback_token_group_filter_entirely() {
    let source = INDEX_SOURCE;
    let start = source
        .find("fn collect_metadata_left_candidate_batch")
        .expect("candidate collector");
    let end = source[start..]
        .find("fn collect_metadata_left_candidate_wave")
        .map(|offset| start + offset)
        .expect("candidate collector end");
    let collector = &source[start..end];
    let fallback_guard = collector
        .find("if matches!(collection.scope, MetadataCandidateUnionScope::Fallback)")
        .expect("fallback scope guard");
    let token_group_filter = collector
        .find("atoms_have_disjoint_token_groups")
        .expect("token group filter");

    assert!(fallback_guard < token_group_filter);
}

#[test]
fn fallback_exclusion_bitmap_is_prepared_only_after_dimension_filtering() {
    let source = INDEX_SOURCE;
    let start = source
        .find("fn collect_metadata_left_candidate_batch")
        .expect("candidate collector");
    let end = source[start..]
        .find("fn collect_metadata_left_candidate_wave")
        .map(|offset| start + offset)
        .expect("candidate collector end");
    let collector = &source[start..end];
    let dimension_filter = collector
        .find("metadata_candidate_intersects_both_dimensions")
        .expect("dimension filter");
    let exclusion_prepare = collector
        .find("prepare_left")
        .expect("fallback exclusion preparation");

    assert!(dimension_filter < exclusion_prepare);
}

#[test]
fn representative_fallback_reuses_bounded_parallel_left_waves() {
    let source = INDEX_SOURCE;
    let start = source
        .find("fn union_metadata_no_common_atom_core")
        .expect("representative fallback core");
    let end = source[start..]
        .find("pub(in super::super) fn union_metadata_content_candidates")
        .map(|offset| start + offset)
        .expect("representative fallback core end");
    let core = &source[start..end];

    assert!(core.contains("MetadataCandidateScratchPool::new(atoms.len())"));
    assert!(core.contains("collect_metadata_left_candidate_wave"));
    assert!(core.contains("consume_metadata_left_candidate_wave"));
    assert!(core.contains("METADATA_PARALLEL_LEFT_WAVE_MULTIPLIER"));
}

#[test]
fn representative_fallback_calibration_is_posting_work_bounded() {
    let source = INDEX_SOURCE;
    assert!(source.contains("fn metadata_conservative_calibration_plan_with_work_budget"));
    assert!(source.contains("estimate_exact_posting_visits"));
    assert!(source.contains("plan_metadata_calibration_work_items"));
    assert!(source.contains("METADATA_CONSERVATIVE_CALIBRATION_MAX_POSTING_VISITS"));

    let start = source
        .find("fn union_metadata_no_common_atom_core")
        .expect("representative fallback core");
    let end = source[start..]
        .find("pub(in super::super) fn union_metadata_content_candidates")
        .map(|offset| start + offset)
        .expect("representative fallback core end");
    let core = &source[start..end];
    assert!(core.contains("metadata_conservative_calibration_plan_with_work_budget"));
}

#[test]
fn conservative_production_order_uses_effective_candidate_index_work() {
    assert_eq!(
        INDEX_SOURCE
            .matches("metadata_production_work_plan(")
            .count(),
        3,
        "the planner definition and both shared-token/fallback production paths must use effective-index work"
    );
}

#[test]
fn metadata_full_work_estimates_fill_the_retained_vector_in_parallel() {
    let source = INDEX_SOURCE;
    let start = source
        .find("fn metadata_exact_posting_visit_estimates")
        .expect("parallel posting work estimator");
    let end = source[start..]
        .find("fn metadata_production_posting_visit_estimates")
        .map(|offset| start + offset)
        .expect("parallel posting work estimator end");
    let estimator = &source[start..end];

    assert!(estimator.contains("pool.install"));
    assert!(estimator.contains("par_iter_mut()"));
    assert!(estimator.contains("map_init"));

    let order_start = source
        .find("fn metadata_difficult_first_left_order_with_pool")
        .expect("parallel difficult-first sorter");
    let order_end = source[order_start..]
        .find("fn plan_metadata_calibration_work_items")
        .map(|offset| order_start + offset)
        .expect("parallel difficult-first sorter end");
    let order = &source[order_start..order_end];
    assert!(order.contains("pool.install"));
    assert!(order.contains("par_sort_unstable_by"));
}

#[test]
fn representative_fallback_uses_single_calibration_then_bounded_exact_rescue() {
    let source = INDEX_SOURCE;
    let start = source
        .find("fn union_metadata_no_common_atom_core")
        .expect("representative fallback core");
    let end = source[start..]
        .find("pub(in super::super) fn union_metadata_content_candidates")
        .map(|offset| start + offset)
        .expect("representative fallback core end");
    let core = &source[start..end];

    assert_eq!(
        core.matches("calibrate_metadata_conservative_recall")
            .count(),
        1
    );
    let calibration = core
        .find("calibrate_metadata_conservative_recall")
        .expect("recall calibration");
    let rescue = core
        .find("plan_metadata_bounded_exact_rescue")
        .expect("bounded exact rescue");
    assert!(calibration < rescue);
    assert!(!core.contains("MetadataConservativeRecallProfile::Widened"));
}

#[test]
fn conservative_drift_uses_bounded_per_left_exact_rescue_instead_of_aborting() {
    let source = INDEX_SOURCE;
    assert!(source.contains("plan_metadata_bounded_exact_rescue"));
    assert!(source.matches("exact_recall_by_left").count() >= 2);
    assert!(!source.contains("metadata shared-token conservative recall drift exceeds limits"));
    assert!(!source.contains("metadata representative conservative recall drift exceeds limits"));
}

#[test]
fn compact_metadata_scoring_builds_flat_storage_without_nested_lists() {
    let source = include_str!("../bm25.rs");
    let start = source
        .find("impl CompactMetadataScoring")
        .expect("compact scoring implementation");
    let end = source[start..]
        .find("impl CompactMetadataPostings")
        .map(|offset| start + offset)
        .expect("compact postings implementation");
    let implementation = &source[start..end];

    assert!(implementation.contains("query_token_offsets"));
    assert!(implementation.contains("prepared_weight_values"));
    assert!(!implementation.contains("CompactMetadataPostings::from_nested(query_tokens)"));
    assert!(!implementation.contains("CompactF64Lists::from_nested(prepared_weights)"));
}

#[test]
fn compact_metadata_scoring_reuses_one_term_csr_for_queries_and_prepared_docs() {
    let source = include_str!("../bm25.rs");
    let declaration_start = source
        .find("pub(super) struct CompactMetadataScoring")
        .unwrap();
    let declaration_end = source[declaration_start..]
        .find("\n}")
        .map(|offset| declaration_start + offset)
        .unwrap();
    let declaration = &source[declaration_start..declaration_end];
    assert!(declaration.contains("query_tokens"));
    assert!(!declaration.contains("prepared_tokens"));

    let implementation_start = source.find("impl CompactMetadataScoring").unwrap();
    let implementation_end = source[implementation_start..]
        .find("impl CompactMetadataPostings")
        .map(|offset| implementation_start + offset)
        .unwrap();
    let implementation = &source[implementation_start..implementation_end];
    assert!(!implementation.contains("prepared_token_offsets"));
    assert!(!implementation.contains("prepared_token_values"));
    assert!(!implementation.contains("prepared_tokens.bin"));
    assert!(implementation.contains("let right_tokens = self.query_tokens.posting(right)"));
}

#[test]
fn metadata_index_releases_source_build_state_before_compact_scoring() {
    let source = INDEX_SOURCE;
    let start = source
        .find("pub(super) fn from_source_doc_entries")
        .expect("metadata index builder");
    let implementation = &source[start..];
    let drop_entries = implementation
        .find("drop(entries)")
        .expect("source entries are released");
    let drop_source_docs = implementation
        .find("drop(source_docs)")
        .expect("interned source documents are released");
    let compact_scoring = implementation
        .find("CompactMetadataScoring::from_nested")
        .expect("compact scoring construction");

    assert!(drop_entries < compact_scoring);
    assert!(drop_source_docs < compact_scoring);
}

#[test]
fn releasing_reuse_cache_recomputes_a_larger_fallback_working_allowance() {
    let raw = format!(r#"{{"description":"{}"}}"#, "x".repeat(16 * 1024));
    let shared_content = Arc::new(MetadataBm25Document::from_text("gold dragon details").unwrap());
    let mut reused = ReusedMetadataDocuments::new();
    reused.insert(
        raw,
        ReusedMetadataDocument {
            prefilter: MetadataBm25Document::from_text(&"template ".repeat(1024)).map(Arc::new),
            content: Some(shared_content.clone()),
            doc_key: "cached-template".to_string(),
        },
    );
    let mut builder = MetadataDataBuilder::new(1);
    builder.merge_indexed_row(IndexedMetadataRow {
        chain_index: 0,
        nft_count: 1,
        content_doc: Some(shared_content),
        doc: MetadataBm25Document::from_text("gold dragon template")
            .unwrap()
            .into(),
        doc_key: metadata_document_key("gold dragon template"),
    });
    let mut data = builder.finish_with_reused_documents(reused);
    let before = metadata_resident_memory_bytes(&data, None, 1);
    let analysis_budget = before.saturating_add(4096);
    let shared_allowance = analysis_budget - before;

    drop(std::mem::take(&mut data.reused_documents));
    let after = metadata_resident_memory_bytes(&data, None, 1);
    let fallback_allowance = analysis_budget - after;

    assert!(data.contracts[0].content_doc.is_some());
    assert!(after < before);
    assert!(fallback_allowance > shared_allowance);

    let source = include_str!("../mod.rs");
    let release = source
        .find("drop(std::mem::take(&mut data.reused_documents))")
        .unwrap();
    let recompute = source[release..]
        .find("let fallback_resident_bytes")
        .map(|offset| release + offset)
        .unwrap();
    let fallback = source[recompute..]
        .find("maximum_fallback_working_bytes")
        .map(|offset| recompute + offset)
        .unwrap();
    assert!(release < recompute && recompute < fallback);
}

#[test]
fn metadata_worker_stack_is_subtracted_before_pool_and_working_budgets() {
    let source = include_str!("../mod.rs");
    let analysis_start = source.find("pub(super) fn run_metadata_analysis").unwrap();
    let analysis_end = source[analysis_start..]
        .find("fn scalar_u64")
        .map(|offset| analysis_start + offset)
        .unwrap();
    let analysis = &source[analysis_start..analysis_end];
    let structure_budget = analysis
        .find("metadata_structure_memory_budget_bytes")
        .expect("worker stacks must be subtracted into one structural budget");
    let pool_build = analysis.find("rayon::ThreadPoolBuilder::new()").unwrap();

    assert!(structure_budget < pool_build);
    assert!(analysis.contains("let analysis_memory_bytes ="));
    assert!(analysis
        .contains("metadata_structure_memory_budget_bytes(total_analysis_memory_bytes, threads)?"));
    assert!(analysis
        .contains("let maximum_shared_working_bytes = analysis_memory_bytes - resident_bytes"));
    assert!(analysis.contains(
        "let maximum_fallback_working_bytes = analysis_memory_bytes - fallback_resident_bytes"
    ));
}

#[test]
fn metadata_completion_labels_drift_as_a_pre_rescue_sample() {
    let source = include_str!("../mod.rs");

    assert!(source.contains("sample drift before rescue contracts/components"));
    assert!(!source.contains("; drift contracts/components"));
}

#[test]
fn metadata_index_is_bounded_before_contract_tokens_are_loaded() {
    let source = include_str!("../mod.rs");
    let reserve = source
        .find("metadata_contract_token_reserve_bytes(")
        .unwrap();
    let pre_token_check = source
        .find("metadata_pre_token_resident_budget_bytes(")
        .unwrap();
    let load_tokens = source
        .find("load_metadata_contract_tokens(conn, &data, &pool, Some(progress))")
        .unwrap();

    assert!(reserve < pre_token_check && pre_token_check < load_tokens);
}

#[test]
fn long_metadata_loaders_report_progress_for_each_arrow_batch() {
    let source = include_str!("../load.rs");
    for (start_marker, end_marker) in [
        (
            "pub(super) fn load_reused_metadata_documents(",
            "pub(super) fn load_metadata_data(",
        ),
        (
            "pub(super) fn load_metadata_data(",
            "fn index_metadata_load_chunk(",
        ),
        (
            "pub(super) fn load_metadata_contract_tokens(",
            "const CONTRACT_TOKEN_SORT_LEAF_CONTRACTS",
        ),
    ] {
        let start = source.find(start_marker).unwrap();
        let end = source[start..]
            .find(end_marker)
            .map(|offset| start + offset)
            .unwrap();
        let loader = &source[start..end];
        assert!(loader.contains("progress"));
        assert!(loader.contains("batch.num_rows() as u64"));
        assert!(loader.contains("advance_task"));
    }
}

#[test]
fn metadata_parallel_filter_keeps_an_indexed_output_before_flattening() {
    let source = include_str!("../load.rs");
    let start = source
        .find("pub(super) fn index_metadata_raw_row_chunk_with_cache")
        .unwrap();
    let end = source[start..]
        .find("pub(super) fn metadata_document_key")
        .map(|offset| start + offset)
        .unwrap();
    let implementation = &source[start..end];

    assert!(implementation.contains(".map("));
    assert!(!implementation.contains(".filter_map("));
}
