use super::super::analysis_contracts_sql;
use super::parse::*;
use super::*;
use crate::analysis::PipelineStage;

#[test]
fn metadata_bm25_tokens_use_unicode_letter_number_regex() {
    assert_eq!(
        metadata_bm25_tokens("金色 dragon"),
        vec!["金色".to_string(), "dragon".to_string()]
    );
    assert_eq!(
        metadata_bm25_tokens("gold_dragon"),
        vec!["gold_dragon".to_string()]
    );
    assert_eq!(metadata_bm25_tokens("a b cd"), vec!["cd".to_string()]);
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

    let source = include_str!("parse.rs");
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

    let source = include_str!("bm25.rs");
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
    let source = include_str!("bm25.rs");
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
    let source = include_str!("bm25.rs");
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
fn metadata_is_dedup_eligible_accepts_leading_whitespace_json() {
    assert!(metadata_is_dedup_eligible("  {\"a\":1}"));
    assert!(metadata_is_dedup_eligible("\n[1]"));
    assert!(!metadata_is_dedup_eligible("  x{}"));
}

#[test]
fn metadata_prefilter_document_matches_top_contract_template_semantics() {
    let document = metadata_prefilter_document_from_json(
        r#"{
            "name": "Seed #1",
            "description": "Shared Story",
            "attributes": [
                {"trait_type": "Background", "value": "Red"}
            ],
            "image": "ipfs://seed/1.png"
        }"#,
    );

    assert_eq!(
        document,
        "attributes background description image name shared story trait_type value"
    );
}

#[test]
fn metadata_token_rows_reuse_precomputed_eligibility() {
    let sql = metadata_contract_token_rows_sql();

    assert!(sql.contains("AND a.metadata_eligible"));
    assert!(!sql.contains("starts_with"));
    assert!(!sql.contains("length("));
    assert_eq!(sql.matches("arg_min(").count(), 1);
    assert!(sql.contains("struct_pack("));
    assert!(!sql.contains("dense_rank()"));
    assert!(!sql.contains("ORDER BY metadata.token_id"));
    assert!(sql.contains("metadata_retained_tokens"));
}

#[test]
fn metadata_token_content_stream_marks_loaded_representative_without_changing_payload() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
        CREATE TABLE analysis_contracts AS
        SELECT 0::BIGINT AS metadata_contract_index,
               7::UINTEGER AS metadata_source_file,
               11::UBIGINT AS metadata_source_row_number;
        CREATE TABLE metadata_contract_token_rows AS
        SELECT * FROM (VALUES
            (3::BIGINT, 0::BIGINT, 7::UINTEGER, 11::UBIGINT),
            (4::BIGINT, 0::BIGINT, 7::UINTEGER, 12::UBIGINT)
        ) rows(token_index, contract_index, metadata_source_file, metadata_source_row_number);
        CREATE TABLE metadata_rows AS
        SELECT * FROM (VALUES
            (7::UINTEGER, 11::UBIGINT, '{"description":"representative"}'),
            (7::UINTEGER, 12::UBIGINT, '{"description":"alternate"}')
        ) rows(source_file, source_row_number, metadata_json);
        "#,
    )
    .unwrap();

    let mut stmt = conn.prepare(metadata_token_content_rows_sql()).unwrap();
    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(
        rows,
        vec![
            (
                3,
                1,
                Some(r#"{"description":"representative"}"#.to_string())
            ),
            (4, 0, Some(r#"{"description":"alternate"}"#.to_string())),
        ]
    );
}

#[test]
fn retained_metadata_tokens_use_csr_without_a_global_sql_sort() {
    let load_source = include_str!("load.rs");
    let load_start = load_source
        .find("pub(super) fn load_metadata_contract_tokens")
        .unwrap();
    let loader = &load_source[load_start..];
    assert!(!loader.contains("ORDER BY contract_index, token_index"));
    assert!(loader.contains("counts_and_cursors"));
    assert!(loader.contains("sort_compact_contract_token_slices"));
    assert!(loader.contains("rayon::join"));
    assert!(!loader.contains("Vec<Vec<u32>>"));

    let index_source = include_str!("index.rs");
    let union_start = index_source
        .find("pub(super) fn union_metadata_token_content_matches")
        .unwrap();
    let union_end = index_source[union_start..]
        .find("fn metadata_content_document")
        .map(|offset| union_start + offset)
        .unwrap();
    assert!(!index_source[union_start..union_end].contains("WITH shared_tokens"));
}

#[test]
fn metadata_shared_token_groups_prepare_in_bounded_parallel_batches() {
    let source = include_str!("index.rs");
    let prepare_start = source
        .find("fn prepare_metadata_token_group_batch")
        .expect("parallel token-group preparation helper");
    let prepare_end = source[prepare_start..]
        .find("pub(super) fn union_metadata_token_content_matches")
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
        .find("pub(super) fn metadata_token_content_rows_sql")
        .map(|offset| stream_start + offset)
        .expect("token content SQL follows stream");
    let stream = &source[stream_start..stream_end];
    assert!(stream.contains("METADATA_TOKEN_GROUP_BATCH_MULTIPLIER"));
    assert!(stream.contains("parallel_prepare_bytes"));
}

#[test]
fn large_shared_token_groups_generate_candidates_in_bounded_parallel_waves() {
    let source = include_str!("index.rs");
    let collector_start = source
        .find("fn collect_metadata_left_candidate_wave")
        .unwrap();
    let collector_end = source[collector_start..]
        .find("fn consume_metadata_left_candidate_wave")
        .map(|offset| collector_start + offset)
        .unwrap();
    let collector = &source[collector_start..collector_end];
    assert!(collector.contains("into_par_iter()"));
    assert!(collector.contains("map_init("));

    let core_start = source
        .find("fn union_metadata_shared_token_atom_core")
        .unwrap();
    let core_end = source[core_start..]
        .find("pub(super) fn union_metadata_no_common_content_candidates")
        .map(|offset| core_start + offset)
        .unwrap();
    let core = &source[core_start..core_end];
    assert!(core.contains("METADATA_PARALLEL_LEFT_WAVE_MULTIPLIER"));
    assert!(core.contains("collect_metadata_left_candidate_wave"));
    assert!(core.contains("consume_metadata_left_candidate_wave"));
}

#[test]
fn parallel_shared_token_waves_preserve_single_thread_results_and_stats() {
    let record_count = METADATA_CONTENT_PARALLEL_MIN_RECORDS + 17;
    let mut builder = MetadataDataBuilder::new(2);
    let mut records = Vec::with_capacity(record_count);
    for index in 0..record_count {
        let content = format!("shared dragon collection unique{index}");
        let template = format!("shared template collection variant{}", index % 4);
        builder.merge_indexed_row(IndexedMetadataRow {
            chain_index: index % 2,
            nft_count: 1,
            content_doc: MetadataBm25Document::from_text(&content).map(Arc::new),
            doc: MetadataBm25Document::from_text(&template).unwrap().into(),
            doc_key: metadata_document_key(&template),
        });
        records.push(MetadataContentRecord {
            contract_index: metadata_contract_index_from_usize(index),
            doc: MetadataBm25Document::from_text(&content).unwrap().into(),
        });
    }
    let data = builder.finish();
    let contract_tokens = CompactContractTokens::from_nested(vec![vec![1]; record_count]);

    let run = |threads| {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()
            .unwrap();
        let context = MetadataContentUnionContext {
            data: &data,
            template_compatibility: MetadataTemplateCompatibility::Scored(
                &data.metadata_index.scoring,
            ),
            contract_tokens: &contract_tokens,
            chain_count: 2,
            pool: &pool,
            recall_mode: MetadataRecallMode::Exact,
        };
        let mut state = MetadataUnionState {
            intra: UnionFind::new(record_count),
            cross: Some(SparseUnionFind::default()),
            chain_matrix: Some(vec![SparseUnionFind::default()]),
        };
        let stats = union_metadata_content_candidates(
            &records,
            MetadataContentScope::SharedToken,
            &context,
            &mut state,
        );
        let intra = (0..record_count)
            .map(|index| state.intra.find(index))
            .collect::<Vec<_>>();
        let cross = (0..record_count)
            .flat_map(|left| (left + 1..record_count).map(move |right| (left, right)))
            .map(|(left, right)| state.cross.as_mut().unwrap().connected(left, right))
            .collect::<Vec<_>>();
        let matrix = (0..record_count)
            .flat_map(|left| (left + 1..record_count).map(move |right| (left, right)))
            .map(|(left, right)| state.chain_matrix.as_mut().unwrap()[0].connected(left, right))
            .collect::<Vec<_>>();
        (stats, intra, cross, matrix)
    };

    assert_eq!(run(1), run(4));
}

#[test]
fn shared_token_group_progress_combines_prior_and_live_counters() {
    let base = ProgressCounters {
        groups: 2,
        candidates: 100,
        scored: 50,
        matched: 4,
    };
    let live = MetadataContentUnionStats {
        atom_count: 10,
        candidate_pairs: 25,
        scored_pairs: 8,
        matched_pairs: 2,
        template_candidate_pairs: 0,
        template_scored_pairs: 0,
        template_matched_pairs: 0,
        ..MetadataContentUnionStats::default()
    };

    assert_eq!(
        metadata_shared_token_group_progress_counters(3, base, &live),
        ProgressCounters {
            groups: 3,
            candidates: 125,
            scored: 58,
            matched: 6,
        }
    );
}

#[test]
fn metadata_union_stats_preserve_candidate_filter_and_cache_diagnostics() {
    let mut stats = MetadataContentUnionStats {
        raw_candidate_pairs: 20,
        dimension_rejected_pairs: 8,
        already_connected_pairs: 3,
        ..MetadataContentUnionStats::default()
    };
    stats.accumulate_pair_scoring(MetadataPairScoringStats {
        template_candidate_pairs: 9,
        template_scored_pairs: 12,
        template_matched_pairs: 4,
        content_scored_pairs: 4,
        content_matched_pairs: 1,
        template_cache_hits: 5,
        template_cache_misses: 4,
        template_rejected_pairs: 5,
        ..MetadataPairScoringStats::default()
    });

    assert_eq!(stats.raw_candidate_pairs, 20);
    assert_eq!(stats.dimension_rejected_pairs, 8);
    assert_eq!(stats.already_connected_pairs, 3);
    assert_eq!(stats.template_cache_hits, 5);
    assert_eq!(stats.template_cache_misses, 4);
    assert_eq!(stats.template_rejected_pairs, 5);
}

#[test]
fn shared_token_groups_are_processed_smallest_first_with_stable_ties() {
    let sql = metadata_token_content_rows_sql();
    let normalized = sql.split_whitespace().collect::<Vec<_>>().join(" ");

    assert!(normalized.contains(
        "ORDER BY count(*) OVER (PARTITION BY t.token_index), t.token_index, t.contract_index"
    ));
    let prepare_sql = metadata_contract_token_rows_sql()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    assert!(prepare_sql.contains("row_number() OVER (ORDER BY token_id)"));
}

#[test]
fn template_score_cache_uses_available_memory_for_cross_batch_reuse() {
    let slots = std::hint::black_box(METADATA_TEMPLATE_SCORE_CACHE_SLOTS);
    let ways = std::hint::black_box(METADATA_TEMPLATE_SCORE_CACHE_WAYS);
    assert!(slots >= 256 * 1024);
    assert!(ways >= 4);
    assert_eq!(slots % ways, 0);
    assert!(MetadataTemplateScoreCache::memory_bytes() >= 128 * 1024);
    assert!(
        std::mem::size_of::<MetadataTemplateScoreCache>()
            < MetadataTemplateScoreCache::memory_bytes()
    );
}

#[test]
fn template_score_cache_is_reused_across_content_score_batches() {
    let index = InternedMetadataIndex::from_source_doc_entries(vec![
        metadata_doc_entry("gold dragon collection"),
        metadata_doc_entry("gold dragon collection variant"),
    ]);
    let records = (0..2)
        .map(|contract_index| MetadataContentRecord {
            contract_index,
            doc: MetadataBm25Document::from_text("gold dragon")
                .unwrap()
                .into(),
        })
        .collect::<Vec<_>>();
    let compact = CompactMetadataContentSet::from_records(&records);
    let atoms = (0..2)
        .map(|index| MetadataContentAtom {
            chain_index: 0,
            template_doc_index: metadata_doc_index_from_usize(index),
            representative_record_index: metadata_doc_index_from_usize(index),
            members: vec![metadata_contract_index_from_usize(index)],
            fallback_token_groups: Vec::new(),
        })
        .collect::<Vec<_>>();
    let pairs = vec![(0, metadata_doc_index_from_usize(1))];
    let rayon_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .unwrap();
    let cache_pool = MetadataTemplateScoreCachePool::default();

    let first = collect_metadata_validated_atom_pair_hits(
        &pairs,
        &atoms,
        &compact.docs,
        MetadataTemplateCompatibility::Scored(&index.scoring),
        &rayon_pool,
        &cache_pool,
    );
    let second = collect_metadata_validated_atom_pair_hits(
        &pairs,
        &atoms,
        &compact.docs,
        MetadataTemplateCompatibility::Scored(&index.scoring),
        &rayon_pool,
        &cache_pool,
    );

    assert!(first.stats.template_scored_pairs > 0);
    assert_eq!(first.stats.template_cache_misses, 1);
    assert_eq!(first.stats.template_cache_hits, 0);
    assert_eq!(second.stats.template_cache_misses, 0);
    assert_eq!(second.stats.template_cache_hits, 1);
    assert_eq!(
        second.stats.template_scored_pairs,
        first.stats.template_scored_pairs
    );
    assert_eq!(first.hits, second.hits);
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

    let source = include_str!("index.rs");
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
    let source = include_str!("index.rs");
    let start = source
        .find("pub(super) fn metadata_pair_already_connected")
        .unwrap();
    let end = source[start..]
        .find("pub(super) fn lexical_metadata_token_ids")
        .map(|offset| start + offset)
        .unwrap();
    let implementation = &source[start..end];
    let cross = implementation.find("let cross_connected").unwrap();
    let early_return = implementation
        .find("if !cross_connected")
        .expect("cross=false early return");
    let matrix = implementation.find("let matrix_connected").unwrap();
    assert!(cross < early_return && early_return < matrix);
}

#[test]
fn shared_token_candidate_generation_overlaps_the_next_wave_with_consumption() {
    let source = include_str!("index.rs");
    let start = source
        .find("fn union_metadata_shared_token_atom_core")
        .unwrap();
    let end = source[start..]
        .find("pub(super) fn union_metadata_no_common_content_candidates")
        .map(|offset| start + offset)
        .unwrap();
    let implementation = &source[start..end];

    assert!(implementation.contains("rayon::join"));
    assert!(implementation.contains("next_left_batches"));
}

#[test]
fn production_metadata_path_does_not_materialize_global_template_match_pairs() {
    let source = include_str!("mod.rs");
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
    let source = include_str!("mod.rs");
    let analysis_start = source.find("pub(super) fn run_metadata_analysis").unwrap();
    let analysis_end = source[analysis_start..]
        .find("fn scalar_u64")
        .map(|offset| analysis_start + offset)
        .unwrap();
    let analysis = &source[analysis_start..analysis_end];

    assert!(
        source.contains("const METADATA_ANALYSIS_WORKER_STACK_BYTES: usize = 16 * 1024 * 1024;")
    );
    assert!(analysis.contains(".stack_size(METADATA_ANALYSIS_WORKER_STACK_BYTES)"));
    assert!(analysis.contains(".thread_name(|index| format!(\"metadata-{index}\"))"));
}

#[test]
fn template_compatibility_is_scored_inside_parallel_content_batches() {
    let source = include_str!("index.rs");
    let shared_start = source
        .find("fn union_metadata_shared_token_atom_core")
        .unwrap();
    let fallback_end = source[shared_start..]
        .find("pub(super) fn union_metadata_content_candidates")
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
    let source = include_str!("index.rs");
    let start = source
        .find("pub(super) fn score_and_apply_metadata_atom_pair_batch")
        .unwrap();
    let end = source[start..]
        .find("pub(super) fn score_and_apply_metadata_fallback_atom_pair_batch")
        .map(|offset| start + offset)
        .unwrap();
    let shared_batch = &source[start..end];

    assert!(!shared_batch.contains("Vec::with_capacity"));
    assert!(!shared_batch.contains("extend_from_slice"));
    assert!(shared_batch.contains("apply_metadata_atom_pair_union"));
}

#[test]
fn template_score_cache_pool_is_reused_across_shared_token_groups() {
    let source = include_str!("index.rs");
    let stream_start = source
        .find("pub(super) fn union_metadata_token_content_matches")
        .unwrap();
    let stream_end = source[stream_start..]
        .find("pub(super) fn metadata_token_content_rows_sql")
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

    let source = include_str!("index.rs");
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
        .find("pub(super) fn score_and_apply_metadata_atom_pair_batch")
        .map(|offset| collect_start + offset)
        .unwrap();
    assert!(source[collect_start..collect_end].contains("template_evaluations"));
}

#[test]
fn score_batch_compacts_repeated_template_pairs_before_bm25() {
    let index = InternedMetadataIndex::from_source_doc_entries(vec![
        metadata_doc_entry("gold dragon collection"),
        metadata_doc_entry("gold dragon collection variant"),
    ]);
    let pair_count = METADATA_CONTENT_PARALLEL_MIN_RECORDS * 4;
    let mut atoms = Vec::with_capacity(pair_count * 2);
    let mut records = Vec::with_capacity(pair_count * 2);
    let mut pairs = Vec::with_capacity(pair_count);
    for pair_index in 0..pair_count {
        let left = atoms.len();
        for template_doc_index in [0usize, 1usize] {
            records.push(MetadataContentRecord {
                contract_index: metadata_contract_index_from_usize(atoms.len()),
                doc: MetadataBm25Document::from_text("gold dragon content")
                    .unwrap()
                    .into(),
            });
            atoms.push(MetadataContentAtom {
                chain_index: 0,
                template_doc_index: metadata_doc_index_from_usize(template_doc_index),
                representative_record_index: metadata_doc_index_from_usize(atoms.len()),
                members: vec![metadata_contract_index_from_usize(atoms.len())],
                fallback_token_groups: Vec::new(),
            });
        }
        pairs.push((left, metadata_doc_index_from_usize(left + 1)));
        assert_eq!(pairs.len(), pair_index + 1);
    }
    let compact = CompactMetadataContentSet::from_records(&records);
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap();
    let cache_pool = MetadataTemplateScoreCachePool::default();

    let batch = collect_metadata_validated_atom_pair_hits(
        &pairs,
        &atoms,
        &compact.docs,
        MetadataTemplateCompatibility::Scored(&index.scoring),
        &pool,
        &cache_pool,
    );

    assert_eq!(batch.stats.template_batch_unique_pairs, 1);
    assert_eq!(
        batch.stats.template_batch_reused_pairs,
        (pair_count - 1) as u64
    );
    assert_eq!(batch.stats.template_cache_misses, 1);
}

#[test]
fn score_batch_skips_template_pair_sort_for_unique_heavy_batches() {
    let pair_count = METADATA_CONTENT_PARALLEL_MIN_RECORDS * 4;
    let entries = (0..pair_count * 2)
        .map(|index| metadata_doc_entry(&format!("unique template identity {index}")))
        .collect();
    let index = InternedMetadataIndex::from_source_doc_entries(entries);
    let mut atoms = Vec::with_capacity(pair_count * 2);
    let mut records = Vec::with_capacity(pair_count * 2);
    let mut pairs = Vec::with_capacity(pair_count);
    for pair_index in 0..pair_count {
        let left = atoms.len();
        for template_doc_index in [left, left + 1] {
            records.push(MetadataContentRecord {
                contract_index: metadata_contract_index_from_usize(atoms.len()),
                doc: MetadataBm25Document::from_text("shared content")
                    .unwrap()
                    .into(),
            });
            atoms.push(MetadataContentAtom {
                chain_index: 0,
                template_doc_index: metadata_doc_index_from_usize(template_doc_index),
                representative_record_index: metadata_doc_index_from_usize(atoms.len()),
                members: vec![metadata_contract_index_from_usize(atoms.len())],
                fallback_token_groups: Vec::new(),
            });
        }
        pairs.push((left, metadata_doc_index_from_usize(left + 1)));
        assert_eq!(pairs.len(), pair_index + 1);
    }
    let compact = CompactMetadataContentSet::from_records(&records);
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap();
    let cache_pool = MetadataTemplateScoreCachePool::default();

    let batch = collect_metadata_validated_atom_pair_hits(
        &pairs,
        &atoms,
        &compact.docs,
        MetadataTemplateCompatibility::Scored(&index.scoring),
        &pool,
        &cache_pool,
    );

    assert_eq!(batch.stats.template_batch_unique_pairs, 0);
    assert_eq!(batch.stats.template_batch_reused_pairs, 0);
    assert!(batch.stats.template_cache_misses > 0);
}

#[test]
fn dense_candidate_intersection_keeps_the_exact_two_dimension_pair_set() {
    let atom_count = METADATA_DENSE_INTERSECTION_MIN_SCAN_COST + 2;
    let mut builder = MetadataDataBuilder::new(1);
    let mut records = Vec::with_capacity(atom_count);
    for index in 0..atom_count {
        let content = format!("shared content{index}");
        builder.merge_indexed_row(IndexedMetadataRow {
            chain_index: 0,
            nft_count: 1,
            content_doc: MetadataBm25Document::from_text(&content).map(Arc::new),
            doc: MetadataBm25Document::from_text("shared template identity")
                .unwrap()
                .into(),
            doc_key: metadata_document_key("shared template identity"),
        });
        records.push(MetadataContentRecord {
            contract_index: metadata_contract_index_from_usize(index),
            doc: MetadataBm25Document::from_text(&content).unwrap().into(),
        });
    }
    let data = builder.finish();
    let compact = CompactMetadataContentSet::from_records(&records);
    let atoms = build_metadata_content_atoms(&records, &compact.docs, &data);
    let compatibility = MetadataTemplateCompatibility::Scored(&data.metadata_index.scoring);
    let candidate_index =
        MetadataLocalCandidateIndex::from_atoms(&compact.docs, &atoms, compatibility, false);
    let mut scratch = MetadataCandidateScratch::new(atoms.len());
    scratch.clear_for_next_left();

    let basis = candidate_index.append_candidates_after(
        0,
        &atoms[0],
        &compact.docs[0],
        compatibility,
        &mut scratch,
    );
    let actual = scratch
        .candidates
        .iter()
        .copied()
        .collect::<std::collections::HashSet<_>>();
    let expected = (1..atoms.len())
        .map(metadata_doc_index_from_usize)
        .filter(|&right| {
            metadata_content_atoms_share_token(0, right, &atoms, &compact.docs)
                && metadata_template_atoms_share_safe_prefix(0, right, &atoms, compatibility)
        })
        .collect::<std::collections::HashSet<_>>();

    assert_eq!(basis, MetadataLocalCandidateBasis::Intersection);
    assert_eq!(actual, expected);
}

#[test]
fn template_candidate_postings_use_sparse_csr_and_reuse_planned_ranges() {
    let source = include_str!("index.rs");
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
fn conservative_dimension_index_recalls_shared_idf_anchor_candidates() {
    let mut builder = MetadataDataBuilder::new(1);
    let contents = [
        "rareone sharedalpha",
        "raretwo sharedalpha",
        "unrelated isolated",
    ];
    let mut records = Vec::new();
    for (index, content) in contents.into_iter().enumerate() {
        builder.merge_indexed_row(IndexedMetadataRow {
            chain_index: 0,
            nft_count: 1,
            content_doc: MetadataBm25Document::from_text(content).map(Arc::new),
            doc: MetadataBm25Document::from_text(&format!("template {index}"))
                .unwrap()
                .into(),
            doc_key: metadata_document_key(&format!("template {index}")),
        });
        records.push(MetadataContentRecord {
            contract_index: metadata_contract_index_from_usize(index),
            doc: MetadataBm25Document::from_text(content).unwrap().into(),
        });
    }
    let data = builder.finish();
    let compact = CompactMetadataContentSet::from_records(&records);
    let atoms = build_metadata_content_atoms(&records, &compact.docs, &data);
    let index = MetadataConservativeDimensionIndex::from_content_docs(&compact.docs, &atoms, false);
    let mut scratch = MetadataCandidateScratch::new(atoms.len());
    scratch.clear_for_next_left();

    index.append_candidates_after(0, &mut scratch);

    assert!(scratch
        .candidates
        .contains(&metadata_doc_index_from_usize(1)));
    assert!(index.matches(0, 1));
    assert!(index.memory_bytes() > 0);
}

#[test]
fn conservative_local_index_requires_both_sketch_dimensions() {
    let mut builder = MetadataDataBuilder::new(1);
    let contents = [
        "rareone sharedalpha",
        "raretwo sharedalpha",
        "unrelated isolated",
    ];
    let templates = [
        "collection sharedtemplate",
        "collection sharedtemplate",
        "different isolatedtemplate",
    ];
    let mut records = Vec::new();
    for (index, (content, template)) in contents.into_iter().zip(templates).enumerate() {
        builder.merge_indexed_row(IndexedMetadataRow {
            chain_index: 0,
            nft_count: 1,
            content_doc: MetadataBm25Document::from_text(content).map(Arc::new),
            doc: MetadataBm25Document::from_text(template).unwrap().into(),
            doc_key: metadata_document_key(template),
        });
        records.push(MetadataContentRecord {
            contract_index: metadata_contract_index_from_usize(index),
            doc: MetadataBm25Document::from_text(content).unwrap().into(),
        });
    }
    let data = builder.finish();
    let compact = CompactMetadataContentSet::from_records(&records);
    let atoms = build_metadata_content_atoms(&records, &compact.docs, &data);
    let compatibility = MetadataTemplateCompatibility::Scored(&data.metadata_index.scoring);
    let index = MetadataLocalCandidateIndex::from_atoms_with_mode(
        &compact.docs,
        &atoms,
        compatibility,
        false,
        MetadataRecallMode::Conservative,
    );
    let mut scratch = MetadataCandidateScratch::new(atoms.len());
    scratch.clear_for_next_left();

    let basis =
        index.append_candidates_after(0, &atoms[0], &compact.docs[0], compatibility, &mut scratch);

    assert_eq!(basis, MetadataLocalCandidateBasis::ConservativeIntersection);
    assert!(scratch
        .candidates
        .contains(&metadata_doc_index_from_usize(1)));
    assert!(!scratch
        .candidates
        .contains(&metadata_doc_index_from_usize(2)));
    assert!(!metadata_candidate_intersects_both_dimensions(
        MetadataLocalCandidateBasis::ConservativeIntersection,
        0,
        metadata_doc_index_from_usize(2),
        &atoms,
        &compact.docs,
        compatibility,
    ));
}

#[test]
fn conservative_calibration_falls_back_only_above_drift_limits() {
    let within_limits = MetadataRecallCalibrationStats {
        exact_duplicate_contract_members: 200,
        missed_duplicate_contract_members: 1,
        exact_component_members: 500,
        shifted_component_members: 1,
        ..MetadataRecallCalibrationStats::default()
    };
    assert!(!within_limits.requires_exact_fallback());

    let contract_drift_above_limit = MetadataRecallCalibrationStats {
        exact_duplicate_contract_members: 199,
        missed_duplicate_contract_members: 1,
        ..MetadataRecallCalibrationStats::default()
    };
    assert!(contract_drift_above_limit.requires_exact_fallback());

    let component_drift_above_limit = MetadataRecallCalibrationStats {
        exact_component_members: 499,
        shifted_component_members: 1,
        ..MetadataRecallCalibrationStats::default()
    };
    assert!(component_drift_above_limit.requires_exact_fallback());
}

#[test]
fn conservative_large_group_runs_deterministic_calibration_before_union() {
    let record_count = 256usize;
    let mut builder = MetadataDataBuilder::new(1);
    let mut records = Vec::with_capacity(record_count);
    for index in 0..record_count {
        let pair = index / 2;
        let side = if index % 2 == 0 {
            "sidealpha"
        } else {
            "sidebeta"
        };
        let content = format!(
            "pair{pair} sharedone sharedtwo sharedthree sharedfour sharedfive sharedsix sharedseven shared eight {side}"
        );
        let template = format!("collection pair{pair} shared template identity");
        builder.merge_indexed_row(IndexedMetadataRow {
            chain_index: 0,
            nft_count: 1,
            content_doc: MetadataBm25Document::from_text(&content).map(Arc::new),
            doc: MetadataBm25Document::from_text(&template).unwrap().into(),
            doc_key: metadata_document_key(&template),
        });
        records.push(MetadataContentRecord {
            contract_index: metadata_contract_index_from_usize(index),
            doc: MetadataBm25Document::from_text(&content).unwrap().into(),
        });
    }
    let data = builder.finish();
    let compact = CompactMetadataContentSet::from_records(&records);
    let contract_tokens = CompactContractTokens::from_nested(vec![vec![1]; record_count]);
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
        recall_mode: MetadataRecallMode::Conservative,
    };
    let mut state = MetadataUnionState {
        intra: UnionFind::new(record_count),
        cross: None,
        chain_matrix: None,
    };

    let stats = union_metadata_shared_token_atoms_with_mode(
        &records,
        &compact.docs,
        &context,
        &mut state,
        MetadataRecallMode::Conservative,
    );
    let mut exact_state = MetadataUnionState {
        intra: UnionFind::new(record_count),
        cross: None,
        chain_matrix: None,
    };
    union_metadata_shared_token_atoms_with_mode(
        &records,
        &compact.docs,
        &context,
        &mut exact_state,
        MetadataRecallMode::Exact,
    );
    let canonical_components = |union: &mut UnionFind| {
        let mut minimum_by_root = HashMap::new();
        for contract in 0..record_count {
            let root = union.find(contract);
            minimum_by_root
                .entry(root)
                .and_modify(|minimum: &mut usize| *minimum = (*minimum).min(contract))
                .or_insert(contract);
        }
        (0..record_count)
            .map(|contract| {
                let root = union.find(contract);
                minimum_by_root[&root]
            })
            .collect::<Vec<_>>()
    };

    assert_eq!(stats.atom_count, record_count);
    assert_eq!(stats.conservative_groups, 1);
    assert!(stats.recall_calibration.sampled_left_atoms >= 2);
    assert_eq!(stats.exact_fallback_groups, 0);
    assert_eq!(stats.recall_calibration.missed_matched_pairs, 0);
    assert_eq!(
        canonical_components(&mut state.intra),
        canonical_components(&mut exact_state.intra)
    );

    let single_thread_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .build()
        .unwrap();
    let single_thread_context = MetadataContentUnionContext {
        data: &data,
        template_compatibility: MetadataTemplateCompatibility::Scored(&data.metadata_index.scoring),
        contract_tokens: &contract_tokens,
        chain_count: 1,
        pool: &single_thread_pool,
        recall_mode: MetadataRecallMode::Conservative,
    };
    let mut single_thread_state = MetadataUnionState {
        intra: UnionFind::new(record_count),
        cross: None,
        chain_matrix: None,
    };
    let single_thread_stats = union_metadata_shared_token_atoms_with_mode(
        &records,
        &compact.docs,
        &single_thread_context,
        &mut single_thread_state,
        MetadataRecallMode::Conservative,
    );
    assert_eq!(
        single_thread_stats.recall_calibration,
        stats.recall_calibration
    );
    assert_eq!(
        canonical_components(&mut single_thread_state.intra),
        canonical_components(&mut exact_state.intra)
    );

    let mut below_threshold_state = MetadataUnionState {
        intra: UnionFind::new(record_count),
        cross: None,
        chain_matrix: None,
    };
    let below_threshold = union_metadata_shared_token_atoms_with_mode(
        &records[..record_count - 1],
        &compact.docs[..record_count - 1],
        &context,
        &mut below_threshold_state,
        MetadataRecallMode::Conservative,
    );
    assert_eq!(below_threshold.atom_count, record_count - 1);
    assert_eq!(below_threshold.conservative_groups, 0);
}

#[test]
fn conservative_calibration_fallback_preserves_exact_components() {
    let record_count = 256usize;
    let shared_content = (0..20)
        .map(|token| format!("ubiquitous{token}"))
        .collect::<Vec<_>>()
        .join(" ");
    let mut builder = MetadataDataBuilder::new(1);
    let mut records = Vec::with_capacity(record_count);
    for index in 0..record_count {
        let pair = index / 2;
        let content = format!("{shared_content} contentunique{index}");
        let template = format!("collection pair{pair} stable template identity");
        builder.merge_indexed_row(IndexedMetadataRow {
            chain_index: 0,
            nft_count: 1,
            content_doc: MetadataBm25Document::from_text(&content).map(Arc::new),
            doc: MetadataBm25Document::from_text(&template).unwrap().into(),
            doc_key: metadata_document_key(&template),
        });
        records.push(MetadataContentRecord {
            contract_index: metadata_contract_index_from_usize(index),
            doc: MetadataBm25Document::from_text(&content).unwrap().into(),
        });
    }
    let data = builder.finish();
    let compact = CompactMetadataContentSet::from_records(&records);
    assert!(
        compact_metadata_content_pair_score(&compact.docs[0], &compact.docs[1])
            >= METADATA_THRESHOLD
    );
    let contract_tokens = CompactContractTokens::from_nested(vec![vec![1]; record_count]);
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
        recall_mode: MetadataRecallMode::Conservative,
    };
    let mut conservative_state = MetadataUnionState {
        intra: UnionFind::new(record_count),
        cross: None,
        chain_matrix: None,
    };
    let mut exact_state = MetadataUnionState {
        intra: UnionFind::new(record_count),
        cross: None,
        chain_matrix: None,
    };

    let stats = union_metadata_shared_token_atoms_with_mode(
        &records,
        &compact.docs,
        &context,
        &mut conservative_state,
        MetadataRecallMode::Conservative,
    );
    union_metadata_shared_token_atoms_with_mode(
        &records,
        &compact.docs,
        &context,
        &mut exact_state,
        MetadataRecallMode::Exact,
    );

    assert_eq!(stats.conservative_groups, 1);
    assert!(stats.recall_calibration.exact_matched_pairs > 0);
    assert!(stats.recall_calibration.missed_matched_pairs > 0);
    assert_eq!(stats.exact_fallback_groups, 1);
    for left in 0..record_count {
        for right in 0..record_count {
            assert_eq!(
                conservative_state.intra.find(left) == conservative_state.intra.find(right),
                exact_state.intra.find(left) == exact_state.intra.find(right),
                "component mismatch for {left}/{right}"
            );
        }
    }
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
    let source = include_str!("index.rs");
    assert!(source.contains("MetadataTemplateCandidateIndex"));
}

#[test]
fn local_candidate_index_uses_cheaper_content_postings_for_identical_templates() {
    let contents = [
        "amber uniqueone",
        "bronze uniquetwo",
        "crimson uniquethree",
        "denim uniquefour",
    ];
    let mut builder = MetadataDataBuilder::new(1);
    for content in contents {
        builder.merge_indexed_row(IndexedMetadataRow {
            chain_index: 0,
            nft_count: 1,
            content_doc: MetadataBm25Document::from_text(content).map(Arc::new),
            doc: MetadataBm25Document::from_text("shared template identity")
                .unwrap()
                .into(),
            doc_key: metadata_document_key("shared template identity"),
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
    let compact = CompactMetadataContentSet::from_records(&records);
    let atoms = build_metadata_content_atoms(&records, &compact.docs, &data);
    let candidate_index = MetadataLocalCandidateIndex::from_atoms(
        &compact.docs,
        &atoms,
        MetadataTemplateCompatibility::Scored(&data.metadata_index.scoring),
        false,
    );
    let mut scratch = MetadataCandidateScratch::new(atoms.len());
    scratch.clear_for_next_left();

    let basis = candidate_index.append_candidates_after(
        0,
        &atoms[0],
        &compact.docs[0],
        MetadataTemplateCompatibility::Scored(&data.metadata_index.scoring),
        &mut scratch,
    );

    assert_eq!(basis, MetadataLocalCandidateBasis::Content);
    assert!(scratch.candidates.is_empty());
}

#[test]
fn two_atom_groups_bypass_adaptive_index_construction() {
    let source = include_str!("index.rs");
    for (start_marker, end_marker) in [
        (
            "fn union_metadata_shared_token_atom_core",
            "pub(super) fn union_metadata_no_common_content_candidates",
        ),
        (
            "fn union_metadata_no_common_atom_core",
            "pub(super) fn union_metadata_content_candidates",
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
fn local_template_prefix_candidates_keep_every_bidirectional_bm25_match() {
    let index = InternedMetadataIndex::from_source_doc_entries(vec![
        metadata_doc_entry("gold dragon rare collection"),
        metadata_doc_entry("gold dragon rare collection edition"),
        metadata_doc_entry("silver cat common series"),
        metadata_doc_entry("silver cat common series edition"),
        metadata_doc_entry("solana pixel bird traits"),
        metadata_doc_entry("unrelated isolated metadata"),
    ]);
    let atoms = (0..index.doc_count())
        .map(|doc_index| MetadataContentAtom {
            chain_index: 0,
            template_doc_index: metadata_doc_index_from_usize(doc_index),
            representative_record_index: metadata_doc_index_from_usize(doc_index),
            members: vec![metadata_contract_index_from_usize(doc_index)],
            fallback_token_groups: Vec::new(),
        })
        .collect::<Vec<_>>();
    let candidate_index = MetadataTemplateCandidateIndex::from_atoms(&index.scoring, &atoms);
    let mut scratch = MetadataCandidateScratch::new(atoms.len());
    let mut candidates = std::collections::HashSet::new();
    for (left, atom) in atoms.iter().enumerate().take(atoms.len().saturating_sub(1)) {
        scratch.clear_for_next_left();
        candidate_index.append_candidates_after(left, atom, &index.scoring, &mut scratch);
        candidates.extend(
            scratch
                .candidates
                .iter()
                .map(|&right| (left, metadata_doc_index_to_usize(right))),
        );
    }

    for left in 0..index.doc_count().saturating_sub(1) {
        for right in left + 1..index.doc_count() {
            let matched = index.scoring.score(left, right) >= METADATA_THRESHOLD
                || index.scoring.score(right, left) >= METADATA_THRESHOLD;
            if matched {
                assert!(
                    candidates.contains(&(left, right)),
                    "safe prefix dropped matching template pair ({left}, {right})"
                );
            }
        }
    }
    assert!(candidates.len() < index.doc_count() * (index.doc_count() - 1) / 2);
}

#[test]
fn metadata_token_stats_count_removed_singletons_without_indexing_them() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
        CREATE TEMP TABLE metadata_rows AS
        SELECT *, 0::UINTEGER AS source_file,
               row_number() OVER ()::UBIGINT AS source_row_number,
               true AS metadata_eligible
        FROM (VALUES
            (0::UINTEGER, 'shared', '{}'),
            (1::UINTEGER, 'shared', '{}'),
            (0::UINTEGER, 'only-a', '{}'),
            (1::UINTEGER, 'only-b', '{}')
        ) rows(contract_id, token_id, metadata_json);
        CREATE TEMP TABLE analysis_contracts AS
        SELECT contract_id, contract_id::BIGINT AS metadata_contract_index
        FROM (VALUES (0::UINTEGER), (1::UINTEGER)) contracts(contract_id);
        "#,
    )
    .unwrap();

    prepare_metadata_contract_token_rows(&conn).unwrap();

    let stats = conn
        .query_row(
            "SELECT singleton_token_count, retained_shared_token_count FROM metadata_token_stats",
            [],
            |row| Ok((row.get::<_, u64>(0)?, row.get::<_, u64>(1)?)),
        )
        .unwrap();
    let indexed_tokens = conn
        .query_row(
            "SELECT count(DISTINCT token_index)::UBIGINT FROM metadata_contract_token_rows",
            [],
            |row| row.get::<_, u64>(0),
        )
        .unwrap();
    assert_eq!(stats, (2, 1));
    assert_eq!(indexed_tokens, 1);
}

#[test]
fn metadata_content_pair_match_is_symmetric_and_thresholded() {
    let left = MetadataBm25Document::from_text("gold dragon rare").unwrap();
    let identical = MetadataBm25Document::from_text("gold dragon rare").unwrap();
    let unrelated = MetadataBm25Document::from_text("silver cat").unwrap();
    let records = [left.clone(), identical.clone(), unrelated.clone()]
        .into_iter()
        .enumerate()
        .map(|(contract_index, doc)| MetadataContentRecord {
            contract_index: metadata_contract_index_from_usize(contract_index),
            doc: doc.into(),
        })
        .collect::<Vec<_>>();
    let compact = CompactMetadataContentSet::from_records(&records);

    assert!(metadata_content_pair_matches(
        &compact.docs[0],
        &compact.docs[1],
        METADATA_THRESHOLD
    ));
    assert!(!metadata_content_pair_matches(
        &compact.docs[0],
        &compact.docs[2],
        METADATA_THRESHOLD
    ));
    assert_eq!(
        metadata_content_pair_score(&left, &identical),
        metadata_content_pair_score(&identical, &left)
    );
    assert!(
        (compact_metadata_content_pair_score(&compact.docs[0], &compact.docs[1],)
            - metadata_content_pair_score(&left, &identical))
        .abs()
            < 1e-9
    );
    assert!(
        (compact_metadata_content_pair_score(&compact.docs[0], &compact.docs[2],)
            - metadata_content_pair_score(&left, &unrelated))
        .abs()
            < 1e-9
    );
}

#[test]
fn fused_compact_metadata_pair_score_matches_reference_on_boundaries_and_random_docs() {
    fn random_document(seed: &mut u64) -> CompactMetadataContentDocument {
        let mut terms = Vec::new();
        for token in 0..24u32 {
            *seed = seed
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            if *seed >> 61 == 0 {
                let frequency = ((*seed >> 32) as u32 % 7) + 1;
                terms.push((token, frequency));
            }
        }
        CompactMetadataContentDocument {
            len: terms.iter().map(|(_, frequency)| *frequency as usize).sum(),
            terms,
        }
    }

    let boundaries = [
        CompactMetadataContentDocument {
            len: 0,
            terms: Vec::new(),
        },
        CompactMetadataContentDocument {
            len: 1,
            terms: vec![(0, 1)],
        },
        CompactMetadataContentDocument {
            len: 9,
            terms: vec![(0, 7), (9, 2)],
        },
        CompactMetadataContentDocument {
            len: 11,
            terms: vec![(1, 1), (4, 5), (12, 5)],
        },
    ];
    for left in &boundaries {
        for right in &boundaries {
            let actual = compact_metadata_content_pair_score(left, right);
            let expected = compact_metadata_content_pair_score_reference(left, right);
            assert!(
                (actual - expected).abs() <= 1e-12,
                "boundary mismatch: actual={actual}, expected={expected}, left={left:?}, right={right:?}"
            );
        }
    }

    let mut seed = 0x9e37_79b9_7f4a_7c15;
    for case in 0..2_000 {
        let left = random_document(&mut seed);
        let right = random_document(&mut seed);
        let actual = compact_metadata_content_pair_score(&left, &right);
        let expected = compact_metadata_content_pair_score_reference(&left, &right);
        assert!(
            (actual - expected).abs() <= 1e-12,
            "random case {case}: actual={actual}, expected={expected}, left={left:?}, right={right:?}"
        );
    }
}

#[test]
fn compact_metadata_pair_score_uses_one_linear_term_merge() {
    let source = include_str!("bm25.rs");
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
fn metadata_template_matches_accept_exact_or_scored_document_pairs() {
    let matches = MetadataTemplateMatches::from_pairs(6, vec![(2, 5), (1, 4)]);

    assert!(matches.matches(3, 3));
    assert!(matches.matches(2, 5));
    assert!(matches.matches(5, 2));
    assert!(!matches.matches(2, 4));
}

#[test]
fn lazy_template_compatibility_matches_bidirectional_bm25_semantics() {
    let index = InternedMetadataIndex::from_source_doc_entries(vec![
        metadata_doc_entry("gold dragon alpha omega"),
        metadata_doc_entry("dragon gold alpha"),
        metadata_doc_entry("silver cat"),
    ]);
    let compatibility = MetadataTemplateCompatibility::Scored(&index.scoring);

    for left in 0..index.doc_count() {
        for right in 0..index.doc_count() {
            let expected = left == right
                || index.scoring.score(left, right) >= METADATA_THRESHOLD
                || index.scoring.score(right, left) >= METADATA_THRESHOLD;
            let (actual, scored_directions) = compatibility.evaluate(
                metadata_doc_index_from_usize(left),
                metadata_doc_index_from_usize(right),
            );
            assert_eq!(actual, expected, "template pair {left}-{right}");
            assert!(scored_directions <= 2);
            assert_eq!(scored_directions == 0, left == right);
        }
    }
}

#[test]
fn lowest_common_metadata_token_uses_sorted_compact_ids() {
    assert_eq!(
        lowest_common_metadata_token(&[1, 4, 8, 13], &[2, 4, 7, 13]),
        Some(4)
    );
    assert_eq!(lowest_common_metadata_token(&[1, 3], &[2, 4]), None);
    assert_eq!(lowest_common_metadata_token(&[], &[1]), None);
}

#[test]
fn compact_contract_tokens_round_trip_sorted_unique_slices_and_memory() {
    let tokens = CompactContractTokens::from_nested(vec![vec![9, 2, 9], Vec::new(), vec![7, 3]]);

    assert_eq!(tokens.len(), 3);
    assert!(!tokens.is_empty());
    assert_eq!(tokens.tokens(0), &[2, 9]);
    assert!(tokens[1].is_empty());
    assert_eq!(tokens.tokens(2), &[3, 7]);
    assert_eq!(
        tokens.memory_bytes(),
        4 * std::mem::size_of::<u64>() + 4 * std::mem::size_of::<u32>()
    );
    assert!(
        tokens.memory_bytes()
            < 3 * std::mem::size_of::<Vec<u32>>() + 4 * std::mem::size_of::<u32>()
    );
    assert!(CompactContractTokens::default().is_empty());
}

#[test]
fn compact_contract_token_loader_maps_sources_and_sorts_unique_rows() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
        CREATE TABLE metadata_contract_token_rows (
            contract_index BIGINT,
            token_index BIGINT
        );
        INSERT INTO metadata_contract_token_rows VALUES
            (0, 9), (2, 7), (0, 2), (1, 99), (2, 3);
        "#,
    )
    .unwrap();
    let mut builder = MetadataDataBuilder::new(1);
    for (source_index, document) in [(0, "alpha template"), (2, "beta template")] {
        builder.merge_source_indexed_row(
            source_index,
            IndexedMetadataRow {
                chain_index: 0,
                nft_count: 1,
                content_doc: MetadataBm25Document::from_text(document).map(Arc::new),
                doc: MetadataBm25Document::from_text(document).unwrap().into(),
                doc_key: document.to_string(),
            },
            true,
        );
    }
    let data = builder.finish();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .unwrap();

    let tokens = load_metadata_contract_tokens(&conn, &data, &pool, None).unwrap();

    assert_eq!(tokens.len(), 2);
    assert_eq!(tokens.tokens(0), &[2, 9]);
    assert_eq!(tokens.tokens(1), &[3, 7]);
}

#[test]
fn compact_token_index_keeps_only_cross_contract_tokens() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
        CREATE TEMP TABLE metadata_rows AS
        SELECT *, 0::UINTEGER AS source_file,
               row_number() OVER ()::UBIGINT AS source_row_number,
               metadata_json <> '' AS metadata_eligible FROM (
            VALUES
            (0::UINTEGER, '10', '{"description":"a ten"}'),
            (0::UINTEGER, '2',  '{"description":"shared token"}'),
            (1::UINTEGER, '2',  '{"description":"shared token"}'),
            (1::UINTEGER, '3',  '{"description":"b three"}')
        ) AS t(contract_id, token_id, metadata_json);
        CREATE TEMP TABLE analysis_contracts AS
        SELECT contract_id, contract_id::BIGINT AS metadata_contract_index
        FROM (VALUES (0::UINTEGER), (1::UINTEGER)) contracts(contract_id);
        "#,
    )
    .unwrap();
    let mut builder = MetadataDataBuilder::new(1);
    for _ in ["0xaaa", "0xbbb"] {
        let prefilter = "description".to_string();
        builder.merge_indexed_row(IndexedMetadataRow {
            chain_index: 0,
            nft_count: 2,
            content_doc: MetadataBm25Document::from_text("shared token").map(Arc::new),
            doc: MetadataBm25Document::from_text(&prefilter).unwrap().into(),
            doc_key: prefilter,
        });
    }
    let data = builder.finish();
    prepare_metadata_contract_token_rows(&conn).unwrap();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .unwrap();
    let contract_tokens = load_metadata_contract_tokens(&conn, &data, &pool, None).unwrap();
    assert_eq!(contract_tokens.tokens(0), &[0]);
    assert_eq!(contract_tokens.tokens(1), &[0]);
}

#[test]
fn reused_metadata_cache_contains_only_documents_used_by_multiple_sources() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
        CREATE TEMP TABLE metadata_rows AS
        SELECT *, 0::UINTEGER AS source_file,
               row_number() OVER ()::UBIGINT AS source_row_number,
               true AS metadata_eligible
        FROM (VALUES
            (0::UINTEGER, '1', '{"description":"shared"}'),
            (1::UINTEGER, '1', '{"description":"shared"}'),
            (2::UINTEGER, '9', '{"description":"unique"}')
        ) rows(contract_id, token_id, metadata_json);
        CREATE TEMP TABLE analysis_contracts AS
        SELECT contract_id,
               contract_id::BIGINT AS metadata_contract_index,
               min(source_file)::UINTEGER AS metadata_source_file,
               min(source_row_number)::UBIGINT AS metadata_source_row_number
        FROM metadata_rows
        GROUP BY contract_id;
        "#,
    )
    .unwrap();
    prepare_metadata_contract_token_rows(&conn).unwrap();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .unwrap();

    let cache = load_reused_metadata_documents(&conn, &pool, None, usize::MAX, None).unwrap();

    assert_eq!(cache.len(), 1);
    assert!(cache.contains_key(r#"{"description":"shared"}"#));
    assert!(!cache.contains_key(r#"{"description":"unique"}"#));

    let chain_indexes = HashMap::from([("ethereum", 0usize)]);
    let indexed = index_metadata_raw_row_chunk_with_cache(
        vec![
            (
                0,
                RawMetadataRow {
                    chain: "ethereum".to_string(),
                    metadata_json: r#"{"description":"shared"}"#.to_string(),
                    nft_count: 1,
                },
            ),
            (
                1,
                RawMetadataRow {
                    chain: "ethereum".to_string(),
                    metadata_json: r#"{"description":"shared"}"#.to_string(),
                    nft_count: 1,
                },
            ),
        ],
        &chain_indexes,
        &cache,
    );
    assert!(Arc::ptr_eq(
        indexed[0].1.content_doc.as_ref().unwrap(),
        indexed[1].1.content_doc.as_ref().unwrap()
    ));
    assert!(indexed[0].1.doc.is_shared());
    assert!(indexed[0].1.doc.shares_allocation_with(&indexed[1].1.doc));
    let uncached = index_metadata_raw_row_chunk(
        vec![(
            2,
            RawMetadataRow {
                chain: "ethereum".to_string(),
                metadata_json: r#"{"description":"unique template"}"#.to_string(),
                nft_count: 1,
            },
        )],
        &chain_indexes,
    );
    assert!(uncached[0].1.doc.is_owned());
    let bounded_cache =
        load_reused_metadata_documents(&conn, &pool, Some(1), usize::MAX, None).unwrap();
    assert!(bounded_cache.is_empty());
    assert!(reused_metadata_documents_memory_bytes(&bounded_cache) <= 1);
}

#[test]
fn reused_metadata_cache_respects_actual_parsed_memory_budget() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
        CREATE TEMP TABLE metadata_rows AS
        SELECT *, 0::UINTEGER AS source_file,
               row_number() OVER ()::UBIGINT AS source_row_number,
               true AS metadata_eligible
        FROM (VALUES
            (0::UINTEGER, '1', '{"description":"shared gold dragon"}'),
            (1::UINTEGER, '1', '{"description":"shared gold dragon"}')
        ) rows(contract_id, token_id, metadata_json);
        CREATE TEMP TABLE analysis_contracts AS
        SELECT contract_id,
               contract_id::BIGINT AS metadata_contract_index,
               min(source_file)::UINTEGER AS metadata_source_file,
               min(source_row_number)::UBIGINT AS metadata_source_row_number
        FROM metadata_rows
        GROUP BY contract_id;
        CREATE TEMP TABLE metadata_contract_token_rows AS
        SELECT * FROM (VALUES
            (0::BIGINT, 0::UINTEGER, 1::UBIGINT),
            (1::BIGINT, 0::UINTEGER, 2::UBIGINT)
        ) rows(metadata_contract_index, metadata_source_file, metadata_source_row_number);
        "#,
    )
    .unwrap();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .unwrap();
    let unbounded = load_reused_metadata_documents(&conn, &pool, None, usize::MAX, None).unwrap();
    let exact_bytes = reused_metadata_documents_memory_bytes(&unbounded);
    assert!(exact_bytes > 1);

    let bounded = load_reused_metadata_documents(
        &conn,
        &pool,
        Some(exact_bytes.saturating_sub(1)),
        usize::MAX,
        None,
    )
    .unwrap();

    assert!(reused_metadata_documents_memory_bytes(&bounded) < exact_bytes);
}

#[test]
fn reused_metadata_cache_skips_rows_larger_than_its_transient_parse_budget() {
    let conn = Connection::open_in_memory().unwrap();
    let large = format!(r#"{{"description":"{}"}}"#, "dragon ".repeat(10_000));
    let small = r#"{"description":"shared gold dragon"}"#;
    conn.execute_batch(&format!(
        r#"
        CREATE TEMP TABLE metadata_rows AS
        SELECT *, 0::UINTEGER AS source_file,
               row_number() OVER ()::UBIGINT AS source_row_number,
               true AS metadata_eligible
        FROM (VALUES
            (0::UINTEGER, '1', '{large}'),
            (1::UINTEGER, '1', '{large}'),
            (2::UINTEGER, '1', '{small}'),
            (3::UINTEGER, '1', '{small}')
        ) rows(contract_id, token_id, metadata_json);
        CREATE TEMP TABLE analysis_contracts AS
        SELECT contract_id,
               contract_id::BIGINT AS metadata_contract_index,
               min(source_file)::UINTEGER AS metadata_source_file,
               min(source_row_number)::UBIGINT AS metadata_source_row_number
        FROM metadata_rows
        GROUP BY contract_id;
        CREATE TEMP TABLE metadata_contract_token_rows AS
        SELECT metadata_contract_index, metadata_source_file, metadata_source_row_number
        FROM analysis_contracts;
        "#
    ))
    .unwrap();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .unwrap();
    let transient_budget = reused_metadata_load_row_transient_bytes(small);

    let cache = load_reused_metadata_documents(&conn, &pool, None, transient_budget, None).unwrap();

    assert!(cache.contains_key(small));
    assert!(!cache.contains_key(&large));
}

#[test]
fn reused_metadata_cache_budget_does_not_rescan_all_entries_per_insert() {
    let source = include_str!("load.rs");
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
fn reused_metadata_cache_incremental_accounting_matches_a_full_scan() {
    let mut documents = ReusedMetadataDocuments::new();
    let raw = String::from(r#"{"description":"shared gold dragon"}"#);
    let parsed = metadata_documents_from_json(&raw);
    let cached = ReusedMetadataDocument {
        prefilter: MetadataBm25Document::from_text(&parsed.prefilter).map(Arc::new),
        content: MetadataBm25Document::from_text(&parsed.content).map(Arc::new),
        doc_key: metadata_document_key(&parsed.prefilter),
    };
    documents.try_reserve(1).unwrap();
    let retained_payload_bytes = raw
        .capacity()
        .saturating_add(cached.doc_key.capacity())
        .saturating_add(
            cached
                .prefilter
                .as_ref()
                .map_or(0, metadata_content_arc_memory_bytes),
        )
        .saturating_add(
            cached
                .content
                .as_ref()
                .map_or(0, metadata_content_arc_memory_bytes),
        );
    let projected_bytes = hash_table_allocation_bytes(
        documents.capacity(),
        std::mem::size_of::<(String, ReusedMetadataDocument)>(),
    )
    .saturating_add(retained_payload_bytes);

    assert!(documents.insert(raw, cached).is_none());
    assert_eq!(
        projected_bytes,
        reused_metadata_documents_memory_bytes(&documents)
    );
}

#[test]
fn skipped_metadata_documents_preserve_source_to_compact_contract_mapping() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
        CREATE TEMP TABLE source_rows AS
        SELECT * FROM (
            VALUES
            ('ethereum', '0xaaa', '1', '', '{"description":"gold dragon"}'),
            ('ethereum', '0xbbb', '1', '', '{}'),
            ('ethereum', '0xccc', '1', '', '{"description":"gold dragon"}')
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
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .unwrap();

    let data = load_metadata_data(
        &conn,
        &["ethereum".to_string()],
        &pool,
        ReusedMetadataDocuments::new(),
        MetadataLoadBudgets::new(usize::MAX, usize::MAX),
        None,
    )
    .unwrap();
    prepare_metadata_contract_token_rows(&conn).unwrap();
    let contract_tokens = load_metadata_contract_tokens(&conn, &data, &pool, None).unwrap();

    assert_eq!(data.contracts.len(), 2);
    assert_eq!(data.compact_contract_index_for_source(0), Some(0));
    assert_eq!(data.compact_contract_index_for_source(1), None);
    assert_eq!(data.compact_contract_index_for_source(2), Some(1));
    assert_eq!(contract_tokens.tokens(0), &[0]);
    assert_eq!(contract_tokens.tokens(1), &[0]);
}

#[test]
fn token_content_groups_union_matches_without_contract_pair_table() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
        CREATE TEMP TABLE metadata_rows AS
        SELECT *, 0::UINTEGER AS source_file,
               row_number() OVER ()::UBIGINT AS source_row_number,
               metadata_json <> '' AS metadata_eligible FROM (
            VALUES
            (0::UINTEGER, '1', '{"description":"different lower"}'),
            (0::UINTEGER, '2', '{"description":"gold dragon"}'),
            (1::UINTEGER, '2', '{"description":"gold dragon"}')
        ) AS t(contract_id, token_id, metadata_json);
        CREATE TEMP TABLE analysis_contracts AS
        SELECT contract_id,
               contract_id::BIGINT AS metadata_contract_index,
               arg_min(source_file, row(token_id, source_file, source_row_number))::UINTEGER
                   AS metadata_source_file,
               arg_min(source_row_number, row(token_id, source_file, source_row_number))::UBIGINT
                   AS metadata_source_row_number
        FROM metadata_rows
        GROUP BY contract_id;
        "#,
    )
    .unwrap();
    let mut builder = MetadataDataBuilder::new(1);
    for _ in ["0xaaa", "0xbbb"] {
        builder.merge_indexed_row(IndexedMetadataRow {
            chain_index: 0,
            nft_count: 2,
            content_doc: MetadataBm25Document::from_text("gold dragon").map(Arc::new),
            doc: MetadataBm25Document::from_text("shared template")
                .unwrap()
                .into(),
            doc_key: metadata_document_key("shared template"),
        });
    }
    let data = builder.finish();
    prepare_metadata_contract_token_rows(&conn).unwrap();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .unwrap();
    let contract_tokens = load_metadata_contract_tokens(&conn, &data, &pool, None).unwrap();
    let mut state = MetadataUnionState {
        intra: UnionFind::new(2),
        cross: None,
        chain_matrix: None,
    };
    let template_matches = MetadataTemplateMatches::default();
    let context = MetadataContentUnionContext {
        data: &data,
        template_compatibility: MetadataTemplateCompatibility::Precomputed(&template_matches),
        contract_tokens: &contract_tokens,
        chain_count: 1,
        pool: &pool,
        recall_mode: MetadataRecallMode::Exact,
    };
    let progress = ProgressTracker::for_pipeline_stage(PipelineStage::Metadata, true);
    progress.start_task("shared-token memberships", Some(2), "memberships");

    union_metadata_token_content_matches(
        &conn,
        &context,
        &mut state,
        usize::MAX,
        MetadataRecallMode::Exact,
        &progress,
    )
    .unwrap();

    assert_eq!(state.intra.find(0), state.intra.find(1));
    let ProgressTracker::Enabled { task, .. } = &progress else {
        panic!("progress must be enabled");
    };
    assert_eq!(task.position(), 2);
}

#[test]
fn token_content_fast_path_does_not_substitute_a_fallback_document() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
        CREATE TABLE metadata_rows AS
        SELECT * FROM (VALUES
            (0::UINTEGER, '1', '{}', 0::UINTEGER, 1::UBIGINT, true),
            (0::UINTEGER, '2', '{"description":"gold dragon"}', 0::UINTEGER, 2::UBIGINT, true),
            (1::UINTEGER, '1', '{"description":"gold dragon"}', 0::UINTEGER, 3::UBIGINT, true)
        ) rows(contract_id, token_id, metadata_json, source_file, source_row_number, metadata_eligible);
        CREATE TABLE analysis_contracts AS
        SELECT * FROM (VALUES
            (0::UINTEGER, 'ethereum', 1::BIGINT, 0::UINTEGER, 1::UBIGINT, 0::BIGINT),
            (1::UINTEGER, 'ethereum', 1::BIGINT, 0::UINTEGER, 3::UBIGINT, 1::BIGINT)
        ) rows(contract_id, chain, nft_count, metadata_source_file,
               metadata_source_row_number, metadata_contract_index);
        "#,
    )
    .unwrap();
    prepare_metadata_contract_token_rows(&conn).unwrap();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .unwrap();
    let data = load_metadata_data(
        &conn,
        &["ethereum".to_string()],
        &pool,
        ReusedMetadataDocuments::new(),
        MetadataLoadBudgets::new(usize::MAX, usize::MAX),
        None,
    )
    .unwrap();
    let contract_tokens = load_metadata_contract_tokens(&conn, &data, &pool, None).unwrap();
    let mut state = MetadataUnionState {
        intra: UnionFind::new(2),
        cross: None,
        chain_matrix: None,
    };
    let context = MetadataContentUnionContext {
        data: &data,
        template_compatibility: MetadataTemplateCompatibility::Scored(&data.metadata_index.scoring),
        contract_tokens: &contract_tokens,
        chain_count: 1,
        pool: &pool,
        recall_mode: MetadataRecallMode::Exact,
    };

    union_metadata_token_content_matches(
        &conn,
        &context,
        &mut state,
        usize::MAX,
        MetadataRecallMode::Exact,
        &ProgressTracker::Disabled,
    )
    .unwrap();

    assert_ne!(state.intra.find(0), state.intra.find(1));
}

#[test]
fn bounded_metadata_raw_group_matches_owned_record_path() {
    let raw_documents = [
        r#"{"description":"gold dragon alpha"}"#,
        r#"{"description":"gold dragon alpha"}"#,
        r#"{"description":"gold dragon beta"}"#,
        r#"{"description":"silver cat"}"#,
    ];
    let mut builder = MetadataDataBuilder::new(2);
    for (contract_index, _) in raw_documents.iter().enumerate() {
        builder.merge_indexed_row(IndexedMetadataRow {
            chain_index: contract_index % 2,
            nft_count: 1,
            content_doc: None,
            doc: MetadataBm25Document::from_text("shared template")
                .unwrap()
                .into(),
            doc_key: metadata_document_key("shared template"),
        });
    }
    let data = builder.finish();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .unwrap();
    let template_matches = MetadataTemplateMatches::default();
    let contract_tokens = CompactContractTokens::from_nested(vec![vec![0]; raw_documents.len()]);
    let context = MetadataContentUnionContext {
        data: &data,
        template_compatibility: MetadataTemplateCompatibility::Precomputed(&template_matches),
        contract_tokens: &contract_tokens,
        chain_count: 2,
        pool: &pool,
        recall_mode: MetadataRecallMode::Exact,
    };

    let records = raw_documents
        .iter()
        .enumerate()
        .map(|(contract_index, raw)| MetadataContentRecord {
            contract_index: metadata_contract_index_from_usize(contract_index),
            doc: MetadataBm25Document::from_text(&metadata_document_from_json(raw))
                .unwrap()
                .into(),
        })
        .collect::<Vec<_>>();
    let mut expected_state = MetadataUnionState {
        intra: UnionFind::new(raw_documents.len()),
        cross: Some(SparseUnionFind::default()),
        chain_matrix: Some(new_chain_matrix_reuse_states(1)),
    };
    let expected = union_metadata_content_candidates(
        &records,
        MetadataContentScope::SharedToken,
        &context,
        &mut expected_state,
    );

    let mut group = MetadataRawTokenGroup::default();
    for (contract_index, raw) in raw_documents.iter().enumerate() {
        group.push_raw(
            metadata_contract_index_from_usize(contract_index),
            (*raw).to_owned(),
            &context,
        );
    }
    let mut actual_state = MetadataUnionState {
        intra: UnionFind::new(raw_documents.len()),
        cross: Some(SparseUnionFind::default()),
        chain_matrix: Some(new_chain_matrix_reuse_states(1)),
    };
    let actual = group.union(&context, &mut actual_state);

    assert_eq!(actual, expected);
    for left in 0..raw_documents.len() {
        for right in left + 1..raw_documents.len() {
            assert_eq!(
                actual_state.intra.find(left) == actual_state.intra.find(right),
                expected_state.intra.find(left) == expected_state.intra.find(right),
                "intra component mismatch for {left}-{right}"
            );
            assert_eq!(
                actual_state.cross.as_mut().unwrap().connected(left, right),
                expected_state
                    .cross
                    .as_mut()
                    .unwrap()
                    .connected(left, right),
                "cross component mismatch for {left}-{right}"
            );
            assert_eq!(
                actual_state.chain_matrix.as_mut().unwrap()[0].connected(left, right),
                expected_state.chain_matrix.as_mut().unwrap()[0].connected(left, right),
                "matrix component mismatch for {left}-{right}"
            );
        }
    }
}

#[test]
fn metadata_raw_group_never_buffers_more_than_one_chunk() {
    let mut builder = MetadataDataBuilder::new(1);
    builder.merge_indexed_row(IndexedMetadataRow {
        chain_index: 0,
        nft_count: 1,
        content_doc: None,
        doc: MetadataBm25Document::from_text("shared template")
            .unwrap()
            .into(),
        doc_key: metadata_document_key("shared template"),
    });
    let data = builder.finish();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .unwrap();
    let template_matches = MetadataTemplateMatches::default();
    let contract_tokens = CompactContractTokens::default();
    let context = MetadataContentUnionContext {
        data: &data,
        template_compatibility: MetadataTemplateCompatibility::Precomputed(&template_matches),
        contract_tokens: &contract_tokens,
        chain_count: 1,
        pool: &pool,
        recall_mode: MetadataRecallMode::Exact,
    };
    let mut group = MetadataRawTokenGroup::default();

    for _ in 0..METADATA_RAW_GROUP_CHUNK_SIZE * 2 + 1 {
        group.push_raw(0, r#"{"description":"gold dragon"}"#.to_owned(), &context);
        assert!(group.raw_buffer_len() <= METADATA_RAW_GROUP_CHUNK_SIZE);
    }

    assert_eq!(group.max_raw_buffer_len(), METADATA_RAW_GROUP_CHUNK_SIZE);
    assert_eq!(group.compact_doc_count(), 1);
    assert_eq!(
        group.compact_member_count(),
        METADATA_RAW_GROUP_CHUNK_SIZE * 2
    );
    assert_eq!(group.raw_buffer_len(), 1);
}

#[test]
fn metadata_raw_group_rejects_content_working_set_before_unbounded_growth() {
    let data = MetadataDataBuilder::new(1).finish();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .build()
        .unwrap();
    let template_matches = MetadataTemplateMatches::default();
    let contract_tokens = CompactContractTokens::default();
    let context = MetadataContentUnionContext {
        data: &data,
        template_compatibility: MetadataTemplateCompatibility::Precomputed(&template_matches),
        contract_tokens: &contract_tokens,
        chain_count: 1,
        pool: &pool,
        recall_mode: MetadataRecallMode::Exact,
    };
    let mut group = MetadataRawTokenGroup::default();

    let error = group
        .push_raw_with_budget(
            0,
            r#"{"description":"gold dragon"}"#.to_owned(),
            &context,
            1,
        )
        .unwrap_err();

    assert!(error.to_string().contains("metadata content working set"));
}

#[test]
fn metadata_raw_group_flushes_a_fitting_chunk_before_accepting_the_next_row() {
    let mut builder = MetadataDataBuilder::new(1);
    builder.merge_indexed_row(IndexedMetadataRow {
        chain_index: 0,
        nft_count: 1,
        content_doc: None,
        doc: MetadataBm25Document::from_text("shared template")
            .unwrap()
            .into(),
        doc_key: metadata_document_key("shared template"),
    });
    let data = builder.finish();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .build()
        .unwrap();
    let template_matches = MetadataTemplateMatches::default();
    let contract_tokens = CompactContractTokens::default();
    let context = MetadataContentUnionContext {
        data: &data,
        template_compatibility: MetadataTemplateCompatibility::Precomputed(&template_matches),
        contract_tokens: &contract_tokens,
        chain_count: 1,
        pool: &pool,
        recall_mode: MetadataRecallMode::Exact,
    };
    let metadata_json = format!(r#"{{"description":"{}"}}"#, "x".repeat(60 * 1024));
    let mut one_row = MetadataRawTokenGroup::default();
    one_row.push_raw(0, metadata_json.clone(), &context);
    let one_row_reserve = one_row.raw_parse_reserve_bytes();
    let maximum_bytes = one_row_reserve.saturating_add(512 * 1024);

    let mut group = MetadataRawTokenGroup::default();
    group
        .push_raw_with_budget(0, metadata_json.clone(), &context, maximum_bytes)
        .unwrap();
    group
        .push_raw_with_budget(0, metadata_json.clone(), &context, maximum_bytes)
        .unwrap();

    assert_eq!(group.compact_member_count(), 1);
    assert_eq!(group.raw_buffer_len(), 1);
}

#[test]
fn shared_token_parser_borrows_cached_document_without_cloning_arc() {
    let raw = r#"{"description":"gold dragon"}"#;
    let cached = Arc::new(MetadataBm25Document::from_text("gold dragon").unwrap());
    let mut reused = ReusedMetadataDocuments::new();
    reused.insert(
        raw.to_owned(),
        ReusedMetadataDocument {
            prefilter: None,
            content: Some(cached.clone()),
            doc_key: metadata_document_key("gold dragon"),
        },
    );
    let data = MetadataDataBuilder::new(1).finish_with_reused_documents(reused);
    let strong_count = Arc::strong_count(&cached);

    let document = metadata_content_document(&data, raw).unwrap();

    assert!(matches!(document, std::borrow::Cow::Borrowed(_)));
    assert_eq!(Arc::strong_count(&cached), strong_count);
}

fn metadata_doc_entry(text: &str) -> SourceMetadataDocEntry {
    SourceMetadataDocEntry {
        doc: MetadataBm25Document::from_text(text).unwrap().into(),
        contracts: vec![0],
    }
}

#[test]
fn metadata_document_key_uses_direct_document_text() {
    let left = "gold dragon gold";
    let right = "dragon gold gold";

    assert_ne!(metadata_document_key(left), metadata_document_key(right));
}

#[test]
fn metadata_document_uses_top_contract_content_values_for_global_matching() {
    let document = metadata_document_from_json(
        r#"{
            "description": "Gold Dragon",
            "attributes": [
                {"trait_type": "Background", "value": "Gold"},
                {"trait_type": "Background", "value": "Gold"},
                {"trait_type": "Eyes", "value": "Laser"}
            ],
            "seller_fee_basis_points": 500,
            "irrelevant": "Hidden Lore"
        }"#,
    );

    assert!(document.contains("gold dragon"));
    assert!(document.contains("background"));
    assert!(document.contains("eyes"));
    assert!(document.contains("laser"));
    assert!(!document.contains("seller_fee_basis_points"));
    assert!(!document.contains("hidden lore"));
}

#[test]
fn metadata_document_preserves_content_values_for_representative_matching() {
    let left = metadata_document_from_json(
        r#"{
            "name": "Alpha #1",
            "image": "ipfs://alpha/1.png",
            "attributes": [
                {"trait_type": "Background", "value": "Blue"}
            ]
        }"#,
    );
    let right = metadata_document_from_json(
        r#"{
            "name": "Beta #9",
            "image": "ipfs://beta/9.png",
            "attributes": [
                {"trait_type": "Background", "value": "Red"}
            ]
        }"#,
    );

    assert!(left.contains("alpha"));
    assert!(left.contains("blue"));
    assert!(right.contains("beta"));
    assert!(right.contains("red"));
    assert_ne!(metadata_document_key(&left), metadata_document_key(&right));
}

#[test]
fn metadata_document_rejects_non_json_and_overlong_raw_metadata() {
    assert_eq!(metadata_document_from_json("not json metadata"), "");
    let overlong = format!(
        "{{\"description\":\"{}\"}}",
        "x".repeat(MAX_METADATA_BYTES_FOR_DEDUP)
    );

    assert_eq!(metadata_document_from_json(&overlong), "");
}

#[test]
fn metadata_document_normalizes_nfkc_content_values() {
    let document =
        metadata_document_from_json(r#"{"description":"\uFF27\uFF4F\uFF4C\uFF44\u3000Dragon"}"#);

    assert_eq!(document, "gold dragon");
}

#[test]
fn metadata_doc_pair_hits_are_collected_for_left_range() {
    let docs = vec![
        metadata_doc_entry("gold dragon alpha"),
        metadata_doc_entry("dragon gold beta"),
        metadata_doc_entry("silver cat"),
        metadata_doc_entry("gold dragon beta"),
    ];
    let index = InternedMetadataIndex::from_source_doc_entries(docs);
    let scratch_pool = MetadataCandidateScratchPool::new(index.doc_count());

    let batch = collect_metadata_doc_pair_hits_for_left_range(
        1..3,
        MetadataPairScoringContext {
            postings: &index.postings,
            scoring: &index.scoring,
        },
        &scratch_pool,
    );

    assert_eq!(
        batch,
        MetadataDocPairBatch {
            hits: vec![(1, 3)],
            candidate_pairs: 1,
        }
    );
}

#[test]
fn metadata_doc_pair_hit_collection_never_exceeds_remaining_pair_permits() {
    let docs = (0..4)
        .map(|_| metadata_doc_entry("gold dragon shared template"))
        .collect::<Vec<_>>();
    let index = InternedMetadataIndex::from_source_doc_entries(docs);
    let scratch_pool = MetadataCandidateScratchPool::new(index.doc_count());
    let context = MetadataPairScoringContext {
        postings: &index.postings,
        scoring: &index.scoring,
    };

    let exact = collect_metadata_doc_pair_hits_for_left_range_bounded(
        0..1,
        MetadataPairScoringContext {
            postings: &index.postings,
            scoring: &index.scoring,
        },
        &scratch_pool,
        3,
    )
    .unwrap();
    assert_eq!(exact.hits.len(), 3);

    let error = collect_metadata_doc_pair_hits_for_left_range_bounded(
        0..index.doc_count(),
        context,
        &scratch_pool,
        1,
    )
    .unwrap_err();

    assert_eq!(error.retained_hits, 1);
}

#[test]
fn metadata_doc_pair_prefilter_does_not_use_a_rare_anchor_gate() {
    let shared = "attributes image name trait_type value description external_url animation_url \
         metadata raw collection creator royalty license marketplace contract chain story \
         lore summary";
    let docs = vec![
        metadata_doc_entry(&format!("{shared} alpha")),
        metadata_doc_entry(&format!("{shared} beta")),
        metadata_doc_entry(&format!("{shared} gamma")),
    ];
    let index = InternedMetadataIndex::from_source_doc_entries(docs);
    let scratch_pool = MetadataCandidateScratchPool::new(index.doc_count());

    let batch = collect_metadata_doc_pair_hits_for_left_range(
        0..1,
        MetadataPairScoringContext {
            postings: &index.postings,
            scoring: &index.scoring,
        },
        &scratch_pool,
    );

    assert!(batch.hits.contains(&(0, 1)));
}

#[test]
fn metadata_bm25_prefix_candidates_are_selective_and_complete() {
    let shared = "attributes image name trait_type value description";
    let mut docs = vec![
        metadata_doc_entry(&format!(
            "{shared} copied_collection shared_story golden_dragon rare_anchor left_variant"
        )),
        metadata_doc_entry(&format!(
            "{shared} copied_collection shared_story golden_dragon rare_anchor right_variant"
        )),
    ];
    docs.extend((0..96).map(|index| metadata_doc_entry(&format!("{shared} unrelated_{index}"))));
    let index = InternedMetadataIndex::from_source_doc_entries(docs);
    let scratch_pool = MetadataCandidateScratchPool::new(index.doc_count());
    let context = MetadataPairScoringContext {
        postings: &index.postings,
        scoring: &index.scoring,
    };
    let mut scratch = scratch_pool.take();

    let candidates =
        metadata_candidate_indices_for_left_with_scratch(0, &context, &mut scratch).to_vec();
    let brute_force_matches = (1..index.doc_count())
        .filter(|&right| index.scoring.score(0, right) >= METADATA_THRESHOLD)
        .map(metadata_doc_index_from_usize)
        .collect::<Vec<_>>();

    assert!(index.scoring.candidate_tokens(0).len() < index.scoring.query_terms_len(0));
    assert!(candidates.len() < index.doc_count() / 4);
    assert!(brute_force_matches
        .iter()
        .all(|right| candidates.contains(right)));
    assert!(brute_force_matches.contains(&metadata_doc_index_from_usize(1)));
}

#[test]
fn metadata_bm25_prefix_pair_hits_equal_brute_force_results() {
    let docs = vec![
        metadata_doc_entry("attributes description gold dragon rare"),
        metadata_doc_entry("attributes description gold dragon"),
        metadata_doc_entry("attributes description silver dragon"),
        metadata_doc_entry("attributes image blue cat"),
        metadata_doc_entry("description gold dragon rare edition"),
        metadata_doc_entry("collection creator unrelated item"),
    ];
    let index = InternedMetadataIndex::from_source_doc_entries(docs);
    let context = MetadataPairScoringContext {
        postings: &index.postings,
        scoring: &index.scoring,
    };
    let scratch_pool = MetadataCandidateScratchPool::new(index.doc_count());
    let actual =
        collect_metadata_doc_pair_hits_for_left_range(0..index.doc_count(), context, &scratch_pool)
            .hits;
    let mut expected = Vec::new();
    for left in 0..index.doc_count() {
        for right in left + 1..index.doc_count() {
            if index.scoring.score(left, right) >= METADATA_THRESHOLD
                || index.scoring.score(right, left) >= METADATA_THRESHOLD
            {
                expected.push((
                    metadata_doc_index_from_usize(left),
                    metadata_doc_index_from_usize(right),
                ));
            }
        }
    }

    assert_eq!(actual, expected);
}

#[test]
fn metadata_template_recall_keeps_high_tf_bm25_match() {
    let common = std::iter::repeat_n("common", 5_000)
        .collect::<Vec<_>>()
        .join(" ");
    let suffix = |prefix: &str| {
        (0..17)
            .map(|index| format!("{prefix}{index}"))
            .collect::<Vec<_>>()
            .join(" ")
    };
    let mut docs = vec![
        metadata_doc_entry(&format!("{common} {}", suffix("a"))),
        metadata_doc_entry(&format!("{common} {}", suffix("b"))),
    ];
    docs.extend((0..40).map(|document| {
        let rare = (0..17)
            .map(|index| format!("z{document}_{index}"))
            .collect::<Vec<_>>()
            .join(" ");
        metadata_doc_entry(&format!("{common} {rare}"))
    }));
    let index = InternedMetadataIndex::from_source_doc_entries(docs);
    let score = index.scoring.score(0, 1).max(index.scoring.score(1, 0));
    assert!(score >= METADATA_THRESHOLD, "score={score}");
    let context = MetadataPairScoringContext {
        postings: &index.postings,
        scoring: &index.scoring,
    };
    let scratch_pool = MetadataCandidateScratchPool::new(index.doc_count());

    let actual = collect_metadata_doc_pair_hits_for_left_range(0..2, context, &scratch_pool).hits;

    assert!(
        actual.contains(&(0, 1)),
        "valid BM25 pair was dropped: score={score}"
    );
}

#[test]
fn metadata_content_inverted_index_partitions_shared_terms_by_compatible_template() {
    let records = vec![
        MetadataContentRecord {
            contract_index: 0,
            doc: MetadataBm25Document::from_text("ipfs gold dragon")
                .unwrap()
                .into(),
        },
        MetadataContentRecord {
            contract_index: 1,
            doc: MetadataBm25Document::from_text("ipfs gold cat")
                .unwrap()
                .into(),
        },
        MetadataContentRecord {
            contract_index: 2,
            doc: MetadataBm25Document::from_text("ipfs silver bird")
                .unwrap()
                .into(),
        },
        MetadataContentRecord {
            contract_index: 3,
            doc: MetadataBm25Document::from_text("ipfs silver fox")
                .unwrap()
                .into(),
        },
    ];
    let template_docs = vec![0, 0, 1, 1];
    let candidates = collect_metadata_content_candidate_pairs(
        &records,
        &template_docs,
        &MetadataTemplateMatches::default(),
    );

    assert_eq!(candidates, vec![(0, 1), (2, 3)]);
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

    let source = include_str!("index.rs");
    let scoring_peak = source
        .split("fn scoring_peak_bytes")
        .nth(1)
        .and_then(|tail| tail.split("fn ensure_within_memory_budget").next())
        .expect("metadata scoring peak implementation");
    assert!(scoring_peak.contains("2usize.saturating_mul(std::mem::size_of::<u64>())"));
}

#[test]
fn metadata_content_pair_batch_parallel_scoring_keeps_only_matching_candidates() {
    let mut records = vec![MetadataContentRecord {
        contract_index: 0,
        doc: MetadataBm25Document::from_text("gold dragon")
            .unwrap()
            .into(),
    }];
    for contract_index in 1..=METADATA_CONTENT_PARALLEL_MIN_RECORDS {
        let content = if contract_index % 2 == 0 {
            "gold dragon"
        } else {
            "silver cat"
        };
        records.push(MetadataContentRecord {
            contract_index: metadata_contract_index_from_usize(contract_index),
            doc: MetadataBm25Document::from_text(content).unwrap().into(),
        });
    }
    let atoms = (0..records.len())
        .map(|index| MetadataContentAtom {
            chain_index: 0,
            template_doc_index: 0,
            representative_record_index: metadata_doc_index_from_usize(index),
            members: vec![metadata_contract_index_from_usize(index)],
            fallback_token_groups: Vec::new(),
        })
        .collect::<Vec<_>>();
    let candidates = (1..records.len())
        .map(|right| (0, metadata_doc_index_from_usize(right)))
        .collect::<Vec<_>>();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap();
    let compact = CompactMetadataContentSet::from_records(&records);
    let hits = collect_metadata_content_atom_pair_hits(&candidates, &atoms, &compact.docs, &pool);
    let expected = (2..=METADATA_CONTENT_PARALLEL_MIN_RECORDS)
        .step_by(2)
        .map(|right| (0, metadata_doc_index_from_usize(right)))
        .collect::<Vec<_>>();

    assert_eq!(hits, expected);
}

#[test]
fn metadata_content_candidates_accept_matching_later_common_token() {
    let mut builder = MetadataDataBuilder::new(1);
    for _ in ["0xaaa", "0xbbb", "0xccc", "0xddd"] {
        builder.merge_indexed_row(IndexedMetadataRow {
            chain_index: 0,
            nft_count: 1,
            content_doc: MetadataBm25Document::from_text("gold dragon").map(Arc::new),
            doc: MetadataBm25Document::from_text("shared template")
                .unwrap()
                .into(),
            doc_key: metadata_document_key("shared template"),
        });
    }
    let data = builder.finish();
    let template_matches = MetadataTemplateMatches::default();
    let contract_tokens =
        CompactContractTokens::from_nested(vec![vec![1, 4], vec![1, 4], vec![4], vec![4]]);
    let records = vec![
        MetadataContentRecord {
            contract_index: 0,
            doc: MetadataBm25Document::from_text("gold dragon")
                .unwrap()
                .into(),
        },
        MetadataContentRecord {
            contract_index: 1,
            doc: MetadataBm25Document::from_text("gold dragon")
                .unwrap()
                .into(),
        },
        MetadataContentRecord {
            contract_index: 2,
            doc: MetadataBm25Document::from_text("silver cat")
                .unwrap()
                .into(),
        },
        MetadataContentRecord {
            contract_index: 3,
            doc: MetadataBm25Document::from_text("silver cat")
                .unwrap()
                .into(),
        },
    ];
    let mut state = MetadataUnionState {
        intra: UnionFind::new(4),
        cross: None,
        chain_matrix: None,
    };
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap();
    let context = MetadataContentUnionContext {
        data: &data,
        template_compatibility: MetadataTemplateCompatibility::Precomputed(&template_matches),
        contract_tokens: &contract_tokens,
        chain_count: 1,
        pool: &pool,
        recall_mode: MetadataRecallMode::Exact,
    };

    union_metadata_content_candidates(
        &records,
        MetadataContentScope::SharedToken,
        &context,
        &mut state,
    );

    assert_eq!(state.intra.find(0), state.intra.find(1));
    assert_eq!(state.intra.find(2), state.intra.find(3));
}

#[test]
fn metadata_content_union_collapses_identical_dense_component_to_one_atom() {
    let mut builder = MetadataDataBuilder::new(1);
    for _ in ["0xaaa", "0xbbb", "0xccc", "0xddd"] {
        builder.merge_indexed_row(IndexedMetadataRow {
            chain_index: 0,
            nft_count: 1,
            content_doc: MetadataBm25Document::from_text("gold dragon").map(Arc::new),
            doc: MetadataBm25Document::from_text("shared template")
                .unwrap()
                .into(),
            doc_key: metadata_document_key("shared template"),
        });
    }
    let data = builder.finish();
    let template_matches = MetadataTemplateMatches::default();
    let contract_tokens =
        CompactContractTokens::from_nested(vec![vec![1], vec![1], vec![1], vec![1]]);
    let records = (0..4)
        .map(|contract_index| MetadataContentRecord {
            contract_index,
            doc: MetadataBm25Document::from_text("gold dragon")
                .unwrap()
                .into(),
        })
        .collect::<Vec<_>>();
    let mut state = MetadataUnionState {
        intra: UnionFind::new(4),
        cross: None,
        chain_matrix: None,
    };
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap();
    let context = MetadataContentUnionContext {
        data: &data,
        template_compatibility: MetadataTemplateCompatibility::Precomputed(&template_matches),
        contract_tokens: &contract_tokens,
        chain_count: 1,
        pool: &pool,
        recall_mode: MetadataRecallMode::Exact,
    };

    let stats = union_metadata_content_candidates(
        &records,
        MetadataContentScope::SharedToken,
        &context,
        &mut state,
    );

    assert_eq!(stats.atom_count, 1);
    assert_eq!(stats.candidate_pairs, 0);
    assert_eq!(stats.scored_pairs, 0);
    assert_eq!(state.intra.find(0), state.intra.find(3));
}

#[test]
fn metadata_content_atoms_ignore_bm25_token_order() {
    let mut builder = MetadataDataBuilder::new(1);
    for _ in ["0xaaa", "0xbbb"] {
        builder.merge_indexed_row(IndexedMetadataRow {
            chain_index: 0,
            nft_count: 1,
            content_doc: MetadataBm25Document::from_text("gold dragon rare").map(Arc::new),
            doc: MetadataBm25Document::from_text("shared template")
                .unwrap()
                .into(),
            doc_key: metadata_document_key("shared template"),
        });
    }
    let data = builder.finish();
    let template_matches = MetadataTemplateMatches::default();
    let contract_tokens = CompactContractTokens::from_nested(vec![vec![1], vec![1]]);
    let records = vec![
        MetadataContentRecord {
            contract_index: 0,
            doc: MetadataBm25Document::from_text("gold dragon rare")
                .unwrap()
                .into(),
        },
        MetadataContentRecord {
            contract_index: 1,
            doc: MetadataBm25Document::from_text("rare gold dragon")
                .unwrap()
                .into(),
        },
    ];
    let mut state = MetadataUnionState {
        intra: UnionFind::new(2),
        cross: None,
        chain_matrix: None,
    };
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .unwrap();
    let context = MetadataContentUnionContext {
        data: &data,
        template_compatibility: MetadataTemplateCompatibility::Precomputed(&template_matches),
        contract_tokens: &contract_tokens,
        chain_count: 1,
        pool: &pool,
        recall_mode: MetadataRecallMode::Exact,
    };

    let stats = union_metadata_content_candidates(
        &records,
        MetadataContentScope::SharedToken,
        &context,
        &mut state,
    );

    assert_eq!(stats.atom_count, 1);
    assert_eq!(stats.scored_pairs, 0);
    assert_eq!(state.intra.find(0), state.intra.find(1));
}

#[test]
fn metadata_content_atoms_preserve_cross_chain_matrix_membership() {
    let mut builder = MetadataDataBuilder::new(2);
    for (chain_index, _) in [
        (0, "0xeth-a"),
        (0, "0xeth-b"),
        (1, "0xbase-a"),
        (1, "0xbase-b"),
    ] {
        builder.merge_indexed_row(IndexedMetadataRow {
            chain_index,
            nft_count: 1,
            content_doc: MetadataBm25Document::from_text("gold dragon").map(Arc::new),
            doc: MetadataBm25Document::from_text("shared template")
                .unwrap()
                .into(),
            doc_key: metadata_document_key("shared template"),
        });
    }
    let data = builder.finish();
    let template_matches = MetadataTemplateMatches::default();
    let contract_tokens =
        CompactContractTokens::from_nested(vec![vec![1], vec![1], vec![1], vec![1]]);
    let records = (0..4)
        .map(|contract_index| MetadataContentRecord {
            contract_index,
            doc: MetadataBm25Document::from_text("gold dragon")
                .unwrap()
                .into(),
        })
        .collect::<Vec<_>>();
    let mut state = MetadataUnionState {
        intra: UnionFind::new(4),
        cross: Some(SparseUnionFind::default()),
        chain_matrix: Some(new_chain_matrix_reuse_states(1)),
    };
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap();
    let context = MetadataContentUnionContext {
        data: &data,
        template_compatibility: MetadataTemplateCompatibility::Precomputed(&template_matches),
        contract_tokens: &contract_tokens,
        chain_count: 2,
        pool: &pool,
        recall_mode: MetadataRecallMode::Exact,
    };

    let stats = union_metadata_content_candidates(
        &records,
        MetadataContentScope::SharedToken,
        &context,
        &mut state,
    );

    assert_eq!(stats.atom_count, 2);
    assert_eq!(stats.candidate_pairs, 1);
    assert_eq!(stats.scored_pairs, 1);
    assert_eq!(state.intra.find(0), state.intra.find(1));
    assert_eq!(state.intra.find(2), state.intra.find(3));
    let cross = state.cross.as_mut().unwrap();
    assert!(cross.connected(0, 3));
    assert!(cross.connected(1, 2));
    let matrix = state.chain_matrix.as_mut().unwrap();
    assert!(matrix[0].connected(0, 3));
    assert!(matrix[0].connected(1, 2));
}

#[test]
fn metadata_content_atoms_expand_members_when_representatives_are_preconnected() {
    let mut builder = MetadataDataBuilder::new(2);
    for (chain_index, _) in [
        (0, "0xeth-a"),
        (0, "0xeth-b"),
        (1, "0xbase-a"),
        (1, "0xbase-b"),
    ] {
        builder.merge_indexed_row(IndexedMetadataRow {
            chain_index,
            nft_count: 1,
            content_doc: MetadataBm25Document::from_text("gold dragon").map(Arc::new),
            doc: MetadataBm25Document::from_text("shared template")
                .unwrap()
                .into(),
            doc_key: metadata_document_key("shared template"),
        });
    }
    let data = builder.finish();
    let template_matches = MetadataTemplateMatches::default();
    let contract_tokens =
        CompactContractTokens::from_nested(vec![vec![1, 2], vec![2], vec![1, 2], vec![2]]);
    let records = (0..4)
        .map(|contract_index| MetadataContentRecord {
            contract_index,
            doc: MetadataBm25Document::from_text("gold dragon")
                .unwrap()
                .into(),
        })
        .collect::<Vec<_>>();
    let mut state = MetadataUnionState {
        intra: UnionFind::new(4),
        cross: Some(SparseUnionFind::default()),
        chain_matrix: Some(new_chain_matrix_reuse_states(1)),
    };
    apply_metadata_contract_pair_union(&data, 2, &mut state, 0, 2);
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap();
    let context = MetadataContentUnionContext {
        data: &data,
        template_compatibility: MetadataTemplateCompatibility::Precomputed(&template_matches),
        contract_tokens: &contract_tokens,
        chain_count: 2,
        pool: &pool,
        recall_mode: MetadataRecallMode::Exact,
    };

    let stats = union_metadata_content_candidates(
        &records,
        MetadataContentScope::SharedToken,
        &context,
        &mut state,
    );

    assert_eq!(stats.atom_count, 2);
    assert_eq!(stats.candidate_pairs, 1);
    assert_eq!(stats.scored_pairs, 1);
    let cross = state.cross.as_mut().unwrap();
    assert!(cross.connected(1, 3));
    let matrix = state.chain_matrix.as_mut().unwrap();
    assert!(matrix[0].connected(1, 3));
}

#[test]
fn metadata_representative_fallback_unions_only_without_common_token() {
    let mut builder = MetadataDataBuilder::new(1);
    for (_, content) in [
        ("0xaaa", "gold dragon"),
        ("0xbbb", "gold dragon"),
        ("0xccc", "silver cat"),
        ("0xddd", "silver cat"),
    ] {
        builder.merge_indexed_row(IndexedMetadataRow {
            chain_index: 0,
            nft_count: 1,
            content_doc: MetadataBm25Document::from_text(content).map(Arc::new),
            doc: MetadataBm25Document::from_text("shared template")
                .unwrap()
                .into(),
            doc_key: metadata_document_key("shared template"),
        });
    }
    let data = builder.finish();
    let contract_tokens =
        CompactContractTokens::from_nested(vec![vec![1], vec![2], vec![3], vec![3]]);
    let mut state = MetadataUnionState {
        intra: UnionFind::new(4),
        cross: None,
        chain_matrix: None,
    };
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap();
    let template_matches = MetadataTemplateMatches::default();
    let context = MetadataContentUnionContext {
        data: &data,
        template_compatibility: MetadataTemplateCompatibility::Precomputed(&template_matches),
        contract_tokens: &contract_tokens,
        chain_count: 1,
        pool: &pool,
        recall_mode: MetadataRecallMode::Exact,
    };

    union_metadata_representative_content_fallback(
        &context,
        &mut state,
        usize::MAX,
        &ProgressTracker::Disabled,
    )
    .unwrap();

    assert_eq!(state.intra.find(0), state.intra.find(1));
    assert_ne!(state.intra.find(2), state.intra.find(3));
}

#[test]
fn scored_adaptive_fallback_preserves_disjoint_token_group_semantics() {
    let templates_and_contents = [
        (
            "shared template gold collection",
            "gold dragon rare collection",
        ),
        (
            "shared template gold collection variant",
            "gold dragon rare collection variantgold",
        ),
        ("shared template silver series", "silver cat common series"),
        (
            "shared template silver series variant",
            "silver cat common series variantsilver",
        ),
    ];
    let mut builder = MetadataDataBuilder::new(1);
    for (template, content) in templates_and_contents {
        builder.merge_indexed_row(IndexedMetadataRow {
            chain_index: 0,
            nft_count: 1,
            content_doc: MetadataBm25Document::from_text(content).map(Arc::new),
            doc: MetadataBm25Document::from_text(template).unwrap().into(),
            doc_key: metadata_document_key(template),
        });
    }
    let data = builder.finish();
    assert!(
        data.metadata_index.scoring.score(0, 1) >= METADATA_THRESHOLD
            || data.metadata_index.scoring.score(1, 0) >= METADATA_THRESHOLD
    );
    assert!(
        data.metadata_index.scoring.score(2, 3) >= METADATA_THRESHOLD
            || data.metadata_index.scoring.score(3, 2) >= METADATA_THRESHOLD
    );
    let contract_tokens =
        CompactContractTokens::from_nested(vec![vec![10], vec![11], vec![20], vec![20]]);
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
        intra: UnionFind::new(templates_and_contents.len()),
        cross: None,
        chain_matrix: None,
    };

    let stats = union_metadata_representative_content_fallback(
        &context,
        &mut state,
        usize::MAX,
        &ProgressTracker::Disabled,
    )
    .unwrap();

    assert_eq!(stats.atom_count, 4);
    assert!(stats.template_scored_pairs > 0);
    assert_eq!(state.intra.find(0), state.intra.find(1));
    assert_ne!(state.intra.find(2), state.intra.find(3));
}

#[test]
fn online_representative_fallback_matches_owned_record_path() {
    let contents = [
        "gold dragon",
        "gold dragon",
        "gold dragon rare",
        "silver cat",
    ];
    let mut builder = MetadataDataBuilder::new(2);
    for (contract_index, content) in contents.iter().enumerate() {
        builder.merge_indexed_row(IndexedMetadataRow {
            chain_index: contract_index % 2,
            nft_count: 1,
            content_doc: MetadataBm25Document::from_text(content).map(Arc::new),
            doc: MetadataBm25Document::from_text("shared template")
                .unwrap()
                .into(),
            doc_key: metadata_document_key("shared template"),
        });
    }
    let data = builder.finish();
    let contract_tokens =
        CompactContractTokens::from_nested(vec![Vec::new(), vec![1], vec![1], vec![2]]);
    let template_matches = MetadataTemplateMatches::default();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .unwrap();
    let context = MetadataContentUnionContext {
        data: &data,
        template_compatibility: MetadataTemplateCompatibility::Precomputed(&template_matches),
        contract_tokens: &contract_tokens,
        chain_count: 2,
        pool: &pool,
        recall_mode: MetadataRecallMode::Exact,
    };
    let records = data
        .contracts
        .iter()
        .enumerate()
        .filter_map(|(contract_index, contract)| {
            contract
                .content_doc
                .clone()
                .map(|doc| MetadataContentRecord {
                    contract_index: metadata_contract_index_from_usize(contract_index),
                    doc,
                })
        })
        .collect::<Vec<_>>();
    let mut expected_state = MetadataUnionState {
        intra: UnionFind::new(contents.len()),
        cross: Some(SparseUnionFind::default()),
        chain_matrix: Some(new_chain_matrix_reuse_states(1)),
    };
    let expected = union_metadata_content_candidates(
        &records,
        MetadataContentScope::NoCommonToken,
        &context,
        &mut expected_state,
    );
    let mut actual_state = MetadataUnionState {
        intra: UnionFind::new(contents.len()),
        cross: Some(SparseUnionFind::default()),
        chain_matrix: Some(new_chain_matrix_reuse_states(1)),
    };

    let actual = union_metadata_representative_content_fallback(
        &context,
        &mut actual_state,
        usize::MAX,
        &ProgressTracker::Disabled,
    )
    .unwrap();

    assert_eq!(actual, expected);
    for left in 0..contents.len() {
        for right in left + 1..contents.len() {
            assert_eq!(
                actual_state.intra.find(left) == actual_state.intra.find(right),
                expected_state.intra.find(left) == expected_state.intra.find(right)
            );
            assert_eq!(
                actual_state.cross.as_mut().unwrap().connected(left, right),
                expected_state
                    .cross
                    .as_mut()
                    .unwrap()
                    .connected(left, right)
            );
            assert_eq!(
                actual_state.chain_matrix.as_mut().unwrap()[0].connected(left, right),
                expected_state.chain_matrix.as_mut().unwrap()[0].connected(left, right)
            );
        }
    }
}

#[test]
fn representative_fallback_builds_compact_atoms_without_owned_record_vector() {
    let source = include_str!("index.rs");
    let start = source
        .find("pub(super) fn union_metadata_representative_content_fallback")
        .unwrap();
    let end = source[start..]
        .find("pub(super) fn apply_metadata_contract_pair_union")
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
fn metadata_fallback_atoms_collapse_identical_nonempty_token_sets_without_unioning() {
    let mut builder = MetadataDataBuilder::new(1);
    for _ in ["0xaaa", "0xbbb", "0xccc", "0xddd"] {
        builder.merge_indexed_row(IndexedMetadataRow {
            chain_index: 0,
            nft_count: 1,
            content_doc: MetadataBm25Document::from_text("gold dragon").map(Arc::new),
            doc: MetadataBm25Document::from_text("shared template")
                .unwrap()
                .into(),
            doc_key: metadata_document_key("shared template"),
        });
    }
    let data = builder.finish();
    let template_matches = MetadataTemplateMatches::default();
    let contract_tokens = CompactContractTokens::from_nested(vec![vec![1]; 4]);
    let records = (0..4)
        .map(|contract_index| MetadataContentRecord {
            contract_index,
            doc: MetadataBm25Document::from_text("gold dragon")
                .unwrap()
                .into(),
        })
        .collect::<Vec<_>>();
    let mut state = MetadataUnionState {
        intra: UnionFind::new(4),
        cross: None,
        chain_matrix: None,
    };
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap();
    let context = MetadataContentUnionContext {
        data: &data,
        template_compatibility: MetadataTemplateCompatibility::Precomputed(&template_matches),
        contract_tokens: &contract_tokens,
        chain_count: 1,
        pool: &pool,
        recall_mode: MetadataRecallMode::Exact,
    };

    let stats = union_metadata_content_candidates(
        &records,
        MetadataContentScope::NoCommonToken,
        &context,
        &mut state,
    );

    assert_eq!(stats.atom_count, 1);
    assert_eq!(stats.candidate_pairs, 0);
    assert_eq!(stats.scored_pairs, 0);
    assert_ne!(state.intra.find(0), state.intra.find(1));
}

#[test]
fn metadata_fallback_atoms_union_identical_members_without_token_ids() {
    let mut builder = MetadataDataBuilder::new(1);
    for _ in ["0xaaa", "0xbbb", "0xccc", "0xddd"] {
        builder.merge_indexed_row(IndexedMetadataRow {
            chain_index: 0,
            nft_count: 1,
            content_doc: MetadataBm25Document::from_text("gold dragon").map(Arc::new),
            doc: MetadataBm25Document::from_text("shared template")
                .unwrap()
                .into(),
            doc_key: metadata_document_key("shared template"),
        });
    }
    let data = builder.finish();
    let template_matches = MetadataTemplateMatches::default();
    let contract_tokens = CompactContractTokens::from_nested(vec![Vec::new(); 4]);
    let records = (0..4)
        .map(|contract_index| MetadataContentRecord {
            contract_index,
            doc: MetadataBm25Document::from_text("gold dragon")
                .unwrap()
                .into(),
        })
        .collect::<Vec<_>>();
    let mut state = MetadataUnionState {
        intra: UnionFind::new(4),
        cross: None,
        chain_matrix: None,
    };
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap();
    let context = MetadataContentUnionContext {
        data: &data,
        template_compatibility: MetadataTemplateCompatibility::Precomputed(&template_matches),
        contract_tokens: &contract_tokens,
        chain_count: 1,
        pool: &pool,
        recall_mode: MetadataRecallMode::Exact,
    };

    let stats = union_metadata_content_candidates(
        &records,
        MetadataContentScope::NoCommonToken,
        &context,
        &mut state,
    );

    assert_eq!(stats.atom_count, 1);
    assert_eq!(stats.candidate_pairs, 0);
    assert_eq!(stats.scored_pairs, 0);
    assert_eq!(state.intra.find(0), state.intra.find(3));
}

#[test]
fn metadata_fallback_atoms_avoid_quadratic_pairs_for_disjoint_token_sets() {
    const CONTRACT_COUNT: usize = 128;
    let mut builder = MetadataDataBuilder::new(1);
    for _ in 0..CONTRACT_COUNT {
        builder.merge_indexed_row(IndexedMetadataRow {
            chain_index: 0,
            nft_count: 1,
            content_doc: MetadataBm25Document::from_text("gold dragon").map(Arc::new),
            doc: MetadataBm25Document::from_text("shared template")
                .unwrap()
                .into(),
            doc_key: metadata_document_key("shared template"),
        });
    }
    let data = builder.finish();
    let template_matches = MetadataTemplateMatches::default();
    let contract_tokens = CompactContractTokens::from_nested(
        (0..CONTRACT_COUNT)
            .map(|index| vec![u32::try_from(index).unwrap()])
            .collect::<Vec<_>>(),
    );
    let records = (0..CONTRACT_COUNT)
        .map(|contract_index| MetadataContentRecord {
            contract_index: metadata_contract_index_from_usize(contract_index),
            doc: MetadataBm25Document::from_text("gold dragon")
                .unwrap()
                .into(),
        })
        .collect::<Vec<_>>();
    let mut state = MetadataUnionState {
        intra: UnionFind::new(CONTRACT_COUNT),
        cross: None,
        chain_matrix: None,
    };
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap();
    let context = MetadataContentUnionContext {
        data: &data,
        template_compatibility: MetadataTemplateCompatibility::Precomputed(&template_matches),
        contract_tokens: &contract_tokens,
        chain_count: 1,
        pool: &pool,
        recall_mode: MetadataRecallMode::Exact,
    };

    let stats = union_metadata_content_candidates(
        &records,
        MetadataContentScope::NoCommonToken,
        &context,
        &mut state,
    );

    assert_eq!(stats.atom_count, 1);
    assert_eq!(stats.candidate_pairs, 0);
    assert_eq!(stats.scored_pairs, 0);
    assert_eq!(state.intra.find(0), state.intra.find(CONTRACT_COUNT - 1));
}

#[test]
fn metadata_fallback_atoms_match_brute_force_connectivity() {
    let fixtures = [
        (0, "0xeth-a", "gold dragon", vec![1]),
        (0, "0xeth-b", "gold dragon", vec![1, 2]),
        (0, "0xeth-c", "gold dragon", vec![2]),
        (0, "0xeth-d", "gold dragon rare", vec![3]),
        (1, "0xbase-a", "gold dragon", vec![1]),
        (1, "0xbase-b", "gold dragon", vec![4]),
        (1, "0xbase-c", "gold dragon rare", Vec::new()),
        (1, "0xbase-d", "silver cat", vec![5]),
    ];
    let mut builder = MetadataDataBuilder::new(2);
    for (chain_index, _, content, _) in &fixtures {
        builder.merge_indexed_row(IndexedMetadataRow {
            chain_index: *chain_index,
            nft_count: 1,
            content_doc: MetadataBm25Document::from_text(content).map(Arc::new),
            doc: MetadataBm25Document::from_text("shared template")
                .unwrap()
                .into(),
            doc_key: metadata_document_key("shared template"),
        });
    }
    let data = builder.finish();
    let template_matches = MetadataTemplateMatches::default();
    let contract_tokens = CompactContractTokens::from_nested(
        fixtures
            .iter()
            .map(|(_, _, _, tokens)| tokens.clone())
            .collect::<Vec<_>>(),
    );
    let records = fixtures
        .iter()
        .enumerate()
        .map(
            |(contract_index, (_, _, content, _))| MetadataContentRecord {
                contract_index: metadata_contract_index_from_usize(contract_index),
                doc: MetadataBm25Document::from_text(content).unwrap().into(),
            },
        )
        .collect::<Vec<_>>();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap();
    let context = MetadataContentUnionContext {
        data: &data,
        template_compatibility: MetadataTemplateCompatibility::Precomputed(&template_matches),
        contract_tokens: &contract_tokens,
        chain_count: 2,
        pool: &pool,
        recall_mode: MetadataRecallMode::Exact,
    };
    let new_state = || MetadataUnionState {
        intra: UnionFind::new(fixtures.len()),
        cross: Some(SparseUnionFind::default()),
        chain_matrix: Some(new_chain_matrix_reuse_states(1)),
    };
    let mut optimized = new_state();
    union_metadata_content_candidates(
        &records,
        MetadataContentScope::NoCommonToken,
        &context,
        &mut optimized,
    );

    let mut reference = new_state();
    let compact = CompactMetadataContentSet::from_records(&records);
    for left in 0..records.len() {
        for right in left + 1..records.len() {
            if lowest_common_metadata_token(&contract_tokens[left], &contract_tokens[right])
                .is_none()
                && metadata_content_pair_matches(
                    &compact.docs[left],
                    &compact.docs[right],
                    METADATA_THRESHOLD,
                )
            {
                apply_metadata_contract_pair_union(&data, 2, &mut reference, left, right);
            }
        }
    }

    for left in 0..records.len() {
        for right in left + 1..records.len() {
            assert_eq!(
                optimized.intra.find(left) == optimized.intra.find(right),
                reference.intra.find(left) == reference.intra.find(right),
                "intra connectivity differs for {left}-{right}"
            );
            assert_eq!(
                optimized.cross.as_mut().unwrap().connected(left, right),
                reference.cross.as_mut().unwrap().connected(left, right),
                "cross connectivity differs for {left}-{right}"
            );
            assert_eq!(
                optimized.chain_matrix.as_mut().unwrap()[0].connected(left, right),
                reference.chain_matrix.as_mut().unwrap()[0].connected(left, right),
                "matrix connectivity differs for {left}-{right}"
            );
        }
    }
}

#[test]
fn metadata_doc_pair_hits_score_one_left_with_reused_scratch() {
    let docs = vec![
        metadata_doc_entry("gold dragon alpha omega"),
        metadata_doc_entry("dragon gold alpha"),
        metadata_doc_entry("silver cat"),
        metadata_doc_entry("gold dragon omega"),
    ];
    let index = InternedMetadataIndex::from_source_doc_entries(docs);
    let mut scratch = MetadataCandidateScratch::new(index.doc_count());
    let mut hits = Vec::new();

    let candidate_pairs = collect_metadata_doc_pair_hits_for_left_with_scratch(
        0,
        &MetadataPairScoringContext {
            postings: &index.postings,
            scoring: &index.scoring,
        },
        &mut scratch,
        &mut hits,
    );

    assert_eq!(candidate_pairs, 2);
    assert_eq!(hits, vec![(0, 1), (0, 3)]);
}

#[test]
fn metadata_pair_progress_message_shows_throughput_and_eta() {
    assert_eq!(
        metadata_pair_progress_message(333, 2, 6, 7, std::time::Duration::from_secs(2)),
        "metadata candidate pairs scored 333; left docs 2/6; estimated remaining 666; throughput 166.5 pairs/s; ETA 4s; matched doc pairs 7"
    );
}

#[test]
fn metadata_pair_progress_message_uses_unknown_eta_before_first_scored_pair() {
    assert_eq!(
        metadata_pair_progress_message(0, 0, 6, 0, std::time::Duration::from_secs(0)),
        "metadata candidate pairs scored 0; left docs 0/6; estimated remaining 0; throughput n/a; ETA n/a; matched doc pairs 0"
    );
}

#[test]
fn metadata_scoring_progress_units_track_left_docs_not_candidate_pairs() {
    assert_eq!(metadata_scoring_progress_units(10), 10);
    assert_eq!(metadata_scoring_batch_progress_units(2, 7), 5);
}

#[test]
fn metadata_pair_left_chunk_respects_match_pair_budget() {
    assert_eq!(metadata_pair_left_chunk_size(1_000, 10_000), 10);
    assert_eq!(metadata_pair_left_chunk_size(100, 1), 1);
    assert_eq!(metadata_pair_left_chunk_size(100, 100_000), 256);
    assert_eq!(metadata_pair_left_chunk_size(0, 0), 1);
}

#[test]
fn metadata_template_pair_budget_reserves_flat_offsets_and_conversion_overlap() {
    let doc_count = 10;
    let fixed_bytes = doc_count * 2 * std::mem::size_of::<u64>();

    assert_eq!(
        metadata_template_match_pair_budget(fixed_bytes - 1, doc_count),
        0
    );
    assert_eq!(
        metadata_template_match_pair_budget(fixed_bytes, doc_count),
        0
    );
    assert_eq!(
        metadata_template_match_pair_budget(fixed_bytes + 3 * 40 - 1, doc_count),
        2
    );
    assert_eq!(
        metadata_template_match_pair_budget(fixed_bytes + 3 * 40, doc_count),
        3
    );
}

#[test]
fn metadata_candidate_scratch_deduplicates_selected_prefix_postings() {
    let mut scratch = MetadataCandidateScratch::new(3);
    scratch.clear_for_next_left();
    append_metadata_posting_except(&[0, 1, 2], 0, &mut scratch);
    append_metadata_posting_except(&[0, 1], 0, &mut scratch);

    assert_eq!(scratch.candidates, vec![1, 2]);
}

#[test]
fn metadata_candidate_scratch_pool_reuses_state_across_batches() {
    let pool = MetadataCandidateScratchPool::new(3);
    let first_allocation = {
        let mut first = pool.take();
        first.clear_for_next_left();
        first.push_once(1);
        first.seen_generation.as_ptr()
    };

    let mut second = pool.take();
    let second_allocation = second.seen_generation.as_ptr();
    second.clear_for_next_left();

    assert_eq!(second.seen_generation.len(), 3);
    assert!(second.candidates.is_empty());
    assert_eq!(first_allocation, second_allocation);
}

#[test]
fn metadata_candidate_scratch_pool_releases_lock_before_cold_allocation() {
    let source = include_str!("index.rs");
    let take_start = source
        .find("pub(super) fn take(&self) -> MetadataCandidateScratchLease<'_>")
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
    let source = include_str!("index.rs");
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
fn metadata_bm25_index_interns_tokens_and_integer_postings() {
    let docs = vec![
        metadata_doc_entry("gold dragon"),
        metadata_doc_entry("dragon gold"),
        metadata_doc_entry("silver cat"),
    ];
    let index = InternedMetadataIndex::from_source_doc_entries(docs);
    let gold = index.token_id("gold").unwrap();
    let dragon = index.token_id("dragon").unwrap();

    assert_eq!(index.postings.posting(gold), &[0, 1]);
    assert_eq!(index.postings.posting(dragon), &[0, 1]);
    let _: &[u32] = index.postings.posting(gold);
    assert_eq!(index.scoring.query_term_frequency(0, gold as u32), Some(1));
}

#[test]
fn compact_metadata_postings_round_trip_empty_and_nonempty_lists() {
    let postings =
        CompactMetadataPostings::from_nested(vec![vec![1, 3], Vec::new(), vec![2, 4, 8]]);

    assert_eq!(postings.len(), 3);
    assert_eq!(postings.posting(0), &[1, 3]);
    assert!(postings.posting(1).is_empty());
    assert_eq!(postings.posting(2), &[2, 4, 8]);
}

#[test]
fn compact_metadata_postings_persist_and_remap_without_semantic_change() {
    let temp = tempfile::tempdir().unwrap();
    let postings = CompactMetadataPostings::from_nested(vec![vec![1, 3], vec![2], vec![4, 8]])
        .persist_and_remap(temp.path())
        .unwrap();

    assert!(postings.is_mapped());
    assert_eq!(postings.posting(0), &[1, 3]);
    assert_eq!(postings.posting(1), &[2]);
    assert_eq!(postings.posting(2), &[4, 8]);
    assert!(temp.path().join("posting_offsets.bin").is_file());
    assert!(temp.path().join("postings.bin").is_file());
    assert!(!temp.path().join("posting_offsets.bin.partial").exists());
    assert!(!temp.path().join("postings.bin.partial").exists());
}

#[test]
fn compact_metadata_postings_remap_preserves_logical_memory_bytes() {
    let temp = tempfile::tempdir().unwrap();
    let postings = CompactMetadataPostings::from_nested(vec![vec![1, 3], vec![2], vec![4, 8]]);
    let logical_bytes = postings.logical_memory_bytes();

    assert!(logical_bytes > 0);
    assert_eq!(postings.mapped_bytes(), 0);
    let postings = postings.persist_and_remap(temp.path()).unwrap();

    assert_eq!(postings.logical_memory_bytes(), logical_bytes);
    assert_eq!(postings.mapped_bytes(), logical_bytes);
    assert_eq!(postings.owned_memory_bytes(), 0);
}

#[test]
fn compact_metadata_scoring_persist_and_remap_preserves_scores() {
    let entries = vec![
        metadata_doc_entry("gold dragon rare"),
        metadata_doc_entry("gold dragon"),
    ];
    let mut index = InternedMetadataIndex::from_source_doc_entries(entries);
    let expected = index.scoring.score(0, 1);
    let temp = tempfile::tempdir().unwrap();

    index.remap_postings(temp.path()).unwrap();

    assert!(index.scoring.is_mapped());
    assert_eq!(index.scoring.score(0, 1), expected);
    assert!(!index.scoring.candidate_tokens(0).is_empty());
    assert!(!temp.path().join("doc_token_offsets.bin").exists());
    assert!(!temp.path().join("doc_tokens.bin").exists());
}

#[test]
fn compact_metadata_scoring_remap_preserves_logical_memory_bytes() {
    let entries = vec![
        metadata_doc_entry("gold dragon rare"),
        metadata_doc_entry("gold dragon"),
    ];
    let mut index = InternedMetadataIndex::from_source_doc_entries(entries);
    let logical_bytes = index.scoring.logical_memory_bytes();
    let temp = tempfile::tempdir().unwrap();

    assert!(logical_bytes > 0);
    assert_eq!(index.scoring.mapped_bytes(), 0);
    index.remap_postings(temp.path()).unwrap();

    assert_eq!(index.scoring.logical_memory_bytes(), logical_bytes);
    assert_eq!(index.scoring.mapped_bytes(), logical_bytes);
    assert_eq!(index.scoring.owned_memory_bytes(), 0);
}

#[test]
fn compact_metadata_scoring_builds_flat_storage_without_nested_lists() {
    let source = include_str!("bm25.rs");
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
    let source = include_str!("bm25.rs");
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
    let source = include_str!("index.rs");
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
fn compact_metadata_scoring_direct_layout_preserves_repeated_and_empty_terms() {
    let scoring = CompactMetadataScoring::from_nested(
        vec![
            PreparedInternedMetadataQuery {
                terms: vec![(1, 3), (4, 1)],
                denominator: 4.0,
                candidate_tokens: vec![1],
            },
            PreparedInternedMetadataQuery {
                terms: Vec::new(),
                denominator: 1.0,
                candidate_tokens: Vec::new(),
            },
        ],
        vec![
            PreparedInternedMetadataDoc {
                token_weights: vec![(1, 1.0), (4, 0.5)],
            },
            PreparedInternedMetadataDoc {
                token_weights: Vec::new(),
            },
        ],
    );

    assert_eq!(scoring.query_terms_len(0), 2);
    assert_eq!(scoring.query_term_frequency(0, 1), Some(3));
    assert_eq!(scoring.candidate_tokens(0), &[1]);
    assert!((scoring.score(0, 0) - 0.875).abs() < f64::EPSILON);
    assert_eq!(scoring.score(1, 0), 0.0);
    assert_eq!(scoring.score(0, 1), 0.0);
}

#[test]
fn metadata_bm25_index_assigns_lexical_token_ids_for_stable_score_order() {
    let docs = vec![
        metadata_doc_entry("gold dragon"),
        metadata_doc_entry("silver cat"),
    ];
    let index = InternedMetadataIndex::from_source_doc_entries(docs);

    assert!(
        index.token_id("cat").unwrap() < index.token_id("dragon").unwrap()
            && index.token_id("dragon").unwrap() < index.token_id("gold").unwrap()
            && index.token_id("gold").unwrap() < index.token_id("silver").unwrap()
    );
}

#[test]
fn metadata_index_remaps_only_when_heap_budget_is_exceeded() {
    let docs = vec![
        metadata_doc_entry("gold dragon rare"),
        metadata_doc_entry("silver cat common"),
    ];
    let mut index = InternedMetadataIndex::from_source_doc_entries(docs);
    let heap_bytes = index.owned_memory_bytes();
    assert!(heap_bytes > 0);
    let directory = tempfile::tempdir().unwrap();

    assert!(!index
        .remap_if_over_budget(directory.path(), heap_bytes)
        .unwrap());
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);

    assert!(index
        .remap_if_over_budget(directory.path(), heap_bytes - 1)
        .unwrap());
    assert_eq!(index.owned_memory_bytes(), 0);
    assert!(std::fs::read_dir(directory.path()).unwrap().count() > 0);
}

#[test]
fn mapped_metadata_index_remains_charged_to_the_resident_budget() {
    let mut builder = MetadataDataBuilder::new(1);
    builder.merge_indexed_row(IndexedMetadataRow {
        chain_index: 0,
        nft_count: 1,
        content_doc: MetadataBm25Document::from_text("gold dragon details").map(Arc::new),
        doc: MetadataBm25Document::from_text("gold dragon rare")
            .unwrap()
            .into(),
        doc_key: metadata_document_key("gold dragon rare"),
    });
    let mut data = builder.finish();
    let directory = tempfile::tempdir().unwrap();
    data.metadata_index
        .remap_if_over_budget(directory.path(), 0)
        .unwrap();
    let mapped_bytes = data.metadata_index.mapped_bytes();
    let resident_bytes = metadata_resident_memory_bytes(&data, None, 1);
    let empty_index = InternedMetadataIndex::from_source_doc_entries(Vec::new());
    let empty_index_bytes = empty_index.logical_memory_bytes();
    let mapped_index = std::mem::replace(&mut data.metadata_index, empty_index);
    let resident_without_index = metadata_resident_memory_bytes(&data, None, 1);
    data.metadata_index = mapped_index;
    let too_small_budget = resident_bytes - 1;

    let returned = remap_metadata_index_for_resident_budget(
        &mut data,
        resident_bytes,
        too_small_budget,
        Some(directory.path()),
    )
    .unwrap();

    assert!(mapped_bytes > 0);
    assert_eq!(
        resident_bytes - resident_without_index,
        mapped_bytes - empty_index_bytes
    );
    assert_eq!(returned, resident_bytes);
    assert!(returned > too_small_budget);
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

    let source = include_str!("mod.rs");
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
fn template_matches_remap_only_when_heap_budget_is_exceeded() {
    let mut matches = MetadataTemplateMatches {
        compatible_docs: CompactMetadataPostings::from_nested(vec![vec![1, 2], vec![0]]),
    };
    let heap_bytes = matches.owned_memory_bytes();
    assert!(heap_bytes > 0);
    let directory = tempfile::tempdir().unwrap();

    assert!(!matches
        .remap_if_over_budget(directory.path(), heap_bytes)
        .unwrap());
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);

    assert!(matches
        .remap_if_over_budget(directory.path(), heap_bytes - 1)
        .unwrap());
    assert_eq!(matches.owned_memory_bytes(), 0);
}

#[test]
fn lexical_metadata_token_dictionary_borrows_existing_tokens() {
    let docs = vec![
        metadata_doc_entry("gold dragon"),
        metadata_doc_entry("silver cat"),
    ];
    let token_ids = lexical_metadata_token_ids(&docs);
    let dictionary_token: &str = token_ids
        .keys()
        .find_map(|token| (*token == "gold").then_some(*token))
        .unwrap();
    let source_token = docs[0]
        .doc
        .terms()
        .iter()
        .map(|(token, _)| token)
        .find(|token| token.as_str() == "gold")
        .unwrap();

    assert_eq!(dictionary_token.as_ptr(), source_token.as_ptr());
}

#[test]
fn metadata_memory_is_guarded_during_build_and_counts_contract_tokens() {
    let mut builder = MetadataDataBuilder::new(1);
    let prefilter = "gold dragon gold rare".to_string();
    builder.merge_indexed_row(IndexedMetadataRow {
        chain_index: 0,
        nft_count: 2,
        content_doc: MetadataBm25Document::from_text("gold dragon details").map(Arc::new),
        doc: MetadataBm25Document::from_text(&prefilter).unwrap().into(),
        doc_key: prefilter,
    });
    let loaded_bytes = builder.memory_bytes();
    let projected_peak = builder.estimated_finish_peak_memory_bytes();

    assert!(projected_peak > loaded_bytes);
    let error = builder
        .ensure_within_memory_budget(projected_peak - 1)
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("projected metadata index build peak"));
    builder.ensure_within_memory_budget(projected_peak).unwrap();
}

#[test]
fn metadata_load_chunk_flushes_before_worst_case_parse_peak() {
    let metadata_json = format!("{{\"description\":\"{}\"}}", "x".repeat(64 * 1024 - 18));
    let row_bytes = metadata_load_row_transient_bytes("ethereum", &metadata_json, None);
    let mut chunk = MetadataLoadChunk::new(row_bytes.saturating_mul(2));

    assert!(chunk
        .try_push(
            0,
            "ethereum",
            &metadata_json,
            1,
            &ReusedMetadataDocuments::new()
        )
        .unwrap());
    assert!(chunk
        .try_push(
            1,
            "ethereum",
            &metadata_json,
            1,
            &ReusedMetadataDocuments::new()
        )
        .unwrap());
    assert!(!chunk
        .try_push(
            2,
            "ethereum",
            &metadata_json,
            1,
            &ReusedMetadataDocuments::new()
        )
        .unwrap());
    assert_eq!(chunk.len(), 2);
}

#[test]
fn metadata_load_chunk_rejects_one_oversized_row_before_storing_it() {
    let metadata_json = r#"{"description":"gold dragon"}"#;
    let required = metadata_load_row_transient_bytes("ethereum", metadata_json, None);
    let mut chunk = MetadataLoadChunk::new(required.saturating_sub(1));

    let error = chunk
        .try_push(
            0,
            "ethereum",
            metadata_json,
            1,
            &ReusedMetadataDocuments::new(),
        )
        .unwrap_err();

    assert!(error.to_string().contains("single metadata row parse peak"));
    assert!(chunk.is_empty());
}

#[test]
fn metadata_load_transient_estimate_covers_cached_prefilter_and_key_clones() {
    let metadata_json = "{}";
    let cached = ReusedMetadataDocument {
        prefilter: MetadataBm25Document::from_text(&"gold dragon ".repeat(1_024)).map(Arc::new),
        content: None,
        doc_key: "cached-template-key".repeat(1_024),
    };
    let uncached_bytes = metadata_load_row_transient_bytes("ethereum", metadata_json, None);
    let cached_bytes = metadata_load_row_transient_bytes("ethereum", metadata_json, Some(&cached));

    assert!(cached_bytes > uncached_bytes);

    let reused = ReusedMetadataDocuments::from([(metadata_json.to_string(), cached)]);
    let mut chunk = MetadataLoadChunk::new(uncached_bytes);
    let error = chunk
        .try_push(0, "ethereum", metadata_json, 1, &reused)
        .unwrap_err();
    assert!(error.to_string().contains("single metadata row parse peak"));
    assert!(chunk.is_empty());
}

fn high_cardinality_metadata_json() -> String {
    const BASE36: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut description = String::new();
    for index in 0usize.. {
        let token = String::from_utf8(vec![
            BASE36[(index / (36 * 36)) % 36],
            BASE36[(index / 36) % 36],
            BASE36[index % 36],
        ])
        .unwrap();
        if description
            .len()
            .saturating_add(token.len())
            .saturating_add(32)
            >= MAX_METADATA_BYTES_FOR_DEDUP
        {
            break;
        }
        if !description.is_empty() {
            description.push(' ');
        }
        description.push_str(&token);
    }
    format!(r#"{{"description":"{description}"}}"#)
}

#[test]
fn metadata_load_transient_estimate_covers_high_cardinality_uncached_documents() {
    let metadata_json = high_cardinality_metadata_json();
    assert!(metadata_json.len() <= MAX_METADATA_BYTES_FOR_DEDUP);

    let documents = metadata_documents_from_json(&metadata_json);
    let prefilter = MetadataBm25Document::from_text(&documents.prefilter).unwrap();
    let content = MetadataBm25Document::from_text(&documents.content).unwrap();
    let doc_key = metadata_document_key(&documents.prefilter);
    let modeled_live_bytes = metadata_json
        .capacity()
        .saturating_add(documents.prefilter.capacity())
        .saturating_add(documents.content.capacity())
        .saturating_add(prefilter.memory_bytes())
        .saturating_add(content.memory_bytes())
        .saturating_add(doc_key.capacity())
        .saturating_add(std::mem::size_of::<MetadataBm25Document>() * 2)
        .saturating_add(std::mem::size_of::<MetadataDocuments>());
    let modeled_live_with_allocator_slack =
        modeled_live_bytes.saturating_add(modeled_live_bytes.saturating_div(4));
    let estimated = metadata_load_row_transient_bytes("ethereum", &metadata_json, None);

    assert!(
        estimated >= modeled_live_with_allocator_slack,
        "estimate {estimated} must cover high-cardinality live state {modeled_live_with_allocator_slack}"
    );
}

#[test]
fn metadata_raw_group_parse_reserve_covers_high_cardinality_content_document() {
    let metadata_json = high_cardinality_metadata_json();
    let shared_loader_estimate = metadata_load_row_transient_bytes("", &metadata_json, None);
    let documents = metadata_documents_from_json(&metadata_json);
    let content = MetadataBm25Document::from_text(&documents.content).unwrap();
    let json_parse_peak = metadata_json
        .capacity()
        .saturating_add(documents.prefilter.capacity())
        .saturating_add(documents.content.capacity())
        .saturating_add(std::mem::size_of::<MetadataDocuments>());
    let content_index_peak = metadata_json
        .capacity()
        .saturating_add(documents.content.capacity())
        .saturating_add(content.memory_bytes())
        .saturating_add(std::mem::size_of::<MetadataBm25Document>());
    let modeled_peak = json_parse_peak.max(content_index_peak);
    let modeled_peak_with_allocator_slack =
        modeled_peak.saturating_add(modeled_peak.saturating_div(4));

    let data = MetadataDataBuilder::new(1).finish();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .build()
        .unwrap();
    let template_matches = MetadataTemplateMatches::default();
    let contract_tokens = CompactContractTokens::default();
    let context = MetadataContentUnionContext {
        data: &data,
        template_compatibility: MetadataTemplateCompatibility::Precomputed(&template_matches),
        contract_tokens: &contract_tokens,
        chain_count: 1,
        pool: &pool,
        recall_mode: MetadataRecallMode::Exact,
    };
    let mut group = MetadataRawTokenGroup::default();
    group.push_raw(0, metadata_json, &context);

    assert!(group.raw_parse_reserve_bytes() >= shared_loader_estimate);
    assert!(
        group.raw_parse_reserve_bytes() >= modeled_peak_with_allocator_slack,
        "raw-group reserve {} must cover high-cardinality live state {modeled_peak_with_allocator_slack}",
        group.raw_parse_reserve_bytes()
    );
}

#[test]
fn metadata_load_chunk_retains_row_ceiling_for_typical_small_rows() {
    let metadata_json = r#"{"description":"gold dragon"}"#;
    let row_bytes = metadata_load_row_transient_bytes("ethereum", metadata_json, None);
    let mut chunk = MetadataLoadChunk::new(
        row_bytes
            .saturating_mul(METADATA_LOAD_CHUNK_ROWS)
            .saturating_add(row_bytes),
    );
    let reused = ReusedMetadataDocuments::new();

    for source_index in 0..METADATA_LOAD_CHUNK_ROWS {
        assert!(chunk
            .try_push(source_index as u32, "ethereum", metadata_json, 1, &reused,)
            .unwrap());
    }
    assert!(!chunk
        .try_push(
            METADATA_LOAD_CHUNK_ROWS as u32,
            "ethereum",
            metadata_json,
            1,
            &reused,
        )
        .unwrap());
    assert_eq!(chunk.len(), METADATA_LOAD_CHUNK_ROWS);
}

#[test]
fn metadata_builder_budget_excludes_load_transient_reserve() {
    assert_eq!(
        metadata_builder_peak_budget_bytes(1_000, 100, 200, 300).unwrap(),
        400
    );
    assert!(metadata_builder_peak_budget_bytes(1_000, 100, 600, 300).is_err());
}

#[test]
fn metadata_structure_budget_subtracts_each_worker_stack() {
    let total = 8 * METADATA_ANALYSIS_WORKER_STACK_BYTES;
    let one_worker = metadata_structure_memory_budget_bytes(total, 1).unwrap();
    let four_workers = metadata_structure_memory_budget_bytes(total, 4).unwrap();

    assert_eq!(
        one_worker.saturating_sub(four_workers),
        3 * METADATA_ANALYSIS_WORKER_STACK_BYTES
    );
    assert!(
        metadata_structure_memory_budget_bytes(4 * METADATA_ANALYSIS_WORKER_STACK_BYTES, 4)
            .is_err()
    );
}

#[test]
fn metadata_worker_stack_is_subtracted_before_pool_and_working_budgets() {
    let source = include_str!("mod.rs");
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
fn metadata_completion_labels_drift_as_a_pre_fallback_sample() {
    let source = include_str!("mod.rs");

    assert!(source.contains("sample drift before fallback contracts/components"));
    assert!(!source.contains("; drift contracts/components"));
}

#[test]
fn metadata_load_transient_reserve_is_capped_to_preserve_resident_index_memory() {
    let gib = 1024usize * 1024 * 1024;
    let reserve = metadata_load_transient_reserve_bytes(
        192usize * gib,
        &["ethereum".to_string(), "solana".to_string()],
    )
    .unwrap();

    assert_eq!(reserve, 4usize * gib);
}

#[test]
fn metadata_pre_token_budget_reserves_contract_token_allocation() {
    assert_eq!(metadata_contract_token_reserve_bytes(10, 25), 256);
    assert_eq!(
        metadata_pre_token_resident_budget_bytes(1_000, 256).unwrap(),
        744
    );
    assert!(metadata_pre_token_resident_budget_bytes(1_000, 1_000).is_err());
}

#[test]
fn metadata_index_is_bounded_before_contract_tokens_are_loaded() {
    let source = include_str!("mod.rs");
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
    let source = include_str!("load.rs");
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
fn metadata_builder_does_not_double_count_cache_owned_content_arc() {
    let shared = Arc::new(MetadataBm25Document::from_text("shared content").unwrap());
    let prefilter = "shared template".to_string();
    let mut cached_builder = MetadataDataBuilder::new(1);
    cached_builder.merge_indexed_row(IndexedMetadataRow {
        chain_index: 0,
        nft_count: 1,
        content_doc: Some(shared.clone()),
        doc: MetadataBm25Document::from_text(&prefilter).unwrap().into(),
        doc_key: prefilter.clone(),
    });
    assert_eq!(cached_builder.content_doc_bytes, 0);

    let mut unique_builder = MetadataDataBuilder::new(1);
    unique_builder.merge_indexed_row(IndexedMetadataRow {
        chain_index: 0,
        nft_count: 1,
        content_doc: MetadataBm25Document::from_text("unique content").map(Arc::new),
        doc: MetadataBm25Document::from_text(&prefilter).unwrap().into(),
        doc_key: prefilter,
    });
    assert!(unique_builder.content_doc_bytes > 0);
}

#[test]
fn metadata_builder_does_not_double_count_cache_owned_template_arc() {
    let shared = Arc::new(MetadataBm25Document::from_text("shared template").unwrap());
    let mut cached_builder = MetadataDataBuilder::new(1);
    cached_builder.merge_indexed_row(IndexedMetadataRow {
        chain_index: 0,
        nft_count: 1,
        content_doc: None,
        doc: shared.clone().into(),
        doc_key: "shared template".to_string(),
    });
    assert_eq!(cached_builder.document_payload_bytes, 0);

    let mut unique_builder = MetadataDataBuilder::new(1);
    unique_builder.merge_indexed_row(IndexedMetadataRow {
        chain_index: 0,
        nft_count: 1,
        content_doc: None,
        doc: MetadataBm25Document::from_text("unique template")
            .unwrap()
            .into(),
        doc_key: "unique template".to_string(),
    });
    assert!(unique_builder.document_payload_bytes > 0);
}

#[test]
fn metadata_hash_memory_includes_raw_buckets_and_control_bytes() {
    let capacity = 14;
    let entry_bytes = std::mem::size_of::<(String, ReusedMetadataDocument)>();
    let measured = hash_table_allocation_bytes(capacity, entry_bytes);

    assert!(measured > capacity * entry_bytes);
    assert!(hash_table_allocation_for_len_upper(15, entry_bytes) >= measured);
}

#[test]
fn metadata_sparse_state_bound_counts_contract_memberships_not_pair_count() {
    assert_eq!(metadata_sparse_membership_factor(0), 0);
    assert_eq!(metadata_sparse_membership_factor(1), 0);
    assert_eq!(metadata_sparse_membership_factor(2), 2);
    assert_eq!(metadata_sparse_membership_factor(3), 3);
    assert_eq!(metadata_sparse_membership_factor(64), 64);
}

#[test]
fn metadata_scoring_state_is_released_before_summary_scratch() {
    let mut builder = MetadataDataBuilder::new(1);
    let prefilter = "gold dragon".to_string();
    builder.merge_indexed_row(IndexedMetadataRow {
        chain_index: 0,
        nft_count: 2,
        content_doc: MetadataBm25Document::from_text("gold dragon details").map(Arc::new),
        doc: MetadataBm25Document::from_text(&prefilter).unwrap().into(),
        doc_key: prefilter,
    });
    let mut data = builder.finish();
    assert_eq!(data.metadata_index.doc_count(), 1);
    assert!(data.contracts[0].content_doc.is_some());

    release_metadata_scoring_state(&mut data);

    assert!(data.metadata_index.is_empty());
    assert!(data.compact_contract_indexes_by_source.is_empty());
    assert!(data.reused_documents.is_empty());
    assert!(data.contracts[0].content_doc.is_none());
    assert_eq!(data.contracts[0].nft_count, 2);
}

#[test]
fn prepared_metadata_doc_score_matches_bm25_terms() {
    let docs = vec![
        metadata_doc_entry("gold dragon gold rare"),
        metadata_doc_entry("gold dragon rare shiny"),
    ];
    let token_ids = lexical_metadata_token_ids(&docs);
    let left = InternedMetadataSourceDoc::from_metadata_doc(&docs[0].doc, &token_ids);
    let right = InternedMetadataSourceDoc::from_metadata_doc(&docs[1].doc, &token_ids);
    let source_docs = vec![left, right];
    let corpus = InternedMetadataCorpus::from_doc_weights(&[1, 1], &source_docs, token_ids.len());
    let terms = source_docs[0]
        .terms()
        .iter()
        .map(|&(token, frequency)| (token as usize, frequency as usize))
        .collect::<Vec<_>>();
    let denominator = bm25_score_terms(&terms, &source_docs[0], &corpus);
    let expected =
        (bm25_score_terms(&terms, &source_docs[1], &corpus) / denominator).clamp(0.0, 1.0);

    let index = InternedMetadataIndex::from_source_doc_entries(docs);

    let actual = index.scoring.score(0, 1);

    assert!((actual - expected).abs() < 1e-12);
}

#[test]
fn metadata_data_builder_builds_bm25_index_for_content_representative_matching() {
    let mut builder = MetadataDataBuilder::new(1);
    let doc = MetadataBm25Document::from_text("gold dragon").unwrap();
    let doc_key = metadata_document_key("gold dragon");
    builder.merge_indexed_row(IndexedMetadataRow {
        chain_index: 0,
        nft_count: 2,
        content_doc: MetadataBm25Document::from_text("gold dragon").map(Arc::new),
        doc: doc.into(),
        doc_key,
    });

    let data = builder.finish();

    assert_eq!(data.metadata_index.doc_count(), 1);
    assert!(data.metadata_index.token_id("gold").is_some());
}

#[test]
fn metadata_memberships_use_compact_contract_indexes() {
    let mut builder = MetadataDataBuilder::new(1);
    let doc = MetadataBm25Document::from_text("gold dragon").unwrap();
    let doc_key = metadata_document_key("gold dragon");
    builder.merge_indexed_row(IndexedMetadataRow {
        chain_index: 0,
        nft_count: 2,
        content_doc: MetadataBm25Document::from_text("gold dragon").map(Arc::new),
        doc: doc.into(),
        doc_key,
    });

    let data = builder.finish();

    let _: &[MetadataContractIndex] = data.contracts_by_chain[0].as_slice();
}

#[test]
fn metadata_contracts_keep_their_template_document_index() {
    let mut builder = MetadataDataBuilder::new(1);
    for (_, document) in [
        ("0xaaa", "gold dragon"),
        ("0xbbb", "gold dragon"),
        ("0xccc", "silver cat"),
    ] {
        builder.merge_indexed_row(IndexedMetadataRow {
            chain_index: 0,
            nft_count: 1,
            content_doc: MetadataBm25Document::from_text(document).map(Arc::new),
            doc: MetadataBm25Document::from_text(document).unwrap().into(),
            doc_key: metadata_document_key(document),
        });
    }

    let data = builder.finish();

    assert_eq!(
        data.contracts
            .iter()
            .map(|contract| contract.template_doc_index)
            .collect::<Vec<_>>(),
        vec![0, 0, 1]
    );
}

#[test]
fn metadata_index_consumes_source_docs_without_retaining_contract_memberships() {
    let docs = vec![metadata_doc_entry("gold dragon")];

    let index = InternedMetadataIndex::from_source_doc_entries(docs);

    assert_eq!(index.doc_count(), 1);
    assert!(index.token_id("gold").is_some());
}

#[test]
fn interned_metadata_index_keeps_scores_without_redundant_document_postings() {
    let docs = vec![metadata_doc_entry("gold dragon gold")];

    let index = InternedMetadataIndex::from_source_doc_entries(docs);

    assert_eq!(index.doc_count(), 1);
    assert_eq!(index.scoring.query_terms_len(0), 2);
}

#[test]
fn metadata_raw_row_chunk_indexes_valid_rows_only() {
    let chains = ["ethereum".to_string(), "base".to_string()];
    let chain_indexes = chains
        .iter()
        .enumerate()
        .map(|(index, chain)| (chain.as_str(), index))
        .collect::<HashMap<_, _>>();
    let rows = vec![
        RawMetadataRow {
            chain: "ethereum".into(),
            metadata_json: r#"{"description":"gold dragon"}"#.into(),
            nft_count: 2,
        },
        RawMetadataRow {
            chain: "missing".into(),
            metadata_json: r#"{"description":"gold dragon"}"#.into(),
            nft_count: 1,
        },
        RawMetadataRow {
            chain: "base".into(),
            metadata_json: "not json".into(),
            nft_count: 1,
        },
    ];

    let indexed = index_metadata_raw_row_chunk(
        rows.into_iter()
            .enumerate()
            .map(|(index, row)| (index as u32, row))
            .collect(),
        &chain_indexes,
    );

    assert_eq!(indexed.len(), 1);
    assert_eq!(indexed[0].1.chain_index, 0);
    assert_eq!(indexed[0].1.nft_count, 2);
}

#[test]
fn metadata_index_build_uses_configured_rayon_pool() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
        CREATE TEMP TABLE source_rows AS
        SELECT * FROM (
            VALUES
            ('ethereum', '0xaaa', '1', '', '{"description":"gold dragon"}'),
            ('ethereum', '0xbbb', '1', '', '{"description":"silver cat"}')
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
    let global_threads = rayon::current_num_threads();
    let configured_threads = if global_threads == 1 { 2 } else { 1 };
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(configured_threads)
        .build()
        .unwrap();

    let data = load_metadata_data(
        &conn,
        &["ethereum".to_string()],
        &pool,
        ReusedMetadataDocuments::new(),
        MetadataLoadBudgets::new(usize::MAX, usize::MAX),
        None,
    )
    .unwrap();

    assert_eq!(data.metadata_index.build_thread_count, configured_threads);
}

#[test]
fn metadata_raw_row_builds_distinct_prefilter_and_content_documents() {
    let chains = ["ethereum".to_string()];
    let chain_indexes = chains
        .iter()
        .enumerate()
        .map(|(index, chain)| (chain.as_str(), index))
        .collect::<HashMap<_, _>>();
    let rows = vec![RawMetadataRow {
        chain: "ethereum".into(),
        metadata_json: r#"{
            "name":"Alpha #1",
            "image":"ipfs://alpha/1.png",
            "attributes":[{"trait_type":"Background","value":"Blue"}]
        }"#
        .into(),
        nft_count: 2,
    }];

    let indexed =
        index_metadata_raw_row_chunk(vec![(0, rows.into_iter().next().unwrap())], &chain_indexes);

    assert_eq!(
        indexed[0]
            .1
            .doc
            .terms()
            .iter()
            .map(|(token, _)| token.as_str())
            .collect::<Vec<_>>()
            .join(" "),
        "attributes background image name trait_type value"
    );
    let content = indexed[0].1.content_doc.as_ref().unwrap();
    assert!(content.term_frequency("ipfs") > 0);
    assert!(content.term_frequency("png") > 0);
    assert!(content.term_frequency("blue") > 0);
}

#[test]
fn metadata_raw_row_chunk_preserves_input_order_of_survivors() {
    // `load_metadata_fallback_rows` builds `raw_rows` in SQL order
    // (`ORDER BY metadata_contract_index, token_id, stable SourceId`) and keeps the
    // first indexed row per source contract, so it depends on
    // `index_metadata_raw_row_chunk` preserving the relative input order of
    // surviving rows. Verify that across a chunk large enough to be split
    // across rayon workers: the survivor at output position N must be the
    // N-th input row. (Rayon's `filter_map` produces a non-indexed iterator
    // whose `collect` does not guarantee order; this test pins the
    // order-preservation contract the caller relies on.)
    let chains = ["ethereum".to_string()];
    let chain_indexes = chains
        .iter()
        .enumerate()
        .map(|(index, chain)| (chain.as_str(), index))
        .collect::<HashMap<_, _>>();
    let count = 60_000usize;
    let raw_rows: Vec<(u32, RawMetadataRow)> = (0..count)
        .map(|position| {
            (
                position as u32,
                RawMetadataRow {
                    chain: "ethereum".into(),
                    metadata_json: format!(r#"{{"description":"row {position}}}"#),
                    nft_count: 1,
                },
            )
        })
        .collect();
    let indexed = index_metadata_raw_row_chunk(raw_rows, &chain_indexes);

    assert_eq!(indexed.len(), count, "every row should survive");
    for (output_position, (source, _)) in indexed.iter().enumerate() {
        assert_eq!(
            *source as usize, output_position,
            "survivor at output position {output_position} has source {source}, \
             expected {output_position} — input order not preserved"
        );
    }
}

#[test]
fn metadata_parallel_filter_keeps_an_indexed_output_before_flattening() {
    let source = include_str!("load.rs");
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

#[test]
fn metadata_documents_parse_once_and_preserve_both_semantics() {
    let raw = r#"{"name":"Seed #1","description":"Gold Dragon","attributes":[{"trait_type":"Background","value":"Red"}],"image":"ipfs://seed/1.png"}"#;

    let documents = metadata_documents_from_json(raw);

    assert_eq!(
        documents.prefilter,
        metadata_prefilter_document_from_json(raw)
    );
    assert_eq!(documents.content, metadata_document_from_json(raw));
    assert!(documents.prefilter.contains("description"));
    assert!(documents.content.contains("gold dragon"));
}
