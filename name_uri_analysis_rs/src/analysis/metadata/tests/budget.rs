use super::*;

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
fn metadata_load_transient_reserve_can_grow_to_one_maximum_row() {
    let analysis_memory_bytes = 32usize * 1024 * 1024;
    let maximum_row_bytes = metadata_load_row_transient_bytes(
        "ethereum",
        &"x".repeat(MAX_METADATA_BYTES_FOR_DEDUP),
        None,
    );

    let reserve =
        metadata_load_transient_reserve_bytes(analysis_memory_bytes, &["ethereum".to_string()])
            .unwrap();

    assert_eq!(reserve, maximum_row_bytes);
    assert!(reserve < analysis_memory_bytes);
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
