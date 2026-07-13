use super::*;

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
