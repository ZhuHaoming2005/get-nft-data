use super::*;

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
