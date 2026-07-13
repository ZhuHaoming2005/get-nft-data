use super::*;

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
fn metadata_template_matches_accept_exact_or_scored_document_pairs() {
    let matches = MetadataTemplateMatches::from_pairs(6, vec![(2, 5), (1, 4)]);

    assert!(matches.matches(3, 3));
    assert!(matches.matches(2, 5));
    assert!(matches.matches(5, 2));
    assert!(!matches.matches(2, 4));
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
