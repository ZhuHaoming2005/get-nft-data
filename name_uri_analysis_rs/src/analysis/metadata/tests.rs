use super::*;
use super::parse::*;
use super::sketch::*;
use super::super::analysis_contracts_sql;

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
fn metadata_sketch_source_match_uses_anchor_or_hamming_distance() {
    let anchored_left = MetadataSketch {
        simhash: 0,
        anchors: vec![1, 3],
    };
    let anchored_right = MetadataSketch {
        simhash: u64::MAX,
        anchors: vec![3, 5],
    };
    let near_left = MetadataSketch {
        simhash: 1_u64 << 63,
        anchors: Vec::new(),
    };
    let near_right = MetadataSketch {
        simhash: (1_u64 << 63) | ((1_u64 << 31) - 1),
        anchors: Vec::new(),
    };
    let far_right = MetadataSketch {
        simhash: (1_u64 << 63) | ((1_u64 << 33) - 1),
        anchors: Vec::new(),
    };

    assert!(metadata_sketch_source_match(
        &anchored_left,
        &anchored_right,
        32
    ));
    assert!(metadata_sketch_source_match(&near_left, &near_right, 32));
    assert!(!metadata_sketch_source_match(&near_left, &far_right, 32));
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
            contract_index: metadata_contract_index_from_usize(
                contract_index,
            ),
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
        (compact_metadata_content_pair_score(
            &compact.docs[0],
            &compact.docs[1],
        ) - metadata_content_pair_score(&left, &identical))
            .abs()
            < 1e-9
    );
    assert!(
        (compact_metadata_content_pair_score(
            &compact.docs[0],
            &compact.docs[2],
        ) - metadata_content_pair_score(&left, &unrelated))
            .abs()
            < 1e-9
    );
}

#[test]
fn metadata_template_matches_accept_exact_or_scored_document_pairs() {
    let matches = MetadataTemplateMatches::from_pairs([(2usize, 5usize), (1, 4)]);

    assert!(matches.matches(3, 3));
    assert!(matches.matches(2, 5));
    assert!(matches.matches(5, 2));
    assert!(!matches.matches(2, 4));
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
fn compact_token_index_preserves_lexical_token_order() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
        CREATE TEMP TABLE analysis_rows AS
        SELECT * FROM (
            VALUES
            ('ethereum', '0xaaa', '10', '{"description":"a ten"}'),
            ('ethereum', '0xaaa', '2',  '{"description":"shared token"}'),
            ('ethereum', '0xbbb', '2',  '{"description":"shared token"}'),
            ('ethereum', '0xbbb', '3',  '{"description":"b three"}')
        ) AS t(chain, contract_address, token_id, metadata_json);
        CREATE TEMP TABLE analysis_contracts AS
        SELECT chain,
               contract_address,
               row_number() OVER (ORDER BY chain, contract_address) - 1
                   AS metadata_contract_index
        FROM analysis_rows
        GROUP BY chain, contract_address;
        "#,
    )
    .unwrap();
    let mut builder = MetadataDataBuilder::new(1);
    for _ in ["0xaaa", "0xbbb"] {
        let prefilter = "description".to_string();
        builder.merge_indexed_row(IndexedMetadataRow {
            chain_index: 0,
            nft_count: 2,
            content_document: "shared token".to_string(),
            doc: MetadataBm25Document::from_text(&prefilter).unwrap(),
            doc_key: prefilter,
        });
    }
    let data = builder.finish();
    prepare_metadata_contract_token_rows(&conn).unwrap();
    let contract_tokens = load_metadata_contract_tokens(&conn, &data).unwrap();
    assert_eq!(contract_tokens, vec![vec![0, 1], vec![1, 2]]);
}

#[test]
fn skipped_metadata_documents_preserve_source_to_compact_contract_mapping() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
        CREATE TEMP TABLE analysis_rows AS
        SELECT * FROM (
            VALUES
            ('ethereum', '0xaaa', '1', '', '{"description":"gold dragon"}'),
            ('ethereum', '0xbbb', '1', '', '{}'),
            ('ethereum', '0xccc', '1', '', '{"description":"gold dragon"}')
        ) AS t(chain, contract_address, token_id, name_norm, metadata_json);
        "#,
    )
    .unwrap();
    conn.execute_batch(&analysis_contracts_sql()).unwrap();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .unwrap();

    let data =
        load_metadata_data(&conn, &["ethereum".to_string()], &pool).unwrap();
    prepare_metadata_contract_token_rows(&conn).unwrap();
    let contract_tokens = load_metadata_contract_tokens(&conn, &data).unwrap();

    assert_eq!(data.contracts.len(), 2);
    assert_eq!(data.compact_contract_index_for_source(0), Some(0));
    assert_eq!(data.compact_contract_index_for_source(1), None);
    assert_eq!(data.compact_contract_index_for_source(2), Some(1));
    assert_eq!(contract_tokens, vec![vec![0], vec![0]]);
}

#[test]
fn token_content_groups_union_matches_without_contract_pair_table() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
        CREATE TEMP TABLE analysis_rows AS
        SELECT * FROM (
            VALUES
            ('ethereum', '0xaaa', '1', '{"description":"different lower"}'),
            ('ethereum', '0xaaa', '2', '{"description":"gold dragon"}'),
            ('ethereum', '0xbbb', '2', '{"description":"gold dragon"}')
        ) AS t(chain, contract_address, token_id, metadata_json);
        CREATE TEMP TABLE analysis_contracts AS
        SELECT chain,
               contract_address,
               row_number() OVER (ORDER BY chain, contract_address) - 1
                   AS metadata_contract_index
        FROM analysis_rows
        GROUP BY chain, contract_address;
        "#,
    )
    .unwrap();
    let mut builder = MetadataDataBuilder::new(1);
    for _ in ["0xaaa", "0xbbb"] {
        builder.merge_indexed_row(IndexedMetadataRow {
            chain_index: 0,
            nft_count: 2,
            content_document: "gold dragon".to_string(),
            doc: MetadataBm25Document::from_text("shared template").unwrap(),
            doc_key: metadata_document_key("shared template"),
        });
    }
    let data = builder.finish();
    prepare_metadata_contract_token_rows(&conn).unwrap();
    let contract_tokens = load_metadata_contract_tokens(&conn, &data).unwrap();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .unwrap();
    let mut state = MetadataUnionState {
        intra: UnionFind::new(2),
        cross: None,
        chain_matrix: None,
    };
    let template_matches = MetadataTemplateMatches::default();
    let context = MetadataContentUnionContext {
        data: &data,
        template_matches: &template_matches,
        contract_tokens: &contract_tokens,
        chain_count: 1,
        pool: &pool,
    };

    union_metadata_token_content_matches(&conn, &context, &mut state)
        .unwrap();

    assert_eq!(state.intra.find(0), state.intra.find(1));
}

fn metadata_doc_entry(text: &str) -> SourceMetadataDocEntry {
    SourceMetadataDocEntry {
        doc: MetadataBm25Document::from_text(text).unwrap(),
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
    let document = metadata_document_from_json(
        r#"{"description":"\uFF27\uFF4F\uFF4C\uFF44\u3000Dragon"}"#,
    );

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
    let scratch_pool = MetadataCandidateScratchPool::new(index.docs.len());

    let batch = collect_metadata_doc_pair_hits_for_left_range(
        1..3,
        MetadataPairScoringContext {
            docs: &index.docs,
            sketches: &index.sketches,
            postings: &index.postings,
            queries: &index.queries,
            prepared_docs: &index.prepared_docs,
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
fn metadata_doc_pair_prefilter_uses_sketch_instead_of_rare_anchor_gate() {
    let shared =
        "attributes image name trait_type value description external_url animation_url \
         metadata raw collection creator royalty license marketplace contract chain story \
         lore summary";
    let docs = vec![
        metadata_doc_entry(&format!("{shared} alpha")),
        metadata_doc_entry(&format!("{shared} beta")),
        metadata_doc_entry(&format!("{shared} gamma")),
    ];
    let index = InternedMetadataIndex::from_source_doc_entries(docs);
    let scratch_pool = MetadataCandidateScratchPool::new(index.docs.len());

    let batch = collect_metadata_doc_pair_hits_for_left_range(
        0..1,
        MetadataPairScoringContext {
            docs: &index.docs,
            sketches: &index.sketches,
            postings: &index.postings,
            queries: &index.queries,
            prepared_docs: &index.prepared_docs,
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
    docs.extend(
        (0..96).map(|index| metadata_doc_entry(&format!("{shared} unrelated_{index}"))),
    );
    let index = InternedMetadataIndex::from_source_doc_entries(docs);
    let scratch_pool = MetadataCandidateScratchPool::new(index.docs.len());
    let context = MetadataPairScoringContext {
        docs: &index.docs,
        sketches: &index.sketches,
        postings: &index.postings,
        queries: &index.queries,
        prepared_docs: &index.prepared_docs,
    };
    let mut scratch = scratch_pool.take();

    let candidates =
        metadata_candidate_indices_for_left_with_scratch(0, &context, &mut scratch).to_vec();
    let brute_force_matches = (1..index.docs.len())
        .filter(|&right| {
            metadata_sketch_source_match(
                &index.sketches[0],
                &index.sketches[right],
                METADATA_SKETCH_SOURCE_HAMMING_THRESHOLD,
            ) && score_metadata_with_prepared_doc(
                &index.queries[0],
                &index.prepared_docs[right],
            ) >= METADATA_THRESHOLD
        })
        .map(metadata_doc_index_from_usize)
        .collect::<Vec<_>>();

    assert!(index.queries[0].candidate_tokens.len() < index.queries[0].terms.len());
    assert!(candidates.len() < index.docs.len() / 4);
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
        docs: &index.docs,
        sketches: &index.sketches,
        postings: &index.postings,
        queries: &index.queries,
        prepared_docs: &index.prepared_docs,
    };
    let scratch_pool = MetadataCandidateScratchPool::new(index.docs.len());
    let actual = collect_metadata_doc_pair_hits_for_left_range(
        0..index.docs.len(),
        context,
        &scratch_pool,
    )
    .hits;
    let mut expected = Vec::new();
    for left in 0..index.docs.len() {
        for right in left + 1..index.docs.len() {
            if metadata_sketch_source_match(
                &index.sketches[left],
                &index.sketches[right],
                METADATA_SKETCH_SOURCE_HAMMING_THRESHOLD,
            ) && (score_metadata_with_prepared_doc(
                &index.queries[left],
                &index.prepared_docs[right],
            ) >= METADATA_THRESHOLD
                || score_metadata_with_prepared_doc(
                    &index.queries[right],
                    &index.prepared_docs[left],
                ) >= METADATA_THRESHOLD)
            {
                expected.push((left, right));
            }
        }
    }

    assert_eq!(actual, expected);
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
            contract_index: metadata_contract_index_from_usize(
                contract_index,
            ),
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
    let hits = collect_metadata_content_atom_pair_hits(
        &candidates,
        &atoms,
        &compact.docs,
        &pool,
    );
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
            content_document: "gold dragon".to_string(),
            doc: MetadataBm25Document::from_text("shared template").unwrap(),
            doc_key: metadata_document_key("shared template"),
        });
    }
    let data = builder.finish();
    let template_matches = MetadataTemplateMatches::default();
    let contract_tokens = vec![vec![1, 4], vec![1, 4], vec![4], vec![4]];
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
        template_matches: &template_matches,
        contract_tokens: &contract_tokens,
        chain_count: 1,
        pool: &pool,
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
            content_document: "gold dragon".to_string(),
            doc: MetadataBm25Document::from_text("shared template").unwrap(),
            doc_key: metadata_document_key("shared template"),
        });
    }
    let data = builder.finish();
    let template_matches = MetadataTemplateMatches::default();
    let contract_tokens = vec![vec![1], vec![1], vec![1], vec![1]];
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
        template_matches: &template_matches,
        contract_tokens: &contract_tokens,
        chain_count: 1,
        pool: &pool,
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
            content_document: "gold dragon rare".to_string(),
            doc: MetadataBm25Document::from_text("shared template").unwrap(),
            doc_key: metadata_document_key("shared template"),
        });
    }
    let data = builder.finish();
    let template_matches = MetadataTemplateMatches::default();
    let contract_tokens = vec![vec![1], vec![1]];
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
        template_matches: &template_matches,
        contract_tokens: &contract_tokens,
        chain_count: 1,
        pool: &pool,
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
            content_document: "gold dragon".to_string(),
            doc: MetadataBm25Document::from_text("shared template").unwrap(),
            doc_key: metadata_document_key("shared template"),
        });
    }
    let data = builder.finish();
    let template_matches = MetadataTemplateMatches::default();
    let contract_tokens = vec![vec![1], vec![1], vec![1], vec![1]];
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
        template_matches: &template_matches,
        contract_tokens: &contract_tokens,
        chain_count: 2,
        pool: &pool,
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
            content_document: "gold dragon".to_string(),
            doc: MetadataBm25Document::from_text("shared template").unwrap(),
            doc_key: metadata_document_key("shared template"),
        });
    }
    let data = builder.finish();
    let template_matches = MetadataTemplateMatches::default();
    let contract_tokens = vec![vec![1, 2], vec![2], vec![1, 2], vec![2]];
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
        template_matches: &template_matches,
        contract_tokens: &contract_tokens,
        chain_count: 2,
        pool: &pool,
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
            content_document: content.to_string(),
            doc: MetadataBm25Document::from_text("shared template").unwrap(),
            doc_key: metadata_document_key("shared template"),
        });
    }
    let data = builder.finish();
    let contract_tokens = vec![vec![1], vec![2], vec![3], vec![3]];
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
        template_matches: &template_matches,
        contract_tokens: &contract_tokens,
        chain_count: 1,
        pool: &pool,
    };

    union_metadata_representative_content_fallback(&context, &mut state);

    assert_eq!(state.intra.find(0), state.intra.find(1));
    assert_ne!(state.intra.find(2), state.intra.find(3));
}

#[test]
fn metadata_fallback_atoms_collapse_identical_nonempty_token_sets_without_unioning() {
    let mut builder = MetadataDataBuilder::new(1);
    for _ in ["0xaaa", "0xbbb", "0xccc", "0xddd"] {
        builder.merge_indexed_row(IndexedMetadataRow {
            chain_index: 0,
            nft_count: 1,
            content_document: "gold dragon".to_string(),
            doc: MetadataBm25Document::from_text("shared template").unwrap(),
            doc_key: metadata_document_key("shared template"),
        });
    }
    let data = builder.finish();
    let template_matches = MetadataTemplateMatches::default();
    let contract_tokens = vec![vec![1]; 4];
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
        template_matches: &template_matches,
        contract_tokens: &contract_tokens,
        chain_count: 1,
        pool: &pool,
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
            content_document: "gold dragon".to_string(),
            doc: MetadataBm25Document::from_text("shared template").unwrap(),
            doc_key: metadata_document_key("shared template"),
        });
    }
    let data = builder.finish();
    let template_matches = MetadataTemplateMatches::default();
    let contract_tokens = vec![Vec::new(); 4];
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
        template_matches: &template_matches,
        contract_tokens: &contract_tokens,
        chain_count: 1,
        pool: &pool,
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
            content_document: "gold dragon".to_string(),
            doc: MetadataBm25Document::from_text("shared template").unwrap(),
            doc_key: metadata_document_key("shared template"),
        });
    }
    let data = builder.finish();
    let template_matches = MetadataTemplateMatches::default();
    let contract_tokens = (0..CONTRACT_COUNT)
        .map(|index| vec![u32::try_from(index).unwrap()])
        .collect::<Vec<_>>();
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
        template_matches: &template_matches,
        contract_tokens: &contract_tokens,
        chain_count: 1,
        pool: &pool,
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
            content_document: (*content).to_string(),
            doc: MetadataBm25Document::from_text("shared template").unwrap(),
            doc_key: metadata_document_key("shared template"),
        });
    }
    let data = builder.finish();
    let template_matches = MetadataTemplateMatches::default();
    let contract_tokens = fixtures
        .iter()
        .map(|(_, _, _, tokens)| tokens.clone())
        .collect::<Vec<_>>();
    let records = fixtures
        .iter()
        .enumerate()
        .map(|(contract_index, (_, _, content, _))| MetadataContentRecord {
            contract_index: metadata_contract_index_from_usize(contract_index),
            doc: MetadataBm25Document::from_text(content).unwrap().into(),
        })
        .collect::<Vec<_>>();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap();
    let context = MetadataContentUnionContext {
        data: &data,
        template_matches: &template_matches,
        contract_tokens: &contract_tokens,
        chain_count: 2,
        pool: &pool,
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
            if lowest_common_metadata_token(
                &contract_tokens[left],
                &contract_tokens[right],
            )
            .is_none()
                && metadata_content_pair_matches(
                    &compact.docs[left],
                    &compact.docs[right],
                    METADATA_THRESHOLD,
                )
            {
                apply_metadata_contract_pair_union(
                    &data,
                    2,
                    &mut reference,
                    left,
                    right,
                );
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
                optimized.chain_matrix.as_mut().unwrap()[0]
                    .connected(left, right),
                reference.chain_matrix.as_mut().unwrap()[0]
                    .connected(left, right),
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
    let mut scratch = MetadataCandidateScratch::new(index.docs.len());
    let mut hits = Vec::new();

    let candidate_pairs = collect_metadata_doc_pair_hits_for_left_with_scratch(
        0,
        &MetadataPairScoringContext {
            docs: &index.docs,
            sketches: &index.sketches,
            postings: &index.postings,
            queries: &index.queries,
            prepared_docs: &index.prepared_docs,
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
fn metadata_bm25_index_interns_tokens_and_integer_postings() {
    let docs = vec![
        metadata_doc_entry("gold dragon"),
        metadata_doc_entry("dragon gold"),
        metadata_doc_entry("silver cat"),
    ];
    let index = InternedMetadataIndex::from_source_doc_entries(docs);
    let gold = index.token_id("gold").unwrap();
    let dragon = index.token_id("dragon").unwrap();

    assert_eq!(index.postings[gold], vec![0, 1]);
    assert_eq!(index.postings[dragon], vec![0, 1]);
    let _: &[u32] = index.postings[gold].as_slice();
    assert!(index.docs[0].unique_tokens().contains(&gold));
    assert!(index.queries[0].terms.iter().any(|(token, tf)| {
        *token == gold && *tf == 1
    }));
}

#[test]
fn metadata_bm25_index_builds_top_contract_sketches() {
    let docs = vec![
        metadata_doc_entry("gold dragon"),
        metadata_doc_entry("dragon silver"),
        metadata_doc_entry("cat"),
    ];

    let index = InternedMetadataIndex::from_source_doc_entries(docs);

    assert_eq!(index.sketches.len(), 3);
    assert!(index.sketches.iter().all(|sketch| sketch.simhash != 0));
    assert!(index
        .sketches
        .iter()
        .all(|sketch| sketch.anchors.len() <= METADATA_SKETCH_ANCHOR_COUNT));
}

#[test]
fn metadata_bm25_index_assigns_lexical_token_ids_for_stable_score_order() {
    let docs = vec![
        metadata_doc_entry("gold dragon"),
        metadata_doc_entry("silver cat"),
    ];
    let index = InternedMetadataIndex::from_source_doc_entries(docs);

    assert!(
        index.token_id("cat").unwrap()
            < index.token_id("dragon").unwrap()
            && index.token_id("dragon").unwrap() < index.token_id("gold").unwrap()
            && index.token_id("gold").unwrap() < index.token_id("silver").unwrap()
    );
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
    let terms = query_terms_from_token_ids(&source_docs[0].tokens);
    let denominator = bm25_score_terms(&terms, &source_docs[0], &corpus);
    let expected = (bm25_score_terms(&terms, &source_docs[1], &corpus) / denominator)
        .clamp(0.0, 1.0);

    let index = InternedMetadataIndex::from_source_doc_entries(docs);

    let actual = score_metadata_with_prepared_doc(&index.queries[0], &index.prepared_docs[1]);

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
        content_document: "gold dragon".to_string(),
        doc,
        doc_key,
    });

    let data = builder.finish();

    assert_eq!(data.metadata_index.docs.len(), 1);
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
        content_document: "gold dragon".to_string(),
        doc,
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
            content_document: document.to_string(),
            doc: MetadataBm25Document::from_text(document).unwrap(),
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

    assert_eq!(index.docs.len(), 1);
    assert!(index.token_id("gold").is_some());
}

#[test]
fn interned_metadata_index_keeps_only_compact_candidate_docs_after_preparation() {
    let docs = vec![metadata_doc_entry("gold dragon gold")];

    let index = InternedMetadataIndex::from_source_doc_entries(docs);

    assert_eq!(
        std::mem::size_of::<InternedMetadataDoc>(),
        std::mem::size_of::<Vec<usize>>()
    );
    assert_eq!(index.docs[0].unique_tokens().len(), 2);
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
        CREATE TEMP TABLE analysis_rows AS
        SELECT * FROM (
            VALUES
            ('ethereum', '0xaaa', '1', '', '{"description":"gold dragon"}'),
            ('ethereum', '0xbbb', '1', '', '{"description":"silver cat"}')
        ) AS t(chain, contract_address, token_id, name_norm, metadata_json);
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
    )
    .unwrap();

    assert_eq!(
        data.metadata_index.build_thread_count,
        configured_threads
    );
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
        indexed[0].1.doc.tokens.join(" "),
        "attributes background image name trait_type value"
    );
    assert!(indexed[0].1.content_document.contains("ipfs://alpha/1.png"));
    assert!(indexed[0].1.content_document.contains("blue"));
    assert!(!indexed[0].1.content_document.contains("alpha #1"));
}

#[test]
fn metadata_raw_row_chunk_preserves_input_order_of_survivors() {
    // `load_metadata_fallback_rows` builds `raw_rows` in SQL order
    // (`ORDER BY metadata_contract_index, token_id, rowid`) and keeps the
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

