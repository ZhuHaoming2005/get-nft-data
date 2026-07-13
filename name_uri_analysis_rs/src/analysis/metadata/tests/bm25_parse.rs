use super::*;

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
