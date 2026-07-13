use super::*;

#[test]
fn load_snapshot_recalls_metadata_from_only_one_seed_example() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[DatabaseNftRecord {
                contract_address: "0xcandidate".into(),
                token_id: "1".into(),
                metadata_json: r#"{"second_unique":"silver cat"}"#.into(),
                ..Default::default()
            }],
        )
        .unwrap();
    let seed_nfts = vec![
        SeedNft {
            contract_address: "0xseed".into(),
            token_id: "1".into(),
            metadata_json: r#"{"first_unique":"gold dragon"}"#.into(),
            ..Default::default()
        },
        SeedNft {
            contract_address: "0xseed".into(),
            token_id: "2".into(),
            metadata_json: r#"{"second_unique":"silver cat"}"#.into(),
            ..Default::default()
        },
    ];

    let snapshot = store
        .load_snapshot("ethereum", &seed_nfts, 95.0, 0.6, 0, 0)
        .unwrap();

    assert!(snapshot.nft_rows.is_empty());
}

#[test]
fn load_snapshot_marks_rows_that_were_recalled_by_metadata() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[
                DatabaseNftRecord {
                    contract_address: "0xmetadata".into(),
                    token_id: "1".into(),
                    metadata_json: r#"{"description":"gold dragon"}"#.into(),
                    ..Default::default()
                },
                DatabaseNftRecord {
                    contract_address: "0ximage".into(),
                    token_id: "1".into(),
                    image_uri: "ipfs://seed-image.png".into(),
                    ..Default::default()
                },
            ],
        )
        .unwrap();
    let seed_nfts = vec![SeedNft {
        contract_address: "0xseed".into(),
        token_id: "1".into(),
        image_uri: "ipfs://seed-image.png".into(),
        metadata_json: r#"{"description":"gold dragon"}"#.into(),
        ..Default::default()
    }];

    let snapshot = store
        .load_snapshot("ethereum", &seed_nfts, 95.0, 0.6, 0, 0)
        .unwrap();
    let by_contract: BTreeMap<_, _> = snapshot
        .nft_rows
        .iter()
        .map(|row| (row.contract_address.as_str(), row))
        .collect();

    assert!(by_contract["0xmetadata"].metadata_recall_checked);
    assert!(by_contract["0xmetadata"].metadata_recall_match);
    assert!(by_contract["0ximage"].metadata_recall_checked);
    assert!(!by_contract["0ximage"].metadata_recall_match);
}

#[test]
fn load_snapshot_uses_max_recall_rows_as_batch_size_not_total_limit() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[
                DatabaseNftRecord {
                    contract_address: "0xcandidate_a".into(),
                    token_id: "1".into(),
                    token_uri: "ipfs://shared/1".into(),
                    ..Default::default()
                },
                DatabaseNftRecord {
                    contract_address: "0xcandidate_b".into(),
                    token_id: "1".into(),
                    token_uri: "ipfs://shared/1".into(),
                    ..Default::default()
                },
            ],
        )
        .unwrap();
    let seed_nfts = vec![SeedNft {
        contract_address: "0xseed".into(),
        token_id: "1".into(),
        token_uri: "ipfs://shared/1".into(),
        ..Default::default()
    }];

    let snapshot = store
        .load_snapshot("ethereum", &seed_nfts, 95.0, 0.6, 0, 1)
        .unwrap();
    let contracts: Vec<_> = snapshot
        .nft_rows
        .iter()
        .map(|row| row.contract_address.as_str())
        .collect();

    assert_eq!(contracts, vec!["0xcandidate_a", "0xcandidate_b"]);
}

#[test]
fn load_snapshot_recalls_metadata_without_persistent_keyword_index() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[DatabaseNftRecord {
                contract_address: "0xcandidate".into(),
                token_id: "1".into(),
                metadata_json: r#"{"description":"gold dragon"}"#.into(),
                ..Default::default()
            }],
        )
        .unwrap();

    let conn = store.conn().unwrap();
    let has_persistent_index = conn
        .query_row(
            "
                SELECT EXISTS(
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_name = 'metadata_keyword_index'
                )
                ",
            [],
            |row| row.get::<_, bool>(0),
        )
        .unwrap();
    drop(conn);
    let snapshot = store
        .load_snapshot(
            "ethereum",
            &[SeedNft {
                contract_address: "0xseed".into(),
                token_id: "1".into(),
                metadata_json: r#"{"description":"gold dragon"}"#.into(),
                ..Default::default()
            }],
            95.0,
            0.6,
            0,
            0,
        )
        .unwrap();

    assert!(!has_persistent_index);
    assert_eq!(snapshot.duplicate_contract_rows.len(), 1);
    assert!(snapshot.contract_signals["0xcandidate"].keyword_match);
}

#[test]
fn opening_existing_feature_db_does_not_backfill_persistent_metadata_keyword_index() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("features.duckdb");
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
                "
                CREATE TABLE nft_features (
                    chain VARCHAR NOT NULL,
                    contract_address VARCHAR NOT NULL,
                    token_id VARCHAR NOT NULL,
                    token_uri VARCHAR,
                    image_uri VARCHAR,
                    name VARCHAR,
                    symbol VARCHAR,
                    metadata_json VARCHAR,
                    token_uri_norm VARCHAR,
                    image_uri_norm VARCHAR,
                    name_norm VARCHAR
                );
                INSERT INTO nft_features VALUES (
                    'ethereum', '0xcandidate', '1', '', '', '', '', '{\"description\":\"gold dragon\"}',
                    '', '', ''
                );
                ",
            )
            .unwrap();
    }

    let store = DuckDbFeatureStore::new(&db_path.to_string_lossy()).unwrap();
    let conn = store.conn().unwrap();
    let has_persistent_index = conn
        .query_row(
            "
                SELECT EXISTS(
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_name = 'metadata_keyword_index'
                )
                ",
            [],
            |row| row.get::<_, bool>(0),
        )
        .unwrap();
    drop(conn);
    let snapshot = store
        .load_snapshot(
            "ethereum",
            &[SeedNft {
                contract_address: "0xseed".into(),
                token_id: "1".into(),
                metadata_json: r#"{"description":"gold dragon"}"#.into(),
                ..Default::default()
            }],
            95.0,
            0.6,
            0,
            0,
        )
        .unwrap();

    assert!(!has_persistent_index);
    assert_eq!(snapshot.duplicate_contract_rows.len(), 1);
}

#[test]
fn load_snapshot_reuses_and_invalidates_metadata_recall_index_cache() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[DatabaseNftRecord {
                contract_address: "0xdup".into(),
                token_id: "1".into(),
                metadata_json: r#"{"description":"gold dragon"}"#.into(),
                ..Default::default()
            }],
        )
        .unwrap();
    let seed = vec![SeedNft {
        chain: "ethereum".into(),
        contract_address: "0xseed".into(),
        token_id: "1".into(),
        metadata_json: r#"{"description":"gold dragon"}"#.into(),
        ..Default::default()
    }];

    assert_eq!(store.metadata_recall_index_cache_len(), 0);
    store
        .load_snapshot("ethereum", &seed, 101.0, 0.6, 0, 0)
        .unwrap();
    assert_eq!(store.metadata_recall_index_cache_len(), 1);
    store
        .load_snapshot("ethereum", &seed, 101.0, 0.6, 0, 0)
        .unwrap();
    assert_eq!(store.metadata_recall_index_cache_len(), 1);

    store
        .replace_chain_rows(
            "ethereum",
            &[DatabaseNftRecord {
                contract_address: "0xother".into(),
                token_id: "1".into(),
                metadata_json: r#"{"description":"silver cat"}"#.into(),
                ..Default::default()
            }],
        )
        .unwrap();
    assert_eq!(store.metadata_recall_index_cache_len(), 0);
}

#[test]
fn prepared_metadata_docs_persist_only_the_compact_runtime_projection() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    let conn = store.conn().unwrap();
    let columns = DuckDbFeatureStore::table_columns(&conn, "nft_metadata_recall_docs").unwrap();

    assert_eq!(
        columns,
        HashSet::from([
            "chain".to_string(),
            "feature_rowid".to_string(),
            "contract_address".to_string(),
            "recall_doc".to_string(),
        ])
    );
}

#[test]
fn metadata_recall_never_uses_simhash_as_a_hard_exclusion_filter() {
    let shared = "zzzzsharedterm";
    let seed_terms = (0..40).map(|index| format!("seedterm{index:02}"));
    let target_terms = (0..40).map(|index| format!("targetterm{index:02}"));
    let seed_text = seed_terms
        .clone()
        .chain(std::iter::once(shared.to_string()))
        .collect::<Vec<_>>()
        .join(" ");
    let target_text = target_terms
        .chain(std::iter::once(shared.to_string()))
        .collect::<Vec<_>>()
        .join(" ");
    let mut rows = vec![DatabaseNftRecord {
        contract_address: "0xtarget".into(),
        token_id: "1".into(),
        metadata_json: serde_json::json!({"description": target_text}).to_string(),
        ..Default::default()
    }];
    rows.extend(
        seed_terms
            .enumerate()
            .map(|(index, term)| DatabaseNftRecord {
                contract_address: format!("0xfiller{index:02}"),
                token_id: "1".into(),
                metadata_json: serde_json::json!({"description": term}).to_string(),
                ..Default::default()
            }),
    );
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store.replace_chain_rows("ethereum", &rows).unwrap();
    let seed_metadata = serde_json::json!({"description": seed_text}).to_string();

    {
        let conn = store.conn().unwrap();
        let state = DuckDbFeatureStore::prepared_recall_state(&conn, "ethereum").unwrap();
        let index =
            DuckDbFeatureStore::load_metadata_recall_index(&conn, "ethereum", state, usize::MAX)
                .unwrap();
        let seed_doc =
            MetadataBm25Document::from_text(&metadata_recall_document(&seed_metadata)).unwrap();
        let seed_sketch = metadata_sketch_from_compact_corpus(&seed_doc, &index.compact_corpus);
        let target_index = index
            .candidates
            .iter()
            .position(|candidate| candidate.contract_address == "0xtarget")
            .unwrap();
        let target_sketch = metadata_sketch_from_compact_document(
            &index.compact_documents[target_index],
            &index.compact_corpus,
        );
        assert!(!metadata_sketch_source_match(
            &seed_sketch,
            &target_sketch,
            METADATA_SKETCH_SOURCE_HAMMING_THRESHOLD,
        ));
    }

    let snapshot = store
        .load_snapshot(
            "ethereum",
            &[SeedNft {
                contract_address: "0xseed".into(),
                token_id: "1".into(),
                metadata_json: seed_metadata,
                ..Default::default()
            }],
            101.0,
            0.0,
            0,
            0,
        )
        .unwrap();

    assert!(snapshot
        .nft_rows
        .iter()
        .any(|row| row.contract_address == "0xtarget"));
}

#[test]
fn metadata_term_postings_equal_exhaustive_exact_bm25_across_thresholds() {
    let documents = (0..96)
        .map(|index| {
            MetadataBm25Document::from_text(&format!(
                "common group{} token{} token{} marker{}",
                index % 7,
                index % 19,
                (index * 11) % 31,
                index
            ))
            .unwrap()
        })
        .collect::<Vec<_>>();
    let (compact_corpus, compact_documents) =
        crate::analysis::scoring::CompactMetadataBm25Corpus::from_indexed_documents(&documents);
    let term_postings =
        DuckDbFeatureStore::build_metadata_term_postings(&compact_corpus, &compact_documents)
            .unwrap();
    let candidates = (0..documents.len())
        .map(|index| MetadataRecallCandidate {
            feature_rowid: index as i64,
            contract_address: format!("0x{index:040x}"),
        })
        .collect::<Vec<_>>();
    let index = MetadataRecallIndex {
        candidates,
        compact_corpus,
        compact_documents,
        term_postings,
    };
    let queries = [
        "common token1 marker1",
        "group3 token7 absent",
        "marker95 common",
        "entirely absent vocabulary",
    ];

    let mut scratch = MetadataCandidateScratch::new(index.candidates.len());
    for query_text in queries {
        let query = MetadataBm25Document::from_text(query_text).unwrap();
        let exact_query = CompactMetadataBm25Query::new(&query, &index.compact_corpus);
        for threshold in [0.0, 0.2, 0.6, 1.0] {
            let posting_candidates = DuckDbFeatureStore::metadata_term_candidate_indices(
                &query,
                &index,
                &HashSet::new(),
                &mut scratch,
            );
            let indexed_matches = posting_candidates
                .iter()
                .copied()
                .filter(|candidate_index| {
                    let document = &index.compact_documents[*candidate_index as usize];
                    exact_query.has_term_overlap(document)
                        && score_compact_metadata_indexed_pair_with_query(&exact_query, document)
                            >= threshold
                })
                .map(|candidate_index| candidate_index as usize)
                .collect::<BTreeSet<_>>();
            let exhaustive_matches = index
                .compact_documents
                .iter()
                .enumerate()
                .filter_map(|(candidate_index, document)| {
                    (exact_query.has_term_overlap(document)
                        && score_compact_metadata_indexed_pair_with_query(&exact_query, document)
                            >= threshold)
                        .then_some(candidate_index)
                })
                .collect::<BTreeSet<_>>();

            assert_eq!(
                indexed_matches, exhaustive_matches,
                "{query_text} @ {threshold}"
            );
        }
    }
}

#[test]
fn metadata_sketch_keeps_more_low_frequency_anchor_terms_for_prefilter_recall() {
    let doc = MetadataBm25Document::from_text(
        "alpha bravo charlie delta echo foxtrot golf hotel india juliet kilo lima mike november oscar papa",
    )
    .unwrap();
    let doc_freqs = doc
        .tokens()
        .iter()
        .map(|token| (token.clone(), 1usize))
        .collect::<HashMap<_, _>>();

    let sketch = metadata_sketch_from_document(&doc, 100, &doc_freqs);

    assert_eq!(sketch.1.len(), 16);
}

#[test]
fn metadata_sketch_source_match_uses_bounded_simhash_prefilter_window() {
    let seed = MetadataSketch {
        simhash: u64::MAX,
        anchors: vec![],
    };
    let inside = MetadataSketch {
        simhash: u64::MAX ^ ((1u64 << 16) - 1),
        anchors: vec![],
    };
    let outside = MetadataSketch {
        simhash: u64::MAX ^ ((1u64 << 17) - 1),
        anchors: vec![],
    };

    assert!(metadata_sketch_source_match(
        &seed,
        &inside,
        METADATA_SKETCH_SOURCE_HAMMING_THRESHOLD
    ));
    assert!(!metadata_sketch_source_match(
        &seed,
        &outside,
        METADATA_SKETCH_SOURCE_HAMMING_THRESHOLD
    ));
}

#[test]
fn compact_corpus_metadata_sketch_matches_string_doc_frequency_reference() {
    let documents = [
        MetadataBm25Document::from_text("gold dragon rare").unwrap(),
        MetadataBm25Document::from_text("gold cat").unwrap(),
        MetadataBm25Document::from_text("silver bird").unwrap(),
    ];
    let mut builder = CompactMetadataBm25CorpusBuilder::default();
    let mut doc_freqs = HashMap::new();
    for document in &documents {
        builder.add_tokens(document.tokens());
        for token in document.tokens().iter().collect::<HashSet<_>>() {
            *doc_freqs.entry((*token).clone()).or_insert(0) += 1;
        }
    }
    let corpus = builder.finish();

    for document in &documents {
        let reference = metadata_sketch_from_document(document, documents.len(), &doc_freqs);
        let compact = metadata_sketch_from_compact_corpus(document, &corpus);
        assert_eq!(compact.simhash, reference.0);
        let mut reference_anchor_ids = reference
            .1
            .iter()
            .map(|token| corpus.token_id(token).unwrap())
            .collect::<Vec<_>>();
        reference_anchor_ids.sort_unstable();
        assert_eq!(compact.anchors, reference_anchor_ids);
    }
}

#[test]
fn compact_corpus_builder_compacts_documents_during_ingest() {
    let documents = [
        MetadataBm25Document::from_text("gold dragon gold").unwrap(),
        MetadataBm25Document::from_text("silver dragon").unwrap(),
    ];
    let mut builder = CompactMetadataBm25CorpusBuilder::default();
    let compact_documents = documents
        .iter()
        .map(|document| builder.add_document(document))
        .collect::<Vec<_>>();
    let corpus = builder.finish();

    for (document, compact_document) in documents.iter().zip(&compact_documents) {
        let query = CompactMetadataBm25Query::new(document, &corpus);
        assert!(query.has_term_overlap(compact_document));
        assert_eq!(
            metadata_sketch_from_compact_document(compact_document, &corpus).simhash,
            metadata_sketch_from_compact_corpus(document, &corpus).simhash
        );
    }
}
