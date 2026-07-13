use super::*;

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
