use super::*;

#[test]
fn conservative_calibration_positions_are_deterministic_and_bounded() {
    let left_count = 13_533_772usize;
    let first = metadata_conservative_calibration_sample_positions(left_count, 17);
    let repeated = metadata_conservative_calibration_sample_positions(left_count, 17);
    let different_seed = metadata_conservative_calibration_sample_positions(left_count, 19);

    assert_eq!(first, repeated);
    assert_ne!(first, different_seed);
    assert_eq!(first.len(), METADATA_CONSERVATIVE_CALIBRATION_MAX_LEFTS);
    assert!(first.windows(2).all(|pair| pair[0] < pair[1]));
    assert!(first.iter().all(|&left| left < left_count));

    let small = metadata_conservative_calibration_sample_positions(255, 17);
    assert_eq!(small, (0..255).collect::<Vec<_>>());
}

#[test]
fn conservative_calibration_work_budget_preserves_cost_strata_or_fails() {
    let items = [
        MetadataCalibrationWorkItem {
            left: 0,
            chain_index: 0,
            estimated_posting_visits: 10,
        },
        MetadataCalibrationWorkItem {
            left: 1,
            chain_index: 0,
            estimated_posting_visits: 11,
        },
        MetadataCalibrationWorkItem {
            left: 2,
            chain_index: 0,
            estimated_posting_visits: 1_000,
        },
        MetadataCalibrationWorkItem {
            left: 3,
            chain_index: 1,
            estimated_posting_visits: 10,
        },
    ];

    let selected = select_metadata_calibration_work_items(&items, 3, 1_020).unwrap();
    assert_eq!(selected, vec![0, 2, 3]);
    let used = items
        .iter()
        .filter(|item| selected.contains(&item.left))
        .map(|item| item.estimated_posting_visits)
        .sum::<u64>();
    assert!(used <= 1_020);

    let error = select_metadata_calibration_work_items(&items, 3, 1_019).unwrap_err();
    assert!(error.to_string().contains("calibration work budget"));
}

#[test]
fn calibration_plan_oversamples_high_cost_strata_and_preserves_population_weights() {
    let mut items = Vec::new();
    for left in 0..64usize {
        items.push(MetadataCalibrationWorkItem {
            left,
            chain_index: 0,
            estimated_posting_visits: 8,
        });
    }
    for left in 64..128usize {
        items.push(MetadataCalibrationWorkItem {
            left,
            chain_index: 0,
            estimated_posting_visits: 8_192,
        });
    }

    let plan = plan_metadata_calibration_work_items(items, 8, 16, 1_000_000).unwrap();
    let low = plan
        .samples
        .iter()
        .filter(|sample| sample.cost_bucket == 3)
        .collect::<Vec<_>>();
    let high = plan
        .samples
        .iter()
        .filter(|sample| sample.cost_bucket == 13)
        .collect::<Vec<_>>();

    assert!(high.len() > low.len(), "high-cost tail was not oversampled");
    assert_eq!(low[0].stratum_population, 64);
    assert_eq!(high[0].stratum_population, 64);
    assert!(low
        .iter()
        .all(|sample| sample.stratum_sample_count == low.len() as u64));
    assert!(high
        .iter()
        .all(|sample| sample.stratum_sample_count == high.len() as u64));
    assert!(plan.estimated_sample_posting_visits <= 1_000_000);
}

#[test]
fn calibration_planner_retains_only_a_bounded_global_reservoir() {
    let items = (0..20_000usize).map(|left| MetadataCalibrationWorkItem {
        left,
        chain_index: 0,
        estimated_posting_visits: 1u64 << (left % 4),
    });

    let plan = plan_metadata_calibration_work_items(items, 16, 32, u64::MAX).unwrap();

    assert_eq!(plan.samples.len(), 32);
    assert!(plan.retained_calibration_candidates <= 32 + 4);
}

#[test]
fn metadata_processing_order_runs_highest_estimated_posting_work_first() {
    let order = metadata_difficult_first_left_order(&[7, 200, 200, 1, 64]);
    let plan = plan_metadata_calibration_work_items(
        [
            MetadataCalibrationWorkItem {
                left: 0,
                chain_index: 0,
                estimated_posting_visits: 7,
            },
            MetadataCalibrationWorkItem {
                left: 1,
                chain_index: 0,
                estimated_posting_visits: 200,
            },
            MetadataCalibrationWorkItem {
                left: 2,
                chain_index: 0,
                estimated_posting_visits: 200,
            },
            MetadataCalibrationWorkItem {
                left: 3,
                chain_index: 0,
                estimated_posting_visits: 1,
            },
            MetadataCalibrationWorkItem {
                left: 4,
                chain_index: 0,
                estimated_posting_visits: 64,
            },
        ],
        5,
        5,
        u64::MAX,
    )
    .unwrap();

    assert_eq!(order, vec![1, 2, 4, 0, 3]);
    assert_eq!(
        plan.samples
            .iter()
            .map(|sample| sample.left)
            .collect::<Vec<_>>(),
        order
    );
}

#[test]
fn exact_posting_visit_estimate_bounds_calibration_candidate_enumeration() {
    let mut builder = MetadataDataBuilder::new(1);
    let contents = [
        "gold dragon shared rareone",
        "gold dragon shared raretwo",
        "gold dragon shared rarethree",
    ];
    let mut records = Vec::new();
    for (index, content) in contents.into_iter().enumerate() {
        let template = format!("collection shared template {index}");
        builder.merge_indexed_row(IndexedMetadataRow {
            chain_index: 0,
            nft_count: 1,
            content_doc: MetadataBm25Document::from_text(content).map(Arc::new),
            doc: MetadataBm25Document::from_text(&template).unwrap().into(),
            doc_key: metadata_document_key(&template),
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
    let mut plan = MetadataCandidatePostingPlan::default();

    let estimated = index.estimate_exact_posting_visits(
        0,
        &atoms[0],
        &compact.docs[0],
        compatibility,
        &mut plan,
    );
    let mut scratch = MetadataCandidateScratch::new(atoms.len());
    let contract_tokens = CompactContractTokens::from_nested(vec![vec![7], vec![7], vec![8]]);
    let collection = MetadataCandidateCollectionContext {
        atoms: &atoms,
        compact_docs: &compact.docs,
        candidate_index: &index,
        compatibility,
        exact_recall: true,
        exact_recall_by_left: None,
        scope: MetadataCandidateUnionScope::SharedToken,
        contract_tokens: &contract_tokens,
        fallback_token_exclusion_index: None,
        candidate_buffer_pool: None,
        estimated_posting_visits_by_left: None,
    };
    let exact = collect_metadata_left_candidate_batch(0, &collection, &mut scratch);
    let exact_candidates = exact.candidates.iter().collect::<Vec<_>>();

    assert!(estimated > 0);
    assert!(estimated >= exact.raw_candidate_pairs as usize);
    assert_eq!(exact.estimated_posting_visits, estimated as u64);
    assert!(exact.visited_posting_entries > 0);

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .unwrap();
    let production_plan = metadata_production_work_plan(
        &atoms,
        &compact.docs,
        &index,
        compatibility,
        &pool,
        None,
        None,
    )
    .unwrap();
    let fallback_atoms =
        build_metadata_fallback_atoms(&records, &compact.docs, &data, &contract_tokens);
    let fallback_candidate_index = MetadataLocalCandidateIndex::from_atoms_with_mode(
        &compact.docs,
        &fallback_atoms,
        compatibility,
        false,
        MetadataRecallMode::Conservative,
    );
    let fallback_candidate_plan = metadata_production_work_plan(
        &fallback_atoms,
        &compact.docs,
        &fallback_candidate_index,
        compatibility,
        &pool,
        None,
        None,
    )
    .unwrap();
    let exclusion_index =
        MetadataFallbackTokenExclusionIndex::from_atoms(&fallback_atoms, &contract_tokens);
    let fallback_plan = metadata_production_work_plan(
        &fallback_atoms,
        &compact.docs,
        &fallback_candidate_index,
        compatibility,
        &pool,
        None,
        Some((&exclusion_index, &contract_tokens)),
    )
    .unwrap();
    assert_eq!(
        fallback_plan.estimated_posting_visits_by_left[0],
        fallback_candidate_plan.estimated_posting_visits_by_left[0] + 1
    );
    let conservative_collection = MetadataCandidateCollectionContext {
        exact_recall: false,
        estimated_posting_visits_by_left: Some(&production_plan.estimated_posting_visits_by_left),
        ..collection
    };
    let conservative =
        collect_metadata_left_candidate_batch(0, &conservative_collection, &mut scratch);

    assert_eq!(
        conservative.visited_posting_entries,
        production_plan.estimated_posting_visits_by_left[0]
    );

    let exact_recall_by_left = vec![true; atoms.len().saturating_sub(1)];
    let mixed_plan = metadata_production_work_plan(
        &atoms,
        &compact.docs,
        &index,
        compatibility,
        &pool,
        Some(&exact_recall_by_left),
        None,
    )
    .unwrap();
    let mixed_collection = MetadataCandidateCollectionContext {
        exact_recall: false,
        exact_recall_by_left: Some(&exact_recall_by_left),
        estimated_posting_visits_by_left: Some(&mixed_plan.estimated_posting_visits_by_left),
        ..collection
    };
    let mixed = collect_metadata_left_candidate_batch(0, &mixed_collection, &mut scratch);
    assert_eq!(
        mixed.candidates.iter().collect::<Vec<_>>(),
        exact_candidates
    );
    assert_eq!(mixed.visited_posting_entries, estimated as u64);
    assert_eq!(
        mixed_plan.estimated_posting_visits_by_left[0],
        estimated as u64
    );
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

    index.append_candidates_after(0, MetadataConservativeRecallProfile::Base, &mut scratch);

    assert!(scratch
        .candidates
        .contains(&metadata_doc_index_from_usize(1)));
    assert!(index.matches(0, 1));
    assert!(index.memory_bytes() > 0);
}

#[test]
fn widened_conservative_profile_recalls_one_bit_neighbor_bands() {
    let left_simhash = 0u64;
    let right_simhash = (0..METADATA_CONSERVATIVE_SIMHASH_BANDS)
        .fold(0u64, |value, band| value | (1u64 << (band * 8)));
    let sketches = vec![
        MetadataConservativeSketch {
            simhash: left_simhash,
            anchors: [0; METADATA_CONSERVATIVE_ANCHOR_COUNT],
            anchor_len: 0,
            has_terms: true,
        },
        MetadataConservativeSketch {
            simhash: right_simhash,
            anchors: [0; METADATA_CONSERVATIVE_ANCHOR_COUNT],
            anchor_len: 0,
            has_terms: true,
        },
    ];
    let band_entries = sketches
        .iter()
        .enumerate()
        .flat_map(|(atom_index, sketch)| {
            (0..METADATA_CONSERVATIVE_SIMHASH_BANDS).map(move |band_index| {
                (
                    metadata_recall_simhash_band_key(sketch.simhash, band_index),
                    metadata_doc_index_from_usize(atom_index),
                )
            })
        })
        .collect();
    let index = MetadataConservativeDimensionIndex {
        sketches,
        anchor_postings: MetadataSparseCandidatePostings::from_sorted_entries(Vec::new()),
        simhash_band_postings: MetadataSparseCandidatePostings::from_bounded_unsorted_entries(
            band_entries,
            METADATA_CONSERVATIVE_SIMHASH_BANDS << METADATA_CONSERVATIVE_SIMHASH_BAND_BITS,
        ),
    };
    let mut base_scratch = MetadataCandidateScratch::new(2);
    base_scratch.clear_for_next_left();
    index.append_candidates_after(
        0,
        MetadataConservativeRecallProfile::Base,
        &mut base_scratch,
    );
    let mut widened_scratch = MetadataCandidateScratch::new(2);
    widened_scratch.clear_for_next_left();
    index.append_candidates_after(
        0,
        MetadataConservativeRecallProfile::Widened,
        &mut widened_scratch,
    );

    assert!(!base_scratch
        .candidates
        .contains(&metadata_doc_index_from_usize(1)));
    assert!(widened_scratch
        .candidates
        .contains(&metadata_doc_index_from_usize(1)));
    assert!(index.matches(0, 1));
}

#[test]
fn widened_band_probes_do_not_cover_the_full_conservative_hamming_predicate() {
    let four_bits_per_band = (0..METADATA_CONSERVATIVE_SIMHASH_BANDS)
        .fold(0u64, |value, band| value | (0x0fu64 << (band * 8)));
    let index = conservative_dimension_from_simhashes(&[0, four_bits_per_band]);
    let mut scratch = MetadataCandidateScratch::new(2);
    scratch.clear_for_next_left();
    index.append_candidates_after(0, MetadataConservativeRecallProfile::Widened, &mut scratch);

    assert_eq!(four_bits_per_band.count_ones(), 32);
    assert!(index.matches(0, 1));
    assert!(scratch.candidates.is_empty());
}

fn conservative_dimension_from_simhashes(simhashes: &[u64]) -> MetadataConservativeDimensionIndex {
    let sketches = simhashes
        .iter()
        .copied()
        .map(|simhash| MetadataConservativeSketch {
            simhash,
            anchors: [0; METADATA_CONSERVATIVE_ANCHOR_COUNT],
            anchor_len: 0,
            has_terms: true,
        })
        .collect::<Vec<_>>();
    let band_entries = sketches
        .iter()
        .enumerate()
        .flat_map(|(atom_index, sketch)| {
            (0..METADATA_CONSERVATIVE_SIMHASH_BANDS).map(move |band_index| {
                (
                    metadata_recall_simhash_band_key(sketch.simhash, band_index),
                    metadata_doc_index_from_usize(atom_index),
                )
            })
        })
        .collect();
    MetadataConservativeDimensionIndex {
        sketches,
        anchor_postings: MetadataSparseCandidatePostings::from_sorted_entries(Vec::new()),
        simhash_band_postings: MetadataSparseCandidatePostings::from_bounded_unsorted_entries(
            band_entries,
            METADATA_CONSERVATIVE_SIMHASH_BANDS << METADATA_CONSERVATIVE_SIMHASH_BAND_BITS,
        ),
    }
}

fn conservative_dimension_with_anchors(
    simhashes: &[u64],
    anchors_by_atom: &[&[u32]],
) -> MetadataConservativeDimensionIndex {
    let mut dimension = conservative_dimension_from_simhashes(simhashes);
    let mut anchor_entries = Vec::new();
    for (atom_index, anchors) in anchors_by_atom.iter().enumerate() {
        let sketch = &mut dimension.sketches[atom_index];
        sketch.anchor_len = anchors.len() as u8;
        sketch.anchors[..anchors.len()].copy_from_slice(anchors);
        anchor_entries.extend(
            anchors
                .iter()
                .copied()
                .map(|anchor| (anchor, metadata_doc_index_from_usize(atom_index))),
        );
    }
    anchor_entries.sort_unstable();
    dimension.anchor_postings =
        MetadataSparseCandidatePostings::from_sorted_entries(anchor_entries);
    dimension
}

#[test]
fn joint_conservative_band_index_pushes_dimension_intersection_into_postings() {
    let one_bit_per_band = (0..METADATA_CONSERVATIVE_SIMHASH_BANDS)
        .fold(0u64, |value, band| value | (1u64 << (band * 8)));
    let template = conservative_dimension_from_simhashes(&[0, 0, 0, one_bit_per_band]);
    let content =
        conservative_dimension_from_simhashes(&[0, 0, one_bit_per_band, one_bit_per_band]);
    let joint = MetadataConservativeJointBandIndex::from_dimensions(&template, &content, false);

    let mut base_scratch = MetadataCandidateScratch::new(4);
    base_scratch.clear_for_next_left();
    let base_estimate = joint.estimate_posting_visits_after(
        0,
        template.sketches[0].simhash,
        content.sketches[0].simhash,
        MetadataConservativeRecallProfile::Base,
    );
    joint.append_candidates_after(
        0,
        template.sketches[0].simhash,
        content.sketches[0].simhash,
        MetadataConservativeRecallProfile::Base,
        &mut base_scratch,
    );
    assert_eq!(base_scratch.visited_posting_entries, base_estimate as u64);
    assert_eq!(
        base_scratch.candidates,
        vec![metadata_doc_index_from_usize(1)]
    );

    let mut widened_scratch = MetadataCandidateScratch::new(4);
    widened_scratch.clear_for_next_left();
    joint.append_candidates_after(
        0,
        template.sketches[0].simhash,
        content.sketches[0].simhash,
        MetadataConservativeRecallProfile::Widened,
        &mut widened_scratch,
    );
    widened_scratch.candidates.sort_unstable();
    assert_eq!(
        widened_scratch.candidates,
        vec![
            metadata_doc_index_from_usize(1),
            metadata_doc_index_from_usize(2),
            metadata_doc_index_from_usize(3),
        ]
    );

    let conservative = MetadataConservativeCandidateIndex {
        exact_template: None,
        exact_content: None,
        template,
        content,
        joint_bands: Some(joint),
        profile: MetadataConservativeRecallProfile::Base,
    };
    let estimated = conservative.estimate_posting_visits_after(0);
    let mut production_scratch = MetadataCandidateScratch::new(4);
    production_scratch.clear_for_next_left();
    conservative.append_candidates_after(0, &mut production_scratch);
    assert_eq!(production_scratch.visited_posting_entries, estimated as u64);
}

#[test]
fn joint_conservative_index_preserves_anchor_and_band_cross_product_candidates() {
    let far = u64::MAX;
    let template = conservative_dimension_with_anchors(
        &[0, far, 0, far, far, 0],
        &[&[10], &[10], &[], &[10], &[10], &[]],
    );
    let content = conservative_dimension_with_anchors(
        &[0, 0, far, far, far, far],
        &[&[20], &[], &[20], &[20], &[], &[]],
    );
    let joint = MetadataConservativeJointBandIndex::from_dimensions(&template, &content, false);
    let legacy = MetadataConservativeCandidateIndex {
        exact_template: None,
        exact_content: None,
        template: conservative_dimension_with_anchors(
            &[0, far, 0, far, far, 0],
            &[&[10], &[10], &[], &[10], &[10], &[]],
        ),
        content: conservative_dimension_with_anchors(
            &[0, 0, far, far, far, far],
            &[&[20], &[], &[20], &[20], &[], &[]],
        ),
        joint_bands: None,
        profile: MetadataConservativeRecallProfile::Base,
    };
    let indexed = MetadataConservativeCandidateIndex {
        exact_template: None,
        exact_content: None,
        template,
        content,
        joint_bands: Some(joint),
        profile: MetadataConservativeRecallProfile::Base,
    };
    let collect = |index: &MetadataConservativeCandidateIndex| {
        let mut scratch = MetadataCandidateScratch::new(6);
        scratch.clear_for_next_left();
        index.append_candidates_after(0, &mut scratch);
        scratch.candidates.sort_unstable();
        scratch.candidates
    };

    let expected = vec![
        metadata_doc_index_from_usize(1),
        metadata_doc_index_from_usize(2),
        metadata_doc_index_from_usize(3),
    ];
    assert_eq!(collect(&legacy), expected);
    assert_eq!(collect(&indexed), expected);
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
    let MetadataLocalCandidateIndex::Conservative(conservative) = &index else {
        panic!("conservative recall must build a conservative candidate index");
    };
    assert!(conservative.joint_bands.is_none());
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

    let weighted_tail_drift_above_limit = MetadataRecallCalibrationStats {
        exact_duplicate_contract_members: 10_000,
        missed_duplicate_contract_members: 1,
        weighted_exact_duplicate_contract_members: 100,
        weighted_missed_duplicate_contract_members: 1,
        ..MetadataRecallCalibrationStats::default()
    };
    assert!(weighted_tail_drift_above_limit.requires_exact_fallback());
}

#[test]
fn bounded_exact_rescue_selects_whole_risk_strata_without_exceeding_work_budget() {
    let atom = |chain_index| MetadataContentAtom {
        chain_index,
        template_doc_index: metadata_doc_index_from_usize(0),
        representative_record_index: metadata_doc_index_from_usize(0),
        members: vec![metadata_contract_index_from_usize(0)],
        fallback_token_groups: Vec::new(),
    };
    let atoms = vec![atom(0), atom(0), atom(1), atom(1)];
    let exact_estimates = vec![8, 4, 16];
    let rescue =
        plan_metadata_bounded_exact_rescue(&atoms, &exact_estimates, &[(0, 3), (1, 4)], 10);

    assert_eq!(rescue.exact_recall_by_left, vec![true, false, false]);
    assert_eq!(rescue.exact_left_atoms, 1);
    assert_eq!(rescue.estimated_exact_posting_visits, 8);
    assert_eq!(rescue.unrescued_risk_strata, 1);
}

#[test]
fn representative_recall_risk_uses_wilson_bound_only_when_informative() {
    let underpowered_zero_miss = MetadataRecallCalibrationStats {
        exact_matched_pairs: 767,
        ..MetadataRecallCalibrationStats::default()
    };
    assert!(!underpowered_zero_miss.representative_recall_risk_exceeded());

    let informative_zero_miss = MetadataRecallCalibrationStats {
        exact_matched_pairs: 768,
        ..MetadataRecallCalibrationStats::default()
    };
    assert!(!informative_zero_miss.representative_recall_risk_exceeded());

    let uncertain_single_miss = MetadataRecallCalibrationStats {
        exact_matched_pairs: 1_000,
        missed_matched_pairs: 1,
        ..MetadataRecallCalibrationStats::default()
    };
    assert!(uncertain_single_miss.representative_recall_risk_exceeded());

    let well_bounded_single_miss = MetadataRecallCalibrationStats {
        exact_matched_pairs: 10_000,
        missed_matched_pairs: 1,
        ..MetadataRecallCalibrationStats::default()
    };
    assert!(!well_bounded_single_miss.representative_recall_risk_exceeded());

    let weighted_tail_misses = MetadataRecallCalibrationStats {
        exact_matched_pairs: 10_000,
        missed_matched_pairs: 1,
        weighted_exact_matched_pairs: 1_000,
        weighted_missed_matched_pairs: 100,
        ..MetadataRecallCalibrationStats::default()
    };
    assert!(weighted_tail_misses.representative_recall_risk_exceeded());
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
    )
    .unwrap();
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
    )
    .unwrap();
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
    )
    .unwrap();
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
    )
    .unwrap();
    assert_eq!(below_threshold.atom_count, record_count - 1);
    assert_eq!(below_threshold.conservative_groups, 0);
}

#[test]
fn conservative_representative_fallback_builds_and_calibrates_conservative_index() {
    let record_count = METADATA_CONSERVATIVE_MIN_ATOMS;
    let mut builder = MetadataDataBuilder::new(1);
    let mut records = Vec::with_capacity(record_count);
    for index in 0..record_count {
        let pair = index / 2;
        let side = if index % 2 == 0 {
            "sidealpha"
        } else {
            "sidebeta"
        };
        let content =
            format!("pair{pair} sharedone sharedtwo sharedthree sharedfour sharedfive {side}");
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
    let contract_tokens = CompactContractTokens::from_nested(
        (0..record_count)
            .map(|index| vec![u32::try_from(index).unwrap()])
            .collect(),
    );
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

    let stats =
        union_metadata_no_common_content_candidates(&records, &compact.docs, &context, &mut state);

    assert_eq!(stats.atom_count, record_count);
    assert_eq!(stats.conservative_groups, 1);
    assert!(stats.recall_calibration.sampled_left_atoms > 0);
    assert!(stats.recall_calibration.exact_candidate_pairs > 0);
}

#[test]
fn representative_fallback_calibration_excludes_common_token_pairs() {
    let record_count = METADATA_CONSERVATIVE_MIN_ATOMS;
    let mut builder = MetadataDataBuilder::new(1);
    let mut records = Vec::with_capacity(record_count);
    for index in 0..record_count {
        let pair = index / 2;
        let side = if index % 2 == 0 {
            "sidealpha"
        } else {
            "sidebeta"
        };
        let content =
            format!("pair{pair} sharedone sharedtwo sharedthree sharedfour sharedfive {side}");
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
    let contract_tokens = CompactContractTokens::from_nested(vec![vec![7]; record_count]);
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

    let stats =
        union_metadata_no_common_content_candidates(&records, &compact.docs, &context, &mut state);

    assert_eq!(stats.conservative_groups, 1);
    assert_eq!(stats.recall_calibration.exact_candidate_pairs, 0);
    assert_eq!(stats.recall_calibration.conservative_candidate_pairs, 0);
    assert_eq!(stats.recall_calibration.exact_matched_pairs, 0);
    assert_eq!(stats.recall_calibration.missed_matched_pairs, 0);
    assert_eq!(stats.exact_fallback_groups, 0);
}

#[test]
fn conservative_representative_fallback_never_silently_runs_global_exact() {
    let record_count = METADATA_CONSERVATIVE_MIN_ATOMS;
    let shared_content = (0..20)
        .map(|token| format!("ubiquitous{token}"))
        .collect::<Vec<_>>()
        .join(" ");
    let mut builder = MetadataDataBuilder::new(1);
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
    }
    let data = builder.finish();
    let contract_tokens = CompactContractTokens::from_nested(
        (0..record_count)
            .map(|index| vec![u32::try_from(index).unwrap()])
            .collect(),
    );
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

    match union_metadata_representative_content_fallback(
        &context,
        &mut state,
        usize::MAX,
        &ProgressTracker::Disabled,
    ) {
        Ok(stats) => assert_eq!(stats.exact_fallback_groups, 0),
        Err(error) => assert!(error.to_string().contains("conservative recall drift")),
    }
}

#[test]
fn conservative_shared_token_calibration_never_silently_runs_exact() {
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

    let result = union_metadata_shared_token_atoms_with_mode(
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
    )
    .unwrap();

    match result {
        Ok(stats) => {
            assert_eq!(stats.conservative_groups, 1);
            assert!(stats.recall_calibration.exact_matched_pairs > 0);
            assert_eq!(stats.exact_fallback_groups, 0);
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
        Err(error) => assert!(error
            .to_string()
            .contains("refusing an unbounded whole-group Exact fallback")),
    }
}
