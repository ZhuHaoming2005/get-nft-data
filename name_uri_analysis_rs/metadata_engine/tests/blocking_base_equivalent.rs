//! BaseEquivalent blocking compile: joint relation + optional profile keys.

use std::collections::{BTreeSet, HashSet};

use metadata_engine::blocking::{
    build_base_equivalent_atom_sketches, build_base_equivalent_atom_sketches_parallel,
    compile_base_equivalent, compile_base_equivalent_parallel_with_progress,
    compile_base_equivalent_with_progress, scoring_owner, simhash_band_key, simhash_band_value,
    AtomSketch, BaseEquivalentAtomInput, BlockKind, BlockingCompileConfig, BlockingError,
    RoutingStatus, ANCHOR_COUNT, BANDS, BAND_BITS, BLOCKING_REVISION, JOINT_BAND_FAMILIES,
};
use metadata_engine::format::{map_u32_array, map_u64_array};

fn band_value(simhash: u64, band_index: usize) -> u8 {
    let shift = band_index.saturating_mul(BAND_BITS);
    ((simhash >> shift) & 0xff) as u8
}

/// Legacy `metadata_recall_simhash_band_key` formula (pinned independently of crate helper).
fn legacy_simhash_band_key(simhash: u64, band_index: usize) -> u32 {
    let shift = band_index.saturating_mul(BAND_BITS);
    let value = ((simhash >> shift) as u32) & ((1u32 << BAND_BITS) - 1);
    (band_index as u32) << BAND_BITS | value
}

/// Legacy joint co-bucket: same (template_band_i, content_band_j) values in any of 64 families.
fn legacy_joint_co_bucket(a: &AtomSketch, b: &AtomSketch) -> bool {
    if !a.has_template_terms
        || !a.has_content_terms
        || !b.has_template_terms
        || !b.has_content_terms
    {
        return false;
    }
    for family in 0..JOINT_BAND_FAMILIES {
        let template_band = family / BANDS;
        let content_band = family % BANDS;
        if band_value(a.template_simhash, template_band)
            == band_value(b.template_simhash, template_band)
            && band_value(a.content_simhash, content_band)
                == band_value(b.content_simhash, content_band)
        {
            return true;
        }
    }
    false
}

fn shared_block_ids(
    atom_block_offsets: &[u64],
    atom_block_ids: &[u32],
    left: u32,
    right: u32,
) -> Vec<u32> {
    let l0 = atom_block_offsets[left as usize] as usize;
    let l1 = atom_block_offsets[left as usize + 1] as usize;
    let r0 = atom_block_offsets[right as usize] as usize;
    let r1 = atom_block_offsets[right as usize + 1] as usize;
    let left_set: BTreeSet<u32> = atom_block_ids[l0..l1].iter().copied().collect();
    let mut shared = Vec::new();
    for &bid in &atom_block_ids[r0..r1] {
        if left_set.contains(&bid) {
            shared.push(bid);
        }
    }
    shared.sort_unstable();
    shared.dedup();
    shared
}

fn membership_len(offsets: &[u64], atom: usize) -> u64 {
    offsets[atom + 1] - offsets[atom]
}

fn fixture_atoms() -> Vec<AtomSketch> {
    // Atom 0,1: identical hashes → co-bucket in all 64 families
    // Atom 2: template matches 0/1, content differs in all bands → no joint co-bucket with 0/1
    // Atom 3: empty content → ProvenNoCandidate
    // Atom 4: shares template-anchor 42 with atom 0 (bridge path)
    let one_bit_per_band = (0..BANDS).fold(0u64, |v, band| v | (1u64 << (band * BAND_BITS)));
    vec![
        AtomSketch {
            template_simhash: 0,
            content_simhash: 0,
            template_anchors: vec![42],
            content_anchors: vec![7],
            has_template_terms: true,
            has_content_terms: true,
        },
        AtomSketch {
            template_simhash: 0,
            content_simhash: 0,
            template_anchors: vec![],
            content_anchors: vec![],
            has_template_terms: true,
            has_content_terms: true,
        },
        AtomSketch {
            template_simhash: 0,
            content_simhash: one_bit_per_band,
            template_anchors: vec![],
            content_anchors: vec![],
            has_template_terms: true,
            has_content_terms: true,
        },
        AtomSketch {
            template_simhash: 0,
            content_simhash: 0,
            template_anchors: vec![],
            content_anchors: vec![],
            has_template_terms: true,
            has_content_terms: false,
        },
        AtomSketch {
            template_simhash: one_bit_per_band,
            content_simhash: one_bit_per_band,
            template_anchors: vec![42],
            content_anchors: vec![],
            has_template_terms: true,
            has_content_terms: true,
        },
    ]
}

#[test]
fn parallel_blocking_compile_is_identical_to_single_lane() {
    let atoms = fixture_atoms();
    let config = BlockingCompileConfig {
        max_routing_block_members: 1_000_000,
    };
    let sequential_dir = tempfile::tempdir().unwrap();
    let parallel_dir = tempfile::tempdir().unwrap();
    let sequential = compile_base_equivalent_parallel_with_progress(
        &atoms,
        &config,
        sequential_dir.path(),
        1,
        |_| {},
    )
    .unwrap();
    let parallel = compile_base_equivalent_parallel_with_progress(
        &atoms,
        &config,
        parallel_dir.path(),
        8,
        |_| {},
    )
    .unwrap();

    assert_eq!(sequential, parallel);
}

#[test]
fn blocking_revision_is_two() {
    assert_eq!(BLOCKING_REVISION, 2);
    // Must stay equal to name_uri METADATA_CONSERVATIVE_* values.
    assert_eq!(BANDS, 8);
    assert_eq!(BAND_BITS, 8);
    assert_eq!(JOINT_BAND_FAMILIES, 64);
    assert_eq!(ANCHOR_COUNT, 16);
}

#[test]
fn atom_sketches_use_global_document_frequency_like_the_legacy_index() {
    let template_terms: Vec<Vec<(u32, u32)>> = (0..32)
        .map(|index| vec![(1, 100), (100 + index, 1)])
        .collect();
    let content_terms: Vec<Vec<(u32, u32)>> = (0..32)
        .map(|index| vec![(2, 100), (200 + index, 1)])
        .collect();
    let inputs: Vec<_> = template_terms
        .iter()
        .zip(&content_terms)
        .map(|(template_terms, content_terms)| BaseEquivalentAtomInput {
            template_terms,
            content_terms,
        })
        .collect();

    let sketches = build_base_equivalent_atom_sketches(&inputs);
    assert_eq!(
        sketches,
        build_base_equivalent_atom_sketches_parallel(&inputs, 8)
    );

    assert_eq!(sketches.len(), 32);
    for (index, sketch) in sketches.iter().enumerate() {
        assert_eq!(sketch.template_anchors, vec![100 + index as u32]);
        assert_eq!(sketch.content_anchors, vec![200 + index as u32]);
        assert!(sketch.has_template_terms);
        assert!(sketch.has_content_terms);
    }
    assert_ne!(
        sketches[0].template_simhash,
        local_frequency_simhash(&template_terms[0]),
        "legacy BaseEquivalent simhash is IDF-weighted, not term-frequency weighted"
    );
}

#[test]
fn persists_block_kind_and_routing_key_descriptors() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("blocking");
    let bundle = compile_base_equivalent(
        &fixture_atoms(),
        &BlockingCompileConfig {
            max_routing_block_members: 1_000_000,
        },
        &out,
    )
    .unwrap();

    let kinds = map_u32_array(&out.join("block_kinds.u32")).unwrap();
    let keys = map_u64_array(&out.join("block_keys.u64")).unwrap();
    assert_eq!(kinds.len(), bundle.block_kinds.len());
    assert_eq!(keys.len(), bundle.block_keys.len());
    assert_eq!(&*keys, bundle.block_keys.as_slice());
}

#[test]
fn blocking_compile_reports_bucket_and_finalize_work_to_exact_totals() {
    use metadata_engine::progress::ProgressPhase;

    let dir = tempfile::tempdir().unwrap();
    let mut events = Vec::new();
    compile_base_equivalent_with_progress(
        &fixture_atoms(),
        &BlockingCompileConfig {
            max_routing_block_members: 1_000_000,
        },
        &dir.path().join("blocking"),
        |event| events.push(event),
    )
    .unwrap();

    for phase in [
        ProgressPhase::BlockingCompile,
        ProgressPhase::BlockingFinalize,
    ] {
        let phase_events = events
            .iter()
            .filter(|event| event.phase == phase)
            .collect::<Vec<_>>();
        assert!(phase_events.len() > 1, "missing incremental {phase:?}");
        assert!(phase_events
            .windows(2)
            .all(|window| window[0].completed <= window[1].completed));
        let terminal = phase_events.last().unwrap();
        assert_eq!(terminal.completed, terminal.total.unwrap(), "{phase:?}");
    }
}

fn local_frequency_simhash(terms: &[(u32, u32)]) -> u64 {
    let mut weights = [0.0f64; 64];
    for &(term, freq) in terms {
        let hash = stable_hash(term);
        for (bit, weight) in weights.iter_mut().enumerate() {
            if (hash >> bit) & 1 == 1 {
                *weight += f64::from(freq);
            } else {
                *weight -= f64::from(freq);
            }
        }
    }
    weights
        .into_iter()
        .enumerate()
        .fold(0, |hash, (bit, weight)| {
            hash | (u64::from(weight >= 0.0) << bit)
        })
}

fn stable_hash(term: u32) -> u64 {
    let mut value = u64::from(term).wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[test]
fn simhash_band_key_and_joint_bucket_match_legacy() {
    // Constants pin vs name_uri METADATA_CONSERVATIVE_SIMHASH_BANDS/BITS/ANCHOR_COUNT/FAMILIES.
    assert_eq!(BANDS, 8, "METADATA_CONSERVATIVE_SIMHASH_BANDS");
    assert_eq!(BAND_BITS, 8, "METADATA_CONSERVATIVE_SIMHASH_BAND_BITS");
    assert_eq!(ANCHOR_COUNT, 16, "METADATA_CONSERVATIVE_ANCHOR_COUNT");
    assert_eq!(
        JOINT_BAND_FAMILIES, 64,
        "METADATA_CONSERVATIVE_JOINT_BAND_FAMILIES"
    );

    let fixtures: &[u64] = &[
        0,
        0xff,
        0x0102_0304_0506_0708,
        u64::MAX,
        (0..BANDS).fold(0u64, |v, band| v | (1u64 << (band * BAND_BITS))),
        0x00ff_00ff_00ff_00ff,
        0xa5a5_a5a5_a5a5_a5a5,
    ];

    for &simhash in fixtures {
        for band in 0..BANDS {
            let expected = legacy_simhash_band_key(simhash, band);
            assert_eq!(
                simhash_band_key(simhash, band),
                expected,
                "simhash_band_key({simhash:#x}, {band})"
            );
            assert_eq!(
                simhash_band_value(simhash, band),
                band_value(simhash, band),
                "simhash_band_value({simhash:#x}, {band})"
            );
            // band key packs (band_index << BAND_BITS) | value
            let value = u32::from(simhash_band_value(simhash, band));
            assert_eq!(expected, (band as u32) << BAND_BITS | value);
        }
    }

    // Joint bucket `(tv << 8) | cv` for several (template, content, family) fixtures.
    let joint_cases: &[(u64, u64)] = &[
        (0, 0),
        (0x0102_0304_0506_0708, 0x0807_0605_0403_0201),
        (u64::MAX, 0),
        (0xa5a5_a5a5_a5a5_a5a5, 0x5a5a_5a5a_5a5a_5a5a),
    ];
    for &(template, content) in joint_cases {
        for family in 0..JOINT_BAND_FAMILIES {
            let template_band = family / BANDS;
            let content_band = family % BANDS;
            let tv = simhash_band_value(template, template_band);
            let cv = simhash_band_value(content, content_band);
            let bucket = (u16::from(tv) << BAND_BITS) | u16::from(cv);
            let legacy_bucket = (u16::from(band_value(template, template_band)) << BAND_BITS)
                | u16::from(band_value(content, content_band));
            assert_eq!(bucket, legacy_bucket, "family {family}");
            assert!(u32::from(bucket) < (1u32 << (2 * BAND_BITS)));
        }
    }
}

#[test]
fn base_equivalent_joint_relation_matches_legacy_bucket_membership() {
    let atoms = fixture_atoms();
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("blocking");
    let config = BlockingCompileConfig {
        max_routing_block_members: 1_000_000,
    };

    let bundle = compile_base_equivalent(&atoms, &config, &out).expect("compile");

    // Persist expected artifacts
    assert!(out.join("atom_primary_storage_shard.u32").is_file());
    assert!(out.join("atom_template_simhash.u64").is_file());
    assert!(out.join("atom_content_simhash.u64").is_file());
    assert!(out.join("atom_routing_status.u8").is_file());
    assert!(out.join("atom_block_offsets.u64").is_file());
    assert!(out.join("atom_block_ids.u32").is_file());
    assert!(out.join("block_atom_offsets.u64").is_file());
    assert!(out.join("block_atoms.u32").is_file());
    assert!(out.join("block_stats.bin").is_file());
    assert!(out.join("hot_block_plans.bin").is_file());

    let shards = map_u32_array(&out.join("atom_primary_storage_shard.u32")).unwrap();
    let mut seen = HashSet::new();
    for &s in shards.iter() {
        assert!(
            seen.insert(s),
            "primary_storage_shard must be unique per atom"
        );
    }
    assert_eq!(seen.len(), atoms.len());

    // Atom 3: empty content → ProvenNoCandidate, no routing membership
    assert_eq!(
        bundle.routing_statuses[3],
        RoutingStatus::ProvenNoCandidate as u8
    );
    assert_eq!(
        membership_len(&bundle.atom_block_offsets, 3),
        0,
        "ProvenNoCandidate must have empty block membership"
    );

    // Routing invariant: every non-ProvenNoCandidate atom has ≥1 membership.
    for (i, &status) in bundle.routing_statuses.iter().enumerate() {
        let len = membership_len(&bundle.atom_block_offsets, i);
        if status == RoutingStatus::ProvenNoCandidate as u8 {
            assert_eq!(len, 0, "atom {i}");
        } else {
            assert!(len >= 1, "atom {i} status={status} must have ≥1 block");
        }
    }

    // Atoms with both term dims are Routed (under generous cap)
    for i in [0usize, 1, 2, 4] {
        assert_eq!(
            bundle.routing_statuses[i],
            RoutingStatus::Routed as u8,
            "atom {i}"
        );
    }

    let n = atoms.len() as u32;
    for left in 0..n {
        for right in (left + 1)..n {
            let legacy = legacy_joint_co_bucket(&atoms[left as usize], &atoms[right as usize]);
            let shared = shared_block_ids(
                &bundle.atom_block_offsets,
                &bundle.atom_block_ids,
                left,
                right,
            );
            if legacy {
                assert!(
                    !shared.is_empty(),
                    "legacy joint co-bucket ({left},{right}) must share ≥1 routing block_id"
                );
            }
        }
    }

    // Vice versa for BaseEquivalent joint blocks: if a pair shares a joint-kind block,
    // they must share a legacy joint bucket. (Anchor-only shared blocks are allowed without
    // joint co-bucket; scoring_owner may still pick them.)
    for left in 0..n {
        for right in (left + 1)..n {
            let shared = shared_block_ids(
                &bundle.atom_block_offsets,
                &bundle.atom_block_ids,
                left,
                right,
            );
            let joint_shared: Vec<_> = shared
                .iter()
                .copied()
                .filter(|&bid| bundle.block_kinds[bid as usize].is_joint())
                .collect();
            if !joint_shared.is_empty() {
                assert!(
                    legacy_joint_co_bucket(&atoms[left as usize], &atoms[right as usize]),
                    "joint block membership ({left},{right}) must imply legacy joint co-bucket"
                );
            }
        }
    }

    // primary_storage_shard is NOT used for recall: different shards still share blocks
    assert_ne!(shards[0], shards[1]);
    let shared_01 = shared_block_ids(&bundle.atom_block_offsets, &bundle.atom_block_ids, 0, 1);
    assert!(!shared_01.is_empty());
}

#[test]
fn hot_block_plan_is_constant_space_even_for_production_scale_membership() {
    let plan = metadata_engine::blocking::HotBlockPlan::cover_upper_triangle(7, 13_533_773, 1024);
    assert!(plan.tile_count > 80_000_000);
    assert!(std::mem::size_of_val(&plan) <= 32);
    assert_eq!(plan.tiles().take(1).count(), 1);
}

#[test]
fn group_local_base_equivalent_routes_each_overlapping_pair_once() {
    let sketches = vec![
        AtomSketch {
            template_simhash: 0,
            content_simhash: 0,
            template_anchors: vec![1],
            content_anchors: vec![2],
            has_template_terms: true,
            has_content_terms: true,
        },
        AtomSketch {
            template_simhash: 0,
            content_simhash: 0,
            template_anchors: vec![1],
            content_anchors: vec![2],
            has_template_terms: true,
            has_content_terms: true,
        },
        AtomSketch {
            template_simhash: 0,
            content_simhash: 0,
            template_anchors: vec![1],
            content_anchors: vec![2],
            has_template_terms: true,
            has_content_terms: true,
        },
    ];
    let mut pairs = Vec::new();
    metadata_engine::blocking::for_each_local_base_equivalent_pair(&sketches, |a, b| {
        pairs.push((a, b))
    });
    pairs.sort_unstable();
    assert_eq!(pairs, vec![(0, 1), (0, 2), (1, 2)]);
}

#[test]
fn template_less_content_without_anchors_fails_closed() {
    // Has content terms (not exact-safe ProvenNoCandidate) but no joint placement and no anchors.
    let atoms = vec![AtomSketch {
        template_simhash: 0,
        content_simhash: 0,
        template_anchors: vec![],
        content_anchors: vec![],
        has_template_terms: false,
        has_content_terms: true,
    }];
    let dir = tempfile::tempdir().unwrap();
    let err = compile_base_equivalent(
        &atoms,
        &BlockingCompileConfig {
            max_routing_block_members: 1_000_000,
        },
        &dir.path().join("blocking"),
    )
    .expect_err("must fail closed");
    assert!(
        matches!(
            err,
            BlockingError::AtomWithoutRoutingMembership {
                atom_index: 0,
                has_template_terms: false,
            }
        ),
        "got {err:?}"
    );
}

#[test]
fn hot_block_overflow_emits_covering_plan_and_keeps_scoring() {
    // Three atoms in the same joint buckets → membership 3 > cap 2.
    let atoms = vec![
        AtomSketch {
            template_simhash: 0,
            content_simhash: 0,
            template_anchors: vec![],
            content_anchors: vec![],
            has_template_terms: true,
            has_content_terms: true,
        },
        AtomSketch {
            template_simhash: 0,
            content_simhash: 0,
            template_anchors: vec![],
            content_anchors: vec![],
            has_template_terms: true,
            has_content_terms: true,
        },
        AtomSketch {
            template_simhash: 0,
            content_simhash: 0,
            template_anchors: vec![],
            content_anchors: vec![],
            has_template_terms: true,
            has_content_terms: true,
        },
    ];
    let dir = tempfile::tempdir().unwrap();
    let bundle = compile_base_equivalent(
        &atoms,
        &BlockingCompileConfig {
            max_routing_block_members: 2,
        },
        &dir.path().join("blocking"),
    )
    .expect("compile");

    for i in 0..3 {
        assert_eq!(
            bundle.routing_statuses[i],
            RoutingStatus::HotBlock as u8,
            "atom {i}"
        );
        assert!(membership_len(&bundle.atom_block_offsets, i) >= 1);
    }
    assert!(!bundle.hot_block_plans.is_empty());

    // Every hot plan covers all upper-triangle member-index pairs.
    for plan in &bundle.hot_block_plans {
        assert_eq!(plan.member_count, 3);
        assert!(plan.tile_count > 0);
        for left in 0..plan.member_count {
            for right in left..plan.member_count {
                let covered = plan.tiles().any(|t| {
                    t.left_start <= left
                        && left < t.left_end
                        && t.right_start <= right
                        && right < t.right_end
                });
                assert!(
                    covered,
                    "plan block {} missing tile for ({left},{right})",
                    plan.block_id
                );
            }
        }
    }

    // Scoring still covers all pairs via shared hot block_ids.
    assert!(scoring_owner(&bundle, 0, 1).is_some());
    assert!(scoring_owner(&bundle, 0, 2).is_some());
    assert!(scoring_owner(&bundle, 1, 2).is_some());
}

#[test]
fn hot_block_unplannable_cap_zero_fails_closed() {
    let atoms = vec![
        AtomSketch {
            template_simhash: 0,
            content_simhash: 0,
            template_anchors: vec![],
            content_anchors: vec![],
            has_template_terms: true,
            has_content_terms: true,
        },
        AtomSketch {
            template_simhash: 0,
            content_simhash: 0,
            template_anchors: vec![],
            content_anchors: vec![],
            has_template_terms: true,
            has_content_terms: true,
        },
    ];
    let dir = tempfile::tempdir().unwrap();
    let err = compile_base_equivalent(
        &atoms,
        &BlockingCompileConfig {
            max_routing_block_members: 0,
        },
        &dir.path().join("blocking"),
    )
    .expect_err("cap 0 must fail closed");
    assert!(
        matches!(err, BlockingError::HotBlockUnplannable { cap: 0, .. }),
        "got {err:?}"
    );
}

#[test]
fn anchor_sharing_pair_shares_bridge_block_id() {
    let atoms = fixture_atoms();
    let dir = tempfile::tempdir().unwrap();
    let bundle = compile_base_equivalent(
        &atoms,
        &BlockingCompileConfig {
            max_routing_block_members: 1_000_000,
        },
        &dir.path().join("blocking"),
    )
    .unwrap();

    // Atoms 0 and 4 share template-anchor 42; may not share a joint bucket.
    let shared = shared_block_ids(&bundle.atom_block_offsets, &bundle.atom_block_ids, 0, 4);
    let bridge: Vec<_> = shared
        .iter()
        .copied()
        .filter(|&bid| bundle.block_kinds[bid as usize].is_anchor_bridge())
        .collect();
    assert!(
        !bridge.is_empty(),
        "anchor-sharing (0,4) must share ≥1 bridge block_id; shared={shared:?} kinds={:?}",
        shared
            .iter()
            .map(|&b| bundle.block_kinds[b as usize])
            .collect::<Vec<_>>()
    );
    assert!(matches!(
        bundle.block_kinds[bridge[0] as usize],
        BlockKind::TemplateAnchorBridge
    ));
}

#[test]
fn local_base_equivalent_routing_stops_when_the_visitor_cancels() {
    let sketches = (0..64)
        .map(|_| AtomSketch {
            template_simhash: 0,
            content_simhash: 0,
            template_anchors: vec![1],
            content_anchors: vec![2],
            has_template_terms: true,
            has_content_terms: true,
        })
        .collect::<Vec<_>>();
    let mut visits = 0;

    let completed =
        metadata_engine::blocking::for_each_local_base_equivalent_pair_while(&sketches, |_, _| {
            visits += 1;
            visits < 3
        });

    assert!(!completed);
    assert_eq!(visits, 3);
}
