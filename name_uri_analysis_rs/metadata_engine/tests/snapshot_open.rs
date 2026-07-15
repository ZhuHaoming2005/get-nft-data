use metadata_engine::blocking::{compile_base_equivalent, AtomSketch, BlockingCompileConfig};
use metadata_engine::encode::{
    write_encode_artifacts, write_encode_artifacts_with_contracts_and_atoms, EncodeContractRow,
    EncodePayloadRow, EncodeSourceRow,
};
use metadata_engine::format::{
    commit_ready, map_u32_array, map_u64_array, write_f64_array, write_u32_array, write_u64_array,
    write_u8_array, ArrayKind,
};
use metadata_engine::snapshot::{MetadataSnapshot, SnapshotError};

fn fixture() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let features = dir.path().join("encode-1");
    let blocking = dir.path().join("blocking-1");
    write_encode_artifacts(
        &features,
        &[
            EncodeSourceRow {
                contract_id: 0,
                payload_id: 0,
                retained_token_ids: vec![7, 9],
            },
            EncodeSourceRow {
                contract_id: 1,
                payload_id: 1,
                retained_token_ids: vec![9],
            },
        ],
        &[
            EncodePayloadRow {
                template_terms: vec![(1, 1), (2, 1)],
                content_terms: vec![(2, 1)],
            },
            EncodePayloadRow {
                template_terms: vec![(1, 1)],
                content_terms: vec![(3, 1)],
            },
        ],
    )
    .unwrap();
    compile_base_equivalent(
        &[
            AtomSketch {
                template_simhash: 1,
                content_simhash: 2,
                template_anchors: vec![1],
                content_anchors: vec![2],
                has_template_terms: true,
                has_content_terms: true,
            },
            AtomSketch {
                template_simhash: 1,
                content_simhash: 2,
                template_anchors: vec![1],
                content_anchors: vec![3],
                has_template_terms: true,
                has_content_terms: true,
            },
        ],
        &BlockingCompileConfig {
            max_routing_block_members: 100,
        },
        &blocking,
    )
    .unwrap();
    commit_ready(
        &features,
        "features.ready",
        r#"{"schema_revision":1,"source_count":2,"payload_count":2,"chains":["ethereum"],"chain_totals":[{"name":"ethereum","contracts":2,"nfts":2}]}"#,
    )
    .unwrap();
    commit_ready(
        &blocking,
        "blocking.ready",
        r#"{"blocking_revision":1,"atom_count":2}"#,
    )
    .unwrap();
    (dir, features, blocking)
}

fn shared_atom_fixture() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let features = dir.path().join("features");
    let blocking = dir.path().join("blocking");
    let sources = [
        EncodeSourceRow {
            contract_id: 0,
            payload_id: 0,
            retained_token_ids: vec![],
        },
        EncodeSourceRow {
            contract_id: 1,
            payload_id: 1,
            retained_token_ids: vec![],
        },
    ];
    let payloads = [
        EncodePayloadRow {
            template_terms: vec![(1, 1)],
            content_terms: vec![(2, 1)],
        },
        EncodePayloadRow {
            template_terms: vec![(1, 1)],
            content_terms: vec![(2, 1)],
        },
    ];
    let contracts = [
        EncodeContractRow {
            contract_id: 0,
            chain_id: 0,
            source_doc_id: 0,
            payload_id: 0,
            weight: 1,
        },
        EncodeContractRow {
            contract_id: 1,
            chain_id: 0,
            source_doc_id: 1,
            payload_id: 1,
            weight: 1,
        },
    ];
    write_encode_artifacts_with_contracts_and_atoms(
        &features,
        &sources,
        &payloads,
        &contracts,
        &[vec![0, 1]],
    )
    .unwrap();
    compile_base_equivalent(
        &[AtomSketch {
            template_simhash: 0x1234,
            content_simhash: 0x5678,
            template_anchors: vec![9],
            content_anchors: vec![10],
            has_template_terms: true,
            has_content_terms: true,
        }],
        &BlockingCompileConfig {
            max_routing_block_members: 100,
        },
        &blocking,
    )
    .unwrap();
    commit_ready(
        &features,
        "features.ready",
        r#"{"schema_revision":1,"source_count":2,"payload_count":2,"chains":["ethereum"],"chain_totals":[{"name":"ethereum","contracts":2,"nfts":2}]}"#,
    )
    .unwrap();
    commit_ready(
        &blocking,
        "blocking.ready",
        r#"{"blocking_revision":1,"atom_count":1}"#,
    )
    .unwrap();
    (dir, features, blocking)
}

fn rewrite_u32(path: &std::path::Path, values: &[u32]) {
    write_u32_array(path, ArrayKind::U32, values).unwrap();
}

fn rewrite_u64(path: &std::path::Path, values: &[u64]) {
    write_u64_array(path, ArrayKind::U64, values).unwrap();
}

fn rewrite_f64(path: &std::path::Path, values: &[f64]) {
    write_f64_array(path, ArrayKind::F64, values).unwrap();
}

fn assert_invariant(features: &std::path::Path, blocking: &std::path::Path, needle: &str) {
    let error = MetadataSnapshot::open(features, blocking)
        .err()
        .expect("corrupt semantic artifact must fail closed");
    assert!(
        matches!(&error, SnapshotError::Invariant(_)),
        "expected invariant error containing {needle:?}, got {error}"
    );
    assert!(
        error.to_string().contains(needle),
        "expected invariant error containing {needle:?}, got {error}"
    );
}

#[test]
fn opens_validated_snapshot() {
    let (_dir, features, blocking) = fixture();
    let snapshot = MetadataSnapshot::open(&features, &blocking).unwrap();
    assert_eq!(snapshot.contract_count(), 2);
    assert_eq!(snapshot.atom_count(), 2);
    assert!(!snapshot.blocking().block_atoms.is_empty());
    assert_eq!(snapshot.blocking().atom_block_offsets.len(), 3);
}

#[test]
fn rejects_revision_mismatch_before_match() {
    let (_dir, features, blocking) = fixture();
    commit_ready(
        &blocking,
        "blocking.ready",
        r#"{"blocking_revision":99,"atom_count":2}"#,
    )
    .unwrap();
    assert!(matches!(
        MetadataSnapshot::open(&features, &blocking),
        Err(SnapshotError::Revision {
            artifact: "blocking",
            ..
        })
    ));
}

#[test]
fn feature_ready_requires_explicit_chain_identity_fields() {
    let (_dir, features, blocking) = fixture();
    commit_ready(
        &features,
        "features.ready",
        r#"{"schema_revision":1,"source_count":2,"payload_count":2}"#,
    )
    .unwrap();

    assert!(matches!(
        MetadataSnapshot::open(&features, &blocking),
        Err(SnapshotError::InvalidReady { .. })
    ));
}

#[test]
fn rejects_corrupt_or_inconsistent_membership_csr() {
    let (_dir, features, blocking) = fixture();
    let path = blocking.join("atom_block_offsets.u64");
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[40] ^= 0x40;
    std::fs::write(&path, bytes).unwrap();
    assert!(MetadataSnapshot::open(&features, &blocking).is_err());
}

#[test]
fn snapshot_open_reports_each_verified_array_in_bytes() {
    let (_dir, features, blocking) = fixture();
    let expected = MetadataSnapshot::verification_bytes(&features, &blocking).unwrap();
    let mut increments = Vec::new();

    let snapshot = MetadataSnapshot::open_with_progress(&features, &blocking, |bytes| {
        increments.push(bytes);
    })
    .unwrap();

    assert_eq!(snapshot.atom_count(), 2);
    assert!(
        increments.len() >= 30,
        "progress must advance per typed array"
    );
    assert_eq!(increments.iter().sum::<u64>(), expected);
    assert!(increments.iter().all(|&bytes| bytes > 0));
}

#[test]
fn rejects_contract_token_csr_with_wrong_contract_cardinality() {
    let (_dir, features, blocking) = fixture();
    rewrite_u64(&features.join("contract_token_offsets.u64"), &[0, 3]);

    assert_invariant(&features, &blocking, "contract_token_offsets length");
}

#[test]
fn rejects_out_of_range_token_member_contract() {
    let (_dir, features, blocking) = fixture();
    let path = features.join("token_member_contracts.u32");
    let mut values = map_u32_array(&path).unwrap().to_vec();
    values[0] = 2;
    rewrite_u32(&path, &values);

    assert_invariant(
        &features,
        &blocking,
        "token member references missing contract",
    );
}

#[test]
fn rejects_out_of_range_token_member_source() {
    let (_dir, features, blocking) = fixture();
    let path = features.join("token_member_sources.u32");
    let mut values = map_u32_array(&path).unwrap().to_vec();
    values[0] = 2;
    rewrite_u32(&path, &values);

    assert_invariant(
        &features,
        &blocking,
        "token member references missing source",
    );
}

#[test]
fn rejects_source_identity_assigned_to_multiple_contracts() {
    let (_dir, features, blocking) = fixture();
    let path = features.join("token_member_sources.u32");
    let mut values = map_u32_array(&path).unwrap().to_vec();
    values[0] = 1;
    rewrite_u32(&path, &values);

    assert_invariant(
        &features,
        &blocking,
        "source identity belongs to multiple contracts",
    );
}

#[test]
fn rejects_contract_source_payload_identity_mismatch() {
    let (_dir, features, blocking) = fixture();
    rewrite_u32(&features.join("contract_payload.u32"), &[1, 1]);

    assert_invariant(
        &features,
        &blocking,
        "contract source/payload identity mismatch",
    );
}

#[test]
fn rejects_unsorted_contract_token_membership() {
    let (_dir, features, blocking) = fixture();
    let path = features.join("contract_tokens.u32");
    let mut values = map_u32_array(&path).unwrap().to_vec();
    values.swap(0, 1);
    rewrite_u32(&path, &values);

    assert_invariant(&features, &blocking, "contract tokens not strictly sorted");
}

#[test]
fn rejects_unsorted_token_member_identity_pairs() {
    let (_dir, features, blocking) = fixture();
    let contracts_path = features.join("token_member_contracts.u32");
    let sources_path = features.join("token_member_sources.u32");
    let offsets = map_u64_array(&features.join("token_member_offsets.u64")).unwrap();
    let token = 9usize;
    let begin = offsets[token] as usize;
    let end = offsets[token + 1] as usize;
    assert_eq!(end - begin, 2);
    let mut contracts = map_u32_array(&contracts_path).unwrap().to_vec();
    let mut sources = map_u32_array(&sources_path).unwrap().to_vec();
    contracts.swap(begin, begin + 1);
    sources.swap(begin, begin + 1);
    rewrite_u32(&contracts_path, &contracts);
    rewrite_u32(&sources_path, &sources);

    assert_invariant(&features, &blocking, "token members not strictly sorted");
}

#[test]
fn rejects_mismatched_bidirectional_token_membership() {
    let (_dir, features, blocking) = fixture();
    rewrite_u64(&features.join("contract_token_offsets.u64"), &[0, 1, 2]);
    rewrite_u32(&features.join("contract_tokens.u32"), &[7, 9]);

    assert_invariant(&features, &blocking, "token membership directions disagree");
}

#[test]
fn rejects_unsorted_payload_terms() {
    let (_dir, features, blocking) = fixture();
    let path = features.join("payload_template_terms.u32");
    let mut values = map_u32_array(&path).unwrap().to_vec();
    values.swap(0, 1);
    rewrite_u32(&path, &values);

    assert_invariant(
        &features,
        &blocking,
        "payload template terms not strictly sorted",
    );
}

#[test]
fn rejects_unsorted_atom_block_memberships() {
    let (_dir, features, blocking) = fixture();
    let offsets = map_u64_array(&blocking.join("atom_block_offsets.u64")).unwrap();
    let path = blocking.join("atom_block_ids.u32");
    let mut values = map_u32_array(&path).unwrap().to_vec();
    let atom = (0..2)
        .find(|&atom| offsets[atom + 1] - offsets[atom] >= 2)
        .expect("fixture atom must have multiple routing memberships");
    let begin = offsets[atom] as usize;
    values.swap(begin, begin + 1);
    rewrite_u32(&path, &values);

    assert_invariant(&features, &blocking, "atom block ids not strictly sorted");
}

#[test]
fn rejects_mismatched_bidirectional_block_membership() {
    let (_dir, features, blocking) = fixture();
    let offsets = map_u64_array(&blocking.join("atom_block_offsets.u64")).unwrap();
    let path = blocking.join("atom_block_ids.u32");
    let mut values = map_u32_array(&path).unwrap().to_vec();
    let begin = offsets[0] as usize;
    let end = offsets[1] as usize;
    let replacement = (0..map_u32_array(&blocking.join("block_kinds.u32"))
        .unwrap()
        .len() as u32)
        .find(|candidate| values[begin..end].binary_search(candidate).is_err())
        .expect("fixture must have a block outside atom zero memberships");
    values[begin] = replacement;
    values[begin..end].sort_unstable();
    rewrite_u32(&path, &values);

    assert_invariant(&features, &blocking, "block membership directions disagree");
}

#[test]
fn rejects_unsorted_block_atom_memberships() {
    let (_dir, features, blocking) = fixture();
    let offsets = map_u64_array(&blocking.join("block_atom_offsets.u64")).unwrap();
    let path = blocking.join("block_atoms.u32");
    let mut values = map_u32_array(&path).unwrap().to_vec();
    let block = (0..offsets.len() - 1)
        .find(|&block| offsets[block + 1] - offsets[block] >= 2)
        .expect("fixture block must have multiple atoms");
    let begin = offsets[block] as usize;
    values.swap(begin, begin + 1);
    rewrite_u32(&path, &values);

    assert_invariant(&features, &blocking, "block atoms not strictly sorted");
}

#[test]
fn rejects_unknown_block_kind() {
    let (_dir, features, blocking) = fixture();
    let path = blocking.join("block_kinds.u32");
    let mut values = map_u32_array(&path).unwrap().to_vec();
    values[0] = 99;
    rewrite_u32(&path, &values);

    assert_invariant(&features, &blocking, "unknown block kind");
}

#[test]
fn rejects_payload_content_length_mismatch() {
    let (_dir, features, blocking) = fixture();
    rewrite_u32(&features.join("payload_lengths.u32"), &[999, 1]);
    assert_invariant(&features, &blocking, "payload content length mismatch");
}

#[test]
fn rejects_unknown_routing_status() {
    let (_dir, features, blocking) = fixture();
    write_u8_array(&blocking.join("atom_routing_status.u8"), &[99, 0]).unwrap();

    assert_invariant(&features, &blocking, "unknown routing status");
}

#[test]
fn rejects_non_finite_scoring_columns() {
    let (_dir, features, blocking) = fixture();
    rewrite_f64(&features.join("query_denominators.f64"), &[f64::NAN, 1.0]);

    assert_invariant(&features, &blocking, "invalid query denominator");
}

#[test]
fn rejects_duplicate_chain_identity() {
    let (_dir, features, blocking) = fixture();
    commit_ready(
        &features,
        "features.ready",
        r#"{"schema_revision":1,"source_count":2,"payload_count":2,"chains":["ethereum","ethereum"],"chain_totals":[{"name":"ethereum","contracts":2,"nfts":2},{"name":"ethereum","contracts":0,"nfts":0}]}"#,
    )
    .unwrap();

    assert_invariant(
        &features,
        &blocking,
        "chain identities are empty or duplicated",
    );
}

#[test]
fn rejects_empty_chain_identity_for_nonempty_contracts() {
    let (_dir, features, blocking) = fixture();
    commit_ready(
        &features,
        "features.ready",
        r#"{"schema_revision":1,"source_count":2,"payload_count":2,"chains":[],"chain_totals":[]}"#,
    )
    .unwrap();

    assert_invariant(&features, &blocking, "contract references missing chain");
}

#[test]
fn rejects_negative_chain_totals() {
    let (_dir, features, blocking) = fixture();
    commit_ready(
        &features,
        "features.ready",
        r#"{"schema_revision":1,"source_count":2,"payload_count":2,"chains":["ethereum"],"chain_totals":[{"name":"ethereum","contracts":-1,"nfts":2}]}"#,
    )
    .unwrap();

    assert_invariant(&features, &blocking, "chain totals must be non-negative");
}

#[test]
fn rejects_fallback_atom_with_different_persisted_scoring_identity() {
    let (_dir, features, blocking) = shared_atom_fixture();
    let path = features.join("query_denominators.f64");
    rewrite_f64(&path, &[1.0, 2.0]);

    assert_invariant(
        &features,
        &blocking,
        "fallback atom 0 mixes chain or scoring-feature identity",
    );
}
