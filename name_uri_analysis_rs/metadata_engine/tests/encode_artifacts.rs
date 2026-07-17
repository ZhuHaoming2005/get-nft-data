//! Encode artifact tests: payload CAS, feature SoA, and Match-facing views.

use metadata_engine::encode::{
    payload_digest, write_encode_artifacts,
    write_encode_artifacts_with_contracts_and_atoms_with_progress, EncodeBundle, EncodeContractRow,
    EncodePayloadRow, EncodeSourceRow, PayloadCasWriter, ENCODE_SCHEMA_REVISION,
};
use metadata_engine::format::{map_f64_array, map_u32_array, map_u64_array};

#[test]
fn payload_cas_requires_full_byte_equality_on_hash_hit() {
    let dir = tempfile::tempdir().unwrap();
    let blobs = dir.path().join("payload_blobs");
    let mut writer = PayloadCasWriter::create(&blobs, 64 * 1024).unwrap();

    let bytes_a = b"payload-aaa-unique-bytes";
    let id_a = writer.insert(bytes_a).unwrap();

    // Identical bytes reuse the same payload_id.
    let id_reuse = writer.insert(bytes_a).unwrap();
    assert_eq!(id_a, id_reuse);

    // Digest collision with different bytes must allocate a new payload_id.
    let digest_a = payload_digest(bytes_a);
    let bytes_b = b"payload-BBB-DIFFERENT!!";
    assert_ne!(bytes_a.as_slice(), bytes_b.as_slice());
    let id_b = writer
        .insert_with_digest_for_test(bytes_b, digest_a)
        .unwrap();
    assert_ne!(
        id_a, id_b,
        "hash hit with unequal bytes must not reuse payload_id"
    );

    // Same colliding digest + equal bytes to B should reuse B.
    let id_b_reuse = writer
        .insert_with_digest_for_test(bytes_b, digest_a)
        .unwrap();
    assert_eq!(id_b, id_b_reuse);

    let index = writer.finish().unwrap();
    assert_eq!(index.payload_count(), 2);

    assert!(blobs.join("pack-000000.bin").is_file());
    assert!(blobs.join("payload_offsets.u64").is_file());
    assert!(blobs.join("payload_lengths.u32").is_file());
    assert!(blobs.join("payload_hashes.bin").is_file());

    let lengths = map_u32_array(&blobs.join("payload_lengths.u32")).unwrap();
    assert_eq!(lengths[id_a as usize], bytes_a.len() as u32);
    assert_eq!(lengths[id_b as usize], bytes_b.len() as u32);

    let recovered_a = index.read_payload_bytes(id_a).unwrap();
    let recovered_b = index.read_payload_bytes(id_b).unwrap();
    assert_eq!(recovered_a, bytes_a);
    assert_eq!(recovered_b, bytes_b);
}

#[test]
fn payload_cas_reads_multi_pack_ranges_in_id_order() {
    let dir = tempfile::tempdir().unwrap();
    let blobs = dir.path().join("payload_blobs");
    let mut writer = PayloadCasWriter::create(&blobs, 12).unwrap();
    let payloads = [
        b"first".as_slice(),
        b"second".as_slice(),
        b"third".as_slice(),
        b"fourth".as_slice(),
    ];
    for payload in payloads {
        writer.insert(payload).unwrap();
    }
    let index = writer.finish().unwrap();

    assert_eq!(index.payload_len(2).unwrap(), payloads[2].len());
    assert_eq!(
        index.read_payload_range(1..4).unwrap(),
        payloads[1..]
            .iter()
            .map(|payload| payload.to_vec())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        index.read_payload_ids(&[3, 0, 2]).unwrap(),
        [payloads[3], payloads[0], payloads[2]]
            .into_iter()
            .map(|payload| payload.to_vec())
            .collect::<Vec<_>>()
    );
}

#[test]
fn writes_feature_soa_and_bidirectional_contract_token_csr() {
    let dir = tempfile::tempdir().unwrap();
    let bundle = dir.path().join(format!("encode-{ENCODE_SCHEMA_REVISION}"));

    let payloads = vec![
        EncodePayloadRow {
            template_terms: vec![(10, 2), (11, 1)],
            content_terms: vec![(20, 3)],
        },
        EncodePayloadRow {
            template_terms: vec![(12, 1)],
            content_terms: vec![(21, 1), (22, 4)],
        },
    ];
    // source 0 -> contract 0, payload 0, tokens {1, 3}
    // source 1 -> contract 0, payload 0, tokens {3, 5}
    // source 2 -> contract 1, payload 1, tokens {1, 7}
    let sources = vec![
        EncodeSourceRow {
            contract_id: 0,
            payload_id: 0,
            retained_token_ids: vec![1, 3],
        },
        EncodeSourceRow {
            contract_id: 0,
            payload_id: 0,
            retained_token_ids: vec![3, 5],
        },
        EncodeSourceRow {
            contract_id: 1,
            payload_id: 1,
            retained_token_ids: vec![1, 7],
        },
    ];

    write_encode_artifacts(&bundle, &sources, &payloads).unwrap();

    assert!(bundle.join("source_to_payload.u32").is_file());
    assert!(bundle.join("payload_template_offsets.u64").is_file());
    assert!(bundle.join("payload_template_terms.u32").is_file());
    assert!(bundle.join("payload_template_freqs.u32").is_file());
    assert!(bundle.join("payload_content_offsets.u64").is_file());
    assert!(bundle.join("payload_content_terms.u32").is_file());
    assert!(bundle.join("payload_content_freqs.u32").is_file());
    assert!(bundle.join("contract_token_offsets.u64").is_file());
    assert!(bundle.join("contract_tokens.u32").is_file());
    assert!(bundle.join("token_member_offsets.u64").is_file());
    assert!(bundle.join("token_member_contracts.u32").is_file());
    assert!(bundle.join("token_member_sources.u32").is_file());

    let source_to_payload = map_u32_array(&bundle.join("source_to_payload.u32")).unwrap();
    assert_eq!(&*source_to_payload, &[0, 0, 1]);

    let tpl_off = map_u64_array(&bundle.join("payload_template_offsets.u64")).unwrap();
    let tpl_terms = map_u32_array(&bundle.join("payload_template_terms.u32")).unwrap();
    let tpl_freqs = map_u32_array(&bundle.join("payload_template_freqs.u32")).unwrap();
    // offsets length = payload_count + 1
    assert_eq!(&*tpl_off, &[0, 2, 3]);
    assert_eq!(&*tpl_terms, &[10, 11, 12]);
    assert_eq!(&*tpl_freqs, &[2, 1, 1]);

    let cnt_off = map_u64_array(&bundle.join("payload_content_offsets.u64")).unwrap();
    let cnt_terms = map_u32_array(&bundle.join("payload_content_terms.u32")).unwrap();
    let cnt_freqs = map_u32_array(&bundle.join("payload_content_freqs.u32")).unwrap();
    assert_eq!(&*cnt_off, &[0, 1, 3]);
    assert_eq!(&*cnt_terms, &[20, 21, 22]);
    assert_eq!(&*cnt_freqs, &[3, 1, 4]);

    // contract 0 -> tokens {1,3,5}; contract 1 -> tokens {1,7}
    let c_off = map_u64_array(&bundle.join("contract_token_offsets.u64")).unwrap();
    let c_tok = map_u32_array(&bundle.join("contract_tokens.u32")).unwrap();
    assert_eq!(&*c_off, &[0, 3, 5]);
    assert_eq!(&*c_tok, &[1, 3, 5, 1, 7]);

    // Dense token-id CSR: empty slots for unused ids 0,2,4,6.
    // token 1: (0,0), (1,2)
    // token 3: (0,0), (0,1)
    // token 5: (0,1)
    // token 7: (1,2)
    let t_off = map_u64_array(&bundle.join("token_member_offsets.u64")).unwrap();
    let t_contracts = map_u32_array(&bundle.join("token_member_contracts.u32")).unwrap();
    let t_sources = map_u32_array(&bundle.join("token_member_sources.u32")).unwrap();
    assert_eq!(&*t_off, &[0, 0, 2, 2, 4, 4, 5, 5, 6]);
    assert_eq!(&*t_contracts, &[0, 1, 0, 0, 0, 1]);
    assert_eq!(&*t_sources, &[0, 2, 0, 1, 1, 2]);

    // Stable IDs: same input order → same ids / same files.
    let bundle2 = dir
        .path()
        .join(format!("encode-{ENCODE_SCHEMA_REVISION}-again"));
    write_encode_artifacts(&bundle2, &sources, &payloads).unwrap();
    let s2 = map_u32_array(&bundle2.join("source_to_payload.u32")).unwrap();
    assert_eq!(&*source_to_payload, &*s2);
    let c2 = map_u32_array(&bundle2.join("contract_tokens.u32")).unwrap();
    assert_eq!(&*c_tok, &*c2);

    let opened = EncodeBundle::open(&bundle).unwrap();
    let view = opened.feature_view();
    assert_eq!(&*view.source_to_payload, &[0, 0, 1]);
    assert_eq!(view.contract_tokens(0), &[1, 3, 5]);
}

#[test]
fn feature_persist_progress_is_monotonic_and_exact() {
    let dir = tempfile::tempdir().unwrap();
    let sources = vec![EncodeSourceRow {
        contract_id: 0,
        payload_id: 0,
        retained_token_ids: vec![1, 2],
    }];
    let payloads = vec![EncodePayloadRow {
        template_terms: vec![(1, 2)],
        content_terms: vec![(2, 3)],
    }];
    let contracts = vec![EncodeContractRow {
        contract_id: 0,
        chain_id: 0,
        source_doc_id: 0,
        payload_id: 0,
        weight: 1,
    }];
    let mut events = Vec::new();

    write_encode_artifacts_with_contracts_and_atoms_with_progress(
        dir.path(),
        &sources,
        &payloads,
        &contracts,
        &[vec![0]],
        |completed, total| events.push((completed, total)),
    )
    .unwrap();

    assert_eq!(events.first().copied(), Some((0, events[0].1)));
    assert!(events[0].1 > 0);
    assert!(events.windows(2).all(|pair| pair[0].0 <= pair[1].0));
    let terminal = events.last().copied().unwrap();
    assert_eq!(terminal.0, terminal.1);
    assert!(events.iter().all(|(_, total)| *total == terminal.1));
}

#[test]
fn feature_persist_reports_candidate_group_work_without_reopening_artifacts() {
    let dir = tempfile::tempdir().unwrap();
    let sources = vec![
        EncodeSourceRow {
            contract_id: 0,
            payload_id: 0,
            retained_token_ids: vec![1, 2],
        },
        EncodeSourceRow {
            contract_id: 1,
            payload_id: 0,
            retained_token_ids: vec![1],
        },
        EncodeSourceRow {
            contract_id: 2,
            payload_id: 0,
            retained_token_ids: vec![1],
        },
    ];
    let payloads = vec![EncodePayloadRow {
        template_terms: vec![(1, 1)],
        content_terms: vec![(2, 1)],
    }];
    let contracts = (0..3)
        .map(|contract_id| EncodeContractRow {
            contract_id,
            chain_id: 0,
            source_doc_id: contract_id,
            payload_id: 0,
            weight: 1,
        })
        .collect::<Vec<_>>();

    let stats = write_encode_artifacts_with_contracts_and_atoms_with_progress(
        dir.path(),
        &sources,
        &payloads,
        &contracts,
        &[vec![0, 1, 2]],
        |_, _| {},
    )
    .unwrap();

    assert_eq!(stats.token_pair_work, 3);
    assert_eq!(stats.max_token_members, 3);
    assert_eq!(stats.fallback_pair_work, 3);
    assert_eq!(stats.max_fallback_members, 3);
}

#[test]
fn feature_bundle_open_ignores_payload_cas() {
    let dir = tempfile::tempdir().unwrap();
    let bundle = dir.path().join(format!("encode-{ENCODE_SCHEMA_REVISION}"));

    write_encode_artifacts(
        &bundle,
        &[EncodeSourceRow {
            contract_id: 0,
            payload_id: 0,
            retained_token_ids: vec![1],
        }],
        &[EncodePayloadRow {
            template_terms: vec![(1, 1)],
            content_terms: vec![],
        }],
    )
    .unwrap();

    let mut cas = PayloadCasWriter::create(&bundle.join("payload_blobs"), 4096).unwrap();
    cas.insert(b"secret-raw-json").unwrap();
    cas.finish().unwrap();
    assert!(bundle.join("payload_blobs").is_dir());

    let opened = EncodeBundle::open(&bundle).unwrap();
    let view = opened.feature_view();

    assert_eq!(&*view.source_to_payload, &[0]);
    assert_eq!(view.contract_tokens(0), &[1]);
}

#[test]
fn writes_spec_53_identity_and_scoring_columns() {
    let dir = tempfile::tempdir().unwrap();
    let bundle = dir.path().join(format!("encode-{ENCODE_SCHEMA_REVISION}"));

    write_encode_artifacts(
        &bundle,
        &[EncodeSourceRow {
            contract_id: 0,
            payload_id: 0,
            retained_token_ids: vec![1],
        }],
        &[EncodePayloadRow {
            template_terms: vec![(1, 1)],
            content_terms: vec![],
        }],
    )
    .unwrap();

    // Root payload_lengths is distinct from payload_blobs/payload_lengths.u32.
    assert!(bundle.join("payload_lengths.u32").is_file());
    assert!(!bundle.join("payload_blobs").exists());
    assert!(bundle.join("query_denominators.f64").is_file());
    assert!(bundle.join("prepared_weight_offsets.u64").is_file());
    assert!(bundle.join("prepared_weights.f64").is_file());
    assert!(bundle.join("contract_source.u32").is_file());
    assert!(bundle.join("contract_payload.u32").is_file());
    assert!(bundle.join("contract_weight.u64").is_file());
    assert!(bundle.join("fallback_atoms_offsets.u64").is_file());
    assert!(bundle.join("fallback_atoms_members.u32").is_file());

    assert_eq!(
        &*map_u32_array(&bundle.join("payload_lengths.u32")).unwrap(),
        &[0]
    );
    assert_eq!(
        map_f64_array(&bundle.join("query_denominators.f64"))
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        &*map_u64_array(&bundle.join("prepared_weight_offsets.u64")).unwrap(),
        &[0, 1]
    );
    assert_eq!(
        map_f64_array(&bundle.join("prepared_weights.f64"))
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        &*map_u32_array(&bundle.join("contract_source.u32")).unwrap(),
        &[0]
    );
    assert_eq!(
        &*map_u32_array(&bundle.join("contract_payload.u32")).unwrap(),
        &[0]
    );
    assert_eq!(
        &*map_u64_array(&bundle.join("contract_weight.u64")).unwrap(),
        &[1]
    );
    assert_eq!(
        &*map_u64_array(&bundle.join("fallback_atoms_offsets.u64")).unwrap(),
        &[0, 1]
    );
    assert_eq!(
        &*map_u32_array(&bundle.join("fallback_atoms_members.u32")).unwrap(),
        &[0]
    );
}
