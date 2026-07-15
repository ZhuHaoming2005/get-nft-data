use metadata_engine::cascade::{content_upper_safe, score_pair, PairScoreDecision};
use metadata_engine::encode::{
    write_encode_artifacts_with_contracts, EncodeBundle, EncodeContractRow, EncodePayloadRow,
    EncodeSourceRow,
};
use metadata_engine::scoring::{content_pair_score, template_score_bidirectional};

#[test]
fn scoring_columns_are_complete_and_exact_content_is_symmetric() {
    let dir = tempfile::tempdir().unwrap();
    let payloads = vec![
        EncodePayloadRow {
            template_terms: vec![(1, 2), (2, 1)],
            content_terms: vec![(10, 2), (11, 1)],
        },
        EncodePayloadRow {
            template_terms: vec![(1, 1), (3, 1)],
            content_terms: vec![(10, 1), (12, 2)],
        },
    ];
    let sources = vec![
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
    write_encode_artifacts_with_contracts(
        dir.path(),
        &sources,
        &payloads,
        &[
            EncodeContractRow {
                contract_id: 0,
                chain_id: 0,
                source_doc_id: 0,
                payload_id: 0,
                weight: 10,
            },
            EncodeContractRow {
                contract_id: 1,
                chain_id: 0,
                source_doc_id: 1,
                payload_id: 1,
                weight: 2,
            },
        ],
    )
    .unwrap();
    let bundle = EncodeBundle::open(dir.path()).unwrap();
    let view = bundle.feature_view();
    assert_eq!(view.query_denominators.len(), 2);
    assert_eq!(&*view.prepared_weight_offsets, &[0, 2, 4]);
    assert_eq!(view.prepared_weights.len(), 4);
    let (lr, rl) = template_score_bidirectional(view, 0, 1);
    assert!(lr.is_finite() && rl.is_finite() && lr > 0.0 && rl > 0.0);
    let a = content_pair_score(view, 0, 1);
    let b = content_pair_score(view, 1, 0);
    assert_eq!(a.to_bits(), b.to_bits());
    assert!((0.0..=1.0).contains(&a));
    assert!(content_upper_safe(view, 0, 1) >= a);
}

#[test]
fn production_pair_scoring_applies_only_proof_safe_cascade_rejects() {
    let dir = tempfile::tempdir().unwrap();
    let payloads = vec![
        EncodePayloadRow {
            template_terms: vec![(1, 1)],
            content_terms: vec![(10, 1)],
        },
        EncodePayloadRow {
            template_terms: vec![(2, 1)],
            content_terms: vec![(10, 1)],
        },
        EncodePayloadRow {
            template_terms: vec![(1, 1)],
            content_terms: vec![(20, 1)],
        },
    ];
    let sources = (0..3)
        .map(|contract_id| EncodeSourceRow {
            contract_id,
            payload_id: contract_id,
            retained_token_ids: vec![],
        })
        .collect::<Vec<_>>();
    write_encode_artifacts_with_contracts(
        dir.path(),
        &sources,
        &payloads,
        &(0..3)
            .map(|contract_id| EncodeContractRow {
                contract_id,
                chain_id: 0,
                source_doc_id: contract_id,
                payload_id: contract_id,
                weight: 1,
            })
            .collect::<Vec<_>>(),
    )
    .unwrap();
    let bundle = EncodeBundle::open(dir.path()).unwrap();
    let view = bundle.feature_view();

    assert_eq!(
        score_pair(view, 0, 1),
        PairScoreDecision::RejectL0TemplateNoOverlap
    );
    assert_eq!(
        score_pair(view, 0, 2),
        PairScoreDecision::RejectL2ContentNoOverlap
    );
}
