use dedup_engine::metadata::canonicalize_json;
use dedup_model::MetadataPrefilterParameters;

#[test]
fn canonical_metadata_golden_bytes_are_stable() {
    let value = canonicalize_json(
        r#"{"Z":"  CAFÉ ","n":1.00,"attributes":[{"value":"Blue","trait_type":"Color"}],"a":true}"#,
    )
    .unwrap();
    assert_eq!(
        value.canonical_bytes(),
        r#"{"a":true,"attributes":[{"trait_type":"color","value":"blue"}],"n":1e0,"z":"café"}"#
            .as_bytes()
    );
}

#[test]
fn derived_lsh_shape_golden_is_stable() {
    let parameters = MetadataPrefilterParameters {
        template_jaccard_threshold: 0.75,
        lsh_bands: 999,
        lsh_rows_per_band: 999,
        target_candidate_recall: 0.99,
        neighbors_per_target_chain: 16,
        max_candidates_per_target_chain: 64,
        max_outgoing_candidates_per_contract: 256,
        exact_bucket_size_cap: 4_096,
    };
    assert_eq!(parameters.derived_lsh_shape(), Some((17, 5)));
}

#[test]
fn canonical_metadata_depth_boundary_golden_is_stable() {
    let accepted = format!("{}0{}", "[".repeat(128), "]".repeat(128));
    let rejected = format!("{}0{}", "[".repeat(130), "]".repeat(130));
    assert!(canonicalize_json(&accepted).is_ok());
    assert!(canonicalize_json(&rejected).is_err());
}
