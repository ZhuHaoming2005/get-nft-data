use top_contract_analysis_rs::analysis::scoring::{
    metadata_document_from_json, score_metadata_documents, score_name_pairs,
};
use top_contract_analysis_rs::normalize::{normalize_name, normalize_symbol, normalize_url};

#[test]
fn normalize_name_strips_trailing_token_numbers() {
    assert_eq!(normalize_name("Azuki #123"), "azuki");
}

#[test]
fn normalize_symbol_trims_and_lowercases() {
    assert_eq!(normalize_symbol(" AZUKI "), "azuki");
}

#[test]
fn normalize_symbol_applies_nfkc_before_lowercasing() {
    assert_eq!(normalize_symbol(" ＡＺＵＫＩ "), "azuki");
}

#[test]
fn normalize_url_canonicalizes_reference_schemes() {
    assert_eq!(
        normalize_url(" ipfs://ipfs/QmHash/metadata.json?download=1 "),
        Some("ipfs:QmHash/metadata.json".into())
    );
    assert_eq!(
        normalize_url(
            " https://arweave.net/abcdefghijklmnopqrstuvwxyzABCDEFG1234567890/image.png#view "
        ),
        Some("ar:abcdefghijklmnopqrstuvwxyzABCDEFG1234567890/image.png".into())
    );
    assert_eq!(
        normalize_url(" HTTPS://EXAMPLE.COM/Path/ "),
        Some("https://example.com/path".into())
    );
}

#[test]
fn normalize_url_rejects_nullish_and_data_values() {
    assert_eq!(normalize_url("null"), None);
    assert_eq!(normalize_url("data:image/png;base64,AAAA"), None);
}

#[test]
fn metadata_document_from_json_flattens_relevant_fields() {
    let raw = r#"{"description":"cool cat","attributes":[{"trait_type":"Mood","value":"Happy"}]}"#;
    assert_eq!(metadata_document_from_json(raw), "mood happy cool cat");
}

#[test]
fn score_name_pairs_matches_existing_threshold_behavior() {
    let scores = score_name_pairs(&["Azuki".into()], &["Azuki #1".into()]).unwrap();
    assert_eq!(scores.len(), 1);
    assert!(scores[0] >= 95.0);
}

#[test]
fn score_metadata_documents_rewards_shared_keywords() {
    let scores =
        score_metadata_documents(&["gold dragon rare".into()], &["rare dragon gold".into()])
            .unwrap();
    assert!(scores[0] >= 0.75);
}

#[test]
fn score_name_pairs_rejects_mismatched_input_lengths() {
    let err = score_name_pairs(&["Azuki".into()], &[]).unwrap_err();
    assert!(err.contains("identical lengths"));
}

#[test]
fn score_metadata_documents_rejects_mismatched_input_lengths() {
    let err = score_metadata_documents(&["gold dragon rare".into()], &[]).unwrap_err();
    assert!(err.contains("identical lengths"));
}
