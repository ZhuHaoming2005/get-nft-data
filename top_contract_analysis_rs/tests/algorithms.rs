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
fn normalize_url_trims_trailing_slashes_and_lowercases() {
    assert_eq!(normalize_url(" HTTPS://EXAMPLE.COM/Path/ "), "https://example.com/path");
}

#[test]
fn metadata_document_from_json_flattens_relevant_fields() {
    let raw = r#"{"description":"cool cat","attributes":[{"trait_type":"Mood","value":"Happy"}]}"#;
    assert_eq!(metadata_document_from_json(raw), "mood happy cool cat");
}

#[test]
fn score_name_pairs_matches_existing_threshold_behavior() {
    let scores = score_name_pairs(&["Azuki".into()], &["Azuki #1".into()]);
    assert_eq!(scores.len(), 1);
    assert!(scores[0] >= 95.0);
}

#[test]
fn score_metadata_documents_rewards_shared_keywords() {
    let scores = score_metadata_documents(&["gold dragon rare".into()], &["rare dragon gold".into()]);
    assert!(scores[0] >= 0.8);
}
