//! Streaming parser differentials against the compatibility oracle.

use metadata_engine::encode::parse_metadata_documents;

mod support;
use support::parser_oracle::legacy_parse_metadata_documents;

const FIXTURES: &[&str] = &[
    // valid object with nested metadata / raw / rawMetadata
    r#"{
        "name": "Seed #1",
        "description": "Shared Story",
        "attributes": [
            {"trait_type": "Background", "value": "Red"}
        ],
        "image": "ipfs://seed/1.png",
        "metadata": {
            "description": "Inner Meta",
            "raw": {"description": "Raw Nested"},
            "rawMetadata": {"description": "Raw Meta Nested"}
        }
    }"#,
    // valid array root
    r#"[{"description":"Alpha Item"},{"description":"Beta Item"}]"#,
    // duplicate keys: serde_json::Value last-wins
    r#"{"description":"first","description":"second last wins"}"#,
    // invalid JSON → raw-text fallback
    "not json metadata at all",
    // invalid JSON that is still eligibility-shaped
    "{not valid json but brace",
    // Unicode NFKC / lowercase / whitespace collapse
    "{\"description\":\"\\uFF27\\uFF4F\\uFF4C\\uFF44\\u3000Dragon\"}",
    // short tokens filtered (len >= 2); underscore kept
    r#"{"description":"a b cd gold_dragon 金色"}"#,
    // numbers/bools on prefilter whitelist paths (content ignores leaf number/bool)
    r#"{
        "description": true,
        "seller_fee_basis_points": 500,
        "royalty": false,
        "attributes": [{"trait_type": "Level", "value": "10"}]
    }"#,
    // content whitelist vs non-whitelist
    r#"{
        "description": "Gold Dragon",
        "attributes": [
            {"trait_type": "Background", "value": "Gold"},
            {"trait_type": "Eyes", "value": "Laser"}
        ],
        "seller_fee_basis_points": 500,
        "irrelevant": "Hidden Lore"
    }"#,
    // empty / whitespace-only
    "",
    "   ",
];

#[test]
fn streaming_parser_matches_legacy_documents() {
    for raw in FIXTURES {
        let legacy = legacy_parse_metadata_documents(raw);
        let streamed = parse_metadata_documents(raw);
        assert_eq!(
            streamed.prefilter_tokens, legacy.prefilter_tokens,
            "prefilter mismatch for fixture: {raw}"
        );
        assert_eq!(
            streamed.content_tokens, legacy.content_tokens,
            "content mismatch for fixture: {raw}"
        );
    }
}

#[test]
fn sixty_four_kib_eligibility_boundary_clears_content_when_ineligible() {
    let overlong = format!("{{\"description\":\"{}\"}}", "x".repeat(64 * 1024));
    assert!(overlong.len() > 64 * 1024);

    let legacy = legacy_parse_metadata_documents(&overlong);
    let streamed = parse_metadata_documents(&overlong);

    assert!(
        legacy.content_tokens.is_empty(),
        "legacy content must be empty when over 64 KiB"
    );
    assert_eq!(streamed.content_tokens, legacy.content_tokens);
    assert_eq!(streamed.prefilter_tokens, legacy.prefilter_tokens);
    // Oversize valid JSON still yields a prefilter (description + x's), but no content.
    assert!(
        !streamed.prefilter_tokens.is_empty(),
        "prefilter should still be populated for oversize valid JSON"
    );
}

#[test]
fn exactly_64_kib_eligible_json_keeps_content_tokens() {
    // Build a payload whose byte length is exactly MAX (64 KiB) after construction.
    let prefix = "{\"description\":\"";
    let suffix = "\"}";
    let fill = 64 * 1024 - prefix.len() - suffix.len();
    let exactly = format!("{prefix}{}{suffix}", "y".repeat(fill));
    assert_eq!(exactly.len(), 64 * 1024);

    let legacy = legacy_parse_metadata_documents(&exactly);
    let streamed = parse_metadata_documents(&exactly);

    assert_eq!(streamed.prefilter_tokens, legacy.prefilter_tokens);
    assert_eq!(streamed.content_tokens, legacy.content_tokens);
    assert!(
        !streamed.content_tokens.is_empty(),
        "exactly 64 KiB eligible JSON should keep content tokens"
    );
}

#[test]
fn duplicate_keys_match_serde_json_last_wins() {
    let raw = r#"{"description":"first","description":"second last wins"}"#;
    let streamed = parse_metadata_documents(raw);
    let legacy = legacy_parse_metadata_documents(raw);
    assert_eq!(streamed.content_tokens, legacy.content_tokens);
    assert!(
        streamed
            .content_tokens
            .iter()
            .any(|t| t == "second" || t.contains("second")),
        "last-wins description should appear in content tokens: {:?}",
        streamed.content_tokens
    );
    assert!(
        !streamed.content_tokens.iter().any(|t| t == "first"),
        "first duplicate key value must not win: {:?}",
        streamed.content_tokens
    );
}

#[test]
fn invalid_json_raw_text_fallback_tokens() {
    let raw = "Gold   DRAGON   lore";
    let streamed = parse_metadata_documents(raw);
    let legacy = legacy_parse_metadata_documents(raw);
    assert_eq!(streamed.prefilter_tokens, legacy.prefilter_tokens);
    assert_eq!(streamed.content_tokens, legacy.content_tokens);
    // Not eligible (does not start with { or [) → content empty.
    assert!(streamed.content_tokens.is_empty());
    assert_eq!(
        streamed.prefilter_tokens,
        vec!["gold".to_string(), "dragon".to_string(), "lore".to_string()]
    );
}

#[test]
fn unicode_nfkc_and_token_length_filter() {
    let raw = "{\"description\":\"\\uFF27\\uFF4F\\uFF4C\\uFF44\\u3000Dragon a b\"}";
    let streamed = parse_metadata_documents(raw);
    let legacy = legacy_parse_metadata_documents(raw);
    assert_eq!(streamed, legacy);
    assert!(streamed.content_tokens.contains(&"gold".to_string()));
    assert!(streamed.content_tokens.contains(&"dragon".to_string()));
    assert!(!streamed.content_tokens.iter().any(|t| t == "a" || t == "b"));
}

#[test]
fn numbers_and_bools_appear_in_prefilter_not_content_leaves() {
    let raw = r#"{"description":true,"seller_fee_basis_points":500,"royalty":false}"#;
    let streamed = parse_metadata_documents(raw);
    let legacy = legacy_parse_metadata_documents(raw);
    assert_eq!(streamed.prefilter_tokens, legacy.prefilter_tokens);
    assert_eq!(streamed.content_tokens, legacy.content_tokens);

    assert!(streamed.prefilter_tokens.iter().any(|t| t == "true"));
    assert!(streamed.prefilter_tokens.iter().any(|t| t == "500"));
    assert!(streamed.prefilter_tokens.iter().any(|t| t == "false"));
    assert!(
        streamed.content_tokens.is_empty(),
        "content must ignore number/bool leaves: {:?}",
        streamed.content_tokens
    );
}

#[test]
fn pathological_json_nesting_falls_back_without_recursive_deserialization() {
    let depth = 20_000;
    let raw = format!("{}\"leaf\"{}", "[".repeat(depth), "]".repeat(depth));
    let parsed = parse_metadata_documents(&raw);
    assert!(parsed.prefilter_tokens.iter().any(|token| token == "leaf"));
}
