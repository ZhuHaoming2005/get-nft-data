use std::collections::HashSet;

use once_cell::sync::Lazy;
use regex::Regex;
use strsim::{jaro_winkler, normalized_levenshtein};
use top_contract_analysis_rs::analysis::address_records::build_infringing_token_records;
use top_contract_analysis_rs::analysis::duplicate::build_duplicate_candidates;
use top_contract_analysis_rs::analysis::scoring::{
    metadata_document_from_json, score_metadata_documents, score_name_pairs, ScoringError,
};
use top_contract_analysis_rs::analysis::signals::analyze_transfer_signals;
use top_contract_analysis_rs::normalize::{normalize_name, normalize_symbol, normalize_url};
use top_contract_analysis_rs::models::{
    DatabaseNftRecord, DuplicateCandidate, SeedNft, TransferRecord,
};
use unicode_normalization::UnicodeNormalization;

static TOKEN_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"[\p{L}\p{N}_]+").unwrap());
static TRAILING_PATTERNS: Lazy<Vec<Regex>> = Lazy::new(|| {
    vec![
        Regex::new(r"\s*#\s*[0-9a-fA-FxX]+\s*$").unwrap(),
        Regex::new(r"\s*#\s*\d+\s*$").unwrap(),
        Regex::new(r"\s*-\s*\d+\s*$").unwrap(),
        Regex::new(r"\s*:\s*\d+\s*$").unwrap(),
        Regex::new(r"\s*\(\s*\d+\s*\)\s*$").unwrap(),
        Regex::new(r"\s*\[\s*\d+\s*\]\s*$").unwrap(),
        Regex::new(r"\s*/\s*\d+\s*$").unwrap(),
        Regex::new(r"\s+No\.?\s*\d+\s*$").unwrap(),
        Regex::new(r"\s+nr\.?\s*\d+\s*$").unwrap(),
        Regex::new(r"\s+\d{1,12}\s*$").unwrap(),
    ]
});
static WHITESPACE_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\s+").unwrap());

fn reference_normalize_nfkc(raw: &str) -> String {
    raw.nfkc().collect::<String>()
}

fn reference_strip_trailing_number_suffix(raw: &str) -> String {
    let mut text = reference_normalize_nfkc(raw).trim().to_string();
    let mut changed = true;
    let mut guard = 0;
    while changed && guard < 20 {
        changed = false;
        guard += 1;
        for pattern in TRAILING_PATTERNS.iter() {
            let updated = pattern.replace(&text, "").trim().to_string();
            if updated != text {
                text = updated;
                changed = true;
                break;
            }
        }
    }
    WHITESPACE_RE.replace_all(&text, " ").trim().to_string()
}

fn reference_normalize_name(raw: &str) -> String {
    reference_strip_trailing_number_suffix(raw).to_lowercase()
}

fn reference_normalize_text(raw: &str) -> String {
    let text = reference_normalize_nfkc(raw).to_lowercase();
    WHITESPACE_RE.replace_all(text.trim(), " ").to_string()
}

fn reference_name_score(left: &str, right: &str) -> f64 {
    let left_norm = reference_normalize_name(left);
    let right_norm = reference_normalize_name(right);
    if left_norm.is_empty() || right_norm.is_empty() {
        return 0.0;
    }
    if left_norm == right_norm {
        return 100.0;
    }
    ((jaro_winkler(&left_norm, &right_norm) * 0.65)
        + (normalized_levenshtein(&left_norm, &right_norm) * 0.35))
        * 100.0
}

fn reference_metadata_document_score(left: &str, right: &str) -> f64 {
    let left_doc = reference_normalize_text(left);
    let right_doc = reference_normalize_text(right);
    if left_doc.is_empty() || right_doc.is_empty() {
        return 0.0;
    }

    let left_tokens: HashSet<String> = TOKEN_RE
        .find_iter(&left_doc)
        .map(|m| m.as_str().to_lowercase())
        .filter(|token| token.len() >= 2)
        .collect();
    let right_tokens: HashSet<String> = TOKEN_RE
        .find_iter(&right_doc)
        .map(|m| m.as_str().to_lowercase())
        .filter(|token| token.len() >= 2)
        .collect();

    let union = left_tokens.union(&right_tokens).count();
    let overlap = left_tokens.intersection(&right_tokens).count();
    let jaccard = if union == 0 {
        0.0
    } else {
        overlap as f64 / union as f64
    };
    let similarity = jaro_winkler(&left_doc, &right_doc);
    (jaccard * 0.45) + (similarity * 0.55)
}

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
    assert_eq!(scores[0], 100.0);
}

#[test]
fn score_name_pairs_matches_reference_weighting_for_non_identical_names() {
    let scores = score_name_pairs(&["Moonbirds".into()], &["Moonbird".into()]).unwrap();
    assert_eq!(scores.len(), 1);
    assert!((scores[0] - reference_name_score("Moonbirds", "Moonbird")).abs() < 1e-12);
}

#[test]
fn score_metadata_documents_matches_reference_formula_for_reordered_keywords() {
    let scores =
        score_metadata_documents(&["gold dragon rare".into()], &["rare dragon gold".into()])
            .unwrap();
    assert_eq!(scores.len(), 1);
    assert!(
        (scores[0] - reference_metadata_document_score("gold dragon rare", "rare dragon gold"))
            .abs()
            < 1e-12
    );
}

#[test]
fn score_name_pairs_rejects_mismatched_input_lengths() {
    let err = score_name_pairs(&["Azuki".into()], &[]).unwrap_err();
    assert_eq!(err, ScoringError::MismatchedInputLengths);
}

#[test]
fn score_metadata_documents_rejects_mismatched_input_lengths() {
    let err = score_metadata_documents(&["gold dragon rare".into()], &[]).unwrap_err();
    assert_eq!(err, ScoringError::MismatchedInputLengths);
}

#[test]
fn duplicate_candidates_use_token_uri_and_name_reason_flags() {
    let seed_nfts = vec![SeedNft {
        token_uri: "ipfs://seed/1".into(),
        name: "Azuki #1".into(),
        ..Default::default()
    }];
    let snapshot_rows = vec![DatabaseNftRecord {
        contract_address: "0xdup".into(),
        token_id: "1".into(),
        token_uri: "ipfs://seed/1".into(),
        name: "Azuki Mirror #1".into(),
        symbol: "AZUKI".into(),
        ..Default::default()
    }];

    let rows = build_duplicate_candidates(&seed_nfts, &snapshot_rows, 95.0, 0.55);

    assert_eq!(rows[0].contract_address, "0xdup");
    assert!(rows[0].match_reasons.contains(&"token_uri_match".into()));
}

#[test]
fn transfer_signals_calculate_fast_spread() {
    let signals = analyze_transfer_signals(&[
        TransferRecord::mint("0xdup", "1", 100, "0xholder1"),
        TransferRecord::transfer("0xdup", "1", 120, "0xholder1", "0xholder2"),
    ]);

    assert_eq!(signals.mint_to_first_transfer_seconds, Some(20));
    assert!(signals.fast_spread);
}

#[test]
fn infringing_token_records_capture_mint_context_and_match_reasons() {
    let candidates = vec![DuplicateCandidate {
        contract_address: "0xdup".into(),
        token_id: "1".into(),
        match_reasons: vec!["token_uri_match".into(), "name_match".into()],
        ..Default::default()
    }];
    let transfers = vec![
        TransferRecord::mint("0xdup", "1", 100, "0xminter"),
        TransferRecord::transfer("0xdup", "1", 120, "0xminter", "0xholder"),
    ];

    let rows = build_infringing_token_records("0xdup", &candidates, &transfers);

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].token_id, "1");
    assert_eq!(rows[0].minter_address, "0xminter");
    assert_eq!(rows[0].first_transfer_time, 100);
    assert_eq!(
        rows[0].match_reasons,
        vec!["token_uri_match".to_string(), "name_match".to_string()]
    );
}
