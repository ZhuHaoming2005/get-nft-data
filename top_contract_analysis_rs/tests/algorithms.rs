use std::collections::{HashMap, HashSet};

use once_cell::sync::Lazy;
use regex::Regex;
use strsim::jaro_winkler;
use top_contract_analysis_rs::analysis::address_records::{
    build_infringing_token_records, build_infringing_token_records_with_context,
};
use top_contract_analysis_rs::analysis::duplicate::build_duplicate_candidates;
use top_contract_analysis_rs::analysis::scoring::{
    metadata_document_from_json, score_metadata_documents, score_name_pairs, ScoringError,
};
use top_contract_analysis_rs::analysis::signals::analyze_transfer_signals;
use top_contract_analysis_rs::models::{
    DatabaseNftRecord, DuplicateCandidate, SeedNft, TransferRecord,
};
use top_contract_analysis_rs::normalize::{normalize_name, normalize_symbol, normalize_url};
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
    jaro_winkler(&left_norm, &right_norm) * 100.0
}

fn reference_metadata_tokens(raw: &str) -> Vec<String> {
    TOKEN_RE
        .find_iter(&reference_normalize_text(raw))
        .map(|m| m.as_str().to_lowercase())
        .filter(|token| token.len() >= 2)
        .collect()
}

fn reference_bm25_corpus_stats(docs: &[String]) -> (f64, HashMap<String, usize>) {
    let mut total_terms = 0usize;
    let mut doc_freqs = HashMap::new();

    for doc in docs {
        let tokens = reference_metadata_tokens(doc);
        total_terms += tokens.len();
        for token in tokens.into_iter().collect::<HashSet<_>>() {
            *doc_freqs.entry(token).or_insert(0) += 1;
        }
    }

    let avg_doc_len = if docs.is_empty() {
        0.0
    } else {
        total_terms as f64 / docs.len() as f64
    };
    (avg_doc_len, doc_freqs)
}

fn reference_bm25_score_tokens(
    query_tokens: &[String],
    doc_tokens: &[String],
    total_docs: usize,
    avg_doc_len: f64,
    doc_freqs: &HashMap<String, usize>,
) -> f64 {
    if query_tokens.is_empty() || doc_tokens.is_empty() || total_docs == 0 || avg_doc_len <= 0.0 {
        return 0.0;
    }

    let mut term_freqs = HashMap::new();
    for token in doc_tokens {
        *term_freqs.entry(token).or_insert(0usize) += 1;
    }

    let k1 = 1.2;
    let b = 0.75;
    let doc_len = doc_tokens.len() as f64;
    let norm = k1 * (1.0 - b + b * doc_len / avg_doc_len);

    query_tokens
        .iter()
        .map(|token| {
            let tf = *term_freqs.get(token).unwrap_or(&0) as f64;
            if tf == 0.0 {
                return 0.0;
            }
            let df = *doc_freqs.get(token).unwrap_or(&0) as f64;
            let idf = ((total_docs as f64 - df + 0.5) / (df + 0.5) + 1.0).ln();
            idf * (tf * (k1 + 1.0)) / (tf + norm)
        })
        .sum()
}

fn reference_metadata_document_score(left: &str, right: &str, corpus_docs: &[String]) -> f64 {
    let query_tokens = reference_metadata_tokens(left);
    let doc_tokens = reference_metadata_tokens(right);
    let (avg_doc_len, doc_freqs) = reference_bm25_corpus_stats(corpus_docs);
    let self_score = reference_bm25_score_tokens(
        &query_tokens,
        &query_tokens,
        corpus_docs.len(),
        avg_doc_len,
        &doc_freqs,
    );
    let denominator = if self_score > 0.0 { self_score } else { 1.0 };
    (reference_bm25_score_tokens(
        &query_tokens,
        &doc_tokens,
        corpus_docs.len(),
        avg_doc_len,
        &doc_freqs,
    ) / denominator)
        .clamp(0.0, 1.0)
}

fn reference_metadata_document_score_old(left: &str, right: &str) -> f64 {
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
fn score_name_pairs_matches_jaro_winkler_for_non_identical_names() {
    let scores = score_name_pairs(&["Moonbirds".into()], &["Moonbird".into()]).unwrap();
    assert_eq!(scores.len(), 1);
    assert!((scores[0] - reference_name_score("Moonbirds", "Moonbird")).abs() < 1e-12);
}

#[test]
fn score_metadata_documents_uses_normalized_bm25_for_reordered_keywords() {
    let scores =
        score_metadata_documents(&["gold dragon rare".into()], &["rare dragon gold".into()])
            .unwrap();
    assert_eq!(scores.len(), 1);
    let corpus_docs = vec!["rare dragon gold".to_string()];
    assert!(
        (scores[0]
            - reference_metadata_document_score(
                "gold dragon rare",
                "rare dragon gold",
                &corpus_docs
            ))
        .abs()
            < 1e-12
    );
    assert!(
        (scores[0] - reference_metadata_document_score_old("gold dragon rare", "rare dragon gold"))
            .abs()
            > 1e-6
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
fn duplicate_candidates_compare_seed_names_without_length_prefilter() {
    let seed_nfts = vec![SeedNft {
        name: "Moonbird".into(),
        ..Default::default()
    }];
    let snapshot_rows = vec![DatabaseNftRecord {
        contract_address: "0xdup".into(),
        token_id: "1".into(),
        name: "Moonbirds".into(),
        ..Default::default()
    }];

    let rows = build_duplicate_candidates(&seed_nfts, &snapshot_rows, 95.0, 0.55);

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].contract_address, "0xdup");
    assert_eq!(rows[0].match_reasons, vec!["name_match".to_string()]);
}

#[test]
fn duplicate_candidates_score_short_metadata_tokens_with_bm25() {
    let seed_nfts = vec![SeedNft {
        metadata_doc: "cat".into(),
        ..Default::default()
    }];
    let snapshot_rows = vec![DatabaseNftRecord {
        contract_address: "0xdup".into(),
        token_id: "1".into(),
        metadata_doc: "cat".into(),
        ..Default::default()
    }];

    let rows = build_duplicate_candidates(&seed_nfts, &snapshot_rows, 95.0, 0.55);

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].contract_address, "0xdup");
    assert_eq!(rows[0].match_reasons, vec!["metadata_match".to_string()]);
}

#[test]
fn transfer_signals_calculate_fast_spread() {
    let signals = analyze_transfer_signals(&[
        TransferRecord::mint("0xdup", "1", 100, "0xholder1"),
        TransferRecord::transfer("0xdup", "1", 120, "0xholder1", "0xholder2"),
    ]);

    assert_eq!(signals.mint_to_first_transfer_seconds, 20);
    assert!(signals.fast_spread);
}

#[test]
fn transfer_signals_return_zero_when_delay_is_zero_or_invalid() {
    let zero_delay = analyze_transfer_signals(&[
        TransferRecord::mint("0xdup", "1", 100, "0xholder1"),
        TransferRecord::transfer("0xdup", "1", 100, "0xholder1", "0xholder2"),
    ]);
    assert_eq!(zero_delay.mint_to_first_transfer_seconds, 0);
    assert!(!zero_delay.fast_spread);

    let no_mint = analyze_transfer_signals(&[TransferRecord::transfer(
        "0xdup",
        "1",
        120,
        "0xholder1",
        "0xholder2",
    )]);
    assert_eq!(no_mint.mint_to_first_transfer_seconds, 0);
    assert!(!no_mint.fast_spread);
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
    assert!(!rows[0].candidate_open_license);
    assert!(!rows[0].official_or_legit_reissue);
}

#[test]
fn infringing_token_records_fall_back_to_first_transfer_without_mint() {
    let candidates = vec![DuplicateCandidate {
        contract_address: "0xdup".into(),
        token_id: "1".into(),
        match_reasons: vec!["name_match".into()],
        ..Default::default()
    }];
    let transfers = vec![TransferRecord {
        contract_address: "0xdup".into(),
        token_id: "1".into(),
        tx_hash: "0xfallback".into(),
        block_number: 7,
        log_index: 3,
        block_time: 120,
        from_address: "0xholder1".into(),
        to_address: "0xholder2".into(),
        event_type: "transfer".into(),
        source: "test".into(),
    }];

    let rows = build_infringing_token_records("0xdup", &candidates, &transfers);

    assert_eq!(rows[0].mint_tx_hash, "0xfallback");
    assert_eq!(rows[0].mint_block, 7);
    assert_eq!(rows[0].minter_address, "0xholder2");
    assert_eq!(rows[0].first_transfer_time, 120);
    assert!(!rows[0].candidate_open_license);
    assert!(!rows[0].official_or_legit_reissue);
}

#[test]
fn infringing_token_records_ignore_other_contracts_with_same_token_id() {
    let candidates = vec![DuplicateCandidate {
        contract_address: "0xdup".into(),
        token_id: "1".into(),
        match_reasons: vec!["name_match".into()],
        ..Default::default()
    }];
    let transfers = vec![
        TransferRecord {
            contract_address: "0xother".into(),
            token_id: "1".into(),
            tx_hash: "0xothermint".into(),
            block_number: 1,
            log_index: 0,
            block_time: 100,
            from_address: "0x0000000000000000000000000000000000000000".into(),
            to_address: "0xwrongminter".into(),
            event_type: "mint".into(),
            source: "test".into(),
        },
        TransferRecord {
            contract_address: "0xdup".into(),
            token_id: "1".into(),
            tx_hash: "0xrighttransfer".into(),
            block_number: 2,
            log_index: 0,
            block_time: 200,
            from_address: "0xholder1".into(),
            to_address: "0xholder2".into(),
            event_type: "transfer".into(),
            source: "test".into(),
        },
    ];

    let rows = build_infringing_token_records("0xdup", &candidates, &transfers);

    assert_eq!(rows[0].mint_tx_hash, "0xrighttransfer");
    assert_eq!(rows[0].minter_address, "0xholder2");
    assert_eq!(rows[0].first_transfer_time, 200);
    assert!(!rows[0].candidate_open_license);
    assert!(!rows[0].official_or_legit_reissue);
}

#[test]
fn infringing_token_records_use_token_open_license_and_official_reissue_flags() {
    let candidates = vec![DuplicateCandidate {
        contract_address: "0xdup".into(),
        token_id: "1".into(),
        match_reasons: vec!["name_match".into()],
        ..Default::default()
    }];
    let transfers = vec![TransferRecord::mint("0xdup", "1", 100, "0xofficial")];
    let official_addresses = HashSet::from(["0xofficial".to_string()]);
    let candidate_open_license_by_token =
        HashMap::from([(("0xdup".to_string(), "1".to_string()), true)]);

    let rows = build_infringing_token_records_with_context(
        "0xdup",
        &candidates,
        &transfers,
        &official_addresses,
        &candidate_open_license_by_token,
    );

    assert!(rows[0].candidate_open_license);
    assert!(rows[0].official_or_legit_reissue);
}
