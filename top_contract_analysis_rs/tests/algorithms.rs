use std::collections::{BTreeMap, HashMap, HashSet};

use once_cell::sync::Lazy;
use regex::Regex;
use strsim::jaro_winkler;
use top_contract_analysis_rs::analysis::address_records::{
    build_address_attribution_records, build_infringing_token_records,
    build_infringing_token_records_with_context, build_malicious_address_records,
};
use top_contract_analysis_rs::analysis::duplicate::build_duplicate_candidates;
use top_contract_analysis_rs::analysis::lifecycle::{
    build_lifecycle_model_outputs, LifecycleModelInput,
};
use top_contract_analysis_rs::analysis::propagation::{
    build_nft_propagation_path, NftPropagationInput,
};
use top_contract_analysis_rs::analysis::scoring::{
    metadata_document_from_json, score_metadata_documents, score_name_pairs, ScoringError,
};
use top_contract_analysis_rs::analysis::signals::analyze_transfer_signals;
use top_contract_analysis_rs::models::{
    AddressAttributionPayload, AddressEvidencePayload, DatabaseNftRecord, DuplicateCandidate,
    DuplicateContractPayload, HonestAddressPayload, InfringingTokenRecord, NftMarketEventRecord,
    NftPropagationPathPayload, NftSaleRecord, OwnerBalance, SecondarySaleVictimAddressPayload,
    SeedContractPayload, SeedNft, TransferRecord, ValueFlowEdgePayload, ZERO_ADDRESS,
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

    let rows = build_duplicate_candidates("ethereum", &seed_nfts, &snapshot_rows, 95.0, 0.55);

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

    let rows = build_duplicate_candidates("ethereum", &seed_nfts, &snapshot_rows, 95.0, 0.55);

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].contract_address, "0xdup");
    assert_eq!(rows[0].match_reasons, vec!["name_match".to_string()]);
}

#[test]
fn duplicate_candidates_score_short_metadata_tokens_with_bm25() {
    let seed_nfts = vec![SeedNft {
        token_id: "1".into(),
        metadata_json: r#"{"description":"cat"}"#.into(),
        ..Default::default()
    }];
    let snapshot_rows = vec![DatabaseNftRecord {
        contract_address: "0xdup".into(),
        token_id: "1".into(),
        metadata_json: r#"{"description":"cat"}"#.into(),
        ..Default::default()
    }];

    let rows = build_duplicate_candidates("ethereum", &seed_nfts, &snapshot_rows, 95.0, 0.55);

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

    assert_eq!(signals.first_transfer_delay_seconds, 20);
    assert!(signals.fast_spread);
}

#[test]
fn transfer_signals_return_zero_when_delay_is_zero_or_invalid() {
    let zero_delay = analyze_transfer_signals(&[
        TransferRecord::mint("0xdup", "1", 100, "0xholder1"),
        TransferRecord::transfer("0xdup", "1", 100, "0xholder1", "0xholder2"),
    ]);
    assert_eq!(zero_delay.first_transfer_delay_seconds, 0);
    assert!(!zero_delay.fast_spread);

    let no_mint = analyze_transfer_signals(&[TransferRecord::transfer(
        "0xdup",
        "1",
        120,
        "0xholder1",
        "0xholder2",
    )]);
    assert_eq!(no_mint.first_transfer_delay_seconds, 0);
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
    assert_eq!(rows[0].first_transfer_time, 120);
    assert_eq!(
        rows[0].match_reasons,
        vec!["token_uri_match".to_string(), "name_match".to_string()]
    );
    assert!(!rows[0].candidate_open_license);
    assert!(!rows[0].official_or_legit_reissue);
}

#[test]
fn infringing_token_records_do_not_treat_mint_as_first_transfer() {
    let candidates = vec![DuplicateCandidate {
        contract_address: "0xdup".into(),
        token_id: "1".into(),
        match_reasons: vec!["name_match".into()],
        ..Default::default()
    }];
    let transfers = vec![TransferRecord::mint("0xdup", "1", 100, "0xminter")];

    let rows = build_infringing_token_records("0xdup", &candidates, &transfers);

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].minter_address, "0xminter");
    assert_eq!(rows[0].mint_tx_hash, "");
    assert_eq!(rows[0].first_transfer_time, 0);
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

#[test]
fn mint_only_recipient_is_not_marked_malicious_without_other_behavior() {
    let transfers = vec![TransferRecord {
        contract_address: "0xdup".into(),
        token_id: "1".into(),
        tx_hash: "0xmint".into(),
        block_number: 1,
        block_time: 100,
        from_address: ZERO_ADDRESS.into(),
        to_address: "0xpaidminter".into(),
        event_type: "erc721".into(),
        source: "test".into(),
        ..TransferRecord::default()
    }];
    let infringing_tokens = vec![InfringingTokenRecord {
        contract_address: "0xdup".into(),
        token_id: "1".into(),
        minter_address: "0xpaidminter".into(),
        ..InfringingTokenRecord::default()
    }];

    let rows = build_malicious_address_records("0xdup", &transfers, &infringing_tokens, &[]);

    assert!(rows.is_empty());
}

#[test]
fn address_attribution_records_emit_multiscore_labels() {
    let infringing_tokens = vec![InfringingTokenRecord {
        contract_address: "0xdup".into(),
        token_id: "1".into(),
        minter_address: "0xseller".into(),
        mint_tx_hash: "0xmint".into(),
        ..InfringingTokenRecord::default()
    }];
    let sales = vec![NftSaleRecord {
        contract_address: "0xdup".into(),
        token_id: "1".into(),
        tx_hash: "0xsale".into(),
        block_number: 2,
        buyer_address: "0xbuyer".into(),
        seller_address: "0xseller".into(),
        price_eth: Some(1.0),
        price_usd: Some(3000.0),
        is_native_eth: true,
        ..NftSaleRecord::default()
    }];
    let victims = vec![SecondarySaleVictimAddressPayload {
        address: "0xbuyer".into(),
        last_buy_tx_hash: "0xsale".into(),
        is_stuck: true,
        buy_asset_ratio: Some(0.72),
        ..SecondarySaleVictimAddressPayload::default()
    }];
    let honest = vec![HonestAddressPayload {
        contract_address: "0xdup".into(),
        address: "0xbuyer".into(),
        interacted_token_count: 1,
        currently_holding_token_count: 1,
        ..HonestAddressPayload::default()
    }];

    let rows = build_address_attribution_records(
        "0xdup",
        &infringing_tokens,
        &sales,
        &[],
        &[],
        &honest,
        &victims,
    );

    let seller = rows
        .iter()
        .find(|row| row.address == "0xseller")
        .expect("seller attribution");
    assert_eq!(seller.attribution_label, "neutral_participant");
    assert!(seller.neutral_score > seller.operator_score);
    assert!(seller.observed_roles.iter().any(|role| role == "seller"));

    let buyer = rows
        .iter()
        .find(|row| row.address == "0xbuyer")
        .expect("buyer attribution");
    assert_eq!(buyer.attribution_label, "likely_victim");
    assert!(buyer.victim_score > buyer.operator_score);
    assert!(buyer.neutral_score > 0.0);
}

#[test]
fn paid_mint_payment_marks_minter_as_victim_without_operator_evidence() {
    let infringing_tokens = vec![InfringingTokenRecord {
        contract_address: "0xdup".into(),
        token_id: "1".into(),
        minter_address: "0xpaidminter".into(),
        mint_tx_hash: "0xmint".into(),
        mint_block: 10,
        ..InfringingTokenRecord::default()
    }];
    let mint_payment_edges = vec![ValueFlowEdgePayload {
        edge_id: "value:mint_payment:0xmint:0xpaidminter:0xdup".into(),
        contract_address: "0xdup".into(),
        from_address: "0xpaidminter".into(),
        to_address: "0xdup".into(),
        tx_hash: "0xmint".into(),
        block_number: 10,
        block_time: 100,
        token_id: "1".into(),
        value_eth: Some(0.2),
        payment_token_symbol: "ETH".into(),
        payment_token_address: ZERO_ADDRESS.into(),
        channel: "mint_payment".into(),
        from_role: "paid_minter".into(),
        to_role: "mint_contract".into(),
        recipient_known: true,
        evidence_flags: vec!["paid_mint".into()],
        ..ValueFlowEdgePayload::default()
    }];

    let rows = build_address_attribution_records(
        "0xdup",
        &infringing_tokens,
        &[],
        &mint_payment_edges,
        &[],
        &[],
        &[],
    );

    let minter = rows
        .iter()
        .find(|row| row.address == "0xpaidminter")
        .expect("paid minter attribution");
    assert_eq!(minter.attribution_label, "likely_victim");
    assert!(minter.victim_score > minter.operator_score);
    assert!(minter
        .evidence
        .iter()
        .any(|evidence| evidence.evidence_type == "paid_mint_payment"));
}

#[test]
fn propagation_edges_merge_sale_transfer_and_aggregate_mints() {
    let transfers = vec![
        TransferRecord {
            contract_address: "0xdup".into(),
            token_id: "1".into(),
            tx_hash: "0xmint1".into(),
            block_number: 1,
            block_time: 100,
            from_address: ZERO_ADDRESS.into(),
            to_address: "0xseller".into(),
            event_type: "erc721".into(),
            source: "test".into(),
            ..TransferRecord::default()
        },
        TransferRecord {
            contract_address: "0xdup".into(),
            token_id: "2".into(),
            tx_hash: "0xmint2".into(),
            block_number: 1,
            block_time: 101,
            from_address: ZERO_ADDRESS.into(),
            to_address: "0xseller".into(),
            event_type: "erc721".into(),
            source: "test".into(),
            ..TransferRecord::default()
        },
        TransferRecord {
            contract_address: "0xdup".into(),
            token_id: "1".into(),
            tx_hash: "0xsale".into(),
            block_number: 2,
            block_time: 200,
            from_address: "0xseller".into(),
            to_address: "0xbuyer".into(),
            event_type: "erc721".into(),
            source: "test".into(),
            ..TransferRecord::default()
        },
        TransferRecord {
            contract_address: "0xdup".into(),
            token_id: "2".into(),
            tx_hash: "0xmove".into(),
            block_number: 3,
            block_time: 300,
            from_address: "0xseller".into(),
            to_address: "0xholder".into(),
            event_type: "erc721".into(),
            source: "test".into(),
            ..TransferRecord::default()
        },
    ];
    let sales = vec![NftSaleRecord {
        contract_address: "0xdup".into(),
        token_id: "1".into(),
        tx_hash: "0xsale".into(),
        block_number: 2,
        log_index: 7,
        buyer_address: "0xbuyer".into(),
        seller_address: "0xseller".into(),
        marketplace: "opensea".into(),
        price_eth: Some(1.0),
        source: "test".into(),
        ..NftSaleRecord::default()
    }];
    let infringing_tokens = vec![
        InfringingTokenRecord {
            contract_address: "0xdup".into(),
            token_id: "1".into(),
            minter_address: "0xseller".into(),
            ..InfringingTokenRecord::default()
        },
        InfringingTokenRecord {
            contract_address: "0xdup".into(),
            token_id: "2".into(),
            minter_address: "0xseller".into(),
            ..InfringingTokenRecord::default()
        },
    ];

    let path = build_nft_propagation_path(NftPropagationInput {
        contract_address: "0xdup",
        transfers: &transfers,
        sales: &sales,
        owners: &[] as &[OwnerBalance],
        infringing_tokens: &infringing_tokens,
        malicious_addresses: &[],
        honest_addresses: &[],
        secondary_sale_victim_addresses: &[],
    });

    let mint_edge = path
        .edges
        .iter()
        .find(|edge| edge.channel == "mint")
        .unwrap();
    assert_eq!(mint_edge.aggregate_count, 2);
    assert_eq!(
        mint_edge
            .token_ids
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        vec!["1", "2"]
    );
    let sale_edge = path
        .edges
        .iter()
        .find(|edge| edge.channel == "sale")
        .unwrap();
    assert!(sale_edge.merged_transfer);
    assert_eq!(sale_edge.underlying_channels, vec!["sale", "transfer"]);
    assert!(!path
        .edges
        .iter()
        .any(|edge| edge.channel == "transfer" && edge.tx_hash == "0xsale"));
    assert_eq!(path.summary.transfer_edge_count, 1);
}

#[test]
fn lifecycle_model_outputs_expose_research_graph_payloads() {
    let seed_contract = SeedContractPayload {
        chain: "ethereum".into(),
        contract_address: "0xseed".into(),
        name: "Seed".into(),
        symbol: "SEED".into(),
        token_type: "ERC721".into(),
        contract_deployer: "0xdeployer".into(),
        deployed_block_number: 1,
    };
    let candidates = vec![DuplicateCandidate {
        contract_address: "0xdup".into(),
        token_id: "1".into(),
        match_reasons: vec!["token_uri_match".into(), "name_match".into()],
        confidence: "high".into(),
        ..DuplicateCandidate::default()
    }];
    let duplicate_contracts = vec![DuplicateContractPayload {
        contract_address: "0xdup".into(),
        candidate_count: 1,
        match_reasons: vec!["token_uri_match".into()],
        mint_recipients: vec!["0xseller".into()],
        deployed_block_number: 8,
        deployed_block_time: 80,
        ..DuplicateContractPayload::default()
    }];
    let transfers = vec![
        TransferRecord {
            contract_address: "0xdup".into(),
            token_id: "1".into(),
            tx_hash: "0xmint".into(),
            block_number: 10,
            block_time: 100,
            from_address: ZERO_ADDRESS.into(),
            to_address: "0xseller".into(),
            event_type: "erc721".into(),
            source: "test".into(),
            ..TransferRecord::default()
        },
        TransferRecord {
            contract_address: "0xdup".into(),
            token_id: "1".into(),
            tx_hash: "0xsale".into(),
            block_number: 12,
            block_time: 180,
            from_address: "0xseller".into(),
            to_address: "0xbuyer".into(),
            event_type: "erc721".into(),
            source: "test".into(),
            ..TransferRecord::default()
        },
    ];
    let sales = vec![NftSaleRecord {
        contract_address: "0xdup".into(),
        token_id: "1".into(),
        tx_hash: "0xsale".into(),
        block_number: 12,
        log_index: 4,
        buyer_address: "0xbuyer".into(),
        seller_address: "0xseller".into(),
        marketplace: "opensea".into(),
        payment_token_symbol: "ETH".into(),
        price_eth: Some(1.5),
        price_usd: Some(4500.0),
        seller_fee_eth: 1.2,
        seller_fee_usd: 3600.0,
        protocol_fee_eth: 0.1,
        protocol_fee_usd: 300.0,
        royalty_fee_eth: 0.2,
        royalty_fee_usd: 600.0,
        royalty_recipient_address: "0xroyalty".into(),
        is_native_eth: true,
        ..NftSaleRecord::default()
    }];
    let infringing_tokens = vec![InfringingTokenRecord {
        contract_address: "0xdup".into(),
        token_id: "1".into(),
        minter_address: "0xseller".into(),
        mint_tx_hash: "0xmint".into(),
        mint_block: 10,
        first_transfer_time: 180,
        match_reasons: vec!["token_uri_match".into()],
        ..InfringingTokenRecord::default()
    }];
    let victims = vec![SecondarySaleVictimAddressPayload {
        address: "0xbuyer".into(),
        buy_tx_hashes: vec!["0xsale".into()],
        buy_amount_eth: 1.5,
        buy_amount_usd: 4500.0,
        last_buy_tx_hash: "0xsale".into(),
        is_stuck: true,
        ..SecondarySaleVictimAddressPayload::default()
    }];
    let attributions = build_address_attribution_records(
        "0xdup",
        &infringing_tokens,
        &sales,
        &[],
        &[],
        &[],
        &victims,
    );
    let path = build_nft_propagation_path(NftPropagationInput {
        contract_address: "0xdup",
        transfers: &transfers,
        sales: &sales,
        owners: &[] as &[OwnerBalance],
        infringing_tokens: &infringing_tokens,
        malicious_addresses: &[],
        honest_addresses: &[],
        secondary_sale_victim_addresses: &victims,
    });
    let propagation_paths = BTreeMap::from([("0xdup".to_string(), path)]);
    let market_events = vec![
        NftMarketEventRecord {
            contract_address: "0xdup".into(),
            token_id: "1".into(),
            event_type: "order".into(),
            order_type: "listing".into(),
            order_hash: "0xorder".into(),
            event_timestamp: 120,
            actor_address: "0xseller".into(),
            maker_address: "0xseller".into(),
            price_eth: Some(2.0),
            price_usd: Some(6000.0),
            marketplace: "opensea".into(),
            source: "opensea".into(),
            ..NftMarketEventRecord::default()
        },
        NftMarketEventRecord {
            contract_address: "0xdup".into(),
            token_id: "1".into(),
            event_type: "cancel".into(),
            order_type: "listing".into(),
            order_hash: "0xorder".into(),
            event_timestamp: 220,
            actor_address: "0xseller".into(),
            maker_address: "0xseller".into(),
            marketplace: "opensea".into(),
            source: "opensea".into(),
            ..NftMarketEventRecord::default()
        },
    ];
    let mint_payment_edges = vec![
        ValueFlowEdgePayload {
            edge_id: "value:mint_payment:0xmint:0xseller:0xdup".into(),
            contract_address: "0xdup".into(),
            from_address: "0xseller".into(),
            to_address: "0xdup".into(),
            tx_hash: "0xmint".into(),
            block_number: 10,
            block_time: 100,
            token_id: "1".into(),
            value_eth: Some(0.08),
            value_usd: None,
            value_with_gas_eth: None,
            value_with_gas_usd: None,
            from_before_eth_balance: None,
            from_before_usd_balance: None,
            payment_token_symbol: "ETH".into(),
            payment_token_address: ZERO_ADDRESS.into(),
            channel: "mint_payment".into(),
            marketplace: String::new(),
            evidence_type: "same_tx_eth_transfer".into(),
            from_role: "paid_minter".into(),
            to_role: "mint_contract".into(),
            recipient_known: true,
            evidence_flags: vec!["paid_mint".into(), "same_tx_eth_transfer".into()],
        },
        ValueFlowEdgePayload {
            edge_id: "value:funding:0xmint:0xcreator:0xseller".into(),
            contract_address: "0xdup".into(),
            from_address: "0xcreator".into(),
            to_address: "0xseller".into(),
            tx_hash: "0xmint".into(),
            block_number: 10,
            block_time: 100,
            token_id: "1".into(),
            value_eth: Some(0.08),
            value_usd: None,
            value_with_gas_eth: None,
            value_with_gas_usd: None,
            from_before_eth_balance: None,
            from_before_usd_balance: None,
            payment_token_symbol: "ETH".into(),
            payment_token_address: ZERO_ADDRESS.into(),
            channel: "funding".into(),
            marketplace: String::new(),
            evidence_type: "same_tx_eth_transfer:external".into(),
            from_role: "external_funder".into(),
            to_role: "paid_minter".into(),
            recipient_known: true,
            evidence_flags: vec!["same_tx_mint_funding".into()],
        },
        ValueFlowEdgePayload {
            edge_id: "value:withdrawal:0xmint:0xdup:0xcreator".into(),
            contract_address: "0xdup".into(),
            from_address: "0xdup".into(),
            to_address: "0xcreator".into(),
            tx_hash: "0xmint".into(),
            block_number: 10,
            block_time: 100,
            token_id: "1".into(),
            value_eth: Some(0.4),
            value_usd: None,
            value_with_gas_eth: None,
            value_with_gas_usd: None,
            from_before_eth_balance: None,
            from_before_usd_balance: None,
            payment_token_symbol: "ETH".into(),
            payment_token_address: ZERO_ADDRESS.into(),
            channel: "withdrawal".into(),
            marketplace: String::new(),
            evidence_type: "same_tx_contract_outflow:external".into(),
            from_role: "mint_contract".into(),
            to_role: "contract_deployer".into(),
            recipient_known: true,
            evidence_flags: vec!["same_tx_contract_withdrawal".into()],
        },
    ];

    let outputs = build_lifecycle_model_outputs(LifecycleModelInput {
        seed_contract: &seed_contract,
        duplicate_candidates: &candidates,
        duplicate_contracts: &duplicate_contracts,
        address_attributions: &attributions,
        nft_propagation_paths: &propagation_paths,
        mint_payment_edges: &mint_payment_edges,
        market_events: &market_events,
    });

    assert!(outputs
        .contract_lifecycle_events
        .iter()
        .any(|event| event.lifecycle_stage == "replica_mint"));
    assert!(outputs
        .contract_lifecycle_events
        .iter()
        .any(|event| event.lifecycle_stage == "monetization"));
    assert!(outputs
        .contract_lifecycle_events
        .iter()
        .any(|event| event.lifecycle_stage == "market_exposure"));
    assert!(outputs.contract_lifecycle_events.iter().any(|event| {
        event.lifecycle_stage == "primary_monetization"
            && event.event_type == "mint_payment"
            && event.tx_hash == "0xmint"
            && event.value_eth == Some(0.08)
    }));
    assert!(outputs.contract_lifecycle_events.iter().any(|event| {
        event.lifecycle_stage == "copy_preparation"
            && event.event_type == "funding"
            && event.tx_hash == "0xmint"
    }));
    assert!(outputs.contract_lifecycle_events.iter().any(|event| {
        event.lifecycle_stage == "exit_or_cleanup"
            && event.event_type == "withdrawal"
            && event.tx_hash == "0xmint"
    }));
    assert!(outputs
        .contract_lifecycle_events
        .iter()
        .any(|event| event.lifecycle_stage == "exit_or_cleanup"));
    let value_edge = outputs
        .value_flow_edges
        .iter()
        .find(|edge| edge.tx_hash == "0xsale" && edge.channel == "sale_payment")
        .expect("sale value flow edge");
    assert_eq!(value_edge.from_address, "0xbuyer");
    assert_eq!(value_edge.to_address, "0xseller");
    assert_eq!(value_edge.value_usd, Some(3600.0));
    assert_eq!(value_edge.payment_token_symbol, "ETH");
    let royalty_edge = outputs
        .value_flow_edges
        .iter()
        .find(|edge| edge.tx_hash == "0xsale" && edge.channel == "royalty_fee")
        .expect("royalty value flow edge");
    assert_eq!(royalty_edge.from_address, "0xbuyer");
    assert_eq!(royalty_edge.to_address, "0xroyalty");
    assert!(royalty_edge.recipient_known);
    assert_eq!(royalty_edge.value_usd, Some(600.0));
    assert_eq!(royalty_edge.evidence_type, "marketplace_royalty_fee");
    let protocol_edge = outputs
        .value_flow_edges
        .iter()
        .find(|edge| edge.tx_hash == "0xsale" && edge.channel == "protocol_fee")
        .expect("protocol fee value flow edge");
    assert_eq!(protocol_edge.value_usd, Some(300.0));
    let mint_payment_edge = outputs
        .value_flow_edges
        .iter()
        .find(|edge| edge.channel == "mint_payment")
        .expect("mint payment value flow edge");
    assert_eq!(mint_payment_edge.from_address, "0xseller");
    assert_eq!(mint_payment_edge.to_address, "0xdup");
    assert_eq!(mint_payment_edge.value_eth, Some(0.08));
    assert_eq!(outputs.content_similarity_edges.len(), 1);
    let transition = outputs
        .contract_lifecycle_events
        .iter()
        .find(|event| {
            event.lifecycle_stage == "stage_transition"
                && event.event_type == "replica_deployment_to_replica_mint"
        })
        .expect("deployment to mint transition event");
    assert!(transition.detail.contains("evidence_flags"));
    assert!(!outputs.campaign_clusters[0]
        .suspected_operator_addresses
        .contains(&"0xseller".to_string()));
    assert!(outputs.campaign_clusters[0]
        .victim_addresses
        .contains(&"0xbuyer".to_string()));
    assert!(outputs.campaign_clusters[0]
        .contract_addresses
        .contains(&"0xdup".to_string()));
    assert!(!outputs.campaign_clusters[0]
        .contract_addresses
        .contains(&"0xseed".to_string()));
    assert!(!outputs.campaign_clusters[0]
        .lifecycle_stages
        .contains(&"reference_deployment".to_string()));
    assert_eq!(outputs.campaign_clusters[0].first_block_number, 8);
    assert!(outputs
        .lifecycle_metrics
        .iter()
        .all(|metric| metric.contract_address != "0xseed"));
    let metric = outputs
        .lifecycle_metrics
        .iter()
        .find(|metric| metric.contract_address == "0xdup")
        .expect("contract lifecycle metric");
    assert_eq!(metric.time_to_first_listing_seconds, Some(40));
    assert_eq!(metric.time_to_first_sale_seconds, Some(100));
    assert_eq!(metric.first_victim_time, 100);
    assert_eq!(metric.time_to_first_victim_seconds, Some(20));
    assert_eq!(metric.market_event_count, 2);
    assert!((metric.gross_revenue_eth - 1.58).abs() < 1e-9);
    assert!((metric.operator_revenue_eth - 0.08).abs() < 1e-9);
    assert_eq!(metric.marketplace_fee_eth, 0.1);
    assert_eq!(metric.funding_edge_count, 1);
    assert_eq!(metric.withdrawal_edge_count, 1);
    assert_eq!(metric.revenue_backflow_edge_count, 1);
    assert_eq!(
        metric.value_flow_coverage_scope,
        "same_block_native_eth_and_stablecoin_erc20_with_value_constrained_cashout"
    );
    assert!(metric
        .value_flow_coverage_gaps
        .contains(&"later_withdrawals_not_exhaustive".to_string()));
    assert!(metric
        .value_flow_coverage_gaps
        .contains(&"cashout_trace_same_block_value_constrained".to_string()));
    assert!(metric
        .value_flow_coverage_gaps
        .contains(&"known_cex_bridge_mixer_labels_incomplete".to_string()));
    assert_eq!(metric.top_value_recipient_address, "0xdup");
    assert!((metric.top_value_recipient_eth - 0.08).abs() < 1e-9);
    assert!((metric.withdrawal_amount_eth - 0.4).abs() < 1e-9);
    assert!(!metric.early_detection_positive);
    assert!(outputs.weak_supervision_labels.iter().any(|label| {
        label.entity_type == "contract"
            && label.contract_address == "0xdup"
            && label.label == "probable_infringement_campaign"
    }));
    assert!(outputs.early_detection_features.iter().any(|row| {
        row.contract_address == "0xdup"
            && row.observation_window_seconds == 60
            && row.weak_label == "positive_observed_sale_or_victimization"
    }));
}

#[test]
fn lifecycle_metrics_handle_proxy_admin_and_usd_only_top_recipient() {
    let seed_contract = SeedContractPayload {
        chain: "ethereum".into(),
        contract_address: "0xseed".into(),
        token_type: "ERC721".into(),
        ..SeedContractPayload::default()
    };
    let duplicate_contracts = vec![DuplicateContractPayload {
        contract_address: "0xdup".into(),
        candidate_count: 1,
        match_reasons: vec!["metadata_match".into()],
        proxy_admin_address: "0xproxy".into(),
        ..DuplicateContractPayload::default()
    }];
    let candidates = vec![DuplicateCandidate {
        contract_address: "0xdup".into(),
        token_id: "1".into(),
        match_reasons: vec!["metadata_match".into()],
        confidence: "high".into(),
        ..DuplicateCandidate::default()
    }];
    let propagation_paths = BTreeMap::from([(
        "0xdup".to_string(),
        NftPropagationPathPayload {
            contract_address: "0xdup".into(),
            summary: Default::default(),
            ..NftPropagationPathPayload::default()
        },
    )]);
    let value_edges = vec![
        ValueFlowEdgePayload {
            edge_id: "value:mint_payment:0xmint:0xvictim:0xproxy".into(),
            contract_address: "0xdup".into(),
            from_address: "0xvictim".into(),
            to_address: "0xproxy".into(),
            tx_hash: "0xmint".into(),
            block_number: 10,
            block_time: 100,
            token_id: "1".into(),
            value_usd: Some(100.0),
            payment_token_symbol: "USDC".into(),
            channel: "mint_payment".into(),
            from_role: "paid_minter".into(),
            to_role: "proxy_admin".into(),
            recipient_known: true,
            evidence_flags: vec!["paid_mint".into()],
            ..ValueFlowEdgePayload::default()
        },
        ValueFlowEdgePayload {
            edge_id: "value:withdrawal:0xwithdraw:0xdup:0xproxy".into(),
            contract_address: "0xdup".into(),
            from_address: "0xdup".into(),
            to_address: "0xproxy".into(),
            tx_hash: "0xwithdraw".into(),
            block_number: 11,
            block_time: 120,
            value_usd: Some(100.0),
            payment_token_symbol: "USDC".into(),
            channel: "withdrawal".into(),
            from_role: "mint_contract".into(),
            to_role: "proxy_admin".into(),
            recipient_known: true,
            ..ValueFlowEdgePayload::default()
        },
    ];

    let outputs = build_lifecycle_model_outputs(LifecycleModelInput {
        seed_contract: &seed_contract,
        duplicate_candidates: &candidates,
        duplicate_contracts: &duplicate_contracts,
        address_attributions: &[],
        nft_propagation_paths: &propagation_paths,
        mint_payment_edges: &value_edges,
        market_events: &[],
    });

    let metric = outputs
        .lifecycle_metrics
        .iter()
        .find(|metric| metric.contract_address == "0xdup")
        .expect("contract lifecycle metric");
    assert_eq!(metric.operator_revenue_usd, 100.0);
    assert_eq!(metric.gross_revenue_usd, 100.0);
    assert_eq!(metric.top_value_recipient_address, "0xproxy");
    assert_eq!(metric.top_value_recipient_usd, 100.0);
    assert_eq!(metric.withdrawal_amount_usd, 100.0);
}

#[test]
fn early_detection_features_do_not_treat_undated_sales_as_window_observed() {
    let seed_contract = SeedContractPayload {
        chain: "ethereum".into(),
        contract_address: "0xseed".into(),
        token_type: "ERC721".into(),
        ..SeedContractPayload::default()
    };
    let duplicate_contracts = vec![DuplicateContractPayload {
        contract_address: "0xdup".into(),
        candidate_count: 1,
        match_reasons: vec!["token_uri_match".into()],
        deployed_block_number: 9,
        deployed_block_time: 90,
        ..DuplicateContractPayload::default()
    }];
    let candidates = vec![DuplicateCandidate {
        contract_address: "0xdup".into(),
        token_id: "1".into(),
        match_reasons: vec!["token_uri_match".into()],
        confidence: "high".into(),
        ..DuplicateCandidate::default()
    }];
    let path = build_nft_propagation_path(NftPropagationInput {
        contract_address: "0xdup",
        transfers: &[TransferRecord {
            contract_address: "0xdup".into(),
            token_id: "1".into(),
            tx_hash: "0xmint".into(),
            block_number: 10,
            block_time: 100,
            from_address: ZERO_ADDRESS.into(),
            to_address: "0xseller".into(),
            ..TransferRecord::default()
        }],
        sales: &[NftSaleRecord {
            contract_address: "0xdup".into(),
            token_id: "1".into(),
            tx_hash: "0xsale_without_transfer_time".into(),
            block_number: 12,
            buyer_address: "0xbuyer".into(),
            seller_address: "0xseller".into(),
            price_eth: Some(1.0),
            ..NftSaleRecord::default()
        }],
        owners: &[] as &[OwnerBalance],
        infringing_tokens: &[InfringingTokenRecord {
            contract_address: "0xdup".into(),
            token_id: "1".into(),
            minter_address: "0xseller".into(),
            mint_tx_hash: "0xmint".into(),
            mint_block: 10,
            ..InfringingTokenRecord::default()
        }],
        malicious_addresses: &[],
        honest_addresses: &[],
        secondary_sale_victim_addresses: &[],
    });

    let unknown_time_value_edges = vec![ValueFlowEdgePayload {
        edge_id: "value:funding:0xunknown".into(),
        contract_address: "0xdup".into(),
        from_address: "0xfunder".into(),
        to_address: "0xseller".into(),
        tx_hash: "0xunknown".into(),
        block_number: 12,
        block_time: 0,
        token_id: "1".into(),
        value_eth: Some(0.5),
        channel: "funding".into(),
        recipient_known: true,
        ..ValueFlowEdgePayload::default()
    }];

    let outputs = build_lifecycle_model_outputs(LifecycleModelInput {
        seed_contract: &seed_contract,
        duplicate_candidates: &candidates,
        duplicate_contracts: &duplicate_contracts,
        address_attributions: &[],
        nft_propagation_paths: &BTreeMap::from([("0xdup".to_string(), path)]),
        mint_payment_edges: &unknown_time_value_edges,
        market_events: &[],
    });

    let metric = outputs
        .lifecycle_metrics
        .iter()
        .find(|metric| metric.contract_address == "0xdup")
        .expect("contract lifecycle metric");
    assert_eq!(metric.first_sale_time, 0);
    assert_eq!(metric.sale_count, 1);
    assert!(!metric.early_detection_positive);
    assert!(!outputs.weak_supervision_labels.iter().any(|label| {
        label.contract_address == "0xdup" && label.label == "early_detection_positive"
    }));
    assert!(outputs.early_detection_features.iter().all(|row| {
        row.contract_address != "0xdup"
            || row.weak_label != "positive_observed_sale_or_victimization"
    }));
    let first_window = outputs
        .early_detection_features
        .iter()
        .find(|row| row.contract_address == "0xdup" && row.observation_window_seconds == 60)
        .expect("first early detection window");
    assert_eq!(first_window.sale_event_count, 0);
    assert_eq!(first_window.value_flow_count, 0);
    assert_eq!(first_window.funding_edge_count, 0);
}

#[test]
fn campaign_clusters_use_funding_source_not_paid_minter_as_shared_evidence() {
    let seed_contract = SeedContractPayload {
        chain: "ethereum".into(),
        contract_address: "0xseed".into(),
        token_type: "ERC721".into(),
        ..SeedContractPayload::default()
    };
    let duplicate_contracts = vec![
        DuplicateContractPayload {
            contract_address: "0xdup1".into(),
            candidate_count: 1,
            ..DuplicateContractPayload::default()
        },
        DuplicateContractPayload {
            contract_address: "0xdup2".into(),
            candidate_count: 1,
            ..DuplicateContractPayload::default()
        },
    ];
    let candidates = vec![
        DuplicateCandidate {
            contract_address: "0xdup1".into(),
            token_id: "1".into(),
            match_reasons: vec!["token_uri_match".into()],
            confidence: "high".into(),
            ..DuplicateCandidate::default()
        },
        DuplicateCandidate {
            contract_address: "0xdup2".into(),
            token_id: "1".into(),
            match_reasons: vec!["token_uri_match".into()],
            confidence: "high".into(),
            ..DuplicateCandidate::default()
        },
    ];
    let same_minter_edges = vec![
        ValueFlowEdgePayload {
            edge_id: "value:funding:0x1:0xfunder1:0xsharedminter".into(),
            contract_address: "0xdup1".into(),
            from_address: "0xfunder1".into(),
            to_address: "0xsharedminter".into(),
            channel: "funding".into(),
            recipient_known: true,
            ..ValueFlowEdgePayload::default()
        },
        ValueFlowEdgePayload {
            edge_id: "value:funding:0x2:0xfunder2:0xsharedminter".into(),
            contract_address: "0xdup2".into(),
            from_address: "0xfunder2".into(),
            to_address: "0xsharedminter".into(),
            channel: "funding".into(),
            recipient_known: true,
            ..ValueFlowEdgePayload::default()
        },
    ];
    let shared_funder_edges = vec![
        ValueFlowEdgePayload {
            edge_id: "value:funding:0x3:0xsharedfunder:0xminter1".into(),
            contract_address: "0xdup1".into(),
            from_address: "0xsharedfunder".into(),
            to_address: "0xminter1".into(),
            channel: "funding".into(),
            recipient_known: true,
            ..ValueFlowEdgePayload::default()
        },
        ValueFlowEdgePayload {
            edge_id: "value:funding:0x4:0xsharedfunder:0xminter2".into(),
            contract_address: "0xdup2".into(),
            from_address: "0xsharedfunder".into(),
            to_address: "0xminter2".into(),
            channel: "funding".into(),
            recipient_known: true,
            ..ValueFlowEdgePayload::default()
        },
    ];

    let same_minter_outputs = build_lifecycle_model_outputs(LifecycleModelInput {
        seed_contract: &seed_contract,
        duplicate_candidates: &candidates,
        duplicate_contracts: &duplicate_contracts,
        address_attributions: &[],
        nft_propagation_paths: &BTreeMap::new(),
        mint_payment_edges: &same_minter_edges,
        market_events: &[],
    });
    assert_eq!(same_minter_outputs.campaign_clusters.len(), 2);

    let shared_funder_outputs = build_lifecycle_model_outputs(LifecycleModelInput {
        seed_contract: &seed_contract,
        duplicate_candidates: &candidates,
        duplicate_contracts: &duplicate_contracts,
        address_attributions: &[],
        nft_propagation_paths: &BTreeMap::new(),
        mint_payment_edges: &shared_funder_edges,
        market_events: &[],
    });
    assert_eq!(shared_funder_outputs.campaign_clusters.len(), 1);
    assert!(shared_funder_outputs.campaign_clusters[0]
        .shared_evidence
        .iter()
        .any(|item| item == "shared_funding_source:0xsharedfunder"));
}

#[test]
fn campaign_clusters_do_not_use_aggregation_attribution_as_shared_operator_evidence() {
    let seed_contract = SeedContractPayload {
        chain: "ethereum".into(),
        contract_address: "0xseed".into(),
        token_type: "ERC721".into(),
        ..SeedContractPayload::default()
    };
    let duplicate_contracts = vec![
        DuplicateContractPayload {
            contract_address: "0xdup1".into(),
            candidate_count: 1,
            ..DuplicateContractPayload::default()
        },
        DuplicateContractPayload {
            contract_address: "0xdup2".into(),
            candidate_count: 1,
            ..DuplicateContractPayload::default()
        },
    ];
    let candidates = vec![
        DuplicateCandidate {
            contract_address: "0xdup1".into(),
            token_id: "1".into(),
            match_reasons: vec!["token_uri_match".into()],
            confidence: "high".into(),
            ..DuplicateCandidate::default()
        },
        DuplicateCandidate {
            contract_address: "0xdup2".into(),
            token_id: "1".into(),
            match_reasons: vec!["token_uri_match".into()],
            confidence: "high".into(),
            ..DuplicateCandidate::default()
        },
    ];
    let address_attributions = vec![
        AddressAttributionPayload {
            contract_address: "0xdup1".into(),
            address: "0xcollector".into(),
            attribution_label: "suspected_operator".into(),
            operator_score: 0.35,
            attacker_score: 0.35,
            evidence: vec![AddressEvidencePayload {
                evidence_type: "nft_aggregation".into(),
                contract_address: "0xdup1".into(),
                weight: 0.35,
                ..AddressEvidencePayload::default()
            }],
            ..AddressAttributionPayload::default()
        },
        AddressAttributionPayload {
            contract_address: "0xdup2".into(),
            address: "0xcollector".into(),
            attribution_label: "suspected_operator".into(),
            operator_score: 0.35,
            attacker_score: 0.35,
            evidence: vec![AddressEvidencePayload {
                evidence_type: "nft_aggregation".into(),
                contract_address: "0xdup2".into(),
                weight: 0.35,
                ..AddressEvidencePayload::default()
            }],
            ..AddressAttributionPayload::default()
        },
    ];

    let outputs = build_lifecycle_model_outputs(LifecycleModelInput {
        seed_contract: &seed_contract,
        duplicate_candidates: &candidates,
        duplicate_contracts: &duplicate_contracts,
        address_attributions: &address_attributions,
        nft_propagation_paths: &BTreeMap::new(),
        mint_payment_edges: &[],
        market_events: &[],
    });

    assert_eq!(outputs.campaign_clusters.len(), 2);
    assert!(outputs.campaign_clusters.iter().all(|cluster| {
        !cluster
            .shared_evidence
            .iter()
            .any(|item| item == "shared_address:suspected_operator:0xcollector")
    }));
}

#[test]
fn campaign_clusters_do_not_merge_unrelated_contracts_without_shared_evidence() {
    let seed_contract = SeedContractPayload {
        chain: "ethereum".into(),
        contract_address: "0xseed".into(),
        name: "Seed".into(),
        symbol: "SEED".into(),
        token_type: "ERC721".into(),
        contract_deployer: "0xseeddeployer".into(),
        deployed_block_number: 1,
    };
    let duplicate_contracts = vec![
        DuplicateContractPayload {
            contract_address: "0xdup1".into(),
            contract_deployer: "0xdeployer1".into(),
            deployed_block_number: 10,
            candidate_count: 1,
            ..DuplicateContractPayload::default()
        },
        DuplicateContractPayload {
            contract_address: "0xdup2".into(),
            contract_deployer: "0xdeployer2".into(),
            deployed_block_number: 20,
            candidate_count: 1,
            ..DuplicateContractPayload::default()
        },
    ];
    let candidates = vec![
        DuplicateCandidate {
            contract_address: "0xdup1".into(),
            token_id: "1".into(),
            match_reasons: vec!["token_uri_match".into()],
            confidence: "high".into(),
            ..DuplicateCandidate::default()
        },
        DuplicateCandidate {
            contract_address: "0xdup2".into(),
            token_id: "1".into(),
            match_reasons: vec!["name_match".into()],
            confidence: "medium".into(),
            ..DuplicateCandidate::default()
        },
    ];

    let outputs = build_lifecycle_model_outputs(LifecycleModelInput {
        seed_contract: &seed_contract,
        duplicate_candidates: &candidates,
        duplicate_contracts: &duplicate_contracts,
        address_attributions: &[],
        nft_propagation_paths: &BTreeMap::new(),
        mint_payment_edges: &[],
        market_events: &[],
    });

    assert_eq!(outputs.campaign_clusters.len(), 2);
    assert!(!outputs.campaign_clusters.iter().any(|cluster| {
        cluster.contract_addresses.contains(&"0xdup1".to_string())
            && cluster.contract_addresses.contains(&"0xdup2".to_string())
    }));
}

#[test]
fn campaign_clusters_do_not_merge_contracts_only_because_they_share_sale_seller() {
    let seed_contract = SeedContractPayload {
        chain: "ethereum".into(),
        contract_address: "0xseed".into(),
        name: "Seed".into(),
        symbol: "SEED".into(),
        token_type: "ERC721".into(),
        contract_deployer: "0xseeddeployer".into(),
        deployed_block_number: 1,
    };
    let duplicate_contracts = vec![
        DuplicateContractPayload {
            contract_address: "0xdup1".into(),
            contract_deployer: "0xdeployer1".into(),
            deployed_block_number: 10,
            candidate_count: 1,
            ..DuplicateContractPayload::default()
        },
        DuplicateContractPayload {
            contract_address: "0xdup2".into(),
            contract_deployer: "0xdeployer2".into(),
            deployed_block_number: 20,
            candidate_count: 1,
            ..DuplicateContractPayload::default()
        },
    ];
    let candidates = vec![
        DuplicateCandidate {
            contract_address: "0xdup1".into(),
            token_id: "1".into(),
            match_reasons: vec!["token_uri_match".into()],
            confidence: "high".into(),
            ..DuplicateCandidate::default()
        },
        DuplicateCandidate {
            contract_address: "0xdup2".into(),
            token_id: "1".into(),
            match_reasons: vec!["token_uri_match".into()],
            confidence: "high".into(),
            ..DuplicateCandidate::default()
        },
    ];
    let mut propagation_paths = BTreeMap::new();
    let mut attributions = Vec::new();
    for (contract, buyer, block) in [
        ("0xdup1", "0xbuyer1", 10_i64),
        ("0xdup2", "0xbuyer2", 20_i64),
    ] {
        let transfers = vec![
            TransferRecord {
                contract_address: contract.into(),
                token_id: "1".into(),
                tx_hash: format!("0xmint{contract}"),
                block_number: block,
                block_time: block * 10,
                from_address: ZERO_ADDRESS.into(),
                to_address: "0xsharedseller".into(),
                event_type: "erc721".into(),
                source: "test".into(),
                ..TransferRecord::default()
            },
            TransferRecord {
                contract_address: contract.into(),
                token_id: "1".into(),
                tx_hash: format!("0xsale{contract}"),
                block_number: block + 1,
                block_time: block * 10 + 5,
                from_address: "0xsharedseller".into(),
                to_address: buyer.into(),
                event_type: "erc721".into(),
                source: "test".into(),
                ..TransferRecord::default()
            },
        ];
        let sales = vec![NftSaleRecord {
            contract_address: contract.into(),
            token_id: "1".into(),
            tx_hash: format!("0xsale{contract}"),
            block_number: block + 1,
            buyer_address: buyer.into(),
            seller_address: "0xsharedseller".into(),
            price_eth: Some(1.0),
            seller_fee_eth: 1.0,
            is_native_eth: true,
            ..NftSaleRecord::default()
        }];
        let tokens = vec![InfringingTokenRecord {
            contract_address: contract.into(),
            token_id: "1".into(),
            minter_address: "0xsharedseller".into(),
            mint_tx_hash: format!("0xmint{contract}"),
            mint_block: block,
            match_reasons: vec!["token_uri_match".into()],
            ..InfringingTokenRecord::default()
        }];
        let victims = vec![SecondarySaleVictimAddressPayload {
            address: buyer.into(),
            buy_tx_hashes: vec![format!("0xsale{contract}")],
            buy_amount_eth: 1.0,
            last_buy_tx_hash: format!("0xsale{contract}"),
            is_stuck: true,
            ..SecondarySaleVictimAddressPayload::default()
        }];
        attributions.extend(build_address_attribution_records(
            contract,
            &tokens,
            &sales,
            &[],
            &[],
            &[],
            &victims,
        ));
        propagation_paths.insert(
            contract.to_string(),
            build_nft_propagation_path(NftPropagationInput {
                contract_address: contract,
                transfers: &transfers,
                sales: &sales,
                owners: &[] as &[OwnerBalance],
                infringing_tokens: &tokens,
                malicious_addresses: &[],
                honest_addresses: &[],
                secondary_sale_victim_addresses: &victims,
            }),
        );
    }

    let outputs = build_lifecycle_model_outputs(LifecycleModelInput {
        seed_contract: &seed_contract,
        duplicate_candidates: &candidates,
        duplicate_contracts: &duplicate_contracts,
        address_attributions: &attributions,
        nft_propagation_paths: &propagation_paths,
        mint_payment_edges: &[],
        market_events: &[],
    });

    assert_eq!(outputs.campaign_clusters.len(), 2);
}

#[test]
fn lifecycle_stage_transitions_do_not_move_backward_in_time() {
    let seed_contract = SeedContractPayload {
        chain: "ethereum".into(),
        contract_address: "0xseed".into(),
        name: "Seed".into(),
        symbol: "SEED".into(),
        token_type: "ERC721".into(),
        contract_deployer: "0xseeddeployer".into(),
        deployed_block_number: 1,
    };
    let candidates = vec![DuplicateCandidate {
        contract_address: "0xdup".into(),
        token_id: "1".into(),
        match_reasons: vec!["token_uri_match".into()],
        confidence: "high".into(),
        ..DuplicateCandidate::default()
    }];
    let duplicate_contracts = vec![DuplicateContractPayload {
        contract_address: "0xdup".into(),
        contract_deployer: "0xdeployer".into(),
        deployed_block_number: 5,
        candidate_count: 1,
        ..DuplicateContractPayload::default()
    }];
    let transfers = vec![TransferRecord {
        contract_address: "0xdup".into(),
        token_id: "1".into(),
        tx_hash: "0xmint".into(),
        block_number: 20,
        block_time: 200,
        from_address: ZERO_ADDRESS.into(),
        to_address: "0xminter".into(),
        event_type: "erc721".into(),
        source: "test".into(),
        ..TransferRecord::default()
    }];
    let tokens = vec![InfringingTokenRecord {
        contract_address: "0xdup".into(),
        token_id: "1".into(),
        minter_address: "0xminter".into(),
        mint_tx_hash: "0xmint".into(),
        mint_block: 20,
        match_reasons: vec!["token_uri_match".into()],
        ..InfringingTokenRecord::default()
    }];
    let propagation_paths = BTreeMap::from([(
        "0xdup".to_string(),
        build_nft_propagation_path(NftPropagationInput {
            contract_address: "0xdup",
            transfers: &transfers,
            sales: &[],
            owners: &[] as &[OwnerBalance],
            infringing_tokens: &tokens,
            malicious_addresses: &[],
            honest_addresses: &[],
            secondary_sale_victim_addresses: &[],
        }),
    )]);
    let market_events = vec![NftMarketEventRecord {
        contract_address: "0xdup".into(),
        token_id: "1".into(),
        event_type: "order".into(),
        order_type: "listing".into(),
        event_timestamp: 100,
        block_number: 10,
        block_time: 100,
        actor_address: "0xminter".into(),
        marketplace: "opensea".into(),
        source: "opensea".into(),
        ..NftMarketEventRecord::default()
    }];

    let outputs = build_lifecycle_model_outputs(LifecycleModelInput {
        seed_contract: &seed_contract,
        duplicate_candidates: &candidates,
        duplicate_contracts: &duplicate_contracts,
        address_attributions: &[],
        nft_propagation_paths: &propagation_paths,
        mint_payment_edges: &[],
        market_events: &market_events,
    });

    assert!(!outputs.contract_lifecycle_events.iter().any(|event| {
        event.lifecycle_stage == "stage_transition"
            && event.event_type == "replica_mint_to_market_exposure"
    }));
}
