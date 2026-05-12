use std::collections::{BTreeMap, HashMap, HashSet};

use rayon::prelude::*;

use crate::analysis::scoring::{
    metadata_bm25_tokens, metadata_document_from_json, metadata_is_dedup_eligible,
    metadata_prefilter_document_from_json, score_metadata_indexed_pair_with_query, score_name_pair,
    MetadataBm25Corpus, MetadataBm25CorpusBuilder, MetadataBm25Document, MetadataBm25Query,
    MetadataBm25SingleDocumentQuery,
};
use crate::models::{ContractDuplicateRecord, DatabaseNftRecord, DuplicateCandidate, SeedNft};
use crate::normalize::{normalize_name, normalize_url};
use crate::platform_infrastructure::is_platform_infrastructure_contract_blacklisted;

fn has_name_match(row_name_norm: &str, name_threshold: f64, seed_name_norms: &[String]) -> bool {
    if row_name_norm.is_empty() {
        return false;
    }
    seed_name_norms
        .iter()
        .any(|candidate| score_name_pair(row_name_norm, candidate) >= name_threshold)
}

fn build_contract_name_match_index(
    contract_rows: &[&ContractDuplicateRecord],
    name_threshold: f64,
    seed_name_norms: &[String],
) -> Vec<bool> {
    if seed_name_norms.is_empty() || name_threshold > 100.0 {
        return vec![false; contract_rows.len()];
    }

    let mut contract_indices_by_name = HashMap::<String, Vec<usize>>::new();
    for (contract_index, row) in contract_rows.iter().enumerate() {
        for name_norm in &row.name_norms {
            if !name_norm.is_empty() {
                contract_indices_by_name
                    .entry(name_norm.clone())
                    .or_default()
                    .push(contract_index);
            }
        }
    }

    let matching_contract_indices: HashSet<usize> = contract_indices_by_name
        .into_par_iter()
        .filter_map(|(name_norm, contract_indices)| {
            has_name_match(&name_norm, name_threshold, seed_name_norms).then_some(contract_indices)
        })
        .flatten()
        .collect();

    let mut matches = vec![false; contract_rows.len()];
    for index in matching_contract_indices {
        if let Some(value) = matches.get_mut(index) {
            *value = true;
        }
    }
    matches
}

fn record_metadata_text(metadata_json: &str) -> String {
    if !metadata_is_dedup_eligible(metadata_json) {
        return String::new();
    }
    metadata_document_from_json(metadata_json)
}

fn record_metadata_prefilter_text(metadata_json: &str) -> String {
    if !metadata_is_dedup_eligible(metadata_json) {
        return String::new();
    }
    metadata_prefilter_document_from_json(metadata_json)
}

fn record_metadata_text_from_record(row: &DatabaseNftRecord) -> String {
    record_metadata_text(&row.metadata_json)
}

fn record_metadata_prefilter_text_from_record(row: &DatabaseNftRecord) -> String {
    record_metadata_prefilter_text(&row.metadata_json)
}

fn seed_metadata_representative_doc(seed_nfts: &[SeedNft]) -> Option<MetadataBm25Document> {
    seed_nfts.iter().find_map(|item| {
        let seed_text = record_metadata_prefilter_text(&item.metadata_json);
        MetadataBm25Document::from_text(&seed_text)
    })
}

struct MetadataBm25Index {
    corpus: MetadataBm25Corpus,
    docs: Vec<MetadataBm25Document>,
    candidate_doc_indices: Vec<Option<usize>>,
}

impl MetadataBm25Index {
    fn candidate_doc(&self, contract_index: usize) -> Option<&MetadataBm25Document> {
        self.candidate_doc_indices
            .get(contract_index)
            .and_then(|index| *index)
            .and_then(|index| self.docs.get(index))
    }
}

fn should_score_metadata(row: &ContractDuplicateRecord, has_metadata_recall_flags: bool) -> bool {
    !has_metadata_recall_flags || row.metadata_recall_match
}

fn seed_metadata_queries_by_token(
    seed_nfts: &[SeedNft],
) -> BTreeMap<String, MetadataBm25SingleDocumentQuery> {
    let mut docs = BTreeMap::new();
    for item in seed_nfts {
        if item.token_id.trim().is_empty() {
            continue;
        }
        let metadata_text = record_metadata_text(&item.metadata_json);
        let Some(doc) = MetadataBm25Document::from_text(&metadata_text) else {
            continue;
        };
        docs.entry(item.token_id.clone())
            .or_insert_with(|| MetadataBm25SingleDocumentQuery::new(doc));
    }
    docs
}

fn first_overlapping_metadata_match<'a>(
    seed_queries_by_token: &BTreeMap<String, MetadataBm25SingleDocumentQuery>,
    row: &'a ContractDuplicateRecord,
    metadata_threshold: f64,
) -> Option<&'a DatabaseNftRecord> {
    let source_rows = if row.metadata_token_rows.is_empty() {
        std::slice::from_ref(&row.representative)
    } else {
        row.metadata_token_rows.as_slice()
    };
    let mut candidate_rows = Vec::new();
    let mut candidate_docs = Vec::new();
    for candidate_row in source_rows {
        if !seed_queries_by_token.contains_key(&candidate_row.token_id) {
            continue;
        }
        let metadata_text = record_metadata_text_from_record(candidate_row);
        let Some(doc) = MetadataBm25Document::from_text(&metadata_text) else {
            continue;
        };
        candidate_rows.push(candidate_row);
        candidate_docs.push(doc);
    }
    if candidate_docs.is_empty() {
        return None;
    }

    if candidate_docs.len() == 1 {
        let candidate_row = candidate_rows[0];
        let candidate_doc = &candidate_docs[0];
        let seed_query = seed_queries_by_token.get(&candidate_row.token_id)?;
        return (seed_query.has_term_overlap(candidate_doc)
            && seed_query.score(candidate_doc) >= metadata_threshold)
            .then_some(candidate_row);
    }

    let has_any_term_overlap =
        candidate_rows
            .iter()
            .zip(candidate_docs.iter())
            .any(|(candidate_row, candidate_doc)| {
                seed_queries_by_token
                    .get(&candidate_row.token_id)
                    .is_some_and(|seed_query| seed_query.has_term_overlap(candidate_doc))
            });
    if !has_any_term_overlap {
        return None;
    }

    let corpus = MetadataBm25Corpus::from_indexed_documents(&candidate_docs);
    let mut prepared_queries_by_token = BTreeMap::<&str, MetadataBm25Query<'_>>::new();
    for (candidate_row, candidate_doc) in candidate_rows.into_iter().zip(candidate_docs.iter()) {
        if !prepared_queries_by_token.contains_key(candidate_row.token_id.as_str()) {
            if let Some(seed_query) = seed_queries_by_token.get(&candidate_row.token_id) {
                prepared_queries_by_token.insert(
                    candidate_row.token_id.as_str(),
                    MetadataBm25Query::new(seed_query.document(), &corpus),
                );
            }
        }
        let Some(seed_query) = prepared_queries_by_token.get(candidate_row.token_id.as_str())
        else {
            continue;
        };
        if !seed_query.has_term_overlap(candidate_doc) {
            continue;
        }
        if score_metadata_indexed_pair_with_query(seed_query, candidate_doc) >= metadata_threshold {
            return Some(candidate_row);
        }
    }
    None
}

fn new_contract_duplicate_record(row: &DatabaseNftRecord) -> ContractDuplicateRecord {
    let mut record = ContractDuplicateRecord {
        contract_address: row.contract_address.clone(),
        representative: row.clone(),
        ..ContractDuplicateRecord::default()
    };
    push_metadata_token_row(&mut record, row);
    record
}

fn push_metadata_token_row(record: &mut ContractDuplicateRecord, row: &DatabaseNftRecord) {
    if record_metadata_text_from_record(row).is_empty() {
        return;
    }
    if record
        .metadata_token_rows
        .iter()
        .any(|item| item.token_id == row.token_id)
    {
        return;
    }
    record.metadata_token_rows.push(row.clone());
}

fn aggregate_contract_rows(
    chain: &str,
    seed_contracts: &HashSet<String>,
    seed_token_uri_keys: &HashSet<String>,
    seed_image_uri_keys: &HashSet<String>,
    snapshot_rows: &[DatabaseNftRecord],
) -> Vec<ContractDuplicateRecord> {
    let mut rows_by_contract = HashMap::<String, ContractDuplicateRecord>::new();
    for row in snapshot_rows {
        let contract_key = row.contract_address.to_lowercase();
        if seed_contracts.contains(&contract_key) {
            continue;
        }
        if is_platform_infrastructure_contract_blacklisted(chain, &row.contract_address) {
            continue;
        }

        let entry = rows_by_contract
            .entry(row.contract_address.clone())
            .or_insert_with(|| new_contract_duplicate_record(row));
        if let Some(token_key) = normalize_url(&row.token_uri) {
            entry.token_uri_match |= seed_token_uri_keys.contains(&token_key);
        }
        if let Some(image_key) = normalize_url(&row.image_uri) {
            entry.image_uri_match |= seed_image_uri_keys.contains(&image_key);
        }
        let row_name_norm = normalize_name(&row.name);
        if !row_name_norm.is_empty() && !entry.name_norms.contains(&row_name_norm) {
            entry.name_norms.push(row_name_norm);
        }
        push_metadata_token_row(entry, row);

        entry.metadata_recall_checked |= row.metadata_recall_checked;
        entry.metadata_recall_match |= row.metadata_recall_match;
        if !row.metadata_recall_match || entry.representative.metadata_recall_match {
            continue;
        }
        entry.representative = row.clone();
    }
    let mut rows: Vec<_> = rows_by_contract.into_values().collect();
    for row in &mut rows {
        row.metadata_token_rows
            .sort_by(|left, right| left.token_id.cmp(&right.token_id));
    }
    rows.sort_by(|left, right| left.contract_address.cmp(&right.contract_address));
    rows
}

fn build_metadata_bm25_index(
    seed_metadata_docs: &[MetadataBm25Document],
    contract_rows: &[&ContractDuplicateRecord],
    has_metadata_recall_flags: bool,
) -> MetadataBm25Index {
    let query_tokens: HashSet<String> = seed_metadata_docs
        .iter()
        .flat_map(|doc| doc.tokens().iter().cloned())
        .collect();
    if query_tokens.is_empty() {
        return MetadataBm25Index {
            corpus: MetadataBm25Corpus::from_indexed_documents(&[]),
            docs: Vec::new(),
            candidate_doc_indices: vec![None; contract_rows.len()],
        };
    }

    let mut docs = Vec::new();
    let mut candidate_doc_indices = vec![None; contract_rows.len()];
    let mut corpus_builder = MetadataBm25CorpusBuilder::default();

    for (contract_index, row) in contract_rows.iter().enumerate() {
        let row = *row;
        if !should_score_metadata(row, has_metadata_recall_flags) {
            continue;
        }

        let metadata_text = record_metadata_prefilter_text_from_record(&row.representative);
        if metadata_text.is_empty() {
            continue;
        }

        let tokens = metadata_bm25_tokens(&metadata_text);
        corpus_builder.add_tokens(&tokens);

        // Keep corpus statistics over every scoreable representative document, but only
        // cache documents that can actually match the seed metadata query.
        if !tokens.iter().any(|token| query_tokens.contains(token)) {
            continue;
        }

        let Some(indexed_doc) = MetadataBm25Document::from_tokens(tokens) else {
            continue;
        };
        let doc_index = docs.len();
        candidate_doc_indices[contract_index] = Some(doc_index);
        docs.push(indexed_doc);
    }

    MetadataBm25Index {
        corpus: corpus_builder.finish(),
        docs,
        candidate_doc_indices,
    }
}

pub fn build_duplicate_candidates(
    chain: &str,
    seed_nfts: &[SeedNft],
    snapshot_rows: &[DatabaseNftRecord],
    name_threshold: f64,
    metadata_threshold: f64,
) -> Vec<DuplicateCandidate> {
    let seed_contracts: HashSet<String> = seed_nfts
        .iter()
        .map(|item| item.contract_address.to_lowercase())
        .collect();
    let seed_token_uri_keys: HashSet<String> = seed_nfts
        .iter()
        .filter_map(|item| normalize_url(&item.token_uri))
        .collect();
    let seed_image_uri_keys: HashSet<String> = seed_nfts
        .iter()
        .filter_map(|item| normalize_url(&item.image_uri))
        .collect();

    let contract_rows = aggregate_contract_rows(
        chain,
        &seed_contracts,
        &seed_token_uri_keys,
        &seed_image_uri_keys,
        snapshot_rows,
    );
    build_duplicate_candidates_from_contract_rows(
        chain,
        seed_nfts,
        &contract_rows,
        name_threshold,
        metadata_threshold,
    )
}

pub fn build_duplicate_candidates_from_contract_rows(
    chain: &str,
    seed_nfts: &[SeedNft],
    contract_rows: &[ContractDuplicateRecord],
    name_threshold: f64,
    metadata_threshold: f64,
) -> Vec<DuplicateCandidate> {
    let seed_contracts: HashSet<String> = seed_nfts
        .iter()
        .map(|item| item.contract_address.to_lowercase())
        .collect();
    let contract_rows: Vec<&ContractDuplicateRecord> = contract_rows
        .iter()
        .filter(|row| {
            let contract_key = row.contract_address.to_lowercase();
            !seed_contracts.contains(&contract_key)
                && !is_platform_infrastructure_contract_blacklisted(chain, &row.contract_address)
        })
        .collect();

    let seed_name_norms: Vec<String> = seed_nfts
        .iter()
        .map(|item| normalize_name(&item.name))
        .filter(|name| !name.is_empty())
        .collect();

    let seed_metadata_docs: Vec<MetadataBm25Document> = seed_metadata_representative_doc(seed_nfts)
        .into_iter()
        .collect();
    let seed_final_metadata_queries_by_token = seed_metadata_queries_by_token(seed_nfts);

    let name_match_contracts =
        build_contract_name_match_index(&contract_rows, name_threshold, &seed_name_norms);
    let has_metadata_recall_flags = contract_rows.iter().any(|row| row.metadata_recall_checked);
    let metadata_index = build_metadata_bm25_index(
        &seed_metadata_docs,
        &contract_rows,
        has_metadata_recall_flags,
    );
    let metadata_queries = seed_metadata_docs
        .iter()
        .map(|seed_doc| MetadataBm25Query::new(seed_doc, &metadata_index.corpus))
        .collect::<Vec<_>>();

    let mut rows: Vec<DuplicateCandidate> = contract_rows
        .par_iter()
        .enumerate()
        .filter_map(|(contract_index, row)| {
            let row = *row;
            let mut reasons = Vec::new();
            if row.token_uri_match {
                reasons.push("token_uri_match".to_string());
            }
            if row.image_uri_match {
                reasons.push("image_uri_match".to_string());
            }
            if name_match_contracts
                .get(contract_index)
                .copied()
                .unwrap_or(false)
            {
                reasons.push("name_match".to_string());
            }
            let mut metadata_match_row = None;
            if should_score_metadata(row, has_metadata_recall_flags) {
                if let Some(row_doc) = metadata_index.candidate_doc(contract_index) {
                    if metadata_queries.iter().any(|query| {
                        query.has_term_overlap(row_doc)
                            && score_metadata_indexed_pair_with_query(query, row_doc)
                                >= metadata_threshold
                    }) {
                        metadata_match_row = first_overlapping_metadata_match(
                            &seed_final_metadata_queries_by_token,
                            row,
                            metadata_threshold,
                        );
                    }
                    if metadata_match_row.is_some() {
                        reasons.push("metadata_match".to_string());
                    }
                }
            }

            if reasons.is_empty() {
                return None;
            }
            reasons.sort();
            reasons.dedup();
            let has_high_reason = reasons.iter().any(|reason| {
                matches!(
                    reason.as_str(),
                    "token_uri_match" | "image_uri_match" | "metadata_match"
                )
            });
            let confidence = if has_high_reason { "high" } else { "low" };

            let representative = metadata_match_row.unwrap_or(&row.representative);

            Some(DuplicateCandidate {
                contract_address: row.contract_address.clone(),
                token_id: representative.token_id.clone(),
                match_reasons: reasons,
                confidence: confidence.to_string(),
                token_uri: representative.token_uri.clone(),
                image_uri: representative.image_uri.clone(),
                name: representative.name.clone(),
                symbol: representative.symbol.clone(),
            })
        })
        .collect();

    rows.sort_by(|left, right| {
        (&left.contract_address, &left.token_id).cmp(&(&right.contract_address, &right.token_id))
    });
    rows
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metadata_json(text: &str) -> String {
        format!(
            r#"{{"description":{}}}"#,
            serde_json::to_string(text).unwrap()
        )
    }

    #[test]
    fn metadata_bm25_index_keeps_full_corpus_stats_while_caching_only_query_token_candidates() {
        let seed_docs = vec![MetadataBm25Document::from_text("gold ai dragon").unwrap()];
        let seed_contracts = HashSet::from(["0xseed".to_string()]);
        let snapshot_rows = vec![
            DatabaseNftRecord {
                contract_address: "0xseed".into(),
                token_id: "1".into(),
                metadata_json: metadata_json("gold ai dragon"),
                ..Default::default()
            },
            DatabaseNftRecord {
                contract_address: "0xgold".into(),
                token_id: "1".into(),
                metadata_json: metadata_json("gold rare"),
                ..Default::default()
            },
            DatabaseNftRecord {
                contract_address: "0xai".into(),
                token_id: "1".into(),
                metadata_json: metadata_json("ai generated"),
                ..Default::default()
            },
            DatabaseNftRecord {
                contract_address: "0xmiss".into(),
                token_id: "1".into(),
                metadata_json: metadata_json("silver cat"),
                ..Default::default()
            },
        ];

        let empty = HashSet::new();
        let contract_rows =
            aggregate_contract_rows("ethereum", &seed_contracts, &empty, &empty, &snapshot_rows);
        let contract_row_refs: Vec<_> = contract_rows.iter().collect();
        let index = build_metadata_bm25_index(&seed_docs, &contract_row_refs, false);

        let gold_index = contract_rows
            .iter()
            .position(|row| row.contract_address == "0xgold")
            .unwrap();
        let ai_index = contract_rows
            .iter()
            .position(|row| row.contract_address == "0xai")
            .unwrap();
        let miss_index = contract_rows
            .iter()
            .position(|row| row.contract_address == "0xmiss")
            .unwrap();

        assert!(index.candidate_doc(gold_index).is_some());
        assert!(index.candidate_doc(ai_index).is_some());
        assert!(index.candidate_doc(miss_index).is_none());
        assert_eq!(index.docs.len(), 2);
        assert_eq!(index.corpus.total_docs(), 3);
    }

    #[test]
    fn contract_name_match_index_matches_any_normalized_name_per_contract() {
        let contract_rows = vec![
            ContractDuplicateRecord {
                contract_address: "0xhit".into(),
                name_norms: vec!["unrelated".into(), "azuki".into()],
                ..Default::default()
            },
            ContractDuplicateRecord {
                contract_address: "0xmiss".into(),
                name_norms: vec!["doodles".into()],
                ..Default::default()
            },
        ];
        let contract_row_refs = contract_rows.iter().collect::<Vec<_>>();
        let seed_name_norms = vec!["azuki".to_string()];

        let index = build_contract_name_match_index(&contract_row_refs, 100.0, &seed_name_norms);

        assert!(index[0]);
        assert!(!index[1]);
    }

    #[test]
    fn contract_name_match_index_preserves_name_scoring_normalization() {
        let contract_rows = vec![ContractDuplicateRecord {
            contract_address: "0xhit".into(),
            name_norms: vec!["Azuki #123".into()],
            ..Default::default()
        }];
        let contract_row_refs = contract_rows.iter().collect::<Vec<_>>();
        let seed_name_norms = vec!["azuki".to_string()];

        let index = build_contract_name_match_index(&contract_row_refs, 100.0, &seed_name_norms);

        assert!(index[0]);
    }

    #[test]
    fn duplicate_candidates_use_representative_seed_metadata_template_before_token_id_recheck() {
        let seed_nfts = vec![
            SeedNft {
                contract_address: "0xseed".into(),
                token_id: "1".into(),
                metadata_json: metadata_json("gold dragon"),
                ..Default::default()
            },
            SeedNft {
                contract_address: "0xseed".into(),
                token_id: "2".into(),
                metadata_json: metadata_json("silver cat"),
                ..Default::default()
            },
        ];
        let snapshot_rows = vec![DatabaseNftRecord {
            contract_address: "0xcandidate".into(),
            token_id: "2".into(),
            metadata_json: metadata_json("silver cat"),
            ..Default::default()
        }];

        let candidates =
            build_duplicate_candidates("ethereum", &seed_nfts, &snapshot_rows, 95.0, 0.55);

        assert!(candidates.is_empty());
    }

    #[test]
    fn duplicate_candidates_skip_metadata_scoring_for_non_metadata_recall_rows() {
        let seed_nfts = vec![SeedNft {
            contract_address: "0xseed".into(),
            token_id: "1".into(),
            metadata_json: metadata_json("gold dragon"),
            ..Default::default()
        }];
        let snapshot_rows = vec![DatabaseNftRecord {
            contract_address: "0xcandidate".into(),
            token_id: "1".into(),
            metadata_json: metadata_json("gold dragon"),
            metadata_recall_checked: true,
            metadata_recall_match: false,
            ..Default::default()
        }];

        let candidates =
            build_duplicate_candidates("ethereum", &seed_nfts, &snapshot_rows, 95.0, 0.55);

        assert!(candidates.is_empty());
    }

    #[test]
    fn duplicate_candidates_emit_one_candidate_per_contract() {
        let seed_nfts = vec![SeedNft {
            contract_address: "0xseed".into(),
            token_id: "1".into(),
            metadata_json: metadata_json("gold dragon"),
            ..Default::default()
        }];
        let snapshot_rows = vec![
            DatabaseNftRecord {
                contract_address: "0xcandidate".into(),
                token_id: "1".into(),
                metadata_json: metadata_json("gold dragon"),
                metadata_recall_checked: true,
                metadata_recall_match: true,
                ..Default::default()
            },
            DatabaseNftRecord {
                contract_address: "0xcandidate".into(),
                token_id: "2".into(),
                metadata_json: metadata_json("gold dragon"),
                metadata_recall_checked: true,
                metadata_recall_match: true,
                ..Default::default()
            },
        ];

        let candidates =
            build_duplicate_candidates("ethereum", &seed_nfts, &snapshot_rows, 95.0, 0.55);

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].contract_address, "0xcandidate");
        assert_eq!(candidates[0].match_reasons, vec!["metadata_match"]);
    }

    #[test]
    fn duplicate_candidates_recheck_metadata_candidates_by_overlapping_token_id() {
        let seed_nfts = vec![SeedNft {
            contract_address: "0xseed".into(),
            token_id: "2".into(),
            metadata_json: metadata_json("shared collection background red"),
            ..Default::default()
        }];
        let snapshot_rows = vec![
            DatabaseNftRecord {
                contract_address: "0xaccepted".into(),
                token_id: "1".into(),
                metadata_json: metadata_json("shared collection background red"),
                ..Default::default()
            },
            DatabaseNftRecord {
                contract_address: "0xaccepted".into(),
                token_id: "2".into(),
                metadata_json: metadata_json("shared collection background red"),
                ..Default::default()
            },
            DatabaseNftRecord {
                contract_address: "0xrejected".into(),
                token_id: "1".into(),
                metadata_json: metadata_json("shared collection background red"),
                ..Default::default()
            },
            DatabaseNftRecord {
                contract_address: "0xrejected".into(),
                token_id: "2".into(),
                metadata_json: metadata_json("shared collection background blue"),
                ..Default::default()
            },
        ];

        let candidates =
            build_duplicate_candidates("ethereum", &seed_nfts, &snapshot_rows, 95.0, 0.9);

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].contract_address, "0xaccepted");
        assert_eq!(candidates[0].token_id, "2");
        assert_eq!(candidates[0].match_reasons, vec!["metadata_match"]);
    }

    #[test]
    fn duplicate_candidates_exclude_known_platform_infrastructure_contract_addresses_only() {
        let seed_nfts = vec![SeedNft {
            contract_address: "0xseed".into(),
            token_id: "1".into(),
            metadata_json: metadata_json("gold dragon"),
            ..Default::default()
        }];
        let snapshot_rows = vec![
            DatabaseNftRecord {
                contract_address: "0x7C770595a2Be9A87CF49B35eA9bC534f1a59552D".into(),
                token_id: "1".into(),
                metadata_json: metadata_json("gold dragon"),
                metadata_recall_checked: true,
                metadata_recall_match: true,
                ..Default::default()
            },
            DatabaseNftRecord {
                contract_address: "0xfactorynameonly".into(),
                token_id: "1".into(),
                name: "NFT Factory Test".into(),
                metadata_json: metadata_json("gold dragon"),
                metadata_recall_checked: true,
                metadata_recall_match: true,
                ..Default::default()
            },
        ];

        let candidates =
            build_duplicate_candidates("ethereum", &seed_nfts, &snapshot_rows, 95.0, 0.55);

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].contract_address, "0xfactorynameonly");
    }

    #[test]
    fn duplicate_candidates_apply_platform_infrastructure_blacklist_by_chain() {
        let seed_nfts = vec![SeedNft {
            contract_address: "0xseed".into(),
            token_id: "1".into(),
            metadata_json: metadata_json("gold dragon"),
            ..Default::default()
        }];
        let snapshot_rows = vec![DatabaseNftRecord {
            contract_address: "0x7C770595a2Be9A87CF49B35eA9bC534f1a59552D".into(),
            token_id: "1".into(),
            metadata_json: metadata_json("gold dragon"),
            metadata_recall_checked: true,
            metadata_recall_match: true,
            ..Default::default()
        }];

        let ethereum_candidates =
            build_duplicate_candidates("ethereum", &seed_nfts, &snapshot_rows, 95.0, 0.55);
        let polygon_candidates =
            build_duplicate_candidates("polygon", &seed_nfts, &snapshot_rows, 95.0, 0.55);

        assert!(ethereum_candidates.is_empty());
        assert_eq!(polygon_candidates.len(), 1);
    }

    #[test]
    fn duplicate_candidates_use_metadata_recall_row_as_representative() {
        let seed_nfts = vec![SeedNft {
            contract_address: "0xseed".into(),
            token_id: "2".into(),
            metadata_json: metadata_json("gold dragon"),
            ..Default::default()
        }];
        let snapshot_rows = vec![
            DatabaseNftRecord {
                contract_address: "0xcandidate".into(),
                token_id: "1".into(),
                metadata_json: metadata_json("silver cat"),
                metadata_recall_checked: true,
                metadata_recall_match: false,
                ..Default::default()
            },
            DatabaseNftRecord {
                contract_address: "0xcandidate".into(),
                token_id: "2".into(),
                metadata_json: metadata_json("gold dragon"),
                metadata_recall_checked: true,
                metadata_recall_match: true,
                ..Default::default()
            },
        ];

        let candidates =
            build_duplicate_candidates("ethereum", &seed_nfts, &snapshot_rows, 95.0, 0.55);

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].token_id, "2");
        assert_eq!(candidates[0].match_reasons, vec!["metadata_match"]);
    }
}
