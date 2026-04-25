use std::collections::{HashMap, HashSet};

use rayon::prelude::*;

use crate::analysis::scoring::{
    metadata_bm25_tokens, metadata_document_from_json, score_metadata_indexed_pair_with_corpus,
    score_name_pair, MetadataBm25Corpus, MetadataBm25CorpusBuilder, MetadataBm25Document,
};
use crate::models::{DatabaseNftRecord, DuplicateCandidate, SeedNft};
use crate::normalize::{normalize_name, normalize_url};

fn has_name_match(row_name_norm: &str, name_threshold: f64, seed_name_norms: &[String]) -> bool {
    if row_name_norm.is_empty() {
        return false;
    }
    seed_name_norms
        .iter()
        .any(|candidate| score_name_pair(row_name_norm, candidate) >= name_threshold)
}

fn record_metadata_doc(metadata_doc: &str, metadata_json: &str) -> String {
    if !metadata_doc.is_empty() {
        metadata_doc.to_string()
    } else {
        metadata_document_from_json(metadata_json)
    }
}

fn seed_metadata_example_doc(seed_nfts: &[SeedNft]) -> Option<MetadataBm25Document> {
    seed_nfts.iter().find_map(|item| {
        let seed_doc = record_metadata_doc(&item.metadata_doc, &item.metadata_json);
        MetadataBm25Document::from_text(&seed_doc)
    })
}

struct MetadataBm25Index {
    corpus: MetadataBm25Corpus,
    docs: Vec<MetadataBm25Document>,
    candidate_doc_index_by_contract: HashMap<String, usize>,
}

impl MetadataBm25Index {
    fn candidate_doc(&self, contract_address: &str) -> Option<&MetadataBm25Document> {
        self.candidate_doc_index_by_contract
            .get(contract_address)
            .and_then(|index| self.docs.get(*index))
    }
}

struct ContractDuplicateRow {
    contract_address: String,
    representative: DatabaseNftRecord,
    token_uri_match: bool,
    image_uri_match: bool,
    name_norms: HashSet<String>,
    metadata_doc: String,
    metadata_recall_checked: bool,
    metadata_recall_match: bool,
}

impl ContractDuplicateRow {
    fn new(row: &DatabaseNftRecord) -> Self {
        Self {
            contract_address: row.contract_address.clone(),
            representative: row.clone(),
            token_uri_match: false,
            image_uri_match: false,
            name_norms: HashSet::new(),
            metadata_doc: String::new(),
            metadata_recall_checked: false,
            metadata_recall_match: false,
        }
    }

    fn should_score_metadata(&self, has_metadata_recall_flags: bool) -> bool {
        !has_metadata_recall_flags || self.metadata_recall_match
    }
}

fn aggregate_contract_rows(
    seed_contracts: &HashSet<String>,
    seed_token_uri_keys: &HashSet<String>,
    seed_image_uri_keys: &HashSet<String>,
    snapshot_rows: &[DatabaseNftRecord],
) -> Vec<ContractDuplicateRow> {
    let mut rows_by_contract = HashMap::<String, ContractDuplicateRow>::new();
    for row in snapshot_rows {
        let contract_key = row.contract_address.to_lowercase();
        if seed_contracts.contains(&contract_key) {
            continue;
        }

        let entry = rows_by_contract
            .entry(row.contract_address.clone())
            .or_insert_with(|| ContractDuplicateRow::new(row));
        if let Some(token_key) = normalize_url(&row.token_uri) {
            entry.token_uri_match |= seed_token_uri_keys.contains(&token_key);
        }
        if let Some(image_key) = normalize_url(&row.image_uri) {
            entry.image_uri_match |= seed_image_uri_keys.contains(&image_key);
        }
        let row_name_norm = normalize_name(&row.name);
        if !row_name_norm.is_empty() {
            entry.name_norms.insert(row_name_norm);
        }

        entry.metadata_recall_checked |= row.metadata_recall_checked;
        entry.metadata_recall_match |= row.metadata_recall_match;
        let should_update_metadata_doc = entry.metadata_doc.is_empty()
            || (row.metadata_recall_match && !entry.representative.metadata_recall_match);
        if !should_update_metadata_doc {
            continue;
        }

        let metadata_doc = record_metadata_doc(&row.metadata_doc, &row.metadata_json);
        if metadata_doc.is_empty() {
            continue;
        }
        entry.metadata_doc = metadata_doc;
        if row.metadata_recall_match {
            entry.representative = row.clone();
        }
    }
    let mut rows: Vec<_> = rows_by_contract.into_values().collect();
    rows.sort_by(|left, right| left.contract_address.cmp(&right.contract_address));
    rows
}

fn build_metadata_bm25_index(
    seed_metadata_docs: &[MetadataBm25Document],
    contract_rows: &[ContractDuplicateRow],
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
            candidate_doc_index_by_contract: HashMap::new(),
        };
    }

    let mut docs = Vec::new();
    let mut candidate_doc_index_by_contract = HashMap::new();
    let mut corpus_builder = MetadataBm25CorpusBuilder::default();

    for row in contract_rows {
        if !row.should_score_metadata(has_metadata_recall_flags) || row.metadata_doc.is_empty() {
            continue;
        }

        let tokens = metadata_bm25_tokens(&row.metadata_doc);
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
        candidate_doc_index_by_contract.insert(row.contract_address.clone(), doc_index);
        docs.push(indexed_doc);
    }

    MetadataBm25Index {
        corpus: corpus_builder.finish(),
        docs,
        candidate_doc_index_by_contract,
    }
}

pub fn build_duplicate_candidates(
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

    let seed_name_norms: Vec<String> = seed_nfts
        .iter()
        .map(|item| normalize_name(&item.name))
        .filter(|name| !name.is_empty())
        .collect();

    let seed_metadata_docs: Vec<MetadataBm25Document> =
        seed_metadata_example_doc(seed_nfts).into_iter().collect();

    let contract_rows = aggregate_contract_rows(
        &seed_contracts,
        &seed_token_uri_keys,
        &seed_image_uri_keys,
        snapshot_rows,
    );
    let has_metadata_recall_flags = contract_rows.iter().any(|row| row.metadata_recall_checked);
    let metadata_index = build_metadata_bm25_index(
        &seed_metadata_docs,
        &contract_rows,
        has_metadata_recall_flags,
    );

    let mut rows: Vec<DuplicateCandidate> = contract_rows
        .par_iter()
        .filter_map(|row| {
            let mut reasons = Vec::new();
            if row.token_uri_match {
                reasons.push("token_uri_match".to_string());
            }
            if row.image_uri_match {
                reasons.push("image_uri_match".to_string());
            }
            if row
                .name_norms
                .iter()
                .any(|name_norm| has_name_match(name_norm, name_threshold, &seed_name_norms))
            {
                reasons.push("name_match".to_string());
            }
            if row.should_score_metadata(has_metadata_recall_flags) {
                if let Some(row_doc) = metadata_index.candidate_doc(&row.contract_address) {
                    if seed_metadata_docs.iter().any(|seed_doc| {
                        score_metadata_indexed_pair_with_corpus(
                            seed_doc,
                            row_doc,
                            &metadata_index.corpus,
                        ) >= metadata_threshold
                    }) {
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

            Some(DuplicateCandidate {
                contract_address: row.contract_address.clone(),
                token_id: row.representative.token_id.clone(),
                match_reasons: reasons,
                confidence: confidence.to_string(),
                token_uri: row.representative.token_uri.clone(),
                image_uri: row.representative.image_uri.clone(),
                name: row.representative.name.clone(),
                symbol: row.representative.symbol.clone(),
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

    #[test]
    fn metadata_bm25_index_keeps_full_corpus_stats_while_caching_only_query_token_candidates() {
        let seed_docs = vec![MetadataBm25Document::from_text("gold ai dragon").unwrap()];
        let seed_contracts = HashSet::from(["0xseed".to_string()]);
        let snapshot_rows = vec![
            DatabaseNftRecord {
                contract_address: "0xseed".into(),
                token_id: "1".into(),
                metadata_doc: "gold ai dragon".into(),
                ..Default::default()
            },
            DatabaseNftRecord {
                contract_address: "0xgold".into(),
                token_id: "1".into(),
                metadata_doc: "gold rare".into(),
                ..Default::default()
            },
            DatabaseNftRecord {
                contract_address: "0xai".into(),
                token_id: "1".into(),
                metadata_doc: "ai generated".into(),
                ..Default::default()
            },
            DatabaseNftRecord {
                contract_address: "0xmiss".into(),
                token_id: "1".into(),
                metadata_doc: "silver cat".into(),
                ..Default::default()
            },
        ];

        let empty = HashSet::new();
        let contract_rows =
            aggregate_contract_rows(&seed_contracts, &empty, &empty, &snapshot_rows);
        let index = build_metadata_bm25_index(&seed_docs, &contract_rows, false);

        assert!(index.candidate_doc("0xgold").is_some());
        assert!(index.candidate_doc("0xai").is_some());
        assert!(index.candidate_doc("0xseed").is_none());
        assert!(index.candidate_doc("0xmiss").is_none());
        assert_eq!(index.docs.len(), 2);
        assert_eq!(index.corpus.total_docs(), 3);
    }

    #[test]
    fn duplicate_candidates_use_only_one_seed_metadata_example() {
        let seed_nfts = vec![
            SeedNft {
                contract_address: "0xseed".into(),
                token_id: "1".into(),
                metadata_doc: "gold dragon".into(),
                ..Default::default()
            },
            SeedNft {
                contract_address: "0xseed".into(),
                token_id: "2".into(),
                metadata_doc: "silver cat".into(),
                ..Default::default()
            },
        ];
        let snapshot_rows = vec![DatabaseNftRecord {
            contract_address: "0xcandidate".into(),
            token_id: "1".into(),
            metadata_doc: "silver cat".into(),
            ..Default::default()
        }];

        let candidates = build_duplicate_candidates(&seed_nfts, &snapshot_rows, 95.0, 0.55);

        assert!(candidates.is_empty());
    }

    #[test]
    fn duplicate_candidates_skip_metadata_scoring_for_non_metadata_recall_rows() {
        let seed_nfts = vec![SeedNft {
            contract_address: "0xseed".into(),
            token_id: "1".into(),
            metadata_doc: "gold dragon".into(),
            ..Default::default()
        }];
        let snapshot_rows = vec![DatabaseNftRecord {
            contract_address: "0xcandidate".into(),
            token_id: "1".into(),
            metadata_doc: "gold dragon".into(),
            metadata_recall_checked: true,
            metadata_recall_match: false,
            ..Default::default()
        }];

        let candidates = build_duplicate_candidates(&seed_nfts, &snapshot_rows, 95.0, 0.55);

        assert!(candidates.is_empty());
    }

    #[test]
    fn duplicate_candidates_emit_one_candidate_per_contract() {
        let seed_nfts = vec![SeedNft {
            contract_address: "0xseed".into(),
            token_id: "1".into(),
            metadata_doc: "gold dragon".into(),
            ..Default::default()
        }];
        let snapshot_rows = vec![
            DatabaseNftRecord {
                contract_address: "0xcandidate".into(),
                token_id: "1".into(),
                metadata_doc: "gold dragon".into(),
                metadata_recall_checked: true,
                metadata_recall_match: true,
                ..Default::default()
            },
            DatabaseNftRecord {
                contract_address: "0xcandidate".into(),
                token_id: "2".into(),
                metadata_doc: "gold dragon".into(),
                metadata_recall_checked: true,
                metadata_recall_match: true,
                ..Default::default()
            },
        ];

        let candidates = build_duplicate_candidates(&seed_nfts, &snapshot_rows, 95.0, 0.55);

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].contract_address, "0xcandidate");
        assert_eq!(candidates[0].match_reasons, vec!["metadata_match"]);
    }

    #[test]
    fn duplicate_candidates_use_metadata_recall_row_as_representative() {
        let seed_nfts = vec![SeedNft {
            contract_address: "0xseed".into(),
            token_id: "1".into(),
            metadata_doc: "gold dragon".into(),
            ..Default::default()
        }];
        let snapshot_rows = vec![
            DatabaseNftRecord {
                contract_address: "0xcandidate".into(),
                token_id: "1".into(),
                metadata_doc: "silver cat".into(),
                metadata_recall_checked: true,
                metadata_recall_match: false,
                ..Default::default()
            },
            DatabaseNftRecord {
                contract_address: "0xcandidate".into(),
                token_id: "2".into(),
                metadata_doc: "gold dragon".into(),
                metadata_recall_checked: true,
                metadata_recall_match: true,
                ..Default::default()
            },
        ];

        let candidates = build_duplicate_candidates(&seed_nfts, &snapshot_rows, 95.0, 0.55);

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].token_id, "2");
        assert_eq!(candidates[0].match_reasons, vec!["metadata_match"]);
    }
}
