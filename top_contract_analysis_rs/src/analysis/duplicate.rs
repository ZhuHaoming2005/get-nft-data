use std::collections::{HashMap, HashSet};

use rayon::prelude::*;

use crate::analysis::scoring::{
    metadata_bm25_tokens, metadata_document_from_json, score_metadata_indexed_pair_with_corpus,
    score_name_pair, MetadataBm25Corpus, MetadataBm25CorpusBuilder, MetadataBm25Document,
};
use crate::models::{DatabaseNftRecord, DuplicateCandidate, SeedNft};
use crate::normalize::{normalize_name, normalize_symbol, normalize_text, normalize_url};

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
        normalize_text(metadata_doc)
    } else {
        metadata_document_from_json(metadata_json)
    }
}

struct MetadataBm25Index {
    corpus: MetadataBm25Corpus,
    docs: Vec<MetadataBm25Document>,
    candidate_doc_index_by_key: HashMap<(String, String), usize>,
}

impl MetadataBm25Index {
    fn candidate_doc(&self, key: &(String, String)) -> Option<&MetadataBm25Document> {
        self.candidate_doc_index_by_key
            .get(key)
            .and_then(|index| self.docs.get(*index))
    }
}

fn build_metadata_bm25_index(
    seed_metadata_docs: &[MetadataBm25Document],
    snapshot_rows: &[DatabaseNftRecord],
    seed_contracts: &HashSet<String>,
) -> MetadataBm25Index {
    let query_tokens: HashSet<String> = seed_metadata_docs
        .iter()
        .flat_map(|doc| doc.tokens().iter().cloned())
        .collect();
    if query_tokens.is_empty() {
        return MetadataBm25Index {
            corpus: MetadataBm25Corpus::from_indexed_documents(&[]),
            docs: Vec::new(),
            candidate_doc_index_by_key: HashMap::new(),
        };
    }

    let mut docs = Vec::new();
    let mut candidate_doc_index_by_key = HashMap::new();
    let mut corpus_builder = MetadataBm25CorpusBuilder::default();

    for row in snapshot_rows {
        if seed_contracts.contains(&row.contract_address.to_lowercase()) {
            continue;
        }

        let doc = record_metadata_doc(&row.metadata_doc, &row.metadata_json);
        if doc.is_empty() {
            continue;
        }

        let tokens = metadata_bm25_tokens(&doc);
        corpus_builder.add_tokens(&tokens);
        if !tokens.iter().any(|token| query_tokens.contains(token)) {
            continue;
        }

        let Some(indexed_doc) = MetadataBm25Document::from_tokens(tokens) else {
            continue;
        };
        let doc_index = docs.len();
        candidate_doc_index_by_key.insert(
            (row.contract_address.clone(), row.token_id.clone()),
            doc_index,
        );
        docs.push(indexed_doc);
    }

    MetadataBm25Index {
        corpus: corpus_builder.finish(),
        docs,
        candidate_doc_index_by_key,
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
    let seed_symbol_norms: HashSet<String> = seed_nfts
        .iter()
        .map(|item| normalize_symbol(&item.symbol))
        .filter(|symbol| !symbol.is_empty())
        .collect();

    let seed_name_norms: Vec<String> = seed_nfts
        .iter()
        .map(|item| normalize_name(&item.name))
        .filter(|name| !name.is_empty())
        .collect();

    let seed_metadata_docs: Vec<MetadataBm25Document> = seed_nfts
        .iter()
        .filter_map(|item| {
            let seed_doc = record_metadata_doc(&item.metadata_doc, &item.metadata_json);
            MetadataBm25Document::from_text(&seed_doc)
        })
        .collect();

    let metadata_index =
        build_metadata_bm25_index(&seed_metadata_docs, snapshot_rows, &seed_contracts);

    let mut rows: Vec<DuplicateCandidate> = snapshot_rows
        .par_iter()
        .filter_map(|row| {
            if seed_contracts.contains(&row.contract_address.to_lowercase()) {
                return None;
            }

            let token_key = normalize_url(&row.token_uri);
            let image_key = normalize_url(&row.image_uri);
            let symbol_norm = normalize_symbol(&row.symbol);
            let row_name_norm = normalize_name(&row.name);

            let mut reasons = Vec::new();
            if token_key
                .as_ref()
                .map(|value| seed_token_uri_keys.contains(value))
                .unwrap_or(false)
            {
                reasons.push("token_uri_match".to_string());
            }
            if image_key
                .as_ref()
                .map(|value| seed_image_uri_keys.contains(value))
                .unwrap_or(false)
            {
                reasons.push("image_uri_match".to_string());
            }
            if !symbol_norm.is_empty() && seed_symbol_norms.contains(&symbol_norm) {
                reasons.push("symbol_match".to_string());
            }
            if has_name_match(&row_name_norm, name_threshold, &seed_name_norms) {
                reasons.push("name_match".to_string());
            }
            let metadata_key = (row.contract_address.clone(), row.token_id.clone());
            if let Some(row_doc) = metadata_index.candidate_doc(&metadata_key) {
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
            let has_name_and_symbol = reasons.iter().any(|reason| reason == "name_match")
                && reasons.iter().any(|reason| reason == "symbol_match");
            let confidence = if has_high_reason || has_name_and_symbol {
                "high"
            } else {
                "low"
            };

            Some(DuplicateCandidate {
                contract_address: row.contract_address.clone(),
                token_id: row.token_id.clone(),
                match_reasons: reasons,
                confidence: confidence.to_string(),
                token_uri: row.token_uri.clone(),
                image_uri: row.image_uri.clone(),
                name: row.name.clone(),
                symbol: row.symbol.clone(),
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
    fn metadata_bm25_index_keeps_only_query_token_candidates() {
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

        let index = build_metadata_bm25_index(&seed_docs, &snapshot_rows, &seed_contracts);

        assert!(index
            .candidate_doc(&("0xgold".into(), "1".into()))
            .is_some());
        assert!(index.candidate_doc(&("0xai".into(), "1".into())).is_some());
        assert!(index
            .candidate_doc(&("0xseed".into(), "1".into()))
            .is_none());
        assert!(index
            .candidate_doc(&("0xmiss".into(), "1".into()))
            .is_none());
        assert_eq!(index.docs.len(), 2);
        assert_eq!(index.corpus.total_docs(), 3);
    }
}
