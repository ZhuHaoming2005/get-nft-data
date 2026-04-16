use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use duckdb::{params, Connection};
use once_cell::sync::Lazy;
use regex::Regex;

use crate::analysis::scoring::metadata_document_from_json;
use crate::error::AppError;
use crate::models::{
    ContractNameRecord, ContractSignal, DatabaseNftRecord, DatabaseSnapshot, SeedNft,
};
use crate::normalize::{normalize_name, normalize_symbol, normalize_url};

static TOKEN_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"[\p{L}\p{N}_]+").unwrap());

const PRECOMPUTED_COLUMNS: [&str; 6] = [
    "token_uri_norm",
    "image_uri_norm",
    "name_norm",
    "symbol_norm",
    "metadata_doc",
    "metadata_keywords_arr",
];

fn metadata_keywords(document: &str, limit: usize) -> Vec<String> {
    let mut counts: HashMap<String, usize> = HashMap::new();
    for token in TOKEN_RE.find_iter(document) {
        let normalized = token.as_str().to_lowercase();
        if normalized.len() < 4 {
            continue;
        }
        *counts.entry(normalized).or_insert(0) += 1;
    }
    let mut ranked: Vec<(String, usize)> = counts.into_iter().collect();
    ranked.sort_by(|left, right| {
        right
            .1
            .cmp(&left.1)
            .then_with(|| right.0.len().cmp(&left.0.len()))
            .then_with(|| left.0.cmp(&right.0))
    });
    ranked
        .into_iter()
        .take(limit)
        .map(|(token, _)| token)
        .collect()
}

pub struct DuckDbFeatureStore {
    conn: Connection,
}

impl DuckDbFeatureStore {
    pub fn new(database_path: &str) -> Result<Self, AppError> {
        let conn = if database_path == ":memory:" {
            Connection::open_in_memory()?
        } else {
            Connection::open(database_path)?
        };
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS nft_features (
                chain VARCHAR NOT NULL,
                contract_address VARCHAR NOT NULL,
                token_id VARCHAR NOT NULL,
                token_uri VARCHAR,
                image_uri VARCHAR,
                name VARCHAR,
                symbol VARCHAR,
                metadata_json VARCHAR,
                token_uri_norm VARCHAR,
                image_uri_norm VARCHAR,
                name_norm VARCHAR,
                symbol_norm VARCHAR,
                metadata_doc VARCHAR
            );
            ",
        )?;
        Ok(Self { conn })
    }

    pub fn replace_chain_rows(
        &self,
        chain: &str,
        rows: &[DatabaseNftRecord],
    ) -> Result<(), AppError> {
        self.conn
            .execute("DELETE FROM nft_features WHERE chain = ?", params![chain])?;

        let mut stmt = self.conn.prepare(
            "
            INSERT INTO nft_features (
                chain, contract_address, token_id, token_uri, image_uri, name, symbol, metadata_json,
                token_uri_norm, image_uri_norm, name_norm, symbol_norm, metadata_doc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ",
        )?;

        for row in rows {
            let metadata_doc = if row.metadata_doc.trim().is_empty() {
                metadata_document_from_json(&row.metadata_json)
            } else {
                row.metadata_doc.clone()
            };
            stmt.execute(params![
                chain,
                row.contract_address.to_lowercase(),
                row.token_id,
                row.token_uri,
                row.image_uri,
                row.name,
                row.symbol,
                row.metadata_json,
                normalize_url(&row.token_uri).unwrap_or_default(),
                normalize_url(&row.image_uri).unwrap_or_default(),
                normalize_name(&row.name),
                normalize_symbol(&row.symbol),
                metadata_doc,
            ])?;
        }

        Ok(())
    }

    pub fn load_parquet_dataset(
        &self,
        chain: &str,
        parquet_path: &str,
        strict: bool,
    ) -> Result<(), AppError> {
        let probe_sql = format!(
            "DESCRIBE SELECT * FROM read_parquet('{}')",
            parquet_path.replace('\\', "/")
        );
        let mut stmt = self.conn.prepare(&probe_sql)?;
        let describe_rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut column_names: HashSet<String> = HashSet::new();
        for row in describe_rows {
            column_names.insert(row?);
        }

        let missing: Vec<&str> = PRECOMPUTED_COLUMNS
            .iter()
            .copied()
            .filter(|column| !column_names.contains(*column))
            .collect();
        if strict && !missing.is_empty() {
            return Err(AppError::InvalidData(format!(
                "Parquet file {parquet_path:?} is missing pre-computed columns {missing:?}. Re-export the snapshot with export_snapshot.py or disable strict mode."
            )));
        }

        let metadata_json_expr = if column_names.contains("metadata_json") {
            "coalesce(CAST(metadata_json AS VARCHAR), '')"
        } else {
            "''"
        };
        let metadata_doc_expr = if column_names.contains("metadata_doc") {
            "coalesce(CAST(metadata_doc AS VARCHAR), '')"
        } else {
            "''"
        };
        let select_sql = format!(
            "
            SELECT
                lower(CAST(contract_address AS VARCHAR)) AS contract_address,
                CAST(token_id AS VARCHAR) AS token_id,
                coalesce(CAST(token_uri AS VARCHAR), '') AS token_uri,
                coalesce(CAST(image_uri AS VARCHAR), '') AS image_uri,
                coalesce(CAST(name AS VARCHAR), '') AS name,
                coalesce(CAST(symbol AS VARCHAR), '') AS symbol,
                {metadata_json_expr} AS metadata_json,
                {metadata_doc_expr} AS metadata_doc
            FROM read_parquet('{}')
            ",
            parquet_path.replace('\\', "/")
        );

        let mut query = self.conn.prepare(&select_sql)?;
        let rows = query.query_map([], |row| {
            Ok(DatabaseNftRecord {
                contract_address: row.get::<_, String>(0)?,
                token_id: row.get::<_, String>(1)?,
                token_uri: row.get::<_, String>(2)?,
                image_uri: row.get::<_, String>(3)?,
                name: row.get::<_, String>(4)?,
                symbol: row.get::<_, String>(5)?,
                metadata_json: row.get::<_, String>(6)?,
                metadata_doc: row.get::<_, String>(7)?,
            })
        })?;

        let mut collected = Vec::new();
        for row in rows {
            collected.push(row?);
        }
        self.replace_chain_rows(chain, &collected)?;
        Ok(())
    }

    pub fn load_snapshot(
        &self,
        chain: &str,
        seed_nfts: &[SeedNft],
        max_tokens_per_contract: usize,
        max_recall_rows: usize,
    ) -> Result<DatabaseSnapshot, AppError> {
        let seed_contracts: HashSet<String> = seed_nfts
            .iter()
            .map(|item| item.contract_address.to_lowercase())
            .collect();
        let exact_token_keys: HashSet<String> = seed_nfts
            .iter()
            .filter_map(|item| normalize_url(&item.token_uri))
            .collect();
        let exact_image_keys: HashSet<String> = seed_nfts
            .iter()
            .filter_map(|item| normalize_url(&item.image_uri))
            .collect();
        let exact_symbols: HashSet<String> = seed_nfts
            .iter()
            .map(|item| normalize_symbol(&item.symbol))
            .filter(|value| !value.is_empty())
            .collect();
        let name_prefixes: HashSet<String> = seed_nfts
            .iter()
            .map(|item| normalize_name(&item.name))
            .filter(|value| !value.is_empty())
            .map(|value| value.chars().take(8).collect::<String>())
            .collect();
        let metadata_recall_terms: HashSet<String> = seed_nfts
            .iter()
            .flat_map(|item| {
                let doc = if item.metadata_doc.trim().is_empty() {
                    metadata_document_from_json(&item.metadata_json)
                } else {
                    item.metadata_doc.clone()
                };
                metadata_keywords(&doc, 8)
            })
            .collect();

        let mut stmt = self.conn.prepare(
            "
            SELECT contract_address, token_id, token_uri, image_uri, name, symbol, metadata_json, metadata_doc,
                   token_uri_norm, image_uri_norm, name_norm, symbol_norm
            FROM nft_features
            WHERE chain = ?
            ORDER BY contract_address, token_id
            ",
        )?;

        let rows = stmt.query_map(params![chain], |row| {
            Ok((
                DatabaseNftRecord {
                    contract_address: row.get::<_, String>(0)?,
                    token_id: row.get::<_, String>(1)?,
                    token_uri: row.get::<_, String>(2)?,
                    image_uri: row.get::<_, String>(3)?,
                    name: row.get::<_, String>(4)?,
                    symbol: row.get::<_, String>(5)?,
                    metadata_json: row.get::<_, String>(6)?,
                    metadata_doc: row.get::<_, String>(7)?,
                },
                row.get::<_, String>(8)?,
                row.get::<_, String>(9)?,
                row.get::<_, String>(10)?,
                row.get::<_, String>(11)?,
            ))
        })?;

        let mut selected_rows = Vec::new();
        let mut per_contract_counts: BTreeMap<String, usize> = BTreeMap::new();
        for row in rows {
            let (record, token_uri_norm, image_uri_norm, name_norm, symbol_norm) = row?;
            if seed_contracts.contains(&record.contract_address) {
                continue;
            }

            let metadata_doc = if record.metadata_doc.trim().is_empty() {
                metadata_document_from_json(&record.metadata_json)
            } else {
                record.metadata_doc.clone()
            };
            let row_keywords: HashSet<String> = metadata_keywords(&metadata_doc, 8).into_iter().collect();
            let name_prefix = name_norm.chars().take(8).collect::<String>();
            let matches = exact_token_keys.contains(&token_uri_norm)
                || exact_image_keys.contains(&image_uri_norm)
                || exact_symbols.contains(&symbol_norm)
                || (!name_prefix.is_empty() && name_prefixes.contains(&name_prefix))
                || (!metadata_recall_terms.is_empty()
                    && !row_keywords.is_empty()
                    && !row_keywords.is_disjoint(&metadata_recall_terms));

            if !matches {
                continue;
            }

            let entry = per_contract_counts.entry(record.contract_address.clone()).or_default();
            if max_tokens_per_contract > 0 && *entry >= max_tokens_per_contract {
                continue;
            }
            *entry += 1;
            selected_rows.push((record, token_uri_norm, image_uri_norm, name_norm, symbol_norm, row_keywords));
            if max_recall_rows > 0 && selected_rows.len() >= max_recall_rows {
                break;
            }
        }

        let mut nft_rows = Vec::new();
        let mut seen_contract_name_pairs: BTreeSet<(String, String)> = BTreeSet::new();
        let mut contract_names = Vec::new();
        let mut symbol_contracts: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        let mut contract_signals_raw: BTreeMap<String, ContractSignal> = BTreeMap::new();
        for (record, token_uri_norm, image_uri_norm, name_norm, symbol_norm, row_keywords) in
            selected_rows
        {
            if !name_norm.is_empty()
                && seen_contract_name_pairs
                    .insert((record.contract_address.clone(), name_norm.clone()))
            {
                contract_names.push(ContractNameRecord {
                    contract_address: record.contract_address.clone(),
                    name_norm: name_norm.clone(),
                });
            }
            if !symbol_norm.is_empty() {
                symbol_contracts
                    .entry(symbol_norm.clone())
                    .or_default()
                    .insert(record.contract_address.clone());
            }

            let signal = contract_signals_raw
                .entry(record.contract_address.clone())
                .or_insert_with(|| ContractSignal {
                    contract_address: record.contract_address.clone(),
                    ..ContractSignal::default()
                });
            signal.token_count += 1;
            if exact_token_keys.contains(&token_uri_norm) {
                signal.uri_match_count += 1;
            }
            if exact_image_keys.contains(&image_uri_norm) {
                signal.image_match_count += 1;
            }
            if exact_symbols.contains(&symbol_norm) {
                signal.symbol_match = true;
            }
            let name_prefix = name_norm.chars().take(8).collect::<String>();
            if !name_prefix.is_empty() && name_prefixes.contains(&name_prefix) {
                signal.name_prefix_match = true;
            }
            if !metadata_recall_terms.is_empty()
                && !row_keywords.is_empty()
                && !row_keywords.is_disjoint(&metadata_recall_terms)
            {
                signal.keyword_match = true;
            }

            nft_rows.push(record);
        }

        Ok(DatabaseSnapshot {
            nft_rows,
            contract_names,
            symbol_contracts,
            contract_signals: contract_signals_raw,
        })
    }
}
