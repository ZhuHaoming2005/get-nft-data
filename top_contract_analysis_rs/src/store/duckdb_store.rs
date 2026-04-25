use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};

use duckdb::{params, Connection};
use once_cell::sync::Lazy;
use regex::Regex;

use crate::analysis::scoring::metadata_document_from_json;
use crate::error::AppError;
use crate::models::{
    ContractNameRecord, ContractSignal, DatabaseNftRecord, DatabaseSnapshot, SeedNft,
};
use crate::normalize::{normalize_name, normalize_url};

static TOKEN_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"[\p{L}\p{N}_]+").unwrap());

const PRECOMPUTED_COLUMNS: [&str; 5] = [
    "token_uri_norm",
    "image_uri_norm",
    "name_norm",
    "metadata_doc",
    "metadata_keywords_arr",
];

fn metadata_keywords(document: &str, limit: usize) -> Vec<String> {
    let mut counts: HashMap<String, usize> = HashMap::new();
    for token in TOKEN_RE.find_iter(document) {
        let normalized = token.as_str().to_lowercase();
        if normalized.len() < 2 {
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
    resource_options: DuckDbResourceOptions,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DuckDbResourceOptions {
    pub threads: usize,
    pub memory_limit: String,
}

impl Default for DuckDbResourceOptions {
    fn default() -> Self {
        Self {
            threads: std::thread::available_parallelism()
                .map(|threads| threads.get())
                .unwrap_or(1),
            memory_limit: "80GB".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_snapshot_recalls_metadata_from_only_one_seed_example() {
        let store = DuckDbFeatureStore::new(":memory:").unwrap();
        store
            .replace_chain_rows(
                "ethereum",
                &[DatabaseNftRecord {
                    contract_address: "0xcandidate".into(),
                    token_id: "1".into(),
                    metadata_doc: "silver cat".into(),
                    ..Default::default()
                }],
            )
            .unwrap();
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

        let snapshot = store.load_snapshot("ethereum", &seed_nfts, 0, 0).unwrap();

        assert!(snapshot.nft_rows.is_empty());
    }

    #[test]
    fn load_snapshot_marks_rows_that_were_recalled_by_metadata() {
        let store = DuckDbFeatureStore::new(":memory:").unwrap();
        store
            .replace_chain_rows(
                "ethereum",
                &[
                    DatabaseNftRecord {
                        contract_address: "0xmetadata".into(),
                        token_id: "1".into(),
                        metadata_doc: "gold dragon".into(),
                        ..Default::default()
                    },
                    DatabaseNftRecord {
                        contract_address: "0ximage".into(),
                        token_id: "1".into(),
                        image_uri: "ipfs://seed-image.png".into(),
                        metadata_doc: "silver cat".into(),
                        ..Default::default()
                    },
                ],
            )
            .unwrap();
        let seed_nfts = vec![SeedNft {
            contract_address: "0xseed".into(),
            token_id: "1".into(),
            image_uri: "ipfs://seed-image.png".into(),
            metadata_doc: "gold dragon".into(),
            ..Default::default()
        }];

        let snapshot = store.load_snapshot("ethereum", &seed_nfts, 0, 0).unwrap();
        let by_contract: BTreeMap<_, _> = snapshot
            .nft_rows
            .iter()
            .map(|row| (row.contract_address.as_str(), row))
            .collect();

        assert!(by_contract["0xmetadata"].metadata_recall_checked);
        assert!(by_contract["0xmetadata"].metadata_recall_match);
        assert!(by_contract["0ximage"].metadata_recall_checked);
        assert!(!by_contract["0ximage"].metadata_recall_match);
    }
}

impl DuckDbResourceOptions {
    pub fn from_cli(threads: usize, memory_limit: &str) -> Result<Self, AppError> {
        let mut options = Self::default();
        if threads > 0 {
            options.threads = threads;
        }
        if !memory_limit.trim().is_empty() {
            options.memory_limit = memory_limit.trim().to_string();
        }
        validate_duckdb_memory_limit(&options.memory_limit)?;
        Ok(options)
    }
}

fn validate_duckdb_memory_limit(value: &str) -> Result<(), AppError> {
    if value.is_empty()
        || !value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_'))
    {
        return Err(AppError::InvalidData(format!(
            "invalid DuckDB memory limit {value:?}; expected a value like 80GB or 512MB"
        )));
    }
    Ok(())
}

fn seed_metadata_recall_terms(seed_nfts: &[SeedNft]) -> HashSet<String> {
    seed_nfts
        .iter()
        .find_map(|item| {
            let doc = if item.metadata_doc.trim().is_empty() {
                metadata_document_from_json(&item.metadata_json)
            } else {
                item.metadata_doc.clone()
            };
            let keywords = metadata_keywords(&doc, 8);
            if keywords.is_empty() {
                None
            } else {
                Some(keywords.into_iter().collect())
            }
        })
        .unwrap_or_default()
}

impl DuckDbFeatureStore {
    pub fn new(database_path: &str) -> Result<Self, AppError> {
        Self::new_with_options(database_path, DuckDbResourceOptions::default())
    }

    pub fn new_with_options(
        database_path: &str,
        resource_options: DuckDbResourceOptions,
    ) -> Result<Self, AppError> {
        validate_duckdb_memory_limit(&resource_options.memory_limit)?;
        let conn = if database_path == ":memory:" {
            Connection::open_in_memory()?
        } else {
            Connection::open(database_path)?
        };
        let temp_directory = Self::default_temp_directory(database_path);
        std::fs::create_dir_all(&temp_directory)?;
        Self::apply_resource_options(&conn, &resource_options, &temp_directory)?;
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
                metadata_doc VARCHAR,
                metadata_keywords_arr VARCHAR
            );
            ",
        )?;
        Self::validate_schema(&conn)?;
        Ok(Self {
            conn,
            resource_options,
        })
    }

    pub fn resource_options(&self) -> &DuckDbResourceOptions {
        &self.resource_options
    }

    fn apply_resource_options(
        conn: &Connection,
        options: &DuckDbResourceOptions,
        temp_directory: &Path,
    ) -> Result<(), AppError> {
        let memory_limit = options.memory_limit.replace('\'', "''");
        let temp_directory = temp_directory.to_string_lossy().replace('\'', "''");
        conn.execute_batch(&format!(
            "
            PRAGMA threads={};
            PRAGMA memory_limit='{}';
            PRAGMA temp_directory='{}';
            PRAGMA preserve_insertion_order=false;
            ",
            options.threads.max(1),
            memory_limit,
            temp_directory
        ))?;
        Ok(())
    }

    fn default_temp_directory(database_path: &str) -> PathBuf {
        if database_path == ":memory:" {
            std::env::temp_dir().join("top_contract_analysis_rs_duckdb")
        } else {
            Path::new(database_path).with_extension("duckdb.tmp")
        }
    }

    fn validate_schema(conn: &Connection) -> Result<(), AppError> {
        let mut stmt = conn.prepare("DESCRIBE nft_features")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut columns = HashSet::new();
        for row in rows {
            columns.insert(row?);
        }
        let missing: Vec<&str> = PRECOMPUTED_COLUMNS
            .iter()
            .copied()
            .filter(|column| !columns.contains(*column))
            .collect();
        if !missing.is_empty() {
            return Err(AppError::InvalidData(format!(
                "feature DB nft_features table is missing current pre-computed columns {missing:?}. Rebuild it from a current export-snapshot Parquet file."
            )));
        }
        Ok(())
    }

    pub fn has_chain_rows(&self, chain: &str) -> Result<bool, AppError> {
        let exists = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM nft_features WHERE chain = ? LIMIT 1)",
            params![chain],
            |row| row.get::<_, bool>(0),
        )?;
        Ok(exists)
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
                token_uri_norm, image_uri_norm, name_norm, metadata_doc, metadata_keywords_arr
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ",
        )?;

        for row in rows {
            let metadata_doc = if row.metadata_doc.trim().is_empty() {
                metadata_document_from_json(&row.metadata_json)
            } else {
                row.metadata_doc.clone()
            };
            let metadata_keywords_arr =
                serde_json::to_string(&metadata_keywords(&metadata_doc, 8))?;
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
                metadata_doc,
                metadata_keywords_arr,
            ])?;
        }

        Ok(())
    }

    fn load_parquet_dataset_via_duckdb(
        &self,
        chain: &str,
        parquet_path: &str,
        column_names: &HashSet<String>,
    ) -> Result<(), AppError> {
        let path = Self::sql_string_literal(&parquet_path.replace('\\', "/"));
        let metadata_json_expr = if column_names.contains("metadata_json") {
            "coalesce(CAST(metadata_json AS VARCHAR), '')"
        } else {
            "''"
        };
        let insert_sql = format!(
            "
            INSERT INTO nft_features (
                chain, contract_address, token_id, token_uri, image_uri, name, symbol, metadata_json,
                token_uri_norm, image_uri_norm, name_norm, metadata_doc, metadata_keywords_arr
            )
            SELECT
                ? AS chain,
                lower(CAST(contract_address AS VARCHAR)) AS contract_address,
                CAST(token_id AS VARCHAR) AS token_id,
                coalesce(CAST(token_uri AS VARCHAR), '') AS token_uri,
                coalesce(CAST(image_uri AS VARCHAR), '') AS image_uri,
                coalesce(CAST(name AS VARCHAR), '') AS name,
                coalesce(CAST(symbol AS VARCHAR), '') AS symbol,
                {metadata_json_expr} AS metadata_json,
                coalesce(CAST(token_uri_norm AS VARCHAR), '') AS token_uri_norm,
                coalesce(CAST(image_uri_norm AS VARCHAR), '') AS image_uri_norm,
                coalesce(CAST(name_norm AS VARCHAR), '') AS name_norm,
                coalesce(CAST(metadata_doc AS VARCHAR), '') AS metadata_doc,
                coalesce(CAST(metadata_keywords_arr AS VARCHAR), '[]') AS metadata_keywords_arr
            FROM read_parquet({path})
            ",
        );
        self.conn
            .execute("DELETE FROM nft_features WHERE chain = ?", params![chain])?;
        self.conn.execute(&insert_sql, params![chain])?;
        Ok(())
    }

    pub fn load_parquet_dataset(&self, chain: &str, parquet_path: &str) -> Result<(), AppError> {
        let parquet_path_literal = Self::sql_string_literal(&parquet_path.replace('\\', "/"));
        let probe_sql = format!("DESCRIBE SELECT * FROM read_parquet({parquet_path_literal})");
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
        if !missing.is_empty() {
            return Err(AppError::InvalidData(format!(
                "Parquet file {parquet_path:?} is missing pre-computed columns {missing:?}. Re-export the snapshot with the current export-snapshot command."
            )));
        }

        self.load_parquet_dataset_via_duckdb(chain, parquet_path, &column_names)
    }

    pub fn load_parquet_dataset_if_chain_missing(
        &self,
        chain: &str,
        parquet_path: &str,
    ) -> Result<bool, AppError> {
        if self.has_chain_rows(chain)? {
            return Ok(false);
        }
        self.load_parquet_dataset(chain, parquet_path)?;
        Ok(true)
    }

    fn sql_string_literal(value: &str) -> String {
        format!("'{}'", value.replace('\'', "''"))
    }

    fn sql_in_predicate(column: &str, values: &HashSet<String>) -> Option<String> {
        if values.is_empty() {
            return None;
        }
        let values = values
            .iter()
            .filter(|value| !value.is_empty())
            .map(|value| Self::sql_string_literal(value))
            .collect::<BTreeSet<_>>();
        if values.is_empty() {
            None
        } else {
            Some(format!(
                "{column} IN ({})",
                values.into_iter().collect::<Vec<_>>().join(", ")
            ))
        }
    }

    fn sql_metadata_keyword_predicate(values: &HashSet<String>) -> Option<String> {
        if values.is_empty() {
            return None;
        }
        let clauses = values
            .iter()
            .filter_map(|value| serde_json::to_string(value).ok())
            .map(|json_string| {
                format!(
                    "instr(coalesce(metadata_keywords_arr, '[]'), {}) > 0",
                    Self::sql_string_literal(&json_string),
                )
            })
            .collect::<BTreeSet<_>>();
        if clauses.is_empty() {
            None
        } else {
            Some(format!(
                "({})",
                clauses.into_iter().collect::<Vec<_>>().join(" OR ")
            ))
        }
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
        let name_prefixes: HashSet<String> = seed_nfts
            .iter()
            .map(|item| normalize_name(&item.name))
            .filter(|value| !value.is_empty())
            .map(|value| value.chars().take(8).collect::<String>())
            .collect();
        let metadata_recall_terms = seed_metadata_recall_terms(seed_nfts);

        let mut predicates = Vec::new();
        predicates.push("chain = ?".to_string());
        if let Some(predicate) = Self::sql_in_predicate("token_uri_norm", &exact_token_keys) {
            predicates.push(format!("({predicate})"));
        }
        if let Some(predicate) = Self::sql_in_predicate("image_uri_norm", &exact_image_keys) {
            predicates.push(format!("({predicate})"));
        }
        if let Some(predicate) = Self::sql_in_predicate("substr(name_norm, 1, 8)", &name_prefixes) {
            predicates.push(format!("({predicate})"));
        }
        let metadata_recall_predicate =
            Self::sql_metadata_keyword_predicate(&metadata_recall_terms);
        if let Some(predicate) = metadata_recall_predicate.as_ref() {
            predicates.push(predicate.clone());
        }
        let metadata_recall_expr = metadata_recall_predicate
            .as_deref()
            .unwrap_or("FALSE")
            .to_string();
        let recall_predicate = if predicates.len() == 1 {
            "FALSE".to_string()
        } else {
            format!("({})", predicates[1..].join(" OR "))
        };
        let seed_contract_filter = if seed_contracts.is_empty() {
            String::new()
        } else {
            let values = seed_contracts
                .iter()
                .map(|value| Self::sql_string_literal(value))
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>()
                .join(", ");
            format!(" AND contract_address NOT IN ({values})")
        };
        let per_contract_filter = if max_tokens_per_contract > 0 {
            format!("WHERE rn <= {max_tokens_per_contract}")
        } else {
            String::new()
        };
        let total_limit = if max_recall_rows > 0 {
            format!("LIMIT {max_recall_rows}")
        } else {
            String::new()
        };
        let select_sql = format!(
            "
            SELECT contract_address, token_id, token_uri, image_uri, name, symbol, metadata_json, metadata_doc,
                   token_uri_norm, image_uri_norm, name_norm, metadata_recall_match
            FROM (
                SELECT contract_address, token_id, token_uri, image_uri, name, symbol, metadata_json, metadata_doc,
                       token_uri_norm, image_uri_norm, name_norm,
                       {metadata_recall_expr} AS metadata_recall_match,
                       row_number() OVER (PARTITION BY contract_address ORDER BY token_id) AS rn
                FROM nft_features
                WHERE chain = ?{seed_contract_filter}
                  AND {recall_predicate}
            )
            {per_contract_filter}
            ORDER BY contract_address, token_id
            {total_limit}
            "
        );

        let mut stmt = self.conn.prepare(&select_sql)?;

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
                    metadata_recall_checked: false,
                    metadata_recall_match: false,
                },
                row.get::<_, String>(8)?,
                row.get::<_, String>(9)?,
                row.get::<_, String>(10)?,
                row.get::<_, bool>(11)?,
            ))
        })?;

        let mut selected_rows = Vec::new();
        let mut per_contract_counts: HashMap<String, usize> = HashMap::new();
        for row in rows {
            let (mut record, token_uri_norm, image_uri_norm, name_norm, metadata_recall_match) =
                row?;
            if seed_contracts.contains(&record.contract_address) {
                continue;
            }

            let name_prefix = name_norm.chars().take(8).collect::<String>();
            let matches = exact_token_keys.contains(&token_uri_norm)
                || exact_image_keys.contains(&image_uri_norm)
                || (!name_prefix.is_empty() && name_prefixes.contains(&name_prefix))
                || metadata_recall_match;

            if !matches {
                continue;
            }
            record.metadata_recall_checked = true;
            record.metadata_recall_match = metadata_recall_match;

            let entry = per_contract_counts
                .entry(record.contract_address.clone())
                .or_default();
            if max_tokens_per_contract > 0 && *entry >= max_tokens_per_contract {
                continue;
            }
            *entry += 1;
            selected_rows.push((
                record,
                token_uri_norm,
                image_uri_norm,
                name_norm,
                metadata_recall_match,
            ));
            if max_recall_rows > 0 && selected_rows.len() >= max_recall_rows {
                break;
            }
        }

        let mut nft_rows = Vec::new();
        let mut seen_contract_name_pairs: BTreeSet<(String, String)> = BTreeSet::new();
        let mut contract_names = Vec::new();
        let mut contract_signals_raw: BTreeMap<String, ContractSignal> = BTreeMap::new();
        for (record, token_uri_norm, image_uri_norm, name_norm, metadata_recall_match) in
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
            let name_prefix = name_norm.chars().take(8).collect::<String>();
            if !name_prefix.is_empty() && name_prefixes.contains(&name_prefix) {
                signal.name_prefix_match = true;
            }
            if metadata_recall_match {
                signal.keyword_match = true;
            }

            nft_rows.push(record);
        }

        Ok(DatabaseSnapshot {
            nft_rows,
            contract_names,
            contract_signals: contract_signals_raw,
        })
    }
}
