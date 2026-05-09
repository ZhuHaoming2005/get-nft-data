use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use crate::analysis::scoring::{
    metadata_document_from_json, metadata_is_dedup_eligible, metadata_recall_document,
    metadata_recall_keywords, MAX_METADATA_BYTES_FOR_DEDUP,
};
use crate::error::AppError;
use crate::models::{
    ContractDuplicateRecord, ContractNameRecord, ContractSignal, DatabaseNftRecord,
    DatabaseSnapshot, SeedNft,
};
use crate::normalize::{normalize_name, normalize_url};
use duckdb::{params, AccessMode, Config, Connection};

const MAX_OVERLAPPING_METADATA_ROWS_PER_CONTRACT: usize = 1;

const PRECOMPUTED_COLUMNS: [&str; 5] = [
    "token_uri_norm",
    "image_uri_norm",
    "name_norm",
    "metadata_doc",
    "metadata_keywords_arr",
];

pub struct DuckDbFeatureStore {
    conn: Mutex<Connection>,
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
                        metadata_json: r#"{"description":"gold dragon"}"#.into(),
                        ..Default::default()
                    },
                    DatabaseNftRecord {
                        contract_address: "0ximage".into(),
                        token_id: "1".into(),
                        image_uri: "ipfs://seed-image.png".into(),
                        ..Default::default()
                    },
                ],
            )
            .unwrap();
        let seed_nfts = vec![SeedNft {
            contract_address: "0xseed".into(),
            token_id: "1".into(),
            image_uri: "ipfs://seed-image.png".into(),
            metadata_json: r#"{"description":"gold dragon"}"#.into(),
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

    #[test]
    fn load_snapshot_uses_max_recall_rows_as_batch_size_not_total_limit() {
        let store = DuckDbFeatureStore::new(":memory:").unwrap();
        store
            .replace_chain_rows(
                "ethereum",
                &[
                    DatabaseNftRecord {
                        contract_address: "0xcandidate_a".into(),
                        token_id: "1".into(),
                        token_uri: "ipfs://shared/1".into(),
                        ..Default::default()
                    },
                    DatabaseNftRecord {
                        contract_address: "0xcandidate_b".into(),
                        token_id: "1".into(),
                        token_uri: "ipfs://shared/1".into(),
                        ..Default::default()
                    },
                ],
            )
            .unwrap();
        let seed_nfts = vec![SeedNft {
            contract_address: "0xseed".into(),
            token_id: "1".into(),
            token_uri: "ipfs://shared/1".into(),
            ..Default::default()
        }];

        let snapshot = store.load_snapshot("ethereum", &seed_nfts, 0, 1).unwrap();
        let contracts: Vec<_> = snapshot
            .nft_rows
            .iter()
            .map(|row| row.contract_address.as_str())
            .collect();

        assert_eq!(contracts, vec!["0xcandidate_a", "0xcandidate_b"]);
    }

    #[test]
    fn duplicate_contract_row_uses_precomputed_name_pair_uniqueness() {
        let record = DatabaseNftRecord {
            contract_address: "0xcandidate".into(),
            token_id: "1".into(),
            ..Default::default()
        };
        let mut rows_by_contract = HashMap::new();

        update_duplicate_contract_row(
            &mut rows_by_contract,
            &record,
            false,
            false,
            "azuki",
            true,
            false,
        );
        update_duplicate_contract_row(
            &mut rows_by_contract,
            &record,
            false,
            false,
            "azuki",
            false,
            false,
        );

        assert_eq!(
            rows_by_contract["0xcandidate"].name_norms,
            vec!["azuki".to_string()]
        );
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
            let doc = metadata_recall_document(&item.metadata_doc, &item.metadata_json);
            let keywords = metadata_recall_keywords(&doc, 8);
            if keywords.is_empty() {
                None
            } else {
                Some(keywords.into_iter().collect())
            }
        })
        .unwrap_or_default()
}

fn update_duplicate_contract_row(
    rows_by_contract: &mut HashMap<String, ContractDuplicateRecord>,
    record: &DatabaseNftRecord,
    token_uri_match: bool,
    image_uri_match: bool,
    name_norm: &str,
    name_pair_is_new: bool,
    metadata_recall_match: bool,
) {
    let entry = rows_by_contract
        .entry(record.contract_address.clone())
        .or_insert_with(|| ContractDuplicateRecord {
            contract_address: record.contract_address.clone(),
            representative: record.clone(),
            ..ContractDuplicateRecord::default()
        });

    entry.token_uri_match |= token_uri_match;
    entry.image_uri_match |= image_uri_match;
    if name_pair_is_new && !name_norm.is_empty() {
        entry.name_norms.push(name_norm.to_string());
    }
    push_metadata_token_row(entry, record);

    entry.metadata_recall_checked = true;
    entry.metadata_recall_match |= metadata_recall_match;
    let should_update_metadata_doc = entry.metadata_doc.is_empty()
        || (metadata_recall_match && !entry.representative.metadata_recall_match);
    if !should_update_metadata_doc {
        return;
    }

    if record.metadata_doc.is_empty() {
        return;
    }
    entry.metadata_doc = record.metadata_doc.clone();
    if metadata_recall_match {
        entry.representative = record.clone();
    }
}

fn push_metadata_token_row(entry: &mut ContractDuplicateRecord, record: &DatabaseNftRecord) {
    if !metadata_is_dedup_eligible(&record.metadata_doc, &record.metadata_json) {
        return;
    }
    let metadata_doc = metadata_document_from_json(&record.metadata_json);
    if metadata_doc.is_empty() {
        return;
    }
    if entry
        .metadata_token_rows
        .iter()
        .any(|row| row.token_id == record.token_id)
    {
        return;
    }
    entry.metadata_token_rows.push(record.clone());
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
            conn: Mutex::new(conn),
            resource_options,
        })
    }

    pub fn open_read_only_with_options(
        database_path: &str,
        resource_options: DuckDbResourceOptions,
    ) -> Result<Self, AppError> {
        if database_path == ":memory:" {
            return Self::new_with_options(database_path, resource_options);
        }
        validate_duckdb_memory_limit(&resource_options.memory_limit)?;
        let conn = Connection::open_with_flags(
            database_path,
            Config::default().access_mode(AccessMode::ReadOnly)?,
        )?;
        let temp_directory = Self::default_temp_directory(database_path);
        std::fs::create_dir_all(&temp_directory)?;
        Self::apply_resource_options(&conn, &resource_options, &temp_directory)?;
        Self::validate_schema(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
            resource_options,
        })
    }

    pub fn resource_options(&self) -> &DuckDbResourceOptions {
        &self.resource_options
    }

    fn conn(&self) -> Result<MutexGuard<'_, Connection>, AppError> {
        self.conn
            .lock()
            .map_err(|err| AppError::DuckDb(format!("DuckDB connection lock poisoned: {err}")))
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
        let conn = self.conn()?;
        let exists = conn.query_row(
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
        let conn = self.conn()?;
        conn.execute("DELETE FROM nft_features WHERE chain = ?", params![chain])?;

        let mut stmt = conn.prepare(
            "
            INSERT INTO nft_features (
                chain, contract_address, token_id, token_uri, image_uri, name, symbol, metadata_json,
                token_uri_norm, image_uri_norm, name_norm, metadata_doc, metadata_keywords_arr
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ",
        )?;

        for row in rows {
            let metadata_doc = if metadata_is_dedup_eligible(&row.metadata_doc, &row.metadata_json)
            {
                metadata_document_from_json(&row.metadata_json)
            } else {
                String::new()
            };
            let metadata_recall_doc = metadata_recall_document(&metadata_doc, &row.metadata_json);
            let metadata_keywords_arr =
                serde_json::to_string(&metadata_recall_keywords(&metadata_recall_doc, 8))?;
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
        let conn = self.conn()?;
        conn.execute("DELETE FROM nft_features WHERE chain = ?", params![chain])?;
        conn.execute(&insert_sql, params![chain])?;
        Ok(())
    }

    pub fn load_parquet_dataset(&self, chain: &str, parquet_path: &str) -> Result<(), AppError> {
        let parquet_path_literal = Self::sql_string_literal(&parquet_path.replace('\\', "/"));
        let probe_sql = format!("DESCRIBE SELECT * FROM read_parquet({parquet_path_literal})");
        let column_names: HashSet<String> = {
            let conn = self.conn()?;
            let mut stmt = conn.prepare(&probe_sql)?;
            let describe_rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
            let mut column_names = HashSet::new();
            for row in describe_rows {
                column_names.insert(row?);
            }
            column_names
        };

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

    fn sql_metadata_json_eligible_predicate() -> String {
        format!(
            "length(trim(coalesce(CAST(metadata_json AS VARCHAR), ''))) <= {MAX_METADATA_BYTES_FOR_DEDUP} \
             AND (trim(coalesce(CAST(metadata_json AS VARCHAR), '')) LIKE '{{%' \
             OR trim(coalesce(CAST(metadata_json AS VARCHAR), '')) LIKE '[%')"
        )
    }

    fn append_overlapping_metadata_token_rows(
        conn: &Connection,
        chain: &str,
        seed_token_ids: &HashSet<String>,
        rows_by_contract: &mut HashMap<String, ContractDuplicateRecord>,
    ) -> Result<(), AppError> {
        if seed_token_ids.is_empty() || rows_by_contract.is_empty() {
            return Ok(());
        }
        let contract_key_by_lower = rows_by_contract
            .keys()
            .map(|key| (key.to_lowercase(), key.clone()))
            .collect::<BTreeMap<_, _>>();
        let token_values = seed_token_ids
            .iter()
            .filter(|value| !value.is_empty())
            .map(|value| Self::sql_string_literal(value))
            .collect::<BTreeSet<_>>();
        if token_values.is_empty() {
            return Ok(());
        }
        let token_values = token_values.into_iter().collect::<Vec<_>>().join(", ");
        let contract_keys = contract_key_by_lower.keys().cloned().collect::<Vec<_>>();
        for chunk in contract_keys.chunks(500) {
            let contract_values = chunk
                .iter()
                .map(|value| Self::sql_string_literal(value))
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "
                SELECT contract_key, contract_address, token_id, token_uri, image_uri, name, symbol,
                       metadata_json, metadata_doc
                FROM (
                    SELECT lower(contract_address) AS contract_key,
                           contract_address, token_id, coalesce(token_uri, '') AS token_uri,
                           coalesce(image_uri, '') AS image_uri, coalesce(name, '') AS name,
                           coalesce(symbol, '') AS symbol, coalesce(metadata_json, '') AS metadata_json,
                           coalesce(metadata_doc, '') AS metadata_doc,
                           row_number() OVER (
                               PARTITION BY lower(contract_address)
                               ORDER BY CASE
                                   WHEN trim(coalesce(CAST(metadata_json AS VARCHAR), '')) LIKE '{{%'
                                        OR trim(coalesce(CAST(metadata_json AS VARCHAR), '')) LIKE '[%' THEN 0
                                   WHEN trim(coalesce(CAST(metadata_json AS VARCHAR), '')) <> '' THEN 1
                                   WHEN trim(coalesce(CAST(metadata_doc AS VARCHAR), '')) <> '' THEN 2
                                   ELSE 3
                               END,
                               CAST(token_id AS VARCHAR)
                           ) AS overlap_rank
                    FROM nft_features
                    WHERE chain = ?
                      AND lower(contract_address) IN ({contract_values})
                      AND CAST(token_id AS VARCHAR) IN ({token_values})
                      AND length(trim(coalesce(CAST(metadata_json AS VARCHAR), ''))) <= {MAX_METADATA_BYTES_FOR_DEDUP}
                      AND (
                          trim(coalesce(CAST(metadata_json AS VARCHAR), '')) LIKE '{{%'
                          OR trim(coalesce(CAST(metadata_json AS VARCHAR), '')) LIKE '[%'
                      )
                )
                WHERE overlap_rank <= {MAX_OVERLAPPING_METADATA_ROWS_PER_CONTRACT}
                ORDER BY contract_key, token_id
                "
            );
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map(params![chain], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    DatabaseNftRecord {
                        contract_address: row.get::<_, String>(1)?,
                        token_id: row.get::<_, String>(2)?,
                        token_uri: row.get::<_, String>(3)?,
                        image_uri: row.get::<_, String>(4)?,
                        name: row.get::<_, String>(5)?,
                        symbol: row.get::<_, String>(6)?,
                        metadata_json: row.get::<_, String>(7)?,
                        metadata_doc: row.get::<_, String>(8)?,
                        metadata_recall_checked: false,
                        metadata_recall_match: false,
                    },
                ))
            })?;
            for row in rows {
                let (contract_key, record) = row?;
                let Some(original_key) = contract_key_by_lower.get(&contract_key) else {
                    continue;
                };
                if let Some(entry) = rows_by_contract.get_mut(original_key) {
                    entry.metadata_token_rows.clear();
                    push_metadata_token_row(entry, &record);
                }
            }
        }
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
        let seed_token_ids: HashSet<String> = seed_nfts
            .iter()
            .map(|item| item.token_id.clone())
            .filter(|value| !value.is_empty())
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
            Self::sql_metadata_keyword_predicate(&metadata_recall_terms).map(|predicate| {
                format!(
                    "({}) AND ({predicate})",
                    Self::sql_metadata_json_eligible_predicate()
                )
            });
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
        let base_select_sql = format!(
            "
            SELECT feature_rowid, contract_address, token_id,
                   token_uri_norm, image_uri_norm, name_norm, metadata_recall_match
            FROM (
                SELECT rowid AS feature_rowid, contract_address, token_id,
                       token_uri_norm, image_uri_norm, name_norm,
                       {metadata_recall_expr} AS metadata_recall_match,
                       row_number() OVER (PARTITION BY contract_address ORDER BY token_id) AS rn
                FROM nft_features
                WHERE chain = ?{seed_contract_filter}
                  AND {recall_predicate}
            )
            {per_contract_filter}
            ORDER BY contract_address, token_id
            "
        );

        let mut per_contract_counts: HashMap<String, usize> = HashMap::new();
        let mut nft_rows = Vec::new();
        let mut duplicate_rows_by_contract = HashMap::<String, ContractDuplicateRecord>::new();
        let mut seen_contract_name_pairs: BTreeSet<(String, String)> = BTreeSet::new();
        let mut contract_names = Vec::new();
        let mut contract_signals_raw: BTreeMap<String, ContractSignal> = BTreeMap::new();
        let recall_batch_size = max_recall_rows;
        let temp_table = "__top_contract_analysis_recall_snapshot";
        let conn = self.conn()?;
        conn.execute_batch(&format!("DROP TABLE IF EXISTS {temp_table}"))?;
        conn.execute(
            &format!(
                "
                CREATE TEMP TABLE {temp_table} AS
                SELECT row_number() OVER (ORDER BY contract_address, token_id) AS recall_row_id,
                       feature_rowid, contract_address, token_id, token_uri_norm, image_uri_norm,
                       name_norm, metadata_recall_match
                FROM ({base_select_sql})
                "
            ),
            params![chain],
        )?;
        let mut last_recall_row_id = 0_i64;
        loop {
            let select_sql = if recall_batch_size > 0 {
                format!(
                    "
                    SELECT f.contract_address, f.token_id, coalesce(f.token_uri, ''),
                           coalesce(f.image_uri, ''), coalesce(f.name, ''), coalesce(f.symbol, ''),
                           coalesce(f.metadata_json, ''), coalesce(f.metadata_doc, ''),
                           t.token_uri_norm, t.image_uri_norm, t.name_norm,
                           t.metadata_recall_match, t.recall_row_id
                    FROM {temp_table} t
                    JOIN nft_features f ON f.rowid = t.feature_rowid
                    WHERE t.recall_row_id > {last_recall_row_id}
                    ORDER BY t.recall_row_id
                    LIMIT {recall_batch_size}
                    "
                )
            } else {
                format!(
                    "
                    SELECT f.contract_address, f.token_id, coalesce(f.token_uri, ''),
                           coalesce(f.image_uri, ''), coalesce(f.name, ''), coalesce(f.symbol, ''),
                           coalesce(f.metadata_json, ''), coalesce(f.metadata_doc, ''),
                           t.token_uri_norm, t.image_uri_norm, t.name_norm,
                           t.metadata_recall_match, t.recall_row_id
                    FROM {temp_table} t
                    JOIN nft_features f ON f.rowid = t.feature_rowid
                    ORDER BY t.recall_row_id
                    "
                )
            };
            let mut stmt = conn.prepare(&select_sql)?;
            let rows = stmt.query_map([], |row| {
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
                    row.get::<_, i64>(12)?,
                ))
            })?;

            let mut fetched_rows = 0usize;
            let mut last_seen_recall_row_id = last_recall_row_id;
            for row in rows {
                fetched_rows += 1;
                let (
                    mut record,
                    token_uri_norm,
                    image_uri_norm,
                    name_norm,
                    metadata_recall_match,
                    recall_row_id,
                ) = row?;
                last_seen_recall_row_id = recall_row_id;
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

                let token_uri_match = exact_token_keys.contains(&token_uri_norm);
                let image_uri_match = exact_image_keys.contains(&image_uri_norm);
                let name_pair_is_new = !name_norm.is_empty()
                    && seen_contract_name_pairs
                        .insert((record.contract_address.clone(), name_norm.clone()));
                if name_pair_is_new {
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
                if token_uri_match {
                    signal.uri_match_count += 1;
                }
                if image_uri_match {
                    signal.image_match_count += 1;
                }
                let name_prefix = name_norm.chars().take(8).collect::<String>();
                if !name_prefix.is_empty() && name_prefixes.contains(&name_prefix) {
                    signal.name_prefix_match = true;
                }
                if metadata_recall_match {
                    signal.keyword_match = true;
                }

                update_duplicate_contract_row(
                    &mut duplicate_rows_by_contract,
                    &record,
                    token_uri_match,
                    image_uri_match,
                    &name_norm,
                    name_pair_is_new,
                    metadata_recall_match,
                );
                nft_rows.push(record);
            }
            drop(stmt);

            if recall_batch_size == 0 || fetched_rows < recall_batch_size {
                break;
            }
            last_recall_row_id = last_seen_recall_row_id;
        }
        conn.execute_batch(&format!("DROP TABLE IF EXISTS {temp_table}"))?;
        Self::append_overlapping_metadata_token_rows(
            &conn,
            chain,
            &seed_token_ids,
            &mut duplicate_rows_by_contract,
        )?;
        let mut duplicate_contract_rows: Vec<_> =
            duplicate_rows_by_contract.into_values().collect();
        for row in &mut duplicate_contract_rows {
            row.metadata_token_rows
                .sort_by(|left, right| left.token_id.cmp(&right.token_id));
        }
        duplicate_contract_rows
            .sort_by(|left, right| left.contract_address.cmp(&right.contract_address));

        Ok(DatabaseSnapshot {
            nft_rows,
            duplicate_contract_rows,
            contract_names,
            contract_signals: contract_signals_raw,
        })
    }
}
