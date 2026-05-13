use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::analysis::scoring::{
    metadata_recall_document, score_metadata_indexed_pair_with_query, score_name_pair,
    MetadataBm25CorpusBuilder, MetadataBm25Document, MetadataBm25Query,
    MAX_METADATA_BYTES_FOR_DEDUP,
};
use crate::error::AppError;
use crate::models::{ContractDuplicateRecord, DatabaseNftRecord, DatabaseSnapshot, SeedNft};
use crate::normalize::{normalize_name, normalize_url};
use duckdb::{params, AccessMode, Config, Connection};

const MAX_OVERLAPPING_METADATA_ROWS_PER_CONTRACT: usize = 1;
const DEFAULT_RECALL_BATCH_SIZE: usize = 500_000;
const SELECTED_RECALL_ROWID_CHUNK_SIZE: usize = 50_000;
const BULK_IMPORT_CHECKPOINT_THRESHOLD: &str = "1TB";
const BULK_IMPORT_SKIP_WAL_THRESHOLD_BYTES: u64 = 1_099_511_627_776;
#[cfg(test)]
const PREPARED_METADATA_DOC_BATCH_SIZE: usize = 2;
#[cfg(not(test))]
const PREPARED_METADATA_DOC_BATCH_SIZE: usize = 50_000;
const SEED_TOKEN_URI_TABLE: &str = "__top_contract_analysis_seed_token_uri_keys";
const SEED_IMAGE_URI_TABLE: &str = "__top_contract_analysis_seed_image_uri_keys";
const SEED_TOKEN_ID_TABLE: &str = "__top_contract_analysis_seed_token_ids";
const CANDIDATE_CONTRACT_TABLE: &str = "__top_contract_analysis_candidate_contracts";
const SELECTED_RECALL_ROWID_TABLE: &str = "__top_contract_analysis_selected_recall_rowids";
const CONTRACT_REPRESENTATIVE_TABLE: &str = "nft_contract_representatives";
const FEATURE_RECALL_TABLE: &str = "nft_feature_recall_rows";
const METADATA_RECALL_DOC_TABLE: &str = "nft_metadata_recall_docs";
const PREPARED_RECALL_CHAIN_TABLE: &str = "nft_prepared_recall_chains";
const METADATA_SKETCH_ANCHOR_COUNT: usize = 8;
const METADATA_SKETCH_SOURCE_HAMMING_THRESHOLD: u32 = 24;
const METADATA_SIMHASH_BAND_COUNT: usize = 8;
const METADATA_SIMHASH_BAND_BITS: usize = 8;
const METADATA_SIMHASH_BAND_VALUES: usize = 1 << METADATA_SIMHASH_BAND_BITS;
const METADATA_SKETCH_HIGH_FREQ_MIN_DOCS: usize = 32;
const METADATA_SKETCH_HIGH_FREQ_DIVISOR: usize = 5;

const REQUIRED_SNAPSHOT_COLUMNS: [&str; 4] = [
    "metadata_json",
    "token_uri_norm",
    "image_uri_norm",
    "name_norm",
];

mod support;

use support::*;

pub struct DuckDbFeatureStore {
    conn: Mutex<Connection>,
    resource_options: DuckDbResourceOptions,
    metadata_recall_index_cache: Mutex<HashMap<String, Arc<MetadataRecallIndex>>>,
    writable: bool,
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
mod tests;

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

struct ExactUriRecallInput<'a> {
    chain: &'a str,
    profiles: &'a [SeedRecallProfile],
    profile_index: &'a SeedProfileIndex,
    all_token_keys: &'a HashSet<String>,
    all_image_keys: &'a HashSet<String>,
    name_threshold: f64,
    max_tokens_per_contract: usize,
    max_recall_rows: usize,
    prepared_recall_state: PreparedRecallState,
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
                name_norm VARCHAR
            );
            ",
        )?;
        Self::create_prepared_recall_tables(&conn)?;
        Self::drop_obsolete_nft_feature_columns(&conn)?;
        Self::validate_schema(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
            resource_options,
            metadata_recall_index_cache: Mutex::new(HashMap::new()),
            writable: true,
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
            metadata_recall_index_cache: Mutex::new(HashMap::new()),
            writable: false,
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

    fn metadata_recall_index_cache(
        &self,
    ) -> Result<MutexGuard<'_, HashMap<String, Arc<MetadataRecallIndex>>>, AppError> {
        self.metadata_recall_index_cache.lock().map_err(|err| {
            AppError::DuckDb(format!("metadata recall index cache lock poisoned: {err}"))
        })
    }

    #[cfg(test)]
    fn metadata_recall_index_cache_len(&self) -> usize {
        self.metadata_recall_index_cache
            .lock()
            .expect("metadata recall index cache lock")
            .len()
    }

    fn invalidate_metadata_recall_index(&self, chain: &str) -> Result<(), AppError> {
        self.metadata_recall_index_cache()?.remove(chain);
        Ok(())
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
            PRAGMA disable_checkpoint_on_shutdown;
            SET checkpoint_threshold='{}';
            SET auto_checkpoint_skip_wal_threshold={};
            SET write_buffer_row_group_count=1;
            ",
            options.threads.max(1),
            memory_limit,
            temp_directory,
            BULK_IMPORT_CHECKPOINT_THRESHOLD,
            BULK_IMPORT_SKIP_WAL_THRESHOLD_BYTES
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
        let columns = Self::table_columns(conn, "nft_features")?;
        let missing: Vec<&str> = REQUIRED_SNAPSHOT_COLUMNS
            .iter()
            .copied()
            .filter(|column| !columns.contains(*column))
            .collect();
        if !missing.is_empty() {
            return Err(AppError::InvalidData(format!(
                "feature DB nft_features table is missing current required snapshot columns {missing:?}. Rebuild it from a current export-snapshot Parquet file."
            )));
        }

        Ok(())
    }

    fn create_contract_representative_table(conn: &Connection) -> Result<(), AppError> {
        conn.execute_batch(&format!(
            "
            CREATE TABLE IF NOT EXISTS {CONTRACT_REPRESENTATIVE_TABLE} (
                chain VARCHAR NOT NULL,
                contract_address VARCHAR NOT NULL,
                name_feature_rowid BIGINT,
                metadata_feature_rowid BIGINT
            );
            "
        ))?;
        Ok(())
    }

    fn create_prepared_recall_tables(conn: &Connection) -> Result<(), AppError> {
        Self::create_contract_representative_table(conn)?;
        conn.execute_batch(&format!(
            "
            CREATE TABLE IF NOT EXISTS {FEATURE_RECALL_TABLE} (
                chain VARCHAR NOT NULL,
                feature_rowid BIGINT NOT NULL,
                contract_address VARCHAR NOT NULL,
                token_id VARCHAR NOT NULL,
                token_uri_norm VARCHAR,
                image_uri_norm VARCHAR,
                name_norm VARCHAR,
                name_sort VARCHAR
            );
            CREATE TABLE IF NOT EXISTS {METADATA_RECALL_DOC_TABLE} (
                chain VARCHAR NOT NULL,
                feature_rowid BIGINT NOT NULL,
                contract_address VARCHAR NOT NULL,
                token_id VARCHAR NOT NULL,
                token_uri_norm VARCHAR,
                image_uri_norm VARCHAR,
                name_norm VARCHAR,
                recall_doc VARCHAR NOT NULL
            );
            CREATE TABLE IF NOT EXISTS {PREPARED_RECALL_CHAIN_TABLE} (
                chain VARCHAR NOT NULL,
                feature_row_count BIGINT,
                max_feature_rowid BIGINT,
                feature_fingerprint VARCHAR
            );
            "
        ))?;
        Self::ensure_prepared_recall_chain_columns(conn)?;
        Ok(())
    }

    fn ensure_prepared_recall_chain_columns(conn: &Connection) -> Result<(), AppError> {
        let columns = Self::table_columns(conn, PREPARED_RECALL_CHAIN_TABLE)?;
        for (column, definition) in [
            ("feature_row_count", "BIGINT DEFAULT -1"),
            ("max_feature_rowid", "BIGINT DEFAULT -1"),
            ("feature_fingerprint", "VARCHAR DEFAULT ''"),
        ] {
            if !columns.contains(column) {
                conn.execute(
                    &format!(
                        "ALTER TABLE {PREPARED_RECALL_CHAIN_TABLE} ADD COLUMN {column} {definition}"
                    ),
                    [],
                )?;
            }
        }
        Ok(())
    }

    fn table_exists(conn: &Connection, table_name: &str) -> Result<bool, AppError> {
        let exists = conn.query_row(
            "
            SELECT EXISTS(
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = ?
            )
            ",
            params![table_name],
            |row| row.get::<_, bool>(0),
        )?;
        Ok(exists)
    }

    fn table_has_chain(conn: &Connection, table_name: &str, chain: &str) -> Result<bool, AppError> {
        if !Self::table_exists(conn, table_name)? {
            return Ok(false);
        }
        let exists = conn.query_row(
            &format!("SELECT EXISTS(SELECT 1 FROM {table_name} WHERE chain = ? LIMIT 1)"),
            params![chain],
            |row| row.get::<_, bool>(0),
        )?;
        Ok(exists)
    }

    fn prepared_recall_state(
        conn: &Connection,
        chain: &str,
    ) -> Result<PreparedRecallState, AppError> {
        let tables_exist = Self::table_exists(conn, CONTRACT_REPRESENTATIVE_TABLE)?
            && Self::table_exists(conn, FEATURE_RECALL_TABLE)?
            && Self::table_exists(conn, METADATA_RECALL_DOC_TABLE)?
            && Self::table_exists(conn, PREPARED_RECALL_CHAIN_TABLE)?;
        if !tables_exist {
            return Ok(PreparedRecallState { ready: false });
        }
        let prepared_columns = Self::table_columns(conn, PREPARED_RECALL_CHAIN_TABLE)?;
        if !prepared_columns.contains("feature_row_count")
            || !prepared_columns.contains("max_feature_rowid")
            || !prepared_columns.contains("feature_fingerprint")
        {
            return Ok(PreparedRecallState { ready: false });
        }
        let current_stats = Self::feature_chain_stats(conn, chain)?;
        let prepared_stats = Self::prepared_recall_chain_stats(conn, chain)?;
        let recall_row_count = Self::chain_row_count(conn, FEATURE_RECALL_TABLE, chain)?;
        Ok(PreparedRecallState {
            ready: prepared_stats.as_ref() == Some(&current_stats)
                && recall_row_count == current_stats.row_count,
        })
    }

    fn chain_row_count(conn: &Connection, table_name: &str, chain: &str) -> Result<i64, AppError> {
        if !Self::table_exists(conn, table_name)? {
            return Ok(0);
        }
        let count = conn.query_row(
            &format!("SELECT CAST(count(*) AS BIGINT) FROM {table_name} WHERE chain = ?"),
            params![chain],
            |row| row.get::<_, i64>(0),
        )?;
        Ok(count)
    }

    fn feature_chain_stats(conn: &Connection, chain: &str) -> Result<FeatureChainStats, AppError> {
        conn.query_row(
            "
            SELECT
                CAST(count(*) AS BIGINT) AS row_count,
                CAST(coalesce(max(rowid), -1) AS BIGINT) AS max_feature_rowid,
                CAST(coalesce(sum(rowid), -1) AS VARCHAR) AS feature_fingerprint
            FROM nft_features
            WHERE chain = ?
            ",
            params![chain],
            |row| {
                Ok(FeatureChainStats {
                    row_count: row.get(0)?,
                    max_feature_rowid: row.get(1)?,
                    fingerprint: row.get(2)?,
                })
            },
        )
        .map_err(AppError::from)
    }

    fn prepared_recall_chain_stats(
        conn: &Connection,
        chain: &str,
    ) -> Result<Option<FeatureChainStats>, AppError> {
        let mut stmt = conn.prepare(&format!(
            "
            SELECT
                coalesce(feature_row_count, -1),
                coalesce(max_feature_rowid, -1),
                coalesce(feature_fingerprint, '')
            FROM {PREPARED_RECALL_CHAIN_TABLE}
            WHERE chain = ?
            LIMIT 1
            "
        ))?;
        let mut rows = stmt.query_map(params![chain], |row| {
            Ok(FeatureChainStats {
                row_count: row.get(0)?,
                max_feature_rowid: row.get(1)?,
                fingerprint: row.get(2)?,
            })
        })?;
        match rows.next() {
            Some(row) => Ok(Some(row?)),
            None => Ok(None),
        }
    }

    fn refresh_contract_representatives_for_chain(
        conn: &Connection,
        chain: &str,
    ) -> Result<(), AppError> {
        Self::create_contract_representative_table(conn)?;
        conn.execute(
            &format!("DELETE FROM {CONTRACT_REPRESENTATIVE_TABLE} WHERE chain = ?"),
            params![chain],
        )?;
        let sql = format!(
            "
            INSERT INTO {CONTRACT_REPRESENTATIVE_TABLE} (
                chain, contract_address, name_feature_rowid, metadata_feature_rowid
            )
            WITH name_reps AS (
                SELECT chain, contract_address, feature_rowid AS name_feature_rowid
                FROM (
                    SELECT chain,
                           lower(CAST(contract_address AS VARCHAR)) AS contract_address,
                           rowid AS feature_rowid,
                           row_number() OVER (
                               PARTITION BY lower(CAST(contract_address AS VARCHAR))
                               ORDER BY trim(coalesce(CAST(name AS VARCHAR), '')), rowid
                           ) AS name_rank
                    FROM nft_features
                    WHERE chain = ?
                      AND trim(coalesce(CAST(name_norm AS VARCHAR), '')) <> ''
                )
                WHERE name_rank = 1
            ),
            metadata_reps AS (
                SELECT chain, contract_address, feature_rowid AS metadata_feature_rowid
                FROM (
                    SELECT chain,
                           lower(CAST(contract_address AS VARCHAR)) AS contract_address,
                           rowid AS feature_rowid,
                           row_number() OVER (
                               PARTITION BY lower(CAST(contract_address AS VARCHAR))
                               ORDER BY CAST(token_id AS VARCHAR), rowid
                           ) AS metadata_rank
                    FROM nft_features
                    WHERE chain = ?
                      AND {}
                )
                WHERE metadata_rank = 1
            )
            SELECT
                coalesce(name_reps.chain, metadata_reps.chain) AS chain,
                coalesce(name_reps.contract_address, metadata_reps.contract_address) AS contract_address,
                name_reps.name_feature_rowid,
                metadata_reps.metadata_feature_rowid
            FROM name_reps
            FULL OUTER JOIN metadata_reps
              ON name_reps.chain = metadata_reps.chain
             AND name_reps.contract_address = metadata_reps.contract_address
            ",
            Self::sql_metadata_json_eligible_predicate()
        );
        conn.execute(&sql, params![chain, chain])?;
        Ok(())
    }

    fn refresh_feature_recall_rows_for_chain(
        conn: &Connection,
        chain: &str,
    ) -> Result<(), AppError> {
        conn.execute(
            &format!("DELETE FROM {FEATURE_RECALL_TABLE} WHERE chain = ?"),
            params![chain],
        )?;
        let sql = format!(
            "
            INSERT INTO {FEATURE_RECALL_TABLE} (
                chain, feature_rowid, contract_address, token_id, token_uri_norm,
                image_uri_norm, name_norm, name_sort
            )
            SELECT
                chain,
                rowid AS feature_rowid,
                lower(CAST(contract_address AS VARCHAR)) AS contract_address,
                CAST(token_id AS VARCHAR) AS token_id,
                coalesce(CAST(token_uri_norm AS VARCHAR), '') AS token_uri_norm,
                coalesce(CAST(image_uri_norm AS VARCHAR), '') AS image_uri_norm,
                coalesce(CAST(name_norm AS VARCHAR), '') AS name_norm,
                trim(coalesce(CAST(name AS VARCHAR), '')) AS name_sort
            FROM nft_features
            WHERE chain = ?
            "
        );
        conn.execute(&sql, params![chain])?;
        Ok(())
    }

    fn refresh_metadata_recall_docs_for_chain(
        conn: &Connection,
        chain: &str,
    ) -> Result<(), AppError> {
        conn.execute(
            &format!("DELETE FROM {METADATA_RECALL_DOC_TABLE} WHERE chain = ?"),
            params![chain],
        )?;
        let mut insert = conn.prepare(&format!(
            "
            INSERT INTO {METADATA_RECALL_DOC_TABLE} (
                chain, feature_rowid, contract_address, token_id, token_uri_norm,
                image_uri_norm, name_norm, recall_doc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            "
        ))?;
        let mut last_feature_rowid = -1_i64;
        loop {
            let sql = format!(
                "
                SELECT f.rowid, lower(CAST(f.contract_address AS VARCHAR)) AS contract_address,
                       CAST(f.token_id AS VARCHAR) AS token_id,
                       coalesce(CAST(f.token_uri_norm AS VARCHAR), '') AS token_uri_norm,
                       coalesce(CAST(f.image_uri_norm AS VARCHAR), '') AS image_uri_norm,
                       coalesce(CAST(f.name_norm AS VARCHAR), '') AS name_norm,
                       coalesce(CAST(f.metadata_json AS VARCHAR), '') AS metadata_json
                FROM {CONTRACT_REPRESENTATIVE_TABLE} r
                JOIN nft_features f ON f.rowid = r.metadata_feature_rowid
                WHERE r.chain = ?
                  AND r.metadata_feature_rowid IS NOT NULL
                  AND f.rowid > {last_feature_rowid}
                  AND {}
                ORDER BY f.rowid
                LIMIT {PREPARED_METADATA_DOC_BATCH_SIZE}
                ",
                Self::sql_metadata_json_eligible_predicate()
                    .replace("metadata_json", "f.metadata_json")
            );
            let rows = {
                let mut stmt = conn.prepare(&sql)?;
                let rows = stmt.query_map(params![chain], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                    ))
                })?;
                let mut collected = Vec::new();
                for row in rows {
                    collected.push(row?);
                }
                collected
            };
            if rows.is_empty() {
                break;
            }
            let fetched_rows = rows.len();
            for (
                feature_rowid,
                contract_address,
                token_id,
                token_uri_norm,
                image_uri_norm,
                name_norm,
                metadata_json,
            ) in rows
            {
                last_feature_rowid = feature_rowid;
                let recall_doc = metadata_recall_document(&metadata_json);
                if MetadataBm25Document::from_text(&recall_doc).is_none() {
                    continue;
                }
                insert.execute(params![
                    chain,
                    feature_rowid,
                    contract_address,
                    token_id,
                    token_uri_norm,
                    image_uri_norm,
                    name_norm,
                    recall_doc
                ])?;
            }
            if fetched_rows < PREPARED_METADATA_DOC_BATCH_SIZE {
                break;
            }
        }
        Ok(())
    }

    fn refresh_prepared_recall_tables_for_chain(
        conn: &Connection,
        chain: &str,
    ) -> Result<(), AppError> {
        Self::create_prepared_recall_tables(conn)?;
        conn.execute(
            &format!("DELETE FROM {PREPARED_RECALL_CHAIN_TABLE} WHERE chain = ?"),
            params![chain],
        )?;
        Self::refresh_contract_representatives_for_chain(conn, chain)?;
        Self::refresh_feature_recall_rows_for_chain(conn, chain)?;
        Self::refresh_metadata_recall_docs_for_chain(conn, chain)?;
        let stats = Self::feature_chain_stats(conn, chain)?;
        conn.execute(
            &format!(
                "
                INSERT INTO {PREPARED_RECALL_CHAIN_TABLE} (
                    chain, feature_row_count, max_feature_rowid, feature_fingerprint
                ) VALUES (?, ?, ?, ?)
                "
            ),
            params![
                chain,
                stats.row_count,
                stats.max_feature_rowid,
                stats.fingerprint
            ],
        )?;
        Ok(())
    }

    fn ensure_prepared_recall_state(
        &self,
        conn: &Connection,
        chain: &str,
    ) -> Result<PreparedRecallState, AppError> {
        let mut state = Self::prepared_recall_state(conn, chain)?;
        if !state.ready && self.writable && {
            conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM nft_features WHERE chain = ? LIMIT 1)",
                params![chain],
                |row| row.get::<_, bool>(0),
            )?
        } {
            self.invalidate_metadata_recall_index(chain)?;
            Self::refresh_prepared_recall_tables_for_chain(conn, chain)?;
            state = Self::prepared_recall_state(conn, chain)?;
        }
        Ok(state)
    }

    fn drop_obsolete_nft_feature_columns(conn: &Connection) -> Result<(), AppError> {
        let columns = Self::table_columns(conn, "nft_features")?;
        for column in ["metadata_doc", "name_prefix8", "metadata_keywords_arr"] {
            if columns.contains(column) {
                conn.execute(
                    &format!("ALTER TABLE nft_features DROP COLUMN {column}"),
                    [],
                )?;
            }
        }
        Ok(())
    }

    fn table_columns(conn: &Connection, table_name: &str) -> Result<HashSet<String>, AppError> {
        let mut stmt = conn.prepare(&format!("DESCRIBE {table_name}"))?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut columns = HashSet::new();
        for row in rows {
            columns.insert(row?);
        }
        Ok(columns)
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
        self.invalidate_metadata_recall_index(chain)?;
        let conn = self.conn()?;
        conn.execute("DELETE FROM nft_features WHERE chain = ?", params![chain])?;

        let mut stmt = conn.prepare(
            "
            INSERT INTO nft_features (
                chain, contract_address, token_id, token_uri, image_uri, name, symbol, metadata_json,
                token_uri_norm, image_uri_norm, name_norm
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ",
        )?;

        for row in rows {
            let name_norm = normalize_name(&row.name);
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
                name_norm,
            ])?;
        }
        drop(stmt);
        Self::refresh_prepared_recall_tables_for_chain(&conn, chain)?;
        Ok(())
    }

    fn load_parquet_dataset_via_duckdb(
        &self,
        chain: &str,
        parquet_path: &str,
    ) -> Result<(), AppError> {
        self.invalidate_metadata_recall_index(chain)?;
        let path = Self::sql_string_literal(&parquet_path.replace('\\', "/"));
        let insert_sql = format!(
            "
            INSERT INTO nft_features (
                chain, contract_address, token_id, token_uri, image_uri, name, symbol, metadata_json,
                token_uri_norm, image_uri_norm, name_norm
            )
            SELECT
                ? AS chain,
                lower(CAST(contract_address AS VARCHAR)) AS contract_address,
                CAST(token_id AS VARCHAR) AS token_id,
                coalesce(CAST(token_uri AS VARCHAR), '') AS token_uri,
                coalesce(CAST(image_uri AS VARCHAR), '') AS image_uri,
                coalesce(CAST(name AS VARCHAR), '') AS name,
                coalesce(CAST(symbol AS VARCHAR), '') AS symbol,
                coalesce(CAST(metadata_json AS VARCHAR), '') AS metadata_json,
                coalesce(CAST(token_uri_norm AS VARCHAR), '') AS token_uri_norm,
                coalesce(CAST(image_uri_norm AS VARCHAR), '') AS image_uri_norm,
                coalesce(CAST(name_norm AS VARCHAR), '') AS name_norm
            FROM read_parquet({path})
            ",
        );
        let conn = self.conn()?;
        conn.execute("DELETE FROM nft_features WHERE chain = ?", params![chain])?;
        conn.execute(&insert_sql, params![chain])?;
        Self::refresh_prepared_recall_tables_for_chain(&conn, chain)?;
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

        let missing: Vec<&str> = REQUIRED_SNAPSHOT_COLUMNS
            .iter()
            .copied()
            .filter(|column| !column_names.contains(*column))
            .collect();
        if !missing.is_empty() {
            return Err(AppError::InvalidData(format!(
                "Parquet file {parquet_path:?} is missing required snapshot columns {missing:?}. Re-export the snapshot with the current export-snapshot command."
            )));
        }

        self.load_parquet_dataset_via_duckdb(chain, parquet_path)
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

    fn sql_metadata_json_eligible_predicate() -> String {
        format!(
            "length(trim(coalesce(CAST(metadata_json AS VARCHAR), ''))) <= {MAX_METADATA_BYTES_FOR_DEDUP} \
             AND (trim(coalesce(CAST(metadata_json AS VARCHAR), '')) LIKE '{{%' \
             OR trim(coalesce(CAST(metadata_json AS VARCHAR), '')) LIKE '[%')"
        )
    }

    fn build_metadata_source_index(candidates: &[MetadataRecallCandidate]) -> MetadataSourceIndex {
        let mut index = MetadataSourceIndex {
            anchor_indices: HashMap::new(),
            simhash_band_indices: vec![
                Vec::new();
                METADATA_SIMHASH_BAND_COUNT * METADATA_SIMHASH_BAND_VALUES
            ],
        };
        for (candidate_index, candidate) in candidates.iter().enumerate() {
            if candidate.sketch.simhash == 0 && candidate.sketch.anchors.is_empty() {
                continue;
            }
            for anchor in &candidate.sketch.anchors {
                index
                    .anchor_indices
                    .entry(anchor.clone())
                    .or_default()
                    .push(candidate_index);
            }
            for band_index in 0..METADATA_SIMHASH_BAND_COUNT {
                let band_value = metadata_simhash_band_value(candidate.sketch.simhash, band_index);
                index.simhash_band_indices[metadata_simhash_band_key(band_index, band_value)]
                    .push(candidate_index);
            }
        }
        index
    }

    fn load_metadata_recall_index(
        conn: &Connection,
        chain: &str,
        prepared_recall_state: PreparedRecallState,
    ) -> Result<MetadataRecallIndex, AppError> {
        let sql = if prepared_recall_state.ready {
            format!(
                "
                SELECT feature_rowid, contract_address, token_id, token_uri_norm, image_uri_norm,
                       name_norm, recall_doc
                FROM {METADATA_RECALL_DOC_TABLE}
                WHERE chain = ?
                ORDER BY feature_rowid
                "
            )
        } else if Self::table_has_chain(conn, CONTRACT_REPRESENTATIVE_TABLE, chain)? {
            format!(
                "
                SELECT f.rowid, lower(CAST(f.contract_address AS VARCHAR)) AS contract_address,
                       CAST(f.token_id AS VARCHAR) AS token_id,
                       coalesce(CAST(f.token_uri_norm AS VARCHAR), '') AS token_uri_norm,
                       coalesce(CAST(f.image_uri_norm AS VARCHAR), '') AS image_uri_norm,
                       coalesce(CAST(f.name_norm AS VARCHAR), '') AS name_norm,
                       coalesce(CAST(f.metadata_json AS VARCHAR), '') AS metadata_json
                FROM {CONTRACT_REPRESENTATIVE_TABLE} r
                JOIN nft_features f ON f.rowid = r.metadata_feature_rowid
                WHERE r.chain = ?
                  AND r.metadata_feature_rowid IS NOT NULL
                  AND {}
                ORDER BY f.rowid
                ",
                Self::sql_metadata_json_eligible_predicate()
                    .replace("metadata_json", "f.metadata_json")
            )
        } else {
            format!(
                "
                SELECT rowid, contract_address, token_id, token_uri_norm, image_uri_norm, name_norm,
                       metadata_json
                FROM (
                    SELECT rowid,
                           lower(CAST(contract_address AS VARCHAR)) AS contract_address,
                           CAST(token_id AS VARCHAR) AS token_id,
                           coalesce(CAST(token_uri_norm AS VARCHAR), '') AS token_uri_norm,
                           coalesce(CAST(image_uri_norm AS VARCHAR), '') AS image_uri_norm,
                           coalesce(CAST(name_norm AS VARCHAR), '') AS name_norm,
                           coalesce(CAST(metadata_json AS VARCHAR), '') AS metadata_json,
                           row_number() OVER (
                               PARTITION BY lower(CAST(contract_address AS VARCHAR))
                               ORDER BY CAST(token_id AS VARCHAR), rowid
                           ) AS metadata_rank
                    FROM nft_features
                    WHERE chain = ?
                      AND {}
                )
                WHERE metadata_rank = 1
                ORDER BY rowid
                ",
                Self::sql_metadata_json_eligible_predicate()
            )
        };
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![chain], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
            ))
        })?;

        let mut candidates = Vec::new();
        let mut corpus_builder = MetadataBm25CorpusBuilder::default();
        let mut doc_freqs = HashMap::<String, usize>::new();
        for row in rows {
            let (
                feature_rowid,
                contract_address,
                token_id,
                token_uri_norm,
                image_uri_norm,
                name_norm,
                metadata_source_doc,
            ) = row?;
            let recall_doc = if prepared_recall_state.ready {
                metadata_source_doc
            } else {
                metadata_recall_document(&metadata_source_doc)
            };
            let Some(doc) = MetadataBm25Document::from_text(&recall_doc) else {
                continue;
            };
            corpus_builder.add_tokens(doc.tokens());
            for token in doc.tokens().iter().collect::<HashSet<_>>() {
                *doc_freqs.entry((*token).clone()).or_insert(0) += 1;
            }
            candidates.push(MetadataRecallCandidate {
                row: RecallRow {
                    feature_rowid,
                    contract_address,
                    token_id,
                    token_uri_norm,
                    image_uri_norm,
                    name_norm,
                    metadata_recall_match: true,
                },
                doc,
                sketch: MetadataSketch::default(),
            });
        }
        let corpus = corpus_builder.finish();
        let total_docs = candidates.len();
        for candidate in &mut candidates {
            candidate.sketch =
                metadata_sketch_from_document(&candidate.doc, total_docs, &doc_freqs);
        }
        let source_index = Self::build_metadata_source_index(&candidates);
        Ok(MetadataRecallIndex {
            candidates,
            corpus,
            doc_freqs,
            source_index,
        })
    }

    fn cached_metadata_recall_index(
        &self,
        conn: &Connection,
        chain: &str,
        prepared_recall_state: PreparedRecallState,
    ) -> Result<Arc<MetadataRecallIndex>, AppError> {
        if let Some(index) = self.metadata_recall_index_cache()?.get(chain).cloned() {
            return Ok(index);
        }

        let index = Arc::new(Self::load_metadata_recall_index(
            conn,
            chain,
            prepared_recall_state,
        )?);
        self.metadata_recall_index_cache()?
            .insert(chain.to_string(), Arc::clone(&index));
        Ok(index)
    }

    fn estimate_metadata_source_bucket_hits(
        seed_sketch: &MetadataSketch,
        source_index: &MetadataSourceIndex,
        hamming_threshold: u32,
        cap: usize,
    ) -> usize {
        if cap == 0 {
            return 0;
        }
        let mut seen = HashSet::new();
        for anchor in &seed_sketch.anchors {
            if let Some(indices) = source_index.anchor_indices.get(anchor) {
                for index in indices {
                    seen.insert(*index);
                    if seen.len() >= cap {
                        return cap;
                    }
                }
            }
        }
        let band_radius = hamming_threshold / METADATA_SIMHASH_BAND_COUNT as u32;
        for band_index in 0..METADATA_SIMHASH_BAND_COUNT {
            let seed_band = metadata_simhash_band_value(seed_sketch.simhash, band_index);
            for band_value in 0..METADATA_SIMHASH_BAND_VALUES {
                let band_value = band_value as u8;
                if (seed_band ^ band_value).count_ones() > band_radius {
                    continue;
                }
                let band_key = metadata_simhash_band_key(band_index, band_value);
                if let Some(indices) = source_index.simhash_band_indices.get(band_key) {
                    for index in indices {
                        seen.insert(*index);
                        if seen.len() >= cap {
                            return cap;
                        }
                    }
                }
            }
        }
        seen.len()
    }

    fn metadata_source_candidate_indices(
        seed_sketch: &MetadataSketch,
        metadata_index: &MetadataRecallIndex,
        seed_contracts: &HashSet<String>,
    ) -> Vec<usize> {
        if seed_sketch.simhash == 0 && seed_sketch.anchors.is_empty() {
            return Vec::new();
        }
        let use_full_scan = metadata_index.source_index.anchor_indices.is_empty()
            || metadata_index
                .source_index
                .simhash_band_indices
                .iter()
                .all(Vec::is_empty)
            || Self::estimate_metadata_source_bucket_hits(
                seed_sketch,
                &metadata_index.source_index,
                METADATA_SKETCH_SOURCE_HAMMING_THRESHOLD,
                metadata_index.candidates.len(),
            ) >= metadata_index.candidates.len();

        if use_full_scan {
            return metadata_index
                .candidates
                .iter()
                .enumerate()
                .filter_map(|(index, candidate)| {
                    (!seed_contracts.contains(&candidate.row.contract_address)
                        && metadata_sketch_source_match(
                            seed_sketch,
                            &candidate.sketch,
                            METADATA_SKETCH_SOURCE_HAMMING_THRESHOLD,
                        ))
                    .then_some(index)
                })
                .collect();
        }

        let mut seen = HashSet::new();
        let mut indices = Vec::new();
        for anchor in &seed_sketch.anchors {
            let Some(anchor_indices) = metadata_index.source_index.anchor_indices.get(anchor)
            else {
                continue;
            };
            for index in anchor_indices {
                Self::push_metadata_source_candidate_index(
                    *index,
                    seed_sketch,
                    metadata_index,
                    seed_contracts,
                    &mut seen,
                    &mut indices,
                );
            }
        }
        let band_radius =
            METADATA_SKETCH_SOURCE_HAMMING_THRESHOLD / METADATA_SIMHASH_BAND_COUNT as u32;
        for band_index in 0..METADATA_SIMHASH_BAND_COUNT {
            let seed_band = metadata_simhash_band_value(seed_sketch.simhash, band_index);
            for band_value in 0..METADATA_SIMHASH_BAND_VALUES {
                let band_value = band_value as u8;
                if (seed_band ^ band_value).count_ones() > band_radius {
                    continue;
                }
                let band_key = metadata_simhash_band_key(band_index, band_value);
                let Some(bucket_indices) = metadata_index
                    .source_index
                    .simhash_band_indices
                    .get(band_key)
                else {
                    continue;
                };
                for index in bucket_indices {
                    Self::push_metadata_source_candidate_index(
                        *index,
                        seed_sketch,
                        metadata_index,
                        seed_contracts,
                        &mut seen,
                        &mut indices,
                    );
                }
            }
        }
        indices.sort_unstable();
        indices
    }

    fn push_metadata_source_candidate_index(
        index: usize,
        seed_sketch: &MetadataSketch,
        metadata_index: &MetadataRecallIndex,
        seed_contracts: &HashSet<String>,
        seen: &mut HashSet<usize>,
        output: &mut Vec<usize>,
    ) {
        if !seen.insert(index) {
            return;
        }
        let Some(candidate) = metadata_index.candidates.get(index) else {
            return;
        };
        if seed_contracts.contains(&candidate.row.contract_address) {
            return;
        }
        if metadata_sketch_source_match(
            seed_sketch,
            &candidate.sketch,
            METADATA_SKETCH_SOURCE_HAMMING_THRESHOLD,
        ) {
            output.push(index);
        }
    }

    fn prepare_seed_value_table(
        conn: &Connection,
        table_name: &str,
        values: &HashSet<String>,
    ) -> Result<(), AppError> {
        conn.execute_batch(&format!(
            "
            DROP TABLE IF EXISTS {table_name};
            CREATE TEMP TABLE {table_name} (
                value VARCHAR NOT NULL
            );
            "
        ))?;
        if values.is_empty() {
            return Ok(());
        }

        let mut stmt = conn.prepare(&format!("INSERT INTO {table_name} (value) VALUES (?)"))?;
        let values = values
            .iter()
            .filter(|value| !value.is_empty())
            .collect::<BTreeSet<_>>();
        for value in values {
            stmt.execute(params![value])?;
        }
        Ok(())
    }

    fn drop_seed_temp_tables(conn: &Connection) -> Result<(), AppError> {
        conn.execute_batch(&format!(
            "
            DROP TABLE IF EXISTS {SEED_TOKEN_URI_TABLE};
            DROP TABLE IF EXISTS {SEED_IMAGE_URI_TABLE};
            DROP TABLE IF EXISTS {SEED_TOKEN_ID_TABLE};
            DROP TABLE IF EXISTS {CANDIDATE_CONTRACT_TABLE};
            DROP TABLE IF EXISTS {SELECTED_RECALL_ROWID_TABLE};
            "
        ))?;
        Ok(())
    }

    fn create_selected_recall_rowid_table(conn: &Connection) -> Result<(), AppError> {
        conn.execute_batch(&format!(
            "
            DROP TABLE IF EXISTS {SELECTED_RECALL_ROWID_TABLE};
            CREATE TEMP TABLE {SELECTED_RECALL_ROWID_TABLE} (
                feature_rowid BIGINT NOT NULL
            );
            "
        ))?;
        Ok(())
    }

    fn replace_selected_recall_rowids(conn: &Connection, rowids: &[i64]) -> Result<(), AppError> {
        conn.execute(&format!("DELETE FROM {SELECTED_RECALL_ROWID_TABLE}"), [])?;
        let mut stmt = conn.prepare(&format!(
            "INSERT INTO {SELECTED_RECALL_ROWID_TABLE} (feature_rowid) VALUES (?)"
        ))?;
        let unique_rowids = rowids.iter().copied().collect::<BTreeSet<_>>();
        for rowid in unique_rowids {
            stmt.execute(params![rowid])?;
        }
        Ok(())
    }

    fn seed_row_match(
        profile: &SeedRecallProfile,
        row: &RecallRow,
        name_threshold: f64,
        metadata_recall_match: bool,
    ) -> SeedRowMatch {
        let name_match = profile
            .seed_name_norms
            .iter()
            .any(|seed_name| score_name_pair(&row.name_norm, seed_name) >= name_threshold);
        SeedRowMatch {
            token_uri_match: profile.exact_token_keys.contains(&row.token_uri_norm),
            image_uri_match: profile.exact_image_keys.contains(&row.image_uri_norm),
            name_prefix_match: !row.name_norm.is_empty() && name_match,
            metadata_recall_match,
        }
    }

    fn can_select_recall_row(
        seed_index: usize,
        profile: &SeedRecallProfile,
        row: &RecallRow,
        accumulators: &BTreeMap<String, SnapshotAccumulator>,
        pending_contract_counts: &HashMap<(usize, String), usize>,
        max_tokens_per_contract: usize,
    ) -> bool {
        if profile.seed_contracts.contains(&row.contract_address) {
            return false;
        }
        let Some(accumulator) = accumulators.get(&profile.seed_address) else {
            return false;
        };
        if accumulator.seen_feature_rowids.contains(&row.feature_rowid) {
            return false;
        }
        if max_tokens_per_contract == 0 {
            return true;
        }
        let accepted = accumulator
            .per_contract_counts
            .get(&row.contract_address)
            .copied()
            .unwrap_or_default();
        let pending = pending_contract_counts
            .get(&(seed_index, row.contract_address.clone()))
            .copied()
            .unwrap_or_default();
        accepted + pending < max_tokens_per_contract
    }

    fn note_pending_recall_row(
        seed_index: usize,
        row: &RecallRow,
        pending_contract_counts: &mut HashMap<(usize, String), usize>,
    ) {
        *pending_contract_counts
            .entry((seed_index, row.contract_address.clone()))
            .or_default() += 1;
    }

    fn can_stage_metadata_recall_row(
        profile: &SeedRecallProfile,
        row: &RecallRow,
        accumulators: &BTreeMap<String, SnapshotAccumulator>,
        max_tokens_per_contract: usize,
    ) -> bool {
        if profile.seed_contracts.contains(&row.contract_address) {
            return false;
        }
        let Some(accumulator) = accumulators.get(&profile.seed_address) else {
            return false;
        };
        if accumulator.seen_feature_rowids.contains(&row.feature_rowid) {
            return false;
        }
        let accepted = accumulator
            .per_contract_counts
            .get(&row.contract_address)
            .copied()
            .unwrap_or_default();
        accepted < max_tokens_per_contract
    }

    fn stage_metadata_recall_row(
        pending_rows: &mut HashMap<(usize, String), Vec<PendingMetadataRecallRow>>,
        seed_index: usize,
        row: &RecallRow,
        row_match: SeedRowMatch,
        max_tokens_per_contract: usize,
    ) {
        if max_tokens_per_contract == 0 {
            return;
        }
        let entry = pending_rows
            .entry((seed_index, row.contract_address.clone()))
            .or_default();
        if entry
            .iter()
            .any(|pending| pending.row.feature_rowid == row.feature_rowid)
        {
            return;
        }
        entry.push(PendingMetadataRecallRow {
            seed_index,
            row: row.clone(),
            row_match,
        });
        entry.sort_by(|left, right| {
            (&left.row.token_id, left.row.feature_rowid)
                .cmp(&(&right.row.token_id, right.row.feature_rowid))
        });
        entry.truncate(max_tokens_per_contract);
    }

    fn drain_selected_recall_rows(
        conn: &Connection,
        profiles: &[SeedRecallProfile],
        batch_rows: &[RecallRow],
        selected_rows: &mut Vec<SelectedRecallRow>,
        accumulators: &mut BTreeMap<String, SnapshotAccumulator>,
        max_tokens_per_contract: usize,
    ) -> Result<(), AppError> {
        if selected_rows.is_empty() {
            return Ok(());
        }

        let selected_rowids = selected_rows
            .iter()
            .map(|selected| batch_rows[selected.row_index].feature_rowid)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let full_records_by_rowid = Self::fetch_records_by_feature_rowid(conn, &selected_rowids)?;

        for selected in selected_rows.drain(..) {
            let Some(row) = batch_rows.get(selected.row_index) else {
                continue;
            };
            let Some(record) = full_records_by_rowid.get(&row.feature_rowid) else {
                continue;
            };
            let Some(profile) = profiles.get(selected.seed_index) else {
                continue;
            };
            let Some(accumulator) = accumulators.get_mut(&profile.seed_address) else {
                continue;
            };
            accumulator.push_recall_row(
                profile,
                row,
                record.clone(),
                &selected.row_match,
                max_tokens_per_contract,
            );
        }

        Ok(())
    }

    fn drain_pending_metadata_recall_rows(
        conn: &Connection,
        profiles: &[SeedRecallProfile],
        pending_rows: &mut HashMap<(usize, String), Vec<PendingMetadataRecallRow>>,
        accumulators: &mut BTreeMap<String, SnapshotAccumulator>,
        max_tokens_per_contract: usize,
    ) -> Result<(), AppError> {
        if pending_rows.is_empty() {
            return Ok(());
        }

        let mut rows = pending_rows
            .drain()
            .flat_map(|(_, rows)| rows)
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            (
                left.seed_index,
                &left.row.contract_address,
                &left.row.token_id,
                left.row.feature_rowid,
            )
                .cmp(&(
                    right.seed_index,
                    &right.row.contract_address,
                    &right.row.token_id,
                    right.row.feature_rowid,
                ))
        });

        for chunk in rows.chunks(SELECTED_RECALL_ROWID_CHUNK_SIZE) {
            let rowids = chunk
                .iter()
                .map(|pending| pending.row.feature_rowid)
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            let full_records_by_rowid = Self::fetch_records_by_feature_rowid(conn, &rowids)?;
            for pending in chunk {
                let Some(record) = full_records_by_rowid.get(&pending.row.feature_rowid) else {
                    continue;
                };
                let Some(profile) = profiles.get(pending.seed_index) else {
                    continue;
                };
                let Some(accumulator) = accumulators.get_mut(&profile.seed_address) else {
                    continue;
                };
                accumulator.push_recall_row(
                    profile,
                    &pending.row,
                    record.clone(),
                    &pending.row_match,
                    max_tokens_per_contract,
                );
            }
        }

        Ok(())
    }

    fn drain_owned_recall_rows(
        conn: &Connection,
        profiles: &[SeedRecallProfile],
        rows: &mut Vec<PendingMetadataRecallRow>,
        accumulators: &mut BTreeMap<String, SnapshotAccumulator>,
        max_tokens_per_contract: usize,
    ) -> Result<(), AppError> {
        if rows.is_empty() {
            return Ok(());
        }
        rows.sort_by(|left, right| {
            (
                left.seed_index,
                &left.row.contract_address,
                &left.row.token_id,
                left.row.feature_rowid,
            )
                .cmp(&(
                    right.seed_index,
                    &right.row.contract_address,
                    &right.row.token_id,
                    right.row.feature_rowid,
                ))
        });
        for chunk in rows.chunks(SELECTED_RECALL_ROWID_CHUNK_SIZE) {
            let rowids = chunk
                .iter()
                .map(|pending| pending.row.feature_rowid)
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            let full_records_by_rowid = Self::fetch_records_by_feature_rowid(conn, &rowids)?;
            for pending in chunk {
                let Some(record) = full_records_by_rowid.get(&pending.row.feature_rowid) else {
                    continue;
                };
                let Some(profile) = profiles.get(pending.seed_index) else {
                    continue;
                };
                let Some(accumulator) = accumulators.get_mut(&profile.seed_address) else {
                    continue;
                };
                accumulator.push_recall_row(
                    profile,
                    &pending.row,
                    record.clone(),
                    &pending.row_match,
                    max_tokens_per_contract,
                );
            }
        }
        rows.clear();
        Ok(())
    }

    fn fetch_records_by_feature_rowid(
        conn: &Connection,
        rowids: &[i64],
    ) -> Result<HashMap<i64, DatabaseNftRecord>, AppError> {
        if rowids.is_empty() {
            return Ok(HashMap::new());
        }
        let mut records = HashMap::new();
        for chunk in rowids.chunks(SELECTED_RECALL_ROWID_CHUNK_SIZE) {
            if chunk.is_empty() {
                continue;
            }
            Self::replace_selected_recall_rowids(conn, chunk)?;
            let sql = format!(
                "
                SELECT f.rowid, f.contract_address, f.token_id, coalesce(f.token_uri, ''),
                       coalesce(f.image_uri, ''), coalesce(f.name, ''), coalesce(f.symbol, ''),
                       coalesce(f.metadata_json, '')
                FROM nft_features f
                JOIN {SELECTED_RECALL_ROWID_TABLE} selected
                  ON selected.feature_rowid = f.rowid
                ORDER BY f.rowid
                "
            );
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    DatabaseNftRecord {
                        contract_address: row.get::<_, String>(1)?,
                        token_id: row.get::<_, String>(2)?,
                        token_uri: row.get::<_, String>(3)?,
                        image_uri: row.get::<_, String>(4)?,
                        name: row.get::<_, String>(5)?,
                        symbol: row.get::<_, String>(6)?,
                        metadata_json: row.get::<_, String>(7)?,
                        metadata_recall_checked: false,
                        metadata_recall_match: false,
                    },
                ))
            })?;
            for row in rows {
                let (rowid, record) = row?;
                records.insert(rowid, record);
            }
        }
        Ok(records)
    }

    fn append_exact_uri_recall_rows(
        conn: &Connection,
        input: ExactUriRecallInput<'_>,
        accumulators: &mut BTreeMap<String, SnapshotAccumulator>,
    ) -> Result<(), AppError> {
        let ExactUriRecallInput {
            chain,
            profiles,
            profile_index,
            all_token_keys,
            all_image_keys,
            name_threshold,
            max_tokens_per_contract,
            max_recall_rows,
            prepared_recall_state,
        } = input;
        if all_token_keys.is_empty() && all_image_keys.is_empty() {
            return Ok(());
        }

        Self::prepare_seed_value_table(conn, SEED_TOKEN_URI_TABLE, all_token_keys)?;
        Self::prepare_seed_value_table(conn, SEED_IMAGE_URI_TABLE, all_image_keys)?;

        let token_uri_match_expr = if all_token_keys.is_empty() {
            "FALSE".to_string()
        } else {
            format!("token_uri_norm IN (SELECT value FROM {SEED_TOKEN_URI_TABLE})")
        };
        let image_uri_match_expr = if all_image_keys.is_empty() {
            "FALSE".to_string()
        } else {
            format!("image_uri_norm IN (SELECT value FROM {SEED_IMAGE_URI_TABLE})")
        };
        let exact_uri_predicate = format!("({token_uri_match_expr}) OR ({image_uri_match_expr})");
        let recall_batch_size = if max_recall_rows == 0 {
            DEFAULT_RECALL_BATCH_SIZE
        } else {
            max_recall_rows
        };

        let mut last_feature_rowid = -1_i64;
        loop {
            let select_sql = if prepared_recall_state.ready {
                format!(
                    "
                    SELECT feature_rowid,
                           contract_address,
                           token_id,
                           coalesce(token_uri_norm, '') AS token_uri_norm,
                           coalesce(image_uri_norm, '') AS image_uri_norm,
                           coalesce(name_norm, '') AS name_norm
                    FROM {FEATURE_RECALL_TABLE}
                    WHERE chain = ?
                      AND ({exact_uri_predicate})
                      AND feature_rowid > {last_feature_rowid}
                    ORDER BY feature_rowid
                    LIMIT {recall_batch_size}
                    "
                )
            } else {
                format!(
                    "
                    SELECT rowid AS feature_rowid,
                           lower(CAST(contract_address AS VARCHAR)) AS contract_address,
                           CAST(token_id AS VARCHAR) AS token_id,
                           coalesce(CAST(token_uri_norm AS VARCHAR), '') AS token_uri_norm,
                           coalesce(CAST(image_uri_norm AS VARCHAR), '') AS image_uri_norm,
                           coalesce(CAST(name_norm AS VARCHAR), '') AS name_norm
                    FROM nft_features
                    WHERE chain = ?
                      AND ({exact_uri_predicate})
                      AND rowid > {last_feature_rowid}
                    ORDER BY rowid
                    LIMIT {recall_batch_size}
                    "
                )
            };
            let mut stmt = conn.prepare(&select_sql)?;
            let rows = stmt.query_map(params![chain], |row| {
                Ok(RecallRow {
                    feature_rowid: row.get::<_, i64>(0)?,
                    contract_address: row.get::<_, String>(1)?,
                    token_id: row.get::<_, String>(2)?,
                    token_uri_norm: row.get::<_, String>(3)?,
                    image_uri_norm: row.get::<_, String>(4)?,
                    name_norm: row.get::<_, String>(5)?,
                    metadata_recall_match: false,
                })
            })?;

            let mut fetched_rows = 0usize;
            let mut last_seen_feature_rowid = last_feature_rowid;
            let mut batch_rows = Vec::new();
            for row in rows {
                fetched_rows += 1;
                let row = row?;
                last_seen_feature_rowid = row.feature_rowid;
                batch_rows.push(row);
            }
            drop(stmt);

            batch_rows.sort_by(|left, right| {
                (&left.contract_address, &left.token_id, left.feature_rowid).cmp(&(
                    &right.contract_address,
                    &right.token_id,
                    right.feature_rowid,
                ))
            });

            let mut selected_rows = Vec::with_capacity(SELECTED_RECALL_ROWID_CHUNK_SIZE);
            let mut pending_contract_counts = HashMap::new();
            for (row_index, row) in batch_rows.iter().enumerate() {
                let mut seed_indices = profile_index.strong_match_profiles(row);
                seed_indices.sort_unstable();
                for seed_index in seed_indices {
                    let Some(profile) = profiles.get(seed_index) else {
                        continue;
                    };
                    let row_match = Self::seed_row_match(profile, row, name_threshold, false);
                    if !(row_match.token_uri_match || row_match.image_uri_match) {
                        continue;
                    }
                    if !Self::can_select_recall_row(
                        seed_index,
                        profile,
                        row,
                        accumulators,
                        &pending_contract_counts,
                        max_tokens_per_contract,
                    ) {
                        continue;
                    }

                    selected_rows.push(SelectedRecallRow {
                        seed_index,
                        row_index,
                        row_match,
                    });
                    Self::note_pending_recall_row(seed_index, row, &mut pending_contract_counts);
                    if selected_rows.len() >= SELECTED_RECALL_ROWID_CHUNK_SIZE {
                        Self::drain_selected_recall_rows(
                            conn,
                            profiles,
                            &batch_rows,
                            &mut selected_rows,
                            accumulators,
                            max_tokens_per_contract,
                        )?;
                        pending_contract_counts.clear();
                    }
                }
            }
            Self::drain_selected_recall_rows(
                conn,
                profiles,
                &batch_rows,
                &mut selected_rows,
                accumulators,
                max_tokens_per_contract,
            )?;

            if fetched_rows < recall_batch_size {
                break;
            }
            last_feature_rowid = last_seen_feature_rowid;
        }

        Ok(())
    }

    fn append_name_recall_rows(
        conn: &Connection,
        chain: &str,
        profiles: &[SeedRecallProfile],
        name_threshold: f64,
        accumulators: &mut BTreeMap<String, SnapshotAccumulator>,
        max_tokens_per_contract: usize,
        prepared_recall_state: PreparedRecallState,
    ) -> Result<(), AppError> {
        if name_threshold > 100.0
            || !profiles
                .iter()
                .any(|profile| !profile.seed_name_norms.is_empty())
        {
            return Ok(());
        }
        let sql = if prepared_recall_state.ready {
            format!(
                "
                SELECT rr.feature_rowid,
                       rr.contract_address,
                       rr.token_id,
                       coalesce(rr.token_uri_norm, '') AS token_uri_norm,
                       coalesce(rr.image_uri_norm, '') AS image_uri_norm,
                       coalesce(rr.name_norm, '') AS name_norm
                FROM {CONTRACT_REPRESENTATIVE_TABLE} r
                JOIN {FEATURE_RECALL_TABLE} rr
                  ON rr.chain = r.chain
                 AND rr.feature_rowid = r.name_feature_rowid
                WHERE r.chain = ?
                  AND r.name_feature_rowid IS NOT NULL
                  AND trim(coalesce(rr.name_norm, '')) <> ''
                ORDER BY rr.feature_rowid
                "
            )
        } else if Self::table_has_chain(conn, CONTRACT_REPRESENTATIVE_TABLE, chain)? {
            format!(
                "
                SELECT f.rowid,
                       lower(CAST(f.contract_address AS VARCHAR)) AS contract_address,
                       CAST(f.token_id AS VARCHAR) AS token_id,
                       coalesce(CAST(f.token_uri_norm AS VARCHAR), '') AS token_uri_norm,
                       coalesce(CAST(f.image_uri_norm AS VARCHAR), '') AS image_uri_norm,
                       coalesce(CAST(f.name_norm AS VARCHAR), '') AS name_norm
                FROM {CONTRACT_REPRESENTATIVE_TABLE} r
                JOIN nft_features f ON f.rowid = r.name_feature_rowid
                WHERE r.chain = ?
                  AND r.name_feature_rowid IS NOT NULL
                  AND trim(coalesce(CAST(f.name_norm AS VARCHAR), '')) <> ''
                ORDER BY f.rowid
                "
            )
        } else {
            "
                SELECT rowid, contract_address, token_id, token_uri_norm, image_uri_norm, name_norm
                FROM (
                    SELECT rowid,
                           lower(CAST(contract_address AS VARCHAR)) AS contract_address,
                           CAST(token_id AS VARCHAR) AS token_id,
                           coalesce(CAST(token_uri_norm AS VARCHAR), '') AS token_uri_norm,
                           coalesce(CAST(image_uri_norm AS VARCHAR), '') AS image_uri_norm,
                           coalesce(CAST(name_norm AS VARCHAR), '') AS name_norm,
                           row_number() OVER (
                               PARTITION BY lower(CAST(contract_address AS VARCHAR))
                               ORDER BY trim(coalesce(CAST(name AS VARCHAR), '')), rowid
                           ) AS name_rank
                    FROM nft_features
                    WHERE chain = ?
                      AND trim(coalesce(CAST(name_norm AS VARCHAR), '')) <> ''
                )
                WHERE name_rank = 1
                ORDER BY rowid
                "
            .to_string()
        };
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![chain], |row| {
            Ok(RecallRow {
                feature_rowid: row.get::<_, i64>(0)?,
                contract_address: row.get::<_, String>(1)?,
                token_id: row.get::<_, String>(2)?,
                token_uri_norm: row.get::<_, String>(3)?,
                image_uri_norm: row.get::<_, String>(4)?,
                name_norm: row.get::<_, String>(5)?,
                metadata_recall_match: false,
            })
        })?;

        let mut selected_rows = Vec::new();
        let mut pending_contract_counts = HashMap::new();
        for row in rows {
            let row = row?;
            for (seed_index, profile) in profiles.iter().enumerate() {
                if !profile
                    .seed_name_norms
                    .iter()
                    .any(|seed_name| score_name_pair(&row.name_norm, seed_name) >= name_threshold)
                {
                    continue;
                }
                let row_match = Self::seed_row_match(profile, &row, name_threshold, false);
                if !row_match.name_prefix_match {
                    continue;
                }
                if !Self::can_select_recall_row(
                    seed_index,
                    profile,
                    &row,
                    accumulators,
                    &pending_contract_counts,
                    max_tokens_per_contract,
                ) {
                    continue;
                }
                selected_rows.push(PendingMetadataRecallRow {
                    seed_index,
                    row: row.clone(),
                    row_match,
                });
                Self::note_pending_recall_row(seed_index, &row, &mut pending_contract_counts);
                if selected_rows.len() >= SELECTED_RECALL_ROWID_CHUNK_SIZE {
                    Self::drain_owned_recall_rows(
                        conn,
                        profiles,
                        &mut selected_rows,
                        accumulators,
                        max_tokens_per_contract,
                    )?;
                    pending_contract_counts.clear();
                }
            }
        }
        Self::drain_owned_recall_rows(
            conn,
            profiles,
            &mut selected_rows,
            accumulators,
            max_tokens_per_contract,
        )
    }

    fn append_metadata_recall_rows(
        conn: &Connection,
        profiles: &[SeedRecallProfile],
        metadata_threshold: f64,
        metadata_index: &MetadataRecallIndex,
        accumulators: &mut BTreeMap<String, SnapshotAccumulator>,
        max_tokens_per_contract: usize,
    ) -> Result<(), AppError> {
        if metadata_index.candidates.is_empty() {
            return Ok(());
        }
        let mut pending_metadata_rows = HashMap::new();
        let mut selected_rows = Vec::new();
        for (seed_index, profile) in profiles.iter().enumerate() {
            let Some(seed_doc) = profile.seed_metadata_doc.as_ref() else {
                continue;
            };
            let Some(seed_doc) = metadata_seed_doc_for_index(seed_doc, metadata_index) else {
                continue;
            };
            let seed_sketch = metadata_sketch_from_document(
                &seed_doc,
                metadata_index.candidates.len(),
                &metadata_index.doc_freqs,
            );
            let seed_query = MetadataBm25Query::new(&seed_doc, &metadata_index.corpus);
            let candidate_indices = Self::metadata_source_candidate_indices(
                &seed_sketch,
                metadata_index,
                &profile.seed_contracts,
            );
            for candidate_index in candidate_indices {
                let Some(candidate) = metadata_index.candidates.get(candidate_index) else {
                    continue;
                };
                if !seed_query.has_term_overlap(&candidate.doc)
                    || score_metadata_indexed_pair_with_query(&seed_query, &candidate.doc)
                        < metadata_threshold
                {
                    continue;
                }
                let mut row = candidate.row.clone();
                row.metadata_recall_match = true;
                if let Some(accumulator) = accumulators.get_mut(&profile.seed_address) {
                    if accumulator.mark_selected_metadata_recall(&row) {
                        continue;
                    }
                }
                let row_match = Self::seed_row_match(profile, &row, 101.0, true);
                let strong_match = row_match.token_uri_match
                    || row_match.image_uri_match
                    || row_match.name_prefix_match;
                if strong_match {
                    continue;
                }
                if max_tokens_per_contract > 0 {
                    if !Self::can_stage_metadata_recall_row(
                        profile,
                        &row,
                        accumulators,
                        max_tokens_per_contract,
                    ) {
                        continue;
                    }
                    Self::stage_metadata_recall_row(
                        &mut pending_metadata_rows,
                        seed_index,
                        &row,
                        row_match,
                        max_tokens_per_contract,
                    );
                    continue;
                }
                selected_rows.push(PendingMetadataRecallRow {
                    seed_index,
                    row,
                    row_match,
                });
                if selected_rows.len() >= SELECTED_RECALL_ROWID_CHUNK_SIZE {
                    Self::drain_owned_recall_rows(
                        conn,
                        profiles,
                        &mut selected_rows,
                        accumulators,
                        max_tokens_per_contract,
                    )?;
                }
            }
        }
        Self::drain_owned_recall_rows(
            conn,
            profiles,
            &mut selected_rows,
            accumulators,
            max_tokens_per_contract,
        )?;
        Self::drain_pending_metadata_recall_rows(
            conn,
            profiles,
            &mut pending_metadata_rows,
            accumulators,
            max_tokens_per_contract,
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
        Self::prepare_seed_value_table(conn, SEED_TOKEN_ID_TABLE, seed_token_ids)?;
        let contract_key_by_lower = rows_by_contract
            .keys()
            .map(|key| (key.to_lowercase(), key.clone()))
            .collect::<BTreeMap<_, _>>();
        let contract_keys = contract_key_by_lower
            .keys()
            .cloned()
            .collect::<HashSet<_>>();
        Self::prepare_seed_value_table(conn, CANDIDATE_CONTRACT_TABLE, &contract_keys)?;

        let sql = format!(
            "
            SELECT contract_key, contract_address, token_id, token_uri, image_uri, name, symbol,
                   metadata_json
            FROM (
                SELECT lower(f.contract_address) AS contract_key,
                       f.contract_address, f.token_id, coalesce(f.token_uri, '') AS token_uri,
                       coalesce(f.image_uri, '') AS image_uri, coalesce(f.name, '') AS name,
                       coalesce(f.symbol, '') AS symbol, coalesce(f.metadata_json, '') AS metadata_json,
                       row_number() OVER (
                           PARTITION BY lower(f.contract_address)
                           ORDER BY CASE
                               WHEN trim(coalesce(CAST(f.metadata_json AS VARCHAR), '')) LIKE '{{%'
                                    OR trim(coalesce(CAST(f.metadata_json AS VARCHAR), '')) LIKE '[%' THEN 0
                               WHEN trim(coalesce(CAST(f.metadata_json AS VARCHAR), '')) <> '' THEN 1
                               ELSE 2
                           END,
                           CAST(f.token_id AS VARCHAR)
                       ) AS overlap_rank
                FROM nft_features f
                JOIN {CANDIDATE_CONTRACT_TABLE} c
                  ON c.value = lower(f.contract_address)
                WHERE f.chain = ?
                  AND CAST(f.token_id AS VARCHAR) IN (SELECT value FROM {SEED_TOKEN_ID_TABLE})
                  AND length(trim(coalesce(CAST(f.metadata_json AS VARCHAR), ''))) <= {MAX_METADATA_BYTES_FOR_DEDUP}
                  AND (
                      trim(coalesce(CAST(f.metadata_json AS VARCHAR), '')) LIKE '{{%'
                      OR trim(coalesce(CAST(f.metadata_json AS VARCHAR), '')) LIKE '[%'
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
        Ok(())
    }

    pub fn load_snapshot(
        &self,
        chain: &str,
        seed_nfts: &[SeedNft],
        name_threshold: f64,
        metadata_threshold: f64,
        max_tokens_per_contract: usize,
        max_recall_rows: usize,
    ) -> Result<DatabaseSnapshot, AppError> {
        let profiles = vec![SeedRecallProfile::new(String::new(), seed_nfts)];
        let mut accumulators = BTreeMap::from([(String::new(), SnapshotAccumulator::default())]);
        let profile_index = SeedProfileIndex::new(&profiles);

        let conn = self.conn()?;
        let prepared_recall_state = self.ensure_prepared_recall_state(&conn, chain)?;
        Self::create_selected_recall_rowid_table(&conn)?;
        let result = (|| {
            Self::append_exact_uri_recall_rows(
                &conn,
                ExactUriRecallInput {
                    chain,
                    profiles: &profiles,
                    profile_index: &profile_index,
                    all_token_keys: &profiles[0].exact_token_keys,
                    all_image_keys: &profiles[0].exact_image_keys,
                    name_threshold,
                    max_tokens_per_contract,
                    max_recall_rows,
                    prepared_recall_state,
                },
                &mut accumulators,
            )?;
            Self::append_name_recall_rows(
                &conn,
                chain,
                &profiles,
                name_threshold,
                &mut accumulators,
                max_tokens_per_contract,
                prepared_recall_state,
            )?;
            if metadata_threshold <= 1.0
                && profiles
                    .iter()
                    .any(|profile| profile.seed_metadata_doc.is_some())
            {
                let metadata_index =
                    self.cached_metadata_recall_index(&conn, chain, prepared_recall_state)?;
                Self::append_metadata_recall_rows(
                    &conn,
                    &profiles,
                    metadata_threshold,
                    metadata_index.as_ref(),
                    &mut accumulators,
                    max_tokens_per_contract,
                )?;
            }
            Self::append_overlapping_metadata_token_rows(
                &conn,
                chain,
                &profiles[0].seed_token_ids,
                &mut accumulators
                    .get_mut("")
                    .expect("single-seed snapshot accumulator exists")
                    .duplicate_rows_by_contract,
            )?;
            Ok(accumulators
                .remove("")
                .expect("single-seed snapshot accumulator exists")
                .finish())
        })();
        let cleanup_result = Self::drop_seed_temp_tables(&conn);
        match (result, cleanup_result) {
            (Ok(snapshot), Ok(())) => Ok(snapshot),
            (Err(err), _) => Err(err),
            (Ok(_), Err(err)) => Err(err),
        }
    }

    pub fn load_snapshots(
        &self,
        chain: &str,
        seeds: &[(String, Vec<SeedNft>)],
        name_threshold: f64,
        metadata_threshold: f64,
        max_tokens_per_contract: usize,
        max_recall_rows: usize,
    ) -> Result<BTreeMap<String, DatabaseSnapshot>, AppError> {
        if seeds.len() <= 1 {
            let mut snapshots = BTreeMap::new();
            for (seed_address, seed_nfts) in seeds {
                snapshots.insert(
                    seed_address.clone(),
                    self.load_snapshot(
                        chain,
                        seed_nfts,
                        name_threshold,
                        metadata_threshold,
                        max_tokens_per_contract,
                        max_recall_rows,
                    )?,
                );
            }
            return Ok(snapshots);
        }

        let profiles = seeds
            .iter()
            .map(|(seed_address, seed_nfts)| {
                SeedRecallProfile::new(seed_address.clone(), seed_nfts)
            })
            .collect::<Vec<_>>();
        let mut all_token_keys = HashSet::new();
        let mut all_image_keys = HashSet::new();
        for profile in &profiles {
            all_token_keys.extend(profile.exact_token_keys.iter().cloned());
            all_image_keys.extend(profile.exact_image_keys.iter().cloned());
        }

        let mut accumulators = profiles
            .iter()
            .map(|profile| (profile.seed_address.clone(), SnapshotAccumulator::default()))
            .collect::<BTreeMap<_, _>>();
        let profile_index = SeedProfileIndex::new(&profiles);
        if !profiles
            .iter()
            .any(SeedRecallProfile::has_strong_recall_keys)
            && !profiles
                .iter()
                .any(|profile| profile.seed_metadata_doc.is_some())
        {
            return Ok(accumulators
                .into_iter()
                .map(|(seed_address, accumulator)| (seed_address, accumulator.finish()))
                .collect());
        }

        let conn = self.conn()?;
        let prepared_recall_state = self.ensure_prepared_recall_state(&conn, chain)?;
        Self::create_selected_recall_rowid_table(&conn)?;
        let result = (|| {
            Self::append_exact_uri_recall_rows(
                &conn,
                ExactUriRecallInput {
                    chain,
                    profiles: &profiles,
                    profile_index: &profile_index,
                    all_token_keys: &all_token_keys,
                    all_image_keys: &all_image_keys,
                    name_threshold,
                    max_tokens_per_contract,
                    max_recall_rows,
                    prepared_recall_state,
                },
                &mut accumulators,
            )?;
            Self::append_name_recall_rows(
                &conn,
                chain,
                &profiles,
                name_threshold,
                &mut accumulators,
                max_tokens_per_contract,
                prepared_recall_state,
            )?;
            if metadata_threshold <= 1.0
                && profiles
                    .iter()
                    .any(|profile| profile.seed_metadata_doc.is_some())
            {
                let metadata_index =
                    self.cached_metadata_recall_index(&conn, chain, prepared_recall_state)?;
                Self::append_metadata_recall_rows(
                    &conn,
                    &profiles,
                    metadata_threshold,
                    metadata_index.as_ref(),
                    &mut accumulators,
                    max_tokens_per_contract,
                )?;
            }
            for profile in &profiles {
                let Some(accumulator) = accumulators.get_mut(&profile.seed_address) else {
                    continue;
                };
                Self::append_overlapping_metadata_token_rows(
                    &conn,
                    chain,
                    &profile.seed_token_ids,
                    &mut accumulator.duplicate_rows_by_contract,
                )?;
            }

            let snapshots = accumulators
                .into_iter()
                .map(|(seed_address, accumulator)| (seed_address, accumulator.finish()))
                .collect();
            Ok(snapshots)
        })();
        let cleanup_result = Self::drop_seed_temp_tables(&conn);
        match (result, cleanup_result) {
            (Ok(snapshots), Ok(())) => Ok(snapshots),
            (Err(err), _) => Err(err),
            (Ok(_), Err(err)) => Err(err),
        }
    }
}
