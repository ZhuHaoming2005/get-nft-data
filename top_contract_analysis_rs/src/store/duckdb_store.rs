use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use crate::analysis::scoring::{
    metadata_bm25_has_terms, metadata_recall_document,
    score_compact_metadata_indexed_pair_with_query, CompactMetadataBm25CorpusBuilder,
    CompactMetadataBm25Query, MetadataBm25Document, MAX_METADATA_BYTES_FOR_DEDUP,
};
use crate::error::AppError;
use crate::models::{ContractDuplicateRecord, DatabaseNftRecord, DatabaseSnapshot, SeedNft};
use crate::normalize::{normalize_name, normalize_url};
use duckdb::{params, AccessMode, Config, Connection};
use rayon::prelude::*;

const MAX_OVERLAPPING_METADATA_ROWS_PER_CONTRACT: usize = 1;
const DEFAULT_RECALL_BATCH_SIZE: usize = 500_000;
const SELECTED_RECALL_ROWID_CHUNK_SIZE: usize = 50_000;
const SELECTED_RECALL_ROWID_VALUES_CHUNK_SIZE: usize = 10_000;
const SNAPSHOT_LOAD_LOG_THRESHOLD: Duration = Duration::from_millis(250);
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
const UNUSABLE_METADATA_CONTRACT_TABLE: &str = "__top_contract_unusable_metadata_contracts";
const RESOLVED_METADATA_REP_TABLE: &str = "__top_contract_resolved_metadata_reps";
const METADATA_SKETCH_ANCHOR_COUNT: usize = 16;
const METADATA_SKETCH_SOURCE_HAMMING_THRESHOLD: u32 = 32;
const METADATA_SIMHASH_BAND_COUNT: usize = 8;
const METADATA_SIMHASH_BAND_BITS: usize = 8;
const METADATA_SIMHASH_BAND_VALUES: usize = 1 << METADATA_SIMHASH_BAND_BITS;
const METADATA_SKETCH_HIGH_FREQ_MIN_DOCS: usize = 32;
const METADATA_SKETCH_HIGH_FREQ_DIVISOR: usize = 5;

const REQUIRED_SNAPSHOT_COLUMNS: [&str; 7] = [
    "chain",
    "contract_address",
    "token_id",
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
    snapshot_identity_cache: Mutex<HashMap<String, String>>,
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

struct NameRecallMatch {
    row_index: usize,
    seed_index: usize,
    row_match: SeedRowMatch,
}

struct NameRecallAppendState<'a> {
    conn: &'a Connection,
    profiles: &'a [SeedRecallProfile],
    selected_rows: &'a mut Vec<PendingMetadataRecallRow>,
    pending_contract_counts: &'a mut HashMap<(usize, String), usize>,
    accumulators: &'a mut BTreeMap<String, SnapshotAccumulator>,
    max_tokens_per_contract: usize,
}

struct SnapshotLoadTimer {
    label: &'static str,
    phase_start: Instant,
    total_start: Instant,
}

struct MetadataRecallSourceRow {
    feature_rowid: i64,
    contract_address: String,
    token_id: String,
    token_uri_norm: String,
    image_uri_norm: String,
    name_norm: String,
    metadata_json: String,
}

struct PreparedMetadataRecallRow {
    feature_rowid: i64,
    contract_address: String,
    token_id: String,
    token_uri_norm: String,
    image_uri_norm: String,
    name_norm: String,
    recall_doc: String,
}

impl MetadataRecallSourceRow {
    fn prepare(self) -> Result<PreparedMetadataRecallRow, String> {
        let recall_doc = metadata_recall_document(&self.metadata_json);
        if !metadata_bm25_has_terms(&recall_doc) {
            return Err(self.contract_address);
        }
        Ok(PreparedMetadataRecallRow {
            feature_rowid: self.feature_rowid,
            contract_address: self.contract_address,
            token_id: self.token_id,
            token_uri_norm: self.token_uri_norm,
            image_uri_norm: self.image_uri_norm,
            name_norm: self.name_norm,
            recall_doc,
        })
    }
}

/// Verify that the Arrow column at `index` is named `name`. Reads bind columns
/// positionally, so this guards against a later SELECT reordering or renaming a
/// column without updating the reader: every SELECT feeding these helpers aliases
/// its columns to the expected names, so a mismatch means the SQL and reader have
/// desynced. Fail fast with a clear error instead of silently reading the wrong
/// column.
fn arrow_verify_column_name(
    batch: &duckdb::arrow::record_batch::RecordBatch,
    index: usize,
    name: &str,
) -> Result<(), AppError> {
    let schema = batch.schema();
    match schema.fields().get(index) {
        Some(field) if field.name() == name => Ok(()),
        Some(field) => Err(AppError::InvalidData(format!(
            "DuckDB Arrow column at index {index} is {:?}, expected {name}",
            field.name()
        ))),
        None => Err(AppError::InvalidData(format!(
            "DuckDB Arrow column {name} is missing at index {index}"
        ))),
    }
}

fn arrow_i64_column<'a>(
    batch: &'a duckdb::arrow::record_batch::RecordBatch,
    index: usize,
    name: &str,
) -> Result<&'a duckdb::arrow::array::Int64Array, AppError> {
    arrow_verify_column_name(batch, index, name)?;
    batch
        .columns()
        .get(index)
        .and_then(|column| {
            column
                .as_any()
                .downcast_ref::<duckdb::arrow::array::Int64Array>()
        })
        .ok_or_else(|| {
            AppError::InvalidData(format!(
                "DuckDB Arrow column {name} is missing or is not BIGINT"
            ))
        })
}

fn arrow_string_column<'a>(
    batch: &'a duckdb::arrow::record_batch::RecordBatch,
    index: usize,
    name: &str,
) -> Result<&'a duckdb::arrow::array::StringArray, AppError> {
    arrow_verify_column_name(batch, index, name)?;
    batch
        .columns()
        .get(index)
        .and_then(|column| {
            column
                .as_any()
                .downcast_ref::<duckdb::arrow::array::StringArray>()
        })
        .ok_or_else(|| {
            AppError::InvalidData(format!(
                "DuckDB Arrow column {name} is missing or is not VARCHAR"
            ))
        })
}

fn append_recall_rows_from_arrow_batch(
    batch: &duckdb::arrow::record_batch::RecordBatch,
    metadata_recall_match: bool,
    target: &mut Vec<RecallRow>,
) -> Result<(), AppError> {
    let rowid_column = arrow_i64_column(batch, 0, "feature_rowid")?;
    let contract_column = arrow_string_column(batch, 1, "contract_address")?;
    let token_column = arrow_string_column(batch, 2, "token_id")?;
    let token_uri_column = arrow_string_column(batch, 3, "token_uri_norm")?;
    let image_uri_column = arrow_string_column(batch, 4, "image_uri_norm")?;
    let name_column = arrow_string_column(batch, 5, "name_norm")?;
    target.reserve(batch.num_rows());
    for row_index in 0..batch.num_rows() {
        target.push(RecallRow {
            feature_rowid: rowid_column.value(row_index),
            contract_address: contract_column.value(row_index).to_owned(),
            token_id: token_column.value(row_index).to_owned(),
            token_uri_norm: token_uri_column.value(row_index).to_owned(),
            image_uri_norm: image_uri_column.value(row_index).to_owned(),
            name_norm: name_column.value(row_index).to_owned(),
            metadata_recall_match,
        });
    }
    Ok(())
}

impl SnapshotLoadTimer {
    fn new(label: &'static str) -> Self {
        let now = Instant::now();
        Self {
            label,
            phase_start: now,
            total_start: now,
        }
    }

    fn finish_phase(&mut self, phase: &str) {
        let elapsed = self.phase_start.elapsed();
        if elapsed >= SNAPSHOT_LOAD_LOG_THRESHOLD {
            eprintln!(
                "[{}] {} completed in {:.3}s",
                self.label,
                phase,
                elapsed.as_secs_f64()
            );
        }
        self.phase_start = Instant::now();
    }

    fn finish(&self) {
        let elapsed = self.total_start.elapsed();
        if elapsed >= SNAPSHOT_LOAD_LOG_THRESHOLD {
            eprintln!(
                "[{}] total completed in {:.3}s",
                self.label,
                elapsed.as_secs_f64()
            );
        }
    }
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
            snapshot_identity_cache: Mutex::new(HashMap::new()),
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
            snapshot_identity_cache: Mutex::new(HashMap::new()),
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
        self.snapshot_identity_cache
            .lock()
            .map_err(|err| {
                AppError::DuckDb(format!("snapshot identity cache lock poisoned: {err}"))
            })?
            .remove(chain);
        Ok(())
    }
}

mod loading;
mod metadata_recall;
mod prepared;
mod recall;
mod schema;
mod snapshot;
