use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, TryLockError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
const METADATA_MATCH_PAIR_CHUNK_SIZE: usize = 1_000_000;
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
const SEED_CONTRACT_TABLE: &str = "__top_contract_analysis_seed_contracts";
const CANDIDATE_CONTRACT_TABLE: &str = "__top_contract_analysis_candidate_contracts";
const CANDIDATE_MATCH_TABLE: &str = "__top_contract_analysis_candidate_matches";
const URI_SELECTED_MATCH_TABLE: &str = "__top_contract_analysis_uri_selected_matches";
const TOKEN_URI_REASON: u8 = 1;
const IMAGE_URI_REASON: u8 = 2;
const NAME_REASON: u8 = 4;
const METADATA_REASON: u8 = 8;
const SELECTED_RECALL_ROWID_TABLE: &str = "__top_contract_analysis_selected_recall_rowids";
const CONTRACT_REPRESENTATIVE_TABLE: &str = "nft_contract_representatives";
const NAME_RECALL_ROW_TABLE: &str = "nft_name_recall_rows";
const URI_RECALL_POSTING_TABLE: &str = "nft_uri_recall_postings";
const METADATA_RECALL_DOC_TABLE: &str = "nft_metadata_recall_docs";
const PREPARED_RECALL_CHAIN_TABLE: &str = "nft_prepared_recall_chains";
const FEATURE_GENERATION_TABLE: &str = "nft_feature_generations";
const PREPARE_JOURNAL_TABLE: &str = "nft_prepare_journal";
const UNUSABLE_METADATA_CONTRACT_TABLE: &str = "__top_contract_unusable_metadata_contracts";
const RESOLVED_METADATA_REP_TABLE: &str = "__top_contract_resolved_metadata_reps";
#[cfg(test)]
const METADATA_SKETCH_ANCHOR_COUNT: usize = 16;
#[cfg(test)]
const METADATA_SKETCH_SOURCE_HAMMING_THRESHOLD: u32 = 16;
#[cfg(test)]
const METADATA_SKETCH_HIGH_FREQ_MIN_DOCS: usize = 32;
#[cfg(test)]
const METADATA_SKETCH_HIGH_FREQ_DIVISOR: usize = 5;
const NAME_RECALL_CACHE_MAX_CHAINS: usize = 4;
const METADATA_RECALL_CACHE_MAX_CHAINS: usize = 4;
const DEFAULT_RECALL_INDEX_MEMORY_LIMIT_BYTES: usize = 260_000_000_000;
const DEFAULT_MAX_SNAPSHOT_BYTES_PER_SEED: usize = 24_000_000_000;
const DEFAULT_MAX_CANDIDATE_CONTRACTS_PER_SEED: usize = 100_000;
const DEFAULT_MAX_SELECTED_ROWS_PER_SEED: usize = 2_000_000;
pub const ANALYSIS_COMPACT_PLAN_MEMORY_BUDGET_BYTES: usize = 48_000_000_000;
pub const ANALYSIS_NAME_SCRATCH_BUDGET_BYTES: usize = 16_000_000_000;
const ANALYSIS_MAX_RESIDENT_SNAPSHOTS: usize = 2;
const TARGET_MACHINE_MEMORY_BYTES: usize = 512 * 1_073_741_824;
const TARGET_MACHINE_RESERVED_MEMORY_BYTES: usize = 48 * 1_073_741_824;
// Accounts for retained maps, sets, vectors, duplicate projections, and
// allocator slack that cannot be measured from the persisted VARCHAR payload.
const SNAPSHOT_PREPAYLOAD_ROW_OVERHEAD_BYTES: usize = 2_048;
const PREPARED_FORMAT_VERSION: &str = "3";
const NORMALIZATION_VERSION: &str = "1";
const RECALL_ALGORITHM_VERSION: &str = "4-all-distinct-names-exact-term-postings";
const REPORT_SCHEMA_VERSION: &str = "2";
const BUILD_FINGERPRINT: &str = env!("TCA_BUILD_FINGERPRINT");

const REQUIRED_SNAPSHOT_COLUMNS: [&str; 7] = [
    "chain",
    "contract_address",
    "token_id",
    "metadata_json",
    "token_uri_norm",
    "image_uri_norm",
    "name_norm",
];

static FEATURE_GENERATION_SEQUENCE: AtomicU64 = AtomicU64::new(0);

mod name_recall;
mod support;

use name_recall::*;
use support::*;

pub struct DuckDbFeatureStore {
    connections: Vec<Mutex<Connection>>,
    next_connection: AtomicUsize,
    resource_options: DuckDbResourceOptions,
    name_recall_index_cache: Mutex<RecallIndexCache<ManagedRecallIndex<NameRecallIndex>>>,
    metadata_recall_index_cache: Mutex<RecallIndexCache<ManagedRecallIndex<MetadataRecallIndex>>>,
    snapshot_identity_cache: Mutex<HashMap<String, String>>,
    memory_governor: Arc<MemoryGovernor>,
    recall_index_build_lock: Mutex<()>,
    writable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DuckDbResourceOptions {
    pub threads: usize,
    pub memory_limit: String,
    pub recall_index_memory_limit_bytes: usize,
    pub read_connections: usize,
    pub max_snapshot_bytes_per_seed: usize,
    pub max_candidate_contracts_per_seed: usize,
    pub max_selected_rows_per_seed: usize,
}

impl Default for DuckDbResourceOptions {
    fn default() -> Self {
        Self {
            threads: std::thread::available_parallelism()
                .map(|threads| threads.get())
                .unwrap_or(1),
            memory_limit: "96GB".to_string(),
            recall_index_memory_limit_bytes: DEFAULT_RECALL_INDEX_MEMORY_LIMIT_BYTES,
            read_connections: 2,
            max_snapshot_bytes_per_seed: DEFAULT_MAX_SNAPSHOT_BYTES_PER_SEED,
            max_candidate_contracts_per_seed: DEFAULT_MAX_CANDIDATE_CONTRACTS_PER_SEED,
            max_selected_rows_per_seed: DEFAULT_MAX_SELECTED_ROWS_PER_SEED,
        }
    }
}

struct CachedRecallIndex<T> {
    value: Arc<T>,
    bytes: usize,
}

struct RecallIndexCache<T> {
    entries: HashMap<String, CachedRecallIndex<T>>,
    recency: VecDeque<String>,
    resident_bytes: usize,
    max_entries: usize,
    max_bytes: usize,
}

#[derive(Clone, Debug)]
struct PrepareJournalState {
    input_fingerprint: String,
    expected_chains: Vec<String>,
    imported_generations: BTreeMap<String, String>,
    prepared_chains: BTreeSet<String>,
    phase: String,
    prepared_format_version: String,
    normalization_version: String,
    recall_algorithm_version: String,
    report_schema_version: String,
    build_fingerprint: String,
}

#[derive(Clone, Debug)]
struct AuthoritativeInputFingerprint {
    combined_sha256: String,
    canonical_inputs: Vec<String>,
    manifest: Vec<AuthoritativeInputManifestEntry>,
}

#[derive(Clone, Debug, serde::Serialize)]
struct AuthoritativeInputManifestEntry {
    path: String,
    size_bytes: u64,
    modified_unix_nanos: u128,
    sha256: String,
}

struct MemoryGovernor {
    limit_bytes: usize,
    reserved_bytes: Mutex<usize>,
}

impl MemoryGovernor {
    fn new(limit_bytes: usize) -> Self {
        Self {
            limit_bytes,
            reserved_bytes: Mutex::new(0),
        }
    }

    #[cfg(test)]
    fn try_reserve(
        self: &Arc<Self>,
        category: &str,
        bytes: usize,
    ) -> Result<MemoryLease, AppError> {
        let mut reserved = self.reserved_bytes.lock().map_err(|error| {
            AppError::InvalidData(format!("memory governor lock poisoned: {error}"))
        })?;
        let requested_total = reserved.saturating_add(bytes);
        if requested_total > self.limit_bytes {
            return Err(AppError::ResourceLimit(format!(
                "memory reservation for {category} requires {bytes} bytes with {} bytes already reserved, exceeding the configured {}-byte managed limit",
                *reserved, self.limit_bytes
            )));
        }
        *reserved = requested_total;
        Ok(MemoryLease {
            governor: Arc::clone(self),
            bytes,
        })
    }

    fn reserved_bytes(&self) -> usize {
        self.reserved_bytes
            .lock()
            .map_or(self.limit_bytes, |value| *value)
    }

    fn available_bytes(&self) -> usize {
        self.limit_bytes.saturating_sub(self.reserved_bytes())
    }

    fn reserve_available(self: &Arc<Self>, category: &str) -> Result<MemoryLease, AppError> {
        let mut reserved = self.reserved_bytes.lock().map_err(|error| {
            AppError::InvalidData(format!("memory governor lock poisoned: {error}"))
        })?;
        let available = self.limit_bytes.saturating_sub(*reserved);
        if available == 0 {
            return Err(AppError::ResourceLimit(format!(
                "memory reservation for {category} has no capacity available; {} bytes are active within the configured {}-byte managed limit",
                *reserved, self.limit_bytes
            )));
        }
        *reserved = self.limit_bytes;
        Ok(MemoryLease {
            governor: Arc::clone(self),
            bytes: available,
        })
    }
}

struct MemoryLease {
    governor: Arc<MemoryGovernor>,
    bytes: usize,
}

struct ManagedRecallIndex<T> {
    value: T,
    _lease: MemoryLease,
}

impl<T> std::ops::Deref for ManagedRecallIndex<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl MemoryLease {
    fn bytes(&self) -> usize {
        self.bytes
    }

    fn resize(&mut self, category: &str, bytes: usize) -> Result<(), AppError> {
        if bytes == self.bytes {
            return Ok(());
        }
        let mut reserved = self.governor.reserved_bytes.lock().map_err(|error| {
            AppError::InvalidData(format!("memory governor lock poisoned: {error}"))
        })?;
        if bytes > self.bytes {
            let growth = bytes - self.bytes;
            let requested_total = reserved.saturating_add(growth);
            if requested_total > self.governor.limit_bytes {
                return Err(AppError::ResourceLimit(format!(
                    "memory reservation growth for {category} requires {growth} bytes with {} bytes already reserved, exceeding the configured {}-byte managed limit",
                    *reserved, self.governor.limit_bytes
                )));
            }
            *reserved = requested_total;
        } else {
            *reserved = reserved.saturating_sub(self.bytes - bytes);
        }
        self.bytes = bytes;
        Ok(())
    }
}

impl Drop for MemoryLease {
    fn drop(&mut self) {
        if let Ok(mut reserved) = self.governor.reserved_bytes.lock() {
            *reserved = reserved.saturating_sub(self.bytes);
        }
    }
}

impl<T> RecallIndexCache<T> {
    fn new(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            entries: HashMap::new(),
            recency: VecDeque::new(),
            resident_bytes: 0,
            max_entries,
            max_bytes,
        }
    }

    fn get(&mut self, key: &str) -> Option<Arc<T>> {
        let value = Arc::clone(&self.entries.get(key)?.value);
        self.recency.retain(|candidate| candidate != key);
        self.recency.push_back(key.to_string());
        Some(value)
    }

    fn insert(&mut self, key: String, value: Arc<T>, bytes: usize) -> bool {
        if self.max_entries == 0 || bytes > self.max_bytes {
            return false;
        }
        self.remove(&key);
        while self.entries.len() >= self.max_entries
            || self.resident_bytes.saturating_add(bytes) > self.max_bytes
        {
            let Some(oldest) = self.recency.pop_front() else {
                break;
            };
            if let Some(removed) = self.entries.remove(&oldest) {
                self.resident_bytes = self.resident_bytes.saturating_sub(removed.bytes);
            }
        }
        if self.entries.len() >= self.max_entries
            || self.resident_bytes.saturating_add(bytes) > self.max_bytes
        {
            return false;
        }
        self.resident_bytes = self.resident_bytes.saturating_add(bytes);
        self.recency.push_back(key.clone());
        self.entries.insert(key, CachedRecallIndex { value, bytes });
        true
    }

    fn remove(&mut self, key: &str) {
        self.recency.retain(|candidate| candidate != key);
        if let Some(removed) = self.entries.remove(key) {
            self.resident_bytes = self.resident_bytes.saturating_sub(removed.bytes);
        }
    }

    fn evict_oldest(&mut self) -> bool {
        while let Some(oldest) = self.recency.pop_front() {
            if let Some(removed) = self.entries.remove(&oldest) {
                self.resident_bytes = self.resident_bytes.saturating_sub(removed.bytes);
                return true;
            }
        }
        false
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
    fn contains(&self, key: &str) -> bool {
        self.entries.contains_key(key)
    }

    #[cfg(test)]
    fn resident_bytes(&self) -> usize {
        self.resident_bytes
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

    pub fn from_analysis_cli(
        threads: usize,
        memory_limit: &str,
        recall_index_memory_limit: &str,
        read_connections: usize,
        max_snapshot_bytes_per_seed: &str,
        max_candidate_contracts_per_seed: usize,
        max_selected_rows_per_seed: usize,
    ) -> Result<Self, AppError> {
        let mut options = Self::from_cli(threads, memory_limit)?;
        options.recall_index_memory_limit_bytes =
            parse_memory_limit_bytes(recall_index_memory_limit)?;
        if read_connections == 0 || read_connections > 8 {
            return Err(AppError::InvalidData(
                "DuckDB read connection count must be between 1 and 8".to_string(),
            ));
        }
        options.read_connections = read_connections;
        options.max_snapshot_bytes_per_seed =
            parse_memory_limit_bytes(max_snapshot_bytes_per_seed)?;
        if max_candidate_contracts_per_seed == 0 || max_selected_rows_per_seed == 0 {
            return Err(AppError::InvalidData(
                "per-seed candidate-contract and selected-row limits must be greater than zero"
                    .to_string(),
            ));
        }
        options.max_candidate_contracts_per_seed = max_candidate_contracts_per_seed;
        options.max_selected_rows_per_seed = max_selected_rows_per_seed;
        options.validate_read_only_options()?;
        Ok(options)
    }

    fn validate_read_only_options(&self) -> Result<(), AppError> {
        validate_duckdb_memory_limit(&self.memory_limit)?;
        if self.threads == 0 {
            return Err(AppError::InvalidData(
                "DuckDB thread count must be greater than zero".to_string(),
            ));
        }
        if self.read_connections == 0 || self.read_connections > 8 {
            return Err(AppError::InvalidData(
                "DuckDB read connection count must be between 1 and 8".to_string(),
            ));
        }
        if self.threads < self.read_connections {
            return Err(AppError::InvalidData(format!(
                "DuckDB thread count {} must be at least the read connection count {}",
                self.threads, self.read_connections
            )));
        }
        if self.recall_index_memory_limit_bytes == 0 || self.max_snapshot_bytes_per_seed == 0 {
            return Err(AppError::InvalidData(
                "recall-index and per-seed snapshot memory limits must be greater than zero"
                    .to_string(),
            ));
        }
        if self.max_candidate_contracts_per_seed == 0 || self.max_selected_rows_per_seed == 0 {
            return Err(AppError::InvalidData(
                "per-seed candidate-contract and selected-row limits must be greater than zero"
                    .to_string(),
            ));
        }
        self.validate_analysis_memory_envelope()
    }

    fn validate_analysis_memory_envelope(&self) -> Result<(), AppError> {
        let duckdb_bytes = parse_memory_limit_bytes(&self.memory_limit)?;
        let configured_bytes = duckdb_bytes
            .checked_add(self.recall_index_memory_limit_bytes)
            .and_then(|bytes| {
                self.max_snapshot_bytes_per_seed
                    .checked_mul(ANALYSIS_MAX_RESIDENT_SNAPSHOTS)
                    .and_then(|snapshots| bytes.checked_add(snapshots))
            })
            .and_then(|bytes| bytes.checked_add(ANALYSIS_COMPACT_PLAN_MEMORY_BUDGET_BYTES))
            .and_then(|bytes| bytes.checked_add(ANALYSIS_NAME_SCRATCH_BUDGET_BYTES))
            .ok_or_else(|| {
                AppError::ResourceLimit(
                    "combined analysis memory envelope overflows the supported byte range"
                        .to_string(),
                )
            })?;
        let safe_bytes = TARGET_MACHINE_MEMORY_BYTES - TARGET_MACHINE_RESERVED_MEMORY_BYTES;
        if configured_bytes > safe_bytes {
            return Err(AppError::ResourceLimit(format!(
                "combined analysis memory envelope is {configured_bytes} bytes, above the {safe_bytes}-byte safe limit for the 512 GiB target after reserving 48 GiB for the OS, API payloads, serialization, and allocator slack; lower DuckDB, recall-index, or per-seed snapshot limits"
            )));
        }
        Ok(())
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
    parse_memory_limit_bytes(value)?;
    Ok(())
}

fn parse_memory_limit_bytes(value: &str) -> Result<usize, AppError> {
    let normalized = value.trim().to_ascii_uppercase();
    let unit_start = normalized
        .bytes()
        .position(|byte| byte.is_ascii_alphabetic())
        .unwrap_or(normalized.len());
    let (number, unit) = normalized.split_at(unit_start);
    let number = number.replace('_', "").parse::<f64>().map_err(|_| {
        AppError::InvalidData(format!(
            "invalid memory limit {value:?}; expected a value like 160GB"
        ))
    })?;
    let multiplier = match unit {
        "B" | "" => 1_f64,
        "KB" => 1_000_f64,
        "KIB" => 1_024_f64,
        "MB" => 1_000_000_f64,
        "MIB" => 1_048_576_f64,
        "GB" => 1_000_000_000_f64,
        "GIB" => 1_073_741_824_f64,
        "TB" => 1_000_000_000_000_f64,
        "TIB" => 1_099_511_627_776_f64,
        _ => {
            return Err(AppError::InvalidData(format!(
                "invalid memory limit {value:?}; expected B/KB/KiB/MB/MiB/GB/GiB/TB/TiB"
            )))
        }
    };
    let bytes = number * multiplier;
    if !bytes.is_finite() || bytes <= 0.0 || bytes > usize::MAX as f64 {
        return Err(AppError::InvalidData(format!(
            "memory limit {value:?} is outside the supported range"
        )));
    }
    Ok(bytes as usize)
}

fn split_resource_budget(total: usize, partitions: usize, index: usize) -> usize {
    debug_assert!(partitions > 0);
    debug_assert!(index < partitions);
    total / partitions + usize::from(index < total % partitions)
}

struct ExactUriRecallInput<'a> {
    chain: &'a str,
    profiles: &'a [SeedRecallProfile],
    all_token_keys: &'a HashSet<String>,
    all_image_keys: &'a HashSet<String>,
    prepared_recall_state: PreparedRecallState,
}

struct NameRecallInput<'a> {
    chain: &'a str,
    profiles: &'a [SeedRecallProfile],
    name_threshold: f64,
    prepared_recall_state: PreparedRecallState,
}

struct MaterializeRecallInput<'a> {
    chain: &'a str,
    profiles: &'a [SeedRecallProfile],
    name_threshold: f64,
    max_tokens_per_contract: usize,
    max_recall_rows: usize,
}

#[cfg(test)]
struct NameRecallMatch {
    row_index: usize,
    seed_index: usize,
}

struct SnapshotLoadTimer {
    label: &'static str,
    phase_start: Instant,
    total_start: Instant,
}

struct MetadataRecallSourceRow {
    feature_rowid: i64,
    contract_address: String,
    metadata_json: String,
}

struct PreparedMetadataRecallRow {
    feature_rowid: i64,
    contract_address: String,
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
        let recall_index_budget = resource_options.recall_index_memory_limit_bytes;
        let memory_governor = Arc::new(MemoryGovernor::new(recall_index_budget));
        Ok(Self {
            connections: vec![Mutex::new(conn)],
            next_connection: AtomicUsize::new(0),
            resource_options,
            name_recall_index_cache: Mutex::new(RecallIndexCache::new(
                NAME_RECALL_CACHE_MAX_CHAINS,
                recall_index_budget,
            )),
            metadata_recall_index_cache: Mutex::new(RecallIndexCache::new(
                METADATA_RECALL_CACHE_MAX_CHAINS,
                recall_index_budget,
            )),
            snapshot_identity_cache: Mutex::new(HashMap::new()),
            memory_governor,
            recall_index_build_lock: Mutex::new(()),
            writable: true,
        })
    }

    pub fn open_read_only_with_options(
        database_path: &str,
        resource_options: DuckDbResourceOptions,
    ) -> Result<Self, AppError> {
        resource_options.validate_read_only_options()?;
        if database_path == ":memory:" {
            return Self::new_with_options(database_path, resource_options);
        }
        let connection_count = resource_options.read_connections;
        let total_memory_bytes = parse_memory_limit_bytes(&resource_options.memory_limit)?;
        let temp_root = Self::default_temp_directory(database_path);
        std::fs::create_dir_all(&temp_root)?;
        let mut connections = Vec::with_capacity(connection_count);
        for index in 0..connection_count {
            let conn = Connection::open_with_flags(
                database_path,
                Config::default().access_mode(AccessMode::ReadOnly)?,
            )?;
            let temp_directory = temp_root.join(format!("reader-{index}"));
            std::fs::create_dir_all(&temp_directory)?;
            let mut connection_options = resource_options.clone();
            connection_options.threads =
                split_resource_budget(resource_options.threads, connection_count, index);
            connection_options.memory_limit = format!(
                "{}B",
                split_resource_budget(total_memory_bytes, connection_count, index)
            );
            connection_options.read_connections = 1;
            Self::apply_resource_options(&conn, &connection_options, &temp_directory)?;
            Self::validate_schema(&conn)?;
            connections.push(Mutex::new(conn));
        }
        let recall_index_budget = resource_options.recall_index_memory_limit_bytes;
        let memory_governor = Arc::new(MemoryGovernor::new(recall_index_budget));
        Ok(Self {
            connections,
            next_connection: AtomicUsize::new(0),
            resource_options,
            name_recall_index_cache: Mutex::new(RecallIndexCache::new(
                NAME_RECALL_CACHE_MAX_CHAINS,
                recall_index_budget,
            )),
            metadata_recall_index_cache: Mutex::new(RecallIndexCache::new(
                METADATA_RECALL_CACHE_MAX_CHAINS,
                recall_index_budget,
            )),
            snapshot_identity_cache: Mutex::new(HashMap::new()),
            memory_governor,
            recall_index_build_lock: Mutex::new(()),
            writable: false,
        })
    }

    pub fn resource_options(&self) -> &DuckDbResourceOptions {
        &self.resource_options
    }

    fn conn(&self) -> Result<MutexGuard<'_, Connection>, AppError> {
        let count = self.connections.len();
        let start = self.next_connection.fetch_add(1, Ordering::Relaxed) % count;
        for offset in 0..count {
            match self.connections[(start + offset) % count].try_lock() {
                Ok(connection) => return Ok(connection),
                Err(TryLockError::WouldBlock) => {}
                Err(TryLockError::Poisoned(err)) => {
                    return Err(AppError::DuckDb(format!(
                        "DuckDB connection lock poisoned: {err}"
                    )))
                }
            }
        }
        self.connections[start]
            .lock()
            .map_err(|err| AppError::DuckDb(format!("DuckDB connection lock poisoned: {err}")))
    }

    #[cfg(test)]
    fn connection_count(&self) -> usize {
        self.connections.len()
    }

    fn metadata_recall_index_cache(
        &self,
    ) -> Result<MutexGuard<'_, RecallIndexCache<ManagedRecallIndex<MetadataRecallIndex>>>, AppError>
    {
        self.metadata_recall_index_cache.lock().map_err(|err| {
            AppError::DuckDb(format!("metadata recall index cache lock poisoned: {err}"))
        })
    }

    fn name_recall_index_cache(
        &self,
    ) -> Result<MutexGuard<'_, RecallIndexCache<ManagedRecallIndex<NameRecallIndex>>>, AppError>
    {
        self.name_recall_index_cache.lock().map_err(|err| {
            AppError::DuckDb(format!("name recall index cache lock poisoned: {err}"))
        })
    }

    fn reserve_recall_index_build(
        &self,
        category: &str,
        estimated_bytes: usize,
    ) -> Result<MemoryLease, AppError> {
        loop {
            let available = self.memory_governor.available_bytes();
            if available >= estimated_bytes.max(1) {
                return self.memory_governor.reserve_available(category);
            }
            let evicted_name = self.name_recall_index_cache()?.evict_oldest();
            let evicted_metadata = self.metadata_recall_index_cache()?.evict_oldest();
            if !evicted_name && !evicted_metadata {
                return Err(AppError::ResourceLimit(format!(
                    "memory admission for {category} estimates {estimated_bytes} bytes, but only {available} bytes are available because active recall indexes retain their leases within the configured {}-byte limit",
                    self.memory_governor.limit_bytes
                )));
            }
        }
    }

    #[cfg(test)]
    fn name_recall_index_cache_len(&self) -> usize {
        self.name_recall_index_cache
            .lock()
            .expect("name recall index cache lock")
            .len()
    }

    #[cfg(test)]
    fn metadata_recall_index_cache_len(&self) -> usize {
        self.metadata_recall_index_cache
            .lock()
            .expect("metadata recall index cache lock")
            .len()
    }

    fn invalidate_metadata_recall_index(&self, chain: &str) -> Result<(), AppError> {
        self.name_recall_index_cache()?.remove(chain);
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
