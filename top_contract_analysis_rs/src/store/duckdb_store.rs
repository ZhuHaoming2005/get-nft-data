use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use crate::analysis::scoring::{
    metadata_document_from_json, metadata_is_dedup_eligible, metadata_recall_all_keywords,
    metadata_recall_document, MAX_METADATA_BYTES_FOR_DEDUP,
};
use crate::error::AppError;
use crate::models::{
    ContractDuplicateRecord, ContractNameRecord, ContractSignal, DatabaseNftRecord,
    DatabaseSnapshot, SeedNft,
};
use crate::normalize::{normalize_name, normalize_url};
use duckdb::{params, AccessMode, Config, Connection};

const MAX_OVERLAPPING_METADATA_ROWS_PER_CONTRACT: usize = 1;
const DEFAULT_RECALL_BATCH_SIZE: usize = 500_000;
const SELECTED_RECALL_ROWID_CHUNK_SIZE: usize = 50_000;
const BULK_IMPORT_CHECKPOINT_THRESHOLD: &str = "1TB";
const BULK_IMPORT_SKIP_WAL_THRESHOLD_BYTES: u64 = 1_099_511_627_776;
const SEED_METADATA_TERM_TABLE: &str = "__top_contract_analysis_seed_metadata_terms";
const SEED_METADATA_ROWID_TABLE: &str = "__top_contract_analysis_seed_metadata_rowids";
const SEED_TOKEN_URI_TABLE: &str = "__top_contract_analysis_seed_token_uri_keys";
const SEED_IMAGE_URI_TABLE: &str = "__top_contract_analysis_seed_image_uri_keys";
const SEED_NAME_PREFIX_TABLE: &str = "__top_contract_analysis_seed_name_prefixes";
const SEED_TOKEN_ID_TABLE: &str = "__top_contract_analysis_seed_token_ids";

const PRECOMPUTED_COLUMNS: [&str; 5] = [
    "token_uri_norm",
    "image_uri_norm",
    "name_norm",
    "name_prefix8",
    "metadata_keywords_arr",
];

#[derive(Clone)]
struct RecallRow {
    feature_rowid: i64,
    contract_address: String,
    token_id: String,
    token_uri_norm: String,
    image_uri_norm: String,
    name_norm: String,
    metadata_keywords_arr: String,
    metadata_recall_match: bool,
}

struct SeedRecallProfile {
    seed_address: String,
    seed_contracts: HashSet<String>,
    seed_token_ids: HashSet<String>,
    exact_token_keys: HashSet<String>,
    exact_image_keys: HashSet<String>,
    name_prefixes: HashSet<String>,
    metadata_terms: HashSet<String>,
}

#[derive(Default)]
struct SnapshotAccumulator {
    per_contract_counts: HashMap<String, usize>,
    nft_rows: Vec<DatabaseNftRecord>,
    duplicate_rows_by_contract: HashMap<String, ContractDuplicateRecord>,
    seen_contract_name_pairs: BTreeSet<(String, String)>,
    contract_names: Vec<ContractNameRecord>,
    contract_signals_raw: BTreeMap<String, ContractSignal>,
}

struct SeedRowMatch {
    token_uri_match: bool,
    image_uri_match: bool,
    name_prefix_match: bool,
    metadata_recall_match: bool,
}

struct SelectedRecallRow {
    seed_index: usize,
    row_index: usize,
    row_match: SeedRowMatch,
}

struct SeedProfileIndex {
    token_uri: HashMap<String, Vec<usize>>,
    image_uri: HashMap<String, Vec<usize>>,
    name_prefix: HashMap<String, Vec<usize>>,
    metadata_term: HashMap<String, Vec<usize>>,
}

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
                    metadata_json: r#"{"second_unique":"silver cat"}"#.into(),
                    ..Default::default()
                }],
            )
            .unwrap();
        let seed_nfts = vec![
            SeedNft {
                contract_address: "0xseed".into(),
                token_id: "1".into(),
                metadata_json: r#"{"first_unique":"gold dragon"}"#.into(),
                ..Default::default()
            },
            SeedNft {
                contract_address: "0xseed".into(),
                token_id: "2".into(),
                metadata_json: r#"{"second_unique":"silver cat"}"#.into(),
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

    #[test]
    fn load_snapshot_recalls_metadata_without_persistent_keyword_index() {
        let store = DuckDbFeatureStore::new(":memory:").unwrap();
        store
            .replace_chain_rows(
                "ethereum",
                &[DatabaseNftRecord {
                    contract_address: "0xcandidate".into(),
                    token_id: "1".into(),
                    metadata_json: r#"{"description":"gold dragon"}"#.into(),
                    ..Default::default()
                }],
            )
            .unwrap();

        let conn = store.conn().unwrap();
        let has_persistent_index = conn
            .query_row(
                "
                SELECT EXISTS(
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_name = 'metadata_keyword_index'
                )
                ",
                [],
                |row| row.get::<_, bool>(0),
            )
            .unwrap();
        drop(conn);
        let snapshot = store
            .load_snapshot(
                "ethereum",
                &[SeedNft {
                    contract_address: "0xseed".into(),
                    token_id: "1".into(),
                    metadata_json: r#"{"description":"gold dragon"}"#.into(),
                    ..Default::default()
                }],
                0,
                0,
            )
            .unwrap();

        assert!(!has_persistent_index);
        assert_eq!(snapshot.duplicate_contract_rows.len(), 1);
        assert!(snapshot.contract_signals["0xcandidate"].keyword_match);
    }

    #[test]
    fn opening_existing_feature_db_does_not_backfill_persistent_metadata_keyword_index() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("features.duckdb");
        {
            let conn = Connection::open(&db_path).unwrap();
            conn.execute_batch(
                "
                CREATE TABLE nft_features (
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
                    name_prefix8 VARCHAR,
                    metadata_keywords_arr VARCHAR
                );
                INSERT INTO nft_features VALUES (
                    'ethereum', '0xcandidate', '1', '', '', '', '', '{\"description\":\"gold dragon\"}',
                    '', '', '', '', '[\"description\",\"dragon\",\"gold\"]'
                );
                ",
            )
            .unwrap();
        }

        let store = DuckDbFeatureStore::new(&db_path.to_string_lossy()).unwrap();
        let conn = store.conn().unwrap();
        let has_persistent_index = conn
            .query_row(
                "
                SELECT EXISTS(
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_name = 'metadata_keyword_index'
                )
                ",
                [],
                |row| row.get::<_, bool>(0),
            )
            .unwrap();
        drop(conn);
        let snapshot = store
            .load_snapshot(
                "ethereum",
                &[SeedNft {
                    contract_address: "0xseed".into(),
                    token_id: "1".into(),
                    metadata_json: r#"{"description":"gold dragon"}"#.into(),
                    ..Default::default()
                }],
                0,
                0,
            )
            .unwrap();

        assert!(!has_persistent_index);
        assert_eq!(snapshot.duplicate_contract_rows.len(), 1);
    }

    #[test]
    fn opening_writable_feature_db_drops_obsolete_metadata_doc_column() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("features.duckdb");
        {
            let conn = Connection::open(&db_path).unwrap();
            conn.execute_batch(
                "
                CREATE TABLE nft_features (
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
                    name_prefix8 VARCHAR,
                    metadata_keywords_arr VARCHAR
                );
                INSERT INTO nft_features VALUES (
                    'ethereum', '0xcandidate', '1', '', '', '', '', '{\"description\":\"gold dragon\"}',
                    '', '', '', 'gold dragon', '', '[\"description\",\"dragon\",\"gold\"]'
                );
                ",
            )
            .unwrap();
        }

        let store = DuckDbFeatureStore::new(&db_path.to_string_lossy()).unwrap();
        let conn = store.conn().unwrap();
        let columns = DuckDbFeatureStore::table_columns(&conn, "nft_features").unwrap();
        let row_count: i64 = conn
            .query_row("SELECT count(*) FROM nft_features", [], |row| row.get(0))
            .unwrap();

        assert!(!columns.contains("metadata_doc"));
        assert_eq!(row_count, 1);
    }

    #[test]
    fn feature_store_configures_bulk_import_checkpoint_limits() {
        let store = DuckDbFeatureStore::new(":memory:").unwrap();
        let conn = store.conn().unwrap();
        let checkpoint_threshold: String = conn
            .query_row(
                "SELECT current_setting('checkpoint_threshold')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let skip_wal_threshold: u64 = conn
            .query_row(
                "SELECT current_setting('auto_checkpoint_skip_wal_threshold')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let write_buffer_row_group_count: u64 = conn
            .query_row(
                "SELECT current_setting('write_buffer_row_group_count')",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert!(checkpoint_threshold.contains("GiB"));
        assert_eq!(skip_wal_threshold, BULK_IMPORT_SKIP_WAL_THRESHOLD_BYTES);
        assert_eq!(write_buffer_row_group_count, 1);
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
            let doc = metadata_recall_document(&item.metadata_json);
            let keywords = metadata_recall_all_keywords(&doc);
            if keywords.is_empty() {
                None
            } else {
                Some(keywords.into_iter().collect())
            }
        })
        .unwrap_or_default()
}

impl SeedRecallProfile {
    fn new(seed_address: String, seed_nfts: &[SeedNft]) -> Self {
        Self {
            seed_address,
            seed_contracts: seed_nfts
                .iter()
                .map(|item| item.contract_address.to_lowercase())
                .collect(),
            seed_token_ids: seed_nfts
                .iter()
                .map(|item| item.token_id.clone())
                .filter(|value| !value.is_empty())
                .collect(),
            exact_token_keys: seed_nfts
                .iter()
                .filter_map(|item| normalize_url(&item.token_uri))
                .collect(),
            exact_image_keys: seed_nfts
                .iter()
                .filter_map(|item| normalize_url(&item.image_uri))
                .collect(),
            name_prefixes: seed_nfts
                .iter()
                .map(|item| normalize_name(&item.name))
                .filter(|value| !value.is_empty())
                .map(|value| value.chars().take(8).collect::<String>())
                .collect(),
            metadata_terms: seed_metadata_recall_terms(seed_nfts),
        }
    }

    fn has_strong_recall_keys(&self) -> bool {
        !self.exact_token_keys.is_empty()
            || !self.exact_image_keys.is_empty()
            || !self.name_prefixes.is_empty()
    }
}

impl SnapshotAccumulator {
    fn push_recall_row(
        &mut self,
        profile: &SeedRecallProfile,
        row: &RecallRow,
        mut record: DatabaseNftRecord,
        row_match: &SeedRowMatch,
        max_tokens_per_contract: usize,
    ) {
        if profile.seed_contracts.contains(&row.contract_address) {
            return;
        }

        let entry = self
            .per_contract_counts
            .entry(row.contract_address.clone())
            .or_default();
        if max_tokens_per_contract > 0 && *entry >= max_tokens_per_contract {
            return;
        }
        *entry += 1;

        record.metadata_recall_checked = true;
        record.metadata_recall_match = row_match.metadata_recall_match;

        let name_pair_is_new = !row.name_norm.is_empty()
            && self
                .seen_contract_name_pairs
                .insert((record.contract_address.clone(), row.name_norm.clone()));
        if name_pair_is_new {
            self.contract_names.push(ContractNameRecord {
                contract_address: record.contract_address.clone(),
                name_norm: row.name_norm.clone(),
            });
        }

        let signal = self
            .contract_signals_raw
            .entry(record.contract_address.clone())
            .or_insert_with(|| ContractSignal {
                contract_address: record.contract_address.clone(),
                ..ContractSignal::default()
            });
        signal.token_count += 1;
        if row_match.token_uri_match {
            signal.uri_match_count += 1;
        }
        if row_match.image_uri_match {
            signal.image_match_count += 1;
        }
        if row_match.name_prefix_match {
            signal.name_prefix_match = true;
        }
        if row_match.metadata_recall_match {
            signal.keyword_match = true;
        }

        update_duplicate_contract_row(
            &mut self.duplicate_rows_by_contract,
            &record,
            row_match.token_uri_match,
            row_match.image_uri_match,
            &row.name_norm,
            name_pair_is_new,
            row_match.metadata_recall_match,
        );
        self.nft_rows.push(record);
    }

    fn finish(self) -> DatabaseSnapshot {
        let mut duplicate_contract_rows: Vec<_> =
            self.duplicate_rows_by_contract.into_values().collect();
        for row in &mut duplicate_contract_rows {
            row.metadata_token_rows
                .sort_by(|left, right| left.token_id.cmp(&right.token_id));
        }
        duplicate_contract_rows
            .sort_by(|left, right| left.contract_address.cmp(&right.contract_address));

        DatabaseSnapshot {
            nft_rows: self.nft_rows,
            duplicate_contract_rows,
            contract_names: self.contract_names,
            contract_signals: self.contract_signals_raw,
        }
    }
}

impl SeedProfileIndex {
    fn new(profiles: &[SeedRecallProfile]) -> Self {
        let mut index = Self {
            token_uri: HashMap::new(),
            image_uri: HashMap::new(),
            name_prefix: HashMap::new(),
            metadata_term: HashMap::new(),
        };
        for (profile_index, profile) in profiles.iter().enumerate() {
            Self::insert_values(
                &mut index.token_uri,
                &profile.exact_token_keys,
                profile_index,
            );
            Self::insert_values(
                &mut index.image_uri,
                &profile.exact_image_keys,
                profile_index,
            );
            Self::insert_values(
                &mut index.name_prefix,
                &profile.name_prefixes,
                profile_index,
            );
            Self::insert_values(
                &mut index.metadata_term,
                &profile.metadata_terms,
                profile_index,
            );
        }
        index
    }

    fn insert_values(
        target: &mut HashMap<String, Vec<usize>>,
        values: &HashSet<String>,
        profile_index: usize,
    ) {
        for value in values {
            if value.is_empty() {
                continue;
            }
            target.entry(value.clone()).or_default().push(profile_index);
        }
    }

    fn append_matching_profiles(
        target: &mut Vec<usize>,
        source: &HashMap<String, Vec<usize>>,
        value: &str,
    ) {
        if value.is_empty() {
            return;
        }
        let Some(profile_indices) = source.get(value) else {
            return;
        };
        for profile_index in profile_indices {
            if !target.contains(profile_index) {
                target.push(*profile_index);
            }
        }
    }

    fn strong_match_profiles(&self, row: &RecallRow) -> Vec<usize> {
        let mut matches = Vec::new();
        Self::append_matching_profiles(&mut matches, &self.token_uri, &row.token_uri_norm);
        Self::append_matching_profiles(&mut matches, &self.image_uri, &row.image_uri_norm);
        let name_prefix = row.name_norm.chars().take(8).collect::<String>();
        Self::append_matching_profiles(&mut matches, &self.name_prefix, &name_prefix);
        matches
    }

    fn metadata_match_profiles(&self, metadata_keywords: &HashSet<String>) -> Vec<usize> {
        let mut matches = Vec::new();
        for keyword in metadata_keywords {
            Self::append_matching_profiles(&mut matches, &self.metadata_term, keyword);
        }
        matches
    }
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
    let should_update_representative = metadata_recall_match
        && (!entry.representative.metadata_recall_match
            || record.token_id < entry.representative.token_id);
    if !should_update_representative {
        return;
    }
    entry.representative = record.clone();
}

fn push_metadata_token_row(entry: &mut ContractDuplicateRecord, record: &DatabaseNftRecord) {
    if !metadata_is_dedup_eligible(&record.metadata_json) {
        return;
    }
    if metadata_document_from_json(&record.metadata_json).is_empty() {
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
                name_prefix8 VARCHAR,
                metadata_keywords_arr VARCHAR
            );
            ",
        )?;
        Self::drop_obsolete_nft_feature_columns(&conn)?;
        Self::add_missing_nft_feature_columns(&conn)?;
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

    fn drop_obsolete_nft_feature_columns(conn: &Connection) -> Result<(), AppError> {
        let columns = Self::table_columns(conn, "nft_features")?;
        if columns.contains("metadata_doc") {
            conn.execute("ALTER TABLE nft_features DROP COLUMN metadata_doc", [])?;
        }
        Ok(())
    }

    fn add_missing_nft_feature_columns(conn: &Connection) -> Result<(), AppError> {
        let columns = Self::table_columns(conn, "nft_features")?;
        if !columns.contains("name_prefix8") && columns.contains("name_norm") {
            conn.execute(
                "ALTER TABLE nft_features ADD COLUMN name_prefix8 VARCHAR",
                [],
            )?;
            conn.execute(
                "UPDATE nft_features SET name_prefix8 = substr(coalesce(name_norm, ''), 1, 8)",
                [],
            )?;
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
        let conn = self.conn()?;
        conn.execute("DELETE FROM nft_features WHERE chain = ?", params![chain])?;

        let mut stmt = conn.prepare(
            "
            INSERT INTO nft_features (
                chain, contract_address, token_id, token_uri, image_uri, name, symbol, metadata_json,
                token_uri_norm, image_uri_norm, name_norm, name_prefix8, metadata_keywords_arr
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ",
        )?;

        for row in rows {
            let metadata_recall_doc = metadata_recall_document(&row.metadata_json);
            let metadata_keywords_arr =
                serde_json::to_string(&metadata_recall_all_keywords(&metadata_recall_doc))?;
            let name_norm = normalize_name(&row.name);
            let name_prefix8 = name_norm.chars().take(8).collect::<String>();
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
                name_prefix8,
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
                token_uri_norm, image_uri_norm, name_norm, name_prefix8, metadata_keywords_arr
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
                coalesce(CAST(name_prefix8 AS VARCHAR), '') AS name_prefix8,
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

    fn sql_metadata_json_eligible_predicate() -> String {
        format!(
            "length(trim(coalesce(CAST(metadata_json AS VARCHAR), ''))) <= {MAX_METADATA_BYTES_FOR_DEDUP} \
             AND (trim(coalesce(CAST(metadata_json AS VARCHAR), '')) LIKE '{{%' \
             OR trim(coalesce(CAST(metadata_json AS VARCHAR), '')) LIKE '[%')"
        )
    }

    fn sql_metadata_rowid_candidate_predicate() -> String {
        format!("rowid IN (SELECT feature_rowid FROM {SEED_METADATA_ROWID_TABLE})")
    }

    fn prepare_seed_metadata_term_table(
        conn: &Connection,
        metadata_recall_terms: &HashSet<String>,
    ) -> Result<(), AppError> {
        conn.execute_batch(&format!(
            "
            DROP TABLE IF EXISTS {SEED_METADATA_TERM_TABLE};
            CREATE TEMP TABLE {SEED_METADATA_TERM_TABLE} (
                keyword VARCHAR NOT NULL
            );
            "
        ))?;
        if metadata_recall_terms.is_empty() {
            return Ok(());
        }

        let mut stmt = conn.prepare(&format!(
            "INSERT INTO {SEED_METADATA_TERM_TABLE} (keyword) VALUES (?)"
        ))?;
        let terms = metadata_recall_terms
            .iter()
            .filter(|value| !value.is_empty())
            .collect::<BTreeSet<_>>();
        for term in terms {
            stmt.execute(params![term])?;
        }
        Ok(())
    }

    fn prepare_seed_metadata_rowid_table(conn: &Connection, chain: &str) -> Result<(), AppError> {
        conn.execute_batch(&format!(
            "
            DROP TABLE IF EXISTS {SEED_METADATA_ROWID_TABLE};
            CREATE TEMP TABLE {SEED_METADATA_ROWID_TABLE} (
                feature_rowid BIGINT NOT NULL
            );
            "
        ))?;
        conn.execute(
            &format!(
                "
                INSERT INTO {SEED_METADATA_ROWID_TABLE} (feature_rowid)
                SELECT DISTINCT f.rowid
                FROM nft_features f,
                     LATERAL (
                         SELECT DISTINCT json_extract_string(value, '$') AS keyword
                         FROM json_each(
                             CASE
                                 WHEN json_valid(coalesce(f.metadata_keywords_arr, '[]'))
                                 THEN coalesce(f.metadata_keywords_arr, '[]')
                                 ELSE '[]'
                             END
                         )
                     ) k
                JOIN {SEED_METADATA_TERM_TABLE} t ON t.keyword = k.keyword
                WHERE f.chain = ?
                  AND k.keyword IS NOT NULL
                  AND k.keyword <> ''
                  AND {}
                ",
                Self::sql_metadata_json_eligible_predicate()
            ),
            params![chain],
        )?;
        Ok(())
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
            DROP TABLE IF EXISTS {SEED_METADATA_TERM_TABLE};
            DROP TABLE IF EXISTS {SEED_METADATA_ROWID_TABLE};
            DROP TABLE IF EXISTS {SEED_TOKEN_URI_TABLE};
            DROP TABLE IF EXISTS {SEED_IMAGE_URI_TABLE};
            DROP TABLE IF EXISTS {SEED_NAME_PREFIX_TABLE};
            DROP TABLE IF EXISTS {SEED_TOKEN_ID_TABLE};
            "
        ))?;
        Ok(())
    }

    fn metadata_keywords_from_recall_row(row: &RecallRow) -> HashSet<String> {
        serde_json::from_str::<Vec<String>>(&row.metadata_keywords_arr)
            .unwrap_or_default()
            .into_iter()
            .filter(|value| !value.is_empty())
            .collect()
    }

    fn seed_row_match(
        profile: &SeedRecallProfile,
        row: &RecallRow,
        metadata_keywords: &HashSet<String>,
    ) -> SeedRowMatch {
        let name_prefix = row.name_norm.chars().take(8).collect::<String>();
        let metadata_recall_match = row.metadata_recall_match
            && metadata_keywords
                .iter()
                .any(|keyword| profile.metadata_terms.contains(keyword));
        SeedRowMatch {
            token_uri_match: profile.exact_token_keys.contains(&row.token_uri_norm),
            image_uri_match: profile.exact_image_keys.contains(&row.image_uri_norm),
            name_prefix_match: !name_prefix.is_empty()
                && profile.name_prefixes.contains(&name_prefix),
            metadata_recall_match,
        }
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

    fn fetch_records_by_feature_rowid(
        conn: &Connection,
        rowids: &[i64],
    ) -> Result<HashMap<i64, DatabaseNftRecord>, AppError> {
        let mut records = HashMap::new();
        for chunk in rowids.chunks(SELECTED_RECALL_ROWID_CHUNK_SIZE) {
            if chunk.is_empty() {
                continue;
            }
            let values = chunk
                .iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "
                SELECT rowid, contract_address, token_id, coalesce(token_uri, ''),
                       coalesce(image_uri, ''), coalesce(name, ''), coalesce(symbol, ''),
                       coalesce(metadata_json, '')
                FROM nft_features
                WHERE rowid IN ({values})
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
                       metadata_json
                FROM (
                    SELECT lower(contract_address) AS contract_key,
                           contract_address, token_id, coalesce(token_uri, '') AS token_uri,
                           coalesce(image_uri, '') AS image_uri, coalesce(name, '') AS name,
                           coalesce(symbol, '') AS symbol, coalesce(metadata_json, '') AS metadata_json,
                           row_number() OVER (
                               PARTITION BY lower(contract_address)
                               ORDER BY CASE
                                   WHEN trim(coalesce(CAST(metadata_json AS VARCHAR), '')) LIKE '{{%'
                                        OR trim(coalesce(CAST(metadata_json AS VARCHAR), '')) LIKE '[%' THEN 0
                                   WHEN trim(coalesce(CAST(metadata_json AS VARCHAR), '')) <> '' THEN 1
                                   ELSE 2
                               END,
                               CAST(token_id AS VARCHAR)
                           ) AS overlap_rank
                    FROM nft_features
                    WHERE chain = ?
                      AND lower(contract_address) IN ({contract_values})
                      AND CAST(token_id AS VARCHAR) IN (SELECT value FROM {SEED_TOKEN_ID_TABLE})
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
        let profiles = vec![SeedRecallProfile {
            seed_address: String::new(),
            seed_contracts: seed_contracts.clone(),
            seed_token_ids: seed_token_ids.clone(),
            exact_token_keys: exact_token_keys.clone(),
            exact_image_keys: exact_image_keys.clone(),
            name_prefixes: name_prefixes.clone(),
            metadata_terms: metadata_recall_terms.clone(),
        }];
        let mut accumulators = BTreeMap::from([(String::new(), SnapshotAccumulator::default())]);

        let conn = self.conn()?;
        Self::prepare_seed_value_table(&conn, SEED_TOKEN_URI_TABLE, &exact_token_keys)?;
        Self::prepare_seed_value_table(&conn, SEED_IMAGE_URI_TABLE, &exact_image_keys)?;
        Self::prepare_seed_value_table(&conn, SEED_NAME_PREFIX_TABLE, &name_prefixes)?;
        Self::prepare_seed_metadata_term_table(&conn, &metadata_recall_terms)?;
        if !metadata_recall_terms.is_empty() {
            Self::prepare_seed_metadata_rowid_table(&conn, chain)?;
        }
        let token_uri_match_expr = if exact_token_keys.is_empty() {
            "FALSE".to_string()
        } else {
            format!("token_uri_norm IN (SELECT value FROM {SEED_TOKEN_URI_TABLE})")
        };
        let image_uri_match_expr = if exact_image_keys.is_empty() {
            "FALSE".to_string()
        } else {
            format!("image_uri_norm IN (SELECT value FROM {SEED_IMAGE_URI_TABLE})")
        };
        let name_prefix_match_expr = if name_prefixes.is_empty() {
            "FALSE".to_string()
        } else {
            format!("name_prefix8 IN (SELECT value FROM {SEED_NAME_PREFIX_TABLE})")
        };
        let metadata_recall_predicate = if metadata_recall_terms.is_empty() {
            None
        } else {
            Some(format!(
                "({}) AND ({})",
                Self::sql_metadata_json_eligible_predicate(),
                Self::sql_metadata_rowid_candidate_predicate()
            ))
        };
        let metadata_recall_expr = metadata_recall_predicate
            .as_deref()
            .unwrap_or("FALSE")
            .to_string();
        let strong_recall_predicate = format!(
            "({token_uri_match_expr}) OR ({image_uri_match_expr}) OR ({name_prefix_match_expr})"
        );
        let mut recall_pass_predicates = Vec::new();
        if !exact_token_keys.is_empty() || !exact_image_keys.is_empty() || !name_prefixes.is_empty()
        {
            recall_pass_predicates.push(strong_recall_predicate.clone());
        }
        if metadata_recall_predicate.is_some() {
            recall_pass_predicates.push(format!(
                "({metadata_recall_expr}) AND NOT ({strong_recall_predicate})"
            ));
        }
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

        let recall_batch_size = if max_recall_rows == 0 {
            DEFAULT_RECALL_BATCH_SIZE
        } else {
            max_recall_rows
        };
        for recall_predicate in recall_pass_predicates {
            let mut last_feature_rowid = -1_i64;
            loop {
                let select_sql = format!(
                    "
                    SELECT rowid AS feature_rowid, contract_address,
                           token_uri_norm, image_uri_norm, name_norm,
                           coalesce(metadata_keywords_arr, '[]') AS metadata_keywords_arr,
                           {metadata_recall_expr} AS metadata_recall_match,
                           CAST(token_id AS VARCHAR) AS token_id
                    FROM nft_features
                    WHERE chain = ?{seed_contract_filter}
                      AND ({recall_predicate})
                      AND rowid > {last_feature_rowid}
                    ORDER BY rowid
                    LIMIT {recall_batch_size}
                    "
                );
                let mut stmt = conn.prepare(&select_sql)?;
                let rows = stmt.query_map(params![chain], |row| {
                    Ok(RecallRow {
                        feature_rowid: row.get::<_, i64>(0)?,
                        contract_address: row.get::<_, String>(1)?,
                        token_uri_norm: row.get::<_, String>(2)?,
                        image_uri_norm: row.get::<_, String>(3)?,
                        name_norm: row.get::<_, String>(4)?,
                        metadata_keywords_arr: row.get::<_, String>(5)?,
                        metadata_recall_match: row.get::<_, bool>(6)?,
                        token_id: row.get::<_, String>(7)?,
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
                for (row_index, row) in batch_rows.iter().enumerate() {
                    if seed_contracts.contains(&row.contract_address) {
                        continue;
                    }

                    let name_prefix = row.name_norm.chars().take(8).collect::<String>();
                    let matches = exact_token_keys.contains(&row.token_uri_norm)
                        || exact_image_keys.contains(&row.image_uri_norm)
                        || (!name_prefix.is_empty() && name_prefixes.contains(&name_prefix))
                        || row.metadata_recall_match;

                    if !matches {
                        continue;
                    }

                    selected_rows.push(SelectedRecallRow {
                        seed_index: 0,
                        row_index,
                        row_match: SeedRowMatch {
                            token_uri_match: exact_token_keys.contains(&row.token_uri_norm),
                            image_uri_match: exact_image_keys.contains(&row.image_uri_norm),
                            name_prefix_match: !name_prefix.is_empty()
                                && name_prefixes.contains(&name_prefix),
                            metadata_recall_match: row.metadata_recall_match,
                        },
                    });
                    if selected_rows.len() >= SELECTED_RECALL_ROWID_CHUNK_SIZE {
                        Self::drain_selected_recall_rows(
                            &conn,
                            &profiles,
                            &batch_rows,
                            &mut selected_rows,
                            &mut accumulators,
                            max_tokens_per_contract,
                        )?;
                    }
                }
                Self::drain_selected_recall_rows(
                    &conn,
                    &profiles,
                    &batch_rows,
                    &mut selected_rows,
                    &mut accumulators,
                    max_tokens_per_contract,
                )?;

                if fetched_rows < recall_batch_size {
                    break;
                }
                last_feature_rowid = last_seen_feature_rowid;
            }
        }
        Self::drop_seed_temp_tables(&conn)?;
        Self::append_overlapping_metadata_token_rows(
            &conn,
            chain,
            &seed_token_ids,
            &mut accumulators
                .get_mut("")
                .expect("single-seed snapshot accumulator exists")
                .duplicate_rows_by_contract,
        )?;
        Ok(accumulators
            .remove("")
            .expect("single-seed snapshot accumulator exists")
            .finish())
    }

    pub fn load_snapshots(
        &self,
        chain: &str,
        seeds: &[(String, Vec<SeedNft>)],
        max_tokens_per_contract: usize,
        max_recall_rows: usize,
    ) -> Result<BTreeMap<String, DatabaseSnapshot>, AppError> {
        if seeds.len() <= 1 {
            let mut snapshots = BTreeMap::new();
            for (seed_address, seed_nfts) in seeds {
                snapshots.insert(
                    seed_address.clone(),
                    self.load_snapshot(chain, seed_nfts, max_tokens_per_contract, max_recall_rows)?,
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
        let mut all_name_prefixes = HashSet::new();
        let mut all_metadata_terms = HashSet::new();
        for profile in &profiles {
            all_token_keys.extend(profile.exact_token_keys.iter().cloned());
            all_image_keys.extend(profile.exact_image_keys.iter().cloned());
            all_name_prefixes.extend(profile.name_prefixes.iter().cloned());
            all_metadata_terms.extend(profile.metadata_terms.iter().cloned());
        }

        let mut accumulators = profiles
            .iter()
            .map(|profile| (profile.seed_address.clone(), SnapshotAccumulator::default()))
            .collect::<BTreeMap<_, _>>();
        let profile_index = SeedProfileIndex::new(&profiles);
        if !profiles
            .iter()
            .any(SeedRecallProfile::has_strong_recall_keys)
            && all_metadata_terms.is_empty()
        {
            return Ok(accumulators
                .into_iter()
                .map(|(seed_address, accumulator)| (seed_address, accumulator.finish()))
                .collect());
        }

        let conn = self.conn()?;
        Self::prepare_seed_value_table(&conn, SEED_TOKEN_URI_TABLE, &all_token_keys)?;
        Self::prepare_seed_value_table(&conn, SEED_IMAGE_URI_TABLE, &all_image_keys)?;
        Self::prepare_seed_value_table(&conn, SEED_NAME_PREFIX_TABLE, &all_name_prefixes)?;
        Self::prepare_seed_metadata_term_table(&conn, &all_metadata_terms)?;
        if !all_metadata_terms.is_empty() {
            Self::prepare_seed_metadata_rowid_table(&conn, chain)?;
        }
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
        let name_prefix_match_expr = if all_name_prefixes.is_empty() {
            "FALSE".to_string()
        } else {
            format!("name_prefix8 IN (SELECT value FROM {SEED_NAME_PREFIX_TABLE})")
        };
        let strong_recall_predicate = format!(
            "({token_uri_match_expr}) OR ({image_uri_match_expr}) OR ({name_prefix_match_expr})"
        );
        let metadata_recall_predicate = if all_metadata_terms.is_empty() {
            None
        } else {
            Some(format!(
                "({}) AND ({})",
                Self::sql_metadata_json_eligible_predicate(),
                Self::sql_metadata_rowid_candidate_predicate()
            ))
        };
        let metadata_recall_expr = metadata_recall_predicate
            .as_deref()
            .unwrap_or("FALSE")
            .to_string();
        let mut recall_pass_predicates = Vec::new();
        if !all_token_keys.is_empty() || !all_image_keys.is_empty() || !all_name_prefixes.is_empty()
        {
            recall_pass_predicates.push((strong_recall_predicate, false));
        }
        if metadata_recall_predicate.is_some() {
            recall_pass_predicates.push((metadata_recall_expr.clone(), true));
        }
        let recall_batch_size = if max_recall_rows == 0 {
            DEFAULT_RECALL_BATCH_SIZE
        } else {
            max_recall_rows
        };

        for (recall_predicate, metadata_only_pass) in recall_pass_predicates {
            let mut last_feature_rowid = -1_i64;
            loop {
                let select_sql = format!(
                    "
                    SELECT rowid AS feature_rowid, contract_address,
                           token_uri_norm, image_uri_norm, name_norm,
                           coalesce(metadata_keywords_arr, '[]') AS metadata_keywords_arr,
                           {metadata_recall_expr} AS metadata_recall_match,
                           CAST(token_id AS VARCHAR) AS token_id
                    FROM nft_features
                    WHERE chain = ?
                      AND ({recall_predicate})
                      AND rowid > {last_feature_rowid}
                    ORDER BY rowid
                    LIMIT {recall_batch_size}
                    "
                );
                let mut stmt = conn.prepare(&select_sql)?;
                let rows = stmt.query_map(params![chain], |row| {
                    Ok(RecallRow {
                        feature_rowid: row.get::<_, i64>(0)?,
                        contract_address: row.get::<_, String>(1)?,
                        token_uri_norm: row.get::<_, String>(2)?,
                        image_uri_norm: row.get::<_, String>(3)?,
                        name_norm: row.get::<_, String>(4)?,
                        metadata_keywords_arr: row.get::<_, String>(5)?,
                        metadata_recall_match: row.get::<_, bool>(6)?,
                        token_id: row.get::<_, String>(7)?,
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
                for (row_index, row) in batch_rows.iter().enumerate() {
                    let metadata_keywords = if row.metadata_recall_match {
                        Self::metadata_keywords_from_recall_row(row)
                    } else {
                        HashSet::new()
                    };
                    let mut seed_indices = if metadata_only_pass {
                        profile_index.metadata_match_profiles(&metadata_keywords)
                    } else {
                        profile_index.strong_match_profiles(row)
                    };
                    seed_indices.sort_unstable();
                    for seed_index in seed_indices {
                        let Some(profile) = profiles.get(seed_index) else {
                            continue;
                        };
                        let row_match = Self::seed_row_match(profile, row, &metadata_keywords);
                        let strong_match = row_match.token_uri_match
                            || row_match.image_uri_match
                            || row_match.name_prefix_match;
                        if metadata_only_pass {
                            if strong_match || !row_match.metadata_recall_match {
                                continue;
                            }
                        } else if !strong_match {
                            continue;
                        }
                        selected_rows.push(SelectedRecallRow {
                            seed_index,
                            row_index,
                            row_match,
                        });
                        if selected_rows.len() >= SELECTED_RECALL_ROWID_CHUNK_SIZE {
                            Self::drain_selected_recall_rows(
                                &conn,
                                &profiles,
                                &batch_rows,
                                &mut selected_rows,
                                &mut accumulators,
                                max_tokens_per_contract,
                            )?;
                        }
                    }
                }
                Self::drain_selected_recall_rows(
                    &conn,
                    &profiles,
                    &batch_rows,
                    &mut selected_rows,
                    &mut accumulators,
                    max_tokens_per_contract,
                )?;

                if fetched_rows < recall_batch_size {
                    break;
                }
                last_feature_rowid = last_seen_feature_rowid;
            }
        }
        Self::drop_seed_temp_tables(&conn)?;
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
    }
}
