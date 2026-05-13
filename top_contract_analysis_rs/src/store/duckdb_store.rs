use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use crate::analysis::scoring::{
    metadata_document_from_json, metadata_is_dedup_eligible, metadata_recall_document,
    score_metadata_indexed_pair_with_query, score_name_pair, MetadataBm25Corpus,
    MetadataBm25CorpusBuilder, MetadataBm25Document, MetadataBm25Query,
    MAX_METADATA_BYTES_FOR_DEDUP,
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
const SEED_TOKEN_URI_TABLE: &str = "__top_contract_analysis_seed_token_uri_keys";
const SEED_IMAGE_URI_TABLE: &str = "__top_contract_analysis_seed_image_uri_keys";
const SEED_TOKEN_ID_TABLE: &str = "__top_contract_analysis_seed_token_ids";
const CANDIDATE_CONTRACT_TABLE: &str = "__top_contract_analysis_candidate_contracts";
const METADATA_SKETCH_ANCHOR_COUNT: usize = 8;
const METADATA_SKETCH_SOURCE_HAMMING_THRESHOLD: u32 = 24;
const METADATA_SIMHASH_BAND_COUNT: usize = 8;
const METADATA_SIMHASH_BAND_BITS: usize = 8;
const METADATA_SIMHASH_BAND_VALUES: usize = 1 << METADATA_SIMHASH_BAND_BITS;
const METADATA_SKETCH_HIGH_FREQ_MIN_DOCS: usize = 32;
const METADATA_SKETCH_HIGH_FREQ_DIVISOR: usize = 5;

const PRECOMPUTED_COLUMNS: [&str; 3] = ["token_uri_norm", "image_uri_norm", "name_norm"];

#[derive(Clone)]
struct RecallRow {
    feature_rowid: i64,
    contract_address: String,
    token_id: String,
    token_uri_norm: String,
    image_uri_norm: String,
    name_norm: String,
    metadata_recall_match: bool,
}

struct SeedRecallProfile {
    seed_address: String,
    seed_contracts: HashSet<String>,
    seed_token_ids: HashSet<String>,
    exact_token_keys: HashSet<String>,
    exact_image_keys: HashSet<String>,
    seed_name_norms: Vec<String>,
    seed_metadata_doc: Option<MetadataBm25Document>,
}

#[derive(Default)]
struct SnapshotAccumulator {
    per_contract_counts: HashMap<String, usize>,
    nft_rows: Vec<DatabaseNftRecord>,
    selected_rowids: HashMap<i64, usize>,
    duplicate_rows_by_contract: HashMap<String, ContractDuplicateRecord>,
    seen_contract_name_pairs: BTreeSet<(String, String)>,
    seen_feature_rowids: HashSet<i64>,
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

struct PendingMetadataRecallRow {
    seed_index: usize,
    row: RecallRow,
    row_match: SeedRowMatch,
}

struct SeedProfileIndex {
    token_uri: HashMap<String, Vec<usize>>,
    image_uri: HashMap<String, Vec<usize>>,
}

#[derive(Clone, Debug, Default)]
struct MetadataSketch {
    simhash: u64,
    anchors: Vec<String>,
}

#[derive(Clone)]
struct MetadataRecallCandidate {
    row: RecallRow,
    doc: MetadataBm25Document,
    sketch: MetadataSketch,
}

#[derive(Default)]
struct MetadataSourceIndex {
    anchor_indices: HashMap<String, Vec<usize>>,
    simhash_band_indices: Vec<Vec<usize>>,
}

struct MetadataRecallIndex {
    candidates: Vec<MetadataRecallCandidate>,
    corpus: MetadataBm25Corpus,
    doc_freqs: HashMap<String, usize>,
    source_index: MetadataSourceIndex,
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

        let snapshot = store
            .load_snapshot("ethereum", &seed_nfts, 95.0, 0.6, 0, 0)
            .unwrap();

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

        let snapshot = store
            .load_snapshot("ethereum", &seed_nfts, 95.0, 0.6, 0, 0)
            .unwrap();
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

        let snapshot = store
            .load_snapshot("ethereum", &seed_nfts, 95.0, 0.6, 0, 1)
            .unwrap();
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
                95.0,
                0.6,
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
                    name_norm VARCHAR
                );
                INSERT INTO nft_features VALUES (
                    'ethereum', '0xcandidate', '1', '', '', '', '', '{\"description\":\"gold dragon\"}',
                    '', '', ''
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
                95.0,
                0.6,
                0,
                0,
            )
            .unwrap();

        assert!(!has_persistent_index);
        assert_eq!(snapshot.duplicate_contract_rows.len(), 1);
    }

    #[test]
    fn opening_writable_feature_db_drops_obsolete_metadata_columns() {
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
        assert!(!columns.contains("name_prefix8"));
        assert!(!columns.contains("metadata_keywords_arr"));
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

fn seed_metadata_representative_doc(seed_nfts: &[SeedNft]) -> Option<MetadataBm25Document> {
    seed_nfts.first().and_then(|item| {
        let doc = metadata_recall_document(&item.metadata_json);
        MetadataBm25Document::from_text(&doc)
    })
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
            seed_name_norms: seed_nfts
                .iter()
                .map(|item| normalize_name(&item.name))
                .filter(|value| !value.is_empty())
                .collect(),
            seed_metadata_doc: seed_metadata_representative_doc(seed_nfts),
        }
    }

    fn has_strong_recall_keys(&self) -> bool {
        !self.exact_token_keys.is_empty()
            || !self.exact_image_keys.is_empty()
            || !self.seed_name_norms.is_empty()
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

        if self.seen_feature_rowids.contains(&row.feature_rowid) {
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
        self.seen_feature_rowids.insert(row.feature_rowid);
        self.selected_rowids
            .insert(row.feature_rowid, self.nft_rows.len());

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

    fn mark_selected_metadata_recall(&mut self, row: &RecallRow) -> bool {
        let Some(index) = self.selected_rowids.get(&row.feature_rowid).copied() else {
            return false;
        };
        let Some(record) = self.nft_rows.get_mut(index) else {
            return false;
        };
        record.metadata_recall_checked = true;
        record.metadata_recall_match = true;
        let record = record.clone();

        if let Some(signal) = self.contract_signals_raw.get_mut(&record.contract_address) {
            signal.keyword_match = true;
        }
        if let Some(entry) = self
            .duplicate_rows_by_contract
            .get_mut(&record.contract_address)
        {
            entry.metadata_recall_checked = true;
            entry.metadata_recall_match = true;
            for token_row in &mut entry.metadata_token_rows {
                if token_row.token_id == record.token_id {
                    token_row.metadata_recall_checked = true;
                    token_row.metadata_recall_match = true;
                }
            }
            if !entry.representative.metadata_recall_match
                || record.token_id < entry.representative.token_id
            {
                entry.representative = record;
            }
        }
        true
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
        matches
    }
}

fn metadata_token_idf(total_docs: usize, doc_freq: usize) -> f64 {
    (((total_docs + 1) as f64) / ((doc_freq + 1) as f64)).ln() + 1.0
}

fn metadata_token_is_high_frequency(total_docs: usize, doc_freq: usize) -> bool {
    doc_freq >= METADATA_SKETCH_HIGH_FREQ_MIN_DOCS
        && doc_freq.saturating_mul(METADATA_SKETCH_HIGH_FREQ_DIVISOR) > total_docs
}

fn stable_token_hash(token: &str) -> u64 {
    let mut value = 0xcbf2_9ce4_8422_2325u64;
    for byte in token.as_bytes() {
        value ^= u64::from(*byte);
        value = value.wrapping_mul(0x0000_0100_0000_01b3);
    }
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn metadata_simhash_from_weights(weights: [f64; 64]) -> u64 {
    let mut simhash = 0u64;
    for (bit, weight) in weights.into_iter().enumerate() {
        if weight >= 0.0 {
            simhash |= 1u64 << bit;
        }
    }
    simhash
}

fn compare_metadata_anchor_quality(
    left: &(String, f64),
    right: &(String, f64),
) -> std::cmp::Ordering {
    left.1
        .partial_cmp(&right.1)
        .unwrap_or(std::cmp::Ordering::Equal)
        .then_with(|| right.0.cmp(&left.0))
}

fn push_metadata_anchor_candidate(anchors: &mut Vec<(String, f64)>, candidate: (String, f64)) {
    if anchors.len() < METADATA_SKETCH_ANCHOR_COUNT {
        anchors.push(candidate);
        return;
    }

    let Some((worst_index, worst_anchor)) = anchors
        .iter()
        .enumerate()
        .min_by(|(_, left), (_, right)| compare_metadata_anchor_quality(left, right))
    else {
        return;
    };
    if compare_metadata_anchor_quality(&candidate, worst_anchor).is_gt() {
        anchors[worst_index] = candidate;
    }
}

fn metadata_sketch_from_document(
    document: &MetadataBm25Document,
    total_docs: usize,
    doc_freqs: &HashMap<String, usize>,
) -> MetadataSketch {
    let mut weights = [0.0f64; 64];
    let mut anchors = Vec::<(String, f64)>::new();
    let unique_tokens = document.tokens().iter().collect::<BTreeSet<_>>();
    for token in unique_tokens {
        let df = doc_freqs.get(token).copied().unwrap_or(0);
        let idf = metadata_token_idf(total_docs, df);
        let token_hash = stable_token_hash(token);
        for (bit, weight) in weights.iter_mut().enumerate() {
            if ((token_hash >> bit) & 1) == 1 {
                *weight += idf;
            } else {
                *weight -= idf;
            }
        }
        if metadata_token_is_high_frequency(total_docs, df) {
            continue;
        }
        push_metadata_anchor_candidate(&mut anchors, ((*token).clone(), idf));
    }
    let mut anchors = anchors
        .into_iter()
        .map(|(token, _)| token)
        .collect::<Vec<_>>();
    anchors.sort();
    MetadataSketch {
        simhash: metadata_simhash_from_weights(weights),
        anchors,
    }
}

fn metadata_simhash_band_key(band_index: usize, band_value: u8) -> usize {
    band_index * METADATA_SIMHASH_BAND_VALUES + band_value as usize
}

fn metadata_simhash_band_value(simhash: u64, band_index: usize) -> u8 {
    ((simhash >> (band_index * METADATA_SIMHASH_BAND_BITS)) & 0xff) as u8
}

fn sorted_strings_intersect(left: &[String], right: &[String]) -> bool {
    let mut left_index = 0;
    let mut right_index = 0;
    while left_index < left.len() && right_index < right.len() {
        match left[left_index].cmp(&right[right_index]) {
            std::cmp::Ordering::Equal => return true,
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
        }
    }
    false
}

fn metadata_sketch_source_match(
    seed: &MetadataSketch,
    candidate: &MetadataSketch,
    hamming_threshold: u32,
) -> bool {
    if (seed.simhash == 0 && seed.anchors.is_empty())
        || (candidate.simhash == 0 && candidate.anchors.is_empty())
    {
        return false;
    }
    if !seed.anchors.is_empty() && sorted_strings_intersect(&seed.anchors, &candidate.anchors) {
        return true;
    }
    (seed.simhash ^ candidate.simhash).count_ones() <= hamming_threshold
}

fn metadata_seed_doc_for_index(
    seed_doc: &MetadataBm25Document,
    metadata_index: &MetadataRecallIndex,
) -> Option<MetadataBm25Document> {
    let tokens = seed_doc
        .tokens()
        .iter()
        .filter(|token| metadata_index.doc_freqs.contains_key(*token))
        .cloned()
        .collect::<Vec<_>>();
    MetadataBm25Document::from_tokens(tokens)
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
                name_norm VARCHAR
            );
            ",
        )?;
        Self::drop_obsolete_nft_feature_columns(&conn)?;
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
                {metadata_json_expr} AS metadata_json,
                coalesce(CAST(token_uri_norm AS VARCHAR), '') AS token_uri_norm,
                coalesce(CAST(image_uri_norm AS VARCHAR), '') AS image_uri_norm,
                coalesce(CAST(name_norm AS VARCHAR), '') AS name_norm
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
    ) -> Result<MetadataRecallIndex, AppError> {
        let sql = format!(
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
        );
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
                metadata_json,
            ) = row?;
            let recall_doc = metadata_recall_document(&metadata_json);
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

    fn estimate_metadata_source_bucket_hits(
        seed_sketch: &MetadataSketch,
        source_index: &MetadataSourceIndex,
        hamming_threshold: u32,
    ) -> usize {
        let mut hits = 0usize;
        for anchor in &seed_sketch.anchors {
            if let Some(indices) = source_index.anchor_indices.get(anchor) {
                hits = hits.saturating_add(indices.len());
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
                    hits = hits.saturating_add(indices.len());
                }
            }
        }
        hits
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
            "
        ))?;
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

    fn append_exact_uri_recall_rows(
        conn: &Connection,
        chain: &str,
        profiles: &[SeedRecallProfile],
        profile_index: &SeedProfileIndex,
        all_token_keys: &HashSet<String>,
        all_image_keys: &HashSet<String>,
        name_threshold: f64,
        accumulators: &mut BTreeMap<String, SnapshotAccumulator>,
        max_tokens_per_contract: usize,
        max_recall_rows: usize,
    ) -> Result<(), AppError> {
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
            let select_sql = format!(
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
            );
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
    ) -> Result<(), AppError> {
        if name_threshold > 100.0
            || !profiles
                .iter()
                .any(|profile| !profile.seed_name_norms.is_empty())
        {
            return Ok(());
        }
        let sql = "
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
            ";
        let mut stmt = conn.prepare(sql)?;
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
        Self::append_exact_uri_recall_rows(
            &conn,
            chain,
            &profiles,
            &profile_index,
            &profiles[0].exact_token_keys,
            &profiles[0].exact_image_keys,
            name_threshold,
            &mut accumulators,
            max_tokens_per_contract,
            max_recall_rows,
        )?;
        Self::append_name_recall_rows(
            &conn,
            chain,
            &profiles,
            name_threshold,
            &mut accumulators,
            max_tokens_per_contract,
        )?;
        if metadata_threshold <= 1.0
            && profiles
                .iter()
                .any(|profile| profile.seed_metadata_doc.is_some())
        {
            let metadata_index = Self::load_metadata_recall_index(&conn, chain)?;
            Self::append_metadata_recall_rows(
                &conn,
                &profiles,
                metadata_threshold,
                &metadata_index,
                &mut accumulators,
                max_tokens_per_contract,
            )?;
        }
        Self::drop_seed_temp_tables(&conn)?;
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
        Self::append_exact_uri_recall_rows(
            &conn,
            chain,
            &profiles,
            &profile_index,
            &all_token_keys,
            &all_image_keys,
            name_threshold,
            &mut accumulators,
            max_tokens_per_contract,
            max_recall_rows,
        )?;
        Self::append_name_recall_rows(
            &conn,
            chain,
            &profiles,
            name_threshold,
            &mut accumulators,
            max_tokens_per_contract,
        )?;
        if metadata_threshold <= 1.0
            && profiles
                .iter()
                .any(|profile| profile.seed_metadata_doc.is_some())
        {
            let metadata_index = Self::load_metadata_recall_index(&conn, chain)?;
            Self::append_metadata_recall_rows(
                &conn,
                &profiles,
                metadata_threshold,
                &metadata_index,
                &mut accumulators,
                max_tokens_per_contract,
            )?;
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
