use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

use duckdb::{params, AccessMode, Config, Connection};
use once_cell::sync::Lazy;
use rayon::prelude::*;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use unicode_normalization::UnicodeNormalization;

type TokenId = u32;

const METADATA_REGIONS: [&str; 7] = [
    "title",
    "description",
    "attributes",
    "references",
    "auxiliary_fields",
    "platform_fields",
    "structure",
];
const METADATA_CONTENT_REGIONS: [&str; 5] = [
    "title",
    "description",
    "attributes",
    "references",
    "auxiliary_fields",
];
const METADATA_NON_CONTENT_REGIONS: [&str; 2] = ["platform_fields", "structure"];
const METADATA_OPERATIONS: [&str; 4] = ["added", "removed", "replaced", "reordered"];

static TRAILING_PATTERNS: Lazy<Vec<Regex>> = Lazy::new(|| {
    vec![
        Regex::new(r"\s*#\s*[0-9a-fA-FxX]+\s*$").unwrap(),
        Regex::new(r"\s*#\s*\d+\s*$").unwrap(),
        Regex::new(r"\s*-\s*\d+\s*$").unwrap(),
        Regex::new(r"\s*:\s*\d+\s*$").unwrap(),
        Regex::new(r"\s*\(\s*\d+\s*\)\s*$").unwrap(),
        Regex::new(r"\s*\[\s*\d+\s*\]\s*$").unwrap(),
        Regex::new(r"\s*/\s*\d+\s*$").unwrap(),
        Regex::new(r"(?i)\s+No\.?\s*\d+\s*$").unwrap(),
        Regex::new(r"(?i)\s+nr\.?\s*\d+\s*$").unwrap(),
        Regex::new(r"\s+\d{1,12}\s*$").unwrap(),
        Regex::new(r"\s*(?:404|420|777|888|999)\s*$").unwrap(),
        Regex::new(r"(?i)(?:\s|[-_.])*(?:v|version)\s*\d+\s*$").unwrap(),
        Regex::new(r"\s*\d+\.0\s*$").unwrap(),
        Regex::new(r"(?i)(?:\s|[-_.])*(?:gen|generation)\s*\d+\s*$").unwrap(),
    ]
});
static ATTACHED_TRAILING_NUMBER_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)([\p{L}])\d{1,6}\s*$").unwrap());
static WHITESPACE_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\s+").unwrap());
static TOKEN_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"[\p{L}\p{N}_]+").unwrap());
static DERIVATIVE_SUFFIX_PATTERNS: Lazy<Vec<Regex>> = Lazy::new(|| {
    vec![
        Regex::new(r"(?i)\.fun\s*$").unwrap(),
        Regex::new(r"(?i)(?:\s|[-_.])?(?:2d|3d|vx|x)\s*$").unwrap(),
        Regex::new(r"(?i)(?:\s|[-_.])?(?:ai|xr|gif|fc|id|art)\s*$").unwrap(),
        Regex::new(r"(?i)(?:\s|[-_.])?(?:1st|2nd|3rd|\d+th)\s*$").unwrap(),
        Regex::new(r"(?i)(?:\s|[-_.])?\(\s*test\s*\)\s*$").unwrap(),
        Regex::new(r"(?i)(?:\s|[-_.])(?:viii|vii|vi|iv|iii|ii|ix|i|v|x)\s*$").unwrap(),
        Regex::new(
            r"(?i)(?:\s|[-_.])(?:official|nft|club|dao|pass|mint|claim|free|vip|collection|edition|clone|copy|reloaded|remastered|alpha|beta)\s*$",
        )
        .unwrap(),
    ]
});

const METADATA_BM25_K1: f64 = 1.2;
const METADATA_BM25_B: f64 = 0.75;
const ART_BLOCKS_STUDIO_NAME_PREFIX: &str = "art blocks studio";
const MAX_METADATA_BYTES_FOR_DEDUP: usize = 64 * 1024;
const MAX_OVERLAPPING_METADATA_ROWS_PER_CONTRACT: usize = 1;
const SAMPLE_PROGRESS_STAGE_COUNT: usize = 14;
const METADATA_SKETCH_ANCHOR_COUNT: usize = 8;
const METADATA_SKETCH_SOURCE_HAMMING_THRESHOLD: u32 = 24;
const METADATA_SIMHASH_BAND_COUNT: usize = 8;
const METADATA_SIMHASH_BAND_BITS: usize = 8;
const METADATA_SIMHASH_BAND_VALUES: usize = 1 << METADATA_SIMHASH_BAND_BITS;
const METADATA_SKETCH_HIGH_FREQ_MIN_DOCS: usize = 32;
const METADATA_SKETCH_HIGH_FREQ_DIVISOR: usize = 5;

#[derive(Clone, Debug, PartialEq)]
pub struct SampleCollectionConfig {
    pub chain: String,
    pub feature_db: PathBuf,
    pub input: PathBuf,
    pub output: PathBuf,
    pub name_threshold: f64,
    pub metadata_threshold: f64,
    pub max_recall_rows: usize,
    pub max_seed_tokens: usize,
    pub duckdb_threads: usize,
    pub duckdb_memory_limit: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SampleReport {
    pub chain: String,
    pub seed_reports: Vec<SeedSampleReport>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SeedSampleReport {
    pub name: TextComparison,
    pub metadata: MetadataComparison,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TextComparison {
    pub seed: String,
    pub matches: Vec<String>,
    pub labeled_matches: Vec<LabeledTextMatch>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetadataComparison {
    pub seed: String,
    pub matches: Vec<String>,
    pub labeled_matches: Vec<LabeledTextMatch>,
    pub paired_matches: Vec<MetadataPairMatch>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetadataPairMatch {
    pub seed: String,
    pub text: String,
    pub labels: Vec<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct LabeledTextMatch {
    pub text: String,
    pub labels: Vec<String>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum SampleProgressStage {
    ReadSeedRows,
    LoadNameCandidates,
    ScoreNameCandidates,
    PrepareMetadataQuery,
    BuildMetadataSeedDoc,
    BuildMetadataSeedSketch,
    CollectMetadataSourceBuckets,
    VerifyMetadataSourceBuckets,
    CollectMetadataCandidates,
    ScoreMetadataPrefilter,
    LoadOverlappingMetadata,
    ScoreOverlappingMetadata,
    BuildReport,
    FinishedSeed,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SampleProgress {
    pub seed_index: usize,
    pub total_seeds: usize,
    pub stage: SampleProgressStage,
    pub stage_index: usize,
    pub stage_count: usize,
    pub candidate_count: Option<usize>,
}

#[derive(Debug, Error)]
pub enum SampleCollectionError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("duckdb error: {0}")]
    DuckDb(#[from] duckdb::Error),
    #[error("input file did not contain any contract addresses")]
    EmptyInput,
    #[error("feature DB does not exist: {0}")]
    MissingFeatureDb(String),
}

#[derive(Clone, Debug, Default)]
struct NftTextRow {
    token_id: String,
    name: String,
    metadata_doc: String,
    metadata_json: String,
}

#[derive(Clone, Debug)]
struct NameCandidate {
    contract_address: String,
    display_name: String,
    normalized_names: Vec<String>,
}

#[derive(Clone, Debug)]
struct MetadataCandidate {
    contract_address: String,
    doc: MetadataDocument,
    sketch: MetadataSketch,
}

#[derive(Clone, Debug, Default)]
struct TextIndex {
    names: Vec<NameCandidate>,
    name_indices_by_normalized: Vec<NormalizedNameEntry>,
    official_contract_addresses: BTreeSet<String>,
    metadata: Vec<MetadataCandidate>,
    metadata_token_ids: HashMap<String, TokenId>,
    metadata_corpus: MetadataCorpus,
    metadata_indices_by_contract: HashMap<String, Vec<usize>>,
    metadata_source_index: MetadataSourceIndex,
    metadata_corpus_exclusions_by_contract: HashMap<String, ContractMetadataCorpusExclusion>,
}

#[derive(Clone, Debug)]
struct NormalizedNameEntry {
    normalized: String,
    candidate_indices: Vec<usize>,
}

#[derive(Default)]
struct SampleScratch {
    metadata_seen_epochs: Vec<u32>,
    metadata_epoch: u32,
    metadata_verify_count: usize,
}

#[derive(Clone, Debug, Default)]
struct MetadataSketch {
    simhash: u64,
    anchors: Vec<TokenId>,
}

#[derive(Clone, Debug, Default)]
struct MetadataSourceIndex {
    anchor_indices: HashMap<TokenId, Vec<usize>>,
    simhash_band_indices: Vec<Vec<usize>>,
}

#[derive(Clone, Debug, Default)]
struct MetadataSourceCandidates {
    indices: Vec<usize>,
    bucket_hits: usize,
    verified: usize,
    full_scan: bool,
}

#[derive(Clone, Debug, Default)]
struct ContractMetadataCorpusExclusion {
    doc_count: usize,
    term_count: usize,
    doc_freqs: HashMap<TokenId, usize>,
}

#[derive(Clone, Copy)]
struct SeedPosition {
    index: usize,
    total: usize,
}

impl SampleScratch {
    fn new(metadata_len: usize) -> Self {
        Self {
            metadata_seen_epochs: vec![0; metadata_len],
            metadata_epoch: 0,
            metadata_verify_count: 0,
        }
    }

    fn next_metadata_epoch(&mut self) -> u32 {
        self.metadata_epoch = self.metadata_epoch.wrapping_add(1);
        if self.metadata_epoch == 0 {
            self.metadata_seen_epochs.fill(0);
            self.metadata_epoch = 1;
        }
        self.metadata_epoch
    }
}

#[derive(Default)]
struct TokenInterner {
    ids: HashMap<String, TokenId>,
}

impl TokenInterner {
    fn intern(&mut self, token: String) -> TokenId {
        if let Some(token_id) = self.ids.get(&token) {
            return *token_id;
        }
        let token_id = self.ids.len() as TokenId;
        self.ids.insert(token, token_id);
        token_id
    }

    fn into_ids(self) -> HashMap<String, TokenId> {
        self.ids
    }
}

pub fn collect_samples(
    config: SampleCollectionConfig,
) -> Result<SampleReport, SampleCollectionError> {
    collect_samples_with_progress(config, |_| {})
}

pub fn collect_samples_with_progress<F>(
    config: SampleCollectionConfig,
    mut progress: F,
) -> Result<SampleReport, SampleCollectionError>
where
    F: FnMut(SampleProgress),
{
    ensure_feature_db_exists(&config.feature_db)?;
    let seed_contracts = read_seed_contracts(&config.input)?;
    let total = seed_contracts.len();
    let conn = open_read_only_connection_with_options(&config.feature_db, &config)?;
    let text_index = load_text_index(&conn, &config.chain)?;
    let mut scratch = SampleScratch::new(text_index.metadata.len());
    let mut seed_reports = Vec::with_capacity(seed_contracts.len());

    for (index, seed_contract) in seed_contracts.iter().enumerate() {
        seed_reports.push(process_seed_contract(
            &conn,
            &text_index,
            &mut scratch,
            &config,
            seed_contract,
            SeedPosition {
                index: index + 1,
                total,
            },
            &mut progress,
        )?);
    }

    let report = SampleReport {
        chain: config.chain.clone(),
        seed_reports,
    };
    write_markdown_report(&config.output, &report)?;
    Ok(report)
}

fn process_seed_contract(
    conn: &Connection,
    text_index: &TextIndex,
    scratch: &mut SampleScratch,
    config: &SampleCollectionConfig,
    seed_contract: &str,
    position: SeedPosition,
    progress: &mut impl FnMut(SampleProgress),
) -> Result<SeedSampleReport, SampleCollectionError> {
    let seed_rows = read_seed_rows(conn, &config.chain, seed_contract, config.max_seed_tokens)?;
    emit_progress(
        progress,
        position.index,
        position.total,
        SampleProgressStage::ReadSeedRows,
        1,
        None,
    );

    emit_progress(
        progress,
        position.index,
        position.total,
        SampleProgressStage::LoadNameCandidates,
        2,
        Some(text_index.names.len()),
    );
    let seed_name = first_seed_name(&seed_rows);
    let seed_name_norms = seed_name_norms(&seed_rows);
    let name_matches = match_names(
        &seed_name_norms,
        text_index,
        seed_contract,
        config.name_threshold,
        config.max_recall_rows,
    );
    emit_progress(
        progress,
        position.index,
        position.total,
        SampleProgressStage::ScoreNameCandidates,
        3,
        Some(name_matches.len()),
    );

    let seed_metadata = first_seed_metadata(&seed_rows);
    emit_progress(
        progress,
        position.index,
        position.total,
        SampleProgressStage::PrepareMetadataQuery,
        4,
        Some(text_index.metadata.len()),
    );
    let metadata_matches = match_metadata(
        conn,
        &config.chain,
        &seed_rows,
        text_index,
        scratch,
        seed_contract,
        config.metadata_threshold,
        config.max_recall_rows,
        position,
        progress,
    )?;
    emit_progress(
        progress,
        position.index,
        position.total,
        SampleProgressStage::BuildReport,
        13,
        None,
    );

    let report = SeedSampleReport {
        name: build_name_comparison(seed_name, name_matches),
        metadata: build_metadata_comparison(seed_metadata, metadata_matches),
    };
    let match_count = report.name.matches.len() + report.metadata.matches.len();
    emit_progress(
        progress,
        position.index,
        position.total,
        SampleProgressStage::FinishedSeed,
        14,
        Some(match_count),
    );
    Ok(report)
}

fn emit_progress(
    progress: &mut impl FnMut(SampleProgress),
    seed_index: usize,
    total_seeds: usize,
    stage: SampleProgressStage,
    stage_index: usize,
    candidate_count: Option<usize>,
) {
    progress(SampleProgress {
        seed_index,
        total_seeds,
        stage,
        stage_index,
        stage_count: SAMPLE_PROGRESS_STAGE_COUNT,
        candidate_count,
    });
}

fn ensure_feature_db_exists(path: &Path) -> Result<(), SampleCollectionError> {
    if path == Path::new(":memory:") || path.exists() {
        Ok(())
    } else {
        Err(SampleCollectionError::MissingFeatureDb(
            path.to_string_lossy().into_owned(),
        ))
    }
}

fn read_seed_contracts(path: &Path) -> Result<Vec<String>, SampleCollectionError> {
    let content = fs::read_to_string(path)?;
    let contracts = content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(|line| line.to_lowercase())
        .collect::<Vec<_>>();
    if contracts.is_empty() {
        Err(SampleCollectionError::EmptyInput)
    } else {
        Ok(contracts)
    }
}

fn read_seed_rows(
    conn: &Connection,
    chain: &str,
    contract_address: &str,
    limit: usize,
) -> Result<Vec<NftTextRow>, duckdb::Error> {
    let limit_sql = if limit > 0 {
        format!(" LIMIT {limit}")
    } else {
        String::new()
    };
    let sql = format!(
        "
        WITH seed_rows AS (
            SELECT CAST(token_id AS VARCHAR) AS token_id,
                   coalesce(CAST(name AS VARCHAR), '') AS name,
                   coalesce(CAST(metadata_doc AS VARCHAR), '') AS metadata_doc,
                   coalesce(CAST(metadata_json AS VARCHAR), '') AS metadata_json
            FROM nft_features
            WHERE chain = ? AND lower(contract_address) = ?
        )
        SELECT token_id,
               name,
               CASE
                   WHEN length(trim(metadata_json)) <= {MAX_METADATA_BYTES_FOR_DEDUP}
                        AND (
                            trim(metadata_json) LIKE '{{%'
                            OR trim(metadata_json) LIKE '[%'
                        )
                   THEN metadata_doc
                   ELSE ''
               END AS metadata_doc,
               CASE
                   WHEN length(trim(metadata_json)) <= {MAX_METADATA_BYTES_FOR_DEDUP}
                        AND (
                            trim(metadata_json) LIKE '{{%'
                            OR trim(metadata_json) LIKE '[%'
                        )
                   THEN metadata_json
                   ELSE ''
               END AS metadata_json
        FROM seed_rows
        ORDER BY token_id
        {limit_sql}
        "
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(params![chain, contract_address.to_lowercase()], |row| {
        Ok(NftTextRow {
            token_id: row.get::<_, String>(0)?,
            name: row.get::<_, String>(1)?,
            metadata_doc: row.get::<_, String>(2)?,
            metadata_json: row.get::<_, String>(3)?,
        })
    })?;
    rows.collect()
}

fn load_text_index(conn: &Connection, chain: &str) -> Result<TextIndex, duckdb::Error> {
    let names = load_name_index(conn, chain)?;
    let name_indices_by_normalized = build_name_indices_by_normalized(&names);
    let official_contract_addresses = build_official_contract_addresses(&names);
    let mut metadata_token_interner = TokenInterner::default();
    let mut metadata = load_metadata_index(conn, chain, &mut metadata_token_interner)?;
    let metadata_token_ids = metadata_token_interner.into_ids();
    let metadata_corpus = MetadataCorpus::from_documents(metadata.iter().map(|item| &item.doc));
    populate_metadata_sketches(&mut metadata, &metadata_corpus);
    let metadata_indices_by_contract = build_metadata_indices_by_contract(&metadata);
    let metadata_source_index = build_metadata_source_index(&metadata);
    let metadata_corpus_exclusions_by_contract =
        build_contract_metadata_corpus_exclusions(&metadata);
    Ok(TextIndex {
        names,
        name_indices_by_normalized,
        official_contract_addresses,
        metadata,
        metadata_token_ids,
        metadata_corpus,
        metadata_indices_by_contract,
        metadata_source_index,
        metadata_corpus_exclusions_by_contract,
    })
}

fn build_official_contract_addresses(names: &[NameCandidate]) -> BTreeSet<String> {
    names
        .iter()
        .filter(|candidate| is_official_contract_name(&candidate.display_name))
        .map(|candidate| candidate.contract_address.clone())
        .collect()
}

fn load_name_index(conn: &Connection, chain: &str) -> Result<Vec<NameCandidate>, duckdb::Error> {
    let mut stmt = conn.prepare(
        "
        SELECT lower(contract_address) AS contract_address,
               min(trim(coalesce(CAST(name AS VARCHAR), ''))) AS name
        FROM nft_features
        WHERE chain = ? AND trim(coalesce(CAST(name AS VARCHAR), '')) <> ''
        GROUP BY lower(contract_address)
        ORDER BY contract_address
        ",
    )?;
    let rows = stmt.query_map(params![chain], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;

    let mut candidates = Vec::new();
    for row in rows {
        let (contract_address, name) = row?;
        let trimmed = name.trim();
        if trimmed.is_empty() {
            continue;
        }
        let normalized = normalize_name(trimmed);
        if normalized.is_empty() {
            continue;
        }
        candidates.push(NameCandidate {
            contract_address,
            display_name: trimmed.to_string(),
            normalized_names: vec![normalized],
        });
    }

    candidates.sort_by(|left, right| {
        (&left.display_name, &left.contract_address)
            .cmp(&(&right.display_name, &right.contract_address))
    });
    Ok(candidates)
}

fn build_name_indices_by_normalized(names: &[NameCandidate]) -> Vec<NormalizedNameEntry> {
    let mut by_normalized = HashMap::<String, Vec<usize>>::new();
    for (candidate_index, candidate) in names.iter().enumerate() {
        for normalized in &candidate.normalized_names {
            by_normalized
                .entry(normalized.clone())
                .or_default()
                .push(candidate_index);
        }
    }
    let mut entries = by_normalized
        .into_iter()
        .map(|(normalized, candidate_indices)| NormalizedNameEntry {
            normalized,
            candidate_indices,
        })
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| left.normalized.cmp(&right.normalized));
    entries
}

fn load_metadata_index(
    conn: &Connection,
    chain: &str,
    token_interner: &mut TokenInterner,
) -> Result<Vec<MetadataCandidate>, duckdb::Error> {
    let mut stmt = conn.prepare(
        "
        WITH selected AS (
            SELECT lower(contract_address) AS contract_address,
                   CAST(token_id AS VARCHAR) AS token_id,
                   coalesce(CAST(metadata_doc AS VARCHAR), '') AS metadata_doc,
                   coalesce(CAST(metadata_json AS VARCHAR), '') AS metadata_json
            FROM nft_features
            WHERE chain = ?
        ),
        ranked AS (
            SELECT contract_address, token_id, metadata_doc, metadata_json,
                   row_number() OVER (
                       PARTITION BY contract_address
                       ORDER BY token_id
                   ) AS metadata_rank
            FROM selected
        )
        SELECT contract_address, metadata_doc, metadata_json
        FROM ranked
        WHERE metadata_rank = 1
          AND length(trim(metadata_json)) <= 65536
          AND (
              trim(metadata_json) LIKE '{%'
              OR trim(metadata_json) LIKE '[%'
          )
        ORDER BY contract_address
        ",
    )?;
    let rows = stmt.query_map(params![chain], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;

    let mut candidates = Vec::new();
    for row in rows {
        let (contract_address, metadata_doc, metadata_json) = row?;
        let doc_text = record_metadata_prefilter_text(&metadata_doc, &metadata_json);
        let Some(doc) = MetadataDocument::from_text_with_interner(&doc_text, token_interner) else {
            continue;
        };
        candidates.push(MetadataCandidate {
            contract_address,
            doc,
            sketch: MetadataSketch::default(),
        });
    }
    Ok(candidates)
}

fn build_metadata_indices_by_contract(
    metadata: &[MetadataCandidate],
) -> HashMap<String, Vec<usize>> {
    let mut index = HashMap::<String, Vec<usize>>::new();
    for (doc_index, candidate) in metadata.iter().enumerate() {
        index
            .entry(candidate.contract_address.clone())
            .or_default()
            .push(doc_index);
    }
    index
}

fn build_metadata_source_index(metadata: &[MetadataCandidate]) -> MetadataSourceIndex {
    let mut index = MetadataSourceIndex {
        anchor_indices: HashMap::new(),
        simhash_band_indices: vec![
            Vec::new();
            METADATA_SIMHASH_BAND_COUNT * METADATA_SIMHASH_BAND_VALUES
        ],
    };
    for (metadata_index, candidate) in metadata.iter().enumerate() {
        if candidate.sketch.is_empty() {
            continue;
        }
        for anchor in &candidate.sketch.anchors {
            index
                .anchor_indices
                .entry(*anchor)
                .or_default()
                .push(metadata_index);
        }
        for band_index in 0..METADATA_SIMHASH_BAND_COUNT {
            let band_value = metadata_simhash_band_value(candidate.sketch.simhash, band_index);
            index.simhash_band_indices[metadata_simhash_band_key(band_index, band_value)]
                .push(metadata_index);
        }
    }
    index
}

fn build_contract_metadata_corpus_exclusions(
    metadata: &[MetadataCandidate],
) -> HashMap<String, ContractMetadataCorpusExclusion> {
    let mut exclusions = HashMap::<String, ContractMetadataCorpusExclusion>::new();
    for candidate in metadata {
        let exclusion = exclusions
            .entry(candidate.contract_address.clone())
            .or_default();
        exclusion.doc_count += 1;
        exclusion.term_count += candidate.doc.tokens.len();
        for token in &candidate.doc.unique_tokens {
            *exclusion.doc_freqs.entry(*token).or_insert(0) += 1;
        }
    }
    exclusions
}

fn populate_metadata_sketches(metadata: &mut [MetadataCandidate], corpus: &MetadataCorpus) {
    for candidate in metadata {
        candidate.sketch =
            metadata_sketch_from_document(&candidate.doc, corpus.total_docs, |token| {
                corpus.doc_freqs.get(&token).copied().unwrap_or(0)
            });
    }
}

fn metadata_sketch_from_document(
    document: &MetadataDocument,
    total_docs: usize,
    mut document_frequency: impl FnMut(TokenId) -> usize,
) -> MetadataSketch {
    let mut weights = [0.0f64; 64];
    let mut anchors = Vec::<(TokenId, f64)>::new();
    for token in &document.unique_tokens {
        let df = document_frequency(*token);
        let idf = metadata_token_idf(total_docs, df);
        let token_hash = stable_token_hash(*token);
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
        push_metadata_anchor_candidate(&mut anchors, (*token, idf));
    }
    let simhash = metadata_simhash_from_weights(weights);
    let mut anchors = anchors
        .into_iter()
        .map(|(token, _)| token)
        .collect::<Vec<_>>();
    anchors.sort_unstable();
    MetadataSketch { simhash, anchors }
}

fn push_metadata_anchor_candidate(anchors: &mut Vec<(TokenId, f64)>, candidate: (TokenId, f64)) {
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

fn compare_metadata_anchor_quality(
    left: &(TokenId, f64),
    right: &(TokenId, f64),
) -> std::cmp::Ordering {
    left.1
        .partial_cmp(&right.1)
        .unwrap_or(std::cmp::Ordering::Equal)
        .then_with(|| right.0.cmp(&left.0))
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

fn stable_token_hash(token: TokenId) -> u64 {
    let mut value = token as u64 + 0x9E37_79B9_7F4A_7C15;
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

fn metadata_token_idf(total_docs: usize, doc_freq: usize) -> f64 {
    (((total_docs + 1) as f64) / ((doc_freq + 1) as f64)).ln() + 1.0
}

fn metadata_token_is_high_frequency(total_docs: usize, doc_freq: usize) -> bool {
    doc_freq >= METADATA_SKETCH_HIGH_FREQ_MIN_DOCS
        && doc_freq.saturating_mul(METADATA_SKETCH_HIGH_FREQ_DIVISOR) > total_docs
}

impl MetadataSketch {
    fn is_empty(&self) -> bool {
        self.simhash == 0 && self.anchors.is_empty()
    }

    fn has_anchors(&self) -> bool {
        !self.anchors.is_empty()
    }
}

impl MetadataSourceIndex {
    fn is_empty(&self) -> bool {
        self.anchor_indices.is_empty() && self.simhash_band_indices.iter().all(Vec::is_empty)
    }
}

fn metadata_simhash_band_key(band_index: usize, band_value: u8) -> usize {
    band_index * METADATA_SIMHASH_BAND_VALUES + band_value as usize
}

fn metadata_simhash_band_value(simhash: u64, band_index: usize) -> u8 {
    ((simhash >> (band_index * METADATA_SIMHASH_BAND_BITS)) & 0xff) as u8
}

fn first_seed_name(rows: &[NftTextRow]) -> String {
    rows.iter()
        .find_map(|row| non_empty(&row.name))
        .unwrap_or_default()
}

fn seed_name_norms(rows: &[NftTextRow]) -> Vec<String> {
    rows.iter()
        .filter_map(|row| non_empty(&row.name))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|name| normalize_name(&name))
        .filter(|name| !name.is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn first_seed_metadata(rows: &[NftTextRow]) -> String {
    rows.first()
        .and_then(|row| non_empty(&record_metadata_text(&row.metadata_doc, &row.metadata_json)))
        .unwrap_or_default()
}

fn match_names(
    seed_name_norms: &[String],
    text_index: &TextIndex,
    seed_contract: &str,
    threshold: f64,
    limit: usize,
) -> Vec<String> {
    if seed_name_norms.is_empty() {
        return Vec::new();
    }
    if threshold > 100.0 {
        return Vec::new();
    }
    let seed_contract = seed_contract.to_lowercase();
    let mut matched_candidate_indices = if threshold >= 100.0 {
        exact_name_candidate_indices(seed_name_norms, text_index, &seed_contract)
    } else {
        text_index
            .name_indices_by_normalized
            .par_iter()
            .filter_map(|entry| {
                if !seed_name_norms.iter().any(|seed_name| {
                    score_normalized_name_pair(&entry.normalized, seed_name) >= threshold
                }) {
                    return None;
                }
                Some(
                    entry
                        .candidate_indices
                        .iter()
                        .copied()
                        .filter(|index| {
                            !is_excluded_candidate_contract(
                                text_index,
                                &text_index.names[*index].contract_address,
                                &seed_contract,
                            )
                        })
                        .collect::<Vec<_>>(),
                )
            })
            .flatten()
            .collect::<Vec<_>>()
    };
    matched_candidate_indices.sort_unstable();
    matched_candidate_indices.dedup();
    materialize_name_matches(text_index, matched_candidate_indices, limit)
}

fn exact_name_candidate_indices(
    seed_name_norms: &[String],
    text_index: &TextIndex,
    seed_contract: &str,
) -> Vec<usize> {
    let mut indices = Vec::new();
    for seed_name in seed_name_norms {
        if let Ok(entry_index) = text_index
            .name_indices_by_normalized
            .binary_search_by(|entry| entry.normalized.as_str().cmp(seed_name.as_str()))
        {
            indices.extend(
                text_index.name_indices_by_normalized[entry_index]
                    .candidate_indices
                    .iter()
                    .copied()
                    .filter(|index| {
                        !is_excluded_candidate_contract(
                            text_index,
                            &text_index.names[*index].contract_address,
                            seed_contract,
                        )
                    }),
            );
        }
    }
    indices
}

fn is_excluded_candidate_contract(
    text_index: &TextIndex,
    candidate_contract: &str,
    seed_contract: &str,
) -> bool {
    candidate_contract == seed_contract
        || text_index
            .official_contract_addresses
            .contains(candidate_contract)
}

fn seed_metadata_prefilter_text(seed_rows: &[NftTextRow]) -> Option<String> {
    seed_rows.first().and_then(|row| {
        non_empty(&record_metadata_prefilter_text(
            &row.metadata_doc,
            &row.metadata_json,
        ))
    })
}

fn match_metadata(
    conn: &Connection,
    chain: &str,
    seed_rows: &[NftTextRow],
    text_index: &TextIndex,
    scratch: &mut SampleScratch,
    seed_contract: &str,
    threshold: f64,
    limit: usize,
    position: SeedPosition,
    progress: &mut impl FnMut(SampleProgress),
) -> Result<Vec<FinalMetadataMatch>, duckdb::Error> {
    let Some(scoring_seed_metadata) = seed_metadata_prefilter_text(seed_rows) else {
        emit_empty_metadata_scoring_progress(progress, position);
        return Ok(Vec::new());
    };
    emit_progress(
        progress,
        position.index,
        position.total,
        SampleProgressStage::BuildMetadataSeedDoc,
        5,
        Some(scoring_seed_metadata.len()),
    );
    let Some(seed_doc) = MetadataDocument::from_text_with_vocab(
        &scoring_seed_metadata,
        &text_index.metadata_token_ids,
    ) else {
        emit_empty_metadata_scoring_progress(progress, position);
        return Ok(Vec::new());
    };
    let seed_contract = seed_contract.to_lowercase();
    let corpus = if text_index.official_contract_addresses.is_empty() {
        let corpus_exclusion = text_index
            .metadata_corpus_exclusions_by_contract
            .get(&seed_contract);
        MetadataCorpusView::from_exclusion(&text_index.metadata_corpus, corpus_exclusion)
    } else {
        let excluded_indices = metadata_corpus_excluded_indices(text_index, &seed_contract);
        MetadataCorpusView::new(
            &text_index.metadata_corpus,
            &text_index.metadata,
            &excluded_indices,
        )
    };
    if corpus.total_docs == 0 {
        emit_empty_metadata_scoring_progress(progress, position);
        return Ok(Vec::new());
    }

    emit_progress(
        progress,
        position.index,
        position.total,
        SampleProgressStage::BuildMetadataSeedSketch,
        6,
        Some(seed_doc.unique_tokens.len()),
    );
    let seed_sketch = metadata_sketch_from_document(&seed_doc, corpus.total_docs, |token| {
        corpus.document_frequency(token)
    });
    emit_progress(
        progress,
        position.index,
        position.total,
        SampleProgressStage::CollectMetadataSourceBuckets,
        7,
        Some(0),
    );
    let source_candidates = collect_metadata_source_candidates(
        &seed_sketch,
        text_index,
        scratch,
        &seed_contract,
        METADATA_SKETCH_SOURCE_HAMMING_THRESHOLD,
    );
    emit_progress(
        progress,
        position.index,
        position.total,
        SampleProgressStage::CollectMetadataSourceBuckets,
        7,
        Some(source_candidates.bucket_hits),
    );
    emit_progress(
        progress,
        position.index,
        position.total,
        SampleProgressStage::VerifyMetadataSourceBuckets,
        8,
        Some(source_candidates.verified),
    );
    let seed_query = PreparedMetadataQuery::new(seed_doc, &corpus);
    let candidate_indices = if source_candidates.full_scan {
        metadata_prefilter_score_candidate_indices(
            &seed_query,
            &text_index.metadata,
            &source_candidates.indices,
        )
    } else {
        source_candidates.indices
    };
    emit_progress(
        progress,
        position.index,
        position.total,
        SampleProgressStage::CollectMetadataCandidates,
        9,
        Some(candidate_indices.len()),
    );

    emit_progress(
        progress,
        position.index,
        position.total,
        SampleProgressStage::ScoreMetadataPrefilter,
        10,
        Some(candidate_indices.len()),
    );
    let matched_indices = candidate_indices
        .par_iter()
        .filter_map(|index| {
            let candidate = &text_index.metadata[*index];
            (seed_query.score(&candidate.doc) >= threshold).then_some(*index)
        })
        .collect::<Vec<_>>();
    let candidate_contracts = matched_indices
        .into_iter()
        .map(|index| text_index.metadata[index].contract_address.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    emit_progress(
        progress,
        position.index,
        position.total,
        SampleProgressStage::LoadOverlappingMetadata,
        11,
        Some(candidate_contracts.len()),
    );
    let matches = final_metadata_matches_for_overlapping_tokens(
        conn,
        chain,
        seed_rows,
        &candidate_contracts,
        threshold,
        limit,
    )?;
    emit_progress(
        progress,
        position.index,
        position.total,
        SampleProgressStage::ScoreOverlappingMetadata,
        12,
        Some(matches.len()),
    );
    Ok(matches)
}

fn metadata_corpus_excluded_indices(text_index: &TextIndex, seed_contract: &str) -> Vec<usize> {
    let mut excluded_indices = Vec::new();
    if let Some(indices) = text_index.metadata_indices_by_contract.get(seed_contract) {
        excluded_indices.extend(indices.iter().copied());
    }
    for contract in &text_index.official_contract_addresses {
        if let Some(indices) = text_index.metadata_indices_by_contract.get(contract) {
            excluded_indices.extend(indices.iter().copied());
        }
    }
    excluded_indices.sort_unstable();
    excluded_indices.dedup();
    excluded_indices
}

fn metadata_prefilter_score_candidate_indices(
    seed_query: &PreparedMetadataQuery<'_>,
    metadata: &[MetadataCandidate],
    candidate_indices: &[usize],
) -> Vec<usize> {
    candidate_indices
        .par_iter()
        .copied()
        .filter(|index| seed_query.has_term_overlap(&metadata[*index].doc))
        .collect()
}

fn collect_metadata_source_candidates(
    seed_sketch: &MetadataSketch,
    text_index: &TextIndex,
    scratch: &mut SampleScratch,
    seed_contract: &str,
    hamming_threshold: u32,
) -> MetadataSourceCandidates {
    scratch.metadata_verify_count = 0;
    if seed_sketch.is_empty() {
        return MetadataSourceCandidates::default();
    }
    let epoch = scratch.next_metadata_epoch();
    let mut candidates = MetadataSourceCandidates::default();
    if text_index.metadata_source_index.is_empty() {
        candidates = collect_metadata_source_candidates_full_scan(
            seed_sketch,
            text_index,
            seed_contract,
            hamming_threshold,
        );
        scratch.metadata_verify_count = candidates.verified;
    } else if estimate_metadata_source_bucket_hits(seed_sketch, text_index, hamming_threshold)
        >= text_index.metadata.len()
    {
        candidates = collect_metadata_source_candidates_full_scan(
            seed_sketch,
            text_index,
            seed_contract,
            hamming_threshold,
        );
        scratch.metadata_verify_count = candidates.verified;
    } else {
        collect_metadata_source_candidates_from_index(
            seed_sketch,
            text_index,
            scratch,
            seed_contract,
            hamming_threshold,
            epoch,
            &mut candidates,
        );
    }
    candidates.verified = scratch.metadata_verify_count;
    candidates
}

fn collect_metadata_source_candidates_full_scan(
    seed_sketch: &MetadataSketch,
    text_index: &TextIndex,
    seed_contract: &str,
    hamming_threshold: u32,
) -> MetadataSourceCandidates {
    let (verified, mut indices) = text_index
        .metadata
        .par_iter()
        .enumerate()
        .fold(
            || (0usize, Vec::new()),
            |mut output, (index, candidate)| {
                if candidate.contract_address == seed_contract {
                    return output;
                }
                if text_index
                    .official_contract_addresses
                    .contains(&candidate.contract_address)
                {
                    return output;
                }
                output.0 += 1;
                if metadata_sketch_source_match(seed_sketch, &candidate.sketch, hamming_threshold) {
                    output.1.push(index);
                }
                output
            },
        )
        .reduce(
            || (0usize, Vec::new()),
            |mut left, mut right| {
                left.0 += right.0;
                left.1.append(&mut right.1);
                left
            },
        );
    indices.sort_unstable();
    MetadataSourceCandidates {
        indices,
        bucket_hits: text_index.metadata.len(),
        verified,
        full_scan: true,
    }
}

fn estimate_metadata_source_bucket_hits(
    seed_sketch: &MetadataSketch,
    text_index: &TextIndex,
    hamming_threshold: u32,
) -> usize {
    let mut hits = 0usize;
    for anchor in &seed_sketch.anchors {
        if let Some(indices) = text_index.metadata_source_index.anchor_indices.get(anchor) {
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
            if let Some(indices) = text_index
                .metadata_source_index
                .simhash_band_indices
                .get(band_key)
            {
                hits = hits.saturating_add(indices.len());
            }
        }
    }
    hits
}

fn collect_metadata_source_candidates_from_index(
    seed_sketch: &MetadataSketch,
    text_index: &TextIndex,
    scratch: &mut SampleScratch,
    seed_contract: &str,
    hamming_threshold: u32,
    epoch: u32,
    candidates: &mut MetadataSourceCandidates,
) {
    for anchor in &seed_sketch.anchors {
        let Some(indices) = text_index.metadata_source_index.anchor_indices.get(anchor) else {
            continue;
        };
        for index in indices {
            push_metadata_source_candidate(
                *index,
                seed_sketch,
                text_index,
                scratch,
                seed_contract,
                hamming_threshold,
                epoch,
                candidates,
            );
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
            let Some(indices) = text_index
                .metadata_source_index
                .simhash_band_indices
                .get(band_key)
            else {
                continue;
            };
            for index in indices {
                push_metadata_source_candidate(
                    *index,
                    seed_sketch,
                    text_index,
                    scratch,
                    seed_contract,
                    hamming_threshold,
                    epoch,
                    candidates,
                );
            }
        }
    }
}

fn push_metadata_source_candidate(
    index: usize,
    seed_sketch: &MetadataSketch,
    text_index: &TextIndex,
    scratch: &mut SampleScratch,
    seed_contract: &str,
    hamming_threshold: u32,
    epoch: u32,
    candidates: &mut MetadataSourceCandidates,
) {
    candidates.bucket_hits += 1;
    if scratch.metadata_seen_epochs[index] == epoch {
        return;
    }
    scratch.metadata_seen_epochs[index] = epoch;
    let candidate = &text_index.metadata[index];
    if is_excluded_candidate_contract(text_index, &candidate.contract_address, seed_contract) {
        return;
    }
    scratch.metadata_verify_count += 1;
    if metadata_sketch_source_match(seed_sketch, &candidate.sketch, hamming_threshold) {
        candidates.indices.push(index);
    }
}

fn metadata_sketch_source_match(
    seed: &MetadataSketch,
    candidate: &MetadataSketch,
    hamming_threshold: u32,
) -> bool {
    if seed.is_empty() || candidate.is_empty() {
        return false;
    }
    if seed.has_anchors() && sorted_tokens_intersect(&seed.anchors, &candidate.anchors) {
        return true;
    }
    (seed.simhash ^ candidate.simhash).count_ones() <= hamming_threshold
}

fn sorted_tokens_intersect(left: &[TokenId], right: &[TokenId]) -> bool {
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

fn emit_empty_metadata_scoring_progress(
    progress: &mut impl FnMut(SampleProgress),
    position: SeedPosition,
) {
    for (stage, stage_index) in [
        (SampleProgressStage::BuildMetadataSeedDoc, 5),
        (SampleProgressStage::BuildMetadataSeedSketch, 6),
        (SampleProgressStage::CollectMetadataSourceBuckets, 7),
        (SampleProgressStage::VerifyMetadataSourceBuckets, 8),
        (SampleProgressStage::CollectMetadataCandidates, 9),
        (SampleProgressStage::ScoreMetadataPrefilter, 10),
        (SampleProgressStage::LoadOverlappingMetadata, 11),
        (SampleProgressStage::ScoreOverlappingMetadata, 12),
    ] {
        emit_progress(
            progress,
            position.index,
            position.total,
            stage,
            stage_index,
            Some(0),
        );
    }
}

struct FinalMetadataRow {
    token_id: String,
    text: String,
    doc: MetadataDocument,
}

struct FinalMetadataMatch {
    seed_text: String,
    match_text: String,
}

fn final_metadata_matches_for_overlapping_tokens(
    conn: &Connection,
    chain: &str,
    seed_rows: &[NftTextRow],
    candidate_contracts: &[String],
    threshold: f64,
    limit: usize,
) -> Result<Vec<FinalMetadataMatch>, duckdb::Error> {
    if candidate_contracts.is_empty() {
        return Ok(Vec::new());
    }
    let mut token_interner = TokenInterner::default();
    let seed_docs = seed_metadata_docs_by_token(seed_rows, &mut token_interner);
    if seed_docs.is_empty() {
        return Ok(Vec::new());
    }
    let seed_token_ids = seed_docs.keys().cloned().collect::<BTreeSet<_>>();
    let candidate_rows = read_candidate_metadata_rows(
        conn,
        chain,
        candidate_contracts,
        &seed_token_ids,
        &mut token_interner,
    )?;
    let mut matches = Vec::new();
    let mut seen_match_texts = BTreeSet::new();
    for contract in candidate_contracts {
        let Some(rows) = candidate_rows.get(contract) else {
            continue;
        };
        if let Some(match_pair) = first_overlapping_metadata_match(&seed_docs, rows, threshold) {
            if seen_match_texts.insert(match_pair.match_text.clone()) {
                matches.push(match_pair);
            }
        }
        if limit > 0 && matches.len() >= limit {
            break;
        }
    }
    if limit > 0 && matches.len() > limit {
        matches.truncate(limit);
    }
    Ok(matches)
}

fn seed_metadata_docs_by_token(
    seed_rows: &[NftTextRow],
    token_interner: &mut TokenInterner,
) -> BTreeMap<String, PreparedSingleMetadataQuery> {
    let mut docs = BTreeMap::new();
    for row in seed_rows {
        if row.token_id.trim().is_empty() {
            continue;
        }
        let display_text = record_metadata_text(&row.metadata_doc, &row.metadata_json);
        let scoring_text = record_metadata_doc(&row.metadata_doc, &row.metadata_json);
        let Some(doc) = MetadataDocument::from_text_with_interner(&scoring_text, token_interner)
        else {
            continue;
        };
        docs.entry(row.token_id.clone())
            .or_insert_with(|| PreparedSingleMetadataQuery::new(display_text, doc));
    }
    docs
}

fn read_candidate_metadata_rows(
    conn: &Connection,
    chain: &str,
    candidate_contracts: &[String],
    seed_token_ids: &BTreeSet<String>,
    token_interner: &mut TokenInterner,
) -> Result<BTreeMap<String, Vec<FinalMetadataRow>>, duckdb::Error> {
    if seed_token_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let token_values = seed_token_ids
        .iter()
        .map(|value| sql_string_literal(value))
        .collect::<Vec<_>>()
        .join(", ");
    let mut rows_by_contract = BTreeMap::<String, Vec<FinalMetadataRow>>::new();
    for chunk in candidate_contracts.chunks(500) {
        let contract_values = chunk
            .iter()
            .map(|value| sql_string_literal(value))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "
            SELECT contract_address, token_id, metadata_doc, metadata_json
            FROM (
                SELECT lower(contract_address) AS contract_address,
                       CAST(token_id AS VARCHAR) AS token_id,
                       coalesce(CAST(metadata_doc AS VARCHAR), '') AS metadata_doc,
                       coalesce(CAST(metadata_json AS VARCHAR), '') AS metadata_json,
                       row_number() OVER (
                           PARTITION BY lower(contract_address)
                           ORDER BY CAST(token_id AS VARCHAR)
                       ) AS overlap_rank
                FROM nft_features
                WHERE chain = ?
                  AND lower(contract_address) IN ({contract_values})
                  AND CAST(token_id AS VARCHAR) IN ({token_values})
            )
            WHERE overlap_rank <= {MAX_OVERLAPPING_METADATA_ROWS_PER_CONTRACT}
              AND length(trim(metadata_json)) <= {MAX_METADATA_BYTES_FOR_DEDUP}
              AND (
                  trim(metadata_json) LIKE '{{%'
                  OR trim(metadata_json) LIKE '[%'
              )
            ORDER BY contract_address, token_id
            "
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![chain], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;
        for row in rows {
            let (contract_address, token_id, metadata_doc, metadata_json) = row?;
            let text = record_metadata_text(&metadata_doc, &metadata_json);
            let scoring_text = record_metadata_doc(&metadata_doc, &metadata_json);
            let Some(doc) =
                MetadataDocument::from_text_with_interner(&scoring_text, token_interner)
            else {
                continue;
            };
            rows_by_contract
                .entry(contract_address)
                .or_default()
                .push(FinalMetadataRow {
                    token_id,
                    text,
                    doc,
                });
        }
    }
    Ok(rows_by_contract)
}

fn first_overlapping_metadata_match(
    seed_queries: &BTreeMap<String, PreparedSingleMetadataQuery>,
    rows: &[FinalMetadataRow],
    threshold: f64,
) -> Option<FinalMetadataMatch> {
    if rows.len() == 1 {
        let row = &rows[0];
        let seed_query = seed_queries.get(&row.token_id)?;
        return (seed_query.score(&row.doc) >= threshold).then(|| FinalMetadataMatch {
            seed_text: seed_query.text.trim().to_string(),
            match_text: row.text.trim().to_string(),
        });
    }

    let corpus = MetadataCorpus::from_documents(rows.iter().map(|row| &row.doc));
    let corpus = MetadataCorpusView::from_corpus(&corpus);
    let mut seed_queries_by_token = BTreeMap::<&str, PreparedMetadataQuery<'_>>::new();
    let mut first_match = None;
    for row in rows {
        if !seed_queries_by_token.contains_key(row.token_id.as_str()) {
            if let Some(seed_query) = seed_queries.get(&row.token_id) {
                seed_queries_by_token.insert(
                    row.token_id.as_str(),
                    PreparedMetadataQuery::new(seed_query.document().clone(), &corpus),
                );
            }
        }
        let Some(seed_query) = seed_queries_by_token.get(row.token_id.as_str()) else {
            continue;
        };
        if seed_query.score(&row.doc) >= threshold {
            let match_text = row.text.trim().to_string();
            let match_pair = FinalMetadataMatch {
                seed_text: seed_queries
                    .get(&row.token_id)
                    .map(|query| query.text.trim().to_string())
                    .unwrap_or_default(),
                match_text,
            };
            if looks_like_json(&match_pair.match_text) {
                return Some(match_pair);
            }
            first_match.get_or_insert(match_pair);
        }
    }
    first_match
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn materialize_name_matches(
    text_index: &TextIndex,
    indices: Vec<usize>,
    limit: usize,
) -> Vec<String> {
    take_recall_limit(
        indices
            .into_iter()
            .map(|index| text_index.names[index].display_name.trim().to_string())
            .collect(),
        limit,
    )
}

fn take_recall_limit(values: BTreeSet<String>, limit: usize) -> Vec<String> {
    let iter = values.into_iter();
    if limit > 0 {
        iter.take(limit).collect()
    } else {
        iter.collect()
    }
}

fn is_official_contract_name(name: &str) -> bool {
    normalize_nfkc(name)
        .trim_start()
        .to_ascii_lowercase()
        .starts_with(ART_BLOCKS_STUDIO_NAME_PREFIX)
}

fn normalize_nfkc(raw: &str) -> String {
    raw.nfkc().collect::<String>()
}

fn strip_trailing_number_suffix(raw: &str) -> String {
    let mut text = normalize_nfkc(raw).trim().to_string();
    let mut changed = true;
    let mut guard = 0;
    while changed && guard < 20 {
        changed = false;
        guard += 1;
        for pattern in TRAILING_PATTERNS.iter() {
            let updated = pattern.replace(&text, "").trim().to_string();
            if updated != text {
                text = updated;
                changed = true;
                break;
            }
        }
        if !changed {
            let updated = ATTACHED_TRAILING_NUMBER_RE
                .replace(&text, "$1")
                .trim()
                .to_string();
            if updated != text {
                text = updated;
                changed = true;
            }
        }
    }
    WHITESPACE_RE.replace_all(&text, " ").trim().to_string()
}

fn normalize_name(raw: &str) -> String {
    strip_trailing_number_suffix(raw).to_lowercase()
}

fn normalize_text(raw: &str) -> String {
    let text = normalize_nfkc(raw).to_lowercase();
    WHITESPACE_RE.replace_all(text.trim(), " ").to_string()
}

fn flatten_metadata(value: &Value, parts: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            for (key, item) in map {
                let key = key.to_lowercase();
                if matches!(
                    key.as_str(),
                    "description"
                        | "trait_type"
                        | "value"
                        | "display_type"
                        | "image"
                        | "image_url"
                        | "animation_url"
                        | "external_url"
                        | "attributes"
                        | "metadata"
                        | "rawmetadata"
                        | "raw"
                ) {
                    flatten_metadata(item, parts);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                flatten_metadata(item, parts);
            }
        }
        Value::String(text) if !text.trim().is_empty() => {
            parts.push(text.trim().to_string());
        }
        _ => {}
    }
}

fn metadata_document_from_json(raw: &str) -> String {
    if raw.trim().is_empty() {
        return String::new();
    }
    match serde_json::from_str::<Value>(raw) {
        Ok(value) => {
            let mut parts = Vec::new();
            flatten_metadata(&value, &mut parts);
            normalize_text(&parts.join(" "))
        }
        Err(_) => normalize_text(raw),
    }
}

fn metadata_prefilter_text(raw: &str) -> String {
    if raw.trim().is_empty() {
        return String::new();
    }
    match serde_json::from_str::<Value>(raw) {
        Ok(value) => {
            let mut parts = BTreeSet::new();
            collect_metadata_prefilter_parts(&value, &mut Vec::new(), &mut parts);
            parts.into_iter().collect::<Vec<_>>().join(" ")
        }
        Err(_) => normalize_text(raw),
    }
}

fn collect_metadata_prefilter_parts(
    value: &Value,
    path: &mut Vec<String>,
    parts: &mut BTreeSet<String>,
) {
    match value {
        Value::Object(map) => {
            for (key, item) in map {
                let key_norm = normalize_text(key);
                if key_norm.is_empty() {
                    continue;
                }
                path.push(key_norm.clone());
                if is_structure_wrapper_key(&key_norm) {
                    collect_metadata_prefilter_parts(item, path, parts);
                } else if key_norm == "trait_type" {
                    push_metadata_prefilter_part(parts, &key_norm);
                    if let Some(text) = item.as_str() {
                        push_metadata_prefilter_part(parts, text);
                    }
                } else if metadata_prefilter_includes_value(&key_norm) {
                    push_metadata_prefilter_part(parts, &key_norm);
                    collect_metadata_prefilter_values(item, parts);
                } else {
                    push_metadata_prefilter_part(parts, &key_norm);
                    collect_metadata_prefilter_parts(item, path, parts);
                }
                path.pop();
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_metadata_prefilter_parts(item, path, parts);
            }
        }
        _ => {}
    }
}

fn collect_metadata_prefilter_values(value: &Value, parts: &mut BTreeSet<String>) {
    match value {
        Value::String(text) => push_metadata_prefilter_part(parts, text),
        Value::Number(number) => push_metadata_prefilter_part(parts, &number.to_string()),
        Value::Bool(value) => push_metadata_prefilter_part(parts, &value.to_string()),
        Value::Array(items) => {
            for item in items {
                collect_metadata_prefilter_values(item, parts);
            }
        }
        Value::Object(map) => {
            for (key, item) in map {
                push_metadata_prefilter_part(parts, key);
                collect_metadata_prefilter_values(item, parts);
            }
        }
        Value::Null => {}
    }
}

fn metadata_prefilter_includes_value(key: &str) -> bool {
    is_description_key(key) || is_platform_key(key)
}

fn push_metadata_prefilter_part(parts: &mut BTreeSet<String>, raw: &str) {
    let text = normalize_text(raw);
    if !text.is_empty() {
        parts.insert(text);
    }
}

fn metadata_is_dedup_eligible(metadata_doc: &str, metadata_json: &str) -> bool {
    let _ = metadata_doc;
    let metadata_json = metadata_json.trim();
    !metadata_json.is_empty()
        && metadata_json.len() <= MAX_METADATA_BYTES_FOR_DEDUP
        && looks_like_json(metadata_json)
}

fn record_metadata_doc(metadata_doc: &str, metadata_json: &str) -> String {
    if !metadata_is_dedup_eligible(metadata_doc, metadata_json) {
        return String::new();
    }
    metadata_document_from_json(metadata_json)
}

fn record_metadata_text(metadata_doc: &str, metadata_json: &str) -> String {
    if !metadata_is_dedup_eligible(metadata_doc, metadata_json) {
        return String::new();
    }
    metadata_json.to_string()
}

fn record_metadata_prefilter_text(metadata_doc: &str, metadata_json: &str) -> String {
    if !metadata_is_dedup_eligible(metadata_doc, metadata_json) {
        return String::new();
    }
    metadata_prefilter_text(metadata_json)
}

fn metadata_tokens(value: &str) -> Vec<String> {
    TOKEN_RE
        .find_iter(&normalize_text(value))
        .map(|m| m.as_str().to_string())
        .filter(|token| token.len() >= 2)
        .collect()
}

fn score_normalized_name_pair(left_norm: &str, right_norm: &str) -> f64 {
    if left_norm.is_empty() || right_norm.is_empty() {
        0.0
    } else if left_norm == right_norm {
        100.0
    } else {
        strsim::jaro_winkler(left_norm, right_norm) * 100.0
    }
}

#[derive(Clone, Debug)]
struct MetadataDocument {
    tokens: Vec<TokenId>,
    unique_tokens: Vec<TokenId>,
    term_freqs: Vec<(TokenId, usize)>,
}

impl MetadataDocument {
    fn from_text_with_interner(value: &str, interner: &mut TokenInterner) -> Option<Self> {
        let tokens = metadata_tokens(value)
            .into_iter()
            .map(|token| interner.intern(token))
            .collect();
        Self::from_tokens(tokens)
    }

    fn from_text_with_vocab(value: &str, vocab: &HashMap<String, TokenId>) -> Option<Self> {
        let mut unknown_ids = HashMap::<String, TokenId>::new();
        let mut next_unknown_id = vocab.len() as TokenId;
        let tokens = metadata_tokens(value)
            .into_iter()
            .map(|token| {
                if let Some(token_id) = vocab.get(&token) {
                    *token_id
                } else if let Some(token_id) = unknown_ids.get(&token) {
                    *token_id
                } else {
                    let token_id = next_unknown_id;
                    next_unknown_id += 1;
                    unknown_ids.insert(token, token_id);
                    token_id
                }
            })
            .collect();
        Self::from_tokens(tokens)
    }

    fn from_tokens(tokens: Vec<TokenId>) -> Option<Self> {
        if tokens.is_empty() {
            return None;
        }
        let mut term_freqs = HashMap::<TokenId, usize>::new();
        for token in &tokens {
            *term_freqs.entry(*token).or_insert(0) += 1;
        }
        let unique_tokens = tokens
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let mut term_freqs = term_freqs.into_iter().collect::<Vec<_>>();
        term_freqs.sort_by_key(|(token, _)| *token);
        Some(Self {
            tokens,
            unique_tokens,
            term_freqs,
        })
    }

    fn term_frequency(&self, token: TokenId) -> usize {
        self.term_freqs
            .binary_search_by_key(&token, |(candidate_token, _)| *candidate_token)
            .map(|index| self.term_freqs[index].1)
            .unwrap_or(0)
    }
}

#[derive(Clone, Debug, Default)]
struct MetadataCorpus {
    total_docs: usize,
    total_terms: usize,
    doc_freqs: HashMap<TokenId, usize>,
}

impl MetadataCorpus {
    fn from_documents<'a>(documents: impl Iterator<Item = &'a MetadataDocument>) -> Self {
        let mut corpus = Self::default();
        for document in documents {
            corpus.total_docs += 1;
            corpus.total_terms += document.tokens.len();
            for token in &document.unique_tokens {
                *corpus.doc_freqs.entry(*token).or_insert(0) += 1;
            }
        }
        corpus
    }
}

struct MetadataCorpusView<'a> {
    base: &'a MetadataCorpus,
    excluded_doc_freqs: Option<&'a HashMap<TokenId, usize>>,
    owned_excluded_doc_freqs: HashMap<TokenId, usize>,
    total_docs: usize,
    avg_doc_len: f64,
}

struct PreparedMetadataQuery<'a> {
    terms: Vec<(TokenId, usize)>,
    denominator: f64,
    corpus: &'a MetadataCorpusView<'a>,
}

#[derive(Clone)]
struct PreparedSingleMetadataQuery {
    document: MetadataDocument,
    terms: Vec<(TokenId, usize)>,
    text: String,
}

impl<'a> MetadataCorpusView<'a> {
    fn from_corpus(base: &'a MetadataCorpus) -> Self {
        let avg_doc_len = if base.total_docs == 0 {
            0.0
        } else {
            base.total_terms as f64 / base.total_docs as f64
        };
        Self {
            base,
            excluded_doc_freqs: None,
            owned_excluded_doc_freqs: HashMap::new(),
            total_docs: base.total_docs,
            avg_doc_len,
        }
    }

    fn new(
        base: &'a MetadataCorpus,
        documents: &'a [MetadataCandidate],
        excluded_indices: &[usize],
    ) -> Self {
        let mut excluded_doc_freqs = HashMap::new();
        let mut excluded_terms = 0usize;
        for index in excluded_indices {
            let document = &documents[*index].doc;
            excluded_terms += document.tokens.len();
            for token in &document.unique_tokens {
                *excluded_doc_freqs.entry(*token).or_insert(0) += 1;
            }
        }
        let total_docs = base.total_docs.saturating_sub(excluded_indices.len());
        let total_terms = base.total_terms.saturating_sub(excluded_terms);
        let avg_doc_len = if total_docs == 0 {
            0.0
        } else {
            total_terms as f64 / total_docs as f64
        };
        Self {
            base,
            excluded_doc_freqs: None,
            owned_excluded_doc_freqs: excluded_doc_freqs,
            total_docs,
            avg_doc_len,
        }
    }

    fn from_exclusion(
        base: &'a MetadataCorpus,
        exclusion: Option<&'a ContractMetadataCorpusExclusion>,
    ) -> Self {
        let excluded_docs = exclusion.map(|item| item.doc_count).unwrap_or(0);
        let excluded_terms = exclusion.map(|item| item.term_count).unwrap_or(0);
        let total_docs = base.total_docs.saturating_sub(excluded_docs);
        let total_terms = base.total_terms.saturating_sub(excluded_terms);
        let avg_doc_len = if total_docs == 0 {
            0.0
        } else {
            total_terms as f64 / total_docs as f64
        };
        Self {
            base,
            excluded_doc_freqs: exclusion.map(|item| &item.doc_freqs),
            owned_excluded_doc_freqs: HashMap::new(),
            total_docs,
            avg_doc_len,
        }
    }

    fn document_frequency(&self, token: TokenId) -> usize {
        let excluded_frequency = self
            .excluded_doc_freqs
            .and_then(|doc_freqs| doc_freqs.get(&token).copied())
            .or_else(|| self.owned_excluded_doc_freqs.get(&token).copied())
            .unwrap_or(0);
        self.base
            .doc_freqs
            .get(&token)
            .copied()
            .unwrap_or(0)
            .saturating_sub(excluded_frequency)
    }
}

impl<'a> PreparedMetadataQuery<'a> {
    fn new(document: MetadataDocument, corpus: &'a MetadataCorpusView<'a>) -> Self {
        let terms = query_terms_from_tokens(&document.tokens);
        let self_score = bm25_score_terms(&terms, &document, corpus);
        let denominator = if self_score > 0.0 { self_score } else { 1.0 };
        Self {
            terms,
            denominator,
            corpus,
        }
    }

    fn score(&self, document: &MetadataDocument) -> f64 {
        (bm25_score_terms(&self.terms, document, self.corpus) / self.denominator).clamp(0.0, 1.0)
    }

    fn has_term_overlap(&self, document: &MetadataDocument) -> bool {
        sorted_token_terms_overlap(&self.terms, &document.term_freqs)
    }
}

fn sorted_token_terms_overlap(left: &[(TokenId, usize)], right: &[(TokenId, usize)]) -> bool {
    let mut left_index = 0usize;
    let mut right_index = 0usize;
    while left_index < left.len() && right_index < right.len() {
        let left_token = left[left_index].0;
        let right_token = right[right_index].0;
        match left_token.cmp(&right_token) {
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
            std::cmp::Ordering::Equal => return true,
        }
    }
    false
}

impl PreparedSingleMetadataQuery {
    fn new(text: String, document: MetadataDocument) -> Self {
        let terms = query_terms_from_tokens(&document.tokens);
        Self {
            document,
            terms,
            text,
        }
    }

    fn document(&self) -> &MetadataDocument {
        &self.document
    }

    fn score(&self, document: &MetadataDocument) -> f64 {
        let self_score =
            bm25_score_terms_with_single_document_corpus(&self.terms, &self.document, document);
        let denominator = if self_score > 0.0 { self_score } else { 1.0 };
        (bm25_score_terms_with_single_document_corpus(&self.terms, document, document)
            / denominator)
            .clamp(0.0, 1.0)
    }
}

#[cfg(test)]
fn score_metadata_pair(
    left: &MetadataDocument,
    right: &MetadataDocument,
    corpus: &MetadataCorpusView<'_>,
) -> f64 {
    PreparedMetadataQuery::new(left.clone(), corpus).score(right)
}

#[cfg(test)]
fn score_metadata_pair_with_single_document_corpus(
    left: &MetadataDocument,
    right: &MetadataDocument,
) -> f64 {
    PreparedSingleMetadataQuery::new(String::new(), left.clone()).score(right)
}

fn query_terms_from_tokens(query_tokens: &[TokenId]) -> Vec<(TokenId, usize)> {
    let mut query_terms = HashMap::<TokenId, usize>::new();
    for token in query_tokens {
        *query_terms.entry(*token).or_insert(0) += 1;
    }
    let mut query_terms = query_terms.into_iter().collect::<Vec<_>>();
    query_terms.sort_by_key(|(token, _)| *token);
    query_terms
}

fn bm25_score_terms_with_single_document_corpus(
    query_terms: &[(TokenId, usize)],
    document: &MetadataDocument,
    corpus_document: &MetadataDocument,
) -> f64 {
    if query_terms.is_empty() || document.tokens.is_empty() || corpus_document.tokens.is_empty() {
        return 0.0;
    }
    let doc_len = document.tokens.len() as f64;
    let avg_doc_len = corpus_document.tokens.len() as f64;
    let norm = METADATA_BM25_K1 * (1.0 - METADATA_BM25_B + METADATA_BM25_B * doc_len / avg_doc_len);
    query_terms
        .iter()
        .map(|(token, query_tf)| {
            let tf = document.term_frequency(*token) as f64;
            if tf == 0.0 {
                return 0.0;
            }
            let df = if corpus_document.term_frequency(*token) > 0 {
                1.0
            } else {
                0.0
            };
            let idf = ((1.0_f64 - df + 0.5) / (df + 0.5) + 1.0).ln();
            *query_tf as f64 * idf * (tf * (METADATA_BM25_K1 + 1.0)) / (tf + norm)
        })
        .sum()
}

fn bm25_score_terms(
    query_terms: &[(TokenId, usize)],
    document: &MetadataDocument,
    corpus: &MetadataCorpusView<'_>,
) -> f64 {
    if query_terms.is_empty()
        || document.tokens.is_empty()
        || corpus.total_docs == 0
        || corpus.avg_doc_len <= 0.0
    {
        return 0.0;
    }
    let doc_len = document.tokens.len() as f64;
    let norm =
        METADATA_BM25_K1 * (1.0 - METADATA_BM25_B + METADATA_BM25_B * doc_len / corpus.avg_doc_len);
    query_terms
        .iter()
        .map(|(token, query_tf)| {
            let tf = document.term_frequency(*token) as f64;
            if tf == 0.0 {
                return 0.0;
            }
            let df = corpus.document_frequency(*token) as f64;
            let total_docs = corpus.total_docs as f64;
            let idf = ((total_docs - df + 0.5) / (df + 0.5) + 1.0).ln();
            *query_tf as f64 * idf * (tf * (METADATA_BM25_K1 + 1.0)) / (tf + norm)
        })
        .sum()
}

fn non_empty(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn open_read_only_connection_with_options(
    path: &Path,
    config: &SampleCollectionConfig,
) -> Result<Connection, duckdb::Error> {
    let conn = if path == Path::new(":memory:") {
        Connection::open_in_memory()?
    } else {
        Connection::open_with_flags(path, Config::default().access_mode(AccessMode::ReadOnly)?)?
    };
    let memory_limit = config.duckdb_memory_limit.replace('\'', "''");
    conn.execute_batch(&format!(
        "
        PRAGMA threads={};
        PRAGMA memory_limit='{}';
        PRAGMA preserve_insertion_order=false;
        ",
        effective_duckdb_threads(config.duckdb_threads),
        memory_limit
    ))?;
    Ok(conn)
}

fn effective_duckdb_threads(requested: usize) -> usize {
    if requested > 0 {
        requested
    } else {
        std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1)
    }
}

fn build_name_comparison(seed: String, matches: Vec<String>) -> TextComparison {
    let labeled_matches = matches
        .iter()
        .map(|value| LabeledTextMatch {
            text: value.clone(),
            labels: classify_name_modifications(&seed, value),
        })
        .collect();
    TextComparison {
        seed,
        matches,
        labeled_matches,
    }
}

fn build_metadata_comparison(seed: String, matches: Vec<FinalMetadataMatch>) -> MetadataComparison {
    let seed = matches
        .first()
        .map(|value| value.seed_text.clone())
        .unwrap_or(seed);
    let paired_matches = matches
        .iter()
        .map(|value| MetadataPairMatch {
            seed: value.seed_text.clone(),
            text: value.match_text.clone(),
            labels: classify_metadata_modifications(&value.seed_text, &value.match_text),
        })
        .collect::<Vec<_>>();
    let matches = paired_matches
        .iter()
        .map(|value| value.text.clone())
        .collect::<Vec<_>>();
    let labeled_matches = paired_matches
        .iter()
        .map(|value| LabeledTextMatch {
            text: value.text.clone(),
            labels: value.labels.clone(),
        })
        .collect::<Vec<_>>();
    MetadataComparison {
        seed,
        matches,
        labeled_matches,
        paired_matches,
    }
}

const NAME_LABEL_EXACT_CLONE: &str = "exact_clone";
const NAME_LABEL_FORMAT_PERTURBATION: &str = "format_perturbation";
const NAME_LABEL_LEXICAL_MUTATION: &str = "lexical_mutation";
const NAME_LABEL_OTHER: &str = "other";
const NAME_LABEL_SUFFIX_AUGMENTATION: &str = "suffix_augmentation";

#[derive(Clone, Debug, PartialEq, Eq)]
enum NameClassificationState {
    ExactClone,
    Compare(NameComparisonState),
}

// Name labels are emitted from this state machine:
// ExactClone -> exact_clone; otherwise compare the value core after any confirmed
// suffix removal, then emit suffix, format, and lexical labels in stable order.
#[derive(Clone, Debug, PartialEq, Eq)]
struct NameComparisonState {
    seed_raw: String,
    value_core_after_suffix: String,
    lexical_seed_core: String,
    lexical_value_core: String,
    suffix_augmented: bool,
}

impl NameClassificationState {
    fn from_pair(seed: &str, value: &str) -> Self {
        let seed_raw = seed.trim().to_string();
        let value_raw = value.trim().to_string();
        if seed_raw == value_raw && !seed_raw.is_empty() {
            return Self::ExactClone;
        }

        let suffix_augmented = has_suffix_augmentation(&seed_raw, &value_raw);
        let value_core_after_suffix = if suffix_augmented {
            strip_raw_augmentation_suffix_matching_seed(&seed_raw, &value_raw)
        } else {
            value_raw
        };
        let seed_lexical_raw = if suffix_augmented {
            strip_raw_trailing_number_suffix(&seed_raw)
        } else {
            seed_raw.clone()
        };

        Self::Compare(NameComparisonState {
            lexical_seed_core: canonical_format_name(&seed_lexical_raw),
            lexical_value_core: canonical_format_name(&value_core_after_suffix),
            seed_raw,
            value_core_after_suffix,
            suffix_augmented,
        })
    }

    fn labels(&self) -> Vec<&'static str> {
        match self {
            Self::ExactClone => vec![NAME_LABEL_EXACT_CLONE],
            Self::Compare(state) => state.labels(),
        }
    }
}

impl NameComparisonState {
    fn labels(&self) -> Vec<&'static str> {
        let mut labels = Vec::new();
        if self.suffix_augmented {
            labels.push(NAME_LABEL_SUFFIX_AUGMENTATION);
        }
        if self.has_format_perturbation() {
            labels.push(NAME_LABEL_FORMAT_PERTURBATION);
        }
        if self.has_lexical_mutation() {
            labels.push(NAME_LABEL_LEXICAL_MUTATION);
        }
        if labels.is_empty() {
            labels.push(NAME_LABEL_OTHER);
        }
        labels
    }

    fn has_format_perturbation(&self) -> bool {
        has_name_format_perturbation(&self.seed_raw, &self.value_core_after_suffix)
    }

    fn has_lexical_mutation(&self) -> bool {
        has_name_lexical_mutation(&self.lexical_seed_core, &self.lexical_value_core)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NameFormatRelation {
    SameRaw,
    CanonicalEquivalent,
    SeparatorShiftedLexicalNeighbor,
    NoFormatEvidence,
}

impl NameFormatRelation {
    fn emits_format_label(self) -> bool {
        matches!(
            self,
            Self::CanonicalEquivalent | Self::SeparatorShiftedLexicalNeighbor
        )
    }
}

fn classify_name_modifications(seed: &str, value: &str) -> Vec<String> {
    NameClassificationState::from_pair(seed, value)
        .labels()
        .into_iter()
        .map(str::to_string)
        .collect()
}

fn classify_metadata_modifications(seed: &str, value: &str) -> Vec<String> {
    classify_json_metadata_modifications(seed, value)
}

fn looks_like_json(value: &str) -> bool {
    matches!(value.trim().chars().next(), Some('{') | Some('['))
}

fn classify_json_metadata_modifications(seed: &str, value: &str) -> Vec<String> {
    let seed_trimmed = seed.trim();
    let value_trimmed = value.trim();
    let seed_json = serde_json::from_str::<Value>(seed_trimmed);
    let value_json = serde_json::from_str::<Value>(value_trimmed);
    let (Ok(seed_json), Ok(value_json)) = (seed_json, value_json) else {
        if normalize_text(seed_trimmed) == normalize_text(value_trimmed)
            && !value_trimmed.is_empty()
        {
            return vec!["exact_match".to_string()];
        }
        return vec!["unparseable_changed".to_string()];
    };

    if seed_json == value_json {
        return vec!["exact_match".to_string()];
    }

    let mut labels = BTreeSet::new();
    diff_metadata_json(&seed_json, &value_json, &mut Vec::new(), &mut labels);
    if labels.is_empty() {
        labels.insert("metadata_unchanged".to_string());
    }
    labels.into_iter().collect()
}

fn diff_metadata_json(
    seed: &Value,
    value: &Value,
    path: &mut Vec<String>,
    labels: &mut BTreeSet<String>,
) {
    match (seed, value) {
        (Value::Object(seed_map), Value::Object(value_map)) => {
            if diff_metadata_wrapper_transform(seed_map, value_map, path, labels) {
                return;
            }
            let keys = seed_map
                .keys()
                .chain(value_map.keys())
                .map(|key| key.as_str())
                .collect::<BTreeSet<_>>();
            for key in keys {
                path.push(key.to_lowercase());
                match (seed_map.get(key), value_map.get(key)) {
                    (Some(seed_value), Some(value_value)) => {
                        diff_metadata_json(seed_value, value_value, path, labels);
                    }
                    (None, Some(value_value)) => {
                        if metadata_value_has_content(value_value) {
                            add_metadata_change(path, "added", labels);
                        }
                    }
                    (Some(seed_value), None) => {
                        if metadata_value_has_content(seed_value) {
                            add_metadata_change(path, "removed", labels);
                        }
                    }
                    (None, None) => {}
                }
                path.pop();
            }
        }
        (Value::Array(seed_items), Value::Array(value_items)) => {
            if same_json_multiset(seed_items, value_items) && seed_items != value_items {
                add_metadata_change(path, "reordered", labels);
                return;
            }
            if diff_keyed_attribute_array(seed_items, value_items, path, labels) {
                return;
            }
            let common_len = seed_items.len().min(value_items.len());
            for index in 0..common_len {
                path.push(index.to_string());
                diff_metadata_json(&seed_items[index], &value_items[index], path, labels);
                path.pop();
            }
            for value_item in &value_items[common_len..] {
                if !metadata_value_has_content(value_item) {
                    continue;
                }
                path.push(common_len.to_string());
                add_metadata_change(path, "added", labels);
                path.pop();
            }
            for seed_item in &seed_items[common_len..] {
                if !metadata_value_has_content(seed_item) {
                    continue;
                }
                path.push(common_len.to_string());
                add_metadata_change(path, "removed", labels);
                path.pop();
            }
        }
        _ if seed == value => {}
        _ => {
            let operation_region = metadata_region_for_path(path);
            add_metadata_label(operation_region, "replaced", labels);
        }
    }
}

fn metadata_value_has_content(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(_) | Value::Number(_) => true,
        Value::String(text) => !text.trim().is_empty(),
        Value::Array(items) => items.iter().any(metadata_value_has_content),
        Value::Object(map) => map.values().any(metadata_value_has_content),
    }
}

fn same_json_multiset(seed_items: &[Value], value_items: &[Value]) -> bool {
    if seed_items.len() != value_items.len() {
        return false;
    }
    let mut seed_canonical = seed_items.iter().map(canonical_json).collect::<Vec<_>>();
    let mut value_canonical = value_items.iter().map(canonical_json).collect::<Vec<_>>();
    seed_canonical.sort();
    value_canonical.sort();
    seed_canonical == value_canonical
}

fn canonical_json(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_default()
}

fn diff_metadata_wrapper_transform(
    seed_map: &serde_json::Map<String, Value>,
    value_map: &serde_json::Map<String, Value>,
    path: &mut Vec<String>,
    labels: &mut BTreeSet<String>,
) -> bool {
    let wrapped_seed = structure_wrapper_entry(seed_map);
    let wrapped_value = structure_wrapper_entry(value_map);
    if let (None, Some((wrapper_key, wrapped_value))) = (wrapped_seed, wrapped_value) {
        if seed_map.contains_key(wrapper_key) {
            return false;
        }
        add_metadata_label(Some("structure"), "added", labels);
        let seed_value = Value::Object(seed_map.clone());
        diff_metadata_json(&seed_value, wrapped_value, path, labels);
        diff_wrapper_siblings(value_map, wrapper_key, "added", path, labels);
        return true;
    }
    if let (Some((wrapper_key, wrapped_seed)), None) = (wrapped_seed, wrapped_value) {
        if value_map.contains_key(wrapper_key) {
            return false;
        }
        add_metadata_label(Some("structure"), "removed", labels);
        let value_value = Value::Object(value_map.clone());
        diff_metadata_json(wrapped_seed, &value_value, path, labels);
        diff_wrapper_siblings(seed_map, wrapper_key, "removed", path, labels);
        return true;
    }
    false
}

fn structure_wrapper_entry(map: &serde_json::Map<String, Value>) -> Option<(&str, &Value)> {
    let mut entries = map
        .iter()
        .filter(|(key, value)| is_structure_wrapper_key(&key.to_lowercase()) && value.is_object());
    let (key, value) = entries.next()?;
    if entries.next().is_some() {
        return None;
    }
    Some((key.as_str(), value))
}

fn diff_wrapper_siblings(
    map: &serde_json::Map<String, Value>,
    wrapper_key: &str,
    operation: &str,
    path: &mut Vec<String>,
    labels: &mut BTreeSet<String>,
) {
    for key in map.keys().filter(|key| key.as_str() != wrapper_key) {
        path.push(key.to_lowercase());
        add_metadata_change(path, operation, labels);
        path.pop();
    }
}

fn diff_keyed_attribute_array(
    seed_items: &[Value],
    value_items: &[Value],
    path: &mut Vec<String>,
    labels: &mut BTreeSet<String>,
) -> bool {
    if metadata_region_for_path(path) != Some("attributes") {
        return false;
    }
    let Some(seed_by_key) = attribute_items_by_key(seed_items) else {
        return false;
    };
    let Some(value_by_key) = attribute_items_by_key(value_items) else {
        return false;
    };
    if shared_attribute_order_changed(seed_items, value_items) {
        add_metadata_change(path, "reordered", labels);
    }

    let keys = seed_by_key
        .keys()
        .chain(value_by_key.keys())
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    for key in keys {
        match (seed_by_key.get(key), value_by_key.get(key)) {
            (Some(seed_index), Some(value_index)) => {
                path.push(key.to_string());
                diff_metadata_json(
                    &seed_items[*seed_index],
                    &value_items[*value_index],
                    path,
                    labels,
                );
                path.pop();
            }
            (None, Some(_value_index)) => {
                path.push(key.to_string());
                add_metadata_change(path, "added", labels);
                path.pop();
            }
            (Some(_seed_index), None) => {
                path.push(key.to_string());
                add_metadata_change(path, "removed", labels);
                path.pop();
            }
            (None, None) => {}
        }
    }
    true
}

fn attribute_items_by_key(items: &[Value]) -> Option<BTreeMap<String, usize>> {
    let mut by_key = BTreeMap::new();
    for (index, item) in items.iter().enumerate() {
        let key = attribute_item_key(item)?;
        if by_key.insert(key, index).is_some() {
            return None;
        }
    }
    Some(by_key)
}

fn shared_attribute_order_changed(seed_items: &[Value], value_items: &[Value]) -> bool {
    let seed_order = attribute_item_order(seed_items);
    let value_order = attribute_item_order(value_items);
    if seed_order.is_empty() || value_order.is_empty() {
        return false;
    }
    let shared = seed_order
        .iter()
        .filter(|key| value_order.contains(*key))
        .cloned()
        .collect::<BTreeSet<_>>();
    if shared.len() < 2 {
        return false;
    }
    let seed_shared = seed_order
        .into_iter()
        .filter(|key| shared.contains(key))
        .collect::<Vec<_>>();
    let value_shared = value_order
        .into_iter()
        .filter(|key| shared.contains(key))
        .collect::<Vec<_>>();
    seed_shared != value_shared
}

fn attribute_item_order(items: &[Value]) -> Vec<String> {
    items.iter().filter_map(attribute_item_key).collect()
}

fn attribute_item_key(item: &Value) -> Option<String> {
    let key = item
        .as_object()?
        .get("trait_type")?
        .as_str()
        .map(normalize_text)?;
    (!key.is_empty()).then_some(key)
}

fn add_metadata_change(path: &[String], operation: &str, labels: &mut BTreeSet<String>) {
    add_metadata_label(metadata_region_for_path(path), operation, labels);
}

fn add_metadata_label(
    region: Option<&'static str>,
    operation: &str,
    labels: &mut BTreeSet<String>,
) {
    let region = region.unwrap_or("auxiliary_fields");
    labels.insert(format!("{region}:{operation}"));
}

fn metadata_region_for_path(path: &[String]) -> Option<&'static str> {
    if path.is_empty() {
        return Some("structure");
    }
    let semantic_path = semantic_path_segments(path);
    let primary_segment = metadata_primary_region_segment(&semantic_path)?;
    if is_structure_wrapper_key(primary_segment) {
        return Some("structure");
    }

    if is_attribute_key(primary_segment) {
        return Some("attributes");
    }
    if is_platform_key(primary_segment) {
        return Some("platform_fields");
    }
    if is_reference_key(primary_segment) {
        return Some("references");
    }
    if is_title_key(primary_segment) {
        return Some("title");
    }
    if is_description_key(primary_segment) {
        return Some("description");
    }
    None
}

fn semantic_path_segments(path: &[String]) -> Vec<&str> {
    path.iter()
        .map(String::as_str)
        .filter(|part| !part.chars().all(|ch| ch.is_ascii_digit()))
        .collect()
}

fn metadata_primary_region_segment<'a>(semantic_path: &'a [&str]) -> Option<&'a str> {
    let first = semantic_path.first().copied()?;
    if is_structure_wrapper_key(first) {
        semantic_path.get(1).copied().or(Some(first))
    } else {
        Some(first)
    }
}

fn is_structure_wrapper_key(key: &str) -> bool {
    matches!(key, "metadata" | "rawmetadata" | "raw")
}

fn is_attribute_key(key: &str) -> bool {
    matches!(
        key,
        "attributes"
            | "attribute"
            | "traits"
            | "trait"
            | "trait_type"
            | "display_type"
            | "levels"
            | "level"
            | "stats"
            | "stat"
    )
}

fn is_reference_key(key: &str) -> bool {
    matches!(
        key,
        "image"
            | "image_url"
            | "image_data"
            | "animation_url"
            | "external_url"
            | "youtube_url"
            | "asset_url"
            | "media_url"
            | "background_image"
            | "thumbnail"
            | "uri"
            | "url"
    )
}

fn is_title_key(key: &str) -> bool {
    matches!(key, "name" | "title" | "token_name")
}

fn is_description_key(key: &str) -> bool {
    matches!(
        key,
        "description" | "bio" | "story" | "lore" | "summary" | "about"
    )
}

fn is_platform_key(key: &str) -> bool {
    matches!(
        key,
        "seller_fee_basis_points"
            | "fee_recipient"
            | "royalty"
            | "royalties"
            | "creator"
            | "creators"
            | "compiler"
            | "license"
            | "collection"
            | "marketplace"
            | "contract"
            | "chain"
    )
}

fn has_trailing_number_suffix(value: &str) -> bool {
    let normalized = normalize_nfkc(value);
    TRAILING_PATTERNS
        .iter()
        .any(|pattern| pattern.is_match(&normalized))
        || ATTACHED_TRAILING_NUMBER_RE.is_match(&normalized)
}

fn has_derivative_suffix(value: &str) -> bool {
    let value = normalize_nfkc(value);
    DERIVATIVE_SUFFIX_PATTERNS
        .iter()
        .any(|pattern| pattern.is_match(&value))
}

fn has_suffix_augmentation(seed: &str, value: &str) -> bool {
    has_seed_preserving_derivative_suffix(seed, value)
        || has_seed_preserving_numeric_suffix(seed, value)
}

fn has_seed_preserving_derivative_suffix(seed: &str, value: &str) -> bool {
    if !has_derivative_suffix(value) {
        return false;
    }
    let seed_canonical = canonical_format_name(seed);
    raw_augmentation_suffix_candidates(value)
        .iter()
        .any(|candidate| seed_canonical == canonical_format_name(candidate))
}

fn has_seed_preserving_numeric_suffix(seed: &str, value: &str) -> bool {
    let seed_has_numeric_suffix = has_trailing_number_suffix(seed);
    let value_has_numeric_suffix = has_trailing_number_suffix(value);
    if !seed_has_numeric_suffix && !value_has_numeric_suffix {
        return false;
    }
    let seed_norm = normalize_name(seed);
    let value_norm = normalize_name(value);
    if seed_norm == value_norm && seed != value {
        return true;
    }
    if !value_has_numeric_suffix {
        return false;
    }

    has_related_name_core(seed, &strip_raw_trailing_number_suffix(value))
}

fn has_related_name_core(seed: &str, value: &str) -> bool {
    let seed_core = strip_raw_augmentation_suffix(seed);
    let value_core = strip_raw_augmentation_suffix(value);
    let seed_core_norm = normalize_name(&seed_core);
    let value_core_norm = normalize_name(&value_core);
    if seed_core_norm.is_empty() || value_core_norm.is_empty() {
        return false;
    }
    seed_core_norm == value_core_norm
        || canonical_format_name(&seed_core) == canonical_format_name(&value_core)
        || has_name_lexical_mutation(&seed_core_norm, &value_core_norm)
}

fn canonical_format_name(value: &str) -> String {
    normalize_nfkc(value)
        .to_lowercase()
        .chars()
        .filter(|ch| !is_name_format_separator(*ch))
        .collect()
}

fn is_name_format_separator(ch: char) -> bool {
    ch.is_whitespace()
        || should_render_as_codepoint(ch)
        || matches!(ch, '-' | '_' | '.' | ':' | '：' | '/' | '\\' | '|')
}

fn has_name_format_perturbation(seed: &str, value: &str) -> bool {
    name_format_relation(seed, value).emits_format_label()
}

fn name_format_relation(seed: &str, value: &str) -> NameFormatRelation {
    let seed_trimmed = seed.trim();
    let value_trimmed = value.trim();
    if seed_trimmed == value_trimmed {
        return NameFormatRelation::SameRaw;
    }

    let seed_canonical = canonical_format_name(seed_trimmed);
    let value_canonical = canonical_format_name(value_trimmed);
    if seed_canonical == value_canonical {
        return NameFormatRelation::CanonicalEquivalent;
    }

    if has_name_format_separator_perturbation(seed_trimmed, value_trimmed)
        && !suffix_removal_absorbs_format_delta(seed_trimmed, value_trimmed)
        && seed_canonical.len().abs_diff(value_canonical.len()) <= 2
        && has_name_lexical_mutation(&seed_canonical, &value_canonical)
    {
        return NameFormatRelation::SeparatorShiftedLexicalNeighbor;
    }

    NameFormatRelation::NoFormatEvidence
}

fn has_name_format_separator_perturbation(seed: &str, value: &str) -> bool {
    let seed_signature = name_format_separator_signature(seed);
    let value_signature = name_format_separator_signature(value);
    seed_signature != value_signature && (seed_signature.is_empty() || value_signature.is_empty())
}

fn name_format_separator_signature(value: &str) -> Vec<char> {
    normalize_nfkc(value)
        .chars()
        .filter_map(|ch| {
            if ch.is_whitespace() {
                Some(' ')
            } else if should_render_as_codepoint(ch) {
                Some('\u{0}')
            } else if matches!(ch, '-' | '_' | '.' | ':' | '：' | '/' | '\\' | '|') {
                Some(ch)
            } else {
                None
            }
        })
        .collect()
}

fn suffix_removal_absorbs_format_delta(seed: &str, value: &str) -> bool {
    let seed_stripped = strip_raw_trailing_number_suffix(seed);
    let value_stripped = strip_raw_trailing_number_suffix(value);
    let same_after_number_suffix_removal = (seed_stripped != seed.trim()
        || value_stripped != value.trim())
        && canonical_format_name(&seed_stripped) == canonical_format_name(&value_stripped);
    let seed_suffix_removed_from_value = (has_trailing_number_suffix(seed)
        || has_derivative_suffix(seed))
        && !has_trailing_number_suffix(value)
        && !has_derivative_suffix(value);
    same_after_number_suffix_removal || seed_suffix_removed_from_value
}

fn strip_raw_trailing_number_suffix(raw: &str) -> String {
    let mut text = raw.trim().to_string();
    loop {
        let mut changed = false;
        for pattern in TRAILING_PATTERNS.iter() {
            let updated = pattern.replace(&text, "").trim().to_string();
            if updated != text {
                text = updated;
                changed = true;
                break;
            }
        }
        if !changed {
            let updated = ATTACHED_TRAILING_NUMBER_RE
                .replace(&text, "$1")
                .trim()
                .to_string();
            if updated != text {
                text = updated;
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    text
}

fn strip_raw_augmentation_suffix(raw: &str) -> String {
    raw_augmentation_suffix_candidates(raw)
        .last()
        .cloned()
        .unwrap_or_else(|| strip_raw_trailing_number_suffix(raw))
}

fn strip_raw_augmentation_suffix_matching_seed(seed: &str, raw: &str) -> String {
    let seed_canonical = canonical_format_name(seed);
    raw_augmentation_suffix_candidates(raw)
        .into_iter()
        .find(|candidate| canonical_format_name(candidate) == seed_canonical)
        .unwrap_or_else(|| strip_raw_augmentation_suffix(raw))
}

fn raw_augmentation_suffix_candidates(raw: &str) -> Vec<String> {
    let mut candidates = Vec::new();
    let mut text = strip_raw_trailing_number_suffix(raw);
    if text != raw.trim() {
        candidates.push(text.clone());
    }
    loop {
        let mut changed = false;
        for pattern in DERIVATIVE_SUFFIX_PATTERNS.iter() {
            let updated = pattern.replace(&text, "").trim().to_string();
            if updated != text {
                text = updated;
                candidates.push(text.clone());
                changed = true;
                break;
            }
        }
        if !changed {
            break;
        }
    }
    candidates
}

fn has_name_lexical_mutation(seed_norm: &str, value_norm: &str) -> bool {
    if seed_norm.is_empty() || value_norm.is_empty() || seed_norm == value_norm {
        return false;
    }
    score_normalized_name_pair(seed_norm, value_norm) >= 82.0
}

fn modification_summary<'a>(
    reports: impl Iterator<Item = &'a TextComparison>,
    classifier: fn(&str, &str) -> Vec<String>,
) -> (usize, BTreeMap<String, usize>) {
    let mut counts = BTreeMap::new();
    for comparison in reports {
        if comparison.labeled_matches.is_empty() {
            let total = comparison.matches.len();
            for value in &comparison.matches {
                for label in classifier(&comparison.seed, value) {
                    *counts.entry(label).or_default() += 1;
                }
            }
            *counts.entry("__total__".to_string()).or_default() += total;
        } else {
            *counts.entry("__total__".to_string()).or_default() += comparison.labeled_matches.len();
            for labeled in &comparison.labeled_matches {
                for label in &labeled.labels {
                    *counts.entry(label.clone()).or_default() += 1;
                }
            }
        }
    }
    let total = counts.remove("__total__").unwrap_or_default();
    (total, counts)
}

fn write_markdown_report(path: &Path, report: &SampleReport) -> Result<(), std::io::Error> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    fs::write(path, render_markdown_report(report))
}

fn render_markdown_report(report: &SampleReport) -> String {
    let mut out = String::new();
    out.push_str("# Name/Metadata Duplicate Samples\n\n");
    out.push_str(&format!("- chain: {}\n", report.chain));
    out.push_str(&format!(
        "- seed contracts: {}\n\n",
        report.seed_reports.len()
    ));

    push_modification_summary(&mut out, report);

    out.push_str("## Name Matches\n\n");
    for seed in &report.seed_reports {
        if !is_usable_seed_name(&seed.name.seed) {
            continue;
        }
        let values = labeled_matches_for_report(&seed.name, classify_name_modifications);
        if values.is_empty() {
            continue;
        }
        out.push_str(&format!("- seed: {}\n", inline_or_empty(&seed.name.seed)));
        for value in values {
            out.push_str(&format!(
                "  - [{}] {}\n",
                value.labels.join(", "),
                inline_or_empty(&value.text)
            ));
        }
    }

    out.push_str("\n## Metadata Matches\n\n");
    for seed in &report.seed_reports {
        let values = metadata_pairs_for_report(&seed.metadata);
        if values.is_empty() {
            continue;
        }
        for value in values {
            out.push_str("- seed:\n\n");
            push_fenced(&mut out, &value.seed);
            out.push_str(&format!("- match labels: {}\n\n", value.labels.join(", ")));
            push_fenced(&mut out, &value.text);
        }
    }

    out
}

fn push_modification_summary(out: &mut String, report: &SampleReport) {
    out.push_str("## Modification Summary\n\n");
    out.push_str("### Name\n\n");
    push_counts_with_ratios(
        out,
        modification_summary(
            report
                .seed_reports
                .iter()
                .filter(|seed| is_usable_seed_name(&seed.name.seed))
                .map(|seed| &seed.name),
            classify_name_modifications,
        ),
    );
    out.push_str("\n### Metadata\n\n");
    push_metadata_matrix_summary(out, report.seed_reports.iter().map(|seed| &seed.metadata));
    out.push('\n');
}

fn push_metadata_matrix_summary<'a>(
    out: &mut String,
    reports: impl Iterator<Item = &'a MetadataComparison>,
) {
    let mut total = 0usize;
    let mut content_total = 0usize;
    let mut non_content_total = 0usize;
    let mut region_totals = BTreeMap::<String, usize>::new();
    let mut matrix = BTreeMap::<(String, String), usize>::new();
    let mut residual_counts = BTreeMap::<String, usize>::new();
    for comparison in reports {
        let values = metadata_pairs_for_report(comparison);
        total += values.len();
        for labeled in values {
            let mut has_content_change = false;
            let mut has_non_content_change = false;
            let mut sample_regions = BTreeSet::<String>::new();
            for label in labeled.labels {
                if let Some((region, operation)) = label.split_once(':') {
                    if METADATA_REGIONS.contains(&region)
                        && METADATA_OPERATIONS.contains(&operation)
                    {
                        has_content_change |= METADATA_CONTENT_REGIONS.contains(&region);
                        has_non_content_change |= METADATA_NON_CONTENT_REGIONS.contains(&region);
                        sample_regions.insert(region.to_string());
                        *matrix
                            .entry((operation.to_string(), region.to_string()))
                            .or_default() += 1;
                    } else {
                        *residual_counts
                            .entry(metadata_residual_summary_label(&label).to_string())
                            .or_default() += 1;
                    }
                } else if matches!(label.as_str(), "unparseable_changed" | "metadata_unchanged") {
                    *residual_counts.entry(label).or_default() += 1;
                } else {
                    *residual_counts
                        .entry(metadata_residual_summary_label(&label).to_string())
                        .or_default() += 1;
                }
            }
            content_total += usize::from(has_content_change);
            non_content_total += usize::from(has_non_content_change);
            for region in sample_regions {
                *region_totals.entry(region).or_default() += 1;
            }
        }
    }

    out.push_str(&format!(
        "- total matches: {}\n",
        count_with_total_ratio(total, total)
    ));
    out.push_str(&format!(
        "- content-bearing changes: {}\n",
        count_with_total_ratio(content_total, total)
    ));
    out.push_str(&format!(
        "- non-content-bearing changes: {}\n",
        count_with_total_ratio(non_content_total, total)
    ));
    for label in ["exact_match", "metadata_unchanged", "unparseable_changed"] {
        if let Some(count) = residual_counts.get(label) {
            out.push_str(&format!(
                "- {label}: {}\n",
                count_with_total_ratio(*count, total)
            ));
        }
    }

    out.push_str("\n#### Metadata Change Matrix\n\n");
    out.push_str("| operation |");
    for region in METADATA_REGIONS {
        out.push_str(&format!(" {region} |"));
    }
    out.push('\n');
    out.push_str("| --- |");
    for _ in METADATA_REGIONS {
        out.push_str(" ---: |");
    }
    out.push('\n');
    out.push_str("| total |");
    for region in METADATA_REGIONS {
        let count = region_totals.get(region).copied().unwrap_or(0);
        out.push_str(&format!(" {} |", count_with_total_ratio(count, total)));
    }
    out.push('\n');
    for operation in METADATA_OPERATIONS {
        out.push_str(&format!("| {operation} |"));
        for region in METADATA_REGIONS {
            let count = matrix
                .get(&(operation.to_string(), region.to_string()))
                .copied()
                .unwrap_or(0);
            out.push_str(&format!(" {} |", count_with_total_ratio(count, total)));
        }
        out.push('\n');
    }
}

fn metadata_residual_summary_label(label: &str) -> &str {
    label
}

fn push_counts_with_ratios(out: &mut String, summary: (usize, BTreeMap<String, usize>)) {
    let (total, counts) = summary;
    out.push_str(&format!("- total matches: {total}\n"));
    if counts.is_empty() {
        out.push_str("- _none_\n");
        return;
    }
    for (label, count) in counts {
        let ratio = if total > 0 {
            count as f64 * 100.0 / total as f64
        } else {
            0.0
        };
        out.push_str(&format!("- {label}: {count} ({ratio:.1}%)\n"));
    }
}

fn count_with_total_ratio(count: usize, total: usize) -> String {
    let ratio = if total > 0 {
        count as f64 * 100.0 / total as f64
    } else {
        0.0
    };
    format!("{count} ({ratio:.1}%)")
}

fn labeled_matches_for_report(
    comparison: &TextComparison,
    classifier: fn(&str, &str) -> Vec<String>,
) -> Vec<LabeledTextMatch> {
    if !comparison.labeled_matches.is_empty() {
        return comparison.labeled_matches.clone();
    }
    comparison
        .matches
        .iter()
        .map(|value| LabeledTextMatch {
            text: value.clone(),
            labels: classifier(&comparison.seed, value),
        })
        .collect()
}

fn metadata_pairs_for_report(comparison: &MetadataComparison) -> Vec<MetadataPairMatch> {
    if !comparison.paired_matches.is_empty() {
        return comparison.paired_matches.clone();
    }
    if !comparison.labeled_matches.is_empty() {
        return comparison
            .labeled_matches
            .iter()
            .map(|value| MetadataPairMatch {
                seed: comparison.seed.clone(),
                text: value.text.clone(),
                labels: value.labels.clone(),
            })
            .collect();
    }
    comparison
        .matches
        .iter()
        .map(|value| MetadataPairMatch {
            seed: comparison.seed.clone(),
            text: value.clone(),
            labels: classify_metadata_modifications(&comparison.seed, value),
        })
        .collect()
}

fn inline_or_empty(value: &str) -> String {
    if value.trim().is_empty() {
        "_empty_".to_string()
    } else if is_unavailable_display_text(value) {
        "_unavailable_".to_string()
    } else {
        render_visible_text(value)
    }
}

fn push_fenced(out: &mut String, value: &str) {
    if value.trim().is_empty() {
        out.push_str("_empty_\n\n");
        return;
    }
    let value = render_visible_text(value);
    out.push_str(&format!("````text\n{value}\n````\n\n"));
}

fn render_visible_text(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        if should_render_as_codepoint(ch) {
            out.push_str(&format!("<U+{:04X}>", ch as u32));
        } else {
            out.push(ch);
        }
    }
    out
}

fn is_unavailable_display_text(value: &str) -> bool {
    let trimmed = value.trim();
    !trimmed.is_empty()
        && trimmed
            .chars()
            .all(|ch| ch == '?' || ch == '\u{fffd}' || ch.is_whitespace())
}

fn is_usable_seed_name(value: &str) -> bool {
    let trimmed = value.trim();
    if trimmed.is_empty() || is_unavailable_display_text(trimmed) {
        return false;
    }
    let normalized = normalize_nfkc(trimmed);
    if !normalized.chars().any(char::is_alphanumeric) {
        return false;
    }
    !matches!(
        normalized.to_lowercase().as_str(),
        "none" | "null" | "undefined" | "n/a" | "na" | "_empty_" | "_unavailable_"
    )
}

fn should_render_as_codepoint(ch: char) -> bool {
    matches!(
        ch as u32,
        0x0000..=0x0008
            | 0x000B..=0x000C
            | 0x000E..=0x001F
            | 0x007F
            | 0x00AD
            | 0x034F
            | 0x061C
            | 0x180E
            | 0x200B..=0x200F
            | 0x202A..=0x202E
            | 0x2060
            | 0xFEFF
            | 0xE000..=0xF8FF
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect_test_metadata_source_indices(
        seed_sketch: &MetadataSketch,
        text_index: &TextIndex,
        scratch: &mut SampleScratch,
        seed_contract: &str,
    ) -> Vec<usize> {
        collect_metadata_source_candidates(
            seed_sketch,
            text_index,
            scratch,
            seed_contract,
            METADATA_SKETCH_SOURCE_HAMMING_THRESHOLD,
        )
        .indices
    }

    #[test]
    fn name_normalization_matches_top_contract_analysis_style() {
        assert_eq!(normalize_name("Azuki #123"), "azuki");
        assert_eq!(normalize_name("Ａｚｕｋｉ #123"), "azuki");
    }

    #[test]
    fn metadata_json_fallback_extracts_relevant_fields() {
        let json = r#"{"description":"Gold Dragon","ignored":"noise","attributes":[{"trait_type":"Background","value":"Red"}]}"#;
        assert_eq!(
            metadata_document_from_json(json),
            "background red gold dragon"
        );
    }

    #[test]
    fn metadata_dedup_rejects_non_json_and_overlong_raw_metadata() {
        assert!(metadata_is_dedup_eligible(
            "gold dragon",
            r#"{"description":"Gold Dragon"}"#
        ));
        assert!(!metadata_is_dedup_eligible("gold dragon", ""));
        assert!(!metadata_is_dedup_eligible(
            "gold dragon",
            "not json metadata"
        ));

        let overlong_json = format!(
            r#"{{"description":"{}"}}"#,
            "x".repeat(MAX_METADATA_BYTES_FOR_DEDUP)
        );
        assert!(!metadata_is_dedup_eligible("gold dragon", &overlong_json));
        assert_eq!(record_metadata_doc("gold dragon", "not json metadata"), "");
        assert_eq!(
            record_metadata_prefilter_text("gold dragon", "not json metadata"),
            ""
        );
    }

    #[test]
    fn metadata_prefilter_text_keeps_insensitive_values_but_only_sensitive_keys() {
        let json = r#"{"name":"Seed #1","description":"Shared Story","attributes":[{"trait_type":"Background","value":"Red"}],"image":"ipfs://seed/1.png"}"#;

        let text = metadata_prefilter_text(json);

        assert!(text.contains("description"));
        assert!(text.contains("shared story"));
        assert!(text.contains("background"));
        assert!(text.contains("name"));
        assert!(text.contains("image"));
        let tokens = text.split_whitespace().collect::<Vec<_>>();
        assert!(!tokens.contains(&"seed"));
        assert!(!tokens.contains(&"red"));
        assert!(!tokens.contains(&"ipfs"));
    }

    #[test]
    fn metadata_bm25_uses_expected_constants() {
        assert_eq!(METADATA_BM25_K1, 1.2);
        assert_eq!(METADATA_BM25_B, 0.75);
    }

    #[test]
    fn overlapping_metadata_row_loader_keeps_one_row_per_candidate_contract() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            r#"
            CREATE TABLE nft_features (
                chain VARCHAR NOT NULL,
                contract_address VARCHAR NOT NULL,
                token_id VARCHAR NOT NULL,
                metadata_doc VARCHAR,
                metadata_json VARCHAR
            );
            INSERT INTO nft_features VALUES
                ('ethereum', '0xdup', '1', '', '{"description":"alpha beta"}'),
                ('ethereum', '0xdup', '2', '', '{"description":"gold dragon"}'),
                ('ethereum', '0xother', '1', '', '{"description":"alpha beta"}');
            "#,
        )
        .unwrap();
        let mut token_interner = TokenInterner::default();
        let rows = read_candidate_metadata_rows(
            &conn,
            "ethereum",
            &["0xdup".to_string()],
            &BTreeSet::from(["1".to_string(), "2".to_string()]),
            &mut token_interner,
        )
        .unwrap();

        let token_ids = rows["0xdup"]
            .iter()
            .map(|row| row.token_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(token_ids, vec!["1"]);
    }

    #[test]
    fn seed_row_loader_removes_ineligible_metadata_without_dropping_names() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            r#"
            CREATE TABLE nft_features (
                chain VARCHAR NOT NULL,
                contract_address VARCHAR NOT NULL,
                token_id VARCHAR NOT NULL,
                name VARCHAR,
                metadata_doc VARCHAR,
                metadata_json VARCHAR
            );
            "#,
        )
        .unwrap();
        let overlong_json = format!(
            "{{\"description\":\"{}\"}}",
            "x".repeat(MAX_METADATA_BYTES_FOR_DEDUP + 1)
        );
        let mut stmt = conn
            .prepare(
                "
                INSERT INTO nft_features
                    (chain, contract_address, token_id, name, metadata_doc, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?)
                ",
            )
            .unwrap();
        stmt.execute(params![
            "ethereum",
            "0xseed",
            "1",
            "Seed One",
            "non-json doc",
            "not json"
        ])
        .unwrap();
        stmt.execute(params![
            "ethereum",
            "0xseed",
            "2",
            "Seed Two",
            "overlong doc",
            overlong_json
        ])
        .unwrap();
        stmt.execute(params![
            "ethereum",
            "0xseed",
            "3",
            "Seed Three",
            "valid doc",
            r#"{"description":"valid"}"#
        ])
        .unwrap();

        let rows = read_seed_rows(&conn, "ethereum", "0xseed", 0).unwrap();

        assert_eq!(
            rows.iter().map(|row| row.name.as_str()).collect::<Vec<_>>(),
            vec!["Seed One", "Seed Two", "Seed Three"]
        );
        assert_eq!(rows[0].metadata_doc, "");
        assert_eq!(rows[0].metadata_json, "");
        assert_eq!(rows[1].metadata_doc, "");
        assert_eq!(rows[1].metadata_json, "");
        assert_eq!(rows[2].metadata_doc, "valid doc");
        assert_eq!(rows[2].metadata_json, r#"{"description":"valid"}"#);
    }

    #[test]
    fn seed_metadata_prefilter_does_not_scan_past_first_metadata() {
        let rows = vec![
            NftTextRow {
                token_id: "1".into(),
                name: "Seed One".into(),
                metadata_doc: String::new(),
                metadata_json: String::new(),
            },
            NftTextRow {
                token_id: "2".into(),
                name: "Seed Two".into(),
                metadata_doc: String::new(),
                metadata_json: r#"{"description":"valid"}"#.into(),
            },
        ];

        assert_eq!(first_seed_metadata(&rows), "");
        assert_eq!(seed_metadata_prefilter_text(&rows), None);
    }

    #[test]
    fn query_terms_preserve_duplicate_token_frequency() {
        assert_eq!(query_terms_from_tokens(&[7, 7, 9]), vec![(7, 2), (9, 1)]);
    }

    #[test]
    fn single_document_metadata_pair_score_matches_corpus_path() {
        let mut token_interner = TokenInterner::default();
        let query =
            MetadataDocument::from_text_with_interner("gold dragon", &mut token_interner).unwrap();
        let doc =
            MetadataDocument::from_text_with_interner("rare gold dragon", &mut token_interner)
                .unwrap();
        let corpus = MetadataCorpus::from_documents(std::iter::once(&doc));
        let corpus = MetadataCorpusView::from_corpus(&corpus);

        let single_doc_score = score_metadata_pair_with_single_document_corpus(&query, &doc);
        let corpus_score = score_metadata_pair(&query, &doc, &corpus);

        assert!((single_doc_score - corpus_score).abs() < 1e-9);
    }

    #[test]
    fn prepared_metadata_query_reuses_terms_without_changing_score() {
        let mut token_interner = TokenInterner::default();
        let query =
            MetadataDocument::from_text_with_interner("gold dragon gold", &mut token_interner)
                .unwrap();
        let doc =
            MetadataDocument::from_text_with_interner("rare gold dragon", &mut token_interner)
                .unwrap();
        let corpus = MetadataCorpus::from_documents(std::iter::once(&doc));
        let corpus = MetadataCorpusView::from_corpus(&corpus);

        let prepared_query = PreparedMetadataQuery::new(query.clone(), &corpus);
        let prepared_score = prepared_query.score(&doc);
        let direct_score = score_metadata_pair(&query, &doc, &corpus);

        assert!((prepared_score - direct_score).abs() < 1e-9);
    }

    #[test]
    fn metadata_prefilter_candidates_skip_zero_bm25_overlap() {
        let mut token_interner = TokenInterner::default();
        let query =
            MetadataDocument::from_text_with_interner("gold dragon", &mut token_interner).unwrap();
        let no_overlap =
            MetadataDocument::from_text_with_interner("silver forest", &mut token_interner)
                .unwrap();
        let overlap =
            MetadataDocument::from_text_with_interner("rare gold", &mut token_interner).unwrap();
        let metadata = vec![
            MetadataCandidate {
                contract_address: "0xmiss".into(),
                doc: no_overlap.clone(),
                sketch: MetadataSketch::default(),
            },
            MetadataCandidate {
                contract_address: "0xhit".into(),
                doc: overlap.clone(),
                sketch: MetadataSketch::default(),
            },
        ];
        let corpus =
            MetadataCorpus::from_documents(metadata.iter().map(|candidate| &candidate.doc));
        let corpus = MetadataCorpusView::from_corpus(&corpus);
        let seed_query = PreparedMetadataQuery::new(query, &corpus);

        let filtered = metadata_prefilter_score_candidate_indices(&seed_query, &metadata, &[0, 1]);

        assert_eq!(
            bm25_score_terms(&seed_query.terms, &no_overlap, &corpus),
            0.0
        );
        assert_eq!(filtered, vec![1]);
    }

    #[test]
    fn metadata_sketch_source_scan_ignores_shared_high_frequency_structure_tokens() {
        let mut token_interner = TokenInterner::default();
        let seed_doc = MetadataDocument::from_text_with_interner(
            "name image attributes description rarealpha",
            &mut token_interner,
        )
        .unwrap();
        let mut metadata = vec![MetadataCandidate {
            contract_address: "0xhit".into(),
            doc: MetadataDocument::from_text_with_interner(
                "name image attributes description rarealpha",
                &mut token_interner,
            )
            .unwrap(),
            sketch: MetadataSketch::default(),
        }];
        for index in 0..96 {
            metadata.push(MetadataCandidate {
                contract_address: format!("0xnoise{index}"),
                doc: MetadataDocument::from_text_with_interner(
                    &format!("name image attributes description noise{index}"),
                    &mut token_interner,
                )
                .unwrap(),
                sketch: MetadataSketch::default(),
            });
        }
        let metadata_corpus =
            MetadataCorpus::from_documents(metadata.iter().map(|candidate| &candidate.doc));
        populate_metadata_sketches(&mut metadata, &metadata_corpus);
        let metadata_indices_by_contract = build_metadata_indices_by_contract(&metadata);
        let metadata_source_index = build_metadata_source_index(&metadata);
        let text_index = TextIndex {
            metadata,
            metadata_token_ids: token_interner.into_ids(),
            metadata_corpus,
            metadata_indices_by_contract,
            metadata_source_index,
            ..TextIndex::default()
        };
        let corpus = MetadataCorpusView::from_corpus(&text_index.metadata_corpus);
        let seed_sketch = metadata_sketch_from_document(&seed_doc, corpus.total_docs, |token| {
            corpus.document_frequency(token)
        });
        let mut scratch = SampleScratch::new(text_index.metadata.len());

        let source_indices =
            collect_test_metadata_source_indices(&seed_sketch, &text_index, &mut scratch, "0xseed");
        let source_contracts = source_indices
            .iter()
            .map(|index| text_index.metadata[*index].contract_address.as_str())
            .collect::<Vec<_>>();

        assert!(source_contracts.contains(&"0xhit"));
        assert!(
            source_contracts.len() < 20,
            "high-frequency structure tokens should not route every representative metadata"
        );
    }

    #[test]
    fn metadata_sketch_source_scan_includes_fallback_distance_sources_initially() {
        let mut token_interner = TokenInterner::default();
        let doc =
            MetadataDocument::from_text_with_interner("description rarealpha", &mut token_interner)
                .unwrap();
        let text_index = TextIndex {
            metadata: vec![MetadataCandidate {
                contract_address: "0xfallback".into(),
                doc,
                sketch: MetadataSketch {
                    simhash: (1u64 << 20) - 1,
                    anchors: vec![2],
                },
            }],
            ..TextIndex::default()
        };
        let seed_sketch = MetadataSketch {
            simhash: 0,
            anchors: vec![1],
        };
        let mut scratch = SampleScratch::new(text_index.metadata.len());

        let source_indices =
            collect_test_metadata_source_indices(&seed_sketch, &text_index, &mut scratch, "0xseed");

        assert_eq!(source_indices, vec![0]);
    }

    #[test]
    fn metadata_source_index_routes_hamming_sources_without_anchor_match() {
        let mut token_interner = TokenInterner::default();
        let doc =
            MetadataDocument::from_text_with_interner("description rarealpha", &mut token_interner)
                .unwrap();
        let metadata = vec![MetadataCandidate {
            contract_address: "0xfallback".into(),
            doc,
            sketch: MetadataSketch {
                simhash: (1u64 << 20) - 1,
                anchors: vec![2],
            },
        }];
        let metadata_source_index = build_metadata_source_index(&metadata);
        let text_index = TextIndex {
            metadata,
            metadata_source_index,
            ..TextIndex::default()
        };
        let seed_sketch = MetadataSketch {
            simhash: 0,
            anchors: vec![1],
        };
        let mut scratch = SampleScratch::new(text_index.metadata.len());

        let source_indices =
            collect_test_metadata_source_indices(&seed_sketch, &text_index, &mut scratch, "0xseed");

        assert_eq!(source_indices, vec![0]);
    }

    #[test]
    fn metadata_source_candidates_are_deduped_during_bucket_scan() {
        let mut token_interner = TokenInterner::default();
        let doc =
            MetadataDocument::from_text_with_interner("description rarealpha", &mut token_interner)
                .unwrap();
        let mut metadata = vec![MetadataCandidate {
            contract_address: "0xhit".into(),
            doc,
            sketch: MetadataSketch {
                simhash: 0,
                anchors: vec![7],
            },
        }];
        for index in 0..64 {
            metadata.push(MetadataCandidate {
                contract_address: format!("0xmiss{index}"),
                doc: MetadataDocument {
                    tokens: vec![index as TokenId + 100],
                    unique_tokens: vec![index as TokenId + 100],
                    term_freqs: vec![(index as TokenId + 100, 1)],
                },
                sketch: MetadataSketch {
                    simhash: u64::MAX,
                    anchors: vec![8],
                },
            });
        }
        let metadata_source_index = build_metadata_source_index(&metadata);
        let text_index = TextIndex {
            metadata,
            metadata_source_index,
            ..TextIndex::default()
        };
        let seed_sketch = MetadataSketch {
            simhash: 0,
            anchors: vec![7],
        };
        let mut scratch = SampleScratch::new(text_index.metadata.len());

        let source_candidates = collect_metadata_source_candidates(
            &seed_sketch,
            &text_index,
            &mut scratch,
            "0xseed",
            METADATA_SKETCH_SOURCE_HAMMING_THRESHOLD,
        );

        assert!(source_candidates.bucket_hits > source_candidates.verified);
        assert_eq!(source_candidates.verified, 1);
        assert_eq!(source_candidates.indices, vec![0]);
        assert!(!source_candidates.full_scan);
    }

    #[test]
    fn metadata_source_candidates_use_full_scan_for_broad_bucket_queries() {
        let mut metadata = Vec::new();
        for index in 0..64 {
            metadata.push(MetadataCandidate {
                contract_address: format!("0xhit{index}"),
                doc: MetadataDocument {
                    tokens: vec![index as TokenId],
                    unique_tokens: vec![index as TokenId],
                    term_freqs: vec![(index as TokenId, 1)],
                },
                sketch: MetadataSketch {
                    simhash: 0,
                    anchors: vec![7],
                },
            });
        }
        let metadata_source_index = build_metadata_source_index(&metadata);
        let text_index = TextIndex {
            metadata,
            metadata_source_index,
            ..TextIndex::default()
        };
        let seed_sketch = MetadataSketch {
            simhash: 0,
            anchors: vec![7],
        };
        let mut scratch = SampleScratch::new(text_index.metadata.len());

        let source_candidates = collect_metadata_source_candidates(
            &seed_sketch,
            &text_index,
            &mut scratch,
            "0xseed",
            METADATA_SKETCH_SOURCE_HAMMING_THRESHOLD,
        );

        assert_eq!(source_candidates.bucket_hits, text_index.metadata.len());
        assert_eq!(source_candidates.verified, text_index.metadata.len());
        assert_eq!(source_candidates.indices.len(), text_index.metadata.len());
        assert!(source_candidates.full_scan);
    }

    #[test]
    fn contract_metadata_corpus_exclusion_matches_dynamic_view() {
        let mut token_interner = TokenInterner::default();
        let metadata = vec![
            MetadataCandidate {
                contract_address: "0xseed".into(),
                doc: MetadataDocument::from_text_with_interner(
                    "description gold dragon",
                    &mut token_interner,
                )
                .unwrap(),
                sketch: MetadataSketch::default(),
            },
            MetadataCandidate {
                contract_address: "0xseed".into(),
                doc: MetadataDocument::from_text_with_interner(
                    "description silver dragon",
                    &mut token_interner,
                )
                .unwrap(),
                sketch: MetadataSketch::default(),
            },
            MetadataCandidate {
                contract_address: "0xother".into(),
                doc: MetadataDocument::from_text_with_interner(
                    "description green forest",
                    &mut token_interner,
                )
                .unwrap(),
                sketch: MetadataSketch::default(),
            },
        ];
        let corpus =
            MetadataCorpus::from_documents(metadata.iter().map(|candidate| &candidate.doc));
        let indices_by_contract = build_metadata_indices_by_contract(&metadata);
        let dynamic =
            MetadataCorpusView::new(&corpus, &metadata, indices_by_contract["0xseed"].as_slice());
        let exclusions = build_contract_metadata_corpus_exclusions(&metadata);
        let cached = MetadataCorpusView::from_exclusion(&corpus, exclusions.get("0xseed"));

        assert_eq!(cached.total_docs, dynamic.total_docs);
        assert!((cached.avg_doc_len - dynamic.avg_doc_len).abs() < 1e-9);
        for token in token_interner.into_ids().values() {
            assert_eq!(
                cached.document_frequency(*token),
                dynamic.document_frequency(*token)
            );
        }
    }

    #[test]
    fn report_text_makes_invisible_characters_visible() {
        assert_eq!(render_visible_text("Dood\u{034f}les"), "Dood<U+034F>les");
        assert_eq!(render_visible_text("Dood\u{e002}0les"), "Dood<U+E002>0les");
    }

    #[test]
    fn report_summarizes_overlapping_modification_labels() {
        let report = SampleReport {
            chain: "ethereum".into(),
            seed_reports: vec![SeedSampleReport {
                name: TextComparison {
                    seed: "Azuki #1".into(),
                    matches: vec!["Ａｚｕｋｉ #456".into(), "Unrelated".into()],
                    labeled_matches: Vec::new(),
                },
                metadata: MetadataComparison {
                    seed: r#"{"description":"background gold","image":"ipfs://seed/image.png"}"#
                        .into(),
                    matches: vec![
                        r#"{"description":"background gold","image":"ipfs://copy/image.png"}"#
                            .into(),
                    ],
                    labeled_matches: Vec::new(),
                    paired_matches: Vec::new(),
                },
            }],
        };

        let output = render_markdown_report(&report);

        assert!(output.contains("## Modification Summary"));
        assert!(output.contains("- total matches: 2"));
        assert!(output.contains("- suffix_augmentation: 1 (50.0%)"));
        assert!(output.contains("- other: 1 (50.0%)"));
        assert!(output.contains("- total matches: 1"));
        assert!(output.contains("#### Metadata Change Matrix"));
        assert!(output.contains(
            "| replaced | 0 (0.0%) | 0 (0.0%) | 0 (0.0%) | 1 (100.0%) | 0 (0.0%) | 0 (0.0%) | 0 (0.0%) |"
        ));
        assert!(!output.contains("- asset_pointer_reuse:"));
        assert!(!output.contains("- trait_schema_reuse:"));
        assert!(!output.contains("1/2"));
        assert!(!output.contains("1/1"));
    }

    #[test]
    fn name_labels_remove_ai_and_homoglyph_and_count_versions_as_token_suffix() {
        assert_eq!(
            classify_name_modifications("Azuki", "AIZUKI"),
            vec!["lexical_mutation"]
        );
        assert_eq!(
            classify_name_modifications("Azuki", "Azuki 404"),
            vec!["suffix_augmentation"]
        );
        assert_eq!(
            classify_name_modifications("Azuki", "Azuki v2"),
            vec!["suffix_augmentation"]
        );
        assert_eq!(
            classify_name_modifications("Azuki", "Azuki Official"),
            vec!["suffix_augmentation"]
        );
    }

    #[test]
    fn name_uses_paper_level_labels() {
        assert_eq!(
            classify_name_modifications("BoredApeYachtClub", "Bored Ape Yacht Club"),
            vec!["format_perturbation"]
        );
        assert_eq!(
            classify_name_modifications("Azuki", "Azuk\u{034F}i"),
            vec!["format_perturbation"]
        );
        assert_eq!(
            classify_name_modifications("BoredApeYachtClub", "Bored Ape Yacht Clubie"),
            vec!["format_perturbation", "lexical_mutation"]
        );
        assert_eq!(
            classify_name_modifications("Azuki", "Azuki2"),
            vec!["suffix_augmentation"]
        );
        assert_eq!(
            classify_name_modifications("TEST NFT", "TEST-NFT"),
            vec!["format_perturbation"]
        );
        assert_eq!(
            classify_name_modifications("Opepen Edition", "O$pepen Edition"),
            vec!["lexical_mutation"]
        );
        assert_eq!(
            classify_name_modifications("Azuki", "Ａｚｕｋｉ"),
            vec!["format_perturbation"]
        );
        assert_eq!(
            classify_name_modifications("Azuki", "Ａｚｕｋｉ v2"),
            vec!["suffix_augmentation", "format_perturbation"]
        );
        assert_eq!(
            classify_name_modifications("Azuki", "Azuki v2"),
            vec!["suffix_augmentation"]
        );
        assert_eq!(
            classify_name_modifications("Azuki", "Azuki Official"),
            vec!["suffix_augmentation"]
        );
    }

    #[test]
    fn name_3d_suffix_preserves_seed_terminal_words() {
        assert!(has_derivative_suffix("Mutant Ape Yacht Club 3D"));
        assert_eq!(
            strip_raw_augmentation_suffix_matching_seed(
                "MutantApeYachtClub",
                "Mutant Ape Yacht Club 3D"
            ),
            "Mutant Ape Yacht Club"
        );
        assert_eq!(
            classify_name_modifications("MutantApeYachtClub", "Mutant Ape Yacht Club 3D"),
            vec!["suffix_augmentation", "format_perturbation"]
        );
        assert_eq!(
            classify_name_modifications("Camels", "Camels3D"),
            vec!["suffix_augmentation"]
        );
    }

    #[test]
    fn name_derivative_tail_tokens_are_suffixes_only_when_seed_core_is_preserved() {
        assert_eq!(
            classify_name_modifications("Mutant Hounds", "Mutant Hounds AI"),
            vec!["suffix_augmentation"]
        );
        assert_eq!(
            classify_name_modifications("Checks", "ChecksAI"),
            vec!["suffix_augmentation"]
        );
        assert_eq!(
            classify_name_modifications("Elemental", "ElementalArt"),
            vec!["suffix_augmentation"]
        );
        assert_eq!(
            classify_name_modifications("Nakamigos", "NakamigosGif"),
            vec!["suffix_augmentation"]
        );
        assert_eq!(
            classify_name_modifications("parallel", "ParallelID"),
            vec!["suffix_augmentation", "format_perturbation"]
        );
        assert_eq!(
            classify_name_modifications("Cool Cats", "Cool Cats FC"),
            vec!["suffix_augmentation"]
        );
        assert_eq!(
            classify_name_modifications("Saint of LA | ETERNAL", "Saint of LA | ETERNAL (TEST)"),
            vec!["suffix_augmentation"]
        );
        assert_eq!(
            classify_name_modifications("Chain Runners", "Chain Runners XR"),
            vec!["suffix_augmentation"]
        );
        assert_eq!(
            classify_name_modifications("Planets", "planetsid"),
            vec!["suffix_augmentation", "format_perturbation"]
        );
        assert_eq!(
            classify_name_modifications("Average Creatures", "Average Creatures II"),
            vec!["suffix_augmentation"]
        );
        assert_eq!(
            classify_name_modifications("Metamorphosis", "Metamorphosis I"),
            vec!["suffix_augmentation"]
        );
        assert_eq!(
            classify_name_modifications("Moonrunners", "Moonrunners 2D"),
            vec!["suffix_augmentation"]
        );
        assert_eq!(
            classify_name_modifications("Wanderers", "Wanderers2nd"),
            vec!["suffix_augmentation"]
        );
        assert_eq!(
            classify_name_modifications("SupDucks", "SupDucksVX"),
            vec!["suffix_augmentation"]
        );
        assert_eq!(
            classify_name_modifications("CyberKongz", "CyberKongz VX"),
            vec!["suffix_augmentation"]
        );
        assert_eq!(
            classify_name_modifications("Kitaro World", "Kitaro World V2"),
            vec!["suffix_augmentation"]
        );
        assert_eq!(
            classify_name_modifications("KILLABEARS", "KILLABEARS-V43"),
            vec!["suffix_augmentation"]
        );
        assert_eq!(
            classify_name_modifications("SHRIMPERS", "Shrimpers V2"),
            vec!["suffix_augmentation", "format_perturbation"]
        );
        assert_eq!(
            classify_name_modifications("VeeFriends", "VeeFriendsV2"),
            vec!["suffix_augmentation"]
        );
        assert_eq!(
            classify_name_modifications("Critters Quest Blind Box", "Critters Quest Blind Box x12"),
            vec!["suffix_augmentation"]
        );
        assert_eq!(
            classify_name_modifications("MidnightBreeze", "MidnightBreezeV2"),
            vec!["suffix_augmentation"]
        );
        assert_eq!(
            strip_raw_trailing_number_suffix("Bored Ape Yacht Club 2.0"),
            "Bored Ape Yacht Club"
        );
        assert_eq!(
            classify_name_modifications("BoredApeYachtClub", "Bored Ape Yacht Club 2.0"),
            vec!["suffix_augmentation", "format_perturbation"]
        );
        assert_eq!(
            classify_name_modifications("CloneX", "CLONEX404"),
            vec!["suffix_augmentation", "format_perturbation"]
        );

        assert_eq!(
            classify_name_modifications("BoredApeYachtClub", "BoredAIYachtClub"),
            vec!["lexical_mutation"]
        );
        assert_eq!(
            classify_name_modifications("Nakamigos", "NakamAIgos"),
            vec!["lexical_mutation"]
        );
        assert_eq!(
            classify_name_modifications("Elemental", "ElementalsAI"),
            vec!["lexical_mutation"]
        );
    }

    #[test]
    fn name_format_perturbation_does_not_hide_seed_version_words() {
        assert_eq!(
            classify_name_modifications("CryptoDickbutts S3", "CryptoDickbuttss"),
            vec!["lexical_mutation"]
        );
        assert_eq!(
            classify_name_modifications("CryptoDickbutts S3", "CryptoBickdutts"),
            vec!["lexical_mutation"]
        );
        assert_eq!(
            classify_name_modifications("Invisible Friends 3D", "Invisible Friends"),
            vec!["lexical_mutation"]
        );
        assert_eq!(
            classify_name_modifications("CyberKongz", "Cyberkongz 404"),
            vec!["suffix_augmentation", "format_perturbation"]
        );
    }

    #[test]
    fn name_format_perturbation_overlaps_with_lexical_mutation() {
        assert_eq!(
            classify_name_modifications("LilPudgys", "Lil Pudygs"),
            vec!["format_perturbation", "lexical_mutation"]
        );
        assert_eq!(
            classify_name_modifications("PudgyPenguins", "Phudgy Penguins"),
            vec!["format_perturbation", "lexical_mutation"]
        );
        assert_eq!(
            classify_name_modifications("Elemental", "Element 280"),
            vec!["suffix_augmentation", "lexical_mutation"]
        );
    }

    #[test]
    fn name_other_examples_are_broad_lexical_mutations() {
        assert_eq!(
            classify_name_modifications("PudgyPenguins", "Phudgy Penguins"),
            vec!["format_perturbation", "lexical_mutation"]
        );
        assert_eq!(
            classify_name_modifications("World Of Women", "WORLD OF MEN"),
            vec!["lexical_mutation"]
        );
        assert_eq!(
            classify_name_modifications("Art Blocks", "Art Block"),
            vec!["lexical_mutation"]
        );
    }

    #[test]
    fn name_numeric_suffix_detects_lexical_core_variants() {
        let labels = classify_name_modifications("PudgyPenguins", "Pudgy Penguin #1839");

        assert!(labels.contains(&"suffix_augmentation".to_string()));
    }

    #[test]
    fn name_summary_ignores_unusable_seed_names() {
        let report = SampleReport {
            chain: "ethereum".into(),
            seed_reports: vec![
                SeedSampleReport {
                    name: TextComparison {
                        seed: String::new(),
                        matches: vec!["Empty Clone".into()],
                        labeled_matches: Vec::new(),
                    },
                    metadata: MetadataComparison::default(),
                },
                SeedSampleReport {
                    name: TextComparison {
                        seed: "None".into(),
                        matches: vec!["None".into()],
                        labeled_matches: Vec::new(),
                    },
                    metadata: MetadataComparison::default(),
                },
                SeedSampleReport {
                    name: TextComparison {
                        seed: "Real Seed".into(),
                        matches: vec!["Real Seed".into()],
                        labeled_matches: Vec::new(),
                    },
                    metadata: MetadataComparison::default(),
                },
            ],
        };

        let output = render_markdown_report(&report);
        let name_summary = output
            .split("### Metadata")
            .next()
            .expect("name summary section");

        assert!(name_summary.contains("- total matches: 1"));
        assert!(name_summary.contains("- exact_clone: 1 (100.0%)"));
    }

    #[test]
    fn name_matches_do_not_emit_unusable_seed_sections() {
        let report = SampleReport {
            chain: "ethereum".into(),
            seed_reports: vec![
                SeedSampleReport {
                    name: TextComparison {
                        seed: "????: ??????".into(),
                        matches: vec!["????: ?????? clone".into()],
                        labeled_matches: Vec::new(),
                    },
                    metadata: MetadataComparison::default(),
                },
                SeedSampleReport {
                    name: TextComparison {
                        seed: "Real Seed".into(),
                        matches: vec!["Real Seed".into()],
                        labeled_matches: Vec::new(),
                    },
                    metadata: MetadataComparison::default(),
                },
            ],
        };

        let output = render_markdown_report(&report);
        let name_matches = output
            .split("## Name Matches")
            .nth(1)
            .and_then(|section| section.split("## Metadata Matches").next())
            .expect("name matches section");

        assert!(!name_matches.contains("????"));
        assert!(name_matches.contains("- seed: Real Seed"));
        assert!(name_matches.contains("[exact_clone] Real Seed"));
    }

    #[test]
    fn report_omits_seed_sections_without_matches() {
        let report = SampleReport {
            chain: "ethereum".into(),
            seed_reports: vec![
                SeedSampleReport {
                    name: TextComparison {
                        seed: "No Hit Seed".into(),
                        matches: Vec::new(),
                        labeled_matches: Vec::new(),
                    },
                    metadata: MetadataComparison {
                        seed: "no hit metadata".into(),
                        matches: Vec::new(),
                        labeled_matches: Vec::new(),
                        paired_matches: Vec::new(),
                    },
                },
                SeedSampleReport {
                    name: TextComparison {
                        seed: "Name Hit Seed".into(),
                        matches: vec!["Name Hit Seed".into()],
                        labeled_matches: Vec::new(),
                    },
                    metadata: MetadataComparison::default(),
                },
                SeedSampleReport {
                    name: TextComparison::default(),
                    metadata: MetadataComparison {
                        seed: "metadata hit seed".into(),
                        matches: vec!["metadata hit seed changed".into()],
                        labeled_matches: Vec::new(),
                        paired_matches: Vec::new(),
                    },
                },
            ],
        };

        let output = render_markdown_report(&report);

        assert!(!output.contains("No Hit Seed"));
        assert!(!output.contains("no hit metadata"));
        assert!(output.contains("- seed: Name Hit Seed"));
        assert!(output.contains("metadata hit seed"));
        assert!(output.contains("metadata hit seed changed"));
    }

    #[test]
    fn metadata_json_diff_labels_are_path_based_region_operations() {
        let seed = r#"{
            "name":"Seed #1",
            "description":"Original story",
            "image":"ipfs://seed-image",
            "external_url":"https://seed.example",
            "attributes":[
                {"trait_type":"Background","value":"Blue"},
                {"trait_type":"Eyes","value":"Open"}
            ],
            "seller_fee_basis_points":500
        }"#;
        let value = r#"{
            "name":"Seed #404",
            "description":"Copied story",
            "image":"ipfs://copy-image",
            "external_url":"https://copy.example",
            "attributes":[
                {"trait_type":"Background","value":"Red"},
                {"trait_type":"Eyes","value":"Open"},
                {"trait_type":"Hat","value":"Cap"}
            ],
            "seller_fee_basis_points":750
        }"#;

        let labels = classify_metadata_modifications(seed, value);

        assert!(labels.contains(&"title:replaced".to_string()));
        assert!(labels.contains(&"description:replaced".to_string()));
        assert!(labels.contains(&"references:replaced".to_string()));
        assert!(labels.contains(&"attributes:added".to_string()));
        assert!(labels.contains(&"attributes:replaced".to_string()));
        assert!(labels.contains(&"platform_fields:replaced".to_string()));
        assert!(!labels.iter().any(|label| label.starts_with("other")));
    }

    #[test]
    fn metadata_json_diff_detects_narrow_structure_and_reordered_changes() {
        let seed = r#"{"attributes":[{"trait_type":"A","value":"1"},{"trait_type":"B","value":"2"}],"metadata":{"image":"ipfs://seed"}}"#;
        let value = r#"{"attributes":[{"trait_type":"B","value":"2"},{"trait_type":"A","value":"1"}],"metadata":[{"image":"ipfs://seed"}]}"#;

        let labels = classify_metadata_modifications(seed, value);

        assert!(labels.contains(&"attributes:reordered".to_string()));
        assert!(labels.contains(&"structure:replaced".to_string()));
    }

    #[test]
    fn metadata_wrapper_added_or_removed_counts_as_structure() {
        let added = classify_metadata_modifications(
            r#"{"name":"Seed"}"#,
            r#"{"metadata":{"name":"Seed"}}"#,
        );
        let removed = classify_metadata_modifications(
            r#"{"rawmetadata":{"name":"Seed"}}"#,
            r#"{"name":"Seed"}"#,
        );

        assert_eq!(added, vec!["structure:added"]);
        assert_eq!(removed, vec!["structure:removed"]);
    }

    #[test]
    fn metadata_wrapper_transform_with_sibling_fields_does_not_emit_false_removals() {
        let added = classify_metadata_modifications(
            r#"{"name":"Seed"}"#,
            r#"{"metadata":{"name":"Seed"},"compiler":"copybot"}"#,
        );
        let removed = classify_metadata_modifications(
            r#"{"rawmetadata":{"name":"Seed"},"compiler":"copybot"}"#,
            r#"{"name":"Seed"}"#,
        );

        assert_eq!(
            added,
            vec![
                "platform_fields:added".to_string(),
                "structure:added".to_string()
            ]
        );
        assert_eq!(
            removed,
            vec![
                "platform_fields:removed".to_string(),
                "structure:removed".to_string()
            ]
        );
    }

    #[test]
    fn metadata_structure_changed_does_not_swallow_semantic_field_type_changes() {
        let labels = classify_metadata_modifications(
            r#"{"image":"ipfs://seed","attributes":[{"trait_type":"A","value":"1"}]}"#,
            r#"{"image":["ipfs://copy"],"attributes":{"A":"1"}}"#,
        );

        assert!(labels.contains(&"references:replaced".to_string()));
        assert!(labels.contains(&"attributes:replaced".to_string()));
        assert!(!labels.contains(&"structure:replaced".to_string()));
    }

    #[test]
    fn metadata_region_rules_are_parent_anchored_and_non_overlapping() {
        let labels = classify_metadata_modifications(
            r#"{"collection":{"name":"Seed Collection"},"attributes":[{"trait_type":"Image","image":"ipfs://seed"}]}"#,
            r#"{"collection":{"name":"Copy Collection"},"attributes":[{"trait_type":"Image","image":"ipfs://copy"}]}"#,
        );

        assert_eq!(
            labels,
            vec![
                "attributes:replaced".to_string(),
                "platform_fields:replaced".to_string()
            ]
        );
    }

    #[test]
    fn metadata_attribute_array_insertions_are_matched_by_trait_type() {
        let labels = classify_metadata_modifications(
            r#"{"attributes":[{"trait_type":"Background","value":"Blue"},{"trait_type":"Eyes","value":"Open"}]}"#,
            r#"{"attributes":[{"trait_type":"Hat","value":"Cap"},{"trait_type":"Background","value":"Blue"},{"trait_type":"Eyes","value":"Open"}]}"#,
        );

        assert_eq!(labels, vec!["attributes:added"]);
    }

    #[test]
    fn metadata_attribute_array_reorder_is_counted_with_content_changes() {
        let labels = classify_metadata_modifications(
            r#"{"attributes":[{"trait_type":"Background","value":"Blue"},{"trait_type":"Eyes","value":"Open"},{"trait_type":"Mouth","value":"Smile"}]}"#,
            r#"{"attributes":[{"trait_type":"Eyes","value":"Closed"},{"trait_type":"Background","value":"Blue"},{"trait_type":"Mouth","value":"Smile"}]}"#,
        );

        assert!(labels.contains(&"attributes:reordered".to_string()));
        assert!(labels.contains(&"attributes:replaced".to_string()));
    }

    #[test]
    fn metadata_unknown_json_paths_are_auxiliary_fields_not_other() {
        let labels =
            classify_metadata_modifications(r#"{"artist":"alice"}"#, r#"{"artist":"bob"}"#);

        assert_eq!(labels, vec!["auxiliary_fields:replaced"]);
    }

    #[test]
    fn metadata_reference_region_is_path_based_not_value_based() {
        let labels = classify_metadata_modifications(
            r#"{"artist":"https://seed.example"}"#,
            r#"{"artist":"https://copy.example"}"#,
        );

        assert_eq!(labels, vec!["auxiliary_fields:replaced"]);
    }

    #[test]
    fn metadata_exact_match_is_reported_separately_from_unchanged() {
        let labels = classify_metadata_modifications(
            r#"{"name":"Seed","image":"ipfs://seed"}"#,
            r#"{"name":"Seed","image":"ipfs://seed"}"#,
        );

        assert_eq!(labels, vec!["exact_match"]);
    }

    #[test]
    fn metadata_empty_optional_field_additions_do_not_inflate_changes() {
        let labels = classify_metadata_modifications(
            r#"{"name":"Seed","image":"ipfs://seed"}"#,
            r#"{"name":"Seed","image":"ipfs://seed","external_url":null,"description":"","attributes":[]}"#,
        );

        assert_eq!(labels, vec!["metadata_unchanged"]);
    }

    #[test]
    fn metadata_unparseable_is_not_other() {
        let labels =
            classify_metadata_modifications(r#"{"name":"Seed"}"#, "not valid json metadata");

        assert_eq!(labels, vec!["unparseable_changed"]);
    }

    #[test]
    fn metadata_non_json_inputs_do_not_use_text_similarity_labels() {
        assert_eq!(
            classify_metadata_modifications(
                "background gold ipfs://seed/image.png",
                "background red ipfs://copy/image.png"
            ),
            vec!["unparseable_changed"]
        );
        assert_eq!(
            classify_metadata_modifications("background gold", "background gold"),
            vec!["exact_match"]
        );
    }

    #[test]
    fn metadata_summary_uses_operation_region_matrix_and_group_totals() {
        let report = SampleReport {
            chain: "ethereum".into(),
            seed_reports: vec![SeedSampleReport {
                name: TextComparison::default(),
                metadata: MetadataComparison {
                    seed:
                        r#"{"name":"Seed","title":"Old","image":"ipfs://seed","seller_fee_basis_points":500}"#
                            .into(),
                    matches: vec![
                        r#"{"name":"Copy","image":"ipfs://copy","seller_fee_basis_points":750}"#
                            .into(),
                        r#"{"name":"Seed","title":"Old","image":"ipfs://seed","seller_fee_basis_points":500}"#
                            .into(),
                    ],
                    labeled_matches: Vec::new(),
                    paired_matches: Vec::new(),
                },
            }],
        };

        let output = render_markdown_report(&report);

        assert!(output.contains("#### Metadata Change Matrix"));
        assert!(output.contains("| operation | title | description | attributes | references | auxiliary_fields | platform_fields | structure |"));
        assert!(output.contains(
            "| total | 1 (50.0%) | 0 (0.0%) | 0 (0.0%) | 1 (50.0%) | 0 (0.0%) | 1 (50.0%) | 0 (0.0%) |"
        ));
        assert!(output.contains(
            "| removed | 1 (50.0%) | 0 (0.0%) | 0 (0.0%) | 0 (0.0%) | 0 (0.0%) | 0 (0.0%) | 0 (0.0%) |"
        ));
        assert!(output.contains(
            "| replaced | 1 (50.0%) | 0 (0.0%) | 0 (0.0%) | 1 (50.0%) | 0 (0.0%) | 1 (50.0%) | 0 (0.0%) |"
        ));
        assert!(output.contains("- content-bearing changes: 1 (50.0%)"));
        assert!(output.contains("- non-content-bearing changes: 1 (50.0%)"));
        assert!(output.contains("- exact_match: 1 (50.0%)"));
    }

    #[test]
    fn report_hides_all_question_mark_display_text_without_seed_contract_identifier() {
        let report = SampleReport {
            chain: "ethereum".into(),
            seed_reports: vec![SeedSampleReport {
                name: TextComparison {
                    seed: "????".into(),
                    matches: vec!["????".into()],
                    labeled_matches: Vec::new(),
                },
                metadata: MetadataComparison {
                    seed: String::new(),
                    matches: Vec::new(),
                    labeled_matches: Vec::new(),
                    paired_matches: Vec::new(),
                },
            }],
        };

        let output = render_markdown_report(&report);

        assert!(!output.contains("- seed: _unavailable_"));
        assert!(!output.contains("[exact_clone] _unavailable_"));
        assert!(!output.contains("contract:"));
    }
}
