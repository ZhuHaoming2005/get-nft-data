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
        Regex::new(r"\s+No\.?\s*\d+\s*$").unwrap(),
        Regex::new(r"\s+nr\.?\s*\d+\s*$").unwrap(),
        Regex::new(r"\s+\d{1,12}\s*$").unwrap(),
        Regex::new(r"\s*(?:404|420|777|888|999)\s*$").unwrap(),
        Regex::new(r"\s*(?:v|version)\s*\d+\s*$").unwrap(),
        Regex::new(r"\s*\d+\.0\s*$").unwrap(),
        Regex::new(r"\s*(?:gen|generation)\s*\d+\s*$").unwrap(),
    ]
});
static ATTACHED_TRAILING_NUMBER_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)([\p{L}])\d{1,6}\s*$").unwrap());
static WHITESPACE_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\s+").unwrap());
static TOKEN_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"[\p{L}\p{N}_]+").unwrap());
static DERIVATIVE_SUFFIX_PATTERNS: Lazy<Vec<Regex>> = Lazy::new(|| {
    vec![
        Regex::new(r"(?i)(?:\.fun|x)\s*$").unwrap(),
        Regex::new(
            r"(?i)(?:\s|[-_.])(?:official|nft|club|dao|pass|mint|claim|free|vip|collection|edition|clone|copy|reloaded|remastered|alpha|beta)\s*$",
        )
        .unwrap(),
    ]
});

const METADATA_BM25_K1: f64 = 1.2;
const METADATA_BM25_B: f64 = 0.75;

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
    pub metadata: TextComparison,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TextComparison {
    pub seed: String,
    pub matches: Vec<String>,
    pub labeled_matches: Vec<LabeledTextMatch>,
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
    LoadMetadataCandidates,
    ScoreMetadataCandidates,
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
    text: String,
    doc: MetadataDocument,
}

#[derive(Clone, Debug, Default)]
struct TextIndex {
    names: Vec<NameCandidate>,
    name_indices_by_normalized: Vec<NormalizedNameEntry>,
    metadata: Vec<MetadataCandidate>,
    metadata_token_ids: HashMap<String, TokenId>,
    metadata_corpus: MetadataCorpus,
    metadata_indices_by_token: HashMap<TokenId, Vec<usize>>,
    metadata_indices_by_contract: HashMap<String, Vec<usize>>,
}

#[derive(Clone, Debug)]
struct NormalizedNameEntry {
    normalized: String,
    candidate_indices: Vec<usize>,
}

#[derive(Default)]
struct NameCandidateBuilder {
    display_name: Option<String>,
    normalized_names: BTreeSet<String>,
}

#[derive(Default)]
struct SampleScratch {
    metadata_seen_epochs: Vec<u32>,
    metadata_epoch: u32,
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

    emit_progress(
        progress,
        position.index,
        position.total,
        SampleProgressStage::LoadMetadataCandidates,
        4,
        Some(text_index.metadata.len()),
    );
    let seed_metadata = first_seed_metadata(&seed_rows);
    let metadata_matches = match_metadata(
        &seed_metadata,
        text_index,
        scratch,
        seed_contract,
        config.metadata_threshold,
        config.max_recall_rows,
    );
    emit_progress(
        progress,
        position.index,
        position.total,
        SampleProgressStage::ScoreMetadataCandidates,
        5,
        Some(metadata_matches.len()),
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
        6,
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
        stage_count: 6,
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
        SELECT coalesce(CAST(name AS VARCHAR), ''),
               coalesce(CAST(metadata_doc AS VARCHAR), ''),
               coalesce(CAST(metadata_json AS VARCHAR), '')
        FROM nft_features
        WHERE chain = ? AND lower(contract_address) = ?
        ORDER BY token_id
        {limit_sql}
        "
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(params![chain, contract_address.to_lowercase()], |row| {
        Ok(NftTextRow {
            name: row.get::<_, String>(0)?,
            metadata_doc: row.get::<_, String>(1)?,
            metadata_json: row.get::<_, String>(2)?,
        })
    })?;
    rows.collect()
}

fn load_text_index(conn: &Connection, chain: &str) -> Result<TextIndex, duckdb::Error> {
    let names = load_name_index(conn, chain)?;
    let name_indices_by_normalized = build_name_indices_by_normalized(&names);
    let mut metadata_token_interner = TokenInterner::default();
    let metadata = load_metadata_index(conn, chain, &mut metadata_token_interner)?;
    let metadata_token_ids = metadata_token_interner.into_ids();
    let metadata_corpus = MetadataCorpus::from_documents(metadata.iter().map(|item| &item.doc));
    let metadata_indices_by_token = build_metadata_indices_by_token(&metadata);
    let metadata_indices_by_contract = build_metadata_indices_by_contract(&metadata);
    Ok(TextIndex {
        names,
        name_indices_by_normalized,
        metadata,
        metadata_token_ids,
        metadata_corpus,
        metadata_indices_by_token,
        metadata_indices_by_contract,
    })
}

fn load_name_index(conn: &Connection, chain: &str) -> Result<Vec<NameCandidate>, duckdb::Error> {
    let mut stmt = conn.prepare(
        "
        SELECT lower(contract_address) AS contract_address,
               coalesce(CAST(name AS VARCHAR), '') AS name,
               min(CAST(token_id AS VARCHAR)) AS first_token_id
        FROM nft_features
        WHERE chain = ? AND trim(coalesce(CAST(name AS VARCHAR), '')) <> ''
        GROUP BY contract_address, name
        ORDER BY contract_address, first_token_id
        ",
    )?;
    let rows = stmt.query_map(params![chain], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;

    let mut by_contract = HashMap::<String, NameCandidateBuilder>::new();
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
        let builder = by_contract.entry(contract_address).or_default();
        if builder.display_name.is_none() {
            builder.display_name = Some(trimmed.to_string());
        }
        builder.normalized_names.insert(normalized);
    }

    let mut candidates = by_contract
        .into_iter()
        .filter_map(|(contract_address, builder)| {
            let display_name = builder.display_name?;
            let normalized_names = builder.normalized_names.into_iter().collect::<Vec<_>>();
            (!normalized_names.is_empty()).then_some(NameCandidate {
                contract_address,
                display_name,
                normalized_names,
            })
        })
        .collect::<Vec<_>>();
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
            SELECT contract_address, metadata_doc, metadata_json,
                   row_number() OVER (
                       PARTITION BY contract_address
                       ORDER BY CASE
                           WHEN trim(metadata_json) LIKE '{%' OR trim(metadata_json) LIKE '[%' THEN 0
                           WHEN trim(metadata_json) <> '' THEN 1
                           WHEN trim(metadata_doc) <> '' THEN 2
                           ELSE 3
                       END, token_id
                   ) AS metadata_rank
            FROM selected
        )
        SELECT contract_address, metadata_doc, metadata_json
        FROM ranked
        WHERE metadata_rank = 1
          AND (trim(metadata_doc) <> '' OR trim(metadata_json) <> '')
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
        let doc_text = record_metadata_doc(&metadata_doc, &metadata_json);
        let Some(doc) = MetadataDocument::from_text_with_interner(&doc_text, token_interner) else {
            continue;
        };
        let text = record_metadata_text(&metadata_doc, &metadata_json);
        candidates.push(MetadataCandidate {
            contract_address,
            text,
            doc,
        });
    }
    Ok(candidates)
}

fn build_metadata_indices_by_token(metadata: &[MetadataCandidate]) -> HashMap<TokenId, Vec<usize>> {
    let mut index = HashMap::<TokenId, Vec<usize>>::new();
    for (doc_index, candidate) in metadata.iter().enumerate() {
        for token in &candidate.doc.unique_tokens {
            index.entry(*token).or_default().push(doc_index);
        }
    }
    index
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

fn first_seed_name(rows: &[NftTextRow]) -> String {
    rows.iter()
        .find_map(|row| non_empty(&row.name))
        .unwrap_or_default()
}

fn seed_name_norms(rows: &[NftTextRow]) -> Vec<String> {
    rows.iter()
        .map(|row| normalize_name(&row.name))
        .filter(|name| !name.is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn first_seed_metadata(rows: &[NftTextRow]) -> String {
    rows.iter()
        .find_map(|row| non_empty_json(&row.metadata_json))
        .or_else(|| {
            rows.iter()
                .find_map(|row| non_empty(&record_metadata_text(&row.metadata_doc, &row.metadata_json)))
        })
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
                        .filter(|index| text_index.names[*index].contract_address != seed_contract)
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
                    .filter(|index| text_index.names[*index].contract_address != seed_contract),
            );
        }
    }
    indices
}

fn match_metadata(
    seed_metadata: &str,
    text_index: &TextIndex,
    scratch: &mut SampleScratch,
    seed_contract: &str,
    threshold: f64,
    limit: usize,
) -> Vec<String> {
    let scoring_seed_metadata = metadata_scoring_text(seed_metadata);
    let Some(seed_doc) = MetadataDocument::from_text_with_vocab(
        &scoring_seed_metadata,
        &text_index.metadata_token_ids,
    ) else {
        return Vec::new();
    };
    let seed_contract = seed_contract.to_lowercase();
    let excluded_indices = text_index
        .metadata_indices_by_contract
        .get(&seed_contract)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let corpus = MetadataCorpusView::new(
        &text_index.metadata_corpus,
        &text_index.metadata,
        excluded_indices,
    );
    if corpus.total_docs == 0 {
        return Vec::new();
    }

    let epoch = scratch.next_metadata_epoch();
    let mut candidate_indices = Vec::new();
    for token in &seed_doc.unique_tokens {
        let Some(indices) = text_index.metadata_indices_by_token.get(token) else {
            continue;
        };
        for index in indices {
            if text_index.metadata[*index].contract_address == seed_contract {
                continue;
            }
            if scratch.metadata_seen_epochs[*index] == epoch {
                continue;
            }
            scratch.metadata_seen_epochs[*index] = epoch;
            candidate_indices.push(*index);
        }
    }

    let matched_indices = candidate_indices
        .par_iter()
        .filter_map(|index| {
            let candidate = &text_index.metadata[*index];
            (score_metadata_pair(&seed_doc, &candidate.doc, &corpus) >= threshold).then_some(*index)
        })
        .collect::<Vec<_>>();
    materialize_metadata_matches(text_index, matched_indices, limit)
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

fn materialize_metadata_matches(
    text_index: &TextIndex,
    indices: Vec<usize>,
    limit: usize,
) -> Vec<String> {
    take_recall_limit(
        indices
            .into_iter()
            .map(|index| text_index.metadata[index].text.trim().to_string())
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

fn record_metadata_doc(metadata_doc: &str, metadata_json: &str) -> String {
    if !metadata_doc.trim().is_empty() {
        metadata_doc.to_string()
    } else {
        metadata_document_from_json(metadata_json)
    }
}

fn record_metadata_text(metadata_doc: &str, metadata_json: &str) -> String {
    if !metadata_json.trim().is_empty() {
        metadata_json.to_string()
    } else {
        metadata_doc.to_string()
    }
}

fn metadata_scoring_text(value: &str) -> String {
    if looks_like_json(value) {
        metadata_document_from_json(value)
    } else {
        value.to_string()
    }
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
    excluded_doc_freqs: HashMap<TokenId, usize>,
    total_docs: usize,
    avg_doc_len: f64,
}

impl<'a> MetadataCorpusView<'a> {
    fn new(
        base: &'a MetadataCorpus,
        documents: &'a [MetadataCandidate],
        excluded_indices: &'a [usize],
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
            excluded_doc_freqs,
            total_docs,
            avg_doc_len,
        }
    }

    fn document_frequency(&self, token: TokenId) -> usize {
        let excluded_frequency = self.excluded_doc_freqs.get(&token).copied().unwrap_or(0);
        self.base
            .doc_freqs
            .get(&token)
            .copied()
            .unwrap_or(0)
            .saturating_sub(excluded_frequency)
    }
}

fn score_metadata_pair(
    left: &MetadataDocument,
    right: &MetadataDocument,
    corpus: &MetadataCorpusView<'_>,
) -> f64 {
    let query_terms = query_terms_from_tokens(&left.tokens);
    let self_score = bm25_score_terms(&query_terms, left, corpus);
    let denominator = if self_score > 0.0 { self_score } else { 1.0 };
    (bm25_score_terms(&query_terms, right, corpus) / denominator).clamp(0.0, 1.0)
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

fn non_empty_json(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if looks_like_json(trimmed) {
        Some(trimmed.to_string())
    } else {
        None
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

fn build_metadata_comparison(seed: String, matches: Vec<String>) -> TextComparison {
    let labeled_matches = matches
        .iter()
        .map(|value| LabeledTextMatch {
            text: value.clone(),
            labels: classify_metadata_modifications(&seed, value),
        })
        .collect();
    TextComparison {
        seed,
        matches,
        labeled_matches,
    }
}

fn classify_name_modifications(seed: &str, value: &str) -> Vec<String> {
    let mut labels = Vec::new();
    let seed_trimmed = seed.trim();
    let value_trimmed = value.trim();
    let seed_norm = normalize_name(seed_trimmed);
    let value_norm = normalize_name(value_trimmed);

    if seed_trimmed == value_trimmed && !seed_trimmed.is_empty() {
        return vec!["exact_clone".to_string()];
    }
    let suffix_augmented =
        has_suffix_augmentation(seed_trimmed, value_trimmed, &seed_norm, &value_norm);
    if suffix_augmented {
        labels.push("suffix_augmentation");
    }
    if has_name_format_perturbation(seed_trimmed, value_trimmed) {
        labels.push("format_perturbation");
    }
    let seed_lexical_raw = strip_raw_trailing_number_suffix(seed_trimmed);
    let value_lexical_raw = if suffix_augmented {
        strip_raw_augmentation_suffix(value_trimmed)
    } else {
        strip_raw_trailing_number_suffix(value_trimmed)
    };
    let seed_lexical_core = canonical_format_name(&seed_lexical_raw);
    let value_lexical_core = canonical_format_name(&value_lexical_raw);
    if has_name_lexical_mutation(&seed_lexical_core, &value_lexical_core) {
        labels.push("lexical_mutation");
    }
    if labels.is_empty() {
        labels.push("other");
    }
    labels.into_iter().map(str::to_string).collect()
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
            return vec!["metadata_unchanged".to_string()];
        }
        return vec!["unparseable_changed".to_string()];
    };

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
                    (None, Some(_value_value)) => {
                        add_metadata_change(path, "added", labels);
                    }
                    (Some(_seed_value), None) => {
                        add_metadata_change(path, "removed", labels);
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
            for _ in &value_items[common_len..] {
                path.push(common_len.to_string());
                add_metadata_change(path, "added", labels);
                path.pop();
            }
            for _ in &seed_items[common_len..] {
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

fn has_suffix_augmentation(seed: &str, value: &str, seed_norm: &str, value_norm: &str) -> bool {
    if has_derivative_suffix(value) {
        let stripped_value = strip_raw_augmentation_suffix(value);
        return canonical_format_name(seed) == canonical_format_name(&stripped_value);
    }

    let seed_has_numeric_suffix = has_trailing_number_suffix(seed);
    let value_has_numeric_suffix = has_trailing_number_suffix(value);
    if !seed_has_numeric_suffix && !value_has_numeric_suffix {
        return false;
    }
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
    let seed_core = strip_raw_trailing_number_suffix(seed);
    let value_core = strip_raw_trailing_number_suffix(value);
    seed_core != value_core
        && canonical_format_name(&seed_core) == canonical_format_name(&value_core)
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
    let mut text = strip_raw_trailing_number_suffix(raw);
    loop {
        let mut changed = false;
        for pattern in DERIVATIVE_SUFFIX_PATTERNS.iter() {
            let updated = pattern.replace(&text, "").trim().to_string();
            if updated != text {
                text = updated;
                changed = true;
                break;
            }
        }
        if !changed {
            break;
        }
    }
    text
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
        let values = labeled_matches_for_report(&seed.metadata, classify_metadata_modifications);
        if values.is_empty() {
            continue;
        }
        out.push_str("- seed:\n\n");
        push_fenced(&mut out, &seed.metadata.seed);
        for value in values {
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
    reports: impl Iterator<Item = &'a TextComparison>,
) {
    let mut total = 0usize;
    let mut content_total = 0usize;
    let mut non_content_total = 0usize;
    let mut region_totals = BTreeMap::<String, usize>::new();
    let mut matrix = BTreeMap::<(String, String), usize>::new();
    let mut residual_counts = BTreeMap::<String, usize>::new();
    for comparison in reports {
        let values = labeled_matches_for_report(comparison, classify_metadata_modifications);
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

    out.push_str(&format!("- total matches: {total}\n"));
    out.push_str(&format!("- content-bearing changes: {content_total}\n"));
    out.push_str(&format!(
        "- non-content-bearing changes: {non_content_total}\n"
    ));
    for label in ["unparseable_changed", "metadata_unchanged"] {
        if let Some(count) = residual_counts.get(label) {
            out.push_str(&format!("- {label}: {count}\n"));
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
        out.push_str(&format!(" {count} |"));
    }
    out.push('\n');
    for operation in METADATA_OPERATIONS {
        out.push_str(&format!("| {operation} |"));
        for region in METADATA_REGIONS {
            let count = matrix
                .get(&(operation.to_string(), region.to_string()))
                .copied()
                .unwrap_or(0);
            out.push_str(&format!(" {count} |"));
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
    fn metadata_bm25_uses_expected_constants() {
        assert_eq!(METADATA_BM25_K1, 1.2);
        assert_eq!(METADATA_BM25_B, 0.75);
    }

    #[test]
    fn query_terms_preserve_duplicate_token_frequency() {
        assert_eq!(query_terms_from_tokens(&[7, 7, 9]), vec![(7, 2), (9, 1)]);
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
                metadata: TextComparison {
                    seed: r#"{"description":"background gold","image":"ipfs://seed/image.png"}"#
                        .into(),
                    matches: vec![
                        r#"{"description":"background gold","image":"ipfs://copy/image.png"}"#
                            .into(),
                    ],
                    labeled_matches: Vec::new(),
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
        assert!(output.contains("| replaced | 0 | 0 | 0 | 1 | 0 | 0 | 0 |"));
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
    fn name_uses_paper_level_mutually_exclusive_labels() {
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
            vec!["lexical_mutation"]
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
    fn name_other_examples_are_broad_lexical_mutations() {
        assert_eq!(
            classify_name_modifications("PudgyPenguins", "Phudgy Penguins"),
            vec!["lexical_mutation"]
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
                    metadata: TextComparison::default(),
                },
                SeedSampleReport {
                    name: TextComparison {
                        seed: "None".into(),
                        matches: vec!["None".into()],
                        labeled_matches: Vec::new(),
                    },
                    metadata: TextComparison::default(),
                },
                SeedSampleReport {
                    name: TextComparison {
                        seed: "Real Seed".into(),
                        matches: vec!["Real Seed".into()],
                        labeled_matches: Vec::new(),
                    },
                    metadata: TextComparison::default(),
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
                    metadata: TextComparison::default(),
                },
                SeedSampleReport {
                    name: TextComparison {
                        seed: "Real Seed".into(),
                        matches: vec!["Real Seed".into()],
                        labeled_matches: Vec::new(),
                    },
                    metadata: TextComparison::default(),
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
                    metadata: TextComparison {
                        seed: "no hit metadata".into(),
                        matches: Vec::new(),
                        labeled_matches: Vec::new(),
                    },
                },
                SeedSampleReport {
                    name: TextComparison {
                        seed: "Name Hit Seed".into(),
                        matches: vec!["Name Hit Seed".into()],
                        labeled_matches: Vec::new(),
                    },
                    metadata: TextComparison::default(),
                },
                SeedSampleReport {
                    name: TextComparison::default(),
                    metadata: TextComparison {
                        seed: "metadata hit seed".into(),
                        matches: vec!["metadata hit seed changed".into()],
                        labeled_matches: Vec::new(),
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
            vec!["metadata_unchanged"]
        );
    }

    #[test]
    fn metadata_summary_uses_operation_region_matrix_and_group_totals() {
        let report = SampleReport {
            chain: "ethereum".into(),
            seed_reports: vec![SeedSampleReport {
                name: TextComparison::default(),
                metadata: TextComparison {
                    seed:
                        r#"{"name":"Seed","title":"Old","image":"ipfs://seed","seller_fee_basis_points":500}"#
                            .into(),
                    matches: vec![
                        r#"{"name":"Copy","image":"ipfs://copy","seller_fee_basis_points":750}"#
                            .into(),
                    ],
                    labeled_matches: Vec::new(),
                },
            }],
        };

        let output = render_markdown_report(&report);

        assert!(output.contains("#### Metadata Change Matrix"));
        assert!(output.contains("| operation | title | description | attributes | references | auxiliary_fields | platform_fields | structure |"));
        assert!(output.contains("| total | 1 | 0 | 0 | 1 | 0 | 1 | 0 |"));
        assert!(output.contains("| removed | 1 | 0 | 0 | 0 | 0 | 0 | 0 |"));
        assert!(output.contains("| replaced | 1 | 0 | 0 | 1 | 0 | 1 | 0 |"));
        assert!(output.contains("- content-bearing changes: 1"));
        assert!(output.contains("- non-content-bearing changes: 1"));
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
                metadata: TextComparison {
                    seed: String::new(),
                    matches: Vec::new(),
                    labeled_matches: Vec::new(),
                },
            }],
        };

        let output = render_markdown_report(&report);

        assert!(!output.contains("- seed: _unavailable_"));
        assert!(!output.contains("[exact_clone] _unavailable_"));
        assert!(!output.contains("contract:"));
    }
}
