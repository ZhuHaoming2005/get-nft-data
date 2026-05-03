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
    ]
});
static WHITESPACE_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\s+").unwrap());
static TOKEN_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"[\p{L}\p{N}_]+").unwrap());
static ASSET_REF_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)(ipfs://[^\s]+|https?://[^\s]*ipfs[^\s]+|\bqm[1-9a-hj-np-z]{20,}\b|\bbafy[a-z2-7]{20,}\b)")
        .unwrap()
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
                           WHEN trim(metadata_doc) <> '' OR trim(metadata_json) <> '' THEN 0
                           ELSE 1
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
        let text = record_metadata_doc(&metadata_doc, &metadata_json);
        let Some(doc) = MetadataDocument::from_text_with_interner(&text, token_interner) else {
            continue;
        };
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
        .find_map(|row| non_empty(&record_metadata_doc(&row.metadata_doc, &row.metadata_json)))
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
    let Some(seed_doc) =
        MetadataDocument::from_text_with_vocab(seed_metadata, &text_index.metadata_token_ids)
    else {
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
    let seed_lower = normalize_nfkc(seed_trimmed).to_lowercase();
    let value_lower = normalize_nfkc(value_trimmed).to_lowercase();
    let seed_compact = WHITESPACE_RE.replace_all(&seed_lower, "").to_string();
    let value_compact = WHITESPACE_RE.replace_all(&value_lower, "").to_string();
    let seed_norm = normalize_name(seed_trimmed);
    let value_norm = normalize_name(value_trimmed);

    if seed_trimmed == value_trimmed && !seed_trimmed.is_empty() {
        labels.push("exact_clone");
    }
    if seed_lower == value_lower && seed_trimmed != value_trimmed {
        labels.push("case_change");
    }
    if seed_compact == value_compact && seed_lower != value_lower {
        labels.push("spacing_change");
    }
    if value_trimmed.chars().any(should_render_as_codepoint) {
        labels.push("invisible_unicode");
    }
    if normalize_nfkc(value_trimmed) != value_trimmed {
        labels.push("unicode_compatibility");
    }
    if seed_norm == value_norm
        && seed_trimmed != value_trimmed
        && (has_trailing_number_suffix(seed_trimmed) || has_trailing_number_suffix(value_trimmed))
    {
        labels.push("token_number_suffix");
    }
    if has_derivative_suffix(value_trimmed) {
        labels.push("derivative_suffix");
    }
    if has_inserted_ai_marker(seed_trimmed, value_trimmed) {
        labels.push("ai_marker");
    }
    if labels.is_empty()
        && score_normalized_name_pair(&seed_norm, &value_norm) >= 90.0
        && seed_norm != value_norm
    {
        labels.push("homoglyph_or_typo");
    }
    if labels.is_empty() {
        labels.push("other");
    }
    labels.into_iter().map(str::to_string).collect()
}

fn classify_metadata_modifications(seed: &str, value: &str) -> Vec<String> {
    let mut labels = Vec::new();
    if normalize_text(seed) == normalize_text(value) && !value.trim().is_empty() {
        labels.push("exact_metadata_clone");
    }
    if !shared_asset_refs(seed, value).is_empty() {
        labels.push("asset_pointer_reuse");
    } else if !asset_refs(value).is_empty() {
        labels.push("asset_pointer_present");
    }
    if has_shared_terms(seed, value, TRAIT_SCHEMA_TERMS) {
        labels.push("trait_schema_reuse");
    }
    if has_shared_terms(seed, value, COLLECTION_TERMS) {
        labels.push("collection_terms_reuse");
    }
    let value_lower = normalize_text(value);
    if value_lower.contains("trait") && value_lower.contains('%') {
        labels.push("rarity_text_added");
    }
    if labels.is_empty() {
        labels.push("other");
    }
    labels.into_iter().map(str::to_string).collect()
}

const TRAIT_SCHEMA_TERMS: &[&str] = &[
    "attribute",
    "attributes",
    "background",
    "clothes",
    "eyes",
    "fur",
    "hat",
    "headwear",
    "mouth",
    "trait",
];

const COLLECTION_TERMS: &[&str] = &[
    "ape", "azuki", "bayc", "beanz", "bored", "cool", "doodle", "milady", "mooncat", "mutant",
    "pudgy",
];

fn has_trailing_number_suffix(value: &str) -> bool {
    let normalized = normalize_nfkc(value);
    TRAILING_PATTERNS
        .iter()
        .any(|pattern| pattern.is_match(&normalized))
}

fn has_derivative_suffix(value: &str) -> bool {
    let value = normalize_nfkc(value).to_lowercase();
    value.ends_with("404")
        || value.ends_with("v2")
        || value.ends_with(".fun")
        || value.ends_with('x')
}

fn has_inserted_ai_marker(seed: &str, value: &str) -> bool {
    !normalize_nfkc(seed).to_lowercase().contains("ai")
        && normalize_nfkc(value).to_lowercase().contains("ai")
}

fn asset_refs(value: &str) -> BTreeSet<String> {
    ASSET_REF_RE
        .find_iter(value)
        .map(|m| {
            m.as_str()
                .trim_matches(|ch| ch == '"' || ch == '\'')
                .to_lowercase()
        })
        .collect()
}

fn shared_asset_refs(seed: &str, value: &str) -> BTreeSet<String> {
    let seed_refs = asset_refs(seed);
    asset_refs(value)
        .into_iter()
        .filter(|reference| seed_refs.contains(reference))
        .collect()
}

fn has_shared_terms(seed: &str, value: &str, terms: &[&str]) -> bool {
    let seed_tokens = metadata_tokens(seed).into_iter().collect::<BTreeSet<_>>();
    let value_tokens = metadata_tokens(value).into_iter().collect::<BTreeSet<_>>();
    terms
        .iter()
        .any(|term| seed_tokens.contains(*term) && value_tokens.contains(*term))
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
        out.push_str(&format!("- seed: {}\n", inline_or_empty(&seed.name.seed)));
        for value in labeled_matches_for_report(&seed.name, classify_name_modifications) {
            out.push_str(&format!(
                "  - [{}] {}\n",
                value.labels.join(", "),
                inline_or_empty(&value.text)
            ));
        }
    }

    out.push_str("\n## Metadata Matches\n\n");
    for seed in &report.seed_reports {
        out.push_str("- seed:\n\n");
        push_fenced(&mut out, &seed.metadata.seed);
        for value in labeled_matches_for_report(&seed.metadata, classify_metadata_modifications) {
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
    push_counts_with_ratios(
        out,
        modification_summary(
            report.seed_reports.iter().map(|seed| &seed.metadata),
            classify_metadata_modifications,
        ),
    );
    out.push('\n');
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
    !matches!(
        normalize_nfkc(trimmed).to_lowercase().as_str(),
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
                    seed: "background gold ipfs://seed/image.png".into(),
                    matches: vec!["background gold ipfs://seed/image.png".into()],
                    labeled_matches: Vec::new(),
                },
            }],
        };

        let output = render_markdown_report(&report);

        assert!(output.contains("## Modification Summary"));
        assert!(output.contains("- total matches: 2"));
        assert!(output.contains("- token_number_suffix: 1 (50.0%)"));
        assert!(output.contains("- unicode_compatibility: 1 (50.0%)"));
        assert!(output.contains("- other: 1 (50.0%)"));
        assert!(output.contains("- total matches: 1"));
        assert!(output.contains("- exact_metadata_clone: 1 (100.0%)"));
        assert!(output.contains("- asset_pointer_reuse: 1 (100.0%)"));
        assert!(output.contains("- trait_schema_reuse: 1 (100.0%)"));
        assert!(!output.contains("1/2"));
        assert!(!output.contains("1/1"));
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

        assert!(output.contains("- seed: _unavailable_"));
        assert!(output.contains("[exact_clone] _unavailable_"));
        assert!(!output.contains("contract:"));
    }
}
