use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use duckdb::{params, AccessMode, Config, Connection};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use top_contract_analysis_rs::analysis::duplicate::build_duplicate_candidates;
use top_contract_analysis_rs::error::AppError;
use top_contract_analysis_rs::models::{DatabaseNftRecord, DuplicateCandidate, SeedNft};
use top_contract_analysis_rs::store::{DuckDbFeatureStore, DuckDbResourceOptions};

#[derive(Clone, Debug, PartialEq)]
pub struct SampleCollectionConfig {
    pub chain: String,
    pub feature_db: PathBuf,
    pub input: PathBuf,
    pub output: PathBuf,
    pub name_threshold: f64,
    pub metadata_threshold: f64,
    pub max_tokens_per_contract: usize,
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
    pub contract_address: String,
    pub seed_sample: Option<ContractSample>,
    pub seed_row_count: usize,
    pub recalled_row_count: usize,
    pub candidate_reports: Vec<CandidateSampleReport>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContractSample {
    pub contract_address: String,
    pub name: String,
    pub symbol: String,
    pub metadata_source_token_id: String,
    pub metadata_doc: String,
    pub metadata_json: String,
    pub row_count: usize,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct CandidateSampleReport {
    pub contract_address: String,
    pub match_reasons: Vec<String>,
    pub confidence: String,
    pub recalled_row_count: usize,
    pub sample: ContractSample,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum SampleProgressStage {
    ReadSeedRows,
    LoadRecallRows,
    ScoreCandidates,
    LoadCandidateSamples,
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
    #[error("analysis error: {0}")]
    Analysis(#[from] AppError),
    #[error("input file did not contain any contract addresses")]
    EmptyInput,
    #[error("feature DB does not exist: {0}")]
    MissingFeatureDb(String),
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
    let resource_options =
        DuckDbResourceOptions::from_cli(config.duckdb_threads, &config.duckdb_memory_limit)?;
    let mut worker = SampleWorker::new(&config, &resource_options)?;
    let mut seed_reports = Vec::with_capacity(seed_contracts.len());
    for (index, seed_contract) in seed_contracts.iter().enumerate() {
        seed_reports.push(worker.process_seed_contract(
            &config,
            seed_contract,
            index + 1,
            total,
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

struct SampleWorker {
    store: DuckDbFeatureStore,
    sample_conn: Connection,
}

impl SampleWorker {
    fn new(
        config: &SampleCollectionConfig,
        resource_options: &DuckDbResourceOptions,
    ) -> Result<Self, SampleCollectionError> {
        let store = DuckDbFeatureStore::open_read_only_with_options(
            &config.feature_db.to_string_lossy(),
            resource_options.clone(),
        )?;
        let sample_conn =
            open_read_only_connection_with_options(&config.feature_db, resource_options)?;
        Ok(Self { store, sample_conn })
    }

    fn process_seed_contract(
        &mut self,
        config: &SampleCollectionConfig,
        seed_contract: &str,
        seed_index: usize,
        total_seeds: usize,
        progress: &mut impl FnMut(SampleProgress),
    ) -> Result<SeedSampleReport, SampleCollectionError> {
        let seed_rows = read_contract_rows_with_conn(
            &self.sample_conn,
            &config.chain,
            seed_contract,
            config.max_seed_tokens,
        )?;
        emit_progress(
            progress,
            seed_index,
            total_seeds,
            SampleProgressStage::ReadSeedRows,
            1,
            None,
        );
        let seed_nfts = seed_rows
            .iter()
            .map(|row| seed_nft_from_database_row(&config.chain, row))
            .collect::<Vec<_>>();
        let snapshot = if seed_nfts.is_empty() {
            Default::default()
        } else {
            self.store.load_snapshot(
                &config.chain,
                &seed_nfts,
                config.max_tokens_per_contract,
                config.max_recall_rows,
            )?
        };
        emit_progress(
            progress,
            seed_index,
            total_seeds,
            SampleProgressStage::LoadRecallRows,
            2,
            None,
        );
        let candidates = build_duplicate_candidates(
            &config.chain,
            &seed_nfts,
            &snapshot.nft_rows,
            config.name_threshold,
            config.metadata_threshold,
        )
        .into_iter()
        .filter(is_name_or_metadata_candidate)
        .collect::<Vec<_>>();
        emit_progress(
            progress,
            seed_index,
            total_seeds,
            SampleProgressStage::ScoreCandidates,
            3,
            Some(candidates.len()),
        );
        let recalled_row_count = snapshot.nft_rows.len();
        let candidate_reports =
            candidate_reports_with_conn(&self.sample_conn, &config.chain, &candidates)?;
        emit_progress(
            progress,
            seed_index,
            total_seeds,
            SampleProgressStage::LoadCandidateSamples,
            4,
            Some(candidate_reports.len()),
        );

        let seed_report = SeedSampleReport {
            seed_sample: contract_sample_from_rows(seed_contract, &seed_rows),
            contract_address: seed_contract.to_string(),
            seed_row_count: seed_rows.len(),
            recalled_row_count,
            candidate_reports,
        };
        emit_progress(
            progress,
            seed_index,
            total_seeds,
            SampleProgressStage::FinishedSeed,
            5,
            Some(seed_report.candidate_reports.len()),
        );
        Ok(seed_report)
    }
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
        stage_count: 5,
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

fn read_contract_rows_with_conn(
    conn: &Connection,
    chain: &str,
    contract_address: &str,
    limit: usize,
) -> Result<Vec<DatabaseNftRecord>, duckdb::Error> {
    let limit_sql = if limit > 0 {
        format!(" LIMIT {limit}")
    } else {
        String::new()
    };
    let sql = format!(
        "
        SELECT contract_address, token_id, coalesce(token_uri, ''), coalesce(image_uri, ''),
               coalesce(name, ''), coalesce(symbol, ''), coalesce(metadata_json, ''),
               coalesce(metadata_doc, '')
        FROM nft_features
        WHERE chain = ? AND lower(contract_address) = ?
        ORDER BY token_id
        {limit_sql}
        "
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(params![chain, contract_address.to_lowercase()], |row| {
        Ok(DatabaseNftRecord {
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
        })
    })?;

    rows.collect()
}

pub fn collect_contract_samples(
    feature_db: &Path,
    chain: &str,
    contract_addresses: &[String],
) -> Result<Vec<ContractSample>, SampleCollectionError> {
    let conn = open_read_only_connection(feature_db)?;
    collect_contract_samples_with_conn(&conn, chain, contract_addresses)
}

fn collect_contract_samples_with_conn(
    conn: &Connection,
    chain: &str,
    contract_addresses: &[String],
) -> Result<Vec<ContractSample>, SampleCollectionError> {
    if contract_addresses.is_empty() {
        return Ok(Vec::new());
    }

    let mut requested_addresses = Vec::new();
    for address in contract_addresses {
        let normalized = address.to_lowercase();
        if !requested_addresses.contains(&normalized) {
            requested_addresses.push(normalized);
        }
    }
    let address_values = requested_addresses
        .iter()
        .map(|address| sql_string_literal(address))
        .collect::<Vec<_>>()
        .join(", ");

    let sql = format!(
        "
        WITH selected AS (
            SELECT lower(contract_address) AS contract_address,
                   CAST(token_id AS VARCHAR) AS token_id,
                   coalesce(CAST(name AS VARCHAR), '') AS name,
                   coalesce(CAST(symbol AS VARCHAR), '') AS symbol,
                   coalesce(CAST(metadata_doc AS VARCHAR), '') AS metadata_doc,
                   coalesce(CAST(metadata_json AS VARCHAR), '') AS metadata_json
            FROM nft_features
            WHERE chain = ? AND lower(contract_address) IN ({address_values})
        ),
        ranked AS (
            SELECT contract_address, token_id, name, symbol, metadata_doc, metadata_json,
                   count(*) OVER (PARTITION BY contract_address) AS row_count,
                   row_number() OVER (
                       PARTITION BY contract_address
                       ORDER BY CASE WHEN name <> '' THEN 0 ELSE 1 END, token_id
                   ) AS name_rank,
                   row_number() OVER (
                       PARTITION BY contract_address
                       ORDER BY CASE WHEN symbol <> '' THEN 0 ELSE 1 END, token_id
                   ) AS symbol_rank,
                   row_number() OVER (
                       PARTITION BY contract_address
                       ORDER BY CASE WHEN metadata_doc <> '' OR metadata_json <> '' THEN 0 ELSE 1 END, token_id
                   ) AS metadata_rank
            FROM selected
        )
        SELECT contract_address,
               max(CASE WHEN name_rank = 1 THEN name ELSE NULL END) AS name,
               max(CASE WHEN symbol_rank = 1 THEN symbol ELSE NULL END) AS symbol,
               max(CASE WHEN metadata_rank = 1 THEN token_id ELSE NULL END) AS metadata_source_token_id,
               max(CASE WHEN metadata_rank = 1 THEN metadata_doc ELSE NULL END) AS metadata_doc,
               max(CASE WHEN metadata_rank = 1 THEN metadata_json ELSE NULL END) AS metadata_json,
               max(row_count) AS row_count
        FROM ranked
        GROUP BY contract_address
        "
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(params![chain], |row| {
        Ok(ContractSample {
            contract_address: row.get::<_, String>(0)?,
            name: row.get::<_, String>(1)?,
            symbol: row.get::<_, String>(2)?,
            metadata_source_token_id: row.get::<_, String>(3)?,
            metadata_doc: row.get::<_, String>(4)?,
            metadata_json: row.get::<_, String>(5)?,
            row_count: row.get::<_, i64>(6)? as usize,
        })
    })?;
    let mut samples_by_contract = HashMap::new();
    for row in rows {
        let sample = row?;
        samples_by_contract.insert(sample.contract_address.clone(), sample);
    }

    Ok(requested_addresses
        .into_iter()
        .filter_map(|address| samples_by_contract.remove(&address))
        .collect())
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn open_read_only_connection(path: &Path) -> Result<Connection, duckdb::Error> {
    if path == Path::new(":memory:") {
        Connection::open_in_memory()
    } else {
        Connection::open_with_flags(path, Config::default().access_mode(AccessMode::ReadOnly)?)
    }
}

fn open_read_only_connection_with_options(
    path: &Path,
    options: &DuckDbResourceOptions,
) -> Result<Connection, duckdb::Error> {
    let conn = open_read_only_connection(path)?;
    let memory_limit = options.memory_limit.replace('\'', "''");
    conn.execute_batch(&format!(
        "
        PRAGMA threads={};
        PRAGMA memory_limit='{}';
        PRAGMA preserve_insertion_order=false;
        ",
        options.threads.max(1),
        memory_limit
    ))?;
    Ok(conn)
}

fn seed_nft_from_database_row(chain: &str, row: &DatabaseNftRecord) -> SeedNft {
    SeedNft {
        chain: chain.to_string(),
        contract_address: row.contract_address.clone(),
        token_id: row.token_id.clone(),
        name: row.name.clone(),
        symbol: row.symbol.clone(),
        token_uri: row.token_uri.clone(),
        image_uri: row.image_uri.clone(),
        metadata_json: row.metadata_json.clone(),
        metadata_doc: row.metadata_doc.clone(),
    }
}

fn is_name_or_metadata_candidate(candidate: &DuplicateCandidate) -> bool {
    candidate
        .match_reasons
        .iter()
        .any(|reason| matches!(reason.as_str(), "name_match" | "metadata_match"))
}

fn candidate_reports_with_conn(
    conn: &Connection,
    chain: &str,
    candidates: &[DuplicateCandidate],
) -> Result<Vec<CandidateSampleReport>, SampleCollectionError> {
    let contract_addresses = candidates
        .iter()
        .map(|candidate| candidate.contract_address.clone())
        .collect::<Vec<_>>();
    let mut samples_by_contract =
        collect_contract_samples_with_conn(conn, chain, &contract_addresses)?
            .into_iter()
            .map(|sample| (sample.contract_address.clone(), sample))
            .collect::<HashMap<_, _>>();

    candidates
        .iter()
        .map(
            |candidate| -> Result<CandidateSampleReport, SampleCollectionError> {
                let sample = samples_by_contract
                    .remove(&candidate.contract_address.to_lowercase())
                    .unwrap_or_else(|| ContractSample {
                        contract_address: candidate.contract_address.clone(),
                        ..ContractSample::default()
                    });
                Ok(CandidateSampleReport {
                    contract_address: candidate.contract_address.clone(),
                    match_reasons: candidate.match_reasons.clone(),
                    confidence: candidate.confidence.clone(),
                    recalled_row_count: sample.row_count,
                    sample,
                })
            },
        )
        .collect()
}

fn contract_sample_from_rows(
    contract_address: &str,
    rows: &[DatabaseNftRecord],
) -> Option<ContractSample> {
    if rows.is_empty() {
        return None;
    }

    let mut sorted_rows = rows.to_vec();
    sorted_rows.sort_by(|left, right| left.token_id.cmp(&right.token_id));

    let name = sorted_rows
        .iter()
        .find_map(|row| non_empty(&row.name))
        .unwrap_or_default();
    let symbol = sorted_rows
        .iter()
        .find_map(|row| non_empty(&row.symbol))
        .unwrap_or_default();
    let metadata_row = sorted_rows
        .iter()
        .find(|row| !row.metadata_doc.trim().is_empty() || !row.metadata_json.trim().is_empty())
        .unwrap_or(&sorted_rows[0]);

    Some(ContractSample {
        contract_address: contract_address.to_string(),
        name,
        symbol,
        metadata_source_token_id: metadata_row.token_id.clone(),
        metadata_doc: metadata_row.metadata_doc.clone(),
        metadata_json: metadata_row.metadata_json.clone(),
        row_count: sorted_rows.len(),
    })
}

fn non_empty(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
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

    for seed in &report.seed_reports {
        out.push_str(&format!("## Seed Contract `{}`\n\n", seed.contract_address));
        out.push_str(&format!("- local seed rows: {}\n", seed.seed_row_count));
        out.push_str(&format!(
            "- recalled local rows: {}\n",
            seed.recalled_row_count
        ));
        out.push_str(&format!(
            "- name/metadata candidates: {}\n\n",
            seed.candidate_reports.len()
        ));

        out.push_str("### Seed Name/Metadata\n\n");
        render_optional_contract_sample(&mut out, seed.seed_sample.as_ref());

        for candidate in &seed.candidate_reports {
            out.push_str(&format!(
                "### Candidate Contract `{}`\n\n",
                candidate.contract_address
            ));
            out.push_str(&format!(
                "- match reasons: {}\n",
                candidate.match_reasons.join(", ")
            ));
            out.push_str(&format!("- confidence: {}\n", candidate.confidence));
            out.push_str(&format!(
                "- recalled rows in local DB: {}\n\n",
                candidate.recalled_row_count
            ));
            render_contract_sample(&mut out, &candidate.sample);
        }
    }

    out
}

fn render_optional_contract_sample(out: &mut String, sample: Option<&ContractSample>) {
    if let Some(sample) = sample {
        render_contract_sample(out, sample);
    } else {
        out.push_str("_No local rows found._\n\n");
    }
}

fn render_contract_sample(out: &mut String, sample: &ContractSample) {
    out.push_str(&format!("- contract: `{}`\n", sample.contract_address));
    out.push_str(&format!("- name: {}\n", inline_or_empty(&sample.name)));
    out.push_str(&format!("- symbol: {}\n", inline_or_empty(&sample.symbol)));
    out.push_str(&format!(
        "- metadata source token: `{}`\n",
        sample.metadata_source_token_id
    ));
    out.push_str(&format!("- local row count: {}\n", sample.row_count));
    out.push_str("\nmetadata_doc:\n\n");
    push_fenced(out, "text", &sample.metadata_doc);
    out.push_str("\nmetadata_json:\n\n");
    push_fenced(out, "json", &sample.metadata_json);
    out.push('\n');
}

fn inline_or_empty(value: &str) -> String {
    if value.trim().is_empty() {
        "_empty_".to_string()
    } else {
        value.to_string()
    }
}

fn push_fenced(out: &mut String, language: &str, value: &str) {
    if value.trim().is_empty() {
        out.push_str("_empty_\n");
        return;
    }
    out.push_str(&format!("````{language}\n{value}\n````\n"));
}
