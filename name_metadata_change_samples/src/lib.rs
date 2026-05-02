use std::fs;
use std::path::{Path, PathBuf};

use duckdb::{params, Connection};
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
    ensure_feature_db_exists(&config.feature_db)?;
    let seed_contracts = read_seed_contracts(&config.input)?;

    let mut seed_inputs = Vec::new();
    for seed_contract in seed_contracts {
        let seed_rows = read_contract_rows(
            &config.feature_db,
            &config.chain,
            &seed_contract,
            config.max_seed_tokens,
        )?;
        seed_inputs.push((seed_contract, seed_rows));
    }

    let resource_options =
        DuckDbResourceOptions::from_cli(config.duckdb_threads, &config.duckdb_memory_limit)?;

    let mut seed_reports = Vec::new();
    for (seed_contract, seed_rows) in seed_inputs {
        let seed_nfts = seed_rows
            .iter()
            .map(|row| seed_nft_from_database_row(&config.chain, row))
            .collect::<Vec<_>>();
        let (recalled_row_count, candidates) = {
            let store = DuckDbFeatureStore::new_with_options(
                &config.feature_db.to_string_lossy(),
                resource_options.clone(),
            )?;
            let snapshot = if seed_nfts.is_empty() {
                Default::default()
            } else {
                store.load_snapshot(
                    &config.chain,
                    &seed_nfts,
                    config.max_tokens_per_contract,
                    config.max_recall_rows,
                )?
            };
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
            (snapshot.nft_rows.len(), candidates)
        };
        let candidate_reports = candidate_reports(&config.feature_db, &config.chain, &candidates)?;

        seed_reports.push(SeedSampleReport {
            seed_sample: contract_sample_from_rows(&seed_contract, &seed_rows),
            contract_address: seed_contract,
            seed_row_count: seed_rows.len(),
            recalled_row_count,
            candidate_reports,
        });
    }

    let report = SampleReport {
        chain: config.chain.clone(),
        seed_reports,
    };
    write_markdown_report(&config.output, &report)?;
    Ok(report)
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

fn read_contract_rows(
    feature_db: &Path,
    chain: &str,
    contract_address: &str,
    limit: usize,
) -> Result<Vec<DatabaseNftRecord>, duckdb::Error> {
    let conn = Connection::open(feature_db)?;
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

fn candidate_reports(
    feature_db: &Path,
    chain: &str,
    candidates: &[DuplicateCandidate],
) -> Result<Vec<CandidateSampleReport>, SampleCollectionError> {
    candidates
        .iter()
        .map(
            |candidate| -> Result<CandidateSampleReport, SampleCollectionError> {
                let rows = read_contract_rows(feature_db, chain, &candidate.contract_address, 0)?;
                let recalled_row_count = rows.len();
                Ok(CandidateSampleReport {
                    contract_address: candidate.contract_address.clone(),
                    match_reasons: candidate.match_reasons.clone(),
                    confidence: candidate.confidence.clone(),
                    recalled_row_count,
                    sample: contract_sample_from_rows(&candidate.contract_address, &rows)
                        .unwrap_or_else(|| ContractSample {
                            contract_address: candidate.contract_address.clone(),
                            ..ContractSample::default()
                        }),
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
