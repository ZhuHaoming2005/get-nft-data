use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use duckdb::Connection;
use name_uri_analysis_rs::analysis::{parquet_sql_literal, DUCKDB_THREAD_CAP};
use name_uri_analysis_rs::{sha256_file, sha256_hex};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct InputFingerprint {
    pub(crate) file_id: u32,
    pub(crate) path: PathBuf,
    pub(crate) size: u64,
    pub(crate) modified_unix_nanos: u128,
    pub(crate) row_count: u64,
    pub(crate) row_group_count: u64,
    pub(crate) min_row_group_rows: u64,
    pub(crate) max_row_group_rows: u64,
    pub(crate) schema_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ArtifactFingerprint {
    pub(crate) path: PathBuf,
    pub(crate) size: u64,
    pub(crate) row_count: Option<u64>,
    pub(crate) sha256: String,
}

pub(crate) fn binary_identity() -> Result<String, Box<dyn std::error::Error>> {
    let executable = std::env::current_exe()?;
    let fingerprint = fingerprint_artifact(&executable)?;
    Ok(format!(
        "{}+sha256:{}",
        env!("CARGO_PKG_VERSION"),
        fingerprint.sha256
    ))
}

pub(crate) fn fingerprint_inputs(
    paths: &[PathBuf],
) -> Result<Vec<InputFingerprint>, Box<dyn std::error::Error>> {
    let conn = Connection::open_in_memory()?;
    let mut seen = HashSet::with_capacity(paths.len());
    let canonical_paths = paths
        .iter()
        .map(|path| {
            let canonical = path.canonicalize()?;
            if !seen.insert(canonical.clone()) {
                return Err(format!("duplicate Parquet input: {}", canonical.display()).into());
            }
            Ok(canonical)
        })
        .collect::<Result<Vec<PathBuf>, Box<dyn std::error::Error>>>()?;
    canonical_paths
        .iter()
        .enumerate()
        .map(|(file_id, path)| {
            let metadata = fs::metadata(path)?;
            let modified_unix_nanos = metadata
                .modified()?
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let input = parquet_sql_literal(path);
            let (row_count, row_group_count, min_row_group_rows, max_row_group_rows) = conn
                .query_row(
                    &format!(
                        "SELECT coalesce(sum(row_group_num_rows), 0)::UBIGINT,
                            count(*)::UBIGINT,
                            coalesce(min(row_group_num_rows), 0)::UBIGINT,
                            coalesce(max(row_group_num_rows), 0)::UBIGINT
                     FROM (
                         SELECT DISTINCT row_group_id, row_group_num_rows
                         FROM parquet_metadata({input})
                     ) groups"
                    ),
                    [],
                    |row| {
                        Ok((
                            row.get::<_, u64>(0)?,
                            row.get::<_, u64>(1)?,
                            row.get::<_, u64>(2)?,
                            row.get::<_, u64>(3)?,
                        ))
                    },
                )?;
            let mut statement =
                conn.prepare(&format!("DESCRIBE SELECT * FROM read_parquet({input})"))?;
            let mut schema = Vec::new();
            let columns = statement.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            for column in columns {
                let (name, data_type) = column?;
                schema.extend_from_slice(name.as_bytes());
                schema.push(0);
                schema.extend_from_slice(data_type.as_bytes());
                schema.push(0xff);
            }
            Ok(InputFingerprint {
                file_id: u32::try_from(file_id)
                    .map_err(|_| "Parquet input count exceeds u32 file IDs")?,
                path: path.clone(),
                size: metadata.len(),
                modified_unix_nanos,
                row_count,
                row_group_count,
                min_row_group_rows,
                max_row_group_rows,
                schema_sha256: sha256_hex(Sha256::digest(&schema).as_ref()),
            })
        })
        .collect()
}

pub(crate) fn row_group_parallelism_warning(
    inputs: &[InputFingerprint],
    effective_threads: usize,
) -> Option<String> {
    let row_group_count = inputs.iter().fold(0u64, |total, input| {
        total.saturating_add(input.row_group_count)
    });
    if row_group_count < u64::try_from(effective_threads).unwrap_or(u64::MAX) {
        return Some(format!(
            "only {row_group_count} Parquet row groups are available for {effective_threads} workers"
        ));
    }
    None
}

pub(crate) fn duckdb_threads_for_row_group_warning(effective_threads: usize) -> usize {
    effective_threads.clamp(1, DUCKDB_THREAD_CAP)
}

pub(crate) fn warn_for_suboptimal_row_groups(
    inputs: &[InputFingerprint],
    effective_threads: usize,
) {
    if let Some(warning) = row_group_parallelism_warning(inputs, effective_threads) {
        eprintln!("warning: {warning}");
    }
    for input in inputs {
        if input.row_group_count > 0
            && (input.min_row_group_rows < 100_000 || input.max_row_group_rows > 1_000_000)
        {
            eprintln!(
                "warning: {} row-group sizes range from {} to {} rows; about 100k-1m is preferred",
                input.path.display(),
                input.min_row_group_rows,
                input.max_row_group_rows
            );
        }
    }
}

pub(crate) fn fingerprint_artifact(
    path: &Path,
) -> Result<ArtifactFingerprint, Box<dyn std::error::Error>> {
    let canonical_path = path.canonicalize()?;
    let (size, sha256) = sha256_file(&canonical_path, 8 * 1024 * 1024)?;
    Ok(ArtifactFingerprint {
        path: canonical_path,
        size,
        row_count: artifact_row_count(path),
        sha256,
    })
}

pub(crate) fn artifact_row_count(path: &Path) -> Option<u64> {
    match path.extension().and_then(|extension| extension.to_str()) {
        Some("json") => fs::read(path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
            .and_then(|value| value["summary_rows"].as_array().map(std::vec::Vec::len))
            .and_then(|count| u64::try_from(count).ok()),
        Some("csv") => fs::read_to_string(path)
            .ok()
            .map(|text| text.lines().count().saturating_sub(1))
            .and_then(|count| u64::try_from(count).ok()),
        _ => None,
    }
}
