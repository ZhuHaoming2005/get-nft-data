use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use duckdb::Connection;
use name_uri_analysis_rs::analysis::{parquet_sql_literal, DUCKDB_THREAD_CAP};
use name_uri_analysis_rs::{sha256_file, sha256_hex};
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const FINGERPRINT_HASH_BUFFER_BYTES: usize = 8 * 1024 * 1024;
// Full-file hashing is storage-bound. A small fixed cap keeps fast SSD/RAID
// devices busy without turning a 128-vCPU host into 128 competing file scans.
const MAX_FINGERPRINT_IO_LANES: usize = 8;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct InputFingerprint {
    pub(crate) file_id: u32,
    pub(crate) path: PathBuf,
    pub(crate) size: u64,
    pub(crate) modified_unix_nanos: u128,
    #[serde(default)]
    pub(crate) content_sha256: String,
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
    if paths.len() > u32::MAX as usize {
        return Err("Parquet input count exceeds u32 file IDs".into());
    }
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
    let lanes = fingerprint_input_lanes(
        canonical_paths.len(),
        std::thread::available_parallelism()
            .map(|value| value.get())
            .unwrap_or(1),
    );
    if lanes == 0 {
        return Ok(Vec::new());
    }

    let results = if lanes == 1 {
        let connection = open_fingerprint_connection()?;
        canonical_paths
            .iter()
            .enumerate()
            .map(|(file_id, path)| fingerprint_input(&connection, file_id, path))
            .collect()
    } else {
        let pool = ThreadPoolBuilder::new()
            .num_threads(lanes)
            .thread_name(|index| format!("input-fingerprint-{index}"))
            .build()?;
        pool.install(|| {
            canonical_paths
                .par_iter()
                .enumerate()
                .map_init(
                    open_fingerprint_connection,
                    |connection, (file_id, path)| match connection {
                        Ok(connection) => fingerprint_input(connection, file_id, path),
                        Err(error) => Err(error.clone()),
                    },
                )
                .collect::<Vec<_>>()
        })
    };

    // The indexed parallel iterator retains input order. Collecting Result only
    // after all tasks finish also makes the first reported failure deterministic.
    results
        .into_iter()
        .collect::<Result<Vec<_>, String>>()
        .map_err(Into::into)
}

fn fingerprint_input_lanes(file_count: usize, effective_threads: usize) -> usize {
    file_count
        .min(effective_threads.max(1))
        .min(MAX_FINGERPRINT_IO_LANES)
}

fn open_fingerprint_connection() -> Result<Connection, String> {
    let connection = Connection::open_in_memory().map_err(|error| error.to_string())?;
    connection
        .execute_batch("SET threads = 1")
        .map_err(|error| error.to_string())?;
    Ok(connection)
}

fn fingerprint_input(
    connection: &Connection,
    file_id: usize,
    path: &Path,
) -> Result<InputFingerprint, String> {
    let metadata = fs::metadata(path).map_err(|error| error.to_string())?;
    let modified_unix_nanos = metadata
        .modified()
        .map_err(|error| error.to_string())?
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let (hashed_size, content_sha256) =
        sha256_file(path, FINGERPRINT_HASH_BUFFER_BYTES).map_err(|error| error.to_string())?;
    if hashed_size != metadata.len() {
        return Err(format!(
            "Parquet input changed while fingerprinting: {}",
            path.display()
        ));
    }
    let input = parquet_sql_literal(path);
    let (row_count, row_group_count, min_row_group_rows, max_row_group_rows) = connection
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
        )
        .map_err(|error| error.to_string())?;
    let mut statement = connection
        .prepare(&format!("DESCRIBE SELECT * FROM read_parquet({input})"))
        .map_err(|error| error.to_string())?;
    let mut schema = Vec::new();
    let columns = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|error| error.to_string())?;
    for column in columns {
        let (name, data_type) = column.map_err(|error| error.to_string())?;
        schema.extend_from_slice(name.as_bytes());
        schema.push(0);
        schema.extend_from_slice(data_type.as_bytes());
        schema.push(0xff);
    }
    let metadata_after = fs::metadata(path).map_err(|error| error.to_string())?;
    let modified_after = metadata_after
        .modified()
        .map_err(|error| error.to_string())?
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    if metadata_after.len() != metadata.len() || modified_after != modified_unix_nanos {
        return Err(format!(
            "Parquet input changed while fingerprinting: {}",
            path.display()
        ));
    }
    Ok(InputFingerprint {
        file_id: u32::try_from(file_id)
            .map_err(|_| "Parquet input count exceeds u32 file IDs".to_string())?,
        path: path.to_path_buf(),
        size: metadata.len(),
        modified_unix_nanos,
        content_sha256,
        row_count,
        row_group_count,
        min_row_group_rows,
        max_row_group_rows,
        schema_sha256: sha256_hex(Sha256::digest(&schema).as_ref()),
    })
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
    let (size, sha256) = sha256_file(&canonical_path, FINGERPRINT_HASH_BUFFER_BYTES)?;
    Ok(ArtifactFingerprint {
        path: canonical_path,
        size,
        row_count: artifact_row_count(path),
        sha256,
    })
}

pub(crate) fn fingerprint_artifact_for_expected(
    path: &Path,
    expected_sha256: &str,
) -> Result<ArtifactFingerprint, Box<dyn std::error::Error>> {
    let Some(expected_checksum) =
        expected_sha256.strip_prefix(metadata_engine::format::TYPED_ARRAY_CHECKSUM_PREFIX)
    else {
        return fingerprint_artifact(path);
    };
    let canonical_path = path.canonicalize()?;
    let (size, checksum) =
        metadata_engine::format::verify_typed_array_fingerprint(&canonical_path)?;
    if checksum != expected_checksum {
        return Err(format!("typed-array footer changed: {}", canonical_path.display()).into());
    }
    Ok(ArtifactFingerprint {
        path: canonical_path,
        size,
        row_count: artifact_row_count(path),
        sha256: format!(
            "{}{}",
            metadata_engine::format::TYPED_ARRAY_CHECKSUM_PREFIX,
            checksum
        ),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_input_parallelism_is_bounded() {
        assert_eq!(fingerprint_input_lanes(0, 128), 0);
        assert_eq!(fingerprint_input_lanes(1, 128), 1);
        assert_eq!(fingerprint_input_lanes(4, 128), 4);
        assert_eq!(fingerprint_input_lanes(128, 128), MAX_FINGERPRINT_IO_LANES);
        assert_eq!(fingerprint_input_lanes(128, 3), 3);
        assert_eq!(fingerprint_input_lanes(128, 0), 1);
    }
}
