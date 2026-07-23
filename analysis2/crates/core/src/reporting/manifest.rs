//! Run manifest and failure log writers.

use std::fs;
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::Analysis2Error;

use super::json::DedupRunParams;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunManifestSeeds {
    pub selected: u64,
    pub analyzed: u64,
    pub failed: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunManifest {
    pub status: String,
    pub command: String,
    pub params: DedupRunParams,
    pub snapshot: Value,
    pub seeds: RunManifestSeeds,
    pub completeness: Value,
    pub pricing_policy: String,
    pub stage_timings: Value,
}

/// One recoverable failure (seed / stage); siblings continue.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailureRecord {
    pub seed_chain: String,
    pub seed_address: String,
    pub scope: String,
    pub stage: String,
    pub provider: String,
    pub retryable: bool,
    pub error: String,
}

impl FailureRecord {
    pub fn seed_stage(chain: &str, address: &str, stage: &str, error: impl Into<String>) -> Self {
        Self {
            seed_chain: chain.to_owned(),
            seed_address: address.to_owned(),
            scope: "seed".into(),
            stage: stage.to_owned(),
            provider: "local".into(),
            retryable: false,
            error: error.into(),
        }
    }

    pub fn candidate_stage(
        chain: &str,
        address: &str,
        stage: &str,
        error: impl Into<String>,
    ) -> Self {
        Self {
            seed_chain: chain.to_owned(),
            seed_address: address.to_owned(),
            scope: "candidate".into(),
            stage: stage.to_owned(),
            provider: "local".into(),
            retryable: false,
            error: error.into(),
        }
    }

    pub fn is_seed_scope(&self) -> bool {
        self.scope == "seed"
    }
}

/// Unique seeds that failed a seed-stage (resolve / dedup / incomplete seed path).
/// Candidate-stage rows in `failures.jsonl` do **not** inflate this count;
/// incomplete four-scope seeds are tracked separately via `incomplete_seed_count`.
pub fn count_failed_seeds(failures: &[FailureRecord]) -> u64 {
    let mut keys: Vec<(&str, &str)> = failures
        .iter()
        .filter(|f| f.is_seed_scope())
        .map(|f| (f.seed_chain.as_str(), f.seed_address.as_str()))
        .collect();
    keys.sort_unstable();
    keys.dedup();
    keys.len() as u64
}

pub fn write_failures_jsonl(path: &Path, failures: &[FailureRecord]) -> Result<(), Analysis2Error> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = fs::File::create(path)?;
    for fail in failures {
        let line = serde_json::to_string(fail)
            .map_err(|e| Analysis2Error::invalid(format!("failures jsonl encode: {e}")))?;
        writeln!(file, "{line}")?;
    }
    Ok(())
}
