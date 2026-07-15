use std::collections::BTreeSet;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sysinfo::System;

use super::AnalysisError;
use crate::{sha256_hex, write_json_atomically};
use metadata_engine::pipeline::MetadataPipelineResult;

const READINESS_INPUT_SCHEMA_REVISION: u32 = 1;
const PRODUCTION_EVIDENCE_SCHEMA_REVISION: u32 = 2;
const OUTPUT_ADVISORY_DIRECTORY: &str = "advisory";
const OUTPUT_READINESS_INPUT_FILE: &str = "metadata-readiness-input.json";
const OUTPUT_READINESS_FILE: &str = "metadata-production-readiness.json";
const WORK_READINESS_INPUT_FILE: &str = "artifacts/metadata/readiness-input.json";
const WORK_READINESS_FILE: &str = "artifacts/metadata/production-readiness.json";
const EVIDENCE_FILE: &str = "production-evidence/metadata-v2.json";

struct MetadataReadinessPaths {
    input: PathBuf,
    readiness: PathBuf,
    evidence: PathBuf,
}

impl MetadataReadinessPaths {
    fn work(work_directory: &Path) -> Self {
        Self {
            input: work_directory.join(WORK_READINESS_INPUT_FILE),
            readiness: work_directory.join(WORK_READINESS_FILE),
            evidence: work_directory.join(EVIDENCE_FILE),
        }
    }

    fn output(output_directory: &Path) -> Self {
        let advisory = output_directory.join(OUTPUT_ADVISORY_DIRECTORY);
        Self {
            input: advisory.join(OUTPUT_READINESS_INPUT_FILE),
            readiness: advisory.join(OUTPUT_READINESS_FILE),
            evidence: output_directory.join(EVIDENCE_FILE),
        }
    }

    fn create_parent_directory(&self) -> Result<(), AnalysisError> {
        fs::create_dir_all(self.input.parent().ok_or_else(|| {
            AnalysisError::InvalidData("metadata readiness input has no parent directory".into())
        })?)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct MetadataReadinessInput {
    pub(crate) schema_revision: u32,
    pub(crate) binary_version: String,
    pub(crate) snapshot_fingerprint: String,
    pub(crate) engine_match_revision: u32,
    pub(crate) snapshot_atoms: u64,
    pub(crate) match_summary_sha256: String,
    pub(crate) semantic_ready: bool,
    pub(crate) observed_logical_cpus: usize,
    pub(crate) observed_memory_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct MetadataProductionEvidence {
    pub(crate) schema_revision: u32,
    pub(crate) binary_version: String,
    pub(crate) snapshot_fingerprint: String,
    pub(crate) engine_match_revision: u32,
    pub(crate) same_snapshot_differential_passed: bool,
    pub(crate) performance_gate_passed: bool,
    pub(crate) target_logical_cpus: usize,
    pub(crate) target_memory_bytes: u64,
    pub(crate) completed_tiers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct MetadataProductionReadiness {
    pub(crate) production_ready: bool,
    pub(crate) semantic_ready: bool,
    pub(crate) deployment_ready: bool,
    pub(crate) blockers: Vec<String>,
}

fn readiness_input_from_result(
    result: &MetadataPipelineResult,
) -> Result<MetadataReadinessInput, AnalysisError> {
    let summary = serde_json::to_vec(&result.summary_rows)?;
    let system = System::new_all();
    Ok(MetadataReadinessInput {
        schema_revision: READINESS_INPUT_SCHEMA_REVISION,
        binary_version: env!("CARGO_PKG_VERSION").into(),
        snapshot_fingerprint: result.snapshot_fingerprint.clone(),
        engine_match_revision: result.schema_revision,
        snapshot_atoms: result.snapshot_atoms,
        match_summary_sha256: sha256_hex(Sha256::digest(summary).as_ref()),
        semantic_ready: result.evidence_gate_report.passed,
        observed_logical_cpus: system.cpus().len(),
        observed_memory_bytes: system.total_memory(),
    })
}

fn validate_readiness_input(input: &MetadataReadinessInput) -> Result<(), AnalysisError> {
    if input.schema_revision != READINESS_INPUT_SCHEMA_REVISION {
        return Err(AnalysisError::InvalidData(format!(
            "unsupported metadata readiness input schema revision {}",
            input.schema_revision
        )));
    }
    if input.binary_version.is_empty()
        || input.snapshot_fingerprint.is_empty()
        || input.match_summary_sha256.len() != 64
        || input.observed_logical_cpus == 0
        || input.observed_memory_bytes == 0
    {
        return Err(AnalysisError::InvalidData(
            "metadata readiness input is incomplete".into(),
        ));
    }
    Ok(())
}

fn derive_metadata_production_readiness(
    input: &MetadataReadinessInput,
    evidence: Option<&MetadataProductionEvidence>,
) -> MetadataProductionReadiness {
    let mut blockers = Vec::new();
    if !input.semantic_ready {
        blockers.push("internal exact-evidence gate did not pass".to_string());
    }
    let deployment_ready = if let Some(evidence) = evidence {
        let tiers = evidence
            .completed_tiers
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let valid = evidence.schema_revision == PRODUCTION_EVIDENCE_SCHEMA_REVISION
            && evidence.binary_version == input.binary_version
            && evidence.snapshot_fingerprint == input.snapshot_fingerprint
            && evidence.engine_match_revision == input.engine_match_revision
            && evidence.same_snapshot_differential_passed
            && evidence.performance_gate_passed
            && evidence.target_logical_cpus >= 128
            && evidence.target_memory_bytes >= 512 * metadata_engine::resource::GIB
            && input.observed_logical_cpus >= 128
            && input.observed_memory_bytes >= 512 * metadata_engine::resource::GIB
            && ["1%", "10%", "full"]
                .into_iter()
                .all(|tier| tiers.contains(tier));
        if !valid {
            blockers.push(
                "target-host evidence is stale, incomplete, or not bound to this snapshot"
                    .to_string(),
            );
        }
        valid
    } else {
        blockers.push("target-host production evidence is missing".to_string());
        false
    };
    MetadataProductionReadiness {
        production_ready: input.semantic_ready && deployment_ready,
        semantic_ready: input.semantic_ready,
        deployment_ready,
        blockers,
    }
}

fn read_evidence(path: &Path) -> (Option<MetadataProductionEvidence>, Option<String>) {
    match fs::read(path) {
        Ok(bytes) => match serde_json::from_slice(&bytes) {
            Ok(evidence) => (Some(evidence), None),
            Err(error) => (None, Some(error.to_string())),
        },
        Err(error) if error.kind() == ErrorKind::NotFound => (None, None),
        Err(error) => (None, Some(error.to_string())),
    }
}

fn derive_with_current_evidence(
    input: &MetadataReadinessInput,
    evidence_path: &Path,
) -> MetadataProductionReadiness {
    let (evidence, evidence_error) = read_evidence(evidence_path);
    let mut readiness = derive_metadata_production_readiness(input, evidence.as_ref());
    if let Some(error) = evidence_error {
        readiness.production_ready = false;
        readiness.deployment_ready = false;
        readiness
            .blockers
            .retain(|blocker| blocker != "target-host production evidence is missing");
        readiness.blockers.push(format!(
            "target-host production evidence is invalid: {error}"
        ));
    }
    readiness
}

fn load_readiness_input(path: &Path) -> Result<MetadataReadinessInput, AnalysisError> {
    let input = serde_json::from_slice::<MetadataReadinessInput>(&fs::read(path)?)?;
    validate_readiness_input(&input)?;
    Ok(input)
}

fn write_current_readiness(
    paths: &MetadataReadinessPaths,
    input: &MetadataReadinessInput,
) -> Result<(), AnalysisError> {
    let readiness = derive_with_current_evidence(input, &paths.evidence);
    write_json_atomically(&readiness, &paths.readiness)?;
    Ok(())
}

/// Persist Match-owned facts in the work directory. The legacy work-directory
/// readiness remains useful while a run is in progress, but is never a gate.
pub(crate) fn write_metadata_production_readiness(
    work_directory: &Path,
    result: &MetadataPipelineResult,
) -> Result<(), AnalysisError> {
    let input = readiness_input_from_result(result)?;
    let paths = MetadataReadinessPaths::work(work_directory);
    paths.create_parent_directory()?;
    write_json_atomically(&input, &paths.input)?;
    write_current_readiness(&paths, &input)
}

/// Publish immutable Match facts and recompute the advisory from the latest
/// output-owned evidence. Callers intentionally treat errors as advisory.
pub(crate) fn publish_metadata_production_readiness(
    work_directory: &Path,
    output_directory: &Path,
) -> Result<(), AnalysisError> {
    let work_paths = MetadataReadinessPaths::work(work_directory);
    let output_paths = MetadataReadinessPaths::output(output_directory);
    let input = load_readiness_input(&work_paths.input)?;
    output_paths.create_parent_directory()?;
    write_json_atomically(&input, &output_paths.input)?;
    write_current_readiness(&output_paths, &input)
}

/// Recompute only the derived advisory. Missing or malformed evidence is a
/// valid not-ready result; missing or malformed immutable Match input is an
/// explicit refresh error.
pub fn refresh_metadata_production_readiness(output_directory: &Path) -> Result<(), AnalysisError> {
    let paths = MetadataReadinessPaths::output(output_directory);
    let input = load_readiness_input(&paths.input)?;
    write_current_readiness(&paths, &input)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input() -> MetadataReadinessInput {
        MetadataReadinessInput {
            schema_revision: READINESS_INPUT_SCHEMA_REVISION,
            binary_version: env!("CARGO_PKG_VERSION").into(),
            snapshot_fingerprint: "snapshot".into(),
            engine_match_revision: metadata_engine::scoring::MATCH_SEMANTICS_REVISION,
            snapshot_atoms: 42,
            match_summary_sha256: "a".repeat(64),
            semantic_ready: true,
            observed_logical_cpus: 128,
            observed_memory_bytes: 512 * metadata_engine::resource::GIB,
        }
    }

    fn evidence(input: &MetadataReadinessInput) -> MetadataProductionEvidence {
        MetadataProductionEvidence {
            schema_revision: PRODUCTION_EVIDENCE_SCHEMA_REVISION,
            binary_version: input.binary_version.clone(),
            snapshot_fingerprint: input.snapshot_fingerprint.clone(),
            engine_match_revision: input.engine_match_revision,
            same_snapshot_differential_passed: true,
            performance_gate_passed: true,
            target_logical_cpus: 128,
            target_memory_bytes: 512 * metadata_engine::resource::GIB,
            completed_tiers: vec!["1%".into(), "10%".into(), "full".into()],
        }
    }

    fn write_output_input(output: &Path, input: &MetadataReadinessInput) {
        let paths = MetadataReadinessPaths::output(output);
        paths.create_parent_directory().unwrap();
        write_json_atomically(input, &paths.input).unwrap();
    }

    #[test]
    fn missing_target_evidence_is_explicitly_not_production_ready() {
        let readiness = derive_metadata_production_readiness(&input(), None);
        assert!(!readiness.production_ready);
        assert!(readiness.semantic_ready);
        assert!(!readiness.deployment_ready);
        assert!(!readiness.blockers.is_empty());
    }

    #[test]
    fn target_evidence_must_match_immutable_match_input() {
        let input = input();
        let evidence = evidence(&input);
        assert!(derive_metadata_production_readiness(&input, Some(&evidence)).production_ready);

        let mut stale = evidence;
        stale.snapshot_fingerprint = "other-snapshot".into();
        assert!(!derive_metadata_production_readiness(&input, Some(&stale)).production_ready);
    }

    #[test]
    fn evidence_added_after_match_can_be_refreshed_without_rerunning_match() {
        let temp = tempfile::tempdir().unwrap();
        let input = input();
        write_output_input(temp.path(), &input);

        refresh_metadata_production_readiness(temp.path()).unwrap();
        let paths = MetadataReadinessPaths::output(temp.path());
        let initially: MetadataProductionReadiness =
            serde_json::from_slice(&fs::read(&paths.readiness).unwrap()).unwrap();
        assert!(!initially.production_ready);

        fs::create_dir_all(temp.path().join("production-evidence")).unwrap();
        write_json_atomically(&evidence(&input), &temp.path().join(EVIDENCE_FILE)).unwrap();
        refresh_metadata_production_readiness(temp.path()).unwrap();

        let refreshed: MetadataProductionReadiness =
            serde_json::from_slice(&fs::read(&paths.readiness).unwrap()).unwrap();
        assert!(refreshed.production_ready);
    }

    #[test]
    fn malformed_evidence_refreshes_to_an_advisory_blocker() {
        let temp = tempfile::tempdir().unwrap();
        write_output_input(temp.path(), &input());
        fs::create_dir_all(temp.path().join("production-evidence")).unwrap();
        fs::write(temp.path().join(EVIDENCE_FILE), b"{bad json").unwrap();

        refresh_metadata_production_readiness(temp.path()).unwrap();

        let paths = MetadataReadinessPaths::output(temp.path());
        let readiness: MetadataProductionReadiness =
            serde_json::from_slice(&fs::read(&paths.readiness).unwrap()).unwrap();
        assert!(!readiness.production_ready);
        assert!(readiness
            .blockers
            .iter()
            .any(|blocker| blocker.contains("invalid")));
    }

    #[test]
    fn explicit_refresh_rejects_missing_immutable_input() {
        let temp = tempfile::tempdir().unwrap();
        assert!(refresh_metadata_production_readiness(temp.path()).is_err());
    }
}
