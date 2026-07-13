use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use duckdb::Connection;
use name_uri_analysis_rs::analysis::AnalysisOptions;
use name_uri_analysis_rs::write_json_atomically;
use serde::{Deserialize, Serialize};

use crate::controller_constants::{
    FINALIZER_STAGE_REVISION, METADATA_STAGE_REVISION, NAME_STAGE_REVISION, PREPARE_STAGE_REVISION,
};
use crate::controller_fingerprint::{fingerprint_artifact, ArtifactFingerprint, InputFingerprint};
use crate::InternalPhase;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PipelineManifest {
    pub(crate) schema_version: u32,
    pub(crate) binary_version: String,
    #[serde(default)]
    pub(crate) stage_revisions: StageRevisions,
    pub(crate) inputs: Vec<InputFingerprint>,
    pub(crate) chains: Vec<String>,
    pub(crate) options: AnalysisOptions,
    pub(crate) stages: BTreeMap<String, StageCheckpoint>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub(crate) struct StageRevisions {
    pub(crate) prepare: u32,
    pub(crate) name: u32,
    pub(crate) metadata: u32,
    pub(crate) finalizer: u32,
}

impl StageRevisions {
    pub(crate) const fn current() -> Self {
        Self {
            prepare: PREPARE_STAGE_REVISION,
            name: NAME_STAGE_REVISION,
            metadata: METADATA_STAGE_REVISION,
            finalizer: FINALIZER_STAGE_REVISION,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct StageCheckpoint {
    pub(crate) complete: bool,
    pub(crate) artifacts: Vec<ArtifactFingerprint>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PhaseReady {
    pub(crate) phase: String,
    pub(crate) partial_file: String,
    pub(crate) size: u64,
    pub(crate) sha256: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct PhaseMetric<'a> {
    pub(crate) phase: &'a str,
    pub(crate) wall_millis: u128,
    pub(crate) cpu_millis: u64,
    pub(crate) success: bool,
    pub(crate) input_rows: u64,
    pub(crate) summary_rows: u64,
    pub(crate) peak_rss_bytes: u64,
    pub(crate) peak_duckdb_temp_bytes: u64,
    pub(crate) io_read_bytes: u64,
    pub(crate) io_written_bytes: u64,
    pub(crate) database_bytes: u64,
    pub(crate) artifact_bytes: u64,
}

pub(crate) fn prepare_work_directory(
    work_directory: &Path,
    manifest: PipelineManifest,
    resume: bool,
) -> Result<(PathBuf, PipelineManifest), Box<dyn std::error::Error>> {
    let config_path = work_directory.join("manifest.json");
    if resume {
        let mut existing: PipelineManifest =
            serde_json::from_slice(&fs::read(&config_path).map_err(|error| {
                format!("cannot resume without {}: {error}", config_path.display())
            })?)?;
        if !manifests_have_same_inputs_and_options(&existing, &manifest) {
            return Err("resume rejected: input fingerprint or analysis options changed".into());
        }
        let metadata_recall_mode_changed =
            existing.options.metadata_recall_mode != manifest.options.metadata_recall_mode;
        if metadata_recall_mode_changed {
            invalidate_stage_checkpoints(&mut existing, &["metadata_complete", "finalized"]);
            remove_ready_checkpoints(work_directory, &["metadata"])?;
        }
        let revisions_changed = invalidate_changed_stage_revisions(
            &mut existing,
            manifest.stage_revisions,
            work_directory,
        )?;
        if metadata_recall_mode_changed
            || revisions_changed
            || existing.binary_version != manifest.binary_version
            || existing.options != manifest.options
        {
            existing.binary_version = manifest.binary_version;
            existing.options = manifest.options;
            write_manifest_atomically(&config_path, &existing)?;
        }
        return Ok((config_path, existing));
    }

    if work_directory.exists() && fs::read_dir(work_directory)?.next().is_some() {
        return Err(format!(
            "work directory {} is not empty; use --resume only for an identical run",
            work_directory.display()
        )
        .into());
    }
    fs::create_dir_all(work_directory.join("partial"))?;
    fs::create_dir_all(work_directory.join("duckdb-temp"))?;
    write_manifest_atomically(&config_path, &manifest)?;
    Ok((config_path, manifest))
}

pub(crate) fn invalidate_changed_stage_revisions(
    manifest: &mut PipelineManifest,
    expected: StageRevisions,
    work_directory: &Path,
) -> Result<bool, Box<dyn std::error::Error>> {
    let previous = manifest.stage_revisions;
    if previous == expected {
        return Ok(false);
    }

    if previous.prepare != expected.prepare {
        invalidate_stage_checkpoints(
            manifest,
            &[
                "contracts_ready",
                "uri_complete",
                "metadata_compact_ready",
                "prepare_complete",
                "name_complete",
                "metadata_complete",
                "finalized",
            ],
        );
        remove_ready_checkpoints(work_directory, &["prepare", "name", "metadata"])?;
    } else {
        if previous.name != expected.name {
            invalidate_stage_checkpoints(manifest, &["name_complete", "finalized"]);
            remove_ready_checkpoints(work_directory, &["name"])?;
        }
        if previous.metadata != expected.metadata {
            invalidate_stage_checkpoints(manifest, &["metadata_complete", "finalized"]);
            remove_ready_checkpoints(work_directory, &["metadata"])?;
        }
        if previous.finalizer != expected.finalizer {
            invalidate_stage_checkpoints(manifest, &["finalized"]);
        }
    }

    manifest.stage_revisions = expected;
    Ok(true)
}

pub(crate) fn invalidate_stage_checkpoints(manifest: &mut PipelineManifest, stages: &[&str]) {
    for stage in stages {
        manifest
            .stages
            .insert((*stage).to_string(), StageCheckpoint::default());
    }
}

pub(crate) fn remove_ready_checkpoints(
    work_directory: &Path,
    phases: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    for phase in phases {
        let path = work_directory
            .join("checkpoints")
            .join(format!("{phase}.ready.json"));
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(format!(
                    "could not invalidate stale ready checkpoint {}: {error}",
                    path.display()
                )
                .into());
            }
        }
    }
    Ok(())
}

pub(crate) fn initial_stage_checkpoints() -> BTreeMap<String, StageCheckpoint> {
    [
        ("input_validated", true),
        ("contracts_ready", false),
        ("uri_complete", false),
        ("metadata_compact_ready", false),
        ("prepare_complete", false),
        ("name_complete", false),
        ("metadata_complete", false),
        ("finalized", false),
    ]
    .into_iter()
    .map(|(name, complete)| {
        (
            name.to_string(),
            StageCheckpoint {
                complete,
                artifacts: Vec::new(),
            },
        )
    })
    .collect()
}

pub(crate) fn manifests_have_same_inputs_and_options(
    existing: &PipelineManifest,
    expected: &PipelineManifest,
) -> bool {
    existing.schema_version == expected.schema_version
        && existing.inputs == expected.inputs
        && existing.chains == expected.chains
        && existing.options.database_path == expected.options.database_path
        && existing.options.parquet_inputs == expected.options.parquet_inputs
        && existing.options.output_dir == expected.options.output_dir
        && existing.options.name_threshold == expected.options.name_threshold
}

pub(crate) fn checkpoint_is_complete_and_valid(
    manifest: &PipelineManifest,
    stage: &str,
    _work_directory: &Path,
) -> Result<bool, Box<dyn std::error::Error>> {
    let Some(checkpoint) = manifest.stages.get(stage) else {
        return Err(format!("manifest is missing required stage {stage:?}").into());
    };
    if !checkpoint.complete {
        return Ok(false);
    }
    for expected in &checkpoint.artifacts {
        let actual = fingerprint_artifact(&expected.path).map_err(|error| {
            format!(
                "resume rejected: artifact for stage {stage:?} is unavailable ({}): {error}",
                expected.path.display()
            )
        })?;
        if actual != *expected {
            return Err(format!(
                "resume rejected: artifact for stage {stage:?} changed: {}",
                expected.path.display()
            )
            .into());
        }
    }
    Ok(true)
}

pub(crate) fn validate_resume_database_for_downstream(
    manifest: &PipelineManifest,
    completed_stage: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let stage_complete = |stage: &str| {
        manifest
            .stages
            .get(stage)
            .is_some_and(|checkpoint| checkpoint.complete)
    };
    let required_tables: &[&str] = match completed_stage {
        "prepare_complete" if !stage_complete("name_complete") => &[
            "analysis_contracts",
            "metadata_rows",
            "metadata_contract_token_rows",
            "metadata_token_stats",
            "name_atoms",
            "selected_chains",
        ],
        "prepare_complete" if !stage_complete("metadata_complete") => &[
            "analysis_contracts",
            "metadata_rows",
            "metadata_contract_token_rows",
            "metadata_token_stats",
            "selected_chains",
        ],
        "name_complete" if !stage_complete("metadata_complete") => &[
            "analysis_contracts",
            "metadata_rows",
            "metadata_contract_token_rows",
            "metadata_token_stats",
            "selected_chains",
        ],
        _ => return Ok(()),
    };
    if !manifest.options.database_path.is_file() {
        return Err(format!(
            "resume rejected: {} is missing for incomplete downstream stages",
            manifest.options.database_path.display()
        )
        .into());
    }
    let conn = Connection::open(&manifest.options.database_path)?;
    for table in required_tables {
        let exists = conn.query_row(
            "SELECT count(*) > 0 FROM duckdb_tables() WHERE table_name = ?",
            [*table],
            |row| row.get::<_, bool>(0),
        )?;
        if !exists {
            return Err(format!(
                "resume rejected: stage database is missing required table {table:?}"
            )
            .into());
        }
    }
    Ok(())
}

pub(crate) fn mark_phase_complete(
    manifest: &mut PipelineManifest,
    phase: InternalPhase,
    artifact: ArtifactFingerprint,
) {
    let stages: &[&str] = match phase {
        InternalPhase::Prepare => &[
            "contracts_ready",
            "uri_complete",
            "metadata_compact_ready",
            "prepare_complete",
        ],
        InternalPhase::Name => &["name_complete"],
        InternalPhase::Metadata => &["metadata_complete"],
    };
    for stage in stages {
        manifest.stages.insert(
            (*stage).to_string(),
            StageCheckpoint {
                complete: true,
                artifacts: if *stage == "prepare_complete"
                    || *stage == "name_complete"
                    || *stage == "metadata_complete"
                {
                    vec![artifact.clone()]
                } else {
                    Vec::new()
                },
            },
        );
    }
}

pub(crate) fn promote_ready_phase(
    manifest: &mut PipelineManifest,
    phase: InternalPhase,
    expected_partial: &str,
    work_directory: &Path,
) -> Result<bool, Box<dyn std::error::Error>> {
    let phase_name = match phase {
        InternalPhase::Prepare => "prepare",
        InternalPhase::Name => "name",
        InternalPhase::Metadata => "metadata",
    };
    let ready_path = work_directory
        .join("checkpoints")
        .join(format!("{phase_name}.ready.json"));
    if !ready_path.is_file() {
        return Ok(false);
    }
    let ready: PhaseReady = serde_json::from_slice(&fs::read(&ready_path)?)?;
    if ready.phase != phase_name || ready.partial_file != expected_partial {
        return Err(format!(
            "resume rejected: malformed ready checkpoint {}",
            ready_path.display()
        )
        .into());
    }
    let artifact = fingerprint_artifact(&work_directory.join("partial").join(expected_partial))?;
    if artifact.size != ready.size || artifact.sha256 != ready.sha256 {
        return Err(format!(
            "resume rejected: ready checkpoint hash does not match {}",
            artifact.path.display()
        )
        .into());
    }
    mark_phase_complete(manifest, phase, artifact);
    Ok(true)
}

pub(crate) fn write_manifest_atomically(
    destination: &Path,
    manifest: &PipelineManifest,
) -> Result<(), Box<dyn std::error::Error>> {
    write_json_atomically(manifest, destination)?;
    Ok(())
}

pub(crate) fn write_metric_atomically(
    work_directory: &Path,
    metric: &PhaseMetric<'_>,
) -> Result<(), Box<dyn std::error::Error>> {
    let metrics_directory = work_directory.join("metrics");
    fs::create_dir_all(&metrics_directory)?;
    let destination = metrics_directory.join(format!("{}-phase.json", metric.phase));
    write_json_atomically(metric, &destination)?;
    Ok(())
}

pub(crate) fn record_phase_metric(work_directory: &Path, metric: &PhaseMetric<'_>) {
    if let Err(error) = write_metric_atomically(work_directory, metric) {
        eprintln!(
            "warning: could not persist {} phase metrics: {error}",
            metric.phase
        );
    }
}
