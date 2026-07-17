use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use duckdb::Connection;
use name_uri_analysis_rs::analysis::{
    effective_memory_capacity_bytes, AnalysisOptions, MATCH_ETA_FORECAST_SCHEMA_VERSION,
};
use name_uri_analysis_rs::write_json_atomically;
use serde::{Deserialize, Serialize};
use sysinfo::System;

use crate::controller_constants::{
    FINALIZER_STAGE_REVISION, METADATA_ENCODE_STAGE_REVISION, METADATA_MATCH_STAGE_REVISION,
    NAME_STAGE_REVISION, PIPELINE_SCHEMA_VERSION, PREPARE_STAGE_REVISION,
};
use crate::controller_fingerprint::{
    fingerprint_artifact, fingerprint_artifact_for_expected, ArtifactFingerprint, InputFingerprint,
};
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
    pub(crate) metadata_encode: u32,
    pub(crate) metadata_match: u32,
    pub(crate) finalizer: u32,
}

impl StageRevisions {
    pub(crate) const fn current() -> Self {
        Self {
            prepare: PREPARE_STAGE_REVISION,
            name: NAME_STAGE_REVISION,
            metadata_encode: METADATA_ENCODE_STAGE_REVISION,
            metadata_match: METADATA_MATCH_STAGE_REVISION,
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
    #[serde(default)]
    pub(crate) artifacts: Vec<ArtifactFingerprint>,
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

const MATCH_OBSERVATION_SCHEMA_VERSION: u32 = MATCH_ETA_FORECAST_SCHEMA_VERSION;
const MATCH_SCALE_SCHEMA_VERSION: u32 = 3;
const MIN_MATCH_FORECAST_SAMPLES: usize = 8;
const MAX_MATCH_OBSERVATIONS_PER_PARTITION: usize = 256;
const MATCH_FORECAST_LOWER_PERCENTILE: usize = 20;
const MATCH_FORECAST_UPPER_PERCENTILE: usize = 80;
static OBSERVATION_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct MatchHardwareKey {
    architecture: String,
    cpu_brand: String,
    logical_cpus: usize,
    total_memory_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct MatchScaleKey {
    schema_version: u32,
    input_rows_bucket: u64,
    source_count_bucket: u64,
    atom_count_bucket: u64,
    token_membership_bytes_bucket: u64,
    fallback_membership_bytes_bucket: u64,
    payload_term_bytes_bucket: u64,
    block_membership_bytes_bucket: u64,
    token_pair_work_bucket: u64,
    max_token_members_bucket: u64,
    fallback_pair_work_bucket: u64,
    max_fallback_members_bucket: u64,
    block_pair_work_bucket: u64,
    contract_expansion_pair_work_bucket: u64,
    max_block_members_bucket: u64,
    evidence_pair_work_bucket: u64,
    rescue_pair_work_budget_bucket: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct MatchObservationKey {
    schema_version: u32,
    pub(crate) controller_match_revision: u32,
    engine_match_revision: u32,
    hardware: MatchHardwareKey,
    threads: usize,
    scale: MatchScaleKey,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MatchExecutionKind {
    Fresh,
    ResumeRecompute,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MatchOutcome {
    Success,
    Failure,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct MatchObservation {
    schema_version: u32,
    pub(crate) key: MatchObservationKey,
    pub(crate) execution: MatchExecutionKind,
    pub(crate) outcome: MatchOutcome,
    pub(crate) wall_millis: u64,
    pub(crate) sampled_peak_rss_bytes: u64,
    pub(crate) sampled_io_read_bytes: u64,
    pub(crate) sampled_io_written_bytes: u64,
    pub(crate) sample_interval_millis: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MatchSampledResources {
    pub(crate) peak_rss_bytes: u64,
    pub(crate) io_read_bytes: u64,
    pub(crate) io_written_bytes: u64,
    pub(crate) sample_interval_millis: u64,
}

impl MatchObservation {
    pub(crate) fn new(
        key: MatchObservationKey,
        execution: MatchExecutionKind,
        outcome: MatchOutcome,
        wall_millis: u64,
        resources: MatchSampledResources,
    ) -> Self {
        Self {
            schema_version: MATCH_OBSERVATION_SCHEMA_VERSION,
            key,
            execution,
            outcome,
            wall_millis,
            sampled_peak_rss_bytes: resources.peak_rss_bytes,
            sampled_io_read_bytes: resources.io_read_bytes,
            sampled_io_written_bytes: resources.io_written_bytes,
            sample_interval_millis: resources.sample_interval_millis,
        }
    }

    #[cfg(test)]
    pub(crate) fn for_test(
        key: MatchObservationKey,
        execution: MatchExecutionKind,
        outcome: MatchOutcome,
        wall_millis: u64,
    ) -> Self {
        Self::new(
            key,
            execution,
            outcome,
            wall_millis,
            MatchSampledResources {
                peak_rss_bytes: 0,
                io_read_bytes: 0,
                io_written_bytes: 0,
                sample_interval_millis: 200,
            },
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct MatchEtaForecast {
    pub(crate) schema_version: u32,
    pub(crate) sample_count: usize,
    pub(crate) lower_total_millis: Option<u64>,
    pub(crate) upper_total_millis: Option<u64>,
}

#[derive(Deserialize)]
struct FeatureScaleReady {
    source_count: u64,
    #[serde(default)]
    token_pair_work: u64,
    #[serde(default)]
    max_token_members: u64,
    #[serde(default)]
    fallback_pair_work: u64,
    #[serde(default)]
    max_fallback_members: u64,
}

#[derive(Deserialize)]
struct BlockingScaleReady {
    atom_count: u64,
    #[serde(default)]
    block_pair_work: u64,
    #[serde(default)]
    contract_expansion_pair_work: u64,
    #[serde(default)]
    max_block_members: u64,
}

pub(crate) fn match_observation_key(
    manifest: &PipelineManifest,
    work_directory: &Path,
) -> Result<MatchObservationKey, Box<dyn std::error::Error>> {
    let layout = metadata_engine::artifacts::MetadataArtifactLayout::new(work_directory);
    let feature_directory = layout.encode_dir();
    let blocking_directory = layout.blocking_dir();
    let feature: FeatureScaleReady =
        serde_json::from_slice(&fs::read(feature_directory.join("features.ready"))?)?;
    let blocking: BlockingScaleReady =
        serde_json::from_slice(&fs::read(blocking_directory.join("blocking.ready"))?)?;
    let input_rows = manifest
        .inputs
        .iter()
        .fold(0u64, |total, input| total.saturating_add(input.row_count));
    let system = System::new_all();
    let cpu_brand = system
        .cpus()
        .first()
        .map_or_else(String::new, |cpu| cpu.brand().to_string());
    let exact_plan = metadata_engine::exact_islands::plan_exact_evidence(
        blocking.atom_count,
        metadata_engine::pipeline::DEFAULT_EXACT_SAMPLE_LEFTS,
        metadata_engine::pipeline::DEFAULT_EXACT_PAIR_WORK,
    )?;
    let evidence_pair_work = exact_plan.pair_work.saturating_add(
        feature
            .token_pair_work
            .min(exact_plan.remaining_pair_work)
            .min(metadata_engine::exact_islands::SHARED_EXACT_TOTAL_PAIR_SAMPLE),
    );
    let base_pair_work = blocking
        .contract_expansion_pair_work
        .saturating_add(feature.fallback_pair_work);
    let rescue_pair_work_budget =
        metadata_engine::pipeline::DEFAULT_MAX_CANDIDATE_PAIR_VISITS.saturating_sub(base_pair_work);
    Ok(MatchObservationKey {
        schema_version: MATCH_OBSERVATION_SCHEMA_VERSION,
        controller_match_revision: METADATA_MATCH_STAGE_REVISION,
        engine_match_revision: metadata_engine::scoring::MATCH_SEMANTICS_REVISION,
        hardware: MatchHardwareKey {
            architecture: std::env::consts::ARCH.to_string(),
            cpu_brand,
            logical_cpus: std::thread::available_parallelism()
                .map(|value| value.get())
                .unwrap_or(1),
            total_memory_bytes: effective_memory_capacity_bytes(),
        },
        threads: std::env::var("NAME_URI_ANALYSIS_METADATA_MATCH_THREADS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|&value| value > 0)
            .unwrap_or(manifest.options.threads)
            .min(
                std::thread::available_parallelism()
                    .map(|value| value.get())
                    .unwrap_or(1),
            )
            .max(1),
        scale: MatchScaleKey {
            schema_version: MATCH_SCALE_SCHEMA_VERSION,
            input_rows_bucket: scale_bucket(input_rows),
            source_count_bucket: scale_bucket(feature.source_count),
            atom_count_bucket: scale_bucket(blocking.atom_count),
            token_membership_bytes_bucket: scale_bucket(file_len_or_zero(
                &feature_directory.join("token_member_contracts.u32"),
            )),
            fallback_membership_bytes_bucket: scale_bucket(file_len_or_zero(
                &feature_directory.join("fallback_atoms_members.u32"),
            )),
            payload_term_bytes_bucket: scale_bucket(
                file_len_or_zero(&feature_directory.join("payload_template_terms.u32"))
                    .saturating_add(file_len_or_zero(
                        &feature_directory.join("payload_content_terms.u32"),
                    )),
            ),
            block_membership_bytes_bucket: scale_bucket(
                file_len_or_zero(&blocking_directory.join("block_atoms.u32")).saturating_add(
                    file_len_or_zero(&blocking_directory.join("atom_block_ids.u32")),
                ),
            ),
            token_pair_work_bucket: scale_bucket(feature.token_pair_work),
            max_token_members_bucket: scale_bucket(feature.max_token_members),
            fallback_pair_work_bucket: scale_bucket(feature.fallback_pair_work),
            max_fallback_members_bucket: scale_bucket(feature.max_fallback_members),
            block_pair_work_bucket: scale_bucket(blocking.block_pair_work),
            contract_expansion_pair_work_bucket: scale_bucket(
                blocking.contract_expansion_pair_work,
            ),
            max_block_members_bucket: scale_bucket(blocking.max_block_members),
            evidence_pair_work_bucket: scale_bucket(evidence_pair_work),
            rescue_pair_work_budget_bucket: scale_bucket(rescue_pair_work_budget),
        },
    })
}

fn file_len_or_zero(path: &Path) -> u64 {
    fs::metadata(path).map_or(0, |metadata| metadata.len())
}

fn scale_bucket(value: u64) -> u64 {
    if value <= 1 {
        value
    } else {
        value.checked_next_power_of_two().unwrap_or(u64::MAX)
    }
}

fn match_history_root(output_directory: &Path) -> PathBuf {
    output_directory
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(".name-uri-analysis-history")
        .join("metadata-match-v3")
}

fn observation_partition(observation: &MatchObservation) -> &'static str {
    match (observation.execution, observation.outcome) {
        (MatchExecutionKind::Fresh, MatchOutcome::Success) => "fresh-success",
        (MatchExecutionKind::Fresh, MatchOutcome::Failure) => "fresh-failure",
        (MatchExecutionKind::ResumeRecompute, MatchOutcome::Success) => "resume-recompute-success",
        (MatchExecutionKind::ResumeRecompute, MatchOutcome::Failure) => "resume-recompute-failure",
    }
}

pub(crate) fn record_match_observation(
    output_directory: &Path,
    observation: &MatchObservation,
) -> Result<(), Box<dyn std::error::Error>> {
    let directory = match_history_root(output_directory).join(observation_partition(observation));
    fs::create_dir_all(&directory)?;
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let sequence = OBSERVATION_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let destination = directory.join(format!(
        "{nanos:039}-{:010}-{sequence:010}.json",
        std::process::id()
    ));
    write_json_atomically(observation, &destination)?;
    trim_observation_partition(&directory)?;
    Ok(())
}

fn trim_observation_partition(directory: &Path) -> std::io::Result<()> {
    let mut paths = fs::read_dir(directory)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|value| value.to_str()) == Some("json"))
        .collect::<Vec<_>>();
    if paths.len() <= MAX_MATCH_OBSERVATIONS_PER_PARTITION {
        return Ok(());
    }
    paths.sort_unstable();
    let remove_count = paths
        .len()
        .saturating_sub(MAX_MATCH_OBSERVATIONS_PER_PARTITION);
    for path in paths.into_iter().take(remove_count) {
        match fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

pub(crate) fn load_match_eta_forecast(
    output_directory: &Path,
    expected_key: &MatchObservationKey,
) -> Result<MatchEtaForecast, Box<dyn std::error::Error>> {
    let directory = match_history_root(output_directory).join("fresh-success");
    let mut wall_millis = Vec::new();
    if directory.is_dir() {
        for entry in fs::read_dir(directory)? {
            let path = entry?.path();
            if path.extension().and_then(|value| value.to_str()) != Some("json") {
                continue;
            }
            let Ok(bytes) = fs::read(path) else {
                continue;
            };
            let Ok(observation) = serde_json::from_slice::<MatchObservation>(&bytes) else {
                continue;
            };
            if observation.schema_version == MATCH_OBSERVATION_SCHEMA_VERSION
                && observation.execution == MatchExecutionKind::Fresh
                && observation.outcome == MatchOutcome::Success
                && &observation.key == expected_key
            {
                wall_millis.push(observation.wall_millis);
            }
        }
    }
    wall_millis.sort_unstable();
    let sample_count = wall_millis.len();
    let (lower_total_millis, upper_total_millis) = if sample_count >= MIN_MATCH_FORECAST_SAMPLES {
        (
            percentile(&wall_millis, MATCH_FORECAST_LOWER_PERCENTILE, false),
            percentile(&wall_millis, MATCH_FORECAST_UPPER_PERCENTILE, true),
        )
    } else {
        (None, None)
    };
    Ok(MatchEtaForecast {
        schema_version: MATCH_OBSERVATION_SCHEMA_VERSION,
        sample_count,
        lower_total_millis,
        upper_total_millis,
    })
}

fn percentile(sorted: &[u64], percentile: usize, round_up: bool) -> Option<u64> {
    let last = sorted.len().checked_sub(1)?;
    let numerator = last.saturating_mul(percentile.min(100));
    let index = if round_up {
        numerator.saturating_add(99) / 100
    } else {
        numerator / 100
    };
    sorted.get(index.min(last)).copied()
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
        validate_manifest_schema(&existing, &config_path)?;
        if !manifests_have_same_inputs_and_options(&existing, &manifest) {
            return Err("resume rejected: input fingerprint or analysis options changed".into());
        }
        let upgraded_legacy_input_fingerprint = existing
            .inputs
            .iter()
            .any(|input| input.content_sha256.is_empty());
        let revisions_changed = invalidate_changed_stage_revisions(
            &mut existing,
            manifest.stage_revisions,
            work_directory,
        )?;
        if revisions_changed
            || existing.binary_version != manifest.binary_version
            || existing.options != manifest.options
            || existing.inputs != manifest.inputs
        {
            if upgraded_legacy_input_fingerprint {
                eprintln!(
                    "warning: upgrading legacy resume manifest with full Parquet content hashes"
                );
            }
            existing.binary_version = manifest.binary_version;
            existing.inputs = manifest.inputs;
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

fn validate_manifest_schema(
    manifest: &PipelineManifest,
    config_path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    if manifest.schema_version != PIPELINE_SCHEMA_VERSION {
        return Err(format!(
            "resume rejected: pipeline schema version {} in {} is incompatible with {}; \
             re-run without --resume",
            manifest.schema_version,
            config_path.display(),
            PIPELINE_SCHEMA_VERSION
        )
        .into());
    }
    Ok(())
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

    // Durable storage pins follow checkpoint validity. Release them before
    // clearing manifest state so an Encode revision bump cannot permanently
    // reserve the previous bundle.
    if previous.prepare != expected.prepare || previous.metadata_encode != expected.metadata_encode
    {
        let mut broker = metadata_engine::storage::StorageBroker::open(work_directory)?;
        broker.release_checkpoint_pins("metadata_encode_complete")?;
        broker.release_checkpoint_pins("metadata_match_complete")?;
        broker.retire_checkpoint_artifacts("metadata_complete", "invalidated metadata revision")?;
    } else if previous.metadata_match != expected.metadata_match {
        let mut broker = metadata_engine::storage::StorageBroker::open(work_directory)?;
        broker.release_checkpoint_pins("metadata_match_complete")?;
        broker.retire_checkpoint_artifacts("metadata_complete", "invalidated metadata revision")?;
    }

    if previous.prepare != expected.prepare {
        invalidate_stage_checkpoints(
            manifest,
            &[
                "contracts_ready",
                "uri_complete",
                "metadata_compact_ready",
                "prepare_complete",
                "metadata_encode_complete",
                "name_complete",
                "metadata_match_complete",
                "finalized",
            ],
        );
        remove_ready_checkpoints(
            work_directory,
            &["prepare", "metadata-encode", "name", "metadata-match"],
        )?;
    } else {
        if previous.name != expected.name {
            invalidate_stage_checkpoints(manifest, &["name_complete", "finalized"]);
            remove_ready_checkpoints(work_directory, &["name"])?;
        }
        if previous.metadata_encode != expected.metadata_encode {
            // Encode is independent of Name: bumping encode must not clear name_complete.
            invalidate_stage_checkpoints(
                manifest,
                &[
                    "metadata_encode_complete",
                    "metadata_match_complete",
                    "finalized",
                ],
            );
            remove_ready_checkpoints(work_directory, &["metadata-encode", "metadata-match"])?;
        }
        if previous.metadata_match != expected.metadata_match {
            invalidate_stage_checkpoints(manifest, &["metadata_match_complete", "finalized"]);
            remove_ready_checkpoints(work_directory, &["metadata-match"])?;
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
        ("metadata_encode_complete", false),
        ("name_complete", false),
        ("metadata_match_complete", false),
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
        && input_fingerprints_are_compatible(&existing.inputs, &expected.inputs)
        && existing.chains == expected.chains
        && existing.options.database_path == expected.options.database_path
        && existing.options.parquet_inputs == expected.options.parquet_inputs
        && existing.options.output_dir == expected.options.output_dir
        && existing.options.name_threshold == expected.options.name_threshold
}

fn input_fingerprints_are_compatible(
    existing: &[InputFingerprint],
    expected: &[InputFingerprint],
) -> bool {
    existing.len() == expected.len()
        && existing.iter().zip(expected).all(|(left, right)| {
            left.file_id == right.file_id
                && left.path == right.path
                && left.size == right.size
                && left.modified_unix_nanos == right.modified_unix_nanos
                && left.row_count == right.row_count
                && left.row_group_count == right.row_group_count
                && left.min_row_group_rows == right.min_row_group_rows
                && left.max_row_group_rows == right.max_row_group_rows
                && left.schema_sha256 == right.schema_sha256
                && (left.content_sha256.is_empty() || left.content_sha256 == right.content_sha256)
        })
}

pub(crate) fn checkpoint_is_complete_and_valid(
    manifest: &PipelineManifest,
    stage: &str,
    work_directory: &Path,
) -> Result<bool, Box<dyn std::error::Error>> {
    let Some(checkpoint) = manifest.stages.get(stage) else {
        return Err(format!("manifest is missing required stage {stage:?}").into());
    };
    if !checkpoint.complete {
        return Ok(false);
    }
    let (artifact_root, artifact_root_label): (&Path, &str) = if stage == "finalized" {
        (manifest.options.output_dir.as_path(), "output directory")
    } else {
        (work_directory, "work directory")
    };
    let canonical_artifact_root = artifact_root.canonicalize()?;
    for expected in &checkpoint.artifacts {
        let canonical_artifact = expected.path.canonicalize().map_err(|error| {
            format!(
                "resume rejected: artifact for stage {stage:?} is unavailable ({}): {error}",
                expected.path.display()
            )
        })?;
        if !canonical_artifact.starts_with(&canonical_artifact_root) {
            return Err(format!(
                "resume rejected: artifact for stage {stage:?} is outside {artifact_root_label}: {}",
                canonical_artifact.display()
            )
            .into());
        }
        let actual = fingerprint_artifact_for_expected(&expected.path, &expected.sha256).map_err(
            |error| {
                format!(
                    "resume rejected: artifact for stage {stage:?} is unavailable ({}): {error}",
                    expected.path.display()
                )
            },
        )?;
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
            "metadata_token_dictionary",
            "name_atoms",
            "chain_totals",
            "selected_chains",
        ],
        "prepare_complete" if !stage_complete("metadata_encode_complete") => &[
            "analysis_contracts",
            "metadata_rows",
            "metadata_contract_token_rows",
            "metadata_token_stats",
            "metadata_token_dictionary",
            "chain_totals",
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
    artifacts: Vec<ArtifactFingerprint>,
) {
    let stages: &[&str] = match phase {
        InternalPhase::Prepare => &[
            "contracts_ready",
            "uri_complete",
            "metadata_compact_ready",
            "prepare_complete",
        ],
        InternalPhase::MetadataEncode => &["metadata_encode_complete"],
        InternalPhase::Name => &["name_complete"],
        InternalPhase::MetadataMatch => &["metadata_match_complete"],
    };
    for stage in stages {
        manifest.stages.insert(
            (*stage).to_string(),
            StageCheckpoint {
                complete: true,
                artifacts: if *stage == "prepare_complete"
                    || *stage == "metadata_encode_complete"
                    || *stage == "name_complete"
                    || *stage == "metadata_match_complete"
                {
                    artifacts.clone()
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
    let phase_name = phase.cli_name();
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
    if phase == InternalPhase::MetadataEncode && ready.artifacts.is_empty() {
        return Err(
            "resume rejected: metadata-encode ready checkpoint has no artifact dependencies".into(),
        );
    }
    let canonical_work = work_directory.canonicalize()?;
    let artifact = fingerprint_artifact(&work_directory.join("partial").join(expected_partial))?;
    if !artifact.path.starts_with(&canonical_work) {
        return Err(format!(
            "resume rejected: ready checkpoint partial is outside work directory: {}",
            artifact.path.display()
        )
        .into());
    }
    if artifact.size != ready.size || artifact.sha256 != ready.sha256 {
        return Err(format!(
            "resume rejected: ready checkpoint hash does not match {}",
            artifact.path.display()
        )
        .into());
    }
    let mut artifacts = vec![artifact];
    for expected in ready.artifacts {
        let canonical_artifact = expected.path.canonicalize().map_err(|error| {
            format!(
                "resume rejected: artifact dependency is missing ({}): {error}",
                expected.path.display()
            )
        })?;
        if !canonical_artifact.starts_with(&canonical_work) {
            return Err(format!(
                "resume rejected: artifact dependency is outside work directory: {}",
                canonical_artifact.display()
            )
            .into());
        }
        let actual = fingerprint_artifact_for_expected(&expected.path, &expected.sha256)?;
        if actual.size != expected.size || actual.sha256 != expected.sha256 {
            return Err(format!(
                "resume rejected: artifact dependency hash does not match {}",
                expected.path.display()
            )
            .into());
        }
        artifacts.push(actual);
    }
    mark_phase_complete(manifest, phase, artifacts);
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
