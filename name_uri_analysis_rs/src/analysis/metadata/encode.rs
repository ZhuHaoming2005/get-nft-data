//! MetadataEncode adapter: DuckDB stream → metadata_engine.
//!
//! Writes feature/blocking artifacts under `artifacts/metadata/`.
//! Never mutates Prepare/Name tables and never produces production summary rows.

use std::collections::HashMap;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use duckdb::arrow::array::{
    Array, Int64Array, StringArray, StringViewArray, UInt32Array, UInt64Array,
};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::types::Value;
use duckdb::Connection;
use metadata_engine::blocking::{
    build_base_equivalent_atom_sketches_from_soa_parallel,
    compile_base_equivalent_parallel_with_progress, AtomSketch, BlockingCompileConfig,
    BLOCKING_REVISION, DEFAULT_MAX_ROUTING_BLOCK_MEMBERS,
};
#[cfg(test)]
use metadata_engine::encode::PayloadArena;
use metadata_engine::encode::{
    metadata_has_prefilter_tokens, parse_metadata_documents,
    write_encode_artifacts_soa_with_progress, EncodeContractRow, EncodeContractSoA,
    EncodePayloadRow, EncodeSourceRow, EncodeSourceSoA, FallbackAtomCsr, ParsedMetadataDocuments,
    PayloadRef, PayloadTermListBatch, PayloadTermLists, PayloadTermSoA, ShardedPayloadArena,
    DEFAULT_ARENA_CHUNK_BYTES, DEFAULT_PAYLOAD_SHARD_COUNT, ENCODE_SCHEMA_REVISION,
};
use metadata_engine::format::commit_ready;
use metadata_engine::progress::{
    ProgressCounters as EngineCounters, ProgressEvent, ProgressPhase, WorkUnit,
};
use metadata_engine::resource::{MemoryBroker, MemoryLease};
use metadata_engine::storage::{ArtifactClass, ArtifactRegistration, StorageBroker};
use rayon::prelude::*;
use serde::Serialize;

use crate::{sha256_file, write_json_atomically};

use super::super::duckdb_prep::configure_duckdb;
use super::super::{
    diagnostics_enabled, encode_process_memory_plan, format_byte_size, physical_memory_bytes,
    total_memory_budget_bytes, AnalysisError, AnalysisOptions, AnalysisPhase, AnalysisReport,
    PipelineStage, ProgressTracker,
};
use super::prepare::metadata_is_dedup_eligible;

const ENCODE_RESIDENT_FIXED_BYTES: u64 = 64 * 1024 * 1024;
const HASH_BUCKET_OVERHEAD_BYTES: usize = 16;
/// Safety margin on distinct source JSON bytes for Arc/String overhead vs DuckDB
/// `metadata_max_json_bytes` (25%).
const TOKEN_JSON_ADMISSION_NUM: u64 = 5;
const TOKEN_JSON_ADMISSION_DEN: u64 = 4;
/// Bound unique-payload parse scratch: parse → intern → drop before the next
/// batch so arena bodies and ParsedMetadataDocuments never fully coexist.
const ENCODE_UNIQUE_PARSE_BATCH_LEN: usize = 4_096;

#[cfg(test)]
type FallbackEncodeRow = (u32, String, u32, u64);

/// Presence-only fallback selection: picks the first row per contract whose
/// JSON would yield a non-empty prefilter token list, without ever running
/// the full parse. Full parsing happens exactly once per unique payload,
/// after every contract's chosen source is known.
#[cfg(test)]
pub(super) fn resolve_fallback_contracts(
    rows: &[FallbackEncodeRow],
    has_prefilter_tokens: &(impl Fn(&str) -> bool + Sync),
) -> Vec<FallbackEncodeRow> {
    let mut ranges = Vec::new();
    let mut begin = 0usize;
    while begin < rows.len() {
        let contract = rows[begin].0;
        let mut end = begin + 1;
        while end < rows.len() && rows[end].0 == contract {
            end += 1;
        }
        ranges.push(begin..end);
        begin = end;
    }
    ranges
        .into_par_iter()
        .filter_map(|range| {
            rows[range]
                .iter()
                .find(|row| has_prefilter_tokens(&row.1))
                .cloned()
        })
        .collect()
}

#[derive(Serialize)]
struct EncodeMetrics {
    schema_version: u32,
    encode_wall_millis: u64,
    blocking_wall_millis: u64,
    source_rows: u64,
    payload_count: u64,
    contract_count: u64,
    atom_count: u64,
    template_term_count: u64,
    content_term_count: u64,
    token_membership_count: u64,
    routing_membership_count: u64,
    fallback_membership_count: u64,
    admitted_resident_peak_bytes: u64,
    admitted_final_bytes: u64,
    admitted_partial_peak_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct EncodeAdmissionEstimate {
    pub(super) final_bytes: u64,
    pub(super) provisional_feature_bytes: u64,
    pub(super) resident_peak_bytes: u64,
    pub(super) partial_peak_bytes: u64,
    pub(super) token_relation_peak_bytes: u64,
    pub(super) representative_rows: u64,
    pub(super) token_rows: u64,
}

struct EncodeResidentAdmission {
    lease: MemoryLease,
    baseline_bytes: u64,
    peak_bytes: u64,
}

impl EncodeResidentAdmission {
    fn new(lease: MemoryLease, baseline_bytes: u64) -> Self {
        let current = lease.bytes();
        Self {
            lease,
            baseline_bytes,
            peak_bytes: current,
        }
    }

    fn reserve_growth(
        &mut self,
        resident_bytes: u64,
        growth_bytes: u64,
    ) -> Result<(), AnalysisError> {
        let target = resident_bytes.checked_add(growth_bytes).ok_or_else(|| {
            AnalysisError::InvalidData("Encode live resident admission overflow".into())
        })?;
        self.set_current(target)
    }

    fn commit(&mut self, resident_bytes: u64) -> Result<(), AnalysisError> {
        self.set_current(resident_bytes)
    }

    fn set_current(&mut self, resident_bytes: u64) -> Result<(), AnalysisError> {
        let target = self.baseline_bytes.max(resident_bytes);
        self.lease.resize(target).map_err(|error| {
            AnalysisError::InvalidData(format!(
                "metadata encode live cardinality admission: {error}"
            ))
        })?;
        self.peak_bytes = self.peak_bytes.max(target);
        Ok(())
    }

    #[cfg(test)]
    fn current_bytes(&self) -> u64 {
        self.lease.bytes()
    }

    fn peak_bytes(&self) -> u64 {
        self.peak_bytes
    }
}

/// Resident-bytes accounting for the final source/payload/contract columns.
/// Kept as tested admission-model infrastructure (see `memory_dedup_tests`)
/// even though the streaming pipeline now only needs the one-shot
/// `frozen_encode_state_resident_bytes` snapshot at the very end.
#[allow(dead_code)]
#[derive(Default)]
struct EncodeResidentAccounting {
    observed_sources: usize,
    observed_payloads: usize,
    retained_token_capacity: u64,
    payload_term_capacity: u64,
}

impl EncodeResidentAccounting {
    // Vec capacity is the measured value; slices would discard the allocation
    // information this admission model exists to retain.
    #[allow(clippy::ptr_arg)]
    #[allow(clippy::too_many_arguments)]
    #[allow(dead_code)]
    fn resident_bytes(
        &mut self,
        sources: &Vec<EncodeSourceRow>,
        payloads: &Vec<EncodePayloadRow>,
        contracts: &Vec<EncodeContractRow>,
        payload_interner: Option<&PayloadTermInterner>,
        cas: Option<&ShardedPayloadArena>,
        pending_fallbacks: &HashMap<u32, PendingFallbackContract>,
    ) -> Result<u64, AnalysisError> {
        for source in &sources[self.observed_sources..] {
            self.retained_token_capacity = self
                .retained_token_capacity
                .checked_add(capacity_bytes::<u32>(source.retained_token_ids.capacity())?)
                .ok_or_else(|| {
                    AnalysisError::InvalidData("Encode source capacity overflow".into())
                })?;
        }
        self.observed_sources = sources.len();
        for payload in &payloads[self.observed_payloads..] {
            self.payload_term_capacity = self
                .payload_term_capacity
                .checked_add(capacity_bytes::<(u32, u32)>(
                    payload.template_terms.capacity(),
                )?)
                .and_then(|bytes| {
                    bytes.checked_add(
                        capacity_bytes::<(u32, u32)>(payload.content_terms.capacity()).ok()?,
                    )
                })
                .ok_or_else(|| {
                    AnalysisError::InvalidData("Encode payload term capacity overflow".into())
                })?;
        }
        self.observed_payloads = payloads.len();

        let mut total = ENCODE_RESIDENT_FIXED_BYTES;
        for bytes in [
            capacity_bytes::<EncodeSourceRow>(sources.capacity())?,
            self.retained_token_capacity,
            capacity_bytes::<EncodePayloadRow>(payloads.capacity())?,
            self.payload_term_capacity,
            capacity_bytes::<EncodeContractRow>(contracts.capacity())?,
            hash_map_capacity_bytes::<u32, PendingFallbackContract>(pending_fallbacks.capacity())?,
            payload_interner.map_or(0, PayloadTermInterner::resident_bytes),
            cas.map_or(0, ShardedPayloadArena::resident_bytes),
        ] {
            total = total.checked_add(bytes).ok_or_else(|| {
                AnalysisError::InvalidData("Encode resident accounting overflow".into())
            })?;
        }
        Ok(total)
    }
}

#[derive(Clone, Serialize)]
pub(super) struct EncodeChainTotal {
    name: String,
    contracts: i64,
    nfts: i64,
}

pub(crate) fn run_metadata_encode(
    options: &AnalysisOptions,
    work_directory: &Path,
) -> Result<(), AnalysisError> {
    let progress =
        ProgressTracker::for_pipeline_stage(PipelineStage::MetadataEncode, options.progress);
    let result: Result<(), AnalysisError> = (|| {
        progress.start_stage("metadata encode", 4);
        let artifact_layout =
            metadata_engine::artifacts::MetadataArtifactLayout::new(work_directory);
        // Reclaim crash leftovers before storage admission observes free space.
        artifact_layout.cleanup_stale_staging()?;
        let mut broker = StorageBroker::open(work_directory).map_err(storage_err)?;

        let conn = open_prepare_for_encode(options)?;
        progress.step_stage("opened Prepare DuckDB for isolated Encode");
        let estimate = estimate_encode_storage_bytes(&conn)?;
        // Admit the owned Encode builder before materializing row/token vectors.
        let host_total_memory = physical_memory_bytes();
        let memory_plan = encode_process_memory_plan(
            &options.duckdb_memory_limit,
            total_memory_budget_bytes(&options.memory_limit)?,
            estimate.resident_peak_bytes,
            host_total_memory,
        )?;
        conn.execute(
            &format!(
                "PRAGMA memory_limit='{}'",
                format_byte_size(memory_plan.duckdb_bytes as usize)
            ),
            [],
        )?;
        let memory_hard_top = memory_plan.rust_hard_top_bytes;
        let memory_broker =
            MemoryBroker::new(host_total_memory, memory_hard_top).map_err(|err| {
                AnalysisError::InvalidData(format!("metadata encode memory admission: {err}"))
            })?;
        let encode_memory_lease = memory_broker
            .reserve(estimate.resident_peak_bytes)
            .map_err(|err| {
                AnalysisError::InvalidData(format!("metadata encode memory admission: {err}"))
            })?;
        let mut resident_admission =
            EncodeResidentAdmission::new(encode_memory_lease, estimate.resident_peak_bytes);
        let storage_reservation = broker
            .reserve(
                ArtifactClass::Feature,
                estimate.provisional_feature_bytes,
                estimate.partial_peak_bytes,
            )
            .map_err(storage_err)?;

        let encode_started = Instant::now();
        let (sources, payloads, contracts, atoms, fallback_atoms, chain_totals) =
            stream_encode_inputs_with_admission(
                &conn,
                work_directory,
                &mut broker,
                &memory_broker,
                &mut resident_admission,
                options.threads,
                estimate,
                |event| progress.observe_engine_event(event),
            )?;
        // The in-memory token relation is released before feature persistence.
        // Replace the provisional storage reservation for the final arrays.
        drop(storage_reservation);
        let storage_reservation = broker
            .reserve(
                ArtifactClass::Feature,
                estimate.final_bytes,
                estimate.partial_peak_bytes,
            )
            .map_err(storage_err)?;
        let run_id = metadata_engine::artifacts::new_artifact_run_id();
        let encode_staging = artifact_layout.encode_run_staging_dir(&run_id);
        let blocking_staging = artifact_layout.blocking_run_staging_dir(&run_id);
        let _staging_cleanup = metadata_engine::artifacts::StagingCleanupGuard::new([
            encode_staging.clone(),
            blocking_staging.clone(),
        ]);
        let encode_dir = artifact_layout.encode_dir();
        let blocking_dir = artifact_layout.blocking_dir();
        if encode_staging.exists() {
            fs::remove_dir_all(&encode_staging)?;
        }
        if blocking_staging.exists() {
            fs::remove_dir_all(&blocking_staging)?;
        }
        fs::create_dir_all(&encode_staging)?;
        // Encode no longer publishes payload CAS. A rerun on an older
        // encode-N directory can still leave payload_blobs behind; delete
        // them before writing features so fingerprint/register cannot pin
        // stale packs as Feature artifacts.
        remove_stale_encode_payload_blobs(&encode_dir)?;
        let frozen_resident_bytes = frozen_encode_state_resident_bytes(
            &sources,
            &payloads,
            &contracts,
            &atoms,
            &fallback_atoms,
        )?;
        resident_admission.reserve_growth(
            frozen_resident_bytes,
            planned_feature_persist_growth(&sources, &payloads, &contracts)?,
        )?;
        let encode_persist_stats = write_encode_artifacts_soa_with_progress(
            &encode_staging,
            &sources,
            &payloads,
            &contracts,
            &fallback_atoms,
            |completed, total| {
                progress.observe_engine_event(ProgressEvent::determinate(
                    ProgressPhase::EncodePersist,
                    completed,
                    total,
                    WorkUnit::Bytes,
                    EngineCounters::default(),
                ));
            },
        )
        .map_err(encode_err)?;
        resident_admission.commit(frozen_resident_bytes)?;
        let encode_wall_millis = millis_since(encode_started);
        progress.step_stage(format!(
            "wrote encode features for {} sources / {} payloads",
            sources.source_count(),
            payloads.payload_count()
        ));

        let blocking_started = Instant::now();
        fs::create_dir_all(&blocking_staging)?;
        let config = BlockingCompileConfig {
            max_routing_block_members: DEFAULT_MAX_ROUTING_BLOCK_MEMBERS,
        };
        let blocking_bundle = compile_base_equivalent_parallel_with_progress(
            &atoms,
            &config,
            &blocking_staging,
            options.threads,
            |event| progress.observe_engine_event(event),
        )
        .map_err(blocking_err)?;
        let blocking_wall_millis = millis_since(blocking_started);
        progress.step_stage(format!(
            "compiled BaseEquivalent blocking for {} atoms",
            atoms.len()
        ));

        progress.observe_engine_event(ProgressEvent::indeterminate(
            ProgressPhase::EncodePublish,
            0,
            WorkUnit::Items,
            EngineCounters::default(),
        ));
        let encode_publish =
            metadata_engine::artifacts::publish_staged_bundle(&encode_staging, &encode_dir)
                .map_err(|error| {
                    AnalysisError::InvalidData(format!(
                        "publish encode staging to {}: {error}",
                        encode_dir.display()
                    ))
                })?;
        let blocking_publish =
            metadata_engine::artifacts::publish_staged_bundle(&blocking_staging, &blocking_dir)
                .map_err(|error| {
                    AnalysisError::InvalidData(format!(
                        "publish blocking staging to {}: {error}",
                        blocking_dir.display()
                    ))
                })?;
        let feature_manifest = serde_json::json!({
            "schema_revision": ENCODE_SCHEMA_REVISION,
            "artifact_run_id": &run_id,
            "source_count": sources.source_count(),
            "payload_count": payloads.payload_count(),
            "token_pair_work": encode_persist_stats.token_pair_work,
            "max_token_members": encode_persist_stats.max_token_members,
            "fallback_pair_work": encode_persist_stats.fallback_pair_work,
            "max_fallback_members": encode_persist_stats.max_fallback_members,
            "chains": chain_totals.iter().map(|total| &total.name).collect::<Vec<_>>(),
            "chain_totals": chain_totals,
        })
        .to_string();
        commit_ready(&encode_dir, "features.ready", &feature_manifest).map_err(format_err)?;
        let blocking_manifest = serde_json::json!({
            "blocking_revision": BLOCKING_REVISION,
            "artifact_run_id": &run_id,
            "atom_count": atoms.len(),
            "block_pair_work": blocking_bundle.block_stats.bucket_pair_work,
            "contract_expansion_pair_work": blocking_contract_expansion_pair_work(
                &blocking_bundle,
                &fallback_atoms,
            )?,
            "max_block_members": blocking_bundle.block_stats.smax,
        })
        .to_string();
        commit_ready(&blocking_dir, "blocking.ready", &blocking_manifest).map_err(format_err)?;
        blocking_publish.finalize()?;
        encode_publish.finalize()?;
        progress.observe_engine_event(ProgressEvent::indeterminate(
            ProgressPhase::EncodePublish,
            1,
            WorkUnit::Items,
            EngineCounters::default(),
        ));
        let artifact_fingerprints = fingerprint_bundle_files(&[&encode_dir, &blocking_dir])?;
        // Payload CAS is not an Encode output. Defensively keep leftover
        // payload_blobs paths out of the ready marker and registration set.
        let checkpoint_artifact_fingerprints = artifact_fingerprints
            .iter()
            .filter(|artifact| !path_is_under_payload_blobs(&artifact.path))
            .cloned()
            .collect();
        progress.observe_engine_event(ProgressEvent::indeterminate(
            ProgressPhase::EncodePublish,
            2,
            WorkUnit::Items,
            EngineCounters::default(),
        ));
        drop(storage_reservation);

        let blocking_root = blocking_dir.canonicalize()?;
        let mut registrations = Vec::new();
        let mut registered = Vec::new();
        for path in artifact_fingerprints
            .iter()
            .map(|artifact| &artifact.path)
            .filter(|path| !path_is_under_payload_blobs(path))
        {
            let class = if path.starts_with(&blocking_root) {
                ArtifactClass::Blocking
            } else {
                ArtifactClass::Feature
            };
            registrations.push(ArtifactRegistration::new(
                path.clone(),
                class,
                fs::metadata(path)?.len(),
                0,
                Vec::new(),
            ));
            registered.push(path.clone());
        }
        broker.register_batch(registrations).map_err(storage_err)?;
        let checkpoint_pins = broker
            .pin_batch(&registered, "metadata_encode_complete")
            .map_err(storage_err)?;
        progress.observe_engine_event(ProgressEvent::indeterminate(
            ProgressPhase::EncodePublish,
            3,
            WorkUnit::Items,
            EngineCounters::default(),
        ));

        if diagnostics_enabled() {
            let metrics = EncodeMetrics {
                schema_version: 3,
                encode_wall_millis,
                blocking_wall_millis,
                source_rows: sources.source_count() as u64,
                payload_count: payloads.payload_count() as u64,
                contract_count: contracts.contract_count() as u64,
                atom_count: atoms.len() as u64,
                template_term_count: payloads.template_terms.len() as u64,
                content_term_count: payloads.content_terms.len() as u64,
                token_membership_count: sources.token_ids.len() as u64,
                routing_membership_count: atoms
                    .iter()
                    .map(|atom| {
                        atom.template_anchors.len() as u64 + atom.content_anchors.len() as u64
                    })
                    .sum(),
                fallback_membership_count: fallback_atoms.members.len() as u64,
                admitted_resident_peak_bytes: resident_admission.peak_bytes(),
                admitted_final_bytes: estimate.final_bytes,
                admitted_partial_peak_bytes: estimate.partial_peak_bytes,
            };
            let metrics_dir = work_directory.join("metrics");
            fs::create_dir_all(&metrics_dir)?;
            let _ = write_json_atomically(&metrics, &metrics_dir.join("metadata-encode.json"));
        }

        let partial_dir = work_directory.join("partial");
        fs::create_dir_all(&partial_dir)?;
        write_json_atomically(
            &AnalysisReport {
                summary_rows: Vec::new(),
            },
            &partial_dir.join(AnalysisPhase::MetadataEncode.partial_file_name()),
        )?;
        write_phase_ready_marker(work_directory, checkpoint_artifact_fingerprints)?;
        for pin in checkpoint_pins {
            pin.persist().map_err(storage_err)?;
        }
        progress.finish_stage("metadata encode complete");
        progress.finish_pipeline_stage("metadata encode complete");
        progress.finish_display("metadata encode phase complete");
        Ok(())
    })();
    if let Err(error) = &result {
        progress.fail(error.to_string());
    }
    result
}

pub(super) fn open_prepare_for_encode(
    options: &AnalysisOptions,
) -> Result<Connection, AnalysisError> {
    let conn = Connection::open(&options.database_path)?;
    configure_duckdb(&conn, options)?;
    Ok(conn)
}

type EncodeStreamInputs = (
    EncodeSourceSoA,
    PayloadTermSoA,
    EncodeContractSoA,
    Vec<AtomSketch>,
    FallbackAtomCsr,
    Vec<EncodeChainTotal>,
);

#[derive(Debug)]
struct TokenSourceInput {
    token_ids: Vec<u32>,
    source_file: u32,
    source_row_number: u64,
    payload_ref: PayloadRef,
}

#[derive(Debug)]
struct TokenSourceRecord {
    source_file: u32,
    source_row_number: u64,
    payload_ref: PayloadRef,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct SourceCoordinate {
    source_file: u32,
    source_row_number: u64,
}

#[derive(Debug, Clone, Copy)]
struct SelectedTokenSource {
    contract_index: u32,
    token_index: u32,
    coordinate: SourceCoordinate,
    payload_ref: PayloadRef,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ResolvedTokenMembership {
    contract_index: u32,
    source_id: u32,
    token_id: u32,
}

struct TokenSourceRelation {
    sources: Vec<TokenSourceRecord>,
    memberships: Vec<ResolvedTokenMembership>,
    contract_offsets: Vec<usize>,
    logical_bytes: u64,
}

impl TokenSourceRelation {
    fn read_contract(&self, contract_index: u32) -> Result<Vec<TokenSourceInput>, AnalysisError> {
        let contract = usize::try_from(contract_index).map_err(|_| {
            AnalysisError::InvalidData("metadata contract index exceeds usize".into())
        })?;
        let Some((&start, &end)) = self
            .contract_offsets
            .get(contract)
            .zip(self.contract_offsets.get(contract.saturating_add(1)))
        else {
            return Err(AnalysisError::InvalidData(
                "metadata contract index is outside token-source offsets".into(),
            ));
        };
        let mut output = Vec::new();
        let mut cursor = start;
        while cursor < end {
            let source_id = self.memberships[cursor].source_id;
            let source = self.sources.get(source_id as usize).ok_or_else(|| {
                AnalysisError::InvalidData(
                    "token-source membership references unknown source".into(),
                )
            })?;
            let mut token_ids = Vec::new();
            while cursor < end && self.memberships[cursor].source_id == source_id {
                token_ids.push(self.memberships[cursor].token_id);
                cursor += 1;
            }
            output.push(TokenSourceInput {
                token_ids,
                source_file: source.source_file,
                source_row_number: source.source_row_number,
                payload_ref: source.payload_ref,
            });
        }
        Ok(output)
    }

    fn bytes(&self) -> u64 {
        self.logical_bytes
    }
}

fn planned_token_relation_peak(
    token_rows: u64,
    representative_rows: u64,
    distinct_token_json_bytes: u64,
) -> Result<u64, AnalysisError> {
    // JSON bodies are inserted into the payload arena once during catalog
    // load; the relation itself only retains coords + PayloadRef.
    let admitted_json_bytes = distinct_token_json_bytes
        .checked_mul(TOKEN_JSON_ADMISSION_NUM)
        .map(|bytes| bytes / TOKEN_JSON_ADMISSION_DEN)
        .ok_or_else(|| AnalysisError::InvalidData("token-source JSON admission overflow".into()))?;
    // Peak construction keeps the selected rows, coordinate/payload sort
    // copy, source records, source-id hash table, and memberships alive at
    // once. 192 bytes/retained token conservatively includes Vec spare
    // capacity, hash buckets, and alignment; the returned relation is smaller.
    token_rows
        .checked_mul(192)
        .and_then(|bytes| representative_rows.checked_mul(8)?.checked_add(bytes))
        .and_then(|bytes| admitted_json_bytes.checked_add(bytes))
        .and_then(|bytes| bytes.checked_add(64 * 1024 * 1024))
        .ok_or_else(|| AnalysisError::InvalidData("token-source relation estimate overflow".into()))
}

fn capacity_bytes<T>(capacity: usize) -> Result<u64, AnalysisError> {
    let bytes = capacity
        .checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| AnalysisError::InvalidData("Encode capacity overflow".into()))?;
    u64::try_from(bytes)
        .map_err(|_| AnalysisError::InvalidData("Encode capacity exceeds u64".into()))
}

fn hash_map_capacity_bytes<K, V>(capacity: usize) -> Result<u64, AnalysisError> {
    let bucket_bytes = std::mem::size_of::<K>()
        .checked_add(std::mem::size_of::<V>())
        .and_then(|bytes| bytes.checked_add(HASH_BUCKET_OVERHEAD_BYTES))
        .ok_or_else(|| AnalysisError::InvalidData("Encode hash bucket overflow".into()))?;
    let bytes = capacity
        .checked_mul(bucket_bytes)
        .ok_or_else(|| AnalysisError::InvalidData("Encode hash capacity overflow".into()))?;
    u64::try_from(bytes)
        .map_err(|_| AnalysisError::InvalidData("Encode hash capacity exceeds u64".into()))
}

fn planned_encode_batch_growth(json_bytes: u64, row_count: usize) -> Result<u64, AnalysisError> {
    json_bytes
        .checked_mul(16)
        .and_then(|bytes| {
            u64::try_from(row_count)
                .ok()?
                .checked_mul(2_048)?
                .checked_add(bytes)
        })
        .ok_or_else(|| AnalysisError::InvalidData("Encode batch admission overflow".into()))
}

/// Temporary ParsedMetadataDocuments / rayon scratch for one unique-parse
/// batch. Arena bodies remain resident until every batch finishes, so this
/// bound must cover parsed Strings and token Vecs without assuming the whole
/// unique set is parsed at once.
fn planned_unique_parse_batch_growth(
    batch_json_bytes: u64,
    batch_len: usize,
) -> Result<u64, AnalysisError> {
    batch_json_bytes
        .checked_mul(32)
        .and_then(|bytes| {
            u64::try_from(batch_len)
                .ok()?
                .checked_mul(8_192)?
                .checked_add(bytes)
        })
        .ok_or_else(|| {
            AnalysisError::InvalidData("Encode unique parse batch admission overflow".into())
        })
}

#[allow(dead_code)]
fn planned_encoded_contract_growth(
    representative_json: &str,
    token_sources: &[TokenSourceInput],
) -> Result<u64, AnalysisError> {
    let json_bytes = u64::try_from(representative_json.len())
        .map_err(|_| AnalysisError::InvalidData("Encode representative JSON exceeds u64".into()))?;
    // Token-source JSON already lives in the arena; only account for slot overhead.
    let membership_count = token_sources.iter().try_fold(0u64, |total, source| {
        total.checked_add(source.token_ids.len() as u64)
    });
    let source_count = u64::try_from(token_sources.len())
        .ok()
        .and_then(|count| count.checked_add(1));
    json_bytes
        .checked_mul(16)
        .and_then(|bytes| source_count?.checked_mul(2_048)?.checked_add(bytes))
        .and_then(|bytes| membership_count?.checked_mul(8)?.checked_add(bytes))
        .ok_or_else(|| AnalysisError::InvalidData("Encode contract admission overflow".into()))
}

fn planned_encode_finalize_growth(
    payload_count: usize,
    contract_count: usize,
) -> Result<u64, AnalysisError> {
    let mut total = ENCODE_RESIDENT_FIXED_BYTES;
    for bytes in [
        capacity_bytes::<u32>(payload_count)?,
        capacity_bytes::<EncodePayloadRow>(payload_count)?,
        // Conservative term-slot headroom while the interner is still live.
        capacity_bytes::<(u32, u32)>(payload_count.saturating_mul(32))?,
        hash_map_capacity_bytes::<Arc<str>, u32>(payload_count)?,
        hash_map_capacity_bytes::<String, u32>(payload_count)?,
        hash_map_capacity_bytes::<(u32, u32), usize>(contract_count)?,
        capacity_bytes::<u32>(contract_count)?,
        capacity_bytes::<Vec<u32>>(contract_count)?,
        capacity_bytes::<(&'static [u32], &'static [u32])>(contract_count)?,
    ] {
        total = total.checked_add(bytes).ok_or_else(|| {
            AnalysisError::InvalidData("Encode finalize admission overflow".into())
        })?;
    }
    Ok(total)
}

// Vec capacity is intentionally part of the frozen resident model.
#[allow(clippy::ptr_arg)]
fn frozen_encode_state_resident_bytes(
    sources: &EncodeSourceSoA,
    payloads: &PayloadTermSoA,
    contracts: &EncodeContractSoA,
    atoms: &Vec<AtomSketch>,
    fallback_atoms: &FallbackAtomCsr,
) -> Result<u64, AnalysisError> {
    let mut total = ENCODE_RESIDENT_FIXED_BYTES;
    for bytes in [
        capacity_bytes::<u32>(sources.contract_ids.capacity())?,
        capacity_bytes::<u32>(sources.payload_ids.capacity())?,
        capacity_bytes::<u64>(sources.token_offsets.capacity())?,
        capacity_bytes::<u32>(sources.token_ids.capacity())?,
        capacity_bytes::<u64>(payloads.template_offsets.capacity())?,
        capacity_bytes::<u32>(payloads.template_terms.capacity())?,
        capacity_bytes::<u32>(payloads.template_freqs.capacity())?,
        capacity_bytes::<u64>(payloads.content_offsets.capacity())?,
        capacity_bytes::<u32>(payloads.content_terms.capacity())?,
        capacity_bytes::<u32>(payloads.content_freqs.capacity())?,
        capacity_bytes::<u32>(contracts.contract_ids.capacity())?,
        capacity_bytes::<u32>(contracts.chain_ids.capacity())?,
        capacity_bytes::<u32>(contracts.source_doc_ids.capacity())?,
        capacity_bytes::<u32>(contracts.payload_ids.capacity())?,
        capacity_bytes::<u64>(contracts.weights.capacity())?,
        capacity_bytes::<AtomSketch>(atoms.capacity())?,
        capacity_bytes::<u64>(fallback_atoms.offsets.capacity())?,
        capacity_bytes::<u32>(fallback_atoms.members.capacity())?,
        capacity_bytes::<u32>(fallback_atoms.atom_payloads.capacity())?,
    ] {
        total = total.checked_add(bytes).ok_or_else(|| {
            AnalysisError::InvalidData("Encode frozen resident accounting overflow".into())
        })?;
    }
    for atom in atoms {
        for capacity in [
            atom.template_anchors.capacity(),
            atom.content_anchors.capacity(),
        ] {
            total = total
                .checked_add(capacity_bytes::<u32>(capacity)?)
                .ok_or_else(|| {
                    AnalysisError::InvalidData("Encode atom accounting overflow".into())
                })?;
        }
    }
    Ok(total)
}

fn planned_feature_persist_growth(
    sources: &EncodeSourceSoA,
    payloads: &PayloadTermSoA,
    contracts: &EncodeContractSoA,
) -> Result<u64, AnalysisError> {
    let occurrences = u64::try_from(sources.token_ids.len()).map_err(|_| {
        AnalysisError::InvalidData("Encode CSR occurrence count exceeds u64".into())
    })?;
    let max_token = sources.token_ids.iter().copied().max();
    let token_count = max_token.map_or(0u64, |token| u64::from(token) + 1);
    let contract_count = u64::try_from(contracts.contract_count())
        .map_err(|_| AnalysisError::InvalidData("Encode contract count exceeds u64".into()))?;
    let source_count = u64::try_from(sources.source_count())
        .map_err(|_| AnalysisError::InvalidData("Encode source count exceeds u64".into()))?;
    let template_terms = Some(payloads.template_terms.len() as u64);
    let payload_count = u64::try_from(payloads.payload_count())
        .map_err(|_| AnalysisError::InvalidData("Encode payload count exceeds u64".into()))?;
    occurrences
        .checked_mul(32)
        .and_then(|bytes| {
            contract_count
                .checked_add(token_count)?
                .checked_mul(40)?
                .checked_add(bytes)
        })
        .and_then(|bytes| source_count.checked_mul(32)?.checked_add(bytes))
        .and_then(|bytes| template_terms?.checked_mul(8)?.checked_add(bytes))
        .and_then(|bytes| payload_count.checked_mul(96)?.checked_add(bytes))
        .and_then(|bytes| bytes.checked_add(ENCODE_RESIDENT_FIXED_BYTES))
        .ok_or_else(|| AnalysisError::InvalidData("Encode CSR admission overflow".into()))
}

fn token_source_relation_dimensions(conn: &Connection) -> Result<(u64, u64), AnalysisError> {
    let table_exists: bool = conn.query_row(
        "SELECT count(*) > 0
         FROM duckdb_tables() WHERE table_name = 'metadata_contract_token_rows'",
        [],
        |row| row.get(0),
    )?;
    if !table_exists {
        return Ok((0, 0));
    }
    conn.query_row(
        "SELECT count(*)::UBIGINT,
                coalesce((
                    SELECT sum(source_json_bytes)::UBIGINT
                    FROM (
                        SELECT max(metadata_max_json_bytes)::UBIGINT AS source_json_bytes
                        FROM metadata_contract_token_rows
                        GROUP BY metadata_source_file, metadata_source_row_number
                    ) distinct_sources
                ), 0)::UBIGINT
         FROM metadata_contract_token_rows",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .map_err(AnalysisError::from)
}

#[derive(Debug)]
struct PendingFallbackContract {
    source_contract_index: u32,
    chain_id: u32,
    weight: u64,
}

/// One retained token-specific metadata source registered into the
/// [`ShardedPayloadArena`] on behalf of a pending contract.
#[derive(Debug)]
struct PendingSourceSlot {
    source_file: u32,
    source_row_number: u64,
    payload_ref: PayloadRef,
    token_ids: Vec<u32>,
}

/// A contract whose representative payload (and retained token sources) has
/// been registered in the arena, but whose full parse / term interning has
/// not happened yet. Built once per contract during the presence-only
/// registration pass, then consumed in original contract order to build the
/// final Encode columns after global payload IDs are assigned.
#[derive(Debug)]
struct PendingContractSlot {
    chain_id: u32,
    weight: u64,
    representative_file: u32,
    representative_row: u64,
    representative_payload_ref: PayloadRef,
    token_sources: Vec<PendingSourceSlot>,
}

/// Final contract slot after shard-local refs are remapped to dense global IDs.
#[derive(Debug)]
struct GlobalPendingContractSlot {
    chain_id: u32,
    weight: u64,
    representative_file: u32,
    representative_row: u64,
    representative_payload_id: u32,
    token_sources: Vec<GlobalPendingSourceSlot>,
}

#[derive(Debug)]
struct GlobalPendingSourceSlot {
    source_file: u32,
    source_row_number: u64,
    payload_id: u32,
    token_ids: Vec<u32>,
}

/// Resident accounting for the presence-only registration pass (before any
/// payload is parsed or interned). Tracked at Arrow batch boundaries so the
/// live memory lease grows without a per-contract `MemoryLease::resize`.
#[derive(Default)]
struct EncodeRegistrationAccounting {
    observed_contracts: usize,
    token_source_capacity: u64,
}

impl EncodeRegistrationAccounting {
    #[allow(clippy::ptr_arg)]
    fn resident_bytes(
        &mut self,
        pending_contracts: &Vec<PendingContractSlot>,
        pending_fallbacks: &HashMap<u32, PendingFallbackContract>,
        arena: &ShardedPayloadArena,
    ) -> Result<u64, AnalysisError> {
        for contract in &pending_contracts[self.observed_contracts..] {
            self.token_source_capacity = self
                .token_source_capacity
                .checked_add(capacity_bytes::<PendingSourceSlot>(
                    contract.token_sources.capacity(),
                )?)
                .ok_or_else(|| {
                    AnalysisError::InvalidData("Encode pending contract capacity overflow".into())
                })?;
        }
        self.observed_contracts = pending_contracts.len();

        let mut total = ENCODE_RESIDENT_FIXED_BYTES;
        for bytes in [
            capacity_bytes::<PendingContractSlot>(pending_contracts.capacity())?,
            self.token_source_capacity,
            hash_map_capacity_bytes::<u32, PendingFallbackContract>(pending_fallbacks.capacity())?,
            arena.resident_bytes(),
        ] {
            total = total.checked_add(bytes).ok_or_else(|| {
                AnalysisError::InvalidData("Encode registration accounting overflow".into())
            })?;
        }
        Ok(total)
    }
}

#[cfg(test)]
pub(super) fn stream_encode_inputs_with_progress(
    conn: &Connection,
    work_directory: &Path,
    broker: &mut StorageBroker,
    memory_broker: &MemoryBroker,
    threads: usize,
    estimate: EncodeAdmissionEstimate,
    progress: impl FnMut(ProgressEvent),
) -> Result<EncodeStreamInputs, AnalysisError> {
    let lease = memory_broker
        .reserve(estimate.resident_peak_bytes)
        .map_err(|error| {
            AnalysisError::InvalidData(format!("metadata encode memory admission: {error}"))
        })?;
    let mut resident_admission = EncodeResidentAdmission::new(lease, estimate.resident_peak_bytes);
    stream_encode_inputs_with_admission(
        conn,
        work_directory,
        broker,
        memory_broker,
        &mut resident_admission,
        threads,
        estimate,
        progress,
    )
}

#[allow(clippy::too_many_arguments)]
fn stream_encode_inputs_with_admission(
    conn: &Connection,
    _work_directory: &Path,
    _broker: &mut StorageBroker,
    _memory_broker: &MemoryBroker,
    resident_admission: &mut EncodeResidentAdmission,
    threads: usize,
    estimate: EncodeAdmissionEstimate,
    mut progress: impl FnMut(ProgressEvent),
) -> Result<EncodeStreamInputs, AnalysisError> {
    let (token_rows, token_json_bytes) = token_source_relation_dimensions(conn)?;
    let representative_rows: u64 = conn.query_row(
        "SELECT count(*)::UBIGINT
         FROM analysis_contracts
         WHERE metadata_contract_index IS NOT NULL",
        [],
        |row| row.get(0),
    )?;
    let required_relation_peak =
        planned_token_relation_peak(token_rows, representative_rows, token_json_bytes)?;
    if token_rows != estimate.token_rows
        || representative_rows != estimate.representative_rows
        || required_relation_peak != estimate.token_relation_peak_bytes
    {
        return Err(AnalysisError::InvalidData(format!(
            "token-source relation admission is stale or insufficient: token_rows={token_rows}, representative_rows={representative_rows}, required={required_relation_peak}, admitted_token_rows={}, admitted_representative_rows={}, admitted_relation={}",
            estimate.token_rows, estimate.representative_rows, estimate.token_relation_peak_bytes
        )));
    }
    let contract_count = u32::try_from(estimate.representative_rows).map_err(|_| {
        AnalysisError::InvalidData("metadata contract count exceeds u32 identity space".into())
    })?;
    let parse_lanes = threads.max(1);
    let parse_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(parse_lanes)
        .thread_name(|index| format!("metadata-encode-parse-{index}"))
        .build()
        .map_err(|error| AnalysisError::InvalidData(format!("encode parse pool: {error}")))?;
    let shard_count = parse_lanes
        .next_power_of_two()
        .clamp(1, DEFAULT_PAYLOAD_SHARD_COUNT.max(1));
    let arena = ShardedPayloadArena::with_shard_count(shard_count, DEFAULT_ARENA_CHUNK_BYTES);
    let token_source_relation = build_retained_token_source_relation(
        conn,
        contract_count,
        &arena,
        &parse_pool,
        &mut progress,
    )?;
    let relation_resident_bytes = token_source_relation
        .bytes()
        .checked_add(arena.resident_bytes())
        .ok_or_else(|| {
            AnalysisError::InvalidData("token-source+arena admission overflow".into())
        })?;
    if relation_resident_bytes > estimate.resident_peak_bytes {
        return Err(AnalysisError::InvalidData(format!(
            "in-memory token-source relation exceeded resident admission ({} > {})",
            relation_resident_bytes, estimate.resident_peak_bytes
        )));
    }

    let chain_totals = load_encode_chain_totals(conn)?;
    let (arena, pending_contracts, committed_resident_bytes, global_offsets) =
        register_representative_payloads(
            conn,
            &token_source_relation,
            relation_resident_bytes,
            arena,
            resident_admission,
            &parse_pool,
            &estimate,
            &mut progress,
        )?;
    drop(token_source_relation);
    // Relation coords are gone; shrink the lease to pending columns + arena.
    resident_admission.commit(committed_resident_bytes)?;

    // Phase C/D: parse unique payloads in bounded batches, intern immediately,
    // then drop the ParsedMetadataDocuments before the next batch.
    let payload_count = arena.len().map_err(encode_err)?;
    let finalize_growth = planned_encode_finalize_growth(payload_count, pending_contracts.len())?;
    resident_admission.reserve_growth(committed_resident_bytes, finalize_growth)?;
    let admitted_after_finalize = committed_resident_bytes
        .checked_add(finalize_growth)
        .ok_or_else(|| AnalysisError::InvalidData("Encode finalize admission overflow".into()))?;
    let arena = arena.freeze().map_err(encode_err)?;
    let payload_total = payload_count as u64;
    progress(ProgressEvent::determinate(
        ProgressPhase::EncodeParseUniquePayloads,
        0,
        payload_total,
        WorkUnit::Items,
        EngineCounters::default(),
    ));
    progress(ProgressEvent::determinate(
        ProgressPhase::EncodeBuildTermDictionary,
        0,
        payload_total,
        WorkUnit::Items,
        EngineCounters::default(),
    ));
    let mut payloads = PayloadTermSoA::with_payload_capacity(payload_count);
    let payload_interner = ShardedPayloadTermInterner::with_shard_count(shard_count);
    let mut parsed_completed = 0u64;
    for batch_start in (0..payload_count).step_by(ENCODE_UNIQUE_PARSE_BATCH_LEN) {
        let batch_end = (batch_start + ENCODE_UNIQUE_PARSE_BATCH_LEN).min(payload_count);
        let batch_len = batch_end - batch_start;
        let mut batch_json_bytes = 0u64;
        for payload_id in batch_start..batch_end {
            let len = arena
                .with_global_bytes(payload_id as u32, &global_offsets, |bytes| bytes.len())
                .map_err(encode_err)? as u64;
            batch_json_bytes = batch_json_bytes.checked_add(len).ok_or_else(|| {
                AnalysisError::InvalidData("Encode unique parse JSON bytes overflow".into())
            })?;
        }
        // Peak = durable finalize headroom + one batch of parse scratch.
        resident_admission.reserve_growth(
            admitted_after_finalize,
            planned_unique_parse_batch_growth(batch_json_bytes, batch_len)?,
        )?;
        let parsed_batch = parse_pool.install(|| {
            (batch_start..batch_end)
                .into_par_iter()
                .map(|payload_id| {
                    arena
                        .with_global_bytes(payload_id as u32, &global_offsets, |bytes| {
                            let text = std::str::from_utf8(bytes).map_err(|error| {
                                AnalysisError::InvalidData(format!(
                                    "encode payload bytes were not valid utf-8: {error}"
                                ))
                            })?;
                            Ok(parse_metadata_documents(text))
                        })
                        .map_err(encode_err)?
                })
                .collect::<Result<Vec<_>, AnalysisError>>()
        })?;
        let interned_batch = parse_pool.install(|| payload_interner.intern_batch(parsed_batch))?;
        parsed_completed = parsed_completed.saturating_add(interned_batch.len() as u64);
        progress(ProgressEvent::determinate(
            ProgressPhase::EncodeParseUniquePayloads,
            parsed_completed,
            payload_total,
            WorkUnit::Items,
            EngineCounters::default(),
        ));
        let batch_soa = PayloadTermSoA::from_term_lists_owned(interned_batch).map_err(|error| {
            AnalysisError::InvalidData(format!("payload term SoA pack: {error}"))
        })?;
        payloads.append_soa(&batch_soa).map_err(|error| {
            AnalysisError::InvalidData(format!("payload term SoA append: {error}"))
        })?;
        emit_encode_progress(
            &mut progress,
            ProgressPhase::EncodeBuildTermDictionary,
            payloads.payload_count() as u64,
            payload_total,
        );
        // Parsed scratch is dropped with the batch Vec; keep finalize floor.
        resident_admission.commit(admitted_after_finalize)?;
    }
    drop(payload_interner);
    // Payload bodies are only needed until every unique payload has been
    // parsed and interned; no transient payload_blobs directory is created.
    drop(arena);

    // Phase E: build final columns from the already-resolved payload_ids, in
    // original contract order (immediate contracts first, then fallback
    // contracts, matching the order they were registered above).
    let contract_total = pending_contracts.len() as u64;
    progress(ProgressEvent::determinate(
        ProgressPhase::EncodeBuildColumns,
        0,
        contract_total,
        WorkUnit::Items,
        EngineCounters::default(),
    ));
    let mut sources = EncodeSourceSoA::with_source_capacity(pending_contracts.len());
    let mut contracts = EncodeContractSoA::with_contract_capacity(pending_contracts.len());
    for (index, slot) in pending_contracts.into_iter().enumerate() {
        let contract_id = u32::try_from(index).map_err(|_| {
            AnalysisError::InvalidData("metadata contract count exceeds u32".into())
        })?;
        build_encoded_contract(slot, contract_id, &mut sources, &mut contracts)?;
        emit_encode_progress(
            &mut progress,
            ProgressPhase::EncodeBuildColumns,
            index as u64 + 1,
            contract_total,
        );
    }

    // Phase F: atoms / blocking sketches, unchanged from the legacy pass.
    let atoms_total = 1u64.saturating_add(contracts.contract_count() as u64);
    progress(ProgressEvent::determinate(
        ProgressPhase::EncodeBuildAtoms,
        0,
        atoms_total,
        WorkUnit::Items,
        EngineCounters::default(),
    ));
    let mut atoms_completed = 0u64;
    let payload_feature_identity = payload_feature_identity_ids(&payloads);
    let fallback_atoms = build_fallback_atoms_hash_sharded(
        &contracts,
        &payload_feature_identity,
        shard_count,
        |completed| {
            atoms_completed = completed;
            emit_encode_progress(
                &mut progress,
                ProgressPhase::EncodeBuildAtoms,
                atoms_completed,
                atoms_total,
            );
        },
    )?;
    let atoms = build_base_equivalent_atom_sketches_from_soa_parallel(
        &payloads,
        &fallback_atoms.atom_payloads,
        threads,
    );
    atoms_completed = atoms_completed.saturating_add(1);
    emit_encode_progress(
        &mut progress,
        ProgressPhase::EncodeBuildAtoms,
        atoms_completed,
        atoms_total,
    );
    drop(payload_feature_identity);

    progress(ProgressEvent::determinate(
        ProgressPhase::EncodeFinalize,
        0,
        1,
        WorkUnit::Items,
        EngineCounters::default(),
    ));
    resident_admission.commit(frozen_encode_state_resident_bytes(
        &sources,
        &payloads,
        &contracts,
        &atoms,
        &fallback_atoms,
    )?)?;
    progress(ProgressEvent::determinate(
        ProgressPhase::EncodeFinalize,
        1,
        1,
        WorkUnit::Items,
        EngineCounters::default(),
    ));

    Ok((
        sources,
        payloads,
        contracts,
        atoms,
        fallback_atoms,
        chain_totals,
    ))
}

/// Presence-only registration: reads representative rows via Arrow, computes
/// eligibility + prefilter-token presence in rayon-parallel per batch, then
/// sequentially inserts eligible JSON into the [`ShardedPayloadArena`]. Token
/// sources reuse catalog `PayloadRef` values (JSON already in the arena).
/// Full parse is deferred to a unique pass over every arena payload.
///
/// Returns columns+arena resident bytes **without** the token-source relation
/// structural bytes, plus global payload ID offsets for the unique-parse pass.
#[allow(clippy::too_many_arguments)]
fn register_representative_payloads(
    conn: &Connection,
    token_source_relation: &TokenSourceRelation,
    relation_resident_bytes: u64,
    arena: ShardedPayloadArena,
    resident_admission: &mut EncodeResidentAdmission,
    parse_pool: &rayon::ThreadPool,
    estimate: &EncodeAdmissionEstimate,
    progress: &mut impl FnMut(ProgressEvent),
) -> Result<
    (
        ShardedPayloadArena,
        Vec<GlobalPendingContractSlot>,
        u64,
        Vec<u32>,
    ),
    AnalysisError,
> {
    let mut pending_contracts = Vec::<PendingContractSlot>::new();
    let mut pending_fallbacks = HashMap::<u32, PendingFallbackContract>::new();
    let mut registration_accounting = EncodeRegistrationAccounting::default();

    let mut columns_resident_bytes =
        registration_accounting.resident_bytes(&pending_contracts, &pending_fallbacks, &arena)?;
    let mut committed_resident_bytes = columns_resident_bytes
        .checked_add(relation_resident_bytes)
        .ok_or_else(|| {
            AnalysisError::InvalidData("Encode relation+columns admission overflow".into())
        })?;
    // Relation coords stay in the committed base until the caller drops them.
    // Arena already holds token-source JSON from catalog load (no second copy).
    resident_admission.commit(committed_resident_bytes)?;

    progress(ProgressEvent::determinate(
        ProgressPhase::EncodeReadRepresentatives,
        0,
        estimate.representative_rows,
        WorkUnit::Items,
        EngineCounters::default(),
    ));
    progress(ProgressEvent::determinate(
        ProgressPhase::EncodeRegisterPayloads,
        0,
        estimate.representative_rows,
        WorkUnit::Items,
        EngineCounters::default(),
    ));

    let mut statement = conn.prepare(
        "SELECT contracts.metadata_contract_index::UINTEGER AS metadata_contract_index,
                selected.chain_index::UINTEGER AS chain_id,
                rows.metadata_json,
                contracts.nft_count::BIGINT AS nft_count,
                contracts.metadata_source_file::UINTEGER AS metadata_source_file,
                contracts.metadata_source_row_number::UBIGINT AS metadata_source_row_number
         FROM analysis_contracts contracts
         JOIN metadata_rows rows
           ON rows.source_file = contracts.metadata_source_file
          AND rows.source_row_number = contracts.metadata_source_row_number
         JOIN selected_chains selected
           ON selected.chain = contracts.chain
         WHERE contracts.metadata_contract_index IS NOT NULL
         ORDER BY contracts.metadata_contract_index",
    )?;
    let batches = statement.query_arrow([])?;

    let mut representative_rows_read = 0u64;
    let mut representative_rows_registered = 0u64;
    for batch in batches {
        let row_count = batch.num_rows();
        if row_count == 0 {
            continue;
        }
        let contract_indexes =
            required_arrow_column::<UInt32Array>(&batch, 0, "metadata_contract_index")?;
        let chain_ids = required_arrow_column::<UInt32Array>(&batch, 1, "chain_id")?;
        let json_column = batch.column(2).as_ref();
        let nft_counts = required_arrow_column::<Int64Array>(&batch, 3, "nft_count")?;
        let source_files = required_arrow_column::<UInt32Array>(&batch, 4, "metadata_source_file")?;
        let source_rows =
            required_arrow_column::<UInt64Array>(&batch, 5, "metadata_source_row_number")?;

        let presence = parse_pool.install(|| {
            (0..row_count)
                .into_par_iter()
                .map(|index| {
                    if contract_indexes.is_null(index)
                        || chain_ids.is_null(index)
                        || json_column.is_null(index)
                        || nft_counts.is_null(index)
                        || source_files.is_null(index)
                        || source_rows.is_null(index)
                    {
                        return Err(AnalysisError::InvalidData(
                            "metadata representative row contains NULL".into(),
                        ));
                    }
                    let json = required_arrow_string(json_column, index)?;
                    let eligible = metadata_is_dedup_eligible(json);
                    let has_tokens = eligible && metadata_has_prefilter_tokens(json);
                    Ok((eligible, has_tokens))
                })
                .collect::<Result<Vec<_>, AnalysisError>>()
        })?;
        representative_rows_read = representative_rows_read.saturating_add(row_count as u64);
        progress(ProgressEvent::determinate(
            ProgressPhase::EncodeReadRepresentatives,
            representative_rows_read,
            estimate.representative_rows,
            WorkUnit::Items,
            EngineCounters::default(),
        ));

        let mut batch_json_bytes = 0u64;
        for index in 0..row_count {
            let json = required_arrow_string(json_column, index)?;
            batch_json_bytes =
                batch_json_bytes
                    .checked_add(json.len() as u64)
                    .ok_or_else(|| {
                        AnalysisError::InvalidData("Encode batch JSON bytes overflow".into())
                    })?;
        }
        let batch_growth_bytes = planned_encode_batch_growth(batch_json_bytes, row_count)?;
        resident_admission.reserve_growth(committed_resident_bytes, batch_growth_bytes)?;

        for (index, &(eligible, has_tokens)) in presence.iter().enumerate() {
            let contract_index = contract_indexes.value(index);
            let chain_id = chain_ids.value(index);
            let json = required_arrow_string(json_column, index)?;
            let nft_count = nft_counts.value(index);
            let source_file = source_files.value(index);
            let source_row_number = source_rows.value(index);
            representative_rows_registered = representative_rows_registered.saturating_add(1);

            if !eligible {
                emit_encode_progress(
                    progress,
                    ProgressPhase::EncodeRegisterPayloads,
                    representative_rows_registered,
                    estimate.representative_rows,
                );
                continue;
            }
            let weight = u64::try_from(nft_count).map_err(|_| {
                AnalysisError::InvalidData("negative metadata contract nft_count".into())
            })?;
            if !has_tokens {
                pending_fallbacks.insert(
                    contract_index,
                    PendingFallbackContract {
                        source_contract_index: contract_index,
                        chain_id,
                        weight,
                    },
                );
                emit_encode_progress(
                    progress,
                    ProgressPhase::EncodeRegisterPayloads,
                    representative_rows_registered,
                    estimate.representative_rows,
                );
                continue;
            }

            let representative_payload_ref = arena.insert(json.as_bytes()).map_err(encode_err)?;
            let token_sources = token_source_relation.read_contract(contract_index)?;
            let mut token_slots = Vec::with_capacity(token_sources.len());
            for source in token_sources {
                token_slots.push(PendingSourceSlot {
                    source_file: source.source_file,
                    source_row_number: source.source_row_number,
                    payload_ref: source.payload_ref,
                    token_ids: source.token_ids,
                });
            }
            pending_contracts.push(PendingContractSlot {
                chain_id,
                weight,
                representative_file: source_file,
                representative_row: source_row_number,
                representative_payload_ref,
                token_sources: token_slots,
            });
            emit_encode_progress(
                progress,
                ProgressPhase::EncodeRegisterPayloads,
                representative_rows_registered,
                estimate.representative_rows,
            );
        }

        columns_resident_bytes = registration_accounting.resident_bytes(
            &pending_contracts,
            &pending_fallbacks,
            &arena,
        )?;
        committed_resident_bytes = columns_resident_bytes
            .checked_add(relation_resident_bytes)
            .ok_or_else(|| {
                AnalysisError::InvalidData("Encode relation+columns admission overflow".into())
            })?;
        resident_admission.commit(committed_resident_bytes)?;
    }
    drop(statement);

    if !pending_fallbacks.is_empty() {
        columns_resident_bytes = resolve_pending_fallback_contracts(
            conn,
            token_source_relation,
            relation_resident_bytes,
            &arena,
            &mut pending_contracts,
            &mut pending_fallbacks,
            resident_admission,
            &mut registration_accounting,
            columns_resident_bytes,
            parse_pool,
            progress,
        )?;
    }
    drop(pending_fallbacks);

    let global_offsets = arena.global_offsets().map_err(encode_err)?;
    let pending_contracts =
        materialize_global_pending_contracts(&arena, &global_offsets, pending_contracts)?;
    Ok((
        arena,
        pending_contracts,
        columns_resident_bytes,
        global_offsets,
    ))
}

fn materialize_global_pending_contracts(
    arena: &ShardedPayloadArena,
    offsets: &[u32],
    pending: Vec<PendingContractSlot>,
) -> Result<Vec<GlobalPendingContractSlot>, AnalysisError> {
    pending
        .into_iter()
        .map(|slot| {
            let representative_payload_id = arena
                .global_id(slot.representative_payload_ref, offsets)
                .map_err(encode_err)?;
            let mut token_sources = Vec::with_capacity(slot.token_sources.len());
            for source in slot.token_sources {
                token_sources.push(GlobalPendingSourceSlot {
                    source_file: source.source_file,
                    source_row_number: source.source_row_number,
                    payload_id: arena
                        .global_id(source.payload_ref, offsets)
                        .map_err(encode_err)?,
                    token_ids: source.token_ids,
                });
            }
            Ok(GlobalPendingContractSlot {
                chain_id: slot.chain_id,
                weight: slot.weight,
                representative_file: slot.representative_file,
                representative_row: slot.representative_row,
                representative_payload_id,
                token_sources,
            })
        })
        .collect()
}

/// Fallback resolution: for every contract whose representative row had no
/// retained prefilter tokens, stream candidate rows in stable order and keep
/// only the first JSON that would have prefilter tokens. Candidates are
/// admitted one-at-a-time before retention; rejected rows are dropped without
/// Presence-only fallback selection over Arrow batches.
///
/// Keeps only a cross-batch contract cursor + selected flag. The first
/// presence hit inserts JSON into the arena immediately and records a
/// [`PayloadRef`]; later rows for the same contract are skipped without
/// copying JSON. Memory is admitted per selected row only.
pub(super) fn fallback_contract_candidates_sql() -> &'static str {
    "SELECT fallback.contract_index::UINTEGER AS contract_index,
                rows.metadata_json,
                rows.source_file::UINTEGER AS source_file,
                rows.source_row_number::UBIGINT AS source_row_number
         FROM unnest(?::UINTEGER[]) AS fallback(contract_index)
         JOIN analysis_contracts contracts
           ON contracts.metadata_contract_index = fallback.contract_index
         JOIN metadata_rows rows ON rows.contract_id = contracts.contract_id
         WHERE rows.metadata_eligible
         ORDER BY fallback.contract_index,
                  rows.token_id,
                  rows.source_file,
                  rows.source_row_number"
}

#[allow(clippy::too_many_arguments)]
fn resolve_pending_fallback_contracts(
    conn: &Connection,
    token_source_relation: &TokenSourceRelation,
    relation_resident_bytes: u64,
    arena: &ShardedPayloadArena,
    pending_contracts: &mut Vec<PendingContractSlot>,
    pending_fallbacks: &mut HashMap<u32, PendingFallbackContract>,
    resident_admission: &mut EncodeResidentAdmission,
    registration_accounting: &mut EncodeRegistrationAccounting,
    mut columns_resident_bytes: u64,
    parse_pool: &rayon::ThreadPool,
    progress: &mut impl FnMut(ProgressEvent),
) -> Result<u64, AnalysisError> {
    let mut fallback_ids = pending_fallbacks.keys().copied().collect::<Vec<_>>();
    fallback_ids.sort_unstable();
    let fallback_ids = Value::List(fallback_ids.into_iter().map(Value::UInt).collect());
    progress(ProgressEvent::indeterminate(
        ProgressPhase::EncodeFallbackSources,
        0,
        WorkUnit::Items,
        EngineCounters::default(),
    ));
    let mut stmt = conn.prepare(fallback_contract_candidates_sql())?;
    let batches = stmt.query_arrow([fallback_ids])?;

    progress(ProgressEvent::indeterminate(
        ProgressPhase::EncodeResolveFallbacks,
        0,
        WorkUnit::Items,
        EngineCounters::default(),
    ));

    let mut current_contract: Option<u32> = None;
    let mut selected: Option<SelectedFallbackRow> = None;
    let mut resolved_count = 0u64;
    let mut scanned = 0u64;

    for batch in batches {
        let row_count = batch.num_rows();
        if row_count == 0 {
            continue;
        }
        let contract_indexes = required_arrow_column::<UInt32Array>(&batch, 0, "contract_index")?;
        let json_column = batch.column(1).as_ref();
        let source_files = required_arrow_column::<UInt32Array>(&batch, 2, "source_file")?;
        let source_rows = required_arrow_column::<UInt64Array>(&batch, 3, "source_row_number")?;

        // Presence checks for rows that still need a selection; already-selected
        // contracts skip JSON access entirely after the cursor advances.
        let presence = parse_pool.install(|| {
            (0..row_count)
                .into_par_iter()
                .map(|index| {
                    if contract_indexes.is_null(index)
                        || json_column.is_null(index)
                        || source_files.is_null(index)
                        || source_rows.is_null(index)
                    {
                        return Err(AnalysisError::InvalidData(
                            "metadata fallback row contains NULL".into(),
                        ));
                    }
                    let json = required_arrow_string(json_column, index)?;
                    Ok(metadata_has_prefilter_tokens(json))
                })
                .collect::<Result<Vec<_>, AnalysisError>>()
        })?;

        for (index, &has_tokens) in presence.iter().enumerate() {
            let contract_index = contract_indexes.value(index);
            scanned = scanned.saturating_add(1);
            emit_encode_indeterminate_progress(
                progress,
                ProgressPhase::EncodeFallbackSources,
                scanned,
            );
            if Some(contract_index) != current_contract {
                if let Some(chosen) = selected.take() {
                    columns_resident_bytes = register_resolved_fallback_contract(
                        chosen,
                        token_source_relation,
                        relation_resident_bytes,
                        arena,
                        pending_contracts,
                        pending_fallbacks,
                        resident_admission,
                        registration_accounting,
                    )?;
                    resolved_count = resolved_count.saturating_add(1);
                }
                current_contract = Some(contract_index);
            }
            if selected.is_some() {
                continue;
            }
            if !has_tokens {
                continue;
            }
            let json = required_arrow_string(json_column, index)?;
            let committed = columns_resident_bytes
                .checked_add(relation_resident_bytes)
                .ok_or_else(|| {
                    AnalysisError::InvalidData("Encode fallback relation admission overflow".into())
                })?;
            let growth = planned_encode_batch_growth(json.len() as u64, 1)?;
            resident_admission.reserve_growth(committed, growth)?;
            let payload_ref = arena.insert(json.as_bytes()).map_err(encode_err)?;
            selected = Some(SelectedFallbackRow {
                contract_index,
                source_file: source_files.value(index),
                source_row_number: source_rows.value(index),
                payload_ref,
            });
        }
    }
    if let Some(chosen) = selected.take() {
        columns_resident_bytes = register_resolved_fallback_contract(
            chosen,
            token_source_relation,
            relation_resident_bytes,
            arena,
            pending_contracts,
            pending_fallbacks,
            resident_admission,
            registration_accounting,
        )?;
        resolved_count = resolved_count.saturating_add(1);
    }

    progress(ProgressEvent::indeterminate(
        ProgressPhase::EncodeResolveFallbacks,
        resolved_count,
        WorkUnit::Items,
        EngineCounters::default(),
    ));
    progress(ProgressEvent::indeterminate(
        ProgressPhase::EncodeFallbackSources,
        scanned,
        WorkUnit::Items,
        EngineCounters::default(),
    ));
    Ok(columns_resident_bytes)
}

struct SelectedFallbackRow {
    contract_index: u32,
    source_file: u32,
    source_row_number: u64,
    payload_ref: PayloadRef,
}

#[allow(clippy::too_many_arguments)]
fn register_resolved_fallback_contract(
    row: SelectedFallbackRow,
    token_source_relation: &TokenSourceRelation,
    relation_resident_bytes: u64,
    arena: &ShardedPayloadArena,
    pending_contracts: &mut Vec<PendingContractSlot>,
    pending_fallbacks: &mut HashMap<u32, PendingFallbackContract>,
    resident_admission: &mut EncodeResidentAdmission,
    registration_accounting: &mut EncodeRegistrationAccounting,
) -> Result<u64, AnalysisError> {
    let Some(pending) = pending_fallbacks.remove(&row.contract_index) else {
        return registration_accounting.resident_bytes(pending_contracts, pending_fallbacks, arena);
    };
    let token_sources = token_source_relation.read_contract(pending.source_contract_index)?;
    let mut token_slots = Vec::with_capacity(token_sources.len());
    for source in token_sources {
        token_slots.push(PendingSourceSlot {
            source_file: source.source_file,
            source_row_number: source.source_row_number,
            payload_ref: source.payload_ref,
            token_ids: source.token_ids,
        });
    }
    pending_contracts.push(PendingContractSlot {
        chain_id: pending.chain_id,
        weight: pending.weight,
        representative_file: row.source_file,
        representative_row: row.source_row_number,
        representative_payload_ref: row.payload_ref,
        token_sources: token_slots,
    });
    let columns =
        registration_accounting.resident_bytes(pending_contracts, pending_fallbacks, arena)?;
    let committed = columns
        .checked_add(relation_resident_bytes)
        .ok_or_else(|| {
            AnalysisError::InvalidData("Encode fallback relation admission overflow".into())
        })?;
    resident_admission.commit(committed)?;
    Ok(columns)
}

fn build_encoded_contract(
    slot: GlobalPendingContractSlot,
    contract_id: u32,
    sources: &mut EncodeSourceSoA,
    contracts: &mut EncodeContractSoA,
) -> Result<(), AnalysisError> {
    let mut selected_tokens = None::<Vec<u32>>;
    let mut remaining_sources = Vec::with_capacity(slot.token_sources.len());
    for source in slot.token_sources {
        if source.source_file == slot.representative_file
            && source.source_row_number == slot.representative_row
        {
            if let Some(tokens) = selected_tokens.as_mut() {
                tokens.extend(source.token_ids);
            } else {
                selected_tokens = Some(source.token_ids);
            }
        } else {
            remaining_sources.push(source);
        }
    }
    let source_doc_id = u32::try_from(sources.source_count())
        .map_err(|_| AnalysisError::InvalidData("metadata source count exceeds u32".into()))?;
    sources
        .push_source(
            contract_id,
            slot.representative_payload_id,
            selected_tokens.as_deref().unwrap_or(&[]),
        )
        .map_err(encode_err)?;
    for source in remaining_sources {
        let mut token_ids = source.token_ids;
        if token_ids.windows(2).any(|pair| pair[0] >= pair[1]) {
            token_ids.sort_unstable();
            token_ids.dedup();
        }
        sources
            .push_source(contract_id, source.payload_id, &token_ids)
            .map_err(encode_err)?;
    }
    contracts.push_contract(
        contract_id,
        slot.chain_id,
        source_doc_id,
        slot.representative_payload_id,
        slot.weight,
    );
    Ok(())
}

fn payload_feature_identity_ids(payloads: &PayloadTermSoA) -> Vec<u32> {
    enum IdentityBucket {
        Single { payload_index: usize, identity: u32 },
        Collision(Vec<(usize, u32)>),
    }

    let mut buckets = HashMap::<u64, IdentityBucket>::new();
    let mut identities = Vec::with_capacity(payloads.payload_count());
    let mut next_identity = 0u32;
    for payload_index in 0..payloads.payload_count() {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        payloads.hash_payload(payload_index, &mut hasher);
        let hash = hasher.finish();
        let identity = match buckets.entry(hash) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                let identity = next_identity;
                next_identity = next_identity.saturating_add(1);
                entry.insert(IdentityBucket::Single {
                    payload_index,
                    identity,
                });
                identity
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => match entry.get_mut() {
                IdentityBucket::Single {
                    payload_index: representative,
                    identity,
                } => {
                    if payloads.payload_eq(*representative, payload_index) {
                        *identity
                    } else {
                        let new_identity = next_identity;
                        next_identity = next_identity.saturating_add(1);
                        let collision =
                            vec![(*representative, *identity), (payload_index, new_identity)];
                        *entry.get_mut() = IdentityBucket::Collision(collision);
                        new_identity
                    }
                }
                IdentityBucket::Collision(bucket) => bucket
                    .iter()
                    .find_map(|&(representative, identity)| {
                        payloads
                            .payload_eq(representative, payload_index)
                            .then_some(identity)
                    })
                    .unwrap_or_else(|| {
                        let identity = next_identity;
                        next_identity = next_identity.saturating_add(1);
                        bucket.push((payload_index, identity));
                        identity
                    }),
            },
        };
        identities.push(identity);
    }
    identities
}

/// Retained for differential/unit tests exercising the arena-dedup +
/// interning contract directly (production registration defers parsing; see
/// `register_representative_payloads` / the unique-parse pass).
#[cfg(test)]
fn intern_payload_with_parser(
    metadata_json: &str,
    cas: &mut PayloadArena,
    payloads: &mut Vec<EncodePayloadRow>,
    payload_interner: &mut PayloadTermInterner,
    parse: impl FnOnce(&str) -> ParsedMetadataDocuments,
) -> Result<u32, AnalysisError> {
    let payload_id = cas.insert(metadata_json.as_bytes()).map_err(encode_err)?;
    if payload_id as usize >= payloads.len() {
        payloads.push(payload_interner.intern(parse(metadata_json))?);
    }
    Ok(payload_id)
}

fn load_encode_chain_totals(conn: &Connection) -> Result<Vec<EncodeChainTotal>, AnalysisError> {
    let mut stmt = conn.prepare(
        "SELECT selected.chain,
                count(contracts.chain)::BIGINT,
                coalesce(sum(contracts.nft_count), 0)::BIGINT
         FROM selected_chains selected
         LEFT JOIN analysis_contracts contracts ON contracts.chain = selected.chain
         GROUP BY selected.chain_index, selected.chain
         ORDER BY selected.chain_index",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(EncodeChainTotal {
            name: row.get(0)?,
            contracts: row.get(1)?,
            nfts: row.get(2)?,
        })
    })?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(AnalysisError::from)
}

pub(super) fn retained_token_candidates_sql() -> &'static str {
    "SELECT token_rows.contract_index::UINTEGER AS contract_index,
            token_rows.token_index::UINTEGER AS token_index,
            rows.source_file::UINTEGER AS source_file,
            rows.source_row_number::UBIGINT AS source_row_number,
            rows.metadata_json
     FROM metadata_contract_token_rows token_rows
     JOIN analysis_contracts contracts
       ON contracts.metadata_contract_index = token_rows.contract_index
     JOIN metadata_token_dictionary dictionary
       ON dictionary.token_index = token_rows.token_index
     JOIN metadata_rows rows
       ON rows.contract_id = contracts.contract_id
      AND rows.token_id = dictionary.token_id
     WHERE rows.metadata_eligible
     ORDER BY token_rows.contract_index,
              token_rows.token_index,
              rows.source_file,
              rows.source_row_number"
}

fn build_retained_token_source_relation(
    conn: &Connection,
    contract_count: u32,
    arena: &ShardedPayloadArena,
    parse_pool: &rayon::ThreadPool,
    progress: &mut impl FnMut(ProgressEvent),
) -> Result<TokenSourceRelation, AnalysisError> {
    progress(ProgressEvent::determinate(
        ProgressPhase::EncodeCollectTokenSources,
        0,
        1,
        WorkUnit::Work,
        EngineCounters::default(),
    ));
    let exists: bool = conn.query_row(
        "SELECT count(*) > 0 FROM duckdb_tables() WHERE table_name = ?",
        ["metadata_contract_token_rows"],
        |row| row.get(0),
    )?;
    if !exists {
        progress(ProgressEvent::determinate(
            ProgressPhase::EncodeCollectTokenSources,
            1,
            1,
            WorkUnit::Work,
            EngineCounters::default(),
        ));
        return Ok(TokenSourceRelation {
            sources: Vec::new(),
            memberships: Vec::new(),
            contract_offsets: vec![0; contract_count as usize + 1],
            logical_bytes: 0,
        });
    }

    progress(ProgressEvent::determinate(
        ProgressPhase::EncodeCollectTokenSources,
        1,
        1,
        WorkUnit::Work,
        EngineCounters::default(),
    ));
    let mut statement = conn.prepare(retained_token_candidates_sql())?;
    let batches = statement.query_arrow([])?;
    let mut selected = Vec::<SelectedTokenSource>::new();
    let mut current_group = None;
    let mut group_selected = false;
    let mut scanned = 0u64;
    progress(ProgressEvent::indeterminate(
        ProgressPhase::EncodeTokenSources,
        0,
        WorkUnit::Items,
        EngineCounters::default(),
    ));
    for batch in batches {
        let contracts = required_arrow_column::<UInt32Array>(&batch, 0, "contract_index")?;
        let tokens = required_arrow_column::<UInt32Array>(&batch, 1, "token_index")?;
        let source_files = required_arrow_column::<UInt32Array>(&batch, 2, "source_file")?;
        let source_rows = required_arrow_column::<UInt64Array>(&batch, 3, "source_row_number")?;
        let json = batch.column(4).as_ref();
        let usable = parse_pool.install(|| {
            (0..batch.num_rows())
                .into_par_iter()
                .map(|index| {
                    if contracts.is_null(index)
                        || tokens.is_null(index)
                        || source_files.is_null(index)
                        || source_rows.is_null(index)
                        || json.is_null(index)
                    {
                        return Err(AnalysisError::InvalidData(
                            "retained-token candidate contains NULL".into(),
                        ));
                    }
                    Ok(metadata_has_prefilter_tokens(required_arrow_string(
                        json, index,
                    )?))
                })
                .collect::<Result<Vec<_>, AnalysisError>>()
        })?;
        for (index, &is_usable) in usable.iter().enumerate() {
            let group = (contracts.value(index), tokens.value(index));
            if current_group != Some(group) {
                current_group = Some(group);
                group_selected = false;
            }
            scanned = scanned.saturating_add(1);
            if group_selected || !is_usable {
                continue;
            }
            let payload_ref = arena
                .insert(required_arrow_string(json, index)?.as_bytes())
                .map_err(encode_err)?;
            selected.push(SelectedTokenSource {
                contract_index: group.0,
                token_index: group.1,
                coordinate: SourceCoordinate {
                    source_file: source_files.value(index),
                    source_row_number: source_rows.value(index),
                },
                payload_ref,
            });
            group_selected = true;
        }
        progress(ProgressEvent::indeterminate(
            ProgressPhase::EncodeTokenSources,
            scanned,
            WorkUnit::Items,
            EngineCounters {
                matched: selected.len() as u64,
                ..EngineCounters::default()
            },
        ));
    }
    for phase in [
        ProgressPhase::EncodePrepareFallbackTokenSources,
        ProgressPhase::EncodeTokenFallbackSources,
    ] {
        progress(ProgressEvent::determinate(
            phase,
            0,
            0,
            WorkUnit::Items,
            EngineCounters::default(),
        ));
    }

    let mut source_rows = selected
        .iter()
        .map(|row| (row.coordinate, row.payload_ref))
        .collect::<Vec<_>>();
    progress(ProgressEvent::determinate(
        ProgressPhase::EncodeResolveTokenMemberships,
        0,
        selected.len() as u64,
        WorkUnit::Items,
        EngineCounters::default(),
    ));
    source_rows.par_sort_unstable_by_key(|(coordinate, _)| *coordinate);
    source_rows.dedup_by_key(|(coordinate, _)| *coordinate);
    let source_count = u32::try_from(source_rows.len()).map_err(|_| {
        AnalysisError::InvalidData("token source dictionary exceeds u32 identity space".into())
    })?;
    let sources = source_rows
        .iter()
        .map(|(coordinate, payload_ref)| TokenSourceRecord {
            source_file: coordinate.source_file,
            source_row_number: coordinate.source_row_number,
            payload_ref: *payload_ref,
        })
        .collect::<Vec<_>>();
    progress(ProgressEvent::determinate(
        ProgressPhase::EncodeLoadTokenSources,
        sources.len() as u64,
        sources.len() as u64,
        WorkUnit::Items,
        EngineCounters::default(),
    ));
    let source_ids = source_rows
        .iter()
        .enumerate()
        .map(|(source_id, (coordinate, _))| {
            Ok((
                *coordinate,
                u32::try_from(source_id).map_err(|_| {
                    AnalysisError::InvalidData("token source dictionary exceeds u32".into())
                })?,
            ))
        })
        .collect::<Result<HashMap<_, _>, AnalysisError>>()?;
    let mut memberships = selected
        .iter()
        .map(|row| {
            Ok(ResolvedTokenMembership {
                contract_index: row.contract_index,
                token_id: row.token_index,
                source_id: *source_ids.get(&row.coordinate).ok_or_else(|| {
                    AnalysisError::InvalidData(
                        "selected token source is absent from dictionary".into(),
                    )
                })?,
            })
        })
        .collect::<Result<Vec<_>, AnalysisError>>()?;
    let membership_count = memberships.len() as u64;
    progress(ProgressEvent::determinate(
        ProgressPhase::EncodeResolveTokenMemberships,
        membership_count,
        membership_count,
        WorkUnit::Items,
        EngineCounters::default(),
    ));
    progress(ProgressEvent::determinate(
        ProgressPhase::EncodeLoadTokenMemberships,
        membership_count,
        membership_count,
        WorkUnit::Items,
        EngineCounters::default(),
    ));
    progress(ProgressEvent::indeterminate(
        ProgressPhase::EncodeSortTokenMemberships,
        0,
        WorkUnit::Items,
        EngineCounters::default(),
    ));
    parse_pool.install(|| memberships.par_sort_unstable());
    validate_token_memberships(&memberships, contract_count, source_count)?;
    progress(ProgressEvent::indeterminate(
        ProgressPhase::EncodeSortTokenMemberships,
        membership_count,
        WorkUnit::Items,
        EngineCounters::default(),
    ));
    let contract_offsets = token_membership_offsets(&memberships, contract_count)?;
    // JSON bodies live only in the arena; relation retains coords + PayloadRef.
    let logical_bytes = capacity_bytes::<TokenSourceRecord>(sources.capacity())?
        .checked_add(capacity_bytes::<ResolvedTokenMembership>(
            memberships.capacity(),
        )?)
        .and_then(|bytes| {
            bytes.checked_add(capacity_bytes::<usize>(contract_offsets.capacity()).ok()?)
        })
        .ok_or_else(|| AnalysisError::InvalidData("token-source memory size overflow".into()))?;
    Ok(TokenSourceRelation {
        sources,
        memberships,
        contract_offsets,
        logical_bytes,
    })
}

fn required_arrow_column<'a, T: Array + 'static>(
    batch: &'a RecordBatch,
    index: usize,
    name: &str,
) -> Result<&'a T, AnalysisError> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| AnalysisError::InvalidData(format!("Arrow column {name} has wrong type")))
}

fn required_arrow_string(array: &dyn Array, index: usize) -> Result<&str, AnalysisError> {
    if let Some(strings) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(strings.value(index));
    }
    if let Some(strings) = array.as_any().downcast_ref::<StringViewArray>() {
        return Ok(strings.value(index));
    }
    Err(AnalysisError::InvalidData(
        "Arrow metadata_json column is not a string array".into(),
    ))
}

fn validate_token_memberships(
    memberships: &[ResolvedTokenMembership],
    contract_count: u32,
    source_count: u32,
) -> Result<(), AnalysisError> {
    if memberships.iter().any(|membership| {
        membership.contract_index >= contract_count || membership.source_id >= source_count
    }) {
        return Err(AnalysisError::InvalidData(
            "resolved token membership identity is out of range".into(),
        ));
    }
    if memberships.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(AnalysisError::InvalidData(
            "resolved token memberships are duplicated or not strictly ordered".into(),
        ));
    }
    Ok(())
}

fn token_membership_offsets(
    memberships: &[ResolvedTokenMembership],
    contract_count: u32,
) -> Result<Vec<usize>, AnalysisError> {
    let count = usize::try_from(contract_count)
        .map_err(|_| AnalysisError::InvalidData("metadata contract count exceeds usize".into()))?;
    let mut offsets = Vec::with_capacity(count.saturating_add(1));
    offsets.push(0);
    let mut cursor = 0usize;
    for contract in 0..contract_count {
        while cursor < memberships.len() && memberships[cursor].contract_index == contract {
            cursor += 1;
        }
        offsets.push(cursor);
    }
    if cursor != memberships.len() {
        return Err(AnalysisError::InvalidData(
            "resolved token membership contract is out of range".into(),
        ));
    }
    Ok(offsets)
}

fn emit_encode_progress(
    progress: &mut impl FnMut(ProgressEvent),
    phase: ProgressPhase,
    completed: u64,
    total: u64,
) {
    if completed.is_multiple_of(16_384) || completed == total {
        progress(ProgressEvent::determinate(
            phase,
            completed,
            total,
            WorkUnit::Items,
            EngineCounters::default(),
        ));
    }
}

fn emit_encode_indeterminate_progress(
    progress: &mut impl FnMut(ProgressEvent),
    phase: ProgressPhase,
    completed: u64,
) {
    if completed.is_multiple_of(16_384) {
        progress(ProgressEvent::indeterminate(
            phase,
            completed,
            WorkUnit::Items,
            EngineCounters::default(),
        ));
    }
}

fn build_fallback_atoms_hash_sharded(
    contracts: &EncodeContractSoA,
    payload_feature_identity: &[u32],
    shard_count: usize,
    mut on_progress: impl FnMut(u64),
) -> Result<FallbackAtomCsr, AnalysisError> {
    let shard_count = shard_count.next_power_of_two().max(1);
    let shard_mask = shard_count - 1;
    let shards = (0..shard_count)
        .map(|_| Mutex::new(HashMap::<(u32, u32), (u32, Vec<u32>)>::new()))
        .collect::<Vec<_>>();
    (0..contracts.contract_count())
        .into_par_iter()
        .try_for_each(|index| {
            let payload_id = contracts.payload_ids[index];
            let feature = *payload_feature_identity
                .get(payload_id as usize)
                .ok_or_else(|| {
                    AnalysisError::InvalidData("atom feature identity out of range".into())
                })?;
            let key = (contracts.chain_ids[index], feature);
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            key.hash(&mut hasher);
            let shard = (hasher.finish() as usize) & shard_mask;
            let mut map = shards[shard]
                .lock()
                .map_err(|_| AnalysisError::InvalidData("atom shard lock poisoned".into()))?;
            let entry = map.entry(key).or_insert_with(|| (payload_id, Vec::new()));
            entry.1.push(contracts.contract_ids[index]);
            Ok::<_, AnalysisError>(())
        })?;

    let mut offsets = Vec::new();
    offsets.push(0u64);
    let mut members = Vec::new();
    let mut atom_payloads = Vec::new();
    let mut completed = 0u64;
    for shard in &shards {
        let mut map = shard
            .lock()
            .map_err(|_| AnalysisError::InvalidData("atom shard lock poisoned".into()))?;
        for (_key, (payload_id, mut atom_members)) in map.drain() {
            atom_members.sort_unstable();
            atom_payloads.push(payload_id);
            completed = completed.saturating_add(atom_members.len() as u64);
            members.extend(atom_members);
            offsets.push(members.len() as u64);
            on_progress(completed);
        }
    }
    Ok(FallbackAtomCsr {
        offsets,
        members,
        atom_payloads,
    })
}

#[allow(dead_code)] // retained for memory_dedup_tests / differential helpers
#[derive(Default)]
struct PayloadTermInterner {
    template_ids: HashMap<Arc<str>, u32>,
    template_tokens: Vec<Arc<str>>,
    content_ids: HashMap<String, u32>,
    template_string_bytes: u64,
    content_string_bytes: u64,
}

#[allow(dead_code)]
impl PayloadTermInterner {
    fn intern(
        &mut self,
        parsed: ParsedMetadataDocuments,
    ) -> Result<EncodePayloadRow, AnalysisError> {
        let template_freqs = string_term_frequencies(parsed.prefilter_tokens);
        let content_freqs = string_term_frequencies(parsed.content_tokens);
        let mut template_terms = Vec::with_capacity(template_freqs.len());
        for (token, frequency) in template_freqs {
            let token_id = if let Some(&token_id) = self.template_ids.get(token.as_str()) {
                token_id
            } else {
                let token_id = u32::try_from(self.template_tokens.len()).map_err(|_| {
                    AnalysisError::InvalidData("template token count exceeds u32".into())
                })?;
                self.template_string_bytes = self
                    .template_string_bytes
                    .checked_add(token.len() as u64)
                    .and_then(|bytes| bytes.checked_add(2 * std::mem::size_of::<usize>() as u64))
                    .ok_or_else(|| {
                        AnalysisError::InvalidData(
                            "template token resident accounting overflow".into(),
                        )
                    })?;
                let token: Arc<str> = Arc::from(token);
                self.template_tokens.push(Arc::clone(&token));
                self.template_ids.insert(token, token_id);
                token_id
            };
            template_terms.push((token_id, frequency));
        }
        let mut content_terms = Vec::with_capacity(content_freqs.len());
        for (token, frequency) in content_freqs {
            let token_id = if let Some(&token_id) = self.content_ids.get(&token) {
                token_id
            } else {
                let next_id = u32::try_from(self.content_ids.len()).map_err(|_| {
                    AnalysisError::InvalidData("content token count exceeds u32".into())
                })?;
                self.content_string_bytes = self
                    .content_string_bytes
                    .checked_add(token.capacity() as u64)
                    .ok_or_else(|| {
                        AnalysisError::InvalidData(
                            "content token resident accounting overflow".into(),
                        )
                    })?;
                self.content_ids.insert(token, next_id);
                next_id
            };
            content_terms.push((token_id, frequency));
        }
        content_terms.sort_unstable_by_key(|(token, _)| *token);
        template_terms.sort_unstable_by_key(|(token, _)| *token);
        Ok(EncodePayloadRow {
            template_terms,
            content_terms,
        })
    }

    #[allow(dead_code)]
    fn resident_bytes(&self) -> u64 {
        hash_map_capacity_bytes::<Arc<str>, u32>(self.template_ids.capacity())
            .unwrap_or(u64::MAX)
            .saturating_add(
                capacity_bytes::<Arc<str>>(self.template_tokens.capacity()).unwrap_or(u64::MAX),
            )
            .saturating_add(
                hash_map_capacity_bytes::<String, u32>(self.content_ids.capacity())
                    .unwrap_or(u64::MAX),
            )
            .saturating_add(self.template_string_bytes)
            .saturating_add(self.content_string_bytes)
    }
}

/// Hash-sharded term dictionaries with globally unique arbitrary IDs.
/// Template and content stay separate; no lexical ID finalize pass.
struct ShardedPayloadTermInterner {
    template_shards: Vec<Mutex<HashMap<Arc<str>, u32>>>,
    content_shards: Vec<Mutex<HashMap<String, u32>>>,
    next_template_id: AtomicU32,
    next_content_id: AtomicU32,
    template_string_bytes: AtomicU64,
    content_string_bytes: AtomicU64,
    shard_mask: usize,
}

struct PendingPayloadTerms {
    template: Vec<(String, u32)>,
    content: Vec<(String, u32)>,
}

struct PendingTermRequest {
    payload: usize,
    token: String,
    frequency: u32,
}

impl ShardedPayloadTermInterner {
    fn with_shard_count(shard_count: usize) -> Self {
        let shard_count = shard_count.next_power_of_two().max(1);
        Self {
            template_shards: (0..shard_count)
                .map(|_| Mutex::new(HashMap::new()))
                .collect(),
            content_shards: (0..shard_count)
                .map(|_| Mutex::new(HashMap::new()))
                .collect(),
            next_template_id: AtomicU32::new(0),
            next_content_id: AtomicU32::new(0),
            template_string_bytes: AtomicU64::new(0),
            content_string_bytes: AtomicU64::new(0),
            shard_mask: shard_count - 1,
        }
    }

    fn shard_for(token: &str, shard_mask: usize) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        token.hash(&mut hasher);
        (hasher.finish() as usize) & shard_mask
    }

    #[allow(dead_code)]
    fn intern(&self, parsed: ParsedMetadataDocuments) -> Result<PayloadTermLists, AnalysisError> {
        self.intern_batch(vec![parsed])?
            .pop()
            .ok_or_else(|| AnalysisError::InvalidData("missing interned payload".into()))
    }

    /// Intern a parse batch with at most one lock acquisition per non-empty
    /// dictionary shard and dimension.
    fn intern_batch(
        &self,
        parsed_batch: Vec<ParsedMetadataDocuments>,
    ) -> Result<PayloadTermListBatch, AnalysisError> {
        let payload_count = parsed_batch.len();
        let pending = parsed_batch
            .into_par_iter()
            .map(|parsed| PendingPayloadTerms {
                template: string_term_frequencies(parsed.prefilter_tokens),
                content: string_term_frequencies(parsed.content_tokens),
            })
            .collect::<Vec<_>>();
        let shard_count = self.shard_mask + 1;
        let mut template_requests = (0..shard_count)
            .map(|_| Vec::<PendingTermRequest>::new())
            .collect::<Vec<_>>();
        let mut content_requests = (0..shard_count)
            .map(|_| Vec::<PendingTermRequest>::new())
            .collect::<Vec<_>>();
        for (payload, pending) in pending.into_iter().enumerate() {
            for (token, frequency) in pending.template {
                let shard = Self::shard_for(&token, self.shard_mask);
                template_requests[shard].push(PendingTermRequest {
                    payload,
                    token,
                    frequency,
                });
            }
            for (token, frequency) in pending.content {
                let shard = Self::shard_for(&token, self.shard_mask);
                content_requests[shard].push(PendingTermRequest {
                    payload,
                    token,
                    frequency,
                });
            }
        }

        let template_results = template_requests
            .into_par_iter()
            .enumerate()
            .map(|(shard, requests)| {
                if requests.is_empty() {
                    return Ok(Vec::new());
                }
                let mut map = self.template_shards[shard].lock().map_err(|_| {
                    AnalysisError::InvalidData("template shard lock poisoned".into())
                })?;
                let mut resolved = Vec::with_capacity(requests.len());
                for request in requests {
                    let token_id = if let Some(&token_id) = map.get(request.token.as_str()) {
                        token_id
                    } else {
                        let token_id = self.next_template_id.fetch_add(1, Ordering::Relaxed);
                        if token_id == u32::MAX {
                            return Err(AnalysisError::InvalidData(
                                "template token count exceeds u32".into(),
                            ));
                        }
                        let bytes = (request.token.len() as u64)
                            .checked_add(2 * std::mem::size_of::<usize>() as u64)
                            .ok_or_else(|| {
                                AnalysisError::InvalidData(
                                    "template token resident accounting overflow".into(),
                                )
                            })?;
                        self.template_string_bytes
                            .fetch_add(bytes, Ordering::Relaxed);
                        let token: Arc<str> = Arc::from(request.token.as_str());
                        map.insert(token, token_id);
                        token_id
                    };
                    resolved.push((request.payload, token_id, request.frequency));
                }
                Ok(resolved)
            })
            .collect::<Result<Vec<_>, AnalysisError>>()?;
        let content_results = content_requests
            .into_par_iter()
            .enumerate()
            .map(|(shard, requests)| {
                if requests.is_empty() {
                    return Ok(Vec::new());
                }
                let mut map = self.content_shards[shard].lock().map_err(|_| {
                    AnalysisError::InvalidData("content shard lock poisoned".into())
                })?;
                let mut resolved = Vec::with_capacity(requests.len());
                for request in requests {
                    let token_id = if let Some(&token_id) = map.get(&request.token) {
                        token_id
                    } else {
                        let token_id = self.next_content_id.fetch_add(1, Ordering::Relaxed);
                        if token_id == u32::MAX {
                            return Err(AnalysisError::InvalidData(
                                "content token count exceeds u32".into(),
                            ));
                        }
                        self.content_string_bytes
                            .fetch_add(request.token.capacity() as u64, Ordering::Relaxed);
                        map.insert(request.token, token_id);
                        token_id
                    };
                    resolved.push((request.payload, token_id, request.frequency));
                }
                Ok(resolved)
            })
            .collect::<Result<Vec<_>, AnalysisError>>()?;

        let mut output = (0..payload_count)
            .map(|_| (Vec::new(), Vec::new()))
            .collect::<PayloadTermListBatch>();
        for (payload, token_id, frequency) in template_results.into_iter().flatten() {
            output[payload].0.push((token_id, frequency));
        }
        for (payload, token_id, frequency) in content_results.into_iter().flatten() {
            output[payload].1.push((token_id, frequency));
        }
        output.par_iter_mut().for_each(|(template, content)| {
            template.sort_unstable_by_key(|(token_id, _)| *token_id);
            content.sort_unstable_by_key(|(token_id, _)| *token_id);
        });
        Ok(output)
    }
}

fn string_term_frequencies(mut tokens: Vec<String>) -> Vec<(String, u32)> {
    tokens.sort_unstable();
    let mut frequencies: Vec<(String, u32)> = Vec::with_capacity(tokens.len());
    for token in tokens {
        if let Some((previous, count)) = frequencies.last_mut() {
            if *previous == token {
                *count = count.saturating_add(1);
                continue;
            }
        }
        frequencies.push((token, 1u32));
    }
    frequencies
}

#[derive(Clone, Serialize)]
struct ArtifactFingerprintRecord {
    path: PathBuf,
    size: u64,
    row_count: Option<u64>,
    sha256: String,
}

fn write_phase_ready_marker(
    work_directory: &Path,
    artifacts: Vec<ArtifactFingerprintRecord>,
) -> Result<(), AnalysisError> {
    #[derive(Serialize)]
    struct PhaseReady<'a> {
        phase: &'a str,
        partial_file: &'a str,
        size: u64,
        sha256: String,
        artifacts: Vec<ArtifactFingerprintRecord>,
    }

    let partial_file = AnalysisPhase::MetadataEncode.partial_file_name();
    let partial_path = work_directory.join("partial").join(partial_file);
    let (size, sha256) = sha256_file(&partial_path, 1024 * 1024)?;
    let ready = PhaseReady {
        phase: "metadata-encode",
        partial_file,
        size,
        sha256,
        artifacts,
    };
    let directory = work_directory.join("checkpoints");
    fs::create_dir_all(&directory)?;
    write_json_atomically(&ready, &directory.join("metadata-encode.ready.json"))
        .map_err(AnalysisError::from)
}

fn fingerprint_bundle_files(
    roots: &[&Path],
) -> Result<Vec<ArtifactFingerprintRecord>, AnalysisError> {
    let mut paths = Vec::new();
    for root in roots {
        collect_files(root, &mut paths)?;
    }
    paths.sort();
    paths
        .into_iter()
        .map(|path| {
            let path = path.canonicalize()?;
            let (size, sha256) = if let Some((size, checksum)) =
                metadata_engine::format::typed_array_footer_fingerprint(&path)
                    .map_err(encode_err)?
            {
                (
                    size,
                    format!(
                        "{}{}",
                        metadata_engine::format::TYPED_ARRAY_CHECKSUM_PREFIX,
                        checksum
                    ),
                )
            } else {
                sha256_file(&path, 8 * 1024 * 1024)?
            };
            Ok(ArtifactFingerprintRecord {
                path,
                size,
                row_count: None,
                sha256,
            })
        })
        .collect()
}

fn path_is_under_payload_blobs(path: &Path) -> bool {
    path.components()
        .any(|component| component.as_os_str() == "payload_blobs")
}

fn remove_stale_encode_payload_blobs(encode_dir: &Path) -> Result<(), AnalysisError> {
    let blobs = encode_dir.join("payload_blobs");
    if blobs.exists() {
        fs::remove_dir_all(&blobs)?;
    }
    Ok(())
}

fn collect_files(path: &Path, output: &mut Vec<PathBuf>) -> Result<(), AnalysisError> {
    if path.is_file() {
        output.push(path.to_path_buf());
        return Ok(());
    }
    for entry in fs::read_dir(path)? {
        collect_files(&entry?.path(), output)?;
    }
    Ok(())
}

pub(super) fn estimate_encode_storage_bytes(
    conn: &Connection,
) -> Result<EncodeAdmissionEstimate, AnalysisError> {
    let (source_rows, raw_bytes): (u64, u64) = conn.query_row(
        "SELECT count(*)::UBIGINT,
                coalesce(sum(contracts.metadata_max_json_bytes), 0)::UBIGINT
         FROM analysis_contracts contracts
         WHERE contracts.metadata_contract_index IS NOT NULL",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let (token_rows, token_json_bytes) = token_source_relation_dimensions(conn)?;
    let final_bytes = raw_bytes
        .checked_mul(16)
        .and_then(|bytes| bytes.checked_add(source_rows.checked_mul(2_048)?))
        .and_then(|bytes| bytes.checked_add(token_rows.checked_mul(32)?))
        .and_then(|bytes| bytes.checked_add(64 * 1024 * 1024))
        .ok_or_else(|| AnalysisError::InvalidData("Encode storage estimate overflow".into()))?;
    let token_relation_peak_bytes =
        planned_token_relation_peak(token_rows, source_rows, token_json_bytes)?;
    let partial_peak_bytes = ENCODE_RESIDENT_FIXED_BYTES;
    let modeled_resident_peak = raw_bytes
        .checked_mul(4)
        .and_then(|bytes| bytes.checked_add(source_rows.checked_mul(2_048)?))
        .and_then(|bytes| bytes.checked_add(token_rows.checked_mul(24)?))
        .and_then(|bytes| bytes.checked_add(token_json_bytes))
        .and_then(|bytes| bytes.checked_add(64 * 1024 * 1024))
        .ok_or_else(|| AnalysisError::InvalidData("Encode memory estimate overflow".into()))?;
    // The global payload/interner/CSR state grows with all unique small
    // documents, not just the largest contract. Use the complete conservative
    // durable envelope as the global resident admission floor; this avoids a
    // second JSON preflight while covering high-cardinality payload/term maps.
    let resident_peak_bytes = modeled_resident_peak
        .max(final_bytes)
        .max(token_relation_peak_bytes);
    let provisional_feature_bytes = final_bytes;
    Ok(EncodeAdmissionEstimate {
        final_bytes,
        provisional_feature_bytes,
        resident_peak_bytes,
        partial_peak_bytes,
        token_relation_peak_bytes,
        representative_rows: source_rows,
        token_rows,
    })
}

fn blocking_contract_expansion_pair_work(
    blocking: &metadata_engine::blocking::BlockingBundle,
    fallback_atoms: &FallbackAtomCsr,
) -> Result<u64, AnalysisError> {
    let mut total = 0u64;
    for block in 0..blocking.block_kinds.len() {
        let begin = blocking.block_atom_offsets[block] as usize;
        let end = blocking.block_atom_offsets[block + 1] as usize;
        let mut prefix = 0u64;
        for &atom in &blocking.block_atoms[begin..end] {
            let members = fallback_atoms.members_of(atom as usize).len() as u64;
            total = total
                .checked_add(prefix.checked_mul(members).ok_or_else(|| {
                    AnalysisError::InvalidData("blocking contract expansion work overflow".into())
                })?)
                .ok_or_else(|| {
                    AnalysisError::InvalidData("blocking contract expansion work overflow".into())
                })?;
            prefix = prefix.checked_add(members).ok_or_else(|| {
                AnalysisError::InvalidData("blocking contract membership overflow".into())
            })?;
        }
    }
    Ok(total)
}

fn millis_since(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn storage_err(err: impl std::fmt::Display) -> AnalysisError {
    AnalysisError::InvalidData(format!("storage broker: {err}"))
}

fn encode_err(err: impl std::fmt::Display) -> AnalysisError {
    AnalysisError::InvalidData(format!("metadata encode: {err}"))
}

fn blocking_err(err: impl std::fmt::Display) -> AnalysisError {
    AnalysisError::InvalidData(format!("blocking compile: {err}"))
}

fn format_err(err: impl std::fmt::Display) -> AnalysisError {
    AnalysisError::InvalidData(format!("artifact ready: {err}"))
}

#[cfg(test)]
mod memory_dedup_tests {
    use std::collections::HashMap;

    use super::{
        build_fallback_atoms_hash_sharded, intern_payload_with_parser,
        payload_feature_identity_ids, planned_encoded_contract_growth, planned_token_relation_peak,
        EncodePayloadRow, EncodeResidentAccounting, EncodeResidentAdmission, PayloadTermInterner,
        ShardedPayloadTermInterner, TokenSourceInput,
    };
    use metadata_engine::encode::{
        parse_metadata_documents, EncodeContractRow, EncodeContractSoA, EncodeSourceRow,
        PayloadArena, PayloadRef, PayloadTermSoA, ShardedPayloadArena,
    };
    use metadata_engine::resource::{MemoryBroker, GIB};
    #[test]
    fn payload_feature_identity_deduplicates_without_owning_term_vector_keys() {
        let payloads = vec![
            EncodePayloadRow {
                template_terms: vec![(1, 2)],
                content_terms: vec![(3, 4)],
            },
            EncodePayloadRow {
                template_terms: vec![(1, 2)],
                content_terms: vec![(3, 4)],
            },
            EncodePayloadRow {
                template_terms: vec![(9, 1)],
                content_terms: vec![],
            },
        ];
        let soa = PayloadTermSoA::from_rows(&payloads).unwrap();

        assert_eq!(payload_feature_identity_ids(&soa), vec![0, 0, 1]);
    }

    #[test]
    fn fallback_atom_members_are_canonicalized_before_persist() {
        let mut contracts = EncodeContractSoA::with_contract_capacity(4);
        for contract_id in [3, 2, 1, 0] {
            contracts.push_contract(contract_id, 0, contract_id, 0, 1);
        }

        let atoms = build_fallback_atoms_hash_sharded(&contracts, &[0], 1, |_| {}).unwrap();

        assert_eq!(atoms.atom_count(), 1);
        assert_eq!(atoms.members_of(0), &[0, 1, 2, 3]);
    }

    #[test]
    fn duplicate_payload_is_looked_up_in_cas_before_parsing_again() {
        let mut cas = PayloadArena::new(1024 * 1024);
        let mut payloads = Vec::new();
        let mut interner = PayloadTermInterner::default();
        let mut parse_calls = 0usize;
        let json = r#"{"description":"same payload"}"#;

        for _ in 0..2 {
            intern_payload_with_parser(json, &mut cas, &mut payloads, &mut interner, |raw| {
                parse_calls += 1;
                parse_metadata_documents(raw)
            })
            .unwrap();
        }

        assert_eq!(parse_calls, 1);
        assert_eq!(payloads.len(), 1);
    }

    #[test]
    fn batch_term_interning_reuses_ids_within_and_across_batches() {
        let interner = ShardedPayloadTermInterner::with_shard_count(4);
        let first = interner
            .intern_batch(vec![
                parse_metadata_documents(r#"{"name":"shared","description":"alpha"}"#),
                parse_metadata_documents(r#"{"name":"shared","description":"beta"}"#),
            ])
            .unwrap();
        let repeated = interner
            .intern_batch(vec![parse_metadata_documents(
                r#"{"name":"shared","description":"alpha"}"#,
            )])
            .unwrap();

        assert_eq!(first[0], repeated[0]);
        assert!(first[0]
            .0
            .iter()
            .any(|(id, _)| first[1].0.iter().any(|(other, _)| other == id)));
        assert!(first.iter().all(|(template, content)| {
            template.windows(2).all(|pair| pair[0].0 < pair[1].0)
                && content.windows(2).all(|pair| pair[0].0 < pair[1].0)
        }));
    }

    #[test]
    fn live_cardinality_admission_expands_for_unique_payload_and_interner_state() {
        let cas = ShardedPayloadArena::with_shard_count(4, 1024 * 1024);
        let mut payloads = Vec::new();
        let mut interner = PayloadTermInterner::default();
        for index in 0..4_096 {
            let json = format!(r#"{{"description":"unique payload term {index}"}}"#);
            let payload_ref = cas.insert(json.as_bytes()).unwrap();
            let offsets = cas.global_offsets().unwrap();
            let payload_id = cas.global_id(payload_ref, &offsets).unwrap();
            if payload_id as usize >= payloads.len() {
                payloads.push(interner.intern(parse_metadata_documents(&json)).unwrap());
            }
        }

        let sources = Vec::<EncodeSourceRow>::new();
        let contracts = Vec::<EncodeContractRow>::new();
        let pending = HashMap::new();
        let mut accounting = EncodeResidentAccounting::default();
        let resident_bytes = accounting
            .resident_bytes(
                &sources,
                &payloads,
                &contracts,
                Some(&interner),
                Some(&cas),
                &pending,
            )
            .unwrap();
        let broker = MemoryBroker::new(4 * GIB, 3 * GIB).unwrap();
        let lease = broker.reserve(1).unwrap();
        let mut admission = EncodeResidentAdmission::new(lease, 1);

        admission.commit(resident_bytes).unwrap();

        assert!(resident_bytes > 1);
        assert_eq!(admission.current_bytes(), resident_bytes);
        assert_eq!(admission.peak_bytes(), resident_bytes);
    }

    #[test]
    fn contract_growth_guard_includes_token_specific_json_and_memberships() {
        let sources = vec![
            TokenSourceInput {
                token_ids: vec![1, 2, 3],
                source_file: 1,
                source_row_number: 1,
                payload_ref: PayloadRef {
                    shard_id: 0,
                    local_id: 0,
                },
            },
            TokenSourceInput {
                token_ids: vec![4, 5],
                source_file: 1,
                source_row_number: 2,
                payload_ref: PayloadRef {
                    shard_id: 0,
                    local_id: 1,
                },
            },
        ];

        let guarded = planned_encoded_contract_growth("{}", &sources).unwrap();
        let representative_only = planned_encoded_contract_growth("{}", &[]).unwrap();

        assert!(guarded > representative_only);
        assert!(guarded >= 5 * std::mem::size_of::<u32>() as u64);
    }

    #[test]
    fn token_source_relation_admission_includes_json_and_fixed_width_rows() {
        let rows = 10_000u64;
        let distinct_json = rows * 128;
        let admitted_json = distinct_json * 5 / 4;

        assert_eq!(
            planned_token_relation_peak(rows, rows, distinct_json).unwrap(),
            rows * 192 + rows * 8 + admitted_json + 64 * 1024 * 1024
        );
    }
}
