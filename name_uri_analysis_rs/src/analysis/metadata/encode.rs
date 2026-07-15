//! MetadataEncode adapter: DuckDB stream → metadata_engine.
//!
//! Writes feature/blocking artifacts under `artifacts/metadata/`.
//! Never mutates Prepare/Name tables and never produces production summary rows.

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use duckdb::arrow::array::{
    Array, BooleanArray, StringArray, StringViewArray, UInt32Array, UInt64Array,
};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;
use metadata_engine::blocking::{
    build_base_equivalent_atom_sketches_parallel, compile_base_equivalent_parallel_with_progress,
    AtomSketch, BaseEquivalentAtomInput, BlockingCompileConfig, BLOCKING_REVISION,
    DEFAULT_MAX_ROUTING_BLOCK_MEMBERS,
};
use metadata_engine::encode::{
    parse_metadata_documents, write_encode_artifacts_with_contracts_and_atoms_with_progress,
    EncodeContractRow, EncodePayloadRow, EncodeSourceRow, ParsedMetadataDocuments,
    PayloadCasWriter, DEFAULT_MAX_PACK_BYTES, ENCODE_SCHEMA_REVISION,
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

const ENCODE_PARSE_BATCHES_PER_LANE: usize = 8;
const MAX_ENCODE_PARSE_BATCH_ROWS: usize = 4_096;
const ENCODE_RESIDENT_FIXED_BYTES: u64 = 64 * 1024 * 1024;
const HASH_BUCKET_OVERHEAD_BYTES: usize = 16;

type RepresentativeEncodeRow = (i64, String, String, i64, u32, u64);
type FallbackEncodeRow = (u32, String, u32, u64);

pub(super) fn resolve_fallback_contracts(
    rows: &[FallbackEncodeRow],
    parse: &(impl Fn(&str) -> ParsedMetadataDocuments + Sync),
) -> Vec<(FallbackEncodeRow, ParsedMetadataDocuments)> {
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
            rows[range].iter().find_map(|row| {
                let parsed = parse(&row.1);
                (!parsed.prefilter_tokens.is_empty()).then(|| (row.clone(), parsed))
            })
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
    fn resident_bytes(
        &mut self,
        sources: &Vec<EncodeSourceRow>,
        payloads: &Vec<EncodePayloadRow>,
        contracts: &Vec<EncodeContractRow>,
        payload_interner: Option<&PayloadTermInterner>,
        cas: Option<&PayloadCasWriter>,
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
            cas.map_or(0, PayloadCasWriter::resident_bytes),
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
        let artifact_layout =
            metadata_engine::artifacts::MetadataArtifactLayout::new(work_directory);
        let encode_dir = artifact_layout.encode_dir();
        fs::create_dir_all(&encode_dir)?;
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
        let encode_persist_stats = write_encode_artifacts_with_contracts_and_atoms_with_progress(
            &encode_dir,
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
            sources.len(),
            payloads.len()
        ));

        let blocking_started = Instant::now();
        let blocking_dir = artifact_layout.blocking_dir();
        fs::create_dir_all(&blocking_dir)?;
        let config = BlockingCompileConfig {
            max_routing_block_members: DEFAULT_MAX_ROUTING_BLOCK_MEMBERS,
        };
        let blocking_bundle = compile_base_equivalent_parallel_with_progress(
            &atoms,
            &config,
            &blocking_dir,
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
        let feature_manifest = serde_json::json!({
            "schema_revision": ENCODE_SCHEMA_REVISION,
            "source_count": sources.len(),
            "payload_count": payloads.len(),
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
        progress.observe_engine_event(ProgressEvent::indeterminate(
            ProgressPhase::EncodePublish,
            1,
            WorkUnit::Items,
            EngineCounters::default(),
        ));
        let artifact_fingerprints = fingerprint_bundle_files(&[&encode_dir, &blocking_dir])?;
        // Payload CAS is a transient Match input, not an Encode checkpoint
        // dependency. Keeping it out of the ready marker lets a later Match
        // revision reuse the immutable Encode snapshot after CAS collection.
        let checkpoint_artifact_fingerprints = artifact_fingerprints
            .iter()
            .filter(|artifact| {
                !artifact
                    .path
                    .components()
                    .any(|component| component.as_os_str() == "payload_blobs")
            })
            .cloned()
            .collect();
        progress.observe_engine_event(ProgressEvent::indeterminate(
            ProgressPhase::EncodePublish,
            2,
            WorkUnit::Items,
            EngineCounters::default(),
        ));
        drop(storage_reservation);

        let payload_blobs = encode_dir.join("payload_blobs").canonicalize()?;
        let blocking_root = blocking_dir.canonicalize()?;
        let mut registrations = vec![ArtifactRegistration::new(
            payload_blobs.clone(),
            ArtifactClass::PayloadCas,
            directory_bytes(&payload_blobs)?,
            0,
            Vec::new(),
        )];
        let mut registered = vec![payload_blobs.clone()];
        for path in artifact_fingerprints.iter().map(|artifact| &artifact.path) {
            if path.starts_with(&payload_blobs) {
                continue;
            }
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
                source_rows: sources.len() as u64,
                payload_count: payloads.len() as u64,
                contract_count: contracts.len() as u64,
                atom_count: atoms.len() as u64,
                template_term_count: payloads
                    .iter()
                    .map(|payload| payload.template_terms.len() as u64)
                    .sum(),
                content_term_count: payloads
                    .iter()
                    .map(|payload| payload.content_terms.len() as u64)
                    .sum(),
                token_membership_count: sources
                    .iter()
                    .map(|source| source.retained_token_ids.len() as u64)
                    .sum(),
                routing_membership_count: atoms
                    .iter()
                    .map(|atom| {
                        atom.template_anchors.len() as u64 + atom.content_anchors.len() as u64
                    })
                    .sum(),
                fallback_membership_count: fallback_atoms
                    .iter()
                    .map(|members| members.len() as u64)
                    .sum(),
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
    Vec<EncodeSourceRow>,
    Vec<EncodePayloadRow>,
    Vec<EncodeContractRow>,
    Vec<AtomSketch>,
    Vec<Vec<u32>>,
    Vec<EncodeChainTotal>,
);

#[derive(Debug)]
struct TokenSourceInput {
    token_ids: Vec<u32>,
    source_file: u32,
    source_row_number: u64,
    metadata_json: Arc<str>,
}

#[derive(Debug)]
struct TokenSourceRecord {
    source_file: u32,
    source_row_number: u64,
    metadata_json: Arc<str>,
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
                metadata_json: Arc::clone(&source.metadata_json),
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
    token_json_bytes: u64,
) -> Result<u64, AnalysisError> {
    token_rows
        .checked_mul(64)
        .and_then(|bytes| representative_rows.checked_mul(8)?.checked_add(bytes))
        .and_then(|bytes| token_json_bytes.checked_add(bytes))
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

fn planned_encoded_contract_growth(
    representative_json: &str,
    token_sources: &[TokenSourceInput],
) -> Result<u64, AnalysisError> {
    let json_bytes = token_sources.iter().try_fold(
        u64::try_from(representative_json.len()).map_err(|_| {
            AnalysisError::InvalidData("Encode representative JSON exceeds u64".into())
        })?,
        |total, source| total.checked_add(source.metadata_json.len() as u64),
    );
    let membership_count = token_sources.iter().try_fold(0u64, |total, source| {
        total.checked_add(source.token_ids.len() as u64)
    });
    let source_count = u64::try_from(token_sources.len())
        .ok()
        .and_then(|count| count.checked_add(1));
    json_bytes
        .and_then(|bytes| bytes.checked_mul(16))
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
        hash_map_capacity_bytes::<(u32, u32), usize>(contract_count)?,
        capacity_bytes::<u32>(contract_count)?,
        capacity_bytes::<Vec<u32>>(contract_count)?,
        capacity_bytes::<BaseEquivalentAtomInput<'static>>(contract_count)?,
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
    sources: &Vec<EncodeSourceRow>,
    payloads: &Vec<EncodePayloadRow>,
    contracts: &Vec<EncodeContractRow>,
    atoms: &Vec<AtomSketch>,
    fallback_atoms: &Vec<Vec<u32>>,
) -> Result<u64, AnalysisError> {
    let mut total = ENCODE_RESIDENT_FIXED_BYTES;
    for bytes in [
        capacity_bytes::<EncodeSourceRow>(sources.capacity())?,
        capacity_bytes::<EncodePayloadRow>(payloads.capacity())?,
        capacity_bytes::<EncodeContractRow>(contracts.capacity())?,
        capacity_bytes::<AtomSketch>(atoms.capacity())?,
        capacity_bytes::<Vec<u32>>(fallback_atoms.capacity())?,
    ] {
        total = total.checked_add(bytes).ok_or_else(|| {
            AnalysisError::InvalidData("Encode frozen resident accounting overflow".into())
        })?;
    }
    for source in sources {
        total = total
            .checked_add(capacity_bytes::<u32>(source.retained_token_ids.capacity())?)
            .ok_or_else(|| {
                AnalysisError::InvalidData("Encode source token accounting overflow".into())
            })?;
    }
    for payload in payloads {
        for capacity in [
            payload.template_terms.capacity(),
            payload.content_terms.capacity(),
        ] {
            total = total
                .checked_add(capacity_bytes::<(u32, u32)>(capacity)?)
                .ok_or_else(|| {
                    AnalysisError::InvalidData("Encode payload accounting overflow".into())
                })?;
        }
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
    for members in fallback_atoms {
        total = total
            .checked_add(capacity_bytes::<u32>(members.capacity())?)
            .ok_or_else(|| {
                AnalysisError::InvalidData("Encode fallback accounting overflow".into())
            })?;
    }
    Ok(total)
}

fn planned_feature_persist_growth(
    sources: &[EncodeSourceRow],
    payloads: &[EncodePayloadRow],
    contracts: &[EncodeContractRow],
) -> Result<u64, AnalysisError> {
    let mut occurrences = 0u64;
    let mut max_token = None::<u32>;
    for source in sources {
        occurrences = occurrences
            .checked_add(u64::try_from(source.retained_token_ids.len()).map_err(|_| {
                AnalysisError::InvalidData("Encode CSR occurrence count exceeds u64".into())
            })?)
            .ok_or_else(|| AnalysisError::InvalidData("Encode CSR occurrence overflow".into()))?;
        for &token in &source.retained_token_ids {
            max_token = Some(max_token.map_or(token, |current| current.max(token)));
        }
    }
    let token_count = max_token.map_or(0u64, |token| u64::from(token) + 1);
    let contract_count = u64::try_from(contracts.len())
        .map_err(|_| AnalysisError::InvalidData("Encode contract count exceeds u64".into()))?;
    let source_count = u64::try_from(sources.len())
        .map_err(|_| AnalysisError::InvalidData("Encode source count exceeds u64".into()))?;
    let template_terms = payloads.iter().try_fold(0u64, |total, payload| {
        total.checked_add(payload.template_terms.len() as u64)
    });
    let payload_count = u64::try_from(payloads.len())
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
        .and_then(|bytes| payload_count.checked_mul(32)?.checked_add(bytes))
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
                coalesce(sum(metadata_max_json_bytes), 0)::UBIGINT
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
    work_directory: &Path,
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
    let parse_batch_rows = parse_lanes
        .saturating_mul(ENCODE_PARSE_BATCHES_PER_LANE)
        .clamp(1, MAX_ENCODE_PARSE_BATCH_ROWS);
    let parse_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(parse_lanes)
        .thread_name(|index| format!("metadata-encode-parse-{index}"))
        .build()
        .map_err(|error| AnalysisError::InvalidData(format!("encode parse pool: {error}")))?;
    let token_source_relation = build_retained_token_source_relation(
        conn,
        contract_count,
        estimate.token_rows,
        &parse_pool,
        &mut progress,
    )?;
    if token_source_relation.bytes() > estimate.resident_peak_bytes {
        return Err(AnalysisError::InvalidData(format!(
            "in-memory token-source relation exceeded resident admission ({} > {})",
            token_source_relation.bytes(),
            estimate.resident_peak_bytes
        )));
    }
    let encode_dir =
        metadata_engine::artifacts::MetadataArtifactLayout::new(work_directory).encode_dir();
    let payload_blobs = encode_dir.join("payload_blobs");
    let mut cas =
        PayloadCasWriter::create(&payload_blobs, DEFAULT_MAX_PACK_BYTES).map_err(encode_err)?;

    let mut sources = Vec::new();
    let mut payloads = Vec::new();
    let mut payload_interner = PayloadTermInterner::default();
    let mut contracts = Vec::new();
    let chain_totals = load_encode_chain_totals(conn)?;
    let chain_ids = chain_totals
        .iter()
        .enumerate()
        .map(|(index, total)| {
            u32::try_from(index)
                .map(|index| (total.name.clone(), index))
                .map_err(|_| AnalysisError::InvalidData("chain count exceeds u32".into()))
        })
        .collect::<Result<HashMap<_, _>, _>>()?;

    let mut stmt = conn.prepare(
        "SELECT contracts.metadata_contract_index,
                contracts.chain,
                rows.metadata_json,
                contracts.nft_count,
                contracts.metadata_source_file,
                contracts.metadata_source_row_number
         FROM analysis_contracts contracts
         JOIN metadata_rows rows
           ON rows.source_file = contracts.metadata_source_file
          AND rows.source_row_number = contracts.metadata_source_row_number
         WHERE contracts.metadata_contract_index IS NOT NULL
         ORDER BY contracts.metadata_contract_index",
    )?;
    let mut rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, i64>(3)?,
            row.get::<_, u32>(4)?,
            row.get::<_, u64>(5)?,
        ))
    })?;

    progress(ProgressEvent::determinate(
        ProgressPhase::EncodeRows,
        0,
        estimate.representative_rows,
        WorkUnit::Items,
        EngineCounters::default(),
    ));
    let mut representative_rows = 0u64;
    let mut pending_fallbacks = HashMap::<u32, PendingFallbackContract>::new();
    let mut resident_accounting = EncodeResidentAccounting::default();
    let mut committed_resident_bytes = resident_accounting.resident_bytes(
        &sources,
        &payloads,
        &contracts,
        Some(&payload_interner),
        Some(&cas),
        &pending_fallbacks,
    )?;
    resident_admission.commit(committed_resident_bytes)?;
    loop {
        let batch = rows
            .by_ref()
            .take(parse_batch_rows)
            .collect::<Result<Vec<RepresentativeEncodeRow>, _>>()?;
        if batch.is_empty() {
            break;
        }
        let batch_json_bytes = batch
            .iter()
            .try_fold(0u64, |total, row| total.checked_add(row.2.len() as u64))
            .ok_or_else(|| AnalysisError::InvalidData("Encode batch JSON bytes overflow".into()))?;
        let batch_growth_bytes = planned_encode_batch_growth(batch_json_bytes, batch.len())?;
        resident_admission.reserve_growth(committed_resident_bytes, batch_growth_bytes)?;
        let parsed_batch = parse_pool.install(|| {
            batch
                .par_iter()
                .map(|row| {
                    metadata_is_dedup_eligible(&row.2).then(|| parse_metadata_documents(&row.2))
                })
                .collect::<Vec<_>>()
        });
        for (row, parsed) in batch.into_iter().zip(parsed_batch) {
            let (
                contract_index_i64,
                chain,
                metadata_json,
                nft_count,
                representative_file,
                representative_row,
            ) = row;
            representative_rows = representative_rows.saturating_add(1);
            let source_contract_index = u32::try_from(contract_index_i64).map_err(|_| {
                AnalysisError::InvalidData("metadata_contract_index out of u32 range".into())
            })?;
            if !metadata_is_dedup_eligible(&metadata_json) {
                emit_encode_progress(
                    &mut progress,
                    ProgressPhase::EncodeRows,
                    representative_rows,
                    estimate.representative_rows,
                );
                continue;
            }
            let parsed = parsed.ok_or_else(|| {
                AnalysisError::InvalidData(
                    "eligible metadata row was omitted from parallel parse batch".into(),
                )
            })?;
            let chain_id = *chain_ids.get(&chain).ok_or_else(|| {
                AnalysisError::InvalidData(format!(
                    "metadata chain {chain:?} missing selected-chain id"
                ))
            })?;

            let weight = u64::try_from(nft_count).map_err(|_| {
                AnalysisError::InvalidData("negative metadata contract nft_count".into())
            })?;
            if parsed.prefilter_tokens.is_empty() {
                pending_fallbacks.insert(
                    source_contract_index,
                    PendingFallbackContract {
                        source_contract_index,
                        chain_id,
                        weight,
                    },
                );
            } else {
                let token_sources = token_source_relation.read_contract(source_contract_index)?;
                let contract_growth_bytes =
                    planned_encoded_contract_growth(&metadata_json, &token_sources)?;
                resident_admission.reserve_growth(
                    committed_resident_bytes,
                    batch_growth_bytes
                        .checked_add(contract_growth_bytes)
                        .ok_or_else(|| {
                            AnalysisError::InvalidData(
                                "Encode batch and contract admission overflow".into(),
                            )
                        })?,
                )?;
                append_encoded_contract(
                    chain_id,
                    weight,
                    representative_file,
                    representative_row,
                    &metadata_json,
                    parsed,
                    token_sources,
                    &mut cas,
                    &mut payloads,
                    &mut payload_interner,
                    &mut sources,
                    &mut contracts,
                )?;
                committed_resident_bytes = resident_accounting.resident_bytes(
                    &sources,
                    &payloads,
                    &contracts,
                    Some(&payload_interner),
                    Some(&cas),
                    &pending_fallbacks,
                )?;
                resident_admission.reserve_growth(committed_resident_bytes, batch_growth_bytes)?;
            }
            emit_encode_progress(
                &mut progress,
                ProgressPhase::EncodeRows,
                representative_rows,
                estimate.representative_rows,
            );
        }
        committed_resident_bytes = resident_accounting.resident_bytes(
            &sources,
            &payloads,
            &contracts,
            Some(&payload_interner),
            Some(&cas),
            &pending_fallbacks,
        )?;
        resident_admission.commit(committed_resident_bytes)?;
    }
    drop(stmt);

    if !pending_fallbacks.is_empty() {
        conn.execute_batch(
            "DROP TABLE IF EXISTS encode_fallback_contracts;
             CREATE TEMP TABLE encode_fallback_contracts(contract_index UINTEGER PRIMARY KEY);",
        )?;
        {
            let mut appender = conn.appender("encode_fallback_contracts")?;
            appender.append_rows(
                pending_fallbacks
                    .keys()
                    .copied()
                    .map(|contract_index| [contract_index]),
            )?;
        }
        let fallback_total: u64 = conn.query_row(
            "SELECT count(*)::UBIGINT
             FROM encode_fallback_contracts fallback
             JOIN analysis_contracts contracts
               ON contracts.metadata_contract_index = fallback.contract_index
             JOIN metadata_rows rows ON rows.contract_id = contracts.contract_id
             WHERE rows.metadata_eligible",
            [],
            |row| row.get(0),
        )?;
        progress(ProgressEvent::determinate(
            ProgressPhase::EncodeFallbackSources,
            0,
            fallback_total,
            WorkUnit::Items,
            EngineCounters::default(),
        ));
        let mut stmt = conn.prepare(
            "SELECT fallback.contract_index,
                    rows.metadata_json,
                    rows.source_file,
                    rows.source_row_number
             FROM encode_fallback_contracts fallback
             JOIN analysis_contracts contracts
               ON contracts.metadata_contract_index = fallback.contract_index
             JOIN metadata_rows rows ON rows.contract_id = contracts.contract_id
             WHERE rows.metadata_eligible
             ORDER BY fallback.contract_index,
                      rows.token_id,
                      rows.source_file,
                      rows.source_row_number",
        )?;
        let fallback_rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, u32>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, u32>(2)?,
                row.get::<_, u64>(3)?,
            ))
        })?;
        let fallback_rows = fallback_rows.collect::<Result<Vec<FallbackEncodeRow>, _>>()?;
        let fallback_json_bytes = fallback_rows
            .iter()
            .try_fold(0u64, |total, row| total.checked_add(row.1.len() as u64))
            .ok_or_else(|| {
                AnalysisError::InvalidData("Encode fallback JSON bytes overflow".into())
            })?;
        let fallback_growth_bytes =
            planned_encode_batch_growth(fallback_json_bytes, fallback_rows.len())?;
        resident_admission.reserve_growth(committed_resident_bytes, fallback_growth_bytes)?;
        let resolved = parse_pool
            .install(|| resolve_fallback_contracts(&fallback_rows, &parse_metadata_documents));
        drop(fallback_rows);
        for (row, parsed) in resolved {
            let (source_contract_index, metadata_json, source_file, source_row_number) = row;
            let Some(pending) = pending_fallbacks.remove(&source_contract_index) else {
                continue;
            };
            let token_sources =
                token_source_relation.read_contract(pending.source_contract_index)?;
            let contract_growth_bytes =
                planned_encoded_contract_growth(&metadata_json, &token_sources)?;
            resident_admission.reserve_growth(
                committed_resident_bytes,
                fallback_growth_bytes
                    .checked_add(contract_growth_bytes)
                    .ok_or_else(|| {
                        AnalysisError::InvalidData(
                            "Encode fallback and contract admission overflow".into(),
                        )
                    })?,
            )?;
            append_encoded_contract(
                pending.chain_id,
                pending.weight,
                source_file,
                source_row_number,
                &metadata_json,
                parsed,
                token_sources,
                &mut cas,
                &mut payloads,
                &mut payload_interner,
                &mut sources,
                &mut contracts,
            )?;
            committed_resident_bytes = resident_accounting.resident_bytes(
                &sources,
                &payloads,
                &contracts,
                Some(&payload_interner),
                Some(&cas),
                &pending_fallbacks,
            )?;
            resident_admission.reserve_growth(committed_resident_bytes, fallback_growth_bytes)?;
        }
        resident_admission.commit(committed_resident_bytes)?;
        emit_encode_progress(
            &mut progress,
            ProgressPhase::EncodeFallbackSources,
            fallback_total,
            fallback_total,
        );
        conn.execute_batch("DROP TABLE encode_fallback_contracts")?;
    }
    drop(token_source_relation);

    let finalize_total = 3u64
        .saturating_add(payloads.len() as u64)
        .saturating_add(contracts.len() as u64);
    progress(ProgressEvent::determinate(
        ProgressPhase::EncodeFinalize,
        0,
        finalize_total,
        WorkUnit::Items,
        EngineCounters::default(),
    ));
    resident_admission.reserve_growth(
        committed_resident_bytes,
        planned_encode_finalize_growth(payloads.len(), contracts.len())?,
    )?;
    cas.finish().map_err(encode_err)?;
    let mut finalized = 1u64;
    payload_interner.finalize_template_lexical_ids(&mut payloads);
    drop(payload_interner);
    finalized = finalized.saturating_add(1);
    emit_encode_progress(
        &mut progress,
        ProgressPhase::EncodeFinalize,
        finalized,
        finalize_total,
    );

    let payload_feature_identity = payload_feature_identity_ids(&payloads);
    for _ in &payloads {
        finalized = finalized.saturating_add(1);
        emit_encode_progress(
            &mut progress,
            ProgressPhase::EncodeFinalize,
            finalized,
            finalize_total,
        );
    }
    let mut atom_ids = HashMap::<(u32, u32), usize>::new();
    let mut atom_payloads = Vec::<u32>::new();
    let mut fallback_atoms = Vec::<Vec<u32>>::new();
    for contract in &contracts {
        let key = (
            contract.chain_id,
            payload_feature_identity[contract.payload_id as usize],
        );
        let atom_id = if let Some(&atom_id) = atom_ids.get(&key) {
            atom_id
        } else {
            let atom_id = fallback_atoms.len();
            atom_ids.insert(key, atom_id);
            atom_payloads.push(contract.payload_id);
            fallback_atoms.push(Vec::new());
            atom_id
        };
        fallback_atoms[atom_id].push(contract.contract_id);
        finalized = finalized.saturating_add(1);
        emit_encode_progress(
            &mut progress,
            ProgressPhase::EncodeFinalize,
            finalized,
            finalize_total,
        );
    }
    let atom_inputs: Vec<_> = atom_payloads
        .iter()
        .map(|&payload_id| {
            let payload = &payloads[payload_id as usize];
            BaseEquivalentAtomInput {
                template_terms: &payload.template_terms,
                content_terms: &payload.content_terms,
            }
        })
        .collect();
    let atoms = build_base_equivalent_atom_sketches_parallel(&atom_inputs, threads);
    finalized = finalized.saturating_add(1);
    emit_encode_progress(
        &mut progress,
        ProgressPhase::EncodeFinalize,
        finalized,
        finalize_total,
    );
    drop(atom_inputs);
    drop(atom_payloads);
    drop(atom_ids);
    drop(payload_feature_identity);
    drop(pending_fallbacks);
    resident_admission.commit(frozen_encode_state_resident_bytes(
        &sources,
        &payloads,
        &contracts,
        &atoms,
        &fallback_atoms,
    )?)?;

    Ok((
        sources,
        payloads,
        contracts,
        atoms,
        fallback_atoms,
        chain_totals,
    ))
}

fn payload_feature_identity_ids(payloads: &[EncodePayloadRow]) -> Vec<u32> {
    enum IdentityBucket {
        Single { payload_index: usize, identity: u32 },
        Collision(Vec<(usize, u32)>),
    }

    let mut buckets = HashMap::<u64, IdentityBucket>::new();
    let mut identities = Vec::with_capacity(payloads.len());
    let mut next_identity = 0u32;
    for (payload_index, payload) in payloads.iter().enumerate() {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        payload.template_terms.hash(&mut hasher);
        payload.content_terms.hash(&mut hasher);
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
                    let existing = &payloads[*representative];
                    if existing.template_terms == payload.template_terms
                        && existing.content_terms == payload.content_terms
                    {
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
                        let existing = &payloads[representative];
                        (existing.template_terms == payload.template_terms
                            && existing.content_terms == payload.content_terms)
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

fn intern_payload(
    metadata_json: &str,
    cas: &mut PayloadCasWriter,
    payloads: &mut Vec<EncodePayloadRow>,
    payload_interner: &mut PayloadTermInterner,
) -> Result<u32, AnalysisError> {
    intern_payload_with_parser(
        metadata_json,
        cas,
        payloads,
        payload_interner,
        parse_metadata_documents,
    )
}

fn intern_payload_with_parser(
    metadata_json: &str,
    cas: &mut PayloadCasWriter,
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

fn intern_parsed_payload(
    metadata_json: &str,
    parsed: ParsedMetadataDocuments,
    cas: &mut PayloadCasWriter,
    payloads: &mut Vec<EncodePayloadRow>,
    payload_interner: &mut PayloadTermInterner,
) -> Result<u32, AnalysisError> {
    let payload_id = cas.insert(metadata_json.as_bytes()).map_err(encode_err)?;
    if payload_id as usize >= payloads.len() {
        payloads.push(payload_interner.intern(parsed)?);
    }
    Ok(payload_id)
}

#[allow(clippy::too_many_arguments)]
fn append_encoded_contract(
    chain_id: u32,
    weight: u64,
    selected_source_file: u32,
    selected_source_row: u64,
    metadata_json: &str,
    parsed: ParsedMetadataDocuments,
    token_sources: Vec<TokenSourceInput>,
    cas: &mut PayloadCasWriter,
    payloads: &mut Vec<EncodePayloadRow>,
    payload_interner: &mut PayloadTermInterner,
    sources: &mut Vec<EncodeSourceRow>,
    contracts: &mut Vec<EncodeContractRow>,
) -> Result<(), AnalysisError> {
    let contract_id = u32::try_from(contracts.len())
        .map_err(|_| AnalysisError::InvalidData("metadata contract count exceeds u32".into()))?;
    let payload_id = intern_parsed_payload(metadata_json, parsed, cas, payloads, payload_interner)?;
    let mut selected_tokens = None::<Vec<u32>>;
    let mut remaining_sources = Vec::with_capacity(token_sources.len().saturating_sub(1));
    for source in token_sources {
        if source.source_file == selected_source_file
            && source.source_row_number == selected_source_row
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
    let source_doc_id = u32::try_from(sources.len())
        .map_err(|_| AnalysisError::InvalidData("metadata source count exceeds u32".into()))?;
    sources.push(EncodeSourceRow {
        contract_id,
        payload_id,
        retained_token_ids: selected_tokens.unwrap_or_default(),
    });
    for source in remaining_sources {
        let source_json = source.metadata_json;
        let mut source_tokens = source.token_ids;
        if source_tokens.windows(2).any(|pair| pair[0] >= pair[1]) {
            source_tokens.sort_unstable();
            source_tokens.dedup();
        }
        let source_payload_id = intern_payload(&source_json, cas, payloads, payload_interner)?;
        sources.push(EncodeSourceRow {
            contract_id,
            payload_id: source_payload_id,
            retained_token_ids: source_tokens,
        });
    }
    contracts.push(EncodeContractRow {
        contract_id,
        chain_id,
        source_doc_id,
        payload_id,
        weight,
    });
    Ok(())
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

fn build_retained_token_source_relation(
    conn: &Connection,
    contract_count: u32,
    expected_rows: u64,
    parse_pool: &rayon::ThreadPool,
    progress: &mut impl FnMut(ProgressEvent),
) -> Result<TokenSourceRelation, AnalysisError> {
    progress(ProgressEvent::indeterminate(
        ProgressPhase::EncodeCollectTokenSources,
        0,
        WorkUnit::Items,
        EngineCounters::default(),
    ));
    let exists: bool = conn.query_row(
        "SELECT count(*) > 0 FROM duckdb_tables() WHERE table_name = ?",
        ["metadata_contract_token_rows"],
        |row| row.get(0),
    )?;
    if !exists {
        return Ok(TokenSourceRelation {
            sources: Vec::new(),
            memberships: Vec::new(),
            contract_offsets: vec![0; contract_count as usize + 1],
            logical_bytes: 0,
        });
    }

    conn.execute_batch(
        "DROP TABLE IF EXISTS encode_token_source_candidates;
         DROP TABLE IF EXISTS encode_resolved_token_sources;
         DROP TABLE IF EXISTS encode_source_catalog;
         DROP TABLE IF EXISTS encode_fallback_source_catalog;
         DROP TABLE IF EXISTS encode_source_dictionary;
         DROP TABLE IF EXISTS encode_source_usability;
         DROP TABLE IF EXISTS encode_fallback_source_usability;
         CREATE TEMP TABLE encode_source_catalog AS
         SELECT DISTINCT token_rows.metadata_source_file::UINTEGER AS source_file,
                         token_rows.metadata_source_row_number::UBIGINT AS source_row_number,
                         metadata_rows.metadata_json
         FROM metadata_contract_token_rows token_rows
         JOIN metadata_rows
           ON metadata_rows.source_file = token_rows.metadata_source_file
          AND metadata_rows.source_row_number = token_rows.metadata_source_row_number;
         CREATE TEMP TABLE encode_source_usability(
             source_file UINTEGER,
             source_row_number UBIGINT,
             usable BOOLEAN
         );",
    )?;
    progress(ProgressEvent::indeterminate(
        ProgressPhase::EncodeCollectTokenSources,
        1,
        WorkUnit::Items,
        EngineCounters::default(),
    ));
    classify_source_catalog(
        conn,
        "encode_source_catalog",
        "encode_source_usability",
        ProgressPhase::EncodeTokenSources,
        parse_pool,
        progress,
    )?;

    progress(ProgressEvent::indeterminate(
        ProgressPhase::EncodeResolveTokenMemberships,
        0,
        WorkUnit::Items,
        EngineCounters::default(),
    ));
    conn.execute_batch(
        "CREATE TEMP TABLE encode_token_source_candidates AS
         SELECT token_rows.contract_index::UINTEGER AS contract_index,
                token_rows.token_index::UINTEGER AS token_index,
                token_rows.metadata_source_file AS source_file,
                token_rows.metadata_source_row_number AS source_row_number,
                usability.usable
         FROM metadata_contract_token_rows token_rows
         JOIN encode_source_usability usability
           ON usability.source_file = token_rows.metadata_source_file
          AND usability.source_row_number = token_rows.metadata_source_row_number;
         CREATE TEMP TABLE encode_resolved_token_sources AS
         SELECT contract_index, token_index, source_file, source_row_number
         FROM encode_token_source_candidates
         WHERE usable;",
    )?;
    progress(ProgressEvent::determinate(
        ProgressPhase::EncodeResolveTokenMemberships,
        expected_rows,
        expected_rows,
        WorkUnit::Items,
        EngineCounters::default(),
    ));

    progress(ProgressEvent::indeterminate(
        ProgressPhase::EncodeTokenFallbackSources,
        0,
        WorkUnit::Items,
        EngineCounters::default(),
    ));
    conn.execute_batch(
        "CREATE TEMP TABLE encode_fallback_source_catalog AS
         SELECT DISTINCT rows.source_file::UINTEGER AS source_file,
                         rows.source_row_number::UBIGINT AS source_row_number,
                         rows.metadata_json
         FROM encode_token_source_candidates fallback
         JOIN analysis_contracts contracts
           ON contracts.metadata_contract_index = fallback.contract_index
         JOIN metadata_token_dictionary dictionary
           ON dictionary.token_index = fallback.token_index
         JOIN metadata_rows rows
           ON rows.contract_id = contracts.contract_id
          AND rows.token_id = dictionary.token_id
         WHERE NOT fallback.usable
           AND rows.metadata_eligible;
         CREATE TEMP TABLE encode_fallback_source_usability(
             source_file UINTEGER,
             source_row_number UBIGINT,
             usable BOOLEAN
         );",
    )?;
    classify_source_catalog(
        conn,
        "encode_fallback_source_catalog",
        "encode_fallback_source_usability",
        ProgressPhase::EncodeTokenFallbackSources,
        parse_pool,
        progress,
    )?;
    conn.execute_batch(
        "INSERT INTO encode_resolved_token_sources
         SELECT contract_index, token_index, source_file, source_row_number
         FROM (
             SELECT fallback.contract_index,
                    fallback.token_index,
                    rows.source_file,
                    rows.source_row_number,
                    row_number() OVER (
                        PARTITION BY fallback.contract_index, fallback.token_index
                        ORDER BY rows.source_file, rows.source_row_number
                    ) AS source_rank
             FROM encode_token_source_candidates fallback
             JOIN analysis_contracts contracts
               ON contracts.metadata_contract_index = fallback.contract_index
             JOIN metadata_token_dictionary dictionary
               ON dictionary.token_index = fallback.token_index
             JOIN metadata_rows rows
               ON rows.contract_id = contracts.contract_id
              AND rows.token_id = dictionary.token_id
             JOIN encode_fallback_source_usability usability
               ON usability.source_file = rows.source_file
              AND usability.source_row_number = rows.source_row_number
             WHERE NOT fallback.usable
               AND rows.metadata_eligible
               AND usability.usable
         ) ranked
         WHERE source_rank = 1;",
    )?;

    conn.execute_batch(
        "DROP TABLE IF EXISTS encode_source_dictionary;
         CREATE TEMP TABLE encode_source_dictionary AS
         SELECT (row_number() OVER (
                    ORDER BY source_file, source_row_number
                ) - 1)::UINTEGER AS source_id,
                source_file,
                source_row_number
         FROM (
             SELECT DISTINCT resolved.source_file,
                             resolved.source_row_number
             FROM encode_resolved_token_sources resolved
         ) sources;",
    )?;
    let source_count: u64 = conn.query_row(
        "SELECT count(*)::UBIGINT FROM encode_source_dictionary",
        [],
        |row| row.get(0),
    )?;
    let membership_count: u64 = conn.query_row(
        "SELECT count(*)::UBIGINT FROM encode_resolved_token_sources",
        [],
        |row| row.get(0),
    )?;
    let source_count = u32::try_from(source_count).map_err(|_| {
        AnalysisError::InvalidData("token source dictionary exceeds u32 identity space".into())
    })?;
    let sources = load_token_source_records(conn, source_count, parse_pool, progress)?;
    let mut memberships = load_token_memberships(conn, membership_count, parse_pool, progress)?;
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
    conn.execute_batch(
        "DROP TABLE encode_token_source_candidates;
         DROP TABLE encode_resolved_token_sources;
         DROP TABLE encode_source_dictionary;
         DROP TABLE encode_source_catalog;
         DROP TABLE encode_fallback_source_catalog;
         DROP TABLE encode_source_usability;
         DROP TABLE encode_fallback_source_usability;",
    )?;
    let source_json_bytes = sources
        .iter()
        .try_fold(0u64, |total, source| {
            total.checked_add(source.metadata_json.len() as u64)
        })
        .ok_or_else(|| AnalysisError::InvalidData("token-source JSON size overflow".into()))?;
    let logical_bytes = capacity_bytes::<TokenSourceRecord>(sources.capacity())?
        .checked_add(source_json_bytes)
        .and_then(|bytes| {
            bytes.checked_add(
                capacity_bytes::<ResolvedTokenMembership>(memberships.capacity()).ok()?,
            )
        })
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

fn classify_source_catalog(
    conn: &Connection,
    catalog_table: &str,
    output_table: &str,
    phase: ProgressPhase,
    parse_pool: &rayon::ThreadPool,
    progress: &mut impl FnMut(ProgressEvent),
) -> Result<u64, AnalysisError> {
    let total: u64 = conn.query_row(
        &format!("SELECT count(*)::UBIGINT FROM {catalog_table}"),
        [],
        |row| row.get(0),
    )?;
    progress(ProgressEvent::determinate(
        phase,
        0,
        total,
        WorkUnit::Items,
        EngineCounters::default(),
    ));
    let mut statement = conn.prepare(&format!(
        "SELECT source_file, source_row_number, metadata_json FROM {catalog_table}"
    ))?;
    let batches = statement.query_arrow([])?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("source_file", DataType::UInt32, false),
        Field::new("source_row_number", DataType::UInt64, false),
        Field::new("usable", DataType::Boolean, false),
    ]));
    let mut appender = conn.appender(output_table)?;
    let mut completed = 0u64;
    for batch in batches {
        let source_files = required_arrow_column::<UInt32Array>(&batch, 0, "source_file")?;
        let source_rows = required_arrow_column::<UInt64Array>(&batch, 1, "source_row_number")?;
        let json = batch.column(2).as_ref();
        let usable = parse_pool.install(|| {
            (0..batch.num_rows())
                .into_par_iter()
                .map(|index| {
                    if source_files.is_null(index)
                        || source_rows.is_null(index)
                        || json.is_null(index)
                    {
                        return Err(AnalysisError::InvalidData(
                            "token-source catalog contains NULL".into(),
                        ));
                    }
                    Ok(
                        !parse_metadata_documents(required_arrow_string(json, index)?)
                            .prefilter_tokens
                            .is_empty(),
                    )
                })
                .collect::<Result<Vec<_>, AnalysisError>>()
        })?;
        let output = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                batch.column(0).clone(),
                batch.column(1).clone(),
                Arc::new(BooleanArray::from(usable)),
            ],
        )
        .map_err(|error| AnalysisError::InvalidData(format!("source usability batch: {error}")))?;
        appender.append_record_batch(output)?;
        completed = completed.saturating_add(batch.num_rows() as u64);
        progress(ProgressEvent::determinate(
            phase,
            completed,
            total,
            WorkUnit::Items,
            EngineCounters::default(),
        ));
    }
    if completed != total {
        return Err(AnalysisError::InvalidData(format!(
            "source usability count differs from catalog ({completed} != {total})"
        )));
    }
    Ok(total)
}

fn load_token_source_records(
    conn: &Connection,
    source_count: u32,
    parse_pool: &rayon::ThreadPool,
    progress: &mut impl FnMut(ProgressEvent),
) -> Result<Vec<TokenSourceRecord>, AnalysisError> {
    let total = u64::from(source_count);
    progress(ProgressEvent::determinate(
        ProgressPhase::EncodeLoadTokenSources,
        0,
        total,
        WorkUnit::Items,
        EngineCounters::default(),
    ));
    let mut statement = conn.prepare(
        "SELECT dictionary.source_id,
                dictionary.source_file,
                dictionary.source_row_number,
                rows.metadata_json
         FROM encode_source_dictionary dictionary
         JOIN metadata_rows rows
           ON rows.source_file = dictionary.source_file
          AND rows.source_row_number = dictionary.source_row_number
         ORDER BY dictionary.source_id",
    )?;
    let batches = statement.query_arrow([])?;
    let mut sources = Vec::with_capacity(source_count as usize);
    for batch in batches {
        let source_ids = required_arrow_column::<UInt32Array>(&batch, 0, "source_id")?;
        let source_files = required_arrow_column::<UInt32Array>(&batch, 1, "source_file")?;
        let source_rows = required_arrow_column::<UInt64Array>(&batch, 2, "source_row_number")?;
        let json = batch.column(3).as_ref();
        let first_id = sources.len();
        let batch_sources = parse_pool.install(|| {
            (0..batch.num_rows())
                .into_par_iter()
                .map(|index| {
                    if source_ids.is_null(index)
                        || source_files.is_null(index)
                        || source_rows.is_null(index)
                        || json.is_null(index)
                    {
                        return Err(AnalysisError::InvalidData(
                            "token-source dictionary contains NULL".into(),
                        ));
                    }
                    let expected = u32::try_from(first_id + index).map_err(|_| {
                        AnalysisError::InvalidData("token-source dictionary exceeds u32".into())
                    })?;
                    if source_ids.value(index) != expected {
                        return Err(AnalysisError::InvalidData(
                            "token-source dictionary IDs are not dense".into(),
                        ));
                    }
                    Ok(TokenSourceRecord {
                        source_file: source_files.value(index),
                        source_row_number: source_rows.value(index),
                        metadata_json: Arc::from(required_arrow_string(json, index)?),
                    })
                })
                .collect::<Result<Vec<_>, AnalysisError>>()
        })?;
        sources.extend(batch_sources);
        progress(ProgressEvent::determinate(
            ProgressPhase::EncodeLoadTokenSources,
            sources.len() as u64,
            total,
            WorkUnit::Items,
            EngineCounters::default(),
        ));
    }
    if sources.len() != source_count as usize {
        return Err(AnalysisError::InvalidData(
            "token-source dictionary count differs from plan".into(),
        ));
    }
    Ok(sources)
}

fn load_token_memberships(
    conn: &Connection,
    membership_count: u64,
    parse_pool: &rayon::ThreadPool,
    progress: &mut impl FnMut(ProgressEvent),
) -> Result<Vec<ResolvedTokenMembership>, AnalysisError> {
    let capacity = usize::try_from(membership_count).map_err(|_| {
        AnalysisError::InvalidData("token membership count exceeds addressable memory".into())
    })?;
    progress(ProgressEvent::determinate(
        ProgressPhase::EncodeLoadTokenMemberships,
        0,
        membership_count,
        WorkUnit::Items,
        EngineCounters::default(),
    ));
    let mut statement = conn.prepare(
        "SELECT resolved.contract_index::UINTEGER,
                resolved.token_index::UINTEGER,
                dictionary.source_id
         FROM encode_resolved_token_sources resolved
         JOIN encode_source_dictionary dictionary
           ON dictionary.source_file = resolved.source_file
          AND dictionary.source_row_number = resolved.source_row_number",
    )?;
    let batches = statement.query_arrow([])?;
    let mut memberships = Vec::with_capacity(capacity);
    for batch in batches {
        let contracts = required_arrow_column::<UInt32Array>(&batch, 0, "contract_index")?;
        let tokens = required_arrow_column::<UInt32Array>(&batch, 1, "token_index")?;
        let sources = required_arrow_column::<UInt32Array>(&batch, 2, "source_id")?;
        let batch_memberships = parse_pool.install(|| {
            (0..batch.num_rows())
                .into_par_iter()
                .map(|index| {
                    if contracts.is_null(index) || tokens.is_null(index) || sources.is_null(index) {
                        return Err(AnalysisError::InvalidData(
                            "resolved token membership contains NULL".into(),
                        ));
                    }
                    Ok(ResolvedTokenMembership {
                        contract_index: contracts.value(index),
                        source_id: sources.value(index),
                        token_id: tokens.value(index),
                    })
                })
                .collect::<Result<Vec<_>, AnalysisError>>()
        })?;
        memberships.extend(batch_memberships);
        progress(ProgressEvent::determinate(
            ProgressPhase::EncodeLoadTokenMemberships,
            memberships.len() as u64,
            membership_count,
            WorkUnit::Items,
            EngineCounters::default(),
        ));
    }
    if memberships.len() != capacity {
        return Err(AnalysisError::InvalidData(
            "resolved token membership count differs from plan".into(),
        ));
    }
    Ok(memberships)
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

#[derive(Default)]
struct PayloadTermInterner {
    template_ids: HashMap<Arc<str>, u32>,
    template_tokens: Vec<Arc<str>>,
    content_ids: HashMap<String, u32>,
    template_string_bytes: u64,
    content_string_bytes: u64,
}

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
        Ok(EncodePayloadRow {
            template_terms,
            content_terms,
        })
    }

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

    fn finalize_template_lexical_ids(&self, payloads: &mut [EncodePayloadRow]) {
        let mut lexical: Vec<(u32, &str)> = self
            .template_tokens
            .iter()
            .enumerate()
            .map(|(old_id, token)| (old_id as u32, token.as_ref()))
            .collect();
        lexical.sort_unstable_by(|left, right| left.1.cmp(right.1));
        let mut remap = vec![0u32; lexical.len()];
        for (new_id, (old_id, _)) in lexical.into_iter().enumerate() {
            remap[old_id as usize] = new_id as u32;
        }
        for payload in payloads {
            for (token, _) in &mut payload.template_terms {
                *token = remap[*token as usize];
            }
            payload
                .template_terms
                .sort_unstable_by_key(|(token, _)| *token);
        }
    }
}

fn string_term_frequencies(tokens: Vec<String>) -> BTreeMap<String, u32> {
    let mut frequencies = BTreeMap::new();
    for token in tokens {
        *frequencies.entry(token).or_insert(0u32) += 1;
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
            let (size, sha256) = sha256_file(&path, 8 * 1024 * 1024)?;
            Ok(ArtifactFingerprintRecord {
                path,
                size,
                row_count: None,
                sha256,
            })
        })
        .collect()
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

fn directory_bytes(path: &Path) -> Result<u64, AnalysisError> {
    if !path.exists() {
        return Ok(0);
    }
    if path.is_file() {
        return Ok(fs::metadata(path)?.len());
    }
    let mut total = 0u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        total = total.saturating_add(directory_bytes(&entry.path())?);
    }
    Ok(total)
}

fn blocking_contract_expansion_pair_work(
    blocking: &metadata_engine::blocking::BlockingBundle,
    fallback_atoms: &[Vec<u32>],
) -> Result<u64, AnalysisError> {
    let mut total = 0u64;
    for block in 0..blocking.block_kinds.len() {
        let begin = blocking.block_atom_offsets[block] as usize;
        let end = blocking.block_atom_offsets[block + 1] as usize;
        let mut prefix = 0u64;
        for &atom in &blocking.block_atoms[begin..end] {
            let members = fallback_atoms
                .get(atom as usize)
                .map_or(0u64, |members| members.len() as u64);
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
    use std::sync::Arc;

    use super::{
        intern_payload_with_parser, payload_feature_identity_ids, planned_encoded_contract_growth,
        planned_token_relation_peak, EncodePayloadRow, EncodeResidentAccounting,
        EncodeResidentAdmission, PayloadTermInterner, TokenSourceInput,
    };
    use metadata_engine::encode::{
        parse_metadata_documents, EncodeContractRow, EncodeSourceRow, PayloadCasWriter,
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

        assert_eq!(payload_feature_identity_ids(&payloads), vec![0, 0, 1]);
    }

    #[test]
    fn duplicate_payload_is_looked_up_in_cas_before_parsing_again() {
        let directory = tempfile::tempdir().unwrap();
        let mut cas = PayloadCasWriter::create(directory.path(), 1024 * 1024).unwrap();
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
    fn live_cardinality_admission_expands_for_unique_payload_and_interner_state() {
        let directory = tempfile::tempdir().unwrap();
        let mut cas = PayloadCasWriter::create(directory.path(), 1024 * 1024).unwrap();
        let mut payloads = Vec::new();
        let mut interner = PayloadTermInterner::default();
        for index in 0..4_096 {
            let json = format!(r#"{{"description":"unique payload term {index}"}}"#);
            intern_payload_with_parser(
                &json,
                &mut cas,
                &mut payloads,
                &mut interner,
                parse_metadata_documents,
            )
            .unwrap();
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
                metadata_json: Arc::from(r#"{"description":"token one"}"#),
            },
            TokenSourceInput {
                token_ids: vec![4, 5],
                source_file: 1,
                source_row_number: 2,
                metadata_json: Arc::from(r#"{"description":"token two"}"#),
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

        assert_eq!(
            planned_token_relation_peak(rows, rows, rows * 128).unwrap(),
            rows * 200 + 64 * 1024 * 1024
        );
    }
}
