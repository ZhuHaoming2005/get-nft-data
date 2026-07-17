//! MetadataEncode adapter: DuckDB stream → metadata_engine.
//!
//! Writes feature/blocking artifacts under `artifacts/metadata/`.
//! Never mutates Prepare/Name tables and never produces production summary rows.

use std::cell::Cell;
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use duckdb::arrow::array::{
    Array, Int64Array, StringArray, StringViewArray, UInt32Array, UInt64Array,
};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;
use metadata_engine::blocking::{
    blocking_artifact_upper_bound_view, build_base_equivalent_atom_sketch_soa_from_view_parallel,
    compile_base_equivalent_view_parallel_with_progress, simhash_band_value,
    AtomDimensionAccumulator, AtomSketchSoA, AtomSketchView, BlockStats, BlockingCompileConfig,
    HotBlockPlan, HotBlockPlanSink, RoutingStatus, ANCHOR_COUNT, BANDS, BAND_BITS,
    BLOCKING_REVISION, DEFAULT_MAX_ROUTING_BLOCK_MEMBERS, JOINT_BAND_FAMILIES,
};
#[cfg(test)]
use metadata_engine::encode::PayloadArena;
use metadata_engine::encode::{
    encode_artifact_upper_bound_all_views, metadata_has_prefilter_tokens, parse_metadata_documents,
    payload_digest, write_encode_artifacts_all_views_with_csr_progress,
    write_encode_artifacts_all_views_with_progress, BidirectionalCsrView, EncodeContractRow,
    EncodeContractSoA, EncodeContractView, EncodePayloadRow, EncodeSourceRow, EncodeSourceSoA,
    EncodeSourceView, FallbackAtomCsr, FallbackAtomView, ParsedMetadataDocuments, PayloadCasIndex,
    PayloadCasWriter, PayloadDigest, PayloadRef, PayloadTermListBatch, PayloadTermLists,
    PayloadTermSoA, PayloadTermView, ShardedPayloadArena, DEFAULT_ARENA_CHUNK_BYTES,
    DEFAULT_MAX_PACK_BYTES, DEFAULT_PAYLOAD_SHARD_COUNT, ENCODE_SCHEMA_REVISION,
};
use metadata_engine::format::{
    commit_ready, map_u32_array, map_u64_array, map_u8_array, ArrayKind, MappedU32Array,
    MappedU64Array, MappedU8Array, TypedArraySink,
};
use metadata_engine::progress::{
    ProgressCounters as EngineCounters, ProgressEvent, ProgressPhase, WorkUnit,
};
use metadata_engine::resource::{MemoryBroker, MemoryError, MemoryLease};
use metadata_engine::storage::{ArtifactClass, ArtifactRegistration, StorageBroker, StorageLease};
use rayon::prelude::*;
use serde::Serialize;

use crate::{sha256_file, write_json_atomically};

use super::super::duckdb_prep::{configure_duckdb, resolve_duckdb_memory_limit};
use super::super::{
    diagnostics_enabled, duckdb_buffer_cap_bytes, effective_memory_snapshot_bytes,
    encode_process_memory_plan, format_byte_size, parse_byte_size, process_memory_envelope_bytes,
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

pub(super) fn ordered_group_ranges<K: Copy + Eq>(
    row_count: usize,
    mut key_at: impl FnMut(usize) -> K,
) -> Vec<(K, std::ops::Range<usize>)> {
    let mut ranges = Vec::new();
    let mut begin = 0usize;
    while begin < row_count {
        let key = key_at(begin);
        let mut end = begin + 1;
        while end < row_count && key_at(end) == key {
            end += 1;
        }
        ranges.push((key, begin..end));
        begin = end;
    }
    ranges
}

pub(super) fn observe_ordered_group<K: Copy + Eq>(
    group: K,
    current_group: &mut Option<K>,
    completed_groups: &mut u64,
) -> bool {
    if *current_group == Some(group) {
        return false;
    }
    if current_group.is_some() {
        *completed_groups = completed_groups.saturating_add(1);
    }
    *current_group = Some(group);
    true
}

pub(super) fn finish_ordered_group_count<K>(
    current_group: Option<K>,
    completed_groups: u64,
) -> u64 {
    completed_groups.saturating_add(u64::from(current_group.is_some()))
}

pub(super) fn first_usable_rows_by_ordered_group<K, F>(
    ranges: &[(K, std::ops::Range<usize>)],
    already_selected: Option<K>,
    is_usable: F,
) -> Result<Vec<Option<usize>>, AnalysisError>
where
    K: Copy + Eq + Send + Sync,
    F: Fn(usize) -> Result<bool, AnalysisError> + Sync,
{
    ranges
        .par_iter()
        .map(|(key, range)| {
            if already_selected == Some(*key) {
                return Ok(None);
            }
            for index in range.clone() {
                if is_usable(index)? {
                    return Ok(Some(index));
                }
            }
            Ok(None)
        })
        .collect()
}

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
    planned_final_bytes: u64,
    admitted_final_bytes: u64,
    admitted_partial_peak_bytes: u64,
    storage_admission_warnings: u64,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct EncodeAdmissionEstimate {
    pub(super) resident_peak_bytes: u64,
    pub(super) partial_peak_bytes: u64,
    pub(super) token_relation_peak_bytes: u64,
    pub(super) payload_spill_upper_bound_bytes: u64,
    pub(super) representative_rows: u64,
    pub(super) token_rows: u64,
}

struct EncodeResidentAdmission {
    lease: MemoryLease,
    floor_bytes: u64,
    peak_bytes: u64,
}

impl EncodeResidentAdmission {
    fn new(lease: MemoryLease, floor_bytes: u64) -> Self {
        let current = lease.bytes();
        Self {
            lease,
            floor_bytes,
            peak_bytes: current,
        }
    }

    fn reserve_growth(
        &mut self,
        resident_bytes: u64,
        growth_bytes: u64,
    ) -> Result<(), AnalysisError> {
        match self.try_reserve_growth(resident_bytes, growth_bytes) {
            Ok(()) => Ok(()),
            Err(MemoryError::Budget { .. }) => {
                let target = resident_bytes
                    .checked_add(growth_bytes)
                    .ok_or_else(|| {
                        AnalysisError::InvalidData(
                            "metadata encode live cardinality overflow".into(),
                        )
                    })?
                    .max(self.floor_bytes);
                self.peak_bytes = self.peak_bytes.max(target);
                Ok(())
            }
            Err(error) => Err(AnalysisError::InvalidData(format!(
                "metadata encode live cardinality admission: {error}"
            ))),
        }
    }

    fn try_reserve_growth(
        &mut self,
        resident_bytes: u64,
        growth_bytes: u64,
    ) -> Result<(), MemoryError> {
        let target = resident_bytes
            .checked_add(growth_bytes)
            .ok_or(MemoryError::Overflow)?;
        self.try_set_current(target)
    }

    fn commit(&mut self, resident_bytes: u64) -> Result<(), AnalysisError> {
        self.set_current(resident_bytes)
    }

    fn set_current(&mut self, resident_bytes: u64) -> Result<(), AnalysisError> {
        match self.try_set_current(resident_bytes) {
            Ok(()) => Ok(()),
            Err(MemoryError::Budget { .. }) => {
                self.peak_bytes = self.peak_bytes.max(self.floor_bytes.max(resident_bytes));
                Ok(())
            }
            Err(error) => Err(AnalysisError::InvalidData(format!(
                "metadata encode live cardinality admission: {error}"
            ))),
        }
    }

    fn try_set_current(&mut self, resident_bytes: u64) -> Result<(), MemoryError> {
        // The initial conservative reservation is only a preflight guard. It
        // must be allowed to shrink to measured live capacity between phases;
        // otherwise a raw-JSON estimate can permanently pin hundreds of GiB
        // and make later bounded scratch reservations fail spuriously.
        let target = self.floor_bytes.max(resident_bytes);
        self.lease.resize(target)?;
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

        let (host_total_memory, host_available_memory) = effective_memory_snapshot_bytes();
        let process_envelope =
            process_memory_envelope_bytes(host_total_memory, host_available_memory);
        let configured_duckdb =
            parse_byte_size(&resolve_duckdb_memory_limit(&options.duckdb_memory_limit)?)? as u64;
        let preliminary_duckdb = configured_duckdb.min(duckdb_buffer_cap_bytes(process_envelope));
        let conn = open_prepare_for_encode(options)?;
        conn.execute(
            &format!(
                "PRAGMA memory_limit='{}'",
                format_byte_size(usize::try_from(preliminary_duckdb).unwrap_or(usize::MAX))
            ),
            [],
        )?;
        if preliminary_duckdb < configured_duckdb {
            progress.warn(format!(
                "metadata Encode preliminary DuckDB budget reduced from {} to {} so storage \
                 estimation remains inside the current process-memory envelope",
                format_byte_size(usize::try_from(configured_duckdb).unwrap_or(usize::MAX)),
                format_byte_size(usize::try_from(preliminary_duckdb).unwrap_or(usize::MAX)),
            ));
        }
        progress.step_stage("opened Prepare DuckDB for isolated Encode");
        let estimate = estimate_encode_storage_bytes(&conn)?;
        // Admit the owned Encode builder before materializing row/token vectors.
        let memory_plan = encode_process_memory_plan(
            &options.duckdb_memory_limit,
            total_memory_budget_bytes(&options.memory_limit)?,
            estimate.resident_peak_bytes,
            host_total_memory,
            host_available_memory,
        )?;
        if memory_plan.duckdb_bytes < preliminary_duckdb {
            progress.warn(format!(
                "metadata Encode rebalanced DuckDB from {} to {} after resident-state estimation; \
                 the remaining process envelope is reserved for Rust hot data and exact external \
                 fallbacks",
                format_byte_size(usize::try_from(preliminary_duckdb).unwrap_or(usize::MAX)),
                format_byte_size(usize::try_from(memory_plan.duckdb_bytes).unwrap_or(usize::MAX)),
            ));
        }
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
        let initial_resident_reservation = estimate.resident_peak_bytes.min(memory_hard_top);
        if estimate.resident_peak_bytes > memory_hard_top {
            progress.warn(format!(
                "metadata encode conservative resident estimate {} exceeds the Rust envelope {}; \
                 continuing with live-capacity admission and bounded batches",
                format_byte_size(
                    usize::try_from(estimate.resident_peak_bytes).unwrap_or(usize::MAX)
                ),
                format_byte_size(usize::try_from(memory_hard_top).unwrap_or(usize::MAX)),
            ));
        }
        let encode_memory_lease = memory_broker
            .reserve(initial_resident_reservation)
            .map_err(|err| {
                AnalysisError::InvalidData(format!("metadata encode memory admission: {err}"))
            })?;
        let mut resident_admission =
            EncodeResidentAdmission::new(encode_memory_lease, ENCODE_RESIDENT_FIXED_BYTES);
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
                |message| progress.warn(message),
                |event| progress.observe_engine_event(event),
            )?;
        let source_count = sources.source_count();
        let payload_count = payloads.payload_count();
        let contract_count = contracts.contract_count();
        let template_term_count = payloads.view().template_terms.len();
        let content_term_count = payloads.view().content_terms.len();
        let token_membership_count = sources.view().token_ids.len();
        let atom_count = atoms.len();
        let diagnostics = diagnostics_enabled();
        let mut routing_membership_count = if diagnostics {
            atoms.anchor_membership_count()
        } else {
            0
        };
        let fallback_membership_count = fallback_atoms.view().members.len();
        // Input streaming does not write Encode artifacts. Record the storage
        // lifetime only after the frozen SoA cardinalities are known; raw JSON
        // bytes are not a durable-size proxy for the fixed-width representation.
        let admitted_feature_bytes = encode_artifact_upper_bound_all_views(
            sources.view(),
            payloads.view(),
            contracts.view(),
            fallback_atoms.view(),
        )
        .map_err(encode_err)?;
        let feature_storage_reservation = reserve_storage_advisory(
            &mut broker,
            ArtifactClass::Feature,
            admitted_feature_bytes,
            estimate.partial_peak_bytes,
            "encoded feature bundle",
            |message| progress.warn(message),
        )?;
        let feature_storage_admitted = feature_storage_reservation.is_some();
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
        let persist_growth =
            planned_feature_persist_growth(sources.view(), payloads.view(), contracts.view())?;
        let external_csr =
            match resident_admission.try_reserve_growth(frozen_resident_bytes, persist_growth) {
                Ok(()) => None,
                Err(error @ MemoryError::Budget { .. }) => {
                    progress.warn(format!(
                        "metadata Encode resident bidirectional CSR/persist scratch exceeded the \
                         Rust envelope ({error}); building the exact contract/token directions in \
                         a DuckDB spill table and continuing from demand-paged typed arrays"
                    ));
                    resident_admission.commit(frozen_resident_bytes)?;
                    let temporary_bytes = external_csr_storage_upper_bound(sources.view())?;
                    let temporary_reservation = reserve_storage_advisory(
                        &mut broker,
                        ArtifactClass::Feature,
                        temporary_bytes,
                        64 * 1024 * 1024,
                        "temporary Encode bidirectional CSR spill",
                        |message| progress.warn(message),
                    )?;
                    let available = memory_broker.available_bytes();
                    let batch_records = usize::try_from((available / 16).clamp(4_096, 1_048_576))
                        .unwrap_or(1_048_576);
                    let spill = DuckDbCsrSpill::create(&conn)?;
                    let csr = spill.build(work_directory, sources.view(), batch_records)?;
                    drop(spill);
                    drop(temporary_reservation);
                    Some(csr)
                }
                Err(error) => {
                    return Err(AnalysisError::InvalidData(format!(
                        "metadata encode feature persist admission: {error}"
                    )));
                }
            };
        let persist_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(options.threads.max(1))
            .thread_name(|index| format!("metadata-encode-persist-{index}"))
            .build()
            .map_err(|error| AnalysisError::InvalidData(format!("encode persist pool: {error}")))?;
        let encode_persist_stats = persist_pool
            .install(|| {
                let observe = |completed, total| {
                    progress.observe_engine_event(ProgressEvent::determinate(
                        ProgressPhase::EncodePersist,
                        completed,
                        total,
                        WorkUnit::Bytes,
                        EngineCounters::default(),
                    ));
                };
                if let Some(csr) = external_csr.as_ref() {
                    write_encode_artifacts_all_views_with_csr_progress(
                        &encode_staging,
                        sources.view(),
                        payloads.view(),
                        contracts.view(),
                        fallback_atoms.view(),
                        csr.view(),
                        observe,
                    )
                } else {
                    write_encode_artifacts_all_views_with_progress(
                        &encode_staging,
                        sources.view(),
                        payloads.view(),
                        contracts.view(),
                        fallback_atoms.view(),
                        observe,
                    )
                }
            })
            .map_err(encode_err)?;
        drop(persist_pool);
        drop(external_csr);
        // Releasing the feature lifetime lease avoids counting it twice while
        // recording the independently written blocking bundle.
        drop(feature_storage_reservation);
        let blocking_resident_bytes =
            blocking_encode_state_resident_bytes(&atoms, &fallback_atoms)?;
        drop(sources);
        drop(payloads);
        drop(contracts);
        resident_admission.commit(blocking_resident_bytes)?;
        let encode_wall_millis = millis_since(encode_started);
        progress.step_stage(format!(
            "wrote encode features for {} sources / {} payloads",
            source_count, payload_count
        ));

        let blocking_started = Instant::now();
        fs::create_dir_all(&blocking_staging)?;
        let config = BlockingCompileConfig {
            max_routing_block_members: DEFAULT_MAX_ROUTING_BLOCK_MEMBERS,
        };
        let admitted_blocking_bytes =
            blocking_artifact_upper_bound_view(atoms.view()).map_err(blocking_err)?;
        let blocking_storage_reservation = reserve_storage_advisory(
            &mut broker,
            ArtifactClass::Blocking,
            admitted_blocking_bytes,
            estimate.partial_peak_bytes,
            "blocking bundle",
            |message| progress.warn(message),
        )?;
        let blocking_storage_admitted = blocking_storage_reservation.is_some();
        let compile_growth = planned_blocking_resident_growth(atoms.view())?;
        let external_blocking =
            match resident_admission.try_reserve_growth(blocking_resident_bytes, compile_growth) {
                Ok(()) => false,
                Err(error @ MemoryError::Budget { .. }) => {
                    progress.warn(format!(
                        "metadata BlockingBundle forward/inverse routing CSR exceeded the Rust \
                     envelope ({error}); externalizing exact memberships and block descriptors \
                     through DuckDB sort/group, then writing both typed-array directions directly"
                    ));
                    resident_admission.commit(blocking_resident_bytes)?;
                    true
                }
                Err(error) => {
                    return Err(AnalysisError::InvalidData(format!(
                        "metadata blocking compile admission: {error}"
                    )));
                }
            };
        let (block_stats, contract_expansion_pair_work, compiled_routing_memberships) =
            if external_blocking {
                let temporary_bytes = external_blocking_storage_upper_bound(atoms.view())?;
                let temporary_reservation = reserve_storage_advisory(
                    &mut broker,
                    ArtifactClass::Blocking,
                    temporary_bytes,
                    256 * 1024 * 1024,
                    "temporary BlockingBundle membership spill",
                    |message| progress.warn(message),
                )?;
                let batch_records =
                    usize::try_from((memory_broker.available_bytes() / 16).clamp(4_096, 1_048_576))
                        .unwrap_or(1_048_576);
                let spill = DuckDbBlockingSpill::create(&conn)?;
                let summary = spill.compile(
                    atoms.view(),
                    &config,
                    &blocking_staging,
                    batch_records,
                    options.threads,
                    |event| progress.observe_engine_event(event),
                )?;
                drop(spill);
                drop(temporary_reservation);
                let expansion = blocking_contract_expansion_pair_work_files(
                    &blocking_staging,
                    fallback_atoms.view(),
                )?;
                (
                    summary.block_stats,
                    expansion,
                    summary.routing_membership_count,
                )
            } else {
                let blocking_bundle = compile_base_equivalent_view_parallel_with_progress(
                    atoms.view(),
                    &config,
                    &blocking_staging,
                    options.threads,
                    |event| progress.observe_engine_event(event),
                )
                .map_err(blocking_err)?;
                let expansion =
                    blocking_contract_expansion_pair_work(&blocking_bundle, fallback_atoms.view())?;
                let memberships = blocking_bundle.block_atoms.len() as u64;
                (blocking_bundle.block_stats, expansion, memberships)
            };
        if diagnostics {
            routing_membership_count = compiled_routing_memberships;
        }
        drop(blocking_storage_reservation);
        let blocking_wall_millis = millis_since(blocking_started);
        progress.step_stage(format!(
            "compiled BaseEquivalent blocking for {} atoms",
            atom_count
        ));
        let block_pair_work = block_stats.bucket_pair_work;
        let max_block_members = block_stats.smax;
        drop(atoms);
        drop(fallback_atoms);
        resident_admission.commit(ENCODE_RESIDENT_FIXED_BYTES)?;

        progress.observe_engine_event(ProgressEvent::determinate(
            ProgressPhase::EncodePublish,
            0,
            4,
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
            "source_count": source_count,
            "payload_count": payload_count,
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
            "atom_count": atom_count,
            "block_pair_work": block_pair_work,
            "contract_expansion_pair_work": contract_expansion_pair_work,
            "max_block_members": max_block_members,
        })
        .to_string();
        commit_ready(&blocking_dir, "blocking.ready", &blocking_manifest).map_err(format_err)?;
        blocking_publish.finalize()?;
        encode_publish.finalize()?;
        progress.observe_engine_event(ProgressEvent::determinate(
            ProgressPhase::EncodePublish,
            1,
            4,
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
        progress.observe_engine_event(ProgressEvent::determinate(
            ProgressPhase::EncodePublish,
            2,
            4,
            WorkUnit::Items,
            EngineCounters::default(),
        ));
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
        progress.observe_engine_event(ProgressEvent::determinate(
            ProgressPhase::EncodePublish,
            3,
            4,
            WorkUnit::Items,
            EngineCounters::default(),
        ));

        if diagnostics {
            let metrics = EncodeMetrics {
                schema_version: 4,
                encode_wall_millis,
                blocking_wall_millis,
                source_rows: source_count as u64,
                payload_count: payload_count as u64,
                contract_count: contract_count as u64,
                atom_count: atom_count as u64,
                template_term_count: template_term_count as u64,
                content_term_count: content_term_count as u64,
                token_membership_count: token_membership_count as u64,
                routing_membership_count,
                fallback_membership_count: fallback_membership_count as u64,
                admitted_resident_peak_bytes: resident_admission.peak_bytes(),
                planned_final_bytes: admitted_feature_bytes.saturating_add(admitted_blocking_bytes),
                admitted_final_bytes: u64::from(feature_storage_admitted)
                    .saturating_mul(admitted_feature_bytes)
                    .saturating_add(
                        u64::from(blocking_storage_admitted)
                            .saturating_mul(admitted_blocking_bytes),
                    ),
                admitted_partial_peak_bytes: estimate.partial_peak_bytes,
                storage_admission_warnings: u64::from(!feature_storage_admitted)
                    + u64::from(!blocking_storage_admitted),
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
        progress.observe_engine_event(ProgressEvent::determinate(
            ProgressPhase::EncodePublish,
            4,
            4,
            WorkUnit::Items,
            EngineCounters::default(),
        ));
        progress.step_stage("published encoded metadata snapshot");
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
    EncodeSources,
    EncodePayloadTerms,
    EncodeContracts,
    EncodeAtomSketches,
    EncodeFallbackAtoms,
    Vec<EncodeChainTotal>,
);

const ENCODE_PAYLOAD_SPILL_PREFIX: &str = ".staging-encode-payloads-";
const ENCODE_TERM_SPILL_PREFIX: &str = ".staging-encode-terms-";
const ENCODE_CSR_SPILL_PREFIX: &str = ".staging-encode-csr-";
const ENCODE_COLUMN_SPILL_PREFIX: &str = ".staging-encode-columns-";
const ENCODE_ATOM_SPILL_PREFIX: &str = ".staging-encode-atoms-";
const ENCODE_SKETCH_SPILL_PREFIX: &str = ".staging-encode-sketches-";

pub(super) enum EncodeSources {
    Memory(EncodeSourceSoA),
    Disk(Box<DiskEncodeSources>),
}

impl EncodeSources {
    pub(super) fn view(&self) -> EncodeSourceView<'_> {
        match self {
            Self::Memory(sources) => sources.as_view(),
            Self::Disk(sources) => sources.view(),
        }
    }

    pub(super) fn source_count(&self) -> usize {
        self.view().source_count()
    }

    fn resident_capacity_bytes(&self) -> Result<u64, AnalysisError> {
        match self {
            Self::Memory(sources) => [
                capacity_bytes::<u32>(sources.contract_ids.capacity())?,
                capacity_bytes::<u32>(sources.payload_ids.capacity())?,
                capacity_bytes::<u64>(sources.token_offsets.capacity())?,
                capacity_bytes::<u32>(sources.token_ids.capacity())?,
            ]
            .into_iter()
            .try_fold(0u64, |total, bytes| {
                total.checked_add(bytes).ok_or_else(|| {
                    AnalysisError::InvalidData("Encode source resident accounting overflow".into())
                })
            }),
            Self::Disk(_) => Ok(0),
        }
    }
}

pub(super) enum EncodeContracts {
    Memory(EncodeContractSoA),
    Disk(Box<DiskEncodeContracts>),
}

impl EncodeContracts {
    pub(super) fn view(&self) -> EncodeContractView<'_> {
        match self {
            Self::Memory(contracts) => contracts.as_view(),
            Self::Disk(contracts) => contracts.view(),
        }
    }

    pub(super) fn contract_count(&self) -> usize {
        self.view().contract_count()
    }

    fn resident_capacity_bytes(&self) -> Result<u64, AnalysisError> {
        match self {
            Self::Memory(contracts) => [
                capacity_bytes::<u32>(contracts.contract_ids.capacity())?,
                capacity_bytes::<u32>(contracts.chain_ids.capacity())?,
                capacity_bytes::<u32>(contracts.source_doc_ids.capacity())?,
                capacity_bytes::<u32>(contracts.payload_ids.capacity())?,
                capacity_bytes::<u64>(contracts.weights.capacity())?,
            ]
            .into_iter()
            .try_fold(0u64, |total, bytes| {
                total.checked_add(bytes).ok_or_else(|| {
                    AnalysisError::InvalidData(
                        "Encode contract resident accounting overflow".into(),
                    )
                })
            }),
            Self::Disk(_) => Ok(0),
        }
    }
}

pub(super) enum EncodeFallbackAtoms {
    Memory(FallbackAtomCsr),
    Disk(Box<DiskFallbackAtoms>),
}

impl EncodeFallbackAtoms {
    pub(super) fn view(&self) -> FallbackAtomView<'_> {
        match self {
            Self::Memory(atoms) => atoms.as_view(),
            Self::Disk(atoms) => atoms.view(),
        }
    }

    fn resident_capacity_bytes(&self) -> Result<u64, AnalysisError> {
        match self {
            Self::Memory(atoms) => [
                capacity_bytes::<u64>(atoms.offsets.capacity())?,
                capacity_bytes::<u32>(atoms.members.capacity())?,
                capacity_bytes::<u32>(atoms.atom_payloads.capacity())?,
            ]
            .into_iter()
            .try_fold(0u64, |total, bytes| {
                total.checked_add(bytes).ok_or_else(|| {
                    AnalysisError::InvalidData(
                        "Encode fallback atom resident accounting overflow".into(),
                    )
                })
            }),
            Self::Disk(_) => Ok(0),
        }
    }
}

pub(super) enum EncodeAtomSketches {
    Memory(AtomSketchSoA),
    Disk(Box<DiskAtomSketches>),
}

impl EncodeAtomSketches {
    pub(super) fn view(&self) -> AtomSketchView<'_> {
        match self {
            Self::Memory(atoms) => atoms.as_view(),
            Self::Disk(atoms) => atoms.view(),
        }
    }

    pub(super) fn len(&self) -> usize {
        self.view().len()
    }

    fn resident_capacity_bytes(&self) -> Result<u64, AnalysisError> {
        match self {
            Self::Memory(atoms) => [
                capacity_bytes::<u64>(atoms.template_simhashes.capacity())?,
                capacity_bytes::<u64>(atoms.content_simhashes.capacity())?,
                capacity_bytes::<u64>(atoms.template_anchor_offsets.capacity())?,
                capacity_bytes::<u32>(atoms.template_anchors.capacity())?,
                capacity_bytes::<u64>(atoms.content_anchor_offsets.capacity())?,
                capacity_bytes::<u32>(atoms.content_anchors.capacity())?,
                capacity_bytes::<u8>(atoms.has_template_terms.capacity())?,
                capacity_bytes::<u8>(atoms.has_content_terms.capacity())?,
            ]
            .into_iter()
            .try_fold(0u64, |total, bytes| {
                total.checked_add(bytes).ok_or_else(|| {
                    AnalysisError::InvalidData("Encode sketch resident accounting overflow".into())
                })
            }),
            Self::Disk(_) => Ok(0),
        }
    }

    fn anchor_membership_count(&self) -> u64 {
        let view = self.view();
        view.template_anchors.len() as u64 + view.content_anchors.len() as u64
    }
}

pub(super) enum EncodePayloadTerms {
    Memory(PayloadTermSoA),
    Disk(Box<DiskPayloadTermSoA>),
}

impl EncodePayloadTerms {
    pub(super) fn view(&self) -> PayloadTermView<'_> {
        match self {
            Self::Memory(payloads) => payloads.as_view(),
            Self::Disk(payloads) => payloads.view(),
        }
    }

    pub(super) fn payload_count(&self) -> usize {
        self.view().payload_count()
    }

    fn resident_capacity_bytes(&self) -> Result<u64, AnalysisError> {
        match self {
            Self::Memory(payloads) => [
                capacity_bytes::<u64>(payloads.template_offsets.capacity())?,
                capacity_bytes::<u32>(payloads.template_terms.capacity())?,
                capacity_bytes::<u32>(payloads.template_freqs.capacity())?,
                capacity_bytes::<u64>(payloads.content_offsets.capacity())?,
                capacity_bytes::<u32>(payloads.content_terms.capacity())?,
                capacity_bytes::<u32>(payloads.content_freqs.capacity())?,
            ]
            .into_iter()
            .try_fold(0u64, |total, bytes| {
                total.checked_add(bytes).ok_or_else(|| {
                    AnalysisError::InvalidData(
                        "Encode payload term resident accounting overflow".into(),
                    )
                })
            }),
            // The typed arrays are demand-paged and are backed by the term
            // staging directory. Heap ownership is constant-size; later
            // phases reserve their own bounded hot/scratch windows.
            Self::Disk(_) => Ok(0),
        }
    }
}

pub(super) struct DiskPayloadTermSoA {
    template_offsets: MappedU64Array,
    template_terms: MappedU32Array,
    template_freqs: MappedU32Array,
    content_offsets: MappedU64Array,
    content_terms: MappedU32Array,
    content_freqs: MappedU32Array,
    _cleanup: metadata_engine::artifacts::StagingCleanupGuard,
}

impl DiskPayloadTermSoA {
    fn view(&self) -> PayloadTermView<'_> {
        PayloadTermView {
            template_offsets: &self.template_offsets,
            template_terms: &self.template_terms,
            template_freqs: &self.template_freqs,
            content_offsets: &self.content_offsets,
            content_terms: &self.content_terms,
            content_freqs: &self.content_freqs,
        }
    }

    fn open(
        directory: &Path,
        cleanup: metadata_engine::artifacts::StagingCleanupGuard,
    ) -> Result<Self, AnalysisError> {
        Ok(Self {
            template_offsets: map_u64_array(&directory.join("payload_template_offsets.u64"))
                .map_err(encode_err)?,
            template_terms: map_u32_array(&directory.join("payload_template_terms.u32"))
                .map_err(encode_err)?,
            template_freqs: map_u32_array(&directory.join("payload_template_freqs.u32"))
                .map_err(encode_err)?,
            content_offsets: map_u64_array(&directory.join("payload_content_offsets.u64"))
                .map_err(encode_err)?,
            content_terms: map_u32_array(&directory.join("payload_content_terms.u32"))
                .map_err(encode_err)?,
            content_freqs: map_u32_array(&directory.join("payload_content_freqs.u32"))
                .map_err(encode_err)?,
            _cleanup: cleanup,
        })
    }
}

struct SpillDirectoryCleanup {
    path: PathBuf,
}

impl Drop for SpillDirectoryCleanup {
    fn drop(&mut self) {
        if self.path.is_dir() {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

pub(super) struct DiskEncodeSources {
    contract_ids: MappedU32Array,
    payload_ids: MappedU32Array,
    token_offsets: MappedU64Array,
    token_ids: MappedU32Array,
    _cleanup: Arc<SpillDirectoryCleanup>,
}

impl DiskEncodeSources {
    fn view(&self) -> EncodeSourceView<'_> {
        EncodeSourceView {
            contract_ids: &self.contract_ids,
            payload_ids: &self.payload_ids,
            token_offsets: &self.token_offsets,
            token_ids: &self.token_ids,
        }
    }
}

pub(super) struct DiskEncodeContracts {
    contract_ids: MappedU32Array,
    chain_ids: MappedU32Array,
    source_doc_ids: MappedU32Array,
    payload_ids: MappedU32Array,
    weights: MappedU64Array,
    _cleanup: Arc<SpillDirectoryCleanup>,
}

impl DiskEncodeContracts {
    fn view(&self) -> EncodeContractView<'_> {
        EncodeContractView {
            contract_ids: &self.contract_ids,
            chain_ids: &self.chain_ids,
            source_doc_ids: &self.source_doc_ids,
            payload_ids: &self.payload_ids,
            weights: &self.weights,
        }
    }
}

pub(super) struct DiskFallbackAtoms {
    offsets: MappedU64Array,
    members: MappedU32Array,
    atom_payloads: MappedU32Array,
    _cleanup: metadata_engine::artifacts::StagingCleanupGuard,
}

impl DiskFallbackAtoms {
    fn view(&self) -> FallbackAtomView<'_> {
        FallbackAtomView {
            offsets: &self.offsets,
            members: &self.members,
            atom_payloads: &self.atom_payloads,
        }
    }
}

pub(super) struct DiskAtomSketches {
    template_simhashes: MappedU64Array,
    content_simhashes: MappedU64Array,
    template_anchor_offsets: MappedU64Array,
    template_anchors: MappedU32Array,
    content_anchor_offsets: MappedU64Array,
    content_anchors: MappedU32Array,
    has_template_terms: MappedU8Array,
    has_content_terms: MappedU8Array,
    _cleanup: metadata_engine::artifacts::StagingCleanupGuard,
}

impl DiskAtomSketches {
    fn view(&self) -> AtomSketchView<'_> {
        AtomSketchView {
            template_simhashes: &self.template_simhashes,
            content_simhashes: &self.content_simhashes,
            template_anchor_offsets: &self.template_anchor_offsets,
            template_anchors: &self.template_anchors,
            content_anchor_offsets: &self.content_anchor_offsets,
            content_anchors: &self.content_anchors,
            has_template_terms: &self.has_template_terms,
            has_content_terms: &self.has_content_terms,
        }
    }

    fn open(
        directory: &Path,
        cleanup: metadata_engine::artifacts::StagingCleanupGuard,
    ) -> Result<Self, AnalysisError> {
        Ok(Self {
            template_simhashes: map_u64_array(&directory.join("atom_template_simhash.u64"))
                .map_err(encode_err)?,
            content_simhashes: map_u64_array(&directory.join("atom_content_simhash.u64"))
                .map_err(encode_err)?,
            template_anchor_offsets: map_u64_array(
                &directory.join("atom_template_anchor_offsets.u64"),
            )
            .map_err(encode_err)?,
            template_anchors: map_u32_array(&directory.join("atom_template_anchors.u32"))
                .map_err(encode_err)?,
            content_anchor_offsets: map_u64_array(
                &directory.join("atom_content_anchor_offsets.u64"),
            )
            .map_err(encode_err)?,
            content_anchors: map_u32_array(&directory.join("atom_content_anchors.u32"))
                .map_err(encode_err)?,
            has_template_terms: map_u8_array(&directory.join("atom_has_template_terms.u8"))
                .map_err(encode_err)?,
            has_content_terms: map_u8_array(&directory.join("atom_has_content_terms.u8"))
                .map_err(encode_err)?,
            _cleanup: cleanup,
        })
    }
}

const ENCODE_TEMPLATE_TERM_SPILL_TABLE: &str = "encode_template_term_spill";
const ENCODE_CONTENT_TERM_SPILL_TABLE: &str = "encode_content_term_spill";
const ENCODE_ATOM_TERM_TABLE: &str = "encode_atom_term_spill";
const ENCODE_ATOM_TERM_DF_TABLE: &str = "encode_atom_term_df_spill";
const ENCODE_BLOCK_MEMBERSHIP_TABLE: &str = "encode_block_membership_spill";
const ENCODE_BLOCK_DISTINCT_TABLE: &str = "encode_block_membership_distinct";
const ENCODE_BLOCK_TABLE: &str = "encode_block_descriptor_spill";
const ENCODE_EXTERNAL_PAYLOAD_INDEX_TABLE: &str = "encode_external_payload_index";
const ENCODE_EXTERNAL_PAYLOAD_SHARD_TABLE: &str = "encode_external_payload_shards";

/// Exact external term dictionary. DuckDB owns the variable-width strings and
/// spills its DISTINCT/window sorts under its configured memory limit; Rust
/// retains only one parsed batch and the final demand-paged typed arrays.
struct DuckDbPayloadTermSpill<'connection> {
    conn: &'connection Connection,
    template_request_count: Cell<u64>,
    content_request_count: Cell<u64>,
}

impl<'connection> DuckDbPayloadTermSpill<'connection> {
    fn create(conn: &'connection Connection) -> Result<Self, AnalysisError> {
        conn.execute_batch(&format!(
            "DROP TABLE IF EXISTS {ENCODE_TEMPLATE_TERM_SPILL_TABLE};
             DROP TABLE IF EXISTS {ENCODE_CONTENT_TERM_SPILL_TABLE};
             CREATE TEMP TABLE {ENCODE_TEMPLATE_TERM_SPILL_TABLE}(
                 payload_id UINTEGER NOT NULL,
                 token VARCHAR NOT NULL,
                 frequency UINTEGER NOT NULL,
                 request_order UBIGINT NOT NULL
             );
             CREATE TEMP TABLE {ENCODE_CONTENT_TERM_SPILL_TABLE}(
                 payload_id UINTEGER NOT NULL,
                 token VARCHAR NOT NULL,
                 frequency UINTEGER NOT NULL,
                 request_order UBIGINT NOT NULL
             );"
        ))?;
        Ok(Self {
            conn,
            template_request_count: Cell::new(0),
            content_request_count: Cell::new(0),
        })
    }

    fn append_batch(
        &self,
        batch_start: usize,
        parsed_batch: Vec<ParsedMetadataDocuments>,
        parse_pool: &rayon::ThreadPool,
    ) -> Result<usize, AnalysisError> {
        let pending = parse_pool.install(|| {
            parsed_batch
                .into_par_iter()
                .map(|parsed| PendingPayloadTerms {
                    template: string_term_frequencies(parsed.prefilter_tokens),
                    content: string_term_frequencies(parsed.content_tokens),
                })
                .collect::<Vec<_>>()
        });
        let batch_start = u32::try_from(batch_start).map_err(|_| {
            AnalysisError::InvalidData("payload term spill identity exceeds u32".into())
        })?;
        let batch_len = pending.len();
        let batch_end = u64::from(batch_start)
            .checked_add(batch_len as u64)
            .ok_or_else(|| {
                AnalysisError::InvalidData("payload term spill identity overflow".into())
            })?;
        if batch_end > u64::from(u32::MAX) + 1 {
            return Err(AnalysisError::InvalidData(
                "payload term spill identity exceeds u32".into(),
            ));
        }
        self.append_dimension_batch(
            ENCODE_TEMPLATE_TERM_SPILL_TABLE,
            batch_start,
            &pending,
            true,
            &self.template_request_count,
        )?;
        self.append_dimension_batch(
            ENCODE_CONTENT_TERM_SPILL_TABLE,
            batch_start,
            &pending,
            false,
            &self.content_request_count,
        )?;
        Ok(batch_len)
    }

    fn append_dimension_batch(
        &self,
        table: &str,
        batch_start: u32,
        pending: &[PendingPayloadTerms],
        template: bool,
        request_count: &Cell<u64>,
    ) -> Result<(), AnalysisError> {
        let count = pending.iter().try_fold(0usize, |total, payload| {
            total
                .checked_add(if template {
                    payload.template.len()
                } else {
                    payload.content.len()
                })
                .ok_or_else(|| {
                    AnalysisError::InvalidData("payload term spill batch overflow".into())
                })
        })?;
        if count == 0 {
            return Ok(());
        }
        let base = request_count.get();
        let next = base.checked_add(count as u64).ok_or_else(|| {
            AnalysisError::InvalidData("payload term request order overflow".into())
        })?;
        let mut payload_ids = Vec::with_capacity(count);
        let mut tokens = Vec::with_capacity(count);
        let mut frequencies = Vec::with_capacity(count);
        let mut request_orders = Vec::with_capacity(count);
        for (local_payload, payload) in pending.iter().enumerate() {
            let payload_id = batch_start + local_payload as u32;
            let terms = if template {
                &payload.template
            } else {
                &payload.content
            };
            for (token, frequency) in terms {
                payload_ids.push(payload_id);
                tokens.push(token.as_str());
                frequencies.push(*frequency);
                request_orders.push(base + request_orders.len() as u64);
            }
        }
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("payload_id", DataType::UInt32, false),
                Field::new("token", DataType::Utf8, false),
                Field::new("frequency", DataType::UInt32, false),
                Field::new("request_order", DataType::UInt64, false),
            ])),
            vec![
                Arc::new(UInt32Array::from(payload_ids)),
                Arc::new(StringArray::from(tokens)),
                Arc::new(UInt32Array::from(frequencies)),
                Arc::new(UInt64Array::from(request_orders)),
            ],
        )
        .map_err(encode_err)?;
        let mut appender = self.conn.appender(table)?;
        appender.append_record_batch(batch)?;
        appender.flush()?;
        request_count.set(next);
        Ok(())
    }

    fn materialize(
        &self,
        work_directory: &Path,
        payload_count: usize,
    ) -> Result<DiskPayloadTermSoA, AnalysisError> {
        let directory = work_directory.join("artifacts/metadata").join(format!(
            "{ENCODE_TERM_SPILL_PREFIX}{}",
            metadata_engine::artifacts::new_artifact_run_id()
        ));
        let cleanup = metadata_engine::artifacts::StagingCleanupGuard::new([directory.clone()]);
        fs::create_dir_all(&directory)?;
        self.write_dimension(
            ENCODE_TEMPLATE_TERM_SPILL_TABLE,
            "payload_template",
            payload_count,
            &directory,
        )?;
        self.write_dimension(
            ENCODE_CONTENT_TERM_SPILL_TABLE,
            "payload_content",
            payload_count,
            &directory,
        )?;
        DiskPayloadTermSoA::open(&directory, cleanup)
    }

    fn write_dimension(
        &self,
        table: &str,
        prefix: &str,
        payload_count: usize,
        directory: &Path,
    ) -> Result<(), AnalysisError> {
        let (row_count, distinct_terms): (u64, u64) = self.conn.query_row(
            &format!("SELECT count(*)::UBIGINT, count(DISTINCT token)::UBIGINT FROM {table}"),
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if distinct_terms > u64::from(u32::MAX) {
            return Err(AnalysisError::InvalidData(format!(
                "{prefix} dictionary exceeds u32 identity space"
            )));
        }
        let offset_count = u64::try_from(payload_count)
            .ok()
            .and_then(|count| count.checked_add(1))
            .ok_or_else(|| {
                AnalysisError::InvalidData("payload term offset count overflow".into())
            })?;
        let mut offsets = TypedArraySink::create(
            &directory.join(format!("{prefix}_offsets.u64")),
            ArrayKind::U64,
            offset_count,
        )
        .map_err(encode_err)?;
        let mut terms = TypedArraySink::create(
            &directory.join(format!("{prefix}_terms.u32")),
            ArrayKind::U32,
            row_count,
        )
        .map_err(encode_err)?;
        let mut freqs = TypedArraySink::create(
            &directory.join(format!("{prefix}_freqs.u32")),
            ArrayKind::U32,
            row_count,
        )
        .map_err(encode_err)?;
        offsets.push_u64(0).map_err(encode_err)?;

        let sql = format!(
            "WITH dictionary AS (
                 SELECT token,
                        (row_number() OVER (ORDER BY first_request, token) - 1)::UINTEGER AS term_id
                 FROM (
                     SELECT token, min(request_order)::UBIGINT AS first_request
                     FROM {table}
                     GROUP BY token
                 ) first_occurrence
             )
             SELECT rows.payload_id::UINTEGER AS payload_id,
                    dictionary.term_id::UINTEGER AS term_id,
                    rows.frequency::UINTEGER AS frequency
             FROM {table} rows
             JOIN dictionary USING (token)
             ORDER BY rows.payload_id, dictionary.term_id"
        );
        let mut statement = self.conn.prepare(&sql)?;
        let batches = statement.stream_arrow(
            [],
            Arc::new(Schema::new(vec![
                Field::new("payload_id", DataType::UInt32, false),
                Field::new("term_id", DataType::UInt32, false),
                Field::new("frequency", DataType::UInt32, false),
            ])),
        )?;
        let mut open_payload = 0usize;
        let mut written_terms = 0u64;
        let mut previous = None::<(u32, u32)>;
        for batch in batches {
            let payload_ids = required_arrow_column::<UInt32Array>(&batch, 0, "payload_id")?;
            let term_ids = required_arrow_column::<UInt32Array>(&batch, 1, "term_id")?;
            let frequencies = required_arrow_column::<UInt32Array>(&batch, 2, "frequency")?;
            for row in 0..batch.num_rows() {
                if payload_ids.is_null(row) || term_ids.is_null(row) || frequencies.is_null(row) {
                    return Err(AnalysisError::InvalidData(format!(
                        "{prefix} spill query returned NULL"
                    )));
                }
                let payload_id = payload_ids.value(row);
                let payload = payload_id as usize;
                if payload >= payload_count {
                    return Err(AnalysisError::InvalidData(format!(
                        "{prefix} spill payload {payload_id} is out of range"
                    )));
                }
                while open_payload < payload {
                    offsets.push_u64(written_terms).map_err(encode_err)?;
                    open_payload += 1;
                }
                let term_id = term_ids.value(row);
                if previous.is_some_and(|(previous_payload, previous_term)| {
                    previous_payload > payload_id
                        || (previous_payload == payload_id && previous_term >= term_id)
                }) {
                    return Err(AnalysisError::InvalidData(format!(
                        "{prefix} spill output is not canonical"
                    )));
                }
                previous = Some((payload_id, term_id));
                terms.push_u32(term_id).map_err(encode_err)?;
                freqs.push_u32(frequencies.value(row)).map_err(encode_err)?;
                written_terms = written_terms.checked_add(1).ok_or_else(|| {
                    AnalysisError::InvalidData("payload term spill count overflow".into())
                })?;
            }
        }
        while open_payload < payload_count {
            offsets.push_u64(written_terms).map_err(encode_err)?;
            open_payload += 1;
        }
        if written_terms != row_count {
            return Err(AnalysisError::InvalidData(format!(
                "{prefix} spill row count changed during materialization: expected={row_count}, actual={written_terms}"
            )));
        }
        offsets.finish().map_err(encode_err)?;
        terms.finish().map_err(encode_err)?;
        freqs.finish().map_err(encode_err)?;
        Ok(())
    }
}

impl Drop for DuckDbPayloadTermSpill<'_> {
    fn drop(&mut self) {
        let _ = self.conn.execute_batch(&format!(
            "DROP TABLE IF EXISTS {ENCODE_TEMPLATE_TERM_SPILL_TABLE};
             DROP TABLE IF EXISTS {ENCODE_CONTENT_TERM_SPILL_TABLE};"
        ));
    }
}

const ENCODE_CSR_SPILL_TABLE: &str = "encode_csr_membership_spill";
const ENCODE_ATOM_SPILL_TABLE: &str = "encode_fallback_atom_spill";

struct DiskBidirectionalCsr {
    contract_token_offsets: MappedU64Array,
    contract_tokens: MappedU32Array,
    token_member_offsets: MappedU64Array,
    token_member_contracts: MappedU32Array,
    token_member_sources: MappedU32Array,
    _cleanup: metadata_engine::artifacts::StagingCleanupGuard,
}

impl DiskBidirectionalCsr {
    fn view(&self) -> BidirectionalCsrView<'_> {
        BidirectionalCsrView {
            contract_token_offsets: &self.contract_token_offsets,
            contract_tokens: &self.contract_tokens,
            token_member_offsets: &self.token_member_offsets,
            token_member_contracts: &self.token_member_contracts,
            token_member_sources: &self.token_member_sources,
        }
    }

    fn open(
        directory: &Path,
        cleanup: metadata_engine::artifacts::StagingCleanupGuard,
    ) -> Result<Self, AnalysisError> {
        Ok(Self {
            contract_token_offsets: map_u64_array(&directory.join("contract_token_offsets.u64"))
                .map_err(encode_err)?,
            contract_tokens: map_u32_array(&directory.join("contract_tokens.u32"))
                .map_err(encode_err)?,
            token_member_offsets: map_u64_array(&directory.join("token_member_offsets.u64"))
                .map_err(encode_err)?,
            token_member_contracts: map_u32_array(&directory.join("token_member_contracts.u32"))
                .map_err(encode_err)?,
            token_member_sources: map_u32_array(&directory.join("token_member_sources.u32"))
                .map_err(encode_err)?,
            _cleanup: cleanup,
        })
    }
}

struct DuckDbCsrSpill<'connection> {
    conn: &'connection Connection,
}

impl<'connection> DuckDbCsrSpill<'connection> {
    fn create(conn: &'connection Connection) -> Result<Self, AnalysisError> {
        conn.execute_batch(&format!(
            "DROP TABLE IF EXISTS {ENCODE_CSR_SPILL_TABLE};
             CREATE TEMP TABLE {ENCODE_CSR_SPILL_TABLE}(
                 source_doc_id UINTEGER NOT NULL,
                 contract_id UINTEGER NOT NULL,
                 token_id UINTEGER NOT NULL
             );"
        ))?;
        Ok(Self { conn })
    }

    fn build(
        &self,
        work_directory: &Path,
        sources: EncodeSourceView<'_>,
        batch_records: usize,
    ) -> Result<DiskBidirectionalCsr, AnalysisError> {
        self.append_sources(sources, batch_records.max(1))?;
        let contract_count = sources
            .contract_ids
            .iter()
            .copied()
            .max()
            .map_or(0usize, |id| id as usize + 1);
        let token_count = sources
            .token_ids
            .iter()
            .copied()
            .max()
            .map_or(0usize, |id| id as usize + 1);
        let directory = work_directory.join("artifacts/metadata").join(format!(
            "{ENCODE_CSR_SPILL_PREFIX}{}",
            metadata_engine::artifacts::new_artifact_run_id()
        ));
        let cleanup = metadata_engine::artifacts::StagingCleanupGuard::new([directory.clone()]);
        fs::create_dir_all(&directory)?;
        self.write_contract_direction(contract_count, &directory)?;
        self.write_token_direction(token_count, &directory)?;
        DiskBidirectionalCsr::open(&directory, cleanup)
    }

    fn append_sources(
        &self,
        sources: EncodeSourceView<'_>,
        batch_records: usize,
    ) -> Result<(), AnalysisError> {
        let mut source_ids = Vec::with_capacity(batch_records);
        let mut contract_ids = Vec::with_capacity(batch_records);
        let mut token_ids = Vec::with_capacity(batch_records);
        let mut appender = self.conn.appender(ENCODE_CSR_SPILL_TABLE)?;
        for source in 0..sources.source_count() {
            let source_id = u32::try_from(source).map_err(|_| {
                AnalysisError::InvalidData("CSR source identity exceeds u32".into())
            })?;
            let contract_id = sources.contract_ids[source];
            for &token_id in sources.tokens_of(source) {
                source_ids.push(source_id);
                contract_ids.push(contract_id);
                token_ids.push(token_id);
                if source_ids.len() == batch_records {
                    append_csr_record_batch(
                        &mut appender,
                        &mut source_ids,
                        &mut contract_ids,
                        &mut token_ids,
                    )?;
                }
            }
        }
        append_csr_record_batch(
            &mut appender,
            &mut source_ids,
            &mut contract_ids,
            &mut token_ids,
        )?;
        appender.flush()?;
        Ok(())
    }

    fn write_contract_direction(
        &self,
        contract_count: usize,
        directory: &Path,
    ) -> Result<(), AnalysisError> {
        let row_count: u64 = self.conn.query_row(
            &format!(
                "SELECT count(*)::UBIGINT
                 FROM (
                     SELECT DISTINCT contract_id, token_id
                     FROM {ENCODE_CSR_SPILL_TABLE}
                 ) memberships"
            ),
            [],
            |row| row.get(0),
        )?;
        let mut offsets = TypedArraySink::create(
            &directory.join("contract_token_offsets.u64"),
            ArrayKind::U64,
            contract_count as u64 + 1,
        )
        .map_err(encode_err)?;
        let mut values = TypedArraySink::create(
            &directory.join("contract_tokens.u32"),
            ArrayKind::U32,
            row_count,
        )
        .map_err(encode_err)?;
        offsets.push_u64(0).map_err(encode_err)?;
        let mut statement = self.conn.prepare(&format!(
            "SELECT contract_id::UINTEGER, token_id::UINTEGER
             FROM {ENCODE_CSR_SPILL_TABLE}
             GROUP BY contract_id, token_id
             ORDER BY contract_id, token_id"
        ))?;
        let batches = statement.stream_arrow(
            [],
            Arc::new(Schema::new(vec![
                Field::new("contract_id", DataType::UInt32, false),
                Field::new("token_id", DataType::UInt32, false),
            ])),
        )?;
        let mut open_contract = 0usize;
        let mut written = 0u64;
        for batch in batches {
            let contracts = required_arrow_column::<UInt32Array>(&batch, 0, "contract_id")?;
            let tokens = required_arrow_column::<UInt32Array>(&batch, 1, "token_id")?;
            for row in 0..batch.num_rows() {
                let contract = contracts.value(row) as usize;
                if contract >= contract_count {
                    return Err(AnalysisError::InvalidData(
                        "external CSR contract is out of range".into(),
                    ));
                }
                while open_contract < contract {
                    offsets.push_u64(written).map_err(encode_err)?;
                    open_contract += 1;
                }
                values.push_u32(tokens.value(row)).map_err(encode_err)?;
                written = written.checked_add(1).ok_or_else(|| {
                    AnalysisError::InvalidData("external CSR membership overflow".into())
                })?;
            }
        }
        while open_contract < contract_count {
            offsets.push_u64(written).map_err(encode_err)?;
            open_contract += 1;
        }
        if written != row_count {
            return Err(AnalysisError::InvalidData(
                "external contract CSR row count changed".into(),
            ));
        }
        offsets.finish().map_err(encode_err)?;
        values.finish().map_err(encode_err)?;
        Ok(())
    }

    fn write_token_direction(
        &self,
        token_count: usize,
        directory: &Path,
    ) -> Result<(), AnalysisError> {
        let row_count: u64 = self.conn.query_row(
            &format!(
                "SELECT count(*)::UBIGINT
                 FROM (
                     SELECT DISTINCT token_id, contract_id, source_doc_id
                     FROM {ENCODE_CSR_SPILL_TABLE}
                 ) memberships"
            ),
            [],
            |row| row.get(0),
        )?;
        let mut offsets = TypedArraySink::create(
            &directory.join("token_member_offsets.u64"),
            ArrayKind::U64,
            token_count as u64 + 1,
        )
        .map_err(encode_err)?;
        let mut contracts = TypedArraySink::create(
            &directory.join("token_member_contracts.u32"),
            ArrayKind::U32,
            row_count,
        )
        .map_err(encode_err)?;
        let mut sources = TypedArraySink::create(
            &directory.join("token_member_sources.u32"),
            ArrayKind::U32,
            row_count,
        )
        .map_err(encode_err)?;
        offsets.push_u64(0).map_err(encode_err)?;
        let mut statement = self.conn.prepare(&format!(
            "SELECT token_id::UINTEGER, contract_id::UINTEGER, source_doc_id::UINTEGER
             FROM {ENCODE_CSR_SPILL_TABLE}
             GROUP BY token_id, contract_id, source_doc_id
             ORDER BY token_id, contract_id, source_doc_id"
        ))?;
        let batches = statement.stream_arrow(
            [],
            Arc::new(Schema::new(vec![
                Field::new("token_id", DataType::UInt32, false),
                Field::new("contract_id", DataType::UInt32, false),
                Field::new("source_doc_id", DataType::UInt32, false),
            ])),
        )?;
        let mut open_token = 0usize;
        let mut written = 0u64;
        for batch in batches {
            let tokens = required_arrow_column::<UInt32Array>(&batch, 0, "token_id")?;
            let contract_ids = required_arrow_column::<UInt32Array>(&batch, 1, "contract_id")?;
            let source_ids = required_arrow_column::<UInt32Array>(&batch, 2, "source_doc_id")?;
            for row in 0..batch.num_rows() {
                let token = tokens.value(row) as usize;
                if token >= token_count {
                    return Err(AnalysisError::InvalidData(
                        "external CSR token is out of range".into(),
                    ));
                }
                while open_token < token {
                    offsets.push_u64(written).map_err(encode_err)?;
                    open_token += 1;
                }
                contracts
                    .push_u32(contract_ids.value(row))
                    .map_err(encode_err)?;
                sources
                    .push_u32(source_ids.value(row))
                    .map_err(encode_err)?;
                written = written.checked_add(1).ok_or_else(|| {
                    AnalysisError::InvalidData("external CSR membership overflow".into())
                })?;
            }
        }
        while open_token < token_count {
            offsets.push_u64(written).map_err(encode_err)?;
            open_token += 1;
        }
        if written != row_count {
            return Err(AnalysisError::InvalidData(
                "external token CSR row count changed".into(),
            ));
        }
        offsets.finish().map_err(encode_err)?;
        contracts.finish().map_err(encode_err)?;
        sources.finish().map_err(encode_err)?;
        Ok(())
    }
}

impl Drop for DuckDbCsrSpill<'_> {
    fn drop(&mut self) {
        let _ = self
            .conn
            .execute_batch(&format!("DROP TABLE IF EXISTS {ENCODE_CSR_SPILL_TABLE};"));
    }
}

fn append_csr_record_batch(
    appender: &mut duckdb::Appender<'_>,
    source_ids: &mut Vec<u32>,
    contract_ids: &mut Vec<u32>,
    token_ids: &mut Vec<u32>,
) -> Result<(), AnalysisError> {
    if source_ids.is_empty() {
        return Ok(());
    }
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("source_doc_id", DataType::UInt32, false),
            Field::new("contract_id", DataType::UInt32, false),
            Field::new("token_id", DataType::UInt32, false),
        ])),
        vec![
            Arc::new(UInt32Array::from(take_vec_preserving_capacity(source_ids))),
            Arc::new(UInt32Array::from(take_vec_preserving_capacity(
                contract_ids,
            ))),
            Arc::new(UInt32Array::from(take_vec_preserving_capacity(token_ids))),
        ],
    )
    .map_err(encode_err)?;
    appender.append_record_batch(batch)?;
    Ok(())
}

struct DuckDbAtomSpill<'connection> {
    conn: &'connection Connection,
}

impl<'connection> DuckDbAtomSpill<'connection> {
    fn create(conn: &'connection Connection) -> Result<Self, AnalysisError> {
        conn.execute_batch(&format!(
            "DROP TABLE IF EXISTS {ENCODE_ATOM_SPILL_TABLE};
             CREATE TEMP TABLE {ENCODE_ATOM_SPILL_TABLE}(
                 chain_id UINTEGER NOT NULL,
                 feature_id UINTEGER NOT NULL,
                 payload_id UINTEGER NOT NULL,
                 contract_id UINTEGER NOT NULL
             );"
        ))?;
        Ok(Self { conn })
    }

    fn build(
        &self,
        work_directory: &Path,
        contracts: EncodeContractView<'_>,
        payload_feature_identity: &[u32],
        batch_records: usize,
    ) -> Result<DiskFallbackAtoms, AnalysisError> {
        let batch_records = batch_records.max(1);
        let mut chain_ids = Vec::with_capacity(batch_records);
        let mut feature_ids = Vec::with_capacity(batch_records);
        let mut payload_ids = Vec::with_capacity(batch_records);
        let mut contract_ids = Vec::with_capacity(batch_records);
        let mut appender = self.conn.appender(ENCODE_ATOM_SPILL_TABLE)?;
        for index in 0..contracts.contract_count() {
            let payload_id = contracts.payload_ids[index];
            let feature_id = *payload_feature_identity
                .get(payload_id as usize)
                .ok_or_else(|| {
                    AnalysisError::InvalidData("atom feature identity out of range".into())
                })?;
            chain_ids.push(contracts.chain_ids[index]);
            feature_ids.push(feature_id);
            payload_ids.push(payload_id);
            contract_ids.push(contracts.contract_ids[index]);
            if chain_ids.len() == batch_records {
                append_atom_record_batch(
                    &mut appender,
                    &mut chain_ids,
                    &mut feature_ids,
                    &mut payload_ids,
                    &mut contract_ids,
                )?;
            }
        }
        append_atom_record_batch(
            &mut appender,
            &mut chain_ids,
            &mut feature_ids,
            &mut payload_ids,
            &mut contract_ids,
        )?;
        appender.flush()?;
        drop(appender);

        let atom_count: u64 = self.conn.query_row(
            &format!(
                "SELECT count(*)::UBIGINT
                 FROM (
                     SELECT chain_id, feature_id
                     FROM {ENCODE_ATOM_SPILL_TABLE}
                     GROUP BY chain_id, feature_id
                 ) atoms"
            ),
            [],
            |row| row.get(0),
        )?;
        let member_count = contracts.contract_count() as u64;
        let directory = work_directory.join("artifacts/metadata").join(format!(
            "{ENCODE_ATOM_SPILL_PREFIX}{}",
            metadata_engine::artifacts::new_artifact_run_id()
        ));
        let cleanup = metadata_engine::artifacts::StagingCleanupGuard::new([directory.clone()]);
        fs::create_dir_all(&directory)?;
        let mut offsets = TypedArraySink::create(
            &directory.join("fallback_atom_offsets.u64"),
            ArrayKind::U64,
            atom_count + 1,
        )
        .map_err(encode_err)?;
        let mut members = TypedArraySink::create(
            &directory.join("fallback_atom_members.u32"),
            ArrayKind::U32,
            member_count,
        )
        .map_err(encode_err)?;
        let mut atom_payloads = TypedArraySink::create(
            &directory.join("fallback_atom_payloads.u32"),
            ArrayKind::U32,
            atom_count,
        )
        .map_err(encode_err)?;
        offsets.push_u64(0).map_err(encode_err)?;
        let mut statement = self.conn.prepare(&format!(
            "SELECT chain_id::UINTEGER,
                    feature_id::UINTEGER,
                    min(payload_id) OVER (
                        PARTITION BY chain_id, feature_id
                    )::UINTEGER AS atom_payload_id,
                    contract_id::UINTEGER
             FROM {ENCODE_ATOM_SPILL_TABLE}
             ORDER BY chain_id, feature_id, contract_id"
        ))?;
        let batches = statement.stream_arrow(
            [],
            Arc::new(Schema::new(vec![
                Field::new("chain_id", DataType::UInt32, false),
                Field::new("feature_id", DataType::UInt32, false),
                Field::new("atom_payload_id", DataType::UInt32, false),
                Field::new("contract_id", DataType::UInt32, false),
            ])),
        )?;
        let mut previous_key = None::<(u32, u32)>;
        let mut written_members = 0u64;
        let mut written_atoms = 0u64;
        for batch in batches {
            let chains = required_arrow_column::<UInt32Array>(&batch, 0, "chain_id")?;
            let features = required_arrow_column::<UInt32Array>(&batch, 1, "feature_id")?;
            let payloads = required_arrow_column::<UInt32Array>(&batch, 2, "atom_payload_id")?;
            let contracts = required_arrow_column::<UInt32Array>(&batch, 3, "contract_id")?;
            for row in 0..batch.num_rows() {
                let key = (chains.value(row), features.value(row));
                if previous_key != Some(key) {
                    if previous_key.is_some() {
                        offsets.push_u64(written_members).map_err(encode_err)?;
                    }
                    atom_payloads
                        .push_u32(payloads.value(row))
                        .map_err(encode_err)?;
                    written_atoms += 1;
                    previous_key = Some(key);
                }
                members.push_u32(contracts.value(row)).map_err(encode_err)?;
                written_members += 1;
            }
        }
        if previous_key.is_some() {
            offsets.push_u64(written_members).map_err(encode_err)?;
        }
        if written_atoms != atom_count || written_members != member_count {
            return Err(AnalysisError::InvalidData(
                "external fallback atom cardinality changed".into(),
            ));
        }
        offsets.finish().map_err(encode_err)?;
        members.finish().map_err(encode_err)?;
        atom_payloads.finish().map_err(encode_err)?;
        Ok(DiskFallbackAtoms {
            offsets: map_u64_array(&directory.join("fallback_atom_offsets.u64"))
                .map_err(encode_err)?,
            members: map_u32_array(&directory.join("fallback_atom_members.u32"))
                .map_err(encode_err)?,
            atom_payloads: map_u32_array(&directory.join("fallback_atom_payloads.u32"))
                .map_err(encode_err)?,
            _cleanup: cleanup,
        })
    }
}

impl Drop for DuckDbAtomSpill<'_> {
    fn drop(&mut self) {
        let _ = self
            .conn
            .execute_batch(&format!("DROP TABLE IF EXISTS {ENCODE_ATOM_SPILL_TABLE};"));
    }
}

fn append_atom_record_batch(
    appender: &mut duckdb::Appender<'_>,
    chain_ids: &mut Vec<u32>,
    feature_ids: &mut Vec<u32>,
    payload_ids: &mut Vec<u32>,
    contract_ids: &mut Vec<u32>,
) -> Result<(), AnalysisError> {
    if chain_ids.is_empty() {
        return Ok(());
    }
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("chain_id", DataType::UInt32, false),
            Field::new("feature_id", DataType::UInt32, false),
            Field::new("payload_id", DataType::UInt32, false),
            Field::new("contract_id", DataType::UInt32, false),
        ])),
        vec![
            Arc::new(UInt32Array::from(take_vec_preserving_capacity(chain_ids))),
            Arc::new(UInt32Array::from(take_vec_preserving_capacity(feature_ids))),
            Arc::new(UInt32Array::from(take_vec_preserving_capacity(payload_ids))),
            Arc::new(UInt32Array::from(take_vec_preserving_capacity(
                contract_ids,
            ))),
        ],
    )
    .map_err(encode_err)?;
    appender.append_record_batch(batch)?;
    Ok(())
}

fn take_vec_preserving_capacity<T>(values: &mut Vec<T>) -> Vec<T> {
    let capacity = values.capacity();
    std::mem::replace(values, Vec::with_capacity(capacity))
}

struct DuckDbAtomSketchSpill<'connection> {
    conn: &'connection Connection,
}

impl<'connection> DuckDbAtomSketchSpill<'connection> {
    fn create(conn: &'connection Connection) -> Result<Self, AnalysisError> {
        conn.execute_batch(&format!(
            "DROP TABLE IF EXISTS {ENCODE_ATOM_TERM_TABLE};
             DROP TABLE IF EXISTS {ENCODE_ATOM_TERM_DF_TABLE};
             CREATE TEMP TABLE {ENCODE_ATOM_TERM_TABLE}(
                 dimension UINTEGER NOT NULL,
                 atom UINTEGER NOT NULL,
                 term UINTEGER NOT NULL
             );"
        ))?;
        Ok(Self { conn })
    }

    fn build(
        &self,
        work_directory: &Path,
        payloads: PayloadTermView<'_>,
        atom_payloads: &[u32],
        batch_records: usize,
    ) -> Result<DiskAtomSketches, AnalysisError> {
        let batch_records = batch_records.max(1);
        let mut dimensions = Vec::with_capacity(batch_records);
        let mut atoms = Vec::with_capacity(batch_records);
        let mut terms = Vec::with_capacity(batch_records);
        let mut appender = self.conn.appender(ENCODE_ATOM_TERM_TABLE)?;
        for (atom, &payload_id) in atom_payloads.iter().enumerate() {
            let atom = u32::try_from(atom).map_err(|_| {
                AnalysisError::InvalidData("atom sketch identity exceeds u32".into())
            })?;
            let payload = payload_id as usize;
            for (dimension, dimension_terms) in [
                (0u32, payloads.template_term_ids(payload)),
                (1u32, payloads.content_term_ids(payload)),
            ] {
                for &term in dimension_terms {
                    dimensions.push(dimension);
                    atoms.push(atom);
                    terms.push(term);
                    if dimensions.len() == batch_records {
                        append_atom_term_record_batch(
                            &mut appender,
                            &mut dimensions,
                            &mut atoms,
                            &mut terms,
                        )?;
                    }
                }
            }
        }
        append_atom_term_record_batch(&mut appender, &mut dimensions, &mut atoms, &mut terms)?;
        appender.flush()?;
        drop(appender);
        self.conn.execute_batch(&format!(
            "CREATE TEMP TABLE {ENCODE_ATOM_TERM_DF_TABLE} AS
             SELECT terms.dimension,
                    terms.atom,
                    terms.term,
                    frequencies.document_frequency
             FROM {ENCODE_ATOM_TERM_TABLE} terms
             JOIN (
                 SELECT dimension,
                        term,
                        count(*)::UINTEGER AS document_frequency
                 FROM {ENCODE_ATOM_TERM_TABLE}
                 GROUP BY dimension, term
             ) frequencies
             USING (dimension, term)
             ORDER BY terms.dimension, terms.atom, terms.term;
             DROP TABLE {ENCODE_ATOM_TERM_TABLE};"
        ))?;

        let directory = work_directory.join("artifacts/metadata").join(format!(
            "{ENCODE_SKETCH_SPILL_PREFIX}{}",
            metadata_engine::artifacts::new_artifact_run_id()
        ));
        let cleanup = metadata_engine::artifacts::StagingCleanupGuard::new([directory.clone()]);
        fs::create_dir_all(&directory)?;
        self.materialize_dimension(&directory, atom_payloads.len(), false)?;
        self.materialize_dimension(&directory, atom_payloads.len(), true)?;
        DiskAtomSketches::open(&directory, cleanup)
    }

    fn materialize_dimension(
        &self,
        directory: &Path,
        atom_count: usize,
        content: bool,
    ) -> Result<(), AnalysisError> {
        let (simhash_name, offset_name, anchor_name, has_terms_name, dimension) = if content {
            (
                "atom_content_simhash.u64",
                "atom_content_anchor_offsets.u64",
                "atom_content_anchors.u32",
                "atom_has_content_terms.u8",
                1u32,
            )
        } else {
            (
                "atom_template_simhash.u64",
                "atom_template_anchor_offsets.u64",
                "atom_template_anchors.u32",
                "atom_has_template_terms.u8",
                0u32,
            )
        };
        let atom_count_u64 = atom_count as u64;
        let mut simhashes = TypedArraySink::create(
            &directory.join(simhash_name),
            ArrayKind::U64,
            atom_count_u64,
        )
        .map_err(encode_err)?;
        let mut offsets = TypedArraySink::create(
            &directory.join(offset_name),
            ArrayKind::U64,
            atom_count_u64 + 1,
        )
        .map_err(encode_err)?;
        let mut has_terms = TypedArraySink::create(
            &directory.join(has_terms_name),
            ArrayKind::U8,
            atom_count_u64,
        )
        .map_err(encode_err)?;
        offsets.push_u64(0).map_err(encode_err)?;
        let raw_anchor_path = directory.join(format!("{anchor_name}.raw"));
        let mut raw_anchors = BufWriter::new(fs::File::create(&raw_anchor_path)?);
        let mut anchor_count = 0u64;
        let mut next_atom = 0usize;
        let mut active_atom = None::<usize>;
        let mut accumulator = AtomDimensionAccumulator::default();

        let mut statement = self.conn.prepare(&format!(
            "SELECT atom::UINTEGER,
                    term::UINTEGER,
                    document_frequency::UINTEGER
             FROM {ENCODE_ATOM_TERM_DF_TABLE}
             WHERE dimension = {dimension}
             ORDER BY atom, term"
        ))?;
        let batches = statement.stream_arrow(
            [],
            Arc::new(Schema::new(vec![
                Field::new("atom", DataType::UInt32, false),
                Field::new("term", DataType::UInt32, false),
                Field::new("document_frequency", DataType::UInt32, false),
            ])),
        )?;
        for batch in batches {
            let atoms = required_arrow_column::<UInt32Array>(&batch, 0, "atom")?;
            let terms = required_arrow_column::<UInt32Array>(&batch, 1, "term")?;
            let frequencies =
                required_arrow_column::<UInt32Array>(&batch, 2, "document_frequency")?;
            for row in 0..batch.num_rows() {
                let atom = atoms.value(row) as usize;
                if atom >= atom_count {
                    return Err(AnalysisError::InvalidData(
                        "external atom sketch identity out of range".into(),
                    ));
                }
                if active_atom != Some(atom) {
                    if let Some(previous) = active_atom.take() {
                        write_external_dimension_sketch(
                            std::mem::take(&mut accumulator).finish(),
                            &mut simhashes,
                            &mut offsets,
                            &mut has_terms,
                            &mut raw_anchors,
                            &mut anchor_count,
                        )?;
                        next_atom = previous + 1;
                    }
                    while next_atom < atom {
                        write_external_dimension_sketch(
                            AtomDimensionAccumulator::default().finish(),
                            &mut simhashes,
                            &mut offsets,
                            &mut has_terms,
                            &mut raw_anchors,
                            &mut anchor_count,
                        )?;
                        next_atom += 1;
                    }
                    active_atom = Some(atom);
                }
                accumulator.observe(atom_count, terms.value(row), frequencies.value(row));
            }
        }
        if let Some(previous) = active_atom {
            write_external_dimension_sketch(
                accumulator.finish(),
                &mut simhashes,
                &mut offsets,
                &mut has_terms,
                &mut raw_anchors,
                &mut anchor_count,
            )?;
            next_atom = previous + 1;
        }
        while next_atom < atom_count {
            write_external_dimension_sketch(
                AtomDimensionAccumulator::default().finish(),
                &mut simhashes,
                &mut offsets,
                &mut has_terms,
                &mut raw_anchors,
                &mut anchor_count,
            )?;
            next_atom += 1;
        }
        raw_anchors.flush()?;
        raw_anchors.get_ref().sync_all()?;
        drop(raw_anchors);
        simhashes.finish().map_err(encode_err)?;
        offsets.finish().map_err(encode_err)?;
        has_terms.finish().map_err(encode_err)?;

        let mut anchors =
            TypedArraySink::create(&directory.join(anchor_name), ArrayKind::U32, anchor_count)
                .map_err(encode_err)?;
        let mut raw = BufReader::new(fs::File::open(&raw_anchor_path)?);
        let mut bytes = [0u8; 4];
        for _ in 0..anchor_count {
            raw.read_exact(&mut bytes)?;
            anchors
                .push_u32(u32::from_le_bytes(bytes))
                .map_err(encode_err)?;
        }
        if raw.read(&mut bytes[..1])? != 0 {
            return Err(AnalysisError::InvalidData(
                "external atom anchor staging length changed".into(),
            ));
        }
        anchors.finish().map_err(encode_err)?;
        drop(raw);
        fs::remove_file(raw_anchor_path)?;
        Ok(())
    }
}

impl Drop for DuckDbAtomSketchSpill<'_> {
    fn drop(&mut self) {
        let _ = self.conn.execute_batch(&format!(
            "DROP TABLE IF EXISTS {ENCODE_ATOM_TERM_TABLE};
             DROP TABLE IF EXISTS {ENCODE_ATOM_TERM_DF_TABLE};"
        ));
    }
}

fn append_atom_term_record_batch(
    appender: &mut duckdb::Appender<'_>,
    dimensions: &mut Vec<u32>,
    atoms: &mut Vec<u32>,
    terms: &mut Vec<u32>,
) -> Result<(), AnalysisError> {
    if dimensions.is_empty() {
        return Ok(());
    }
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("dimension", DataType::UInt32, false),
            Field::new("atom", DataType::UInt32, false),
            Field::new("term", DataType::UInt32, false),
        ])),
        vec![
            Arc::new(UInt32Array::from(take_vec_preserving_capacity(dimensions))),
            Arc::new(UInt32Array::from(take_vec_preserving_capacity(atoms))),
            Arc::new(UInt32Array::from(take_vec_preserving_capacity(terms))),
        ],
    )
    .map_err(encode_err)?;
    appender.append_record_batch(batch)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn write_external_dimension_sketch(
    sketch: metadata_engine::blocking::AtomDimensionSketch,
    simhashes: &mut TypedArraySink,
    offsets: &mut TypedArraySink,
    has_terms: &mut TypedArraySink,
    raw_anchors: &mut BufWriter<fs::File>,
    anchor_count: &mut u64,
) -> Result<(), AnalysisError> {
    simhashes.push_u64(sketch.simhash).map_err(encode_err)?;
    has_terms
        .push_u8(u8::from(sketch.has_terms))
        .map_err(encode_err)?;
    for &anchor in &sketch.anchors[..sketch.anchor_count as usize] {
        raw_anchors.write_all(&anchor.to_le_bytes())?;
    }
    *anchor_count = anchor_count
        .checked_add(u64::from(sketch.anchor_count))
        .ok_or_else(|| AnalysisError::InvalidData("atom anchor count overflow".into()))?;
    offsets.push_u64(*anchor_count).map_err(encode_err)?;
    Ok(())
}

struct ExternalBlockingSummary {
    block_stats: BlockStats,
    routing_membership_count: u64,
}

struct DuckDbBlockingSpill<'connection> {
    conn: &'connection Connection,
}

impl<'connection> DuckDbBlockingSpill<'connection> {
    fn create(conn: &'connection Connection) -> Result<Self, AnalysisError> {
        conn.execute_batch(&format!(
            "DROP TABLE IF EXISTS {ENCODE_BLOCK_MEMBERSHIP_TABLE};
             DROP TABLE IF EXISTS {ENCODE_BLOCK_DISTINCT_TABLE};
             DROP TABLE IF EXISTS {ENCODE_BLOCK_TABLE};
             CREATE TEMP TABLE {ENCODE_BLOCK_MEMBERSHIP_TABLE}(
                 kind UINTEGER NOT NULL,
                 routing_key UBIGINT NOT NULL,
                 atom UINTEGER NOT NULL
             );"
        ))?;
        Ok(Self { conn })
    }

    #[allow(clippy::too_many_arguments)]
    fn compile(
        &self,
        atoms: AtomSketchView<'_>,
        config: &BlockingCompileConfig,
        out_dir: &Path,
        batch_records: usize,
        lanes: usize,
        mut progress: impl FnMut(ProgressEvent),
    ) -> Result<ExternalBlockingSummary, AnalysisError> {
        fs::create_dir_all(out_dir)?;
        u32::try_from(atoms.len())
            .map_err(|_| AnalysisError::InvalidData("blocking atom count exceeds u32".into()))?;
        let expected_memberships = blocking_membership_count(atoms)?;
        progress(ProgressEvent::determinate(
            ProgressPhase::BlockingCompile,
            0,
            expected_memberships,
            WorkUnit::Items,
            EngineCounters::default(),
        ));
        let batch_records = batch_records.max(1);
        let mut kinds = Vec::with_capacity(batch_records);
        let mut keys = Vec::with_capacity(batch_records);
        let mut atom_ids = Vec::with_capacity(batch_records);
        let mut appender = self.conn.appender(ENCODE_BLOCK_MEMBERSHIP_TABLE)?;
        let mut generated = 0u64;
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(lanes.max(1))
            .thread_name(|index| format!("metadata-blocking-spill-{index}"))
            .build()
            .map_err(|error| {
                AnalysisError::InvalidData(format!("external blocking worker pool: {error}"))
            })?;
        let atoms_per_batch = (batch_records / 96).max(1);
        for start in (0..atoms.len()).step_by(atoms_per_batch) {
            let end = start.saturating_add(atoms_per_batch).min(atoms.len());
            let rows = pool.install(|| {
                (start..end)
                    .into_par_iter()
                    .fold(Vec::<PendingBlockMembership>::new, |mut rows, atom| {
                        append_atom_block_memberships(atoms, atom, &mut rows);
                        rows
                    })
                    .reduce(Vec::<PendingBlockMembership>::new, |mut left, mut right| {
                        left.append(&mut right);
                        left
                    })
            });
            generated = generated
                .checked_add(rows.len() as u64)
                .ok_or_else(|| AnalysisError::InvalidData("blocking membership overflow".into()))?;
            for row in rows {
                kinds.push(row.kind);
                keys.push(row.key);
                atom_ids.push(row.atom);
            }
            append_block_membership_record_batch(
                &mut appender,
                &mut kinds,
                &mut keys,
                &mut atom_ids,
            )?;
            progress(ProgressEvent::determinate(
                ProgressPhase::BlockingCompile,
                generated,
                expected_memberships,
                WorkUnit::Items,
                EngineCounters::default(),
            ));
        }
        append_block_membership_record_batch(&mut appender, &mut kinds, &mut keys, &mut atom_ids)?;
        appender.flush()?;
        drop(appender);
        if generated != expected_memberships {
            return Err(AnalysisError::InvalidData(format!(
                "blocking membership estimate changed: generated={generated}, expected={expected_memberships}"
            )));
        }
        progress(ProgressEvent::determinate(
            ProgressPhase::BlockingCompile,
            generated,
            expected_memberships,
            WorkUnit::Items,
            EngineCounters::default(),
        ));

        self.conn.execute_batch(&format!(
            "CREATE TEMP TABLE {ENCODE_BLOCK_DISTINCT_TABLE} AS
             SELECT kind, routing_key, atom
             FROM {ENCODE_BLOCK_MEMBERSHIP_TABLE}
             GROUP BY kind, routing_key, atom;
             DROP TABLE {ENCODE_BLOCK_MEMBERSHIP_TABLE};
             CREATE TEMP TABLE {ENCODE_BLOCK_TABLE} AS
             SELECT kind,
                    routing_key,
                    (row_number() OVER (ORDER BY kind, routing_key) - 1)::UINTEGER AS block_id,
                    member_count
             FROM (
                 SELECT kind,
                        routing_key,
                        count(*)::UINTEGER AS member_count
                 FROM {ENCODE_BLOCK_DISTINCT_TABLE}
                 GROUP BY kind, routing_key
             ) blocks
             ORDER BY kind, routing_key;"
        ))?;
        let block_count: u64 = self.conn.query_row(
            &format!("SELECT count(*)::UBIGINT FROM {ENCODE_BLOCK_TABLE}"),
            [],
            |row| row.get(0),
        )?;
        let membership_count: u64 = self.conn.query_row(
            &format!("SELECT count(*)::UBIGINT FROM {ENCODE_BLOCK_DISTINCT_TABLE}"),
            [],
            |row| row.get(0),
        )?;
        if membership_count != expected_memberships {
            return Err(AnalysisError::InvalidData(format!(
                "blocking membership dedup changed cardinality: {membership_count}/{expected_memberships}"
            )));
        }
        u32::try_from(block_count)
            .map_err(|_| AnalysisError::InvalidData("blocking block count exceeds u32".into()))?;

        let finalize_total = block_count
            .checked_mul(2)
            .and_then(|value| value.checked_add((atoms.len() as u64).checked_mul(2)?))
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| {
                AnalysisError::InvalidData("blocking finalize progress overflow".into())
            })?;
        progress(ProgressEvent::determinate(
            ProgressPhase::BlockingFinalize,
            0,
            finalize_total,
            WorkUnit::Items,
            EngineCounters::default(),
        ));
        self.write_atom_identity_columns(out_dir, atoms)?;
        self.write_forward_columns(out_dir, config, block_count, membership_count)?;
        self.write_inverse_columns(out_dir, atoms, config, block_count, membership_count)?;
        let block_stats = self.block_stats(atoms.len(), block_count, membership_count)?;
        block_stats
            .write_bin(&out_dir.join("block_stats.bin"))
            .map_err(encode_err)?;
        progress(ProgressEvent::determinate(
            ProgressPhase::BlockingFinalize,
            finalize_total,
            finalize_total,
            WorkUnit::Items,
            EngineCounters::default(),
        ));
        Ok(ExternalBlockingSummary {
            block_stats,
            routing_membership_count: membership_count,
        })
    }

    fn write_atom_identity_columns(
        &self,
        out_dir: &Path,
        atoms: AtomSketchView<'_>,
    ) -> Result<(), AnalysisError> {
        metadata_engine::format::write_u32_iter(
            &out_dir.join("atom_primary_storage_shard.u32"),
            ArrayKind::U32,
            atoms.len() as u64,
            (0..atoms.len()).map(|atom| atom as u32),
        )
        .map_err(encode_err)?;
        metadata_engine::format::write_u64_iter(
            &out_dir.join("atom_template_simhash.u64"),
            ArrayKind::U64,
            atoms.len() as u64,
            atoms.template_simhashes.iter().copied(),
        )
        .map_err(encode_err)?;
        metadata_engine::format::write_u64_iter(
            &out_dir.join("atom_content_simhash.u64"),
            ArrayKind::U64,
            atoms.len() as u64,
            atoms.content_simhashes.iter().copied(),
        )
        .map_err(encode_err)?;
        Ok(())
    }

    fn write_forward_columns(
        &self,
        out_dir: &Path,
        config: &BlockingCompileConfig,
        block_count: u64,
        membership_count: u64,
    ) -> Result<(), AnalysisError> {
        let mut offsets = TypedArraySink::create(
            &out_dir.join("block_atom_offsets.u64"),
            ArrayKind::U64,
            block_count + 1,
        )
        .map_err(encode_err)?;
        let mut members = TypedArraySink::create(
            &out_dir.join("block_atoms.u32"),
            ArrayKind::U32,
            membership_count,
        )
        .map_err(encode_err)?;
        let mut kinds = TypedArraySink::create(
            &out_dir.join("block_kinds.u32"),
            ArrayKind::U32,
            block_count,
        )
        .map_err(encode_err)?;
        let mut keys =
            TypedArraySink::create(&out_dir.join("block_keys.u64"), ArrayKind::U64, block_count)
                .map_err(encode_err)?;
        let mut hot_plans =
            HotBlockPlanSink::create(&out_dir.join("hot_block_plans.bin")).map_err(encode_err)?;
        offsets.push_u64(0).map_err(encode_err)?;
        let mut statement = self.conn.prepare(&format!(
            "SELECT blocks.block_id::UINTEGER,
                    blocks.kind::UINTEGER,
                    blocks.routing_key::UBIGINT,
                    memberships.atom::UINTEGER,
                    blocks.member_count::UINTEGER
             FROM {ENCODE_BLOCK_TABLE} blocks
             JOIN {ENCODE_BLOCK_DISTINCT_TABLE} memberships
             USING (kind, routing_key)
             ORDER BY blocks.block_id, memberships.atom"
        ))?;
        let batches = statement.stream_arrow(
            [],
            Arc::new(Schema::new(vec![
                Field::new("block_id", DataType::UInt32, false),
                Field::new("kind", DataType::UInt32, false),
                Field::new("routing_key", DataType::UInt64, false),
                Field::new("atom", DataType::UInt32, false),
                Field::new("member_count", DataType::UInt32, false),
            ])),
        )?;
        let mut current_block = None::<u32>;
        let mut written_blocks = 0u64;
        let mut written_members = 0u64;
        for batch in batches {
            let block_ids = required_arrow_column::<UInt32Array>(&batch, 0, "block_id")?;
            let block_kinds = required_arrow_column::<UInt32Array>(&batch, 1, "kind")?;
            let block_keys = required_arrow_column::<UInt64Array>(&batch, 2, "routing_key")?;
            let block_atoms = required_arrow_column::<UInt32Array>(&batch, 3, "atom")?;
            let block_sizes = required_arrow_column::<UInt32Array>(&batch, 4, "member_count")?;
            for row in 0..batch.num_rows() {
                let block_id = block_ids.value(row);
                if current_block != Some(block_id) {
                    if current_block.is_some() {
                        offsets.push_u64(written_members).map_err(encode_err)?;
                    }
                    if u64::from(block_id) != written_blocks {
                        return Err(AnalysisError::InvalidData(
                            "external blocking block identities are not dense".into(),
                        ));
                    }
                    let member_count = block_sizes.value(row) as usize;
                    if member_count > config.max_routing_block_members {
                        if config.max_routing_block_members == 0 {
                            return Err(AnalysisError::InvalidData(format!(
                                "hot block {block_id} cannot be planned under cap 0"
                            )));
                        }
                        let tile_size = 1_024u32
                            .min(
                                u32::try_from(config.max_routing_block_members).unwrap_or(u32::MAX),
                            )
                            .max(1);
                        hot_plans
                            .push(&HotBlockPlan::cover_upper_triangle(
                                block_id,
                                member_count as u32,
                                tile_size,
                            ))
                            .map_err(encode_err)?;
                    }
                    kinds.push_u32(block_kinds.value(row)).map_err(encode_err)?;
                    keys.push_u64(block_keys.value(row)).map_err(encode_err)?;
                    written_blocks += 1;
                    current_block = Some(block_id);
                }
                members
                    .push_u32(block_atoms.value(row))
                    .map_err(encode_err)?;
                written_members += 1;
            }
        }
        if current_block.is_some() {
            offsets.push_u64(written_members).map_err(encode_err)?;
        }
        if written_blocks != block_count || written_members != membership_count {
            return Err(AnalysisError::InvalidData(
                "external blocking forward cardinality changed".into(),
            ));
        }
        offsets.finish().map_err(encode_err)?;
        members.finish().map_err(encode_err)?;
        kinds.finish().map_err(encode_err)?;
        keys.finish().map_err(encode_err)?;
        hot_plans.finish().map_err(encode_err)?;
        Ok(())
    }

    fn write_inverse_columns(
        &self,
        out_dir: &Path,
        atoms: AtomSketchView<'_>,
        config: &BlockingCompileConfig,
        _block_count: u64,
        membership_count: u64,
    ) -> Result<(), AnalysisError> {
        let mut offsets = TypedArraySink::create(
            &out_dir.join("atom_block_offsets.u64"),
            ArrayKind::U64,
            atoms.len() as u64 + 1,
        )
        .map_err(encode_err)?;
        let mut block_ids = TypedArraySink::create(
            &out_dir.join("atom_block_ids.u32"),
            ArrayKind::U32,
            membership_count,
        )
        .map_err(encode_err)?;
        let mut statuses = TypedArraySink::create(
            &out_dir.join("atom_routing_status.u8"),
            ArrayKind::U8,
            atoms.len() as u64,
        )
        .map_err(encode_err)?;
        offsets.push_u64(0).map_err(encode_err)?;
        let mut statement = self.conn.prepare(&format!(
            "SELECT memberships.atom::UINTEGER,
                    blocks.block_id::UINTEGER,
                    blocks.member_count::UINTEGER
             FROM {ENCODE_BLOCK_DISTINCT_TABLE} memberships
             JOIN {ENCODE_BLOCK_TABLE} blocks
             USING (kind, routing_key)
             ORDER BY memberships.atom, blocks.block_id"
        ))?;
        let batches = statement.stream_arrow(
            [],
            Arc::new(Schema::new(vec![
                Field::new("atom", DataType::UInt32, false),
                Field::new("block_id", DataType::UInt32, false),
                Field::new("member_count", DataType::UInt32, false),
            ])),
        )?;
        let mut next_atom = 0usize;
        let mut active_atom = None::<usize>;
        let mut active_members = 0u64;
        let mut active_hot = false;
        let mut written = 0u64;
        for batch in batches {
            let atom_ids = required_arrow_column::<UInt32Array>(&batch, 0, "atom")?;
            let blocks = required_arrow_column::<UInt32Array>(&batch, 1, "block_id")?;
            let sizes = required_arrow_column::<UInt32Array>(&batch, 2, "member_count")?;
            for row in 0..batch.num_rows() {
                let atom = atom_ids.value(row) as usize;
                if atom >= atoms.len() {
                    return Err(AnalysisError::InvalidData(
                        "external blocking inverse atom out of range".into(),
                    ));
                }
                if active_atom != Some(atom) {
                    if let Some(previous) = active_atom.take() {
                        finish_external_atom_routing(
                            atoms,
                            previous,
                            active_members,
                            active_hot,
                            written,
                            &mut offsets,
                            &mut statuses,
                        )?;
                        next_atom = previous + 1;
                        active_members = 0;
                        active_hot = false;
                    }
                    while next_atom < atom {
                        finish_external_atom_routing(
                            atoms,
                            next_atom,
                            0,
                            false,
                            written,
                            &mut offsets,
                            &mut statuses,
                        )?;
                        next_atom += 1;
                    }
                    active_atom = Some(atom);
                }
                block_ids.push_u32(blocks.value(row)).map_err(encode_err)?;
                written += 1;
                active_members += 1;
                active_hot |= sizes.value(row) as usize > config.max_routing_block_members;
            }
        }
        if let Some(previous) = active_atom {
            finish_external_atom_routing(
                atoms,
                previous,
                active_members,
                active_hot,
                written,
                &mut offsets,
                &mut statuses,
            )?;
            next_atom = previous + 1;
        }
        while next_atom < atoms.len() {
            finish_external_atom_routing(
                atoms,
                next_atom,
                0,
                false,
                written,
                &mut offsets,
                &mut statuses,
            )?;
            next_atom += 1;
        }
        if written != membership_count {
            return Err(AnalysisError::InvalidData(
                "external blocking inverse cardinality changed".into(),
            ));
        }
        offsets.finish().map_err(encode_err)?;
        block_ids.finish().map_err(encode_err)?;
        statuses.finish().map_err(encode_err)?;
        Ok(())
    }

    fn block_stats(
        &self,
        atom_count: usize,
        block_count: u64,
        membership_count: u64,
    ) -> Result<BlockStats, AnalysisError> {
        let (smax, bucket_pair_work): (u64, u64) = self.conn.query_row(
            &format!(
                "SELECT coalesce(max(member_count), 0)::UBIGINT,
                        coalesce(
                            sum(
                                member_count::UHUGEINT
                                * (member_count::UHUGEINT - 1)
                                / 2
                            ),
                            0
                        )::UBIGINT
                 FROM {ENCODE_BLOCK_TABLE}"
            ),
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let percentile = |pct: u64| -> Result<u32, AnalysisError> {
            if block_count == 0 {
                return Ok(0);
            }
            let rank = (((pct as f64 / 100.0) * (block_count - 1) as f64).round()) as u64;
            self.conn
                .query_row(
                    &format!(
                        "SELECT member_count::UINTEGER
                         FROM {ENCODE_BLOCK_TABLE}
                         ORDER BY member_count
                         LIMIT 1 OFFSET {rank}"
                    ),
                    [],
                    |row| row.get(0),
                )
                .map_err(AnalysisError::from)
        };
        Ok(BlockStats {
            block_count: u32::try_from(block_count).map_err(|_| {
                AnalysisError::InvalidData("blocking block count exceeds u32".into())
            })?,
            atom_count: u32::try_from(atom_count).map_err(|_| {
                AnalysisError::InvalidData("blocking atom count exceeds u32".into())
            })?,
            smax: u32::try_from(smax).map_err(|_| {
                AnalysisError::InvalidData("blocking block membership exceeds u32".into())
            })?,
            p50: percentile(50)?,
            p95: percentile(95)?,
            p99: percentile(99)?,
            replication: if atom_count == 0 {
                0.0
            } else {
                membership_count as f64 / atom_count as f64
            },
            bucket_pair_work,
        })
    }
}

impl Drop for DuckDbBlockingSpill<'_> {
    fn drop(&mut self) {
        let _ = self.conn.execute_batch(&format!(
            "DROP TABLE IF EXISTS {ENCODE_BLOCK_MEMBERSHIP_TABLE};
             DROP TABLE IF EXISTS {ENCODE_BLOCK_DISTINCT_TABLE};
             DROP TABLE IF EXISTS {ENCODE_BLOCK_TABLE};"
        ));
    }
}

#[derive(Clone, Copy)]
struct PendingBlockMembership {
    kind: u32,
    key: u64,
    atom: u32,
}

fn append_atom_block_memberships(
    atoms: AtomSketchView<'_>,
    atom: usize,
    rows: &mut Vec<PendingBlockMembership>,
) {
    if !atoms.has_content_terms(atom) {
        return;
    }
    let atom_id = atom as u32;
    if atoms.has_template_terms(atom) {
        for family in 0..JOINT_BAND_FAMILIES {
            let template_band = family / BANDS;
            let content_band = family % BANDS;
            let tv = simhash_band_value(atoms.template_simhashes[atom], template_band);
            let cv = simhash_band_value(atoms.content_simhashes[atom], content_band);
            let bucket = (u16::from(tv) << BAND_BITS) | u16::from(cv);
            rows.push(PendingBlockMembership {
                kind: 0,
                key: ((family as u64) << 16) | u64::from(bucket),
                atom: atom_id,
            });
        }
        rows.extend(
            atoms
                .template_anchors(atom)
                .iter()
                .take(ANCHOR_COUNT)
                .map(|&anchor| PendingBlockMembership {
                    kind: 1,
                    key: u64::from(anchor),
                    atom: atom_id,
                }),
        );
    }
    rows.extend(
        atoms
            .content_anchors(atom)
            .iter()
            .take(ANCHOR_COUNT)
            .map(|&anchor| PendingBlockMembership {
                kind: 2,
                key: u64::from(anchor),
                atom: atom_id,
            }),
    );
}

fn append_block_membership_record_batch(
    appender: &mut duckdb::Appender<'_>,
    kinds: &mut Vec<u32>,
    keys: &mut Vec<u64>,
    atom_ids: &mut Vec<u32>,
) -> Result<(), AnalysisError> {
    if kinds.is_empty() {
        return Ok(());
    }
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("kind", DataType::UInt32, false),
            Field::new("routing_key", DataType::UInt64, false),
            Field::new("atom", DataType::UInt32, false),
        ])),
        vec![
            Arc::new(UInt32Array::from(take_vec_preserving_capacity(kinds))),
            Arc::new(UInt64Array::from(take_vec_preserving_capacity(keys))),
            Arc::new(UInt32Array::from(take_vec_preserving_capacity(atom_ids))),
        ],
    )
    .map_err(encode_err)?;
    appender.append_record_batch(batch)?;
    Ok(())
}

fn finish_external_atom_routing(
    atoms: AtomSketchView<'_>,
    atom: usize,
    membership_count: u64,
    hot: bool,
    written: u64,
    offsets: &mut TypedArraySink,
    statuses: &mut TypedArraySink,
) -> Result<(), AnalysisError> {
    let status = if !atoms.has_content_terms(atom) {
        if membership_count != 0 {
            return Err(AnalysisError::InvalidData(format!(
                "proven atom {atom} unexpectedly has routing memberships"
            )));
        }
        RoutingStatus::ProvenNoCandidate
    } else if membership_count == 0 {
        return Err(AnalysisError::InvalidData(format!(
            "routable atom {atom} has no routing membership"
        )));
    } else if hot {
        RoutingStatus::HotBlock
    } else {
        RoutingStatus::Routed
    };
    offsets.push_u64(written).map_err(encode_err)?;
    statuses.push_u8(status as u8).map_err(encode_err)?;
    Ok(())
}

fn blocking_membership_count(atoms: AtomSketchView<'_>) -> Result<u64, AnalysisError> {
    (0..atoms.len()).try_fold(0u64, |total, atom| {
        if !atoms.has_content_terms(atom) {
            return Ok(total);
        }
        let joint = if atoms.has_template_terms(atom) {
            JOINT_BAND_FAMILIES as u64
        } else {
            0
        };
        let template = if atoms.has_template_terms(atom) {
            atoms.template_anchors(atom).len().min(ANCHOR_COUNT) as u64
        } else {
            0
        };
        let content = atoms.content_anchors(atom).len().min(ANCHOR_COUNT) as u64;
        total
            .checked_add(joint)
            .and_then(|value| value.checked_add(template))
            .and_then(|value| value.checked_add(content))
            .ok_or_else(|| AnalysisError::InvalidData("blocking membership overflow".into()))
    })
}

#[derive(Debug, Clone, Copy)]
struct ExternalPayloadMeta {
    pack_id: u32,
    offset: u64,
    length: u32,
    payload_ref: PayloadRef,
}

struct ExternalPayloadCasWriter {
    conn: Connection,
    directory: PathBuf,
    max_pack_bytes: u64,
    current_pack_id: u32,
    current_len: u64,
    current_file: Option<fs::File>,
    payload_count: u32,
    shard_counts: Vec<u32>,
    shard_bits: u32,
    hot_cache: HashMap<PayloadDigest, ExternalPayloadMeta>,
    hot_cache_limit: usize,
    transaction_open: bool,
}

impl ExternalPayloadCasWriter {
    fn create(
        directory: &Path,
        max_pack_bytes: u64,
        shard_count: usize,
        hot_cache_limit: usize,
        duckdb_memory_bytes: u64,
        threads: usize,
    ) -> Result<Self, AnalysisError> {
        fs::create_dir_all(directory)?;
        let conn = Connection::open(directory.join("payload-index.duckdb"))?;
        conn.execute_batch(&format!(
            "PRAGMA threads={};
             PRAGMA memory_limit='{}';",
            threads.max(1),
            format_byte_size(
                usize::try_from(duckdb_memory_bytes.max(16 * 1024 * 1024)).unwrap_or(usize::MAX)
            )
        ))?;
        conn.execute_batch(&format!(
            "DROP TABLE IF EXISTS {ENCODE_EXTERNAL_PAYLOAD_INDEX_TABLE};
             DROP TABLE IF EXISTS {ENCODE_EXTERNAL_PAYLOAD_SHARD_TABLE};
             CREATE TABLE {ENCODE_EXTERNAL_PAYLOAD_INDEX_TABLE}(
                 digest BLOB NOT NULL,
                 cas_id UINTEGER NOT NULL,
                 shard_id UINTEGER NOT NULL,
                 local_id UINTEGER NOT NULL,
                 pack_id UINTEGER NOT NULL,
                 byte_offset UBIGINT NOT NULL,
                 byte_length UINTEGER NOT NULL
             );
             CREATE INDEX encode_external_payload_digest_idx
             ON {ENCODE_EXTERNAL_PAYLOAD_INDEX_TABLE}(digest);
             BEGIN TRANSACTION;"
        ))?;
        let hot_cache_limit = hot_cache_limit.clamp(1, 1_048_576);
        Ok(Self {
            conn,
            directory: directory.to_path_buf(),
            max_pack_bytes: max_pack_bytes.max(1),
            current_pack_id: 0,
            current_len: 0,
            current_file: None,
            payload_count: 0,
            shard_counts: vec![0; shard_count],
            shard_bits: shard_count.trailing_zeros(),
            hot_cache: HashMap::with_capacity(hot_cache_limit),
            hot_cache_limit,
            transaction_open: true,
        })
    }

    fn insert(&mut self, bytes: &[u8]) -> Result<PayloadRef, AnalysisError> {
        let digest = payload_digest(bytes);
        self.insert_with_digest(bytes, digest)
    }

    fn insert_with_digest(
        &mut self,
        bytes: &[u8],
        digest: PayloadDigest,
    ) -> Result<PayloadRef, AnalysisError> {
        if let Some(&meta) = self.hot_cache.get(&digest) {
            if meta.length as usize == bytes.len() && self.bytes_equal(meta, bytes)? {
                return Ok(meta.payload_ref);
            }
        }
        let matched = {
            let mut statement = self.conn.prepare_cached(&format!(
                "SELECT pack_id::UINTEGER,
                        byte_offset::UBIGINT,
                        byte_length::UINTEGER,
                        shard_id::UINTEGER,
                        local_id::UINTEGER
                 FROM {ENCODE_EXTERNAL_PAYLOAD_INDEX_TABLE}
                 WHERE digest = ?
                 ORDER BY cas_id"
            ))?;
            let mut rows = statement.query(duckdb::params![digest.as_slice()])?;
            let mut matched = None;
            while let Some(row) = rows.next()? {
                let meta = ExternalPayloadMeta {
                    pack_id: row.get(0)?,
                    offset: row.get(1)?,
                    length: row.get(2)?,
                    payload_ref: PayloadRef {
                        shard_id: row.get::<_, u32>(3)?.try_into().map_err(|_| {
                            AnalysisError::InvalidData("external payload shard exceeds u16".into())
                        })?,
                        local_id: row.get(4)?,
                    },
                };
                if meta.length as usize == bytes.len() && self.bytes_equal(meta, bytes)? {
                    matched = Some(meta);
                    break;
                }
            }
            matched
        };
        if let Some(meta) = matched {
            self.cache(digest, meta);
            return Ok(meta.payload_ref);
        }
        self.append_new(bytes, digest)
    }

    fn append_new(
        &mut self,
        bytes: &[u8],
        digest: PayloadDigest,
    ) -> Result<PayloadRef, AnalysisError> {
        let length = u32::try_from(bytes.len())
            .map_err(|_| AnalysisError::InvalidData("payload bytes exceed u32 length".into()))?;
        if bytes.len() as u64 > self.max_pack_bytes {
            return Err(AnalysisError::InvalidData(format!(
                "payload larger than max pack size ({} > {})",
                bytes.len(),
                self.max_pack_bytes
            )));
        }
        if self.current_file.is_none()
            || self.current_len.saturating_add(bytes.len() as u64) > self.max_pack_bytes
        {
            self.rotate_pack()?;
        }
        let offset = self.current_len;
        self.current_file
            .as_mut()
            .expect("external payload pack is open")
            .write_all(bytes)?;
        self.current_len = self.current_len.saturating_add(bytes.len() as u64);
        let shard_id = payload_shard_for_digest(&digest, self.shard_bits);
        let local_id = *self.shard_counts.get(shard_id as usize).ok_or_else(|| {
            AnalysisError::InvalidData("external payload shard is out of range".into())
        })?;
        let next_local = local_id.checked_add(1).ok_or_else(|| {
            AnalysisError::InvalidData("external payload shard exceeds u32".into())
        })?;
        let cas_id = self.payload_count;
        let next_payload_count = cas_id.checked_add(1).ok_or_else(|| {
            AnalysisError::InvalidData("external payload count exceeds u32".into())
        })?;
        self.conn.execute(
            &format!(
                "INSERT INTO {ENCODE_EXTERNAL_PAYLOAD_INDEX_TABLE}
                 VALUES (?, ?, ?, ?, ?, ?, ?)"
            ),
            duckdb::params![
                digest.as_slice(),
                cas_id,
                u32::from(shard_id),
                local_id,
                self.current_pack_id,
                offset,
                length
            ],
        )?;
        self.shard_counts[shard_id as usize] = next_local;
        self.payload_count = next_payload_count;
        let meta = ExternalPayloadMeta {
            pack_id: self.current_pack_id,
            offset,
            length,
            payload_ref: PayloadRef { shard_id, local_id },
        };
        self.cache(digest, meta);
        Ok(meta.payload_ref)
    }

    fn cache(&mut self, digest: PayloadDigest, meta: ExternalPayloadMeta) {
        if self.hot_cache.len() >= self.hot_cache_limit {
            self.hot_cache.clear();
        }
        self.hot_cache.insert(digest, meta);
    }

    fn bytes_equal(&self, meta: ExternalPayloadMeta, bytes: &[u8]) -> Result<bool, AnalysisError> {
        let mut file = fs::File::open(external_payload_pack_path(&self.directory, meta.pack_id))?;
        use std::io::Seek;
        file.seek(std::io::SeekFrom::Start(meta.offset))?;
        const COMPARE_BUFFER_BYTES: usize = 64 * 1024;
        let mut buffer = [0u8; COMPARE_BUFFER_BYTES];
        let mut compared = 0usize;
        while compared < bytes.len() {
            let chunk = (bytes.len() - compared).min(buffer.len());
            file.read_exact(&mut buffer[..chunk])?;
            if buffer[..chunk] != bytes[compared..compared + chunk] {
                return Ok(false);
            }
            compared += chunk;
        }
        Ok(true)
    }

    fn rotate_pack(&mut self) -> Result<(), AnalysisError> {
        if let Some(mut file) = self.current_file.take() {
            file.flush()?;
            file.sync_all()?;
            self.current_pack_id = self.current_pack_id.checked_add(1).ok_or_else(|| {
                AnalysisError::InvalidData("external payload pack count exceeds u32".into())
            })?;
        }
        self.current_file = Some(
            fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(external_payload_pack_path(
                    &self.directory,
                    self.current_pack_id,
                ))?,
        );
        self.current_len = 0;
        Ok(())
    }

    fn resident_bytes(&self) -> Result<u64, AnalysisError> {
        [
            capacity_bytes::<u32>(self.shard_counts.capacity())?,
            hash_map_capacity_bytes::<PayloadDigest, ExternalPayloadMeta>(
                self.hot_cache.capacity(),
            )?,
            self.directory.as_os_str().len() as u64,
        ]
        .into_iter()
        .try_fold(std::mem::size_of::<Self>() as u64, |total, bytes| {
            total.checked_add(bytes).ok_or_else(|| {
                AnalysisError::InvalidData("external payload cache accounting overflow".into())
            })
        })
    }

    fn finish(mut self) -> Result<(ExternalPayloadCasIndex, Vec<u32>, usize), AnalysisError> {
        if let Some(mut file) = self.current_file.take() {
            file.flush()?;
            file.sync_all()?;
        } else {
            fs::File::create(external_payload_pack_path(&self.directory, 0))?;
        }
        self.conn.execute_batch("COMMIT;")?;
        self.transaction_open = false;
        let mut global_offsets = Vec::with_capacity(self.shard_counts.len().saturating_add(1));
        global_offsets.push(0u32);
        for &count in &self.shard_counts {
            global_offsets.push(
                global_offsets
                    .last()
                    .copied()
                    .unwrap_or(0)
                    .checked_add(count)
                    .ok_or_else(|| {
                        AnalysisError::InvalidData(
                            "external payload global identity exceeds u32".into(),
                        )
                    })?,
            );
        }
        self.conn.execute_batch(&format!(
            "DROP TABLE IF EXISTS {ENCODE_EXTERNAL_PAYLOAD_SHARD_TABLE};
             CREATE TABLE {ENCODE_EXTERNAL_PAYLOAD_SHARD_TABLE}(
                 shard_id UINTEGER PRIMARY KEY,
                 global_base UINTEGER NOT NULL
             );"
        ))?;
        {
            let mut appender = self.conn.appender(ENCODE_EXTERNAL_PAYLOAD_SHARD_TABLE)?;
            let mut shard_ids = Vec::with_capacity(self.shard_counts.len());
            let mut bases = Vec::with_capacity(self.shard_counts.len());
            for (shard, &base) in global_offsets
                .iter()
                .take(self.shard_counts.len())
                .enumerate()
            {
                shard_ids.push(shard as u32);
                bases.push(base);
            }
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("shard_id", DataType::UInt32, false),
                    Field::new("global_base", DataType::UInt32, false),
                ])),
                vec![
                    Arc::new(UInt32Array::from(shard_ids)),
                    Arc::new(UInt32Array::from(bases)),
                ],
            )
            .map_err(encode_err)?;
            appender.append_record_batch(batch)?;
            appender.flush()?;
        }
        let payload_count = self.payload_count as u64;
        let mut pack_ids = TypedArraySink::create(
            &self.directory.join("payload_global_pack_ids.u32"),
            ArrayKind::U32,
            payload_count,
        )
        .map_err(encode_err)?;
        let mut byte_offsets = TypedArraySink::create(
            &self.directory.join("payload_global_offsets.u64"),
            ArrayKind::U64,
            payload_count,
        )
        .map_err(encode_err)?;
        let mut lengths = TypedArraySink::create(
            &self.directory.join("payload_global_lengths.u32"),
            ArrayKind::U32,
            payload_count,
        )
        .map_err(encode_err)?;
        let mut statement = self.conn.prepare(&format!(
            "SELECT payload.pack_id::UINTEGER,
                    payload.byte_offset::UBIGINT,
                    payload.byte_length::UINTEGER
             FROM {ENCODE_EXTERNAL_PAYLOAD_INDEX_TABLE} payload
             JOIN {ENCODE_EXTERNAL_PAYLOAD_SHARD_TABLE} shards
               ON shards.shard_id = payload.shard_id
             ORDER BY shards.global_base + payload.local_id"
        ))?;
        let batches = statement.stream_arrow(
            [],
            Arc::new(Schema::new(vec![
                Field::new("pack_id", DataType::UInt32, false),
                Field::new("byte_offset", DataType::UInt64, false),
                Field::new("byte_length", DataType::UInt32, false),
            ])),
        )?;
        let mut written = 0u64;
        for batch in batches {
            let packs = required_arrow_column::<UInt32Array>(&batch, 0, "pack_id")?;
            let offsets = required_arrow_column::<UInt64Array>(&batch, 1, "byte_offset")?;
            let sizes = required_arrow_column::<UInt32Array>(&batch, 2, "byte_length")?;
            for row in 0..batch.num_rows() {
                pack_ids.push_u32(packs.value(row)).map_err(encode_err)?;
                byte_offsets
                    .push_u64(offsets.value(row))
                    .map_err(encode_err)?;
                lengths.push_u32(sizes.value(row)).map_err(encode_err)?;
                written += 1;
            }
        }
        if written != payload_count {
            return Err(AnalysisError::InvalidData(
                "external payload index cardinality changed".into(),
            ));
        }
        pack_ids.finish().map_err(encode_err)?;
        byte_offsets.finish().map_err(encode_err)?;
        lengths.finish().map_err(encode_err)?;
        let index = ExternalPayloadCasIndex {
            directory: self.directory.clone(),
            pack_ids: map_u32_array(&self.directory.join("payload_global_pack_ids.u32"))
                .map_err(encode_err)?,
            byte_offsets: map_u64_array(&self.directory.join("payload_global_offsets.u64"))
                .map_err(encode_err)?,
            lengths: map_u32_array(&self.directory.join("payload_global_lengths.u32"))
                .map_err(encode_err)?,
        };
        self.conn.execute_batch(&format!(
            "DROP TABLE IF EXISTS {ENCODE_EXTERNAL_PAYLOAD_INDEX_TABLE};
             DROP TABLE IF EXISTS {ENCODE_EXTERNAL_PAYLOAD_SHARD_TABLE};"
        ))?;
        Ok((index, global_offsets, payload_count as usize))
    }
}

impl Drop for ExternalPayloadCasWriter {
    fn drop(&mut self) {
        if self.transaction_open {
            let _ = self.conn.execute_batch("ROLLBACK;");
        }
        let _ = self.conn.execute_batch(&format!(
            "DROP TABLE IF EXISTS {ENCODE_EXTERNAL_PAYLOAD_INDEX_TABLE};
             DROP TABLE IF EXISTS {ENCODE_EXTERNAL_PAYLOAD_SHARD_TABLE};"
        ));
    }
}

struct ExternalPayloadCasIndex {
    directory: PathBuf,
    pack_ids: MappedU32Array,
    byte_offsets: MappedU64Array,
    lengths: MappedU32Array,
}

impl ExternalPayloadCasIndex {
    fn payload_len(&self, payload_id: u32) -> Result<usize, AnalysisError> {
        self.lengths
            .get(payload_id as usize)
            .copied()
            .map(|length| length as usize)
            .ok_or_else(|| AnalysisError::InvalidData("unknown external payload".into()))
    }

    fn read_payload_ids(&self, payload_ids: &[u32]) -> Result<Vec<Vec<u8>>, AnalysisError> {
        let mut groups = BTreeMap::<u32, Vec<(usize, u32)>>::new();
        for (output, &payload_id) in payload_ids.iter().enumerate() {
            let pack_id = *self
                .pack_ids
                .get(payload_id as usize)
                .ok_or_else(|| AnalysisError::InvalidData("unknown external payload".into()))?;
            groups
                .entry(pack_id)
                .or_default()
                .push((output, payload_id));
        }
        let grouped = groups
            .into_par_iter()
            .map(|(pack_id, mut requests)| {
                requests.sort_unstable_by_key(|&(_, payload_id)| {
                    self.byte_offsets[payload_id as usize]
                });
                self.read_pack_requests(pack_id, &requests)
            })
            .collect::<Result<Vec<_>, AnalysisError>>()?;
        let mut output = (0..payload_ids.len()).map(|_| None).collect::<Vec<_>>();
        for (index, bytes) in grouped.into_iter().flatten() {
            output[index] = Some(bytes);
        }
        output
            .into_iter()
            .map(|bytes| {
                bytes.ok_or_else(|| {
                    AnalysisError::InvalidData("external payload read was incomplete".into())
                })
            })
            .collect()
    }

    fn read_pack_requests(
        &self,
        pack_id: u32,
        requests: &[(usize, u32)],
    ) -> Result<Vec<(usize, Vec<u8>)>, AnalysisError> {
        let mut file = fs::File::open(external_payload_pack_path(&self.directory, pack_id))?;
        use std::io::Seek;
        let mut output = Vec::with_capacity(requests.len());
        for &(output_index, payload_id) in requests {
            let payload = payload_id as usize;
            file.seek(std::io::SeekFrom::Start(self.byte_offsets[payload]))?;
            let mut bytes = vec![0u8; self.lengths[payload] as usize];
            file.read_exact(&mut bytes)?;
            output.push((output_index, bytes));
        }
        Ok(output)
    }
}

fn external_payload_pack_path(directory: &Path, pack_id: u32) -> PathBuf {
    directory.join(format!("pack-{pack_id:06}.bin"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PayloadStorageMode {
    Memory,
    Spill,
    SpillExternalIndex,
}

fn payload_storage_mode(estimate: &EncodeAdmissionEstimate, hard_top: u64) -> PayloadStorageMode {
    let index_upper = payload_resident_index_upper_bound(estimate).unwrap_or(u64::MAX);
    if estimate.resident_peak_bytes.max(index_upper) <= hard_top {
        return PayloadStorageMode::Memory;
    }
    let index_budget = hard_top.saturating_sub(ENCODE_RESIDENT_FIXED_BYTES + 256 * 1024 * 1024);
    if index_upper <= index_budget {
        PayloadStorageMode::Spill
    } else {
        PayloadStorageMode::SpillExternalIndex
    }
}

fn payload_resident_index_upper_bound(
    estimate: &EncodeAdmissionEstimate,
) -> Result<u64, AnalysisError> {
    let unique_upper = estimate
        .token_rows
        .checked_add(estimate.representative_rows.saturating_mul(2))
        .map(|count| count.min(u64::from(u32::MAX)))
        .ok_or_else(|| AnalysisError::InvalidData("payload index estimate overflow".into()))?;
    // Registration peak per unique payload:
    // - PayloadMeta Vec capacity < 2N: at most 2 * 56 = 112 bytes
    // - digest HashMap reported capacity < 2N: 2 * (32 key + 24 entry
    //   + 16 bucket allowance) = 144 bytes
    // - worst collision IDs: 4 bytes
    // - CAS->shard PayloadRef Vec capacity < 2N: 16 bytes
    // 276N is the structural bound; 288N leaves allocator/alignment slack.
    unique_upper
        .checked_mul(288)
        .and_then(|bytes| bytes.checked_add(64 * 1024 * 1024))
        .ok_or_else(|| AnalysisError::InvalidData("payload index estimate overflow".into()))
}

fn payload_external_index_storage_upper_bound(
    estimate: &EncodeAdmissionEstimate,
) -> Result<u64, AnalysisError> {
    let unique_upper = estimate
        .token_rows
        .checked_add(estimate.representative_rows.saturating_mul(2))
        .map(|count| count.min(u64::from(u32::MAX)))
        .ok_or_else(|| AnalysisError::InvalidData("payload index estimate overflow".into()))?;
    unique_upper
        .checked_mul(192)
        .and_then(|bytes| bytes.checked_add(256 * 1024 * 1024))
        .ok_or_else(|| {
            AnalysisError::InvalidData("external payload index storage estimate overflow".into())
        })
}

impl PayloadStorageMode {
    fn is_spill(self) -> bool {
        !matches!(self, Self::Memory)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PayloadHandle {
    Memory(PayloadRef),
    Spill(PayloadRef),
}

enum PayloadRegistrationStore {
    Memory(ShardedPayloadArena),
    Spill {
        writer: PayloadCasWriter,
        cas_payload_refs: Vec<PayloadRef>,
        shard_counts: Vec<u32>,
        shard_bits: u32,
        _cleanup: metadata_engine::artifacts::StagingCleanupGuard,
    },
    SpillExternalIndex {
        writer: ExternalPayloadCasWriter,
        _cleanup: metadata_engine::artifacts::StagingCleanupGuard,
    },
}

impl PayloadRegistrationStore {
    fn create(
        mode: PayloadStorageMode,
        work_directory: &Path,
        shard_count: usize,
        external_cache_limit: usize,
        external_duckdb_memory_bytes: u64,
        threads: usize,
    ) -> Result<Self, AnalysisError> {
        match mode {
            PayloadStorageMode::Memory => Ok(Self::Memory(ShardedPayloadArena::with_shard_count(
                shard_count,
                DEFAULT_ARENA_CHUNK_BYTES,
            ))),
            PayloadStorageMode::Spill => {
                let shard_count = shard_count.next_power_of_two().max(1);
                if shard_count > usize::from(u16::MAX).saturating_add(1) {
                    return Err(AnalysisError::InvalidData(
                        "payload spill shard count exceeds u16 identity space".into(),
                    ));
                }
                let spill_directory = work_directory.join("artifacts/metadata").join(format!(
                    "{ENCODE_PAYLOAD_SPILL_PREFIX}{}",
                    metadata_engine::artifacts::new_artifact_run_id()
                ));
                let cleanup =
                    metadata_engine::artifacts::StagingCleanupGuard::new([spill_directory.clone()]);
                let writer = PayloadCasWriter::create(&spill_directory, DEFAULT_MAX_PACK_BYTES)
                    .map_err(encode_err)?;
                Ok(Self::Spill {
                    writer,
                    cas_payload_refs: Vec::new(),
                    shard_counts: vec![0; shard_count],
                    shard_bits: shard_count.trailing_zeros(),
                    _cleanup: cleanup,
                })
            }
            PayloadStorageMode::SpillExternalIndex => {
                let shard_count = shard_count.next_power_of_two().max(1);
                if shard_count > usize::from(u16::MAX).saturating_add(1) {
                    return Err(AnalysisError::InvalidData(
                        "payload spill shard count exceeds u16 identity space".into(),
                    ));
                }
                let spill_directory = work_directory.join("artifacts/metadata").join(format!(
                    "{ENCODE_PAYLOAD_SPILL_PREFIX}{}",
                    metadata_engine::artifacts::new_artifact_run_id()
                ));
                let cleanup =
                    metadata_engine::artifacts::StagingCleanupGuard::new([spill_directory.clone()]);
                let writer = ExternalPayloadCasWriter::create(
                    &spill_directory,
                    DEFAULT_MAX_PACK_BYTES,
                    shard_count,
                    external_cache_limit,
                    external_duckdb_memory_bytes,
                    threads,
                )?;
                Ok(Self::SpillExternalIndex {
                    writer,
                    _cleanup: cleanup,
                })
            }
        }
    }

    fn insert(&mut self, bytes: &[u8]) -> Result<PayloadHandle, AnalysisError> {
        match self {
            Self::Memory(arena) => arena
                .insert(bytes)
                .map(PayloadHandle::Memory)
                .map_err(encode_err),
            Self::Spill {
                writer,
                cas_payload_refs,
                shard_counts,
                shard_bits,
                ..
            } => {
                let digest = payload_digest(bytes);
                let cas_id = writer
                    .insert_with_digest(bytes, digest)
                    .map_err(encode_err)?;
                let payload_ref = if cas_id as usize == cas_payload_refs.len() {
                    let shard_id = payload_shard_for_digest(&digest, *shard_bits);
                    let local_id = *shard_counts.get(shard_id as usize).ok_or_else(|| {
                        AnalysisError::InvalidData(
                            "payload spill shard is outside the configured range".into(),
                        )
                    })?;
                    shard_counts[shard_id as usize] = local_id.checked_add(1).ok_or_else(|| {
                        AnalysisError::InvalidData(
                            "payload spill shard exceeds u32 identity space".into(),
                        )
                    })?;
                    let payload_ref = PayloadRef { shard_id, local_id };
                    cas_payload_refs.push(payload_ref);
                    payload_ref
                } else {
                    *cas_payload_refs.get(cas_id as usize).ok_or_else(|| {
                        AnalysisError::InvalidData(
                            "payload CAS returned a non-dense identity".into(),
                        )
                    })?
                };
                Ok(PayloadHandle::Spill(payload_ref))
            }
            Self::SpillExternalIndex { writer, .. } => {
                writer.insert(bytes).map(PayloadHandle::Spill)
            }
        }
    }

    fn resident_bytes(&self) -> Result<u64, AnalysisError> {
        match self {
            Self::Memory(arena) => Ok(arena.resident_bytes()),
            Self::Spill {
                writer,
                cas_payload_refs,
                shard_counts,
                ..
            } => writer
                .resident_bytes()
                .checked_add(capacity_bytes::<PayloadRef>(cas_payload_refs.capacity())?)
                .and_then(|bytes| {
                    bytes.checked_add(capacity_bytes::<u32>(shard_counts.capacity()).ok()?)
                })
                .ok_or_else(|| {
                    AnalysisError::InvalidData("payload spill index accounting overflow".into())
                }),
            Self::SpillExternalIndex { writer, .. } => writer.resident_bytes(),
        }
    }

    fn finish(self) -> Result<PayloadReadStore, AnalysisError> {
        match self {
            Self::Memory(arena) => {
                let global_offsets = arena.global_offsets().map_err(encode_err)?;
                let payload_count = global_offsets.last().copied().unwrap_or(0) as usize;
                let arena = arena.freeze().map_err(encode_err)?;
                Ok(PayloadReadStore::Memory {
                    arena,
                    global_offsets,
                    payload_count,
                })
            }
            Self::Spill {
                writer,
                cas_payload_refs,
                shard_counts,
                _cleanup: cleanup,
                ..
            } => {
                let index = writer.finish().map_err(encode_err)?;
                if index.payload_count() != cas_payload_refs.len() {
                    return Err(AnalysisError::InvalidData(
                        "payload spill CAS identity count changed during finalize".into(),
                    ));
                }
                let mut global_offsets = Vec::with_capacity(shard_counts.len().saturating_add(1));
                global_offsets.push(0u32);
                for count in shard_counts {
                    let next = global_offsets
                        .last()
                        .copied()
                        .unwrap_or(0)
                        .checked_add(count)
                        .ok_or_else(|| {
                            AnalysisError::InvalidData(
                                "payload spill global identity overflow".into(),
                            )
                        })?;
                    global_offsets.push(next);
                }
                let payload_count = index.payload_count();
                let mut global_to_cas = vec![0u32; payload_count];
                let mut assigned = vec![false; payload_count];
                for (cas_id, payload_ref) in cas_payload_refs.into_iter().enumerate() {
                    let global_id = global_payload_ref_id(payload_ref, &global_offsets)? as usize;
                    let was_assigned = assigned.get_mut(global_id).ok_or_else(|| {
                        AnalysisError::InvalidData(
                            "payload spill global identity is out of range".into(),
                        )
                    })?;
                    if *was_assigned {
                        return Err(AnalysisError::InvalidData(
                            "payload spill global identity is duplicated".into(),
                        ));
                    }
                    *was_assigned = true;
                    global_to_cas[global_id] = u32::try_from(cas_id).map_err(|_| {
                        AnalysisError::InvalidData(
                            "payload spill CAS exceeds u32 identity space".into(),
                        )
                    })?;
                }
                if assigned.iter().any(|assigned| !assigned) {
                    return Err(AnalysisError::InvalidData(
                        "payload spill global identity map is incomplete".into(),
                    ));
                }
                Ok(PayloadReadStore::Spill {
                    index,
                    global_offsets,
                    global_to_cas,
                    _cleanup: cleanup,
                })
            }
            Self::SpillExternalIndex {
                writer,
                _cleanup: cleanup,
            } => {
                let (index, global_offsets, payload_count) = writer.finish()?;
                Ok(PayloadReadStore::ExternalSpill {
                    index,
                    global_offsets,
                    payload_count,
                    _cleanup: cleanup,
                })
            }
        }
    }
}

enum PayloadReadStore {
    Memory {
        arena: metadata_engine::encode::FrozenShardedPayloadArena,
        global_offsets: Vec<u32>,
        payload_count: usize,
    },
    Spill {
        index: PayloadCasIndex,
        global_offsets: Vec<u32>,
        global_to_cas: Vec<u32>,
        _cleanup: metadata_engine::artifacts::StagingCleanupGuard,
    },
    ExternalSpill {
        index: ExternalPayloadCasIndex,
        global_offsets: Vec<u32>,
        payload_count: usize,
        _cleanup: metadata_engine::artifacts::StagingCleanupGuard,
    },
}

impl PayloadReadStore {
    fn payload_count(&self) -> usize {
        match self {
            Self::Memory { payload_count, .. } => *payload_count,
            Self::Spill { global_to_cas, .. } => global_to_cas.len(),
            Self::ExternalSpill { payload_count, .. } => *payload_count,
        }
    }

    fn global_offsets(&self) -> &[u32] {
        match self {
            Self::Memory { global_offsets, .. }
            | Self::Spill { global_offsets, .. }
            | Self::ExternalSpill { global_offsets, .. } => global_offsets,
        }
    }

    fn payload_len(&self, global_id: u32) -> Result<usize, AnalysisError> {
        match self {
            Self::Memory {
                arena,
                global_offsets,
                ..
            } => arena
                .with_global_bytes(global_id, global_offsets, |bytes| bytes.len())
                .map_err(encode_err),
            Self::Spill {
                index,
                global_to_cas,
                ..
            } => {
                let cas_id = *global_to_cas.get(global_id as usize).ok_or_else(|| {
                    AnalysisError::InvalidData("payload spill identity is out of range".into())
                })?;
                index.payload_len(cas_id).map_err(encode_err)
            }
            Self::ExternalSpill { index, .. } => index.payload_len(global_id),
        }
    }

    fn parse_batch(
        &self,
        range: std::ops::Range<usize>,
        parse_pool: &rayon::ThreadPool,
    ) -> Result<Vec<ParsedMetadataDocuments>, AnalysisError> {
        match self {
            Self::Memory {
                arena,
                global_offsets,
                ..
            } => parse_pool.install(|| {
                range
                    .into_par_iter()
                    .map(|payload_id| {
                        arena
                            .with_global_bytes(payload_id as u32, global_offsets, |bytes| {
                                let text = std::str::from_utf8(bytes).map_err(|error| {
                                    AnalysisError::InvalidData(format!(
                                        "encode payload bytes were not valid utf-8: {error}"
                                    ))
                                })?;
                                Ok(parse_metadata_documents(text))
                            })
                            .map_err(encode_err)?
                    })
                    .collect()
            }),
            Self::Spill {
                index,
                global_to_cas,
                ..
            } => {
                let cas_ids = global_to_cas.get(range).ok_or_else(|| {
                    AnalysisError::InvalidData("payload spill parse range is out of bounds".into())
                })?;
                let bodies = parse_pool
                    .install(|| index.read_payload_ids(cas_ids))
                    .map_err(encode_err)?;
                parse_pool.install(|| {
                    bodies
                        .into_par_iter()
                        .map(|bytes| {
                            let text = std::str::from_utf8(&bytes).map_err(|error| {
                                AnalysisError::InvalidData(format!(
                                    "encode spilled payload bytes were not valid utf-8: {error}"
                                ))
                            })?;
                            Ok(parse_metadata_documents(text))
                        })
                        .collect()
                })
            }
            Self::ExternalSpill { index, .. } => {
                let ids = range
                    .map(|payload_id| {
                        u32::try_from(payload_id).map_err(|_| {
                            AnalysisError::InvalidData(
                                "external payload identity exceeds u32".into(),
                            )
                        })
                    })
                    .collect::<Result<Vec<_>, AnalysisError>>()?;
                let bodies = parse_pool.install(|| index.read_payload_ids(&ids))?;
                parse_pool.install(|| {
                    bodies
                        .into_par_iter()
                        .map(|bytes| {
                            let text = std::str::from_utf8(&bytes).map_err(|error| {
                                AnalysisError::InvalidData(format!(
                                    "encode externally indexed payload bytes were not valid utf-8: {error}"
                                ))
                            })?;
                            Ok(parse_metadata_documents(text))
                        })
                        .collect()
                })
            }
        }
    }
}

fn payload_shard_for_digest(digest: &[u8; 32], shard_bits: u32) -> u16 {
    if shard_bits == 0 {
        return 0;
    }
    let high = u16::from_be_bytes([digest[0], digest[1]]);
    high >> 16u32.saturating_sub(shard_bits.min(16))
}

fn global_payload_ref_id(
    payload_ref: PayloadRef,
    global_offsets: &[u32],
) -> Result<u32, AnalysisError> {
    global_offsets
        .get(payload_ref.shard_id as usize)
        .copied()
        .and_then(|base| base.checked_add(payload_ref.local_id))
        .ok_or_else(|| {
            AnalysisError::InvalidData("pending payload reference is out of range".into())
        })
}

#[derive(Debug)]
struct TokenSourceInput {
    token_ids: Vec<u32>,
}

#[derive(Debug, Clone, Copy)]
struct TokenSourceRecord {
    source_file: u32,
    source_row_number: u64,
    payload_ref: PayloadHandle,
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
    payload_ref: PayloadHandle,
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

enum TokenSourceCatalog<'connection> {
    Memory(TokenSourceRelation),
    External(ExternalRegistrationSpill<'connection>),
}

impl TokenSourceCatalog<'_> {
    fn bytes(&self) -> u64 {
        match self {
            Self::Memory(relation) => relation.bytes(),
            Self::External(_) => 0,
        }
    }
}

impl TokenSourceRelation {
    fn append_contract_layout(
        &self,
        contract_index: u32,
        representative: SourceCoordinate,
        pending_token_ids: &mut Vec<u32>,
    ) -> Result<(TokenRange, Vec<PendingSourceSlot>), AnalysisError> {
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

        let mut representative_source = None;
        let mut cursor = start;
        while cursor < end {
            let source_id = self.memberships[cursor].source_id;
            let source = self.sources.get(source_id as usize).ok_or_else(|| {
                AnalysisError::InvalidData(
                    "token-source membership references unknown source".into(),
                )
            })?;
            while cursor < end && self.memberships[cursor].source_id == source_id {
                cursor += 1;
            }
            if (SourceCoordinate {
                source_file: source.source_file,
                source_row_number: source.source_row_number,
            }) == representative
                && representative_source.replace(source_id).is_some()
            {
                return Err(AnalysisError::InvalidData(
                    "token-source dictionary contains duplicate representative coordinates".into(),
                ));
            }
        }

        let representative_start = pending_token_ids.len();
        if let Some(source_id) = representative_source {
            self.append_source_tokens(start, end, source_id, pending_token_ids)?;
        }
        let representative_range = TokenRange {
            start: representative_start,
            end: pending_token_ids.len(),
        };

        let mut output = Vec::new();
        let mut cursor = start;
        while cursor < end {
            let group_start = cursor;
            let source_id = self.memberships[cursor].source_id;
            while cursor < end && self.memberships[cursor].source_id == source_id {
                cursor += 1;
            }
            if Some(source_id) == representative_source {
                continue;
            }
            let source = self.sources.get(source_id as usize).ok_or_else(|| {
                AnalysisError::InvalidData(
                    "token-source membership references unknown source".into(),
                )
            })?;
            let token_start = pending_token_ids.len();
            pending_token_ids.extend(
                self.memberships[group_start..cursor]
                    .iter()
                    .map(|membership| membership.token_id),
            );
            output.push(PendingSourceSlot {
                payload_ref: source.payload_ref,
                token_range: TokenRange {
                    start: token_start,
                    end: pending_token_ids.len(),
                },
            });
        }
        Ok((representative_range, output))
    }

    fn append_source_tokens(
        &self,
        start: usize,
        end: usize,
        source_id: u32,
        pending_token_ids: &mut Vec<u32>,
    ) -> Result<(), AnalysisError> {
        let group_start = self.memberships[start..end]
            .partition_point(|membership| membership.source_id < source_id)
            .checked_add(start)
            .ok_or_else(|| {
                AnalysisError::InvalidData("token-source membership offset overflow".into())
            })?;
        let group_end = self.memberships[group_start..end]
            .partition_point(|membership| membership.source_id == source_id)
            .checked_add(group_start)
            .ok_or_else(|| {
                AnalysisError::InvalidData("token-source membership offset overflow".into())
            })?;
        if group_start == group_end {
            return Err(AnalysisError::InvalidData(
                "representative token source has no memberships".into(),
            ));
        }
        pending_token_ids.extend(
            self.memberships[group_start..group_end]
                .iter()
                .map(|membership| membership.token_id),
        );
        Ok(())
    }

    fn bytes(&self) -> u64 {
        self.logical_bytes
    }
}

const EXTERNAL_SELECTED_SOURCE_TABLE: &str = "encode_selected_token_source_spill";
const EXTERNAL_RESOLVED_CONTRACT_TABLE: &str = "encode_resolved_contract_spill";
const EXTERNAL_FALLBACK_CONTRACT_TABLE: &str = "encode_fallback_contract_spill";
const EXTERNAL_FINAL_SOURCE_TABLE: &str = "encode_final_source_spill";
const EXTERNAL_SHARD_OFFSET_TABLE: &str = "encode_payload_shard_offset_spill";

#[derive(Clone, Copy)]
struct ExternalResolvedContract {
    contract_index: u32,
    chain_id: u32,
    weight: u64,
    source_file: u32,
    source_row_number: u64,
    payload_ref: PayloadHandle,
}

#[derive(Clone, Copy)]
struct ExternalFallbackContract {
    contract_index: u32,
    chain_id: u32,
    weight: u64,
}

struct ExternalRegistrationSpill<'connection> {
    conn: &'connection Connection,
    next_order: Cell<u64>,
}

impl<'connection> ExternalRegistrationSpill<'connection> {
    fn create(conn: &'connection Connection) -> Result<Self, AnalysisError> {
        conn.execute_batch(&format!(
            "DROP TABLE IF EXISTS {EXTERNAL_SELECTED_SOURCE_TABLE};
             DROP TABLE IF EXISTS {EXTERNAL_RESOLVED_CONTRACT_TABLE};
             DROP TABLE IF EXISTS {EXTERNAL_FALLBACK_CONTRACT_TABLE};
             DROP TABLE IF EXISTS {EXTERNAL_FINAL_SOURCE_TABLE};
             DROP TABLE IF EXISTS {EXTERNAL_SHARD_OFFSET_TABLE};
             CREATE TEMP TABLE {EXTERNAL_SELECTED_SOURCE_TABLE}(
                 contract_index UINTEGER NOT NULL,
                 token_index UINTEGER NOT NULL,
                 source_file UINTEGER NOT NULL,
                 source_row_number UBIGINT NOT NULL,
                 payload_shard UINTEGER NOT NULL,
                 payload_local UINTEGER NOT NULL
             );
             CREATE TEMP TABLE {EXTERNAL_RESOLVED_CONTRACT_TABLE}(
                 order_id UBIGINT NOT NULL,
                 contract_index UINTEGER NOT NULL,
                 chain_id UINTEGER NOT NULL,
                 weight UBIGINT NOT NULL,
                 source_file UINTEGER NOT NULL,
                 source_row_number UBIGINT NOT NULL,
                 payload_shard UINTEGER NOT NULL,
                 payload_local UINTEGER NOT NULL
             );
             CREATE TEMP TABLE {EXTERNAL_FALLBACK_CONTRACT_TABLE}(
                 contract_index UINTEGER PRIMARY KEY,
                 chain_id UINTEGER NOT NULL,
                 weight UBIGINT NOT NULL
             );"
        ))?;
        Ok(Self {
            conn,
            next_order: Cell::new(0),
        })
    }

    fn append_selected(&self, rows: &[SelectedTokenSource]) -> Result<(), AnalysisError> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut contract_ids = Vec::with_capacity(rows.len());
        let mut token_ids = Vec::with_capacity(rows.len());
        let mut source_files = Vec::with_capacity(rows.len());
        let mut source_rows = Vec::with_capacity(rows.len());
        let mut payload_shards = Vec::with_capacity(rows.len());
        let mut payload_locals = Vec::with_capacity(rows.len());
        for row in rows {
            let payload = payload_handle_ref(row.payload_ref);
            contract_ids.push(row.contract_index);
            token_ids.push(row.token_index);
            source_files.push(row.coordinate.source_file);
            source_rows.push(row.coordinate.source_row_number);
            payload_shards.push(u32::from(payload.shard_id));
            payload_locals.push(payload.local_id);
        }
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("contract_index", DataType::UInt32, false),
                Field::new("token_index", DataType::UInt32, false),
                Field::new("source_file", DataType::UInt32, false),
                Field::new("source_row_number", DataType::UInt64, false),
                Field::new("payload_shard", DataType::UInt32, false),
                Field::new("payload_local", DataType::UInt32, false),
            ])),
            vec![
                Arc::new(UInt32Array::from(contract_ids)),
                Arc::new(UInt32Array::from(token_ids)),
                Arc::new(UInt32Array::from(source_files)),
                Arc::new(UInt64Array::from(source_rows)),
                Arc::new(UInt32Array::from(payload_shards)),
                Arc::new(UInt32Array::from(payload_locals)),
            ],
        )
        .map_err(encode_err)?;
        let mut appender = self.conn.appender(EXTERNAL_SELECTED_SOURCE_TABLE)?;
        appender.append_record_batch(batch)?;
        appender.flush()?;
        Ok(())
    }

    fn append_registration(
        &self,
        resolved: &[ExternalResolvedContract],
        fallbacks: &[ExternalFallbackContract],
    ) -> Result<(), AnalysisError> {
        if !resolved.is_empty() {
            let first_order = self.next_order.get();
            let next_order = first_order
                .checked_add(resolved.len() as u64)
                .ok_or_else(|| {
                    AnalysisError::InvalidData("external contract order overflow".into())
                })?;
            let mut orders = Vec::with_capacity(resolved.len());
            let mut contract_ids = Vec::with_capacity(resolved.len());
            let mut chain_ids = Vec::with_capacity(resolved.len());
            let mut weights = Vec::with_capacity(resolved.len());
            let mut source_files = Vec::with_capacity(resolved.len());
            let mut source_rows = Vec::with_capacity(resolved.len());
            let mut payload_shards = Vec::with_capacity(resolved.len());
            let mut payload_locals = Vec::with_capacity(resolved.len());
            for (index, row) in resolved.iter().enumerate() {
                let payload = payload_handle_ref(row.payload_ref);
                orders.push(first_order + index as u64);
                contract_ids.push(row.contract_index);
                chain_ids.push(row.chain_id);
                weights.push(row.weight);
                source_files.push(row.source_file);
                source_rows.push(row.source_row_number);
                payload_shards.push(u32::from(payload.shard_id));
                payload_locals.push(payload.local_id);
            }
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("order_id", DataType::UInt64, false),
                    Field::new("contract_index", DataType::UInt32, false),
                    Field::new("chain_id", DataType::UInt32, false),
                    Field::new("weight", DataType::UInt64, false),
                    Field::new("source_file", DataType::UInt32, false),
                    Field::new("source_row_number", DataType::UInt64, false),
                    Field::new("payload_shard", DataType::UInt32, false),
                    Field::new("payload_local", DataType::UInt32, false),
                ])),
                vec![
                    Arc::new(UInt64Array::from(orders)),
                    Arc::new(UInt32Array::from(contract_ids)),
                    Arc::new(UInt32Array::from(chain_ids)),
                    Arc::new(UInt64Array::from(weights)),
                    Arc::new(UInt32Array::from(source_files)),
                    Arc::new(UInt64Array::from(source_rows)),
                    Arc::new(UInt32Array::from(payload_shards)),
                    Arc::new(UInt32Array::from(payload_locals)),
                ],
            )
            .map_err(encode_err)?;
            let mut appender = self.conn.appender(EXTERNAL_RESOLVED_CONTRACT_TABLE)?;
            appender.append_record_batch(batch)?;
            appender.flush()?;
            self.next_order.set(next_order);
        }
        if !fallbacks.is_empty() {
            let mut appender = self.conn.appender(EXTERNAL_FALLBACK_CONTRACT_TABLE)?;
            appender.append_rows(
                fallbacks
                    .iter()
                    .map(|row| (row.contract_index, row.chain_id, row.weight)),
            )?;
            appender.flush()?;
        }
        Ok(())
    }

    fn resolved_count(&self) -> Result<usize, AnalysisError> {
        let count: u64 = self.conn.query_row(
            &format!("SELECT count(*)::UBIGINT FROM {EXTERNAL_RESOLVED_CONTRACT_TABLE}"),
            [],
            |row| row.get(0),
        )?;
        usize::try_from(count)
            .map_err(|_| AnalysisError::InvalidData("external contract count exceeds usize".into()))
    }

    fn final_source_count(&self) -> Result<usize, AnalysisError> {
        let count: u64 = self.conn.query_row(
            &format!(
                "WITH selected_sources AS (
                     SELECT DISTINCT contract_index, source_file, source_row_number
                     FROM {EXTERNAL_SELECTED_SOURCE_TABLE}
                 )
                 SELECT (
                     SELECT count(*) FROM {EXTERNAL_RESOLVED_CONTRACT_TABLE}
                 )::UBIGINT + (
                     SELECT count(*)
                     FROM {EXTERNAL_RESOLVED_CONTRACT_TABLE} resolved
                     JOIN selected_sources selected
                       ON selected.contract_index = resolved.contract_index
                     WHERE selected.source_file != resolved.source_file
                        OR selected.source_row_number != resolved.source_row_number
                 )::UBIGINT"
            ),
            [],
            |row| row.get(0),
        )?;
        usize::try_from(count)
            .map_err(|_| AnalysisError::InvalidData("external source count exceeds usize".into()))
    }

    fn fallback_count(&self) -> Result<u64, AnalysisError> {
        self.conn
            .query_row(
                &format!("SELECT count(*)::UBIGINT FROM {EXTERNAL_FALLBACK_CONTRACT_TABLE}"),
                [],
                |row| row.get(0),
            )
            .map_err(AnalysisError::from)
    }

    fn build_columns(
        &self,
        work_directory: &Path,
        global_offsets: &[u32],
    ) -> Result<(EncodeSources, EncodeContracts), AnalysisError> {
        let conflicting_source: bool = self.conn.query_row(
            &format!(
                "SELECT count(*) > 0
                 FROM (
                     SELECT contract_index, source_file, source_row_number
                     FROM {EXTERNAL_SELECTED_SOURCE_TABLE}
                     GROUP BY contract_index, source_file, source_row_number
                     HAVING min(payload_shard) != max(payload_shard)
                         OR min(payload_local) != max(payload_local)
                 ) conflicts"
            ),
            [],
            |row| row.get(0),
        )?;
        if conflicting_source {
            return Err(AnalysisError::InvalidData(
                "one external token source coordinate resolved to multiple payloads".into(),
            ));
        }
        self.conn.execute_batch(&format!(
            "DROP TABLE IF EXISTS {EXTERNAL_FINAL_SOURCE_TABLE};
             DROP TABLE IF EXISTS {EXTERNAL_SHARD_OFFSET_TABLE};
             CREATE TEMP TABLE {EXTERNAL_SHARD_OFFSET_TABLE}(
                 shard_id UINTEGER PRIMARY KEY,
                 payload_base UINTEGER NOT NULL
             );"
        ))?;
        {
            let mut appender = self.conn.appender(EXTERNAL_SHARD_OFFSET_TABLE)?;
            appender.append_rows(
                global_offsets
                    .iter()
                    .take(global_offsets.len().saturating_sub(1))
                    .enumerate()
                    .map(|(shard, &base)| (shard as u32, base)),
            )?;
            appender.flush()?;
        }
        self.conn.execute_batch(&format!(
            "CREATE TEMP TABLE {EXTERNAL_FINAL_SOURCE_TABLE} AS
             WITH selected_sources AS (
                 SELECT DISTINCT contract_index,
                                 source_file,
                                 source_row_number,
                                 payload_shard,
                                 payload_local
                 FROM {EXTERNAL_SELECTED_SOURCE_TABLE}
             ),
             source_rows AS (
                 SELECT resolved.order_id,
                        resolved.contract_index,
                        resolved.chain_id,
                        resolved.weight,
                        0::UTINYINT AS source_rank,
                        resolved.source_file,
                        resolved.source_row_number,
                        resolved.payload_shard,
                        resolved.payload_local,
                        resolved.payload_shard AS representative_payload_shard,
                        resolved.payload_local AS representative_payload_local
                 FROM {EXTERNAL_RESOLVED_CONTRACT_TABLE} resolved
                 UNION ALL
                 SELECT resolved.order_id,
                        resolved.contract_index,
                        resolved.chain_id,
                        resolved.weight,
                        1::UTINYINT AS source_rank,
                        selected.source_file,
                        selected.source_row_number,
                        selected.payload_shard,
                        selected.payload_local,
                        resolved.payload_shard AS representative_payload_shard,
                        resolved.payload_local AS representative_payload_local
                 FROM {EXTERNAL_RESOLVED_CONTRACT_TABLE} resolved
                 JOIN selected_sources selected
                   ON selected.contract_index = resolved.contract_index
                 WHERE selected.source_file != resolved.source_file
                    OR selected.source_row_number != resolved.source_row_number
             )
             SELECT order_id,
                    contract_index,
                    chain_id,
                    weight,
                    source_file,
                    source_row_number,
                    payload_shard,
                    payload_local,
                    representative_payload_shard,
                    representative_payload_local,
                    (row_number() OVER (
                        ORDER BY order_id, source_rank, source_file, source_row_number
                    ) - 1)::UBIGINT AS source_doc_id
             FROM source_rows"
        ))?;
        let source_count: u64 = self.conn.query_row(
            &format!("SELECT count(*)::UBIGINT FROM {EXTERNAL_FINAL_SOURCE_TABLE}"),
            [],
            |row| row.get(0),
        )?;
        let contract_count = self.resolved_count()? as u64;
        let token_count: u64 = self.conn.query_row(
            &format!(
                "SELECT count(*)::UBIGINT
                 FROM {EXTERNAL_FINAL_SOURCE_TABLE} sources
                 JOIN {EXTERNAL_SELECTED_SOURCE_TABLE} tokens
                   ON tokens.contract_index = sources.contract_index
                  AND tokens.source_file = sources.source_file
                  AND tokens.source_row_number = sources.source_row_number"
            ),
            [],
            |row| row.get(0),
        )?;
        let directory = work_directory.join("artifacts/metadata").join(format!(
            "{ENCODE_COLUMN_SPILL_PREFIX}{}",
            metadata_engine::artifacts::new_artifact_run_id()
        ));
        fs::create_dir_all(&directory)?;
        let cleanup = Arc::new(SpillDirectoryCleanup {
            path: directory.clone(),
        });
        let mut source_contracts = TypedArraySink::create(
            &directory.join("source_contract_ids.u32"),
            ArrayKind::U32,
            source_count,
        )
        .map_err(encode_err)?;
        let mut source_payloads = TypedArraySink::create(
            &directory.join("source_payload_ids.u32"),
            ArrayKind::U32,
            source_count,
        )
        .map_err(encode_err)?;
        let mut source_offsets = TypedArraySink::create(
            &directory.join("source_token_offsets.u64"),
            ArrayKind::U64,
            source_count + 1,
        )
        .map_err(encode_err)?;
        let mut source_tokens = TypedArraySink::create(
            &directory.join("source_token_ids.u32"),
            ArrayKind::U32,
            token_count,
        )
        .map_err(encode_err)?;
        let mut contract_ids = TypedArraySink::create(
            &directory.join("contract_ids.u32"),
            ArrayKind::U32,
            contract_count,
        )
        .map_err(encode_err)?;
        let mut chain_ids = TypedArraySink::create(
            &directory.join("contract_chain_ids.u32"),
            ArrayKind::U32,
            contract_count,
        )
        .map_err(encode_err)?;
        let mut contract_sources = TypedArraySink::create(
            &directory.join("contract_source_doc_ids.u32"),
            ArrayKind::U32,
            contract_count,
        )
        .map_err(encode_err)?;
        let mut contract_payloads = TypedArraySink::create(
            &directory.join("contract_payload_ids.u32"),
            ArrayKind::U32,
            contract_count,
        )
        .map_err(encode_err)?;
        let mut weights = TypedArraySink::create(
            &directory.join("contract_weights.u64"),
            ArrayKind::U64,
            contract_count,
        )
        .map_err(encode_err)?;
        source_offsets.push_u64(0).map_err(encode_err)?;
        let mut statement = self.conn.prepare(&format!(
            "SELECT sources.order_id::UINTEGER AS contract_id,
                    sources.chain_id::UINTEGER AS chain_id,
                    sources.weight::UBIGINT AS weight,
                    sources.source_doc_id::UINTEGER AS source_doc_id,
                    (source_base.payload_base + sources.payload_local)::UINTEGER
                        AS source_payload_id,
                    (representative_base.payload_base +
                        sources.representative_payload_local)::UINTEGER
                        AS representative_payload_id,
                    tokens.token_index::UINTEGER AS token_id
             FROM {EXTERNAL_FINAL_SOURCE_TABLE} sources
             JOIN {EXTERNAL_SHARD_OFFSET_TABLE} source_base
               ON source_base.shard_id = sources.payload_shard
             JOIN {EXTERNAL_SHARD_OFFSET_TABLE} representative_base
               ON representative_base.shard_id = sources.representative_payload_shard
             LEFT JOIN {EXTERNAL_SELECTED_SOURCE_TABLE} tokens
               ON tokens.contract_index = sources.contract_index
              AND tokens.source_file = sources.source_file
              AND tokens.source_row_number = sources.source_row_number
             ORDER BY sources.order_id, sources.source_doc_id, tokens.token_index NULLS FIRST"
        ))?;
        let batches = statement.stream_arrow(
            [],
            Arc::new(Schema::new(vec![
                Field::new("contract_id", DataType::UInt32, false),
                Field::new("chain_id", DataType::UInt32, false),
                Field::new("weight", DataType::UInt64, false),
                Field::new("source_doc_id", DataType::UInt32, false),
                Field::new("source_payload_id", DataType::UInt32, false),
                Field::new("representative_payload_id", DataType::UInt32, false),
                Field::new("token_id", DataType::UInt32, true),
            ])),
        )?;
        let mut previous_source = None::<u32>;
        let mut previous_contract = None::<u32>;
        let mut sources_written = 0u64;
        let mut contracts_written = 0u64;
        let mut tokens_written = 0u64;
        for batch in batches {
            let contracts = required_arrow_column::<UInt32Array>(&batch, 0, "contract_id")?;
            let chains = required_arrow_column::<UInt32Array>(&batch, 1, "chain_id")?;
            let contract_weights = required_arrow_column::<UInt64Array>(&batch, 2, "weight")?;
            let source_ids = required_arrow_column::<UInt32Array>(&batch, 3, "source_doc_id")?;
            let payload_ids = required_arrow_column::<UInt32Array>(&batch, 4, "source_payload_id")?;
            let representative_payloads =
                required_arrow_column::<UInt32Array>(&batch, 5, "representative_payload_id")?;
            let token_ids = required_arrow_column::<UInt32Array>(&batch, 6, "token_id")?;
            for row in 0..batch.num_rows() {
                let source_id = source_ids.value(row);
                let contract_id = contracts.value(row);
                if previous_source != Some(source_id) {
                    if previous_source.is_some() {
                        source_offsets
                            .push_u64(tokens_written)
                            .map_err(encode_err)?;
                    }
                    if u64::from(source_id) != sources_written {
                        return Err(AnalysisError::InvalidData(
                            "external source identities are not dense".into(),
                        ));
                    }
                    source_contracts.push_u32(contract_id).map_err(encode_err)?;
                    source_payloads
                        .push_u32(payload_ids.value(row))
                        .map_err(encode_err)?;
                    sources_written += 1;
                    previous_source = Some(source_id);
                }
                if previous_contract != Some(contract_id) {
                    if u64::from(contract_id) != contracts_written {
                        return Err(AnalysisError::InvalidData(
                            "external contract identities are not dense".into(),
                        ));
                    }
                    contract_ids.push_u32(contract_id).map_err(encode_err)?;
                    chain_ids.push_u32(chains.value(row)).map_err(encode_err)?;
                    contract_sources.push_u32(source_id).map_err(encode_err)?;
                    contract_payloads
                        .push_u32(representative_payloads.value(row))
                        .map_err(encode_err)?;
                    weights
                        .push_u64(contract_weights.value(row))
                        .map_err(encode_err)?;
                    contracts_written += 1;
                    previous_contract = Some(contract_id);
                }
                if !token_ids.is_null(row) {
                    source_tokens
                        .push_u32(token_ids.value(row))
                        .map_err(encode_err)?;
                    tokens_written += 1;
                }
            }
        }
        if previous_source.is_some() {
            source_offsets
                .push_u64(tokens_written)
                .map_err(encode_err)?;
        }
        if sources_written != source_count
            || contracts_written != contract_count
            || tokens_written != token_count
        {
            return Err(AnalysisError::InvalidData(format!(
                "external final column count changed: sources={sources_written}/{source_count}, \
                 contracts={contracts_written}/{contract_count}, tokens={tokens_written}/{token_count}"
            )));
        }
        source_contracts.finish().map_err(encode_err)?;
        source_payloads.finish().map_err(encode_err)?;
        source_offsets.finish().map_err(encode_err)?;
        source_tokens.finish().map_err(encode_err)?;
        contract_ids.finish().map_err(encode_err)?;
        chain_ids.finish().map_err(encode_err)?;
        contract_sources.finish().map_err(encode_err)?;
        contract_payloads.finish().map_err(encode_err)?;
        weights.finish().map_err(encode_err)?;
        Ok((
            EncodeSources::Disk(Box::new(DiskEncodeSources {
                contract_ids: map_u32_array(&directory.join("source_contract_ids.u32"))
                    .map_err(encode_err)?,
                payload_ids: map_u32_array(&directory.join("source_payload_ids.u32"))
                    .map_err(encode_err)?,
                token_offsets: map_u64_array(&directory.join("source_token_offsets.u64"))
                    .map_err(encode_err)?,
                token_ids: map_u32_array(&directory.join("source_token_ids.u32"))
                    .map_err(encode_err)?,
                _cleanup: Arc::clone(&cleanup),
            })),
            EncodeContracts::Disk(Box::new(DiskEncodeContracts {
                contract_ids: map_u32_array(&directory.join("contract_ids.u32"))
                    .map_err(encode_err)?,
                chain_ids: map_u32_array(&directory.join("contract_chain_ids.u32"))
                    .map_err(encode_err)?,
                source_doc_ids: map_u32_array(&directory.join("contract_source_doc_ids.u32"))
                    .map_err(encode_err)?,
                payload_ids: map_u32_array(&directory.join("contract_payload_ids.u32"))
                    .map_err(encode_err)?,
                weights: map_u64_array(&directory.join("contract_weights.u64"))
                    .map_err(encode_err)?,
                _cleanup: cleanup,
            })),
        ))
    }
}

impl Drop for ExternalRegistrationSpill<'_> {
    fn drop(&mut self) {
        let _ = self.conn.execute_batch(&format!(
            "DROP TABLE IF EXISTS {EXTERNAL_SELECTED_SOURCE_TABLE};
             DROP TABLE IF EXISTS {EXTERNAL_RESOLVED_CONTRACT_TABLE};
             DROP TABLE IF EXISTS {EXTERNAL_FALLBACK_CONTRACT_TABLE};
             DROP TABLE IF EXISTS {EXTERNAL_FINAL_SOURCE_TABLE};
             DROP TABLE IF EXISTS {EXTERNAL_SHARD_OFFSET_TABLE};"
        ));
    }
}

fn payload_handle_ref(handle: PayloadHandle) -> PayloadRef {
    match handle {
        PayloadHandle::Memory(payload) | PayloadHandle::Spill(payload) => payload,
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
    source_count: usize,
    unique_payload_bytes: u64,
) -> Result<u64, AnalysisError> {
    let source_offset_count = source_count
        .checked_add(1)
        .ok_or_else(|| AnalysisError::InvalidData("Encode source offset count overflow".into()))?;
    let mut total = ENCODE_RESIDENT_FIXED_BYTES;
    // A retained token cannot consume fewer than one input byte.  Sixty-four
    // bytes/input-byte covers both term dictionaries (owned strings + hash
    // buckets), per-payload `(term,frequency)` slots, offsets and allocator
    // load-factor slack.  The byte count is measured after CAS dedup, so this
    // remains substantially tighter than the raw Prepare relation.
    let term_state_upper = unique_payload_bytes
        .checked_mul(64)
        .ok_or_else(|| AnalysisError::InvalidData("Encode term-state estimate overflow".into()))?;
    for bytes in [
        capacity_bytes::<u32>(payload_count)?,
        capacity_bytes::<EncodePayloadRow>(payload_count)?,
        term_state_upper,
        hash_map_capacity_bytes::<(u32, u32), usize>(contract_count)?,
        capacity_bytes::<u32>(source_count)?,
        capacity_bytes::<u32>(source_count)?,
        capacity_bytes::<u64>(source_offset_count)?,
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

fn payload_finalize_admission_error(mode: PayloadStorageMode, error: MemoryError) -> AnalysisError {
    let context = if mode.is_spill() {
        "metadata payload bodies were spilled successfully, but the remaining resident \
         source/contract/atom state still exceeds the Encode memory envelope"
    } else {
        "metadata encode live cardinality admission"
    };
    AnalysisError::InvalidData(format!("{context}: {error}"))
}

fn frozen_encode_state_resident_bytes(
    sources: &EncodeSources,
    payloads: &EncodePayloadTerms,
    contracts: &EncodeContracts,
    atoms: &EncodeAtomSketches,
    fallback_atoms: &EncodeFallbackAtoms,
) -> Result<u64, AnalysisError> {
    pre_atom_encode_state_resident_bytes(sources, payloads, contracts, fallback_atoms)?
        .checked_add(atoms.resident_capacity_bytes()?)
        .ok_or_else(|| {
            AnalysisError::InvalidData("Encode frozen resident accounting overflow".into())
        })
}

fn pre_atom_encode_state_resident_bytes(
    sources: &EncodeSources,
    payloads: &EncodePayloadTerms,
    contracts: &EncodeContracts,
    fallback_atoms: &EncodeFallbackAtoms,
) -> Result<u64, AnalysisError> {
    [
        sources.resident_capacity_bytes()?,
        payloads.resident_capacity_bytes()?,
        contracts.resident_capacity_bytes()?,
        fallback_atoms.resident_capacity_bytes()?,
    ]
    .into_iter()
    .try_fold(ENCODE_RESIDENT_FIXED_BYTES, |total, bytes| {
        total.checked_add(bytes).ok_or_else(|| {
            AnalysisError::InvalidData("Encode pre-atom resident accounting overflow".into())
        })
    })
}

// After feature persistence, Blocking only needs the compact atom sketches and
// fallback membership CSR. Releasing source/payload/contract columns here
// avoids retaining the full Encode state while another parallel compiler and
// its scratch space are active.
fn blocking_encode_state_resident_bytes(
    atoms: &EncodeAtomSketches,
    fallback_atoms: &EncodeFallbackAtoms,
) -> Result<u64, AnalysisError> {
    [
        atoms.resident_capacity_bytes()?,
        fallback_atoms.resident_capacity_bytes()?,
    ]
    .into_iter()
    .try_fold(ENCODE_RESIDENT_FIXED_BYTES, |total, bytes| {
        total.checked_add(bytes).ok_or_else(|| {
            AnalysisError::InvalidData("Encode blocking resident accounting overflow".into())
        })
    })
}

fn planned_feature_persist_growth(
    sources: EncodeSourceView<'_>,
    payloads: PayloadTermView<'_>,
    contracts: EncodeContractView<'_>,
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
    let template_token_count = payloads
        .template_terms
        .iter()
        .copied()
        .max()
        .map_or(0u64, |term| u64::from(term) + 1);
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
        .and_then(|bytes| template_token_count.checked_mul(8)?.checked_add(bytes))
        .and_then(|bytes| payload_count.checked_mul(96)?.checked_add(bytes))
        .ok_or_else(|| AnalysisError::InvalidData("Encode CSR admission overflow".into()))
}

fn external_csr_storage_upper_bound(sources: EncodeSourceView<'_>) -> Result<u64, AnalysisError> {
    const DUCKDB_ROW_AND_SORT_OVERHEAD: u64 = 64;
    const FILE_AND_SORT_ALLOWANCE: u64 = 256 * 1024 * 1024;
    let memberships = u64::try_from(sources.token_ids.len()).map_err(|_| {
        AnalysisError::InvalidData("external CSR membership count exceeds u64".into())
    })?;
    let contracts = sources
        .contract_ids
        .iter()
        .copied()
        .max()
        .map_or(0u64, |id| u64::from(id) + 1);
    let tokens = sources
        .token_ids
        .iter()
        .copied()
        .max()
        .map_or(0u64, |id| u64::from(id) + 1);
    memberships
        .checked_mul(DUCKDB_ROW_AND_SORT_OVERHEAD)
        .and_then(|bytes| {
            contracts
                .checked_add(tokens)?
                .checked_mul(16)?
                .checked_add(bytes)
        })
        .and_then(|bytes| bytes.checked_add(FILE_AND_SORT_ALLOWANCE))
        .ok_or_else(|| AnalysisError::InvalidData("external CSR storage estimate overflow".into()))
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TokenRange {
    start: usize,
    end: usize,
}

/// One retained token-specific metadata source registered into the
/// [`ShardedPayloadArena`] on behalf of a pending contract.
#[derive(Debug, Clone)]
struct PendingSourceSlot {
    payload_ref: PayloadHandle,
    token_range: TokenRange,
}

/// A contract whose representative payload (and retained token sources) has
/// been registered in the arena, but whose full parse / term interning has
/// not happened yet. Built once per contract during the presence-only
/// registration pass, then consumed in original contract order to build the
/// final Encode columns after global payload IDs are assigned.
#[derive(Debug, Clone)]
struct PendingContractSlot {
    chain_id: u32,
    weight: u64,
    representative_payload_ref: PayloadHandle,
    representative_token_range: TokenRange,
    token_sources: Vec<PendingSourceSlot>,
}

enum RegisteredPayloadLayout<'connection> {
    Memory {
        pending_contracts: Vec<PendingContractSlot>,
        pending_token_ids: Vec<u32>,
    },
    External(ExternalRegistrationSpill<'connection>),
}

struct RegisteredPayloads<'connection> {
    store: PayloadRegistrationStore,
    layout: RegisteredPayloadLayout<'connection>,
    resident_bytes: u64,
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
        store: &PayloadRegistrationStore,
        pending_token_ids: &Vec<u32>,
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
            capacity_bytes::<u32>(pending_token_ids.capacity())?,
            hash_map_capacity_bytes::<u32, PendingFallbackContract>(pending_fallbacks.capacity())?,
            store.resident_bytes()?,
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
    stream_encode_inputs_with_advisory(
        conn,
        work_directory,
        broker,
        memory_broker,
        threads,
        estimate,
        |message| eprintln!("warning: {message}"),
        progress,
    )
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(super) fn stream_encode_inputs_with_advisory(
    conn: &Connection,
    work_directory: &Path,
    broker: &mut StorageBroker,
    memory_broker: &MemoryBroker,
    threads: usize,
    estimate: EncodeAdmissionEstimate,
    advisory: impl FnMut(String),
    progress: impl FnMut(ProgressEvent),
) -> Result<EncodeStreamInputs, AnalysisError> {
    let lease = memory_broker
        .reserve(
            estimate
                .resident_peak_bytes
                .min(memory_broker.hard_top_bytes()),
        )
        .map_err(|error| {
            AnalysisError::InvalidData(format!("metadata encode memory admission: {error}"))
        })?;
    let mut resident_admission = EncodeResidentAdmission::new(lease, ENCODE_RESIDENT_FIXED_BYTES);
    stream_encode_inputs_with_admission(
        conn,
        work_directory,
        broker,
        memory_broker,
        &mut resident_admission,
        threads,
        estimate,
        advisory,
        progress,
    )
}

#[allow(clippy::too_many_arguments)]
fn stream_encode_inputs_with_admission(
    conn: &Connection,
    work_directory: &Path,
    broker: &mut StorageBroker,
    memory_broker: &MemoryBroker,
    resident_admission: &mut EncodeResidentAdmission,
    threads: usize,
    mut estimate: EncodeAdmissionEstimate,
    mut advisory: impl FnMut(String),
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
    // The database is authoritative. Estimates can become stale after resume
    // or due to engine cardinality-estimation differences; update progress and
    // path selection from the observed dimensions instead of terminating.
    estimate.token_rows = token_rows;
    estimate.representative_rows = representative_rows;
    estimate.token_relation_peak_bytes = required_relation_peak;
    estimate.resident_peak_bytes = estimate.resident_peak_bytes.max(required_relation_peak);
    let contract_count = u32::try_from(representative_rows).map_err(|_| {
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
    let storage_mode = payload_storage_mode(&estimate, memory_broker.hard_top_bytes());
    let payload_spill_reservation = if storage_mode.is_spill() {
        advisory(format!(
            "metadata encode conservative resident estimate {} exceeds the Rust envelope {}; \
             spilling unique JSON bodies to a temporary payload CAS plus exact \
             token-source/registration state to temporary storage; payload indexes and parse \
             batches stay bounded, and later term/final columns use resident or demand-paged \
             typed-array views according to measured admission",
            format_byte_size(
                usize::try_from(
                    estimate
                        .resident_peak_bytes
                        .max(payload_resident_index_upper_bound(&estimate)?)
                )
                .unwrap_or(usize::MAX)
            ),
            format_byte_size(usize::try_from(memory_broker.hard_top_bytes()).unwrap_or(usize::MAX)),
        ));
        let spill_bytes = if storage_mode == PayloadStorageMode::SpillExternalIndex {
            estimate
                .payload_spill_upper_bound_bytes
                .checked_add(payload_external_index_storage_upper_bound(&estimate)?)
                .ok_or_else(|| {
                    AnalysisError::InvalidData("payload spill storage estimate overflow".into())
                })?
        } else {
            estimate.payload_spill_upper_bound_bytes
        };
        reserve_storage_advisory(
            broker,
            ArtifactClass::PayloadCas,
            spill_bytes,
            DEFAULT_MAX_PACK_BYTES,
            "temporary Encode payload spill",
            &mut advisory,
        )?
    } else {
        None
    };
    if storage_mode == PayloadStorageMode::SpillExternalIndex {
        advisory(format!(
            "metadata payload CAS resident index upper bound {} exceeds its Rust envelope; \
             using an exact DuckDB digest index plus globally ordered typed-array pack metadata",
            payload_resident_index_upper_bound(&estimate)?
        ));
    }
    let external_cache_limit =
        usize::try_from((memory_broker.hard_top_bytes() / 64 / 160).clamp(4_096, 1_048_576))
            .unwrap_or(1_048_576);
    let external_duckdb_memory_bytes =
        (memory_broker.hard_top_bytes() / 256).clamp(16 * 1024 * 1024, 64 * 1024 * 1024);
    let mut payload_store = PayloadRegistrationStore::create(
        storage_mode,
        work_directory,
        shard_count,
        external_cache_limit,
        external_duckdb_memory_bytes,
        threads,
    )?;
    let token_source_catalog = build_retained_token_source_relation(
        conn,
        contract_count,
        &mut payload_store,
        &parse_pool,
        storage_mode.is_spill(),
        &mut progress,
    )?;
    let relation_resident_bytes = token_source_catalog.bytes();
    let relation_with_payload_store = relation_resident_bytes
        .checked_add(payload_store.resident_bytes()?)
        .ok_or_else(|| {
            AnalysisError::InvalidData("token-source+payload-store admission overflow".into())
        })?;
    if relation_with_payload_store > estimate.resident_peak_bytes {
        advisory(format!(
            "token-source relation plus payload index exceeded the conservative estimate \
             ({} > {}); continuing under measured live-capacity admission",
            relation_with_payload_store, estimate.resident_peak_bytes
        ));
    }

    let chain_totals = load_encode_chain_totals(conn)?;
    let RegisteredPayloads {
        store,
        layout,
        resident_bytes: committed_resident_bytes,
    } = match token_source_catalog {
        TokenSourceCatalog::Memory(token_source_relation) => register_representative_payloads(
            conn,
            &token_source_relation,
            relation_resident_bytes,
            payload_store,
            resident_admission,
            &parse_pool,
            &estimate,
            &mut progress,
        )?,
        TokenSourceCatalog::External(spill) => register_representative_payloads_external(
            conn,
            spill,
            payload_store,
            resident_admission,
            &parse_pool,
            &estimate,
            &mut advisory,
            &mut progress,
        )?,
    };
    // Relation coords are gone; shrink the lease to pending columns + arena.
    if let Err(error @ MemoryError::Budget { .. }) =
        resident_admission.try_set_current(committed_resident_bytes)
    {
        if matches!(&layout, RegisteredPayloadLayout::External(_)) {
            advisory(format!(
                "metadata external payload index remains above the Rust accounting envelope \
                 ({error}); continuing with all source/contract state on disk"
            ));
        } else {
            return Err(AnalysisError::InvalidData(format!(
                "metadata encode live cardinality admission: {error}"
            )));
        }
    }

    // Phase C/D: parse unique payloads in bounded batches, intern immediately,
    // then drop the ParsedMetadataDocuments before the next batch.
    let payload_reader = store.finish()?;
    let payload_count = payload_reader.payload_count();
    let mut unique_payload_bytes = 0u64;
    let mut maximum_parse_batch_growth = 0u64;
    let mut batch_bytes = 0u64;
    let mut batch_len = 0usize;
    for payload_id in 0..payload_count {
        let payload_bytes = payload_reader.payload_len(payload_id as u32)? as u64;
        unique_payload_bytes =
            unique_payload_bytes
                .checked_add(payload_bytes)
                .ok_or_else(|| {
                    AnalysisError::InvalidData("Encode unique payload bytes overflow".into())
                })?;
        batch_bytes = batch_bytes.checked_add(payload_bytes).ok_or_else(|| {
            AnalysisError::InvalidData("Encode parse batch bytes overflow".into())
        })?;
        batch_len += 1;
        if batch_len == ENCODE_UNIQUE_PARSE_BATCH_LEN || payload_id + 1 == payload_count {
            maximum_parse_batch_growth = maximum_parse_batch_growth
                .max(planned_unique_parse_batch_growth(batch_bytes, batch_len)?);
            batch_bytes = 0;
            batch_len = 0;
        }
    }
    let global_offsets = payload_reader.global_offsets().to_vec();
    let (registered_contract_count, final_source_count) = match &layout {
        RegisteredPayloadLayout::Memory {
            pending_contracts, ..
        } => (
            pending_contracts.len(),
            pending_contracts
                .iter()
                .try_fold(0usize, |total, contract| {
                    let contract_sources =
                        contract.token_sources.len().checked_add(1).ok_or_else(|| {
                            AnalysisError::InvalidData("Encode final source count overflow".into())
                        })?;
                    total.checked_add(contract_sources).ok_or_else(|| {
                        AnalysisError::InvalidData("Encode final source count overflow".into())
                    })
                })?,
        ),
        RegisteredPayloadLayout::External(spill) => {
            (spill.resolved_count()?, spill.final_source_count()?)
        }
    };
    let external_registration = matches!(&layout, RegisteredPayloadLayout::External(_));
    let finalize_growth = planned_encode_finalize_growth(
        payload_count,
        registered_contract_count,
        final_source_count,
        unique_payload_bytes,
    )?;
    let finalize_and_parse_growth = finalize_growth
        .checked_add(maximum_parse_batch_growth)
        .ok_or_else(|| AnalysisError::InvalidData("Encode finalize peak overflow".into()))?;
    let (spill_terms, admitted_after_finalize) = match resident_admission
        .try_reserve_growth(committed_resident_bytes, finalize_and_parse_growth)
    {
        Ok(()) => (
            false,
            committed_resident_bytes
                .checked_add(finalize_growth)
                .ok_or_else(|| {
                    AnalysisError::InvalidData("Encode finalize admission overflow".into())
                })?,
        ),
        Err(error @ MemoryError::Budget { .. }) => {
            advisory(format!(
                "metadata Encode resident term dictionary/final SoA plan exceeded the Rust \
                     envelope ({error}); externalizing exact token dictionaries and payload term \
                     columns through DuckDB spill, then continuing from demand-paged typed arrays"
            ));
            resident_admission.commit(committed_resident_bytes)?;
            (true, committed_resident_bytes)
        }
        Err(error) => return Err(payload_finalize_admission_error(storage_mode, error)),
    };
    resident_admission.commit(admitted_after_finalize)?;
    let term_spill_storage_reservation = if spill_terms {
        reserve_storage_advisory(
            broker,
            ArtifactClass::Feature,
            estimate.payload_spill_upper_bound_bytes.saturating_mul(4),
            DEFAULT_MAX_PACK_BYTES,
            "temporary Encode term dictionary spill",
            &mut advisory,
        )?
    } else {
        None
    };
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
    let mut resident_payloads =
        (!spill_terms).then(|| PayloadTermSoA::with_payload_capacity(payload_count));
    let payload_interner =
        (!spill_terms).then(|| ShardedPayloadTermInterner::with_shard_count(shard_count));
    let term_spill = spill_terms
        .then(|| DuckDbPayloadTermSpill::create(conn))
        .transpose()?;
    let mut parsed_completed = 0u64;
    let mut batch_start = 0usize;
    let mut target_batch_len = ENCODE_UNIQUE_PARSE_BATCH_LEN;
    let mut external_parse_admission_warned = false;
    while batch_start < payload_count {
        let mut batch_len = target_batch_len.min(payload_count - batch_start).max(1);
        let (batch_end, batch_json_bytes) = loop {
            let batch_end = batch_start + batch_len;
            let mut batch_json_bytes = 0u64;
            for payload_id in batch_start..batch_end {
                let len = payload_reader.payload_len(payload_id as u32)? as u64;
                batch_json_bytes = batch_json_bytes.checked_add(len).ok_or_else(|| {
                    AnalysisError::InvalidData("Encode unique parse JSON bytes overflow".into())
                })?;
            }
            let growth = planned_unique_parse_batch_growth(batch_json_bytes, batch_len)?;
            match resident_admission.try_reserve_growth(admitted_after_finalize, growth) {
                Ok(()) => break (batch_end, batch_json_bytes),
                Err(error @ MemoryError::Budget { .. }) if batch_len > 1 => {
                    let reduced = batch_len.div_ceil(2);
                    advisory(format!(
                        "metadata unique-payload parse batch of {batch_len} documents exceeded the \
                         live memory budget ({error}); reducing subsequent batches to {reduced} \
                         and continuing"
                    ));
                    batch_len = reduced;
                    target_batch_len = reduced;
                }
                Err(error @ MemoryError::Budget { .. }) if external_registration => {
                    advisory(format!(
                        "metadata external unique-payload parse reached a one-document batch above \
                         the accounting envelope ({error}); continuing with the bounded batch and \
                         disk-backed dictionaries"
                    ));
                    break (batch_end, batch_json_bytes);
                }
                Err(error) => {
                    return Err(AnalysisError::InvalidData(format!(
                        "metadata encode live cardinality admission: {error}"
                    )));
                }
            }
        };
        debug_assert!(batch_json_bytes <= (batch_len as u64).saturating_mul(64 * 1024));
        let parsed_batch = payload_reader.parse_batch(batch_start..batch_end, &parse_pool)?;
        let completed_batch = if let Some(term_spill) = term_spill.as_ref() {
            term_spill.append_batch(batch_start, parsed_batch, &parse_pool)?
        } else {
            let interned_batch = parse_pool.install(|| {
                payload_interner
                    .as_ref()
                    .expect("resident interner exists in the memory path")
                    .intern_batch(parsed_batch)
            })?;
            let batch_soa =
                PayloadTermSoA::from_term_lists_owned(interned_batch).map_err(|error| {
                    AnalysisError::InvalidData(format!("payload term SoA pack: {error}"))
                })?;
            resident_payloads
                .as_mut()
                .expect("resident term columns exist in the memory path")
                .append_soa(&batch_soa)
                .map_err(|error| {
                    AnalysisError::InvalidData(format!("payload term SoA append: {error}"))
                })?;
            batch_soa.payload_count()
        };
        parsed_completed = parsed_completed.saturating_add(completed_batch as u64);
        progress(ProgressEvent::determinate(
            ProgressPhase::EncodeParseUniquePayloads,
            parsed_completed,
            payload_total,
            WorkUnit::Items,
            EngineCounters::default(),
        ));
        emit_encode_progress(
            &mut progress,
            ProgressPhase::EncodeBuildTermDictionary,
            parsed_completed,
            payload_total,
        );
        // Parsed scratch is dropped with the batch Vec; keep finalize floor.
        if let Err(error @ MemoryError::Budget { .. }) =
            resident_admission.try_set_current(admitted_after_finalize)
        {
            if external_registration {
                if !external_parse_admission_warned {
                    advisory(format!(
                        "metadata external parse baseline remains above the accounting envelope \
                         ({error}); bounded batches and disk-backed columns remain active"
                    ));
                    external_parse_admission_warned = true;
                }
            } else {
                return Err(AnalysisError::InvalidData(format!(
                    "metadata encode live cardinality admission: {error}"
                )));
            }
        }
        batch_start = batch_end;
    }
    drop(payload_interner);
    // Payload bodies are only needed until every unique payload has been
    // parsed and interned. Spill packs and their storage reservation are
    // released before final columns and artifacts are built.
    drop(payload_reader);
    drop(payload_spill_reservation);
    let payloads = if let Some(term_spill) = term_spill {
        EncodePayloadTerms::Disk(Box::new(
            term_spill.materialize(work_directory, payload_count)?,
        ))
    } else {
        EncodePayloadTerms::Memory(
            resident_payloads
                .take()
                .expect("resident payload columns exist in the memory path"),
        )
    };
    drop(term_spill_storage_reservation);

    // Phase E: build final columns from the already-resolved payload_ids, in
    // original contract order (immediate contracts first, then fallback
    // contracts, matching the order they were registered above).
    let contract_total = registered_contract_count as u64;
    progress(ProgressEvent::determinate(
        ProgressPhase::EncodeBuildColumns,
        0,
        contract_total,
        WorkUnit::Items,
        EngineCounters::default(),
    ));
    let spill_columns = if external_registration {
        advisory(
            "metadata Encode source/contract SoA exceeded the Rust envelope together with the \
             token-source relation; writing exact fixed-width columns to temporary typed arrays \
             and continuing from demand-paged views"
                .into(),
        );
        true
    } else if spill_terms {
        let column_growth =
            planned_final_column_growth(final_source_count, registered_contract_count)?;
        match resident_admission.try_reserve_growth(admitted_after_finalize, column_growth) {
            Ok(()) => false,
            Err(error @ MemoryError::Budget { .. }) => {
                advisory(format!(
                    "metadata Encode source/contract SoA exceeded the Rust envelope ({error}); \
                     writing exact fixed-width columns to temporary typed arrays and continuing \
                     from demand-paged views"
                ));
                resident_admission.commit(admitted_after_finalize)?;
                true
            }
            Err(error) => {
                return Err(AnalysisError::InvalidData(format!(
                    "metadata Encode final column admission: {error}"
                )));
            }
        }
    } else {
        false
    };
    let (sources, contracts) = match layout {
        RegisteredPayloadLayout::External(spill) => {
            let columns = spill.build_columns(work_directory, &global_offsets)?;
            emit_encode_progress(
                &mut progress,
                ProgressPhase::EncodeBuildColumns,
                contract_total,
                contract_total,
            );
            columns
        }
        RegisteredPayloadLayout::Memory {
            pending_contracts,
            pending_token_ids,
        } if spill_columns => {
            let columns = build_disk_encoded_columns(
                work_directory,
                pending_contracts,
                pending_token_ids,
                &global_offsets,
                final_source_count,
            )?;
            emit_encode_progress(
                &mut progress,
                ProgressPhase::EncodeBuildColumns,
                contract_total,
                contract_total,
            );
            columns
        }
        RegisteredPayloadLayout::Memory {
            pending_contracts,
            pending_token_ids,
        } => {
            let mut sources = EncodeSourceSoA::with_source_capacity(final_source_count);
            // Registration writes the token arena in exact final CSR source order.
            // Moving the Vec here avoids a second all-token allocation/copy peak.
            sources.token_ids = pending_token_ids;
            let mut contracts = EncodeContractSoA::with_contract_capacity(pending_contracts.len());
            let mut token_cursor = 0usize;
            for (index, slot) in pending_contracts.into_iter().enumerate() {
                let contract_id = u32::try_from(index).map_err(|_| {
                    AnalysisError::InvalidData("metadata contract count exceeds u32".into())
                })?;
                build_encoded_contract(
                    slot,
                    contract_id,
                    &global_offsets,
                    &mut token_cursor,
                    &mut sources,
                    &mut contracts,
                )?;
                emit_encode_progress(
                    &mut progress,
                    ProgressPhase::EncodeBuildColumns,
                    index as u64 + 1,
                    contract_total,
                );
            }
            if token_cursor != sources.token_ids.len() {
                return Err(AnalysisError::InvalidData(format!(
                    "pending token arena was not consumed exactly: consumed={token_cursor}, total={}",
                    sources.token_ids.len()
                )));
            }
            (
                EncodeSources::Memory(sources),
                EncodeContracts::Memory(contracts),
            )
        }
    };

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
    let payload_feature_identity = match &payloads {
        EncodePayloadTerms::Memory(_) => payload_feature_identity_ids(payloads.view()),
        EncodePayloadTerms::Disk(_) => payload_feature_identity_ids_sorted(payloads.view()),
    };
    let fallback_growth = planned_fallback_atom_growth(contracts.contract_count())?;
    let spill_fallback_atoms = spill_columns || fallback_growth > memory_broker.available_bytes();
    let fallback_atoms = if spill_fallback_atoms {
        advisory(format!(
            "metadata Encode fallback-atom grouping estimate {} exceeds the remaining Rust \
             envelope {}; grouping exact (chain, feature) atoms in DuckDB and continuing from \
             demand-paged CSR columns",
            fallback_growth,
            memory_broker.available_bytes()
        ));
        let temporary_reservation = reserve_storage_advisory(
            broker,
            ArtifactClass::Feature,
            fallback_growth.saturating_mul(2),
            64 * 1024 * 1024,
            "temporary Encode fallback atom spill",
            &mut advisory,
        )?;
        let batch_records =
            usize::try_from((memory_broker.available_bytes() / 20).clamp(4_096, 1_048_576))
                .unwrap_or(1_048_576);
        let spill = DuckDbAtomSpill::create(conn)?;
        let atoms = spill.build(
            work_directory,
            contracts.view(),
            &payload_feature_identity,
            batch_records,
        )?;
        drop(spill);
        drop(temporary_reservation);
        atoms_completed = contract_total;
        emit_encode_progress(
            &mut progress,
            ProgressPhase::EncodeBuildAtoms,
            atoms_completed,
            atoms_total,
        );
        EncodeFallbackAtoms::Disk(Box::new(atoms))
    } else {
        EncodeFallbackAtoms::Memory(build_fallback_atoms_hash_sharded(
            contracts.view(),
            &payload_feature_identity,
            shard_count,
            &parse_pool,
            |completed| {
                atoms_completed = completed;
                emit_encode_progress(
                    &mut progress,
                    ProgressPhase::EncodeBuildAtoms,
                    atoms_completed,
                    atoms_total,
                );
            },
        )?)
    };
    let pre_atom_resident_bytes =
        pre_atom_encode_state_resident_bytes(&sources, &payloads, &contracts, &fallback_atoms)?;
    let sketch_growth =
        planned_atom_sketch_growth(payloads.view(), fallback_atoms.view().atom_payloads)?;
    let atoms = match resident_admission.try_reserve_growth(pre_atom_resident_bytes, sketch_growth)
    {
        Ok(()) => {
            let atoms = parse_pool.install(|| {
                build_base_equivalent_atom_sketch_soa_from_view_parallel(
                    payloads.view(),
                    fallback_atoms.view().atom_payloads,
                    threads,
                )
            });
            EncodeAtomSketches::Memory(atoms)
        }
        Err(error @ MemoryError::Budget { .. }) => {
            advisory(format!(
                "metadata Encode AtomSketch/anchor SoA exceeded the Rust envelope ({error}); \
                 computing exact global document frequencies and sketches through a DuckDB spill, \
                 then continuing from demand-paged typed arrays"
            ));
            resident_admission.commit(pre_atom_resident_bytes)?;
            let temporary_bytes = external_atom_sketch_storage_upper_bound(
                payloads.view(),
                fallback_atoms.view().atom_payloads,
            )?;
            let temporary_reservation = reserve_storage_advisory(
                broker,
                ArtifactClass::Blocking,
                temporary_bytes,
                128 * 1024 * 1024,
                "temporary Encode atom-sketch spill",
                &mut advisory,
            )?;
            let batch_records =
                usize::try_from((memory_broker.available_bytes() / 12).clamp(4_096, 1_048_576))
                    .unwrap_or(1_048_576);
            let spill = DuckDbAtomSketchSpill::create(conn)?;
            let atoms = spill.build(
                work_directory,
                payloads.view(),
                fallback_atoms.view().atom_payloads,
                batch_records,
            )?;
            drop(spill);
            drop(temporary_reservation);
            EncodeAtomSketches::Disk(Box::new(atoms))
        }
        Err(error) => {
            return Err(AnalysisError::InvalidData(format!(
                "metadata Encode atom sketch admission: {error}"
            )));
        }
    };
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
    let frozen_resident_bytes = frozen_encode_state_resident_bytes(
        &sources,
        &payloads,
        &contracts,
        &atoms,
        &fallback_atoms,
    )?;
    resident_admission
        .try_set_current(frozen_resident_bytes)
        .map_err(|error| payload_finalize_admission_error(storage_mode, error))?;
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
/// sequentially inserts eligible JSON into the selected payload store. Token
/// sources reuse catalog handles (JSON is already in memory or in the pack).
/// Full parse is deferred to a unique pass over every arena payload.
///
/// Returns columns+arena resident bytes **without** the token-source relation
/// structural bytes, plus global payload ID offsets for the unique-parse pass.
#[allow(clippy::too_many_arguments)]
fn register_representative_payloads<'connection>(
    conn: &'connection Connection,
    token_source_relation: &TokenSourceRelation,
    relation_resident_bytes: u64,
    mut store: PayloadRegistrationStore,
    resident_admission: &mut EncodeResidentAdmission,
    parse_pool: &rayon::ThreadPool,
    estimate: &EncodeAdmissionEstimate,
    progress: &mut impl FnMut(ProgressEvent),
) -> Result<RegisteredPayloads<'connection>, AnalysisError> {
    let mut pending_contracts = Vec::<PendingContractSlot>::new();
    let mut pending_fallbacks = HashMap::<u32, PendingFallbackContract>::new();
    let mut pending_token_ids = Vec::<u32>::new();
    let mut registration_accounting = EncodeRegistrationAccounting::default();

    let mut columns_resident_bytes = registration_accounting.resident_bytes(
        &pending_contracts,
        &pending_fallbacks,
        &store,
        &pending_token_ids,
    )?;
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
    let batches = statement.stream_arrow(
        [],
        Arc::new(Schema::new(vec![
            Field::new("metadata_contract_index", DataType::UInt32, false),
            Field::new("chain_id", DataType::UInt32, false),
            Field::new("metadata_json", DataType::Utf8, false),
            Field::new("nft_count", DataType::Int64, false),
            Field::new("metadata_source_file", DataType::UInt32, false),
            Field::new("metadata_source_row_number", DataType::UInt64, false),
        ])),
    )?;

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

            let representative_payload_ref = store.insert(json.as_bytes())?;
            let (representative_token_range, token_slots) = token_source_relation
                .append_contract_layout(
                    contract_index,
                    SourceCoordinate {
                        source_file,
                        source_row_number,
                    },
                    &mut pending_token_ids,
                )?;
            pending_contracts.push(PendingContractSlot {
                chain_id,
                weight,
                representative_payload_ref,
                representative_token_range,
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
            &store,
            &pending_token_ids,
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
            &mut store,
            &mut pending_contracts,
            &mut pending_fallbacks,
            &mut pending_token_ids,
            resident_admission,
            &mut registration_accounting,
            columns_resident_bytes,
            parse_pool,
            progress,
        )?;
    }
    drop(pending_fallbacks);

    Ok(RegisteredPayloads {
        store,
        layout: RegisteredPayloadLayout::Memory {
            pending_contracts,
            pending_token_ids,
        },
        resident_bytes: columns_resident_bytes,
    })
}

#[allow(clippy::too_many_arguments)]
fn register_representative_payloads_external<'connection>(
    conn: &'connection Connection,
    spill: ExternalRegistrationSpill<'connection>,
    mut store: PayloadRegistrationStore,
    resident_admission: &mut EncodeResidentAdmission,
    parse_pool: &rayon::ThreadPool,
    estimate: &EncodeAdmissionEstimate,
    advisory: &mut impl FnMut(String),
    progress: &mut impl FnMut(ProgressEvent),
) -> Result<RegisteredPayloads<'connection>, AnalysisError> {
    let mut committed_resident_bytes = ENCODE_RESIDENT_FIXED_BYTES
        .checked_add(store.resident_bytes()?)
        .ok_or_else(|| AnalysisError::InvalidData("external registration overflow".into()))?;
    if let Err(error @ MemoryError::Budget { .. }) =
        resident_admission.try_set_current(committed_resident_bytes)
    {
        advisory(format!(
            "metadata external registration payload index exceeded the Rust accounting envelope \
             ({error}); continuing with disk-backed relation/columns and bounded Arrow batches"
        ));
    }
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
    let batches = statement.stream_arrow(
        [],
        Arc::new(Schema::new(vec![
            Field::new("metadata_contract_index", DataType::UInt32, false),
            Field::new("chain_id", DataType::UInt32, false),
            Field::new("metadata_json", DataType::Utf8, false),
            Field::new("nft_count", DataType::Int64, false),
            Field::new("metadata_source_file", DataType::UInt32, false),
            Field::new("metadata_source_row_number", DataType::UInt64, false),
        ])),
    )?;
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
                    Ok((eligible, eligible && metadata_has_prefilter_tokens(json)))
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
        let mut resolved = Vec::new();
        let mut fallbacks = Vec::new();
        for (index, &(eligible, has_tokens)) in presence.iter().enumerate() {
            representative_rows_registered = representative_rows_registered.saturating_add(1);
            if !eligible {
                continue;
            }
            let weight = u64::try_from(nft_counts.value(index)).map_err(|_| {
                AnalysisError::InvalidData("negative metadata contract nft_count".into())
            })?;
            let contract_index = contract_indexes.value(index);
            let chain_id = chain_ids.value(index);
            if has_tokens {
                resolved.push(ExternalResolvedContract {
                    contract_index,
                    chain_id,
                    weight,
                    source_file: source_files.value(index),
                    source_row_number: source_rows.value(index),
                    payload_ref: store
                        .insert(required_arrow_string(json_column, index)?.as_bytes())?,
                });
            } else {
                fallbacks.push(ExternalFallbackContract {
                    contract_index,
                    chain_id,
                    weight,
                });
            }
        }
        spill.append_registration(&resolved, &fallbacks)?;
        committed_resident_bytes = ENCODE_RESIDENT_FIXED_BYTES
            .checked_add(store.resident_bytes()?)
            .ok_or_else(|| AnalysisError::InvalidData("external registration overflow".into()))?;
        if let Err(error @ MemoryError::Budget { .. }) =
            resident_admission.try_set_current(committed_resident_bytes)
        {
            advisory(format!(
                "metadata external payload index exceeded the Rust envelope after {} \
                 representatives ({error}); continuing because contract/source state is on disk",
                representative_rows_registered
            ));
        }
        emit_encode_progress(
            progress,
            ProgressPhase::EncodeRegisterPayloads,
            representative_rows_registered,
            estimate.representative_rows,
        );
    }
    drop(statement);
    resolve_external_fallback_contracts(conn, &spill, &mut store, parse_pool, progress)?;
    committed_resident_bytes = ENCODE_RESIDENT_FIXED_BYTES
        .checked_add(store.resident_bytes()?)
        .ok_or_else(|| AnalysisError::InvalidData("external registration overflow".into()))?;
    Ok(RegisteredPayloads {
        store,
        layout: RegisteredPayloadLayout::External(spill),
        resident_bytes: committed_resident_bytes,
    })
}

fn resolve_external_fallback_contracts(
    conn: &Connection,
    spill: &ExternalRegistrationSpill<'_>,
    store: &mut PayloadRegistrationStore,
    parse_pool: &rayon::ThreadPool,
    progress: &mut impl FnMut(ProgressEvent),
) -> Result<(), AnalysisError> {
    let fallback_total = spill.fallback_count()?;
    progress(ProgressEvent::determinate(
        ProgressPhase::EncodeFallbackSources,
        0,
        fallback_total,
        WorkUnit::Contracts,
        EngineCounters::default(),
    ));
    if fallback_total == 0 {
        return Ok(());
    }
    let mut statement = conn.prepare(&format!(
        "SELECT fallback.contract_index::UINTEGER,
                fallback.chain_id::UINTEGER,
                fallback.weight::UBIGINT,
                rows.metadata_json,
                rows.source_file::UINTEGER,
                rows.source_row_number::UBIGINT
         FROM {EXTERNAL_FALLBACK_CONTRACT_TABLE} fallback
         JOIN analysis_contracts contracts
           ON contracts.metadata_contract_index = fallback.contract_index
         JOIN metadata_rows rows ON rows.contract_id = contracts.contract_id
         WHERE rows.metadata_eligible
         ORDER BY fallback.contract_index,
                  rows.token_id,
                  rows.source_file,
                  rows.source_row_number"
    ))?;
    let batches = statement.stream_arrow(
        [],
        Arc::new(Schema::new(vec![
            Field::new("contract_index", DataType::UInt32, false),
            Field::new("chain_id", DataType::UInt32, false),
            Field::new("weight", DataType::UInt64, false),
            Field::new("metadata_json", DataType::Utf8, false),
            Field::new("source_file", DataType::UInt32, false),
            Field::new("source_row_number", DataType::UInt64, false),
        ])),
    )?;
    let mut current_contract = None::<u32>;
    let mut selected = None::<ExternalResolvedContract>;
    let mut output = Vec::with_capacity(4_096);
    let mut completed = 0u64;
    let mut selected_count = 0u64;
    let mut scanned = 0u64;
    for batch in batches {
        let contracts = required_arrow_column::<UInt32Array>(&batch, 0, "contract_index")?;
        let chains = required_arrow_column::<UInt32Array>(&batch, 1, "chain_id")?;
        let weights = required_arrow_column::<UInt64Array>(&batch, 2, "weight")?;
        let json = batch.column(3).as_ref();
        let source_files = required_arrow_column::<UInt32Array>(&batch, 4, "source_file")?;
        let source_rows = required_arrow_column::<UInt64Array>(&batch, 5, "source_row_number")?;
        let ranges = ordered_group_ranges(batch.num_rows(), |index| contracts.value(index));
        let already_selected = selected.as_ref().map(|row| row.contract_index);
        let indexes = parse_pool.install(|| {
            first_usable_rows_by_ordered_group(&ranges, already_selected, |index| {
                Ok(metadata_has_prefilter_tokens(required_arrow_string(
                    json, index,
                )?))
            })
        })?;
        for ((contract_id, range), selected_index) in ranges.iter().zip(indexes) {
            if current_contract != Some(*contract_id) {
                if let Some(row) = selected.take() {
                    output.push(row);
                    selected_count += 1;
                    if output.len() == output.capacity() {
                        spill.append_registration(&output, &[])?;
                        output.clear();
                    }
                }
                if current_contract.is_some() {
                    completed += 1;
                }
                current_contract = Some(*contract_id);
            }
            scanned = scanned.saturating_add(range.len() as u64);
            if selected.is_none() {
                if let Some(index) = selected_index {
                    selected = Some(ExternalResolvedContract {
                        contract_index: *contract_id,
                        chain_id: chains.value(index),
                        weight: weights.value(index),
                        source_file: source_files.value(index),
                        source_row_number: source_rows.value(index),
                        payload_ref: store
                            .insert(required_arrow_string(json, index)?.as_bytes())?,
                    });
                }
            }
        }
        progress(ProgressEvent::determinate(
            ProgressPhase::EncodeFallbackSources,
            completed,
            fallback_total,
            WorkUnit::Contracts,
            EngineCounters {
                candidates: scanned,
                selected: selected_count,
                ..EngineCounters::default()
            },
        ));
    }
    if let Some(row) = selected {
        output.push(row);
        selected_count += 1;
    }
    spill.append_registration(&output, &[])?;
    completed = finish_ordered_group_count(current_contract, completed);
    progress(ProgressEvent::determinate(
        ProgressPhase::EncodeFallbackSources,
        completed,
        completed,
        WorkUnit::Contracts,
        EngineCounters {
            candidates: scanned,
            selected: selected_count,
            ..EngineCounters::default()
        },
    ));
    Ok(())
}

/// Stable candidate stream for the bounded fallback-contract filter table.
pub(super) fn fallback_contract_candidates_sql() -> &'static str {
    "SELECT fallback.contract_index::UINTEGER AS contract_index,
                rows.metadata_json,
                rows.source_file::UINTEGER AS source_file,
                rows.source_row_number::UBIGINT AS source_row_number
         FROM encode_fallback_contracts fallback
         JOIN analysis_contracts contracts
           ON contracts.metadata_contract_index = fallback.contract_index
         JOIN metadata_rows rows ON rows.contract_id = contracts.contract_id
         WHERE rows.metadata_eligible
         ORDER BY fallback.contract_index,
                  rows.token_id,
                  rows.source_file,
                  rows.source_row_number"
}

struct FallbackContractFilterTable<'connection> {
    conn: &'connection Connection,
}

impl<'connection> FallbackContractFilterTable<'connection> {
    fn create(
        conn: &'connection Connection,
        contract_indices: impl IntoIterator<Item = u32>,
    ) -> Result<Self, AnalysisError> {
        conn.execute_batch(
            "DROP TABLE IF EXISTS encode_fallback_contracts;
             CREATE TEMP TABLE encode_fallback_contracts(
                 contract_index UINTEGER PRIMARY KEY
             );",
        )?;
        let table = Self { conn };
        {
            let mut appender = conn.appender("encode_fallback_contracts")?;
            appender.append_rows(
                contract_indices
                    .into_iter()
                    .map(|contract_index| [contract_index]),
            )?;
            appender.flush()?;
        }
        Ok(table)
    }
}

impl Drop for FallbackContractFilterTable<'_> {
    fn drop(&mut self) {
        let _ = self
            .conn
            .execute_batch("DROP TABLE IF EXISTS encode_fallback_contracts");
    }
}

/// For every contract whose representative row had no retained prefilter
/// tokens, stream candidate rows in stable order and keep only the first JSON
/// that would have prefilter tokens. The cross-batch cursor skips later rows
/// after a hit; rejected JSON is never copied into the payload arena.
#[allow(clippy::too_many_arguments)]
fn resolve_pending_fallback_contracts(
    conn: &Connection,
    token_source_relation: &TokenSourceRelation,
    relation_resident_bytes: u64,
    store: &mut PayloadRegistrationStore,
    pending_contracts: &mut Vec<PendingContractSlot>,
    pending_fallbacks: &mut HashMap<u32, PendingFallbackContract>,
    pending_token_ids: &mut Vec<u32>,
    resident_admission: &mut EncodeResidentAdmission,
    registration_accounting: &mut EncodeRegistrationAccounting,
    mut columns_resident_bytes: u64,
    parse_pool: &rayon::ThreadPool,
    progress: &mut impl FnMut(ProgressEvent),
) -> Result<u64, AnalysisError> {
    let fallback_total = pending_fallbacks.len() as u64;
    let fallback_filter =
        FallbackContractFilterTable::create(conn, pending_fallbacks.keys().copied())?;
    progress(ProgressEvent::determinate(
        ProgressPhase::EncodeFallbackSources,
        0,
        fallback_total,
        WorkUnit::Contracts,
        EngineCounters::default(),
    ));
    let mut stmt = conn.prepare(fallback_contract_candidates_sql())?;
    let batches = stmt.stream_arrow(
        [],
        Arc::new(Schema::new(vec![
            Field::new("contract_index", DataType::UInt32, false),
            Field::new("metadata_json", DataType::Utf8, false),
            Field::new("source_file", DataType::UInt32, false),
            Field::new("source_row_number", DataType::UInt64, false),
        ])),
    )?;

    let mut current_contract: Option<u32> = None;
    let mut selected: Option<SelectedFallbackRow> = None;
    let mut completed_contracts = 0u64;
    let mut selected_count = 0u64;
    let mut scanned = 0u64;
    let mut reported_contracts = 0u64;
    let mut reported_candidates = 0u64;

    for batch in batches {
        let row_count = batch.num_rows();
        if row_count == 0 {
            continue;
        }
        let contract_indexes = required_arrow_column::<UInt32Array>(&batch, 0, "contract_index")?;
        let json_column = batch.column(1).as_ref();
        let source_files = required_arrow_column::<UInt32Array>(&batch, 2, "source_file")?;
        let source_rows = required_arrow_column::<UInt64Array>(&batch, 3, "source_row_number")?;

        for index in 0..row_count {
            if contract_indexes.is_null(index)
                || json_column.is_null(index)
                || source_files.is_null(index)
                || source_rows.is_null(index)
            {
                return Err(AnalysisError::InvalidData(
                    "metadata fallback row contains NULL".into(),
                ));
            }
        }
        let ranges = ordered_group_ranges(row_count, |index| contract_indexes.value(index));
        let already_selected = selected.as_ref().map(|row| row.contract_index);
        let selected_indexes = parse_pool.install(|| {
            first_usable_rows_by_ordered_group(&ranges, already_selected, |index| {
                Ok(metadata_has_prefilter_tokens(required_arrow_string(
                    json_column,
                    index,
                )?))
            })
        })?;

        for ((contract_index, range), selected_index) in ranges.iter().zip(selected_indexes) {
            if Some(*contract_index) != current_contract {
                if let Some(chosen) = selected.take() {
                    columns_resident_bytes = register_resolved_fallback_contract(
                        chosen,
                        token_source_relation,
                        relation_resident_bytes,
                        store,
                        pending_contracts,
                        pending_fallbacks,
                        pending_token_ids,
                        resident_admission,
                        registration_accounting,
                    )?;
                    selected_count = selected_count.saturating_add(1);
                }
                if current_contract.is_some() {
                    completed_contracts = completed_contracts.saturating_add(1);
                }
                current_contract = Some(*contract_index);
            }
            scanned = scanned.saturating_add(range.len() as u64);
            if selected.is_some() {
                emit_fallback_source_progress(
                    progress,
                    completed_contracts,
                    fallback_total,
                    scanned,
                    selected_count,
                    &mut reported_contracts,
                    &mut reported_candidates,
                );
                continue;
            }
            let Some(index) = selected_index else {
                emit_fallback_source_progress(
                    progress,
                    completed_contracts,
                    fallback_total,
                    scanned,
                    selected_count,
                    &mut reported_contracts,
                    &mut reported_candidates,
                );
                continue;
            };
            let json = required_arrow_string(json_column, index)?;
            let committed = columns_resident_bytes
                .checked_add(relation_resident_bytes)
                .ok_or_else(|| {
                    AnalysisError::InvalidData("Encode fallback relation admission overflow".into())
                })?;
            let growth = planned_encode_batch_growth(json.len() as u64, 1)?;
            resident_admission.reserve_growth(committed, growth)?;
            let payload_ref = store.insert(json.as_bytes())?;
            selected = Some(SelectedFallbackRow {
                contract_index: *contract_index,
                source_file: source_files.value(index),
                source_row_number: source_rows.value(index),
                payload_ref,
            });
            emit_fallback_source_progress(
                progress,
                completed_contracts,
                fallback_total,
                scanned,
                selected_count,
                &mut reported_contracts,
                &mut reported_candidates,
            );
        }
    }
    if let Some(chosen) = selected.take() {
        columns_resident_bytes = register_resolved_fallback_contract(
            chosen,
            token_source_relation,
            relation_resident_bytes,
            store,
            pending_contracts,
            pending_fallbacks,
            pending_token_ids,
            resident_admission,
            registration_accounting,
        )?;
        selected_count = selected_count.saturating_add(1);
    }
    completed_contracts = finish_ordered_group_count(current_contract, completed_contracts);
    progress(ProgressEvent::determinate(
        ProgressPhase::EncodeFallbackSources,
        completed_contracts,
        completed_contracts,
        WorkUnit::Contracts,
        EngineCounters {
            candidates: scanned,
            selected: selected_count,
            ..EngineCounters::default()
        },
    ));
    drop(stmt);
    drop(fallback_filter);
    Ok(columns_resident_bytes)
}

#[allow(clippy::too_many_arguments)]
fn emit_fallback_source_progress(
    progress: &mut impl FnMut(ProgressEvent),
    completed: u64,
    total: u64,
    candidates: u64,
    selected: u64,
    reported_completed: &mut u64,
    reported_candidates: &mut u64,
) {
    if completed.saturating_sub(*reported_completed) >= 16_384
        || candidates.saturating_sub(*reported_candidates) >= 16_384
    {
        progress(ProgressEvent::determinate(
            ProgressPhase::EncodeFallbackSources,
            completed,
            total,
            WorkUnit::Contracts,
            EngineCounters {
                candidates,
                selected,
                ..EngineCounters::default()
            },
        ));
        *reported_completed = completed;
        *reported_candidates = candidates;
    }
}

struct SelectedFallbackRow {
    contract_index: u32,
    source_file: u32,
    source_row_number: u64,
    payload_ref: PayloadHandle,
}

#[allow(clippy::too_many_arguments)]
fn register_resolved_fallback_contract(
    row: SelectedFallbackRow,
    token_source_relation: &TokenSourceRelation,
    relation_resident_bytes: u64,
    store: &PayloadRegistrationStore,
    pending_contracts: &mut Vec<PendingContractSlot>,
    pending_fallbacks: &mut HashMap<u32, PendingFallbackContract>,
    pending_token_ids: &mut Vec<u32>,
    resident_admission: &mut EncodeResidentAdmission,
    registration_accounting: &mut EncodeRegistrationAccounting,
) -> Result<u64, AnalysisError> {
    let Some(pending) = pending_fallbacks.remove(&row.contract_index) else {
        return registration_accounting.resident_bytes(
            pending_contracts,
            pending_fallbacks,
            store,
            pending_token_ids,
        );
    };
    let (representative_token_range, token_slots) = token_source_relation.append_contract_layout(
        pending.source_contract_index,
        SourceCoordinate {
            source_file: row.source_file,
            source_row_number: row.source_row_number,
        },
        pending_token_ids,
    )?;
    pending_contracts.push(PendingContractSlot {
        chain_id: pending.chain_id,
        weight: pending.weight,
        representative_payload_ref: row.payload_ref,
        representative_token_range,
        token_sources: token_slots,
    });
    let columns = registration_accounting.resident_bytes(
        pending_contracts,
        pending_fallbacks,
        store,
        pending_token_ids,
    )?;
    let committed = columns
        .checked_add(relation_resident_bytes)
        .ok_or_else(|| {
            AnalysisError::InvalidData("Encode fallback relation admission overflow".into())
        })?;
    resident_admission.commit(committed)?;
    Ok(columns)
}

fn build_encoded_contract(
    slot: PendingContractSlot,
    contract_id: u32,
    global_offsets: &[u32],
    token_cursor: &mut usize,
    sources: &mut EncodeSourceSoA,
    contracts: &mut EncodeContractSoA,
) -> Result<(), AnalysisError> {
    let representative_payload_id =
        global_payload_id(slot.representative_payload_ref, global_offsets)?;
    let source_doc_id = u32::try_from(sources.source_count())
        .map_err(|_| AnalysisError::InvalidData("metadata source count exceeds u32".into()))?;
    push_pending_source_header(
        sources,
        contract_id,
        representative_payload_id,
        slot.representative_token_range,
        token_cursor,
    )?;
    for source in slot.token_sources {
        push_pending_source_header(
            sources,
            contract_id,
            global_payload_id(source.payload_ref, global_offsets)?,
            source.token_range,
            token_cursor,
        )?;
    }
    contracts.push_contract(
        contract_id,
        slot.chain_id,
        source_doc_id,
        representative_payload_id,
        slot.weight,
    );
    Ok(())
}

fn planned_final_column_growth(
    source_count: usize,
    contract_count: usize,
) -> Result<u64, AnalysisError> {
    let sources = u64::try_from(source_count)
        .map_err(|_| AnalysisError::InvalidData("Encode source count exceeds u64".into()))?;
    let contracts = u64::try_from(contract_count)
        .map_err(|_| AnalysisError::InvalidData("Encode contract count exceeds u64".into()))?;
    sources
        .checked_mul(16)
        .and_then(|bytes| contracts.checked_mul(24)?.checked_add(bytes))
        .and_then(|bytes| bytes.checked_add(64 * 1024 * 1024))
        .ok_or_else(|| AnalysisError::InvalidData("Encode final column estimate overflow".into()))
}

fn planned_fallback_atom_growth(contract_count: usize) -> Result<u64, AnalysisError> {
    u64::try_from(contract_count)
        .ok()
        .and_then(|count| count.checked_mul(96))
        .and_then(|bytes| bytes.checked_add(64 * 1024 * 1024))
        .ok_or_else(|| AnalysisError::InvalidData("Encode fallback atom estimate overflow".into()))
}

fn planned_atom_sketch_growth(
    payloads: PayloadTermView<'_>,
    atom_payloads: &[u32],
) -> Result<u64, AnalysisError> {
    let atoms = atom_payloads.len() as u64;
    // Two simhash columns (16A), two offset columns (16(A+1)), at most
    // 32 anchors (128A), and two presence bytes (2A).
    let output = atoms
        .checked_mul(162)
        .and_then(|bytes| bytes.checked_add(16))
        .ok_or_else(|| AnalysisError::InvalidData("Encode atom sketch estimate overflow".into()))?;
    let template_df = payloads
        .template_terms
        .iter()
        .copied()
        .max()
        .map_or(0u64, |term| u64::from(term) + 1);
    let content_df = payloads
        .content_terms
        .iter()
        .copied()
        .max()
        .map_or(0u64, |term| u64::from(term) + 1);
    let df = template_df
        .max(content_df)
        .checked_mul(std::mem::size_of::<AtomicU32>() as u64)
        .ok_or_else(|| AnalysisError::InvalidData("Encode atom DF estimate overflow".into()))?;
    let batch = atoms
        .min(4_096)
        .checked_mul(std::mem::size_of::<metadata_engine::blocking::AtomDimensionSketch>() as u64)
        .ok_or_else(|| AnalysisError::InvalidData("Encode atom batch estimate overflow".into()))?;
    output
        .checked_add(df)
        .and_then(|bytes| bytes.checked_add(batch))
        .and_then(|bytes| bytes.checked_add(64 * 1024 * 1024))
        .ok_or_else(|| AnalysisError::InvalidData("Encode atom sketch estimate overflow".into()))
}

fn external_atom_sketch_storage_upper_bound(
    payloads: PayloadTermView<'_>,
    atom_payloads: &[u32],
) -> Result<u64, AnalysisError> {
    let mut term_occurrences = 0u64;
    for &payload_id in atom_payloads {
        let payload = payload_id as usize;
        term_occurrences = term_occurrences
            .checked_add(payloads.template_term_ids(payload).len() as u64)
            .and_then(|count| count.checked_add(payloads.content_term_ids(payload).len() as u64))
            .ok_or_else(|| {
                AnalysisError::InvalidData("Encode external atom term estimate overflow".into())
            })?;
    }
    let atoms = atom_payloads.len() as u64;
    // DuckDB owns the source + ordered DF tables concurrently at the sort
    // boundary.  64 bytes/term is conservative for three u32 keys, count,
    // vector/chunk metadata and temporary merge state.
    term_occurrences
        .checked_mul(64)
        .and_then(|bytes| atoms.checked_mul(162)?.checked_add(bytes))
        .and_then(|bytes| bytes.checked_add(128 * 1024 * 1024))
        .ok_or_else(|| {
            AnalysisError::InvalidData("Encode external atom storage estimate overflow".into())
        })
}

fn planned_blocking_resident_growth(atoms: AtomSketchView<'_>) -> Result<u64, AnalysisError> {
    let atom_count = atoms.len() as u64;
    let memberships = blocking_membership_count(atoms)?;
    // Resident compiler peak:
    // - final forward+inverse columns and descriptors: <= 32M + 40A
    // - inverse count/cursor/atomic construction: <= 16M + 24A
    // - at most sixteen joint-family maps or both anchor pair tables:
    //   <= 16M + 64A
    // The 128 MiB allowance covers BTree nodes, Vec headers, thread stacks and
    // typed-array persistence buffers.  M already includes all 64 joint
    // memberships and both anchor dimensions.
    memberships
        .checked_mul(64)
        .and_then(|bytes| atom_count.checked_mul(128)?.checked_add(bytes))
        .and_then(|bytes| bytes.checked_add(128 * 1024 * 1024))
        .ok_or_else(|| {
            AnalysisError::InvalidData("blocking resident compile estimate overflow".into())
        })
}

fn external_blocking_storage_upper_bound(atoms: AtomSketchView<'_>) -> Result<u64, AnalysisError> {
    let atom_count = atoms.len() as u64;
    let memberships = blocking_membership_count(atoms)?;
    // Raw membership + deduplicated membership can coexist at the GROUP BY
    // boundary; DuckDB merge runs and the final two CSR directions are covered
    // by 96 bytes/membership. Atom descriptors and typed-array framing use
    // another 64 bytes/atom plus fixed staging slack.
    memberships
        .checked_mul(96)
        .and_then(|bytes| atom_count.checked_mul(64)?.checked_add(bytes))
        .and_then(|bytes| bytes.checked_add(256 * 1024 * 1024))
        .ok_or_else(|| {
            AnalysisError::InvalidData("blocking external storage estimate overflow".into())
        })
}

fn build_disk_encoded_columns(
    work_directory: &Path,
    pending_contracts: Vec<PendingContractSlot>,
    pending_token_ids: Vec<u32>,
    global_offsets: &[u32],
    final_source_count: usize,
) -> Result<(EncodeSources, EncodeContracts), AnalysisError> {
    let directory = work_directory.join("artifacts/metadata").join(format!(
        "{ENCODE_COLUMN_SPILL_PREFIX}{}",
        metadata_engine::artifacts::new_artifact_run_id()
    ));
    fs::create_dir_all(&directory)?;
    let cleanup = Arc::new(SpillDirectoryCleanup {
        path: directory.clone(),
    });
    let source_count = u64::try_from(final_source_count)
        .map_err(|_| AnalysisError::InvalidData("Encode source count exceeds u64".into()))?;
    let contract_count = u64::try_from(pending_contracts.len())
        .map_err(|_| AnalysisError::InvalidData("Encode contract count exceeds u64".into()))?;
    let token_count = u64::try_from(pending_token_ids.len())
        .map_err(|_| AnalysisError::InvalidData("Encode token count exceeds u64".into()))?;
    let mut source_contracts = TypedArraySink::create(
        &directory.join("source_contract_ids.u32"),
        ArrayKind::U32,
        source_count,
    )
    .map_err(encode_err)?;
    let mut source_payloads = TypedArraySink::create(
        &directory.join("source_payload_ids.u32"),
        ArrayKind::U32,
        source_count,
    )
    .map_err(encode_err)?;
    let mut source_offsets = TypedArraySink::create(
        &directory.join("source_token_offsets.u64"),
        ArrayKind::U64,
        source_count + 1,
    )
    .map_err(encode_err)?;
    let mut source_tokens = TypedArraySink::create(
        &directory.join("source_token_ids.u32"),
        ArrayKind::U32,
        token_count,
    )
    .map_err(encode_err)?;
    let mut contract_ids = TypedArraySink::create(
        &directory.join("contract_ids.u32"),
        ArrayKind::U32,
        contract_count,
    )
    .map_err(encode_err)?;
    let mut chain_ids = TypedArraySink::create(
        &directory.join("contract_chain_ids.u32"),
        ArrayKind::U32,
        contract_count,
    )
    .map_err(encode_err)?;
    let mut source_doc_ids = TypedArraySink::create(
        &directory.join("contract_source_doc_ids.u32"),
        ArrayKind::U32,
        contract_count,
    )
    .map_err(encode_err)?;
    let mut contract_payloads = TypedArraySink::create(
        &directory.join("contract_payload_ids.u32"),
        ArrayKind::U32,
        contract_count,
    )
    .map_err(encode_err)?;
    let mut weights = TypedArraySink::create(
        &directory.join("contract_weights.u64"),
        ArrayKind::U64,
        contract_count,
    )
    .map_err(encode_err)?;
    source_offsets.push_u64(0).map_err(encode_err)?;
    for token in pending_token_ids {
        source_tokens.push_u32(token).map_err(encode_err)?;
    }
    let mut token_cursor = 0usize;
    let mut sources_written = 0usize;
    for (index, slot) in pending_contracts.into_iter().enumerate() {
        let contract_id = u32::try_from(index).map_err(|_| {
            AnalysisError::InvalidData("metadata contract count exceeds u32".into())
        })?;
        let representative_payload_id =
            global_payload_id(slot.representative_payload_ref, global_offsets)?;
        let source_doc_id = u32::try_from(sources_written)
            .map_err(|_| AnalysisError::InvalidData("metadata source count exceeds u32".into()))?;
        write_disk_pending_source(
            contract_id,
            representative_payload_id,
            slot.representative_token_range,
            token_count as usize,
            &mut token_cursor,
            &mut sources_written,
            &mut source_contracts,
            &mut source_payloads,
            &mut source_offsets,
        )?;
        for source in slot.token_sources {
            write_disk_pending_source(
                contract_id,
                global_payload_id(source.payload_ref, global_offsets)?,
                source.token_range,
                token_count as usize,
                &mut token_cursor,
                &mut sources_written,
                &mut source_contracts,
                &mut source_payloads,
                &mut source_offsets,
            )?;
        }
        contract_ids.push_u32(contract_id).map_err(encode_err)?;
        chain_ids.push_u32(slot.chain_id).map_err(encode_err)?;
        source_doc_ids.push_u32(source_doc_id).map_err(encode_err)?;
        contract_payloads
            .push_u32(representative_payload_id)
            .map_err(encode_err)?;
        weights.push_u64(slot.weight).map_err(encode_err)?;
    }
    if token_cursor != token_count as usize || sources_written != final_source_count {
        return Err(AnalysisError::InvalidData(format!(
            "disk Encode columns were not consumed exactly: tokens={token_cursor}/{token_count}, \
             sources={sources_written}/{final_source_count}"
        )));
    }
    source_contracts.finish().map_err(encode_err)?;
    source_payloads.finish().map_err(encode_err)?;
    source_offsets.finish().map_err(encode_err)?;
    source_tokens.finish().map_err(encode_err)?;
    contract_ids.finish().map_err(encode_err)?;
    chain_ids.finish().map_err(encode_err)?;
    source_doc_ids.finish().map_err(encode_err)?;
    contract_payloads.finish().map_err(encode_err)?;
    weights.finish().map_err(encode_err)?;
    let sources = DiskEncodeSources {
        contract_ids: map_u32_array(&directory.join("source_contract_ids.u32"))
            .map_err(encode_err)?,
        payload_ids: map_u32_array(&directory.join("source_payload_ids.u32"))
            .map_err(encode_err)?,
        token_offsets: map_u64_array(&directory.join("source_token_offsets.u64"))
            .map_err(encode_err)?,
        token_ids: map_u32_array(&directory.join("source_token_ids.u32")).map_err(encode_err)?,
        _cleanup: Arc::clone(&cleanup),
    };
    let contracts = DiskEncodeContracts {
        contract_ids: map_u32_array(&directory.join("contract_ids.u32")).map_err(encode_err)?,
        chain_ids: map_u32_array(&directory.join("contract_chain_ids.u32")).map_err(encode_err)?,
        source_doc_ids: map_u32_array(&directory.join("contract_source_doc_ids.u32"))
            .map_err(encode_err)?,
        payload_ids: map_u32_array(&directory.join("contract_payload_ids.u32"))
            .map_err(encode_err)?,
        weights: map_u64_array(&directory.join("contract_weights.u64")).map_err(encode_err)?,
        _cleanup: cleanup,
    };
    Ok((
        EncodeSources::Disk(Box::new(sources)),
        EncodeContracts::Disk(Box::new(contracts)),
    ))
}

#[allow(clippy::too_many_arguments)]
fn write_disk_pending_source(
    contract_id: u32,
    payload_id: u32,
    token_range: TokenRange,
    token_count: usize,
    token_cursor: &mut usize,
    sources_written: &mut usize,
    source_contracts: &mut TypedArraySink,
    source_payloads: &mut TypedArraySink,
    source_offsets: &mut TypedArraySink,
) -> Result<(), AnalysisError> {
    if token_range.start != *token_cursor
        || token_range.end < token_range.start
        || token_range.end > token_count
    {
        return Err(AnalysisError::InvalidData(format!(
            "pending token range is not contiguous or is out of bounds: cursor={}, range={}..{}, total={token_count}",
            *token_cursor, token_range.start, token_range.end
        )));
    }
    source_contracts.push_u32(contract_id).map_err(encode_err)?;
    source_payloads.push_u32(payload_id).map_err(encode_err)?;
    source_offsets
        .push_u64(token_range.end as u64)
        .map_err(encode_err)?;
    *token_cursor = token_range.end;
    *sources_written = sources_written
        .checked_add(1)
        .ok_or_else(|| AnalysisError::InvalidData("Encode disk source count overflow".into()))?;
    Ok(())
}

fn global_payload_id(
    payload_ref: PayloadHandle,
    global_offsets: &[u32],
) -> Result<u32, AnalysisError> {
    match payload_ref {
        PayloadHandle::Memory(payload_ref) | PayloadHandle::Spill(payload_ref) => {
            global_payload_ref_id(payload_ref, global_offsets)
        }
    }
}

fn push_pending_source_header(
    sources: &mut EncodeSourceSoA,
    contract_id: u32,
    payload_id: u32,
    token_range: TokenRange,
    token_cursor: &mut usize,
) -> Result<(), AnalysisError> {
    if token_range.start != *token_cursor
        || token_range.end < token_range.start
        || token_range.end > sources.token_ids.len()
    {
        return Err(AnalysisError::InvalidData(format!(
            "pending token range is not contiguous or is out of bounds: cursor={}, range={}..{}, total={}",
            *token_cursor,
            token_range.start,
            token_range.end,
            sources.token_ids.len()
        )));
    }
    sources.contract_ids.push(contract_id);
    sources.payload_ids.push(payload_id);
    sources.token_offsets.push(
        u64::try_from(token_range.end)
            .map_err(|_| AnalysisError::InvalidData("metadata token offset exceeds u64".into()))?,
    );
    *token_cursor = token_range.end;
    Ok(())
}

fn payload_feature_identity_ids(payloads: PayloadTermView<'_>) -> Vec<u32> {
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

/// Lower-overhead exact identity construction for the disk-backed term path.
/// The resident fast path uses an O(P) hash table; this fallback keeps only a
/// packed `(hash, payload_id)` vector plus the final identities, parallel-sorts
/// by hash and full canonical payload columns, then labels equal neighbours.
fn payload_feature_identity_ids_sorted(payloads: PayloadTermView<'_>) -> Vec<u32> {
    let mut payload_order = (0..payloads.payload_count())
        .into_par_iter()
        .map(|payload_index| {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            payloads.hash_payload(payload_index, &mut hasher);
            (hasher.finish(), payload_index as u32)
        })
        .collect::<Vec<_>>();
    payload_order.par_sort_unstable_by(|&(left_hash, left), &(right_hash, right)| {
        left_hash
            .cmp(&right_hash)
            .then_with(|| compare_payload_features(payloads, left as usize, right as usize))
    });
    let mut identities = vec![0u32; payloads.payload_count()];
    let mut next_identity = 0u32;
    let mut previous = None::<u32>;
    for &(_, payload_id) in &payload_order {
        let same = previous.is_some_and(|previous_id| {
            payloads.payload_eq(previous_id as usize, payload_id as usize)
        });
        if !same {
            next_identity = next_identity.saturating_add(u32::from(previous.is_some()));
        }
        identities[payload_id as usize] = next_identity;
        previous = Some(payload_id);
    }
    identities
}

fn compare_payload_features(
    payloads: PayloadTermView<'_>,
    left: usize,
    right: usize,
) -> std::cmp::Ordering {
    payloads
        .template_term_ids(left)
        .cmp(payloads.template_term_ids(right))
        .then_with(|| {
            payloads
                .template_freqs(left)
                .cmp(payloads.template_freqs(right))
        })
        .then_with(|| {
            payloads
                .content_term_ids(left)
                .cmp(payloads.content_term_ids(right))
        })
        .then_with(|| {
            payloads
                .content_freqs(left)
                .cmp(payloads.content_freqs(right))
        })
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
                coalesce(totals.contract_count, 0)::BIGINT,
                coalesce(totals.nft_count, 0)::BIGINT
         FROM selected_chains selected
         LEFT JOIN chain_totals totals ON totals.chain = selected.chain
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

fn build_retained_token_source_relation<'connection>(
    conn: &'connection Connection,
    contract_count: u32,
    store: &mut PayloadRegistrationStore,
    parse_pool: &rayon::ThreadPool,
    external: bool,
    progress: &mut impl FnMut(ProgressEvent),
) -> Result<TokenSourceCatalog<'connection>, AnalysisError> {
    let external_spill = external
        .then(|| ExternalRegistrationSpill::create(conn))
        .transpose()?;
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
        return Ok(if let Some(spill) = external_spill {
            TokenSourceCatalog::External(spill)
        } else {
            TokenSourceCatalog::Memory(TokenSourceRelation {
                sources: Vec::new(),
                memberships: Vec::new(),
                contract_offsets: vec![0; contract_count as usize + 1],
                logical_bytes: 0,
            })
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
    let batches = statement.stream_arrow(
        [],
        Arc::new(Schema::new(vec![
            Field::new("contract_index", DataType::UInt32, false),
            Field::new("token_index", DataType::UInt32, false),
            Field::new("source_file", DataType::UInt32, false),
            Field::new("source_row_number", DataType::UInt64, false),
            Field::new("metadata_json", DataType::Utf8, false),
        ])),
    )?;
    let mut selected = Vec::<SelectedTokenSource>::new();
    let mut selected_count = 0u64;
    let mut current_group = None;
    let mut group_selected = false;
    let mut completed_groups = 0u64;
    progress(ProgressEvent::indeterminate(
        ProgressPhase::EncodeTokenSources,
        0,
        WorkUnit::TokenGroups,
        EngineCounters::default(),
    ));
    for batch in batches {
        let contracts = required_arrow_column::<UInt32Array>(&batch, 0, "contract_index")?;
        let tokens = required_arrow_column::<UInt32Array>(&batch, 1, "token_index")?;
        let source_files = required_arrow_column::<UInt32Array>(&batch, 2, "source_file")?;
        let source_rows = required_arrow_column::<UInt64Array>(&batch, 3, "source_row_number")?;
        let json = batch.column(4).as_ref();
        for index in 0..batch.num_rows() {
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
        }
        let ranges = ordered_group_ranges(batch.num_rows(), |index| {
            (contracts.value(index), tokens.value(index))
        });
        let already_selected = if group_selected { current_group } else { None };
        let selected_indexes = parse_pool.install(|| {
            first_usable_rows_by_ordered_group(&ranges, already_selected, |index| {
                Ok(metadata_has_prefilter_tokens(required_arrow_string(
                    json, index,
                )?))
            })
        })?;
        let mut selected_batch = Vec::new();
        for ((group, _range), selected_index) in ranges.iter().zip(selected_indexes) {
            if observe_ordered_group(*group, &mut current_group, &mut completed_groups) {
                group_selected = false;
            }
            if group_selected {
                continue;
            }
            let Some(index) = selected_index else {
                continue;
            };
            let payload_ref = store.insert(required_arrow_string(json, index)?.as_bytes())?;
            let selected_row = SelectedTokenSource {
                contract_index: group.0,
                token_index: group.1,
                coordinate: SourceCoordinate {
                    source_file: source_files.value(index),
                    source_row_number: source_rows.value(index),
                },
                payload_ref,
            };
            if external_spill.is_some() {
                selected_batch.push(selected_row);
            } else {
                selected.push(selected_row);
            }
            selected_count = selected_count.saturating_add(1);
            group_selected = true;
        }
        if let Some(spill) = external_spill.as_ref() {
            spill.append_selected(&selected_batch)?;
        }
        progress(ProgressEvent::indeterminate(
            ProgressPhase::EncodeTokenSources,
            completed_groups,
            WorkUnit::TokenGroups,
            EngineCounters {
                selected: selected_count,
                ..EngineCounters::default()
            },
        ));
    }
    completed_groups = finish_ordered_group_count(current_group, completed_groups);
    progress(ProgressEvent::determinate(
        ProgressPhase::EncodeTokenSources,
        completed_groups,
        completed_groups,
        WorkUnit::TokenGroups,
        EngineCounters {
            selected: selected_count,
            ..EngineCounters::default()
        },
    ));
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

    progress(ProgressEvent::determinate(
        ProgressPhase::EncodeResolveTokenMemberships,
        0,
        selected_count,
        WorkUnit::Items,
        EngineCounters::default(),
    ));
    if let Some(spill) = external_spill {
        progress(ProgressEvent::determinate(
            ProgressPhase::EncodeLoadTokenSources,
            selected_count,
            selected_count,
            WorkUnit::Items,
            EngineCounters::default(),
        ));
        for phase in [
            ProgressPhase::EncodeResolveTokenMemberships,
            ProgressPhase::EncodeLoadTokenMemberships,
            ProgressPhase::EncodeSortTokenMemberships,
        ] {
            progress(ProgressEvent::determinate(
                phase,
                selected_count,
                selected_count,
                WorkUnit::Items,
                EngineCounters::default(),
            ));
        }
        return Ok(TokenSourceCatalog::External(spill));
    }

    parse_pool.install(|| {
        selected.par_sort_unstable_by_key(|row| row.coordinate);
    });
    let mut sources = Vec::<TokenSourceRecord>::new();
    let mut memberships = Vec::<ResolvedTokenMembership>::with_capacity(selected.len());
    let mut current_coordinate = None;
    let mut current_source_id = 0u32;
    for row in selected {
        if current_coordinate != Some(row.coordinate) {
            current_source_id = u32::try_from(sources.len()).map_err(|_| {
                AnalysisError::InvalidData(
                    "token source dictionary exceeds u32 identity space".into(),
                )
            })?;
            sources.push(TokenSourceRecord {
                source_file: row.coordinate.source_file,
                source_row_number: row.coordinate.source_row_number,
                payload_ref: row.payload_ref,
            });
            current_coordinate = Some(row.coordinate);
        } else if sources[current_source_id as usize].payload_ref != row.payload_ref {
            return Err(AnalysisError::InvalidData(
                "one token source coordinate resolved to multiple payloads".into(),
            ));
        }
        memberships.push(ResolvedTokenMembership {
            contract_index: row.contract_index,
            token_id: row.token_index,
            source_id: current_source_id,
        });
    }
    let source_count = u32::try_from(sources.len()).map_err(|_| {
        AnalysisError::InvalidData("token source dictionary exceeds u32 identity space".into())
    })?;
    progress(ProgressEvent::determinate(
        ProgressPhase::EncodeLoadTokenSources,
        sources.len() as u64,
        sources.len() as u64,
        WorkUnit::Items,
        EngineCounters::default(),
    ));
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
    progress(ProgressEvent::determinate(
        ProgressPhase::EncodeSortTokenMemberships,
        0,
        membership_count,
        WorkUnit::Items,
        EngineCounters::default(),
    ));
    parse_pool.install(|| memberships.par_sort_unstable());
    validate_token_memberships(&memberships, contract_count, source_count)?;
    progress(ProgressEvent::determinate(
        ProgressPhase::EncodeSortTokenMemberships,
        membership_count,
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
    Ok(TokenSourceCatalog::Memory(TokenSourceRelation {
        sources,
        memberships,
        contract_offsets,
        logical_bytes,
    }))
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

fn build_fallback_atoms_hash_sharded(
    contracts: EncodeContractView<'_>,
    payload_feature_identity: &[u32],
    shard_count: usize,
    pool: &rayon::ThreadPool,
    mut on_progress: impl FnMut(u64),
) -> Result<FallbackAtomCsr, AnalysisError> {
    let shard_count = shard_count.next_power_of_two().max(1);
    let shard_mask = shard_count - 1;
    let shards = (0..shard_count)
        .map(|_| Mutex::new(HashMap::<(u32, u32), (u32, Vec<u32>)>::new()))
        .collect::<Vec<_>>();
    pool.install(|| {
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
            })
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
    let conservative_resident_floor = raw_bytes
        .checked_mul(16)
        .and_then(|bytes| bytes.checked_add(source_rows.checked_mul(2_048)?))
        .and_then(|bytes| bytes.checked_add(token_rows.checked_mul(32)?))
        .and_then(|bytes| bytes.checked_add(64 * 1024 * 1024))
        .ok_or_else(|| AnalysisError::InvalidData("Encode storage estimate overflow".into()))?;
    let token_relation_peak_bytes =
        planned_token_relation_peak(token_rows, source_rows, token_json_bytes)?;
    let payload_spill_upper_bound_bytes = raw_bytes
        .checked_add(token_json_bytes)
        .and_then(|bytes| {
            source_rows
                .checked_add(token_rows)?
                .checked_mul(64)?
                .checked_add(bytes)
        })
        .and_then(|bytes| bytes.checked_add(DEFAULT_MAX_PACK_BYTES))
        .ok_or_else(|| {
            AnalysisError::InvalidData("Encode payload spill estimate overflow".into())
        })?;
    let partial_peak_bytes = ENCODE_RESIDENT_FIXED_BYTES;
    let modeled_resident_peak = raw_bytes
        .checked_add(token_json_bytes)
        .and_then(|bytes| bytes.checked_mul(64))
        .and_then(|bytes| bytes.checked_add(source_rows.checked_mul(2_048)?))
        .and_then(|bytes| bytes.checked_add(token_rows.checked_mul(24)?))
        .and_then(|bytes| bytes.checked_add(64 * 1024 * 1024))
        .ok_or_else(|| AnalysisError::InvalidData("Encode memory estimate overflow".into()))?;
    let payload_index_upper = token_rows
        .checked_add(source_rows.saturating_mul(2))
        .map(|count| count.min(u64::from(u32::MAX)))
        .and_then(|count| count.checked_mul(288))
        .and_then(|bytes| bytes.checked_add(64 * 1024 * 1024))
        .ok_or_else(|| {
            AnalysisError::InvalidData("Encode payload index estimate overflow".into())
        })?;
    // The global payload/interner/CSR state grows with all unique small
    // documents, not just the largest contract. Use the complete conservative
    // durable envelope as the global resident admission floor; this avoids a
    // second JSON preflight while covering high-cardinality payload/term maps.
    let resident_peak_bytes = modeled_resident_peak
        .max(conservative_resident_floor)
        .max(token_relation_peak_bytes)
        .max(payload_index_upper);
    Ok(EncodeAdmissionEstimate {
        resident_peak_bytes,
        partial_peak_bytes,
        token_relation_peak_bytes,
        payload_spill_upper_bound_bytes,
        representative_rows: source_rows,
        token_rows,
    })
}

fn blocking_contract_expansion_pair_work(
    blocking: &metadata_engine::blocking::BlockingBundle,
    fallback_atoms: FallbackAtomView<'_>,
) -> Result<u64, AnalysisError> {
    blocking_contract_expansion_pair_work_view(
        &blocking.block_atom_offsets,
        &blocking.block_atoms,
        fallback_atoms,
    )
}

fn blocking_contract_expansion_pair_work_files(
    blocking_directory: &Path,
    fallback_atoms: FallbackAtomView<'_>,
) -> Result<u64, AnalysisError> {
    let offsets =
        map_u64_array(&blocking_directory.join("block_atom_offsets.u64")).map_err(encode_err)?;
    let atoms = map_u32_array(&blocking_directory.join("block_atoms.u32")).map_err(encode_err)?;
    blocking_contract_expansion_pair_work_view(&offsets, &atoms, fallback_atoms)
}

fn blocking_contract_expansion_pair_work_view(
    block_atom_offsets: &[u64],
    block_atoms: &[u32],
    fallback_atoms: FallbackAtomView<'_>,
) -> Result<u64, AnalysisError> {
    let mut total = 0u64;
    for block in 0..block_atom_offsets.len().saturating_sub(1) {
        let begin = block_atom_offsets[block] as usize;
        let end = block_atom_offsets[block + 1] as usize;
        let mut prefix = 0u64;
        for &atom in &block_atoms[begin..end] {
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

fn reserve_storage_advisory(
    broker: &mut StorageBroker,
    class: ArtifactClass,
    final_bytes: u64,
    partial_peak_bytes: u64,
    _label: &str,
    _warn: impl FnOnce(String),
) -> Result<Option<StorageLease>, AnalysisError> {
    broker
        .reserve(class, final_bytes, partial_peak_bytes)
        .map(Some)
        .map_err(storage_err)
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
    use std::fs;

    use super::{
        build_disk_encoded_columns, build_encoded_contract, build_fallback_atoms_hash_sharded,
        intern_payload_with_parser, payload_feature_identity_ids,
        payload_feature_identity_ids_sorted, payload_finalize_admission_error,
        payload_storage_mode, planned_encoded_contract_growth, planned_token_relation_peak,
        reserve_storage_advisory, DuckDbAtomSketchSpill, DuckDbAtomSpill, DuckDbBlockingSpill,
        DuckDbCsrSpill, DuckDbPayloadTermSpill, EncodeAdmissionEstimate, EncodePayloadRow,
        EncodeRegistrationAccounting, EncodeResidentAccounting, EncodeResidentAdmission,
        ExternalPayloadCasWriter, ExternalRegistrationSpill, ExternalResolvedContract,
        FallbackContractFilterTable, PayloadHandle, PayloadRegistrationStore, PayloadStorageMode,
        PayloadTermInterner, PendingContractSlot, PendingSourceSlot, ResolvedTokenMembership,
        SelectedTokenSource, ShardedPayloadTermInterner, SourceCoordinate, TokenRange,
        TokenSourceInput, TokenSourceRecord, TokenSourceRelation, ENCODE_RESIDENT_FIXED_BYTES,
    };
    use duckdb::Connection;
    use metadata_engine::encode::csr::build_bidirectional_csr_from_iter;
    use metadata_engine::encode::{
        parse_metadata_documents, EncodeContractRow, EncodeContractSoA, EncodeSourceRow,
        PayloadArena, PayloadRef, PayloadTermSoA, ShardedPayloadArena,
    };
    use metadata_engine::resource::{MemoryBroker, MemoryError, GIB};
    use metadata_engine::storage::{ArtifactClass, StorageBroker};

    #[test]
    fn fallback_contract_filter_table_owns_rows_and_cleans_up_on_drop() {
        let conn = Connection::open_in_memory().unwrap();
        {
            let _table = FallbackContractFilterTable::create(&conn, [7u32, 3u32]).unwrap();
            let rows: u64 = conn
                .query_row(
                    "SELECT count(*) FROM encode_fallback_contracts",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(rows, 2);
        }
        let exists: bool = conn
            .query_row(
                "SELECT count(*) > 0 FROM duckdb_tables() WHERE table_name = 'encode_fallback_contracts'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!exists);
    }

    #[test]
    fn fallback_contract_filter_table_propagates_flush_errors_and_cleans_up() {
        let conn = Connection::open_in_memory().unwrap();

        let result = FallbackContractFilterTable::create(&conn, [7u32, 7u32]);

        assert!(result.is_err());
        let exists: bool = conn
            .query_row(
                "SELECT count(*) > 0 FROM duckdb_tables() WHERE table_name = 'encode_fallback_contracts'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!exists);
    }

    #[test]
    fn duckdb_term_spill_matches_single_shard_resident_dictionary_and_cleans_up() {
        let conn = Connection::open_in_memory().unwrap();
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .build()
            .unwrap();
        let json = [
            r#"{"name":"alpha beta","description":"red red blue"}"#,
            r#"{"name":"beta gamma","description":"blue green"}"#,
            r#"{"name":"alpha","description":""}"#,
        ];
        let interner = ShardedPayloadTermInterner::with_shard_count(1);
        let resident_lists = pool
            .install(|| {
                interner.intern_batch(
                    json.iter()
                        .map(|value| parse_metadata_documents(value))
                        .collect(),
                )
            })
            .unwrap();
        let resident = PayloadTermSoA::from_term_lists_owned(resident_lists).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let spill = DuckDbPayloadTermSpill::create(&conn).unwrap();
        spill
            .append_batch(
                0,
                json[..2]
                    .iter()
                    .map(|value| parse_metadata_documents(value))
                    .collect(),
                &pool,
            )
            .unwrap();
        spill
            .append_batch(
                2,
                json[2..]
                    .iter()
                    .map(|value| parse_metadata_documents(value))
                    .collect(),
                &pool,
            )
            .unwrap();
        let mapped = spill.materialize(directory.path(), json.len()).unwrap();
        let mapped_view = mapped.view();
        let resident_view = resident.as_view();

        assert_eq!(mapped_view.template_offsets, resident_view.template_offsets);
        assert_eq!(mapped_view.template_terms, resident_view.template_terms);
        assert_eq!(mapped_view.template_freqs, resident_view.template_freqs);
        assert_eq!(mapped_view.content_offsets, resident_view.content_offsets);
        assert_eq!(mapped_view.content_terms, resident_view.content_terms);
        assert_eq!(mapped_view.content_freqs, resident_view.content_freqs);

        drop(mapped);
        drop(spill);
        let staging_root = directory.path().join("artifacts/metadata");
        assert!(staging_root
            .read_dir()
            .map(|mut entries| entries.next().is_none())
            .unwrap_or(true));
    }

    #[test]
    fn duckdb_csr_spill_matches_resident_builder_and_cleans_up() {
        let conn = Connection::open_in_memory().unwrap();
        let sources = metadata_engine::encode::EncodeSourceSoA::from_rows(&[
            EncodeSourceRow {
                contract_id: 0,
                payload_id: 0,
                retained_token_ids: vec![3, 1, 3],
            },
            EncodeSourceRow {
                contract_id: 0,
                payload_id: 1,
                retained_token_ids: vec![2, 3],
            },
            EncodeSourceRow {
                contract_id: 1,
                payload_id: 2,
                retained_token_ids: vec![1, 2],
            },
        ])
        .unwrap();
        let resident =
            build_bidirectional_csr_from_iter((0..sources.source_count()).map(|source| {
                (
                    source as u32,
                    sources.contract_ids[source],
                    sources.tokens_of(source),
                )
            }))
            .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let spill = DuckDbCsrSpill::create(&conn).unwrap();
        let mapped = spill.build(directory.path(), sources.as_view(), 2).unwrap();
        let mapped = mapped.view();
        let resident = resident.as_view();

        assert_eq!(
            mapped.contract_token_offsets,
            resident.contract_token_offsets
        );
        assert_eq!(mapped.contract_tokens, resident.contract_tokens);
        assert_eq!(mapped.token_member_offsets, resident.token_member_offsets);
        assert_eq!(
            mapped.token_member_contracts,
            resident.token_member_contracts
        );
        assert_eq!(mapped.token_member_sources, resident.token_member_sources);

        drop(spill);
        let exists: bool = conn
            .query_row(
                "SELECT count(*) > 0 FROM duckdb_tables()
                 WHERE table_name = 'encode_csr_membership_spill'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!exists);
    }

    #[test]
    fn external_registration_builds_final_columns_without_resident_pending_state() {
        let conn = Connection::open_in_memory().unwrap();
        let spill = ExternalRegistrationSpill::create(&conn).unwrap();
        let payload = |local_id| {
            PayloadHandle::Spill(PayloadRef {
                shard_id: 0,
                local_id,
            })
        };
        spill
            .append_selected(&[
                SelectedTokenSource {
                    contract_index: 0,
                    token_index: 7,
                    coordinate: SourceCoordinate {
                        source_file: 1,
                        source_row_number: 30,
                    },
                    payload_ref: payload(2),
                },
                SelectedTokenSource {
                    contract_index: 0,
                    token_index: 4,
                    coordinate: SourceCoordinate {
                        source_file: 1,
                        source_row_number: 20,
                    },
                    payload_ref: payload(1),
                },
                SelectedTokenSource {
                    contract_index: 0,
                    token_index: 1,
                    coordinate: SourceCoordinate {
                        source_file: 1,
                        source_row_number: 10,
                    },
                    payload_ref: payload(0),
                },
                SelectedTokenSource {
                    contract_index: 0,
                    token_index: 8,
                    coordinate: SourceCoordinate {
                        source_file: 1,
                        source_row_number: 30,
                    },
                    payload_ref: payload(2),
                },
                SelectedTokenSource {
                    contract_index: 0,
                    token_index: 2,
                    coordinate: SourceCoordinate {
                        source_file: 1,
                        source_row_number: 10,
                    },
                    payload_ref: payload(0),
                },
            ])
            .unwrap();
        spill
            .append_registration(
                &[ExternalResolvedContract {
                    contract_index: 0,
                    chain_id: 9,
                    weight: 11,
                    source_file: 1,
                    source_row_number: 20,
                    payload_ref: payload(1),
                }],
                &[],
            )
            .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let (sources, contracts) = spill.build_columns(directory.path(), &[100, 103]).unwrap();
        let sources = sources.view();
        let contracts = contracts.view();

        assert_eq!(sources.contract_ids, [0, 0, 0]);
        assert_eq!(sources.payload_ids, [101, 100, 102]);
        assert_eq!(sources.token_offsets, [0, 1, 3, 5]);
        assert_eq!(sources.token_ids, [4, 1, 2, 7, 8]);
        assert_eq!(contracts.contract_ids, [0]);
        assert_eq!(contracts.chain_ids, [9]);
        assert_eq!(contracts.source_doc_ids, [0]);
        assert_eq!(contracts.payload_ids, [101]);
        assert_eq!(contracts.weights, [11]);
    }

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

        assert_eq!(payload_feature_identity_ids(soa.as_view()), vec![0, 0, 1]);
        assert_eq!(
            payload_feature_identity_ids_sorted(soa.as_view()),
            vec![0, 0, 1]
        );
    }

    #[test]
    fn fallback_atom_members_are_canonicalized_before_persist() {
        let mut contracts = EncodeContractSoA::with_contract_capacity(4);
        for contract_id in [3, 2, 1, 0] {
            contracts.push_contract(contract_id, 0, contract_id, 0, 1);
        }

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .build()
            .unwrap();
        let atoms =
            build_fallback_atoms_hash_sharded(contracts.as_view(), &[0], 1, &pool, |_| {}).unwrap();

        assert_eq!(atoms.atom_count(), 1);
        assert_eq!(atoms.members_of(0), &[0, 1, 2, 3]);
    }

    #[test]
    fn duckdb_fallback_atom_spill_matches_resident_grouping() {
        let mut contracts = EncodeContractSoA::with_contract_capacity(5);
        for (contract_id, chain_id, payload_id) in
            [(0, 1, 0), (1, 1, 1), (2, 1, 2), (3, 2, 0), (4, 2, 2)]
        {
            contracts.push_contract(contract_id, chain_id, contract_id, payload_id, 1);
        }
        let identities = [7, 7, 9];
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .build()
            .unwrap();
        let resident =
            build_fallback_atoms_hash_sharded(contracts.as_view(), &identities, 2, &pool, |_| {})
                .unwrap();
        let conn = Connection::open_in_memory().unwrap();
        let spill = DuckDbAtomSpill::create(&conn).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let disk = spill
            .build(directory.path(), contracts.as_view(), &identities, 2)
            .unwrap();

        let canonical = |atoms: metadata_engine::encode::FallbackAtomView<'_>| {
            let mut groups = (0..atoms.atom_count())
                .map(|atom| {
                    let members = atoms.members_of(atom).to_vec();
                    let first = members[0] as usize;
                    let payload = atoms.atom_payloads[atom] as usize;
                    (contracts.chain_ids[first], identities[payload], members)
                })
                .collect::<Vec<_>>();
            groups.sort_unstable();
            groups
        };
        assert_eq!(canonical(resident.as_view()), canonical(disk.view()));
    }

    #[test]
    fn duckdb_atom_sketch_spill_matches_compact_resident_builder_and_cleans_up() {
        let payloads = PayloadTermSoA::from_term_lists_owned(vec![
            (vec![(0, 1), (3, 2), (7, 1)], vec![(1, 1), (2, 1)]),
            (vec![(0, 1), (4, 1)], vec![(2, 1), (8, 1), (9, 1)]),
            (vec![], vec![(1, 1), (9, 1)]),
        ])
        .unwrap();
        let atom_payloads = [0u32, 1, 2, 0];
        let resident =
            metadata_engine::blocking::build_base_equivalent_atom_sketch_soa_from_view_parallel(
                payloads.as_view(),
                &atom_payloads,
                2,
            );
        let conn = Connection::open_in_memory().unwrap();
        let spill = DuckDbAtomSketchSpill::create(&conn).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let disk = spill
            .build(directory.path(), payloads.as_view(), &atom_payloads, 3)
            .unwrap();
        let resident = resident.as_view();
        let disk_view = disk.view();

        assert_eq!(disk_view.template_simhashes, resident.template_simhashes);
        assert_eq!(disk_view.content_simhashes, resident.content_simhashes);
        assert_eq!(
            disk_view.template_anchor_offsets,
            resident.template_anchor_offsets
        );
        assert_eq!(disk_view.template_anchors, resident.template_anchors);
        assert_eq!(
            disk_view.content_anchor_offsets,
            resident.content_anchor_offsets
        );
        assert_eq!(disk_view.content_anchors, resident.content_anchors);
        assert_eq!(disk_view.has_template_terms, resident.has_template_terms);
        assert_eq!(disk_view.has_content_terms, resident.has_content_terms);

        drop(disk);
        drop(spill);
        let exists: bool = conn
            .query_row(
                "SELECT count(*) > 0
                 FROM duckdb_tables()
                 WHERE table_name IN (
                     'encode_atom_term_spill',
                     'encode_atom_term_df_spill'
                 )",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!exists);
        let staging_root = directory.path().join("artifacts/metadata");
        assert!(staging_root
            .read_dir()
            .map(|mut entries| entries.next().is_none())
            .unwrap_or(true));
    }

    #[test]
    fn duckdb_blocking_spill_matches_resident_bundle_and_cleans_up() {
        let payloads = PayloadTermSoA::from_term_lists_owned(vec![
            (vec![(0, 1), (3, 1)], vec![(1, 1), (2, 1)]),
            (vec![(0, 1), (4, 1)], vec![(1, 1), (8, 1)]),
            (vec![(5, 1)], vec![(9, 1)]),
            (vec![], vec![]),
        ])
        .unwrap();
        let atom_payloads = [0u32, 1, 2, 3];
        let atoms =
            metadata_engine::blocking::build_base_equivalent_atom_sketch_soa_from_view_parallel(
                payloads.as_view(),
                &atom_payloads,
                2,
            );
        let config = metadata_engine::blocking::BlockingCompileConfig {
            max_routing_block_members: 2,
        };
        let directory = tempfile::tempdir().unwrap();
        let resident_dir = directory.path().join("resident");
        let external_dir = directory.path().join("external");
        let resident =
            metadata_engine::blocking::compile_base_equivalent_view_parallel_with_progress(
                atoms.as_view(),
                &config,
                &resident_dir,
                2,
                |_| {},
            )
            .unwrap();
        let conn = Connection::open_in_memory().unwrap();
        let spill = DuckDbBlockingSpill::create(&conn).unwrap();
        let external = spill
            .compile(atoms.as_view(), &config, &external_dir, 5, 2, |_| {})
            .unwrap();

        for file in [
            "atom_primary_storage_shard.u32",
            "atom_routing_status.u8",
            "atom_block_offsets.u64",
            "atom_block_ids.u32",
            "block_atom_offsets.u64",
            "block_atoms.u32",
            "block_kinds.u32",
            "block_keys.u64",
            "atom_template_simhash.u64",
            "atom_content_simhash.u64",
        ] {
            assert_eq!(
                fs::read(resident_dir.join(file)).unwrap(),
                fs::read(external_dir.join(file)).unwrap(),
                "{file}"
            );
        }
        assert_eq!(
            fs::read(resident_dir.join("hot_block_plans.bin")).unwrap(),
            fs::read(external_dir.join("hot_block_plans.bin")).unwrap()
        );
        assert_eq!(external.block_stats, resident.block_stats);
        assert_eq!(
            external.routing_membership_count,
            resident.block_atoms.len() as u64
        );
        drop(spill);
        let exists: bool = conn
            .query_row(
                "SELECT count(*) > 0
                 FROM duckdb_tables()
                 WHERE table_name IN (
                     'encode_block_membership_spill',
                     'encode_block_membership_distinct',
                     'encode_block_descriptor_spill'
                 )",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!exists);
    }

    #[test]
    fn external_payload_index_is_bounded_exact_and_collision_safe() {
        let directory = tempfile::tempdir().unwrap();
        let mut writer =
            ExternalPayloadCasWriter::create(directory.path(), 8, 4, 2, 16 * 1024 * 1024, 1)
                .unwrap();
        let collision = [7u8; 32];
        let alpha = writer.insert_with_digest(b"alpha", collision).unwrap();
        let beta = writer.insert_with_digest(b"beta", collision).unwrap();
        let alpha_again = writer.insert_with_digest(b"alpha", collision).unwrap();
        let gamma = writer.insert(b"gamma").unwrap();

        assert_eq!(alpha_again, alpha);
        assert_ne!(beta, alpha);
        assert!(writer.resident_bytes().unwrap() < 1024 * 1024);
        let (index, global_offsets, payload_count) = writer.finish().unwrap();
        assert_eq!(payload_count, 3);
        let mut expected = vec![
            (
                super::global_payload_ref_id(alpha, &global_offsets).unwrap(),
                b"alpha".to_vec(),
            ),
            (
                super::global_payload_ref_id(beta, &global_offsets).unwrap(),
                b"beta".to_vec(),
            ),
            (
                super::global_payload_ref_id(gamma, &global_offsets).unwrap(),
                b"gamma".to_vec(),
            ),
        ];
        expected.sort_unstable_by_key(|(global_id, _)| *global_id);
        let ids = expected.iter().map(|(id, _)| *id).collect::<Vec<_>>();
        let bodies = index.read_payload_ids(&ids).unwrap();
        assert_eq!(
            bodies,
            expected
                .into_iter()
                .map(|(_, bytes)| bytes)
                .collect::<Vec<_>>()
        );
        let index_conn = Connection::open(directory.path().join("payload-index.duckdb")).unwrap();
        let exists: bool = index_conn
            .query_row(
                "SELECT count(*) > 0
                 FROM duckdb_tables()
                 WHERE table_name IN (
                     'encode_external_payload_index',
                     'encode_external_payload_shards'
                 )",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!exists);
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
    fn conservative_encode_preflight_reservation_shrinks_to_measured_live_state() {
        let broker = MemoryBroker::new(4 * GIB, 3 * GIB).unwrap();
        let lease = broker.reserve(2 * GIB).unwrap();
        let mut admission = EncodeResidentAdmission::new(lease, ENCODE_RESIDENT_FIXED_BYTES);

        admission.commit(128 * 1024 * 1024).unwrap();

        assert_eq!(admission.current_bytes(), 128 * 1024 * 1024);
        assert_eq!(admission.peak_bytes(), 2 * GIB);
        assert_eq!(broker.used_bytes(), 128 * 1024 * 1024);
    }

    #[test]
    fn measured_live_state_above_broker_budget_does_not_terminate_encode() {
        let broker = MemoryBroker::new(1024, 128).unwrap();
        let lease = broker.reserve(64).unwrap();
        let mut admission = EncodeResidentAdmission::new(lease, 64);

        admission.commit(256).unwrap();
        admission.reserve_growth(256, 128).unwrap();

        assert_eq!(admission.current_bytes(), 64);
        assert_eq!(admission.peak_bytes(), 384);
    }

    #[test]
    fn payload_spill_reports_the_remaining_resident_column_limit() {
        let error = payload_finalize_admission_error(
            PayloadStorageMode::Spill,
            MemoryError::Budget {
                requested: 2,
                used: 9,
                hard_top: 10,
            },
        );

        assert!(error
            .to_string()
            .contains("payload bodies were spilled successfully"));
        assert!(error.to_string().contains("source/contract/atom state"));
    }

    #[test]
    fn payload_index_mode_externalizes_before_the_structural_upper_bound_exceeds_budget() {
        let mut estimate = EncodeAdmissionEstimate {
            resident_peak_bytes: 2 * GIB,
            partial_peak_bytes: 0,
            token_relation_peak_bytes: 0,
            payload_spill_upper_bound_bytes: 0,
            representative_rows: 1,
            token_rows: 1,
        };
        assert_eq!(
            payload_storage_mode(&estimate, GIB),
            PayloadStorageMode::Spill
        );

        estimate.representative_rows = u64::from(u32::MAX);
        estimate.token_rows = u64::from(u32::MAX);
        assert_eq!(
            payload_storage_mode(&estimate, GIB),
            PayloadStorageMode::SpillExternalIndex
        );
    }

    #[test]
    fn contract_growth_guard_includes_token_specific_json_and_memberships() {
        let sources = vec![
            TokenSourceInput {
                token_ids: vec![1, 2, 3],
            },
            TokenSourceInput {
                token_ids: vec![4, 5],
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

    #[test]
    fn storage_reservation_does_not_preflight_physical_space() {
        let directory = tempfile::tempdir().unwrap();
        let mut broker = StorageBroker::open_with_physical_free(directory.path(), 1_000).unwrap();

        let reservation = reserve_storage_advisory(
            &mut broker,
            ArtifactClass::Feature,
            800,
            300,
            "test feature bundle",
            |_| {},
        )
        .unwrap();

        assert!(reservation.is_some());
        assert_eq!(broker.snapshot().committed_bytes, 800);
    }

    #[test]
    fn pending_token_arena_capacity_is_memory_accounted() {
        fn pending() -> Vec<PendingContractSlot> {
            vec![PendingContractSlot {
                chain_id: 0,
                weight: 1,
                representative_payload_ref: PayloadHandle::Memory(PayloadRef {
                    shard_id: 0,
                    local_id: 0,
                }),
                representative_token_range: TokenRange { start: 0, end: 0 },
                token_sources: vec![PendingSourceSlot {
                    payload_ref: PayloadHandle::Memory(PayloadRef {
                        shard_id: 0,
                        local_id: 0,
                    }),
                    token_range: TokenRange { start: 0, end: 0 },
                }],
            }]
        }

        let store =
            PayloadRegistrationStore::Memory(ShardedPayloadArena::with_shard_count(1, 1024));
        let empty_tokens = Vec::new();
        let mut empty_accounting = EncodeRegistrationAccounting::default();
        let empty = empty_accounting
            .resident_bytes(&pending(), &HashMap::new(), &store, &empty_tokens)
            .unwrap();
        let mut populated_tokens = Vec::with_capacity(4);
        populated_tokens.extend([1, 2, 3, 4]);
        let mut populated_accounting = EncodeRegistrationAccounting::default();
        let populated = populated_accounting
            .resident_bytes(&pending(), &HashMap::new(), &store, &populated_tokens)
            .unwrap();

        assert_eq!(populated - empty, 4 * std::mem::size_of::<u32>() as u64);
    }

    #[test]
    fn pending_token_arena_is_written_in_final_csr_order_and_reused() {
        let payload_ref = |local_id| {
            PayloadHandle::Memory(PayloadRef {
                shard_id: 0,
                local_id,
            })
        };
        let relation = TokenSourceRelation {
            sources: vec![
                TokenSourceRecord {
                    source_file: 1,
                    source_row_number: 10,
                    payload_ref: payload_ref(0),
                },
                TokenSourceRecord {
                    source_file: 1,
                    source_row_number: 20,
                    payload_ref: payload_ref(1),
                },
                TokenSourceRecord {
                    source_file: 1,
                    source_row_number: 30,
                    payload_ref: payload_ref(2),
                },
            ],
            memberships: vec![
                ResolvedTokenMembership {
                    contract_index: 0,
                    source_id: 0,
                    token_id: 1,
                },
                ResolvedTokenMembership {
                    contract_index: 0,
                    source_id: 0,
                    token_id: 2,
                },
                ResolvedTokenMembership {
                    contract_index: 0,
                    source_id: 1,
                    token_id: 4,
                },
                ResolvedTokenMembership {
                    contract_index: 0,
                    source_id: 2,
                    token_id: 7,
                },
                ResolvedTokenMembership {
                    contract_index: 0,
                    source_id: 2,
                    token_id: 8,
                },
            ],
            contract_offsets: vec![0, 5],
            logical_bytes: 0,
        };
        let mut pending_token_ids = Vec::new();
        let (representative_token_range, token_sources) = relation
            .append_contract_layout(
                0,
                SourceCoordinate {
                    source_file: 1,
                    source_row_number: 20,
                },
                &mut pending_token_ids,
            )
            .unwrap();

        assert_eq!(pending_token_ids, [4, 1, 2, 7, 8]);
        assert_eq!(representative_token_range, TokenRange { start: 0, end: 1 });
        assert_eq!(token_sources.len(), 2);
        assert_eq!(
            token_sources[0].token_range,
            TokenRange { start: 1, end: 3 }
        );
        assert_eq!(
            token_sources[1].token_range,
            TokenRange { start: 3, end: 5 }
        );

        let token_pointer = pending_token_ids.as_ptr();
        let pending_slot = PendingContractSlot {
            chain_id: 9,
            weight: 11,
            representative_payload_ref: payload_ref(1),
            representative_token_range,
            token_sources,
        };
        let disk_pending_slot = pending_slot.clone();
        let disk_token_ids = pending_token_ids.clone();
        let mut sources = metadata_engine::encode::EncodeSourceSoA::with_source_capacity(3);
        sources.token_ids = pending_token_ids;
        let mut contracts = EncodeContractSoA::with_contract_capacity(1);
        let mut token_cursor = 0usize;
        build_encoded_contract(
            pending_slot,
            0,
            &[100, 103],
            &mut token_cursor,
            &mut sources,
            &mut contracts,
        )
        .unwrap();

        assert_eq!(sources.token_ids.as_ptr(), token_pointer);
        assert_eq!(sources.contract_ids, [0, 0, 0]);
        assert_eq!(sources.payload_ids, [101, 100, 102]);
        assert_eq!(sources.token_offsets, [0, 1, 3, 5]);
        assert_eq!(sources.token_ids, [4, 1, 2, 7, 8]);
        assert_eq!(contracts.source_doc_ids, [0]);
        assert_eq!(token_cursor, 5);

        let directory = tempfile::tempdir().unwrap();
        let (disk_sources, disk_contracts) = build_disk_encoded_columns(
            directory.path(),
            vec![disk_pending_slot],
            disk_token_ids,
            &[100, 103],
            3,
        )
        .unwrap();
        let disk_sources = disk_sources.view();
        let disk_contracts = disk_contracts.view();
        assert_eq!(disk_sources.contract_ids, sources.contract_ids);
        assert_eq!(disk_sources.payload_ids, sources.payload_ids);
        assert_eq!(disk_sources.token_offsets, sources.token_offsets);
        assert_eq!(disk_sources.token_ids, sources.token_ids);
        assert_eq!(disk_contracts.contract_ids, contracts.contract_ids);
        assert_eq!(disk_contracts.chain_ids, contracts.chain_ids);
        assert_eq!(disk_contracts.source_doc_ids, contracts.source_doc_ids);
        assert_eq!(disk_contracts.payload_ids, contracts.payload_ids);
        assert_eq!(disk_contracts.weights, contracts.weights);
    }
}
