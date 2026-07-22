use crate::error::{AnalysisError, Result};
use crate::input::{projection_indices, projection_indices_for};
use crate::model::{ChainId, SourceOrder};
use crate::progress::{Progress, WorkPhase};
use crate::resident::{PreparedMetadataInput, ResidentBaseStore, ResidentBuilder};
use arrow_array::{Array, LargeStringArray, RecordBatch, StringArray, StringViewArray};
use parking_lot::Mutex;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use parquet::arrow::ProjectionMask;
use rayon::prelude::*;
use std::hash::{BuildHasher, Hash, Hasher};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, OnceLock};

pub fn load_resident_store(
    files: &[PathBuf],
    metadata_anchor_count: usize,
    index_shards: usize,
    memory_limit: u64,
    executor: &crate::pipeline::CpuExecutor,
    progress: &Progress,
) -> Result<ResidentBaseStore> {
    // Read and validate each footer once. Row-group workers reopen only the
    // data file and reuse this immutable Arrow metadata in both scan passes.
    progress.begin_phase(WorkPhase::LoadValidate, Some(files.len() as u64));
    let mut prepared_inputs = executor
        .install_on_all(|lane, lane_count| {
            files
                .par_iter()
                .enumerate()
                .filter(|(file_ordinal, _)| file_ordinal % lane_count == lane)
                .map(|(file_ordinal, path)| {
                    let input = PreparedParquetInput::open(path, file_ordinal as u16).map(Arc::new);
                    if input.is_ok() {
                        progress.add_phase_completed(1);
                    }
                    (file_ordinal, input)
                })
                .collect::<Vec<_>>()
        })
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    prepared_inputs.sort_unstable_by_key(|(file_ordinal, _)| *file_ordinal);
    let inputs = prepared_inputs
        .into_iter()
        .map(|(_, input)| input)
        .collect::<Result<Vec<_>>>()?;
    progress.finish_phase();
    let cached_metadata_bytes = inputs.iter().fold(0_u64, |total, input| {
        total.saturating_add(input.cached_bytes)
    });
    // The immutable footer cache remains resident across both scans. Charge it
    // to the same process-wide budget as the builders instead of treating it
    // as free memory.
    enforce_builder_budget(cached_metadata_bytes, memory_limit)?;
    let row_groups = inputs.iter().fold(0_u64, |total, input| {
        total.saturating_add(input.groups.len() as u64)
    });
    // The loader intentionally performs two passes. Publish the stable total
    // up front so progress never reaches 100% and then moves backwards.
    progress.add_total_row_groups(row_groups.saturating_mul(2));
    let builder_shards = Arc::new(
        (0..index_shards)
            .map(|_| Mutex::new(ResidentBuilder::default()))
            .collect::<Vec<_>>(),
    );
    progress.begin_phase(WorkPhase::BaseScan, Some(row_groups));
    scan_files(
        &inputs,
        &builder_shards,
        cached_metadata_bytes,
        memory_limit,
        executor,
        progress,
    )?;
    progress.finish_phase();
    let shard_estimates = builder_shards
        .iter()
        .map(|builder| builder.lock().estimated_bytes())
        .collect::<Vec<_>>();
    let sharded_total = shard_estimates
        .iter()
        .fold(0_u64, |sum, value| sum.saturating_add(*value));
    let largest_shard = shard_estimates.iter().copied().max().unwrap_or(0);
    enforce_builder_budget(
        cached_metadata_bytes
            .saturating_add(sharded_total)
            .saturating_add(largest_shard.saturating_mul(2)),
        memory_limit,
    )?;
    let builder_shards = Arc::try_unwrap(builder_shards)
        .map_err(|_| AnalysisError::State("base builder shards are still referenced".into()))?;
    let mut builders = builder_shards
        .into_iter()
        .map(Mutex::into_inner)
        .collect::<Vec<_>>();
    progress.begin_phase(WorkPhase::Merge, Some(builders.len() as u64));
    let largest_builder = shard_estimates
        .iter()
        .enumerate()
        .fold((0_usize, 0_u64), |largest, (index, &bytes)| {
            if bytes > largest.1 {
                (index, bytes)
            } else {
                largest
            }
        })
        .0;
    // Keep the largest shard's interners and allocations as the merge root.
    // A balanced tree would remap every CompactRow O(log(shards)) times;
    // this path remaps every source row exactly once.
    let mut resident = builders.swap_remove(largest_builder);
    progress.add_phase_completed(1);
    for shard in builders {
        resident.merge_from(shard)?;
        progress.add_phase_completed(1);
    }
    progress.finish_phase();
    enforce_builder_budget(
        cached_metadata_bytes.saturating_add(resident.preparation_peak_bytes()),
        memory_limit,
    )?;
    progress.begin_phase(WorkPhase::PrepareMetadata, Some(1));
    resident.prepare_metadata_numa(metadata_anchor_count, executor)?;
    progress.add_phase_completed(1);
    progress.finish_phase();
    enforce_builder_budget(
        cached_metadata_bytes.saturating_add(resident.estimated_bytes()),
        memory_limit,
    )?;
    progress.begin_phase(WorkPhase::MetadataScan, Some(row_groups));
    scan_metadata_files(
        &inputs,
        &mut resident,
        cached_metadata_bytes,
        memory_limit,
        executor,
        progress,
    )?;
    progress.finish_phase();
    resident.release_metadata_source_lookup();
    // No reader uses the footer cache after the second pass. Releasing it
    // before final compaction restores the budget headroom it occupied.
    drop(inputs);
    progress.begin_phase(WorkPhase::FinalizeStore, Some(1));
    let store = executor.install(|| resident.finish(metadata_anchor_count, index_shards))?;
    progress.add_phase_completed(1);
    progress.finish_phase();
    Ok(store)
}

fn scan_files(
    inputs: &[Arc<PreparedParquetInput>],
    resident: &Arc<Vec<Mutex<ResidentBuilder>>>,
    cached_metadata_bytes: u64,
    memory_limit: u64,
    executor: &crate::pipeline::CpuExecutor,
    progress: &Progress,
) -> Result<()> {
    let groups = inputs
        .iter()
        .enumerate()
        .flat_map(|(input, prepared)| {
            prepared
                .groups
                .iter()
                .copied()
                .map(move |descriptor| (input, descriptor))
        })
        .collect::<Vec<_>>();
    let decode_budget = (memory_limit / 32).clamp(512 * 1024 * 1024, 16 * 1024 * 1024 * 1024);
    let (sender, receiver) = std::sync::mpsc::channel();
    let mut next_group = 0_usize;
    let mut inflight = 0_usize;
    let mut inflight_bytes = 0_u64;
    let mut since_budget_check = 0_usize;
    while next_group < groups.len() || inflight > 0 {
        while next_group < groups.len() && inflight < executor.workers() {
            let (input_index, descriptor) = groups[next_group];
            if inflight > 0
                && inflight_bytes.saturating_add(descriptor.estimated_bytes) > decode_budget
            {
                break;
            }
            let sender = sender.clone();
            let input = inputs[input_index].clone();
            let resident = resident.clone();
            let _ = executor.submit_kind_routed(
                crate::pipeline::CpuTaskKind::Dedup,
                (input.file_ordinal, descriptor.row_group),
                move || {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        decode_row_group_stream(&input, descriptor, &resident)
                    }))
                    .unwrap_or_else(|_| {
                        Err(AnalysisError::State(format!(
                            "Parquet row group {} panicked during decode",
                            descriptor.row_group
                        )))
                    });
                    let _ = sender.send((descriptor.estimated_bytes, result));
                },
            );
            inflight += 1;
            inflight_bytes = inflight_bytes.saturating_add(descriptor.estimated_bytes);
            next_group += 1;
        }
        let (completed_bytes, rows) = receiver
            .recv()
            .map_err(|_| AnalysisError::State("Parquet row-group decode channel closed".into()))?;
        let rows = rows?;
        inflight = inflight
            .checked_sub(1)
            .ok_or_else(|| AnalysisError::State("Parquet decode inflight underflow".into()))?;
        inflight_bytes = inflight_bytes.saturating_sub(completed_bytes);
        progress.add_input_rows(rows);
        progress.add_completed_row_groups(1);
        progress.add_phase_completed(1);
        since_budget_check += 1;
        if since_budget_check >= executor.workers() || (next_group == groups.len() && inflight == 0)
        {
            // The rolling admission window is bounded by compressed bytes.
            // Sample the 128 builder shards once per worker-width of actual
            // completions, not once per row group, to avoid a growing lock scan
            // on the decode hot path.
            enforce_builder_budget(
                cached_metadata_bytes.saturating_add(sharded_builder_bytes(resident)),
                memory_limit,
            )?;
            since_budget_check = 0;
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct RowGroupDescriptor {
    row_group: usize,
    row_start: u64,
    estimated_bytes: u64,
}

struct PreparedParquetInput {
    path: PathBuf,
    file_ordinal: u16,
    metadata: ArrowReaderMetadata,
    base_projection: Vec<usize>,
    metadata_projection: Vec<usize>,
    groups: Vec<RowGroupDescriptor>,
    cached_bytes: u64,
}

impl PreparedParquetInput {
    fn open(path: &Path, file_ordinal: u16) -> Result<Self> {
        let file = std::fs::File::open(path)?;
        let metadata = ArrowReaderMetadata::load(&file, ArrowReaderOptions::new())?;
        projection_indices(metadata.schema())?;
        let base_projection = projection_indices_for(
            metadata.schema(),
            &[
                "chain",
                "contract_address",
                "token_id",
                "name_norm",
                "token_uri_norm",
                "image_uri_norm",
            ],
        )?;
        let metadata_projection = projection_indices_for(metadata.schema(), &["metadata_json"])?;
        let mut row_start = 0_u64;
        let groups: Vec<RowGroupDescriptor> = metadata
            .metadata()
            .row_groups()
            .iter()
            .enumerate()
            .map(|(row_group, row_group_metadata)| {
                let descriptor = RowGroupDescriptor {
                    row_group,
                    row_start,
                    estimated_bytes: row_group_metadata.total_byte_size().max(1) as u64,
                };
                row_start = row_start.saturating_add(row_group_metadata.num_rows().max(0) as u64);
                descriptor
            })
            .collect();
        let path = path.to_path_buf();
        // ParquetMetaData::memory_size accounts for the parsed footer graph.
        // Charge a second copy as a conservative allowance for the derived
        // Arrow schema/fields and allocator overhead, then add our own buffers.
        let cached_bytes = u64::try_from(metadata.metadata().memory_size())
            .unwrap_or(u64::MAX)
            .saturating_mul(2)
            .saturating_add(u64::try_from(std::mem::size_of::<Self>()).unwrap_or(u64::MAX))
            .saturating_add(u64::try_from(path.as_os_str().len()).unwrap_or(u64::MAX))
            .saturating_add(
                u64::try_from(base_projection.capacity())
                    .unwrap_or(u64::MAX)
                    .saturating_mul(std::mem::size_of::<usize>() as u64),
            )
            .saturating_add(
                u64::try_from(metadata_projection.capacity())
                    .unwrap_or(u64::MAX)
                    .saturating_mul(std::mem::size_of::<usize>() as u64),
            )
            .saturating_add(
                u64::try_from(groups.capacity())
                    .unwrap_or(u64::MAX)
                    .saturating_mul(std::mem::size_of::<RowGroupDescriptor>() as u64),
            );
        Ok(Self {
            path,
            file_ordinal,
            metadata,
            base_projection,
            metadata_projection,
            groups,
            cached_bytes,
        })
    }
}

fn decode_row_group_stream(
    input: &PreparedParquetInput,
    descriptor: RowGroupDescriptor,
    resident: &Arc<Vec<Mutex<ResidentBuilder>>>,
) -> Result<u64> {
    let file = std::fs::File::open(&input.path)?;
    let projection = ProjectionMask::leaves(
        input.metadata.metadata().file_metadata().schema_descr(),
        input.base_projection.clone(),
    );
    let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(file, input.metadata.clone())
        .with_projection(projection)
        .with_row_groups(vec![descriptor.row_group])
        .with_batch_size(65_536)
        .build()?;
    let mut file_row_number = descriptor.row_start;
    let mut decoded_rows = 0_u64;
    for batch in reader {
        let batch = batch?;
        let row_count = batch.num_rows() as u64;
        decode_batch_into_resident(&batch, input.file_ordinal, &mut file_row_number, resident)?;
        decoded_rows = decoded_rows.saturating_add(row_count);
    }
    Ok(decoded_rows)
}

fn decode_batch_into_resident(
    batch: &RecordBatch,
    file_ordinal: u16,
    file_row_number: &mut u64,
    resident: &Arc<Vec<Mutex<ResidentBuilder>>>,
) -> Result<()> {
    let chain = StringColumn::new(batch, "chain", false)?;
    let contract = StringColumn::new(batch, "contract_address", false)?;
    let token = StringColumn::new(batch, "token_id", false)?;
    let name = StringColumn::new(batch, "name_norm", true)?;
    let token_uri = StringColumn::new(batch, "token_uri_norm", true)?;
    let image_uri = StringColumn::new(batch, "image_uri_norm", true)?;
    let row_start = *file_row_number;
    let mut shard_rows = (0..resident.len()).map(|_| Vec::new()).collect::<Vec<_>>();
    for row in 0..batch.num_rows() {
        let chain_value = chain.required(row)?;
        let parsed_chain = ChainId::from_str(chain_value).map_err(|message| {
            AnalysisError::Input(format!(
                "{message} at file {file_ordinal}, row {}",
                row_start + row as u64
            ))
        })?;
        let shard = input_owner_shard(parsed_chain, contract.required(row)?, resident.len());
        shard_rows[shard].push((row, parsed_chain));
    }
    for (shard, rows) in shard_rows.into_iter().enumerate() {
        if rows.is_empty() {
            continue;
        }
        let mut builder = resident[shard].lock();
        for (row, parsed_chain) in rows {
            builder.push_borrowed(
                parsed_chain,
                contract.required(row)?,
                token.required(row)?,
                name.optional(row),
                token_uri.optional(row),
                image_uri.optional(row),
                None,
                SourceOrder {
                    file_ordinal,
                    file_row_number: row_start + row as u64,
                },
            )?;
        }
    }
    *file_row_number = row_start + batch.num_rows() as u64;
    Ok(())
}

fn input_owner_shard(chain: ChainId, address: &str, shard_count: usize) -> usize {
    static HASHER: OnceLock<ahash::RandomState> = OnceLock::new();
    let hasher = HASHER.get_or_init(|| ahash::RandomState::with_seeds(11, 13, 17, 19));
    let mut state = hasher.build_hasher();
    chain.hash(&mut state);
    let address = address.trim();
    if chain.is_evm() {
        for byte in address.bytes() {
            state.write_u8(byte.to_ascii_lowercase());
        }
    } else {
        address.hash(&mut state);
    }
    state.finish() as usize & (shard_count - 1)
}

fn sharded_builder_bytes(resident: &[Mutex<ResidentBuilder>]) -> u64 {
    resident.iter().fold(0_u64, |bytes, builder| {
        bytes.saturating_add(builder.lock().estimated_bytes())
    })
}

fn scan_metadata_files(
    inputs: &[Arc<PreparedParquetInput>],
    resident: &mut ResidentBuilder,
    cached_metadata_bytes: u64,
    memory_limit: u64,
    executor: &crate::pipeline::CpuExecutor,
    progress: &Progress,
) -> Result<()> {
    let groups = inputs
        .iter()
        .enumerate()
        .flat_map(|(input, prepared)| {
            prepared
                .groups
                .iter()
                .copied()
                .map(move |descriptor| (input, descriptor))
        })
        .collect::<Vec<_>>();
    let decode_budget = (memory_limit / 32).clamp(512 * 1024 * 1024, 16 * 1024 * 1024 * 1024);
    // Smaller batches shorten the serial commit tail and provide more
    // opportunities for row-group workers to interleave decode and parallel
    // validation. Doubling the queue slots still halves the worst-case queued
    // row count versus the old 16 x 65,536 layout.
    let (sender, receiver) = std::sync::mpsc::sync_channel(executor.workers().clamp(1, 32));
    let mut next_group = 0_usize;
    let mut inflight = 0_usize;
    let mut inflight_bytes = 0_u64;
    let mut since_budget_check = 0_usize;
    while next_group < groups.len() || inflight > 0 {
        while next_group < groups.len() && inflight < executor.workers() {
            let (input_index, descriptor) = groups[next_group];
            if inflight > 0
                && inflight_bytes.saturating_add(descriptor.estimated_bytes) > decode_budget
            {
                break;
            }
            let sender = sender.clone();
            let input = inputs[input_index].clone();
            let _ = executor.submit_kind_routed(
                crate::pipeline::CpuTaskKind::Dedup,
                (input.file_ordinal, descriptor.row_group),
                move || {
                    run_metadata_row_group_worker(descriptor, &sender, || {
                        decode_metadata_row_group_stream(&input, descriptor, &sender)
                    });
                },
            );
            inflight += 1;
            inflight_bytes = inflight_bytes.saturating_add(descriptor.estimated_bytes);
            next_group += 1;
        }
        match receiver
            .recv()
            .map_err(|_| AnalysisError::State("Metadata row-group decode channel closed".into()))?
        {
            DecodedMetadataRows::Batch(batch) => {
                attach_metadata_batch(resident, batch)?;
            }
            DecodedMetadataRows::Finished(completed_bytes) => {
                retire_metadata_row_group(&mut inflight, &mut inflight_bytes, completed_bytes)?;
                progress.add_completed_row_groups(1);
                progress.add_phase_completed(1);
                since_budget_check += 1;
                if since_budget_check >= executor.workers()
                    || (next_group == groups.len() && inflight == 0)
                {
                    enforce_builder_budget(
                        cached_metadata_bytes.saturating_add(resident.estimated_bytes()),
                        memory_limit,
                    )?;
                    since_budget_check = 0;
                }
            }
            DecodedMetadataRows::Failed {
                completed_bytes,
                error,
            } => {
                retire_metadata_row_group(&mut inflight, &mut inflight_bytes, completed_bytes)?;
                return Err(error);
            }
        }
    }
    Ok(())
}

fn enforce_builder_budget(required: u64, limit: u64) -> Result<()> {
    const MAX_ALLOCATOR_RESERVE: u64 = 32 * 1024 * 1024 * 1024;
    let allocator_reserve = (limit / 16).min(MAX_ALLOCATOR_RESERVE);
    let usable = limit.saturating_sub(allocator_reserve);
    if required > usable {
        Err(AnalysisError::MemoryBudget {
            required: required.saturating_add(allocator_reserve),
            limit,
        })
    } else {
        Ok(())
    }
}

enum DecodedMetadataRows {
    Batch(PreparedMetadataBatch),
    Finished(u64),
    Failed {
        completed_bytes: u64,
        error: AnalysisError,
    },
}

fn run_metadata_row_group_worker(
    descriptor: RowGroupDescriptor,
    sender: &std::sync::mpsc::SyncSender<DecodedMetadataRows>,
    task: impl FnOnce() -> Result<()>,
) {
    let terminal = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(task)) {
        Ok(Ok(())) => DecodedMetadataRows::Finished(descriptor.estimated_bytes),
        Ok(Err(error)) => DecodedMetadataRows::Failed {
            completed_bytes: descriptor.estimated_bytes,
            error,
        },
        Err(_) => DecodedMetadataRows::Failed {
            completed_bytes: descriptor.estimated_bytes,
            error: AnalysisError::State(format!(
                "Metadata row group {} panicked during decode",
                descriptor.row_group
            )),
        },
    };
    let _ = sender.send(terminal);
}

fn retire_metadata_row_group(
    inflight: &mut usize,
    inflight_bytes: &mut u64,
    completed_bytes: u64,
) -> Result<()> {
    let remaining = inflight
        .checked_sub(1)
        .ok_or_else(|| AnalysisError::State("Metadata decode inflight underflow".into()))?;
    let remaining_bytes = inflight_bytes.checked_sub(completed_bytes).ok_or_else(|| {
        AnalysisError::State("Metadata decode inflight byte accounting underflow".into())
    })?;
    *inflight = remaining;
    *inflight_bytes = remaining_bytes;
    Ok(())
}

struct PreparedMetadataBatch {
    batch: RecordBatch,
    metadata: Vec<ValidatedMetadataInput>,
    file_ordinal: u16,
    row_start: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ValidatedMetadataInput {
    Missing,
    Oversized,
    Invalid,
    Valid,
}

fn decode_metadata_row_group_stream(
    input: &PreparedParquetInput,
    descriptor: RowGroupDescriptor,
    sender: &std::sync::mpsc::SyncSender<DecodedMetadataRows>,
) -> Result<()> {
    let file = std::fs::File::open(&input.path)?;
    let projection = ProjectionMask::leaves(
        input.metadata.metadata().file_metadata().schema_descr(),
        input.metadata_projection.clone(),
    );
    let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(file, input.metadata.clone())
        .with_projection(projection)
        .with_row_groups(vec![descriptor.row_group])
        .with_batch_size(16_384)
        .build()?;
    let mut file_row_number = descriptor.row_start;
    for batch in reader {
        let batch = prepare_metadata_batch(batch?, input.file_ordinal, file_row_number)?;
        file_row_number += batch.batch.num_rows() as u64;
        sender
            .send(DecodedMetadataRows::Batch(batch))
            .map_err(|_| AnalysisError::State("Metadata row consumer closed".into()))?;
    }
    Ok(())
}

fn prepare_metadata_batch(
    batch: RecordBatch,
    file_ordinal: u16,
    row_start: u64,
) -> Result<PreparedMetadataBatch> {
    let metadata = StringColumn::new(&batch, "metadata_json", true)?;
    // JSON validation/normalization is the CPU-heavy part of the second pass.
    // Run it inside the row-group worker's Rayon pool so all workers in that
    // NUMA lane can steal row work instead of feeding one serial consumer.
    // Indexed collection preserves row order and therefore deterministic error
    // selection and SourceOrder behavior.
    let validated = (0..batch.num_rows())
        .into_par_iter()
        .map(|row| validate_metadata_input(metadata.optional(row)))
        .collect::<Vec<_>>();
    Ok(PreparedMetadataBatch {
        batch,
        metadata: validated,
        file_ordinal,
        row_start,
    })
}

fn validate_metadata_input(raw: Option<&str>) -> ValidatedMetadataInput {
    let Some(raw) = raw.map(str::trim).filter(|value| !value.is_empty()) else {
        return ValidatedMetadataInput::Missing;
    };
    if raw.len() > 64 * 1024 {
        return ValidatedMetadataInput::Oversized;
    }
    if crate::resident::metadata_index::is_valid_metadata_json(raw) {
        ValidatedMetadataInput::Valid
    } else {
        ValidatedMetadataInput::Invalid
    }
}

fn attach_metadata_batch(
    resident: &mut ResidentBuilder,
    prepared: PreparedMetadataBatch,
) -> Result<()> {
    let metadata = StringColumn::new(&prepared.batch, "metadata_json", true)?;
    for (row, validated) in prepared.metadata.into_iter().enumerate() {
        let source = SourceOrder {
            file_ordinal: prepared.file_ordinal,
            file_row_number: prepared.row_start + row as u64,
        };
        let target = resident.metadata_input_target_by_source(source)?;
        let disposition = target.disposition();
        let prepared = match validated {
            ValidatedMetadataInput::Missing => PreparedMetadataInput::Missing,
            ValidatedMetadataInput::Oversized => PreparedMetadataInput::Oversized,
            ValidatedMetadataInput::Invalid => PreparedMetadataInput::Invalid,
            ValidatedMetadataInput::Valid => match disposition {
                crate::resident::MetadataInputDisposition::Anchor => {
                    PreparedMetadataInput::from_raw_for_disposition(
                        metadata.optional(row),
                        disposition,
                    )
                }
                crate::resident::MetadataInputDisposition::NonAnchor => {
                    PreparedMetadataInput::NonAnchor
                }
                crate::resident::MetadataInputDisposition::Duplicate => {
                    PreparedMetadataInput::Ignored
                }
            },
        };
        resident.attach_prepared_metadata_target(target, prepared, source)?;
    }
    Ok(())
}

enum StringColumn<'a> {
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Utf8View(&'a StringViewArray),
}

impl<'a> StringColumn<'a> {
    fn new(batch: &'a RecordBatch, name: &str, nullable: bool) -> Result<Self> {
        let column = batch
            .column_by_name(name)
            .ok_or_else(|| AnalysisError::Input(format!("projected column `{name}` is missing")))?;
        if !nullable && column.null_count() != 0 {
            return Err(AnalysisError::Input(format!(
                "required column `{name}` contains nulls"
            )));
        }
        if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
            return Ok(Self::Utf8(array));
        }
        if let Some(array) = column.as_any().downcast_ref::<LargeStringArray>() {
            return Ok(Self::LargeUtf8(array));
        }
        if let Some(array) = column.as_any().downcast_ref::<StringViewArray>() {
            return Ok(Self::Utf8View(array));
        }
        Err(AnalysisError::Input(format!(
            "column `{name}` must be Utf8, LargeUtf8, or Utf8View, found {}",
            column.data_type()
        )))
    }

    fn optional(&self, row: usize) -> Option<&str> {
        match self {
            Self::Utf8(array) => (!array.is_null(row)).then(|| array.value(row)),
            Self::LargeUtf8(array) => (!array.is_null(row)).then(|| array.value(row)),
            Self::Utf8View(array) => (!array.is_null(row)).then(|| array.value(row)),
        }
    }

    fn required(&self, row: usize) -> Result<&str> {
        self.optional(row)
            .ok_or_else(|| AnalysisError::Input(format!("required string is null at row {row}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn metadata_validation_classifies_without_canonicalizing_the_batch() {
        assert_eq!(
            validate_metadata_input(None),
            ValidatedMetadataInput::Missing
        );
        assert_eq!(
            validate_metadata_input(Some("  ")),
            ValidatedMetadataInput::Missing
        );
        assert_eq!(
            validate_metadata_input(Some("not-json")),
            ValidatedMetadataInput::Invalid
        );
        assert_eq!(
            validate_metadata_input(Some("{}")),
            ValidatedMetadataInput::Invalid
        );
        assert_eq!(
            validate_metadata_input(Some(r#"{"name":"valid"}"#)),
            ValidatedMetadataInput::Valid
        );
        let oversized = format!(r#"{{"value":"{}"}}"#, "x".repeat(64 * 1024));
        assert_eq!(
            validate_metadata_input(Some(&oversized)),
            ValidatedMetadataInput::Oversized
        );
    }

    #[test]
    fn evm_owner_hash_is_case_insensitive_without_normalizing_allocation() {
        assert_eq!(
            input_owner_shard(ChainId::Ethereum, " 0xAbCd ", 128),
            input_owner_shard(ChainId::Ethereum, "0xabcd", 128)
        );
    }

    #[test]
    fn metadata_worker_panic_publishes_failed_terminal_and_retires_budget() {
        let descriptor = RowGroupDescriptor {
            row_group: 7,
            row_start: 0,
            estimated_bytes: 4096,
        };
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        run_metadata_row_group_worker(descriptor, &sender, || -> Result<()> {
            panic!("injected metadata decode panic")
        });

        let (completed_bytes, error) = match receiver.recv_timeout(Duration::from_secs(1)).unwrap()
        {
            DecodedMetadataRows::Failed {
                completed_bytes,
                error,
            } => (completed_bytes, error),
            _ => panic!("panic must publish exactly one failed terminal"),
        };
        assert_eq!(completed_bytes, descriptor.estimated_bytes);
        assert!(matches!(
            error,
            AnalysisError::State(message)
                if message == "Metadata row group 7 panicked during decode"
        ));

        let mut inflight = 1;
        let mut inflight_bytes = descriptor.estimated_bytes;
        retire_metadata_row_group(&mut inflight, &mut inflight_bytes, completed_bytes).unwrap();
        assert_eq!(inflight, 0);
        assert_eq!(inflight_bytes, 0);
    }
}
