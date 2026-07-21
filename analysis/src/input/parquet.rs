use crate::error::{AnalysisError, Result};
use crate::input::{projection_indices, projection_indices_for};
use crate::model::{ChainId, SourceOrder};
use crate::progress::Progress;
use crate::resident::{PreparedMetadataInput, ResidentBaseStore, ResidentBuilder};
use arrow_array::{Array, LargeStringArray, RecordBatch, StringArray, StringViewArray};
use parking_lot::Mutex;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
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
    let builder_shards = Arc::new(
        (0..index_shards)
            .map(|_| Mutex::new(ResidentBuilder::default()))
            .collect::<Vec<_>>(),
    );
    for (file_ordinal, path) in files.iter().enumerate() {
        scan_file(
            path,
            file_ordinal as u16,
            &builder_shards,
            memory_limit,
            executor,
            progress,
        )?;
    }
    let shard_estimates = builder_shards
        .iter()
        .map(|builder| builder.lock().estimated_bytes())
        .collect::<Vec<_>>();
    let sharded_total = shard_estimates
        .iter()
        .fold(0_u64, |sum, value| sum.saturating_add(*value));
    let largest_shard = shard_estimates.iter().copied().max().unwrap_or(0);
    enforce_builder_budget(
        sharded_total.saturating_add(largest_shard.saturating_mul(2)),
        memory_limit,
    )?;
    let builder_shards = Arc::try_unwrap(builder_shards)
        .map_err(|_| AnalysisError::State("base builder shards are still referenced".into()))?;
    let mut builders = builder_shards
        .into_iter()
        .map(Mutex::into_inner)
        .collect::<Vec<_>>();
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
    for shard in builders {
        resident.merge_from(shard)?;
    }
    enforce_builder_budget(resident.preparation_peak_bytes(), memory_limit)?;
    executor.install(|| resident.prepare_metadata(metadata_anchor_count))?;
    enforce_builder_budget(resident.estimated_bytes(), memory_limit)?;
    for (file_ordinal, path) in files.iter().enumerate() {
        scan_metadata_file(
            path,
            file_ordinal as u16,
            &mut resident,
            memory_limit,
            executor,
            progress,
        )?;
    }
    executor.install(|| resident.finish(metadata_anchor_count, index_shards))
}

fn scan_file(
    path: &Path,
    file_ordinal: u16,
    resident: &Arc<Vec<Mutex<ResidentBuilder>>>,
    memory_limit: u64,
    executor: &crate::pipeline::CpuExecutor,
    progress: &Progress,
) -> Result<()> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(path)?)?;
    let row_group_count = builder.metadata().num_row_groups() as u64;
    progress.add_total_row_groups(row_group_count);
    let mut row_start = 0_u64;
    let groups = builder
        .metadata()
        .row_groups()
        .iter()
        .enumerate()
        .map(|(row_group, metadata)| {
            let descriptor = RowGroupDescriptor {
                row_group,
                row_start,
                estimated_bytes: metadata.total_byte_size().max(1) as u64,
            };
            row_start += metadata.num_rows() as u64;
            descriptor
        })
        .collect::<Vec<_>>();
    drop(builder);
    let decode_budget = (memory_limit / 32).clamp(512 * 1024 * 1024, 16 * 1024 * 1024 * 1024);
    let mut start = 0;
    while start < groups.len() {
        let mut end = start;
        let mut admitted = 0_u64;
        while end < groups.len() && end - start < executor.workers() {
            let next = groups[end].estimated_bytes;
            if end > start && admitted.saturating_add(next) > decode_budget {
                break;
            }
            admitted = admitted.saturating_add(next);
            end += 1;
        }
        let (sender, receiver) = std::sync::mpsc::channel();
        for &descriptor in &groups[start..end] {
            let sender = sender.clone();
            let path = path.to_path_buf();
            let resident = resident.clone();
            let _ = executor.submit_kind_routed(
                crate::pipeline::CpuTaskKind::Dedup,
                descriptor.row_group,
                move || {
                    let result =
                        decode_row_group_stream(&path, file_ordinal, descriptor, &resident);
                    let _ = sender.send(result);
                },
            );
        }
        drop(sender);
        for _ in start..end {
            let rows = receiver.recv().map_err(|_| {
                AnalysisError::State("Parquet row-group decode channel closed".into())
            })??;
            progress.add_input_rows(rows);
            progress.add_completed_row_groups(1);
            enforce_builder_budget(sharded_builder_bytes(resident), memory_limit)?;
        }
        enforce_builder_budget(sharded_builder_bytes(resident), memory_limit)?;
        start = end;
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct RowGroupDescriptor {
    row_group: usize,
    row_start: u64,
    estimated_bytes: u64,
}

fn decode_row_group_stream(
    path: &Path,
    file_ordinal: u16,
    descriptor: RowGroupDescriptor,
    resident: &Arc<Vec<Mutex<ResidentBuilder>>>,
) -> Result<u64> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(path)?)?;
    projection_indices(builder.schema())?;
    let indices = projection_indices_for(
        builder.schema(),
        &[
            "chain",
            "contract_address",
            "token_id",
            "name_norm",
            "token_uri_norm",
            "image_uri_norm",
        ],
    )?;
    let projection = ProjectionMask::leaves(builder.parquet_schema(), indices);
    let reader = builder
        .with_projection(projection)
        .with_row_groups(vec![descriptor.row_group])
        .with_batch_size(65_536)
        .build()?;
    let mut file_row_number = descriptor.row_start;
    let mut decoded_rows = 0_u64;
    for batch in reader {
        let batch = batch?;
        let row_count = batch.num_rows() as u64;
        decode_batch_into_resident(&batch, file_ordinal, &mut file_row_number, resident)?;
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

fn scan_metadata_file(
    path: &Path,
    file_ordinal: u16,
    resident: &mut ResidentBuilder,
    memory_limit: u64,
    executor: &crate::pipeline::CpuExecutor,
    progress: &Progress,
) -> Result<()> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(path)?)?;
    let mut row_start = 0_u64;
    let groups = builder
        .metadata()
        .row_groups()
        .iter()
        .enumerate()
        .map(|(row_group, metadata)| {
            let descriptor = RowGroupDescriptor {
                row_group,
                row_start,
                estimated_bytes: metadata.total_byte_size().max(1) as u64,
            };
            row_start += metadata.num_rows() as u64;
            descriptor
        })
        .collect::<Vec<_>>();
    progress.add_total_row_groups(groups.len() as u64);
    drop(builder);
    let decode_budget = (memory_limit / 64).clamp(256 * 1024 * 1024, 4 * 1024 * 1024 * 1024);
    let mut start = 0;
    while start < groups.len() {
        let mut end = start;
        let mut admitted = 0_u64;
        while end < groups.len() && end - start < executor.workers() {
            let next = groups[end].estimated_bytes;
            if end > start && admitted.saturating_add(next) > decode_budget {
                break;
            }
            admitted = admitted.saturating_add(next);
            end += 1;
        }
        executor.scope(|scope| {
            let (sender, receiver) = std::sync::mpsc::sync_channel(executor.workers().clamp(1, 16));
            for &descriptor in &groups[start..end] {
                let sender = sender.clone();
                scope.spawn(move |_| {
                    if let Err(error) =
                        decode_metadata_row_group_stream(path, file_ordinal, descriptor, &sender)
                    {
                        let _ = sender.send(DecodedMetadataRows::Failed(error));
                    }
                });
            }
            drop(sender);
            let mut completed = 0;
            while completed < end - start {
                match receiver.recv().map_err(|_| {
                    AnalysisError::State("Metadata row-group decode channel closed".into())
                })? {
                    DecodedMetadataRows::Batch(batch) => {
                        attach_metadata_batch(resident, file_ordinal, batch)?;
                    }
                    DecodedMetadataRows::Finished => {
                        completed += 1;
                        progress.add_completed_row_groups(1);
                    }
                    DecodedMetadataRows::Failed(error) => return Err(error),
                }
            }
            Ok::<_, AnalysisError>(())
        })?;
        enforce_builder_budget(resident.estimated_bytes(), memory_limit)?;
        start = end;
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
    Finished,
    Failed(AnalysisError),
}

struct PreparedMetadataBatch {
    batch: RecordBatch,
    chains: Vec<ChainId>,
    row_start: u64,
}

fn decode_metadata_row_group_stream(
    path: &Path,
    file_ordinal: u16,
    descriptor: RowGroupDescriptor,
    sender: &std::sync::mpsc::SyncSender<DecodedMetadataRows>,
) -> Result<()> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(path)?)?;
    let indices = projection_indices_for(
        builder.schema(),
        &["chain", "contract_address", "token_id", "metadata_json"],
    )?;
    let projection = ProjectionMask::leaves(builder.parquet_schema(), indices);
    let reader = builder
        .with_projection(projection)
        .with_row_groups(vec![descriptor.row_group])
        .with_batch_size(65_536)
        .build()?;
    let mut file_row_number = descriptor.row_start;
    for batch in reader {
        let batch = prepare_metadata_batch(batch?, file_ordinal, file_row_number)?;
        file_row_number += batch.batch.num_rows() as u64;
        sender
            .send(DecodedMetadataRows::Batch(batch))
            .map_err(|_| AnalysisError::State("Metadata row consumer closed".into()))?;
    }
    sender
        .send(DecodedMetadataRows::Finished)
        .map_err(|_| AnalysisError::State("Metadata row consumer closed".into()))
}

fn prepare_metadata_batch(
    batch: RecordBatch,
    file_ordinal: u16,
    row_start: u64,
) -> Result<PreparedMetadataBatch> {
    let chains = {
        let chain = StringColumn::new(&batch, "chain", false)?;
        let mut chains = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let chain_value = chain.required(row)?;
            chains.push(ChainId::from_str(chain_value).map_err(|message| {
                AnalysisError::Input(format!(
                    "{message} at file {file_ordinal}, row {}",
                    row_start + row as u64
                ))
            })?);
        }
        chains
    };
    Ok(PreparedMetadataBatch {
        batch,
        chains,
        row_start,
    })
}

fn attach_metadata_batch(
    resident: &mut ResidentBuilder,
    file_ordinal: u16,
    prepared: PreparedMetadataBatch,
) -> Result<()> {
    let contract = StringColumn::new(&prepared.batch, "contract_address", false)?;
    let token = StringColumn::new(&prepared.batch, "token_id", false)?;
    let metadata = StringColumn::new(&prepared.batch, "metadata_json", true)?;
    for (row, chain) in prepared.chains.into_iter().enumerate() {
        let source = SourceOrder {
            file_ordinal,
            file_row_number: prepared.row_start + row as u64,
        };
        let contract_address = contract.required(row)?;
        let token_id = token.required(row)?;
        let target = resident.metadata_input_target(chain, contract_address, token_id, source)?;
        let disposition = target.disposition();
        resident.attach_prepared_metadata_target(
            target,
            PreparedMetadataInput::from_raw_for_disposition(metadata.optional(row), disposition),
            source,
        )?;
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

    #[test]
    fn evm_owner_hash_is_case_insensitive_without_normalizing_allocation() {
        assert_eq!(
            input_owner_shard(ChainId::Ethereum, " 0xAbCd ", 128),
            input_owner_shard(ChainId::Ethereum, "0xabcd", 128)
        );
    }
}
