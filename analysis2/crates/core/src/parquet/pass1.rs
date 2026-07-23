//! Pass 1: identity + name + URI column projection.

use arrow_array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow_cast::cast;
use arrow_schema::DataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use rayon::prelude::*;
use std::fs::File;
use std::path::Path;

use crate::entity::{ResidentStore, SourceOrder};
use crate::parquet::merge::merge_shards_ordered;
use crate::parquet::validate::{ValidatedInput, PASS1_COLUMNS};
use crate::parquet::LoadOptions;
use crate::progress::ProgressObserver;
use crate::Analysis2Error;

pub fn scan_pass1(
    inputs: &[ValidatedInput],
    options: &LoadOptions,
    progress: &dyn ProgressObserver,
) -> Result<ResidentStore, Analysis2Error> {
    let shard_results: Vec<Result<ResidentStore, Analysis2Error>> = inputs
        .par_iter()
        .map(|input| scan_file_pass1(input, options, progress))
        .collect();
    merge_shards_ordered(shard_results, options)
}

fn scan_file_pass1(
    input: &ValidatedInput,
    options: &LoadOptions,
    progress: &dyn ProgressObserver,
) -> Result<ResidentStore, Analysis2Error> {
    let mut row_start = 0_u64;
    let mut row_groups = Vec::with_capacity(input.row_group_count);
    for row_group in 0..input.row_group_count {
        row_groups.push((row_group, row_start));
        let rows = input
            .metadata
            .metadata()
            .row_group(row_group)
            .num_rows()
            .max(0) as u64;
        row_start = row_start.saturating_add(rows);
    }
    let row_group_results: Vec<Result<ResidentStore, Analysis2Error>> = row_groups
        .par_iter()
        .map(|&(row_group, row_start)| {
            scan_row_group_pass1(input, row_group, row_start, options, progress)
        })
        .collect();
    merge_shards_ordered(row_group_results, options)
}

fn scan_row_group_pass1(
    input: &ValidatedInput,
    row_group: usize,
    row_start: u64,
    options: &LoadOptions,
    progress: &dyn ProgressObserver,
) -> Result<ResidentStore, Analysis2Error> {
    progress.check_cancelled()?;
    let file = File::open(&input.path)
        .map_err(|error| Analysis2Error::parquet(format!("{}: {error}", input.path.display())))?;
    let mask = ProjectionMask::roots(
        input.metadata.metadata().file_metadata().schema_descr(),
        input.pass1_projection.iter().copied(),
    );
    let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(file, input.metadata.clone())
        .with_projection(mask)
        .with_row_groups(vec![row_group])
        .with_batch_size(8 * 1024)
        .build()
        .map_err(|error| {
            Analysis2Error::parquet(format!("{}: {error}", input.path.display()))
        })?;
    let mut shard = ResidentStore::with_options(options.metadata_anchors, &options.evm_chains);
    let mut row_offset = 0_u64;
    for batch in reader {
        let batch = batch.map_err(|error| {
            Analysis2Error::parquet(format!("{}: {error}", input.path.display()))
        })?;
        let columns = ProjectedUtf8Columns::new(&batch, &input.path, &PASS1_COLUMNS)?;
        for row_index in 0..batch.num_rows() {
            let source_order = SourceOrder {
                file_ordinal: input.file_ordinal,
                file_row_number: row_start + row_offset,
            };
            row_offset += 1;
            let chain = normalize_chain(columns.value_at(0, row_index));
            if !options.allowed_chains.is_empty() && !options.allowed_chains.contains(&chain) {
                continue;
            }
            // Intern directly from Arrow slices — no intermediate IdentityRow Strings.
            shard.ingest_identity_strs(
                &chain,
                columns.value_at(1, row_index).trim(),
                columns.value_at(2, row_index).trim(),
                columns.value_at(3, row_index),
                columns.value_at(4, row_index),
                columns.value_at(5, row_index),
                source_order,
            )?;
        }
        progress.add_completed(batch.num_rows() as u64);
    }
    Ok(shard)
}

pub(crate) struct ProjectedUtf8Columns {
    columns: Vec<ArrayRef>,
}

impl ProjectedUtf8Columns {
    pub(crate) fn new(
        batch: &RecordBatch,
        path: &Path,
        names: &[&str],
    ) -> Result<Self, Analysis2Error> {
        let mut columns = Vec::with_capacity(names.len());
        for required in names {
            let index = batch.schema().index_of(required).map_err(|error| {
                Analysis2Error::parquet(format!("{}: {error}", path.display()))
            })?;
            let source = batch.column(index);
            let converted = cast(source, &DataType::Utf8).map_err(|error| {
                Analysis2Error::parquet(format!(
                    "{}: column `{required}` cannot be cast from {:?} to Utf8: {error}",
                    path.display(),
                    source.data_type()
                ))
            })?;
            columns.push(converted);
        }
        Ok(Self { columns })
    }

    pub(crate) fn value_at(&self, column: usize, row: usize) -> &str {
        let array = self.columns[column]
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("arrow cast to Utf8 must return StringArray");
        if array.is_null(row) {
            ""
        } else {
            array.value(row)
        }
    }
}

pub(crate) fn normalize_chain(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}
