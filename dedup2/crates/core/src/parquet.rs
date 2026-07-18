use crate::entity::{EntityStore, InputRow, SourceOrder};
use crate::error::DedupError;
use crate::progress::ProgressObserver;
use arrow_array::{Array, LargeStringArray, RecordBatch, StringArray};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use std::fs::File;
use std::path::{Path, PathBuf};

pub const REQUIRED_COLUMNS: [&str; 7] = [
    "chain",
    "contract_address",
    "token_id",
    "name_norm",
    "token_uri_norm",
    "image_uri_norm",
    "metadata_json",
];

#[derive(Clone, Debug)]
struct ValidatedInput {
    path: PathBuf,
    file_ordinal: u32,
    row_group_count: usize,
    root_projection: Vec<usize>,
    metadata: ArrowReaderMetadata,
    row_count: u64,
}

/// Scan Parquet files with Arrow, projecting required columns and building
/// entities online. No DuckDB staging.
pub fn load_entities(
    input_files: &[PathBuf],
    progress: &dyn ProgressObserver,
) -> Result<EntityStore, DedupError> {
    if input_files.is_empty() {
        return Err(DedupError::invalid("load", "at least one --input is required"));
    }
    progress.set_stage("load");
    progress.set_phase("validate");
    let inputs = validate_inputs(input_files)?;
    let total_rows: u64 = inputs.iter().map(|input| input.row_count).sum();
    progress.set_total(Some(total_rows));
    progress.set_phase("scan");

    let mut store = EntityStore::default();
    for input in &inputs {
        scan_file(input, &mut store, progress)?;
    }
    progress.set_phase("done");
    Ok(store)
}

fn validate_inputs(input_files: &[PathBuf]) -> Result<Vec<ValidatedInput>, DedupError> {
    input_files
        .iter()
        .enumerate()
        .map(|(ordinal, path)| {
            let file_ordinal = u32::try_from(ordinal).map_err(|_| {
                DedupError::invalid("load", "too many input files for file_ordinal")
            })?;
            let file = File::open(path).map_err(|error| DedupError::ParquetRead {
                path: path.clone(),
                message: error.to_string(),
            })?;
            let metadata = ArrowReaderMetadata::load(&file, ArrowReaderOptions::new())
                .map_err(|error| DedupError::ParquetSchema {
                    path: path.clone(),
                    message: error.to_string(),
                })?;
            let schema = metadata.schema();
            let mut root_projection = Vec::with_capacity(REQUIRED_COLUMNS.len());
            for required in REQUIRED_COLUMNS {
                let Some((index, field)) = schema
                    .fields()
                    .iter()
                    .enumerate()
                    .find(|(_, field)| field.name() == required)
                else {
                    return Err(DedupError::ParquetSchema {
                        path: path.clone(),
                        message: format!("missing required column `{required}`"),
                    });
                };
                match field.data_type() {
                    arrow_schema::DataType::Utf8 | arrow_schema::DataType::LargeUtf8 => {}
                    other => {
                        return Err(DedupError::ParquetSchema {
                            path: path.clone(),
                            message: format!(
                                "column `{required}` must be Utf8 or LargeUtf8 (got {other:?}); \
                                 re-export or cast before loading"
                            ),
                        });
                    }
                }
                root_projection.push(index);
            }
            let row_count = metadata.metadata().file_metadata().num_rows().max(0) as u64;
            Ok(ValidatedInput {
                path: path.clone(),
                file_ordinal,
                row_group_count: metadata.metadata().num_row_groups(),
                root_projection,
                metadata,
                row_count,
            })
        })
        .collect()
}

fn scan_file(
    input: &ValidatedInput,
    store: &mut EntityStore,
    progress: &dyn ProgressObserver,
) -> Result<(), DedupError> {
    let file = File::open(&input.path).map_err(|error| DedupError::ParquetRead {
        path: input.path.clone(),
        message: error.to_string(),
    })?;
    let mut file_row_number = 0_u64;
    for row_group in 0..input.row_group_count {
        progress.check_cancelled()?;
        let mask = ProjectionMask::roots(
            input.metadata.metadata().file_metadata().schema_descr(),
            input.root_projection.iter().copied(),
        );
        let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(
            file.try_clone().map_err(|error| DedupError::ParquetRead {
                path: input.path.clone(),
                message: error.to_string(),
            })?,
            input.metadata.clone(),
        )
        .with_projection(mask)
        .with_row_groups(vec![row_group])
        .with_batch_size(8 * 1024)
        .build()
        .map_err(|error| DedupError::ParquetRead {
            path: input.path.clone(),
            message: error.to_string(),
        })?;
        for batch in reader {
            let batch = batch.map_err(|error| DedupError::ParquetRead {
                path: input.path.clone(),
                message: error.to_string(),
            })?;
            let columns = ProjectedColumns::new(&batch, &input.path)?;
            for row_index in 0..batch.num_rows() {
                let row = columns.decode(
                    row_index,
                    SourceOrder {
                        file_ordinal: input.file_ordinal,
                        file_row_number,
                    },
                )?;
                store.ingest_row(row);
                file_row_number += 1;
            }
            progress.add_completed(batch.num_rows() as u64);
        }
    }
    Ok(())
}

struct ProjectedColumns<'a> {
    chain: StringCol<'a>,
    contract_address: StringCol<'a>,
    token_id: StringCol<'a>,
    name_norm: StringCol<'a>,
    token_uri_norm: StringCol<'a>,
    image_uri_norm: StringCol<'a>,
    metadata_json: StringCol<'a>,
}

impl<'a> ProjectedColumns<'a> {
    fn new(batch: &'a RecordBatch, path: &'a Path) -> Result<Self, DedupError> {
        Ok(Self {
            chain: StringCol::from_array(batch.column(0), path, "chain")?,
            contract_address: StringCol::from_array(batch.column(1), path, "contract_address")?,
            token_id: StringCol::from_array(batch.column(2), path, "token_id")?,
            name_norm: StringCol::from_array(batch.column(3), path, "name_norm")?,
            token_uri_norm: StringCol::from_array(batch.column(4), path, "token_uri_norm")?,
            image_uri_norm: StringCol::from_array(batch.column(5), path, "image_uri_norm")?,
            metadata_json: StringCol::from_array(batch.column(6), path, "metadata_json")?,
        })
    }

    fn decode(&self, row_index: usize, source_order: SourceOrder) -> Result<InputRow, DedupError> {
        Ok(InputRow {
            chain: normalize_chain(self.chain.value(row_index)),
            contract_address: self.contract_address.value(row_index).trim().to_owned(),
            token_id: self.token_id.value(row_index).trim().to_owned(),
            name_norm: coalesce(self.name_norm.value(row_index)),
            token_uri_norm: coalesce(self.token_uri_norm.value(row_index)),
            image_uri_norm: coalesce(self.image_uri_norm.value(row_index)),
            metadata_json: coalesce_metadata(self.metadata_json.value(row_index)),
            source_order,
        })
    }
}

enum StringCol<'a> {
    Utf8(&'a StringArray),
    Large(&'a LargeStringArray),
}

impl<'a> StringCol<'a> {
    fn from_array(
        array: &'a dyn Array,
        path: &Path,
        column: &str,
    ) -> Result<Self, DedupError> {
        if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
            Ok(Self::Utf8(values))
        } else if let Some(values) = array.as_any().downcast_ref::<LargeStringArray>() {
            Ok(Self::Large(values))
        } else {
            Err(DedupError::ParquetSchema {
                path: path.to_path_buf(),
                message: format!("column `{column}` is not a string array"),
            })
        }
    }

    fn value(&self, index: usize) -> &str {
        match self {
            Self::Utf8(array) => {
                if array.is_null(index) {
                    ""
                } else {
                    array.value(index)
                }
            }
            Self::Large(array) => {
                if array.is_null(index) {
                    ""
                } else {
                    array.value(index)
                }
            }
        }
    }
}

fn normalize_chain(raw: &str) -> String {
    raw.trim().to_ascii_lowercase()
}

fn coalesce(raw: &str) -> String {
    raw.trim().to_owned()
}

fn coalesce_metadata(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        String::new()
    } else {
        trimmed.to_owned()
    }
}
