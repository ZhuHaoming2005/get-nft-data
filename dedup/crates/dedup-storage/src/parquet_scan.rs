use arrow_array::{Array, LargeStringArray, RecordBatch, StringArray};
use arrow_schema::{DataType, SchemaRef};
use dedup_model::{
    DedupError, ErrorContext, InputRow, NoopProgress, ProgressObserver, SourceOrder, StageCounters,
};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{SyncSender, channel, sync_channel};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

pub const REQUIRED_COLUMNS: [&str; 7] = [
    "chain",
    "contract_address",
    "token_id",
    "name_norm",
    "token_uri_norm",
    "image_uri_norm",
    "metadata_json",
];

pub trait ParquetRowSink {
    fn push(&mut self, row: InputRow) -> Result<(), DedupError>;
}

impl<F> ParquetRowSink for F
where
    F: FnMut(InputRow) -> Result<(), DedupError>,
{
    fn push(&mut self, row: InputRow) -> Result<(), DedupError> {
        self(row)
    }
}

#[derive(Clone, Debug)]
pub struct ValidatedParquetInput {
    pub path: PathBuf,
    pub file_ordinal: u32,
    pub row_group_count: usize,
    pub row_count: u64,
    root_projection: Vec<usize>,
    metadata: ArrowReaderMetadata,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParquetScanResult {
    pub logical_input_digest: String,
    pub schema_digest: String,
    pub row_group_digests: Vec<String>,
    pub rows_scanned: u64,
}

pub trait WorkerThreadSetup: Send + Sync {
    fn setup(&self, worker_index: usize) -> Result<(), DedupError>;
}

#[derive(Clone)]
pub struct ParallelScanConfig {
    pub workers: usize,
    pub queue_batches_per_worker: usize,
    pub batch_size: usize,
    worker_setup: Option<Arc<dyn WorkerThreadSetup>>,
}

impl ParallelScanConfig {
    pub fn new(
        workers: usize,
        queue_batches_per_worker: usize,
        batch_size: usize,
    ) -> Result<Self, DedupError> {
        if workers == 0 || queue_batches_per_worker == 0 || batch_size == 0 {
            return Err(DedupError::InvalidInput {
                context: ErrorContext::stage("parquet_scan"),
                message: "parallel Parquet capacities must be positive".to_owned(),
            });
        }
        Ok(Self {
            workers,
            queue_batches_per_worker,
            batch_size,
            worker_setup: None,
        })
    }

    pub fn with_worker_setup(mut self, setup: Arc<dyn WorkerThreadSetup>) -> Self {
        self.worker_setup = Some(setup);
        self
    }
}

pub fn validate_parquet_inputs(
    input_files: &[String],
) -> Result<Vec<ValidatedParquetInput>, DedupError> {
    if input_files.is_empty() {
        return Err(DedupError::InvalidInput {
            context: ErrorContext::stage("parquet_schema"),
            message: "input_files must be explicitly configured".to_owned(),
        });
    }
    input_files
        .iter()
        .enumerate()
        .map(|(file_ordinal, configured_path)| {
            let file_ordinal =
                u32::try_from(file_ordinal).map_err(|_| DedupError::InvalidInput {
                    context: ErrorContext::stage("parquet_schema"),
                    message: "too many input files".to_owned(),
                })?;
            let path = PathBuf::from(configured_path);
            let file = File::open(&path)?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)
                .map_err(|error| schema_error(&path, error.to_string()))?;
            let root_projection = validate_schema(&path, builder.schema())?;
            let metadata =
                ArrowReaderMetadata::try_new(builder.metadata().clone(), ArrowReaderOptions::new())
                    .map_err(|error| schema_error(&path, error.to_string()))?;
            let row_count =
                u64::try_from(builder.metadata().file_metadata().num_rows()).map_err(|_| {
                    schema_error(
                        &path,
                        "Parquet row count does not fit the work-counter representation".to_owned(),
                    )
                })?;
            Ok(ValidatedParquetInput {
                path,
                file_ordinal,
                row_group_count: builder.metadata().num_row_groups(),
                row_count,
                root_projection,
                metadata,
            })
        })
        .collect()
}

pub fn scan_parquet_inputs(
    inputs: &[ValidatedParquetInput],
    allowed_chains: &BTreeSet<String>,
    evm_chains: &BTreeSet<String>,
    sink: &mut impl ParquetRowSink,
    counters: &mut StageCounters,
) -> Result<ParquetScanResult, DedupError> {
    scan_parquet_inputs_with_progress(
        inputs,
        allowed_chains,
        evm_chains,
        sink,
        counters,
        &NoopProgress,
    )
}

pub fn scan_parquet_inputs_with_progress(
    inputs: &[ValidatedParquetInput],
    allowed_chains: &BTreeSet<String>,
    evm_chains: &BTreeSet<String>,
    sink: &mut impl ParquetRowSink,
    counters: &mut StageCounters,
    progress: &dyn ProgressObserver,
) -> Result<ParquetScanResult, DedupError> {
    let schema_digest = digest_schema(inputs)?;
    let mut logical_digest = Sha256::new();
    logical_digest.update(schema_digest);
    let mut row_group_digests = Vec::new();
    let mut rows_scanned = 0_u64;

    for input in inputs {
        let file = File::open(&input.path)?;
        let mut file_row_number = 0_u64;
        for row_group in 0..input.row_group_count {
            let mask = ProjectionMask::roots(
                input.metadata.metadata().file_metadata().schema_descr(),
                input.root_projection.iter().copied(),
            );
            let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(
                file.try_clone()?,
                input.metadata.clone(),
            )
            .with_projection(mask)
            .with_row_groups(vec![row_group])
            .with_batch_size(8 * 1024)
            .build()
            .map_err(|error| schema_error(&input.path, error.to_string()))?;
            let mut row_group_digest = Sha256::new();
            for batch in reader {
                let batch = batch.map_err(|error| schema_error(&input.path, error.to_string()))?;
                let batch_rows =
                    u64::try_from(batch.num_rows()).map_err(|_| DedupError::CounterOverflow {
                        counter: "parquet_batch_rows",
                    })?;
                let columns = ProjectedColumns::new(&batch)?;
                for row_index in 0..batch.num_rows() {
                    let row = decode_row(
                        &columns,
                        row_index,
                        SourceOrder::new(input.file_ordinal, file_row_number),
                        allowed_chains,
                        evm_chains,
                    )?;
                    update_row_digest(&mut row_group_digest, &row);
                    sink.push(row)?;
                    file_row_number =
                        file_row_number
                            .checked_add(1)
                            .ok_or(DedupError::CounterOverflow {
                                counter: "file_row_number",
                            })?;
                    rows_scanned =
                        rows_scanned
                            .checked_add(1)
                            .ok_or(DedupError::CounterOverflow {
                                counter: "rows_scanned",
                            })?;
                    counters.rows_scanned(1)?;
                }
                progress.advance(batch_rows);
                progress.check_cancelled("parquet_scan")?;
            }
            let digest: [u8; 32] = row_group_digest.finalize().into();
            logical_digest.update(digest);
            row_group_digests.push(hex(&digest));
        }
    }
    let logical_input_digest: [u8; 32] = logical_digest.finalize().into();
    Ok(ParquetScanResult {
        logical_input_digest: hex(&logical_input_digest),
        schema_digest: hex(&schema_digest),
        row_group_digests,
        rows_scanned,
    })
}

pub fn scan_parquet_inputs_parallel(
    inputs: &[ValidatedParquetInput],
    allowed_chains: &BTreeSet<String>,
    evm_chains: &BTreeSet<String>,
    sink: &mut impl ParquetRowSink,
    counters: &mut StageCounters,
    progress: &dyn ProgressObserver,
    config: ParallelScanConfig,
) -> Result<ParquetScanResult, DedupError> {
    if config.workers <= 1
        || inputs
            .iter()
            .map(|input| input.row_group_count)
            .sum::<usize>()
            <= 1
    {
        return scan_parquet_inputs_with_progress(
            inputs,
            allowed_chains,
            evm_chains,
            sink,
            counters,
            progress,
        );
    }
    let tasks = build_row_group_tasks(inputs)?;
    let schema_digest = digest_schema(inputs)?;
    let next_task = AtomicUsize::new(0);
    let channels: Vec<_> = (0..tasks.len())
        .map(|_| sync_channel(config.queue_batches_per_worker))
        .collect();
    let (senders, receivers): (Vec<_>, Vec<_>) = channels.into_iter().unzip();
    thread::scope(|scope| {
        let worker_count = config.workers.min(tasks.len());
        let startup = Arc::new((Mutex::new((false, false)), Condvar::new()));
        let (setup_sender, setup_receiver) = channel();
        for worker in 0..worker_count {
            let next_task = &next_task;
            let tasks = &tasks;
            let senders = &senders;
            let worker_startup = Arc::clone(&startup);
            let setup_sender = setup_sender.clone();
            let worker_setup = config.worker_setup.clone();
            if let Err(error) = thread::Builder::new()
                .name(format!("dedup-parquet-{worker}"))
                .spawn_scoped(scope, move || {
                    let setup_result = worker_setup
                        .as_ref()
                        .map_or(Ok(()), |setup| setup.setup(worker));
                    let _ = setup_sender.send(setup_result);
                    let (lock, ready) = &*worker_startup;
                    let mut state = lock.lock().unwrap_or_else(|error| error.into_inner());
                    while !state.0 {
                        state = ready.wait(state).unwrap_or_else(|error| error.into_inner());
                    }
                    if state.1 {
                        return;
                    }
                    drop(state);
                    loop {
                        let task_index = next_task.fetch_add(1, Ordering::Relaxed);
                        let Some(task) = tasks.get(task_index) else {
                            break;
                        };
                        if decode_row_group_parallel(
                            &inputs[task.input_index],
                            task,
                            allowed_chains,
                            evm_chains,
                            &senders[task_index],
                            config.batch_size,
                        )
                        .is_err()
                        {
                            break;
                        }
                    }
                })
            {
                let (lock, ready) = &*startup;
                let mut state = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                *state = (true, true);
                ready.notify_all();
                return Err(DedupError::Io(error));
            }
        }
        drop(setup_sender);
        let mut setup_error = None;
        for _ in 0..worker_count {
            match setup_receiver.recv() {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    if setup_error.is_none() {
                        setup_error = Some(error);
                    }
                }
                Err(error) => {
                    if setup_error.is_none() {
                        setup_error = Some(DedupError::InvariantViolation {
                            context: ErrorContext::stage("parquet_scan"),
                            message: format!("worker setup channel closed early: {error}"),
                        });
                    }
                }
            }
        }
        {
            let (lock, ready) = &*startup;
            let mut state = lock.lock().unwrap_or_else(|error| error.into_inner());
            *state = (true, setup_error.is_some());
            ready.notify_all();
        }
        if let Some(error) = setup_error {
            return Err(error);
        }

        let mut logical_digest = Sha256::new();
        logical_digest.update(schema_digest);
        let mut row_group_digests = Vec::with_capacity(tasks.len());
        let mut rows_scanned = 0_u64;
        for (task, receiver) in tasks.iter().zip(receivers) {
            loop {
                match receiver
                    .recv()
                    .map_err(|_| DedupError::InvariantViolation {
                        context: ErrorContext {
                            stage: "parquet_scan",
                            partition: Some(u64::try_from(task.global_index).unwrap_or(u64::MAX)),
                            stable_object_id: None,
                        },
                        message: "parallel Parquet worker ended without a terminal message"
                            .to_owned(),
                    })? {
                    RowGroupMessage::Batch(rows) => {
                        let batch_rows =
                            u64::try_from(rows.len()).map_err(|_| DedupError::CounterOverflow {
                                counter: "parquet_batch_rows",
                            })?;
                        for row in rows {
                            sink.push(row)?;
                        }
                        rows_scanned = rows_scanned.checked_add(batch_rows).ok_or(
                            DedupError::CounterOverflow {
                                counter: "rows_scanned",
                            },
                        )?;
                        counters.rows_scanned(batch_rows)?;
                        progress.advance(batch_rows);
                        progress.check_cancelled("parquet_scan")?;
                    }
                    RowGroupMessage::Complete { digest, rows } => {
                        if rows != task.row_count {
                            return Err(DedupError::InvariantViolation {
                                context: ErrorContext {
                                    stage: "parquet_scan",
                                    partition: Some(
                                        u64::try_from(task.global_index).unwrap_or(u64::MAX),
                                    ),
                                    stable_object_id: None,
                                },
                                message: format!(
                                    "row group decoded {rows} rows but metadata declared {}",
                                    task.row_count
                                ),
                            });
                        }
                        logical_digest.update(digest);
                        row_group_digests.push(hex(&digest));
                        break;
                    }
                    RowGroupMessage::Failed(error) => return Err(error),
                }
            }
        }
        let logical_input_digest: [u8; 32] = logical_digest.finalize().into();
        Ok(ParquetScanResult {
            logical_input_digest: hex(&logical_input_digest),
            schema_digest: hex(&schema_digest),
            row_group_digests,
            rows_scanned,
        })
    })
}

#[derive(Clone, Copy, Debug)]
struct RowGroupTask {
    global_index: usize,
    input_index: usize,
    row_group: usize,
    first_file_row: u64,
    row_count: u64,
}

enum RowGroupMessage {
    Batch(Vec<InputRow>),
    Complete { digest: [u8; 32], rows: u64 },
    Failed(DedupError),
}

fn build_row_group_tasks(
    inputs: &[ValidatedParquetInput],
) -> Result<Vec<RowGroupTask>, DedupError> {
    let mut tasks = Vec::new();
    for (input_index, input) in inputs.iter().enumerate() {
        let mut first_file_row = 0_u64;
        for row_group in 0..input.row_group_count {
            let raw_count = input.metadata.metadata().row_group(row_group).num_rows();
            let row_count = u64::try_from(raw_count).map_err(|_| {
                schema_error(
                    &input.path,
                    "row-group count does not fit the work-counter representation".to_owned(),
                )
            })?;
            tasks.push(RowGroupTask {
                global_index: tasks.len(),
                input_index,
                row_group,
                first_file_row,
                row_count,
            });
            first_file_row =
                first_file_row
                    .checked_add(row_count)
                    .ok_or(DedupError::CounterOverflow {
                        counter: "file_row_number",
                    })?;
        }
    }
    Ok(tasks)
}

fn decode_row_group_parallel(
    input: &ValidatedParquetInput,
    task: &RowGroupTask,
    allowed_chains: &BTreeSet<String>,
    evm_chains: &BTreeSet<String>,
    sender: &SyncSender<RowGroupMessage>,
    batch_size: usize,
) -> Result<(), ()> {
    let result = (|| {
        let file = File::open(&input.path)?;
        let mask = ProjectionMask::roots(
            input.metadata.metadata().file_metadata().schema_descr(),
            input.root_projection.iter().copied(),
        );
        let reader =
            ParquetRecordBatchReaderBuilder::new_with_metadata(file, input.metadata.clone())
                .with_projection(mask)
                .with_row_groups(vec![task.row_group])
                .with_batch_size(batch_size)
                .build()
                .map_err(|error| schema_error(&input.path, error.to_string()))?;
        let mut digest = Sha256::new();
        let mut file_row_number = task.first_file_row;
        let mut rows = 0_u64;
        for batch in reader {
            let batch = batch.map_err(|error| schema_error(&input.path, error.to_string()))?;
            let columns = ProjectedColumns::new(&batch)?;
            let mut decoded = Vec::with_capacity(batch.num_rows());
            for row_index in 0..batch.num_rows() {
                let row = decode_row(
                    &columns,
                    row_index,
                    SourceOrder::new(input.file_ordinal, file_row_number),
                    allowed_chains,
                    evm_chains,
                )?;
                update_row_digest(&mut digest, &row);
                decoded.push(row);
                file_row_number =
                    file_row_number
                        .checked_add(1)
                        .ok_or(DedupError::CounterOverflow {
                            counter: "file_row_number",
                        })?;
                rows = rows.checked_add(1).ok_or(DedupError::CounterOverflow {
                    counter: "rows_scanned",
                })?;
            }
            sender.send(RowGroupMessage::Batch(decoded)).map_err(|_| {
                DedupError::InvariantViolation {
                    context: ErrorContext::stage("parquet_scan"),
                    message: "parallel Parquet coordinator stopped".to_owned(),
                }
            })?;
        }
        let digest: [u8; 32] = digest.finalize().into();
        sender
            .send(RowGroupMessage::Complete { digest, rows })
            .map_err(|_| DedupError::InvariantViolation {
                context: ErrorContext::stage("parquet_scan"),
                message: "parallel Parquet coordinator stopped".to_owned(),
            })
    })();
    match result {
        Ok(()) => Ok(()),
        Err(error) => {
            let _ = sender.send(RowGroupMessage::Failed(error));
            Err(())
        }
    }
}

fn validate_schema(path: &Path, schema: &SchemaRef) -> Result<Vec<usize>, DedupError> {
    REQUIRED_COLUMNS
        .iter()
        .map(|name| {
            let (index, field) = schema
                .column_with_name(name)
                .ok_or_else(|| schema_error(path, format!("required field {name:?} is missing")))?;
            if !matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8) {
                return Err(schema_error(
                    path,
                    format!(
                        "field {name:?} must be uniformly castable to UTF-8, got {:?}",
                        field.data_type()
                    ),
                ));
            }
            Ok(index)
        })
        .collect()
}

fn digest_schema(inputs: &[ValidatedParquetInput]) -> Result<[u8; 32], DedupError> {
    let mut digest = Sha256::new();
    for input in inputs {
        let schema = input.metadata.schema();
        for name in REQUIRED_COLUMNS {
            let (_, field) = schema
                .column_with_name(name)
                .ok_or_else(|| schema_error(&input.path, format!("missing field {name}")))?;
            update_part(&mut digest, name.as_bytes());
            update_part(&mut digest, format!("{:?}", field.data_type()).as_bytes());
            digest.update([u8::from(field.is_nullable())]);
        }
    }
    Ok(digest.finalize().into())
}

struct ProjectedColumns<'a> {
    values: [StringColumn<'a>; 7],
}

impl<'a> ProjectedColumns<'a> {
    fn new(batch: &'a RecordBatch) -> Result<Self, DedupError> {
        let schema = batch.schema();
        let values = REQUIRED_COLUMNS.map(|name| {
            let index = schema
                .index_of(name)
                .map_err(|error| DedupError::SchemaMismatch {
                    context: ErrorContext::stage("parquet_decode"),
                    message: error.to_string(),
                })?;
            StringColumn::new(batch.column(index).as_ref(), name)
        });
        Ok(Self {
            values: values
                .into_iter()
                .collect::<Result<Vec<_>, _>>()?
                .try_into()
                .expect("required column count is fixed"),
        })
    }

    fn value(&self, column: usize, row: usize) -> &str {
        self.values[column].value(row).unwrap_or("")
    }
}

#[derive(Debug)]
enum StringColumn<'a> {
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
}

impl<'a> StringColumn<'a> {
    fn new(array: &'a dyn Array, name: &str) -> Result<Self, DedupError> {
        if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
            Ok(Self::Utf8(array))
        } else if let Some(array) = array.as_any().downcast_ref::<LargeStringArray>() {
            Ok(Self::LargeUtf8(array))
        } else {
            Err(DedupError::SchemaMismatch {
                context: ErrorContext::stage("parquet_decode"),
                message: format!("projected field {name:?} is not UTF-8"),
            })
        }
    }

    fn value(&self, row: usize) -> Option<&str> {
        match self {
            Self::Utf8(array) => (!array.is_null(row)).then(|| array.value(row)),
            Self::LargeUtf8(array) => (!array.is_null(row)).then(|| array.value(row)),
        }
    }
}

fn decode_row(
    columns: &ProjectedColumns<'_>,
    row_index: usize,
    source_order: SourceOrder,
    allowed_chains: &BTreeSet<String>,
    evm_chains: &BTreeSet<String>,
) -> Result<InputRow, DedupError> {
    let chain = columns.value(0, row_index).trim().to_lowercase();
    if chain.is_empty() || !allowed_chains.contains(&chain) {
        return Err(DedupError::InvalidInput {
            context: ErrorContext {
                stage: "parquet_decode",
                partition: Some(u64::from(source_order.file_ordinal)),
                stable_object_id: Some(source_order.file_row_number),
            },
            message: format!("unknown or empty chain {chain:?}"),
        });
    }
    let mut contract_address = columns.value(1, row_index).trim().to_owned();
    let token_id = columns.value(2, row_index).trim().to_owned();
    if contract_address.is_empty() || token_id.is_empty() {
        return Err(DedupError::InvalidInput {
            context: ErrorContext {
                stage: "parquet_decode",
                partition: Some(u64::from(source_order.file_ordinal)),
                stable_object_id: Some(source_order.file_row_number),
            },
            message: "empty logical primary key".to_owned(),
        });
    }
    if evm_chains.contains(&chain) {
        contract_address.make_ascii_lowercase();
    }
    Ok(InputRow {
        chain,
        contract_address,
        token_id,
        name_norm: columns.value(3, row_index).to_owned(),
        token_uri_norm: columns.value(4, row_index).to_owned(),
        image_uri_norm: columns.value(5, row_index).to_owned(),
        metadata_json: columns.value(6, row_index).trim().to_owned(),
        source_order,
    })
}

fn update_row_digest(digest: &mut Sha256, row: &InputRow) {
    for field in [
        &row.chain,
        &row.contract_address,
        &row.token_id,
        &row.name_norm,
        &row.token_uri_norm,
        &row.image_uri_norm,
        &row.metadata_json,
    ] {
        update_part(digest, field.as_bytes());
    }
}

fn update_part(digest: &mut Sha256, bytes: &[u8]) {
    digest.update((bytes.len() as u64).to_le_bytes());
    digest.update(bytes);
}

fn schema_error(path: &Path, message: String) -> DedupError {
    DedupError::SchemaMismatch {
        context: ErrorContext::stage("parquet_schema"),
        message: format!("{}: {message}", path.display()),
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, Int32Array};
    use arrow_schema::{Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    struct CountingSetup(AtomicUsize);

    impl WorkerThreadSetup for CountingSetup {
        fn setup(&self, _worker_index: usize) -> Result<(), DedupError> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    struct FailingSetup;

    impl WorkerThreadSetup for FailingSetup {
        fn setup(&self, worker_index: usize) -> Result<(), DedupError> {
            (worker_index != 1)
                .then_some(())
                .ok_or_else(|| DedupError::PlatformCapabilityMissing {
                    capability: "test worker binding".to_owned(),
                })
        }
    }

    fn write_fixture(path: &Path) {
        let schema = Arc::new(Schema::new(
            REQUIRED_COLUMNS
                .iter()
                .map(|name| Field::new(*name, DataType::Utf8, true))
                .chain([Field::new("unused", DataType::Int32, false)])
                .collect::<Vec<_>>(),
        ));
        let text = |values: Vec<Option<&str>>| Arc::new(StringArray::from(values)) as ArrayRef;
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                text(vec![Some(" Ethereum "), Some("solana"), Some("ethereum")]),
                text(vec![Some(" 0xABC "), Some("SoL"), Some("0xDEF")]),
                text(vec![Some("1"), Some("mint"), Some("2")]),
                text(vec![Some("Name"), None, Some("Name2")]),
                text(vec![Some("uri"), None, Some("uri2")]),
                text(vec![None, Some("image"), None]),
                text(vec![Some(r#"{"x":1}"#), None, Some(r#"{"x":2}"#)]),
                Arc::new(Int32Array::from(vec![1, 2, 3])),
            ],
        )
        .unwrap();
        let properties = WriterProperties::builder()
            .set_max_row_group_size(2)
            .build();
        let file = File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn write_single_row_fixture(path: &Path, values: [&str; 7]) {
        let schema = Arc::new(Schema::new(
            REQUIRED_COLUMNS
                .iter()
                .map(|name| Field::new(*name, DataType::Utf8, true))
                .collect::<Vec<_>>(),
        ));
        let columns = values
            .into_iter()
            .map(|value| Arc::new(StringArray::from(vec![Some(value)])) as ArrayRef)
            .collect();
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
        let file = File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[test]
    fn wrong_required_column_type_is_a_typed_schema_error() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("wrong-type.parquet");
        let schema = Arc::new(Schema::new(
            REQUIRED_COLUMNS
                .iter()
                .enumerate()
                .map(|(index, name)| {
                    Field::new(
                        *name,
                        if index == 2 {
                            DataType::Int32
                        } else {
                            DataType::Utf8
                        },
                        true,
                    )
                })
                .collect::<Vec<_>>(),
        ));
        let columns = (0..REQUIRED_COLUMNS.len())
            .map(|index| {
                if index == 2 {
                    Arc::new(Int32Array::from(vec![1])) as ArrayRef
                } else {
                    Arc::new(StringArray::from(vec![Some("x")])) as ArrayRef
                }
            })
            .collect();
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
        let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        assert!(matches!(
            validate_parquet_inputs(&[path.to_string_lossy().into_owned()]),
            Err(DedupError::SchemaMismatch { .. })
        ));
    }

    #[test]
    fn empty_logical_primary_key_is_a_typed_input_error() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("empty-key.parquet");
        write_single_row_fixture(
            &path,
            [
                "ethereum",
                "",
                "1",
                "name",
                "token-uri",
                "image-uri",
                r#"{"x":1}"#,
            ],
        );
        let inputs = validate_parquet_inputs(&[path.to_string_lossy().into_owned()]).unwrap();
        let error = scan_parquet_inputs(
            &inputs,
            &BTreeSet::from(["ethereum".to_owned()]),
            &BTreeSet::from(["ethereum".to_owned()]),
            &mut |_| Ok(()),
            &mut StageCounters::default(),
        )
        .unwrap_err();
        assert!(matches!(error, DedupError::InvalidInput { .. }));
    }

    #[test]
    fn schema_is_validated_before_single_projected_scan() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("fixture.parquet");
        write_fixture(&path);
        let configured = vec![path.to_string_lossy().into_owned()];
        let inputs = validate_parquet_inputs(&configured).unwrap();
        assert_eq!(inputs[0].row_group_count, 2);
        let mut rows = Vec::new();
        let mut sink = |row| {
            rows.push(row);
            Ok(())
        };
        let result = scan_parquet_inputs(
            &inputs,
            &BTreeSet::from(["ethereum".to_owned(), "solana".to_owned()]),
            &BTreeSet::from(["ethereum".to_owned()]),
            &mut sink,
            &mut StageCounters::default(),
        )
        .unwrap();
        assert_eq!(result.rows_scanned, 3);
        assert_eq!(result.row_group_digests.len(), 2);
        assert_eq!(rows[0].source_order, SourceOrder::new(0, 0));
        assert_eq!(rows[2].source_order, SourceOrder::new(0, 2));
        assert_eq!(rows[0].contract_address, "0xabc");
        assert_eq!(rows[1].contract_address, "SoL");
        assert_eq!(rows[1].name_norm, "");
    }

    #[test]
    fn logical_digest_is_deterministic() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("fixture.parquet");
        write_fixture(&path);
        let inputs = validate_parquet_inputs(&[path.to_string_lossy().into_owned()]).unwrap();
        let scan = || {
            scan_parquet_inputs(
                &inputs,
                &BTreeSet::from(["ethereum".to_owned(), "solana".to_owned()]),
                &BTreeSet::from(["ethereum".to_owned()]),
                &mut |_| Ok(()),
                &mut StageCounters::default(),
            )
            .unwrap()
        };
        assert_eq!(scan(), scan());
    }

    #[test]
    fn parallel_row_groups_preserve_source_order_and_digest() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("fixture.parquet");
        write_fixture(&path);
        let inputs = validate_parquet_inputs(&[path.to_string_lossy().into_owned()]).unwrap();
        let allowed = BTreeSet::from(["ethereum".to_owned(), "solana".to_owned()]);
        let evm = BTreeSet::from(["ethereum".to_owned()]);
        let mut sequential_rows = Vec::new();
        let sequential = scan_parquet_inputs(
            &inputs,
            &allowed,
            &evm,
            &mut |row| {
                sequential_rows.push(row);
                Ok(())
            },
            &mut StageCounters::default(),
        )
        .unwrap();
        let mut parallel_rows = Vec::new();
        let setup = Arc::new(CountingSetup(AtomicUsize::new(0)));
        let parallel = scan_parquet_inputs_parallel(
            &inputs,
            &allowed,
            &evm,
            &mut |row| {
                parallel_rows.push(row);
                Ok(())
            },
            &mut StageCounters::default(),
            &NoopProgress,
            ParallelScanConfig::new(2, 1, 1_024)
                .unwrap()
                .with_worker_setup(setup.clone()),
        )
        .unwrap();
        assert_eq!(setup.0.load(Ordering::Relaxed), 2);
        assert_eq!(parallel, sequential);
        assert_eq!(parallel_rows, sequential_rows);
    }

    #[test]
    fn worker_setup_failure_stops_before_rows_are_emitted() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("fixture.parquet");
        write_fixture(&path);
        let inputs = validate_parquet_inputs(&[path.to_string_lossy().into_owned()]).unwrap();
        let mut rows = 0_u64;
        let result = scan_parquet_inputs_parallel(
            &inputs,
            &BTreeSet::from(["ethereum".to_owned(), "solana".to_owned()]),
            &BTreeSet::from(["ethereum".to_owned()]),
            &mut |_| {
                rows += 1;
                Ok(())
            },
            &mut StageCounters::default(),
            &NoopProgress,
            ParallelScanConfig::new(2, 1, 1_024)
                .unwrap()
                .with_worker_setup(Arc::new(FailingSetup)),
        );
        assert!(matches!(
            result,
            Err(DedupError::PlatformCapabilityMissing { .. })
        ));
        assert_eq!(rows, 0);
    }
}
