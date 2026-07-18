use dedup_model::{DedupError, EntityId, ErrorContext, ProgressObserver, StringId};
use dedup_storage::SpillVolume;
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use tempfile::NamedTempFile;

const NONE_ID: u64 = u64::MAX;
const IO_BUFFER_BYTES: usize = 1024 * 1024;

#[derive(Debug)]
pub(crate) struct ExternalStringStore {
    file: NamedTempFile,
    writer: Option<BufWriter<File>>,
    occurrences: u64,
    payload_bytes: u64,
}

#[derive(Debug)]
pub(crate) struct ExternalStringDictionary {
    pub(crate) offsets: NamedTempFile,
    pub(crate) blob: NamedTempFile,
    pub(crate) occurrence_map: NamedTempFile,
    pub(crate) occurrence_count: u64,
    pub(crate) string_count: u64,
    pub(crate) spill_bytes: u64,
    pub(crate) handle_touches: u64,
}

#[derive(Debug)]
pub(crate) struct OccurrenceMapReader {
    reader: BufReader<File>,
    next_occurrence: u64,
    occurrence_count: u64,
}

impl OccurrenceMapReader {
    pub(crate) fn new(dictionary: &ExternalStringDictionary) -> Result<Self, DedupError> {
        Ok(Self {
            reader: BufReader::with_capacity(IO_BUFFER_BYTES, dictionary.occurrence_map.reopen()?),
            next_occurrence: 0,
            occurrence_count: dictionary.occurrence_count,
        })
    }

    pub(crate) fn resolve(
        &mut self,
        occurrence: u64,
    ) -> Result<(StringId, Option<u64>), DedupError> {
        if occurrence != self.next_occurrence {
            return Err(invalid_external_string(format!(
                "string occurrence {occurrence} is out of input order; expected {}",
                self.next_occurrence
            )));
        }
        let string_id = read_u64(&mut self.reader)?;
        let numeric_rank = read_u64(&mut self.reader)?;
        self.next_occurrence =
            self.next_occurrence
                .checked_add(1)
                .ok_or(DedupError::CounterOverflow {
                    counter: "external_string_occurrence",
                })?;
        Ok((
            StringId::new(
                EntityId::try_from(string_id).map_err(|_| DedupError::InvalidInput {
                    context: ErrorContext::stage("entity"),
                    message: "StringId capacity exceeded; rebuild with wide_ids".to_owned(),
                })?,
            ),
            (numeric_rank != NONE_ID).then_some(numeric_rank),
        ))
    }

    pub(crate) fn finish(mut self) -> Result<(), DedupError> {
        if self.next_occurrence != self.occurrence_count {
            return Err(invalid_external_string(format!(
                "consumed {} of {} string occurrences",
                self.next_occurrence, self.occurrence_count
            )));
        }
        let mut trailing = [0_u8; 1];
        if self.reader.read(&mut trailing)? != 0 {
            return Err(invalid_external_string(
                "string occurrence map has trailing bytes".to_owned(),
            ));
        }
        Ok(())
    }
}

impl ExternalStringStore {
    pub(crate) fn new(root: &Path) -> Result<Self, DedupError> {
        std::fs::create_dir_all(root)?;
        let file = NamedTempFile::new_in(root)?;
        let writer = BufWriter::with_capacity(IO_BUFFER_BYTES, file.reopen()?);
        Ok(Self {
            file,
            writer: Some(writer),
            occurrences: 0,
            payload_bytes: 0,
        })
    }

    pub(crate) fn store(&mut self, bytes: &[u8]) -> Result<u64, DedupError> {
        let occurrence = self.occurrences;
        let length = u64::try_from(bytes.len()).map_err(|_| DedupError::CounterOverflow {
            counter: "external_string_length",
        })?;
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| invalid_external_string("string spool is closed".to_owned()))?;
        write_u64(writer, occurrence)?;
        write_u64(writer, length)?;
        writer.write_all(bytes)?;
        self.occurrences = self
            .occurrences
            .checked_add(1)
            .ok_or(DedupError::CounterOverflow {
                counter: "external_string_occurrences",
            })?;
        self.payload_bytes =
            self.payload_bytes
                .checked_add(length)
                .ok_or(DedupError::CounterOverflow {
                    counter: "external_string_payload_bytes",
                })?;
        Ok(occurrence)
    }

    pub(crate) fn finish(
        mut self,
        volumes: &[SpillVolume],
        sort_chunk_records: usize,
        sort_memory_bytes: usize,
        merge_fan_in: usize,
        progress: &dyn ProgressObserver,
    ) -> Result<ExternalStringDictionary, DedupError> {
        validate_capacities(volumes, sort_chunk_records, sort_memory_bytes, merge_fan_in)?;
        if let Some(mut writer) = self.writer.take() {
            writer.flush()?;
            writer.get_ref().sync_all()?;
        }
        let input_bytes = self
            .payload_bytes
            .saturating_add(self.occurrences.saturating_mul(16));
        progress.begin_phase("entity_external_string_sort", Some(self.occurrences));
        let source = self.file.reopen()?;
        let mut runs = create_variable_runs::<StringOccurrence>(
            source,
            self.occurrences,
            volumes,
            "entity-strings",
            sort_chunk_records,
            sort_memory_bytes,
            progress,
        )?;
        let (string_sort_bytes, string_sort_touches) = merge_variable_runs::<StringOccurrence>(
            &mut runs,
            volumes,
            "entity-strings",
            merge_fan_in,
            progress,
        )?;

        progress.begin_phase("entity_external_string_reduce", Some(self.occurrences));
        let root = &volumes[0].root;
        let mut offsets = NamedTempFile::new_in(root)?;
        let blob = NamedTempFile::new_in(root)?;
        let mapping_pairs = NamedTempFile::new_in(root)?;
        let decimal_records = NamedTempFile::new_in(root)?;
        let mut offsets_writer = BufWriter::with_capacity(IO_BUFFER_BYTES, offsets.reopen()?);
        let mut blob_writer = BufWriter::with_capacity(IO_BUFFER_BYTES, blob.reopen()?);
        let mut mapping_writer = BufWriter::with_capacity(IO_BUFFER_BYTES, mapping_pairs.reopen()?);
        let mut decimal_writer =
            BufWriter::with_capacity(IO_BUFFER_BYTES, decimal_records.reopen()?);
        write_u64(&mut offsets_writer, 0)?;
        let mut string_count = 0_u64;
        let mut decimal_count = 0_u64;
        let mut blob_position = 0_u64;
        let mut previous: Option<Vec<u8>> = None;
        let final_run = runs.pop();
        if let Some(run) = final_run {
            let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, File::open(&run.path)?);
            for index in 0..run.records {
                let record = read_variable_record::<StringOccurrence>(&mut reader)?;
                let new_value = previous
                    .as_deref()
                    .is_none_or(|value| value != record.bytes.as_slice());
                if new_value {
                    EntityId::try_from(string_count).map_err(|_| DedupError::InvalidInput {
                        context: ErrorContext::stage("entity"),
                        message: "StringId capacity exceeded; rebuild with wide_ids".to_owned(),
                    })?;
                    write_u64(&mut offsets_writer, blob_position)?;
                    write_u64(
                        &mut offsets_writer,
                        u64::try_from(record.bytes.len()).map_err(|_| {
                            DedupError::CounterOverflow {
                                counter: "external_string_length",
                            }
                        })?,
                    )?;
                    blob_writer.write_all(&record.bytes)?;
                    blob_position = blob_position
                        .checked_add(u64::try_from(record.bytes.len()).map_err(|_| {
                            DedupError::CounterOverflow {
                                counter: "external_string_blob_bytes",
                            }
                        })?)
                        .ok_or(DedupError::CounterOverflow {
                            counter: "external_string_blob_bytes",
                        })?;
                    if is_decimal(&record.bytes) {
                        write_variable_record(
                            &mut decimal_writer,
                            &DecimalString {
                                bytes: record.bytes.clone(),
                                string_id: string_count,
                            },
                        )?;
                        decimal_count =
                            decimal_count
                                .checked_add(1)
                                .ok_or(DedupError::CounterOverflow {
                                    counter: "external_decimal_strings",
                                })?;
                    }
                    previous = Some(record.bytes.clone());
                    string_count =
                        string_count
                            .checked_add(1)
                            .ok_or(DedupError::CounterOverflow {
                                counter: "external_string_count",
                            })?;
                }
                let string_id = string_count.checked_sub(1).ok_or_else(|| {
                    invalid_external_string("string reducer emitted no StringId".to_owned())
                })?;
                write_fixed_record(&mut mapping_writer, [record.occurrence, string_id])?;
                if index % 4_096 == 0 {
                    progress.advance(4_096.min(run.records - index));
                    progress.check_cancelled("entity")?;
                }
            }
            ensure_eof(&mut reader, "string sort run has trailing bytes")?;
            std::fs::remove_file(run.path)?;
        }
        offsets_writer.flush()?;
        blob_writer.flush()?;
        mapping_writer.flush()?;
        decimal_writer.flush()?;
        offsets
            .as_file_mut()
            .write_all_at(&string_count.to_le_bytes(), 0)?;

        let mut decimal_runs = create_variable_runs::<DecimalString>(
            decimal_records.reopen()?,
            decimal_count,
            volumes,
            "entity-decimals",
            sort_chunk_records,
            sort_memory_bytes,
            progress,
        )?;
        let (decimal_sort_bytes, decimal_sort_touches) = merge_variable_runs::<DecimalString>(
            &mut decimal_runs,
            volumes,
            "entity-decimals",
            merge_fan_in,
            progress,
        )?;
        let numeric_pairs = NamedTempFile::new_in(root)?;
        let mut numeric_writer = BufWriter::with_capacity(IO_BUFFER_BYTES, numeric_pairs.reopen()?);
        if let Some(run) = decimal_runs.pop() {
            let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, File::open(&run.path)?);
            for rank in 0..run.records {
                let record = read_variable_record::<DecimalString>(&mut reader)?;
                write_fixed_record(&mut numeric_writer, [record.string_id, rank])?;
            }
            ensure_eof(&mut reader, "decimal sort run has trailing bytes")?;
            std::fs::remove_file(run.path)?;
        }
        numeric_writer.flush()?;

        let numeric_by_string = external_sort_fixed::<2>(
            numeric_pairs.reopen()?,
            decimal_count,
            volumes,
            "entity-numeric-map",
            sort_chunk_records,
            merge_fan_in,
            progress,
        )?;
        let occurrence_triples = NamedTempFile::new_in(root)?;
        join_numeric_ranks(
            mapping_pairs.reopen()?,
            self.occurrences,
            numeric_by_string.reopen()?,
            decimal_count,
            occurrence_triples.reopen()?,
        )?;
        let sorted_occurrences = external_sort_fixed::<3>(
            occurrence_triples.reopen()?,
            self.occurrences,
            volumes,
            "entity-occurrence-map",
            sort_chunk_records,
            merge_fan_in,
            progress,
        )?;
        let occurrence_map = NamedTempFile::new_in(root)?;
        write_dense_occurrence_map(
            sorted_occurrences.reopen()?,
            self.occurrences,
            occurrence_map.reopen()?,
        )?;

        let fixed_sort_bytes = self
            .occurrences
            .saturating_mul(5 * 8)
            .saturating_add(decimal_count.saturating_mul(4 * 8));
        let fixed_sort_touches = self
            .occurrences
            .saturating_mul(3)
            .saturating_add(decimal_count.saturating_mul(2));
        Ok(ExternalStringDictionary {
            offsets,
            blob,
            occurrence_map,
            occurrence_count: self.occurrences,
            string_count,
            spill_bytes: input_bytes
                .saturating_add(string_sort_bytes)
                .saturating_add(decimal_sort_bytes)
                .saturating_add(fixed_sort_bytes),
            handle_touches: self
                .occurrences
                .saturating_add(string_sort_touches)
                .saturating_add(decimal_sort_touches)
                .saturating_add(fixed_sort_touches),
        })
    }
}

trait VariableSortRecord: Ord {
    fn from_parts(id: u64, bytes: Vec<u8>) -> Self;
    fn id(&self) -> u64;
    fn bytes(&self) -> &[u8];
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct StringOccurrence {
    bytes: Vec<u8>,
    occurrence: u64,
}

impl VariableSortRecord for StringOccurrence {
    fn from_parts(id: u64, bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            occurrence: id,
        }
    }

    fn id(&self) -> u64 {
        self.occurrence
    }

    fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct DecimalString {
    bytes: Vec<u8>,
    string_id: u64,
}

impl Ord for DecimalString {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_decimal_tokens(&self.bytes, &other.bytes)
            .then_with(|| self.string_id.cmp(&other.string_id))
    }
}

impl PartialOrd for DecimalString {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl VariableSortRecord for DecimalString {
    fn from_parts(id: u64, bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            string_id: id,
        }
    }

    fn id(&self) -> u64 {
        self.string_id
    }

    fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

#[derive(Debug)]
struct VariableRun {
    path: PathBuf,
    records: u64,
}

fn create_variable_runs<R: VariableSortRecord>(
    source: File,
    record_count: u64,
    volumes: &[SpillVolume],
    prefix: &str,
    sort_chunk_records: usize,
    sort_memory_bytes: usize,
    progress: &dyn ProgressObserver,
) -> Result<Vec<VariableRun>, DedupError> {
    let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, source);
    let mut pending = None;
    let mut remaining = record_count;
    let mut runs = Vec::new();
    while remaining > 0 {
        let mut chunk = Vec::with_capacity(
            sort_chunk_records.min(usize::try_from(remaining).unwrap_or(usize::MAX)),
        );
        let mut bytes_used = 0_usize;
        while chunk.len() < sort_chunk_records && remaining > 0 {
            let record = match pending.take() {
                Some(record) => record,
                None => read_variable_record::<R>(&mut reader)?,
            };
            let record_bytes = record
                .bytes()
                .len()
                .checked_add(std::mem::size_of::<R>())
                .ok_or(DedupError::CounterOverflow {
                    counter: "external_string_sort_memory",
                })?;
            if record_bytes > sort_memory_bytes {
                return Err(DedupError::ResourceBudgetExceeded {
                    context: ErrorContext::stage("entity"),
                    requested: u64::try_from(record_bytes).unwrap_or(u64::MAX),
                });
            }
            if !chunk.is_empty() && bytes_used.saturating_add(record_bytes) > sort_memory_bytes {
                pending = Some(record);
                break;
            }
            bytes_used = bytes_used.saturating_add(record_bytes);
            chunk.push(record);
            remaining -= 1;
        }
        chunk.sort_unstable();
        let path = run_path(volumes, prefix, 0, runs.len())?;
        let mut writer = BufWriter::with_capacity(IO_BUFFER_BYTES, File::create(&path)?);
        for record in &chunk {
            write_variable_record(&mut writer, record)?;
        }
        writer.flush()?;
        writer.get_ref().sync_all()?;
        let records = u64::try_from(chunk.len()).map_err(|_| DedupError::CounterOverflow {
            counter: "external_variable_sort_records",
        })?;
        progress.advance(records);
        progress.check_cancelled("entity")?;
        runs.push(VariableRun { path, records });
    }
    ensure_eof(&mut reader, "variable string spool has trailing bytes")?;
    Ok(runs)
}

fn merge_variable_runs<R: VariableSortRecord>(
    runs: &mut Vec<VariableRun>,
    volumes: &[SpillVolume],
    prefix: &str,
    merge_fan_in: usize,
    progress: &dyn ProgressObserver,
) -> Result<(u64, u64), DedupError> {
    let mut pass = 1_usize;
    let mut spill_bytes = 0_u64;
    let mut touches = 0_u64;
    while runs.len() > 1 {
        progress.begin_phase(
            "entity_external_string_merge",
            Some(runs.iter().map(|run| run.records).sum()),
        );
        let next_count = runs.len().div_ceil(merge_fan_in);
        let mut next = Vec::with_capacity(next_count);
        for (group, input) in runs.chunks(merge_fan_in).enumerate() {
            if input.len() == 1 {
                next.push(VariableRun {
                    path: input[0].path.clone(),
                    records: input[0].records,
                });
                continue;
            }
            let path = run_path(volumes, prefix, pass, group)?;
            let records = merge_variable_run_group::<R>(input, &path)?;
            let bytes = std::fs::metadata(&path)?.len();
            spill_bytes = spill_bytes.saturating_add(bytes);
            touches = touches.saturating_add(records.saturating_mul(2));
            progress.advance(records);
            progress.check_cancelled("entity")?;
            for run in input {
                std::fs::remove_file(&run.path)?;
            }
            next.push(VariableRun { path, records });
        }
        *runs = next;
        pass = pass.saturating_add(1);
    }
    Ok((spill_bytes, touches))
}

fn merge_variable_run_group<R: VariableSortRecord>(
    runs: &[VariableRun],
    output: &Path,
) -> Result<u64, DedupError> {
    let mut readers = Vec::with_capacity(runs.len());
    let mut remaining = Vec::with_capacity(runs.len());
    let mut heap = BinaryHeap::new();
    let mut total = 0_u64;
    for (index, run) in runs.iter().enumerate() {
        let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, File::open(&run.path)?);
        let mut count = run.records;
        total = total.saturating_add(count);
        if count > 0 {
            heap.push(Reverse((read_variable_record::<R>(&mut reader)?, index)));
            count -= 1;
        }
        readers.push(reader);
        remaining.push(count);
    }
    let mut writer = BufWriter::with_capacity(IO_BUFFER_BYTES, File::create(output)?);
    while let Some(Reverse((record, index))) = heap.pop() {
        write_variable_record(&mut writer, &record)?;
        if remaining[index] > 0 {
            heap.push(Reverse((
                read_variable_record::<R>(&mut readers[index])?,
                index,
            )));
            remaining[index] -= 1;
        }
    }
    writer.flush()?;
    writer.get_ref().sync_all()?;
    Ok(total)
}

fn external_sort_fixed<const N: usize>(
    source: File,
    record_count: u64,
    volumes: &[SpillVolume],
    prefix: &str,
    sort_chunk_records: usize,
    merge_fan_in: usize,
    progress: &dyn ProgressObserver,
) -> Result<NamedTempFile, DedupError> {
    let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, source);
    let mut remaining = record_count;
    let mut runs = Vec::new();
    while remaining > 0 {
        let count =
            usize::try_from(remaining.min(u64::try_from(sort_chunk_records).unwrap_or(u64::MAX)))
                .map_err(|_| DedupError::CounterOverflow {
                counter: "external_fixed_sort_chunk",
            })?;
        let mut chunk = Vec::with_capacity(count);
        for _ in 0..count {
            chunk.push(read_fixed_record::<_, N>(&mut reader)?);
        }
        chunk.sort_unstable();
        let path = run_path(volumes, prefix, 0, runs.len())?;
        let mut writer = BufWriter::with_capacity(IO_BUFFER_BYTES, File::create(&path)?);
        for record in chunk {
            write_fixed_record(&mut writer, record)?;
        }
        writer.flush()?;
        runs.push(VariableRun {
            path,
            records: u64::try_from(count).unwrap_or(u64::MAX),
        });
        remaining -= u64::try_from(count).unwrap_or(remaining);
    }
    ensure_eof(&mut reader, "fixed sort input has trailing bytes")?;
    let mut pass = 1_usize;
    while runs.len() > 1 {
        let next_count = runs.len().div_ceil(merge_fan_in);
        let mut next = Vec::with_capacity(next_count);
        for (group, input) in runs.chunks(merge_fan_in).enumerate() {
            if input.len() == 1 {
                next.push(VariableRun {
                    path: input[0].path.clone(),
                    records: input[0].records,
                });
                continue;
            }
            let path = run_path(volumes, prefix, pass, group)?;
            let records = merge_fixed_run_group::<N>(input, &path)?;
            for run in input {
                std::fs::remove_file(&run.path)?;
            }
            next.push(VariableRun { path, records });
        }
        runs = next;
        pass = pass.saturating_add(1);
    }
    let output = NamedTempFile::new_in(&volumes[0].root)?;
    if let Some(run) = runs.pop() {
        std::fs::copy(&run.path, output.path())?;
        std::fs::remove_file(run.path)?;
    }
    progress.check_cancelled("entity")?;
    Ok(output)
}

fn merge_fixed_run_group<const N: usize>(
    runs: &[VariableRun],
    output: &Path,
) -> Result<u64, DedupError> {
    let mut readers = Vec::with_capacity(runs.len());
    let mut remaining = Vec::with_capacity(runs.len());
    let mut heap = BinaryHeap::new();
    let mut total = 0_u64;
    for (index, run) in runs.iter().enumerate() {
        let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, File::open(&run.path)?);
        let mut count = run.records;
        total = total.saturating_add(count);
        if count > 0 {
            heap.push(Reverse((read_fixed_record::<_, N>(&mut reader)?, index)));
            count -= 1;
        }
        readers.push(reader);
        remaining.push(count);
    }
    let mut writer = BufWriter::with_capacity(IO_BUFFER_BYTES, File::create(output)?);
    while let Some(Reverse((record, index))) = heap.pop() {
        write_fixed_record(&mut writer, record)?;
        if remaining[index] > 0 {
            heap.push(Reverse((
                read_fixed_record::<_, N>(&mut readers[index])?,
                index,
            )));
            remaining[index] -= 1;
        }
    }
    writer.flush()?;
    Ok(total)
}

fn join_numeric_ranks(
    mappings: File,
    mapping_count: u64,
    numeric: File,
    numeric_count: u64,
    output: File,
) -> Result<(), DedupError> {
    let mut mappings = BufReader::with_capacity(IO_BUFFER_BYTES, mappings);
    let mut numeric = BufReader::with_capacity(IO_BUFFER_BYTES, numeric);
    let mut output = BufWriter::with_capacity(IO_BUFFER_BYTES, output);
    let mut numeric_read = 0_u64;
    let mut current = if numeric_count > 0 {
        numeric_read += 1;
        Some(read_fixed_record::<_, 2>(&mut numeric)?)
    } else {
        None
    };
    for _ in 0..mapping_count {
        let [occurrence, string_id] = read_fixed_record::<_, 2>(&mut mappings)?;
        while current.is_some_and(|record| record[0] < string_id) {
            current = if numeric_read < numeric_count {
                numeric_read += 1;
                Some(read_fixed_record::<_, 2>(&mut numeric)?)
            } else {
                None
            };
        }
        let rank = current
            .filter(|record| record[0] == string_id)
            .map_or(NONE_ID, |record| record[1]);
        write_fixed_record(&mut output, [occurrence, string_id, rank])?;
    }
    ensure_eof(&mut mappings, "mapping pairs have trailing bytes")?;
    ensure_eof(&mut numeric, "numeric rank pairs have trailing bytes")?;
    output.flush()?;
    Ok(())
}

fn write_dense_occurrence_map(
    sorted: File,
    occurrence_count: u64,
    output: File,
) -> Result<(), DedupError> {
    let mut sorted = BufReader::with_capacity(IO_BUFFER_BYTES, sorted);
    let mut output = BufWriter::with_capacity(IO_BUFFER_BYTES, output);
    for expected in 0..occurrence_count {
        let [occurrence, string_id, numeric_rank] = read_fixed_record::<_, 3>(&mut sorted)?;
        if occurrence != expected {
            return Err(invalid_external_string(format!(
                "occurrence map expected {expected}, found {occurrence}"
            )));
        }
        write_u64(&mut output, string_id)?;
        write_u64(&mut output, numeric_rank)?;
    }
    ensure_eof(&mut sorted, "sorted occurrence map has trailing bytes")?;
    output.flush()?;
    Ok(())
}

fn write_variable_record(
    writer: &mut impl Write,
    record: &impl VariableSortRecord,
) -> Result<(), DedupError> {
    write_u64(writer, record.id())?;
    write_u64(
        writer,
        u64::try_from(record.bytes().len()).map_err(|_| DedupError::CounterOverflow {
            counter: "external_variable_record_length",
        })?,
    )?;
    writer.write_all(record.bytes())?;
    Ok(())
}

fn read_variable_record<R: VariableSortRecord>(reader: &mut impl Read) -> Result<R, DedupError> {
    let id = read_u64(reader)?;
    let length = read_u64(reader)?;
    let length = usize::try_from(length).map_err(|_| DedupError::ResourceBudgetExceeded {
        context: ErrorContext::stage("entity"),
        requested: length,
    })?;
    let mut bytes = vec![0_u8; length];
    reader.read_exact(&mut bytes)?;
    Ok(R::from_parts(id, bytes))
}

fn write_fixed_record<const N: usize>(
    writer: &mut impl Write,
    record: [u64; N],
) -> Result<(), DedupError> {
    for value in record {
        write_u64(writer, value)?;
    }
    Ok(())
}

fn read_fixed_record<R: Read, const N: usize>(reader: &mut R) -> Result<[u64; N], DedupError> {
    let mut record = [0_u64; N];
    for value in &mut record {
        *value = read_u64(reader)?;
    }
    Ok(record)
}

fn write_u64(writer: &mut impl Write, value: u64) -> Result<(), DedupError> {
    writer.write_all(&value.to_le_bytes())?;
    Ok(())
}

fn read_u64(reader: &mut impl Read) -> Result<u64, DedupError> {
    let mut bytes = [0_u8; 8];
    reader.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

fn ensure_eof(reader: &mut impl Read, message: &str) -> Result<(), DedupError> {
    let mut trailing = [0_u8; 1];
    if reader.read(&mut trailing)? != 0 {
        return Err(invalid_external_string(message.to_owned()));
    }
    Ok(())
}

fn run_path(
    volumes: &[SpillVolume],
    prefix: &str,
    pass: usize,
    index: usize,
) -> Result<PathBuf, DedupError> {
    let volume = volumes
        .get(index % volumes.len())
        .ok_or_else(|| invalid_external_string("no external string volume".to_owned()))?;
    std::fs::create_dir_all(&volume.root)?;
    Ok(volume
        .root
        .join(format!("{prefix}-{pass:03}-{index:08}.bin")))
}

fn validate_capacities(
    volumes: &[SpillVolume],
    sort_chunk_records: usize,
    sort_memory_bytes: usize,
    merge_fan_in: usize,
) -> Result<(), DedupError> {
    if volumes.is_empty() || sort_chunk_records == 0 || sort_memory_bytes == 0 || merge_fan_in < 2 {
        return Err(DedupError::InvalidInput {
            context: ErrorContext::stage("entity"),
            message: "external string capacities or volumes are invalid".to_owned(),
        });
    }
    Ok(())
}

fn is_decimal(value: &[u8]) -> bool {
    !value.is_empty() && value.iter().all(u8::is_ascii_digit)
}

fn compare_decimal_tokens(left: &[u8], right: &[u8]) -> Ordering {
    let left_numeric = left
        .iter()
        .position(|byte| *byte != b'0')
        .map_or(&left[left.len()..], |start| &left[start..]);
    let right_numeric = right
        .iter()
        .position(|byte| *byte != b'0')
        .map_or(&right[right.len()..], |start| &right[start..]);
    left_numeric
        .len()
        .cmp(&right_numeric.len())
        .then_with(|| left_numeric.cmp(right_numeric))
        .then_with(|| left.cmp(right))
}

fn invalid_external_string(message: String) -> DedupError {
    DedupError::ArtifactMismatch {
        context: ErrorContext::stage("entity"),
        message,
    }
}

#[cfg(unix)]
trait FileWriteAt {
    fn write_all_at(&mut self, bytes: &[u8], offset: u64) -> Result<(), std::io::Error>;
}

#[cfg(unix)]
impl FileWriteAt for File {
    fn write_all_at(&mut self, bytes: &[u8], offset: u64) -> Result<(), std::io::Error> {
        use std::os::unix::fs::FileExt;
        FileExt::write_all_at(self, bytes, offset)
    }
}

#[cfg(windows)]
trait FileWriteAt {
    fn write_all_at(&mut self, bytes: &[u8], offset: u64) -> Result<(), std::io::Error>;
}

#[cfg(windows)]
impl FileWriteAt for File {
    fn write_all_at(&mut self, bytes: &[u8], offset: u64) -> Result<(), std::io::Error> {
        use std::os::windows::fs::FileExt;
        FileExt::seek_write(self, bytes, offset).and_then(|written| {
            if written == bytes.len() {
                Ok(())
            } else {
                Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "short positioned write",
                ))
            }
        })
    }
}
