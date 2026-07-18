use crate::{MemoryBudget, MemoryLease, SpillFileSet};
use dedup_model::{DedupError, ErrorContext, ProgressObserver};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

const RECORD_BYTES: u64 = 32;
const DEFAULT_MEMORY_BYTES: u64 = 64 * 1024 * 1024;
const MINIMUM_BUFFER_BYTES: usize = 4 * 1024;
const MAXIMUM_BUFFER_BYTES: usize = 64 * 1024;

/// Fixed-width handle moved by external radix stages.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RadixRecord {
    pub key: u64,
    pub payload: [u64; 3],
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ExternalRadixStats {
    pub records: u64,
    pub handle_touches: u64,
    pub spill_bytes: u64,
    pub partitions: u16,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpillVolume {
    pub root: PathBuf,
    pub weight: u64,
}

impl SpillVolume {
    pub fn new(root: impl Into<PathBuf>, weight: u64) -> Result<Self, DedupError> {
        if weight == 0 {
            return Err(invalid_radix("spill volume weight must be positive"));
        }
        Ok(Self {
            root: root.into(),
            weight,
        })
    }
}

/// Bounded external high-bit radix partitioner.
///
/// The constructor bounds both open spill files and the largest in-memory partition.
pub struct ExternalRadix {
    partition_paths: Vec<PathBuf>,
    partition_bits: u8,
    max_records_per_partition: usize,
    buffer_bytes: usize,
    sort_chunk_records: usize,
    merge_fan_in: usize,
    writers: SpillFileSet<BufWriter<File>>,
    counts: Vec<u64>,
    stats: ExternalRadixStats,
    _memory_lease: Option<MemoryLease>,
}

impl ExternalRadix {
    pub fn create(
        root: impl AsRef<Path>,
        partition_bits: u8,
        max_open_files: usize,
        max_records_per_partition: usize,
    ) -> Result<Self, DedupError> {
        Self::create_internal(
            vec![SpillVolume::new(root.as_ref(), 1)?],
            partition_bits,
            max_open_files,
            max_records_per_partition,
            DEFAULT_MEMORY_BYTES,
            None,
        )
    }

    pub fn create_budgeted(
        root: impl AsRef<Path>,
        partition_bits: u8,
        max_open_files: usize,
        max_records_per_partition: usize,
        memory_budget: &MemoryBudget,
        memory_bytes: u64,
    ) -> Result<Self, DedupError> {
        let lease = memory_budget.require_lease(memory_bytes)?;
        Self::create_internal(
            vec![SpillVolume::new(root.as_ref(), 1)?],
            partition_bits,
            max_open_files,
            max_records_per_partition,
            memory_bytes,
            Some(lease),
        )
    }

    pub fn create_striped(
        volumes: Vec<SpillVolume>,
        partition_bits: u8,
        max_open_files: usize,
        max_records_per_partition: usize,
    ) -> Result<Self, DedupError> {
        Self::create_internal(
            volumes,
            partition_bits,
            max_open_files,
            max_records_per_partition,
            DEFAULT_MEMORY_BYTES,
            None,
        )
    }

    pub fn create_budgeted_striped(
        volumes: Vec<SpillVolume>,
        partition_bits: u8,
        max_open_files: usize,
        max_records_per_partition: usize,
        memory_budget: &MemoryBudget,
        memory_bytes: u64,
    ) -> Result<Self, DedupError> {
        let lease = memory_budget.require_lease(memory_bytes)?;
        Self::create_internal(
            volumes,
            partition_bits,
            max_open_files,
            max_records_per_partition,
            memory_bytes,
            Some(lease),
        )
    }

    fn create_internal(
        volumes: Vec<SpillVolume>,
        partition_bits: u8,
        max_open_files: usize,
        max_records_per_partition: usize,
        memory_bytes: u64,
        memory_lease: Option<MemoryLease>,
    ) -> Result<Self, DedupError> {
        if !(1..=8).contains(&partition_bits) || max_records_per_partition == 0 {
            return Err(invalid_radix(
                "partition_bits must be in 1..=8 and partition capacity must be positive",
            ));
        }
        if volumes.is_empty() {
            return Err(invalid_radix("at least one spill volume is required"));
        }
        let partition_count = 1_usize << partition_bits;
        if partition_count > max_open_files {
            return Err(invalid_radix(
                "radix partition count exceeds the bounded spill file set",
            ));
        }
        let minimum_memory = u64::try_from(partition_count)
            .unwrap_or(u64::MAX)
            .saturating_mul(u64::try_from(MINIMUM_BUFFER_BYTES).unwrap_or(u64::MAX))
            .saturating_add(RECORD_BYTES);
        if memory_bytes < minimum_memory {
            return Err(DedupError::ResourceBudgetExceeded {
                context: ErrorContext::stage("external_radix"),
                requested: minimum_memory,
            });
        }
        let buffer_budget = memory_bytes / 4;
        let buffer_bytes =
            usize::try_from(buffer_budget / u64::try_from(partition_count).unwrap_or(u64::MAX))
                .unwrap_or(MAXIMUM_BUFFER_BYTES)
                .clamp(MINIMUM_BUFFER_BYTES, MAXIMUM_BUFFER_BYTES);
        let total_buffers = u64::try_from(buffer_bytes)
            .unwrap_or(u64::MAX)
            .saturating_mul(u64::try_from(partition_count).unwrap_or(u64::MAX));
        let sort_bytes = memory_bytes.saturating_sub(total_buffers).max(RECORD_BYTES);
        let sort_chunk_records = usize::try_from(sort_bytes / RECORD_BYTES)
            .unwrap_or(usize::MAX)
            .max(1);
        let memory_buffer_slots =
            usize::try_from(memory_bytes / u64::try_from(buffer_bytes).unwrap_or(u64::MAX))
                .unwrap_or(usize::MAX);
        let merge_fan_in = max_open_files
            .saturating_sub(1)
            .min(memory_buffer_slots.saturating_sub(1));
        for volume in &volumes {
            if volume.weight == 0 {
                return Err(invalid_radix("spill volume weight must be positive"));
            }
            fs::create_dir_all(&volume.root)?;
        }
        let partition_paths = weighted_partition_paths(&volumes, partition_count)?;
        let mut writers = SpillFileSet::new(max_open_files)?;
        for path in &partition_paths {
            let file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(path)?;
            writers
                .push(BufWriter::with_capacity(buffer_bytes, file))
                .map_err(|_| invalid_radix("bounded spill file set exceeded its capacity"))?;
        }
        Ok(Self {
            partition_paths,
            partition_bits,
            max_records_per_partition,
            buffer_bytes,
            sort_chunk_records,
            merge_fan_in,
            writers,
            counts: vec![0; partition_count],
            stats: ExternalRadixStats {
                partitions: u16::try_from(partition_count).map_err(|_| {
                    DedupError::InvariantViolation {
                        context: ErrorContext::stage("external_radix"),
                        message: "partition count does not fit u16".to_owned(),
                    }
                })?,
                ..ExternalRadixStats::default()
            },
            _memory_lease: memory_lease,
        })
    }

    pub fn push(&mut self, record: RadixRecord) -> Result<(), DedupError> {
        let partition_mask = (1_u64 << self.partition_bits) - 1;
        let partition = usize::try_from(record.key & partition_mask)
            .map_err(|_| invalid_radix("partition index does not fit usize"))?;
        let next = self.counts[partition]
            .checked_add(1)
            .ok_or(DedupError::CounterOverflow {
                counter: "external_radix_partition_records",
            })?;
        if next
            > u64::try_from(self.max_records_per_partition).map_err(|_| {
                invalid_radix("partition capacity does not fit the work-counter representation")
            })?
        {
            return Err(DedupError::BudgetExhausted {
                context: ErrorContext {
                    stage: "external_radix",
                    partition: Some(u64::try_from(partition).unwrap_or(u64::MAX)),
                    stable_object_id: Some(record.key),
                },
                counter: "external_radix_partition_records",
                limit: u64::try_from(self.max_records_per_partition).unwrap_or(u64::MAX),
            });
        }
        let writer = self
            .writers
            .get_mut(partition)
            .ok_or_else(|| invalid_radix("radix partition has no spill writer"))?;
        write_record(writer, record)?;
        self.counts[partition] = next;
        self.stats.records =
            self.stats
                .records
                .checked_add(1)
                .ok_or(DedupError::CounterOverflow {
                    counter: "external_radix_records",
                })?;
        self.stats.handle_touches =
            self.stats
                .handle_touches
                .checked_add(1)
                .ok_or(DedupError::CounterOverflow {
                    counter: "external_radix_handle_touches",
                })?;
        self.stats.spill_bytes = self.stats.spill_bytes.checked_add(RECORD_BYTES).ok_or(
            DedupError::CounterOverflow {
                counter: "external_radix_spill_bytes",
            },
        )?;
        Ok(())
    }

    pub fn partition_paths(&self) -> &[PathBuf] {
        &self.partition_paths
    }

    pub fn finish(
        self,
        mut emit: impl FnMut(RadixRecord) -> Result<(), DedupError>,
    ) -> Result<ExternalRadixStats, DedupError> {
        self.finish_internal(None, &mut emit)
    }

    pub fn finish_with_progress(
        self,
        progress: &dyn ProgressObserver,
        sort_phase: &'static str,
        emit_phase: &'static str,
        mut emit: impl FnMut(RadixRecord) -> Result<(), DedupError>,
    ) -> Result<ExternalRadixStats, DedupError> {
        self.finish_internal(Some((progress, sort_phase, emit_phase)), &mut emit)
    }

    fn finish_internal(
        mut self,
        progress: Option<(&dyn ProgressObserver, &'static str, &'static str)>,
        emit: &mut impl FnMut(RadixRecord) -> Result<(), DedupError>,
    ) -> Result<ExternalRadixStats, DedupError> {
        for writer in self.writers.iter_mut() {
            writer.flush()?;
            writer.get_ref().sync_all()?;
        }
        self.writers.clear();
        if let Some((observer, sort_phase, _)) = progress {
            observer.begin_phase(sort_phase, None);
        }
        for partition in 0..self.counts.len() {
            self.sort_partition(partition, progress.map(|value| value.0))?;
        }

        if let Some((observer, _, emit_phase)) = progress {
            observer.begin_phase(emit_phase, Some(self.stats.records));
        }
        let mut readers = Vec::with_capacity(self.counts.len());
        let mut remaining = self.counts.clone();
        let mut heap = BinaryHeap::new();
        for (partition, partition_remaining) in remaining.iter_mut().enumerate() {
            let mut reader = BufReader::with_capacity(
                self.buffer_bytes,
                File::open(&self.partition_paths[partition])?,
            );
            if *partition_remaining > 0 {
                let record = read_record(&mut reader)?;
                *partition_remaining -= 1;
                heap.push(Reverse((record, partition)));
            }
            readers.push(reader);
        }
        let mut emitted_work = 0_u64;
        while let Some(Reverse((record, partition))) = heap.pop() {
            emit(record)?;
            emitted_work = emitted_work.saturating_add(1);
            if emitted_work == 1_024 {
                if let Some((observer, _, _)) = progress {
                    observer.advance(emitted_work);
                    observer.check_cancelled("external_radix")?;
                }
                emitted_work = 0;
            }
            self.stats.handle_touches =
                self.stats
                    .handle_touches
                    .checked_add(1)
                    .ok_or(DedupError::CounterOverflow {
                        counter: "external_radix_handle_touches",
                    })?;
            if remaining[partition] > 0 {
                let next = read_record(&mut readers[partition])?;
                remaining[partition] -= 1;
                heap.push(Reverse((next, partition)));
            }
        }
        if let Some((observer, _, _)) = progress {
            observer.advance(emitted_work);
            observer.check_cancelled("external_radix")?;
        }
        drop(readers);
        for partition in 0..self.counts.len() {
            fs::remove_file(&self.partition_paths[partition])?;
        }
        Ok(self.stats)
    }

    fn sort_partition(
        &mut self,
        partition: usize,
        progress: Option<&dyn ProgressObserver>,
    ) -> Result<(), DedupError> {
        let count = usize::try_from(self.counts[partition]).map_err(|_| {
            invalid_radix("partition record count does not fit platform address space")
        })?;
        if count == 0 {
            return Ok(());
        }
        let partition_path = self.partition_paths[partition].clone();
        let partition_root = partition_path
            .parent()
            .ok_or_else(|| invalid_radix("partition path has no parent"))?;
        let mut reader = BufReader::with_capacity(self.buffer_bytes, File::open(&partition_path)?);
        let mut runs = Vec::new();
        let mut remaining = count;
        while remaining > 0 {
            let chunk_length = remaining.min(self.sort_chunk_records);
            let mut records = Vec::with_capacity(chunk_length);
            for _ in 0..chunk_length {
                records.push(read_record(&mut reader)?);
            }
            records.sort_unstable();
            let run_path = partition_root.join(format!(
                "partition-{partition:03}.run-{:05}.bin",
                runs.len()
            ));
            let mut writer = BufWriter::with_capacity(self.buffer_bytes, File::create(&run_path)?);
            for record in records {
                write_record(&mut writer, record)?;
            }
            writer.flush()?;
            writer.get_ref().sync_all()?;
            runs.push((run_path, chunk_length));
            remaining -= chunk_length;
            self.add_sort_io(u64::try_from(chunk_length).unwrap_or(u64::MAX))?;
            if let Some(observer) = progress {
                observer.advance(u64::try_from(chunk_length).unwrap_or(u64::MAX));
                observer.check_cancelled("external_radix_sort")?;
            }
        }
        let mut trailing = [0_u8; 1];
        if reader.read(&mut trailing)? != 0 {
            return Err(invalid_radix("partition contains trailing bytes"));
        }
        drop(reader);
        fs::remove_file(&partition_path)?;
        if runs.len() > 1 && self.merge_fan_in < 2 {
            return Err(invalid_radix(
                "external merge requires at least three open spill files",
            ));
        }
        let mut pass = 0_usize;
        while runs.len() > 1 {
            let mut next_runs = Vec::with_capacity(runs.len().div_ceil(self.merge_fan_in));
            for (group, chunk) in runs.chunks(self.merge_fan_in).enumerate() {
                if chunk.len() == 1 {
                    next_runs.push(chunk[0].clone());
                    continue;
                }
                let merged_path = partition_root.join(format!(
                    "partition-{partition:03}.merge-{pass:03}-{group:05}.bin"
                ));
                let merged_count = self.merge_runs(chunk, &merged_path, progress)?;
                next_runs.push((merged_path, merged_count));
            }
            runs = next_runs;
            pass = pass.saturating_add(1);
        }
        fs::rename(&runs[0].0, partition_path).map_err(DedupError::Io)
    }

    fn merge_runs(
        &mut self,
        runs: &[(PathBuf, usize)],
        output: &Path,
        progress: Option<&dyn ProgressObserver>,
    ) -> Result<usize, DedupError> {
        let mut readers = Vec::with_capacity(runs.len());
        let mut remaining = Vec::with_capacity(runs.len());
        let mut heap = BinaryHeap::new();
        let mut total = 0_usize;
        for (run_index, (path, run_length)) in runs.iter().enumerate() {
            let mut reader = BufReader::with_capacity(self.buffer_bytes, File::open(path)?);
            let mut run_remaining = *run_length;
            total = total
                .checked_add(run_remaining)
                .ok_or(DedupError::CounterOverflow {
                    counter: "external_radix_merge_records",
                })?;
            if run_remaining > 0 {
                heap.push(Reverse((read_record(&mut reader)?, run_index)));
                run_remaining -= 1;
            }
            readers.push(reader);
            remaining.push(run_remaining);
        }
        let mut writer = BufWriter::with_capacity(self.buffer_bytes, File::create(output)?);
        while let Some(Reverse((record, run_index))) = heap.pop() {
            write_record(&mut writer, record)?;
            if remaining[run_index] > 0 {
                heap.push(Reverse((read_record(&mut readers[run_index])?, run_index)));
                remaining[run_index] -= 1;
            }
        }
        writer.flush()?;
        writer.get_ref().sync_all()?;
        drop((writer, readers));
        for (path, _) in runs {
            fs::remove_file(path)?;
        }
        self.add_sort_io(u64::try_from(total).unwrap_or(u64::MAX))?;
        if let Some(observer) = progress {
            observer.advance(u64::try_from(total).unwrap_or(u64::MAX));
            observer.check_cancelled("external_radix_merge")?;
        }
        Ok(total)
    }

    fn add_sort_io(&mut self, records: u64) -> Result<(), DedupError> {
        self.stats.handle_touches = self
            .stats
            .handle_touches
            .checked_add(records.saturating_mul(2))
            .ok_or(DedupError::CounterOverflow {
                counter: "external_radix_handle_touches",
            })?;
        self.stats.spill_bytes = self
            .stats
            .spill_bytes
            .checked_add(records.saturating_mul(RECORD_BYTES))
            .ok_or(DedupError::CounterOverflow {
                counter: "external_radix_spill_bytes",
            })?;
        Ok(())
    }
}

fn weighted_partition_paths(
    volumes: &[SpillVolume],
    partition_count: usize,
) -> Result<Vec<PathBuf>, DedupError> {
    let total_weight = volumes.iter().try_fold(0_u128, |total, volume| {
        total
            .checked_add(u128::from(volume.weight))
            .ok_or(DedupError::CounterOverflow {
                counter: "external_radix_volume_weight",
            })
    })?;
    let partition_count_u128 =
        u128::try_from(partition_count).map_err(|_| DedupError::CounterOverflow {
            counter: "external_radix_partitions",
        })?;
    let mut paths = Vec::with_capacity(partition_count);
    for partition in 0..partition_count {
        let midpoint = u128::try_from(partition)
            .unwrap_or(u128::MAX)
            .saturating_mul(2)
            .saturating_add(1)
            .saturating_mul(total_weight)
            / partition_count_u128.saturating_mul(2);
        let mut cumulative = 0_u128;
        let mut selected = volumes.len() - 1;
        for (index, volume) in volumes.iter().enumerate() {
            cumulative = cumulative.saturating_add(u128::from(volume.weight));
            if midpoint < cumulative {
                selected = index;
                break;
            }
        }
        paths.push(
            volumes[selected]
                .root
                .join(format!("partition-{partition:03}.bin")),
        );
    }
    Ok(paths)
}

fn write_record(writer: &mut impl Write, record: RadixRecord) -> Result<(), DedupError> {
    writer.write_all(&record.key.to_le_bytes())?;
    for value in record.payload {
        writer.write_all(&value.to_le_bytes())?;
    }
    Ok(())
}

fn read_record(reader: &mut impl Read) -> Result<RadixRecord, DedupError> {
    let mut fields = [0_u64; 4];
    for field in &mut fields {
        let mut bytes = [0_u8; 8];
        reader.read_exact(&mut bytes)?;
        *field = u64::from_le_bytes(bytes);
    }
    Ok(RadixRecord {
        key: fields[0],
        payload: [fields[1], fields[2], fields[3]],
    })
}

fn invalid_radix(message: impl Into<String>) -> DedupError {
    DedupError::InvalidInput {
        context: ErrorContext::stage("external_radix"),
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_records_are_globally_sorted_and_touches_are_exact() {
        let directory = tempfile::tempdir().unwrap();
        let mut radix = ExternalRadix::create(directory.path(), 3, 8, 1_000).unwrap();
        for key in [u64::MAX, 9, 1 << 63, 7, 0] {
            radix
                .push(RadixRecord {
                    key,
                    payload: [key, 0, 0],
                })
                .unwrap();
        }
        let mut output = Vec::new();
        let stats = radix.finish(|record| {
            output.push(record);
            Ok(())
        });
        assert_eq!(
            output.iter().map(|record| record.key).collect::<Vec<_>>(),
            [0, 7, 9, 1 << 63, u64::MAX]
        );
        assert_eq!(stats.unwrap().handle_touches, 20);
    }

    #[test]
    fn partition_capacity_fails_without_unbounded_growth() {
        let directory = tempfile::tempdir().unwrap();
        let mut radix = ExternalRadix::create(directory.path(), 1, 2, 1).unwrap();
        let record = RadixRecord {
            key: 0,
            payload: [0; 3],
        };
        radix.push(record).unwrap();
        assert!(matches!(
            radix.push(record),
            Err(DedupError::BudgetExhausted { .. })
        ));
    }

    #[test]
    fn tiny_budget_uses_bounded_multi_pass_runs() {
        let directory = tempfile::tempdir().unwrap();
        let memory_bytes = 3 * u64::try_from(MINIMUM_BUFFER_BYTES).unwrap() + RECORD_BYTES;
        let mut radix = ExternalRadix::create_internal(
            vec![SpillVolume::new(directory.path(), 1).unwrap()],
            1,
            4,
            500,
            memory_bytes,
            None,
        )
        .unwrap();
        for key in (0_u64..300).rev().map(|value| value * 2) {
            radix
                .push(RadixRecord {
                    key,
                    payload: [key, 0, 0],
                })
                .unwrap();
        }
        let mut output = Vec::new();
        let stats = radix
            .finish(|record| {
                output.push(record.key);
                Ok(())
            })
            .unwrap();
        assert!(output.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(output.len(), 300);
        assert!(stats.handle_touches > 4 * 300);
    }

    #[test]
    fn budgeted_radix_returns_its_central_lease() {
        let directory = tempfile::tempdir().unwrap();
        let budget = MemoryBudget::new(128 * 1024 * 1024, 128 * 1024 * 1024);
        let bytes = 16 * 1024 * 1024;
        let mut radix =
            ExternalRadix::create_budgeted(directory.path(), 2, 4, 100, &budget, bytes).unwrap();
        assert_eq!(budget.used(), bytes);
        radix
            .push(RadixRecord {
                key: 1,
                payload: [0; 3],
            })
            .unwrap();
        radix.finish(|_| Ok(())).unwrap();
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn striped_radix_assigns_partitions_by_weight_and_stays_globally_sorted() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        let volumes = vec![
            SpillVolume::new(first.path(), 1).unwrap(),
            SpillVolume::new(second.path(), 3).unwrap(),
        ];
        let mut radix = ExternalRadix::create_striped(volumes, 3, 8, 100).unwrap();
        assert_eq!(
            radix
                .partition_paths()
                .iter()
                .filter(|path| path.starts_with(first.path()))
                .count(),
            2
        );
        assert_eq!(
            radix
                .partition_paths()
                .iter()
                .filter(|path| path.starts_with(second.path()))
                .count(),
            6
        );
        for key in (0_u64..32).rev() {
            radix
                .push(RadixRecord {
                    key,
                    payload: [key, 0, 0],
                })
                .unwrap();
        }
        let mut keys = Vec::new();
        radix
            .finish(|record| {
                keys.push(record.key);
                Ok(())
            })
            .unwrap();
        assert_eq!(keys, (0_u64..32).collect::<Vec<_>>());
    }
}
