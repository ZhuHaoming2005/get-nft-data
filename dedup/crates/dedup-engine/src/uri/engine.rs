use crate::parallel::RayonChunkExecutor;
use ahash::{AHashMap, AHashSet, RandomState};
use dedup_index::{ExternalRadix, MemoryBudget, PairReducerBuffer, RadixRecord, SpillVolume};
use dedup_model::{
    ChainId, ChunkExecutor, Contract, ContractId, DedupError, Dimension, EntityId, EntityKind,
    ErrorContext, ExecutionMode, HitEvent, HitEventSink, Nft, NftId, NoopProgress,
    ProgressObserver, ScopeId, StageCounters, StringId,
};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct UriMember {
    chain_id: ChainId,
    contract_id: ContractId,
    nft_id: NftId,
}

#[derive(Clone, Debug)]
struct UriGroup {
    members: Vec<UriMember>,
    contracts_by_chain: AHashMap<ChainId, AHashSet<ContractId>>,
}

impl UriGroup {
    fn new() -> Self {
        Self {
            members: Vec::new(),
            contracts_by_chain: AHashMap::with_hasher(RandomState::with_seeds(21, 22, 23, 24)),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UriRunResult {
    pub token_groups: u64,
    pub image_groups: u64,
    pub member_accesses: u64,
    pub bitmap_word_operations: u64,
    pub spill_handle_touches: u64,
    pub max_spill_reducer_buffered_members: u64,
    pub max_spill_hit_buffered_events: u64,
    pub spill_hit_shards: u64,
    pub counters: StageCounters,
}

#[derive(Clone, Debug)]
pub struct UriExecutionConfig {
    pub mode: ExecutionMode,
    pub spill_root: PathBuf,
    pub hot_group_member_limit: usize,
    pub radix_partition_bits: u8,
    pub max_open_spill_files: usize,
    pub max_records_per_partition: usize,
    pub radix_memory_budget: Option<(MemoryBudget, u64)>,
    pub radix_volumes: Vec<SpillVolume>,
    pub mark_shards: usize,
    pub mark_buffer_capacity: usize,
    pub workers: usize,
}

impl UriExecutionConfig {
    pub fn new(
        mode: ExecutionMode,
        spill_root: impl Into<PathBuf>,
        hot_group_member_limit: usize,
        radix_partition_bits: u8,
        max_open_spill_files: usize,
        max_records_per_partition: usize,
    ) -> Result<Self, DedupError> {
        if hot_group_member_limit == 0
            || max_open_spill_files == 0
            || max_records_per_partition == 0
        {
            return Err(DedupError::InvalidInput {
                context: ErrorContext::stage("uri"),
                message: "URI spill capacities must be positive".to_owned(),
            });
        }
        let spill_root = spill_root.into();
        Ok(Self {
            mode,
            spill_root: spill_root.clone(),
            hot_group_member_limit,
            radix_partition_bits,
            max_open_spill_files,
            max_records_per_partition,
            radix_memory_budget: None,
            radix_volumes: vec![SpillVolume::new(spill_root, 1)?],
            mark_shards: 1,
            mark_buffer_capacity: 4_096,
            workers: 1,
        })
    }

    pub fn with_radix_memory_budget(
        mut self,
        budget: MemoryBudget,
        bytes: u64,
    ) -> Result<Self, DedupError> {
        if bytes == 0 {
            return Err(DedupError::InvalidInput {
                context: ErrorContext::stage("uri"),
                message: "URI radix memory budget must be positive".to_owned(),
            });
        }
        self.radix_memory_budget = Some((budget, bytes));
        Ok(self)
    }

    pub fn with_mark_shards(
        mut self,
        mark_shards: usize,
        mark_buffer_capacity: usize,
    ) -> Result<Self, DedupError> {
        if mark_shards == 0 || mark_buffer_capacity < mark_shards {
            return Err(DedupError::InvalidInput {
                context: ErrorContext::stage("uri"),
                message:
                    "URI mark shards must be positive and total mark capacity must cover every shard"
                        .to_owned(),
            });
        }
        self.mark_shards = mark_shards;
        self.mark_buffer_capacity = mark_buffer_capacity;
        Ok(self)
    }

    pub fn with_workers(mut self, workers: usize) -> Result<Self, DedupError> {
        if workers == 0 {
            return Err(DedupError::InvalidInput {
                context: ErrorContext::stage("uri"),
                message: "URI worker count must be positive".to_owned(),
            });
        }
        self.workers = workers;
        Ok(self)
    }

    pub fn with_radix_volumes(mut self, volumes: Vec<SpillVolume>) -> Result<Self, DedupError> {
        if volumes.is_empty() {
            return Err(DedupError::InvalidInput {
                context: ErrorContext::stage("uri"),
                message: "URI radix requires at least one temporary volume".to_owned(),
            });
        }
        self.radix_volumes = volumes;
        Ok(self)
    }
}

pub fn run_uri(
    contracts: &[Contract],
    nfts: &[Nft],
    sink: &mut impl HitEventSink,
) -> Result<UriRunResult, DedupError> {
    let executor = RayonChunkExecutor::new(1, "uri")?;
    run_uri_internal(contracts, nfts, sink, None, &NoopProgress, &executor)
}

pub fn run_uri_with_config(
    contracts: &[Contract],
    nfts: &[Nft],
    sink: &mut impl HitEventSink,
    config: &UriExecutionConfig,
) -> Result<UriRunResult, DedupError> {
    let executor = RayonChunkExecutor::new(config.workers, "uri")?;
    run_uri_with_config_progress_and_executor(
        contracts,
        nfts,
        sink,
        config,
        &NoopProgress,
        &executor,
    )
}

pub fn run_uri_with_config_and_progress(
    contracts: &[Contract],
    nfts: &[Nft],
    sink: &mut impl HitEventSink,
    config: &UriExecutionConfig,
    progress: &dyn ProgressObserver,
) -> Result<UriRunResult, DedupError> {
    let executor = RayonChunkExecutor::new(config.workers, "uri")?;
    run_uri_with_config_progress_and_executor(contracts, nfts, sink, config, progress, &executor)
}

pub fn run_uri_with_config_progress_and_executor(
    contracts: &[Contract],
    nfts: &[Nft],
    sink: &mut impl HitEventSink,
    config: &UriExecutionConfig,
    progress: &dyn ProgressObserver,
    executor: &impl ChunkExecutor,
) -> Result<UriRunResult, DedupError> {
    if executor.worker_count() != config.workers {
        return Err(DedupError::InvalidInput {
            context: ErrorContext::stage("uri"),
            message: "executor worker count does not match URI configuration".to_owned(),
        });
    }
    run_uri_internal(contracts, nfts, sink, Some(config), progress, executor)
}

fn run_uri_internal(
    contracts: &[Contract],
    nfts: &[Nft],
    sink: &mut impl HitEventSink,
    config: Option<&UriExecutionConfig>,
    progress: &dyn ProgressObserver,
    executor: &impl ChunkExecutor,
) -> Result<UriRunResult, DedupError> {
    let chain_by_contract: Vec<ChainId> =
        contracts.iter().map(|contract| contract.chain_id).collect();
    let mut token_groups = UriGroupStore::new(config, Dimension::TokenUri, 25)?;
    let mut image_groups = UriGroupStore::new(config, Dimension::ImageUri, 29)?;
    let mut result = UriRunResult::default();

    // Both dimensions share a bounded wave scan. Mapping is parallel, while
    // deterministic group insertion remains coordinator-owned.
    progress.begin_phase("uri_entity_scan", u64::try_from(nfts.len()).ok());
    let wave_size = executor.worker_count().saturating_mul(4_096).max(4_096);
    for wave in nfts.chunks(wave_size) {
        let mapped = executor.map_chunks(wave, 4_096, |chunk| {
            chunk
                .iter()
                .map(|nft| {
                    let chain_id = *chain_by_contract
                        .get(id_index(nft.contract_id.get())?)
                        .ok_or_else(|| DedupError::InvariantViolation {
                            context: ErrorContext::stage("uri"),
                            message: "NFT references missing contract".to_owned(),
                        })?;
                    Ok((
                        nft.token_uri_ref,
                        nft.image_uri_ref,
                        UriMember {
                            chain_id,
                            contract_id: nft.contract_id,
                            nft_id: nft.id,
                        },
                    ))
                })
                .collect::<Result<Vec<_>, DedupError>>()
        })?;
        for (token_uri, image_uri, member) in mapped.into_iter().flatten() {
            if let Some(uri) = token_uri {
                token_groups.add(uri, member)?;
            }
            if let Some(uri) = image_uri {
                image_groups.add(uri, member)?;
            }
        }
        progress.advance(u64::try_from(wave.len()).unwrap_or(u64::MAX));
        progress.check_cancelled("uri")?;
    }
    result.token_groups = token_groups.group_count()?;
    result.image_groups = image_groups.group_count()?;
    token_groups.finish(Dimension::TokenUri, sink, &mut result, progress)?;
    image_groups.finish(Dimension::ImageUri, sink, &mut result, progress)?;
    Ok(result)
}

struct UriGroupStore {
    groups: AHashMap<StringId, UriGroup>,
    spilled: AHashSet<StringId>,
    radix: Option<ExternalRadix>,
    reducer_path: Option<PathBuf>,
    mode: ExecutionMode,
    mark_shards: usize,
    mark_buffer_capacity: usize,
}

impl UriGroupStore {
    fn new(
        config: Option<&UriExecutionConfig>,
        dimension: Dimension,
        seed: u64,
    ) -> Result<Self, DedupError> {
        let mode = config.map_or(ExecutionMode::InMemory, |value| match value.mode {
            ExecutionMode::Auto => ExecutionMode::InMemory,
            mode => mode,
        });
        let mut reducer_path = None;
        let radix = if matches!(mode, ExecutionMode::Hybrid | ExecutionMode::External) {
            let config = config.ok_or_else(|| DedupError::InvariantViolation {
                context: ErrorContext::stage("uri"),
                message: "external URI mode has no capacity configuration".to_owned(),
            })?;
            let subdirectory = match dimension {
                Dimension::TokenUri => "token-uri",
                Dimension::ImageUri => "image-uri",
                _ => {
                    return Err(DedupError::InvariantViolation {
                        context: ErrorContext::stage("uri"),
                        message: "URI store received a non-URI dimension".to_owned(),
                    });
                }
            };
            let directory = config.spill_root.join(subdirectory);
            reducer_path = Some(directory.join("group-reducer.bin"));
            let radix_volumes = config
                .radix_volumes
                .iter()
                .map(|volume| SpillVolume::new(volume.root.join(subdirectory), volume.weight))
                .collect::<Result<Vec<_>, _>>()?;
            Some(if let Some((budget, bytes)) = &config.radix_memory_budget {
                ExternalRadix::create_budgeted_striped(
                    radix_volumes,
                    config.radix_partition_bits,
                    config.max_open_spill_files,
                    config.max_records_per_partition,
                    budget,
                    *bytes,
                )?
            } else {
                ExternalRadix::create_striped(
                    radix_volumes,
                    config.radix_partition_bits,
                    config.max_open_spill_files,
                    config.max_records_per_partition,
                )?
            })
        } else {
            None
        };
        Ok(Self {
            groups: AHashMap::with_hasher(RandomState::with_seeds(
                seed,
                seed + 1,
                seed + 2,
                seed + 3,
            )),
            spilled: AHashSet::with_hasher(RandomState::with_seeds(
                seed + 4,
                seed + 5,
                seed + 6,
                seed + 7,
            )),
            radix,
            reducer_path,
            mode,
            mark_shards: config.map_or(1, |value| value.mark_shards),
            mark_buffer_capacity: config.map_or(4_096, |value| value.mark_buffer_capacity),
        })
    }

    fn add(&mut self, uri: StringId, member: UriMember) -> Result<(), DedupError> {
        // Hybrid uses the same globally bounded external member stream as
        // External. A per-group threshold alone cannot cap the sum of many
        // individually cold resident groups.
        if matches!(self.mode, ExecutionMode::Hybrid | ExecutionMode::External)
            || self.spilled.contains(&uri)
        {
            self.spilled.insert(uri);
            return self.push_spill(uri, member);
        }
        let group = self.groups.entry(uri).or_insert_with(UriGroup::new);
        add_group_member(group, member);
        Ok(())
    }

    fn push_spill(&mut self, uri: StringId, member: UriMember) -> Result<(), DedupError> {
        self.radix
            .as_mut()
            .ok_or_else(|| DedupError::InvariantViolation {
                context: ErrorContext::stage("uri"),
                message: "URI member marked spilled without a radix writer".to_owned(),
            })?
            .push(RadixRecord {
                key: uri.as_u64(),
                payload: [
                    u64::from(member.chain_id.get()),
                    member.contract_id.as_u64(),
                    member.nft_id.as_u64(),
                ],
            })
    }

    fn group_count(&self) -> Result<u64, DedupError> {
        u64::try_from(self.groups.len().saturating_add(self.spilled.len())).map_err(|_| {
            DedupError::CounterOverflow {
                counter: "uri_groups",
            }
        })
    }

    fn finish(
        self,
        dimension: Dimension,
        sink: &mut impl HitEventSink,
        result: &mut UriRunResult,
        progress: &dyn ProgressObserver,
    ) -> Result<(), DedupError> {
        let (resident_phase, sort_phase, reduce_phase) = match dimension {
            Dimension::TokenUri => (
                "token_uri_resident_groups",
                "token_uri_external_sort",
                "token_uri_external_reduce",
            ),
            Dimension::ImageUri => (
                "image_uri_resident_groups",
                "image_uri_external_sort",
                "image_uri_external_reduce",
            ),
            _ => {
                return Err(DedupError::InvariantViolation {
                    context: ErrorContext::stage("uri"),
                    message: "URI store received a non-URI dimension".to_owned(),
                });
            }
        };
        progress.begin_phase(resident_phase, u64::try_from(self.groups.len()).ok());
        classify_groups(&self.groups, dimension, sink, result, progress)?;
        let Some(radix) = self.radix else {
            return Ok(());
        };
        let reducer_path = self
            .reducer_path
            .ok_or_else(|| DedupError::InvariantViolation {
                context: ErrorContext::stage("uri"),
                message: "spilled URI store has no reducer path".to_owned(),
            })?;
        let mut reducer =
            SpilledGroupReducer::new(reducer_path, self.mark_shards, self.mark_buffer_capacity)?;
        let stats = radix.finish_with_progress(progress, sort_phase, reduce_phase, |record| {
            reducer.push(record, dimension, result, progress)
        })?;
        reducer.finish(dimension, sink, result, progress)?;
        result.spill_handle_touches = result
            .spill_handle_touches
            .checked_add(stats.handle_touches)
            .ok_or(DedupError::CounterOverflow {
                counter: "uri_spill_handle_touches",
            })?;
        result
            .counters
            .uri_radix_handle_touches(stats.handle_touches)?;
        result.counters.uri_spilled_members(stats.records)?;
        result.counters.spill_bytes(stats.spill_bytes)?;
        Ok(())
    }
}

struct SpilledGroupReducer {
    path: PathBuf,
    writer: BufWriter<File>,
    current_key: Option<u64>,
    partial: UriGroupPartial,
    hit_shards: Option<SpilledHitShards>,
}

#[derive(Default)]
struct UriGroupPartial {
    contracts_by_chain: BTreeMap<ChainId, u64>,
    previous_contract: Option<(ChainId, ContractId)>,
    member_count: u64,
}

impl UriGroupPartial {
    fn observe(&mut self, member: UriMember) -> Result<(), DedupError> {
        if self.previous_contract != Some((member.chain_id, member.contract_id)) {
            let count = self.contracts_by_chain.entry(member.chain_id).or_default();
            *count = count.checked_add(1).ok_or(DedupError::CounterOverflow {
                counter: "uri_spill_distinct_contracts",
            })?;
            self.previous_contract = Some((member.chain_id, member.contract_id));
        }
        self.member_count =
            self.member_count
                .checked_add(1)
                .ok_or(DedupError::CounterOverflow {
                    counter: "uri_spill_group_members",
                })?;
        Ok(())
    }

    fn clear(&mut self) {
        self.contracts_by_chain.clear();
        self.previous_contract = None;
        self.member_count = 0;
    }
}

impl SpilledGroupReducer {
    fn new(
        path: PathBuf,
        mark_shards: usize,
        mark_buffer_capacity: usize,
    ) -> Result<Self, DedupError> {
        let shard_root = path.with_extension("mark-shards");
        let writer = BufWriter::with_capacity(64 * 1024, File::create(&path)?);
        Ok(Self {
            path,
            writer,
            current_key: None,
            partial: UriGroupPartial::default(),
            hit_shards: Some(SpilledHitShards::new(
                &shard_root,
                mark_shards,
                mark_buffer_capacity,
            )?),
        })
    }

    fn push(
        &mut self,
        record: RadixRecord,
        dimension: Dimension,
        result: &mut UriRunResult,
        progress: &dyn ProgressObserver,
    ) -> Result<(), DedupError> {
        if self.current_key.is_some_and(|key| key != record.key) {
            self.finish_current(dimension, result, progress)?;
        }
        if self.current_key.is_none() {
            self.current_key = Some(record.key);
        }
        let member = decode_member(record)?;
        self.partial.observe(member)?;
        write_spilled_record(&mut self.writer, record)?;
        result.max_spill_reducer_buffered_members =
            result.max_spill_reducer_buffered_members.max(1);
        Ok(())
    }

    fn finish(
        mut self,
        dimension: Dimension,
        sink: &mut impl HitEventSink,
        result: &mut UriRunResult,
        progress: &dyn ProgressObserver,
    ) -> Result<(), DedupError> {
        self.finish_current(dimension, result, progress)?;
        self.hit_shards
            .take()
            .ok_or_else(|| DedupError::InvariantViolation {
                context: ErrorContext::stage("uri"),
                message: "URI hit shards were already finalized".to_owned(),
            })?
            .finish(sink, result)?;
        drop(self.writer);
        fs::remove_file(&self.path)?;
        Ok(())
    }

    fn finish_current(
        &mut self,
        dimension: Dimension,
        result: &mut UriRunResult,
        progress: &dyn ProgressObserver,
    ) -> Result<(), DedupError> {
        if self.current_key.is_none() {
            return Ok(());
        }
        self.writer.flush()?;
        let mut reader = BufReader::with_capacity(64 * 1024, File::open(&self.path)?);
        let present_chains: Vec<_> = self.partial.contracts_by_chain.keys().copied().collect();
        let hit_shards =
            self.hit_shards
                .as_mut()
                .ok_or_else(|| DedupError::InvariantViolation {
                    context: ErrorContext::stage("uri"),
                    message: "URI hit shards are unavailable during group reduction".to_owned(),
                })?;
        for _ in 0..self.partial.member_count {
            let member = decode_member(read_spilled_record(&mut reader)?)?;
            classify_spilled_member(
                member,
                &self.partial.contracts_by_chain,
                &present_chains,
                dimension,
                hit_shards,
                result,
            )?;
        }
        let reducer_touches =
            self.partial
                .member_count
                .checked_mul(2)
                .ok_or(DedupError::CounterOverflow {
                    counter: "uri_spill_reducer_touches",
                })?;
        result.spill_handle_touches = result
            .spill_handle_touches
            .checked_add(reducer_touches)
            .ok_or(DedupError::CounterOverflow {
                counter: "uri_spill_handle_touches",
            })?;
        result.counters.uri_radix_handle_touches(reducer_touches)?;
        result
            .counters
            .spill_bytes(self.partial.member_count.checked_mul(32).ok_or(
                DedupError::CounterOverflow {
                    counter: "uri_spill_reducer_bytes",
                },
            )?)?;
        progress.check_cancelled("uri")?;

        drop(reader);
        self.writer.get_mut().set_len(0)?;
        self.writer.seek(SeekFrom::Start(0))?;
        self.current_key = None;
        self.partial.clear();
        Ok(())
    }
}

fn write_spilled_record(writer: &mut impl Write, record: RadixRecord) -> Result<(), DedupError> {
    writer.write_all(&record.key.to_le_bytes())?;
    for field in record.payload {
        writer.write_all(&field.to_le_bytes())?;
    }
    Ok(())
}

fn read_spilled_record(reader: &mut impl Read) -> Result<RadixRecord, DedupError> {
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

struct SpilledHitShards {
    root: PathBuf,
    paths: Vec<PathBuf>,
    buffers: Vec<PairReducerBuffer<HitEvent>>,
    record_counts: Vec<u64>,
    buffered_events: usize,
    max_buffered_events: usize,
}

impl SpilledHitShards {
    fn new(
        root: &Path,
        shard_count: usize,
        total_buffer_capacity: usize,
    ) -> Result<Self, DedupError> {
        fs::create_dir_all(root)?;
        let capacity_per_shard = total_buffer_capacity.div_ceil(shard_count).max(1);
        let mut paths = Vec::with_capacity(shard_count);
        let mut buffers = Vec::with_capacity(shard_count);
        for shard in 0..shard_count {
            let path = root.join(format!("nft-{shard:04}.bin"));
            drop(File::create(&path)?);
            paths.push(path);
            buffers.push(PairReducerBuffer::new(capacity_per_shard)?);
        }
        Ok(Self {
            root: root.to_path_buf(),
            paths,
            buffers,
            record_counts: vec![0; shard_count],
            buffered_events: 0,
            max_buffered_events: 0,
        })
    }

    fn push(&mut self, event: HitEvent) -> Result<(), DedupError> {
        let shard_count =
            u64::try_from(self.buffers.len()).map_err(|_| DedupError::CounterOverflow {
                counter: "uri_mark_shards",
            })?;
        let shard = usize::try_from(event.entity_id % shard_count).map_err(|_| {
            DedupError::CounterOverflow {
                counter: "uri_mark_shard_index",
            }
        })?;
        if let Err(event) = self.buffers[shard].push(event) {
            self.flush_shard(shard)?;
            self.buffers[shard]
                .push(event)
                .map_err(|_| DedupError::InvariantViolation {
                    context: ErrorContext::stage("uri"),
                    message: "empty URI mark shard rejected an event".to_owned(),
                })?;
        }
        self.buffered_events =
            self.buffered_events
                .checked_add(1)
                .ok_or(DedupError::CounterOverflow {
                    counter: "uri_mark_buffered_events",
                })?;
        self.max_buffered_events = self.max_buffered_events.max(self.buffered_events);
        Ok(())
    }

    fn flush_shard(&mut self, shard: usize) -> Result<(), DedupError> {
        let pending = self.buffers[shard].len();
        if pending == 0 {
            return Ok(());
        }
        let mut writer = BufWriter::with_capacity(
            64 * 1024,
            OpenOptions::new().append(true).open(&self.paths[shard])?,
        );
        for event in self.buffers[shard].drain() {
            write_hit_event(&mut writer, event)?;
        }
        writer.flush()?;
        self.record_counts[shard] = self.record_counts[shard]
            .checked_add(
                u64::try_from(pending).map_err(|_| DedupError::CounterOverflow {
                    counter: "uri_mark_shard_records",
                })?,
            )
            .ok_or(DedupError::CounterOverflow {
                counter: "uri_mark_shard_records",
            })?;
        self.buffered_events = self.buffered_events.checked_sub(pending).ok_or_else(|| {
            DedupError::InvariantViolation {
                context: ErrorContext::stage("uri"),
                message: "URI mark buffer accounting underflow".to_owned(),
            }
        })?;
        Ok(())
    }

    fn finish(
        mut self,
        sink: &mut impl HitEventSink,
        result: &mut UriRunResult,
    ) -> Result<(), DedupError> {
        for shard in 0..self.buffers.len() {
            self.flush_shard(shard)?;
        }
        if self.buffered_events != 0 {
            return Err(DedupError::InvariantViolation {
                context: ErrorContext::stage("uri"),
                message: "URI mark buffers were not fully flushed".to_owned(),
            });
        }
        result.max_spill_hit_buffered_events = result.max_spill_hit_buffered_events.max(
            u64::try_from(self.max_buffered_events).map_err(|_| DedupError::CounterOverflow {
                counter: "uri_mark_buffered_events",
            })?,
        );
        result.spill_hit_shards =
            result
                .spill_hit_shards
                .max(
                    u64::try_from(self.paths.len()).map_err(|_| DedupError::CounterOverflow {
                        counter: "uri_mark_shards",
                    })?,
                );
        for shard in 0..self.paths.len() {
            let mut reader = BufReader::with_capacity(64 * 1024, File::open(&self.paths[shard])?);
            for _ in 0..self.record_counts[shard] {
                submit_hit_event(read_hit_event(&mut reader)?, sink, result)?;
            }
            ensure_eof(&mut reader, "URI mark shard has trailing bytes")?;
            result
                .counters
                .spill_bytes(self.record_counts[shard].checked_mul(32).ok_or(
                    DedupError::CounterOverflow {
                        counter: "uri_mark_shard_bytes",
                    },
                )?)?;
            fs::remove_file(&self.paths[shard])?;
        }
        fs::remove_dir(&self.root)?;
        Ok(())
    }
}

fn write_hit_event(writer: &mut impl Write, event: HitEvent) -> Result<(), DedupError> {
    let fields = [
        dimension_code(event.dimension),
        entity_kind_code(event.entity_kind),
        scope_code(event.scope),
        event.entity_id,
    ];
    for field in fields {
        writer.write_all(&field.to_le_bytes())?;
    }
    Ok(())
}

fn read_hit_event(reader: &mut impl Read) -> Result<HitEvent, DedupError> {
    let mut fields = [0_u64; 4];
    for field in &mut fields {
        let mut bytes = [0_u8; 8];
        reader.read_exact(&mut bytes)?;
        *field = u64::from_le_bytes(bytes);
    }
    Ok(HitEvent {
        dimension: decode_dimension(fields[0])?,
        scope: decode_scope(fields[2])?,
        entity_kind: decode_entity_kind(fields[1])?,
        entity_id: fields[3],
    })
}

fn dimension_code(dimension: Dimension) -> u64 {
    match dimension {
        Dimension::Name => 0,
        Dimension::TokenUri => 1,
        Dimension::ImageUri => 2,
        Dimension::Metadata => 3,
    }
}

fn decode_dimension(value: u64) -> Result<Dimension, DedupError> {
    match value {
        0 => Ok(Dimension::Name),
        1 => Ok(Dimension::TokenUri),
        2 => Ok(Dimension::ImageUri),
        3 => Ok(Dimension::Metadata),
        _ => Err(invalid_hit_record("invalid dimension")),
    }
}

fn entity_kind_code(kind: EntityKind) -> u64 {
    match kind {
        EntityKind::Contract => 0,
        EntityKind::Nft => 1,
    }
}

fn decode_entity_kind(value: u64) -> Result<EntityKind, DedupError> {
    match value {
        0 => Ok(EntityKind::Contract),
        1 => Ok(EntityKind::Nft),
        _ => Err(invalid_hit_record("invalid entity kind")),
    }
}

fn scope_code(scope: ScopeId) -> u64 {
    match scope {
        ScopeId::Intra(chain) => u64::from(chain.get()),
        ScopeId::CrossSummary(chain) => (1_u64 << 32) | u64::from(chain.get()),
        ScopeId::Matrix { primary, secondary } => {
            (2_u64 << 32) | (u64::from(primary.get()) << 16) | u64::from(secondary.get())
        }
    }
}

fn decode_scope(value: u64) -> Result<ScopeId, DedupError> {
    let tag = value >> 32;
    let primary = ChainId::new(
        u16::try_from((value >> 16) & u64::from(u16::MAX))
            .map_err(|_| invalid_hit_record("invalid primary chain"))?,
    );
    let secondary = ChainId::new(
        u16::try_from(value & u64::from(u16::MAX))
            .map_err(|_| invalid_hit_record("invalid secondary chain"))?,
    );
    match tag {
        0 => Ok(ScopeId::Intra(secondary)),
        1 => Ok(ScopeId::CrossSummary(secondary)),
        2 => Ok(ScopeId::Matrix { primary, secondary }),
        _ => Err(invalid_hit_record("invalid scope tag")),
    }
}

fn invalid_hit_record(message: &str) -> DedupError {
    DedupError::ArtifactMismatch {
        context: ErrorContext::stage("uri"),
        message: message.to_owned(),
    }
}

fn ensure_eof(reader: &mut impl Read, message: &str) -> Result<(), DedupError> {
    let mut byte = [0_u8; 1];
    match reader.read(&mut byte)? {
        0 => Ok(()),
        _ => Err(invalid_hit_record(message)),
    }
}

fn classify_spilled_member(
    member: UriMember,
    contracts_by_chain: &BTreeMap<ChainId, u64>,
    present_chains: &[ChainId],
    dimension: Dimension,
    hit_shards: &mut SpilledHitShards,
    result: &mut UriRunResult,
) -> Result<(), DedupError> {
    result.member_accesses =
        result
            .member_accesses
            .checked_add(1)
            .ok_or(DedupError::CounterOverflow {
                counter: "uri_member_accesses",
            })?;
    result.counters.uri_member_accesses(1)?;
    if contracts_by_chain
        .get(&member.chain_id)
        .is_some_and(|count| *count >= 2)
    {
        hit_shards.push(hit_event(
            member.nft_id,
            dimension,
            ScopeId::Intra(member.chain_id),
        ))?;
    }
    let mut cross = false;
    for secondary in present_chains
        .iter()
        .copied()
        .filter(|chain| *chain != member.chain_id)
    {
        cross = true;
        hit_shards.push(hit_event(
            member.nft_id,
            dimension,
            ScopeId::Matrix {
                primary: member.chain_id,
                secondary,
            },
        ))?;
    }
    if cross {
        hit_shards.push(hit_event(
            member.nft_id,
            dimension,
            ScopeId::CrossSummary(member.chain_id),
        ))?;
    }
    Ok(())
}

fn add_group_member(group: &mut UriGroup, member: UriMember) {
    group
        .contracts_by_chain
        .entry(member.chain_id)
        .or_insert_with(|| AHashSet::with_hasher(RandomState::with_seeds(33, 34, 35, 36)))
        .insert(member.contract_id);
    group.members.push(member);
}

fn decode_member(record: RadixRecord) -> Result<UriMember, DedupError> {
    Ok(UriMember {
        chain_id: ChainId::new(
            u16::try_from(record.payload[0])
                .map_err(|_| invalid_spill_record("chain ID does not fit u16", record))?,
        ),
        contract_id: ContractId::new(EntityId::try_from(record.payload[1]).map_err(|_| {
            invalid_spill_record("contract ID does not fit configured EntityId", record)
        })?),
        nft_id: NftId::new(EntityId::try_from(record.payload[2]).map_err(|_| {
            invalid_spill_record("NFT ID does not fit configured EntityId", record)
        })?),
    })
}

fn invalid_spill_record(message: &str, record: RadixRecord) -> DedupError {
    DedupError::ArtifactMismatch {
        context: ErrorContext {
            stage: "uri",
            partition: None,
            stable_object_id: Some(record.key),
        },
        message: message.to_owned(),
    }
}

fn classify_groups(
    groups: &AHashMap<StringId, UriGroup>,
    dimension: Dimension,
    sink: &mut impl HitEventSink,
    result: &mut UriRunResult,
    progress: &dyn ProgressObserver,
) -> Result<(), DedupError> {
    for group in groups.values() {
        classify_group(group, dimension, sink, result)?;
        progress.advance(1);
        progress.check_cancelled("uri")?;
    }
    Ok(())
}

fn classify_group(
    group: &UriGroup,
    dimension: Dimension,
    sink: &mut impl HitEventSink,
    result: &mut UriRunResult,
) -> Result<(), DedupError> {
    let mut present_chains: Vec<ChainId> = group.contracts_by_chain.keys().copied().collect();
    present_chains.sort_unstable();
    if present_chains.len() == 1
        && group
            .contracts_by_chain
            .get(&present_chains[0])
            .is_some_and(|contracts| contracts.len() < 2)
    {
        let skipped =
            u64::try_from(group.members.len()).map_err(|_| DedupError::CounterOverflow {
                counter: "uri_member_accesses",
            })?;
        result.member_accesses =
            result
                .member_accesses
                .checked_add(skipped)
                .ok_or(DedupError::CounterOverflow {
                    counter: "uri_member_accesses",
                })?;
        result.counters.uri_member_accesses(skipped)?;
        return Ok(());
    }
    for member in &group.members {
        result.member_accesses =
            result
                .member_accesses
                .checked_add(1)
                .ok_or(DedupError::CounterOverflow {
                    counter: "uri_member_accesses",
                })?;
        result.counters.uri_member_accesses(1)?;
        if group
            .contracts_by_chain
            .get(&member.chain_id)
            .is_some_and(|contracts| contracts.len() >= 2)
        {
            emit(
                member.nft_id,
                dimension,
                ScopeId::Intra(member.chain_id),
                sink,
                result,
            )?;
        }
        let mut cross = false;
        for secondary in present_chains
            .iter()
            .copied()
            .filter(|chain| *chain != member.chain_id)
        {
            cross = true;
            emit(
                member.nft_id,
                dimension,
                ScopeId::Matrix {
                    primary: member.chain_id,
                    secondary,
                },
                sink,
                result,
            )?;
        }
        if cross {
            emit(
                member.nft_id,
                dimension,
                ScopeId::CrossSummary(member.chain_id),
                sink,
                result,
            )?;
        }
    }
    Ok(())
}

fn emit(
    nft_id: NftId,
    dimension: Dimension,
    scope: ScopeId,
    sink: &mut impl HitEventSink,
    result: &mut UriRunResult,
) -> Result<(), DedupError> {
    submit_hit_event(hit_event(nft_id, dimension, scope), sink, result)
}

fn hit_event(nft_id: NftId, dimension: Dimension, scope: ScopeId) -> HitEvent {
    HitEvent {
        dimension,
        scope,
        entity_kind: EntityKind::Nft,
        entity_id: nft_id.as_u64(),
    }
}

fn submit_hit_event(
    event: HitEvent,
    sink: &mut impl HitEventSink,
    result: &mut UriRunResult,
) -> Result<(), DedupError> {
    sink.submit(event)?;
    result.counters.hit_events(1)?;
    result.bitmap_word_operations =
        result
            .bitmap_word_operations
            .checked_add(1)
            .ok_or(DedupError::CounterOverflow {
                counter: "uri_bitmap_word_operations",
            })?;
    result.counters.uri_bitmap_word_operations(1)?;
    Ok(())
}

fn id_index(value: dedup_model::EntityId) -> Result<usize, DedupError> {
    usize::try_from(value).map_err(|_| DedupError::InvariantViolation {
        context: ErrorContext::stage("uri"),
        message: "ContractId does not fit usize".to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use dedup_model::{ContractId, EntityId, StringId};
    use std::collections::BTreeSet;

    #[derive(Default)]
    struct RecordingSink(Vec<HitEvent>);

    impl HitEventSink for RecordingSink {
        fn submit(&mut self, event: HitEvent) -> Result<(), DedupError> {
            self.0.push(event);
            Ok(())
        }
    }

    fn contract(id: EntityId, chain: u16) -> Contract {
        Contract {
            id: ContractId::new(id),
            chain_id: ChainId::new(chain),
            address_ref: StringId::new(id),
            name_ref: None,
            first_nft_id: NftId::new(id),
            nft_count: 1,
        }
    }

    fn nft(id: EntityId, contract: EntityId, token_uri: EntityId) -> Nft {
        Nft {
            id: NftId::new(id),
            contract_id: ContractId::new(contract),
            token_id_ref: StringId::new(id),
            token_uri_ref: Some(StringId::new(token_uri)),
            image_uri_ref: None,
            has_metadata: false,
        }
    }

    #[test]
    fn intra_requires_distinct_contracts_and_matrix_is_directional() {
        let contracts = vec![contract(0, 0), contract(1, 0), contract(2, 1)];
        let nfts = vec![nft(0, 0, 7), nft(1, 0, 7), nft(2, 2, 7)];
        let mut sink = RecordingSink::default();
        run_uri(&contracts, &nfts, &mut sink).unwrap();
        assert!(!sink.0.iter().any(|event| {
            event.scope == ScopeId::Intra(ChainId::new(0)) && matches!(event.entity_id, 0 | 1)
        }));
        assert!(sink.0.iter().any(|event| {
            event.scope
                == ScopeId::Matrix {
                    primary: ChainId::new(0),
                    secondary: ChainId::new(1),
                }
        }));
        assert!(sink.0.iter().any(|event| {
            event.scope
                == ScopeId::Matrix {
                    primary: ChainId::new(1),
                    secondary: ChainId::new(0),
                }
        }));
    }

    #[test]
    fn same_uri_in_two_contracts_marks_only_actual_members() {
        let contracts = vec![contract(0, 0), contract(1, 0)];
        let nfts = vec![nft(0, 0, 7), nft(1, 1, 7), nft(2, 1, 8)];
        let mut sink = RecordingSink::default();
        run_uri(&contracts, &nfts, &mut sink).unwrap();
        let hits: BTreeSet<_> = sink
            .0
            .iter()
            .filter(|event| event.scope == ScopeId::Intra(ChainId::new(0)))
            .map(|event| event.entity_id)
            .collect();
        assert_eq!(hits, BTreeSet::from([0, 1]));
    }

    #[test]
    fn in_memory_hybrid_and_external_modes_are_identical() {
        let contracts = vec![contract(0, 0), contract(1, 0), contract(2, 1)];
        let nfts = vec![nft(0, 0, 7), nft(1, 1, 7), nft(2, 2, 8)];

        let run_mode = |mode, root: PathBuf| {
            let mut sink = RecordingSink::default();
            let config = UriExecutionConfig::new(mode, root, 1, 1, 2, 16)
                .unwrap()
                .with_mark_shards(2, 2)
                .unwrap();
            let result = run_uri_with_config(&contracts, &nfts, &mut sink, &config).unwrap();
            sink.0.sort_unstable_by_key(|event| {
                (
                    event.dimension,
                    event.scope,
                    event.entity_kind,
                    event.entity_id,
                )
            });
            (sink.0, result)
        };

        let directory = tempfile::tempdir().unwrap();
        let mut resident_sink = RecordingSink::default();
        let resident = run_uri(&contracts, &nfts, &mut resident_sink).unwrap();
        resident_sink.0.sort_unstable_by_key(|event| {
            (
                event.dimension,
                event.scope,
                event.entity_kind,
                event.entity_id,
            )
        });
        let (hybrid_hits, hybrid) =
            run_mode(ExecutionMode::Hybrid, directory.path().join("hybrid"));
        let (external_hits, external) =
            run_mode(ExecutionMode::External, directory.path().join("external"));

        assert_eq!(resident_sink.0, hybrid_hits);
        assert_eq!(resident_sink.0, external_hits);
        assert_eq!(resident.member_accesses, hybrid.member_accesses);
        assert_eq!(resident.member_accesses, external.member_accesses);
        assert_eq!(hybrid.counters.uri_spilled_members, 3);
        assert_eq!(hybrid.spill_handle_touches, 18);
        assert_eq!(external.counters.uri_spilled_members, 3);
        assert_eq!(external.spill_handle_touches, 18);
        assert_eq!(hybrid.max_spill_reducer_buffered_members, 1);
        assert_eq!(external.max_spill_reducer_buffered_members, 1);
        assert!(hybrid.max_spill_hit_buffered_events <= 2);
        assert!(external.max_spill_hit_buffered_events <= 2);
        assert_eq!(hybrid.spill_hit_shards, 2);
        assert_eq!(external.spill_hit_shards, 2);
    }

    #[test]
    fn mark_shard_capacity_is_constructor_enforced() {
        let error = UriExecutionConfig::new(ExecutionMode::External, "spill", 1, 1, 1, 1)
            .unwrap()
            .with_mark_shards(2, 1)
            .unwrap_err();
        assert!(matches!(error, DedupError::InvalidInput { .. }));
    }

    #[test]
    fn uri_work_counters_scale_linearly_for_unique_and_hot_groups() {
        let count = 1_000_u32;
        let contracts: Vec<_> = (0..count)
            .map(|id| contract(EntityId::from(id), 0))
            .collect();
        let unique_nfts: Vec<_> = (0..count)
            .map(|id| nft(EntityId::from(id), EntityId::from(id), EntityId::from(id)))
            .collect();
        let unique = run_uri(&contracts, &unique_nfts, &mut RecordingSink::default()).unwrap();
        assert_eq!(unique.member_accesses, u64::from(count));
        assert_eq!(unique.counters.hit_events, 0);

        let hot_nfts: Vec<_> = (0..count)
            .map(|id| {
                nft(
                    EntityId::from(id),
                    EntityId::from(id),
                    EntityId::from(7_u32),
                )
            })
            .collect();
        let directory = tempfile::tempdir().unwrap();
        let config = UriExecutionConfig::new(
            ExecutionMode::External,
            directory.path(),
            32,
            4,
            16,
            usize::try_from(count).unwrap(),
        )
        .unwrap();
        let hot = run_uri_with_config(
            &contracts,
            &hot_nfts,
            &mut RecordingSink::default(),
            &config,
        )
        .unwrap();
        assert_eq!(hot.member_accesses, u64::from(count));
        assert_eq!(hot.counters.uri_spilled_members, u64::from(count));
        assert_eq!(hot.spill_handle_touches, u64::from(count) * 6);
        assert_eq!(hot.max_spill_reducer_buffered_members, 1);
        assert_eq!(hot.counters.hit_events, u64::from(count));
    }
}
