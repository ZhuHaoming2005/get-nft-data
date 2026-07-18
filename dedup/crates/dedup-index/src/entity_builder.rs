use crate::StringDictionary;
use crate::external_strings::{ExternalStringDictionary, ExternalStringStore, OccurrenceMapReader};
use ahash::{AHashMap, RandomState};
use dedup_model::{
    ChainId, Contract, ContractId, DedupError, EntityArtifacts, EntityId, ErrorContext,
    ExecutionMode, InputRow, MetadataSourceValidator, Nft, NftId, NoopProgress,
    PersistedEntityArtifacts, ProgressObserver, SourceOrder, StringId,
};
use dedup_storage::{
    EntityArtifactFiles, SpillVolume, write_entity_artifact, write_entity_artifact_from_files,
};
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::collections::{BTreeMap, BTreeSet, hash_map::Entry};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, Write};
use std::path::{Path, PathBuf};
use tempfile::NamedTempFile;

const NONE_ID: u64 = u64::MAX;
const EXTERNAL_ROW_FIELDS: usize = 10;
const EXTERNAL_SORT_FIELDS: usize = 12;
const DEFAULT_EXTERNAL_SORT_ROWS: usize = 65_536;
const DEFAULT_EXTERNAL_MERGE_FAN_IN: usize = 15;
const DEFAULT_EXTERNAL_STRING_SORT_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug)]
struct PendingNft {
    name: Option<StringId>,
    token_uri: Option<StringId>,
    image_uri: Option<StringId>,
    metadata: Option<(SourceOrder, StoredMetadata)>,
}

#[derive(Clone, Debug)]
enum StoredMetadata {
    Resident(String),
    Spilled { offset: u64, length: u64 },
}

#[derive(Debug)]
enum MetadataStore {
    Resident,
    Spilled {
        file: NamedTempFile,
        writer: Option<BufWriter<File>>,
        reader: Option<BufReader<File>>,
        bytes: u64,
    },
}

impl MetadataStore {
    fn store(&mut self, content: String) -> Result<StoredMetadata, DedupError> {
        match self {
            Self::Resident => Ok(StoredMetadata::Resident(content)),
            Self::Spilled { writer, bytes, .. } => {
                let length =
                    u64::try_from(content.len()).map_err(|_| DedupError::CounterOverflow {
                        counter: "entity_metadata_spill_bytes",
                    })?;
                let offset = *bytes;
                writer
                    .as_mut()
                    .ok_or_else(|| DedupError::InvariantViolation {
                        context: ErrorContext::stage("entity"),
                        message: "metadata spill writer is already closed".to_owned(),
                    })?
                    .write_all(content.as_bytes())?;
                *bytes = bytes
                    .checked_add(length)
                    .ok_or(DedupError::CounterOverflow {
                        counter: "entity_metadata_spill_bytes",
                    })?;
                Ok(StoredMetadata::Spilled { offset, length })
            }
        }
    }

    fn prepare_read(&mut self) -> Result<(), DedupError> {
        if let Self::Spilled {
            file,
            writer,
            reader,
            ..
        } = self
        {
            if let Some(mut active_writer) = writer.take() {
                active_writer.flush()?;
                active_writer.get_ref().sync_all()?;
            }
            *reader = Some(BufReader::new(file.reopen()?));
        }
        Ok(())
    }

    fn read(&mut self, stored: StoredMetadata) -> Result<String, DedupError> {
        match stored {
            StoredMetadata::Resident(content) => Ok(content),
            StoredMetadata::Spilled { offset, length } => {
                let reader = match self {
                    Self::Spilled { reader, .. } => {
                        reader
                            .as_mut()
                            .ok_or_else(|| DedupError::InvariantViolation {
                                context: ErrorContext::stage("entity"),
                                message: "metadata spill reader is not prepared".to_owned(),
                            })?
                    }
                    Self::Resident => {
                        return Err(DedupError::InvariantViolation {
                            context: ErrorContext::stage("entity"),
                            message: "spilled metadata is backed by a resident store".to_owned(),
                        });
                    }
                };
                reader.seek(std::io::SeekFrom::Start(offset))?;
                let mut bytes = vec![
                    0;
                    usize::try_from(length).map_err(|_| {
                        DedupError::InvalidInput {
                            context: ErrorContext::stage("entity"),
                            message: "metadata spill length does not fit usize".to_owned(),
                        }
                    })?
                ];
                reader.read_exact(&mut bytes)?;
                String::from_utf8(bytes).map_err(|error| DedupError::ArtifactMismatch {
                    context: ErrorContext::stage("entity"),
                    message: error.to_string(),
                })
            }
        }
    }

    fn spill_bytes(&self) -> u64 {
        match self {
            Self::Resident => 0,
            Self::Spilled { bytes, .. } => *bytes,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct ExternalRow {
    chain: u64,
    address_ref: u64,
    token_id_ref: u64,
    name_ref: u64,
    token_uri_ref: u64,
    image_uri_ref: u64,
    source_file: u64,
    source_row: u64,
    metadata_offset: u64,
    metadata_length: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ExternalSortRecord {
    chain: u64,
    address_rank: u64,
    token_rank: u64,
    source_file: u64,
    source_row: u64,
    address_ref: u64,
    token_id_ref: u64,
    name_ref: u64,
    token_uri_ref: u64,
    image_uri_ref: u64,
    metadata_offset: u64,
    metadata_length: u64,
}

#[derive(Debug)]
struct ExternalRowStore {
    volumes: Vec<SpillVolume>,
    file: NamedTempFile,
    writer: Option<BufWriter<File>>,
    rows: u64,
    sort_chunk_rows: usize,
    merge_fan_in: usize,
}

impl ExternalRowStore {
    fn new(
        root: &Path,
        volumes: Vec<SpillVolume>,
        sort_chunk_rows: usize,
        merge_fan_in: usize,
    ) -> Result<Self, DedupError> {
        if sort_chunk_rows == 0 || merge_fan_in < 2 {
            return Err(DedupError::InvalidInput {
                context: ErrorContext::stage("entity"),
                message: "external entity sort capacity must be positive and fan-in at least two"
                    .to_owned(),
            });
        }
        if volumes.is_empty() {
            return Err(DedupError::InvalidInput {
                context: ErrorContext::stage("entity"),
                message: "external entity sort requires a spill volume".to_owned(),
            });
        }
        for volume in &volumes {
            std::fs::create_dir_all(&volume.root)?;
        }
        std::fs::create_dir_all(root)?;
        let file = NamedTempFile::new_in(root)?;
        let writer = BufWriter::with_capacity(64 * 1024, file.reopen()?);
        Ok(Self {
            volumes,
            file,
            writer: Some(writer),
            rows: 0,
            sort_chunk_rows,
            merge_fan_in,
        })
    }

    fn push(&mut self, row: ExternalRow) -> Result<(), DedupError> {
        write_external_row(
            self.writer
                .as_mut()
                .ok_or_else(|| DedupError::InvariantViolation {
                    context: ErrorContext::stage("entity"),
                    message: "external entity row writer is closed".to_owned(),
                })?,
            row,
        )?;
        self.rows = self
            .rows
            .checked_add(1)
            .ok_or(DedupError::CounterOverflow {
                counter: "entity_external_rows",
            })?;
        Ok(())
    }

    fn prepare_reader(&mut self) -> Result<BufReader<File>, DedupError> {
        if let Some(mut writer) = self.writer.take() {
            writer.flush()?;
            writer.get_ref().sync_all()?;
        }
        Ok(BufReader::with_capacity(64 * 1024, self.file.reopen()?))
    }

    fn input_spill_bytes(&self) -> u64 {
        self.rows
            .saturating_mul((EXTERNAL_ROW_FIELDS as u64).saturating_mul(8))
    }
}

#[derive(Debug)]
enum EntityMergeStore {
    Resident(AHashMap<(ChainId, String), PendingContract>),
    External(ExternalRowStore),
}

#[derive(Debug)]
enum EntityStringStore {
    Resident(StringDictionary),
    External(ExternalStringStore),
}

#[derive(Clone, Debug)]
struct PendingContract {
    chain: ChainId,
    address_ref: StringId,
    name: Option<StringId>,
    nfts: AHashMap<StringId, PendingNft>,
}

#[derive(Debug)]
pub struct EntityBuildResult {
    pub artifacts: EntityArtifacts,
    pub strings: StringDictionary,
    pub metadata_by_nft: BTreeMap<NftId, String>,
    pub metadata_spill_bytes: u64,
    pub external_handle_spill_bytes: u64,
    pub external_handle_touches: u64,
    pub external_volumes_used: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EntityBuildSummary {
    pub contract_count: u64,
    pub nft_count: u64,
    pub string_count: u64,
    pub metadata_spill_bytes: u64,
    pub external_handle_spill_bytes: u64,
    pub external_handle_touches: u64,
    pub external_volumes_used: u64,
    pub digest_bucket_max: u64,
}

impl EntityBuildResult {
    pub fn into_persisted(self) -> PersistedEntityArtifacts {
        PersistedEntityArtifacts {
            entities: self.artifacts,
            strings: self.strings.values().map(ToOwned::to_owned).collect(),
            metadata_by_nft: self.metadata_by_nft.into_iter().collect(),
        }
    }

    pub fn from_persisted(
        persisted: PersistedEntityArtifacts,
        digest_bucket_limit: usize,
    ) -> Result<Self, DedupError> {
        Ok(Self {
            artifacts: persisted.entities,
            strings: StringDictionary::from_ordered_values(persisted.strings, digest_bucket_limit)?,
            metadata_by_nft: persisted.metadata_by_nft.into_iter().collect(),
            metadata_spill_bytes: 0,
            external_handle_spill_bytes: 0,
            external_handle_touches: 0,
            external_volumes_used: 0,
        })
    }
}

pub struct EntityBuilder<V> {
    chain_ids: BTreeMap<String, ChainId>,
    evm_chains: BTreeMap<String, ()>,
    evm_chain_ids: BTreeMap<ChainId, ()>,
    strings: EntityStringStore,
    merge_store: EntityMergeStore,
    metadata_validator: V,
    metadata_store: MetadataStore,
    external_string_sort_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct EntityExecutionConfig {
    pub mode: ExecutionMode,
    pub spill_root: Option<PathBuf>,
    pub spill_volumes: Vec<SpillVolume>,
    pub external_sort_rows: usize,
    pub external_merge_fan_in: usize,
    pub external_string_sort_bytes: usize,
}

impl EntityExecutionConfig {
    pub fn new(
        mode: ExecutionMode,
        spill_root: Option<PathBuf>,
        external_sort_rows: usize,
        external_merge_fan_in: usize,
    ) -> Result<Self, DedupError> {
        if external_sort_rows == 0 || external_merge_fan_in < 2 {
            return Err(DedupError::InvalidInput {
                context: ErrorContext::stage("entity"),
                message: "external entity capacities are invalid".to_owned(),
            });
        }
        if matches!(mode, ExecutionMode::Hybrid | ExecutionMode::External) && spill_root.is_none() {
            return Err(DedupError::InvalidInput {
                context: ErrorContext::stage("entity"),
                message: "entity spill mode requires a temporary volume".to_owned(),
            });
        }
        let spill_volumes = spill_root
            .iter()
            .map(|root| SpillVolume::new(root.clone(), 1))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            mode,
            spill_root,
            spill_volumes,
            external_sort_rows,
            external_merge_fan_in,
            external_string_sort_bytes: DEFAULT_EXTERNAL_STRING_SORT_BYTES,
        })
    }

    pub fn with_spill_volumes(mut self, volumes: Vec<SpillVolume>) -> Result<Self, DedupError> {
        if matches!(self.mode, ExecutionMode::Hybrid | ExecutionMode::External)
            && volumes.is_empty()
        {
            return Err(DedupError::InvalidInput {
                context: ErrorContext::stage("entity"),
                message: "entity spill mode requires a spill volume".to_owned(),
            });
        }
        self.spill_volumes = volumes;
        Ok(self)
    }

    pub fn with_string_sort_bytes(mut self, bytes: usize) -> Result<Self, DedupError> {
        if bytes == 0 {
            return Err(DedupError::InvalidInput {
                context: ErrorContext::stage("entity"),
                message: "external string sort memory must be positive".to_owned(),
            });
        }
        self.external_string_sort_bytes = bytes;
        Ok(self)
    }
}

impl<V: MetadataSourceValidator> EntityBuilder<V> {
    pub fn new(
        chains: impl IntoIterator<Item = String>,
        evm_chains: impl IntoIterator<Item = String>,
        digest_bucket_limit: usize,
        metadata_validator: V,
    ) -> Result<Self, DedupError> {
        Self::new_with_execution(
            chains,
            evm_chains,
            digest_bucket_limit,
            metadata_validator,
            EntityExecutionConfig::new(
                ExecutionMode::InMemory,
                None,
                DEFAULT_EXTERNAL_SORT_ROWS,
                DEFAULT_EXTERNAL_MERGE_FAN_IN,
            )?,
        )
    }

    pub fn new_with_execution(
        chains: impl IntoIterator<Item = String>,
        evm_chains: impl IntoIterator<Item = String>,
        digest_bucket_limit: usize,
        metadata_validator: V,
        execution: EntityExecutionConfig,
    ) -> Result<Self, DedupError> {
        let mode = execution.mode;
        let spill_root = execution.spill_root.as_deref();
        let mut chain_ids = BTreeMap::new();
        for (index, chain) in chains.into_iter().enumerate() {
            let id = u16::try_from(index).map_err(|_| DedupError::InvalidInput {
                context: ErrorContext::stage("entity"),
                message: "too many chains".to_owned(),
            })?;
            chain_ids.insert(chain.to_lowercase(), ChainId::new(id));
        }
        let evm_chains: BTreeMap<String, ()> = evm_chains
            .into_iter()
            .map(|chain| (chain.to_lowercase(), ()))
            .collect();
        let evm_chain_ids = evm_chains
            .keys()
            .filter_map(|chain| chain_ids.get(chain).copied())
            .map(|chain| (chain, ()))
            .collect();
        let metadata_store = match mode {
            ExecutionMode::Auto | ExecutionMode::InMemory => MetadataStore::Resident,
            ExecutionMode::Hybrid | ExecutionMode::External => {
                let spill_root = spill_root.ok_or_else(|| DedupError::InvalidInput {
                    context: ErrorContext::stage("entity"),
                    message: "entity spill mode requires a temporary volume".to_owned(),
                })?;
                std::fs::create_dir_all(spill_root)?;
                let file = NamedTempFile::new_in(spill_root)?;
                let writer = BufWriter::new(file.reopen()?);
                MetadataStore::Spilled {
                    file,
                    writer: Some(writer),
                    reader: None,
                    bytes: 0,
                }
            }
        };
        let merge_store = match mode {
            ExecutionMode::Hybrid | ExecutionMode::External => {
                EntityMergeStore::External(ExternalRowStore::new(
                    spill_root.ok_or_else(|| DedupError::InvalidInput {
                        context: ErrorContext::stage("entity"),
                        message: "spilled entity mode requires a temporary volume".to_owned(),
                    })?,
                    execution.spill_volumes.clone(),
                    execution.external_sort_rows,
                    execution.external_merge_fan_in,
                )?)
            }
            ExecutionMode::Auto | ExecutionMode::InMemory => EntityMergeStore::Resident(
                AHashMap::with_hasher(RandomState::with_seeds(5, 6, 7, 8)),
            ),
        };
        let strings = match mode {
            ExecutionMode::Auto | ExecutionMode::InMemory => {
                EntityStringStore::Resident(StringDictionary::new(digest_bucket_limit)?)
            }
            ExecutionMode::Hybrid | ExecutionMode::External => EntityStringStore::External(
                ExternalStringStore::new(spill_root.ok_or_else(|| DedupError::InvalidInput {
                    context: ErrorContext::stage("entity"),
                    message: "external string store requires a spill root".to_owned(),
                })?)?,
            ),
        };
        Ok(Self {
            chain_ids,
            evm_chains,
            evm_chain_ids,
            strings,
            merge_store,
            metadata_validator,
            metadata_store,
            external_string_sort_bytes: execution.external_string_sort_bytes,
        })
    }

    pub fn push(&mut self, row: InputRow) -> Result<(), DedupError> {
        let chain_name = row.chain.trim().to_lowercase();
        let chain =
            self.chain_ids
                .get(&chain_name)
                .copied()
                .ok_or_else(|| DedupError::InvalidInput {
                    context: ErrorContext::stage("entity"),
                    message: format!("unknown chain {:?}", row.chain),
                })?;
        let mut address = row.contract_address.trim().to_owned();
        let token_id = row.token_id.trim().to_owned();
        if address.is_empty() || token_id.is_empty() {
            return Err(DedupError::InvalidInput {
                context: ErrorContext::stage("entity"),
                message: "contract address and token id must be non-empty".to_owned(),
            });
        }
        if self.evm_chains.contains_key(&chain_name) {
            address.make_ascii_lowercase();
        }
        let metadata = if self
            .metadata_validator
            .is_valid_metadata(&row.metadata_json)
        {
            Some((
                row.source_order,
                self.metadata_store.store(row.metadata_json)?,
            ))
        } else {
            None
        };

        if let EntityMergeStore::External(rows) = &mut self.merge_store {
            let strings = match &mut self.strings {
                EntityStringStore::External(strings) => strings,
                EntityStringStore::Resident(_) => {
                    return Err(DedupError::InvariantViolation {
                        context: ErrorContext::stage("entity"),
                        message: "external rows use a resident string dictionary".to_owned(),
                    });
                }
            };
            let address_ref = strings.store(address.as_bytes())?;
            let token_id_ref = strings.store(token_id.as_bytes())?;
            let name_ref = external_optional_string(strings, &row.name_norm)?;
            let token_uri_ref = external_optional_string(strings, &row.token_uri_norm)?;
            let image_uri_ref = external_optional_string(strings, &row.image_uri_norm)?;
            let (metadata_offset, metadata_length) = match metadata {
                Some((_, StoredMetadata::Spilled { offset, length })) => (offset, length),
                None => (NONE_ID, 0),
                Some((_, StoredMetadata::Resident(_))) => {
                    return Err(DedupError::InvariantViolation {
                        context: ErrorContext::stage("entity"),
                        message: "external entity row received resident metadata".to_owned(),
                    });
                }
            };
            return rows.push(ExternalRow {
                chain: u64::from(chain.get()),
                address_ref,
                token_id_ref,
                name_ref,
                token_uri_ref,
                image_uri_ref,
                source_file: u64::from(row.source_order.file_ordinal),
                source_row: row.source_order.file_row_number,
                metadata_offset,
                metadata_length,
            });
        }
        let strings = match &mut self.strings {
            EntityStringStore::Resident(strings) => strings,
            EntityStringStore::External(_) => {
                return Err(DedupError::InvariantViolation {
                    context: ErrorContext::stage("entity"),
                    message: "resident rows use an external string dictionary".to_owned(),
                });
            }
        };
        let address_ref = strings.intern(address.as_bytes())?;
        let token_id_ref = strings.intern(token_id.as_bytes())?;
        let name = resident_optional_string(strings, &row.name_norm)?;
        let token_uri = resident_optional_string(strings, &row.token_uri_norm)?;
        let image_uri = resident_optional_string(strings, &row.image_uri_norm)?;
        let contracts = match &mut self.merge_store {
            EntityMergeStore::Resident(contracts) => contracts,
            EntityMergeStore::External(_) => unreachable!("external rows returned above"),
        };
        let contract = contracts
            .entry((chain, address))
            .or_insert_with(|| PendingContract {
                chain,
                address_ref,
                name,
                nfts: AHashMap::with_hasher(RandomState::with_seeds(9, 10, 11, 12)),
            });
        contract.name = merge_optional(
            contract.name,
            name,
            "multiple distinct non-empty names in one contract",
        )?;
        match contract.nfts.entry(token_id_ref) {
            Entry::Vacant(entry) => {
                entry.insert(PendingNft {
                    name,
                    token_uri,
                    image_uri,
                    metadata,
                });
            }
            Entry::Occupied(mut entry) => {
                let nft = entry.get_mut();
                nft.name = merge_optional(nft.name, name, "conflicting NFT name")?;
                nft.token_uri = merge_optional(nft.token_uri, token_uri, "conflicting token URI")?;
                nft.image_uri = merge_optional(nft.image_uri, image_uri, "conflicting image URI")?;
                if let Some(candidate) = metadata
                    && nft
                        .metadata
                        .as_ref()
                        .is_none_or(|current| candidate.0 < current.0)
                {
                    nft.metadata = Some(candidate);
                }
            }
        }
        Ok(())
    }

    pub fn finish(self) -> Result<EntityBuildResult, DedupError> {
        self.finish_with_progress(&NoopProgress)
    }

    pub fn finish_with_progress(
        mut self,
        progress: &dyn ProgressObserver,
    ) -> Result<EntityBuildResult, DedupError> {
        self.metadata_store.prepare_read()?;
        let strings = std::mem::replace(
            &mut self.strings,
            EntityStringStore::Resident(StringDictionary::new(1)?),
        );
        let merge_store = std::mem::replace(
            &mut self.merge_store,
            EntityMergeStore::Resident(AHashMap::with_hasher(RandomState::with_seeds(5, 6, 7, 8))),
        );
        if let EntityMergeStore::External(rows) = merge_store {
            let EntityStringStore::External(strings) = strings else {
                return Err(DedupError::InvariantViolation {
                    context: ErrorContext::stage("entity"),
                    message: "external entity rows have no external string spool".to_owned(),
                });
            };
            let dictionary = strings.finish(
                &rows.volumes,
                rows.sort_chunk_rows.saturating_mul(5).max(1),
                self.external_string_sort_bytes,
                rows.merge_fan_in,
                progress,
            )?;
            return finish_external_entities(
                rows,
                dictionary,
                &self.evm_chain_ids,
                &mut self.metadata_store,
                progress,
            );
        }
        let EntityStringStore::Resident(strings) = strings else {
            return Err(DedupError::InvariantViolation {
                context: ErrorContext::stage("entity"),
                message: "resident entity rows have no resident string dictionary".to_owned(),
            });
        };
        let EntityMergeStore::Resident(contracts) = merge_store else {
            unreachable!("external entity store returned above");
        };
        let mut artifacts = EntityArtifacts::default();
        let mut metadata_by_nft = BTreeMap::new();
        let mut pending_contracts: Vec<_> = contracts.into_iter().collect();
        pending_contracts.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        progress.begin_phase(
            "entity_resident_reduce",
            u64::try_from(pending_contracts.len()).ok(),
        );
        for (_, pending_contract) in pending_contracts {
            let contract_id = checked_id::<ContractId>(artifacts.contracts.len(), ContractId::new)?;
            let first_nft_id = checked_id::<NftId>(artifacts.nfts.len(), NftId::new)?;
            let nft_count = u64::try_from(pending_contract.nfts.len()).map_err(|_| {
                DedupError::InvalidInput {
                    context: ErrorContext::stage("entity"),
                    message: "NFT count exceeds u64".to_owned(),
                }
            })?;
            let mut pending_nfts: Vec<_> = pending_contract.nfts.into_iter().collect();
            if self.evm_chain_ids.contains_key(&pending_contract.chain) {
                for record in &pending_nfts {
                    let token_bytes = strings.resolve(record.0).ok_or_else(|| {
                        DedupError::InvariantViolation {
                            context: ErrorContext::stage("entity"),
                            message: "pending token StringId is missing".to_owned(),
                        }
                    })?;
                    if !is_decimal_token(token_bytes) {
                        return Err(DedupError::InvalidInput {
                            context: ErrorContext::stage("entity"),
                            message: format!(
                                "EVM token id {:?} is not a non-negative integer",
                                String::from_utf8_lossy(token_bytes)
                            ),
                        });
                    }
                }
                pending_nfts.sort_unstable_by(|left, right| {
                    compare_decimal_tokens(
                        strings
                            .resolve(left.0)
                            .expect("validated token StringId must resolve"),
                        strings
                            .resolve(right.0)
                            .expect("validated token StringId must resolve"),
                    )
                });
            } else {
                pending_nfts.sort_unstable_by(|left, right| {
                    strings.resolve(left.0).cmp(&strings.resolve(right.0))
                });
            }
            for (token_id_ref, pending_nft) in pending_nfts {
                let nft_id = checked_id::<NftId>(artifacts.nfts.len(), NftId::new)?;
                if let Some((_, metadata)) = pending_nft.metadata {
                    metadata_by_nft.insert(nft_id, self.metadata_store.read(metadata)?);
                }
                artifacts.nfts.push(Nft {
                    id: nft_id,
                    contract_id,
                    token_id_ref,
                    token_uri_ref: pending_nft.token_uri,
                    image_uri_ref: pending_nft.image_uri,
                    has_metadata: metadata_by_nft.contains_key(&nft_id),
                });
            }
            artifacts.contracts.push(Contract {
                id: contract_id,
                chain_id: pending_contract.chain,
                address_ref: pending_contract.address_ref,
                name_ref: pending_contract.name,
                first_nft_id,
                nft_count,
            });
            progress.advance(1);
            progress.check_cancelled("entity")?;
        }
        let (artifacts, strings) = remap_strings_lexically(artifacts, strings)?;
        Ok(EntityBuildResult {
            artifacts,
            strings,
            metadata_by_nft,
            metadata_spill_bytes: self.metadata_store.spill_bytes(),
            external_handle_spill_bytes: 0,
            external_handle_touches: 0,
            external_volumes_used: 0,
        })
    }

    pub fn finish_to_artifact(
        mut self,
        path: impl AsRef<Path>,
        logical_input_digest: String,
        configuration_digest: String,
        progress: &dyn ProgressObserver,
    ) -> Result<EntityBuildSummary, DedupError> {
        if matches!(self.merge_store, EntityMergeStore::Resident(_)) {
            let result = self.finish_with_progress(progress)?;
            let summary = EntityBuildSummary {
                contract_count: u64::try_from(result.artifacts.contracts.len()).map_err(|_| {
                    DedupError::CounterOverflow {
                        counter: "entity_contract_count",
                    }
                })?,
                nft_count: u64::try_from(result.artifacts.nfts.len()).map_err(|_| {
                    DedupError::CounterOverflow {
                        counter: "entity_nft_count",
                    }
                })?,
                string_count: u64::try_from(result.strings.len()).map_err(|_| {
                    DedupError::CounterOverflow {
                        counter: "entity_string_count",
                    }
                })?,
                metadata_spill_bytes: result.metadata_spill_bytes,
                external_handle_spill_bytes: result.external_handle_spill_bytes,
                external_handle_touches: result.external_handle_touches,
                external_volumes_used: result.external_volumes_used,
                digest_bucket_max: u64::try_from(result.strings.max_digest_bucket_len())
                    .unwrap_or(u64::MAX),
            };
            write_entity_artifact(
                path,
                &result.into_persisted(),
                logical_input_digest,
                configuration_digest,
            )?;
            return Ok(summary);
        }

        self.metadata_store.prepare_read()?;
        let rows = match std::mem::replace(
            &mut self.merge_store,
            EntityMergeStore::Resident(AHashMap::with_hasher(RandomState::with_seeds(5, 6, 7, 8))),
        ) {
            EntityMergeStore::External(rows) => rows,
            EntityMergeStore::Resident(_) => unreachable!("resident mode returned above"),
        };
        let strings = match std::mem::replace(
            &mut self.strings,
            EntityStringStore::Resident(StringDictionary::new(1)?),
        ) {
            EntityStringStore::External(strings) => strings,
            EntityStringStore::Resident(_) => {
                return Err(DedupError::InvariantViolation {
                    context: ErrorContext::stage("entity"),
                    message: "external entity rows have no external string spool".to_owned(),
                });
            }
        };
        let dictionary = strings.finish(
            &rows.volumes,
            rows.sort_chunk_rows.saturating_mul(5).max(1),
            self.external_string_sort_bytes,
            rows.merge_fan_in,
            progress,
        )?;
        let root = rows
            .volumes
            .first()
            .map(|volume| volume.root.clone())
            .ok_or_else(|| DedupError::InvalidInput {
                context: ErrorContext::stage("entity"),
                message: "external entity reducer has no volume".to_owned(),
            })?;
        let mut sorted =
            prepare_external_sorted_rows(rows, &dictionary, &self.evm_chain_ids, progress)?;
        let mut reducer = ExternalArtifactReducer::new(&root, &mut self.metadata_store)?;
        if let Some(run) = sorted.run.take() {
            progress.begin_phase("entity_external_artifact_reduce", Some(run.records));
            let mut reader = BufReader::with_capacity(1024 * 1024, File::open(&run.path)?);
            let mut work = 0_u64;
            for _ in 0..run.records {
                reducer.push(read_sort_record(&mut reader)?)?;
                work += 1;
                if work == 4_096 {
                    progress.advance(work);
                    progress.check_cancelled("entity")?;
                    work = 0;
                }
            }
            progress.advance(work);
            progress.check_cancelled("entity")?;
            ensure_reader_eof(&mut reader, "external entity sort run has trailing bytes")?;
            sorted.handle_touches = checked_touches(sorted.handle_touches, run.records)?;
            std::fs::remove_file(run.path)?;
        }
        let reduced = reducer.finish()?;
        let files = EntityArtifactFiles {
            strings_offsets: dictionary.offsets.path().to_path_buf(),
            strings_blob: dictionary.blob.path().to_path_buf(),
            contracts: reduced.contracts.path().to_path_buf(),
            nfts: reduced.nfts.path().to_path_buf(),
            metadata_offsets: reduced.metadata_offsets.path().to_path_buf(),
            metadata_blob: reduced.metadata_blob.path().to_path_buf(),
        };
        write_entity_artifact_from_files(path, &files, logical_input_digest, configuration_digest)?;
        Ok(EntityBuildSummary {
            contract_count: reduced.contract_count,
            nft_count: reduced.nft_count,
            string_count: dictionary.string_count,
            metadata_spill_bytes: self.metadata_store.spill_bytes(),
            external_handle_spill_bytes: sorted.spill_bytes.saturating_add(dictionary.spill_bytes),
            external_handle_touches: sorted
                .handle_touches
                .saturating_add(dictionary.handle_touches),
            external_volumes_used: sorted.volumes_used,
            digest_bucket_max: u64::from(dictionary.string_count > 0),
        })
    }
}

#[derive(Debug)]
struct SortRun {
    path: PathBuf,
    records: u64,
}

struct ExternalSortedRows {
    run: Option<SortRun>,
    spill_bytes: u64,
    handle_touches: u64,
    volumes_used: u64,
}

fn finish_external_entities(
    rows: ExternalRowStore,
    dictionary: ExternalStringDictionary,
    evm_chain_ids: &BTreeMap<ChainId, ()>,
    metadata_store: &mut MetadataStore,
    progress: &dyn ProgressObserver,
) -> Result<EntityBuildResult, DedupError> {
    let mut sorted = prepare_external_sorted_rows(rows, &dictionary, evm_chain_ids, progress)?;
    let mut reducer = ExternalEntityReducer::new(metadata_store);
    if let Some(run) = sorted.run.take() {
        progress.begin_phase("entity_external_reduce", Some(run.records));
        let mut reader = BufReader::with_capacity(64 * 1024, File::open(&run.path)?);
        let mut work = 0_u64;
        for _ in 0..run.records {
            reducer.push(read_sort_record(&mut reader)?)?;
            work += 1;
            if work == 4_096 {
                progress.advance(work);
                progress.check_cancelled("entity")?;
                work = 0;
            }
        }
        progress.advance(work);
        progress.check_cancelled("entity")?;
        ensure_reader_eof(&mut reader, "external entity sort run has trailing bytes")?;
        sorted.handle_touches = checked_touches(sorted.handle_touches, run.records)?;
        std::fs::remove_file(run.path)?;
    }
    let (artifacts, metadata_by_nft) = reducer.finish()?;
    let strings = materialize_external_dictionary(&dictionary)?;
    Ok(EntityBuildResult {
        artifacts,
        strings,
        metadata_by_nft,
        metadata_spill_bytes: metadata_store.spill_bytes(),
        external_handle_spill_bytes: sorted.spill_bytes.saturating_add(dictionary.spill_bytes),
        external_handle_touches: sorted
            .handle_touches
            .saturating_add(dictionary.handle_touches),
        external_volumes_used: sorted.volumes_used,
    })
}

fn prepare_external_sorted_rows(
    mut rows: ExternalRowStore,
    dictionary: &ExternalStringDictionary,
    evm_chain_ids: &BTreeMap<ChainId, ()>,
    progress: &dyn ProgressObserver,
) -> Result<ExternalSortedRows, DedupError> {
    let input_spill_bytes = rows.input_spill_bytes();
    let mut occurrence_map = OccurrenceMapReader::new(dictionary)?;
    let mut reader = rows.prepare_reader()?;
    let mut runs = Vec::new();
    let mut remaining = rows.rows;
    let initial_run_count = usize::try_from(
        rows.rows
            .div_ceil(u64::try_from(rows.sort_chunk_rows).unwrap_or(u64::MAX)),
    )
    .map_err(|_| DedupError::CounterOverflow {
        counter: "entity_external_sort_runs",
    })?;
    let mut spill_bytes = input_spill_bytes;
    let mut handle_touches = rows.rows;
    let mut volumes_used = BTreeSet::new();
    progress.begin_phase("entity_external_initial_runs", Some(rows.rows));
    while remaining > 0 {
        let count =
            usize::try_from(remaining.min(u64::try_from(rows.sort_chunk_rows).unwrap_or(u64::MAX)))
                .map_err(|_| DedupError::CounterOverflow {
                    counter: "entity_external_sort_chunk",
                })?;
        let mut chunk = Vec::with_capacity(count);
        for _ in 0..count {
            chunk.push(sortable_external_row(
                read_external_row(&mut reader)?,
                &mut occurrence_map,
                evm_chain_ids,
            )?);
        }
        chunk.sort_unstable();
        let path = weighted_volume_root(&rows.volumes, runs.len(), initial_run_count)?
            .join(format!("entity-sort-run-0-{:05}.bin", runs.len()));
        if let Some(parent) = path.parent() {
            volumes_used.insert(parent.to_owned());
        }
        write_sort_run(&path, &chunk)?;
        let records = u64::try_from(chunk.len()).map_err(|_| DedupError::CounterOverflow {
            counter: "entity_external_sort_records",
        })?;
        spill_bytes = checked_spill_bytes(spill_bytes, records)?;
        handle_touches = checked_touches(handle_touches, records)?;
        progress.advance(records);
        progress.check_cancelled("entity")?;
        runs.push(SortRun { path, records });
        remaining =
            remaining
                .checked_sub(records)
                .ok_or_else(|| DedupError::InvariantViolation {
                    context: ErrorContext::stage("entity"),
                    message: "external entity row count underflow".to_owned(),
                })?;
    }
    ensure_reader_eof(&mut reader, "external entity rows have trailing bytes")?;
    occurrence_map.finish()?;
    let mut pass = 1_usize;
    while runs.len() > 1 {
        progress.begin_phase("entity_external_merge", Some(rows.rows));
        let mut merged_this_pass = 0_u64;
        let next_run_count = runs.len().div_ceil(rows.merge_fan_in);
        let mut next = Vec::with_capacity(next_run_count);
        for (group, chunk) in runs.chunks(rows.merge_fan_in).enumerate() {
            if chunk.len() == 1 {
                next.push(SortRun {
                    path: chunk[0].path.clone(),
                    records: chunk[0].records,
                });
                continue;
            }
            let output = weighted_volume_root(&rows.volumes, group, next_run_count)?
                .join(format!("entity-sort-run-{pass}-{group:05}.bin"));
            if let Some(parent) = output.parent() {
                volumes_used.insert(parent.to_owned());
            }
            let records = merge_sort_runs(chunk, &output)?;
            spill_bytes = checked_spill_bytes(spill_bytes, records)?;
            handle_touches = checked_touches(
                handle_touches,
                records.checked_mul(2).ok_or(DedupError::CounterOverflow {
                    counter: "entity_external_handle_touches",
                })?,
            )?;
            merged_this_pass =
                merged_this_pass
                    .checked_add(records)
                    .ok_or(DedupError::CounterOverflow {
                        counter: "entity_external_merge_progress",
                    })?;
            progress.advance(records);
            progress.check_cancelled("entity")?;
            for run in chunk {
                std::fs::remove_file(&run.path)?;
            }
            next.push(SortRun {
                path: output,
                records,
            });
        }
        runs = next;
        if merged_this_pass < rows.rows && runs.len() > 1 {
            progress.advance(rows.rows - merged_this_pass);
        }
        pass = pass.saturating_add(1);
    }
    Ok(ExternalSortedRows {
        run: runs.pop(),
        spill_bytes,
        handle_touches,
        volumes_used: u64::try_from(volumes_used.len()).map_err(|_| {
            DedupError::CounterOverflow {
                counter: "entity_external_volumes_used",
            }
        })?,
    })
}

fn is_decimal_token(value: &[u8]) -> bool {
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

fn sortable_external_row(
    row: ExternalRow,
    occurrence_map: &mut OccurrenceMapReader,
    evm_chain_ids: &BTreeMap<ChainId, ()>,
) -> Result<ExternalSortRecord, DedupError> {
    let chain = ChainId::new(
        u16::try_from(row.chain).map_err(|_| invalid_external_row("chain ID exceeds u16"))?,
    );
    let (address_ref, _) = occurrence_map.resolve(row.address_ref)?;
    let (token_id_ref, token_numeric_rank) = occurrence_map.resolve(row.token_id_ref)?;
    let name_ref = resolve_external_optional(occurrence_map, row.name_ref)?;
    let token_uri_ref = resolve_external_optional(occurrence_map, row.token_uri_ref)?;
    let image_uri_ref = resolve_external_optional(occurrence_map, row.image_uri_ref)?;
    let address_rank = address_ref.as_u64();
    let token_rank = if evm_chain_ids.contains_key(&chain) {
        token_numeric_rank.ok_or_else(|| DedupError::InvalidInput {
            context: ErrorContext::stage("entity"),
            message: "EVM token id is not a non-negative integer".to_owned(),
        })?
    } else {
        token_id_ref.as_u64()
    };
    Ok(ExternalSortRecord {
        chain: row.chain,
        address_rank,
        token_rank,
        source_file: row.source_file,
        source_row: row.source_row,
        address_ref: address_ref.as_u64(),
        token_id_ref: token_id_ref.as_u64(),
        name_ref: optional_id(name_ref),
        token_uri_ref: optional_id(token_uri_ref),
        image_uri_ref: optional_id(image_uri_ref),
        metadata_offset: row.metadata_offset,
        metadata_length: row.metadata_length,
    })
}

fn write_external_row(writer: &mut impl Write, row: ExternalRow) -> Result<(), DedupError> {
    for field in [
        row.chain,
        row.address_ref,
        row.token_id_ref,
        row.name_ref,
        row.token_uri_ref,
        row.image_uri_ref,
        row.source_file,
        row.source_row,
        row.metadata_offset,
        row.metadata_length,
    ] {
        writer.write_all(&field.to_le_bytes())?;
    }
    Ok(())
}

fn read_external_row(reader: &mut impl Read) -> Result<ExternalRow, DedupError> {
    let fields = read_u64_fields::<EXTERNAL_ROW_FIELDS>(reader)?;
    Ok(ExternalRow {
        chain: fields[0],
        address_ref: fields[1],
        token_id_ref: fields[2],
        name_ref: fields[3],
        token_uri_ref: fields[4],
        image_uri_ref: fields[5],
        source_file: fields[6],
        source_row: fields[7],
        metadata_offset: fields[8],
        metadata_length: fields[9],
    })
}

fn write_sort_run(path: &Path, records: &[ExternalSortRecord]) -> Result<(), DedupError> {
    let mut writer = BufWriter::with_capacity(64 * 1024, File::create(path)?);
    for record in records {
        write_sort_record(&mut writer, *record)?;
    }
    writer.flush()?;
    writer.get_ref().sync_all()?;
    Ok(())
}

fn write_sort_record(
    writer: &mut impl Write,
    record: ExternalSortRecord,
) -> Result<(), DedupError> {
    for field in [
        record.chain,
        record.address_rank,
        record.token_rank,
        record.source_file,
        record.source_row,
        record.address_ref,
        record.token_id_ref,
        record.name_ref,
        record.token_uri_ref,
        record.image_uri_ref,
        record.metadata_offset,
        record.metadata_length,
    ] {
        writer.write_all(&field.to_le_bytes())?;
    }
    Ok(())
}

fn read_sort_record(reader: &mut impl Read) -> Result<ExternalSortRecord, DedupError> {
    let fields = read_u64_fields::<EXTERNAL_SORT_FIELDS>(reader)?;
    Ok(ExternalSortRecord {
        chain: fields[0],
        address_rank: fields[1],
        token_rank: fields[2],
        source_file: fields[3],
        source_row: fields[4],
        address_ref: fields[5],
        token_id_ref: fields[6],
        name_ref: fields[7],
        token_uri_ref: fields[8],
        image_uri_ref: fields[9],
        metadata_offset: fields[10],
        metadata_length: fields[11],
    })
}

fn read_u64_fields<const N: usize>(reader: &mut impl Read) -> Result<[u64; N], DedupError> {
    let mut fields = [0_u64; N];
    for field in &mut fields {
        let mut bytes = [0_u8; 8];
        reader.read_exact(&mut bytes)?;
        *field = u64::from_le_bytes(bytes);
    }
    Ok(fields)
}

fn merge_sort_runs(runs: &[SortRun], output: &Path) -> Result<u64, DedupError> {
    let mut readers = Vec::with_capacity(runs.len());
    let mut remaining = Vec::with_capacity(runs.len());
    let mut heap = BinaryHeap::new();
    let mut total = 0_u64;
    for (index, run) in runs.iter().enumerate() {
        let mut reader = BufReader::with_capacity(64 * 1024, File::open(&run.path)?);
        let mut count = run.records;
        total = total
            .checked_add(count)
            .ok_or(DedupError::CounterOverflow {
                counter: "entity_external_merge_records",
            })?;
        if count > 0 {
            heap.push(Reverse((read_sort_record(&mut reader)?, index)));
            count -= 1;
        }
        readers.push(reader);
        remaining.push(count);
    }
    let mut writer = BufWriter::with_capacity(64 * 1024, File::create(output)?);
    while let Some(Reverse((record, index))) = heap.pop() {
        write_sort_record(&mut writer, record)?;
        if remaining[index] > 0 {
            heap.push(Reverse((read_sort_record(&mut readers[index])?, index)));
            remaining[index] -= 1;
        }
    }
    writer.flush()?;
    writer.get_ref().sync_all()?;
    Ok(total)
}

#[derive(Debug)]
struct ExternalNftAggregate {
    token_id_ref: StringId,
    name_ref: Option<StringId>,
    token_uri_ref: Option<StringId>,
    image_uri_ref: Option<StringId>,
    metadata: Option<StoredMetadata>,
}

#[derive(Debug)]
struct ExternalContractAggregate {
    chain_id: ChainId,
    address_ref: StringId,
    name_ref: Option<StringId>,
    first_nft_id: NftId,
    nft_count: u64,
}

struct ExternalArtifactFiles {
    contracts: NamedTempFile,
    nfts: NamedTempFile,
    metadata_offsets: NamedTempFile,
    metadata_blob: NamedTempFile,
    contract_count: u64,
    nft_count: u64,
}

struct ExternalArtifactReducer<'a> {
    metadata_store: &'a mut MetadataStore,
    contracts: NamedTempFile,
    nfts: NamedTempFile,
    metadata_offsets: NamedTempFile,
    metadata_blob: NamedTempFile,
    contract_writer: BufWriter<File>,
    nft_writer: BufWriter<File>,
    metadata_offset_writer: BufWriter<File>,
    metadata_blob_writer: BufWriter<File>,
    metadata_blob_position: u64,
    metadata_count: u64,
    contract_count: u64,
    nft_count: u64,
    contract: Option<ExternalContractAggregate>,
    nft: Option<ExternalNftAggregate>,
}

impl<'a> ExternalArtifactReducer<'a> {
    fn new(root: &Path, metadata_store: &'a mut MetadataStore) -> Result<Self, DedupError> {
        std::fs::create_dir_all(root)?;
        let contracts = NamedTempFile::new_in(root)?;
        let nfts = NamedTempFile::new_in(root)?;
        let metadata_offsets = NamedTempFile::new_in(root)?;
        let metadata_blob = NamedTempFile::new_in(root)?;
        let mut contract_writer = BufWriter::with_capacity(1024 * 1024, contracts.reopen()?);
        let mut nft_writer = BufWriter::with_capacity(1024 * 1024, nfts.reopen()?);
        let mut metadata_offset_writer =
            BufWriter::with_capacity(1024 * 1024, metadata_offsets.reopen()?);
        write_raw_u64(&mut contract_writer, 0)?;
        write_raw_u64(&mut nft_writer, 0)?;
        write_raw_u64(&mut metadata_offset_writer, 0)?;
        Ok(Self {
            metadata_store,
            contract_writer,
            nft_writer,
            metadata_offset_writer,
            metadata_blob_writer: BufWriter::with_capacity(1024 * 1024, metadata_blob.reopen()?),
            contracts,
            nfts,
            metadata_offsets,
            metadata_blob,
            metadata_blob_position: 0,
            metadata_count: 0,
            contract_count: 0,
            nft_count: 0,
            contract: None,
            nft: None,
        })
    }

    fn push(&mut self, row: ExternalSortRecord) -> Result<(), DedupError> {
        let chain_id = ChainId::new(
            u16::try_from(row.chain).map_err(|_| invalid_external_row("chain ID exceeds u16"))?,
        );
        let address_ref = decode_string_id(row.address_ref, "address StringId")?;
        let token_id_ref = decode_string_id(row.token_id_ref, "token StringId")?;
        if self.contract.as_ref().is_some_and(|contract| {
            (contract.chain_id, contract.address_ref) != (chain_id, address_ref)
        }) {
            self.finish_contract()?;
        }
        if self.contract.is_none() {
            self.contract = Some(ExternalContractAggregate {
                chain_id,
                address_ref,
                name_ref: None,
                first_nft_id: checked_u64_id::<NftId>(self.nft_count, NftId::new)?,
                nft_count: 0,
            });
        }
        if self
            .nft
            .as_ref()
            .is_some_and(|nft| nft.token_id_ref != token_id_ref)
        {
            self.finish_nft()?;
        }
        if self.nft.is_none() {
            self.nft = Some(ExternalNftAggregate {
                token_id_ref,
                name_ref: None,
                token_uri_ref: None,
                image_uri_ref: None,
                metadata: None,
            });
        }
        let name_ref = decode_optional_string_id(row.name_ref, "name StringId")?;
        let token_uri_ref = decode_optional_string_id(row.token_uri_ref, "token URI StringId")?;
        let image_uri_ref = decode_optional_string_id(row.image_uri_ref, "image URI StringId")?;
        let contract = self
            .contract
            .as_mut()
            .ok_or_else(|| invalid_external_row("contract reducer is missing"))?;
        contract.name_ref = merge_optional(
            contract.name_ref,
            name_ref,
            "multiple distinct non-empty names in one contract",
        )?;
        let nft = self
            .nft
            .as_mut()
            .ok_or_else(|| invalid_external_row("NFT reducer is missing"))?;
        nft.name_ref = merge_optional(nft.name_ref, name_ref, "conflicting NFT name")?;
        nft.token_uri_ref =
            merge_optional(nft.token_uri_ref, token_uri_ref, "conflicting token URI")?;
        nft.image_uri_ref =
            merge_optional(nft.image_uri_ref, image_uri_ref, "conflicting image URI")?;
        if nft.metadata.is_none() && row.metadata_offset != NONE_ID {
            nft.metadata = Some(StoredMetadata::Spilled {
                offset: row.metadata_offset,
                length: row.metadata_length,
            });
        }
        Ok(())
    }

    fn finish_nft(&mut self) -> Result<(), DedupError> {
        let Some(nft) = self.nft.take() else {
            return Ok(());
        };
        let contract_id = checked_u64_id::<ContractId>(self.contract_count, ContractId::new)?;
        let nft_id = checked_u64_id::<NftId>(self.nft_count, NftId::new)?;
        let has_metadata = if let Some(metadata) = nft.metadata {
            let content = self.metadata_store.read(metadata)?;
            let length = u64::try_from(content.len()).map_err(|_| DedupError::CounterOverflow {
                counter: "entity_metadata_length",
            })?;
            write_raw_u64(&mut self.metadata_offset_writer, nft_id.as_u64())?;
            write_raw_u64(
                &mut self.metadata_offset_writer,
                self.metadata_blob_position,
            )?;
            write_raw_u64(&mut self.metadata_offset_writer, length)?;
            self.metadata_blob_writer.write_all(content.as_bytes())?;
            self.metadata_blob_position = self.metadata_blob_position.checked_add(length).ok_or(
                DedupError::CounterOverflow {
                    counter: "entity_metadata_blob_bytes",
                },
            )?;
            self.metadata_count =
                self.metadata_count
                    .checked_add(1)
                    .ok_or(DedupError::CounterOverflow {
                        counter: "entity_metadata_count",
                    })?;
            true
        } else {
            false
        };
        write_nft_record(
            &mut self.nft_writer,
            &Nft {
                id: nft_id,
                contract_id,
                token_id_ref: nft.token_id_ref,
                token_uri_ref: nft.token_uri_ref,
                image_uri_ref: nft.image_uri_ref,
                has_metadata,
            },
        )?;
        self.nft_count = self
            .nft_count
            .checked_add(1)
            .ok_or(DedupError::CounterOverflow {
                counter: "entity_nft_count",
            })?;
        let contract = self
            .contract
            .as_mut()
            .ok_or_else(|| invalid_external_row("NFT has no contract reducer"))?;
        contract.nft_count =
            contract
                .nft_count
                .checked_add(1)
                .ok_or(DedupError::CounterOverflow {
                    counter: "entity_contract_nft_count",
                })?;
        Ok(())
    }

    fn finish_contract(&mut self) -> Result<(), DedupError> {
        self.finish_nft()?;
        let Some(contract) = self.contract.take() else {
            return Ok(());
        };
        let id = checked_u64_id::<ContractId>(self.contract_count, ContractId::new)?;
        write_contract_record(
            &mut self.contract_writer,
            &Contract {
                id,
                chain_id: contract.chain_id,
                address_ref: contract.address_ref,
                name_ref: contract.name_ref,
                first_nft_id: contract.first_nft_id,
                nft_count: contract.nft_count,
            },
        )?;
        self.contract_count =
            self.contract_count
                .checked_add(1)
                .ok_or(DedupError::CounterOverflow {
                    counter: "entity_contract_count",
                })?;
        Ok(())
    }

    fn finish(mut self) -> Result<ExternalArtifactFiles, DedupError> {
        self.finish_contract()?;
        self.contract_writer.flush()?;
        self.nft_writer.flush()?;
        self.metadata_offset_writer.flush()?;
        self.metadata_blob_writer.flush()?;
        overwrite_count(&mut self.contracts, self.contract_count)?;
        overwrite_count(&mut self.nfts, self.nft_count)?;
        overwrite_count(&mut self.metadata_offsets, self.metadata_count)?;
        Ok(ExternalArtifactFiles {
            contracts: self.contracts,
            nfts: self.nfts,
            metadata_offsets: self.metadata_offsets,
            metadata_blob: self.metadata_blob,
            contract_count: self.contract_count,
            nft_count: self.nft_count,
        })
    }
}

struct ExternalEntityReducer<'a> {
    metadata_store: &'a mut MetadataStore,
    artifacts: EntityArtifacts,
    metadata_by_nft: BTreeMap<NftId, String>,
    contract: Option<ExternalContractAggregate>,
    nft: Option<ExternalNftAggregate>,
}

impl<'a> ExternalEntityReducer<'a> {
    fn new(metadata_store: &'a mut MetadataStore) -> Self {
        Self {
            metadata_store,
            artifacts: EntityArtifacts::default(),
            metadata_by_nft: BTreeMap::new(),
            contract: None,
            nft: None,
        }
    }

    fn push(&mut self, row: ExternalSortRecord) -> Result<(), DedupError> {
        let chain_id = ChainId::new(
            u16::try_from(row.chain).map_err(|_| invalid_external_row("chain ID exceeds u16"))?,
        );
        let address_ref = decode_string_id(row.address_ref, "address StringId")?;
        let token_id_ref = decode_string_id(row.token_id_ref, "token StringId")?;
        let contract_changed = self.contract.as_ref().is_some_and(|contract| {
            (contract.chain_id, contract.address_ref) != (chain_id, address_ref)
        });
        if contract_changed {
            self.finish_contract()?;
        }
        if self.contract.is_none() {
            self.contract = Some(ExternalContractAggregate {
                chain_id,
                address_ref,
                name_ref: None,
                first_nft_id: checked_id::<NftId>(self.artifacts.nfts.len(), NftId::new)?,
                nft_count: 0,
            });
        }
        let nft_changed = self
            .nft
            .as_ref()
            .is_some_and(|nft| nft.token_id_ref != token_id_ref);
        if nft_changed {
            self.finish_nft()?;
        }
        if self.nft.is_none() {
            self.nft = Some(ExternalNftAggregate {
                token_id_ref,
                name_ref: None,
                token_uri_ref: None,
                image_uri_ref: None,
                metadata: None,
            });
        }
        let name_ref = decode_optional_string_id(row.name_ref, "name StringId")?;
        let token_uri_ref = decode_optional_string_id(row.token_uri_ref, "token URI StringId")?;
        let image_uri_ref = decode_optional_string_id(row.image_uri_ref, "image URI StringId")?;
        let contract = self
            .contract
            .as_mut()
            .ok_or_else(|| invalid_external_row("contract reducer is missing"))?;
        contract.name_ref = merge_optional(
            contract.name_ref,
            name_ref,
            "multiple distinct non-empty names in one contract",
        )?;
        let nft = self
            .nft
            .as_mut()
            .ok_or_else(|| invalid_external_row("NFT reducer is missing"))?;
        nft.name_ref = merge_optional(nft.name_ref, name_ref, "conflicting NFT name")?;
        nft.token_uri_ref =
            merge_optional(nft.token_uri_ref, token_uri_ref, "conflicting token URI")?;
        nft.image_uri_ref =
            merge_optional(nft.image_uri_ref, image_uri_ref, "conflicting image URI")?;
        if nft.metadata.is_none() && row.metadata_offset != NONE_ID {
            nft.metadata = Some(StoredMetadata::Spilled {
                offset: row.metadata_offset,
                length: row.metadata_length,
            });
        }
        Ok(())
    }

    fn finish_nft(&mut self) -> Result<(), DedupError> {
        let Some(nft) = self.nft.take() else {
            return Ok(());
        };
        let contract_id =
            checked_id::<ContractId>(self.artifacts.contracts.len(), ContractId::new)?;
        let nft_id = checked_id::<NftId>(self.artifacts.nfts.len(), NftId::new)?;
        let has_metadata = if let Some(metadata) = nft.metadata {
            self.metadata_by_nft
                .insert(nft_id, self.metadata_store.read(metadata)?);
            true
        } else {
            false
        };
        self.artifacts.nfts.push(Nft {
            id: nft_id,
            contract_id,
            token_id_ref: nft.token_id_ref,
            token_uri_ref: nft.token_uri_ref,
            image_uri_ref: nft.image_uri_ref,
            has_metadata,
        });
        let contract = self
            .contract
            .as_mut()
            .ok_or_else(|| invalid_external_row("NFT has no contract reducer"))?;
        contract.nft_count =
            contract
                .nft_count
                .checked_add(1)
                .ok_or(DedupError::CounterOverflow {
                    counter: "entity_contract_nft_count",
                })?;
        Ok(())
    }

    fn finish_contract(&mut self) -> Result<(), DedupError> {
        self.finish_nft()?;
        let Some(contract) = self.contract.take() else {
            return Ok(());
        };
        let id = checked_id::<ContractId>(self.artifacts.contracts.len(), ContractId::new)?;
        self.artifacts.contracts.push(Contract {
            id,
            chain_id: contract.chain_id,
            address_ref: contract.address_ref,
            name_ref: contract.name_ref,
            first_nft_id: contract.first_nft_id,
            nft_count: contract.nft_count,
        });
        Ok(())
    }

    fn finish(mut self) -> Result<(EntityArtifacts, BTreeMap<NftId, String>), DedupError> {
        self.finish_contract()?;
        Ok((self.artifacts, self.metadata_by_nft))
    }
}

fn optional_id(value: Option<StringId>) -> u64 {
    value.map_or(NONE_ID, StringId::as_u64)
}

fn resident_optional_string(
    strings: &mut StringDictionary,
    value: &str,
) -> Result<Option<StringId>, DedupError> {
    if value.is_empty() {
        Ok(None)
    } else {
        strings.intern(value.as_bytes()).map(Some)
    }
}

fn external_optional_string(
    strings: &mut ExternalStringStore,
    value: &str,
) -> Result<u64, DedupError> {
    if value.is_empty() {
        Ok(NONE_ID)
    } else {
        strings.store(value.as_bytes())
    }
}

fn resolve_external_optional(
    occurrences: &mut OccurrenceMapReader,
    value: u64,
) -> Result<Option<StringId>, DedupError> {
    if value == NONE_ID {
        Ok(None)
    } else {
        occurrences.resolve(value).map(|(id, _)| Some(id))
    }
}

fn materialize_external_dictionary(
    dictionary: &ExternalStringDictionary,
) -> Result<StringDictionary, DedupError> {
    let mut offsets = BufReader::with_capacity(1024 * 1024, dictionary.offsets.reopen()?);
    let mut blob = BufReader::with_capacity(1024 * 1024, dictionary.blob.reopen()?);
    let count = read_u64_fields::<1>(&mut offsets)?[0];
    if count != dictionary.string_count {
        return Err(invalid_external_row(
            "external string dictionary count mismatch",
        ));
    }
    let mut values = Vec::with_capacity(usize::try_from(count).map_err(|_| {
        DedupError::ResourceBudgetExceeded {
            context: ErrorContext::stage("entity"),
            requested: count,
        }
    })?);
    let mut expected_offset = 0_u64;
    for _ in 0..count {
        let [offset, length] = read_u64_fields::<2>(&mut offsets)?;
        if offset != expected_offset {
            return Err(invalid_external_row(
                "external string offsets are not contiguous",
            ));
        }
        let length = usize::try_from(length).map_err(|_| DedupError::ResourceBudgetExceeded {
            context: ErrorContext::stage("entity"),
            requested: length,
        })?;
        let mut value = vec![0_u8; length];
        blob.read_exact(&mut value)?;
        expected_offset = expected_offset
            .checked_add(u64::try_from(length).unwrap_or(u64::MAX))
            .ok_or(DedupError::CounterOverflow {
                counter: "external_string_blob_bytes",
            })?;
        values.push(value);
    }
    ensure_reader_eof(&mut offsets, "external string offsets have trailing bytes")?;
    ensure_reader_eof(&mut blob, "external string blob has trailing bytes")?;
    StringDictionary::from_ordered_values(values, 64)
}

fn remap_strings_lexically(
    mut artifacts: EntityArtifacts,
    strings: StringDictionary,
) -> Result<(EntityArtifacts, StringDictionary), DedupError> {
    let mut ordered = strings
        .values()
        .enumerate()
        .map(|(old, value)| (old, value.to_vec()))
        .collect::<Vec<_>>();
    ordered.sort_unstable_by(|left, right| left.1.cmp(&right.1));
    let mut remap = vec![StringId::new(EntityId::MIN); ordered.len()];
    let mut values = Vec::with_capacity(ordered.len());
    for (new, (old, value)) in ordered.into_iter().enumerate() {
        let new = StringId::new(
            EntityId::try_from(new).map_err(|_| DedupError::InvalidInput {
                context: ErrorContext::stage("entity"),
                message: "StringId capacity exceeded; rebuild with wide_ids".to_owned(),
            })?,
        );
        remap[old] = new;
        values.push(value);
    }
    let map = |id: StringId| -> Result<StringId, DedupError> {
        remap
            .get(
                usize::try_from(id.as_u64())
                    .map_err(|_| invalid_external_row("resident StringId does not fit usize"))?,
            )
            .copied()
            .ok_or_else(|| invalid_external_row("resident StringId is missing"))
    };
    for contract in &mut artifacts.contracts {
        contract.address_ref = map(contract.address_ref)?;
        contract.name_ref = contract.name_ref.map(&map).transpose()?;
    }
    for nft in &mut artifacts.nfts {
        nft.token_id_ref = map(nft.token_id_ref)?;
        nft.token_uri_ref = nft.token_uri_ref.map(&map).transpose()?;
        nft.image_uri_ref = nft.image_uri_ref.map(&map).transpose()?;
    }
    Ok((
        artifacts,
        StringDictionary::from_ordered_values(values, 64)?,
    ))
}

fn decode_optional_string_id(
    value: u64,
    field: &'static str,
) -> Result<Option<StringId>, DedupError> {
    if value == NONE_ID {
        Ok(None)
    } else {
        decode_string_id(value, field).map(Some)
    }
}

fn decode_string_id(value: u64, field: &'static str) -> Result<StringId, DedupError> {
    EntityId::try_from(value)
        .map(StringId::new)
        .map_err(|_| invalid_external_row(&format!("{field} exceeds EntityId")))
}

fn weighted_volume_root(
    volumes: &[SpillVolume],
    item: usize,
    item_count: usize,
) -> Result<&Path, DedupError> {
    if volumes.is_empty() || item_count == 0 {
        return Err(DedupError::InvalidInput {
            context: ErrorContext::stage("entity"),
            message: "external entity volume assignment has no work or volume".to_owned(),
        });
    }
    let total = volumes.iter().try_fold(0_u128, |sum, volume| {
        sum.checked_add(u128::from(volume.weight))
            .ok_or(DedupError::CounterOverflow {
                counter: "entity_external_volume_weight",
            })
    })?;
    let midpoint = u128::try_from(item)
        .unwrap_or(u128::MAX)
        .saturating_mul(2)
        .saturating_add(1)
        .saturating_mul(total)
        / u128::try_from(item_count)
            .unwrap_or(u128::MAX)
            .saturating_mul(2);
    let mut cumulative = 0_u128;
    for volume in volumes {
        cumulative = cumulative.saturating_add(u128::from(volume.weight));
        if midpoint < cumulative {
            return Ok(&volume.root);
        }
    }
    Ok(&volumes[volumes.len() - 1].root)
}

fn checked_spill_bytes(current: u64, records: u64) -> Result<u64, DedupError> {
    current
        .checked_add(
            records
                .checked_mul((EXTERNAL_SORT_FIELDS as u64).saturating_mul(8))
                .ok_or(DedupError::CounterOverflow {
                    counter: "entity_external_spill_bytes",
                })?,
        )
        .ok_or(DedupError::CounterOverflow {
            counter: "entity_external_spill_bytes",
        })
}

fn checked_touches(current: u64, amount: u64) -> Result<u64, DedupError> {
    current
        .checked_add(amount)
        .ok_or(DedupError::CounterOverflow {
            counter: "entity_external_handle_touches",
        })
}

fn ensure_reader_eof(reader: &mut impl Read, message: &str) -> Result<(), DedupError> {
    let mut byte = [0_u8; 1];
    if reader.read(&mut byte)? == 0 {
        Ok(())
    } else {
        Err(invalid_external_row(message))
    }
}

fn invalid_external_row(message: &str) -> DedupError {
    DedupError::ArtifactMismatch {
        context: ErrorContext::stage("entity"),
        message: message.to_owned(),
    }
}

fn merge_optional<T: Copy + Eq>(
    current: Option<T>,
    incoming: Option<T>,
    message: &'static str,
) -> Result<Option<T>, DedupError> {
    match (current, incoming) {
        (Some(a), Some(b)) if a != b => Err(DedupError::SnapshotConflict {
            context: ErrorContext::stage("entity"),
            message: message.to_owned(),
        }),
        (Some(value), _) | (_, Some(value)) => Ok(Some(value)),
        (None, None) => Ok(None),
    }
}

fn checked_id<T>(length: usize, constructor: impl FnOnce(EntityId) -> T) -> Result<T, DedupError> {
    let raw = EntityId::try_from(length).map_err(|_| DedupError::InvalidInput {
        context: ErrorContext::stage("entity"),
        message: "entity ID capacity exceeded; rebuild with wide_ids".to_owned(),
    })?;
    Ok(constructor(raw))
}

fn checked_u64_id<T>(value: u64, constructor: impl FnOnce(EntityId) -> T) -> Result<T, DedupError> {
    EntityId::try_from(value)
        .map(constructor)
        .map_err(|_| DedupError::InvalidInput {
            context: ErrorContext::stage("entity"),
            message: "entity count exceeds configured EntityId; rebuild with wide_ids".to_owned(),
        })
}

fn write_raw_u64(writer: &mut impl Write, value: u64) -> Result<(), DedupError> {
    writer.write_all(&value.to_le_bytes())?;
    Ok(())
}

fn write_contract_record(writer: &mut impl Write, contract: &Contract) -> Result<(), DedupError> {
    write_raw_u64(writer, contract.id.as_u64())?;
    writer.write_all(&contract.chain_id.get().to_le_bytes())?;
    write_raw_u64(writer, contract.address_ref.as_u64())?;
    write_raw_u64(writer, contract.name_ref.map_or(NONE_ID, StringId::as_u64))?;
    write_raw_u64(writer, contract.first_nft_id.as_u64())?;
    write_raw_u64(writer, contract.nft_count)
}

fn write_nft_record(writer: &mut impl Write, nft: &Nft) -> Result<(), DedupError> {
    write_raw_u64(writer, nft.id.as_u64())?;
    write_raw_u64(writer, nft.contract_id.as_u64())?;
    write_raw_u64(writer, nft.token_id_ref.as_u64())?;
    write_raw_u64(writer, nft.token_uri_ref.map_or(NONE_ID, StringId::as_u64))?;
    write_raw_u64(writer, nft.image_uri_ref.map_or(NONE_ID, StringId::as_u64))?;
    writer.write_all(&[u8::from(nft.has_metadata)])?;
    Ok(())
}

fn overwrite_count(file: &mut NamedTempFile, count: u64) -> Result<(), DedupError> {
    file.as_file_mut().seek(std::io::SeekFrom::Start(0))?;
    file.as_file_mut().write_all(&count.to_le_bytes())?;
    file.as_file_mut().sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy)]
    struct TestMetadataValidator;

    impl MetadataSourceValidator for TestMetadataValidator {
        fn is_valid_metadata(&self, content: &str) -> bool {
            serde_json::from_str::<serde_json::Value>(content).is_ok()
        }
    }

    fn row(name: &str, token_uri: &str, order: SourceOrder) -> InputRow {
        InputRow {
            chain: " Ethereum ".to_owned(),
            contract_address: " 0xABC ".to_owned(),
            token_id: " 1 ".to_owned(),
            name_norm: name.to_owned(),
            token_uri_norm: token_uri.to_owned(),
            image_uri_norm: String::new(),
            metadata_json: "{\"name\":\"one\"}".to_owned(),
            source_order: order,
        }
    }

    #[test]
    fn duplicate_rows_merge_empty_with_non_empty() {
        let mut builder = EntityBuilder::new(
            ["ethereum".to_owned()],
            ["ethereum".to_owned()],
            8,
            TestMetadataValidator,
        )
        .unwrap();
        builder.push(row("", "", SourceOrder::new(0, 1))).unwrap();
        builder
            .push(row("collection", "ipfs://x", SourceOrder::new(0, 2)))
            .unwrap();
        let result = builder.finish().unwrap();
        assert_eq!(result.artifacts.contracts.len(), 1);
        assert_eq!(result.artifacts.nfts.len(), 1);
        assert!(result.artifacts.contracts[0].name_ref.is_some());
        assert!(result.artifacts.nfts[0].token_uri_ref.is_some());
    }

    #[test]
    fn conflicting_non_empty_values_fail() {
        let mut builder = EntityBuilder::new(
            ["ethereum".to_owned()],
            ["ethereum".to_owned()],
            8,
            TestMetadataValidator,
        )
        .unwrap();
        builder
            .push(row("collection", "ipfs://a", SourceOrder::new(0, 1)))
            .unwrap();
        assert!(matches!(
            builder.push(row("collection", "ipfs://b", SourceOrder::new(0, 2))),
            Err(DedupError::SnapshotConflict { .. })
        ));
    }

    #[test]
    fn first_valid_metadata_wins_by_source_order() {
        for mode in [ExecutionMode::InMemory, ExecutionMode::External] {
            let directory = tempfile::tempdir().unwrap();
            let mut builder = EntityBuilder::new_with_execution(
                ["ethereum".to_owned()],
                ["ethereum".to_owned()],
                8,
                TestMetadataValidator,
                EntityExecutionConfig::new(mode, Some(directory.path().to_owned()), 1, 2).unwrap(),
            )
            .unwrap();
            let mut invalid = row("", "", SourceOrder::new(0, 0));
            invalid.metadata_json = "not-json".to_owned();
            let mut first_valid = row("", "", SourceOrder::new(0, 1));
            first_valid.metadata_json = r#"{"source":"first"}"#.to_owned();
            let mut later_valid = row("", "", SourceOrder::new(0, 2));
            later_valid.metadata_json = r#"{"source":"later"}"#.to_owned();
            builder.push(later_valid).unwrap();
            builder.push(invalid).unwrap();
            builder.push(first_valid).unwrap();

            let result = builder.finish().unwrap();
            assert_eq!(
                result
                    .metadata_by_nft
                    .get(&NftId::new(0))
                    .map(String::as_str),
                Some(r#"{"source":"first"}"#)
            );
        }
    }

    #[test]
    fn different_non_empty_names_across_nfts_conflict_at_contract_level() {
        let mut builder = EntityBuilder::new(
            ["ethereum".to_owned()],
            ["ethereum".to_owned()],
            8,
            TestMetadataValidator,
        )
        .unwrap();
        let mut first = row("alpha", "", SourceOrder::new(0, 0));
        first.token_id = "1".to_owned();
        let mut second = row("beta", "", SourceOrder::new(0, 1));
        second.token_id = "2".to_owned();
        builder.push(first).unwrap();
        assert!(matches!(
            builder.push(second),
            Err(DedupError::SnapshotConflict { .. })
        ));
    }

    #[test]
    fn evm_nfts_are_ordered_by_arbitrary_precision_token_id() {
        let mut builder = EntityBuilder::new(
            ["ethereum".to_owned()],
            ["ethereum".to_owned()],
            8,
            TestMetadataValidator,
        )
        .unwrap();
        for (index, token_id) in ["10", "2", "999999999999999999999999999999999999", "1"]
            .into_iter()
            .enumerate()
        {
            let mut input = row("", "", SourceOrder::new(0, index as u64));
            input.token_id = token_id.to_owned();
            builder.push(input).unwrap();
        }
        let result = builder.finish().unwrap();
        let token_ids: Vec<_> = result
            .artifacts
            .nfts
            .iter()
            .map(|nft| {
                std::str::from_utf8(result.strings.resolve(nft.token_id_ref).unwrap())
                    .unwrap()
                    .to_owned()
            })
            .collect();
        assert_eq!(
            token_ids,
            ["1", "2", "10", "999999999999999999999999999999999999"]
        );
    }

    #[test]
    fn invalid_evm_token_id_is_rejected() {
        let mut builder = EntityBuilder::new(
            ["ethereum".to_owned()],
            ["ethereum".to_owned()],
            8,
            TestMetadataValidator,
        )
        .unwrap();
        let mut input = row("", "", SourceOrder::new(0, 0));
        input.token_id = "not-a-number".to_owned();
        builder.push(input).unwrap();
        assert!(matches!(
            builder.finish(),
            Err(DedupError::InvalidInput { .. })
        ));
    }

    #[test]
    fn entity_memory_modes_are_logically_identical() {
        let directory = tempfile::tempdir().unwrap();
        let run = |mode| {
            let mut builder = EntityBuilder::new_with_execution(
                ["ethereum".to_owned()],
                ["ethereum".to_owned()],
                8,
                TestMetadataValidator,
                EntityExecutionConfig::new(mode, Some(directory.path().to_owned()), 1, 2).unwrap(),
            )
            .unwrap();
            for (index, token_id) in ["10", "2", "01", "1"].into_iter().enumerate() {
                let mut input = row(
                    "collection",
                    "ipfs://same",
                    SourceOrder::new(0, index as u64),
                );
                input.token_id = token_id.to_owned();
                builder.push(input).unwrap();
            }
            let result = builder.finish().unwrap();
            let spill_bytes = result.metadata_spill_bytes;
            let handle_bytes = result.external_handle_spill_bytes;
            let handle_touches = result.external_handle_touches;
            (
                result.into_persisted(),
                spill_bytes,
                handle_bytes,
                handle_touches,
            )
        };
        let resident = run(ExecutionMode::InMemory);
        let hybrid = run(ExecutionMode::Hybrid);
        let external = run(ExecutionMode::External);
        assert_eq!(resident.0, hybrid.0);
        assert_eq!(resident.0, external.0);
        assert_eq!(resident.1, 0);
        assert!(hybrid.1 > 0);
        assert!(external.1 > 0);
        assert_eq!(resident.2, 0);
        assert!(hybrid.2 > 0);
        assert!(hybrid.3 > 0);
        assert!(external.2 > 0);
        assert!(external.3 > 0);
    }

    #[test]
    fn external_modes_write_logically_identical_artifacts_without_materializing_results() {
        let build_rows = || {
            let mut rows = ["10", "2", "01", "1"]
                .into_iter()
                .enumerate()
                .map(|(index, token_id)| {
                    let mut input = row(
                        "collection",
                        "ipfs://same",
                        SourceOrder::new(0, index as u64),
                    );
                    input.token_id = token_id.to_owned();
                    input
                })
                .collect::<Vec<_>>();
            let mut second_contract = row("collection", "ipfs://same", SourceOrder::new(0, 4));
            second_contract.contract_address = "0xDEF".to_owned();
            second_contract.token_id = "9".to_owned();
            rows.push(second_contract);
            rows
        };
        let mut resident = EntityBuilder::new(
            ["ethereum".to_owned()],
            ["ethereum".to_owned()],
            8,
            TestMetadataValidator,
        )
        .unwrap();
        for input in build_rows() {
            resident.push(input).unwrap();
        }
        let expected = resident.finish().unwrap().into_persisted();

        for mode in [ExecutionMode::Hybrid, ExecutionMode::External] {
            let directory = tempfile::tempdir().unwrap();
            let spill = directory.path().join("spill");
            let artifact = directory.path().join("entities");
            let execution = EntityExecutionConfig::new(mode, Some(spill), 1, 2)
                .unwrap()
                .with_string_sort_bytes(128)
                .unwrap();
            let mut builder = EntityBuilder::new_with_execution(
                ["ethereum".to_owned()],
                ["ethereum".to_owned()],
                8,
                TestMetadataValidator,
                execution,
            )
            .unwrap();
            for input in build_rows() {
                builder.push(input).unwrap();
            }
            let summary = builder
                .finish_to_artifact(
                    &artifact,
                    "input".to_owned(),
                    "config".to_owned(),
                    &NoopProgress,
                )
                .unwrap();
            assert_eq!(
                dedup_storage::read_entity_artifact(&artifact).unwrap(),
                expected
            );
            assert_eq!(summary.contract_count, 2);
            assert_eq!(summary.nft_count, 5);
            assert_eq!(
                summary.string_count,
                u64::try_from(expected.strings.len()).unwrap()
            );
            assert!(summary.external_handle_spill_bytes > 0);
            assert!(summary.external_handle_touches > 0);
        }
    }

    #[test]
    fn external_conflicts_are_detected_by_the_streaming_reducer() {
        let directory = tempfile::tempdir().unwrap();
        let mut builder = EntityBuilder::new_with_execution(
            ["ethereum".to_owned()],
            ["ethereum".to_owned()],
            8,
            TestMetadataValidator,
            EntityExecutionConfig::new(
                ExecutionMode::External,
                Some(directory.path().to_owned()),
                1,
                2,
            )
            .unwrap(),
        )
        .unwrap();
        builder
            .push(row("collection", "ipfs://a", SourceOrder::new(0, 1)))
            .unwrap();
        builder
            .push(row("collection", "ipfs://b", SourceOrder::new(0, 2)))
            .unwrap();
        assert!(matches!(
            builder.finish(),
            Err(DedupError::SnapshotConflict { .. })
        ));
    }

    #[test]
    fn decimal_token_comparator_is_allocation_free_and_total() {
        let mut values = vec![
            b"10".as_slice(),
            b"2".as_slice(),
            b"1".as_slice(),
            b"01".as_slice(),
            b"00".as_slice(),
            b"0".as_slice(),
        ];
        values.sort_unstable_by(|left, right| compare_decimal_tokens(left, right));
        assert_eq!(
            values,
            [
                b"0".as_slice(),
                b"00".as_slice(),
                b"01".as_slice(),
                b"1".as_slice(),
                b"2".as_slice(),
                b"10".as_slice(),
            ]
        );
        assert!(compare_decimal_tokens(b"9", b"10").is_lt());
    }
}
