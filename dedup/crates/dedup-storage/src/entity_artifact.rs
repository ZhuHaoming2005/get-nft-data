use crate::{
    AccessPattern, ArtifactManifest, ArtifactWriter, MemoryBudget, ReadOnlySegment,
    validate_artifact,
};
use dedup_model::{
    ChainId, Contract, ContractId, DedupError, EntityArtifacts, ErrorContext, Nft, NftId,
    PersistedEntityArtifacts, StringId,
};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

const ENTITY_SCHEMA_VERSION: u32 = 1;
const NONE_ID: u64 = u64::MAX;
const STRING_OFFSET_RECORD_BYTES: u64 = 16;
const METADATA_OFFSET_RECORD_BYTES: u64 = 24;
const CONTRACT_RECORD_BYTES: u64 = 42;
const NFT_RECORD_BYTES: u64 = 41;

#[derive(Clone, Debug)]
pub struct EntityArtifactFiles {
    pub strings_offsets: PathBuf,
    pub strings_blob: PathBuf,
    pub contracts: PathBuf,
    pub nfts: PathBuf,
    pub metadata_offsets: PathBuf,
    pub metadata_blob: PathBuf,
}

pub struct MappedContracts {
    records: ReadOnlySegment,
    count: u64,
}

impl MappedContracts {
    pub fn open(
        artifact_path: impl AsRef<Path>,
        budget: &MemoryBudget,
        residency_bytes: u64,
    ) -> Result<Self, DedupError> {
        let artifact_path = artifact_path.as_ref();
        validate_entity_manifest(artifact_path)?;
        let mut records = ReadOnlySegment::open_with_residency(
            artifact_path.join("contracts.bin"),
            budget,
            residency_bytes.max(1),
        )?;
        let count = read_mapped_u64(&records, 0, "contract_count")?;
        validate_fixed_record_length(records.len(), count, CONTRACT_RECORD_BYTES, "contract_mmap")?;
        records.advise(AccessPattern::Random)?;
        Ok(Self { records, count })
    }

    #[must_use]
    pub fn len(&self) -> u64 {
        self.count
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn get(&self, index: u64) -> Result<Contract, DedupError> {
        let record =
            fixed_record_offset(index, self.count, CONTRACT_RECORD_BYTES, "contract_mmap")?;
        let id = ContractId::new(read_entity_id_value(read_mapped_u64(
            &self.records,
            record,
            "contract_id",
        )?)?);
        let chain_offset = record.checked_add(8).ok_or(DedupError::CounterOverflow {
            counter: "contract_record_offset",
        })?;
        let chain_end = chain_offset
            .checked_add(2)
            .ok_or(DedupError::CounterOverflow {
                counter: "contract_record_offset",
            })?;
        let chain_bytes: [u8; 2] = self
            .records
            .bytes(chain_offset..chain_end)?
            .try_into()
            .map_err(|_| fixed_record_error("contract_mmap", "invalid chain ID width"))?;
        let address_ref = StringId::new(read_entity_id_value(read_mapped_u64(
            &self.records,
            record + 10,
            "contract_address_ref",
        )?)?);
        let name_raw = read_mapped_u64(&self.records, record + 18, "contract_name_ref")?;
        let name_ref = (name_raw != NONE_ID)
            .then(|| read_entity_id_value(name_raw).map(StringId::new))
            .transpose()?;
        let first_nft_id = NftId::new(read_entity_id_value(read_mapped_u64(
            &self.records,
            record + 26,
            "contract_first_nft_id",
        )?)?);
        let nft_count = read_mapped_u64(&self.records, record + 34, "contract_nft_count")?;
        if id.as_u64() != index {
            return Err(fixed_record_error(
                "contract_mmap",
                "contract IDs are not dense and ordered",
            ));
        }
        Ok(Contract {
            id,
            chain_id: ChainId::new(u16::from_le_bytes(chain_bytes)),
            address_ref,
            name_ref,
            first_nft_id,
            nft_count,
        })
    }

    pub fn iter(&self) -> MappedContractsIter<'_> {
        MappedContractsIter {
            source: self,
            index: 0,
        }
    }
}

pub struct MappedContractsIter<'a> {
    source: &'a MappedContracts,
    index: u64,
}

impl Iterator for MappedContractsIter<'_> {
    type Item = Result<Contract, DedupError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.source.count {
            return None;
        }
        let index = self.index;
        self.index += 1;
        Some(self.source.get(index))
    }
}

pub struct MappedNfts {
    records: ReadOnlySegment,
    count: u64,
}

impl MappedNfts {
    pub fn open(
        artifact_path: impl AsRef<Path>,
        budget: &MemoryBudget,
        residency_bytes: u64,
    ) -> Result<Self, DedupError> {
        let artifact_path = artifact_path.as_ref();
        validate_entity_manifest(artifact_path)?;
        let mut records = ReadOnlySegment::open_with_residency(
            artifact_path.join("nfts.bin"),
            budget,
            residency_bytes.max(1),
        )?;
        let count = read_mapped_u64(&records, 0, "nft_count")?;
        validate_fixed_record_length(records.len(), count, NFT_RECORD_BYTES, "nft_mmap")?;
        records.advise(AccessPattern::Sequential)?;
        Ok(Self { records, count })
    }

    #[must_use]
    pub fn len(&self) -> u64 {
        self.count
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn get(&self, index: u64) -> Result<Nft, DedupError> {
        let record = fixed_record_offset(index, self.count, NFT_RECORD_BYTES, "nft_mmap")?;
        let id = NftId::new(read_entity_id_value(read_mapped_u64(
            &self.records,
            record,
            "nft_id",
        )?)?);
        let contract_id = ContractId::new(read_entity_id_value(read_mapped_u64(
            &self.records,
            record + 8,
            "nft_contract_id",
        )?)?);
        let token_id_ref = StringId::new(read_entity_id_value(read_mapped_u64(
            &self.records,
            record + 16,
            "nft_token_id_ref",
        )?)?);
        let token_uri_ref = mapped_optional_string_id(&self.records, record + 24, "token_uri_ref")?;
        let image_uri_ref = mapped_optional_string_id(&self.records, record + 32, "image_uri_ref")?;
        let boolean = self.records.bytes(record + 40..record + 41)?[0];
        let has_metadata = match boolean {
            0 => false,
            1 => true,
            _ => return Err(fixed_record_error("nft_mmap", "invalid metadata flag")),
        };
        if id.as_u64() != index {
            return Err(fixed_record_error(
                "nft_mmap",
                "NFT IDs are not dense and ordered",
            ));
        }
        Ok(Nft {
            id,
            contract_id,
            token_id_ref,
            token_uri_ref,
            image_uri_ref,
            has_metadata,
        })
    }

    pub fn iter(&self) -> MappedNftsIter<'_> {
        MappedNftsIter {
            source: self,
            index: 0,
        }
    }
}

pub struct MappedNftsIter<'a> {
    source: &'a MappedNfts,
    index: u64,
}

impl Iterator for MappedNftsIter<'_> {
    type Item = Result<Nft, DedupError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.source.count {
            return None;
        }
        let index = self.index;
        self.index += 1;
        Some(self.source.get(index))
    }
}

pub struct MappedEntityObjects {
    pub contracts: MappedContracts,
    pub nfts: MappedNfts,
}

impl MappedEntityObjects {
    pub fn open(
        artifact_path: impl AsRef<Path>,
        budget: &MemoryBudget,
        residency_bytes: u64,
    ) -> Result<Self, DedupError> {
        if residency_bytes < 2 {
            return Err(DedupError::InvalidInput {
                context: ErrorContext::stage("entity_mmap"),
                message: "entity mmap residency budget must be at least two bytes".to_owned(),
            });
        }
        let artifact_path = artifact_path.as_ref();
        let contract_residency = (residency_bytes / 4).max(1);
        let nft_residency = residency_bytes.saturating_sub(contract_residency).max(1);
        Ok(Self {
            contracts: MappedContracts::open(artifact_path, budget, contract_residency)?,
            nfts: MappedNfts::open(artifact_path, budget, nft_residency)?,
        })
    }
}

pub struct MappedStrings {
    offsets: ReadOnlySegment,
    blob: ReadOnlySegment,
    count: u64,
}

impl MappedStrings {
    pub fn open(
        artifact_path: impl AsRef<Path>,
        budget: &MemoryBudget,
        residency_bytes: u64,
    ) -> Result<Self, DedupError> {
        if residency_bytes == 0 {
            return Err(DedupError::InvalidInput {
                context: ErrorContext::stage("string_mmap"),
                message: "string mmap residency budget must be positive".to_owned(),
            });
        }
        let artifact_path = artifact_path.as_ref();
        validate_entity_manifest(artifact_path)?;
        let offset_residency = (residency_bytes / 4).max(1);
        let blob_residency = residency_bytes.saturating_sub(offset_residency);
        let mut offsets = ReadOnlySegment::open_with_residency(
            artifact_path.join("strings.offsets"),
            budget,
            offset_residency,
        )?;
        let mut blob = ReadOnlySegment::open_with_residency(
            artifact_path.join("strings.blob"),
            budget,
            blob_residency,
        )?;
        let count = read_mapped_u64(&offsets, 0, "string_count")?;
        let expected_length = 8_u64
            .checked_add(count.checked_mul(STRING_OFFSET_RECORD_BYTES).ok_or(
                DedupError::CounterOverflow {
                    counter: "string_offset_bytes",
                },
            )?)
            .ok_or(DedupError::CounterOverflow {
                counter: "string_offset_bytes",
            })?;
        if offsets.len() != expected_length {
            return Err(DedupError::ArtifactMismatch {
                context: ErrorContext::stage("string_mmap"),
                message: format!(
                    "string offset length {} does not match expected {expected_length}",
                    offsets.len()
                ),
            });
        }
        if count == 0 {
            if !blob.is_empty() {
                return Err(blob_range_error(
                    "empty string index references a non-empty blob",
                ));
            }
        } else {
            let last_record = 8_u64
                .checked_add((count - 1).checked_mul(STRING_OFFSET_RECORD_BYTES).ok_or(
                    DedupError::CounterOverflow {
                        counter: "string_offset_position",
                    },
                )?)
                .ok_or(DedupError::CounterOverflow {
                    counter: "string_offset_position",
                })?;
            let offset = read_mapped_u64(&offsets, last_record, "string_blob_offset")?;
            let length = read_mapped_u64(
                &offsets,
                last_record
                    .checked_add(8)
                    .ok_or(DedupError::CounterOverflow {
                        counter: "string_offset_position",
                    })?,
                "string_blob_length",
            )?;
            if offset.checked_add(length) != Some(blob.len()) {
                return Err(blob_range_error(
                    "string blob length does not match the final offset",
                ));
            }
        }
        offsets.advise(AccessPattern::Random)?;
        blob.advise(AccessPattern::Random)?;
        Ok(Self {
            offsets,
            blob,
            count,
        })
    }

    pub fn resolve(&self, id: StringId) -> Result<&[u8], DedupError> {
        let index = id.as_u64();
        if index >= self.count {
            return Err(DedupError::ArtifactMismatch {
                context: ErrorContext::stage("string_mmap"),
                message: format!("StringId {index} exceeds string count {}", self.count),
            });
        }
        let record = 8_u64
            .checked_add(index.checked_mul(STRING_OFFSET_RECORD_BYTES).ok_or(
                DedupError::CounterOverflow {
                    counter: "string_offset_position",
                },
            )?)
            .ok_or(DedupError::CounterOverflow {
                counter: "string_offset_position",
            })?;
        let offset = read_mapped_u64(&self.offsets, record, "string_blob_offset")?;
        let length = read_mapped_u64(
            &self.offsets,
            record.checked_add(8).ok_or(DedupError::CounterOverflow {
                counter: "string_offset_position",
            })?,
            "string_blob_length",
        )?;
        let end = offset
            .checked_add(length)
            .ok_or_else(|| blob_range_error("string blob range overflow"))?;
        self.blob.bytes(offset..end)
    }

    #[must_use]
    pub fn len(&self) -> u64 {
        self.count
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
}

pub struct MappedMetadata {
    offsets: ReadOnlySegment,
    blob: ReadOnlySegment,
    count: u64,
}

impl MappedMetadata {
    pub fn open(
        artifact_path: impl AsRef<Path>,
        budget: &MemoryBudget,
        residency_bytes: u64,
    ) -> Result<Self, DedupError> {
        if residency_bytes == 0 {
            return Err(DedupError::InvalidInput {
                context: ErrorContext::stage("metadata_mmap"),
                message: "metadata mmap residency budget must be positive".to_owned(),
            });
        }
        let artifact_path = artifact_path.as_ref();
        let manifest = validate_artifact(artifact_path)?;
        if manifest.schema_version != ENTITY_SCHEMA_VERSION || manifest.stage != "entities" {
            return Err(DedupError::ArtifactMismatch {
                context: ErrorContext::stage("metadata_mmap"),
                message: "entity artifact schema or stage mismatch".to_owned(),
            });
        }
        let offset_residency = (residency_bytes / 4).max(1);
        let blob_residency = residency_bytes.saturating_sub(offset_residency);
        let mut offsets = ReadOnlySegment::open_with_residency(
            artifact_path.join("metadata.offsets"),
            budget,
            offset_residency,
        )?;
        let mut blob = ReadOnlySegment::open_with_residency(
            artifact_path.join("metadata.blob"),
            budget,
            blob_residency,
        )?;
        let count = read_mapped_u64(&offsets, 0, "metadata_count")?;
        let expected_length = 8_u64
            .checked_add(count.checked_mul(METADATA_OFFSET_RECORD_BYTES).ok_or(
                DedupError::CounterOverflow {
                    counter: "metadata_offset_bytes",
                },
            )?)
            .ok_or(DedupError::CounterOverflow {
                counter: "metadata_offset_bytes",
            })?;
        if offsets.len() != expected_length {
            return Err(DedupError::ArtifactMismatch {
                context: ErrorContext::stage("metadata_mmap"),
                message: format!(
                    "metadata offset length {} does not match expected {expected_length}",
                    offsets.len()
                ),
            });
        }
        offsets.advise(AccessPattern::Sequential)?;
        blob.advise(AccessPattern::Sequential)?;
        Ok(Self {
            offsets,
            blob,
            count,
        })
    }

    #[must_use]
    pub fn len(&self) -> u64 {
        self.count
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    #[must_use]
    pub fn mapped_bytes(&self) -> u64 {
        self.offsets.len().saturating_add(self.blob.len())
    }

    pub fn iter(&self) -> MappedMetadataIter<'_> {
        MappedMetadataIter {
            source: self,
            index: 0,
            blob_position: 0,
            finished: false,
        }
    }
}

pub struct MappedMetadataIter<'a> {
    source: &'a MappedMetadata,
    index: u64,
    blob_position: u64,
    finished: bool,
}

impl<'a> Iterator for MappedMetadataIter<'a> {
    type Item = Result<(NftId, &'a str), DedupError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        if self.index == self.source.count {
            self.finished = true;
            return (self.blob_position != self.source.blob.len()).then(|| {
                Err(blob_range_error(
                    "metadata blob has unreferenced trailing bytes",
                ))
            });
        }
        let result = self.read_next();
        if result.is_err() {
            self.finished = true;
        }
        Some(result)
    }
}

impl<'a> MappedMetadataIter<'a> {
    fn read_next(&mut self) -> Result<(NftId, &'a str), DedupError> {
        let record = 8_u64
            .checked_add(self.index.checked_mul(METADATA_OFFSET_RECORD_BYTES).ok_or(
                DedupError::CounterOverflow {
                    counter: "metadata_offset_position",
                },
            )?)
            .ok_or(DedupError::CounterOverflow {
                counter: "metadata_offset_position",
            })?;
        let nft = NftId::new(read_entity_id_value(read_mapped_u64(
            &self.source.offsets,
            record,
            "metadata_nft_id",
        )?)?);
        let offset = read_mapped_u64(
            &self.source.offsets,
            record.checked_add(8).ok_or(DedupError::CounterOverflow {
                counter: "metadata_offset_position",
            })?,
            "metadata_blob_offset",
        )?;
        let length = read_mapped_u64(
            &self.source.offsets,
            record.checked_add(16).ok_or(DedupError::CounterOverflow {
                counter: "metadata_offset_position",
            })?,
            "metadata_blob_length",
        )?;
        if offset != self.blob_position {
            return Err(blob_range_error("metadata blob offsets are not contiguous"));
        }
        let end = offset
            .checked_add(length)
            .ok_or_else(|| blob_range_error("metadata blob range overflow"))?;
        let bytes = self.source.blob.bytes(offset..end)?;
        let content = std::str::from_utf8(bytes).map_err(|error| DedupError::ArtifactMismatch {
            context: ErrorContext::stage("metadata_mmap"),
            message: error.to_string(),
        })?;
        self.index = self
            .index
            .checked_add(1)
            .ok_or(DedupError::CounterOverflow {
                counter: "metadata_record_index",
            })?;
        self.blob_position = end;
        Ok((nft, content))
    }
}

fn read_mapped_u64(
    segment: &ReadOnlySegment,
    offset: u64,
    field: &'static str,
) -> Result<u64, DedupError> {
    let end = offset
        .checked_add(8)
        .ok_or(DedupError::CounterOverflow { counter: field })?;
    let bytes: [u8; 8] =
        segment
            .bytes(offset..end)?
            .try_into()
            .map_err(|_| DedupError::ArtifactMismatch {
                context: ErrorContext::stage("metadata_mmap"),
                message: format!("{field} is not eight bytes"),
            })?;
    Ok(u64::from_le_bytes(bytes))
}

pub fn write_entity_artifact(
    path: impl AsRef<Path>,
    artifacts: &PersistedEntityArtifacts,
    logical_input_digest: String,
    configuration_digest: String,
) -> Result<ArtifactManifest, DedupError> {
    let manifest = ArtifactManifest {
        schema_version: ENTITY_SCHEMA_VERSION,
        stage: "entities".to_owned(),
        logical_input_digest,
        configuration_digest,
        upstream_checksums: BTreeMap::new(),
        data_checksums: BTreeMap::new(),
    };
    let mut artifact = ArtifactWriter::new(path, manifest)?;
    write_blob(
        artifact.create_data_file("strings.blob")?,
        artifacts.strings.iter().map(Vec::as_slice),
    )?;
    write_string_offsets(
        artifact.create_data_file("strings.offsets")?,
        &artifacts.strings,
    )?;
    write_contracts(
        artifact.create_data_file("contracts.bin")?,
        &artifacts.entities.contracts,
    )?;
    write_nfts(
        artifact.create_data_file("nfts.bin")?,
        &artifacts.entities.nfts,
    )?;
    write_blob(
        artifact.create_data_file("metadata.blob")?,
        artifacts
            .metadata_by_nft
            .iter()
            .map(|(_, value)| value.as_bytes()),
    )?;
    write_metadata_offsets(
        artifact.create_data_file("metadata.offsets")?,
        &artifacts.metadata_by_nft,
    )?;
    artifact.commit()
}

pub fn write_entity_artifact_from_files(
    path: impl AsRef<Path>,
    files: &EntityArtifactFiles,
    logical_input_digest: String,
    configuration_digest: String,
) -> Result<ArtifactManifest, DedupError> {
    let manifest = ArtifactManifest {
        schema_version: ENTITY_SCHEMA_VERSION,
        stage: "entities".to_owned(),
        logical_input_digest,
        configuration_digest,
        upstream_checksums: BTreeMap::new(),
        data_checksums: BTreeMap::new(),
    };
    let mut artifact = ArtifactWriter::new(path, manifest)?;
    for (name, source) in [
        ("strings.offsets", &files.strings_offsets),
        ("strings.blob", &files.strings_blob),
        ("contracts.bin", &files.contracts),
        ("nfts.bin", &files.nfts),
        ("metadata.offsets", &files.metadata_offsets),
        ("metadata.blob", &files.metadata_blob),
    ] {
        let mut input = BufReader::with_capacity(1024 * 1024, File::open(source)?);
        std::io::copy(&mut input, artifact.create_data_file(name)?)?;
    }
    artifact.commit()
}

pub fn read_entity_artifact(
    path: impl AsRef<Path>,
) -> Result<PersistedEntityArtifacts, DedupError> {
    read_entity_artifact_internal(path.as_ref(), true)
}

pub fn read_entity_artifact_without_metadata(
    path: impl AsRef<Path>,
) -> Result<PersistedEntityArtifacts, DedupError> {
    read_entity_artifact_internal(path.as_ref(), false)
}

pub fn read_entity_objects(path: impl AsRef<Path>) -> Result<EntityArtifacts, DedupError> {
    let path = path.as_ref();
    validate_entity_manifest(path)?;
    Ok(EntityArtifacts {
        contracts: read_contracts(&path.join("contracts.bin"))?,
        nfts: read_nfts(&path.join("nfts.bin"))?,
    })
}

fn read_entity_artifact_internal(
    path: &Path,
    include_metadata: bool,
) -> Result<PersistedEntityArtifacts, DedupError> {
    Ok(PersistedEntityArtifacts {
        entities: read_entity_objects(path)?,
        strings: read_strings(&path.join("strings.offsets"), &path.join("strings.blob"))?,
        metadata_by_nft: if include_metadata {
            read_metadata(&path.join("metadata.offsets"), &path.join("metadata.blob"))?
        } else {
            Vec::new()
        },
    })
}

fn validate_entity_manifest(path: &Path) -> Result<ArtifactManifest, DedupError> {
    let manifest = validate_artifact(path)?;
    if manifest.schema_version != ENTITY_SCHEMA_VERSION || manifest.stage != "entities" {
        return Err(DedupError::ArtifactMismatch {
            context: ErrorContext::stage("entities"),
            message: "entity artifact schema or stage mismatch".to_owned(),
        });
    }
    Ok(manifest)
}

fn validate_fixed_record_length(
    actual: u64,
    count: u64,
    record_bytes: u64,
    stage: &'static str,
) -> Result<(), DedupError> {
    let expected = 8_u64
        .checked_add(
            count
                .checked_mul(record_bytes)
                .ok_or(DedupError::CounterOverflow {
                    counter: "mapped_entity_record_bytes",
                })?,
        )
        .ok_or(DedupError::CounterOverflow {
            counter: "mapped_entity_record_bytes",
        })?;
    if actual != expected {
        return Err(fixed_record_error(
            stage,
            &format!("record file length {actual} does not match expected {expected}"),
        ));
    }
    Ok(())
}

fn fixed_record_offset(
    index: u64,
    count: u64,
    record_bytes: u64,
    stage: &'static str,
) -> Result<u64, DedupError> {
    if index >= count {
        return Err(fixed_record_error(
            stage,
            &format!("record index {index} exceeds count {count}"),
        ));
    }
    8_u64
        .checked_add(
            index
                .checked_mul(record_bytes)
                .ok_or(DedupError::CounterOverflow {
                    counter: "mapped_entity_record_offset",
                })?,
        )
        .ok_or(DedupError::CounterOverflow {
            counter: "mapped_entity_record_offset",
        })
}

fn mapped_optional_string_id(
    records: &ReadOnlySegment,
    offset: u64,
    field: &'static str,
) -> Result<Option<StringId>, DedupError> {
    let raw = read_mapped_u64(records, offset, field)?;
    (raw != NONE_ID)
        .then(|| read_entity_id_value(raw).map(StringId::new))
        .transpose()
}

fn fixed_record_error(stage: &'static str, message: &str) -> DedupError {
    DedupError::ArtifactMismatch {
        context: ErrorContext::stage(stage),
        message: message.to_owned(),
    }
}

fn write_string_offsets(offsets: &mut File, values: &[Vec<u8>]) -> Result<(), DedupError> {
    let mut offsets = BufWriter::new(offsets);
    write_u64(&mut offsets, checked_len(values.len(), "string_count")?)?;
    let mut position = 0_u64;
    for value in values {
        write_u64(&mut offsets, position)?;
        write_u64(&mut offsets, checked_len(value.len(), "string_length")?)?;
        position = position
            .checked_add(checked_len(value.len(), "string_blob_bytes")?)
            .ok_or(DedupError::CounterOverflow {
                counter: "string_blob_bytes",
            })?;
    }
    offsets.flush()?;
    Ok(())
}

fn write_blob<'a>(
    blob: &mut File,
    values: impl IntoIterator<Item = &'a [u8]>,
) -> Result<(), DedupError> {
    let mut blob = BufWriter::new(blob);
    for value in values {
        blob.write_all(value)?;
    }
    blob.flush()?;
    Ok(())
}

fn read_strings(offsets: &Path, blob: &Path) -> Result<Vec<Vec<u8>>, DedupError> {
    let mut offsets = BufReader::new(File::open(offsets)?);
    let count = read_count(&mut offsets, "string_count")?;
    let blob_file = File::open(blob)?;
    let blob_length = blob_file.metadata()?.len();
    let mut blob = BufReader::new(blob_file);
    let mut values = Vec::with_capacity(count);
    let mut position = 0_u64;
    for _ in 0..count {
        let offset = read_u64(&mut offsets)?;
        let length = read_u64(&mut offsets)?;
        values.push(read_packed_blob_entry(
            &mut blob,
            offset,
            length,
            &mut position,
            blob_length,
            "string",
        )?);
    }
    if position != blob_length {
        return Err(blob_range_error(
            "string blob has unreferenced trailing bytes",
        ));
    }
    Ok(values)
}

fn write_contracts(file: &mut File, contracts: &[Contract]) -> Result<(), DedupError> {
    let mut writer = BufWriter::new(file);
    write_u64(&mut writer, checked_len(contracts.len(), "contract_count")?)?;
    for contract in contracts {
        write_u64(&mut writer, contract.id.as_u64())?;
        writer.write_all(&contract.chain_id.get().to_le_bytes())?;
        write_u64(&mut writer, contract.address_ref.as_u64())?;
        write_u64(
            &mut writer,
            contract.name_ref.map_or(NONE_ID, StringId::as_u64),
        )?;
        write_u64(&mut writer, contract.first_nft_id.as_u64())?;
        write_u64(&mut writer, contract.nft_count)?;
    }
    writer.flush()?;
    Ok(())
}

fn read_contracts(path: &Path) -> Result<Vec<Contract>, DedupError> {
    let mut reader = BufReader::new(File::open(path)?);
    let count = read_count(&mut reader, "contract_count")?;
    let mut contracts = Vec::with_capacity(count);
    for _ in 0..count {
        contracts.push(Contract {
            id: ContractId::new(read_entity_id(&mut reader)?),
            chain_id: ChainId::new(read_u16(&mut reader)?),
            address_ref: StringId::new(read_entity_id(&mut reader)?),
            name_ref: read_optional_id(&mut reader)?.map(StringId::new),
            first_nft_id: NftId::new(read_entity_id(&mut reader)?),
            nft_count: read_u64(&mut reader)?,
        });
    }
    ensure_eof(reader)?;
    Ok(contracts)
}

fn write_nfts(file: &mut File, nfts: &[Nft]) -> Result<(), DedupError> {
    let mut writer = BufWriter::new(file);
    write_u64(&mut writer, checked_len(nfts.len(), "nft_count")?)?;
    for nft in nfts {
        write_u64(&mut writer, nft.id.as_u64())?;
        write_u64(&mut writer, nft.contract_id.as_u64())?;
        write_u64(&mut writer, nft.token_id_ref.as_u64())?;
        write_u64(
            &mut writer,
            nft.token_uri_ref.map_or(NONE_ID, StringId::as_u64),
        )?;
        write_u64(
            &mut writer,
            nft.image_uri_ref.map_or(NONE_ID, StringId::as_u64),
        )?;
        writer.write_all(&[u8::from(nft.has_metadata)])?;
    }
    writer.flush()?;
    Ok(())
}

fn read_nfts(path: &Path) -> Result<Vec<Nft>, DedupError> {
    let mut reader = BufReader::new(File::open(path)?);
    let count = read_count(&mut reader, "nft_count")?;
    let mut nfts = Vec::with_capacity(count);
    for _ in 0..count {
        let id = NftId::new(read_entity_id(&mut reader)?);
        let contract_id = ContractId::new(read_entity_id(&mut reader)?);
        let token_id_ref = StringId::new(read_entity_id(&mut reader)?);
        let token_uri_ref = read_optional_id(&mut reader)?.map(StringId::new);
        let image_uri_ref = read_optional_id(&mut reader)?.map(StringId::new);
        let mut boolean = [0];
        reader.read_exact(&mut boolean)?;
        let has_metadata = match boolean[0] {
            0 => false,
            1 => true,
            _ => {
                return Err(DedupError::ArtifactMismatch {
                    context: ErrorContext::stage("entities"),
                    message: "invalid boolean in NFT artifact".to_owned(),
                });
            }
        };
        nfts.push(Nft {
            id,
            contract_id,
            token_id_ref,
            token_uri_ref,
            image_uri_ref,
            has_metadata,
        });
    }
    ensure_eof(reader)?;
    Ok(nfts)
}

fn write_metadata_offsets(
    offsets: &mut File,
    metadata: &[(NftId, String)],
) -> Result<(), DedupError> {
    let mut offsets = BufWriter::new(offsets);
    write_u64(&mut offsets, checked_len(metadata.len(), "metadata_count")?)?;
    let mut position = 0_u64;
    for (nft, value) in metadata {
        write_u64(&mut offsets, nft.as_u64())?;
        write_u64(&mut offsets, position)?;
        write_u64(&mut offsets, checked_len(value.len(), "metadata_length")?)?;
        position = position
            .checked_add(checked_len(value.len(), "metadata_blob_bytes")?)
            .ok_or(DedupError::CounterOverflow {
                counter: "metadata_blob_bytes",
            })?;
    }
    offsets.flush()?;
    Ok(())
}

fn read_metadata(offsets: &Path, blob: &Path) -> Result<Vec<(NftId, String)>, DedupError> {
    let mut offsets = BufReader::new(File::open(offsets)?);
    let count = read_count(&mut offsets, "metadata_count")?;
    let blob_file = File::open(blob)?;
    let blob_length = blob_file.metadata()?.len();
    let mut blob = BufReader::new(blob_file);
    let mut metadata = Vec::with_capacity(count);
    let mut position = 0_u64;
    for _ in 0..count {
        let nft = NftId::new(read_entity_id(&mut offsets)?);
        let offset = read_u64(&mut offsets)?;
        let length = read_u64(&mut offsets)?;
        let value = read_packed_blob_entry(
            &mut blob,
            offset,
            length,
            &mut position,
            blob_length,
            "metadata",
        )?;
        let value = String::from_utf8(value).map_err(|error| DedupError::ArtifactMismatch {
            context: ErrorContext::stage("entities"),
            message: error.to_string(),
        })?;
        metadata.push((nft, value));
    }
    if position != blob_length {
        return Err(blob_range_error(
            "metadata blob has unreferenced trailing bytes",
        ));
    }
    Ok(metadata)
}

fn read_packed_blob_entry(
    reader: &mut impl Read,
    offset: u64,
    length: u64,
    position: &mut u64,
    blob_length: u64,
    kind: &'static str,
) -> Result<Vec<u8>, DedupError> {
    if offset != *position {
        return Err(blob_range_error(format!(
            "{kind} blob offsets are not contiguous"
        )));
    }
    let end = offset
        .checked_add(length)
        .ok_or_else(|| blob_range_error(format!("{kind} blob range overflow")))?;
    if end > blob_length {
        return Err(blob_range_error(format!(
            "{kind} blob range is out of bounds"
        )));
    }
    let mut value = vec![
        0;
        usize::try_from(length).map_err(|_| blob_range_error(format!(
            "{kind} length does not fit usize"
        )))?
    ];
    reader.read_exact(&mut value)?;
    *position = end;
    Ok(value)
}

fn blob_range_error(message: impl Into<String>) -> DedupError {
    DedupError::ArtifactMismatch {
        context: ErrorContext::stage("entities"),
        message: message.into(),
    }
}

fn write_u64(writer: &mut impl Write, value: u64) -> Result<(), DedupError> {
    writer.write_all(&value.to_le_bytes())?;
    Ok(())
}

fn read_u64(reader: &mut impl Read) -> Result<u64, DedupError> {
    let mut bytes = [0; 8];
    reader.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

fn read_u16(reader: &mut impl Read) -> Result<u16, DedupError> {
    let mut bytes = [0; 2];
    reader.read_exact(&mut bytes)?;
    Ok(u16::from_le_bytes(bytes))
}

fn read_optional_id(reader: &mut impl Read) -> Result<Option<dedup_model::EntityId>, DedupError> {
    let value = read_u64(reader)?;
    if value == NONE_ID {
        Ok(None)
    } else {
        read_entity_id_value(value).map(Some)
    }
}

fn read_entity_id(reader: &mut impl Read) -> Result<dedup_model::EntityId, DedupError> {
    read_entity_id_value(read_u64(reader)?)
}

fn read_entity_id_value(value: u64) -> Result<dedup_model::EntityId, DedupError> {
    dedup_model::EntityId::try_from(value).map_err(|_| DedupError::ArtifactMismatch {
        context: ErrorContext::stage("entities"),
        message: "entity ID does not fit this build; use wide_ids".to_owned(),
    })
}

fn read_count(reader: &mut impl Read, counter: &'static str) -> Result<usize, DedupError> {
    usize::try_from(read_u64(reader)?).map_err(|_| DedupError::ArtifactMismatch {
        context: ErrorContext::stage("entities"),
        message: format!("{counter} does not fit usize"),
    })
}

fn checked_len(value: usize, counter: &'static str) -> Result<u64, DedupError> {
    u64::try_from(value).map_err(|_| DedupError::CounterOverflow { counter })
}

fn ensure_eof(mut reader: impl Read) -> Result<(), DedupError> {
    let mut byte = [0];
    if reader.read(&mut byte)? != 0 {
        return Err(DedupError::ArtifactMismatch {
            context: ErrorContext::stage("entities"),
            message: "trailing bytes in fixed-width artifact".to_owned(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use dedup_model::{EntityId, PersistedEntityArtifacts};

    #[test]
    fn binary_entity_artifact_round_trips() {
        let persisted = PersistedEntityArtifacts {
            strings: vec![b"address".to_vec(), b"name".to_vec(), b"token".to_vec()],
            entities: EntityArtifacts {
                contracts: vec![Contract {
                    id: ContractId::new(0),
                    chain_id: ChainId::new(1),
                    address_ref: StringId::new(0),
                    name_ref: Some(StringId::new(1)),
                    first_nft_id: NftId::new(0),
                    nft_count: 1,
                }],
                nfts: vec![Nft {
                    id: NftId::new(0),
                    contract_id: ContractId::new(0),
                    token_id_ref: StringId::new(2),
                    token_uri_ref: None,
                    image_uri_ref: None,
                    has_metadata: true,
                }],
            },
            metadata_by_nft: vec![(NftId::new(0), r#"{"x":1}"#.to_owned())],
        };
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("entities");
        write_entity_artifact(&path, &persisted, "input".to_owned(), "config".to_owned()).unwrap();
        assert_eq!(read_entity_artifact(&path).unwrap(), persisted);
        let budget = MemoryBudget::new(1024 * 1024, 1024 * 1024);
        let mapped_strings = MappedStrings::open(&path, &budget, 4096).unwrap();
        assert_eq!(mapped_strings.len(), 3);
        assert_eq!(mapped_strings.resolve(StringId::new(1)).unwrap(), b"name");
        let mapped = MappedMetadata::open(&path, &budget, 4096).unwrap();
        assert_eq!(mapped.len(), 1);
        assert_eq!(
            mapped.iter().collect::<Result<Vec<_>, _>>().unwrap(),
            vec![(NftId::new(0), r#"{"x":1}"#)]
        );
        let mapped_entities = MappedEntityObjects::open(&path, &budget, 8192).unwrap();
        assert_eq!(mapped_entities.contracts.len(), 1);
        assert_eq!(mapped_entities.nfts.len(), 1);
        assert_eq!(
            mapped_entities
                .contracts
                .iter()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            persisted.entities.contracts
        );
        assert_eq!(
            mapped_entities
                .nfts
                .iter()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            persisted.entities.nfts
        );
        drop(mapped_strings);
        drop(mapped);
        drop(mapped_entities);
        assert_eq!(budget.used(), 0);
        assert_eq!(EntityId::try_from(0_u64).unwrap(), 0);
    }
}
