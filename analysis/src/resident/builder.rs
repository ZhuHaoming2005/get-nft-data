use crate::error::{AnalysisError, Result};
use crate::model::{
    ChainId, ContractId, ContractRecord, InputQuality, InputRow, MetadataAnchor, MetadataId,
    NameValueId, NftIdentityRecord, ProfileId, TokenIdId, UriFeatureRecord, UriValueId,
};
use crate::resident::{
    ByteInterner, ContractCatalog, MetadataFeatureStore, MetadataProfile, NameFeatureStore,
    ResidentBaseStore, UriFeatureStore, UriNftIdentityStore,
};
use ahash::AHashMap;
use rayon::prelude::*;
use std::borrow::Cow;
use std::cmp::Ordering;

#[derive(Clone, Copy, Debug)]
struct CompactRow {
    chain: ChainId,
    address_id: u32,
    token_id_id: u32,
    name_id: Option<u32>,
    token_uri_id: Option<u32>,
    image_uri_id: Option<u32>,
    metadata_id: Option<u32>,
    metadata_source: Option<crate::model::SourceOrder>,
    source: crate::model::SourceOrder,
}

enum MetadataPreparationPart {
    Address(Vec<u32>),
    EvmToken(Vec<u32>),
    SolanaToken(Vec<u32>),
}

#[derive(Debug, Default)]
pub struct ResidentBuilder {
    addresses: ByteInterner,
    token_ids: ByteInterner,
    names: ByteInterner,
    uris: ByteInterner,
    metadata: ByteInterner,
    logical_rows: Vec<CompactRow>,
    contract_ranges: Vec<(usize, usize)>,
    contract_by_key: AHashMap<(ChainId, u32), u32>,
    metadata_anchor_rows: Vec<[u32; 8]>,
    metadata_source_rows: Vec<Vec<u32>>,
    metadata_row_contracts: Vec<u32>,
    metadata_anchor_count: usize,
    evm_token_rank: Vec<u32>,
    solana_token_rank: Vec<u32>,
    quality: InputQuality,
}

pub enum PreparedMetadataInput {
    Missing,
    Oversized,
    Invalid,
    Ignored,
    NonAnchor,
    Valid(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataInputDisposition {
    Anchor,
    NonAnchor,
    Duplicate,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct MetadataInputTarget {
    chain: ChainId,
    contract: usize,
    row_index: usize,
    token_rank: u32,
    disposition: MetadataInputDisposition,
}

impl MetadataInputTarget {
    pub(crate) fn disposition(self) -> MetadataInputDisposition {
        self.disposition
    }
}

impl PreparedMetadataInput {
    pub fn from_raw(raw: Option<&str>) -> Self {
        Self::from_raw_for_disposition(raw, MetadataInputDisposition::Anchor)
    }

    pub fn from_raw_for_disposition(
        raw: Option<&str>,
        disposition: MetadataInputDisposition,
    ) -> Self {
        let Some(raw) = raw.map(str::trim).filter(|value| !value.is_empty()) else {
            return Self::Missing;
        };
        if raw.len() > 64 * 1024 {
            return Self::Oversized;
        }
        match disposition {
            MetadataInputDisposition::Anchor => {
                match crate::resident::metadata_index::canonicalize_json(raw) {
                    Some(canonical) if canonical != "{}" => Self::Valid(canonical),
                    _ => Self::Invalid,
                }
            }
            MetadataInputDisposition::NonAnchor | MetadataInputDisposition::Duplicate => {
                if !crate::resident::metadata_index::is_valid_metadata_json(raw) {
                    Self::Invalid
                } else if disposition == MetadataInputDisposition::NonAnchor {
                    Self::NonAnchor
                } else {
                    Self::Ignored
                }
            }
        }
    }
}

impl ResidentBuilder {
    pub fn merge_from(&mut self, mut other: Self) -> Result<()> {
        if !self.contract_ranges.is_empty() || !other.contract_ranges.is_empty() {
            return Err(AnalysisError::State(
                "resident builders can only merge before metadata preparation".into(),
            ));
        }
        let address_remap = merge_pool(&mut self.addresses, &other.addresses);
        let token_remap = merge_pool(&mut self.token_ids, &other.token_ids);
        let name_remap = merge_pool(&mut self.names, &other.names);
        let uri_remap = merge_pool(&mut self.uris, &other.uris);
        let metadata_remap = merge_pool(&mut self.metadata, &other.metadata);
        self.logical_rows.reserve(other.logical_rows.len());
        for mut row in other.logical_rows.drain(..) {
            row.address_id = address_remap[row.address_id as usize];
            row.token_id_id = token_remap[row.token_id_id as usize];
            row.name_id = row.name_id.map(|id| name_remap[id as usize]);
            row.token_uri_id = row.token_uri_id.map(|id| uri_remap[id as usize]);
            row.image_uri_id = row.image_uri_id.map(|id| uri_remap[id as usize]);
            row.metadata_id = row.metadata_id.map(|id| metadata_remap[id as usize]);
            self.logical_rows.push(row);
        }
        accumulate_input_quality(&mut self.quality, &other.quality);
        Ok(())
    }

    pub fn estimated_bytes(&self) -> u64 {
        self.addresses
            .estimated_bytes()
            .saturating_add(self.token_ids.estimated_bytes())
            .saturating_add(self.names.estimated_bytes())
            .saturating_add(self.uris.estimated_bytes())
            .saturating_add(self.metadata.estimated_bytes())
            .saturating_add(
                self.logical_rows.capacity() as u64 * std::mem::size_of::<CompactRow>() as u64,
            )
            .saturating_add(
                self.contract_by_key.capacity() as u64
                    * std::mem::size_of::<((ChainId, u32), u32)>() as u64
                    * 2,
            )
            .saturating_add(
                self.metadata_anchor_rows.capacity() as u64
                    * std::mem::size_of::<[u32; 8]>() as u64,
            )
            .saturating_add(self.metadata_source_rows.iter().fold(
                self.metadata_source_rows.capacity() as u64
                    * std::mem::size_of::<Vec<u32>>() as u64,
                |bytes, rows| {
                    bytes.saturating_add(rows.capacity() as u64 * std::mem::size_of::<u32>() as u64)
                },
            ))
            .saturating_add(
                self.metadata_row_contracts.capacity() as u64 * std::mem::size_of::<u32>() as u64,
            )
            .saturating_add(
                (self.evm_token_rank.capacity() + self.solana_token_rank.capacity()) as u64
                    * std::mem::size_of::<u32>() as u64,
            )
    }

    pub fn preparation_peak_bytes(&self) -> u64 {
        self.estimated_bytes()
            .saturating_add(
                self.logical_rows.len() as u64 * std::mem::size_of::<CompactRow>() as u64,
            )
            .saturating_add(self.addresses.len() as u64 * 8)
            .saturating_add(self.logical_rows.len() as u64 * 64)
    }

    pub fn push(&mut self, row: InputRow) -> Result<()> {
        self.push_borrowed(
            row.chain,
            &row.contract_address,
            &row.token_id,
            row.name_norm.as_deref(),
            row.token_uri_norm.as_deref(),
            row.image_uri_norm.as_deref(),
            row.metadata_json.as_deref(),
            row.source_order,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn push_borrowed(
        &mut self,
        chain: ChainId,
        contract_address: &str,
        token_id: &str,
        name_norm: Option<&str>,
        token_uri_norm: Option<&str>,
        image_uri_norm: Option<&str>,
        metadata_json: Option<&str>,
        source_order: crate::model::SourceOrder,
    ) -> Result<()> {
        self.quality.physical_rows += 1;
        let raw_address = contract_address.trim();
        let address = if chain.is_evm() {
            ascii_lowercase_if_needed(raw_address)
        } else {
            Cow::Borrowed(raw_address)
        };
        let raw_token = token_id.trim();
        let token = if chain.is_evm() {
            if let Some(decimal) = canonical_decimal_digits(raw_token) {
                Cow::Borrowed(decimal)
            } else {
                ascii_lowercase_if_needed(raw_token)
            }
        } else {
            Cow::Borrowed(raw_token)
        };
        if address.is_empty() || token.is_empty() {
            return Err(AnalysisError::Input(format!(
                "empty contract or token at {:?}",
                source_order
            )));
        }
        let address_id = self.addresses.intern(address.as_ref());
        let token_id_id = self.token_ids.intern(token.as_ref());
        let metadata_id = metadata_json.and_then(|raw| {
            if raw.trim().len() > 64 * 1024 {
                self.quality.oversized_metadata += 1;
                return None;
            }
            let canonical = crate::resident::metadata_index::canonicalize_json(raw);
            match canonical {
                Some(value) if value != "{}" => Some(self.metadata.intern(&value)),
                _ => {
                    self.quality.invalid_metadata += 1;
                    None
                }
            }
        });
        let compact = CompactRow {
            chain,
            address_id,
            token_id_id,
            name_id: meaningful_name(name_norm).map(|value| self.names.intern(value)),
            token_uri_id: nonempty(token_uri_norm).map(|value| self.uris.intern(value)),
            image_uri_id: nonempty(image_uri_norm).map(|value| self.uris.intern(value)),
            metadata_id,
            metadata_source: metadata_id.map(|_| source_order),
            source: source_order,
        };
        self.quality.empty_names += u64::from(compact.name_id.is_none());
        self.quality.empty_token_uris += u64::from(compact.token_uri_id.is_none());
        self.quality.empty_image_uris += u64::from(compact.image_uri_id.is_none());

        self.logical_rows.push(compact);
        Ok(())
    }

    pub fn finish(
        mut self,
        metadata_anchor_count: usize,
        index_shards: usize,
    ) -> Result<ResidentBaseStore> {
        if self.contract_ranges.is_empty() && !self.logical_rows.is_empty() {
            self.prepare_metadata(metadata_anchor_count)?;
        }
        self.finish_prepared(index_shards)
    }

    pub fn prepare_metadata(&mut self, metadata_anchor_count: usize) -> Result<()> {
        self.validate_metadata_preparation(metadata_anchor_count)?;
        let address_rank = address_ranks(&self.addresses);
        let (evm_token_rank, solana_token_rank) = rayon::join(
            || token_ranks(&self.logical_rows, &self.token_ids, true),
            || token_ranks(&self.logical_rows, &self.token_ids, false),
        );
        self.prepare_metadata_ranked(
            metadata_anchor_count,
            address_rank,
            evm_token_rank,
            solana_token_rank,
        )
    }

    pub(crate) fn prepare_metadata_numa(
        &mut self,
        metadata_anchor_count: usize,
        executor: &crate::pipeline::CpuExecutor,
    ) -> Result<()> {
        self.validate_metadata_preparation(metadata_anchor_count)?;
        if executor.numa_pool_count() == 1 {
            return executor.install(|| self.prepare_metadata(metadata_anchor_count));
        }
        // The three rank tables are independent and relatively expensive on a
        // large snapshot. Build them on separate NUMA pools so preparation is
        // not artificially limited to the workers of lane zero.
        let parts = executor.install_on_all(|lane, lane_count| {
            (lane..3)
                .step_by(lane_count)
                .map(|part| match part {
                    0 => MetadataPreparationPart::EvmToken(token_ranks(
                        &self.logical_rows,
                        &self.token_ids,
                        true,
                    )),
                    1 => MetadataPreparationPart::SolanaToken(token_ranks(
                        &self.logical_rows,
                        &self.token_ids,
                        false,
                    )),
                    2 => MetadataPreparationPart::Address(address_ranks(&self.addresses)),
                    _ => unreachable!(),
                })
                .collect::<Vec<_>>()
        });
        let mut address_rank = None;
        let mut evm_token_rank = None;
        let mut solana_token_rank = None;
        for part in parts.into_iter().flatten() {
            match part {
                MetadataPreparationPart::Address(value) => address_rank = Some(value),
                MetadataPreparationPart::EvmToken(value) => evm_token_rank = Some(value),
                MetadataPreparationPart::SolanaToken(value) => solana_token_rank = Some(value),
            }
        }
        let missing = || AnalysisError::State("metadata preparation rank task was omitted".into());
        let address_rank = address_rank.ok_or_else(missing)?;
        let evm_token_rank = evm_token_rank.ok_or_else(missing)?;
        let solana_token_rank = solana_token_rank.ok_or_else(missing)?;
        executor.install(|| {
            self.prepare_metadata_ranked(
                metadata_anchor_count,
                address_rank,
                evm_token_rank,
                solana_token_rank,
            )
        })
    }

    fn validate_metadata_preparation(&self, metadata_anchor_count: usize) -> Result<()> {
        if metadata_anchor_count == 0 || metadata_anchor_count > 8 {
            return Err(AnalysisError::Config(
                "metadata anchor count must be in 1..=8".into(),
            ));
        }
        if !self.contract_ranges.is_empty() {
            return Err(AnalysisError::State(
                "resident builder was prepared more than once".into(),
            ));
        }
        Ok(())
    }

    fn prepare_metadata_ranked(
        &mut self,
        metadata_anchor_count: usize,
        address_rank: Vec<u32>,
        evm_token_rank: Vec<u32>,
        solana_token_rank: Vec<u32>,
    ) -> Result<()> {
        self.logical_rows.par_sort_unstable_by(|left, right| {
            left.chain
                .cmp(&right.chain)
                .then_with(|| {
                    address_rank[left.address_id as usize]
                        .cmp(&address_rank[right.address_id as usize])
                })
                .then_with(|| {
                    let ranks = if left.chain.is_evm() {
                        &evm_token_rank
                    } else {
                        &solana_token_rank
                    };
                    ranks[left.token_id_id as usize].cmp(&ranks[right.token_id_id as usize])
                })
                .then(left.source.cmp(&right.source))
        });
        drop(address_rank);
        self.evm_token_rank = evm_token_rank;
        self.solana_token_rank = solana_token_rank;
        let rows = std::mem::take(&mut self.logical_rows);
        check_capacity("physical input rows", rows.len())?;
        let source_file_count = rows
            .iter()
            .map(|row| usize::from(row.source.file_ordinal) + 1)
            .max()
            .unwrap_or(0);
        let mut source_lengths = vec![0_usize; source_file_count];
        for row in &rows {
            let row_number = usize::try_from(row.source.file_row_number).map_err(|_| {
                AnalysisError::Input(format!(
                    "source row does not fit platform usize: {:?}",
                    row.source
                ))
            })?;
            source_lengths[usize::from(row.source.file_ordinal)] = source_lengths
                [usize::from(row.source.file_ordinal)]
            .max(row_number.saturating_add(1));
        }
        let mut metadata_source_rows = source_lengths
            .into_iter()
            .map(|length| vec![u32::MAX; length])
            .collect::<Vec<_>>();
        let mut unique_rows = Vec::with_capacity(rows.len());
        for row in rows {
            let source = row.source;
            let duplicate = unique_rows
                .last()
                .is_some_and(|existing| same_nft(existing, &row));
            let row_index = if duplicate {
                let row_index = unique_rows.len() - 1;
                let existing = &mut unique_rows[row_index];
                self.quality.duplicate_rows += 1;
                self.quality.conflicting_rows += u64::from(differs(existing, &row));
                if existing.metadata_source.is_none() && row.metadata_source.is_some() {
                    existing.metadata_id = row.metadata_id;
                    existing.metadata_source = row.metadata_source;
                }
                row_index
            } else {
                let row_index = unique_rows.len();
                unique_rows.push(row);
                row_index
            };
            let source_slot = metadata_source_rows
                .get_mut(usize::from(source.file_ordinal))
                .zip(usize::try_from(source.file_row_number).ok())
                .and_then(|(rows, row)| rows.get_mut(row))
                .ok_or_else(|| {
                    AnalysisError::State(format!("metadata source lookup omitted {source:?}"))
                })?;
            if *source_slot != u32::MAX {
                return Err(AnalysisError::Input(format!(
                    "duplicate SourceOrder in snapshot: {source:?}"
                )));
            }
            *source_slot = u32::try_from(row_index).map_err(|_| {
                AnalysisError::Input("logical NFT index exceeds u32 capacity".into())
            })?;
        }
        self.logical_rows = unique_rows;
        self.metadata_source_rows = metadata_source_rows;
        check_capacity("logical NFTs", self.logical_rows.len())?;
        self.quality.logical_nfts = self.logical_rows.len() as u64;

        let mut contract_ranges = Vec::<(usize, usize)>::new();
        let mut start = 0;
        while start < self.logical_rows.len() {
            let first = self.logical_rows[start];
            let mut end = start + 1;
            while end < self.logical_rows.len()
                && self.logical_rows[end].chain == first.chain
                && self.logical_rows[end].address_id == first.address_id
            {
                end += 1;
            }
            contract_ranges.push((start, end));
            start = end;
        }
        check_capacity("contracts", contract_ranges.len())?;
        let mut metadata_row_contracts = vec![u32::MAX; self.logical_rows.len()];
        for (contract, &(start, end)) in contract_ranges.iter().enumerate() {
            metadata_row_contracts[start..end].fill(
                u32::try_from(contract).map_err(|_| {
                    AnalysisError::Input("contract index exceeds u32 capacity".into())
                })?,
            );
        }
        self.metadata_row_contracts = metadata_row_contracts;
        self.contract_by_key = AHashMap::with_capacity(contract_ranges.len());
        for (contract, &(start, _)) in contract_ranges.iter().enumerate() {
            let row = self.logical_rows[start];
            self.contract_by_key
                .insert((row.chain, row.address_id), contract as u32);
        }
        self.metadata_anchor_rows = vec![[u32::MAX; 8]; contract_ranges.len()];
        self.metadata_anchor_count = metadata_anchor_count;
        for (contract, &(start, end)) in contract_ranges.iter().enumerate() {
            let mut count = 0;
            for row in start..end {
                if self.logical_rows[row].metadata_id.is_none() {
                    continue;
                }
                if count < metadata_anchor_count {
                    self.metadata_anchor_rows[contract][count] = row as u32;
                    count += 1;
                } else {
                    self.logical_rows[row].metadata_id = None;
                    self.quality.non_anchor_metadata += 1;
                }
            }
        }
        self.contract_ranges = contract_ranges;
        Ok(())
    }

    pub fn attach_metadata(
        &mut self,
        chain: ChainId,
        contract_address: &str,
        token_id: &str,
        raw_metadata: Option<&str>,
        source: crate::model::SourceOrder,
    ) -> Result<()> {
        self.attach_prepared_metadata(
            chain,
            contract_address,
            token_id,
            PreparedMetadataInput::from_raw(raw_metadata),
            source,
        )
    }

    pub fn metadata_input_disposition(
        &self,
        chain: ChainId,
        contract_address: &str,
        token_id: &str,
        source: crate::model::SourceOrder,
    ) -> Result<MetadataInputDisposition> {
        Ok(self
            .metadata_input_target(chain, contract_address, token_id, source)?
            .disposition())
    }

    pub(crate) fn metadata_input_target(
        &self,
        chain: ChainId,
        contract_address: &str,
        token_id: &str,
        source: crate::model::SourceOrder,
    ) -> Result<MetadataInputTarget> {
        let raw_address = contract_address.trim();
        let address = if chain.is_evm() {
            ascii_lowercase_if_needed(raw_address)
        } else {
            Cow::Borrowed(raw_address)
        };
        let raw_token = token_id.trim();
        let token = if chain.is_evm() {
            canonical_decimal_digits(raw_token)
                .map(Cow::Borrowed)
                .unwrap_or_else(|| ascii_lowercase_if_needed(raw_token))
        } else {
            Cow::Borrowed(raw_token)
        };
        let (Some(address_id), Some(token_id_id)) = (
            self.addresses.lookup(address.as_ref()),
            self.token_ids.lookup(token.as_ref()),
        ) else {
            return Err(AnalysisError::Input(format!(
                "metadata pass found an unknown NFT at {source:?}"
            )));
        };
        let contract = *self
            .contract_by_key
            .get(&(chain, address_id))
            .ok_or_else(|| AnalysisError::Input(format!("unknown contract at {source:?}")))?
            as usize;
        let (start, end) = self.contract_ranges[contract];
        let token_rank = self.token_rank(chain, token_id_id);
        let row_offset = self.logical_rows[start..end]
            .binary_search_by_key(&token_rank, |row| self.token_rank(chain, row.token_id_id))
            .map_err(|_| {
                AnalysisError::Input(format!(
                    "metadata pass found an unknown token at {source:?}"
                ))
            })?;
        self.metadata_input_target_for_row(contract, start + row_offset, source)
    }

    pub(crate) fn metadata_input_target_by_source(
        &self,
        source: crate::model::SourceOrder,
    ) -> Result<MetadataInputTarget> {
        let row_index = self
            .metadata_source_rows
            .get(usize::from(source.file_ordinal))
            .and_then(|rows| {
                usize::try_from(source.file_row_number)
                    .ok()
                    .and_then(|row| rows.get(row))
            })
            .copied()
            .filter(|row| *row != u32::MAX)
            .ok_or_else(|| {
                AnalysisError::Input(format!(
                    "metadata pass found an unknown source row at {source:?}"
                ))
            })? as usize;
        let contract = self
            .metadata_row_contracts
            .get(row_index)
            .copied()
            .filter(|contract| *contract != u32::MAX)
            .ok_or_else(|| {
                AnalysisError::State(format!("metadata source row has no contract at {source:?}"))
            })? as usize;
        self.metadata_input_target_for_row(contract, row_index, source)
    }

    fn metadata_input_target_for_row(
        &self,
        contract: usize,
        row_index: usize,
        source: crate::model::SourceOrder,
    ) -> Result<MetadataInputTarget> {
        let row = &self.logical_rows[row_index];
        let chain = row.chain;
        let token_rank = self.token_rank(chain, row.token_id_id);
        let disposition = if row
            .metadata_source
            .is_some_and(|existing| existing <= source)
        {
            MetadataInputDisposition::Duplicate
        } else if row.metadata_id.is_some() {
            MetadataInputDisposition::Anchor
        } else {
            let slots = &self.metadata_anchor_rows[contract];
            let count = slots
                .iter()
                .take(self.metadata_anchor_count)
                .take_while(|row| **row != u32::MAX)
                .count();
            if count < self.metadata_anchor_count {
                MetadataInputDisposition::Anchor
            } else {
                let worst = slots[count - 1] as usize;
                if token_rank < self.token_rank(chain, self.logical_rows[worst].token_id_id) {
                    MetadataInputDisposition::Anchor
                } else {
                    MetadataInputDisposition::NonAnchor
                }
            }
        };
        Ok(MetadataInputTarget {
            chain,
            contract,
            row_index,
            token_rank,
            disposition,
        })
    }

    pub(crate) fn release_metadata_source_lookup(&mut self) {
        self.metadata_source_rows.clear();
        self.metadata_source_rows.shrink_to_fit();
        self.metadata_row_contracts.clear();
        self.metadata_row_contracts.shrink_to_fit();
    }

    pub fn attach_prepared_metadata(
        &mut self,
        chain: ChainId,
        contract_address: &str,
        token_id: &str,
        prepared: PreparedMetadataInput,
        source: crate::model::SourceOrder,
    ) -> Result<()> {
        let target = self.metadata_input_target(chain, contract_address, token_id, source)?;
        self.attach_prepared_metadata_target(target, prepared, source)
    }

    pub(crate) fn attach_prepared_metadata_target(
        &mut self,
        target: MetadataInputTarget,
        prepared: PreparedMetadataInput,
        source: crate::model::SourceOrder,
    ) -> Result<()> {
        let MetadataInputTarget {
            chain,
            contract,
            row_index,
            token_rank,
            ..
        } = target;
        let canonical = match prepared {
            PreparedMetadataInput::Missing => return Ok(()),
            PreparedMetadataInput::Oversized => {
                self.quality.oversized_metadata += 1;
                return Ok(());
            }
            PreparedMetadataInput::Invalid => {
                self.quality.invalid_metadata += 1;
                return Ok(());
            }
            PreparedMetadataInput::Ignored => return Ok(()),
            PreparedMetadataInput::NonAnchor => {
                self.quality.non_anchor_metadata += 1;
                return Ok(());
            }
            PreparedMetadataInput::Valid(canonical) => canonical,
        };
        if self.logical_rows[row_index]
            .metadata_source
            .is_some_and(|existing| existing <= source)
        {
            return Ok(());
        }
        if self.logical_rows[row_index].metadata_source.is_some() {
            self.logical_rows[row_index].metadata_source = Some(source);
            if self.logical_rows[row_index].metadata_id.is_some() {
                self.logical_rows[row_index].metadata_id = Some(self.metadata.intern(&canonical));
            }
            return Ok(());
        }
        self.logical_rows[row_index].metadata_source = Some(source);
        let slots = &self.metadata_anchor_rows[contract];
        let count = slots
            .iter()
            .take(self.metadata_anchor_count)
            .take_while(|row| **row != u32::MAX)
            .count();
        if count == self.metadata_anchor_count {
            let worst = slots[count - 1] as usize;
            if token_rank >= self.token_rank(chain, self.logical_rows[worst].token_id_id) {
                self.quality.non_anchor_metadata += 1;
                return Ok(());
            }
        }
        let metadata_id = self.metadata.intern(&canonical);
        let mut position = 0;
        while position < count {
            let candidate = slots[position] as usize;
            if token_rank < self.token_rank(chain, self.logical_rows[candidate].token_id_id) {
                break;
            }
            position += 1;
        }
        let slots = &mut self.metadata_anchor_rows[contract];
        if count == self.metadata_anchor_count {
            let evicted = slots[count - 1] as usize;
            self.logical_rows[evicted].metadata_id = None;
            self.quality.non_anchor_metadata += 1;
        }
        let end = count.min(self.metadata_anchor_count - 1);
        for slot in (position..end).rev() {
            slots[slot + 1] = slots[slot];
        }
        slots[position] = row_index as u32;
        self.logical_rows[row_index].metadata_id = Some(metadata_id);
        Ok(())
    }

    fn finish_prepared(mut self, index_shards: usize) -> Result<ResidentBaseStore> {
        let contract_ranges = std::mem::take(&mut self.contract_ranges);
        self.release_metadata_source_lookup();
        self.contract_by_key.clear();
        self.contract_by_key.shrink_to_fit();
        self.metadata_anchor_rows.clear();
        self.metadata_anchor_rows.shrink_to_fit();
        self.evm_token_rank.clear();
        self.evm_token_rank.shrink_to_fit();
        self.solana_token_rank.clear();
        self.solana_token_rank.shrink_to_fit();

        let mut name_values = ByteInterner::default();
        let mut documents = ByteInterner::default();
        let mut contract_names = Vec::with_capacity(contract_ranges.len());
        let mut contract_anchor_offsets = Vec::with_capacity(contract_ranges.len() + 1);
        let mut contract_anchors = Vec::new();
        let mut contracts = Vec::with_capacity(contract_ranges.len());
        contract_anchor_offsets.push(0);

        for &(start, end) in &contract_ranges {
            let rows = &self.logical_rows[start..end];
            let name = representative_name(rows, &self.names)
                .map(|id| NameValueId(name_values.intern(self.names.get(id))));
            for row in rows.iter().filter(|row| row.metadata_id.is_some()) {
                let canonical = self.metadata.get(row.metadata_id.unwrap());
                contract_anchors.push(MetadataAnchor {
                    token_id_id: TokenIdId(row.token_id_id),
                    metadata_id: MetadataId(documents.intern(canonical)),
                });
            }
            let first = rows[0];
            contract_names.push(name);
            contract_anchor_offsets.push(contract_anchors.len() as u64);
            contracts.push(ContractRecord {
                chain: first.chain,
                address: std::sync::Arc::from(self.addresses.get(first.address_id)),
                nft_count: (end - start) as u64,
                name_value_id: name,
                metadata_profile_id: None,
                name_owner_shard: name
                    .map(|value| crate::model::owner_shard(value.0, index_shards) as u8),
                metadata_owner_shard: None,
            });
        }

        let mut metadata_tokens = ByteInterner::default();
        for contract in 0..contracts.len() {
            let start = contract_anchor_offsets[contract] as usize;
            let end = contract_anchor_offsets[contract + 1] as usize;
            for anchor in &mut contract_anchors[start..end] {
                let raw = self.token_ids.get(anchor.token_id_id.0);
                let normalized = if contracts[contract].chain.is_evm() {
                    canonical_decimal_digits(raw).unwrap_or(raw)
                } else {
                    raw
                };
                anchor.token_id_id = TokenIdId(metadata_tokens.intern(normalized));
            }
        }
        let (profiles, profile_anchors, profile_members, contract_profiles) = build_profiles(
            &contract_anchor_offsets,
            &contract_anchors,
            &mut contracts,
            index_shards,
        )?;
        drop(contract_anchors);
        drop(contract_anchor_offsets);
        let mut nfts = Vec::with_capacity(self.logical_rows.len());
        let mut uri_features = Vec::with_capacity(self.logical_rows.len());
        let mut contract_offsets = Vec::with_capacity(contract_ranges.len() + 1);
        contract_offsets.push(0);
        for (contract_index, &(start, end)) in contract_ranges.iter().enumerate() {
            for row in &self.logical_rows[start..end] {
                nfts.push(NftIdentityRecord {
                    contract_id: ContractId(contract_index as u32),
                    token_id_id: TokenIdId(row.token_id_id),
                });
                uri_features.push(UriFeatureRecord {
                    token_uri: row.token_uri_id.map(UriValueId),
                    image_uri: row.image_uri_id.map(UriValueId),
                });
            }
            contract_offsets.push(nfts.len() as u64);
        }

        Ok(ResidentBaseStore {
            contracts: ContractCatalog { contracts },
            uri_identity: Some(UriNftIdentityStore {
                token_ids: self.token_ids.freeze(),
                nfts,
                contract_offsets,
            }),
            uri_features: Some(UriFeatureStore {
                values: self.uris.freeze(),
                features: uri_features,
            }),
            name_features: Some(NameFeatureStore {
                values: name_values.freeze(),
                contract_names,
            }),
            metadata_features: Some(MetadataFeatureStore {
                anchor_tokens: metadata_tokens.freeze(),
                documents: documents.freeze(),
                anchors: profile_anchors,
                profile_members,
                profiles,
                contract_profiles,
            }),
            quality: self.quality,
        })
    }

    fn token_rank(&self, chain: ChainId, token_id: u32) -> u32 {
        if chain.is_evm() {
            self.evm_token_rank[token_id as usize]
        } else {
            self.solana_token_rank[token_id as usize]
        }
    }
}

fn merge_pool(target: &mut ByteInterner, source: &ByteInterner) -> Vec<u32> {
    target.reserve_from(source);
    (0..source.len() as u32)
        .map(|id| target.intern(source.get(id)))
        .collect()
}

fn accumulate_input_quality(total: &mut InputQuality, value: &InputQuality) {
    macro_rules! add {
        ($($field:ident),+ $(,)?) => {
            $(total.$field = total.$field.saturating_add(value.$field);)+
        };
    }
    add!(
        physical_rows,
        logical_nfts,
        duplicate_rows,
        conflicting_rows,
        empty_names,
        empty_token_uris,
        empty_image_uris,
        invalid_metadata,
        oversized_metadata,
        non_anchor_metadata,
    );
}

fn differs(left: &CompactRow, right: &CompactRow) -> bool {
    left.name_id != right.name_id
        || left.token_uri_id != right.token_uri_id
        || left.image_uri_id != right.image_uri_id
        || left.metadata_id != right.metadata_id
}

fn same_nft(left: &CompactRow, right: &CompactRow) -> bool {
    left.chain == right.chain
        && left.address_id == right.address_id
        && left.token_id_id == right.token_id_id
}

fn nonempty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn meaningful_name(value: Option<&str>) -> Option<&str> {
    let value = nonempty(value)?;
    const NULL_LIKE: [&str; 11] = [
        "none",
        "null",
        "nil",
        "undefined",
        "n/a",
        "na",
        "n.a.",
        "nan",
        "-",
        "--",
        ".",
    ];
    if NULL_LIKE
        .iter()
        .any(|sentinel| value.eq_ignore_ascii_case(sentinel))
        || (value.len() == 1 && value.as_bytes()[0].is_ascii_digit())
    {
        None
    } else {
        Some(value)
    }
}

fn ascii_lowercase_if_needed(value: &str) -> Cow<'_, str> {
    if value.bytes().any(|byte| byte.is_ascii_uppercase()) {
        Cow::Owned(value.to_ascii_lowercase())
    } else {
        Cow::Borrowed(value)
    }
}

fn representative_name(rows: &[CompactRow], names: &ByteInterner) -> Option<u32> {
    let mut counts = AHashMap::<u32, u64>::new();
    for name in rows.iter().filter_map(|row| row.name_id) {
        *counts.entry(name).or_default() += 1;
    }
    counts
        .into_iter()
        .max_by(|(left_id, left_count), (right_id, right_count)| {
            left_count.cmp(right_count).then_with(|| {
                names
                    .get(right_id.to_owned())
                    .as_bytes()
                    .cmp(names.get(left_id.to_owned()).as_bytes())
            })
        })
        .map(|(id, _)| id)
}

type ProfileBuild = (
    Vec<MetadataProfile>,
    Vec<MetadataAnchor>,
    Vec<ContractId>,
    Vec<Option<ProfileId>>,
);

fn build_profiles(
    contract_anchor_offsets: &[u64],
    contract_anchors: &[MetadataAnchor],
    contracts: &mut [ContractRecord],
    index_shards: usize,
) -> Result<ProfileBuild> {
    let mut buckets = AHashMap::<u64, ProfileBucket>::new();
    let mut profiles = Vec::<MetadataProfile>::new();
    let mut profile_anchors = Vec::<MetadataAnchor>::new();
    let mut contract_profiles = vec![None; contracts.len()];
    for contract in 0..contracts.len() {
        let start = contract_anchor_offsets[contract] as usize;
        let end = contract_anchor_offsets[contract + 1] as usize;
        let anchors = &contract_anchors[start..end];
        if anchors.is_empty() {
            continue;
        }
        let hash = profile_hash(anchors);
        let existing = buckets.get(&hash).and_then(|bucket| {
            bucket.find(|profile_id| {
                let profile = &profiles[profile_id.index()];
                let start = profile.anchor_start as usize;
                profile_anchors[start..start + usize::from(profile.anchor_len)] == *anchors
            })
        });
        let profile_id = if let Some(profile_id) = existing {
            profile_id
        } else {
            check_capacity("metadata profiles", profiles.len().saturating_add(1))?;
            let profile_id = ProfileId(profiles.len() as u32);
            profiles.push(MetadataProfile {
                anchor_start: profile_anchors.len() as u64,
                anchor_len: anchors.len() as u8,
                member_start: 0,
                member_len: 0,
            });
            profile_anchors.extend_from_slice(anchors);
            buckets
                .entry(hash)
                .and_modify(|bucket| bucket.push(profile_id))
                .or_insert(ProfileBucket::One(profile_id));
            profile_id
        };
        profiles[profile_id.index()].member_len += 1;
        contract_profiles[contract] = Some(profile_id);
    }
    let mut member_cursor = 0_u64;
    for profile in &mut profiles {
        profile.member_start = member_cursor;
        member_cursor += u64::from(profile.member_len);
    }
    let mut profile_members = vec![ContractId(0); member_cursor as usize];
    let mut cursors = profiles
        .iter()
        .map(|profile| profile.member_start)
        .collect::<Vec<_>>();
    for (contract, &profile_id) in contract_profiles.iter().enumerate() {
        if let Some(profile_id) = profile_id {
            let slot = cursors[profile_id.index()] as usize;
            profile_members[slot] = ContractId(contract as u32);
            cursors[profile_id.index()] += 1;
            let record = &mut contracts[contract];
            record.metadata_profile_id = Some(profile_id);
            record.metadata_owner_shard =
                Some(crate::model::owner_shard(profile_id.0, index_shards) as u8);
        }
    }
    Ok((
        profiles,
        profile_anchors,
        profile_members,
        contract_profiles,
    ))
}

enum ProfileBucket {
    One(ProfileId),
    Many(Vec<ProfileId>),
}

impl ProfileBucket {
    fn find(&self, predicate: impl Fn(ProfileId) -> bool) -> Option<ProfileId> {
        match self {
            Self::One(profile) => predicate(*profile).then_some(*profile),
            Self::Many(profiles) => profiles.iter().copied().find(|&profile| predicate(profile)),
        }
    }

    fn push(&mut self, profile: ProfileId) {
        match self {
            Self::One(first) => *self = Self::Many(vec![*first, profile]),
            Self::Many(profiles) => profiles.push(profile),
        }
    }
}

fn profile_hash(anchors: &[MetadataAnchor]) -> u64 {
    use std::hash::Hasher;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for anchor in anchors {
        hasher.write_u32(anchor.token_id_id.0);
        hasher.write_u32(anchor.metadata_id.0);
    }
    hasher.finish()
}

pub fn token_cmp(chain: ChainId, left: &str, right: &str) -> Ordering {
    if chain.is_evm() {
        let left_number = canonical_decimal_digits(left);
        let right_number = canonical_decimal_digits(right);
        match (left_number, right_number) {
            (Some(left_number), Some(right_number)) => left_number
                .len()
                .cmp(&right_number.len())
                .then_with(|| left_number.as_bytes().cmp(right_number.as_bytes()))
                .then_with(|| left.as_bytes().cmp(right.as_bytes())),
            _ => left.as_bytes().cmp(right.as_bytes()),
        }
    } else {
        left.as_bytes().cmp(right.as_bytes())
    }
}

fn token_ranks(rows: &[CompactRow], tokens: &ByteInterner, evm: bool) -> Vec<u32> {
    let mut ids = rows
        .iter()
        .filter(|row| row.chain.is_evm() == evm)
        .map(|row| row.token_id_id)
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids.dedup();
    let comparison_chain = if evm {
        ChainId::Ethereum
    } else {
        ChainId::Solana
    };
    ids.par_sort_unstable_by(|left, right| {
        token_cmp(comparison_chain, tokens.get(*left), tokens.get(*right))
    });
    let mut ranks = vec![u32::MAX; tokens.len()];
    for (rank, token) in ids.into_iter().enumerate() {
        ranks[token as usize] = rank as u32;
    }
    ranks
}

fn address_ranks(addresses: &ByteInterner) -> Vec<u32> {
    let mut address_ids = (0..addresses.len() as u32).collect::<Vec<_>>();
    address_ids.par_sort_unstable_by(|left, right| {
        addresses
            .get(*left)
            .as_bytes()
            .cmp(addresses.get(*right).as_bytes())
    });
    let mut address_rank = vec![0_u32; address_ids.len()];
    for (rank, address) in address_ids.into_iter().enumerate() {
        address_rank[address as usize] = rank as u32;
    }
    address_rank
}

fn canonical_decimal_digits(value: &str) -> Option<&str> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let trimmed = value.trim_start_matches('0');
    Some(if trimmed.is_empty() { "0" } else { trimmed })
}

fn check_capacity(kind: &'static str, count: usize) -> Result<()> {
    if count > u32::MAX as usize {
        Err(AnalysisError::IdCapacity {
            kind,
            count: count as u64,
        })
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::SourceOrder;

    fn row(name: &str, token: &str, metadata: &str, ordinal: u64) -> InputRow {
        InputRow {
            chain: ChainId::Ethereum,
            contract_address: "0x1".into(),
            token_id: token.into(),
            name_norm: Some(name.into()),
            token_uri_norm: None,
            image_uri_norm: None,
            metadata_json: Some(metadata.into()),
            source_order: SourceOrder {
                file_ordinal: 0,
                file_row_number: ordinal,
            },
        }
    }

    #[test]
    fn representative_name_uses_frequency_then_bytes() {
        let mut builder = ResidentBuilder::default();
        builder.push(row("zeta", "1", r#"{"a":1}"#, 0)).unwrap();
        builder.push(row("alpha", "2", r#"{"a":2}"#, 1)).unwrap();
        let store = builder.finish(8, 128).unwrap();
        let names = store.name_features.unwrap();
        assert_eq!(
            names.values.get(names.contract_names[0].unwrap().0),
            "alpha"
        );
    }

    #[test]
    fn meaningful_name_keeps_multi_digit_values_and_rejects_placeholders() {
        assert_eq!(meaningful_name(Some("42")), Some("42"));
        assert_eq!(meaningful_name(Some("0x12")), Some("0x12"));
        assert_eq!(meaningful_name(Some("7")), None);
        assert_eq!(meaningful_name(Some(" NULL ")), None);
        assert_eq!(meaningful_name(Some("undefined")), None);
        assert_eq!(meaningful_name(Some("--")), None);
    }

    #[test]
    fn duplicate_key_keeps_first_valid_metadata_not_first_row() {
        let mut builder = ResidentBuilder::default();
        let mut invalid = row("same", "1", "not-json", 0);
        invalid.metadata_json = Some("not-json".into());
        builder.push(invalid).unwrap();
        builder
            .push(row("same", "1", r#"{"source":"valid"}"#, 1))
            .unwrap();

        let store = builder.finish(8, 128).unwrap();
        let metadata = store.metadata_features.unwrap();
        let profile = metadata.contract_profiles[0].unwrap();
        let anchors = metadata.profile_anchors(profile);
        assert_eq!(anchors.len(), 1);
        assert_eq!(
            metadata.documents.get(anchors[0].metadata_id.0),
            r#"{"source":"valid"}"#
        );
        assert_eq!(store.quality.duplicate_rows, 1);
        assert_eq!(store.quality.invalid_metadata, 1);
    }

    #[test]
    fn metadata_source_lookup_maps_duplicate_physical_rows_to_one_logical_nft() {
        let mut builder = ResidentBuilder::default();
        let mut first = row("same", "1", r#"{"ignored":true}"#, 0);
        first.metadata_json = None;
        let mut duplicate = row("same", "1", r#"{"ignored":true}"#, 1);
        duplicate.metadata_json = None;
        builder.push(first).unwrap();
        builder.push(duplicate).unwrap();
        builder.prepare_metadata(8).unwrap();

        let first = builder
            .metadata_input_target_by_source(SourceOrder {
                file_ordinal: 0,
                file_row_number: 0,
            })
            .unwrap();
        let duplicate = builder
            .metadata_input_target_by_source(SourceOrder {
                file_ordinal: 0,
                file_row_number: 1,
            })
            .unwrap();
        assert_eq!(first.row_index, duplicate.row_index);
        assert_eq!(first.contract, duplicate.contract);

        builder.release_metadata_source_lookup();
        assert!(builder
            .metadata_input_target_by_source(SourceOrder {
                file_ordinal: 0,
                file_row_number: 0,
            })
            .is_err());
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn metadata_preparation_uses_multiple_numa_lanes_without_changing_output() {
        use crate::platform::WorkerPlacement;

        let placements = [
            WorkerPlacement {
                cpu: 0,
                numa_node: Some(0),
            },
            WorkerPlacement {
                cpu: 1,
                numa_node: Some(0),
            },
            WorkerPlacement {
                cpu: 2,
                numa_node: Some(1),
            },
            WorkerPlacement {
                cpu: 3,
                numa_node: Some(1),
            },
        ];
        let executor = crate::pipeline::CpuExecutor::new_numa_bounded(4, 8, &placements).unwrap();
        let mut builder = ResidentBuilder::default();
        builder.push(row("alpha", "10", r#"{"a":1}"#, 0)).unwrap();
        builder.push(row("alpha", "2", r#"{"a":2}"#, 1)).unwrap();
        let mut solana = row("solana", "mint-z", r#"{"a":3}"#, 2);
        solana.chain = ChainId::Solana;
        solana.contract_address = "collection".into();
        builder.push(solana).unwrap();

        builder.prepare_metadata_numa(8, &executor).unwrap();
        let store = builder.finish(8, 4).unwrap();
        assert_eq!(store.contracts.contracts.len(), 2);
        assert_eq!(store.uri_identity.as_ref().unwrap().nfts.len(), 3);
    }

    #[test]
    fn metadata_pass_keeps_earliest_valid_source_even_when_attached_late() {
        let mut builder = ResidentBuilder::default();
        let mut base = row("same", "1", r#"{"ignored":true}"#, 0);
        base.metadata_json = None;
        builder.push(base).unwrap();
        builder.prepare_metadata(8).unwrap();
        builder
            .attach_metadata(
                ChainId::Ethereum,
                "0x1",
                "1",
                Some(r#"{"source":"late"}"#),
                SourceOrder {
                    file_ordinal: 1,
                    file_row_number: 2,
                },
            )
            .unwrap();
        builder
            .attach_metadata(
                ChainId::Ethereum,
                "0x1",
                "1",
                Some(r#"{"source":"early"}"#),
                SourceOrder {
                    file_ordinal: 1,
                    file_row_number: 1,
                },
            )
            .unwrap();

        let store = builder.finish(8, 128).unwrap();
        let metadata = store.metadata_features.unwrap();
        let profile = metadata.contract_profiles[0].unwrap();
        let anchors = metadata.profile_anchors(profile);
        assert_eq!(anchors.len(), 1);
        assert_eq!(
            metadata.documents.get(anchors[0].metadata_id.0),
            r#"{"source":"early"}"#
        );
    }

    #[test]
    fn metadata_prefilter_preserves_duplicate_and_invalid_quality_categories() {
        let mut builder = ResidentBuilder::default();
        let mut base = row("same", "1", r#"{"ignored":true}"#, 0);
        base.metadata_json = None;
        builder.push(base).unwrap();
        builder.prepare_metadata(8).unwrap();
        let first = SourceOrder {
            file_ordinal: 1,
            file_row_number: 1,
        };
        let later = SourceOrder {
            file_ordinal: 1,
            file_row_number: 2,
        };
        let disposition = builder
            .metadata_input_disposition(ChainId::Ethereum, "0x1", "1", first)
            .unwrap();
        builder
            .attach_prepared_metadata(
                ChainId::Ethereum,
                "0x1",
                "1",
                PreparedMetadataInput::from_raw_for_disposition(
                    Some(r#"{"source":"first"}"#),
                    disposition,
                ),
                first,
            )
            .unwrap();
        let duplicate = builder
            .metadata_input_disposition(ChainId::Ethereum, "0x1", "1", later)
            .unwrap();
        assert_eq!(duplicate, MetadataInputDisposition::Duplicate);
        builder
            .attach_prepared_metadata(
                ChainId::Ethereum,
                "0x1",
                "1",
                PreparedMetadataInput::from_raw_for_disposition(
                    Some(r#"{"source":"later"}"#),
                    duplicate,
                ),
                later,
            )
            .unwrap();
        builder
            .attach_prepared_metadata(
                ChainId::Ethereum,
                "0x1",
                "1",
                PreparedMetadataInput::from_raw_for_disposition(Some("{invalid"), duplicate),
                later,
            )
            .unwrap();

        let store = builder.finish(8, 128).unwrap();
        assert_eq!(store.quality.non_anchor_metadata, 0);
        assert_eq!(store.quality.invalid_metadata, 1);
    }

    #[test]
    fn earliest_source_wins_duplicate_key() {
        let mut builder = ResidentBuilder::default();
        builder.push(row("late", "1", r#"{"a":2}"#, 2)).unwrap();
        builder.push(row("early", "1", r#"{"a":1}"#, 1)).unwrap();
        let store = builder.finish(8, 128).unwrap();
        let names = store.name_features.unwrap();
        assert_eq!(
            names.values.get(names.contract_names[0].unwrap().0),
            "early"
        );
        assert_eq!(store.quality.duplicate_rows, 1);
        assert_eq!(store.quality.conflicting_rows, 1);
    }

    #[test]
    fn shard_merge_preserves_evm_identity_and_source_order() {
        let mut left = ResidentBuilder::default();
        left.push(row("late", "01", r#"{"a":2}"#, 2)).unwrap();
        let mut right = ResidentBuilder::default();
        let mut early = row("early", "1", r#"{"a":1}"#, 1);
        early.contract_address = "0x1".to_ascii_uppercase();
        right.push(early).unwrap();
        left.merge_from(right).unwrap();
        let store = left.finish(8, 128).unwrap();
        assert_eq!(store.quality.logical_nfts, 1);
        assert_eq!(store.quality.duplicate_rows, 1);
        assert_eq!(store.contracts.contracts[0].address.as_ref(), "0x1");
        let names = store.name_features.unwrap();
        assert_eq!(
            names.values.get(names.contract_names[0].unwrap().0),
            "early"
        );
    }
}
