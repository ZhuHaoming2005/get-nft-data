//! Feature SoA writer and Match-facing EncodeBundle / FeatureView.

use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use rayon::prelude::*;
use thiserror::Error;

use crate::cascade::{term_id_signature, PAYLOAD_TERM_SIG_BYTES};
use crate::encode::csr::{
    build_bidirectional_csr_from_iter, write_csr_files_with_progress, BidirectionalCsr, CsrError,
};
use crate::format::{self, ArrayKind, FormatError, MappedU32Array, MappedU64Array, MappedU8Array};

/// One payload's (template term/freq pairs, content term/freq pairs).
pub type PayloadTermLists = (Vec<(u32, u32)>, Vec<(u32, u32)>);
/// Batch of payload term lists for two-pass SoA packing.
pub type PayloadTermListBatch = Vec<PayloadTermLists>;

/// One source document row (stable `source_doc_id` = index in the input slice).
#[derive(Debug, Clone)]
pub struct EncodeSourceRow {
    pub contract_id: u32,
    pub payload_id: u32,
    pub retained_token_ids: Vec<u32>,
}

/// Stable representative mapping for one contract.
#[derive(Debug, Clone, Copy)]
pub struct EncodeContractRow {
    pub contract_id: u32,
    pub chain_id: u32,
    pub source_doc_id: u32,
    pub payload_id: u32,
    pub weight: u64,
}

/// Columnar source catalog: fixed-width ids + retained-token CSR.
#[derive(Debug, Clone, Default)]
pub struct EncodeSourceSoA {
    pub contract_ids: Vec<u32>,
    pub payload_ids: Vec<u32>,
    pub token_offsets: Vec<u64>,
    pub token_ids: Vec<u32>,
}

impl EncodeSourceSoA {
    pub fn with_source_capacity(source_count: usize) -> Self {
        let mut token_offsets = Vec::with_capacity(source_count.saturating_add(1));
        token_offsets.push(0u64);
        Self {
            contract_ids: Vec::with_capacity(source_count),
            payload_ids: Vec::with_capacity(source_count),
            token_offsets,
            token_ids: Vec::new(),
        }
    }

    pub fn source_count(&self) -> usize {
        self.contract_ids.len()
    }

    pub fn tokens_of(&self, source: usize) -> &[u32] {
        csr_u32(&self.token_offsets, &self.token_ids, source)
    }

    pub fn push_source(
        &mut self,
        contract_id: u32,
        payload_id: u32,
        tokens: &[u32],
    ) -> Result<(), FeatureSoaError> {
        self.contract_ids.push(contract_id);
        self.payload_ids.push(payload_id);
        self.token_ids.extend_from_slice(tokens);
        let next = self
            .token_offsets
            .last()
            .copied()
            .unwrap_or(0u64)
            .checked_add(tokens.len() as u64)
            .ok_or(FeatureSoaError::LengthOverflow)?;
        self.token_offsets.push(next);
        Ok(())
    }

    pub fn from_rows(rows: &[EncodeSourceRow]) -> Result<Self, FeatureSoaError> {
        let mut soa = Self::with_source_capacity(rows.len());
        for row in rows {
            soa.push_source(row.contract_id, row.payload_id, &row.retained_token_ids)?;
        }
        Ok(soa)
    }
}

/// Columnar contract identity / weight table (input order; densified on write).
#[derive(Debug, Clone, Default)]
pub struct EncodeContractSoA {
    pub contract_ids: Vec<u32>,
    pub chain_ids: Vec<u32>,
    pub source_doc_ids: Vec<u32>,
    pub payload_ids: Vec<u32>,
    pub weights: Vec<u64>,
}

impl EncodeContractSoA {
    pub fn with_contract_capacity(contract_count: usize) -> Self {
        Self {
            contract_ids: Vec::with_capacity(contract_count),
            chain_ids: Vec::with_capacity(contract_count),
            source_doc_ids: Vec::with_capacity(contract_count),
            payload_ids: Vec::with_capacity(contract_count),
            weights: Vec::with_capacity(contract_count),
        }
    }

    pub fn contract_count(&self) -> usize {
        self.contract_ids.len()
    }

    pub fn push_contract(
        &mut self,
        contract_id: u32,
        chain_id: u32,
        source_doc_id: u32,
        payload_id: u32,
        weight: u64,
    ) {
        self.contract_ids.push(contract_id);
        self.chain_ids.push(chain_id);
        self.source_doc_ids.push(source_doc_id);
        self.payload_ids.push(payload_id);
        self.weights.push(weight);
    }

    pub fn from_rows(rows: &[EncodeContractRow]) -> Self {
        let mut soa = Self::with_contract_capacity(rows.len());
        for row in rows {
            soa.push_contract(
                row.contract_id,
                row.chain_id,
                row.source_doc_id,
                row.payload_id,
                row.weight,
            );
        }
        soa
    }
}

/// Candidate-work dimensions frozen while the CSR is already resident.  The
/// controller persists these in the ready markers so ETA cohorting never has
/// to reopen and rescan large artifacts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EncodePersistStats {
    pub token_pair_work: u64,
    pub max_token_members: u64,
    pub fallback_pair_work: u64,
    pub max_fallback_members: u64,
}

/// Pre-tokenized template/content term lists for one payload (`term_id`, `freq`).
#[derive(Debug, Clone, Default)]
pub struct EncodePayloadRow {
    pub template_terms: Vec<(u32, u32)>,
    pub content_terms: Vec<(u32, u32)>,
}

/// Columnar payload term storage used on the Encode hot path and for Match persist.
#[derive(Debug, Clone, Default)]
pub struct PayloadTermSoA {
    pub template_offsets: Vec<u64>,
    pub template_terms: Vec<u32>,
    pub template_freqs: Vec<u32>,
    pub content_offsets: Vec<u64>,
    pub content_terms: Vec<u32>,
    pub content_freqs: Vec<u32>,
}

impl PayloadTermSoA {
    pub fn with_payload_capacity(payload_count: usize) -> Self {
        let mut template_offsets = Vec::with_capacity(payload_count.saturating_add(1));
        let mut content_offsets = Vec::with_capacity(payload_count.saturating_add(1));
        template_offsets.push(0);
        content_offsets.push(0);
        Self {
            template_offsets,
            template_terms: Vec::new(),
            template_freqs: Vec::new(),
            content_offsets,
            content_terms: Vec::new(),
            content_freqs: Vec::new(),
        }
    }

    pub fn payload_count(&self) -> usize {
        self.template_offsets.len().saturating_sub(1)
    }

    pub fn template_term_ids(&self, payload: usize) -> &[u32] {
        csr_u32(&self.template_offsets, &self.template_terms, payload)
    }

    pub fn template_freqs(&self, payload: usize) -> &[u32] {
        csr_u32(&self.template_offsets, &self.template_freqs, payload)
    }

    pub fn content_term_ids(&self, payload: usize) -> &[u32] {
        csr_u32(&self.content_offsets, &self.content_terms, payload)
    }

    pub fn content_freqs(&self, payload: usize) -> &[u32] {
        csr_u32(&self.content_offsets, &self.content_freqs, payload)
    }

    pub fn content_token_length(&self, payload: usize) -> u32 {
        self.content_freqs(payload)
            .iter()
            .fold(0u32, |total, frequency| total.saturating_add(*frequency))
    }

    pub fn payload_eq(&self, left: usize, right: usize) -> bool {
        self.template_term_ids(left) == self.template_term_ids(right)
            && self.template_freqs(left) == self.template_freqs(right)
            && self.content_term_ids(left) == self.content_term_ids(right)
            && self.content_freqs(left) == self.content_freqs(right)
    }

    pub fn hash_payload(&self, payload: usize, hasher: &mut impl std::hash::Hasher) {
        use std::hash::Hash;
        self.template_term_ids(payload).hash(hasher);
        self.template_freqs(payload).hash(hasher);
        self.content_term_ids(payload).hash(hasher);
        self.content_freqs(payload).hash(hasher);
    }

    pub fn materialize_template_pairs(&self, payload: usize) -> Vec<(u32, u32)> {
        self.template_term_ids(payload)
            .iter()
            .zip(self.template_freqs(payload))
            .map(|(&term, &frequency)| (term, frequency))
            .collect()
    }

    pub fn materialize_content_pairs(&self, payload: usize) -> Vec<(u32, u32)> {
        self.content_term_ids(payload)
            .iter()
            .zip(self.content_freqs(payload))
            .map(|(&term, &frequency)| (term, frequency))
            .collect()
    }

    pub fn from_rows(rows: &[EncodePayloadRow]) -> Result<Self, FeatureSoaError> {
        let lists = rows
            .iter()
            .map(|row| (row.template_terms.clone(), row.content_terms.clone()))
            .collect::<PayloadTermListBatch>();
        Self::from_term_lists_parallel(lists)
    }

    /// Pack owned term lists directly into the final flat arrays.
    ///
    /// Moving the pairs avoids the four full-size atomic staging arrays that
    /// the former parallel fill required. This path is memory-bandwidth bound,
    /// so a single pass over already-owned vectors is also cheaper in practice.
    pub fn from_term_lists_owned(lists: PayloadTermListBatch) -> Result<Self, FeatureSoaError> {
        let payload_count = lists.len();
        crate::identity::checked_u32_identity("payload rows", payload_count as u64)?;
        let mut template_offsets = Vec::with_capacity(payload_count.saturating_add(1));
        let mut content_offsets = Vec::with_capacity(payload_count.saturating_add(1));
        template_offsets.push(0);
        content_offsets.push(0);
        let (template_len, content_len) = lists.iter().try_fold(
            (0usize, 0usize),
            |(template_len, content_len), (template, content)| {
                Ok::<_, FeatureSoaError>((
                    template_len
                        .checked_add(template.len())
                        .ok_or(FeatureSoaError::LengthOverflow)?,
                    content_len
                        .checked_add(content.len())
                        .ok_or(FeatureSoaError::LengthOverflow)?,
                ))
            },
        )?;
        let mut template_terms = Vec::with_capacity(template_len);
        let mut template_freqs = Vec::with_capacity(template_len);
        let mut content_terms = Vec::with_capacity(content_len);
        let mut content_freqs = Vec::with_capacity(content_len);
        for (template, content) in lists {
            for (term, frequency) in template {
                template_terms.push(term);
                template_freqs.push(frequency);
            }
            template_offsets.push(template_terms.len() as u64);
            for (term, frequency) in content {
                content_terms.push(term);
                content_freqs.push(frequency);
            }
            content_offsets.push(content_terms.len() as u64);
        }
        Ok(Self {
            template_offsets,
            template_terms,
            template_freqs,
            content_offsets,
            content_terms,
            content_freqs,
        })
    }

    /// Compatibility entry point; owned packing no longer needs atomic staging.
    pub fn from_term_lists_parallel(lists: PayloadTermListBatch) -> Result<Self, FeatureSoaError> {
        Self::from_term_lists_owned(lists)
    }

    pub fn append_soa(&mut self, other: &Self) -> Result<(), FeatureSoaError> {
        let template_base = *self.template_offsets.last().unwrap_or(&0);
        let content_base = *self.content_offsets.last().unwrap_or(&0);
        for &offset in other.template_offsets.iter().skip(1) {
            self.template_offsets.push(
                template_base
                    .checked_add(offset)
                    .ok_or(FeatureSoaError::LengthOverflow)?,
            );
        }
        for &offset in other.content_offsets.iter().skip(1) {
            self.content_offsets.push(
                content_base
                    .checked_add(offset)
                    .ok_or(FeatureSoaError::LengthOverflow)?,
            );
        }
        self.template_terms.extend_from_slice(&other.template_terms);
        self.template_freqs.extend_from_slice(&other.template_freqs);
        self.content_terms.extend_from_slice(&other.content_terms);
        self.content_freqs.extend_from_slice(&other.content_freqs);
        Ok(())
    }
}

/// Flat fallback-atom membership CSR (plus representative payload per atom).
#[derive(Debug, Clone, Default)]
pub struct FallbackAtomCsr {
    pub offsets: Vec<u64>,
    pub members: Vec<u32>,
    pub atom_payloads: Vec<u32>,
}

impl FallbackAtomCsr {
    pub fn atom_count(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    pub fn members_of(&self, atom: usize) -> &[u32] {
        csr_u32(&self.offsets, &self.members, atom)
    }

    pub fn from_groups(groups: &[Vec<u32>]) -> Result<Self, FeatureSoaError> {
        let mut offsets = Vec::with_capacity(groups.len().saturating_add(1));
        offsets.push(0u64);
        let mut members = Vec::new();
        for group in groups {
            members.extend_from_slice(group);
            offsets.push(
                offsets
                    .last()
                    .copied()
                    .unwrap_or(0u64)
                    .checked_add(group.len() as u64)
                    .ok_or(FeatureSoaError::LengthOverflow)?,
            );
        }
        Ok(Self {
            offsets,
            members,
            atom_payloads: Vec::new(),
        })
    }
}

fn csr_u32<'a>(offsets: &[u64], values: &'a [u32], row: usize) -> &'a [u32] {
    if row + 1 >= offsets.len() {
        return &[];
    }
    let start = offsets[row] as usize;
    let end = offsets[row + 1] as usize;
    &values[start..end]
}

#[derive(Debug, Error)]
pub enum FeatureSoaError {
    #[error(transparent)]
    Identity(#[from] crate::identity::IdentityOverflow),
    #[error(transparent)]
    Format(#[from] FormatError),
    #[error(transparent)]
    Csr(#[from] CsrError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("payload_id {payload_id} out of range (payloads len {len})")]
    PayloadIdOutOfRange { payload_id: u32, len: usize },
    #[error("missing feature file: {0}")]
    MissingFile(String),
    #[error("contract_id {contract_id} is missing from dense 0..{max_contract_id} mapping")]
    MissingContract {
        contract_id: u32,
        max_contract_id: u32,
    },
    #[error("source_doc_id {source_doc_id} out of range (sources len {len})")]
    SourceIdOutOfRange { source_doc_id: u32, len: usize },
    #[error("feature column length overflow")]
    LengthOverflow,
}

struct PayloadProgress<'a, F> {
    total: u64,
    completed: u64,
    callback: &'a mut F,
}

impl<'a, F: FnMut(u64, u64)> PayloadProgress<'a, F> {
    fn new(total: u64, callback: &'a mut F) -> Self {
        callback(0, total);
        Self {
            total,
            completed: 0,
            callback,
        }
    }

    fn write_u32(
        &mut self,
        path: &Path,
        kind: ArrayKind,
        count: u64,
        values: impl IntoIterator<Item = u32>,
    ) -> Result<(), FormatError> {
        let base = self.completed;
        let total = self.total;
        let callback = &mut self.callback;
        format::write_u32_iter_with_progress(path, kind, count, values, |local| {
            callback(base.saturating_add(local), total);
        })?;
        self.completed = base.saturating_add(count.saturating_mul(4));
        Ok(())
    }

    fn write_u64(
        &mut self,
        path: &Path,
        kind: ArrayKind,
        count: u64,
        values: impl IntoIterator<Item = u64>,
    ) -> Result<(), FormatError> {
        let base = self.completed;
        let total = self.total;
        let callback = &mut self.callback;
        format::write_u64_iter_with_progress(path, kind, count, values, |local| {
            callback(base.saturating_add(local), total);
        })?;
        self.completed = base.saturating_add(count.saturating_mul(8));
        Ok(())
    }

    fn write_f64(
        &mut self,
        path: &Path,
        kind: ArrayKind,
        count: u64,
        values: impl IntoIterator<Item = f64>,
    ) -> Result<(), FormatError> {
        let base = self.completed;
        let total = self.total;
        let callback = &mut self.callback;
        format::write_f64_iter_with_progress(path, kind, count, values, |local| {
            callback(base.saturating_add(local), total);
        })?;
        self.completed = base.saturating_add(count.saturating_mul(8));
        Ok(())
    }

    fn write_u8_iter(
        &mut self,
        path: &Path,
        count: u64,
        values: impl IntoIterator<Item = u8>,
    ) -> Result<(), FormatError> {
        let base = self.completed;
        let total = self.total;
        let callback = &mut self.callback;
        format::write_u8_iter_with_progress(path, count, values, |local| {
            callback(base.saturating_add(local), total);
        })?;
        self.completed = base.saturating_add(count);
        Ok(())
    }

    fn write_csr(&mut self, bundle_dir: &Path, csr: &BidirectionalCsr) -> Result<(), CsrError> {
        let base = self.completed;
        let total = self.total;
        let callback = &mut self.callback;
        write_csr_files_with_progress(bundle_dir, csr, |local| {
            callback(base.saturating_add(local), total);
        })?;
        self.completed = base.saturating_add(csr_payload_bytes(csr));
        Ok(())
    }

    fn finish(&mut self) {
        debug_assert_eq!(self.completed, self.total);
        (self.callback)(self.total, self.total);
    }
}

/// Write feature SoA + bidirectional CSR under `bundle_dir` (`encode-<rev>/`).
pub fn write_encode_artifacts(
    bundle_dir: &Path,
    sources: &[EncodeSourceRow],
    payloads: &[EncodePayloadRow],
) -> Result<(), FeatureSoaError> {
    crate::identity::checked_u32_identity("source rows", sources.len() as u64)?;
    let mut contracts = std::collections::BTreeMap::<u32, EncodeContractRow>::new();
    for (source_doc_id, source) in sources.iter().enumerate() {
        contracts
            .entry(source.contract_id)
            .or_insert(EncodeContractRow {
                contract_id: source.contract_id,
                chain_id: 0,
                source_doc_id: crate::identity::checked_u32_identity(
                    "source_doc_id",
                    source_doc_id as u64,
                )?,
                payload_id: source.payload_id,
                weight: 1,
            });
    }
    write_encode_artifacts_with_contracts(
        bundle_dir,
        sources,
        payloads,
        &contracts.into_values().collect::<Vec<_>>(),
    )
}

/// Write feature SoA using explicit stable contract/source/payload identities.
pub fn write_encode_artifacts_with_contracts(
    bundle_dir: &Path,
    sources: &[EncodeSourceRow],
    payloads: &[EncodePayloadRow],
    contracts: &[EncodeContractRow],
) -> Result<(), FeatureSoaError> {
    let fallback_atoms = contracts
        .iter()
        .map(|contract| vec![contract.contract_id])
        .collect::<Vec<_>>();
    write_encode_artifacts_with_contracts_and_atoms(
        bundle_dir,
        sources,
        payloads,
        contracts,
        &fallback_atoms,
    )
}

pub fn write_encode_artifacts_with_contracts_and_atoms(
    bundle_dir: &Path,
    sources: &[EncodeSourceRow],
    payloads: &[EncodePayloadRow],
    contracts: &[EncodeContractRow],
    fallback_atoms: &[Vec<u32>],
) -> Result<(), FeatureSoaError> {
    write_encode_artifacts_with_contracts_and_atoms_with_progress(
        bundle_dir,
        sources,
        payloads,
        contracts,
        fallback_atoms,
        |_, _| {},
    )
    .map(|_| ())
}

/// Persist all Match-facing feature columns and report exact cumulative
/// payload bytes. The callback is invoked before writing, at bounded chunks,
/// and at the exact terminal total.
pub fn write_encode_artifacts_with_contracts_and_atoms_with_progress(
    bundle_dir: &Path,
    sources: &[EncodeSourceRow],
    payloads: &[EncodePayloadRow],
    contracts: &[EncodeContractRow],
    fallback_atoms: &[Vec<u32>],
    on_progress: impl FnMut(u64, u64),
) -> Result<EncodePersistStats, FeatureSoaError> {
    let payload_soa = PayloadTermSoA::from_rows(payloads)?;
    let mut atom_csr = FallbackAtomCsr::from_groups(fallback_atoms)?;
    atom_csr.atom_payloads = fallback_atoms
        .iter()
        .map(|members| members.first().copied().unwrap_or(0))
        .collect();
    write_encode_artifacts_soa_with_progress(
        bundle_dir,
        &EncodeSourceSoA::from_rows(sources)?,
        &payload_soa,
        &EncodeContractSoA::from_rows(contracts),
        &atom_csr,
        on_progress,
    )
}

/// Persist Match-facing columns from already-columnar payload/atom SoA.
pub fn write_encode_artifacts_soa_with_progress(
    bundle_dir: &Path,
    sources: &EncodeSourceSoA,
    payloads: &PayloadTermSoA,
    contracts: &EncodeContractSoA,
    fallback_atoms: &FallbackAtomCsr,
    mut on_progress: impl FnMut(u64, u64),
) -> Result<EncodePersistStats, FeatureSoaError> {
    fs::create_dir_all(bundle_dir)?;

    crate::identity::checked_u32_identity("source rows", sources.source_count() as u64)?;
    crate::identity::checked_u32_identity("payload rows", payloads.payload_count() as u64)?;
    crate::identity::checked_u32_identity("contract rows", contracts.contract_count() as u64)?;
    crate::identity::checked_u32_identity("fallback atoms", fallback_atoms.atom_count() as u64)?;

    for &payload_id in &sources.payload_ids {
        if payload_id as usize >= payloads.payload_count() {
            return Err(FeatureSoaError::PayloadIdOutOfRange {
                payload_id,
                len: payloads.payload_count(),
            });
        }
    }

    let csr =
        build_bidirectional_csr_from_iter((0..sources.source_count()).map(|source_doc_id| {
            (
                crate::identity::checked_u32_identity("source_doc_id", source_doc_id as u64)
                    .expect("source cardinality checked"),
                sources.contract_ids[source_doc_id],
                sources.tokens_of(source_doc_id),
            )
        }))?;
    let (token_pair_work, max_token_members) = candidate_group_stats(
        csr.token_member_offsets
            .windows(2)
            .map(|window| window[1].saturating_sub(window[0])),
    );
    let (fallback_pair_work, max_fallback_members) = candidate_group_stats(
        (0..fallback_atoms.atom_count()).map(|atom| fallback_atoms.members_of(atom).len() as u64),
    );
    let total = feature_payload_bytes_soa(sources, payloads, contracts, fallback_atoms, &csr)?;
    let mut progress = PayloadProgress::new(total, &mut on_progress);

    progress.write_u32(
        &bundle_dir.join("source_to_payload.u32"),
        ArrayKind::U32,
        sources.source_count() as u64,
        sources.payload_ids.iter().copied(),
    )?;

    write_term_soa_columns(
        &mut progress,
        bundle_dir,
        "payload_template",
        &payloads.template_offsets,
        &payloads.template_terms,
        &payloads.template_freqs,
    )?;
    write_term_soa_columns(
        &mut progress,
        bundle_dir,
        "payload_content",
        &payloads.content_offsets,
        &payloads.content_terms,
        &payloads.content_freqs,
    )?;
    write_payload_term_signatures_soa(&mut progress, bundle_dir, payloads)?;

    progress.write_csr(bundle_dir, &csr)?;

    write_identity_and_placeholder_columns_soa(
        &mut progress,
        bundle_dir,
        sources,
        payloads,
        contracts,
        fallback_atoms,
    )?;

    progress.finish();

    Ok(EncodePersistStats {
        token_pair_work,
        max_token_members,
        fallback_pair_work,
        max_fallback_members,
    })
}

/// Conservative physical-space admission for the complete encoded feature bundle.
///
/// This uses the frozen column cardinalities rather than raw JSON bytes. The
/// latter can exceed the fixed-width Match representation by more than an order
/// of magnitude and must not be used as a durable artifact-size estimate.
pub fn encode_artifact_upper_bound_soa(
    sources: &EncodeSourceSoA,
    payloads: &PayloadTermSoA,
    contracts: &EncodeContractSoA,
    fallback_atoms: &FallbackAtomCsr,
) -> Result<u64, FeatureSoaError> {
    const FILE_AND_MANIFEST_ALLOWANCE: u64 = 64 * 1024 * 1024;
    let contract_count = contracts
        .contract_ids
        .iter()
        .chain(&sources.contract_ids)
        .map(|contract_id| u64::from(*contract_id) + 1)
        .max()
        .unwrap_or(0);
    let token_count = sources
        .token_ids
        .iter()
        .map(|token_id| u64::from(*token_id) + 1)
        .max()
        .unwrap_or(0);
    let memberships = sources.token_ids.len() as u64;
    let csr_payload_bytes = [
        (contract_count + 1, 8u64),
        (memberships, 4),
        (token_count + 1, 8),
        (memberships, 4),
        (memberships, 4),
    ]
    .into_iter()
    .try_fold(0u64, |total, (count, width)| {
        total
            .checked_add(
                count
                    .checked_mul(width)
                    .ok_or(FeatureSoaError::LengthOverflow)?,
            )
            .ok_or(FeatureSoaError::LengthOverflow)
    })?;
    feature_payload_bytes_from_csr_bytes(
        sources,
        payloads,
        contracts,
        fallback_atoms,
        csr_payload_bytes,
    )?
    .checked_add(FILE_AND_MANIFEST_ALLOWANCE)
    .ok_or(FeatureSoaError::LengthOverflow)
}

fn candidate_group_stats(sizes: impl Iterator<Item = u64>) -> (u64, u64) {
    sizes.fold((0u64, 0u64), |(pair_work, maximum), size| {
        (
            pair_work.saturating_add(size.saturating_mul(size.saturating_sub(1)) / 2),
            maximum.max(size),
        )
    })
}

fn feature_payload_bytes_soa(
    sources: &EncodeSourceSoA,
    payloads: &PayloadTermSoA,
    contracts: &EncodeContractSoA,
    fallback_atoms: &FallbackAtomCsr,
    csr: &BidirectionalCsr,
) -> Result<u64, FeatureSoaError> {
    feature_payload_bytes_from_csr_bytes(
        sources,
        payloads,
        contracts,
        fallback_atoms,
        checked_csr_payload_bytes(csr)?,
    )
}

fn feature_payload_bytes_from_csr_bytes(
    sources: &EncodeSourceSoA,
    payloads: &PayloadTermSoA,
    contracts: &EncodeContractSoA,
    fallback_atoms: &FallbackAtomCsr,
    csr_payload_bytes: u64,
) -> Result<u64, FeatureSoaError> {
    let template_terms = payloads.template_terms.len() as u64;
    let content_terms = payloads.content_terms.len() as u64;
    let fallback_members = fallback_atoms.members.len() as u64;
    let contract_count = contracts
        .contract_ids
        .iter()
        .map(|contract_id| u64::from(*contract_id) + 1)
        .max()
        .unwrap_or(0);
    let payload_count = payloads.payload_count() as u64;
    let signature_bytes = payload_count
        .checked_mul(PAYLOAD_TERM_SIG_BYTES as u64)
        .and_then(|bytes| bytes.checked_mul(2))
        .ok_or(FeatureSoaError::LengthOverflow)?;
    let columns = [
        (sources.source_count() as u64, 4),
        (payload_count + 1, 8),
        (template_terms, 8),
        (payload_count + 1, 8),
        (content_terms, 8),
        (signature_bytes, 1),
        (payload_count, 4),
        (payload_count, 8),
        (payload_count + 1, 8),
        (template_terms, 8),
        (contract_count, 20),
        (fallback_atoms.atom_count() as u64 + 1, 8),
        (fallback_members, 4),
    ];
    columns
        .into_iter()
        .try_fold(csr_payload_bytes, |total, (count, width)| {
            total
                .checked_add(
                    count
                        .checked_mul(width)
                        .ok_or(FeatureSoaError::LengthOverflow)?,
                )
                .ok_or(FeatureSoaError::LengthOverflow)
        })
}

fn checked_csr_payload_bytes(csr: &BidirectionalCsr) -> Result<u64, FeatureSoaError> {
    [
        (csr.contract_token_offsets.len() as u64, 8),
        (csr.contract_tokens.len() as u64, 4),
        (csr.token_member_offsets.len() as u64, 8),
        (csr.token_member_contracts.len() as u64, 4),
        (csr.token_member_sources.len() as u64, 4),
    ]
    .into_iter()
    .try_fold(0u64, |total, (count, width)| {
        total
            .checked_add(
                count
                    .checked_mul(width)
                    .ok_or(FeatureSoaError::LengthOverflow)?,
            )
            .ok_or(FeatureSoaError::LengthOverflow)
    })
}

fn csr_payload_bytes(csr: &BidirectionalCsr) -> u64 {
    checked_csr_payload_bytes(csr).unwrap_or(u64::MAX)
}

/// Write Match-facing scoring columns before BM25/fallback fill-in.
fn write_identity_and_placeholder_columns_soa(
    progress: &mut PayloadProgress<'_, impl FnMut(u64, u64)>,
    bundle_dir: &Path,
    sources: &EncodeSourceSoA,
    payloads: &PayloadTermSoA,
    contracts: &EncodeContractSoA,
    fallback_atoms: &FallbackAtomCsr,
) -> Result<(), FeatureSoaError> {
    progress.write_u32(
        &bundle_dir.join("payload_lengths.u32"),
        ArrayKind::U32,
        payloads.payload_count() as u64,
        (0..payloads.payload_count()).map(|payload| payloads.content_token_length(payload)),
    )?;
    write_template_scoring_columns_soa(progress, bundle_dir, payloads, contracts)?;
    let max_contract_id = contracts.contract_ids.iter().copied().max();
    let contract_count = max_contract_id.map_or(0, |id| id as usize + 1);
    let mut dense_index = vec![None; contract_count];
    for (index, &contract_id) in contracts.contract_ids.iter().enumerate() {
        let source_doc_id = contracts.source_doc_ids[index];
        if source_doc_id as usize >= sources.source_count() {
            return Err(FeatureSoaError::SourceIdOutOfRange {
                source_doc_id,
                len: sources.source_count(),
            });
        }
        let payload_id = contracts.payload_ids[index];
        if payload_id as usize >= payloads.payload_count() {
            return Err(FeatureSoaError::PayloadIdOutOfRange {
                payload_id,
                len: payloads.payload_count(),
            });
        }
        dense_index[contract_id as usize] = Some(index);
    }
    if let Some(max_contract_id) = max_contract_id {
        for (contract_id, index) in dense_index.iter().enumerate() {
            if index.is_none() {
                return Err(FeatureSoaError::MissingContract {
                    contract_id: contract_id as u32,
                    max_contract_id,
                });
            }
        }
    }
    progress.write_u32(
        &bundle_dir.join("contract_source.u32"),
        ArrayKind::U32,
        dense_index.len() as u64,
        dense_index
            .iter()
            .map(|index| contracts.source_doc_ids[index.expect("dense contracts validated")]),
    )?;
    progress.write_u32(
        &bundle_dir.join("contract_chain.u32"),
        ArrayKind::U32,
        dense_index.len() as u64,
        dense_index
            .iter()
            .map(|index| contracts.chain_ids[index.expect("dense contracts validated")]),
    )?;
    progress.write_u32(
        &bundle_dir.join("contract_payload.u32"),
        ArrayKind::U32,
        dense_index.len() as u64,
        dense_index
            .iter()
            .map(|index| contracts.payload_ids[index.expect("dense contracts validated")]),
    )?;
    progress.write_u64(
        &bundle_dir.join("contract_weight.u64"),
        ArrayKind::U64,
        dense_index.len() as u64,
        dense_index
            .iter()
            .map(|index| contracts.weights[index.expect("dense contracts validated")]),
    )?;
    progress.write_u64(
        &bundle_dir.join("fallback_atoms_offsets.u64"),
        ArrayKind::U64,
        fallback_atoms.offsets.len() as u64,
        fallback_atoms.offsets.iter().copied(),
    )?;
    progress.write_u32(
        &bundle_dir.join("fallback_atoms_members.u32"),
        ArrayKind::U32,
        fallback_atoms.members.len() as u64,
        fallback_atoms.members.iter().copied(),
    )?;
    Ok(())
}

fn write_template_scoring_columns_soa(
    progress: &mut PayloadProgress<'_, impl FnMut(u64, u64)>,
    bundle_dir: &Path,
    payloads: &PayloadTermSoA,
    contracts: &EncodeContractSoA,
) -> Result<(), FeatureSoaError> {
    let prepared = prepare_template_scoring_soa(payloads, contracts)?;
    let PreparedTemplateScoring {
        payload_document_weights,
        total_docs,
        doc_freqs,
        query_denominators,
        prepared_weights,
    } = prepared;
    debug_assert_eq!(payload_document_weights.len(), payloads.payload_count());
    debug_assert!(total_docs >= payload_document_weights.iter().copied().max().unwrap_or(0));
    debug_assert!(doc_freqs.iter().all(|&frequency| frequency <= total_docs));
    drop(payload_document_weights);
    drop(doc_freqs);
    let prepared_weight_count = prepared_weights.len() as u64;
    progress.write_f64(
        &bundle_dir.join("query_denominators.f64"),
        ArrayKind::F64,
        payloads.payload_count() as u64,
        query_denominators,
    )?;
    progress.write_u64(
        &bundle_dir.join("prepared_weight_offsets.u64"),
        ArrayKind::U64,
        payloads.template_offsets.len() as u64,
        payloads.template_offsets.iter().copied(),
    )?;
    progress.write_f64(
        &bundle_dir.join("prepared_weights.f64"),
        ArrayKind::F64,
        prepared_weight_count,
        prepared_weights,
    )?;
    Ok(())
}

struct PreparedTemplateScoring {
    payload_document_weights: Vec<u64>,
    total_docs: u64,
    doc_freqs: Vec<u64>,
    query_denominators: Vec<f64>,
    prepared_weights: Vec<f64>,
}

#[cfg(test)]
fn prepare_template_scoring(
    payloads: &[EncodePayloadRow],
    contracts: &[EncodeContractRow],
) -> Result<PreparedTemplateScoring, FeatureSoaError> {
    prepare_template_scoring_soa(
        &PayloadTermSoA::from_rows(payloads)?,
        &EncodeContractSoA::from_rows(contracts),
    )
}

fn prepare_template_scoring_soa(
    payloads: &PayloadTermSoA,
    contracts: &EncodeContractSoA,
) -> Result<PreparedTemplateScoring, FeatureSoaError> {
    const K1: f64 = 1.2;
    const B: f64 = 0.75;
    let token_count = payloads
        .template_terms
        .iter()
        .map(|term| *term as usize + 1)
        .max()
        .unwrap_or(0);
    let payload_document_weights = (0..payloads.payload_count())
        .map(|_| AtomicU64::new(0))
        .collect::<Vec<_>>();
    (0..contracts.contract_count())
        .into_par_iter()
        .for_each(|index| {
            let payload_id = contracts.payload_ids[index] as usize;
            let contract_weight = contracts.weights[index];
            let weight = &payload_document_weights[payload_id];
            let _ = weight.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_add(contract_weight))
            });
        });
    let payload_document_weights = payload_document_weights
        .into_iter()
        .map(AtomicU64::into_inner)
        .collect::<Vec<_>>();
    let total_docs = payload_document_weights
        .iter()
        .copied()
        .fold(0u64, u64::saturating_add);
    let doc_freqs = (0..token_count)
        .map(|_| AtomicU64::new(0))
        .collect::<Vec<_>>();
    let payload_lengths = (0..payloads.payload_count())
        .into_par_iter()
        .map(|payload_id| {
            payloads
                .template_freqs(payload_id)
                .iter()
                .map(|frequency| u64::from(*frequency))
                .sum::<u64>()
        })
        .collect::<Vec<_>>();
    let total_terms = (0..payloads.payload_count())
        .into_par_iter()
        .map(|payload_id| {
            let weight = payload_document_weights[payload_id];
            if weight != 0 {
                for &term in payloads.template_term_ids(payload_id) {
                    let frequency = &doc_freqs[term as usize];
                    let _ =
                        frequency.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                            Some(current.saturating_add(weight))
                        });
                }
            }
            u128::from(payload_lengths[payload_id]).saturating_mul(u128::from(weight))
        })
        .reduce(|| 0u128, u128::saturating_add);
    let doc_freqs = doc_freqs
        .into_iter()
        .map(AtomicU64::into_inner)
        .collect::<Vec<_>>();
    let avg_doc_len = if total_docs == 0 {
        0.0
    } else {
        total_terms as f64 / total_docs as f64
    };
    let payload_norms = (0..payloads.payload_count())
        .into_par_iter()
        .map(|payload_id| {
            let len = payload_lengths[payload_id];
            if avg_doc_len > 0.0 {
                K1 * (1.0 - B + B * len as f64 / avg_doc_len)
            } else {
                K1
            }
        })
        .collect::<Vec<_>>();
    drop(payload_lengths);
    const PREPARED_WEIGHT_CHUNK_TERMS: usize = 64 * 1024;
    let (prepared_weights, query_denominators) =
        prepared_weights_and_query_denominators_flat_parallel(
            payloads,
            &payload_norms,
            total_docs,
            &doc_freqs,
            PREPARED_WEIGHT_CHUNK_TERMS,
        );
    Ok(PreparedTemplateScoring {
        payload_document_weights,
        total_docs,
        doc_freqs,
        query_denominators,
        prepared_weights,
    })
}

fn prepared_weights_and_query_denominators_flat_parallel(
    payloads: &PayloadTermSoA,
    payload_norms: &[f64],
    total_docs: u64,
    doc_freqs: &[u64],
    chunk_terms: usize,
) -> (Vec<f64>, Vec<f64>) {
    let prepared_weights =
        prepared_weights_flat_parallel(payloads, payload_norms, total_docs, doc_freqs, chunk_terms);
    let query_denominators = (0..payloads.payload_count())
        .into_par_iter()
        .map(|payload_id| {
            let start = payloads.template_offsets[payload_id] as usize;
            let end = payloads.template_offsets[payload_id + 1] as usize;
            let denominator = payloads.template_freqs[payload_id_range(payloads, payload_id)]
                .iter()
                .zip(&prepared_weights[start..end])
                .map(|(&frequency, &weight)| f64::from(frequency) * weight)
                .sum::<f64>();
            if denominator > 0.0 {
                denominator
            } else {
                1.0
            }
        })
        .collect();
    (prepared_weights, query_denominators)
}

fn payload_id_range(payloads: &PayloadTermSoA, payload_id: usize) -> std::ops::Range<usize> {
    payloads.template_offsets[payload_id] as usize
        ..payloads.template_offsets[payload_id + 1] as usize
}

fn prepared_weights_flat_parallel(
    payloads: &PayloadTermSoA,
    payload_norms: &[f64],
    total_docs: u64,
    doc_freqs: &[u64],
    chunk_terms: usize,
) -> Vec<f64> {
    debug_assert_eq!(payload_norms.len(), payloads.payload_count());
    debug_assert_eq!(payloads.template_terms.len(), payloads.template_freqs.len());
    let chunk_terms = chunk_terms.max(1);
    let mut prepared_weights = vec![0.0; payloads.template_terms.len()];
    prepared_weights
        .par_chunks_mut(chunk_terms)
        .enumerate()
        .for_each(|(chunk_index, output)| {
            let chunk_start = chunk_index.saturating_mul(chunk_terms);
            let mut payload = payloads
                .template_offsets
                .partition_point(|&offset| offset <= chunk_start as u64)
                .saturating_sub(1);
            for (local, weight) in output.iter_mut().enumerate() {
                let flat_index = chunk_start + local;
                while payload + 1 < payloads.template_offsets.len()
                    && flat_index as u64 >= payloads.template_offsets[payload + 1]
                {
                    payload += 1;
                }
                *weight = prepared_term_weight(
                    payloads.template_terms[flat_index],
                    payloads.template_freqs[flat_index],
                    payload_norms[payload],
                    total_docs,
                    doc_freqs,
                );
            }
        });
    prepared_weights
}

fn prepared_term_weight(
    term: u32,
    frequency: u32,
    norm: f64,
    total_docs: u64,
    doc_freqs: &[u64],
) -> f64 {
    const K1: f64 = 1.2;
    let tf = f64::from(frequency);
    let df = doc_freqs.get(term as usize).copied().unwrap_or(0) as f64;
    let idf = if total_docs == 0 {
        0.0
    } else {
        ((total_docs as f64 - df + 0.5) / (df + 0.5) + 1.0).ln()
    };
    if tf == 0.0 {
        0.0
    } else {
        idf * (tf * (K1 + 1.0)) / (tf + norm)
    }
}

fn write_payload_term_signatures_soa(
    progress: &mut PayloadProgress<'_, impl FnMut(u64, u64)>,
    bundle_dir: &Path,
    payloads: &PayloadTermSoA,
) -> Result<(), FeatureSoaError> {
    let signature_bytes = payloads
        .payload_count()
        .checked_mul(PAYLOAD_TERM_SIG_BYTES)
        .ok_or(FeatureSoaError::LengthOverflow)? as u64;
    progress.write_u8_iter(
        &bundle_dir.join("payload_template_sigs.u8"),
        signature_bytes,
        (0..payloads.payload_count()).flat_map(|payload| {
            term_id_signature(payloads.template_term_ids(payload).iter().copied())
        }),
    )?;
    progress.write_u8_iter(
        &bundle_dir.join("payload_content_sigs.u8"),
        signature_bytes,
        (0..payloads.payload_count()).flat_map(|payload| {
            term_id_signature(payloads.content_term_ids(payload).iter().copied())
        }),
    )?;
    Ok(())
}

fn write_term_soa_columns(
    progress: &mut PayloadProgress<'_, impl FnMut(u64, u64)>,
    bundle_dir: &Path,
    prefix: &str,
    offsets: &[u64],
    terms: &[u32],
    freqs: &[u32],
) -> Result<(), FeatureSoaError> {
    progress.write_u64(
        &bundle_dir.join(format!("{prefix}_offsets.u64")),
        ArrayKind::U64,
        offsets.len() as u64,
        offsets.iter().copied(),
    )?;
    progress.write_u32(
        &bundle_dir.join(format!("{prefix}_terms.u32")),
        ArrayKind::U32,
        terms.len() as u64,
        terms.iter().copied(),
    )?;
    progress.write_u32(
        &bundle_dir.join(format!("{prefix}_freqs.u32")),
        ArrayKind::U32,
        freqs.len() as u64,
        freqs.iter().copied(),
    )?;
    Ok(())
}

/// Match-facing handle over an encode bundle (features + CSR only).
pub struct EncodeBundle {
    features: FeatureView,
}

impl EncodeBundle {
    /// Open feature/CSR maps. Does **not** open or mmap `payload_blobs/`.
    pub fn open(bundle_dir: &Path) -> Result<Self, FeatureSoaError> {
        Self::open_with_progress(bundle_dir, |_| {})
    }

    pub(crate) fn open_with_progress(
        bundle_dir: &Path,
        mut progress: impl FnMut(u64),
    ) -> Result<Self, FeatureSoaError> {
        let features = FeatureView::open_with_progress(bundle_dir, &mut progress)?;
        Ok(Self { features })
    }

    pub fn feature_view(&self) -> &FeatureView {
        &self.features
    }
}

pub(crate) const FEATURE_ARRAY_FILES: &[&str] = &[
    "source_to_payload.u32",
    "payload_template_offsets.u64",
    "payload_template_terms.u32",
    "payload_template_freqs.u32",
    "payload_content_offsets.u64",
    "payload_content_terms.u32",
    "payload_content_freqs.u32",
    "payload_template_sigs.u8",
    "payload_content_sigs.u8",
    "contract_token_offsets.u64",
    "contract_tokens.u32",
    "token_member_offsets.u64",
    "token_member_contracts.u32",
    "token_member_sources.u32",
    "payload_lengths.u32",
    "query_denominators.f64",
    "prepared_weight_offsets.u64",
    "prepared_weights.f64",
    "contract_source.u32",
    "contract_chain.u32",
    "contract_payload.u32",
    "contract_weight.u64",
    "fallback_atoms_offsets.u64",
    "fallback_atoms_members.u32",
];

/// Typed feature + CSR maps for Match consumers.
///
/// Intentionally has **no** payload_blobs / pack accessor.
pub struct FeatureView {
    pub source_to_payload: MappedU32Array,
    pub payload_template_offsets: MappedU64Array,
    pub payload_template_terms: MappedU32Array,
    pub payload_template_freqs: MappedU32Array,
    pub payload_content_offsets: MappedU64Array,
    pub payload_content_terms: MappedU32Array,
    pub payload_content_freqs: MappedU32Array,
    pub payload_template_sigs: MappedU8Array,
    pub payload_content_sigs: MappedU8Array,
    pub contract_token_offsets: MappedU64Array,
    pub contract_tokens: MappedU32Array,
    pub token_member_offsets: MappedU64Array,
    pub token_member_contracts: MappedU32Array,
    pub token_member_sources: MappedU32Array,
    pub payload_lengths: MappedU32Array,
    pub query_denominators: crate::format::MappedF64Array,
    pub prepared_weight_offsets: MappedU64Array,
    pub prepared_weights: crate::format::MappedF64Array,
    pub contract_source: MappedU32Array,
    pub contract_chain: MappedU32Array,
    pub contract_payload: MappedU32Array,
    pub contract_weight: MappedU64Array,
    pub fallback_atom_offsets: MappedU64Array,
    pub fallback_atom_contracts: MappedU32Array,
}

impl FeatureView {
    fn open_with_progress(
        bundle_dir: &Path,
        progress: &mut impl FnMut(u64),
    ) -> Result<Self, FeatureSoaError> {
        fn require(path: &Path) -> Result<(), FeatureSoaError> {
            if path.is_file() {
                Ok(())
            } else {
                Err(FeatureSoaError::MissingFile(
                    path.file_name()
                        .map(|s| s.to_string_lossy().into_owned())
                        .unwrap_or_else(|| path.display().to_string()),
                ))
            }
        }

        for name in FEATURE_ARRAY_FILES {
            require(&bundle_dir.join(name))?;
        }

        macro_rules! map {
            ($function:ident, $name:literal) => {{
                let path = bundle_dir.join($name);
                let mapped = format::$function(&path)?;
                progress(std::fs::metadata(path)?.len());
                mapped
            }};
        }

        Ok(Self {
            source_to_payload: map!(map_u32_array, "source_to_payload.u32"),
            payload_template_offsets: map!(map_u64_array, "payload_template_offsets.u64"),
            payload_template_terms: map!(map_u32_array, "payload_template_terms.u32"),
            payload_template_freqs: map!(map_u32_array, "payload_template_freqs.u32"),
            payload_content_offsets: map!(map_u64_array, "payload_content_offsets.u64"),
            payload_content_terms: map!(map_u32_array, "payload_content_terms.u32"),
            payload_content_freqs: map!(map_u32_array, "payload_content_freqs.u32"),
            payload_template_sigs: map!(map_u8_array, "payload_template_sigs.u8"),
            payload_content_sigs: map!(map_u8_array, "payload_content_sigs.u8"),
            contract_token_offsets: map!(map_u64_array, "contract_token_offsets.u64"),
            contract_tokens: map!(map_u32_array, "contract_tokens.u32"),
            token_member_offsets: map!(map_u64_array, "token_member_offsets.u64"),
            token_member_contracts: map!(map_u32_array, "token_member_contracts.u32"),
            token_member_sources: map!(map_u32_array, "token_member_sources.u32"),
            payload_lengths: map!(map_u32_array, "payload_lengths.u32"),
            query_denominators: map!(map_f64_array, "query_denominators.f64"),
            prepared_weight_offsets: map!(map_u64_array, "prepared_weight_offsets.u64"),
            prepared_weights: map!(map_f64_array, "prepared_weights.f64"),
            contract_source: map!(map_u32_array, "contract_source.u32"),
            contract_chain: map!(map_u32_array, "contract_chain.u32"),
            contract_payload: map!(map_u32_array, "contract_payload.u32"),
            contract_weight: map!(map_u64_array, "contract_weight.u64"),
            fallback_atom_offsets: map!(map_u64_array, "fallback_atoms_offsets.u64"),
            fallback_atom_contracts: map!(map_u32_array, "fallback_atoms_members.u32"),
        })
    }

    pub fn payload_template_sig(&self, payload_id: u32) -> &[u8] {
        let start = payload_id as usize * PAYLOAD_TERM_SIG_BYTES;
        &self.payload_template_sigs[start..start + PAYLOAD_TERM_SIG_BYTES]
    }

    pub fn payload_content_sig(&self, payload_id: u32) -> &[u8] {
        let start = payload_id as usize * PAYLOAD_TERM_SIG_BYTES;
        &self.payload_content_sigs[start..start + PAYLOAD_TERM_SIG_BYTES]
    }

    pub fn contract_tokens(&self, contract_id: u32) -> &[u32] {
        let off = &*self.contract_token_offsets;
        let i = contract_id as usize;
        if i + 1 >= off.len() {
            return &[];
        }
        let start = off[i] as usize;
        let end = off[i + 1] as usize;
        &self.contract_tokens[start..end]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn directory_bytes(path: &Path) -> u64 {
        fs::read_dir(path)
            .unwrap()
            .map(|entry| {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    directory_bytes(&path)
                } else {
                    fs::metadata(path).unwrap().len()
                }
            })
            .sum()
    }

    #[test]
    fn frozen_feature_space_bound_covers_the_persisted_bundle() {
        let sources = EncodeSourceSoA::from_rows(&[
            EncodeSourceRow {
                contract_id: 0,
                payload_id: 0,
                retained_token_ids: vec![1, 3],
            },
            EncodeSourceRow {
                contract_id: 1,
                payload_id: 1,
                retained_token_ids: vec![3],
            },
        ])
        .unwrap();
        let payloads = PayloadTermSoA::from_rows(&[
            EncodePayloadRow {
                template_terms: vec![(1, 1)],
                content_terms: vec![(2, 2)],
            },
            EncodePayloadRow {
                template_terms: vec![(3, 1)],
                content_terms: vec![(4, 1)],
            },
        ])
        .unwrap();
        let contracts = EncodeContractSoA::from_rows(&[
            EncodeContractRow {
                contract_id: 0,
                chain_id: 0,
                source_doc_id: 0,
                payload_id: 0,
                weight: 1,
            },
            EncodeContractRow {
                contract_id: 1,
                chain_id: 1,
                source_doc_id: 1,
                payload_id: 1,
                weight: 1,
            },
        ]);
        let mut fallback_atoms = FallbackAtomCsr::from_groups(&[vec![0, 1]]).unwrap();
        fallback_atoms.atom_payloads = vec![0];
        let upper =
            encode_artifact_upper_bound_soa(&sources, &payloads, &contracts, &fallback_atoms)
                .unwrap();
        let directory = tempfile::tempdir().unwrap();

        write_encode_artifacts_soa_with_progress(
            directory.path(),
            &sources,
            &payloads,
            &contracts,
            &fallback_atoms,
            |_, _| {},
        )
        .unwrap();

        assert!(directory_bytes(directory.path()) <= upper);
    }

    #[test]
    fn flat_prepared_weights_match_row_reference_across_empty_rows_and_chunks() {
        let payloads = PayloadTermSoA::from_term_lists_owned(vec![
            (vec![], vec![]),
            (vec![(0, 2), (2, 1), (4, 3)], vec![]),
            (vec![], vec![]),
            (vec![(1, 4), (3, 2)], vec![]),
        ])
        .unwrap();
        let norms = [1.0, 1.5, 1.0, 2.0];
        let doc_freqs = [2, 4, 1, 3, 1];
        let expected = (0..payloads.payload_count())
            .flat_map(|payload| {
                payloads
                    .template_term_ids(payload)
                    .iter()
                    .copied()
                    .zip(payloads.template_freqs(payload).iter().copied())
                    .map(move |(term, frequency)| {
                        prepared_term_weight(term, frequency, norms[payload], 10, &doc_freqs)
                    })
            })
            .collect::<Vec<_>>();

        let actual = prepared_weights_flat_parallel(&payloads, &norms, 10, &doc_freqs, 2);

        assert_eq!(
            actual
                .iter()
                .map(|value| value.to_bits())
                .collect::<Vec<_>>(),
            expected
                .iter()
                .map(|value| value.to_bits())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn flat_prepared_weights_and_denominators_share_one_weight_pass() {
        let payloads = PayloadTermSoA::from_term_lists_owned(vec![
            (vec![], vec![]),
            (vec![(0, 2), (2, 1), (4, 3)], vec![]),
            (vec![(1, 4), (3, 2)], vec![]),
        ])
        .unwrap();
        let norms = [1.0, 1.5, 2.0];
        let doc_freqs = [2, 4, 1, 3, 1];

        let (weights, denominators) = prepared_weights_and_query_denominators_flat_parallel(
            &payloads, &norms, 10, &doc_freqs, 2,
        );

        let expected_denominators = (0..payloads.payload_count())
            .map(|payload| {
                let denominator = payloads
                    .template_freqs(payload)
                    .iter()
                    .zip(
                        weights[payloads.template_offsets[payload] as usize
                            ..payloads.template_offsets[payload + 1] as usize]
                            .iter(),
                    )
                    .map(|(&frequency, &weight)| f64::from(frequency) * weight)
                    .sum::<f64>();
                if denominator > 0.0 {
                    denominator
                } else {
                    1.0
                }
            })
            .collect::<Vec<_>>();

        assert_eq!(denominators, expected_denominators);
    }

    #[test]
    fn owned_term_list_pack_preserves_offsets_and_empty_payloads() {
        let packed = PayloadTermSoA::from_term_lists_owned(vec![
            (vec![(3, 2), (7, 1)], vec![]),
            (vec![], vec![(5, 4)]),
            (vec![(9, 6)], vec![(8, 3), (11, 2)]),
        ])
        .unwrap();

        assert_eq!(packed.template_offsets, vec![0, 2, 2, 3]);
        assert_eq!(packed.template_terms, vec![3, 7, 9]);
        assert_eq!(packed.template_freqs, vec![2, 1, 6]);
        assert_eq!(packed.content_offsets, vec![0, 0, 1, 3]);
        assert_eq!(packed.content_terms, vec![5, 8, 11]);
        assert_eq!(packed.content_freqs, vec![4, 3, 2]);
    }

    #[test]
    fn prepared_template_scoring_aggregates_contract_weights_by_payload() {
        let payloads = vec![
            EncodePayloadRow {
                template_terms: vec![(0, 1)],
                content_terms: vec![],
            },
            EncodePayloadRow {
                template_terms: vec![(1, 2)],
                content_terms: vec![],
            },
        ];
        let contracts = vec![
            EncodeContractRow {
                contract_id: 0,
                chain_id: 0,
                source_doc_id: 0,
                payload_id: 0,
                weight: 2,
            },
            EncodeContractRow {
                contract_id: 1,
                chain_id: 0,
                source_doc_id: 1,
                payload_id: 0,
                weight: 3,
            },
            EncodeContractRow {
                contract_id: 2,
                chain_id: 0,
                source_doc_id: 2,
                payload_id: 1,
                weight: 7,
            },
        ];

        let prepared = prepare_template_scoring(&payloads, &contracts).unwrap();

        assert_eq!(prepared.payload_document_weights, vec![5, 7]);
        assert_eq!(prepared.total_docs, 12);
        assert_eq!(prepared.doc_freqs, vec![5, 7]);
        assert_eq!(prepared.query_denominators.len(), 2);
        assert_eq!(prepared.prepared_weights.len(), 2);
    }
}
