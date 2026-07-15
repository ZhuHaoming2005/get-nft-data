//! Feature SoA writer and Match-facing EncodeBundle / FeatureView.

use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use rayon::prelude::*;
use thiserror::Error;

use crate::encode::csr::{
    build_bidirectional_csr_from_iter, write_csr_files_with_progress, BidirectionalCsr, CsrError,
};
use crate::format::{self, ArrayKind, FormatError, MappedU32Array, MappedU64Array};

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
    mut on_progress: impl FnMut(u64, u64),
) -> Result<EncodePersistStats, FeatureSoaError> {
    fs::create_dir_all(bundle_dir)?;

    crate::identity::checked_u32_identity("source rows", sources.len() as u64)?;
    crate::identity::checked_u32_identity("payload rows", payloads.len() as u64)?;
    crate::identity::checked_u32_identity("contract rows", contracts.len() as u64)?;
    crate::identity::checked_u32_identity("fallback atoms", fallback_atoms.len() as u64)?;

    for src in sources {
        if src.payload_id as usize >= payloads.len() {
            return Err(FeatureSoaError::PayloadIdOutOfRange {
                payload_id: src.payload_id,
                len: payloads.len(),
            });
        }
    }

    let csr = build_bidirectional_csr_from_iter(sources.iter().enumerate().map(
        |(source_doc_id, source)| {
            (
                crate::identity::checked_u32_identity("source_doc_id", source_doc_id as u64)
                    .expect("source cardinality checked"),
                source.contract_id,
                source.retained_token_ids.as_slice(),
            )
        },
    ))?;
    let (token_pair_work, max_token_members) = candidate_group_stats(
        csr.token_member_offsets
            .windows(2)
            .map(|window| window[1].saturating_sub(window[0])),
    );
    let (fallback_pair_work, max_fallback_members) =
        candidate_group_stats(fallback_atoms.iter().map(|members| members.len() as u64));
    let total = feature_payload_bytes(sources, payloads, contracts, fallback_atoms, &csr)?;
    let mut progress = PayloadProgress::new(total, &mut on_progress);

    progress.write_u32(
        &bundle_dir.join("source_to_payload.u32"),
        ArrayKind::U32,
        sources.len() as u64,
        sources.iter().map(|source| source.payload_id),
    )?;

    write_term_soa(
        &mut progress,
        bundle_dir,
        "payload_template",
        payloads,
        |p| &p.template_terms,
    )?;
    write_term_soa(
        &mut progress,
        bundle_dir,
        "payload_content",
        payloads,
        |p| &p.content_terms,
    )?;

    progress.write_csr(bundle_dir, &csr)?;

    write_identity_and_placeholder_columns(
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

fn candidate_group_stats(sizes: impl Iterator<Item = u64>) -> (u64, u64) {
    sizes.fold((0u64, 0u64), |(pair_work, maximum), size| {
        (
            pair_work.saturating_add(size.saturating_mul(size.saturating_sub(1)) / 2),
            maximum.max(size),
        )
    })
}

fn feature_payload_bytes(
    sources: &[EncodeSourceRow],
    payloads: &[EncodePayloadRow],
    contracts: &[EncodeContractRow],
    fallback_atoms: &[Vec<u32>],
    csr: &BidirectionalCsr,
) -> Result<u64, FeatureSoaError> {
    let template_terms = payloads.iter().try_fold(0u64, |total, payload| {
        total
            .checked_add(payload.template_terms.len() as u64)
            .ok_or(FeatureSoaError::LengthOverflow)
    })?;
    let content_terms = payloads.iter().try_fold(0u64, |total, payload| {
        total
            .checked_add(payload.content_terms.len() as u64)
            .ok_or(FeatureSoaError::LengthOverflow)
    })?;
    let fallback_members = fallback_atoms.iter().try_fold(0u64, |total, atom| {
        total
            .checked_add(atom.len() as u64)
            .ok_or(FeatureSoaError::LengthOverflow)
    })?;
    let contract_count = contracts
        .iter()
        .map(|contract| u64::from(contract.contract_id) + 1)
        .max()
        .unwrap_or(0);
    let payload_count = payloads.len() as u64;

    let columns = [
        (sources.len() as u64, 4),
        (payload_count + 1, 8),
        (template_terms, 8),
        (payload_count + 1, 8),
        (content_terms, 8),
        (payload_count, 4),
        (payload_count, 8),
        (payload_count + 1, 8),
        (template_terms, 8),
        (contract_count, 20),
        (fallback_atoms.len() as u64 + 1, 8),
        (fallback_members, 4),
    ];
    columns
        .into_iter()
        .try_fold(checked_csr_payload_bytes(csr)?, |total, (count, width)| {
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
fn write_identity_and_placeholder_columns(
    progress: &mut PayloadProgress<'_, impl FnMut(u64, u64)>,
    bundle_dir: &Path,
    sources: &[EncodeSourceRow],
    payloads: &[EncodePayloadRow],
    contracts: &[EncodeContractRow],
    fallback_atoms: &[Vec<u32>],
) -> Result<(), FeatureSoaError> {
    // Distinct from `payload_blobs/payload_lengths.u32` (CAS byte lengths):
    // this is the exact content BM25 token length for each payload.
    progress.write_u32(
        &bundle_dir.join("payload_lengths.u32"),
        ArrayKind::U32,
        payloads.len() as u64,
        payloads.iter().map(|payload| {
            payload
                .content_terms
                .iter()
                .fold(0u32, |total, (_, frequency)| {
                    total.saturating_add(*frequency)
                })
        }),
    )?;
    write_template_scoring_columns(progress, bundle_dir, payloads, contracts)?;
    let max_contract_id = contracts.iter().map(|row| row.contract_id).max();
    let contract_count = max_contract_id.map_or(0, |id| id as usize + 1);
    let mut dense_contracts = vec![None; contract_count];
    for contract in contracts {
        if contract.source_doc_id as usize >= sources.len() {
            return Err(FeatureSoaError::SourceIdOutOfRange {
                source_doc_id: contract.source_doc_id,
                len: sources.len(),
            });
        }
        if contract.payload_id as usize >= payloads.len() {
            return Err(FeatureSoaError::PayloadIdOutOfRange {
                payload_id: contract.payload_id,
                len: payloads.len(),
            });
        }
        let index = contract.contract_id as usize;
        dense_contracts[index] = Some(contract);
    }
    if let Some(max_contract_id) = max_contract_id {
        for (contract_id, contract) in dense_contracts.iter().enumerate() {
            if contract.is_none() {
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
        dense_contracts.len() as u64,
        dense_contracts
            .iter()
            .map(|contract| contract.expect("dense contracts validated").source_doc_id),
    )?;
    progress.write_u32(
        &bundle_dir.join("contract_chain.u32"),
        ArrayKind::U32,
        dense_contracts.len() as u64,
        dense_contracts
            .iter()
            .map(|contract| contract.expect("dense contracts validated").chain_id),
    )?;
    progress.write_u32(
        &bundle_dir.join("contract_payload.u32"),
        ArrayKind::U32,
        dense_contracts.len() as u64,
        dense_contracts
            .iter()
            .map(|contract| contract.expect("dense contracts validated").payload_id),
    )?;
    progress.write_u64(
        &bundle_dir.join("contract_weight.u64"),
        ArrayKind::U64,
        dense_contracts.len() as u64,
        dense_contracts
            .iter()
            .map(|contract| contract.expect("dense contracts validated").weight),
    )?;
    let fallback_member_count = fallback_atoms.iter().try_fold(0u64, |total, atom| {
        total
            .checked_add(atom.len() as u64)
            .ok_or(FeatureSoaError::LengthOverflow)
    })?;
    progress.write_u64(
        &bundle_dir.join("fallback_atoms_offsets.u64"),
        ArrayKind::U64,
        fallback_atoms.len() as u64 + 1,
        std::iter::once(0).chain(fallback_atoms.iter().scan(0u64, |offset, atom| {
            *offset += atom.len() as u64;
            Some(*offset)
        })),
    )?;
    progress.write_u32(
        &bundle_dir.join("fallback_atoms_members.u32"),
        ArrayKind::U32,
        fallback_member_count,
        fallback_atoms.iter().flat_map(|atom| atom.iter().copied()),
    )?;
    Ok(())
}

fn write_template_scoring_columns(
    progress: &mut PayloadProgress<'_, impl FnMut(u64, u64)>,
    bundle_dir: &Path,
    payloads: &[EncodePayloadRow],
    contracts: &[EncodeContractRow],
) -> Result<(), FeatureSoaError> {
    let prepared = prepare_template_scoring(payloads, contracts)?;
    let PreparedTemplateScoring {
        payload_document_weights,
        total_docs,
        doc_freqs,
        query_denominators,
        prepared_weights,
    } = prepared;
    debug_assert_eq!(payload_document_weights.len(), payloads.len());
    debug_assert!(total_docs >= payload_document_weights.iter().copied().max().unwrap_or(0));
    debug_assert!(doc_freqs.iter().all(|&frequency| frequency <= total_docs));
    drop(payload_document_weights);
    drop(doc_freqs);
    let prepared_weight_count = prepared_weights.len() as u64;
    progress.write_f64(
        &bundle_dir.join("query_denominators.f64"),
        ArrayKind::F64,
        payloads.len() as u64,
        query_denominators,
    )?;
    progress.write_u64(
        &bundle_dir.join("prepared_weight_offsets.u64"),
        ArrayKind::U64,
        payloads.len() as u64 + 1,
        std::iter::once(0).chain(payloads.iter().scan(0u64, |offset, payload| {
            *offset += payload.template_terms.len() as u64;
            Some(*offset)
        })),
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

fn prepare_template_scoring(
    payloads: &[EncodePayloadRow],
    contracts: &[EncodeContractRow],
) -> Result<PreparedTemplateScoring, FeatureSoaError> {
    const K1: f64 = 1.2;
    const B: f64 = 0.75;
    let token_count = payloads
        .iter()
        .flat_map(|p| p.template_terms.iter().map(|(term, _)| *term as usize + 1))
        .max()
        .unwrap_or(0);
    let payload_document_weights = (0..payloads.len())
        .map(|_| AtomicU64::new(0))
        .collect::<Vec<_>>();
    contracts.par_iter().for_each(|contract| {
        let weight = &payload_document_weights[contract.payload_id as usize];
        let _ = weight.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            Some(current.saturating_add(contract.weight))
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
    let payload_lengths = payloads
        .par_iter()
        .map(|payload| {
            payload
                .template_terms
                .iter()
                .map(|(_, frequency)| u64::from(*frequency))
                .sum::<u64>()
        })
        .collect::<Vec<_>>();
    let total_terms = payloads
        .par_iter()
        .enumerate()
        .map(|(payload_id, payload)| {
            let weight = payload_document_weights[payload_id];
            if weight != 0 {
                for &(term, _) in &payload.template_terms {
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
    let query_denominators = payloads
        .par_iter()
        .enumerate()
        .map(|(payload_id, payload)| {
            let len = payload_lengths[payload_id];
            let norm = if avg_doc_len > 0.0 {
                K1 * (1.0 - B + B * len as f64 / avg_doc_len)
            } else {
                K1
            };
            let mut denominator = 0.0;
            for &(term, frequency) in &payload.template_terms {
                let weight = prepared_term_weight(term, frequency, norm, total_docs, &doc_freqs);
                denominator += f64::from(frequency) * weight;
            }
            if denominator > 0.0 {
                denominator
            } else {
                1.0
            }
        })
        .collect::<Vec<_>>();
    let doc_freqs_slice = doc_freqs.as_slice();
    let prepared_weights = payloads
        .par_iter()
        .enumerate()
        .flat_map_iter(|(payload_id, payload)| {
            let len = payload_lengths[payload_id];
            let norm = if avg_doc_len > 0.0 {
                K1 * (1.0 - B + B * len as f64 / avg_doc_len)
            } else {
                K1
            };
            payload
                .template_terms
                .iter()
                .map(move |&(term, frequency)| {
                    prepared_term_weight(term, frequency, norm, total_docs, doc_freqs_slice)
                })
        })
        .collect::<Vec<_>>();
    Ok(PreparedTemplateScoring {
        payload_document_weights,
        total_docs,
        doc_freqs,
        query_denominators,
        prepared_weights,
    })
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

fn write_term_soa(
    progress: &mut PayloadProgress<'_, impl FnMut(u64, u64)>,
    bundle_dir: &Path,
    prefix: &str,
    payloads: &[EncodePayloadRow],
    terms_of: impl Fn(&EncodePayloadRow) -> &[(u32, u32)],
) -> Result<(), FeatureSoaError> {
    let term_count = payloads.iter().try_fold(0u64, |total, payload| {
        total
            .checked_add(terms_of(payload).len() as u64)
            .ok_or(FeatureSoaError::LengthOverflow)
    })?;
    progress.write_u64(
        &bundle_dir.join(format!("{prefix}_offsets.u64")),
        ArrayKind::U64,
        payloads.len() as u64 + 1,
        std::iter::once(0).chain(payloads.iter().scan(0u64, |offset, payload| {
            *offset += terms_of(payload).len() as u64;
            Some(*offset)
        })),
    )?;
    progress.write_u32(
        &bundle_dir.join(format!("{prefix}_terms.u32")),
        ArrayKind::U32,
        term_count,
        payloads
            .iter()
            .flat_map(|payload| terms_of(payload).iter().map(|(term, _)| *term)),
    )?;
    progress.write_u32(
        &bundle_dir.join(format!("{prefix}_freqs.u32")),
        ArrayKind::U32,
        term_count,
        payloads
            .iter()
            .flat_map(|payload| terms_of(payload).iter().map(|(_, frequency)| *frequency)),
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
