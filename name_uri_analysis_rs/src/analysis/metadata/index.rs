use std::borrow::Cow;
use std::collections::{hash_map::RandomState, HashMap};
use std::hash::BuildHasher;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use duckdb::Connection;
use rayon::prelude::*;

use super::super::{
    arrow_i64_column, arrow_string_column, chain_pair_index, AnalysisError, ProgressTracker,
    SparseUnionFind, UnionFind,
};
use super::bm25::{
    compact_metadata_content_pair_score, CompactMetadataContentDocument, CompactMetadataPostings,
    CompactMetadataScoring, InternedMetadataCorpus, InternedMetadataSourceDoc,
    MetadataBm25Document, PreparedInternedMetadataDoc, PreparedInternedMetadataQuery,
};
#[cfg(test)]
use super::bm25::{CompactMetadataContentSet, MetadataContentRecord};
use super::parse::metadata_document_from_json;
use super::{
    metadata_contract_index_from_usize, metadata_contract_index_to_usize,
    metadata_doc_index_from_usize, metadata_doc_index_to_usize, CompactContractTokens,
    MetadataContractIndex, MetadataData, MetadataDocIndex, MetadataDocPair,
    MetadataTemplateMatches, SourceMetadataDocEntry, METADATA_CONTENT_PARALLEL_MIN_RECORDS,
    METADATA_CONTENT_SCORE_BATCH_PAIRS, METADATA_PAIR_LEFT_CHUNK_SIZE, METADATA_THRESHOLD,
};

pub(super) const METADATA_RAW_GROUP_CHUNK_SIZE: usize = 1024;
const NO_METADATA_ATOM: usize = usize::MAX;

pub(super) struct MetadataContentAtom {
    pub(super) chain_index: usize,
    pub(super) template_doc_index: MetadataDocIndex,
    pub(super) representative_record_index: MetadataDocIndex,
    pub(super) members: Vec<MetadataContractIndex>,
    pub(super) fallback_token_groups: Vec<MetadataFallbackTokenGroup>,
}

#[derive(Debug)]
pub(super) struct MetadataFallbackTokenGroup {
    pub(super) members: Vec<MetadataContractIndex>,
}

pub(super) struct MetadataContentCandidateIndex {
    entries: Vec<(u32, MetadataDocIndex, MetadataDocIndex)>,
}

#[derive(Default)]
struct CompactMetadataContentGroupBuilder {
    token_ids: HashMap<String, u32>,
    atom_hasher: RandomState,
    atom_index_by_hash: HashMap<u64, usize>,
    next_atom_with_same_hash: Vec<usize>,
    fallback_group_index_by_hash: HashMap<(usize, u64), usize>,
    next_fallback_group_with_same_hash: Vec<Vec<usize>>,
    docs: Vec<CompactMetadataContentDocument>,
    atoms: Vec<MetadataContentAtom>,
    token_key_bytes: usize,
    term_count: usize,
    member_count: usize,
    fallback_group_count: usize,
    fallback_member_count: usize,
}

#[derive(Default)]
pub(super) struct MetadataRawTokenGroup {
    raw_records: Vec<(MetadataContractIndex, String)>,
    raw_payload_bytes: usize,
    compact: CompactMetadataContentGroupBuilder,
    raw_record_count: usize,
    #[cfg(test)]
    max_raw_buffer_len: usize,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug)]
pub(super) enum MetadataContentScope {
    SharedToken,
    NoCommonToken,
}

#[derive(Debug)]
pub(crate) struct InternedMetadataIndex {
    doc_count: usize,
    pub(super) total_docs: usize,
    pub(super) scoring: CompactMetadataScoring,
    pub(super) postings: CompactMetadataPostings,
    #[cfg(test)]
    pub(super) token_ids: HashMap<String, usize>,
    #[cfg(test)]
    pub(super) build_thread_count: usize,
}

pub(super) struct MetadataCandidateScratch {
    pub(super) seen_generation: Vec<u16>,
    generation: u16,
    pub(super) candidates: Vec<MetadataDocIndex>,
}

pub(super) struct MetadataCandidateScratchPool {
    pub(super) doc_count: usize,
    scratches: Mutex<Vec<MetadataCandidateScratch>>,
}

pub(super) struct MetadataCandidateScratchLease<'a> {
    pool: &'a MetadataCandidateScratchPool,
    scratch: Option<MetadataCandidateScratch>,
}

pub(super) struct MetadataPairScoringContext<'a> {
    pub(super) postings: &'a CompactMetadataPostings,
    pub(super) scoring: &'a CompactMetadataScoring,
}

struct MetadataHitPermits {
    remaining: AtomicUsize,
    exceeded: AtomicBool,
}

impl MetadataHitPermits {
    fn new(remaining: usize) -> Self {
        Self {
            remaining: AtomicUsize::new(remaining),
            exceeded: AtomicBool::new(false),
        }
    }

    fn exceeded(&self) -> bool {
        self.exceeded.load(Ordering::Relaxed)
    }

    fn try_acquire(&self) -> bool {
        if self.exceeded() {
            return false;
        }
        if self
            .remaining
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            true
        } else {
            self.exceeded.store(true, Ordering::Relaxed);
            false
        }
    }
}

pub(super) struct MetadataContentUnionContext<'a> {
    pub(super) data: &'a MetadataData,
    pub(super) template_matches: &'a MetadataTemplateMatches,
    pub(super) contract_tokens: &'a CompactContractTokens,
    pub(super) chain_count: usize,
    pub(super) pool: &'a rayon::ThreadPool,
}

pub(super) struct MetadataUnionState {
    pub(super) intra: UnionFind,
    pub(super) cross: Option<SparseUnionFind>,
    pub(super) chain_matrix: Option<Vec<SparseUnionFind>>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct MetadataContentUnionStats {
    pub(super) atom_count: usize,
    pub(super) candidate_pairs: u64,
    pub(super) scored_pairs: u64,
}

impl MetadataContentUnionStats {
    pub(super) fn accumulate(&mut self, other: Self) {
        self.atom_count = self.atom_count.saturating_add(other.atom_count);
        self.candidate_pairs = self.candidate_pairs.saturating_add(other.candidate_pairs);
        self.scored_pairs = self.scored_pairs.saturating_add(other.scored_pairs);
    }
}

impl CompactMetadataContentGroupBuilder {
    fn vec_bytes_upper(len: usize, element_bytes: usize) -> usize {
        if len == 0 {
            0
        } else {
            len.saturating_mul(2)
                .saturating_add(4)
                .saturating_mul(element_bytes)
        }
    }

    fn atomized_memory_bytes(&self) -> usize {
        Self::vec_bytes_upper(
            self.docs.len(),
            std::mem::size_of::<CompactMetadataContentDocument>(),
        )
        .saturating_add(Self::vec_bytes_upper(
            self.atoms.len(),
            std::mem::size_of::<MetadataContentAtom>(),
        ))
        .saturating_add(Self::vec_bytes_upper(
            self.term_count,
            std::mem::size_of::<(u32, usize)>(),
        ))
        .saturating_add(Self::vec_bytes_upper(
            self.member_count,
            std::mem::size_of::<MetadataContractIndex>(),
        ))
        .saturating_add(Self::vec_bytes_upper(
            self.fallback_group_count,
            std::mem::size_of::<MetadataFallbackTokenGroup>(),
        ))
        .saturating_add(Self::vec_bytes_upper(
            self.fallback_member_count,
            std::mem::size_of::<MetadataContractIndex>(),
        ))
    }

    fn builder_memory_bytes(&self) -> usize {
        self.atomized_memory_bytes()
            .saturating_add(super::load::hash_table_allocation_for_len_upper(
                self.token_ids.len(),
                std::mem::size_of::<(String, u32)>(),
            ))
            .saturating_add(self.token_key_bytes)
            .saturating_add(super::load::hash_table_allocation_for_len_upper(
                self.atom_index_by_hash.len(),
                std::mem::size_of::<(u64, usize)>(),
            ))
            .saturating_add(Self::vec_bytes_upper(
                self.next_atom_with_same_hash.len(),
                std::mem::size_of::<usize>(),
            ))
            .saturating_add(super::load::hash_table_allocation_for_len_upper(
                self.fallback_group_index_by_hash.len(),
                std::mem::size_of::<((usize, u64), usize)>(),
            ))
            .saturating_add(Self::vec_bytes_upper(
                self.next_fallback_group_with_same_hash.len(),
                std::mem::size_of::<Vec<usize>>(),
            ))
            .saturating_add(Self::vec_bytes_upper(
                self.fallback_group_count,
                std::mem::size_of::<usize>(),
            ))
    }

    fn scoring_peak_bytes(&self) -> usize {
        if self.atoms.is_empty() {
            return 0;
        }
        let candidate_index = Self::vec_bytes_upper(
            self.term_count,
            std::mem::size_of::<(u32, MetadataDocIndex, MetadataDocIndex)>(),
        );
        let candidate_scratch = self.atoms.len().saturating_mul(
            std::mem::size_of::<u16>().saturating_add(2 * std::mem::size_of::<MetadataDocIndex>()),
        );
        let pair_batches = METADATA_CONTENT_SCORE_BATCH_PAIRS
            .saturating_mul(std::mem::size_of::<(usize, MetadataDocIndex)>())
            .saturating_mul(2);
        let union_scratch = self.member_count.saturating_mul(
            2usize
                .saturating_mul(std::mem::size_of::<usize>())
                .saturating_add(std::mem::size_of::<MetadataContractIndex>()),
        );
        let peak = self
            .atomized_memory_bytes()
            .saturating_add(candidate_index)
            .saturating_add(candidate_scratch)
            .saturating_add(pair_batches)
            .saturating_add(union_scratch);
        peak.saturating_add(peak.saturating_div(4))
    }

    fn ensure_within_memory_budget(
        &self,
        raw_parse_reserve_bytes: usize,
        maximum_bytes: usize,
    ) -> Result<(), AnalysisError> {
        let build_peak = self
            .builder_memory_bytes()
            .saturating_add(raw_parse_reserve_bytes);
        let peak = build_peak.max(self.scoring_peak_bytes());
        if peak > maximum_bytes {
            return Err(AnalysisError::InvalidData(format!(
                "metadata content working set needs about {}, exceeding remaining analysis budget {}",
                super::super::format_byte_size(peak),
                super::super::format_byte_size(maximum_bytes)
            )));
        }
        Ok(())
    }

    fn push_document(
        &mut self,
        contract_index: MetadataContractIndex,
        document: &MetadataBm25Document,
        data: &MetadataData,
        contract_tokens: Option<&CompactContractTokens>,
    ) {
        let mut terms = Vec::with_capacity(document.unique_len());
        for token in &document.unique_tokens {
            let token_id = if let Some(&token_id) = self.token_ids.get(token.as_str()) {
                token_id
            } else {
                let token_id = u32::try_from(self.token_ids.len())
                    .expect("metadata content token dictionary exceeds u32 indexes");
                let token = token.clone();
                self.token_key_bytes = self.token_key_bytes.saturating_add(token.capacity());
                self.token_ids.insert(token, token_id);
                token_id
            };
            let term_frequency = document
                .term_freqs
                .get(token)
                .copied()
                .expect("unique metadata token must have a term frequency");
            terms.push((token_id, term_frequency));
        }
        terms.sort_unstable_by_key(|(token_id, _)| *token_id);
        self.member_count = self.member_count.saturating_add(1);
        let contract = &data.contracts[metadata_contract_index_to_usize(contract_index)];
        let atom_hash = self.atom_hasher.hash_one((
            contract.chain_index,
            contract.template_doc_index,
            terms.as_slice(),
        ));
        let mut candidate_atom = self.atom_index_by_hash.get(&atom_hash).copied();
        let mut existing_atom = None;
        while let Some(atom_index) = candidate_atom {
            let atom = &self.atoms[atom_index];
            if atom.chain_index == contract.chain_index
                && atom.template_doc_index == contract.template_doc_index
                && self.docs[metadata_doc_index_to_usize(atom.representative_record_index)].terms
                    == terms
            {
                existing_atom = Some(atom_index);
                break;
            }
            let next = self.next_atom_with_same_hash[atom_index];
            candidate_atom = (next != NO_METADATA_ATOM).then_some(next);
        }
        let atom_index = if let Some(atom_index) = existing_atom {
            self.atoms[atom_index].members.push(contract_index);
            atom_index
        } else {
            let compact_doc_index = metadata_doc_index_from_usize(self.docs.len());
            self.term_count = self.term_count.saturating_add(terms.len());
            self.docs.push(CompactMetadataContentDocument {
                len: document.len(),
                terms,
            });
            let atom_index = self.atoms.len();
            self.atoms.push(MetadataContentAtom {
                chain_index: contract.chain_index,
                template_doc_index: contract.template_doc_index,
                representative_record_index: compact_doc_index,
                members: vec![contract_index],
                fallback_token_groups: Vec::new(),
            });
            let previous_atom = self
                .atom_index_by_hash
                .insert(atom_hash, atom_index)
                .unwrap_or(NO_METADATA_ATOM);
            self.next_atom_with_same_hash.push(previous_atom);
            atom_index
        };
        if let Some(contract_tokens) = contract_tokens {
            self.push_fallback_token_group(atom_index, contract_index, contract_tokens);
        }
    }

    fn push_fallback_token_group(
        &mut self,
        atom_index: usize,
        contract_index: MetadataContractIndex,
        contract_tokens: &CompactContractTokens,
    ) {
        let tokens = contract_tokens.tokens(metadata_contract_index_to_usize(contract_index));
        self.fallback_member_count = self.fallback_member_count.saturating_add(1);
        let token_hash = self.atom_hasher.hash_one(tokens);
        let lookup_key = (atom_index, token_hash);
        let mut candidate_group = self.fallback_group_index_by_hash.get(&lookup_key).copied();
        let mut existing_group = None;
        while let Some(group_index) = candidate_group {
            let group = &self.atoms[atom_index].fallback_token_groups[group_index];
            let representative = metadata_contract_index_to_usize(group.members[0]);
            if contract_tokens.tokens(representative) == tokens {
                existing_group = Some(group_index);
                break;
            }
            let next = self.next_fallback_group_with_same_hash[atom_index][group_index];
            candidate_group = (next != NO_METADATA_ATOM).then_some(next);
        }
        if let Some(group_index) = existing_group {
            self.atoms[atom_index].fallback_token_groups[group_index]
                .members
                .push(contract_index);
            return;
        }

        while self.next_fallback_group_with_same_hash.len() <= atom_index {
            self.next_fallback_group_with_same_hash.push(Vec::new());
        }
        let group_index = self.atoms[atom_index].fallback_token_groups.len();
        self.fallback_group_count = self.fallback_group_count.saturating_add(1);
        self.atoms[atom_index]
            .fallback_token_groups
            .push(MetadataFallbackTokenGroup {
                members: vec![contract_index],
            });
        let previous_group = self
            .fallback_group_index_by_hash
            .insert(lookup_key, group_index)
            .unwrap_or(NO_METADATA_ATOM);
        self.next_fallback_group_with_same_hash[atom_index].push(previous_group);
    }

    fn into_atomized_parts(
        self,
    ) -> (
        Vec<MetadataContentAtom>,
        Vec<CompactMetadataContentDocument>,
    ) {
        let Self { docs, atoms, .. } = self;
        (atoms, docs)
    }
}

impl MetadataRawTokenGroup {
    #[cfg(test)]
    pub(super) fn raw_parse_reserve_bytes(&self) -> usize {
        let raw_bytes = self
            .raw_records
            .capacity()
            .saturating_mul(std::mem::size_of::<(MetadataContractIndex, String)>())
            .saturating_add(self.raw_payload_bytes);
        // Use the same adversarial high-cardinality estimate as the initial
        // metadata loader. JSON normalization, token strings, term-frequency
        // maps and Rayon result buffers coexist before online atomization.
        super::load::metadata_uncached_parse_transient_bytes(raw_bytes, 0)
    }

    fn reserve_raw_record(&mut self) -> Result<(), AnalysisError> {
        self.raw_records.try_reserve(1).map_err(|_| {
            AnalysisError::InvalidData(
                "unable to reserve bounded metadata raw-group chunk".to_string(),
            )
        })
    }

    fn projected_raw_parse_reserve_bytes(&self, candidate_payload_bytes: usize) -> usize {
        let raw_bytes = self
            .raw_records
            .capacity()
            .saturating_mul(std::mem::size_of::<(MetadataContractIndex, String)>())
            .saturating_add(self.raw_payload_bytes)
            .saturating_add(candidate_payload_bytes);
        super::load::metadata_uncached_parse_transient_bytes(raw_bytes, 0)
    }

    #[cfg(test)]
    pub(super) fn push_raw(
        &mut self,
        contract_index: MetadataContractIndex,
        metadata_json: String,
        context: &MetadataContentUnionContext<'_>,
    ) {
        self.push_raw_with_budget(contract_index, metadata_json, context, usize::MAX)
            .expect("unbounded metadata test group must fit memory");
    }

    pub(super) fn push_raw_with_budget(
        &mut self,
        contract_index: MetadataContractIndex,
        metadata_json: String,
        context: &MetadataContentUnionContext<'_>,
        maximum_bytes: usize,
    ) -> Result<(), AnalysisError> {
        let candidate_payload_bytes = metadata_json.capacity();
        self.reserve_raw_record()?;
        let projected_reserve = self.projected_raw_parse_reserve_bytes(candidate_payload_bytes);
        if !self.raw_records.is_empty()
            && self
                .compact
                .ensure_within_memory_budget(projected_reserve, maximum_bytes)
                .is_err()
        {
            self.flush_raw(context, maximum_bytes)?;
            self.reserve_raw_record()?;
        }
        self.compact.ensure_within_memory_budget(
            self.projected_raw_parse_reserve_bytes(candidate_payload_bytes),
            maximum_bytes,
        )?;
        self.raw_payload_bytes = self
            .raw_payload_bytes
            .saturating_add(candidate_payload_bytes);
        self.raw_records.push((contract_index, metadata_json));
        self.raw_record_count = self.raw_record_count.saturating_add(1);
        #[cfg(test)]
        {
            self.max_raw_buffer_len = self.max_raw_buffer_len.max(self.raw_records.len());
        }
        if self.raw_records.len() >= METADATA_RAW_GROUP_CHUNK_SIZE {
            self.flush_raw(context, maximum_bytes)?;
        }
        Ok(())
    }

    fn flush_raw(
        &mut self,
        context: &MetadataContentUnionContext<'_>,
        maximum_bytes: usize,
    ) -> Result<(), AnalysisError> {
        if self.raw_records.is_empty() {
            return Ok(());
        }
        let raw_records = std::mem::take(&mut self.raw_records);
        self.raw_payload_bytes = 0;
        if raw_records.len() >= METADATA_CONTENT_PARALLEL_MIN_RECORDS {
            let parsed = context.pool.install(|| {
                raw_records
                    .into_par_iter()
                    .map(|(contract_index, metadata_json)| {
                        metadata_content_document(context.data, &metadata_json)
                            .map(|document| (contract_index, document))
                    })
                    .collect::<Vec<_>>()
            });
            for (contract_index, document) in parsed.into_iter().flatten() {
                self.compact
                    .push_document(contract_index, document.as_ref(), context.data, None);
            }
        } else {
            for (contract_index, metadata_json) in raw_records {
                if let Some(document) = metadata_content_document(context.data, &metadata_json) {
                    self.compact.push_document(
                        contract_index,
                        document.as_ref(),
                        context.data,
                        None,
                    );
                }
            }
        }
        self.compact.ensure_within_memory_budget(0, maximum_bytes)?;
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn union(
        self,
        context: &MetadataContentUnionContext<'_>,
        state: &mut MetadataUnionState,
    ) -> MetadataContentUnionStats {
        self.union_with_budget(context, state, usize::MAX)
            .expect("unbounded metadata test group must fit memory")
    }

    fn union_with_budget(
        mut self,
        context: &MetadataContentUnionContext<'_>,
        state: &mut MetadataUnionState,
        maximum_bytes: usize,
    ) -> Result<MetadataContentUnionStats, AnalysisError> {
        if self.raw_record_count < 2 {
            return Ok(MetadataContentUnionStats::default());
        }
        self.flush_raw(context, maximum_bytes)?;
        drop(self.raw_records);
        self.compact.ensure_within_memory_budget(0, maximum_bytes)?;
        let (atoms, docs) = self.compact.into_atomized_parts();
        Ok(union_metadata_shared_token_atom_core(
            atoms, &docs, context, state,
        ))
    }

    #[cfg(test)]
    pub(super) fn raw_buffer_len(&self) -> usize {
        self.raw_records.len()
    }

    #[cfg(test)]
    pub(super) fn max_raw_buffer_len(&self) -> usize {
        self.max_raw_buffer_len
    }

    #[cfg(test)]
    pub(super) fn compact_doc_count(&self) -> usize {
        self.compact.docs.len()
    }

    #[cfg(test)]
    pub(super) fn compact_member_count(&self) -> usize {
        self.compact
            .atoms
            .iter()
            .map(|atom| atom.members.len())
            .sum()
    }
}

#[derive(Debug, Default)]
pub(super) struct MetadataTemplateScoringStats {
    pub(super) candidate_pairs: u64,
    pub(super) scored_pairs: u64,
    pub(super) matched_pairs: u64,
}

impl MetadataContentCandidateIndex {
    #[cfg(test)]
    pub(super) fn new(
        docs: &[CompactMetadataContentDocument],
        template_docs: &[MetadataDocIndex],
    ) -> Self {
        debug_assert_eq!(docs.len(), template_docs.len());
        let mut entries = Vec::with_capacity(docs.iter().map(|doc| doc.terms.len()).sum());
        for (record_index, (doc, &template_doc)) in docs.iter().zip(template_docs).enumerate() {
            let record_index = metadata_doc_index_from_usize(record_index);
            for &(token_id, _) in &doc.terms {
                entries.push((token_id, template_doc, record_index));
            }
        }
        entries.sort_unstable();
        Self { entries }
    }

    pub(super) fn from_atoms(
        docs: &[CompactMetadataContentDocument],
        atoms: &[MetadataContentAtom],
    ) -> Self {
        let mut entries = Self::atom_entries(docs, atoms);
        entries.sort_unstable();
        Self { entries }
    }

    fn atom_entries(
        docs: &[CompactMetadataContentDocument],
        atoms: &[MetadataContentAtom],
    ) -> Vec<(u32, MetadataDocIndex, MetadataDocIndex)> {
        let mut entries = Vec::with_capacity(
            atoms
                .iter()
                .map(|atom| {
                    docs[metadata_doc_index_to_usize(atom.representative_record_index)]
                        .terms
                        .len()
                })
                .sum(),
        );
        for (atom_index, atom) in atoms.iter().enumerate() {
            let compact_atom_index = metadata_doc_index_from_usize(atom_index);
            let doc = &docs[metadata_doc_index_to_usize(atom.representative_record_index)];
            for &(token_id, _) in &doc.terms {
                entries.push((token_id, atom.template_doc_index, compact_atom_index));
            }
        }
        entries
    }

    pub(super) fn from_atoms_parallel(
        docs: &[CompactMetadataContentDocument],
        atoms: &[MetadataContentAtom],
    ) -> Self {
        let mut entries = Self::atom_entries(docs, atoms);
        entries.par_sort_unstable();
        Self { entries }
    }

    pub(super) fn append_candidates_after(
        &self,
        record_index: usize,
        document: &CompactMetadataContentDocument,
        template_doc: MetadataDocIndex,
        template_matches: &MetadataTemplateMatches,
        scratch: &mut MetadataCandidateScratch,
    ) {
        let compact_record_index = metadata_doc_index_from_usize(record_index);
        for &(token_id, _) in &document.terms {
            self.append_posting_after(token_id, template_doc, compact_record_index, scratch);
            for &compatible_doc in template_matches.compatible_docs(template_doc) {
                self.append_posting_after(token_id, compatible_doc, compact_record_index, scratch);
            }
        }
    }

    fn append_posting_after(
        &self,
        token_id: u32,
        template_doc: MetadataDocIndex,
        record_index: MetadataDocIndex,
        scratch: &mut MetadataCandidateScratch,
    ) {
        let key = (token_id, template_doc);
        let posting_start = self
            .entries
            .partition_point(|&(token, template, _)| (token, template) < key);
        let posting_end = self
            .entries
            .partition_point(|&(token, template, _)| (token, template) <= key);
        let posting = &self.entries[posting_start..posting_end];
        let start = posting.partition_point(|&(_, _, right)| right <= record_index);
        for &(_, _, right) in &posting[start..] {
            scratch.push_once(right);
        }
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
    pub(super) fn memory_bytes(&self) -> usize {
        self.entries.capacity().saturating_mul(std::mem::size_of::<(
            u32,
            MetadataDocIndex,
            MetadataDocIndex,
        )>())
    }
}

impl MetadataCandidateScratch {
    pub(super) fn new(doc_count: usize) -> Self {
        Self {
            seen_generation: vec![0; doc_count],
            generation: 0,
            candidates: Vec::new(),
        }
    }

    pub(super) fn clear_for_next_left(&mut self) {
        self.candidates.clear();
        self.generation = self.generation.wrapping_add(1);
        if self.generation == 0 {
            self.seen_generation.fill(0);
            self.generation = 1;
        }
    }

    pub(super) fn push_once(&mut self, index: MetadataDocIndex) {
        let index_usize = metadata_doc_index_to_usize(index);
        if self.seen_generation[index_usize] == self.generation {
            return;
        }
        self.seen_generation[index_usize] = self.generation;
        self.candidates.push(index);
    }
}

impl MetadataCandidateScratchPool {
    pub(super) fn new(doc_count: usize) -> Self {
        Self {
            doc_count,
            scratches: Mutex::new(Vec::new()),
        }
    }

    pub(super) fn take(&self) -> MetadataCandidateScratchLease<'_> {
        let scratch = self
            .scratches
            .lock()
            .expect("metadata candidate scratch pool lock poisoned")
            .pop()
            .unwrap_or_else(|| MetadataCandidateScratch::new(self.doc_count));
        MetadataCandidateScratchLease {
            pool: self,
            scratch: Some(scratch),
        }
    }
}

impl std::ops::Deref for MetadataCandidateScratchLease<'_> {
    type Target = MetadataCandidateScratch;

    fn deref(&self) -> &Self::Target {
        self.scratch
            .as_ref()
            .expect("metadata candidate scratch lease is empty")
    }
}

impl std::ops::DerefMut for MetadataCandidateScratchLease<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.scratch
            .as_mut()
            .expect("metadata candidate scratch lease is empty")
    }
}

impl Drop for MetadataCandidateScratchLease<'_> {
    fn drop(&mut self) {
        let Some(scratch) = self.scratch.take() else {
            return;
        };
        self.pool
            .scratches
            .lock()
            .expect("metadata candidate scratch pool lock poisoned")
            .push(scratch);
    }
}

pub(super) fn collect_metadata_template_matches(
    data: &MetadataData,
    progress: &ProgressTracker,
    max_match_pairs: u64,
) -> Result<(MetadataTemplateMatches, MetadataTemplateScoringStats), AnalysisError> {
    let index = &data.metadata_index;
    if index.total_docs == 0 {
        return Ok((
            MetadataTemplateMatches::default(),
            MetadataTemplateScoringStats::default(),
        ));
    }
    let scoring_left_count = index.doc_count();
    let mut scored_candidate_pairs = 0u64;
    let mut scored_left_docs = 0usize;
    let mut matched_doc_pairs = 0u64;
    let mut matched_docs = Vec::new();
    let progress_start = Instant::now();
    let scratch_pool = MetadataCandidateScratchPool::new(index.doc_count());
    progress.add_work(metadata_scoring_progress_units(scoring_left_count));
    progress.set_message(metadata_pair_progress_message(
        scored_candidate_pairs,
        scored_left_docs,
        scoring_left_count,
        matched_doc_pairs,
        progress_start.elapsed(),
    ));
    let mut left_start = 0usize;
    while left_start < scoring_left_count {
        let remaining_match_pairs = max_match_pairs.saturating_sub(matched_doc_pairs);
        let left_chunk_size =
            metadata_pair_left_chunk_size(scoring_left_count, remaining_match_pairs);
        let left_end = left_start
            .saturating_add(left_chunk_size)
            .min(scoring_left_count);
        let batch = collect_metadata_doc_pair_hits_for_left_range_bounded(
            left_start..left_end,
            MetadataPairScoringContext {
                postings: &index.postings,
                scoring: &index.scoring,
            },
            &scratch_pool,
            usize::try_from(remaining_match_pairs).unwrap_or(usize::MAX),
        )
        .map_err(|_| {
            AnalysisError::InvalidData(format!(
                "metadata template matches exceed the analysis-memory pair budget ({max_match_pairs})"
            ))
        })?;
        scored_candidate_pairs = scored_candidate_pairs.saturating_add(batch.candidate_pairs);
        scored_left_docs = left_end;
        matched_doc_pairs = matched_doc_pairs.saturating_add(batch.hits.len() as u64);
        debug_assert!(matched_doc_pairs <= max_match_pairs);
        progress.inc(metadata_scoring_batch_progress_units(left_start, left_end));
        progress.set_message(metadata_pair_progress_message(
            scored_candidate_pairs,
            scored_left_docs,
            scoring_left_count,
            matched_doc_pairs,
            progress_start.elapsed(),
        ));
        matched_docs.extend(batch.hits);
        left_start = left_end;
    }
    matched_docs.sort_unstable();
    matched_docs.dedup();
    Ok((
        MetadataTemplateMatches::from_pairs(scoring_left_count, matched_docs),
        MetadataTemplateScoringStats {
            candidate_pairs: scored_candidate_pairs,
            scored_pairs: scored_candidate_pairs,
            matched_pairs: matched_doc_pairs,
        },
    ))
}

pub(super) fn lowest_common_metadata_token(left: &[u32], right: &[u32]) -> Option<u32> {
    let mut left_index = 0usize;
    let mut right_index = 0usize;
    while left_index < left.len() && right_index < right.len() {
        match left[left_index].cmp(&right[right_index]) {
            std::cmp::Ordering::Equal => return Some(left[left_index]),
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
        }
    }
    None
}

pub(super) fn union_metadata_token_content_matches(
    conn: &Connection,
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
    maximum_working_bytes: usize,
) -> Result<MetadataContentUnionStats, AnalysisError> {
    let mut stmt = conn.prepare(
        "
        SELECT t.token_index, t.contract_index, a.metadata_json
        FROM metadata_contract_token_rows t
        JOIN metadata_rows a
          ON a.source_file = t.metadata_source_file
         AND a.source_row_number = t.metadata_source_row_number
        ORDER BY t.token_index, t.contract_index
        ",
    )?;
    let mut current_token = None;
    let mut group = MetadataRawTokenGroup::default();
    let mut stats = MetadataContentUnionStats::default();
    for batch in stmt.query_arrow([])? {
        let token_column = arrow_i64_column(&batch, 0, "token_index")?;
        let contract_column = arrow_i64_column(&batch, 1, "contract_index")?;
        let metadata_column = arrow_string_column(&batch, 2, "metadata_json")?;
        for row_index in 0..batch.num_rows() {
            let token_index = u32::try_from(token_column.value(row_index)).map_err(|_| {
                AnalysisError::InvalidData(
                    "metadata token dictionary exceeds compact u32 indexes".to_string(),
                )
            })?;
            if current_token.is_some_and(|current| current != token_index) {
                stats.accumulate(std::mem::take(&mut group).union_with_budget(
                    context,
                    state,
                    maximum_working_bytes,
                )?);
            }
            current_token = Some(token_index);
            let source_contract_index =
                usize::try_from(contract_column.value(row_index)).map_err(|_| {
                    AnalysisError::InvalidData(
                        "negative metadata source contract index".to_string(),
                    )
                })?;
            let Some(contract_index) = context
                .data
                .compact_contract_index_for_source(source_contract_index)
            else {
                continue;
            };
            group.push_raw_with_budget(
                contract_index,
                metadata_column.value(row_index).to_owned(),
                context,
                maximum_working_bytes,
            )?;
        }
    }
    if current_token.is_some() {
        stats.accumulate(group.union_with_budget(context, state, maximum_working_bytes)?);
    }
    Ok(stats)
}

pub(super) fn metadata_content_document<'a>(
    data: &'a MetadataData,
    raw: &str,
) -> Option<Cow<'a, MetadataBm25Document>> {
    data.reused_documents
        .get(raw)
        .and_then(|cached| cached.content.as_deref())
        .map(Cow::Borrowed)
        .or_else(|| {
            MetadataBm25Document::from_text(&metadata_document_from_json(raw)).map(Cow::Owned)
        })
}

pub(super) fn union_metadata_representative_content_fallback(
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
    maximum_working_bytes: usize,
) -> Result<MetadataContentUnionStats, AnalysisError> {
    let mut builder = CompactMetadataContentGroupBuilder::default();
    for (contract_index, contract) in context.data.contracts.iter().enumerate() {
        if let Some(document) = &contract.content_doc {
            builder.push_document(
                metadata_contract_index_from_usize(contract_index),
                document.as_ref(),
                context.data,
                Some(context.contract_tokens),
            );
            builder.ensure_within_memory_budget(0, maximum_working_bytes)?;
        }
    }
    builder.ensure_within_memory_budget(0, maximum_working_bytes)?;
    let (atoms, docs) = builder.into_atomized_parts();
    Ok(union_metadata_no_common_atom_core(
        atoms, &docs, context, state,
    ))
}

pub(super) fn apply_metadata_contract_pair_union(
    data: &MetadataData,
    chain_count: usize,
    state: &mut MetadataUnionState,
    left: usize,
    right: usize,
) {
    let left_chain = data.contracts[left].chain_index;
    let right_chain = data.contracts[right].chain_index;
    if left_chain == right_chain {
        state.intra.union(left, right);
        return;
    }
    if let Some(cross) = &mut state.cross {
        cross.union(left, right);
    }
    if let Some(matrix) = &mut state.chain_matrix {
        let (primary_chain, secondary_chain) = if left_chain < right_chain {
            (left_chain, right_chain)
        } else {
            (right_chain, left_chain)
        };
        let pair_index = chain_pair_index(primary_chain, secondary_chain, chain_count);
        matrix[pair_index].union(left, right);
    }
}

pub(super) fn apply_metadata_complete_match_group_union(
    data: &MetadataData,
    chain_count: usize,
    state: &mut MetadataUnionState,
    members: &[MetadataContractIndex],
) {
    if members.len() < 2 {
        return;
    }
    let mut members_by_chain = vec![Vec::<usize>::new(); chain_count];
    for &member in members {
        let member = metadata_contract_index_to_usize(member);
        members_by_chain[data.contracts[member].chain_index].push(member);
    }
    for chain_members in &members_by_chain {
        let Some((&anchor, rest)) = chain_members.split_first() else {
            continue;
        };
        for &member in rest {
            apply_metadata_contract_pair_union(data, chain_count, state, anchor, member);
        }
    }
    for left_chain in 0..chain_count {
        let Some((&left_anchor, left_rest)) = members_by_chain[left_chain].split_first() else {
            continue;
        };
        for right_members in members_by_chain.iter().skip(left_chain + 1) {
            let Some((&right_anchor, right_rest)) = right_members.split_first() else {
                continue;
            };
            apply_metadata_contract_pair_union(data, chain_count, state, left_anchor, right_anchor);
            for &right in right_rest {
                apply_metadata_contract_pair_union(data, chain_count, state, left_anchor, right);
            }
            for &left in left_rest {
                apply_metadata_contract_pair_union(data, chain_count, state, left, right_anchor);
            }
        }
    }
}

pub(super) fn apply_metadata_complete_bipartite_group_union(
    data: &MetadataData,
    chain_count: usize,
    state: &mut MetadataUnionState,
    left_members: &[MetadataContractIndex],
    right_members: &[MetadataContractIndex],
) {
    let Some((&left_anchor, left_rest)) = left_members.split_first() else {
        return;
    };
    let Some((&right_anchor, right_rest)) = right_members.split_first() else {
        return;
    };
    apply_metadata_contract_pair_union(
        data,
        chain_count,
        state,
        metadata_contract_index_to_usize(left_anchor),
        metadata_contract_index_to_usize(right_anchor),
    );
    for &left in left_rest {
        apply_metadata_contract_pair_union(
            data,
            chain_count,
            state,
            metadata_contract_index_to_usize(left),
            metadata_contract_index_to_usize(right_anchor),
        );
    }
    for &right in right_rest {
        apply_metadata_contract_pair_union(
            data,
            chain_count,
            state,
            metadata_contract_index_to_usize(left_anchor),
            metadata_contract_index_to_usize(right),
        );
    }
}

pub(super) fn metadata_fallback_token_group_tokens<'a>(
    group: &MetadataFallbackTokenGroup,
    contract_tokens: &'a CompactContractTokens,
) -> &'a [u32] {
    let representative = metadata_contract_index_to_usize(group.members[0]);
    &contract_tokens[representative]
}

pub(super) fn metadata_fallback_token_groups_are_disjoint(
    left: &MetadataFallbackTokenGroup,
    right: &MetadataFallbackTokenGroup,
    contract_tokens: &CompactContractTokens,
) -> bool {
    lowest_common_metadata_token(
        metadata_fallback_token_group_tokens(left, contract_tokens),
        metadata_fallback_token_group_tokens(right, contract_tokens),
    )
    .is_none()
}

pub(super) fn apply_metadata_fallback_atom_internal_unions(
    atom: &MetadataContentAtom,
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
) {
    for group in &atom.fallback_token_groups {
        if metadata_fallback_token_group_tokens(group, context.contract_tokens).is_empty() {
            apply_metadata_complete_match_group_union(
                context.data,
                context.chain_count,
                state,
                &group.members,
            );
        }
    }

    let mut unvisited = (0..atom.fallback_token_groups.len()).collect::<Vec<_>>();
    while let Some(root) = unvisited.pop() {
        let mut queue = vec![root];
        while let Some(current) = queue.pop() {
            let mut index = 0;
            while index < unvisited.len() {
                let other = unvisited[index];
                if !metadata_fallback_token_groups_are_disjoint(
                    &atom.fallback_token_groups[current],
                    &atom.fallback_token_groups[other],
                    context.contract_tokens,
                ) {
                    index += 1;
                    continue;
                }
                let other = unvisited.swap_remove(index);
                apply_metadata_complete_bipartite_group_union(
                    context.data,
                    context.chain_count,
                    state,
                    &atom.fallback_token_groups[current].members,
                    &atom.fallback_token_groups[other].members,
                );
                queue.push(other);
            }
        }
    }
}

pub(super) fn metadata_fallback_atoms_have_disjoint_token_groups(
    left: &MetadataContentAtom,
    right: &MetadataContentAtom,
    contract_tokens: &CompactContractTokens,
) -> bool {
    left.fallback_token_groups.iter().any(|left_group| {
        right.fallback_token_groups.iter().any(|right_group| {
            metadata_fallback_token_groups_are_disjoint(left_group, right_group, contract_tokens)
        })
    })
}

pub(super) fn apply_metadata_fallback_atom_pair_union(
    left: &MetadataContentAtom,
    right: &MetadataContentAtom,
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
) {
    let mut unvisited_left = (0..left.fallback_token_groups.len()).collect::<Vec<_>>();
    let mut unvisited_right = (0..right.fallback_token_groups.len()).collect::<Vec<_>>();
    while let Some(root) = unvisited_left.pop() {
        let mut queue = vec![(true, root)];
        while let Some((is_left, current)) = queue.pop() {
            let (current_group, opposite_groups, unvisited_opposite) = if is_left {
                (
                    &left.fallback_token_groups[current],
                    &right.fallback_token_groups,
                    &mut unvisited_right,
                )
            } else {
                (
                    &right.fallback_token_groups[current],
                    &left.fallback_token_groups,
                    &mut unvisited_left,
                )
            };
            let mut index = 0;
            while index < unvisited_opposite.len() {
                let other = unvisited_opposite[index];
                let other_group = &opposite_groups[other];
                if !metadata_fallback_token_groups_are_disjoint(
                    current_group,
                    other_group,
                    context.contract_tokens,
                ) {
                    index += 1;
                    continue;
                }
                let other = unvisited_opposite.swap_remove(index);
                let (left_group, right_group) = if is_left {
                    (current_group, &right.fallback_token_groups[other])
                } else {
                    (&left.fallback_token_groups[other], current_group)
                };
                apply_metadata_complete_bipartite_group_union(
                    context.data,
                    context.chain_count,
                    state,
                    &left_group.members,
                    &right_group.members,
                );
                queue.push((!is_left, other));
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct MetadataDocPairBatch {
    pub(super) hits: Vec<MetadataDocPair>,
    pub(super) candidate_pairs: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct MetadataHitLimitExceeded {
    pub(super) retained_hits: usize,
}

pub(super) fn metadata_scoring_progress_units(scoring_left_count: usize) -> u64 {
    scoring_left_count as u64
}

pub(super) fn metadata_pair_left_chunk_size(doc_count: usize, max_match_pairs: u64) -> usize {
    let doc_count = u64::try_from(doc_count.max(1)).unwrap_or(u64::MAX);
    let budgeted_chunk = max_match_pairs / doc_count;
    budgeted_chunk.clamp(1, METADATA_PAIR_LEFT_CHUNK_SIZE as u64) as usize
}

pub(super) fn metadata_template_match_pair_budget(available_bytes: usize, doc_count: usize) -> u64 {
    let fixed_offsets_and_cursors = doc_count.saturating_mul(2 * std::mem::size_of::<u64>());
    let pairs = available_bytes
        .saturating_sub(fixed_offsets_and_cursors)
        // A compact pair is 8 bytes. During scoring, the retained pair Vec,
        // Rayon-local/reduced batch Vecs and append reallocation can overlap;
        // conversion then overlaps the retained Vec with 8-byte symmetric
        // postings. Reserve a conservative 40 bytes per logical pair.
        .saturating_div(40);
    u64::try_from(pairs).unwrap_or(u64::MAX)
}

pub(super) fn metadata_scoring_batch_progress_units(left_start: usize, left_end: usize) -> u64 {
    left_end.saturating_sub(left_start) as u64
}

pub(super) fn metadata_pair_progress_message(
    scored_pairs: u64,
    scored_left_docs: usize,
    total_left_docs: usize,
    matched_pairs: u64,
    elapsed: Duration,
) -> String {
    let remaining_left_docs = total_left_docs.saturating_sub(scored_left_docs);
    let estimated_remaining_pairs = estimate_remaining_metadata_candidate_pairs(
        scored_pairs,
        scored_left_docs,
        remaining_left_docs,
    );
    let throughput = format_metadata_pair_throughput(scored_pairs, elapsed);
    let eta = format_metadata_pair_eta(estimated_remaining_pairs, scored_pairs, elapsed);
    format!(
        "metadata candidate pairs scored {scored_pairs}; left docs {scored_left_docs}/{total_left_docs}; estimated remaining {estimated_remaining_pairs}; throughput {throughput}; ETA {eta}; matched doc pairs {matched_pairs}"
    )
}

pub(super) fn estimate_remaining_metadata_candidate_pairs(
    scored_pairs: u64,
    scored_left_docs: usize,
    remaining_left_docs: usize,
) -> u64 {
    if scored_pairs == 0 || scored_left_docs == 0 || remaining_left_docs == 0 {
        return 0;
    }
    let estimated = (scored_pairs as u128)
        .saturating_mul(remaining_left_docs as u128)
        .div_ceil(scored_left_docs as u128);
    estimated.min(u64::MAX as u128) as u64
}

pub(super) fn format_metadata_pair_throughput(scored_pairs: u64, elapsed: Duration) -> String {
    let Some(pairs_per_second) = metadata_pairs_per_second(scored_pairs, elapsed) else {
        return "n/a".to_string();
    };
    format!("{pairs_per_second:.1} pairs/s")
}

pub(super) fn format_metadata_pair_eta(
    remaining_pairs: u64,
    scored_pairs: u64,
    elapsed: Duration,
) -> String {
    if scored_pairs == 0 {
        return "n/a".to_string();
    }
    if remaining_pairs == 0 {
        return "0s".to_string();
    }
    let Some(pairs_per_second) = metadata_pairs_per_second(scored_pairs, elapsed) else {
        return "n/a".to_string();
    };
    format_metadata_duration(Duration::from_secs_f64(
        (remaining_pairs as f64 / pairs_per_second).ceil(),
    ))
}

pub(super) fn metadata_pairs_per_second(scored_pairs: u64, elapsed: Duration) -> Option<f64> {
    if scored_pairs == 0 {
        return None;
    }
    let elapsed_seconds = elapsed.as_secs_f64();
    if elapsed_seconds <= 0.0 {
        return None;
    }
    Some(scored_pairs as f64 / elapsed_seconds)
}

pub(super) fn format_metadata_duration(duration: Duration) -> String {
    let seconds = duration.as_secs();
    if seconds < 60 {
        return format!("{seconds}s");
    }
    let minutes = seconds / 60;
    let remaining_seconds = seconds % 60;
    if minutes < 60 {
        return format!("{minutes}m {remaining_seconds:02}s");
    }
    let hours = minutes / 60;
    let remaining_minutes = minutes % 60;
    format!("{hours}h {remaining_minutes:02}m")
}

#[cfg(test)]
pub(super) fn collect_metadata_doc_pair_hits_for_left_range(
    left_range: std::ops::Range<usize>,
    context: MetadataPairScoringContext<'_>,
    scratch_pool: &MetadataCandidateScratchPool,
) -> MetadataDocPairBatch {
    collect_metadata_doc_pair_hits_for_left_range_bounded(
        left_range,
        context,
        scratch_pool,
        usize::MAX,
    )
    .expect("an unbounded metadata hit collector cannot exhaust pair permits")
}

pub(super) fn collect_metadata_doc_pair_hits_for_left_range_bounded(
    left_range: std::ops::Range<usize>,
    context: MetadataPairScoringContext<'_>,
    scratch_pool: &MetadataCandidateScratchPool,
    maximum_hits: usize,
) -> Result<MetadataDocPairBatch, MetadataHitLimitExceeded> {
    let context = &context;
    let permits = MetadataHitPermits::new(maximum_hits);
    let (mut hits, candidate_pairs) = left_range
        .into_par_iter()
        .map_init(
            || scratch_pool.take(),
            |scratch, left| {
                let mut local_hits = Vec::new();
                let local_candidate_pairs =
                    collect_metadata_doc_pair_hits_for_left_with_scratch_bounded(
                        left,
                        context,
                        scratch,
                        &mut local_hits,
                        Some(&permits),
                    );
                (local_hits, local_candidate_pairs)
            },
        )
        .reduce(
            || (Vec::new(), 0u64),
            |(mut left_hits, left_candidate_pairs), (mut right_hits, right_candidate_pairs)| {
                left_hits.append(&mut right_hits);
                (
                    left_hits,
                    left_candidate_pairs.saturating_add(right_candidate_pairs),
                )
            },
        );
    if permits.exceeded() {
        return Err(MetadataHitLimitExceeded {
            retained_hits: hits.len(),
        });
    }
    hits.sort_unstable();
    hits.dedup();
    Ok(MetadataDocPairBatch {
        hits,
        candidate_pairs,
    })
}

#[cfg(test)]
pub(super) fn collect_metadata_doc_pair_hits_for_left_with_scratch(
    left: usize,
    context: &MetadataPairScoringContext<'_>,
    scratch: &mut MetadataCandidateScratch,
    hits: &mut Vec<MetadataDocPair>,
) -> u64 {
    collect_metadata_doc_pair_hits_for_left_with_scratch_bounded(left, context, scratch, hits, None)
}

fn collect_metadata_doc_pair_hits_for_left_with_scratch_bounded(
    left: usize,
    context: &MetadataPairScoringContext<'_>,
    scratch: &mut MetadataCandidateScratch,
    hits: &mut Vec<MetadataDocPair>,
    permits: Option<&MetadataHitPermits>,
) -> u64 {
    let candidates = metadata_candidate_indices_for_left_with_scratch(left, context, scratch);
    let mut scored_candidates = 0u64;
    for &right in candidates {
        if permits.is_some_and(MetadataHitPermits::exceeded) {
            break;
        }
        let right = metadata_doc_index_to_usize(right);
        scored_candidates = scored_candidates.saturating_add(1);
        if context.scoring.score(left, right) >= METADATA_THRESHOLD {
            if permits.is_some_and(|permits| !permits.try_acquire()) {
                break;
            }
            hits.push(ordered_metadata_doc_pair(left, right));
        }
    }
    scored_candidates
}

pub(super) fn metadata_candidate_indices_for_left_with_scratch<'a>(
    left: usize,
    context: &MetadataPairScoringContext<'_>,
    scratch: &'a mut MetadataCandidateScratch,
) -> &'a [MetadataDocIndex] {
    scratch.clear_for_next_left();
    let compact_left = metadata_doc_index_from_usize(left);
    for &token in context.scoring.candidate_tokens(left) {
        append_metadata_posting_except(
            context.postings.posting(token as usize),
            compact_left,
            scratch,
        );
    }
    scratch.candidates.sort_unstable();
    &scratch.candidates
}

pub(super) fn append_metadata_posting_except(
    posting: &[MetadataDocIndex],
    excluded: MetadataDocIndex,
    scratch: &mut MetadataCandidateScratch,
) {
    for &index in posting {
        if index != excluded {
            scratch.push_once(index);
        }
    }
}

pub(super) fn ordered_metadata_doc_pair(left: usize, right: usize) -> MetadataDocPair {
    let left = metadata_doc_index_from_usize(left);
    let right = metadata_doc_index_from_usize(right);
    if left <= right {
        (left, right)
    } else {
        (right, left)
    }
}

pub(super) fn metadata_content_pair_matches(
    left: &CompactMetadataContentDocument,
    right: &CompactMetadataContentDocument,
    threshold: f64,
) -> bool {
    compact_metadata_content_pair_score(left, right) >= threshold
}

#[cfg(test)]
pub(super) fn build_metadata_content_atoms(
    records: &[MetadataContentRecord],
    compact_docs: &[CompactMetadataContentDocument],
    data: &MetadataData,
) -> Vec<MetadataContentAtom> {
    build_metadata_content_atoms_core(records.len(), compact_docs, data, |record_index| {
        records[record_index].contract_index
    })
}

#[cfg(test)]
fn build_metadata_content_atoms_core(
    record_count: usize,
    compact_docs: &[CompactMetadataContentDocument],
    data: &MetadataData,
    mut contract_index_at: impl FnMut(usize) -> MetadataContractIndex,
) -> Vec<MetadataContentAtom> {
    debug_assert_eq!(record_count, compact_docs.len());
    let mut atom_index_by_key = HashMap::<(usize, MetadataDocIndex, &[(u32, usize)]), usize>::new();
    let mut atoms = Vec::<MetadataContentAtom>::new();
    for (record_index, document) in compact_docs.iter().enumerate() {
        let compact_contract_index = contract_index_at(record_index);
        let contract_index = metadata_contract_index_to_usize(compact_contract_index);
        let contract = &data.contracts[contract_index];
        let key = (
            contract.chain_index,
            contract.template_doc_index,
            document.terms.as_slice(),
        );
        if let Some(&atom_index) = atom_index_by_key.get(&key) {
            atoms[atom_index].members.push(compact_contract_index);
            continue;
        }
        let atom_index = atoms.len();
        atom_index_by_key.insert(key, atom_index);
        atoms.push(MetadataContentAtom {
            chain_index: contract.chain_index,
            template_doc_index: contract.template_doc_index,
            representative_record_index: metadata_doc_index_from_usize(record_index),
            members: vec![compact_contract_index],
            fallback_token_groups: Vec::new(),
        });
    }
    atoms
}

#[cfg(test)]
pub(super) fn build_metadata_fallback_atoms(
    records: &[MetadataContentRecord],
    compact_docs: &[CompactMetadataContentDocument],
    data: &MetadataData,
    contract_tokens: &CompactContractTokens,
) -> Vec<MetadataContentAtom> {
    let mut atom_index_by_key = HashMap::<(usize, MetadataDocIndex, &[(u32, usize)]), usize>::new();
    let mut token_group_index_by_atom = Vec::<HashMap<&[u32], usize>>::new();
    let mut atoms = Vec::<MetadataContentAtom>::new();
    for (record_index, record) in records.iter().enumerate() {
        let contract_index = metadata_contract_index_to_usize(record.contract_index);
        let contract = &data.contracts[contract_index];
        let key = (
            contract.chain_index,
            contract.template_doc_index,
            compact_docs[record_index].terms.as_slice(),
        );
        if let Some(&atom_index) = atom_index_by_key.get(&key) {
            let atom = &mut atoms[atom_index];
            atom.members.push(record.contract_index);
            let token_group_indexes = &mut token_group_index_by_atom[atom_index];
            let tokens = contract_tokens.tokens(contract_index);
            if let Some(&token_group_index) = token_group_indexes.get(tokens) {
                atom.fallback_token_groups[token_group_index]
                    .members
                    .push(record.contract_index);
            } else {
                let token_group_index = atom.fallback_token_groups.len();
                token_group_indexes.insert(tokens, token_group_index);
                atom.fallback_token_groups.push(MetadataFallbackTokenGroup {
                    members: vec![record.contract_index],
                });
            }
            continue;
        }
        let atom_index = atoms.len();
        atom_index_by_key.insert(key, atom_index);
        token_group_index_by_atom
            .push(HashMap::from([(contract_tokens.tokens(contract_index), 0)]));
        atoms.push(MetadataContentAtom {
            chain_index: contract.chain_index,
            template_doc_index: contract.template_doc_index,
            representative_record_index: metadata_doc_index_from_usize(record_index),
            members: vec![record.contract_index],
            fallback_token_groups: vec![MetadataFallbackTokenGroup {
                members: vec![record.contract_index],
            }],
        });
    }
    atoms
}

pub(super) fn metadata_content_atom_pair_matches(
    pair: (usize, MetadataDocIndex),
    atoms: &[MetadataContentAtom],
    compact_docs: &[CompactMetadataContentDocument],
) -> bool {
    let (left, right) = pair;
    let left_record = metadata_doc_index_to_usize(atoms[left].representative_record_index);
    let right_record = metadata_doc_index_to_usize(
        atoms[metadata_doc_index_to_usize(right)].representative_record_index,
    );
    metadata_content_pair_matches(
        &compact_docs[left_record],
        &compact_docs[right_record],
        METADATA_THRESHOLD,
    )
}

pub(super) fn collect_metadata_content_atom_pair_hits(
    candidate_pairs: &[(usize, MetadataDocIndex)],
    atoms: &[MetadataContentAtom],
    compact_docs: &[CompactMetadataContentDocument],
    pool: &rayon::ThreadPool,
) -> Vec<(usize, MetadataDocIndex)> {
    if candidate_pairs.len() >= METADATA_CONTENT_PARALLEL_MIN_RECORDS {
        pool.install(|| {
            candidate_pairs
                .par_iter()
                .copied()
                .filter(|&pair| metadata_content_atom_pair_matches(pair, atoms, compact_docs))
                .collect()
        })
    } else {
        candidate_pairs
            .iter()
            .copied()
            .filter(|&pair| metadata_content_atom_pair_matches(pair, atoms, compact_docs))
            .collect()
    }
}

pub(super) fn score_and_apply_metadata_atom_pair_batch(
    candidate_pairs: &mut Vec<(usize, MetadataDocIndex)>,
    atoms: &[MetadataContentAtom],
    compact_docs: &[CompactMetadataContentDocument],
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
) -> u64 {
    if candidate_pairs.is_empty() {
        return 0;
    }
    let scored_pairs = candidate_pairs.len() as u64;
    let hits =
        collect_metadata_content_atom_pair_hits(candidate_pairs, atoms, compact_docs, context.pool);
    candidate_pairs.clear();
    for (left, right) in hits {
        let left_atom = &atoms[left];
        let right_atom = &atoms[metadata_doc_index_to_usize(right)];
        let mut members = Vec::with_capacity(left_atom.members.len() + right_atom.members.len());
        members.extend_from_slice(&left_atom.members);
        members.extend_from_slice(&right_atom.members);
        apply_metadata_complete_match_group_union(
            context.data,
            context.chain_count,
            state,
            &members,
        );
    }
    scored_pairs
}

pub(super) fn score_and_apply_metadata_fallback_atom_pair_batch(
    candidate_pairs: &mut Vec<(usize, MetadataDocIndex)>,
    atoms: &[MetadataContentAtom],
    compact_docs: &[CompactMetadataContentDocument],
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
) -> u64 {
    if candidate_pairs.is_empty() {
        return 0;
    }
    let scored_pairs = candidate_pairs.len() as u64;
    let hits =
        collect_metadata_content_atom_pair_hits(candidate_pairs, atoms, compact_docs, context.pool);
    candidate_pairs.clear();
    for (left, right) in hits {
        apply_metadata_fallback_atom_pair_union(
            &atoms[left],
            &atoms[metadata_doc_index_to_usize(right)],
            context,
            state,
        );
    }
    scored_pairs
}

#[cfg(test)]
pub(super) fn collect_metadata_content_candidate_pairs(
    records: &[MetadataContentRecord],
    template_docs: &[MetadataDocIndex],
    template_matches: &MetadataTemplateMatches,
) -> Vec<(MetadataContractIndex, MetadataContractIndex)> {
    let compact = CompactMetadataContentSet::from_records(records);
    let index = MetadataContentCandidateIndex::new(&compact.docs, template_docs);
    let mut scratch = MetadataCandidateScratch::new(records.len());
    let mut pairs = Vec::new();
    for left in 0..records.len().saturating_sub(1) {
        scratch.clear_for_next_left();
        index.append_candidates_after(
            left,
            &compact.docs[left],
            template_docs[left],
            template_matches,
            &mut scratch,
        );
        for &right in &scratch.candidates {
            pairs.push((
                records[left].contract_index,
                records[metadata_doc_index_to_usize(right)].contract_index,
            ));
        }
    }
    pairs.sort_unstable();
    pairs
}

#[cfg(test)]
pub(super) fn union_metadata_shared_token_atoms(
    records: &[MetadataContentRecord],
    compact_docs: &[CompactMetadataContentDocument],
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
) -> MetadataContentUnionStats {
    let atoms = build_metadata_content_atoms(records, compact_docs, context.data);
    union_metadata_shared_token_atom_core(atoms, compact_docs, context, state)
}

fn union_metadata_shared_token_atom_core(
    atoms: Vec<MetadataContentAtom>,
    compact_docs: &[CompactMetadataContentDocument],
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
) -> MetadataContentUnionStats {
    let mut stats = MetadataContentUnionStats {
        atom_count: atoms.len(),
        ..MetadataContentUnionStats::default()
    };
    for atom in &atoms {
        apply_metadata_complete_match_group_union(
            context.data,
            context.chain_count,
            state,
            &atom.members,
        );
    }
    if atoms.len() < 2 {
        return stats;
    }
    let candidate_index = if atoms.len() >= METADATA_CONTENT_PARALLEL_MIN_RECORDS {
        context
            .pool
            .install(|| MetadataContentCandidateIndex::from_atoms_parallel(compact_docs, &atoms))
    } else {
        MetadataContentCandidateIndex::from_atoms(compact_docs, &atoms)
    };
    let mut scratch = MetadataCandidateScratch::new(atoms.len());
    let mut candidate_pairs = Vec::with_capacity(METADATA_CONTENT_SCORE_BATCH_PAIRS);
    for left in 0..atoms.len().saturating_sub(1) {
        let left_atom = &atoms[left];
        let left_record_index = metadata_doc_index_to_usize(left_atom.representative_record_index);
        let left_contract_index = metadata_contract_index_to_usize(left_atom.members[0]);
        debug_assert_eq!(
            context.data.contracts[left_contract_index].chain_index,
            left_atom.chain_index
        );
        scratch.clear_for_next_left();
        candidate_index.append_candidates_after(
            left,
            &compact_docs[left_record_index],
            left_atom.template_doc_index,
            context.template_matches,
            &mut scratch,
        );
        stats.candidate_pairs = stats
            .candidate_pairs
            .saturating_add(scratch.candidates.len() as u64);
        for &right in &scratch.candidates {
            let right_atom = &atoms[metadata_doc_index_to_usize(right)];
            let right_contract_index = metadata_contract_index_to_usize(right_atom.members[0]);
            debug_assert!(context.template_matches.matches(
                metadata_doc_index_to_usize(left_atom.template_doc_index),
                metadata_doc_index_to_usize(right_atom.template_doc_index),
            ));
            let singleton_pair = left_atom.members.len() == 1 && right_atom.members.len() == 1;
            if !singleton_pair
                || !metadata_pair_already_connected(
                    context.data,
                    context.chain_count,
                    state,
                    left_contract_index,
                    right_contract_index,
                )
            {
                candidate_pairs.push((left, right));
                if candidate_pairs.len() >= METADATA_CONTENT_SCORE_BATCH_PAIRS {
                    stats.scored_pairs = stats.scored_pairs.saturating_add(
                        score_and_apply_metadata_atom_pair_batch(
                            &mut candidate_pairs,
                            &atoms,
                            compact_docs,
                            context,
                            state,
                        ),
                    );
                }
            }
        }
    }
    stats.scored_pairs =
        stats
            .scored_pairs
            .saturating_add(score_and_apply_metadata_atom_pair_batch(
                &mut candidate_pairs,
                &atoms,
                compact_docs,
                context,
                state,
            ));
    stats
}

#[cfg(test)]
pub(super) fn union_metadata_no_common_content_candidates(
    records: &[MetadataContentRecord],
    compact_docs: &[CompactMetadataContentDocument],
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
) -> MetadataContentUnionStats {
    let atoms =
        build_metadata_fallback_atoms(records, compact_docs, context.data, context.contract_tokens);
    union_metadata_no_common_atom_core(atoms, compact_docs, context, state)
}

fn union_metadata_no_common_atom_core(
    atoms: Vec<MetadataContentAtom>,
    compact_docs: &[CompactMetadataContentDocument],
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
) -> MetadataContentUnionStats {
    let mut stats = MetadataContentUnionStats {
        atom_count: atoms.len(),
        ..MetadataContentUnionStats::default()
    };
    for atom in &atoms {
        apply_metadata_fallback_atom_internal_unions(atom, context, state);
    }
    if atoms.len() < 2 {
        return stats;
    }
    let candidate_index = if atoms.len() >= METADATA_CONTENT_PARALLEL_MIN_RECORDS {
        context
            .pool
            .install(|| MetadataContentCandidateIndex::from_atoms_parallel(compact_docs, &atoms))
    } else {
        MetadataContentCandidateIndex::from_atoms(compact_docs, &atoms)
    };
    let mut scratch = MetadataCandidateScratch::new(atoms.len());
    let mut candidate_pairs = Vec::with_capacity(METADATA_CONTENT_SCORE_BATCH_PAIRS);
    for left in 0..atoms.len().saturating_sub(1) {
        let left_atom = &atoms[left];
        let left_record_index = metadata_doc_index_to_usize(left_atom.representative_record_index);
        scratch.clear_for_next_left();
        candidate_index.append_candidates_after(
            left,
            &compact_docs[left_record_index],
            left_atom.template_doc_index,
            context.template_matches,
            &mut scratch,
        );
        stats.candidate_pairs = stats
            .candidate_pairs
            .saturating_add(scratch.candidates.len() as u64);
        let left_contract_index = metadata_contract_index_to_usize(left_atom.members[0]);
        for &right in &scratch.candidates {
            let right_atom = &atoms[metadata_doc_index_to_usize(right)];
            let right_index = metadata_contract_index_to_usize(right_atom.members[0]);
            debug_assert!(context.template_matches.matches(
                metadata_doc_index_to_usize(left_atom.template_doc_index),
                metadata_doc_index_to_usize(right_atom.template_doc_index),
            ));
            let singleton_pair = left_atom.members.len() == 1 && right_atom.members.len() == 1;
            if singleton_pair
                && metadata_pair_already_connected(
                    context.data,
                    context.chain_count,
                    state,
                    left_contract_index,
                    right_index,
                )
            {
                continue;
            }
            if metadata_fallback_atoms_have_disjoint_token_groups(
                left_atom,
                right_atom,
                context.contract_tokens,
            ) {
                candidate_pairs.push((left, right));
                if candidate_pairs.len() >= METADATA_CONTENT_SCORE_BATCH_PAIRS {
                    stats.scored_pairs = stats.scored_pairs.saturating_add(
                        score_and_apply_metadata_fallback_atom_pair_batch(
                            &mut candidate_pairs,
                            &atoms,
                            compact_docs,
                            context,
                            state,
                        ),
                    );
                }
            }
        }
    }
    stats.scored_pairs =
        stats
            .scored_pairs
            .saturating_add(score_and_apply_metadata_fallback_atom_pair_batch(
                &mut candidate_pairs,
                &atoms,
                compact_docs,
                context,
                state,
            ));
    stats
}

#[cfg(test)]
pub(super) fn union_metadata_content_candidates(
    records: &[MetadataContentRecord],
    scope: MetadataContentScope,
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
) -> MetadataContentUnionStats {
    let compact = CompactMetadataContentSet::from_records(records);
    match scope {
        MetadataContentScope::SharedToken => {
            union_metadata_shared_token_atoms(records, &compact.docs, context, state)
        }
        MetadataContentScope::NoCommonToken => {
            union_metadata_no_common_content_candidates(records, &compact.docs, context, state)
        }
    }
}

pub(super) fn metadata_pair_already_connected(
    data: &MetadataData,
    chain_count: usize,
    state: &mut MetadataUnionState,
    left: usize,
    right: usize,
) -> bool {
    let left_chain = data.contracts[left].chain_index;
    let right_chain = data.contracts[right].chain_index;
    if left_chain == right_chain {
        return state.intra.find(left) == state.intra.find(right);
    }
    let cross_connected = state
        .cross
        .as_mut()
        .is_some_and(|cross| cross.connected(left, right));
    let (primary_chain, secondary_chain) = if left_chain < right_chain {
        (left_chain, right_chain)
    } else {
        (right_chain, left_chain)
    };
    let matrix_connected = state.chain_matrix.as_mut().is_some_and(|matrix| {
        matrix[chain_pair_index(primary_chain, secondary_chain, chain_count)].connected(left, right)
    });
    cross_connected && matrix_connected
}

pub(super) fn lexical_metadata_token_ids(
    entries: &[SourceMetadataDocEntry],
) -> HashMap<&str, usize> {
    let mut tokens = entries
        .iter()
        .flat_map(|entry| entry.doc.unique_tokens.iter().map(String::as_str))
        .collect::<Vec<_>>();
    tokens.par_sort_unstable();
    tokens.dedup();
    tokens
        .into_iter()
        .enumerate()
        .map(|(token_id, token)| (token, token_id))
        .collect()
}

impl InternedMetadataIndex {
    pub(super) fn doc_count(&self) -> usize {
        self.doc_count
    }

    pub(super) fn is_empty(&self) -> bool {
        self.doc_count == 0
    }

    pub(super) fn owned_memory_bytes(&self) -> usize {
        self.scoring
            .owned_memory_bytes()
            .saturating_add(self.postings.owned_memory_bytes())
    }

    pub(super) fn remap_if_over_budget(
        &mut self,
        directory: &Path,
        maximum_owned_bytes: usize,
    ) -> std::io::Result<bool> {
        if self.owned_memory_bytes() <= maximum_owned_bytes {
            return Ok(false);
        }
        self.remap_postings(directory)?;
        Ok(true)
    }

    pub(super) fn remap_postings(&mut self, directory: &Path) -> std::io::Result<()> {
        let postings = std::mem::replace(
            &mut self.postings,
            CompactMetadataPostings::from_nested(Vec::new()),
        );
        self.postings = postings.persist_and_remap(directory)?;
        let scoring = std::mem::replace(
            &mut self.scoring,
            CompactMetadataScoring::from_nested(Vec::new(), Vec::new()),
        );
        self.scoring = scoring.persist_and_remap(directory)?;
        Ok(())
    }

    pub(super) fn from_source_doc_entries(entries: Vec<SourceMetadataDocEntry>) -> Self {
        let token_ids = lexical_metadata_token_ids(&entries);
        let token_count = token_ids.len();

        // Phase 1 (parallel): build per-doc source docs and weights. Each doc
        // does its own tokenization + term-frequency HashMap + unique-token
        // sort, which is the expensive per-doc work; `unzip` preserves
        // doc-index order.
        let (doc_weights, source_docs): (Vec<usize>, Vec<InternedMetadataSourceDoc>) = entries
            .par_iter()
            .map(|entry| {
                let doc_weight = entry.contracts.len();
                let source_doc =
                    InternedMetadataSourceDoc::from_metadata_doc(&entry.doc, &token_ids);
                (doc_weight, source_doc)
            })
            .unzip();

        #[cfg(test)]
        let test_token_ids = token_ids
            .iter()
            .map(|(token, token_id)| ((*token).to_owned(), *token_id))
            .collect();
        drop(token_ids);
        drop(entries);

        // Phase 2: fill postings from the prebuilt source docs. This is plain
        // Vec pushes (no per-doc HashMap), so it stays serial and cheap.
        let mut postings = vec![Vec::new(); token_count];
        for (doc_index, doc) in source_docs.iter().enumerate() {
            let compact_doc_index = metadata_doc_index_from_usize(doc_index);
            for &token_id in &doc.unique_tokens {
                postings[token_id].push(compact_doc_index);
            }
        }
        // Phase 3 (parallel): sort + dedup each posting independently.
        postings.par_iter_mut().for_each(|indices| {
            indices.sort_unstable();
            indices.dedup();
        });
        let corpus =
            InternedMetadataCorpus::from_doc_weights(&doc_weights, &source_docs, token_count);
        drop(doc_weights);
        let prepared_docs = source_docs
            .par_iter()
            .map(|doc| PreparedInternedMetadataDoc::new(doc, &corpus))
            .collect::<Vec<_>>();
        let mut max_token_weights = vec![0.0f64; token_count];
        for doc in &prepared_docs {
            for &(token, weight) in &doc.token_weights {
                max_token_weights[token] = max_token_weights[token].max(weight);
            }
        }
        let postings = CompactMetadataPostings::from_nested(postings);
        let queries = source_docs
            .par_iter()
            .map(|doc| {
                PreparedInternedMetadataQuery::new(doc, &corpus, &max_token_weights, &postings)
            })
            .collect::<Vec<_>>();
        let doc_count = source_docs.len();
        let total_docs = corpus.total_docs;
        drop(source_docs);
        drop(corpus);
        drop(max_token_weights);
        let scoring = CompactMetadataScoring::from_nested(queries, prepared_docs);
        Self {
            doc_count,
            total_docs,
            scoring,
            postings,
            #[cfg(test)]
            token_ids: test_token_ids,
            #[cfg(test)]
            build_thread_count: rayon::current_num_threads(),
        }
    }

    #[cfg(test)]
    pub(super) fn token_id(&self, token: &str) -> Option<usize> {
        self.token_ids.get(token).copied()
    }
}
