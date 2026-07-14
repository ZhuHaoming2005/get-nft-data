use std::borrow::Cow;
use std::hash::BuildHasher;

use rayon::prelude::*;

use super::super::super::{AnalysisError, MetadataRecallMode};
#[cfg(test)]
use super::super::bm25::MetadataContentRecord;
use super::super::bm25::{
    compact_metadata_content_docs_share_token, compact_metadata_content_pair_score,
    CompactMetadataContentDocument, MetadataBm25Document,
};
use super::super::parse::metadata_document_from_json;
use super::super::{
    metadata_contract_index_to_usize, metadata_doc_index_from_usize, metadata_doc_index_to_usize,
    CompactContractTokens, MetadataContractIndex, MetadataData, MetadataDocIndex,
    METADATA_CONTENT_PARALLEL_MIN_RECORDS, METADATA_CONTENT_SCORE_BATCH_PAIRS, METADATA_THRESHOLD,
};

use super::*;

impl CompactMetadataContentGroupBuilder {
    pub(in super::super) fn vec_bytes_upper(len: usize, element_bytes: usize) -> usize {
        if len == 0 {
            0
        } else {
            len.saturating_mul(2)
                .saturating_add(4)
                .saturating_mul(element_bytes)
        }
    }

    pub(in super::super) fn atomized_memory_bytes(&self) -> usize {
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
            std::mem::size_of::<(u32, u32)>(),
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

    pub(in super::super) fn builder_memory_bytes(&self) -> usize {
        self.atomized_memory_bytes()
            .saturating_add(super::super::load::hash_table_allocation_for_len_upper(
                self.token_ids.len(),
                std::mem::size_of::<(String, u32)>(),
            ))
            .saturating_add(self.token_key_bytes)
            .saturating_add(super::super::load::hash_table_allocation_for_len_upper(
                self.atom_index_by_hash.len(),
                std::mem::size_of::<(u64, usize)>(),
            ))
            .saturating_add(Self::vec_bytes_upper(
                self.next_atom_with_same_hash.len(),
                std::mem::size_of::<usize>(),
            ))
            .saturating_add(super::super::load::hash_table_allocation_for_len_upper(
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

    pub(in super::super) fn scoring_peak_bytes(
        &self,
        scoring_workers: usize,
        recall_mode: MetadataRecallMode,
    ) -> usize {
        if self.atoms.is_empty() {
            return 0;
        }
        let sparse_template_entry_upper_bytes = std::mem::size_of::<MetadataDocIndex>()
            .saturating_add(std::mem::size_of::<u32>())
            .saturating_add(std::mem::size_of::<u64>());
        let content_candidate_index =
            Self::vec_bytes_upper(self.term_count, std::mem::size_of::<MetadataDocIndex>())
                .saturating_add(Self::vec_bytes_upper(
                    self.token_ids.len().saturating_add(1),
                    2usize.saturating_mul(std::mem::size_of::<u64>()),
                ));
        let template_candidate_index = Self::vec_bytes_upper(
            self.template_candidate_term_count,
            sparse_template_entry_upper_bytes,
        )
        .saturating_add(4usize.saturating_mul(sparse_template_entry_upper_bytes));
        let template_candidate_flat_build = Self::vec_bytes_upper(
            self.template_candidate_term_count,
            std::mem::size_of::<(u32, MetadataDocIndex)>(),
        );
        let uses_adaptive_index = self.atoms.len() > METADATA_DIRECT_ATOM_GROUP_SIZE;
        let conservative_candidate_index = if uses_adaptive_index
            && recall_mode == MetadataRecallMode::Conservative
            && self.atoms.len() >= METADATA_CONSERVATIVE_MIN_ATOMS
        {
            let posting_count = self.atoms.len().saturating_mul(
                2usize.saturating_mul(
                    METADATA_CONSERVATIVE_ANCHOR_COUNT
                        .saturating_add(METADATA_CONSERVATIVE_SIMHASH_BANDS),
                ),
            );
            let frequency_entry_count = self
                .term_count
                .saturating_add(self.template_candidate_term_count);
            2usize
                .saturating_mul(Self::vec_bytes_upper(
                    self.atoms.len(),
                    std::mem::size_of::<MetadataConservativeSketch>(),
                ))
                .saturating_add(Self::vec_bytes_upper(
                    posting_count,
                    std::mem::size_of::<(u32, MetadataDocIndex)>(),
                ))
                .saturating_add(Self::vec_bytes_upper(
                    posting_count,
                    std::mem::size_of::<MetadataDocIndex>()
                        .saturating_add(std::mem::size_of::<u32>())
                        .saturating_add(std::mem::size_of::<u64>()),
                ))
                .saturating_add(super::super::load::hash_table_allocation_for_len_upper(
                    frequency_entry_count,
                    std::mem::size_of::<(u32, u32)>(),
                ))
                .saturating_add(super::super::load::hash_table_allocation_for_len_upper(
                    frequency_entry_count,
                    std::mem::size_of::<(u32, MetadataConservativeTokenStats)>(),
                ))
                .saturating_add(Self::vec_bytes_upper(
                    self.atoms.len(),
                    2usize
                        .saturating_mul(std::mem::size_of::<usize>())
                        .saturating_add(2usize.saturating_mul(std::mem::size_of::<u8>()))
                        .saturating_add(std::mem::size_of::<u16>()),
                ))
                .saturating_add(3usize.saturating_mul(
                    super::super::load::hash_table_allocation_for_len_upper(
                        self.atoms.len(),
                        std::mem::size_of::<((usize, usize), u64)>(),
                    ),
                ))
        } else {
            0
        };
        let joint_candidate_index = if uses_adaptive_index
            && recall_mode == MetadataRecallMode::Conservative
            && self.atoms.len() >= METADATA_CONSERVATIVE_JOINT_MIN_ATOMS
        {
            let family_postings = self
                .atoms
                .len()
                .saturating_mul(METADATA_CONSERVATIVE_JOINT_BAND_FAMILIES);
            let family_offsets = METADATA_CONSERVATIVE_JOINT_BAND_FAMILIES
                .saturating_mul(METADATA_CONSERVATIVE_JOINT_BAND_BUCKETS.saturating_add(1));
            let build_cursors = METADATA_CONSERVATIVE_JOINT_BAND_FAMILIES
                .saturating_mul(METADATA_CONSERVATIVE_JOINT_BAND_BUCKETS);
            Self::vec_bytes_upper(family_postings, std::mem::size_of::<MetadataDocIndex>())
                .saturating_add(Self::vec_bytes_upper(
                    family_offsets,
                    std::mem::size_of::<u64>(),
                ))
                .saturating_add(Self::vec_bytes_upper(
                    build_cursors,
                    std::mem::size_of::<u64>(),
                ))
                .saturating_add(Self::vec_bytes_upper(
                    METADATA_CONSERVATIVE_JOINT_BAND_FAMILIES,
                    std::mem::size_of::<MetadataConservativeJointBandFamily>(),
                ))
        } else {
            0
        };
        let candidate_index = if uses_adaptive_index {
            content_candidate_index
                .saturating_add(template_candidate_index)
                .saturating_add(template_candidate_flat_build)
                .saturating_add(conservative_candidate_index)
                .saturating_add(joint_candidate_index)
        } else {
            0
        };
        let worker_count = scoring_workers.max(1);
        let fallback_bitmap_word_count = self.atoms.len().saturating_add(63) / 64;
        // The fallback token exclusion index is built from a temporary sorted
        // (token, atom) vector and then retained as sparse CSR postings. Both
        // representations can coexist while the conversion is in progress.
        let fallback_exclusion_index =
            if uses_adaptive_index && self.fallback_token_posting_count > 0 {
                Self::vec_bytes_upper(
                    self.fallback_token_posting_count,
                    std::mem::size_of::<(u32, MetadataDocIndex)>(),
                )
                .saturating_add(Self::vec_bytes_upper(
                    self.fallback_token_posting_count,
                    std::mem::size_of::<MetadataDocIndex>(),
                ))
                .saturating_add(Self::vec_bytes_upper(
                    self.fallback_token_posting_count,
                    std::mem::size_of::<u32>(),
                ))
                .saturating_add(Self::vec_bytes_upper(
                    self.fallback_token_posting_count.saturating_add(1),
                    std::mem::size_of::<u64>(),
                ))
            } else {
                0
            };
        let fallback_exclusion_scratch = if uses_adaptive_index {
            Self::vec_bytes_upper(fallback_bitmap_word_count, std::mem::size_of::<u64>())
                .saturating_add(Self::vec_bytes_upper(
                    fallback_bitmap_word_count,
                    std::mem::size_of::<usize>(),
                ))
                .saturating_mul(worker_count)
        } else {
            0
        };
        // Full-work planning retains one cost estimate and one difficult-first
        // atom index per left. This is the intentional space-for-time tradeoff
        // that avoids re-estimation and makes progress proportional to work.
        let difficult_first_order = if uses_adaptive_index {
            Self::vec_bytes_upper(self.atoms.len(), std::mem::size_of::<u64>()).saturating_add(
                Self::vec_bytes_upper(self.atoms.len(), std::mem::size_of::<usize>()),
            )
        } else {
            0
        };
        let exact_rescue_mask = if uses_adaptive_index
            && recall_mode == MetadataRecallMode::Conservative
            && self.atoms.len() >= METADATA_CONSERVATIVE_MIN_ATOMS
        {
            Self::vec_bytes_upper(self.atoms.len(), std::mem::size_of::<bool>())
        } else {
            0
        };
        let candidate_scratch = if uses_adaptive_index {
            self.atoms
                .len()
                .saturating_mul(
                    2usize
                        .saturating_mul(std::mem::size_of::<u16>())
                        .saturating_add(3 * std::mem::size_of::<MetadataDocIndex>()),
                )
                .saturating_mul(worker_count)
        } else {
            0
        };
        // Each parallel wave retains its filtered right-hand candidates until
        // the serial DSU consumer applies them in deterministic difficult-first
        // order.
        // This deliberately spends bounded memory to parallelize the dominant
        // posting scans without changing union or scoring order.
        let candidate_wave = if uses_adaptive_index {
            Self::vec_bytes_upper(self.atoms.len(), std::mem::size_of::<MetadataDocIndex>())
                .saturating_mul(worker_count)
                .saturating_mul(METADATA_PARALLEL_LEFT_WAVE_MULTIPLIER)
                .saturating_mul(2)
        } else {
            0
        };
        let pair_batch_capacity = if uses_adaptive_index {
            METADATA_CONTENT_SCORE_BATCH_PAIRS
        } else {
            usize::from(self.atoms.len() == METADATA_DIRECT_ATOM_GROUP_SIZE)
        };
        let pair_batch_bytes = 2usize
            .saturating_mul(std::mem::size_of::<(usize, MetadataDocIndex)>())
            .saturating_add(std::mem::size_of::<(u64, usize)>())
            .saturating_add(std::mem::size_of::<u64>())
            .saturating_add(
                2usize.saturating_mul(std::mem::size_of::<MetadataTemplatePairEvaluation>()),
            );
        let pair_batches = pair_batch_capacity.saturating_mul(pair_batch_bytes);
        // A parallel fold and its reduce-side accumulator can coexist for
        // every worker, so reserve both fixed-size template caches.
        let template_cache_count = if uses_adaptive_index {
            worker_count.saturating_mul(2)
        } else {
            pair_batch_capacity
        };
        let template_score_caches =
            template_cache_count.saturating_mul(MetadataTemplateScoreCache::memory_bytes());
        let union_scratch = self.member_count.saturating_mul(
            2usize
                .saturating_mul(std::mem::size_of::<usize>())
                .saturating_add(std::mem::size_of::<MetadataContractIndex>()),
        );
        let calibration_graph_scratch = if uses_adaptive_index
            && recall_mode == MetadataRecallMode::Conservative
            && self.atoms.len() >= METADATA_CONSERVATIVE_MIN_ATOMS
        {
            let atom_count = self.atoms.len();
            let chain_count = self
                .atoms
                .iter()
                .map(|atom| atom.chain_index)
                .max()
                .map_or(0, |maximum| maximum.saturating_add(1));
            let stratum_count_upper = atom_count.min(chain_count.saturating_mul(64));
            let retained_sample_candidates = atom_count.min(
                METADATA_CONSERVATIVE_CALIBRATION_MAX_LEFTS.saturating_add(stratum_count_upper),
            );
            let calibration_vectors = Self::vec_bytes_upper(
                atom_count,
                2usize
                    .saturating_mul(std::mem::size_of::<bool>())
                    .saturating_add(std::mem::size_of::<u16>())
                    .saturating_add(2usize.saturating_mul(
                        std::mem::size_of::<usize>().saturating_add(std::mem::size_of::<u8>()),
                    ))
                    .saturating_add(std::mem::size_of::<u64>()),
            );
            let calibration_maps =
                4usize.saturating_mul(super::super::load::hash_table_allocation_for_len_upper(
                    atom_count,
                    std::mem::size_of::<((usize, usize), u128)>(),
                ));
            let calibration_samples = 2usize.saturating_mul(Self::vec_bytes_upper(
                METADATA_CONSERVATIVE_CALIBRATION_MAX_LEFTS.min(atom_count),
                std::mem::size_of::<MetadataCalibrationSample>(),
            ));
            let calibration_reservoir = 2usize.saturating_mul(Self::vec_bytes_upper(
                retained_sample_candidates,
                std::mem::size_of::<(u64, MetadataCalibrationWorkItem)>(),
            ));
            let calibration_strata = stratum_count_upper.saturating_mul(
                std::mem::size_of::<(
                    u64,
                    Option<(u64, MetadataCalibrationWorkItem)>,
                    Vec<(u64, MetadataCalibrationWorkItem)>,
                )>()
                .saturating_add(4usize.saturating_mul(std::mem::size_of::<usize>())),
            );
            calibration_vectors
                .saturating_add(calibration_maps)
                .saturating_add(calibration_samples)
                .saturating_add(calibration_reservoir)
                .saturating_add(calibration_strata)
        } else {
            0
        };
        let peak = self
            .atomized_memory_bytes()
            .saturating_add(candidate_index)
            .saturating_add(fallback_exclusion_index)
            .saturating_add(fallback_exclusion_scratch)
            .saturating_add(difficult_first_order)
            .saturating_add(exact_rescue_mask)
            .saturating_add(candidate_scratch)
            .saturating_add(candidate_wave)
            .saturating_add(pair_batches)
            .saturating_add(template_score_caches)
            .saturating_add(union_scratch)
            .saturating_add(calibration_graph_scratch);
        peak.saturating_add(peak.saturating_div(4))
    }

    pub(in super::super) fn ensure_within_memory_budget(
        &self,
        raw_parse_reserve_bytes: usize,
        maximum_bytes: usize,
        scoring_workers: usize,
        recall_mode: MetadataRecallMode,
    ) -> Result<(), AnalysisError> {
        let build_peak = self
            .builder_memory_bytes()
            .saturating_add(raw_parse_reserve_bytes);
        let peak = build_peak.max(self.scoring_peak_bytes(scoring_workers, recall_mode));
        if peak > maximum_bytes {
            return Err(AnalysisError::InvalidData(format!(
                "metadata content working set needs about {}, exceeding remaining analysis budget {}",
                super::super::format_byte_size(peak),
                super::super::format_byte_size(maximum_bytes)
            )));
        }
        Ok(())
    }

    pub(in super::super) fn push_document(
        &mut self,
        contract_index: MetadataContractIndex,
        document: &MetadataBm25Document,
        data: &MetadataData,
        contract_tokens: Option<&CompactContractTokens>,
    ) {
        let mut terms = Vec::with_capacity(document.unique_len());
        for (token, term_frequency) in document.terms() {
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
            terms.push((
                token_id,
                u32::try_from(*term_frequency)
                    .expect("metadata content term frequency exceeds u32"),
            ));
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
            let template_doc_index = metadata_doc_index_to_usize(contract.template_doc_index);
            self.template_candidate_term_count = self
                .template_candidate_term_count
                .saturating_add(
                    data.metadata_index
                        .scoring
                        .query_tokens(template_doc_index)
                        .len(),
                )
                .saturating_add(
                    data.metadata_index
                        .scoring
                        .candidate_tokens(template_doc_index)
                        .len(),
                );
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

    pub(in super::super) fn push_fallback_token_group(
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
        self.fallback_token_posting_count = self
            .fallback_token_posting_count
            .saturating_add(tokens.len());
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

    pub(in super::super) fn into_atomized_parts(
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
    pub(in super::super) fn raw_parse_reserve_bytes(&self) -> usize {
        let raw_bytes = self
            .raw_records
            .capacity()
            .saturating_mul(std::mem::size_of::<(MetadataContractIndex, String)>())
            .saturating_add(self.raw_payload_bytes);
        // Use the same adversarial high-cardinality estimate as the initial
        // metadata loader. JSON normalization, token strings, term-frequency
        // maps and Rayon result buffers coexist before online atomization.
        super::super::load::metadata_uncached_parse_transient_bytes(raw_bytes, 0)
    }

    pub(in super::super) fn parallel_prepare_bytes(&self) -> usize {
        self.compact
            .builder_memory_bytes()
            .saturating_add(self.raw_parse_reserve_bytes())
    }

    pub(in super::super) fn record_count(&self) -> usize {
        self.raw_record_count
    }

    pub(in super::super) fn reserve_raw_record(&mut self) -> Result<(), AnalysisError> {
        self.raw_records.try_reserve(1).map_err(|_| {
            AnalysisError::InvalidData(
                "unable to reserve bounded metadata raw-group chunk".to_string(),
            )
        })
    }

    pub(in super::super) fn projected_raw_parse_reserve_bytes(
        &self,
        candidate_payload_bytes: usize,
    ) -> usize {
        let raw_bytes = self
            .raw_records
            .capacity()
            .saturating_mul(std::mem::size_of::<(MetadataContractIndex, String)>())
            .saturating_add(self.raw_payload_bytes)
            .saturating_add(candidate_payload_bytes);
        super::super::load::metadata_uncached_parse_transient_bytes(raw_bytes, 0)
    }

    #[cfg(test)]
    pub(in super::super) fn push_raw(
        &mut self,
        contract_index: MetadataContractIndex,
        metadata_json: String,
        context: &MetadataContentUnionContext<'_>,
    ) {
        self.push_raw_with_budget(contract_index, metadata_json, context, usize::MAX)
            .expect("unbounded metadata test group must fit memory");
    }

    pub(in super::super) fn push_raw_with_budget(
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
                .ensure_within_memory_budget(
                    projected_reserve,
                    maximum_bytes,
                    context.pool.current_num_threads(),
                    context.recall_mode,
                )
                .is_err()
        {
            self.flush_raw(context, maximum_bytes)?;
            self.reserve_raw_record()?;
        }
        self.compact.ensure_within_memory_budget(
            self.projected_raw_parse_reserve_bytes(candidate_payload_bytes),
            maximum_bytes,
            context.pool.current_num_threads(),
            context.recall_mode,
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

    pub(in super::super) fn push_loaded_representative_with_budget(
        &mut self,
        contract_index: MetadataContractIndex,
        context: &MetadataContentUnionContext<'_>,
        maximum_bytes: usize,
    ) -> Result<(), AnalysisError> {
        self.raw_record_count = self.raw_record_count.saturating_add(1);
        if let Some(document) = context.data.contracts
            [metadata_contract_index_to_usize(contract_index)]
        .content_doc
        .as_deref()
        {
            self.compact
                .push_document(contract_index, document, context.data, None);
            self.compact.ensure_within_memory_budget(
                self.projected_raw_parse_reserve_bytes(0),
                maximum_bytes,
                context.pool.current_num_threads(),
                context.recall_mode,
            )?;
        }
        Ok(())
    }

    pub(in super::super) fn flush_raw(
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
        self.compact.ensure_within_memory_budget(
            0,
            maximum_bytes,
            context.pool.current_num_threads(),
            context.recall_mode,
        )?;
        Ok(())
    }

    #[cfg(test)]
    pub(in super::super) fn union(
        self,
        context: &MetadataContentUnionContext<'_>,
        state: &mut MetadataUnionState,
    ) -> MetadataContentUnionStats {
        let template_cache_pool = MetadataTemplateScoreCachePool::default();
        self.union_with_budget(
            context,
            state,
            usize::MAX,
            &template_cache_pool,
            MetadataRecallMode::Exact,
            None,
        )
        .expect("unbounded metadata test group must fit memory")
    }

    pub(in super::super) fn union_with_budget(
        mut self,
        context: &MetadataContentUnionContext<'_>,
        state: &mut MetadataUnionState,
        maximum_bytes: usize,
        template_cache_pool: &MetadataTemplateScoreCachePool,
        recall_mode: MetadataRecallMode,
        progress: Option<MetadataSharedTokenGroupProgress<'_>>,
    ) -> Result<MetadataContentUnionStats, AnalysisError> {
        if self.raw_record_count < 2 {
            return Ok(MetadataContentUnionStats::default());
        }
        self.flush_raw(context, maximum_bytes)?;
        drop(self.raw_records);
        self.compact.ensure_within_memory_budget(
            0,
            maximum_bytes,
            context.pool.current_num_threads(),
            recall_mode,
        )?;
        let (atoms, docs) = self.compact.into_atomized_parts();
        union_metadata_shared_token_atom_core(
            atoms,
            &docs,
            context,
            state,
            template_cache_pool,
            recall_mode,
            progress,
        )
    }

    #[cfg(test)]
    pub(in super::super) fn raw_buffer_len(&self) -> usize {
        self.raw_records.len()
    }

    #[cfg(test)]
    pub(in super::super) fn max_raw_buffer_len(&self) -> usize {
        self.max_raw_buffer_len
    }

    #[cfg(test)]
    pub(in super::super) fn compact_doc_count(&self) -> usize {
        self.compact.docs.len()
    }

    #[cfg(test)]
    pub(in super::super) fn compact_member_count(&self) -> usize {
        self.compact
            .atoms
            .iter()
            .map(|atom| atom.members.len())
            .sum()
    }
}

pub(in super::super) fn prepare_metadata_token_group_batch(
    groups: &mut Vec<MetadataRawTokenGroup>,
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
    maximum_working_bytes: usize,
    template_cache_pool: &MetadataTemplateScoreCachePool,
    recall_mode: MetadataRecallMode,
) -> Result<MetadataContentUnionStats, AnalysisError> {
    if groups.len() > 1 {
        context.pool.install(|| {
            groups
                .par_iter_mut()
                .try_for_each(|group| group.flush_raw(context, maximum_working_bytes))
        })?;
    }
    let mut remaining_prepared_bytes = groups.iter().fold(0usize, |bytes, group| {
        bytes.saturating_add(group.parallel_prepare_bytes())
    });
    if remaining_prepared_bytes > maximum_working_bytes {
        return Err(AnalysisError::InvalidData(format!(
            "prepared metadata token groups need about {}, exceeding remaining analysis budget {}",
            super::super::format_byte_size(remaining_prepared_bytes),
            super::super::format_byte_size(maximum_working_bytes)
        )));
    }
    let mut stats = MetadataContentUnionStats::default();
    for group in groups.drain(..) {
        remaining_prepared_bytes =
            remaining_prepared_bytes.saturating_sub(group.parallel_prepare_bytes());
        let group_working_bytes = maximum_working_bytes.saturating_sub(remaining_prepared_bytes);
        stats.accumulate(group.union_with_budget(
            context,
            state,
            group_working_bytes,
            template_cache_pool,
            recall_mode,
            None,
        )?);
    }
    Ok(stats)
}

pub(in super::super) fn lowest_common_metadata_token(left: &[u32], right: &[u32]) -> Option<u32> {
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

pub(in super::super) fn metadata_content_document<'a>(
    data: &'a MetadataData,
    raw: &str,
) -> Option<Cow<'a, MetadataBm25Document>> {
    data.reused_documents
        .get(raw)
        .and_then(|cached| cached.content.as_deref())
        .map(Cow::Borrowed)
        .or_else(|| {
            MetadataBm25Document::from_normalized_text(&metadata_document_from_json(raw))
                .map(Cow::Owned)
        })
}

pub(in super::super) fn metadata_fallback_token_group_tokens<'a>(
    group: &MetadataFallbackTokenGroup,
    contract_tokens: &'a CompactContractTokens,
) -> &'a [u32] {
    let representative = metadata_contract_index_to_usize(group.members[0]);
    &contract_tokens[representative]
}

pub(in super::super) fn metadata_fallback_token_groups_are_disjoint(
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

impl MetadataFallbackTokenExclusionIndex {
    pub(in super::super) fn from_atoms(
        atoms: &[MetadataContentAtom],
        contract_tokens: &CompactContractTokens,
    ) -> Self {
        let mut entries = Vec::new();
        for (atom_index, atom) in atoms.iter().enumerate() {
            let [group] = atom.fallback_token_groups.as_slice() else {
                continue;
            };
            let atom_index = metadata_doc_index_from_usize(atom_index);
            entries.extend(
                metadata_fallback_token_group_tokens(group, contract_tokens)
                    .iter()
                    .copied()
                    .map(|token| (token, atom_index)),
            );
        }
        entries.par_sort_unstable();
        entries.dedup();
        Self {
            postings: MetadataSparseCandidatePostings::from_sorted_entries(entries),
        }
    }

    pub(in super::super) fn prepare_left(
        &self,
        left: usize,
        atoms: &[MetadataContentAtom],
        contract_tokens: &CompactContractTokens,
        scratch: &mut MetadataFallbackTokenExclusionScratch,
    ) -> usize {
        scratch.clear();
        let [group] = atoms[left].fallback_token_groups.as_slice() else {
            return 0;
        };
        scratch.prepared_single_group = true;
        let compact_left = metadata_doc_index_from_usize(left);
        let mut posting_visits = 0usize;
        for &token in metadata_fallback_token_group_tokens(group, contract_tokens) {
            let range = self.postings.posting_range_after(token, compact_left);
            posting_visits = posting_visits.saturating_add(range.end.saturating_sub(range.start));
            for &right in &self.postings.posting_atoms[range.start..range.end] {
                scratch.insert(metadata_doc_index_to_usize(right));
            }
        }
        posting_visits
    }

    pub(in super::super) fn estimate_left_suffix_visits(
        &self,
        left: usize,
        atoms: &[MetadataContentAtom],
        contract_tokens: &CompactContractTokens,
    ) -> usize {
        let [group] = atoms[left].fallback_token_groups.as_slice() else {
            return 0;
        };
        let compact_left = metadata_doc_index_from_usize(left);
        metadata_fallback_token_group_tokens(group, contract_tokens)
            .iter()
            .copied()
            .map(|token| {
                let range = self.postings.posting_range_after(token, compact_left);
                range.end.saturating_sub(range.start)
            })
            .fold(0usize, usize::saturating_add)
    }

    pub(in super::super) fn prepare_left_if_cheaper(
        &self,
        left: usize,
        candidates: &[MetadataDocIndex],
        atoms: &[MetadataContentAtom],
        contract_tokens: &CompactContractTokens,
        scratch: &mut MetadataFallbackTokenExclusionScratch,
    ) -> usize {
        scratch.clear();
        let [left_group] = atoms[left].fallback_token_groups.as_slice() else {
            return 0;
        };
        let left_token_count =
            metadata_fallback_token_group_tokens(left_group, contract_tokens).len();
        let scalar_token_visits = candidates
            .iter()
            .copied()
            .filter_map(|right| {
                let [right_group] = atoms[metadata_doc_index_to_usize(right)]
                    .fallback_token_groups
                    .as_slice()
                else {
                    return None;
                };
                Some(left_token_count.saturating_add(
                    metadata_fallback_token_group_tokens(right_group, contract_tokens).len(),
                ))
            })
            .fold(0usize, usize::saturating_add);
        let posting_visits = self.estimate_left_suffix_visits(left, atoms, contract_tokens);
        if posting_visits >= scalar_token_visits {
            return 0;
        }
        self.prepare_left(left, atoms, contract_tokens, scratch)
    }

    pub(in super::super) fn atoms_have_disjoint_token_groups(
        &self,
        left: usize,
        right: usize,
        atoms: &[MetadataContentAtom],
        contract_tokens: &CompactContractTokens,
        scratch: &MetadataFallbackTokenExclusionScratch,
    ) -> bool {
        if scratch.prepared_single_group && atoms[right].fallback_token_groups.len() == 1 {
            return !scratch.contains(right);
        }
        metadata_fallback_atoms_have_disjoint_token_groups(
            &atoms[left],
            &atoms[right],
            contract_tokens,
        )
    }
}

impl MetadataFallbackTokenExclusionScratch {
    pub(in super::super) fn new(atom_count: usize) -> Self {
        Self {
            words: vec![0; atom_count.saturating_add(63) / 64],
            touched_words: Vec::new(),
            prepared_single_group: false,
        }
    }

    fn clear(&mut self) {
        for word_index in self.touched_words.drain(..) {
            self.words[word_index] = 0;
        }
        self.prepared_single_group = false;
    }

    fn insert(&mut self, atom_index: usize) {
        let word_index = atom_index / 64;
        let word = &mut self.words[word_index];
        if *word == 0 {
            self.touched_words.push(word_index);
        }
        *word |= 1u64 << (atom_index % 64);
    }

    fn contains(&self, atom_index: usize) -> bool {
        self.words
            .get(atom_index / 64)
            .is_some_and(|word| word & (1u64 << (atom_index % 64)) != 0)
    }
}

pub(in super::super) fn apply_metadata_fallback_atom_internal_unions(
    atom: &MetadataContentAtom,
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
) {
    for group in &atom.fallback_token_groups {
        if metadata_fallback_token_group_tokens(group, context.contract_tokens).is_empty() {
            apply_metadata_same_chain_group_union(
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

pub(in super::super) fn metadata_fallback_atoms_have_disjoint_token_groups(
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

pub(in super::super) fn apply_metadata_fallback_atom_pair_union(
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

pub(in super::super) fn metadata_content_pair_matches(
    left: &CompactMetadataContentDocument,
    right: &CompactMetadataContentDocument,
    threshold: f64,
) -> bool {
    compact_metadata_content_pair_score(left, right) >= threshold
}

#[cfg(test)]
pub(in super::super) fn build_metadata_content_atoms(
    records: &[MetadataContentRecord],
    compact_docs: &[CompactMetadataContentDocument],
    data: &MetadataData,
) -> Vec<MetadataContentAtom> {
    build_metadata_content_atoms_core(records.len(), compact_docs, data, |record_index| {
        records[record_index].contract_index
    })
}

#[cfg(test)]
pub(in super::super) fn build_metadata_content_atoms_core(
    record_count: usize,
    compact_docs: &[CompactMetadataContentDocument],
    data: &MetadataData,
    mut contract_index_at: impl FnMut(usize) -> MetadataContractIndex,
) -> Vec<MetadataContentAtom> {
    debug_assert_eq!(record_count, compact_docs.len());
    let mut atom_index_by_key = HashMap::<(usize, MetadataDocIndex, &[(u32, u32)]), usize>::new();
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
pub(in super::super) fn build_metadata_fallback_atoms(
    records: &[MetadataContentRecord],
    compact_docs: &[CompactMetadataContentDocument],
    data: &MetadataData,
    contract_tokens: &CompactContractTokens,
) -> Vec<MetadataContentAtom> {
    let mut atom_index_by_key = HashMap::<(usize, MetadataDocIndex, &[(u32, u32)]), usize>::new();
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

pub(in super::super) fn metadata_content_atom_pair_matches(
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

pub(in super::super) fn metadata_content_atoms_share_token(
    left: usize,
    right: MetadataDocIndex,
    atoms: &[MetadataContentAtom],
    compact_docs: &[CompactMetadataContentDocument],
) -> bool {
    let left_record = metadata_doc_index_to_usize(atoms[left].representative_record_index);
    let right_record = metadata_doc_index_to_usize(
        atoms[metadata_doc_index_to_usize(right)].representative_record_index,
    );
    compact_metadata_content_docs_share_token(
        &compact_docs[left_record],
        &compact_docs[right_record],
    )
}

pub(in super::super) fn metadata_prefix_intersects_sorted_terms(
    prefix: &[u32],
    terms: &[u32],
) -> bool {
    prefix
        .iter()
        .any(|token| terms.binary_search(token).is_ok())
}

pub(in super::super) fn metadata_template_atoms_share_safe_prefix(
    left: usize,
    right: MetadataDocIndex,
    atoms: &[MetadataContentAtom],
    compatibility: MetadataTemplateCompatibility<'_>,
) -> bool {
    let Some(scoring) = compatibility.scoring() else {
        // Test-only precomputed compatibility performs the exact lookup in the
        // scoring batch and has no compact prefix arrays.
        return true;
    };
    let left_template = metadata_doc_index_to_usize(atoms[left].template_doc_index);
    let right_template =
        metadata_doc_index_to_usize(atoms[metadata_doc_index_to_usize(right)].template_doc_index);
    metadata_prefix_intersects_sorted_terms(
        scoring.candidate_tokens(left_template),
        scoring.query_tokens(right_template),
    ) || metadata_prefix_intersects_sorted_terms(
        scoring.candidate_tokens(right_template),
        scoring.query_tokens(left_template),
    )
}

pub(in super::super) fn metadata_candidate_intersects_both_dimensions(
    basis: MetadataLocalCandidateBasis,
    left: usize,
    right: MetadataDocIndex,
    atoms: &[MetadataContentAtom],
    compact_docs: &[CompactMetadataContentDocument],
    compatibility: MetadataTemplateCompatibility<'_>,
) -> bool {
    match basis {
        MetadataLocalCandidateBasis::Template => {
            metadata_content_atoms_share_token(left, right, atoms, compact_docs)
        }
        MetadataLocalCandidateBasis::Content => {
            metadata_template_atoms_share_safe_prefix(left, right, atoms, compatibility)
        }
        MetadataLocalCandidateBasis::Intersection => true,
        MetadataLocalCandidateBasis::ConservativeIntersection => {
            metadata_content_atoms_share_token(left, right, atoms, compact_docs)
                && metadata_template_atoms_share_safe_prefix(left, right, atoms, compatibility)
        }
    }
}
