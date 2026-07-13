use std::collections::HashMap;

use rayon::prelude::*;

use super::super::super::UnionFind;
use super::super::bm25::{CompactMetadataContentDocument, CompactMetadataScoring};
use super::super::{
    metadata_doc_index_from_usize, metadata_doc_index_to_usize, MetadataDocIndex,
    METADATA_CONTENT_SCORE_BATCH_PAIRS,
};

use super::*;

pub(in super::super) fn insert_metadata_conservative_anchor(
    anchors: &mut [(usize, u32); METADATA_CONSERVATIVE_ANCHOR_COUNT],
    anchor_len: &mut usize,
    candidate: (usize, u32),
) {
    if *anchor_len < anchors.len() {
        anchors[*anchor_len] = candidate;
        *anchor_len += 1;
    } else if candidate >= anchors[*anchor_len - 1] {
        return;
    } else {
        anchors[*anchor_len - 1] = candidate;
    }
    let mut index = (*anchor_len).saturating_sub(1);
    while index > 0 && anchors[index] < anchors[index - 1] {
        anchors.swap(index, index - 1);
        index -= 1;
    }
}

impl MetadataRecallCalibrationStats {
    pub(in super::super) fn requires_exact_fallback(&self) -> bool {
        let contract_drift_exceeded = self.exact_duplicate_contract_members > 0
            && self.missed_duplicate_contract_members.saturating_mul(1_000)
                > self
                    .exact_duplicate_contract_members
                    .saturating_mul(METADATA_CONSERVATIVE_CONTRACT_DRIFT_PER_MILLE);
        let component_drift_exceeded = self.exact_component_members > 0
            && self.shifted_component_members.saturating_mul(1_000)
                > self
                    .exact_component_members
                    .saturating_mul(METADATA_CONSERVATIVE_COMPONENT_DRIFT_PER_MILLE);
        contract_drift_exceeded || component_drift_exceeded
    }

    pub(in super::super) fn accumulate(&mut self, other: Self) {
        self.sampled_left_atoms = self
            .sampled_left_atoms
            .saturating_add(other.sampled_left_atoms);
        self.exact_candidate_pairs = self
            .exact_candidate_pairs
            .saturating_add(other.exact_candidate_pairs);
        self.conservative_candidate_pairs = self
            .conservative_candidate_pairs
            .saturating_add(other.conservative_candidate_pairs);
        self.exact_matched_pairs = self
            .exact_matched_pairs
            .saturating_add(other.exact_matched_pairs);
        self.missed_matched_pairs = self
            .missed_matched_pairs
            .saturating_add(other.missed_matched_pairs);
        self.exact_duplicate_contract_members = self
            .exact_duplicate_contract_members
            .saturating_add(other.exact_duplicate_contract_members);
        self.missed_duplicate_contract_members = self
            .missed_duplicate_contract_members
            .saturating_add(other.missed_duplicate_contract_members);
        self.exact_component_members = self
            .exact_component_members
            .saturating_add(other.exact_component_members);
        self.shifted_component_members = self
            .shifted_component_members
            .saturating_add(other.shifted_component_members);
    }
}

pub(in super::super) fn stable_metadata_recall_token_hash(token: u32) -> u64 {
    let mut value = u64::from(token).wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

pub(in super::super) fn metadata_recall_simhash_band_key(simhash: u64, band_index: usize) -> u32 {
    let shift = band_index.saturating_mul(METADATA_CONSERVATIVE_SIMHASH_BAND_BITS);
    let value = ((simhash >> shift) & 0xff) as u32;
    (band_index as u32) << METADATA_CONSERVATIVE_SIMHASH_BAND_BITS | value
}

impl MetadataConservativeDimensionIndex {
    pub(in super::super) fn from_token_visitor(
        atom_count: usize,
        parallel: bool,
        visit_tokens: impl Fn(usize, &mut dyn FnMut(u32)),
    ) -> Self {
        let mut document_frequencies = HashMap::<u32, u32>::new();
        for atom_index in 0..atom_count {
            visit_tokens(atom_index, &mut |token| {
                let frequency = document_frequencies.entry(token).or_default();
                *frequency = frequency.saturating_add(1);
            });
        }
        let total_documents = atom_count.max(1) as f64;
        let token_stats = document_frequencies
            .into_iter()
            .map(|(token, document_frequency)| {
                let document_frequency = document_frequency as usize;
                let high_frequency = atom_count >= METADATA_CONSERVATIVE_HIGH_FREQUENCY_MIN_DOCS
                    && document_frequency
                        .saturating_mul(METADATA_CONSERVATIVE_HIGH_FREQUENCY_DIVISOR)
                        > atom_count;
                (
                    token,
                    MetadataConservativeTokenStats {
                        document_frequency,
                        idf: ((total_documents + 1.0) / (document_frequency as f64 + 0.5)).ln(),
                        hash: stable_metadata_recall_token_hash(token),
                        anchor_eligible: !high_frequency,
                    },
                )
            })
            .collect::<HashMap<_, _>>();

        let mut sketches = Vec::with_capacity(atom_count);
        let mut anchor_entries = Vec::new();
        let mut band_entries =
            Vec::with_capacity(atom_count.saturating_mul(METADATA_CONSERVATIVE_SIMHASH_BANDS));
        for atom_index in 0..atom_count {
            let mut weights = [0.0f64; 64];
            let mut anchors = [(usize::MAX, u32::MAX); METADATA_CONSERVATIVE_ANCHOR_COUNT];
            let mut anchor_len = 0usize;
            let mut has_terms = false;
            visit_tokens(atom_index, &mut |token| {
                has_terms = true;
                let stats = &token_stats[&token];
                if stats.anchor_eligible {
                    insert_metadata_conservative_anchor(
                        &mut anchors,
                        &mut anchor_len,
                        (stats.document_frequency, token),
                    );
                }
                for (bit, weight) in weights.iter_mut().enumerate() {
                    if (stats.hash >> bit) & 1 == 1 {
                        *weight += stats.idf;
                    } else {
                        *weight -= stats.idf;
                    }
                }
            });
            let mut simhash = 0u64;
            for (bit, weight) in weights.into_iter().enumerate() {
                if weight >= 0.0 {
                    simhash |= 1u64 << bit;
                }
            }
            let mut anchor_values = [0u32; METADATA_CONSERVATIVE_ANCHOR_COUNT];
            for (output, &(_, token)) in anchor_values.iter_mut().zip(&anchors[..anchor_len]) {
                *output = token;
            }
            anchor_values[..anchor_len].sort_unstable();
            let compact_atom_index = metadata_doc_index_from_usize(atom_index);
            anchor_entries.extend(
                anchor_values[..anchor_len]
                    .iter()
                    .map(|&token| (token, compact_atom_index)),
            );
            if has_terms {
                band_entries.extend((0..METADATA_CONSERVATIVE_SIMHASH_BANDS).map(|band_index| {
                    (
                        metadata_recall_simhash_band_key(simhash, band_index),
                        compact_atom_index,
                    )
                }));
            }
            sketches.push(MetadataConservativeSketch {
                simhash,
                anchors: anchor_values,
                anchor_len: anchor_len as u8,
                has_terms,
            });
        }
        if parallel {
            anchor_entries.par_sort_unstable();
        } else {
            anchor_entries.sort_unstable();
        }
        Self {
            sketches,
            anchor_postings: MetadataSparseCandidatePostings::from_sorted_entries(anchor_entries),
            simhash_band_postings: MetadataSparseCandidatePostings::from_bounded_unsorted_entries(
                band_entries,
                METADATA_CONSERVATIVE_SIMHASH_BANDS << METADATA_CONSERVATIVE_SIMHASH_BAND_BITS,
            ),
        }
    }

    pub(in super::super) fn from_content_docs(
        docs: &[CompactMetadataContentDocument],
        atoms: &[MetadataContentAtom],
        parallel: bool,
    ) -> Self {
        Self::from_token_visitor(atoms.len(), parallel, |atom_index, visitor| {
            let record_index =
                metadata_doc_index_to_usize(atoms[atom_index].representative_record_index);
            for &(token, _) in &docs[record_index].terms {
                visitor(token);
            }
        })
    }

    pub(in super::super) fn from_template_docs(
        scoring: &CompactMetadataScoring,
        atoms: &[MetadataContentAtom],
        parallel: bool,
    ) -> Self {
        Self::from_token_visitor(atoms.len(), parallel, |atom_index, visitor| {
            let template_index = metadata_doc_index_to_usize(atoms[atom_index].template_doc_index);
            for &token in scoring.query_tokens(template_index) {
                visitor(token);
            }
        })
    }

    pub(in super::super) fn append_candidates_after(
        &self,
        atom_index: usize,
        scratch: &mut MetadataCandidateScratch,
    ) {
        let compact_atom_index = metadata_doc_index_from_usize(atom_index);
        let sketch = &self.sketches[atom_index];
        for &anchor in &sketch.anchors[..usize::from(sketch.anchor_len)] {
            let range = self
                .anchor_postings
                .posting_range_after(anchor, compact_atom_index);
            for &right in &self.anchor_postings.posting_atoms[range.start..range.end] {
                scratch.push_once(right);
            }
        }
        if sketch.has_terms {
            for band_index in 0..METADATA_CONSERVATIVE_SIMHASH_BANDS {
                let key = metadata_recall_simhash_band_key(sketch.simhash, band_index);
                let range = self
                    .simhash_band_postings
                    .posting_range_after(key, compact_atom_index);
                for &right in &self.simhash_band_postings.posting_atoms[range.start..range.end] {
                    scratch.push_once(right);
                }
            }
        }
    }

    pub(in super::super) fn matches(&self, left: usize, right: usize) -> bool {
        let left = &self.sketches[left];
        let right = &self.sketches[right];
        if !left.has_terms || !right.has_terms {
            return false;
        }
        let shared_anchor = lowest_common_metadata_token(
            &left.anchors[..usize::from(left.anchor_len)],
            &right.anchors[..usize::from(right.anchor_len)],
        )
        .is_some();
        shared_anchor
            || (left.simhash ^ right.simhash).count_ones()
                <= METADATA_CONSERVATIVE_SIMHASH_HAMMING_THRESHOLD
    }

    #[cfg(test)]
    pub(in super::super) fn memory_bytes(&self) -> usize {
        self.sketches
            .capacity()
            .saturating_mul(std::mem::size_of::<MetadataConservativeSketch>())
            .saturating_add(
                self.anchor_postings
                    .token_ids
                    .capacity()
                    .saturating_mul(std::mem::size_of::<u32>()),
            )
            .saturating_add(
                self.anchor_postings
                    .posting_offsets
                    .capacity()
                    .saturating_mul(std::mem::size_of::<u64>()),
            )
            .saturating_add(
                self.anchor_postings
                    .posting_atoms
                    .capacity()
                    .saturating_mul(std::mem::size_of::<MetadataDocIndex>()),
            )
            .saturating_add(
                self.simhash_band_postings
                    .token_ids
                    .capacity()
                    .saturating_mul(std::mem::size_of::<u32>()),
            )
            .saturating_add(
                self.simhash_band_postings
                    .posting_offsets
                    .capacity()
                    .saturating_mul(std::mem::size_of::<u64>()),
            )
            .saturating_add(
                self.simhash_band_postings
                    .posting_atoms
                    .capacity()
                    .saturating_mul(std::mem::size_of::<MetadataDocIndex>()),
            )
    }
}

pub(in super::super) fn metadata_conservative_calibration_lefts(
    atoms: &[MetadataContentAtom],
) -> Vec<usize> {
    let left_count = atoms.len().saturating_sub(1);
    if left_count == 0 {
        return Vec::new();
    }
    let seed = atoms
        .first()
        .and_then(|atom| atom.members.first())
        .copied()
        .map(stable_metadata_recall_token_hash)
        .unwrap_or(0);
    let folded_seed = seed as u32 ^ (seed >> 32) as u32;
    let mut sampled = (0..left_count)
        .filter(|&left| {
            let contract = atoms[left].members.first().copied().unwrap_or_default();
            stable_metadata_recall_token_hash(contract ^ folded_seed)
                .is_multiple_of(METADATA_CONSERVATIVE_CALIBRATION_DIVISOR)
        })
        .collect::<Vec<_>>();
    if sampled.is_empty() {
        sampled.push(seed as usize % left_count);
    }
    sampled
}

pub(in super::super) fn for_each_metadata_calibration_hit(
    left: usize,
    candidates: &[MetadataDocIndex],
    atoms: &[MetadataContentAtom],
    compact_docs: &[CompactMetadataContentDocument],
    context: &MetadataContentUnionContext<'_>,
    template_cache_pool: &MetadataTemplateScoreCachePool,
    mut on_hit: impl FnMut(MetadataDocIndex),
) {
    let mut pairs = Vec::with_capacity(METADATA_CONTENT_SCORE_BATCH_PAIRS);
    for chunk in candidates.chunks(METADATA_CONTENT_SCORE_BATCH_PAIRS) {
        pairs.clear();
        pairs.extend(chunk.iter().copied().map(|right| (left, right)));
        let batch = collect_metadata_validated_atom_pair_hits(
            &pairs,
            atoms,
            compact_docs,
            context.template_compatibility,
            context.pool,
            template_cache_pool,
        );
        for (_, right) in batch.hits {
            on_hit(right);
        }
    }
}

pub(in super::super) fn calibrate_metadata_conservative_recall(
    atoms: &[MetadataContentAtom],
    compact_docs: &[CompactMetadataContentDocument],
    candidate_index: &MetadataLocalCandidateIndex,
    context: &MetadataContentUnionContext<'_>,
    template_cache_pool: &MetadataTemplateScoreCachePool,
    progress: Option<MetadataSharedTokenGroupProgress<'_>>,
) -> MetadataRecallCalibrationStats {
    let lefts = metadata_conservative_calibration_lefts(atoms);
    let total_lefts = lefts.len();
    let mut calibration = MetadataRecallCalibrationStats {
        sampled_left_atoms: lefts.len() as u64,
        ..MetadataRecallCalibrationStats::default()
    };
    let mut scratch = MetadataCandidateScratch::new(atoms.len());
    let mut exact_duplicate_atoms = vec![false; atoms.len()];
    let mut conservative_duplicate_atoms = vec![false; atoms.len()];
    let mut conservative_hit_generations = vec![0u16; atoms.len()];
    let mut conservative_hit_generation = 0u16;
    let mut exact_components = UnionFind::new(atoms.len());
    let mut conservative_components = UnionFind::new(atoms.len());
    for (sample_index, left) in lefts.into_iter().enumerate() {
        conservative_hit_generation = conservative_hit_generation.wrapping_add(1);
        if conservative_hit_generation == 0 {
            conservative_hit_generations.fill(0);
            conservative_hit_generation = 1;
        }
        let conservative_batch = collect_metadata_left_candidate_batch(
            left,
            atoms,
            compact_docs,
            candidate_index,
            context.template_compatibility,
            false,
            &mut scratch,
        );
        calibration.conservative_candidate_pairs = calibration
            .conservative_candidate_pairs
            .saturating_add(conservative_batch.candidates.len() as u64);
        for_each_metadata_calibration_hit(
            left,
            &conservative_batch.candidates,
            atoms,
            compact_docs,
            context,
            template_cache_pool,
            |right| {
                let right = metadata_doc_index_to_usize(right);
                conservative_hit_generations[right] = conservative_hit_generation;
                conservative_duplicate_atoms[left] = true;
                conservative_duplicate_atoms[right] = true;
                conservative_components.union(left, right);
            },
        );
        drop(conservative_batch);

        let exact_batch = collect_metadata_left_candidate_batch(
            left,
            atoms,
            compact_docs,
            candidate_index,
            context.template_compatibility,
            true,
            &mut scratch,
        );
        calibration.exact_candidate_pairs = calibration
            .exact_candidate_pairs
            .saturating_add(exact_batch.candidates.len() as u64);
        for_each_metadata_calibration_hit(
            left,
            &exact_batch.candidates,
            atoms,
            compact_docs,
            context,
            template_cache_pool,
            |right| {
                calibration.exact_matched_pairs = calibration.exact_matched_pairs.saturating_add(1);
                if conservative_hit_generations[metadata_doc_index_to_usize(right)]
                    != conservative_hit_generation
                {
                    calibration.missed_matched_pairs =
                        calibration.missed_matched_pairs.saturating_add(1);
                }
                let right = metadata_doc_index_to_usize(right);
                exact_duplicate_atoms[left] = true;
                exact_duplicate_atoms[right] = true;
                exact_components.union(left, right);
            },
        );
        if let Some(progress) = progress {
            progress.update_calibration(sample_index.saturating_add(1), total_lefts, &calibration);
        }
    }

    let mut exact_component_weights = HashMap::<usize, u64>::new();
    let mut conservative_partition_weights = HashMap::<(usize, usize), u64>::new();
    let mut exact_duplicate_contract_members = 0u64;
    let mut missed_duplicate_contract_members = 0u64;
    for atom_index in 0..atoms.len() {
        if !exact_duplicate_atoms[atom_index] {
            continue;
        }
        let weight = atoms[atom_index].members.len() as u64;
        exact_duplicate_contract_members = exact_duplicate_contract_members.saturating_add(weight);
        if !conservative_duplicate_atoms[atom_index] {
            missed_duplicate_contract_members =
                missed_duplicate_contract_members.saturating_add(weight);
        }
        let exact_root = exact_components.find(atom_index);
        let conservative_root = conservative_components.find(atom_index);
        let exact_weight = exact_component_weights.entry(exact_root).or_default();
        *exact_weight = exact_weight.saturating_add(weight);
        let partition_weight = conservative_partition_weights
            .entry((exact_root, conservative_root))
            .or_default();
        *partition_weight = partition_weight.saturating_add(weight);
    }
    let mut largest_partition_by_exact_component = HashMap::<usize, u64>::new();
    for ((exact_root, _), weight) in conservative_partition_weights {
        let largest = largest_partition_by_exact_component
            .entry(exact_root)
            .or_default();
        *largest = (*largest).max(weight);
    }
    let exact_component_members = exact_component_weights
        .values()
        .copied()
        .fold(0u64, u64::saturating_add);
    let shifted_component_members = exact_component_weights
        .into_iter()
        .map(|(root, weight)| {
            weight.saturating_sub(
                largest_partition_by_exact_component
                    .get(&root)
                    .copied()
                    .unwrap_or(0),
            )
        })
        .fold(0u64, u64::saturating_add);
    calibration.exact_duplicate_contract_members = exact_duplicate_contract_members;
    calibration.missed_duplicate_contract_members = missed_duplicate_contract_members;
    calibration.exact_component_members = exact_component_members;
    calibration.shifted_component_members = shifted_component_members;
    calibration
}
