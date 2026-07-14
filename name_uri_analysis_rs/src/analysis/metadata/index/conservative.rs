use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap};

use rayon::prelude::*;

use super::super::super::{AnalysisError, UnionFind};
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
        let (exact_contract_members, missed_contract_members) =
            if self.weighted_exact_duplicate_contract_members > 0 {
                (
                    self.weighted_exact_duplicate_contract_members,
                    self.weighted_missed_duplicate_contract_members,
                )
            } else {
                (
                    u128::from(self.exact_duplicate_contract_members),
                    u128::from(self.missed_duplicate_contract_members),
                )
            };
        let (exact_component_members, shifted_component_members) =
            if self.weighted_exact_component_members > 0 {
                (
                    self.weighted_exact_component_members,
                    self.weighted_shifted_component_members,
                )
            } else {
                (
                    u128::from(self.exact_component_members),
                    u128::from(self.shifted_component_members),
                )
            };
        let contract_drift_exceeded = exact_contract_members > 0
            && missed_contract_members.saturating_mul(1_000)
                > exact_contract_members
                    .saturating_mul(u128::from(METADATA_CONSERVATIVE_CONTRACT_DRIFT_PER_MILLE));
        let component_drift_exceeded = exact_component_members > 0
            && shifted_component_members.saturating_mul(1_000)
                > exact_component_members
                    .saturating_mul(u128::from(METADATA_CONSERVATIVE_COMPONENT_DRIFT_PER_MILLE));
        contract_drift_exceeded || component_drift_exceeded
    }

    pub(in super::super) fn representative_recall_risk_exceeded(&self) -> bool {
        if self.requires_exact_fallback() {
            return true;
        }
        if self.exact_matched_pairs < METADATA_CONSERVATIVE_PAIR_WILSON_MIN_MATCHES {
            return false;
        }

        const WILSON_Z_SQUARED_95_PERCENT: f64 = 3.841_458_820_694_124;
        let samples = self.exact_matched_pairs as f64;
        let observed_rate = if self.weighted_exact_matched_pairs > 0 {
            self.weighted_missed_matched_pairs
                .min(self.weighted_exact_matched_pairs) as f64
                / self.weighted_exact_matched_pairs as f64
        } else {
            self.missed_matched_pairs.min(self.exact_matched_pairs) as f64 / samples
        };
        let center = observed_rate + WILSON_Z_SQUARED_95_PERCENT / (2.0 * samples);
        let radius = WILSON_Z_SQUARED_95_PERCENT.sqrt()
            * (observed_rate * (1.0 - observed_rate) / samples
                + WILSON_Z_SQUARED_95_PERCENT / (4.0 * samples * samples))
                .sqrt();
        let upper_bound = (center + radius) / (1.0 + WILSON_Z_SQUARED_95_PERCENT / samples);
        upper_bound > METADATA_CONSERVATIVE_PAIR_DRIFT_MAX_RATE
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
        self.weighted_exact_matched_pairs = self
            .weighted_exact_matched_pairs
            .saturating_add(other.weighted_exact_matched_pairs);
        self.weighted_missed_matched_pairs = self
            .weighted_missed_matched_pairs
            .saturating_add(other.weighted_missed_matched_pairs);
        self.weighted_exact_duplicate_contract_members = self
            .weighted_exact_duplicate_contract_members
            .saturating_add(other.weighted_exact_duplicate_contract_members);
        self.weighted_missed_duplicate_contract_members = self
            .weighted_missed_duplicate_contract_members
            .saturating_add(other.weighted_missed_duplicate_contract_members);
        self.weighted_exact_component_members = self
            .weighted_exact_component_members
            .saturating_add(other.weighted_exact_component_members);
        self.weighted_shifted_component_members = self
            .weighted_shifted_component_members
            .saturating_add(other.weighted_shifted_component_members);
    }
}

impl MetadataCalibrationSample {
    fn population_weight_units(&self) -> u64 {
        let numerator = u128::from(self.stratum_population)
            .saturating_mul(u128::from(METADATA_CALIBRATION_WEIGHT_SCALE));
        let denominator = u128::from(self.stratum_sample_count.max(1));
        let rounded = numerator
            .saturating_add(denominator / 2)
            .saturating_div(denominator);
        u64::try_from(rounded).unwrap_or(u64::MAX)
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

fn metadata_recall_simhash_band_value(simhash: u64, band_index: usize) -> u8 {
    let shift = band_index.saturating_mul(METADATA_CONSERVATIVE_SIMHASH_BAND_BITS);
    ((simhash >> shift) & 0xff) as u8
}

fn metadata_recall_band_probe_values(
    value: u8,
    profile: MetadataConservativeRecallProfile,
) -> ([u8; 1 + METADATA_CONSERVATIVE_SIMHASH_BAND_BITS], usize) {
    let mut values = [0u8; 1 + METADATA_CONSERVATIVE_SIMHASH_BAND_BITS];
    values[0] = value;
    let len = if profile == MetadataConservativeRecallProfile::Widened {
        for bit in 0..METADATA_CONSERVATIVE_SIMHASH_BAND_BITS {
            values[bit + 1] = value ^ (1u8 << bit);
        }
        values.len()
    } else {
        1
    };
    (values, len)
}

impl MetadataConservativeJointBandFamily {
    fn from_dimensions(
        template: &MetadataConservativeDimensionIndex,
        content: &MetadataConservativeDimensionIndex,
        template_band: usize,
        content_band: usize,
    ) -> Self {
        debug_assert_eq!(template.sketches.len(), content.sketches.len());
        let mut posting_offsets = vec![0u64; METADATA_CONSERVATIVE_JOINT_BAND_BUCKETS + 1];
        for (template_sketch, content_sketch) in template.sketches.iter().zip(&content.sketches) {
            if !template_sketch.has_terms || !content_sketch.has_terms {
                continue;
            }
            let template_value =
                metadata_recall_simhash_band_value(template_sketch.simhash, template_band);
            let content_value =
                metadata_recall_simhash_band_value(content_sketch.simhash, content_band);
            let bucket = (usize::from(template_value) << METADATA_CONSERVATIVE_SIMHASH_BAND_BITS)
                | usize::from(content_value);
            posting_offsets[bucket + 1] = posting_offsets[bucket + 1].saturating_add(1);
        }
        for bucket in 0..METADATA_CONSERVATIVE_JOINT_BAND_BUCKETS {
            posting_offsets[bucket + 1] =
                posting_offsets[bucket + 1].saturating_add(posting_offsets[bucket]);
        }
        let mut cursors = posting_offsets[..METADATA_CONSERVATIVE_JOINT_BAND_BUCKETS].to_vec();
        let posting_count = posting_offsets.last().copied().unwrap_or(0) as usize;
        let mut posting_atoms = vec![0; posting_count];
        let mut posting_positions_by_atom = vec![MetadataDocIndex::MAX; template.sketches.len()];
        for (atom_index, (template_sketch, content_sketch)) in
            template.sketches.iter().zip(&content.sketches).enumerate()
        {
            if !template_sketch.has_terms || !content_sketch.has_terms {
                continue;
            }
            let template_value =
                metadata_recall_simhash_band_value(template_sketch.simhash, template_band);
            let content_value =
                metadata_recall_simhash_band_value(content_sketch.simhash, content_band);
            let bucket = (usize::from(template_value) << METADATA_CONSERVATIVE_SIMHASH_BAND_BITS)
                | usize::from(content_value);
            let cursor = &mut cursors[bucket];
            posting_atoms[*cursor as usize] = metadata_doc_index_from_usize(atom_index);
            posting_positions_by_atom[atom_index] = metadata_doc_index_from_usize(*cursor as usize);
            *cursor = cursor.saturating_add(1);
        }
        Self {
            posting_offsets,
            posting_atoms,
            posting_positions_by_atom,
        }
    }

    fn posting_range_after(
        &self,
        bucket: usize,
        atom_index: MetadataDocIndex,
    ) -> MetadataPostingRange {
        let posting_start = self.posting_offsets[bucket] as usize;
        let posting_end = self.posting_offsets[bucket + 1] as usize;
        let posting = &self.posting_atoms[posting_start..posting_end];
        let relative_start = posting.partition_point(|&right| right <= atom_index);
        MetadataPostingRange {
            start: posting_start + relative_start,
            end: posting_end,
        }
    }

    fn posting_range_after_own_bucket(
        &self,
        bucket: usize,
        atom_index: usize,
    ) -> MetadataPostingRange {
        let position = metadata_doc_index_to_usize(self.posting_positions_by_atom[atom_index]);
        let posting_start = self.posting_offsets[bucket] as usize;
        let posting_end = self.posting_offsets[bucket + 1] as usize;
        debug_assert!(position >= posting_start && position < posting_end);
        debug_assert_eq!(
            self.posting_atoms[position],
            metadata_doc_index_from_usize(atom_index)
        );
        MetadataPostingRange {
            start: position.saturating_add(1),
            end: posting_end,
        }
    }
}

impl MetadataConservativeJointBandIndex {
    pub(in super::super) fn from_dimensions(
        template: &MetadataConservativeDimensionIndex,
        content: &MetadataConservativeDimensionIndex,
        parallel: bool,
    ) -> Self {
        let build_family = |family_index: usize| {
            let template_band = family_index / METADATA_CONSERVATIVE_SIMHASH_BANDS;
            let content_band = family_index % METADATA_CONSERVATIVE_SIMHASH_BANDS;
            MetadataConservativeJointBandFamily::from_dimensions(
                template,
                content,
                template_band,
                content_band,
            )
        };
        let families = if parallel {
            (0..METADATA_CONSERVATIVE_JOINT_BAND_FAMILIES)
                .into_par_iter()
                .map(build_family)
                .collect()
        } else {
            (0..METADATA_CONSERVATIVE_JOINT_BAND_FAMILIES)
                .map(build_family)
                .collect()
        };
        Self { families }
    }

    fn for_each_posting_range_after(
        &self,
        atom_index: usize,
        template_simhash: u64,
        content_simhash: u64,
        profile: MetadataConservativeRecallProfile,
        mut visit: impl FnMut(&MetadataConservativeJointBandFamily, MetadataPostingRange),
    ) {
        let compact_atom_index = metadata_doc_index_from_usize(atom_index);
        for template_band in 0..METADATA_CONSERVATIVE_SIMHASH_BANDS {
            let template_value =
                metadata_recall_simhash_band_value(template_simhash, template_band);
            let (template_values, template_value_count) =
                metadata_recall_band_probe_values(template_value, profile);
            for content_band in 0..METADATA_CONSERVATIVE_SIMHASH_BANDS {
                let content_value =
                    metadata_recall_simhash_band_value(content_simhash, content_band);
                let (content_values, content_value_count) =
                    metadata_recall_band_probe_values(content_value, profile);
                let family = &self.families
                    [template_band * METADATA_CONSERVATIVE_SIMHASH_BANDS + content_band];
                for &template_probe in &template_values[..template_value_count] {
                    for &content_probe in &content_values[..content_value_count] {
                        let bucket = (usize::from(template_probe)
                            << METADATA_CONSERVATIVE_SIMHASH_BAND_BITS)
                            | usize::from(content_probe);
                        let range =
                            if template_probe == template_value && content_probe == content_value {
                                family.posting_range_after_own_bucket(bucket, atom_index)
                            } else {
                                family.posting_range_after(bucket, compact_atom_index)
                            };
                        visit(family, range);
                    }
                }
            }
        }
    }

    pub(in super::super) fn estimate_posting_visits_after(
        &self,
        atom_index: usize,
        template_simhash: u64,
        content_simhash: u64,
        profile: MetadataConservativeRecallProfile,
    ) -> usize {
        let mut visits = 0usize;
        self.for_each_posting_range_after(
            atom_index,
            template_simhash,
            content_simhash,
            profile,
            |_, range| {
                visits = visits.saturating_add(range.end.saturating_sub(range.start));
            },
        );
        visits
    }

    pub(in super::super) fn append_candidates_after(
        &self,
        atom_index: usize,
        template_simhash: u64,
        content_simhash: u64,
        profile: MetadataConservativeRecallProfile,
        scratch: &mut MetadataCandidateScratch,
    ) {
        self.for_each_posting_range_after(
            atom_index,
            template_simhash,
            content_simhash,
            profile,
            |family, range| {
                scratch.record_posting_visits(range.end.saturating_sub(range.start));
                for &right in &family.posting_atoms[range.start..range.end] {
                    scratch.push_once(right);
                }
            },
        );
    }
}

impl MetadataConservativeDimensionIndex {
    fn estimate_anchor_posting_visits_after(&self, atom_index: usize) -> usize {
        let compact_atom_index = metadata_doc_index_from_usize(atom_index);
        self.sketches[atom_index].anchors[..usize::from(self.sketches[atom_index].anchor_len)]
            .iter()
            .fold(0usize, |visits, &anchor| {
                let range = self
                    .anchor_postings
                    .posting_range_after(anchor, compact_atom_index);
                visits.saturating_add(range.end.saturating_sub(range.start))
            })
    }

    fn estimate_posting_visits_after(
        &self,
        atom_index: usize,
        profile: MetadataConservativeRecallProfile,
    ) -> usize {
        let compact_atom_index = metadata_doc_index_from_usize(atom_index);
        let sketch = &self.sketches[atom_index];
        let mut visits = self.estimate_anchor_posting_visits_after(atom_index);
        if !sketch.has_terms {
            return visits;
        }
        for band_index in 0..METADATA_CONSERVATIVE_SIMHASH_BANDS {
            let key = metadata_recall_simhash_band_key(sketch.simhash, band_index);
            let range = self
                .simhash_band_postings
                .posting_range_after(key, compact_atom_index);
            visits = visits.saturating_add(range.end.saturating_sub(range.start));
            if profile == MetadataConservativeRecallProfile::Widened {
                for bit in 0..METADATA_CONSERVATIVE_SIMHASH_BAND_BITS {
                    let range = self
                        .simhash_band_postings
                        .posting_range_after(key ^ (1u32 << bit), compact_atom_index);
                    visits = visits.saturating_add(range.end.saturating_sub(range.start));
                }
            }
        }
        visits
    }

    fn recalls_candidate(
        &self,
        left: usize,
        right: usize,
        profile: MetadataConservativeRecallProfile,
    ) -> bool {
        let left = &self.sketches[left];
        let right = &self.sketches[right];
        if !left.has_terms || !right.has_terms {
            return false;
        }
        if lowest_common_metadata_token(
            &left.anchors[..usize::from(left.anchor_len)],
            &right.anchors[..usize::from(right.anchor_len)],
        )
        .is_some()
        {
            return true;
        }
        (0..METADATA_CONSERVATIVE_SIMHASH_BANDS).any(|band_index| {
            let left_value = metadata_recall_simhash_band_value(left.simhash, band_index);
            let right_value = metadata_recall_simhash_band_value(right.simhash, band_index);
            left_value == right_value
                || (profile == MetadataConservativeRecallProfile::Widened
                    && (left_value ^ right_value).count_ones() == 1)
        })
    }
}

impl MetadataConservativeCandidateIndex {
    pub(in super::super) fn estimate_posting_visits_after(&self, atom_index: usize) -> usize {
        if let Some(joint_bands) = &self.joint_bands {
            let template_sketch = &self.template.sketches[atom_index];
            let content_sketch = &self.content.sketches[atom_index];
            let joint_visits = if template_sketch.has_terms && content_sketch.has_terms {
                joint_bands.estimate_posting_visits_after(
                    atom_index,
                    template_sketch.simhash,
                    content_sketch.simhash,
                    self.profile,
                )
            } else {
                0
            };
            return joint_visits
                .saturating_add(
                    self.template
                        .estimate_anchor_posting_visits_after(atom_index),
                )
                .saturating_add(
                    self.content
                        .estimate_anchor_posting_visits_after(atom_index),
                );
        }
        self.template
            .estimate_posting_visits_after(atom_index, self.profile)
            .saturating_add(
                self.content
                    .estimate_posting_visits_after(atom_index, self.profile),
            )
    }

    fn append_anchor_candidates_after(
        dimension: &MetadataConservativeDimensionIndex,
        other: &MetadataConservativeDimensionIndex,
        atom_index: usize,
        profile: MetadataConservativeRecallProfile,
        scratch: &mut MetadataCandidateScratch,
    ) {
        let compact_atom_index = metadata_doc_index_from_usize(atom_index);
        let sketch = &dimension.sketches[atom_index];
        for &anchor in &sketch.anchors[..usize::from(sketch.anchor_len)] {
            let range = dimension
                .anchor_postings
                .posting_range_after(anchor, compact_atom_index);
            scratch.record_posting_visits(range.end.saturating_sub(range.start));
            for &right in &dimension.anchor_postings.posting_atoms[range.start..range.end] {
                if other.recalls_candidate(atom_index, metadata_doc_index_to_usize(right), profile)
                {
                    scratch.push_once(right);
                }
            }
        }
    }

    pub(in super::super) fn append_candidates_after(
        &self,
        atom_index: usize,
        scratch: &mut MetadataCandidateScratch,
    ) {
        let Some(joint_bands) = &self.joint_bands else {
            self.template
                .append_candidates_after(atom_index, self.profile, scratch);
            scratch.prepare_secondary_generation();
            self.content
                .append_candidates_after(atom_index, self.profile, scratch);
            scratch.raw_candidate_count = scratch.secondary_candidates.len();
            scratch.retain_secondary_intersection();
            scratch.candidates.retain(|&right| {
                let right = metadata_doc_index_to_usize(right);
                self.template.matches(atom_index, right) && self.content.matches(atom_index, right)
            });
            return;
        };
        let template_sketch = &self.template.sketches[atom_index];
        let content_sketch = &self.content.sketches[atom_index];
        if template_sketch.has_terms && content_sketch.has_terms {
            joint_bands.append_candidates_after(
                atom_index,
                template_sketch.simhash,
                content_sketch.simhash,
                self.profile,
                scratch,
            );
        }
        Self::append_anchor_candidates_after(
            &self.template,
            &self.content,
            atom_index,
            self.profile,
            scratch,
        );
        Self::append_anchor_candidates_after(
            &self.content,
            &self.template,
            atom_index,
            self.profile,
            scratch,
        );
        scratch.raw_candidate_count = scratch.candidates.len();
        scratch.candidates.retain(|&right| {
            let right = metadata_doc_index_to_usize(right);
            self.template.matches(atom_index, right) && self.content.matches(atom_index, right)
        });
    }
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
        profile: MetadataConservativeRecallProfile,
        scratch: &mut MetadataCandidateScratch,
    ) {
        let compact_atom_index = metadata_doc_index_from_usize(atom_index);
        let sketch = &self.sketches[atom_index];
        for &anchor in &sketch.anchors[..usize::from(sketch.anchor_len)] {
            let range = self
                .anchor_postings
                .posting_range_after(anchor, compact_atom_index);
            scratch.record_posting_visits(range.end.saturating_sub(range.start));
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
                scratch.record_posting_visits(range.end.saturating_sub(range.start));
                for &right in &self.simhash_band_postings.posting_atoms[range.start..range.end] {
                    scratch.push_once(right);
                }
                if profile == MetadataConservativeRecallProfile::Widened {
                    for bit in 0..METADATA_CONSERVATIVE_SIMHASH_BAND_BITS {
                        let neighbor_key = key ^ (1u32 << bit);
                        let range = self
                            .simhash_band_postings
                            .posting_range_after(neighbor_key, compact_atom_index);
                        scratch.record_posting_visits(range.end.saturating_sub(range.start));
                        for &right in
                            &self.simhash_band_postings.posting_atoms[range.start..range.end]
                        {
                            scratch.push_once(right);
                        }
                    }
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

#[cfg(test)]
pub(in super::super) fn metadata_conservative_calibration_sample_positions(
    left_count: usize,
    seed: u64,
) -> Vec<usize> {
    if left_count == 0 {
        return Vec::new();
    }
    let minimum = left_count.min(METADATA_CONSERVATIVE_CALIBRATION_MIN_LEFTS);
    let maximum = left_count.min(METADATA_CONSERVATIVE_CALIBRATION_MAX_LEFTS);
    let one_percent = left_count
        .saturating_add(METADATA_CONSERVATIVE_CALIBRATION_DIVISOR as usize - 1)
        / METADATA_CONSERVATIVE_CALIBRATION_DIVISOR as usize;
    let sample_count = one_percent.clamp(minimum, maximum);
    if sample_count == left_count {
        return (0..left_count).collect();
    }
    let folded_seed = seed as u32 ^ (seed >> 32) as u32;
    (0..sample_count)
        .map(|sample_index| {
            let bucket_start = sample_index.saturating_mul(left_count) / sample_count;
            let bucket_end =
                sample_index.saturating_add(1).saturating_mul(left_count) / sample_count;
            let bucket_width = bucket_end.saturating_sub(bucket_start).max(1);
            let hash = stable_metadata_recall_token_hash(
                u32::try_from(sample_index).unwrap_or(u32::MAX) ^ folded_seed,
            );
            bucket_start.saturating_add(hash as usize % bucket_width)
        })
        .collect()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct MetadataCalibrationReservoirEntry {
    priority: u64,
    item: MetadataCalibrationWorkItem,
}

#[derive(Default)]
struct MetadataCalibrationStratum {
    population: u64,
    mandatory: Option<MetadataCalibrationReservoirEntry>,
    candidates: Vec<MetadataCalibrationReservoirEntry>,
}

fn metadata_calibration_sample_priority(left: usize) -> u64 {
    let folded = left as u32 ^ ((left >> 32) as u32).rotate_left(13);
    stable_metadata_recall_token_hash(folded)
}

#[cfg(test)]
pub(in super::super) fn metadata_difficult_first_left_order(
    estimated_posting_visits_by_left: &[u64],
) -> Vec<usize> {
    metadata_difficult_first_left_order_with_pool(estimated_posting_visits_by_left, None)
}

fn metadata_difficult_first_left_order_with_pool(
    estimated_posting_visits_by_left: &[u64],
    pool: Option<&rayon::ThreadPool>,
) -> Vec<usize> {
    let mut lefts = (0..estimated_posting_visits_by_left.len()).collect::<Vec<_>>();
    let compare = |&left: &usize, &right: &usize| {
        estimated_posting_visits_by_left[right]
            .cmp(&estimated_posting_visits_by_left[left])
            .then_with(|| left.cmp(&right))
    };
    if let Some(pool) = pool.filter(|_| lefts.len() >= METADATA_CONSERVATIVE_MIN_ATOMS) {
        pool.install(|| lefts.par_sort_unstable_by(compare));
    } else {
        lefts.sort_unstable_by(compare);
    }
    lefts
}

pub(in super::super) fn plan_metadata_bounded_exact_rescue(
    atoms: &[MetadataContentAtom],
    exact_posting_visits_by_left: &[u64],
    risk_strata: &[(usize, u32)],
    recall_risk_exceeded: bool,
    maximum_posting_visits: u64,
) -> MetadataExactRescuePlan {
    if !recall_risk_exceeded {
        return MetadataExactRescuePlan {
            exact_recall_by_left: vec![false; exact_posting_visits_by_left.len()],
            ..MetadataExactRescuePlan::default()
        };
    }
    let risk_strata = risk_strata.iter().copied().collect::<BTreeSet<_>>();
    let mut lefts_by_stratum = BTreeMap::<(usize, u32), (u64, Vec<usize>)>::new();
    for (left, &posting_visits) in exact_posting_visits_by_left.iter().enumerate() {
        let posting_visits = posting_visits.max(1);
        let cost_bucket = 63u32.saturating_sub(posting_visits.leading_zeros());
        let key = (atoms[left].chain_index, cost_bucket);
        if !risk_strata.contains(&key) {
            continue;
        }
        let (stratum_work, lefts) = lefts_by_stratum.entry(key).or_default();
        *stratum_work = stratum_work.saturating_add(posting_visits);
        lefts.push(left);
    }
    let mut strata = lefts_by_stratum.into_iter().collect::<Vec<_>>();
    strata.sort_unstable_by(|(left_key, (left_work, _)), (right_key, (right_work, _))| {
        left_work
            .cmp(right_work)
            .then_with(|| left_key.cmp(right_key))
    });

    let mut rescue = MetadataExactRescuePlan {
        exact_recall_by_left: vec![false; exact_posting_visits_by_left.len()],
        ..MetadataExactRescuePlan::default()
    };
    for (_, (stratum_work, lefts)) in strata {
        if rescue
            .estimated_exact_posting_visits
            .saturating_add(stratum_work)
            > maximum_posting_visits
        {
            rescue.unrescued_risk_strata = rescue.unrescued_risk_strata.saturating_add(1);
            continue;
        }
        rescue.estimated_exact_posting_visits = rescue
            .estimated_exact_posting_visits
            .saturating_add(stratum_work);
        rescue.exact_left_atoms = rescue.exact_left_atoms.saturating_add(lefts.len() as u64);
        for left in lefts {
            rescue.exact_recall_by_left[left] = true;
        }
    }
    rescue
}

#[cfg(test)]
pub(in super::super) fn plan_metadata_calibration_work_items(
    items: impl IntoIterator<Item = MetadataCalibrationWorkItem>,
    minimum_lefts: usize,
    maximum_lefts: usize,
    maximum_posting_visits: u64,
) -> Result<MetadataCalibrationPlan, AnalysisError> {
    plan_metadata_calibration_work_items_with_estimates(
        items,
        Vec::new(),
        false,
        None,
        minimum_lefts,
        maximum_lefts,
        maximum_posting_visits,
    )
}

fn plan_metadata_calibration_work_items_with_estimates(
    items: impl IntoIterator<Item = MetadataCalibrationWorkItem>,
    mut estimated_posting_visits_by_left: Vec<u64>,
    estimates_are_precomputed: bool,
    pool: Option<&rayon::ThreadPool>,
    minimum_lefts: usize,
    maximum_lefts: usize,
    maximum_posting_visits: u64,
) -> Result<MetadataCalibrationPlan, AnalysisError> {
    let mut strata = BTreeMap::<(usize, u32), MetadataCalibrationStratum>::new();
    let mut item_count = 0usize;
    let mut estimated_total_posting_visits = 0u64;
    let mut global_reservoir = BinaryHeap::<MetadataCalibrationReservoirEntry>::new();
    for mut item in items {
        item_count = item_count.saturating_add(1);
        let posting_visits = if estimates_are_precomputed {
            estimated_posting_visits_by_left
                .get(item.left)
                .copied()
                .ok_or_else(|| {
                    AnalysisError::InvalidData(format!(
                        "metadata posting work estimate missing for left atom {}",
                        item.left
                    ))
                })?
                .max(1)
        } else {
            item.estimated_posting_visits.max(1)
        };
        item.estimated_posting_visits = posting_visits;
        estimated_total_posting_visits =
            estimated_total_posting_visits.saturating_add(posting_visits);
        if estimated_posting_visits_by_left.len() <= item.left {
            estimated_posting_visits_by_left.resize(item.left.saturating_add(1), 0);
        }
        estimated_posting_visits_by_left[item.left] = posting_visits;
        if maximum_lefts == 0 {
            continue;
        }
        let cost_bucket = 63u32.saturating_sub(posting_visits.leading_zeros());
        let stratum = strata.entry((item.chain_index, cost_bucket)).or_default();
        stratum.population = stratum.population.saturating_add(1);
        let priority = metadata_calibration_sample_priority(item.left);
        let mandatory = MetadataCalibrationReservoirEntry { priority, item };
        if stratum.mandatory.is_none_or(|current| mandatory < current) {
            stratum.mandatory = Some(mandatory);
        }
        let entry = MetadataCalibrationReservoirEntry {
            priority: priority / u64::from(cost_bucket.saturating_add(1)),
            item,
        };
        if global_reservoir.len() < maximum_lefts {
            global_reservoir.push(entry);
        } else if global_reservoir
            .peek()
            .is_some_and(|largest| entry < *largest)
        {
            global_reservoir.pop();
            global_reservoir.push(entry);
        }
    }
    if item_count == 0 {
        return Ok(MetadataCalibrationPlan::default());
    }
    if maximum_lefts == 0 {
        return Ok(MetadataCalibrationPlan {
            samples: Vec::new(),
            difficult_first_lefts: metadata_difficult_first_left_order_with_pool(
                &estimated_posting_visits_by_left,
                pool,
            ),
            estimated_posting_visits_by_left,
            estimated_total_posting_visits,
            estimated_sample_posting_visits: 0,
            retained_calibration_candidates: 0,
            uncovered_calibration_strata: Vec::new(),
        });
    }
    let maximum_lefts = maximum_lefts.min(item_count);
    for entry in global_reservoir {
        let posting_visits = entry.item.estimated_posting_visits.max(1);
        let cost_bucket = 63u32.saturating_sub(posting_visits.leading_zeros());
        strata
            .get_mut(&(entry.item.chain_index, cost_bucket))
            .expect("global calibration sample must have an observed stratum")
            .candidates
            .push(entry);
    }
    let mut retained_calibration_candidates = 0usize;

    struct SelectionState {
        chain_index: usize,
        cost_bucket: u32,
        population: u64,
        candidates: Vec<MetadataCalibrationWorkItem>,
        selected: usize,
        blocked: bool,
    }
    let mut selection = strata
        .into_iter()
        .map(|((chain_index, cost_bucket), mut stratum)| {
            let mandatory = stratum
                .mandatory
                .expect("observed calibration stratum must retain one mandatory sample");
            if !stratum
                .candidates
                .iter()
                .any(|entry| entry.item.left == mandatory.item.left)
            {
                stratum.candidates.push(MetadataCalibrationReservoirEntry {
                    priority: 0,
                    item: mandatory.item,
                });
            } else if let Some(entry) = stratum
                .candidates
                .iter_mut()
                .find(|entry| entry.item.left == mandatory.item.left)
            {
                entry.priority = 0;
            }
            stratum
                .candidates
                .sort_unstable_by_key(|entry| (entry.priority, entry.item.left));
            stratum.candidates.dedup_by_key(|entry| entry.item.left);
            retained_calibration_candidates =
                retained_calibration_candidates.saturating_add(stratum.candidates.len());
            SelectionState {
                chain_index,
                cost_bucket,
                population: stratum.population,
                candidates: stratum
                    .candidates
                    .into_iter()
                    .map(|entry| entry.item)
                    .collect(),
                selected: 0,
                blocked: false,
            }
        })
        .collect::<Vec<_>>();

    let mut mandatory_order = (0..selection.len()).collect::<Vec<_>>();
    mandatory_order.sort_unstable_by_key(|&index| {
        let stratum = &selection[index];
        (
            stratum.candidates[0].estimated_posting_visits.max(1),
            stratum.chain_index,
            stratum.cost_bucket,
        )
    });
    let mut estimated_sample_posting_visits = 0u64;
    let mut selected_count = 0usize;
    let mut uncovered_calibration_strata = Vec::new();
    for index in mandatory_order {
        let stratum = &mut selection[index];
        let item_work = stratum.candidates[0].estimated_posting_visits.max(1);
        if selected_count >= maximum_lefts
            || estimated_sample_posting_visits.saturating_add(item_work) > maximum_posting_visits
        {
            stratum.blocked = true;
            uncovered_calibration_strata.push((stratum.chain_index, stratum.cost_bucket));
            continue;
        }
        estimated_sample_posting_visits = estimated_sample_posting_visits.saturating_add(item_work);
        stratum.selected = 1;
        selected_count = selected_count.saturating_add(1);
    }
    uncovered_calibration_strata.sort_unstable();
    while selected_count < maximum_lefts {
        let mut best: Option<usize> = None;
        for index in 0..selection.len() {
            if selection[index].blocked
                || selection[index].selected >= selection[index].candidates.len()
            {
                continue;
            }
            let next_work = selection[index].candidates[selection[index].selected]
                .estimated_posting_visits
                .max(1);
            if estimated_sample_posting_visits.saturating_add(next_work) > maximum_posting_visits {
                selection[index].blocked = true;
                continue;
            }
            let stratum = &selection[index];
            let Some(current_best) = best else {
                best = Some(index);
                continue;
            };
            let current = &selection[current_best];
            let candidate_mass = u128::from(stratum.population)
                .saturating_mul(u128::from(stratum.cost_bucket).saturating_add(1));
            let current_mass = u128::from(current.population)
                .saturating_mul(u128::from(current.cost_bucket).saturating_add(1));
            let candidate_score =
                candidate_mass.saturating_mul(current.selected.saturating_add(1) as u128);
            let current_score =
                current_mass.saturating_mul(stratum.selected.saturating_add(1) as u128);
            if candidate_score > current_score
                || (candidate_score == current_score
                    && (stratum.chain_index, stratum.cost_bucket)
                        > (current.chain_index, current.cost_bucket))
            {
                best = Some(index);
            }
        }
        let Some(best) = best else {
            break;
        };
        let stratum = &mut selection[best];
        let item = stratum.candidates[stratum.selected];
        estimated_sample_posting_visits =
            estimated_sample_posting_visits.saturating_add(item.estimated_posting_visits.max(1));
        stratum.selected = stratum.selected.saturating_add(1);
        selected_count = selected_count.saturating_add(1);
    }
    let required = minimum_lefts.min(item_count);
    if selected_count < required {
        uncovered_calibration_strata.extend(
            selection
                .iter()
                .filter(|stratum| stratum.blocked || (stratum.selected as u64) < stratum.population)
                .map(|stratum| (stratum.chain_index, stratum.cost_bucket)),
        );
        uncovered_calibration_strata.sort_unstable();
        uncovered_calibration_strata.dedup();
    }

    let mut samples = Vec::with_capacity(selected_count);
    for stratum in selection {
        let stratum_sample_count = stratum.selected as u64;
        samples.extend(
            stratum
                .candidates
                .into_iter()
                .take(stratum.selected)
                .map(|item| MetadataCalibrationSample {
                    left: item.left,
                    chain_index: stratum.chain_index,
                    cost_bucket: stratum.cost_bucket,
                    estimated_posting_visits: item.estimated_posting_visits.max(1),
                    stratum_population: stratum.population,
                    stratum_sample_count,
                }),
        );
    }
    samples.sort_unstable_by(|left, right| {
        right
            .estimated_posting_visits
            .cmp(&left.estimated_posting_visits)
            .then_with(|| left.left.cmp(&right.left))
    });
    let difficult_first_lefts =
        metadata_difficult_first_left_order_with_pool(&estimated_posting_visits_by_left, pool);
    Ok(MetadataCalibrationPlan {
        samples,
        estimated_posting_visits_by_left,
        difficult_first_lefts,
        estimated_total_posting_visits,
        estimated_sample_posting_visits,
        retained_calibration_candidates,
        uncovered_calibration_strata,
    })
}

fn metadata_exact_posting_visit_estimates(
    atoms: &[MetadataContentAtom],
    compact_docs: &[CompactMetadataContentDocument],
    candidate_index: &MetadataLocalCandidateIndex,
    compatibility: MetadataTemplateCompatibility<'_>,
    pool: &rayon::ThreadPool,
) -> Vec<u64> {
    let mut estimates = vec![0u64; atoms.len().saturating_sub(1)];
    if estimates.len() >= METADATA_CONSERVATIVE_MIN_ATOMS {
        pool.install(|| {
            estimates
                .par_iter_mut()
                .enumerate()
                .map_init(
                    MetadataCandidatePostingPlan::default,
                    |posting_plan, (left, estimate)| {
                        let atom = &atoms[left];
                        let document = &compact_docs
                            [metadata_doc_index_to_usize(atom.representative_record_index)];
                        *estimate = candidate_index
                            .estimate_exact_posting_visits(
                                left,
                                atom,
                                document,
                                compatibility,
                                posting_plan,
                            )
                            .max(1) as u64;
                    },
                )
                .for_each(drop);
        });
    } else {
        let mut posting_plan = MetadataCandidatePostingPlan::default();
        for (left, estimate) in estimates.iter_mut().enumerate() {
            let atom = &atoms[left];
            let document =
                &compact_docs[metadata_doc_index_to_usize(atom.representative_record_index)];
            *estimate = candidate_index
                .estimate_exact_posting_visits(
                    left,
                    atom,
                    document,
                    compatibility,
                    &mut posting_plan,
                )
                .max(1) as u64;
        }
    }
    estimates
}

fn metadata_production_posting_visit_estimates(
    atoms: &[MetadataContentAtom],
    compact_docs: &[CompactMetadataContentDocument],
    candidate_index: &MetadataLocalCandidateIndex,
    compatibility: MetadataTemplateCompatibility<'_>,
    pool: &rayon::ThreadPool,
    exact_recall_by_left: Option<&[bool]>,
) -> Vec<u64> {
    let mut estimates = vec![0u64; atoms.len().saturating_sub(1)];
    if estimates.len() >= METADATA_CONSERVATIVE_MIN_ATOMS {
        pool.install(|| {
            estimates
                .par_iter_mut()
                .enumerate()
                .map_init(
                    MetadataCandidatePostingPlan::default,
                    |posting_plan, (left, estimate)| {
                        let atom = &atoms[left];
                        let document = &compact_docs
                            [metadata_doc_index_to_usize(atom.representative_record_index)];
                        let exact_recall = exact_recall_by_left
                            .and_then(|exact_by_left| exact_by_left.get(left))
                            .copied()
                            .unwrap_or(false);
                        *estimate = if exact_recall {
                            candidate_index.estimate_exact_posting_visits(
                                left,
                                atom,
                                document,
                                compatibility,
                                posting_plan,
                            )
                        } else {
                            candidate_index.estimate_production_posting_visits(
                                left,
                                atom,
                                document,
                                compatibility,
                                posting_plan,
                            )
                        }
                        .max(1) as u64;
                    },
                )
                .for_each(drop);
        });
    } else {
        let mut posting_plan = MetadataCandidatePostingPlan::default();
        for (left, estimate) in estimates.iter_mut().enumerate() {
            let atom = &atoms[left];
            let document =
                &compact_docs[metadata_doc_index_to_usize(atom.representative_record_index)];
            let exact_recall = exact_recall_by_left
                .and_then(|exact_by_left| exact_by_left.get(left))
                .copied()
                .unwrap_or(false);
            *estimate = if exact_recall {
                candidate_index.estimate_exact_posting_visits(
                    left,
                    atom,
                    document,
                    compatibility,
                    &mut posting_plan,
                )
            } else {
                candidate_index.estimate_production_posting_visits(
                    left,
                    atom,
                    document,
                    compatibility,
                    &mut posting_plan,
                )
            }
            .max(1) as u64;
        }
    }
    estimates
}

pub(in super::super) fn metadata_production_work_plan(
    atoms: &[MetadataContentAtom],
    compact_docs: &[CompactMetadataContentDocument],
    candidate_index: &MetadataLocalCandidateIndex,
    compatibility: MetadataTemplateCompatibility<'_>,
    pool: &rayon::ThreadPool,
    exact_recall_by_left: Option<&[bool]>,
    fallback_token_exclusion: Option<(
        &MetadataFallbackTokenExclusionIndex,
        &CompactContractTokens,
    )>,
) -> Result<MetadataCalibrationPlan, AnalysisError> {
    let mut estimates = metadata_production_posting_visit_estimates(
        atoms,
        compact_docs,
        candidate_index,
        compatibility,
        pool,
        exact_recall_by_left,
    );
    if let Some((exclusion_index, contract_tokens)) = fallback_token_exclusion {
        let add_exclusion_estimate = |left: usize, estimate: &mut u64| {
            *estimate = estimate.saturating_add(exclusion_index.estimate_left_suffix_visits(
                left,
                atoms,
                contract_tokens,
            ) as u64);
        };
        if estimates.len() >= METADATA_CONSERVATIVE_MIN_ATOMS {
            pool.install(|| {
                estimates
                    .par_iter_mut()
                    .enumerate()
                    .for_each(|(left, estimate)| add_exclusion_estimate(left, estimate));
            });
        } else {
            estimates
                .iter_mut()
                .enumerate()
                .for_each(|(left, estimate)| add_exclusion_estimate(left, estimate));
        }
    }
    let work_items = atoms
        .iter()
        .take(estimates.len())
        .enumerate()
        .map(|(left, atom)| MetadataCalibrationWorkItem {
            left,
            chain_index: atom.chain_index,
            estimated_posting_visits: 0,
        });
    plan_metadata_calibration_work_items_with_estimates(
        work_items,
        estimates,
        true,
        Some(pool),
        0,
        0,
        u64::MAX,
    )
}

pub(in super::super) fn metadata_conservative_calibration_plan_with_work_budget(
    atoms: &[MetadataContentAtom],
    compact_docs: &[CompactMetadataContentDocument],
    candidate_index: &MetadataLocalCandidateIndex,
    compatibility: MetadataTemplateCompatibility<'_>,
    pool: &rayon::ThreadPool,
) -> Result<MetadataCalibrationPlan, AnalysisError> {
    let estimates = metadata_exact_posting_visit_estimates(
        atoms,
        compact_docs,
        candidate_index,
        compatibility,
        pool,
    );
    let left_count = estimates.len();
    let work_items = atoms
        .iter()
        .take(left_count)
        .enumerate()
        .map(|(left, atom)| MetadataCalibrationWorkItem {
            left,
            chain_index: atom.chain_index,
            estimated_posting_visits: 0,
        });
    plan_metadata_calibration_work_items_with_estimates(
        work_items,
        estimates,
        true,
        Some(pool),
        METADATA_CONSERVATIVE_CALIBRATION_MIN_LEFTS.min(left_count),
        METADATA_CONSERVATIVE_CALIBRATION_MAX_LEFTS.min(left_count),
        METADATA_CONSERVATIVE_CALIBRATION_MAX_POSTING_VISITS,
    )
}

struct MetadataCalibrationScoringContext<'a, 'context> {
    atoms: &'a [MetadataContentAtom],
    compact_docs: &'a [CompactMetadataContentDocument],
    union: &'a MetadataContentUnionContext<'context>,
    template_cache_pool: &'a MetadataTemplateScoreCachePool,
}

fn for_each_metadata_calibration_hit(
    left: usize,
    candidates: &MetadataCandidateSet,
    pairs: &mut Vec<(usize, MetadataDocIndex)>,
    scoring: &MetadataCalibrationScoringContext<'_, '_>,
    mut on_hit: impl FnMut(MetadataDocIndex),
) {
    pairs.clear();
    for right in candidates.iter() {
        pairs.push((left, right));
        if pairs.len() < METADATA_CONTENT_SCORE_BATCH_PAIRS {
            continue;
        }
        let batch = collect_metadata_validated_atom_pair_hits(
            pairs,
            scoring.atoms,
            scoring.compact_docs,
            scoring.union.template_compatibility,
            scoring.union.pool,
            scoring.template_cache_pool,
        );
        for (_, right) in batch.hits {
            on_hit(right);
        }
        pairs.clear();
    }
    if !pairs.is_empty() {
        let batch = collect_metadata_validated_atom_pair_hits(
            pairs,
            scoring.atoms,
            scoring.compact_docs,
            scoring.union.template_compatibility,
            scoring.union.pool,
            scoring.template_cache_pool,
        );
        for (_, right) in batch.hits {
            on_hit(right);
        }
        pairs.clear();
    }
}

pub(in super::super) fn calibrate_metadata_conservative_recall(
    request: MetadataRecallCalibrationRequest<'_, '_>,
) -> MetadataRecallCalibrationOutcome {
    let MetadataRecallCalibrationRequest {
        atoms,
        compact_docs,
        candidate_index,
        samples,
        estimated_posting_visits_by_left,
        context,
        template_cache_pool,
        scope,
        fallback_token_exclusion_index,
        candidate_buffer_pool,
        progress,
    } = request;
    let total_lefts = samples.len();
    let mut calibration = MetadataRecallCalibrationStats {
        sampled_left_atoms: samples.len() as u64,
        ..MetadataRecallCalibrationStats::default()
    };
    let mut scratch = MetadataCandidateScratch::new(atoms.len());
    let mut exact_duplicate_atoms = vec![false; atoms.len()];
    let mut conservative_duplicate_atoms = vec![false; atoms.len()];
    let mut conservative_hit_generations = vec![0u16; atoms.len()];
    let mut conservative_hit_generation = 0u16;
    let mut exact_components = UnionFind::new(atoms.len());
    let mut conservative_components = UnionFind::new(atoms.len());
    let mut atom_weight_units = vec![0u64; atoms.len()];
    let mut score_pairs = Vec::with_capacity(METADATA_CONTENT_SCORE_BATCH_PAIRS);
    let mut risk_strata = Vec::new();
    let scoring = MetadataCalibrationScoringContext {
        atoms,
        compact_docs,
        union: context,
        template_cache_pool,
    };
    for (sample_index, sample) in samples.into_iter().enumerate() {
        let left = sample.left;
        let population_weight_units = sample.population_weight_units();
        conservative_hit_generation = conservative_hit_generation.wrapping_add(1);
        if conservative_hit_generation == 0 {
            conservative_hit_generations.fill(0);
            conservative_hit_generation = 1;
        }
        let conservative_collection = MetadataCandidateCollectionContext {
            atoms,
            compact_docs,
            candidate_index,
            compatibility: context.template_compatibility,
            exact_recall: false,
            exact_recall_by_left: None,
            scope,
            contract_tokens: context.contract_tokens,
            fallback_token_exclusion_index,
            candidate_buffer_pool,
            estimated_posting_visits_by_left: Some(estimated_posting_visits_by_left),
        };
        let conservative_batch =
            collect_metadata_left_candidate_batch(left, &conservative_collection, &mut scratch);
        calibration.conservative_candidate_pairs = calibration
            .conservative_candidate_pairs
            .saturating_add(conservative_batch.candidates.len() as u64);
        for_each_metadata_calibration_hit(
            left,
            &conservative_batch.candidates,
            &mut score_pairs,
            &scoring,
            |right| {
                let right = metadata_doc_index_to_usize(right);
                conservative_hit_generations[right] = conservative_hit_generation;
                conservative_duplicate_atoms[left] = true;
                conservative_duplicate_atoms[right] = true;
                conservative_components.union(left, right);
            },
        );
        let exact_collection = MetadataCandidateCollectionContext {
            atoms,
            compact_docs,
            candidate_index,
            compatibility: context.template_compatibility,
            exact_recall: true,
            exact_recall_by_left: None,
            scope,
            contract_tokens: context.contract_tokens,
            fallback_token_exclusion_index,
            candidate_buffer_pool,
            estimated_posting_visits_by_left: Some(estimated_posting_visits_by_left),
        };
        let exact_batch =
            collect_metadata_left_candidate_batch(left, &exact_collection, &mut scratch);
        calibration.exact_candidate_pairs = calibration
            .exact_candidate_pairs
            .saturating_add(exact_batch.candidates.len() as u64);
        let mut sample_missed = false;
        for_each_metadata_calibration_hit(
            left,
            &exact_batch.candidates,
            &mut score_pairs,
            &scoring,
            |right| {
                calibration.exact_matched_pairs = calibration.exact_matched_pairs.saturating_add(1);
                calibration.weighted_exact_matched_pairs = calibration
                    .weighted_exact_matched_pairs
                    .saturating_add(u128::from(population_weight_units));
                if conservative_hit_generations[metadata_doc_index_to_usize(right)]
                    != conservative_hit_generation
                {
                    sample_missed = true;
                    calibration.missed_matched_pairs =
                        calibration.missed_matched_pairs.saturating_add(1);
                    calibration.weighted_missed_matched_pairs = calibration
                        .weighted_missed_matched_pairs
                        .saturating_add(u128::from(population_weight_units));
                }
                let right = metadata_doc_index_to_usize(right);
                atom_weight_units[left] = atom_weight_units[left].max(population_weight_units);
                atom_weight_units[right] = atom_weight_units[right].max(population_weight_units);
                exact_duplicate_atoms[left] = true;
                exact_duplicate_atoms[right] = true;
                exact_components.union(left, right);
            },
        );
        if sample_missed {
            risk_strata.push((sample.chain_index, sample.cost_bucket));
        }
        if let Some(progress) = progress {
            progress.update_calibration(sample_index.saturating_add(1), total_lefts, &calibration);
        }
    }

    let mut exact_component_weights = HashMap::<usize, u64>::new();
    let mut conservative_partition_weights = HashMap::<(usize, usize), u64>::new();
    let mut weighted_exact_component_weights = HashMap::<usize, u128>::new();
    let mut weighted_conservative_partition_weights = HashMap::<(usize, usize), u128>::new();
    let mut exact_duplicate_contract_members = 0u64;
    let mut missed_duplicate_contract_members = 0u64;
    for atom_index in 0..atoms.len() {
        if !exact_duplicate_atoms[atom_index] {
            continue;
        }
        let weight = atoms[atom_index].members.len() as u64;
        let weighted = u128::from(weight).saturating_mul(u128::from(
            atom_weight_units[atom_index].max(METADATA_CALIBRATION_WEIGHT_SCALE),
        ));
        exact_duplicate_contract_members = exact_duplicate_contract_members.saturating_add(weight);
        calibration.weighted_exact_duplicate_contract_members = calibration
            .weighted_exact_duplicate_contract_members
            .saturating_add(weighted);
        if !conservative_duplicate_atoms[atom_index] {
            missed_duplicate_contract_members =
                missed_duplicate_contract_members.saturating_add(weight);
            calibration.weighted_missed_duplicate_contract_members = calibration
                .weighted_missed_duplicate_contract_members
                .saturating_add(weighted);
        }
        let exact_root = exact_components.find(atom_index);
        let conservative_root = conservative_components.find(atom_index);
        let exact_weight = exact_component_weights.entry(exact_root).or_default();
        *exact_weight = exact_weight.saturating_add(weight);
        let partition_weight = conservative_partition_weights
            .entry((exact_root, conservative_root))
            .or_default();
        *partition_weight = partition_weight.saturating_add(weight);
        let weighted_exact_weight = weighted_exact_component_weights
            .entry(exact_root)
            .or_default();
        *weighted_exact_weight = weighted_exact_weight.saturating_add(weighted);
        let weighted_partition_weight = weighted_conservative_partition_weights
            .entry((exact_root, conservative_root))
            .or_default();
        *weighted_partition_weight = weighted_partition_weight.saturating_add(weighted);
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
    let mut weighted_largest_partition_by_exact_component = HashMap::<usize, u128>::new();
    for ((exact_root, _), weight) in weighted_conservative_partition_weights {
        let largest = weighted_largest_partition_by_exact_component
            .entry(exact_root)
            .or_default();
        *largest = (*largest).max(weight);
    }
    calibration.weighted_exact_component_members = weighted_exact_component_weights
        .values()
        .copied()
        .fold(0u128, u128::saturating_add);
    calibration.weighted_shifted_component_members = weighted_exact_component_weights
        .into_iter()
        .map(|(root, weight)| {
            weight.saturating_sub(
                weighted_largest_partition_by_exact_component
                    .get(&root)
                    .copied()
                    .unwrap_or(0),
            )
        })
        .fold(0u128, u128::saturating_add);
    calibration.exact_duplicate_contract_members = exact_duplicate_contract_members;
    calibration.missed_duplicate_contract_members = missed_duplicate_contract_members;
    calibration.exact_component_members = exact_component_members;
    calibration.shifted_component_members = shifted_component_members;
    risk_strata.sort_unstable();
    risk_strata.dedup();
    MetadataRecallCalibrationOutcome {
        stats: calibration,
        risk_strata,
    }
}
