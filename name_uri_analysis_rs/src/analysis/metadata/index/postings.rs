use rayon::prelude::*;

use super::super::super::MetadataRecallMode;
use super::super::bm25::{CompactMetadataContentDocument, CompactMetadataScoring};
use super::super::{metadata_doc_index_from_usize, metadata_doc_index_to_usize, MetadataDocIndex};

use super::*;

impl MetadataCandidatePostingPlan {
    pub(in super::super) fn clear(&mut self) {
        self.content.clear();
        self.template_full.clear();
        self.template_prefix.clear();
    }
}

impl MetadataContentCandidateIndex {
    pub(in super::super) fn from_document_iter<'a, I>(documents: I) -> Self
    where
        I: Clone + Iterator<Item = (usize, &'a CompactMetadataContentDocument)>,
    {
        let token_count = documents
            .clone()
            .flat_map(|(_, doc)| doc.terms.iter().map(|&(token_id, _)| token_id as usize + 1))
            .max()
            .unwrap_or(0);
        let posting_count = documents
            .clone()
            .map(|(_, doc)| doc.terms.len())
            .sum::<usize>();
        let mut posting_offsets = vec![0u64; token_count.saturating_add(1)];
        for (_, document) in documents.clone() {
            for &(token_id, _) in &document.terms {
                posting_offsets[token_id as usize + 1] =
                    posting_offsets[token_id as usize + 1].saturating_add(1);
            }
        }
        for token_index in 0..token_count {
            posting_offsets[token_index + 1] =
                posting_offsets[token_index + 1].saturating_add(posting_offsets[token_index]);
        }

        let mut cursors = posting_offsets[..token_count].to_vec();
        let mut posting_atoms = vec![0; posting_count];
        for (atom_index, document) in documents {
            let compact_atom_index = metadata_doc_index_from_usize(atom_index);
            for &(token_id, _) in &document.terms {
                let cursor = &mut cursors[token_id as usize];
                posting_atoms[*cursor as usize] = compact_atom_index;
                *cursor = cursor.saturating_add(1);
            }
        }
        Self {
            posting_offsets,
            posting_atoms,
        }
    }

    #[cfg(test)]
    pub(in super::super) fn new(docs: &[CompactMetadataContentDocument]) -> Self {
        Self::from_document_iter(docs.iter().enumerate())
    }

    pub(in super::super) fn from_atoms(
        docs: &[CompactMetadataContentDocument],
        atoms: &[MetadataContentAtom],
    ) -> Self {
        Self::from_document_iter(atoms.iter().enumerate().map(|(atom_index, atom)| {
            (
                atom_index,
                &docs[metadata_doc_index_to_usize(atom.representative_record_index)],
            )
        }))
    }

    pub(in super::super) fn from_atoms_parallel(
        docs: &[CompactMetadataContentDocument],
        atoms: &[MetadataContentAtom],
    ) -> Self {
        // CSR construction is linear and writes each posting exactly once;
        // comparison sorting costs more than this memory-bandwidth pass. The
        // caller already builds the independent template index concurrently.
        Self::from_atoms(docs, atoms)
    }

    #[cfg(test)]
    pub(in super::super) fn append_candidates_after(
        &self,
        record_index: usize,
        document: &CompactMetadataContentDocument,
        scratch: &mut MetadataCandidateScratch,
    ) {
        let mut plan = MetadataCandidatePostingPlan::default();
        self.plan_candidates_after(record_index, document, &mut plan);
        self.append_planned_candidates(&plan, scratch);
    }

    pub(in super::super) fn plan_candidates_after(
        &self,
        record_index: usize,
        document: &CompactMetadataContentDocument,
        plan: &mut MetadataCandidatePostingPlan,
    ) -> usize {
        let compact_record_index = metadata_doc_index_from_usize(record_index);
        for &(token_id, _) in &document.terms {
            plan.content
                .push(self.posting_range_after(token_id, compact_record_index));
        }
        plan.content.iter().fold(0usize, |cost, range| {
            cost.saturating_add(range.end.saturating_sub(range.start))
        })
    }

    pub(in super::super) fn append_planned_candidates(
        &self,
        plan: &MetadataCandidatePostingPlan,
        scratch: &mut MetadataCandidateScratch,
    ) {
        for range in &plan.content {
            scratch.record_posting_visits(range.end.saturating_sub(range.start));
            for &right in &self.posting_atoms[range.start..range.end] {
                scratch.push_once(right);
            }
        }
    }

    pub(in super::super) fn posting_range_after(
        &self,
        token_id: u32,
        record_index: MetadataDocIndex,
    ) -> MetadataPostingRange {
        let token_index = token_id as usize;
        if token_index + 1 >= self.posting_offsets.len() {
            return MetadataPostingRange { start: 0, end: 0 };
        }
        let posting_start = self.posting_offsets[token_index] as usize;
        let posting_end = self.posting_offsets[token_index + 1] as usize;
        let posting = &self.posting_atoms[posting_start..posting_end];
        let relative_start = posting.partition_point(|&right| right <= record_index);
        MetadataPostingRange {
            start: posting_start + relative_start,
            end: posting_end,
        }
    }

    #[cfg(test)]
    pub(in super::super) fn len(&self) -> usize {
        self.posting_atoms.len()
    }

    #[cfg(test)]
    pub(in super::super) fn offset_count(&self) -> usize {
        self.posting_offsets.len()
    }

    #[cfg(test)]
    pub(in super::super) fn memory_bytes(&self) -> usize {
        self.posting_atoms
            .capacity()
            .saturating_mul(std::mem::size_of::<MetadataDocIndex>())
            .saturating_add(
                self.posting_offsets
                    .capacity()
                    .saturating_mul(std::mem::size_of::<u64>()),
            )
    }
}

impl MetadataSparseCandidatePostings {
    pub(in super::super) fn from_sorted_entries(entries: Vec<(u32, MetadataDocIndex)>) -> Self {
        let mut token_ids = Vec::new();
        let mut posting_offsets = Vec::new();
        let mut posting_atoms = Vec::with_capacity(entries.len());
        for (token_id, atom) in entries {
            if token_ids.last().copied() != Some(token_id) {
                token_ids.push(token_id);
                posting_offsets.push(posting_atoms.len() as u64);
            }
            posting_atoms.push(atom);
        }
        posting_offsets.push(posting_atoms.len() as u64);
        Self {
            token_ids,
            posting_offsets,
            posting_atoms,
        }
    }

    pub(in super::super) fn from_bounded_unsorted_entries(
        entries: Vec<(u32, MetadataDocIndex)>,
        key_count: usize,
    ) -> Self {
        let mut posting_offsets = vec![0u64; key_count.saturating_add(1)];
        for &(key, _) in &entries {
            posting_offsets[key as usize + 1] = posting_offsets[key as usize + 1].saturating_add(1);
        }
        for key in 0..key_count {
            posting_offsets[key + 1] =
                posting_offsets[key + 1].saturating_add(posting_offsets[key]);
        }
        let mut cursors = posting_offsets[..key_count].to_vec();
        let mut posting_atoms = vec![0; entries.len()];
        for (key, atom) in entries {
            let cursor = &mut cursors[key as usize];
            posting_atoms[*cursor as usize] = atom;
            *cursor = cursor.saturating_add(1);
        }
        Self {
            token_ids: (0..key_count as u32).collect(),
            posting_offsets,
            posting_atoms,
        }
    }

    pub(in super::super) fn posting_range_after(
        &self,
        token_id: u32,
        record_index: MetadataDocIndex,
    ) -> MetadataPostingRange {
        let Ok(token_index) = self.token_ids.binary_search(&token_id) else {
            return MetadataPostingRange { start: 0, end: 0 };
        };
        let posting_start = self.posting_offsets[token_index] as usize;
        let posting_end = self.posting_offsets[token_index + 1] as usize;
        let posting = &self.posting_atoms[posting_start..posting_end];
        let relative_start = posting.partition_point(|&right| right <= record_index);
        MetadataPostingRange {
            start: posting_start + relative_start,
            end: posting_end,
        }
    }

    pub(in super::super) fn append_planned_candidates(
        &self,
        ranges: &[MetadataPostingRange],
        scratch: &mut MetadataCandidateScratch,
    ) {
        for range in ranges {
            scratch.record_posting_visits(range.end.saturating_sub(range.start));
            for &right in &self.posting_atoms[range.start..range.end] {
                scratch.push_once(right);
            }
        }
    }
}

impl MetadataTemplateCandidateIndex {
    pub(in super::super) fn atom_entries(
        scoring: &CompactMetadataScoring,
        atoms: &[MetadataContentAtom],
        prefix: bool,
    ) -> Vec<(u32, MetadataDocIndex)> {
        let token_count = atoms
            .iter()
            .map(|atom| {
                let template = metadata_doc_index_to_usize(atom.template_doc_index);
                if prefix {
                    scoring.candidate_tokens(template).len()
                } else {
                    scoring.query_tokens(template).len()
                }
            })
            .sum();
        let mut entries = Vec::with_capacity(token_count);
        for (atom_index, atom) in atoms.iter().enumerate() {
            let atom_index = metadata_doc_index_from_usize(atom_index);
            let template = metadata_doc_index_to_usize(atom.template_doc_index);
            let tokens = if prefix {
                scoring.candidate_tokens(template)
            } else {
                scoring.query_tokens(template)
            };
            entries.extend(tokens.iter().map(|&token| (token, atom_index)));
        }
        entries
    }

    pub(in super::super) fn from_atoms(
        scoring: &CompactMetadataScoring,
        atoms: &[MetadataContentAtom],
    ) -> Self {
        let mut full_entries = Self::atom_entries(scoring, atoms, false);
        let mut prefix_entries = Self::atom_entries(scoring, atoms, true);
        full_entries.sort_unstable();
        prefix_entries.sort_unstable();
        Self {
            full: MetadataSparseCandidatePostings::from_sorted_entries(full_entries),
            prefix: MetadataSparseCandidatePostings::from_sorted_entries(prefix_entries),
        }
    }

    pub(in super::super) fn from_atoms_parallel(
        scoring: &CompactMetadataScoring,
        atoms: &[MetadataContentAtom],
    ) -> Self {
        let mut full_entries = Self::atom_entries(scoring, atoms, false);
        let mut prefix_entries = Self::atom_entries(scoring, atoms, true);
        rayon::join(
            || full_entries.par_sort_unstable(),
            || prefix_entries.par_sort_unstable(),
        );
        Self {
            full: MetadataSparseCandidatePostings::from_sorted_entries(full_entries),
            prefix: MetadataSparseCandidatePostings::from_sorted_entries(prefix_entries),
        }
    }

    #[cfg(test)]
    pub(in super::super) fn append_candidates_after(
        &self,
        atom_index: usize,
        atom: &MetadataContentAtom,
        scoring: &CompactMetadataScoring,
        scratch: &mut MetadataCandidateScratch,
    ) {
        let mut plan = MetadataCandidatePostingPlan::default();
        self.plan_candidates_after(atom_index, atom, scoring, &mut plan);
        self.append_planned_candidates(&plan, scratch);
    }

    pub(in super::super) fn plan_candidates_after(
        &self,
        atom_index: usize,
        atom: &MetadataContentAtom,
        scoring: &CompactMetadataScoring,
        plan: &mut MetadataCandidatePostingPlan,
    ) -> usize {
        let compact_atom_index = metadata_doc_index_from_usize(atom_index);
        let template = metadata_doc_index_to_usize(atom.template_doc_index);
        for &token in scoring.candidate_tokens(template) {
            plan.template_full
                .push(self.full.posting_range_after(token, compact_atom_index));
        }
        for &token in scoring.query_tokens(template) {
            plan.template_prefix
                .push(self.prefix.posting_range_after(token, compact_atom_index));
        }
        plan.template_full
            .iter()
            .chain(&plan.template_prefix)
            .fold(0usize, |cost, range| {
                cost.saturating_add(range.end.saturating_sub(range.start))
            })
    }

    pub(in super::super) fn append_planned_candidates(
        &self,
        plan: &MetadataCandidatePostingPlan,
        scratch: &mut MetadataCandidateScratch,
    ) {
        self.full
            .append_planned_candidates(&plan.template_full, scratch);
        self.prefix
            .append_planned_candidates(&plan.template_prefix, scratch);
    }
}

impl MetadataLocalCandidateIndex {
    pub(in super::super) fn estimate_production_posting_visits(
        &self,
        atom_index: usize,
        atom: &MetadataContentAtom,
        document: &CompactMetadataContentDocument,
        compatibility: MetadataTemplateCompatibility<'_>,
        posting_plan: &mut MetadataCandidatePostingPlan,
    ) -> usize {
        match self {
            Self::Conservative(index) => index.estimate_posting_visits_after(atom_index),
            _ => self.estimate_exact_posting_visits(
                atom_index,
                atom,
                document,
                compatibility,
                posting_plan,
            ),
        }
    }

    pub(in super::super) fn estimate_exact_posting_visits(
        &self,
        atom_index: usize,
        atom: &MetadataContentAtom,
        document: &CompactMetadataContentDocument,
        compatibility: MetadataTemplateCompatibility<'_>,
        posting_plan: &mut MetadataCandidatePostingPlan,
    ) -> usize {
        posting_plan.clear();
        match self {
            Self::Conservative(index) => {
                let scoring = compatibility
                    .scoring()
                    .expect("template candidate index requires scored compatibility");
                let template_cost = index
                    .exact_template
                    .as_ref()
                    .expect("exact metadata calibration index already released")
                    .plan_candidates_after(atom_index, atom, scoring, posting_plan);
                let content_cost = index
                    .exact_content
                    .as_ref()
                    .expect("exact metadata calibration index already released")
                    .plan_candidates_after(atom_index, document, posting_plan);
                Self::planned_exact_posting_visits(template_cost, content_cost)
            }
            Self::Adaptive { template, content } => {
                let scoring = compatibility
                    .scoring()
                    .expect("template candidate index requires scored compatibility");
                let template_cost =
                    template.plan_candidates_after(atom_index, atom, scoring, posting_plan);
                let content_cost =
                    content.plan_candidates_after(atom_index, document, posting_plan);
                Self::planned_exact_posting_visits(template_cost, content_cost)
            }
            #[cfg(test)]
            Self::ContentOnly(index) => {
                index.plan_candidates_after(atom_index, document, posting_plan)
            }
        }
    }

    fn planned_exact_posting_visits(template_cost: usize, content_cost: usize) -> usize {
        let minimum_cost = template_cost.min(content_cost);
        let maximum_cost = template_cost.max(content_cost);
        if minimum_cost >= METADATA_DENSE_INTERSECTION_MIN_SCAN_COST
            && maximum_cost
                <= minimum_cost.saturating_mul(METADATA_DENSE_INTERSECTION_MAX_COST_RATIO)
        {
            template_cost.saturating_add(content_cost)
        } else {
            minimum_cost
        }
    }

    #[cfg(test)]
    pub(in super::super) fn from_atoms(
        docs: &[CompactMetadataContentDocument],
        atoms: &[MetadataContentAtom],
        compatibility: MetadataTemplateCompatibility<'_>,
        parallel: bool,
    ) -> Self {
        Self::from_atoms_with_mode(
            docs,
            atoms,
            compatibility,
            parallel,
            MetadataRecallMode::Exact,
        )
    }

    pub(in super::super) fn from_atoms_with_mode(
        docs: &[CompactMetadataContentDocument],
        atoms: &[MetadataContentAtom],
        compatibility: MetadataTemplateCompatibility<'_>,
        parallel: bool,
        recall_mode: MetadataRecallMode,
    ) -> Self {
        match compatibility {
            MetadataTemplateCompatibility::Scored(scoring) => {
                if recall_mode == MetadataRecallMode::Conservative {
                    let ((exact_template, template), (exact_content, content)) = if parallel {
                        rayon::join(
                            || {
                                (
                                    MetadataTemplateCandidateIndex::from_atoms_parallel(
                                        scoring, atoms,
                                    ),
                                    MetadataConservativeDimensionIndex::from_template_docs(
                                        scoring, atoms, true,
                                    ),
                                )
                            },
                            || {
                                (
                                    MetadataContentCandidateIndex::from_atoms_parallel(docs, atoms),
                                    MetadataConservativeDimensionIndex::from_content_docs(
                                        docs, atoms, true,
                                    ),
                                )
                            },
                        )
                    } else {
                        (
                            (
                                MetadataTemplateCandidateIndex::from_atoms(scoring, atoms),
                                MetadataConservativeDimensionIndex::from_template_docs(
                                    scoring, atoms, false,
                                ),
                            ),
                            (
                                MetadataContentCandidateIndex::from_atoms(docs, atoms),
                                MetadataConservativeDimensionIndex::from_content_docs(
                                    docs, atoms, false,
                                ),
                            ),
                        )
                    };
                    let joint_bands =
                        (atoms.len() >= METADATA_CONSERVATIVE_JOINT_MIN_ATOMS).then(|| {
                            MetadataConservativeJointBandIndex::from_dimensions(
                                &template, &content, parallel,
                            )
                        });
                    return Self::Conservative(Box::new(MetadataConservativeCandidateIndex {
                        exact_template: Some(exact_template),
                        exact_content: Some(exact_content),
                        template,
                        content,
                        joint_bands,
                        profile: MetadataConservativeRecallProfile::Base,
                    }));
                }
                let (template, content) = if parallel {
                    rayon::join(
                        || MetadataTemplateCandidateIndex::from_atoms_parallel(scoring, atoms),
                        || MetadataContentCandidateIndex::from_atoms_parallel(docs, atoms),
                    )
                } else {
                    (
                        MetadataTemplateCandidateIndex::from_atoms(scoring, atoms),
                        MetadataContentCandidateIndex::from_atoms(docs, atoms),
                    )
                };
                Self::Adaptive { template, content }
            }
            #[cfg(test)]
            MetadataTemplateCompatibility::Precomputed(_) => {
                let index = if parallel {
                    MetadataContentCandidateIndex::from_atoms_parallel(docs, atoms)
                } else {
                    MetadataContentCandidateIndex::from_atoms(docs, atoms)
                };
                Self::ContentOnly(index)
            }
        }
    }

    pub(in super::super) fn append_candidates_after(
        &self,
        atom_index: usize,
        atom: &MetadataContentAtom,
        document: &CompactMetadataContentDocument,
        compatibility: MetadataTemplateCompatibility<'_>,
        scratch: &mut MetadataCandidateScratch,
    ) -> MetadataLocalCandidateBasis {
        match self {
            Self::Conservative(index) => {
                index.append_candidates_after(atom_index, scratch);
                MetadataLocalCandidateBasis::ConservativeIntersection
            }
            Self::Adaptive { template, content } => Self::append_exact_index_candidates_after(
                template,
                content,
                atom_index,
                atom,
                document,
                compatibility,
                scratch,
            ),
            #[cfg(test)]
            Self::ContentOnly(index) => {
                index.append_candidates_after(atom_index, document, scratch);
                scratch.raw_candidate_count = scratch.candidates.len();
                MetadataLocalCandidateBasis::Content
            }
        }
    }

    pub(in super::super) fn append_exact_index_candidates_after(
        template: &MetadataTemplateCandidateIndex,
        content: &MetadataContentCandidateIndex,
        atom_index: usize,
        atom: &MetadataContentAtom,
        document: &CompactMetadataContentDocument,
        compatibility: MetadataTemplateCompatibility<'_>,
        scratch: &mut MetadataCandidateScratch,
    ) -> MetadataLocalCandidateBasis {
        let scoring = compatibility
            .scoring()
            .expect("template candidate index requires scored compatibility");
        let mut posting_plan = std::mem::take(&mut scratch.posting_plan);
        posting_plan.clear();
        let template_cost =
            template.plan_candidates_after(atom_index, atom, scoring, &mut posting_plan);
        let content_cost = content.plan_candidates_after(atom_index, document, &mut posting_plan);
        let minimum_cost = template_cost.min(content_cost);
        let maximum_cost = template_cost.max(content_cost);
        let basis = if minimum_cost >= METADATA_DENSE_INTERSECTION_MIN_SCAN_COST
            && maximum_cost
                <= minimum_cost.saturating_mul(METADATA_DENSE_INTERSECTION_MAX_COST_RATIO)
        {
            if content_cost < template_cost {
                template.append_planned_candidates(&posting_plan, scratch);
                scratch.prepare_secondary_generation();
                content.append_planned_candidates(&posting_plan, scratch);
            } else {
                content.append_planned_candidates(&posting_plan, scratch);
                scratch.prepare_secondary_generation();
                template.append_planned_candidates(&posting_plan, scratch);
            }
            scratch.raw_candidate_count = scratch.candidates.len();
            scratch.retain_secondary_intersection();
            MetadataLocalCandidateBasis::Intersection
        } else if content_cost < template_cost {
            content.append_planned_candidates(&posting_plan, scratch);
            scratch.raw_candidate_count = scratch.candidates.len();
            MetadataLocalCandidateBasis::Content
        } else {
            template.append_planned_candidates(&posting_plan, scratch);
            scratch.raw_candidate_count = scratch.candidates.len();
            MetadataLocalCandidateBasis::Template
        };
        scratch.posting_plan = posting_plan;
        basis
    }

    pub(in super::super) fn append_exact_candidates_after(
        &self,
        atom_index: usize,
        atom: &MetadataContentAtom,
        document: &CompactMetadataContentDocument,
        compatibility: MetadataTemplateCompatibility<'_>,
        scratch: &mut MetadataCandidateScratch,
    ) -> MetadataLocalCandidateBasis {
        match self {
            Self::Conservative(index) => Self::append_exact_index_candidates_after(
                index
                    .exact_template
                    .as_ref()
                    .expect("exact metadata calibration index already released"),
                index
                    .exact_content
                    .as_ref()
                    .expect("exact metadata calibration index already released"),
                atom_index,
                atom,
                document,
                compatibility,
                scratch,
            ),
            _ => self.append_candidates_after(atom_index, atom, document, compatibility, scratch),
        }
    }

    pub(in super::super) fn into_effective_recall(
        self,
        exact_recall: bool,
        retain_exact_rescue_indexes: bool,
    ) -> Self {
        match self {
            Self::Conservative(mut index) if exact_recall => Self::Adaptive {
                template: index
                    .exact_template
                    .take()
                    .expect("exact metadata calibration index already released"),
                content: index
                    .exact_content
                    .take()
                    .expect("exact metadata calibration index already released"),
            },
            Self::Conservative(index) if retain_exact_rescue_indexes => Self::Conservative(index),
            Self::Conservative(mut index) => {
                index.exact_template = None;
                index.exact_content = None;
                Self::Conservative(index)
            }
            index => index,
        }
    }
}
