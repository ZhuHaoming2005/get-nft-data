use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use duckdb::Connection;
use rayon::prelude::*;

use super::bm25::{
    compact_metadata_content_pair_score, CompactMetadataContentDocument,
    CompactMetadataContentSet, InternedMetadataCorpus, InternedMetadataDoc,
    InternedMetadataSourceDoc, MetadataBm25Document, MetadataContentRecord,
    PreparedInternedMetadataDoc, PreparedInternedMetadataQuery,
    score_metadata_with_prepared_doc,
};
use super::parse::metadata_document_from_json;
use super::sketch::{
    metadata_sketch_from_interned_document, metadata_sketch_source_match,
    stable_metadata_token_hash, MetadataSketch, METADATA_SKETCH_SOURCE_HAMMING_THRESHOLD,
};
use super::super::{
    arrow_i64_column, arrow_string_column, chain_pair_index, AnalysisError, ProgressTracker,
    SparseUnionFind, UnionFind,
};
use super::{
    metadata_contract_index_from_usize, metadata_contract_index_to_usize,
    metadata_doc_index_from_usize, metadata_doc_index_to_usize, MetadataContractIndex,
    MetadataData, MetadataDocIndex, MetadataTemplateMatches, SourceMetadataDocEntry,
    METADATA_CONTENT_PARALLEL_MIN_RECORDS, METADATA_CONTENT_SCORE_BATCH_PAIRS,
    METADATA_PAIR_LEFT_CHUNK_SIZE, METADATA_THRESHOLD,
};

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
    pub(super) postings: HashMap<(u32, MetadataDocIndex), Vec<MetadataDocIndex>>,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum MetadataContentScope {
    SharedToken,
    NoCommonToken,
}

#[derive(Debug)]
pub(crate) struct InternedMetadataIndex {
    pub(super) docs: Vec<InternedMetadataDoc>,
    pub(super) corpus: InternedMetadataCorpus,
    pub(super) queries: Vec<PreparedInternedMetadataQuery>,
    pub(super) prepared_docs: Vec<PreparedInternedMetadataDoc>,
    pub(super) postings: Vec<Vec<MetadataDocIndex>>,
    pub(super) sketches: Vec<MetadataSketch>,
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
    pub(super) docs: &'a [InternedMetadataDoc],
    pub(super) sketches: &'a [MetadataSketch],
    pub(super) postings: &'a [Vec<MetadataDocIndex>],
    pub(super) queries: &'a [PreparedInternedMetadataQuery],
    pub(super) prepared_docs: &'a [PreparedInternedMetadataDoc],
}

pub(super) struct MetadataContentUnionContext<'a> {
    pub(super) data: &'a MetadataData,
    pub(super) template_matches: &'a MetadataTemplateMatches,
    pub(super) contract_tokens: &'a [Vec<u32>],
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

impl MetadataContentCandidateIndex {
    #[cfg(test)]
    pub(super) fn new(
        docs: &[CompactMetadataContentDocument],
        template_docs: &[MetadataDocIndex],
    ) -> Self {
        debug_assert_eq!(docs.len(), template_docs.len());
        let mut postings = HashMap::new();
        for (record_index, (doc, &template_doc)) in
            docs.iter().zip(template_docs).enumerate()
        {
            let record_index = metadata_doc_index_from_usize(record_index);
            for &(token_id, _) in &doc.terms {
                postings
                    .entry((token_id, template_doc))
                    .or_insert_with(Vec::new)
                    .push(record_index);
            }
        }
        Self { postings }
    }

    pub(super) fn from_atoms(
        docs: &[CompactMetadataContentDocument],
        atoms: &[MetadataContentAtom],
    ) -> Self {
        let mut postings = HashMap::new();
        for (atom_index, atom) in atoms.iter().enumerate() {
            let compact_atom_index = metadata_doc_index_from_usize(atom_index);
            let doc =
                &docs[metadata_doc_index_to_usize(atom.representative_record_index)];
            for &(token_id, _) in &doc.terms {
                postings
                    .entry((token_id, atom.template_doc_index))
                    .or_insert_with(Vec::new)
                    .push(compact_atom_index);
            }
        }
        Self { postings }
    }

    pub(super) fn from_atoms_parallel(
        docs: &[CompactMetadataContentDocument],
        atoms: &[MetadataContentAtom],
    ) -> Self {
        let mut postings = (0..atoms.len())
            .into_par_iter()
            .fold(HashMap::new, |mut local, atom_index| {
                let atom = &atoms[atom_index];
                let doc = &docs[metadata_doc_index_to_usize(
                    atom.representative_record_index,
                )];
                let compact_atom_index =
                    metadata_doc_index_from_usize(atom_index);
                for &(token_id, _) in &doc.terms {
                    local
                        .entry((token_id, atom.template_doc_index))
                        .or_insert_with(Vec::new)
                        .push(compact_atom_index);
                }
                local
            })
            .reduce(HashMap::new, |mut left, mut right| {
                if left.len() < right.len() {
                    std::mem::swap(&mut left, &mut right);
                }
                for (key, mut posting) in right {
                    left.entry(key)
                        .or_insert_with(Vec::new)
                        .append(&mut posting);
                }
                left
            });
        postings
            .par_iter_mut()
            .for_each(|(_, posting)| posting.sort_unstable());
        Self { postings }
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
            self.append_posting_after(
                token_id,
                template_doc,
                compact_record_index,
                scratch,
            );
            for &compatible_doc in template_matches.compatible_docs(template_doc) {
                self.append_posting_after(
                    token_id,
                    compatible_doc,
                    compact_record_index,
                    scratch,
                );
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
        let Some(posting) = self.postings.get(&(token_id, template_doc)) else {
            return;
        };
        let start = posting.partition_point(|&right| right <= record_index);
        for &right in &posting[start..] {
            scratch.push_once(right);
        }
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
) -> MetadataTemplateMatches {
    let index = &data.metadata_index;
    if index.corpus.total_docs == 0 {
        return MetadataTemplateMatches::default();
    }
    let scoring_left_count = index.docs.len();
    let mut scored_candidate_pairs = 0u64;
    let mut scored_left_docs = 0usize;
    let mut matched_doc_pairs = 0u64;
    let mut matched_docs = Vec::new();
    let progress_start = Instant::now();
    let scratch_pool = MetadataCandidateScratchPool::new(index.docs.len());
    progress.add_work(metadata_scoring_progress_units(scoring_left_count));
    progress.set_message(metadata_pair_progress_message(
        scored_candidate_pairs,
        scored_left_docs,
        scoring_left_count,
        matched_doc_pairs,
        progress_start.elapsed(),
    ));
    for left_start in (0..scoring_left_count).step_by(METADATA_PAIR_LEFT_CHUNK_SIZE) {
        let left_end = (left_start + METADATA_PAIR_LEFT_CHUNK_SIZE).min(scoring_left_count);
        let batch = collect_metadata_doc_pair_hits_for_left_range(
            left_start..left_end,
            MetadataPairScoringContext {
                docs: &index.docs,
                sketches: &index.sketches,
                postings: &index.postings,
                queries: &index.queries,
                prepared_docs: &index.prepared_docs,
            },
            &scratch_pool,
        );
        scored_candidate_pairs = scored_candidate_pairs.saturating_add(batch.candidate_pairs);
        scored_left_docs = left_end;
        matched_doc_pairs = matched_doc_pairs.saturating_add(batch.hits.len() as u64);
        progress.inc(metadata_scoring_batch_progress_units(left_start, left_end));
        progress.set_message(metadata_pair_progress_message(
            scored_candidate_pairs,
            scored_left_docs,
            scoring_left_count,
            matched_doc_pairs,
            progress_start.elapsed(),
        ));
        matched_docs.extend(batch.hits);
    }
    matched_docs.sort_unstable();
    matched_docs.dedup();
    MetadataTemplateMatches::from_pairs(matched_docs)
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
) -> Result<(), AnalysisError> {
    let mut stmt = conn.prepare(
        "
        WITH shared_tokens AS (
            SELECT token_index
            FROM metadata_contract_token_rows
            GROUP BY token_index
            HAVING count(*) >= 2
        )
        SELECT t.token_index, t.contract_index, a.metadata_json
        FROM metadata_contract_token_rows t
        JOIN shared_tokens s ON s.token_index = t.token_index
        JOIN analysis_rows a ON a.rowid = t.metadata_row_id
        ORDER BY t.token_index, t.contract_index
        ",
    )?;
    let mut current_token = None;
    let mut raw_records = Vec::new();
    for batch in stmt.query_arrow([])? {
        let token_column = arrow_i64_column(&batch, 0, "token_index")?;
        let contract_column = arrow_i64_column(&batch, 1, "contract_index")?;
        let metadata_column = arrow_string_column(&batch, 2, "metadata_json")?;
        for row_index in 0..batch.num_rows() {
            let token_index =
                u32::try_from(token_column.value(row_index)).map_err(|_| {
                    AnalysisError::InvalidData(
                        "metadata token dictionary exceeds compact u32 indexes".to_string(),
                    )
                })?;
            if current_token.is_some_and(|current| current != token_index) {
                union_metadata_raw_token_group(
                    std::mem::take(&mut raw_records),
                    context,
                    state,
                );
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
            raw_records.push((
                contract_index,
                metadata_column.value(row_index).to_owned(),
            ));
        }
    }
    if current_token.is_some() {
        union_metadata_raw_token_group(raw_records, context, state);
    }
    Ok(())
}

pub(super) fn union_metadata_raw_token_group(
    raw_records: Vec<(MetadataContractIndex, String)>,
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
) {
    if raw_records.len() < 2 {
        return;
    }
    let records = if raw_records.len() >= METADATA_CONTENT_PARALLEL_MIN_RECORDS {
        context.pool.install(|| {
            raw_records
                .into_par_iter()
                .filter_map(|(contract_index, metadata_json)| {
                    MetadataBm25Document::from_text(
                        &metadata_document_from_json(&metadata_json),
                    )
                    .map(|doc| MetadataContentRecord {
                        contract_index,
                        doc: Arc::new(doc),
                    })
                })
                .collect::<Vec<_>>()
        })
    } else {
        raw_records
            .into_iter()
            .filter_map(|(contract_index, metadata_json)| {
                MetadataBm25Document::from_text(&metadata_document_from_json(
                    &metadata_json,
                ))
                .map(|doc| MetadataContentRecord {
                    contract_index,
                    doc: Arc::new(doc),
                })
            })
            .collect::<Vec<_>>()
    };
    union_metadata_content_candidates(
        &records,
        MetadataContentScope::SharedToken,
        context,
        state,
    );
}

pub(super) fn union_metadata_representative_content_fallback(
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
) {
    let records = if context.data.contracts.len()
        >= METADATA_CONTENT_PARALLEL_MIN_RECORDS
    {
        context.pool.install(|| {
            context
                .data
                .contracts
                .par_iter()
                .enumerate()
                .filter_map(|(contract_index, contract)| {
                    contract.content_doc.clone().map(|doc| {
                        MetadataContentRecord {
                            contract_index: metadata_contract_index_from_usize(
                                contract_index,
                            ),
                            doc,
                        }
                    })
                })
                .collect::<Vec<_>>()
        })
    } else {
        context
            .data
            .contracts
            .iter()
            .enumerate()
            .filter_map(|(contract_index, contract)| {
                contract
                    .content_doc
                    .clone()
                    .map(|doc| MetadataContentRecord {
                        contract_index: metadata_contract_index_from_usize(
                            contract_index,
                        ),
                        doc,
                    })
            })
            .collect::<Vec<_>>()
    };
    union_metadata_content_candidates(
        &records,
        MetadataContentScope::NoCommonToken,
        context,
        state,
    );
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
            apply_metadata_contract_pair_union(
                data,
                chain_count,
                state,
                anchor,
                member,
            );
        }
    }
    for left_chain in 0..chain_count {
        let Some((&left_anchor, left_rest)) =
            members_by_chain[left_chain].split_first()
        else {
            continue;
        };
        for right_members in members_by_chain.iter().skip(left_chain + 1) {
            let Some((&right_anchor, right_rest)) = right_members.split_first() else {
                continue;
            };
            apply_metadata_contract_pair_union(
                data,
                chain_count,
                state,
                left_anchor,
                right_anchor,
            );
            for &right in right_rest {
                apply_metadata_contract_pair_union(
                    data,
                    chain_count,
                    state,
                    left_anchor,
                    right,
                );
            }
            for &left in left_rest {
                apply_metadata_contract_pair_union(
                    data,
                    chain_count,
                    state,
                    left,
                    right_anchor,
                );
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
    contract_tokens: &'a [Vec<u32>],
) -> &'a [u32] {
    let representative =
        metadata_contract_index_to_usize(group.members[0]);
    &contract_tokens[representative]
}

pub(super) fn metadata_fallback_token_groups_are_disjoint(
    left: &MetadataFallbackTokenGroup,
    right: &MetadataFallbackTokenGroup,
    contract_tokens: &[Vec<u32>],
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
        if metadata_fallback_token_group_tokens(group, context.contract_tokens)
            .is_empty()
        {
            apply_metadata_complete_match_group_union(
                context.data,
                context.chain_count,
                state,
                &group.members,
            );
        }
    }

    let mut unvisited =
        (0..atom.fallback_token_groups.len()).collect::<Vec<_>>();
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
    contract_tokens: &[Vec<u32>],
) -> bool {
    left.fallback_token_groups.iter().any(|left_group| {
        right.fallback_token_groups.iter().any(|right_group| {
            metadata_fallback_token_groups_are_disjoint(
                left_group,
                right_group,
                contract_tokens,
            )
        })
    })
}

pub(super) fn apply_metadata_fallback_atom_pair_union(
    left: &MetadataContentAtom,
    right: &MetadataContentAtom,
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
) {
    let mut unvisited_left =
        (0..left.fallback_token_groups.len()).collect::<Vec<_>>();
    let mut unvisited_right =
        (0..right.fallback_token_groups.len()).collect::<Vec<_>>();
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
                    (
                        current_group,
                        &right.fallback_token_groups[other],
                    )
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
    pub(super) hits: Vec<(usize, usize)>,
    pub(super) candidate_pairs: u64,
}

pub(super) fn metadata_scoring_progress_units(scoring_left_count: usize) -> u64 {
    scoring_left_count as u64
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

pub(super) fn format_metadata_pair_eta(remaining_pairs: u64, scored_pairs: u64, elapsed: Duration) -> String {
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

pub(super) fn collect_metadata_doc_pair_hits_for_left_range(
    left_range: std::ops::Range<usize>,
    context: MetadataPairScoringContext<'_>,
    scratch_pool: &MetadataCandidateScratchPool,
) -> MetadataDocPairBatch {
    let context = &context;
    let (mut hits, candidate_pairs) = left_range
        .into_par_iter()
        .map_init(
            || scratch_pool.take(),
            |scratch, left| {
                let mut local_hits = Vec::new();
                let local_candidate_pairs = collect_metadata_doc_pair_hits_for_left_with_scratch(
                    left,
                    context,
                    scratch,
                    &mut local_hits,
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
    hits.sort_unstable();
    hits.dedup();
    MetadataDocPairBatch {
        hits,
        candidate_pairs,
    }
}

pub(super) fn collect_metadata_doc_pair_hits_for_left_with_scratch(
    left: usize,
    context: &MetadataPairScoringContext<'_>,
    scratch: &mut MetadataCandidateScratch,
    hits: &mut Vec<(usize, usize)>,
) -> u64 {
    let candidates =
        metadata_candidate_indices_for_left_with_scratch(left, context, scratch);
    let mut scored_candidates = 0u64;
    for &right in candidates {
        let right = metadata_doc_index_to_usize(right);
        if !interned_metadata_docs_share_token(&context.docs[left], &context.docs[right])
            || !metadata_sketch_source_match(
                &context.sketches[left],
                &context.sketches[right],
                METADATA_SKETCH_SOURCE_HAMMING_THRESHOLD,
            )
        {
            continue;
        }
        scored_candidates = scored_candidates.saturating_add(1);
        if score_metadata_with_prepared_doc(&context.queries[left], &context.prepared_docs[right])
            >= METADATA_THRESHOLD
        {
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
    for &token in &context.queries[left].candidate_tokens {
        append_metadata_posting_except(&context.postings[token], compact_left, scratch);
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

pub(super) fn ordered_metadata_doc_pair(left: usize, right: usize) -> (usize, usize) {
    if left <= right {
        (left, right)
    } else {
        (right, left)
    }
}

pub(super) fn interned_metadata_docs_share_token(
    left: &InternedMetadataDoc,
    right: &InternedMetadataDoc,
) -> bool {
    let mut left_index = 0usize;
    let mut right_index = 0usize;
    while left_index < left.unique_tokens.len() && right_index < right.unique_tokens.len() {
        match left.unique_tokens[left_index].cmp(&right.unique_tokens[right_index]) {
            std::cmp::Ordering::Equal => return true,
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
        }
    }
    false
}

pub(super) fn metadata_content_pair_matches(
    left: &CompactMetadataContentDocument,
    right: &CompactMetadataContentDocument,
    threshold: f64,
) -> bool {
    compact_metadata_content_pair_score(left, right) >= threshold
}

pub(super) fn build_metadata_content_atoms(
    records: &[MetadataContentRecord],
    compact_docs: &[CompactMetadataContentDocument],
    data: &MetadataData,
) -> Vec<MetadataContentAtom> {
    let mut atom_index_by_key =
        HashMap::<(usize, MetadataDocIndex, &[(u32, usize)]), usize>::new();
    let mut atoms = Vec::<MetadataContentAtom>::new();
    for (record_index, record) in records.iter().enumerate() {
        let contract_index =
            metadata_contract_index_to_usize(record.contract_index);
        let contract = &data.contracts[contract_index];
        let key = (
            contract.chain_index,
            contract.template_doc_index,
            compact_docs[record_index].terms.as_slice(),
        );
        if let Some(&atom_index) = atom_index_by_key.get(&key) {
            atoms[atom_index].members.push(record.contract_index);
            continue;
        }
        let atom_index = atoms.len();
        atom_index_by_key.insert(key, atom_index);
        atoms.push(MetadataContentAtom {
            chain_index: contract.chain_index,
            template_doc_index: contract.template_doc_index,
            representative_record_index: metadata_doc_index_from_usize(
                record_index,
            ),
            members: vec![record.contract_index],
            fallback_token_groups: Vec::new(),
        });
    }
    atoms
}

pub(super) fn build_metadata_fallback_atoms(
    records: &[MetadataContentRecord],
    compact_docs: &[CompactMetadataContentDocument],
    data: &MetadataData,
    contract_tokens: &[Vec<u32>],
) -> Vec<MetadataContentAtom> {
    let mut atom_index_by_key =
        HashMap::<(usize, MetadataDocIndex, &[(u32, usize)]), usize>::new();
    let mut token_group_index_by_atom = Vec::<HashMap<&[u32], usize>>::new();
    let mut atoms = Vec::<MetadataContentAtom>::new();
    for (record_index, record) in records.iter().enumerate() {
        let contract_index =
            metadata_contract_index_to_usize(record.contract_index);
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
            let tokens = contract_tokens[contract_index].as_slice();
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
        token_group_index_by_atom.push(HashMap::from([(
            contract_tokens[contract_index].as_slice(),
            0,
        )]));
        atoms.push(MetadataContentAtom {
            chain_index: contract.chain_index,
            template_doc_index: contract.template_doc_index,
            representative_record_index: metadata_doc_index_from_usize(
                record_index,
            ),
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
    let left_record = metadata_doc_index_to_usize(
        atoms[left].representative_record_index,
    );
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
                .filter(|&pair| {
                    metadata_content_atom_pair_matches(pair, atoms, compact_docs)
                })
                .collect()
        })
    } else {
        candidate_pairs
            .iter()
            .copied()
            .filter(|&pair| {
                metadata_content_atom_pair_matches(pair, atoms, compact_docs)
            })
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
    let hits = collect_metadata_content_atom_pair_hits(
        candidate_pairs,
        atoms,
        compact_docs,
        context.pool,
    );
    candidate_pairs.clear();
    for (left, right) in hits {
        let left_atom = &atoms[left];
        let right_atom = &atoms[metadata_doc_index_to_usize(right)];
        let mut members =
            Vec::with_capacity(left_atom.members.len() + right_atom.members.len());
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
    let hits = collect_metadata_content_atom_pair_hits(
        candidate_pairs,
        atoms,
        compact_docs,
        context.pool,
    );
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

pub(super) fn union_metadata_shared_token_atoms(
    records: &[MetadataContentRecord],
    compact_docs: &[CompactMetadataContentDocument],
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
) -> MetadataContentUnionStats {
    let atoms = build_metadata_content_atoms(records, compact_docs, context.data);
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
        context.pool.install(|| {
            MetadataContentCandidateIndex::from_atoms_parallel(compact_docs, &atoms)
        })
    } else {
        MetadataContentCandidateIndex::from_atoms(compact_docs, &atoms)
    };
    let mut scratch = MetadataCandidateScratch::new(atoms.len());
    let mut candidate_pairs =
        Vec::with_capacity(METADATA_CONTENT_SCORE_BATCH_PAIRS);
    for left in 0..atoms.len().saturating_sub(1) {
        let left_atom = &atoms[left];
        let left_record_index = metadata_doc_index_to_usize(
            left_atom.representative_record_index,
        );
        let left_contract_index =
            metadata_contract_index_to_usize(left_atom.members[0]);
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
            let right_contract_index =
                metadata_contract_index_to_usize(right_atom.members[0]);
            debug_assert!(context.template_matches.matches(
                metadata_doc_index_to_usize(left_atom.template_doc_index),
                metadata_doc_index_to_usize(right_atom.template_doc_index),
            ));
            let singleton_pair =
                left_atom.members.len() == 1 && right_atom.members.len() == 1;
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
    stats.scored_pairs = stats.scored_pairs.saturating_add(
        score_and_apply_metadata_atom_pair_batch(
            &mut candidate_pairs,
            &atoms,
            compact_docs,
            context,
            state,
        ),
    );
    stats
}

pub(super) fn union_metadata_no_common_content_candidates(
    records: &[MetadataContentRecord],
    compact_docs: &[CompactMetadataContentDocument],
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
) -> MetadataContentUnionStats {
    let atoms =
        build_metadata_fallback_atoms(
            records,
            compact_docs,
            context.data,
            context.contract_tokens,
        );
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
        context.pool.install(|| {
            MetadataContentCandidateIndex::from_atoms_parallel(compact_docs, &atoms)
        })
    } else {
        MetadataContentCandidateIndex::from_atoms(compact_docs, &atoms)
    };
    let mut scratch = MetadataCandidateScratch::new(atoms.len());
    let mut candidate_pairs =
        Vec::with_capacity(METADATA_CONTENT_SCORE_BATCH_PAIRS);
    for left in 0..atoms.len().saturating_sub(1) {
        let left_atom = &atoms[left];
        let left_record_index = metadata_doc_index_to_usize(
            left_atom.representative_record_index,
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
        let left_contract_index =
            metadata_contract_index_to_usize(left_atom.members[0]);
        for &right in &scratch.candidates {
            let right_atom = &atoms[metadata_doc_index_to_usize(right)];
            let right_index =
                metadata_contract_index_to_usize(right_atom.members[0]);
            debug_assert!(context.template_matches.matches(
                metadata_doc_index_to_usize(left_atom.template_doc_index),
                metadata_doc_index_to_usize(right_atom.template_doc_index),
            ));
            let singleton_pair =
                left_atom.members.len() == 1 && right_atom.members.len() == 1;
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
    stats.scored_pairs = stats.scored_pairs.saturating_add(
        score_and_apply_metadata_fallback_atom_pair_batch(
            &mut candidate_pairs,
            &atoms,
            compact_docs,
            context,
            state,
        ),
    );
    stats
}

pub(super) fn union_metadata_content_candidates(
    records: &[MetadataContentRecord],
    scope: MetadataContentScope,
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
) -> MetadataContentUnionStats {
    let compact = CompactMetadataContentSet::from_records(records);
    match scope {
        MetadataContentScope::SharedToken => {
            union_metadata_shared_token_atoms(
                records,
                &compact.docs,
                context,
                state,
            )
        }
        MetadataContentScope::NoCommonToken => {
            union_metadata_no_common_content_candidates(
                records,
                &compact.docs,
                context,
                state,
            )
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
        matrix[chain_pair_index(primary_chain, secondary_chain, chain_count)]
            .connected(left, right)
    });
    cross_connected && matrix_connected
}

pub(super) fn lexical_metadata_token_ids(entries: &[SourceMetadataDocEntry]) -> HashMap<String, usize> {
    let mut tokens = entries
        .iter()
        .flat_map(|entry| entry.doc.unique_tokens.iter().cloned())
        .collect::<Vec<_>>();
    tokens.sort_unstable();
    tokens.dedup();
    tokens
        .into_iter()
        .enumerate()
        .map(|(token_id, token)| (token, token_id))
        .collect()
}

impl InternedMetadataIndex {
    pub(super) fn from_source_doc_entries(entries: Vec<SourceMetadataDocEntry>) -> Self {
        let token_ids = lexical_metadata_token_ids(&entries);
        let token_count = token_ids.len();

        // Phase 1 (parallel): build per-doc source docs and weights. Each doc
        // does its own tokenization + term-frequency HashMap + unique-token
        // sort, which is the expensive per-doc work; `unzip` preserves
        // doc-index order.
        let (doc_weights, source_docs): (Vec<usize>, Vec<InternedMetadataSourceDoc>) = entries
            .into_par_iter()
            .map(|entry| {
                let doc_weight = entry.contracts.len();
                let source_doc =
                    InternedMetadataSourceDoc::from_metadata_doc(&entry.doc, &token_ids);
                (doc_weight, source_doc)
            })
            .unzip();

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
        let mut token_hashes = vec![0u64; token_count];
        for (token, &token_id) in &token_ids {
            token_hashes[token_id] = stable_metadata_token_hash(token);
        }
        let sketches = source_docs
            .par_iter()
            .map(|doc| metadata_sketch_from_interned_document(doc, &corpus, &token_hashes))
            .collect::<Vec<_>>();
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
        let queries = source_docs
            .par_iter()
            .map(|doc| {
                PreparedInternedMetadataQuery::new(
                    doc,
                    &corpus,
                    &max_token_weights,
                    &postings,
                )
            })
            .collect::<Vec<_>>();
        let docs = source_docs
            .into_iter()
            .map(InternedMetadataDoc::from_source_doc)
            .collect();
        Self {
            docs,
            corpus,
            queries,
            prepared_docs,
            postings,
            sketches,
            #[cfg(test)]
            token_ids,
            #[cfg(test)]
            build_thread_count: rayon::current_num_threads(),
        }
    }

    #[cfg(test)]
    pub(super) fn token_id(&self, token: &str) -> Option<usize> {
        self.token_ids.get(token).copied()
    }
}
