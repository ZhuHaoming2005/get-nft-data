use std::collections::HashMap;
use std::sync::Arc;

use duckdb::Connection;

use super::{
    accumulate_pair_component_summary, chain_pair_count, chain_pair_from_index,
    new_chain_matrix_reuse_states, summary_row, AnalysisError, GroupSummary, NameTotals,
    ProgressTracker, SparseUnionFind, SummaryRow, SummarySpec, UnionFind,
};

mod parse;
mod bm25;
mod load;
mod index;
mod sketch;

pub(super) use parse::MAX_METADATA_BYTES_FOR_DEDUP;
#[cfg(test)]
pub(super) use load::metadata_raw_rows_sql;

use bm25::*;
use index::*;
use load::*;

pub(super) const METADATA_THRESHOLD: f64 = 0.6;
const METADATA_MATCH_MODE: &str = "template_recall_hybrid_verify";
pub(super) const METADATA_PAIR_LEFT_CHUNK_SIZE: usize = 256;
pub(super) const METADATA_CONTENT_PARALLEL_MIN_RECORDS: usize = 64;
pub(super) const METADATA_CONTENT_SCORE_BATCH_PAIRS: usize = 16 * 1024;
pub(super) type MetadataDocKey = String;
pub(super) type MetadataContractIndex = u32;
pub(super) type MetadataDocIndex = u32;

#[derive(Clone, Debug)]
pub(super) struct MetadataContract {
    pub(super) chain_index: usize,
    pub(super) nft_count: i64,
    pub(super) content_doc: Option<Arc<MetadataBm25Document>>,
    pub(super) template_doc_index: MetadataDocIndex,
}

#[derive(Debug)]
pub(super) struct SourceMetadataDocEntry {
    pub(super) doc: MetadataBm25Document,
    pub(super) contracts: Vec<MetadataContractIndex>,
}

#[derive(Debug)]
pub(super) struct MetadataData {
    pub(super) contracts: Vec<MetadataContract>,
    pub(super) contracts_by_chain: Vec<Vec<MetadataContractIndex>>,
    pub(super) compact_contract_indexes_by_source: Vec<Option<MetadataContractIndex>>,
    pub(super) metadata_index: InternedMetadataIndex,
}

#[derive(Debug, Default)]
pub(super) struct MetadataTemplateMatches {
    pub(super) compatible_docs: HashMap<MetadataDocIndex, Vec<MetadataDocIndex>>,
}

pub(super) struct MetadataDataBuilder {
    contracts: Vec<MetadataContract>,
    contracts_by_chain: Vec<Vec<MetadataContractIndex>>,
    source_contract_indexes: Vec<u32>,
    docs: Vec<SourceMetadataDocEntry>,
    doc_index_by_key: HashMap<MetadataDocKey, usize>,
}

#[derive(Default)]
struct MetadataComponentAccumulator {
    primary_contract_count: i64,
    primary_nft_count: i64,
    total_contract_count: i64,
    first_chain: Option<usize>,
    has_secondary: bool,
}

#[derive(Default)]
struct MetadataPairComponentAccumulator {
    left_contract_count: i64,
    left_nft_count: i64,
    right_contract_count: i64,
    right_nft_count: i64,
    total_contract_count: i64,
}

impl MetadataTemplateMatches {
    pub(super) fn from_pairs(pairs: impl IntoIterator<Item = (usize, usize)>) -> Self {
        let mut compatible_docs =
            HashMap::<MetadataDocIndex, Vec<MetadataDocIndex>>::new();
        for (left, right) in pairs {
            let left = metadata_doc_index_from_usize(left);
            let right = metadata_doc_index_from_usize(right);
            if left != right {
                compatible_docs.entry(left).or_default().push(right);
                compatible_docs.entry(right).or_default().push(left);
            }
        }
        for docs in compatible_docs.values_mut() {
            docs.sort_unstable();
            docs.dedup();
        }
        Self { compatible_docs }
    }

    pub(super) fn matches(&self, left: usize, right: usize) -> bool {
        left == right
            || self
                .compatible_docs(metadata_doc_index_from_usize(left))
                .binary_search(&metadata_doc_index_from_usize(right))
                .is_ok()
    }

    pub(super) fn compatible_docs(&self, doc: MetadataDocIndex) -> &[MetadataDocIndex] {
        self.compatible_docs.get(&doc).map_or(&[], Vec::as_slice)
    }
}

impl MetadataDataBuilder {
    pub(super) fn new(chain_count: usize) -> Self {
        Self {
            contracts: Vec::new(),
            contracts_by_chain: vec![Vec::new(); chain_count],
            source_contract_indexes: Vec::new(),
            docs: Vec::new(),
            doc_index_by_key: HashMap::new(),
        }
    }

    pub(super) fn merge_indexed_rows(&mut self, indexed_rows: Vec<(u32, IndexedMetadataRow)>) {
        for (source_contract_index, row) in indexed_rows {
            self.merge_source_indexed_row(source_contract_index, row);
        }
    }

    #[cfg(test)]
    fn merge_indexed_row(&mut self, row: IndexedMetadataRow) {
        let source_contract_index = u32::try_from(self.source_contract_indexes.len())
            .expect("metadata source contract index exceeds u32 indexes");
        self.merge_source_indexed_row(source_contract_index, row);
    }

    fn merge_source_indexed_row(
        &mut self,
        source_contract_index: u32,
        row: IndexedMetadataRow,
    ) {
        let doc_index = match self.doc_index_by_key.get(&row.doc_key).copied() {
            Some(index) => index,
            None => {
                let index = self.docs.len();
                self.doc_index_by_key.insert(row.doc_key, index);
                self.docs.push(SourceMetadataDocEntry {
                    doc: row.doc,
                    contracts: Vec::new(),
                });
                index
            }
        };
        let compact_doc_index = metadata_doc_index_from_usize(doc_index);
        let contract_index = self.contracts.len();
        self.contracts.push(MetadataContract {
            chain_index: row.chain_index,
            nft_count: row.nft_count,
            content_doc: MetadataBm25Document::from_text(&row.content_document)
                .map(Arc::new),
            template_doc_index: compact_doc_index,
        });
        self.contracts_by_chain[row.chain_index]
            .push(metadata_contract_index_from_usize(contract_index));
        self.source_contract_indexes.push(source_contract_index);
        let compact_contract_index = metadata_contract_index_from_usize(contract_index);
        self.docs[doc_index].contracts.push(compact_contract_index);
    }

    pub(super) fn finish(self) -> MetadataData {
        let mut compact_contract_indexes_by_source = self
            .source_contract_indexes
            .iter()
            .copied()
            .max()
            .map_or_else(Vec::new, |max_source_index| {
                vec![None; max_source_index as usize + 1]
            });
        for (compact_contract_index, source_contract_index) in
            self.source_contract_indexes.into_iter().enumerate()
        {
            compact_contract_indexes_by_source[source_contract_index as usize] =
                Some(metadata_contract_index_from_usize(compact_contract_index));
        }
        let metadata_index = InternedMetadataIndex::from_source_doc_entries(self.docs);
        MetadataData {
            contracts: self.contracts,
            contracts_by_chain: self.contracts_by_chain,
            compact_contract_indexes_by_source,
            metadata_index,
        }
    }

    pub(super) fn missing_source_indexes(&self, source_count: usize) -> Vec<u32> {
        let mut present = vec![false; source_count];
        for &source_index in &self.source_contract_indexes {
            if let Some(slot) = present.get_mut(source_index as usize) {
                *slot = true;
            }
        }
        present
            .into_iter()
            .enumerate()
            .filter(|(_, present)| !present)
            .map(|(source_index, _)| {
                u32::try_from(source_index)
                    .expect("metadata source contract index exceeds u32 indexes")
            })
            .collect()
    }
}

impl MetadataData {
    fn compact_contract_index_for_source(
        &self,
        source_contract_index: usize,
    ) -> Option<MetadataContractIndex> {
        self.compact_contract_indexes_by_source
            .get(source_contract_index)
            .copied()
            .flatten()
    }
}

pub(super) fn run_metadata_analysis(
    conn: &Connection,
    chains: &[String],
    totals: &HashMap<String, NameTotals>,
    threads: usize,
    progress: &ProgressTracker,
) -> Result<Vec<SummaryRow>, AnalysisError> {
    progress.start_phase("analyzing metadata duplicates", 3);
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads.max(1))
        .build()
        .map_err(|err| AnalysisError::InvalidData(err.to_string()))?;
    let data = load_metadata_data(conn, chains, &pool)?;
    progress.step(format!(
        "loaded {} metadata documents for {} contracts",
        data.metadata_index.docs.len(),
        data.contracts.len()
    ));
    let mut rows = Vec::new();
    if data.contracts.len() < 2 || data.metadata_index.docs.is_empty() {
        push_empty_metadata_rows(&mut rows, chains, totals);
        progress.step("metadata scoring skipped");
        progress.step("metadata rows summarized");
        progress.finish_phase("metadata analysis complete");
        return Ok(rows);
    }

    let mut state = MetadataUnionState {
        intra: UnionFind::new(data.contracts.len()),
        cross: (chains.len() > 1).then(SparseUnionFind::default),
        chain_matrix: (chains.len() > 1)
            .then(|| new_chain_matrix_reuse_states(chain_pair_count(chains.len()))),
    };
    let template_matches =
        pool.install(|| collect_metadata_template_matches(&data, progress));
    prepare_metadata_contract_token_rows(conn)?;
    let contract_tokens = load_metadata_contract_tokens(conn, &data)?;
    let content_context = MetadataContentUnionContext {
        data: &data,
        template_matches: &template_matches,
        contract_tokens: &contract_tokens,
        chain_count: chains.len(),
        pool: &pool,
    };
    union_metadata_token_content_matches(conn, &content_context, &mut state)?;
    union_metadata_representative_content_fallback(
        &content_context,
        &mut state,
    );
    progress.step("metadata documents scored");
    push_metadata_summary_rows(&mut rows, &data, chains, totals, &mut state);
    progress.step("metadata rows summarized");
    progress.finish_phase("metadata analysis complete");
    Ok(rows)
}

fn push_empty_metadata_rows(
    rows: &mut Vec<SummaryRow>,
    chains: &[String],
    totals: &HashMap<String, NameTotals>,
) {
    for chain in chains {
        let total = totals.get(chain).copied().unwrap_or(NameTotals {
            contracts: 0,
            nfts: 0,
        });
        rows.push(metadata_summary_row(
            "intra_chain",
            chain,
            "",
            total,
            GroupSummary::default(),
        ));
        if chains.len() > 1 {
            rows.push(metadata_summary_row(
                "cross_chain_summary",
                chain,
                "",
                total,
                GroupSummary::default(),
            ));
        }
    }
    if chains.len() > 1 {
        for primary_index in 0..chains.len() {
            for secondary_index in 0..chains.len() {
                if primary_index == secondary_index {
                    continue;
                }
                let primary = &chains[primary_index];
                let total = totals.get(primary).copied().unwrap_or(NameTotals {
                    contracts: 0,
                    nfts: 0,
                });
                rows.push(metadata_summary_row(
                    "chain_matrix",
                    primary,
                    &chains[secondary_index],
                    total,
                    GroupSummary::default(),
                ));
            }
        }
    }
}

fn push_metadata_summary_rows(
    rows: &mut Vec<SummaryRow>,
    data: &MetadataData,
    chains: &[String],
    totals: &HashMap<String, NameTotals>,
    state: &mut MetadataUnionState,
) {
    let mut dense_scratch = MetadataDenseComponentScratch::new(data.contracts.len());
    for (chain_index, chain) in chains.iter().enumerate() {
        let total = totals.get(chain).copied().unwrap_or(NameTotals {
            contracts: 0,
            nfts: 0,
        });
        let intra = summarize_metadata_dense_components_for_primary(
            data,
            &data.contracts_by_chain[chain_index],
            &mut state.intra,
            &mut dense_scratch,
        );
        rows.push(metadata_summary_row(
            "intra_chain",
            chain,
            "",
            total,
            intra,
        ));

        if let Some(cross) = &mut state.cross {
            let cross_summary = summarize_metadata_sparse_components_for_primary(data, cross, chain_index);
            rows.push(metadata_summary_row(
                "cross_chain_summary",
                chain,
                "",
                total,
                cross_summary,
            ));
        }
    }

    let Some(matrix) = &mut state.chain_matrix else {
        return;
    };
    for (pair_index, union_find) in matrix.iter_mut().enumerate() {
        let (left_chain, right_chain) = chain_pair_from_index(pair_index, chains.len());
        let (left_summary, right_summary) =
            summarize_metadata_sparse_components_for_chain_pair(data, union_find, left_chain, right_chain);
        push_metadata_chain_matrix_row(rows, chains, totals, left_chain, right_chain, left_summary);
        push_metadata_chain_matrix_row(rows, chains, totals, right_chain, left_chain, right_summary);
    }
}

struct MetadataDenseComponentScratch {
    components: Vec<MetadataComponentAccumulator>,
    touched_roots: Vec<usize>,
}

impl MetadataDenseComponentScratch {
    pub(super) fn new(size: usize) -> Self {
        let mut components = Vec::with_capacity(size);
        components.resize_with(size, MetadataComponentAccumulator::default);
        Self {
            components,
            touched_roots: Vec::new(),
        }
    }

    fn clear_touched(&mut self) {
        for root in self.touched_roots.drain(..) {
            self.components[root] = MetadataComponentAccumulator::default();
        }
    }
}

fn summarize_metadata_dense_components_for_primary(
    data: &MetadataData,
    primary_contracts: &[MetadataContractIndex],
    union_find: &mut UnionFind,
    scratch: &mut MetadataDenseComponentScratch,
) -> GroupSummary {
    for &index in primary_contracts {
        let index = metadata_contract_index_to_usize(index);
        let contract = &data.contracts[index];
        let root = union_find.find(index);
        let component = &mut scratch.components[root];
        if component.total_contract_count == 0 && component.primary_contract_count == 0 {
            scratch.touched_roots.push(root);
        }
        component.total_contract_count += 1;
        component.primary_contract_count += 1;
        component.primary_nft_count += contract.nft_count;
    }

    let mut summary = GroupSummary::default();
    for &root in &scratch.touched_roots {
        let component = &scratch.components[root];
        if component.primary_contract_count == 0 || component.total_contract_count < 2 {
            continue;
        }
        summary.group_count += 1;
        summary.duplicate_contract_count += component.primary_contract_count;
        summary.duplicate_nft_count += component.primary_nft_count;
        summary.group_size_ge_2_count += i64::from(component.total_contract_count >= 2);
        summary.group_size_gt_2_count += i64::from(component.total_contract_count > 2);
    }
    scratch.clear_touched();
    summary
}

fn summarize_metadata_sparse_components_for_primary(
    data: &MetadataData,
    union_find: &mut SparseUnionFind,
    primary: usize,
) -> GroupSummary {
    let mut components = HashMap::<usize, MetadataComponentAccumulator>::new();
    for local_index in 0..union_find.atom_count() {
        let contract_index = union_find.atom_at(local_index);
        let contract = &data.contracts[contract_index];
        let root = union_find.find_local(local_index);
        let component = components.entry(root).or_default();
        component.total_contract_count += 1;
        match component.first_chain {
            Some(first) if first != contract.chain_index => component.has_secondary = true,
            None => component.first_chain = Some(contract.chain_index),
            _ => {}
        }
        if contract.chain_index != primary {
            component.has_secondary = true;
        } else {
            component.primary_contract_count += 1;
            component.primary_nft_count += contract.nft_count;
        }
    }

    let mut summary = GroupSummary::default();
    for component in components.values() {
        if component.primary_contract_count == 0
            || !component.has_secondary
            || component.total_contract_count < 2
        {
            continue;
        }
        summary.group_count += 1;
        summary.duplicate_contract_count += component.primary_contract_count;
        summary.duplicate_nft_count += component.primary_nft_count;
        summary.group_size_ge_2_count += i64::from(component.total_contract_count >= 2);
        summary.group_size_gt_2_count += i64::from(component.total_contract_count > 2);
    }
    summary
}

fn summarize_metadata_sparse_components_for_chain_pair(
    data: &MetadataData,
    union_find: &mut SparseUnionFind,
    left_chain: usize,
    right_chain: usize,
) -> (GroupSummary, GroupSummary) {
    let mut components = HashMap::<usize, MetadataPairComponentAccumulator>::new();
    for local_index in 0..union_find.atom_count() {
        let contract_index = union_find.atom_at(local_index);
        let contract = &data.contracts[contract_index];
        let root = union_find.find_local(local_index);
        let component = components.entry(root).or_default();
        component.total_contract_count += 1;
        if contract.chain_index == left_chain {
            component.left_contract_count += 1;
            component.left_nft_count += contract.nft_count;
        } else if contract.chain_index == right_chain {
            component.right_contract_count += 1;
            component.right_nft_count += contract.nft_count;
        }
    }

    let mut left_summary = GroupSummary::default();
    let mut right_summary = GroupSummary::default();
    for component in components.values() {
        accumulate_pair_component_summary(
            &mut left_summary,
            component.left_contract_count,
            component.left_nft_count,
            component.right_contract_count,
            component.total_contract_count,
        );
        accumulate_pair_component_summary(
            &mut right_summary,
            component.right_contract_count,
            component.right_nft_count,
            component.left_contract_count,
            component.total_contract_count,
        );
    }
    (left_summary, right_summary)
}

fn push_metadata_chain_matrix_row(
    rows: &mut Vec<SummaryRow>,
    chains: &[String],
    totals: &HashMap<String, NameTotals>,
    primary_index: usize,
    secondary_index: usize,
    summary: GroupSummary,
) {
    let primary = &chains[primary_index];
    let total = totals.get(primary).copied().unwrap_or(NameTotals {
        contracts: 0,
        nfts: 0,
    });
    rows.push(metadata_summary_row(
        "chain_matrix",
        primary,
        &chains[secondary_index],
        total,
        summary,
    ));
}

fn metadata_summary_row(
    scope: &str,
    primary_chain: &str,
    secondary_chain: &str,
    total: NameTotals,
    summary: GroupSummary,
) -> SummaryRow {
    summary_row(
        SummarySpec {
            field_name: "metadata",
            scope,
            primary_chain,
            secondary_chain,
            threshold: Some(METADATA_THRESHOLD),
            match_mode: METADATA_MATCH_MODE,
            metric: "duplicate_group",
            total_contracts: total.contracts,
            total_nfts: total.nfts,
        },
        summary,
    )
}

pub(super) fn metadata_contract_index_from_usize(index: usize) -> MetadataContractIndex {
    MetadataContractIndex::try_from(index)
        .expect("metadata contract count must fit in compact u32 membership indexes")
}

pub(super) fn metadata_contract_index_to_usize(index: MetadataContractIndex) -> usize {
    index as usize
}

pub(super) fn metadata_doc_index_from_usize(index: usize) -> MetadataDocIndex {
    MetadataDocIndex::try_from(index)
        .expect("metadata document count must fit in compact u32 postings")
}

pub(super) fn metadata_doc_index_to_usize(index: MetadataDocIndex) -> usize {
    index as usize
}

#[cfg(test)]
mod tests;
